# Memory 后端

> 全内存的 `IStorageBackend` 实现，代码在 `src/storage/memory/memory_backend.{h,cc}`。
> 定位是**单测夹具与演示后端**：无持久化、无后台线程、无线程池，语义与 localfs
> 对齐并通过同一套后端一致性套件（[storage-backend.md](../storage-backend.md) §6）。
> 同时它也在 `storage/registry.cc` 注册为一等后端，可被配置进网关，因此带有
> 容量闸门与 MPU 过期清理，防止误配置时无上界地吃堆内存。

## 1. 定位与配置

`memory_backend.h:MemoryOptions` 只有两个旋钮：

| 参数 | 默认 | 说明 |
| --- | --- | --- |
| `max_bytes` | 0（不限） | 容量上限：对象 + 在途分片的字节总和。0 为单测/演示保留旧行为；生产配置应显式设值，超限的写入返回 503 `SlowDown`（可重试，且指标可见），而不是把裁决权交给 OOM killer |
| `mpu_ttl_sec` | 24h | 从未 complete/abort 的 multipart 上传的过期时间。0 = 永不过期。清理**搭车**在 multipart 操作入口执行（§6），本后端刻意不起定时器——"没有后台线程"本身是它作为单测夹具的一部分 |

`storage/registry.cc` 中以 `"memory"` 名注册：从配置读 `max_bytes` / `mpu_ttl`，
并挂一个回调 gauge `lights3_memory_backend_used_bytes`（经 `weak_ptr` 在渲染时读
`MemoryBackend::used_bytes`），让"离上限还有多远"在监控面可见。

## 2. 内存数据结构

四层结构全部是 `std::map`（有序，天然满足 list 的字典序要求）：

```text
buckets_ : map<bucket名, Bucket>        Bucket { BucketInfo info; map<key, Object> objects; }
uploads_ : map<upload_id, Upload>       Upload { bucket, key, ObjectMeta meta,
                                                 map<part_no, Part> parts, initiated }
Object   { ObjectMeta meta; shared_ptr<const string> data; }
Part     { string data; string etag /*分片MD5 hex*/; time_point uploaded; }
```

要点：

- **对象数据是不可变共享块**（`memory_backend.h:MemoryBackend::Object`）：
  `shared_ptr<const std::string>`。GET 在锁内只抓一次 `shared_ptr`，流式发送在锁外；
  PUT 覆盖同 key 时旧块被仍在流式读它的 GET 持有，读完自然释放——**免费获得快照隔离**。
  历史上 GET 曾在全局锁内整块拷贝对象（1GB 对象 = 2GB 常驻 + 期间锁死所有 bucket）。
- **无版本化**：同 key 覆盖即替换，没有 version id / delete marker（§7）。
- `uploads_` 以 upload_id 为主键、全局一张表（不按 bucket 分），`Upload` 里冗余记
  bucket/key 用于归属校验；代价是按 bucket 枚举必然全表扫（§6）。
- `used_bytes_`：对象 + 在途分片的字节总账，由 `m_` 保护，是容量闸门与指标的依据。

## 3. 锁模型

- 单把全局 `std::mutex m_` 保护 `buckets_` / `uploads_` / `used_bytes_` 全部状态。
- **临界区全部是 O(map 操作) 的微秒级**：所有整对象/整分片的大动作
  （读 body、拷贝数据、算 MD5、流式响应）都刻意放在锁外。因此本后端**不接线程池**，
  在事件循环线程上直接持这把短锁是演示/单测场景下的刻意取舍
  （见 `memory_backend.h` 中 `Object` 处的注释）。
- 锁是普通 mutex 而非协程锁：临界区内没有 `co_await`，不会跨挂起点持锁。

## 4. 对象读写流程

### PUT（`memory_backend.cc:MemoryBackend::put_object`）

1. `validate_bucket_name` / `validate_object_key`（各后端共有的纵深防御）。
2. **锁外**把 body 读到 EOF：64KiB 缓冲循环 `co_await body.read`，边读边喂
   `util::HashStream`（MD5→ETag），追加进 `std::string data`；每轮调
   `check_inflight(data.size())` 做**在途预检**（§5）。读到 EOF 才算完，满足
   `backend.h:IStorageBackend::put_object` 的 body 契约（上层 sha256/aws-chunked
   校验装饰器挂在"读满 + EOF"上）；body 抛异常则自然不落账、不进 map。
3. 填 meta（key/size/etag/last_modified），把 data move 进
   `make_shared<const string>`。
4. **加锁提交**：条件检查与提交在同一临界区内完成（`PutCondition` 契约——
   `if_none_match` 已存在抛 `PreconditionFailed`；`if_match_etag` 不存在抛
   `NoSuchKey`、不等抛 `PreconditionFailed`）；然后 `reserve_locked(新-旧)` 过容量闸，
   **闸门通过后才碰 map**（若先 `operator[]` 插入再被 reserve 抛出，map 里会留下
   data 为空指针的幽灵条目，后续 GET 解引用空指针），最后 `insert_or_assign` 覆盖。

### GET（`memory_backend.cc:MemoryBackend::get_object`）

锁内只拷 `ObjectMeta` 和抓 `shared_ptr` 数据块；锁外用 `resolve_range` 解析
Range（不满足抛 416），构造 `memory_backend.cc:SharedBlobReader`——一个持有
`shared_ptr<const string>` + (off, len) 的只读 `BodyReader`，每次 `read` 就是
`memcpy`，向客户端流式发送全程不占锁、不复制整块。

### HEAD / DELETE

`head_object` 锁内查 map 返回 meta 拷贝；`delete_object` 锁内
`reserve_locked(-size)` 回账并 erase，**key 不存在时静默成功**（S3 幂等删除语义）。
数据块本体若仍被在途 GET 引用，则由 `shared_ptr` 计数延后释放。

### Copy

**没有 fast path**：不覆写 `backend.h:IStorageBackend::copy_object_fast`（默认返回
`nullopt`），CopyObject 由 L2 handler 走"`get_object` 流式读 + `put_object` 流式写"
的通用回退。语义等价，只多一次内存拷贝——对内存后端可接受。

## 5. 容量闸门（docs/gaps.md §6.3）

两道闸，语义一致（超限抛 503 `SlowDown`）：

- **提交闸** `memory_backend.cc:MemoryBackend::reserve_locked`：持 `m_` 调用，
  参数是本次调用的净增量（可为负=回账）。正增量且设了 `max_bytes` 时先判
  `used + delta > max`，超限**不动账本直接抛**；通过才更新 `used_bytes_`。
- **在途闸** `memory_backend.cc:MemoryBackend::check_inflight`：不持锁、在读 body
  的循环里逐轮调用。若只在 EOF 后查容量，超大 body 的字节已经全部进堆，OOM 保护
  恰在最需要时失效；因此保守地按 `used_bytes() + 本请求已缓冲字节` 预判。在途缓冲
  **不计入** `used_bytes_`（记账发生在提交时），所以并发多路大上传仍可能瞬时超冲——
  这是夹具级后端接受的近似。

`complete_multipart` 的记账顺序值得单说：拼接出的新对象与各分片会**短暂同时常驻**，
实现先为新对象 `reserve_locked`（可能超限抛出，此时 upload 完好、客户端可重试或
abort），成功后才回账释放分片并 erase upload；且为保证失败路径上 upload 不被破坏，
`up.meta` 用**拷贝**而非 move。

## 6. Multipart 状态机

- **create**（`memory_backend.cc:MemoryBackend::create_multipart`）：锁内先
  `expire_uploads_locked` 搭车清理，校验 bucket 存在，`multipart.h:new_upload_id`
  生成 32 位随机 hex 作 upload_id，插入 `uploads_`。
- **upload_part**：先短锁校验 upload 存在（fail early），**锁外**读 body（同 PUT，
  含 MD5 与在途闸），再加锁**重新** `upload_or_throw`——读 body 期间 upload 可能已被
  abort。同号重传 last-write-wins；同样"容量闸通过后才碰 map"，否则会留下 etag 为空
  的幽灵分片。
- **complete**：`multipart.h:validate_part_order` 预检（空/非严格递增分别抛
  `InvalidPart`/`InvalidPartOrder`）；锁内逐个核对分片存在且 ETag 相符
  （`strip_etag_quotes` 后比较），把分片数据**串接成一个字符串**落为普通对象；
  合成 ETag 走 `multipart.h:combined_etag`（各分片二进制 MD5 串接再 MD5 + `-N`，
  与 S3 同规则）；记账顺序见 §5；最后 erase upload。注意 5MiB 最小分片是 L2 的协议
  检查，存储层不设限（直接调后端 API 的一致性套件不受约束）。
- **abort**：回账全部分片字节后 erase，upload 不存在抛 `NoSuchUpload`。
- **list_parts**：`parts` 本身是 `map<int,...>`，`upper_bound(marker)` 直接跳过
  marker 之前的条目，再交给 `listing.h:apply_parts_page` 统一分页。
- **list_multipart_uploads**：索引按 upload_id 组织，按 bucket 枚举只能**全扫 + 按
  (key, upload_id) 排序**，然后交 `listing.h:apply_uploads_page`；入口同样搭车
  `expire_uploads_locked`。
- **过期清理** `memory_backend.cc:MemoryBackend::expire_uploads_locked`：持锁遍历
  `uploads_`，`initiated` 早于 `now - mpu_ttl_sec` 的整个 upload 回账并删除。只挂在
  create / list_uploads 两个入口——没有定时器，所以一个再也无人碰 multipart 接口的
  进程里过期 upload 不会被清（夹具场景可接受）。

`memory_backend.cc:MemoryBackend::upload_or_throw` 同时校验 upload_id 存在**且**
bucket/key 归属相符，不符一律 `NoSuchUpload`。

## 7. 对象列举

`memory_backend.cc:MemoryBackend::list_objects` 锁内把 bucket 的全部 key（map 已
字典序）收进 vector，交给 `listing.h:apply_listing` 统一处理
prefix/delimiter/max_keys/start_after 与截断语义——与 localfs 共用同一实现，保证
分页行为逐字节一致。取 meta 用 `objects.at(k)` 而非 `operator[]`（后者会为不存在的
key 静默插入空对象）。整个列举持锁完成，bucket 很大时是这个后端少数的长临界区。

## 8. S3 语义覆盖面

模拟的：bucket CRUD（重名抛 `BucketAlreadyOwnedByYou`、非空删除抛
`BucketNotEmpty`）、对象 CRUD + Range GET（416）、幂等删除、条件 PUT
（If-None-Match:* / If-Match）、ListObjects 全套分页/delimiter、multipart 全套
（含 S3 规则的合成 ETag、两个 multipart 列举的真实分页）、`.sys` 保留桶
（`kAllowReserved`，供 CredentialStore 使用）、错误一律抛 `s3::S3Error`。

不模拟的：版本化 / delete marker、对象锁、ACL/Policy、生命周期、STANDARD 以外的
存储类（接口层即 501）、持久化（进程退出即失、`close` 即清）。

## 9. 在测试中的角色

- `tests/unit/test_storage.cc`：后端一致性套件的基线实现，与 localfs/xlocalfs
  参数化跑同一组用例；另有针对 `max_bytes`/`mpu_ttl_sec` 的专项用例。
- `tests/unit/test_service.cc`：mock HttpRequest + memory 后端跑通 L2 全分发的
  纯逻辑测试。
- `tests/unit/test_tiered.cc` / `test_cloudproxy.cc`：扮演"云端"——tiered 用计数
  包装断言云调用次数，cloudproxy 用 S3Service + MemoryBackend 起真 HTTP 远端。
- `tests/unit/test_credentials.cc`：给 CredentialStore 当 `.sys` 桶的载体。

## 10. 内存与生命周期

- 一切状态随进程存亡；`memory_backend.cc:MemoryBackend::close` 显式清空
  `buckets_`/`uploads_` 并归零 `used_bytes_`，让关停即刻还内存（历史上没有 close，
  内存一直占到进程销毁）。
- 覆盖/删除后的旧数据块由在途 GET 的 `shared_ptr` 引用兜底，最后一个读者读完才释放
  ——不存在悬垂读，但也意味着峰值内存 = 账面 `used_bytes_` + 被在途读者钉住的旧块
  + 未记账的在途上传缓冲。
- `complete_multipart` 峰值时新对象与分片并存，最坏瞬时约 2 倍对象大小。

相关文档：接口契约与扩展指南见 [storage-backend.md](../storage-backend.md)；
读写全链路见 [object-read-write-flow.md](../object-read-write-flow.md)。
