# 待调整与未实现清单

> 生成时间：2026-08-02。方法：按子系统并行深读全部 23k 行源码（core / http / s3 /
> storage 通用与本地系 / duostore / cloudproxy），逐条对照 `docs/` 各设计文档的承诺
> 与真实 AWS S3 语义交叉核对，并做了一遍中英文档一致性巡检。每条标注 `文件:行`、
> 触发条件、严重度与建议。
>
> **本文只记录"需要改"与"还没有"，不记录已完成的能力**；实现现状见各设计文档。
> 行号对应 2026-08-02 的 `main`（commit `b15f9e1` 前后），重构后可能漂移，以函数名为准。

## 严重度约定

| 级别 | 含义 |
| --- | --- |
| **P0** | 安全越权或静默数据丢失，可被普通权限用户触发 |
| **高** | 生产可达的正确性/可用性缺陷，或使某个功能实质不可用 |
| **中** | 特定负载/时序下的正确性、性能或运维问题 |
| **低** | 契约瑕疵、可观测性缺口、性能浪费 |

## 速览

- **第一部分 · 需要调整的实现**：P0 四条（§1）、高 二十余条（§2，其中 12 条展开、13 条列表）、中 三十余条（§3）、低 二十余条（§4）。
- **第二部分 · 未实现的功能**：S3 协议缺口 10 项（§5，**已全部实现**）、存储引擎能力缺口 23 项（§6，**已全部处理**——实现或定性为设计边界）、运维与工程能力缺口 10 项（§7，**已全部处理**）。
- **第三部分 · 文档与实现的偏差**：15 条实现与承诺不符（含"改实现"与"改文档"两类），另有 7 条中英文档漂移（§8，**已全部修复**）。

P0 四条**已于 2026-08-03 全部修复**（详见 §1），高危 25 条（§2 的 12 条展开 + §2.13 的 13 行）
**已于 2026-08-06 全部修复**（详见 §2；其中 §2.6 的下推顺带解决了 §3.6），中危三十余条
**已于 2026-08-08 全部修复**（详见 §3），低危 23 行**已于 2026-08-08 全部修复**（详见 §4）。
第一部分至此清零。第二部分的 **§5 S3 协议缺口 10 小节已于 2026-08-09 全部实现**
（详见 §5），**§6 存储引擎能力缺口 23 项已于 2026-08-11 全部处理**（详见 §6，
含若干条按 workload 论证后的刻意保留），**§7 运维与工程能力缺口 10 项已于
2026-08-12 全部处理**（详见 §7，顺带修复 §6.3 校验收紧引入的 cloudproxy 保留桶
回归）；第二部分至此清零。**§8 中英文档漂移 7 条已于 2026-08-12 全部修复**（详见 §8）。当前未修复的起点是第三部分开头的"文档与实现的偏差"15 条对照表。

P0 四条按影响排：vhost bucket 名不校验（任意文件读 + 全部凭证泄露）、`data=rados` 缺写侧 pin（删在途数据）、tiered 缓存回填缺 fsync（掉电后静默返回零块）、多网关下封存他方 active pack（静默丢数据）。

---

# 第一部分 · 需要调整的实现

## 1. P0 —— 安全越权与静默数据丢失

> **2026-08-03 已全部修复**（四条同批上线，各条末尾附实际修法与回归用例）。
> 验证：主构建 261 / TSan 246（零竞争）/ sqlite 11 套件 / redis 11 套件 /
> rados 变体编译通过。

### 1.1 [✅已修复] vhost 寻址的 bucket 名从不校验 → 任意文件读 + 全部凭证泄露 + 提权

**位置**：`src/s3/service.cc:96-118`（`resolve_address` vhost 分支，`:110` 把 Host 前缀原样当 bucket）、`:149-154`（全链路唯一检查，只看首字符是否为 `.`）；`src/storage/localfs/localfs_backend.cc:54-60`（`object_path = root_ / fs::path(bucket) / key`）、`:173-180`（`get_object` 只校验 key，且 `require_bucket` 只在 `open` **失败**分支才调用）；xlocalfs 同构。

**机制**：三处缺陷叠加。(1) 驱动对 `Host` 头不做字符集校验，`/`、`..`、`%00` 都能进入头值；(2) `resolve_address` 命中 base_domain 后直接取前缀，从不调用 `storage::validate_bucket_name`；(3) `std::filesystem::operator/` 遇绝对路径右操作数会**替换整条路径**（`root_ / "/etc"` == `/etc`），而 localfs 的**读**路径（get/head/list/delete）全部缺 `validate_bucket_name`——只有写路径有（`:76`/`:129`/`:285`）。读写校验不对称是这个洞的直接成因。

**触发**（前提：配置了 `http.base_domain`；攻击者持任一合法凭证，用自己的 SK 签名，验签正常通过）：

```
GET /passwd     Host: /etc.gw.example.com          → 读出 /etc/passwd
GET /?list-type=2   Host: b/../.sys.gw.example.com → 列举全部动态凭证
GET /credentials/L3AK…  Host: b/../.sys.gw.example.com
                                                   → 读出 SK（未设 LIGHTS3_MASTER_KEY 时为明文）
```
`b` 为任一已存在的桶。拿到其它凭证即横向越权，绕过全部 per-credential policy。

**同源入口**：path-style 的 `parse_bucket_key`（`src/s3/router.cc:5-11`）同样零校验。它按 `/` 切分拿不到斜杠，但 `req.path` 是 percent-decode 后的结果，`%00` 会解成真正的 NUL——`GET /%00.sys/credentials/…` 的 `bucket.front()` 是 `'\0'` 而非 `'.'`，绕过保留名闸门，而下游的 `c_str()`/`open()` 在 NUL 处截断。`x-amz-copy-source`（`src/s3/handlers/common.h:63-83`）目前安全，但用的是第三份独立的 `.` 前缀启发式。

**建议**（缺一不可）：
1. `dispatch` 中对非空 bucket 统一调 `storage::validate_bucket_name()`，**替换**掉 `service.cc:152` 的首字符启发式——一次堵死 vhost 与 path-style 两条入口，`.` 前缀检查的三份副本（`service.cc:152`、`common.h:79`、`buckets.cc:14`）也随之收敛为一份。
2. `validate_bucket_name` 增加 `bool allow_reserved = false`，把 `.sys` 的放行（`src/storage/validate.cc:15`）收窄到只有 `CredentialStore` 显式传 true——当前是对所有后端无条件放行。
3. localfs/xlocalfs/memory/tiered 的**全部**数据面入口补 `validate_bucket_name`；`get_object` 的 `require_bucket` 提到 `::open()` 之前。
4. 纵深防御：`object_path()` 之后 `fs::weakly_canonical` 并断言结果仍以 `root_` 为前缀。

**✅已修复**（四处同批）：(1) `dispatch` 对非空 bucket 统一调
`storage::validate_bucket_name()`，替换掉首字符启发式；(2) `validate_bucket_name`
增加 `allow_reserved` 形参（默认 false），`.sys` 只对显式传 true 的调用方开放，
保留名常量提为 `storage::kSysBucketName`；(3) localfs / xlocalfs / memory / tiered
的全部数据面入口补 `validate_bucket_name(bucket, kAllowReserved)`（后端这一层校验
的是路径安全，保留名拦截由 L2 负责），localfs/xlocalfs 的 `get_object` 把
`require_bucket` 提到 `::open()` 之前；(4) `object_path()` 用 `lexically_normal`
比对确认结果仍在 `root_` 之下，否则抛 InvalidBucketName。`.` 前缀检查的三份副本
（service / copy-source / ListBuckets 过滤）收敛为同一个函数。
回归用例四组（vhost 恶意 Host、path-style 含 NUL/控制字符、copy-source、合法桶名
不受影响）——移除 L2 闸门即有 2 个用例失败。
### 1.2 [✅已修复] `data=rados` 无写侧 pin → 孤儿扫描删掉在途大对象已落地的分片

**位置**：`src/storage/duostore/duostore_backend.cc:548-562`（rados 装配分支不注入 `ChunkPinHooks`，`write_pins_` 保持 false；对比 fs 分支 `:578-580`）、`:1180`（`pinned_chunk()` 因此恒 false）、`src/storage/duostore/rados_data_store.cc:293-313`。

**机制**：大对象 PUT 的第 1 片在 T0 落地，整个 PUT 在 T0+Δ 才提交 meta。Δ > `gc_grace`（默认 300s，8MiB 切片下几十 GB 上传或慢客户端即可达到）时跑一次孤儿扫描：该 id 不在 refs、mtime 逾 grace、无 pin、`chunk_referenced` 亦否 → 直接删除。PUT 随后提交成功，得到一个引用已删对象的坏对象，GET 报 500。fs 路径正是为此才有写侧 pin，rados 路径整条缺失。

**建议**：把 `ChunkPinHooks` 提升为 `IDataStore` 的通用装配项，`RadosChunkWriter::start_flush` 分配 file_id 后立即 pin、未 finish 即析构时 unpin；rados 分支同样置 `write_pins_ = true`。补上之前，`data=rados` 应强制关闭孤儿扫描或把 `gc_grace` 下限抬到最长预期 PUT 时长。

**✅已修复**：`ChunkPinHooks` 从 `fs_data_store.h` 提到 `data_store.h`（并加
`pin_one`/`unpin_one` 的空安全包装），`RadosDataOptions` 增加 `pins` 字段；
`RadosChunkWriter::start_flush` 分配 file_id 后立即 pin 并记入 `pinned_`，
`finish()` 成功时清空该表（解 pin 责任随 DataRef 移交调用方），未 finish 即析构
则就地解除；backend 的 rados 装配分支注入与 fs 同源的钩子并置 `write_pins_ = true`。

### 1.3 [✅已修复] tiered 缓存回填 rename 前不 fsync → 掉电后静默返回整文件零块

**位置**：`src/storage/localfs/fs_util.cc:257-267`（`commit_cached` 直接 `fs::rename`，无 `fsync_path`；对比 `commit_object_file:181-187` 有）。

**机制**：数据文件 rename 前不 fsync，**随后写的 sidecar 却是 fsync 过的**。崩溃窗口里 sidecar（`tier=cached`, `size=N`）已持久化而数据块还在 page cache。重启后 `load_object_meta_stat` 读出 tier=cached、size=N；`get_object` 的 StubRace 检查比的是 `st_size != meta.size`，而 ext4 上 rename 后 inode size 已是 N（元数据已提交、数据块未提交），检查**通过**——于是把一整个文件的零块当对象内容返回，ETag 还是正确的。tiered 认为本地缓存有效，不会回云端取。

**建议**：`fs::rename` 前插 `fsync_path(tmp.path)`、后插 `fsync_dir(dest.parent_path())`，与 `commit_object_file` 逐字对齐。

**✅已修复**：按建议实现，注释写明"数据必须在 rename 之前落盘"的理由与 StubRace
检查为何拦不住。回归用例走完整的下沉 → Tee 回填 → 缓存命中读取链路，断言两次读出
的内容与原文逐字节一致。

### 1.4 [✅已修复] duostore 多网关下 `abandon_stale_packs` 封存他方正在写的 pack → 静默数据丢失

**位置**：`src/storage/duostore/duostore_backend.cc:634-642`（构造期无条件把所有 unsealed pack 补封为 `file_size=0`）。

**机制**：该函数不区分"本实例上一代遗留"与"其他实例正在写"。两个网关共享同一 meta + 同一 fs data root（redis/tikv meta 的误配，或滚动重启期间新旧进程重叠）时，网关 B 启动会把网关 A 的 **active** pack 标记为 sealed → 进入下面 2.3 的无条件全量重写 → 若其账一度归零还会被 `remove_pack` 整删，而 A 仍持有该 fd 继续追加，写入落到已删 inode。

**建议**：pack 账记录 owner instance id（rocks 在 schema 初始化时已写了 `instance` 但从未使用），只补封 owner == 本实例的项；或在 `gc_enabled=false`（从网关）时跳过补封。

**✅已修复**（两道门，均不需要改 meta schema）：(1) `gc_enabled=false` 的从网关
直接跳过补封——它本就不该动别人的账；(2) 主网关逐个探测 pack 是否正被写：
`FsDataStore` 给每个 active pack 加 `flock(LOCK_EX|LOCK_NB)` 咨询锁（随 fd 关闭
自动释放，封存/退出/崩溃都算），新增 `IDataStore::pack_write_locked()` 以非阻塞
加锁探测，拿得到锁 ⇒ 无人持有 ⇒ 才补封。默认实现返回 false（保守方向：返回 true
只推迟补封，返回 false 才可能封掉别人的 pack）。选咨询锁而非 owner id 是因为它
不需要动四个 meta 引擎的编码与 schema 版本，且"谁持有 fd"本就是比"谁写过账"更
准确的存活信号。回归用例验证本实例持锁时探测为真、close 后为假。

---

## 2. 高

> **2026-08-06 已全部修复**（12 条展开 + §2.13 的 13 行，分五批上线，各条末尾附实际修法）。
> 对应提交：`617245c`（2.1/2.2/2.7/2.8 与 §2.13 部分）、`ddca047`（2.4/2.9 与 §2.13 部分）、
> `78b770e`（2.3/2.11 与 §2.13 duostore 三行）、`fb1a1c9`（2.12）、
> `021a1d6`（2.5/2.6/2.10 与 §2.13 剩余六行），合入 PR #32。
> 验证：主构建 271 + e2e 12 / TSan 256（零竞争）/ sqlite 259 / redis 256（真实例）/
> rados 257 / tikv 257（tiup playground 真集群）全绿；seastar 无本机构建条件，改动为最小增量。

### 2.1 [✅已修复] 关停期 `std::terminate`：`post` 在 join 后抛异常，却被 noexcept 上下文调用

**位置**：抛出点 `src/core/thread_pool.cc:22`；noexcept 消费点 `src/core/task.h:55`（`FinalAwaiter::await_suspend` 标 noexcept 却调 `cont_executor->post`）、`src/core/semaphore.h:99`（`release_one` 源头是 `Permit::~Permit` 的隐式 noexcept 析构）。

**新证据（使其从理论变为必然路径）**：`src/http/drivers/builtin/builtin_server.cc:441-446` 的 `run()` 在 10s 宽限 + 强关 + 再等 5s 后**无条件返回**，此时 `sh.active` 可能仍 > 0；`main.cc:92-109` 随即 `close()` 后端并 `pool->join()`。残余连接线程上的请求完成时，续体投递与许可释放正好落在上述两个 noexcept 点。beast 的 `ioc_.stop()`（`beast_server.cc:501`）更彻底——丢弃全部挂起 handler，其中的 `coroutine_handle` 永不 resume 也永不 destroy，协程帧与其持有的 BodyReader 全部泄漏。

**建议**：`post` 对 stopping 改为"就地 resume 或丢弃并记 ERROR"，绝不抛（`schedule()` 保留抛出语义，它有 co_await 承接点）；同时给 `IHttpServer::run()` 定义"返回即保证 handler 不再被调用且已返回"的硬契约，或让 `main` 在 `close()` 前等 `inflight->available()` 归位（带超时）。

**✅已修复**：`ThreadPool::post` 在 `stopping_` 之后不再抛，改为就地在调用线程执行并记
ERROR（`thread_pool.cc:24-33`）——noexcept 消费点（`FinalAwaiter::await_suspend`、
`Permit::~Permit`）不再有异常来源；`schedule()` 保留抛出语义（它有 co_await 承接点）。

### 2.2 [✅已修复] 后台线程主循环无异常防线 → 异常逃逸线程函数即 terminate

**位置**：`src/core/thread_pool.cc:85`（`item.fn();` 裸调用）、`src/core/timer.cc:68`（`fn();` 裸调用）。

**次生问题**：TimerQueue 回调抛出时 `running_id_ = 0; done_cv_.notify_all();`（`timer.cc:70-71`）被跳过，于是 `cancel(该 id)` **永久阻塞**——而 `cancel` 正是所有后端关停路径的第一步（`tiered_backend.cc:185`、`duostore_backend.cc:1266`、`credential_store.cc:660`）。

**建议**：两处循环体各包 try/catch 记 ERROR；`running_id_` 复位与 `notify_all` 放进 RAII 守卫，保证任何路径都执行。成本极低，建议优先。

**✅已修复**：`thread_pool.cc:99-101` 与 `timer.cc:76-79` 两处循环体各包 try/catch 记
ERROR；`running_id_ = 0` 与 `done_cv_.notify_all()` 移到回调之后的必经路径
（`timer.cc:82-83`），回调抛出不再让 `cancel()`（所有后端关停路径第一步）永久阻塞。

### 2.3 [✅已修复] duostore 压实的两个不收敛缺陷

**(a) 存活率分母口径不一致 → 小对象负载下永久重写循环**
`duostore_backend.cc:1065-1066` 用 `live_bytes >= ratio * file_size` 判定，而 `live_bytes` 只累计 payload（`fs_data_store.cc:459-461`），`file_size` 含每 record 的 `22 + owner_len` 头部（`:55`/`:61-75`）。小对象（100B payload + 60B owner）即使 pack **100% 存活**比值也只有 ~0.55，默认 `pack_gc_ratio=0.5` 下恒低于阈值。后果：pack A 全量重写 → 存活 record 迁入 B → A 删除 → B 封存后同样低于阈值 → 再重写，**数据被无限搬运，GC 永不收敛**，且 `compact_blocked_` 拦不住（每轮 live_recs 都归零，属"账有推进"）。

**(b) 崩溃遗留 pack 无条件全量重写**
`duostore_backend.cc:1065` 的 `ps.file_size > 0 &&` 短路使 `file_size==0` 直接跳过存活率判定进压实。每次非优雅退出都会把全部 active pack（默认 4 × 128MiB）补封为 0，下轮 GC 无条件完整顺扫 + 逐 record 迁移，只为回填一个 `fstat` 就能拿到的数字。

**建议**：(a) 判据改为"可回收字节"（`file_size - live_bytes_with_header > max(min_reclaim, ratio * file_size)`），并让 `live_bytes` 计入 header 使分子分母同口径；(b) 给 `IDataStore` 加轻量 `stat_pack(pack_id)`，判定前先回填 file_size。

**✅已修复**：(a) `live_bytes` 改为含 record 头口径（`codec::pack_rec_overhead*`），四引擎
put/delete/put_part/complete/abort/swap 全路径同步增减，complete 把选中分片的账从分片口径
重平衡到对象口径以保证删除后精确归零；压实判据改为"有可回收字节 **且** 存活率 ≤
`pack_gc_ratio`"（`duostore_backend.cc:1169-1191`），小对象 pack 100% 存活也低于阈值导致的
永久重写循环消除。(b) `IDataStore` 增 `stat_pack()`（fs 实现即一次 `stat`，
`data_store.h:123`），GC 判定前回填 `seal(0)` 遗留 pack 的 file_size（`:1180`），不支持的
引擎沿用顺扫回报兜底。

### 2.4 [✅已修复] tiered 的 per-key 锁临界区未切回池线程 → 阻塞 IO 跑在 HTTP 响应线程

**位置**：`src/storage/tiered/tiered_backend.cc:541-555`（`commit_cache_fill`）、`:406-416` 与 `:466-481`（demote）、`:714-716`（GC）、`:876-887`（reconcile）——四处 `acquire()` 之后都**没有** `co_await pool_->schedule()`。

**机制**：`AsyncSemaphore` 不传 executor 时在 release 的调用栈上就地 resume（`core/semaphore.h:19-21`）。localfs 正确处理了（`localfs_backend.cc:167-169` 紧跟一行 `co_await pool_->schedule()`），tiered 四处全漏。最严重的是 `commit_cache_fill`：它由 `TeeCacheReader::read()` 在客户端读到 EOF 时 `co_await`，整段 rename + 两次 fsync + sidecar 写入直接压在 HTTP 响应线程上。

**建议**：四处统一补 `co_await pool_->schedule()`；并给 `key_locks_` 传入池 executor，从构造处根除就地 resume。

**✅已修复**：`key_locks_` / `transfers_` 统一传入池 executor，从构造处根除就地 resume；五处
`acquire()` 之后补 `co_await pool_->schedule()`（`tiered_backend.cc:407`、`:417`、`:478`、
`:778`、`:967`），`commit_cache_fill` 的 rename + 两次 fsync + sidecar 写入不再压在 HTTP
响应线程上。

### 2.5 [✅已修复] beast 驱动出站头零校验 → 响应拆分注入面（四驱动中唯一）

**位置**：`src/http/drivers/beast/beast_server.cc:389` 与 `:414`（`for (auto& [k,v] : resp.headers.items()) res.insert(k, v);`）。对比 builtin/seastar 走 `common.h:166-188` 的 `header_emittable` + 保留头过滤，httplib 由上游 `is_field_value` 兜住。Beast 的 `try_create_new_element` 只校验长度，不校验 CR/LF。

**可控输入路径**：`src/s3/handlers/objects.cc:47` 把 `meta.content_type` 直接落头，`:50` 把 `x-amz-meta-<k>` 的用户元数据 key 与 value 直接落头。本网关入站会拒绝裸 CR，但元数据也可能来自 cloudproxy 从上游 S3 拉回的响应、或 duostore 的 redis/tikv/sqlite 元数据存储——这些路径不在 L1 过滤范围内。此外超长头会让 beast 抛异常逃到 `spawn_detached` 的 catch，结果是**连响应都不发直接断连**。

**建议**：把 `common.h` 的过滤提成 `emit_headers(bhttp::fields&, const HeaderMap&)`，beast 两处循环统一改用。

**✅已修复**：`common.h:179` 抽出 `emit_headers(headers, set)`——保留头过滤 + CR/LF 拒写，
`render_response_head` 同步改用；beast 两条写出路径（`beast_server.cc:401`、`:428`）统一走它，
四驱动出站头校验口径一致。回归用例：驱动套件新增 `/badheader` 头注入用例（四驱动共享）。

### 2.6 [✅已修复] 条件 PUT 的条带锁跨整个 body 上传 → 64 条连接即可堵死全网关条件写

**位置**：`src/s3/handlers/objects.cc:99-125`（`cond_lock` 在 `:104`/`:116` 取得，活到 `:125` 的 `put_object` 返回之后）；锁池 `src/s3/service.h:118-121`（64 条带）。

**机制**：临界区本应只覆盖 `head_object` → 提交这一小段 CAS 窗口，实际覆盖了整个流式上传。认证用户挑 64 个哈希到不同条带的 key，各发一个 `If-None-Match: *` 的 PUT 然后每秒喂 1 字节，即可让全网关条件写阻塞至超时。

**建议**：把"检查 + 提交"下沉为后端原子操作——localfs 用 `renameat2(RENAME_NOREPLACE)` 或在已有的 commit 阶段 per-key 锁内比对 ETag，duostore 用元数据 CAS；L2 的锁随之删除。这同时解决下面 3.6 的多实例问题。

**✅已修复**：`IStorageBackend::put_object` 增 `PutCondition` 形参，"检查 + 提交"下沉到各后端
自身的原子提交点——localfs/xlocalfs/memory 在 per-key commit 锁内、duostore 在四引擎的原子区
内（rocks/sqlite 锁+事务、redis CAS 批、tikv 乐观事务，共享 `meta_util.h` 的检查）、tiered 透传
local（stub sidecar 为权威）、cloudproxy 把 `If-None-Match`/`If-Match` 透传上游并补 412 兜底
映射。`service.h` 的 64 条带锁删除，L2 只保留无锁 head 预检做快速失败（`objects.cc:97-101`）。
**这同时解决了 §3.6 的多实例原子性问题。** 回归用例：backend_suite 增条件 PUT 六断言（全后端
共享，cloudproxy 走真实转发链路）。

### 2.7 [✅已修复] localfs LIST 无 prefix/delimiter 剪枝（文档声称有）

**位置**：`src/storage/localfs/localfs_backend.cc:250-277`——不论 prefix/delimiter 是什么，一律 `recursive_directory_iterator` 全桶遍历 + 全部 key 收进 vector + 全排序。而 `docs/storage-backend.md:148-150` 明确声称"prefix 剪枝（prefix 含 `/` 时直接定位起始目录）；delimiter=`/` 时目录即 common prefix，无需展开其内部"。

**后果**：`?max-keys=1&prefix=a/b/c` 在 1000 万对象的桶上要 stat 1000 万次、构造 ~1GB 字符串、做一次 O(n log n) 排序，只为返回 1 个 key。并发 LIST 直接打爆 IO 池与内存。

**建议**：prefix 取最后一个 `/` 之前的部分作为起始目录；delimiter=`/` 时改用非递归 `directory_iterator` 并 `disable_recursion_pending()`；`apply_listing` 改为收满 `max_keys+1` 即停的游标式。

**✅已修复**：`list_objects` 改为有序 `directory_iterator` 遍历 + prefix/delimiter/start_after
三重子树剪枝 + 游标式截断（`localfs_backend.cc:276-380`）：子树与 prefix 不相交或整体 ≤
start_after 即不下钻，delimiter 组命中后整组跳过，收满即停。截断边界落在 common prefix 上时用
`max_key_with_prefix` 求组尾以保持 `next_token` 语义。差分测试对照全量实现验证语义等价。

### 2.8 [✅已修复] upload_part 分片数据不 fsync，但 `.md5` sidecar fsync 了

**位置**：`src/storage/localfs/localfs_backend.cc:334-342`（xlocalfs `:200-208` 同构）。`complete_multipart`（`:369-382`）**信任 `.md5` 不重算校验和**。

**后果**：「上传分片 → 掉电 → 重启 → complete」会产出一个 ETag 完全合法、内容却是零块的对象，且后续所有校验都因 ETag 是 `-N` 形式而被跳过。

**建议**：`upload_part` 在 close 前 `fsync_file`，rename 后 `fsync_dir`；顺序改为"先 fsync 数据 → rename 数据 → 再写 .md5"，使 `.md5` 的出现成为数据已持久的证据。或直接把 md5 写进分片文件 xattr，复用 `set_meta_xattr`。

**✅已修复**：localfs 与 xlocalfs 的 `upload_part` 改为"close 前 `fsync_file` 数据 → rename →
`fsync_dir` → 最后写 `.md5`"（`localfs_backend.cc:511-532`），`.md5` 的出现即分片数据已持久
——`complete_multipart` 信任 `.md5` 不重算校验和的前提由此顺序保证。

### 2.9 [✅已修复] io_uring 收割线程可静默死亡；shutdown 不排空在途

**位置**：`src/storage/xlocalfs/uring.cc:181-183`——`io_uring_enter` 返回非 EINTR/EAGAIN/EBUSY 的任何 errno 都让收割线程**无声退出**：不记日志、不置错误态、不唤醒任何在途 Op。此后所有 `co_await eng_->read/write` 永不 resume（GET 挂死、连接不释放、帧泄漏），而 `submit()` 仍接受新提交。

`shutdown()`（`:186-194`）只投一个 NOP 哨兵，CQE 顺序不保证它排在在途读写之后；随后 `~UringEngine` munmap 掉 ring 并 close fd，而内核可能仍持有指向 `UringBodyReader` 缓冲区的引用。`Op` 内联在协程帧内（`uring.h:38-53`），帧在 IO 在途时被销毁即 UAF。

**建议**：失败退出前 LOG_ERROR + 置 failed 标志 + 唤醒全部在途 Op（`res = -EIO`）；加 `inflight_` 计数，shutdown 先等归零（带超时告警）再投哨兵；`Op` 改堆分配由 engine 持有，帧销毁时只标记 orphan。

**✅已修复**：收割线程遇不可恢复 errno 不再无声退出——置 `failed_` 拒新提交、把全部在途 Op
以 `-EIO` 唤醒并记 ERROR（`uring.cc:202-220`）；新增 `inflight_` 登记与条件变量，`shutdown()`
先等在途 CQE 排空（10s 超时告警，`:234-241`）再投哨兵，避免 munmap/close 之后内核继续写用户
缓冲区。

### 2.10 [✅已修复] builtin/httplib 的 `BodyReader::read()` 阻塞在共享池线程上

**位置**：`src/http/drivers/builtin/builtin_server.cc:177-179` → 裸 `::recv`；`src/http/pushpull.h:93-98` → `cv_pop_.wait`。而 `main.cc:76-81` 使整条请求协程链跑在 `ThreadPool`（默认 16 线程）上。

**后果**：16 个慢速上传的客户端就能把 16 个池线程全部占死，整个进程（含所有其他请求与后端 flush）停摆——而连接线程此时正闲坐在 `sync_wait` 里什么也不干，线程被用反了。beast/seastar 无此问题（真正挂起）。

**建议**：给这两个驱动的 reader 一个"请求线程 executor"，`read()` 先切回连接自己的线程再阻塞；或把 `runtime.io_threads` 与这两个驱动的并发上限做联动校验并在文档写明限制。

**✅已修复**：新增 `PumpExecutor`（`core/executor.h:29`）与 `sync_wait_pumping`
（`core/task.h:234`）——请求线程不再傻等在 `sync_wait` 里，而是边等边运行本请求的任务队列；
`SocketBodyReader` / `QueueBodyReader` 把阻塞 `recv` 与 cv pop 经 `resume_on` 切回连接自己的
线程（`pushpull.h:92-100`）。16 个慢速上传占死全部池线程的通路消除。TSan 抓出首版"锁外
notify"的析构竞态，已改为锁内 notify。回归用例：test_task 增 PumpExecutor 两用例。

### 2.11 [✅已修复] gcq 批量按条数而非 extent 数；单条 Reclaim 可达数十 MB

**位置**：`duostore_backend.cc:965`（`kGcBatch = 256`）、`:1021`；`fs_data_store.cc:482-489`（`remove` 对 N 个 extent 顺序 unlink，全程不让出池线程）。

**机制**：删除一个 5TiB 对象（65 万 extent）产生**一条** gcq 项。一批 256 条最坏内存驻留 GB 级；随后 `remove` 在单个池线程上连续 unlink 65 万次而不 `co_await`，把该线程占死数分钟。

**建议**：`peek_reclaims` 增加"累计 extent 数上限"；`enqueue_reclaim` 对超阈值 DataRef 拆多条（ack 丢失本就无害）；`remove` 每 N 个 extent 让出一次线程。

**✅已修复**：`enqueue_reclaim` 按 `kReclaimMaxExtents = 4096` 把超大 DataRef 拆成多条（四引擎
统一；ack 独立、unlink 幂等，崩溃语义不变）；`peek_reclaims` 增累计 extent 上限
（`kGcBatchExtents = 32768`，至少返回 1 项以兜住拆分前的旧账，`duostore_backend.cc:1052-1123`）；
`FsDataStore::remove` 每 1024 次 unlink 让出一次池线程。回归用例：meta 套件新增 gcq 拆分/封顶
用例（四引擎共享）。

### 2.12 [✅已修复] TiKV 无单值/单事务体积保护

**位置**：`codec.cc:307-318`（`encode_object` 无体积上限）、`tikv_meta_store.cc:509`（整份 manifest 作为单个 mutation value）、`:736-749`（万分片 complete 生成 3 万+ mutation）、`tikv_client.cc:213-215`（16KiB 批**串行**发送）。

**后果**：约 1.3TiB 以上对象的 manifest 突破 raft entry 上限 → PUT 永久失败；已存在的这类对象其 gcq 项同样写不进去 → **删不掉**。万分片 complete 的数百次串行 RPC 还可能超出 20s 的 lock TTL 被判死回滚。

**建议**：manifest 分片存储或对 extent 数设硬上限（超限抛 `EntityTooLarge`）；`TikvClient::commit` 增加 mutation 数/字节的 fail-fast 校验。

**✅已修复**：meta 层 fail-fast——`encode_object` 之后校验 manifest 单值 ≤ 6MiB（raft entry
8MiB 默认值留余量）且 extent ≤ 20 万，超限抛 `EntityTooLarge`（400，可操作：客户端换 multipart
或加大分片），替代此前在 prewrite 反复撞 raft 上限的永久 500；client 层最后防线——
`TikvClient::commit` 在任何 RPC 之前校验单值 ≤ 6MiB、总量 ≤ 96MiB、条数 ≤ 30 万，超限在
prewrite 之前抛出（语义上明确未提交）。回归用例：`duostore_tikv_object_manifest_size_guard`
（20 万+ extent 抛 400 且未落写），tiup playground 真集群实测。

### 2.13 [✅已修复] 其余高危（简列）

> 本表 13 行**已于 2026-08-06 全部修复**，末列为实际修法。

| 位置 | 问题 | ✅ 实际修法 |
| --- | --- | --- |
| `src/s3/service.cc:108-113` | Host 后缀匹配**大小写敏感** → `Host: b.GW.EXAMPLE.COM` 静默降级为 path-style，同一 URL 在两种大小写下指向不同资源，且 policy 判定的输入被攻击者控制 | `resolve_address` 与 `base_domain_` 统一转小写（RFC 4343，`service.cc:98-113` / `service.h:41`）；vhost 用例按新语义更新为"同一 URL 两种大小写指向同一资源" |
| `src/core/util/uri.cc:31-34` | `percent_decode` 无条件把 `+` 解成空格并用于 **path** 与 copy-source → `PUT /b/a+b.txt` 静默存成 `a b.txt`；canonical query 侧还会把字面 `+` 重编码成 `%20` 造成 SignatureDoesNotMatch | 已拆分：`percent_decode` 不再动 `+`（path / copy-source / canonical query 全用它），新增 `percent_decode_query` 供 query 参数值（`uri.h:12-15`） |
| `src/http/drivers/beast/beast_server.cc:436-463` | beast 缺定长流式响应的字节数对账（另三驱动都有）→ 后端流提前 EOF 时连接被复用，客户端把下个响应的状态行当作 body 剩余部分，**响应错位** | 按建议实现：累加 `written`，overrun 与 short 均 LOG_ERROR + 断连（`beast_server.cc:453-475`） |
| `src/http/drivers/beast/beast_server.cc:253` vs `:471` | `acceptor_`/`grace_timer_` 未绑 strand，与停机路径在两个线程上并发访问（asio I/O 对象非线程安全） | 新增 `ctl_strand_`，acceptor / stop_event / grace / force 定时器全部挂它，停机编排经 `asio::post(ctl_strand_, …)`（`beast_server.cc:200-214`、`:519-558`） |
| beast `:251-273` / seastar `:657-685` | 无并发连接上限（builtin 有 4096、httplib 有隐式约束）——beast 是"性能路径首选"却最缺自保 | `HttpConfig` 增 `max_connections`（默认 4096，配置可调）：beast 按会话数拒绝、seastar 按 shard 均摊、builtin 改用配置值，四驱动统一 |
| `src/http/drivers/httplib/httplib_server.cc:32-46` | 未注册 `set_expect_100_continue_handler` → 上游在**调用业务 handler 之前**无条件回 100，与文档要求的延迟应答相反：无效签名的 5GB PUT 会被完整收下再拒绝 | 已注册 `set_expect_100_continue_handler`（`:54`），消息边界违规在邀请上传前即 400 关连接。**残留限制**：cpp-httplib v0.20 的 API 只有"立即 100 / 417 / 最终响应"三种出路，无法表达"抑制自动应答、延迟到首次 pop"，因此签名无效的大 PUT 仍会被邀请上传，由 4MiB 有界排空兜住；代码处已注明 |
| `src/http/drivers/httplib/httplib_server.cc:85` | 缺"shutdown 早于 run"的补偿（另三驱动都有）→ 该顺序下 `run()` 永不返回 | 加 `stopping_` 原子位，`run()` 入口检查（`:103`）；`stop()` 后补 `decommission()`（顺序敏感：`stop()` 会复位 decommission 标志，`:110-116`） |
| `src/core/background.cc:35-43` | `spawn` 先 `++count_` 再 `run_detached`，帧分配失败时计数泄漏 → 后续 `wait_idle()`（无超时无日志）永久阻塞，而它就在 `~TieredBackend` 里 | 按建议实现：帧分配失败时回补计数后重抛（`background.cc:43`），`wait_idle()` 不再被泄漏的计数永久阻塞 |
| `src/core/semaphore.h:103-107` | `AsyncSemaphore` 析构不处理 `waiters_`（既不 resume 也不 destroy → 帧泄漏 + sync_wait 线程永久阻塞）；`Permit` 持裸指针 | 析构断言 `waiters_` 为空，违反时记 ERROR 并保留帧（泄漏是保守方向，`semaphore.h:32-34`）。带取消语义的 `close()` 留到 §3.1 取消体系接线时一并做 |
| `src/storage/tiered/tiered_backend.cc:561-644` | scan/reconcile 把整个实例的对象全量物化进内存，并一次性构造 N 个协程帧（`transfers_` 只限并发执行数，不限帧数）；1000 万对象约 1.5–2GB | `scan_once` 改两遍扫描：第一遍流式判冷 + 崩溃恢复（分批 `co_await`，帧数有界），第二遍仅在超水位时收集候选，multiset 只保留恰好覆盖回收目标的最优前缀；`reconcile` 改本地/云端双游标有序合并（各 1000/页），正反两向对账在 O(页) 内存内完成 |
| `duostore_backend.cc:146-158` + `fs_data_store.cc:447-457` | 压实每条 record 一次 fdatasync + 一次 meta 提交，与业务写抢同一把写锁（128MiB pack ≈ 1000 次） | `rewrite_pack` 攒批（64 条 / 4MiB）交付批量迁移；数据侧 `write_batch` 单槽锁 + 单 fdatasync；meta 侧新增 `swap_extents_batch`（逐项 CAS 独立），rocks/sqlite 覆写为单批/单事务提交，redis/tikv 保持逐条（单条即一次 RTT，合并会让单项 CAS 失败殃及整批） |
| `duostore_backend.cc:134` + 四引擎 `swap_extents` | 同一对象在同一 pack 有多条 record 时，压实退化为 O(n²) 的整份 manifest 重写（redis 更差，还要把整份旧值当 CAS 见证再传一遍） | 批内按 owner 聚合——同一对象的多条 record 一次 `get_object` + 一次换 ref，O(n²) 的整份 manifest 重写消除 |
| `duostore_backend.cc:1019-1021` | GC 每轮从 seq 0 重扫全部不可回收项（断点续扫只在单轮内有效），backlog 下 CPU/内存开销线性增长且永不下降 | 跳过项记录最早 seq 与最早可重试时刻（grace 项取 `enqueue+grace` 的确定性下界；pin/删除失败取下一轮），全部未到期的轮次从上轮高位起扫（`duostore_backend.cc:1108-1162`）。纯内存优化，重启退回全量扫 |

---

## 3. 中

### 3.1 [✅已修复] 取消体系整体是死代码

`docs/concurrency.md §5` 声称取消源有三（客户端断连、请求超时、进程 shutdown），实际一个都没接线：`src/s3/service.cc:123` 的 `RequestContext.cancel` 恒为默认"永不取消" token 且 `ctx` 不传给 route/handler/后端；40+ 处 `pool_->schedule()` 无一传 token；`with_timeout`（`task.h:361`）零生产调用点。

关联问题：(a) `AsyncSemaphore::acquire()` **没有任何取消入口**，而 `max_inflight_requests` 排队正是请求最可能长时间挂起的地方——即便接上 `with_timeout` 也打不断它；(b) `thread_pool.cc:93-126` 整套取消竞态机制（Slot 共享块、认领、注册后补检）永远走不到，却每次调度都实打实付出一次 `make_shared` 的代价。

**建议**：要么接线（dispatch 建 per-request `CancelSource`，driver 断连回调 + `with_timeout` + shutdown 广播三处触发，并给 `acquire` 加取消支持），要么承认不做并删掉半条命的取消分支。**接线前必须先修 3.2**，否则会引入新的故障模式。

**✅已修复**（2026-08-07）：选择接线。Task 的 promise 增 cancel token 沿 co_await 链
向下继承（与 cont_executor 同机制），`ThreadPool::schedule` / `AsyncSemaphore::acquire`
的 await_suspend 模板化后从调用方 promise 取 token——存量 40+ 处 `co_await
pool_->schedule()` 一处未改即获得请求级取消。dispatch 建 per-request `CancelSource`
并把外部 token（进程关停广播）接到同一个源；`AsyncSemaphore` 补取消入口（等待者
摘链 + `OperationCancelled` 唤醒）与 `close()`。挂起点取消统一映射 503 SlowDown。

### 3.2 [✅已修复] 取消回调在触发线程上跑完整条请求续体

`src/core/cancel.h:33` 锁外逐个 `fn()`；`src/core/thread_pool.cc:104-107` 的回调直接 `s->h.resume()`。取消源是 `TimerQueue`（`with_timeout` 的唯一形态）时，这条 resume 会在**单线程的定时器线程上**把整条 L2/L3 续体跑完，期间全进程所有定时器停摆。而 `core/timer.h:25` 明确写着"fn 须轻量"。

**建议**：取消回调改为 `pool.post(...)` 而非直接 resume。

**✅已修复**（2026-08-07）：`TimerQueue` 拆成"调度线程 + 专用回调线程"，到期判定
与回调执行分离——慢回调不再停摆全进程定时器。注意原建议的"改 `pool.post`"按字面
实现会死锁：取消最需要生效的场景正是池被占满，续体排在阻塞任务后面永远轮不上
（test_concurrency 既有用例即刻复现），故保留就地 resume（恢复即抛，只做有界的
异常展开），把防线放在触发侧。

### 3.3 [✅已修复] 无任何请求级超时；`idle_timeout` 覆盖不到 handler 执行期

四驱动的超时都只管 socket 系统调用（builtin 的 `SO_RCVTIMEO`、beast 读头后 `expires_never()`、seastar 的 ArmGuard、httplib 的读写超时），`co_await handler_` 期间无 timer。一个 hang 住的存储调用会永久占住：builtin 的一个连接线程 + 一个池线程；beast/seastar 的一个会话；httplib 的一个池线程 + 一个 pump 线程。配置里也没有对应旋钮。

另：流式响应的分块超时是**逐块重置**的，每 59 秒发 1 字节的客户端可无限期占住连接——四驱动一致。

**建议**：`HttpConfig` 加 `request_timeout_sec` 与传输停滞上限；用 `with_timeout` 包 handler 并与 3.1 的取消打通。

**✅已修复**（2026-08-08）：按建议实施。`HttpConfig` 增 `request_timeout`（默认 300s）
与 `transfer_stall_timeout`（默认 300s，0=关）；dispatch 用 `with_timeout` 包 route
并与 §3.1 的取消源汇合，超时映射 503。传输停滞由 `http/stall_guard.h` 的
`StallGuardReader` 治：按"每窗口至少推进 64KiB"判定，装在 L1/L2 交界处请求体/响应体
各包一层，四驱动一次生效——真慢但在传的连接不受影响，每 59 秒滴 1 字节的被掐断。

### 3.4 [✅已修复] SSE / tagging / object-lock / ACL 的**请求头**被静默吞掉

`src/s3/service.cc:75-90` 只按 query 子资源拒绝，`src/s3/handlers/common.h:25-34` 的元数据提取只认 `Content-Type` 与 `x-amz-meta-*`。于是 `x-amz-server-side-encryption`、SSE-C 的三个头、`x-amz-tagging`、`x-amz-object-lock-*`、`x-amz-acl: public-read` 全部返回 200 但语义未兑现。`docs/s3-protocol.md:15-17` 承诺这些返回 `NotImplemented`——只对子资源做到了，对头没有。

**静默接受比报错危险得多**：合规场景下客户端会据此认为对象已加密/已锁定。

**建议**：增加 `kUnsupportedHeaders` 表，命中即 501；`x-amz-acl` 单独放行 `private`。

**✅已修复**（2026-08-08）：按建议实施（`service.cc` 的 `reject_unsupported_headers`，
route 入口统一执行）。前缀表覆盖 `x-amz-server-side-encryption*`（含 SSE-C、KMS）、
`x-amz-copy-source-server-side-encryption*`、`x-amz-object-lock-*`、`x-amz-grant-*`；
精确表覆盖 `x-amz-tagging`、`x-amz-website-redirect-location`；`x-amz-acl` 仅放行
`private`（即本实现的实际语义），其余值 501。

### 3.5 [✅已修复] 分派表的黑名单兜底 → 未列入名单的子资源变成静默误答

`src/s3/service.cc:212-287` 以 `flag == ""` 兜底，`reject_unsupported_subresource` 是唯一防线且是**黑名单**。确切漏项：`?attributes`（GetObjectAttributes）→ 返回整个对象体而非 XML；`GET ?partNumber=1` → 返回整个对象而非第 1 片；`response-*` 六个参数 → 静默忽略。

黑名单模型的问题是结构性的：任何遗漏都默认降级为"读整对象/写整对象"，而 501 至少是诚实的。

**建议**：反转为白名单——枚举每个 (method, scope) 下允许出现的 query key，出现未知 key 即 501。一处改动同时消掉全部漏项且不会再漂移。

**✅已修复**（2026-08-08）：按建议反转。`Route` 增 `extra_query` 字段逐路由枚举允许的
query key（flag 键与 presigned 签名参数族、SDK 的 `x-id` 全局放行），命中路由后名单外
一律 501——`?attributes`、`GET ?partNumber`、`response-*` 三类漏项一次消掉。两点校准：
分页参数（`max-parts`/`part-number-marker`/`max-uploads`/`key-marker`/`upload-id-marker`）
允许但忽略——handler 一次回全量且 `IsTruncated=false` 是完整的答案（cloudproxy 后端
自己就发这些参数，拒收会破坏自转发）；uploads 列表的 `prefix`/`delimiter` 则不放行，
忽略它们会把过滤范围外的结果混进来。黑名单保留仅为给已知子资源更明确的报错文案。

### 3.6 [✅已修复] 条件写在多实例部署下无原子性，与文档承诺不符

`docs/s3-protocol.md:126-130` 把 If-Match / If-None-Match 列为"对客户端的承诺"，但判定发生在网关内存里。两个实例并发处理同 key 的 `If-None-Match: *` 时各自 head 都得到 NoSuchKey，两者都 put。而 `docs/credential-management.md §10.3` 刚为多实例补齐了凭证同步——项目已把多实例当作支持形态，这条限制不再是理论问题。

**建议**：短期在文档明写限制并在 `auth.sync_interval > 0` 时启动打 WARN；中期按 2.6 下推到后端。

**✅已随 §2.6 修复**（2026-08-06）：条件判定已下推为后端原子操作（`PutCondition` 契约），
localfs/xlocalfs/memory 在 per-key commit 锁内、duostore 在四引擎的原子区内完成检查，
共享同一份元数据的多实例因此天然互斥；cloudproxy 把条件透传上游由其保证。L2 只剩无锁 head
预检做快速失败。

### 3.7 [✅已修复] 在途吊销竞态时 policy 被整体跳过（fail-open 方向反了）

`src/s3/auth/credential_store.cc:361`：`if (it == creds_.end()) return;`。若凭证在验签（`secret_for`）与 `authorize` 之间被 `remove()`/`sync_now()` 删除，结果不是"按原 policy 继续"，而是 **policy 完全消失**——一个 `readonly=true` 的凭证在这个窗口里变成不受限凭证。`sync_now` 每个同步周期都会批量删除，窗口可被重放撞上。

**建议**：让 `verify()` 返回验签那一刻的 `CredentialInfo` 快照，`dispatch` 直接用它 authorize——既修竞态又真正兑现"在途请求按验签时语义完成"。

**✅已修复**（2026-08-08）：按建议实施。`ICredentialProvider::secret_for` 升级为
`lookup`——一次查表同时带出 SK 与 policy 快照；`verify()` 返回
`VerifiedIdentity{access_key, policy}`，dispatch 用快照 authorize、不再回 store 二次
查表。在途请求严格按验签时刻语义完成（readonly 仍拒写），吊销后的新请求因查不到 AK
走 InvalidAccessKeyId（fail-closed）。`CredentialPolicy` 拆到 `s3/auth/policy.h`
（sigv4.h 需要它，留在 credential_store.h 会成环）。回归用例：
credstore_policy_snapshot_survives_revocation。

### 3.8 [✅已修复] `/-/` 内部端点在 vhost 模式下遮蔽合法 key 且匿名可达

`src/s3/service.cc:133-146` 四个内部端点用 `req.path` 精确比较且在 `resolve_address` **之前**。vhost 下 `req.path` 是 key 而非 `/bucket/key`，于是 `GET /-/metrics` with `Host: mybucket.gw.example.com` 命中匿名 metrics 端点；`PUT /-/metrics` 同样命中（不区分 method）→ 返回 200 但对象根本没写，**静默丢数据**。

**建议**：把 `resolve_address` 提到内部端点分流之前，仅当 bucket 为空才进入 `/-/` 分支；或把内部端点挪到独立监听端口。

**✅已修复**（2026-08-08）：取第一方案变体——`resolve_address` 提前并返回 vhost 标记，
只有 **path 寻址**（非 vhost）下的 `/-/` 前缀才进内部分支；vhost 下 `/-/metrics` 按
普通对象键读写。同时补 method 区分：三个读端点只认 GET/HEAD（探活器常用 HEAD），
其余方法 405——`PUT /-/metrics` 不再"200 且静默丢数据"。

### 3.9 [✅已修复] 其余中危（简列）

| 位置 | 问题 |
| --- | --- |
| `src/main.cc:92-109` | 关停序假定 `run()` 返回即无在途请求（实为超时强退）；`all_backends.clear()` 不生效（router 仍持 shared_ptr，真正析构在 `join()` 之后）；启动后期失败直接 `return 1` 跳过全部 `close()` |
| `src/core/config.cc:272-292` | `io_threads` 只校验 `>=1` **无上界**（per-backend 的同名参数却有 `[1,1024]`）；backend `name` 未查重；`to_int` 用 `stoi`，非法值报无上下文的 `stoi`；注释剥离用 `find(" #")` 会截断含该串的密钥；未定义的 `${VAR}` 静默展开为空 |
| `src/core/cancel.h:99-103` | `CancelRegistration::reset()` 不等待在途回调（`request_cancel` 先 swap 再锁外执行），与 `TimerQueue::cancel` 提供的相反保证同库共存且未写明 |
| `src/core/background.cc:30` | `on_done()` 早于 detached 帧完全销毁——与已知的 ramp 竞态同源，应一并修 |
| `src/main.cc:19-23` | 信号处理器读写裸指针 `g_server`（应为 `sig_atomic_t`/atomic）；httplib 的 `shutdown()` 实为 `svr_.stop()`，内部取锁 → 信号落在持锁线程上即死锁；`sigaction` 未 `sigemptyset` |
| `src/http/model.h:20-51` | `HeaderMap` 无 `remove()`、无多值取值（`parse_body_framing` 自己绕开 `get()` 遍历 items 正说明缺口）；`get()` 按值拷贝，`has()` 拷一次只为判空——L1/L2 每请求十几次 |
| builtin `:284-287` / seastar `:585-588` | `Connection` 头用**全等**比较 `close`，`Connection: close, Upgrade` 这类合法 token 列表被漏判 |
| httplib `:32-46` / `:54-59` | 忽略 `max_header_size`（上游硬编码 8KiB）；只注册 6 个方法，未知方法由上游直接回非 S3 XML —— 两处都破坏"四驱动接受/拒绝集合一致" |
| `common.h:192-194` vs beast `:394-398` vs httplib `:191-196` | HEAD + 无长度流式响应在四驱动间行为分裂（chunked vs Content-Length） |
| builtin `:364,371` / seastar `:745,766` | 只支持 IPv4 绑定，`bind: "::"` 在默认驱动下直接抛错，beast/httplib 却正常 |
| `src/storage/tiered/tiered_backend.cc:198-203` | `parse_pct("1%")` 返回 100%（`%` 后缀被丢弃后不参与判定）→ 低水位 100% 时 `(used-low)` 为负、转 uint64 回绕 → 把整个桶全部下沉 |
| `src/storage/registry.cc:180-182` | 回滚守卫**漏登记 tiered 的 metrics scope** → tiered 构建失败时 4 个 gauge 回调残留并持有 ThreadPool 的 shared_ptr，线程永不 join（正是该守卫想防的场景） |
| `src/storage/localfs/localfs_backend.cc:94-101` | `delete_bucket` 用抛异常重载，并发下可留下"marker 已删、目录仍在"的不可见数据；`fs::remove` 抛的 `filesystem_error` 不是 S3Error → 500 |
| `src/storage/localfs/fs_util.cc:149-160` | `setxattr` 返回值**完全不检查** → 超限时静默退回旧一致性模型且无日志无指标；`getxattr` 固定 8KiB 缓冲，ERANGE 时静默回落 |
| `src/storage/localfs/fs_util.cc:165-168` | `create_directories` 的所有失败（含 ENOSPC/EACCES/EIO）都映射成 400 "key 冲突"，客户端不会重试、运维拿不到磁盘满信号 |
| `src/storage/multipart.cc:13-21` | upload_id 用 `mt19937_64`（可由约 2496 个输出还原状态）且种子只有 32 位熵 —— upload_id 直接返回给客户端，可预测即可中止/篡改他人上传 |
| `src/storage/memory/memory_backend.cc:82-97` | 持全局锁做整对象拷贝（1GB 对象 = 2GB 拷贝 + 全程锁住所有 bucket）；整个后端无 `pool_->schedule()`，互斥锁直接锁在事件循环线程上；`list_objects` 用非 const `operator[]` 会静默插入空对象 |
| `redis_meta_store.cc:83-89` / `:134-168` | Lua 脚本内 O(N) 循环（万分片 complete = 1 万次 HGET；list 1000 key = 2000 次调用），脚本原子执行期间**独占整个 Redis** |
| `redis_meta_store.cc:729` 等 | 把整份旧对象值作为 CAS 见证上送（大 manifest 网络与内存翻倍，重试时每轮重发） |
| `sqlite_meta_store.cc:520-555` | 号段连接与业务写连接争库级写锁，BUSY 重试最坏 20 秒后抛 500，把正常 PUT 变成失败 |
| 四引擎 `alloc_id` | 号段逐个派发使并发写者的 file_id 交错 → run 编码失效，编码后反而比平铺**膨胀 28%**，并放大 manifest 相关的全部问题 |
| `duostore_backend.cc:866` + 四引擎 | HEAD/GET 前置读走全量 `decode_object`（65 万 extent 对象要物化 26MB vector 后立刻丢弃），而 `decode_object_meta` 已存在却只被 list 使用 |
| `duostore_backend.cc:1154-1166` | 孤儿扫描把 refs + on_disk + disk 三份全量集合同时驻留内存（1 亿 chunk ≈ 4–5GB） |
| `fs_data_store.cc:465-470` | pack 封存回调在 slot 互斥内执行 meta 提交（网络 RTT / fsync 堵死该槽）；`seal_` 抛出时 fd 已置 -1 而 size 未清 → 该 pack **从此永远不会被封存** |
| `rados_data_store.cc:387-397` | 读侧 aio 直接写调用方缓冲且无在途守卫（写侧刻意做了缓冲移交），一旦引入读超时/取消即 UAF |
| `tikv_meta_store.cc:391,414-421` | 内层号段事务的"结果不明"被翻译成外层业务提交的 `UndeterminedCommit`，而业务事务明确从未提交 → 拒绝清理、制造无谓孤儿 |
| `src/s3/handlers/objects.cc:246-295` | DeleteObjects：空列表与缺 Key 不报 MalformedXML；1000 个 key **串行** `co_await`（cloudproxy/duostore 上即 1000 次串行 RTT）；请求里的 `<VersionId>` 被静默忽略 → "删指定版本"变成"删当前对象" |
| `src/s3/errors.cc:26-30` | 未知远端 wire code 折成 500 → SDK 对 500 自动重试，一个 `InvalidObjectState`(403) 会被无限重试；原文还被 `public_error` 抹掉 |

**✅全部已修复**（2026-08-07/08，按表序逐行）：

1. **main.cc 关停序**：改为"广播取消 → 等许可归位（10s 超时告警）→ close → 按持有
   关系反序释放 → join"；启动后期失败的异常路径走同一份 close，不再跳过 active pack 封存。
2. **config.cc**：`io_threads` 上界 [1,1024] 与 per-backend 取齐；backend name 查重；
   `to_int` 报错带键名与原值并拒尾随垃圾；注释剥离改引号感知；未定义 `${VAR}` 改报错
   （可选值写 `${VAR:-默认}`）。
3. **cancel.h**：`CancelRegistration::reset()` 等待在途回调批次跑完（触发线程上自注销
   不等待，防自死锁），与 `TimerQueue::cancel` 语义对齐。
4. **background.cc**：被等待任务的协程帧移入内层作用域，析构先于 `done()`。
5. **main.cc 信号**：改 self-pipe + 守护线程（httplib 的 shutdown 内部取锁，信号落在
   持锁线程上即自死锁）；`sigemptyset` 补齐。
6. **HeaderMap**：补 `find()`（免拷贝，has/get 改走它）、`get_all`/`count`（多值头）、
   `remove()`、`has_token()`（列表头）。
7. **Connection 列表头**：builtin/seastar/httplib 统一改 `has_token` 按 token 判定；
   httplib 上游不认列表时补发 `Connection: close` 响应头。
8. **httplib 两行**：`max_header_size` 补整体校验（超限 431 + S3 XML；上游单行 8KiB
   编译期上限启动时告警说明）；`set_error_handler` 把上游自产报文统一翻译成 S3 XML
   （路由 404 → 405 MethodNotAllowed，与另三驱动对齐）。
9. **HEAD 无长度**：统一为 `head_length_known` 契约——长度已知写真值，未知则两个框架
   头都不写并关连接（写 0 是撒谎，写 chunked 却不发帧也是）。
10. **IPv4-only**：builtin 改 `sockaddr_storage` 按字面量选族（v6 关 V6ONLY 对齐另两
    驱动），seastar 改 `inet_address` 并同步修 `probe_free_port`。
11. **tiered parse_pct**：`%` 后缀参与判定（"1%" = 0.01），加 (0,100%] 越界启动报错——
    (used-low) 回绕整桶下沉的通路消除。
12. **registry 回滚守卫**：tiered 的 metrics scope 先登记再构造，构建失败不再残留
    持池 gauge 回调。
13. **localfs delete_bucket**：改 error_code 重载；目录删除失败（并发写赢了竞态）时
    恢复 marker 报 BucketNotEmpty，不再留下不可见数据。
14. **fs_util xattr**：setxattr 失败告警（同类 errno 只报一次）——降级 sidecar-only
    不再无声；getxattr ERANGE 按实际大小重取，不静默回落旧元数据。
15. **create_directories**：只有"前缀撞既有文件"保持 400，ENOSPC/EACCES/EIO 改 500。
16. **upload_id**：`mt19937_64` 改 `getentropy`（CSPRNG）。
17. **memory 后端**：数据块改 `shared_ptr<const string>`，GET 锁内只取指针锁外流式
    吐出（put 覆盖时旧块由在途读者持有，天然快照）；`operator[]` 改 `at()`；剩余
    临界区皆 O(map 操作)，不接线程池为有意取舍（注释注明）。
18. **redis Lua O(N)**：parts 指纹检查改一次 HGETALL + Lua 内排序拼接（指纹值不变）；
    list 脚本改 200/页分页取 key + 500/批 HMGET——1000 key 从 2000 次调用降到约 10 次，
    原子性与翻页语义不变。
19. **redis CAS 见证**：`expect_eq` 改上送 40 字节 SHA1 指纹，比对在脚本内对存量值做
    `redis.sha1hex`。
20. **sqlite 号段**：预留期间持 `mu_`——进程内写者被确定性挡开（单进程 flock 保证无
    外部写者），不再与业务事务抽签；有界重试保留给绕过 flock 的外部裸连接。
21. **alloc_id 交错**：`IMetaStore` 升级为 `alloc_file_run(kind, n)` 批派发连续区间，
    四引擎统一"不足即换段、残段弃置"；fs/rados 写者按 1、2、4…封顶 64 的几何 run
    批取——并发写下 run 编码恢复有效。
22. **HEAD 全量解码**：`IMetaStore` 增 `head_object`（同一 raw 读 + `decode_object_meta`），
    四引擎实现；backend 的 HEAD 不再物化整份 manifest。
23. **孤儿扫描内存**：refs 改有序向量（8B/项）+ 盘面流式判定 + 命中位图（1bit/项）
    做反向对账，三份全量哈希集合（4–5GB）降一个数量级；全部既有不变式保留。
24. **pack 封存**：拆"锁内 `close_slot_locked`（关 fd/清槽/入重试队列）+ 锁外
    `flush_seals` 补交 meta"；seal 失败进队列由后续写入/close 重试，append 路径只
    告警、close 路径上抛——"永不封存"与"堵死槽"两个问题同时消除。
25. **rados 读侧**：改缓冲移交（与写侧同策略）——aio 写入在途单自有缓冲，完成后拷给
    调用方，取消/超时不再有 UAF 通路。
26. **tikv 号段误译**：内层计数器事务的 UndeterminedCommit 降为确定性 InternalError
    （号段结果不明只可能烧 id，外层业务事务确定未提交），不再误触发"拒绝清理"。
27. **DeleteObjects**：空列表/缺 Key 整批 MalformedXML；`<VersionId>` 出现即 501；
    删除改 32 一批的有界并发 `when_all`，单 key 失败收敛为结果值不中断整批。
28. **未知远端 4xx**：映射本地 400 InvalidRequest（不经 `public_error` 抹文案），
    远端码与原文随消息带出——SDK 不再对确定性拒绝无限重试。

---

## 4. [✅已修复] 低

> 本表 23 行**已于 2026-08-08 全部修复**，末列为实际修法。
> 修复分两批：主体在 `fix: gaps §4「低」全部 23 行`，逐条复核后发现其中 7 行只落了一半
> （memset 未真正消除、SK 只擦了失败路径、httplib 兜底仍丢 request id 等），残留在
> `fix: gaps §4 复核残留` 补齐。末列凡写"**残留**"者为刻意不做，并给出理由。

| 位置 | 问题 | ✅ 实际修法 |
| --- | --- | --- |
| `src/core/thread_pool.h:39` vs `.cc:23` | 声称"不可失败不可等待"的续体投递与阻塞任务共用同一个 4096 队列，压力下续体要排在 4096 个 IO 之后 | 续体独立 `cont_queue_`（无容量上限，`post` 不检查 `capacity_` 也不抛），worker 严格优先排空它再看 `queue_`/`backlog_`（`thread_pool.cc:95-97`）。**残留**：严格优先意味着续体洪峰理论上可饿死调度队列——续体是已完成任务的有限后继、不自我繁殖，故未加配额；`join` 之后的 `post` 仍就地执行（消费方是 noexcept 上下文，只能这样收尾） |
| `src/core/thread_pool.cc:87-89` | `completed_` 每任务取一次锁与调度队列争用；`wait_hist_` 在持锁段内取时间 | 两者改原子（`completed_` 与 `wait_hist_[]`，`thread_pool.h:109-110`），完成侧计时移出锁；复核时补掉入队侧——`post`/`enqueue_bounded` 的 `Clock::now()` 也移出临界区，`post` 的 `notify_one` 一并挪到锁外 |
| `src/core/metrics.cc:184,189` | 渲染用默认 6 位浮点精度：桶界 ≥1e6 会渲成 `1.04858e+06`，相近桶界可能折叠成重复 le 序列（Prometheus 直接拒绝整个 target） | `fmt_double` 改 `std::to_chars` 最短往返（`metrics.cc:16-24`），桶界不再折叠；复核补掉同一失败模式的另一半——`to_chars` 对非有限值写出的是 `inf`/`nan`，Prometheus 同样拒收整个 target，改为 `+Inf`/`-Inf`/`NaN`。两条均有用例（`metrics_nonfinite_render`、`metrics_large_bucket_bounds_render`） |
| `src/core/metrics.cc:168` | `# HELP` 文本不转义（标签值有转义，help 没有）；同名指标 help 不一致时静默取首次 | `escape_help`（`\` → `\\`、换行 → `\n`，规范只要求这两个）用于 `# HELP` 行；同名不同 help 改 LOG_WARN 后仍取首次（`metrics.cc:105-109`） |
| `src/core/util/crypto.cc:87-99` | 解出的凭证 SK 留在 `std::string` 里不擦除——对一个做了 SK at-rest 加密的系统是防护链缺环 | 首批只加了 `secure_wipe`（OPENSSL_cleanse）并擦解密失败路径，**成功路径的 SK 仍原样留在凭证表里**，即本行原文所指。复核补 `util::SecretString`（析构即 cleanse，继承 `std::string` 故既有 `const std::string&` 形参与比较一处未改），铺到 SK 的整条存活链：`Credential::secret_key`、`CredentialInfo::secret_key`、`CredentialLookup::secret_key`、静态表 `creds_`、`secret_for()` 返回值、以及 `derive_signing_key` 里的 `"AWS4"+sk` 派生串。**残留**：`std::string` 扩容遗留的旧缓冲不在擦除范围内（需自定义分配器），v1 明文凭证对象的 JSON 缓冲同理——那条路径本就是明文落盘 |
| `src/core/timer.cc:27` | 每次 `add` 都 `notify_all`，即便新条目不是最早到期者 | 仅当新条目成为 `items_` 首元素才 `notify_one`（`timer.cc:29-44`）；复核把该唤醒挪到锁外，免得被唤线程醒来即撞锁 |
| `src/core/task.h:133,139,143` | moved-from 的 `Task` 上调用即空指针解引用，无防护 | `co_await`/`via`/`with_cancel`/`start` 四个入口经 `check_valid` 抛 `logic_error`（`Task<T>` 与 `Task<void>` 两份特化各一套）；复核补上未列入原文但同样裸解引用的 `take_result()`。用例 `task_moved_from_throws_not_segv` |
| `src/http/drivers/common.h:116-134` | 驱动兜底的 400/500 响应既无 `x-amz-request-id` 头也无 XML 里的 RequestId——恰恰是最需要追溯的两类错误 | `internal_error_response`/`bad_request_response` 生成 id 并同时写进响应头与 XML。复核发现"能追溯"并未真正兑现：id 从不进日志（客户端拿着 id 也换不回现场），且 httplib 有五条兜底路径只搬 status+body、id 丢在半路。现改为在这两个构造点直接打日志（四驱动因此都不必各记一份，异常文本经参数带入），httplib 新增 `apply_fallback` 统一搬运，另两条自产错误响应（上游 4xx/5xx 翻译、431 头过大）改走新的 `upstream_error_response`，不再发空 `<RequestId>` |
| `src/http/drivers/builtin/builtin_server.cc:422-428` | thread-per-connection 默认 8MiB 栈 × 4096 = 32GiB 虚拟地址空间（实际峰值约 100KiB）；`char buf[16*1024] = {}` 每连接 memset 16KiB | 连接线程改 `pthread_create` + `pthread_attr_setstacksize(512KiB)` + DETACHED（`:211-236`），32GiB 虚拟地址预留消除。memset 那半首批**没真正消除**：`= {}` 从成员声明上删掉了，但构造点 `ConnReader reader{fd};` 是聚合初始化，未列出的成员照样值初始化（反汇编里 `memset` 仍在）。复核改为默认初始化后逐字段赋值，现编译产物中该目标文件已无 `memset` |
| `src/http/drivers/builtin/builtin_server.cc:347-349` | 无法区分空闲 keep-alive 与在途请求，停机固定阻塞 10 秒（beast/seastar 会立刻掐空闲会话） | `ConnShared` 增 idle fd 集合，等待新请求行期间登记（登记与 `stopping` 判定在同一临界区内，关闭竞态因此闭合），停机先 `shutdown()` 全部空闲 fd 再进宽限（`:203`、`:289-300`、`:527`）。**残留**：判定粒度是"是否已读到请求行的第一个字节"，卡在半个请求头上的连接算在途，仍占满宽限 |
| `src/http/drivers/httplib/httplib_server.cc:130,212,226` | 每个带 body 的请求额外起一个 `std::thread`；content provider **每 64KiB 块**构造一次 vector；`pushpull` 让请求体被拷贝两次 | content provider 的缓冲随闭包复用（`:302`，每响应一次 64KiB 分配）。**残留**：每请求一个 pump 线程与 `pushpull` 的两次拷贝是该驱动"推转拉"架构的形态本身，改动等于重写驱动；该驱动定位为功能验证而非性能路径（文件头已注明），故不做 |
| 四驱动 | drain 上限 4MiB、宽限 10s、trailer 16KiB、块大小 64KiB、连接上限 4096 等全部硬编码且散落四处（2.13 的连接上限漏改就是例子） | `drivers/common.h:19-24` 收口 `kDrainMaxBytes`/`kTrailerMaxBytes`/`kIoChunkBytes`/`kScratchBytes`/`kShutdownGrace`/`kShutdownForceWait`，四驱动全部引用，字面量不再有第二份。连接上限走的是另一条路——它已是 `HttpConfig::max_connections` 配置项（§2.13 的修法），运维可调，不该再退回编译期常量。复核另收掉 `main.cc` 里与宽限同值的第二个 `10`：它等的是许可归位（与驱动的连接宽限是两个量），但日志串里又抄了一遍 `"10s"`，改为单一常量 + 格式化 |
| `src/s3/metrics.cc:26-29` | `s3_error` 每个错误响应抢一把全局互斥锁（键有界，问题只在争用；同文件 `by_method_` 已是原子数组） | 改按枚举下标的定长原子数组（`metrics.h:28-30`），互斥锁去除；`kS3ErrorCodeCount` 与枚举同出一张 X-macro 码表（`errors.h:49-53`），加错误码时两者一次改完，不存在漂移 |
| `src/s3/xml.cc:9-23` | `xml_escape` 不处理控制字符，而 `<Resource>` 会反射**未经校验**的 bucket 名（1.1 修好后自动消解，但出口仍应兜底） | `xml_escape` 丢弃 XML 1.0 非法控制字符（`< 0x20` 且非 `\t`/`\n`/`\r`，`xml.cc:23-25`），出口兜底成立 |
| `src/storage/listing.cc:7-28` | `bytes=5-3` 这类**语法非法**的 range 返回 416，RFC 9110 与 AWS 的行为是忽略该头回 200 | 修在解析侧而非 `resolve_range`：`parse_range_header` 判出 `last < first` 即返回 `nullopt`，整个头按无效忽略回 200（`s3/handlers/objects.cc:42-44`）——416 的语义留给"语法合法但不可满足"，两者本就该分开。用例已加 |
| `src/storage/listing.cc:52,66,71` | `next_token` 是明文 key 而非不透明串；S3 层 V1 做了编码、V2 直接透传，两版本不一致 | 同样修在 S3 层：V2 的 `continuation-token` base64 签发/解析，解不开回 400 InvalidArgument；V1 的 `marker` 保持明文 key（规范里它本就是 key 且响应要回显），两版本各归各语义（`s3/handlers/list_objects.cc:11-78`、`:113-125`）。用例覆盖不透明往返与坏 token |
| `src/storage/tiered/tiered_backend.cc:614-639` | 磁盘被本实例之外的东西写满且无可回收候选时，每轮重算 `need>0` 却释放 0 字节，无任何日志 | 淘汰轮结束仍有缺口时 LOG_WARN 并报出还差多少字节（`:714-718`），不再静默空转。频率由 `scan_interval_sec`（默认 3600s）天然限住 |
| `src/storage/tiered/tiered_backend.cc:980-1017` | `atime_` 表无界增长（绕过 tiered 的删除不清理），快照是每 5 分钟**全量重写**（千万对象即数百 MB + fsync） | 有界化：快照时按判冷阈值滚动淘汰（早于 `cold_after_sec` 的记录丢掉——对象已够判冷资格，mtime 兜底得同一结论），表只随"近期被访问的键"增长（`:1109-1130`）。全量重写那半复核时补：加脏位，表自上次快照没动过就整轮跳过（写失败则置回脏位下轮重试）。**残留**：表确有变更时仍是全量重写，增量 journal 未做——变更即写是这份快照的正确性来源，且滚动淘汰后体量已随近期访问集而非对象总数 |
| `src/storage/localfs/localfs_backend.cc:85-86` | `create_bucket` 的 marker 从不 fsync（对象写路径是严格 fsync 的），掉电后 bucket 消失而客户端已收到 200 | marker 与桶目录 fsync；复核补两点：桶目录自己的 dirent 记在 root 里，只 fsync 桶目录换不来它的持久性，故加 `fsync_dir(root_)`；且原实现绕开了 `fsutil` 的 fsync 总开关，改走 `fsutil::fsync_file`/`fsync_dir` 与对象写路径同进退（`:96-110`） |
| `src/storage/duostore/codec.cc:41,180` | 编码路径的超限走 `corrupt()` → 用户提交超长 user-meta 得到 500 "corrupt meta value"，正确语义是 400 | 编码侧超限改抛 `S3Error(InvalidArgument)`（`too_large()`，`codec.cc:42-48`、`:189`），映射 400；`corrupt()`（InternalError/500）此后只用于解码路径 |
| `src/storage/duostore/codec.cc:152-175` | `read_extent_runs` 不校验 pack extent 的 `count` 必须为 1，也无 run 数上限，损坏值会解出一串假 extent | 校验 `kind <= kRados`、`count != 0`、pack 的 `count == 1`、以及 `count` 必须被剩余 crc 字节覆盖（`:159-171`）。复核补上原文点名的"run 数上限"：`n_runs` 是裸 u32，按每 run 至少 33B 头先卡剩余载荷，否则损坏长度要先把 vector 撑到几十万条才轮到 `need()` 抛；`skip_extent_runs` 同步加 |
| `tikv_client.cc:240,308` | region 错误路径用**递归**重做批次，退避预算耗尽前深度无界 | `prewrite_keys`/`commit_keys` 改显式工作栈（`:214-224`、`:293-304`），递归消除。栈驻留的 key 总量不超过本事务的 key 数（`make_batches` 只做切分不复制），终止由退避预算耗尽时抛出保证 |
| `fs_data_store.cc:485` | fs 引擎对 `kRados` extent 静默跳过 → 引擎错配时 GC 全程空转且不告警 | `FsDataStore::remove` 遇非 `kChunk`/`kPack` 的 extent 记一次 LOG_ERROR（函数内静态原子位，全进程一次），指出数据/元引擎错配（`:588-597`）。`kPack` 仍静默跳过是设计——它的空洞由压实回收 |

---

# 第二部分 · 未实现的功能

## 5. [✅已修复] S3 协议缺口

> 本节 10 小节**已于 2026-08-09 全部实现**，各小节末尾为实际修法；
> 凡写"**残留**"者为刻意不做，并给出理由。

### 5.1 [✅已修复] 分页缺失（高）

- **ListMultipartUploads**（`src/s3/handlers/multipart.cc:201-224`）：`(void)req;` —— `prefix`/`delimiter`/`key-marker`/`upload-id-marker`/`max-uploads` **一个都不读**，硬编码 `MaxUploads=1000` / `IsTruncated=false` 却返回全部 upload。5000 个活跃 upload 时客户端按 `IsTruncated` 判定已到尾，同时单请求把整表构造进内存。
- **ListParts**（`:173-199`）：同样恒 `IsTruncated=false`，忽略 `part-number-marker`/`max-parts`。
- 存储层契约本身也没有分页位：`src/storage/backend.h:126-130` 返回裸 vector。

**建议**：接口加 `ListUploadsOptions`/`part_number_marker`+`max_parts` 与带 `is_truncated`/`next_*_marker` 的结果结构；过渡期至少在 L2 按 max 截断并**据实**置 `IsTruncated`，而不是谎报 false。

**✅已修复**（2026-08-09）：按建议做了完整版而非过渡版。契约新增
`ListPartsOptions/Result` 与 `ListUploadsOptions/Result`（形态对齐既有的
`ListOptions/ListResult`）；截断语义只写一份——`storage/listing.h` 的
`apply_parts_page`/`apply_uploads_page`，六个后端共用，免得规则在每个后端里各长
一个样。`max<=0` 一律"空且未截断"（空游标 + truncated 会让循环续传的客户端原地
死循环）；delimiter 分组的续传游标落在**组尾那一条**而非组名（"a/" < "a/x"，
用组名当游标会把整组再列一遍）。
各后端按能力下推：sqlite 用 `(key,id) > (?,?)` 行值比较 + LIMIT，rocks/tikv 直接
seek/抬高扫描下界，memory/localfs 排序后裁剪，**redis 无法下推**（uploads 存成
hash，HSCAN 游标是桶序不是字典序）——如实忽略提示、全量扫完再裁剪，代码处注明
省的是响应体不是扫描。cloudproxy 由"累积远端所有页"改为**转发一页**，客户端
marker 直接成为远端 marker（此前要第一页也得等全量）。
localfs 的 `list_parts` 顺带改成"先按分片号排序、只对本页 stat + 读 .md5"——此前
每个分片都 stat 并读一次 sidecar，翻页白付 9999 次。
**残留**：localfs 的 mpu 是全实例共用的平目录，判断归属必须读 manifest，因此
无论 marker 在哪都得全扫一遍；要省得把布局改成 `mpu/<bucket>/…`，是另一件事。

### 5.2 [✅已修复] 对象元数据只保留 Content-Type 与 x-amz-meta-*（中）

`Cache-Control`、`Content-Disposition`、`Content-Encoding`、`Content-Language`、`Expires`、`x-amz-storage-class` 在 PUT/CreateMultipartUpload 时全部丢弃（`src/s3/handlers/common.h:25-34`），GET/HEAD 也不回（`objects.cc:45-51`）。这些是 S3 的一等对象元数据。丢 `Content-Encoding: gzip` 会让浏览器拿到无法解压的字节流。sidecar/xattr 已是 TSV，扩字段成本低。

**✅已修复**（2026-08-09）：五个字段做成 `kStdMetaFields` 单一事实来源（头名 +
持久化键名 + 成员指针），提取、回显、各后端序列化都遍历它——不会再出现"存了但
不回"的半吊子。落到：localfs TSV（未知键本就忽略，存量 sidecar 逐字节不变）、
MPU manifest（跨 create→complete 存活）、duostore codec 升 v2（新增自描述 kv 段
+ `read_ver` 兼容读 v1，存量记录原地可读；此前 `check_ver` 严格相等，加字段等于
停机重写全量元数据）、cloudproxy 头透传（签名自动覆盖）、tiered 对账重建。
CopyObject 的 COPY 分支改整份拷贝——逐字段抄写正是这类字段被漏掉的原因。值里的
CR/LF 一律 400（TSV 按行存，也是响应头注入面）。
**`x-amz-storage-class` 刻意不入此列**：本实现只有 STANDARD 一种存储类，收下
GLACIER 再原样回显等于替存储层撒谎（对象根本没进任何归档层），非 STANDARD 直接
501，与 `x-amz-acl` 的处理一致。

### 5.3 [✅已修复] `response-*` 覆盖参数未实现（中）

`response-content-type`、`response-content-disposition`、`response-cache-control`、`response-content-encoding`、`response-content-language`、`response-expires` 六个既不生效也不报错。其中 `response-content-disposition` 是 presigned 下载链接最常用的参数，而文档明确承诺支持 presigned GET。实现成本很低（直接 `resp.headers.set` 覆盖，注意仅已认证请求允许）。

**✅已修复**（2026-08-09）：六个参数全部生效，在 206/Content-Range 之前应用
（那两个由本次传输决定，不接受客户端指定），GET 与 HEAD 一致。query 值是攻击者
可控的，塞进响应头前一律挡 CR/LF（响应拆分）。关于"仅已认证请求"：本实现开启
认证时，能走到 handler 的请求必然已验签；关闭认证时不存在这条边界——故无需再判
身份，代码处已注明这个前提。

### 5.4 [✅已修复] CreateBucket 从不读请求体（中）

`src/s3/handlers/buckets.cc:46-51` 连 `HttpRequest&` 参数都没有，`LocationConstraint` 从未解析。而 `docs/s3-protocol.md:93-95` 明确列出"需要解析请求 XML 的三处"包含 CreateBucket。后果：跨 region 的建桶请求静默成功，随后 `GetBucketLocation` 回显的却是本地配置 region。AWS 此时返回 `InvalidLocationConstraint`（错误码表里也没有这个码）。

**✅已修复**（2026-08-09）：`create_bucket` 补 `HttpRequest&` 参数并解析
`CreateBucketConfiguration/LocationConstraint`，与本端 region 不符即
`InvalidLocationConstraint`(400) **且不建桶**；空 body 与空约束均按 us-east-1
处理（S3 惯例：该 region 不写约束）。错误码随 §5.8 一并补入码表。

### 5.5 [✅已修复] ListObjectsV2 的 owner 与 marker 语义（中）

`fetch-owner=true` 被忽略且 `ObjectMeta` 根本没有 owner 字段；V2 请求带 `start-after` 时不回显 `<StartAfter>`；V1/V2 的三种 marker 被塌缩成同一个 `opt.start_after`，导致 V1 响应可能回显客户端从未发过的 `<Marker>`。

**✅已修复**（2026-08-09）：三种 marker 各归各版本——V2 认 continuation-token
（同时出现时它胜出）与 start-after 并回显 `<StartAfter>`，V1 只认 marker；V1 请求
带 start-after 不再生效，也不再回显没发过的值。`fetch-owner=true` 回 `<Owner>`，
与 ListAllMyBuckets 共用 `kOwnerId`——本实现无多租户 owner 概念，与其给
`ObjectMeta` 加一个恒定字段再穿过五个后端，不如在 handler 侧用同一个身份。

### 5.6 [✅已修复] Content-MD5 / x-amz-checksum-* 完全未实现（中）

全仓无任何 `Content-MD5` 处理。AWS **要求** DeleteObjects 带 `Content-MD5`，缺失返回 400——这里不校验，被中间设备改写的批量删除会被照单执行。PutObject/UploadPart 的 `Content-MD5` 不匹配时 AWS 返回 `BadDigest`，本实现完全忽略。`x-amz-content-sha256` 的校验链已很完善，只需在同一位置多挂一个 MD5 装饰器。

**✅已修复**（2026-08-09）：按建议加装饰器（`ChecksumVerifyingReader`，形态照搬
`Sha256VerifyingReader`）。装在 dispatch 的 verify **之后**，于是包在
sha256/aws-chunked 装饰器之外——摘要按解帧后的明文算，与客户端算的是同一份字节；
与签名无关，认证关闭时同样生效。覆盖 Content-MD5 与
`x-amz-checksum-{crc32,crc32c,sha1,sha256}`，一次声明多个则逐个校验。摘要不符
`BadDigest`、格式非法 `InvalidDigest`——分开是因为混为一谈会让客户端分不清是自己
算错了还是链路改写了 body。DeleteObjects 缺完整性头即 400（它是唯一一个"请求体
被改写即静默多删对象"的操作）。
配套：crc32c 从 duostore 上提到 `core/util/checksum.h`（S3 与 extent 校验同用一
份），新增 crc32(IEEE) 与共享 base64（list_objects 的 token 编解码此前是私有的第
二份实现）。
**顺带修掉一个此前无人触发的崩溃**：xlocalfs 的 `auto [total, etag] = co_await
drain_to_tmp(...)`，body 读到一半抛异常时 GCC 会对那个从未构造的绑定目标照跑
`std::string` 析构——double free/SEGV（ASAN 确认）。此前只有客户端断连能触发，
Content-MD5 让它变成一个普通请求就能打到的路径。backend_suite 因此补了
"body 中途抛异常"的契约用例，对全部后端生效。

### 5.7 [✅已修复] Multipart 的 AWS 硬约束缺失（中）

- **最小分片 5 MiB**（末片除外）未校验 → 允许上传 10000 个 1 字节分片，complete 时逐个 open/read/write 拼接，是廉价的放大面（AWS 返回 `EntityTooSmall`）；
- `complete_multipart` 不复核 part_no 上界、不限 `parts.size()`；
- 分片乱序 AWS 返回 `InvalidPartOrder`，这里返回 `InvalidPart`；
- `<Location>` 回相对路径而非完整 URL（部分 Java SDK 直接当 URL 用）。

**✅已修复**（2026-08-09）：四条全补。最小分片校验点放 **L2 而非存储层**——这是
S3 协议规则不是存储规则，直接用后端 API 的调用方（含各后端一致性套件）不该被
5MiB 绑住；尺寸只有存储层知道，故 complete 前先 list 一次。同时做成旋钮
`http.min_part_size`（默认 5MiB，0=关）：网关前面挂的工具链未必守这条规则，且
"proxy 到另一个 lights3"的部署形态不该被两层各判一次。乱序改
`InvalidPartOrder`（`InvalidPart` 会让客户端去重传分片，实际要做的是把列表排好
序）；complete 侧复核分片号上界与 parts 数量上限（此前无限追加，一份构造好的
XML 就能让请求把任意长的列表读进内存）；`<Location>` 用 req.path 重建完整 URL，
path-style 与 vhost 两种寻址一并覆盖，scheme 取 X-Forwarded-Proto。
e2e 的 multipart 分片由 3MiB 改 5MiB——3MiB 打到真 AWS 上同样会被拒。

### 5.8 [✅已修复] 错误码词表缺口（低）

`src/s3/errors.h:15-39` 共 24 个码，缺 `InvalidLocationConstraint`、`BadDigest`/`InvalidDigest`、`EntityTooSmall`/`InvalidPartOrder`、`MissingContentLength`、`RequestTimeout`、`BucketAlreadyExists`、`TooManyBuckets`、`InvalidObjectState`、`PermanentRedirect`、`ExpiredToken`/`InvalidToken` 等。X-macro 结构使新增成本几乎为零。

**✅已修复**（2026-08-09）：点名的 12 个码全部补入（`RequestTimeout` 本就有）。
X-macro 同源派生枚举、状态码表、`kS3ErrorCodeCount` 与指标槽位，新码自动获得
metrics 计数位，正反向映射不可能漂移。

### 5.9 [✅已修复] 响应字段与协议细节（低）

- `HeadBucket` 不回 `x-amz-bucket-region`（boto3 的跨区重定向依赖它）；
- 错误 XML 无 `<HostId>`，响应头无 `x-amz-id-2`；
- 405 响应不带 `Allow` 头（违反 RFC 9110 §15.5.6）；
- 304 响应只带 `ETag`，RFC 要求同时带 `Last-Modified`/`Cache-Control`；
- presigned POST（浏览器表单上传）返回 405 而非文档承诺的 501。

**✅已修复**（2026-08-09）：HeadBucket 与 CreateBucket 回 `x-amz-bucket-region`；
错误 XML 加 `<HostId>` + 响应头 `x-amz-id-2`（AWS 支持工单要的两个 id，客户端只会
转述它看到的这一对，为此 `RequestContext` 增 host_id）；405 带 `Allow`，名单由分派
表二次扫描得出、不会与表漂移（`S3Error` 为此增 headers 槽位承载这类必带头）；
304 补 `Last-Modified`/`Cache-Control`。
**残留**：presigned POST 仍是 405/403 而非 501。真正的表单上传既不带
Authorization 头也不带 X-Amz-Algorithm（policy 与签名在 multipart 表单字段里），
要回 501 得在验签之前识别它——那等于先实现半个 POST 表单解析器，与"明确不支持"
的收益不相称，留待真要做 presigned POST 时一并处理。

### 5.10 [✅已修复] 权限模型的表达力（中）

`CredentialPolicy` 只有 `buckets` glob + `readonly`（`src/s3/auth/credential_store.h:31-35`）：
- **无 key 前缀粒度** → 多租户共桶只能退化成"一租户一桶"；
- **无 action 粒度** → `readonly=false` 同时获得 DeleteObject/DeleteObjects/DeleteBucket，给不出"能写不能删"这个最常见的备份场景；
- **ListBuckets 不过滤**（已记为取舍，但结合 1.1 的桶名混淆，全桶枚举正是攻击链第一步，取舍前提变了）；
- glob 用 `fnmatch(..., 0)`，`*` 跨 `/`；
- policy 创建后不可改，实践中促使运维图省事直接用 root，反噬两级模型本身。

另：root 静态凭证的**明文 SK 可经 admin API 取回**（`admin_credentials.cc:40-54` 的 `is_static()` 只挡了 comment/policy 两个字段）。这把"读配置文件/环境变量"的信任边界降级成一次 HTTP GET，而 root SK 无法经 admin API 吊销。`mask()` 还暴露 40 字符中的 8 个。建议对静态凭证强制 `with_secret = false`，并对 `?show-secret=true` 单独打审计日志。

**✅已修复**（2026-08-09）：policy 从"bucket glob + readonly"扩到三维——
`actions`（read/write/delete）、`prefixes`（key 前缀白名单）、`buckets`，
`readonly` 保留为 `actions:["read"]` 的等价写法。动作按**后果**归类而非 HTTP
方法：DeleteObjects 是 POST 却是删除、CreateMultipartUpload 同为 POST 却是写入，
方法维度分不开这两件事——为此在分派表每条路由上标注 `Action`，并把匹配从
`route()` 提出来（`match_route`），授权在调用 handler 之前就拿到动作。
`fnmatch` 补 `FNM_PATHNAME`，`*` 不再跨 `/`。
ListBuckets 改为按 policy 过滤：原取舍写的是"只泄露桶名，不值得"，但桶名正是
攻击链第一步，且 §1.1 的桶名混淆修好后该取舍的前提已经变了；为此把验签结果沿
分派表传给 handler（Handler 签名加一参，17 条路由机械改一遍）。
静态凭证的明文 SK 不再经 admin API 回传（按建议），掩码收紧为只留前 4 位，任何
`?show-secret=true` 都记 WARN 审计日志。CopyObject 的源侧授权顺带补齐为
源桶 + 源 key（加了前缀粒度后只校验桶就漏了）。
docs/credential-management.md §10.4 重写并新增 §10.5，中英同步；顺带修掉两处早已
失真的描述——执行点写的是 `authorize(ak,bucket,is_write)`，而实际自 §3.7 起就是对
验签快照判定。
**残留**：policy 创建后仍不可改（无 update API）。补 update 会牵动多实例增量同步
——现同步模型只有增删两种，policy 编辑无法传播，得先给落盘对象加版本位，属另一
件事。

## 6. [✅已修复] 存储引擎能力缺口

> 本节三小节**已于 2026-08-11 全部处理完毕**：各表格下方为逐行的实际修法；
> 凡写"**刻意保留**"者为按 workload 论证后的设计边界，并给出理由。

### 6.1 [✅已修复] duostore

| 缺口 | 位置 | 说明 |
| --- | --- | --- |
| **pack 无老化轮转**（高） | `fs_data_store.cc:433` | 只按容量与进程关闭封存。低写入量下 active pack 永不封存 → 其中被覆盖/删除的 record 成为永不可回收的死区，最坏 512MiB 常驻 |
| **压实无预算无优先级**（高） | `duostore_backend.cc:1061-1085` | 单轮把**全部**符合条件的 pack 一次性重写完，无"本轮最多 N 个"预算、不按可回收收益排序 → 批量删除后单轮 GC 可持锁数小时 |
| **RADOS 能力三缺**（高） | `rados_data_store.cc:509-519,454` | `remove_pack`/`rewrite_pack` 恒 no-op（无小对象聚合与压实）；`WriteHint.owner` 被丢弃（灾难恢复无归属信息）；无写侧 pin（见 1.2） |
| **packs/ 目录无反向对账**（中） | `fs_data_store.cc:592-615` | 孤儿扫描只覆盖 chunk。硬崩在"建 pack 文件"之后、首条 record 提交之前 → 账里没有任何行 → 文件永久泄漏且不可观测 |
| **四引擎能力矩阵不齐**（中） | 多处 | redis 未覆写 `ack_reclaims`（退化为逐条 RTT）；默认引擎 rocksdb **完全无指标**；`refs.owner` 有三种互不相通的格式，离线取证工具无法跨引擎复用 |
| **schema 无演进路径**（中） | 四引擎 | 只有"版本必须精确等于当前常量"的硬校验，无 migration 钩子。任何 value 布局变更都会让存量库无法启动 |
| **无备份/恢复与跨引擎迁移**（中） | 全模块 | meta 与 data 是两个独立存储，没有取一致快照的机制；`meta_kind` 一旦选定无法迁移。建议提供以 `IMetaStore` 为中介的 dump/load（天然兼作迁移工具），并把备份顺序固化为"先 data 后 meta + 恢复后强制孤儿扫描" |
| **多网关能力边界比文档更窄**（中） | `duostore_backend.cc:1226-1231` | GC 单实例是**约定**而非强制（无租约/选主），误配两台同开 GC 会互相 unlink 对方判定的空 pack；压实/延迟删除的簿记为进程内，重启即丢 |
| **GC 可观测性不足以运维**（中） | `duostore_backend.cc:598-628` | 无 gcq 深度 gauge（看不到回收是否追得上删除）、无 pack 空间放大比、无存储用量、GC 单轮耗时无直方图、`skipped_*` 无出口 |
| **TiKV 侧车边界**（中） | `tikv_client.cc:153-186` | 只有乐观 2PC，无悲观事务、无 lock TTL 续租、无大事务分片提交、批间无并发 |
| gcq 的 reason 字段声明但未实现（低） | `codec.cc:406` | 回收来源不可区分，无法定位压力来源，也无法按来源施加不同 grace |

**✅已修复**（2026-08-10/11，逐行）：

- **pack 老化轮转**：新增 `pack_max_age`（默认 1h，0=关）。写路径在下一条 record
  处顺带查年龄（与容量判据同一封存分支）；`IDataStore::seal_aged_packs` 供 GC
  每轮补扫——写入停止后写路径不再被触达，恰是低写入量场景的形态。try_lock 拿不到
  即跳过（该槽正被写，它自己会轮转），GC 不与业务写抢锁。
- **压实预算与优先级**：先收齐候选连同可回收字节，按收益降序（大小未知的崩溃
  遗留项排最后），再按 `gc_compact_max_packs`（默认 16）/`gc_compact_max_bytes`
  （默认 1GiB）截断；首个候选恒放行（防单 pack 大于整轮预算时永不推进）。被挤下
  的条数走 DuoGcStats/gauge/日志出口，不静默截断。
- **RADOS 三缺**：写侧 pin 已随 §1.2 修复（分配即 pin，finish 移交调用方解除）；
  `WriteHint.owner` 现随每片对象以单对象原子 write_op 落 `lights3.owner` xattr
  （数据与归属要么都在要么都不在），离线打捞 getxattr 反查。
  `remove_pack`/`rewrite_pack` 恒 no-op **刻意保留**：pack 聚合针对本地 fs 的
  per-file 成本，RADOS 侧小对象放大由 BlueStore min_alloc_size 与池策略承担，
  网关叠 pack 只会引入跨对象压实与读改写放大（论证注释在 rados_data_store.cc）。
- **packs/ 反向对账**：`IDataStore::scan_packs` + 孤儿扫描双向化——账外 pack 过
  三道门（mtime 逾 grace、无 pin、无 flock 写锁）后 unlink；账在文件缺只告警计数
  绝不删 meta。chunks/packs 枚举收敛到一个 scan_shard_tree。
- **四引擎能力矩阵**：redis 覆写 `ack_reclaims`（单 Lua 一次 RTT 批量销账，此前
  逐条往返）；rocksdb 挂上引擎指标（estimate_num_keys / block_cache_usage /
  sst_bytes / memtable_bytes，属性 gauge 渲染时现取）；`refs.owner` 三种历史形态
  收敛为 `codec::parse_pack_owner` 唯一解析器（对象 / mpu 五段 / 旧 mpu 三段，
  离线取证工具与压实回调同一入口），kLegacyPart/kUnknown 即"保守不迁"判据。
- **schema 演进**：四引擎统一"存量 < 当前走迁移链、> 当前拒绝降级运行、乱码/异
  谱系拒绝"（共享判定 `meta_util.h parse_schema_marker`；rocks/tikv/redis 各自
  迁移链钩子，sqlite 走 user_version 同款）。链当前为空——record 级演进走 codec
  `read_ver` 兼容读（§5.2 先例），链留给 CF/键布局变更；"改布局不留迁移"开机
  响亮失败。共享引擎（redis/tikv）升级后旧版网关开机即拒，天然挡混布回写。
- **备份/恢复与跨引擎迁移**：`meta_dump.{h,cc}` 以 IMetaStore 为中介逻辑
  dump/load（自描述二进制流：桶/对象记录含 extent manifest + 封存 pack 账 +
  计数 + crc32c），record 级重放天然兼作四引擎互迁。恢复端把 chunk/pack 计数器
  烧号抬到已见最大 id 之上（防新写撞存量文件号）。入口
  `lights3 --duostore_admin dump:<backend>:<file> / load:...`（server 起动前执行，
  停写天然成立），load 内置强制孤儿扫描。备份顺序固化在 meta_dump.h 头注释：
  先 data 后 meta；恢复先 data、load、孤儿扫描。刻意不入档：进行中 MPU
  （upload_id 无法保值，跨迁移续传本就不成立）与 gcq（无入队原语）——两者的数据
  在恢复侧无引用，孤儿扫描兜底回收；桶创建时间与对象 version 重打（无持久语义）。
- **多网关 GC 边界**：`IMetaStore::try_gc_lease`——GC 与孤儿扫描每轮开始先取带
  TTL 的租约（TTL = max(2×gc_interval, 10min)），共享引擎以原子 CAS 实现（redis
  单 Lua、tikv 快照读+prewrite 冲突检测），同 owner 续租、他人持有即跳过本轮；
  本地引擎单进程文件锁已独占，恒 true。误配两台同开 GC 从"互相 unlink"降级为
  "一台干活一台跳过"。**残留**（无害方向）：空 pack 延迟删除的"首次见空"时刻
  为进程内簿记，重启后重新计时——只推迟回收，不会误删。
- **GC 可观测性**：gcq_depth / gcq_oldest_age（全量轮刷新，防增量轮谎报 0）、
  skipped_grace/pinned 走 gauge（每轮重扫，counter 必然虚高）、
  pack_accounted_bytes/pack_live_bytes（比值即空间放大率，留查询侧算）、
  chunk_bytes/pack_bytes 用量、gc_round_seconds 直方图。
- **TiKV 侧车**：四项**刻意保留**为设计边界（论证注释在 tikv_client.cc
  Committer 上方）——meta 事务按构造有界（对象 value 6MiB 封顶、gcq 单项 4096
  extent 拆分、万分片 complete 实测数十批），txn_lock_ttl 的 √MiB 伸缩已覆盖
  TTL 需求，无需心跳续租；悲观事务在 client-c 无成熟 C++ 路径且本店无长事务；
  批间并发要把 Backoffer/RegionCache 线程安全化，只惠及低频宽事务，不值——
  未来出现常态宽事务时先拆事务而非并发化。
- **gcq reason**：`ReclaimReason`（overwrite/delete/part_overwrite/abort/
  complete）四引擎入队带上来源，GC 按 reason 分桶计数；旧账解出 unknown，不抬
  版本号，新旧账同队列共存。

### 6.2 [✅已修复] cloudproxy

| 缺口 | 位置 | 说明 |
| --- | --- | --- |
| **无长度上行不支持**（中） | `cloudproxy_backend.cc:493-496` | `body.length()` 为空时直接 `NotImplemented`（"首期不做 TRAILER 组帧"）。上游是 chunked 且无 `x-amz-decoded-content-length` 的请求因此在 cloudproxy 后端上无法 PUT |
| **元数据透传范围窄**（中） | `cloudproxy_backend.cc:52-56` | `meta_headers` 只透传 `x-amz-meta-*`，`Content-Type` 单独走参数。SSE 头、storage-class、tagging 一律不透传——与 5.2 是同一个 `ObjectMeta` 字段缺失的下游表现 |
| **同后端 copy 不走远端服务端 COPY**（中） | 见 6.3 的 copy 快路径 | 云端内部 copy 会"下载到网关再传回"，产生 2 倍跨网流量与费用 |
| **分片上传无并发** | `stream_upload` | 每个 part 一条串行传输，未利用远端的并发上传能力；无断点续传 |

> 注：cloudproxy 的正确性问题（阻塞事件循环、响应头等待无超时、200+body Error 不重试）已在 2026-08-02 的上一轮评审中修复，此处不再列出。本节的能力缺口由针对性扫描得出，覆盖深度低于其它子系统——**建议后续单独补一轮 cloudproxy 深读**。

**✅已修复**（2026-08-10，逐行）：

- **无长度上行**：chunked 且无 decoded-content-length 的 PUT 由直接 NotImplemented
  改为 spool 到本地临时文件（O_TMPFILE 匿名 inode，崩溃即回收；不支持则
  unlink-after-open）取得长度后走既有 stream_upload。`spool_max_bytes` 封顶
  （默认 5GiB = AWS 单 PUT 上限，0 = 保留旧拒绝语义）、`spool_dir` 可配。
- **元数据透传**：经核对已被 §5.2 的 `kStdMetaFields` 透传覆盖；SSE/
  storage-class/tagging 是该次改动**刻意 501** 的（收下再回显等于替存储层撒谎）。
- **同后端 copy**：见 6.3 copy 快路径——cloudproxy 实现远端服务端 COPY
  （`x-amz-copy-source` + REPLACE + 我方元数据头），一次远端调用完成，消掉
  2 倍跨网流量；COPY 与 complete 同款"200 先回、错误在 body"陷阱按既有模式处理。
- **分片上传无并发**：**刻意保留**——分片各自是独立客户端请求、本就并发，网关
  无从并行化单条串行到来的 body；断点续传同理是客户端侧语义。

### 6.3 [✅已修复] 本地系与通用层

| 缺口 | 位置 | 说明 |
| --- | --- | --- |
| **xlocalfs 的 io_uring 能力远未用满**（高） | `uring.h:63` 等 | `fsync()` awaitable **零调用**（提交段仍走同步 fsync，正是 xlocalfs 要消除的）；无 writev/readv、无注册缓冲、无固定文件、无 SQPOLL、无批量提交（每 SQE 一次 `io_uring_enter`，退化到 pread/pwrite 同量级）；无内核特性探测；setup 失败直接抛 → 老内核下进程起不来且不回落 localfs |
| **跨后端 copy 无快路径**（中） | `IStorageBackend` | 流式已做对，但同 localfs 内的 copy 仍完整读写一遍（无 `copy_file_range`/reflink）；同云端后端内的 copy 会"下载到网关再传回去"（2 倍跨网流量与费用），而远端本可一个 `x-amz-copy-source` 完成。接口无 copy 钩子 |
| **MPU 孤儿清理只在启动跑一次**（中） | `localfs_backend.cc:501-516` | 跑数月不重启的网关会无限累积从未 complete/abort 的上传目录；`kMpuTtl` 还是硬编码 7 天 |
| **不支持目录标记对象**（中） | `validate.cc:36-42` | `PUT /bucket/folder/` 被拒。S3 控制台"新建文件夹"、s3fs、goofys、rclone 的目录语义全依赖它。同处还拒绝了 AWS 合法的 `a//b`、`a/./b`、前导 `/`——这些限制源自 localfs 的路径映射却放在**共享**校验层，连 memory/duostore/cloudproxy 也一并丧失兼容性。建议把路径不安全规则下沉为 localfs 专用 |
| **memory 后端无任何硬限制**（中） | `memory_backend.cc` | 注释定位为"单测与 demo 用"但被注册为一等后端：无容量上限、无淘汰、无 MPU TTL、无 close |
| **bucket_router 表达力**（中） | `bucket_router.cc:24-28` | 只有 bucket glob 单维度，不支持按 key 前缀路由/否定规则/正则；不校验 glob 语法（写错的 pattern 静默永不匹配）；无"规则不可达"检测；每请求一次堆分配 + 线性 fnmatch |
| **`validate_bucket_name` 缺 AWS 规则**（中） | `validate.cc:16-20` | 缺 IP 地址形式拒绝、保留前后缀（`xn--`/`sthree-`/`-s3alias` 等）、`.-`/`-.` 相邻拒绝 |
| **tiered quota 无增量维护**（低） | `tiered_backend.cc:589` | 文档承诺"遍历累计 + 增量维护"，实现只有遍历累计 → PUT/DELETE 之间的配额超限要等下一轮 scan（默认 1 小时）才发现 |

**✅已修复**（2026-08-10，逐行）：

- **xlocalfs io_uring**：内核能力探测（IORING_REGISTER_PROBE，探不到 READ/WRITE
  回落 READV/WRITEV）；`fsync()` awaitable 接入提交段（FSYNC SQE 替换阻塞
  fdatasync，等待期间线程回池）；批量提交（"当班 flusher"代提 enter，N 条 SQE
  一次系统调用）；SQPOLL 可选（建权失败回落普通模式）；setup 失败回落 localfs
  （两者盘上布局相同，回落无损，WARN 留痕）。注册缓冲/固定文件未做——属吞吐
  调优而非能力缺口，现测瓶颈在盘不在提交路径。
- **copy 快路径**：`IStorageBackend::copy_object_fast` 钩子（默认 nullopt 回落
  流式）。localfs/xlocalfs 用 `copy_file_range`（reflink 文件系统 O(1) 克隆），
  EXDEV/ENOSYS/tier stub/并发截断均回落流式；cloudproxy 走远端服务端 COPY（见
  6.2）。tiered/memory/duostore **刻意维持流式回落**：tiered 的 stub 语义与
  duostore 的引用计数模型下快路径需另行设计，流式路径语义正确。
- **MPU 周期清理**：`LocalFsOptions{mpu_ttl, mpu_scan_interval}`（配置键同名），
  TimerQueue 完成后重臂周期扫，close/dtor 撤定时器等在途；xlocalfs/tiered 链回
  基座。
- **目录标记对象**：全后端支持 `PUT bucket/folder/`——localfs 映射为目录内保留
  标记文件 `.lights3-dir`（全链路），其余后端本就是平面 key 空间。localfs 专属
  路径规则（前导 '/'、空段、单段 255B）下沉为 `validate_fs_object_key`，共享层
  不再让 memory/duostore/cloudproxy 陪绑；`.`/`..` 段留共享层（转发型后端拼 URL
  时 dot-segment 归一会改写对象身份，不只是本地路径问题）。
- **memory 硬限制**：`max_bytes` 容量闸门（超限 503 SlowDown，对象+在途分片
  同账）、mpu_ttl 过期清理（入口顺带扫，不加后台线程）、close() 释放驻留、
  used_bytes gauge 可观测。
- **bucket_router**：坏 glob（未闭合字符类）与永不可能命中的 pattern（含桶名
  字符集外的字面字符）构建期报错；不可达检测（跟在 catch-all 后、重复规则）；
  `!pattern` 否定规则；每请求堆分配换栈缓冲（桶名 ≤63B）。key 前缀路由/正则
  **刻意不做**：桶级操作无法跨后端聚合，一个桶横跨两后端破坏原子性语义（取舍
  注释在头文件）。
- **validate_bucket_name**：补齐 IPv4 形式、`.-`/`-.`、保留前缀
  （xn--/sthree-/amzn-s3-demo-）与后缀（-s3alias/--ol-s3/--x-s3/.mrap）。
- **tiered quota 增量维护**：PUT/DELETE 就地增减估算（stat 拿旧尺寸算净差），
  超高水位提前踢 scan；估算漂移只偏保守方向，每轮 scan 以实测校准。

## 7. [✅已修复] 运维与工程能力缺口

> 本节**已于 2026-08-12 全部处理完毕**：表格下方为逐行的实际修法。
> 顺带修复了一条 §6.3 遗留回归（校验收紧后 cloudproxy 的保留桶远端名非法，
> "cloudproxy 当默认后端 + 动态凭证"启动必炸，见下方最后一条）。

| 缺口 | 说明 |
| --- | --- |
| **无 TLS/HTTPS**（高） | 四驱动均无，`HttpConfig` 也没有承接字段。而 SigV4 的 `UNSIGNED-PAYLOAD` 路径的完整性完全依赖传输层 TLS，文档自己也这么论证——作为服务端却只能跑明文，该论断在入站方向不成立。项目已链 OpenSSL，httplib 已定义 `CPPHTTPLIB_OPENSSL_SUPPORT`（客户端侧），接入成本不高。建议先在 httplib/beast 落地，builtin/seastar 标注不支持并在配置了 TLS 时直接抛错，避免"配了但静默跑明文" |
| **无 CI 配置** | 仓库无 `.github/`、无任何 CI 描述文件。四个构建变体（主/TSan/sqlite/redis）+ 12 个 ctest 套件目前全靠人工在本地跑 |
| **localfs / xlocalfs / tiered 三个后端零指标** | 只有 duostore 与 cloudproxy 接了 `MetricsScope`——**默认部署的后端反而没有任何可观测性** |
| **线程池等待直方图采集了却从不输出** | `thread_pool.cc:83` 写入、`s3/metrics.cc:73-81` 只渲染三个 gauge。而 `docs/concurrency.md §3.1` 把"等待时长直方图右移"作为开启 per-backend 独立池的**唯一判据**——文档推荐的容量决策流程当前无法执行 |
| **入口限流的排队深度不可观测** | `AsyncSemaphore::available()/waiting()` 零调用者。`inflight` 是全进程唯一准入闸门，排了多少人、还剩多少额度，`/-/metrics` 一个字都没有——压测时无法区分"卡在准入"还是"卡在池" |
| **关停挂死无线索** | `BackgroundTaskGroup::wait_idle()` 是无超时、无日志、无计数暴露的裸 `cv_.wait`；`TimerQueue` 无可控停机语义（停机后 `add()` 仍返回"有效但永不触发"的 id） |
| **定时器线程无耗时观测** | 单线程串行执行全部回调，回调超时会连锁推迟 tiered 扫描 / duostore GC / 凭证同步，而目前没有任何指标或日志能发现"定时器被某个回调堵了 3 秒" |
| **无字节数与 per-bucket 维度指标** | 无 bytes in/out 计数器，无按 bucket 的请求分布 |
| **`http.io_threads` 语义随驱动漂移** | beast/httplib/seastar 消费它，**默认驱动 builtin 完全忽略**（thread-per-connection）。用户改了没效果且无任何提示 |
| **停机/背压参数全部硬编码** | drain 上限 4MiB、宽限 10s、trailer 16KiB、块大小 64KiB、连接上限 4096、队列容量 256KiB 散落四个驱动，改要重编且容易漏（连接上限漏改就是例子） |

**✅已修复**（2026-08-12，逐行）：

- **TLS/HTTPS**：`http.tls_cert` + `http.tls_key`（PEM，两个都给才启用，只给一个
  配置期报错）。httplib 走上游 `SSLServer`；beast 把会话循环模板化，明文与
  `asio::ssl::stream`（握手/close_notify/底层超时齐备）共用同一份逻辑；
  builtin/seastar 配置了 TLS 在**构造期直接抛错**——绝不"配了但静默跑明文"。
  证书加载失败同样启动期抛。CMake 侧 `CPPHTTPLIB_OPENSSL_SUPPORT` 与
  `OpenSSL::SSL` 随任一 httplib 消费者（驱动或 cloudproxy）目标级统一定义
  （ODR 约束不变）。测试内嵌百年自签证书：TLS 往返（GET+流式 PUT）、明文客户
  端打 TLS 端口必须握手失败、不支持驱动必须抛、坏证书必须抛，四用例齐备。
- **CI**：`.github/workflows/ci.yml` 三变体矩阵（default / TSan / sqlite+redis
  同编译），直接复用 `build.sh`（子模块按需 init 的逻辑不重写一份），ccache
  按变体缓存对付 rocksdb 冷编译。seastar/tikv/rados 依赖重型系统工具链或外部
  集群，**刻意保留**在本地按需验证（对应 e2e 在无依赖时本就 SKIP）。
- **三后端零指标**：localfs/xlocalfs 接 `lights3_localfs_ops_total` /
  `_op_errors_total`（op 八维全量预注册）+ `_op_seconds`（仅 put/get/list——
  覆盖写盘/读盘/遍历三类代价形态即可定位盘退化，其余 op 同形不膨胀输出）；
  埋点走协程帧内 RAII guard，异常展开自动计 error。xlocalfs 共用基类实例
  （覆写入口重埋，不双计），io_uring 回落时 `lights3_xlocalfs_uring_fallback`
  gauge 常驻置 1（此前只有启动日志一行，滚动即失踪）。tiered 接
  `lights3_tiered_ops_total`/`_op_errors_total`（四个有分层逻辑的 op，纯委托
  路径由 local 的指标覆盖）+ `get_source_total{local|cloud}`（缓存命中率即
  tiered 最核心健康信号）+ demoted/promoted 计数（计在提交点，崩溃恢复补账
  不虚高）+ `scan_seconds` 直方图 + GC 四项（runs/removed/failed 计数，
  deferred 为本轮观测 gauge，取舍同 duostore 的 skipped 类）。
- **线程池等待直方图**：补 `wait_sum_us` 后以标准 histogram 渲染进 `/-/metrics`
  （`lights3_pool_wait_seconds`，桶界 1ms/10ms/100ms/1s）——concurrency.md §3.1
  的专属池判据从此可执行。
- **入口限流可观测**：`lights3_admission_capacity/available/waiting` 三 gauge
  （main 装配期注入 `AsyncSemaphore::available()/waiting()`），压测时"卡在
  准入还是卡在池"一眼可辨。
- **关停挂死**：`wait_idle()` 语义仍是无限等（强杀在途任务只会换来 UAF），但
  每 10s WARN 打出组名与剩余任务数；`TimerQueue` 停机后 `add()` 返回 0 并
  WARN（0 与 `cancel(0)` 的 no-op 约定闭环），不再发"有效但永不触发"的 id。
- **定时器耗时观测**：回调执行时长直方图（`lights3_timer_callback_seconds`）、
  超 1s 慢回调计数 + 逐次 WARN、`lights3_timer_lag_seconds` 队头滞后 gauge
  （"定时器被某个回调堵了 3 秒"直接读数）、pending/due 深度。
- **字节数与 per-bucket**：`lights3_bytes_total{direction}` 全局 +
  `lights3_bucket_requests_total` / `lights3_bucket_bytes_total{bucket,direction}`。
  计数经 dispatch 内外层的 CountingBodyReader 装饰器——入向计 handler 实际
  消费的 payload 字节，出向计驱动实际拉走的字节（流式响应写出发生在 dispatch
  返回之后，只有装饰器能看到）。bucket 标签基数上限 512，溢出并入 `_other`。
- **io_threads 漂移**：解析器打 `io_threads_set` 标记，builtin 在显式配置时
  启动 WARN（并提示并发实际由 max_connections 决定）；示例配置逐驱动注明语义。
- **停机/背压参数配置化**：`drain_limit` / `trailer_max_size` / `io_chunk_size` /
  `body_queue_cap`（httplib 背压水位）/ `shutdown_grace` / `shutdown_force_wait`
  进 `HttpConfig`（默认值即旧常量，带范围校验），四驱动改读配置；连接上限
  §2.13 已修。`kScratchBytes` 等纯内部缓冲**刻意保留**为常量——不是运维会调
  的量，配置面每多一个键都是认知成本。
- **§6.3 遗留回归（顺带修复）**：校验收紧（`-.`/`.-` 相邻非法）后，cloudproxy
  把保留桶 `.sys` 拼成 `<prefix>.sys`（如 `e2e-.sys`）被远端拒绝——真 AWS 同样
  会拒，即"cloudproxy 当默认后端 + 动态凭证"组合启动必炸（e2e_cloudproxy 在
  main 上稳定红）。修法：`remote_bucket()` 咽喉点把 `.sys` 恒定转写为
  `lights3-sys`，该名字对本后端保留（用户桶恰叫此名会与远端凭证桶合流，读得
  到 .sys 对象即凭证泄漏，入口拒绝）；`list_buckets` 反向还原为 `.sys` 交由
  L2 的保留桶过滤，不冒充用户桶出现在列表里。

---

# 第三部分 · 文档与实现的偏差

以下是文档写了、实现没做到的地方。修法分两类：**改实现**（功能确实该有）与**改文档**（承诺本就不该给）。

| 文档位置 | 承诺 | 实际 | 建议 |
| --- | --- | --- | --- |
| `storage-backend.md:148-150` | localfs LIST 做 prefix 剪枝与 delimiter 免展开 | 一律全桶遍历 + 全排序（2.7） | 改实现 |
| `concurrency.md §5` | 取消源有三：断连 / 超时 / shutdown | 三者均未接线（3.1） | 改实现，或改文档承认不做 |
| `concurrency.md §3.1` | 用等待时长直方图判断是否开专属池 | 直方图采集了但从不输出（第 7 节） | 改实现 |
| `concurrency.md §6` | 入口限流是"最底层第二道闸门" | permit 在响应体流式传输**开始之前**就已归还（`main.cc:76-80`），不约束并发流式读 | 改实现：把 Permit 移进 stream_body 生命期 |
| `s3-protocol.md:15-17` | 不支持的功能返回 `NotImplemented` | 只对 query 子资源做到，SSE/tagging/object-lock/ACL 的**请求头**静默吞掉返回 200（3.4） | 改实现 |
| `s3-protocol.md:93-95` | 需解析请求 XML 的三处含 CreateBucket 的 LocationConstraint | 零解析（5.4） | 改实现或改文档 |
| `s3-protocol.md:126-130` | If-Match / If-None-Match 是"对客户端的承诺" | 多实例下不原子（3.6） | 短期改文档标注限制，中期改实现 |
| `s3-protocol.md:150-151` | `/-/` 前缀不与合法 bucket 名冲突 | 对 path-style 成立，vhost 下 `req.path` 是 key，内部端点遮蔽合法 key（3.8） | 改实现 |
| `http-adapter.md §3.1` | Expect: 100-continue 延迟应答，认证失败可不收 body | httplib 驱动是立即应答（2.13），且该分歧被"测进"了契约 | 改实现，或在文档显式声明为该驱动的已知降级 |
| `http-adapter.md §4` | 四驱动接受/拒绝的请求集合一致 | httplib 忽略 `max_header_size`（硬编码 8KiB）、只注册 6 个方法；连接上限只有 builtin 有；IPv6 只有 beast/httplib 支持（3.9） | 改实现 |
| `http-adapter.md §3` | 驱动清单 3.1 Beast / 3.2 httplib / 3.3 Seastar / 3.4 CivetWeb | **默认驱动 builtin 未被文档收录**；`builtin_server.cc:2` 与 `seastar_server.cc:1` 的章节号引用还错位 | 改文档：补 §3.0 描述 builtin 的定位与限制，修正两处注释 |
| `tiered-storage.md:126` | quota "遍历累计、增量维护" | 只有遍历累计（6.2） | 改实现或改文档 |
| `credential-management.md §2` | 查询返回 SK 是"能力上无法避免" | 该论证对动态凭证成立，对**静态 root 凭证**不成立（5.10） | 改实现 |
| `credential-management.md §7` | 吊销不影响已通过验签的在途请求 | 实际是 policy **完全消失**而非按原 policy 继续（3.7） | 改实现 |
| `http-adapter.md §1` | （未提及 trailer 限制） | HTTP/1.1 chunked trailer 在 L1 被静默丢弃，模型无承接字段 | 改文档：写明 trailer 不进中立模型（S3 校验和走 body 内 aws-chunked） |

### 8. [✅已修复] 中英文档漂移

> 本节**已于 2026-08-12 全部处理完毕**：表格下方为逐行的实际修法。

2026-08-02 的 localfs 提交模型修复只改了中文 `storage-backend.md`，英文版与 `object-read-write-flow.md` 都没跟上。漂移集中在这一对文件加 README 的几条次级问题；其余 15 对文件的章节结构、阶段状态表、指标名、yaml 键与默认值经逐格核对无差异。

| 位置 | 问题 | 严重度 |
| --- | --- | --- |
| `docs/en/storage-backend.md:134-135` | 写着 `meta first, then data`——**正是被判定会造成静默损坏的旧实现**（中文版已改为"先数据后 sidecar"，`fs_util.cc:172-174` 的注释直接点名这一反序是静默损坏的成因）。照英文文档移植或新写后端会重新引入该缺陷 | **高** |
| `docs/en/storage-backend.md` §3.1 | 整段缺失中文版新增的三条要点：元数据随数据 xattr 同批提交、提交段 per-key 锁、fsync 持久性。其中 **`LIGHTS3_FSYNC` 开关在英文文档里 0 处提及**（中文 1 处 + 代码 2 处）——这是唯一一处"一方有配置开关另一方完全没有"的漂移，且因为它是环境变量而非 yaml 键，机械比对抓不到 | **高** |
| `docs/en/storage-backend.md:144-146` | GET 段仍写 `open + fstat + read sidecar`，缺 xattr 优先与"绝不对路径二次 stat"的铁律（对应 2.2 的修复） | 中 |
| `docs/object-read-write-flow.md:109-117` 与 `docs/en/*:130-143` | **中英双方**都还写着"sidecar 先于数据文件落位"，与同仓 `storage-backend.md:129` 直接矛盾 | 中 |
| `docs/README.md:71-72` | 称 sidecar 用「`.meta` **JSON**」，实际是 `.lights3-meta` **TSV**（`fs_util.h:19`，`storage-backend.md:118` 自己就写对了）。英文 README 因措辞含糊反而没错 | 中 |
| 全仓 16 处 | `docs/todo.md` 与 `docs/en/todo.md` 已删除，但正文引用残留 ZH 8 处 + EN 8 处（concurrency / architecture / cloudproxy-backend×2 / storage-backend×2 / s3-protocol / duostore-tikv-meta，中英各一份） | 低 |
| `docs/README.zh-CN.md` | 缺仓库根 `README.md:184-205` 那段文档索引表；其余技术事实逐项一致 | 低 |

**建议**：以 `docs/storage-backend.md` §3.1 为唯一参照，一次性同步 `docs/en/storage-backend.md` 与中英两份 `object-read-write-flow.md`；`todo.md` 的残留引用统一改为自述式描述。

**✅已修复**（2026-08-12，逐行）：

- **EN `meta first, then data`**：写原子性小节整段以中文 §3.1 为参照重译——
  删除反序表述，补齐三条子要点（xattr 同批提交 / 提交段 per-key 锁 /
  fdatasync+目录 fsync 持久性，含 **`LIGHTS3_FSYNC` 开关**的首次英文收录）。
- **EN GET 段**：补 xattr 优先回落 sidecar 与"size/mtime 恒取已开 fd 的
  fstat、绝不对路径二次 stat"的铁律及其错位后果。
- **中英 `object-read-write-flow.md`**：②提交框图按 `fs_util.cc:
  commit_object_file` 现实现重写（xattr → fdatasync → rename → fsync 父目录 →
  sidecar）；"sidecar 先于数据文件落位"要点改为"先数据后 sidecar"并给出反序
  即静默损坏的论证。同文件另清出三处表格未列的同源残留：GET 段 `load_meta`
  描述（补 fd fstat + xattr 优先）、全链路示意图的 `sidecar → rename` 标注、
  末尾设计要点回顾的 "sidecar-先行"——中英各一份，共六处。
- **`docs/README.md` sidecar 描述**：「`.meta` JSON」改为 `.lights3-meta` TSV
  （`fs_util.h:kSidecarSuffix`），并注明 xattr 为主、sidecar 兼作外部工具
  可读与回落。
- **todo.md 残留引用**：docs 16 处全部改为自述式描述或指向现存文档的等价
  锚点（如 per-backend 池 → `concurrency.md` §3.1）；顺带清理表格未计的
  **src/ 与 tests/ 注释里的 17 处**同源残留（含两处顺手改错的锚点：多段
  Range → `s3-protocol.md` §6、presigned 时钟偏移 → §3.4，均经与目标文档
  逐节核对）。全仓 `todo.md` 引用现仅存 gaps.md 本节的历史记录。
- **`docs/README.zh-CN.md`**：按根 `README.md` Documentation 段补齐同款
  14 行文档索引表（链接改为 docs/ 目录内相对路径）。

---

## 修复排期建议

按依赖关系而非单纯严重度排：

1. ~~**先修 P0 的四条**（1.1–1.4）。其中 1.1 的三处改动必须同批上线（单改一处不构成防线）。~~ ✅ 2026-08-03 完成。
2. ~~**2.1 + 2.2 一起修**——两条都是"后台线程/关停期异常防线"，成本极低、收益立竿见影，且 2.2 的 TimerQueue 次生问题会让 2.1 的修复更难验证。~~ ✅ 2026-08-06 完成（§2 整节同批）。
3. ~~**3.1 / 3.2 / `AsyncSemaphore` 取消是同一条链**（2.9 已单独修完）：接线取消之前必须先修 3.2（回调不在触发线程跑续体）与信号量取消支持（§2.13 只做了析构断言，`close()` 留在这里），否则接上 `with_timeout` 反而引入"定时器线程跑请求"的新故障模式。~~ ✅ 2026-08-07/08 完成（§3 整节，§4 随后）。
   ~~**下一站是 §5.1 的分页缺失**~~ ✅ 2026-08-09 完成（§5 整节）。~~**当前下一站是 §6 存储引擎能力缺口**~~ ✅ 2026-08-11 完成（§6 整节）。~~**当前下一站是 §7 运维与工程能力缺口**~~ ✅ 2026-08-12 完成（§7 整节 + §6.3 的 cloudproxy 保留桶回归）。~~剩余：第三部分"文档与实现的偏差"与 §8 中英文档漂移~~ §8 已于 2026-08-12 完成。**剩余：第三部分的 15 条对照表**。
4. ~~**6.1 的 pack 老化/预算**——2.3 的存活账口径与压实判据已改，续做时以现判据为基线，避免再次推翻测试。~~ ✅ 2026-08-10 完成（以现判据为基线，见 §6.1）。
5. ~~**3.5 的白名单反转**一次性消掉 5.3、3.4 的一半与未来所有子资源漂移，性价比最高。~~ ✅ 已完成；5.3 随 §5 实现后，白名单也已按新语义放行 `response-*`。
6. ~~第 7 节的工程能力（CI、TLS、后端指标）不阻塞正确性修复，可并行推进。~~ ✅ 2026-08-12 完成。
7. §2.13 的 httplib `Expect: 100-continue` 留有上游 API 限制（无法延迟应答），如需彻底解决要么换驱动、要么向 cpp-httplib 提 PR。
