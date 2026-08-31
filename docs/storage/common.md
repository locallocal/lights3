# 存储公共层实现

本文档覆盖 `src/storage/` 顶层的公共代码：后端接口、注册与构建、bucket 路由、
以及所有后端共享的校验/列举/分片辅助件。设计动机与各后端的具体实现见
[storage-backend.md](../storage-backend.md)；本文只讲实现层的契约与算法。

## 1. 定位与文件清单

存储公共层是 L2（S3 处理器）与 L3（存储引擎）的边界。核心原则写在
`storage/backend.h` 头两行注释里：**后端抛 `s3::S3Error`、不感知 HTTP**；
所有接口返回 `Task<T>` 协程（见 [concurrency.md](../concurrency.md) §2）。

| 文件 | 内容 |
|---|---|
| `src/storage/backend.h` | `IStorageBackend` 接口、全部公共数据结构、校验函数声明 |
| `src/storage/registry.h/.cc` | `StorageRegistry`：type 字符串 → 工厂，按配置批量构建后端 |
| `src/storage/bucket_router.h/.cc` | `BucketRouter`：bucket 名 glob 匹配 → 后端实例 |
| `src/storage/validate.cc` | bucket/key 命名校验实现（声明在 `backend.h`，无独立头文件） |
| `src/storage/listing.h/.cc` | ListObjects / ListParts / ListMultipartUploads 的共享分页与 delimiter 算法；`resolve_range` 的实现也在此 |
| `src/storage/multipart.h/.cc` | upload_id 生成、合并 ETag、parts 预校验等后端无关的分片辅助 |

## 2. IStorageBackend 接口逐方法语义

接口定义：`storage/backend.h: IStorageBackend`。除两处带默认实现
（`copy_object_fast` 返回 `nullopt`、`close` 空操作）外全部为纯虚。

### 2.1 bucket 操作

| 方法 | 契约 |
|---|---|
| `create_bucket` | 创建 bucket；已存在时抛 `BucketAlreadyOwnedByYou`/等价错误（由各后端实现决定） |
| `delete_bucket` | **bucket 必须为空**，否则抛错；不存在抛 `NoSuchBucket` |
| `bucket_exists` | 存在性探测，不抛"不存在"类错误 |
| `list_buckets` | 返回本后端全部 `BucketInfo`（名字 + 创建时间）；跨后端聚合由上层完成 |

### 2.2 object 数据面

**`get_object(bucket, key, range)`** — 返回 `ObjectStream`：`meta.size`
恒为**完整对象长度**，`body` 已按 range 裁剪，`range` 字段回填生效区间
（后缀式已解析、越界已钳位）。range 不可满足时按
`storage/backend.h: resolve_range` 的约定抛 `InvalidRange`(416)。

**`put_object(bucket, key, meta, body, cond)`** — body 契约（`upload_part`
同样适用），这是接口上最重要的一条：

- 实现必须把 `body` **读到 EOF**（`read` 返回 0），而不是读满 `length()`
  字节就停。上层的校验装饰器（`x-amz-content-sha256` / aws-chunked 签名链）
  挂在"读满 + 触到 EOF"这一时刻，少读尾部就等于跳过校验。
- `body.read` 抛异常 ⇒ 后端**不得提交**该对象（丢弃 staging / 中止远端传输）。
- `cond.active()` 时按 `PutCondition` 契约在**后端自身的原子提交点**校验
  （见 §3.3）。

**`head_object`** — 只取元数据；不存在抛 `NoSuchKey`。

**`copy_object_fast(src_bucket, src_key, dst_bucket, dst_key, meta)`** —
同后端拷贝快路径钩子。仅当源和目的都路由到同一后端时，CopyObject 处理器
才会先尝试它；返回 `nullopt` 表示"没有快路径 / 这次不可用"（tiered 存根、
跨设备等），调用方回退到 `get_object` 流式读 + `put_object` 流式写。
`meta` 是最终元数据（REPLACE 模式由处理器组好；COPY 模式抄自源），其中
key/size/etag 由实现从源填充——字节未变，etag 恒等于源。

**`delete_object`** — S3 语义：key 不存在也返回成功（幂等删除）。

**`list_objects(bucket, opt)`** — 按 `ListOptions` 分页；共享实现见 §6.2。

### 2.3 multipart

| 方法 | 契约 |
|---|---|
| `create_multipart` | 返回 upload_id；`meta` 携带期望的 content_type/user_meta，在 complete 时生效 |
| `upload_part` | `part_no ∈ [1,10000]`；同号重传 last-write-wins；返回该片 ETag（内容 MD5） |
| `complete_multipart` | `parts` 号必须严格递增、ETag 与已上传片一致；总 ETag = `md5(各片二进制 md5 拼接)-N`（与 S3 同规则，实现见 §6.3 `combined_etag`） |
| `abort_multipart` | 丢弃全部已传片 |
| `list_parts` | 按 part_no 升序、如实上报 is_truncated；upload 不存在抛 `NoSuchUpload`。**分页语义由 `storage/listing.h: apply_parts_page` 一处定义**：实现可先把 marker 下推到引擎再交给它收尾，但不得自造截断规则 |
| `list_multipart_uploads` | bucket 的活跃上传，按 (key, upload_id) 升序，如实上报 is_truncated |

**`close()`** — 优雅退出钩子，默认空实现（`co_return`）。

## 3. 核心数据结构

均定义在 `storage/backend.h`。

### 3.1 ByteRange 与 resolve_range

`ByteRange{first, last}` 用两个 `optional` 覆盖 HTTP Range 的三种形态：
`a-b`（都有值）、`a-`（只有 first）、`-n`（只有 last，后缀 n 字节）。

`resolve_range(r, size)`（实现在 `storage/listing.cc`）把它解析成闭区间
`[first, last]`：

1. `size == 0` 直接 416（空对象上任何 range 都不可满足）；
2. 有 `first`：`first >= size` 抛 416；`last` 钳位到 `size-1`；
3. 只有 `last`（后缀式）：`n == 0` 抛 416；`n >= size` 时取整个对象；
4. 两者皆空抛 416。

### 3.2 ObjectMeta 与 kStdMetaFields

`storage/backend.h: ObjectMeta` 字段：

| 字段 | 含义 |
|---|---|
| `key` | 对象键（list 结果中回填） |
| `size` | 字节数 |
| `etag` | hex，**不带引号** |
| `content_type` | 默认 `binary/octet-stream` |
| `last_modified` | 系统时钟时间点 |
| `user_meta` | `x-amz-meta-*` 键值对，**前缀已剥掉** |
| `cache_control` 等 5 个 | 一等 S3 元数据（见下），空串 = 未设置、不回传该头 |

五个一等字段（Cache-Control / Content-Disposition / Content-Encoding /
Content-Language / Expires）由常量表 `storage/backend.h: kStdMetaFields`
统一驱动：每项记录请求/响应头名、持久化键名、`ObjectMeta` 成员指针。
提取、回显、各后端序列化都遍历这张表——新增字段只改这一处，避免
"存了但从不返回"的半吊子状态。`x-amz-storage-class` 有意缺席：本实现只有
STANDARD，存储层回显客户端报的 GLACIER 就是在撒谎，非 STANDARD 在 L2 直接 501。

### 3.3 PutCondition（条件 PUT）

`storage/backend.h: PutCondition`，语义见 [s3-protocol.md](../s3-protocol.md) §6：

- `if_none_match = true`（对应 `If-None-Match: *`）：要求对象**不存在**，
  违反抛 `PreconditionFailed`；
- `if_match_etag`：要求当前 ETag 相等，不匹配抛 `PreconditionFailed`，
  对象不存在抛 `NoSuchKey`；
- 两者互斥（由 L2 保证）；`active()` 判断是否携带任一条件。

关键约束写在结构体注释里：**校验和提交必须同处后端自己的原子提交点**
（提交临界区 / 元数据 CAS）。若在 L2 做 "head 再 put"，窗口内的并发写会同时
破坏防覆盖与乐观并发两种语义，跨实例部署下更是完全不成立。条件失败时后端
必须不留任何写入痕迹。

### 3.4 列举相关结构

- `ListOptions{prefix, delimiter, max_keys, start_after}`：delimiter 为空或
  `"/"`；`start_after` 兼任 continuation-token（值就是一个 key）。
- `ListResult{objects, common_prefixes, is_truncated, next_token}`。
- `ListPartsOptions{max_parts, part_number_marker}`：marker 语义为
  "只返回 `part_no >` 该值的片"，0 = 从头。
- `ListPartsResult`：parts 按 part_no 升序；`next_part_number_marker`
  仅在 `is_truncated` 时有意义。
- `ListUploadsOptions`：`(key_marker, upload_id_marker)` 构成复合游标，只返回
  严格大于该二元组的条目；`upload_id_marker` 为空表示 "key > key_marker"。
- `ListUploadsResult`：uploads 按 (key, upload_id) 升序，带
  `next_key_marker` / `next_upload_id_marker`。

分片列举分页结构的存在理由（`backend.h` 注释，对应 gaps §5.1）：早期版本
返回裸 vector 且 IsTruncated 恒 false，客户端把它当"已到末尾"，5000 个活跃
上传永远只看得见第一页，且单次请求在内存里构建全表。

### 3.5 其余结构

`PutResult{etag}`、`BucketInfo{name, created}`、
`PartInfo{part_no, etag}`（CompleteMultipartUpload 请求项，etag 可能带引号，
比较前用 `strip_etag_quotes` 剥掉）、`PartMeta`（ListParts 结果项，多 size 与
last_modified）、`UploadInfo{key, upload_id, initiated}`。

### 3.6 保留 bucket 与 kAllowReserved

`storage/backend.h: kSysBucketName = ".sys"` 是内部保留 bucket（凭据持久化，
见 [credential-management.md](../credential-management.md) §4.1）。只有
CredentialStore 以 `allow_reserved=true` 调校验才放行；用户请求永远拿不到
这个开关。常量 `storage/backend.h: kAllowReserved` 供各后端内部自校验使用：
后端必须能服务 CredentialStore 对 `.sys` 的读写，所以后端层放行保留名、
只查**路径安全性**；拦截保留名是 L2 dispatch 的职责（恒用默认
`allow_reserved=false`）。

## 4. StorageRegistry 构建流程

### 4.1 注册机制

`storage/registry.h: BackendFactory` 签名为
`(const BackendConfig&, shared_ptr<ThreadPool>, MetricsScope) → shared_ptr<IStorageBackend>`。
`BackendConfig`（`core/config.h`）只有 `name`/`type`/`params`（string→string
map），参数解释权在各后端。注册表本体是 `registry.cc` 匿名命名空间里的
函数级 static map；`ensure_registered` 用 static-init 惰性注册内建类型：

| type | 构建要点（`storage/registry.cc: ensure_registered`） |
|---|---|
| `localfs` | `fs_backend_paths` 解析 `root`（必填，缺失抛 runtime_error）与 `staging`（默认 `<root>/.lights3-staging`）；`fs_backend_opts` 解析 `mpu_ttl` / `mpu_scan_interval` |
| `xlocalfs` | 同上，另解析 `queue_depth` / `sqpoll` / `sqpoll_idle` / `rings` / `fixed_buffers` / `fixed_files` / `block_size` / `read_depth` / `write_depth` / `meta_ops` 组成 `UringOptions`（详表见 [xlocalfs.md](xlocalfs.md) §8）。构造抛异常（老内核、seccomp 拦 `io_uring_setup`、memlock 配额不足）时**降级为 LocalFsBackend**——两者磁盘布局与元数据语义完全一致，降级无损、只失去异步 IO；同时 `LOG_WARN` 并置常驻 gauge `lights3_xlocalfs_uring_fallback=1`，让"以为在跑异步 IO 实则回退"在监控面可见 |
| `memory` | `max_bytes`（`parse_size`）、`mpu_ttl`；注册回调 gauge `lights3_memory_backend_used_bytes`（经 `weak_ptr` 读 `used_bytes()`，后端析构后返回 0） |
| `cloudproxy` | 编译开关 `LIGHTS3_CLOUDPROXY`；`CloudProxyConfig::from_params` 解析 |
| `duostore` | 编译开关 `LIGHTS3_DUOSTORE`；`DuoStoreConfig::from_params` 解析 |
| `tiered` | 不走注册表，`build` 内特判延迟构建（见下） |

第三方扩展路径：调用 `StorageRegistry::register_backend(type, factory)`
（[storage-backend.md](../storage-backend.md) §6）。

### 4.2 build 的两阶段与回滚

`storage/registry.cc: StorageRegistry::build` 步骤：

1. **重名检查**：backend name 重复直接抛。
2. **装回滚守卫** `BuildRollback`：中途失败时，已建后端各自持有专属
   ThreadPool（线程已启动）和一组 gauge 回调（闭包持有 pool 的 shared_ptr）。
   守卫析构时对每个已注册 scope 调 `MetricsRegistry::remove_labeled("backend", name)`
   清指标，再 `out.clear()` 触发后端析构 → 池 join，避免孤儿线程与读陈旧
   stats 的回调。`metrics` 可为 null（单测装配路径跳过注册表）。
3. **第一阶段（叶子后端）**：逐条配置，`tiered` 推入 `deferred`；其余按
   type 查工厂（未知 type 抛错），为每个实例建
   `MetricsScope(metrics, {{"backend", cfg.name}})`，调工厂构建。
4. **第二阶段（复合后端）**：`deferred` 上做不动点迭代——某条 tiered 的
   `local`/`cloud` 引用的名字都已就绪时即可构建（`TieredBackend::from_config`），
   tiered 嵌套 tiered 按依赖序解开；一轮无进展仍有剩余 ⇒ 环或未知引用，
   报配置错误。注意 tiered 分支**先注册 scope 再构造**：若构造抛出，其 gauge
   回调可能已注册并持有池的 shared_ptr，漏登记会导致回滚后线程永不 join。
5. `rollback.done = true` 解除守卫，返回 name→instance 表。

### 4.3 专属 IO 池（io_threads）

`storage/registry.cc: backend_pool` 兑现 [concurrency.md](../concurrency.md)
§3.1 的预留：任何 type 的 params 带 `io_threads`（整数 ∈ [1,1024]，越界抛错）
即获得专属 ThreadPool——慢的云端请求灌满共享池会饿死本地盘路径，按后端隔离
池互不拖累；默认仍是共享全局池（多数部署的正确选择，隔离是"确认饥饿症状后
再开"的定向手段）。生命周期：后端持有 shared_ptr，析构即 join；指标回调闭包
也持一份，`stats()` 在注册表存续期内始终可安全调用。挂载的四个带 backend
标签的回调 gauge：`lights3_backend_pool_threads` / `_queue_depth` /
`_backlogged` / `_completed`，与全局池的 `lights3_pool_*`（无标签渲染）
命名空间分开，避免同名 TYPE 行重复。

## 5. BucketRouter 路由

### 5.1 数据结构与 resolve

`storage/bucket_router.h: BucketRouter` 持有：规则表
`Rule{glob, negate, backend}`、`default_`、全量 `backends_` 表。
`resolve(bucket)` 按声明顺序线性扫描，`glob_match(rule.glob, bucket) != rule.negate`
即命中（异或实现取反规则），全不命中落到 `default_`。纯静态无锁读；
配置不支持热加载（重启生效）。`glob_match` 用 `::fnmatch`，bucket 名 ≤63B
（由 `validate_bucket_name` 保证），用 64B 栈缓冲拷出 NUL 结尾串，
避免每请求一次堆分配。

`default_backend()`：内部数据（凭据持久化的 `.sys`）恒落默认后端。
有意不提供 key 前缀路由：bucket 级操作（list/delete-bucket）无法跨后端聚合，
一个 bucket 横跨两个后端会破坏原子性与一致性语义。

### 5.2 build 时的规则校验

`storage/bucket_router.cc: BucketRouter::build` 把所有能在启动期发现的错误
都拒在启动期——手滑的 pattern 静默永不匹配会把 bucket 悄悄路由到默认后端，
数据落错引擎后迁移就是一次全量拷贝：

1. `!` 前缀识别为取反规则（"不匹配 pattern 的 bucket 命中此规则"）；
2. `validate_glob`：fnmatch 对坏 pattern 只报"不匹配"不报错，所以自查两类
   build 期可判定的错误——未闭合的 `[` 字符类（含 `[]a]` 形式的特判：紧跟
   `[` 的 `]` 是字面成员不闭类）；pattern 中出现 bucket 名字符集
   （小写/数字/`-`/`.`）之外的字面字符（大写、`_`、`/` 等），合法 bucket 名
   永远匹配不上；
3. 不可达检查：catch-all 之后的规则必不可达，抛错（多半是把默认规则写在了
   前面）。catch-all 判定包括 `*`/`**`，以及**不含任何通配符的取反规则**
   （`!fixed-string` 匹配除一个名字外的所有 bucket，本身就是 catch-all）；
4. 与先前规则 (glob, negate) 完全重复 ⇒ 不可达，抛错；
5. 规则与 `default_backend` 引用的后端名必须存在于传入表中，否则抛错。

## 6. 公共辅助件

### 6.1 命名校验（validate.cc）

三个函数声明在 `storage/backend.h`，实现在 `storage/validate.cc`。bucket
校验是保留名与路径安全的**唯一防线**：L2 dispatch 对每个请求在路由前调用
（`allow_reserved=false`），各后端数据面入口再调一次作纵深防御。

**`validate_bucket_name(b, allow_reserved)`** — 违规抛
`InvalidBucketName`。规则依次：

1. `.sys` 特判（§3.6）：`allow_reserved` 才放行；早期版本无条件放行，全靠
   L2 的 `.` 前缀启发式，而 vhost 寻址（bucket 完全由 Host 决定）可绕过它；
2. 长度 ∈ [3,63]；字符集小写/数字/`-`/`.`；首尾不得为 `-` 或 `.`；无 `..`；
3. AWS 补齐三条（早期缺失，此处放行的名字在真 S3 上建不出来，"先在 lights3
   跑通再迁 S3"会在最后一刻断掉）：无 `.-` / `-.`；不得形如 IPv4
   （`validate.cc: looks_like_ipv4`：恰好四段、每段 1–3 位数字且 ≤255，
   仅容忍前导零）；保留前缀 `xn--`（IDNA punycode）、`sthree-`、
   `amzn-s3-demo-` 与保留后缀 `-s3alias`、`--ol-s3`、`--x-s3`、`.mrap`
   （access point / MRAP 别名）。

**`validate_object_key(k)`** — 所有后端通用的 AWS 约束：非空且 ≤1024B
（违规 `KeyTooLongError`）；拒绝控制字符含 NUL 与 0x7f（XML 1.0 连数字实体
都表示不了 0x00–0x1F，一个这样的对象会让整个 ListObjects 响应对合规解析器
不可解析）；逐段扫描拒绝 `.` / `..` 段——这不只是本地路径问题：转发型后端把
key 拼进 URL path，RFC 3986 的 remove_dot_segments 允许途中任何代理把
`a/./b` 规范化成 `a/b`，改写对象身份本身。

**`validate_fs_object_key(k)`** — 仅路径映射型后端（localfs/xlocalfs）
追加调用，且**必须先过 `validate_object_key`**：拒绝首字符 `/`（逃逸 bucket
目录）、空段（无对应文件名）、单段 >255B（NAME_MAX，落盘必
ENAMETOOLONG）。**尾部 `/` 是唯一例外**：目录标记对象由 localfs 用目录内的
保留 marker 文件表示——这是真支持而非"先收下再失败"；S3 控制台的"创建文件夹"
和 s3fs/goofys/rclone 的目录语义都依赖此形态。实现上先剥尾 `/` 再逐段查，
剥完必须有剩余（纯 `"/"` 已被首字符规则挡住）。

早期共享层还拒绝首 `/`、空段、目录标记、255B 段——三条源自 localfs 路径映射
的规则连带剥夺了 memory/duostore/cloudproxy 的兼容性，现已下沉到
`validate_fs_object_key`。

### 6.2 列举分页（listing.h/.cc）

**`apply_listing(sorted_keys, opt, fetch)`** — ListObjects 的共享实现
（memory 与 localfs 复用）。前置条件：keys 已按字典序排好；`fetch` 只对
最终进入结果的 key 调用（省掉被 delimiter 折叠的条目的元数据读取）。流程：

1. `max_keys <= 0` 返回空且 `is_truncated=false`——否则"空 token + truncated"
   会把按 IsTruncated 循环的客户端送进死循环；
2. 定位起点：`lower_bound(prefix)`，再 `upper_bound(start_after)`
   （token 语义为严格大于）;
3. 顺序扫描，一旦 key 不再以 prefix 开头即停（有序，越过前缀区间）；
4. 计数达 `max_keys` 时置 `is_truncated`，`next_token = 最后一个产出条目的 key`；
5. delimiter 非空且 key 在 prefix 之后含 delimiter：截到 delimiter（含）作为
   group 推入 `common_prefixes`，计 1 个名额，随后**内层快进跳过同组的其余
   key**，`last_emitted_key` 落在**组内最后一个 key** 上——若用组名自身作
   游标，`"a/" < "a/x"`，下一页会把整组重列一遍；
6. 否则 `fetch(key)` 推入 `objects`。

CommonPrefix 与对象条目共享同一个 `max_keys` 名额池（与 S3 一致）。

**`apply_parts_page(sorted, opt)`** — 输入按 part_no 升序。跳过
`part_no <= part_number_marker` 的片（marker 为严格大于）；攒满 `max_parts`
时置 truncated，`next_part_number_marker = 已返回最后一片的 part_no`。
`max_parts <= 0` 同样返回空 + 不截断。这是全部后端 ListParts 截断语义的
**单一定义点**；能把 marker 下推到引擎（sqlite/rocks/tikv）的后端可以先下推、
取 max+1 条再交给它收尾，语义完全一致。

**`apply_uploads_page(sorted, opt)`** — 输入按 (key, upload_id) 升序。
每条依次过 prefix 过滤与 `storage/listing.h: upload_after_marker`
（复合游标比较：两 marker 皆空放行一切；key 不等比 key；同 key 下
`upload_id_marker` 为空表示"该 key 已整页翻过"，返回 false）。delimiter
分组与 `apply_listing` 同法，游标同样落在组尾条目的 (key, upload_id) 上。
名额按 `uploads.size() + common_prefixes.size()` 合计。未截断时结尾清空
`next_key_marker` / `next_upload_id_marker`（S3 仅在 IsTruncated 时提供）。

### 6.3 分片辅助（multipart.h/.cc）

- `new_upload_id()`：`getentropy` 取 16 字节转 32 个小写 hex。必须用 CSPRNG：
  upload_id 直接回给客户端，是 abort/complete 他人上传的**唯一凭据**；
  mt19937_64 的内部状态可从约 2496 个输出恢复，`random_device` 种子只有
  32 位熵——可预测即可枚举/伪造。取熵失败抛 `InternalError`。
- `is_valid_upload_id(id)`：恰 32 字符且均为 `[0-9a-f]`；后端在触盘/触库前
  用它挡掉畸形 id（localfs 还把 id 直接用作 mpu 目录名）。
- `combined_etag(part_md5_hex)`：S3 合并 ETag 规则——把每片的 hex MD5 还原为
  二进制拼接，再整体 MD5，`hex + "-" + N`。
- `strip_etag_quotes(etag)`：剥外层双引号（客户端可能发 W3C 引号形式），
  比较前统一。
- `validate_part_order(parts)`：complete 预检——空列表抛 `InvalidPart`；
  part 号非严格递增抛 `InvalidPartOrder`（专用错误码：`InvalidPart` 意为
  "这一片坏了"，会诱导客户端重传该片，而实际需要的是排序后重提交）。
- `validate_part_number(part_no)`：不在 [1,10000] 抛 `InvalidArgument`。
- 常量 `kMaxParts = 10000`、`kMinPartSize = 5MiB`：最小片大小只在 complete
  时判且属 S3 协议规则而非存储规则，检查放在 L2（`handlers/multipart.cc`），
  存储层不设限——直接使用后端 API 的调用方（含各后端一致性测试套件）不受
  5MiB 约束。

## 7. 并发与取消约定

- 接口全量返回 `Task<T>`（惰性协程）；实现内部经 `pool_->schedule(token)`
  切到阻塞 IO 池执行文件/网络操作，续体可能在池线程、driver 线程或取消路径
  上恢复——**L2/L3 代码必须线程亲和无关**
  （[concurrency.md](../concurrency.md) §1）。
- 取消是协作式的（`CancelToken`）：排队中被取消立即以 `OperationCancelled`
  异常 resume；已在池线程执行的阻塞段跑完当段后在下个挂起点感知
  （[concurrency.md](../concurrency.md) §5）。
- put/upload_part 的失败路径（body.read 抛出，含取消异常）与"不提交"契约
  （§2.2）共同保证半成品不可见。

## 8. 指标接入

- `build` 为每个后端实例建一个 `MetricsScope`（`core/metrics.h`），底标签
  `backend=<name>`；工厂按需追加维度（op/code 等），不消费指标的工厂直接
  忽略。默认构造的空 scope 返回未注册的孤立实例、调用无害，测试直接构造
  后端无需接线（对应 `StorageRegistry::build` 的 `metrics = nullptr` 路径）。
- 同标签重复构造 scope 幂等（注册表 get-or-create），所以 tiered 的池指标与
  后端自身指标共用同一 scope 无冲突。
- 回滚路径依赖 `MetricsRegistry::remove_labeled("backend", name)` 成组摘除
  某实例的全部序列（含回调 gauge），否则被弃实例的回调闭包会经 shared_ptr
  钉住对象并持续渲染陈旧值。
- 公共层自身登记的指标：`lights3_backend_pool_*` 四项（§4.3）、
  `lights3_xlocalfs_uring_fallback`、`lights3_memory_backend_used_bytes`
  （§4.1）；后端内部指标接入示例见
  [duostore-backend.md](../duostore-backend.md)。
