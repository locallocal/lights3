# 存储后端

## 1. IStorageBackend 接口

L2 与存储的唯一边界。接口按 S3 语义而非文件语义设计，全部返回 `Task<T>`，
数据面走流式 `BodyReader`（以下为节选）：

```cpp
// src/storage/backend.h
namespace lights3::storage {

struct ObjectMeta {
    uint64_t    size = 0;
    std::string etag;                       // 通常为内容 MD5 的 hex
    std::string content_type;
    std::chrono::system_clock::time_point last_modified;
    std::map<std::string,std::string> user_meta;   // x-amz-meta-*
};

struct ObjectStream {                       // GET 返回
    ObjectMeta meta;
    std::unique_ptr<http::BodyReader> body; // 已按 range 裁剪
};

struct PutResult { std::string etag; };

struct ListResult {
    std::vector<ObjectMeta /*含 key*/> objects;
    std::vector<std::string> common_prefixes;
    bool is_truncated = false;
    std::string next_token;
};

struct IStorageBackend {
    // ---- bucket ----
    virtual Task<void> create_bucket(std::string_view bucket) = 0;
    virtual Task<void> delete_bucket(std::string_view bucket) = 0;   // 须为空
    virtual Task<bool> bucket_exists(std::string_view bucket) = 0;

    // ---- object 数据面 ----
    virtual Task<ObjectStream> get_object(std::string_view bucket,
                                          std::string_view key,
                                          std::optional<ByteRange> range) = 0;
    virtual Task<PutResult>    put_object(std::string_view bucket,
                                          std::string_view key,
                                          ObjectMeta meta,             // 期望的 CT/user_meta
                                          http::BodyReader& body) = 0;
    virtual Task<ObjectMeta>   head_object(std::string_view bucket,
                                           std::string_view key) = 0;
    virtual Task<void>         delete_object(std::string_view bucket,
                                             std::string_view key) = 0;
    virtual Task<ListResult>   list_objects(std::string_view bucket,
                                            const ListOptions& opt) = 0;  // prefix/delimiter/max_keys/token

    // ---- multipart ----
    virtual Task<std::string>  create_multipart(std::string_view bucket,
                                                std::string_view key,
                                                ObjectMeta meta) = 0;      // → upload_id
    virtual Task<PutResult>    upload_part(std::string_view bucket, std::string_view key,
                                           std::string_view upload_id, int part_no,
                                           http::BodyReader& body) = 0;
    virtual Task<PutResult>    complete_multipart(std::string_view bucket, std::string_view key,
                                                  std::string_view upload_id,
                                                  std::span<const PartInfo> parts) = 0;
    virtual Task<void>         abort_multipart(std::string_view bucket, std::string_view key,
                                               std::string_view upload_id) = 0;

    virtual Task<void> close() = 0;    // 优雅退出时冲刷/清理
    virtual ~IStorageBackend() = default;
};

} // namespace
```

（快照为节选：backend.h 另有 `list_buckets()` / `list_parts()` /
`list_multipart_uploads()`；`close()` 有默认空实现而非纯虚。）

错误约定：后端抛 `StorageError{S3ErrorCode, message}`（NoSuchKey、
NoSuchBucket、EntityTooLarge…），L2 的 errors 模块统一映射为 HTTP 响应。
后端不感知 HTTP。

## 2. BucketRouter

```text
resolve(bucket) → IStorageBackend&
```

- 配置驱动：按声明顺序做 glob 匹配（`archive-*` → aws-archive），
  无命中落到 `default_backend`。
- 纯静态、无锁读；配置热加载首期不做（重启生效）。
- ListBuckets 语义：聚合各后端 `list_buckets` 结果，标注归属。
- 有意选择 **bucket 粒度**：路由无须元数据服务、无一致性问题。object 级
  分层/迁移是显式的后续特性，可用"影子 bucket + 复制任务"叠加实现，不改接口。

## 3. LocalFsBackend（本地文件系统）

### 3.1 磁盘布局

```text
<root>/
├── mybucket/                          # bucket = 一级目录
│   ├── .lights3-bucket                # bucket 标记与属性（创建时间等）
│   ├── dir/a.bin                      # object 数据文件，key 即相对路径
│   └── dir/a.bin.lights3-meta         # sidecar：ObjectMeta 的 TSV
<staging>/                             # 与 root 同一文件系统（rename 原子性）
├── put/<pid>-<ts>-<seq>               # PUT 进行中的临时文件
└── mpu/<upload_id>/
    ├── manifest                       # bucket/key/meta/创建时间（TSV）
    └── part.00001 ... part.NNNNN      # 每片伴随同目录 part.NNNNN.md5 sidecar
```

关键决策：

- **key → 路径映射**：`/` 作目录分隔直接落盘，保持人类可读、可用普通工具
  操作。逃逸处理：单段 >255B、含 `.`/`..` 或空段的 key 一律在共享校验层
  （`src/storage/validate.cc`）拒绝，各后端行为一致。
  key 与目录冲突（已存在 `a/b` 再 PUT `a`）返回 S3 兼容错误。
- **sidecar 而非 xattr**：xattr 有大小限制且 scp/rsync 易丢；sidecar TSV
  可靠且可检。list 时按后缀过滤掉 sidecar。
- **写入原子性**：PUT 全部写到 staging 临时文件（边写边算 MD5 作 ETag；
  SHA256 校验属 L2 的验签装饰器，不在后端），校验通过后 `rename()` 到最终
  路径，失败路径统一 unlink 临时文件。并发 PUT 同一 key 采用 last-write-wins
  （与 S3 语义一致），rename 的原子性保证读者看不到半截数据。
  - **元数据与数据同批提交**：元数据（etag/content_type/user_meta/tier）在
    rename 之前写进数据临时文件的扩展属性 `user.lights3.meta`（内容与 sidecar
    同款 TSV），因此**一次 rename 同时提交数据与元数据**——xattr 随 inode 走，
    读到的 etag 不可能描述另一个 inode 的 body。sidecar 文件继续写（外部工具
    可读、存量对象兼容），读取侧 xattr 优先、缺失时回落 sidecar；不支持 xattr
    的文件系统上退化为纯 sidecar 语义（提交顺序为先数据后 sidecar）——该降级
    以常驻 gauge `lights3_localfs_xattr_fallback` 暴露（构造期即探测），
    `require_xattr: true` 则改为启动/写入即失败（roadmap §3.5）。
  - **sidecar 写策略**（`sidecar: sync|async|lazy`，默认 sync）：sync 每次
    PUT 4 次 fsync + 2 次 rename；async 把 sidecar 挪到后台任务（响应后落
    盘）；lazy 在 xattr 写成功时根本不写 sidecar（并顺手 unlink 旧的）。
    xattr 写失败时两种模式都退回同步写 sidecar——此时它是唯一元数据源。
  - **提交段 per-key 锁**：数据与 sidecar 毕竟是两次 rename，提交段取 striped
    异步互斥（64 条带，PUT 与 complete_multipart 共用），使 sidecar 也恒描述
    最终落地的那次写入。锁只覆盖提交段，body 读写全并发。
  - **持久性**：数据 tmp 与 sidecar tmp 在 rename 前 `fdatasync`、rename 后
    `fsync` 父目录（目录项落盘）。`LIGHTS3_FSYNC=0` 可关（吞吐优先的部署与
    测试夹具），默认开——已 200 应答的写掉电不丢是 S3 语义的一部分。

### 3.2 各操作实现要点

- 所有 posix 调用都在 `co_await pool.schedule()` 之后执行
  （见 [concurrency.md](concurrency.md) §3）。
- **GET**：open + fstat + 读元数据（该 fd 的 xattr，缺失回落 sidecar）。
  size/mtime 一律取**已打开 fd 的 fstat**、绝不对路径二次 stat——并发覆盖写后
  路径指向新 inode，二次 stat 会让 meta 与 fd 持有的 body 错位（短包/截断）。
  `FdBodyReader` 每次 `read()` 都经池执行 `pread`（带偏移，天然支持 Range）；
  fd 由 RAII 持有，取消/断连自动关闭。
- **元数据缓存**（roadmap §3.8）：HEAD/GET 先查按 (bucket,key) 分片的 LRU
  （`meta_cache.h`），记录携带 inode 戳（dev/ino/size/mtime/ctime）：HEAD 命中
  默认一次 stat 对戳（`meta_cache_validate=false` 则零 syscall），GET 用已持
  fd 的 fstat 对戳，戳不符即重读——外部进程改写同一 root 也不会喂出陈旧
  记录；本后端的每条写路径在提交点后失效。`meta_cache_entries`（默认 64K）、
  `meta_cache_ttl`。见 [storage/localfs.md](storage/localfs.md) §5.1。
- **PUT**：循环 `body.read(64KiB)` → 池内 write + 增量 MD5 → rename。
  ETag = MD5 hex，与 S3 单段上传一致。
- **LIST**：递归目录遍历 + prefix 剪枝（prefix 含 `/` 时直接定位起始目录）；
  delimiter=`/` 时目录即 common prefix，无需展开其内部，天然高效。
  分页 token = 最后返回的 key（目录序即字典序，需保证遍历为排序遍历）。
  不建索引，但（roadmap §3.5）：一页的 stat+getxattr 由多个池线程条带并行
  （`list_meta_concurrency`）；每个目录的排序条目表按目录 inode+mtime/ctime
  缓存（`list_cache_entries`，一次 stat 校验），翻页时二分定位 marker，深页
  成本不再随页码增长。见 [storage/localfs.md](storage/localfs.md) §6。
- **Multipart**：分片落 `staging/mpu/<id>/part.N`；complete 时按 part 顺序
  拼接写入最终临时文件再 rename（顺带算总 ETag：`md5(各分片md5拼接)-N`，
  与 S3 规则一致）；abort 删目录。启动时扫描 mpu 目录清理超期（默认 7 天）
  的孤儿上传。

### 3.3 XLocalFsBackend（xlocalfs，io_uring 数据面变体）

磁盘布局、元数据与 multipart 逻辑**完全复用** LocalFsBackend（继承 + 共享
`fs_util` 落盘原语），仅把数据面的字节搬运换成 io_uring：

- **封装**：`storage/xlocalfs/uring.h` 用原生 syscall（io_uring_setup/enter +
  mmap SQ/CQ）实现最小封装，不引入 liburing 依赖。1..N 个独立 ring
  （`rings`，roadmap §3.4 ④）：每 ring 一把提交互斥锁 + 批量 enter（值班
  flusher）+ 专职收割线程，CQE 完成后把协程续体投递回线程池——磁盘等待期间
  不占任何线程，后续的同步落盘调用（sidecar 等）天然回到池线程。建环时注册
  fixed buffers / 稀疏 fixed files 表（失败只丢优化不丢功能）。
- **覆盖范围**：GET 流式读（`UringStreamBodyReader`，read-ahead 环形缓冲，
  带偏移天然支持 Range）、PUT/UploadPart 流水线写（末块与 fdatasync 链式
  一次提交）、complete 分片拼接（分片读 read-ahead + 拼接写流水线）；内核
  支持时 open/statx/rename/unlink 与提交期目录 fsync 也走 ring。目录遍历
  （listing）仍走线程池（io_uring 无 getdents）。
- **配置**：`type: xlocalfs`，参数同 localfs（root/staging），另有可选
  `queue_depth`（SQ 深度，默认 256）、`rings`、`fixed_buffers` /
  `fixed_files`、`block_size`、`read_depth` / `write_depth`、`meta_ops`
  （详表见 [storage/xlocalfs.md](storage/xlocalfs.md) §8）。
- **生命周期**：`close()` 停止全部收割线程；须在在途请求完成后调用（与
  ThreadPool::join 同一假设）。

## 4. CloudProxyBackend（映射公有云）

把本地 bucket 映射到公有云对象存储（AWS S3 / 兼容 S3 协议的 OSS、COS、MinIO
等），网关充当带本地认证的代理。完整设计见
[cloudproxy-backend.md](cloudproxy-backend.md)，本节保留概述与路线决策。

### 4.1 两种实现路线

| 路线 | 做法 | 取舍 |
| --- | --- | --- |
| A. SDK 封装 | 用 aws-sdk-cpp（或轻量的 aws-c-s3）调用远端 | 正确性省心：重试、region、TLS、分片都是现成的；SDK 同步 API 在线程池里调用即可接入协程模型 |
| B. 直接转发（已定） | 自己构造 HTTP 请求 + 对远端做 SigV4 签名，经 HTTP client 转发 | 零 SDK 依赖、可真流式转发；但要自己处理重试与各云差异 |

设计初期倾向路线 A，**详细设计阶段反转为路线 B**（理由见 docs/cloudproxy-backend.md §2.1）：
出方向签名 `SigV4Authenticator::sign()` 早已随验签一并实现并预留给
cloudproxy；vendored 的 httplib 具备流式 client 能力；而 aws-sdk-cpp 的
依赖体量与本项目"全 vendored 子模块"的构建约束冲突。

要点（详细展开见 docs/cloudproxy-backend.md 对应小节）：

- **凭证隔离**：客户端用网关本地的 AK/SK 认证；网关用自己的云凭证访问远端。
  客户端凭证绝不透传，云凭证只存在于网关配置。
- **流式**：GET 方向 pump 线程 + 有界队列把 httplib 推模型翻成 `BodyReader`
  拉模型；PUT 方向对称反转。避免整对象缓冲（docs/cloudproxy-backend.md §3）。
- **Multipart 透传**：upload_id、part 直接映射远端同名概念，网关不落地分片。
- **超时与重试**：连接/请求超时、指数退避 3 次；远端 5xx 映射为网关 500/503
  对应的 S3 错误码，透传远端 4xx 语义（NoSuchKey 等）（docs/cloudproxy-backend.md §5）。
- **名称映射**：`bucket_prefix` 解决本地 bucket 名与远端全局命名空间冲突；
  key 不变换。
- 线程占用：同步 HTTP client 会占住线程整个请求时长——数据面 pump 使用
  cloudproxy 私有 pump 线程而非共享池（docs/cloudproxy-backend.md §2.3）。
  与之独立的另一机制是通用的 per-backend `io_threads` 池，已作为任意后端
  可配的通用键落地（见 [concurrency.md](concurrency.md) §3.1）。

## 5. DuoStoreBackend（元数据/数据分离引擎）

内部拆成元数据（IMetaStore）与数据（IDataStore）两个可插拔实现、以
DataRef 为唯一耦合点的存储引擎：默认元数据用 RocksDB（submodule）、数据用
本地文件系统——大对象定长 chunk 切片、小对象聚合进 append-only pack、
删除/覆盖经 GC 回收（延迟 unlink + pack 压实 + 孤儿对账；P1-P5 已全部
实现）。multipart 的 complete 是纯
元数据拼接（O(#parts)，零数据搬运）。完整设计见
[duostore-backend.md](duostore-backend.md)。

meta / data 两侧均已有可选替换实现（各有专文，编译开关默认 OFF）：

- meta：Redis（[duostore-redis-meta.md](duostore-redis-meta.md)）、
  SQLite（[duostore-sqlite-meta.md](duostore-sqlite-meta.md)）、
  TiKV（[duostore-tikv-meta.md](duostore-tikv-meta.md)）；
- data：Ceph/RADOS（[duostore-rados-data.md](duostore-rados-data.md)）。

对象元数据缓存（roadmap §3.8）：GET/HEAD 命中时整条 `ObjectRec`（含
manifest）来自进程内 LRU，零 meta 引擎 RTT。rocksdb/sqlite 默认开且精确
失效；redis/tikv 默认关，开启须 `0 < meta_cache_ttl < gc_grace`（对端网关的
写在 TTL 内不可见，read-lease 相应回拨）。见
[storage/duostore-core.md](storage/duostore-core.md) §7.1。

注意：duostore 不能作 tiered 的 local 侧（tiered 绑定 localfs 磁盘布局），
可作其 cloud 侧或独立使用。

## 6. 新增后端的步骤（扩展指南)

1. 实现 `IStorageBackend`（放 `src/storage/<name>/`）。
2. 在 `registry.cc` 的 `ensure_registered()` 里调用
   `StorageRegistry::register_backend("<type>", factory)` 注册，工厂签名
   `(const BackendConfig&, shared_ptr<ThreadPool>, MetricsScope)
   → shared_ptr<IStorageBackend>`。
3. 后端级指标（可选）：工厂收到的 `MetricsScope` 已带
   `backend=<name>` 基础标签，透传给后端构造器、构造期领取实例
   （`scope.counter/gauge/histogram/gauge_callback`，`with()` 派生子件维度），
   热路径无锁递增，`GET /-/metrics` 自动追加输出。不消费指标时忽略该参即可；
   测试直构后端传默认空 scope，计数落孤立实例、无需装配注册表。
4. 配置 `backends[].type` 即可引用；通过通用的**后端一致性测试套件**
   （同一组用例参数化跑所有后端：CRUD、range、list 分页、multipart、
   并发 PUT 同 key、异常 key）验收。
5. 通用键 `io_threads`（可选）：任意后端配置即获得专属
   IO 线程池而非共享全局池（Registry 在调用工厂前按参数注入，工厂/后端
   无感知）——慢后端（云端）占满共享池饿死快后端（本地盘）时的隔离
   手段，见 [concurrency.md](concurrency.md) §3.1。
