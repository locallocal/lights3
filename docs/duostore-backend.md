# DuoStore：元数据/数据分离的存储引擎后端

> 状态：P1 已实现（双接口 + RocksMetaStore 全量 + chunk 数据路径，代码在
> `src/storage/duostore/`）；P2-P5 未开始。元数据引擎 RocksDB
> （`third_party/rocksdb` submodule），数据引擎本地文件系统
> （chunk 切片 + pack 聚合 + GC）。承接 [storage-backend.md](storage-backend.md)
> §1 的接口契约与 §5 扩展指南；实施拆分见 §15。

## 1. 目标与非目标

| 目标 | 说明 |
| --- | --- |
| 实现 `IStorageBackend` 全接口 | bucket CRUD、对象数据面、list、multipart 全套（以 `src/storage/backend.h` 为准），通过后端一致性套件 |
| 元数据/数据分离 | 内部拆成 IMetaStore / IDataStore 两个**可插拔**接口；首期 RocksDB + 本地 fs，未来 meta 可换 redis/TiKV，data 可换 Ceph/RADOS（§12） |
| 大对象切片 | 定长 chunk（默认 8MiB），Range 读 O(1) 定位；multipart complete 零数据搬运（§8） |
| 小对象聚合 | ≤ 阈值（默认 128KiB）的对象追加进 append-only pack 文件，避免海量小文件的 inode/目录开销 |
| GC | 删除/覆盖/abort 产生的垃圾可回收：chunk 延迟 unlink、pack 按存活率压实、孤儿对账（§9） |
| 全链路流式 | GET/PUT 不整对象缓冲，背压传导（chunked PUT 的缓冲上界见 §5.3） |

非目标（首期不做）：

- 内容去重 / chunk 共享（chunk 恒 0/1 引用）；
- pack 写入的 group-commit 聚合（每 record 一次 fdatasync，见 §6.3）；
- 多进程/多网关共享同一 root（单进程独占，与 localfs 同一前提）；
- meta 的 TransactionDB / 分布式事务（复合不变量用 store 内互斥，§4.5）；
- 作 tiered 的 local 侧（§13.1）；
- RocksDB 压缩（元数据体量小，换零外部依赖，§13.3）。

## 2. 架构与路线决策

```text
                 DuoStoreBackend : IStorageBackend
                 （S3 语义、ETag/MD5、校验、泵送循环、GC worker）
                    │                          │
        IMetaStore（同步，池线程调用）    IDataStore（协程 Task<T>）
                    │                          │
             RocksMetaStore                FsDataStore
             （RocksDB WriteBatch）        （chunk 文件 + pack 文件）
                    └────────── DataRef ──────────┘
                     （值语义、可序列化的定位信息，两侧唯一耦合点）
```

### 2.1 内部接口的粒度：语义级 vs 裸 KV

| 路线 | 做法 | 取舍 |
| --- | --- | --- |
| A. 语义级（已定） | IMetaStore 暴露 create_bucket / put_object / complete_upload 等 S3 元数据事务 | 事务原子性用各实现的原生原语表达（RocksDB WriteBatch、SQL 事务、TiKV 事务）；不向上泄漏"有序 KV"假设，SQL/redis 类实现不别扭；GC 记账与元数据提交的同批不变量封装在实现内 |
| B. 裸 KV 级 | IMetaStore 只有 get/put/scan/batch，S3 语义在上层写一遍 | S3 语义代码只写一遍；但把实现空间锁死在有序 KV，批处理表达力覆盖不了"读改写+提交"型事务 |

选 A。代价是 S3 校验逻辑可能在实现间少量重复，用共享 helper
（`storage/validate.cc`、`storage/multipart.h`）压到最低。

### 2.2 同步还是协程：两侧刻意不对称

- **IMetaStore 同步**（契约：必须在池线程调用）：候选实现的客户端 API
  ——RocksDB、SQLite、redis（hiredis）、TiKV 客户端——全是同步阻塞的，
  套 Task 只会让每个实现各写一遍 `co_await pool->schedule()` 样板。由
  DuoStoreBackend 统一在入口切池线程（与 localfs 惯例一致：校验在调用方
  线程 → `co_await pool_->schedule()` → 之后全程池线程），meta 复合操作
  单跳完成。网络型 meta 在池线程上同步调用，与 cloudproxy 在池线程上跑
  同步 httplib 是同一模式（[concurrency.md](concurrency.md) §1）。
- **IDataStore 协程**（Task<T>）：数据面要与 `http::BodyReader` 的协程读
  循环交织流式写；且可预见的替代实现（io_uring 版对照 xlocalfs、Ceph
  librados 异步 API）是原生异步的，同步接口会封死这条路。各实现自行决定
  内部是否切池线程。

## 3. 双接口与 DataRef

### 3.1 DataRef：两侧唯一的耦合点

```cpp
// src/storage/duostore/data_ref.h
struct Extent {
    enum class Kind : uint8_t { kChunk = 0, kPack = 1 };  // 可扩展：kRados…
    Kind kind;
    uint64_t file_id;   // chunk / pack 文件号（全局单调分配，§4.5）
    uint64_t offset;    // pack 内 payload 起始偏移；chunk 恒 0
    uint64_t length;    // 本段字节数
    uint32_t crc32c;    // 本段内容校验和
};
struct DataRef {
    std::vector<Extent> extents;   // 空 = 0 字节对象；持久化用 run 编码（§4.3）
    uint64_t total() const;        // Σ length
};
```

meta 把 DataRef 当**不透明定位信息**存取，data 只负责按它读写。`Kind`
枚举就是数据面的扩展点：Ceph 实现新增 `kRados`（file_id 映射 rados 对象
名），meta 层零改动（§12）。

MD5/ETag 由 DuoStoreBackend 在泵送循环里用 `util::HashStream` 计算——
哈希是 S3 语义，不进数据面接口。

### 3.2 IMetaStore（同步，池线程调用，错误抛 `s3::S3Error`）

```cpp
struct ObjectRec {
    ObjectMeta meta;      // key/size/etag/content_type/last_modified/user_meta
    DataRef    data;
    uint64_t   version;   // 每次写 +1；GC 压实换 ref 的乐观校验（§9.2）
};
struct UploadRec { std::string upload_id; ObjectMeta meta; int64_t initiated_ms; };
struct PartRec   { int part_no; uint64_t size; std::string etag;
                   int64_t modified_ms; DataRef data; };
struct Reclaim   { std::vector<Extent> extents; };   // 待物理回收
struct PackStat  { uint64_t pack_id; uint64_t file_size;
                   int64_t live_bytes; int64_t live_recs; bool sealed; };

struct IMetaStore {
    // bucket
    virtual void create_bucket(std::string_view b) = 0;   // 已存在→BucketAlreadyOwnedByYou
    virtual void delete_bucket(std::string_view b) = 0;   // 不存在→NoSuchBucket；非空→BucketNotEmpty
    virtual bool bucket_exists(std::string_view b) = 0;
    virtual std::vector<BucketInfo> list_buckets() = 0;

    // object：提交类方法内部单事务完成"写新 + 旧 DataRef 入 GC 账 + 引用/统计更新"
    virtual std::optional<ObjectRec> get_object(std::string_view b, std::string_view k) = 0;
    virtual void put_object(std::string_view b, std::string_view k, ObjectRec rec) = 0;
    virtual bool delete_object(std::string_view b, std::string_view k) = 0;  // 不存在返回 false（幂等）
    virtual ListResult list_objects(std::string_view b, const ListOptions& opt) = 0;

    // multipart
    virtual std::string create_upload(std::string_view b, std::string_view k, ObjectMeta meta) = 0;
    virtual UploadRec require_upload(std::string_view b, std::string_view k,
                                     std::string_view id) = 0;               // 缺→NoSuchUpload
    virtual void put_part(std::string_view b, std::string_view k, std::string_view id,
                          PartRec p) = 0;                                    // 同号旧分片同批入 GC 账
    virtual std::vector<PartRec> list_parts(std::string_view b, std::string_view k,
                                            std::string_view id) = 0;
    virtual std::vector<UploadInfo> list_uploads(std::string_view b) = 0;
    virtual std::string complete_upload(std::string_view b, std::string_view k,
                                        std::string_view id,
                                        std::span<const PartInfo> parts) = 0;  // §8
    virtual void abort_upload(std::string_view b, std::string_view k, std::string_view id) = 0;

    // 资源分配与 GC 记账（§9）
    virtual uint64_t alloc_file_id(Extent::Kind kind) = 0;    // 持久单调，号段预留
    virtual std::vector<std::pair<uint64_t, Reclaim>> peek_reclaims(size_t max) = 0;
    virtual void ack_reclaim(uint64_t seq) = 0;               // 物理删除成功后销账
    virtual std::vector<PackStat> pack_stats() = 0;           // 压实候选
    virtual bool swap_extents(std::string_view b, std::string_view k, uint64_t expect_version,
                              const DataRef& from, const DataRef& to) = 0;  // 压实换 ref
    virtual bool chunk_referenced(uint64_t file_id) = 0;      // 孤儿扫描
    virtual void close() = 0;
};
```

### 3.3 IDataStore（协程）

```cpp
struct WriteHint { std::optional<uint64_t> content_length; };  // body.length()，chunked 时 nullopt

struct DataWriter {
    virtual Task<void> write(std::span<const std::byte> buf) = 0;
    virtual Task<DataRef> finish() = 0;   // 落盘+fsync 后返回定位；未 finish 即析构 = 丢弃
    virtual ~DataWriter() = default;
};

struct IDataStore {
    virtual Task<std::unique_ptr<DataWriter>> open_writer(WriteHint hint) = 0;
    // [first,last] 为 resolve_range 后的闭区间；返回流式 BodyReader（length()=last-first+1）
    virtual Task<std::unique_ptr<http::BodyReader>> open_reader(DataRef ref,
                                                    uint64_t first, uint64_t last) = 0;
    virtual Task<void> remove(std::span<const Extent> extents) = 0;  // 幂等（ENOENT 忽略）
    virtual Task<GcRewrite> rewrite_pack(uint64_t pack_id) = 0;      // 压实顺扫（§9.2）
    virtual Task<void> close() = 0;
};
```

## 4. RocksDB 元数据模型（RocksMetaStore）

### 4.1 Column family 划分

| CF | key 编码 | value | 说明 |
| --- | --- | --- | --- |
| `default` | `"schema"` / `"instance"` | 版本号 / uuid | 打开时校验 schema 版本 |
| `buckets` | `<bucket>` | `{v1, created_ms}` | |
| `objects` | `<bucket>\0<key>` | ObjectVal（§4.3） | 字节序 = S3 字典序，直接支撑 list |
| `uploads` | `<bucket>\0<key>\0<upload_id>` | `{v1, initiated_ms, content_type, user_meta}` | 前缀扫 `<bucket>\0` 天然按 (key, upload_id) 排序 |
| `parts` | `<bucket>\0<key>\0<id>\0<be16 part_no>` | `{v1, size, md5, modified_ms, extent_runs}` | big-endian part_no 保证升序，list_parts 前缀扫即得 |
| `refs` | `<be64 chunk_file_id>` | owner 简述（调试用） | chunk 存活引用表；孤儿判定 O(1) |
| `gcq` | `<be64 seq>` | `{extents, reason, enqueue_ms}` | 待回收队列 |
| `stats` | `p<be64 pack_id>` / `c<kind>` | merge 计数器 | pack 存活账（live_bytes/live_recs 增量 merge）；file_id/seq 号段 |

分隔符合法性：`validate_object_key` 已在共享校验层拒绝 key 含 NUL
（`src/storage/validate.cc`），bucket 名限 `[a-z0-9.-]`——`\0` 分隔安全，
无需转义编码。

### 4.2 value 编码：手写小端二进制

| 方案 | 评价 |
| --- | --- |
| 手写二进制（已定） | 值以字节精确整数（offset/length/crc）为主；extent 数组经 run 编码后极小；磁盘格式零第三方依赖 |
| JSON | 编解码省事，但 5TiB 对象 ≈65 万 extent 的 manifest 膨胀与解析成本随对象大小线性；磁盘格式绑第三方库（nlohmann_json 按约定只在 admin/credential 内部使用） |
| TSV（sidecar 惯例） | 不适合嵌套数组与二进制安全 |

首字节为版本号。ObjectVal v1 布局：

```text
u8 ver | u64 size | u64 mtime_ms | u64 version | str etag | str content_type
| u16 n_meta | (str k, str v)* | u32 n_runs | run*        （str = u16 len + bytes）
```

### 4.3 extent run 编码

一次写入会话分配的 chunk file_id 连续（§4.5 号段），故：

```text
run = { u8 kind, u64 first_file_id, u32 count,
        u64 chunk_len, u64 last_len, u64 pack_offset, u32 crc[count] }
```

单次 PUT 的 65 万 chunk 压成 1 个 run（crc 数组 4B/chunk 仍保留）；
multipart complete 后 run 数 = O(分片数)。

### 4.4 list_objects：原生有序迭代

**不复用 `storage/listing.h` 的 `apply_listing`**——它要求先收集全量有序
key，违背引入有序 KV 的初衷。直接在 `objects` CF 上迭代：

- 起点：`Seek(bucket + '\0' + max(prefix, start_after 的后继))`；
  start_after 命中自身时再 Next 一次；
- 终止：key 不再以 `bucket\0prefix` 为前缀，或收满 max_keys（多取一条判
  `is_truncated`，`next_token` = 最后一条 key，与现有各后端语义一致）；
- delimiter="/"：key 去 prefix 后含 "/" → 归入 common_prefix `p`，随后
  **`Seek(p 的最后字节 +1)` 跳过整组**——delimiter 列举复杂度从
  O(桶内 key 数) 降到 O(返回条目数)，这是相对 localfs 目录遍历的实质优势；
- 迭代持固定 snapshot + `iterate_upper_bound`，单次调用一致视图。

### 4.5 WriteBatch 原子事务与互斥

所有提交类操作在 RocksMetaStore 内部构成单个 WriteBatch，一次
`Write(WriteOptions{sync = meta_sync}, &batch)` 即提交点：

| 操作 | 同批内容 |
| --- | --- |
| put_object | 写 objects + 新 chunk 写 refs + 旧 DataRef 入 gcq + 删旧 refs + stats 负 merge |
| delete_object | 删 objects + 旧 DataRef 入 gcq + 删 refs + stats 负 merge |
| put_part（同号重传） | 写 parts + 新 refs + 旧分片入 gcq + 删旧 refs |
| complete_upload | 写 objects + 删 uploads/全部 parts + 未选中分片入 gcq + 旧同名对象入 gcq + refs 转移 + stats merge |
| abort_upload | 删 uploads/parts + 全部分片入 gcq + 删 refs |
| GC 销账 | 删 gcq 项 + 删 refs（物理 unlink 成功之后，§9.1） |

跨 key 复合不变量（bucket 存在性校验+对象提交、delete_bucket 空检查、
complete 校验+提交）用 store 内**一把 `std::mutex`** 序列化；纯读
（get/list，走 snapshot）不加锁。注意：读改写型事务要求提交（含
meta_sync=true 的 WAL fsync）也在锁内完成，写路径吞吐上限 ≈ 1/fsync 延迟
且 RocksDB group commit 失效——P1 明确接受此代价；竞争成为瓶颈时的升级
路径是 RocksDB `TransactionDB`（不做，仅注明）。delete_bucket 的空检查
同时覆盖 objects 与 uploads（有进行中 multipart 即 BucketNotEmpty，对齐
AWS——否则桶删后 put_part 仍可写入、refs 永久泄漏、重建桶复活幽灵上传）。

`alloc_file_id` 用**号段预留**：`stats` 计数器一次 merge +4096、内存派发，
持独立小锁（不排在业务提交的 fsync 之后）；预留提交**恒 WAL fsync**（独立
于 meta_sync）——否则崩溃丢预留后重启重发已用 file_id，与已落盘的 chunk
文件 O_EXCL 冲突。崩溃浪费号段无害（file_id 只需唯一单调，不需连续）。

## 5. 数据布局（FsDataStore）

```text
<root>/
  meta/                             # RocksDB（可用 meta_path 单独指到 SSD）
  chunks/<ss>/<file_id:016x>.chk    # ss = (file_id >> 8) 低 8 位 hex，256 个 shard 目录
  packs/<ss>/<pack_id:016x>.pak     # 连续 id 每 256 个同目录：一次写会话的目录 fsync 收敛到 1-2 次
```

### 5.1 chunk（大对象切片）

- `chunk_size` 默认 **8MiB**：顺序 IO 足以摊薄打开/寻址开销；5TiB 上限
  对象 ≈65 万 chunk 经 run 编码后 manifest 仍是常数级；与主流对象存储
  条带同量级。定长（末块除外）使 Range→(chunk_idx, in-chunk offset)
  为 O(1) 除法。
- chunk 文件一次写成后**不可变**、恰被 0 或 1 个 manifest 引用（不去重）。
  写完 `fdatasync(fd)`；会话结束对涉及的 shard 目录 `fsync(dirfd)`
  （256 个 dirfd 常驻缓存）。

### 5.2 pack（小对象聚合）

- 判定：对象（或分片）长度 ≤ `pack_threshold`（默认 **128KiB**）进 pack，
  否则走 chunk。
- pack 文件 **append-only**，record 格式：

```text
record := header || payload
header := magic u32 "LP3R" | u8 ver=1 | u8 flags | u16 header_len
        | u64 payload_len | u32 crc32c(payload)
        | u16 owner_len | owner（"bucket\0key" 或 "mpu\0<id>\0<part_no>"）
Extent{ kind=kPack, file_id=pack_id, offset=payload 起始, length=payload_len }
```

  owner 内嵌的作用：GC 压实顺扫反查存活（§9.2）与灾难恢复离线打捞，代价
  每 record 几十字节。offset 指向 payload 而非 header——热路径读不解析头。
- **active pack 轮转**：`pack_writers`（默认 4）个 active pack 并存，各带
  互斥锁与追加偏移；写者轮询取锁追加（append = 一次 `pwrite` 头+payload
  + `fdatasync`）。达到 `pack_max_size`（默认 **128MiB**）即封存
  （sealed）、换新 pack_id。锁模型简单成立的前提：payload ≤128KiB，
  临界区是一次小写 + sync，多 writer 摊薄排队。
- **重启不复用旧 active pack**：直接开新文件，旧尾部可能的 torn record
  成为死区、由压实回收——省掉重启时的尾部校验修复逻辑。

### 5.3 长度未知的流式 PUT（chunked encoding）

`open_writer` 拿 `WriteHint.content_length` 判路径；未知长度时 writer 先
**内存缓冲**：累计 ≤ pack_threshold 则 EOF 时整体追加进 pack；一旦超过
阈值即分配 chunk file_id、落盘缓冲并转为 chunk 流式路径。内存代价上界 =
`pack_threshold × max_inflight_requests`（默认 128KiB × 1024 = 128MiB）
——两个配置联动，调大任一侧时注意乘积。

## 6. 写入一致性与崩溃模型

**提交点 = RocksDB WriteBatch 写入（WAL）。数据先落、meta 后提交——任何
时刻崩溃都不产生"meta 指向不存在/不完整数据"。**

### 6.1 PUT 全流程（upload_part 相同）

```text
① 调用方线程：validate_bucket_name / validate_object_key
② co_await pool_->schedule()；bucket 存在性预检（正式检查在提交事务内复查）
③ writer = co_await data_->open_writer({body.length()})
   循环：n = co_await body.read(buf)；md5.update；co_await writer->write(...)
④ ref = co_await writer->finish()      # chunk: fdatasync + shard 目录 fsync
                                        # pack: record pwrite + fdatasync
⑤ meta_->put_object(...)               # WriteBatch 提交点；旧 DataRef 同批入 gcq
   ⑤ 抛异常时：catch 后 co_await data_->remove(ref) 兜底；
   兜底失败也无害——落入孤儿扫描/死区（§9）
```

### 6.2 崩溃窗口矩阵

| 崩溃点 | 后果 | 回收路径 |
| --- | --- | --- |
| ③④ 之间 / ④⑤ 之间（chunk） | chunk 已在盘、refs 无记录 | 孤儿扫描：无 refs 且 mtime 逾宽限 → unlink |
| 同上（pack） | record 已追加但无 live 记账 | 天然死区，压实时自动回收，无需扫描 |
| pack append 中途 | 尾部 torn record，无引用 | 重启弃用 active pack；死区随压实回收 |
| ⑤ 之后 | 一切一致 | 旧数据在 gcq，GC 照常 |

覆盖写（同 key PUT）：新数据完整落盘后，提交事务内原子地"新 rec 生效 +
旧 DataRef 入 gcq"；物理删除**必须异步且延迟**（服务并发读，§7），
决不在 PUT 路径同步 unlink。

### 6.3 fsync 策略汇总

| 面 | 策略 |
| --- | --- |
| chunk | 每文件 `fdatasync` + shard 目录 fsync |
| pack | 每 record `fdatasync`（group-commit 聚合窗口为非目标） |
| meta | `meta_sync: true`（默认）= 每次提交 WAL fsync；关掉则崩溃丢最近数秒元数据但仍自洽（数据成孤儿被回收）——语义与 localfs 现状（rename 无盘屏障）对齐，按需取舍 |

## 7. GET 流式读

`open_reader(ref, first, last)` 返回 `ExtentChainReader : http::BodyReader`：

- 构造时把 [first,last] 映射到起始 run/extent：run 内 chunk 定长，
  `idx = (first − run_base) / chunk_len`，O(#runs) 定位；
- `read(buf)`：**懒打开**当前 extent 的 fd（`co_await pool_->schedule()`
  后 open + pread，切池线程模式与 localfs 的 `FdStreamReader` 一致；不直接
  复用它——那是单 fd 所有权语义，这里是多文件链）；extent 读尽即关 fd
  推进到下一个；`length()` 返回 last−first+1；
- pack extent：整段 payload（≤128KiB）一次读入、**恒校验 crc32c**，再按
  range 切片吐出；chunk extent 直接 pread 流式吐，crc 默认不校验
  （`verify_chunk_crc: false`）；开启后只对"从段首完整读到段尾"的 chunk
  校验——Range 命中 chunk 中段时无从校验，完整性主责在 GC/对账路径。

**与删除/GC 并发的安全性**：POSIX 已保证打开的 fd 不受 unlink 影响
（localfs fd 快照同理），风险只在**懒打开**：对象被 DELETE 且 GC 立即
unlink 后，reader 打开下一个 chunk 会 ENOENT。

| 方案 | 评价 |
| --- | --- |
| 预打开全部 fd | 大对象数万 fd，不可行 |
| 仅延迟删除（grace） | 简单，但慢客户端读大对象可超时限，概率正确 |
| 进程内引用计数（已定） | `open_reader` 把 ref 涉及的全部 file_id 注册进 pin 表（`unordered_map<file_id,int>` + mutex），reader 析构解除；GC 对 pin>0 的 file 跳过本轮。单进程独占 root 是既有前提，进程内计数即完全正确 |

叠加 `gc_grace`（默认 5m）作为实现缺陷的防御纵深。pack 压实对读者同样
安全：旧 pack 从不就地修改（§9.2），已打开 fd 读旧 inode，懒打开由
pin + grace 保护。

## 8. Multipart

- `create_multipart`：`new_upload_id()`（复用 `storage/multipart.h`）+
  uploads CF 写入；
- `upload_part`：与 PUT 同一条数据管线（分片按自身大小走 pack/chunk，
  常规 ≥5MiB 分片天然全走 chunk）；PartRec 记分片自己的 DataRef 与 MD5；
  同号重传 = 事务内换记录、旧分片入 gcq（last-write-wins）；
- `complete_multipart`：**纯元数据事务，零数据搬运**——
  `validate_part_order` + 逐项 ETag 比对（`strip_etag_quotes`）后，把各
  part 的 extent runs 按序拼接成对象 DataRef，`combined_etag()` 合成总
  ETag，单 WriteBatch 提交（§4.5）。对比 localfs 的串接式 complete：
  **O(#parts) vs O(总字节)**，这是 manifest 化布局的直接红利；
- `abort_multipart`：批删 uploads/parts + 分片全部入 gcq；未知 id 抛
  NoSuchUpload（与接口约定一致）；
- 过期清理：GC worker 顺带扫 uploads CF，initiated 早于 `mpu_ttl`
  （默认 7d）的走内部 abort（localfs 的 `kMpuTtl` 同类机制）。

## 9. GC

记账全部随业务事务同批产生（§4.5 表）：`gcq` 待回收队列、`stats` pack
存活账（merge operator 增量，免读改写）、`refs` chunk 引用表。

触发：后台单协程 worker（`TimerQueue` 周期，`gc_interval` 默认 5m）+
**public 手动钩子 `Task<GcStats> run_gc_once()`**（测试直调，仿 tiered
P1"手动触发下沉"先例）；孤儿扫描独立低频（`orphan_scan_interval` 默认
1d）+ 同款钩子。GC 与业务共用 `pool_`，经 `core/semaphore.h` 限流，
压实拷贝分块进行不长占池线程。

生命周期：`DuoStoreBackend::close()` 撤销定时器、等待在途 GC 协程结束，
再依次 `data_->close()`（封存 active pack）与 `meta_->close()`（RocksDB
干净关闭）；与 tiered 的 close 语义一致（须在在途请求完成后调用）。

### 9.1 gcq 消费（删除变现）

`peek_reclaims` 取批；逾 `gc_grace` 且所涉 file 无 pin（§7）的项：

- chunk extent → `unlink`（ENOENT 幂等忽略）→ 成功后 `ack_reclaim`
  （同批删 gcq + refs）。**顺序铁律：先物理删、后销账**——反序在删与销
  之间崩溃会产生永久孤儿的账外文件；正序崩溃只是 gcq 残留，重试 unlink
  幂等；
- pack extent → 存活账已在业务事务扣减，直接销账；顺带检查
  `live_recs == 0` 且 sealed 的 pack → 整文件 unlink + 删 packstat。

### 9.2 pack 压实

`pack_stats()` 挑 sealed 且 `live_bytes / file_size < pack_gc_ratio`
（默认 0.5）且 live>0 的 pack：

1. `rewrite_pack` **顺扫**全部 record（magic 不符 / crc 坏 → 跳过 + 告警，
   绝不静默删，§10）；
2. 凭内嵌 owner 反查 meta：owner 当前 DataRef 仍指向本 pack 该 offset →
   存活，payload 追加到 active pack；
3. `swap_extents(b, k, expect_version, from, to)` 乐观换 ref——version 或
   extent 不符 = 期间被覆盖/删除 → 放弃该条（新写入自会记账）；
4. 扫完整 pack 入 gcq（延迟 unlink，服务在途读者）。

选"顺扫 + 内嵌 owner"而非维护 pack→owner 反向索引 CF：GC 是低频路径，
顺扫 128MiB 可接受，省掉一个必须与业务事务同批维护的索引。

### 9.3 孤儿扫描（对账）

- 遍历 `chunks/`：`chunk_referenced(file_id) == false` 且 mtime 逾
  `gc_grace` → unlink；
- 反向：refs 有而文件缺 → **告警计数，绝不静默删 meta**（数据丢失征兆，
  留人工介入）；
- pack 无需孤儿扫描：未记账 record 即死区，压实自然回收。

## 10. 错误映射

| 来源 | 映射 |
| --- | --- |
| RocksDB 非 ok Status（IOError/Corruption/…） | `InternalError`(500) + error 日志（含 Status 描述） |
| RocksDB NotFound | 不直接映射——由语义层转 `NoSuchKey` / `NoSuchBucket` / `NoSuchUpload`(404) |
| bucket 已存在 / 非空 | `BucketAlreadyOwnedByYou` / `BucketNotEmpty`(409) |
| 分片校验失败 | `InvalidPart` / `InvalidArgument`(400)，复用 multipart.h helper |
| Range 不满足 | `resolve_range` 抛 `InvalidRange`(416)（共享实现） |
| pack record crc / magic 错（GET） | `InternalError`(500) + corruption 计数器 + 告警 |
| chunk open ENOENT 但 refs 在（GET） | `InternalError`(500) + 告警（数据丢失征兆） |
| GC 压实遇损坏 record | 跳过 + 告警，保留原 pack 不删（人工介入），其余存活记录照常迁移 |
| ENOSPC（数据面） | `InternalError`(500)；已产出数据走 remove 兜底 |

## 11. 配置

```yaml
backends:
  - name: duodata
    type: duostore
    root: ./data/duostore         # 必填；meta/ chunks/ packs/ 均在其下
    # meta_path: /ssd/duo-meta    # 可选：RocksDB 目录单独放置
    chunk_size: 8MiB
    pack_threshold: 128KiB
    pack_max_size: 128MiB
    pack_writers: 4
    pack_gc_ratio: "0.5"
    gc_interval: 5m
    gc_grace: 5m
    orphan_scan_interval: 1d
    mpu_ttl: 7d
    meta_sync: true
    verify_chunk_crc: false
    rocksdb_block_cache: 64MiB

buckets:
  default_backend: duodata
```

所有键为 YAML 标量、自动收入 `BackendConfig.params`，无需改配置解析器；
`DuoStoreConfig::from_params(name, params)` 集中解析并做范围校验（仿
cloudproxy），`parse_size` / `parse_duration_sec` 可直接用。

| 键 | 默认 | 说明 |
| --- | --- | --- |
| root | 必填 | 数据根目录 |
| meta_path | `<root>/meta` | RocksDB 目录 |
| chunk_size | 8MiB | 大对象切片粒度 |
| pack_threshold | 128KiB | ≤ 此值进 pack；亦是 chunked PUT 的缓冲上限（§5.3） |
| pack_max_size | 128MiB | active pack 封存阈值（= 压实重写单元） |
| pack_writers | 4 | 并存 active pack 数 |
| pack_gc_ratio | 0.5 | 存活率低于此值触发压实 |
| gc_interval / gc_grace | 5m / 5m | 回收周期 / 延迟删除宽限 |
| orphan_scan_interval | 1d | chunk 孤儿对账周期 |
| mpu_ttl | 7d | 未完成 multipart 过期清理 |
| meta_sync | true | RocksDB 提交是否 WAL fsync（§6.3） |
| verify_chunk_crc | false | GET 链路 chunk crc 校验（pack 恒校验） |
| rocksdb_block_cache | 64MiB | RocksDB block cache 容量 |

## 12. 可插拔演进：分布式实现

双接口 + DataRef 解耦的设计目标就是让两侧独立替换：

| 侧 | 未来实现 | 接入方式 |
| --- | --- | --- |
| meta | redis / TiKV（多网关共 meta） | 实现 IMetaStore：同步客户端在池线程调用即可（§2.2）；事务不变量用各自原语（redis MULTI/Lua、TiKV 事务）表达——语义级接口不假设有序 KV，这正是 §2.1 选 A 的原因。Redis 版详细设计见 [duostore-redis-meta.md](duostore-redis-meta.md) |
| meta | SQLite（单文件部署） | 同上，SQL 事务。详细设计见 [duostore-sqlite-meta.md](duostore-sqlite-meta.md) |
| data | Ceph / RADOS | 实现 IDataStore：新增 `Extent::Kind::kRados`（file_id 映射 rados 对象名），meta 层零改动；librados 异步 API 与协程接口天然契合 |
| data | io_uring 版 FsDataStore | 对照 xlocalfs 的做法，数据面换 IO 引擎、布局不变 |

组合矩阵：RocksDB+本地盘（首期）、TiKV+Ceph（全分布式网关）、
RocksDB+Ceph（本地索引 + 远端数据）均为合法组合。注意：跨网关共享 meta
时 §7 的进程内 pin 表不再充分，需要引入租约/分布式宽限——列为对应实现
的设计前提，不在本文档展开。

## 13. 与现有组件关系及构建接入

### 13.1 组件关系

- **不能作 tiered 的 local 侧**：`TieredBackend` 对 local 侧
  `dynamic_pointer_cast<LocalFsBackend>`，其 stub/sidecar/fd 快照语义绑定
  localfs 磁盘布局；duostore 无此布局。若未来要支持，正路是给 tiered
  抽象 local 侧接口——属 tiered 的演进，非本设计范围；
- **可作 tiered 的 cloud 侧**：cloud 侧只经 `IStorageBackend` 抽象，
  直接可用（P5 以 `e2e_tiered_duostore` 组合验收）；
- **bucket_router 正常路由**：叶子后端，registry 单阶段构造（不进 tiered
  的 deferred 列表）；
- 复用：`validate_*`、`multipart.h` 全套、`util::HashStream`、
  `resolve_range`、`core/semaphore.h`、`TimerQueue`；**不复用**
  `apply_listing`（§4.4）、`FdStreamReader`（§7）、fs_util sidecar 体系。

### 13.2 RocksDB submodule

`.gitmodules` 增 `third_party/rocksdb`（`shallow = true`，仓库体量大）；
build.sh 的 `LIGHT_MODULES` 加入 rocksdb **始终 init**——与 seastar 的
按需拉取不同：RocksDB 压缩全关后零系统级依赖、构建自洽，不需要
`--xxx` 开关做惰性拉取。

### 13.3 CMake 预设（仿 gflags/seastar 模板）

```cmake
set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)      # 仓库已有 gflags 目标；仅 rocksdb 工具需要——OFF 防目标冲突
set(WITH_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_ALL_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_CORE_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_TRACE_TOOLS OFF CACHE BOOL "" FORCE)
set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(WITH_SNAPPY OFF CACHE BOOL "" FORCE)      # 压缩全关：元数据体量小，换零外部依赖
set(WITH_LZ4 OFF CACHE BOOL "" FORCE)
set(WITH_ZLIB OFF CACHE BOOL "" FORCE)
set(WITH_ZSTD OFF CACHE BOOL "" FORCE)
set(WITH_LIBURING OFF CACHE BOOL "" FORCE)    # 机器无 liburing 开发头（seastar 同款注释）
set(PORTABLE ON CACHE BOOL "" FORCE)          # 不用 -march=native
set(USE_RTTI 1 CACHE BOOL "" FORCE)           # lights3 全程异常 + RTTI
set(FAIL_ON_WARNINGS OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/rocksdb EXCLUDE_FROM_ALL SYSTEM)
```

**`LIGHTS3_DUOSTORE` 默认 ON**。对比：默认 OFF（仿 seastar）可省 clean
build 数分钟，但 seastar 默认 OFF 的真因是系统级重依赖（编译版 Boost、
ragel），RocksDB 无此问题；而默认 OFF 意味着 duostore 的
backend_suite/e2e 在日常构建缺席、特性必然腐化。裁剪模板照 cloudproxy：
`if(LIGHTS3_DUOSTORE)` 内 `target_sources`（四个 .cc）+
`target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE)` +
`target_link_libraries(lights3_core PRIVATE rocksdb)`；registry.cc 中
`#ifdef LIGHTS3_DUOSTORE` 包注册。

## 14. 测试策略

1. **一致性套件**：`tests/unit/test_storage.cc` 增 `#ifdef LIGHTS3_DUOSTORE`
   的套件用例，临时目录建 DuoStoreBackend 跑 `run_backend_suite`——与
   memory/localfs/xlocalfs/tiered 同一语义基线。**三种布局变体**同套件
   全绿：默认参数（混合）、`pack_threshold` 调大（强制全 pack）、
   `chunk_size` 调小至 4KiB（强制多 chunk manifest）；
2. **专项单测**（`tests/unit/test_duostore.cc`）：key/value 编解码
   roundtrip（含 run 边界）；list 的 delimiter Seek 跳跃与分页 token；
   pack record 解析 / torn tail / crc 损坏注入；覆盖写与 delete 后
   `run_gc_once()` → 文件消失、packstat 归零、gcq 清空；压实（低存活
   pack 重写、压实期间并发覆盖触发 version 校验放弃路径）；并发 GET 持
   pin 时 GC 跳过；重启模拟（关后端 → 重开同 root：active pack 弃用、
   孤儿 chunk 回收、号段不回退）；mpu_ttl 过期 abort；
3. **e2e**：`run_e2e.sh` 增 backend-type `duostore` 分支（配置块照 §11）；
   CMake `if(LIGHTS3_DUOSTORE AND LIGHTS3_DRIVER_BUILTIN)` 注册
   `e2e_duostore`；P5 增 `e2e_tiered_duostore`（duostore 作 tiered cloud
   侧，复证 §13.1）。

## 15. 实施拆分

| 阶段 | 内容 | 可独立验收 | 状态 |
| --- | --- | --- | --- |
| P1 | rocksdb submodule + CMake/build.sh 接入；DataRef/编码；双接口；RocksMetaStore 全量（bucket/object/list/multipart 事务）；FsDataStore 仅 chunk 路径（`pack_threshold=0` 全走 chunk）；删除只记账不回收 | `duostore_backend_suite` 全绿 + `e2e_duostore` + 编码/list 专项 | 已完成 |
| P2 | pack 聚合：阈值判定（含 chunked 缓冲）、多 active pack 并发追加、record 格式与 crc、重启弃用 active pack | 全 pack/混合布局套件变体全绿 + record/torn tail 专项 | 未开始 |
| P3 | GC 一期：gcq 消费、chunk unlink 与整 pack 删除、pin 计数 + gc_grace、`run_gc_once()` 钩子、mpu_ttl 清理、后台 worker | 覆盖/删除/abort 后 GC 收敛专项 + 并发 GET vs GC 无 ENOENT | 未开始 |
| P4 | GC 二期：pack 压实（顺扫 + owner 反查 + swap_extents）、孤儿扫描与 refs 反向对账告警、崩溃注入（kill -9 重启收敛） | 低存活压实 + 崩溃注入专项全绿 | 未开始 |
| P5 | 打磨：RocksDB 调参外露、s3/metrics 指标（corruption/GC 计数）、`e2e_tiered_duostore` 组合、文档状态头更新 | 全 ctest 矩阵含新 e2e 全绿 | 未开始 |

P1 即含 multipart：`run_backend_suite` 是单入口全语义套件绕不开；且
duostore 的 complete 本就是纯 meta 拼接（§8），multipart 在本架构下反而
是低成本项。GC 后置到 P3 安全：P1/P2 的删除语义已正确（meta 即真相），
只是空间暂不回收且全程记账，P3 只做"旧账变现"。
