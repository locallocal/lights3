# 04 存储后端

## 1. IStorageBackend 接口

L2 与存储的唯一边界。接口按 S3 语义而非文件语义设计，全部返回 `Task<T>`，
数据面走流式 `BodyReader`：

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
│   ├── .bucket.json                   # bucket 标记与属性（创建时间等）
│   ├── dir/a.bin                      # object 数据文件，key 即相对路径
│   └── dir/a.bin.lights3-meta         # sidecar：ObjectMeta 的 JSON
<staging>/                             # 与 root 同一文件系统（rename 原子性）
├── put/<uuid>                         # PUT 进行中的临时文件
└── mpu/<upload_id>/
    ├── manifest.json                  # bucket/key/meta/创建时间
    └── part.00001 ... part.NNNNN
```

关键决策：

- **key → 路径映射**：`/` 作目录分隔直接落盘，保持人类可读、可用普通工具
  操作。逃逸处理：拒绝含 `..` 段的 key；对文件系统非法或超长（>255B）的
  path 段做 percent-encoding 落盘（meta 中记录原始 key，list 时还原）。
  key 与目录冲突（已存在 `a/b` 再 PUT `a`）返回 S3 兼容错误。
- **sidecar 而非 xattr**：xattr 有大小限制且 scp/rsync 易丢；sidecar JSON
  可靠且可检。list 时按后缀过滤掉 sidecar。
- **写入原子性**：PUT 全部写到 staging 临时文件（边写边算 MD5/SHA256），
  校验通过后 `rename()` 到最终路径（先 meta 后 data，读取侧以 data 为准），
  失败路径统一 unlink 临时文件。并发 PUT 同一 key 采用 last-write-wins
  （与 S3 语义一致），rename 的原子性保证读者看不到半截数据。

### 3.2 各操作实现要点

- 所有 posix 调用都在 `co_await pool.schedule()` 之后执行（见 03 篇）。
- **GET**：open + fstat + 读 sidecar；`FdBodyReader` 每次 `read()` 都经池
  执行 `pread`（带偏移，天然支持 Range）；fd 由 RAII 持有，取消/断连自动关闭。
- **PUT**：循环 `body.read(64KiB)` → 池内 write + 增量 MD5 → rename。
  ETag = MD5 hex，与 S3 单段上传一致。
- **LIST**：递归目录遍历 + prefix 剪枝（prefix 含 `/` 时直接定位起始目录）；
  delimiter=`/` 时目录即 common prefix，无需展开其内部，天然高效。
  分页 token = 最后返回的 key（目录序即字典序，需保证遍历为排序遍历）。
  首期不建索引；对超大 bucket 的优化（如 per-directory 缓存）留作后续。
- **Multipart**：分片落 `staging/mpu/<id>/part.N`；complete 时按 part 顺序
  拼接写入最终临时文件再 rename（顺带算总 ETag：`md5(各分片md5拼接)-N`，
  与 S3 规则一致）；abort 删目录。启动时扫描 mpu 目录清理超期（默认 7 天）
  的孤儿上传。

## 4. CloudProxyBackend（映射公有云）

把本地 bucket 映射到公有云对象存储（AWS S3 / 兼容 S3 协议的 OSS、COS、MinIO
等），网关充当带本地认证的代理。

### 4.1 两种实现路线

| 路线 | 做法 | 取舍 |
| --- | --- | --- |
| A. SDK 封装（首选） | 用 aws-sdk-cpp（或轻量的 aws-c-s3）调用远端 | 正确性省心：重试、region、TLS、分片都是现成的；SDK 同步 API 在线程池里调用即可接入协程模型 |
| B. 直接转发 | 自己构造 HTTP 请求 + 对远端做 SigV4 重签名，经 HTTP client 转发 | 零 SDK 依赖、可真流式转发；但要自己处理重试与各云差异 |

首期取 **路线 A**，接口内封装：

```cpp
Task<ObjectStream> CloudProxyBackend::get_object(bucket, key, range) {
    co_await pool_->schedule();
    auto remote_bucket = cfg_.bucket_prefix + std::string(bucket);
    // SDK 的 GetObject 返回 IOStream，包装成 BodyReader（每次 read 都经线程池）
    ...
}
```

要点：

- **凭证隔离**：客户端用网关本地的 AK/SK 认证；网关用自己的云凭证访问远端。
  客户端凭证绝不透传，云凭证只存在于网关配置。
- **流式**：PUT 方向用 SDK 的流式接口把 `BodyReader` 包装成 `std::streambuf`
  喂给 SDK；GET 方向反向包装。避免整对象缓冲。
- **Multipart 透传**：upload_id、part 直接映射远端同名概念，网关不落地分片。
- **超时与重试**：SDK 配置层面设置（连接/请求超时、指数退避 3 次）；
  远端 5xx 映射为网关 502/503 对应的 S3 错误码，透传远端 4xx 语义
  （NoSuchKey 等）。
- **名称映射**：`bucket_prefix` 解决本地 bucket 名与远端全局命名空间冲突；
  key 不变换。
- 线程占用：SDK 同步调用会占住池线程整个请求时长，高并发远端访问时这是
  容量瓶颈——通过 backend 独立线程池（03 篇 §3 预留）或切换路线 B 演进。

## 5. 新增后端的步骤（扩展指南)

1. 实现 `IStorageBackend`（放 `src/storage/<name>/`）。
2. `REGISTER_STORAGE_BACKEND("<type>", factory)` 注册，工厂签名
   `(const BackendConfig&, shared_ptr<ThreadPool>) → unique_ptr<IStorageBackend>`。
3. 配置 `backends[].type` 即可引用；通过通用的**后端一致性测试套件**
   （同一组用例参数化跑所有后端：CRUD、range、list 分页、multipart、
   并发 PUT 同 key、异常 key）验收。
