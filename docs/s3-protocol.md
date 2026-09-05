# S3 协议实现

## 1. API 范围

首期实现覆盖主流客户端（aws cli、boto3、s3cmd、MinIO SDK）日常操作所需的子集：

| 类别 | API | 备注 |
| --- | --- | --- |
| Service | ListBuckets | 聚合各后端 |
| Bucket | CreateBucket / DeleteBucket / HeadBucket | 单 region：CreateBucket 的 LocationConstraint 与配置 region 不符即 `InvalidLocationConstraint`；HeadBucket/CreateBucket 回 `x-amz-bucket-region` |
| Object | PutObject / GetObject / HeadObject / DeleteObject / DeleteObjects(批量) / CopyObject | Get 支持 Range、条件请求（If-Match/If-None-Match/If-Modified-Since）、六个 `response-*` 覆盖参数与 `?partNumber`（206 + `x-amz-mp-parts-count`，分片布局在 complete 时记入 `part_sizes`）；Cache-Control/Content-Disposition/Content-Encoding/Content-Language/Expires 随对象持久化并回显；Content-MD5 与 `x-amz-checksum-*`（crc32/crc32c/crc64nvme/sha1/sha256，头部或 aws-chunked trailer 形态均可）校验请求体并**随对象持久化**，GET/HEAD 带 `x-amz-checksum-mode: ENABLED` 回显（含 `x-amz-checksum-type`；Range/partNumber 206 不回显）；DeleteObjects **要求**完整性头 |
| Tagging | GetObjectTagging / PutObjectTagging / DeleteObjectTagging | `x-amz-tagging` 写入时带标签（各后端均持久化）；`x-amz-tagging-count` 回显；就地改标签（PUT/DELETE ?tagging）在 memory/localfs/xlocalfs/tiered/cloudproxy 可用，duostore 诚实 501（无 meta 原地更新原语） |
| CORS | GetBucketCors / PutBucketCors / DeleteBucketCors + OPTIONS 预检 | root 专属（与 ?website 同两级模型）；规则持久化 `.sys/cors/<bucket>`；预检在验签**之前**分派（浏览器不给 OPTIONS 签名）；实际请求（成功与错误响应）注入 Allow-Origin/Expose-Headers/Vary |
| Lifecycle | GetBucketLifecycle / PutBucketLifecycle / DeleteBucketLifecycle | root 专属；最小子集：Expiration.Days + AbortIncompleteMultipartUpload.DaysAfterInitiation（prefix 过滤）；`lifecycle.scan_interval`（默认 1h，0=关）周期执行；Transition/按 tag 过滤/Date 形态 → 501 |
| Quota / 租户 | GetBucketQuota / PutBucketQuota / DeleteBucketQuota（`?quota`，本实现自定义 XML） | 桶级 `MaxBytes`/`MaxObjects`（PUT/DELETE root 专属）；用量计数器由网关维护并周期全量校准；超限写回 `QuotaExceeded`(403)，MPU 分片计入用量、Complete 仅在完成后仍放不下时拒绝且上传保留；租户凭证（`tenant`/`role` 字段）只见本租户所有的桶，ListBuckets `Owner` 为租户；管理面 `/-/admin/tenants`、`/-/admin/usage`；审计日志 JSON 行。详见 [multi-tenancy.md](multi-tenancy.md) |
| STS | AssumeRole | `POST /`（path-style 部署）form 表单、SigV4 service scope `sts`；会话凭证（L3SA 前缀 AK/SK/token + TTL 900–43200s）继承**调用者**的 policy（无角色目录，永不越权）；内存态单实例、session 不能再 AssumeRole |
| List | ListObjectsV2（含 V1 兼容） | prefix / delimiter / max-keys / continuation-token / start-after / fetch-owner；V1 只认 marker，V2 只认 continuation-token 与 start-after |
| Multipart | CreateMultipartUpload / UploadPart / UploadPartCopy / CompleteMultipartUpload / AbortMultipartUpload / ListParts / ListMultipartUploads | UploadPartCopy 支持 x-amz-copy-source-if-* 与 x-amz-copy-source-range（bytes=first-last，两端必填），源/目标可在不同后端；ListParts/ListMultipartUploads **真分页**（marker + max-*，据实回 IsTruncated；两者均支持 encoding-type，uploads 的 delimiter 任意）；非末片最小 5MiB（`http.min_part_size`，0=关），乱序回 `InvalidPartOrder`；分片校验和随 part 记录持久化，complete 由**已验证**的分片值算复合（`-N`）校验和（COMPOSITE；CRC64NVME/显式 FULL_OBJECT → 501），complete XML 的 Checksum* 声明与存量对照（不符 BadDigest） |

静态网站托管**已支持**（docs/static-website.md）：按桶匿名 GET/HEAD 对象读、
index/error 文档、`?website` 动态配置 API（root 专属）与
`x-amz-website-redirect-location`。

明确不支持（返回 `NotImplemented`）：versioning、ACL 细粒度（只认
private）、bucket policy、lifecycle Transition/按 tag 过滤、SSE-C/KMS、
Object Lock、storage-class（只认 STANDARD）、presigned POST（presigned
GET/PUT 的 query 签名**支持**，见 §3.4）。拒绝面同时覆盖 query 子资源
（白名单反转，名单外 → 501）与**请求头**（`x-amz-server-side-encryption*` /
`x-amz-object-lock-*` / `x-amz-grant-*` 等携带即 501，不再静默吞掉回 200）。
PutObject/UploadPart 缺 Content-Length/Transfer-Encoding → 411
`MissingContentLength`；`list-type=3` → `InvalidArgument`。

## 2. 路由与寻址

- **path-style**（默认）：`GET /{bucket}/{key...}`。
- **virtual-host style**：`Host: {bucket}.gw.example.com`，需配置
  `http.base_domain` 后启用；两种同时支持。
- 操作识别 = method + 路径形态 + query 标志位（如 `?uploads`、`?uploadId=`、
  `?delete`、`?list-type=2`）。Router 用一张显式的分派表而非正则，保证
  可读与可测：

```cpp
// (method, scope, query-flag) → handler
{ "GET",  Scope::Bucket, "list-type=2" } → ListObjectsV2Handler
{ "POST", Scope::Object, "uploads"     } → CreateMultipartHandler
{ "PUT",  Scope::Object, "partNumber"  } → UploadPartHandler
{ "PUT",  Scope::Object, {}            } → PutObjectHandler   // 兜底
...
```

## 3. AWS Signature V4 认证

自实现（协议公开且稳定，避免为验签引入整只 SDK），代码在 `src/s3/auth/`。

### 3.1 校验流程

```text
1. 解析 Authorization 头（或 query 参数，presigned 场景）
   → access_key, date, region/service, SignedHeaders, Signature
2. 用 access_key 查本地凭证表 → secret_key（查不到 → InvalidAccessKeyId）
3. 重建 CanonicalRequest：
   method + canonical_uri(未解码 path 再按 SigV4 规则编码)
          + canonical_query(排序+编码)
          + canonical_headers(SignedHeaders 列出的头)
          + hashed_payload
4. StringToSign = "AWS4-HMAC-SHA256" + 时间戳 + scope + sha256(CanonicalRequest)
5. 派生 signing key（HMAC 链：date→region→service→"aws4_request"）
   → 计算签名，恒定时间比较（防时序侧信道）
6. 校验时钟偏移：|x-amz-date - now| > 15min → RequestTimeTooSkewed
```

### 3.2 Payload 校验的三种形态

| `x-amz-content-sha256` | 处理 |
| --- | --- |
| 十六进制摘要 | 流式收 body 时增量算 SHA256，收完比对，不符 → XAmzContentSHA256Mismatch。注意：**必须在 body 全部消费后才能给出 2xx**，因此 PUT 的成功响应天然在校验之后 |
| `UNSIGNED-PAYLOAD` | 跳过 body 校验（HTTPS 下常见），仅验头签名 |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` | aws-chunked 编码：在 L2 提供 `ChunkedSigV4BodyReader` 装饰器，逐 chunk 剥壳并验证 chunk 签名链，向下游暴露纯数据流。放在 L2 而非 driver，所有 driver 免费获得支持 |
| `STREAMING-*-TRAILER` 两变体 | 2025 起 SDK 的默认上传形态：`STREAMING-UNSIGNED-PAYLOAD-TRAILER`（chunk 不签名，靠 trailer 校验和保完整性）与 `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER`（chunk 签名链 + trailer 签名）。trailer 段按行严格解析并双向对照 `x-amz-trailer` 声明（未声明的 trailer 拒绝、声明了没到的也拒绝），声明的 `x-amz-checksum-*` 对解码后全量 payload 校验（错格式 InvalidDigest / 不匹配 BadDigest）；signed 变体还验 `x-amz-trailer-signature`（`AWS4-HMAC-SHA256-TRAILER` string-to-sign，对规范化 trailer 串哈希，链在末 chunk 签名之后）。trailer 段上限 16KiB |

### 3.3 与流式模型的配合（含 trailing checksum）

签名校验装饰器模式：`Sha256VerifyingReader` 包装原始 `BodyReader`，
透传数据同时累积摘要；存储层消费的就是这个装饰后的 reader。校验失败时
`complete()` 抛异常 → handler 走错误路径 → LocalFs 的 staging 临时文件
被 RAII 清理，不会留下半个对象。

Trailing checksum 的分工：头部声明的 `Content-MD5` / `x-amz-checksum-*` 由
`ChecksumVerifyingReader`（checksum_guard.h）在 chunked 剥壳装饰器**之外**
校验；`x-amz-trailer` 声明的校验和的期望值到 payload 结束才出现，因此在
`ChunkedSigV4BodyReader` **内部**边解码边累积摘要、解析出 trailer 后比对，
两处共用同一张算法表（crc32 / crc32c / crc64nvme / sha1 / sha256，
`checksum_spec`）与 `StreamingDigest` 增量摘要器。
`x-amz-checksum-algorithm` / `x-amz-sdk-checksum-algorithm` 声明也被强制：
未知算法 InvalidRequest；带 body 的请求声明了算法却既无对应头也无对应
trailer 声明 → InvalidRequest（无 body 的声明如 CreateMultipartUpload 放行，
分片各自带各自的校验和）。

### 3.4 Presigned URL

`X-Amz-Signature` 等参数出现在 query 中：同一套 canonical 算法，payload 按
`UNSIGNED-PAYLOAD` 处理，额外校验 `X-Amz-Expires`。过期只约束过去一侧；
`X-Amz-Date` 超前服务器 15min 以上同样拒绝（AccessDenied "Request is not
valid yet"），防未来时间戳把有效期无限外推。

### 3.5 凭证管理与 STS 会话

STS 会话凭证（roadmap §2.6）：`AssumeRole` 铸造 `L3SA` 前缀的会话
AK/SK/token（TTL 900–43200s），policy 继承调用者快照；数据面请求需带
`x-amz-security-token`（header 或 presigned query）——token 不符
`InvalidToken`、过期 `ExpiredToken`（重试信号）、永久 AK 携带 token 同样
拒绝。会话表为内存态（单实例语义，会话本就短命），session 不入 `.sys`、
不能是 root、不能再 AssumeRole。



配置文件静态 AK/SK 表（secret 支持环境变量引用）是一期形态；二期已落地
三来源模型（static=root / file / dynamic）、凭证文件热加载与轻量的
per-credential policy。运行期生成/查询 AK/SK 并持久化到存储的方案与
二期设计详见 [credential-management.md](credential-management.md) §10。

## 4. XML 编解码

S3 的 XML 结构简单且模式固定，不引入大型 XML 库：

- **生成**：小型 writer（转义 + 嵌套栈），每个响应类型一个纯函数
  `to_xml(const ListResult&) → std::string`。
- **解析**：仅三处需要解析请求 XML——CompleteMultipartUpload、
  DeleteObjects、CreateBucket(LocationConstraint)。结构都很浅，用
  自写的小型解析器（`src/s3/xml.cc`）处理，限制请求 XML ≤ 1MiB。

## 5. 错误处理

单一事实来源 `src/s3/errors.h`：

```cpp
enum class S3ErrorCode { NoSuchBucket, NoSuchKey, AccessDenied,
                         InvalidAccessKeyId, SignatureDoesNotMatch,
                         BucketNotEmpty, EntityTooLarge, InvalidPart,
                         NoSuchUpload, PreconditionFailed, NotImplemented,
                         SlowDown, InternalError, ... };

struct S3Error : std::exception {   // L2/L3 统一抛这个
    S3ErrorCode code; std::string message; std::string resource;
};
// 表驱动：code → (http_status, wire_code_string)
```

响应体为标准 S3 错误 XML（含 `Code/Message/Resource/RequestId`）。
`RequestId` 每请求生成（时间戳+随机），同时写入访问日志与 `x-amz-request-id`
响应头，作为端到端排查的关联键。

未知异常 → `InternalError`(500)，日志记完整堆栈，响应不泄漏内部信息。

本实现自定义（AWS 无对应）的错误码：`QuotaExceeded`(403，配额)、
`NoSuchQuotaConfiguration`(404)、`NoSuchTenant`(404)、`TenantAlreadyExists`(409)、
`TenantNotEmpty`(409)，语义见 [multi-tenancy.md](multi-tenancy.md)。

## 6. 一致性与语义说明（对客户端的承诺）

- **PUT 后读**：LocalFs 借助 rename 原子性提供 read-after-write 强一致；
  CloudProxy 继承远端一致性（现代 S3 均为强一致）。
- **并发 PUT 同 key**：last-write-wins，无版本保留。
- **ETag**：单段 = 内容 MD5；multipart = `md5(分片md5拼接)-N`，两个后端一致。
- **条件请求**：GET/HEAD/PUT 支持 If-Match / If-None-Match，
  CopyObject / UploadPartCopy 支持 x-amz-copy-source-if-*。
  两处与 AWS 逐字对齐的边界：PUT `If-None-Match` 仅支持 `*`（AWS conditional
  writes 同样只支持 `*`，带 ETag → 501）；GET 多段 Range（含逗号）AWS 不支持，
  行为同 AWS——整个 Range 头忽略、返回 200 全量。
  **原子性范围**：条件判定与提交同处后端自身的原子提交点（localfs 提交段
  per-key 锁、duostore 元数据事务、cloudproxy 透传上游条件头），L2 只做
  无锁快速失败预检——duostore/cloudproxy 形态下跨实例成立；localfs/xlocalfs
  的互斥为**进程内**，多实例共享同一文件系统的部署不在支持范围
  （[architecture.md](architecture.md) 的非目标：数据面单实例假设）。
- **copy-source 的安全边界**：`x-amz-copy-source` 不走 dispatch 的路径拦截，
  两道检查分处两地：`.` 开头的保留 bucket（`.sys`）在 `parse_copy_source()`
  （src/s3/handlers/common.h）拒绝（InvalidBucketName）；per-credential
  policy 的源桶"读"授权在 dispatch 入口（src/s3/service.cc）单独执行
  （AccessDenied）。

## 7. 可观测性

构建身份 `lights3_build_info{version,commit,build_type}`（恒 1，roadmap §6.3，
[deployment.md §1](deployment.md)）。
L1 连接与限流指标（roadmap §4.2）：`lights3_http_connections_total{result}`、
`lights3_http_connections_active`、`lights3_http_keepalive_closes_total`、
`lights3_http_timeouts_total{phase=idle|header|body|write}`、
`lights3_ratelimit_rejections_total{scope=ip|ak}`，见 [http-adapter.md §2.2–§2.3](http-adapter.md)。

- **访问日志**（roadmap §5.2）：每请求一行，经独立的 `lights3.access` logger
  与运行日志共用 sink。文本格式（`log.format: text`）：
  `access <request_id> <AK|-> <method> "<path>" <status> <bytes> <总耗时>ms
  api=<路由名> backend=<后端名>:<后端耗时>ms remote=<客户端地址> bucket=<bucket|->
  ttfb=<ms>ms ua="<User-Agent>"`——前七字段对齐 S3 server access log，`path` 与
  UA 加引号并转义（`"`、`\`、控制字符），含空格的 key 不再破坏切分；后端耗时
  由包在路由后端外的计时装饰器（`storage/metered_backend.h`）经请求的取消令牌
  回填，"网关排队慢还是后端慢"一行可判。**流式响应**（GetObject 等）的行在
  响应体读尽时写出：`bytes` 是实际发出的字节数、总耗时含传输、`ttfb` 是响应头
  就绪时刻（即指标直方图记录的量）；客户端中途断开则由装饰器析构补写并标
  `truncated=1`。JSON 格式（`log.format: json`）下整条运行日志都是每行一个对象
  `{"ts","level","thread","msg"}`，访问记录以平铺字段落在同一对象里：
  `request_id / remote / ak / method / path / query / bucket / key / status /
  bytes / ms / ttfb_ms / auth_ms / handler_ms / backend_ms / backend_calls / api /
  backend / ua`，缺省值省略而非空串（`ak` 匿名时不出现），可直接接 Loki/ELK。
- **慢请求通道**：`log.slow_request_threshold`（如 `500ms`，0 = 关，可热更新）——
  总耗时达到阈值的访问行升为 WARN 并附阶段耗时（文本：`slow=1 auth=<ms>
  handler=<ms> backend_calls=<n>`；JSON：`"slow":true` + 上述字段），生产
  `log.level: warn` 时仍能看到值得看的请求。auth = 从进入 dispatch 到身份验证
  完成，handler = 路由处理器整段（后端耗时是其子集）。
- **分布式 trace（轻量层，roadmap §5.4，`core/trace.h`）**：接受 W3C
  `traceparent`（严格校验：版本 00、小写 hex、非零 id；畸形即视为无）与
  `tracestate`（原样透传）；缺失时网关自起一条 trace。每个请求持有自己的 span，
  来访的 span 记为 parent。落点：访问行文本槽 `trace=<trace_id>/<span_id>
  [parent=<span>]`、JSON 字段 `trace_id / span_id / parent_span_id`、dispatch
  的取消/内部错误 WARN/ERROR 行 `trace=`、审计数据面记录 `trace_id`；响应头
  `traceresponse: 00-<trace_id>-<span_id>-<flags>`（W3C 草案）让无 tracer 的
  客户端也能引用。出站：cloudproxy 每个远端请求带网关自身 span 的 `traceparent`
  （远端以之为 parent，链接落在双方日志里）+ `tracestate`（[cloudproxy-backend.md §2.2](cloudproxy-backend.md)），
  多跳（网关→cloudproxy 远端 lights3 / tiered 冷层）按同一 trace id 关联；
  后台任务（tiered 降冷、GC）无请求上下文，不带 trace 头。不导出 span：
  otel-cpp 全量埋点仍是长期项。
- **sink 与异步**：`log.file` 为空写 stderr，否则写按大小轮转的文件
  （`max_size` / `max_files`，每秒定时 flush，WARN 及以上立即 flush）；
  `log.async: true` 时请求线程只入队（容量 `async_queue`），单独的写线程落盘，
  队列满时按 `async_overflow` 阻塞或覆盖最旧记录；进程退出前 `Logger::shutdown`
  排空队列。审计日志（[multi-tenancy.md §5](multi-tenancy.md)）仍是独立文件，
  不受这些开关影响。
- **Metrics**（Prometheus 文本格式，`GET /-/metrics`，与数据面同一监听；
  `http.metrics_access: anonymous`（默认，经典抓取）或 `root`——后者要求
  GET 由 root 凭证签名，因输出含 bucket 名与后端拓扑等业务信息；可热更新，
  认证整体关闭时无意义；`/-/healthz|readyz` 始终匿名）：
  请求数/延迟直方图（全局 + 按 `(api, backend)` 分维：
  `lights3_api_requests_total{api,backend,class}`、
  `lights3_api_request_duration_seconds{api,backend}`，api 取自分派表 `Route::name`，
  CopyObject/UploadPartCopy 按 `x-amz-copy-source` 细分）、在途请求数、线程池
  队列深度、multipart 活跃数。后端级指标经 `core/metrics.h` 注册表
  （backend=<name> 标签）追加在 L2 请求指标之后输出，其中
  `lights3_backend_op_seconds{backend,op}` 直方图与
  `lights3_backend_errors_total{backend,op}`（5xx/传输异常计错误，4xx 不计）
  由计时装饰器对六个后端统一产出（roadmap §5.1）。
  roadmap §5.3 补口：**精确状态码** `lights3_responses_by_status_total{status}`
  （稀疏输出，出现过的码才有一行；206/304 比例是网站/CDN 场景的关键量）；
  **准入闸门** `lights3_admission_wait_seconds` 直方图（每个请求一样本，直接
  拿到许可的落在首桶）、`lights3_admission_queued_total`（排过队的请求数）、
  `lights3_admission_cancelled_total`（排队中被取消 → 503）、
  `lights3_transfer_stalls_total{direction=in|out}`（停滞守卫掐断次数）；
  **L1** `lights3_http_requests_total`（L1 解析成功的请求数，÷ accepted 即
  keep-alive 复用率）、`lights3_http_tls_handshakes_total{result=ok|failed}`、
  `lights3_http_parse_errors_total`（请求行/头部/framing 畸形；详见
  [http-adapter.md §2.2](http-adapter.md)）；**网站面**
  `lights3_website_events_total{event=anon_read|index_rewrite|error_document|redirect|throttled}`
  （[static-website.md §6](static-website.md)）。
- **健康检查**：`GET /-/healthz`（进程存活）与 `GET /-/readyz`
  （各后端探活：对所有后端一律 `co_await list_buckets()`，任一失败
  返回 503 并在响应体中报告失败的后端名）。三个读端点只认 GET/HEAD，
  其余方法 405。
  `/-/` 前缀不与合法 bucket 名冲突（S3 bucket 命名不允许该形态）——该论断
  **仅对 path-style 寻址成立**，内部端点分流因此以"非 vhost"为前置：vhost
  寻址下 `req.path` 整体是对象 key，`/-/metrics` 是合法键名，按普通对象
  读写、不被内部端点遮蔽。

## 8. 测试策略

1. **单元测试**：SigV4 用 AWS 官方测试向量全量回放；XML、路由分派表、
   错误映射逐项覆盖。
2. **后端一致性套件**：同一组用例参数化跑 LocalFs / CloudProxy(对 MinIO) /
   内存 mock。
3. **驱动一致性套件**：同一组 HTTP 行为用例跑所有编译进来的 driver。
4. **端到端**：CI 起网关（LocalFs 后端），用 aws cli 与 boto3 跑真实操作
   脚本（含 100MB 级 multipart、Range 下载、presigned URL）；
   另跑 MinIO 的 `mint` 兼容性测试集作为回归门槛——已落地
   `tests/e2e/run_mint.sh`（起 lights3 + `minio/mint` 容器对打，docker 不可用
   时显式 SKIP）。mint 依赖 docker daemon 权限，本地开发机通常跑不了；全集含
   versioning/tagging 等明确不支持的 API，以 `s3cmd`、`awscli` 子集起步——
   自 roadmap §6.1 起挂为 ctest `mint`（无 docker 显示 Not Run），跑完打印
   每套件 PASS/FAIL 基线（[testing.md §6](testing.md)）。
