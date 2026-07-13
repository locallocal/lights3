# 05 S3 协议实现

## 1. API 范围

首期实现覆盖主流客户端（aws cli、boto3、s3cmd、MinIO SDK）日常操作所需的子集：

| 类别 | API | 备注 |
| --- | --- | --- |
| Service | ListBuckets | 聚合各后端 |
| Bucket | CreateBucket / DeleteBucket / HeadBucket | 无 region 约束，LocationConstraint 忽略但回显配置 region |
| Object | PutObject / GetObject / HeadObject / DeleteObject / DeleteObjects(批量) / CopyObject | Get 支持 Range 与条件请求（If-Match/If-None-Match/If-Modified-Since） |
| List | ListObjectsV2（含 V1 兼容） | prefix / delimiter / max-keys / continuation-token |
| Multipart | CreateMultipartUpload / UploadPart / CompleteMultipartUpload / AbortMultipartUpload / ListParts / ListMultipartUploads | UploadPartCopy 二期 |

明确不支持（返回 `NotImplemented`）：versioning、ACL 细粒度（只认
private）、policy、website、lifecycle、SSE-C/KMS、Object Lock、
presigned POST（presigned GET/PUT 的 query 签名**支持**，见 §3.4）。

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

### 3.3 与流式模型的配合

签名校验装饰器模式：`SigV4VerifyingReader` 包装原始 `BodyReader`，
透传数据同时累积摘要；存储层消费的就是这个装饰后的 reader。校验失败时
`complete()` 抛异常 → handler 走错误路径 → LocalFs 的 staging 临时文件
被 RAII 清理，不会留下半个对象。

### 3.4 Presigned URL

`X-Amz-Signature` 等参数出现在 query 中：同一套 canonical 算法，payload 按
`UNSIGNED-PAYLOAD` 处理，额外校验 `X-Amz-Expires`。

### 3.5 凭证管理

首期：配置文件静态 AK/SK 表（secret 支持环境变量引用），全部凭证等价于
超级用户（无 IAM policy）。接口上预留 `ICredentialProvider`，
后续可接文件热加载或外部服务。

## 4. XML 编解码

S3 的 XML 结构简单且模式固定，不引入大型 XML 库：

- **生成**：小型 writer（转义 + 嵌套栈），每个响应类型一个纯函数
  `to_xml(const ListResult&) → std::string`。
- **解析**：仅三处需要解析请求 XML——CompleteMultipartUpload、
  DeleteObjects、CreateBucket(LocationConstraint)。结构都很浅，用
  vendored 的 pugixml（header-only，MIT）解析，限制请求 XML ≤ 1MiB。

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

## 6. 一致性与语义说明（对客户端的承诺）

- **PUT 后读**：LocalFs 借助 rename 原子性提供 read-after-write 强一致；
  CloudProxy 继承远端一致性（现代 S3 均为强一致）。
- **并发 PUT 同 key**：last-write-wins，无版本保留。
- **ETag**：单段 = 内容 MD5；multipart = `md5(分片md5拼接)-N`，两个后端一致。
- **条件请求**：GET/HEAD/PUT 支持 If-Match / If-None-Match，
  CopyObject 支持 x-amz-copy-source-if-*。

## 7. 可观测性

- **访问日志**：每请求一行结构化日志（request_id、AK、method、bucket/key、
  status、字节数、总耗时、后端耗时），格式对齐 S3 server access log 便于
  复用现有分析工具。
- **Metrics**（Prometheus 文本格式，`GET /-/metrics`，仅内网 bind）：
  请求数/延迟直方图（按 API 与后端分维度）、在途请求数、线程池队列深度、
  multipart 活跃数、后端错误率。
- **健康检查**：`GET /-/healthz`（进程存活）与 `GET /-/readyz`
  （各后端探活：LocalFs 写探针文件、CloudProxy HeadBucket）。
  `/-/` 前缀不与合法 bucket 名冲突（S3 bucket 命名不允许该形态）。

## 8. 测试策略

1. **单元测试**：SigV4 用 AWS 官方测试向量全量回放；XML、路由分派表、
   错误映射逐项覆盖。
2. **后端一致性套件**：同一组用例参数化跑 LocalFs / CloudProxy(对 MinIO) /
   内存 mock。
3. **驱动一致性套件**：同一组 HTTP 行为用例跑所有编译进来的 driver。
4. **端到端**：CI 起网关（LocalFs 后端），用 aws cli 与 boto3 跑真实操作
   脚本（含 100MB 级 multipart、Range 下载、presigned URL）；
   另跑 MinIO 的 `mint` 兼容性测试集作为回归门槛。
