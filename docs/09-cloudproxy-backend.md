# 09 CloudProxyBackend：映射公有云的代理后端

> 状态：P1–P5 已实现（`src/storage/cloudproxy/`），单测双栈自举跑一致性套件
> （`test_cloudproxy.cc`），e2e 双实例场景 `e2e_cloudproxy` / `e2e_tiered_cloudproxy`
> 全绿。§8.2 指标与 §2.3 `control_in_pump` 未做；`force_path_style: false`
> 未实现（配置加载期报错）。承接 docs/04 §4 的概述与 docs/08 §10 P5 的预留
> （tiered 的 cloud 侧接入真实云端）。本文档确定实现路线为**自签 SigV4 +
> vendored httplib 直连**（docs/04 §4.1 的路线 B）。

## 1. 目标与非目标

目标：

| 目标 | 说明 |
| --- | --- |
| 1:1 映射远端 S3 兼容存储 | 本地 bucket ↔ 远端 bucket（`bucket_prefix` 前缀映射），远端可以是 AWS S3、MinIO、OSS/COS 的 S3 兼容端点，甚至另一个 lights3 实例 |
| 实现 `IStorageBackend` 全接口 | 含 bucket CRUD、对象数据面、list、multipart 全套（docs/04 §1，以 `src/storage/backend.h` 为准） |
| 全链路流式 | GET/PUT 不在内存缓冲整对象，背压传导到 TCP 层（延续 docs/04 §4 原则） |
| 凭证隔离 | 客户端用网关本地 AK/SK 认证；网关用自己的云凭证访问远端；两者绝不混淆透传 |
| 支撑 tiered P5 | 作为 docs/08 TieredBackend 的 cloud 侧后端（首要消费方），验收清单见 §9 |

非目标（首期）：

- 不做 IAM Role / IMDS / STS 临时凭证自动获取——这是放弃云 SDK 的主要代价，
  凭证为静态 AK/SK（配置支持 `${ENV}` 展开）；
- 不做多 endpoint 负载均衡/故障转移，一个 backend 实例对应一个远端端点；
- 不缓存远端数据——缓存是 TieredBackend 的职责（docs/08 §6），职责分离；
- 不代理远端的 ACL / policy / versioning / lifecycle 等扩展 API，仅覆盖
  `IStorageBackend` 表达的对象语义。

## 2. 架构定位与路线决策

### 2.1 路线反转：A（SDK）→ B（自签直连）

docs/04 §4.1 原首选路线 A（aws-sdk-cpp 封装）。本设计**反转为路线 B**，理由：

1. **出方向签名已就绪**：`SigV4Authenticator::sign()`（`src/s3/auth/sigv4.h`）
   在实现验签时同步实现，头注释即写明"签名端供单测与后续 cloudproxy 转发复用"；
   底层 crypto 原语（`src/core/util/crypto.h` 的 HMAC-SHA256、增量 MD5/SHA256）
   齐全。路线 B 的最难部分（签名）实际是零新增代码。
2. **HTTP 客户端已 vendored**：`third_party/httplib` 0.20.0 的 `httplib::Client`
   支持流式下载（ContentReceiver）与流式上传（ContentProvider）；HTTPS 仅需
   `CPPHTTPLIB_OPENSSL_SUPPORT` 编译宏 + 链接 `OpenSSL::SSL`（§8.3）。
3. **SDK 引入代价不可接受**：aws-sdk-cpp 依赖树庞大（libcurl 等），与本项目
   "依赖全靠 vendored 子模块、零系统包依赖"的构建约束冲突。
4. **线程模型可控**：自实现可与项目协程/ThreadPool 模型精确融合；SDK 的
   同步/异步 API 反而要做第二次适配（docs/04 §4 已指出同步 SDK 占线程的瓶颈）。

代价（已列入 §1 非目标）：无自动凭证链；S3 协议边角（如 §4.4 complete 的
200-带错误体）需自行处理——好在覆盖面只有 `IStorageBackend` 这一层接口，
且远端交互可用一致性套件对着 lights3 自身回归（§10）。

### 2.2 类结构与通用请求管线

```text
src/storage/cloudproxy/
├── cloudproxy_backend.h/.cc    # CloudProxyBackend : IStorageBackend
└── remote_client.h/.cc         # ClientPool（httplib::Client 连接池）
                                # + RemoteRequest 通用管线 + 错误映射
（复用）s3/auth/sigv4            # 出方向签名
（复用）s3/xml                   # 远端 XML 响应解析 / 请求体生成
（复用）core/util/{crypto,uri,time}
（复用，P1 提取）http/pushpull    # BlockQueue / QueueBodyReader（§3.1）
```

每个操作走同一条管线：

```text
① 构造中立 http::HttpRequest（仅为签名计算）：
     method / raw_path / raw_query / headers（含 host 与业务 x-amz-*）
② authenticator_.sign(req, cred, payload_hash)
     —— 补 x-amz-date / x-amz-content-sha256 / Authorization
③ 把 req.headers 搬运为 httplib::Headers，经 ClientPool 取连接发送
④ 响应：2xx → 解析头/XML 组装返回值；否则 map_remote_error() 抛 s3::S3Error
```

**签名衔接方式**：`sign()` 就地修改 `HttpRequest`，其 canonical 计算只依赖
`method`、`raw_path`（即 canonical URI）、`raw_query`（内部自行排序）与
`headers`（SignedHeaders 自动取 host + 全部 `x-amz-*`）。因此选择"构造最小
HttpRequest 只为签名，再搬运 headers"——另一种做法（直接对 httplib::Headers
重写 canonical 计算）等于复制一份签名逻辑，违反复用初衷。要点：

- `raw_path = "/" + aws_uri_encode(remote_bucket + "/" + key, /*encode_slash=*/false)`
  （`core/util/uri.h` 现成）；`raw_query` 按操作拼（`uploads`、
  `list-type=2&prefix=...` 等），值用 `aws_uri_encode(v, true)`；
- **host 一致性陷阱**：httplib 实际发出的 `Host` 头是 `host[:port]`
  （非默认端口带 port）。签名侧 HttpRequest 里预置的 `host` 必须与之
  **逐字节一致**（`sign()` 不会代填），否则远端报 SignatureDoesNotMatch。
  实现上从 endpoint 解析出规范 host 串，两处共用一个值；
- 业务头（`x-amz-meta-*`、`Range`、`Content-Type`）在 ② 之前放入。
  `x-amz-*` 自动进 SignedHeaders；`Range`/`Content-Type` 不参与签名
  （SignedHeaders 只含 host + x-amz-*，S3 接受）；
- Authenticator 实例：每 backend 一个，
  `SigV4Authenticator::build(AuthConfig{云凭证, 远端 region, "s3"})`，
  region 独立于本地 L2 验签用的 region。

### 2.3 线程模型：私有 pump 线程，不占共享池

`httplib::Client` 是同步阻塞 API：`Get/Put` 一直阻塞到整个传输结束。若把这些
调用放进共享 ThreadPool，存在**全局死锁**：并发代理请求数 ≥ 池大小时，全部
池线程阻塞在 pump（等 §3 的队列消费者），而消费者——handler 协程——恰恰
需要池线程才能 resume。因此：

- **数据面 pump 跑在 CloudProxyBackend 私有线程上**（per in-flight transfer
  一个 `std::thread`，上限 = `max_connections`，由 ClientPool 容量自然限制）；
- 控制面短请求（HEAD/DELETE/LIST 等，响应体小）仍可 `co_await pool->schedule()`
  后在池线程同步调用——单次占用时间与 localfs 的一次磁盘操作同量级，可接受；
  若远端 RTT 高导致池占用可观，配置 `control_in_pump: true` 让控制面也走
  私有线程（P4 视压测决定默认值）。

这正是 docs/03 §3"如出现云端慢请求占满池饿死本地盘，再按 backend 配置独立池"
预留的落地形态——只是独立的不是通用 ThreadPool，而是 cloudproxy 自管的
pump 线程集。

## 3. 数据面流式设计

### 3.1 GET：推转拉（复用 BlockQueue 先例）

httplib 的 ContentReceiver 是推模型回调，而 `get_object` 要返回拉模型
`BodyReader`。项目在 httplib **server** 驱动里已解决过一模一样的问题：
`src/http/drivers/httplib/httplib_server.cc` 的 `BlockQueue`（按字节限容的
有界缓冲，push 返回 false 表示消费方取消，pop 返回 0 表示 EOF、异常表示
中途失败）+ `QueueBodyReader`。**P1 把这两个类从该文件的匿名命名空间提取为
共享组件（`src/http/pushpull.h`），server 驱动与 cloudproxy 共用。**

```text
get_object(bucket, key, range)：
① 从 ClientPool 取连接，启动 pump 线程，自身阻塞等"headers 就绪"信号
     （promise/条件变量，超时 = request_timeout；调用方协程此刻本来就在等
      首字节；它在共享池、pump 在私有线程，无互等）
② pump 线程：client.Get(path, headers, ResponseHandler, ContentReceiver)
     - ResponseHandler（响应头到达即回调）：
         2xx → 从头组装 ObjectMeta（§4.1 的头映射）+ 解析 Content-Range，
               经 promise 交还 ①；
         错误状态 → 继续收完错误体做 §5 映射，经 promise 交还异常
     - ContentReceiver：循环 queue->push(data, n)；push 返回 false 则
       返回 false 中止传输
     - 结束：queue->close(ok)
③ ① 拿到 meta 后 co_return ObjectStream{meta,
       make_unique<CancelOnDropReader>(QueueBodyReader(queue, len)), range}
```

- **背压**：BlockQueue 容量（默认 1 MiB，可配）→ push 阻塞 → httplib 停止
  recv → TCP 窗口收紧 → 远端限速。与 server 侧同一机制，天然传导；
- **取消**：客户端断连/handler 异常时 reader 析构 → 对 queue 调 `cancel()`
  → push 返回 false → ContentReceiver 返回 false → httplib 中止本次传输。
  需要在 QueueBodyReader 外包一层"析构即 cancel"（server 侧先例是入方向，
  无此需求）。注明代价：被中止的连接作废，归还 ClientPool 后由 httplib
  在下次请求时自动重连（损失一次 keep-alive）。

### 3.2 PUT / upload_part：拉转拉，经队列解耦线程

入参 `BodyReader&` 是拉模型，httplib 的 ContentProvider 也是拉模型，但两者
**不能在同一线程衔接**：Provider 是同步回调，无法在里面驱动 `co_await
body.read()`；且 BodyReader 的契约是只由 handler 协程链驱动。对称复用
BlockQueue、方向反转：

```text
put_object(bucket, key, meta, body)：
① 启动 pump 线程：client.Put(path, headers, length, ContentProvider, ct)
     Provider 从 queue pop 写入 DataSink；pop 到 EOF 后结束
② 调用方协程（留在共享池）循环：
     co_await body.read(64KiB 缓冲) → HashStream(Md5) 增量更新
       → queue->push()；EOF 后 queue->close(ok=true)
     body.read() 抛异常（客户端断连）→ queue->close(ok=false) →
       Provider 返回 false 中止上传
③ join pump，取远端响应：2xx → 校验 ETag（§6）→ co_return PutResult
```

**payload hash 决策：`UNSIGNED-PAYLOAD`**。三个候选的取舍：

| 候选 | 取舍 |
| --- | --- |
| 精确 SHA256 | 需先读完整个 body 才能发第一个字节——违反流式原则，否决 |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` | 要实现出方向 aws-chunked 组帧 + 逐块签名链，复杂度高，收益仅是明文 HTTP 下的完整性——不做 |
| `UNSIGNED-PAYLOAD`（取） | AWS/MinIO/lights3 自身均接受；HTTPS 下完整性由 TLS 保证，再叠加 §6 的 ETag 端到端比对做补偿 |

文档级约定：**明文 HTTP + unsigned payload 的组合仅建议用于内网/测试**；
生产走 HTTPS。

**Content-Length 两种情况**：

- `body.length()` 有值（绝大多数：客户端带 Content-Length；aws-chunked 入流
  也有 `x-amz-decoded-content-length` 兜底）→ 定长 `client.Put(...)`,
  兼容性最好；
- nullopt（真 chunked 且无长度）：AWS S3 不接受裸 `Transfer-Encoding:
  chunked`（要求定长或 aws-chunked）。首期抛 `NotImplemented`（罕见路径），
  P4 视需要补 `STREAMING-UNSIGNED-PAYLOAD-TRAILER` 出方向组帧。

### 3.3 Range GET 透传

`range` 参数格式化为 `Range: bytes=...`（`first-last` / `first-` / `-suffix`
三种形态原样透传）：

- 远端 206：从 `Content-Range: bytes a-b/total` 解析 total 填 `meta.size`
  （接口约定 size 为对象全长），生效区间填 `ObjectStream.range`；
- 远端 416 → `InvalidRange`；
- 远端异常返回 200（忽略 Range 的非标端点）：按全量处理，`range` 置空——
  L2 会按全量 200 响应，语义仍正确。

## 4. 控制面

### 4.1 head / delete 与头映射

- **head_object**：HEAD 请求 → 响应头组装 `ObjectMeta`：`ETag` 去引号存 hex
  （`backend.h` 约定不带引号）、`Content-Length` → size、`Content-Type`、
  `Last-Modified` 用现成 `util::parse_http_date`、`x-amz-meta-*` 去前缀进
  `user_meta`。404 → `NoSuchKey`。
- **delete_object**：DELETE；远端 204 与 404 都视为成功（S3 幂等删除语义，
  与本地后端一致）。

### 4.2 list：ListObjectsV2 + 恒用 start-after 分页

本地 `ListOptions.start_after` 的 token 语义是"上一页最后一个 key"，而 V2 的
`continuation-token` 是远端不透明串——两者不匹配。解法：**每页都发**
`?list-type=2&start-after=<token>&prefix=&delimiter=&max-keys=`。`next_token`
的取法：末元素是普通 key 时即该 key；是 common prefix 组时取
`group_skip_token(prefix)` = prefix 用 0xff 填充到 key 长度上限（1024）——
排他语义下组内 key 全部被跳过、组外后继 key（含与"末字符 +1"同名的字面 key）
一个不漏。`start-after` 对任意页都合法，无状态（正确性优先于
continuation-token 可能的服务端优化）。

- 响应解析用现成 `s3::xml_parse`（`src/s3/xml.h`，浅结构解析器，属性跳过——
  ListBucketResult 只有 xmlns 属性，够用）；
- **工具缺口**：响应里 `LastModified` 是 ISO8601，`core/util/time.h` 现只有
  `iso8601()` 格式化——P1 补 `parse_iso8601()`；
- **list_buckets**：GET / → 解析 ListAllMyBucketsResult，只保留名字带
  `bucket_prefix` 前缀的，剥前缀后返回（其余是远端账号下无关 bucket）。

### 4.3 bucket 操作：直接映射远端

不做"本地虚拟 bucket"——tiered P5 需要远端 bucket 真实存在，且虚拟化会引入
本地状态，违背无状态代理定位。

- 名称映射：`remote = bucket_prefix + local`；拼接后按 S3 规则校验（长度
  ≤ 63 等，复用现有 bucket 名校验），超限在**配置加载期**即报错；
- `create_bucket` → PUT bucket（region ≠ us-east-1 时带
  CreateBucketConfiguration XML，`XmlWriter` 生成）；409 已存在 →
  `BucketAlreadyOwnedByYou` 透传；
- `bucket_exists` → HEAD bucket：200 → true；404 → false；**403 → true**
  （与 AWS HeadBucket 语义一致：存在但无权也返回 403，视为存在并记 warn 日志）。
  该歧义意味着调用方不得拿 bucket_exists 结果决定是否建桶——tiered 的
  `ensure_cloud_bucket` 因此直接 create + 409 视为已存在，不经 exists 判断；
- `delete_bucket` → DELETE；409 → `BucketNotEmpty`。

### 4.4 multipart 透传

- `create_multipart` → POST `?uploads`（meta 的 content_type / user_meta
  此时上头）→ 解析 InitiateMultipartUploadResult 取 UploadId，
  **upload_id 原样透传**，网关不落任何本地状态；
- `upload_part` → PUT `?partNumber=N&uploadId=...`，流式同 §3.2，ETag 透传；
- `complete_multipart` → POST，body 用 `XmlWriter` 生成
  （CompleteMultipartUpload/Part/PartNumber/ETag，ETag 需补回引号）。
  **必须处理 S3 特有的"200 OK 但 body 是 `<Error>`"**：complete 耗时长时
  AWS 先回 200 再在体内报错（自实现路线最著名的坑）——收完 body 后按根节点
  名判别 CompleteMultipartUploadResult / Error，后者走 §5 错误映射；
- `abort_multipart` → DELETE `?uploadId=...`，404 → `NoSuchUpload`；
- `list_parts` / `list_multipart_uploads` → GET 对应查询串，XML 解析同 §4.2。

## 5. 错误映射矩阵与重试

### 5.1 映射矩阵

远端响应 → 本地 `s3::S3Error`（`map_remote_error()` 单点实现）：

| 远端表现 | 本地行为 |
| --- | --- |
| 4xx + 可解析 XML `<Error><Code>` | 按 wire code 反查 `S3ErrorCode` 原样透传（NoSuchKey / NoSuchBucket / NoSuchUpload / InvalidRange / InvalidPart / BucketNotEmpty / BucketAlreadyOwnedByYou / EntityTooLarge…）；`errors.cc` 需补一个 wire code → enum 的反向查找 |
| 远端 403（代理凭证/权限故障） | **`InternalError`，不透传 AccessDenied**——客户端已通过本地认证，403 是网关配置故障，透传会误导客户端排查自己的凭证；日志记 warn 含远端原始码（`bucket_exists` 的 HEAD 403 例外，见 §4.3） |
| 404 且体不可解析 | 按操作上下文补 NoSuchKey / NoSuchBucket |
| 429 / 503 / SlowDown | `SlowDown`（本地 503，客户端可退避重试） |
| 500 / 502 / 504、体不可解析的 5xx | `InternalError`（本地 500）。**不引入 502**：S3 错误词表本无 BadGateway，标准 S3 客户端把 500/503 视为可重试，保持协议忠实（docs/04 §4 原"502/503"表述随本文档修订） |
| 连接拒绝 / DNS 失败 / 超时（重试耗尽后） | `InternalError`，message 含 endpoint 与底层原因（httplib `Result.error()` 枚举转文字） |

### 5.2 重试策略

- 可重试条件：网络层错误、5xx、SlowDown。指数退避 `base × 2^n + 抖动`
  （默认 base 100ms、3 次，`retry_max` / `retry_base_ms` 可配，加载期做
  范围校验；单次退避钳制在 60s 内）；
- 幂等操作全量适用：GET / HEAD / LIST / DELETE / abort / bucket 操作 /
  create_multipart / complete_multipart。注：create_multipart 严格说非幂等
  ——响应丢失后的重试会在远端留下一个空的孤儿 upload（业界通行做法，AWS SDK
  同此行为），建议远端账号配 AbortIncompleteMultipartUpload 生命周期规则；
- **complete 重试的歧义**：重试后收 `NoSuchUpload` 可能意味着前一次实际已
  成功（upload 已消失）——此时降级为 HEAD 目标对象验证，存在且 ETag 形如
  `-N` 则视为成功；
- **PUT / upload_part 不可盲重试**：body 是一次性 `BodyReader`，首字节泵出
  后无法重放——仅当失败发生在**连接建立阶段**（httplib 返回 Connection /
  SSLConnection 类错误且 Provider 从未被调用，用标志位判定）才重试。

## 6. ETag 语义与端到端校验

- 全部**透传远端 ETag**（去引号存 hex）；
- 单段 PUT / upload_part：远端 ETag = 内容 MD5。§3.2 推流时已用
  `util::HashStream(Md5)` 增量计算，响应到达后与远端 ETag 比对，不一致抛
  `InternalError`（"upload corrupted in transit"）——既是 UNSIGNED-PAYLOAD
  的完整性补偿，也直接满足 docs/08 §5.2 "云端返回 etag 与本地内容校验"的依赖；
- multipart 总 ETag `hex-N` 规则与本地实现一致（`md5(各分片 md5 拼接)-N`），
  tiered 拿云端 etag 与本地 sidecar 比对语义自洽；
- 例外：远端开 SSE-KMS / SSE-C 时 ETag 非内容 MD5——提供 `verify_etag: false`
  关闭比对（默认开启）。

## 7. 配置

```yaml
backends:
  - name: aws
    type: cloudproxy
    endpoint: "https://s3.ap-east-1.amazonaws.com"   # 或 http://127.0.0.1:19100（测试）
    region: ap-east-1
    access_key: "${CLOUD_AK}"        # 配置解析器已支持 ${ENV} 展开
    secret_key: "${CLOUD_SK}"
    bucket_prefix: "lights3-"        # 远端 bucket = 前缀 + 本地名；默认空
    force_path_style: true           # 默认 true，见下
    tls_verify: true                 # 自签名远端可关；可选 ca_cert: /path/to/ca.pem
    connect_timeout_ms: 5000
    request_timeout_ms: 60000        # httplib read/write timeout（按次 recv/send 计）
    retry_max: 3
    retry_base_ms: 100
    max_connections: 16              # ClientPool 上限 = pump 并发上限
    queue_cap: 1MiB                  # 数据面 BlockQueue 容量（背压水位，§3.1）
    verify_etag: true                # §6；远端 SSE-KMS 时关
```

全部键经 `BackendConfig::params` 自动收集（yaml 后端条目下非 name/type 的
标量键都进 params），**无需改配置解析器**。

**`force_path_style` 默认 true**：virtual-hosted 风格要求泛域名 DNS，
MinIO / 自建端点 / lights3 作远端都必须 path-style，且 AWS 至今兼容
path-style。显式关掉时 host 变为 `<remote_bucket>.<endpoint-host>`（签名侧
host 同步变化），Client 连接按 bucket 独立——ClientPool 退化为 per-bucket，
属低优先路径，**当前未实现**（配置 `force_path_style: false` 在加载期报错）。

## 8. 连接管理与构建改动

### 8.1 ClientPool

`httplib::Client` 非线程安全（单 socket 顺序复用）→ 互斥保护的**空闲栈式
连接池**：acquire 时弹出空闲实例（无则新建，总数上限 `max_connections`，
到上限则阻塞等待 + 超时），RAII guard 归还。否决 per-thread client：pump 在
私有线程、控制面在任意池线程，thread_local 会让连接数不可控。

每个 Client 创建时统一设置：`set_connection_timeout` / `set_read_timeout` /
`set_write_timeout`、`set_keep_alive(true)`、TLS 校验开关
（`enable_server_certificate_verification` / `set_ca_cert_path`）。
被取消的传输（§3.1）连接作废，归还后由 httplib 自动重连。

### 8.2 指标（未实现）

沿用现有 metrics 机制新增：远端请求计数/时延分布（按操作）、重试次数、
错误映射计数（按远端码）、ETag 校验失败计数、ClientPool 等待时长。
现有 `Metrics` 是 L2 请求维度、无后端级注册机制，接入需先扩展 metrics
框架——留待独立特性；当前以 warn 日志覆盖关键路径（403 映射、ETag 兜底）。

### 8.3 CMake

- `CPPHTTPLIB_OPENSSL_SUPPORT` 必须在 **lights3_core 目标级**定义——
  httplib_server.cc 与 cloudproxy 两个 TU 一个开一个不开是 ODR 违规；
- 链接从 `OpenSSL::Crypto` 扩展为 + `OpenSSL::SSL`；
- 新增 option `LIGHTS3_CLOUDPROXY`（默认 ON）；`registry.cc` 中 cloudproxy 的
  TODO 注释替换为真实注册（工厂从 `BackendConfig::params` 读 §7 各键）。

## 9. 与 TieredBackend 的对接验收（docs/08 P5）

作为 tiered 的 cloud 侧后端，验收清单：

1. `put_object` 携带 `user_meta`（`x-amz-meta-lights3-*` 冗余头，docs/08 §4.2）
   上传后，`head_object` / `get_object` 能原样取回；
2. put / upload_part / complete 返回的 etag 非空，单段 = 内容 MD5
   （docs/08 §5.2 步骤 ③ 的校验依赖）；
3. Range GET 三种形态正确（docs/08 §6.3 透传依赖）;
4. head 返回 size / etag / last_modified 齐全（docs/08 §6.1 条件请求依赖）；
5. `list_objects` 可用于 docs/08 §9 对账遍历；
6. 远端不可达时抛 `InternalError` / `SlowDown` 而非挂死（docs/08 §9 故障矩阵
   依赖可预期的异常）。

## 10. 测试策略

**单测：in-process 双栈自举，不 mock httplib。** 测试内起 lights3 自己的
`HttplibServer + S3Service + MemoryBackend` 当"远端"（127.0.0.1 随机端口，
配一条静态凭证），构造 CloudProxyBackend 指向它，直接跑
`tests/unit/test_storage.cc` 的 `run_backend_suite()` 一致性套件。红利：
同时覆盖自签 `sign()` 与本地 `verify()` 的互操作（互为镜像，等于免费回归）。
P1 需核实 HttpConfig 是否支持 port=0 自选端口，不支持则测试内探测空闲端口。

专项测试：错误映射（裸 httplib::Server 返回构造的错误 XML / 中途断连）、
GET 中途取消（reader 提前析构，断言远端流被中止）、Range 三形态、
ETag 校验失败路径、重试计数（可注入失败的假端点）、complete 的 200-错误体。

**e2e**：`tests/e2e/run_e2e.sh` 扩展双实例场景——起实例 B（localfs 后端）
充当"云端"，实例 A 配 cloudproxy 指向 B，跑既有 curl --aws-sigv4 用例集；
再加 `tiered(cloud=cloudproxy→B)` 组合冒烟（P5 预演）。全程无 MinIO/docker
等外部依赖。

## 11. 实施拆分

| 阶段 | 内容 | 可独立验收 | 状态 |
| --- | --- | --- | --- |
| P1 | CMake（OPENSSL_SUPPORT + OpenSSL::SSL + option）；BlockQueue/QueueBodyReader 提取为 `http/pushpull.h`（server 驱动同步改用）；配置解析与校验；ClientPool；签名衔接管线；控制面 head/delete/bucket CRUD；错误映射骨架；`parse_iso8601`；registry 注册 | in-process 远端上控制面用例过；既有全量单测不回归 | ✅ |
| P2 | GET 数据面（pump + ResponseHandler + BlockQueue、Range、取消）；list_objects / list_buckets XML 解析 | run_backend_suite 读/列路径过；取消专项测试过 | ✅ |
| P3 | PUT / upload_part 流式（拉转拉 + UNSIGNED-PAYLOAD + MD5 校验）；multipart 全套（含 200-错误体处理） | `run_backend_suite(CloudProxyBackend)` 全绿 | ✅ |
| P4 | 重试/退避、超时细化、指标、日志；无长度 body 路径决断（NotImplemented 或 TRAILER 组帧）；`control_in_pump` 压测定默认值 | 故障注入专项测试过 | 重试/退避/超时/日志 ✅；无长度 body 已定为 NotImplemented；指标与 `control_in_pump` 未做 |
| P5 | e2e 双实例脚本；tiered 对接（§9 清单）；docs/08 P5 状态更新 | e2e 过；tiered + cloudproxy 冒烟过 | ✅（`e2e_cloudproxy` + `e2e_tiered_cloudproxy`） |
