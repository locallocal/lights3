# CloudProxy 后端实现详解

> 本文是 [../cloudproxy-backend.md](../cloudproxy-backend.md)（设计文档，含路线决策、
> 目标/非目标、测试策略）的实现级配套文档：以代码为准逐一说明
> `src/storage/cloudproxy/cloudproxy_backend.h/.cc` 与
> `src/storage/cloudproxy/remote_client.h/.cc` 的内部结构、请求流、错误映射、
> 重试/取消行为与指标。接口契约见 `src/storage/backend.h`
> （`backend.h:IStorageBackend`）。设计文档中已充分展开的取舍
> （§2.1 路线反转、§3.2 payload hash 决策、§10 测试）此处不再重复。

## 1. 文件与类结构

```text
src/storage/cloudproxy/
├── cloudproxy_backend.h/.cc   # CloudProxyBackend : IStorageBackend
│                              # 数据面 pump、control_io、spool、各操作请求流
└── remote_client.h/.cc        # cloudproxy 内部头（含 httplib，只许 cloudproxy/*.cc
                               # 与 lights3_core 内部 TU 包含）：
                               # Endpoint / Target / ClientPool / RemoteMetrics
                               # / RemoteContext（签名管线 + 错误映射 + 重试）
```

分层原则：`cloudproxy_backend.h` 对外不暴露任何 httplib 类型；一切 httplib
细节收敛在 `remote_client.h:RemoteContext` 及其成员里。`CloudProxyBackend`
持有 `std::shared_ptr<RemoteContext>`（`cloudproxy_backend.h:CloudProxyBackend`
的 `ctx_`），pump 线程按值捕获该 shared_ptr，因此即使 backend 先析构，
仍在飞行中的传输也能安全收尾。

## 2. RemoteContext：连接、寻址与签名

### 2.1 配置装载与端点解析

- `cloudproxy_backend.h:CloudProxyConfig` 承载全部配置项；
  `remote_client.cc:CloudProxyConfig::from_params` 从 `BackendConfig::params`
  解析，**所有数值键在加载期做范围校验**（如 `retry_max ∈ [0,16]`、
  `queue_cap ∈ [4KiB,1GiB]`），排除运行期算术意外（如退避左移溢出）；
  `bucket_prefix` 以 `prefix + "aaa"`（最短合法本地名）整体过
  `validate_bucket_name`，注定非法的前缀在加载期即报错。
- `remote_client.cc:Endpoint::parse` 只接受 `scheme://host[:port]`（不许带
  路径），产出三个字段：
  - `signed_host`：**与 httplib 实际发出的 Host 头逐字节一致**——默认端口
    （https 443 / http 80）只发 host，非默认端口发 `host:port`。这是设计文档
    §2.2 的"host 一致性陷阱"的实现落点：签名侧与发送侧共用这一个串，
    否则远端报 SignatureDoesNotMatch；
  - `base_url`：`scheme://host:port`，作为 `httplib::Client` 的构造参数；
  - `https`：决定是否启用 TLS 相关设置。

### 2.2 寻址：path-style 与 virtual-hosted

`remote_client.h:Target` 是寻址的单点抽象，由 `remote_client.cc:RemoteContext::target`
按 `force_path_style` 产出：

| 风格 | `Target::prefix` | `Target::host` |
| --- | --- | --- |
| path-style（默认） | `"/<rb>"`（bucket 已 aws_uri_encode） | `ep.signed_host` |
| virtual-hosted | 空 | `<rb>.<endpoint-host>[:port]` |

两种风格下 **TCP 连接与 TLS SNI 恒指 endpoint 本身**（`ClientPool` 不按
bucket 分化）；vhost 只改 Host 头/签名/路径。可行前提是 httplib 仅在 Host
缺席时才自设 Host，而本管线（§2.3）恒显式携带签名后的 Host。
`Target::bucket_path()` / `Target::object_path()` 生成最终请求路径。

### 2.3 签名管线：signed_headers

`remote_client.cc:RemoteContext::signed_headers` 实现设计文档 §2.2 的
"构造最小 HttpRequest 只为签名，再搬运 headers"：

1. 组一个中立 `http::HttpRequest`（method / raw_path / raw_query / Host /
   extra 业务头）；Host 为空参时取 `ep.signed_host`，vhost 请求传入
   `Target::host`；
2. `auth.sign(req, cred, payload_hash)`——复用 `s3/auth/sigv4.h:SigV4Authenticator`
   出方向签名（每 backend 一个实例，region 独立于本地验签），就地补
   `x-amz-date` / `x-amz-content-sha256` / `Authorization`。SignedHeaders
   自动收 host + 全部 `x-amz-*`，因此 `meta_headers` 产出的 `x-amz-meta-*`
   与首等元数据头无需签名侧改动即被覆盖；`Content-Type` 走 httplib 参数，
   不进 extra（不参与签名）；
3. 把 `req.headers` 逐项搬为 `httplib::Headers` 返回。

payload hash 的三档用法：控制面带体请求用精确 `util::sha256_hex(body)`
（如 create_bucket 的 LocationConstraint XML、complete 的 Part 列表）；
无体请求传空串；流式上传用 `remote_client.h:kUnsignedPayload`
（`UNSIGNED-PAYLOAD`，取舍见设计文档 §3.2，完整性由 TLS + §7 ETag 比对补偿）。

### 2.3.1 凭证链（roadmap §3.3）

`access_key`/`secret_key` **均未配置**时，RemoteContext 构造
`aws_credentials.h:CredentialProvider`，签名时按链解析：环境变量
（AWS_ACCESS_KEY_ID/SECRET_ACCESS_KEY[/SESSION_TOKEN]）→ 容器端点
（AWS_CONTAINER_CREDENTIALS_RELATIVE_URI/_FULL_URI，ECS/EKS pod identity）→
EC2 IMDSv2（token PUT → 实例角色 → 凭证 JSON）——EC2/EKS 部署的标配形态。
要点：

- **惰性解析 + 负缓存**：首次签名才探测；全链落空缓存 60s 再试（非 EC2
  主机不为每个请求付一次 169.254.169.254 的连接超时），并保留旧凭证到其
  自身过期；
- **提前续期**：带 Expiration 的会话凭证在到期前 5 分钟边际内刷新，互斥
  串行、阻塞在池/pump 线程上（IMDS 1s/2s 短超时封顶）；
- **会话 token**：`signed_headers` 在签名前置入 `x-amz-security-token`，
  自动进 SignedHeaders；
- `imds_endpoint` 配置项可指向测试桩/代理（单测
  `cloudproxy_credential_chain_imds` 即由此驱动全链）。

### 2.4 ClientPool：互斥保护的空闲队列连接池

`httplib::Client` 非线程安全，故租约独占（`remote_client.h:ClientPool`）：

- **同步 `acquire`**（pump 私有线程用）：先剪掉队头过期空闲项，再弹出最新
  空闲实例；无空闲且总数未达 `max_connections` 则新建（新建抛异常时回滚
  计数并让出槽位，池容量不会被永久蚕食）；达上限则在条件变量上等待，
  **超时（`request_timeout_ms`）抛 `SlowDown`**。等待时长无论成败都记入
  `pool_wait` 直方图；
- **异步 `acquire_async`**（协程控制面用，roadmap §3.3）：达上限时不再把
  池线程停在 cv 上——waiter 入队挂起，`release()` 直接把连接交接给队头
  waiter（跳过已超时的僵尸项）并经 backend 注入的 executor 在池线程恢复；
  TimerQueue 单发定时器兑现同一份 `request_timeout` → `SlowDown` 契约。
  超时回调只触碰 waiter 自身的共享状态（自带锁），与池的销毁无竞态；
- **空闲回收与寿命**（roadmap §3.3）：空闲项带时间戳（队头最旧），逾
  `pool_idle_timeout_ms`（默认 60s，0=不过期）者**绝不复用**——远端/NAT
  静默断掉的连接复用出去就是首请求重试尖峰；acquire 路径惰性剪 +
  TimerQueue 轻量 reaper（间隔 = max(ttl/2, 1s)，完成后重臂）在静默期关闭
  socket，`total_` 随之收缩。`pool_max_lifetime_ms`（默认 0=不限）在归还
  时按龄退休，creation 时间随 `PooledClient` 穿越租约周期；
- `ClientPool::Lease`：RAII 归还，归还优先交接 waiter，否则入空闲队列并
  `notify_one`；
- `remote_client.cc:ClientPool::make_client` 统一设置：连接/读/写超时、
  `set_keep_alive(true)`、`set_tcp_nodelay(true)`；https 端点上按
  `tls_verify` 开关证书校验（`enable_server_certificate_verification`），
  `ca_cert` 非空时 `set_ca_cert_path` 指定私有 CA——自签名远端可
  `tls_verify: false` 或配 ca_cert。

被 `stop()` 打断的连接归还池后，由 httplib 在下次请求时自动重连
（损失一次 keep-alive，见 §8 取消）。析构先摘 reaper（`TimerQueue::cancel`
等在途回调），空闲连接随队列析构关闭。

## 3. 线程模型：control_io 与 pump

设计文档 §2.3 给出了"数据面私有 pump 线程、控制面默认池线程"的结论，
实现上分两个机制：

### 3.1 控制面：control_io

`cloudproxy_backend.cc:CloudProxyBackend::control_io` 是控制面阻塞段的统一
执行环境（fn 为纯阻塞函数、不得含 co_await，异常经 exception_ptr 原样透传）：

- 默认（`control_in_pump=false`）：`co_await pool_->schedule()` 切到共享池
  线程后同步执行 fn，单次占用 ≈ 一次远端往返 + 重试退避；
- `control_in_pump=true`：自定义 Awaiter 起**一次性私有线程**执行 fn，完成后
  经 `exec_`（`ThreadPoolExecutor`）把续体 post 回池线程——私有线程只负责
  post，业务逻辑不在其上继续。实现细节两处值得注意：
  - `binary_semaphore gate`：线程体先 `gate.acquire()` 等待
    `await_suspend` 完成 `th = std::thread(...)` 的 move 赋值——否则极快的
    fn 可能在赋值完成前就 post，池线程恢复协程并析构 awaiter，与本线程的
    move 赋值发生数据竞争；`gate.release()` 之后不得再触碰任何成员；
  - `await_resume` 里 `th.join()`：线程 post 后立即收尾，join 仅微秒级。

### 3.2 数据面：per-transfer pump 线程

GET 与流式上传各起一个 `std::thread`（每传输一个），并发上限由
`ClientPool` 容量自然限定（pump 必须先 `acquire()` 到连接）。pump 与
handler 协程之间用 `http/pushpull.h:BlockQueue`（按字节限容的
单生产者/单消费者有界缓冲）交换数据：`push` 返回 false = 消费方已取消；
`pop` 返回 0 = EOF；`close(ok=false)` 后的 pop 抛异常（"对端中途失败"）；
`cancel()` 同时唤醒两侧。容量（`queue_cap`，默认 1 MiB）即背压水位：
队满 → push 阻塞 → httplib 停止 recv/send → TCP 窗口收紧 → 远端限速。

## 4. GET：推转拉（`get_object`）

`cloudproxy_backend.cc:CloudProxyBackend::get_object` 的完整流程：

1. 校验 key、映射 bucket（§6.1）、range 格式化为 `Range: bytes=...`
   进 extra 头；
2. 建 `BlockQueue` + `TransferAbort` + `std::promise<GetHead>`，起 pump 线程；
3. **pump 线程**（循环重试，每轮租一个连接）：
   `client.Get(path, headers, ResponseHandler, ContentReceiver)`
   - ResponseHandler（响应头到达即回调）：状态为 200/206 时经
     `cloudproxy_backend.cc:head_from_response` 组装 `GetHead`
     （meta + 生效 range + body_len）并 `promise.set_value` 交还等待方，
     置 `delivered=true`。**206 的 Content-Range 必须可解析为
     `bytes a-b/total`**（total 填 `meta.size`——接口约定 size 为对象全长；
     `body_len = b-a+1`），否则 set_exception 并返回 false 中止传输——
     绝不静默退化为全量语义把部分内容当完整对象返回。其余状态返回 true
     继续收错误体；
   - ContentReceiver：headers 已交付则 `queue->push(data,n)`（false 即中止
     传输）；未交付则累积错误体（上限 64 KiB）供映射用；
   - 一轮结束：已交付则 `queue->close(res 是否成功)` 收尾返回；未交付则按
     `RemoteContext::retryable_transport` / `retryable_status` 判定重试
     （计 `count_retry("get")`，先归还连接再 `backoff`），耗尽后把映射好的
     异常（`throw_transport_error` / `throw_remote_error(ErrCtx::Key)`）
     set_exception；
   - 兜底 catch：acquire 超时等意外按交付阶段选择传播路径（未交付 →
     promise；已交付 → `queue->close(false)`）。
4. **调用方协程**（已切到池线程）`future.wait_for` 等 headers，预算 =
   `request_timeout_ms × (retry_max + 1)`（覆盖最坏重试链；每轮实际 IO 由
   httplib 自身超时兜底）。超时则 `abort() + cancel() + join`，抛
   `SlowDown`——否则涓流远端可让单个 GET 永不完成，并发 GET ≈ 池大小时
   共享池被填满全局停摆；
5. 返回 `ObjectStream`，body 为 `cloudproxy_backend.cc:PumpBodyReader`：
   - 内部包 `http/pushpull.h:QueueBodyReader`，但 `read()` 先
     `co_await pool_->schedule()` 再进可能阻塞的 pop——读方协程可能在
     beast io 线程 / seastar reactor 分片上恢复，直接阻塞会卡死事件循环；
   - **析构 = 取消**：`queue->cancel()` + `abort_->abort()` + join pump
     （§8）。

Range 三形态（`a-b` / `a-` / `-n`）由 `cloudproxy_backend.cc:format_range`
原样透传；远端忽略 Range 回 200 时按全量处理（`range` 为空）。远端响应缺
Content-Length（如 chunked）时 `cloudproxy_backend.cc:require_content_length`
直接抛 `InternalError`，宁可报错也不静默截断为 0 长度。

## 5. 流式上传：`stream_upload`（PUT 与 upload_part 共用）

`cloudproxy_backend.cc:CloudProxyBackend::stream_upload` 是拉转拉管线，
`put_object` 与 `upload_part` 只是参数不同（`multipart_ctx` 决定无体 404 的
语义回退是 Upload 还是 Bucket，以及 op 标签 put/upload_part）：

1. `body.length()` 为空时转 §5.1 spool 路径（`spool_max_bytes=0` 则
   `NotImplemented`）；
2. 建 `BlockQueue` + `TransferAbort` + 共享 `Outcome`，起 pump 线程：
   `client.Put(full, headers, len, ContentProvider, content_type)`，签名用
   `kUnsignedPayload`。Provider 每次从 queue `pop` 至多 64 KiB 写入
   DataSink；pop 抛异常（上游中途失败）或返回 0（Content-Length 未满即
   EOF）则返回 false 中止。**连接建立阶段重试**：仅当
   `!res && Provider 从未被调用 && 未被主动 abort &&
   RemoteContext::connection_stage_error(err)`（Connection /
   ConnectionTimeout / SSLConnection）且未超 `retry_max` 才重试——首字节
   一旦泵出，一次性 `BodyReader` 无法重放，盲重试会发出损坏的请求
   （设计文档 §5.2）。pump 收尾时 `queue->cancel()` 释放可能阻塞在 push
   的生产者；
3. **生产者**（handler 协程链）循环 `co_await body.read(64KiB)`：
   - 每次 read 后 `co_await pool_->schedule()`——read 可能经对称转移把协程
     恢复到 L1 驱动线程（beast strand），而 push 会因背压阻塞，必须回到
     池线程再 push；
   - 增量 `util::HashStream(Md5)` 更新；push 返回 false 记
     `remote_gone`（远端先挂了）并跳出；
   - **读到 EOF（n==0）而非读满 length 即止**：`backend.h` 的 body 契约要求
     排干到 EOF——上层验证装饰器（x-amz-content-sha256 / aws-chunked 签名链）
     挂在整读/EOF 点上，读满即停会绕过校验；
4. 收尾：`queue->close(读净且字节数恰为 len)`；上游 read 抛异常时主动
   `abortst->abort()` 打断可能阻塞在 socket write 的 pump（不干等超时）；
   切回池线程 join pump（至多等一个远端响应周期）；
5. 结果判定顺序：pump 内部异常 → 上游 read 异常 → 传输层错误
   （`throw_transport_error`）→ 2xx：`verify_etag` 开且远端 ETag 形如
   32 位 hex（MD5 形状，`cloudproxy_backend.cc:is_md5_hex`）时与本地增量
   MD5 比对，不一致计 `etag_mismatch` 并抛 `InternalError`
   （"upload corrupted in transit"，SSE-KMS 远端 ETag 非 MD5 形状则自动
   跳过比对）→ 非 2xx：`throw_remote_error(ErrCtx::Upload 或 Bucket)`。

`put_object`（`cloudproxy_backend.cc:CloudProxyBackend::put_object`）额外把
`PutCondition` 直译为 `If-None-Match: *` / `If-Match: "<etag>"` **透传给
上游**——只有持有对象的一方能保证 check-and-commit 原子性，代理侧任何
head+put 组合都不行；不支持的上游回 4xx/501 原样映射，412 →
`PreconditionFailed`（§7）。

### 5.1 无长度上传：spool_and_upload

`cloudproxy_backend.cc:CloudProxyBackend::spool_and_upload`（docs/archive/gaps.md
§6.2 的补课，此前是一刀切 NotImplemented）：AWS 拒绝裸 chunked，故先把
body 全量落到本地临时文件取得长度，再走定长 `stream_upload`：

- 临时文件优先 `O_TMPFILE`（匿名 inode，进程崩溃自动回收）；不支持的文件
  系统（老内核/NFS）回退"创建具名文件后立即 unlink"，同样无残留；目录取
  `spool_dir`，空则 `std::filesystem::temp_directory_path()`；
- 边读边写，累计超过 `spool_max_bytes`（默认 5 GiB，对齐 AWS 单 PUT 上限）
  抛 `EntityTooLarge`；每次 read 后同样 `pool_->schedule()` 回池线程写盘；
- 写完把 fd 交给 `storage/localfs/fs_util.h:FdStreamReader` 作为可重放的
  定长 body 递归进 `stream_upload`。代价 = 一次本地盘写读 + 全量到齐的
  首字节延迟，换取罕见路径可用性。

## 6. 各操作请求流

### 6.1 bucket 名映射：remote_bucket

`cloudproxy_backend.cc:CloudProxyBackend::remote_bucket`：先
`validate_bucket_name(bucket, kAllowReserved)`（纵深防御），再做两层映射：

- 保留桶 `.sys` 音译为 `cloudproxy_backend.cc:kRemoteSysBucket`
  （`lights3-sys`）——前导 `.` 违反 S3 命名规则，前缀拼接还会产生相邻
  `-.`，真 AWS 与 lights3 远端都会拒绝；用户桶恰好叫 `lights3-sys` 则抛
  `InvalidBucketName`（否则会与远端凭证桶合流造成凭证泄漏）；
- `remote = bucket_prefix + local`，拼接后超 63 字节抛 `InvalidBucketName`。

`list_buckets` 做对偶的逆变换：只保留带前缀的远端桶、剥前缀返回，
`lights3-sys` 逆映射回 `.sys`（交给 L2 的保留桶过滤，不冒充用户桶）。

### 6.2 控制面操作一览

全部走 `control_io(with_retry(op, fn))` 模板（§3.1 + §7.2）：

| 操作 | 请求 | 特殊点 |
| --- | --- | --- |
| create_bucket | PUT `<bucket_path>` | region ≠ us-east-1 带 CreateBucketConfiguration XML（body 精确 sha256 签名） |
| delete_bucket | DELETE `<bucket_path>` | 非 2xx → `ErrCtx::Bucket` 映射（409 → BucketNotEmpty 经 wire code） |
| bucket_exists | HEAD `<bucket_path>` | 2xx→true；404→false；**403→true + warn**（AWS HeadBucket 语义：存在但无权也 403） |
| list_buckets | GET `/` | 恒发往 endpoint 本身（服务级操作与寻址风格无关）；前缀过滤 + 逆映射（§6.1） |
| head_object | HEAD `<object_path>` | 200 → `cloudproxy_backend.cc:meta_from_response`（ETag 去引号、Content-Length→size、Last-Modified、五个首等元数据头按 `backend.h:kStdMetaFields` 表回填、`x-amz-meta-*` 剥前缀小写化进 user_meta）；HEAD 无错误体，404 靠 `ErrCtx::Key` 补 NoSuchKey |
| delete_object | DELETE `<object_path>` | **2xx 与 404 都算成功**（S3 幂等删除语义） |
| list_objects | GET `?list-type=2&max-keys=&prefix=&delimiter=&start-after=` | 恒用 start-after 无状态分页；截断时 next_token 取末元素——末元素是 common prefix 组时取 `remote_client.cc:group_skip_token`（prefix 用 0xff 填充到 1024 字节键长上限：组内 key 全部 ≤ 该值被跳过，组外后继 key 必 > 该值一个不漏，且不会误跳与"末字符+1"同名的字面 key） |

### 6.3 copy_object_fast：远端服务端 COPY

`cloudproxy_backend.cc:CloudProxyBackend::copy_object_fast`（docs/archive/gaps.md
§6.2：此前同后端复制要"下到网关再传回去"，双倍跨网流量）：PUT 目标路径 +
`x-amz-copy-source: /<src_rb>/<src_key>` + **恒
`x-amz-metadata-directive: REPLACE`**（handler 已把 COPY/REPLACE 语义折叠进
meta，远端只需照抄）。经 `control_io` 执行但**不走 with_retry**（COPY 非
幂等敏感但也无法安全重放判定，单发）。响应处理与 complete 同坑：**慢 COPY
先回 200、错误在体内**——body 解析失败抛 `InternalError`；根节点为
`Error` 时经 `remote_client.cc:map_remote_code` 映射后抛出；正常取
CopyObjectResult 的 ETag 去引号返回。

### 6.4 multipart 全套

- `create_multipart`：POST `?uploads`（meta 头此时上，content_type 走
  httplib 参数）→ 解析 InitiateMultipartUploadResult 取 UploadId **原样
  透传**（网关零本地状态）；根节点名不符或 id 为空抛 `InternalError`。
  注意 create 走 `with_retry`，响应丢失后的重试可能在远端留下空孤儿
  upload——已知取舍，建议远端配 AbortIncompleteMultipartUpload 生命周期规则；
- `upload_part`：PUT `?partNumber=N&uploadId=`，直接进 §5 的
  `stream_upload(multipart_ctx=true)`，ETag 透传；
- `complete_multipart`：见 §7.3（含 200-错误体与歧义消解）；
- `abort_multipart`：DELETE `?uploadId=`，非 2xx → `ErrCtx::Upload`
  （404 → NoSuchUpload）；
- `list_parts` / `list_multipart_uploads`：单页转发（docs/archive/gaps.md §5.1：
  客户端 marker 直译远端 marker，IsTruncated 原样回传，不再全量聚页）。
  防自旋兜底：远端报截断却不给游标时，用本页末元素补游标；连末元素都没有
  则把 is_truncated 改为 false——宁可诚实报"到头"也不让客户端原地打转。

## 7. 错误映射与重试

### 7.1 映射单点：throw_remote_error

`remote_client.cc:RemoteContext::throw_remote_error` 实现设计文档 §5.1 矩阵，
先尽力解析错误体 XML 取 wire code/message，随后按序判定：

1. 记 `remote_errors_total` 计数（优先 wire code，体不可解析归
   `http_<status>`）；
2. 429 / 503 / wire=SlowDown → `SlowDown`（本地 503，客户端可退避）；
3. **403 → `InternalError` + warn 日志**，不透传 AccessDenied——客户端已过
   本地认证，403 是网关侧云凭证/权限故障，透传会误导客户端排查自己的凭证
   （`bucket_exists` 的 HEAD 403 在调用点特判为存在，不进此函数）；
4. 其余 4xx：wire code 经 `remote_client.cc:map_remote_code` 反查本地
   `S3ErrorCode`（`s3::code_from_wire` + 近义词别名：BucketAlreadyExists →
   BucketAlreadyOwnedByYou，TooManyRequests/RequestLimitExceeded → SlowDown）
   命中即透传；未命中则 412 → `PreconditionFailed`（条件写透传语义，体不可
   解析也不得掉进 500）；404 按 `remote_client.h:ErrCtx` 上下文补语义
   （Key→NoSuchKey / Bucket→NoSuchBucket / Upload→NoSuchUpload）；仍未命中
   的未知 4xx → `InvalidRequest`（本地 400）并把远端码与原文带进
   message——**不塌缩成 500**（docs/archive/gaps.md §3.9：SDK 自动重试 500，会把
   InvalidObjectState 这类确定性拒绝变成无限重试）；
5. 5xx / 其余 → `InternalError`（不引入 502：S3 词表无 BadGateway）。

传输层失败（连接拒绝/DNS/超时，重试耗尽后）走
`remote_client.cc:RemoteContext::throw_transport_error`：计数归
`transport` 桶，抛 `InternalError`，message 含 endpoint 与 httplib 错误枚举
文字。resource 一律用**客户端视角**的 `/bucket/key`
（`cloudproxy_backend.cc:resource_of`），不泄漏带前缀的远端路径。

### 7.2 重试策略：retry_io 与三条独立路径（roadmap §3.3 重构）

- 判定集合：`remote_client.h:RemoteContext::retryable_status`
  （429/500/502/503/504）与 `retryable_transport`
  （Connection/ConnectionTimeout/SSLConnection/Read/Write）；
- 退避：`remote_client.cc:RemoteContext::backoff_delay_ms` = `retry_base_ms ×
  2^n + 等值抖动`，64 位算术、指数钳制 `min(n,10)`、单次上限 60s（防溢出喂
  uniform_int_distribution 的 UB）；**429/503 带 `Retry-After` 时以远端提示
  为准**（`retry_after_hint`：整秒或 HTTP-date 两形态，钳制 [0,60s]）；
- **幂等控制面**走 `cloudproxy_backend.cc:CloudProxyBackend::retry_io` 协程
  模板（取代旧的阻塞 with_retry——其退避睡在池线程上，最坏 700ms/请求，
  远端抖动时并发退避成片吃掉池容量）：每轮 = 熔断闸（`breaker_gate`）→
  `acquire_async` 异步租约 → control_io 内一次阻塞发送（每往返各记一次 op
  时延）→ 结果喂熔断器；轮间经 `async_backoff`（`core/timer.h:async_sleep`
  + 回池）退避，**从不睡在池线程上**，租约在退避前随作用域归还。适用：
  head/delete/list/bucket CRUD/tagging/create_multipart/abort/list_parts/
  list_uploads/**copy**（服务端 COPY 是幂等 PUT，现同样重试；200-错误体
  之坑在环后照旧消解）；
- **GET** 在 pump 内自带重试环（§4 步骤 3），仅限 headers 尚未交付阶段——
  body 已开始流出后失败只能 `close(false)` 让读方见异常；pump 是每传输
  私有线程，阻塞退避不占池容量，同样吃 Retry-After 提示与熔断闸；
- **流式上传**仅连接建立阶段重试（§5 步骤 2），同样过熔断闸；熔断观测
  排除主动 abort 与"provider 已被调但无响应"（可能是我方生产者断流，
  不能算远端健康失格）。

### 7.2.1 熔断器与 per-op deadline（roadmap §3.3）

- **熔断器**（`remote_client.cc:RemoteContext::breaker_*`）：连续
  `breaker_threshold`（默认 10，0=关）次决定性失败（传输错误或 5xx；429
  节流中性不计）→ 开闸 `breaker_cooldown_ms`（默认 10s），期间请求直接
  `SlowDown` 快败（`remote_errors_total{code="breaker_open"}` 计数），
  冷却结束放**单个半开探针**，成功归零、失败续开。远端整体挂掉时每请求
  不再走满 `(retry_max+1)` 轮超时；
- **per-op deadline**（`op_deadline_ms`，默认 0=关）：一次操作整个重试环的
  总预算——某次退避会落到 deadline 之外就不再重试，直接以最后结果收场。
  **只裁剪重试环，绝不中断在飞传输**（半路掐 body = 给客户端残缺响应）；
  GET 的 headers 等待预算取 `min((retry_max+1)×request_timeout, op_deadline)`。

### 7.3 complete_multipart：200-错误体与 NoSuchUpload 歧义

`cloudproxy_backend.cc:CloudProxyBackend::complete_multipart` 是重试逻辑最厚
的一处。重试环在协程层驱动（每次 POST 是 control_io 内的一次阻塞 attempt，
退避走 TimerQueue，同样过熔断闸与 deadline；歧义消解本就在协程侧）：

1. body 用 `s3::XmlWriter` 生成（ETag 补回引号），精确 sha256 签名；
2. 每轮 POST 后先按常规 `retryable_transport` / `retryable_status` 判定；
3. **200-带错误体的重试扩展**：S3 的著名坑——耗时长的 complete 先回 200，
   错误在体内。若常规判定不重试且 status==200 且体内含 `<Error`，解析后
   Code 映射为 `InternalError` 或 `SlowDown` 的**同样重试**——它们与同名
   HTTP 状态是一回事，不重试就直接给客户端 500，客户端重试还要再经一次
   NoSuchUpload 歧义消解，白费一轮往返；
4. 耗尽/不可重试后：非 200 → `throw_remote_error(ErrCtx::Upload)`；200 →
   解析体，不可解析抛 `InternalError`，根节点 `Error` 经 `map_remote_code`
   抛出，否则取 ETag；
5. **NoSuchUpload 歧义消解**（设计文档 §5.2）：仅当**发生过重试**
   （attempt>0）后收到 NoSuchUpload——前一次可能实际已成功（upload 已
   消失）。回到协程侧：用 `cloudproxy_backend.cc:expected_total_etag`
   （复用 `storage/multipart.h:combined_etag` 的 `md5(各分片 md5 拼接)-N`
   规则；任一分片 ETag 非 MD5 形状则放弃预测）算出预期总 ETag，
   `head_object` 目标对象比对——一致视为成功返回预期 ETag，否则重抛
   原 NoSuchUpload。

## 8. 取消与生命周期

- **取消源**：客户端断连或 handler 异常导致 `ObjectStream.body` 提前析构。
  `cloudproxy_backend.cc:PumpBodyReader` 析构序列 = `queue->cancel()`（解开
  阻塞在 push 的 pump）→ `abort->abort()` → `pump.join()`；
- **为什么 cancel 不够**：pump 可能阻塞在 socket read/write（远端僵住）而
  非 push——`cloudproxy_backend.cc:TransferAbort` 持互斥保护的
  `httplib::Client*`，`abort()` 对在飞连接调 `client.stop()` 直接打断
  socket IO，否则析构方要被扣押到 read/write 超时（默认 60s）。`arm/disarm`
  与 `abort` 的竞态由"aborted 标志先行"覆盖：abort 先到、arm 后到时 arm
  当场 stop；
- 上传方向对称：上游 `body.read` 抛异常时 `queue->close(false)` +
  `abortst->abort()`（§5 步骤 4）；已 abort 的传输不进连接建立阶段重试；
- GET 的 headers 等待超时（§4 步骤 4）同样 abort + cancel + join；
- **close 语义**：`CloudProxyBackend` **不覆写** `backend.h:IStorageBackend::close`
  （默认空实现）——本后端无本地持久状态需要刷盘；在飞传输的 pump 由各自的
  `PumpBodyReader` 析构 / `stream_upload` 尾部 join 负责收束，pump 按值持有
  `RemoteContext` shared_ptr，backend 析构（`~CloudProxyBackend` 为
  default）不会悬空正在收尾的传输；`ClientPool` 随最后一个 RemoteContext
  引用释放时析构，空闲连接随之关闭。

## 9. 指标

`remote_client.h:RemoteMetrics` 经 `MetricsScope`（构造第三参，默认空
scope——测试直连构造时计数落在孤儿实例上，调用无害）单点封装；op/code
维度实例经互斥缓存 get-or-create（避免每次调用打全局注册表锁），码集有界
（wire code 词表 + `http_<status>` + `transport`）：

| 指标 | 类型/标签 | 观测点 |
| --- | --- | --- |
| `lights3_cloudproxy_remote_request_seconds` | histogram, op | `retry_io` 每次往返各一次；数据面（get/put/upload_part/complete_multipart/copy）由各自路径观测，get/put 的一整段传输记为一次观测 |
| `lights3_cloudproxy_retries_total` | counter, op | 实际退避重试处：`retry_io`、GET pump、上传连接阶段、complete 环 |
| `lights3_cloudproxy_remote_errors_total` | counter, code | `throw_remote_error`（wire code 或 `http_<status>`）与 `throw_transport_error`（`transport`）；GET headers 等待超时另计 `transport`；熔断快败计 `breaker_open` |
| `lights3_cloudproxy_etag_mismatch_total` | counter | §5 步骤 5 的在途损坏信号；构造期注册 0 值可见 |
| `lights3_cloudproxy_pool_wait_seconds` | histogram | `ClientPool::acquire` 等待时长（拿到租约与 SlowDown 超时两条路径都记）；右移 = `max_connections` 调优信号 |

op 标签取值与设计文档 §8.2 一致：控制面 create_bucket / delete_bucket /
head_bucket / list_buckets / head / delete / list / create_multipart /
complete_multipart / abort_multipart / list_parts / list_uploads / copy，
数据面 get / put / upload_part。
