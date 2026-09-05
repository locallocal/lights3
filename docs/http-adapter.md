# HTTP 协议库插拔层

目标：协议层（L2）完全不感知具体 HTTP 库。为此定义三样东西：

1. **中立的请求/响应模型**（`HttpRequest` / `HttpResponse`）；
2. **流式 Body 抽象**（`BodyReader` / `BodyWriter`）；
3. **服务器接口与工厂**（`IHttpServer` / `HttpServerFactory`）。

任何 HTTP 库只要能把自己的请求翻译成中立模型、把中立响应写回连接，就能接入。

## 1. 中立请求/响应模型

```cpp
// src/http/model.h —— 只依赖标准库与 core/task.h
namespace lights3::http {

using HeaderMap = /* 大小写不敏感 key 的 multimap，保序 */;

// 流式请求体：拉模型。返回读到的字节数；0 表示 EOF。
struct BodyReader {
    // buf 由调用方提供，实现方不得保留其引用超出本次调用
    virtual Task<size_t> read(std::span<std::byte> buf) = 0;
    // 内容总长（Content-Length 已知时），chunked 时为 nullopt
    virtual std::optional<uint64_t> length() const = 0;
    virtual ~BodyReader() = default;
};

struct HttpRequest {
    std::string method;                 // "GET" "PUT" ...
    std::string raw_path;               // 未解码，SigV4 需要
    std::string path;                   // 已解码
    std::string raw_query;              // 未解码原始 query 串，SigV4 canonical query 用它
    std::vector<std::pair<std::string,std::string>> query;   // 已解码，保序
    HeaderMap   headers;
    std::string remote_addr;
    std::unique_ptr<BodyReader> body;   // 可能为 nullptr（无 body）
};

struct HttpResponse {
    int         status = 200;
    HeaderMap   headers;
    // 三选一的 body 形态：
    std::string small_body;                       // 小响应（XML 错误、列表结果）
    std::unique_ptr<BodyReader> stream_body;      // 大响应（GetObject）
    std::optional<uint64_t> content_length;       // stream_body 时必须给出或走 chunked
};

} // namespace
```

设计说明：

- **未解码 raw_path / raw_query**：SigV4 的 canonical request 对 query 排序和
  URI 编码规则有严格要求，canonical query 基于未解码的 `raw_query` 重建而非
  已解码的 `query`；必须保留原始信息，不能只给解析后的 map。
- **Body 用拉模型（BodyReader）而不是推模型**：协议层与存储层作为消费者按需
  `co_await read()`，天然形成反压——存储写得慢，就不会从 socket 继续收数据
  （对异步库体现为不再投递 async_read；对同步库体现为线程阻塞在 recv）。
  响应方向同理，L1 作为消费者拉取 `stream_body`。
- **不做零拷贝抽象过度设计**：统一以 `span<byte>` 块传递，块大小由调用方决定
  （默认 64KiB）。零拷贝只留一个可选出口：`BodyReader::try_as_file()` /
  `file_bytes_sent()`（默认无快路径，§2.4 ④），不影响其他实现。
- **HTTP/1.1 chunked trailer 不进中立模型**：`HttpRequest` 无承接字段，L1 在
  剥壳时有界读掉并丢弃（`http.trailer_max_size` 上限防灌注，超限断连）。这是
  刻意取舍：S3 的流式校验和走 body 内 aws-chunked 编码（`STREAMING-*-TRAILER`
  的 trailer 位于 aws-chunked 帧内，由 L2 的验签装饰器解析），不依赖 HTTP 层
  trailer；通用 HTTP trailer 无 S3 消费方，透传只会扩大攻击面。

## 2. 服务器接口与工厂

```cpp
// src/http/server.h
namespace lights3::http {

using Handler = std::function<Task<HttpResponse>(HttpRequest)>;

struct IHttpServer {
    virtual void set_handler(Handler h) = 0;
    virtual void listen(const std::string& addr, uint16_t port) = 0;
    virtual uint16_t bound_port() const = 0;   // listen 后实际端口（port=0 场景用）
    virtual void run() = 0;          // 阻塞运行
    virtual void shutdown() = 0;     // 线程安全；停 accept，等在途请求
    virtual ~IHttpServer() = default;
};

// 各 driver 的工厂在 server.cc 的 ensure_registered() 里集中注册，
// 由 #ifdef LIGHTS3_DRIVER_* 决定编译进哪些（与 storage registry 同套路）
struct HttpServerFactory {
    static std::unique_ptr<IHttpServer> create(const std::string& driver,
                                               const HttpConfig& cfg);
    static void register_driver(std::string name,
                                std::function<std::unique_ptr<IHttpServer>(const HttpConfig&)>);
};

} // namespace
```

- 驱动经 `server.cc` 的 `ensure_registered()` **集中注册**到工厂
  （`#ifdef LIGHTS3_DRIVER_*` 裁剪，与 storage registry 同套路）；CMake 选项
  决定哪些驱动编译进二进制，运行期由配置 `http.driver` 选择。二者结合实现
  "编译期裁剪 + 运行期切换"。
- `Handler` 返回 `Task<HttpResponse>`，这是对 driver 的唯一执行约定：
  driver 负责在自己的执行环境里驱动这个协程直至完成
  （方式见 [concurrency.md](concurrency.md)）。

### 2.1 TLS 与停机/背压参数（docs/archive/gaps.md §7）

- **TLS**：`http.tls_cert` + `http.tls_key`（PEM，两个都给才启用）。**四个驱动
  都支持**（roadmap §4.1，[tls.md](tls.md)）：builtin/beast/httplib 共用
  `src/http/tls.h` 的 OpenSSL 证书回调层（SNI 多证书、mTLS、最低版本、cipher、
  证书热重载都在那一处），seastar 走 `seastar::tls`（无 SNI）。证书加载失败在
  启动期抛出——SigV4 `UNSIGNED-PAYLOAD` 的完整性依赖传输层加密，"配了但静默
  跑明文"不可接受。
- **可配置边界**（曾是四驱动各写一份的硬编码，默认值即旧常量）：
  `drain_limit`（4MiB，回错前排空请求体上限）、`trailer_max_size`（16KiB）、
  `io_chunk_size`（64KiB 流式块）、`body_queue_cap`（256KiB，仅 httplib 的
  推转拉背压水位）、`shutdown_grace`（10s；也是关停时等待许可归还的排空死线，
  roadmap §4.5）、`shutdown_force_wait`（5s）、`sendfile`（true，builtin 的文件
  body 零拷贝出口，§2.4 ④）。关停失败（后端 close / 池 join）
  以退出码 `3` 上报，见 [cli.md §2.1](cli.md)。
- **独立 admin 端口**（backlog-sequence ②）：`http.admin_port`（缺省 = 不起；`0` =
  内核选端口，同 `port`）+ `http.admin_bind`（空 = 同 `bind`）。配置后再起一个
  **同驱动**的 `IHttpServer`（数据面是 seastar 时 admin 面用 builtin——seastar 引擎是
  进程单例），只服务 `/-/` 面：`/-/metrics` 与全部 `/-/admin/*` 搬到 admin 端口，
  数据面端口对它们答 `404`；反过来 admin 端口对任何数据面路径（含 `POST /` STS、
  OPTIONS 预检）也答 `404`；`/-/healthz` / `/-/readyz` 两边都留（探针不需要知道
  布局）。两个监听共用同一准入信号量（admin 请求计入 `max_inflight_requests`）、
  同一 `shutdown_grace`、同一 TLS 素材（各自一个 `tls::Holder` 读同一组文件，
  `reload` 两边一起重读）；`lights3_http_connections_*` 等 L1 计数器为两监听之和。
  `metrics_access: root` 语义不变（换端口不等于免签）。分派侧的实现是
  `HttpRequest.admin_face`（admin 监听的 handler 置位）+ `S3Service::set_admin_split`
  的门控：先于限流与验签判定，错误面是不带提示的 404 XML。热重载不覆盖
  （`requires_restart`）。`s3adm` 的 `--endpoint` 须指向 admin 端口
  （[cli.md §3.1](cli.md)），Prometheus 抓 admin 端口（[monitoring.md](monitoring.md)）。
- `http.io_threads` 的语义随驱动漂移，见 §2.2 的矩阵。

### 2.2 超时体系与连接治理（roadmap §4.2）

原先一个 `idle_timeout` 撑起四类语义，"空闲连接 5s 回收"与"慢客户端 body 允许
300s"无法分开。现拆为四项，四驱动各接一遍：

| 键 | 默认 | 语义 | 计入指标 |
| --- | --- | --- | --- |
| `header_timeout` | 30s | 新连接的请求行 + 头部块必须在此内到齐（slowloris 上界；TLS 握手也在其内） | `timeouts{phase="header"}` |
| `idle_timeout` | 60s | keep-alive 连接等待下一个请求的空闲上界 | `phase="idle"` |
| `body_timeout` | 60s | 单次请求体读的无进展上界（慢上传） | `phase="body"` |
| `write_timeout` | 60s | 单次响应写的无进展上界（慢下载） | `phase="write"` |
| `request_timeout` / `transfer_stall_timeout` | 300s / 300s | 不变：整请求处理上界 / 整次传输总停滞上界 | — |

各驱动映射：builtin 在阶段边界重设 `SO_RCVTIMEO`/`SO_SNDTIMEO`；beast 每个异步
操作前 `expires_after` 对应阶段；seastar 的 `ArmGuard` 按当前阶段武装定时器；
httplib 上游只有一个读超时，**头部阶段由 `body_timeout` 约束**（写与 keep-alive
一一对应）。同一 `async_read_header` 覆盖请求行与头部，因此 beast 无法把"第一个
字节前"与"头部中途"分开——新连接按 `header_timeout`、复用连接按 `idle_timeout`。

**keep-alive 请求数上限** `max_requests_per_connection`（默认 1024，0=不限）：
第 N 个响应带 `Connection: close`，之后关闭——让负载均衡器有机会重新分片长连接。
原先只有 httplib 有（硬编码 1024）。

**连接计数器** `IHttpServer::stats()` → `/-/metrics`：
`lights3_http_connections_total{result=accepted|rejected_limit}`、
`lights3_http_connections_active`、`lights3_http_keepalive_closes_total`、
`lights3_http_timeouts_total{phase}`。httplib 跑上游的 accept 循环，四组都为 0
（文档化限制）。roadmap §5.3 追加 `lights3_http_requests_total`（L1 解析成功的
请求数，÷ accepted = keep-alive 复用率）、`lights3_http_tls_handshakes_total{result=ok|failed}`
（builtin/beast 自持握手可计；httplib/seastar 的握手在上游内部，恒 0）、
`lights3_http_parse_errors_total`（请求行/头部块/framing 畸形，无论静默关闭还是
回 400，四驱动都计；对端在头部中途断开或超时不算）。

**`http.io_threads` 语义矩阵**（保留单键，启动日志各自打印实际含义）：

| 驱动 | 含义 | 启动日志 |
| --- | --- | --- |
| builtin | 忽略（thread-per-connection，并发 = `max_connections`） | 显式配置时 WARN |
| beast | `io_context` 线程数 | `io_threads=N -> N io_context thread(s)` |
| httplib | 请求线程池大小，下限 8 | `io_threads=N -> request thread pool of max(N,8)` |
| seastar | shard 数（进程内引擎只启一次，之后不可变） | `io_threads=N -> smp=N shard(s)` |

### 2.3 per-IP / per-AK 限流（roadmap §4.2）

全局 `runtime.max_inflight_requests` 之外的按客户端闸门（`src/s3/ratelimit.h`）：

```yaml
ratelimit:
  per_ip_rps: 0            # 每客户端 IP 的持续速率；0 = 关
  per_ip_burst: 0          # 令牌桶容量；0 = 等于 rps
  per_ip_max_inflight: 0   # 每 IP 并发上限；0 = 关
  per_ak_rps: 0            # 每 access key（验签后）
  per_ak_burst: 0
  per_ak_max_inflight: 0
  max_tracked: 10000       # 每张表保留的键数（超出按 LRU 淘汰无在途请求的键）
```

- per-IP 在 `resolve_address` 之后、验签之前判定（洪水到不了 HMAC），
  `/-/healthz|readyz|metrics` 探针豁免；per-AK 在验签之后按已认证的 AK 判定
  （伪造头不能烧别人的额度），只覆盖 S3 数据面（admin/STS 分支不计）；
- 超限回 `503 SlowDown` + `Retry-After: 1`（SDK 本就对 SlowDown 退避重试）；
  指标 `lights3_ratelimit_rejections_total{scope=ip|ak}`；
- 在途槽位在 dispatch 返回时释放（流式响应体的后续写不再计入）；
- 反向代理后的部署要么把限流放在代理，要么让 per-IP 关掉——网关看到的是
  代理地址（`X-Forwarded-For` 不被信任）。

**客户端断连独立取消源**仍是刻意取舍（roadmap §4.2 末项）：长 handler 靠
`request_timeout` 兜底，驱动只在下一次 socket 操作时发现断连。

### 2.4 数据面性能（roadmap §4.3）

流式响应路径的四项改动，全部在 L1 内、不改 `BodyReader` 的串行单消费者契约；
基线数据见 [performance-baseline.md](performance-baseline.md)。

| # | 项 | 实现 |
| --- | --- | --- |
| ① | **双缓冲预取** | `drivers/common.h` `StreamPrefetch`：持两块 chunk 缓冲，向 socket 写当前块的同时后端读下一块已在飞行（`Started<size_t>`，[concurrency.md §2.3](concurrency.md)）。下一次 `read()` 只在上一次完成后才发起，契约不破。四驱动共用：beast/seastar/builtin 走 `co_await next()`，httplib 的同步 content provider 走 `next_sync()`。中途出错在下一块浮出，驱动照旧断连（`http_driver_backend_error_mid_stream_closes_connection`）。提前放弃响应时析构会等在飞的那次读完成（缓冲是自己的），代价是一次读延迟，不会 use-after-free |
| ② | **缓冲池** | `IoBuffer`：thread_local 空闲表（每线程最多 16 块），`new std::byte[n]` 默认初始化——不再每个响应构造一个**清零**的 64KiB `std::vector`。跨线程归还无妨（只是缓存） |
| ④ | **sendfile** | `BodyReader::try_as_file()` 返回 `FileSpan{fd, offset, length}`（剩余字节恰是一段连续文件区间时），驱动内核态搬运后经 `file_bytes_sent(n)` 回报，reader 位置与计数装饰器保持一致。实现方：localfs `FdStreamReader`（含 Range）；L2 `CountingBodyReader` 转发两者（字节仍进指标与访问日志）；校验和/tee 类装饰器保持默认（无快路径）。**只有 builtin 驱动接**：定长 + 明文 + `http.sendfile: true`（默认）；TLS、chunked、非文件 body 一律走 `read()`；首次调用被 `EINVAL/ENOSYS` 拒绝也回退。beast 不接——strand 线程上的 sendfile 会因文件冷读阻塞 I/O 线程；httplib 无 socket 句柄；seastar 原生栈无 sendfile |
| ⑤ | **builtin 流式写进 pumping** | 整个 body 循环是一个协程，由 `sync_wait_pumping` 驱动（此前每 64KiB 一次裸 `sync_wait`：condvar + 两次线程跳转，1GiB = 16384 次）；每块前 `co_await resume_on(exec)` 把续体拉回连接线程发送（慢客户端不占共享池），`PumpExecutor::running_in_this_thread()` 已在本线程时内联继续 |
| ⑥ | **beast `ResumeOn` 快路径** | 连接 executor 都是 `make_strand` 出的 `strand<io_context::executor_type>`；`any_io_executor::target<Strand>()` 探到后 `running_in_this_thread()` 为真即 `await_ready`，省一次 `asio::post`。seastar 的 `ResumeOnShard` 本就有同样判断 |
| ⑦ | **per-bucket 指标去锁** | `CountingBodyReader` 每块只加全局原子计数（`add_bytes_*_total`），桶维度累计到流末或每 16MiB 才 `add_bucket_bytes` 进一次互斥锁（此前每 64KiB 一次全局锁） |
| ⑧ | HeaderMap / BlockQueue | 维持：线性扫描与双拷贝的绝对量小，未动 |
| ⑨ | **beast 请求体读粒度**（基线跑出的发现） | 会话的 `flat_buffer` 不预留容量时，beast 的 `read_size = max(512, capacity − size)` 让每次 socket 读只取 512 字节：4 MiB 请求体 = 8192 次 `recvmsg` + 同样多次 `timerfd_settime`（每次 `expires_after`）+ 7.7 万次 futex，单次 PUT 40 ms 对 builtin 6 ms。修复：`buffer.reserve(io_chunk_size)`，PUT 4 MiB 91 → 914 ops/s |

③ 异步日志已随 §5.2 完成。

## 3. 各驱动实现要点

### 3.0 builtin（默认驱动，零依赖 POSIX socket）

- 定位：默认驱动与参考实现。手写 HTTP/1.1 解析器（请求行/头/chunked 剥壳全在
  仓内），零第三方依赖；消息边界校验、出站头过滤等安全侧辅助与另三驱动共享
  `drivers/common.h` 同一套实现。
- 模型：thread-per-connection 同步模型。每连接一个 512KiB 栈的分离线程，协程
  经 `sync_wait_pumping` 桥接（body 的阻塞读切回连接线程执行，不占共享池）；
  并发上限即 `http.max_connections`，`http.io_threads` 对它无意义（显式配置时
  启动 WARN，见 §2.1）。
- IPv4/IPv6 双栈（`::` 默认 v6only=0），同一份配置与另三驱动互换。
- TLS：连接线程上以 OpenSSL 阻塞 I/O 包裹 socket（`Io` 抽象统一 recv/send），
  证书/SNI/热重载来自共享的 `tls::Holder`（[tls.md](tls.md) §3）。
- 流式响应（§2.4）：定长明文的文件 body 走 `sendfile(2)`（`http.sendfile`），
  其余走单协程的 pumping 循环 + 双缓冲预取；发送始终在连接线程。

### 3.1 Boost.Beast（异步驱动，性能路径首选）

- 结构：N 个线程共跑一个 `asio::io_context`（或 per-thread io_context，
  首期用前者，简单）；每连接一个 `asio::co_spawn` 的会话协程。
- 会话流程：`async_read_header` → 构造 `HttpRequest`（body 封装为
  `BeastBodyReader`，其 `read()` 内部 `async_read_some` 续读）→
  `co_await handler(req)` → 序列化响应头 → 循环拉 `stream_body` 写 socket。
- `Task<T>` 与 asio 的衔接：`Task` 是我们自己的协程类型，在 asio 协程里
  `co_await` 它需要一个适配 awaiter（见 [concurrency.md](concurrency.md) §4），
  resume 回到当前 executor。
- 支持 `Expect: 100-continue`：Beast 解析到该头后，由 driver 在 handler 首次
  调用 `body->read()` 时先回 `100 Continue` 再收 body——这样认证失败可以在
  不接收 body 的情况下直接拒绝，符合 S3 行为。
- 响应循环：`StreamPrefetch` 双缓冲 + `ResumeOn` 同 strand 快路径（§2.4 ①⑥）；
  不接 sendfile（strand 线程不能被冷文件读阻塞）。

### 3.2 cpp-httplib（同步驱动，thread-per-request）

- httplib 自带线程池，每请求占一个线程，handler 内可以放心阻塞。
- 适配方式：在 httplib 的 handler 回调里 `sync_wait(handler(req))`——
  当前线程阻塞直到协程完成。协程内部切换到 IO 线程池的 `co_await`
  依旧成立，完成后 resume 发生在池线程，`sync_wait` 用 event 等待最终结果。
- Body 适配：httplib 的 `ContentReader` 是推模型，用一个有界缓冲队列
  （单生产者单消费者，容量 2~4 块）翻转成拉模型的 `BodyReader`。
- 定位：功能验证、低并发场景、快速排查问题时使用；不是性能路径。
- **已知降级**（上游 API 限制，驱动一致性测试按此放行）：
  - `Expect: 100-continue` 无法延迟应答——上游只有"立即回 100 / 回 417 / 以
    最终响应关连接"三种出路，"抑制自动应答、handler 决定后再回 100"在 v0.20
    不可表达。§3.1 的延迟应答承诺对本驱动降级为立即应答；消息边界违规仍在
    邀请上传之前以 400 拒绝，误发 100 后的无效 body 由有界排空 + 关连接兜底。
  - 单行头有编译期上限 `CPPHTTPLIB_HEADER_MAX_LENGTH`（8KiB）：
    `http.max_header_size` 配置值大于它时，超长单行仍会被上游先拒（启动
    WARN）；头部总量上限则按配置在驱动内补做。
  - 未注册方法无法转发给 handler：统一以 405 + S3 XML 拒绝（另三驱动原样
    转发交 L2 判定；两种结局均为契约 7 允许的形态）。

### 3.3 Seastar（shard-per-core 异步驱动）

- 结构：seastar reactor 每进程只能启动一次，驱动内维护**进程级引擎单例**
  （首次 `listen()` 拉起 `app_template` 后台线程，`atexit` 收尾）；每个
  server 实例只管理自己的 listener 与连接，可反复建销（单测依赖此行为）。
- 每 shard 一个 listener（posix 栈 SO_REUSEPORT 分流）+ accept 循环；
  会话协程用项目自己的 `Task<void>`，`seastar::future` 经 awaiter 适配。
- 跨线程衔接：handler/stream_body 可能在池线程 resume，发起下一个 socket
  操作前经 `seastar::alien` 投递回连接所在 shard（对应 beast 的 ResumeOn）。
- `shutdown()` 写 eventfd（async-signal-safe），shard0 上的 watcher 协程
  编排停机：停 accept → 掐空闲连接 → 10s 宽限 → 强断 → run() 返回。
- 构建要求 `SEASTAR_DEFAULT_ALLOCATOR`：进程内 reactor 之外还有自有线程池，
  统一用系统分配器绕开 seastar allocator 的线程归属约束。
- port=0 时先用一次性 POSIX socket 探测空闲端口再交给 seastar：posix 栈
  的监听地址在各 shard 间必须是同一个具体端口。
- 依赖较重（编译版 Boost、fmt、c-ares、lz4、yaml-cpp、protobuf、ragel、
  xfs 头），默认不编译；`-DLIGHTS3_DRIVER_SEASTAR=ON` 启用，无 root 机器
  可把依赖解包到 `~/.local/opt/seastar-deps`（apt-get download + dpkg -x）。
- TLS：每 shard 用 `seastar::tls::credentials_builder` 建凭证并 `tls::listen`
  包裹 listener；`tls_reload_interval > 0` 时用可重载凭证（seastar 自己监视
  文件）；不支持 `tls_sni`（配置即构造期抛错），cipher 串只对 OpenSSL 后端生效
  （[tls.md](tls.md) §4）。

### 3.4 CivetWeb / 其他（仅为扩展示例，未实现，不在计划内）

- CivetWeb 同为线程池同步模型，适配方式与 httplib 相同；其 C API 的
  `mg_read` 本身是拉模型，`BodyReader` 直接包一层即可，比 httplib 更顺。
- 若引入 asio 独立协程 HTTP 库或自研解析器，按 Beast 模式接入。

## 4. driver 必须遵守的契约（写进适配层单测）

1. `HttpRequest.body` 的 `read()` 串行调用、单消费者；EOF 后再调用返回 0。
2. handler 抛异常或 Task 携带异常时，driver 回 500 + S3 InternalError XML
   （复用 L2 的 errors 模块），并记录日志；连接可以关闭。
3. handler 完成前客户端断连：driver 使 `body->read()` 返回错误
   （以 exception 形式传播），并在响应写出阶段丢弃结果；L2/L3 靠 RAII 清理。
4. `shutdown()` 后 `run()` 必须在"在途请求完成或超时"后返回。
5. keep-alive、HTTP/1.1 chunked 编解码、长度/编码/连接管理头是 driver 内部
   职责，L2 不感知；出站头统一过滤（非法头名/含 CR/LF 的值直接丢弃）。
6. HEAD 响应的框架头四驱动统一：长度已知 → 写 Content-Length（值即对应 GET
   会返回的长度）；长度未知 → Content-Length 与 Transfer-Encoding **都不写**
   并关连接——写 0 是撒谎，写 chunked 则承诺了不会发出的 chunk 帧。
7. 接受/拒绝的请求集合一致：消息边界（CL/TE 冲突、重复 CL、坏 chunk 等走私
   前置条件）、`http.max_header_size`、连接上限、IPv4/IPv6 双栈在四驱动同
   语义；httplib 的两处上游残留（单行头上限、未注册方法）以 §3.2 已知降级
   显式声明。

契约用一套**驱动一致性测试**（parametrize 所有已编译 driver 跑同一组用例：
大文件 PUT/GET、range、100-continue、中途断连、并发 shutdown）保证行为一致。
