# 02 HTTP 协议库插拔层

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
    std::vector<std::pair<std::string,std::string>> query;   // 保序，SigV4 需要
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

- **保序 query / 未解码 path**：SigV4 的 canonical request 对 query 排序和
  URI 编码规则有严格要求，必须保留原始信息，不能只给解析后的 map。
- **Body 用拉模型（BodyReader）而不是推模型**：协议层与存储层作为消费者按需
  `co_await read()`，天然形成反压——存储写得慢，就不会从 socket 继续收数据
  （对异步库体现为不再投递 async_read；对同步库体现为线程阻塞在 recv）。
  响应方向同理，L1 作为消费者拉取 `stream_body`。
- **不做零拷贝抽象过度设计**：统一以 `span<byte>` 块传递，块大小由调用方决定
  （默认 64KiB）。后续如某 driver 支持 `sendfile`，可给 `BodyReader` 增加
  `try_as_file()` 可选接口做特化，不影响现有实现。

## 2. 服务器接口与工厂

```cpp
// src/http/server.h
namespace lights3::http {

using Handler = std::function<Task<HttpResponse>(HttpRequest)>;

struct IHttpServer {
    virtual void set_handler(Handler h) = 0;
    virtual void listen(const std::string& addr, uint16_t port) = 0;
    virtual void run() = 0;          // 阻塞运行
    virtual void shutdown() = 0;     // 线程安全；停 accept，等在途请求
    virtual ~IHttpServer() = default;
};

// 各 driver 在自己的编译单元里注册：
//   REGISTER_HTTP_DRIVER("beast", [](const HttpConfig& c){ return ...; });
struct HttpServerFactory {
    static std::unique_ptr<IHttpServer> create(const std::string& driver,
                                               const HttpConfig& cfg);
    static void register_driver(std::string name,
                                std::function<std::unique_ptr<IHttpServer>(const HttpConfig&)>);
};

} // namespace
```

- 驱动通过**静态注册宏**挂到工厂；CMake 选项决定哪些驱动编译进二进制，
  运行期由配置 `http.driver` 选择。二者结合实现"编译期裁剪 + 运行期切换"。
- `Handler` 返回 `Task<HttpResponse>`，这是对 driver 的唯一执行约定：
  driver 负责在自己的执行环境里驱动这个协程直至完成（方式见 03 篇）。

## 3. 各驱动实现要点

### 3.1 Boost.Beast（默认，异步驱动）

- 结构：N 个线程共跑一个 `asio::io_context`（或 per-thread io_context，
  首期用前者，简单）；每连接一个 `asio::co_spawn` 的会话协程。
- 会话流程：`async_read_header` → 构造 `HttpRequest`（body 封装为
  `BeastBodyReader`，其 `read()` 内部 `async_read_some` 续读）→
  `co_await handler(req)` → 序列化响应头 → 循环拉 `stream_body` 写 socket。
- `Task<T>` 与 asio 的衔接：`Task` 是我们自己的协程类型，在 asio 协程里
  `co_await` 它需要一个适配 awaiter（见 03 篇 §4），resume 回到当前 executor。
- 支持 `Expect: 100-continue`：Beast 解析到该头后，由 driver 在 handler 首次
  调用 `body->read()` 时先回 `100 Continue` 再收 body——这样认证失败可以在
  不接收 body 的情况下直接拒绝，符合 S3 行为。

### 3.2 cpp-httplib（同步驱动，thread-per-request）

- httplib 自带线程池，每请求占一个线程，handler 内可以放心阻塞。
- 适配方式：在 httplib 的 handler 回调里 `sync_wait(handler(req))`——
  当前线程阻塞直到协程完成。协程内部切换到 IO 线程池的 `co_await`
  依旧成立，完成后 resume 发生在池线程，`sync_wait` 用 event 等待最终结果。
- Body 适配：httplib 的 `ContentReader` 是推模型，用一个有界缓冲队列
  （单生产者单消费者，容量 2~4 块）翻转成拉模型的 `BodyReader`。
- 定位：功能验证、低并发场景、快速排查问题时使用；不是性能路径。

### 3.3 CivetWeb / 其他

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
5. keep-alive、HTTP/1.1 chunked 编解码是 driver 内部职责，L2 不感知。

契约用一套**驱动一致性测试**（parametrize 所有已编译 driver 跑同一组用例：
大文件 PUT/GET、range、100-continue、中途断连、并发 shutdown）保证行为一致。
