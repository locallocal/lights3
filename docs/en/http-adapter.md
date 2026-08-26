# HTTP Protocol Library Pluggability Layer

> English translation of [../http-adapter.md](../http-adapter.md). The Chinese original is authoritative; section numbering matches.

Goal: the protocol layer (L2) is entirely unaware of any concrete HTTP library. To that end, three things are defined:

1. **A neutral request/response model** (`HttpRequest` / `HttpResponse`);
2. **A streaming body abstraction** (`BodyReader` / `BodyWriter`);
3. **A server interface and factory** (`IHttpServer` / `HttpServerFactory`).

Any HTTP library can plug in as long as it can translate its requests into the neutral model and write neutral responses back to the connection.

## 1. Neutral Request/Response Model

```cpp
// src/http/model.h —— depends only on the standard library and core/task.h
namespace lights3::http {

using HeaderMap = /* multimap with case-insensitive keys, order-preserving */;

// Streaming request body: pull model. Returns bytes read; 0 means EOF.
struct BodyReader {
    // buf is provided by the caller; implementations must not keep a reference beyond this call
    virtual Task<size_t> read(std::span<std::byte> buf) = 0;
    // total content length (when Content-Length is known), nullopt when chunked
    virtual std::optional<uint64_t> length() const = 0;
    virtual ~BodyReader() = default;
};

struct HttpRequest {
    std::string method;                 // "GET" "PUT" ...
    std::string raw_path;               // undecoded, needed by SigV4
    std::string path;                   // decoded
    std::string raw_query;              // undecoded raw query string; SigV4 canonical query uses it
    std::vector<std::pair<std::string,std::string>> query;   // decoded, order-preserving
    HeaderMap   headers;
    std::string remote_addr;
    std::unique_ptr<BodyReader> body;   // may be nullptr (no body)
};

struct HttpResponse {
    int         status = 200;
    HeaderMap   headers;
    // one of three body forms:
    std::string small_body;                       // small responses (XML errors, list results)
    std::unique_ptr<BodyReader> stream_body;      // large responses (GetObject)
    std::optional<uint64_t> content_length;       // with stream_body, must be given or go chunked
};

} // namespace
```

Design notes:

- **Undecoded raw_path / raw_query**: SigV4's canonical request has strict
  requirements on query sorting and URI encoding rules; the canonical query is
  rebuilt from the undecoded `raw_query` rather than the decoded `query`; the
  original information must be preserved — a parsed map alone is not enough.
- **The body uses a pull model (BodyReader) rather than a push model**: the
  protocol and storage layers, as consumers, `co_await read()` on demand,
  naturally forming backpressure — if storage writes slowly, no more data is
  received from the socket (for async libraries this shows as no further
  async_read being posted; for sync libraries as the thread blocking in recv).
  Same for the response direction: L1, as the consumer, pulls `stream_body`.
- **No over-engineered zero-copy abstraction**: data is uniformly passed as
  `span<byte>` blocks, block size chosen by the caller (default 64KiB). If some
  driver later supports `sendfile`, an optional `try_as_file()` interface can be
  added to `BodyReader` as a specialization without affecting existing
  implementations.
- **HTTP/1.1 chunked trailers do not enter the neutral model**: `HttpRequest`
  has no field for them; L1 reads them off with a bound and discards them during
  de-framing (`http.trailer_max_size` caps injection; over the cap the
  connection is closed). This is a deliberate trade-off: S3's streaming
  checksums travel inside the body via aws-chunked encoding (the trailer of
  `STREAMING-*-TRAILER` lives inside the aws-chunked frames and is parsed by
  L2's signature decorator), not via HTTP-layer trailers; generic HTTP trailers
  have no S3 consumer, and passing them through would only widen the attack
  surface.

## 2. Server Interface and Factory

```cpp
// src/http/server.h
namespace lights3::http {

using Handler = std::function<Task<HttpResponse>(HttpRequest)>;

struct IHttpServer {
    virtual void set_handler(Handler h) = 0;
    virtual void listen(const std::string& addr, uint16_t port) = 0;
    virtual uint16_t bound_port() const = 0;   // actual port after listen (for the port=0 case)
    virtual void run() = 0;          // blocking run
    virtual void shutdown() = 0;     // thread-safe; stop accepting, wait for in-flight requests
    virtual ~IHttpServer() = default;
};

// Each driver's factory is registered centrally in server.cc's ensure_registered();
// #ifdef LIGHTS3_DRIVER_* decides which are compiled in (same pattern as the storage registry)
struct HttpServerFactory {
    static std::unique_ptr<IHttpServer> create(const std::string& driver,
                                               const HttpConfig& cfg);
    static void register_driver(std::string name,
                                std::function<std::unique_ptr<IHttpServer>(const HttpConfig&)>);
};

} // namespace
```

- Drivers are **registered centrally** with the factory in `server.cc`'s
  `ensure_registered()` (pruned by `#ifdef LIGHTS3_DRIVER_*`, same pattern as the
  storage registry); CMake options decide which drivers get compiled into the
  binary, and the `http.driver` config selects one at runtime. Together these
  achieve "compile-time pruning + runtime switching".
- `Handler` returns `Task<HttpResponse>`; this is the sole execution contract with
  drivers: a driver is responsible for driving this coroutine to completion in
  its own execution environment
  (how: see [concurrency.md](concurrency.md)).

### 2.1 TLS and Shutdown/Backpressure Knobs (docs/archive/gaps.md §7)

- **TLS**: `http.tls_cert` + `http.tls_key` (PEM; both required to enable). Only
  httplib (`SSLServer`) and beast (`asio::ssl::stream` layered over `tcp_stream`;
  the session loop is templated so plaintext/TLS share one implementation)
  support it; builtin/seastar throw at construction when TLS is configured —
  SigV4 `UNSIGNED-PAYLOAD` integrity relies on transport encryption, so
  "configured but silently plaintext" is unacceptable. Certificate load failures
  also throw at startup.
- **Configurable boundaries** (formerly per-driver hard-coded constants; the
  defaults are the old values): `drain_limit` (4MiB, request-body drain cap
  before erroring), `trailer_max_size` (16KiB), `io_chunk_size` (64KiB streaming
  chunk), `body_queue_cap` (256KiB, httplib-only push-to-pull backpressure
  watermark), `shutdown_grace` (10s), `shutdown_force_wait` (5s).
- `http.io_threads` semantics drift per driver (beast = IO threads / httplib =
  thread-pool floor of 8 / seastar = shard count) and **builtin ignores it
  entirely** (thread-per-connection) — builtin now WARNs at startup when it is
  explicitly configured instead of silently swallowing it.

## 3. Implementation Notes per Driver

### 3.0 builtin (default driver, zero-dependency POSIX sockets)

- Positioning: the default driver and reference implementation. Hand-written
  HTTP/1.1 parser (request line/headers/chunked de-framing all in-repo), zero
  third-party dependencies; the security-side helpers (message-framing checks,
  outbound header filtering) are shared with the other three drivers via
  `drivers/common.h`.
- Model: thread-per-connection, synchronous. One detached thread with a 512KiB
  stack per connection; coroutines bridge via `sync_wait_pumping` (blocking body
  reads run back on the connection thread, never occupying the shared pool);
  concurrency is bounded by `http.max_connections`, and `http.io_threads` is
  meaningless for it (a startup WARN fires when explicitly configured, §2.1).
- IPv4/IPv6 dual stack (`::` defaults to v6only=0); the same config is
  interchangeable with the other three drivers.
- Limits: no TLS (configuring it throws at construction, §2.1); use beast for
  the performance path.

### 3.1 Boost.Beast (async driver, preferred performance path)

- Structure: N threads jointly running one `asio::io_context` (or per-thread
  io_context; phase 1 uses the former — simpler); one `asio::co_spawn` session
  coroutine per connection.
- Session flow: `async_read_header` → build `HttpRequest` (body wrapped as
  `BeastBodyReader`, whose `read()` continues reading via `async_read_some`
  internally) → `co_await handler(req)` → serialize response headers → loop
  pulling `stream_body` and writing to the socket.
- Bridging `Task<T>` with asio: `Task` is our own coroutine type; `co_await`ing
  it inside an asio coroutine needs an adapter awaiter
  (see [concurrency.md](concurrency.md) §4) that resumes back onto the current
  executor.
- `Expect: 100-continue` support: once Beast parses that header, the driver sends
  `100 Continue` only when the handler first calls `body->read()`, then receives
  the body — so authentication failures can reject outright without receiving the
  body, matching S3 behavior.

### 3.2 cpp-httplib (sync driver, thread-per-request)

- httplib brings its own thread pool; each request occupies a thread, and the
  handler may block freely.
- Adaptation: inside httplib's handler callback, `sync_wait(handler(req))` — the
  current thread blocks until the coroutine finishes. `co_await`s that switch to
  the IO thread pool inside the coroutine still hold; on completion the resume
  happens on a pool thread, and `sync_wait` waits for the final result on an
  event.
- Body adaptation: httplib's `ContentReader` is a push model; a bounded buffer
  queue (single-producer single-consumer, capacity 2~4 blocks) flips it into the
  pull-model `BodyReader`.
- Positioning: for functional verification, low-concurrency scenarios, and quick
  troubleshooting; not the performance path.
- **Known degradations** (upstream API limits; the driver conformance tests
  accept them):
  - `Expect: 100-continue` cannot be answered lazily — upstream offers only
    "reply 100 immediately / reply 417 / close with the final response";
    "suppress the automatic reply and let the handler decide" is not expressible
    in v0.20. §3.1's lazy-reply promise degrades to an immediate reply for this
    driver; framing violations are still rejected with 400 before inviting the
    upload, and the useless body after a premature 100 is handled by bounded
    draining + closing the connection.
  - Single header lines have a compile-time cap `CPPHTTPLIB_HEADER_MAX_LENGTH`
    (8KiB): when `http.max_header_size` exceeds it, over-long single lines are
    still rejected by upstream first (startup WARN); the total-header cap is
    enforced in-driver per the config.
  - Unregistered methods cannot be forwarded to the handler: they are uniformly
    rejected with 405 + S3 XML (the other three drivers forward them verbatim
    for L2 to judge; both outcomes are allowed by contract item 7).

### 3.3 Seastar (shard-per-core async driver)

- Structure: the seastar reactor can only be started once per process, so the
  driver maintains a **process-level engine singleton** (the first `listen()`
  spins up an `app_template` background thread; `atexit` finalizes); each server
  instance manages only its own listener and connections and can be created and
  destroyed repeatedly (unit tests depend on this behavior).
- One listener per shard (posix stack, SO_REUSEPORT fan-out) + accept loop;
  session coroutines use the project's own `Task<void>`, with `seastar::future`
  adapted via an awaiter.
- Cross-thread bridging: handler/stream_body may resume on a pool thread; before
  initiating the next socket operation, post back to the connection's shard via
  `seastar::alien` (the counterpart of beast's ResumeOn).
- `shutdown()` writes an eventfd (async-signal-safe); a watcher coroutine on
  shard0 orchestrates shutdown: stop accept → kill idle connections → 10s grace
  → force-close → run() returns.
- Build requires `SEASTAR_DEFAULT_ALLOCATOR`: the process has its own thread pool
  besides the reactor, so the system allocator is used throughout to sidestep the
  seastar allocator's thread-affinity constraints.
- With port=0, probe a free port first with a one-shot POSIX socket, then hand it
  to seastar: on the posix stack the listen address must be the same concrete
  port across all shards.
- Heavy dependencies (compiled Boost, fmt, c-ares, lz4, yaml-cpp, protobuf,
  ragel, xfs headers); not compiled by default. Enable with
  `-DLIGHTS3_DRIVER_SEASTAR=ON`; on machines without root the dependencies can be
  unpacked into `~/.local/opt/seastar-deps` (apt-get download + dpkg -x).

### 3.4 CivetWeb / Others (extension examples only; not implemented, not planned)

- CivetWeb is likewise a thread-pool sync model; adaptation is the same as
  httplib. Its C API's `mg_read` is itself a pull model — `BodyReader` just wraps
  it directly, even smoother than httplib.
- If an asio standalone-coroutine HTTP library or a home-grown parser is
  introduced, plug it in following the Beast pattern.

## 4. The Contract Drivers Must Honor (written into adapter-layer unit tests)

1. `HttpRequest.body`'s `read()` is called serially, single consumer; calling
   again after EOF returns 0.
2. When the handler throws or the Task carries an exception, the driver replies
   500 + S3 InternalError XML (reusing L2's errors module) and logs it; the
   connection may be closed.
3. If the client disconnects before the handler finishes: the driver makes
   `body->read()` return an error (propagated as an exception) and discards the
   result at the response-writing stage; L2/L3 clean up via RAII.
4. After `shutdown()`, `run()` must return once "in-flight requests complete or
   time out".
5. keep-alive, HTTP/1.1 chunked encoding/decoding, and the
   length/encoding/connection-management headers are the driver's internal
   responsibility; L2 is unaware of them. Outbound headers pass a shared filter
   (illegal header names / values containing CR/LF are dropped).
6. HEAD response framing headers are uniform across the four drivers: length
   known → write Content-Length (the value a GET would return); length unknown →
   write **neither** Content-Length nor Transfer-Encoding and close the
   connection — writing 0 is a lie, and writing chunked promises chunk frames
   that will never be sent.
7. The accepted/rejected request sets are identical: message framing (CL/TE
   conflict, duplicate CL, malformed chunks — request-smuggling preconditions),
   `http.max_header_size`, the connection cap, and IPv4/IPv6 dual stack carry
   the same semantics on all four drivers; httplib's two upstream residuals
   (single-line header cap, unregistered methods) are declared as known
   degradations in §3.2.

The contract is guaranteed by a **driver conformance test** suite (parameterize
all compiled drivers over the same set of cases: large-file PUT/GET, range,
100-continue, mid-transfer disconnect, concurrent shutdown) to keep behavior
consistent.
