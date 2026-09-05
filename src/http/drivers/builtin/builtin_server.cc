// L1: builtin driver — zero-dependency POSIX socket HTTP/1.1, thread-per-connection synchronous model.
// Demonstrates how a synchronous driver plugs into the adapter layer (coroutines bridged via sync_wait; see docs/http-adapter.md §3.0, docs/concurrency.md §4.2).
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <pthread.h>
#include <signal.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <set>
#include <thread>
#include <vector>

#include "core/log.h"
#include "core/task.h"
#include "core/util/time.h"
#include "core/util/uri.h"
#include "http/drivers/common.h"
#include "http/server.h"
#include "http/tls.h"

namespace lights3::http {

namespace {

// Connection I/O: plaintext socket or an OpenSSL session on top of it (roadmap
// §4.1). Blocking either way — the socket's SO_RCVTIMEO/SO_SNDTIMEO stay the
// timeout mechanism, and OpenSSL reports a timed-out read/write as an error
struct Io {
    int fd = -1;
    SSL* ssl = nullptr;

    // Phase timeouts (roadmap §4.2): the blocking model expresses them as socket
    // timeouts re-armed at each phase boundary; a read/write that times out fails
    // with EAGAIN, which the callers attribute to the phase they were in
    void set_recv_timeout(int sec) {
        timeval tv{sec, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    }
    void set_send_timeout(int sec) {
        timeval tv{sec, 0};
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    }
    static bool timed_out() { return errno == EAGAIN || errno == EWOULDBLOCK; }

    ssize_t recv(void* buf, size_t len) {
        if (ssl) {
            int n = SSL_read(ssl, buf, static_cast<int>(std::min<size_t>(len, INT32_MAX)));
            return n > 0 ? n : -1;
        }
        return ::recv(fd, buf, len, 0);
    }
    bool send_all(const char* data, size_t len) {
        while (len > 0) {
            ssize_t n;
            if (ssl) {
                int m = SSL_write(ssl, data, static_cast<int>(std::min<size_t>(len, INT32_MAX)));
                n = m > 0 ? m : -1;
            } else {
                n = ::send(fd, data, len, MSG_NOSIGNAL);
            }
            if (n <= 0) return false;
            data += n;
            len -= static_cast<size_t>(n);
        }
        return true;
    }
};

// Buffered connection reader; shared by request-header parsing and body reads.
// buf is not zero-initialized (docs/archive/gaps.md §4): pos/end delimit the valid
// region, and memset-ing 16KiB per connection is pure waste
struct ConnReader {
    Io* io = nullptr;
    char buf[driver::kScratchBytes];
    size_t pos = 0, end = 0;

    // Reads one line (content before \n, with \r\n stripped); returns false on failure/over-limit
    bool read_line(std::string& line, size_t max_len) {
        line.clear();
        for (;;) {
            while (pos < end) {
                char c = buf[pos++];
                if (c == '\n') {
                    if (!line.empty() && line.back() == '\r') line.pop_back();
                    return true;
                }
                if (line.size() >= max_len) return false;
                line.push_back(c);
            }
            ssize_t n = io->recv(buf, sizeof(buf));
            if (n <= 0) return false;
            pos = 0;
            end = static_cast<size_t>(n);
        }
    }

    size_t read_some(std::byte* dst, size_t want) {
        if (pos < end) {
            size_t n = std::min(want, end - pos);
            memcpy(dst, buf + pos, n);
            pos += n;
            return n;
        }
        ssize_t n = io->recv(dst, want);
        return n > 0 ? static_cast<size_t>(n) : 0;
    }
};

// Body-read state belongs to the connection (after the handler's reader is
// destroyed, the connection still needs to drain leftover bytes).
// Contract (docs/http-adapter.md §4): normal EOF returns 0; client disconnect / bad chunked propagate as exceptions.
struct BodyState {
    ConnReader* conn = nullptr;
    Io* io = nullptr;
    driver::ConnCounters* counters = nullptr;
    bool need_continue = false;   // Expect: 100-continue not yet answered; reply only on first read
    bool chunked = false;
    uint64_t remaining = 0;       // Fixed-length mode: bytes remaining
    uint64_t chunk_left = 0;      // chunked mode: remaining in the current chunk
    bool after_chunk_data = false;  // Just finished a chunk's data; next line must be CRLF
    bool chunk_eof = false;
    bool error = false;
    size_t trailer_max = 16 * 1024;  // Overridden by http.trailer_max_size (docs/archive/gaps.md §7)

    [[noreturn]] void fail(const char* what) {
        error = true;
        throw std::runtime_error(std::string("http body: ") + what);
    }

    size_t read_some(std::byte* dst, size_t want) {
        if (error) fail("read after connection error");
        // Deferred 100-continue: the client is told to send only once the
        // handler decides it wants the body (docs/http-adapter.md §3.1);
        // cases like auth failure can reject outright without receiving the body
        if (need_continue) {
            need_continue = false;
            if (!io->send_all("HTTP/1.1 100 Continue\r\n\r\n", 25))
                fail("failed to send 100 Continue");
        }
        if (!chunked) {
            if (remaining == 0) return 0;
            size_t n = conn->read_some(dst, std::min<uint64_t>(want, remaining));
            if (n == 0) {
                if (Io::timed_out() && counters) driver::count_timeout(*counters, driver::Phase::Body);
                fail("client disconnected mid-body");
            }
            remaining -= n;
            return n;
        }
        // chunked
        while (chunk_left == 0) {
            if (chunk_eof) return 0;
            std::string line;
            if (after_chunk_data) {
                // Chunk data must be followed by exactly one CRLF: any
                // "hex-looking" garbage or extra blank line must not be
                // silently swallowed as the next chunk size
                if (!conn->read_line(line, 2)) fail("client disconnected mid-body");
                if (!line.empty()) fail("missing CRLF after chunk data");
                after_chunk_data = false;
            }
            if (!conn->read_line(line, 1024)) fail("client disconnected mid-body");
            uint64_t sz = 0;
            if (!driver::parse_chunk_size(line, sz)) fail("malformed chunk size");
            if (sz == 0) {
                // Final chunk: consume trailers until the blank line. The
                // total is capped to prevent unbounded trailer flooding; a
                // read failure is body truncation and must be an error, not a
                // normal EOF
                std::string t;
                size_t trailer_bytes = 0;
                for (;;) {
                    if (!conn->read_line(t, 1024)) fail("client disconnected in trailers");
                    if (t.empty()) break;
                    trailer_bytes += t.size();
                    if (trailer_bytes > trailer_max) fail("trailer section too large");
                }
                chunk_eof = true;
                return 0;
            }
            chunk_left = sz;
        }
        size_t n = conn->read_some(dst, std::min<uint64_t>(want, chunk_left));
        if (n == 0) {
            if (Io::timed_out() && counters) driver::count_timeout(*counters, driver::Phase::Body);
            fail("client disconnected mid-body");
        }
        chunk_left -= n;
        if (chunk_left == 0) after_chunk_data = true;
        return n;
    }

    bool at_eof() const {
        return error || (chunked ? chunk_eof : remaining == 0);
    }
    // Drains leftover body after the response so the connection can be
    // reused; gives up if too large or on error (the caller then closes the connection)
    bool drain(uint64_t limit) {
        // 100-continue was never sent, so the client may never send a body; do not wait blindly
        if (need_continue) return false;
        std::byte tmp[driver::kScratchBytes];
        uint64_t drained = 0;
        try {
            while (!at_eof()) {
                size_t n = read_some(tmp, sizeof(tmp));
                if (n == 0) break;
                drained += n;
                if (drained > limit) return false;
            }
        } catch (...) {
            return false;
        }
        return !error;
    }
};

class SocketBodyReader final : public BodyReader {
public:
    SocketBodyReader(BodyState* st, std::optional<uint64_t> len, PumpExecutor* conn_exec)
        : st_(st), len_(len), conn_exec_(conn_exec) {}
    Task<size_t> read(std::span<std::byte> buf) override {
        // The blocking recv switches back to the connection's own thread
        // (docs/archive/gaps.md §2.10): the handler coroutine chain runs on the
        // shared ThreadPool, and recv-ing in place would pin pool threads on
        // slow clients — 16 slow uploads could occupy every pool thread; the
        // connection thread is idling in sync_wait_pumping at this moment
        co_await resume_on(*conn_exec_);
        co_return st_->read_some(buf.data(), buf.size());
    }
    std::optional<uint64_t> length() const override { return len_; }

private:
    BodyState* st_;
    std::optional<uint64_t> len_;
    PumpExecutor* conn_exec_;
};

// Server state shared with connection threads: run() may return before
// leftover connection threads exit (force-kill wait timed out); threads hold
// this struct via shared_ptr, so it stays safe after the server object is
// destroyed (otherwise a use-after-free during destruction)
struct ConnShared {
    HttpConfig cfg;
    Handler handler;
    driver::ConnCounters counters;  // IHttpServer::stats() (roadmap §4.2)
    // TLS (roadmap §4.1): the holder supplies certificates/SNI/client CA per
    // handshake and hot-reloads them; the SSL_CTX carries the static knobs. Both
    // live here because connection threads outlive the server object
    std::shared_ptr<tls::Holder> tls;
    SSL_CTX* tls_ctx = nullptr;
    ~ConnShared() {
        if (tls_ctx) SSL_CTX_free(tls_ctx);
    }
    std::atomic<bool> stopping{false};
    std::mutex m;
    std::condition_variable cv;
    std::set<int> conns;
    // Connections in keep-alive waiting for the next request (docs/archive/gaps.md
    // §4): these can be cut immediately on shutdown, with the grace period
    // reserved for in-flight requests — previously there was no distinction
    // and idle connections made shutdown wait a pointless 10 seconds
    std::set<int> idle;
    int active = 0;
};

// Connection threads are created explicitly with a 512KiB stack (docs/archive/gaps.md
// §4): std::thread uses the default 8MiB, x max_connections(4096) = 32GiB of
// reserved virtual address space, while the measured stack peak is ~100KiB
// (coroutine frames live on the heap; the stack only carries parsing and the
// blocking IO call chain). Detached: lifetime is managed by the shared_ptr in
// the closure, same as the old std::thread(...).detach()
bool spawn_conn_thread(std::function<void()> fn) {
    constexpr size_t kConnThreadStack = 512 * 1024;
    struct Ctx {
        std::function<void()> fn;
    };
    auto* ctx = new Ctx{std::move(fn)};
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setstacksize(&attr, kConnThreadStack);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    pthread_t tid;
    int rc = pthread_create(
        &tid, &attr,
        [](void* p) -> void* {
            std::unique_ptr<Ctx> c(static_cast<Ctx*>(p));
            c->fn();
            return nullptr;
        },
        ctx);
    pthread_attr_destroy(&attr);
    if (rc != 0) {
        delete ctx;
        return false;
    }
    return true;
}

bool write_response(Io& io, HttpResponse& resp, bool head_request, bool keep_alive,
                    size_t io_chunk = driver::kIoChunkBytes,
                    driver::ConnCounters* counters = nullptr) {
    // A send that fails with EAGAIN hit write_timeout (roadmap §4.2)
    auto send = [&](const char* p, size_t n) {
        if (io.send_all(p, n)) return true;
        if (Io::timed_out() && counters) driver::count_timeout(*counters, driver::Phase::Write);
        return false;
    };
    bool no_body_status = resp.status == 204 || resp.status == 304 || resp.status < 200;
    auto head = driver::render_response_head(resp, keep_alive, head_request);
    bool chunked = head.chunked;
    if (!send(head.text.data(), head.text.size())) return false;
    if (head_request || no_body_status) return true;

    if (!resp.stream_body) return send(resp.small_body.data(), resp.small_body.size());

    // Streaming response: pulled in http.io_chunk_size chunks
    // (docs/architecture.md request lifecycle). The chunk size is a runtime
    // setting, so the buffer moved to the heap (a stack array needs a
    // compile-time size)
    std::vector<std::byte> buf(io_chunk);
    uint64_t written = 0;
    for (;;) {
        size_t n = 0;
        try {
            n = sync_wait(resp.stream_body->read(std::span(buf)));
        } catch (const std::exception& e) {
            LOG_ERROR("stream body read failed mid-response: {}", e.what());
            return false;  // Response head already sent; can only disconnect
        }
        if (n == 0) break;
        if (!chunked && resp.content_length && written + n > *resp.content_length) {
            LOG_ERROR("stream body overruns declared Content-Length ({} + {} > {})", written, n,
                      *resp.content_length);
            return false;
        }
        if (chunked) {
            char sz[32];
            int m = snprintf(sz, sizeof(sz), "%zx\r\n", n);
            if (!send(sz, static_cast<size_t>(m))) return false;
        }
        if (!send(reinterpret_cast<const char*>(buf.data()), n)) return false;
        if (chunked && !send("\r\n", 2)) return false;
        written += n;
    }
    if (chunked) return send("0\r\n\r\n", 5);
    // A fixed-length response that wrote too little must not stay keep-alive: the client would read the next response head as the rest of this body
    if (resp.content_length && written != *resp.content_length) {
        LOG_ERROR("stream body short of declared Content-Length ({} != {})", written,
                  *resp.content_length);
        return false;
    }
    return true;
}

// Handles one request; false means the connection should be closed
bool serve_one(ConnShared& sh, Io& io, ConnReader& reader, const std::string& peer,
               bool& keep_alive, int served) {
    const size_t max_line = sh.cfg.max_header_size;
    const int fd = io.fd;
    // Phase timeouts (roadmap §4.2): waiting for the request line is the keep-alive
    // idle wait on a reused connection, the header bound on a fresh one; headers
    // and body then get their own bounds, the response write its own
    io.set_recv_timeout(served == 0 ? sh.cfg.header_timeout_sec : sh.cfg.idle_timeout_sec);

    // Until the request line is read, this connection is "idle keep-alive":
    // register it in idle so the shutdown sweep cuts it directly; on the
    // first byte read it becomes in-flight (entitled to the shutdown grace)
    std::string line;
    {
        std::lock_guard lk(sh.m);
        if (sh.stopping.load()) return false;
        sh.idle.insert(fd);
    }
    bool got = reader.read_line(line, max_line);
    {
        std::lock_guard lk(sh.m);
        sh.idle.erase(fd);
    }
    if (!got) {
        if (Io::timed_out())
            driver::count_timeout(sh.counters,
                                  served == 0 ? driver::Phase::Header : driver::Phase::Idle);
        return false;
    }
    if (line.empty()) return false;
    io.set_recv_timeout(sh.cfg.header_timeout_sec);

    HttpRequest req;
    req.remote_addr = peer;
    // Malformed request line / header block / framing (roadmap §5.3): counted once,
    // whether the connection is closed silently or answered 400
    auto malformed = [&] {
        sh.counters.parse_error();
        return false;
    };
    {
        auto sp1 = line.find(' ');
        auto sp2 = line.rfind(' ');
        if (sp1 == std::string::npos || sp2 == sp1) return malformed();
        req.method = line.substr(0, sp1);
        std::string target = line.substr(sp1 + 1, sp2 - sp1 - 1);
        std::string version = line.substr(sp2 + 1);
        if (version == "HTTP/1.0") keep_alive = false;
        driver::parse_target(target, req);
    }

    // Headers
    size_t header_bytes = 0;
    for (;;) {
        if (!reader.read_line(line, max_line)) {
            if (Io::timed_out()) driver::count_timeout(sh.counters, driver::Phase::Header);
            return false;
        }
        if (line.empty()) break;
        header_bytes += line.size();
        if (header_bytes > sh.cfg.max_header_size) return malformed();
        // A bare CR must not remain in the header name/value (read_line only strips the single trailing \r)
        if (line.find('\r') != std::string::npos) return malformed();
        auto colon = line.find(':');
        if (colon == std::string::npos || colon == 0) return malformed();
        std::string k = line.substr(0, colon);
        std::string v = line.substr(colon + 1);
        v.erase(0, v.find_first_not_of(" \t"));
        auto tail = v.find_last_not_of(" \t");
        if (tail != std::string::npos) v.erase(tail + 1);
        req.headers.add(std::move(k), std::move(v));
    }

    if (req.headers.has("Connection")) {
        // List header: "Connection: close, Upgrade" is valid; full-equality comparison would miss the close
        if (req.headers.has_token("Connection", "close")) keep_alive = false;
        else if (req.headers.has_token("Connection", "keep-alive")) keep_alive = true;
    }

    // Body framing: CL/TE conflict, duplicate CL, and invalid values are all
    // rejected with the connection closed (request-smuggling preconditions,
    // see drivers/common.h parse_body_framing)
    auto framing = driver::parse_body_framing(req.headers);
    if (!framing.valid) {
        auto bad = driver::bad_request_response("Invalid message framing.");
        write_response(io, bad, req.method == "HEAD", /*keep_alive=*/false);
        return malformed();
    }
    sh.counters.request_parsed();
    io.set_recv_timeout(sh.cfg.body_timeout_sec);
    BodyState body_state;
    body_state.conn = &reader;
    body_state.io = &io;
    body_state.counters = &sh.counters;
    body_state.trailer_max = sh.cfg.trailer_max_size;
    std::optional<uint64_t> content_length = framing.content_length;
    bool has_body = false;
    if (framing.chunked) {
        body_state.chunked = true;
        has_body = true;
    } else if (content_length) {
        body_state.remaining = *content_length;
        has_body = *content_length > 0;
    }
    PumpExecutor conn_exec;
    if (has_body || content_length)
        req.body = std::make_unique<SocketBodyReader>(&body_state, content_length, &conn_exec);
    if (auto e = req.headers.get("Expect"); e && HeaderMap::ieq(*e, "100-continue"))
        body_state.need_continue = true;

    bool head_request = req.method == "HEAD";
    HttpResponse resp;
    try {
        // Pumping variant: while waiting, the connection thread runs the conn_exec queue, taking over the body's blocking reads
        resp = sync_wait_pumping(conn_exec, sh.handler(std::move(req)));
    } catch (const std::exception& e) {
        // L2 catches all exceptions; reaching here means something failed outside L2 (contract 2: 500 + InternalError XML)
        resp = driver::internal_error_response(e.what());
        keep_alive = false;
    }

    // The unconsumed body must be drained before reusing the connection. If
    // the body errored, the stream is out of sync (leftover bytes would be
    // parsed as the next request), so the connection must close; if
    // 100-continue was never sent, the client may never send a body — do not
    // wait blindly, close as well
    if (body_state.error) keep_alive = false;
    else if (!body_state.at_eof()) {
        if (body_state.need_continue) keep_alive = false;
        else if (keep_alive) keep_alive = body_state.drain(sh.cfg.drain_limit);
    }

    // Keep-alive budget (http.max_requests_per_connection): the last allowed
    // response already announces Connection: close
    if (keep_alive && driver::keepalive_budget_exhausted(served + 1,
                                                         sh.cfg.max_requests_per_connection)) {
        keep_alive = false;
        sh.counters.keepalive_closes.fetch_add(1, std::memory_order_relaxed);
    }
    if (!write_response(io, resp, head_request, keep_alive, sh.cfg.io_chunk_size, &sh.counters))
        return false;
    return keep_alive;
}

void handle_connection(ConnShared& sh, int fd, const std::string& peer) {
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    Io io;
    io.fd = fd;
    io.set_recv_timeout(sh.cfg.header_timeout_sec);  // covers the TLS handshake too
    io.set_send_timeout(sh.cfg.write_timeout_sec);
    if (sh.tls_ctx) {
        // TLS handshake on the connection thread (blocking, bounded by the socket
        // timeouts). The certificate callback installed by the holder picks the
        // SNI-matching bundle from the current snapshot
        io.ssl = SSL_new(sh.tls_ctx);
        if (!io.ssl || SSL_set_fd(io.ssl, fd) != 1 || SSL_accept(io.ssl) != 1) {
            // Plaintext client / probe / rejected client certificate: one line, no request loop
            unsigned long e = ERR_peek_last_error();
            char buf[128] = "handshake failed";
            if (e) ERR_error_string_n(e, buf, sizeof(buf));
            ERR_clear_error();
            LOG_WARN("TLS handshake failed from {}: {}", peer, buf);
            sh.counters.tls_handshake(false);
            if (io.ssl) SSL_free(io.ssl);
            return;
        }
        sh.counters.tls_handshake(true);
    }

    ConnReader reader;
    reader.io = &io;  // Field-by-field assignment: aggregate init would value-initialize the unlisted buf (memset 16KiB)
    bool keep_alive = true;
    int served = 0;
    while (keep_alive && !sh.stopping.load()) {
        if (!serve_one(sh, io, reader, peer, keep_alive, served)) break;
        ++served;
    }
    if (io.ssl) {
        SSL_shutdown(io.ssl);  // best-effort close_notify; TCP is closed right after anyway
        SSL_free(io.ssl);
        ERR_clear_error();
    }
}

class BuiltinServer final : public IHttpServer {
public:
    explicit BuiltinServer(const HttpConfig& cfg) : shared_(std::make_shared<ConnShared>()) {
        // TLS (roadmap §4.1): OpenSSL on the connection's blocking socket; the
        // shared holder supplies certificates (SNI + hot reload) and the knobs.
        // Loading failures throw right here — never "configured but silently
        // plaintext", which would void the UNSIGNED-PAYLOAD integrity argument
        if (!cfg.tls_cert.empty()) {
            try {
                shared_->tls = std::make_shared<tls::Holder>(cfg);
                shared_->tls_ctx = SSL_CTX_new(TLS_server_method());
                if (!shared_->tls_ctx) throw std::runtime_error("SSL_CTX_new failed");
                shared_->tls->configure(shared_->tls_ctx);
            } catch (const std::exception& e) {
                throw std::runtime_error(std::string("builtin driver: failed to set up TLS: ") +
                                         e.what());
            }
            // SSL_write goes through write(2) (no MSG_NOSIGNAL): a client that
            // vanished mid-response must not SIGPIPE the process. The app installs
            // this too; the drivers built on asio/httplib avoid the syscall shape
            signal(SIGPIPE, SIG_IGN);
        }
        // The thread-per-connection model has no notion of an IO thread
        // count; configuring it explicitly means the user expects an effect
        // that will not happen (docs/archive/gaps.md §7)
        if (cfg.io_threads_set)
            LOG_WARN(
                "builtin driver ignores http.io_threads={} (thread-per-connection model; "
                "concurrency is bounded by http.max_connections={})",
                cfg.io_threads, cfg.max_connections);
        shared_->cfg = cfg;
    }
    ~BuiltinServer() override {
        if (listen_fd_ >= 0) ::close(listen_fd_);
    }

    void set_handler(Handler h) override { shared_->handler = std::move(h); }

    void listen(const std::string& addr, uint16_t port) override {
        // Dual IPv4/IPv6 support: previously hard-coded AF_INET +
        // inet_pton(AF_INET), so bind: "::" in the config threw
        // "bad bind address" outright while beast/httplib started fine
        // (the same config failed to start when swapping drivers)
        sockaddr_storage ss{};
        socklen_t sslen = 0;
        int family = AF_INET;
        if (auto* v6 = reinterpret_cast<sockaddr_in6*>(&ss);
            inet_pton(AF_INET6, addr.c_str(), &v6->sin6_addr) == 1) {
            family = AF_INET6;
            v6->sin6_family = AF_INET6;
            v6->sin6_port = htons(port);
            sslen = sizeof(sockaddr_in6);
        } else if (auto* v4 = reinterpret_cast<sockaddr_in*>(&ss);
                   inet_pton(AF_INET, addr.c_str(), &v4->sin_addr) == 1) {
            v4->sin_family = AF_INET;
            v4->sin_port = htons(port);
            sslen = sizeof(sockaddr_in);
        } else {
            throw std::runtime_error("bad bind address: " + addr);
        }
        listen_fd_ = ::socket(family, SOCK_STREAM, 0);
        if (listen_fd_ < 0) throw std::runtime_error("socket() failed");
        int one = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        // "::" defaults to dual-stack (v6only=0), matching beast/httplib behavior
        if (family == AF_INET6) {
            int off = 0;
            setsockopt(listen_fd_, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));
        }
        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&ss), sslen) != 0)
            throw std::runtime_error("bind failed: " + std::string(strerror(errno)));
        if (::listen(listen_fd_, 256) != 0) throw std::runtime_error("listen failed");
        sockaddr_storage bound{};
        socklen_t blen = sizeof(bound);
        getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound), &blen);
        port_ = ntohs(bound.ss_family == AF_INET6
                          ? reinterpret_cast<sockaddr_in6*>(&bound)->sin6_port
                          : reinterpret_cast<sockaddr_in*>(&bound)->sin_port);
        if (shared_->tls) shared_->tls->start_watch(shared_->cfg.tls_reload_interval_sec);
        LOG_INFO("builtin http{} server listening on {}:{}{}", shared_->tls ? "s" : "", addr,
                 port_, shared_->tls ? std::string(" (tls: ") + shared_->tls->summary() + ")" : "");
    }

    uint16_t bound_port() const override { return port_; }
    ConnStats stats() const override { return shared_->counters.snapshot(); }
    bool reload_tls() override { return shared_->tls && shared_->tls->reload_now(); }

    void run() override {
        auto& sh = *shared_;
        // shutdown() may arrive before run() or even before listen(): check
        // once before entering the loop, otherwise the signal has already
        // been swallowed and accept blocks forever
        while (!sh.stopping.load()) {
            sockaddr_storage peer{};
            socklen_t plen = sizeof(peer);
            int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &plen);
            if (fd < 0) {
                if (sh.stopping.load()) break;
                if (errno == EINTR || errno == ECONNABORTED) continue;
                if (errno == EMFILE || errno == ENFILE) {
                    // fd exhaustion is transient (in-flight connections will release some); back off and continue rather than stop accepting
                    LOG_WARN("accept: {}, throttling", strerror(errno));
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                    continue;
                }
                LOG_ERROR("accept failed: {}", strerror(errno));
                break;
            }
            if (sh.stopping.load()) {
                ::close(fd);
                break;
            }
            char ip[INET6_ADDRSTRLEN] = {0};
            if (peer.ss_family == AF_INET6)
                inet_ntop(AF_INET6, &reinterpret_cast<sockaddr_in6*>(&peer)->sin6_addr, ip,
                          sizeof(ip));
            else
                inet_ntop(AF_INET, &reinterpret_cast<sockaddr_in*>(&peer)->sin_addr, ip,
                          sizeof(ip));
            {
                std::lock_guard lk(sh.m);
                // Hard cap on concurrent connections (cfg.max_connections,
                // uniform across the four drivers): without a cap, the
                // thread-per-connection model's one-thread-per-connection can
                // exhaust memory/thread counts
                if (sh.active >= sh.cfg.max_connections) {
                    LOG_WARN("connection limit ({}) reached, rejecting {}",
                             sh.cfg.max_connections, ip);
                    sh.counters.rejected_limit.fetch_add(1, std::memory_order_relaxed);
                    ::close(fd);
                    continue;
                }
                ++sh.active;
                sh.counters.accepted.fetch_add(1, std::memory_order_relaxed);
                sh.counters.active.store(static_cast<uint64_t>(sh.active), std::memory_order_relaxed);
                sh.conns.insert(fd);
            }
            bool spawned = spawn_conn_thread([sp = shared_, fd, peer_ip = std::string(ip)] {
                handle_connection(*sp, fd, peer_ip);
                std::lock_guard lk(sp->m);
                sp->conns.erase(fd);
                sp->idle.erase(fd);
                ::close(fd);
                if (--sp->active == 0) sp->cv.notify_all();
                sp->counters.active.store(static_cast<uint64_t>(sp->active), std::memory_order_relaxed);
            });
            if (!spawned) {
                // Thread creation failed (resource exhaustion): roll back the
                // count and reject this connection; an exception must not
                // escape run() and kill the process
                LOG_ERROR("failed to spawn connection thread");
                std::lock_guard lk(sh.m);
                sh.conns.erase(fd);
                ::close(fd);
                if (--sh.active == 0) sh.cv.notify_all();
                sh.counters.active.store(static_cast<uint64_t>(sh.active), std::memory_order_relaxed);
            }
        }
        // Graceful exit: idle keep-alive connections are cut immediately
        // (they are only waiting for the next request, docs/archive/gaps.md §4), with
        // the grace period reserved for in-flight requests; on timeout, force
        // all of them closed. Leftover threads hold the shared state via
        // shared_ptr and finish up on their own after run() returns or even
        // after the server is destroyed — no dangling references
        std::unique_lock lk(sh.m);
        for (int cfd : sh.idle) ::shutdown(cfd, SHUT_RDWR);
        if (!sh.cv.wait_for(lk, std::chrono::seconds(sh.cfg.shutdown_grace_sec),
                            [&] { return sh.active == 0; })) {
            LOG_WARN("forcing {} connection(s) closed on shutdown", sh.active);
            for (int fd : sh.conns) ::shutdown(fd, SHUT_RDWR);
            sh.cv.wait_for(lk, std::chrono::seconds(sh.cfg.shutdown_force_wait_sec),
                           [&] { return sh.active == 0; });
        }
        LOG_INFO("builtin http server stopped");
    }

    // Performs only async-signal-safe operations; callable from a signal handler
    void shutdown() override {
        shared_->stopping.store(true);
        if (listen_fd_ >= 0) ::shutdown(listen_fd_, SHUT_RDWR);
    }

private:
    std::shared_ptr<ConnShared> shared_;
    int listen_fd_ = -1;
    uint16_t port_ = 0;
};

}  // namespace

void register_builtin_driver() {
    HttpServerFactory::register_driver("builtin", [](const HttpConfig& cfg) {
        return std::make_unique<BuiltinServer>(cfg);
    });
}

}  // namespace lights3::http
