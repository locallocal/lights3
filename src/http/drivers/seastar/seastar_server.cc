// L1: seastar driver — shard-per-core asynchronous model (docs/http-adapter.md §3.3).
//
// Key difference from the other drivers: the seastar reactor can only start
// once per process, so the engine is a process-level singleton (brought up by
// the first listen(), wound down via atexit), and each SeastarServer instance
// manages only its own listener and connections. Unit tests create/destroy
// drivers repeatedly, all reusing the same reactor.
//
// Session coroutines use the project's own Task<void> directly (same approach
// as the beast driver):
//  - seastar::future is adapted to suspend/resume via FutAwaiter, with resume
//    happening on this shard;
//  - handler/stream_body may resume on a pool thread, and must switch back to
//    this shard via ResumeOnShard before starting the next socket operation
//    (cross-thread posting goes through seastar::alien).
//
// The build requires SEASTAR_DEFAULT_ALLOCATOR (see root CMakeLists): the
// process has its own thread pools outside the reactor, so using the system
// allocator throughout sidesteps the seastar allocator's thread-affinity constraints.
#include <unistd.h>

#include <seastar/core/alien.hh>
#include <seastar/core/app-template.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/sleep.hh>
#include <seastar/core/smp.hh>
#include <seastar/core/temporary_buffer.hh>
#include <seastar/core/timer.hh>
#include <seastar/net/api.hh>
#include <seastar/net/inet_address.hh>
#include <seastar/net/socket_defs.hh>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include <atomic>
#include <condition_variable>
#include <coroutine>
#include <cstring>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <sstream>
#include <thread>
#include <vector>

#include "core/log.h"
#include "core/task.h"
#include "http/drivers/common.h"
#include "http/server.h"

namespace lights3::http {

namespace {

namespace ss = seastar;

// ---------- Process-level engine singleton ----------

class SeastarEngine {
public:
    static SeastarEngine& instance() {
        static SeastarEngine e;
        return e;
    }

    // The first call brings up the reactor (smp = io_threads); later calls reuse it, and smp can no longer change
    void ensure_started(int io_threads) {
        std::lock_guard lk(m_);
        if (started_) {
            if (io_threads != smp_)
                LOG_WARN("seastar engine already running with smp={}, ignoring io_threads={}",
                         smp_, io_threads);
            return;
        }
        smp_ = std::max(1, io_threads);
        auto ready = std::make_shared<std::promise<void>>();
        auto fut = ready->get_future();
        thread_ = std::thread([this, ready] { engine_thread(ready); });
        try {
            fut.get();  // Startup failures (missing dependency, smp above core count, etc.) are thrown to the caller
        } catch (...) {
            thread_.join();
            throw;
        }
        started_ = true;
        std::atexit([] { instance().stop(); });
    }

    ss::alien::instance& alien() { return *alien_; }
    unsigned shards() const { return shards_; }

private:
    // Reactor main coroutine (shard0): registers the alien entry point,
    // signals startup completion, then sleeps on the eventfd until stop().
    // Member coroutine + by-value parameters: the frame references no
    // short-lived lambda objects
    ss::future<> engine_main(std::shared_ptr<std::promise<void>> ready) {
        alien_ = &ss::engine().alien();
        shards_ = ss::smp::count;
        auto evfd = std::make_unique<ss::readable_eventfd>();
        stop_fd_ = evfd->get_write_fd();
        ready_signaled_.store(true);
        ready->set_value();
        co_await evfd->wait();
    }

    void engine_thread(std::shared_ptr<std::promise<void>> ready) {
        ss::app_template::config cfg;
        cfg.name = "lights3-seastar";
        cfg.auto_handle_sigint_sigterm = false;  // Signal handling belongs to main
        ss::app_template app(std::move(cfg));

        std::string smp_arg = std::to_string(smp_);
        char arg0[] = "lights3", arg1[] = "--smp";
        std::vector<char*> argv{arg0, arg1, smp_arg.data(), nullptr};

        try {
            app.run(static_cast<int>(argv.size()) - 1, argv.data(),
                    [this, ready] { return engine_main(ready); });
        } catch (...) {
            if (!ready_signaled_.exchange(true)) ready->set_exception(std::current_exception());
            return;
        }
        // app.run returned normally without ever entering the main function (e.g. bad arguments): carry the failure back to ensure_started
        if (!ready_signaled_.exchange(true))
            ready->set_exception(std::make_exception_ptr(
                std::runtime_error("seastar engine failed to start (see stderr)")));
    }

    void stop() {
        std::lock_guard lk(m_);
        if (!started_ || stopped_) return;
        stopped_ = true;
        uint64_t one = 1;
        [[maybe_unused]] ssize_t r = ::write(stop_fd_, &one, sizeof(one));
        thread_.join();
    }

    std::mutex m_;
    std::thread thread_;
    bool started_ = false;
    bool stopped_ = false;
    int smp_ = 1;
    std::atomic<bool> ready_signaled_{false};
    ss::alien::instance* alien_ = nullptr;
    unsigned shards_ = 0;
    int stop_fd_ = -1;
};

// ---------- Bridging Task<T> and seastar ----------

// co_await a seastar::future from within a lights3 coroutine; resume happens
// in this shard's reactor context (then_wrapped's continuation), and
// exceptions are rethrown as-is via get()
template <class T>
struct FutAwaiter {
    ss::future<T> fut;
    std::optional<ss::future<T>> done;

    bool await_ready() {
        if (fut.available()) {
            done.emplace(std::move(fut));
            return true;
        }
        return false;
    }
    void await_suspend(std::coroutine_handle<> h) {
        (void)std::move(fut).then_wrapped([this, h](ss::future<T> f) {
            done.emplace(std::move(f));
            h.resume();
        });
    }
    T await_resume() {
        // future<>'s get() returns an internal placeholder type rather than void; cannot return it directly
        if constexpr (std::is_void_v<T>)
            done->get();
        else
            return done->get();
    }
};

template <class T>
FutAwaiter<T> fut_await(ss::future<T> f) {
    return {std::move(f), std::nullopt};
}

// Posts the continuation back onto the given shard: handler/stream_body may
// resume on a pool thread, and we must switch back to the connection's shard
// before starting the next socket operation (counterpart of beast's ResumeOn)
struct ResumeOnShard {
    unsigned shard;

    bool await_ready() const noexcept {
        return ss::engine_is_ready() && ss::this_shard_id() == shard;
    }
    void await_suspend(std::coroutine_handle<> h) const {
        ss::alien::run_on(SeastarEngine::instance().alien(), shard,
                          [h]() noexcept { h.resume(); });
    }
    void await_resume() const noexcept {}
};

// Fire-and-forget launch of a Task<void>: same as the beast driver (the driver's entry point for session coroutines)
struct Detached {
    struct promise_type {
        Detached get_return_object() { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }  // spawn_detached already catches everything
    };
};

template <class Done>
Detached spawn_detached(Task<void> t, Done done) {
    try {
        co_await std::move(t);
    } catch (const std::exception& e) {
        LOG_ERROR("seastar session escaped exception: {}", e.what());
    } catch (...) {
        // Non-std::exception must be caught too; otherwise it hits the
        // promise's unhandled_exception, which means terminate
        LOG_ERROR("seastar session escaped non-standard exception");
    }
    done();
}

// ---------- Connections and sessions ----------

// Buffered connection reader + write outlet; shared by request-header parsing
// and body reads. All methods may only be called on the connection's shard.
struct SeaConn {
    ss::connected_socket cs;
    ss::input_stream<char> in;
    ss::output_stream<char> out;
    ss::temporary_buffer<char> buf;
    size_t pos = 0;
    // Idle timeout: every pending socket operation runs under the timer's
    // protection (slowloris defense, covering the request line, header block,
    // body reads, and response writes — not just the single request-line
    // read). The expiry callback shuts down both directions, pending
    // operations wake to EOF/exception, and the session winds down naturally
    ss::timer<>* idle = nullptr;
    std::chrono::seconds idle_timeout{0};

    struct ArmGuard {
        ss::timer<>* t;
        explicit ArmGuard(SeaConn& c) : t(c.idle) {
            if (t) t->arm(c.idle_timeout);
        }
        ~ArmGuard() {
            if (t) t->cancel();
        }
    };

    explicit SeaConn(ss::connected_socket s)
        : cs(std::move(s)), in(cs.input()), out(cs.output()) {}

    // Ensures the buffer is non-empty; returns false on EOF
    Task<bool> fill() {
        while (pos == buf.size()) {
            ArmGuard g(*this);
            buf = co_await fut_await(in.read());
            pos = 0;
            if (buf.empty()) co_return false;
        }
        co_return true;
    }

    // Reads one line (with \r\n stripped); returns false on EOF/over-limit
    Task<bool> read_line(std::string& line, size_t max_len) {
        line.clear();
        for (;;) {
            if (!co_await fill()) co_return false;
            while (pos < buf.size()) {
                char c = buf[pos++];
                if (c == '\n') {
                    if (!line.empty() && line.back() == '\r') line.pop_back();
                    co_return true;
                }
                if (line.size() >= max_len) co_return false;
                line.push_back(c);
            }
        }
    }

    Task<size_t> read_some(std::byte* dst, size_t want) {
        if (!co_await fill()) co_return 0;
        size_t n = std::min(want, buf.size() - pos);
        std::memcpy(dst, buf.get() + pos, n);
        pos += n;
        co_return n;
    }

    Task<void> write(const char* p, size_t n) {
        ArmGuard g(*this);
        co_await fut_await(out.write(p, n));
    }
    Task<void> flush() {
        ArmGuard g(*this);
        co_await fut_await(out.flush());
    }
};

// Body-read state belongs to the session coroutine frame (after the handler's
// reader is destroyed, the connection still needs to drain).
// Contract (docs/http-adapter.md §4): normal EOF returns 0; client disconnect / bad chunked propagate as exceptions.
struct BodyState {
    SeaConn* conn = nullptr;
    unsigned shard = 0;
    bool need_continue = false;  // Expect: 100-continue not yet answered; reply only on first read
    bool chunked = false;
    uint64_t remaining = 0;
    uint64_t chunk_left = 0;
    bool after_chunk_data = false;  // Just finished a chunk's data; next line must be CRLF
    bool chunk_eof = false;
    bool error = false;
    size_t trailer_max = 16 * 1024;  // Overridden by http.trailer_max_size (docs/archive/gaps.md §7)

    [[noreturn]] void fail(const char* what) {
        error = true;
        throw std::runtime_error(std::string("http body: ") + what);
    }

    Task<size_t> read_some(std::byte* dst, size_t want) {
        co_await ResumeOnShard{shard};  // The consumer may resume on a pool thread
        if (error) fail("read after connection error");
        // Deferred 100-continue: the client is told to send only once the handler decides it wants the body (docs/http-adapter.md §3.1)
        if (need_continue) {
            need_continue = false;
            try {
                co_await conn->write("HTTP/1.1 100 Continue\r\n\r\n", 25);
                co_await conn->flush();
            } catch (...) {
                fail("failed to send 100 Continue");
            }
        }
        if (!chunked) {
            if (remaining == 0) co_return 0;
            size_t n = co_await conn->read_some(dst, std::min<uint64_t>(want, remaining));
            if (n == 0) fail("client disconnected mid-body");
            remaining -= n;
            co_return n;
        }
        // chunked
        while (chunk_left == 0) {
            if (chunk_eof) co_return 0;
            std::string line;
            if (after_chunk_data) {
                // Chunk data must be followed by exactly one CRLF: any
                // "hex-looking" garbage or extra blank line must not be
                // silently swallowed as the next chunk size
                if (!co_await conn->read_line(line, 2)) fail("client disconnected mid-body");
                if (!line.empty()) fail("missing CRLF after chunk data");
                after_chunk_data = false;
            }
            if (!co_await conn->read_line(line, 1024)) fail("client disconnected mid-body");
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
                    if (!co_await conn->read_line(t, 1024))
                        fail("client disconnected in trailers");
                    if (t.empty()) break;
                    trailer_bytes += t.size();
                    if (trailer_bytes > trailer_max) fail("trailer section too large");
                }
                chunk_eof = true;
                co_return 0;
            }
            chunk_left = sz;
        }
        size_t n = co_await conn->read_some(dst, std::min<uint64_t>(want, chunk_left));
        if (n == 0) fail("client disconnected mid-body");
        chunk_left -= n;
        if (chunk_left == 0) after_chunk_data = true;
        co_return n;
    }

    bool at_eof() const { return error || (chunked ? chunk_eof : remaining == 0); }

    // Drains leftover body before the response so the connection can be
    // reused; gives up if too large or on error (the caller then closes the connection)
    Task<bool> drain(uint64_t limit) {
        // 100-continue was never sent, so the client may never send a body; do not wait blindly
        if (need_continue) co_return false;
        std::vector<std::byte> tmp(driver::kScratchBytes);
        uint64_t drained = 0;
        try {
            while (!at_eof()) {
                size_t n = co_await read_some(tmp.data(), tmp.size());
                if (n == 0) break;
                drained += n;
                if (drained > limit) co_return false;
            }
        } catch (...) {
            co_return false;
        }
        co_return !error;
    }
};

class SeastarBodyReader final : public BodyReader {
public:
    SeastarBodyReader(BodyState* st, std::optional<uint64_t> len) : st_(st), len_(len) {}
    Task<size_t> read(std::span<std::byte> buf) override {
        co_return co_await st_->read_some(buf.data(), buf.size());
    }
    std::optional<uint64_t> length() const override { return len_; }

private:
    BodyState* st_;
    std::optional<uint64_t> len_;
};

struct Session {
    SeaConn conn;
    bool in_flight = false;  // Read/written only on this shard

    explicit Session(ss::connected_socket s) : conn(std::move(s)) {}
};

// One per shard; touched only on its owning shard except during construction
struct ShardState {
    std::optional<ss::server_socket> listener;
    std::set<std::shared_ptr<Session>> sessions;
    bool stopping = false;
};

// Server core shared across threads: the contents of shards[i] are touched only by shard i
struct ServerCore {
    HttpConfig cfg;
    Handler handler;
    std::vector<std::shared_ptr<ShardState>> shards;
    std::atomic<bool> stopping{false};

    std::mutex m;
    std::condition_variable cv;
    bool stopped = false;

    void notify_stopped() {
        {
            std::lock_guard lk(m);
            stopped = true;
        }
        cv.notify_all();
    }
    void wait_stopped() {
        std::unique_lock lk(m);
        cv.wait(lk, [&] { return stopped; });
    }
    bool is_stopped() {
        std::lock_guard lk(m);
        return stopped;
    }
};

Task<bool> write_response(SeaConn& conn, HttpResponse& resp, bool head_request, bool keep,
                          size_t io_chunk,
                          unsigned shard) {
    bool no_body_status = resp.status == 204 || resp.status == 304 || resp.status < 200;
    auto head = driver::render_response_head(resp, keep, head_request);
    try {
        co_await conn.write(head.text.data(), head.text.size());
        if (head_request || no_body_status) {
            co_await conn.flush();
            co_return true;
        }
        if (!resp.stream_body) {
            co_await conn.write(resp.small_body.data(), resp.small_body.size());
            co_await conn.flush();
            co_return true;
        }
    } catch (...) {
        co_return false;  // Write failure such as peer disconnect: close the connection
    }

    // Streaming response: pulled in http.io_chunk_size chunks (docs/architecture.md request lifecycle)
    std::vector<std::byte> buf(io_chunk);
    uint64_t written = 0;
    for (;;) {
        size_t n = 0;
        try {
            n = co_await resp.stream_body->read(std::span(buf));
        } catch (const std::exception& e) {
            LOG_ERROR("stream body read failed mid-response: {}", e.what());
            co_return false;  // Response head already sent; can only disconnect (contract 3: discard the result)
        }
        co_await ResumeOnShard{shard};
        try {
            if (n == 0) {
                if (head.chunked) {
                    co_await conn.write("0\r\n\r\n", 5);
                } else if (resp.content_length && written != *resp.content_length) {
                    // A fixed-length response that wrote too little must not
                    // stay keep-alive: the client would read the next response
                    // head as the rest of this body -> responses misaligned
                    LOG_ERROR("stream body short of declared Content-Length ({} != {})",
                              written, *resp.content_length);
                    co_return false;
                }
                co_await conn.flush();
                co_return true;
            }
            if (!head.chunked && resp.content_length &&
                written + n > *resp.content_length) {
                LOG_ERROR("stream body overruns declared Content-Length ({} + {} > {})",
                          written, n, *resp.content_length);
                co_return false;
            }
            if (head.chunked) {
                char sz[32];
                int m = snprintf(sz, sizeof(sz), "%zx\r\n", n);
                co_await conn.write(sz, static_cast<size_t>(m));
            }
            co_await conn.write(reinterpret_cast<const char*>(buf.data()), n);
            if (head.chunked) co_await conn.write("\r\n", 2);
            written += n;
        } catch (...) {
            co_return false;
        }
    }
}

Task<void> session_run(std::shared_ptr<ServerCore> core, std::shared_ptr<Session> sess,
                       std::string peer, unsigned shard) {
    auto& conn = sess->conn;
    conn.cs.set_nodelay(true);
    // Idle/slow timeout: on expiry both directions are shut down, pending
    // operations wake to EOF/exception, and the session winds down naturally.
    // The timer is armed by SeaConn during every pending socket operation
    // (ArmGuard), covering the request line, header block, body reads, and
    // response writes end to end (slowloris defense)
    ss::timer<> idle_timer([sess] {
        try {
            sess->conn.cs.shutdown_input();
            sess->conn.cs.shutdown_output();
        } catch (...) {}
    });
    conn.idle = &idle_timer;
    conn.idle_timeout = std::chrono::seconds(core->cfg.idle_timeout_sec);
    bool keep = true;

    // Socket errors such as a peer RST surface from seastar futures as exceptions: catch them and take the unified stream-close wrap-up
    try {
    while (keep && !core->stopping.load(std::memory_order_relaxed)) {
        const size_t max_line = core->cfg.max_header_size;
        std::string line;
        bool got = co_await conn.read_line(line, max_line);
        if (!got || line.empty()) break;

        HttpRequest req;
        req.remote_addr = peer;
        {
            auto sp1 = line.find(' ');
            auto sp2 = line.rfind(' ');
            if (sp1 == std::string::npos || sp2 == sp1) break;
            req.method = line.substr(0, sp1);
            std::string target = line.substr(sp1 + 1, sp2 - sp1 - 1);
            std::string version = line.substr(sp2 + 1);
            if (version == "HTTP/1.0") keep = false;
            driver::parse_target(target, req);
        }

        // Headers
        bool bad = false;
        size_t header_bytes = 0;
        for (;;) {
            if (!co_await conn.read_line(line, max_line)) {
                bad = true;
                break;
            }
            if (line.empty()) break;
            header_bytes += line.size();
            if (header_bytes > core->cfg.max_header_size) {
                bad = true;
                break;
            }
            // A bare CR must not remain in the header name/value (read_line only strips the single trailing \r)
            auto colon = line.find(':');
            if (colon == std::string::npos || colon == 0 ||
                line.find('\r') != std::string::npos) {
                bad = true;
                break;
            }
            std::string k = line.substr(0, colon);
            std::string v = line.substr(colon + 1);
            v.erase(0, v.find_first_not_of(" \t"));
            auto tail = v.find_last_not_of(" \t");
            if (tail != std::string::npos) v.erase(tail + 1);
            req.headers.add(std::move(k), std::move(v));
        }
        if (bad) break;
        sess->in_flight = true;

        if (req.headers.has("Connection")) {
            // List header: "Connection: close, Upgrade" is valid; full-equality comparison would miss the close
            if (req.headers.has_token("Connection", "close")) keep = false;
            else if (req.headers.has_token("Connection", "keep-alive")) keep = true;
        }

        // Body framing: CL/TE conflict, duplicate CL, and invalid values are
        // all rejected with the connection closed (request-smuggling
        // preconditions, see drivers/common.h parse_body_framing)
        auto framing = driver::parse_body_framing(req.headers);
        if (!framing.valid) {
            auto bad = driver::bad_request_response("Invalid message framing.");
            co_await write_response(conn, bad, req.method == "HEAD", /*keep=*/false,
                                    core->cfg.io_chunk_size, shard);
            break;
        }
        BodyState bstate;
        bstate.conn = &conn;
        bstate.shard = shard;
        bstate.trailer_max = core->cfg.trailer_max_size;
        std::optional<uint64_t> content_length = framing.content_length;
        bool has_body = false;
        if (framing.chunked) {
            bstate.chunked = true;
            has_body = true;
        } else if (content_length) {
            bstate.remaining = *content_length;
            has_body = *content_length > 0;
        }
        if (has_body || content_length)
            req.body = std::make_unique<SeastarBodyReader>(&bstate, content_length);
        if (auto e = req.headers.get("Expect"); e && HeaderMap::ieq(*e, "100-continue"))
            bstate.need_continue = true;

        bool head_request = req.method == "HEAD";
        HttpResponse resp;
        try {
            resp = co_await core->handler(std::move(req));
        } catch (const std::exception& e) {
            // L2 catches all exceptions; reaching here means something failed outside L2 (contract 2)
            resp = driver::internal_error_response(e.what());
            keep = false;
        }
        co_await ResumeOnShard{shard};  // The handler may resume on a pool thread

        if (core->stopping.load(std::memory_order_relaxed)) keep = false;
        // The unconsumed body must be drained before reusing the connection.
        // If the body errored, the stream is out of sync (leftover bytes
        // would be parsed as the next request), so the connection must close;
        // if 100-continue was never sent, the client may never send a body —
        // do not wait blindly, close as well
        if (bstate.error) keep = false;
        else if (!bstate.at_eof()) {
            if (bstate.need_continue) keep = false;
            else if (keep) keep = co_await bstate.drain(core->cfg.drain_limit);
        }

        bool ok = co_await write_response(conn, resp, head_request, keep,
                                          core->cfg.io_chunk_size, shard);
        sess->in_flight = false;
        if (!ok) break;
    }
    } catch (const std::exception& e) {
        LOG_DEBUG("seastar session ended with error: {}", e.what());
    }
    idle_timer.cancel();
    conn.idle = nullptr;  // The timer is about to be destroyed with this frame; ArmGuard must not touch it again
    sess->in_flight = false;
    // output_stream must be closed explicitly (flush + release); failure (peer already gone) is ignored
    try {
        co_await fut_await(conn.out.close());
    } catch (...) {}
    try {
        co_await fut_await(conn.in.close());
    } catch (...) {}
}

// Accept loop: one per shard; abort_accept() makes accept() return with an exception and exit
ss::future<> accept_loop(std::shared_ptr<ServerCore> core, std::shared_ptr<ShardState> st,
                         unsigned shard) {
    while (!st->stopping) {
        std::optional<ss::accept_result> ar;
        bool retry = false;  // co_await cannot appear inside a catch block; record a flag, then back off
        try {
            ar.emplace(co_await st->listener->accept());
        } catch (const std::exception& e) {
            if (st->stopping) break;  // Normal exit path of abort_accept
            // Transient errors (fd exhaustion etc.): back off and continue; accepting must not stop permanently
            LOG_WARN("seastar accept failed: {}, throttling", e.what());
            retry = true;
        } catch (...) {
            break;
        }
        if (retry) {
            co_await ss::sleep(std::chrono::milliseconds(100));
            continue;
        }
        if (st->stopping) break;  // Connections landing in the race window are simply dropped (closed on destruction)
        // Concurrent-connection cap (cfg.max_connections, uniform across the
        // four drivers): apportioned per shard; over the limit new
        // connections are dropped (ar closes on destruction) — without a
        // cap, per-connection coroutine frames/buffers can exhaust memory
        size_t shard_cap = std::max<size_t>(
            1, static_cast<size_t>(core->cfg.max_connections) / ss::smp::count);
        if (st->sessions.size() >= shard_cap) {
            LOG_WARN("connection limit ({}/shard) reached, rejecting", shard_cap);
            continue;
        }
        std::ostringstream oss;
        oss << ar->remote_address.addr();
        auto sess = std::make_shared<Session>(std::move(ar->connection));
        st->sessions.insert(sess);
        spawn_detached(session_run(core, sess, oss.str(), shard),
                       [st, sess] { st->sessions.erase(sess); });
    }
    st->listener.reset();
}

ss::future<size_t> count_sessions(std::shared_ptr<ServerCore> core) {
    size_t total = 0;
    for (unsigned s = 0; s < core->shards.size(); ++s)
        total += co_await ss::smp::submit_to(
            s, [st = core->shards[s].get()] { return st->sessions.size(); });
    co_return total;
}

ss::future<> shutdown_sessions(std::shared_ptr<ServerCore> core, bool idle_only) {
    for (unsigned s = 0; s < core->shards.size(); ++s) {
        co_await ss::smp::submit_to(s, [st = core->shards[s].get(), idle_only] {
            st->stopping = true;
            if (st->listener) st->listener->abort_accept();
            for (auto& sess : st->sessions) {
                if (idle_only && sess->in_flight) continue;
                sess->conn.cs.shutdown_input();
                sess->conn.cs.shutdown_output();
            }
        });
    }
}

// Waits for all sessions to end, up to grace; returns the number of sessions left
ss::future<size_t> wait_drained(std::shared_ptr<ServerCore> core, std::chrono::seconds grace) {
    auto deadline = std::chrono::steady_clock::now() + grace;
    for (;;) {
        size_t n = co_await count_sessions(core);
        if (n == 0 || std::chrono::steady_clock::now() >= deadline) co_return n;
        co_await ss::sleep(std::chrono::milliseconds(100));
    }
}

// Graceful shutdown orchestration (contract 4): stop accepting -> cut idle
// connections -> wait for in-flight requests (10s grace) -> force disconnect
// -> wait another 5s -> make run() return no matter what
ss::future<> stop_watcher(std::shared_ptr<ServerCore> core, ss::readable_eventfd evfd) {
    co_await evfd.wait();
    co_await shutdown_sessions(core, /*idle_only=*/true);

    size_t left = co_await wait_drained(core, std::chrono::seconds(core->cfg.shutdown_grace_sec));
    if (left > 0) {
        LOG_WARN("forcing {} connection(s) closed on shutdown", left);
        co_await shutdown_sessions(core, /*idle_only=*/false);
        left = co_await wait_drained(core,
                                     std::chrono::seconds(core->cfg.shutdown_force_wait_sec));
        if (left > 0) LOG_WARN("{} connection(s) still alive after force close", left);
    }
    core->notify_stopped();
}

// Assembles the whole server on shard0: each shard creates its listener +
// accept loop, then the shutdown watcher is attached; returns the write-end
// fd of the shutdown eventfd.
// Free coroutine function + by-value parameters: the frame does not depend on
// the caller's lambda object staying alive (a coroutine lambda's captures
// dangle once the lambda object is destroyed, and the alien/smp posting
// closures do not outlive the first suspension)
ss::future<int> setup_server(std::shared_ptr<ServerCore> core, std::string addr, uint16_t p) {
    for (unsigned s = 0; s < ss::smp::count; ++s) {
        co_await ss::smp::submit_to(s, [core, &addr, p, s] {
            auto st = std::make_shared<ShardState>();
            ss::listen_options lo;
            lo.reuse_address = true;
            // inet_address accepts both v4/v6 literals: with ipv4_addr
            // hard-coded, bind: "::" in the config threw outright while
            // beast/httplib started fine (docs/archive/gaps.md §3.9)
            st->listener =
                ss::engine().listen(ss::socket_address(ss::inet_address(addr), p), lo);
            core->shards[s] = st;
            (void)accept_loop(core, st, s).handle_exception([](std::exception_ptr) {});
        });
    }
    ss::readable_eventfd evfd;
    int wfd = evfd.get_write_fd();
    (void)stop_watcher(core, std::move(evfd)).handle_exception([core](std::exception_ptr) {
        LOG_ERROR("seastar stop watcher failed unexpectedly");
        core->notify_stopped();  // run() must never hang, no matter what
    });
    co_return wfd;
}

// When port=0, resolve the actual port first with a throwaway socket: the
// posix stack's cross-shard connection dispatch is paired by the address
// passed to listen, so every shard must listen on the same concrete port number
uint16_t probe_free_port(const std::string& addr) {
    // Same as listen: the address family is decided from the literal, and v6 must be probeable too
    sockaddr_storage ss{};
    socklen_t sslen = 0;
    int family = AF_INET;
    if (auto* v6 = reinterpret_cast<sockaddr_in6*>(&ss);
        inet_pton(AF_INET6, addr.c_str(), &v6->sin6_addr) == 1) {
        family = AF_INET6;
        v6->sin6_family = AF_INET6;
        sslen = sizeof(sockaddr_in6);
    } else if (auto* v4 = reinterpret_cast<sockaddr_in*>(&ss);
               inet_pton(AF_INET, addr.c_str(), &v4->sin_addr) == 1) {
        v4->sin_family = AF_INET;
        sslen = sizeof(sockaddr_in);
    } else {
        throw std::runtime_error("bad bind address: " + addr);
    }
    int fd = ::socket(family, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket() failed");
    if (family == AF_INET6) {
        int off = 0;
        setsockopt(fd, IPPROTO_IPV6, IPV6_V6ONLY, &off, sizeof(off));
    }
    if (::bind(fd, reinterpret_cast<sockaddr*>(&ss), sslen) != 0) {
        ::close(fd);
        throw std::runtime_error("bind failed: " + std::string(strerror(errno)));
    }
    sockaddr_storage bound{};
    socklen_t blen = sizeof(bound);
    getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &blen);
    ::close(fd);
    return ntohs(bound.ss_family == AF_INET6
                     ? reinterpret_cast<sockaddr_in6*>(&bound)->sin6_port
                     : reinterpret_cast<sockaddr_in*>(&bound)->sin_port);
}

class SeastarServer final : public IHttpServer {
public:
    explicit SeastarServer(const HttpConfig& cfg) : cfg_(cfg) {
        // TLS unsupported (docs/archive/gaps.md §7): configuring it must fail on the spot; never silently run plaintext
        if (!cfg.tls_cert.empty())
            throw std::runtime_error(
                "http driver 'seastar' does not support TLS; use 'httplib' or 'beast'");
    }

    ~SeastarServer() override {
        // The engine is resident, but this instance's fibers reference core_: cannot destruct before fully stopped
        if (core_ && !core_->is_stopped()) {
            shutdown();
            core_->wait_stopped();
        }
    }

    // Calling after listen() also takes effect (core_ is updated in sync,
    // matching the other drivers' "handler read at request time" timing
    // semantics); calling concurrently with in-flight requests is still API misuse
    void set_handler(Handler h) override {
        handler_ = std::move(h);
        if (core_) core_->handler = handler_;
    }

    void listen(const std::string& addr, uint16_t port) override {
        auto& eng = SeastarEngine::instance();
        eng.ensure_started(cfg_.io_threads);

        uint16_t p = port != 0 ? port : probe_free_port(addr);
        core_ = std::make_shared<ServerCore>();
        core_->cfg = cfg_;
        core_->handler = handler_;
        core_->shards.resize(eng.shards());

        auto core = core_;
        stop_fd_ = ss::alien::submit_to(eng.alien(), 0, [core, addr, p] {
            return setup_server(core, addr, p);
        }).get();

        port_ = p;
        // When shutdown() arrives before listen(), stop_fd_ is still -1 and
        // the signal is swallowed: re-emit it here so the subsequent run()
        // can return
        if (stopping_.load()) {
            uint64_t one = 1;
            [[maybe_unused]] ssize_t r = ::write(stop_fd_, &one, sizeof(one));
        }
        LOG_INFO("seastar http server listening on {}:{} (smp={})", addr, port_, eng.shards());
    }

    uint16_t bound_port() const override { return port_; }

    void run() override {
        core_->wait_stopped();
        LOG_INFO("seastar http server stopped");
    }

    // Performs only async-signal-safe operations; callable from a signal
    // handler. The exchange guarantees the eventfd is written only once:
    // after a full stop the fd is destroyed with the watcher and must not be
    // written again
    void shutdown() override {
        if (stopping_.exchange(true)) return;
        if (core_) core_->stopping.store(true);
        if (stop_fd_ >= 0) {
            uint64_t one = 1;
            [[maybe_unused]] ssize_t r = ::write(stop_fd_, &one, sizeof(one));
        }
    }

private:
    HttpConfig cfg_;
    Handler handler_;
    std::shared_ptr<ServerCore> core_;
    uint16_t port_ = 0;
    int stop_fd_ = -1;
    std::atomic<bool> stopping_{false};
};

}  // namespace

void register_seastar_driver() {
    HttpServerFactory::register_driver("seastar", [](const HttpConfig& cfg) {
        return std::make_unique<SeastarServer>(cfg);
    });
}

}  // namespace lights3::http
