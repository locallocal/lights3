// L1: Boost.Beast driver — asynchronous model (docs/http-adapter.md §3.1).
// N threads share one io_context; one session coroutine per connection (one
// strand per connection). Session coroutines use the project's own Task<void>
// directly: asio async operations are adapted to suspend/resume via awaiters,
// matching the junction-point semantics of docs/concurrency.md §4.1 (the
// handler's continuation runs back on the connection strand), just without
// converting between the asio::awaitable and Task coroutine types.
#include <sys/eventfd.h>
#include <unistd.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <atomic>
#include <coroutine>
#include <mutex>
#include <optional>
#include <set>
#include <thread>
#include <vector>

#include "core/log.h"
#include "core/task.h"
#include "core/util/time.h"
#include "http/drivers/common.h"
#include "http/tls.h"
#include "http/server.h"

namespace lights3::http {

namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace bhttp = boost::beast::http;  // Avoids clashing with lights3::http
using tcp = asio::ip::tcp;

// Adapts (error_code, size_t)-shaped asio async operations into an awaiter;
// the completion callback runs on the initiating I/O object's executor (the
// connection strand) and resumes the coroutine
template <class Init>
struct IoAwaiter {
    Init init;
    beast::error_code ec{};
    size_t n = 0;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) {
        init([this, h](beast::error_code e, size_t bytes) {
            ec = e;
            n = bytes;
            h.resume();
        });
    }
    std::pair<beast::error_code, size_t> await_resume() const { return {ec, n}; }
};

template <class Init>
IoAwaiter<std::decay_t<Init>> io_op(Init&& init) {
    return {std::forward<Init>(init)};
}

// Posts the continuation back onto the given executor: handler/stream_body
// may resume on a pool thread, and we must switch back to the connection
// strand before starting the next socket operation
struct ResumeOn {
    asio::any_io_executor ex;
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) {
        asio::post(ex, [h] { h.resume(); });
    }
    void await_resume() const noexcept {}
};

struct AcceptAwaiter {
    tcp::acceptor& acc;
    asio::io_context& ioc;
    beast::error_code ec{};
    std::optional<tcp::socket> sock;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) {
        // One strand per connection: all of this socket's completion callbacks run serialized on the strand
        acc.async_accept(asio::make_strand(ioc), [this, h](beast::error_code e, tcp::socket s) {
            ec = e;
            sock.emplace(std::move(s));
            h.resume();
        });
    }
    std::pair<beast::error_code, tcp::socket> await_resume() {
        return {ec, std::move(*sock)};
    }
};

// Fire-and-forget launch of a Task<void>: the driver's entry point for running the handler coroutine to completion in its own execution environment
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
        LOG_ERROR("beast session escaped exception: {}", e.what());
    } catch (...) {
        // Non-std::exception must be caught too; otherwise it hits the
        // promise's unhandled_exception, which means terminate
        LOG_ERROR("beast session escaped non-standard exception");
    }
    done();
}

struct Session {
    beast::tcp_stream stream;
    std::atomic<bool> in_flight{false};

    explicit Session(tcp::socket&& s) : stream(std::move(s)) {}
};

// TLS session stream: references the underlying tcp_stream (owned by
// Session) and itself lives on the session coroutine frame. Timeouts still
// apply to the underlying tcp_stream (beast::get_lowest_layer) — byte
// timeouts below the TLS record layer cover handshake and reads/writes alike
using TlsStream = asio::ssl::stream<beast::tcp_stream&>;

// Per-request body-read state; owned by the session coroutine frame (still
// needs draining after the handler's reader is destroyed).
// Stream = beast::tcp_stream (plaintext) or TlsStream: async_read/async_write
// reach the corresponding record layer via the template; all other logic is identical
template <class Stream>
struct BodyCtx {
    bhttp::request_parser<bhttp::buffer_body>* parser;
    Stream* stream;
    beast::flat_buffer* buffer;
    int idle_timeout_sec;  // body_timeout: inactivity bound on one body read (roadmap §4.2)
    driver::ConnCounters* counters = nullptr;
    bool need_100 = false;  // Expect: 100-continue not yet answered; reply only on the first body read
    bool errored = false;
};

template <class Stream>
class BeastBodyReader final : public BodyReader {
public:
    BeastBodyReader(BodyCtx<Stream>* ctx, std::optional<uint64_t> len) : ctx_(ctx), len_(len) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        co_await ResumeOn{ctx_->stream->get_executor()};
        if (ctx_->errored) throw std::runtime_error("http body: read after connection error");
        // Deferred 100-continue: the client is told to send only once the handler decides it wants the body (docs/http-adapter.md §3.1)
        if (ctx_->need_100) {
            ctx_->need_100 = false;
            bhttp::response<bhttp::empty_body> cont{bhttp::status::continue_, 11};
            auto [ec, n] = co_await io_op([&](auto cb) {
                bhttp::async_write(*ctx_->stream, cont, std::move(cb));
            });
            (void)n;
            if (ec) fail(ec, "failed to send 100 Continue");
        }
        if (ctx_->parser->is_done()) co_return 0;

        auto& body = ctx_->parser->get().body();
        body.data = buf.data();
        body.size = buf.size();
        beast::get_lowest_layer(*ctx_->stream)
            .expires_after(std::chrono::seconds(ctx_->idle_timeout_sec));
        auto [ec, n] = co_await io_op([&](auto cb) {
            bhttp::async_read(*ctx_->stream, *ctx_->buffer, *ctx_->parser, std::move(cb));
        });
        (void)n;
        beast::get_lowest_layer(*ctx_->stream).expires_never();
        if (ec == beast::error::timeout && ctx_->counters)
            driver::count_timeout(*ctx_->counters, driver::Phase::Body);
        if (ec == bhttp::error::need_buffer) ec = {};
        if (ec) fail(ec, "client disconnected mid-body");
        size_t got = buf.size() - body.size;
        body.data = nullptr;
        body.size = 0;
        co_return got;
    }

    std::optional<uint64_t> length() const override { return len_; }

private:
    [[noreturn]] void fail(const beast::error_code& ec, const char* what) {
        ctx_->errored = true;
        throw std::runtime_error(std::string("http body: ") + what + " (" + ec.message() + ")");
    }

    BodyCtx<Stream>* ctx_;
    std::optional<uint64_t> len_;
};

class BeastServer final : public IHttpServer {
public:
    explicit BeastServer(const HttpConfig& cfg) : cfg_(cfg) {
        // TLS (docs/archive/gaps.md §7): certificates are loaded at construction; a
        // bad path / bad PEM throws right here — must not be discovered only
        // at the first connection's handshake
        if (!cfg.tls_cert.empty()) {
            // Certificates/SNI/client CA come from the shared holder's snapshot at
            // handshake time (roadmap §4.1, http/tls.h); the asio context only carries
            // the static knobs the holder configures onto its SSL_CTX
            try {
                tls_holder_ = std::make_shared<tls::Holder>(cfg);
                tls_ctx_.emplace(asio::ssl::context::tls_server);
                tls_holder_->configure(tls_ctx_->native_handle());
            } catch (const std::exception& e) {
                throw std::runtime_error(std::string("beast driver: failed to set up TLS: ") +
                                         e.what());
            }
        }
    }

    ~BeastServer() override {
        if (event_fd_ >= 0 && !stop_event_) ::close(event_fd_);
    }

    void set_handler(Handler h) override { handler_ = std::move(h); }

    void listen(const std::string& addr, uint16_t port) override {
        beast::error_code ec;
        auto address = asio::ip::make_address(addr, ec);
        if (ec) throw std::runtime_error("bad bind address: " + addr);
        // Control-plane objects (acceptor/stop_event/grace timers) all hang
        // off ctl_strand_: asio I/O objects are not thread-safe, and the
        // accept loop and shutdown orchestration used to access the same
        // acceptor/timer concurrently from two threads
        acceptor_.emplace(ctl_strand_);
        tcp::endpoint ep{address, port};
        acceptor_->open(ep.protocol());
        acceptor_->set_option(asio::socket_base::reuse_address(true));
        acceptor_->bind(ep);
        acceptor_->listen(asio::socket_base::max_listen_connections);
        port_ = acceptor_->local_endpoint().port();

        // shutdown() only writes the eventfd (async-signal-safe); shutdown orchestration happens entirely on io threads
        event_fd_ = ::eventfd(0, EFD_CLOEXEC);
        if (event_fd_ < 0) throw std::runtime_error("eventfd() failed");
        stop_event_.emplace(ctl_strand_, event_fd_);
        stop_event_->async_read_some(asio::buffer(&stop_buf_, sizeof(stop_buf_)),
                                     [this](beast::error_code e, size_t) {
                                         if (!e) on_stop_signal();
                                     });

        work_.emplace(asio::make_work_guard(ioc_));
        spawn_detached(accept_loop(), [] {});
        // When shutdown() arrives before listen(), event_fd_ is still -1 and
        // the signal is swallowed: re-emit it here so the subsequent run()
        // can return
        if (stopping_.load()) {
            uint64_t one = 1;
            [[maybe_unused]] ssize_t r = ::write(event_fd_, &one, sizeof(one));
        }
        if (tls_holder_) tls_holder_->start_watch(cfg_.tls_reload_interval_sec);
        LOG_INFO("beast http{} server listening on {}:{}{}", tls_ctx_ ? "s" : "", addr, port_,
                 tls_holder_ ? std::string(" (tls: ") + tls_holder_->summary() + ")" : "");
    }

    uint16_t bound_port() const override { return port_; }
    ConnStats stats() const override { return counters_.snapshot(); }
    bool reload_tls() override { return tls_holder_ && tls_holder_->reload_now(); }

    void run() override {
        int n = std::max(1, cfg_.io_threads);
        LOG_INFO("beast driver: io_threads={} -> {} io_context thread(s)", cfg_.io_threads, n);
        std::vector<std::thread> threads;
        threads.reserve(n - 1);
        for (int i = 1; i < n; ++i) threads.emplace_back([this] { ioc_.run(); });
        ioc_.run();
        for (auto& t : threads) t.join();
        LOG_INFO("beast http server stopped");
    }

    // Performs only async-signal-safe operations; callable from a signal
    // handler. The exchange guarantees the eventfd is written only once:
    // after finish() the fd number may have been reused and must not be
    // written again
    void shutdown() override {
        if (stopping_.exchange(true)) return;
        if (event_fd_ >= 0) {
            uint64_t one = 1;
            [[maybe_unused]] ssize_t r = ::write(event_fd_, &one, sizeof(one));
        }
    }

private:
    Task<void> accept_loop() {
        for (;;) {
            auto [ec, sock] = co_await AcceptAwaiter{*acceptor_, ioc_, {}, {}};
            if (ec) {
                if (stopping_.load() || ec == asio::error::operation_aborted) break;
                // Retrying transient errors (fd exhaustion like EMFILE) immediately would busy-spin; back off, then continue
                LOG_WARN("accept failed: {}, throttling", ec.message());
                asio::steady_timer backoff(ctl_strand_, std::chrono::milliseconds(100));
                co_await io_op([&](auto cb) {
                    backoff.async_wait(
                        [cb = std::move(cb)](beast::error_code e) mutable { cb(e, size_t{0}); });
                });
                continue;
            }
            auto sess = std::make_shared<Session>(std::move(sock));
            {
                std::lock_guard lk(m_);
                if (stopping_.load()) break;
                // Concurrent-connection cap (uniform across the four drivers): without one, per-connection coroutine frames/buffers can exhaust memory
                if (sessions_.size() >= static_cast<size_t>(cfg_.max_connections)) {
                    LOG_WARN("connection limit ({}) reached, rejecting", cfg_.max_connections);
                    counters_.rejected_limit.fetch_add(1, std::memory_order_relaxed);
                    continue;  // sess closes the socket as it leaves scope
                }
                sessions_.insert(sess);
                counters_.accepted.fetch_add(1, std::memory_order_relaxed);
                counters_.active.store(sessions_.size(), std::memory_order_relaxed);
            }
            spawn_detached(session_run(sess), [this, sess] { on_session_done(sess); });
        }
    }

    // Session entry: plaintext goes straight into the request loop; TLS
    // handshakes first and sends close_notify after the loop ends. TlsStream
    // references the tcp_stream in Session and lives on this coroutine frame
    // — destroyed only after session_loop completes, so nothing dangles
    Task<void> session_run(std::shared_ptr<Session> sess) {
        sess->stream.socket().set_option(tcp::no_delay(true));
        auto idle = std::chrono::seconds(cfg_.header_timeout_sec);  // handshake: header bound
        if (tls_ctx_) {
            TlsStream tls(sess->stream, *tls_ctx_);
            sess->stream.expires_after(idle);
            auto [hec, hn] = co_await io_op([&](auto cb) {
                tls.async_handshake(asio::ssl::stream_base::server,
                                    [cb = std::move(cb)](beast::error_code e) mutable {
                                        cb(e, size_t{0});
                                    });
            });
            (void)hn;
            sess->stream.expires_never();
            counters_.tls_handshake(!hec);
            if (hec) {
                // Plaintext client hitting the TLS port / probe traffic: one warning line suffices; skip the request loop
                LOG_WARN("TLS handshake failed from client: {}", hec.message());
            } else {
                co_await session_loop(sess, tls);
                // Best-effort close_notify (with a timeout backstop); failure is fine, TCP gets closed right after anyway
                sess->stream.expires_after(idle);
                co_await io_op([&](auto cb) {
                    tls.async_shutdown(
                        [cb = std::move(cb)](beast::error_code e) mutable { cb(e, size_t{0}); });
                });
                sess->stream.expires_never();
            }
        } else {
            co_await session_loop(sess, sess->stream);
        }
        beast::error_code ig;
        sess->stream.socket().shutdown(tcp::socket::shutdown_both, ig);
    }

    template <class Stream>
    Task<void> session_loop(std::shared_ptr<Session> sess, Stream& stream) {
        beast::flat_buffer buffer;  // Kept across keep-alive requests (the parser may over-read)
        bool keep = true;
        int served = 0;  // keep-alive budget (http.max_requests_per_connection)

        while (keep && !stopping_.load()) {
            bhttp::request_parser<bhttp::buffer_body> parser;
            parser.header_limit(static_cast<uint32_t>(cfg_.max_header_size));
            // Size limits are L2's responsibility: XML-style requests are
            // capped at 1MiB via read_body (s3/handlers/common.h), and the
            // PUT data plane streams through in 64KiB chunks without landing
            // in memory; no object-size cap is set (consistent with the other
            // drivers — an S3-semantics decision)
            parser.body_limit(boost::none);
            // Phase timeouts (roadmap §4.2): a fresh connection's request line + headers
            // are bounded by header_timeout, a reused one's wait by idle_timeout (one
            // read op covers both, so the two cannot be told apart finer than this)
            bool fresh = served == 0;
            beast::get_lowest_layer(stream).expires_after(std::chrono::seconds(
                fresh ? cfg_.header_timeout_sec : cfg_.idle_timeout_sec));
            {
                auto [ec, n] = co_await io_op([&](auto cb) {
                    bhttp::async_read_header(stream, buffer, parser, std::move(cb));
                });
                (void)n;
                beast::get_lowest_layer(stream).expires_never();
                if (ec == beast::error::timeout)
                    driver::count_timeout(counters_,
                                          fresh ? driver::Phase::Header : driver::Phase::Idle);
                // A parser verdict (bad request line / header, header_limit) is a
                // malformed request (roadmap §5.3); a peer that closed mid-message
                // (end_of_stream / partial_message) or a transport error is not
                else if (ec && ec.category() == bhttp::make_error_code(bhttp::error::bad_method).category() &&
                         ec != bhttp::error::end_of_stream && ec != bhttp::error::partial_message)
                    counters_.parse_error();
                if (ec) break;  // eof / timeout / closed by shutdown
            }
            sess->in_flight.store(true);

            auto& preq = parser.get();
            HttpRequest req;
            req.method = std::string(preq.method_string().data(), preq.method_string().size());
            driver::parse_target(
                std::string_view(preq.target().data(), preq.target().size()), req);
            for (auto& f : preq.base())
                req.headers.add(std::string(f.name_string()), std::string(f.value()));
            {
                beast::error_code epc;
                auto ep = beast::get_lowest_layer(stream).socket().remote_endpoint(epc);
                if (!epc) req.remote_addr = ep.address().to_string();
            }

            // Message framing validation (drivers/common.h parse_body_framing):
            // beast's own parsing is more lenient about CL/TE conflicts etc.,
            // and this implementation decodes no transfer encoding other than
            // chunked — reject all of it at L1, so all four drivers
            // accept/reject the same request set
            if (!driver::parse_body_framing(req.headers).valid) {
                counters_.parse_error();
                auto bad = driver::bad_request_response("Invalid message framing.");
                co_await write_response(stream, bad, /*head_request=*/false, /*keep=*/false);
                break;
            }
            counters_.request_parsed();

            BodyCtx<Stream> bctx{&parser, &stream, &buffer, cfg_.body_timeout_sec, &counters_};
            if (auto e = req.headers.get("Expect"); e && HeaderMap::ieq(*e, "100-continue"))
                bctx.need_100 = true;
            std::optional<uint64_t> content_length;
            if (auto l = parser.content_length()) content_length = *l;
            if (!parser.is_done() || content_length)
                req.body = std::make_unique<BeastBodyReader<Stream>>(&bctx, content_length);

            bool head_request = req.method == "HEAD";
            bool client_keep = preq.keep_alive();
            HttpResponse resp;
            try {
                resp = co_await handler_(std::move(req));
            } catch (const std::exception& e) {
                // L2 catches all exceptions; reaching here means something failed outside L2 (contract 2)
                resp = driver::internal_error_response(e.what());
                keep = false;
            }
            co_await ResumeOn{stream.get_executor()};  // The handler may resume on a pool thread

            if (stopping_.load() || !client_keep) keep = false;
            // The unconsumed body must be drained before reusing the
            // connection; if 100-continue was never sent, the client may
            // never send a body — do not wait blindly, just close
            if (!parser.is_done()) {
                if (bctx.need_100 || bctx.errored) keep = false;
                else if (keep) keep = co_await drain_body(bctx);
            }

            if (keep && driver::keepalive_budget_exhausted(served + 1,
                                                           cfg_.max_requests_per_connection)) {
                keep = false;
                counters_.keepalive_closes.fetch_add(1, std::memory_order_relaxed);
            }
            bool ok = co_await write_response(stream, resp, head_request, keep);
            sess->in_flight.store(false);
            if (!ok) co_return;
            ++served;
        }
    }

    template <class Stream>
    Task<bool> drain_body(BodyCtx<Stream>& ctx) {
        std::vector<std::byte> tmp(driver::kScratchBytes);
        uint64_t drained = 0;
        while (!ctx.parser->is_done()) {
            auto& body = ctx.parser->get().body();
            body.data = tmp.data();
            body.size = tmp.size();
            beast::get_lowest_layer(*ctx.stream)
                .expires_after(std::chrono::seconds(ctx.idle_timeout_sec));
            auto [ec, n] = co_await io_op([&](auto cb) {
                bhttp::async_read(*ctx.stream, *ctx.buffer, *ctx.parser, std::move(cb));
            });
            (void)n;
            beast::get_lowest_layer(*ctx.stream).expires_never();
            if (ec == bhttp::error::need_buffer) ec = {};
            if (ec) co_return false;
            drained += tmp.size() - body.size;
            if (drained > cfg_.drain_limit) co_return false;  // Too large; give up and close the connection
        }
        co_return true;
    }

    template <class Stream>
    Task<bool> write_response(Stream& stream, HttpResponse& resp, bool head_request,
                              bool keep) {
        bool no_body_status = resp.status == 204 || resp.status == 304 || resp.status < 200;
        auto idle = std::chrono::seconds(cfg_.write_timeout_sec);  // write_timeout per write op
        auto note_write = [&](const beast::error_code& ec) {
            if (ec == beast::error::timeout)
                driver::count_timeout(counters_, driver::Phase::Write);
        };

        // Small response / HEAD / bodyless status code: write the whole message at once
        if (!resp.stream_body || head_request || no_body_status) {
            bhttp::response<bhttp::string_body> res;
            res.result(static_cast<unsigned>(resp.status));
            res.version(11);
            // Unified outbound-header filtering (drivers/common.h, contract
            // 5): beast's try_create_new_element only checks length, not
            // CR/LF, so a direct insert was the one response-splitting
            // injection surface among the four drivers (backend metadata can
            // come from upstream S3 / duostore metadata storage, outside L1
            // inbound filtering); an over-long header would also throw and
            // prevent any response from being sent at all
            driver::emit_headers(resp.headers, [&](const std::string& k, const std::string& v) {
                res.insert(k, v);
            });
            if (!resp.headers.has("Date"))
                res.set(bhttp::field::date, util::http_date(std::chrono::system_clock::now()));
            // HEAD with unknown length (streaming without content_length):
            // write neither Content-Length nor Transfer-Encoding; close the
            // connection instead (drivers/common.h contract 6, uniform across
            // the four drivers). The old behavior of writing
            // Content-Length: 0 was a lie — GET does not return 0 bytes
            bool head_unknown_len = head_request && !driver::head_length_known(resp);
            if (head_unknown_len) keep = false;
            res.keep_alive(keep);
            if (!no_body_status && !head_unknown_len) {
                uint64_t len = resp.content_length.value_or(
                    resp.stream_body && resp.stream_body->length()
                        ? *resp.stream_body->length()
                        : resp.small_body.size());
                res.set(bhttp::field::content_length, std::to_string(len));
                if (!head_request) res.body() = std::move(resp.small_body);
            }
            beast::get_lowest_layer(stream).expires_after(idle);
            auto [ec, n] = co_await io_op([&](auto cb) {
                bhttp::async_write(stream, res, std::move(cb));
            });
            (void)n;
            beast::get_lowest_layer(stream).expires_never();
            note_write(ec);
            co_return !ec;
        }

        // Streaming response: serializer + buffer_body, pulled in 64KiB chunks (docs/architecture.md request lifecycle)
        bhttp::response<bhttp::buffer_body> res;
        res.result(static_cast<unsigned>(resp.status));
        res.version(11);
        driver::emit_headers(resp.headers, [&](const std::string& k, const std::string& v) {
            res.insert(k, v);
        });
        if (!resp.headers.has("Date"))
            res.set(bhttp::field::date, util::http_date(std::chrono::system_clock::now()));
        res.keep_alive(keep);
        if (resp.content_length)
            res.content_length(*resp.content_length);
        else
            res.chunked(true);
        res.body().data = nullptr;
        res.body().more = true;

        bhttp::response_serializer<bhttp::buffer_body> sr{res};
        beast::get_lowest_layer(stream).expires_after(idle);
        {
            auto [ec, n] = co_await io_op([&](auto cb) {
                bhttp::async_write_header(stream, sr, std::move(cb));
            });
            (void)n;
            beast::get_lowest_layer(stream).expires_never();
            note_write(ec);
            if (ec) co_return false;
        }

        std::vector<std::byte> buf(cfg_.io_chunk_size);
        uint64_t written = 0;
        for (;;) {
            size_t n = 0;
            try {
                n = co_await resp.stream_body->read(std::span(buf));
            } catch (const std::exception& e) {
                LOG_ERROR("stream body read failed mid-response: {}", e.what());
                co_return false;  // Response head already sent; can only disconnect (contract 3: discard the result)
            }
            co_await ResumeOn{stream.get_executor()};
            // Byte accounting for fixed-length responses (consistent with the
            // other three drivers): writing too much breaks message framing;
            // writing too little must not stay keep-alive — the client would
            // read the next response's status line as the rest of this body
            if (resp.content_length && written + n > *resp.content_length) {
                LOG_ERROR("stream body overruns declared Content-Length ({} + {} > {})",
                          written, n, *resp.content_length);
                co_return false;
            }
            if (n == 0 && resp.content_length && written != *resp.content_length) {
                LOG_ERROR("stream body short of declared Content-Length ({} != {})", written,
                          *resp.content_length);
                co_return false;
            }
            written += n;
            if (n == 0) {
                res.body().data = nullptr;
                res.body().more = false;
            } else {
                res.body().data = buf.data();
                res.body().size = n;
                res.body().more = true;
            }
            beast::get_lowest_layer(stream).expires_after(idle);
            auto [ec, wrote] = co_await io_op([&](auto cb) {
                bhttp::async_write(stream, sr, std::move(cb));
            });
            (void)wrote;
            beast::get_lowest_layer(stream).expires_never();
            if (ec == bhttp::error::need_buffer) ec = {};
            note_write(ec);
            if (ec) co_return false;
            if (n == 0) break;
        }
        co_return true;
    }

    // ---- Graceful shutdown orchestration (contract 4: run() returns after in-flight requests finish or time out) ----

    void on_stop_signal() {
        beast::error_code ig;
        acceptor_->close(ig);
        std::vector<std::shared_ptr<Session>> idle;
        bool empty;
        {
            std::lock_guard lk(m_);
            // Sessions that ended before this do not trigger finish() (see
            // on_session_done), guaranteeing the acceptor closes before
            // finish(); otherwise the pending async_accept would keep the
            // io_context busy forever and run() could not return
            stop_handled_ = true;
            for (auto& s : sessions_)
                if (!s->in_flight.load()) idle.push_back(s);
            empty = sessions_.empty();
        }
        for (auto& s : idle) close_session(s);  // Idle keep-alive connections get cut directly
        if (empty) {
            finish();
            return;
        }
        grace_timer_.emplace(ctl_strand_, std::chrono::seconds(cfg_.shutdown_grace_sec));
        grace_timer_->async_wait([this](beast::error_code e) {
            if (e) return;
            std::vector<std::shared_ptr<Session>> rest;
            {
                std::lock_guard lk(m_);
                rest.assign(sessions_.begin(), sessions_.end());
            }
            LOG_WARN("forcing {} connection(s) closed on shutdown", rest.size());
            for (auto& s : rest) close_session(s);
            force_timer_.emplace(ctl_strand_, std::chrono::seconds(cfg_.shutdown_force_wait_sec));
            force_timer_->async_wait([this](beast::error_code e2) {
                if (!e2) ioc_.stop();  // Last resort: stop waiting for stuck sessions
            });
        });
    }

    void close_session(const std::shared_ptr<Session>& s) {
        // Sockets are not thread-safe: the close is posted onto the connection's own strand
        asio::post(s->stream.get_executor(), [s] { s->stream.close(); });
    }

    void on_session_done(const std::shared_ptr<Session>& sess) {
        bool finish_now;
        {
            std::lock_guard lk(m_);
            sessions_.erase(sess);
            counters_.active.store(sessions_.size(), std::memory_order_relaxed);
            // No finish() before stop_handled_: when shutdown() has just set
            // stopping_ but the eventfd event is not yet processed, the last
            // session ending must not jump the gun (otherwise finish would
            // close the stop eventfd, on_stop_signal would be skipped, and
            // the acceptor would never close)
            finish_now = stop_handled_ && sessions_.empty();
        }
        if (finish_now) finish();
    }

    void finish() {
        std::call_once(finish_once_, [this] {
            // The wrap-up touches the grace/force timers and stop_event, so
            // it must serialize on the same strand as on_stop_signal
            asio::post(ctl_strand_, [this] {
                if (grace_timer_) grace_timer_->cancel();
                if (force_timer_) force_timer_->cancel();
                beast::error_code ig;
                if (stop_event_) stop_event_->close(ig);
                work_.reset();  // run() returns once the io_context drains
            });
        });
    }

    HttpConfig cfg_;
    Handler handler_;
    std::shared_ptr<tls::Holder> tls_holder_;    // Declared before tls_ctx_: the context's cert callback points here
    std::optional<asio::ssl::context> tls_ctx_;  // Present means HTTPS (knobs applied at construction)
    asio::io_context ioc_;
    // Control-plane strand: all operations on acceptor / stop_event /
    // grace_timer / force_timer serialize here (the data plane still has one
    // strand per connection)
    asio::strand<asio::io_context::executor_type> ctl_strand_ = asio::make_strand(ioc_);
    std::optional<asio::executor_work_guard<asio::io_context::executor_type>> work_;
    std::optional<tcp::acceptor> acceptor_;
    std::optional<asio::posix::stream_descriptor> stop_event_;
    std::optional<asio::steady_timer> grace_timer_;
    std::optional<asio::steady_timer> force_timer_;
    int event_fd_ = -1;
    uint64_t stop_buf_ = 0;
    uint16_t port_ = 0;
    std::atomic<bool> stopping_{false};
    std::mutex m_;
    bool stop_handled_ = false;  // on_stop_signal has run (guarded by m_)
    std::set<std::shared_ptr<Session>> sessions_;
    driver::ConnCounters counters_;  // IHttpServer::stats() (roadmap §4.2)
    std::once_flag finish_once_;
};

}  // namespace

void register_beast_driver() {
    HttpServerFactory::register_driver("beast", [](const HttpConfig& cfg) {
        return std::make_unique<BeastServer>(cfg);
    });
}

}  // namespace lights3::http
