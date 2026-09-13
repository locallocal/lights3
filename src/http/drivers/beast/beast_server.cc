// L1: Boost.Beast driver — asynchronous model (docs/architecture/http-adapter.md §3.1).
// One io_context per io thread, each run by exactly that thread; a connection is
// pinned to one of them at accept, so every completion of its socket is a
// same-thread continuation (http-adapter.md §2.4 ⑩: with N threads sharing one
// io_context every completion crossed threads -- ~2.5 futex wake/wait pairs per
// TLS record, measured 655 futex calls per 4 MiB TLS GET). Session coroutines
// use the project's own Task<void> directly: asio async operations are adapted
// to suspend/resume via awaiters, matching the junction-point semantics of
// docs/architecture/concurrency.md §4.1 (the handler's continuation runs back on
// the connection's io thread), just without converting between the
// asio::awaitable and Task coroutine types.
#include <sys/eventfd.h>
#include <unistd.h>

#include <openssl/bio.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/post.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/asio/steady_timer.hpp>
#include <boost/asio/strand.hpp>
#include <boost/asio/write.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>

#include <array>
#include <atomic>
#include <coroutine>
#include <cstring>
#include <mutex>
#include <optional>
#include <set>
#include <thread>
#include <vector>

#include "core/log.h"
#include "core/task.h"
#include "core/util/time.h"
#include "http/drivers/common.h"
#include "http/server.h"
#include "http/tls.h"

namespace lights3::http {

namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
// Avoids clashing with lights3::http
namespace bhttp = boost::beast::http;
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
// may resume on a pool thread, and we must switch back to the connection's io
// thread before starting the next socket operation
struct ResumeOn {
    asio::any_io_executor ex;
    // Fast path (roadmap §4.3 ⑥): a coroutine that is already running on this
    // connection's io thread (the previous await completed inline, or the
    // handler resumed there) continues without an asio::post round trip. The
    // connection executors are the plain executors of the per-thread
    // io_contexts (no strand: one thread per context serializes by
    // construction), so the type probe matches; anything else takes the safe post
    bool await_ready() const noexcept {
        if (auto* ex_ioc = ex.target<asio::io_context::executor_type>()) return ex_ioc->running_in_this_thread();
        return false;
    }
    void await_suspend(std::coroutine_handle<> h) {
        asio::post(ex, [h] { h.resume(); });
    }
    void await_resume() const noexcept {}
};

struct AcceptAwaiter {
    tcp::acceptor& acc;
    // io thread the new connection is pinned to: all of the socket's completion
    // callbacks run on the single thread driving this executor's io_context
    asio::io_context::executor_type peer_ex;
    beast::error_code ec{};
    std::optional<tcp::socket> sock;

    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) {
        acc.async_accept(peer_ex, [this, h](beast::error_code e, tcp::socket s) {
            ec = e;
            sock.emplace(std::move(s));
            h.resume();
        });
    }
    std::pair<beast::error_code, tcp::socket> await_resume() { return {ec, std::move(*sock)}; }
};

// Fire-and-forget launch of a Task<void>: the driver's entry point for running the handler coroutine to completion in
// its own execution environment
struct Detached {
    struct promise_type {
        Detached get_return_object() { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        // spawn_detached already catches everything
        void unhandled_exception() { std::terminate(); }
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

    // Inactivity watchdog for the multi-operation phases -- body reads and
    // streaming writes (http-adapter.md §2.4 ⑪). beast::basic_stream arms and
    // cancels its per-operation timer around every async_read_some /
    // async_write_some once an expiry is set; under TLS that is once per 16 KiB
    // record (263 timerfd_settime per 4 MiB GET measured), so those phases run
    // with expires_never() and this one timer instead. It is armed once, and on
    // expiry checks the operation in flight: overdue -> the phase's timeout is
    // counted and the socket closed (the pending operation fails); otherwise it is
    // re-armed for the remainder, or left dormant when nothing is in flight. All
    // fields are touched on the connection's io thread only
    asio::steady_timer watchdog;
    std::chrono::steady_clock::time_point op_start{};
    std::chrono::seconds op_timeout{0};
    driver::Phase op_phase = driver::Phase::Body;
    bool in_op = false;
    bool armed = false;
    bool timed_out = false;
    // async_wait completions carry the generation they were scheduled with
    uint64_t wd_gen = 0;

    explicit Session(tcp::socket&& s) : stream(std::move(s)), watchdog(stream.get_executor()) {}
};

void wd_schedule(std::shared_ptr<Session> s, std::chrono::steady_clock::duration d, driver::ConnCounters& counters) {
    s->armed = true;
    uint64_t gen = ++s->wd_gen;
    s->watchdog.expires_after(d);
    s->watchdog.async_wait([s, gen, &counters](beast::error_code ec) {
        // cancelled, or superseded by a later schedule
        if (ec || gen != s->wd_gen) return;
        s->armed = false;
        if (!s->in_op) return;
        auto elapsed = std::chrono::steady_clock::now() - s->op_start;
        if (elapsed < s->op_timeout) {
            wd_schedule(s, s->op_timeout - elapsed, counters);
            return;
        }
        s->timed_out = true;
        driver::count_timeout(counters, s->op_phase);
        s->stream.close();
    });
}

// Marks the start of one socket operation bounded by `timeout`; arms the watchdog
// on first use. Pair with wd_end() once the operation completed
void wd_begin(const std::shared_ptr<Session>& s, int timeout_sec, driver::Phase phase, driver::ConnCounters& counters) {
    s->in_op = true;
    s->op_start = std::chrono::steady_clock::now();
    s->op_timeout = std::chrono::seconds(timeout_sec);
    s->op_phase = phase;
    if (!s->armed) wd_schedule(s, s->op_timeout, counters);
}
void wd_end(Session& s) { s.in_op = false; }
// Session end: drop the pending wait so the handler's shared_ptr does not keep
// the socket alive until the next expiry
void wd_stop(Session& s) {
    ++s.wd_gen;
    s.watchdog.cancel();
}

// TLS session stream over the session's tcp_stream (http-adapter.md §2.4 ⑫).
// asio::ssl::stream drives OpenSSL through a 17 KiB BIO pair and a 17 KiB
// output buffer, so every 64 KiB chunk beast hands it becomes four
// async_write_some rounds on the socket (one per TLS record), each a composed
// operation with its own completion; that fixed cost per record is what kept
// beast's TLS GET at ~80% of the blocking drivers. This stream uses memory BIOs
// instead: async_write_some encrypts the whole buffer sequence with SSL_write
// (the memory BIO grows), drains the ciphertext once and sends it with a single
// asio::async_write; async_read_some pulls up to one io chunk of ciphertext per
// socket read and decrypts as many records as that yields. Handshake and
// close_notify are plain coroutine loops. The object references the tcp_stream
// (owned by Session) and lives on the session coroutine frame; timeouts apply to
// the tcp_stream underneath (beast::get_lowest_layer), covering handshake and
// reads/writes alike
class TlsStream {
public:
    using executor_type = beast::tcp_stream::executor_type;

    TlsStream(beast::tcp_stream& next, asio::ssl::context& ctx, size_t chunk)
        : next_(next), ssl_(::SSL_new(ctx.native_handle())), in_(chunk) {
        if (!ssl_) throw std::runtime_error("SSL_new failed");
        BIO* rbio = ::BIO_new(::BIO_s_mem());
        BIO* wbio = ::BIO_new(::BIO_s_mem());
        if (!rbio || !wbio) {
            ::BIO_free(rbio);
            ::BIO_free(wbio);
            ::SSL_free(ssl_);
            throw std::runtime_error("BIO_new failed");
        }
        // the SSL owns both BIOs from here on
        ::SSL_set_bio(ssl_, rbio, wbio);
        rbio_ = rbio;
        wbio_ = wbio;
        ::SSL_set_accept_state(ssl_);
    }
    ~TlsStream() { ::SSL_free(ssl_); }
    TlsStream(const TlsStream&) = delete;
    TlsStream& operator=(const TlsStream&) = delete;

    executor_type get_executor() { return next_.get_executor(); }
    beast::tcp_stream& next_layer() { return next_; }
    SSL* native_handle() { return ssl_; }

    // Server-side handshake; the caller bounds it with an expiry on next_layer()
    Task<beast::error_code> handshake() {
        for (;;) {
            int r = ::SSL_do_handshake(ssl_);
            int err = r == 1 ? SSL_ERROR_NONE : ::SSL_get_error(ssl_, r);
            // ServerHello & co. are waiting in the write BIO whatever the verdict
            if (auto ec = co_await flush()) co_return ec;
            if (r == 1) co_return beast::error_code{};
            if (err != SSL_ERROR_WANT_READ) co_return ssl_error(err);
            if (auto ec = co_await fill()) co_return ec;
        }
    }

    // One-way close_notify (best effort, like asio's async_shutdown without
    // waiting for the peer's reply)
    Task<beast::error_code> shutdown() {
        ::SSL_shutdown(ssl_);
        co_return co_await flush();
    }

    // AsyncReadStream: decrypts into the first non-empty buffer of the sequence;
    // may deliver fewer bytes than requested (read_some semantics)
    template <class MutableBufferSequence, class Handler>
    void async_read_some(const MutableBufferSequence& buffers, Handler&& h) {
        asio::mutable_buffer dst = first_nonempty(buffers);
        if (dst.size() == 0) {
            complete(std::forward<Handler>(h), beast::error_code{}, 0);
            return;
        }
        read_step(dst, std::forward<Handler>(h), /*from_initiation=*/true);
    }

    // AsyncWriteStream: encrypts the whole sequence and sends it in one
    // composed write; completes with the number of plaintext bytes consumed.
    // Every SSL_write call closes a TLS record, and beast hands the response
    // head over as one small buffer per field name / value / CRLF (15 buffers
    // for a typical S3 response): written one by one they became 15 tiny
    // records, which cost the server ~6 us and the client ~45 us per request
    // (asio::ssl::stream avoids it by linearising up to 17 KiB per round). So
    // small buffers are coalesced into a record-sized staging area first, and
    // only a buffer that is itself at least one record long (the 64 KiB body
    // chunks) goes to SSL_write directly, without a copy
    template <class ConstBufferSequence, class Handler>
    void async_write_some(const ConstBufferSequence& buffers, Handler&& h) {
        size_t consumed = 0;
        auto fail = [&](int n) { complete(std::forward<Handler>(h), ssl_error(::SSL_get_error(ssl_, n)), 0); };
        auto ssl_write_all = [&](const unsigned char* p, size_t len) -> bool {
            while (len > 0) {
                int n = ::SSL_write(ssl_, p, static_cast<int>(std::min<size_t>(len, INT32_MAX)));
                if (n <= 0) {
                    fail(n);
                    return false;
                }
                p += n;
                len -= static_cast<size_t>(n);
            }
            return true;
        };
        auto flush_staging = [&]() -> bool {
            if (staged_ == 0) return true;
            bool ok = ssl_write_all(staging_.data(), staged_);
            staged_ = 0;
            return ok;
        };
        for (auto it = asio::buffer_sequence_begin(buffers); it != asio::buffer_sequence_end(buffers); ++it) {
            asio::const_buffer b = *it;
            const auto* p = static_cast<const unsigned char*>(b.data());
            size_t left = b.size();
            consumed += left;
            while (left > 0) {
                if (staged_ == 0 && left >= kRecordPlain) {
                    if (!ssl_write_all(p, left)) return;
                    break;
                }
                size_t take = std::min(left, kRecordPlain - staged_);
                std::memcpy(staging_.data() + staged_, p, take);
                staged_ += take;
                p += take;
                left -= take;
                if (staged_ == kRecordPlain && !flush_staging()) return;
            }
        }
        if (!flush_staging()) return;
        if (!drain_output()) {
            complete(std::forward<Handler>(h), beast::error_code{}, consumed);
            return;
        }
        asio::async_write(next_, asio::buffer(out_),
                          [h = std::forward<Handler>(h), consumed](beast::error_code ec, size_t) mutable {
                              h(ec, ec ? 0 : consumed);
                          });
    }

private:
    template <class Handler>
    void complete(Handler&& h, beast::error_code ec, size_t n) {
        // An initiating function must not run its completion inline
        asio::post(next_.get_executor(), [h = std::forward<Handler>(h), ec, n]() mutable { h(ec, n); });
    }

    template <class MutableBufferSequence>
    static asio::mutable_buffer first_nonempty(const MutableBufferSequence& buffers) {
        for (auto it = asio::buffer_sequence_begin(buffers); it != asio::buffer_sequence_end(buffers); ++it) {
            asio::mutable_buffer b = *it;
            if (b.size() > 0) return b;
        }
        return {};
    }

    // Moves the write BIO's ciphertext into out_; false when there is none
    bool drain_output() {
        size_t pend = ::BIO_ctrl_pending(wbio_);
        if (pend == 0) return false;
        out_.resize(pend);
        int got = ::BIO_read(wbio_, out_.data(), static_cast<int>(std::min<size_t>(pend, INT32_MAX)));
        out_.resize(got > 0 ? static_cast<size_t>(got) : 0);
        return !out_.empty();
    }

    Task<beast::error_code> flush() {
        if (!drain_output()) co_return beast::error_code{};
        auto [ec, n] = co_await io_op([&](auto cb) { asio::async_write(next_, asio::buffer(out_), std::move(cb)); });
        (void)n;
        co_return ec;
    }

    // One socket read into the read BIO
    Task<beast::error_code> fill() {
        auto [ec, n] = co_await io_op([&](auto cb) { next_.async_read_some(asio::buffer(in_), std::move(cb)); });
        if (ec) co_return ec;
        ::BIO_write(rbio_, in_.data(), static_cast<int>(n));
        co_return beast::error_code{};
    }

    template <class Handler>
    void read_step(asio::mutable_buffer dst, Handler&& h, bool from_initiation) {
        int n = ::SSL_read(ssl_, dst.data(), static_cast<int>(std::min<size_t>(dst.size(), INT32_MAX)));
        if (n > 0) {
            if (from_initiation)
                complete(std::forward<Handler>(h), beast::error_code{}, static_cast<size_t>(n));
            else
                h(beast::error_code{}, static_cast<size_t>(n));
            return;
        }
        int err = ::SSL_get_error(ssl_, n);
        if (err == SSL_ERROR_WANT_READ) {
            // Post-handshake messages the peer must see before it sends more
            // (TLS 1.3 tickets, KeyUpdate replies) sit in the write BIO: send them
            // first, then wait for ciphertext
            if (drain_output()) {
                asio::async_write(next_, asio::buffer(out_),
                                  [this, dst, h = std::forward<Handler>(h)](beast::error_code ec, size_t) mutable {
                                      if (ec) {
                                          h(ec, 0);
                                          return;
                                      }
                                      read_step(dst, std::move(h), false);
                                  });
                return;
            }
            next_.async_read_some(asio::buffer(in_),
                                  [this, dst, h = std::forward<Handler>(h)](beast::error_code ec, size_t got) mutable {
                                      if (ec) {
                                          h(ec, 0);
                                          return;
                                      }
                                      ::BIO_write(rbio_, in_.data(), static_cast<int>(got));
                                      read_step(dst, std::move(h), false);
                                  });
            return;
        }
        beast::error_code ec = err == SSL_ERROR_ZERO_RETURN ? beast::error_code(asio::error::eof) : ssl_error(err);
        if (from_initiation)
            complete(std::forward<Handler>(h), ec, 0);
        else
            h(ec, 0);
    }

    static beast::error_code ssl_error(int err) {
        unsigned long e = ::ERR_get_error();
        ::ERR_clear_error();
        if (e != 0) return beast::error_code(static_cast<int>(e), asio::error::get_ssl_category());
        // No queued reason: a syscall-level failure or a truncated stream
        if (err == SSL_ERROR_SYSCALL) return beast::error_code(asio::ssl::error::stream_truncated);
        return beast::error_code(asio::error::operation_not_supported);
    }

    // TLS plaintext record size: the coalescing threshold
    static constexpr size_t kRecordPlain = 16384;

    beast::tcp_stream& next_;
    SSL* ssl_;
    BIO* rbio_ = nullptr;
    BIO* wbio_ = nullptr;
    // ciphertext in / out staging
    std::vector<unsigned char> in_;
    std::vector<unsigned char> out_;
    // plaintext coalescing area for small buffers (one record)
    std::array<unsigned char, kRecordPlain> staging_{};
    size_t staged_ = 0;
};

// Per-request body-read state; owned by the session coroutine frame (still
// needs draining after the handler's reader is destroyed).
// Stream = beast::tcp_stream (plaintext) or TlsStream: async_read/async_write
// reach the corresponding record layer via the template; all other logic is identical
template <class Stream>
struct BodyCtx {
    bhttp::request_parser<bhttp::buffer_body>* parser;
    Stream* stream;
    beast::flat_buffer* buffer;
    // body_timeout: inactivity bound on one body read (roadmap §4.2)
    int idle_timeout_sec;
    driver::ConnCounters* counters = nullptr;
    // watchdog owner (Session outlives the request)
    std::shared_ptr<Session> sess;
    // bounds the deferred 100 Continue write
    int write_timeout_sec = 0;
    // Expect: 100-continue not yet answered; reply only on the first body read
    bool need_100 = false;
    bool errored = false;
};

template <class Stream>
class BeastBodyReader final : public BodyReader {
public:
    BeastBodyReader(BodyCtx<Stream>* ctx, std::optional<uint64_t> len) : ctx_(ctx), len_(len) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        co_await ResumeOn{ctx_->stream->get_executor()};
        if (ctx_->errored) throw std::runtime_error("http body: read after connection error");
        // Deferred 100-continue: the client is told to send only once the handler decides it wants the body
        // (docs/architecture/http-adapter.md §3.1)
        if (ctx_->need_100) {
            ctx_->need_100 = false;
            bhttp::response<bhttp::empty_body> cont{bhttp::status::continue_, 11};
            wd_begin(ctx_->sess, ctx_->write_timeout_sec, driver::Phase::Write, *ctx_->counters);
            auto [ec, n] = co_await io_op([&](auto cb) { bhttp::async_write(*ctx_->stream, cont, std::move(cb)); });
            (void)n;
            wd_end(*ctx_->sess);
            if (ec) fail(ec, ctx_->sess->timed_out ? "100 Continue write timed out" : "failed to send 100 Continue");
        }
        if (ctx_->parser->is_done()) co_return 0;

        auto& body = ctx_->parser->get().body();
        body.data = buf.data();
        body.size = buf.size();
        // Bounded by the session watchdog, not the per-operation stream expiry
        wd_begin(ctx_->sess, ctx_->idle_timeout_sec, driver::Phase::Body, *ctx_->counters);
        auto [ec, n] = co_await io_op(
            [&](auto cb) { bhttp::async_read(*ctx_->stream, *ctx_->buffer, *ctx_->parser, std::move(cb)); });
        (void)n;
        wd_end(*ctx_->sess);
        if (ec == bhttp::error::need_buffer) ec = {};
        if (ec) fail(ec, ctx_->sess->timed_out ? "body read timed out" : "client disconnected mid-body");
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
    explicit BeastServer(const HttpConfig& cfg)
        : cfg_(cfg), io_(make_io_threads(cfg)), ctl_strand_(asio::make_strand(io_[0]->ioc)) {
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
                throw std::runtime_error(std::string("beast driver: failed to set up TLS: ") + e.what());
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
        stop_event_->async_read_some(asio::buffer(&stop_buf_, sizeof(stop_buf_)), [this](beast::error_code e, size_t) {
            if (!e) on_stop_signal();
        });

        for (auto& io : io_) io->work.emplace(asio::make_work_guard(io->ioc));
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
        LOG_INFO("beast driver: io_threads={} -> {} io_context(s), one thread each", cfg_.io_threads, io_.size());
        std::vector<std::thread> threads;
        threads.reserve(io_.size() - 1);
        for (size_t i = 1; i < io_.size(); ++i) threads.emplace_back([this, i] { io_[i]->ioc.run(); });
        io_[0]->ioc.run();
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
    // One io_context per io thread (http-adapter.md §2.4 ⑩). Concurrency hint 1:
    // exactly one thread calls run() on each, which lets asio skip the reactor
    // wake-ups between its own threads; posting into it from pool threads
    // (ResumeOn) stays allowed
    struct IoThread {
        asio::io_context ioc{1};
        std::optional<asio::executor_work_guard<asio::io_context::executor_type>> work;
    };
    static std::vector<std::unique_ptr<IoThread>> make_io_threads(const HttpConfig& cfg) {
        std::vector<std::unique_ptr<IoThread>> v;
        int n = std::max(1, cfg.io_threads);
        v.reserve(static_cast<size_t>(n));
        for (int i = 0; i < n; ++i) v.push_back(std::make_unique<IoThread>());
        return v;
    }

    Task<void> accept_loop() {
        for (;;) {
            // Round-robin over the io threads: the connection lives on the chosen
            // thread for its whole lifetime
            auto& peer = *io_[next_io_++ % io_.size()];
            auto accepted = co_await AcceptAwaiter{*acceptor_, peer.ioc.get_executor(), {}, {}};
            auto& ec = accepted.first;
            auto& sock = accepted.second;
            if (ec) {
                if (stopping_.load() || ec == asio::error::operation_aborted) break;
                // Retrying transient errors (fd exhaustion like EMFILE) immediately would busy-spin; back off, then
                // continue
                LOG_WARN("accept failed: {}, throttling", ec.message());
                asio::steady_timer backoff(ctl_strand_, std::chrono::milliseconds(100));
                co_await io_op([&](auto cb) {
                    backoff.async_wait([cb = std::move(cb)](beast::error_code e) mutable { cb(e, size_t{0}); });
                });
                continue;
            }
            auto sess = std::make_shared<Session>(std::move(sock));
            {
                std::lock_guard lk(m_);
                if (stopping_.load()) break;
                // Concurrent-connection cap (uniform across the four drivers): without one, per-connection coroutine
                // frames/buffers can exhaust memory
                if (sessions_.size() >= static_cast<size_t>(cfg_.max_connections)) {
                    LOG_WARN("connection limit ({}) reached, rejecting", cfg_.max_connections);
                    counters_.rejected_limit.fetch_add(1, std::memory_order_relaxed);
                    // sess closes the socket as it leaves scope
                    continue;
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
        // handshake: header bound
        auto idle = std::chrono::seconds(cfg_.header_timeout_sec);
        if (tls_ctx_) {
            TlsStream tls(sess->stream, *tls_ctx_, cfg_.io_chunk_size);
            sess->stream.expires_after(idle);
            beast::error_code hec = co_await tls.handshake();
            sess->stream.expires_never();
            counters_.tls_handshake(!hec);
            if (hec) {
                // Plaintext client hitting the TLS port / probe traffic: one warning line suffices; skip the request
                // loop
                LOG_WARN("TLS handshake failed from client: {}", hec.message());
            } else {
                // Verified client certificate (backlog-sequence ⑥): once per connection
                co_await session_loop(sess, tls, tls::peer_identity(tls.native_handle()));
                // Best-effort close_notify (with a timeout backstop); failure is fine, TCP gets closed right after
                // anyway
                sess->stream.expires_after(idle);
                co_await tls.shutdown();
                sess->stream.expires_never();
            }
        } else {
            co_await session_loop(sess, sess->stream, std::nullopt);
        }
        wd_stop(*sess);
        beast::error_code ig;
        sess->stream.socket().shutdown(tcp::socket::shutdown_both, ig);
    }

    template <class Stream>
    Task<void> session_loop(std::shared_ptr<Session> sess, Stream& stream, std::optional<TlsIdentity> tls_identity) {
        // Kept across keep-alive requests (the parser may over-read)
        beast::flat_buffer buffer;
        // Socket reads are sized by beast::read_size = max(512, capacity - size):
        // an unreserved flat_buffer grows to 512 bytes on the first read and then
        // stays there, so a 4MiB request body was pulled in ~8000 recv calls
        // (measured 40ms vs 6ms on builtin, roadmap §4.3 baseline). Reserving one
        // io chunk makes every body read a full-size recv
        buffer.reserve(cfg_.io_chunk_size);
        bool keep = true;
        // keep-alive budget (http.max_requests_per_connection)
        int served = 0;

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
            beast::get_lowest_layer(stream).expires_after(
                std::chrono::seconds(fresh ? cfg_.header_timeout_sec : cfg_.idle_timeout_sec));
            {
                auto [ec, n] = co_await io_op(
                    [&](auto cb) { bhttp::async_read_header(stream, buffer, parser, std::move(cb)); });
                (void)n;
                beast::get_lowest_layer(stream).expires_never();
                if (ec == beast::error::timeout)
                    driver::count_timeout(counters_, fresh ? driver::Phase::Header : driver::Phase::Idle);
                // A parser verdict (bad request line / header, header_limit) is a
                // malformed request (roadmap §5.3); a peer that closed mid-message
                // (end_of_stream / partial_message) or a transport error is not
                else if (ec && ec.category() == bhttp::make_error_code(bhttp::error::bad_method).category() &&
                         ec != bhttp::error::end_of_stream && ec != bhttp::error::partial_message)
                    counters_.parse_error();
                // eof / timeout / closed by shutdown
                if (ec) break;
            }
            sess->in_flight.store(true);

            auto& preq = parser.get();
            HttpRequest req;
            req.method = std::string(preq.method_string().data(), preq.method_string().size());
            driver::parse_target(std::string_view(preq.target().data(), preq.target().size()), req);
            for (auto& f : preq.base()) req.headers.add(std::string(f.name_string()), std::string(f.value()));
            {
                beast::error_code epc;
                auto ep = beast::get_lowest_layer(stream).socket().remote_endpoint(epc);
                if (!epc) req.remote_addr = ep.address().to_string();
            }
            req.tls_identity = tls_identity;

            // Message framing validation (drivers/common.h parse_body_framing):
            // beast's own parsing is more lenient about CL/TE conflicts etc.,
            // and this implementation decodes no transfer encoding other than
            // chunked — reject all of it at L1, so all four drivers
            // accept/reject the same request set
            if (!driver::parse_body_framing(req.headers).valid) {
                counters_.parse_error();
                auto bad = driver::bad_request_response("Invalid message framing.");
                co_await write_response(sess, stream, bad, /*head_request=*/false, /*keep=*/false);
                break;
            }
            counters_.request_parsed();

            BodyCtx<Stream> bctx{
                &parser, &stream, &buffer, cfg_.body_timeout_sec, &counters_, sess, cfg_.write_timeout_sec};
            if (auto e = req.headers.get("Expect"); e && HeaderMap::ieq(*e, "100-continue")) bctx.need_100 = true;
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
            // The handler may resume on a pool thread
            co_await ResumeOn{stream.get_executor()};

            if (stopping_.load() || !client_keep) keep = false;
            // The unconsumed body must be drained before reusing the
            // connection; if 100-continue was never sent, the client may
            // never send a body — do not wait blindly, just close
            if (!parser.is_done()) {
                if (bctx.need_100 || bctx.errored)
                    keep = false;
                else if (keep)
                    keep = co_await drain_body(bctx);
            }

            if (keep && driver::keepalive_budget_exhausted(served + 1, cfg_.max_requests_per_connection)) {
                keep = false;
                counters_.keepalive_closes.fetch_add(1, std::memory_order_relaxed);
            }
            bool ok = co_await write_response(sess, stream, resp, head_request, keep);
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
            wd_begin(ctx.sess, ctx.idle_timeout_sec, driver::Phase::Body, *ctx.counters);
            auto [ec, n] = co_await io_op(
                [&](auto cb) { bhttp::async_read(*ctx.stream, *ctx.buffer, *ctx.parser, std::move(cb)); });
            (void)n;
            wd_end(*ctx.sess);
            if (ec == bhttp::error::need_buffer) ec = {};
            if (ec) co_return false;
            drained += tmp.size() - body.size;
            // Too large; give up and close the connection
            if (drained > cfg_.drain_limit) co_return false;
        }
        co_return true;
    }

    template <class Stream>
    Task<bool> write_response(const std::shared_ptr<Session>& sess, Stream& stream, HttpResponse& resp,
                              bool head_request, bool keep) {
        bool no_body_status = resp.status == 204 || resp.status == 304 || resp.status < 200;
        // write_timeout per write op, enforced by the session watchdog (the
        // counter is bumped there)
        auto begin = [&] { wd_begin(sess, cfg_.write_timeout_sec, driver::Phase::Write, counters_); };
        auto end = [&] { wd_end(*sess); };

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
            driver::emit_headers(resp.headers, [&](const std::string& k, const std::string& v) { res.insert(k, v); });
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
                uint64_t len = resp.content_length.value_or(resp.stream_body && resp.stream_body->length()
                                                                ? *resp.stream_body->length()
                                                                : resp.small_body.size());
                res.set(bhttp::field::content_length, std::to_string(len));
                if (!head_request) res.body() = std::move(resp.small_body);
            }
            begin();
            auto [ec, n] = co_await io_op([&](auto cb) { bhttp::async_write(stream, res, std::move(cb)); });
            (void)n;
            end();
            co_return !ec;
        }

        // Streaming response: serializer + buffer_body, pulled in 64KiB chunks (docs/architecture/overview.md request
        // lifecycle)
        bhttp::response<bhttp::buffer_body> res;
        res.result(static_cast<unsigned>(resp.status));
        res.version(11);
        driver::emit_headers(resp.headers, [&](const std::string& k, const std::string& v) { res.insert(k, v); });
        if (!resp.headers.has("Date")) res.set(bhttp::field::date, util::http_date(std::chrono::system_clock::now()));
        res.keep_alive(keep);
        if (resp.content_length)
            res.content_length(*resp.content_length);
        else
            res.chunked(true);
        res.body().data = nullptr;
        res.body().more = true;

        bhttp::response_serializer<bhttp::buffer_body> sr{res};
        begin();
        {
            auto [ec, n] = co_await io_op([&](auto cb) { bhttp::async_write_header(stream, sr, std::move(cb)); });
            (void)n;
            end();
            if (ec) co_return false;
        }

        // One backend read in flight while the previous chunk is on the wire
        // (roadmap §4.3 ①/②: pooled buffers, no per-response zeroed vector)
        driver::StreamPrefetch pf(*resp.stream_body, cfg_.io_chunk_size);
        uint64_t written = 0;
        for (;;) {
            std::span<const std::byte> chunk;
            try {
                chunk = co_await pf.next();
            } catch (const std::exception& e) {
                LOG_ERROR("stream body read failed mid-response: {}", e.what());
                // Response head already sent; can only disconnect (contract 3: discard the result)
                co_return false;
            }
            co_await ResumeOn{stream.get_executor()};
            size_t n = chunk.size();
            // Byte accounting for fixed-length responses (consistent with the
            // other three drivers): writing too much breaks message framing;
            // writing too little must not stay keep-alive — the client would
            // read the next response's status line as the rest of this body
            if (resp.content_length && written + n > *resp.content_length) {
                LOG_ERROR("stream body overruns declared Content-Length ({} + {} > {})", written, n,
                          *resp.content_length);
                co_return false;
            }
            if (n == 0 && resp.content_length && written != *resp.content_length) {
                LOG_ERROR("stream body short of declared Content-Length ({} != {})", written, *resp.content_length);
                co_return false;
            }
            written += n;
            if (n == 0) {
                res.body().data = nullptr;
                res.body().more = false;
            } else {
                // buffer_body wants void*; beast only reads
                res.body().data = const_cast<std::byte*>(chunk.data());
                res.body().size = n;
                res.body().more = true;
            }
            begin();
            auto [ec, wrote] = co_await io_op([&](auto cb) { bhttp::async_write(stream, sr, std::move(cb)); });
            (void)wrote;
            end();
            if (ec == bhttp::error::need_buffer) ec = {};
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
        // Idle keep-alive connections get cut directly
        for (auto& s : idle) close_session(s);
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
                // Last resort: stop waiting for stuck sessions
                if (!e2)
                    for (auto& io : io_) io->ioc.stop();
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
                // run() returns once every io_context drains
                for (auto& io : io_) io->work.reset();
            });
        });
    }

    HttpConfig cfg_;
    Handler handler_;
    // Declared before tls_ctx_: the context's cert callback points here
    std::shared_ptr<tls::Holder> tls_holder_;
    // Present means HTTPS (knobs applied at construction)
    std::optional<asio::ssl::context> tls_ctx_;
    // Data plane: io_[i] is run by io thread i only; connections are pinned at
    // accept (accept_loop). Declared before ctl_strand_, which lives on io_[0]
    std::vector<std::unique_ptr<IoThread>> io_;
    size_t next_io_ = 0;
    // Control-plane strand on io_[0]: all operations on acceptor / stop_event /
    // grace_timer / force_timer serialize here (the data plane needs no strand:
    // a connection's io_context has exactly one thread)
    asio::strand<asio::io_context::executor_type> ctl_strand_;
    std::optional<tcp::acceptor> acceptor_;
    std::optional<asio::posix::stream_descriptor> stop_event_;
    std::optional<asio::steady_timer> grace_timer_;
    std::optional<asio::steady_timer> force_timer_;
    int event_fd_ = -1;
    uint64_t stop_buf_ = 0;
    uint16_t port_ = 0;
    std::atomic<bool> stopping_{false};
    std::mutex m_;
    // on_stop_signal has run (guarded by m_)
    bool stop_handled_ = false;
    std::set<std::shared_ptr<Session>> sessions_;
    // IHttpServer::stats() (roadmap §4.2)
    driver::ConnCounters counters_;
    std::once_flag finish_once_;
};

}  // namespace

void register_beast_driver() {
    HttpServerFactory::register_driver("beast",
                                       [](const HttpConfig& cfg) { return std::make_unique<BeastServer>(cfg); });
}

}  // namespace lights3::http
