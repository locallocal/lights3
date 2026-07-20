// L1: Boost.Beast 驱动 —— 异步模型（docs/http-adapter.md §3.1）。
// N 个线程共跑一个 io_context；每连接一个会话协程（每连接一个 strand）。
// 会话协程直接使用项目自己的 Task<void>：asio 异步操作经 awaiter 适配挂起/恢复，
// 与 docs/concurrency.md §4.1 的衔接点语义一致（handler 的续体回到连接 strand 上运行），
// 只是无需在 asio::awaitable 与 Task 两套协程类型之间转换。
#include <sys/eventfd.h>
#include <unistd.h>

#include <boost/asio/io_context.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/posix/stream_descriptor.hpp>
#include <boost/asio/post.hpp>
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
#include "http/server.h"

namespace lights3::http {

namespace {

namespace asio = boost::asio;
namespace beast = boost::beast;
namespace bhttp = boost::beast::http;  // 避免与 lights3::http 同名
using tcp = asio::ip::tcp;

// 把 (error_code, size_t) 形式的 asio 异步操作适配为 awaiter；
// 完成回调在发起操作的 I/O 对象 executor（连接 strand）上运行并 resume 协程
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

// 把续体投递回指定 executor：handler/stream_body 可能在池线程 resume，
// 发起下一个 socket 操作前必须切回连接 strand
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
        // 每连接一个 strand：该 socket 的所有完成回调都在 strand 上串行执行
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

// 脱缰启动 Task<void>：驱动在自己的执行环境里把 handler 协程跑到完成的入口
struct Detached {
    struct promise_type {
        Detached get_return_object() { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }  // spawn_detached 已兜底
    };
};

template <class Done>
Detached spawn_detached(Task<void> t, Done done) {
    try {
        co_await std::move(t);
    } catch (const std::exception& e) {
        LOG_ERROR("beast session escaped exception: {}", e.what());
    }
    done();
}

struct Session {
    beast::tcp_stream stream;
    std::atomic<bool> in_flight{false};

    explicit Session(tcp::socket&& s) : stream(std::move(s)) {}
};

// 每请求的 body 读取状态；归属会话协程帧（handler 内的 reader 销毁后仍要 drain）
struct BodyCtx {
    bhttp::request_parser<bhttp::buffer_body>* parser;
    beast::tcp_stream* stream;
    beast::flat_buffer* buffer;
    int idle_timeout_sec;
    bool need_100 = false;  // Expect: 100-continue 尚未答复，首次读 body 时才回
    bool errored = false;
};

class BeastBodyReader final : public BodyReader {
public:
    BeastBodyReader(BodyCtx* ctx, std::optional<uint64_t> len) : ctx_(ctx), len_(len) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        co_await ResumeOn{ctx_->stream->get_executor()};
        if (ctx_->errored) throw std::runtime_error("http body: read after connection error");
        // 延迟 100-continue：handler 决定要 body 了才叫客户端发（docs/http-adapter.md §3.1）
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
        ctx_->stream->expires_after(std::chrono::seconds(ctx_->idle_timeout_sec));
        auto [ec, n] = co_await io_op([&](auto cb) {
            bhttp::async_read(*ctx_->stream, *ctx_->buffer, *ctx_->parser, std::move(cb));
        });
        (void)n;
        ctx_->stream->expires_never();
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

    BodyCtx* ctx_;
    std::optional<uint64_t> len_;
};

class BeastServer final : public IHttpServer {
public:
    explicit BeastServer(const HttpConfig& cfg) : cfg_(cfg) {}

    ~BeastServer() override {
        if (event_fd_ >= 0 && !stop_event_) ::close(event_fd_);
    }

    void set_handler(Handler h) override { handler_ = std::move(h); }

    void listen(const std::string& addr, uint16_t port) override {
        beast::error_code ec;
        auto address = asio::ip::make_address(addr, ec);
        if (ec) throw std::runtime_error("bad bind address: " + addr);
        acceptor_.emplace(ioc_);
        tcp::endpoint ep{address, port};
        acceptor_->open(ep.protocol());
        acceptor_->set_option(asio::socket_base::reuse_address(true));
        acceptor_->bind(ep);
        acceptor_->listen(asio::socket_base::max_listen_connections);
        port_ = acceptor_->local_endpoint().port();

        // shutdown() 只写 eventfd（async-signal-safe），停机编排全部在 io 线程内完成
        event_fd_ = ::eventfd(0, EFD_CLOEXEC);
        if (event_fd_ < 0) throw std::runtime_error("eventfd() failed");
        stop_event_.emplace(ioc_, event_fd_);
        stop_event_->async_read_some(asio::buffer(&stop_buf_, sizeof(stop_buf_)),
                                     [this](beast::error_code e, size_t) {
                                         if (!e) on_stop_signal();
                                     });

        work_.emplace(asio::make_work_guard(ioc_));
        spawn_detached(accept_loop(), [] {});
        LOG_INFO("beast http server listening on {}:{}", addr, port_);
    }

    uint16_t bound_port() const override { return port_; }

    void run() override {
        int n = std::max(1, cfg_.io_threads);
        std::vector<std::thread> threads;
        threads.reserve(n - 1);
        for (int i = 1; i < n; ++i) threads.emplace_back([this] { ioc_.run(); });
        ioc_.run();
        for (auto& t : threads) t.join();
        LOG_INFO("beast http server stopped");
    }

    // 仅做 async-signal-safe 操作，可在信号处理器中调用。
    // exchange 保证只写一次 eventfd：finish() 之后 fd 号可能已被复用，不能再写
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
                LOG_WARN("accept failed: {}", ec.message());
                continue;
            }
            auto sess = std::make_shared<Session>(std::move(sock));
            {
                std::lock_guard lk(m_);
                if (stopping_.load()) break;
                sessions_.insert(sess);
            }
            spawn_detached(session_run(sess), [this, sess] { on_session_done(sess); });
        }
    }

    Task<void> session_run(std::shared_ptr<Session> sess) {
        auto& stream = sess->stream;
        beast::get_lowest_layer(stream).socket().set_option(tcp::no_delay(true));
        beast::flat_buffer buffer;  // 跨 keep-alive 请求保留（parser 可能超读）
        bool keep = true;

        while (keep && !stopping_.load()) {
            bhttp::request_parser<bhttp::buffer_body> parser;
            parser.header_limit(static_cast<uint32_t>(cfg_.max_header_size));
            parser.body_limit(boost::none);  // 大小限制是 L2 的职责
            stream.expires_after(std::chrono::seconds(cfg_.idle_timeout_sec));
            {
                auto [ec, n] = co_await io_op([&](auto cb) {
                    bhttp::async_read_header(stream, buffer, parser, std::move(cb));
                });
                (void)n;
                stream.expires_never();
                if (ec) break;  // eof / 超时 / shutdown 关闭
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

            BodyCtx bctx{&parser, &stream, &buffer, cfg_.idle_timeout_sec};
            if (auto e = req.headers.get("Expect"); e && HeaderMap::ieq(*e, "100-continue"))
                bctx.need_100 = true;
            std::optional<uint64_t> content_length;
            if (auto l = parser.content_length()) content_length = *l;
            if (!parser.is_done() || content_length)
                req.body = std::make_unique<BeastBodyReader>(&bctx, content_length);

            bool head_request = req.method == "HEAD";
            bool client_keep = preq.keep_alive();
            HttpResponse resp;
            try {
                resp = co_await handler_(std::move(req));
            } catch (const std::exception& e) {
                // L2 会兜底一切异常，到这里说明 L2 之外出了问题（契约 2）
                LOG_ERROR("handler escaped exception: {}", e.what());
                resp = driver::internal_error_response();
                keep = false;
            }
            co_await ResumeOn{stream.get_executor()};  // handler 可能在池线程 resume

            if (stopping_.load() || !client_keep) keep = false;
            // 复用连接前必须排空未消费的 body；从未回过 100-continue 则客户端
            // 可能根本不会发 body，不能傻等，直接关连接
            if (!parser.is_done()) {
                if (bctx.need_100 || bctx.errored) keep = false;
                else if (keep) keep = co_await drain_body(bctx);
            }

            bool ok = co_await write_response(stream, resp, head_request, keep);
            sess->in_flight.store(false);
            if (!ok) co_return;
        }
        beast::error_code ig;
        beast::get_lowest_layer(stream).socket().shutdown(tcp::socket::shutdown_both, ig);
    }

    Task<bool> drain_body(BodyCtx& ctx) {
        std::vector<std::byte> tmp(16 * 1024);
        uint64_t drained = 0;
        while (!ctx.parser->is_done()) {
            auto& body = ctx.parser->get().body();
            body.data = tmp.data();
            body.size = tmp.size();
            ctx.stream->expires_after(std::chrono::seconds(ctx.idle_timeout_sec));
            auto [ec, n] = co_await io_op([&](auto cb) {
                bhttp::async_read(*ctx.stream, *ctx.buffer, *ctx.parser, std::move(cb));
            });
            (void)n;
            ctx.stream->expires_never();
            if (ec == bhttp::error::need_buffer) ec = {};
            if (ec) co_return false;
            drained += tmp.size() - body.size;
            if (drained > 4 * 1024 * 1024) co_return false;  // 过大放弃，关连接
        }
        co_return true;
    }

    Task<bool> write_response(beast::tcp_stream& stream, HttpResponse& resp, bool head_request,
                              bool keep) {
        bool no_body_status = resp.status == 204 || resp.status == 304 || resp.status < 200;
        auto idle = std::chrono::seconds(cfg_.idle_timeout_sec);

        // 小响应 / HEAD / 无 body 状态码：整消息一次写出
        if (!resp.stream_body || head_request || no_body_status) {
            bhttp::response<bhttp::string_body> res;
            res.result(static_cast<unsigned>(resp.status));
            res.version(11);
            for (auto& [k, v] : resp.headers.items()) res.insert(k, v);
            if (!resp.headers.has("Date"))
                res.set(bhttp::field::date, util::http_date(std::chrono::system_clock::now()));
            res.keep_alive(keep);
            if (!no_body_status) {
                uint64_t len = resp.content_length.value_or(
                    resp.stream_body && resp.stream_body->length()
                        ? *resp.stream_body->length()
                        : resp.small_body.size());
                res.set(bhttp::field::content_length, std::to_string(len));
                if (!head_request) res.body() = std::move(resp.small_body);
            }
            stream.expires_after(idle);
            auto [ec, n] = co_await io_op([&](auto cb) {
                bhttp::async_write(stream, res, std::move(cb));
            });
            (void)n;
            stream.expires_never();
            co_return !ec;
        }

        // 流式响应：serializer + buffer_body，64KiB 块拉取（docs/architecture.md 请求生命周期）
        bhttp::response<bhttp::buffer_body> res;
        res.result(static_cast<unsigned>(resp.status));
        res.version(11);
        for (auto& [k, v] : resp.headers.items()) res.insert(k, v);
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
        stream.expires_after(idle);
        {
            auto [ec, n] = co_await io_op([&](auto cb) {
                bhttp::async_write_header(stream, sr, std::move(cb));
            });
            (void)n;
            stream.expires_never();
            if (ec) co_return false;
        }

        std::vector<std::byte> buf(64 * 1024);
        for (;;) {
            size_t n = 0;
            try {
                n = co_await resp.stream_body->read(std::span(buf));
            } catch (const std::exception& e) {
                LOG_ERROR("stream body read failed mid-response: {}", e.what());
                co_return false;  // 响应头已发出，只能断连（契约 3：丢弃结果）
            }
            co_await ResumeOn{stream.get_executor()};
            if (n == 0) {
                res.body().data = nullptr;
                res.body().more = false;
            } else {
                res.body().data = buf.data();
                res.body().size = n;
                res.body().more = true;
            }
            stream.expires_after(idle);
            auto [ec, wrote] = co_await io_op([&](auto cb) {
                bhttp::async_write(stream, sr, std::move(cb));
            });
            (void)wrote;
            stream.expires_never();
            if (ec == bhttp::error::need_buffer) ec = {};
            if (ec) co_return false;
            if (n == 0) break;
        }
        co_return true;
    }

    // ---- 优雅停机编排（契约 4：run() 在在途请求完成或超时后返回）----

    void on_stop_signal() {
        beast::error_code ig;
        acceptor_->close(ig);
        std::vector<std::shared_ptr<Session>> idle;
        bool empty;
        {
            std::lock_guard lk(m_);
            // 此前结束的会话不会触发 finish()（见 on_session_done），
            // 保证 acceptor 一定先于 finish() 关闭，否则挂起的 async_accept
            // 会让 io_context 永远有工作，run() 无法返回
            stop_handled_ = true;
            for (auto& s : sessions_)
                if (!s->in_flight.load()) idle.push_back(s);
            empty = sessions_.empty();
        }
        for (auto& s : idle) close_session(s);  // 空闲 keep-alive 连接直接掐掉
        if (empty) {
            finish();
            return;
        }
        grace_timer_.emplace(ioc_, std::chrono::seconds(10));
        grace_timer_->async_wait([this](beast::error_code e) {
            if (e) return;
            std::vector<std::shared_ptr<Session>> rest;
            {
                std::lock_guard lk(m_);
                rest.assign(sessions_.begin(), sessions_.end());
            }
            LOG_WARN("forcing {} connection(s) closed on shutdown", rest.size());
            for (auto& s : rest) close_session(s);
            force_timer_.emplace(ioc_, std::chrono::seconds(5));
            force_timer_->async_wait([this](beast::error_code e2) {
                if (!e2) ioc_.stop();  // 最后兜底：卡死的会话不再等
            });
        });
    }

    void close_session(const std::shared_ptr<Session>& s) {
        // socket 非线程安全：关闭动作投递到该连接自己的 strand 上执行
        asio::post(s->stream.get_executor(), [s] { s->stream.close(); });
    }

    void on_session_done(const std::shared_ptr<Session>& sess) {
        bool finish_now;
        {
            std::lock_guard lk(m_);
            sessions_.erase(sess);
            // stop_handled_ 之前不 finish：shutdown() 刚置位 stopping_ 而
            // eventfd 事件尚未处理时，最后一个会话结束不能抢跑（否则 finish
            // 会关掉 stop eventfd，on_stop_signal 被跳过，acceptor 永不关闭）
            finish_now = stop_handled_ && sessions_.empty();
        }
        if (finish_now) finish();
    }

    void finish() {
        std::call_once(finish_once_, [this] {
            asio::post(ioc_, [this] {
                if (grace_timer_) grace_timer_->cancel();
                if (force_timer_) force_timer_->cancel();
                beast::error_code ig;
                if (stop_event_) stop_event_->close(ig);
                work_.reset();  // io_context 排空后 run() 返回
            });
        });
    }

    HttpConfig cfg_;
    Handler handler_;
    asio::io_context ioc_;
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
    bool stop_handled_ = false;  // on_stop_signal 已执行（guarded by m_）
    std::set<std::shared_ptr<Session>> sessions_;
    std::once_flag finish_once_;
};

}  // namespace

void register_beast_driver() {
    HttpServerFactory::register_driver("beast", [](const HttpConfig& cfg) {
        return std::make_unique<BeastServer>(cfg);
    });
}

}  // namespace lights3::http
