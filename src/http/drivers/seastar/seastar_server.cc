// L1: seastar 驱动 —— shard-per-core 异步模型（docs/http-adapter.md §3.4）。
//
// 与其他驱动的关键差异：seastar reactor 每进程只能启动一次，因此引擎是进程级
// 单例（首次 listen() 拉起，atexit 收尾），每个 SeastarServer 实例只管理自己的
// listener 与连接。单测里驱动会被反复建/销，全部复用同一 reactor。
//
// 会话协程直接使用项目自己的 Task<void>（同 beast 驱动的做法）：
//  - seastar::future 经 FutAwaiter 适配挂起/恢复，resume 发生在本 shard；
//  - handler/stream_body 可能在池线程 resume，发起下一个 socket 操作前必须
//    经 ResumeOnShard 切回本 shard（跨线程投递走 seastar::alien）。
//
// 构建要求 SEASTAR_DEFAULT_ALLOCATOR（见根 CMakeLists）：进程里 reactor 之外
// 还有自有线程池，统一用系统分配器绕开 seastar allocator 的线程归属约束。
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

// ---------- 进程级引擎单例 ----------

class SeastarEngine {
public:
    static SeastarEngine& instance() {
        static SeastarEngine e;
        return e;
    }

    // 首次调用拉起 reactor（smp = io_threads）；之后复用，smp 不可再改
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
            fut.get();  // 启动失败（依赖缺失、smp 超核数等）以异常抛给调用方
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
    // reactor 主协程（shard0）：登记 alien 入口、通知启动完成，然后睡在
    // eventfd 上直到 stop()。成员协程 + 值参数：帧不引用任何短命 lambda 对象
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
        cfg.auto_handle_sigint_sigterm = false;  // 信号处理归 main 管
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
        // app.run 正常返回但从未进入主函数（如参数错误）：把失败带回 ensure_started
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

// ---------- Task<T> 与 seastar 的衔接 ----------

// 在 lights3 协程里 co_await 一个 seastar::future；resume 在本 shard 的
// reactor 上下文中发生（then_wrapped 的续体），异常经 get() 原样重抛
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
        // future<> 的 get() 返回内部占位类型而非 void，不能直接 return
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

// 把续体投递回指定 shard：handler/stream_body 可能在池线程 resume，
// 发起下一个 socket 操作前必须切回连接所在 shard（对应 beast 的 ResumeOn）
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

// 脱缰启动 Task<void>：与 beast 驱动同款（session 协程的驱动入口）
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
        LOG_ERROR("seastar session escaped exception: {}", e.what());
    }
    done();
}

// ---------- 连接与会话 ----------

// 带缓冲的连接读取器 + 写出口；请求头解析与 body 读取共用。
// 所有方法只能在连接所在 shard 上调用。
struct SeaConn {
    ss::connected_socket cs;
    ss::input_stream<char> in;
    ss::output_stream<char> out;
    ss::temporary_buffer<char> buf;
    size_t pos = 0;

    explicit SeaConn(ss::connected_socket s)
        : cs(std::move(s)), in(cs.input()), out(cs.output()) {}

    // 保证缓冲非空；EOF 返回 false
    Task<bool> fill() {
        while (pos == buf.size()) {
            buf = co_await fut_await(in.read());
            pos = 0;
            if (buf.empty()) co_return false;
        }
        co_return true;
    }

    // 读一行（去掉 \r\n）；EOF/超限返回 false
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

    Task<void> write(const char* p, size_t n) { co_await fut_await(out.write(p, n)); }
    Task<void> flush() { co_await fut_await(out.flush()); }
};

// body 读取状态归属会话协程帧（handler 内的 reader 销毁后，连接仍需 drain）。
// 契约（docs/http-adapter.md §4）：正常 EOF 返回 0；客户端断连/坏 chunked 以异常传播。
struct BodyState {
    SeaConn* conn = nullptr;
    unsigned shard = 0;
    bool need_continue = false;  // Expect: 100-continue 尚未答复，首次读时才回
    bool chunked = false;
    uint64_t remaining = 0;
    uint64_t chunk_left = 0;
    bool chunk_eof = false;
    bool error = false;

    [[noreturn]] void fail(const char* what) {
        error = true;
        throw std::runtime_error(std::string("http body: ") + what);
    }

    Task<size_t> read_some(std::byte* dst, size_t want) {
        co_await ResumeOnShard{shard};  // 消费方可能在池线程 resume
        if (error) fail("read after connection error");
        // 延迟 100-continue：handler 决定要 body 了才叫客户端发（docs/http-adapter.md §3.1）
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
            if (!co_await conn->read_line(line, 1024)) fail("client disconnected mid-body");
            if (line.empty()) continue;  // chunk 数据后的 CRLF
            size_t sz = 0;
            try {
                sz = std::stoull(line, nullptr, 16);
            } catch (...) {
                fail("malformed chunk size");
            }
            if (sz == 0) {  // 末 chunk：吃掉 trailer 直到空行
                std::string t;
                while (co_await conn->read_line(t, 1024) && !t.empty()) {}
                chunk_eof = true;
                co_return 0;
            }
            chunk_left = sz;
        }
        size_t n = co_await conn->read_some(dst, std::min<uint64_t>(want, chunk_left));
        if (n == 0) fail("client disconnected mid-body");
        chunk_left -= n;
        co_return n;
    }

    bool at_eof() const { return error || (chunked ? chunk_eof : remaining == 0); }

    // 响应前排空残余 body 以复用连接；过大/出错放弃（调用方随即关连接）
    Task<bool> drain(uint64_t limit = 4 * 1024 * 1024) {
        // 从未回过 100-continue，客户端可能根本不会发 body，不能傻等
        if (need_continue) co_return false;
        std::vector<std::byte> tmp(16 * 1024);
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
    bool in_flight = false;  // 仅在本 shard 上读写

    explicit Session(ss::connected_socket s) : conn(std::move(s)) {}
};

// 每 shard 一份；除构造外只在所属 shard 上触碰
struct ShardState {
    std::optional<ss::server_socket> listener;
    std::set<std::shared_ptr<Session>> sessions;
    bool stopping = false;
};

// 跨线程共享的服务器核心：shards[i] 的内容仅由 shard i 触碰
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
                          unsigned shard) {
    bool no_body_status = resp.status == 204 || resp.status == 304 || resp.status < 200;
    auto head = driver::render_response_head(resp, keep);
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
        co_return false;  // 对端断连等写失败：关连接
    }

    // 流式响应：64KiB 块拉取（docs/architecture.md 请求生命周期）
    std::vector<std::byte> buf(64 * 1024);
    for (;;) {
        size_t n = 0;
        try {
            n = co_await resp.stream_body->read(std::span(buf));
        } catch (const std::exception& e) {
            LOG_ERROR("stream body read failed mid-response: {}", e.what());
            co_return false;  // 响应头已发出，只能断连（契约 3：丢弃结果）
        }
        co_await ResumeOnShard{shard};
        try {
            if (n == 0) {
                if (head.chunked) co_await conn.write("0\r\n\r\n", 5);
                co_await conn.flush();
                co_return true;
            }
            if (head.chunked) {
                char sz[32];
                int m = snprintf(sz, sizeof(sz), "%zx\r\n", n);
                co_await conn.write(sz, static_cast<size_t>(m));
            }
            co_await conn.write(reinterpret_cast<const char*>(buf.data()), n);
            if (head.chunked) co_await conn.write("\r\n", 2);
        } catch (...) {
            co_return false;
        }
    }
}

Task<void> session_run(std::shared_ptr<ServerCore> core, std::shared_ptr<Session> sess,
                       std::string peer, unsigned shard) {
    auto& conn = sess->conn;
    conn.cs.set_nodelay(true);
    // keep-alive 空闲超时：到点关读端，挂起的读醒来见 EOF，会话自然收尾
    ss::timer<> idle_timer([sess] { sess->conn.cs.shutdown_input(); });
    bool keep = true;

    // 对端 RST 等 socket 错误从 seastar future 以异常浮出：兜住后统一走关流收尾
    try {
    while (keep && !core->stopping.load(std::memory_order_relaxed)) {
        const size_t max_line = core->cfg.max_header_size;
        std::string line;
        idle_timer.arm(std::chrono::seconds(core->cfg.idle_timeout_sec));
        bool got = co_await conn.read_line(line, max_line);
        idle_timer.cancel();
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

        // 头部
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
            auto colon = line.find(':');
            if (colon == std::string::npos) {
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

        if (auto c = req.headers.get("Connection")) {
            if (HeaderMap::ieq(*c, "close")) keep = false;
            else if (HeaderMap::ieq(*c, "keep-alive")) keep = true;
        }

        // body
        BodyState bstate;
        bstate.conn = &conn;
        bstate.shard = shard;
        std::optional<uint64_t> content_length;
        bool has_body = false;
        if (auto te = req.headers.get("Transfer-Encoding");
            te && HeaderMap::ieq(*te, "chunked")) {
            bstate.chunked = true;
            has_body = true;
        } else if (auto cl = req.headers.get("Content-Length")) {
            try {
                content_length = std::stoull(*cl);
            } catch (...) {
                break;
            }
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
            // L2 会兜底一切异常，到这里说明 L2 之外出了问题（契约 2）
            LOG_ERROR("handler escaped exception: {}", e.what());
            resp = driver::internal_error_response();
            keep = false;
        }
        co_await ResumeOnShard{shard};  // handler 可能在池线程 resume

        if (core->stopping.load(std::memory_order_relaxed)) keep = false;
        // 复用连接前必须排空未消费的 body；从未回过 100-continue 则客户端
        // 可能根本不会发 body，不能傻等，直接关连接
        if (!bstate.at_eof()) {
            if (bstate.need_continue || bstate.error) keep = false;
            else if (keep) keep = co_await bstate.drain();
        }

        bool ok = co_await write_response(conn, resp, head_request, keep, shard);
        sess->in_flight = false;
        if (!ok) break;
    }
    } catch (const std::exception& e) {
        LOG_DEBUG("seastar session ended with error: {}", e.what());
    }
    idle_timer.cancel();
    sess->in_flight = false;
    // output_stream 必须显式 close（flush + 释放），失败（对端已断）忽略
    try {
        co_await fut_await(conn.out.close());
    } catch (...) {}
    try {
        co_await fut_await(conn.in.close());
    } catch (...) {}
}

// accept 循环：每 shard 一个，abort_accept() 使 accept() 以异常返回而退出
ss::future<> accept_loop(std::shared_ptr<ServerCore> core, std::shared_ptr<ShardState> st,
                         unsigned shard) {
    while (!st->stopping) {
        std::optional<ss::accept_result> ar;
        try {
            ar.emplace(co_await st->listener->accept());
        } catch (...) {
            break;
        }
        if (st->stopping) break;  // 竞态窗口内接入的连接直接丢弃（析构即关闭）
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

// 等待全部会话结束，最多 grace；返回残余会话数
ss::future<size_t> wait_drained(std::shared_ptr<ServerCore> core, std::chrono::seconds grace) {
    auto deadline = std::chrono::steady_clock::now() + grace;
    for (;;) {
        size_t n = co_await count_sessions(core);
        if (n == 0 || std::chrono::steady_clock::now() >= deadline) co_return n;
        co_await ss::sleep(std::chrono::milliseconds(100));
    }
}

// 优雅停机编排（契约 4）：停 accept → 掐空闲连接 → 等在途请求（10s 宽限）
// → 强制断开 → 再等 5s → 无论如何让 run() 返回
ss::future<> stop_watcher(std::shared_ptr<ServerCore> core, ss::readable_eventfd evfd) {
    co_await evfd.wait();
    co_await shutdown_sessions(core, /*idle_only=*/true);

    size_t left = co_await wait_drained(core, std::chrono::seconds(10));
    if (left > 0) {
        LOG_WARN("forcing {} connection(s) closed on shutdown", left);
        co_await shutdown_sessions(core, /*idle_only=*/false);
        left = co_await wait_drained(core, std::chrono::seconds(5));
        if (left > 0) LOG_WARN("{} connection(s) still alive after force close", left);
    }
    core->notify_stopped();
}

// 在 shard0 上装配整台服务器：各 shard 建 listener + accept 循环，再挂
// 停机 watcher；返回停机 eventfd 的写端 fd。
// 自由协程函数 + 值参数：帧不依赖调用方的 lambda 对象存活（协程 lambda 的
// 捕获在 lambda 对象销毁后即悬空，alien/smp 的投递闭包都活不过首次挂起）
ss::future<int> setup_server(std::shared_ptr<ServerCore> core, std::string addr, uint16_t p) {
    for (unsigned s = 0; s < ss::smp::count; ++s) {
        co_await ss::smp::submit_to(s, [core, &addr, p, s] {
            auto st = std::make_shared<ShardState>();
            ss::listen_options lo;
            lo.reuse_address = true;
            st->listener = ss::engine().listen(ss::socket_address(ss::ipv4_addr(addr, p)), lo);
            core->shards[s] = st;
            (void)accept_loop(core, st, s).handle_exception([](std::exception_ptr) {});
        });
    }
    ss::readable_eventfd evfd;
    int wfd = evfd.get_write_fd();
    (void)stop_watcher(core, std::move(evfd)).handle_exception([core](std::exception_ptr) {
        LOG_ERROR("seastar stop watcher failed unexpectedly");
        core->notify_stopped();  // 无论如何不能让 run() 卡死
    });
    co_return wfd;
}

// port=0 时先用一次性 socket 解析实际端口：posix 栈的跨 shard 连接分发按
// listen 时传入的地址配对，各 shard 必须用同一个具体端口号监听
uint16_t probe_free_port(const std::string& addr) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) throw std::runtime_error("socket() failed");
    sockaddr_in sa{};
    sa.sin_family = AF_INET;
    if (inet_pton(AF_INET, addr.c_str(), &sa.sin_addr) != 1) {
        ::close(fd);
        throw std::runtime_error("bad bind address: " + addr);
    }
    if (::bind(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0) {
        ::close(fd);
        throw std::runtime_error("bind failed: " + std::string(strerror(errno)));
    }
    sockaddr_in bound{};
    socklen_t blen = sizeof(bound);
    getsockname(fd, reinterpret_cast<sockaddr*>(&bound), &blen);
    ::close(fd);
    return ntohs(bound.sin_port);
}

class SeastarServer final : public IHttpServer {
public:
    explicit SeastarServer(const HttpConfig& cfg) : cfg_(cfg) {}

    ~SeastarServer() override {
        // 引擎常驻，但本实例的 fiber 引用着 core_：未停净不能析构
        if (core_ && !core_->is_stopped()) {
            shutdown();
            core_->wait_stopped();
        }
    }

    void set_handler(Handler h) override { handler_ = std::move(h); }

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
        LOG_INFO("seastar http server listening on {}:{} (smp={})", addr, port_, eng.shards());
    }

    uint16_t bound_port() const override { return port_; }

    void run() override {
        core_->wait_stopped();
        LOG_INFO("seastar http server stopped");
    }

    // 仅做 async-signal-safe 操作，可在信号处理器中调用。
    // exchange 保证只写一次 eventfd：停净后 fd 已随 watcher 销毁，不能再写
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
