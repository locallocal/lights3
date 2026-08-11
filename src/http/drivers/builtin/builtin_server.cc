// L1: builtin 驱动 —— 零依赖 POSIX socket HTTP/1.1，thread-per-connection 同步模型。
// 演示插拔层的同步驱动接入方式（协程经 sync_wait 桥接，见 docs/http-adapter.md §3.2、docs/concurrency.md §4.2）。
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
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

namespace lights3::http {

namespace {

bool send_all(int fd, const char* data, size_t len) {
    while (len > 0) {
        ssize_t n = ::send(fd, data, len, MSG_NOSIGNAL);
        if (n <= 0) return false;
        data += n;
        len -= static_cast<size_t>(n);
    }
    return true;
}

// 带缓冲的连接读取器；请求头解析与 body 读取共用。
// buf 不做零初始化（docs/gaps.md §4）：pos/end 界定有效区，每连接 memset 16KiB
// 纯属浪费
struct ConnReader {
    int fd = -1;
    char buf[driver::kScratchBytes];
    size_t pos = 0, end = 0;

    // 读一行（含 \n 之前的内容，去掉 \r\n）；失败/超限返回 false
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
            ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
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
        ssize_t n = ::recv(fd, dst, want, 0);
        return n > 0 ? static_cast<size_t>(n) : 0;
    }
};

// body 读取状态归属连接（handler 内的 reader 销毁后，连接仍需 drain 残余字节）
// 契约（docs/http-adapter.md §4）：正常 EOF 返回 0；客户端断连/坏 chunked 以异常传播。
struct BodyState {
    ConnReader* conn = nullptr;
    int fd = -1;
    bool need_continue = false;   // Expect: 100-continue 尚未答复，首次读时才回
    bool chunked = false;
    uint64_t remaining = 0;       // 定长模式：剩余字节
    uint64_t chunk_left = 0;      // chunked 模式：当前 chunk 剩余
    bool after_chunk_data = false;  // 刚读完一个 chunk 的数据，下一行必须是 CRLF
    bool chunk_eof = false;
    bool error = false;
    size_t trailer_max = 16 * 1024;  // 由 http.trailer_max_size 覆盖（docs/gaps.md §7）

    [[noreturn]] void fail(const char* what) {
        error = true;
        throw std::runtime_error(std::string("http body: ") + what);
    }

    size_t read_some(std::byte* dst, size_t want) {
        if (error) fail("read after connection error");
        // 延迟 100-continue：handler 决定要 body 了才叫客户端发（docs/http-adapter.md §3.1），
        // 认证失败等场景可以在不接收 body 的情况下直接拒绝
        if (need_continue) {
            need_continue = false;
            if (!send_all(fd, "HTTP/1.1 100 Continue\r\n\r\n", 25))
                fail("failed to send 100 Continue");
        }
        if (!chunked) {
            if (remaining == 0) return 0;
            size_t n = conn->read_some(dst, std::min<uint64_t>(want, remaining));
            if (n == 0) fail("client disconnected mid-body");
            remaining -= n;
            return n;
        }
        // chunked
        while (chunk_left == 0) {
            if (chunk_eof) return 0;
            std::string line;
            if (after_chunk_data) {
                // chunk 数据后必须紧跟一个 CRLF，且只允许一个：任意"看似 hex"
                // 的垃圾或多余空行都不能被当作下一个 chunk size 静默吞掉
                if (!conn->read_line(line, 2)) fail("client disconnected mid-body");
                if (!line.empty()) fail("missing CRLF after chunk data");
                after_chunk_data = false;
            }
            if (!conn->read_line(line, 1024)) fail("client disconnected mid-body");
            uint64_t sz = 0;
            if (!driver::parse_chunk_size(line, sz)) fail("malformed chunk size");
            if (sz == 0) {
                // 末 chunk：吃掉 trailer 直到空行。总量设上限防无限 trailer 灌注；
                // 读失败是 body 截断，必须报错而非当正常 EOF
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
        if (n == 0) fail("client disconnected mid-body");
        chunk_left -= n;
        if (chunk_left == 0) after_chunk_data = true;
        return n;
    }

    bool at_eof() const {
        return error || (chunked ? chunk_eof : remaining == 0);
    }
    // 响应后排空残余 body 以复用连接；过大/出错放弃（调用方随即关连接）
    bool drain(uint64_t limit) {
        // 从未回过 100-continue，客户端可能根本不会发 body，不能傻等
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
        // 阻塞 recv 切回连接自己的线程执行（docs/gaps.md §2.10）：handler 协程链
        // 跑在共享 ThreadPool 上，就地 recv 会把池线程堵在慢速客户端上——16 个
        // 慢速上传即可占死全部池线程；连接线程此刻正闲在 sync_wait_pumping 里
        co_await resume_on(*conn_exec_);
        co_return st_->read_some(buf.data(), buf.size());
    }
    std::optional<uint64_t> length() const override { return len_; }

private:
    BodyState* st_;
    std::optional<uint64_t> len_;
    PumpExecutor* conn_exec_;
};

// 连接线程共享的服务器状态：run() 可能在残余连接线程退出前返回（强杀等待超时），
// 线程经 shared_ptr 持有本结构，服务器对象析构后仍安全（否则析构期 UAF）
struct ConnShared {
    HttpConfig cfg;
    Handler handler;
    std::atomic<bool> stopping{false};
    std::mutex m;
    std::condition_variable cv;
    std::set<int> conns;
    // 正在 keep-alive 等待下一请求的连接（docs/gaps.md §4）：停机时这些可以
    // 立即掐断，宽限只留给在途请求——此前不区分，空闲连接也让停机干等 10 秒
    std::set<int> idle;
    int active = 0;
};

// 连接线程用 512KiB 栈显式创建（docs/gaps.md §4）：std::thread 走默认 8MiB，
// × max_connections(4096) = 32GiB 虚拟地址空间预留，而实测栈峰值 ~100KiB
//（协程帧在堆上，栈只承载解析与阻塞 IO 调用链）。detached：生命周期由闭包里的
// shared_ptr 管，与旧 std::thread(...).detach() 相同
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

bool write_response(int fd, HttpResponse& resp, bool head_request, bool keep_alive,
                    size_t io_chunk = driver::kIoChunkBytes) {
    bool no_body_status = resp.status == 204 || resp.status == 304 || resp.status < 200;
    auto head = driver::render_response_head(resp, keep_alive, head_request);
    bool chunked = head.chunked;
    if (!send_all(fd, head.text.data(), head.text.size())) return false;
    if (head_request || no_body_status) return true;

    if (!resp.stream_body) return send_all(fd, resp.small_body.data(), resp.small_body.size());

    // 流式响应：http.io_chunk_size 块拉取（docs/architecture.md 请求生命周期）。
    // 块大小是运行期配置，缓冲改在堆上（栈数组需编译期大小）
    std::vector<std::byte> buf(io_chunk);
    uint64_t written = 0;
    for (;;) {
        size_t n = 0;
        try {
            n = sync_wait(resp.stream_body->read(std::span(buf)));
        } catch (const std::exception& e) {
            LOG_ERROR("stream body read failed mid-response: {}", e.what());
            return false;  // 响应头已发出，只能断连
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
            if (!send_all(fd, sz, static_cast<size_t>(m))) return false;
        }
        if (!send_all(fd, reinterpret_cast<const char*>(buf.data()), n)) return false;
        if (chunked && !send_all(fd, "\r\n", 2)) return false;
        written += n;
    }
    if (chunked) return send_all(fd, "0\r\n\r\n", 5);
    // 定长响应写少了不能保持 keep-alive：客户端会把下个响应头当作本次 body 剩余
    if (resp.content_length && written != *resp.content_length) {
        LOG_ERROR("stream body short of declared Content-Length ({} != {})", written,
                  *resp.content_length);
        return false;
    }
    return true;
}

// 处理一个请求；返回 false 表示连接应关闭
bool serve_one(ConnShared& sh, int fd, ConnReader& reader, const std::string& peer,
               bool& keep_alive) {
    const size_t max_line = sh.cfg.max_header_size;

    // 请求行读到之前本连接是"空闲 keep-alive"：登记进 idle，停机扫荡直接掐；
    // 读到首字节即转在途（享受停机宽限）
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
    if (!got || line.empty()) return false;

    HttpRequest req;
    req.remote_addr = peer;
    {
        auto sp1 = line.find(' ');
        auto sp2 = line.rfind(' ');
        if (sp1 == std::string::npos || sp2 == sp1) return false;
        req.method = line.substr(0, sp1);
        std::string target = line.substr(sp1 + 1, sp2 - sp1 - 1);
        std::string version = line.substr(sp2 + 1);
        if (version == "HTTP/1.0") keep_alive = false;
        driver::parse_target(target, req);
    }

    // 头部
    size_t header_bytes = 0;
    for (;;) {
        if (!reader.read_line(line, max_line)) return false;
        if (line.empty()) break;
        header_bytes += line.size();
        if (header_bytes > sh.cfg.max_header_size) return false;
        // 裸 CR 不得留在头名/头值里（read_line 只剥行尾的单个 \r）
        if (line.find('\r') != std::string::npos) return false;
        auto colon = line.find(':');
        if (colon == std::string::npos || colon == 0) return false;
        std::string k = line.substr(0, colon);
        std::string v = line.substr(colon + 1);
        v.erase(0, v.find_first_not_of(" \t"));
        auto tail = v.find_last_not_of(" \t");
        if (tail != std::string::npos) v.erase(tail + 1);
        req.headers.add(std::move(k), std::move(v));
    }

    if (req.headers.has("Connection")) {
        // 列表头：Connection: close, Upgrade 是合法写法，全等比较会漏判 close
        if (req.headers.has_token("Connection", "close")) keep_alive = false;
        else if (req.headers.has_token("Connection", "keep-alive")) keep_alive = true;
    }

    // body 边界：CL/TE 冲突、重复 CL、非法值一律拒绝关连接（请求走私前置条件，
    // 见 drivers/common.h parse_body_framing）
    auto framing = driver::parse_body_framing(req.headers);
    if (!framing.valid) {
        auto bad = driver::bad_request_response("Invalid message framing.");
        write_response(fd, bad, req.method == "HEAD", /*keep_alive=*/false);
        return false;
    }
    BodyState body_state;
    body_state.conn = &reader;
    body_state.fd = fd;
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
        // pumping 变体：等待期间连接线程运行 conn_exec 队列，承接 body 的阻塞读
        resp = sync_wait_pumping(conn_exec, sh.handler(std::move(req)));
    } catch (const std::exception& e) {
        // L2 会兜底一切异常，到这里说明 L2 之外出了问题（契约 2：500 + InternalError XML）
        resp = driver::internal_error_response(e.what());
        keep_alive = false;
    }

    // 复用连接前必须排空未消费的 body。body 出过错则流已失步（残余字节会被
    // 当下一个请求解析），必须关连接；从未回过 100-continue 则客户端可能根本
    // 不会发 body，不能傻等，同样关连接
    if (body_state.error) keep_alive = false;
    else if (!body_state.at_eof()) {
        if (body_state.need_continue) keep_alive = false;
        else if (keep_alive) keep_alive = body_state.drain(sh.cfg.drain_limit);
    }

    if (!write_response(fd, resp, head_request, keep_alive, sh.cfg.io_chunk_size)) return false;
    return keep_alive;
}

void handle_connection(ConnShared& sh, int fd, const std::string& peer) {
    timeval tv{sh.cfg.idle_timeout_sec, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    ConnReader reader;
    reader.fd = fd;  // 逐字段赋值：聚合初始化会把未列出的 buf 值初始化（memset 16KiB）
    bool keep_alive = true;
    while (keep_alive && !sh.stopping.load()) {
        if (!serve_one(sh, fd, reader, peer, keep_alive)) break;
    }
}

class BuiltinServer final : public IHttpServer {
public:
    explicit BuiltinServer(const HttpConfig& cfg) : shared_(std::make_shared<ConnShared>()) {
        // TLS 不支持（docs/gaps.md §7）：配了必须当场报错——静默跑明文会让
        // UNSIGNED-PAYLOAD 请求的完整性论证（依赖传输层加密）整个失效
        if (!cfg.tls_cert.empty())
            throw std::runtime_error(
                "http driver 'builtin' does not support TLS; use 'httplib' or 'beast'");
        // thread-per-connection 模型没有 IO 线程数的概念；显式配置说明用户在
        // 预期一个不会发生的效果（docs/gaps.md §7）
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
        // IPv4/IPv6 双支持：此前写死 AF_INET + inet_pton(AF_INET)，配置里写
        // bind: "::" 会直接抛 "bad bind address"，而 beast/httplib 都能起
        // （同一份配置换驱动就起不来）
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
        // "::" 默认双栈（v6only=0），与 beast/httplib 的行为对齐
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
        LOG_INFO("builtin http server listening on {}:{}", addr, port_);
    }

    uint16_t bound_port() const override { return port_; }

    void run() override {
        auto& sh = *shared_;
        // shutdown() 可能先于 run() 甚至先于 listen() 到达：入循环前先看一眼，
        // 否则信号已被吞掉，accept 会永久阻塞
        while (!sh.stopping.load()) {
            sockaddr_storage peer{};
            socklen_t plen = sizeof(peer);
            int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &plen);
            if (fd < 0) {
                if (sh.stopping.load()) break;
                if (errno == EINTR || errno == ECONNABORTED) continue;
                if (errno == EMFILE || errno == ENFILE) {
                    // fd 耗尽是暂态（在途连接会释放），退避后继续而非停止 accept
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
                // 并发连接硬上限（cfg.max_connections，四驱动统一）：thread-per-
                // connection 模型无上限时每连接一线程可耗尽内存/线程数
                if (sh.active >= sh.cfg.max_connections) {
                    LOG_WARN("connection limit ({}) reached, rejecting {}",
                             sh.cfg.max_connections, ip);
                    ::close(fd);
                    continue;
                }
                ++sh.active;
                sh.conns.insert(fd);
            }
            bool spawned = spawn_conn_thread([sp = shared_, fd, peer_ip = std::string(ip)] {
                handle_connection(*sp, fd, peer_ip);
                std::lock_guard lk(sp->m);
                sp->conns.erase(fd);
                sp->idle.erase(fd);
                ::close(fd);
                if (--sp->active == 0) sp->cv.notify_all();
            });
            if (!spawned) {
                // 线程创建失败（资源耗尽）：回滚计数并拒绝该连接，不能让异常
                // 穿出 run() 终结进程
                LOG_ERROR("failed to spawn connection thread");
                std::lock_guard lk(sh.m);
                sh.conns.erase(fd);
                ::close(fd);
                if (--sh.active == 0) sh.cv.notify_all();
            }
        }
        // 优雅退出：空闲 keep-alive 连接立即掐断（它们只是在等下一个请求，
        // docs/gaps.md §4），宽限只留给在途请求；超时再强制断开全部。残余线程经
        // shared_ptr 持有共享状态，run() 返回乃至 server 析构后自行收尾，无悬空引用
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

    // 仅做 async-signal-safe 操作，可在信号处理器中调用
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
