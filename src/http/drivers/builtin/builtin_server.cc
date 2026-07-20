// L1: builtin 驱动 —— 零依赖 POSIX socket HTTP/1.1，thread-per-connection 同步模型。
// 演示插拔层的同步驱动接入方式（协程经 sync_wait 桥接，见 docs/http-adapter.md §3.2、docs/concurrency.md §4.2）。
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <unistd.h>

#include <atomic>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <set>
#include <thread>

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

// 带缓冲的连接读取器；请求头解析与 body 读取共用
struct ConnReader {
    int fd = -1;
    char buf[16 * 1024] = {};
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
    bool chunk_eof = false;
    bool error = false;

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
            if (!conn->read_line(line, 1024)) fail("client disconnected mid-body");
            if (line.empty()) continue;  // chunk 数据后的 CRLF
            size_t sz = 0;
            try {
                sz = std::stoull(line, nullptr, 16);
            } catch (...) {
                fail("malformed chunk size");
            }
            if (sz == 0) {  // 末 chunk：吃掉 trailer 直到空行
                std::string t;
                while (conn->read_line(t, 1024) && !t.empty()) {}
                chunk_eof = true;
                return 0;
            }
            chunk_left = sz;
        }
        size_t n = conn->read_some(dst, std::min<uint64_t>(want, chunk_left));
        if (n == 0) fail("client disconnected mid-body");
        chunk_left -= n;
        return n;
    }

    bool at_eof() const {
        return error || (chunked ? chunk_eof : remaining == 0);
    }
    // 响应后排空残余 body 以复用连接；过大/出错放弃（调用方随即关连接）
    bool drain(uint64_t limit = 4 * 1024 * 1024) {
        // 从未回过 100-continue，客户端可能根本不会发 body，不能傻等
        if (need_continue) return false;
        std::byte tmp[16 * 1024];
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
    SocketBodyReader(BodyState* st, std::optional<uint64_t> len) : st_(st), len_(len) {}
    Task<size_t> read(std::span<std::byte> buf) override {
        co_return st_->read_some(buf.data(), buf.size());
    }
    std::optional<uint64_t> length() const override { return len_; }

private:
    BodyState* st_;
    std::optional<uint64_t> len_;
};

class BuiltinServer final : public IHttpServer {
public:
    explicit BuiltinServer(const HttpConfig& cfg) : cfg_(cfg) {}
    ~BuiltinServer() override {
        if (listen_fd_ >= 0) ::close(listen_fd_);
    }

    void set_handler(Handler h) override { handler_ = std::move(h); }

    void listen(const std::string& addr, uint16_t port) override {
        listen_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listen_fd_ < 0) throw std::runtime_error("socket() failed");
        int one = 1;
        setsockopt(listen_fd_, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
        sockaddr_in sa{};
        sa.sin_family = AF_INET;
        sa.sin_port = htons(port);
        if (inet_pton(AF_INET, addr.c_str(), &sa.sin_addr) != 1)
            throw std::runtime_error("bad bind address: " + addr);
        if (::bind(listen_fd_, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) != 0)
            throw std::runtime_error("bind failed: " + std::string(strerror(errno)));
        if (::listen(listen_fd_, 256) != 0) throw std::runtime_error("listen failed");
        sockaddr_in bound{};
        socklen_t blen = sizeof(bound);
        getsockname(listen_fd_, reinterpret_cast<sockaddr*>(&bound), &blen);
        port_ = ntohs(bound.sin_port);
        LOG_INFO("builtin http server listening on {}:{}", addr, port_);
    }

    uint16_t bound_port() const override { return port_; }

    void run() override {
        for (;;) {
            sockaddr_in peer{};
            socklen_t plen = sizeof(peer);
            int fd = ::accept(listen_fd_, reinterpret_cast<sockaddr*>(&peer), &plen);
            if (fd < 0) {
                if (stopping_.load()) break;
                if (errno == EINTR) continue;
                break;
            }
            if (stopping_.load()) {
                ::close(fd);
                break;
            }
            char ip[64] = {0};
            inet_ntop(AF_INET, &peer.sin_addr, ip, sizeof(ip));
            {
                std::lock_guard lk(m_);
                ++active_;
                conns_.insert(fd);
            }
            std::thread([this, fd, peer_ip = std::string(ip)] {
                handle_connection(fd, peer_ip);
                std::lock_guard lk(m_);
                conns_.erase(fd);
                ::close(fd);
                if (--active_ == 0) cv_.notify_all();
            }).detach();
        }
        // 优雅退出：等待在途连接，超时强制断开
        std::unique_lock lk(m_);
        if (!cv_.wait_for(lk, std::chrono::seconds(10), [&] { return active_ == 0; })) {
            LOG_WARN("forcing {} connection(s) closed on shutdown", active_);
            for (int fd : conns_) ::shutdown(fd, SHUT_RDWR);
            cv_.wait_for(lk, std::chrono::seconds(5), [&] { return active_ == 0; });
        }
        LOG_INFO("builtin http server stopped");
    }

    // 仅做 async-signal-safe 操作，可在信号处理器中调用
    void shutdown() override {
        stopping_.store(true);
        if (listen_fd_ >= 0) ::shutdown(listen_fd_, SHUT_RDWR);
    }

private:
    void handle_connection(int fd, const std::string& peer);
    bool serve_one(int fd, ConnReader& reader, const std::string& peer, bool& keep_alive);
    bool write_response(int fd, HttpResponse& resp, bool head_request, bool keep_alive);

    HttpConfig cfg_;
    Handler handler_;
    int listen_fd_ = -1;
    uint16_t port_ = 0;
    std::atomic<bool> stopping_{false};
    std::mutex m_;
    std::condition_variable cv_;
    std::set<int> conns_;
    int active_ = 0;
};

void BuiltinServer::handle_connection(int fd, const std::string& peer) {
    timeval tv{cfg_.idle_timeout_sec, 0};
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    ConnReader reader{fd};
    bool keep_alive = true;
    while (keep_alive && !stopping_.load()) {
        if (!serve_one(fd, reader, peer, keep_alive)) break;
    }
}

// 处理一个请求；返回 false 表示连接应关闭
bool BuiltinServer::serve_one(int fd, ConnReader& reader, const std::string& peer,
                              bool& keep_alive) {
    const size_t max_line = cfg_.max_header_size;

    std::string line;
    if (!reader.read_line(line, max_line) || line.empty()) return false;

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
        if (header_bytes > cfg_.max_header_size) return false;
        auto colon = line.find(':');
        if (colon == std::string::npos) return false;
        std::string k = line.substr(0, colon);
        std::string v = line.substr(colon + 1);
        v.erase(0, v.find_first_not_of(" \t"));
        auto tail = v.find_last_not_of(" \t");
        if (tail != std::string::npos) v.erase(tail + 1);
        req.headers.add(std::move(k), std::move(v));
    }

    if (auto c = req.headers.get("Connection")) {
        if (HeaderMap::ieq(*c, "close")) keep_alive = false;
        else if (HeaderMap::ieq(*c, "keep-alive")) keep_alive = true;
    }

    // body
    BodyState body_state;
    body_state.conn = &reader;
    std::optional<uint64_t> content_length;
    bool has_body = false;
    if (auto te = req.headers.get("Transfer-Encoding");
        te && HeaderMap::ieq(*te, "chunked")) {
        body_state.chunked = true;
        has_body = true;
    } else if (auto cl = req.headers.get("Content-Length")) {
        try {
            content_length = std::stoull(*cl);
        } catch (...) {
            return false;
        }
        body_state.remaining = *content_length;
        has_body = *content_length > 0;
    }
    if (has_body || content_length)
        req.body = std::make_unique<SocketBodyReader>(&body_state, content_length);

    if (auto e = req.headers.get("Expect"); e && HeaderMap::ieq(*e, "100-continue")) {
        if (!send_all(fd, "HTTP/1.1 100 Continue\r\n\r\n", 25)) return false;
    }

    bool head_request = req.method == "HEAD";
    HttpResponse resp;
    try {
        resp = sync_wait(handler_(std::move(req)));
    } catch (const std::exception& e) {
        // L2 会兜底一切异常，到这里说明 L2 之外出了问题（契约 2：500 + InternalError XML）
        LOG_ERROR("handler escaped exception: {}", e.what());
        resp = driver::internal_error_response();
        keep_alive = false;
    }

    // 复用连接前必须排空未消费的 body
    if (!body_state.drain()) keep_alive = false;

    if (!write_response(fd, resp, head_request, keep_alive)) return false;
    return keep_alive;
}

bool BuiltinServer::write_response(int fd, HttpResponse& resp, bool head_request,
                                   bool keep_alive) {
    bool no_body_status = resp.status == 204 || resp.status == 304 || resp.status < 200;
    auto head = driver::render_response_head(resp, keep_alive);
    bool chunked = head.chunked;
    if (!send_all(fd, head.text.data(), head.text.size())) return false;
    if (head_request || no_body_status) return true;

    if (!resp.stream_body) return send_all(fd, resp.small_body.data(), resp.small_body.size());

    // 流式响应：64KiB 块拉取（docs/architecture.md 请求生命周期）
    std::byte buf[64 * 1024];
    for (;;) {
        size_t n = 0;
        try {
            n = sync_wait(resp.stream_body->read(std::span(buf)));
        } catch (const std::exception& e) {
            LOG_ERROR("stream body read failed mid-response: {}", e.what());
            return false;  // 响应头已发出，只能断连
        }
        if (n == 0) break;
        if (chunked) {
            char sz[32];
            int m = snprintf(sz, sizeof(sz), "%zx\r\n", n);
            if (!send_all(fd, sz, static_cast<size_t>(m))) return false;
        }
        if (!send_all(fd, reinterpret_cast<const char*>(buf), n)) return false;
        if (chunked && !send_all(fd, "\r\n", 2)) return false;
    }
    if (chunked && !send_all(fd, "0\r\n\r\n", 5)) return false;
    return true;
}

}  // namespace

void register_builtin_driver() {
    HttpServerFactory::register_driver("builtin", [](const HttpConfig& cfg) {
        return std::make_unique<BuiltinServer>(cfg);
    });
}

}  // namespace lights3::http
