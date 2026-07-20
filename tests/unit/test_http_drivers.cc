// 驱动一致性测试（docs/http-adapter.md §4）：所有已编译 driver 跑同一组契约用例。
// 用裸 TCP 客户端直接说 HTTP/1.1，验证驱动行为而非 L2 语义。
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <functional>
#include <thread>

#include "core/config.h"
#include "core/task.h"
#include "http/server.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::http;

namespace {

// ---------- 测试 handler ----------

std::atomic<bool> g_disconnect_seen{false};

uint8_t pattern_byte(uint64_t i) { return static_cast<uint8_t>(i * 131 + 7); }

uint64_t pattern_sum(uint64_t n) {
    uint64_t s = 0;
    for (uint64_t i = 0; i < n; ++i) s += pattern_byte(i);
    return s;
}

class PatternReader final : public BodyReader {
public:
    explicit PatternReader(uint64_t size) : size_(size) {}
    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = static_cast<size_t>(std::min<uint64_t>(buf.size(), size_ - pos_));
        for (size_t i = 0; i < n; ++i) buf[i] = std::byte{pattern_byte(pos_ + i)};
        pos_ += n;
        co_return n;
    }
    std::optional<uint64_t> length() const override { return size_; }

private:
    uint64_t size_;
    uint64_t pos_ = 0;
};

Task<HttpResponse> consume_and_sum(HttpRequest req, HttpResponse resp) {
    uint64_t total = 0, sum = 0;
    std::vector<std::byte> buf(64 * 1024);
    for (;;) {
        size_t n = co_await req.body->read(std::span(buf));
        if (n == 0) break;
        for (size_t i = 0; i < n; ++i) sum += std::to_integer<uint8_t>(buf[i]);
        total += n;
    }
    // 契约 1：EOF 后再调用仍返回 0
    size_t again = co_await req.body->read(std::span(buf));
    resp.small_body = std::to_string(total) + ":" + std::to_string(sum) + ":" +
                      (again == 0 ? "eof-ok" : "eof-bad");
    co_return resp;
}

Task<HttpResponse> test_handler(HttpRequest req) {
    HttpResponse resp;
    resp.headers.set("Content-Type", "text/plain");

    if (req.path == "/small") {
        resp.small_body = req.body ? "hasbody" : "nobody";
        co_return resp;
    }
    if (req.path == "/sum") co_return co_await consume_and_sum(std::move(req), std::move(resp));
    if (req.path == "/disc") {
        try {
            co_return co_await consume_and_sum(std::move(req), std::move(resp));
        } catch (const std::exception&) {
            g_disconnect_seen.store(true);  // 契约 3：断连以异常传播到消费方
            throw;
        }
    }
    if (req.path == "/stream" || req.path == "/chunked") {
        uint64_t size = std::stoull(req.query_get("size").value_or("0"));
        resp.stream_body = std::make_unique<PatternReader>(size);
        if (req.path == "/stream") resp.content_length = size;
        co_return resp;
    }
    if (req.path == "/noread") {  // 故意不消费 body（100-continue 拒绝场景）
        resp.small_body = "ok";
        co_return resp;
    }
    if (req.path == "/slow") {
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
        resp.small_body = "done";
        co_return resp;
    }
    if (req.path == "/throw") throw std::runtime_error("boom");
    resp.status = 404;
    co_return resp;
}

// ---------- 服务器夹具 ----------

struct TestServer {
    std::unique_ptr<IHttpServer> srv;
    std::thread th;
    uint16_t port = 0;

    explicit TestServer(const std::string& driver) {
        HttpConfig cfg;
        cfg.driver = driver;
        cfg.io_threads = 2;
        cfg.idle_timeout_sec = 5;
        srv = HttpServerFactory::create(driver, cfg);
        srv->set_handler([](HttpRequest req) { return test_handler(std::move(req)); });
        srv->listen("127.0.0.1", 0);
        port = srv->bound_port();
        th = std::thread([this] { srv->run(); });
    }
    ~TestServer() { stop(); }
    void stop() {
        if (th.joinable()) {
            srv->shutdown();
            th.join();
        }
    }
};

// ---------- 裸 TCP HTTP 客户端 ----------

struct Resp {
    bool ok = false;
    int status = 0;
    std::vector<std::pair<std::string, std::string>> headers;
    std::string body;

    std::optional<std::string> header(std::string_view key) const {
        for (auto& [k, v] : headers)
            if (HeaderMap::ieq(k, key)) return v;
        return std::nullopt;
    }
};

struct Client {
    int fd = -1;
    char buf[16 * 1024];
    size_t pos = 0, end = 0;

    explicit Client(uint16_t port) {
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        CHECK(fd >= 0);
        timeval tv{10, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        sockaddr_in sa{};
        sa.sin_family = AF_INET;
        sa.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
        CHECK(::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) == 0);
    }
    ~Client() { close_now(); }
    void close_now() {
        if (fd >= 0) ::close(fd);
        fd = -1;
    }

    void send_str(std::string_view s) {
        const char* p = s.data();
        size_t left = s.size();
        while (left > 0) {
            ssize_t n = ::send(fd, p, left, MSG_NOSIGNAL);
            CHECK(n > 0);
            p += n;
            left -= static_cast<size_t>(n);
        }
    }

    bool read_line(std::string& line) {
        line.clear();
        for (;;) {
            while (pos < end) {
                char c = buf[pos++];
                if (c == '\n') {
                    if (!line.empty() && line.back() == '\r') line.pop_back();
                    return true;
                }
                line.push_back(c);
            }
            ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0) return false;
            pos = 0;
            end = static_cast<size_t>(n);
        }
    }

    bool read_n(std::string& out, size_t want) {
        while (want > 0) {
            if (pos < end) {
                size_t n = std::min(want, end - pos);
                out.append(buf + pos, n);
                pos += n;
                want -= n;
                continue;
            }
            ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0) return false;
            pos = 0;
            end = static_cast<size_t>(n);
        }
        return true;
    }

    struct Head {
        int status = 0;
        std::vector<std::pair<std::string, std::string>> headers;
    };

    bool read_head(Head& h) {
        std::string line;
        if (!read_line(line) || line.size() < 12) return false;
        h.status = std::atoi(line.c_str() + 9);
        h.headers.clear();
        while (read_line(line)) {
            if (line.empty()) return true;
            auto colon = line.find(':');
            if (colon == std::string::npos) return false;
            std::string v = line.substr(colon + 1);
            v.erase(0, v.find_first_not_of(' '));
            h.headers.emplace_back(line.substr(0, colon), v);
        }
        return false;
    }

    Resp finish_body(const Head& h, bool head_request = false) {
        Resp r;
        r.status = h.status;
        r.headers = h.headers;
        if (head_request || h.status == 204 || h.status == 304) {
            r.ok = true;
            return r;
        }
        std::string te, cl;
        for (auto& [k, v] : h.headers) {
            if (HeaderMap::ieq(k, "Transfer-Encoding")) te = v;
            if (HeaderMap::ieq(k, "Content-Length")) cl = v;
        }
        if (HeaderMap::ieq(te, "chunked")) {
            std::string line;
            for (;;) {
                if (!read_line(line)) return r;
                if (line.empty()) continue;
                size_t sz = std::stoull(line, nullptr, 16);
                if (sz == 0) {  // trailer 直到空行
                    while (read_line(line) && !line.empty()) {}
                    r.ok = true;
                    return r;
                }
                if (!read_n(r.body, sz)) return r;
            }
        }
        if (!cl.empty()) {
            r.ok = read_n(r.body, std::stoull(cl));
            return r;
        }
        // 无长度信息：读到连接关闭
        for (;;) {
            if (pos < end) {
                r.body.append(buf + pos, end - pos);
                pos = end;
            }
            ssize_t n = ::recv(fd, buf, sizeof(buf), 0);
            if (n <= 0) break;
            pos = 0;
            end = static_cast<size_t>(n);
        }
        r.ok = true;
        return r;
    }

    // 读一个完整响应，自动跳过 1xx 中间应答
    Resp read_response(bool head_request = false) {
        Head h;
        for (;;) {
            if (!read_head(h)) return {};
            if (h.status / 100 == 1) continue;
            return finish_body(h, head_request);
        }
    }
};

std::string make_pattern(uint64_t n) {
    std::string s(n, '\0');
    for (uint64_t i = 0; i < n; ++i) s[i] = static_cast<char>(pattern_byte(i));
    return s;
}

std::string expected_sum(uint64_t n) {
    return std::to_string(n) + ":" + std::to_string(pattern_sum(n)) + ":eof-ok";
}

void for_each_driver(const std::function<void(const std::string&)>& fn) {
    signal(SIGPIPE, SIG_IGN);  // 断连场景驱动可能写已关闭的 socket
    auto drivers = HttpServerFactory::drivers();
    CHECK(!drivers.empty());
    for (auto& d : drivers) {
        try {
            fn(d);
        } catch (const mini_test::Failure& f) {
            throw mini_test::Failure("[driver=" + d + "] " + f.what());
        } catch (const std::exception& e) {
            throw mini_test::Failure("[driver=" + d + "] unexpected exception: " + e.what());
        }
    }
}

}  // namespace

// ---------- 用例 ----------

TEST(http_driver_registry_complete) {
    auto ds = HttpServerFactory::drivers();
    auto has = [&](const char* name) {
        return std::find(ds.begin(), ds.end(), name) != ds.end();
    };
#ifdef LIGHTS3_DRIVER_BUILTIN
    CHECK(has("builtin"));
#endif
#ifdef LIGHTS3_DRIVER_BEAST
    CHECK(has("beast"));
#endif
#ifdef LIGHTS3_DRIVER_HTTPLIB
    CHECK(has("httplib"));
#endif
#ifdef LIGHTS3_DRIVER_SEASTAR
    CHECK(has("seastar"));
#endif
}

TEST(http_driver_large_put) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        const uint64_t size = 8 * 1024 * 1024;
        Client c(ts.port);
        c.send_str("PUT /sum HTTP/1.1\r\nHost: t\r\nContent-Length: " + std::to_string(size) +
                   "\r\n\r\n");
        c.send_str(make_pattern(size));
        auto r = c.read_response();
        CHECK(r.ok);
        CHECK_EQ(r.status, 200);
        CHECK_EQ(r.body, expected_sum(size));
    });
}

TEST(http_driver_large_get) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        const uint64_t size = 8 * 1024 * 1024;
        Client c(ts.port);
        c.send_str("GET /stream?size=" + std::to_string(size) + " HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r = c.read_response();
        CHECK(r.ok);
        CHECK_EQ(r.status, 200);
        CHECK_EQ(r.header("Content-Length").value_or(""), std::to_string(size));
        CHECK_EQ(r.body.size(), size);
        CHECK(r.body == make_pattern(size));
    });
}

TEST(http_driver_chunked_response) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        const uint64_t size = 1000 * 1000;
        Client c(ts.port);
        c.send_str("GET /chunked?size=" + std::to_string(size) + " HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r = c.read_response();
        CHECK(r.ok);
        CHECK_EQ(r.status, 200);
        CHECK(HeaderMap::ieq(r.header("Transfer-Encoding").value_or(""), "chunked"));
        CHECK(r.body == make_pattern(size));
    });
}

TEST(http_driver_chunked_request) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        const uint64_t size = 300 * 1000;
        Client c(ts.port);
        c.send_str("PUT /sum HTTP/1.1\r\nHost: t\r\nTransfer-Encoding: chunked\r\n\r\n");
        auto data = make_pattern(size);
        size_t sent = 0;
        while (sent < data.size()) {  // 分不规则块发送
            size_t n = std::min<size_t>(data.size() - sent, 40000);
            char hdr[32];
            snprintf(hdr, sizeof(hdr), "%zx\r\n", n);
            c.send_str(hdr);
            c.send_str(std::string_view(data).substr(sent, n));
            c.send_str("\r\n");
            sent += n;
        }
        c.send_str("0\r\n\r\n");
        auto r = c.read_response();
        CHECK(r.ok);
        CHECK_EQ(r.status, 200);
        CHECK_EQ(r.body, expected_sum(size));
    });
}

TEST(http_driver_expect_100_continue) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        const uint64_t size = 100 * 1000;
        Client c(ts.port);
        c.send_str("PUT /sum HTTP/1.1\r\nHost: t\r\nExpect: 100-continue\r\nContent-Length: " +
                   std::to_string(size) + "\r\n\r\n");
        Client::Head h;
        CHECK(c.read_head(h));  // 所有驱动最终都会给 100（beast 在 handler 首读时才回）
        CHECK_EQ(h.status, 100);
        c.send_str(make_pattern(size));
        auto r = c.read_response();
        CHECK(r.ok);
        CHECK_EQ(r.status, 200);
        CHECK_EQ(r.body, expected_sum(size));
    });
}

TEST(http_driver_expect_100_rejected_without_body) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        c.send_str("PUT /noread HTTP/1.1\r\nHost: t\r\nExpect: 100-continue\r\n"
                   "Content-Length: 1000000\r\n\r\n");
        // handler 不读 body 直接回复。延迟 100 的驱动（builtin/beast）直接给最终响应，
        // 客户端无需上传 1MB；httplib 会先回 100，客户端按协议把 body 发完
        Client::Head h;
        CHECK(c.read_head(h));
        if (h.status == 100) {
            c.send_str(make_pattern(1000000));
            auto r = c.read_response();
            CHECK(r.ok);
            CHECK_EQ(r.status, 200);
            CHECK_EQ(r.body, "ok");
        } else {
            auto r = c.finish_body(h);
            CHECK(r.ok);
            CHECK_EQ(r.status, 200);
            CHECK_EQ(r.body, "ok");
        }
    });
}

TEST(http_driver_handler_exception_500_xml) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        c.send_str("GET /throw HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r = c.read_response();
        CHECK(r.ok);
        CHECK_EQ(r.status, 500);  // 契约 2：500 + S3 InternalError XML
        CHECK(r.body.find("<Code>InternalError</Code>") != std::string::npos);
    });
}

TEST(http_driver_keep_alive) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r1 = c.read_response();
        CHECK(r1.ok);
        CHECK_EQ(r1.status, 200);
        CHECK_EQ(r1.body, "nobody");  // GET 无 body：req.body 为 nullptr
        c.send_str("PUT /sum HTTP/1.1\r\nHost: t\r\nContent-Length: 5\r\n\r\nhello");
        auto r2 = c.read_response();  // 同一连接第二个请求
        CHECK(r2.ok);
        CHECK_EQ(r2.status, 200);
        uint64_t hsum = 0;
        for (char ch : std::string("hello")) hsum += static_cast<uint8_t>(ch);
        CHECK_EQ(r2.body, "5:" + std::to_string(hsum) + ":eof-ok");
    });
}

TEST(http_driver_unconsumed_body_then_reuse) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        // handler 不读 body；驱动必须排空后才能复用连接
        std::string body = make_pattern(100 * 1000);
        c.send_str("PUT /noread HTTP/1.1\r\nHost: t\r\nContent-Length: " +
                   std::to_string(body.size()) + "\r\n\r\n");
        c.send_str(body);
        auto r1 = c.read_response();
        CHECK(r1.ok);
        CHECK_EQ(r1.status, 200);
        CHECK_EQ(r1.body, "ok");
        c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r2 = c.read_response();
        CHECK(r2.ok);
        CHECK_EQ(r2.status, 200);
        CHECK_EQ(r2.body, "nobody");
    });
}

TEST(http_driver_mid_body_disconnect) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        g_disconnect_seen.store(false);
        {
            Client c(ts.port);
            c.send_str("PUT /disc HTTP/1.1\r\nHost: t\r\nContent-Length: 1000000\r\n\r\n");
            c.send_str(make_pattern(1000));  // 只发 1KB 就断
        }
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!g_disconnect_seen.load() && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        CHECK(g_disconnect_seen.load());  // 契约 3：断连传播为 body->read() 异常
    });
}

TEST(http_driver_concurrent_shutdown) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        c.send_str("GET /slow HTTP/1.1\r\nHost: t\r\n\r\n");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));  // 请求已在途
        auto t0 = std::chrono::steady_clock::now();
        ts.srv->shutdown();
        auto r = c.read_response();  // 契约 4：在途请求必须完成
        CHECK(r.ok);
        CHECK_EQ(r.status, 200);
        CHECK_EQ(r.body, "done");
        c.close_now();
        ts.th.join();  // run() 在在途请求完成后返回
        auto elapsed = std::chrono::steady_clock::now() - t0;
        CHECK(elapsed < std::chrono::seconds(8));
    });
}

TEST(http_driver_head_request) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        c.send_str("HEAD /small HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r = c.read_response(/*head_request=*/true);
        CHECK(r.ok);
        CHECK_EQ(r.status, 200);
        // HEAD 不发 body：连接上应能直接跑下一个请求
        c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r2 = c.read_response();
        CHECK(r2.ok);
        CHECK_EQ(r2.body, "nobody");
    });
}
