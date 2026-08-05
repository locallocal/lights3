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
#include "http/pushpull.h"
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
    if (req.path == "/short") {  // 后端截断：声明 size，实际只给一半
        uint64_t size = std::stoull(req.query_get("size").value_or("0"));
        resp.stream_body = std::make_unique<PatternReader>(size / 2);
        resp.content_length = size;
        co_return resp;
    }
    if (req.path == "/noread") {  // 故意不消费 body（100-continue 拒绝场景）
        resp.small_body = "ok";
        co_return resp;
    }
    if (req.path == "/badheader") {  // 出站头注入面：CR/LF 值与非法头名必须被丢弃
        resp.headers.set("X-Evil", "a\r\nInjected: 1");
        resp.headers.set("Bad Name", "v");
        resp.headers.set("X-Fine", "ok");
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

// ---------- 消息边界（framing）：请求走私防护 ----------
//
// RFC 9112 §6.1：边界有歧义的请求必须 400 或关连接。对所有驱动统一断言两件事：
//   1) 第一个响应不是成功（错误状态码，或连接直接关闭）；
//   2) 攻击载荷里夹带的第二个请求不被当作独立请求应答（走私未发生）。
void check_framing_rejected(const std::string& driver, const std::string& raw) {
    TestServer ts(driver);
    Client c(ts.port);
    c.send_str(raw);
    auto r1 = c.read_response();
    // ok=false 表示驱动直接关连接，也是允许的处置方式
    if (r1.ok) CHECK(r1.status >= 400);
    // 夹带的 GET /small 若被独立应答，会得到 200 + "nobody"
    auto r2 = c.read_response();
    CHECK(!(r2.ok && r2.status == 200 && r2.body == "nobody"));
}

TEST(http_driver_rejects_cl_te_conflict) {
    for_each_driver([](const std::string& d) {
        // CL.TE 走私：前置代理按 Content-Length 断帧，后端按 chunked 断帧时，
        // "GET /small" 会成为下一个请求
        check_framing_rejected(d,
                               "POST /sum HTTP/1.1\r\nHost: t\r\n"
                               "Content-Length: 6\r\nTransfer-Encoding: chunked\r\n\r\n"
                               "0\r\n\r\nGET /small HTTP/1.1\r\nHost: t\r\n\r\n");
    });
}

TEST(http_driver_rejects_duplicate_content_length) {
    for_each_driver([](const std::string& d) {
        check_framing_rejected(d,
                               "POST /sum HTTP/1.1\r\nHost: t\r\n"
                               "Content-Length: 0\r\nContent-Length: 44\r\n\r\n"
                               "GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
    });
}

TEST(http_driver_rejects_negative_content_length) {
    for_each_driver([](const std::string& d) {
        // stoull("-1") 回绕成 2^64-1：驱动会"永远等 body"，连接挂死
        check_framing_rejected(d,
                               "POST /sum HTTP/1.1\r\nHost: t\r\nContent-Length: -1\r\n\r\n"
                               "GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
    });
}

TEST(http_driver_rejects_content_length_trailing_garbage) {
    for_each_driver([](const std::string& d) {
        // stoull("5abc") 截成 5：声明与实际断帧脱节
        check_framing_rejected(d,
                               "POST /sum HTTP/1.1\r\nHost: t\r\nContent-Length: 5abc\r\n\r\n"
                               "GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
    });
}

TEST(http_driver_rejects_non_chunked_transfer_encoding) {
    for_each_driver([](const std::string& d) {
        // "gzip, chunked" 不被识别为 chunked 时，body 会被当作下一个请求行解析
        check_framing_rejected(d,
                               "POST /sum HTTP/1.1\r\nHost: t\r\n"
                               "Transfer-Encoding: gzip, chunked\r\n\r\n"
                               "0\r\n\r\nGET /small HTTP/1.1\r\nHost: t\r\n\r\n");
    });
}

TEST(http_driver_rejects_bad_chunk_size) {
    for_each_driver([](const std::string& d) {
        // chunk size "-1"：stoull 接受负号，body 长度脱离声明
        check_framing_rejected(d,
                               "POST /sum HTTP/1.1\r\nHost: t\r\n"
                               "Transfer-Encoding: chunked\r\n\r\n"
                               "-1\r\nxx\r\n0\r\n\r\n"
                               "GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
    });
}

TEST(http_driver_rejects_missing_crlf_after_chunk) {
    for_each_driver([](const std::string& d) {
        // chunk 数据后不是 CRLF 而是"看似 hex"的垃圾：不得被当作下一个 chunk size
        check_framing_rejected(d,
                               "POST /sum HTTP/1.1\r\nHost: t\r\n"
                               "Transfer-Encoding: chunked\r\n\r\n"
                               "4\r\nAAAAdead\r\n0\r\n\r\n"
                               "GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
    });
}

TEST(http_driver_truncated_stream_closes_connection) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        // 后端只给出声明长度的一半：驱动必须断连，否则客户端会把下一个响应头
        // 当作本次 body 的剩余部分（响应错位）
        c.send_str("GET /short?size=100000 HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r = c.read_response();
        CHECK(!r.ok);  // 声明的字节数读不满，连接被关闭
        // 连接确已不可复用：后续请求得不到响应
        c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r2 = c.read_response();
        CHECK(!r2.ok);
    });
}

TEST(http_driver_response_headers_not_duplicated) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r = c.read_response();
        CHECK(r.ok);
        // 驱动自管的头只能出现一次：重复 Content-Length 即断帧漏洞
        auto count = [&](const char* name) {
            int n = 0;
            for (auto& [k, v] : r.headers)
                if (HeaderMap::ieq(k, name)) ++n;
            return n;
        };
        CHECK_EQ(count("Content-Length"), 1);
        CHECK(count("Transfer-Encoding") == 0);
        CHECK(count("Connection") <= 1);
    });
}

TEST(http_driver_filters_header_injection) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        c.send_str("GET /badheader HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r = c.read_response();
        CHECK(r.ok);
        CHECK_EQ(r.status, 200);
        // CR/LF 值（响应拆分注入面）与非法头名整体丢弃，正常头保留
        CHECK(!r.header("Injected"));
        CHECK(!r.header("X-Evil"));
        CHECK(!r.header("Bad Name"));
        CHECK_EQ(r.header("X-Fine").value_or(""), "ok");
        CHECK_EQ(r.body, "ok");
    });
}

// ---------- pushpull：共享的推转拉组件 ----------

TEST(block_queue_cancel_wakes_blocked_consumer) {
    auto q = std::make_shared<BlockQueue>(64 * 1024);
    std::atomic<bool> threw{false}, done{false};
    std::thread consumer([&] {
        std::byte buf[1024];
        try {
            q->pop(std::span(buf));
        } catch (const std::exception&) {
            threw.store(true);
        }
        done.store(true);
    });
    std::this_thread::sleep_for(std::chrono::milliseconds(50));  // 确保已阻塞在 pop
    CHECK(!done.load());
    q->cancel();  // 必须同时唤醒 pop 侧，否则消费者永久阻塞
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!done.load() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    consumer.join();
    CHECK(done.load());
    CHECK(threw.load());  // cancel 不是正常 EOF，以异常传播
}

TEST(block_queue_normal_eof_still_returns_zero) {
    auto q = std::make_shared<BlockQueue>(64 * 1024);
    CHECK(q->push("hello", 5));
    q->close(true);
    std::byte buf[16];
    CHECK_EQ(q->pop(std::span(buf)), size_t(5));
    CHECK_EQ(q->pop(std::span(buf)), size_t(0));  // 正常 EOF
    // close 之后再 cancel（消费端析构的常规顺序）不得把 EOF 变成异常
    q->cancel();
    CHECK_EQ(q->pop(std::span(buf)), size_t(0));
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
