// Driver consistency tests (docs/http-adapter.md §4): every compiled driver runs the same set of contract cases.
// A raw TCP client speaks HTTP/1.1 directly, verifying driver behavior rather than L2 semantics.
#include <arpa/inet.h>
#include <netinet/in.h>
#include <openssl/ssl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstdio>
#include <cstdlib>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <functional>
#include <thread>

#include "core/config.h"
#include "core/task.h"
#include "http/drivers/common.h"
#include "http/pushpull.h"
#include "http/server.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::http;

namespace {

// ---------- Test handler ----------

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

// File-backed body for the sendfile contract (roadmap §4.3 ④): pread on the
// calling thread (tests only), remaining range exposed through try_as_file
std::atomic<uint64_t> g_file_bytes_sent{0};  // bytes moved by a driver's sendfile path
std::atomic<uint64_t> g_file_bytes_read{0};  // bytes pulled through read()

class FileRangeReader final : public BodyReader {
public:
    FileRangeReader(int fd, uint64_t off, uint64_t len) : fd_(fd), off_(off), left_(len), total_(len) {}
    ~FileRangeReader() override { ::close(fd_); }
    Task<size_t> read(std::span<std::byte> buf) override {
        size_t want = static_cast<size_t>(std::min<uint64_t>(buf.size(), left_));
        if (want == 0) co_return 0;
        ssize_t n = ::pread(fd_, buf.data(), want, static_cast<off_t>(off_));
        if (n <= 0) co_return 0;
        off_ += static_cast<uint64_t>(n);
        left_ -= static_cast<uint64_t>(n);
        co_return static_cast<size_t>(n);
    }
    std::optional<uint64_t> length() const override { return total_; }
    std::optional<FileSpan> try_as_file() override { return FileSpan{fd_, off_, left_}; }
    void file_bytes_sent(uint64_t n) override {
        off_ += n;
        left_ -= n;
    }

private:
    int fd_;
    uint64_t off_, left_, total_;
};

// Accounting decorator in the shape of L2's CountingBodyReader: forwards the
// fast path and still sees every byte
class ForwardingCounter final : public BodyReader {
public:
    explicit ForwardingCounter(std::unique_ptr<BodyReader> inner) : inner_(std::move(inner)) {}
    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = co_await inner_->read(buf);
        g_file_bytes_read += n;
        co_return n;
    }
    std::optional<uint64_t> length() const override { return inner_->length(); }
    std::optional<FileSpan> try_as_file() override { return inner_->try_as_file(); }
    void file_bytes_sent(uint64_t n) override {
        inner_->file_bytes_sent(n);
        g_file_bytes_sent += n;
    }

private:
    std::unique_ptr<BodyReader> inner_;
};

// A temp file holding make_pattern(bytes); returns an fd (unlinked)
int pattern_file(uint64_t bytes) {
    char name[] = "/tmp/lights3-sendfile.XXXXXX";
    int fd = ::mkstemp(name);
    CHECK(fd >= 0);
    ::unlink(name);
    std::string data(bytes, '\0');
    for (uint64_t i = 0; i < bytes; ++i) data[i] = static_cast<char>(pattern_byte(i));
    size_t done = 0;
    while (done < data.size()) {
        ssize_t n = ::write(fd, data.data() + done, data.size() - done);
        CHECK(n > 0);
        done += static_cast<size_t>(n);
    }
    return fd;
}

// Throws on the k-th read: the prefetch must surface it at the right chunk
class FailingReader final : public BodyReader {
public:
    FailingReader(uint64_t size, int fail_at) : inner_(size), fail_at_(fail_at) {}
    Task<size_t> read(std::span<std::byte> buf) override {
        if (++calls_ == fail_at_) throw std::runtime_error("backend read failed");
        co_return co_await inner_.read(buf);
    }
    std::optional<uint64_t> length() const override { return inner_.length(); }

private:
    PatternReader inner_;
    int fail_at_;
    int calls_ = 0;
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
    // Contract 1: calls after EOF still return 0
    size_t again = co_await req.body->read(std::span(buf));
    resp.small_body = std::to_string(total) + ":" + std::to_string(sum) + ":" +
                      (again == 0 ? "eof-ok" : "eof-bad");
    co_return resp;
}

Task<HttpResponse> test_handler(HttpRequest req) {
    HttpResponse resp;
    resp.headers.set("Content-Type", "text/plain");

    if (req.path == "/method") {  // echo the method: verifies whether L1 forwards unknown methods verbatim
        resp.small_body = req.method;
        co_return resp;
    }
    if (req.path == "/small") {
        resp.small_body = req.body ? "hasbody" : "nobody";
        co_return resp;
    }
    if (req.path == "/sum") co_return co_await consume_and_sum(std::move(req), std::move(resp));
    if (req.path == "/disc") {
        try {
            co_return co_await consume_and_sum(std::move(req), std::move(resp));
        } catch (const std::exception&) {
            g_disconnect_seen.store(true);  // contract 3: disconnect propagates to the consumer as an exception
            throw;
        }
    }
    if (req.path == "/stream" || req.path == "/chunked") {
        uint64_t size = std::stoull(req.query_get("size").value_or("0"));
        resp.stream_body = std::make_unique<PatternReader>(size);
        if (req.path == "/stream") resp.content_length = size;
        co_return resp;
    }
    if (req.path == "/file") {  // file-backed body: sendfile on builtin/plaintext, read() elsewhere
        uint64_t size = std::stoull(req.query_get("size").value_or("0"));
        uint64_t off = std::stoull(req.query_get("off").value_or("0"));
        bool short_file = req.query_has("short");  // file ends halfway through the declared range
        int fd = pattern_file(off + (short_file ? size / 2 : size));
        resp.stream_body = std::make_unique<ForwardingCounter>(
            std::make_unique<FileRangeReader>(fd, off, size));
        resp.content_length = size;
        co_return resp;
    }
    if (req.path == "/failing") {  // backend error mid-stream, after `at` successful chunks
        uint64_t size = std::stoull(req.query_get("size").value_or("0"));
        int at = std::stoi(req.query_get("at").value_or("3"));
        resp.stream_body = std::make_unique<FailingReader>(size, at);
        resp.content_length = size;
        co_return resp;
    }
    if (req.path == "/short") {  // backend truncation: declares size but delivers only half
        uint64_t size = std::stoull(req.query_get("size").value_or("0"));
        resp.stream_body = std::make_unique<PatternReader>(size / 2);
        resp.content_length = size;
        co_return resp;
    }
    if (req.path == "/noread") {  // deliberately do not consume the body (100-continue rejection scenario)
        resp.small_body = "ok";
        co_return resp;
    }
    if (req.path == "/badheader") {  // outbound header injection surface: CR/LF values and illegal header names must be dropped
        resp.headers.set("X-Evil", "a\r\nInjected: 1");
        resp.headers.set("Bad Name", "v");
        resp.headers.set("X-Fine", "ok");
        resp.small_body = "ok";
        co_return resp;
    }
    if (req.path == "/slow") {  // ms tunable: the shutdown contract tests use it to simulate in-flight requests
        uint64_t ms = std::stoull(req.query_get("ms").value_or("500"));
        std::this_thread::sleep_for(std::chrono::milliseconds(ms));
        resp.small_body = "done";
        co_return resp;
    }
    if (req.path == "/throw") throw std::runtime_error("boom");
    resp.status = 404;
    co_return resp;
}

// ---------- Server fixture ----------

struct TestServer {
    std::unique_ptr<IHttpServer> srv;
    std::thread th;
    uint16_t port = 0;

    // A non-empty tls_cert/tls_key starts the TLS port. The unwind path of a failed assertion is joined by the destructor --
    // with a hand-written joinable std::thread in a test case, unwinding would destroy the thread first, i.e. std::terminate
    explicit TestServer(const std::string& driver, const std::string& tls_cert = "",
                        const std::string& tls_key = "") {
        HttpConfig cfg;
        cfg.driver = driver;
        cfg.io_threads = 2;
        cfg.idle_timeout_sec = 5;
        cfg.tls_cert = tls_cert;
        cfg.tls_key = tls_key;
        start(driver, cfg);
    }
    // For timeout/limit contract tests: tweak rewrites the target knob on the default config
    TestServer(const std::string& driver, const std::function<void(HttpConfig&)>& tweak) {
        HttpConfig cfg;
        cfg.driver = driver;
        cfg.io_threads = 2;
        cfg.idle_timeout_sec = 5;
        tweak(cfg);
        start(driver, cfg);
    }
    void start(const std::string& driver, const HttpConfig& cfg) {
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

// ---------- Raw TCP HTTP client ----------

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
                if (sz == 0) {  // trailers until the empty line
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
        // No length information: read until the connection closes
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

    // Read one complete response, automatically skipping 1xx interim replies
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
    signal(SIGPIPE, SIG_IGN);  // in disconnect scenarios the driver may write to an already-closed socket
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

// ---------- Test cases ----------

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

TEST(http_driver_file_body_sendfile_or_read) {
    // Every driver serves a file-backed body correctly; builtin/plaintext takes
    // the sendfile exit (bytes reported through file_bytes_sent), the others
    // (and TLS / sendfile: false) pull through read() -- the decorator sees the
    // full byte count either way (roadmap §4.3 ④)
    const uint64_t size = 3 * 1024 * 1024 + 12345, off = 777;
    std::string expect = make_pattern(off + size).substr(off);
    for_each_driver([&](const std::string& d) {
        TestServer ts(d);
        g_file_bytes_sent = 0;
        g_file_bytes_read = 0;
        Client c(ts.port);
        c.send_str("GET /file?size=" + std::to_string(size) + "&off=" + std::to_string(off) +
                   " HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r = c.read_response();
        CHECK(r.ok);
        CHECK_EQ(r.status, 200);
        CHECK_EQ(r.header("Content-Length").value_or(""), std::to_string(size));
        CHECK(r.body == expect);
        if (d == "builtin") {
            CHECK_EQ(g_file_bytes_sent.load(), size);
            CHECK_EQ(g_file_bytes_read.load(), uint64_t{0});
        } else {
            CHECK_EQ(g_file_bytes_sent.load(), uint64_t{0});
            CHECK_EQ(g_file_bytes_read.load(), size);
        }
        // keep-alive survives the fast path: framing was exact
        c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r2 = c.read_response();
        CHECK(r2.ok);
        CHECK_EQ(r2.body, "nobody");
    });
    {
        TestServer ts("builtin", [](HttpConfig& c) { c.sendfile = false; });
        g_file_bytes_sent = 0;
        g_file_bytes_read = 0;
        Client c(ts.port);
        c.send_str("GET /file?size=100000 HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r = c.read_response();
        CHECK(r.ok);
        CHECK(r.body == make_pattern(100000));
        CHECK_EQ(g_file_bytes_sent.load(), uint64_t{0});
        CHECK_EQ(g_file_bytes_read.load(), uint64_t{100000});
    }
}

TEST(http_driver_file_body_short_file_closes_connection) {
    // The file ends before the declared length: sendfile reports the shortfall,
    // the driver disconnects (same contract as a short stream_body)
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        c.send_str("GET /file?size=200000&short=1 HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r = c.read_response();
        CHECK(!r.ok);
        c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
        CHECK(!c.read_response().ok);
    });
}

TEST(http_driver_backend_error_mid_stream_closes_connection) {
    // With one read in flight ahead of the socket (roadmap §4.3 ①) an error on
    // the k-th chunk must still end in a disconnect, never a silently short
    // body that looks complete
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        c.send_str("GET /failing?size=1000000&at=4 HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r = c.read_response();
        CHECK(!r.ok);
    });
}

TEST(io_buffer_pool_reuses_per_thread) {
    using lights3::http::driver::IoBuffer;
    size_t base = IoBuffer::cached_count();
    std::byte* first;
    {
        IoBuffer a(64 * 1024);
        first = a.data();
        CHECK_EQ(a.size(), size_t{64 * 1024});
    }
    CHECK_EQ(IoBuffer::cached_count(), base + 1);
    {
        IoBuffer b(32 * 1024);  // a cached buffer with enough capacity is handed out
        CHECK(b.data() == first);
        CHECK_EQ(b.size(), size_t{32 * 1024});  // ...but presents only the requested size
        CHECK_EQ(b.capacity(), size_t{64 * 1024});
        CHECK_EQ(b.span().size(), size_t{32 * 1024});
        CHECK_EQ(IoBuffer::cached_count(), base);
        IoBuffer c(128 * 1024);  // none large enough: fresh allocation
        CHECK(c.data() != first);
    }
    CHECK_EQ(IoBuffer::cached_count(), base + 2);
    // Bounded: releasing more than the cap drops the surplus
    {
        std::vector<IoBuffer> many;
        for (size_t i = 0; i < IoBuffer::kPerThreadCap + 4; ++i) many.emplace_back(4096);
    }
    CHECK(IoBuffer::cached_count() <= IoBuffer::kPerThreadCap);
}

TEST(stream_prefetch_delivers_in_order_and_surfaces_errors) {
    using lights3::http::driver::StreamPrefetch;
    const uint64_t size = 200000;
    {
        PatternReader r(size);
        StreamPrefetch pf(r, 64 * 1024);
        std::string got;
        for (;;) {
            auto chunk = pf.next_sync();
            if (chunk.empty()) break;
            got.append(reinterpret_cast<const char*>(chunk.data()), chunk.size());
        }
        CHECK(got == make_pattern(size));
        CHECK(pf.at_eof());
        CHECK(pf.next_sync().empty());  // EOF is sticky
    }
    {
        FailingReader r(size, 2);
        StreamPrefetch pf(r, 64 * 1024);
        CHECK_EQ(pf.next_sync().size(), size_t{64 * 1024});  // chunk 1 fine, chunk 2 is prefetched and fails
        bool threw = false;
        try {
            pf.next_sync();
        } catch (const std::runtime_error&) {
            threw = true;
        }
        CHECK(threw);
    }
    {
        // Coroutine collect path
        PatternReader r(size);
        auto run = [&]() -> Task<std::string> {
            StreamPrefetch pf(r, 50000);
            std::string got;
            for (;;) {
                auto chunk = co_await pf.next();
                if (chunk.empty()) break;
                got.append(reinterpret_cast<const char*>(chunk.data()), chunk.size());
            }
            co_return got;
        };
        CHECK(sync_wait(run()) == make_pattern(size));
    }
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
        while (sent < data.size()) {  // send in irregular pieces
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
        CHECK(c.read_head(h));  // all drivers eventually send 100 (beast replies only at the handler's first read)
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
        // The handler replies without reading the body. Drivers that delay the 100 (builtin/beast) send the final response directly,
        // so the client need not upload 1MB; httplib sends 100 first and the client finishes the body per protocol
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
        CHECK_EQ(r.status, 500);  // contract 2: 500 + S3 InternalError XML
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
        CHECK_EQ(r1.body, "nobody");  // GET has no body: req.body is nullptr
        c.send_str("PUT /sum HTTP/1.1\r\nHost: t\r\nContent-Length: 5\r\n\r\nhello");
        auto r2 = c.read_response();  // second request on the same connection
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
        // The handler does not read the body; the driver must drain it before the connection can be reused
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
            c.send_str(make_pattern(1000));  // send only 1KB then disconnect
        }
        auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
        while (!g_disconnect_seen.load() && std::chrono::steady_clock::now() < deadline)
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        CHECK(g_disconnect_seen.load());  // contract 3: disconnect propagates as a body->read() exception
    });
}

TEST(http_driver_concurrent_shutdown) {
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        c.send_str("GET /slow HTTP/1.1\r\nHost: t\r\n\r\n");
        std::this_thread::sleep_for(std::chrono::milliseconds(100));  // request is in flight
        auto t0 = std::chrono::steady_clock::now();
        ts.srv->shutdown();
        auto r = c.read_response();  // contract 4: the in-flight request must complete
        CHECK(r.ok);
        CHECK_EQ(r.status, 200);
        CHECK_EQ(r.body, "done");
        c.close_now();
        ts.th.join();  // run() returns after the in-flight request completes
        auto elapsed = std::chrono::steady_clock::now() - t0;
        CHECK(elapsed < std::chrono::seconds(8));
    });
}

// ---------- Message boundaries (framing): request smuggling protection ----------
//
// RFC 9112 §6.1: requests with ambiguous boundaries must get a 400 or a closed connection. Two things are asserted uniformly across drivers:
//   1) the first response is not a success (an error status, or the connection simply closed);
//   2) the second request embedded in the attack payload is not answered as an independent request (no smuggling happened).
void check_framing_rejected(const std::string& driver, const std::string& raw) {
    TestServer ts(driver);
    Client c(ts.port);
    c.send_str(raw);
    auto r1 = c.read_response();
    // ok=false means the driver closed the connection outright, also an acceptable disposition
    if (r1.ok) CHECK(r1.status >= 400);
    // If the embedded GET /small were answered independently, it would get 200 + "nobody"
    auto r2 = c.read_response();
    CHECK(!(r2.ok && r2.status == 200 && r2.body == "nobody"));
}

TEST(http_driver_rejects_cl_te_conflict) {
    for_each_driver([](const std::string& d) {
        // CL.TE smuggling: when a front proxy frames by Content-Length while the backend frames by chunked,
        // "GET /small" would become the next request
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
        // stoull("-1") wraps to 2^64-1: the driver would "wait for the body forever", hanging the connection
        check_framing_rejected(d,
                               "POST /sum HTTP/1.1\r\nHost: t\r\nContent-Length: -1\r\n\r\n"
                               "GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
    });
}

TEST(http_driver_rejects_content_length_trailing_garbage) {
    for_each_driver([](const std::string& d) {
        // stoull("5abc") truncates to 5: the declaration and the actual framing diverge
        check_framing_rejected(d,
                               "POST /sum HTTP/1.1\r\nHost: t\r\nContent-Length: 5abc\r\n\r\n"
                               "GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
    });
}

TEST(http_driver_rejects_non_chunked_transfer_encoding) {
    for_each_driver([](const std::string& d) {
        // If "gzip, chunked" is not recognized as chunked, the body would be parsed as the next request line
        check_framing_rejected(d,
                               "POST /sum HTTP/1.1\r\nHost: t\r\n"
                               "Transfer-Encoding: gzip, chunked\r\n\r\n"
                               "0\r\n\r\nGET /small HTTP/1.1\r\nHost: t\r\n\r\n");
    });
}

TEST(http_driver_rejects_bad_chunk_size) {
    for_each_driver([](const std::string& d) {
        // Chunk size "-1": stoull accepts the minus sign, the body length escapes the declaration
        check_framing_rejected(d,
                               "POST /sum HTTP/1.1\r\nHost: t\r\n"
                               "Transfer-Encoding: chunked\r\n\r\n"
                               "-1\r\nxx\r\n0\r\n\r\n"
                               "GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
    });
}

TEST(http_driver_rejects_missing_crlf_after_chunk) {
    for_each_driver([](const std::string& d) {
        // After the chunk data comes not CRLF but "hex-looking" garbage: it must not be treated as the next chunk size
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
        // The backend delivers only half the declared length: the driver must disconnect, otherwise the client would treat the next
        // response head as the remainder of this body (response misalignment)
        c.send_str("GET /short?size=100000 HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r = c.read_response();
        CHECK(!r.ok);  // the declared byte count cannot be read in full, the connection is closed
        // The connection is indeed unusable: subsequent requests get no response
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
        // Driver-managed headers may appear only once: a duplicate Content-Length is a framing hole
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
        // CR/LF values (a response-splitting injection surface) and illegal header names are dropped entirely, normal headers kept
        CHECK(!r.header("Injected"));
        CHECK(!r.header("X-Evil"));
        CHECK(!r.header("Bad Name"));
        CHECK_EQ(r.header("X-Fine").value_or(""), "ok");
        CHECK_EQ(r.body, "ok");
    });
}

// ---------- pushpull: the shared push-to-pull component ----------

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
    std::this_thread::sleep_for(std::chrono::milliseconds(50));  // make sure it is blocked in pop
    CHECK(!done.load());
    q->cancel();  // must also wake the pop side, otherwise the consumer blocks forever
    auto deadline = std::chrono::steady_clock::now() + std::chrono::seconds(5);
    while (!done.load() && std::chrono::steady_clock::now() < deadline)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    consumer.join();
    CHECK(done.load());
    CHECK(threw.load());  // cancel is not a normal EOF, it propagates as an exception
}

TEST(block_queue_normal_eof_still_returns_zero) {
    auto q = std::make_shared<BlockQueue>(64 * 1024);
    CHECK(q->push("hello", 5));
    q->close(true);
    std::byte buf[16];
    CHECK_EQ(q->pop(std::span(buf)), size_t(5));
    CHECK_EQ(q->pop(std::span(buf)), size_t(0));  // normal EOF
    // cancel after close (the usual order in the consumer's destructor) must not turn EOF into an exception
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
        // HEAD sends no body: the next request should run directly on the connection
        c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r2 = c.read_response();
        CHECK(r2.ok);
        CHECK_EQ(r2.body, "nobody");
    });
}

// ---------- Driver consistency (gaps §3.9) ----------

TEST(http_driver_connection_close_token_list) {
    // Connection: close, Upgrade -- a legal token list. An exact-equality comparison would miss the close, so the
    // server keeps reusing the connection while the client is already about to close it
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        c.send_str("GET /small HTTP/1.1\r\nHost: t\r\nConnection: close, Upgrade\r\n\r\n");
        auto r = c.read_response();
        CHECK(r.ok);
        CHECK_EQ(r.status, 200);
        bool close_sent = false;
        for (auto& [k, v] : r.headers)
            if (HeaderMap::ieq(k, "Connection") && HeaderMap::ieq(v, "close")) close_sent = true;
        CHECK(close_sent);
    });
}

TEST(http_driver_unknown_method_forwarded_or_s3_xml) {
    // An unknown method has only two legitimate outcomes: handed verbatim to the handler for L2 to judge (builtin/beast/
    // seastar), or rejected by the driver but with **S3 XML** (httplib cannot route unregistered methods and previously
    // replied upstream with its own message, breaking four-driver consistency)
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        c.send_str("BREW /method HTTP/1.1\r\nHost: t\r\n\r\n");
        auto r = c.read_response();
        CHECK(r.ok);
        if (r.status < 400) {
            CHECK_EQ(r.body, "BREW");  // forwarded to the handler
        } else {
            CHECK(r.body.find("<Error>") != std::string::npos);
            CHECK(r.body.find("<Code>") != std::string::npos);
        }
    });
}

TEST(http_driver_rejects_oversized_headers) {
    // http.max_header_size: httplib previously ignored this setting entirely, making the four drivers' acceptance sets inconsistent
    for_each_driver([](const std::string& d) {
        TestServer ts(d);
        Client c(ts.port);
        std::string big(64 * 1024, 'x');
        c.send_str("GET /small HTTP/1.1\r\nHost: t\r\nX-Pad: " + big + "\r\n\r\n");
        auto r = c.read_response();
        // Either reject with an error status or disconnect outright -- neither may be treated as a normal request
        CHECK(!r.ok || r.status >= 400);
    });
}


// ---------- TLS（docs/archive/gaps.md §7）----------
// Self-signed test certificate (CN=localhost, SAN includes 127.0.0.1, valid until 2126 -- embedded in source so the
// tests have zero external dependencies; the client does no validation, so apart from expiry the certificate content does not matter)
constexpr const char* kTestTlsCert =
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDJzCCAg+gAwIBAgIUQoKpxX6iKmcQaP/a3uSLGHSVf0MwDQYJKoZIhvcNAQEL\n"
    "BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MCAXDTI2MDgxMTE1MTAxM1oYDzIxMjYw\n"
    "NzE4MTUxMDEzWjAUMRIwEAYDVQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEB\n"
    "AQUAA4IBDwAwggEKAoIBAQDGBT8CR7Rorh2TnTDRGaBWb5eJUZmQaB2ahQyn5DWM\n"
    "Mx3zf8OwdTwv+cc1IcA++HufUuCf7OclMVgVITpXEg6smktNa8FFgWoUGLEuWAu4\n"
    "yBHf4/MTlPZAGf5H/KXFXvTZp2BXK5sCgKXLy8jFY8/dcUkNXM5GHqapI6RD4UoS\n"
    "t/bQ2+zELHbQmrK2bCM+yzcVFoDfczvCWiOcnzLJpb6SAJICMhlOS33ad2VZy0A0\n"
    "skBipCuvyCMXklJj9T5YccOZZV4zfOERmp5U2Y1yT1B0GJ+PkAKj9AtPzlxv8L+l\n"
    "l79pnpHzAt7NnkbFcH8KvF7GJjV8xI43ZXQEewel4x95AgMBAAGjbzBtMB0GA1Ud\n"
    "DgQWBBQ6+g5anGZA+PW5Fu+sH5cWeUP9uzAfBgNVHSMEGDAWgBQ6+g5anGZA+PW5\n"
    "Fu+sH5cWeUP9uzAPBgNVHRMBAf8EBTADAQH/MBoGA1UdEQQTMBGCCWxvY2FsaG9z\n"
    "dIcEfwAAATANBgkqhkiG9w0BAQsFAAOCAQEAINxixmt6ntEAdRZuhKhxmeTDrI01\n"
    "10D1pc31g5RLD6Jsmi8MFEAB/Nf102Y1WxSmcYL3LwpsGjLhUeWkhHruFg4mP/Qg\n"
    "fpd3ckOwPx6IEEp8oFWhJ6QzKFapMFY+ljBe/qFSpR9n3yYyh8y4B8qMNIRHsv5c\n"
    "/Iok6dMtoAmdpveqK/KeXx1ilMAa5d49YPOtvrmoYeQCs6uLzxOtrGnQM3RuUUWd\n"
    "/HwLopGiliKwaADD0tPN/xcHguaOnqPeKVi+5+2xmK4O1dwis1D3em9YcWu2vYq6\n"
    "/6qRLDrGZSgFFGCMxmlS7DDhXF4IZYWEPXwm5+YsggIunzZ9yPffbGxxRQ==\n"
    "-----END CERTIFICATE-----\n";

constexpr const char* kTestTlsKey =
    "-----BEGIN PRIVATE KEY-----\n"
    "MIIEvwIBADANBgkqhkiG9w0BAQEFAASCBKkwggSlAgEAAoIBAQDGBT8CR7Rorh2T\n"
    "nTDRGaBWb5eJUZmQaB2ahQyn5DWMMx3zf8OwdTwv+cc1IcA++HufUuCf7OclMVgV\n"
    "ITpXEg6smktNa8FFgWoUGLEuWAu4yBHf4/MTlPZAGf5H/KXFXvTZp2BXK5sCgKXL\n"
    "y8jFY8/dcUkNXM5GHqapI6RD4UoSt/bQ2+zELHbQmrK2bCM+yzcVFoDfczvCWiOc\n"
    "nzLJpb6SAJICMhlOS33ad2VZy0A0skBipCuvyCMXklJj9T5YccOZZV4zfOERmp5U\n"
    "2Y1yT1B0GJ+PkAKj9AtPzlxv8L+ll79pnpHzAt7NnkbFcH8KvF7GJjV8xI43ZXQE\n"
    "ewel4x95AgMBAAECggEABzwxFeGzPOloCtO7xpdpporX1gm5bd+DUw5g3LVfrakU\n"
    "TghZU4YpuNoxngEnEO3w4gyr83qbOfPJo1EA3JD8ILCXsdhvGf2SjTGB73QXroyp\n"
    "1tzmwwyxQCeVpRadOHRMcjOVl1hwUW5hpcFNq0T8YZu2Zs12UHUlff5/mEbVTJB5\n"
    "WePlN/Y9gTtHjvVG/bVqw5zpA7a63SyNmN7vfi7N8ZRASRkX9FziaKxBKOqmIKV+\n"
    "nLCesfQw5Ddyv3y7Flgv5FCdXhOdIkhuxJUD6MxuvJNRRND2ABzRWnaq0J5e+Bmx\n"
    "luMtuFSh4rLmsansw1iQXtnlUI8R4Y3hD/ppig87MQKBgQD+YFOo5oO7P/fM1OQz\n"
    "Gq+SP/A4As+pDbxUhRZlUi+hT1wBVpc42HQy/LtV/MTNoT9XA5yck5le6vVShpur\n"
    "+5OVSCYFTfqyk1na7K1sl406XuaUWN2hjeQQfFc+FQL2qqVQMWHyHnap7Az8+tJX\n"
    "KQIq0sEG5u6H3xhANIBnv2hxEQKBgQDHSNQ7D6FnbO8Fu4Y5WVq8qCzi01IRe+6S\n"
    "lavNrBm56vy1ezN9SKqiDKeuLo/oFlepfHnoKr958trFOPy6/CJZyzesvy7wUFrV\n"
    "v9EqtKcdT8xt52KfMWNuVZOpE2QJJ+u6VBkniJGC2YudnFlJdmrUl4JMEzIYCwXC\n"
    "xB6CQPDH6QKBgQC9GEJYljNq6Rx+aevRiY7mex1Jpd1U4F8VvXFulG/PzDyqygHU\n"
    "QiPvGyzvuN1btvhs6MRtKNOkWalQVbw3VubY3C9XViZ8xUjQk4w/41EbCR0DPiRT\n"
    "SjU1hBkej2QKlcQaHvuejsLLgiwNiy79mACCcPUI+nZrDo7qe5zQgttS4QKBgQCc\n"
    "dgOaszTnvNEU0Rwa3pqsz+Ud2QfwDjtK/xO6EMrJ+0KZQbc1P94oCIOF76ywbQo6\n"
    "WS5lJ1rZ5d/5RDq4m8hkc3asvBWgO5Z1h3oza05hZwt7plT5447LS4j5D+5UefFL\n"
    "g0eUkFaeQyqofd5kHQLXEnUMQW3tDophVhUV8uKYMQKBgQCE0KN7o/QYvQEKdGlW\n"
    "rSk7y23+MVZyUoiChKI1Vukw8cGBtWtOWWVGNsRI9rQcR1L7RQlzuhB3/I42Efks\n"
    "+r0mcaH9yNajNK0JaryQgwe53IuYkY+3vlDr2Pily/ey+JZ7yBYqvjUkjW/25NHD\n"
    "dfKnXoouOV+cpdjDRSyNsyESRA==\n"
    "-----END PRIVATE KEY-----\n";

struct TlsCertFiles {
    std::string cert_path, key_path;
    TlsCertFiles() {
        char tmpl[] = "/tmp/lights3-tls-XXXXXX";
        CHECK(mkdtemp(tmpl) != nullptr);
        dir = tmpl;
        cert_path = dir + "/cert.pem";
        key_path = dir + "/key.pem";
        write_file(cert_path, kTestTlsCert);
        write_file(key_path, kTestTlsKey);
    }
    ~TlsCertFiles() {
        ::unlink(cert_path.c_str());
        ::unlink(key_path.c_str());
        ::rmdir(dir.c_str());
    }
    static void write_file(const std::string& p, const char* content) {
        FILE* f = fopen(p.c_str(), "w");
        CHECK(f != nullptr);
        fputs(content, f);
        fclose(f);
    }
    std::string dir;
};

// Minimal TLS client (direct OpenSSL, no certificate validation): the project already links libssl,
// so HTTPS can be verified end to end without extra dependencies
struct TlsClient {
    int fd = -1;
    SSL_CTX* ctx = nullptr;
    SSL* ssl = nullptr;

    explicit TlsClient(uint16_t port) {
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
        ctx = SSL_CTX_new(TLS_client_method());
        CHECK(ctx != nullptr);
        ssl = SSL_new(ctx);
        CHECK(ssl != nullptr);
        SSL_set_fd(ssl, fd);
        CHECK(SSL_connect(ssl) == 1);
    }
    ~TlsClient() {
        if (ssl) {
            SSL_shutdown(ssl);
            SSL_free(ssl);
        }
        if (ctx) SSL_CTX_free(ctx);
        if (fd >= 0) ::close(fd);
    }

    void send_str(std::string_view s) { CHECK(SSL_write(ssl, s.data(), (int)s.size()) > 0); }

    // Read the whole response (Connection: close scenario): until the peer closes
    std::string read_all() {
        std::string out;
        char buf[4096];
        for (;;) {
            int n = SSL_read(ssl, buf, sizeof(buf));
            if (n <= 0) break;
            out.append(buf, n);
        }
        return out;
    }
};

// The drivers close the socket before their completion handler bumps the counter,
// so the client can observe the close first: wait briefly for the counter to land
template <class Pred>
bool eventually(Pred&& pred, int max_ms = 3000) {
    for (int i = 0; i < max_ms / 20; ++i) {
        if (pred()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    return pred();
}

TEST(http_driver_tls_round_trip) {
    // Every OpenSSL-backed driver serves TLS (roadmap §4.1); seastar goes through
    // seastar::tls and is covered by its own build. The knobs/SNI/reload cases live in test_tls.cc
    TlsCertFiles certs;
    auto drivers = HttpServerFactory::drivers();
    for (auto& d : drivers) {
        try {
            TestServer ts(d, certs.cert_path, certs.key_path);
            {
                TlsClient c(ts.port);
                c.send_str("GET /small HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
                std::string r = c.read_all();
                CHECK(r.find("200") != std::string::npos);
                CHECK(r.find("nobody") != std::string::npos);
            }
            {
                // Run a PUT with a body too: streaming reads under the TLS record layer hold as well
                TlsClient c(ts.port);
                std::string payload = make_pattern(1024);
                c.send_str("PUT /sum HTTP/1.1\r\nHost: t\r\nContent-Length: 1024\r\n"
                           "Connection: close\r\n\r\n" +
                           payload);
                std::string r = c.read_all();
                CHECK(r.find(expected_sum(1024)) != std::string::npos);
            }
        } catch (const mini_test::Failure& f) {
            throw mini_test::Failure("[driver=" + d + "] " + f.what());
        }
    }
}

TEST(http_driver_tls_file_body_falls_back_to_read) {
    TlsCertFiles certs;
    for (auto& d : HttpServerFactory::drivers()) {
        if (d == "seastar") continue;  // seastar::tls is covered by its own build
        try {
            TestServer ts(d, certs.cert_path, certs.key_path);
            g_file_bytes_sent = 0;
            g_file_bytes_read = 0;
            TlsClient c(ts.port);
            c.send_str("GET /file?size=300000 HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
            std::string r = c.read_all();
            auto body_at = r.find("\r\n\r\n");
            CHECK(body_at != std::string::npos);
            CHECK(r.substr(body_at + 4) == make_pattern(300000));
            CHECK_EQ(g_file_bytes_sent.load(), uint64_t{0});
            CHECK_EQ(g_file_bytes_read.load(), uint64_t{300000});
        } catch (const mini_test::Failure& f) {
            throw mini_test::Failure("[driver=" + d + "] " + f.what());
        }
    }
}

TEST(http_driver_tls_plaintext_client_rejected) {
    // A plaintext client hitting the TLS port: handshake fails and the connection closes, it must never answer as plaintext HTTP
    TlsCertFiles certs;
    for (auto& d : HttpServerFactory::drivers()) {
        TestServer ts(d, certs.cert_path, certs.key_path);
        {
            Client c(ts.port);
            c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
            auto r = c.read_response();
            CHECK(!r.ok);  // only a disconnect (or TLS alert noise) is possible, never an HTTP 200
        }
        // roadmap §5.3: the failed handshake is counted (OpenSSL-backed drivers with
        // their own accept loop; httplib and seastar handshake inside upstream)
        if (d == "builtin" || d == "beast") {
            try {
                CHECK(eventually([&] { return ts.srv->stats().tls_handshakes_failed == 1; }));
                CHECK_EQ(ts.srv->stats().tls_handshakes_ok, uint64_t(0));
                TlsClient ok(ts.port);
                ok.send_str("GET /small HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
                CHECK(ok.read_all().find("200") != std::string::npos);
                CHECK(eventually([&] { return ts.srv->stats().tls_handshakes_ok == 1; }));
            } catch (const mini_test::Failure& f) {
                throw mini_test::Failure("[driver=" + d + "] " + f.what());
            }
        }
    }
}

// roadmap §5.3: requests parsed at L1 (÷ accepted = keep-alive reuse) and malformed
// requests — request line, header block, message framing — across the drivers
TEST(http_driver_request_and_parse_error_counters) {
    for (auto& d : HttpServerFactory::drivers()) {
        try {
            TestServer ts(d);
            {
                Client c(ts.port);  // two requests over one keep-alive connection
                c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
                CHECK(c.read_response().ok);
                c.send_str("GET /small HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
                CHECK(c.read_response().ok);
            }
            CHECK(eventually([&] { return ts.srv->stats().requests == 2; }));
            if (d != "httplib") CHECK_EQ(ts.srv->stats().accepted, uint64_t(1));
            CHECK_EQ(ts.srv->stats().parse_errors, uint64_t(0));
            {
                Client c(ts.port);  // no request line structure at all
                c.send_str("GARBAGE\r\n\r\n");
                auto r = c.read_response();
                CHECK(!r.ok || r.status >= 400);
            }
            CHECK(eventually([&] { return ts.srv->stats().parse_errors >= 1; }));
            {
                Client c(ts.port);  // framing rejected by the shared validator: 400 + close
                c.send_str("PUT /sum HTTP/1.1\r\nHost: t\r\nContent-Length: abc\r\n\r\n");
                auto r = c.read_response();
                CHECK(!r.ok || r.status == 400);
            }
            CHECK(eventually([&] { return ts.srv->stats().parse_errors >= 2; }));
            CHECK_EQ(ts.srv->stats().requests, uint64_t(2));  // malformed ones are not requests
        } catch (const mini_test::Failure& f) {
            throw mini_test::Failure("[driver=" + d + "] " + f.what());
        }
    }
}

TEST(http_driver_tls_seastar_rejects_sni) {
    // seastar's credentials carry one certificate per listener (docs/tls.md §4): a
    // tls_sni list must fail at construction rather than serve the wrong certificate
    TlsCertFiles certs;
    for (auto& d : HttpServerFactory::drivers()) {
        if (d != "seastar") continue;
        HttpConfig cfg;
        cfg.driver = d;
        cfg.tls_cert = certs.cert_path;
        cfg.tls_key = certs.key_path;
        cfg.tls_sni.push_back({"a.example", certs.cert_path, certs.key_path});
        bool threw = false;
        try {
            HttpServerFactory::create(d, cfg);
        } catch (const std::exception&) {
            threw = true;
        }
        CHECK(threw);
    }
}

TEST(http_driver_tls_bad_cert_throws) {
    // A bad certificate path must throw at construction/startup, not be discovered at the first connection
    for (auto& d : HttpServerFactory::drivers()) {
        HttpConfig cfg;
        cfg.driver = d;
        cfg.tls_cert = "/nonexistent/cert.pem";
        cfg.tls_key = "/nonexistent/key.pem";
        bool threw = false;
        try {
            HttpServerFactory::create(d, cfg);
        } catch (const std::exception&) {
            threw = true;
        }
        CHECK(threw);
    }
}

// ---------- Timeouts / connection limit / shutdown contract (config.h timeout knobs section / http-adapter.md §5,
// docs/archive/issues.md T10): each driver implements these behaviors on its own, most prone to divergence, must be asserted across all four ----------

TEST(http_driver_idle_timeout_closes_idle_connection) {
    // An idle connection (keep-alive that has completed a request) must be closed by the server after idle_timeout;
    // a connection that never sends data is also covered by it (the stall guard only handles bodies in transfer)
    for (auto& d : HttpServerFactory::drivers()) {
        try {
            TestServer ts(d, [](HttpConfig& c) { c.idle_timeout_sec = 1; });
            Client c(ts.port);
            c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
            CHECK(c.read_response().ok);  // keep-alive connection established
            auto t0 = std::chrono::steady_clock::now();
            char b;
            ssize_t n = ::recv(c.fd, &b, 1, 0);  // block waiting for the peer to close (SO_RCVTIMEO=10s as backstop)
            CHECK_EQ(n, ssize_t{0});             // 0=peer closed; -1=the driver never closed (timeout)
            CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::seconds(8));
        } catch (const mini_test::Failure& f) {
            throw mini_test::Failure("[driver=" + d + "] " + f.what());
        }
    }
}

// ---------- Timeout family / keep-alive budget / connection counters (roadmap §4.2, http-adapter.md §2.1) ----------

TEST(http_driver_header_timeout_bounds_slow_headers) {
    // A fresh connection that never completes its request line/headers is cut by
    // header_timeout, independently of the (much longer) idle/body timeouts.
    // httplib bounds headers with body_timeout instead (upstream has one read
    // timeout), so it gets that knob set low
    for (auto& d : HttpServerFactory::drivers()) {
        try {
            TestServer ts(d, [&](HttpConfig& c) {
                c.header_timeout_sec = 1;
                c.body_timeout_sec = d == "httplib" ? 1 : 30;
                c.idle_timeout_sec = 30;
                c.write_timeout_sec = 30;
            });
            Client c(ts.port);
            c.send_str("GET /small HTTP/1.1\r\nHost: t\r\nX-Slow: ");  // headers never finish
            auto t0 = std::chrono::steady_clock::now();
            auto r = c.read_response();  // a plain close, or httplib's 408 — never a success
            CHECK(!r.ok || r.status >= 400);
            CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::seconds(8));
            if (d != "httplib") {
                CHECK(eventually([&] { return ts.srv->stats().timeouts_header == 1; }));
                CHECK_EQ(ts.srv->stats().timeouts_idle, uint64_t(0));
            }
        } catch (const mini_test::Failure& f) {
            throw mini_test::Failure("[driver=" + d + "] " + f.what());
        }
    }
}

TEST(http_driver_body_timeout_bounds_stalled_upload) {
    // Headers arrive, the declared body never does: body_timeout ends it while
    // header/idle timeouts stay long
    for (auto& d : HttpServerFactory::drivers()) {
        try {
            TestServer ts(d, [](HttpConfig& c) {
                c.body_timeout_sec = 1;
                c.header_timeout_sec = 30;
                c.idle_timeout_sec = 30;
                c.write_timeout_sec = 30;
                c.transfer_stall_timeout_sec = 0;
            });
            Client c(ts.port);
            c.send_str("PUT /sum HTTP/1.1\r\nHost: t\r\nContent-Length: 100\r\n\r\nabc");
            auto t0 = std::chrono::steady_clock::now();
            auto r = c.read_response();  // either an error response or a plain close
            CHECK(!r.ok || r.status >= 400);
            CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::seconds(8));
            if (d != "httplib") {
                CHECK(eventually([&] { return ts.srv->stats().timeouts_body == 1; }));
                CHECK_EQ(ts.srv->stats().timeouts_header, uint64_t(0));
            }
        } catch (const mini_test::Failure& f) {
            throw mini_test::Failure("[driver=" + d + "] " + f.what());
        }
    }
}

TEST(http_driver_keepalive_request_budget) {
    // max_requests_per_connection=2: the second response announces Connection: close
    // and the connection is closed afterwards; a fresh connection starts a new budget
    for (auto& d : HttpServerFactory::drivers()) {
        try {
            TestServer ts(d, [](HttpConfig& c) { c.max_requests_per_connection = 2; });
            Client c(ts.port);
            c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
            auto r1 = c.read_response();
            CHECK(r1.ok);
            CHECK(!r1.header("Connection") || !HeaderMap::ieq(*r1.header("Connection"), "close"));
            c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
            auto r2 = c.read_response();
            CHECK(r2.ok);
            CHECK(r2.header("Connection") && HeaderMap::ieq(*r2.header("Connection"), "close"));
            c.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
            CHECK(!c.read_response().ok);  // budget spent: closed
            Client c2(ts.port);
            c2.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
            CHECK(c2.read_response().ok);
            if (d != "httplib") CHECK(eventually([&] { return ts.srv->stats().keepalive_closes == 1; }));
        } catch (const mini_test::Failure& f) {
            throw mini_test::Failure("[driver=" + d + "] " + f.what());
        }
    }
}

TEST(http_driver_connection_counters) {
    // accepted / active / rejected_limit / idle timeouts through IHttpServer::stats()
    // (httplib runs upstream's accept loop and exposes none of these)
    for (auto& d : HttpServerFactory::drivers()) {
        if (d == "httplib") continue;
        try {
            // max_connections=2 like the limit test above: seastar's resident engine keeps
            // the smp it was first started with, and the cap is apportioned per shard
            TestServer ts(d, [](HttpConfig& c) {
                c.max_connections = 2;
                c.io_threads = 1;
                c.idle_timeout_sec = 1;
            });
            Client a(ts.port), b(ts.port);
            a.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
            CHECK(a.read_response().ok);
            b.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
            CHECK(b.read_response().ok);
            CHECK_EQ(ts.srv->stats().accepted, uint64_t(2));
            CHECK_EQ(ts.srv->stats().active, uint64_t(2));
            Client c3(ts.port);
            c3.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
            CHECK(!c3.read_response().ok);
            CHECK(eventually([&] { return ts.srv->stats().rejected_limit == 1; }));
            // The idle keep-alive connections time out and the active gauge drops
            char ch;
            CHECK_EQ(::recv(a.fd, &ch, 1, 0), ssize_t{0});
            CHECK(eventually([&] { return ts.srv->stats().active == 0; }));
            CHECK(eventually([&] { return ts.srv->stats().timeouts_idle == 2; }));
        } catch (const mini_test::Failure& f) {
            throw mini_test::Failure("[driver=" + d + "] " + f.what());
        }
    }
}

TEST(http_driver_max_connections_rejects_excess) {
    // Over the limit, new connections are refused (builtin/beast close, seastar discards), established ones are unaffected.
    // httplib's limit is implicitly constrained by its thread pool (as the config.h comment states), different semantics, skipped
    for (auto& d : HttpServerFactory::drivers()) {
        if (d == "httplib") continue;
        try {
            // seastar splits per shard: io_threads=1 ensures the single-shard limit is the global limit
            TestServer ts(d, [](HttpConfig& c) {
                c.max_connections = 2;
                c.io_threads = 1;
            });
            Client a(ts.port), b(ts.port);
            a.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
            CHECK(a.read_response().ok);
            b.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
            CHECK(b.read_response().ok);  // two keep-alive connections fill the limit
            Client c3(ts.port);           // the TCP three-way handshake still succeeds at the backlog layer
            c3.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
            CHECK(!c3.read_response().ok);  // the third must get no response (closed/discarded)
            // Established connections are unaffected
            a.send_str("GET /small HTTP/1.1\r\nHost: t\r\n\r\n");
            CHECK(a.read_response().ok);
        } catch (const mini_test::Failure& f) {
            throw mini_test::Failure("[driver=" + d + "] " + f.what());
        }
    }
}

TEST(http_driver_shutdown_waits_for_inflight_within_grace) {
    // First half of http-adapter.md contract 4: for an in-flight request finishing within the grace period, shutdown waits for it
    // and run() returns only after the response is fully delivered -- in-flight requests must not be strangled
    for (auto& d : HttpServerFactory::drivers()) {
        try {
            TestServer ts(d, [](HttpConfig& c) { c.shutdown_grace_sec = 5; });
            Client c(ts.port);
            c.send_str("GET /slow?ms=600 HTTP/1.1\r\nHost: t\r\nConnection: close\r\n\r\n");
            std::this_thread::sleep_for(std::chrono::milliseconds(150));  // let the request enter the handler
            ts.stop();  // shutdown + join run()
            auto r = c.read_response();
            CHECK(r.ok);
            CHECK_EQ(r.body, "done");
        } catch (const mini_test::Failure& f) {
            throw mini_test::Failure("[driver=" + d + "] " + f.what());
        }
    }
}

TEST(http_driver_shutdown_grace_bounds_return) {
    // Second half of contract 4: "or timeout" -- an in-flight request exceeding the grace period is force-closed, and run() is not
    // dragged out indefinitely by it. The handler (a blocking 2.2s sleep, unwakeable within the 1s grace) finishes on its own within
    // the force window, and run() returns right after: the upper bound is far below the force backstop (1+3=4s), let alone unbounded.
    // The lower bound confirms the grace period really exists -- not strangling in-flight requests and returning immediately
    for (auto& d : HttpServerFactory::drivers()) {
        try {
            TestServer ts(d, [](HttpConfig& c) {
                c.shutdown_grace_sec = 1;
                c.shutdown_force_wait_sec = 3;
            });
            Client c(ts.port);
            c.send_str("GET /slow?ms=2200 HTTP/1.1\r\nHost: t\r\n\r\n");
            std::this_thread::sleep_for(std::chrono::milliseconds(150));
            auto t0 = std::chrono::steady_clock::now();
            ts.stop();
            auto took = std::chrono::steady_clock::now() - t0;
            CHECK(took >= std::chrono::milliseconds(700));
            CHECK(took < std::chrono::milliseconds(3300));
        } catch (const mini_test::Failure& f) {
            throw mini_test::Failure("[driver=" + d + "] " + f.what());
        }
    }
}

TEST(http_driver_shutdown_force_deadline_builtin) {
    // The strictest shape: the handler sleeps through the whole grace+force window (4s > 1+1+margin), run() must still return
    // on time. builtin only -- it has an explicit design for "leftover threads hold shared state via shared_ptr and clean up on
    // their own after run() returns or even after server destruction"; the force-close paths of beast/seastar abandon
    // unfinished session coroutine frames (a bounded leak in the process-exit scenario), not reproducibly executable under ASan
    TestServer ts("builtin", [](HttpConfig& c) {
        c.shutdown_grace_sec = 1;
        c.shutdown_force_wait_sec = 1;
    });
    Client c(ts.port);
    c.send_str("GET /slow?ms=4000 HTTP/1.1\r\nHost: t\r\n\r\n");
    std::this_thread::sleep_for(std::chrono::milliseconds(150));
    auto t0 = std::chrono::steady_clock::now();
    ts.stop();
    auto took = std::chrono::steady_clock::now() - t0;
    CHECK(took < std::chrono::milliseconds(3500));  // did not wait out the handler's 4s sleep
    c.close_now();
    // Let the leftover handler finish sleeping before the case exits: no race with process teardown
    std::this_thread::sleep_for(std::chrono::milliseconds(4200));
}
