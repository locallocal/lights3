// roadmap §5.2: the logging system — text/JSON access lines through the full
// dispatch (quoted path, remote/bucket/UA/TTFB slots, end-of-stream emission with
// the bytes actually sent), the slow-request WARN channel, the JSON envelope of
// ordinary log lines, and the async + rotating-file sink
#include <spdlog/sinks/ostream_sink.h>

#include <chrono>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <sstream>
#include <thread>

#include "core/config.h"
#include "core/log.h"
#include "core/task.h"
#include "core/metrics.h"
#include "s3/service.h"
#include "storage/memory/memory_backend.h"
#include "storage/metered_backend.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::s3;

namespace {

bool contains(const std::string& s, const std::string& sub) { return s.find(sub) != std::string::npos; }

// Captures every rendered line; restores the stderr logger on scope exit so the
// remaining tests do not write into a destroyed stream
struct Capture {
    std::ostringstream out;
    std::shared_ptr<spdlog::sinks::ostream_sink_mt> sink =
        std::make_shared<spdlog::sinks::ostream_sink_mt>(out);
    explicit Capture(const std::string& format) {
        LogConfig cfg;
        cfg.format = format;
        Logger::init(cfg, sink);
    }
    ~Capture() { Logger::init(LogConfig{}); }
    // Lines in emission order
    std::vector<std::string> lines() {
        std::vector<std::string> v;
        std::istringstream in(out.str());
        for (std::string l; std::getline(in, l);)
            if (!l.empty()) v.push_back(l);
        return v;
    }
    std::string last_access() {
        auto v = lines();
        for (auto it = v.rbegin(); it != v.rend(); ++it)
            if (contains(*it, "access")) return *it;
        return {};
    }
};

// Routed through the metering decorator like the real assembly, so the backend
// slots (calls / ms) of the line are populated
S3Service make_service() {
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends;
    backends["mem"] = std::make_shared<storage::MeteredBackend>(
        "mem", std::make_shared<storage::MemoryBackend>(), std::make_shared<MetricsRegistry>());
    BucketsConfig cfg;
    cfg.default_backend = "mem";
    return S3Service(storage::BucketRouter::build(cfg, std::move(backends)),
                     SigV4Authenticator::build(AuthConfig{}));
}

http::HttpRequest make_req(std::string method, std::string path, std::string body = "") {
    http::HttpRequest req;
    req.method = std::move(method);
    req.raw_path = path;
    req.path = std::move(path);
    req.headers.add("Host", "localhost");
    req.headers.add("Content-Length", std::to_string(body.size()));
    req.headers.add("User-Agent", "aws-cli/2.0 \"quoted\"");
    req.remote_addr = "10.0.0.7";
    if (!body.empty()) req.body = std::make_unique<http::StringBodyReader>(std::move(body));
    return req;
}

// A request body whose first read stalls: makes the handler stage measurably slow
class SlowBodyReader final : public http::BodyReader {
public:
    explicit SlowBodyReader(std::string data) : inner_(std::move(data)) {}
    Task<size_t> read(std::span<std::byte> buf) override {
        if (!slept_) {
            slept_ = true;
            std::this_thread::sleep_for(std::chrono::milliseconds(30));
        }
        co_return co_await inner_.read(buf);
    }
    std::optional<uint64_t> length() const override { return inner_.length(); }

private:
    http::StringBodyReader inner_;
    bool slept_ = false;
};

// Drains a streaming body the way a driver does, stopping early after `stop_after` bytes
Task<uint64_t> drain(http::BodyReader& body, uint64_t stop_after = UINT64_MAX) {
    std::byte buf[4096];
    uint64_t total = 0;
    while (total < stop_after) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        total += n;
    }
    co_return total;
}

}  // namespace

TEST(access_log_text_line_fields) {
    Capture cap("text");
    auto svc = make_service();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    // A key with a space and a quote: the historical unquoted path broke field splitting
    auto resp = sync_wait(svc.dispatch(make_req("PUT", "/bkt/a b\"c.txt", "hello")));
    CHECK_EQ(resp.status, 200);
    std::string line = cap.last_access();
    // Level token per the text pattern's %-5!l: "info " padded, "warning" truncated to "warni"
    CHECK(contains(line, "Z info  access "));
    CHECK(contains(line, " PUT \"/bkt/a b\\\"c.txt\" 200 0 "));
    CHECK(contains(line, "ms api=PutObject backend=mem:"));
    CHECK(contains(line, " remote=10.0.0.7 bucket=bkt ttfb="));
    CHECK(contains(line, " ua=\"aws-cli/2.0 \\\"quoted\\\"\""));
    CHECK(!contains(line, "slow=1"));
    CHECK(!contains(line, "truncated"));
    // Streaming response: the line is emitted at end of body with the bytes sent
    // and, after the driver drained it, a total >= ttfb
    resp = sync_wait(svc.dispatch(make_req("GET", "/bkt/a b\"c.txt")));
    CHECK_EQ(resp.status, 200);
    CHECK(resp.stream_body != nullptr);
    CHECK(!contains(cap.last_access(), " GET "));  // not yet: body still pending
    CHECK_EQ(sync_wait(drain(*resp.stream_body)), uint64_t(5));
    line = cap.last_access();
    CHECK(contains(line, " GET \"/bkt/a b\\\"c.txt\" 200 5 "));
    CHECK(contains(line, "api=GetObject backend=mem:"));
    CHECK(!contains(line, "truncated"));
    size_t before = cap.lines().size();
    resp.stream_body.reset();  // already emitted: destroying the reader adds nothing
    CHECK_EQ(cap.lines().size(), before);
}

TEST(access_log_truncated_stream) {
    Capture cap("text");
    auto svc = make_service();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    std::string big(10000, 'x');
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/big", big)));
    auto resp = sync_wait(svc.dispatch(make_req("GET", "/bkt/big")));
    CHECK(resp.stream_body != nullptr);
    // The client goes away after the first chunk: the driver drops the body
    CHECK_EQ(sync_wait(drain(*resp.stream_body, 1)), uint64_t(4096));
    resp.stream_body.reset();
    std::string line = cap.last_access();
    CHECK(contains(line, " GET \"/bkt/big\" 200 4096 "));
    CHECK(contains(line, " truncated=1"));
}

TEST(access_log_slow_request_warn) {
    Capture cap("text");
    auto svc = make_service();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    svc.set_slow_request_threshold(std::chrono::milliseconds(20));
    auto fast = sync_wait(svc.dispatch(make_req("PUT", "/bkt/fast", "x")));
    CHECK_EQ(fast.status, 200);
    CHECK(contains(cap.last_access(), "Z info  access "));
    auto req = make_req("PUT", "/bkt/slow");
    req.headers.set("Content-Length", "5");
    req.body = std::make_unique<SlowBodyReader>("hello");
    auto slow = sync_wait(svc.dispatch(std::move(req)));
    CHECK_EQ(slow.status, 200);
    std::string line = cap.last_access();
    CHECK(contains(line, "Z warni access "));
    CHECK(contains(line, " PUT \"/bkt/slow\" 200 "));
    CHECK(contains(line, " slow=1 auth="));
    CHECK(contains(line, "ms handler="));
    CHECK(contains(line, "ms backend_calls="));
    // Off again: the same slow request is an ordinary INFO line
    svc.set_slow_request_threshold(std::chrono::milliseconds(0));
    req = make_req("PUT", "/bkt/slow2");
    req.headers.set("Content-Length", "5");
    req.body = std::make_unique<SlowBodyReader>("hello");
    sync_wait(svc.dispatch(std::move(req)));
    CHECK(contains(cap.last_access(), "Z info  access "));
    CHECK(!contains(cap.last_access(), "slow=1"));
}

TEST(access_log_json_records) {
    Capture cap("json");
    // Ordinary lines: a JSON envelope with the message as a string, escapes intact
    LOG_INFO("hello \"world\"\n{}", 42);
    auto lines = cap.lines();
    CHECK_EQ(lines.size(), size_t(1));
    auto env = nlohmann::json::parse(lines[0]);
    CHECK_EQ(env["level"].get<std::string>(), "info");
    CHECK_EQ(env["msg"].get<std::string>(), "hello \"world\"\n42");
    CHECK(env.contains("ts") && env["ts"].get<std::string>().size() == 24);
    CHECK(env.contains("thread"));

    auto svc = make_service();
    svc.set_slow_request_threshold(std::chrono::milliseconds(20));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k 1", "hello")));
    auto j = nlohmann::json::parse(cap.last_access());
    CHECK_EQ(j["msg"].get<std::string>(), "access");
    CHECK_EQ(j["level"].get<std::string>(), "info");
    CHECK_EQ(j["method"].get<std::string>(), "PUT");
    CHECK_EQ(j["path"].get<std::string>(), "/bkt/k 1");
    CHECK_EQ(j["bucket"].get<std::string>(), "bkt");
    CHECK_EQ(j["key"].get<std::string>(), "k 1");
    CHECK_EQ(j["status"].get<int>(), 200);
    CHECK_EQ(j["bytes"].get<uint64_t>(), uint64_t(0));
    CHECK_EQ(j["api"].get<std::string>(), "PutObject");
    CHECK_EQ(j["backend"].get<std::string>(), "mem");
    CHECK_EQ(j["remote"].get<std::string>(), "10.0.0.7");
    CHECK_EQ(j["ua"].get<std::string>(), "aws-cli/2.0 \"quoted\"");
    CHECK(j["request_id"].get<std::string>().size() > 8);
    CHECK(j["backend_calls"].get<int>() >= 1);
    CHECK(j["backend_ms"].get<double>() >= 0);
    CHECK(j["ms"].get<double>() >= j["ttfb_ms"].get<double>());
    CHECK(!j.contains("ak"));      // auth disabled: omitted, never ""
    CHECK(!j.contains("slow"));
    CHECK(!j.contains("query"));
    // Streaming GET: bytes = what went out, total >= ttfb, and a slow one says so
    auto req = make_req("GET", "/bkt/k 1");
    req.raw_query = "response-content-type=text%2Fplain";
    req.query = {{"response-content-type", "text/plain"}};
    auto resp = sync_wait(svc.dispatch(std::move(req)));
    CHECK(resp.stream_body != nullptr);
    std::this_thread::sleep_for(std::chrono::milliseconds(25));  // a slow client
    CHECK_EQ(sync_wait(drain(*resp.stream_body)), uint64_t(5));
    j = nlohmann::json::parse(cap.last_access());
    CHECK_EQ(j["level"].get<std::string>(), "warn");
    CHECK_EQ(j["method"].get<std::string>(), "GET");
    CHECK_EQ(j["bytes"].get<uint64_t>(), uint64_t(5));
    CHECK_EQ(j["query"].get<std::string>(), "response-content-type=text%2Fplain");
    CHECK(j["slow"].get<bool>());
    CHECK(j["ms"].get<double>() >= 25.0);
    CHECK(j["ttfb_ms"].get<double>() < j["ms"].get<double>());
}

TEST(access_log_level_gates_lines) {
    Capture cap("text");
    Logger::set_level(LogLevel::Warn);
    auto svc = make_service();
    svc.set_slow_request_threshold(std::chrono::milliseconds(20));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/fast", "x")));
    CHECK(cap.lines().empty());  // INFO access lines are dropped at warn
    auto req = make_req("PUT", "/bkt/slow");
    req.headers.set("Content-Length", "5");
    req.body = std::make_unique<SlowBodyReader>("hello");
    sync_wait(svc.dispatch(std::move(req)));
    CHECK_EQ(cap.lines().size(), size_t(1));  // the slow one still surfaces
    CHECK(contains(cap.lines()[0], "slow=1"));
    Logger::set_level(LogLevel::Info);
}

TEST(logger_async_rotating_file) {
    auto dir = std::filesystem::temp_directory_path() /
               ("lights3-log-" + std::to_string(::getpid()));
    std::filesystem::remove_all(dir);
    std::filesystem::create_directories(dir);
    auto path = (dir / "lights3.log").string();
    LogConfig cfg;
    cfg.file = path;
    cfg.max_size = 65536;
    cfg.max_files = 2;
    cfg.async = true;
    cfg.async_queue = 128;
    Logger::init(cfg);
    std::string filler(200, 'z');
    for (int i = 0; i < 1000; ++i) LOG_INFO("line {} {}", i, filler);
    Logger::access().info("access probe");
    // shutdown drains the queue: everything is on disk afterwards, spread over the
    // rotation set; logging still works synchronously on the same file
    Logger::shutdown();
    LOG_WARN("after shutdown");
    Logger::shutdown();  // idempotent
    Logger::init(LogConfig{});  // back to stderr for the remaining tests
    std::string all;
    size_t files = 0;
    for (auto& e : std::filesystem::directory_iterator(dir)) {
        ++files;
        std::ifstream in(e.path());
        all += std::string((std::istreambuf_iterator<char>(in)), {});
    }
    CHECK(files >= 2 && files <= 3);  // rotated: lights3.log + .1 (+ .2)
    CHECK(contains(all, "line 999 "));
    CHECK(contains(all, "access probe"));
    CHECK(contains(all, "after shutdown"));
    std::filesystem::remove_all(dir);
}

// roadmap §5.4: trace correlation — an inherited traceparent keeps the client's
// trace id and records the caller's span as parent; without one the gateway starts
// a trace; the response carries traceresponse either way
TEST(access_log_trace_fields) {
    Capture cap("json");
    auto svc = make_service();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto req = make_req("PUT", "/bkt/k", "v");
    req.headers.add("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01");
    req.headers.add("tracestate", "vendor=abc");
    auto resp = sync_wait(svc.dispatch(std::move(req)));
    CHECK_EQ(resp.status, 200);
    auto tr = resp.headers.get("traceresponse").value_or("");
    CHECK(tr.rfind("00-4bf92f3577b34da6a3ce929d0e0e4736-", 0) == 0 && tr.ends_with("-01"));
    auto j = nlohmann::json::parse(cap.last_access());
    CHECK_EQ(j["trace_id"].get<std::string>(), "4bf92f3577b34da6a3ce929d0e0e4736");
    CHECK_EQ(j["parent_span_id"].get<std::string>(), "00f067aa0ba902b7");
    std::string span = j["span_id"].get<std::string>();
    CHECK_EQ(span.size(), size_t(16));
    CHECK_EQ(tr, "00-4bf92f3577b34da6a3ce929d0e0e4736-" + span + "-01");
    // No / malformed header: a fresh trace, no parent
    auto bad = make_req("GET", "/bkt/k");
    bad.headers.add("traceparent", "not-a-traceparent");
    resp = sync_wait(svc.dispatch(std::move(bad)));
    CHECK_EQ(sync_wait(drain(*resp.stream_body)), uint64_t(1));
    j = nlohmann::json::parse(cap.last_access());
    CHECK_EQ(j["trace_id"].get<std::string>().size(), size_t(32));
    CHECK(j["trace_id"].get<std::string>() != "4bf92f3577b34da6a3ce929d0e0e4736");
    CHECK(!j.contains("parent_span_id"));
    CHECK_EQ(resp.headers.get("traceresponse").value_or(""),
             "00-" + j["trace_id"].get<std::string>() + "-" + j["span_id"].get<std::string>() + "-01");
}

TEST(access_log_trace_text_slot) {
    Capture cap("text");
    auto svc = make_service();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto req = make_req("PUT", "/bkt/k", "v");
    req.headers.add("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01");
    sync_wait(svc.dispatch(std::move(req)));
    std::string line = cap.last_access();
    CHECK(contains(line, " trace=0af7651916cd43dd8448eb211c80319c/"));
    CHECK(contains(line, " parent=b7ad6b7169203331"));
    sync_wait(svc.dispatch(make_req("HEAD", "/bkt/k")));
    line = cap.last_access();
    CHECK(contains(line, " trace=") && !contains(line, " parent="));
}
