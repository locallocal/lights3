// roadmap §5.1: API x backend dimension — the metered backend decorator (per-op
// histograms, error counting, per-request backend-time accumulation over the
// cancellation token), Route names, and the (api, backend) series on /-/metrics
#include <set>

#include "core/cancel.h"
#include "core/metrics.h"
#include "core/task.h"
#include "s3/service.h"
#include "storage/memory/memory_backend.h"
#include "storage/metered_backend.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::s3;

namespace {

bool contains(const std::string& s, const std::string& sub) { return s.find(sub) != std::string::npos; }

http::HttpRequest make_req(std::string method, std::string path, std::string body = "",
                           std::vector<std::pair<std::string, std::string>> headers = {}) {
    http::HttpRequest req;
    req.method = std::move(method);
    req.raw_path = path;
    req.path = std::move(path);
    req.headers.add("Host", "localhost");
    req.headers.add("Content-Length", std::to_string(body.size()));
    for (auto& [k, v] : headers) req.headers.add(k, v);
    if (!body.empty()) req.body = std::make_unique<http::StringBodyReader>(std::move(body));
    return req;
}

// Drives backend calls under a token that carries a RequestBackendStats payload,
// the way dispatch does for a request
Task<void> put_and_head(storage::IStorageBackend& b, std::string key, bool expect_missing) {
    http::StringBodyReader body("hello");
    co_await b.put_object("bkt", key, storage::ObjectMeta{}, body);
    try {
        co_await b.head_object("bkt", expect_missing ? "missing" : key);
    } catch (const S3Error&) {
    }
}

}  // namespace

TEST(metered_backend_records_ops_errors_and_request_share) {
    auto reg = std::make_shared<MetricsRegistry>();
    auto raw = std::make_shared<storage::MemoryBackend>();
    storage::MeteredBackend mb("mem", raw, reg);
    sync_wait(mb.create_bucket("bkt"));

    auto stats = std::make_shared<storage::RequestBackendStats>();
    CancelSource src;
    src.set_data(stats);
    auto t = put_and_head(mb, "k", /*expect_missing=*/true);
    t.with_cancel(src.token());
    sync_wait(std::move(t));
    // Three calls reached the decorator under this token: put + head (404) ... the
    // 404 is the client's outcome, not a backend error
    CHECK_EQ(stats->calls.load(), uint32_t(2));
    CHECK_EQ(stats->errors.load(), uint32_t(0));
    CHECK(stats->nanos.load() > 0);

    std::string out = reg->render();
    CHECK(contains(out, "lights3_backend_op_seconds_count{backend=\"mem\",op=\"create_bucket\"} 1"));
    CHECK(contains(out, "lights3_backend_op_seconds_count{backend=\"mem\",op=\"put_object\"} 1"));
    CHECK(contains(out, "lights3_backend_op_seconds_count{backend=\"mem\",op=\"head_object\"} 1"));
    CHECK(contains(out, "lights3_backend_errors_total{backend=\"mem\",op=\"head_object\"} 0"));

    // A 5xx from the backend counts as an error: the memory backend answers SlowDown
    // once its byte budget is spent
    storage::MemoryOptions opt;
    opt.max_bytes = 4;
    auto tiny = std::make_shared<storage::MemoryBackend>(opt);
    storage::MeteredBackend mt("tiny", tiny, reg);
    sync_wait(mt.create_bucket("bkt"));
    auto stats2 = std::make_shared<storage::RequestBackendStats>();
    CancelSource src2;
    src2.set_data(stats2);
    auto t2 = put_and_head(mt, "big", false);
    t2.with_cancel(src2.token());
    CHECK_THROWS_S3(sync_wait(std::move(t2)), S3ErrorCode::SlowDown);
    CHECK_EQ(stats2->errors.load(), uint32_t(1));
    out = reg->render();
    CHECK(contains(out, "lights3_backend_errors_total{backend=\"tiny\",op=\"put_object\"} 1"));

    // Without a payload on the token the decorator still meters, nothing dangles
    sync_wait(mb.head_object("bkt", "k"));
    CHECK(contains(reg->render(), "op=\"head_object\"} 2"));
}

TEST(route_table_names_are_unique_and_complete) {
    std::set<std::string_view> seen;
    for (auto& r : S3Service::route_table()) {
        CHECK(!r.name.empty());
        CHECK(seen.insert(r.name).second);
    }
    CHECK(seen.count("PutObject") && seen.count("GetObject") && seen.count("ListBuckets") &&
          seen.count("CompleteMultipartUpload") && seen.count("DeleteObjects"));
}

TEST(dispatch_records_api_by_backend_series) {
    auto reg = std::make_shared<MetricsRegistry>();
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> raw;
    raw["mem"] = std::make_shared<storage::MemoryBackend>();
    raw["cold"] = std::make_shared<storage::MemoryBackend>();
    auto metered = storage::meter_backends(raw, reg);
    BucketsConfig bcfg;
    bcfg.default_backend = "mem";
    bcfg.rules.push_back({"cold-*", "cold"});
    S3Service svc(storage::BucketRouter::build(bcfg, metered),
                  SigV4Authenticator::build(AuthConfig{}));
    svc.set_backend_metrics(reg);
    auto call = [&](http::HttpRequest req) { return sync_wait(svc.dispatch(std::move(req))); };
    CHECK_EQ(call(make_req("PUT", "/bkt")).status, 200);
    CHECK_EQ(call(make_req("PUT", "/cold-1")).status, 200);
    CHECK_EQ(call(make_req("PUT", "/bkt/a", "data")).status, 200);
    CHECK_EQ(call(make_req("PUT", "/bkt/b", "", {{"x-amz-copy-source", "/bkt/a"}})).status, 200);
    CHECK_EQ(call(make_req("GET", "/bkt/a")).status, 200);
    CHECK_EQ(call(make_req("GET", "/bkt/nope")).status, 404);
    CHECK_EQ(call(make_req("PUT", "/cold-1/x", "cold")).status, 200);
    CHECK_EQ(call(make_req("GET", "/")).status, 200);
    auto m = call(make_req("GET", "/-/metrics"));
    const std::string& out = m.small_body;
    CHECK(contains(out, "lights3_api_requests_total{api=\"CreateBucket\",backend=\"mem\",class=\"2xx\"} 1"));
    CHECK(contains(out, "lights3_api_requests_total{api=\"CreateBucket\",backend=\"cold\",class=\"2xx\"} 1"));
    CHECK(contains(out, "lights3_api_requests_total{api=\"PutObject\",backend=\"mem\",class=\"2xx\"} 1"));
    CHECK(contains(out, "lights3_api_requests_total{api=\"CopyObject\",backend=\"mem\",class=\"2xx\"} 1"));
    CHECK(contains(out, "lights3_api_requests_total{api=\"GetObject\",backend=\"mem\",class=\"2xx\"} 1"));
    CHECK(contains(out, "lights3_api_requests_total{api=\"GetObject\",backend=\"mem\",class=\"4xx\"} 1"));
    CHECK(contains(out, "lights3_api_requests_total{api=\"PutObject\",backend=\"cold\",class=\"2xx\"} 1"));
    CHECK(contains(out, "lights3_api_requests_total{api=\"ListBuckets\",backend=\"-\",class=\"2xx\"} 1"));
    CHECK(contains(out, "lights3_api_request_duration_seconds_count{api=\"GetObject\",backend=\"mem\"} 2"));
    CHECK(contains(out, "lights3_api_request_duration_seconds_bucket{api=\"PutObject\",backend=\"mem\",le=\"+Inf\"} 1"));
    // The backend-level series rendered from the shared registry follow
    CHECK(contains(out, "lights3_backend_op_seconds_count{backend=\"cold\",op=\"put_object\"} 1"));
    // GET a, GET nope (404 reaches the backend), and the CopyObject fallback's source read
    CHECK(contains(out, "lights3_backend_op_seconds_count{backend=\"mem\",op=\"get_object\"} 3"));
    CHECK(contains(out, "lights3_backend_errors_total{backend=\"mem\",op=\"get_object\"} 0"));
}
