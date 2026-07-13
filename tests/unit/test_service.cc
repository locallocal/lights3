// L2 纯逻辑测试：mock HttpRequest + memory 后端走完整 dispatch（docs/01 §2）
#include "core/util/crypto.h"
#include "s3/service.h"
#include "storage/memory/memory_backend.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::s3;

namespace {

storage::BucketRouter make_router() {
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends;
    backends["mem"] = std::make_shared<storage::MemoryBackend>();
    BucketsConfig cfg;
    cfg.default_backend = "mem";
    return storage::BucketRouter::build(cfg, std::move(backends));
}

S3Service make_service_noauth() {
    return S3Service(make_router(), SigV4Authenticator::build(AuthConfig{}));
}

http::HttpRequest make_req(std::string method, std::string path, std::string body = "",
                           std::vector<std::pair<std::string, std::string>> query = {}) {
    http::HttpRequest req;
    req.method = std::move(method);
    req.raw_path = path;
    req.path = std::move(path);
    req.query = query;
    for (auto& [k, v] : query) {
        if (!req.raw_query.empty()) req.raw_query += "&";
        req.raw_query += k + "=" + v;
    }
    req.headers.add("Host", "localhost");
    if (!body.empty()) req.body = std::make_unique<http::StringBodyReader>(std::move(body));
    return req;
}

std::string body_of(http::HttpResponse& resp) {
    if (!resp.stream_body) return resp.small_body;
    std::string out;
    std::byte buf[8192];
    for (;;) {
        size_t n = sync_wait(resp.stream_body->read(std::span(buf)));
        if (n == 0) break;
        out.append(reinterpret_cast<const char*>(buf), n);
    }
    return out;
}

bool contains(const std::string& s, const std::string& sub) {
    return s.find(sub) != std::string::npos;
}

}  // namespace

TEST(service_put_get_roundtrip) {
    auto svc = make_service_noauth();

    auto create = sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    CHECK_EQ(create.status, 200);

    auto put = sync_wait(svc.dispatch(make_req("PUT", "/bkt/dir/hello.txt", "hi lights3")));
    CHECK_EQ(put.status, 200);
    CHECK(put.headers.has("ETag"));
    CHECK(put.headers.has("x-amz-request-id"));

    auto get = sync_wait(svc.dispatch(make_req("GET", "/bkt/dir/hello.txt")));
    CHECK_EQ(get.status, 200);
    CHECK_EQ(body_of(get), "hi lights3");
    CHECK_EQ(*get.headers.get("ETag"), *put.headers.get("ETag"));

    auto head = sync_wait(svc.dispatch(make_req("HEAD", "/bkt/dir/hello.txt")));
    CHECK_EQ(head.status, 200);
    CHECK_EQ(*head.content_length, uint64_t(10));
    CHECK(!head.stream_body);
}

TEST(service_range_request) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "0123456789")));

    auto req = make_req("GET", "/bkt/k");
    req.headers.add("Range", "bytes=2-5");
    auto resp = sync_wait(svc.dispatch(std::move(req)));
    CHECK_EQ(resp.status, 206);
    CHECK_EQ(body_of(resp), "2345");
    CHECK_EQ(*resp.headers.get("Content-Range"), "bytes 2-5/10");
}

TEST(service_error_xml) {
    auto svc = make_service_noauth();
    auto resp = sync_wait(svc.dispatch(make_req("GET", "/nobucket/k")));
    CHECK_EQ(resp.status, 404);
    CHECK(contains(resp.small_body, "<Code>NoSuchBucket</Code>"));
    CHECK(contains(resp.small_body, "<RequestId>"));

    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto nk = sync_wait(svc.dispatch(make_req("GET", "/bkt/missing")));
    CHECK_EQ(nk.status, 404);
    CHECK(contains(nk.small_body, "<Code>NoSuchKey</Code>"));
}

TEST(service_list_objects_v2) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/a/1.txt", "x")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/a/2.txt", "y")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/b.txt", "z")));

    auto resp = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"list-type", "2"}, {"delimiter", "/"}})));
    CHECK_EQ(resp.status, 200);
    CHECK(contains(resp.small_body, "<Key>b.txt</Key>"));
    CHECK(contains(resp.small_body, "<Prefix>a/</Prefix>"));
    CHECK(contains(resp.small_body, "<KeyCount>2</KeyCount>"));

    auto buckets = sync_wait(svc.dispatch(make_req("GET", "/")));
    CHECK(contains(buckets.small_body, "<Name>bkt</Name>"));
}

TEST(service_delete_semantics) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "v")));

    auto del = sync_wait(svc.dispatch(make_req("DELETE", "/bkt/k")));
    CHECK_EQ(del.status, 204);
    auto again = sync_wait(svc.dispatch(make_req("DELETE", "/bkt/k")));
    CHECK_EQ(again.status, 204);  // 幂等

    auto delb = sync_wait(svc.dispatch(make_req("DELETE", "/bkt")));
    CHECK_EQ(delb.status, 204);
    auto headb = sync_wait(svc.dispatch(make_req("HEAD", "/bkt")));
    CHECK_EQ(headb.status, 404);
    CHECK_EQ(headb.small_body, "");  // HEAD 错误响应不带 body
}

TEST(service_not_implemented_apis) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto mpu = sync_wait(
        svc.dispatch(make_req("POST", "/bkt/k", "", {{"uploads", ""}})));
    CHECK_EQ(mpu.status, 501);
    CHECK(contains(mpu.small_body, "NotImplemented"));
}

TEST(service_with_auth) {
    AuthConfig acfg;
    acfg.credentials = {{"TESTAK", "test-sk"}};
    auto auth = SigV4Authenticator::build(acfg);
    S3Service svc(make_router(), auth);

    // 未签名 → 403
    auto denied = sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    CHECK_EQ(denied.status, 403);
    CHECK(contains(denied.small_body, "AccessDenied"));

    // 正确签名 → 通过（用同一套签名端生成）
    auto req = make_req("PUT", "/bkt");
    auth.sign(req, acfg.credentials[0]);
    auto ok = sync_wait(svc.dispatch(std::move(req)));
    CHECK_EQ(ok.status, 200);

    // 签名对但 body hash 不符 → 传输中检出
    auto put = make_req("PUT", "/bkt/k", "tampered body");
    auth.sign(put, acfg.credentials[0], util::sha256_hex("original body"));
    auto resp = sync_wait(svc.dispatch(std::move(put)));
    CHECK_EQ(resp.status, 400);
    CHECK(contains(resp.small_body, "XAmzContentSHA256Mismatch"));

    // healthz 免认证
    auto hz = sync_wait(svc.dispatch(make_req("GET", "/-/healthz")));
    CHECK_EQ(hz.status, 200);
}
