// SigV4：AWS 官方测试向量（get-vanilla）+ 自签自验 + 篡改检测
#include "core/util/crypto.h"
#include "core/util/time.h"
#include "http/model.h"
#include "s3/auth/sigv4.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::s3;

namespace {

// AWS SigV4 官方测试套件 get-vanilla 的固定时刻
util::SysTime vector_time() { return *util::parse_amz_date("20150830T123600Z"); }

AuthConfig vector_auth_config() {
    AuthConfig cfg;
    cfg.credentials = {{"AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}};
    cfg.region = "us-east-1";
    cfg.service = "service";  // 官方向量的 service 名
    return cfg;
}

http::HttpRequest vector_request() {
    http::HttpRequest req;
    req.method = "GET";
    req.raw_path = "/";
    req.path = "/";
    req.headers.add("Host", "example.amazonaws.com");
    req.headers.add("x-amz-date", "20150830T123600Z");
    req.headers.add(
        "Authorization",
        "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/aws4_request, "
        "SignedHeaders=host;x-amz-date, "
        "Signature=5fa00fa31553b73ebf1942676e86291e8372ff2a2260956d9b8aae1d763fbf31");
    return req;
}

}  // namespace

TEST(sigv4_official_get_vanilla_vector) {
    auto auth = SigV4Authenticator::build(vector_auth_config());
    auth.clock = vector_time;
    auto req = vector_request();
    auth.verify(req);  // 不抛即通过
}

TEST(sigv4_rejects_tampered_signature) {
    auto auth = SigV4Authenticator::build(vector_auth_config());
    auth.clock = vector_time;
    auto req = vector_request();
    // 篡改路径 → 签名不再匹配
    req.raw_path = "/other";
    req.path = "/other";
    CHECK_THROWS_S3(auth.verify(req), S3ErrorCode::SignatureDoesNotMatch);
}

TEST(sigv4_rejects_unknown_access_key) {
    auto cfg = vector_auth_config();
    cfg.credentials[0].access_key = "SOMEOTHERKEY";
    auto auth = SigV4Authenticator::build(cfg);
    auth.clock = vector_time;
    auto req = vector_request();
    CHECK_THROWS_S3(auth.verify(req), S3ErrorCode::InvalidAccessKeyId);
}

TEST(sigv4_rejects_clock_skew) {
    auto auth = SigV4Authenticator::build(vector_auth_config());
    auth.clock = [] { return vector_time() + std::chrono::hours(1); };
    auto req = vector_request();
    CHECK_THROWS_S3(auth.verify(req), S3ErrorCode::RequestTimeTooSkewed);
}

TEST(sigv4_rejects_missing_authorization) {
    auto auth = SigV4Authenticator::build(vector_auth_config());
    http::HttpRequest req;
    req.method = "GET";
    req.raw_path = "/";
    req.path = "/";
    CHECK_THROWS_S3(auth.verify(req), S3ErrorCode::AccessDenied);
}

TEST(sigv4_sign_then_verify_roundtrip) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    cfg.region = "us-east-1";
    cfg.service = "s3";
    auto auth = SigV4Authenticator::build(cfg);

    // 带 query 与 body 的 PUT：签名端 → 验签端闭环
    std::string body = "hello lights3";
    http::HttpRequest req;
    req.method = "PUT";
    req.raw_path = "/bkt/dir/a.txt";
    req.path = "/bkt/dir/a.txt";
    req.raw_query = "foo=bar%20baz&flag";
    req.query = {{"foo", "bar baz"}, {"flag", ""}};
    req.headers.add("Host", "localhost:9000");
    req.body = std::make_unique<http::StringBodyReader>(body);
    auth.sign(req, cfg.credentials[0], util::sha256_hex(body));
    auth.verify(req);

    // verify 应包装 body 做流式 SHA256 校验，读到 EOF 不抛
    std::byte buf[64];
    while (sync_wait(req.body->read(std::span(buf))) > 0) {}
}

TEST(sigv4_detects_payload_mismatch) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);

    http::HttpRequest req;
    req.method = "PUT";
    req.raw_path = "/bkt/x";
    req.path = "/bkt/x";
    req.headers.add("Host", "localhost");
    // 声明的 payload hash 与实际 body 不符
    req.body = std::make_unique<http::StringBodyReader>("actual body");
    auth.sign(req, cfg.credentials[0], util::sha256_hex("declared body"));
    auth.verify(req);  // 头签名一致，先通过

    std::byte buf[64];
    bool thrown = false;
    try {
        while (sync_wait(req.body->read(std::span(buf))) > 0) {}
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::XAmzContentSHA256Mismatch));
    }
    CHECK(thrown);
}
