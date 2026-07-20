// SigV4：AWS 官方测试向量（get-vanilla）+ 自签自验 + 篡改检测
#include "core/util/crypto.h"
#include "core/util/hex.h"
#include "core/util/time.h"
#include "core/util/uri.h"
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

// ---------- docs/s3-protocol.md §3.2/§3.4：aws-chunked 与 presigned ----------

namespace {

util::Sha256Digest test_signing_key(const std::string& secret, const std::string& date) {
    std::string init = "AWS4" + secret;
    auto k = util::hmac_sha256(
        std::span(reinterpret_cast<const uint8_t*>(init.data()), init.size()), date);
    k = util::hmac_sha256(k, "us-east-1");
    k = util::hmac_sha256(k, "s3");
    return util::hmac_sha256(k, "aws4_request");
}

std::string read_all_body(http::BodyReader& r) {
    std::string out;
    std::byte buf[4096];
    for (;;) {
        size_t n = sync_wait(r.read(std::span(buf)));
        if (n == 0) break;
        out.append(reinterpret_cast<const char*>(buf), n);
    }
    return out;
}

// 构造一个签名正确的 aws-chunked 请求；tamper 时篡改第二个 chunk 的数据
http::HttpRequest make_chunked_request(SigV4Authenticator& auth, const Credential& cred,
                                       bool tamper) {
    http::HttpRequest req;
    req.method = "PUT";
    req.raw_path = "/bkt/big";
    req.path = "/bkt/big";
    req.headers.add("Host", "localhost");
    req.headers.add("x-amz-decoded-content-length", "11");
    auth.sign(req, cred, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");

    std::string authz = *req.headers.get("Authorization");
    std::string seed = authz.substr(authz.find("Signature=") + 10);
    std::string amz_date = *req.headers.get("x-amz-date");
    std::string date = amz_date.substr(0, 8);
    std::string scope = date + "/us-east-1/s3/aws4_request";
    auto key = test_signing_key(cred.secret_key, date);

    auto chunk_sig = [&](const std::string& prev, const std::string& data) {
        std::string sts = "AWS4-HMAC-SHA256-PAYLOAD\n" + amz_date + "\n" + scope + "\n" +
                          prev + "\n" + util::sha256_hex("") + "\n" + util::sha256_hex(data);
        return util::to_hex(util::hmac_sha256(key, sts));
    };
    std::string s1 = chunk_sig(seed, "hello ");
    std::string s2 = chunk_sig(s1, "world");
    std::string s3 = chunk_sig(s2, "");
    std::string body = "6;chunk-signature=" + s1 + "\r\nhello \r\n" +
                       "5;chunk-signature=" + s2 + "\r\n" + (tamper ? "worlx" : "world") +
                       "\r\n0;chunk-signature=" + s3 + "\r\n\r\n";
    req.body = std::make_unique<http::StringBodyReader>(std::move(body));
    return req;
}

}  // namespace

TEST(sigv4_chunked_streaming_payload) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);

    auto req = make_chunked_request(auth, cfg.credentials[0], false);
    auth.verify(req);
    // 剥壳后是纯数据流；EOF 处校验解码长度（11 字节）
    CHECK_EQ(read_all_body(*req.body), "hello world");
    CHECK_EQ(*req.body->length(), uint64_t(11));
}

TEST(sigv4_chunked_rejects_tampered_chunk) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);

    auto req = make_chunked_request(auth, cfg.credentials[0], true);
    auth.verify(req);  // 头签名仍一致
    bool thrown = false;
    try {
        read_all_body(*req.body);
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::SignatureDoesNotMatch));
    }
    CHECK(thrown);
}

TEST(sigv4_presigned_url_expiry) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);

    std::string amz_date = "20260714T000000Z", date = "20260714";
    std::string cred = "TESTAK/" + date + "/us-east-1/s3/aws4_request";
    std::string cq = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=" +
                     util::aws_uri_encode(cred, true) + "&X-Amz-Date=" + amz_date +
                     "&X-Amz-Expires=300&X-Amz-SignedHeaders=host";
    std::string canonical = "GET\n/bkt/k\n" + cq + "\nhost:localhost\n\nhost\nUNSIGNED-PAYLOAD";
    std::string sts = "AWS4-HMAC-SHA256\n" + amz_date + "\n" + date +
                      "/us-east-1/s3/aws4_request\n" + util::sha256_hex(canonical);
    std::string sig =
        util::to_hex(util::hmac_sha256(test_signing_key("test-secret-key", date), sts));

    auto make = [&] {
        http::HttpRequest req;
        req.method = "GET";
        req.raw_path = "/bkt/k";
        req.path = "/bkt/k";
        req.raw_query = cq + "&X-Amz-Signature=" + sig;
        req.query = {{"X-Amz-Algorithm", "AWS4-HMAC-SHA256"},
                     {"X-Amz-Credential", cred},
                     {"X-Amz-Date", amz_date},
                     {"X-Amz-Expires", "300"},
                     {"X-Amz-SignedHeaders", "host"},
                     {"X-Amz-Signature", sig}};
        req.headers.add("Host", "localhost");
        return req;
    };

    // 有效期内（60s < 300s）；presigned 不受 15min 偏移限制
    auth.clock = [] { return *util::parse_amz_date("20260714T000100Z"); };
    auto ok = make();
    auth.verify(ok);

    // 过期（600s > 300s）→ AccessDenied
    auth.clock = [] { return *util::parse_amz_date("20260714T001000Z"); };
    auto expired = make();
    CHECK_THROWS_S3(auth.verify(expired), S3ErrorCode::AccessDenied);
}
