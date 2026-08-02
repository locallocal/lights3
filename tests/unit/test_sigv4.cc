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

// 构造一个签名正确的 aws-chunked 请求；tamper 时篡改第二个 chunk 的数据；
// bad_final 时把 0 号尾块签名换成垃圾（末块校验路径）
http::HttpRequest make_chunked_request(SigV4Authenticator& auth, const Credential& cred,
                                       bool tamper, bool bad_final = false) {
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
    std::string s3 = bad_final ? std::string(64, '0') : chunk_sig(s2, "");
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

// ---------- docs/code-review/s3.md 修复回归 ----------

// 校验不绑 EOF：消费者只读满 length() 字节（cloudproxy 的消费模式）也必须检出 mismatch
TEST(sigv4_payload_mismatch_detected_without_eof_read) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);

    http::HttpRequest req;
    req.method = "PUT";
    req.raw_path = "/bkt/x";
    req.path = "/bkt/x";
    req.headers.add("Host", "localhost");
    req.body = std::make_unique<http::StringBodyReader>("actual body");
    auth.sign(req, cfg.credentials[0], util::sha256_hex("declared body"));
    auth.verify(req);

    uint64_t len = *req.body->length();
    std::vector<std::byte> buf(len);
    bool thrown = false;
    try {
        size_t got = 0;
        while (got < len)
            got += sync_wait(req.body->read(std::span(buf.data() + got, len - got)));
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::XAmzContentSHA256Mismatch));
    }
    CHECK(thrown);
}

// chunked 同理：读满 decoded length 即触发末块/0 号尾块验签，无需再读一次 EOF
TEST(sigv4_chunked_final_signature_checked_without_eof_read) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);

    auto req = make_chunked_request(auth, cfg.credentials[0], false, /*bad_final=*/true);
    auth.verify(req);
    std::byte buf[64];
    bool thrown = false;
    size_t got = 0;
    try {
        while (got < 11) got += sync_wait(req.body->read(std::span(buf, 11 - got)));
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::SignatureDoesNotMatch));
    }
    CHECK(thrown);
}

// streaming 变体缺 x-amz-decoded-content-length → InvalidRequest（AWS 强制该头）
TEST(sigv4_chunked_requires_decoded_length) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);

    http::HttpRequest req;
    req.method = "PUT";
    req.raw_path = "/bkt/big";
    req.path = "/bkt/big";
    req.headers.add("Host", "localhost");
    auth.sign(req, cfg.credentials[0], "STREAMING-AWS4-HMAC-SHA256-PAYLOAD");
    req.body = std::make_unique<http::StringBodyReader>("x");
    CHECK_THROWS_S3(auth.verify(req), S3ErrorCode::InvalidRequest);
}

// 声明空摘要（sha256("")）+ 非空 body：body 不得脱离签名保护
TEST(sigv4_empty_digest_with_nonempty_body_rejected) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);

    http::HttpRequest req;
    req.method = "PUT";
    req.raw_path = "/bkt/x";
    req.path = "/bkt/x";
    req.headers.add("Host", "localhost");
    req.body = std::make_unique<http::StringBodyReader>("smuggled");
    auth.sign(req, cfg.credentials[0], util::sha256_hex(""));
    auth.verify(req);
    bool thrown = false;
    try {
        read_all_body(*req.body);
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::XAmzContentSHA256Mismatch));
    }
    CHECK(thrown);
}

// 大写 hex 摘要：签名按字面值参与，内容比对大小写不敏感 → 正确 body 应通过
TEST(sigv4_uppercase_hex_digest_accepted) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);

    std::string body = "hello upper";
    std::string upper = util::sha256_hex(body);
    for (char& c : upper) c = static_cast<char>(toupper(static_cast<unsigned char>(c)));

    http::HttpRequest req;
    req.method = "PUT";
    req.raw_path = "/bkt/x";
    req.path = "/bkt/x";
    req.headers.add("Host", "localhost");
    req.body = std::make_unique<http::StringBodyReader>(body);
    auth.sign(req, cfg.credentials[0], upper);
    auth.verify(req);
    CHECK_EQ(read_all_body(*req.body), body);  // 不抛 = 校验通过
}

// host 不在 SignedHeaders → 拒绝（vhost 下不绑 host 的签名可换 Host 头跨桶重放）
TEST(sigv4_requires_host_in_signed_headers) {
    auto auth = SigV4Authenticator::build(vector_auth_config());
    auth.clock = vector_time;
    auto req = vector_request();
    req.headers.set("Authorization",
                    "AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/20150830/us-east-1/service/"
                    "aws4_request, SignedHeaders=x-amz-date, Signature=" +
                        std::string(64, '0'));
    CHECK_THROWS_S3(auth.verify(req), S3ErrorCode::AuthorizationHeaderMalformed);
}

// parse_amz_date 严格消费：缺 Z / 尾部垃圾 / 年份超界一律拒绝
TEST(amz_date_strict_parse) {
    CHECK(util::parse_amz_date("20260714T000000Z").has_value());
    CHECK(!util::parse_amz_date("20260714T000000").has_value());
    CHECK(!util::parse_amz_date("20260714T000000Zjunk").has_value());
    CHECK(!util::parse_amz_date("99990714T000000Z").has_value());
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

    // 签发时间超前（docs/todo.md §4）：X-Amz-Date 比 now 晚 16min → 未生效拒绝；
    // 15min 内的时钟偏移放行
    auth.clock = [] { return *util::parse_amz_date("20260713T234400Z"); };
    auto future = make();
    CHECK_THROWS_S3(auth.verify(future), S3ErrorCode::AccessDenied);
    auth.clock = [] { return *util::parse_amz_date("20260713T235000Z"); };
    auto skewed = make();
    auth.verify(skewed);
}
