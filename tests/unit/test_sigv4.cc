// SigV4: official AWS test vector (get-vanilla) + sign-then-verify round trip + tamper detection
#include "core/util/checksum.h"
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

// Fixed timestamp from the official AWS SigV4 test suite get-vanilla
util::SysTime vector_time() { return *util::parse_amz_date("20150830T123600Z"); }

AuthConfig vector_auth_config() {
    AuthConfig cfg;
    cfg.credentials = {{"AKIDEXAMPLE", "wJalrXUtnFEMI/K7MDENG+bPxRfiCYEXAMPLEKEY"}};
    cfg.region = "us-east-1";
    cfg.service = "service";  // service name used by the official vector
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
    auth.verify(req);  // passing = no throw
}

TEST(sigv4_rejects_tampered_signature) {
    auto auth = SigV4Authenticator::build(vector_auth_config());
    auth.clock = vector_time;
    auto req = vector_request();
    // Tamper with the path -> signature no longer matches
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

    // PUT with query and body: signing side -> verifying side round trip
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

    // verify should wrap the body for streaming SHA256 validation; reading to EOF must not throw
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
    // Declared payload hash does not match the actual body
    req.body = std::make_unique<http::StringBodyReader>("actual body");
    auth.sign(req, cfg.credentials[0], util::sha256_hex("declared body"));
    auth.verify(req);  // header signature matches, passes at first

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

// ---------- docs/s3-protocol.md §3.2/§3.4: aws-chunked and presigned ----------

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

// Build a correctly signed aws-chunked request; on tamper, corrupt the second chunk's data;
// on bad_final, replace the zero-length trailer chunk's signature with garbage (final-chunk validation path)
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
    // After unwrapping it is a pure data stream; decoded length (11 bytes) is validated at EOF
    CHECK_EQ(read_all_body(*req.body), "hello world");
    CHECK_EQ(*req.body->length(), uint64_t(11));
}

TEST(sigv4_chunked_rejects_tampered_chunk) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);

    auto req = make_chunked_request(auth, cfg.credentials[0], true);
    auth.verify(req);  // header signature still matches
    bool thrown = false;
    try {
        read_all_body(*req.body);
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::SignatureDoesNotMatch));
    }
    CHECK(thrown);
}

// ---------- docs/s3-protocol.md §3.3: -TRAILER variants (trailing checksums) ----------

namespace {

std::string hex_size(size_t n) {
    std::ostringstream os;
    os << std::hex << n;
    return os.str();
}

std::string be_bytes(uint64_t v, int bytes) {
    std::string out;
    for (int s = (bytes - 1) * 8; s >= 0; s -= 8) out.push_back(char((v >> s) & 0xff));
    return out;
}

// STREAMING-UNSIGNED-PAYLOAD-TRAILER: the default upload path of post-2025 SDKs -- chunks are
// unsigned, the trailing checksum is the only end-to-end integrity cover
http::HttpRequest make_unsigned_trailer_request(SigV4Authenticator& auth, const Credential& cred,
                                                const std::string& payload,
                                                const std::string& declared_name,
                                                const std::string& trailer_line) {
    http::HttpRequest req;
    req.method = "PUT";
    req.raw_path = "/bkt/big";
    req.path = "/bkt/big";
    req.headers.add("Host", "localhost");
    req.headers.add("x-amz-decoded-content-length", std::to_string(payload.size()));
    if (!declared_name.empty()) req.headers.add("x-amz-trailer", declared_name);
    auth.sign(req, cred, "STREAMING-UNSIGNED-PAYLOAD-TRAILER");

    std::string half1 = payload.substr(0, payload.size() / 2);
    std::string half2 = payload.substr(payload.size() / 2);
    std::string body;
    for (auto& c : {half1, half2})
        if (!c.empty()) body += hex_size(c.size()) + "\r\n" + c + "\r\n";
    body += "0\r\n" + trailer_line + "\r\n\r\n";  // trailer section ends with a blank line
    req.body = std::make_unique<http::StringBodyReader>(std::move(body));
    return req;
}

std::string crc32_trailer_value(const std::string& payload) {
    return util::base64_encode(
        be_bytes(util::crc32_update(
                     0, std::span(reinterpret_cast<const std::byte*>(payload.data()),
                                  payload.size())),
                 4));
}

}  // namespace

TEST(sigv4_unsigned_trailer_checksum_verified) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);
    std::string payload = "hello trailer world";

    auto ok = make_unsigned_trailer_request(
        auth, cfg.credentials[0], payload, "x-amz-checksum-crc32",
        "x-amz-checksum-crc32:" + crc32_trailer_value(payload));
    auth.verify(ok);
    CHECK_EQ(read_all_body(*ok.body), payload);

    // crc64nvme goes through the same path (the post-2025 SDK default algorithm)
    auto v64 = util::base64_encode(be_bytes(util::crc64nvme_of(payload), 8));
    auto ok64 = make_unsigned_trailer_request(auth, cfg.credentials[0], payload,
                                              "x-amz-checksum-crc64nvme",
                                              "x-amz-checksum-crc64nvme:" + v64);
    auth.verify(ok64);
    CHECK_EQ(read_all_body(*ok64.body), payload);
}

TEST(sigv4_unsigned_trailer_checksum_mismatch) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);
    std::string payload = "hello trailer world";

    // Trailer carries the checksum of different bytes -> BadDigest before EOF is reported
    auto req = make_unsigned_trailer_request(
        auth, cfg.credentials[0], payload, "x-amz-checksum-crc32",
        "x-amz-checksum-crc32:" + crc32_trailer_value("tampered payload!!!"));
    auth.verify(req);
    bool thrown = false;
    try {
        read_all_body(*req.body);
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::BadDigest));
    }
    CHECK(thrown);

    // Not-base64 trailer value -> InvalidDigest, distinct from the mismatch case
    auto junk = make_unsigned_trailer_request(auth, cfg.credentials[0], payload,
                                              "x-amz-checksum-crc32",
                                              "x-amz-checksum-crc32:not-base64!!");
    auth.verify(junk);
    thrown = false;
    try {
        read_all_body(*junk.body);
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::InvalidDigest));
    }
    CHECK(thrown);
}

TEST(sigv4_trailer_declaration_enforced_both_ways) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);
    std::string payload = "hello trailer world";

    // Declared crc32 but the body carries crc32c -> both "missing declared" and "undeclared"
    auto swapped = make_unsigned_trailer_request(
        auth, cfg.credentials[0], payload, "x-amz-checksum-crc32",
        "x-amz-checksum-crc32c:" + crc32_trailer_value(payload));
    auth.verify(swapped);
    bool thrown = false;
    try {
        read_all_body(*swapped.body);
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::InvalidRequest));
    }
    CHECK(thrown);

    // Unknown checksum trailer name is 501 at verify time, not silently skipped
    auto unknown = make_unsigned_trailer_request(auth, cfg.credentials[0], payload,
                                                 "x-amz-checksum-md99",
                                                 "x-amz-checksum-md99:AAAA");
    CHECK_THROWS_S3(auth.verify(unknown), S3ErrorCode::NotImplemented);

    // x-amz-trailer without a -TRAILER payload type -> InvalidRequest at verify time
    http::HttpRequest plain;
    plain.method = "PUT";
    plain.raw_path = "/bkt/x";
    plain.path = "/bkt/x";
    plain.headers.add("Host", "localhost");
    plain.headers.add("x-amz-trailer", "x-amz-checksum-crc32");
    plain.body = std::make_unique<http::StringBodyReader>("data");
    auth.sign(plain, cfg.credentials[0], util::sha256_hex("data"));
    CHECK_THROWS_S3(auth.verify(plain), S3ErrorCode::InvalidRequest);
}

// STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER: chunk signature chain + trailer signature
// ("AWS4-HMAC-SHA256-TRAILER" string-to-sign over the canonicalized trailers)
namespace {

http::HttpRequest make_signed_trailer_request(SigV4Authenticator& auth, const Credential& cred,
                                              const std::string& checksum_value,
                                              bool bad_trailer_sig, bool omit_trailer_sig) {
    http::HttpRequest req;
    req.method = "PUT";
    req.raw_path = "/bkt/big";
    req.path = "/bkt/big";
    req.headers.add("Host", "localhost");
    req.headers.add("x-amz-decoded-content-length", "11");
    req.headers.add("x-amz-trailer", "x-amz-checksum-crc32");
    auth.sign(req, cred, "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER");

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
    std::string trailer_line = "x-amz-checksum-crc32:" + checksum_value;
    std::string tsig_sts = "AWS4-HMAC-SHA256-TRAILER\n" + amz_date + "\n" + scope + "\n" + s3 +
                           "\n" + util::sha256_hex(trailer_line + "\n");
    std::string tsig = bad_trailer_sig ? std::string(64, '0')
                                       : util::to_hex(util::hmac_sha256(key, tsig_sts));
    std::string body = "6;chunk-signature=" + s1 + "\r\nhello \r\n" +
                       "5;chunk-signature=" + s2 + "\r\nworld\r\n" +
                       "0;chunk-signature=" + s3 + "\r\n" + trailer_line + "\r\n";
    if (!omit_trailer_sig) body += "x-amz-trailer-signature:" + tsig + "\r\n";
    body += "\r\n";
    req.body = std::make_unique<http::StringBodyReader>(std::move(body));
    return req;
}

}  // namespace

TEST(sigv4_signed_trailer_verified) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);

    auto ok = make_signed_trailer_request(auth, cfg.credentials[0],
                                          crc32_trailer_value("hello world"), false, false);
    auth.verify(ok);
    CHECK_EQ(read_all_body(*ok.body), "hello world");

    // Corrupt trailer signature -> SignatureDoesNotMatch (checked before the checksum)
    auto bad = make_signed_trailer_request(auth, cfg.credentials[0],
                                           crc32_trailer_value("hello world"), true, false);
    auth.verify(bad);
    bool thrown = false;
    try {
        read_all_body(*bad.body);
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::SignatureDoesNotMatch));
    }
    CHECK(thrown);

    // Correctly signed but wrong checksum (client-side digest bug) -> BadDigest
    auto mismatch = make_signed_trailer_request(auth, cfg.credentials[0],
                                                crc32_trailer_value("other bytes"), false, false);
    auth.verify(mismatch);
    thrown = false;
    try {
        read_all_body(*mismatch.body);
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::BadDigest));
    }
    CHECK(thrown);

    // Missing x-amz-trailer-signature in the signed variant -> InvalidRequest
    auto omitted = make_signed_trailer_request(auth, cfg.credentials[0],
                                               crc32_trailer_value("hello world"), false, true);
    auth.verify(omitted);
    thrown = false;
    try {
        read_all_body(*omitted.body);
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::InvalidRequest));
    }
    CHECK(thrown);
}

// ---------- Regression cases found in review ----------

// Validation must not be tied to EOF: a consumer that reads exactly length() bytes (cloudproxy's consumption pattern) must also detect the mismatch
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

// Same for chunked: reading the full decoded length triggers final-chunk/zero-trailer verification, no extra EOF read needed
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

// Streaming variant missing x-amz-decoded-content-length -> InvalidRequest (AWS mandates this header)
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

// Declared empty digest (sha256("")) + non-empty body: the body must not escape signature protection
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

// Uppercase hex digest: the signature uses the literal value, content comparison is case-insensitive -> a correct body should pass
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
    CHECK_EQ(read_all_body(*req.body), body);  // no throw = validation passed
}

// host not in SignedHeaders -> reject (under vhost, a signature not bound to host could be replayed across buckets by swapping the Host header)
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

// parse_amz_date strict consumption: missing Z / trailing garbage / out-of-range year are all rejected
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

    // Within validity (60s < 300s); presigned is not subject to the 15min skew limit
    auth.clock = [] { return *util::parse_amz_date("20260714T000100Z"); };
    auto ok = make();
    auth.verify(ok);

    // Expired (600s > 300s) -> AccessDenied
    auth.clock = [] { return *util::parse_amz_date("20260714T001000Z"); };
    auto expired = make();
    CHECK_THROWS_S3(auth.verify(expired), S3ErrorCode::AccessDenied);

    // Issued in the future (docs/s3-protocol.md §3.4): X-Amz-Date 16min later than now -> rejected as not yet effective;
    // clock skew within 15min is allowed
    auth.clock = [] { return *util::parse_amz_date("20260713T234400Z"); };
    auto future = make();
    CHECK_THROWS_S3(auth.verify(future), S3ErrorCode::AccessDenied);
    auth.clock = [] { return *util::parse_amz_date("20260713T235000Z"); };
    auto skewed = make();
    auth.verify(skewed);
}

// ---------- percent_decode semantic split (gaps §2.13) ----------

TEST(percent_decode_preserves_literal_plus) {
    // path / copy-source / canonical query: '+' is a legal literal character
    CHECK_EQ(util::percent_decode("a+b.txt"), "a+b.txt");
    CHECK_EQ(util::percent_decode("a%2Bb%20c"), "a+b c");
}

TEST(percent_decode_query_form_semantics) {
    // query parameters: a bare '+' is a form-encoded space; a '+' decoded from %2B is unaffected
    CHECK_EQ(util::percent_decode_query("a+b"), "a b");
    CHECK_EQ(util::percent_decode_query("a%2Bb"), "a+b");
    CHECK_EQ(util::percent_decode_query("a%20b+c"), "a b c");
}
