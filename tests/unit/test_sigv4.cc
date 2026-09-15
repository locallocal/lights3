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
    // service name used by the official vector
    cfg.service = "service";
    return cfg;
}

http::HttpRequest vector_request() {
    http::HttpRequest req;
    req.method = "GET";
    req.raw_path = "/";
    req.path = "/";
    req.headers.add("Host", "example.amazonaws.com");
    req.headers.add("x-amz-date", "20150830T123600Z");
    req.headers.add("Authorization",
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
    // passing = no throw
    auth.verify(req);
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
    while (sync_wait(req.body->read(std::span(buf))) > 0) {
    }
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
    // header signature matches, passes at first
    auth.verify(req);

    std::byte buf[64];
    bool thrown = false;
    try {
        while (sync_wait(req.body->read(std::span(buf))) > 0) {
        }
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::XAmzContentSHA256Mismatch));
    }
    CHECK(thrown);
}

// ---------- docs/architecture/s3-protocol.md §3.2/§3.4: aws-chunked and presigned ----------

namespace {

util::Sha256Digest test_signing_key(const std::string& secret, const std::string& date) {
    std::string init = "AWS4" + secret;
    auto k = util::hmac_sha256(std::span(reinterpret_cast<const uint8_t*>(init.data()), init.size()), date);
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
http::HttpRequest make_chunked_request(SigV4Authenticator& auth, const Credential& cred, bool tamper,
                                       bool bad_final = false) {
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
        std::string sts = "AWS4-HMAC-SHA256-PAYLOAD\n" + amz_date + "\n" + scope + "\n" + prev + "\n" +
                          util::sha256_hex("") + "\n" + util::sha256_hex(data);
        return util::to_hex(util::hmac_sha256(key, sts));
    };
    std::string s1 = chunk_sig(seed, "hello ");
    std::string s2 = chunk_sig(s1, "world");
    std::string s3 = bad_final ? std::string(64, '0') : chunk_sig(s2, "");
    std::string body = "6;chunk-signature=" + s1 + "\r\nhello \r\n" + "5;chunk-signature=" + s2 + "\r\n" +
                       (tamper ? "worlx" : "world") + "\r\n0;chunk-signature=" + s3 + "\r\n\r\n";
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
    // header signature still matches
    auth.verify(req);
    bool thrown = false;
    try {
        read_all_body(*req.body);
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::SignatureDoesNotMatch));
    }
    CHECK(thrown);
}

// ---------- docs/architecture/s3-protocol.md §3.3: -TRAILER variants (trailing checksums) ----------

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
                                                const std::string& payload, const std::string& declared_name,
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
    // trailer section ends with a blank line
    body += "0\r\n" + trailer_line + "\r\n\r\n";
    req.body = std::make_unique<http::StringBodyReader>(std::move(body));
    return req;
}

std::string crc32_trailer_value(const std::string& payload) {
    return util::base64_encode(be_bytes(
        util::crc32_update(0, std::span(reinterpret_cast<const std::byte*>(payload.data()), payload.size())), 4));
}

}  // namespace

TEST(sigv4_unsigned_trailer_checksum_verified) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);
    std::string payload = "hello trailer world";

    auto ok = make_unsigned_trailer_request(auth, cfg.credentials[0], payload, "x-amz-checksum-crc32",
                                            "x-amz-checksum-crc32:" + crc32_trailer_value(payload));
    auth.verify(ok);
    CHECK_EQ(read_all_body(*ok.body), payload);

    // crc64nvme goes through the same path (the post-2025 SDK default algorithm)
    auto v64 = util::base64_encode(be_bytes(util::crc64nvme_of(payload), 8));
    auto ok64 = make_unsigned_trailer_request(auth, cfg.credentials[0], payload, "x-amz-checksum-crc64nvme",
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
    auto req = make_unsigned_trailer_request(auth, cfg.credentials[0], payload, "x-amz-checksum-crc32",
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
    auto junk = make_unsigned_trailer_request(auth, cfg.credentials[0], payload, "x-amz-checksum-crc32",
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
    auto swapped = make_unsigned_trailer_request(auth, cfg.credentials[0], payload, "x-amz-checksum-crc32",
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
    auto unknown = make_unsigned_trailer_request(auth, cfg.credentials[0], payload, "x-amz-checksum-md99",
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
                                              const std::string& checksum_value, bool bad_trailer_sig,
                                              bool omit_trailer_sig) {
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
        std::string sts = "AWS4-HMAC-SHA256-PAYLOAD\n" + amz_date + "\n" + scope + "\n" + prev + "\n" +
                          util::sha256_hex("") + "\n" + util::sha256_hex(data);
        return util::to_hex(util::hmac_sha256(key, sts));
    };
    std::string s1 = chunk_sig(seed, "hello ");
    std::string s2 = chunk_sig(s1, "world");
    std::string s3 = chunk_sig(s2, "");
    std::string trailer_line = "x-amz-checksum-crc32:" + checksum_value;
    std::string tsig_sts = "AWS4-HMAC-SHA256-TRAILER\n" + amz_date + "\n" + scope + "\n" + s3 + "\n" +
                           util::sha256_hex(trailer_line + "\n");
    std::string tsig = bad_trailer_sig ? std::string(64, '0') : util::to_hex(util::hmac_sha256(key, tsig_sts));
    std::string body = "6;chunk-signature=" + s1 + "\r\nhello \r\n" + "5;chunk-signature=" + s2 + "\r\nworld\r\n" +
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

    auto ok = make_signed_trailer_request(auth, cfg.credentials[0], crc32_trailer_value("hello world"), false, false);
    auth.verify(ok);
    CHECK_EQ(read_all_body(*ok.body), "hello world");

    // Corrupt trailer signature -> SignatureDoesNotMatch (checked before the checksum)
    auto bad = make_signed_trailer_request(auth, cfg.credentials[0], crc32_trailer_value("hello world"), true, false);
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
    auto mismatch = make_signed_trailer_request(auth, cfg.credentials[0], crc32_trailer_value("other bytes"), false,
                                                false);
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
    auto omitted = make_signed_trailer_request(auth, cfg.credentials[0], crc32_trailer_value("hello world"), false,
                                               true);
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

// Validation must not be tied to EOF: a consumer that reads exactly length() bytes (cloudproxy's consumption pattern)
// must also detect the mismatch
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
        while (got < len) got += sync_wait(req.body->read(std::span(buf.data() + got, len - got)));
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::XAmzContentSHA256Mismatch));
    }
    CHECK(thrown);
}

// Same for chunked: reading the full decoded length triggers final-chunk/zero-trailer verification, no extra EOF read
// needed
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

// Uppercase hex digest: the signature uses the literal value, content comparison is case-insensitive -> a correct body
// should pass
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
    // no throw = validation passed
    CHECK_EQ(read_all_body(*req.body), body);
}

// host not in SignedHeaders -> reject (under vhost, a signature not bound to host could be replayed across buckets by
// swapping the Host header)
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
    std::string cq = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=" + util::aws_uri_encode(cred, true) +
                     "&X-Amz-Date=" + amz_date + "&X-Amz-Expires=300&X-Amz-SignedHeaders=host";
    std::string canonical = "GET\n/bkt/k\n" + cq + "\nhost:localhost\n\nhost\nUNSIGNED-PAYLOAD";
    std::string sts = "AWS4-HMAC-SHA256\n" + amz_date + "\n" + date + "/us-east-1/s3/aws4_request\n" +
                      util::sha256_hex(canonical);
    std::string sig = util::to_hex(util::hmac_sha256(test_signing_key("test-secret-key", date), sts));

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

    // Issued in the future (docs/architecture/s3-protocol.md §3.4): X-Amz-Date 16min later than now -> rejected as not
    // yet effective; clock skew within 15min is allowed
    auth.clock = [] { return *util::parse_amz_date("20260713T234400Z"); };
    auto future = make();
    CHECK_THROWS_S3(auth.verify(future), S3ErrorCode::AccessDenied);
    auth.clock = [] { return *util::parse_amz_date("20260713T235000Z"); };
    auto skewed = make();
    auth.verify(skewed);
}

// x-amz-decoded-content-length becomes the de-framed body's length() all the way down to
// the backend, so it is parsed with the same strictness L1 applies to Content-Length
// (http/model.h). std::stoull used to accept "-1" as 2^64-1, " 5" and "5abc" as 5 — and
// the quota gate, the backend's expected length and the metrics all believed the result
TEST(sigv4_decoded_content_length_parsed_strictly) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);

    // Signed, so the header's exact text is covered by the signature: whatever the parser
    // sees is what the client sent
    auto declaring = [&](const char* value) {
        http::HttpRequest req;
        req.method = "PUT";
        req.raw_path = "/bkt/big";
        req.path = "/bkt/big";
        req.headers.add("Host", "localhost");
        req.headers.add("x-amz-decoded-content-length", value);
        auth.sign(req, cfg.credentials[0], "STREAMING-UNSIGNED-PAYLOAD-TRAILER");
        req.body = std::make_unique<http::StringBodyReader>("0\r\n\r\n");
        return req;
    };
    for (const char* bad : {"-1", "+7", " 5", "5abc", "0x10", "", "18446744073709551616"}) {
        auto req = declaring(bad);
        CHECK_THROWS_S3(auth.verify(req), S3ErrorCode::InvalidRequest);
    }
    auto ok = declaring("0");
    auth.verify(ok);
    CHECK_EQ(*ok.body->length(), uint64_t(0));
    CHECK_EQ(read_all_body(*ok.body), "");
}

// ---------- De-framing reads payload straight into the caller's buffer ----------

namespace {

// A body reader that hands out at most `piece` bytes per read, the way a socket does.
// The de-framer used to funnel everything through its own 16KiB staging buffer, which hid
// both short reads and the caller's span size; it now reads chunk data directly into the
// caller's buffer, so those two shapes are the ones that matter
class TrickleReader final : public http::BodyReader {
public:
    TrickleReader(std::string data, size_t piece) : data_(std::move(data)), piece_(piece) {}
    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = std::min({buf.size(), piece_, data_.size() - pos_});
        if (n > 0) {
            std::memcpy(buf.data(), data_.data() + pos_, n);
            pos_ += n;
        }
        co_return n;
    }
    std::optional<uint64_t> length() const override { return std::nullopt; }

private:
    std::string data_;
    size_t piece_;
    size_t pos_ = 0;
};

// Reads through a fixed-size window, so the caller's span is exercised too
std::string read_all_in(http::BodyReader& r, size_t window) {
    std::string out;
    std::vector<std::byte> buf(window);
    for (;;) {
        size_t n = sync_wait(r.read(std::span(buf)));
        if (n == 0) break;
        CHECK(n <= window);
        out.append(reinterpret_cast<const char*>(buf.data()), n);
    }
    return out;
}

}  // namespace

TEST(sigv4_chunked_direct_read_handles_every_split) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);
    // Long enough to span many chunks and to make the window sizes below meaningful
    std::string payload;
    for (int i = 0; i < 4096; ++i) payload += static_cast<char>('a' + (i % 26));

    // chunk sizes deliberately coprime-ish with the windows and the trickle sizes, so the
    // chunk boundary lands mid-window, mid-piece and exactly on both
    for (size_t chunk : {size_t(1), size_t(7), size_t(64), size_t(1000), payload.size()}) {
        for (size_t piece : {size_t(1), size_t(13), size_t(4096), size_t(1) << 20}) {
            for (size_t window : {size_t(1), size_t(5), size_t(512), size_t(1) << 20}) {
                std::string body;
                for (size_t off = 0; off < payload.size(); off += chunk) {
                    std::string part = payload.substr(off, chunk);
                    body += hex_size(part.size()) + "\r\n" + part + "\r\n";
                }
                body += "0\r\n\r\n";

                http::HttpRequest req;
                req.method = "PUT";
                req.raw_path = "/bkt/big";
                req.path = "/bkt/big";
                req.headers.add("Host", "localhost");
                req.headers.add("x-amz-decoded-content-length", std::to_string(payload.size()));
                auth.sign(req, cfg.credentials[0], "STREAMING-UNSIGNED-PAYLOAD-TRAILER");
                req.body = std::make_unique<TrickleReader>(std::move(body), piece);
                auth.verify(req);
                std::string got = read_all_in(*req.body, window);
                if (got != payload)
                    throw mini_test::Failure("chunk=" + std::to_string(chunk) + " piece=" + std::to_string(piece) +
                                             " window=" + std::to_string(window) + ": got " +
                                             std::to_string(got.size()) + " bytes");
            }
        }
    }
}

TEST(sigv4_chunked_direct_read_keeps_the_signature_chain) {
    // The signed variants hash the delivered bytes for the per-chunk signature; reading
    // them into the caller's buffer instead of the staging one must hash exactly the same
    // bytes, whatever the split
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);
    for (size_t piece : {size_t(1), size_t(4), size_t(4096)}) {
        for (size_t window : {size_t(1), size_t(3), size_t(65536)}) {
            auto req = make_chunked_request(auth, cfg.credentials[0], /*tamper=*/false);
            // re-wrap the already-built body so it trickles
            std::string body = read_all_body(*req.body);
            req.body = std::make_unique<TrickleReader>(std::move(body), piece);
            auth.verify(req);
            CHECK_EQ(read_all_in(*req.body, window), "hello world");
        }
    }
    // and a tampered chunk is still caught when the data arrives in pieces
    auto bad = make_chunked_request(auth, cfg.credentials[0], /*tamper=*/true);
    std::string body = read_all_body(*bad.body);
    bad.body = std::make_unique<TrickleReader>(std::move(body), 3);
    auth.verify(bad);
    bool thrown = false;
    try {
        read_all_in(*bad.body, 5);
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::SignatureDoesNotMatch));
    }
    CHECK(thrown);
}

TEST(sigv4_chunked_zero_length_read_is_not_a_truncation) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);
    auto req = make_chunked_request(auth, cfg.credentials[0], false);
    auth.verify(req);
    // An empty destination reads as "nothing moved", not as a truncated chunk
    CHECK_EQ(sync_wait(req.body->read(std::span<std::byte>{})), size_t(0));
    CHECK_EQ(read_all_body(*req.body), "hello world");
}

// ---------- Auth disabled: the transport framing still comes off ----------
//
// aws-chunked is the transport encoding named by x-amz-content-sha256, not an
// authentication artifact. verify() used to return on its first line whenever no
// credential was configured, before reaching the de-framing step, so the chunk headers
// were written into the object: an 11-byte body stored as 21 bytes, the ETag computed over
// the framing, 200 and no error anywhere. Post-2025 SDKs send this payload type by
// default, which made "start it without credentials to try it out" corrupt every upload.

namespace {

// No credentials configured -> auth disabled. sign() still works: the client signs with a
// credential this side has never heard of, exactly as a real SDK would
SigV4Authenticator unauthenticated() {
    AuthConfig cfg;
    cfg.region = "us-east-1";
    return SigV4Authenticator::build(cfg);
}

const Credential& stranger() {
    static const Credential c{"UNKNOWNAK", "not-in-any-credential-table"};
    return c;
}

}  // namespace

TEST(sigv4_disabled_deframes_unsigned_trailer) {
    auto auth = unauthenticated();
    CHECK(!auth.enabled());
    std::string payload = "hello trailer world";

    auto req = make_unsigned_trailer_request(auth, stranger(), payload, "x-amz-checksum-crc32",
                                             "x-amz-checksum-crc32:" + crc32_trailer_value(payload));
    // Admitted without an identity, and de-framed all the same
    CHECK(auth.verify(req).access_key.empty());
    CHECK_EQ(*req.body->length(), uint64_t(payload.size()));
    CHECK_EQ(read_all_body(*req.body), payload);
}

TEST(sigv4_disabled_deframes_signed_chunks) {
    auto auth = unauthenticated();
    // Per-chunk signatures are present and cannot be checked here: parsed past, not rejected
    auto req = make_chunked_request(auth, stranger(), /*tamper=*/false);
    auth.verify(req);
    CHECK_EQ(read_all_body(*req.body), "hello world");

    // x-amz-trailer-signature likewise -- accepted and dropped, never mistaken for an
    // undeclared trailer. The declared checksum needs no secret, so it stays verified
    auto tr = make_signed_trailer_request(auth, stranger(), crc32_trailer_value("hello world"), false, false);
    auth.verify(tr);
    CHECK_EQ(read_all_body(*tr.body), "hello world");

    auto bad = make_signed_trailer_request(auth, stranger(), crc32_trailer_value("other bytes"), false, false);
    auth.verify(bad);
    bool thrown = false;
    try {
        read_all_body(*bad.body);
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::BadDigest));
    }
    CHECK(thrown);
}

TEST(sigv4_disabled_keeps_the_streaming_contract) {
    auto auth = unauthenticated();
    auto streaming_put = [](const char* payload_type) {
        http::HttpRequest req;
        req.method = "PUT";
        req.raw_path = "/bkt/big";
        req.path = "/bkt/big";
        req.headers.add("Host", "localhost");
        req.headers.add("x-amz-content-sha256", payload_type);
        req.body = std::make_unique<http::StringBodyReader>("0\r\n\r\n");
        return req;
    };

    // The de-framer reports this length downstream, so it stays mandatory -- and is parsed
    // as strictly here as on the signed path
    auto no_len = streaming_put("STREAMING-UNSIGNED-PAYLOAD-TRAILER");
    CHECK_THROWS_S3(auth.verify(no_len), S3ErrorCode::InvalidRequest);
    auto negative_len = streaming_put("STREAMING-UNSIGNED-PAYLOAD-TRAILER");
    negative_len.headers.add("x-amz-decoded-content-length", "-1");
    CHECK_THROWS_S3(auth.verify(negative_len), S3ErrorCode::InvalidRequest);

    // A payload type this implementation cannot de-frame must refuse loudly rather than
    // store the framing -- which is the whole point of the fix
    auto unknown = streaming_put("STREAMING-SOMETHING-FUTURE");
    unknown.headers.add("x-amz-decoded-content-length", "0");
    CHECK_THROWS_S3(auth.verify(unknown), S3ErrorCode::NotImplemented);

    // A plain body is handed through untouched (no digest is verified without a signature)
    http::HttpRequest plain;
    plain.method = "PUT";
    plain.raw_path = "/bkt/x";
    plain.path = "/bkt/x";
    plain.headers.add("Host", "localhost");
    plain.headers.add("x-amz-content-sha256", util::sha256_hex("hello world"));
    plain.body = std::make_unique<http::StringBodyReader>("hello world");
    auth.verify(plain);
    CHECK_EQ(read_all_body(*plain.body), "hello world");
}

// ---------- presigned URLs that commit to a payload hash ----------

namespace {

// Where the signer put the payload hash it committed to
enum class HashIn { None, Query, SignedHeader, UnsignedHeader };

// Hand-built presigned PUT, so the canonical request is spelled out rather than produced
// by the same code under test. The canonical query is sorted by key, as SigV4 requires
http::HttpRequest make_presigned(const std::string& secret, const std::string& hash, HashIn where) {
    const std::string amz_date = "20260714T000000Z", date = "20260714";
    const std::string cred = "TESTAK/" + date + "/us-east-1/s3/aws4_request";
    const std::string signed_headers = where == HashIn::SignedHeader ? "host;x-amz-content-sha256" : "host";

    std::string cq = "X-Amz-Algorithm=AWS4-HMAC-SHA256";
    if (where == HashIn::Query) cq += "&X-Amz-Content-Sha256=" + util::aws_uri_encode(hash, true);
    cq += "&X-Amz-Credential=" + util::aws_uri_encode(cred, true);
    cq += "&X-Amz-Date=" + amz_date;
    cq += "&X-Amz-Expires=300";
    cq += "&X-Amz-SignedHeaders=" + util::aws_uri_encode(signed_headers, true);

    // An unsigned header is not part of what was signed, so the URL still commits to
    // UNSIGNED-PAYLOAD
    const std::string effective = (where == HashIn::Query || where == HashIn::SignedHeader) ? hash : "UNSIGNED-PAYLOAD";
    std::string canon_headers = "host:localhost\n";
    if (where == HashIn::SignedHeader) canon_headers += "x-amz-content-sha256:" + hash + "\n";
    const std::string canonical = "PUT\n/bkt/k\n" + cq + "\n" + canon_headers + "\n" + signed_headers + "\n" +
                                  effective;
    const std::string sts = "AWS4-HMAC-SHA256\n" + amz_date + "\n" + date + "/us-east-1/s3/aws4_request\n" +
                            util::sha256_hex(canonical);
    const std::string sig = util::to_hex(util::hmac_sha256(test_signing_key(secret, date), sts));

    http::HttpRequest req;
    req.method = "PUT";
    req.raw_path = "/bkt/k";
    req.path = "/bkt/k";
    req.raw_query = cq + "&X-Amz-Signature=" + sig;
    req.query.push_back({"X-Amz-Algorithm", "AWS4-HMAC-SHA256"});
    if (where == HashIn::Query) req.query.push_back({"X-Amz-Content-Sha256", hash});
    req.query.push_back({"X-Amz-Credential", cred});
    req.query.push_back({"X-Amz-Date", amz_date});
    req.query.push_back({"X-Amz-Expires", "300"});
    req.query.push_back({"X-Amz-SignedHeaders", signed_headers});
    req.query.push_back({"X-Amz-Signature", sig});
    req.headers.add("Host", "localhost");
    if (where == HashIn::SignedHeader || where == HashIn::UnsignedHeader) req.headers.add("x-amz-content-sha256", hash);
    return req;
}

}  // namespace

TEST(sigv4_presigned_honours_a_committed_payload_hash) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);
    auth.clock = [] { return *util::parse_amz_date("20260714T000100Z"); };
    const std::string body = "presigned body";
    const std::string digest = util::sha256_hex(body);

    // The query parameter is on the common allowlist, so it was accepted and then ignored:
    // a URL presigned with a real digest came back SignatureDoesNotMatch
    auto q = make_presigned("test-secret-key", digest, HashIn::Query);
    q.body = std::make_unique<http::StringBodyReader>(body);
    auth.verify(q);
    CHECK_EQ(read_all_body(*q.body), body);

    // and once honoured, it is also enforced
    auto tampered = make_presigned("test-secret-key", digest, HashIn::Query);
    tampered.body = std::make_unique<http::StringBodyReader>("a different body!");
    auth.verify(tampered);
    bool thrown = false;
    try {
        read_all_body(*tampered.body);
    } catch (const S3Error& e) {
        thrown = true;
        CHECK_EQ(wire_code(e.code), wire_code(S3ErrorCode::XAmzContentSHA256Mismatch));
    }
    CHECK(thrown);

    // Same through a header the signer listed in SignedHeaders
    auto h = make_presigned("test-secret-key", digest, HashIn::SignedHeader);
    h.body = std::make_unique<http::StringBodyReader>(body);
    auth.verify(h);
    CHECK_EQ(read_all_body(*h.body), body);

    // A header the signer did NOT sign must not be consulted: the URL still commits to
    // UNSIGNED-PAYLOAD, and letting the header speak would break a client's own URL
    auto u = make_presigned("test-secret-key", digest, HashIn::UnsignedHeader);
    u.body = std::make_unique<http::StringBodyReader>("whatever the client sends");
    auth.verify(u);
    CHECK_EQ(read_all_body(*u.body), "whatever the client sends");

    // The ordinary presigned URL is unchanged
    auto plain = make_presigned("test-secret-key", "", HashIn::None);
    auth.verify(plain);
}

TEST(sigv4_presigned_payload_hash_cannot_be_forged) {
    AuthConfig cfg;
    cfg.credentials = {{"TESTAK", "test-secret-key"}};
    auto auth = SigV4Authenticator::build(cfg);
    auth.clock = [] { return *util::parse_amz_date("20260714T000100Z"); };
    const std::string digest = util::sha256_hex("presigned body");

    // Rewriting the parameter changes the canonical query, so the signature stops matching
    auto forged = make_presigned("test-secret-key", digest, HashIn::Query);
    const std::string other = util::sha256_hex("something else");
    for (auto& [k, v] : forged.query)
        if (k == "X-Amz-Content-Sha256") v = other;
    auto at = forged.raw_query.find(digest);
    CHECK(at != std::string::npos);
    forged.raw_query.replace(at, digest.size(), other);
    CHECK_THROWS_S3(auth.verify(forged), S3ErrorCode::SignatureDoesNotMatch);

    // Adding the parameter to a URL signed without it does the same
    auto added = make_presigned("test-secret-key", "", HashIn::None);
    added.query.insert(added.query.begin() + 1, {"X-Amz-Content-Sha256", digest});
    added.raw_query = "X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Content-Sha256=" + digest +
                      added.raw_query.substr(std::string("X-Amz-Algorithm=AWS4-HMAC-SHA256").size());
    CHECK_THROWS_S3(auth.verify(added), S3ErrorCode::SignatureDoesNotMatch);
}

// ---------- percent_decode semantic split ----------

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
