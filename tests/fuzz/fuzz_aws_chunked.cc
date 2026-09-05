// Fuzz target: aws-chunked de-framing (the streaming body reader installed by
// SigV4 verification for STREAMING-* payloads, docs/s3-protocol.md §3.2/§3.3).
// A correctly signed request header set is built once; the fuzz bytes are the
// body. Both the unsigned-trailer and the signed-chunk variants are driven, so
// chunk-size lines, chunk-signature fields, trailers and the terminal chunk are
// all parsed from hostile input; the only acceptable outcome is an S3Error
#include <cstddef>
#include <cstdint>
#include <span>
#include <string>

#include "core/config.h"
#include "core/task.h"
#include "s3/auth/sigv4.h"
#include "s3/errors.h"

namespace {
using namespace lights3;

AuthConfig& auth_cfg() {
    static AuthConfig cfg = [] {
        AuthConfig c;
        c.credentials = {{"FUZZAK1234567890", "fuzz-secret-key"}};
        c.region = "us-east-1";
        return c;
    }();
    return cfg;
}
const Credential& cred() { return auth_cfg().credentials[0]; }
s3::SigV4Authenticator& auth() {
    static s3::SigV4Authenticator a = s3::SigV4Authenticator::build(auth_cfg());
    return a;
}

Task<void> drain(http::BodyReader& r) {
    std::byte buf[512];
    for (int i = 0; i < 4096; ++i) {  // bounded: a parser must not loop forever on garbage
        size_t n = co_await r.read(std::span(buf));
        if (n == 0) break;
    }
}

void run(std::string_view body, bool signed_chunks, bool trailer) {
    http::HttpRequest req;
    req.method = "PUT";
    req.raw_path = "/bkt/key";
    req.path = "/bkt/key";
    req.headers.add("Host", "localhost");
    req.headers.add("Content-Length", std::to_string(body.size()));
    req.headers.add("x-amz-decoded-content-length", std::to_string(body.size() / 2));
    req.headers.add("Content-Encoding", "aws-chunked");
    if (trailer) req.headers.add("x-amz-trailer", "x-amz-checksum-crc32c");
    std::string hash = signed_chunks ? (trailer ? "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER"
                                               : "STREAMING-AWS4-HMAC-SHA256-PAYLOAD")
                                     : "STREAMING-UNSIGNED-PAYLOAD-TRAILER";
    req.headers.add("x-amz-content-sha256", hash);
    req.body = std::make_unique<http::StringBodyReader>(std::string(body));
    auth().sign(req, cred(), hash);
    try {
        auth().verify(req);  // installs the de-framing reader over req.body
        sync_wait(drain(*req.body));
    } catch (const s3::S3Error&) {
    }
}
}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    std::string_view body(reinterpret_cast<const char*>(data), size);
    run(body, /*signed_chunks=*/false, /*trailer=*/true);
    run(body, /*signed_chunks=*/true, /*trailer=*/false);
    run(body, /*signed_chunks=*/true, /*trailer=*/true);
    return 0;
}
