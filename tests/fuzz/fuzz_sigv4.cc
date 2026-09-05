// Fuzz target: SigV4 verification (s3/auth/sigv4.h) — the Authorization header,
// x-amz-date / X-Amz-* presigned query parsing, canonicalization. Runs fully
// unauthenticated on every request, so it must only ever answer with an S3Error.
// Input layout: "<header block>\n\n<query>" — header lines "name: value"; when
// the block has no Authorization header a presigned-style query is exercised
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "core/config.h"
#include "s3/auth/sigv4.h"
#include "s3/errors.h"

namespace {
lights3::s3::SigV4Authenticator& auth() {
    static lights3::s3::SigV4Authenticator a = [] {
        lights3::AuthConfig cfg;
        cfg.credentials = {{"FUZZAK1234567890", "fuzz-secret-key"}};
        cfg.region = "us-east-1";
        return lights3::s3::SigV4Authenticator::build(cfg);
    }();
    return a;
}
}  // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    using namespace lights3;
    std::string_view in(reinterpret_cast<const char*>(data), size);
    http::HttpRequest req;
    req.method = "GET";
    req.raw_path = "/bkt/key";
    req.path = "/bkt/key";
    size_t split = in.find("\n\n");
    std::string_view headers = in.substr(0, split);
    std::string_view query = split == std::string_view::npos ? std::string_view() : in.substr(split + 2);
    size_t pos = 0;
    while (pos < headers.size()) {
        size_t nl = headers.find('\n', pos);
        std::string_view line = headers.substr(pos, nl == std::string_view::npos ? std::string_view::npos : nl - pos);
        pos = nl == std::string_view::npos ? headers.size() : nl + 1;
        size_t colon = line.find(':');
        if (colon == std::string_view::npos || colon == 0) continue;
        std::string_view v = line.substr(colon + 1);
        while (!v.empty() && v.front() == ' ') v.remove_prefix(1);
        req.headers.add(std::string(line.substr(0, colon)), std::string(v));
    }
    if (!req.headers.has("Host")) req.headers.add("Host", "localhost");
    req.raw_query = std::string(query);
    pos = 0;
    while (pos < query.size()) {
        size_t amp = query.find('&', pos);
        std::string_view kv = query.substr(pos, amp == std::string_view::npos ? std::string_view::npos : amp - pos);
        pos = amp == std::string_view::npos ? query.size() : amp + 1;
        size_t eq = kv.find('=');
        req.query.emplace_back(std::string(kv.substr(0, eq)),
                               eq == std::string_view::npos ? std::string() : std::string(kv.substr(eq + 1)));
    }
    try {
        auth().verify(req);
    } catch (const s3::S3Error&) {
    }
    return 0;
}
