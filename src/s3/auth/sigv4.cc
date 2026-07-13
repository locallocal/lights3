#include "s3/auth/sigv4.h"

#include <algorithm>
#include <cstring>
#include <sstream>

#include "core/util/crypto.h"
#include "core/util/hex.h"
#include "core/util/uri.h"

namespace lights3::s3 {

namespace {

constexpr const char* kAlgo = "AWS4-HMAC-SHA256";
constexpr const char* kEmptySha256 =
    "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

bool constant_time_eq(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    unsigned char diff = 0;
    for (size_t i = 0; i < a.size(); ++i)
        diff |= static_cast<unsigned char>(a[i]) ^ static_cast<unsigned char>(b[i]);
    return diff == 0;
}

std::string lower(std::string s) {
    for (char& c : s) c = http::HeaderMap::lower(c);
    return s;
}

// 值 trim + 连续空白折叠（SigV4 canonical headers 规则）
std::string canonical_header_value(const std::string& v) {
    std::string out;
    bool in_space = false;
    for (char c : v) {
        if (c == ' ' || c == '\t') {
            in_space = true;
            continue;
        }
        if (in_space && !out.empty()) out.push_back(' ');
        in_space = false;
        out.push_back(c);
    }
    return out;
}

std::vector<std::string> split(const std::string& s, char sep) {
    std::vector<std::string> out;
    std::string cur;
    for (char c : s) {
        if (c == sep) {
            out.push_back(cur);
            cur.clear();
        } else {
            cur.push_back(c);
        }
    }
    out.push_back(cur);
    return out;
}

// 原始 query 串 → canonical query（解码后重编码、排序；可排除某个 key）
std::string canonical_query(const std::string& raw_query, std::string_view exclude = "") {
    std::vector<std::pair<std::string, std::string>> params;
    if (!raw_query.empty()) {
        for (auto& kv : split(raw_query, '&')) {
            if (kv.empty()) continue;
            auto eq = kv.find('=');
            std::string k = util::percent_decode(eq == std::string::npos ? kv : kv.substr(0, eq));
            std::string v = eq == std::string::npos ? "" : util::percent_decode(kv.substr(eq + 1));
            if (k == exclude) continue;
            params.emplace_back(util::aws_uri_encode(k, true), util::aws_uri_encode(v, true));
        }
    }
    std::sort(params.begin(), params.end());
    std::string out;
    for (auto& [k, v] : params) {
        if (!out.empty()) out.push_back('&');
        out += k + "=" + v;
    }
    return out;
}

struct AuthFields {
    std::string access_key, date, region, service, terminal;
    std::string signed_headers, signature, amz_date;
    bool presigned = false;
};

[[noreturn]] void malformed(const std::string& why) {
    throw S3Error(S3ErrorCode::AuthorizationHeaderMalformed, why);
}

void parse_credential(const std::string& cred, AuthFields& f) {
    auto parts = split(cred, '/');
    if (parts.size() != 5) malformed("Credential must be AK/date/region/service/aws4_request");
    f.access_key = parts[0];
    f.date = parts[1];
    f.region = parts[2];
    f.service = parts[3];
    f.terminal = parts[4];
}

// Authorization: AWS4-HMAC-SHA256 Credential=..., SignedHeaders=..., Signature=...
AuthFields parse_auth_header(const std::string& value) {
    AuthFields f;
    std::string_view v(value);
    if (v.rfind(kAlgo, 0) != 0) malformed("unsupported signing algorithm");
    v.remove_prefix(std::strlen(kAlgo));
    for (auto& piece : split(std::string(v), ',')) {
        auto p = piece;
        p.erase(0, p.find_first_not_of(" \t"));
        auto eq = p.find('=');
        if (eq == std::string::npos) continue;
        std::string k = p.substr(0, eq), val = p.substr(eq + 1);
        if (k == "Credential") parse_credential(val, f);
        else if (k == "SignedHeaders") f.signed_headers = lower(val);
        else if (k == "Signature") f.signature = val;
    }
    if (f.access_key.empty() || f.signed_headers.empty() || f.signature.empty())
        malformed("missing Credential/SignedHeaders/Signature");
    return f;
}

// EOF 时校验 SHA256 的流式装饰器
class Sha256VerifyingReader final : public http::BodyReader {
public:
    Sha256VerifyingReader(std::unique_ptr<http::BodyReader> inner, std::string expected_hex)
        : inner_(std::move(inner)),
          expected_(std::move(expected_hex)),
          hash_(util::HashStream::Algo::Sha256) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        if (done_) co_return 0;
        size_t n = co_await inner_->read(buf);
        if (n > 0) {
            hash_.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
        } else {
            done_ = true;
            if (hash_.final_hex() != expected_)
                throw S3Error(S3ErrorCode::XAmzContentSHA256Mismatch,
                              "The provided 'x-amz-content-sha256' does not match what was computed.");
        }
        co_return n;
    }
    std::optional<uint64_t> length() const override { return inner_->length(); }

private:
    std::unique_ptr<http::BodyReader> inner_;
    std::string expected_;
    util::HashStream hash_;
    bool done_ = false;
};

bool is_hex_digest(const std::string& s) {
    if (s.size() != 64) return false;
    for (char c : s)
        if (!isxdigit(static_cast<unsigned char>(c))) return false;
    return true;
}

}  // namespace

SigV4Authenticator SigV4Authenticator::build(const AuthConfig& cfg) {
    SigV4Authenticator a;
    a.region_ = cfg.region;
    a.service_ = cfg.service;
    for (auto& c : cfg.credentials) a.creds_[c.access_key] = c.secret_key;
    return a;
}

std::string SigV4Authenticator::signature_for(const http::HttpRequest& req,
                                              const std::string& secret_key,
                                              const std::string& amz_date,
                                              const std::string& scope,
                                              const std::string& signed_headers,
                                              const std::string& payload_hash) const {
    // canonical headers（按 SignedHeaders 列表取值；列表须已排序）
    std::string canon_headers;
    for (auto& name : split(signed_headers, ';')) {
        auto v = req.headers.get(name);
        if (!v)
            throw S3Error(S3ErrorCode::SignatureDoesNotMatch,
                          "Signed header '" + name + "' is missing from the request.");
        canon_headers += name + ":" + canonical_header_value(*v) + "\n";
    }

    std::string canonical_uri = req.raw_path.empty() ? "/" : req.raw_path;
    std::string presigned_exclude =
        req.query_has("X-Amz-Signature") ? "X-Amz-Signature" : "";
    std::ostringstream canonical;
    canonical << req.method << "\n"
              << canonical_uri << "\n"
              << canonical_query(req.raw_query, presigned_exclude) << "\n"
              << canon_headers << "\n"
              << signed_headers << "\n"
              << payload_hash;

    std::string sts = std::string(kAlgo) + "\n" + amz_date + "\n" + scope + "\n" +
                      util::sha256_hex(canonical.str());

    // HMAC 链派生签名密钥
    std::string date = amz_date.substr(0, 8);
    auto parts = split(scope, '/');  // date/region/service/aws4_request
    std::string init = "AWS4" + secret_key;
    auto k = util::hmac_sha256(
        std::span(reinterpret_cast<const uint8_t*>(init.data()), init.size()), date);
    k = util::hmac_sha256(k, parts[1]);
    k = util::hmac_sha256(k, parts[2]);
    k = util::hmac_sha256(k, "aws4_request");
    auto sig = util::hmac_sha256(k, sts);
    return util::to_hex(sig);
}

void SigV4Authenticator::verify(http::HttpRequest& req) const {
    if (!enabled()) return;

    AuthFields f;
    if (auto auth = req.headers.get("Authorization")) {
        f = parse_auth_header(*auth);
        auto date = req.headers.get("x-amz-date");
        if (!date) date = req.headers.get("Date");
        if (!date) malformed("missing x-amz-date");
        f.amz_date = *date;
    } else if (auto alg = req.query_get("X-Amz-Algorithm")) {  // presigned URL
        if (*alg != kAlgo) malformed("unsupported signing algorithm");
        f.presigned = true;
        parse_credential(req.query_get("X-Amz-Credential").value_or(""), f);
        f.signed_headers = lower(req.query_get("X-Amz-SignedHeaders").value_or(""));
        f.signature = req.query_get("X-Amz-Signature").value_or("");
        f.amz_date = req.query_get("X-Amz-Date").value_or("");
        if (f.access_key.empty() || f.signature.empty() || f.amz_date.empty())
            malformed("missing presigned query parameters");
    } else {
        throw S3Error(S3ErrorCode::AccessDenied, "Missing Authorization header");
    }

    // scope 检查
    if (f.terminal != "aws4_request" || f.service != service_ || f.region != region_)
        malformed("credential scope does not match this endpoint (" + region_ + "/" + service_ +
                  ")");
    if (f.amz_date.substr(0, 8) != f.date)
        malformed("credential date does not match x-amz-date");

    // 时钟偏移
    auto t = util::parse_amz_date(f.amz_date);
    if (!t) malformed("cannot parse x-amz-date");
    auto skew = std::chrono::duration_cast<std::chrono::seconds>(clock() - *t).count();
    if (skew > kMaxClockSkewSec || skew < -kMaxClockSkewSec)
        throw S3Error(S3ErrorCode::RequestTimeTooSkewed,
                      "The difference between the request time and the server's time is too large.");

    // 凭证
    auto it = creds_.find(f.access_key);
    if (it == creds_.end())
        throw S3Error(S3ErrorCode::InvalidAccessKeyId,
                      "The AWS access key ID you provided does not exist in our records.");

    // payload hash
    std::string payload_hash;
    if (f.presigned) {
        payload_hash = "UNSIGNED-PAYLOAD";
    } else if (auto h = req.headers.get("x-amz-content-sha256")) {
        payload_hash = *h;
        if (payload_hash.rfind("STREAMING-", 0) == 0)
            throw S3Error(S3ErrorCode::NotImplemented,
                          "aws-chunked streaming payloads are not supported yet.");
    } else {
        bool has_body = req.body && req.body->length().value_or(0) > 0;
        if (has_body)
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "Missing required header: x-amz-content-sha256");
        payload_hash = kEmptySha256;
    }

    std::string scope = f.date + "/" + f.region + "/" + f.service + "/aws4_request";
    std::string expect =
        signature_for(req, it->second, f.amz_date, scope, f.signed_headers, payload_hash);
    if (!constant_time_eq(expect, f.signature))
        throw S3Error(S3ErrorCode::SignatureDoesNotMatch,
                      "The request signature we calculated does not match the signature you "
                      "provided.");

    // 流式 payload 校验（docs/05 §3.3）
    if (is_hex_digest(payload_hash) && payload_hash != kEmptySha256 && req.body)
        req.body = std::make_unique<Sha256VerifyingReader>(std::move(req.body), payload_hash);
}

void SigV4Authenticator::sign(http::HttpRequest& req, const Credential& cred,
                              std::string payload_hash) const {
    if (payload_hash.empty()) payload_hash = kEmptySha256;
    std::string amz_date = util::amz_date(clock());
    req.headers.set("x-amz-date", amz_date);
    req.headers.set("x-amz-content-sha256", payload_hash);

    // SignedHeaders：host + 全部 x-amz-*（排序）
    std::vector<std::string> names;
    for (auto& [k, _] : req.headers.items()) {
        std::string lk = lower(k);
        if (lk == "host" || lk.rfind("x-amz-", 0) == 0) names.push_back(lk);
    }
    std::sort(names.begin(), names.end());
    names.erase(std::unique(names.begin(), names.end()), names.end());
    std::string signed_headers;
    for (auto& n : names) signed_headers += (signed_headers.empty() ? "" : ";") + n;

    std::string date = amz_date.substr(0, 8);
    std::string scope = date + "/" + region_ + "/" + service_ + "/aws4_request";
    std::string sig =
        signature_for(req, cred.secret_key, amz_date, scope, signed_headers, payload_hash);
    req.headers.set("Authorization",
                    std::string(kAlgo) + " Credential=" + cred.access_key + "/" + scope +
                        ", SignedHeaders=" + signed_headers + ", Signature=" + sig);
}

}  // namespace lights3::s3
