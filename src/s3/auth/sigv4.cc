#include "s3/auth/sigv4.h"

#include <algorithm>
#include <cstring>
#include <map>
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

// HMAC 链派生签名密钥：date → region → service → "aws4_request"
util::Sha256Digest derive_signing_key(const std::string& secret_key, const std::string& date,
                                      const std::string& region, const std::string& service) {
    std::string init = "AWS4" + secret_key;
    auto k = util::hmac_sha256(
        std::span(reinterpret_cast<const uint8_t*>(init.data()), init.size()), date);
    k = util::hmac_sha256(k, region);
    k = util::hmac_sha256(k, service);
    return util::hmac_sha256(k, "aws4_request");
}

// aws-chunked 剥壳装饰器（docs/s3-protocol.md §3.2）：
// 逐 chunk 解析 "<hex-size>[;chunk-signature=<sig>]\r\n<data>\r\n"，向下游只暴露纯数据。
// signed 模式验证签名链：sig_n = HMAC(key, "AWS4-HMAC-SHA256-PAYLOAD" \n amz_date \n scope
//                                   \n sig_{n-1} \n sha256("") \n sha256(chunk_data))；
// 0 号尾 chunk 同样验证；其后的 trailer 行（-TRAILER 变体）读掉不校验。
class ChunkedSigV4BodyReader final : public http::BodyReader {
public:
    ChunkedSigV4BodyReader(std::unique_ptr<http::BodyReader> inner, bool signed_chunks,
                           util::Sha256Digest signing_key, std::string seed_signature,
                           std::string amz_date, std::string scope,
                           std::optional<uint64_t> decoded_length)
        : inner_(std::move(inner)),
          signed_(signed_chunks),
          key_(signing_key),
          prev_sig_(std::move(seed_signature)),
          amz_date_(std::move(amz_date)),
          scope_(std::move(scope)),
          decoded_length_(decoded_length) {}

    Task<size_t> read(std::span<std::byte> out) override {
        while (state_ != State::Done) {
            if (state_ == State::Header) {
                if (!co_await parse_header()) continue;  // 需要更多数据
            } else if (state_ == State::Data) {
                if (chunk_remaining_ == 0) {
                    co_await finish_chunk();
                    continue;
                }
                if (buf_.empty() && !co_await fill()) malformed_body("truncated chunk data");
                size_t n = std::min({out.size(), buf_.size(),
                                     static_cast<size_t>(chunk_remaining_)});
                std::memcpy(out.data(), buf_.data(), n);
                if (chunk_hash_)
                    chunk_hash_->update(
                        std::span(reinterpret_cast<const uint8_t*>(buf_.data()), n));
                buf_.erase(0, n);
                chunk_remaining_ -= n;
                delivered_ += n;
                co_return n;
            } else {  // Trailer：读掉剩余输入（trailer 头与结尾空行）
                buf_.clear();
                if (!co_await fill()) state_ = State::Done;
            }
        }
        if (decoded_length_ && delivered_ != *decoded_length_)
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "Decoded body size does not match x-amz-decoded-content-length.");
        co_return 0;
    }

    std::optional<uint64_t> length() const override { return decoded_length_; }

private:
    enum class State { Header, Data, Trailer, Done };

    [[noreturn]] static void malformed_body(const char* why) {
        throw S3Error(S3ErrorCode::InvalidRequest,
                      std::string("Malformed aws-chunked body: ") + why);
    }

    Task<bool> fill() {
        std::byte tmp[16 * 1024];
        size_t n = co_await inner_->read(std::span(tmp));
        if (n == 0) co_return false;
        buf_.append(reinterpret_cast<const char*>(tmp), n);
        co_return true;
    }

    // 返回 false 表示还需 fill；解析出 header 后切到 Data
    Task<bool> parse_header() {
        auto eol = buf_.find("\r\n");
        if (eol == std::string::npos) {
            if (buf_.size() > 4096) malformed_body("chunk header too long");
            if (!co_await fill()) malformed_body("truncated chunk header");
            co_return false;
        }
        std::string line = buf_.substr(0, eol);
        buf_.erase(0, eol + 2);

        auto semi = line.find(';');
        std::string size_hex = line.substr(0, semi == std::string::npos ? line.size() : semi);
        chunk_sig_.clear();
        if (semi != std::string::npos) {
            constexpr std::string_view kSigKey = "chunk-signature=";
            auto at = line.find(kSigKey, semi);
            if (at != std::string::npos) {
                chunk_sig_ = line.substr(at + kSigKey.size());
                if (auto extra = chunk_sig_.find(';'); extra != std::string::npos)
                    chunk_sig_.resize(extra);
            }
        }
        if (size_hex.empty() || size_hex.size() > 16) malformed_body("bad chunk size");
        uint64_t size = 0;
        for (char c : size_hex) {
            if (!isxdigit(static_cast<unsigned char>(c))) malformed_body("bad chunk size");
            size = size * 16 + (c <= '9' ? c - '0' : (tolower(c) - 'a' + 10));
        }
        if (signed_ && chunk_sig_.empty()) malformed_body("missing chunk-signature");

        chunk_remaining_ = size;
        final_chunk_ = size == 0;
        if (signed_) chunk_hash_.emplace(util::HashStream::Algo::Sha256);
        state_ = State::Data;
        co_return true;
    }

    // 当前 chunk 数据读完：验证签名、消费结尾 CRLF（尾 chunk 无 CRLF，直接进 Trailer）
    Task<void> finish_chunk() {
        if (signed_) {
            std::string data_hash = chunk_hash_->final_hex();
            chunk_hash_.reset();
            std::string sts = std::string("AWS4-HMAC-SHA256-PAYLOAD\n") + amz_date_ + "\n" +
                              scope_ + "\n" + prev_sig_ + "\n" + kEmptySha256 + "\n" + data_hash;
            std::string expect = util::to_hex(util::hmac_sha256(key_, sts));
            if (!constant_time_eq(expect, chunk_sig_))
                throw S3Error(S3ErrorCode::SignatureDoesNotMatch,
                              "Chunk signature does not match.");
            prev_sig_ = expect;
        }
        if (final_chunk_) {
            state_ = State::Trailer;
            co_return;
        }
        while (buf_.size() < 2)
            if (!co_await fill()) malformed_body("truncated chunk terminator");
        if (buf_[0] != '\r' || buf_[1] != '\n') malformed_body("missing chunk terminator");
        buf_.erase(0, 2);
        state_ = State::Header;
    }

    std::unique_ptr<http::BodyReader> inner_;
    bool signed_;
    util::Sha256Digest key_;
    std::string prev_sig_;
    std::string amz_date_;
    std::string scope_;
    std::optional<uint64_t> decoded_length_;

    std::string buf_;
    State state_ = State::Header;
    uint64_t chunk_remaining_ = 0;
    uint64_t delivered_ = 0;
    bool final_chunk_ = false;
    std::string chunk_sig_;
    std::optional<util::HashStream> chunk_hash_;
};

}  // namespace

namespace {

// build() 的默认实现：配置文件静态表，构造后只读
class StaticCredentialProvider final : public ICredentialProvider {
public:
    explicit StaticCredentialProvider(const AuthConfig& cfg) {
        for (auto& c : cfg.credentials) creds_[c.access_key] = c.secret_key;
    }
    std::optional<std::string> secret_for(std::string_view ak) const override {
        auto it = creds_.find(ak);
        if (it == creds_.end()) return std::nullopt;
        return it->second;
    }
    bool has_credentials() const override { return !creds_.empty(); }

private:
    std::map<std::string, std::string, std::less<>> creds_;
};

}  // namespace

SigV4Authenticator SigV4Authenticator::build(const AuthConfig& cfg) {
    SigV4Authenticator a;
    a.region_ = cfg.region;
    a.service_ = cfg.service;
    a.provider_ = std::make_shared<StaticCredentialProvider>(cfg);
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

    auto parts = split(scope, '/');  // date/region/service/aws4_request
    auto k = derive_signing_key(secret_key, parts[0], parts[1], parts[2]);
    return util::to_hex(util::hmac_sha256(k, sts));
}

std::string SigV4Authenticator::verify(http::HttpRequest& req) const {
    if (!enabled()) return "";

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

    auto t = util::parse_amz_date(f.amz_date);
    if (!t) malformed("cannot parse x-amz-date");
    if (f.presigned) {
        // presigned 按 X-Amz-Expires 判有效期（docs/s3-protocol.md §3.4），不做 15min 偏移检查
        auto exp = req.query_get("X-Amz-Expires");
        if (!exp) malformed("missing X-Amz-Expires");
        long expires = 0;
        try {
            expires = std::stol(*exp);
        } catch (...) {
            malformed("invalid X-Amz-Expires");
        }
        if (expires < 1 || expires > kMaxPresignExpires) malformed("invalid X-Amz-Expires");
        if (clock() > *t + std::chrono::seconds(expires))
            throw S3Error(S3ErrorCode::AccessDenied, "Request has expired");
    } else {
        auto skew = std::chrono::duration_cast<std::chrono::seconds>(clock() - *t).count();
        if (skew > kMaxClockSkewSec || skew < -kMaxClockSkewSec)
            throw S3Error(
                S3ErrorCode::RequestTimeTooSkewed,
                "The difference between the request time and the server's time is too large.");
    }

    // 凭证
    auto secret = provider_->secret_for(f.access_key);
    if (!secret)
        throw S3Error(S3ErrorCode::InvalidAccessKeyId,
                      "The AWS access key ID you provided does not exist in our records.");

    // payload hash（streaming 变体在 canonical request 中按字面值参与签名）
    std::string payload_hash;
    bool chunked_signed = false, chunked_unsigned = false;
    if (f.presigned) {
        payload_hash = "UNSIGNED-PAYLOAD";
    } else if (auto h = req.headers.get("x-amz-content-sha256")) {
        payload_hash = *h;
        if (payload_hash == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" ||
            payload_hash == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
            chunked_signed = true;
        else if (payload_hash == "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
            chunked_unsigned = true;
        else if (payload_hash.rfind("STREAMING-", 0) == 0)
            throw S3Error(S3ErrorCode::NotImplemented,
                          "This streaming payload type is not supported.");
    } else {
        bool has_body = req.body && req.body->length().value_or(0) > 0;
        if (has_body)
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "Missing required header: x-amz-content-sha256");
        payload_hash = kEmptySha256;
    }

    std::string scope = f.date + "/" + f.region + "/" + f.service + "/aws4_request";
    std::string expect =
        signature_for(req, *secret, f.amz_date, scope, f.signed_headers, payload_hash);
    if (!constant_time_eq(expect, f.signature))
        throw S3Error(S3ErrorCode::SignatureDoesNotMatch,
                      "The request signature we calculated does not match the signature you "
                      "provided.");

    // 流式 payload 校验（docs/s3-protocol.md §3.2/§3.3）
    if ((chunked_signed || chunked_unsigned) && req.body) {
        std::optional<uint64_t> decoded_len;
        if (auto dl = req.headers.get("x-amz-decoded-content-length")) {
            try {
                decoded_len = std::stoull(*dl);
            } catch (...) {
                throw S3Error(S3ErrorCode::InvalidRequest,
                              "Invalid x-amz-decoded-content-length.");
            }
        }
        req.body = std::make_unique<ChunkedSigV4BodyReader>(
            std::move(req.body), chunked_signed,
            derive_signing_key(*secret, f.date, f.region, f.service), f.signature,
            f.amz_date, scope, decoded_len);
    } else if (is_hex_digest(payload_hash) && payload_hash != kEmptySha256 && req.body) {
        req.body = std::make_unique<Sha256VerifyingReader>(std::move(req.body), payload_hash);
    }
    return f.access_key;
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
