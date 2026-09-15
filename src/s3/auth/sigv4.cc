#include "s3/auth/sigv4.h"

#include <algorithm>
#include <cstring>
#include <map>
#include <sstream>

#include "core/util/checksum.h"
#include "core/util/crypto.h"
#include "core/util/hex.h"
#include "core/util/uri.h"
#include "s3/checksum_guard.h"

namespace lights3::s3 {

namespace {

constexpr const char* kAlgo = "AWS4-HMAC-SHA256";
constexpr const char* kEmptySha256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";

bool constant_time_eq(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    unsigned char diff = 0;
    for (size_t i = 0; i < a.size(); ++i) diff |= static_cast<unsigned char>(a[i]) ^ static_cast<unsigned char>(b[i]);
    return diff == 0;
}

std::string lower(std::string s) {
    for (char& c : s) c = http::HeaderMap::lower(c);
    return s;
}

// Value trim + consecutive whitespace folding (SigV4 canonical headers rule; whitespace inside quotes kept verbatim)
std::string canonical_header_value(const std::string& v) {
    std::string out;
    bool in_space = false, in_quotes = false;
    for (char c : v) {
        if (c == '"') in_quotes = !in_quotes;
        if (!in_quotes && (c == ' ' || c == '\t')) {
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

// Raw query string -> canonical query (decode then re-encode, sort; one key may be excluded)
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

[[noreturn]] void malformed(const std::string& why) { throw S3Error(S3ErrorCode::AuthorizationHeaderMalformed, why); }

// Whether a (lowercase) name appears in a ';'-separated SignedHeaders list. What it really
// answers is "did the signer commit to this header's value", which is the only reason to
// let a header influence anything
bool header_is_signed(const std::string& signed_headers, std::string_view name) {
    for (auto& n : split(signed_headers, ';'))
        if (n == name) return true;
    return false;
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
        if (k == "Credential")
            parse_credential(val, f);
        else if (k == "SignedHeaders")
            f.signed_headers = lower(val);
        else if (k == "Signature")
            f.signature = val;
    }
    if (f.access_key.empty() || f.signed_headers.empty() || f.signature.empty())
        malformed("missing Credential/SignedHeaders/Signature");
    return f;
}

// Streaming SHA256-verifying decorator. Verification is not tied to EOF: it compares once the self-reported
// length() is fully read (consumers like cloudproxy read exactly length() bytes and never do an extra read to EOF);
// the EOF path covers the no-length case.
// On mismatch, the exception fires before the last chunk is delivered downstream -- working with backend.h's "throw
// means no commit" contract.
class Sha256VerifyingReader final : public http::BodyReader {
public:
    Sha256VerifyingReader(std::unique_ptr<http::BodyReader> inner, std::string expected_hex)
        : inner_(std::move(inner)),
          // AWS digests are always lowercase; tolerate uppercase input
          expected_(lower(std::move(expected_hex))),
          hash_(util::HashStream::Algo::Sha256) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        if (done_) co_return 0;
        size_t n = co_await inner_->read(buf);
        if (n > 0) {
            hash_.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
            consumed_ += n;
            if (auto len = inner_->length(); len && consumed_ >= *len) verify();
        } else {
            verify();
        }
        co_return n;
    }
    std::optional<uint64_t> length() const override { return inner_->length(); }

private:
    void verify() {
        done_ = true;
        if (hash_.final_hex() != expected_)
            throw S3Error(S3ErrorCode::XAmzContentSHA256Mismatch,
                          "The provided 'x-amz-content-sha256' does not match what was computed.");
    }

    std::unique_ptr<http::BodyReader> inner_;
    std::string expected_;
    util::HashStream hash_;
    uint64_t consumed_ = 0;
    bool done_ = false;
};

bool is_hex_digest(const std::string& s) {
    if (s.size() != 64) return false;
    for (char c : s)
        if (!isxdigit(static_cast<unsigned char>(c))) return false;
    return true;
}

// HMAC chain derives the signing key: date -> region -> service -> "aws4_request"
util::Sha256Digest derive_signing_key(const std::string& secret_key, const std::string& date, const std::string& region,
                                      const std::string& service) {
    // SK derivative, wiped when it leaves scope
    util::SecretString init = "AWS4" + secret_key;
    auto k = util::hmac_sha256(std::span(reinterpret_cast<const uint8_t*>(init.data()), init.size()), date);
    k = util::hmac_sha256(k, region);
    k = util::hmac_sha256(k, service);
    return util::hmac_sha256(k, "aws4_request");
}

// A trailer declared via x-amz-trailer: the digest of the whole decoded payload arrives *after* the
// data, so it is verified here rather than by the header-driven ChecksumVerifyingReader
struct DeclaredTrailer {
    // lowercase x-amz-checksum-*
    std::string name;
    ExpectedDigest::Algo algo;
    size_t bytes;
};

// aws-chunked de-framing decorator (docs/architecture/s3-protocol.md §3.2/§3.3):
// parses "<hex-size>[;chunk-signature=<sig>]\r\n<data>\r\n" chunk by chunk, exposing only pure data downstream.
// signed mode verifies the signature chain: sig_n = HMAC(key, "AWS4-HMAC-SHA256-PAYLOAD" \n amz_date \n scope
//                                   \n sig_{n-1} \n sha256("") \n sha256(chunk_data))；
// The zero-size final chunk is verified too. For the -TRAILER variants the trailer section after it is
// parsed strictly: declared checksum trailers are verified against the decoded payload, and the signed
// variant additionally verifies x-amz-trailer-signature ("AWS4-HMAC-SHA256-TRAILER" string-to-sign over
// the canonicalized trailers, chained onto the final chunk signature).
class ChunkedSigV4BodyReader final : public http::BodyReader {
public:
    ChunkedSigV4BodyReader(std::unique_ptr<http::BodyReader> inner, bool signed_chunks, util::Sha256Digest signing_key,
                           std::string seed_signature, std::string amz_date, std::string scope,
                           std::optional<uint64_t> decoded_length, bool trailer_expected, bool trailer_signed,
                           bool unverifiable_signatures, std::vector<DeclaredTrailer> declared_trailers)
        : inner_(std::move(inner)),
          signed_(signed_chunks),
          key_(signing_key),
          prev_sig_(std::move(seed_signature)),
          amz_date_(std::move(amz_date)),
          scope_(std::move(scope)),
          decoded_length_(decoded_length),
          trailer_expected_(trailer_expected),
          trailer_signed_(trailer_signed),
          unverifiable_(unverifiable_signatures),
          declared_(std::move(declared_trailers)) {
        for (auto& d : declared_) trailer_digests_.emplace_back(d.algo);
    }

    Task<size_t> read(std::span<std::byte> out) override {
        while (state_ != State::Done) {
            if (state_ == State::Header) {
                // needs more data
                if (!co_await parse_header()) continue;
            } else if (state_ == State::Data) {
                if (chunk_remaining_ == 0) {
                    co_await finish_chunk();
                    continue;
                }
                // A zero-length destination would otherwise read 0 from the inner reader
                // and be mistaken for a truncated body
                if (out.empty()) co_return 0;
                size_t n;
                if (buf_.empty()) {
                    // Nothing buffered: read the chunk's data straight into the caller's
                    // buffer. Routing payload through buf_ costs a copy in and a memmove
                    // out, and -- worse -- caps every read at the fill buffer's 16KiB no
                    // matter how much the caller asked for: the drivers ask for
                    // io_chunk_size (64KiB), so a MiB of body took four times the
                    // coroutine round trips it needed to. Only the framing (chunk headers,
                    // the trailer section) goes through buf_ now
                    size_t want = std::min<uint64_t>(out.size(), chunk_remaining_);
                    n = co_await inner_->read(out.subspan(0, want));
                    if (n == 0) malformed_body("truncated chunk data");
                } else {
                    // Payload left over from the fill that parsed this chunk's header
                    n = std::min({out.size(), buf_.size(), static_cast<size_t>(chunk_remaining_)});
                    std::memcpy(out.data(), buf_.data(), n);
                    buf_.erase(0, n);
                }
                // The delivered bytes are the same either way, so both digests read them
                // from the caller's buffer
                if (chunk_hash_) chunk_hash_->update(std::span(reinterpret_cast<const uint8_t*>(out.data()), n));
                for (auto& d : trailer_digests_) d.update(std::span<const std::byte>(out.data(), n));
                chunk_remaining_ -= n;
                delivered_ += n;
                // Verification is not tied to EOF: after delivering the declared length, synchronously finish the
                // last-chunk verification, the zero-size final chunk, and the trailer -- consumers that stop after
                // exactly length() bytes (cloudproxy) still get full verification; on failure, these n bytes are not
                // delivered
                if (chunk_remaining_ == 0 && decoded_length_ && delivered_ == *decoded_length_)
                    co_await drain_to_done();
                co_return n;
            } else {
                // Trailer
                co_await consume_trailer();
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
        throw S3Error(S3ErrorCode::InvalidRequest, std::string("Malformed aws-chunked body: ") + why);
    }

    Task<bool> fill() {
        std::byte tmp[16 * 1024];
        size_t n = co_await inner_->read(std::span(tmp));
        if (n == 0) co_return false;
        buf_.append(reinterpret_cast<const char*>(tmp), n);
        co_return true;
    }

    // After all data is delivered, drive the state machine to Done: verify the last chunk's signature, parse the
    // zero-size final chunk, consume the trailer. If the next parsed chunk still carries data (actual data exceeds
    // the declared length) -> length reconciliation fails
    Task<void> drain_to_done() {
        while (state_ != State::Done) {
            if (state_ == State::Header) {
                co_await parse_header();
            } else if (state_ == State::Data) {
                if (chunk_remaining_ != 0)
                    throw S3Error(S3ErrorCode::InvalidRequest,
                                  "Decoded body size does not match x-amz-decoded-content-length.");
                co_await finish_chunk();
            } else {
                // Trailer
                co_await consume_trailer();
            }
        }
    }

    // -TRAILER variants: parse "name:value\r\n"* + blank line strictly, then verify. The plain
    // -PAYLOAD variants keep the historical lenient behavior -- drain to EOF ignoring residue
    // (the chunk signature chain already covers every delivered byte)
    Task<void> consume_trailer() {
        if (!trailer_expected_) {
            buf_.clear();
            if (!co_await fill()) state_ = State::Done;
            co_return;
        }
        for (;;) {
            auto eol = buf_.find("\r\n");
            if (eol == std::string::npos) {
                if (trailer_bytes_ + buf_.size() > kTrailerMax) malformed_body("trailer too long");
                if (!co_await fill()) malformed_body("truncated trailer");
                continue;
            }
            std::string line = buf_.substr(0, eol);
            buf_.erase(0, eol + 2);
            // end of the trailer section
            if (line.empty()) break;
            trailer_bytes_ += line.size() + 2;
            if (trailer_bytes_ > kTrailerMax) malformed_body("trailer too long");
            auto colon = line.find(':');
            if (colon == std::string::npos) malformed_body("malformed trailer line");
            std::string name = lower(line.substr(0, colon));
            std::string value = line.substr(colon + 1);
            value.erase(0, value.find_first_not_of(" \t"));
            if (auto end = value.find_last_not_of(" \t"); end != std::string::npos) value.resize(end + 1);
            if (!trailers_.emplace(std::move(name), std::move(value)).second) malformed_body("duplicate trailer");
        }
        verify_trailer();
        // Residue after the blank line (if any) stays undelivered and unread -- same
        // indifference the lenient path has always shown
        state_ = State::Done;
    }

    void verify_trailer() {
        if (trailer_signed_) {
            auto it = trailers_.find("x-amz-trailer-signature");
            if (it == trailers_.end()) malformed_body("missing x-amz-trailer-signature");
            // Canonicalized trailers: "name:value\n" each, sorted by name (std::map order),
            // the signature line itself excluded
            std::string canon;
            for (auto& [n, v] : trailers_)
                if (n != "x-amz-trailer-signature") canon += n + ":" + v + "\n";
            std::string sts = std::string("AWS4-HMAC-SHA256-TRAILER\n") + amz_date_ + "\n" + scope_ + "\n" + prev_sig_ +
                              "\n" + util::sha256_hex(canon);
            if (!constant_time_eq(util::to_hex(util::hmac_sha256(key_, sts)), it->second))
                throw S3Error(S3ErrorCode::SignatureDoesNotMatch, "Trailer signature does not match.");
            trailers_.erase(it);
        } else if (auto it = trailers_.find("x-amz-trailer-signature"); it != trailers_.end()) {
            // A signature on a payload type that does not carry one is a malformed body. The
            // exception is the auth-disabled deployment: the client signed as usual, this side
            // has no secret to check it against, so the line is dropped rather than rejected
            // (it must not survive into the "undeclared trailer" check below either)
            if (!unverifiable_) malformed_body("unexpected x-amz-trailer-signature");
            trailers_.erase(it);
        }
        // Exact match against the x-amz-trailer declaration in both directions: an undeclared
        // trailer is outside every integrity promise; a missing declared one would silently skip
        // the very check the client asked for
        for (auto& d : declared_)
            if (!trailers_.count(d.name)) malformed_body("missing declared trailer");
        for (auto& [n, v] : trailers_) {
            bool declared = false;
            for (auto& d : declared_)
                if (d.name == n) declared = true;
            if (!declared) malformed_body("undeclared trailer");
        }
        for (size_t i = 0; i < declared_.size(); ++i) {
            auto& d = declared_[i];
            auto raw = util::base64_decode(trailers_[d.name]);
            if (!raw || raw->size() != d.bytes)
                throw S3Error(S3ErrorCode::InvalidDigest, "The " + d.name + " trailer you specified is not valid.");
            if (trailer_digests_[i].final_raw() != *raw)
                throw S3Error(S3ErrorCode::BadDigest,
                              "The " + d.name + " you specified did not match what we received.");
        }
    }

    // Returns false when more fill is needed; switches to Data once a header is parsed
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
                if (auto extra = chunk_sig_.find(';'); extra != std::string::npos) chunk_sig_.resize(extra);
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

    // Current chunk data fully read: verify the signature, consume the trailing CRLF (the final chunk has no CRLF, goes
    // straight to Trailer)
    Task<void> finish_chunk() {
        if (signed_) {
            std::string data_hash = chunk_hash_->final_hex();
            chunk_hash_.reset();
            std::string sts = std::string("AWS4-HMAC-SHA256-PAYLOAD\n") + amz_date_ + "\n" + scope_ + "\n" + prev_sig_ +
                              "\n" + kEmptySha256 + "\n" + data_hash;
            std::string expect = util::to_hex(util::hmac_sha256(key_, sts));
            if (!constant_time_eq(expect, chunk_sig_))
                throw S3Error(S3ErrorCode::SignatureDoesNotMatch, "Chunk signature does not match.");
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

    // Trailer size cap: same 16KiB bound L1 applies to HTTP trailers (http-adapter.md §1)
    static constexpr size_t kTrailerMax = 16 * 1024;

    std::string buf_;
    State state_ = State::Header;
    uint64_t chunk_remaining_ = 0;
    uint64_t delivered_ = 0;
    bool final_chunk_ = false;
    std::string chunk_sig_;
    std::optional<util::HashStream> chunk_hash_;

    bool trailer_expected_;
    bool trailer_signed_;
    // Signatures may be present in the framing but cannot be checked (no credential
    // configured): parse past them instead of rejecting
    bool unverifiable_;
    std::vector<DeclaredTrailer> declared_;
    // over the decoded payload, one per declared
    std::vector<StreamingDigest> trailer_digests_;
    // parsed trailer lines (sorted for canon)
    std::map<std::string, std::string> trailers_;
    size_t trailer_bytes_ = 0;
};

// What the x-amz-content-sha256 value says the body looks like on the wire
// (docs/architecture/s3-protocol.md §3.2/§3.3). Deliberately separate from "can this
// request's signature be checked": aws-chunked is a **transport framing**, and it has to
// come off whether or not there is a credential to verify against — see
// install_chunked_body
struct StreamingPayload {
    // the body arrives aws-chunked framed and must be de-framed
    bool chunked = false;
    // the framing carries a per-chunk signature chain
    bool signed_chunks = false;
    // a trailer section follows the zero-size final chunk
    bool trailer = false;
};

StreamingPayload classify_payload(const std::string& payload_hash) {
    StreamingPayload p;
    if (payload_hash == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD" ||
        payload_hash == "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER")
        p.chunked = p.signed_chunks = true;
    else if (payload_hash == "STREAMING-UNSIGNED-PAYLOAD-TRAILER")
        p.chunked = true;
    else if (payload_hash.rfind("STREAMING-", 0) == 0)
        throw S3Error(S3ErrorCode::NotImplemented, "This streaming payload type is not supported.");
    p.trailer = payload_hash.size() >= 8 && payload_hash.compare(payload_hash.size() - 8, 8, "-TRAILER") == 0;
    return p;
}

// x-amz-trailer declaration (docs/architecture/s3-protocol.md §3.3). Validated regardless of body
// presence: a declaration the payload type cannot carry, or one naming a checksum this
// implementation cannot verify, must fail loudly rather than upload with silently-skipped
// integrity
std::vector<DeclaredTrailer> declared_trailers_of(const http::HttpRequest& req, bool trailer_variant) {
    std::vector<DeclaredTrailer> out;
    for (auto& name : parse_declared_trailers(req)) {
        if (!trailer_variant)
            throw S3Error(S3ErrorCode::InvalidRequest, "x-amz-trailer requires a STREAMING-*-TRAILER payload type.");
        const auto* spec = checksum_spec(name);
        if (!spec) {
            if (name.rfind("x-amz-checksum-", 0) == 0)
                throw S3Error(S3ErrorCode::NotImplemented, "The trailing checksum '" + name + "' is not implemented.");
            throw S3Error(S3ErrorCode::InvalidRequest, "The trailer '" + name + "' is not supported.");
        }
        for (auto& d : out)
            if (d.name == name) throw S3Error(S3ErrorCode::InvalidRequest, "Duplicate trailer declared: " + name);
        out.push_back({std::move(name), spec->algo, spec->bytes});
    }
    return out;
}

// Signature-chain inputs for the de-framer; nullptr = de-frame only, verify nothing
struct ChunkSigning {
    util::Sha256Digest key;
    std::string seed_signature;
    std::string amz_date;
    std::string scope;
};

// Streaming payload de-framing (docs/architecture/s3-protocol.md §3.2/§3.3). The caller has already
// decided whether the signature chain can be verified; the framing itself is stripped
// either way, because it is the transport encoding of the body and not an authentication
// artifact: left in place, "b\r\nhello world\r\n0\r\n\r\n" is what lands in the object,
// with the ETag computed over the chunk headers and no error anywhere
void install_chunked_body(http::HttpRequest& req, const StreamingPayload& p, std::vector<DeclaredTrailer> declared,
                          const ChunkSigning* signing) {
    // AWS mandates this header for streaming variants; without it the decoded length is unknown, the
    // "verify when fully read" trigger cannot fire, and the length cannot be reported to the backend
    const std::string* dl = req.headers.find("x-amz-decoded-content-length");
    if (!dl) throw S3Error(S3ErrorCode::InvalidRequest, "Missing required header: x-amz-decoded-content-length");
    // Strict, like Content-Length at L1 (http/model.h): this number becomes the body's
    // length() all the way down to the backend, so "-1" must not arrive there as 2^64-1
    uint64_t decoded_len = 0;
    if (!http::parse_content_length(*dl, decoded_len))
        throw S3Error(S3ErrorCode::InvalidRequest, "Invalid x-amz-decoded-content-length.");
    const bool verify = signing && p.signed_chunks;
    req.body = std::make_unique<ChunkedSigV4BodyReader>(
        std::move(req.body), verify, signing ? signing->key : util::Sha256Digest{},
        signing ? signing->seed_signature : std::string{}, signing ? signing->amz_date : std::string{},
        signing ? signing->scope : std::string{}, decoded_len, p.trailer,
        /*trailer_signed=*/verify && p.trailer, /*unverifiable_signatures=*/signing == nullptr, std::move(declared));
}

}  // namespace

namespace {

// Default implementation for build(): static table from the config file, read-only after construction
class StaticCredentialProvider final : public ICredentialProvider {
public:
    explicit StaticCredentialProvider(const AuthConfig& cfg) {
        for (auto& c : cfg.credentials) creds_[c.access_key] = c.secret_key;
    }
    std::optional<CredentialLookup> lookup(std::string_view ak) const override {
        auto it = creds_.find(ak);
        if (it == creds_.end()) return std::nullopt;
        // Static credentials are always unrestricted (root semantics, docs/architecture/credential-management.md §3)
        return CredentialLookup{it->second, std::nullopt};
    }
    bool has_credentials() const override { return !creds_.empty(); }

private:
    std::map<std::string, util::SecretString, std::less<>> creds_;
};

}  // namespace

SigV4Authenticator SigV4Authenticator::build(const AuthConfig& cfg) {
    SigV4Authenticator a;
    a.region_ = cfg.region;
    a.service_ = cfg.service;
    a.provider_ = std::make_shared<StaticCredentialProvider>(cfg);
    return a;
}

std::string SigV4Authenticator::signature_for(const http::HttpRequest& req, const std::string& secret_key,
                                              const std::string& amz_date, const std::string& scope,
                                              const std::string& signed_headers, const std::string& payload_hash,
                                              bool presigned) const {
    // canonical headers (values taken per the SignedHeaders list; the list must already be sorted;
    // same-name headers comma-joined in order of appearance -- SigV4 rule)
    std::string canon_headers;
    for (auto& name : split(signed_headers, ';')) {
        std::string joined;
        bool found = false;
        for (auto& [k, v] : req.headers.items()) {
            if (!http::HeaderMap::ieq(k, name)) continue;
            if (found) joined += ",";
            joined += canonical_header_value(v);
            found = true;
        }
        if (!found)
            throw S3Error(S3ErrorCode::SignatureDoesNotMatch,
                          "Signed header '" + name + "' is missing from the request.");
        canon_headers += name + ":" + joined + "\n";
    }

    std::string canonical_uri = req.raw_path.empty() ? "/" : req.raw_path;
    // Only presigned requests exclude X-Amz-Signature from the canonical query; when a header-authenticated
    // request carries this query parameter it is signed as an ordinary parameter (matching AWS)
    std::string presigned_exclude = presigned ? "X-Amz-Signature" : "";
    std::ostringstream canonical;
    canonical << req.method << "\n"
              << canonical_uri << "\n"
              << canonical_query(req.raw_query, presigned_exclude) << "\n"
              << canon_headers << "\n"
              << signed_headers << "\n"
              << payload_hash;

    std::string sts = std::string(kAlgo) + "\n" + amz_date + "\n" + scope + "\n" + util::sha256_hex(canonical.str());

    // date/region/service/aws4_request
    auto parts = split(scope, '/');
    auto k = derive_signing_key(secret_key, parts[0], parts[1], parts[2]);
    return util::to_hex(util::hmac_sha256(k, sts));
}

std::optional<std::string> SigV4Authenticator::peek_access_key(const http::HttpRequest& req) {
    try {
        AuthFields f;
        if (auto auth = req.headers.get("Authorization"))
            f = parse_auth_header(*auth);
        else if (auto cred = req.query_get("X-Amz-Credential"))
            parse_credential(*cred, f);
        else
            return std::nullopt;
        if (f.access_key.empty()) return std::nullopt;
        return f.access_key;
    } catch (const S3Error&) {
        // verify reports the malformed header itself
        return std::nullopt;
    }
}

void SigV4Authenticator::strip_transport_framing(http::HttpRequest& req) {
    // No credential is configured, so nothing about this request can be verified — but the
    // aws-chunked framing named by x-amz-content-sha256 still has to come off. It used to
    // stay on: verify_impl returned on the line below before reaching the de-framing step,
    // and the chunk headers were written into the object (an 11-byte body stored as 21
    // bytes, the ETag computed over the framing), with a 200 and no error anywhere. Modern
    // SDKs (aws-cli v2, SDK v3 with default checksums) send this payload type by default,
    // so "start it without credentials and try it out" silently corrupted every upload.
    // Declared checksum trailers are still verified inside the de-framer: like Content-MD5
    // they are integrity declarations, independent of the signature
    const std::string* h = req.headers.find("x-amz-content-sha256");
    if (!h || !req.body) return;
    StreamingPayload p = classify_payload(*h);
    if (!p.chunked) return;
    install_chunked_body(req, p, declared_trailers_of(req, p.trailer), /*signing=*/nullptr);
}

VerifiedIdentity SigV4Authenticator::verify_impl(http::HttpRequest& req, std::span<const std::string_view> services,
                                                 const std::string* explicit_payload_hash) const {
    if (!enabled()) {
        strip_transport_framing(req);
        return {};
    }

    AuthFields f;
    if (auto auth = req.headers.get("Authorization")) {
        f = parse_auth_header(*auth);
        auto date = req.headers.get("x-amz-date");
        if (!date) date = req.headers.get("Date");
        if (!date) malformed("missing x-amz-date");
        f.amz_date = *date;
    } else if (auto alg = req.query_get("X-Amz-Algorithm")) {
        // presigned URL
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

    // scope check: the service must be one of the names this endpoint answers to
    bool service_ok = false;
    std::string names;
    for (auto sv : services) {
        if (f.service == sv) service_ok = true;
        names += (names.empty() ? "" : "|") + std::string(sv);
    }
    if (f.terminal != "aws4_request" || !service_ok || f.region != region_)
        malformed("credential scope does not match this endpoint (" + region_ + "/" + names + ")");
    if (f.amz_date.substr(0, 8) != f.date) malformed("credential date does not match x-amz-date");

    // host must be in SignedHeaders (AWS requirement; under vhost the bucket comes from Host, and a signature
    // not bound to host could be replayed cross-bucket by swapping the Host header) -- enforced for presigned too
    if (!header_is_signed(f.signed_headers, "host")) malformed("SignedHeaders must include 'host'");

    auto t = util::parse_amz_date(f.amz_date);
    if (!t) malformed("cannot parse x-amz-date");
    if (f.presigned) {
        // presigned validity is judged by X-Amz-Expires (docs/architecture/s3-protocol.md §3.4). Expiry only constrains
        // the past side; the issue time must not lead the server by more than 15min (prevents future timestamps from
        // extending validity indefinitely)
        if (*t - clock() > std::chrono::seconds(kMaxClockSkewSec))
            throw S3Error(S3ErrorCode::AccessDenied, "Request is not valid yet");
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
            throw S3Error(S3ErrorCode::RequestTimeTooSkewed,
                          "The difference between the request time and the server's time is too large.");
    }

    // Credentials: one lookup returns both the SK and the policy snapshot -- authorization
    // uses this snapshot, so even if the credential is revoked while this request is in flight, it completes with
    // verify-time semantics
    auto cred = provider_->lookup(f.access_key);
    if (!cred)
        throw S3Error(S3ErrorCode::InvalidAccessKeyId,
                      "The AWS access key ID you provided does not exist in our records.");
    const std::string& secret_key = cred->secret_key;

    // STS session credentials: a session AK is only usable with its
    // matching token; a token alongside a permanent AK is equally invalid. Token match
    // is judged before expiry so a wrong token never reads as merely "expired"
    {
        std::optional<std::string> req_token;
        if (auto h = req.headers.get("x-amz-security-token"))
            req_token = *h;
        else if (auto q = req.query_get("X-Amz-Security-Token"))
            req_token = *q;
        if (cred->session_token) {
            if (!req_token)
                throw S3Error(S3ErrorCode::AccessDenied,
                              "Session credentials require the X-Amz-Security-Token "
                              "header or query parameter.");
            if (!constant_time_eq(*req_token, *cred->session_token))
                throw S3Error(S3ErrorCode::InvalidToken, "The security token included in the request is invalid.");
            if (cred->session_expires && clock() > *cred->session_expires)
                throw S3Error(S3ErrorCode::ExpiredToken, "The provided security token has expired.");
        } else if (req_token) {
            throw S3Error(S3ErrorCode::InvalidToken, "The security token included in the request is invalid.");
        }
    }

    // payload hash (streaming variants participate in the canonical request by their literal value)
    std::string payload_hash;
    StreamingPayload stream;
    if (explicit_payload_hash) {
        // STS form POST: caller hashed the body
        payload_hash = *explicit_payload_hash;
    } else if (f.presigned) {
        // A presigned URL is signed with UNSIGNED-PAYLOAD unless its signer committed to a
        // payload hash, and there are exactly two places that commitment can live -- both
        // covered by the signature itself, which is what makes honouring them safe:
        //   - the X-Amz-Content-Sha256 query parameter, part of the canonical query (the
        //     only parameter excluded from it is X-Amz-Signature). It is already on the
        //     common query allowlist, so it was being accepted and then ignored, and a
        //     client that presigned with a real digest got SignatureDoesNotMatch;
        //   - the x-amz-content-sha256 header, but only when the signer listed it in
        //     SignedHeaders and it therefore entered the canonical headers. An unsigned
        //     header must not be consulted: a client sending one it did not sign would
        //     otherwise flip the hash this side computes with and break its own URL.
        // Neither can be added or altered by anyone but the signer -- doing so changes the
        // canonical request and the signature stops matching
        payload_hash = "UNSIGNED-PAYLOAD";
        if (auto q = req.query_get("X-Amz-Content-Sha256")) {
            payload_hash = *q;
        } else if (header_is_signed(f.signed_headers, "x-amz-content-sha256")) {
            if (const std::string* h = req.headers.find("x-amz-content-sha256")) payload_hash = *h;
        }
        stream = classify_payload(payload_hash);
    } else if (const std::string* h = req.headers.find("x-amz-content-sha256")) {
        payload_hash = *h;
        stream = classify_payload(payload_hash);
    } else {
        bool has_body = req.body && req.body->length().value_or(0) > 0;
        if (has_body) throw S3Error(S3ErrorCode::InvalidRequest, "Missing required header: x-amz-content-sha256");
        payload_hash = kEmptySha256;
    }

    std::string scope = f.date + "/" + f.region + "/" + f.service + "/aws4_request";
    std::string expect = signature_for(req, secret_key, f.amz_date, scope, f.signed_headers, payload_hash, f.presigned);
    if (!constant_time_eq(expect, f.signature))
        throw S3Error(S3ErrorCode::SignatureDoesNotMatch,
                      "The request signature we calculated does not match the signature you "
                      "provided.");

    std::vector<DeclaredTrailer> declared_trailers = declared_trailers_of(req, stream.trailer);

    // Streaming payload verification (docs/architecture/s3-protocol.md §3.2/§3.3)
    if (stream.chunked && req.body) {
        ChunkSigning signing{derive_signing_key(secret_key, f.date, f.region, f.service), f.signature, f.amz_date,
                             scope};
        install_chunked_body(req, stream, std::move(declared_trailers), &signing);
    } else if (!explicit_payload_hash && is_hex_digest(payload_hash) && req.body) {
        // A declared empty digest (sha256("")) still gets wrapped for verification: without checking that the
        // actual body is empty, empty digest + non-empty body would slip the body out of signature protection
        req.body = std::make_unique<Sha256VerifyingReader>(std::move(req.body), payload_hash);
    }
    return {std::move(f.access_key), std::move(cred->policy), std::move(cred->tenant), cred->tenant_admin};
}

void SigV4Authenticator::sign(http::HttpRequest& req, const Credential& cred, std::string payload_hash,
                              std::string_view service) const {
    const std::string svc = service.empty() ? service_ : std::string(service);
    if (payload_hash.empty()) payload_hash = kEmptySha256;
    std::string amz_date = util::amz_date(clock());
    req.headers.set("x-amz-date", amz_date);
    req.headers.set("x-amz-content-sha256", payload_hash);

    // SignedHeaders: host + all x-amz-* (sorted)
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
    std::string scope = date + "/" + region_ + "/" + svc + "/aws4_request";
    std::string sig = signature_for(req, cred.secret_key, amz_date, scope, signed_headers, payload_hash);
    req.headers.set("Authorization", std::string(kAlgo) + " Credential=" + cred.access_key + "/" + scope +
                                         ", SignedHeaders=" + signed_headers + ", Signature=" + sig);
}

}  // namespace lights3::s3
