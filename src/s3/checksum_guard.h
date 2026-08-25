// L2: request body integrity verification (docs/gaps.md §5.6). Content-MD5 and x-amz-checksum-*
// are both "client pre-declares the body digest"; unlike x-amz-content-sha256 they are independent of
// the signature: they still apply when auth is disabled, catching request bodies rewritten in transit.
//
// Shape mirrors Sha256VerifyingReader in sigv4.cc: compare once the self-reported length() is fully read
// (consumers like cloudproxy read exactly length() bytes and never do an extra read to EOF); the EOF path
// covers the no-length case. On mismatch the exception is thrown before the last chunk is delivered downstream,
// working with backend.h's contract that "if body.read throws, the backend must not commit".
//
// Digests declared as aws-chunked **trailers** (x-amz-trailer, docs/s3-protocol.md §3.3) are verified
// inside ChunkedSigV4BodyReader instead -- the expected value only arrives after the payload -- reusing
// StreamingDigest and the checksum_spec table below.
#pragma once

#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "core/task.h"
#include "core/util/checksum.h"
#include "core/util/crypto.h"
#include "core/util/hex.h"
#include "http/model.h"
#include "s3/errors.h"

namespace lights3::s3 {

// One declaration may carry multiple digests (Content-MD5 + some x-amz-checksum-*); all must be verified
struct ExpectedDigest {
    enum class Algo { Md5, Sha1, Sha256, Crc32, Crc32c, Crc64Nvme };
    Algo algo;
    std::string header;    // points back to the specific header when reporting errors
    std::string expected;  // decoded raw bytes
};

// The x-amz-checksum-* family (header or trailer form; Content-MD5 is header-only and handled apart)
struct ChecksumSpec {
    std::string_view header;  // lowercase
    ExpectedDigest::Algo algo;
    size_t bytes;  // decoded digest length; CRCs travel base64-encoded big-endian
};
inline constexpr ChecksumSpec kChecksumSpecs[] = {
    {"x-amz-checksum-crc32", ExpectedDigest::Algo::Crc32, 4},
    {"x-amz-checksum-crc32c", ExpectedDigest::Algo::Crc32c, 4},
    {"x-amz-checksum-crc64nvme", ExpectedDigest::Algo::Crc64Nvme, 8},
    {"x-amz-checksum-sha1", ExpectedDigest::Algo::Sha1, 20},
    {"x-amz-checksum-sha256", ExpectedDigest::Algo::Sha256, 32},
};

inline const ChecksumSpec* checksum_spec(std::string_view lower_name) {
    for (auto& sp : kChecksumSpecs)
        if (sp.header == lower_name) return &sp;
    return nullptr;
}

// Incremental digest over one algorithm; final_raw() yields the AWS wire form (hash bytes, or the
// CRC as 4/8 big-endian bytes) for direct comparison against a base64-decoded declared value
class StreamingDigest {
public:
    explicit StreamingDigest(ExpectedDigest::Algo algo) : algo_(algo) {
        auto make = [this](util::HashStream::Algo a) {
            hash_ = std::make_unique<util::HashStream>(a);  // HashStream is not movable
        };
        switch (algo) {
            case ExpectedDigest::Algo::Md5: make(util::HashStream::Algo::Md5); break;
            case ExpectedDigest::Algo::Sha1: make(util::HashStream::Algo::Sha1); break;
            case ExpectedDigest::Algo::Sha256: make(util::HashStream::Algo::Sha256); break;
            default: break;  // crc family accumulates in crc_
        }
    }

    void update(std::span<const std::byte> data) {
        if (hash_) {
            hash_->update(
                std::span(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
        } else if (algo_ == ExpectedDigest::Algo::Crc32) {
            crc_ = util::crc32_update(static_cast<uint32_t>(crc_), data);
        } else if (algo_ == ExpectedDigest::Algo::Crc32c) {
            crc_ = util::crc32c_update(static_cast<uint32_t>(crc_), data);
        } else {
            crc_ = util::crc64nvme_update(crc_, data);
        }
    }

    std::string final_raw() {  // may be called only once (HashStream contract)
        if (hash_) {
            auto b = hash_->final_bytes();
            return {b.begin(), b.end()};
        }
        std::string out;
        for (int s = (algo_ == ExpectedDigest::Algo::Crc64Nvme ? 56 : 24); s >= 0; s -= 8)
            out.push_back(char((crc_ >> s) & 0xff));
        return out;
    }

private:
    ExpectedDigest::Algo algo_;
    std::unique_ptr<util::HashStream> hash_;
    uint64_t crc_ = 0;
};

class ChecksumVerifyingReader final : public http::BodyReader {
public:
    ChecksumVerifyingReader(std::unique_ptr<http::BodyReader> inner,
                            std::vector<ExpectedDigest> expected)
        : inner_(std::move(inner)), expected_(std::move(expected)) {
        for (auto& e : expected_) digests_.emplace_back(e.algo);
    }

    Task<size_t> read(std::span<std::byte> buf) override {
        if (done_) co_return 0;
        size_t n = co_await inner_->read(buf);
        if (n > 0) {
            for (auto& d : digests_) d.update(std::span(buf.data(), n));
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
        for (size_t i = 0; i < expected_.size(); ++i) {
            if (digests_[i].final_raw() != expected_[i].expected)
                throw S3Error(S3ErrorCode::BadDigest,
                              "The " + expected_[i].header +
                                  " you specified did not match what we received.");
        }
    }

    std::unique_ptr<http::BodyReader> inner_;
    std::vector<ExpectedDigest> expected_;
    std::vector<StreamingDigest> digests_;
    uint64_t consumed_ = 0;
    bool done_ = false;
};

// Digests declared in request headers. Malformed format (not base64 / wrong length) is InvalidDigest(400),
// kept separate from BadDigest for "digest mismatch" -- conflating the two leaves clients unable to tell
// whether they computed it wrong or the transport rewrote the body
inline std::vector<ExpectedDigest> parse_expected_digests(const http::HttpRequest& req) {
    auto decode = [&](std::string_view header, size_t bytes) -> std::optional<std::string> {
        auto v = req.headers.get(header);
        if (!v) return std::nullopt;
        auto raw = util::base64_decode(*v);
        if (!raw || raw->size() != bytes)
            throw S3Error(S3ErrorCode::InvalidDigest,
                          std::string("The ") + std::string(header) +
                              " you specified is not valid.");
        return raw;
    };
    std::vector<ExpectedDigest> out;
    if (auto raw = decode("Content-MD5", 16))
        out.push_back({ExpectedDigest::Algo::Md5, "Content-MD5", std::move(*raw)});
    for (auto& sp : kChecksumSpecs)
        if (auto raw = decode(sp.header, sp.bytes))
            out.push_back({sp.algo, std::string(sp.header), std::move(*raw)});
    return out;
}

// x-amz-trailer: comma-separated declared trailer names (lowercased, OWS-trimmed, empties dropped)
inline std::vector<std::string> parse_declared_trailers(const http::HttpRequest& req) {
    std::vector<std::string> out;
    auto v = req.headers.get("x-amz-trailer");
    if (!v) return out;
    std::string cur;
    auto flush = [&] {
        while (!cur.empty() && (cur.back() == ' ' || cur.back() == '\t')) cur.pop_back();
        if (!cur.empty()) out.push_back(std::move(cur));
        cur.clear();
    };
    for (char c : *v) {
        if (c == ',') {
            flush();
        } else if ((c == ' ' || c == '\t') && cur.empty()) {
            continue;  // leading OWS
        } else {
            cur.push_back(http::HeaderMap::lower(c));
        }
    }
    flush();
    return out;
}

// x-amz-checksum-algorithm / x-amz-sdk-checksum-algorithm: the SDK's declaration of which checksum
// accompanies the request. Previously swallowed silently -- accepting the declaration while skipping the
// check it names is exactly the "silent lie" this layer exists to prevent. Rules: an unknown algorithm is
// InvalidRequest; a declaration whose digest arrives neither as a header nor as a declared trailer on a
// request that has a body is InvalidRequest. A body-less declaration (CreateMultipartUpload) is accepted:
// each part carries -- and is verified against -- its own digest.
inline void validate_checksum_algorithm(const http::HttpRequest& req) {
    for (std::string_view h : {"x-amz-checksum-algorithm", "x-amz-sdk-checksum-algorithm"}) {
        auto v = req.headers.get(h);
        if (!v) continue;
        std::string name = "x-amz-checksum-";
        for (char c : *v) name.push_back(http::HeaderMap::lower(c));
        if (!checksum_spec(name))
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "Unsupported value for " + std::string(h) + ": " + *v);
        bool provided = req.headers.get(name).has_value();
        if (!provided)
            for (auto& t : parse_declared_trailers(req))
                if (t == name) provided = true;
        bool has_body = req.body && req.body->length().value_or(1) > 0;
        if (!provided && has_body)
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "The " + std::string(h) + " header requires a matching " + name +
                              " header or trailer.");
    }
}

// Digests declared with an empty body (GET/DELETE etc.) are verified too: the MD5 of an empty body is a
// fixed value, and a mismatching declaration is still BadDigest
inline void install_checksum_guard(http::HttpRequest& req) {
    validate_checksum_algorithm(req);
    auto expected = parse_expected_digests(req);
    if (expected.empty()) return;
    if (!req.body) req.body = std::make_unique<http::StringBodyReader>("");
    req.body = std::make_unique<ChecksumVerifyingReader>(std::move(req.body),
                                                         std::move(expected));
}

}  // namespace lights3::s3
