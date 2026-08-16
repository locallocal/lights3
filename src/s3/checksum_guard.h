// L2: request body integrity verification (docs/gaps.md §5.6). Content-MD5 and x-amz-checksum-*
// are both "client pre-declares the body digest"; unlike x-amz-content-sha256 they are independent of
// the signature: they still apply when auth is disabled, catching request bodies rewritten in transit.
//
// Shape mirrors Sha256VerifyingReader in sigv4.cc: compare once the self-reported length() is fully read
// (consumers like cloudproxy read exactly length() bytes and never do an extra read to EOF); the EOF path
// covers the no-length case. On mismatch the exception is thrown before the last chunk is delivered downstream,
// working with backend.h's contract that "if body.read throws, the backend must not commit".
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
    enum class Algo { Md5, Sha1, Sha256, Crc32, Crc32c };
    Algo algo;
    std::string header;    // points back to the specific header when reporting errors
    std::string expected;  // decoded raw bytes
};

class ChecksumVerifyingReader final : public http::BodyReader {
public:
    ChecksumVerifyingReader(std::unique_ptr<http::BodyReader> inner,
                            std::vector<ExpectedDigest> expected)
        : inner_(std::move(inner)), expected_(std::move(expected)) {
        for (auto& e : expected_) {
            switch (e.algo) {
                case ExpectedDigest::Algo::Md5:
                    hashes_.push_back(std::make_unique<util::HashStream>(
                        util::HashStream::Algo::Md5));
                    break;
                case ExpectedDigest::Algo::Sha1:
                    hashes_.push_back(std::make_unique<util::HashStream>(
                        util::HashStream::Algo::Sha1));
                    break;
                case ExpectedDigest::Algo::Sha256:
                    hashes_.push_back(std::make_unique<util::HashStream>(
                        util::HashStream::Algo::Sha256));
                    break;
                default:
                    hashes_.push_back(nullptr);  // crc family goes through crcs_
                    break;
            }
            crcs_.push_back(0);
        }
    }

    Task<size_t> read(std::span<std::byte> buf) override {
        if (done_) co_return 0;
        size_t n = co_await inner_->read(buf);
        if (n > 0) {
            auto bytes = std::span(buf.data(), n);
            for (size_t i = 0; i < expected_.size(); ++i) {
                if (hashes_[i]) {
                    hashes_[i]->update(
                        std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
                } else if (expected_[i].algo == ExpectedDigest::Algo::Crc32) {
                    crcs_[i] = util::crc32_update(crcs_[i], bytes);
                } else {
                    crcs_[i] = util::crc32c_update(crcs_[i], bytes);
                }
            }
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
            std::string got;
            if (hashes_[i]) {
                auto b = hashes_[i]->final_bytes();
                got.assign(b.begin(), b.end());
            } else {
                for (int s = 24; s >= 0; s -= 8)  // crc compared as 4 big-endian bytes (AWS format)
                    got.push_back(char((crcs_[i] >> s) & 0xff));
            }
            if (got != expected_[i].expected)
                throw S3Error(S3ErrorCode::BadDigest,
                              "The " + expected_[i].header +
                                  " you specified did not match what we received.");
        }
    }

    std::unique_ptr<http::BodyReader> inner_;
    std::vector<ExpectedDigest> expected_;
    std::vector<std::unique_ptr<util::HashStream>> hashes_;
    std::vector<uint32_t> crcs_;
    uint64_t consumed_ = 0;
    bool done_ = false;
};

// Digests declared in request headers. Malformed format (not base64 / wrong length) is InvalidDigest(400),
// kept separate from BadDigest for "digest mismatch" -- conflating the two leaves clients unable to tell
// whether they computed it wrong or the transport rewrote the body
inline std::vector<ExpectedDigest> parse_expected_digests(const http::HttpRequest& req) {
    struct Spec {
        const char* header;
        ExpectedDigest::Algo algo;
        size_t bytes;
    };
    static constexpr Spec kSpecs[] = {
        {"Content-MD5", ExpectedDigest::Algo::Md5, 16},
        {"x-amz-checksum-crc32", ExpectedDigest::Algo::Crc32, 4},
        {"x-amz-checksum-crc32c", ExpectedDigest::Algo::Crc32c, 4},
        {"x-amz-checksum-sha1", ExpectedDigest::Algo::Sha1, 20},
        {"x-amz-checksum-sha256", ExpectedDigest::Algo::Sha256, 32},
    };
    std::vector<ExpectedDigest> out;
    for (auto& sp : kSpecs) {
        auto v = req.headers.get(sp.header);
        if (!v) continue;
        auto raw = util::base64_decode(*v);
        if (!raw || raw->size() != sp.bytes)
            throw S3Error(S3ErrorCode::InvalidDigest,
                          std::string("The ") + sp.header + " you specified is not valid.");
        out.push_back({sp.algo, sp.header, std::move(*raw)});
    }
    return out;
}

// Digests declared with an empty body (GET/DELETE etc.) are verified too: the MD5 of an empty body is a
// fixed value, and a mismatching declaration is still BadDigest
inline void install_checksum_guard(http::HttpRequest& req) {
    auto expected = parse_expected_digests(req);
    if (expected.empty()) return;
    if (!req.body) req.body = std::make_unique<http::StringBodyReader>("");
    req.body = std::make_unique<ChecksumVerifyingReader>(std::move(req.body),
                                                         std::move(expected));
}

}  // namespace lights3::s3
