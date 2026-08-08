// L2: 请求体完整性校验（docs/gaps.md §5.6）。Content-MD5 与 x-amz-checksum-*
// 都是"客户端预先声明 body 摘要"，与 x-amz-content-sha256 的区别在于它们独立于
// 签名：认证关闭时同样生效，作用是挡住传输途中被改写的请求体。
//
// 形态照搬 sigv4.cc 的 Sha256VerifyingReader：读满自报 length() 即比对
//（cloudproxy 等消费者只读 length() 字节、不再多读一次到 EOF），EOF 路径兜底
// 无长度的情况；比对失败抛出时最后一块尚未交付下游，配合 backend.h 的
//"body.read 抛异常则后端不得提交"契约。
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

// 一次声明可以带多个摘要（Content-MD5 + 某个 x-amz-checksum-*），全部都要校验
struct ExpectedDigest {
    enum class Algo { Md5, Sha1, Sha256, Crc32, Crc32c };
    Algo algo;
    std::string header;    // 报错时回指具体是哪个头
    std::string expected;  // 已解码的原始字节
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
                    hashes_.push_back(nullptr);  // crc 系走 crcs_
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
                for (int s = 24; s >= 0; s -= 8)  // crc 按大端 4 字节比对（AWS 形态）
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

// 请求头里声明的摘要。格式非法（非 base64 / 长度不对）是 InvalidDigest(400)，
// 与"摘要不符"的 BadDigest 分开——把两者混为一谈会让客户端分不清是自己算错了
// 还是链路改写了 body
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

// body 为空（GET/DELETE 等）时声明的摘要同样要校验：空 body 的 MD5 是确定值，
// 声明不符照样是 BadDigest
inline void install_checksum_guard(http::HttpRequest& req) {
    auto expected = parse_expected_digests(req);
    if (expected.empty()) return;
    if (!req.body) req.body = std::make_unique<http::StringBodyReader>("");
    req.body = std::make_unique<ChecksumVerifyingReader>(std::move(req.body),
                                                         std::move(expected));
}

}  // namespace lights3::s3
