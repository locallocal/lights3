// L4: SHA256 / HMAC-SHA256 / MD5（OpenSSL EVP 封装），SigV4 与 ETag 使用
#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <string_view>

namespace lights3::util {

using Sha256Digest = std::array<uint8_t, 32>;
using Md5Digest = std::array<uint8_t, 16>;

Sha256Digest sha256(std::span<const uint8_t> data);
Sha256Digest sha256(std::string_view data);
std::string sha256_hex(std::string_view data);

Sha256Digest hmac_sha256(std::span<const uint8_t> key, std::string_view data);

// 增量哈希（流式 body 校验 / ETag 计算）
class HashStream {
public:
    enum class Algo { Sha256, Md5 };
    explicit HashStream(Algo algo);
    ~HashStream();
    HashStream(const HashStream&) = delete;

    void update(std::span<const uint8_t> data);
    std::string final_hex();  // 只能调用一次

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace lights3::util
