// L4: SHA256 / HMAC-SHA256 / MD5 / AES-256-GCM（OpenSSL EVP 封装），
// SigV4、ETag 与凭证 at-rest 加密使用
#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
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

// AES-256-GCM 认证加密（docs/credential-management.md §10.1，SK at-rest 加密）。
// seal 输出布局：12B 随机 nonce || ciphertext || 16B tag；
// open 解不开（截断/篡改/密钥不符）返回 nullopt
using Aes256Key = std::array<uint8_t, 32>;
std::string aes256gcm_seal(const Aes256Key& key, std::string_view plaintext);
std::optional<std::string> aes256gcm_open(const Aes256Key& key, std::string_view sealed);

// 编译器删不掉的清零（OPENSSL_cleanse）：解出的 SK 临时串用完就地擦
// （docs/gaps.md §4——对做了 SK at-rest 加密的系统，明文残留是防护链缺环）
void secure_wipe(std::string& s);

// 析构即擦除的字符串：SK 解出来之后要在凭证表里长期存活，只擦解密失败路径
// 等于防护链只修了最短那一段。继承 std::string 是为了让既有的
// const std::string& 形参、string_view 转换、比较与 JSON 赋值一处都不用改。
// 已知局限：扩容遗留的旧缓冲区不在擦除范围内——根治需要自定义分配器
class SecretString : public std::string {
public:
    using std::string::string;  // const char* / string_view / (n, c) 等一并继承
    SecretString() = default;
    SecretString(std::string s) : std::string(std::move(s)) {}  // NOLINT(google-explicit-constructor)
    SecretString(const SecretString&) = default;
    SecretString(SecretString&&) = default;
    SecretString& operator=(const SecretString&) = default;
    SecretString& operator=(SecretString&&) = default;
    using std::string::operator=;
    ~SecretString() { secure_wipe(*this); }
};

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
