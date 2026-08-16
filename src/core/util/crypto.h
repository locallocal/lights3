// L4: SHA256 / HMAC-SHA256 / MD5 / AES-256-GCM (OpenSSL EVP wrappers),
// used by SigV4, ETag, and credential at-rest encryption
#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

namespace lights3::util {

using Sha256Digest = std::array<uint8_t, 32>;
using Md5Digest = std::array<uint8_t, 16>;

Sha256Digest sha256(std::span<const uint8_t> data);
Sha256Digest sha256(std::string_view data);
std::string sha256_hex(std::string_view data);

Sha256Digest hmac_sha256(std::span<const uint8_t> key, std::string_view data);

// AES-256-GCM authenticated encryption (docs/credential-management.md §10.1, SK
// at-rest encryption). seal output layout: 12B random nonce || ciphertext || 16B tag;
// open returns nullopt when it cannot decrypt (truncation/tampering/wrong key)
using Aes256Key = std::array<uint8_t, 32>;
std::string aes256gcm_seal(const Aes256Key& key, std::string_view plaintext);
std::optional<std::string> aes256gcm_open(const Aes256Key& key, std::string_view sealed);

// Zeroization the compiler cannot elide (OPENSSL_cleanse): temporary strings of
// decrypted SKs are wiped in place after use (docs/gaps.md §4 — for a system doing
// SK at-rest encryption, lingering plaintext is a missing link in the defense chain)
void secure_wipe(std::string& s);

// A string wiped on destruction: once decrypted, an SK lives long-term in the
// credential table, and wiping only the decryption-failure path would mean fixing
// only the shortest link of the defense chain. Inheriting std::string means existing
// const std::string& parameters, string_view conversions, comparisons, and JSON
// assignments all work unchanged.
// Known limitation: old buffers left behind by reallocation are outside the wipe's
// reach — a real fix needs a custom allocator
class SecretString : public std::string {
public:
    using std::string::string;  // inherits const char* / string_view / (n, c) etc. in one go
    SecretString() = default;
    SecretString(std::string s) : std::string(std::move(s)) {}  // NOLINT(google-explicit-constructor)
    SecretString(const SecretString&) = default;
    SecretString(SecretString&&) = default;
    SecretString& operator=(const SecretString&) = default;
    SecretString& operator=(SecretString&&) = default;
    using std::string::operator=;
    ~SecretString() { secure_wipe(*this); }
};

// Incremental hashing (streaming body verification / ETag computation)
class HashStream {
public:
    enum class Algo { Sha256, Sha1, Md5 };
    explicit HashStream(Algo algo);
    ~HashStream();
    HashStream(const HashStream&) = delete;

    void update(std::span<const uint8_t> data);
    std::string final_hex();                  // may be called only once
    std::vector<uint8_t> final_bytes();       // ditto, pick one of the two. base64-style digests use it to skip the hex round-trip

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace lights3::util
