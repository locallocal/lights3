#include "core/util/crypto.h"

#include <openssl/evp.h>
#include <openssl/hmac.h>
#include <unistd.h>

#include <cstring>
#include <stdexcept>

#include "core/util/hex.h"

namespace lights3::util {

Sha256Digest sha256(std::span<const uint8_t> data) {
    Sha256Digest out{};
    unsigned int len = 0;
    if (!EVP_Digest(data.data(), data.size(), out.data(), &len, EVP_sha256(), nullptr))
        throw std::runtime_error("EVP_Digest(sha256) failed");
    return out;
}

Sha256Digest sha256(std::string_view data) {
    return sha256(std::span(reinterpret_cast<const uint8_t*>(data.data()), data.size()));
}

std::string sha256_hex(std::string_view data) {
    auto d = sha256(data);
    return to_hex(d);
}

Sha256Digest hmac_sha256(std::span<const uint8_t> key, std::string_view data) {
    Sha256Digest out{};
    unsigned int len = 0;
    if (!HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
              reinterpret_cast<const uint8_t*>(data.data()), data.size(), out.data(), &len))
        throw std::runtime_error("HMAC(sha256) failed");
    return out;
}

namespace {

constexpr size_t kGcmNonceLen = 12;
constexpr size_t kGcmTagLen = 16;

struct CipherCtx {
    EVP_CIPHER_CTX* ctx = EVP_CIPHER_CTX_new();
    ~CipherCtx() {
        if (ctx) EVP_CIPHER_CTX_free(ctx);
    }
};

}  // namespace

std::string aes256gcm_seal(const Aes256Key& key, std::string_view plaintext) {
    uint8_t nonce[kGcmNonceLen];
    if (::getentropy(nonce, sizeof(nonce)) != 0)
        throw std::runtime_error("getentropy failed: cannot generate GCM nonce");

    CipherCtx c;
    if (!c.ctx ||
        !EVP_EncryptInit_ex(c.ctx, EVP_aes_256_gcm(), nullptr, key.data(), nonce))
        throw std::runtime_error("EVP_EncryptInit(aes-256-gcm) failed");

    std::string out(kGcmNonceLen + plaintext.size() + kGcmTagLen, '\0');
    memcpy(out.data(), nonce, kGcmNonceLen);
    auto* ct = reinterpret_cast<uint8_t*>(out.data()) + kGcmNonceLen;
    int n = 0;
    if (!EVP_EncryptUpdate(c.ctx, ct, &n,
                           reinterpret_cast<const uint8_t*>(plaintext.data()),
                           static_cast<int>(plaintext.size())) ||
        !EVP_EncryptFinal_ex(c.ctx, ct + n, &n) ||
        !EVP_CIPHER_CTX_ctrl(c.ctx, EVP_CTRL_GCM_GET_TAG, kGcmTagLen,
                             out.data() + kGcmNonceLen + plaintext.size()))
        throw std::runtime_error("EVP_Encrypt(aes-256-gcm) failed");
    return out;
}

std::optional<std::string> aes256gcm_open(const Aes256Key& key, std::string_view sealed) {
    if (sealed.size() < kGcmNonceLen + kGcmTagLen) return std::nullopt;
    auto* p = reinterpret_cast<const uint8_t*>(sealed.data());
    size_t ct_len = sealed.size() - kGcmNonceLen - kGcmTagLen;

    CipherCtx c;
    if (!c.ctx || !EVP_DecryptInit_ex(c.ctx, EVP_aes_256_gcm(), nullptr, key.data(), p))
        throw std::runtime_error("EVP_DecryptInit(aes-256-gcm) failed");

    std::string out(ct_len, '\0');
    int n = 0;
    // Failure paths wipe the partially decrypted plaintext (tag verification happens
    // in Final; bytes are already readable during the Update phase)
    if (!EVP_DecryptUpdate(c.ctx, reinterpret_cast<uint8_t*>(out.data()), &n,
                           p + kGcmNonceLen, static_cast<int>(ct_len))) {
        secure_wipe(out);
        return std::nullopt;
    }
    // Tag verification happens in Final: failure = tampered ciphertext or wrong key
    uint8_t tag[kGcmTagLen];
    memcpy(tag, p + kGcmNonceLen + ct_len, kGcmTagLen);
    if (!EVP_CIPHER_CTX_ctrl(c.ctx, EVP_CTRL_GCM_SET_TAG, kGcmTagLen, tag)) {
        secure_wipe(out);
        return std::nullopt;
    }
    int fin = 0;
    if (EVP_DecryptFinal_ex(c.ctx, reinterpret_cast<uint8_t*>(out.data()) + n, &fin) <= 0) {
        secure_wipe(out);
        return std::nullopt;
    }
    return out;
}

void secure_wipe(std::string& s) {
    if (!s.empty()) OPENSSL_cleanse(s.data(), s.size());
    s.clear();
}

struct HashStream::Impl {
    EVP_MD_CTX* ctx = nullptr;
};

HashStream::HashStream(Algo algo) : impl_(std::make_unique<Impl>()) {
    impl_->ctx = EVP_MD_CTX_new();
    const EVP_MD* md = (algo == Algo::Sha256)  ? EVP_sha256()
                       : (algo == Algo::Sha1) ? EVP_sha1()
                                              : EVP_md5();
    if (!impl_->ctx || !EVP_DigestInit_ex(impl_->ctx, md, nullptr))
        throw std::runtime_error("EVP_DigestInit failed");
}

HashStream::~HashStream() {
    if (impl_ && impl_->ctx) EVP_MD_CTX_free(impl_->ctx);
}

void HashStream::update(std::span<const uint8_t> data) {
    if (!EVP_DigestUpdate(impl_->ctx, data.data(), data.size()))
        throw std::runtime_error("EVP_DigestUpdate failed");
}

std::string HashStream::final_hex() {
    auto b = final_bytes();
    return to_hex(std::span(b.data(), b.size()));
}

std::vector<uint8_t> HashStream::final_bytes() {
    uint8_t buf[EVP_MAX_MD_SIZE];
    unsigned int len = 0;
    if (!EVP_DigestFinal_ex(impl_->ctx, buf, &len))
        throw std::runtime_error("EVP_DigestFinal failed");
    return std::vector<uint8_t>(buf, buf + len);
}

}  // namespace lights3::util
