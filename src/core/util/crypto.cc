#include "core/util/crypto.h"

#include <openssl/evp.h>
#include <openssl/hmac.h>

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

struct HashStream::Impl {
    EVP_MD_CTX* ctx = nullptr;
};

HashStream::HashStream(Algo algo) : impl_(std::make_unique<Impl>()) {
    impl_->ctx = EVP_MD_CTX_new();
    const EVP_MD* md = (algo == Algo::Sha256) ? EVP_sha256() : EVP_md5();
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
    uint8_t buf[EVP_MAX_MD_SIZE];
    unsigned int len = 0;
    if (!EVP_DigestFinal_ex(impl_->ctx, buf, &len))
        throw std::runtime_error("EVP_DigestFinal failed");
    return to_hex(std::span(buf, len));
}

}  // namespace lights3::util
