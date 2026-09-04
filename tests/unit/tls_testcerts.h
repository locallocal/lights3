// Test-only certificate factory (roadmap §4.1 tests): self-signed / CA-signed
// EC P-256 certificates generated at runtime through the OpenSSL API, so the
// TLS cases need no fixture files and can mint fresh material for reload tests
#pragma once

#include <openssl/bio.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <cstdio>
#include <stdexcept>
#include <string>

namespace tls_test {

struct Cert {
    std::string cert_pem;
    std::string key_pem;
    std::string cn;
};

namespace detail {
inline std::string pem_of(X509* x) {
    BIO* b = BIO_new(BIO_s_mem());
    PEM_write_bio_X509(b, x);
    char* p = nullptr;
    long n = BIO_get_mem_data(b, &p);
    std::string out(p, static_cast<size_t>(n));
    BIO_free(b);
    return out;
}
inline std::string pem_of(EVP_PKEY* k) {
    BIO* b = BIO_new(BIO_s_mem());
    PEM_write_bio_PrivateKey(b, k, nullptr, nullptr, 0, nullptr, nullptr);
    char* p = nullptr;
    long n = BIO_get_mem_data(b, &p);
    std::string out(p, static_cast<size_t>(n));
    BIO_free(b);
    return out;
}
inline X509* x509_of(const std::string& pem) {
    BIO* b = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
    X509* x = PEM_read_bio_X509(b, nullptr, nullptr, nullptr);
    BIO_free(b);
    return x;
}
inline EVP_PKEY* key_of(const std::string& pem) {
    BIO* b = BIO_new_mem_buf(pem.data(), static_cast<int>(pem.size()));
    EVP_PKEY* k = PEM_read_bio_PrivateKey(b, nullptr, nullptr, nullptr);
    BIO_free(b);
    return k;
}
inline void add_ext(X509* cert, X509* issuer, int nid, const char* value) {
    X509V3_CTX ctx;
    X509V3_set_ctx_nodb(&ctx);
    X509V3_set_ctx(&ctx, issuer, cert, nullptr, nullptr, 0);
    X509_EXTENSION* ex = X509V3_EXT_conf_nid(nullptr, &ctx, nid, value);
    if (!ex) throw std::runtime_error(std::string("bad extension ") + value);
    X509_add_ext(cert, ex, -1);
    X509_EXTENSION_free(ex);
}
}  // namespace detail

// issuer = nullptr -> self-signed. ca = true marks the certificate as a CA.
// Server/client leaves get SAN DNS:localhost, IP:127.0.0.1 (+ the CN as DNS)
inline Cert make_cert(const std::string& cn, const Cert* issuer = nullptr, bool ca = false,
                      long serial = 0) {
    static long next_serial = 1000;
    EVP_PKEY* key = EVP_PKEY_Q_keygen(nullptr, nullptr, "EC", "P-256");
    if (!key) throw std::runtime_error("keygen failed");
    X509* x = X509_new();
    X509_set_version(x, 2);
    ASN1_INTEGER_set(X509_get_serialNumber(x), serial ? serial : next_serial++);
    X509_gmtime_adj(X509_getm_notBefore(x), -3600);
    X509_gmtime_adj(X509_getm_notAfter(x), 3600L * 24 * 365);
    X509_set_pubkey(x, key);
    X509_NAME* name = X509_get_subject_name(x);
    X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                               reinterpret_cast<const unsigned char*>(cn.c_str()), -1, -1, 0);
    X509* issuer_x = issuer ? detail::x509_of(issuer->cert_pem) : nullptr;
    EVP_PKEY* issuer_key = issuer ? detail::key_of(issuer->key_pem) : nullptr;
    X509_set_issuer_name(x, issuer_x ? X509_get_subject_name(issuer_x) : name);
    detail::add_ext(x, issuer_x ? issuer_x : x, NID_basic_constraints,
                    ca ? "critical,CA:TRUE" : "critical,CA:FALSE");
    if (!ca) {
        std::string san = "DNS:localhost,IP:127.0.0.1,DNS:" + cn;
        detail::add_ext(x, issuer_x ? issuer_x : x, NID_subject_alt_name, san.c_str());
    }
    if (X509_sign(x, issuer_key ? issuer_key : key, EVP_sha256()) == 0)
        throw std::runtime_error("sign failed");
    Cert out{detail::pem_of(x), detail::pem_of(key), cn};
    X509_free(x);
    EVP_PKEY_free(key);
    if (issuer_x) X509_free(issuer_x);
    if (issuer_key) EVP_PKEY_free(issuer_key);
    return out;
}

inline void write_file(const std::string& path, const std::string& content) {
    FILE* f = fopen(path.c_str(), "w");
    if (!f) throw std::runtime_error("cannot write " + path);
    fputs(content.c_str(), f);
    fclose(f);
}

}  // namespace tls_test
