#include "http/tls.h"

#include <openssl/crypto.h>
#include <openssl/err.h>
#include <openssl/pem.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

#include <algorithm>
#include <cctype>
#include <cstring>
#include <stdexcept>

#include "core/log.h"

namespace lights3::http::tls {

namespace {

std::string ssl_errors() {
    std::string out;
    unsigned long e;
    char buf[256];
    while ((e = ERR_get_error()) != 0) {
        ERR_error_string_n(e, buf, sizeof(buf));
        if (!out.empty()) out += "; ";
        out += buf;
    }
    return out.empty() ? "unknown OpenSSL error" : out;
}

struct BioDeleter {
    void operator()(BIO* b) const { BIO_free(b); }
};
using BioPtr = std::unique_ptr<BIO, BioDeleter>;

std::string lower(std::string s) {
    for (char& c : s)
        if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    return s;
}

// "a.example.com" matches itself; "*.example.com" matches exactly one extra
// label in front of ".example.com" (RFC 6125 §6.4.3 shape), never the bare domain
bool host_matches(const std::string& pattern, std::string_view host) {
    if (pattern.rfind("*.", 0) != 0) return pattern == host;
    // ".example.com"
    std::string_view suffix = std::string_view(pattern).substr(1);
    if (host.size() <= suffix.size() || !host.ends_with(suffix)) return false;
    std::string_view label = host.substr(0, host.size() - suffix.size());
    return !label.empty() && label.find('.') == std::string_view::npos;
}

std::string subject_of(X509* x) {
    char buf[256] = {0};
    X509_NAME_oneline(X509_get_subject_name(x), buf, sizeof(buf) - 1);
    return buf;
}

// PEM file with the leaf first followed by intermediates (the usual "fullchain" shape)
void load_cert_file(const std::string& path, CertBundle& out) {
    BioPtr bio(BIO_new_file(path.c_str(), "r"));
    if (!bio) throw std::runtime_error("cannot open certificate file " + path);
    X509* leaf = PEM_read_bio_X509_AUX(bio.get(), nullptr, nullptr, nullptr);
    if (!leaf) throw std::runtime_error("no certificate in " + path + ": " + ssl_errors());
    out.leaf = leaf;
    out.chain = sk_X509_new_null();
    for (;;) {
        X509* extra = PEM_read_bio_X509(bio.get(), nullptr, nullptr, nullptr);
        if (!extra) {
            // EOF, not an error
            ERR_clear_error();
            break;
        }
        sk_X509_push(out.chain, extra);
    }
    out.subject = subject_of(leaf);
}

void load_key_file(const std::string& path, CertBundle& out) {
    BioPtr bio(BIO_new_file(path.c_str(), "r"));
    if (!bio) throw std::runtime_error("cannot open private key file " + path);
    out.key = PEM_read_bio_PrivateKey(bio.get(), nullptr, nullptr, nullptr);
    if (!out.key) throw std::runtime_error("no private key in " + path + ": " + ssl_errors());
    if (X509_check_private_key(out.leaf, out.key) != 1)
        throw std::runtime_error("private key " + path + " does not match the certificate");
}

}  // namespace

// ---------- CertBundle ----------

CertBundle::CertBundle(CertBundle&& o) noexcept
    : leaf(o.leaf), chain(o.chain), key(o.key), hosts(std::move(o.hosts)), subject(std::move(o.subject)) {
    o.leaf = nullptr;
    o.chain = nullptr;
    o.key = nullptr;
}

CertBundle::~CertBundle() {
    if (leaf) X509_free(leaf);
    if (chain) sk_X509_pop_free(chain, X509_free);
    if (key) EVP_PKEY_free(key);
}

// ---------- Material ----------

Material::Stamp Material::stamp_of(const std::string& path) {
    std::error_code ec;
    Stamp s;
    s.path = path;
    s.size = std::filesystem::file_size(path, ec);
    if (ec) throw std::runtime_error("cannot stat " + path + ": " + ec.message());
    s.mtime = std::filesystem::last_write_time(path, ec);
    if (ec) throw std::runtime_error("cannot stat " + path + ": " + ec.message());
    return s;
}

std::shared_ptr<const Material> Material::load(const HttpConfig& cfg) {
    auto m = std::shared_ptr<Material>(new Material());
    auto load_bundle = [&](const std::string& cert, const std::string& key, const std::string& hosts) {
        CertBundle b;
        // Stamps are taken before the parse: a file rewritten during the load is
        // seen as "changed" on the next poll and loaded again, never missed
        m->stamps_.push_back(stamp_of(cert));
        m->stamps_.push_back(stamp_of(key));
        load_cert_file(cert, b);
        load_key_file(key, b);
        size_t pos = 0;
        while (pos <= hosts.size()) {
            size_t comma = hosts.find(',', pos);
            if (comma == std::string::npos) comma = hosts.size();
            std::string h = hosts.substr(pos, comma - pos);
            h.erase(0, h.find_first_not_of(" \t"));
            if (auto t = h.find_last_not_of(" \t"); t != std::string::npos)
                h.erase(t + 1);
            else
                h.clear();
            if (!h.empty()) b.hosts.push_back(lower(h));
            pos = comma + 1;
        }
        m->bundles_.push_back(std::move(b));
    };
    load_bundle(cfg.tls_cert, cfg.tls_key, "");
    for (auto& e : cfg.tls_sni) load_bundle(e.cert, e.key, e.hosts);
    if (!cfg.tls_client_ca.empty()) {
        m->stamps_.push_back(stamp_of(cfg.tls_client_ca));
        m->client_store_ = X509_STORE_new();
        if (!m->client_store_ || X509_STORE_load_locations(m->client_store_, cfg.tls_client_ca.c_str(), nullptr) != 1)
            throw std::runtime_error("cannot load client CA bundle " + cfg.tls_client_ca + ": " + ssl_errors());
        m->ca_names_ = SSL_load_client_CA_file(cfg.tls_client_ca.c_str());
        if (!m->ca_names_) throw std::runtime_error("no CA certificates in " + cfg.tls_client_ca + ": " + ssl_errors());
    }
    return m;
}

Material::~Material() {
    if (client_store_) X509_STORE_free(client_store_);
    if (ca_names_) sk_X509_NAME_pop_free(ca_names_, X509_NAME_free);
}

const CertBundle& Material::select(std::string_view servername) const {
    if (!servername.empty() && bundles_.size() > 1) {
        std::string host = lower(std::string(servername));
        if (!host.empty() && host.back() == '.') host.pop_back();
        // Exact matches beat wildcards, in configuration order
        for (size_t i = 1; i < bundles_.size(); ++i)
            for (auto& h : bundles_[i].hosts)
                if (h == host) return bundles_[i];
        for (size_t i = 1; i < bundles_.size(); ++i)
            for (auto& h : bundles_[i].hosts)
                if (host_matches(h, host)) return bundles_[i];
    }
    return bundles_[0];
}

// ---------- knobs ----------

ClientAuth parse_client_auth(const std::string& s) {
    if (s == "off" || s.empty()) return ClientAuth::Off;
    if (s == "optional") return ClientAuth::Optional;
    if (s == "require") return ClientAuth::Require;
    throw std::runtime_error("tls_client_auth must be off|optional|require, got '" + s + "'");
}

long min_version_of(const std::string& s) {
    if (s == "1.2" || s.empty()) return TLS1_2_VERSION;
    if (s == "1.3") return TLS1_3_VERSION;
    throw std::runtime_error("tls_min_version must be 1.2 or 1.3, got '" + s + "'");
}

// ---------- peer identity ----------

std::optional<TlsIdentity> peer_identity(const SSL* ssl) {
    if (!ssl) return std::nullopt;
    X509* x = SSL_get1_peer_certificate(ssl);
    if (!x) return std::nullopt;
    struct Free {
        X509* x;
        ~Free() { X509_free(x); }
    } guard{x};
    if (SSL_get_verify_result(ssl) != X509_V_OK) return std::nullopt;
    TlsIdentity id;
    // CN as UTF-8 regardless of the string type used in the certificate
    // (X509_NAME_get_text_by_NID would truncate / mis-decode BMPString)
    X509_NAME* name = X509_get_subject_name(x);
    int idx = X509_NAME_get_index_by_NID(name, NID_commonName, -1);
    if (idx >= 0) {
        ASN1_STRING* v = X509_NAME_ENTRY_get_data(X509_NAME_get_entry(name, idx));
        unsigned char* utf8 = nullptr;
        int n = ASN1_STRING_to_UTF8(&utf8, v);
        if (n > 0) id.subject_cn.assign(reinterpret_cast<char*>(utf8), static_cast<size_t>(n));
        if (utf8) OPENSSL_free(utf8);
    }
    auto* sans = static_cast<GENERAL_NAMES*>(X509_get_ext_d2i(x, NID_subject_alt_name, nullptr, nullptr));
    if (sans) {
        for (int i = 0; i < sk_GENERAL_NAME_num(sans) && id.san_uri.empty(); ++i) {
            GENERAL_NAME* gn = sk_GENERAL_NAME_value(sans, i);
            if (gn->type != GEN_URI) continue;
            const unsigned char* d = ASN1_STRING_get0_data(gn->d.uniformResourceIdentifier);
            int n = ASN1_STRING_length(gn->d.uniformResourceIdentifier);
            if (n > 0) id.san_uri.assign(reinterpret_cast<const char*>(d), static_cast<size_t>(n));
        }
        GENERAL_NAMES_free(sans);
    }
    // Embedded NULs would let two subjects print alike; treat such a certificate as identity-less
    if (id.subject_cn.find('\0') != std::string::npos || id.san_uri.find('\0') != std::string::npos)
        return std::nullopt;
    return id;
}

std::string cn_of_dn(std::string_view dn) {
    // Split on unescaped commas, then look for the CN attribute
    size_t start = 0;
    while (start <= dn.size()) {
        size_t end = start;
        bool quoted = false;
        for (; end < dn.size(); ++end) {
            char c = dn[end];
            if (c == '\\' && end + 1 < dn.size()) {
                ++end;
                continue;
            }
            if (c == '"')
                quoted = !quoted;
            else if (c == ',' && !quoted)
                break;
        }
        std::string_view rdn = dn.substr(start, end - start);
        while (!rdn.empty() && rdn.front() == ' ') rdn.remove_prefix(1);
        auto eq = rdn.find('=');
        if (eq != std::string_view::npos) {
            std::string_view type = rdn.substr(0, eq);
            while (!type.empty() && type.back() == ' ') type.remove_suffix(1);
            if (type.size() == 2 && (type[0] == 'C' || type[0] == 'c') && (type[1] == 'N' || type[1] == 'n')) {
                std::string_view raw = rdn.substr(eq + 1);
                while (!raw.empty() && raw.front() == ' ') raw.remove_prefix(1);
                while (!raw.empty() && raw.back() == ' ') raw.remove_suffix(1);
                if (raw.size() >= 2 && raw.front() == '"' && raw.back() == '"') raw = raw.substr(1, raw.size() - 2);
                std::string out;
                for (size_t i = 0; i < raw.size(); ++i) {
                    if (raw[i] == '\\' && i + 1 < raw.size()) {
                        ++i;
                        // \XX hex escapes (RFC 4514 §3): decode a single byte
                        if (i + 1 < raw.size() && std::isxdigit(static_cast<unsigned char>(raw[i])) &&
                            std::isxdigit(static_cast<unsigned char>(raw[i + 1]))) {
                            out.push_back(static_cast<char>(std::stoi(std::string(raw.substr(i, 2)), nullptr, 16)));
                            ++i;
                            continue;
                        }
                    }
                    out.push_back(raw[i]);
                }
                return out;
            }
        }
        start = end + 1;
    }
    return "";
}

// ---------- Holder ----------

Holder::Holder(const HttpConfig& cfg) : cfg_(cfg) {
    cur_ = Material::load(cfg);
    std::string s = "min " + (cfg.tls_min_version.empty() ? std::string("1.2") : cfg.tls_min_version);
    if (!cfg.tls_sni.empty()) s += ", " + std::to_string(cfg.tls_sni.size()) + " SNI cert(s)";
    if (!cfg.tls_client_ca.empty()) s += ", client auth " + cfg.tls_client_auth;
    if (cfg.tls_reload_interval_sec > 0) s += ", reload every " + std::to_string(cfg.tls_reload_interval_sec) + "s";
    summary_ = s;
}

Holder::~Holder() { stop_watch(); }

std::shared_ptr<const Material> Holder::current() const {
    std::lock_guard lk(mu_);
    return cur_;
}

const char* Holder::summary() const { return summary_.c_str(); }

void Holder::configure(SSL_CTX* ctx) {
    // Modern-only floor (the pre-existing behavior: no SSLv3/TLS1.0/1.1) plus the
    // configured minimum; renegotiation and compression stay off
    SSL_CTX_set_min_proto_version(ctx, static_cast<int>(min_version_of(cfg_.tls_min_version)));
    SSL_CTX_set_options(ctx, SSL_OP_NO_COMPRESSION | SSL_OP_NO_RENEGOTIATION | SSL_OP_CIPHER_SERVER_PREFERENCE);
    if (!cfg_.tls_ciphers.empty() && SSL_CTX_set_cipher_list(ctx, cfg_.tls_ciphers.c_str()) != 1)
        throw std::runtime_error("tls_ciphers: no cipher matched '" + cfg_.tls_ciphers + "'");
    if (!cfg_.tls_ciphersuites.empty() && SSL_CTX_set_ciphersuites(ctx, cfg_.tls_ciphersuites.c_str()) != 1)
        throw std::runtime_error("tls_ciphersuites: invalid '" + cfg_.tls_ciphersuites + "'");
    switch (parse_client_auth(cfg_.tls_client_auth)) {
        case ClientAuth::Off:
            SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, nullptr);
            break;
        case ClientAuth::Optional:
            SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, nullptr);
            break;
        case ClientAuth::Require:
            SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
            break;
    }
    // Certificates come from the snapshot at handshake time (SNI + hot reload in one place)
    SSL_CTX_set_cert_cb(ctx, &Holder::cert_cb, this);
}

int Holder::cert_cb(SSL* ssl, void* arg) {
    auto* self = static_cast<Holder*>(arg);
    auto m = self->current();
    const char* sni = SSL_get_servername(ssl, TLSEXT_NAMETYPE_host_name);
    const CertBundle& b = m->select(sni ? sni : "");
    // override=1: replaces whatever the SSL carries; all three objects are
    // reference-counted, so the snapshot may be swapped away afterwards
    if (SSL_use_cert_and_key(ssl, b.leaf, b.key, b.chain, 1) != 1) {
        LOG_WARN("tls: cannot install certificate {}: {}", b.subject, ssl_errors());
        return 0;
    }
    if (m->client_store()) {
        SSL_set1_verify_cert_store(ssl, m->client_store());
        SSL_set_client_CA_list(ssl, SSL_dup_CA_list(m->client_ca_names()));
    }
    return 1;
}

bool Holder::reload_now() {
    std::shared_ptr<const Material> fresh;
    try {
        fresh = Material::load(cfg_);
    } catch (const std::exception& e) {
        LOG_WARN("tls: certificate reload failed, keeping the previous material: {}", e.what());
        return false;
    }
    {
        std::lock_guard lk(mu_);
        cur_ = fresh;
    }
    LOG_INFO("tls: certificate material reloaded ({} bundle(s), default {})", fresh->stamps().size(),
             fresh->select("").subject);
    return true;
}

bool Holder::reload_if_changed() {
    auto m = current();
    bool changed = false;
    for (auto& s : m->stamps()) {
        try {
            auto now = Material::stamp_of(s.path);
            if (now.size != s.size || now.mtime != s.mtime) {
                changed = true;
                break;
            }
        } catch (const std::exception& e) {
            // A file vanishing mid-rotation: keep serving the loaded material, the
            // next poll sees the replacement
            LOG_WARN("tls: {}", e.what());
            return false;
        }
    }
    if (!changed) return false;
    return reload_now();
}

void Holder::start_watch(int interval_sec) {
    if (interval_sec <= 0) return;
    interval_sec_ = interval_sec;
    {
        std::lock_guard lk(timer_mu_);
        watching_ = true;
    }
    schedule();
}

void Holder::stop_watch() {
    TimerQueue::Id id = 0;
    {
        std::lock_guard lk(timer_mu_);
        watching_ = false;
        id = timer_;
        timer_ = 0;
    }
    // cancel outside the lock: TimerQueue::cancel blocks on an in-flight callback,
    // and the callback takes timer_mu_ to re-arm
    if (id) TimerQueue::instance().cancel(id);
}

void Holder::schedule() {
    std::lock_guard lk(timer_mu_);
    if (!watching_) return;
    timer_ = TimerQueue::instance().add(std::chrono::seconds(interval_sec_), [this] {
        // Three small files on the timer thread: a stat each, a parse only on change
        try {
            reload_if_changed();
        } catch (const std::exception& e) {
            LOG_WARN("tls: reload check failed: {}", e.what());
        }
        // The timer thread lives for the whole process: drop OpenSSL's per-thread
        // error state here, or it is reported as leaked at exit (recreated on demand)
        OPENSSL_thread_stop();
        schedule();
    });
}

}  // namespace lights3::http::tls
