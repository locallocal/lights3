// L1: server-side TLS shared by the OpenSSL-backed drivers (builtin / beast /
// httplib; roadmap §4.1, docs/tls.md). One mechanism covers three roadmap items:
//   - certificate material (default cert + SNI extras + client CA) is an
//     immutable snapshot (Material) selected per handshake by an OpenSSL
//     certificate callback — so a new snapshot swapped into the Holder takes
//     effect on the next handshake without touching the SSL_CTX (hot reload),
//     and the same callback answers SNI with the matching extra certificate;
//   - the static knobs (minimum version, cipher list / TLS 1.3 suites, client
//     authentication mode) are applied once to each driver's SSL_CTX by
//     configure(). Drivers keep owning their SSL_CTX (asio::ssl::context,
//     httplib::SSLServer) and only hand it here.
// The seastar driver (GnuTLS through seastar::tls) implements the same config
// surface on its own; see seastar_server.cc and docs/tls.md §4.
#pragma once

#include <chrono>
#include <filesystem>
#include <memory>
#include <mutex>
#include <string>
#include <string_view>
#include <vector>

#include "core/config.h"
#include "core/timer.h"

typedef struct ssl_ctx_st SSL_CTX;
typedef struct ssl_st SSL;
typedef struct x509_st X509;
typedef struct evp_pkey_st EVP_PKEY;
typedef struct x509_store_st X509_STORE;
typedef struct stack_st_X509 STACK_OF_X509;
typedef struct stack_st_X509_NAME STACK_OF_X509_NAME;

namespace lights3::http::tls {

// One certificate + key + intermediate chain, and the SNI host patterns it
// answers for (empty = the default certificate)
struct CertBundle {
    X509* leaf = nullptr;
    STACK_OF_X509* chain = nullptr;  // intermediates in file order (may be empty)
    EVP_PKEY* key = nullptr;
    std::vector<std::string> hosts;  // lowercase; "*.example.com" matches one label
    std::string subject;             // for logs

    CertBundle() = default;
    CertBundle(const CertBundle&) = delete;
    CertBundle& operator=(const CertBundle&) = delete;
    CertBundle(CertBundle&& o) noexcept;
    ~CertBundle();
};

// Immutable certificate snapshot: everything the handshake needs that can change
// on disk. Loaded from the tls_* paths of an HttpConfig; throws
// std::runtime_error naming the file on any problem (startup fails loudly, a
// reload keeps the previous snapshot)
class Material {
public:
    static std::shared_ptr<const Material> load(const HttpConfig& cfg);
    ~Material();

    // SNI selection: exact host, then wildcard, else the default certificate
    const CertBundle& select(std::string_view servername) const;
    X509_STORE* client_store() const { return client_store_; }       // null = no client CA
    STACK_OF_X509_NAME* client_ca_names() const { return ca_names_; }  // null = no client CA

    // The files this snapshot came from with their (size, mtime) at load time;
    // reload_if_changed compares against the current filesystem state
    struct Stamp {
        std::string path;
        uintmax_t size = 0;
        std::filesystem::file_time_type mtime{};
    };
    const std::vector<Stamp>& stamps() const { return stamps_; }
    static Stamp stamp_of(const std::string& path);  // throws when the file cannot be stat'ed

private:
    Material() = default;
    std::vector<CertBundle> bundles_;  // [0] = default
    X509_STORE* client_store_ = nullptr;
    STACK_OF_X509_NAME* ca_names_ = nullptr;
    std::vector<Stamp> stamps_;
};

// Client authentication mode from http.tls_client_auth
enum class ClientAuth { Off, Optional, Require };
ClientAuth parse_client_auth(const std::string& s);  // throws std::runtime_error on unknown

// Per-server holder: current snapshot + reload watch + SSL_CTX configuration
class Holder {
public:
    // Loads the initial snapshot (throws like Material::load)
    explicit Holder(const HttpConfig& cfg);
    ~Holder();

    std::shared_ptr<const Material> current() const;

    // Applies the static knobs and installs the certificate callback. The
    // Holder must outlive every SSL created from ctx (drivers own both)
    void configure(SSL_CTX* ctx);

    // Re-reads the files when any stamp changed; true = a new snapshot is live.
    // A load failure logs a WARN and keeps the old snapshot
    bool reload_if_changed();
    // Force a reload regardless of stamps (tests / SIGHUP-style hooks)
    bool reload_now();

    // Periodic reload_if_changed on the timer thread (tls_reload_interval; 0 = off)
    void start_watch(int interval_sec);
    void stop_watch();

    const HttpConfig& config() const { return cfg_; }
    const char* summary() const;  // one-line description for the listen log

private:
    static int cert_cb(SSL* ssl, void* arg);
    void schedule();

    HttpConfig cfg_;
    mutable std::mutex mu_;
    std::shared_ptr<const Material> cur_;
    std::string summary_;
    int interval_sec_ = 0;
    TimerQueue::Id timer_ = 0;
    std::mutex timer_mu_;
    bool watching_ = false;
};

// Static knobs shared by the drivers (also used by tests to build client contexts)
long min_version_of(const std::string& s);  // "1.2" -> TLS1_2_VERSION, "1.3" -> TLS1_3_VERSION

}  // namespace lights3::http::tls
