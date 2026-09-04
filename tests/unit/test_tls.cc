// roadmap §4.1: TLS on every OpenSSL-backed driver (builtin/beast/httplib) —
// knobs (min version, cipher suites, client auth), SNI multi-certificate,
// certificate hot reload, config validation (docs/tls.md)
#include <arpa/inet.h>
#include <netinet/in.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <sys/socket.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <thread>

#include "core/config.h"
#include "http/server.h"
#include "http/tls.h"
#include "unit/mini_test.h"
#include "unit/tls_testcerts.h"

using namespace lights3;
using namespace lights3::http;

namespace {

Task<HttpResponse> echo_handler(HttpRequest req) {
    HttpResponse resp;
    resp.headers.set("Content-Type", "text/plain");
    std::string body = req.method + " " + req.path;
    if (req.body) {
        std::vector<std::byte> buf(4096);
        uint64_t total = 0;
        for (;;) {
            size_t n = co_await req.body->read(std::span(buf));
            if (n == 0) break;
            total += n;
        }
        body += " body=" + std::to_string(total);
    }
    resp.small_body = body;
    co_return resp;
}

// with_seastar: the seastar driver (GnuTLS/OpenSSL through seastar::tls) joins the
// cases whose knobs it implements — versions and client auth; SNI, cipher strings
// and the polling reload are OpenSSL-holder features (docs/tls.md §4)
std::vector<std::string> tls_drivers(bool with_seastar = false) {
    std::vector<std::string> out;
    for (auto& d : HttpServerFactory::drivers())
        if (d == "builtin" || d == "beast" || d == "httplib" || (with_seastar && d == "seastar"))
            out.push_back(d);
    return out;
}

struct Files {
    std::string dir;
    Files() {
        char tmpl[] = "/tmp/lights3-tls-test-XXXXXX";
        CHECK(mkdtemp(tmpl) != nullptr);
        dir = tmpl;
    }
    ~Files() { std::filesystem::remove_all(dir); }
    std::string put(const char* name, const std::string& content) {
        std::string p = dir + "/" + name;
        tls_test::write_file(p, content);
        return p;
    }
};

struct Server {
    std::unique_ptr<IHttpServer> srv;
    std::thread th;
    uint16_t port = 0;
    Server(const std::string& driver, const HttpConfig& cfg_in) {
        HttpConfig cfg = cfg_in;
        cfg.driver = driver;
        cfg.io_threads = 2;
        cfg.idle_timeout_sec = 5;
        srv = HttpServerFactory::create(driver, cfg);
        srv->set_handler([](HttpRequest req) { return echo_handler(std::move(req)); });
        srv->listen("127.0.0.1", 0);
        port = srv->bound_port();
        th = std::thread([this] { srv->run(); });
    }
    ~Server() {
        srv->shutdown();
        th.join();
    }
};

// OpenSSL client with the knobs the tests need; no server verification (the
// tests inspect the presented certificate instead)
struct ClientOpts {
    std::string sni;
    const tls_test::Cert* client_cert = nullptr;
    int max_version = 0;  // 0 = library default
    std::string ciphersuites;
};

struct Client {
    int fd = -1;
    SSL_CTX* ctx = nullptr;
    SSL* ssl = nullptr;
    bool handshake_ok = false;
    std::string error;

    Client(uint16_t port, const ClientOpts& o = {}) {
        fd = ::socket(AF_INET, SOCK_STREAM, 0);
        timeval tv{10, 0};
        setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
        setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
        sockaddr_in sa{};
        sa.sin_family = AF_INET;
        sa.sin_port = htons(port);
        inet_pton(AF_INET, "127.0.0.1", &sa.sin_addr);
        CHECK(::connect(fd, reinterpret_cast<sockaddr*>(&sa), sizeof(sa)) == 0);
        ctx = SSL_CTX_new(TLS_client_method());
        if (o.max_version) SSL_CTX_set_max_proto_version(ctx, o.max_version);
        if (!o.ciphersuites.empty()) CHECK(SSL_CTX_set_ciphersuites(ctx, o.ciphersuites.c_str()) == 1);
        if (o.client_cert) {
            BIO* b = BIO_new_mem_buf(o.client_cert->cert_pem.data(),
                                     static_cast<int>(o.client_cert->cert_pem.size()));
            X509* x = PEM_read_bio_X509(b, nullptr, nullptr, nullptr);
            BIO_free(b);
            CHECK(SSL_CTX_use_certificate(ctx, x) == 1);
            X509_free(x);
            BIO* kb = BIO_new_mem_buf(o.client_cert->key_pem.data(),
                                      static_cast<int>(o.client_cert->key_pem.size()));
            EVP_PKEY* k = PEM_read_bio_PrivateKey(kb, nullptr, nullptr, nullptr);
            BIO_free(kb);
            CHECK(SSL_CTX_use_PrivateKey(ctx, k) == 1);
            EVP_PKEY_free(k);
        }
        ssl = SSL_new(ctx);
        SSL_set_fd(ssl, fd);
        if (!o.sni.empty()) SSL_set_tlsext_host_name(ssl, o.sni.c_str());
        ERR_clear_error();
        handshake_ok = SSL_connect(ssl) == 1;
        if (!handshake_ok) {
            char buf[256] = "handshake failed";
            if (unsigned long e = ERR_peek_last_error()) ERR_error_string_n(e, buf, sizeof(buf));
            error = buf;
        }
    }
    ~Client() {
        if (ssl) {
            if (handshake_ok) SSL_shutdown(ssl);
            SSL_free(ssl);
        }
        if (ctx) SSL_CTX_free(ctx);
        if (fd >= 0) ::close(fd);
    }

    std::string peer_cn() {
        X509* x = SSL_get1_peer_certificate(ssl);
        if (!x) return "";
        char buf[128] = {0};
        X509_NAME_get_text_by_NID(X509_get_subject_name(x), NID_commonName, buf, sizeof(buf));
        X509_free(x);
        return buf;
    }
    // One request with Connection: close; empty result = the server refused/closed
    std::string request(const std::string& method, const std::string& path,
                        const std::string& body = "") {
        if (!handshake_ok) return "";
        std::string req = method + " " + path + " HTTP/1.1\r\nHost: t\r\nConnection: close\r\n" +
                          (body.empty() ? "" : "Content-Length: " + std::to_string(body.size()) + "\r\n") +
                          "\r\n" + body;
        if (SSL_write(ssl, req.data(), static_cast<int>(req.size())) <= 0) return "";
        std::string out;
        char buf[4096];
        for (;;) {
            int n = SSL_read(ssl, buf, sizeof(buf));
            if (n <= 0) break;
            out.append(buf, static_cast<size_t>(n));
        }
        return out;
    }
};

bool contains(const std::string& s, const std::string& sub) { return s.find(sub) != std::string::npos; }

struct Driver {
    std::string name;
    explicit Driver(std::string n) : name(std::move(n)) {}
    void fail(const mini_test::Failure& f) { throw mini_test::Failure("[driver=" + name + "] " + f.what()); }
};

}  // namespace

TEST(tls_every_openssl_driver_serves_https) {
    Files files;
    auto cert = tls_test::make_cert("default.test");
    HttpConfig cfg;
    cfg.tls_cert = files.put("cert.pem", cert.cert_pem);
    cfg.tls_key = files.put("key.pem", cert.key_pem);
    cfg.tls_reload_interval_sec = 0;
    for (auto& d : tls_drivers(/*with_seastar=*/true)) {
        try {
            Server s(d, cfg);
            Client c(s.port);
            CHECK(c.handshake_ok);
            CHECK_EQ(c.peer_cn(), std::string("default.test"));
            std::string r = c.request("PUT", "/x", std::string(3000, 'a'));
            CHECK(contains(r, "200"));
            CHECK(contains(r, "PUT /x body=3000"));
            // Below the floor (1.2 by default): 1.1 is refused
            Client old(s.port, {.max_version = TLS1_1_VERSION});
            CHECK(!old.handshake_ok);
        } catch (const mini_test::Failure& f) {
            Driver(d).fail(f);
        }
    }
}

TEST(tls_min_version_and_ciphersuites) {
    Files files;
    auto cert = tls_test::make_cert("v13.test");
    HttpConfig cfg;
    cfg.tls_cert = files.put("cert.pem", cert.cert_pem);
    cfg.tls_key = files.put("key.pem", cert.key_pem);
    cfg.tls_min_version = "1.3";
    cfg.tls_ciphersuites = "TLS_AES_256_GCM_SHA384";
    cfg.tls_reload_interval_sec = 0;
    for (auto& d : tls_drivers(/*with_seastar=*/true)) {
        try {
            Server s(d, cfg);
            Client ok(s.port);
            CHECK(ok.handshake_ok);
            CHECK_EQ(std::string(SSL_get_version(ok.ssl)), std::string("TLSv1.3"));
            Client v12(s.port, {.max_version = TLS1_2_VERSION});
            CHECK(!v12.handshake_ok);
            if (d == "seastar") continue;  // cipher strings only reach seastar's OpenSSL backend
            CHECK_EQ(std::string(SSL_get_cipher_name(ok.ssl)), std::string("TLS_AES_256_GCM_SHA384"));
            Client chacha(s.port, {.ciphersuites = "TLS_CHACHA20_POLY1305_SHA256"});
            CHECK(!chacha.handshake_ok);
        } catch (const mini_test::Failure& f) {
            Driver(d).fail(f);
        }
    }
}

TEST(tls_client_auth_require_and_optional) {
    Files files;
    auto ca = tls_test::make_cert("Test CA", nullptr, /*ca=*/true);
    auto other_ca = tls_test::make_cert("Other CA", nullptr, /*ca=*/true);
    auto server = tls_test::make_cert("mtls.test", &ca);
    auto good = tls_test::make_cert("client-good", &ca);
    auto bad = tls_test::make_cert("client-bad", &other_ca);
    HttpConfig cfg;
    cfg.tls_cert = files.put("cert.pem", server.cert_pem);
    cfg.tls_key = files.put("key.pem", server.key_pem);
    cfg.tls_client_ca = files.put("ca.pem", ca.cert_pem);
    cfg.tls_client_auth = "require";
    cfg.tls_reload_interval_sec = 0;
    for (auto& d : tls_drivers(/*with_seastar=*/true)) {
        try {
            {
                Server s(d, cfg);
                Client with(s.port, {.client_cert = &good});
                CHECK(with.handshake_ok);
                CHECK(contains(with.request("GET", "/ok"), "GET /ok"));
                // No certificate / a certificate from another CA: TLS 1.3 reports the
                // server's verdict on the first read, so judge by the request outcome
                Client none(s.port);
                CHECK(none.request("GET", "/x").empty());
                Client foreign(s.port, {.client_cert = &bad});
                CHECK(foreign.request("GET", "/x").empty());
            }
            {
                HttpConfig opt = cfg;
                opt.tls_client_auth = "optional";
                Server s(d, opt);
                Client none(s.port);
                CHECK(contains(none.request("GET", "/anon"), "GET /anon"));
                Client with(s.port, {.client_cert = &good});
                CHECK(contains(with.request("GET", "/id"), "GET /id"));
                Client foreign(s.port, {.client_cert = &bad});
                CHECK(foreign.request("GET", "/x").empty());  // presented but invalid: still refused
            }
        } catch (const mini_test::Failure& f) {
            Driver(d).fail(f);
        }
    }
}

TEST(tls_sni_selects_certificate) {
    Files files;
    auto def = tls_test::make_cert("default.test");
    auto alt = tls_test::make_cert("alt.example");
    auto wild = tls_test::make_cert("wild.example");
    HttpConfig cfg;
    cfg.tls_cert = files.put("cert.pem", def.cert_pem);
    cfg.tls_key = files.put("key.pem", def.key_pem);
    cfg.tls_sni.push_back({"alt.example, ALT2.example", files.put("alt.pem", alt.cert_pem),
                           files.put("alt.key", alt.key_pem)});
    cfg.tls_sni.push_back({"*.wild.example", files.put("wild.pem", wild.cert_pem),
                           files.put("wild.key", wild.key_pem)});
    cfg.tls_reload_interval_sec = 0;
    for (auto& d : tls_drivers()) {
        try {
            Server s(d, cfg);
            CHECK_EQ(Client(s.port).peer_cn(), std::string("default.test"));
            CHECK_EQ(Client(s.port, {.sni = "alt.example"}).peer_cn(), std::string("alt.example"));
            CHECK_EQ(Client(s.port, {.sni = "alt2.example"}).peer_cn(), std::string("alt.example"));
            CHECK_EQ(Client(s.port, {.sni = "a.wild.example"}).peer_cn(), std::string("wild.example"));
            CHECK_EQ(Client(s.port, {.sni = "wild.example"}).peer_cn(), std::string("default.test"));
            CHECK_EQ(Client(s.port, {.sni = "x.y.wild.example"}).peer_cn(), std::string("default.test"));
            CHECK_EQ(Client(s.port, {.sni = "unknown.test"}).peer_cn(), std::string("default.test"));
            Client c(s.port, {.sni = "alt.example"});
            CHECK(contains(c.request("GET", "/sni"), "GET /sni"));
        } catch (const mini_test::Failure& f) {
            Driver(d).fail(f);
        }
    }
}

TEST(tls_hot_reload_swaps_certificate) {
    Files files;
    auto first = tls_test::make_cert("first.test");
    auto second = tls_test::make_cert("second.test");
    HttpConfig cfg;
    cfg.tls_cert = files.put("cert.pem", first.cert_pem);
    cfg.tls_key = files.put("key.pem", first.key_pem);
    cfg.tls_reload_interval_sec = 1;
    for (auto& d : tls_drivers()) {
        try {
            files.put("cert.pem", first.cert_pem);
            files.put("key.pem", first.key_pem);
            Server s(d, cfg);
            CHECK_EQ(Client(s.port).peer_cn(), std::string("first.test"));
            // Rotate: key first, then certificate (a poll between the two sees a
            // mismatched pair, keeps the old material and retries next round)
            files.put("key.pem", second.key_pem);
            files.put("cert.pem", second.cert_pem);
            std::string cn;
            for (int i = 0; i < 80 && cn != "second.test"; ++i) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
                cn = Client(s.port).peer_cn();
            }
            CHECK_EQ(cn, std::string("second.test"));
            // A broken file never replaces working material
            files.put("cert.pem", "garbage");
            std::this_thread::sleep_for(std::chrono::milliseconds(1500));
            CHECK_EQ(Client(s.port).peer_cn(), std::string("second.test"));
        } catch (const mini_test::Failure& f) {
            Driver(d).fail(f);
        }
    }
}

TEST(tls_holder_reload_semantics) {
    Files files;
    auto a = tls_test::make_cert("a.test");
    auto b = tls_test::make_cert("b.test");
    HttpConfig cfg;
    cfg.tls_cert = files.put("cert.pem", a.cert_pem);
    cfg.tls_key = files.put("key.pem", a.key_pem);
    tls::Holder h(cfg);
    CHECK(contains(h.current()->select("").subject, "a.test"));
    CHECK(!h.reload_if_changed());  // nothing changed
    files.put("cert.pem", b.cert_pem);  // cert only: key mismatch -> refused, old kept
    CHECK(!h.reload_if_changed());
    CHECK(contains(h.current()->select("").subject, "a.test"));
    files.put("key.pem", b.key_pem);
    CHECK(h.reload_if_changed());
    CHECK(contains(h.current()->select("").subject, "b.test"));
    // Old snapshots stay valid for sessions still holding them
    auto old = h.current();
    files.put("cert.pem", a.cert_pem);
    files.put("key.pem", a.key_pem);
    CHECK(h.reload_now());
    CHECK(contains(old->select("").subject, "b.test"));
    CHECK(contains(h.current()->select("").subject, "a.test"));
    // Bad paths at construction throw with the file named
    HttpConfig bad = cfg;
    bad.tls_client_ca = files.dir + "/missing-ca.pem";
    bool threw = false;
    try {
        tls::Holder h2(bad);
    } catch (const std::runtime_error& e) {
        threw = contains(e.what(), "missing-ca.pem");
    }
    CHECK(threw);
}

TEST(tls_config_validation) {
    std::string base = "backends:\n  - name: m\n    type: memory\nhttp:\n  tls_cert: /c.pem\n  tls_key: /k.pem\n";
    auto cfg = Config::from_string(base + "  tls_client_ca: /ca.pem\n  tls_client_auth: require\n"
                                          "  tls_min_version: \"1.3\"\n  tls_ciphersuites: TLS_AES_128_GCM_SHA256\n"
                                          "  tls_reload_interval: 5m\n  tls_sni:\n    - hosts: a.example, *.b.example\n"
                                          "      cert: /a.pem\n      key: /a.key\n");
    CHECK_EQ(cfg.http.tls_client_auth, std::string("require"));
    CHECK_EQ(cfg.http.tls_min_version, std::string("1.3"));
    CHECK_EQ(cfg.http.tls_reload_interval_sec, 300);
    CHECK_EQ(cfg.http.tls_sni.size(), size_t(1));
    CHECK_EQ(cfg.http.tls_sni[0].hosts, std::string("a.example, *.b.example"));
    auto rejects = [&](const std::string& text) {
        try {
            Config::from_string(text);
        } catch (const std::runtime_error&) {
            return true;
        }
        return false;
    };
    CHECK(rejects(base + "  tls_client_auth: require\n"));                       // needs a CA
    CHECK(rejects(base + "  tls_client_auth: always\n"));                        // unknown mode
    CHECK(rejects(base + "  tls_min_version: \"1.1\"\n"));                        // below the floor
    CHECK(rejects(base + "  tls_sni:\n    - hosts: x\n      cert: /x.pem\n"));   // missing key
    CHECK(rejects("backends:\n  - name: m\n    type: memory\nhttp:\n  tls_client_ca: /ca.pem\n"));  // knob without listener
    CHECK(rejects(base + "  tls_reload_interval: 2d\n"));
    CHECK(!rejects(base + "  tls_reload_interval: 0s\n"));
}
