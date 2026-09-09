// backlog-sequence ⑥: mTLS client certificate -> credential / tenant identity
// mapping (docs/tls.md §2.1, docs/multi-tenancy.md §4.2). Full-dispatch tests on
// the memory backend; the certificate is injected on HttpRequest::tls_identity
// exactly as the drivers set it (test_tls.cc proves the driver side)
#include <nlohmann/json.hpp>

#include "core/config.h"
#include "core/util/crypto.h"
#include "http/tls.h"
#include "s3/auth/credential_store.h"
#include "s3/quota.h"
#include "s3/service.h"
#include "s3/tenant.h"
#include "s3/tls_identity_store.h"
#include "s3/usage.h"
#include "s3/website_store.h"
#include "storage/memory/memory_backend.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::s3;
using nlohmann::json;

namespace {

constexpr const char* kRootAk = "ROOTKEYTLSID";
constexpr const char* kRootSk = "root-secret-key";

struct Cert {
    std::string cn;
    std::string uri;
};

struct Env {
    std::shared_ptr<storage::MemoryBackend> backend = std::make_shared<storage::MemoryBackend>();
    AuthConfig acfg;
    std::shared_ptr<CredentialStore> cred_store;
    std::shared_ptr<TlsIdentityStore> tls_store;
    std::shared_ptr<TenantStore> tenant_store;
    std::shared_ptr<OwnerStore> owner_store;
    std::shared_ptr<TenantRegistry> tenants;
    std::shared_ptr<UsageTracker> usage;
    std::shared_ptr<QuotaStore> quota;
    std::unique_ptr<S3Service> svc;
    SigV4Authenticator signer;
    Credential root{kRootAk, util::SecretString(std::string(kRootSk))};

    static AuthConfig root_cfg() {
        AuthConfig cfg;
        cfg.credentials = {{kRootAk, kRootSk}};
        return cfg;
    }
    explicit Env(S3Service::TlsIdentityMode mode = S3Service::TlsIdentityMode::SubjectCn,
                 std::shared_ptr<storage::MemoryBackend> shared = nullptr)
        : acfg(root_cfg()), signer(SigV4Authenticator::build(root_cfg())) {
        if (shared) backend = shared;
        std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends;
        backends["mem"] = backend;
        BucketsConfig bcfg;
        bcfg.default_backend = "mem";
        cred_store = sync_wait(CredentialStore::load(backend, acfg));
        auto auth = SigV4Authenticator::build(acfg);
        auth.set_provider(cred_store);
        tls_store = sync_wait(TlsIdentityStore::load(backend));
        tenant_store = sync_wait(TenantStore::load(backend));
        owner_store = sync_wait(OwnerStore::load(backend));
        tenants = std::make_shared<TenantRegistry>(tenant_store, owner_store);
        usage = sync_wait(UsageTracker::load(storage::BucketRouter::build(bcfg, backends), {}, nullptr));
        quota = sync_wait(QuotaStore::load(backend));
        svc = std::make_unique<S3Service>(storage::BucketRouter::build(bcfg, backends), std::move(auth));
        svc->set_usage_tracker(usage);
        svc->set_quota_store(quota);
        svc->set_credential_store(cred_store);
        svc->set_tls_identity_store(tls_store);
        svc->set_tls_identity_mode(mode);
        svc->set_tenant_registry(tenants);
    }

    // cred = nullptr -> unsigned; cert = nullopt -> no client certificate
    http::HttpResponse call(std::string method, std::string path, const Credential* cred, std::optional<Cert> cert,
                            std::string body = "", std::vector<std::pair<std::string, std::string>> query = {}) {
        http::HttpRequest req;
        req.method = std::move(method);
        req.raw_path = path;
        req.path = std::move(path);
        req.query = query;
        for (auto& [k, v] : query) {
            if (!req.raw_query.empty()) req.raw_query += "&";
            req.raw_query += k + (v.empty() ? "" : "=" + v);
        }
        req.headers.add("Host", "localhost");
        req.headers.add("Content-Length", std::to_string(body.size()));
        std::string hash = util::sha256_hex(body);
        if (!body.empty()) req.body = std::make_unique<http::StringBodyReader>(std::move(body));
        if (cert) req.tls_identity = http::TlsIdentity{cert->cn, cert->uri};
        if (cred) signer.sign(req, *cred, hash);
        return sync_wait(svc->dispatch(std::move(req)));
    }
    json admin(const Credential* cred, std::optional<Cert> cert, std::string method, std::string path, json body = {},
               int expect = 200) {
        auto r = call(std::move(method), path, cred, std::move(cert), body.is_null() ? "" : body.dump());
        if (r.status != expect)
            throw mini_test::Failure("admin " + path + " -> HTTP " + std::to_string(r.status) + " " + r.small_body);
        if (r.small_body.empty()) return json::object();
        return json::parse(r.small_body);
    }
    Credential mint(json body) {
        auto j = admin(&root, std::nullopt, "POST", "/-/admin/credentials", std::move(body), 201);
        return Credential{j["access_key"].get<std::string>(), util::SecretString(j["secret_key"].get<std::string>())};
    }
    void bind(const std::string& subject, const std::string& ak, int expect = 201) {
        admin(&root, std::nullopt, "PUT", "/-/admin/tls-identities/" + subject, {{"access_key", ak}}, expect);
    }
};

bool contains(const std::string& s, const std::string& sub) { return s.find(sub) != std::string::npos; }

Cert cn(std::string c) { return Cert{std::move(c), ""}; }

}  // namespace

TEST(tls_identity_cn_of_dn_and_subject_rules) {
    CHECK_EQ(http::tls::cn_of_dn("CN=alice"), std::string("alice"));
    CHECK_EQ(http::tls::cn_of_dn("O=Example,CN=alice,C=US"), std::string("alice"));
    CHECK_EQ(http::tls::cn_of_dn("cn = spaced , O=x"), std::string("spaced"));
    CHECK_EQ(http::tls::cn_of_dn("CN=a\\,b,O=x"), std::string("a,b"));
    CHECK_EQ(http::tls::cn_of_dn("CN=\"quoted, name\",O=x"), std::string("quoted, name"));
    CHECK_EQ(http::tls::cn_of_dn("CN=hex\\2Cx"), std::string("hex,x"));
    CHECK_EQ(http::tls::cn_of_dn("O=Example,C=US"), std::string(""));
    CHECK_EQ(http::tls::cn_of_dn(""), std::string(""));

    CHECK(valid_tls_subject("alice"));
    CHECK(valid_tls_subject("spiffe://example.org/ns/prod/sa/api"));
    CHECK(valid_tls_subject("CN with spaces"));
    CHECK(!valid_tls_subject(""));
    CHECK(!valid_tls_subject(" lead"));
    CHECK(!valid_tls_subject("trail "));
    CHECK(!valid_tls_subject(std::string("ctl\x01")));
    CHECK(!valid_tls_subject(std::string(kMaxTlsSubjectLen + 1, 'a')));

    // Object keys: every subject becomes one opaque path segment, round-trips
    for (auto s : {"alice", "spiffe://a/b c=d?e&f", "CN=x, O=y/z", "百分号%41"}) {
        auto enc = TlsIdentityTraits::encode_key(s);
        CHECK(enc.find('/') == std::string::npos);
        CHECK_EQ(TlsIdentityTraits::decode_key(enc), std::string(s));
    }
}

// Unsigned request + bound certificate runs as the bound credential (policy applied);
// unbound / no certificate / mode off are refused as before
TEST(tls_identity_unsigned_request_runs_as_bound_credential) {
    Env env;
    CHECK_EQ(env.call("PUT", "/shared", &env.root, std::nullopt).status, 200);
    CHECK_EQ(env.call("PUT", "/shared/k", &env.root, std::nullopt, "v1").status, 200);
    CHECK_EQ(env.call("PUT", "/private", &env.root, std::nullopt).status, 200);
    auto reader = env.mint({{"comment", "alice"}, {"policy", {{"buckets", {"shared"}}, {"readonly", true}}}});
    env.bind("alice", reader.access_key);

    auto get = env.call("GET", "/shared/k", nullptr, cn("alice"));
    CHECK_EQ(get.status, 200);
    CHECK_EQ(get.small_body.empty() ? std::string("stream") : get.small_body, std::string("stream"));
    // The bound credential's policy: readonly, bucket allowlist
    CHECK_EQ(env.call("PUT", "/shared/k2", nullptr, cn("alice"), "x").status, 403);
    CHECK_EQ(env.call("GET", "/private", nullptr, cn("alice")).status, 403);
    // Runs *as* the credential: the admin plane is closed to it like to any non-root
    CHECK_EQ(env.call("GET", "/-/admin/credentials", nullptr, cn("alice")).status, 403);

    // Unbound certificate: refused with a hint; no certificate: the classic AccessDenied
    auto unbound = env.call("GET", "/shared/k", nullptr, cn("bob"));
    CHECK_EQ(unbound.status, 403);
    CHECK(contains(unbound.small_body, "not bound"));
    CHECK_EQ(env.call("GET", "/shared/k", nullptr, std::nullopt).status, 403);
    // A certificate without the selected field is no identity at all
    CHECK_EQ(env.call("GET", "/shared/k", nullptr, Cert{"", "spiffe://x"}).status, 403);

    // Mode off: the binding table is inert
    env.svc->set_tls_identity_mode(S3Service::TlsIdentityMode::Off);
    CHECK_EQ(env.call("GET", "/shared/k", nullptr, cn("alice")).status, 403);
    env.svc->set_tls_identity_mode(S3Service::TlsIdentityMode::SubjectCn);

    // Bound credential revoked: the binding dangles and is refused
    env.admin(&env.root, std::nullopt, "DELETE", "/-/admin/credentials/" + reader.access_key, {}, 204);
    auto gone = env.call("GET", "/shared/k", nullptr, cn("alice"));
    CHECK_EQ(gone.status, 403);
    CHECK(contains(gone.small_body, "no longer exists"));
}

// Signed request: the certificate's credential and the signing credential must be
// of one tenant; root is exempt; legacy (tenant-less) credentials form one tenant
TEST(tls_identity_signed_request_tenant_consistency) {
    Env env;
    env.admin(&env.root, std::nullopt, "POST", "/-/admin/tenants", {{"id", "acme"}}, 201);
    env.admin(&env.root, std::nullopt, "POST", "/-/admin/tenants", {{"id", "beta"}}, 201);
    auto a1 = env.mint({{"tenant", "acme"}});
    auto a2 = env.mint({{"tenant", "acme"}});
    auto b1 = env.mint({{"tenant", "beta"}});
    auto l1 = env.mint({{"comment", "legacy-1"}});
    auto l2 = env.mint({{"comment", "legacy-2"}});
    env.bind("alice", a1.access_key);
    env.bind("alan", a2.access_key);
    env.bind("bob", b1.access_key);
    env.bind("lee", l1.access_key);
    CHECK_EQ(env.call("PUT", "/acme-b", &env.root, std::nullopt).status, 200);
    env.admin(&env.root, std::nullopt, "PUT", "/-/admin/tenants/acme/buckets/acme-b");

    // Own certificate / a colleague's certificate: same tenant, admitted
    CHECK_EQ(env.call("PUT", "/acme-b/k", &a1, cn("alice"), "v").status, 200);
    CHECK_EQ(env.call("GET", "/acme-b/k", &a1, cn("alan")).status, 200);
    // Another tenant's certificate: refused even though the signature is valid
    auto cross = env.call("GET", "/acme-b/k", &a1, cn("bob"));
    CHECK_EQ(cross.status, 403);
    CHECK(contains(cross.small_body, "different tenant"));
    // Unbound certificate on a signed request: refused (the mode means "every
    // certificate is accounted for")
    CHECK_EQ(env.call("GET", "/acme-b/k", &a1, cn("carol")).status, 403);
    // No certificate at all (client auth optional): plain signature semantics
    CHECK_EQ(env.call("GET", "/acme-b/k", &a1, std::nullopt).status, 200);
    // Root signs: any certificate, bound or not
    CHECK_EQ(env.call("GET", "/acme-b/k", &env.root, cn("bob")).status, 200);
    CHECK_EQ(env.call("GET", "/acme-b/k", &env.root, cn("carol")).status, 200);
    // Legacy credentials: "" == "" -- one tenant among themselves, not with acme
    CHECK_EQ(env.call("PUT", "/legacy", &l2, cn("lee")).status, 200);
    CHECK_EQ(env.call("GET", "/legacy", &a1, cn("lee")).status, 403);
    CHECK_EQ(env.call("GET", "/acme-b/k", &l2, cn("alice")).status, 403);
    // The unsigned path inherits the tenant: alice's certificate alone reaches acme's
    // bucket, bob's does not
    CHECK_EQ(env.call("GET", "/acme-b/k", nullptr, cn("alice")).status, 200);
    CHECK_EQ(env.call("GET", "/acme-b/k", nullptr, cn("bob")).status, 403);
}

// san-uri mode selects the URI SAN and ignores the CN
TEST(tls_identity_san_uri_mode) {
    Env env(S3Service::TlsIdentityMode::SanUri);
    CHECK_EQ(env.call("PUT", "/bkt", &env.root, std::nullopt).status, 200);
    auto c = env.mint({{"comment", "svc"}});
    const std::string uri = "spiffe://example.org/ns/prod/sa/api";
    env.bind(uri, c.access_key);
    // Listed back with the subject intact (the object key is percent-encoded)
    auto list = env.admin(&env.root, std::nullopt, "GET", "/-/admin/tls-identities");
    CHECK_EQ(list["mode"].get<std::string>(), std::string("san-uri"));
    CHECK_EQ(list["identities"].size(), size_t(1));
    CHECK_EQ(list["identities"][0]["subject"].get<std::string>(), uri);
    CHECK_EQ(
        env.admin(&env.root, std::nullopt, "GET", "/-/admin/tls-identities/" + uri)["access_key"].get<std::string>(),
        c.access_key);

    CHECK_EQ(env.call("GET", "/bkt", nullptr, Cert{"anything", uri}).status, 200);
    CHECK_EQ(env.call("GET", "/bkt", nullptr, Cert{uri, ""}).status, 403);  // CN is not the subject here
    CHECK_EQ(env.call("GET", "/bkt", nullptr, Cert{"", "spiffe://example.org/other"}).status, 403);
}

// Admin API: root only, validation, persistence and cross-instance sync
TEST(tls_identity_admin_api_and_persistence) {
    Env env;
    auto c = env.mint({{"comment", "c"}});
    auto other = env.mint({{"comment", "other"}});
    constexpr const char* kBase = "/-/admin/tls-identities";

    // Only root manages bindings
    CHECK_EQ(env.call("GET", kBase, &c, std::nullopt).status, 403);
    CHECK_EQ(
        env.call("PUT", std::string(kBase) + "/x", &c, std::nullopt, json{{"access_key", c.access_key}}.dump()).status,
        403);
    // Validation
    CHECK_EQ(env.admin(&env.root, std::nullopt, "PUT", std::string(kBase) + "/x", {}, 400)["code"], "InvalidRequest");
    CHECK_EQ(env.admin(&env.root, std::nullopt, "PUT", std::string(kBase) + "/x", {{"access_key", "L3AKDOESNOTEXIST"}},
                       403)["code"],
             "InvalidAccessKeyId");
    CHECK_EQ(env.admin(&env.root, std::nullopt, "PUT", std::string(kBase) + "/x",
                       {{"access_key", "L3SASESSIONSESSION"}}, 400)["code"],
             "InvalidRequest");
    CHECK_EQ(env.admin(&env.root, std::nullopt, "PUT", std::string(kBase) + "/x",
                       {{"access_key", c.access_key}, {"bogus", 1}}, 400)["code"],
             "InvalidRequest");
    CHECK_EQ(env.admin(&env.root, std::nullopt, "PUT", std::string(kBase) + "/ bad", {{"access_key", c.access_key}},
                       400)["code"],
             "InvalidRequest");
    CHECK_EQ(env.admin(&env.root, std::nullopt, "PUT", kBase, {{"access_key", c.access_key}}, 400)["code"],
             "InvalidRequest");
    CHECK_EQ(env.admin(&env.root, std::nullopt, "GET", std::string(kBase) + "/nobody", {}, 404)["code"], "NoSuchKey");
    CHECK_EQ(env.admin(&env.root, std::nullopt, "POST", std::string(kBase) + "/x", {}, 405)["code"],
             "MethodNotAllowed");

    // Bind (201), rebind (200) replaces, ?subject= form, DELETE idempotent
    auto made = env.admin(&env.root, std::nullopt, "PUT", std::string(kBase) + "/x",
                          {{"access_key", c.access_key}, {"comment", "first"}}, 201);
    CHECK_EQ(made["subject"].get<std::string>(), "x");
    CHECK_EQ(made["created_by"].get<std::string>(), std::string(kRootAk));
    auto re = env.admin(&env.root, std::nullopt, "PUT", std::string(kBase) + "/x", {{"access_key", other.access_key}},
                        200);
    CHECK_EQ(re["access_key"].get<std::string>(), other.access_key);
    CHECK(!re.contains("comment"));
    auto q = env.call("PUT", kBase, &env.root, std::nullopt, json{{"access_key", c.access_key}}.dump(),
                      {{"subject", "CN=y, O=z"}});
    CHECK_EQ(q.status, 201);
    CHECK_EQ(env.admin(&env.root, std::nullopt, "GET", kBase)["identities"].size(), size_t(2));

    // Persistence: a fresh store on the same backend sees both, subjects decoded
    auto reloaded = sync_wait(TlsIdentityStore::load(env.backend));
    CHECK(TlsIdentityStore::find(reloaded->snapshot(), "x") != nullptr);
    auto* y = TlsIdentityStore::find(reloaded->snapshot(), "CN=y, O=z");
    CHECK(y != nullptr);
    CHECK_EQ(y->access_key, c.access_key);
    // Sync: a second store picks up a later binding and a removal
    env.bind("z", c.access_key);
    sync_wait(reloaded->sync_now());
    CHECK(TlsIdentityStore::find(reloaded->snapshot(), "z") != nullptr);
    CHECK_EQ(env.call("DELETE", std::string(kBase) + "/z", &env.root, std::nullopt).status, 204);
    CHECK_EQ(env.call("DELETE", std::string(kBase) + "/z", &env.root, std::nullopt).status, 204);
    sync_wait(reloaded->sync_now());
    CHECK(TlsIdentityStore::find(reloaded->snapshot(), "z") == nullptr);
    // A second gateway on the same backend enforces bindings made by the first
    Env peer(S3Service::TlsIdentityMode::SubjectCn, env.backend);
    CHECK_EQ(peer.call("PUT", "/pbkt", &peer.root, std::nullopt).status, 200);
    CHECK_EQ(peer.call("GET", "/pbkt", nullptr, cn("x")).status, 200);
    CHECK_EQ(peer.call("GET", "/pbkt", nullptr, cn("z")).status, 403);
}

// The root-gated metrics endpoint and the admin plane accept a certificate bound
// to root in place of a signature; an anonymous website read yields to a binding
TEST(tls_identity_root_binding_and_website_precedence) {
    Env env;
    env.bind("ops", kRootAk);
    env.svc->set_metrics_root_only(true);
    CHECK_EQ(env.call("GET", "/-/metrics", nullptr, cn("ops")).status, 200);
    CHECK_EQ(env.call("GET", "/-/metrics", nullptr, cn("nobody")).status, 403);
    CHECK_EQ(env.call("GET", "/-/admin/credentials", nullptr, cn("ops")).status, 200);
    CHECK_EQ(env.call("GET", "/-/admin/tls-identities", nullptr, cn("ops")).status, 200);

    // Website bucket: anonymous GET is 200, but a bound certificate whose credential
    // may not read the bucket is judged as that credential (403), never as anonymous
    auto wstore = sync_wait(WebsiteStore::load(env.backend, {WebsiteBucket{"site", "index.html", ""}}));
    env.svc->set_website_store(wstore);
    CHECK_EQ(env.call("PUT", "/site", &env.root, std::nullopt).status, 200);
    CHECK_EQ(env.call("PUT", "/site/index.html", &env.root, std::nullopt, "<p>hi</p>").status, 200);
    auto limited = env.mint({{"policy", {{"buckets", {"elsewhere"}}}}});
    env.bind("limited", limited.access_key);
    CHECK_EQ(env.call("GET", "/site/index.html", nullptr, std::nullopt).status, 200);
    CHECK_EQ(env.call("GET", "/site/index.html", nullptr, cn("limited")).status, 403);
    // Unbound certificate keeps the anonymous path (the site stays public)
    CHECK_EQ(env.call("GET", "/site/index.html", nullptr, cn("stranger")).status, 200);
}
