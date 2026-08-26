// docs/credential-management.md: CredentialStore persistence / two-tier permissions + the full /-/admin/credentials flow
// + phase two (§10): at-rest encryption / per-credential policy / file hot reload / multi-instance incremental sync
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>

#include "core/util/checksum.h"
#include "core/util/crypto.h"
#include "s3/auth/credential_store.h"
#include "s3/service.h"
#include "storage/memory/memory_backend.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::s3;
using nlohmann::json;

namespace {

constexpr const char* kRootAk = "ROOTKEYEXAMPLE";
constexpr const char* kRootSk = "root-secret-key";

AuthConfig root_cfg() {
    AuthConfig cfg;
    cfg.credentials = {{kRootAk, kRootSk}};
    return cfg;
}

std::shared_ptr<CredentialStore> load_store(std::shared_ptr<storage::IStorageBackend> be,
                                            const AuthConfig& cfg) {
    return sync_wait(CredentialStore::load(std::move(be), cfg));
}

// Fully assemble a service with credential management (the memory backend carries both the data plane and .sys)
struct SvcEnv {
    std::shared_ptr<storage::MemoryBackend> backend;
    std::shared_ptr<CredentialStore> store;
    std::unique_ptr<S3Service> svc;
    SigV4Authenticator signer;  // test-side signer (same region/service as the server)

    explicit SvcEnv(AuthConfig cfg = root_cfg())
        : backend(std::make_shared<storage::MemoryBackend>()),
          store(load_store(backend, cfg)),
          signer(SigV4Authenticator::build(cfg)) {
        std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends;
        backends["mem"] = backend;
        BucketsConfig bcfg;
        bcfg.default_backend = "mem";
        auto router = storage::BucketRouter::build(bcfg, std::move(backends));
        auto auth = SigV4Authenticator::build(cfg);
        auth.set_provider(store);
        svc = std::make_unique<S3Service>(std::move(router), std::move(auth));
        svc->set_credential_store(store);
    }

    http::HttpResponse call(std::string method, std::string path, const Credential& cred,
                            std::vector<std::pair<std::string, std::string>> query = {},
                            std::string body = "",
                            std::vector<std::pair<std::string, std::string>> headers = {}) {
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
        for (auto& [k, v] : headers) req.headers.add(k, v);
        std::string hash = util::sha256_hex(body);
        if (!body.empty()) req.body = std::make_unique<http::StringBodyReader>(std::move(body));
        signer.sign(req, cred, hash);
        return sync_wait(svc->dispatch(std::move(req)));
    }
};

json body_json(const http::HttpResponse& resp) { return json::parse(resp.small_body); }

bool in_charset(const std::string& s, const std::string& allowed) {
    return s.find_first_not_of(allowed) == std::string::npos;
}

}  // namespace

// ---------- CredentialStore ----------

TEST(credstore_generate_lookup_and_format) {
    auto be = std::make_shared<storage::MemoryBackend>();
    auto store = load_store(be, root_cfg());
    auto c = sync_wait(store->generate("ci"));

    CHECK_EQ(c.access_key.size(), size_t{20});
    CHECK_EQ(c.access_key.substr(0, 4), "L3AK");
    CHECK(in_charset(c.access_key, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"));
    CHECK_EQ(c.secret_key.size(), size_t{40});
    CHECK(store->secret_for(c.access_key).value_or("") == c.secret_key);
    CHECK(!store->is_root(c.access_key));
    CHECK(store->is_root(kRootAk));
}

TEST(credstore_reload_restores_generated) {
    auto be = std::make_shared<storage::MemoryBackend>();
    auto c = sync_wait(load_store(be, root_cfg())->generate("persist-me"));

    auto reloaded = load_store(be, root_cfg());  // simulates a process restart
    CHECK(reloaded->secret_for(c.access_key).value_or("") == c.secret_key);
    auto info = reloaded->find(c.access_key);
    CHECK(info && info->comment == "persist-me" && !info->is_static());
}

TEST(credstore_remove_semantics) {
    auto be = std::make_shared<storage::MemoryBackend>();
    auto store = load_store(be, root_cfg());
    auto c = sync_wait(store->generate(""));

    sync_wait(store->remove(c.access_key));
    CHECK(!store->secret_for(c.access_key));
    CHECK_THROWS_S3(sync_wait(store->remove(c.access_key)), S3ErrorCode::InvalidAccessKeyId);
    CHECK_THROWS_S3(sync_wait(store->remove(kRootAk)), S3ErrorCode::MethodNotAllowed);
    // A revoked credential does not come back after a restart either
    CHECK(!load_store(be, root_cfg())->secret_for(c.access_key));
}

TEST(credstore_static_wins_on_conflict) {
    auto be = std::make_shared<storage::MemoryBackend>();
    // A store without a static table generates a dynamic credential, then reloads with a "static table with the same AK"
    auto c = sync_wait(load_store(be, AuthConfig{})->generate(""));
    AuthConfig cfg;
    cfg.credentials = {{c.access_key, "static-overrides"}};
    auto store = load_store(be, cfg);
    CHECK(store->secret_for(c.access_key).value_or("") == "static-overrides");
    CHECK(store->is_root(c.access_key));
}

TEST(credstore_sigv4_roundtrip) {
    auto be = std::make_shared<storage::MemoryBackend>();
    auto store = load_store(be, root_cfg());
    auto c = sync_wait(store->generate(""));

    auto auth = SigV4Authenticator::build(root_cfg());
    auth.set_provider(store);
    http::HttpRequest req;
    req.method = "GET";
    req.raw_path = "/bkt/k";
    req.path = "/bkt/k";
    req.headers.add("Host", "localhost");
    auth.sign(req, {c.access_key, c.secret_key});
    CHECK_EQ(auth.verify(req).access_key, c.access_key);
}

// ---------- /-/admin/credentials ----------

TEST(admin_api_full_flow) {
    SvcEnv env;
    Credential root{kRootAk, kRootSk};

    // Generate
    auto created = env.call("POST", "/-/admin/credentials", root, {{"comment", "ci"}});
    CHECK_EQ(created.status, 201);
    auto j = body_json(created);
    Credential dyn{j.at("access_key").get<std::string>(),
                   j.at("secret_key").get<std::string>()};
    CHECK_EQ(j.at("source").get<std::string>(), "dynamic");
    CHECK_EQ(j.at("comment").get<std::string>(), "ci");

    // The new credential goes through the full data-plane flow
    CHECK_EQ(env.call("PUT", "/bkt", dyn).status, 200);
    CHECK_EQ(env.call("PUT", "/bkt/hello.txt", dyn, {}, "hi").status, 200);
    CHECK_EQ(env.call("GET", "/bkt/hello.txt", dyn).status, 200);

    // Listing: contains root and the dynamic credential, the SK is masked and not leaked
    auto listed = env.call("GET", "/-/admin/credentials", root);
    CHECK_EQ(listed.status, 200);
    auto lj = body_json(listed);
    CHECK_EQ(lj.at("credentials").size(), size_t{2});
    for (auto& e : lj.at("credentials")) {
        CHECK(!e.contains("secret_key"));
        CHECK(e.at("secret_key_masked").get<std::string>().find("****") != std::string::npos);
    }

    // Single lookup: masked by default, show-secret requests it explicitly
    auto masked = env.call("GET", "/-/admin/credentials/" + dyn.access_key, root);
    CHECK(!body_json(masked).contains("secret_key"));
    auto shown = env.call("GET", "/-/admin/credentials/" + dyn.access_key, root,
                          {{"show-secret", "true"}});
    CHECK_EQ(body_json(shown).at("secret_key").get<std::string>(), dyn.secret_key);

    // Revoke: 204 -> new data-plane requests get 403, admin single lookup gets 403
    CHECK_EQ(env.call("DELETE", "/-/admin/credentials/" + dyn.access_key, root).status, 204);
    CHECK_EQ(env.call("GET", "/bkt/hello.txt", dyn).status, 403);
    auto gone = env.call("GET", "/-/admin/credentials/" + dyn.access_key, root);
    CHECK_EQ(gone.status, 403);
    CHECK_EQ(body_json(gone).at("code").get<std::string>(), "InvalidAccessKeyId");
}

TEST(admin_api_requires_root) {
    SvcEnv env;
    Credential root{kRootAk, kRootSk};
    auto j = body_json(env.call("POST", "/-/admin/credentials", root));
    Credential dyn{j.at("access_key").get<std::string>(),
                   j.at("secret_key").get<std::string>()};

    // A dynamic credential cannot mint credentials (privilege-escalation chain)
    auto resp = env.call("POST", "/-/admin/credentials", dyn);
    CHECK_EQ(resp.status, 403);
    CHECK_EQ(body_json(resp).at("code").get<std::string>(), "AccessDenied");
    // The error body is JSON, not S3 XML
    CHECK_EQ(*resp.headers.get("Content-Type"), "application/json");
}

TEST(admin_api_denied_when_auth_disabled) {
    SvcEnv env{AuthConfig{}};  // no static credentials: auth is off, and without root there is no admin plane
    http::HttpRequest req;
    req.method = "POST";
    req.raw_path = "/-/admin/credentials";
    req.path = req.raw_path;
    req.headers.add("Host", "localhost");
    auto resp = sync_wait(env.svc->dispatch(std::move(req)));
    CHECK_EQ(resp.status, 403);
    CHECK_EQ(body_json(resp).at("code").get<std::string>(), "AccessDenied");
}

TEST(admin_api_method_not_allowed) {
    SvcEnv env;
    Credential root{kRootAk, kRootSk};
    CHECK_EQ(env.call("PUT", "/-/admin/credentials", root).status, 405);
    CHECK_EQ(env.call("DELETE", "/-/admin/credentials", root).status, 405);
}

// ---------- Phase two (docs/credential-management.md §10) ----------

namespace {

// 64 hex chars = 32-byte master key (fixed value for tests)
constexpr const char* kTestMasterKeyHex =
    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

struct EnvGuard {  // sets an environment variable for the test, cleared when the scope ends
    const char* name;
    EnvGuard(const char* n, const char* v) : name(n) { ::setenv(n, v, 1); }
    ~EnvGuard() { ::unsetenv(name); }
};

std::string read_object_raw(storage::IStorageBackend& be, const std::string& key) {
    auto read = [&]() -> Task<std::string> {
        auto stream = co_await be.get_object(kSysBucket, key, std::nullopt);
        std::string out;
        std::byte buf[4096];
        for (;;) {
            size_t n = co_await stream.body->read(std::span(buf));
            if (n == 0) break;
            out.append(reinterpret_cast<const char*>(buf), n);
        }
        co_return out;
    };
    return sync_wait(read());
}

bool load_throws(std::shared_ptr<storage::IStorageBackend> be, const AuthConfig& cfg) {
    try {
        load_store(std::move(be), cfg);
    } catch (const std::runtime_error&) {
        return true;
    }
    return false;
}

}  // namespace

TEST(aes256gcm_roundtrip_and_tamper) {
    util::Aes256Key key{};
    for (size_t i = 0; i < key.size(); ++i) key[i] = static_cast<uint8_t>(i);
    auto sealed = util::aes256gcm_seal(key, "hello secret");
    CHECK_EQ(sealed.size(), size_t{12 + 12 + 16});  // nonce + ct + tag
    CHECK_EQ(util::aes256gcm_open(key, sealed).value_or(""), "hello secret");

    auto tampered = sealed;
    tampered[15] = static_cast<char>(tampered[15] ^ 1);
    CHECK(!util::aes256gcm_open(key, tampered));
    util::Aes256Key wrong = key;
    wrong[0] ^= 1;
    CHECK(!util::aes256gcm_open(wrong, sealed));
    CHECK(!util::aes256gcm_open(key, "short"));
    // Random nonce: sealing the same plaintext twice yields different outputs
    CHECK(util::aes256gcm_seal(key, "x") != util::aes256gcm_seal(key, "x"));
}

TEST(credstore_encrypted_at_rest) {
    EnvGuard env(kMasterKeyEnv, kTestMasterKeyHex);
    auto be = std::make_shared<storage::MemoryBackend>();
    auto c = sync_wait(load_store(be, root_cfg())->generate("enc"));

    auto raw = read_object_raw(*be, "credentials/" + c.access_key);
    CHECK(raw.find("\"version\": 2") != std::string::npos);
    CHECK(raw.find("sk_enc") != std::string::npos);
    CHECK(raw.find(c.secret_key) == std::string::npos);  // the plaintext SK never hits disk

    auto reloaded = load_store(be, root_cfg());
    CHECK(reloaded->secret_for(c.access_key).value_or("") == c.secret_key);
}

TEST(credstore_v2_requires_correct_master_key) {
    auto be = std::make_shared<storage::MemoryBackend>();
    {
        EnvGuard env(kMasterKeyEnv, kTestMasterKeyHex);
        sync_wait(load_store(be, root_cfg())->generate("enc"));
    }
    // No key / wrong key / malformed key are all configuration errors: fail fast rather than silently losing credentials
    CHECK(load_throws(be, root_cfg()));
    {
        EnvGuard env(kMasterKeyEnv,
                     "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff");
        CHECK(load_throws(be, root_cfg()));
    }
    {
        EnvGuard env(kMasterKeyEnv, "not-hex");
        CHECK(load_throws(be, root_cfg()));
    }
}

TEST(credstore_v1_upgraded_to_v2_on_load) {
    auto be = std::make_shared<storage::MemoryBackend>();
    auto c = sync_wait(load_store(be, root_cfg())->generate("old"));  // no key: v1 plaintext
    CHECK(read_object_raw(*be, "credentials/" + c.access_key).find("\"sk\"") !=
          std::string::npos);

    EnvGuard env(kMasterKeyEnv, kTestMasterKeyHex);
    auto store = load_store(be, root_cfg());  // upgraded in place at load time
    CHECK(store->secret_for(c.access_key).value_or("") == c.secret_key);
    auto raw = read_object_raw(*be, "credentials/" + c.access_key);
    CHECK(raw.find("\"version\": 2") != std::string::npos);
    CHECK(raw.find(c.secret_key) == std::string::npos);
}

TEST(credstore_policy_enforced_and_persisted) {
    auto be = std::make_shared<storage::MemoryBackend>();
    auto store = load_store(be, root_cfg());
    CredentialPolicy p;
    p.buckets = {"logs-*"};
    p.readonly = true;
    auto c = sync_wait(store->generate("scoped", p));

    store->authorize(c.access_key, "logs-app", "", Action::Read);  // matches the glob
    store->authorize(c.access_key, "", "", Action::Read);            // ListBuckets allowed
    store->authorize(kRootAk, "anything", "", Action::Write);        // root is unrestricted
    CHECK_THROWS_S3(store->authorize(c.access_key, "logs-app", "", Action::Write),
                    S3ErrorCode::AccessDenied);  // readonly
    CHECK_THROWS_S3(store->authorize(c.access_key, "other", "", Action::Read),
                    S3ErrorCode::AccessDenied);  // outside the allowlist

    auto info = load_store(be, root_cfg())->find(c.access_key);  // persisted
    CHECK(info && info->policy && info->policy->readonly);
    CHECK(info->policy->buckets == std::vector<std::string>{"logs-*"});
}

TEST(admin_api_policy_flow) {
    SvcEnv env;
    Credential root{kRootAk, kRootSk};
    CHECK_EQ(env.call("PUT", "/logs-a", root).status, 200);
    CHECK_EQ(env.call("PUT", "/logs-a/k", root, {}, "v").status, 200);
    CHECK_EQ(env.call("PUT", "/private", root).status, 200);

    auto created = env.call(
        "POST", "/-/admin/credentials", root, {},
        R"({"comment":"scoped","policy":{"buckets":["logs-*"],"readonly":true}})");
    CHECK_EQ(created.status, 201);
    auto j = body_json(created);
    CHECK_EQ(j.at("comment").get<std::string>(), "scoped");
    CHECK(j.at("policy").at("readonly").get<bool>());
    Credential dyn{j.at("access_key").get<std::string>(),
                   j.at("secret_key").get<std::string>()};

    CHECK_EQ(env.call("GET", "/logs-a/k", dyn).status, 200);
    CHECK_EQ(env.call("PUT", "/logs-a/new", dyn, {}, "x").status, 403);  // readonly
    CHECK_EQ(env.call("GET", "/private/k", dyn).status, 403);            // outside the allowlist
    // ListBuckets is now filtered by policy (docs/archive/gaps.md §5.10): bucket names are precisely the first step
    // of an attack chain, and a restricted credential should not see that buckets outside its allowlist exist
    auto lb = env.call("GET", "/", dyn);
    CHECK_EQ(lb.status, 200);
    CHECK(lb.small_body.find("<Name>logs-a</Name>") != std::string::npos);
    CHECK(lb.small_body.find("<Name>private</Name>") == std::string::npos);

    // copy-source is also policy-constrained (sealing the read side channel, docs/credential-management.md §10.4): a writable scoped
    // credential copying from a bucket outside the allowlist -> 403, inside -> 200
    auto j2 = body_json(env.call("POST", "/-/admin/credentials", root, {},
                                 R"({"policy":{"buckets":["logs-*"]}})"));
    Credential dyn2{j2.at("access_key").get<std::string>(),
                    j2.at("secret_key").get<std::string>()};
    CHECK_EQ(env.call("PUT", "/logs-a/stolen", dyn2, {}, "",
                      {{"x-amz-copy-source", "/private/k"}})
                 .status,
             403);
    CHECK_EQ(env.call("PUT", "/private/k", root, {}, "secret").status, 200);
    CHECK_EQ(env.call("PUT", "/logs-a/copied", dyn2, {}, "",
                      {{"x-amz-copy-source", "/logs-a/k"}})
                 .status,
             200);

    // Strict validation: unknown field / invalid policy -> 400
    CHECK_EQ(env.call("POST", "/-/admin/credentials", root, {}, R"({"bogus":1})").status,
             400);
    CHECK_EQ(env.call("POST", "/-/admin/credentials", root, {},
                      R"({"policy":{"buckets":"not-an-array"}})")
                 .status,
             400);
}

TEST(credstore_file_provider_and_hot_reload) {
    namespace fs = std::filesystem;
    // With pid: unit_tests of multiple build variants running in parallel must not overwrite each other
    auto path = fs::temp_directory_path() /
                ("lights3-test-creds-" + std::to_string(::getpid()) + ".json");
    std::ofstream(path) << R"({"credentials":[
        {"access_key":"FILEAKAAA","secret_key":"file-secret-1","comment":"from-file"},
        {"access_key":"FILEAKBBB","secret_key":"file-secret-2","policy":{"readonly":true}},
        {"access_key":")" << kRootAk << R"(","secret_key":"file-tries-override"}]})";
    AuthConfig cfg = root_cfg();
    cfg.credentials_file = path.string();
    auto be = std::make_shared<storage::MemoryBackend>();
    auto store = load_store(be, cfg);

    CHECK(store->secret_for("FILEAKAAA").value_or("") == "file-secret-1");
    CHECK(!store->is_root("FILEAKAAA"));  // file-sourced credentials are data-plane only
    CHECK(store->secret_for(kRootAk).value_or("") == kRootSk);  // static wins for the same AK
    CHECK_THROWS_S3(store->authorize("FILEAKBBB", "b", "", Action::Write), S3ErrorCode::AccessDenied);
    CHECK_THROWS_S3(sync_wait(store->remove("FILEAKAAA")), S3ErrorCode::MethodNotAllowed);
    auto info = store->find("FILEAKAAA");
    CHECK(info && info->source == CredSource::kFile && info->comment == "from-file");

    // Hot reload: AAA removed, CCC added
    std::ofstream(path, std::ios::trunc)
        << R"({"credentials":[{"access_key":"FILEAKCCC","secret_key":"file-secret-3"}]})";
    store->reload_file_now();
    CHECK(!store->secret_for("FILEAKAAA"));
    CHECK(store->secret_for("FILEAKCCC").value_or("") == "file-secret-3");

    // Parse failure: the old table is kept
    std::ofstream(path, std::ios::trunc) << "{broken json";
    store->reload_file_now();
    CHECK(store->secret_for("FILEAKCCC").value_or("") == "file-secret-3");

    fs::remove(path);
}

TEST(credstore_multi_instance_sync) {
    auto be = std::make_shared<storage::MemoryBackend>();  // two instances share the same backend
    auto a = load_store(be, root_cfg());
    auto b = load_store(be, root_cfg());

    // A generates -> B does not notice, visible after incremental sync
    auto c = sync_wait(a->generate("from-a"));
    CHECK(!b->secret_for(c.access_key));
    sync_wait(b->sync_now());
    CHECK(b->secret_for(c.access_key).value_or("") == c.secret_key);

    // A revokes -> invalid on B after sync; static credentials are unaffected
    sync_wait(a->remove(c.access_key));
    sync_wait(b->sync_now());
    CHECK(!b->secret_for(c.access_key));
    CHECK(b->is_root(kRootAk));
}

TEST(sys_bucket_hidden_from_data_plane) {
    SvcEnv env;
    Credential root{kRootAk, kRootSk};
    env.call("POST", "/-/admin/credentials", root);  // triggers creation of the .sys bucket

    // User requests cannot reach .sys (intercepted at L2), and ListBuckets does not show it
    CHECK_EQ(env.call("GET", "/.sys/credentials/x", root).status, 400);
    auto listed = env.call("GET", "/", root);
    CHECK(listed.small_body.find(".sys") == std::string::npos);
}

// ---------- Regression cases found in review (auth fail-open protection / revocation races) ----------

// Fail-open protection: in the pure-file configuration, the file being wiped to an empty array must not empty the credential table (keep the old table + degraded)
TEST(credstore_file_reload_refuses_empty_table) {
    namespace fs = std::filesystem;
    auto path = fs::temp_directory_path() /
                ("lights3-test-empty-creds-" + std::to_string(::getpid()) + ".json");
    std::ofstream(path) << R"({"credentials":[
        {"access_key":"FILEAKAAA","secret_key":"file-secret-1"}]})";
    AuthConfig cfg;  // no static credentials: auth depends entirely on the file
    cfg.credentials_file = path.string();
    auto be = std::make_shared<storage::MemoryBackend>();
    auto store = load_store(be, cfg);
    CHECK(store->secret_for("FILEAKAAA").has_value());
    CHECK(!store->degraded());

    std::ofstream(path, std::ios::trunc) << R"({"credentials":[]})";
    store->reload_file_now();
    CHECK(store->secret_for("FILEAKAAA").has_value());  // the old table is kept, no anonymous open access
    CHECK(store->degraded());

    // File restored -> applied normally and the flag resets
    std::ofstream(path, std::ios::trunc)
        << R"({"credentials":[{"access_key":"FILEAKBBB","secret_key":"file-secret-2"}]})";
    store->reload_file_now();
    CHECK(!store->secret_for("FILEAKAAA"));
    CHECK(store->secret_for("FILEAKBBB").has_value());
    CHECK(!store->degraded());
    fs::remove(path);
}

// remove vs. sync race: the tombstone refuses to pull a just-revoked credential back into memory
TEST(credstore_sync_tombstone_blocks_revival) {
    auto be = std::make_shared<storage::MemoryBackend>();
    auto store = load_store(be, root_cfg());
    auto c = sync_wait(store->generate("revoke-me"));
    std::string obj_key = std::string(kCredPrefix) + c.access_key;

    // Capture the persisted object; put it back into storage after remove, simulating the interleaving
    // where sync's list already saw the object before the delete took effect
    auto stream = sync_wait(be->get_object(kSysBucket, obj_key, std::nullopt));
    std::string body;
    {
        std::byte buf[4096];
        for (;;) {
            size_t n = sync_wait(stream.body->read(std::span(buf)));
            if (n == 0) break;
            body.append(reinterpret_cast<const char*>(buf), n);
        }
    }
    sync_wait(store->remove(c.access_key));
    storage::ObjectMeta meta;
    meta.content_type = "application/json";
    http::StringBodyReader rb(body);
    sync_wait(be->put_object(kSysBucket, obj_key, std::move(meta), rb));

    sync_wait(store->sync_now());
    CHECK(!store->secret_for(c.access_key));  // revocation takes effect immediately, no resurrection
}

// gaps §3.7: authorization uses the policy snapshot taken at signature-verification time. When a credential is
// revoked after verification (sync_now bulk-deletes every sync cycle, so the window can be hit), the old behavior
// was that the table lookup failed -> the policy vanished entirely -- a readonly credential became unrestricted
// within the window. The snapshot makes in-flight requests complete strictly with the semantics of verification
// time, while new requests fail closed because the AK cannot be found
TEST(credstore_policy_snapshot_survives_revocation) {
    SvcEnv env;
    CredentialPolicy p;
    p.readonly = true;
    auto c = sync_wait(env.store->generate("scoped", p));

    auto auth = SigV4Authenticator::build(root_cfg());
    auth.set_provider(env.store);
    http::HttpRequest req;
    req.method = "PUT";
    req.raw_path = req.path = "/bkt/k";
    req.headers.add("Host", "localhost");
    env.signer.sign(req, Credential{c.access_key, c.secret_key});
    auto ident = auth.verify(req);
    CHECK_EQ(ident.access_key, c.access_key);
    CHECK(ident.policy && ident.policy->readonly);

    sync_wait(env.store->remove(c.access_key));
    // In-flight requests keep deciding by the snapshot: writes still denied, reads still allowed, matching verification time
    CHECK(!ident.policy->allows("bkt", "", Action::Write));
    CHECK(ident.policy->allows("bkt", "", Action::Read));
    // A new request after revocation cannot find the AK -> InvalidAccessKeyId (fail-closed, not unrestricted)
    CHECK(!env.store->lookup(c.access_key));
}

TEST(policy_action_and_prefix_granularity) {
    // §5.10: previously readonly was the only switch, so "can write" necessarily meant "can delete"; there was no key-prefix granularity either
    SvcEnv env;
    Credential root{kRootAk, kRootSk};
    CHECK_EQ(env.call("PUT", "/data", root).status, 200);
    CHECK_EQ(env.call("PUT", "/data/keep.txt", root, {}, "v").status, 200);
    CHECK_EQ(env.call("PUT", "/data/tenant-a/x", root, {}, "v").status, 200);
    CHECK_EQ(env.call("PUT", "/data/tenant-b/x", root, {}, "v").status, 200);

    // Backup scenario: can read and write, cannot delete
    auto j = body_json(env.call("POST", "/-/admin/credentials", root, {},
                                R"({"policy":{"actions":["read","write"]}})"));
    Credential backup{j.at("access_key").get<std::string>(),
                      j.at("secret_key").get<std::string>()};
    CHECK_EQ(env.call("GET", "/data/keep.txt", backup).status, 200);
    CHECK_EQ(env.call("PUT", "/data/new.txt", backup, {}, "x").status, 200);
    CHECK_EQ(env.call("DELETE", "/data/keep.txt", backup).status, 403);
    CHECK_EQ(env.call("GET", "/data/keep.txt", root).status, 200);  // indeed not deleted
    // DeleteObjects is a POST, but classified by action it is still a delete -- the method dimension cannot tell these apart
    CHECK_EQ(env.call("POST", "/data", backup, {{"delete", ""}},
                      "<Delete><Object><Key>keep.txt</Key></Object></Delete>")
                 .status,
             403);
    // Creating a multipart upload is a write, allowed (also a POST; the method dimension cannot tell these two apart)
    CHECK_EQ(env.call("POST", "/data/mp", backup, {{"uploads", ""}}).status, 200);

    // Multi-tenant shared bucket: key-prefix granularity
    auto j2 = body_json(env.call("POST", "/-/admin/credentials", root, {},
                                 R"({"policy":{"prefixes":["tenant-a/"]}})"));
    Credential ta{j2.at("access_key").get<std::string>(),
                  j2.at("secret_key").get<std::string>()};
    CHECK_EQ(env.call("GET", "/data/tenant-a/x", ta).status, 200);
    CHECK_EQ(env.call("GET", "/data/tenant-b/x", ta).status, 403);
    CHECK_EQ(env.call("PUT", "/data/tenant-b/y", ta, {}, "x").status, 403);
    CHECK_EQ(env.call("PUT", "/data/tenant-a/y", ta, {}, "x").status, 200);
    // Bucket-level operations (listing) concern no specific object and are not prefix-restricted
    CHECK_EQ(env.call("GET", "/data", ta).status, 200);

    // Invalid action name -> 400 (silently ignoring a misspelled restriction field amounts to granting access)
    CHECK_EQ(env.call("POST", "/-/admin/credentials", root, {},
                      R"({"policy":{"actions":["destroy"]}})")
                 .status,
             400);
    CHECK_EQ(env.call("POST", "/-/admin/credentials", root, {},
                      R"({"policy":{"actions":[]}})")
                 .status,
             400);
}

TEST(policy_glob_does_not_cross_slash) {
    // §5.10: fnmatch without FNM_PATHNAME lets '*' cross '/'; using the same matcher on prefixes
    // would make "logs/*" also allow "logs/a/b"
    CredentialPolicy p;
    p.buckets = {"logs-*"};
    CHECK(p.allows_bucket("logs-app"));
    CHECK(!p.allows_bucket("other"));
    CredentialPolicy q;
    q.buckets = {"a/*"};
    CHECK(q.allows_bucket("a/b"));
    CHECK(!q.allows_bucket("a/b/c"));  // '*' does not cross '/'
}

TEST(admin_api_never_returns_static_secret) {
    // §5.10: root's plaintext SK could be retrieved with a single HTTP GET, yet it cannot be revoked via the admin API
    SvcEnv env;
    Credential root{kRootAk, kRootSk};
    auto got = body_json(env.call("GET", std::string("/-/admin/credentials/") + kRootAk, root,
                                  {{"show-secret", "true"}}));
    CHECK(!got.contains("secret_key"));
    CHECK(got.contains("secret_key_masked"));
    CHECK(got.at("secret_key_masked").get<std::string>().find(kRootSk) == std::string::npos);

    // Dynamic credentials are unaffected: issued by the API, they can also be revoked by the API
    auto j = body_json(env.call("POST", "/-/admin/credentials", root, {}, "{}"));
    std::string ak = j.at("access_key").get<std::string>();
    auto dyn = body_json(env.call("GET", "/-/admin/credentials/" + ak, root,
                                  {{"show-secret", "true"}}));
    CHECK(dyn.contains("secret_key"));
}

// Per-key policy re-check for batch delete and listing: POST /bucket?delete and GET /bucket are both authorized
// at bucket scope (the key is empty, so the prefix check is skipped entirely); batch delete must re-examine every
// Key in the XML with the same decision as a single delete, and listing must filter results by the policy's
// prefixes -- otherwise a prefix-restricted credential (multi-tenant shared bucket) could delete/enumerate objects outside its allowlist
TEST(policy_prefix_batch_delete_and_listing) {
    SvcEnv env;
    Credential root{kRootAk, kRootSk};
    CHECK_EQ(env.call("PUT", "/shared", root).status, 200);
    CHECK_EQ(env.call("PUT", "/shared/logs/mine", root, {}, "v1").status, 200);
    CHECK_EQ(env.call("PUT", "/shared/other/secret", root, {}, "v2").status, 200);

    auto j = body_json(env.call(
        "POST", "/-/admin/credentials", root, {},
        R"({"policy":{"buckets":["shared"],"actions":["read","delete"],"prefixes":["logs/"]}})"));
    Credential dyn{j.at("access_key").get<std::string>(),
                   j.at("secret_key").get<std::string>()};

    // Single-delete benchmark: outside the allowlist gets 403 -- the batch-delete decision must match it
    CHECK_EQ(env.call("DELETE", "/shared/other/secret", dyn).status, 403);

    // Listing without a prefix: keys outside the allowlist must not appear
    auto lo = env.call("GET", "/shared", dyn);
    CHECK_EQ(lo.status, 200);
    CHECK(lo.small_body.find("logs/mine") != std::string::npos);
    CHECK(lo.small_body.find("other/secret") == std::string::npos);
    // Under delimiter grouping, CommonPrefixes outside the allowlist must not appear either
    auto lg = env.call("GET", "/shared", dyn, {{"delimiter", "/"}});
    CHECK_EQ(lg.status, 200);
    CHECK(lg.small_body.find("<Prefix>logs/</Prefix>") != std::string::npos);
    CHECK(lg.small_body.find("<Prefix>other/</Prefix>") == std::string::npos);

    // Batch delete mixing inside and outside the allowlist: outside keys get per-key AccessDenied with the object intact, inside keys delete normally
    std::string xml = "<Delete><Object><Key>other/secret</Key></Object>"
                      "<Object><Key>logs/mine</Key></Object></Delete>";
    util::HashStream h(util::HashStream::Algo::Md5);
    h.update(std::span(reinterpret_cast<const uint8_t*>(xml.data()), xml.size()));
    auto d = h.final_bytes();
    auto resp = env.call("POST", "/shared", dyn, {{"delete", ""}}, xml,
                         {{"Content-MD5", util::base64_encode(std::span(d.data(), d.size()))}});
    CHECK_EQ(resp.status, 200);
    CHECK(resp.small_body.find("<Error><Key>other/secret</Key><Code>AccessDenied</Code>") !=
          std::string::npos);
    CHECK(resp.small_body.find("<Deleted><Key>logs/mine</Key>") != std::string::npos);
    CHECK_EQ(env.call("GET", "/shared/other/secret", root).status, 200);  // not deleted beyond authority
    CHECK_EQ(env.call("GET", "/shared/logs/mine", root).status, 404);
}
