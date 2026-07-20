// docs/credential-management.md：CredentialStore 持久化/两级权限 + /-/admin/credentials 全流程
#include <nlohmann/json.hpp>

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

// 完整装配一台带凭证管理的 service（memory 后端同时承载数据面与 .sys）
struct SvcEnv {
    std::shared_ptr<storage::MemoryBackend> backend;
    std::shared_ptr<CredentialStore> store;
    std::unique_ptr<S3Service> svc;
    SigV4Authenticator signer;  // 测试侧签名端（与服务端同 region/service）

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
                            std::string body = "") {
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

    auto reloaded = load_store(be, root_cfg());  // 模拟进程重启
    CHECK(reloaded->secret_for(c.access_key).value_or("") == c.secret_key);
    auto info = reloaded->find(c.access_key);
    CHECK(info && info->comment == "persist-me" && !info->is_static);
}

TEST(credstore_remove_semantics) {
    auto be = std::make_shared<storage::MemoryBackend>();
    auto store = load_store(be, root_cfg());
    auto c = sync_wait(store->generate(""));

    sync_wait(store->remove(c.access_key));
    CHECK(!store->secret_for(c.access_key));
    CHECK_THROWS_S3(sync_wait(store->remove(c.access_key)), S3ErrorCode::InvalidAccessKeyId);
    CHECK_THROWS_S3(sync_wait(store->remove(kRootAk)), S3ErrorCode::MethodNotAllowed);
    // 吊销后重启也不复活
    CHECK(!load_store(be, root_cfg())->secret_for(c.access_key));
}

TEST(credstore_static_wins_on_conflict) {
    auto be = std::make_shared<storage::MemoryBackend>();
    // 无静态表的 store 生成动态凭证，再以"同 AK 的静态表"重启加载
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
    CHECK_EQ(auth.verify(req), c.access_key);
}

// ---------- /-/admin/credentials ----------

TEST(admin_api_full_flow) {
    SvcEnv env;
    Credential root{kRootAk, kRootSk};

    // 生成
    auto created = env.call("POST", "/-/admin/credentials", root, {{"comment", "ci"}});
    CHECK_EQ(created.status, 201);
    auto j = body_json(created);
    Credential dyn{j.at("access_key").get<std::string>(),
                   j.at("secret_key").get<std::string>()};
    CHECK_EQ(j.at("source").get<std::string>(), "dynamic");
    CHECK_EQ(j.at("comment").get<std::string>(), "ci");

    // 新凭证走数据面全流程
    CHECK_EQ(env.call("PUT", "/bkt", dyn).status, 200);
    CHECK_EQ(env.call("PUT", "/bkt/hello.txt", dyn, {}, "hi").status, 200);
    CHECK_EQ(env.call("GET", "/bkt/hello.txt", dyn).status, 200);

    // 列表：含 root 与动态凭证，SK 掩码不外泄
    auto listed = env.call("GET", "/-/admin/credentials", root);
    CHECK_EQ(listed.status, 200);
    auto lj = body_json(listed);
    CHECK_EQ(lj.at("credentials").size(), size_t{2});
    for (auto& e : lj.at("credentials")) {
        CHECK(!e.contains("secret_key"));
        CHECK(e.at("secret_key_masked").get<std::string>().find("****") != std::string::npos);
    }

    // 单查：默认掩码，show-secret 显式索取
    auto masked = env.call("GET", "/-/admin/credentials/" + dyn.access_key, root);
    CHECK(!body_json(masked).contains("secret_key"));
    auto shown = env.call("GET", "/-/admin/credentials/" + dyn.access_key, root,
                          {{"show-secret", "true"}});
    CHECK_EQ(body_json(shown).at("secret_key").get<std::string>(), dyn.secret_key);

    // 吊销：204 → 数据面新请求 403，admin 单查 403
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

    // 动态凭证不能生凭证（提权链）
    auto resp = env.call("POST", "/-/admin/credentials", dyn);
    CHECK_EQ(resp.status, 403);
    CHECK_EQ(body_json(resp).at("code").get<std::string>(), "AccessDenied");
    // 错误体是 JSON 而非 S3 XML
    CHECK_EQ(*resp.headers.get("Content-Type"), "application/json");
}

TEST(admin_api_denied_when_auth_disabled) {
    SvcEnv env{AuthConfig{}};  // 无静态凭证：认证关闭，没有 root 就没有管理面
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

TEST(sys_bucket_hidden_from_data_plane) {
    SvcEnv env;
    Credential root{kRootAk, kRootSk};
    env.call("POST", "/-/admin/credentials", root);  // 触发 .sys 建桶

    // 用户请求到不了 .sys（L2 拦截），ListBuckets 也看不到
    CHECK_EQ(env.call("GET", "/.sys/credentials/x", root).status, 400);
    auto listed = env.call("GET", "/", root);
    CHECK(listed.small_body.find(".sys") == std::string::npos);
}
