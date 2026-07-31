// docs/credential-management.md：CredentialStore 持久化/两级权限 + /-/admin/credentials 全流程
// + 二期（§10）：at-rest 加密 / per-credential policy / 文件热加载 / 多实例增量同步
#include <cstdlib>
#include <filesystem>
#include <fstream>
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

    auto reloaded = load_store(be, root_cfg());  // 模拟进程重启
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

// ---------- 二期（docs/credential-management.md §10）----------

namespace {

// 64 hex = 32 字节 master key（测试固定值）
constexpr const char* kTestMasterKeyHex =
    "000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f";

struct EnvGuard {  // 测试期设置环境变量，作用域结束清除
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
    // 随机 nonce：同明文两次 seal 输出不同
    CHECK(util::aes256gcm_seal(key, "x") != util::aes256gcm_seal(key, "x"));
}

TEST(credstore_encrypted_at_rest) {
    EnvGuard env(kMasterKeyEnv, kTestMasterKeyHex);
    auto be = std::make_shared<storage::MemoryBackend>();
    auto c = sync_wait(load_store(be, root_cfg())->generate("enc"));

    auto raw = read_object_raw(*be, "credentials/" + c.access_key);
    CHECK(raw.find("\"version\": 2") != std::string::npos);
    CHECK(raw.find("sk_enc") != std::string::npos);
    CHECK(raw.find(c.secret_key) == std::string::npos);  // 明文 SK 不落盘

    auto reloaded = load_store(be, root_cfg());
    CHECK(reloaded->secret_for(c.access_key).value_or("") == c.secret_key);
}

TEST(credstore_v2_requires_correct_master_key) {
    auto be = std::make_shared<storage::MemoryBackend>();
    {
        EnvGuard env(kMasterKeyEnv, kTestMasterKeyHex);
        sync_wait(load_store(be, root_cfg())->generate("enc"));
    }
    // 无 key / 错 key / 格式错的 key 都是配置错误：fail-fast 而非静默丢凭证
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
    auto c = sync_wait(load_store(be, root_cfg())->generate("old"));  // 无 key：v1 明文
    CHECK(read_object_raw(*be, "credentials/" + c.access_key).find("\"sk\"") !=
          std::string::npos);

    EnvGuard env(kMasterKeyEnv, kTestMasterKeyHex);
    auto store = load_store(be, root_cfg());  // load 时就地升级
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

    store->authorize(c.access_key, "logs-app", /*is_write=*/false);  // 命中 glob
    store->authorize(c.access_key, "", false);                       // ListBuckets 放行
    store->authorize(kRootAk, "anything", true);                     // root 无限制
    CHECK_THROWS_S3(store->authorize(c.access_key, "logs-app", true),
                    S3ErrorCode::AccessDenied);  // readonly
    CHECK_THROWS_S3(store->authorize(c.access_key, "other", false),
                    S3ErrorCode::AccessDenied);  // 白名单外

    auto info = load_store(be, root_cfg())->find(c.access_key);  // 持久化
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
    CHECK_EQ(env.call("GET", "/private/k", dyn).status, 403);            // 白名单外
    CHECK_EQ(env.call("GET", "/", dyn).status, 200);                     // ListBuckets

    // copy-source 也受 policy 约束（读旁路封堵，docs/todo.md §4）：可写的 scoped
    // 凭证从白名单外的桶 copy → 403，白名单内 → 200
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

    // 严格校验：未知字段 / 非法 policy → 400
    CHECK_EQ(env.call("POST", "/-/admin/credentials", root, {}, R"({"bogus":1})").status,
             400);
    CHECK_EQ(env.call("POST", "/-/admin/credentials", root, {},
                      R"({"policy":{"buckets":"not-an-array"}})")
                 .status,
             400);
}

TEST(credstore_file_provider_and_hot_reload) {
    namespace fs = std::filesystem;
    auto path = fs::temp_directory_path() / "lights3-test-creds.json";
    std::ofstream(path) << R"({"credentials":[
        {"access_key":"FILEAKAAA","secret_key":"file-secret-1","comment":"from-file"},
        {"access_key":"FILEAKBBB","secret_key":"file-secret-2","policy":{"readonly":true}},
        {"access_key":")" << kRootAk << R"(","secret_key":"file-tries-override"}]})";
    AuthConfig cfg = root_cfg();
    cfg.credentials_file = path.string();
    auto be = std::make_shared<storage::MemoryBackend>();
    auto store = load_store(be, cfg);

    CHECK(store->secret_for("FILEAKAAA").value_or("") == "file-secret-1");
    CHECK(!store->is_root("FILEAKAAA"));  // file 来源仅数据面
    CHECK(store->secret_for(kRootAk).value_or("") == kRootSk);  // 同 AK 静态优先
    CHECK_THROWS_S3(store->authorize("FILEAKBBB", "b", true), S3ErrorCode::AccessDenied);
    CHECK_THROWS_S3(sync_wait(store->remove("FILEAKAAA")), S3ErrorCode::MethodNotAllowed);
    auto info = store->find("FILEAKAAA");
    CHECK(info && info->source == CredSource::kFile && info->comment == "from-file");

    // 热加载：AAA 删除、CCC 新增
    std::ofstream(path, std::ios::trunc)
        << R"({"credentials":[{"access_key":"FILEAKCCC","secret_key":"file-secret-3"}]})";
    store->reload_file_now();
    CHECK(!store->secret_for("FILEAKAAA"));
    CHECK(store->secret_for("FILEAKCCC").value_or("") == "file-secret-3");

    // 解析失败：保留旧表
    std::ofstream(path, std::ios::trunc) << "{broken json";
    store->reload_file_now();
    CHECK(store->secret_for("FILEAKCCC").value_or("") == "file-secret-3");

    fs::remove(path);
}

TEST(credstore_multi_instance_sync) {
    auto be = std::make_shared<storage::MemoryBackend>();  // 两实例共享同一后端
    auto a = load_store(be, root_cfg());
    auto b = load_store(be, root_cfg());

    // A 生成 → B 感知不到，增量同步后可见
    auto c = sync_wait(a->generate("from-a"));
    CHECK(!b->secret_for(c.access_key));
    sync_wait(b->sync_now());
    CHECK(b->secret_for(c.access_key).value_or("") == c.secret_key);

    // A 吊销 → B 同步后失效；静态凭证不受影响
    sync_wait(a->remove(c.access_key));
    sync_wait(b->sync_now());
    CHECK(!b->secret_for(c.access_key));
    CHECK(b->is_root(kRootAk));
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
