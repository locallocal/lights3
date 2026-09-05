// roadmap §6.2: GET /-/admin/objects/<bucket>/<key> (s3adm object inspect) — root
// gate, JSON shape, and each engine's layout: localfs (data path, inode extent,
// metadata source), duostore (pack/chunk extents with crc), tiered (tier view +
// the local engine's layout), memory (no layout), plus the error mapping
#include <algorithm>
#include <filesystem>
#include <nlohmann/json.hpp>

#include "core/util/crypto.h"
#include "storage/registry.h"

#include "core/thread_pool.h"
#include "s3/auth/credential_store.h"
#include "s3/service.h"
#include "storage/localfs/localfs_backend.h"
#include "storage/memory/memory_backend.h"
#include "storage/tiered/tiered_backend.h"
#include "unit/backend_suite.h"
#include "unit/mini_test.h"
#ifdef LIGHTS3_DUOSTORE
#include "storage/duostore/duostore_backend.h"
#endif

using namespace lights3;
using namespace lights3::s3;
using json = nlohmann::json;

namespace {

struct Env {
    AuthConfig acfg;
    SigV4Authenticator auth;
    S3Service svc;
    Env(std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends,
        std::vector<BucketRule> rules = {}, std::string def = "main")
        : acfg(make_auth()),
          auth(SigV4Authenticator::build(acfg)),
          svc(make_router(std::move(backends), std::move(rules), std::move(def)), auth) {
        svc.set_credential_store(
            sync_wait(CredentialStore::load(std::make_shared<storage::MemoryBackend>(), acfg)));
    }
    static AuthConfig make_auth() {
        AuthConfig a;
        a.credentials = {{"ROOTAK", "root-sk"}};
        return a;
    }
    static storage::BucketRouter make_router(
        std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends,
        std::vector<BucketRule> rules, std::string def) {
        BucketsConfig cfg;
        cfg.default_backend = std::move(def);
        cfg.rules = std::move(rules);
        return storage::BucketRouter::build(cfg, std::move(backends));
    }
    http::HttpRequest req(std::string method, std::string path, std::string body = "",
                          bool sign = true) {
        http::HttpRequest r;
        r.method = std::move(method);
        r.raw_path = path;
        r.path = std::move(path);
        r.headers.add("Host", "localhost");
        r.headers.add("Content-Length", std::to_string(body.size()));
        std::string hash = body.empty() ? "" : util::sha256_hex(body);
        if (!body.empty()) r.body = std::make_unique<http::StringBodyReader>(std::move(body));
        if (sign) auth.sign(r, acfg.credentials[0], hash);
        return r;
    }
    json inspect(const std::string& bucket, const std::string& key, int expect = 200) {
        auto resp = sync_wait(svc.dispatch(req("GET", "/-/admin/objects/" + bucket + "/" + key)));
        if (resp.status != expect)
            throw mini_test::Failure("inspect " + bucket + "/" + key + ": status " +
                                     std::to_string(resp.status) + " body " + resp.small_body);
        return json::parse(resp.small_body);
    }
    void put(const std::string& path, const std::string& body) {
        auto resp = sync_wait(svc.dispatch(req("PUT", path, body)));
        if (resp.status != 200)
            throw mini_test::Failure("PUT " + path + ": " + std::to_string(resp.status));
    }
};

}  // namespace

TEST(admin_inspect_gate_and_errors) {
    Env env({{"main", std::make_shared<storage::MemoryBackend>()}});
    env.put("/bkt", "");
    env.put("/bkt/k", "v");
    // Unsigned / non-GET / malformed path / missing key / missing bucket
    auto anon = sync_wait(env.svc.dispatch(env.req("GET", "/-/admin/objects/bkt/k", "", false)));
    CHECK_EQ(anon.status, 403);
    auto del = sync_wait(env.svc.dispatch(env.req("DELETE", "/-/admin/objects/bkt/k")));
    CHECK_EQ(del.status, 405);
    auto bad = sync_wait(env.svc.dispatch(env.req("GET", "/-/admin/objects/bkt")));
    CHECK_EQ(bad.status, 400);
    CHECK_EQ(env.inspect("bkt", "nope", 404)["code"].get<std::string>(), "NoSuchKey");
    CHECK_EQ(env.inspect("ghost", "k", 404)["code"].get<std::string>(), "NoSuchBucket");
    // memory: no layout, but the routing is reported
    auto j = env.inspect("bkt", "k");
    CHECK_EQ(j["bucket"].get<std::string>(), "bkt");
    CHECK_EQ(j["key"].get<std::string>(), "k");
    CHECK_EQ(j["backend"].get<std::string>(), "main");
    CHECK(j["layout"].is_null());
    CHECK(j.contains("note"));
    // Keys with slashes are the rest of the path
    env.put("/bkt/a/b/c.txt", "deep");
    CHECK_EQ(env.inspect("bkt", "a/b/c.txt")["key"].get<std::string>(), "a/b/c.txt");
}

TEST(admin_inspect_localfs_layout) {
    backend_suite::TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    auto fs = std::make_shared<storage::LocalFsBackend>(tmp.path / "data", tmp.path / "staging", pool);
    Env env({{"main", fs}});
    env.put("/bkt", "");
    env.put("/bkt/dir/obj.bin", "twelve bytes");
    auto j = env.inspect("bkt", "dir/obj.bin");
    auto& l = j["layout"];
    CHECK_EQ(l["engine"].get<std::string>(), "localfs");
    auto& a = l["attrs"];
    CHECK(a["data_path"].get<std::string>().find(tmp.path.string()) == 0);
    CHECK_EQ(a["logical_size"].get<std::string>(), "12");
    CHECK_EQ(a["on_disk_bytes"].get<std::string>(), "12");
    CHECK_EQ(a["tier"].get<std::string>(), "local");
    CHECK(a["etag"].get<std::string>().size() == 32);
    CHECK(a.contains("meta_xattr") && a.contains("sidecar"));
    CHECK_EQ(l["extents"].size(), size_t(1));
    CHECK_EQ(l["extents"][0]["kind"].get<std::string>(), "file");
    CHECK_EQ(l["extents"][0]["length"].get<uint64_t>(), uint64_t(12));
    CHECK(l["extents"][0]["id"].get<uint64_t>() > 0);  // inode
    sync_wait(fs->close());
}

TEST(admin_inspect_tiered_layout) {
    backend_suite::TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    auto local = std::make_shared<storage::LocalFsBackend>(tmp.path / "data", tmp.path / "staging", pool);
    storage::TieredConfig tcfg;
    tcfg.scan_interval_sec = 0;
    auto tiered = std::make_shared<storage::TieredBackend>(local, std::make_shared<storage::MemoryBackend>(),
                                                           pool, tcfg);
    Env env({{"main", tiered}});
    env.put("/bkt", "");
    env.put("/bkt/hot", "hot bytes");
    auto j = env.inspect("bkt", "hot");
    auto& l = j["layout"];
    CHECK_EQ(l["engine"].get<std::string>(), "tiered");
    CHECK_EQ(l["attrs"]["tier"].get<std::string>(), "local");
    CHECK_EQ(l["attrs"]["local_bytes"].get<std::string>(), "9");
    CHECK_EQ(l["attrs"]["local_engine"].get<std::string>(), "localfs");
    CHECK(l["attrs"].contains("local.data_path"));
    CHECK_EQ(l["extents"].size(), size_t(1));
    // Demoted: the local side keeps a stub, the layout says so
    sync_wait(tiered->demote_object("bkt", "hot"));
    j = env.inspect("bkt", "hot");
    CHECK_EQ(j["layout"]["attrs"]["tier"].get<std::string>(), "remote");
    CHECK_EQ(j["layout"]["attrs"]["local_bytes"].get<std::string>(), "0");
    CHECK(j["layout"]["attrs"].contains("remote_etag"));
    CHECK_EQ(j["layout"]["attrs"]["local.on_disk_bytes"].get<std::string>(), "0");
    sync_wait(tiered->close());
}

#ifdef LIGHTS3_DUOSTORE
TEST(admin_inspect_duostore_layout) {
    backend_suite::TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    storage::DuoStoreConfig cfg;
    cfg.name = "duo";
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.meta_sync = false;
    cfg.chunk_size = 4096;
    cfg.pack_threshold = 8192;  // <= 8KiB packed, above chunked
    auto duo = std::make_shared<storage::DuoStoreBackend>(std::move(cfg), pool);
    Env env({{"main", duo}});
    env.put("/bkt", "");
    env.put("/bkt/small", "packed");               // below pack_threshold -> one pack extent
    env.put("/bkt/big", std::string(20000, 'x'));  // 5 chunks of 4KiB
    auto j = env.inspect("bkt", "small");
    auto& l = j["layout"];
    CHECK_EQ(l["engine"].get<std::string>(), "duostore");
    CHECK_EQ(l["attrs"]["logical_size"].get<std::string>(), "6");
    CHECK_EQ(l["attrs"]["meta_version"].get<std::string>(), "1");
    CHECK_EQ(l["attrs"]["tier"].get<std::string>(), "local");
    CHECK_EQ(l["extents"].size(), size_t(1));
    CHECK_EQ(l["extents"][0]["kind"].get<std::string>(), "pack");
    CHECK_EQ(l["extents"][0]["length"].get<uint64_t>(), uint64_t(6));
    CHECK(l["extents"][0]["crc32c"].get<uint32_t>() != 0);
    auto big = env.inspect("bkt", "big")["layout"];
    CHECK_EQ(big["extents"].size(), size_t(5));
    CHECK_EQ(big["extents"][0]["kind"].get<std::string>(), "chunk");
    CHECK_EQ(big["attrs"]["stored_bytes"].get<std::string>(), "20000");
    // Overwrite bumps the meta version
    env.put("/bkt/small", "packed2");
    CHECK_EQ(env.inspect("bkt", "small")["layout"]["attrs"]["meta_version"].get<std::string>(), "2");
    sync_wait(duo->close());
}
#endif

TEST(registry_lists_registered_types) {
    auto types = storage::StorageRegistry::registered_types();
    auto has = [&](const char* t) { return std::find(types.begin(), types.end(), t) != types.end(); };
    CHECK(has("localfs") && has("memory") && has("tiered"));
}
