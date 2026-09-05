// roadmap §3.9: usage accounting, bucket/tenant quotas, tenancy, audit log
// (docs/multi-tenancy.md). Full-dispatch tests on the memory backend.
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>

#include "core/config.h"
#include "core/util/checksum.h"
#include "core/util/crypto.h"
#include "s3/audit.h"
#include "s3/auth/credential_store.h"
#include "s3/lifecycle.h"
#include "s3/quota.h"
#include "s3/service.h"
#include "s3/tenant.h"
#include "s3/usage.h"
#include "storage/memory/memory_backend.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::s3;
using nlohmann::json;

namespace {

constexpr const char* kRootAk = "ROOTKEYTENANCY";
constexpr const char* kRootSk = "root-secret-key";

AuthConfig root_cfg() {
    AuthConfig cfg;
    cfg.credentials = {{kRootAk, kRootSk}};
    return cfg;
}

std::string temp_path(const char* stem) {
    return "/tmp/lights3-tenancy-" + std::to_string(::getpid()) + "-" + stem;
}

// Everything §3.9 wires in main, on one memory backend (data plane + .sys)
struct Env {
    std::shared_ptr<storage::MemoryBackend> backend = std::make_shared<storage::MemoryBackend>();
    AuthConfig acfg;
    std::shared_ptr<CredentialStore> cred_store;
    std::shared_ptr<UsageTracker> usage;
    std::shared_ptr<QuotaStore> quota;
    std::shared_ptr<TenantStore> tenant_store;
    std::shared_ptr<OwnerStore> owner_store;
    std::shared_ptr<TenantRegistry> tenants;
    std::shared_ptr<AuditLog> audit;
    std::unique_ptr<S3Service> svc;
    SigV4Authenticator signer;
    Credential root{kRootAk, util::SecretString(std::string(kRootSk))};

    explicit Env(AuthConfig cfg = root_cfg(), UsageConfig ucfg = {}, AuditConfig audit_cfg = {},
                 std::shared_ptr<storage::MemoryBackend> shared = nullptr)
        : acfg(cfg), signer(SigV4Authenticator::build(cfg)) {
        if (shared) backend = shared;
        std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends;
        backends["mem"] = backend;
        BucketsConfig bcfg;
        bcfg.default_backend = "mem";
        auto router = storage::BucketRouter::build(bcfg, backends);
        cred_store = sync_wait(CredentialStore::load(backend, cfg));
        auto auth = SigV4Authenticator::build(cfg);
        auth.set_provider(cred_store);
        usage = sync_wait(UsageTracker::load(storage::BucketRouter::build(bcfg, backends), ucfg,
                                             nullptr));
        quota = sync_wait(QuotaStore::load(backend));
        tenant_store = sync_wait(TenantStore::load(backend));
        owner_store = sync_wait(OwnerStore::load(backend));
        tenants = std::make_shared<TenantRegistry>(tenant_store, owner_store);
        audit = AuditLog::open(audit_cfg);
        svc = std::make_unique<S3Service>(std::move(router), std::move(auth));
        svc->set_credential_store(cred_store);
        svc->set_usage_tracker(usage);
        svc->set_quota_store(quota);
        svc->set_tenant_registry(tenants);
        svc->set_audit_log(audit);
        svc->set_min_part_size(0);
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
        req.headers.add("Content-Length", std::to_string(body.size()));
        for (auto& [k, v] : headers) req.headers.add(k, v);
        std::string hash = util::sha256_hex(body);
        if (!body.empty()) req.body = std::make_unique<http::StringBodyReader>(std::move(body));
        signer.sign(req, cred, hash);
        return sync_wait(svc->dispatch(std::move(req)));
    }
    http::HttpResponse as_root(std::string method, std::string path,
                               std::vector<std::pair<std::string, std::string>> query = {},
                               std::string body = "",
                               std::vector<std::pair<std::string, std::string>> headers = {}) {
        return call(std::move(method), std::move(path), root, std::move(query), std::move(body),
                    std::move(headers));
    }
    // Admin plane helpers (JSON in/out)
    json admin(const Credential& cred, std::string method, std::string path, json body = {},
               std::vector<std::pair<std::string, std::string>> query = {}, int expect = 200) {
        auto r = call(std::move(method), std::move(path), cred, std::move(query),
                      body.is_null() ? "" : body.dump());
        if (r.status != expect)
            throw mini_test::Failure("admin " + path + " -> HTTP " + std::to_string(r.status) +
                                     " " + r.small_body);
        if (r.small_body.empty()) return json::object();
        return json::parse(r.small_body);
    }
    Credential mint(const Credential& by, json body) {
        auto j = admin(by, "POST", "/-/admin/credentials", std::move(body), {}, 201);
        return Credential{j["access_key"].get<std::string>(),
                          util::SecretString(j["secret_key"].get<std::string>())};
    }
    BucketUsage usage_of(const std::string& bucket) {
        return usage->get(bucket).value_or(BucketUsage{});
    }
};

std::string md5_header(const std::string& body) {
    util::HashStream h(util::HashStream::Algo::Md5);
    h.update(std::span(reinterpret_cast<const uint8_t*>(body.data()), body.size()));
    auto d = h.final_bytes();
    return util::base64_encode(std::span(d.data(), d.size()));
}

bool contains(const std::string& s, const std::string& sub) {
    return s.find(sub) != std::string::npos;
}

std::string xelem(const std::string& xml, const std::string& tag) {
    auto open = "<" + tag + ">", close = "</" + tag + ">";
    auto b = xml.find(open);
    if (b == std::string::npos) return "";
    b += open.size();
    auto e = xml.find(close, b);
    return e == std::string::npos ? "" : xml.substr(b, e - b);
}

}  // namespace

// ---------- ① usage accounting ----------

TEST(usage_counters_follow_every_write_path) {
    Env env;
    CHECK_EQ(env.as_root("PUT", "/bkt").status, 200);
    CHECK_EQ(env.as_root("PUT", "/bkt/a", {}, "0123456789").status, 200);
    auto u = env.usage_of("bkt");
    CHECK_EQ(u.objects, int64_t(1));
    CHECK_EQ(u.bytes, int64_t(10));
    // Overwrite nets out the replaced size, object count unchanged
    CHECK_EQ(env.as_root("PUT", "/bkt/a", {}, "abc").status, 200);
    u = env.usage_of("bkt");
    CHECK_EQ(u.objects, int64_t(1));
    CHECK_EQ(u.bytes, int64_t(3));
    // Copy adds the source size under the destination
    CHECK_EQ(env.as_root("PUT", "/bkt/copy", {}, "", {{"x-amz-copy-source", "/bkt/a"}}).status, 200);
    u = env.usage_of("bkt");
    CHECK_EQ(u.objects, int64_t(2));
    CHECK_EQ(u.bytes, int64_t(6));
    // Single delete + idempotent re-delete
    CHECK_EQ(env.as_root("DELETE", "/bkt/copy").status, 204);
    CHECK_EQ(env.as_root("DELETE", "/bkt/copy").status, 204);
    u = env.usage_of("bkt");
    CHECK_EQ(u.objects, int64_t(1));
    CHECK_EQ(u.bytes, int64_t(3));
    // Batch delete
    CHECK_EQ(env.as_root("PUT", "/bkt/x", {}, "xxxx").status, 200);
    CHECK_EQ(env.as_root("PUT", "/bkt/y", {}, "yy").status, 200);
    std::string del = "<Delete><Object><Key>x</Key></Object><Object><Key>y</Key></Object>"
                      "<Object><Key>missing</Key></Object></Delete>";
    CHECK_EQ(env.as_root("POST", "/bkt", {{"delete", ""}}, del, {{"Content-MD5", md5_header(del)}})
                 .status,
             200);
    u = env.usage_of("bkt");
    CHECK_EQ(u.objects, int64_t(1));
    CHECK_EQ(u.bytes, int64_t(3));

    // Multipart: parts sit in mpu_bytes until complete moves them to bytes
    auto init = env.as_root("POST", "/bkt/mp", {{"uploads", ""}});
    CHECK_EQ(init.status, 200);
    std::string upload_id = xelem(init.small_body, "UploadId");
    auto p1 = env.as_root("PUT", "/bkt/mp", {{"partNumber", "1"}, {"uploadId", upload_id}}, "11111");
    auto p2 = env.as_root("PUT", "/bkt/mp", {{"partNumber", "2"}, {"uploadId", upload_id}}, "222");
    CHECK_EQ(p1.status, 200);
    CHECK_EQ(p2.status, 200);
    u = env.usage_of("bkt");
    CHECK_EQ(u.mpu_bytes, int64_t(8));
    CHECK_EQ(u.bytes, int64_t(3));
    std::string complete = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" +
                           *p1.headers.get("ETag") + "</ETag></Part><Part><PartNumber>2</PartNumber><ETag>" +
                           *p2.headers.get("ETag") + "</ETag></Part></CompleteMultipartUpload>";
    CHECK_EQ(env.as_root("POST", "/bkt/mp", {{"uploadId", upload_id}}, complete).status, 200);
    u = env.usage_of("bkt");
    CHECK_EQ(u.objects, int64_t(2));
    CHECK_EQ(u.bytes, int64_t(11));
    CHECK_EQ(u.mpu_bytes, int64_t(0));

    // Abort releases in-flight bytes
    init = env.as_root("POST", "/bkt/mp2", {{"uploads", ""}});
    upload_id = xelem(init.small_body, "UploadId");
    CHECK_EQ(env.as_root("PUT", "/bkt/mp2", {{"partNumber", "1"}, {"uploadId", upload_id}}, "zzzzzz").status, 200);
    CHECK_EQ(env.usage_of("bkt").mpu_bytes, int64_t(6));
    CHECK_EQ(env.as_root("DELETE", "/bkt/mp2", {{"uploadId", upload_id}}).status, 204);
    CHECK_EQ(env.usage_of("bkt").mpu_bytes, int64_t(0));

    // A full count agrees with the incremental counters, and is persisted
    auto scanned = sync_wait(env.usage->rescan("bkt"));
    CHECK_EQ(scanned.objects, int64_t(2));
    CHECK_EQ(scanned.bytes, int64_t(11));
    CHECK(scanned.scanned());
    CHECK(sync_wait(env.backend->bucket_exists(".sys")));
    auto obj = sync_wait(env.backend->head_object(".sys", "usage/bkt"));
    CHECK(obj.size > 0);

    // DeleteBucket forgets the counters
    CHECK_EQ(env.as_root("DELETE", "/bkt/a").status, 204);
    CHECK_EQ(env.as_root("DELETE", "/bkt/mp").status, 204);
    CHECK_EQ(env.as_root("DELETE", "/bkt").status, 204);
    CHECK(!env.usage->get("bkt"));
    CHECK_THROWS_S3(sync_wait(env.backend->head_object(".sys", "usage/bkt")), S3ErrorCode::NoSuchKey);
}

TEST(usage_persists_across_restart_and_bootstraps_unknown_buckets) {
    auto backend = std::make_shared<storage::MemoryBackend>();
    {
        Env env(root_cfg(), {}, {}, backend);
        CHECK_EQ(env.as_root("PUT", "/bkt").status, 200);
        CHECK_EQ(env.as_root("PUT", "/bkt/a", {}, "0123456789").status, 200);
        sync_wait(env.usage->flush());
    }
    // Objects written while accounting was off (or by another process) are found by
    // the bootstrap scan, which only counts buckets without a scanned record
    sync_wait(backend->create_bucket("fresh"));
    http::StringBodyReader body("12345");
    sync_wait(backend->put_object("fresh", "k", storage::ObjectMeta{}, body));
    {
        Env env(root_cfg(), {}, {}, backend);
        auto u = env.usage_of("bkt");
        CHECK_EQ(u.objects, int64_t(1));
        CHECK_EQ(u.bytes, int64_t(10));
        CHECK(!env.usage->get("fresh"));
        size_t n = sync_wait(env.usage->reconcile_all());
        CHECK_EQ(n, size_t(2));
        CHECK_EQ(env.usage_of("fresh").bytes, int64_t(5));
        CHECK(env.usage_of("bkt").scanned());
    }
}

TEST(usage_disabled_skips_accounting_and_quota) {
    UsageConfig ucfg;
    ucfg.enabled = false;
    Env env(root_cfg(), ucfg);
    CHECK_EQ(env.as_root("PUT", "/bkt").status, 200);
    CHECK_EQ(env.as_root("PUT", "/bkt/a", {}, "0123456789").status, 200);
    CHECK(!env.usage->get("bkt"));
    // ?quota refuses to be set: it could never be enforced
    auto r = env.as_root("PUT", "/bkt", {{"quota", ""}}, quota_xml({100, 0}));
    CHECK_EQ(r.status, 400);
    CHECK(contains(r.small_body, "usage.enabled"));
}

TEST(usage_admin_api_and_rescan) {
    Env env;
    CHECK_EQ(env.as_root("PUT", "/bkt").status, 200);
    CHECK_EQ(env.as_root("PUT", "/bkt/a", {}, "0123456789").status, 200);
    auto all = env.admin(env.root, "GET", "/-/admin/usage");
    CHECK_EQ(all["buckets"].size(), size_t(1));
    CHECK_EQ(all["buckets"][0]["bucket"].get<std::string>(), std::string("bkt"));
    CHECK_EQ(all["buckets"][0]["bytes"].get<int64_t>(), int64_t(10));
    CHECK(!all["buckets"][0]["scanned"].get<bool>());
    auto one = env.admin(env.root, "POST", "/-/admin/usage/bkt/rescan");
    CHECK(one["scanned"].get<bool>());
    CHECK_EQ(one["objects"].get<int64_t>(), int64_t(1));
    CHECK_EQ(env.admin(env.root, "GET", "/-/admin/usage/nope", {}, {}, 404)["code"],
             std::string("NoSuchBucket"));
    // Plain dynamic credentials are not admins
    auto plain = env.mint(env.root, {{"comment", "plain"}});
    CHECK_EQ(env.call("GET", "/-/admin/usage", plain).status, 403);
}

// ---------- ② quotas ----------

TEST(bucket_quota_api_and_enforcement) {
    Env env;
    CHECK_EQ(env.as_root("PUT", "/bkt").status, 200);
    auto none = env.as_root("GET", "/bkt", {{"quota", ""}});
    CHECK_EQ(none.status, 404);
    CHECK(contains(none.small_body, "NoSuchQuotaConfiguration"));
    // Root only for PUT; any admitted credential may GET
    auto plain = env.mint(env.root, {{"comment", "plain"}});
    CHECK_EQ(env.call("PUT", "/bkt", plain, {{"quota", ""}}, quota_xml({20, 2})).status, 403);
    CHECK_EQ(env.as_root("PUT", "/nope", {{"quota", ""}}, quota_xml({20, 2})).status, 404);
    CHECK_EQ(env.as_root("PUT", "/bkt", {{"quota", ""}}, quota_xml({0, 0})).status, 400);
    CHECK_EQ(env.as_root("PUT", "/bkt", {{"quota", ""}}, quota_xml({20, 2})).status, 200);
    auto got = env.call("GET", "/bkt", plain, {{"quota", ""}});
    CHECK_EQ(got.status, 200);
    CHECK_EQ(xelem(got.small_body, "MaxBytes"), std::string("20"));
    CHECK_EQ(xelem(got.small_body, "MaxObjects"), std::string("2"));

    // Bytes axis: 12 + 12 > 20
    CHECK_EQ(env.as_root("PUT", "/bkt/a", {}, "012345678901").status, 200);
    auto over = env.as_root("PUT", "/bkt/b", {}, "012345678901");
    CHECK_EQ(over.status, 403);
    CHECK(contains(over.small_body, "QuotaExceeded"));
    // An overwrite that shrinks the object is admitted (replaced size netted out)
    CHECK_EQ(env.as_root("PUT", "/bkt/a", {}, "01").status, 200);
    CHECK_EQ(env.as_root("PUT", "/bkt/b", {}, "012345678901").status, 200);
    // Objects axis: third distinct key refused, overwrite of an existing key allowed
    auto third = env.as_root("PUT", "/bkt/c", {}, "1");
    CHECK_EQ(third.status, 403);
    CHECK(contains(third.small_body, "objects"));
    CHECK_EQ(env.as_root("PUT", "/bkt/a", {}, "01x").status, 200);
    // Copy is gated like a PUT
    CHECK_EQ(env.as_root("PUT", "/bkt/cp", {}, "", {{"x-amz-copy-source", "/bkt/a"}}).status, 403);
    // Removing the quota lifts the gate
    CHECK_EQ(env.as_root("DELETE", "/bkt", {{"quota", ""}}).status, 204);
    CHECK_EQ(env.as_root("PUT", "/bkt/c", {}, "1").status, 200);
    // Counters stayed exact through the refusals
    auto u = env.usage_of("bkt");
    CHECK_EQ(u.objects, int64_t(3));
    CHECK_EQ(u.bytes, int64_t(16));
}

TEST(quota_multipart_mid_flight_semantics) {
    Env env;
    CHECK_EQ(env.as_root("PUT", "/bkt").status, 200);
    CHECK_EQ(env.as_root("PUT", "/bkt", {{"quota", ""}}, quota_xml({10, 0})).status, 200);
    auto init = env.as_root("POST", "/bkt/mp", {{"uploads", ""}});
    CHECK_EQ(init.status, 200);
    std::string id = xelem(init.small_body, "UploadId");
    auto p1 = env.as_root("PUT", "/bkt/mp", {{"partNumber", "1"}, {"uploadId", id}}, "123456");
    CHECK_EQ(p1.status, 200);
    // In-flight bytes count: 6 + 6 > 10
    auto p2 = env.as_root("PUT", "/bkt/mp", {{"partNumber", "2"}, {"uploadId", id}}, "123456");
    CHECK_EQ(p2.status, 403);
    CHECK(contains(p2.small_body, "QuotaExceeded"));
    p2 = env.as_root("PUT", "/bkt/mp", {{"partNumber", "2"}, {"uploadId", id}}, "12");
    CHECK_EQ(p2.status, 200);
    // Quota lowered below the finished object: complete refused, upload kept for abort
    CHECK_EQ(env.as_root("PUT", "/bkt", {{"quota", ""}}, quota_xml({5, 0})).status, 200);
    std::string complete = "<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>" +
                           *p1.headers.get("ETag") + "</ETag></Part><Part><PartNumber>2</PartNumber><ETag>" +
                           *p2.headers.get("ETag") + "</ETag></Part></CompleteMultipartUpload>";
    auto done = env.as_root("POST", "/bkt/mp", {{"uploadId", id}}, complete);
    CHECK_EQ(done.status, 403);
    CHECK_EQ(env.as_root("GET", "/bkt/mp", {{"uploadId", id}}).status, 200);  // still listable
    CHECK_EQ(env.usage_of("bkt").mpu_bytes, int64_t(8));
    // Once a bucket is full, new uploads are refused up front
    CHECK_EQ(env.as_root("POST", "/bkt/other", {{"uploads", ""}}).status, 403);
    CHECK_EQ(env.as_root("DELETE", "/bkt/mp", {{"uploadId", id}}).status, 204);
    CHECK_EQ(env.usage_of("bkt").mpu_bytes, int64_t(0));
    CHECK_EQ(env.as_root("POST", "/bkt/other", {{"uploads", ""}}).status, 200);
}

// ---------- ③ tenants ----------

TEST(tenant_admin_api_lifecycle) {
    Env env;
    // Validation: id charset, unknown fields, duplicates
    CHECK_EQ(env.admin(env.root, "POST", "/-/admin/tenants", {{"id", "Bad Id"}}, {}, 400)["code"],
             std::string("InvalidRequest"));
    CHECK_EQ(env.admin(env.root, "POST", "/-/admin/tenants", {{"id", "acme"}, {"x", 1}}, {}, 400)["code"],
             std::string("InvalidRequest"));
    auto t = env.admin(env.root, "POST", "/-/admin/tenants",
                       {{"id", "acme"}, {"display_name", "ACME"}, {"quota", {{"max_buckets", 2}}}},
                       {}, 201);
    CHECK_EQ(t["id"].get<std::string>(), std::string("acme"));
    CHECK_EQ(t["quota"]["max_buckets"].get<uint64_t>(), uint64_t(2));
    CHECK_EQ(env.admin(env.root, "POST", "/-/admin/tenants", {{"id", "acme"}}, {}, 409)["code"],
             std::string("TenantAlreadyExists"));
    CHECK_EQ(env.admin(env.root, "GET", "/-/admin/tenants")["tenants"].size(), size_t(1));
    CHECK_EQ(env.admin(env.root, "GET", "/-/admin/tenants/ghost", {}, {}, 404)["code"],
             std::string("NoSuchTenant"));
    // Update replaces the quota as a whole
    auto upd = env.admin(env.root, "PUT", "/-/admin/tenants/acme",
                         {{"display_name", "ACME Corp"}, {"quota", {{"max_bytes", 100}}}});
    CHECK_EQ(upd["display_name"].get<std::string>(), std::string("ACME Corp"));
    CHECK_EQ(upd["quota"]["max_bytes"].get<uint64_t>(), uint64_t(100));
    CHECK_EQ(upd["quota"]["max_buckets"].get<uint64_t>(), uint64_t(0));
    CHECK_EQ(upd["rev"].get<uint64_t>(), uint64_t(2));
    // Ownership assignment of a root-created bucket
    CHECK_EQ(env.as_root("PUT", "/shared").status, 200);
    env.admin(env.root, "PUT", "/-/admin/tenants/acme/buckets/shared");
    CHECK_EQ(env.tenants->owner_of("shared"), std::string("acme"));
    CHECK_EQ(env.admin(env.root, "PUT", "/-/admin/tenants/acme/buckets/missing", {}, {}, 404)["code"],
             std::string("NoSuchBucket"));
    // Delete refused while buckets are owned, then allowed
    CHECK_EQ(env.admin(env.root, "DELETE", "/-/admin/tenants/acme", {}, {}, 409)["code"],
             std::string("TenantNotEmpty"));
    env.admin(env.root, "DELETE", "/-/admin/tenants/acme/buckets/shared", {}, {}, 204);
    CHECK(env.tenants->owner_of("shared").empty());
    env.admin(env.root, "DELETE", "/-/admin/tenants/acme", {}, {}, 204);
    CHECK_EQ(env.admin(env.root, "GET", "/-/admin/tenants")["tenants"].size(), size_t(0));
}

TEST(tenant_isolation_on_the_data_plane) {
    Env env;
    env.admin(env.root, "POST", "/-/admin/tenants", {{"id", "t1"}, {"display_name", "Tenant One"}}, {}, 201);
    env.admin(env.root, "POST", "/-/admin/tenants", {{"id", "t2"}}, {}, 201);
    // Unknown tenant on a credential is refused
    CHECK_EQ(env.admin(env.root, "POST", "/-/admin/credentials", {{"tenant", "nope"}}, {}, 404)["code"],
             std::string("NoSuchTenant"));
    auto c1 = env.mint(env.root, {{"tenant", "t1"}});
    auto c2 = env.mint(env.root, {{"tenant", "t2"}});
    CHECK_EQ(env.as_root("PUT", "/legacy").status, 200);  // unowned

    // t1 creates a bucket -> owned by t1
    CHECK_EQ(env.call("PUT", "/bk1", c1).status, 200);
    CHECK_EQ(env.tenants->owner_of("bk1"), std::string("t1"));
    CHECK_EQ(env.call("PUT", "/bk1/k", c1, {}, "hello").status, 200);
    // t2: foreign bucket is forbidden, a missing bucket stays 404, unowned is forbidden
    auto r = env.call("GET", "/bk1/k", c2);
    CHECK_EQ(r.status, 403);
    CHECK(contains(r.small_body, "not owned by tenant"));
    CHECK_EQ(env.call("HEAD", "/missing", c2).status, 404);
    CHECK_EQ(env.call("PUT", "/legacy/k", c2, {}, "x").status, 403);
    // t2 cannot claim an existing unowned name via CreateBucket
    CHECK_EQ(env.call("PUT", "/legacy", c2).status, 409);
    CHECK(env.tenants->owner_of("legacy").empty());
    // Copy source is gated too
    CHECK_EQ(env.call("PUT", "/bk2", c2).status, 200);
    CHECK_EQ(env.call("PUT", "/bk2/cp", c2, {}, "", {{"x-amz-copy-source", "/bk1/k"}}).status, 403);
    // ListBuckets: each tenant sees its own, with its identity as Owner; root sees all
    auto l1 = env.call("GET", "/", c1);
    CHECK(contains(l1.small_body, "<Name>bk1</Name>"));
    CHECK(!contains(l1.small_body, "<Name>bk2</Name>"));
    CHECK(!contains(l1.small_body, "<Name>legacy</Name>"));
    CHECK_EQ(xelem(l1.small_body, "ID"), std::string("t1"));
    CHECK_EQ(xelem(l1.small_body, "DisplayName"), std::string("Tenant One"));
    auto lr = env.as_root("GET", "/");
    CHECK(contains(lr.small_body, "<Name>bk1</Name>") && contains(lr.small_body, "<Name>legacy</Name>"));
    CHECK_EQ(xelem(lr.small_body, "ID"), std::string("lights3"));
    // ListObjectsV2 fetch-owner names the bucket's tenant
    auto lo = env.as_root("GET", "/bk1", {{"list-type", "2"}, {"fetch-owner", "true"}});
    CHECK_EQ(xelem(lo.small_body, "DisplayName"), std::string("Tenant One"));
    // Root may still operate on tenant buckets; deleting one drops the ownership record
    CHECK_EQ(env.as_root("DELETE", "/bk1/k").status, 204);
    CHECK_EQ(env.as_root("DELETE", "/bk1").status, 204);
    CHECK(env.tenants->owner_of("bk1").empty());
    // Tenant credential survives a store reload with its tenant
    auto reloaded = sync_wait(CredentialStore::load(env.backend, env.acfg));
    auto info = reloaded->find(c2.access_key);
    CHECK(info && info->tenant == "t2" && !info->tenant_admin);
}

TEST(tenant_quota_aggregates_over_owned_buckets) {
    Env env;
    env.admin(env.root, "POST", "/-/admin/tenants",
              {{"id", "t"}, {"quota", {{"max_bytes", 10}, {"max_objects", 3}, {"max_buckets", 2}}}},
              {}, 201);
    auto c = env.mint(env.root, {{"tenant", "t"}});
    CHECK_EQ(env.call("PUT", "/bka", c).status, 200);
    CHECK_EQ(env.call("PUT", "/bkt", c).status, 200);
    auto third = env.call("PUT", "/bkc", c);
    CHECK_EQ(third.status, 403);
    CHECK(contains(third.small_body, "bucket limit"));
    CHECK_EQ(env.call("PUT", "/bka/1", c, {}, "123456").status, 200);
    // 6 + 6 > 10 across the two buckets
    auto over = env.call("PUT", "/bkt/1", c, {}, "123456");
    CHECK_EQ(over.status, 403);
    CHECK(contains(over.small_body, "tenant"));
    CHECK_EQ(env.call("PUT", "/bkt/1", c, {}, "1234").status, 200);
    CHECK_EQ(env.call("PUT", "/bkt/2", c, {}, "").status, 200);
    CHECK_EQ(env.call("PUT", "/bkt/3", c, {}, "").status, 403);  // 4th object
    auto tj = env.admin(env.root, "GET", "/-/admin/tenants/t");
    CHECK_EQ(tj["usage"]["bytes"].get<int64_t>(), int64_t(10));
    CHECK_EQ(tj["usage"]["objects"].get<int64_t>(), int64_t(3));
    CHECK_EQ(tj["buckets"].size(), size_t(2));
    CHECK_EQ(tj["credentials"].get<size_t>(), size_t(1));
}

TEST(tenant_admin_is_scoped_to_its_tenant) {
    Env env;
    env.admin(env.root, "POST", "/-/admin/tenants", {{"id", "t1"}}, {}, 201);
    env.admin(env.root, "POST", "/-/admin/tenants", {{"id", "t2"}}, {}, 201);
    // role needs a tenant; unknown role refused
    CHECK_EQ(env.admin(env.root, "POST", "/-/admin/credentials", {{"role", "admin"}}, {}, 400)["code"],
             std::string("InvalidRequest"));
    CHECK_EQ(env.admin(env.root, "POST", "/-/admin/credentials", {{"tenant", "t1"}, {"role", "boss"}}, {}, 400)["code"],
             std::string("InvalidRequest"));
    auto a1 = env.mint(env.root, {{"tenant", "t1"}, {"role", "admin"}});
    auto u2 = env.mint(env.root, {{"tenant", "t2"}});
    // Mints inside its own tenant only (tenant pinned server-side)
    auto u1 = env.mint(a1, {{"comment", "worker"}});
    CHECK_EQ(env.cred_store->find(u1.access_key)->tenant, std::string("t1"));
    CHECK_EQ(env.admin(a1, "POST", "/-/admin/credentials", {{"tenant", "t2"}}, {}, 403)["code"],
             std::string("AccessDenied"));
    // Sees only its tenant's credentials; foreign ones read as nonexistent
    auto list = env.admin(a1, "GET", "/-/admin/credentials")["credentials"];
    CHECK_EQ(list.size(), size_t(2));
    for (auto& c : list) CHECK_EQ(c["tenant"].get<std::string>(), std::string("t1"));
    CHECK_EQ(env.admin(a1, "GET", "/-/admin/credentials/" + u2.access_key, {}, {}, 403)["code"],
             std::string("InvalidAccessKeyId"));
    CHECK_EQ(env.admin(a1, "DELETE", "/-/admin/credentials/" + u2.access_key, {}, {}, 403)["code"],
             std::string("InvalidAccessKeyId"));
    CHECK_EQ(env.admin(a1, "PUT", "/-/admin/credentials/" + u1.access_key, {{"tenant", "t2"}}, {}, 403)["code"],
             std::string("AccessDenied"));
    env.admin(a1, "PUT", "/-/admin/credentials/" + u1.access_key, {{"comment", "renamed"}});
    env.admin(a1, "GET", "/-/admin/credentials/" + u1.access_key, {}, {{"show-secret", "true"}});
    env.admin(a1, "DELETE", "/-/admin/credentials/" + u1.access_key, {}, {}, 204);
    // Tenant plane: own tenant readable, others and mutations refused
    CHECK_EQ(env.admin(a1, "GET", "/-/admin/tenants")["tenants"].size(), size_t(1));
    env.admin(a1, "GET", "/-/admin/tenants/t1");
    CHECK_EQ(env.admin(a1, "GET", "/-/admin/tenants/t2", {}, {}, 403)["code"], std::string("AccessDenied"));
    CHECK_EQ(env.admin(a1, "POST", "/-/admin/tenants", {{"id", "t3"}}, {}, 403)["code"],
             std::string("AccessDenied"));
    CHECK_EQ(env.admin(a1, "PUT", "/-/admin/tenants/t1", {{"quota", {{"max_bytes", 1}}}}, {}, 403)["code"],
             std::string("AccessDenied"));
    // Usage plane: own buckets only, rescan allowed on them
    CHECK_EQ(env.call("PUT", "/own", a1).status, 200);
    CHECK_EQ(env.call("PUT", "/theirs", u2).status, 200);
    auto usage = env.admin(a1, "GET", "/-/admin/usage")["buckets"];
    CHECK_EQ(usage.size(), size_t(1));
    CHECK_EQ(usage[0]["bucket"].get<std::string>(), std::string("own"));
    env.admin(a1, "POST", "/-/admin/usage/own/rescan");
    CHECK_EQ(env.admin(a1, "GET", "/-/admin/usage/theirs", {}, {}, 403)["code"], std::string("AccessDenied"));
    // ?quota: a tenant admin still cannot set one (operator decision), but can read it
    CHECK_EQ(env.call("PUT", "/own", a1, {{"quota", ""}}, quota_xml({100, 0})).status, 403);
    CHECK_EQ(env.as_root("PUT", "/own", {{"quota", ""}}, quota_xml({100, 0})).status, 200);
    CHECK_EQ(env.call("GET", "/own", a1, {{"quota", ""}}).status, 200);
    // Root can move the credential and revoke the admin role
    auto moved = env.admin(env.root, "PUT", "/-/admin/credentials/" + a1.access_key,
                           {{"tenant", "t2"}, {"role", "user"}});
    CHECK_EQ(moved["tenant"].get<std::string>(), std::string("t2"));
    CHECK_EQ(moved["role"].get<std::string>(), std::string("user"));
    CHECK_EQ(env.call("GET", "/-/admin/tenants", a1).status, 403);
    CHECK_EQ(env.call("HEAD", "/theirs", a1).status, 200);
    auto detached = env.admin(env.root, "PUT", "/-/admin/credentials/" + a1.access_key, {{"tenant", nullptr}});
    CHECK(!detached.contains("tenant"));
    CHECK_EQ(env.call("HEAD", "/own", a1).status, 200);  // legacy credential again: sees everything
}

TEST(tenant_sessions_inherit_tenant_but_never_admin) {
    Env env;
    env.admin(env.root, "POST", "/-/admin/tenants", {{"id", "t1"}}, {}, 201);
    env.admin(env.root, "POST", "/-/admin/tenants", {{"id", "t2"}}, {}, 201);
    auto a1 = env.mint(env.root, {{"tenant", "t1"}, {"role", "admin"}});
    CHECK_EQ(env.call("PUT", "/mine", a1).status, 200);
    auto sc = sync_wait(env.cred_store->mint_session(a1.access_key, 900));
    Credential session{sc.access_key, sc.secret_key};
    std::vector<std::pair<std::string, std::string>> tok{{"x-amz-security-token", sc.token}};
    CHECK_EQ(env.call("PUT", "/mine/k", session, {}, "v", tok).status, 200);
    CHECK_EQ(env.call("PUT", "/other", session, {}, "", tok).status, 200);
    CHECK_EQ(env.tenants->owner_of("other"), std::string("t1"));
    CHECK_EQ(env.call("GET", "/-/admin/tenants", session, {}, "", tok).status, 403);
    CHECK_EQ(env.call("GET", "/-/admin/credentials", session, {}, "", tok).status, 403);
}

TEST(file_credentials_carry_tenant_and_role) {
    std::string path = temp_path("creds.json");
    {
        std::ofstream f(path);
        f << R"({"credentials": [{"access_key": "FILET1ADMIN", "secret_key": "s1", "tenant": "t1", "role": "admin"},
                                {"access_key": "FILEPLAIN", "secret_key": "s2"}]})";
    }
    AuthConfig cfg = root_cfg();
    cfg.credentials_file = path;
    Env env(cfg);
    auto a = env.cred_store->find("FILET1ADMIN");
    CHECK(a && a->tenant == "t1" && a->tenant_admin);
    auto p = env.cred_store->find("FILEPLAIN");
    CHECK(p && p->tenant.empty() && !p->tenant_admin);
    // A bad role in the file is a startup error, not a silent downgrade
    {
        std::ofstream f(path);
        f << R"({"credentials": [{"access_key": "X", "secret_key": "s", "tenant": "t1", "role": "root"}]})";
    }
    bool threw = false;
    try {
        (void)sync_wait(CredentialStore::load(std::make_shared<storage::MemoryBackend>(), cfg));
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
    std::filesystem::remove(path);
}

TEST(lifecycle_expiry_adjusts_usage) {
    Env env;
    CHECK_EQ(env.as_root("PUT", "/bkt").status, 200);
    CHECK_EQ(env.as_root("PUT", "/bkt/old", {}, "0123456789").status, 200);
    auto init = env.as_root("POST", "/bkt/mp", {{"uploads", ""}});
    std::string id = xelem(init.small_body, "UploadId");
    CHECK_EQ(env.as_root("PUT", "/bkt/mp", {{"partNumber", "1"}, {"uploadId", id}}, "abc").status, 200);
    CHECK_EQ(env.usage_of("bkt").mpu_bytes, int64_t(3));
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends{{"mem", env.backend}};
    BucketsConfig bcfg;
    bcfg.default_backend = "mem";
    auto store = sync_wait(LifecycleStore::load(env.backend));
    sync_wait(store->put("bkt", {LifecycleRule{"r", true, "", 1, 1}}));
    LifecycleRunner runner(storage::BucketRouter::build(bcfg, backends), store);
    runner.set_usage_tracker(env.usage);
    runner.set_now_for_tests([] { return std::chrono::system_clock::now() + std::chrono::hours(72); });
    auto stats = sync_wait(runner.run_once());
    CHECK_EQ(stats.objects_expired, uint64_t(1));
    CHECK_EQ(stats.uploads_aborted, uint64_t(1));
    auto u = env.usage_of("bkt");
    CHECK_EQ(u.objects, int64_t(0));
    CHECK_EQ(u.bytes, int64_t(0));
    CHECK_EQ(u.mpu_bytes, int64_t(0));
}

// ---------- ④ audit ----------

TEST(audit_log_records_control_and_data_plane) {
    std::string path = temp_path("audit.log");
    std::filesystem::remove(path);
    AuditConfig acfg;
    acfg.path = path;
    acfg.data_plane = true;
    {
        Env env(root_cfg(), {}, acfg);
        env.admin(env.root, "POST", "/-/admin/tenants", {{"id", "t1"}}, {}, 201);
        auto c = env.mint(env.root, {{"tenant", "t1"}, {"comment", "w"}});
        CHECK_EQ(env.call("PUT", "/bkt", c).status, 200);
        CHECK_EQ(env.as_root("PUT", "/bkt", {{"quota", ""}}, quota_xml({3, 0})).status, 200);
        CHECK_EQ(env.call("PUT", "/bkt/k", c, {}, "toolong").status, 403);
        env.admin(env.root, "GET", "/-/admin/credentials/" + c.access_key, {}, {{"show-secret", "true"}});
        env.admin(env.root, "DELETE", "/-/admin/credentials/" + c.access_key, {}, {}, 204);
        env.audit->flush();
    }
    std::ifstream in(path);
    std::vector<json> lines;
    for (std::string line; std::getline(in, line);) lines.push_back(json::parse(line));
    auto count = [&](const char* ev) {
        size_t n = 0;
        for (auto& j : lines)
            if (j["event"] == ev) ++n;
        return n;
    };
    CHECK_EQ(count("tenant.create"), size_t(1));
    CHECK_EQ(count("cred.create"), size_t(1));
    CHECK_EQ(count("bucket.create"), size_t(1));
    CHECK_EQ(count("quota.set"), size_t(1));
    CHECK_EQ(count("quota.reject"), size_t(1));
    CHECK_EQ(count("cred.show_secret"), size_t(1));
    CHECK_EQ(count("cred.delete"), size_t(1));
    CHECK(count("access") >= 6);
    bool saw_reject = false, saw_access = false;
    for (auto& j : lines) {
        CHECK(j.contains("ts"));
        if (j["event"] == "quota.reject") {
            saw_reject = true;
            CHECK_EQ(j["tenant"].get<std::string>(), std::string("t1"));
            CHECK_EQ(j["bucket"].get<std::string>(), std::string("bkt"));
            CHECK(j.contains("request_id"));
        }
        if (j["event"] == "access" && j.value("path", "") == "/bkt/k") {
            saw_access = true;
            CHECK_EQ(j["status"].get<int>(), 403);
            CHECK_EQ(j["method"].get<std::string>(), std::string("PUT"));
            CHECK_EQ(j["tenant"].get<std::string>(), std::string("t1"));
        }
    }
    CHECK(saw_reject && saw_access);
    std::filesystem::remove(path);
}

// ---------- config ----------

TEST(config_usage_and_audit_sections) {
    std::string base = "backends:\n  - name: m\n    type: memory\n";
    auto cfg = Config::from_string(base);
    CHECK(cfg.usage.enabled);
    CHECK_EQ(cfg.usage.flush_interval_sec, 60);
    CHECK_EQ(cfg.usage.reconcile_interval_sec, 86400);
    CHECK(cfg.usage.reconcile);
    CHECK(cfg.audit.path.empty());
    cfg = Config::from_string(base +
                              "usage:\n  enabled: false\n  flush_interval: 5m\n  reconcile_interval: 0s\n  reconcile: false\n"
                              "audit:\n  path: /tmp/a.log\n  data_plane: true\n  max_size: 1MiB\n  max_files: 3\n");
    CHECK(!cfg.usage.enabled);
    CHECK_EQ(cfg.usage.flush_interval_sec, 300);
    CHECK_EQ(cfg.usage.reconcile_interval_sec, 0);
    CHECK(!cfg.usage.reconcile);
    CHECK_EQ(cfg.audit.path, std::string("/tmp/a.log"));
    CHECK(cfg.audit.data_plane);
    CHECK_EQ(cfg.audit.max_size, uint64_t(1024 * 1024));
    CHECK_EQ(cfg.audit.max_files, 3);
    auto rejects = [&](const std::string& extra) {
        try {
            Config::from_string(base + extra);
        } catch (const std::runtime_error&) {
            return true;
        }
        return false;
    };
    CHECK(rejects("usage:\n  reconcile_interval: 30d\n"));
    CHECK(rejects("audit:\n  data_plane: true\n"));
    CHECK(rejects("audit:\n  path: /tmp/a\n  max_files: 0\n"));
    CHECK(rejects("audit:\n  path: /tmp/a\n  max_size: 1KiB\n"));
}

TEST(usage_metrics_render_and_forget) {
    // Per-bucket callback gauges + rejection counters through the shared registry;
    // rendering locks the registry then the tracker, registration must never
    // take them in the opposite order (exercised here under the same registry)
    auto reg = std::make_shared<MetricsRegistry>();
    Env env;
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends{{"mem", env.backend}};
    BucketsConfig bcfg;
    bcfg.default_backend = "mem";
    auto tracker = sync_wait(UsageTracker::load(storage::BucketRouter::build(bcfg, backends), {}, reg));
    tracker->apply("bkt", 2, 10);
    tracker->quota_rejected(false);
    tracker->quota_rejected(true);
    std::string out = reg->render();
    CHECK(contains(out, "lights3_bucket_usage_bytes{bucket=\"bkt\"} 10"));
    CHECK(contains(out, "lights3_bucket_usage_objects{bucket=\"bkt\"} 2"));
    CHECK(contains(out, "lights3_quota_rejections_total{scope=\"bucket\"} 1"));
    CHECK(contains(out, "lights3_quota_rejections_total{scope=\"tenant\"} 1"));
    sync_wait(tracker->remove("bkt"));
    out = reg->render();
    CHECK(!contains(out, "bucket=\"bkt\""));
    tracker->shutdown_background();
}
