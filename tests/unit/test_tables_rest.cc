// Iceberg REST surface through the full S3Service dispatch (docs/s3-tables-design.md §6, §8.1,
// §13): endpoints, error model, guard, config drift
#include <chrono>
#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>
#include <set>
#include <thread>

#include "app/admin_jobs.h"
#include "core/fault.h"
#include "core/util/checksum.h"
#include "core/util/crypto.h"
#include "core/util/uri.h"
#include "s3/audit.h"
#include "s3/auth/credential_store.h"
#include "s3/lifecycle.h"
#include "s3/service.h"
#include "s3/tenant.h"
#include "storage/memory/memory_backend.h"
#include "tables/bucket_guard.h"
#include "tables/catalog.h"
#include "tables/object_catalog_store.h"
#include "tables/rest_api.h"
#include "unit/mini_test.h"
#include "unit/tables_fixtures.h"

using namespace lights3;
using namespace lights3::s3;
using nlohmann::json;

namespace {

struct TablesEnv {
    std::shared_ptr<storage::MemoryBackend> backend = std::make_shared<storage::MemoryBackend>();
    AuthConfig acfg;
    SigV4Authenticator auth;
    std::shared_ptr<CredentialStore> cred_store;
    std::shared_ptr<tables::TableBucketStore> buckets;
    std::shared_ptr<tables::Catalog> catalog;
    std::shared_ptr<tables::RestApi> api;
    std::shared_ptr<tables::TableBucketGuard> guard;
    std::unique_ptr<AdminJobs> jobs;
    std::unique_ptr<S3Service> svc;
    TablesConfig cfg;

    static AuthConfig root_acfg() {
        AuthConfig a;
        a.credentials = {{"ROOTAK", "root-sk"}};
        return a;
    }

    struct Options {
        bool enabled = true;
        bool vending = false;
        bool accept_s3tables = true;
        bool tenants = false;
        // step ⑥: the /_iceberg alias and an audit log file for reportMetrics
        std::string compat_prefix{};
        std::string audit_path{};
    };
    std::shared_ptr<AuditLog> audit;
    std::shared_ptr<TenantStore> tenant_store;
    std::shared_ptr<OwnerStore> owner_store;
    std::shared_ptr<TenantRegistry> tenants;

    explicit TablesEnv(bool enabled = true) : TablesEnv(Options{enabled}) {}
    explicit TablesEnv(Options o) : acfg(root_acfg()), auth(SigV4Authenticator::build(acfg)) {
        bool enabled = o.enabled;
        cfg.enabled = enabled;
        cfg.credential_vending = o.vending;
        cfg.accept_s3tables_signing = o.accept_s3tables;
        cfg.compat_prefix = o.compat_prefix;
        cred_store = sync_wait(CredentialStore::load(backend, acfg));
        auth.set_provider(cred_store);
        std::map<std::string, std::shared_ptr<storage::IStorageBackend>> bmap{{"mem", backend}};
        BucketsConfig bcfg;
        bcfg.default_backend = "mem";
        auto router = storage::BucketRouter::build(bcfg, std::move(bmap));
        svc = std::make_unique<S3Service>(router, auth);
        svc->set_credential_store(cred_store);
        if (!o.audit_path.empty()) {
            AuditConfig acfg2;
            acfg2.path = o.audit_path;
            audit = AuditLog::open(acfg2);
            svc->set_audit_log(audit);
        }
        if (o.tenants) {
            tenant_store = sync_wait(TenantStore::load(backend));
            owner_store = sync_wait(OwnerStore::load(backend));
            tenants = std::make_shared<TenantRegistry>(tenant_store, owner_store);
            svc->set_tenant_registry(tenants);
        }
        if (enabled) {
            buckets = sync_wait(tables::TableBucketStore::load(backend));
            auto store = std::make_shared<tables::ObjectCatalogStore>(backend);
            catalog = std::make_shared<tables::Catalog>(store, buckets, router, nullptr, cfg, MetricsScope{});
            api = std::make_shared<tables::RestApi>(catalog, cfg, MetricsScope{});
            guard = std::make_shared<tables::TableBucketGuard>(buckets, catalog, cfg.path_prefix, cfg.compat_prefix);
            svc->set_tables(api, guard);
            // the job framework the application installs (step ④ §5)
            jobs = std::make_unique<AdminJobs>(std::map<std::string, std::shared_ptr<storage::IStorageBackend>>{});
            tables::JobHooks jh;
            jh.start = [this](const std::string& resource, const std::string& op, tables::JobHooks::Fn fn) {
                auto o = parse_job_op("tables", op);
                if (!o) throw S3Error(S3ErrorCode::InvalidRequest, "no operation");
                try {
                    uint64_t id = jobs->start_custom(resource, *o, [fn] {
                        JobOutcome out;
                        out.kind = "tables";
                        out.stats = fn();
                        return out;
                    });
                    json j = jobs->status(resource, *o);
                    j["job_id"] = id;
                    j["running"] = true;
                    return j;
                } catch (const AdminJobs::Failure& f) {
                    throw S3Error(S3ErrorCode::JobInProgress, f.message);
                }
            };
            jh.status = [this](const std::string& resource, const std::string& op) {
                return jobs->status(resource, *parse_job_op("tables", op));
            };
            jh.status_by_id = [this](uint64_t id) { return jobs->status_by_id(id); };
            api->set_job_hooks(jh);
        }
    }

    http::HttpRequest make_req(std::string method, std::string raw_path, std::string body,
                               std::vector<std::pair<std::string, std::string>> query,
                               std::vector<std::pair<std::string, std::string>> headers) {
        http::HttpRequest req;
        req.method = std::move(method);
        req.raw_path = raw_path;
        req.path = util::percent_decode(raw_path);
        req.query = query;
        for (auto& [k, v] : query) {
            if (!req.raw_query.empty()) req.raw_query += "&";
            req.raw_query += k + "=" + util::aws_uri_encode(v, true);
        }
        req.headers.add("Host", "localhost");
        req.headers.add("Content-Length", std::to_string(body.size()));
        for (auto& [k, v] : headers) req.headers.add(k, v);
        if (!body.empty()) req.body = std::make_unique<http::StringBodyReader>(std::move(body));
        return req;
    }
    http::HttpResponse call_as(const Credential& cred, std::string method, std::string raw_path, std::string body = "",
                               std::vector<std::pair<std::string, std::string>> query = {},
                               std::vector<std::pair<std::string, std::string>> headers = {},
                               std::string_view service = {}) {
        auto r = make_req(std::move(method), std::move(raw_path), body, std::move(query), std::move(headers));
        auth.sign(r, cred, body.empty() ? "" : util::sha256_hex(body), service);
        return sync_wait(svc->dispatch(std::move(r)));
    }
    http::HttpResponse call(std::string method, std::string raw_path, std::string body = "",
                            std::vector<std::pair<std::string, std::string>> query = {},
                            std::vector<std::pair<std::string, std::string>> headers = {}, size_t cred = 0,
                            std::string_view service = {}) {
        return call_as(acfg.credentials[cred], std::move(method), std::move(raw_path), std::move(body),
                       std::move(query), std::move(headers), service);
    }
    http::HttpResponse anon(std::string method, std::string raw_path) {
        auto r = make_req(std::move(method), std::move(raw_path), "", {}, {});
        return sync_wait(svc->dispatch(std::move(r)));
    }
    static json body_json(const http::HttpResponse& r) { return json::parse(r.small_body); }
    // fixture "1" (snapshot 1: f1 100 B, f2 200 B) with its manifest list at ml_key of bucket tbk
    void install_snapshot(const std::string& ml_key) {
        CHECK_EQ(call("PUT", "/tbk/" + ml_key, tables_fixtures::slurp("ml-1.avro")).status, 200);
        CHECK_EQ(call("PUT", "/tbk/n/t/metadata/m-1.avro", tables_fixtures::slurp("m-1.avro")).status, 200);
        CHECK_EQ(call("PUT", "/tbk/n/t/data/f1.parquet", std::string(100, 'a')).status, 200);
        CHECK_EQ(call("PUT", "/tbk/n/t/data/f2.parquet", std::string(200, 'b')).status, 200);
    }
    static std::string error_type(const http::HttpResponse& r) { return body_json(r)["error"]["type"]; }
    // adds a dynamic credential with a policy; returns its index in acfg.credentials
    size_t add_policy_cred(const std::string& comment, const std::string& policy_json, const std::string& tenant = "") {
        auto info = sync_wait(cred_store->generate(comment, parse_policy_json(policy_json), tenant));
        Credential c;
        c.access_key = info.access_key;
        c.secret_key = info.secret_key;
        acfg.credentials.push_back(std::move(c));
        return acfg.credentials.size() - 1;
    }
};

const char* kSchema = R"({"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]})";

std::string create_body(const std::string& name) {
    return std::string(R"({"name":")") + name + R"(","schema":)" + kSchema + "}";
}

std::string append_body(int64_t snap, int64_t seq, const std::string& manifest, const std::string& commit_id = "") {
    json s;
    s["snapshot-id"] = snap;
    s["sequence-number"] = seq;
    s["timestamp-ms"] = 1000 + snap;
    s["manifest-list"] = manifest;
    s["summary"] = json::object({{"operation", "append"}});
    json body;
    if (!commit_id.empty()) body["commit-id"] = commit_id;
    body["requirements"] = json::array();
    body["updates"] = json::array(
        {json::object({{"action", "add-snapshot"}, {"snapshot", s}}),
         json::parse(R"({"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":)" +
                     std::to_string(snap) + "}")});
    return body.dump();
}

}  // namespace

TEST(tables_rest_config_endpoints_match_route_table) {
    TablesEnv env;
    auto r = env.call("GET", "/iceberg/v1/config", "", {{"warehouse", "tbk"}});
    CHECK_EQ(r.status, 200);
    auto j = TablesEnv::body_json(r);
    CHECK_EQ(j["overrides"]["prefix"].get<std::string>(), "tbk");
    CHECK_EQ(j["overrides"]["namespace-separator"].get<std::string>(), "%1F");
    std::set<std::string> advertised;
    for (auto& e : j["endpoints"]) advertised.insert(e.get<std::string>());
    // every standard route is advertised, and only those
    std::set<std::string> from_table;
    for (auto& e : tables::RestApi::advertised_endpoints()) from_table.insert(e);
    CHECK(advertised == from_table);
    size_t standard = 0;
    for (auto& rt : tables::RestApi::routes())
        if (rt.standard) ++standard;
    CHECK_EQ(advertised.size(), standard);
    CHECK(advertised.count("POST /v1/{prefix}/namespaces/{namespace}/tables/{table}") == 1);
    CHECK(advertised.count("GET /v1/{prefix}/namespaces/{namespace}/tables/{table}/metadata-location") == 0);
    // no warehouse -> no prefix override; empty warehouse -> 400
    CHECK(!TablesEnv::body_json(env.call("GET", "/iceberg/v1/config"))["overrides"].contains("prefix"));
    CHECK_EQ(env.call("GET", "/iceberg/v1/config", "", {{"warehouse", ""}}).status, 400);
}

TEST(tables_rest_full_flow_and_error_model) {
    TablesEnv env;
    CHECK_EQ(env.call("PUT", "/tbk").status, 200);
    // catalog on a bucket that is not table-enabled
    auto r0 = env.call("GET", "/iceberg/v1/tbk/namespaces");
    CHECK_EQ(r0.status, 404);
    CHECK_EQ(TablesEnv::error_type(r0), "NoSuchNamespaceException");
    auto en = env.call("PUT", "/iceberg/v1/buckets/tbk");
    CHECK_EQ(en.status, 200);
    CHECK_EQ(TablesEnv::body_json(en)["reserved-prefix"].get<std::string>(), ".lights3-table/");
    CHECK_EQ(env.call("GET", "/iceberg/v1/buckets/tbk").status, 200);
    CHECK_EQ(env.call("PUT", "/iceberg/v1/buckets/nobucket").status, 404);
    // namespaces
    auto ns = env.call("POST", "/iceberg/v1/tbk/namespaces", R"({"namespace":["sales","eu"],"properties":{"o":"me"}})");
    CHECK_EQ(ns.status, 200);
    CHECK_EQ(TablesEnv::body_json(ns)["namespace"][1].get<std::string>(), "eu");
    auto dup = env.call("POST", "/iceberg/v1/tbk/namespaces", R"({"namespace":["sales","eu"]})");
    CHECK_EQ(dup.status, 409);
    CHECK_EQ(TablesEnv::error_type(dup), "AlreadyExistsException");
    CHECK_EQ(env.call("HEAD", "/iceberg/v1/tbk/namespaces/sales%1Feu").status, 204);
    CHECK_EQ(env.call("HEAD", "/iceberg/v1/tbk/namespaces/sales").status, 204);
    auto nf = env.call("HEAD", "/iceberg/v1/tbk/namespaces/nope");
    CHECK_EQ(nf.status, 404);
    CHECK(nf.small_body.empty());
    auto lst = env.call("GET", "/iceberg/v1/tbk/namespaces", "", {{"parent", "sales"}});
    CHECK_EQ(TablesEnv::body_json(lst)["namespaces"][0][1].get<std::string>(), "eu");
    CHECK(TablesEnv::body_json(lst)["next-page-token"].is_null());
    auto props = env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/properties",
                          R"({"removals":["o"],"updates":{"x":"1"}})");
    CHECK_EQ(props.status, 200);
    CHECK_EQ(TablesEnv::body_json(props)["removed"][0].get<std::string>(), "o");
    auto both = env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/properties",
                         R"({"removals":["x"],"updates":{"x":"1"}})");
    CHECK_EQ(both.status, 422);
    // tables
    auto ct = env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables", create_body("orders"));
    CHECK_EQ(ct.status, 200);
    auto ctj = TablesEnv::body_json(ct);
    std::string ml1 = ctj["metadata-location"];
    CHECK_EQ(ml1.rfind("s3://tbk/.lights3-table/sales/eu/orders/metadata/00001-", 0), size_t(0));
    CHECK_EQ(ctj["config"]["s3.path-style-access"].get<std::string>(), "true");
    CHECK(ct.headers.has("ETag"));
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables", create_body("orders")).status, 409);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables",
                      R"({"name":"s","stage-create":true,"schema":{"type":"struct","fields":[]}})")
                 .status,
             406);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables", R"({"name":"x"})").status, 400);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables", "not json").status, 400);
    CHECK_EQ(env.call("HEAD", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders").status, 204);
    auto lt = env.call("GET", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables");
    CHECK_EQ(TablesEnv::body_json(lt)["identifiers"][0]["name"].get<std::string>(), "orders");
    // the metadata file is readable, but not writable, through the S3 plane
    std::string key1 = ml1.substr(std::string("s3://tbk/").size());
    CHECK_EQ(env.call("GET", "/tbk/" + key1).status, 200);
    auto blocked = env.call("PUT", "/tbk/" + key1, "{}");
    CHECK_EQ(blocked.status, 400);
    CHECK(blocked.small_body.find("InvalidRequest") != std::string::npos);
    CHECK_EQ(env.call("DELETE", "/tbk/" + key1).status, 400);
    CHECK_EQ(env.call("PUT", "/tbk/.lights3-table", "x").status, 400);
    CHECK_EQ(env.call("PUT", "/tbk/.lights3-tablex", "x").status, 200);
    CHECK_EQ(env.call("POST", "/tbk/.lights3-table/mpu", "", {{"uploads", ""}}).status, 400);
    CHECK_EQ(env.call("PUT", "/tbk/other", "y", {}, {{"x-amz-copy-source", "/tbk/" + key1}}).status, 200);
    CHECK_EQ(env.call("PUT", "/tbk/.lights3-table/copy", "", {}, {{"x-amz-copy-source", "/tbk/other"}}).status, 400);
    // commit: a PyIceberg-written manifest list, its manifest and data files (step ③ walks them)
    env.install_snapshot("sales/eu/orders/metadata/snap-1.avro");
    auto cm = env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders",
                       append_body(1, 1, "s3://tbk/sales/eu/orders/metadata/snap-1.avro", "c-1"));
    CHECK_EQ(cm.status, 200);
    auto cmj = TablesEnv::body_json(cm);
    CHECK_EQ(cmj["generation"].get<int>(), 2);
    CHECK_EQ(cmj["metadata"]["current-snapshot-id"].get<int>(), 1);
    auto replay = env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders",
                           append_body(1, 1, "s3://tbk/sales/eu/orders/metadata/snap-1.avro", "c-1"));
    CHECK_EQ(replay.status, 200);
    CHECK_EQ(TablesEnv::body_json(replay)["generation"].get<int>(), 2);
    auto stale = env.call(
        "POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders",
        R"({"requirements":[{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null}],"updates":[]})");
    CHECK_EQ(stale.status, 409);
    CHECK_EQ(TablesEnv::error_type(stale), "CommitFailedException");
    auto badid = env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders",
                          R"({"identifier":{"namespace":["sales"],"name":"orders"},"requirements":[],"updates":[]})");
    CHECK_EQ(badid.status, 400);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders",
                      R"({"requirements":[],"updates":[{"action":"add-encryption-key"}]})")
                 .status,
             406);
    // load / snapshots=refs / metadata-location
    auto ld = env.call("GET", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders", "", {{"snapshots", "refs"}});
    CHECK_EQ(ld.status, 200);
    CHECK_EQ(TablesEnv::body_json(ld)["metadata"]["snapshots"].size(), size_t(1));
    CHECK_EQ(env.call("GET", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders", "", {{"snapshots", "some"}}).status,
             400);
    auto mlr = env.call("GET", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders/metadata-location");
    CHECK_EQ(mlr.status, 200);
    std::string token = TablesEnv::body_json(mlr)["versionToken"];
    std::string ml2 = TablesEnv::body_json(mlr)["metadataLocation"];
    auto wrong = env.call("PUT", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders/metadata-location",
                          json::object({{"metadataLocation", ml2}, {"versionToken", "t-wrong"}}).dump());
    CHECK_EQ(wrong.status, 409);
    auto same = env.call("PUT", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders/metadata-location",
                         json::object({{"metadataLocation", ml2}, {"versionToken", token}}).dump());
    CHECK_EQ(same.status, 200);
    // metrics report is accepted and discarded
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders/metrics",
                      R"({"report-type":"scan-report"})")
                 .status,
             204);
    // rename / drop / namespace drop
    CHECK_EQ(
        env.call(
               "POST", "/iceberg/v1/tbk/tables/rename",
               R"({"source":{"namespace":["sales","eu"],"name":"orders"},"destination":{"namespace":["sales","eu"],"name":"orders2"}})")
            .status,
        204);
    CHECK_EQ(env.call("HEAD", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders").status, 404);
    CHECK_EQ(env.call("HEAD", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders2").status, 204);
    // purgeRequested=true is a drop plus a purge job (step ④, tested in
    // tables_rest_maintenance_endpoints); the flag itself is validated
    auto purge = env.call("DELETE", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders2", "",
                          {{"purgeRequested", "maybe"}});
    CHECK_EQ(purge.status, 400);
    CHECK_EQ(TablesEnv::error_type(purge), "BadRequestException");
    CHECK_EQ(env.call("DELETE", "/tbk").status, 409);
    CHECK_EQ(
        env.call("DELETE", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders2", "", {{"purgeRequested", "false"}})
            .status,
        204);
    auto gone = env.call("GET", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders2");
    CHECK_EQ(gone.status, 404);
    CHECK_EQ(TablesEnv::error_type(gone), "NoSuchTableException");
    CHECK_EQ(env.call("DELETE", "/iceberg/v1/tbk/namespaces/sales%1Feu").status, 204);
    auto nsgone = env.call("GET", "/iceberg/v1/tbk/namespaces/sales%1Feu");
    CHECK_EQ(nsgone.status, 404);
    CHECK_EQ(TablesEnv::error_type(nsgone), "NoSuchNamespaceException");
    // unknown resource / wrong method
    auto unk = env.call("GET", "/iceberg/v1/tbk/whatever");
    CHECK_EQ(unk.status, 404);
    CHECK_EQ(TablesEnv::error_type(unk), "NoSuchResourceException");
    CHECK_EQ(env.call("PATCH", "/iceberg/v1/tbk/namespaces").status, 405);
    // the bucket cannot be deleted while catalog objects remain, then can
    CHECK_EQ(env.call("DELETE", "/iceberg/v1/tbk/namespaces/sales").status, 404);
    CHECK_EQ(env.call("DELETE", "/tbk").status, 409);
    CHECK_EQ(env.call("DELETE", "/tbk/sales/eu/orders/metadata/snap-1.avro").status, 204);
    CHECK_EQ(env.call("DELETE", "/tbk/other").status, 204);
    CHECK_EQ(env.call("DELETE", "/tbk/.lights3-tablex").status, 204);
    // reserved objects remain: DeleteBucket refuses until the catalog is disabled
    CHECK_EQ(env.call("DELETE", "/tbk").status, 409);
    CHECK_EQ(env.call("DELETE", "/iceberg/v1/buckets/tbk").status, 204);
    CHECK_EQ(env.call("DELETE", "/tbk/" + key1).status, 204);
}

TEST(tables_rest_delete_objects_per_key_and_bucket_lifecycle) {
    TablesEnv env;
    CHECK_EQ(env.call("PUT", "/tbk").status, 200);
    CHECK_EQ(env.call("PUT", "/iceberg/v1/buckets/tbk").status, 200);
    CHECK_EQ(env.call("PUT", "/tbk/plain", "x").status, 200);
    std::string body = "<Delete><Object><Key>plain</Key></Object><Object><Key>.lights3-table/x</Key></Object></Delete>";
    util::HashStream h(util::HashStream::Algo::Md5);
    h.update(std::span(reinterpret_cast<const uint8_t*>(body.data()), body.size()));
    auto d = h.final_bytes();
    auto r = env.call("POST", "/tbk", body, {{"delete", ""}},
                      {{"Content-MD5", util::base64_encode(std::span(d.data(), d.size()))}});
    CHECK_EQ(r.status, 200);
    CHECK(r.small_body.find("<Deleted><Key>plain</Key>") != std::string::npos);
    CHECK(r.small_body.find("<Error><Key>.lights3-table/x</Key><Code>InvalidRequest</Code>") != std::string::npos);
    // an empty table bucket can be deleted and its marker goes with it
    CHECK_EQ(env.call("DELETE", "/tbk").status, 204);
    CHECK(env.catalog->table_bucket(env.buckets->snapshot(), "tbk") == nullptr);
    // the catalog prefix's first segment is not a legal bucket name
    auto ib = env.call("PUT", "/iceberg");
    CHECK_EQ(ib.status, 400);
    CHECK(ib.small_body.find("InvalidBucketName") != std::string::npos);
    // catalog requests need a signature
    CHECK_EQ(env.anon("GET", "/iceberg/v1/config").status, 403);
    // pagination tokens are bound to their list operation
    CHECK_EQ(env.call("PUT", "/tbk").status, 200);
    CHECK_EQ(env.call("PUT", "/iceberg/v1/buckets/tbk").status, 200);
    for (const char* n : {"a", "b", "c"})
        CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces", std::string(R"({"namespace":[")") + n + "\"]}").status,
                 200);
    auto p1 = TablesEnv::body_json(env.call("GET", "/iceberg/v1/tbk/namespaces", "", {{"pageSize", "2"}}));
    CHECK_EQ(p1["namespaces"].size(), size_t(2));
    std::string tok = p1["next-page-token"];
    auto p2 = TablesEnv::body_json(
        env.call("GET", "/iceberg/v1/tbk/namespaces", "", {{"pageSize", "2"}, {"pageToken", tok}}));
    CHECK_EQ(p2["namespaces"].size(), size_t(1));
    CHECK_EQ(p2["namespaces"][0][0].get<std::string>(), "c");
    CHECK(p2["next-page-token"].is_null());
    CHECK_EQ(env.call("GET", "/iceberg/v1/tbk/namespaces/a/tables", "", {{"pageToken", tok}}).status, 400);
    CHECK_EQ(env.call("GET", "/iceberg/v1/tbk/namespaces", "", {{"pageSize", "0"}}).status, 400);
    CHECK_EQ(env.call("GET", "/iceberg/v1/tbk/namespaces", "", {{"pageToken", "!!!"}}).status, 400);
}

TEST(tables_rest_policy_and_root_gates) {
    TablesEnv env;
    CHECK_EQ(env.call("PUT", "/tbk").status, 200);
    CHECK_EQ(env.call("PUT", "/iceberg/v1/buckets/tbk").status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces", R"({"namespace":["sales"]})").status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces", R"({"namespace":["hr"]})").status, 200);
    const size_t scoped = env.add_policy_cred("scoped", R"({"buckets":["tbk"],"prefixes":["sales/"]})");
    const size_t ro = env.add_policy_cred("ro", R"({"buckets":["tbk"],"readonly":true})");
    // enabling / disabling table buckets is root-only
    auto en = env.call("PUT", "/iceberg/v1/buckets/tbk", "", {}, {}, scoped);
    CHECK_EQ(en.status, 403);
    CHECK_EQ(TablesEnv::error_type(en), "ForbiddenException");
    // prefix-scoped: sales yes, hr no
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables", create_body("t"), {}, {}, scoped).status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/hr/tables", create_body("t"), {}, {}, scoped).status, 403);
    CHECK_EQ(env.call("GET", "/iceberg/v1/tbk/namespaces/hr", "", {}, {}, scoped).status, 403);
    CHECK_EQ(env.call("GET", "/iceberg/v1/tbk/namespaces/sales/tables/t", "", {}, {}, scoped).status, 200);
    // read-only: load yes, create no, commit no, drop no
    CHECK_EQ(env.call("GET", "/iceberg/v1/tbk/namespaces/sales/tables/t", "", {}, {}, ro).status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables", create_body("u"), {}, {}, ro).status, 403);
    CHECK_EQ(
        env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables/t", R"({"requirements":[],"updates":[]})", {}, {}, ro)
            .status,
        403);
    CHECK_EQ(env.call("DELETE", "/iceberg/v1/tbk/namespaces/sales/tables/t", "", {}, {}, ro).status, 403);
    // rename needs delete on the source and write on the destination
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/tables/rename",
                      R"({"source":{"namespace":["sales"],"name":"t"},"destination":{"namespace":["hr"],"name":"t"}})",
                      {}, {}, scoped)
                 .status,
             403);
    CHECK_EQ(
        env.call("POST", "/iceberg/v1/tbk/tables/rename",
                 R"({"source":{"namespace":["sales"],"name":"t"},"destination":{"namespace":["sales"],"name":"t2"}})",
                 {}, {}, scoped)
            .status,
        204);
}

TEST(tables_rest_disabled_leaves_s3_plane_untouched) {
    TablesEnv env(/*enabled=*/false);
    // no catalog: the path is an ordinary S3 bucket "iceberg"
    auto r = env.call("GET", "/iceberg/v1/config");
    CHECK_EQ(r.status, 404);
    CHECK(r.small_body.find("NoSuchBucket") != std::string::npos);
    CHECK_EQ(env.call("PUT", "/iceberg").status, 200);
    CHECK_EQ(env.call("PUT", "/iceberg/v1/config", "x").status, 200);
    CHECK_EQ(env.call("PUT", "/tbk").status, 200);
    CHECK_EQ(env.call("PUT", "/tbk/.lights3-table/x", "x").status, 200);
}

// ---- step ②: policy-filtered listings, s3tables signing, vending, tenants, lifecycle ----

TEST(tables_rest_listing_filtered_by_policy) {
    TablesEnv env;
    CHECK_EQ(env.call("PUT", "/tbk").status, 200);
    CHECK_EQ(env.call("PUT", "/iceberg/v1/buckets/tbk").status, 200);
    for (const char* n : {"sales", "hr"})
        CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces", std::string(R"({"namespace":[")") + n + "\"]}").status,
                 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces", R"({"namespace":["sales","eu"]})").status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables", create_body("orders")).status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables", create_body("secret")).status, 200);
    const size_t scoped = env.add_policy_cred("scoped",
                                              R"({"buckets":["tbk"],"prefixes":["sales/orders","sales/eu/"]})");
    // top level: "sales" may contain allowlisted keys, "hr" may not
    auto ns = TablesEnv::body_json(env.call("GET", "/iceberg/v1/tbk/namespaces", "", {}, {}, scoped));
    CHECK_EQ(ns["namespaces"].size(), size_t(1));
    CHECK_EQ(ns["namespaces"][0][0].get<std::string>(), "sales");
    auto kids = TablesEnv::body_json(
        env.call("GET", "/iceberg/v1/tbk/namespaces", "", {{"parent", "sales"}}, {}, scoped));
    CHECK_EQ(kids["namespaces"].size(), size_t(1));
    // tables: only the allowlisted name
    auto tl = TablesEnv::body_json(env.call("GET", "/iceberg/v1/tbk/namespaces/sales/tables", "", {}, {}, scoped));
    CHECK_EQ(tl["identifiers"].size(), size_t(1));
    CHECK_EQ(tl["identifiers"][0]["name"].get<std::string>(), "orders");
    // root sees everything
    CHECK_EQ(TablesEnv::body_json(env.call("GET", "/iceberg/v1/tbk/namespaces/sales/tables"))["identifiers"].size(),
             size_t(2));
}

TEST(tables_rest_s3tables_signing_name) {
    TablesEnv env;
    CHECK_EQ(env.call("GET", "/iceberg/v1/config", "", {}, {}, 0, "s3tables").status, 200);
    CHECK_EQ(env.call("GET", "/iceberg/v1/config", "", {}, {}, 0, "s3").status, 200);
    auto other = env.call("GET", "/iceberg/v1/config", "", {}, {}, 0, "execute-api");
    CHECK_EQ(other.status, 403);
    CHECK_EQ(TablesEnv::error_type(other), "ForbiddenException");
    // the S3 plane keeps accepting only "s3" (a foreign scope is a malformed Authorization header)
    auto plane = env.call("PUT", "/tbk", "", {}, {}, 0, "s3tables");
    CHECK_EQ(plane.status, 400);
    CHECK(plane.small_body.find("AuthorizationHeaderMalformed") != std::string::npos);
    TablesEnv strict(TablesEnv::Options{.accept_s3tables = false});
    CHECK_EQ(strict.call("GET", "/iceberg/v1/config", "", {}, {}, 0, "s3tables").status, 403);
    CHECK_EQ(strict.call("GET", "/iceberg/v1/config").status, 200);
}

TEST(tables_rest_credential_vending) {
    TablesEnv env(TablesEnv::Options{.vending = true});
    CHECK_EQ(env.call("PUT", "/tbk").status, 200);
    CHECK_EQ(env.call("PUT", "/iceberg/v1/buckets/tbk").status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces", R"({"namespace":["sales"]})").status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables", create_body("orders")).status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables", create_body("other")).status, 200);
    // no negotiation: no credentials, config says supported
    auto plain = env.call("GET", "/iceberg/v1/tbk/namespaces/sales/tables/orders");
    CHECK_EQ(plain.status, 200);
    CHECK(!TablesEnv::body_json(plain).contains("storage-credentials"));
    CHECK_EQ(TablesEnv::body_json(plain)["config"]["lights3.credential-vending"].get<std::string>(), "supported");
    // negotiated: a session scoped to the table prefix
    auto r = env.call("GET", "/iceberg/v1/tbk/namespaces/sales/tables/orders", "", {},
                      {{"X-Iceberg-Access-Delegation", "remote-signing, Vended-Credentials"}});
    CHECK_EQ(r.status, 200);
    CHECK_EQ(r.headers.get("Cache-Control").value_or(""), "no-store, private");
    auto j = TablesEnv::body_json(r);
    CHECK_EQ(j["storage-credentials"].size(), size_t(1));
    CHECK_EQ(j["storage-credentials"][0]["prefix"].get<std::string>(), "s3://tbk/sales/orders/");
    auto c = j["storage-credentials"][0]["config"];
    CHECK_EQ(j["config"]["lights3.credential-mode"].get<std::string>(), "catalog-vended-temporary-credentials");
    CHECK_EQ(j["config"]["s3.access-key-id"].get<std::string>(), c["s3.access-key-id"].get<std::string>());
    Credential sess{c["s3.access-key-id"].get<std::string>(),
                    util::SecretString(c["s3.secret-access-key"].get<std::string>())};
    std::vector<std::pair<std::string, std::string>> tok{
        {"x-amz-security-token", c["s3.session-token"].get<std::string>()}};
    CHECK_EQ(sess.access_key.rfind("L3SA", 0), size_t(0));
    // the session works inside the table prefix, on its metadata (read), and nowhere else
    CHECK_EQ(env.call_as(sess, "PUT", "/tbk/sales/orders/data/f.parquet", "bytes", {}, tok).status, 200);
    CHECK_EQ(env.call_as(sess, "GET", "/tbk/sales/orders/data/f.parquet", "", {}, tok).status, 200);
    CHECK_EQ(env.call_as(sess, "DELETE", "/tbk/sales/orders/data/f.parquet", "", {}, tok).status, 204);
    std::string ml = j["metadata-location"];
    CHECK_EQ(env.call_as(sess, "GET", "/tbk/" + ml.substr(std::string("s3://tbk/").size()), "", {}, tok).status, 200);
    CHECK_EQ(env.call_as(sess, "PUT", "/tbk/" + ml.substr(std::string("s3://tbk/").size()), "x", {}, tok).status, 400);
    CHECK_EQ(env.call_as(sess, "PUT", "/tbk/sales/other/data/f.parquet", "bytes", {}, tok).status, 403);
    CHECK_EQ(env.call_as(sess, "PUT", "/tbk/elsewhere", "bytes", {}, tok).status, 403);
    CHECK_EQ(env.call_as(sess, "GET", "/tbk", "", {}, tok).status, 200);
    // the session may load the table through the catalog but cannot vend again
    auto again = env.call_as(sess, "GET", "/iceberg/v1/tbk/namespaces/sales/tables/orders", "", {},
                             {{"x-amz-security-token", c["s3.session-token"].get<std::string>()},
                              {"X-Iceberg-Access-Delegation", "vended-credentials"}});
    CHECK_EQ(again.status, 200);
    auto aj = TablesEnv::body_json(again);
    CHECK(!aj.contains("storage-credentials"));
    CHECK_EQ(aj["config"]["lights3.credential-vending-reason"].get<std::string>(), "credential-vending-not-authorized");
    // a read-only caller gets a read-only session
    const size_t ro = env.add_policy_cred("ro", R"({"buckets":["tbk"],"readonly":true})");
    auto rr = TablesEnv::body_json(
        env.call("GET", "/iceberg/v1/tbk/namespaces/sales/tables/orders/credentials", "", {}, {}, ro));
    auto rc = rr["storage-credentials"][0]["config"];
    Credential rsess{rc["s3.access-key-id"].get<std::string>(),
                     util::SecretString(rc["s3.secret-access-key"].get<std::string>())};
    std::vector<std::pair<std::string, std::string>> rtok{
        {"x-amz-security-token", rc["s3.session-token"].get<std::string>()}};
    CHECK_EQ(env.call_as(rsess, "PUT", "/tbk/sales/orders/data/g.parquet", "bytes", {}, rtok).status, 403);
    CHECK_EQ(env.call_as(rsess, "GET", "/tbk/sales/orders/data/none", "", {}, rtok).status, 404);
    // the dedicated endpoint returns the credentials only
    auto ce = env.call("GET", "/iceberg/v1/tbk/namespaces/sales/tables/orders/credentials");
    CHECK_EQ(ce.status, 200);
    CHECK(TablesEnv::body_json(ce).contains("storage-credentials"));
    CHECK(!TablesEnv::body_json(ce).contains("config"));
    CHECK_EQ(env.call("GET", "/iceberg/v1/tbk/namespaces/sales/tables/nope/credentials").status, 404);
    CHECK_EQ(env.cred_store->session_count(), size_t(3));
    // vending disabled: the table is served with an explicit reason, the endpoint is 406
    TablesEnv off;
    CHECK_EQ(off.call("PUT", "/tbk").status, 200);
    CHECK_EQ(off.call("PUT", "/iceberg/v1/buckets/tbk").status, 200);
    CHECK_EQ(off.call("POST", "/iceberg/v1/tbk/namespaces", R"({"namespace":["sales"]})").status, 200);
    CHECK_EQ(off.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables", create_body("orders")).status, 200);
    auto d = TablesEnv::body_json(off.call("GET", "/iceberg/v1/tbk/namespaces/sales/tables/orders", "", {},
                                           {{"X-Iceberg-Access-Delegation", "vended-credentials"}}));
    CHECK(!d.contains("storage-credentials"));
    CHECK_EQ(d["config"]["lights3.credential-vending-reason"].get<std::string>(), "credential-vending-disabled");
    CHECK_EQ(off.call("GET", "/iceberg/v1/tbk/namespaces/sales/tables/orders/credentials").status, 406);
}

TEST(tables_rest_tenant_gate) {
    TablesEnv env(TablesEnv::Options{.tenants = true});
    Tenant a, b;
    a.id = "ta";
    b.id = "tb";
    sync_wait(env.tenant_store->put("ta", a));
    sync_wait(env.tenant_store->put("tb", b));
    CHECK_EQ(env.call("PUT", "/tbk").status, 200);
    sync_wait(env.tenants->assign("tbk", "tb", "ROOTAK", true));
    CHECK_EQ(env.call("PUT", "/iceberg/v1/buckets/tbk").status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces", R"({"namespace":["n"]})").status, 200);
    const size_t ta = env.add_policy_cred("ta-user", R"({})", "ta");
    const size_t tb = env.add_policy_cred("tb-user", R"({})", "tb");
    auto denied = env.call("GET", "/iceberg/v1/tbk/namespaces", "", {}, {}, ta);
    CHECK_EQ(denied.status, 403);
    CHECK_EQ(TablesEnv::error_type(denied), "ForbiddenException");
    CHECK_EQ(env.call("GET", "/iceberg/v1/tbk/namespaces", "", {}, {}, tb).status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/n/tables", create_body("t"), {}, {}, ta).status, 403);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/n/tables", create_body("t"), {}, {}, tb).status, 200);
    // config needs no bucket and is open to both
    CHECK_EQ(env.call("GET", "/iceberg/v1/config", "", {}, {}, ta).status, 200);
}

TEST(tables_rest_lifecycle_skips_table_buckets) {
    TablesEnv env;
    CHECK_EQ(env.call("PUT", "/tbk").status, 200);
    CHECK_EQ(env.call("PUT", "/plain").status, 200);
    CHECK_EQ(env.call("PUT", "/iceberg/v1/buckets/tbk").status, 200);
    CHECK_EQ(env.call("PUT", "/tbk/old/a.log", "x").status, 200);
    CHECK_EQ(env.call("PUT", "/plain/old/a.log", "x").status, 200);
    auto store = sync_wait(LifecycleStore::load(env.backend));
    std::vector<LifecycleRule> rules;
    rules.push_back({.id = "exp", .prefix = "old/", .expiration_days = 7});
    sync_wait(store->put("tbk", rules));
    sync_wait(store->put("plain", rules));
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> bmap{{"mem", env.backend}};
    BucketsConfig bcfg;
    bcfg.default_backend = "mem";
    LifecycleRunner runner(storage::BucketRouter::build(bcfg, std::move(bmap)), store);
    runner.set_skip_predicate([guard = env.guard](std::string_view b) { return guard->is_table_bucket(b); });
    runner.set_now_for_tests([] { return std::chrono::system_clock::now() + std::chrono::hours(24 * 10); });
    auto st = sync_wait(runner.run_once());
    CHECK_EQ(st.objects_expired, uint64_t(1));
    CHECK_EQ(env.call("GET", "/tbk/old/a.log").status, 200);
    CHECK_EQ(env.call("GET", "/plain/old/a.log").status, 404);
    // the rule API accepts table buckets (like AWS) but the runner keeps ignoring them
    env.svc->set_lifecycle_store(store);
    CHECK_EQ(env.call("PUT", "/tbk",
                      "<LifecycleConfiguration><Rule><ID>x</ID><Status>Enabled</Status><Filter><Prefix>old/</Prefix></"
                      "Filter><Expiration><Days>1</Days></Expiration></Rule></LifecycleConfiguration>",
                      {{"lifecycle", ""}})
                 .status,
             200);
}

// ---------- step ③: diagnostics / recovery endpoints, If-None-Match, validation flag ----------

TEST(tables_rest_diagnostics_recovery_and_etag) {
    struct FaultReset {
        ~FaultReset() { fault::reset(); }
    } guard;
    TablesEnv env;
    CHECK_EQ(env.call("PUT", "/tbk").status, 200);
    CHECK_EQ(env.call("PUT", "/iceberg/v1/buckets/tbk").status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces", R"({"namespace":["sales"]})").status, 200);
    auto ct = env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables", create_body("t"));
    CHECK_EQ(ct.status, 200);
    const std::string table = "/iceberg/v1/tbk/namespaces/sales/tables/t";
    // ETag / If-None-Match (step ③ §8): a hit is a bodiless 304 that still carries the ETag
    auto lt = env.call("GET", table);
    CHECK_EQ(lt.status, 200);
    std::string etag = *lt.headers.get("ETag");
    CHECK_EQ(TablesEnv::body_json(lt)["config"]["lights3.catalog-etag"].get<std::string>(),
             etag.substr(1, etag.size() - 2));
    CHECK_EQ(TablesEnv::body_json(lt)["config"]["lights3.snapshot-validation"].get<std::string>(), "deep");
    auto nm = env.call("GET", table, "", {}, {{"If-None-Match", etag}});
    CHECK_EQ(nm.status, 304);
    CHECK(nm.small_body.empty());
    CHECK_EQ(*nm.headers.get("ETag"), etag);
    CHECK_EQ(env.call("GET", table, "", {}, {{"If-None-Match", "W/" + etag + ", \"other\""}}).status, 304);
    CHECK_EQ(env.call("GET", table, "", {}, {{"If-None-Match", "\"stale\""}}).status, 200);
    // diagnostics of a fresh table: no records, nothing unreferenced
    auto dg = env.call("GET", table + "/catalog/diagnostics");
    CHECK_EQ(dg.status, 200);
    auto dj = TablesEnv::body_json(dg);
    CHECK_EQ(dj["commits"].size(), size_t(0));
    CHECK_EQ(dj["unreferenced-metadata"].size(), size_t(0));
    CHECK_EQ(dj["table"]["name"].get<std::string>(), "t");
    CHECK_EQ(dj["table"]["etag"].get<std::string>(), etag.substr(1, etag.size() - 2));
    // a commit that dies after the pointer CAS leaves a finalization gap (design §5.4)
    env.install_snapshot("sales/t/metadata/ml.avro");
    fault::arm("tables.commit.after_cas:1");
    auto gap = env.call("POST", table, append_body(1, 1, "s3://tbk/sales/t/metadata/ml.avro", "c-gap"));
    CHECK_EQ(gap.status, 500);
    auto ptr = env.call("GET", table + "/metadata-location");
    CHECK_EQ(TablesEnv::body_json(ptr)["generation"].get<int>(), 2);
    dj = TablesEnv::body_json(env.call("GET", table + "/catalog/diagnostics"));
    CHECK_EQ(dj["commits"].size(), size_t(1));
    CHECK_EQ(dj["commits"][0]["commit-id"].get<std::string>(), "c-gap");
    CHECK_EQ(dj["commits"][0]["state"].get<std::string>(), "FinalizationRequired");
    std::string etag2 = *env.call("GET", table).headers.get("ETag");
    auto rc = env.call("POST", table + "/catalog/recovery", R"({"prune":false})");
    CHECK_EQ(rc.status, 200);
    CHECK_EQ(TablesEnv::body_json(rc)["finalized"].get<int>(), 1);
    CHECK_EQ(TablesEnv::body_json(rc)["pruned"].get<int>(), 0);
    dj = TablesEnv::body_json(env.call("GET", table + "/catalog/diagnostics"));
    CHECK_EQ(dj["commits"][0]["state"].get<std::string>(), "Committed");
    // the pointer did not move
    CHECK_EQ(*env.call("GET", table).headers.get("ETag"), etag2);
    CHECK_EQ(env.call("POST", table + "/catalog/recovery", R"({"prune":"yes"})").status, 400);
    CHECK_EQ(env.call("POST", table + "/catalog/recovery").status, 200);
    CHECK_EQ(env.call("GET", "/iceberg/v1/tbk/namespaces/sales/tables/nope/catalog/diagnostics").status, 404);
    // permissions: diagnostics is a read, recovery a write
    const size_t ro = env.add_policy_cred("ro", R"({"buckets":["tbk"],"readonly":true})");
    CHECK_EQ(env.call("GET", table + "/catalog/diagnostics", "", {}, {}, ro).status, 200);
    CHECK_EQ(env.call("POST", table + "/catalog/recovery", "{}", {}, {}, ro).status, 403);
    // a manifest list in a codec the reader cannot decode: accepted, flagged in config
    CHECK_EQ(env.call("PUT", "/tbk/sales/t/metadata/snappy.avro", tables_fixtures::snappy_manifest_list()).status, 200);
    auto sc = env.call("POST", table, append_body(2, 2, "s3://tbk/sales/t/metadata/snappy.avro"));
    CHECK_EQ(sc.status, 200);
    CHECK_EQ(TablesEnv::body_json(sc)["config"]["lights3.snapshot-validation"].get<std::string>(), "skipped-codec");
    auto ok = env.call("POST", table, append_body(3, 3, "s3://tbk/sales/t/metadata/ml.avro"));
    CHECK_EQ(ok.status, 200);
    CHECK_EQ(TablesEnv::body_json(ok)["config"]["lights3.snapshot-validation"].get<std::string>(), "deep");
    // a data file the manifest names is gone: 409 CommitFailedException
    CHECK_EQ(env.call("DELETE", "/tbk/n/t/data/f1.parquet").status, 204);
    auto missing = env.call("POST", table, append_body(4, 4, "s3://tbk/sales/t/metadata/ml.avro"));
    CHECK_EQ(missing.status, 409);
    CHECK_EQ(TablesEnv::error_type(missing), "CommitFailedException");
    // the endpoints are extensions: not advertised in /config
    for (auto& e : tables::RestApi::advertised_endpoints()) CHECK(e.find("catalog/") == std::string::npos);
}

// ---------- step ④: maintenance endpoints, admin-plane jobs, purge ----------

TEST(tables_rest_maintenance_endpoints) {
    TablesEnv env;
    CHECK_EQ(env.call("PUT", "/tbk").status, 200);
    CHECK_EQ(env.call("PUT", "/iceberg/v1/buckets/tbk").status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces", R"({"namespace":["sales"]})").status, 200);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables", create_body("t")).status, 200);
    const std::string table = "/iceberg/v1/tbk/namespaces/sales/tables/t";
    env.install_snapshot("sales/t/metadata/ml.avro");
    CHECK_EQ(env.call("POST", table, append_body(1, 1, "s3://tbk/sales/t/metadata/ml.avro")).status, 200);
    // settings: defaults, then a table object, validated
    auto cfg0 = env.call("GET", table + "/maintenance/config");
    CHECK_EQ(cfg0.status, 200);
    auto cj = TablesEnv::body_json(cfg0);
    CHECK(!cj["effective"]["delete_enabled"].get<bool>());
    CHECK(cj["table-config"].is_null());
    CHECK_EQ(cj["defaults"]["retain_recent_metadata_files"].get<int>(), 10);
    auto put = env.call("PUT", table + "/maintenance/config",
                        R"({"delete_enabled":true,"retain_recent_metadata_files":1})");
    CHECK_EQ(put.status, 200);
    CHECK(TablesEnv::body_json(put)["effective"]["delete_enabled"].get<bool>());
    CHECK_EQ(TablesEnv::body_json(put)["effective"]["retain_recent_metadata_files"].get<int>(), 1);
    CHECK_EQ(env.call("PUT", table + "/maintenance/config", R"({"delete_enabled":"yes"})").status, 400);
    CHECK_EQ(env.call("PUT", table + "/maintenance/config", R"({"bogus":1})").status, 400);
    CHECK_EQ(env.call("PUT", "/iceberg/v1/tbk/namespaces/sales/tables/nope/maintenance/config", "{}").status, 404);
    auto poll = [&](const std::string& path) {
        for (int i = 0; i < 500; ++i) {
            auto r = env.call("GET", path);
            CHECK_EQ(r.status, 200);
            auto j = TablesEnv::body_json(r);
            if (!j.value("running", false)) return j;
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        throw std::runtime_error("job did not finish");
    };
    // plan: 202 + job id; the job document carries the plan
    auto pl = env.call("POST", table + "/maintenance/plan");
    CHECK_EQ(pl.status, 202);
    uint64_t plan_id = TablesEnv::body_json(pl)["job_id"].get<uint64_t>();
    auto plan_doc = poll(table + "/maintenance/jobs/" + std::to_string(plan_id));
    CHECK_EQ(plan_doc["op"].get<std::string>(), "plan");
    CHECK(plan_doc["stats"].contains("version-token"));
    CHECK(!plan_doc["stats"]["manual-review"].get<bool>());
    CHECK(plan_doc["stats"]["effective"]["delete_enabled"].get<bool>());
    // run the latest plan (nothing is old enough to delete: safety window)
    auto rn = env.call("POST", table + "/maintenance/run", "{}");
    CHECK_EQ(rn.status, 202);
    auto run_doc = poll(table + "/maintenance/jobs/" +
                        std::to_string(TablesEnv::body_json(rn)["job_id"].get<uint64_t>()));
    CHECK_EQ(run_doc["op"].get<std::string>(), "run");
    CHECK(run_doc["stats"]["delete_enabled"].get<bool>());
    CHECK_EQ(run_doc["stats"]["deleted_metadata"].get<int>(), 0);
    // by plan job id, inline plan, and the refusals
    CHECK_EQ(env.call("POST", table + "/maintenance/run", R"({"job_id":)" + std::to_string(plan_id) + "}").status, 202);
    poll(table + "/maintenance/jobs/" + std::to_string(plan_id + 2));
    CHECK_EQ(env.call("POST", table + "/maintenance/run", R"({"job_id":999})").status, 400);
    CHECK_EQ(env.call("POST", table + "/maintenance/run", R"({"plan":{"x":1}})").status, 400);
    json stale = plan_doc["stats"];
    stale["version-token"] = "t-stale";
    auto st = env.call("POST", table + "/maintenance/run", json({{"plan", stale}}).dump());
    CHECK_EQ(st.status, 202);
    auto stale_doc = poll(table + "/maintenance/jobs/" +
                          std::to_string(TablesEnv::body_json(st)["job_id"].get<uint64_t>()));
    CHECK(stale_doc.contains("error"));
    CHECK(stale_doc["error"].get<std::string>().find("StalePlan") != std::string::npos);
    CHECK_EQ(env.call("GET", table + "/maintenance/jobs/abc").status, 400);
    CHECK_EQ(env.call("GET", table + "/maintenance/jobs/999").status, 404);
    // permissions: config / job status are reads, plan / run / config PUT writes
    const size_t ro = env.add_policy_cred("ro", R"({"buckets":["tbk"],"readonly":true})");
    CHECK_EQ(env.call("GET", table + "/maintenance/config", "", {}, {}, ro).status, 200);
    CHECK_EQ(env.call("GET", table + "/maintenance/jobs/" + std::to_string(plan_id), "", {}, {}, ro).status, 200);
    CHECK_EQ(env.call("POST", table + "/maintenance/plan", "", {}, {}, ro).status, 403);
    CHECK_EQ(env.call("PUT", table + "/maintenance/config", "{}", {}, {}, ro).status, 403);
    // the admin plane: root only, same jobs
    auto ap = env.call("POST", "/-/admin/tables/tbk/sales/t/plan");
    CHECK_EQ(ap.status, 202);
    uint64_t admin_plan = TablesEnv::body_json(ap)["job_id"].get<uint64_t>();
    auto ad = poll(table + "/maintenance/jobs/" + std::to_string(admin_plan));
    CHECK_EQ(ad["backend"].get<std::string>(), "tables:tbk/sales/t");
    auto ag = env.call("GET", "/-/admin/tables/tbk/sales/t/plan");
    CHECK_EQ(ag.status, 200);
    CHECK_EQ(TablesEnv::body_json(ag)["job_id"].get<uint64_t>(), admin_plan);
    CHECK_EQ(env.call("POST", "/-/admin/tables/tbk/sales/t/nope").status, 400);
    CHECK_EQ(env.call("POST", "/-/admin/tables/tbk/sales/missing/plan").status, 404);
    CHECK_EQ(env.call("POST", "/-/admin/tables/tbk/sales/t/plan", "", {}, {}, ro).status, 403);
    CHECK_EQ(env.call("POST", "/-/admin/tables/tbk/sales/t/run", R"({"job_id":999})").status, 400);
    // purge: tombstone at once, the data in a job
    auto dp = env.call("DELETE", table, "", {{"purgeRequested", "true"}});
    CHECK_EQ(dp.status, 204);
    CHECK(dp.headers.has("x-lights3-job-id"));
    uint64_t purge_id = std::stoull(*dp.headers.get("x-lights3-job-id"));
    auto pd = poll(table + "/maintenance/jobs/" + std::to_string(purge_id));
    CHECK_EQ(pd["op"].get<std::string>(), "purge");
    CHECK(pd["stats"]["tombstone_removed"].get<bool>());
    // the reserved directory (2 metadata files) and the location prefix (the manifest list);
    // the fixture's manifest and data files live under n/t and belong to no table
    CHECK_EQ(pd["stats"]["deleted_objects"].get<uint64_t>(), uint64_t(3));
    CHECK_EQ(env.call("HEAD", table).status, 404);
    CHECK_EQ(env.call("GET", "/tbk/sales/t/metadata/ml.avro").status, 404);
    CHECK_EQ(env.call("GET", "/tbk/n/t/metadata/m-1.avro").status, 200);
    auto lt = env.call("GET", "/iceberg/v1/tbk/namespaces/sales/tables");
    CHECK_EQ(TablesEnv::body_json(lt)["identifiers"].size(), size_t(0));
    CHECK_EQ(env.call("DELETE", table, "", {{"purgeRequested", "maybe"}}).status, 400);
    // none of it is advertised
    for (auto& e : tables::RestApi::advertised_endpoints()) CHECK(e.find("maintenance") == std::string::npos);
}

// ---------- step ⑥: views, the /_iceberg alias, reportMetrics into the audit log ----------

TEST(tables_rest_views_compat_prefix_and_metrics) {
    std::string audit_path = (std::filesystem::temp_directory_path() /
                              ("lights3-tables-audit-" + std::to_string(::getpid()) + ".log"))
                                 .string();
    std::filesystem::remove(audit_path);
    TablesEnv::Options o;
    o.compat_prefix = "/_iceberg";
    o.audit_path = audit_path;
    {
        TablesEnv env(o);
        CHECK_EQ(env.call("PUT", "/tbk").status, 200);
        CHECK_EQ(env.call("PUT", "/iceberg/v1/buckets/tbk").status, 200);
        // the alias reaches the same catalog; /config advertises it; its bucket name is reserved
        auto cfg = env.call("GET", "/_iceberg/v1/config", "", {{"warehouse", "tbk"}});
        CHECK_EQ(cfg.status, 200);
        CHECK_EQ(TablesEnv::body_json(cfg)["defaults"]["lights3.catalog-compat-prefix"].get<std::string>(),
                 "/_iceberg/v1");
        CHECK_EQ(TablesEnv::body_json(cfg)["overrides"]["prefix"].get<std::string>(), "tbk");
        CHECK_EQ(env.call("POST", "/_iceberg/v1/tbk/namespaces", R"({"namespace":["sales"]})").status, 200);
        CHECK_EQ(env.call("HEAD", "/iceberg/v1/tbk/namespaces/sales").status, 204);
        CHECK_EQ(env.call("PUT", "/_iceberg").status, 400);
        CHECK_EQ(env.call("PUT", "/iceberg").status, 400);
        // views: create / list / load / exists / replace / rename / drop, and the endpoints list
        const std::string body =
            R"({"name":"v","schema":{"type":"struct","fields":[{"id":1,"name":"x","required":false,"type":"int"}]},
            "view-version":{"representations":[{"type":"sql","sql":"select 1","dialect":"spark"}],"default-namespace":["sales"]},
            "properties":{"comment":"c"}})";
        auto cv = env.call("POST", "/iceberg/v1/tbk/namespaces/sales/views", body);
        CHECK_EQ(cv.status, 200);
        auto cvj = TablesEnv::body_json(cv);
        std::string uuid = cvj["metadata"]["view-uuid"];
        CHECK_EQ(cvj["metadata"]["current-version-id"].get<int>(), 1);
        CHECK_EQ(cvj["metadata-location"].get<std::string>().rfind("s3://tbk/.lights3-table/sales/v/view-metadata/", 0),
                 size_t(0));
        CHECK(cv.headers.has("ETag"));
        CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/views", body).status, 409);
        CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/views", R"({"name":"w"})").status, 400);
        auto lv = env.call("GET", "/_iceberg/v1/tbk/namespaces/sales/views");
        CHECK_EQ(TablesEnv::body_json(lv)["identifiers"][0]["name"].get<std::string>(), "v");
        CHECK_EQ(env.call("HEAD", "/iceberg/v1/tbk/namespaces/sales/views/v").status, 204);
        auto nf = env.call("GET", "/iceberg/v1/tbk/namespaces/sales/views/nope");
        CHECK_EQ(nf.status, 404);
        CHECK_EQ(TablesEnv::error_type(nf), "NoSuchViewException");
        // a table may not take the view's name and vice versa
        CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables", create_body("v")).status, 409);
        CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables", create_body("t")).status, 200);
        CHECK_EQ(
            env.call(
                   "POST", "/iceberg/v1/tbk/namespaces/sales/views",
                   R"({"name":"t","schema":{"type":"struct","fields":[]},"view-version":{"representations":[{"type":"sql","sql":"x","dialect":"d"}]}})")
                .status,
            409);
        auto rv = env.call(
            "POST", "/iceberg/v1/tbk/namespaces/sales/views/v",
            R"({"identifier":{"namespace":["sales"],"name":"v"},"requirements":[{"type":"assert-view-uuid","uuid":")" +
                uuid +
                R"("}],"updates":[{"action":"add-view-version","view-version":{"representations":[{"type":"sql","sql":"select 2","dialect":"spark"}],"schema-id":-1}},{"action":"set-current-view-version","view-version-id":-1}]})");
        CHECK_EQ(rv.status, 200);
        CHECK_EQ(TablesEnv::body_json(rv)["metadata"]["current-version-id"].get<int>(), 2);
        auto stale = env.call(
            "POST", "/iceberg/v1/tbk/namespaces/sales/views/v",
            R"({"requirements":[{"type":"assert-view-uuid","uuid":"00000000-0000-4000-8000-000000000000"}],"updates":[]})");
        CHECK_EQ(stale.status, 409);
        CHECK_EQ(TablesEnv::error_type(stale), "CommitFailedException");
        CHECK_EQ(
            env.call(
                   "POST", "/iceberg/v1/tbk/views/rename",
                   R"({"source":{"namespace":["sales"],"name":"v"},"destination":{"namespace":["sales"],"name":"v2"}})")
                .status,
            204);
        CHECK_EQ(env.call("HEAD", "/iceberg/v1/tbk/namespaces/sales/views/v").status, 404);
        CHECK_EQ(env.call("GET", "/iceberg/v1/tbk/namespaces/sales/views/v2").status, 200);
        // a read-only credential loads but may not replace / drop
        const size_t ro = env.add_policy_cred("ro", R"({"buckets":["tbk"],"readonly":true})");
        CHECK_EQ(env.call("GET", "/iceberg/v1/tbk/namespaces/sales/views/v2", "", {}, {}, ro).status, 200);
        CHECK_EQ(env.call("DELETE", "/iceberg/v1/tbk/namespaces/sales/views/v2", "", {}, {}, ro).status, 403);
        CHECK_EQ(env.call("DELETE", "/iceberg/v1/tbk/namespaces/sales/views/v2").status, 204);
        CHECK_EQ(env.call("DELETE", "/iceberg/v1/tbk/namespaces/sales/views/v2").status, 404);
        std::set<std::string> endpoints;
        for (auto& e : tables::RestApi::advertised_endpoints()) endpoints.insert(e);
        CHECK(endpoints.count("POST /v1/{prefix}/namespaces/{namespace}/views"));
        CHECK(endpoints.count("GET /v1/{prefix}/namespaces/{namespace}/views/{view}"));
        CHECK(endpoints.count("POST /v1/{prefix}/views/rename"));
        // reportMetrics: recorded as tables.metrics; oversized reports are accepted silently
        auto mr = env.call(
            "POST", "/iceberg/v1/tbk/namespaces/sales/tables/t/metrics",
            R"({"report-type":"scan-report","table-name":"sales.t","snapshot-id":1,"filter":{"type":"true"},
                               "schema-id":0,"projected-field-names":["id"],
                               "metrics":{"result-data-files":{"unit":"count","value":3},"total-planning-duration":{"count":1,"time-unit":"nanoseconds","total-duration":1200}},
                               "metadata":{"engine-name":"pyiceberg"}})");
        CHECK_EQ(mr.status, 204);
        std::string big = R"({"report-type":"scan-report","table-name":"sales.t","metrics":{},"metadata":{"pad":")" +
                          std::string(70 * 1024, 'x') + R"("}})";
        CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables/t/metrics", big).status, 204);
        CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables/t/metrics", "not json").status, 204);
        env.audit->flush();
    }
    std::ifstream in(audit_path);
    std::vector<json> lines;
    for (std::string line; std::getline(in, line);) lines.push_back(json::parse(line));
    size_t metrics = 0;
    for (auto& j : lines) {
        if (j.value("event", "") != "tables.metrics") continue;
        ++metrics;
        json detail = json::parse(j.value("detail", "{}"));
        CHECK_EQ(detail["report-type"].get<std::string>(), "scan-report");
        CHECK_EQ(detail["metrics"]["result-data-files"].get<int>(), 3);
        CHECK_EQ(detail["metrics"]["total-planning-duration"]["count"].get<int>(), 1);
        CHECK_EQ(detail["projected-field-names"][0].get<std::string>(), "id");
        CHECK_EQ(j.value("key", ""), "sales/t");
    }
    CHECK_EQ(metrics, size_t(1));
    std::filesystem::remove(audit_path);
}
