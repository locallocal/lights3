// Iceberg REST surface through the full S3Service dispatch (docs/s3-tables-design.md §6, §8.1,
// docs/s3-tables/step-1-catalog-core.md §15): endpoints, error model, guard, config drift
#include <nlohmann/json.hpp>
#include <set>

#include "core/util/checksum.h"
#include "core/util/crypto.h"
#include "core/util/uri.h"
#include "s3/auth/credential_store.h"
#include "s3/service.h"
#include "storage/memory/memory_backend.h"
#include "tables/bucket_guard.h"
#include "tables/catalog.h"
#include "tables/object_catalog_store.h"
#include "tables/rest_api.h"
#include "unit/mini_test.h"

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
    std::unique_ptr<S3Service> svc;
    TablesConfig cfg;

    static AuthConfig root_acfg() {
        AuthConfig a;
        a.credentials = {{"ROOTAK", "root-sk"}};
        return a;
    }

    explicit TablesEnv(bool enabled = true) : acfg(root_acfg()), auth(SigV4Authenticator::build(acfg)) {
        cfg.enabled = enabled;
        cred_store = sync_wait(CredentialStore::load(backend, acfg));
        auth.set_provider(cred_store);
        std::map<std::string, std::shared_ptr<storage::IStorageBackend>> bmap{{"mem", backend}};
        BucketsConfig bcfg;
        bcfg.default_backend = "mem";
        auto router = storage::BucketRouter::build(bcfg, std::move(bmap));
        svc = std::make_unique<S3Service>(router, auth);
        svc->set_credential_store(cred_store);
        if (enabled) {
            buckets = sync_wait(tables::TableBucketStore::load(backend));
            auto store = std::make_shared<tables::ObjectCatalogStore>(backend);
            catalog = std::make_shared<tables::Catalog>(store, buckets, router, nullptr, cfg, MetricsScope{});
            api = std::make_shared<tables::RestApi>(catalog, cfg, MetricsScope{});
            guard = std::make_shared<tables::TableBucketGuard>(buckets, catalog, cfg.path_prefix, cfg.compat_prefix);
            svc->set_tables(api, guard);
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
    http::HttpResponse call(std::string method, std::string raw_path, std::string body = "",
                            std::vector<std::pair<std::string, std::string>> query = {},
                            std::vector<std::pair<std::string, std::string>> headers = {}, size_t cred = 0) {
        auto r = make_req(std::move(method), std::move(raw_path), body, std::move(query), std::move(headers));
        auth.sign(r, acfg.credentials[cred], body.empty() ? "" : util::sha256_hex(body));
        return sync_wait(svc->dispatch(std::move(r)));
    }
    http::HttpResponse anon(std::string method, std::string raw_path) {
        auto r = make_req(std::move(method), std::move(raw_path), "", {}, {});
        return sync_wait(svc->dispatch(std::move(r)));
    }
    static json body_json(const http::HttpResponse& r) { return json::parse(r.small_body); }
    static std::string error_type(const http::HttpResponse& r) { return body_json(r)["error"]["type"]; }
    // adds a dynamic credential with a policy; returns its index in acfg.credentials
    size_t add_policy_cred(const std::string& comment, const std::string& policy_json) {
        auto info = sync_wait(cred_store->generate(comment, parse_policy_json(policy_json)));
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
    body["updates"] = json::array({json::object({{"action", "add-snapshot"}, {"snapshot", s}}),
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
    auto props = env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/properties", R"({"removals":["o"],"updates":{"x":"1"}})");
    CHECK_EQ(props.status, 200);
    CHECK_EQ(TablesEnv::body_json(props)["removed"][0].get<std::string>(), "o");
    auto both = env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/properties", R"({"removals":["x"],"updates":{"x":"1"}})");
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
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables", R"({"name":"s","stage-create":true,"schema":{"type":"struct","fields":[]}})").status, 406);
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
    // commit
    CHECK_EQ(env.call("PUT", "/tbk/sales/eu/orders/metadata/snap-1.avro", "avro").status, 200);
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
    auto stale = env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders",
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
    CHECK_EQ(env.call("GET", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders", "", {{"snapshots", "some"}}).status, 400);
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
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders/metrics", R"({"report-type":"scan-report"})").status, 204);
    // rename / drop / namespace drop
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/tables/rename",
                      R"({"source":{"namespace":["sales","eu"],"name":"orders"},"destination":{"namespace":["sales","eu"],"name":"orders2"}})")
                 .status,
             204);
    CHECK_EQ(env.call("HEAD", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders").status, 404);
    CHECK_EQ(env.call("HEAD", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders2").status, 204);
    auto purge = env.call("DELETE", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders2", "", {{"purgeRequested", "true"}});
    CHECK_EQ(purge.status, 406);
    CHECK_EQ(TablesEnv::error_type(purge), "UnsupportedOperationException");
    CHECK_EQ(env.call("DELETE", "/tbk").status, 409);
    CHECK_EQ(env.call("DELETE", "/iceberg/v1/tbk/namespaces/sales%1Feu/tables/orders2", "", {{"purgeRequested", "false"}}).status, 204);
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
    auto r = env.call("POST", "/tbk", body, {{"delete", ""}}, {{"Content-MD5", util::base64_encode(std::span(d.data(), d.size()))}});
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
        CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces", std::string(R"({"namespace":[")") + n + "\"]}").status, 200);
    auto p1 = TablesEnv::body_json(env.call("GET", "/iceberg/v1/tbk/namespaces", "", {{"pageSize", "2"}}));
    CHECK_EQ(p1["namespaces"].size(), size_t(2));
    std::string tok = p1["next-page-token"];
    auto p2 = TablesEnv::body_json(env.call("GET", "/iceberg/v1/tbk/namespaces", "", {{"pageSize", "2"}, {"pageToken", tok}}));
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
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/namespaces/sales/tables/t", R"({"requirements":[],"updates":[]})", {}, {}, ro).status, 403);
    CHECK_EQ(env.call("DELETE", "/iceberg/v1/tbk/namespaces/sales/tables/t", "", {}, {}, ro).status, 403);
    // rename needs delete on the source and write on the destination
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/tables/rename",
                      R"({"source":{"namespace":["sales"],"name":"t"},"destination":{"namespace":["hr"],"name":"t"}})", {}, {}, scoped)
                 .status,
             403);
    CHECK_EQ(env.call("POST", "/iceberg/v1/tbk/tables/rename",
                      R"({"source":{"namespace":["sales"],"name":"t"},"destination":{"namespace":["sales"],"name":"t2"}})", {}, {}, scoped)
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
