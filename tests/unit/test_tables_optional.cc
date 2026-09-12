// Step ⑥ (docs/s3-tables-design.md §14 ⑥): the catalog store suite over both backings,
// Iceberg views through the Catalog, transactional commits on the duostore-meta backing,
// compaction candidates in the maintenance plan, the catalog_backing configuration
#include <nlohmann/json.hpp>
#include <set>

#include "core/config.h"
#include "storage/memory/memory_backend.h"
#include "tables/catalog.h"
#include "tables/iceberg/metadata.h"
#include "tables/iceberg/view_metadata.h"
#include "tables/maintenance.h"
#include "tables/object_catalog_store.h"
#include "tables/rest_error.h"
#include "unit/backend_suite.h"
#include "unit/catalog_store_suite.h"
#include "unit/mini_test.h"
#include "unit/tables_fixtures.h"
#ifdef LIGHTS3_DUOSTORE
#include "storage/duostore/rocks_meta_store.h"
#include "tables/duo_meta_catalog_store.h"
#endif

using namespace lights3;
using namespace lights3::tables;
using nlohmann::json;

namespace {

Levels ns(std::initializer_list<const char*> l) { return Levels(l.begin(), l.end()); }

// A catalog over an arbitrary store; the table bucket lives on a MemoryBackend
struct Env {
    std::shared_ptr<storage::MemoryBackend> backend = std::make_shared<storage::MemoryBackend>();
    std::shared_ptr<TableBucketStore> buckets;
    std::shared_ptr<ITableCatalogStore> store;
    std::shared_ptr<Catalog> catalog;
    TablesConfig cfg;

    explicit Env(std::shared_ptr<ITableCatalogStore> s = nullptr) {
        cfg.enabled = true;
        std::map<std::string, std::shared_ptr<storage::IStorageBackend>> bmap{{"mem", backend}};
        BucketsConfig bcfg;
        bcfg.default_backend = "mem";
        auto router = storage::BucketRouter::build(bcfg, std::move(bmap));
        buckets = sync_wait(TableBucketStore::load(backend));
        store = s ? std::move(s) : std::make_shared<ObjectCatalogStore>(backend);
        catalog = std::make_shared<Catalog>(store, buckets, router, nullptr, cfg, MetricsScope{});
        sync_wait(backend->create_bucket("tbk"));
        sync_wait(catalog->enable_bucket("tbk"));
        sync_wait(catalog->create_namespace("tbk", ns({"n"}), {}));
    }
    void put(const std::string& key, const std::string& body) {
        storage::ObjectMeta meta;
        http::StringBodyReader r(body);
        sync_wait(backend->put_object("tbk", key, std::move(meta), r));
    }
    bool exists(const std::string& key) {
        try {
            sync_wait(backend->head_object("tbk", key));
            return true;
        } catch (const s3::S3Error&) {
            return false;
        }
    }
    Catalog::LoadedTable create_table(const std::string& name) {
        CreateTableRequest r;
        r.name = name;
        r.schema = json::parse(R"({"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]})");
        return sync_wait(catalog->create_table("tbk", ns({"n"}), r, {}));
    }
    CreateViewRequest view_req(const std::string& name, const char* sql = "select 1") {
        CreateViewRequest r;
        r.name = name;
        r.schema = json::parse(R"({"type":"struct","fields":[{"id":1,"name":"x","required":false,"type":"int"}]})");
        r.view_version = json::parse(std::string(R"({"representations":[{"type":"sql","sql":")") + sql +
                                     R"(","dialect":"spark"}],"default-namespace":["n"]})");
        r.properties = {{"comment", "c"}};
        return r;
    }
    void install_snapshot(const std::string& ml_key) {
        put(ml_key, tables_fixtures::slurp("ml-1.avro"));
        put("n/t/metadata/m-1.avro", tables_fixtures::slurp("m-1.avro"));
        put("n/t/data/f1.parquet", std::string(100, 'a'));
        put("n/t/data/f2.parquet", std::string(200, 'b'));
    }
    Catalog::LoadedTable commit(int64_t snap, const std::string& ml_key, const std::string& id = "") {
        CommitRequest c;
        c.commit_id = id;
        json s;
        s["snapshot-id"] = snap;
        s["sequence-number"] = snap;
        s["timestamp-ms"] = 1000 + snap;
        s["manifest-list"] = "s3://tbk/" + ml_key;
        s["summary"] = json::object({{"operation", "append"}});
        c.updates = json::array(
            {json::object({{"action", "add-snapshot"}, {"snapshot", s}}),
             json::parse(R"({"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":)" +
                         std::to_string(snap) + "}")});
        return sync_wait(catalog->commit_table("tbk", ns({"n"}), "t", c, {}));
    }
};

template <class F>
int status_of(F&& f) {
    try {
        f();
    } catch (const RestError& e) {
        return e.status;
    } catch (const s3::S3Error& e) {
        return from_s3_error(e, "").status;
    }
    return 0;
}

void view_lifecycle(Env& env) {
    // create / load: the initial metadata is a format-1 view with one version
    auto v = sync_wait(env.catalog->create_view("tbk", ns({"n"}), env.view_req("v")));
    CHECK_EQ(v.entry.generation, uint64_t(1));
    CHECK_EQ(v.entry.location, "s3://tbk/n/v");
    CHECK_EQ(v.entry.metadata_location.rfind(".lights3-table/n/v/view-metadata/00001-", 0), size_t(0));
    CHECK(env.exists(v.entry.metadata_location));
    CHECK_EQ(v.metadata["format-version"].get<int>(), 1);
    CHECK_EQ(v.metadata["current-version-id"].get<int>(), 1);
    CHECK_EQ(v.metadata["versions"][0]["representations"][0]["sql"].get<std::string>(), "select 1");
    CHECK_EQ(v.metadata["versions"][0]["summary"]["operation"].get<std::string>(), "create");
    CHECK_EQ(v.metadata["properties"]["comment"].get<std::string>(), "c");
    auto l = sync_wait(env.catalog->load_view("tbk", ns({"n"}), "v"));
    CHECK_EQ(l.entry.view_id, v.entry.view_id);
    CHECK_EQ(l.etag, v.etag);
    CHECK(sync_wait(env.catalog->view_exists("tbk", ns({"n"}), "v")));
    CHECK(!sync_wait(env.catalog->view_exists("tbk", ns({"n"}), "nope")));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->load_view("tbk", ns({"n"}), "nope")); }), 404);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->create_view("tbk", ns({"n"}), env.view_req("v"))); }), 409);
    // a table and a view never share a name
    env.create_table("t");
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->create_view("tbk", ns({"n"}), env.view_req("t"))); }), 409);
    CHECK_EQ(status_of([&] { env.create_table("v"); }), 409);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->rename_table("tbk", ns({"n"}), "t", ns({"n"}), "v")); }), 409);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->rename_view("tbk", ns({"n"}), "v", ns({"n"}), "t")); }), 409);
    // the namespace now holds a table and a view; listings are separate
    CHECK_EQ(sync_wait(env.catalog->list_views("tbk", ns({"n"}), PageCursor{})).items.size(), size_t(1));
    CHECK_EQ(sync_wait(env.catalog->list_tables("tbk", ns({"n"}), PageCursor{})).items.size(), size_t(1));
    // replace: a new version becomes current, the generation moves, the old file stays
    json updates = json::array({
        json::parse(R"({"action":"set-properties","updates":{"comment":"d"}})"),
        json::parse(
            R"({"action":"add-view-version","view-version":{"representations":[{"type":"sql","sql":"select 2","dialect":"spark"}],"default-namespace":["n"],"schema-id":-1}})"),
        json::parse(R"({"action":"set-current-view-version","view-version-id":-1})"),
    });
    json reqs = json::array({json{{"type", "assert-view-uuid"}, {"uuid", v.entry.view_uuid}}});
    auto r = sync_wait(env.catalog->replace_view("tbk", ns({"n"}), "v", reqs, updates));
    CHECK_EQ(r.entry.generation, uint64_t(2));
    CHECK(r.entry.version_token != v.entry.version_token);
    CHECK_EQ(r.metadata["current-version-id"].get<int>(), 2);
    CHECK_EQ(r.metadata["versions"].size(), size_t(2));
    CHECK_EQ(r.metadata["version-log"].size(), size_t(2));
    CHECK_EQ(r.metadata["properties"]["comment"].get<std::string>(), "d");
    CHECK(env.exists(v.entry.metadata_location));
    CHECK(env.exists(r.entry.metadata_location));
    // a wrong uuid requirement, an unknown update, a bad representation
    json bad_req = json::array({json{{"type", "assert-view-uuid"}, {"uuid", "00000000-0000-4000-8000-000000000000"}}});
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->replace_view("tbk", ns({"n"}), "v", bad_req, json::array())); }),
             409);
    CHECK_EQ(status_of([&] {
                 sync_wait(env.catalog->replace_view("tbk", ns({"n"}), "v", json::array(),
                                                     json::array({json::parse(R"({"action":"add-snapshot"})")})));
             }),
             406);
    CHECK_EQ(status_of([&] {
                 sync_wait(env.catalog->replace_view(
                     "tbk", ns({"n"}), "v", json::array(),
                     json::array({json::parse(
                         R"({"action":"add-view-version","view-version":{"representations":[{"type":"sql"}]}})")})));
             }),
             400);
    // rename, then drop: the tombstone frees the name for a table
    sync_wait(env.catalog->rename_view("tbk", ns({"n"}), "v", ns({"n"}), "w"));
    CHECK(!sync_wait(env.catalog->view_exists("tbk", ns({"n"}), "v")));
    auto w = sync_wait(env.catalog->load_view("tbk", ns({"n"}), "w"));
    CHECK_EQ(w.entry.view_id, v.entry.view_id);
    CHECK_EQ(w.entry.generation, uint64_t(2));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->rename_view("tbk", ns({"n"}), "w", ns({"zz"}), "w")); }), 404);
    sync_wait(env.catalog->drop_view("tbk", ns({"n"}), "w"));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->drop_view("tbk", ns({"n"}), "w")); }), 404);
    CHECK(sync_wait(env.catalog->list_views("tbk", ns({"n"}), PageCursor{})).items.empty());
    env.create_table("w");
    sync_wait(env.catalog->drop_table("tbk", ns({"n"}), "w"));
    sync_wait(env.catalog->drop_table("tbk", ns({"n"}), "t"));
    // a namespace with only tombstones can go
    sync_wait(env.catalog->drop_namespace("tbk", ns({"n"})));
}

}  // namespace

TEST(tables_catalog_store_suite_object) {
    auto backend = std::make_shared<storage::MemoryBackend>();
    catalog_store_suite::run([backend] { return std::make_shared<ObjectCatalogStore>(backend); });
}

TEST(tables_views_lifecycle_object_backing) {
    Env env;
    view_lifecycle(env);
}

TEST(tables_view_metadata_model) {
    using namespace lights3::tables::iceberg;
    CreateViewInput in;
    in.name = "v";
    in.schema = json::parse(R"({"type":"struct","fields":[{"id":1,"name":"x","required":false,"type":"int"}]})");
    in.view_version = json::parse(R"({"representations":[{"type":"sql","sql":"select 1","dialect":"trino"}]})");
    in.location = "s3://b/v";
    in.view_uuid = "0e7c4f3a-0000-4000-8000-000000000009";
    in.now_ms = 5;
    json md = initial_view_metadata(in);
    validate_view_metadata(md);
    CHECK_EQ(md["schemas"][0]["schema-id"].get<int>(), 0);
    CHECK_EQ(md["versions"][0]["default-namespace"].size(), size_t(0));
    // the round trip through the parser and the size limit
    CHECK_EQ(parse_and_validate_view(md.dump(), 1 << 20)["view-uuid"].get<std::string>(), in.view_uuid);
    CHECK_EQ(status_of([&] { parse_and_validate_view(md.dump(), 10); }), 400);
    json v2 = md;
    v2["format-version"] = 2;
    CHECK_EQ(status_of([&] { validate_view_metadata(v2); }), 406);
    json noschema = md;
    noschema["versions"][0]["schema-id"] = 7;
    CHECK_EQ(status_of([&] { validate_view_metadata(noschema); }), 400);
    // updates: add-schema + add-view-version (schema-id -1 = last added), version log grows
    json ups = json::array({
        json::parse(
            R"({"action":"add-schema","schema":{"type":"struct","fields":[{"id":1,"name":"y","required":false,"type":"string"}]}})"),
        json::parse(
            R"({"action":"add-view-version","view-version":{"representations":[{"type":"sql","sql":"select 'y'","dialect":"trino"}],"schema-id":-1}})"),
        json::parse(R"({"action":"set-current-view-version","view-version-id":-1})"),
        json::parse(R"({"action":"set-location","location":"s3://b/v2"})"),
        json::parse(R"({"action":"remove-properties","removals":["nope"]})"),
    });
    json next = apply_view_updates(md, ups, 9);
    CHECK_EQ(next["schemas"].size(), size_t(2));
    CHECK_EQ(next["schemas"][1]["schema-id"].get<int>(), 1);
    CHECK_EQ(next["versions"][1]["schema-id"].get<int>(), 1);
    CHECK_EQ(current_view_version_id(next), 2);
    CHECK_EQ(next["version-log"].size(), size_t(2));
    CHECK_EQ(next["location"].get<std::string>(), "s3://b/v2");
    // a version id that already exists, an unknown version, uuid reassignment
    CHECK_EQ(
        status_of([&] {
            apply_view_updates(
                next,
                json::array({json::parse(
                    R"({"action":"add-view-version","view-version":{"version-id":1,"representations":[{"type":"sql","sql":"x","dialect":"d"}]}})")}),
                1);
        }),
        409);
    CHECK_EQ(status_of([&] {
                 apply_view_updates(
                     next, json::array({json::parse(R"({"action":"set-current-view-version","view-version-id":9})")}),
                     1);
             }),
             400);
    CHECK_EQ(
        status_of([&] {
            apply_view_updates(
                next,
                json::array({json::parse(R"({"action":"assign-uuid","uuid":"0e7c4f3a-0000-4000-8000-000000000001"})")}),
                1);
        }),
        409);
    check_view_requirements(md, json::array({json{{"type", "assert-view-uuid"}, {"uuid", in.view_uuid}}}));
    CHECK_EQ(status_of([&] { check_view_requirements(md, json::array({json{{"type", "assert-ref-snapshot-id"}}})); }),
             400);
}

TEST(tables_plan_compaction_candidates) {
    Env env;
    env.create_table("t");
    env.install_snapshot("n/t/metadata/ml.avro");
    env.commit(1, "n/t/metadata/ml.avro");
    PlannerOptions o;
    o.safety_window_sec = 0;
    auto plan = sync_wait(plan_table(*env.catalog, "tbk", ns({"n"}), "t", o, now_unix()));
    // f1 (100 B) and f2 (200 B) share the directory and the sort order: one bin
    CHECK_EQ(plan.compaction_candidates.size(), size_t(1));
    auto& c = plan.compaction_candidates[0];
    CHECK_EQ(c.partition, "n/t/data");
    CHECK_EQ(c.sort_order_id, 0);
    CHECK_EQ(c.files.size(), size_t(2));
    CHECK_EQ(c.bytes, int64_t(300));
    CHECK(!c.row_level_required);
    json j = plan.to_json();
    CHECK_EQ(j["compaction-candidates"][0]["files"].size(), size_t(2));
    auto back = MaintenancePlan::from_json(j);
    CHECK(back && back->compaction_candidates.size() == 1 && back->compaction_candidates[0].bytes == 300);
    // a target below the file sizes produces no bin (nothing is "small"); off = none
    o.target_file_size_bytes = 100;
    CHECK(sync_wait(plan_table(*env.catalog, "tbk", ns({"n"}), "t", o, now_unix())).compaction_candidates.empty());
    o.target_file_size_bytes = 0;
    CHECK(sync_wait(plan_table(*env.catalog, "tbk", ns({"n"}), "t", o, now_unix())).compaction_candidates.empty());
    // the table property sets the target
    auto loaded = sync_wait(env.catalog->load_table("tbk", ns({"n"}), "t"));
    json props = loaded.metadata.value("properties", json::object());
    props["write.target-file-size-bytes"] = "1024";
    auto eff = resolve_maintenance(env.cfg, std::nullopt, props);
    CHECK_EQ(eff.planner.target_file_size_bytes, int64_t(1024));
    // an unreadable manifest is a note, not a failure
    env.put("n/t/metadata/m-1.avro", "junk");
    auto p2 = sync_wait(plan_table(*env.catalog, "tbk", ns({"n"}), "t", PlannerOptions{}, now_unix()));
    CHECK(p2.compaction_candidates.empty());
    CHECK(!p2.notes.empty());
}

TEST(tables_config_catalog_backing) {
    const char* base = R"(
backends:
  - name: local
    type: localfs
    params: {root: /tmp/x, staging: /tmp/y}
buckets:
  default_backend: local
tables:
  enabled: true
)";
    auto cfg = Config::from_string(base);
    CHECK_EQ(cfg.tables.catalog_backing, "object");
    bool threw = false;
    try {
        Config::from_string(std::string(base) + "  catalog_backing: duostore\n");
    } catch (const std::exception& e) {
        threw = std::string(e.what()).find("duostore backend") != std::string::npos;
    }
    CHECK(threw);
    threw = false;
    try {
        Config::from_string(std::string(base) + "  catalog_backing: nope\n");
    } catch (const std::exception&) {
        threw = true;
    }
    CHECK(threw);
    // tables off: any backing parses (it is inert)
    auto off = Config::from_string(R"(
backends:
  - name: local
    type: localfs
    params: {root: /tmp/x, staging: /tmp/y}
buckets:
  default_backend: local
tables:
  enabled: false
  catalog_backing: duostore
)");
    CHECK_EQ(off.tables.catalog_backing, "duostore");
}

#ifdef LIGHTS3_DUOSTORE
namespace {

struct RocksMetaHolder {
    backend_suite::TmpDir tmp;
    std::unique_ptr<storage::duostore::RocksMetaStore> meta;
    RocksMetaHolder() {
        meta = std::make_unique<storage::duostore::RocksMetaStore>(
            storage::duostore::RocksMetaOptions{(tmp.path / "meta").string(), /*sync=*/false, 8ull << 20});
    }
    ~RocksMetaHolder() { meta->close(); }
};

}  // namespace

TEST(tables_catalog_store_suite_duostore_rocksdb) {
    RocksMetaHolder h;
    catalog_store_suite::run([&] { return std::make_shared<DuoMetaCatalogStore>(*h.meta); });
}

TEST(tables_views_lifecycle_duostore_backing) {
    RocksMetaHolder h;
    Env env(std::make_shared<DuoMetaCatalogStore>(*h.meta));
    view_lifecycle(env);
}

// The transactional backing: a commit lands the record and the pointer together, so no
// STAGED record ever exists, replay finds COMMITTED, and concurrent writers still leave
// exactly one winner
TEST(tables_atomic_commit_on_duostore_backing) {
    RocksMetaHolder h;
    auto store = std::make_shared<DuoMetaCatalogStore>(*h.meta);
    CHECK(store->supports_atomic_commit());
    Env env(store);
    auto t = env.create_table("t");
    env.install_snapshot("n/t/metadata/ml.avro");
    auto c1 = env.commit(1, "n/t/metadata/ml.avro", "c-1");
    CHECK_EQ(c1.entry.generation, uint64_t(2));
    auto rec = sync_wait(store->get_commit("tbk", t.entry.table_id, "c-1"));
    CHECK(rec && rec->value.status == "COMMITTED");
    auto replay = env.commit(1, "n/t/metadata/ml.avro", "c-1");
    CHECK_EQ(replay.entry.generation, uint64_t(2));
    CHECK_EQ(replay.entry.metadata_location, c1.entry.metadata_location);
    // every record of the table is COMMITTED: the diagnostics never see a gap
    auto d = sync_wait(env.catalog->diagnose("tbk", ns({"n"}), "t"));
    for (auto& c : d.commits) CHECK(c.state == CommitState::Committed);
    // 20 concurrent commits asserting main is unset: one winner, 19 conflicts
    auto attempt = [&](int i) -> Task<int> {
        CommitRequest c;
        json s;
        s["snapshot-id"] = 100 + i;
        s["sequence-number"] = 2;
        s["timestamp-ms"] = 2000 + i;
        s["manifest-list"] = "s3://tbk/n/t/metadata/ml.avro";
        s["summary"] = json::object({{"operation", "append"}});
        c.requirements = json::array(
            {json::parse(R"({"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":1})")});
        c.updates = json::array(
            {json::object({{"action", "add-snapshot"}, {"snapshot", s}}),
             json::parse(R"({"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":)" +
                         std::to_string(100 + i) + "}")});
        try {
            co_await env.catalog->commit_table("tbk", ns({"n"}), "t", c, {});
        } catch (const RestError& e) {
            co_return e.status;
        }
        co_return 200;
    };
    std::vector<Task<int>> tasks;
    for (int i = 0; i < 20; ++i) tasks.push_back(attempt(i));
    auto res = sync_wait(when_all(std::move(tasks)));
    int ok = 0, conflict = 0;
    for (int s : res) {
        if (s == 200) ++ok;
        if (s == 409) ++conflict;
    }
    CHECK_EQ(ok, 1);
    CHECK_EQ(conflict, 19);
    CHECK_EQ(sync_wait(env.catalog->load_table("tbk", ns({"n"}), "t")).entry.generation, uint64_t(3));
    CHECK_EQ(sync_wait(store->list_commits("tbk", t.entry.table_id)).size(), size_t(2));
    // the pointer update endpoint and the maintenance planner work on this backing too
    auto plan = sync_wait(plan_table(*env.catalog, "tbk", ns({"n"}), "t", PlannerOptions{}, now_unix()));
    CHECK(!plan.manual_review);
    // export / import: the object backing receives the same keys
    auto raw = sync_wait(store->export_raw("tbk"));
    CHECK(raw.size() >= 4);
    auto other = std::make_shared<ObjectCatalogStore>(env.backend);
    for (auto& e : raw) sync_wait(other->import_raw("tbk", e));
    auto moved = sync_wait(other->get_table("tbk", ns({"n"}), "t"));
    CHECK(moved && moved->value.generation == 3);
    CHECK_EQ(sync_wait(other->list_commits("tbk", t.entry.table_id)).size(), size_t(2));
}
#endif
