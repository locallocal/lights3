// Catalog + ObjectCatalogStore on MemoryBackend (docs/s3-tables-design.md §4–§5,
// docs/s3-tables/step-1-catalog-core.md §15): namespaces, tables, the commit protocol,
// idempotent replay, crash windows, rename, drop
#include <nlohmann/json.hpp>
#include <set>

#include "core/fault.h"
#include "storage/memory/memory_backend.h"
#include "tables/catalog.h"
#include "tables/iceberg/metadata.h"
#include "tables/object_catalog_store.h"
#include "tables/rest_error.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::tables;
using nlohmann::json;

namespace {

struct FaultReset {
    ~FaultReset() { fault::reset(); }
};

struct Env {
    std::shared_ptr<storage::MemoryBackend> backend = std::make_shared<storage::MemoryBackend>();
    std::shared_ptr<TableBucketStore> buckets;
    std::shared_ptr<ObjectCatalogStore> store;
    std::shared_ptr<Catalog> catalog;
    TablesConfig cfg;

    Env() {
        cfg.enabled = true;
        std::map<std::string, std::shared_ptr<storage::IStorageBackend>> bmap{{"mem", backend}};
        BucketsConfig bcfg;
        bcfg.default_backend = "mem";
        auto router = storage::BucketRouter::build(bcfg, std::move(bmap));
        buckets = sync_wait(TableBucketStore::load(backend));
        store = std::make_shared<ObjectCatalogStore>(backend);
        catalog = std::make_shared<Catalog>(store, buckets, router, nullptr, cfg, MetricsScope{});
        sync_wait(backend->create_bucket("tbk"));
        sync_wait(catalog->enable_bucket("tbk"));
    }
    // a second catalog instance sharing the same backend (another gateway)
    std::shared_ptr<Catalog> peer() {
        std::map<std::string, std::shared_ptr<storage::IStorageBackend>> bmap{{"mem", backend}};
        BucketsConfig bcfg;
        bcfg.default_backend = "mem";
        auto router = storage::BucketRouter::build(bcfg, std::move(bmap));
        auto b = sync_wait(TableBucketStore::load(backend));
        return std::make_shared<Catalog>(std::make_shared<ObjectCatalogStore>(backend), b, router, nullptr, cfg,
                                         MetricsScope{});
    }
    void put(const std::string& key, const std::string& body = "x") {
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
};

CreateTableRequest table_req(const std::string& name) {
    CreateTableRequest r;
    r.name = name;
    r.schema = json::parse(R"({"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]})");
    return r;
}

Levels ns(std::initializer_list<const char*> l) { return Levels(l.begin(), l.end()); }

CommitRequest append_commit(int64_t snap, int64_t seq, const std::string& manifest, const std::string& commit_id = "") {
    CommitRequest c;
    c.commit_id = commit_id;
    c.requirements = json::array();
    json s;
    s["snapshot-id"] = snap;
    s["sequence-number"] = seq;
    s["timestamp-ms"] = 1000 + snap;
    s["manifest-list"] = "s3://tbk/" + manifest;
    s["summary"] = json::object({{"operation", "append"}});
    c.updates = json::array({json::object({{"action", "add-snapshot"}, {"snapshot", s}}),
                             json::parse(R"({"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":)" +
                                         std::to_string(snap) + "}")});
    return c;
}

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

}  // namespace

TEST(tables_catalog_bucket_enable_disable) {
    Env env;
    auto snap = env.buckets->snapshot();
    CHECK(env.catalog->table_bucket(snap, "tbk") != nullptr);
    CHECK(env.catalog->table_bucket(snap, "other") == nullptr);
    // idempotent
    sync_wait(env.catalog->enable_bucket("tbk"));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->enable_bucket("missing")); }), 404);
    // objects under the reserved prefix block enablement
    sync_wait(env.backend->create_bucket("dirty"));
    env.backend.get();
    {
        storage::ObjectMeta meta;
        http::StringBodyReader r("x");
        sync_wait(env.backend->put_object("dirty", ".lights3-table/x", std::move(meta), r));
    }
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->enable_bucket("dirty")); }), 400);
    sync_wait(env.catalog->create_namespace("tbk", ns({"a"}), {}));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->disable_bucket("tbk")); }), 409);
    sync_wait(env.catalog->drop_namespace("tbk", ns({"a"})));
    sync_wait(env.catalog->disable_bucket("tbk"));
    CHECK(env.catalog->table_bucket(env.buckets->snapshot(), "tbk") == nullptr);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->create_namespace("tbk", ns({"a"}), {})); }), 404);
}

TEST(tables_catalog_namespaces_evidence_and_paging) {
    Env env;
    auto e = sync_wait(env.catalog->create_namespace("tbk", ns({"sales", "eu"}), {{"owner", "me"}}));
    CHECK_EQ(e.properties.at("owner"), "me");
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->create_namespace("tbk", ns({"sales", "eu"}), {})); }), 409);
    // parent exists by evidence only
    auto parent = sync_wait(env.catalog->load_namespace("tbk", ns({"sales"})));
    CHECK(parent.has_value());
    CHECK(!parent->explicit_entry);
    CHECK(!sync_wait(env.catalog->load_namespace("tbk", ns({"nope"}))).has_value());
    // listing children / paging
    sync_wait(env.catalog->create_namespace("tbk", ns({"sales", "us"}), {}));
    sync_wait(env.catalog->create_namespace("tbk", ns({"hr"}), {}));
    PageCursor c;
    c.limit = 1;
    auto p1 = sync_wait(env.catalog->list_namespaces("tbk", {}, c));
    CHECK_EQ(p1.items.size(), size_t(1));
    CHECK_EQ(ns_path(p1.items[0]), "hr");
    CHECK(!p1.next_after.empty());
    c.after = p1.next_after;
    auto p2 = sync_wait(env.catalog->list_namespaces("tbk", {}, c));
    CHECK_EQ(ns_path(p2.items[0]), "sales");
    CHECK(p2.next_after.empty());
    auto kids = sync_wait(env.catalog->list_namespaces("tbk", ns({"sales"}), PageCursor{}));
    CHECK_EQ(kids.items.size(), size_t(2));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->list_namespaces("tbk", ns({"nope"}), PageCursor{})); }), 404);
    // properties
    auto res = sync_wait(env.catalog->update_namespace_properties("tbk", ns({"sales", "eu"}), {"owner", "gone"},
                                                                  {{"team", "x"}}));
    CHECK_EQ(res.removed.size(), size_t(1));
    CHECK_EQ(res.missing.size(), size_t(1));
    CHECK_EQ(res.updated.size(), size_t(1));
    CHECK_EQ(status_of([&] {
                 sync_wait(env.catalog->update_namespace_properties("tbk", ns({"sales", "eu"}), {"team"}, {{"team", "y"}}));
             }),
             422);
    // an evidence-only namespace becomes explicit when properties are set
    sync_wait(env.catalog->update_namespace_properties("tbk", ns({"sales"}), {}, {{"p", "q"}}));
    CHECK(sync_wait(env.catalog->load_namespace("tbk", ns({"sales"})))->explicit_entry);
    // drop: non-empty 409, then leaves
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->drop_namespace("tbk", ns({"sales"}))); }), 409);
    sync_wait(env.catalog->drop_namespace("tbk", ns({"sales", "eu"})));
    sync_wait(env.catalog->drop_namespace("tbk", ns({"sales", "us"})));
    sync_wait(env.catalog->drop_namespace("tbk", ns({"sales"})));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->drop_namespace("tbk", ns({"sales"}))); }), 404);
    // bad identifiers
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->create_namespace("tbk", ns({"Bad"}), {})); }), 400);
}

TEST(tables_catalog_create_load_list_drop) {
    Env env;
    sync_wait(env.catalog->create_namespace("tbk", ns({"n"}), {}));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->create_table("tbk", ns({"nope"}), table_req("t"), {})); }), 404);
    auto t = sync_wait(env.catalog->create_table("tbk", ns({"n"}), table_req("t"), {}));
    CHECK_EQ(t.entry.generation, uint64_t(1));
    CHECK_EQ(t.entry.location, "s3://tbk/n/t");
    CHECK_EQ(t.entry.metadata_location.rfind(".lights3-table/n/t/metadata/00001-", 0), size_t(0));
    CHECK(env.exists(t.entry.metadata_location));
    CHECK_EQ(t.metadata["table-uuid"].get<std::string>(), t.entry.table_uuid);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->create_table("tbk", ns({"n"}), table_req("t"), {})); }), 409);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->create_table("tbk", ns({"n"}), table_req("Bad"), {})); }), 400);
    auto l = sync_wait(env.catalog->load_table("tbk", ns({"n"}), "t"));
    CHECK_EQ(l.entry.table_id, t.entry.table_id);
    CHECK(sync_wait(env.catalog->table_exists("tbk", ns({"n"}), "t")));
    CHECK(!sync_wait(env.catalog->table_exists("tbk", ns({"n"}), "u")));
    auto page = sync_wait(env.catalog->list_tables("tbk", ns({"n"}), PageCursor{}));
    CHECK_EQ(page.items.size(), size_t(1));
    // explicit locations: inside the bucket, no nesting with another table
    auto r2 = table_req("u");
    r2.location = "s3://tbk/custom/u/";
    auto u = sync_wait(env.catalog->create_table("tbk", ns({"n"}), r2, {}));
    CHECK_EQ(u.entry.location, "s3://tbk/custom/u");
    auto r3 = table_req("v");
    r3.location = "s3://tbk/custom/u/inner";
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->create_table("tbk", ns({"n"}), r3, {})); }), 409);
    auto r4 = table_req("w");
    r4.location = "s3://elsewhere/w";
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->create_table("tbk", ns({"n"}), r4, {})); }), 400);
    auto r5 = table_req("x");
    r5.properties["format-version"] = "3";
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->create_table("tbk", ns({"n"}), r5, {})); }), 406);
    auto r6 = table_req("y");
    r6.properties["format-version"] = "1";
    auto y = sync_wait(env.catalog->create_table("tbk", ns({"n"}), r6, {}));
    CHECK_EQ(y.entry.format_version, 1);
    CHECK(!y.metadata.contains("last-sequence-number"));
    // drop hides the table, keeps a tombstone, and the name can be reused
    sync_wait(env.catalog->drop_table("tbk", ns({"n"}), "t"));
    CHECK(!sync_wait(env.catalog->table_exists("tbk", ns({"n"}), "t")));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->load_table("tbk", ns({"n"}), "t")); }), 404);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->drop_table("tbk", ns({"n"}), "t")); }), 404);
    auto stored = sync_wait(env.store->get_table("tbk", ns({"n"}), "t"));
    CHECK(stored && stored->value.state == TableState::Deleted);
    auto again = sync_wait(env.catalog->create_table("tbk", ns({"n"}), table_req("t"), {}));
    CHECK(again.entry.table_id != t.entry.table_id);
    CHECK_EQ(sync_wait(env.catalog->list_tables("tbk", ns({"n"}), PageCursor{})).items.size(), size_t(3));
    // a namespace holding only tombstones can be dropped
    sync_wait(env.catalog->create_namespace("tbk", ns({"m"}), {}));
    sync_wait(env.catalog->create_table("tbk", ns({"m"}), table_req("z"), {}));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->drop_namespace("tbk", ns({"m"}))); }), 409);
    sync_wait(env.catalog->drop_table("tbk", ns({"m"}), "z"));
    sync_wait(env.catalog->drop_namespace("tbk", ns({"m"})));
    CHECK(!sync_wait(env.store->get_table("tbk", ns({"m"}), "z")).has_value());
}

TEST(tables_catalog_commit_protocol) {
    Env env;
    sync_wait(env.catalog->create_namespace("tbk", ns({"n"}), {}));
    auto t = sync_wait(env.catalog->create_table("tbk", ns({"n"}), table_req("t"), {}));
    env.put("n/t/metadata/snap-1.avro");
    // missing manifest list -> 409
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", append_commit(1, 1, "n/t/metadata/missing.avro"), {})); }), 409);
    int64_t usage_objects = 0, usage_bytes = 0;
    CommitHooks hooks;
    hooks.note_usage = [&](std::string_view, int64_t o, int64_t b) {
        usage_objects += o;
        usage_bytes += b;
    };
    auto c1 = sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", append_commit(1, 1, "n/t/metadata/snap-1.avro", "c-1"), hooks));
    CHECK_EQ(c1.entry.generation, uint64_t(2));
    CHECK(c1.entry.version_token != t.entry.version_token);
    CHECK_EQ(iceberg::current_snapshot_id(c1.metadata), 1);
    CHECK_EQ(c1.metadata["metadata-log"].size(), size_t(1));
    CHECK_EQ(c1.entry.metadata_location.rfind(".lights3-table/n/t/metadata/00002-", 0), size_t(0));
    CHECK_EQ(usage_objects, 1);
    CHECK(usage_bytes > 0);
    // the record is COMMITTED
    auto rec = sync_wait(env.store->get_commit("tbk", t.entry.table_id, "c-1"));
    CHECK(rec && rec->value.status == "COMMITTED");
    // replay: same payload -> same result, nothing written; different payload -> 409
    auto replay = sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", append_commit(1, 1, "n/t/metadata/snap-1.avro", "c-1"), hooks));
    CHECK_EQ(replay.entry.generation, uint64_t(2));
    CHECK_EQ(replay.entry.metadata_location, c1.entry.metadata_location);
    CHECK_EQ(usage_objects, 1);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", append_commit(2, 2, "n/t/metadata/snap-1.avro", "c-1"), hooks)); }), 409);
    // stale requirement -> 409
    CommitRequest stale;
    stale.requirements = json::array({json::parse(R"({"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null})")});
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", stale, {})); }), 409);
    // a commit with no id is recorded too and advances the generation
    env.put("n/t/metadata/snap-2.avro");
    auto c2 = sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", append_commit(2, 2, "n/t/metadata/snap-2.avro"), {}));
    CHECK_EQ(c2.entry.generation, uint64_t(3));
    CHECK_EQ(sync_wait(env.store->list_commits("tbk", t.entry.table_id)).size(), size_t(2));
    // pointer-only update: wrong token 409, outside the metadata dir 400, valid file OK
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->update_metadata_location("tbk", ns({"n"}), "t", "s3://tbk/" + c1.entry.metadata_location, "t-bogus")); }), 409);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->update_metadata_location("tbk", ns({"n"}), "t", "s3://tbk/n/t/metadata/snap-2.avro", c2.entry.version_token)); }), 400);
    json md3 = c2.metadata;
    md3["properties"]["manual"] = "yes";
    std::string k3 = ".lights3-table/n/t/metadata/00004-manual.metadata.json";
    env.put(k3, iceberg::canonical(md3));
    auto e3 = sync_wait(env.catalog->update_metadata_location("tbk", ns({"n"}), "t", "s3://tbk/" + k3, c2.entry.version_token));
    CHECK_EQ(e3.generation, uint64_t(4));
    CHECK_EQ(e3.metadata_location, k3);
    auto l3 = sync_wait(env.catalog->load_table("tbk", ns({"n"}), "t"));
    CHECK_EQ(l3.metadata["properties"]["manual"].get<std::string>(), "yes");
}

TEST(tables_catalog_concurrent_commits_single_winner) {
    Env env;
    sync_wait(env.catalog->create_namespace("tbk", ns({"n"}), {}));
    sync_wait(env.catalog->create_table("tbk", ns({"n"}), table_req("t"), {}));
    env.put("n/t/metadata/snap.avro");
    // every commit asserts main is unset: after the first wins the others fail the requirement
    auto make = [&](int i) -> Task<int> {
        auto c = append_commit(100 + i, 1, "n/t/metadata/snap.avro", "c-" + std::to_string(i));
        c.requirements = json::array({json::parse(R"({"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null})")});
        try {
            co_await env.catalog->commit_table("tbk", ns({"n"}), "t", c, {});
        } catch (const RestError& e) {
            co_return e.status;
        }
        co_return 200;
    };
    std::vector<Task<int>> tasks;
    for (int i = 0; i < 20; ++i) tasks.push_back(make(i));
    auto res = sync_wait(when_all(std::move(tasks)));
    int ok = 0, conflict = 0;
    for (int s : res) {
        if (s == 200) ++ok;
        if (s == 409) ++conflict;
    }
    CHECK_EQ(ok, 1);
    CHECK_EQ(conflict, 19);
    auto l = sync_wait(env.catalog->load_table("tbk", ns({"n"}), "t"));
    CHECK_EQ(l.entry.generation, uint64_t(2));
    // two gateways sharing the state: the peer sees the commit and a stale CAS from the
    // first gateway's cached view is impossible because every commit re-reads the pointer
    auto peer = env.peer();
    env.put("n/t/metadata/snap-2.avro");
    auto c = sync_wait(peer->commit_table("tbk", ns({"n"}), "t", append_commit(200, 2, "n/t/metadata/snap-2.avro"), {}));
    CHECK_EQ(c.entry.generation, uint64_t(3));
    CHECK_EQ(sync_wait(env.catalog->load_table("tbk", ns({"n"}), "t")).entry.generation, uint64_t(3));
}

TEST(tables_catalog_crash_windows_and_replay) {
    FaultReset guard;
    Env env;
    sync_wait(env.catalog->create_namespace("tbk", ns({"n"}), {}));
    auto t = sync_wait(env.catalog->create_table("tbk", ns({"n"}), table_req("t"), {}));
    env.put("n/t/metadata/snap.avro");
    // crash after staging (design §5.4 row 2): pointer unchanged, STAGED record present
    fault::arm("tables.commit.after_stage:1");
    auto req = append_commit(1, 1, "n/t/metadata/snap.avro", "c-stage");
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", req, {})); }), 500);
    auto p = sync_wait(env.catalog->table_pointer("tbk", ns({"n"}), "t"));
    CHECK_EQ(p->value.generation, uint64_t(1));
    auto rec = sync_wait(env.store->get_commit("tbk", t.entry.table_id, "c-stage"));
    CHECK(rec && rec->value.status == "STAGED");
    // replay of the same commit continues from the staged record and succeeds
    auto done = sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", req, {}));
    CHECK_EQ(done.entry.generation, uint64_t(2));
    CHECK_EQ(done.entry.version_token, rec->value.new_token);
    CHECK_EQ(sync_wait(env.store->get_commit("tbk", t.entry.table_id, "c-stage"))->value.status, "COMMITTED");
    // crash after the CAS (row 3): externally committed, record still STAGED
    env.put("n/t/metadata/snap-2.avro");
    fault::arm("tables.commit.after_cas:1");
    auto req2 = append_commit(2, 2, "n/t/metadata/snap-2.avro", "c-cas");
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", req2, {})); }), 500);
    p = sync_wait(env.catalog->table_pointer("tbk", ns({"n"}), "t"));
    CHECK_EQ(p->value.generation, uint64_t(3));
    CHECK_EQ(sync_wait(env.store->get_commit("tbk", t.entry.table_id, "c-cas"))->value.status, "STAGED");
    // replay finalizes and returns the committed state without a new generation
    auto fin = sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", req2, {}));
    CHECK_EQ(fin.entry.generation, uint64_t(3));
    CHECK_EQ(sync_wait(env.store->get_commit("tbk", t.entry.table_id, "c-cas"))->value.status, "COMMITTED");
    // a staged record superseded by another commit is refused on replay
    fault::arm("tables.commit.after_stage:1");
    env.put("n/t/metadata/snap-3.avro");
    auto req3 = append_commit(3, 3, "n/t/metadata/snap-3.avro", "c-super");
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", req3, {})); }), 500);
    sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", append_commit(4, 3, "n/t/metadata/snap-3.avro"), {}));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", req3, {})); }), 409);
}

TEST(tables_catalog_rename) {
    Env env;
    sync_wait(env.catalog->create_namespace("tbk", ns({"a"}), {}));
    sync_wait(env.catalog->create_namespace("tbk", ns({"b"}), {}));
    auto t = sync_wait(env.catalog->create_table("tbk", ns({"a"}), table_req("t"), {}));
    sync_wait(env.catalog->create_table("tbk", ns({"b"}), table_req("taken"), {}));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->rename_table("tbk", ns({"a"}), "t", ns({"b"}), "taken")); }), 409);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->rename_table("tbk", ns({"a"}), "t", ns({"zz"}), "t")); }), 404);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->rename_table("tbk", ns({"a"}), "nope", ns({"b"}), "t")); }), 404);
    // a failed rename leaves the source active and no intent behind
    CHECK(sync_wait(env.catalog->table_exists("tbk", ns({"a"}), "t")));
    CHECK(sync_wait(env.store->list_renames("tbk")).empty());
    sync_wait(env.catalog->rename_table("tbk", ns({"a"}), "t", ns({"b"}), "t2"));
    CHECK(!sync_wait(env.catalog->table_exists("tbk", ns({"a"}), "t")));
    auto moved = sync_wait(env.catalog->load_table("tbk", ns({"b"}), "t2"));
    CHECK_EQ(moved.entry.table_id, t.entry.table_id);
    CHECK_EQ(moved.entry.location, "s3://tbk/a/t");
    CHECK_EQ(moved.entry.metadata_location, t.entry.metadata_location);
    CHECK(sync_wait(env.store->list_renames("tbk")).empty());
    auto tomb = sync_wait(env.store->get_table("tbk", ns({"a"}), "t"));
    CHECK(tomb && tomb->value.state == TableState::Deleted);
    // commits after the rename write under the new name
    env.put("a/t/metadata/snap.avro");
    auto c = sync_wait(env.catalog->commit_table("tbk", ns({"b"}), "t2", append_commit(1, 1, "a/t/metadata/snap.avro"), {}));
    CHECK_EQ(c.entry.metadata_location.rfind(".lights3-table/b/t2/metadata/00002-", 0), size_t(0));
    // a table stuck in RENAMING is unavailable to readers and writers
    TableEntry stuck = moved.entry;
    stuck.state = TableState::Renaming;
    stuck.rename_id = "x";
    sync_wait(env.store->put_table("tbk", ns({"b"}), "t2", stuck, {}));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->load_table("tbk", ns({"b"}), "t2")); }), 503);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->drop_table("tbk", ns({"b"}), "t2")); }), 503);
}

TEST(tables_catalog_register_and_entity_identity) {
    Env env;
    sync_wait(env.catalog->create_namespace("tbk", ns({"n"}), {}));
    // an engine-written metadata file anywhere in the bucket can be registered
    iceberg::CreateTableInput in;
    in.name = "ext";
    in.schema = json::parse(R"({"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]})");
    in.location = "s3://tbk/legacy/ext";
    in.table_uuid = "11111111-2222-4333-8444-555555555555";
    in.now_ms = 1;
    json md = iceberg::initial_metadata(in);
    env.put("legacy/ext/metadata/v1.metadata.json", iceberg::canonical(md));
    auto r = sync_wait(env.catalog->register_table("tbk", ns({"n"}), "ext", "s3://tbk/legacy/ext/metadata/v1.metadata.json", {}));
    CHECK_EQ(r.entry.table_uuid, in.table_uuid);
    CHECK_EQ(r.entry.location, "s3://tbk/legacy/ext");
    CHECK_EQ(r.entry.metadata_location.rfind(".lights3-table/n/ext/metadata/00001-", 0), size_t(0));
    CHECK(env.exists("legacy/ext/metadata/v1.metadata.json"));
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->register_table("tbk", ns({"n"}), "ext2", "s3://tbk/legacy/nope.json", {})); }), 404);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->register_table("tbk", ns({"n"}), "ext2", "s3://other/x.json", {})); }), 400);
    CHECK_EQ(status_of([&] { sync_wait(env.catalog->register_table("tbk", ns({"n"}), "ext2", "s3://tbk/x.metadata.json.gz", {})); }), 406);
    // a catalog entry copied to another key does not describe itself -> internal error
    auto raw = sync_wait(env.backend->get_object(".sys", ObjectCatalogStore::tbl_key("tbk", ns({"n"}), "ext"), std::nullopt));
    std::string body;
    std::byte buf[4096];
    for (;;) {
        size_t n = sync_wait(raw.body->read(std::span(buf)));
        if (!n) break;
        body.append(reinterpret_cast<const char*>(buf), n);
    }
    {
        storage::ObjectMeta meta;
        http::StringBodyReader rd(body);
        sync_wait(env.backend->put_object(".sys", ObjectCatalogStore::tbl_key("tbk", ns({"n"}), "copy"), std::move(meta), rd));
    }
    CHECK_THROWS_S3(sync_wait(env.store->get_table("tbk", ns({"n"}), "copy")), s3::S3ErrorCode::InternalError);
    // bucket state cleanup
    CHECK(!sync_wait(env.catalog->catalog_empty("tbk")));
    sync_wait(env.catalog->forget_bucket("tbk"));
    CHECK(sync_wait(env.catalog->catalog_empty("tbk")));
    CHECK(env.catalog->table_bucket(env.buckets->snapshot(), "tbk") == nullptr);
}
