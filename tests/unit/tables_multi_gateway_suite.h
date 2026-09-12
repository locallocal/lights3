// Two-gateway S3 Tables suite (docs/s3-tables-design.md §5.5, docs/s3-tables/step-5-multi-gateway-docs.md
// §2): two independent catalog stacks ("gateways" A and B, each its own TableBucketStore /
// ObjectCatalogStore / Catalog) over two IStorageBackend handles that share one medium.
// What converges across gateways is decided by the medium's PutCondition CAS alone:
// concurrent commits leave exactly one winner, a rename interrupted on A is completed by
// B's next write, a drop on A is a 404 on B. Included by the memory (same instance as
// both handles), redis and tikv test files; the duostore variants share meta and one
// data engine object the way multi_gateway_suite.h does
#pragma once

#include <functional>
#include <memory>
#include <nlohmann/json.hpp>
#include <string>
#include <utility>
#include <vector>

#include "core/fault.h"
#include "core/task.h"
#include "storage/backend.h"
#include "storage/bucket_router.h"
#include "tables/catalog.h"
#include "tables/object_catalog_store.h"
#include "tables/rest_error.h"
#include "unit/mini_test.h"
#include "unit/tables_fixtures.h"

namespace tables_multi_gateway_suite {

using namespace lights3;
using namespace lights3::tables;
using nlohmann::json;

struct Gateway {
    std::shared_ptr<storage::IStorageBackend> backend;
    std::shared_ptr<TableBucketStore> buckets;
    std::shared_ptr<ObjectCatalogStore> store;
    std::shared_ptr<Catalog> catalog;
};

inline Gateway make_gateway(std::shared_ptr<storage::IStorageBackend> backend) {
    Gateway g;
    g.backend = std::move(backend);
    TablesConfig cfg;
    cfg.enabled = true;
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> bmap{{"shared", g.backend}};
    BucketsConfig bcfg;
    bcfg.default_backend = "shared";
    auto router = storage::BucketRouter::build(bcfg, std::move(bmap));
    g.buckets = sync_wait(TableBucketStore::load(g.backend));
    g.store = std::make_shared<ObjectCatalogStore>(g.backend);
    g.catalog = std::make_shared<Catalog>(g.store, g.buckets, router, nullptr, cfg, MetricsScope{});
    return g;
}

inline Levels ns(std::initializer_list<const char*> l) { return Levels(l.begin(), l.end()); }

inline void put(storage::IStorageBackend& b, const std::string& key, const std::string& body) {
    storage::ObjectMeta meta;
    http::StringBodyReader r(body);
    sync_wait(b.put_object("tbk", key, std::move(meta), r));
}

inline CommitRequest append_commit(int64_t snap, const std::string& ml_key, const std::string& id = "") {
    CommitRequest c;
    c.commit_id = id;
    // every commit asserts main is unset: after the first wins the rest fail the requirement
    c.requirements = json::array({json::parse(R"({"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":null})")});
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

// b1 / b2: two handles on one shared medium (the same instance for MemoryBackend)
inline void run(std::shared_ptr<storage::IStorageBackend> b1, std::shared_ptr<storage::IStorageBackend> b2) {
    struct FaultReset {
        ~FaultReset() { fault::reset(); }
    } guard;
    Gateway a = make_gateway(std::move(b1));
    Gateway b = make_gateway(std::move(b2));

    // ① A enables the table bucket and creates the namespace / table; B sees the marker
    // after its store syncs (SysConfigStore pulls on sync_interval; a call before that is
    // the documented 404) and the catalog state at once (no cache)
    if (!sync_wait(a.backend->bucket_exists("tbk"))) sync_wait(a.backend->create_bucket("tbk"));
    sync_wait(a.catalog->enable_bucket("tbk"));
    sync_wait(a.catalog->create_namespace("tbk", ns({"n"}), {}));
    CreateTableRequest req;
    req.name = "t";
    req.schema = json::parse(R"({"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]})");
    auto t = sync_wait(a.catalog->create_table("tbk", ns({"n"}), req, {}));
    CHECK_EQ(status_of([&] { sync_wait(b.catalog->load_table("tbk", ns({"n"}), "t")); }), 404);
    sync_wait(b.buckets->sync_now());
    auto seen = sync_wait(b.catalog->load_table("tbk", ns({"n"}), "t"));
    CHECK_EQ(seen.entry.table_id, t.entry.table_id);
    CHECK_EQ(seen.entry.generation, uint64_t(1));

    // ② 10 commits from each gateway against the same expected state: exactly one wins,
    // the others are 409 (requirement or CAS), and both gateways agree on the result
    put(*a.backend, "n/t/metadata/ml.avro", tables_fixtures::slurp("ml-1.avro"));
    put(*a.backend, "n/t/metadata/m-1.avro", tables_fixtures::slurp("m-1.avro"));
    put(*a.backend, "n/t/data/f1.parquet", std::string(100, 'a'));
    put(*a.backend, "n/t/data/f2.parquet", std::string(200, 'b'));
    auto attempt = [&](Catalog& c, int i) -> Task<int> {
        try {
            co_await c.commit_table("tbk", ns({"n"}), "t", append_commit(100 + i, "n/t/metadata/ml.avro"), {});
        } catch (const RestError& e) {
            co_return e.status;
        }
        co_return 200;
    };
    std::vector<Task<int>> tasks;
    for (int i = 0; i < 10; ++i) tasks.push_back(attempt(*a.catalog, i));
    for (int i = 10; i < 20; ++i) tasks.push_back(attempt(*b.catalog, i));
    auto res = sync_wait(when_all(std::move(tasks)));
    int ok = 0, conflict = 0;
    for (int s : res) {
        if (s == 200) ++ok;
        if (s == 409) ++conflict;
    }
    CHECK_EQ(ok, 1);
    CHECK_EQ(conflict, 19);
    auto la = sync_wait(a.catalog->load_table("tbk", ns({"n"}), "t"));
    auto lb = sync_wait(b.catalog->load_table("tbk", ns({"n"}), "t"));
    CHECK_EQ(la.entry.generation, uint64_t(2));
    CHECK_EQ(lb.entry.generation, uint64_t(2));
    CHECK_EQ(la.entry.version_token, lb.entry.version_token);
    CHECK_EQ(la.etag, lb.etag);
    // a follow-up commit from B lands on top and A sees it
    auto c2 = append_commit(300, "n/t/metadata/ml.avro");
    c2.requirements = json::array();
    sync_wait(b.catalog->commit_table("tbk", ns({"n"}), "t", c2, {}));
    CHECK_EQ(sync_wait(a.catalog->load_table("tbk", ns({"n"}), "t")).entry.generation, uint64_t(3));

    // ③ A's rename dies after the destination is written; a reader on B gets 503, a
    // writer on B drives the intent to completion first (destination Active, source
    // tombstoned, no intent left) and then answers for the moved table
    sync_wait(a.catalog->create_namespace("tbk", ns({"m"}), {}));
    fault::arm("tables.rename.after_destination:1");
    CHECK_EQ(status_of([&] { sync_wait(a.catalog->rename_table("tbk", ns({"n"}), "t", ns({"m"}), "t2")); }), 500);
    CHECK_EQ(sync_wait(b.store->list_renames("tbk")).size(), size_t(1));
    CHECK_EQ(status_of([&] { sync_wait(b.catalog->load_table("tbk", ns({"n"}), "t")); }), 503);
    CHECK_EQ(status_of([&] { sync_wait(b.catalog->commit_table("tbk", ns({"n"}), "t", c2, {})); }), 404);
    CHECK(sync_wait(b.store->list_renames("tbk")).empty());
    auto moved = sync_wait(b.catalog->load_table("tbk", ns({"m"}), "t2"));
    CHECK_EQ(moved.entry.table_id, t.entry.table_id);
    CHECK_EQ(moved.entry.generation, uint64_t(3));
    auto src = sync_wait(a.store->get_table("tbk", ns({"n"}), "t"));
    CHECK(src && src->value.state == TableState::Deleted);
    CHECK_EQ(status_of([&] { sync_wait(a.catalog->load_table("tbk", ns({"n"}), "t")); }), 404);

    // ④ A drops, B loads → 404; the tombstone is shared too
    sync_wait(a.catalog->drop_table("tbk", ns({"m"}), "t2"));
    CHECK_EQ(status_of([&] { sync_wait(b.catalog->load_table("tbk", ns({"m"}), "t2")); }), 404);
    CHECK(!sync_wait(b.catalog->table_exists("tbk", ns({"m"}), "t2")));
    // clean up what the suite created (shared media are reused by other cases)
    sync_wait(a.catalog->forget_bucket("tbk"));
    sync_wait(b.buckets->sync_now());
    CHECK(a.catalog->table_bucket(b.buckets->snapshot(), "tbk") == nullptr);
}

}  // namespace tables_multi_gateway_suite
