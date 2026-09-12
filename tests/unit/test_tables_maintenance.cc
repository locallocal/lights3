// Table maintenance (docs/s3-tables-design.md §9, docs/s3-tables/step-4-maintenance.md §8):
// planner (retention set, snapshot expiry, orphans), runner (StalePlan, delete gate,
// safety window), purge, the periodic runner and the resource-level job framework
#include <chrono>
#include <future>
#include <nlohmann/json.hpp>
#include <set>
#include <thread>

#include "app/admin_jobs.h"
#include "storage/memory/memory_backend.h"
#include "tables/catalog.h"
#include "tables/iceberg/metadata.h"
#include "tables/maintenance.h"
#include "tables/maintenance_runner.h"
#include "tables/object_catalog_store.h"
#include "tables/rest_error.h"
#include "unit/mini_test.h"
#include "unit/tables_fixtures.h"

using namespace lights3;
using namespace lights3::tables;
using nlohmann::json;

namespace {

Levels ns(std::initializer_list<const char*> l) { return Levels(l.begin(), l.end()); }

struct Env {
    std::shared_ptr<storage::MemoryBackend> backend = std::make_shared<storage::MemoryBackend>();
    std::shared_ptr<TableBucketStore> buckets;
    std::shared_ptr<ObjectCatalogStore> store;
    std::shared_ptr<Catalog> catalog;
    TablesConfig cfg;

    explicit Env(int metadata_log_keep = 100) {
        cfg.enabled = true;
        cfg.metadata_log_keep = metadata_log_keep;
        std::map<std::string, std::shared_ptr<storage::IStorageBackend>> bmap{{"mem", backend}};
        BucketsConfig bcfg;
        bcfg.default_backend = "mem";
        auto router = storage::BucketRouter::build(bcfg, std::move(bmap));
        buckets = sync_wait(TableBucketStore::load(backend));
        store = std::make_shared<ObjectCatalogStore>(backend);
        catalog = std::make_shared<Catalog>(store, buckets, router, nullptr, cfg, MetricsScope{});
        sync_wait(backend->create_bucket("tbk"));
        sync_wait(catalog->enable_bucket("tbk"));
        sync_wait(catalog->create_namespace("tbk", ns({"n"}), {}));
    }
    Catalog::LoadedTable create(const std::string& name = "t") {
        CreateTableRequest r;
        r.name = name;
        r.schema = json::parse(R"({"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]})");
        return sync_wait(catalog->create_table("tbk", ns({"n"}), r, {}));
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
    void age(const std::string& key, int seconds = 3600) {
        CHECK(
            backend->set_mtime_for_tests("tbk", key, std::chrono::system_clock::now() - std::chrono::seconds(seconds)));
    }
    // every object under a prefix is back-dated
    void age_all(const std::string& prefix, int seconds = 3600) {
        storage::ListOptions opt;
        opt.prefix = prefix;
        auto page = sync_wait(backend->list_objects("tbk", opt));
        for (auto& o : page.objects) age(o.key, seconds);
    }
    std::vector<std::string> keys(const std::string& prefix) {
        storage::ListOptions opt;
        opt.prefix = prefix;
        std::vector<std::string> out;
        auto page = sync_wait(backend->list_objects("tbk", opt));
        for (auto& o : page.objects) out.push_back(o.key);
        return out;
    }
    // fixture "1" (f1 100 B + f2 200 B) as snapshot `snap`; parent / timestamp optional
    Catalog::LoadedTable commit(int64_t snap, const std::string& ml_key, std::optional<int64_t> parent = std::nullopt,
                                std::optional<int64_t> ts_ms = std::nullopt, const std::string& name = "t") {
        put(ml_key, tables_fixtures::slurp("ml-1.avro"));
        put("n/t/metadata/m-1.avro", tables_fixtures::slurp("m-1.avro"));
        put("n/t/data/f1.parquet", std::string(100, 'a'));
        put("n/t/data/f2.parquet", std::string(200, 'b'));
        CommitRequest c;
        json s;
        s["snapshot-id"] = snap;
        s["sequence-number"] = snap;
        s["timestamp-ms"] = ts_ms ? *ts_ms : 1000 + snap;
        if (parent) s["parent-snapshot-id"] = *parent;
        s["manifest-list"] = "s3://tbk/" + ml_key;
        s["summary"] = json::object({{"operation", "append"}});
        c.updates = json::array(
            {json::object({{"action", "add-snapshot"}, {"snapshot", s}}),
             json::parse(R"({"action":"set-snapshot-ref","ref-name":"main","type":"branch","snapshot-id":)" +
                         std::to_string(snap) + "}")});
        return sync_wait(catalog->commit_table("tbk", ns({"n"}), name, c, {}));
    }
    PlannerOptions opts(int retain = 2, int window = 900) {
        PlannerOptions o;
        o.retain_recent = retain;
        o.safety_window_sec = window;
        return o;
    }
    MaintenancePlan plan(const PlannerOptions& o, const std::string& name = "t") {
        return sync_wait(plan_table(*catalog, "tbk", ns({"n"}), name, o, now_unix()));
    }
};

template <class F>
std::string fails_with(F&& f) {
    try {
        f();
    } catch (const RestError& e) {
        return "rest:" + std::to_string(e.status) + ":" + e.message;
    } catch (const s3::S3Error& e) {
        return "s3:" + std::to_string(static_cast<int>(e.code)) + ":" + e.message;
    }
    return "";
}

std::set<std::string> as_set(const std::vector<std::string>& v) { return {v.begin(), v.end()}; }

}  // namespace

TEST(tables_maintenance_plan_metadata_retention) {
    Env env(/*metadata_log_keep=*/1);
    env.create();
    for (int i = 1; i <= 4; ++i) env.commit(i, "n/t/metadata/ml-" + std::to_string(i) + ".avro");
    const std::string dir = ".lights3-table/n/t/metadata/";
    CHECK_EQ(env.keys(dir).size(), size_t(5));
    // stray metadata files nobody references: two old, one fresh
    env.put(dir + "00000-old-a.metadata.json", "{}");
    env.put(dir + "00000-old-b.metadata.json", "{}");
    env.put(dir + "00000-young.metadata.json", "{}");
    env.age(dir + "00000-old-a.metadata.json");
    env.age(dir + "00000-old-b.metadata.json");
    auto real = env.keys(dir);
    for (auto& k : real)
        if (k.find("00000-") == std::string::npos) env.age(k);
    MaintenancePlan p = env.plan(env.opts(2));
    // retained: the pointer (gen 5), the metadata-log (gen 4 only: keep=1), every COMMITTED
    // record's file (gen 2..5) and the 2 newest by name; gen 1 (the create) and the two old
    // stray files are candidates, the fresh stray one waits for the safety window
    auto cands = as_set(p.metadata_candidates);
    CHECK_EQ(cands.size(), size_t(3));
    CHECK(cands.count(dir + "00000-old-a.metadata.json"));
    CHECK(cands.count(dir + "00000-old-b.metadata.json"));
    bool gen1 = false;
    for (auto& k : cands)
        if (k.rfind(dir + "00001-", 0) == 0) gen1 = true;
    CHECK(gen1);
    CHECK(!p.manual_review);
    CHECK(p.expire_snapshots.empty());
    CHECK_EQ(p.version_token, sync_wait(env.catalog->table_pointer("tbk", ns({"n"}), "t"))->value.version_token);
    // with a zero safety window the young one is a candidate too; retaining the 6 newest by
    // name keeps gen 1 and the young stray, leaving the two old strays
    CHECK_EQ(env.plan(env.opts(2, 0)).metadata_candidates.size(), size_t(4));
    CHECK_EQ(env.plan(env.opts(6)).metadata_candidates.size(), size_t(2));
    // JSON round trip
    auto back = MaintenancePlan::from_json(p.to_json());
    CHECK(back.has_value());
    CHECK(back->metadata_candidates == p.metadata_candidates);
    CHECK_EQ(back->version_token, p.version_token);
    CHECK(!MaintenancePlan::from_json(json::object()).has_value());
}

TEST(tables_maintenance_plan_snapshot_expiry) {
    Env env;
    env.create();
    const int64_t now_ms = now_unix() * 1000;
    const int64_t day = 86400000;
    env.commit(1, "n/t/metadata/ml-1.avro", std::nullopt, now_ms - 10 * day);
    env.commit(2, "n/t/metadata/ml-2.avro", 1, now_ms - 5 * day);
    env.commit(3, "n/t/metadata/ml-3.avro", std::nullopt, now_ms - 1 * day);
    // a tag pins snapshot 2; the age rule comes from the table property
    CommitRequest tag;
    tag.updates = json::array(
        {json::parse(R"({"action":"set-snapshot-ref","ref-name":"v2","type":"tag","snapshot-id":2})"),
         json::parse(R"({"action":"set-properties","updates":{"history.expire.max-snapshot-age-ms":"172800000"}})")});
    sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", tag, {}));
    auto loaded = sync_wait(env.catalog->load_table("tbk", ns({"n"}), "t"));
    auto eff = resolve_maintenance(env.cfg, std::nullopt, loaded.metadata["properties"]);
    CHECK(eff.planner.max_snapshot_age_ms.has_value());
    CHECK_EQ(*eff.planner.max_snapshot_age_ms, int64_t(172800000));
    CHECK(!eff.conflict);
    PlannerOptions o = eff.planner;
    o.safety_window_sec = 0;
    MaintenancePlan p = env.plan(o);
    // 3 is current, 2 is tagged (and 1 is its parent? no: 2's parent is 1 -> kept by the chain)
    CHECK(p.expire_snapshots.empty());
    // drop the tag: 2 and its parent 1 are older than two days and unreferenced
    CommitRequest untag;
    untag.updates = json::array({json::parse(R"({"action":"remove-snapshot-ref","ref-name":"v2"})")});
    sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", untag, {}));
    p = env.plan(o);
    CHECK(p.expire_snapshots == (std::vector<int64_t>{1, 2}));
    CHECK_EQ(p.expire_updates.size(), size_t(1));
    CHECK_EQ(p.expire_updates[0]["action"].get<std::string>(), "remove-snapshots");
    CHECK_EQ(p.expire_requirements[0]["ref"].get<std::string>(), "main");
    CHECK_EQ(p.expire_requirements[0]["snapshot-id"].get<int64_t>(), 3);
    // min_snapshots_to_keep protects the newest ones even past the age
    o.min_snapshots_to_keep = 3;
    CHECK(env.plan(o).expire_snapshots.empty());
    o.min_snapshots_to_keep = 1;
    // a table object disagreeing with the property is a conflict
    MaintenanceConfig mc;
    mc.max_snapshot_age_ms = day;
    auto eff2 = resolve_maintenance(env.cfg, mc, loaded.metadata["properties"]);
    CHECK(eff2.conflict);
    CHECK_EQ(*eff2.planner.max_snapshot_age_ms, int64_t(172800000));
    // a ref with its own retention rules → manual review, nothing expires
    CommitRequest ref;
    ref.updates = json::array({json::parse(
        R"({"action":"set-snapshot-ref","ref-name":"hold","type":"branch","snapshot-id":1,"max-ref-age-ms":1000})")});
    sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", ref, {}));
    p = env.plan(o);
    CHECK(p.manual_review);
    CHECK(p.expire_snapshots.empty());
    // run refuses a manual-review plan only at the REST layer; the runner executes what it
    // is given, so the expiry itself is exercised on the un-held table
    CommitRequest unhold;
    unhold.updates = json::array({json::parse(R"({"action":"remove-snapshot-ref","ref-name":"hold"})")});
    sync_wait(env.catalog->commit_table("tbk", ns({"n"}), "t", unhold, {}));
    p = env.plan(o);
    CHECK_EQ(p.expire_snapshots.size(), size_t(2));
    auto before = sync_wait(env.catalog->table_pointer("tbk", ns({"n"}), "t"));
    RunOptions ro;
    RunReport rep = sync_wait(run_table(*env.catalog, p, ro, now_unix()));
    CHECK_EQ(rep.expired_snapshots, 2);
    CHECK_EQ(rep.generation, before->value.generation + 1);
    auto after = sync_wait(env.catalog->load_table("tbk", ns({"n"}), "t"));
    CHECK_EQ(after.metadata["snapshots"].size(), size_t(1));
    CHECK_EQ(iceberg::current_snapshot_id(after.metadata), 3);
    bool logged = false;
    for (auto& r : sync_wait(env.store->list_commits("tbk", after.entry.table_id)))
        if (r.commit_id.rfind("maint-", 0) == 0 && r.status == "COMMITTED") logged = true;
    CHECK(logged);
}

TEST(tables_maintenance_plan_orphans) {
    Env env;
    env.create();
    env.commit(1, "n/t/metadata/ml-1.avro");
    env.put("n/t/data/stray.parquet", "zzz");
    env.put("n/t/data/fresh.parquet", "zzz");
    env.age("n/t/data/stray.parquet");
    env.age("n/t/data/f1.parquet");
    env.age("n/t/data/f2.parquet");
    MaintenancePlan p = env.plan(env.opts(10));
    CHECK(p.orphan_candidates == (std::vector<std::string>{"n/t/data/stray.parquet"}));
    CHECK(!p.manual_review);
    // orphan cleanup off
    PlannerOptions o = env.opts(10);
    o.orphan_cleanup = false;
    CHECK(env.plan(o).orphan_candidates.empty());
    // a manifest that no longer parses: fail closed
    env.put("n/t/metadata/m-1.avro", "garbage");
    p = env.plan(env.opts(10));
    CHECK(p.manual_review);
    CHECK(p.orphan_candidates.empty());
    CHECK(!p.notes.empty());
    // an unreadable codec: the same
    env.put("n/t/metadata/m-1.avro", tables_fixtures::slurp("m-1.avro"));
    env.put("n/t/metadata/ml-1.avro", tables_fixtures::snappy_manifest_list());
    p = env.plan(env.opts(10));
    CHECK(p.manual_review);
    CHECK(p.orphan_candidates.empty());
}

TEST(tables_maintenance_run_gates) {
    Env env(1);
    env.create();
    env.commit(1, "n/t/metadata/ml-1.avro");
    env.commit(2, "n/t/metadata/ml-2.avro");
    const std::string dir = ".lights3-table/n/t/metadata/";
    env.put(dir + "00000-old.metadata.json", "{}");
    env.put("n/t/data/stray.parquet", "zzz");
    env.age_all(dir);
    env.age_all("n/t/data/");
    MaintenancePlan p = env.plan(env.opts(1));
    // the stray file and gen 1 (neither in the log nor in a record)
    CHECK_EQ(p.metadata_candidates.size(), size_t(2));
    CHECK_EQ(p.metadata_candidates[0], dir + "00000-old.metadata.json");
    CHECK(p.orphan_candidates == (std::vector<std::string>{"n/t/data/stray.parquet"}));
    // StalePlan: the table moved
    env.commit(3, "n/t/metadata/ml-3.avro");
    RunOptions ro;
    ro.delete_enabled = true;
    std::string err = fails_with([&] { sync_wait(run_table(*env.catalog, p, ro, now_unix())); });
    CHECK(err.find("StalePlan") != std::string::npos);
    CHECK(env.exists(dir + "00000-old.metadata.json"));
    // delete_enabled=false: nothing deleted, nothing to expire
    p = env.plan(env.opts(1));
    ro.delete_enabled = false;
    RunReport rep = sync_wait(run_table(*env.catalog, p, ro, now_unix()));
    CHECK_EQ(rep.deleted_metadata + rep.deleted_orphans, 0);
    CHECK(env.exists("n/t/data/stray.parquet"));
    // the safety window is re-checked at delete time: a candidate touched since is skipped
    env.put("n/t/data/stray.parquet", "zzz-rewritten");
    ro.delete_enabled = true;
    int64_t usage_objects = 0, usage_bytes = 0;
    ro.note_usage = [&](std::string_view, int64_t o, int64_t b) {
        usage_objects += o;
        usage_bytes += b;
    };
    rep = sync_wait(run_table(*env.catalog, p, ro, now_unix()));
    CHECK_EQ(rep.deleted_metadata, 2);
    CHECK_EQ(rep.deleted_orphans, 0);
    CHECK_EQ(rep.skipped, 1);
    CHECK(!env.exists(dir + "00000-old.metadata.json"));
    CHECK(env.exists("n/t/data/stray.parquet"));
    CHECK_EQ(usage_objects, -2);
    CHECK(usage_bytes < -2);
    // the pointer moved onto a candidate between plan and delete: skipped (no token change)
    env.put(dir + "00000-old2.metadata.json", "{}");
    env.age(dir + "00000-old2.metadata.json");
    p = env.plan(env.opts(1));
    CHECK(p.metadata_candidates == (std::vector<std::string>{dir + "00000-old2.metadata.json"}));
    auto cur = sync_wait(env.store->get_table("tbk", ns({"n"}), "t"));
    TableEntry moved = cur->value;
    moved.metadata_location = dir + "00000-old2.metadata.json";
    sync_wait(env.store->put_table("tbk", ns({"n"}), "t", moved, {}));
    rep = sync_wait(run_table(*env.catalog, p, ro, now_unix()));
    CHECK_EQ(rep.deleted_metadata, 0);
    CHECK_EQ(rep.skipped, 1);
    CHECK(env.exists(dir + "00000-old2.metadata.json"));
}

TEST(tables_maintenance_purge) {
    Env env;
    auto t = env.create();
    env.commit(1, "n/t/metadata/ml-1.avro");
    MaintenanceConfig mc;
    mc.delete_enabled = true;
    sync_wait(env.store->put_maintenance_config("tbk", ns({"n"}), "t", mc));
    // purge needs a tombstone
    RunOptions ro;
    std::string err = fails_with([&] { sync_wait(purge_table(*env.catalog, "tbk", ns({"n"}), "t", ro)); });
    CHECK(err.find("not dropped") != std::string::npos);
    sync_wait(env.catalog->drop_table("tbk", ns({"n"}), "t"));
    int64_t objects = 0;
    ro.note_usage = [&](std::string_view, int64_t o, int64_t) { objects += o; };
    PurgeReport rep = sync_wait(purge_table(*env.catalog, "tbk", ns({"n"}), "t", ro));
    CHECK(rep.tombstone_removed);
    CHECK(rep.deleted_objects >= 5);
    CHECK_EQ(-objects, static_cast<int64_t>(rep.deleted_objects));
    CHECK(env.keys(".lights3-table/n/t/").empty());
    CHECK(env.keys("n/t/").empty());
    CHECK(!sync_wait(env.store->get_table("tbk", ns({"n"}), "t")).has_value());
    CHECK(!sync_wait(env.store->get_maintenance_config("tbk", ns({"n"}), "t")).has_value());
    CHECK(sync_wait(env.store->list_commits("tbk", t.entry.table_id)).empty());
    CHECK(sync_wait(env.catalog->list_tables("tbk", ns({"n"}), PageCursor{})).items.empty());
    CHECK_EQ(fails_with([&] { sync_wait(purge_table(*env.catalog, "tbk", ns({"n"}), "t", ro)); }).substr(0, 8),
             "rest:404");
    // the namespace holds nothing now
    sync_wait(env.catalog->drop_namespace("tbk", ns({"n"})));
}

TEST(tables_maintenance_runner_pass_and_jobs) {
    Env env(1);
    env.cfg.maintenance.delete_enabled = true;
    env.cfg.maintenance.safety_window_sec = 0;
    env.cfg.maintenance.retain_recent_metadata_files = 1;
    env.cfg.maintenance.tombstone_ttl_sec = 1;
    env.create();
    env.commit(1, "n/t/metadata/ml-1.avro");
    env.commit(2, "n/t/metadata/ml-2.avro");
    env.put(".lights3-table/n/t/metadata/00000-old.metadata.json", "{}");
    env.put("n/t/data/stray.parquet", "zzz");
    // a dropped table whose tombstone is past its ttl
    env.create("gone");
    sync_wait(env.catalog->drop_table("tbk", ns({"n"}), "gone"));
    auto tomb = sync_wait(env.store->get_table("tbk", ns({"n"}), "gone"));
    TableEntry old = tomb->value;
    old.updated_unix -= 100;
    sync_wait(env.store->put_table("tbk", ns({"n"}), "gone", old, {}));
    MaintenanceRunner runner(env.catalog, env.cfg);
    // through the job framework with a manual job holding the table: skipped
    AdminJobs jobs(std::map<std::string, std::shared_ptr<storage::IStorageBackend>>{});
    JobHooks jh;
    jh.start = [&](const std::string& resource, const std::string& op, JobHooks::Fn fn) {
        auto o = parse_job_op("tables", op);
        CHECK(o.has_value());
        try {
            uint64_t id = jobs.start_custom(resource, *o, [fn] {
                JobOutcome out;
                out.kind = "tables";
                out.stats = fn();
                return out;
            });
            json j = jobs.status(resource, *o);
            j["job_id"] = id;
            return j;
        } catch (const AdminJobs::Failure& f) {
            throw s3::S3Error(s3::S3ErrorCode::JobInProgress, f.message);
        }
    };
    jh.status = [&](const std::string& resource, const std::string& op) {
        return jobs.status(resource, *parse_job_op("tables", op));
    };
    jh.status_by_id = [&](uint64_t id) { return jobs.status_by_id(id); };
    runner.set_job_hooks(jh);
    std::promise<void> release;
    auto gate = release.get_future().share();
    uint64_t held = jobs.start_custom(job_resource("tbk", ns({"n"}), "t"), JobOp::TablePlan, [gate] {
        gate.wait();
        JobOutcome o;
        o.kind = "tables";
        return o;
    });
    auto st = sync_wait(runner.run_once());
    CHECK_EQ(st.buckets, uint64_t(1));
    CHECK_EQ(st.tables, uint64_t(1));
    CHECK_EQ(st.skipped_busy, uint64_t(1));
    CHECK_EQ(st.ran, uint64_t(0));
    CHECK_EQ(st.tombstones_removed, uint64_t(1));
    CHECK(env.exists(".lights3-table/n/t/metadata/00000-old.metadata.json"));
    release.set_value();
    for (int i = 0; i < 500 && jobs.status_by_id(held)->value("running", false); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    // a manual job of another op on the same table would still block; the slot is free now
    st = sync_wait(runner.run_once());
    CHECK_EQ(st.ran, uint64_t(1));
    CHECK_EQ(st.skipped_busy, uint64_t(0));
    CHECK_EQ(st.failed, uint64_t(0));
    CHECK(!env.exists(".lights3-table/n/t/metadata/00000-old.metadata.json"));
    CHECK(!env.exists("n/t/data/stray.parquet"));
    json run_doc = jobs.status(job_resource("tbk", ns({"n"}), "t"), JobOp::TableRun);
    CHECK_EQ(run_doc["stats"]["run"]["deleted_orphans"].get<int>(), 1);
    CHECK_EQ(run_doc["stats"]["plan"]["version-token"].get<std::string>().rfind("t-", 0), size_t(0));
    // the same pass without hooks runs inline and finds nothing left to do
    MaintenanceRunner inline_runner(env.catalog, env.cfg);
    st = sync_wait(inline_runner.run_once());
    CHECK_EQ(st.ran, uint64_t(1));
    json again = jobs.status(job_resource("tbk", ns({"n"}), "t"), JobOp::TableRun);
    CHECK_EQ(again["job_id"].get<uint64_t>(), run_doc["job_id"].get<uint64_t>());
    // status_by_id / parse_job_op / group names
    CHECK(jobs.status_by_id(held).has_value());
    CHECK(!jobs.status_by_id(999).has_value());
    CHECK_EQ(std::string(job_group_name(JobOp::TableRun)), "tables");
    CHECK_EQ(std::string(job_op_name(JobOp::TablePurge)), "purge");
    CHECK(parse_job_op("tables", "nope") == std::nullopt);
    // a custom resource never collides with a backend name lookup
    CHECK_EQ(jobs.status("tables:tbk/n/other", JobOp::TablePlan)["job_id"].is_null(), true);
    // the background timer path: one tick is one pass
    auto pool = std::make_shared<ThreadPool>(2);
    env.put("n/t/data/stray2.parquet", "zzz");
    inline_runner.start_background(pool, 1);
    bool gone = false;
    for (int i = 0; i < 400 && !gone; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(25));
        gone = !env.exists("n/t/data/stray2.parquet");
    }
    inline_runner.shutdown_background();
    CHECK(gone);
    jobs.shutdown();
}
