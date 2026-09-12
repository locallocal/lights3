// ITableCatalogStore conformance suite (docs/s3-tables-design.md §12): the same
// cases over every backing -- ObjectCatalogStore on a MemoryBackend and
// DuoMetaCatalogStore on each meta engine's KV facade (rocksdb always; sqlite / redis /
// tikv where built and reachable). Factory convention: one store per call over the same
// medium; the suite isolates its scenarios by bucket name and cleans up after itself
#pragma once

#include <functional>
#include <memory>
#include <nlohmann/json.hpp>
#include <set>
#include <string>

#include "core/task.h"
#include "s3/errors.h"
#include "tables/catalog_store.h"
#include "unit/mini_test.h"

namespace catalog_store_suite {

using namespace lights3;
using namespace lights3::tables;
using nlohmann::json;

using StoreFactory = std::function<std::shared_ptr<ITableCatalogStore>()>;

inline Levels ns(std::initializer_list<const char*> l) { return Levels(l.begin(), l.end()); }

template <class F>
s3::S3ErrorCode code_of(F&& f) {
    try {
        f();
    } catch (const s3::S3Error& e) {
        return e.code;
    }
    return s3::S3ErrorCode::InternalError;
}

inline TableEntry table(const Levels& levels, const char* name, const char* id) {
    TableEntry e;
    e.levels = levels;
    e.name = name;
    e.table_id = id;
    e.table_uuid = std::string("00000000-0000-4000-8000-0000000000") + id;
    e.location = std::string("s3://cs/") + name;
    e.metadata_location = std::string(".lights3-table/n/") + name + "/metadata/00001-x.metadata.json";
    e.version_token = "t-1";
    e.generation = 1;
    return e;
}

inline void run(const StoreFactory& make) {
    auto s = make();
    const std::string b = "cs";
    // ---- namespaces: explicit entries, children by evidence, paging ----
    CHECK(sync_wait(s->bucket_state_empty(b)));
    NamespaceEntry n1;
    n1.levels = ns({"n"});
    n1.properties = {{"o", "me"}};
    storage::PutCondition fresh;
    fresh.if_none_match = true;
    sync_wait(s->put_namespace(b, n1, fresh));
    CHECK(code_of([&] { sync_wait(s->put_namespace(b, n1, fresh)); }) == s3::S3ErrorCode::PreconditionFailed);
    auto got = sync_wait(s->get_namespace(b, ns({"n"})));
    CHECK(got && got->value.properties.at("o") == "me");
    CHECK(!got->etag.empty());
    CHECK(!sync_wait(s->get_namespace(b, ns({"nope"}))).has_value());
    CHECK(!sync_wait(s->namespace_has_children(b, ns({"n"}))));
    NamespaceEntry n2;
    n2.levels = ns({"n", "child"});
    sync_wait(s->put_namespace(b, n2, {}));
    NamespaceEntry n3;
    n3.levels = ns({"a"});
    sync_wait(s->put_namespace(b, n3, {}));
    CHECK(sync_wait(s->namespace_has_children(b, ns({"n"}))));
    CHECK(!sync_wait(s->bucket_state_empty(b)));
    PageCursor c;
    c.limit = 1;
    auto p1 = sync_wait(s->list_child_namespaces(b, {}, c));
    CHECK_EQ(p1.items.size(), size_t(1));
    CHECK_EQ(p1.items[0], "a");
    CHECK_EQ(p1.next_after, "a");
    c.after = p1.next_after;
    auto p2 = sync_wait(s->list_child_namespaces(b, {}, c));
    CHECK_EQ(p2.items.size(), size_t(1));
    CHECK_EQ(p2.items[0], "n");
    CHECK(p2.next_after.empty());
    auto kids = sync_wait(s->list_child_namespaces(b, ns({"n"}), PageCursor{}));
    CHECK_EQ(kids.items.size(), size_t(1));
    CHECK_EQ(kids.items[0], "child");
    // ---- tables: CAS semantics, listing, tombstones ----
    TableEntry t = table(ns({"n"}), "t", "01");
    std::string e1 = sync_wait(s->put_table(b, ns({"n"}), "t", t, fresh));
    CHECK(!e1.empty());
    CHECK(code_of([&] { sync_wait(s->put_table(b, ns({"n"}), "t", t, fresh)); }) ==
          s3::S3ErrorCode::PreconditionFailed);
    auto tv = sync_wait(s->get_table(b, ns({"n"}), "t"));
    CHECK(tv && tv->etag == e1 && tv->value.table_id == "01");
    storage::PutCondition wrong;
    wrong.if_match_etag = "nope";
    CHECK(code_of([&] { sync_wait(s->put_table(b, ns({"n"}), "t", t, wrong)); }) ==
          s3::S3ErrorCode::PreconditionFailed);
    CHECK(code_of([&] { sync_wait(s->put_table(b, ns({"n"}), "missing", t, wrong)); }) == s3::S3ErrorCode::NoSuchKey);
    storage::PutCondition match;
    match.if_match_etag = e1;
    t.generation = 2;
    t.version_token = "t-2";
    std::string e2 = sync_wait(s->put_table(b, ns({"n"}), "t", t, match));
    CHECK(e2 != e1);
    CHECK_EQ(sync_wait(s->get_table(b, ns({"n"}), "t"))->value.generation, uint64_t(2));
    // the stale etag no longer matches
    CHECK(code_of([&] { sync_wait(s->put_table(b, ns({"n"}), "t", t, match)); }) ==
          s3::S3ErrorCode::PreconditionFailed);
    sync_wait(s->put_table(b, ns({"n"}), "u", table(ns({"n"}), "u", "02"), {}));
    TableEntry dead = table(ns({"n"}), "v", "03");
    dead.state = TableState::Deleted;
    sync_wait(s->put_table(b, ns({"n"}), "v", dead, {}));
    c = PageCursor{};
    c.limit = 2;
    auto tp = sync_wait(s->list_tables(b, ns({"n"}), c));
    CHECK_EQ(tp.items.size(), size_t(2));
    CHECK_EQ(tp.items[0], "t");
    CHECK_EQ(tp.next_after, "u");
    c.after = tp.next_after;
    auto tp2 = sync_wait(s->list_tables(b, ns({"n"}), c));
    CHECK_EQ(tp2.items.size(), size_t(1));
    CHECK_EQ(tp2.items[0], "v");
    CHECK(sync_wait(s->namespace_has_children(b, ns({"n"}))));
    // ---- views share the namespace's children evidence ----
    ViewEntry v;
    v.levels = ns({"a"});
    v.name = "w";
    v.view_id = "v1";
    v.view_uuid = "00000000-0000-4000-8000-0000000000v1";
    v.location = "s3://cs/a/w";
    v.metadata_location = ".lights3-table/a/w/view-metadata/00001-x.metadata.json";
    v.version_token = "t-v";
    CHECK(!sync_wait(s->namespace_has_children(b, ns({"a"}))));
    std::string ve = sync_wait(s->put_view(b, ns({"a"}), "w", v, fresh));
    CHECK(code_of([&] { sync_wait(s->put_view(b, ns({"a"}), "w", v, fresh)); }) == s3::S3ErrorCode::PreconditionFailed);
    CHECK(sync_wait(s->namespace_has_children(b, ns({"a"}))));
    auto vv = sync_wait(s->get_view(b, ns({"a"}), "w"));
    CHECK(vv && vv->etag == ve && vv->value.view_id == "v1");
    CHECK_EQ(sync_wait(s->list_views(b, ns({"a"}), PageCursor{})).items.size(), size_t(1));
    CHECK(sync_wait(s->list_tables(b, ns({"a"}), PageCursor{})).items.empty());
    CHECK(sync_wait(s->list_child_namespaces(b, ns({"a"}), PageCursor{})).items.empty());
    // ---- commits and renames ----
    CommitRecord r;
    r.commit_id = "c-1";
    r.table_id = "01";
    r.expected_token = "t-1";
    r.new_token = "t-2";
    r.prev_metadata_location = "p";
    r.new_metadata_location = "q";
    r.status = "STAGED";
    sync_wait(s->put_commit(b, "01", r, fresh));
    CHECK(code_of([&] { sync_wait(s->put_commit(b, "01", r, fresh)); }) == s3::S3ErrorCode::PreconditionFailed);
    r.status = "COMMITTED";
    sync_wait(s->put_commit(b, "01", r, {}));
    auto rv = sync_wait(s->get_commit(b, "01", "c-1"));
    CHECK(rv && rv->value.status == "COMMITTED");
    CHECK(!sync_wait(s->get_commit(b, "01", "c-2")).has_value());
    CommitRecord r2 = r;
    r2.commit_id = "c-2";
    sync_wait(s->put_commit(b, "01", r2, {}));
    CHECK_EQ(sync_wait(s->list_commits(b, "01")).size(), size_t(2));
    sync_wait(s->delete_commit(b, "01", "c-2"));
    sync_wait(s->delete_commit(b, "01", "c-2"));
    CHECK_EQ(sync_wait(s->list_commits(b, "01")).size(), size_t(1));
    RenameIntent ri;
    ri.rename_id = "r-1";
    ri.src_levels = ns({"n"});
    ri.dst_levels = ns({"n"});
    ri.src_name = "t";
    ri.dst_name = "t2";
    ri.src_etag = e2;
    std::string re = sync_wait(s->put_rename(b, ri, fresh));
    CHECK(!re.empty());
    ri.stage = RenameIntent::Stage::SourceFenced;
    storage::PutCondition rcas;
    rcas.if_match_etag = re;
    std::string re2 = sync_wait(s->put_rename(b, ri, rcas));
    CHECK(re2 != re);
    CHECK(code_of([&] { sync_wait(s->put_rename(b, ri, rcas)); }) == s3::S3ErrorCode::PreconditionFailed);
    auto rget = sync_wait(s->get_rename(b, "r-1"));
    CHECK(rget && rget->value.stage == RenameIntent::Stage::SourceFenced && rget->etag == re2);
    CHECK_EQ(sync_wait(s->list_renames(b)).size(), size_t(1));
    sync_wait(s->delete_rename(b, "r-1"));
    CHECK(sync_wait(s->list_renames(b)).empty());
    // ---- maintenance settings ----
    CHECK(!sync_wait(s->get_maintenance_config(b, ns({"n"}), "t")).has_value());
    MaintenanceConfig mc;
    mc.delete_enabled = true;
    mc.max_snapshot_age_ms = 42;
    sync_wait(s->put_maintenance_config(b, ns({"n"}), "t", mc));
    auto mcg = sync_wait(s->get_maintenance_config(b, ns({"n"}), "t"));
    CHECK(mcg && *mcg->delete_enabled && *mcg->max_snapshot_age_ms == 42 && !mcg->orphan_cleanup);
    sync_wait(s->delete_maintenance_config(b, ns({"n"}), "t"));
    CHECK(!sync_wait(s->get_maintenance_config(b, ns({"n"}), "t")).has_value());
    // ---- atomic commit (transactional backings only) ----
    if (s->supports_atomic_commit()) {
        auto cur = sync_wait(s->get_table(b, ns({"n"}), "t"));
        TableEntry next = cur->value;
        next.generation = 3;
        next.version_token = "t-3";
        CommitRecord fin = r;
        fin.commit_id = "c-atomic";
        fin.status = "COMMITTED";
        storage::PutCondition stale;
        stale.if_match_etag = "stale";
        // a failed pointer condition leaves no record behind
        CHECK(code_of([&] { sync_wait(s->commit_atomic(b, ns({"n"}), "t", next, stale, fin)); }) ==
              s3::S3ErrorCode::PreconditionFailed);
        CHECK(!sync_wait(s->get_commit(b, "01", "c-atomic")).has_value());
        CHECK_EQ(sync_wait(s->get_table(b, ns({"n"}), "t"))->value.generation, uint64_t(2));
        storage::PutCondition ok;
        ok.if_match_etag = cur->etag;
        std::string e3 = sync_wait(s->commit_atomic(b, ns({"n"}), "t", next, ok, fin));
        CHECK(!e3.empty());
        CHECK_EQ(sync_wait(s->get_table(b, ns({"n"}), "t"))->value.generation, uint64_t(3));
        CHECK_EQ(sync_wait(s->get_table(b, ns({"n"}), "t"))->etag, e3);
        CHECK_EQ(sync_wait(s->get_commit(b, "01", "c-atomic"))->value.status, "COMMITTED");
        // a repeated record id is refused (the record's own if_none_match)
        CHECK(code_of([&] { sync_wait(s->commit_atomic(b, ns({"n"}), "t", next, storage::PutCondition{}, fin)); }) ==
              s3::S3ErrorCode::PreconditionFailed);
    }
    // ---- raw export / import round trip into a second bucket ----
    auto raw = sync_wait(s->export_raw(b));
    CHECK(raw.size() >= 7);
    std::set<std::string> keys;
    for (auto& e : raw) keys.insert(e.key);
    CHECK(keys.count("tables-catalog/cs/ns/n/tbl/t.json"));
    CHECK(keys.count("tables-catalog/cs/ns/a/view/w.json"));
    CHECK(keys.count("tables-catalog/cs/commits/01/c-1.json"));
    CHECK(code_of([&] { sync_wait(s->import_raw("other", raw[0])); }) == s3::S3ErrorCode::InvalidRequest);
    for (auto& e : raw) {
        RawEntry copy = e;
        copy.key = "tables-catalog/cs2/" + e.key.substr(std::string("tables-catalog/cs/").size());
        sync_wait(s->import_raw("cs2", copy));
    }
    CHECK_EQ(sync_wait(s->export_raw("cs2")).size(), raw.size());
    CHECK(sync_wait(s->get_table("cs2", ns({"n"}), "t")).has_value());
    // ---- delete everything; the medium is shared with other cases ----
    sync_wait(s->delete_table(b, ns({"n"}), "t"));
    CHECK(!sync_wait(s->get_table(b, ns({"n"}), "t")).has_value());
    sync_wait(s->delete_view(b, ns({"a"}), "w"));
    CHECK(!sync_wait(s->get_view(b, ns({"a"}), "w")).has_value());
    sync_wait(s->delete_namespace(b, ns({"n", "child"})));
    CHECK(!sync_wait(s->get_namespace(b, ns({"n", "child"}))).has_value());
    sync_wait(s->delete_bucket_state(b));
    sync_wait(s->delete_bucket_state("cs2"));
    CHECK(sync_wait(s->bucket_state_empty(b)));
    CHECK(sync_wait(s->export_raw(b)).empty());
    CHECK(sync_wait(s->export_raw("cs2")).empty());
}

}  // namespace catalog_store_suite
