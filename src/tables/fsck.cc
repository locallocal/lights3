#include "tables/fsck.h"

#include <set>

#include "core/log.h"
#include "s3/errors.h"
#include "tables/identifier.h"
#include "tables/object_catalog_store.h"
#include "tables/table_bucket_store.h"

namespace lights3::tables {

using nlohmann::json;
using s3::S3Error;
using s3::S3ErrorCode;

namespace {

bool is_missing(const S3Error& e) { return e.code == S3ErrorCode::NoSuchKey || e.code == S3ErrorCode::NoSuchBucket; }

Task<bool> object_exists(storage::IStorageBackend& backend, std::string_view bucket, std::string_view key) {
    try {
        co_await backend.head_object(bucket, key);
    } catch (const S3Error& e) {
        if (is_missing(e)) co_return false;
        throw;
    }
    co_return true;
}

// every key under a prefix of .sys
Task<std::vector<std::string>> sys_keys(storage::IStorageBackend& sys, std::string prefix) {
    std::vector<std::string> out;
    storage::ListOptions opt;
    opt.prefix = prefix;
    for (;;) {
        auto page = co_await sys.list_objects(storage::kSysBucketName, opt);
        for (auto& o : page.objects) out.push_back(o.key);
        if (!page.is_truncated) break;
        opt.start_after = page.next_token;
    }
    co_return out;
}

// the bucket names that have catalog state ("tables-catalog/<bucket>/")
Task<std::vector<std::string>> catalog_buckets(storage::IStorageBackend& sys) {
    std::vector<std::string> out;
    storage::ListOptions opt;
    opt.prefix = std::string(ObjectCatalogStore::kRoot);
    opt.delimiter = "/";
    for (;;) {
        auto page = co_await sys.list_objects(storage::kSysBucketName, opt);
        for (auto& cp : page.common_prefixes) {
            std::string b = cp.substr(opt.prefix.size());
            if (!b.empty() && b.back() == '/') b.pop_back();
            if (!b.empty()) out.push_back(b);
        }
        if (!page.is_truncated) break;
        opt.start_after = page.next_token;
    }
    co_return out;
}

struct TableRef {
    Levels levels;
    std::string name;
};

// "tables-catalog/<bucket>/ns/a/b/tbl/t.json" → {a,b}, t
std::optional<TableRef> table_ref(const std::string& key, const std::string& bucket) {
    std::string root = ObjectCatalogStore::bucket_root(bucket) + "ns/";
    if (key.rfind(root, 0) != 0) return std::nullopt;
    std::string rel = key.substr(root.size());
    auto tbl = rel.find("/tbl/");
    if (tbl == std::string::npos) return std::nullopt;
    std::string file = rel.substr(tbl + 5);
    if (file.size() < 5 || file.substr(file.size() - 5) != ".json" || file.find('/') != std::string::npos)
        return std::nullopt;
    TableRef r;
    r.name = file.substr(0, file.size() - 5);
    std::string ns = rel.substr(0, tbl);
    size_t pos = 0;
    while (pos <= ns.size()) {
        size_t next = ns.find('/', pos);
        if (next == std::string::npos) next = ns.size();
        if (next > pos) r.levels.push_back(ns.substr(pos, next - pos));
        pos = next + 1;
    }
    if (r.levels.empty()) return std::nullopt;
    return r;
}

}  // namespace

json ReconcileReport::to_json() const {
    json j;
    j["table_buckets"] = table_buckets;
    j["tables"] = tables;
    j["intents"] = intents;
    j["findings"] = json::array();
    for (auto& f : findings) j["findings"].push_back({{"kind", f.kind}, {"bucket", f.bucket}, {"detail", f.detail}});
    return j;
}

Task<ReconcileReport> reconcile_catalog(std::shared_ptr<storage::IStorageBackend> sys_backend,
                                        storage::BucketRouter router) {
    ReconcileReport rep;
    auto note = [&](const char* kind, const std::string& bucket, std::string detail) {
        LOG_WARN("fsck tables: {} bucket={} {}", kind, bucket, detail);
        rep.findings.push_back({kind, bucket, std::move(detail)});
    };
    if (!co_await sys_backend->bucket_exists(storage::kSysBucketName)) co_return rep;
    auto markers = co_await TableBucketStore::load(sys_backend);
    auto snap = markers->snapshot();
    std::set<std::string> enabled;
    for (auto& [b, e] : *snap) {
        if (!e.enabled) continue;
        ++rep.table_buckets;
        if (co_await router.resolve(b).bucket_exists(b))
            enabled.insert(b);
        else
            note("tables.orphan_state", b, "table-bucket marker for a bucket that does not exist");
    }
    ObjectCatalogStore store(sys_backend);
    for (auto& bucket : co_await catalog_buckets(*sys_backend)) {
        if (!enabled.count(bucket)) {
            note("tables.orphan_state", bucket,
                 snap->count(bucket) ? "catalog state for a bucket that does not exist"
                                     : "catalog state for a bucket that is not table-enabled");
            continue;
        }
        auto& backend = router.resolve(bucket);
        std::set<std::string> intent_ids;
        std::vector<RenameIntent> intents;
        try {
            intents = co_await store.list_renames(bucket);
        } catch (const S3Error& e) {
            note("tables.malformed_entry", bucket, std::string("rename intents unreadable: ") + e.message);
        }
        for (auto& i : intents) intent_ids.insert(i.rename_id);
        for (auto& key : co_await sys_keys(*sys_backend, ObjectCatalogStore::bucket_root(bucket) + "ns/")) {
            auto ref = table_ref(key, bucket);
            if (!ref) continue;
            ++rep.tables;
            std::optional<Versioned<TableEntry>> e;
            try {
                e = co_await store.get_table(bucket, ref->levels, ref->name);
            } catch (const S3Error& err) {
                note("tables.malformed_entry", bucket, key + ": " + err.message);
                continue;
            }
            if (!e) continue;
            std::string ident = ns_display(ref->levels) + "." + ref->name;
            if (e->value.state == TableState::Deleted) continue;
            if (!co_await object_exists(backend, bucket, e->value.metadata_location))
                note("tables.dangling_pointer", bucket,
                     ident + " points at missing " + key_to_location(bucket, e->value.metadata_location));
            if (e->value.state == TableState::Renaming && !intent_ids.count(e->value.rename_id))
                note("tables.stale_renaming", bucket,
                     ident + " is RENAMING but intent " + e->value.rename_id + " does not exist");
        }
        for (auto& i : intents) {
            ++rep.intents;
            using Stage = RenameIntent::Stage;
            std::optional<Versioned<TableEntry>> src, dst;
            try {
                src = co_await store.get_table(bucket, i.src_levels, i.src_name);
                dst = co_await store.get_table(bucket, i.dst_levels, i.dst_name);
            } catch (const S3Error& err) {
                note("tables.malformed_entry", bucket, "rename " + i.rename_id + ": " + err.message);
                continue;
            }
            std::string ident = "rename " + i.rename_id + " (" + ns_display(i.src_levels) + "." + i.src_name + " → " +
                                ns_display(i.dst_levels) + "." + i.dst_name + ", " + rename_stage_name(i.stage) + ")";
            bool src_fenced = src && src->value.state == TableState::Renaming && src->value.rename_id == i.rename_id;
            bool dst_written = src && dst && dst->value.state == TableState::Active &&
                               dst->value.table_id == src->value.table_id;
            switch (i.stage) {
                case Stage::Prepared:
                    if (!src) note("tables.inconsistent_rename", bucket, ident + ": source entry is missing");
                    break;
                case Stage::SourceFenced:
                    if (!src_fenced) note("tables.inconsistent_rename", bucket, ident + ": source is not fenced");
                    break;
                case Stage::DestinationWritten:
                    if (!src_fenced && !(src && src->value.state == TableState::Deleted))
                        note("tables.inconsistent_rename", bucket, ident + ": source is neither fenced nor tombstoned");
                    if (!dst_written)
                        note("tables.inconsistent_rename", bucket, ident + ": destination is not the moved table");
                    break;
                case Stage::SourceTombstoned:
                    if (src && src->value.state != TableState::Deleted)
                        note("tables.inconsistent_rename", bucket, ident + ": source is not tombstoned");
                    if (!dst || dst->value.state != TableState::Active)
                        note("tables.inconsistent_rename", bucket, ident + ": destination is not active");
                    break;
            }
        }
    }
    co_return rep;
}

}  // namespace lights3::tables
