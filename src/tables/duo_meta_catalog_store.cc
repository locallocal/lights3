#include "tables/duo_meta_catalog_store.h"

#include <algorithm>

#include "core/log.h"
#include "s3/errors.h"
#include "tables/object_catalog_store.h"

namespace lights3::tables {

using nlohmann::json;
using s3::S3Error;
using s3::S3ErrorCode;
using storage::duostore::KvItem;
using storage::duostore::KvPut;
using Keys = ObjectCatalogStore;

namespace {

constexpr size_t kPage = 1000;

template <class T, class Parse>
std::optional<Versioned<T>> decode(const std::optional<KvItem>& item, const std::string& key, Parse parse) {
    if (!item) return std::nullopt;
    std::optional<T> e;
    try {
        e = parse(json::parse(item->value));
    } catch (const json::exception&) {
    }
    if (!e) throw S3Error(S3ErrorCode::InternalError, "catalog entry at " + key + " is malformed");
    return Versioned<T>{std::move(*e), item->etag};
}

}  // namespace

std::vector<KvItem> DuoMetaCatalogStore::scan_all(const std::string& prefix) {
    std::vector<KvItem> out;
    std::string after;
    for (;;) {
        auto page = meta_.kv_scan(prefix, after, kPage);
        if (page.empty()) break;
        after = page.back().key;
        for (auto& it : page) out.push_back(std::move(it));
        if (page.size() < kPage) break;
    }
    return out;
}

ListPage<std::string> DuoMetaCatalogStore::list_json_names(const std::string& dir, PageCursor cursor) {
    ListPage<std::string> page;
    std::string after = cursor.after.empty() ? std::string() : dir + cursor.after + ".json";
    for (;;) {
        auto items = meta_.kv_scan(dir, after, kPage);
        if (items.empty()) break;
        for (auto& it : items) {
            after = it.key;
            std::string n = it.key.substr(dir.size());
            if (n.size() < 5 || n.substr(n.size() - 5) != ".json" || n.find('/') != std::string::npos) continue;
            n.resize(n.size() - 5);
            if (static_cast<int>(page.items.size()) >= cursor.limit) {
                page.next_after = page.items.back();
                return page;
            }
            page.items.push_back(n);
        }
        if (items.size() < kPage) break;
    }
    return page;
}

// ---------- namespaces ----------

Task<std::optional<Versioned<NamespaceEntry>>> DuoMetaCatalogStore::get_namespace(std::string_view bucket,
                                                                                  const Levels& levels) {
    std::string key = Keys::ns_key(bucket, levels);
    auto v = decode<NamespaceEntry>(meta_.kv_get(key), key, namespace_from_json);
    if (v && v->value.levels != levels)
        throw S3Error(S3ErrorCode::InternalError, "catalog entry at " + key + " does not describe itself");
    co_return v;
}

Task<ListPage<std::string>> DuoMetaCatalogStore::list_child_namespaces(std::string_view bucket, const Levels& parent,
                                                                       PageCursor cursor) {
    ListPage<std::string> page;
    std::string dir = parent.empty() ? Keys::bucket_root(bucket) + "ns/" : Keys::ns_dir(bucket, parent);
    // one scan per child: read the first key under dir past `after`, take its first
    // segment, then jump past that segment's subtree (DEL sorts above [a-z0-9_-/])
    std::string after = cursor.after.empty() ? std::string() : dir + cursor.after + "/\x7f";
    for (;;) {
        auto items = meta_.kv_scan(dir, after, 1);
        if (items.empty()) break;
        std::string rel = items[0].key.substr(dir.size());
        auto slash = rel.find('/');
        if (slash == std::string::npos) {
            // "_ns.json" of the parent itself or a stray file: skip it
            after = items[0].key;
            continue;
        }
        std::string seg = rel.substr(0, slash);
        after = dir + seg + "/\x7f";
        if (seg == "tbl" || seg == "view" || seg.empty()) continue;
        if (static_cast<int>(page.items.size()) >= cursor.limit) {
            page.next_after = page.items.back();
            co_return page;
        }
        page.items.push_back(seg);
    }
    co_return page;
}

Task<bool> DuoMetaCatalogStore::namespace_has_children(std::string_view bucket, const Levels& levels) {
    std::string dir = Keys::ns_dir(bucket, levels);
    std::string marker = dir + "_ns.json";
    std::string tbl = dir + "tbl/";
    std::string view = dir + "view/";
    for (auto& it : scan_all(dir)) {
        if (it.key == marker) continue;
        bool is_view = it.key.rfind(view, 0) == 0;
        if (it.key.rfind(tbl, 0) != 0 && !is_view) co_return true;
        try {
            json j = json::parse(it.value);
            if (is_view) {
                auto v = view_from_json(j);
                if (v && v->state != TableState::Deleted) co_return true;
            } else {
                auto e = table_from_json(j);
                if (e && e->state != TableState::Deleted) co_return true;
            }
        } catch (const json::exception&) {
        }
    }
    co_return false;
}

Task<void> DuoMetaCatalogStore::put_namespace(std::string_view bucket, const NamespaceEntry& e,
                                              storage::PutCondition cond) {
    meta_.kv_put(Keys::ns_key(bucket, e.levels), to_json(e).dump(), cond);
    co_return;
}

Task<void> DuoMetaCatalogStore::delete_namespace(std::string_view bucket, const Levels& levels) {
    meta_.kv_delete(Keys::ns_key(bucket, levels));
    co_return;
}

// ---------- tables ----------

Task<std::optional<Versioned<TableEntry>>> DuoMetaCatalogStore::get_table(std::string_view bucket, const Levels& levels,
                                                                          std::string_view name) {
    std::string key = Keys::tbl_key(bucket, levels, name);
    auto v = decode<TableEntry>(meta_.kv_get(key), key, table_from_json);
    if (v && (v->value.levels != levels || v->value.name != name))
        throw S3Error(S3ErrorCode::InternalError, "catalog entry at " + key + " does not describe itself");
    co_return v;
}

Task<ListPage<std::string>> DuoMetaCatalogStore::list_tables(std::string_view bucket, const Levels& levels,
                                                             PageCursor cursor) {
    co_return list_json_names(Keys::tbl_dir(bucket, levels), cursor);
}

Task<std::string> DuoMetaCatalogStore::put_table(std::string_view bucket, const Levels& levels, std::string_view name,
                                                 const TableEntry& e, storage::PutCondition cond) {
    co_return meta_.kv_put(Keys::tbl_key(bucket, levels, name), to_json(e).dump(), cond);
}

Task<void> DuoMetaCatalogStore::delete_table(std::string_view bucket, const Levels& levels, std::string_view name) {
    meta_.kv_delete(Keys::tbl_key(bucket, levels, name));
    co_return;
}

// ---------- commits ----------

Task<std::optional<Versioned<CommitRecord>>> DuoMetaCatalogStore::get_commit(std::string_view bucket,
                                                                             std::string_view table_id,
                                                                             std::string_view commit_id) {
    std::string key = Keys::commit_key(bucket, table_id, commit_id);
    auto v = decode<CommitRecord>(meta_.kv_get(key), key, commit_from_json);
    if (v && (v->value.table_id != table_id || v->value.commit_id != commit_id))
        throw S3Error(S3ErrorCode::InternalError, "catalog entry at " + key + " does not describe itself");
    co_return v;
}

Task<void> DuoMetaCatalogStore::put_commit(std::string_view bucket, std::string_view table_id, const CommitRecord& r,
                                           storage::PutCondition cond) {
    meta_.kv_put(Keys::commit_key(bucket, table_id, r.commit_id), to_json(r).dump(), cond);
    co_return;
}

Task<std::vector<CommitRecord>> DuoMetaCatalogStore::list_commits(std::string_view bucket, std::string_view table_id) {
    std::vector<CommitRecord> out;
    for (auto& it : scan_all(Keys::commit_dir(bucket, table_id))) {
        std::optional<CommitRecord> r;
        try {
            r = commit_from_json(json::parse(it.value));
        } catch (const json::exception&) {
        }
        if (!r) {
            LOG_WARN("tables: skipping malformed commit record {}", it.key);
            continue;
        }
        out.push_back(std::move(*r));
    }
    co_return out;
}

Task<void> DuoMetaCatalogStore::delete_commit(std::string_view bucket, std::string_view table_id,
                                              std::string_view commit_id) {
    meta_.kv_delete(Keys::commit_key(bucket, table_id, commit_id));
    co_return;
}

// ---------- renames ----------

Task<std::optional<Versioned<RenameIntent>>> DuoMetaCatalogStore::get_rename(std::string_view bucket,
                                                                             std::string_view id) {
    std::string key = Keys::rename_key(bucket, id);
    auto v = decode<RenameIntent>(meta_.kv_get(key), key, rename_from_json);
    if (v && v->value.rename_id != id)
        throw S3Error(S3ErrorCode::InternalError, "catalog entry at " + key + " does not describe itself");
    co_return v;
}

Task<std::vector<RenameIntent>> DuoMetaCatalogStore::list_renames(std::string_view bucket) {
    std::vector<RenameIntent> out;
    for (auto& it : scan_all(Keys::rename_dir(bucket))) {
        std::optional<RenameIntent> r;
        try {
            r = rename_from_json(json::parse(it.value));
        } catch (const json::exception&) {
        }
        if (!r) {
            LOG_WARN("tables: skipping malformed rename intent {}", it.key);
            continue;
        }
        out.push_back(std::move(*r));
    }
    co_return out;
}

Task<std::string> DuoMetaCatalogStore::put_rename(std::string_view bucket, const RenameIntent& r,
                                                  storage::PutCondition cond) {
    co_return meta_.kv_put(Keys::rename_key(bucket, r.rename_id), to_json(r).dump(), cond);
}

Task<void> DuoMetaCatalogStore::delete_rename(std::string_view bucket, std::string_view id) {
    meta_.kv_delete(Keys::rename_key(bucket, id));
    co_return;
}

// ---------- maintenance settings ----------

Task<std::optional<MaintenanceConfig>> DuoMetaCatalogStore::get_maintenance_config(std::string_view bucket,
                                                                                   const Levels& levels,
                                                                                   std::string_view name) {
    std::string key = Keys::maint_key(bucket, levels, name);
    auto v = decode<MaintenanceConfig>(meta_.kv_get(key), key, maintenance_from_json);
    if (!v) co_return std::nullopt;
    co_return v->value;
}

Task<void> DuoMetaCatalogStore::put_maintenance_config(std::string_view bucket, const Levels& levels,
                                                       std::string_view name, const MaintenanceConfig& c) {
    meta_.kv_put(Keys::maint_key(bucket, levels, name), to_json(c).dump(), {});
    co_return;
}

Task<void> DuoMetaCatalogStore::delete_maintenance_config(std::string_view bucket, const Levels& levels,
                                                          std::string_view name) {
    meta_.kv_delete(Keys::maint_key(bucket, levels, name));
    co_return;
}

// ---------- views ----------

Task<std::optional<Versioned<ViewEntry>>> DuoMetaCatalogStore::get_view(std::string_view bucket, const Levels& levels,
                                                                        std::string_view name) {
    std::string key = Keys::view_key(bucket, levels, name);
    auto v = decode<ViewEntry>(meta_.kv_get(key), key, view_from_json);
    if (v && (v->value.levels != levels || v->value.name != name))
        throw S3Error(S3ErrorCode::InternalError, "catalog entry at " + key + " does not describe itself");
    co_return v;
}

Task<ListPage<std::string>> DuoMetaCatalogStore::list_views(std::string_view bucket, const Levels& levels,
                                                            PageCursor cursor) {
    co_return list_json_names(Keys::view_dir(bucket, levels), cursor);
}

Task<std::string> DuoMetaCatalogStore::put_view(std::string_view bucket, const Levels& levels, std::string_view name,
                                                const ViewEntry& e, storage::PutCondition cond) {
    co_return meta_.kv_put(Keys::view_key(bucket, levels, name), to_json(e).dump(), cond);
}

Task<void> DuoMetaCatalogStore::delete_view(std::string_view bucket, const Levels& levels, std::string_view name) {
    meta_.kv_delete(Keys::view_key(bucket, levels, name));
    co_return;
}

// ---------- atomic commit ----------

Task<std::string> DuoMetaCatalogStore::commit_atomic(std::string_view bucket, const Levels& levels,
                                                     std::string_view name, const TableEntry& next,
                                                     storage::PutCondition table_cond, const CommitRecord& committed) {
    std::vector<KvPut> puts;
    puts.push_back({Keys::tbl_key(bucket, levels, name), to_json(next).dump(), table_cond});
    storage::PutCondition fresh;
    fresh.if_none_match = true;
    puts.push_back(
        {Keys::commit_key(bucket, committed.table_id, committed.commit_id), to_json(committed).dump(), fresh});
    auto etags = meta_.kv_put_batch(puts);
    co_return etags.front();
}

// ---------- raw export / import, bucket state ----------

Task<std::vector<RawEntry>> DuoMetaCatalogStore::export_raw(std::string_view bucket) {
    std::vector<RawEntry> out;
    for (auto& it : scan_all(Keys::bucket_root(bucket))) out.push_back({it.key, std::move(it.value)});
    co_return out;
}

Task<void> DuoMetaCatalogStore::import_raw(std::string_view bucket, const RawEntry& e) {
    if (e.key.rfind(Keys::bucket_root(bucket), 0) != 0)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "catalog key " + e.key + " does not belong to bucket " + std::string(bucket));
    meta_.kv_put(e.key, e.body, {});
    co_return;
}

Task<void> DuoMetaCatalogStore::delete_bucket_state(std::string_view bucket) {
    for (auto& it : scan_all(Keys::bucket_root(bucket))) meta_.kv_delete(it.key);
    co_return;
}

Task<bool> DuoMetaCatalogStore::bucket_state_empty(std::string_view bucket) {
    std::string root = Keys::bucket_root(bucket) + "ns/";
    for (auto& it : scan_all(root)) {
        std::string rel = it.key.substr(root.size());
        if (rel.size() >= 8 && rel.substr(rel.size() - 8) == "_ns.json") co_return false;
        auto tbl = rel.find("/tbl/");
        auto view = rel.find("/view/");
        if (tbl == std::string::npos && view == std::string::npos) continue;
        try {
            json j = json::parse(it.value);
            if (tbl != std::string::npos) {
                auto e = table_from_json(j);
                if (e && e->state != TableState::Deleted) co_return false;
            } else {
                auto v = view_from_json(j);
                if (v && v->state != TableState::Deleted) co_return false;
            }
        } catch (const json::exception&) {
        }
    }
    co_return true;
}

}  // namespace lights3::tables
