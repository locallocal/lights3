#include "tables/object_catalog_store.h"

#include <algorithm>

#include "core/log.h"
#include "s3/errors.h"

namespace lights3::tables {

using nlohmann::json;
using s3::S3Error;
using s3::S3ErrorCode;

// ---------- entity JSON ----------

namespace {

constexpr size_t kMaxEntityBody = 1024 * 1024;

void reject_unknown(const json& j, std::initializer_list<const char*> allowed) {
    for (auto& [k, v] : j.items()) {
        bool ok = false;
        for (auto a : allowed)
            if (k == a) ok = true;
        if (!ok) throw std::runtime_error("unknown field '" + k + "'");
    }
}

Levels levels_of(const json& j) {
    Levels out;
    for (auto& e : j) out.push_back(e.get<std::string>());
    return out;
}

template <class F>
auto guarded(F&& f) -> decltype(f()) {
    try {
        return f();
    } catch (const std::exception& e) {
        LOG_WARN("tables: malformed catalog entry: {}", e.what());
        return std::nullopt;
    }
}

}  // namespace

const char* table_state_name(TableState s) {
    switch (s) {
        case TableState::Active:
            return "ACTIVE";
        case TableState::Renaming:
            return "RENAMING";
        case TableState::Deleted:
            return "DELETED";
    }
    return "ACTIVE";
}

std::optional<TableState> table_state_from_name(std::string_view s) {
    if (s == "ACTIVE") return TableState::Active;
    if (s == "RENAMING") return TableState::Renaming;
    if (s == "DELETED") return TableState::Deleted;
    return std::nullopt;
}

const char* rename_stage_name(RenameIntent::Stage s) {
    switch (s) {
        case RenameIntent::Stage::Prepared:
            return "PREPARED";
        case RenameIntent::Stage::SourceFenced:
            return "SOURCE_FENCED";
        case RenameIntent::Stage::DestinationWritten:
            return "DESTINATION_WRITTEN";
        case RenameIntent::Stage::SourceTombstoned:
            return "SOURCE_TOMBSTONED";
    }
    return "PREPARED";
}

std::optional<RenameIntent::Stage> rename_stage_from_name(std::string_view s) {
    if (s == "PREPARED") return RenameIntent::Stage::Prepared;
    if (s == "SOURCE_FENCED") return RenameIntent::Stage::SourceFenced;
    if (s == "DESTINATION_WRITTEN") return RenameIntent::Stage::DestinationWritten;
    if (s == "SOURCE_TOMBSTONED") return RenameIntent::Stage::SourceTombstoned;
    return std::nullopt;
}

json to_json(const NamespaceEntry& e) {
    json j;
    j["version"] = e.version;
    j["levels"] = e.levels;
    j["properties"] = e.properties;
    j["created_unix"] = e.created_unix;
    j["updated_unix"] = e.updated_unix;
    return j;
}

json to_json(const TableEntry& e) {
    json j;
    j["version"] = e.version;
    j["levels"] = e.levels;
    j["name"] = e.name;
    j["table_id"] = e.table_id;
    j["table_uuid"] = e.table_uuid;
    j["location"] = e.location;
    j["metadata_location"] = e.metadata_location;
    j["version_token"] = e.version_token;
    j["generation"] = e.generation;
    j["format_version"] = e.format_version;
    j["state"] = table_state_name(e.state);
    j["rename_id"] = e.rename_id;
    j["created_unix"] = e.created_unix;
    j["updated_unix"] = e.updated_unix;
    return j;
}

json to_json(const CommitRecord& r) {
    json j;
    j["version"] = r.version;
    j["commit_id"] = r.commit_id;
    j["table_id"] = r.table_id;
    j["expected_token"] = r.expected_token;
    j["new_token"] = r.new_token;
    j["prev_metadata_location"] = r.prev_metadata_location;
    j["new_metadata_location"] = r.new_metadata_location;
    j["status"] = r.status;
    j["request_digest"] = r.request_digest;
    j["created_unix"] = r.created_unix;
    return j;
}

json to_json(const RenameIntent& r) {
    json j;
    j["version"] = r.version;
    j["rename_id"] = r.rename_id;
    j["src_levels"] = r.src_levels;
    j["dst_levels"] = r.dst_levels;
    j["src_name"] = r.src_name;
    j["dst_name"] = r.dst_name;
    j["src_etag"] = r.src_etag;
    j["stage"] = rename_stage_name(r.stage);
    j["created_unix"] = r.created_unix;
    return j;
}

std::optional<NamespaceEntry> namespace_from_json(const json& j) {
    return guarded([&]() -> std::optional<NamespaceEntry> {
        reject_unknown(j, {"version", "levels", "properties", "created_unix", "updated_unix"});
        if (j.at("version").get<int>() != 1) return std::nullopt;
        NamespaceEntry e;
        e.levels = levels_of(j.at("levels"));
        // items() on a temporary dangles: bind the value first
        json props = j.value("properties", json::object());
        for (auto& [k, v] : props.items()) e.properties[k] = v.get<std::string>();
        e.created_unix = j.value("created_unix", int64_t(0));
        e.updated_unix = j.value("updated_unix", int64_t(0));
        return e;
    });
}

std::optional<TableEntry> table_from_json(const json& j) {
    return guarded([&]() -> std::optional<TableEntry> {
        reject_unknown(
            j, {"version", "levels", "name", "table_id", "table_uuid", "location", "metadata_location", "version_token",
                "generation", "format_version", "state", "rename_id", "created_unix", "updated_unix"});
        if (j.at("version").get<int>() != 1) return std::nullopt;
        TableEntry e;
        e.levels = levels_of(j.at("levels"));
        e.name = j.at("name").get<std::string>();
        e.table_id = j.at("table_id").get<std::string>();
        e.table_uuid = j.at("table_uuid").get<std::string>();
        e.location = j.at("location").get<std::string>();
        e.metadata_location = j.at("metadata_location").get<std::string>();
        e.version_token = j.at("version_token").get<std::string>();
        e.generation = j.at("generation").get<uint64_t>();
        e.format_version = j.at("format_version").get<int>();
        auto st = table_state_from_name(j.at("state").get<std::string>());
        if (!st) return std::nullopt;
        e.state = *st;
        e.rename_id = j.value("rename_id", std::string());
        e.created_unix = j.value("created_unix", int64_t(0));
        e.updated_unix = j.value("updated_unix", int64_t(0));
        return e;
    });
}

std::optional<CommitRecord> commit_from_json(const json& j) {
    return guarded([&]() -> std::optional<CommitRecord> {
        reject_unknown(j, {"version", "commit_id", "table_id", "expected_token", "new_token", "prev_metadata_location",
                           "new_metadata_location", "status", "request_digest", "created_unix"});
        if (j.at("version").get<int>() != 1) return std::nullopt;
        CommitRecord r;
        r.commit_id = j.at("commit_id").get<std::string>();
        r.table_id = j.at("table_id").get<std::string>();
        r.expected_token = j.at("expected_token").get<std::string>();
        r.new_token = j.at("new_token").get<std::string>();
        r.prev_metadata_location = j.at("prev_metadata_location").get<std::string>();
        r.new_metadata_location = j.at("new_metadata_location").get<std::string>();
        r.status = j.at("status").get<std::string>();
        if (r.status != "STAGED" && r.status != "COMMITTED") return std::nullopt;
        r.request_digest = j.value("request_digest", std::string());
        r.created_unix = j.value("created_unix", int64_t(0));
        return r;
    });
}

std::optional<RenameIntent> rename_from_json(const json& j) {
    return guarded([&]() -> std::optional<RenameIntent> {
        reject_unknown(j, {"version", "rename_id", "src_levels", "dst_levels", "src_name", "dst_name", "src_etag",
                           "stage", "created_unix"});
        if (j.at("version").get<int>() != 1) return std::nullopt;
        RenameIntent r;
        r.rename_id = j.at("rename_id").get<std::string>();
        r.src_levels = levels_of(j.at("src_levels"));
        r.dst_levels = levels_of(j.at("dst_levels"));
        r.src_name = j.at("src_name").get<std::string>();
        r.dst_name = j.at("dst_name").get<std::string>();
        r.src_etag = j.at("src_etag").get<std::string>();
        auto st = rename_stage_from_name(j.at("stage").get<std::string>());
        if (!st) return std::nullopt;
        r.stage = *st;
        r.created_unix = j.value("created_unix", int64_t(0));
        return r;
    });
}

// ---------- key layout ----------

std::string ObjectCatalogStore::bucket_root(std::string_view bucket) {
    return std::string(kRoot) + std::string(bucket) + "/";
}
std::string ObjectCatalogStore::ns_dir(std::string_view bucket, const Levels& levels) {
    return bucket_root(bucket) + "ns/" + ns_path(levels) + "/";
}
std::string ObjectCatalogStore::ns_key(std::string_view bucket, const Levels& levels) {
    return ns_dir(bucket, levels) + "_ns.json";
}
std::string ObjectCatalogStore::tbl_dir(std::string_view bucket, const Levels& levels) {
    return ns_dir(bucket, levels) + "tbl/";
}
std::string ObjectCatalogStore::tbl_key(std::string_view bucket, const Levels& levels, std::string_view name) {
    return tbl_dir(bucket, levels) + std::string(name) + ".json";
}
std::string ObjectCatalogStore::commit_dir(std::string_view bucket, std::string_view table_id) {
    return bucket_root(bucket) + "commits/" + std::string(table_id) + "/";
}
std::string ObjectCatalogStore::commit_key(std::string_view bucket, std::string_view table_id,
                                           std::string_view commit_id) {
    return commit_dir(bucket, table_id) + std::string(commit_id) + ".json";
}
std::string ObjectCatalogStore::rename_dir(std::string_view bucket) { return bucket_root(bucket) + "renames/"; }
std::string ObjectCatalogStore::rename_key(std::string_view bucket, std::string_view id) {
    return rename_dir(bucket) + std::string(id) + ".json";
}

// ---------- raw object IO ----------

Task<std::optional<ObjectCatalogStore::Raw>> ObjectCatalogStore::read(std::string key) {
    storage::ObjectStream stream;
    try {
        stream = co_await backend_->get_object(storage::kSysBucketName, key, std::nullopt);
    } catch (const S3Error& e) {
        if (e.code == S3ErrorCode::NoSuchKey || e.code == S3ErrorCode::NoSuchBucket) co_return std::nullopt;
        throw;
    }
    Raw raw;
    raw.etag = stream.meta.etag;
    std::byte buf[16 * 1024];
    for (;;) {
        size_t n = co_await stream.body->read(std::span(buf));
        if (n == 0) break;
        if (raw.body.size() + n > kMaxEntityBody)
            throw S3Error(S3ErrorCode::InternalError, "catalog entry " + key + " exceeds the size limit");
        raw.body.append(reinterpret_cast<const char*>(buf), n);
    }
    co_return raw;
}

Task<void> ObjectCatalogStore::ensure_sys_bucket() {
    if (co_await backend_->bucket_exists(storage::kSysBucketName)) co_return;
    try {
        co_await backend_->create_bucket(storage::kSysBucketName);
    } catch (const S3Error& e) {
        if (e.code != S3ErrorCode::BucketAlreadyExists && e.code != S3ErrorCode::BucketAlreadyOwnedByYou) throw;
    }
}

Task<std::string> ObjectCatalogStore::write(std::string key, std::string body, storage::PutCondition cond) {
    co_await ensure_sys_bucket();
    storage::ObjectMeta meta;
    meta.content_type = "application/json";
    http::StringBodyReader reader(std::move(body));
    auto res = co_await backend_->put_object(storage::kSysBucketName, key, std::move(meta), reader, cond);
    co_return res.etag;
}

Task<void> ObjectCatalogStore::remove(std::string key) {
    try {
        co_await backend_->delete_object(storage::kSysBucketName, key);
    } catch (const S3Error& e) {
        if (e.code != S3ErrorCode::NoSuchKey && e.code != S3ErrorCode::NoSuchBucket) throw;
    }
}

Task<std::vector<std::string>> ObjectCatalogStore::list_keys(std::string prefix) {
    std::vector<std::string> out;
    if (!co_await backend_->bucket_exists(storage::kSysBucketName)) co_return out;
    storage::ListOptions opt;
    opt.prefix = prefix;
    for (;;) {
        auto page = co_await backend_->list_objects(storage::kSysBucketName, opt);
        for (auto& o : page.objects) out.push_back(o.key);
        if (!page.is_truncated) break;
        opt.start_after = page.next_token;
    }
    co_return out;
}

// ---------- namespaces ----------

Task<std::optional<Versioned<NamespaceEntry>>> ObjectCatalogStore::get_namespace(std::string_view bucket,
                                                                                 const Levels& levels) {
    std::string key = ns_key(bucket, levels);
    auto raw = co_await read(key);
    if (!raw) co_return std::nullopt;
    std::optional<NamespaceEntry> e;
    try {
        e = namespace_from_json(json::parse(raw->body));
    } catch (const json::exception&) {
    }
    if (!e || e->levels != levels)
        throw S3Error(S3ErrorCode::InternalError, "catalog entry at " + key + " does not describe itself");
    co_return Versioned<NamespaceEntry>{std::move(*e), raw->etag};
}

Task<ListPage<std::string>> ObjectCatalogStore::list_child_namespaces(std::string_view bucket, const Levels& parent,
                                                                      PageCursor cursor) {
    ListPage<std::string> page;
    if (!co_await backend_->bucket_exists(storage::kSysBucketName)) co_return page;
    std::string dir = parent.empty() ? bucket_root(bucket) + "ns/" : ns_dir(bucket, parent);
    storage::ListOptions opt;
    opt.prefix = dir;
    opt.delimiter = "/";
    opt.max_keys = std::max(1, cursor.limit) + 2;
    // keys strictly after every key below the last listed child: DEL sorts above [a-z0-9_-/]
    if (!cursor.after.empty()) opt.start_after = dir + cursor.after + "/\x7f";
    for (;;) {
        auto res = co_await backend_->list_objects(storage::kSysBucketName, opt);
        for (auto& cp : res.common_prefixes) {
            std::string seg = cp.substr(dir.size());
            if (!seg.empty() && seg.back() == '/') seg.pop_back();
            if (seg == "tbl" || seg.empty()) continue;
            if (static_cast<int>(page.items.size()) >= cursor.limit) {
                page.next_after = page.items.back();
                co_return page;
            }
            page.items.push_back(seg);
        }
        if (!res.is_truncated) break;
        opt.start_after = res.next_token;
    }
    co_return page;
}

// Active children only: a dropped-table tombstone under tbl/ is not evidence of a live
// namespace (otherwise a namespace could never be dropped before step ④'s cleanup)
Task<bool> ObjectCatalogStore::namespace_has_children(std::string_view bucket, const Levels& levels) {
    if (!co_await backend_->bucket_exists(storage::kSysBucketName)) co_return false;
    std::string dir = ns_dir(bucket, levels);
    std::string marker = dir + "_ns.json";
    std::string tbl = dir + "tbl/";
    storage::ListOptions opt;
    opt.prefix = dir;
    opt.max_keys = 100;
    for (;;) {
        auto res = co_await backend_->list_objects(storage::kSysBucketName, opt);
        for (auto& o : res.objects) {
            if (o.key == marker) continue;
            if (o.key.rfind(tbl, 0) != 0) co_return true;
            std::string name = o.key.substr(tbl.size());
            if (name.size() < 5 || name.substr(name.size() - 5) != ".json") continue;
            name.resize(name.size() - 5);
            auto e = co_await get_table(bucket, levels, name);
            if (e && e->value.state != TableState::Deleted) co_return true;
        }
        if (!res.is_truncated) break;
        opt.start_after = res.next_token;
    }
    co_return false;
}

Task<void> ObjectCatalogStore::put_namespace(std::string_view bucket, const NamespaceEntry& e,
                                             storage::PutCondition cond) {
    co_await write(ns_key(bucket, e.levels), to_json(e).dump(), cond);
}

Task<void> ObjectCatalogStore::delete_namespace(std::string_view bucket, const Levels& levels) {
    co_await remove(ns_key(bucket, levels));
}

// ---------- tables ----------

Task<std::optional<Versioned<TableEntry>>> ObjectCatalogStore::get_table(std::string_view bucket, const Levels& levels,
                                                                         std::string_view name) {
    std::string key = tbl_key(bucket, levels, name);
    auto raw = co_await read(key);
    if (!raw) co_return std::nullopt;
    std::optional<TableEntry> e;
    try {
        e = table_from_json(json::parse(raw->body));
    } catch (const json::exception&) {
    }
    if (!e || e->levels != levels || e->name != name)
        throw S3Error(S3ErrorCode::InternalError, "catalog entry at " + key + " does not describe itself");
    co_return Versioned<TableEntry>{std::move(*e), raw->etag};
}

Task<ListPage<std::string>> ObjectCatalogStore::list_tables(std::string_view bucket, const Levels& levels,
                                                            PageCursor cursor) {
    ListPage<std::string> page;
    if (!co_await backend_->bucket_exists(storage::kSysBucketName)) co_return page;
    std::string dir = tbl_dir(bucket, levels);
    storage::ListOptions opt;
    opt.prefix = dir;
    opt.max_keys = std::max(1, cursor.limit) + 1;
    if (!cursor.after.empty()) opt.start_after = dir + cursor.after + ".json";
    for (;;) {
        auto res = co_await backend_->list_objects(storage::kSysBucketName, opt);
        for (auto& o : res.objects) {
            std::string n = o.key.substr(dir.size());
            if (n.size() < 5 || n.substr(n.size() - 5) != ".json" || n.find('/') != std::string::npos) continue;
            n.resize(n.size() - 5);
            if (static_cast<int>(page.items.size()) >= cursor.limit) {
                page.next_after = page.items.back();
                co_return page;
            }
            page.items.push_back(n);
        }
        if (!res.is_truncated) break;
        opt.start_after = res.next_token;
    }
    co_return page;
}

Task<std::string> ObjectCatalogStore::put_table(std::string_view bucket, const Levels& levels, std::string_view name,
                                                const TableEntry& e, storage::PutCondition cond) {
    co_return co_await write(tbl_key(bucket, levels, name), to_json(e).dump(), cond);
}

Task<void> ObjectCatalogStore::delete_table(std::string_view bucket, const Levels& levels, std::string_view name) {
    co_await remove(tbl_key(bucket, levels, name));
}

// ---------- commits ----------

Task<std::optional<Versioned<CommitRecord>>> ObjectCatalogStore::get_commit(std::string_view bucket,
                                                                            std::string_view table_id,
                                                                            std::string_view commit_id) {
    std::string key = commit_key(bucket, table_id, commit_id);
    auto raw = co_await read(key);
    if (!raw) co_return std::nullopt;
    std::optional<CommitRecord> r;
    try {
        r = commit_from_json(json::parse(raw->body));
    } catch (const json::exception&) {
    }
    if (!r || r->table_id != table_id || r->commit_id != commit_id)
        throw S3Error(S3ErrorCode::InternalError, "catalog entry at " + key + " does not describe itself");
    co_return Versioned<CommitRecord>{std::move(*r), raw->etag};
}

Task<void> ObjectCatalogStore::put_commit(std::string_view bucket, std::string_view table_id, const CommitRecord& r,
                                          storage::PutCondition cond) {
    co_await write(commit_key(bucket, table_id, r.commit_id), to_json(r).dump(), cond);
}

Task<std::vector<CommitRecord>> ObjectCatalogStore::list_commits(std::string_view bucket, std::string_view table_id) {
    std::vector<CommitRecord> out;
    for (auto& key : co_await list_keys(commit_dir(bucket, table_id))) {
        auto raw = co_await read(key);
        if (!raw) continue;
        std::optional<CommitRecord> r;
        try {
            r = commit_from_json(json::parse(raw->body));
        } catch (const json::exception&) {
        }
        if (!r) {
            LOG_WARN("tables: skipping malformed commit record {}", key);
            continue;
        }
        out.push_back(std::move(*r));
    }
    co_return out;
}

// ---------- renames ----------

Task<std::optional<Versioned<RenameIntent>>> ObjectCatalogStore::get_rename(std::string_view bucket,
                                                                            std::string_view id) {
    std::string key = rename_key(bucket, id);
    auto raw = co_await read(key);
    if (!raw) co_return std::nullopt;
    std::optional<RenameIntent> r;
    try {
        r = rename_from_json(json::parse(raw->body));
    } catch (const json::exception&) {
    }
    if (!r || r->rename_id != id)
        throw S3Error(S3ErrorCode::InternalError, "catalog entry at " + key + " does not describe itself");
    co_return Versioned<RenameIntent>{std::move(*r), raw->etag};
}

Task<std::vector<RenameIntent>> ObjectCatalogStore::list_renames(std::string_view bucket) {
    std::vector<RenameIntent> out;
    for (auto& key : co_await list_keys(rename_dir(bucket))) {
        auto raw = co_await read(key);
        if (!raw) continue;
        std::optional<RenameIntent> r;
        try {
            r = rename_from_json(json::parse(raw->body));
        } catch (const json::exception&) {
        }
        if (!r) {
            LOG_WARN("tables: skipping malformed rename intent {}", key);
            continue;
        }
        out.push_back(std::move(*r));
    }
    co_return out;
}

Task<void> ObjectCatalogStore::put_rename(std::string_view bucket, const RenameIntent& r, storage::PutCondition cond) {
    co_await write(rename_key(bucket, r.rename_id), to_json(r).dump(), cond);
}

Task<void> ObjectCatalogStore::delete_rename(std::string_view bucket, std::string_view id) {
    co_await remove(rename_key(bucket, id));
}

// ---------- bucket state ----------

Task<void> ObjectCatalogStore::delete_bucket_state(std::string_view bucket) {
    for (auto& key : co_await list_keys(bucket_root(bucket))) co_await remove(key);
}

// Empty = no explicit namespace and no active table anywhere under ns/ (tombstones and
// commit records do not count; DeleteBucket drops them with delete_bucket_state)
Task<bool> ObjectCatalogStore::bucket_state_empty(std::string_view bucket) {
    if (!co_await backend_->bucket_exists(storage::kSysBucketName)) co_return true;
    std::string root = bucket_root(bucket) + "ns/";
    storage::ListOptions opt;
    opt.prefix = root;
    opt.max_keys = 100;
    for (;;) {
        auto res = co_await backend_->list_objects(storage::kSysBucketName, opt);
        for (auto& o : res.objects) {
            std::string rel = o.key.substr(root.size());
            if (rel.size() >= 8 && rel.substr(rel.size() - 8) == "_ns.json") co_return false;
            auto tbl = rel.find("/tbl/");
            if (tbl == std::string::npos) continue;
            auto raw = co_await read(o.key);
            if (!raw) continue;
            std::optional<TableEntry> e;
            try {
                e = table_from_json(json::parse(raw->body));
            } catch (const json::exception&) {
            }
            if (e && e->state != TableState::Deleted) co_return false;
        }
        if (!res.is_truncated) break;
        opt.start_after = res.next_token;
    }
    co_return true;
}

}  // namespace lights3::tables
