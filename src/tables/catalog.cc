#include "tables/catalog.h"

#include <algorithm>
#include <chrono>
#include <set>

#include "core/fault.h"
#include "core/log.h"
#include "core/util/crypto.h"
#include "s3/errors.h"
#include "tables/iceberg/metadata.h"
#include "tables/iceberg/requirements.h"
#include "tables/iceberg/snapshots.h"
#include "tables/iceberg/transition.h"
#include "tables/iceberg/updates.h"
#include "tables/iceberg/view_metadata.h"
#include "tables/rest_error.h"

namespace lights3::tables {

using nlohmann::json;
using s3::S3Error;
using s3::S3ErrorCode;

namespace {

bool is_precondition(const S3Error& e) {
    return e.code == S3ErrorCode::PreconditionFailed || e.code == S3ErrorCode::NoSuchKey;
}

const std::vector<double> kCommitBounds = {0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10};

}  // namespace

Catalog::Catalog(std::shared_ptr<ITableCatalogStore> store, std::shared_ptr<TableBucketStore> buckets,
                 storage::BucketRouter router, std::shared_ptr<ThreadPool> pool, TablesConfig cfg, MetricsScope metrics)
    : store_(std::move(store)),
      buckets_(std::move(buckets)),
      router_(std::move(router)),
      pool_(std::move(pool)),
      cfg_(std::move(cfg)),
      metrics_(std::move(metrics)) {
    commits_ok_ = metrics_.counter("lights3_tables_commits_total", "Table commits by outcome", {{"result", "ok"}});
    commits_conflict_ = metrics_.counter("lights3_tables_commits_total", "Table commits by outcome",
                                         {{"result", "conflict"}});
    commits_error_ = metrics_.counter("lights3_tables_commits_total", "Table commits by outcome",
                                      {{"result", "error"}});
    commit_seconds_ = metrics_.histogram("lights3_tables_commit_seconds", "Wall time of a table commit", kCommitBounds);
    validation_files_ = metrics_.counter("lights3_tables_validation_files_total",
                                         "Data / delete files verified by the deep snapshot check");
    validation_skipped_ = metrics_.counter("lights3_tables_validation_skipped_total",
                                           "Snapshots whose Avro codec could not be read (validation skipped)");
}

// ---------- helpers ----------

std::shared_ptr<Catalog::TableLock> Catalog::table_lock(std::string_view bucket, std::string_view table_id) {
    std::string key = std::string(bucket) + "/" + std::string(table_id);
    std::lock_guard lk(locks_mu_);
    auto& slot = locks_[key];
    if (!slot) slot = std::make_shared<TableLock>();
    return slot;
}

Task<void> Catalog::schedule() {
    if (pool_) co_await pool_->schedule();
}

std::string Catalog::new_token() { return "t-" + random_hex16(); }

std::string Catalog::request_digest(const CommitRequest& req) {
    json j;
    j["requirements"] = req.requirements;
    j["updates"] = req.updates;
    return util::sha256_hex(iceberg::canonical(j));
}

std::string Catalog::to_client_location(std::string_view bucket, std::string_view key) {
    return key_to_location(bucket, key);
}

json Catalog::client_metadata(std::string_view, const json& md) const { return md; }

std::string Catalog::metadata_key(const TableBucketEntry& tb, const Levels& levels, std::string_view name, uint64_t gen,
                                  std::string_view token) const {
    char num[16];
    std::snprintf(num, sizeof(num), "%05llu", static_cast<unsigned long long>(gen));
    return tb.reserved_prefix + ns_path(levels) + "/" + std::string(name) + "/metadata/" + num + "-" +
           std::string(token) + ".metadata.json";
}

Task<json> Catalog::read_metadata(storage::IStorageBackend& backend, std::string_view bucket, std::string_view key) {
    storage::ObjectStream stream;
    try {
        stream = co_await backend.get_object(bucket, key, std::nullopt);
    } catch (const S3Error& e) {
        if (e.code == S3ErrorCode::NoSuchKey || e.code == S3ErrorCode::NoSuchBucket)
            throw internal("table metadata file " + std::string(key) + " is missing");
        throw;
    }
    std::string text;
    std::byte buf[64 * 1024];
    for (;;) {
        size_t n = co_await stream.body->read(std::span(buf));
        if (n == 0) break;
        if (text.size() + n > cfg_.metadata_max_size) throw bad_request("table metadata exceeds the size limit");
        text.append(reinterpret_cast<const char*>(buf), n);
    }
    co_await schedule();
    co_return iceberg::parse_and_validate(text, cfg_.metadata_max_size);
}

Task<std::string> Catalog::write_metadata(storage::IStorageBackend& backend, std::string_view bucket,
                                          std::string_view key, std::string body, bool if_none_match) {
    storage::ObjectMeta meta;
    meta.content_type = "application/json";
    storage::PutCondition cond;
    cond.if_none_match = if_none_match;
    http::StringBodyReader reader(std::move(body));
    auto res = co_await backend.put_object(bucket, key, std::move(meta), reader, cond);
    co_return res.etag;
}

Task<bool> Catalog::object_exists(storage::IStorageBackend& backend, std::string_view bucket, std::string_view key) {
    try {
        co_await backend.head_object(bucket, key);
    } catch (const S3Error& e) {
        if (e.code == S3ErrorCode::NoSuchKey || e.code == S3ErrorCode::NoSuchBucket) co_return false;
        throw;
    }
    co_return true;
}

Task<void> Catalog::best_effort_delete(storage::IStorageBackend& backend, std::string_view bucket,
                                       std::string_view key) {
    try {
        co_await backend.delete_object(bucket, key);
    } catch (const std::exception& e) {
        LOG_WARN("tables: could not remove {}/{} after a failed commit: {}", bucket, key, e.what());
    }
}

Task<Versioned<TableEntry>> Catalog::require_active(std::string_view bucket, const Levels& levels,
                                                    std::string_view name, bool writer) {
    auto cur = co_await store_->get_table(bucket, levels, name);
    if (cur && cur->value.state == TableState::Renaming && writer) {
        // a writer drives the pending intent first (design §5.6); it may complete the
        // rename (the name is then gone or tombstoned) or roll it back (active again)
        co_await maybe_recover_renames(bucket, true);
        cur = co_await store_->get_table(bucket, levels, name);
    }
    if (!cur || cur->value.state == TableState::Deleted)
        throw not_found_table("table " + ns_display(levels) + "." + std::string(name) + " does not exist");
    if (cur->value.state == TableState::Renaming)
        throw unavailable("table " + ns_display(levels) + "." + std::string(name) + " is being renamed; retry");
    co_return std::move(*cur);
}

Task<void> Catalog::maybe_recover_renames(std::string_view bucket, bool force) {
    if (!force) {
        std::lock_guard lk(locks_mu_);
        unsigned& n = recovery_calls_[std::string(bucket)];
        bool due = n % kRenameRecoveryEvery == 0;
        ++n;
        if (!due) co_return;
    }
    co_await recover_renames(bucket);
}

Task<int> Catalog::recover_renames(std::string_view bucket) {
    co_return co_await tables::recover_renames(*store_, bucket, cfg_.maintenance.tombstone_ttl_sec);
}

iceberg::DeepCheckOptions Catalog::deep_options() const {
    iceberg::DeepCheckOptions opt;
    opt.concurrency = cfg_.validate_concurrency;
    return opt;
}

std::string Catalog::metadata_dir(const TableBucketEntry& tb, const Levels& levels, std::string_view name) const {
    return tb.reserved_prefix + ns_path(levels) + "/" + std::string(name) + "/metadata/";
}

namespace {

// RFC 7232 If-None-Match against the entry ETag: quoted, weak (W/) and list forms, "*"
bool etag_matches(std::string_view header, std::string_view etag) {
    size_t pos = 0;
    while (pos <= header.size()) {
        size_t comma = header.find(',', pos);
        if (comma == std::string_view::npos) comma = header.size();
        std::string_view tok = header.substr(pos, comma - pos);
        while (!tok.empty() && (tok.front() == ' ' || tok.front() == '\t')) tok.remove_prefix(1);
        while (!tok.empty() && (tok.back() == ' ' || tok.back() == '\t')) tok.remove_suffix(1);
        if (tok.size() >= 2 && (tok[0] == 'W' || tok[0] == 'w') && tok[1] == '/') tok.remove_prefix(2);
        if (tok.size() >= 2 && tok.front() == '"' && tok.back() == '"') tok = tok.substr(1, tok.size() - 2);
        if (tok == "*" || (!tok.empty() && tok == etag)) return true;
        pos = comma + 1;
    }
    return false;
}

}  // namespace

// ---------- table buckets ----------

const TableBucketEntry* Catalog::table_bucket(const TableBucketStore::Snapshot& snap, std::string_view bucket) const {
    const TableBucketEntry* e = TableBucketStore::find(snap, std::string(bucket));
    return e && e->enabled ? e : nullptr;
}

Task<TableBucketEntry> Catalog::require_table_bucket(std::string_view bucket) {
    auto snap = buckets_->snapshot();
    const TableBucketEntry* e = table_bucket(snap, bucket);
    if (!e) throw not_found_ns("bucket " + std::string(bucket) + " is not table-enabled");
    co_return *e;
}

Task<TableBucketEntry> Catalog::enable_bucket(std::string_view bucket) {
    storage::validate_bucket_name(bucket);
    auto snap = buckets_->snapshot();
    if (const TableBucketEntry* e = table_bucket(snap, bucket)) co_return *e;
    auto& backend = router_.resolve(bucket);
    if (!co_await backend.bucket_exists(bucket))
        throw not_found_ns("bucket " + std::string(bucket) + " does not exist");
    storage::ListOptions opt;
    opt.prefix = cfg_.reserved_prefix;
    opt.max_keys = 1;
    auto res = co_await backend.list_objects(bucket, opt);
    if (!res.objects.empty() || !res.common_prefixes.empty())
        throw bad_request("bucket " + std::string(bucket) + " already holds objects under the reserved prefix " +
                          cfg_.reserved_prefix);
    TableBucketEntry entry;
    entry.enabled = true;
    entry.reserved_prefix = cfg_.reserved_prefix;
    entry.created_unix = now_unix();
    co_await buckets_->put(std::string(bucket), entry);
    LOG_INFO("tables: bucket {} enabled as a table bucket (reserved prefix {})", bucket, entry.reserved_prefix);
    co_return entry;
}

Task<void> Catalog::disable_bucket(std::string_view bucket) {
    auto snap = buckets_->snapshot();
    if (!table_bucket(snap, bucket)) throw not_found_ns("bucket " + std::string(bucket) + " is not table-enabled");
    if (!co_await catalog_empty(bucket))
        throw ns_not_empty("bucket " + std::string(bucket) + " still holds namespaces or tables");
    co_await buckets_->remove(std::string(bucket));
    LOG_INFO("tables: bucket {} is no longer a table bucket", bucket);
}

Task<bool> Catalog::catalog_empty(std::string_view bucket) { co_return co_await store_->bucket_state_empty(bucket); }

Task<void> Catalog::forget_bucket(std::string_view bucket) {
    co_await store_->delete_bucket_state(bucket);
    auto snap = buckets_->snapshot();
    if (TableBucketStore::find(snap, std::string(bucket))) co_await buckets_->remove(std::string(bucket));
}

// ---------- namespaces ----------

Task<NamespaceEntry> Catalog::create_namespace(std::string_view bucket, Levels levels,
                                               std::map<std::string, std::string> props) {
    co_await require_table_bucket(bucket);
    require_namespace(levels);
    if (co_await store_->get_namespace(bucket, levels))
        throw already_exists("namespace " + ns_display(levels) + " already exists");
    NamespaceEntry e;
    e.levels = std::move(levels);
    e.properties = std::move(props);
    e.created_unix = e.updated_unix = now_unix();
    storage::PutCondition cond;
    cond.if_none_match = true;
    bool clash = false;
    try {
        co_await store_->put_namespace(bucket, e, cond);
    } catch (const S3Error& err) {
        if (!is_precondition(err)) throw;
        clash = true;
    }
    if (clash) throw already_exists("namespace " + ns_display(e.levels) + " already exists");
    co_return e;
}

Task<std::optional<NamespaceEntry>> Catalog::load_namespace(std::string_view bucket, const Levels& levels) {
    co_await require_table_bucket(bucket);
    require_namespace(levels);
    if (auto e = co_await store_->get_namespace(bucket, levels)) co_return std::move(e->value);
    if (co_await store_->namespace_has_children(bucket, levels)) {
        NamespaceEntry e;
        e.levels = levels;
        e.explicit_entry = false;
        co_return e;
    }
    co_return std::nullopt;
}

Task<bool> Catalog::namespace_exists(std::string_view bucket, const Levels& levels) {
    co_return (co_await load_namespace(bucket, levels)).has_value();
}

Task<ListPage<Levels>> Catalog::list_namespaces(std::string_view bucket, const Levels& parent, PageCursor cursor) {
    co_await require_table_bucket(bucket);
    if (!parent.empty() && !co_await namespace_exists(bucket, parent))
        throw not_found_ns("namespace " + ns_display(parent) + " does not exist");
    auto page = co_await store_->list_child_namespaces(bucket, parent, cursor);
    ListPage<Levels> out;
    for (auto& seg : page.items) {
        Levels l = parent;
        l.push_back(seg);
        out.items.push_back(std::move(l));
    }
    out.next_after = page.next_after;
    co_return out;
}

Task<NamespacePropsResult> Catalog::update_namespace_properties(std::string_view bucket, const Levels& levels,
                                                                const std::vector<std::string>& removals,
                                                                const std::map<std::string, std::string>& updates) {
    auto e = co_await load_namespace(bucket, levels);
    if (!e) throw not_found_ns("namespace " + ns_display(levels) + " does not exist");
    NamespacePropsResult res;
    std::set<std::string> seen;
    for (auto& k : removals) {
        if (!seen.insert(k).second) throw bad_request("property '" + k + "' is listed twice in removals");
        if (updates.count(k)) throw unprocessable("property '" + k + "' appears in both removals and updates");
    }
    for (auto& k : removals) {
        if (e->properties.erase(k))
            res.removed.push_back(k);
        else
            res.missing.push_back(k);
    }
    for (auto& [k, v] : updates) {
        e->properties[k] = v;
        res.updated.push_back(k);
    }
    e->updated_unix = now_unix();
    e->explicit_entry = true;
    co_await store_->put_namespace(bucket, *e, {});
    co_return res;
}

Task<void> Catalog::drop_namespace(std::string_view bucket, const Levels& levels) {
    co_await require_table_bucket(bucket);
    require_namespace(levels);
    if (co_await store_->namespace_has_children(bucket, levels))
        throw ns_not_empty("namespace " + ns_display(levels) + " is not empty");
    if (!co_await store_->get_namespace(bucket, levels))
        throw not_found_ns("namespace " + ns_display(levels) + " does not exist");
    co_await store_->delete_namespace(bucket, levels);
    // the tombstones of dropped tables are meaningless without their namespace
    PageCursor c;
    c.limit = 1000;
    for (;;) {
        auto page = co_await store_->list_tables(bucket, levels, c);
        for (auto& n : page.items) {
            auto e = co_await store_->get_table(bucket, levels, n);
            if (e && e->value.state == TableState::Deleted) co_await store_->delete_table(bucket, levels, n);
        }
        if (page.next_after.empty()) break;
        c.after = page.next_after;
    }
}

// ---------- tables ----------

Task<std::optional<Versioned<TableEntry>>> Catalog::table_pointer(std::string_view bucket, const Levels& levels,
                                                                  std::string_view name) {
    co_await require_table_bucket(bucket);
    auto cur = co_await store_->get_table(bucket, levels, name);
    if (!cur || cur->value.state == TableState::Deleted) co_return std::nullopt;
    co_return cur;
}

Task<bool> Catalog::table_exists(std::string_view bucket, const Levels& levels, std::string_view name) {
    auto p = co_await table_pointer(bucket, levels, name);
    co_return p.has_value();
}

Task<ListPage<std::string>> Catalog::list_tables(std::string_view bucket, const Levels& levels, PageCursor cursor) {
    if (!co_await namespace_exists(bucket, levels))
        throw not_found_ns("namespace " + ns_display(levels) + " does not exist");
    // tombstones are listed by the store; filter them out (one read per name)
    ListPage<std::string> out;
    std::string after = cursor.after;
    while (static_cast<int>(out.items.size()) < cursor.limit) {
        PageCursor c;
        c.after = after;
        c.limit = cursor.limit;
        auto page = co_await store_->list_tables(bucket, levels, c);
        for (auto& n : page.items) {
            auto e = co_await store_->get_table(bucket, levels, n);
            if (e && e->value.state != TableState::Deleted) {
                if (static_cast<int>(out.items.size()) >= cursor.limit) {
                    out.next_after = out.items.back();
                    co_return out;
                }
                out.items.push_back(n);
            }
        }
        if (page.next_after.empty()) break;
        after = page.next_after;
    }
    co_return out;
}

Task<Catalog::LoadedTable> Catalog::load_table(std::string_view bucket, const Levels& levels, std::string_view name,
                                               std::string_view if_none_match) {
    co_await require_table_bucket(bucket);
    auto cur = co_await require_active(bucket, levels, name);
    if (!if_none_match.empty() && etag_matches(if_none_match, cur.etag)) {
        LoadedTable t;
        t.entry = std::move(cur.value);
        t.etag = std::move(cur.etag);
        t.not_modified = true;
        co_return t;
    }
    auto& backend = router_.resolve(bucket);
    json md = co_await read_metadata(backend, bucket, cur.value.metadata_location);
    if (md.value("table-uuid", "") != cur.value.table_uuid)
        throw internal("persisted table metadata does not match the catalog entry");
    co_return LoadedTable{std::move(cur.value), std::move(cur.etag), std::move(md)};
}

Task<Catalog::LoadedTable> Catalog::finish_create(std::string_view bucket, const TableBucketEntry&,
                                                  const Levels& levels, std::string_view name, TableEntry entry,
                                                  json md, std::string body, storage::IStorageBackend& backend,
                                                  const CommitHooks& hooks) {
    // tombstone → conditional replacement; otherwise the name must be free (of tables and views)
    co_await require_name_free(bucket, levels, name, /*for_view=*/false);
    storage::PutCondition cond;
    auto existing = co_await store_->get_table(bucket, levels, name);
    if (existing && existing->value.state == TableState::Renaming) {
        co_await maybe_recover_renames(bucket, true);
        existing = co_await store_->get_table(bucket, levels, name);
    }
    if (existing) {
        if (existing->value.state == TableState::Renaming)
            throw unavailable("table " + std::string(name) + " is being renamed; retry");
        if (existing->value.state != TableState::Deleted)
            throw already_exists("table " + ns_display(levels) + "." + std::string(name) + " already exists");
        cond.if_match_etag = existing->etag;
    } else {
        cond.if_none_match = true;
    }
    if (hooks.quota_check) hooks.quota_check(bucket, static_cast<int64_t>(body.size()), 1);
    size_t size = body.size();
    bool clash = false;
    try {
        co_await write_metadata(backend, bucket, entry.metadata_location, std::move(body), /*if_none_match=*/true);
    } catch (const S3Error& e) {
        if (e.code != S3ErrorCode::PreconditionFailed) throw;
        clash = true;
    }
    if (clash) throw already_exists("table " + ns_display(levels) + "." + std::string(name) + " is being created");
    std::string etag;
    try {
        etag = co_await store_->put_table(bucket, levels, name, entry, cond);
    } catch (const S3Error& e) {
        if (!is_precondition(e)) throw;
        clash = true;
    }
    if (clash) {
        co_await best_effort_delete(backend, bucket, entry.metadata_location);
        throw already_exists("table " + ns_display(levels) + "." + std::string(name) + " already exists");
    }
    if (hooks.note_usage) hooks.note_usage(bucket, 1, static_cast<int64_t>(size));
    co_return LoadedTable{std::move(entry), std::move(etag), std::move(md)};
}

Task<Catalog::LoadedTable> Catalog::create_table(std::string_view bucket, const Levels& levels,
                                                 const CreateTableRequest& req, const CommitHooks& hooks) {
    TableBucketEntry tb = co_await require_table_bucket(bucket);
    require_segment("table name", req.name);
    co_await maybe_recover_renames(bucket, false);
    if (!co_await namespace_exists(bucket, levels))
        throw not_found_ns("namespace " + ns_display(levels) + " does not exist");
    auto& backend = router_.resolve(bucket);
    std::string loc_key = req.location ? location_to_key(bucket, tb.reserved_prefix, *req.location)
                                       : ns_path(levels) + "/" + req.name;
    if (req.location) {
        // an explicit location must not nest with another table of the namespace
        PageCursor c;
        c.limit = 1000;
        for (;;) {
            auto page = co_await store_->list_tables(bucket, levels, c);
            for (auto& n : page.items) {
                auto e = co_await store_->get_table(bucket, levels, n);
                if (!e || e->value.state == TableState::Deleted) continue;
                std::string other = location_to_key(bucket, tb.reserved_prefix, e->value.location) + "/";
                std::string mine = loc_key + "/";
                if (other.rfind(mine, 0) == 0 || mine.rfind(other, 0) == 0)
                    throw already_exists("location overlaps table " + ns_display(levels) + "." + n);
            }
            if (page.next_after.empty()) break;
            c.after = page.next_after;
        }
    }
    int fv = 2;
    std::map<std::string, std::string> props = req.properties;
    if (auto it = props.find("format-version"); it != props.end()) {
        try {
            fv = std::stoi(it->second);
        } catch (const std::exception&) {
            throw bad_request("format-version must be 1 or 2");
        }
        props.erase(it);
    }
    if (fv != 1 && fv != 2) throw unsupported("Iceberg format-version " + std::to_string(fv) + " is not supported");
    TableEntry entry;
    entry.levels = levels;
    entry.name = req.name;
    entry.table_id = new_uuid();
    entry.table_uuid = new_uuid();
    entry.location = key_to_location(bucket, loc_key);
    entry.version_token = new_token();
    entry.generation = 1;
    entry.format_version = fv;
    entry.created_unix = entry.updated_unix = now_unix();
    entry.metadata_location = metadata_key(tb, levels, req.name, 1, entry.table_id);
    co_await schedule();
    iceberg::CreateTableInput in;
    in.name = req.name;
    in.schema = req.schema;
    in.partition_spec = req.partition_spec;
    in.write_order = req.write_order;
    in.properties = std::move(props);
    in.format_version = fv;
    in.location = entry.location;
    in.table_uuid = entry.table_uuid;
    in.now_ms = now_ms();
    json md = iceberg::initial_metadata(in);
    std::string body = iceberg::canonical(md);
    co_return co_await finish_create(bucket, tb, levels, req.name, std::move(entry), std::move(md), std::move(body),
                                     backend, hooks);
}

Task<Catalog::LoadedTable> Catalog::register_table(std::string_view bucket, const Levels& levels, std::string_view name,
                                                   std::string_view metadata_location, const CommitHooks& hooks) {
    TableBucketEntry tb = co_await require_table_bucket(bucket);
    require_segment("table name", name);
    co_await maybe_recover_renames(bucket, false);
    if (!co_await namespace_exists(bucket, levels))
        throw not_found_ns("namespace " + ns_display(levels) + " does not exist");
    std::string src_key;
    try {
        src_key = path_to_key(bucket, metadata_location);
    } catch (const RestError&) {
        throw bad_request("metadata-location must be s3://" + std::string(bucket) + "/<key>");
    }
    if (src_key.size() > 3 && src_key.substr(src_key.size() - 3) == ".gz")
        throw unsupported("compressed metadata files are not supported");
    auto& backend = router_.resolve(bucket);
    json md;
    try {
        md = co_await read_metadata(backend, bucket, src_key);
    } catch (const RestError& e) {
        if (e.status == 500)
            throw not_found_table("metadata file " + std::string(metadata_location) + " does not exist");
        throw;
    }
    // the table location must sit in this bucket, outside the reserved prefix
    location_to_key(bucket, tb.reserved_prefix, md.value("location", ""));
    iceberg::SnapshotCheckContext ctx{backend, std::string(bucket), tb.reserved_prefix};
    json empty;
    empty["snapshots"] = json::array();
    auto report = co_await iceberg::check_new_snapshots_deep(ctx, empty, md, deep_options());
    validation_files_->inc(report.files);
    if (report.skipped_codec) validation_skipped_->inc();
    TableEntry entry;
    entry.levels = levels;
    entry.name = std::string(name);
    entry.table_id = new_uuid();
    entry.table_uuid = md["table-uuid"].get<std::string>();
    entry.location = md["location"].get<std::string>();
    entry.version_token = new_token();
    entry.generation = 1;
    entry.format_version = iceberg::format_version(md);
    entry.created_unix = entry.updated_unix = now_unix();
    entry.metadata_location = metadata_key(tb, levels, name, 1, entry.table_id);
    std::string body = iceberg::canonical(md);
    LoadedTable t = co_await finish_create(bucket, tb, levels, name, std::move(entry), std::move(md), std::move(body),
                                           backend, hooks);
    t.validation = report.skipped_codec ? "skipped-codec" : "deep";
    co_return t;
}

Task<Catalog::LoadedTable> Catalog::commit_table(std::string_view bucket, const Levels& levels, std::string_view name,
                                                 const CommitRequest& req, const CommitHooks& hooks) {
    auto started = std::chrono::steady_clock::now();
    auto observe = [&] {
        commit_seconds_->observe(std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count());
    };
    TableBucketEntry tb = co_await require_table_bucket(bucket);
    auto& backend = router_.resolve(bucket);
    co_await maybe_recover_renames(bucket, false);
    auto cur = co_await require_active(bucket, levels, name, /*writer=*/true);
    // in-process fast path (design §5.1): same-gateway commits on one table serialize here
    auto lock = table_lock(bucket, cur.value.table_id);
    auto permit = co_await lock->sem.acquire();
    cur = co_await require_active(bucket, levels, name, /*writer=*/true);
    const std::string table_id = cur.value.table_id;
    const bool replayable = !req.commit_id.empty();
    const std::string commit_id = replayable ? req.commit_id : new_uuid();
    const std::string digest = request_digest(req);

    // ---- idempotent replay (design §5.3) ----
    std::optional<CommitRecord> staged;
    if (replayable) {
        auto rec = co_await store_->get_commit(bucket, table_id, commit_id);
        if (rec) {
            if (rec->value.request_digest != digest)
                throw commit_failed("commit-id " + commit_id + " was already used with a different payload");
            bool done = rec->value.status == "COMMITTED";
            if (!done && cur.value.version_token == rec->value.new_token) {
                CommitRecord fin = rec->value;
                fin.status = "COMMITTED";
                try {
                    co_await store_->put_commit(bucket, table_id, fin, {});
                } catch (const std::exception& e) {
                    LOG_WARN("tables: finalize of commit {} failed again: {}", commit_id, e.what());
                }
                done = true;
            }
            if (done) {
                json md = co_await read_metadata(backend, bucket, rec->value.new_metadata_location);
                TableEntry e = cur.value;
                if (e.metadata_location != rec->value.new_metadata_location) {
                    e.metadata_location = rec->value.new_metadata_location;
                    e.version_token = rec->value.new_token;
                }
                observe();
                co_return LoadedTable{std::move(e), cur.etag, std::move(md)};
            }
            if (cur.value.version_token == rec->value.expected_token)
                staged = rec->value;
            else
                throw commit_failed("commit-id " + commit_id + " was staged against an older table version");
        }
    }

    // ---- validate and build the next metadata ----
    json current = co_await read_metadata(backend, bucket, cur.value.metadata_location);
    if (current.value("table-uuid", "") != cur.value.table_uuid)
        throw internal("persisted table metadata does not match the catalog entry");
    co_await schedule();
    iceberg::check_requirements(current, req.requirements, true);
    json next = iceberg::apply_updates(current, req.updates, {std::string(bucket), tb.reserved_prefix});
    iceberg::check_transition(current, next);
    iceberg::finish_transition(next, to_client_location(bucket, cur.value.metadata_location), now_ms(),
                               {cfg_.metadata_log_keep});
    iceberg::SnapshotCheckContext ctx{backend, std::string(bucket), tb.reserved_prefix};
    auto report = co_await iceberg::check_new_snapshots_deep(ctx, current, next, deep_options());
    validation_files_->inc(report.files);
    if (report.skipped_codec) validation_skipped_->inc();
    const std::string validation = report.skipped_codec ? "skipped-codec" : "deep";
    std::string body = iceberg::canonical(next);
    if (hooks.quota_check) hooks.quota_check(bucket, static_cast<int64_t>(body.size()), 1);

    // ---- write the new metadata file (idempotent on replay) ----
    std::string new_key = staged ? staged->new_metadata_location
                                 : metadata_key(tb, levels, name, cur.value.generation + 1,
                                                looks_like_uuid(commit_id) ? commit_id : new_uuid());
    bool wrote = true;
    bool exists = false;
    try {
        co_await write_metadata(backend, bucket, new_key, body, /*if_none_match=*/true);
    } catch (const S3Error& e) {
        if (e.code != S3ErrorCode::PreconditionFailed) throw;
        exists = true;
    }
    if (exists) {
        json prev = co_await read_metadata(backend, bucket, new_key);
        if (staged) {
            // replay of a staged commit: the file written by the first attempt is the
            // one the record points at (timestamps make a re-derivation differ)
            next = std::move(prev);
            body = iceberg::canonical(next);
        } else if (iceberg::canonical(prev) != body) {
            throw commit_failed("generated metadata location " + new_key + " already holds a different commit");
        }
        wrote = false;
    }

    // ---- stage the commit record (a transactional backing skips the stage: the record
    // and the pointer land together below, step ⑥ §5) ----
    const bool atomic = store_->supports_atomic_commit();
    CommitRecord rec;
    if (staged) {
        rec = *staged;
    } else {
        rec.commit_id = commit_id;
        rec.table_id = table_id;
        rec.expected_token = cur.value.version_token;
        rec.new_token = new_token();
        rec.prev_metadata_location = cur.value.metadata_location;
        rec.new_metadata_location = new_key;
        rec.status = "STAGED";
        rec.request_digest = digest;
        rec.created_unix = now_unix();
        if (!atomic) {
            storage::PutCondition cond;
            cond.if_none_match = true;
            bool raced = false;
            try {
                co_await store_->put_commit(bucket, table_id, rec, cond);
            } catch (const S3Error& e) {
                if (!is_precondition(e)) throw;
                raced = true;
            }
            if (raced) throw unavailable("commit " + commit_id + " is being processed by another gateway; retry");
        }
    }

    if (!atomic && fault::check("tables.commit.after_stage"))
        throw S3Error(S3ErrorCode::InternalError, "injected failure after staging the commit");

    // ---- CAS the pointer (the only atomic point, design §5.2 step 8) ----
    TableEntry next_entry = cur.value;
    next_entry.metadata_location = new_key;
    next_entry.version_token = rec.new_token;
    next_entry.generation = cur.value.generation + 1;
    next_entry.format_version = iceberg::format_version(next);
    next_entry.location = next.value("location", next_entry.location);
    next_entry.updated_unix = now_unix();
    storage::PutCondition cas;
    cas.if_match_etag = cur.etag;
    std::string etag1;
    bool lost = false;
    std::exception_ptr err;
    try {
        if (atomic) {
            CommitRecord fin = rec;
            fin.status = "COMMITTED";
            etag1 = co_await store_->commit_atomic(bucket, levels, name, next_entry, cas, fin);
        } else {
            etag1 = co_await store_->put_table(bucket, levels, name, next_entry, cas);
        }
    } catch (const S3Error& e) {
        if (is_precondition(e))
            lost = true;
        else
            err = std::current_exception();
    } catch (...) {
        err = std::current_exception();
    }
    if (lost) {
        commits_conflict_->inc();
        if (wrote) co_await best_effort_delete(backend, bucket, new_key);
        observe();
        throw commit_failed("table " + ns_display(levels) + "." + std::string(name) +
                            " was updated concurrently; refresh and retry the commit");
    }
    if (err) {
        commits_error_->inc();
        observe();
        throw commit_state_unknown("the commit may or may not have been applied; reload the table before retrying");
    }

    if (!atomic && fault::check("tables.commit.after_cas"))
        throw S3Error(S3ErrorCode::InternalError, "injected failure after the pointer CAS");

    // ---- finalize (best effort; the CAS already made the commit durable, design §5.4) ----
    rec.status = "COMMITTED";
    if (!atomic) {
        try {
            co_await store_->put_commit(bucket, table_id, rec, {});
        } catch (const std::exception& e) {
            LOG_WARN("tables: commit {} applied but its record could not be finalized: {}", commit_id, e.what());
        }
    }
    if (hooks.note_usage && wrote) hooks.note_usage(bucket, 1, static_cast<int64_t>(body.size()));
    commits_ok_->inc();
    observe();
    co_return LoadedTable{std::move(next_entry), std::move(etag1), std::move(next), validation};
}

Task<TableEntry> Catalog::update_metadata_location(std::string_view bucket, const Levels& levels, std::string_view name,
                                                   std::string_view new_location, std::string_view expected_token) {
    TableBucketEntry tb = co_await require_table_bucket(bucket);
    auto& backend = router_.resolve(bucket);
    co_await maybe_recover_renames(bucket, false);
    auto cur = co_await require_active(bucket, levels, name, /*writer=*/true);
    auto lock = table_lock(bucket, cur.value.table_id);
    auto permit = co_await lock->sem.acquire();
    cur = co_await require_active(bucket, levels, name, /*writer=*/true);
    if (cur.value.version_token != expected_token)
        throw commit_failed("versionToken does not match the current table version");
    std::string new_key;
    try {
        new_key = path_to_key(bucket, new_location);
    } catch (const RestError&) {
        throw bad_request("metadataLocation must be s3://" + std::string(bucket) + "/<key>");
    }
    std::string dir = tb.reserved_prefix + ns_path(levels) + "/" + std::string(name) + "/metadata/";
    if (new_key.rfind(dir, 0) != 0)
        throw bad_request("metadataLocation must lie under " + to_client_location(bucket, dir));
    if (new_key == cur.value.metadata_location) co_return cur.value;
    json current = co_await read_metadata(backend, bucket, cur.value.metadata_location);
    json next;
    try {
        next = co_await read_metadata(backend, bucket, new_key);
    } catch (const RestError& e) {
        if (e.status == 500) throw bad_request("metadataLocation " + std::string(new_location) + " does not exist");
        throw;
    }
    co_await schedule();
    iceberg::check_transition(current, next);
    iceberg::SnapshotCheckContext ctx{backend, std::string(bucket), tb.reserved_prefix};
    auto report = co_await iceberg::check_new_snapshots_deep(ctx, current, next, deep_options());
    validation_files_->inc(report.files);
    if (report.skipped_codec) validation_skipped_->inc();
    CommitRecord rec;
    rec.commit_id = "ptr-" + new_uuid();
    rec.table_id = cur.value.table_id;
    rec.expected_token = cur.value.version_token;
    rec.new_token = new_token();
    rec.prev_metadata_location = cur.value.metadata_location;
    rec.new_metadata_location = new_key;
    rec.status = "STAGED";
    rec.created_unix = now_unix();
    storage::PutCondition cond;
    cond.if_none_match = true;
    co_await store_->put_commit(bucket, rec.table_id, rec, cond);
    TableEntry next_entry = cur.value;
    next_entry.metadata_location = new_key;
    next_entry.version_token = rec.new_token;
    next_entry.generation = cur.value.generation + 1;
    next_entry.format_version = iceberg::format_version(next);
    next_entry.updated_unix = now_unix();
    storage::PutCondition cas;
    cas.if_match_etag = cur.etag;
    bool lost = false;
    try {
        co_await store_->put_table(bucket, levels, name, next_entry, cas);
    } catch (const S3Error& e) {
        if (!is_precondition(e)) throw;
        lost = true;
    }
    if (lost) {
        commits_conflict_->inc();
        throw commit_failed("table was updated concurrently; refresh and retry");
    }
    rec.status = "COMMITTED";
    try {
        co_await store_->put_commit(bucket, rec.table_id, rec, {});
    } catch (const std::exception& e) {
        LOG_WARN("tables: pointer update applied but its record could not be finalized: {}", e.what());
    }
    commits_ok_->inc();
    co_return next_entry;
}

Task<s3::CredentialPolicy> Catalog::vending_policy(std::string_view bucket, const TableEntry& entry, bool readonly) {
    TableBucketEntry tb = co_await require_table_bucket(bucket);
    s3::CredentialPolicy p;
    p.buckets = {std::string(bucket)};
    p.prefixes = {location_to_key(bucket, tb.reserved_prefix, entry.location) + "/",
                  tb.reserved_prefix + ns_path(entry.levels) + "/" + entry.name + "/metadata/"};
    p.readonly = readonly;
    co_return p;
}

Task<void> Catalog::rename_table(std::string_view bucket, const Levels& src_levels, std::string_view src_name,
                                 const Levels& dst_levels, std::string_view dst_name) {
    co_await require_table_bucket(bucket);
    require_namespace(dst_levels);
    require_segment("table name", dst_name);
    if (src_levels == dst_levels && src_name == dst_name) co_return;
    co_await maybe_recover_renames(bucket, false);
    auto src = co_await require_active(bucket, src_levels, src_name, /*writer=*/true);
    if (!co_await namespace_exists(bucket, dst_levels))
        throw not_found_ns("namespace " + ns_display(dst_levels) + " does not exist");
    co_await require_name_free(bucket, dst_levels, dst_name, /*for_view=*/false);
    auto dst_existing = co_await store_->get_table(bucket, dst_levels, dst_name);
    if (dst_existing && dst_existing->value.state == TableState::Renaming) {
        co_await maybe_recover_renames(bucket, true);
        dst_existing = co_await store_->get_table(bucket, dst_levels, dst_name);
    }
    if (dst_existing) {
        if (dst_existing->value.state == TableState::Renaming)
            throw unavailable("table " + std::string(dst_name) + " is being renamed; retry");
        if (dst_existing->value.state != TableState::Deleted)
            throw already_exists("table " + ns_display(dst_levels) + "." + std::string(dst_name) + " already exists");
    }
    auto lock = table_lock(bucket, src.value.table_id);
    auto permit = co_await lock->sem.acquire();
    src = co_await require_active(bucket, src_levels, src_name, /*writer=*/true);

    // ① the intent (design §5.6); steps ②–⑤ are the same driver any writer runs for a
    // stranded intent, so a crash anywhere leaves a state some other gateway completes
    RenameIntent intent;
    intent.rename_id = new_uuid();
    intent.src_levels = src_levels;
    intent.dst_levels = dst_levels;
    intent.src_name = std::string(src_name);
    intent.dst_name = std::string(dst_name);
    intent.src_etag = src.etag;
    intent.stage = RenameIntent::Stage::Prepared;
    intent.created_unix = now_unix();
    storage::PutCondition fresh;
    fresh.if_none_match = true;
    std::string etag = co_await store_->put_rename(bucket, intent, fresh);
    if (fault::check("tables.rename.after_prepare"))
        throw S3Error(S3ErrorCode::InternalError, "injected failure after writing the rename intent");
    RenameOutcome outcome = co_await drive_rename(*store_, bucket, Versioned<RenameIntent>{intent, etag},
                                                  cfg_.maintenance.tombstone_ttl_sec);
    switch (outcome) {
        case RenameOutcome::Completed:
            co_return;
        case RenameOutcome::DestinationTaken:
            throw already_exists("table " + ns_display(dst_levels) + "." + std::string(dst_name) + " already exists");
        case RenameOutcome::Abandoned:
            throw commit_failed("table " + std::string(src_name) + " was updated concurrently; retry the rename");
        case RenameOutcome::Contended:
            throw unavailable("rename of " + std::string(src_name) + " is being completed by another gateway; retry");
    }
}

// ---------- views (step ⑥ §1) ----------

Task<void> Catalog::require_name_free(std::string_view bucket, const Levels& levels, std::string_view name,
                                      bool for_view) {
    if (for_view) {
        auto t = co_await store_->get_table(bucket, levels, name);
        if (t && t->value.state != TableState::Deleted)
            throw already_exists("table " + ns_display(levels) + "." + std::string(name) + " already exists");
    } else {
        auto v = co_await store_->get_view(bucket, levels, name);
        if (v && v->value.state != TableState::Deleted)
            throw already_exists("view " + ns_display(levels) + "." + std::string(name) + " already exists");
    }
}

Task<Versioned<ViewEntry>> Catalog::require_view(std::string_view bucket, const Levels& levels, std::string_view name) {
    auto cur = co_await store_->get_view(bucket, levels, name);
    if (!cur || cur->value.state == TableState::Deleted)
        throw not_found_view("view " + ns_display(levels) + "." + std::string(name) + " does not exist");
    co_return std::move(*cur);
}

Task<json> Catalog::read_view_metadata(storage::IStorageBackend& backend, std::string_view bucket,
                                       std::string_view key) {
    storage::ObjectStream stream;
    try {
        stream = co_await backend.get_object(bucket, key, std::nullopt);
    } catch (const S3Error& e) {
        if (e.code == S3ErrorCode::NoSuchKey || e.code == S3ErrorCode::NoSuchBucket)
            throw internal("view metadata file " + std::string(key) + " is missing");
        throw;
    }
    std::string text;
    std::byte buf[64 * 1024];
    for (;;) {
        size_t n = co_await stream.body->read(std::span(buf));
        if (n == 0) break;
        if (text.size() + n > cfg_.metadata_max_size) throw bad_request("view metadata exceeds the size limit");
        text.append(reinterpret_cast<const char*>(buf), n);
    }
    co_await schedule();
    co_return iceberg::parse_and_validate_view(text, cfg_.metadata_max_size);
}

Task<Catalog::LoadedView> Catalog::create_view(std::string_view bucket, const Levels& levels,
                                               const CreateViewRequest& req) {
    TableBucketEntry tb = co_await require_table_bucket(bucket);
    require_segment("view name", req.name);
    if (!co_await namespace_exists(bucket, levels))
        throw not_found_ns("namespace " + ns_display(levels) + " does not exist");
    co_await require_name_free(bucket, levels, req.name, /*for_view=*/true);
    auto& backend = router_.resolve(bucket);
    std::string loc_key = req.location ? location_to_key(bucket, tb.reserved_prefix, *req.location)
                                       : ns_path(levels) + "/" + req.name;
    ViewEntry entry;
    entry.levels = levels;
    entry.name = req.name;
    entry.view_id = new_uuid();
    entry.view_uuid = new_uuid();
    entry.location = key_to_location(bucket, loc_key);
    entry.version_token = new_token();
    entry.generation = 1;
    entry.created_unix = entry.updated_unix = now_unix();
    entry.metadata_location = tb.reserved_prefix + ns_path(levels) + "/" + req.name + "/view-metadata/00001-" +
                              entry.view_id + ".metadata.json";
    co_await schedule();
    iceberg::CreateViewInput in;
    in.name = req.name;
    in.schema = req.schema;
    in.view_version = req.view_version;
    in.properties = req.properties;
    in.location = entry.location;
    in.view_uuid = entry.view_uuid;
    in.now_ms = now_ms();
    json md = iceberg::initial_view_metadata(in);
    // tombstone → conditional replacement; otherwise the name must be free
    storage::PutCondition cond;
    auto existing = co_await store_->get_view(bucket, levels, req.name);
    if (existing) {
        if (existing->value.state != TableState::Deleted)
            throw already_exists("view " + ns_display(levels) + "." + req.name + " already exists");
        cond.if_match_etag = existing->etag;
    } else {
        cond.if_none_match = true;
    }
    bool clash = false;
    try {
        co_await write_metadata(backend, bucket, entry.metadata_location, iceberg::canonical(md),
                                /*if_none_match=*/true);
    } catch (const S3Error& e) {
        if (e.code != S3ErrorCode::PreconditionFailed) throw;
        clash = true;
    }
    if (clash) throw already_exists("view " + ns_display(levels) + "." + req.name + " is being created");
    std::string etag;
    try {
        etag = co_await store_->put_view(bucket, levels, req.name, entry, cond);
    } catch (const S3Error& e) {
        if (!is_precondition(e)) throw;
        clash = true;
    }
    if (clash) {
        co_await best_effort_delete(backend, bucket, entry.metadata_location);
        throw already_exists("view " + ns_display(levels) + "." + req.name + " already exists");
    }
    co_return LoadedView{std::move(entry), std::move(etag), std::move(md)};
}

Task<Catalog::LoadedView> Catalog::load_view(std::string_view bucket, const Levels& levels, std::string_view name) {
    co_await require_table_bucket(bucket);
    auto cur = co_await require_view(bucket, levels, name);
    auto& backend = router_.resolve(bucket);
    json md = co_await read_view_metadata(backend, bucket, cur.value.metadata_location);
    if (md.value("view-uuid", "") != cur.value.view_uuid)
        throw internal("persisted view metadata does not match the catalog entry");
    co_return LoadedView{std::move(cur.value), std::move(cur.etag), std::move(md)};
}

Task<bool> Catalog::view_exists(std::string_view bucket, const Levels& levels, std::string_view name) {
    co_await require_table_bucket(bucket);
    auto cur = co_await store_->get_view(bucket, levels, name);
    co_return cur && cur->value.state != TableState::Deleted;
}

Task<ListPage<std::string>> Catalog::list_views(std::string_view bucket, const Levels& levels, PageCursor cursor) {
    if (!co_await namespace_exists(bucket, levels))
        throw not_found_ns("namespace " + ns_display(levels) + " does not exist");
    ListPage<std::string> out;
    std::string after = cursor.after;
    while (static_cast<int>(out.items.size()) < cursor.limit) {
        PageCursor c;
        c.after = after;
        c.limit = cursor.limit;
        auto page = co_await store_->list_views(bucket, levels, c);
        for (auto& n : page.items) {
            auto e = co_await store_->get_view(bucket, levels, n);
            if (e && e->value.state != TableState::Deleted) {
                if (static_cast<int>(out.items.size()) >= cursor.limit) {
                    out.next_after = out.items.back();
                    co_return out;
                }
                out.items.push_back(n);
            }
        }
        if (page.next_after.empty()) break;
        after = page.next_after;
    }
    co_return out;
}

Task<Catalog::LoadedView> Catalog::replace_view(std::string_view bucket, const Levels& levels, std::string_view name,
                                                const json& requirements, const json& updates) {
    TableBucketEntry tb = co_await require_table_bucket(bucket);
    auto& backend = router_.resolve(bucket);
    auto cur = co_await require_view(bucket, levels, name);
    auto lock = table_lock(bucket, cur.value.view_id);
    auto permit = co_await lock->sem.acquire();
    cur = co_await require_view(bucket, levels, name);
    json current = co_await read_view_metadata(backend, bucket, cur.value.metadata_location);
    if (current.value("view-uuid", "") != cur.value.view_uuid)
        throw internal("persisted view metadata does not match the catalog entry");
    co_await schedule();
    iceberg::check_view_requirements(current, requirements);
    json next = iceberg::apply_view_updates(current, updates, now_ms());
    if (next.contains("location")) location_to_key(bucket, tb.reserved_prefix, next["location"].get<std::string>());
    char num[16];
    std::snprintf(num, sizeof(num), "%05llu", static_cast<unsigned long long>(cur.value.generation + 1));
    std::string new_key = tb.reserved_prefix + ns_path(levels) + "/" + std::string(name) + "/view-metadata/" + num +
                          "-" + new_uuid() + ".metadata.json";
    co_await write_metadata(backend, bucket, new_key, iceberg::canonical(next), /*if_none_match=*/true);
    ViewEntry next_entry = cur.value;
    next_entry.metadata_location = new_key;
    next_entry.version_token = new_token();
    next_entry.generation = cur.value.generation + 1;
    next_entry.location = next.value("location", next_entry.location);
    next_entry.updated_unix = now_unix();
    storage::PutCondition cas;
    cas.if_match_etag = cur.etag;
    std::string etag;
    bool lost = false;
    try {
        etag = co_await store_->put_view(bucket, levels, name, next_entry, cas);
    } catch (const S3Error& e) {
        if (!is_precondition(e)) throw;
        lost = true;
    }
    if (lost) {
        co_await best_effort_delete(backend, bucket, new_key);
        throw commit_failed("view " + ns_display(levels) + "." + std::string(name) +
                            " was updated concurrently; refresh and retry");
    }
    co_return LoadedView{std::move(next_entry), std::move(etag), std::move(next)};
}

Task<void> Catalog::rename_view(std::string_view bucket, const Levels& src_levels, std::string_view src_name,
                                const Levels& dst_levels, std::string_view dst_name) {
    co_await require_table_bucket(bucket);
    require_namespace(dst_levels);
    require_segment("view name", dst_name);
    if (src_levels == dst_levels && src_name == dst_name) co_return;
    auto src = co_await require_view(bucket, src_levels, src_name);
    if (!co_await namespace_exists(bucket, dst_levels))
        throw not_found_ns("namespace " + ns_display(dst_levels) + " does not exist");
    co_await require_name_free(bucket, dst_levels, dst_name, /*for_view=*/true);
    storage::PutCondition dst_cond;
    auto dst_existing = co_await store_->get_view(bucket, dst_levels, dst_name);
    if (dst_existing) {
        if (dst_existing->value.state != TableState::Deleted)
            throw already_exists("view " + ns_display(dst_levels) + "." + std::string(dst_name) + " already exists");
        dst_cond.if_match_etag = dst_existing->etag;
    } else {
        dst_cond.if_none_match = true;
    }
    // views have no in-flight readers to fence: destination first (the pointer only
    // moves, the metadata stays where it is), then the source becomes a tombstone
    ViewEntry d = src.value;
    d.levels = dst_levels;
    d.name = std::string(dst_name);
    d.updated_unix = now_unix();
    bool clash = false;
    try {
        co_await store_->put_view(bucket, dst_levels, dst_name, d, dst_cond);
    } catch (const S3Error& e) {
        if (!is_precondition(e)) throw;
        clash = true;
    }
    if (clash) throw already_exists("view " + ns_display(dst_levels) + "." + std::string(dst_name) + " already exists");
    ViewEntry s = src.value;
    s.state = TableState::Deleted;
    s.updated_unix = now_unix();
    storage::PutCondition cas;
    cas.if_match_etag = src.etag;
    try {
        co_await store_->put_view(bucket, src_levels, src_name, s, cas);
    } catch (const S3Error& e) {
        if (!is_precondition(e)) throw;
        LOG_WARN("tables: view {} changed while being renamed; both names may exist until the next drop", src_name);
    }
}

Task<void> Catalog::drop_view(std::string_view bucket, const Levels& levels, std::string_view name) {
    co_await require_table_bucket(bucket);
    auto cur = co_await require_view(bucket, levels, name);
    ViewEntry e = cur.value;
    e.state = TableState::Deleted;
    e.updated_unix = now_unix();
    storage::PutCondition cas;
    cas.if_match_etag = cur.etag;
    bool lost = false;
    try {
        co_await store_->put_view(bucket, levels, name, e, cas);
    } catch (const S3Error& err) {
        if (!is_precondition(err)) throw;
        lost = true;
    }
    if (lost) throw unavailable("view " + std::string(name) + " is being updated concurrently; retry the drop");
}

Task<TableDiagnostics> Catalog::diagnose(std::string_view bucket, const Levels& levels, std::string_view name) {
    TableBucketEntry tb = co_await require_table_bucket(bucket);
    auto& backend = router_.resolve(bucket);
    std::string dir = metadata_dir(tb, levels, name);
    co_return co_await diagnose_table(*store_, backend, bucket, levels, name, dir);
}

Task<RecoveryReport> Catalog::recover(std::string_view bucket, const Levels& levels, std::string_view name,
                                      bool prune) {
    co_await require_table_bucket(bucket);
    co_return co_await recover_table(*store_, bucket, levels, name, prune);
}

Task<void> Catalog::drop_table(std::string_view bucket, const Levels& levels, std::string_view name) {
    co_await require_table_bucket(bucket);
    co_await maybe_recover_renames(bucket, false);
    for (int attempt = 0; attempt < 2; ++attempt) {
        auto cur = co_await require_active(bucket, levels, name, /*writer=*/true);
        TableEntry e = cur.value;
        e.state = TableState::Deleted;
        e.updated_unix = now_unix();
        storage::PutCondition cas;
        cas.if_match_etag = cur.etag;
        bool lost = false;
        try {
            co_await store_->put_table(bucket, levels, name, e, cas);
        } catch (const S3Error& err) {
            if (!is_precondition(err)) throw;
            lost = true;
        }
        if (!lost) co_return;
    }
    throw unavailable("table " + std::string(name) + " is being updated concurrently; retry the drop");
}

}  // namespace lights3::tables
