#include "tables/maintenance.h"

#include <algorithm>
#include <chrono>
#include <map>
#include <set>

#include "core/log.h"
#include "s3/errors.h"
#include "tables/rest_error.h"

namespace lights3::tables {

using nlohmann::json;
using s3::S3Error;
using s3::S3ErrorCode;

namespace {

bool is_missing(const S3Error& e) { return e.code == S3ErrorCode::NoSuchKey || e.code == S3ErrorCode::NoSuchBucket; }

int64_t unix_of(std::chrono::system_clock::time_point t) {
    return std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count();
}

struct Listed {
    std::string key;
    uint64_t size = 0;
    int64_t mtime_unix = 0;
};

Task<std::vector<Listed>> list_prefix(storage::IStorageBackend& backend, std::string_view bucket, std::string prefix) {
    std::vector<Listed> out;
    storage::ListOptions opt;
    opt.prefix = prefix;
    for (;;) {
        auto page = co_await backend.list_objects(bucket, opt);
        for (auto& o : page.objects) out.push_back({o.key, o.size, unix_of(o.last_modified)});
        if (!page.is_truncated) break;
        opt.start_after = page.next_token;
    }
    co_return out;
}

// the object as text, bounded; nullopt when missing
Task<std::optional<std::string>> read_text(storage::IStorageBackend& backend, std::string_view bucket, std::string key,
                                           size_t max) {
    storage::ObjectStream stream;
    try {
        stream = co_await backend.get_object(bucket, key, std::nullopt);
    } catch (const S3Error& e) {
        if (is_missing(e)) co_return std::nullopt;
        throw;
    }
    std::string text;
    std::byte buf[64 * 1024];
    for (;;) {
        size_t n = co_await stream.body->read(std::span(buf));
        if (n == 0) break;
        if (text.size() + n > max) throw commit_failed("metadata file " + key + " exceeds the size limit");
        text.append(reinterpret_cast<const char*>(buf), n);
    }
    co_return text;
}

std::optional<int64_t> property_int(const json& props, const char* key) {
    if (!props.is_object()) return std::nullopt;
    auto it = props.find(key);
    if (it == props.end()) return std::nullopt;
    try {
        if (it->is_number_integer()) return it->get<int64_t>();
        if (it->is_string()) return std::stoll(it->get<std::string>());
    } catch (const std::exception&) {
    }
    return std::nullopt;
}

[[noreturn]] void stale(const std::string& why) { throw S3Error(S3ErrorCode::InvalidRequest, "StalePlan: " + why); }

}  // namespace

// ---------- settings ----------

json EffectiveMaintenance::to_json() const {
    json j;
    j["retain_recent_metadata_files"] = planner.retain_recent;
    j["delete_enabled"] = delete_enabled;
    j["max_snapshot_age_ms"] = planner.max_snapshot_age_ms ? json(*planner.max_snapshot_age_ms) : json();
    j["min_snapshots_to_keep"] = planner.min_snapshots_to_keep;
    j["orphan_cleanup"] = planner.orphan_cleanup;
    j["safety_window_sec"] = planner.safety_window_sec;
    j["conflict"] = conflict;
    j["notes"] = notes;
    return j;
}

EffectiveMaintenance resolve_maintenance(const TablesConfig& cfg, const std::optional<MaintenanceConfig>& table_cfg,
                                         const json& table_properties) {
    EffectiveMaintenance e;
    e.planner.retain_recent = cfg.maintenance.retain_recent_metadata_files;
    e.planner.safety_window_sec = cfg.maintenance.safety_window_sec;
    e.planner.deep.concurrency = cfg.validate_concurrency;
    e.delete_enabled = cfg.maintenance.delete_enabled;
    if (table_cfg) {
        if (table_cfg->retain_recent_metadata_files) e.planner.retain_recent = *table_cfg->retain_recent_metadata_files;
        if (table_cfg->delete_enabled) e.delete_enabled = *table_cfg->delete_enabled;
        if (table_cfg->max_snapshot_age_ms) e.planner.max_snapshot_age_ms = *table_cfg->max_snapshot_age_ms;
        if (table_cfg->min_snapshots_to_keep) e.planner.min_snapshots_to_keep = *table_cfg->min_snapshots_to_keep;
        if (table_cfg->orphan_cleanup) e.planner.orphan_cleanup = *table_cfg->orphan_cleanup;
    }
    // Iceberg's own properties win; a disagreement with the table object is flagged
    if (auto age = property_int(table_properties, "history.expire.max-snapshot-age-ms")) {
        if (table_cfg && table_cfg->max_snapshot_age_ms && *table_cfg->max_snapshot_age_ms != *age) {
            e.conflict = true;
            e.notes.push_back("history.expire.max-snapshot-age-ms differs from the maintenance config");
        }
        e.planner.max_snapshot_age_ms = *age;
    }
    if (auto keep = property_int(table_properties, "history.expire.min-snapshots-to-keep")) {
        if (table_cfg && table_cfg->min_snapshots_to_keep && *table_cfg->min_snapshots_to_keep != *keep) {
            e.conflict = true;
            e.notes.push_back("history.expire.min-snapshots-to-keep differs from the maintenance config");
        }
        e.planner.min_snapshots_to_keep = static_cast<int>(*keep);
    }
    if (e.planner.retain_recent < 0) e.planner.retain_recent = 0;
    if (e.planner.min_snapshots_to_keep < 1) e.planner.min_snapshots_to_keep = 1;
    return e;
}

// ---------- plan ----------

json MaintenancePlan::to_json() const {
    json j;
    j["bucket"] = bucket;
    j["namespace"] = levels;
    j["name"] = name;
    j["version-token"] = version_token;
    j["metadata-candidates"] = metadata_candidates;
    j["expire-snapshots"] = expire_snapshots;
    j["expire-updates"] = expire_updates;
    j["expire-requirements"] = expire_requirements;
    j["orphan-candidates"] = orphan_candidates;
    j["manual-review"] = manual_review;
    j["notes"] = notes;
    j["planned-unix"] = planned_unix;
    return j;
}

std::optional<MaintenancePlan> MaintenancePlan::from_json(const json& j) {
    try {
        if (!j.is_object() || !j.contains("version-token") || !j.contains("bucket")) return std::nullopt;
        MaintenancePlan p;
        p.bucket = j.at("bucket").get<std::string>();
        for (auto& l : j.at("namespace")) p.levels.push_back(l.get<std::string>());
        p.name = j.at("name").get<std::string>();
        p.version_token = j.at("version-token").get<std::string>();
        for (auto& k : j.value("metadata-candidates", json::array())) p.metadata_candidates.push_back(k);
        for (auto& id : j.value("expire-snapshots", json::array())) p.expire_snapshots.push_back(id.get<int64_t>());
        p.expire_updates = j.value("expire-updates", json::array());
        p.expire_requirements = j.value("expire-requirements", json::array());
        for (auto& k : j.value("orphan-candidates", json::array())) p.orphan_candidates.push_back(k);
        p.manual_review = j.value("manual-review", false);
        for (auto& n : j.value("notes", json::array())) p.notes.push_back(n);
        p.planned_unix = j.value("planned-unix", int64_t(0));
        if (!p.expire_updates.is_array() || !p.expire_requirements.is_array()) return std::nullopt;
        return p;
    } catch (const json::exception&) {
        return std::nullopt;
    }
}

Task<MaintenancePlan> plan_table(Catalog& catalog, std::string_view bucket, const Levels& levels, std::string_view name,
                                 const PlannerOptions& opt, int64_t now_unix) {
    TableBucketEntry tb = co_await catalog.require_table_bucket(bucket);
    auto t = co_await catalog.load_table(bucket, levels, name);
    auto& backend = catalog.bucket_backend(bucket);
    const json& md = t.metadata;
    MaintenancePlan plan;
    plan.bucket = std::string(bucket);
    plan.levels = levels;
    plan.name = std::string(name);
    plan.version_token = t.entry.version_token;
    plan.planned_unix = now_unix;
    const int64_t window_limit = now_unix - opt.safety_window_sec;
    const std::string dir = tb.reserved_prefix + ns_path(levels) + "/" + std::string(name) + "/metadata/";

    // ---- 1. metadata retention set ----
    std::set<std::string> retained;
    retained.insert(t.entry.metadata_location);
    if (md.contains("metadata-log") && md["metadata-log"].is_array())
        for (auto& e : md["metadata-log"])
            if (e.is_object() && e.contains("metadata-file") && e["metadata-file"].is_string()) {
                try {
                    retained.insert(path_to_key(bucket, e["metadata-file"].get<std::string>()));
                } catch (const RestError&) {
                }
            }
    for (auto& r : co_await catalog.store()->list_commits(bucket, t.entry.table_id))
        if (r.status == "COMMITTED") retained.insert(r.new_metadata_location);
    auto listed = co_await list_prefix(backend, bucket, dir);
    std::vector<const Listed*> metadata_files;
    for (auto& o : listed)
        if (o.key.size() > 14 && o.key.compare(o.key.size() - 14, 14, ".metadata.json") == 0)
            metadata_files.push_back(&o);
    // "00003-<token>.metadata.json": the generation prefix sorts them
    std::sort(metadata_files.begin(), metadata_files.end(),
              [](const Listed* a, const Listed* b) { return a->key > b->key; });
    for (size_t i = 0; i < metadata_files.size() && static_cast<int>(i) < opt.retain_recent; ++i)
        retained.insert(metadata_files[i]->key);
    for (auto* o : metadata_files) {
        if (retained.count(o->key)) continue;
        if (o->mtime_unix > window_limit) {
            plan.notes.push_back("metadata " + o->key + " is younger than the safety window");
            continue;
        }
        plan.metadata_candidates.push_back(o->key);
    }
    std::sort(plan.metadata_candidates.begin(), plan.metadata_candidates.end());

    // ---- 2. snapshot expiry ----
    if (opt.max_snapshot_age_ms && md.contains("snapshots") && md["snapshots"].is_array()) {
        bool ref_rules = false;
        std::set<int64_t> pinned;
        int64_t current = iceberg::current_snapshot_id(md);
        if (current >= 0) pinned.insert(current);
        std::optional<int64_t> main_ref;
        if (md.contains("refs") && md["refs"].is_object())
            for (auto& [n, r] : md["refs"].items()) {
                if (!r.is_object() || !r.contains("snapshot-id")) continue;
                pinned.insert(r["snapshot-id"].get<int64_t>());
                if (n == "main") main_ref = r["snapshot-id"].get<int64_t>();
                if (n != "main" && (r.contains("max-ref-age-ms") || r.contains("min-snapshots-to-keep") ||
                                    r.contains("max-snapshot-age-ms")))
                    ref_rules = true;
            }
        if (ref_rules) {
            plan.manual_review = true;
            plan.notes.push_back("a ref carries its own retention rules; snapshot expiry needs manual review");
        } else {
            std::vector<std::pair<int64_t, int64_t>> by_time;
            std::map<int64_t, int64_t> parent_of;
            for (auto& s : md["snapshots"]) {
                int64_t id = s["snapshot-id"].get<int64_t>();
                by_time.emplace_back(s.value("timestamp-ms", int64_t(0)), id);
                if (s.contains("parent-snapshot-id") && s["parent-snapshot-id"].is_number_integer())
                    parent_of[id] = s["parent-snapshot-id"].get<int64_t>();
            }
            std::sort(by_time.begin(), by_time.end(), [](auto& a, auto& b) { return a.first > b.first; });
            const int64_t watermark = now_unix * 1000 - *opt.max_snapshot_age_ms;
            std::set<int64_t> keep = pinned;
            for (size_t i = 0; i < by_time.size(); ++i)
                if (static_cast<int>(i) < opt.min_snapshots_to_keep || by_time[i].first >= watermark)
                    keep.insert(by_time[i].second);
            // the parent chain of every kept snapshot stays too
            std::vector<int64_t> stack(keep.begin(), keep.end());
            while (!stack.empty()) {
                int64_t id = stack.back();
                stack.pop_back();
                auto p = parent_of.find(id);
                if (p != parent_of.end() && keep.insert(p->second).second) stack.push_back(p->second);
            }
            for (auto& [ts, id] : by_time)
                if (!keep.count(id)) plan.expire_snapshots.push_back(id);
            std::sort(plan.expire_snapshots.begin(), plan.expire_snapshots.end());
            if (!plan.expire_snapshots.empty()) {
                json u;
                u["action"] = "remove-snapshots";
                u["snapshot-ids"] = plan.expire_snapshots;
                plan.expire_updates.push_back(u);
                json r;
                if (main_ref) {
                    r["type"] = "assert-ref-snapshot-id";
                    r["ref"] = "main";
                    r["snapshot-id"] = *main_ref;
                } else {
                    r["type"] = "assert-table-uuid";
                    r["uuid"] = md.value("table-uuid", "");
                }
                plan.expire_requirements.push_back(r);
            }
        }
    }

    // ---- 3. orphans: files under the location no retained metadata reaches ----
    if (opt.orphan_cleanup) {
        std::string loc_key = location_to_key(bucket, tb.reserved_prefix, t.entry.location);
        iceberg::SnapshotCheckContext ctx{backend, std::string(bucket), tb.reserved_prefix};
        std::set<std::string> reach;
        bool ok = true;
        size_t manifests_seen = 0;
        for (auto& key : retained) {
            std::optional<std::string> text;
            try {
                text = co_await read_text(backend, bucket, key, catalog.config().metadata_max_size);
            } catch (const RestError& e) {
                plan.notes.push_back(std::string("orphan scan: ") + e.message);
                ok = false;
                break;
            }
            // a retained location that is gone (an old metadata-log entry) has nothing to reach
            if (!text) continue;
            json rmd;
            try {
                rmd = json::parse(*text);
            } catch (const json::exception&) {
                plan.notes.push_back("orphan scan: metadata " + key + " is not JSON");
                ok = false;
                break;
            }
            std::optional<std::set<std::string>> r;
            std::exception_ptr err;
            try {
                r = co_await iceberg::reachable_files(ctx, rmd, opt.deep, manifests_seen);
            } catch (const RestError& e) {
                plan.notes.push_back("orphan scan: " + e.message);
                ok = false;
            }
            if (!ok) break;
            if (!r) {
                plan.notes.push_back("orphan scan: a manifest of " + key + " uses an unreadable Avro codec");
                ok = false;
                break;
            }
            reach.insert(r->begin(), r->end());
        }
        if (!ok) {
            plan.manual_review = true;
        } else {
            for (const char* sub : {"/data/", "/delete/"}) {
                auto files = co_await list_prefix(backend, bucket, loc_key + sub);
                for (auto& o : files) {
                    if (reach.count(o.key)) continue;
                    if (o.mtime_unix > window_limit) continue;
                    plan.orphan_candidates.push_back(o.key);
                }
            }
            std::sort(plan.orphan_candidates.begin(), plan.orphan_candidates.end());
        }
    }
    co_return plan;
}

// ---------- run ----------

json RunReport::to_json() const {
    json j;
    j["expired_snapshots"] = expired_snapshots;
    j["deleted_metadata"] = deleted_metadata;
    j["deleted_orphans"] = deleted_orphans;
    j["skipped"] = skipped;
    j["delete_enabled"] = delete_enabled;
    j["generation"] = generation;
    return j;
}

Task<RunReport> run_table(Catalog& catalog, const MaintenancePlan& plan, const RunOptions& opt, int64_t now_unix) {
    RunReport rep;
    rep.delete_enabled = opt.delete_enabled;
    co_await catalog.require_table_bucket(plan.bucket);
    auto ptr = co_await catalog.table_pointer(plan.bucket, plan.levels, plan.name);
    if (!ptr) stale("table " + ns_display(plan.levels) + "." + plan.name + " no longer exists");
    if (ptr->value.version_token != plan.version_token) stale("table changed since planning");
    // ---- 1. snapshot expiry through the standard commit ----
    if (!plan.expire_snapshots.empty()) {
        CommitRequest req;
        req.commit_id = "maint-" + new_uuid();
        req.requirements = plan.expire_requirements;
        req.updates = plan.expire_updates;
        CommitHooks hooks;
        hooks.note_usage = opt.note_usage;
        std::optional<RestError> conflict;
        try {
            auto t = co_await catalog.commit_table(plan.bucket, plan.levels, plan.name, req, hooks);
            rep.generation = t.entry.generation;
        } catch (const RestError& e) {
            if (e.status != 409) throw;
            conflict = e;
        }
        if (conflict) stale("expiry commit was refused: " + conflict->message);
        rep.expired_snapshots = static_cast<int>(plan.expire_snapshots.size());
        LOG_INFO("tables: expired {} snapshot(s) of {}.{} (generation {})", rep.expired_snapshots,
                 ns_display(plan.levels), plan.name, rep.generation);
    }
    if (!opt.delete_enabled) co_return rep;
    // ---- 2. deletes, each re-checked against the safety window ----
    auto& backend = catalog.bucket_backend(plan.bucket);
    const int64_t window_limit = now_unix - opt.safety_window_sec;
    int done = 0;
    auto remove = [&](const std::string& key, bool is_metadata) -> Task<bool> {
        if (is_metadata) {
            auto cur = co_await catalog.table_pointer(plan.bucket, plan.levels, plan.name);
            if (cur && cur->value.metadata_location == key) co_return false;
        }
        std::optional<storage::ObjectMeta> meta;
        try {
            meta = co_await backend.head_object(plan.bucket, key);
        } catch (const S3Error& e) {
            if (!is_missing(e)) throw;
        }
        if (!meta || unix_of(meta->last_modified) > window_limit) co_return false;
        co_await backend.delete_object(plan.bucket, key);
        if (opt.note_usage) opt.note_usage(plan.bucket, -1, -static_cast<int64_t>(meta->size));
        if (++done % 64 == 0 && catalog.pool()) co_await catalog.pool()->schedule();
        co_return true;
    };
    for (auto& k : plan.metadata_candidates) {
        if (co_await remove(k, true))
            ++rep.deleted_metadata;
        else
            ++rep.skipped;
    }
    for (auto& k : plan.orphan_candidates) {
        if (co_await remove(k, false))
            ++rep.deleted_orphans;
        else
            ++rep.skipped;
    }
    LOG_INFO("tables: maintenance of {}.{} deleted {} metadata file(s) and {} orphan(s), skipped {}",
             ns_display(plan.levels), plan.name, rep.deleted_metadata, rep.deleted_orphans, rep.skipped);
    co_return rep;
}

// ---------- purge ----------

json PurgeReport::to_json() const {
    json j;
    j["deleted_objects"] = deleted_objects;
    j["deleted_bytes"] = deleted_bytes;
    j["tombstone_removed"] = tombstone_removed;
    return j;
}

Task<PurgeReport> purge_table(Catalog& catalog, std::string_view bucket, const Levels& levels, std::string_view name,
                              const RunOptions& opt) {
    PurgeReport rep;
    TableBucketEntry tb = co_await catalog.require_table_bucket(bucket);
    auto cur = co_await catalog.store()->get_table(bucket, levels, name);
    if (!cur) throw not_found_table("table " + ns_display(levels) + "." + std::string(name) + " does not exist");
    if (cur->value.state != TableState::Deleted)
        throw S3Error(S3ErrorCode::InvalidRequest, "table " + ns_display(levels) + "." + std::string(name) +
                                                       " is not dropped; purge follows a drop");
    auto& backend = catalog.bucket_backend(bucket);
    int done = 0;
    auto wipe = [&](std::string prefix) -> Task<void> {
        for (;;) {
            storage::ListOptions lo;
            lo.prefix = prefix;
            lo.max_keys = 1000;
            auto page = co_await backend.list_objects(bucket, lo);
            for (auto& o : page.objects) {
                try {
                    co_await backend.delete_object(bucket, o.key);
                } catch (const S3Error& e) {
                    if (!is_missing(e)) throw;
                    continue;
                }
                ++rep.deleted_objects;
                rep.deleted_bytes += o.size;
                if (opt.note_usage) opt.note_usage(bucket, -1, -static_cast<int64_t>(o.size));
                if (++done % 64 == 0 && catalog.pool()) co_await catalog.pool()->schedule();
            }
            if (page.objects.empty() || !page.is_truncated) break;
        }
    };
    co_await wipe(tb.reserved_prefix + ns_path(levels) + "/" + std::string(name) + "/");
    std::string loc_key;
    try {
        loc_key = location_to_key(bucket, tb.reserved_prefix, cur->value.location);
    } catch (const RestError& e) {
        LOG_WARN("tables: purge of {}.{} skips the location {}: {}", ns_display(levels), name, cur->value.location,
                 e.message);
    }
    if (!loc_key.empty()) co_await wipe(loc_key + "/");
    auto& store = *catalog.store();
    for (auto& r : co_await store.list_commits(bucket, cur->value.table_id))
        co_await store.delete_commit(bucket, cur->value.table_id, r.commit_id);
    co_await store.delete_maintenance_config(bucket, levels, name);
    co_await store.delete_table(bucket, levels, name);
    rep.tombstone_removed = true;
    LOG_INFO("tables: purged {}.{}: {} object(s), {} bytes", ns_display(levels), name, rep.deleted_objects,
             rep.deleted_bytes);
    co_return rep;
}

}  // namespace lights3::tables
