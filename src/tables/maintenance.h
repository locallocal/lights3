// Table maintenance (docs/architecture/s3-tables-design.md §9):
// a read-only planner that binds its findings to the table's version token, a runner
// that executes a plan (snapshot expiry through the standard commit, then file deletes
// re-checked against the safety window), and the purge that follows a
// purgeRequested=true drop. Everything is a coroutine over the Catalog; the job
// framework (jobs.h) runs them on dedicated threads
#pragma once

#include <cstdint>
#include <functional>
#include <nlohmann/json.hpp>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/task.h"
#include "tables/catalog.h"
#include "tables/iceberg/snapshots.h"

namespace lights3::tables {

struct PlannerOptions {
    int retain_recent = 10;
    int safety_window_sec = 900;
    // no age → no snapshot expiry
    std::optional<int64_t> max_snapshot_age_ms;
    int min_snapshots_to_keep = 1;
    bool orphan_cleanup = true;
    // compaction candidates (step ⑥ §4): data files under small_file_ratio × target are
    // grouped per (partition directory, sort order) and bin-packed to the target; 0 = off.
    // The target follows the table property write.target-file-size-bytes when set
    int64_t target_file_size_bytes = 512ll << 20;
    double small_file_ratio = 0.75;
    iceberg::DeepCheckOptions deep;
};

// One bin of files the engine should rewrite into one (docs/architecture/s3-tables-design.md §9):
// not executed here (Spark rewrite_data_files / the engine's own compaction)
struct CompactionCandidate {
    // the files' directory prefix (Iceberg partitions are directories)
    std::string partition;
    int sort_order_id = 0;
    std::vector<std::string> files;
    int64_t bytes = 0;
    // delete files touch this partition: a rewrite must apply them (row-level)
    bool row_level_required = false;
};

// The settings a table is maintained with, resolved from (highest first) the Iceberg
// table properties history.expire.*, the table's MaintenanceConfig object and
// tables.maintenance (step ④ §3). A property and the table object disagreeing is
// reported (the plan then carries manual_review)
struct EffectiveMaintenance {
    PlannerOptions planner;
    bool delete_enabled = false;
    bool conflict = false;
    std::vector<std::string> notes;
    // {"retain_recent_metadata_files","delete_enabled","max_snapshot_age_ms","min_snapshots_to_keep",
    //  "orphan_cleanup","safety_window_sec","conflict","notes"}
    nlohmann::json to_json() const;
};
EffectiveMaintenance resolve_maintenance(const TablesConfig& cfg, const std::optional<MaintenanceConfig>& table_cfg,
                                         const nlohmann::json& table_properties);

struct MaintenancePlan {
    std::string bucket;
    Levels levels;
    std::string name;
    // the pointer the plan is bound to
    std::string version_token;
    // keys under the reserved metadata/ directory that may be deleted
    std::vector<std::string> metadata_candidates;
    std::vector<int64_t> expire_snapshots;
    // ready-to-commit updates / requirements for the expiry
    nlohmann::json expire_updates = nlohmann::json::array();
    nlohmann::json expire_requirements = nlohmann::json::array();
    // keys under the table location that no retained metadata reaches
    std::vector<std::string> orphan_candidates;
    std::vector<CompactionCandidate> compaction_candidates;
    bool manual_review = false;
    std::vector<std::string> notes;
    int64_t planned_unix = 0;

    nlohmann::json to_json() const;
    // nullopt when the document is not a plan
    static std::optional<MaintenancePlan> from_json(const nlohmann::json& j);
};

// Read-only. Throws RestError (404 / 503 from the catalog, 409 for a malformed table)
Task<MaintenancePlan> plan_table(Catalog& catalog, std::string_view bucket, const Levels& levels, std::string_view name,
                                 const PlannerOptions& opt, int64_t now_unix);

struct RunOptions {
    bool delete_enabled = false;
    int safety_window_sec = 900;
    // (bucket, d_objects, d_bytes) for every deleted object
    std::function<void(std::string_view, int64_t, int64_t)> note_usage;
};

struct RunReport {
    int expired_snapshots = 0;
    int deleted_metadata = 0;
    int deleted_orphans = 0;
    int skipped = 0;
    bool delete_enabled = false;
    // generation after the expiry commit (0 = no commit)
    uint64_t generation = 0;
    nlohmann::json to_json() const;
};

// Throws s3::S3Error(InvalidRequest, "StalePlan: ...") when the table moved since the
// plan (or its expiry commit conflicts); other errors propagate
Task<RunReport> run_table(Catalog& catalog, const MaintenancePlan& plan, const RunOptions& opt, int64_t now_unix);

struct PurgeReport {
    uint64_t deleted_objects = 0;
    uint64_t deleted_bytes = 0;
    bool tombstone_removed = false;
    nlohmann::json to_json() const;
};

// After a drop: the reserved directory, the location prefix, the commit records, the
// maintenance settings and the tombstone. Requires a Deleted entry (InvalidRequest
// otherwise); missing entry → NoSuchTableException
Task<PurgeReport> purge_table(Catalog& catalog, std::string_view bucket, const Levels& levels, std::string_view name,
                              const RunOptions& opt);

}  // namespace lights3::tables
