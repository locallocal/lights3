// Periodic table maintenance (docs/s3-tables/step-4-maintenance.md §6): the LifecycleRunner
// skeleton (TimerQueue tick → BackgroundTaskGroup → one pass, rounds never overlap) over
// every table bucket: each Active table is planned and run with its effective settings,
// tombstones past tombstone_ttl are dropped, stranded rename intents are driven. With
// job hooks installed every table's plan+run is a job on the table's resource, so a
// manual job on that table makes the runner skip it this round
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <string>
#include <string_view>

#include "core/background.h"
#include "core/task.h"
#include "core/thread_pool.h"
#include "core/timer.h"
#include "tables/catalog.h"
#include "tables/jobs.h"
#include "tables/maintenance.h"

namespace lights3::tables {

class MaintenanceRunner {
public:
    MaintenanceRunner(std::shared_ptr<Catalog> catalog, TablesConfig cfg)
        : catalog_(std::move(catalog)), cfg_(std::move(cfg)) {}
    ~MaintenanceRunner() { shutdown_background(); }

    struct PassStats {
        uint64_t buckets = 0;
        uint64_t tables = 0;
        uint64_t planned = 0;
        uint64_t ran = 0;
        uint64_t skipped_busy = 0;
        uint64_t failed = 0;
        uint64_t tombstones_removed = 0;
        uint64_t renames_driven = 0;
    };
    // One full pass (the manual hook for tests / ops)
    Task<PassStats> run_once();

    // Route every table's plan+run through the job framework (resource-level mutual
    // exclusion with manual jobs); without hooks the pass runs the work inline
    void set_job_hooks(JobHooks hooks) { jobs_ = std::move(hooks); }
    void set_usage_hook(std::function<void(std::string_view, int64_t, int64_t)> fn) { note_usage_ = std::move(fn); }
    // Test hook: overrides "now" (unix seconds)
    void set_now_for_tests(std::function<int64_t()> fn) { now_ = std::move(fn); }

    void start_background(std::shared_ptr<ThreadPool> pool, int scan_interval_sec);
    void shutdown_background();

private:
    int64_t now() const { return now_ ? now_() : now_unix(); }
    Task<void> scan_tick();
    void schedule_scan();
    Task<void> walk_namespace(const std::string& bucket, const Levels& levels, PassStats& st);
    Task<void> maintain_table(const std::string& bucket, const Levels& levels, const std::string& name, PassStats& st);
    // plan + run under the table's effective settings; the stats document
    Task<nlohmann::json> plan_and_run(const std::string& bucket, const Levels& levels, const std::string& name);

    std::shared_ptr<Catalog> catalog_;
    TablesConfig cfg_;
    JobHooks jobs_;
    std::function<void(std::string_view, int64_t, int64_t)> note_usage_;
    std::function<int64_t()> now_;

    std::shared_ptr<ThreadPool> pool_;
    int scan_interval_sec_ = 0;
    TimerQueue::Id scan_timer_ = 0;
    BackgroundTaskGroup bg_{"tables-maintenance"};
};

}  // namespace lights3::tables
