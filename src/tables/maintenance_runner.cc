#include "tables/maintenance_runner.h"

#include <future>

#include "core/log.h"
#include "s3/errors.h"
#include "tables/rest_error.h"

namespace lights3::tables {

using nlohmann::json;

Task<json> MaintenanceRunner::plan_and_run(const std::string& bucket, const Levels& levels, const std::string& name) {
    auto t = co_await catalog_->load_table(bucket, levels, name);
    auto table_cfg = co_await catalog_->store()->get_maintenance_config(bucket, levels, name);
    EffectiveMaintenance eff = resolve_maintenance(cfg_, table_cfg, t.metadata.value("properties", json::object()));
    const int64_t now_unix = now();
    MaintenancePlan plan = co_await plan_table(*catalog_, bucket, levels, name, eff.planner, now_unix);
    if (eff.conflict) {
        plan.manual_review = true;
        for (auto& n : eff.notes) plan.notes.push_back(n);
    }
    json out;
    out["plan"] = plan.to_json();
    if (plan.manual_review) {
        out["run"] = nullptr;
        out["skipped"] = "manual-review";
        co_return out;
    }
    RunOptions ro;
    ro.delete_enabled = eff.delete_enabled;
    ro.safety_window_sec = eff.planner.safety_window_sec;
    ro.note_usage = note_usage_;
    RunReport rep = co_await run_table(*catalog_, plan, ro, now_unix);
    out["run"] = rep.to_json();
    co_return out;
}

Task<void> MaintenanceRunner::maintain_table(const std::string& bucket, const Levels& levels, const std::string& name,
                                             PassStats& st) {
    ++st.tables;
    if (!jobs_) {
        std::exception_ptr err;
        try {
            co_await plan_and_run(bucket, levels, name);
            ++st.planned;
            ++st.ran;
        } catch (...) {
            err = std::current_exception();
        }
        if (err) {
            ++st.failed;
            try {
                std::rethrow_exception(err);
            } catch (const std::exception& e) {
                LOG_WARN("tables: maintenance of {}.{} failed: {}", ns_display(levels), name, e.what());
            }
        }
        co_return;
    }
    // through the job framework: the work runs on the job's thread, the pass waits for
    // it (one table at a time, so the pass never fans out threads)
    auto done = std::make_shared<std::promise<void>>();
    auto future = done->get_future();
    auto fn = [this, bucket, levels, name, done]() -> json {
        struct Release {
            std::shared_ptr<std::promise<void>> p;
            ~Release() { p->set_value(); }
        } release{done};
        return sync_wait(plan_and_run(bucket, levels, name));
    };
    bool started = false;
    try {
        jobs_.start(job_resource(bucket, levels, name), "run", std::move(fn));
        started = true;
    } catch (const s3::S3Error& e) {
        if (e.code != s3::S3ErrorCode::JobInProgress) throw;
        LOG_INFO("tables: maintenance of {}.{} skipped: a job is running on it", ns_display(levels), name);
        ++st.skipped_busy;
    }
    if (!started) co_return;
    future.wait();
    json status = jobs_.status(job_resource(bucket, levels, name), "run");
    if (status.contains("error")) {
        ++st.failed;
        LOG_WARN("tables: maintenance of {}.{} failed: {}", ns_display(levels), name,
                 status["error"].get<std::string>());
    } else {
        ++st.planned;
        ++st.ran;
    }
}

Task<void> MaintenanceRunner::walk_namespace(const std::string& bucket, const Levels& levels, PassStats& st) {
    auto& store = *catalog_->store();
    if (!levels.empty()) {
        PageCursor c;
        c.limit = 1000;
        for (;;) {
            auto page = co_await store.list_tables(bucket, levels, c);
            for (auto& name : page.items) {
                auto e = co_await store.get_table(bucket, levels, name);
                if (!e) continue;
                if (e->value.state == TableState::Deleted) {
                    int64_t ttl = cfg_.maintenance.tombstone_ttl_sec;
                    if (ttl > 0 && e->value.updated_unix + ttl < now()) {
                        co_await store.delete_maintenance_config(bucket, levels, name);
                        co_await store.delete_table(bucket, levels, name);
                        ++st.tombstones_removed;
                        LOG_INFO("tables: tombstone of {}.{} removed after {} s", ns_display(levels), name, ttl);
                    }
                    continue;
                }
                if (e->value.state != TableState::Active) continue;
                co_await maintain_table(bucket, levels, name, st);
            }
            if (page.next_after.empty()) break;
            c.after = page.next_after;
        }
    }
    PageCursor c;
    c.limit = 1000;
    for (;;) {
        auto page = co_await store.list_child_namespaces(bucket, levels, c);
        for (auto& seg : page.items) {
            Levels child = levels;
            child.push_back(seg);
            co_await walk_namespace(bucket, child, st);
        }
        if (page.next_after.empty()) break;
        c.after = page.next_after;
    }
}

Task<MaintenanceRunner::PassStats> MaintenanceRunner::run_once() {
    PassStats st;
    auto snap = catalog_->bucket_store()->snapshot();
    for (auto& [bucket, entry] : *snap) {
        if (!entry.enabled) continue;
        ++st.buckets;
        std::exception_ptr err;
        try {
            st.renames_driven += static_cast<uint64_t>(co_await catalog_->recover_renames(bucket));
            co_await walk_namespace(bucket, {}, st);
        } catch (...) {
            err = std::current_exception();
        }
        if (err) {
            ++st.failed;
            try {
                std::rethrow_exception(err);
            } catch (const std::exception& e) {
                LOG_WARN("tables: maintenance pass over bucket {} failed: {}", bucket, e.what());
            }
        }
    }
    LOG_INFO("tables: maintenance pass: {} bucket(s), {} table(s), {} run, {} busy, {} failed, {} tombstone(s) removed",
             st.buckets, st.tables, st.ran, st.skipped_busy, st.failed, st.tombstones_removed);
    co_return st;
}

Task<void> MaintenanceRunner::scan_tick() {
    co_await pool_->schedule();
    std::exception_ptr err;
    try {
        co_await run_once();
    } catch (...) {
        err = std::current_exception();
    }
    schedule_scan();
    if (err) std::rethrow_exception(err);
}

void MaintenanceRunner::schedule_scan() {
    if (scan_interval_sec_ <= 0) return;
    bg_.if_open([&] {
        scan_timer_ = TimerQueue::instance().add(std::chrono::seconds(scan_interval_sec_),
                                                 [this] { bg_.spawn(scan_tick()); });
    });
}

void MaintenanceRunner::start_background(std::shared_ptr<ThreadPool> pool, int scan_interval_sec) {
    pool_ = std::move(pool);
    scan_interval_sec_ = scan_interval_sec;
    schedule_scan();
}

void MaintenanceRunner::shutdown_background() {
    bg_.begin_close();
    TimerQueue::instance().cancel(scan_timer_);
    bg_.wait_idle();
}

}  // namespace lights3::tables
