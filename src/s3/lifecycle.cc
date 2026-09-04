#include "s3/lifecycle.h"

#include <nlohmann/json.hpp>

#include "core/log.h"
#include "s3/errors.h"
#include "storage/multipart.h"

namespace lights3::s3 {

using nlohmann::json;

std::string LifecycleTraits::serialize(const Entry& rules) {
    json arr = json::array();
    for (auto& r : rules) {
        json j;
        if (!r.id.empty()) j["id"] = r.id;
        j["enabled"] = r.enabled;
        if (!r.prefix.empty()) j["prefix"] = r.prefix;
        if (r.expiration_days) j["expiration_days"] = r.expiration_days;
        if (r.abort_incomplete_days) j["abort_incomplete_days"] = r.abort_incomplete_days;
        arr.push_back(std::move(j));
    }
    return json{{"rules", std::move(arr)}}.dump();
}

std::optional<LifecycleTraits::Entry> LifecycleTraits::deserialize(const std::string&,
                                                                   const std::string& body) {
    try {
        auto j = json::parse(body);
        Entry rules;
        for (auto& jr : j.at("rules")) {
            LifecycleRule r;
            r.id = jr.value("id", "");
            r.enabled = jr.value("enabled", true);
            r.prefix = jr.value("prefix", "");
            r.expiration_days = jr.value("expiration_days", 0);
            r.abort_incomplete_days = jr.value("abort_incomplete_days", 0);
            // A rule with no action, or negative days, is not valid live config
            if (r.expiration_days < 0 || r.abort_incomplete_days < 0) return std::nullopt;
            if (r.expiration_days == 0 && r.abort_incomplete_days == 0) return std::nullopt;
            rules.push_back(std::move(r));
        }
        if (rules.empty()) return std::nullopt;
        return rules;
    } catch (...) {
        return std::nullopt;
    }
}

// ---------- Runner ----------

namespace {
constexpr auto kDay = std::chrono::hours(24);
}

Task<LifecycleRunner::PassStats> LifecycleRunner::run_once() {
    PassStats stats;
    auto snap = store_->snapshot();
    if (!snap) co_return stats;
    for (auto& [bucket, rules] : *snap) {
        try {
            auto& backend = router_.resolve(bucket);
            for (auto& rule : rules) {
                if (!rule.enabled) continue;
                // Stale multipart uploads first: their staging space is the roadmap's
                // headline waste ("zombie MPU garbage is never reclaimed")
                if (rule.abort_incomplete_days > 0) {
                    storage::ListUploadsOptions opt;
                    opt.prefix = rule.prefix;
                    for (;;) {
                        auto page = co_await backend.list_multipart_uploads(bucket, opt);
                        for (auto& u : page.uploads) {
                            if (now() - u.initiated < rule.abort_incomplete_days * kDay)
                                continue;
                            try {
                                // Usage accounting (roadmap §3.9 ①): in-flight bytes leave with the upload
                                int64_t stored = 0;
                                if (usage_ && usage_->enabled()) {
                                    storage::ListPartsOptions popt;
                                    popt.max_parts = storage::kMaxParts;
                                    auto parts = co_await backend.list_parts(bucket, u.key,
                                                                             u.upload_id, popt);
                                    for (auto& p : parts.parts) stored += int64_t(p.size);
                                }
                                co_await backend.abort_multipart(bucket, u.key, u.upload_id);
                                if (usage_) usage_->apply(bucket, 0, 0, -stored);
                                ++stats.uploads_aborted;
                            } catch (const std::exception& e) {
                                // Raced with a concurrent complete/abort: skip, next
                                // pass settles it
                                LOG_WARN("lifecycle: abort {}/{} upload {} failed: {}",
                                         bucket, u.key, u.upload_id, e.what());
                            }
                        }
                        if (!page.is_truncated) break;
                        opt.key_marker = page.next_key_marker;
                        opt.upload_id_marker = page.next_upload_id_marker;
                    }
                }
                if (rule.expiration_days > 0) {
                    storage::ListOptions opt;
                    opt.prefix = rule.prefix;
                    for (;;) {
                        auto page = co_await backend.list_objects(bucket, opt);
                        for (auto& o : page.objects) {
                            if (now() - o.last_modified < rule.expiration_days * kDay)
                                continue;
                            try {
                                co_await backend.delete_object(bucket, o.key);
                                if (usage_) usage_->apply(bucket, -1, -int64_t(o.size));
                                ++stats.objects_expired;
                            } catch (const std::exception& e) {
                                LOG_WARN("lifecycle: expire {}/{} failed: {}", bucket, o.key,
                                         e.what());
                            }
                        }
                        if (!page.is_truncated) break;
                        opt.start_after = page.next_token;
                    }
                }
            }
        } catch (const std::exception& e) {
            // A missing bucket (rules outliving it) or a down backend must not stall the
            // other buckets' rules
            LOG_WARN("lifecycle: pass over bucket {} failed: {}", bucket, e.what());
        }
    }
    if (stats.objects_expired || stats.uploads_aborted)
        LOG_INFO("lifecycle: expired {} object(s), aborted {} stale upload(s)",
                 stats.objects_expired, stats.uploads_aborted);
    co_return stats;
}

Task<void> LifecycleRunner::scan_tick() {
    co_await pool_->schedule();  // the timer thread only dispatches; IO moves to a pool thread
    std::exception_ptr err;
    try {
        co_await run_once();
    } catch (...) {
        err = std::current_exception();
    }
    schedule_scan();  // re-arm after completion: rounds never overlap (localfs mpu pattern)
    if (err) std::rethrow_exception(err);  // hand off to BackgroundTaskGroup for logging
}

void LifecycleRunner::schedule_scan() {
    if (scan_interval_sec_ <= 0) return;
    bg_.if_open([&] {
        scan_timer_ = TimerQueue::instance().add(std::chrono::seconds(scan_interval_sec_),
                                                 [this] { bg_.spawn(scan_tick()); });
    });
}

void LifecycleRunner::start_background(std::shared_ptr<ThreadPool> pool,
                                       int scan_interval_sec) {
    pool_ = std::move(pool);
    scan_interval_sec_ = scan_interval_sec;
    schedule_scan();
}

void LifecycleRunner::shutdown_background() {
    bg_.begin_close();
    // cancel outside the group lock (TimerQueue::cancel blocks on in-flight callbacks)
    TimerQueue::instance().cancel(scan_timer_);
    bg_.wait_idle();
}

}  // namespace lights3::s3
