// Lifecycle minimal subset (roadmap §2.4): Expiration.Days +
// AbortIncompleteMultipartUpload.DaysAfterInitiation per bucket. Rules are managed
// through ?lifecycle (root credential only), persisted as .sys/lifecycle/<bucket> JSON
// via SysConfigStore, and enforced by a periodic LifecycleRunner scan. Everything else
// in the AWS Lifecycle surface (Transitions, tag filters, versioned semantics, Date
// forms) answers an honest 501 at the XML gate.
#pragma once

#include <chrono>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "core/background.h"
#include "core/task.h"
#include "core/thread_pool.h"
#include "core/timer.h"
#include "s3/sys_config_store.h"
#include "s3/usage.h"
#include "storage/bucket_router.h"

namespace lights3::s3 {

struct LifecycleRule {
    std::string id;
    bool enabled = true;
    std::string prefix;              // "" = whole bucket
    int expiration_days = 0;         // 0 = no object expiration
    int abort_incomplete_days = 0;   // 0 = no stale-MPU abort

    bool operator==(const LifecycleRule&) const = default;
};

struct LifecycleTraits {
    using Entry = std::vector<LifecycleRule>;
    static constexpr std::string_view kPrefix = "lifecycle/";
    static constexpr const char* kName = "lifecycle";
    static std::string serialize(const Entry& rules);
    static std::optional<Entry> deserialize(const std::string& bucket, const std::string& body);
    static bool differs(const Entry& a, const Entry& b) { return a != b; }
};

using LifecycleStore = SysConfigStore<LifecycleTraits>;

// Periodic enforcement: one pass walks every bucket with rules, expiring objects and
// aborting stale multipart uploads through the normal backend interface (each backend's
// own deletion semantics apply — duostore GC, localfs unlink, cloudproxy remote delete).
// Per-bucket failures are logged and do not abort the pass
class LifecycleRunner {
public:
    LifecycleRunner(storage::BucketRouter router, std::shared_ptr<LifecycleStore> store)
        : router_(std::move(router)), store_(std::move(store)) {}
    ~LifecycleRunner() { shutdown_background(); }

    // One full pass (manual hook for tests/ops); returns {objects expired, uploads aborted}
    struct PassStats {
        uint64_t objects_expired = 0;
        uint64_t uploads_aborted = 0;
    };
    Task<PassStats> run_once();

    void start_background(std::shared_ptr<ThreadPool> pool, int scan_interval_sec);
    void shutdown_background();

    // Test hook: overrides "now" for age decisions (backends stamp real times)
    void set_now_for_tests(std::function<std::chrono::system_clock::time_point()> fn) {
        now_ = std::move(fn);
    }
    // Usage accounting (roadmap §3.9 ①): expirations and aborts adjust the counters
    // like their request-path twins
    void set_usage_tracker(std::shared_ptr<UsageTracker> u) { usage_ = std::move(u); }

private:
    std::chrono::system_clock::time_point now() const {
        return now_ ? now_() : std::chrono::system_clock::now();
    }
    Task<void> scan_tick();
    void schedule_scan();

    storage::BucketRouter router_;
    std::shared_ptr<LifecycleStore> store_;
    std::shared_ptr<UsageTracker> usage_;
    std::function<std::chrono::system_clock::time_point()> now_;

    std::shared_ptr<ThreadPool> pool_;
    int scan_interval_sec_ = 0;
    TimerQueue::Id scan_timer_ = 0;
    BackgroundTaskGroup bg_{"lifecycle"};
};

}  // namespace lights3::s3
