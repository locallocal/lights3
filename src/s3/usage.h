// L2: bucket usage accounting (roadmap §3.9 ①, docs/multi-tenancy.md §2).
// Per-bucket {objects, bytes, mpu_bytes} counters maintained by the gateway:
//   - incremental: every write handler applies its delta after the backend commit
//     (PutObject/Copy/Complete: +size −replaced; Delete: −size; UploadPart: +part
//     into mpu_bytes; Complete/Abort: parts leave mpu_bytes). The replaced/deleted
//     size comes from a HEAD before the write — cheap on backends with the §3.8
//     meta cache, one remote round trip on cloudproxy;
//   - persisted: dirty counters are flushed to .sys/usage/<bucket> every
//     usage.flush_interval and at shutdown, so a restart resumes where it left off;
//   - reconciled: a full listing (objects + in-flight multipart parts) replaces the
//     counters every usage.reconcile_interval, at startup for buckets without a
//     record, and on demand (admin API / s3adm usage --rescan). The listing walks
//     the normal IStorageBackend API, so every backend is covered uniformly.
// Accuracy contract: exact right after a scan; between scans the counters drift by
// at most the effect of concurrent same-key writes and, in multi-gateway setups,
// by the writes other gateways performed (each instance only sees its own
// deltas). Quota enforcement reads these counters and inherits the contract.
#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

#include "core/background.h"
#include "core/config.h"
#include "core/metrics.h"
#include "core/task.h"
#include "core/thread_pool.h"
#include "core/timer.h"
#include "storage/bucket_router.h"

namespace lights3::s3 {

struct BucketUsage {
    int64_t objects = 0;
    int64_t bytes = 0;      // committed object bytes
    int64_t mpu_bytes = 0;  // in-flight multipart part bytes (counted toward quotas)
    // Last full count by any instance; epoch = never scanned (counters started from zero)
    std::chrono::system_clock::time_point scanned_at{};
    bool dirty = false;     // has unflushed local deltas (never persisted)

    int64_t total_bytes() const { return bytes + mpu_bytes; }
    bool scanned() const { return scanned_at.time_since_epoch().count() != 0; }
};

class UsageTracker {
public:
    // Loads .sys/usage/* from the router's default backend. A missing .sys or a
    // malformed object just means "not scanned yet"
    static Task<std::shared_ptr<UsageTracker>> load(storage::BucketRouter router,
                                                    UsageConfig cfg,
                                                    std::shared_ptr<MetricsRegistry> metrics);
    ~UsageTracker();

    bool enabled() const { return cfg_.enabled; }

    // Incremental deltas (post-commit). Counters clamp at zero: a negative excursion
    // means the base was stale, and reconcile fixes the base
    void apply(const std::string& bucket, int64_t d_objects, int64_t d_bytes,
               int64_t d_mpu_bytes = 0);

    std::optional<BucketUsage> get(const std::string& bucket) const;
    std::map<std::string, BucketUsage> all() const;
    BucketUsage sum(const std::vector<std::string>& buckets) const;

    // Full count of one bucket through the backend API; replaces the entry and
    // persists it. Throws (NoSuchBucket etc.) when the bucket cannot be listed
    Task<BucketUsage> rescan(std::string bucket);
    // Every bucket of every backend (skipping .sys); returns the number scanned
    Task<size_t> reconcile_all();
    // Persist dirty entries
    Task<void> flush();
    // Multi-gateway: adopt persisted entries carrying a newer scan than ours
    Task<void> sync_now();
    // DeleteBucket: forget the counters (and the persisted record)
    Task<void> remove(const std::string& bucket);

    // Quota rejection counter (rendered as lights3_quota_rejections_total{scope})
    void quota_rejected(bool tenant_scope);

    // Timers: flush / reconcile / sync; the bootstrap scan (buckets without a
    // record) runs once at start when this instance reconciles
    void start_background(std::shared_ptr<ThreadPool> pool, int sync_interval_sec);
    void shutdown_background();

    // Test hooks
    size_t scans_completed() const { return scans_.load(); }

private:
    UsageTracker() = default;

    static constexpr std::string_view kPrefix = "usage/";
    static std::string object_key(const std::string& bucket) {
        return std::string(kPrefix) + bucket;
    }
    static std::string serialize(const BucketUsage& u);
    static std::optional<BucketUsage> deserialize(const std::string& body);

    Task<void> ensure_sys_bucket();
    Task<void> persist(const std::string& bucket, BucketUsage u);
    Task<std::map<std::string, BucketUsage>> read_persisted();
    Task<void> bootstrap_scan();
    Task<void> flush_tick();
    Task<void> reconcile_tick();
    Task<void> sync_tick();
    void schedule_flush();
    void schedule_reconcile();
    void schedule_sync();
    // Per-bucket gauge registration is split in two so the registry's mutex is
    // never taken while mu_ is held: the render path locks the registry first and
    // then (through the callback) mu_, so the opposite order here would deadlock.
    // claim_gauge_locked reserves the slot under mu_; register_gauges does the
    // registry call with mu_ released
    bool claim_gauge_locked(const std::string& bucket);
    void register_gauges(const std::string& bucket);

    storage::BucketRouter router_;
    UsageConfig cfg_;
    std::shared_ptr<MetricsRegistry> metrics_;
    std::shared_ptr<MetricCounter> m_reject_bucket_, m_reject_tenant_, m_scans_;
    std::shared_ptr<MetricGauge> m_last_scan_;

    mutable std::mutex mu_;
    std::map<std::string, BucketUsage> usage_;
    std::set<std::string> scanning_;      // single-flight per bucket
    std::set<std::string> gauged_;        // buckets with registered per-bucket gauges
    static constexpr size_t kMaxGaugedBuckets = 512;
    std::atomic<bool> sys_bucket_ready_{false};
    std::atomic<size_t> scans_{0};

    std::shared_ptr<ThreadPool> pool_;
    int sync_interval_sec_ = 0;
    TimerQueue::Id flush_timer_ = 0, reconcile_timer_ = 0, sync_timer_ = 0;
    BackgroundTaskGroup bg_{"usage"};
};

}  // namespace lights3::s3
