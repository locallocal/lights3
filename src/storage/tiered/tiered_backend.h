// L3: tiered-storage composite backend (docs/tiered-storage.md)
// Composes local (must be localfs/xlocalfs, sharing the disk layout) with cloud (any
// IStorageBackend): cold objects are uploaded to the cloud and stubbed locally, then
// transparently read back on access and Tee-cached back to local.
#pragma once

#include <array>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/background.h"
#include "core/config.h"
#include "core/metrics.h"
#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "core/timer.h"
#include "storage/backend.h"
#include "storage/localfs/localfs_backend.h"

namespace lights3::storage {

struct TieredConfig {
    int64_t cold_after_sec = 30 * 24 * 3600;  // coldness threshold (docs/tiered-storage.md §5.1)
    int64_t scan_interval_sec = 3600;         // 0 = disable the background task (manual hook for tests)
    double space_high_watermark = 0.85;       // triggers space reclamation
    double space_low_watermark = 0.70;        // reclamation target
    uint64_t min_free_bytes = 1ull << 30;     // minimum headroom required for cache fill (requirement 3)
    bool cache_fill_on_range = true;          // background whole-object promotion when a Range hits remote
    int max_concurrent_transfers = 4;
    uint64_t quota_bytes = 0;                 // 0 = logical quota disabled
    // Exponential backoff for failed GC entries (docs/tiered-storage.md §9): delay =
    // base x 2^attempts, clamped to cap; persisted per entry (attempts/retry_at land in the
    // TSV, not reset by restart)
    int64_t gc_retry_base_sec = 60;
    int64_t gc_retry_cap_sec = 3600;
    // Reconciliation (§9): daily by default; 0 = off (manual hook for tests). Orphan
    // handling defaults to rebuilding the stub (never destroys data; delete mode saves
    // cloud storage cost but a misjudgment loses the replica)
    int64_t reconcile_interval_sec = 86400;
    bool reconcile_delete_orphans = false;
};

// Statistics for run_gc_once() (for backoff / test assertions)
struct TierGcStats {
    uint64_t resolved = 0;       // entries conclusively removed (delete succeeded / cloud never had it / live reference invalidated / corrupt)
    uint64_t removed_cloud = 0;  // orphan cloud replicas actually deleted
    uint64_t deferred = 0;       // backoff not yet due, skipped this round
    uint64_t failed = 0;         // failed this round, rescheduled with exponential backoff
};

// Bidirectional reconciliation statistics for run_reconcile_once() (docs/tiered-storage.md §9)
struct TierReconcileStats {
    uint64_t cloud_objects = 0;    // objects visited in the cloud walk
    uint64_t stubs_rebuilt = 0;    // cloud has it, local does not -> stub rebuilt from redundant headers
    uint64_t orphans_deleted = 0;  // cloud orphans deleted (delete mode, or stale replicas at the local `local` tier)
    uint64_t orphans_skipped = 0;  // undecidable (no lights3 redundant headers / etag ambiguity) -> warn and skip
    uint64_t refs_missing = 0;     // local remote, cloud missing -> warn (data loss; never delete the stub)
};

class TieredBackend final : public IStorageBackend,
                            public std::enable_shared_from_this<TieredBackend> {
public:
    // StorageRegistry two-phase construction entry: local/cloud in params reference
    // already-built backends by name
    static std::shared_ptr<TieredBackend> from_config(
        const BackendConfig& cfg,
        const std::map<std::string, std::shared_ptr<IStorageBackend>>& built,
        std::shared_ptr<ThreadPool> pool, MetricsScope metrics = {});

    TieredBackend(std::shared_ptr<LocalFsBackend> local, std::shared_ptr<IStorageBackend> cloud,
                  std::shared_ptr<ThreadPool> pool, TieredConfig cfg, MetricsScope metrics = {});
    ~TieredBackend() override;

    // ---- bucket: fully delegated to local (the cloud-side bucket is created lazily on first demotion) ----
    Task<void> create_bucket(std::string_view bucket) override;
    Task<void> delete_bucket(std::string_view bucket) override;
    Task<bool> bucket_exists(std::string_view bucket) override;
    Task<std::vector<BucketInfo>> list_buckets() override;

    // ---- object: tier-aware (docs/tiered-storage.md §6/§7) ----
    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta,
                               http::BodyReader& body,
                               PutCondition cond = {}) override;
    Task<ObjectMeta> head_object(std::string_view bucket, std::string_view key) override;
    Task<void> delete_object(std::string_view bucket, std::string_view key) override;
    Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) override;

    // ---- multipart: fully delegated to local; when complete overwrites an old cloud replica it goes to GC ----
    Task<std::string> create_multipart(std::string_view bucket, std::string_view key,
                                       ObjectMeta meta) override;
    Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                std::string_view upload_id, int part_no,
                                http::BodyReader& body) override;
    Task<PutResult> complete_multipart(std::string_view bucket, std::string_view key,
                                       std::string_view upload_id,
                                       std::span<const PartInfo> parts) override;
    Task<void> abort_multipart(std::string_view bucket, std::string_view key,
                               std::string_view upload_id) override;
    Task<ListPartsResult> list_parts(std::string_view bucket, std::string_view key,
                                     std::string_view upload_id,
                                     const ListPartsOptions& opt) override;
    Task<ListUploadsResult> list_multipart_uploads(std::string_view bucket,
                                                   const ListUploadsOptions& opt) override;

    // Stop background timers, wait for in-flight background coroutines, persist the atime
    // snapshot. Does not close the child backends (they are held independently by the
    // registry and may be routed to directly at the same time)
    Task<void> close() override;

    // ---- Background tasks and test hooks (docs/tiered-storage.md §10 P1 "manual trigger") ----
    // Single-object demotion: local -> remote (upload + stubbing) or cached -> remote
    // (zero-traffic stubbing). Returns silently when preconditions fail (already remote /
    // task in flight); if beaten by a concurrent write, the cloud replica goes to GC
    Task<void> demote_object(std::string bucket, std::string key);
    // Single-object whole promotion: remote -> cached (cloud GET -> staging -> verify ->
    // commit); called in the background when a Range GET hits remote (single-flight)
    Task<void> promote_object(std::string bucket, std::string key);
    // One scan round: coldness detection + space-watermark reclamation + crash recovery
    // (remote but data not reclaimed) + atime snapshot
    Task<void> scan_once();
    // Consume one round of the GC queue: delete orphan cloud replicas (verify etag, never
    // delete a live replica); failed entries are rescheduled with exponential backoff
    // (attempts/retry_at persisted in the entry TSV, not reset by restart)
    Task<TierGcStats> run_gc_once();
    // Bidirectional reconciliation (docs/tiered-storage.md §9, low-frequency, daily by
    // default): cloud has it, local does not -> rebuild the stub from lights3-* redundant
    // headers (default) or delete (reconcile_delete_orphans); local remote, cloud missing
    // -> warn, never delete the stub. GC queue snapshot + inflight guards prevent
    // misjudgment (a replica pending deletion is not an orphan, an in-flight demotion is
    // not an orphan -- rebuilding would resurrect a just-DELETEd object)
    Task<TierReconcileStats> run_reconcile_once();

    const TieredConfig& config() const { return cfg_; }

private:
    friend class TeeCacheReader;
    friend struct InflightRelease;

    // ---- Data-plane accounting (docs/gaps.md §7): measure only the four ops where tiered
    // itself has tiering logic; purely delegated multipart/head etc. are covered by
    // local_'s lights3_localfs_* ----
    enum class Op : size_t { kGet, kPut, kDelete, kList };
    static constexpr size_t kOpCount = 4;
    void init_metrics(const MetricsScope& metrics);
    void record_op(Op op, bool ok);
    // RAII within the coroutine frame (like localfs's OpGuard): accounts even during
    // exception unwinding, ok defaults to false
    struct OpGuard {
        TieredBackend* self;
        Op op;
        bool ok = false;
        ~OpGuard() { self->record_op(op, ok); }
    };

    // ---- per-key locks (docs/tiered-storage.md §7.3): striped async mutexes, protecting only the state-commit section ----
    static constexpr size_t kLockStripes = 64;
    AsyncSemaphore& key_lock(std::string_view bucket, std::string_view key);

    // ---- In-flight table: single-flight cache fill + protects demotion uploads from erroneous GC deletion ----
    bool inflight_try_begin(const std::string& ikey);
    void inflight_end(const std::string& ikey);
    bool inflight_contains(const std::string& ikey);
    static std::string make_ikey(std::string_view bucket, std::string_view key) {
        return std::string(bucket) + "/" + std::string(key);
    }

    // ---- TierIndex: atime table (docs/tiered-storage.md §4.3) ----
    void touch_atime(std::string_view bucket, std::string_view key);
    void erase_atime(std::string_view bucket, std::string_view key);
    int64_t atime_or(const std::string& ikey, int64_t fallback);
    void load_atime_snapshot();
    void save_atime_snapshot();

    // ---- GC queue (docs/tiered-storage.md §7.2): <staging>/tier/gc/<seq>, one TSV per entry ----
    void enqueue_gc(std::string_view bucket, std::string_view key, std::string_view remote_etag);

    // Lazy cloud-side bucket creation (AlreadyOwned under concurrency counts as success)
    Task<void> ensure_cloud_bucket(std::string_view bucket);
    // Cache-fill commit: re-verify under the per-key lock that the sidecar is still the
    // same remote version, then rename+sidecar
    Task<void> commit_cache_fill(std::string bucket, std::string key, ObjectMeta expect,
                                 fsutil::TierInfo expect_tier, fsutil::TmpFile& tmp);
    // statvfs headroom precheck (docs/tiered-storage.md §6.2 step 2)
    bool cache_space_ok(uint64_t size) const;

    // Background coroutine management: core/background.h wait group (spawn counting +
    // close() waits for zero)
    void schedule_scan();
    void schedule_snapshot();
    void schedule_reconcile();  // independent low-frequency timer (reconcile_interval; 0 = off)

    Task<void> demote_quiet(std::string bucket, std::string key);
    Task<void> promote_quiet(std::string bucket, std::string key);
    Task<void> scan_and_gc();
    // Incremental quota maintenance (docs/gaps.md §6.3 / docs/tiered-storage.md):
    // PUT/DELETE adjust the estimate in place and kick an early scan round past the
    // watermark -- previously only the periodic walk accumulated, so quota overruns between
    // two scans (default 1 hour) were completely invisible. The estimate drifts with
    // demote/promote (only in the conservative direction: bytes freed by demotion stay on
    // the books), and the first pass of each scan recalibrates against measured values
    void note_local_delta(int64_t delta);
    void maybe_kick_quota_scan();
    Task<void> snapshot_task();
    Task<void> reconcile_task();
    // Reconciliation's orphan handling (executed after re-verification under the per-key
    // lock); reports whether the cloud/local side was touched
    Task<void> reconcile_orphan(std::string bucket, std::string key, std::string cloud_etag,
                                bool local_is_live, TierReconcileStats& st);
    // Reverse adjudication: when a local remote/cached reference is missing in the cloud or
    // its etag mismatches, re-verify with a HEAD at the current point before warning
    Task<void> reconcile_ref_missing(std::string bucket, std::string key, fsutil::TierInfo t,
                                     TierReconcileStats& st);

    std::shared_ptr<LocalFsBackend> local_;
    std::shared_ptr<IStorageBackend> cloud_;
    std::shared_ptr<ThreadPool> pool_;
    TieredConfig cfg_;

    // Metric instances claimed at construction (same paradigm as duostore); with an empty
    // scope they are orphan instances and calls are harmless
    std::array<std::shared_ptr<MetricCounter>, kOpCount> m_ops_, m_op_errors_;
    std::shared_ptr<MetricCounter> m_get_local_, m_get_cloud_;  // GET traffic source split
    std::shared_ptr<MetricCounter> m_demoted_, m_promoted_;
    std::shared_ptr<MetricCounter> m_gc_runs_, m_gc_removed_, m_gc_failed_;
    std::shared_ptr<MetricGauge> m_gc_deferred_;  // per-round observation (non-monotonic, see init_metrics)
    std::shared_ptr<MetricHistogram> m_scan_duration_;
    std::filesystem::path tier_dir_;  // <staging>/tier
    std::filesystem::path gc_dir_;    // <staging>/tier/gc

    // Semaphores uniformly take the pool executor: release posts the waiter's continuation
    // back to a pool thread, eradicating the path where "in-place resume pins blocking IO
    // on the HTTP response thread" (docs/gaps.md §2.4)
    ThreadPoolExecutor pool_exec_{*pool_};
    std::vector<std::unique_ptr<AsyncSemaphore>> key_locks_;
    AsyncSemaphore transfers_;  // max_concurrent_transfers throttle (docs/tiered-storage.md §5.1)

    std::mutex inflight_m_;
    std::set<std::string> inflight_;

    std::mutex atime_m_;
    std::unordered_map<std::string, int64_t> atime_;  // ikey -> epoch seconds
    // Whether the table changed since the last snapshot (docs/gaps.md §4): if unchanged,
    // do not rewrite -- idle instances no longer do a full write + fsync of the same
    // content every 5 minutes
    bool atime_dirty_ = false;

    std::atomic<uint64_t> gc_seq_{0};
    std::atomic<int64_t> local_bytes_est_{-1};      // -1 = not yet calibrated by scan
    std::atomic<bool> quota_kick_inflight_{false};  // only one early-kicked scan at a time

    BackgroundTaskGroup bg_{"tiered"};
    // Timer ids are written only inside bg_.if_open; unchanged after begin_close (readers
    // need no lock). 0 when not armed: TimerQueue ids start at 1, cancel(0) is a safe no-op
    TimerQueue::Id scan_timer_ = 0;
    TimerQueue::Id snap_timer_ = 0;       // atime snapshot period (§4.3, fixed 5 min)
    TimerQueue::Id reconcile_timer_ = 0;  // reconciliation period (§9, default 1d)
};

}  // namespace lights3::storage
