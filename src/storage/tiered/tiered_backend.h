// L3: tiered-storage composite backend (docs/tiered-storage.md)
// Composes a local hot side behind tier::ITierLocal (localfs/xlocalfs or duostore) with a
// cloud side (any IStorageBackend): cold objects are uploaded to the cloud and stubbed
// locally, then transparently read back on access and cached back to local.
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
#include "storage/tiered/tier_local.h"

namespace lights3::storage {

// Prefix-level policy (roadmap §3.6 ②): glob over "bucket/key" (fnmatch, '*' crosses
// '/'), first match wins; cold_after_sec < 0 pins the objects (never demoted or evicted)
struct TierRule {
    std::string glob;
    int64_t cold_after_sec = 0;
};

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

    // ---- roadmap §3.6 ----
    // ① Incremental scanning: a full enumeration of the local side only every
    // full_scan_interval (enrolls untracked objects, recalibrates the quota books, does
    // crash recovery); rounds in between consume the time wheel. 0 = every round is full
    int64_t full_scan_interval_sec = 86400;
    std::vector<TierRule> rules;  // ② prefix policies, first match wins
    // ③ Eviction score = age × (1 + size_weight × log2(1 + size/1MiB)) / (1 + frequency_weight × hits);
    // both 0 = pure LRU (the previous behavior)
    double evict_size_weight = 0.0;
    double evict_frequency_weight = 0.0;
    // ⑤ Write-behind buffer of access records (flushed every 5 min or when this many keys
    // are pending); persisted per object by the local side, not resident
    size_t access_buffer_max = 100000;
    // ⑦ Block-level cache for Range GETs of remote objects (local side permitting)
    bool range_cache = false;
    uint64_t range_cache_block = 1 << 20;
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
    // Quarantine ledger movement this round (roadmap §3.6 ④)
    uint64_t quarantined_new = 0;       // findings seen for the first time (logged at WARN/ERROR)
    uint64_t quarantined_resolved = 0;  // ledger entries whose finding disappeared
};

// One scan round's report (roadmap §3.6 ①)
struct TierScanStats {
    bool full = false;             // full enumeration vs time-wheel round
    uint64_t walked = 0;           // objects enumerated (full) or wheel candidates verified (incremental)
    uint64_t cold_picked = 0;      // coldness demotions launched
    uint64_t recovered = 0;        // half-done stubs finished
    uint64_t enrolled = 0;         // wheel enrollments written
    uint64_t stale = 0;            // wheel entries whose object is gone
    uint64_t evicted = 0;          // watermark victims launched
    uint64_t evicted_bytes = 0;
    uint64_t need_remaining = 0;   // bytes the watermark could not cover
};

// Quarantine ledger entry (roadmap §3.6 ④): a reconciliation finding that is repeated
// every round until an operator acts on it
struct QuarantineEntry {
    std::string kind;    // refs_missing | foreign
    std::string bucket, key, etag;
    int64_t first_seen = 0, last_seen = 0;
    uint64_t count = 0;
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

    TieredBackend(std::shared_ptr<tier::ITierLocal> local, std::shared_ptr<IStorageBackend> cloud,
                  std::shared_ptr<ThreadPool> pool, TieredConfig cfg, MetricsScope metrics = {});
    // Convenience: wrap a localfs/xlocalfs backend in its adapter
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
    Task<void> set_object_tagging(std::string_view bucket, std::string_view key,
                                  std::string tagging) override;
    Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) override;

    // ---- multipart: fully delegated to local; when complete overwrites an old cloud replica it goes to GC ----
    Task<std::string> create_multipart(std::string_view bucket, std::string_view key,
                                       ObjectMeta meta) override;
    using IStorageBackend::upload_part;
    Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                std::string_view upload_id, int part_no, http::BodyReader& body,
                                const std::optional<PartChecksum>& checksum) override;
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

    // Stop background timers, wait for in-flight background coroutines, persist buffered
    // access records. Does not close the cloud backend (held independently by the
    // registry and possibly routed to directly at the same time)
    Task<void> close() override;

    // ---- Background tasks and test hooks (docs/tiered-storage.md §10 P1 "manual trigger") ----
    // Single-object demotion: local -> remote (upload + stubbing) or cached -> remote
    // (zero-traffic stubbing). Returns silently when preconditions fail (already remote /
    // task in flight); if beaten by a concurrent write, the cloud replica goes to GC
    Task<void> demote_object(std::string bucket, std::string key);
    // Single-object whole promotion: remote -> cached (cloud GET -> fill -> verify ->
    // commit); called in the background when a Range GET hits remote (single-flight)
    Task<void> promote_object(std::string bucket, std::string key);
    // One scan round: coldness detection (full enumeration or time-wheel round, see
    // full_scan_interval) + space-watermark eviction + crash recovery + access flush
    Task<TierScanStats> scan_once();
    // Persist the write-behind access buffer now (records + wheel enrollment); the 5-min
    // timer and every scan round do the same
    Task<void> flush_access();
    // Consume one round of the GC queue: delete orphan cloud replicas (verify etag, never
    // delete a live replica); failed entries are rescheduled with exponential backoff
    // (attempts/retry_at persisted in the entry TSV, not reset by restart)
    Task<TierGcStats> run_gc_once();
    // Bidirectional reconciliation (docs/tiered-storage.md §9, low-frequency, daily by
    // default): cloud has it, local does not -> rebuild the stub from lights3-* redundant
    // headers (default) or delete (reconcile_delete_orphans); local remote, cloud missing
    // -> warn, never delete the stub. GC queue snapshot + inflight guards prevent
    // misjudgment (a replica pending deletion is not an orphan, an in-flight demotion is
    // not an orphan -- rebuilding would resurrect a just-DELETEd object). Repeated
    // findings go to the quarantine ledger and stop re-alerting (④)
    Task<TierReconcileStats> run_reconcile_once();

    // ---- Quarantine ledger (roadmap §3.6 ④; CLI `lights3 tier quarantine`) ----
    std::vector<QuarantineEntry> quarantine_list() const;
    // Drop an entry without touching data (the operator judged it benign / fixed it by hand)
    bool quarantine_forget(std::string_view bucket, std::string_view key);
    // Operator resolution of a refs_missing finding: re-verify the cloud copy is still
    // gone, then delete the dead local stub (acknowledged data loss). false = not
    // quarantined as refs_missing, or the cloud copy is back (entry dropped instead)
    Task<bool> quarantine_purge(std::string bucket, std::string key);

    const TieredConfig& config() const { return cfg_; }
    tier::ITierLocal& local() { return *local_; }
    // Effective coldness threshold for an object: first matching rule, else cold_after;
    // -1 = pinned
    int64_t cold_after_for(std::string_view bucket, std::string_view key) const;

private:
    friend class TeeCacheReader;
    friend class RangeTeeReader;
    friend struct InflightRelease;

    // ---- Data-plane accounting (docs/archive/gaps.md §7): measure only the four ops where tiered
    // itself has tiering logic; purely delegated multipart/head etc. are covered by
    // local_'s own metrics ----
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

    // ---- Access records + time wheel (docs/tiered-storage.md §4.3 / §5.1) ----
    struct Touch {
        int64_t atime = 0;
        uint32_t hits = 0;  // touches since the last flush (merged into the stored count)
    };
    void touch(std::string_view bucket, std::string_view key);
    void forget_access(std::string_view bucket, std::string_view key);
    // Merged view: pending touch over the stored record over the mtime fallback
    tier::AccessRec access_of(std::string_view bucket, std::string_view key, int64_t fallback_mtime);
    // Store the record and (re)enroll the key in the wheel slot its deadline falls in;
    // returns true when a wheel line was appended
    bool persist_access(std::string_view bucket, std::string_view key, tier::AccessRec rec);
    void flush_access_sync();
    static constexpr int64_t kWheelSlotSec = 3600;
    int64_t wheel_slot_for(int64_t atime, int64_t cold_after) const {
        return (atime + cold_after) / kWheelSlotSec;
    }
    void wheel_append(int64_t slot, std::string_view bucket, std::string_view key);
    std::vector<std::pair<int64_t, std::filesystem::path>> wheel_slots() const;  // ascending
    void maybe_kick_flush();

    // ---- Scan internals ----
    struct ScanCtx;
    Task<void> scan_full(ScanCtx& cx);
    Task<void> scan_incremental(ScanCtx& cx);
    Task<void> scan_evict(ScanCtx& cx, uint64_t need);
    Task<void> drain_batch(ScanCtx& cx);
    // Verify one candidate (shared by the incremental round and eviction): reads the
    // object, applies rules, decides cold/recovery/enroll
    // from_slot: the wheel slot being consumed (-1 = full enumeration); a key that stays
    // hot but was enrolled in that very slot is re-appended, since the file goes away
    Task<void> consider(ScanCtx& cx, const std::string& bucket, const std::string& key,
                        int64_t from_slot);
    double evict_score(const tier::AccessRec& a, uint64_t size, int64_t now) const;

    // ---- GC queue (docs/tiered-storage.md §7.2): <state>/gc/<seq>, one TSV per entry ----
    void enqueue_gc(std::string_view bucket, std::string_view key, std::string_view remote_etag);

    // ---- Quarantine ledger (④): <state>/quarantine/<md5(kind|bucket|key)> ----
    std::filesystem::path quarantine_path(std::string_view kind, std::string_view bucket,
                                          std::string_view key) const;
    // Record a finding; returns true when it is new (caller logs loudly only then)
    bool quarantine_note(std::string_view kind, std::string_view bucket, std::string_view key,
                         std::string_view etag, std::set<std::string>& seen,
                         TierReconcileStats& st);
    void quarantine_sweep(const std::set<std::string>& seen, TierReconcileStats& st);
    void refresh_quarantine_gauges();

    // Lazy cloud-side bucket creation (AlreadyOwned under concurrency counts as success)
    Task<void> ensure_cloud_bucket(std::string_view bucket);
    // Cache-fill commit: re-verify under the per-key lock that the object is still the
    // same remote version, then commit the fill as cached
    Task<void> commit_cache_fill(std::string bucket, std::string key, ObjectMeta expect,
                                 tier::TierInfo expect_tier, tier::ICacheFill& fill);
    // statvfs headroom precheck (docs/tiered-storage.md §6.2 step 2)
    bool cache_space_ok(uint64_t size) const { return local_->cache_space_ok(size, cfg_.min_free_bytes); }

    // Background coroutine management: core/background.h wait group (spawn counting +
    // close() waits for zero)
    void schedule_scan();
    void schedule_flush();
    void schedule_reconcile();  // independent low-frequency timer (reconcile_interval; 0 = off)

    Task<void> demote_quiet(std::string bucket, std::string key);
    Task<void> promote_quiet(std::string bucket, std::string key);
    Task<void> scan_and_gc();
    // Incremental quota maintenance (docs/archive/gaps.md §6.3 / docs/tiered-storage.md):
    // PUT/DELETE adjust the estimate in place and kick an early scan round past the
    // watermark; the full scan recalibrates against measured values
    void note_local_delta(int64_t delta);
    void maybe_kick_quota_scan();
    Task<void> flush_task();
    Task<void> reconcile_task();
    // Reconciliation's orphan handling (executed after re-verification under the per-key
    // lock); reports whether the cloud/local side was touched
    Task<void> reconcile_orphan(std::string bucket, std::string key, std::string cloud_etag,
                                bool local_is_live, TierReconcileStats& st,
                                std::set<std::string>& seen);
    // Reverse adjudication: when a local remote/cached reference is missing in the cloud or
    // its etag mismatches, re-verify with a HEAD at the current point before warning
    Task<void> reconcile_ref_missing(std::string bucket, std::string key, tier::TierInfo t,
                                     TierReconcileStats& st, std::set<std::string>& seen);

    std::shared_ptr<tier::ITierLocal> local_;
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
    std::shared_ptr<MetricCounter> m_scan_full_, m_scan_incr_, m_evicted_bytes_, m_access_flushed_;
    std::shared_ptr<MetricCounter> m_rcache_hit_, m_rcache_fill_, m_rcache_pass_;
    std::shared_ptr<MetricGauge> m_q_refs_missing_, m_q_foreign_;
    std::filesystem::path tier_dir_;        // local_->state_dir()
    std::filesystem::path gc_dir_;          // <state>/gc
    std::filesystem::path wheel_dir_;       // <state>/wheel
    std::filesystem::path quarantine_dir_;  // <state>/quarantine

    // Semaphores uniformly take the pool executor: release posts the waiter's continuation
    // back to a pool thread, eradicating the path where "in-place resume pins blocking IO
    // on the HTTP response thread" (docs/archive/gaps.md §2.4)
    ThreadPoolExecutor pool_exec_{*pool_};
    std::vector<std::unique_ptr<AsyncSemaphore>> key_locks_;
    AsyncSemaphore transfers_;  // max_concurrent_transfers throttle (docs/tiered-storage.md §5.1)

    std::mutex inflight_m_;
    std::set<std::string> inflight_;

    std::mutex access_m_;
    std::unordered_map<std::string, Touch> access_dirty_;  // ikey -> pending touch
    std::atomic<bool> flush_inflight_{false};
    std::mutex wheel_m_;
    int64_t last_full_scan_ = 0;  // 0 = never (the first round after startup is full)
    mutable std::mutex quarantine_m_;

    std::atomic<uint64_t> gc_seq_{0};
    std::atomic<int64_t> local_bytes_est_{-1};      // -1 = not yet calibrated by scan
    std::atomic<bool> quota_kick_inflight_{false};  // only one early-kicked scan at a time

    BackgroundTaskGroup bg_{"tiered"};
    // Timer ids are written only inside bg_.if_open; unchanged after begin_close (readers
    // need no lock). 0 when not armed: TimerQueue ids start at 1, cancel(0) is a safe no-op
    TimerQueue::Id scan_timer_ = 0;
    TimerQueue::Id flush_timer_ = 0;      // access flush period (§4.3, fixed 5 min)
    TimerQueue::Id reconcile_timer_ = 0;  // reconciliation period (§9, default 1d)
};

}  // namespace lights3::storage
