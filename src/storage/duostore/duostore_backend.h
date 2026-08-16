// L3: DuoStore backend facade (docs/duostore-backend.md): S3 semantics, ETag/MD5,
// pump loops; metadata/data split into the two pluggable interfaces IMetaStore /
// IDataStore, with DataRef as the single coupling point.
// P1: RocksDB meta + chunk data path; P2: pack aggregation (threshold routing +
// liveness accounting + discard-on-restart); P3: GC phase one (gcq consumption +
// pin counting + mpu_ttl + background worker); compaction/orphan scan (P4)
// introduced later.
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/background.h"
#include "core/metrics.h"
#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "core/timer.h"
#include "storage/backend.h"
#include "storage/duostore/data_store.h"
#include "storage/duostore/meta_dump.h"
#include "storage/duostore/meta_store.h"

namespace lights3::storage {

namespace duostore {

// In-process pin counting (main doc §7): open_reader registers every file_id the
// ref involves, reader destruction removes them; GC skips files with pin>0 this
// round, preventing the lazy-open vs unlink ENOENT. Single-process exclusive
// ownership of root is an existing precondition, so in-process counting is fully
// correct; shared_ptr is shared with readers — the ObjectStream escapes the
// backend's lifetime along with the HTTP response, so the pin table must not
// depend on the backend staying alive
struct PinTable {
    // The pin key is (is-pack, file_id): chunk/rados share an id segment while
    // packs use an independent counter — the two spaces necessarily overlap in
    // values, and a single raw-id table would misread "some chunk is being read"
    // as "the same-numbered pack is pinned" (and vice versa), making GC
    // conservatively defer reclamation. Split tables have no such false positives
    struct Handle {
        bool is_pack = false;
        uint64_t file_id = 0;
    };

    // Counted once per extent (multiple extents of the same file_id count multiple
    // times); pin/unpin are symmetric over the same handle list
    std::vector<Handle> pin(std::span<const Extent> extents);
    void unpin(const std::vector<Handle>& handles);
    // Single-id variants (write-side pin, §9.3): ChunkWriter pins on allocation,
    // released after the meta commit/discard. The write side only produces
    // chunk/rados (pack appends allocate no independent file), so always in the
    // chunk space
    void pin_id(uint64_t file_id);
    void unpin_id(uint64_t file_id);
    bool any_pinned(std::span<const Extent> extents);
    bool pinned_chunk(uint64_t file_id);  // orphan scan (chunk/rados entities)
    bool pinned_pack(uint64_t pack_id);   // whole empty-pack deletion

private:
    // file_id hash sharding: the GET hot path's pin/unpin and GC's bulk
    // any_pinned do not contend for one lock
    static constexpr size_t kShards = 16;
    struct Shard {
        std::mutex m;
        std::unordered_map<uint64_t, int> chunk_refs;
        std::unordered_map<uint64_t, int> pack_refs;
    };
    Shard& shard_of(uint64_t id) { return shards_[id % kShards]; }
    std::array<Shard, kShards> shards_;

    void pin_key(bool is_pack, uint64_t id);
    void unpin_key(bool is_pack, uint64_t id);
    bool pinned_key(bool is_pack, uint64_t id);
};

// Reclamation statistics for run_gc_once() (§9.1; for test assertions and later metric wiring)
struct DuoGcStats {
    uint64_t reclaims_acked = 0;    // gcq entries settled
    uint64_t files_removed = 0;     // chunk/rados extents physically deleted (pack records excluded)
    uint64_t skipped_grace = 0;     // gcq entries skipped for not yet exceeding gc_grace
    uint64_t skipped_pinned = 0;    // gcq entries skipped because an involved file was pinned
    uint64_t packs_removed = 0;     // empty packs deleted whole (sealed with live_recs==0)
    uint64_t uploads_expired = 0;   // multiparts internally aborted after mpu_ttl expiry
    uint64_t packs_sealed_aged = 0; // active packs sealed by aging (§6.1)
    uint64_t packs_compacted = 0;   // low-liveness packs sequentially scanned (rewrite_pack) this round (P4 §9.2)
    uint64_t packs_compact_deferred = 0;  // packs eligible but squeezed out by this round's budget (§6.1)
    uint64_t records_migrated = 0;  // records whose refs were successfully swapped by compaction migration
    uint64_t records_corrupt = 0;   // corrupt records detected by the compaction scan (skipped + warned, not deleted)
};

// Reconciliation statistics for run_orphan_scan_once() (§9.3)
struct DuoOrphanStats {
    uint64_t chunks_scanned = 0;   // chunk entities enumerated on the data plane
    uint64_t orphans_removed = 0;  // orphans unreferenced and beyond grace → unlinked
    uint64_t skipped_grace = 0;    // unreferenced but mtime not yet beyond gc_grace (suspected in-flight write)
    uint64_t skipped_pinned = 0;   // unreferenced but pinned (write-side pin / in-flight reader)
    uint64_t refs_missing = 0;     // reverse: refs present but file missing (sign of data loss; warn only, never delete meta)
    // Reverse reconciliation of packs/ (docs/gaps.md §6.1)
    uint64_t packs_scanned = 0;         // pack files enumerated on disk
    uint64_t orphan_packs_removed = 0;  // unaccounted pack files (crash after file creation, before the first record committed)
    uint64_t packs_skipped_active = 0;  // unaccounted but lock-held by a live writer / within grace / pinned
    uint64_t pack_stats_missing = 0;    // reverse: packstat present but file missing (sign of data loss)
    uint64_t chunk_bytes = 0;           // total bytes of on-disk chunk entities (usage metric)
    uint64_t pack_bytes = 0;            // total bytes of on-disk pack files (usage metric)
};

// Standard implementation of PackMigrateFn (§9.2 steps 2-3; shared by the cfg
// constructor and test-injection assembly): owner reverse-lookup of liveness
// (objects "b\0k" looked up directly; mpu "mpu\0b\0k\0id\0no" uses b/k as a hint
// to find the owning object after complete — the owner is only a hint, the
// liveness criterion is always "current DataRef contains from" + the swap's
// version guard; a stale hint only conservatively skips migration, never deletes
// wrongly). Batch form (gaps §2.13): the whole batch is aggregated by owner —
// multiple records of one object do a single get_object + a single ref swap
// (eliminating O(n²) manifest rewrites); live payloads are appended once via
// data.write_batch (single fdatasync in the fs implementation); ref swaps go
// through meta.swap_extents_batch (single-transaction commit on local engines).
// If a racing overwrite fails the swap, appended chunk-kind residue is cleaned up
// (pack-kind becomes a dead region reclaimed by compaction). Returns the number
// of successfully migrated records. pins may be null; it must share provenance
// with data's ChunkPinHooks (symmetric unpin when appends take the chunk path)
Task<uint64_t> migrate_pack_records(IMetaStore& meta, IDataStore& data, PinTable* pins,
                                    std::vector<PackScanRecord> batch);

}  // namespace duostore

// meta engine selection (docs/duostore-redis-meta.md §8 / docs/duostore-sqlite-meta.md §8 /
// docs/duostore-tikv-meta.md §9): redis / sqlite / tikv require the corresponding
// compile-time option, otherwise from_params throws "not compiled in"
enum class DuoMetaKind { kRocksDb, kRedis, kSqlite, kTikv };

// data engine selection (docs/duostore-rados-data.md §10, dual of meta_kind):
// rados requires the compile-time option LIGHTS3_DUOSTORE_RADOS_DATA
enum class DuoDataKind { kFs, kRados };

struct DuoStoreConfig {
    std::string name;
    std::filesystem::path root;       // required; meta/ chunks/ packs/ all live underneath
    std::filesystem::path meta_path;  // default <root>/meta (may point separately to SSD)
    DuoMetaKind meta_kind = DuoMetaKind::kRocksDb;
    std::string redis_uri;                // required when meta=redis
    std::string redis_prefix = "duo:";    // key prefix (multi-instance/test isolation)
    int redis_timeout_sec = 3;            // connect + per-command timeout
    int redis_pool_size = 8;              // connection pool size
    int redis_wait_replicas = 0;          // replicas to WAIT for after commit (0 = no wait)
    std::filesystem::path sqlite_path;    // meta=sqlite: DB file, default <root>/meta.sqlite3
    size_t sqlite_cache = 64ull << 20;    // page cache (PRAGMA cache_size)
    std::vector<std::string> pd_endpoints;  // required when meta=tikv (docs/duostore-tikv-meta.md §9)
    std::string tikv_prefix = "duo:";       // key prefix (multi-instance/test isolation)
    std::string tikv_ca;                    // mTLS triple (enabled only when all three are given)
    std::string tikv_cert;
    std::string tikv_key;
    int tikv_backoff_ms = 0;                // sidecar-path backoff budget (0 = client-c library default)
    int tikv_gc_interval_sec = 60;          // GC safepoint advance period (0 = off, §7.3)
    int tikv_gc_retention_sec = 600;        // safepoint retention window (now − retention)
    DuoDataKind data_kind = DuoDataKind::kFs;
    std::string rados_conf = "/etc/ceph/ceph.conf";  // data=rados keys (docs/duostore-rados-data.md §10)
    std::string rados_client = "client.admin";
    std::string rados_pool;                          // required when data=rados
    std::string rados_namespace;                     // logical isolation within the pool (multi-instance/tests)
    uint64_t rados_chunk_size = 8ull << 20;
    uint64_t rados_buffer_total = 256ull << 20;
    int rados_connect_timeout_sec = 5;
    int rados_op_timeout_sec = 0;                    // 0 = no op timeout
    uint64_t chunk_size = 8ull << 20;
    uint64_t pack_threshold = 128 << 10;   // ≤ this goes into packs; 0 = disabled (everything via chunks)
    uint64_t pack_max_size = 128ull << 20;
    int pack_writers = 4;
    // Age-based sealing of active packs (docs/gaps.md §6.1): with capacity-only
    // sealing under low write volume a pack never rotates, and its dead regions
    // never enter the compaction candidate set. 0 = disabled
    int pack_max_age_sec = 3600;
    double pack_gc_ratio = 0.5;            // effective with P4 compaction
    // Per-round compaction budget (docs/gaps.md §6.1): candidates sorted by
    // reclaimable bytes descending, take the top N / cumulative file_size at most
    // max_bytes. Without a budget, "one GC round rewriting every eligible pack
    // after a bulk delete" can hold the lock for hours; with one, the
    // highest-yield packs go first and the rest continue next round. 0 = unlimited
    int gc_compact_max_packs = 16;
    uint64_t gc_compact_max_bytes = 1ull << 30;
    // Multi-gateway deployment (docs/duostore-rados-data.md §8.3): GC/orphan scan
    // must run on a single instance; set false on non-designated gateways (no
    // background worker scheduled; the manual hooks remain for tests/ops).
    // Concurrent GC over shared meta/data would step on itself (duplicate
    // compaction/scans), and another gateway's GC cannot see this gateway's
    // in-process pin table — single instance + gc_grace ≥ the longest expected
    // GET duration is the initial deployment constraint
    bool gc_enabled = true;
    int gc_interval_sec = 300;
    int gc_grace_sec = 300;
    int orphan_scan_interval_sec = 86400;  // effective with P4
    int mpu_ttl_sec = 7 * 86400;
    bool meta_sync = true;
    bool verify_chunk_crc = false;
    size_t rocksdb_block_cache = 64ull << 20;
    // RocksDB tuning exposed (P5, §11); defaults = RocksDB's own defaults, behavior unchanged
    size_t rocksdb_write_buffer = 64ull << 20;
    int rocksdb_max_write_buffers = 2;
    int rocksdb_max_background_jobs = 2;

    // Centralized parsing + range validation (docs/duostore-backend.md §11);
    // configuration errors throw std::runtime_error
    static DuoStoreConfig from_params(const std::string& name,
                                      const std::map<std::string, std::string>& params);
};

class DuoStoreBackend final : public IStorageBackend {
public:
    // metrics defaults to an empty scope: tests construct directly without
    // assembly, counters land in an isolated instance
    DuoStoreBackend(DuoStoreConfig cfg, std::shared_ptr<ThreadPool> pool,
                    MetricsScope metrics = {});
    // For test injection: self-assembled meta/data. Note cfg.data_kind must match
    // the injected data engine — it determines the extent kind the orphan scan
    // unlinks (the rados engine only accepts kRados)
    DuoStoreBackend(DuoStoreConfig cfg, std::shared_ptr<ThreadPool> pool,
                    std::unique_ptr<duostore::IMetaStore> meta,
                    std::unique_ptr<duostore::IDataStore> data, MetricsScope metrics = {});
    ~DuoStoreBackend() override;

    Task<void> create_bucket(std::string_view bucket) override;
    Task<void> delete_bucket(std::string_view bucket) override;
    Task<bool> bucket_exists(std::string_view bucket) override;
    Task<std::vector<BucketInfo>> list_buckets() override;

    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta,
                               http::BodyReader& body,
                               PutCondition cond = {}) override;
    Task<ObjectMeta> head_object(std::string_view bucket, std::string_view key) override;
    Task<void> delete_object(std::string_view bucket, std::string_view key) override;
    Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) override;

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

    Task<void> close() override;

    // Manual GC hook (§9: direct test invocation, following the tiered
    // "manually trigger demotion" precedent): mpu_ttl expiry cleanup + gcq
    // consumption (grace/pin filtering, physical delete before settling) + pack
    // compaction (P4 §9.2) + whole empty-pack deletion (unlink only after being
    // empty beyond gc_grace, serving in-flight readers). Mutually exclusive with
    // the background worker / orphan scan via an internal semaphore; safe to call
    // concurrently
    Task<duostore::DuoGcStats> run_gc_once();
    // Manual orphan-scan hook (P4 §9.3): forward (on-disk chunk unreferenced,
    // beyond gc_grace, unpinned → unlink) + reverse (refs present but file
    // missing → warn and count, never delete meta). Shares the semaphore with GC
    // — the reverse reconciliation's "file must exist before its ref" argument
    // depends on the gcq's unlink→settle window not running concurrently
    Task<duostore::DuoOrphanStats> run_orphan_scan_once();

    // meta backup/restore and cross-engine migration (docs/gaps.md §6.1; stream
    // format and ops contract in meta_dump.h). Both hold the same semaphore as
    // GC/orphan scan; write quiescence is guaranteed by ops (main's
    // --duostore_admin entry runs before the server starts, so naturally no
    // business traffic). After load finishes, a round of orphan scan is forced —
    // reclaiming files the data side accumulated during the backup window
    Task<duostore::MetaDumpStats> run_meta_dump(std::ostream& out);
    Task<duostore::MetaDumpStats> run_meta_load(std::istream& in);

    // Direct data-plane access (tests only): verifies the active pack's write-lock
    // probing (docs/gaps.md §1.4)
    duostore::IDataStore& data_for_test() { return *data_; }

private:
    void require_bucket(std::string_view bucket);  // called on a pool thread
    // Fetch the object record; on absence distinguish NoSuchBucket / NoSuchKey
    // (GET/HEAD error semantics must agree)
    duostore::ObjectRec require_object(std::string_view bucket, std::string_view key);
    // Discard active packs on restart (§5.2): catch-up seal the accounting of
    // unsealed packs left by the previous generation (called at construction)
    void abandon_stale_packs();

    // Background GC management (§9 lifecycle): BackgroundTaskGroup wait group +
    // a single worker tick re-armed after completion; close/dtor share
    // shutdown_background (begin_close → cancel timers outside the lock
    // [TimerQueue::cancel blocks on in-flight callbacks] → wait for in-flight GC
    // to reach zero)
    void schedule_gc();
    Task<void> gc_tick();
    void schedule_orphan_scan();  // independent low-frequency timer (orphan_scan_interval; 0 = off)
    Task<void> orphan_tick();
    void shutdown_background();
    // GC counter metric registration (shared by both constructors)
    void init_metrics(const MetricsScope& metrics);

    DuoStoreConfig cfg_;
    std::shared_ptr<ThreadPool> pool_;
    std::unique_ptr<duostore::IMetaStore> meta_;
    std::unique_ptr<duostore::IDataStore> data_;
    std::shared_ptr<duostore::PinTable> pins_ = std::make_shared<duostore::PinTable>();
    // Write-side pins assembled (set when the cfg constructor injects
    // ChunkPinHooks into FsDataStore): the put path must symmetrically unpin
    // after commit/discard. The injection constructor has no hooks by default —
    // blind unpinning would wrongly decrement concurrent readers' pins
    bool write_pins_ = false;
    std::atomic<bool> closed_{false};  // close() idempotence latch; in-flight checks go through bg_

    // Accumulated once at the end of a completed GC round (run_gc_once's
    // DuoGcStats → monotonic counters)
    std::shared_ptr<MetricCounter> m_gc_runs_, m_gc_reclaims_, m_gc_files_removed_,
        m_gc_packs_removed_, m_gc_uploads_expired_, m_gc_packs_compacted_,
        m_gc_packs_sealed_aged_, m_gc_records_migrated_, m_gc_records_corrupt_, m_orphan_runs_,
        m_orphan_removed_, m_orphan_packs_removed_;
    // Reclamation source buckets (§6.1): index = ReclaimReason
    std::array<std::shared_ptr<MetricCounter>, 6> m_gc_reclaims_by_reason_;
    std::shared_ptr<MetricGauge> m_gc_compact_deferred_, m_gcq_depth_, m_gcq_oldest_age_,
        m_gc_skipped_grace_, m_gc_skipped_pinned_, m_bytes_chunks_, m_bytes_packs_,
        m_pack_accounted_bytes_, m_pack_live_bytes_, m_packs_total_,
        m_orphan_packstats_missing_;
    std::shared_ptr<MetricHistogram> m_gc_duration_;
    // GET read-path crc mismatch counter (P5 corruption metric): data-plane
    // readers increment via the on_corruption callback — the callback captures
    // only this shared_ptr, so readers escaping the backend's lifetime stay safe
    std::shared_ptr<MetricCounter> m_read_corruption_;
    std::shared_ptr<MetricGauge> m_orphan_refs_missing_;  // files missing in the latest reverse reconciliation round

    // GC per-round bookkeeping (read/written only while holding gc_sem_, so
    // lock-free):
    // pack_empty_since_ = when an empty pack was first seen (§9.2 step 4 delayed
    // unlink: delete whole only after being empty beyond gc_grace, serving
    // readers who held old refs at the instant of compaction/deletion without
    // having pinned yet; in-process readers make in-process timing correct, and
    // a restart resetting it is merely conservative). compact_blocked_ = packs the
    // previous round's compaction could not fully migrate (in-flight mpu parts /
    // legacy-format owner / live corrupt records): if the live account is
    // unchanged and the cooldown window (gc_grace) has not passed, skip
    // rescanning — any account progress retries immediately; the cooldown covers
    // "account unchanged but attributability changed" cases (complete_upload's
    // refs transfer does not touch the pack account)
    struct CompactBlocked {
        int64_t live_recs = 0;
        int64_t retry_at_ms = 0;
    };
    std::unordered_map<uint64_t, int64_t> pack_empty_since_;
    std::unordered_map<uint64_t, CompactBlocked> compact_blocked_;
    // gcq scan watermark (gaps §2.13: rescanning all unreclaimable entries from
    // seq 0 every round costs CPU that grows linearly with backlog and never
    // shrinks): records the earliest seq among skipped entries and the earliest
    // retry time. In rounds where no skipped entry has reached its retry time,
    // scan straight from the previous high watermark (only new enqueued entries),
    // avoiding repeated peek+decode of the backlog at the queue head. For
    // pinned/delete-failure skips the retry time is "this round's time" (full
    // rescan next round — when a pin releases is unknowable, the conservative
    // direction); grace skips use enqueue+grace (a deterministic lower bound).
    // Pure in-memory optimization: a restart resetting it merely falls back to a
    // full scan, correctness unaffected
    struct GcqSkips {
        bool any = false;
        uint64_t lo_seq = 0;       // seq of the earliest skipped entry (valid when any)
        int64_t retry_at_ms = 0;   // earliest retry time among skipped entries
    };
    GcqSkips gcq_skips_;
    uint64_t gcq_hi_ = 0;          // high watermark of scanned seqs (next unseen seq)

    // Instance identity for the multi-gateway GC lease (§6.1): randomly generated
    // in-process; a restart simply gets a new one — the old lease yields via TTL
    // expiry. Lease TTL is max(2×gc_interval, 10min), far above a single round's
    // duration
    std::string gc_owner_;
    BackgroundTaskGroup bg_{"duostore"};
    // Written only inside bg_.if_open, unchanged after begin_close (readers are
    // lock-free); 0 = not armed (cancel(0) is safe)
    TimerQueue::Id gc_timer_ = 0;
    TimerQueue::Id orphan_timer_ = 0;
    // Manual hooks, the background worker, and the orphan scan are mutually
    // exclusive (std::mutex cannot be held across co_await, so a coroutine semaphore)
    AsyncSemaphore gc_sem_{1};
};

}  // namespace lights3::storage
