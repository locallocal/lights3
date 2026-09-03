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
#include "storage/meta_cache.h"

namespace lights3::storage {

namespace duostore {

// Object metadata cache entry (roadmap §3.8): the full record when a GET filled it
// (manifest included, so a hit skips the meta engine entirely), or meta only when a
// HEAD did (head_object decodes just the header -- materializing a 650k-extent
// manifest for a HEAD would be the regression docs/archive/gaps.md §3.9 removed). A GET
// treats a meta-only entry as a miss and upgrades it
struct CachedObject {
    ObjectRec rec;
    bool manifest = false;
};
using ObjectRecCache = MetaCache<CachedObject>;
// Records above this many extents are served but never cached (a large object's meta
// RTT is amortized over its transfer; the budget is for the small-object hot set)
inline constexpr size_t kMetaCacheMaxExtents = 256;

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

// In-flight read registry for the multi-gateway read lease (roadmap §3.7):
// get_object registers a ticket *before* fetching the manifest and the reader's
// destructor ends it, so "oldest in-flight read start" published to the shared
// meta soundly covers every reader that may hold a pre-deref manifest (the
// registration-to-meta-read window needs no grace argument — registration comes
// first). Shared with escaping readers via shared_ptr, same lifetime shape as
// PinTable
class ReadClock {
public:
    uint64_t begin();          // returns a ticket id (monotonic)
    void end(uint64_t ticket); // idempotent for unknown ids
    // Start time of the oldest in-flight read, or fallback when none (the
    // publisher passes "now": an idle gateway holds nothing back)
    int64_t oldest_or(int64_t fallback);

private:
    std::mutex m_;
    uint64_t next_ = 1;
    // Ordered by ticket id; ids and start times are both monotonic, so begin()
    // of the first entry is the oldest start
    std::map<uint64_t, int64_t> active_;
};

// Reclamation statistics for run_gc_once() (§9.1; test assertions, and folded
// into the lights3_duostore_gc_* counters/gauges at the end of every completed
// round — see init_metrics)
struct DuoGcStats {
    uint64_t reclaims_acked = 0;    // gcq entries settled
    uint64_t files_removed = 0;     // chunk/rados extents physically deleted (pack records excluded)
    uint64_t skipped_grace = 0;     // gcq entries skipped for not yet exceeding gc_grace
    uint64_t skipped_pinned = 0;    // gcq entries skipped because an involved file was pinned
    uint64_t skipped_leased = 0;    // gcq entries deferred by a peer gateway's read lease (roadmap §3.7)
    uint64_t packs_removed = 0;     // empty packs deleted whole (sealed with live_recs==0)
    uint64_t uploads_expired = 0;   // multiparts internally aborted after mpu_ttl expiry
    uint64_t packs_sealed_aged = 0; // active packs sealed by aging (§6.1)
    uint64_t packs_compacted = 0;   // low-liveness packs sequentially scanned (rewrite_pack) this round (P4 §9.2)
    uint64_t packs_compact_deferred = 0;  // packs eligible but squeezed out by this round's budget (§6.1)
    uint64_t records_migrated = 0;  // records whose refs were successfully swapped by compaction migration
    uint64_t records_corrupt = 0;   // corrupt records detected by the compaction scan (skipped + warned, not deleted)
    uint64_t packs_quarantined = 0; // packs moved to the corruption quarantine this round (roadmap §3.7)
};

// Corrupt-pack quarantine entry (roadmap §3.7): a pack whose compaction made no
// progress for kQuarantineStrikes consecutive scans while corrupt records were
// the only remaining explanation is parked here — no more cooldown rescans until
// an operator releases it (`lights3 duostore quarantine release`) or its live
// account moves (objects deleted → auto-release / whole deletion). Persisted as
// one small file per pack under <root>/quarantine so restarts and the admin CLI
// (a separate process) see the same ledger
struct DuoQuarantineEntry {
    uint64_t pack_id = 0;
    int64_t live_recs = 0;        // live account at quarantine time (auto-release trigger baseline)
    uint64_t corrupt_records = 0; // corrupt records the last scan counted
    int64_t quarantined_ms = 0;   // unix ms of quarantine entry
    bool purged = false;          // pack file removed by `quarantine purge` (accounting kept until live drains)
};

// Reconciliation statistics for run_orphan_scan_once() (§9.3)
struct DuoOrphanStats {
    uint64_t chunks_scanned = 0;   // chunk entities enumerated on the data plane
    uint64_t orphans_removed = 0;  // orphans unreferenced and beyond grace → unlinked
    uint64_t skipped_grace = 0;    // unreferenced but mtime not yet beyond gc_grace (suspected in-flight write)
    uint64_t skipped_pinned = 0;   // unreferenced but pinned (write-side pin / in-flight reader)
    uint64_t refs_missing = 0;     // reverse: refs present but file missing (sign of data loss; warn only, never delete meta)
    // Reverse reconciliation of packs/ (docs/archive/gaps.md §6.1)
    uint64_t packs_scanned = 0;         // pack files enumerated on disk
    uint64_t orphan_packs_removed = 0;  // unaccounted pack files (crash after file creation, before the first record committed)
    uint64_t packs_skipped_active = 0;  // unaccounted but lock-held by a live writer / within grace / pinned
    uint64_t pack_stats_missing = 0;    // reverse: packstat present but file missing (sign of data loss)
    uint64_t chunk_bytes = 0;           // total bytes of on-disk chunk entities (usage metric)
    uint64_t pack_bytes = 0;            // total bytes of on-disk pack files (usage metric)
    uint64_t skipped_gcq = 0;           // unreferenced chunks left to the gcq path (pending entry exists;
                                        // closes the cross-gateway reader race, roadmap §3.7)
};

// run_scrub_once knobs (roadmap §3.1). Rate limiting is per-call rather than
// config: a scrub is an operator-invoked traversal (CLI), not a resident worker
struct DuoScrubOptions {
    uint64_t max_bytes_per_sec = 0;  // 0 = unthrottled
};

// Integrity report of run_scrub_once() (roadmap §3.1). Read-only: the scrub
// mutates nothing, every finding is a log line plus a counter here.
// corrupt/unreadable/refs_missing are the "data is in danger" signals;
// refs_stale is a space-leak suspect that can also be a transient artifact of
// an MPU completing mid-scrub (re-run to confirm)
struct DuoScrubStats {
    uint64_t objects_scanned = 0;     // committed objects fully walked
    uint64_t parts_scanned = 0;       // in-flight multipart parts walked
    uint64_t extents_checked = 0;
    uint64_t bytes_read = 0;
    uint64_t corrupt_extents = 0;     // read back fine but crc32c != manifest
    uint64_t unreadable_extents = 0;  // open/read failed or short (includes pack-side crc aborts)
    uint64_t objects_bad = 0;         // objects/parts with >= 1 corrupt or unreadable extent
    uint64_t refs_missing = 0;        // manifest references a chunk id absent from the refs
                                      // ledger — the orphan scan could unlink live data
    uint64_t refs_stale = 0;          // refs entry no object/part manifest references (leak suspect)
    uint64_t meta_errors = 0;         // bucket enumerations that failed and were skipped
    bool aborted = false;             // backend close interrupted the scrub (stats are partial)
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
    // io_uring fs data plane (roadmap §3.4 ⑤, data=fs only): chunk/pack byte transfers
    // and durability syncs go through the shared UringEngine; opt-in, and on engine
    // setup failure (old kernel, seccomp, memlock quota) the backend falls back to the
    // synchronous path with a warning plus a resident gauge
    bool fs_uring = false;
    unsigned fs_uring_queue_depth = 256;
    bool fs_uring_sqpoll = false;
    unsigned fs_uring_rings = 1;  // 0 = auto (hardware threads / 8, clamped to [1,8])
    uint64_t pack_threshold = 128 << 10;   // ≤ this goes into packs; 0 = disabled (everything via chunks)
    uint64_t pack_max_size = 128ull << 20;
    int pack_writers = 4;
    // Age-based sealing of active packs (docs/archive/gaps.md §6.1): with capacity-only
    // sealing under low write volume a pack never rotates, and its dead regions
    // never enter the compaction candidate set. 0 = disabled
    int pack_max_age_sec = 3600;
    double pack_gc_ratio = 0.5;            // effective with P4 compaction
    // Per-round compaction budget (docs/archive/gaps.md §6.1): candidates sorted by
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
    // compaction/scans). Peer gateways' in-flight reads are covered by the read
    // lease below (roadmap §3.7) on shared meta engines; gc_grace remains the
    // fallback when the lease is off or the engine is local
    bool gc_enabled = true;
    int gc_interval_sec = 300;
    int gc_grace_sec = 300;
    // Multi-gateway read lease publish period (roadmap §3.7): every gateway
    // (gc_enabled or not) publishes "oldest in-flight read started at" to shared
    // meta engines (redis/tikv); the GC gateway defers reclaiming anything a
    // peer's in-flight read could still reference. 0 = off; local engines
    // (rocksdb/sqlite) report unsupported and the publisher stands down, so the
    // default costs nothing there. Keep well below gc_grace (staleness of the
    // published value adds to reclaim latency, never to risk)
    int read_lease_sec = 5;
    // Object metadata cache (roadmap §3.8; docs/storage/duostore-core.md §7.1): budget
    // in objects, 0 = off. Exact on local engines (rocksdb/sqlite: every write to the
    // meta goes through this process and invalidates). On shared engines (redis/tikv)
    // a peer gateway's write is invisible until the entry expires, so a TTL is
    // mandatory there and must stay below gc_grace; the read-lease publisher also
    // backdates its "oldest in-flight read" by the TTL so a peer's GC cannot reclaim
    // extents a cached manifest may still name. from_params defaults the budget to 0
    // on shared engines unless configured; the constructor refuses a TTL-less cache
    // on a shared engine
    size_t meta_cache_entries = size_t(1) << 16;
    int meta_cache_ttl_sec = 0;  // 0 = no expiry
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

    // Deep integrity scrub (roadmap §3.1): meta-driven — every committed object
    // and in-flight multipart part has its full manifest read back from the data
    // plane with crc32c recomputed per extent (independently of verify_chunk_crc,
    // which only guards the GET hot path), plus a two-way reconciliation of the
    // chunk/rados refs ledger against what the manifests actually reference.
    // Strictly read-only; findings are logged and counted, never repaired.
    // Holds the same semaphore as GC (our own GC cannot unlink mid-read; business
    // deletes only enqueue to the gcq while it is held) and renews the GC lease
    // best-effort so a peer gateway's GC defers. Designed for the offline
    // `lights3 fsck` entry; safe to run against a live instance at the cost of
    // GC standing still for the duration
    Task<duostore::DuoScrubStats> run_scrub_once(duostore::DuoScrubOptions opt = {});

    // meta backup/restore and cross-engine migration (docs/archive/gaps.md §6.1; stream
    // format and ops contract in meta_dump.h). Both hold the same semaphore as
    // GC/orphan scan; write quiescence is guaranteed by ops (main's
    // --duostore_admin entry runs before the server starts, so naturally no
    // business traffic). After load finishes, a round of orphan scan is forced —
    // reclaiming files the data side accumulated during the backup window
    Task<duostore::MetaDumpStats> run_meta_dump(std::ostream& out);
    Task<duostore::MetaDumpStats> run_meta_load(std::istream& in);

    // ---- Corrupt-pack quarantine (roadmap §3.7; CLI `lights3 duostore quarantine`) ----
    std::vector<duostore::DuoQuarantineEntry> quarantine_list();
    // Drop the ledger entry: compaction retries from scratch next round (use after
    // restoring the pack file from a backup, or to force one more scan)
    bool quarantine_release(uint64_t pack_id);
    // Physically remove the quarantined pack file, accepting the loss of its
    // remaining (corrupt) records — reads of objects still referencing them turn
    // from crc aborts into missing-extent errors. The liveness accounting is kept:
    // once the owning objects are deleted and live drains to zero, the regular GC
    // round drops the packstat and the ledger entry (remove_pack is idempotent).
    // Refuses while the pack is pinned by an in-flight reader. Returns false when
    // the pack is not quarantined or already purged
    Task<bool> quarantine_purge(uint64_t pack_id);

    // Direct data-plane access (tests only): verifies the active pack's write-lock
    // probing (docs/archive/gaps.md §1.4)
    duostore::IDataStore& data_for_test() { return *data_; }
    // Object metadata cache observability (roadmap §3.8; tests + docs)
    MetaCacheStats meta_cache_stats() const { return meta_cache_->stats(); }
    bool meta_cache_enabled() const { return meta_cache_->enabled(); }

    // ---- Tiered local-side hooks (roadmap §3.6 ⑥; used by tier::DuoStoreTierLocal) ----
    // Full record incl. tier state; nullopt when bucket or object is absent (sync, pool thread)
    std::optional<duostore::ObjectRec> tier_read(std::string_view bucket, std::string_view key);
    // Stub commit: rewrite the record with no extents and tier=remote, CAS on meta.etag
    // (the old extents enter the gcq in the same meta transaction — that *is* the local
    // space reclamation). Throws PreconditionFailed when beaten by a concurrent write
    Task<void> tier_commit_stub(std::string_view bucket, std::string_view key,
                                const ObjectMeta& meta, const duostore::TierState& ts);
    // Cache-fill commit: pump body into the data plane, then rewrite the record with the
    // new extents and tier=cached, CAS on meta.etag; the produced extents are discarded
    // when the commit fails (same shape as PUT)
    Task<void> tier_commit_cached(std::string_view bucket, std::string_view key,
                                  http::BodyReader& body, const ObjectMeta& meta,
                                  const duostore::TierState& ts);
    const std::filesystem::path& root() const { return cfg_.root; }
    const std::shared_ptr<ThreadPool>& pool() const { return pool_; }

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
    // Read-lease publisher (roadmap §3.7): periodic timer on every gateway; the
    // tick stands down permanently when the engine reports leases unsupported
    // (local engines — in-process pins are already exact there)
    void schedule_read_lease();
    Task<void> lease_tick();
    // One manifest's worth of scrub work (run_scrub_once): refs-ledger presence
    // per chunk/rados extent + full read-back with crc recomputation. refetch
    // re-reads the current manifest to separate a genuine refs hole from a
    // concurrent overwrite/delete; returns whether any extent failed
    Task<bool> scrub_manifest(const duostore::DataRef& ref, const std::string& what,
                              const std::vector<uint64_t>& refs_snapshot,
                              std::vector<bool>& ref_seen,
                              const std::function<std::optional<duostore::DataRef>()>& refetch,
                              class ScrubThrottle& throttle, std::vector<std::byte>& buf,
                              duostore::DuoScrubStats& st);
    void shutdown_background();
    // GC counter metric registration (shared by both constructors)
    void init_metrics(const MetricsScope& metrics);

    DuoStoreConfig cfg_;
    std::shared_ptr<ThreadPool> pool_;
    std::unique_ptr<duostore::IMetaStore> meta_;
    std::unique_ptr<duostore::IDataStore> data_;
    std::shared_ptr<duostore::PinTable> pins_ = std::make_shared<duostore::PinTable>();
    // Object metadata cache (roadmap §3.8): filled by GET/HEAD, dropped by every
    // record-changing path of this process (put/delete/complete/tier commits), cleared
    // whole after a compaction round that swapped refs and after a meta restore
    std::shared_ptr<duostore::ObjectRecCache> meta_cache_;
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
        m_gc_skipped_grace_, m_gc_skipped_pinned_, m_gc_skipped_leased_, m_bytes_chunks_,
        m_bytes_packs_, m_pack_accounted_bytes_, m_pack_live_bytes_, m_packs_total_,
        m_packs_quarantined_, m_orphan_packstats_missing_;
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
        // Consecutive scans with corrupt records and zero migration progress
        // (roadmap §3.7): at kQuarantineStrikes the pack is quarantined instead
        // of cooling down again. Any account movement resets the count
        int strikes = 0;
    };
    static constexpr int kQuarantineStrikes = 3;
    std::unordered_map<uint64_t, int64_t> pack_empty_since_;
    std::unordered_map<uint64_t, CompactBlocked> compact_blocked_;

    // ---- Corrupt-pack quarantine ledger (roadmap §3.7) ----
    // In-memory mirror of <root>/quarantine/<pack_id> files (loaded at
    // construction). q_mu_ orders GC-round mutation against the admin CLI's
    // list/release (purge additionally holds gc_sem_ for the physical removal)
    std::filesystem::path quarantine_dir() const;
    void load_quarantine();
    void quarantine_save(const duostore::DuoQuarantineEntry& e);  // write-through (logs on failure)
    void quarantine_drop(uint64_t pack_id);                       // erase entry + file
    std::mutex q_mu_;
    std::map<uint64_t, duostore::DuoQuarantineEntry> quarantined_;
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
    // In-flight read registry for the read lease (roadmap §3.7); shared with
    // escaping readers like pins_
    std::shared_ptr<duostore::ReadClock> read_clock_ =
        std::make_shared<duostore::ReadClock>();
    BackgroundTaskGroup bg_{"duostore"};
    // Written only inside bg_.if_open, unchanged after begin_close (readers are
    // lock-free); 0 = not armed (cancel(0) is safe)
    TimerQueue::Id gc_timer_ = 0;
    TimerQueue::Id orphan_timer_ = 0;
    TimerQueue::Id lease_timer_ = 0;
    // Manual hooks, the background worker, and the orphan scan are mutually
    // exclusive (std::mutex cannot be held across co_await, so a coroutine semaphore)
    AsyncSemaphore gc_sem_{1};
};

}  // namespace lights3::storage
