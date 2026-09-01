// L3: RocksDB implementation of IMetaStore (docs/duostore-backend.md §4).
// Commit-type operations = a single WriteBatch (§4.5); compound cross-key
// invariants are serialized with one std::mutex, while pure reads (get/list, via
// snapshot) take no lock.
#pragma once

#include <atomic>
#include <cstdint>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "core/metrics.h"
#include "storage/duostore/meta_store.h"

namespace rocksdb {
class DB;
class ColumnFamilyHandle;
class WriteBatch;
}  // namespace rocksdb

namespace lights3::storage::duostore {

struct RocksMetaOptions {
    std::string path;
    bool sync = true;                        // whether commits WAL-fsync (§6.3 meta_sync)
    size_t block_cache_bytes = 64ull << 20;
    // Tuning knobs exposed (P5, docs/duostore-backend.md §11); defaults = RocksDB's
    // own defaults, so existing deployments keep their behavior. Compression is
    // always off (§13.3) and not exposed
    size_t write_buffer_bytes = 64ull << 20;  // memtable capacity per CF
    int max_write_buffers = 2;                // max memtable count per CF
    int max_background_jobs = 2;              // total flush/compaction background threads
    MetricsScope metrics;  // empty scope = isolated instance (tests construct directly with zero wiring, docs/archive/gaps.md §6.1)
};

class RocksMetaStore final : public IMetaStore {
public:
    // Current schema version (existing DBs are upgraded via the migration chain on
    // open, docs/archive/gaps.md §6.1)
    static constexpr int64_t kSchemaCurrent = 1;
    // Schema marker validity check (pure precondition of migrate_schema; static for
    // easy unit testing): parse failure, or a version newer than this build (running
    // downgraded would silently corrupt writes), both throw InternalError; returns
    // the stored version number
    static int64_t validate_schema_marker(const std::string& stored);

    explicit RocksMetaStore(RocksMetaOptions opt);
    ~RocksMetaStore() override;
    RocksMetaStore(const RocksMetaStore&) = delete;

    void create_bucket(std::string_view b) override;
    void delete_bucket(std::string_view b) override;
    bool bucket_exists(std::string_view b) override;
    std::vector<BucketInfo> list_buckets() override;

    std::optional<ObjectRec> get_object(std::string_view b, std::string_view k) override;
    std::optional<ObjectMeta> head_object(std::string_view b, std::string_view k) override;
    void put_object(std::string_view b, std::string_view k, ObjectRec rec,
                    PutCondition cond = {}) override;
    bool delete_object(std::string_view b, std::string_view k) override;
    ListResult list_objects(std::string_view b, const ListOptions& opt) override;

    std::string create_upload(std::string_view b, std::string_view k, ObjectMeta meta) override;
    UploadRec require_upload(std::string_view b, std::string_view k,
                             std::string_view id) override;
    void put_part(std::string_view b, std::string_view k, std::string_view id,
                  PartRec p) override;
    std::vector<PartRec> list_parts(std::string_view b, std::string_view k,
                                    std::string_view id) override;
    std::vector<UploadInfo> list_uploads(std::string_view b, std::string_view key_marker = {},
                                         std::string_view id_marker = {}, int limit = 0,
                                         std::string_view prefix = {}) override;
    std::string complete_upload(std::string_view b, std::string_view k, std::string_view id,
                                std::span<const PartInfo> parts) override;
    void abort_upload(std::string_view b, std::string_view k, std::string_view id) override;

    uint64_t alloc_file_run(Extent::Kind kind, uint32_t n) override;
    std::vector<std::pair<uint64_t, Reclaim>> peek_reclaims(size_t max, uint64_t min_seq = 0,
                                                            size_t max_extents = SIZE_MAX) override;
    void ack_reclaim(uint64_t seq) override;
    void ack_reclaims(std::span<const uint64_t> seqs) override;  // single WriteBatch
    std::vector<PackStat> pack_stats() override;
    void seal_pack(uint64_t pack_id, uint64_t file_size) override;
    void drop_pack_stat(uint64_t pack_id) override;
    bool swap_extents(std::string_view b, std::string_view k, uint64_t expect_version,
                      const DataRef& from, const DataRef& to) override;
    // Single WriteBatch, single commit (batched compaction, gaps §2.13); per-item CAS is independent
    std::vector<bool> swap_extents_batch(std::span<const SwapReq> reqs) override;
    bool chunk_referenced(uint64_t file_id) override;
    void scan_refs(const std::function<void(uint64_t file_id)>& cb) override;
    void close() override;

private:
    // CF indices (table in §4.1)
    enum Cf { kDefault = 0, kBuckets, kObjects, kUploads, kParts, kRefs, kGcq, kStats, kNumCf };

    // Id segment reservation (§4.5): one merge of +kIdSegment on the stats counter,
    // then in-memory dispatch
    struct IdRange {
        uint64_t next = 0, limit = 0;
    };
    static constexpr uint64_t kIdSegment = 4096;

    // Throws InternalError after close (db_ is null) — defense in depth, turning
    // misuse into a 500 instead of a segfault; the contract is still that close
    // must be called after in-flight requests finish (§9 lifecycle)
    rocksdb::DB* db() const;
    std::optional<std::string> get_raw(int cf, std::string_view key);
    void commit(rocksdb::WriteBatch& batch);
    void require_bucket_locked(std::string_view b);
    std::vector<PartRec> scan_parts(std::string_view b, std::string_view k,
                                    std::string_view id);
    uint64_t alloc_id(std::string_view counter_key, IdRange& r, uint32_t n = 1);
    void enqueue_reclaim_locked(rocksdb::WriteBatch& batch, const DataRef& ref,
                                ReclaimReason reason);
    void migrate_schema(const std::string& stored);  // version check + migration chain (called by ctor)
    // Maintain refs (chunk reference table, §4.1) in the same batch: add = write the
    // owner, otherwise delete
    void batch_refs(rocksdb::WriteBatch& batch, const DataRef& ref, bool add,
                    std::string_view owner);
    // Maintain the pack liveness ledger (incremental merge on the stats CF, §9.1)
    // in the same batch. Separate from batch_refs: complete's refs transfer (owner
    // rewrite) must be a no-op for packs, and mixing them would double-count.
    // rec_overhead: per-record header overhead (codec::pack_rec_overhead*);
    // live_bytes uses the same accounting basis as file_size (docs/archive/gaps.md §2.3a)
    void batch_pack_delta(rocksdb::WriteBatch& batch, const DataRef& ref, int sign,
                          int64_t rec_overhead);
    // Single-item CAS core of swap (called holding mu_): on successful validation it
    // appends the whole mutation set to the batch and returns true; on mismatch it
    // returns false without touching the batch. Shared by swap_extents /
    // swap_extents_batch
    bool stage_swap_locked(rocksdb::WriteBatch& batch, std::string_view b, std::string_view k,
                          uint64_t expect_version, const DataRef& from, const DataRef& to);

    RocksMetaOptions opt_;
    std::atomic<rocksdb::DB*> db_{nullptr};
    std::vector<rocksdb::ColumnFamilyHandle*> cfs_;
    // One mutex serializes all commit-type operations. Note: the commit (including
    // the WAL fsync when meta_sync=true) runs inside the lock, so write-path
    // throughput caps at ~1/fsync-latency and RocksDB group commit is defeated —
    // accepted for P1; the upgrade path when contention becomes the bottleneck is
    // TransactionDB (§4.5; not done, just noted)
    std::mutex mu_;
    // Separate small lock for id segment dispatch: alloc is called on the data
    // plane every time a chunk is opened (fs_data_store) and must not queue behind
    // business commits' WAL fsync. Lock order is always mu_ -> alloc_mu_, no cycle
    std::mutex alloc_mu_;
    IdRange file_ids_[2];  // indexed by Extent::Kind
    IdRange seqs_;         // gcq seq
};

}  // namespace lights3::storage::duostore
