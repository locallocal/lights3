// L3: SQLite implementation of IMetaStore (docs/duostore-sqlite-meta.md).
// Commit-class operations = a single SQL transaction (BEGIN IMMEDIATE + RAII guard,
// §3.2); compound invariants are read-checked-written inside the transaction's
// isolation domain, with zero window. One in-process std::mutex serializes writers
// (single write connection, §3.1/§3.4). Pure reads go through a read connection pool
// and run in parallel with the writer under WAL mode. Value encoding reuses codec.cc
// 100% (§2.1); single-process exclusive — multi-process shared meta is a non-goal (§1).
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "core/metrics.h"
#include "storage/duostore/meta_store.h"

namespace lights3::storage::duostore {

struct SqliteMetaOptions {
    std::string path;                  // DB file path (single-file deployment, §1)
    bool sync = true;                  // durable commits: synchronous FULL/NORMAL (§6)
    size_t cache_bytes = 64ull << 20;  // page cache capacity (PRAGMA cache_size, §8)
    int pool_size = 8;                 // read connection pool cap (§3.1)
    int busy_timeout_ms = 5000;        // busy handler wait (§5.2; not in YAML, tests may shorten)
    MetricsScope metrics;              // BUSY / corruption counters (S4; empty scope = isolated instance)
};

class SqliteMetaStore final : public IMetaStore {
public:
    explicit SqliteMetaStore(SqliteMetaOptions opt);
    ~SqliteMetaStore() override;
    SqliteMetaStore(const SqliteMetaStore&) = delete;

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
    void ack_reclaims(std::span<const uint64_t> seqs) override;  // one txn, one fsync (§3.3)
    std::vector<PackStat> pack_stats() override;
    void seal_pack(uint64_t pack_id, uint64_t file_size) override;
    void drop_pack_stat(uint64_t pack_id) override;
    bool swap_extents(std::string_view b, std::string_view k, uint64_t expect_version,
                      const DataRef& from, const DataRef& to) override;
    // One txn, one fsync (batched compaction, gaps §2.13); per-item CAS is independent
    std::vector<bool> swap_extents_batch(std::span<const SwapReq> reqs) override;
    bool chunk_referenced(uint64_t file_id) override;
    void scan_refs(const std::function<void(uint64_t file_id)>& cb) override;
    void close() override;

    // Test-only (§9 S4 consistent-view case): each list_objects call invokes this hook
    // once after emitting the first entry — the hook commits concurrently from another
    // connection, validating the list read transaction's WAL snapshot
    void set_list_pause_for_test(std::function<void()> hook) {
        list_pause_for_test_ = std::move(hook);
    }

private:
    struct Conn;  // sqlite3* + resident prepared-statement cache (defined in the .cc; header leaks no sqlite3 types)
    class Stmt;

    // Shared body of close(): graceful=false is for constructor-failure cleanup — it
    // only releases connections and the file lock, skipping the optimize/checkpoint
    // wrap-up (the db never opened cleanly, so the wrap-up would inevitably fail and
    // double-count corruption)
    void shutdown(bool graceful);
    class Txn;

    // RAII lease of a read connection (§3.1): destructor returns it to the pool;
    // returning after close destroys it. Special members are defined in the .cc
    // (unique_ptr<Conn> needs the complete type)
    struct Lease {
        SqliteMetaStore* store = nullptr;
        std::unique_ptr<Conn> conn;
        Lease(SqliteMetaStore* s, std::unique_ptr<Conn> c);
        Lease(Lease&&) noexcept;
        ~Lease();
        Conn& operator*() const;
    };

    // Id-segment reservation (§4, isomorphic to RocksMetaStore): counters bumped by
    // +kIdSegment at a time, dispensed from memory. gcq seq does not use segments —
    // the AUTOINCREMENT rowid is allocated with the business transaction, naturally
    // transactional (§2.2)
    struct IdRange {
        uint64_t next = 0, limit = 0;
    };
    static constexpr uint64_t kIdSegment = 4096;

    std::unique_ptr<Conn> open_raw();      // open + busy_timeout only (no file writes before lineage check)
    void apply_pragmas(Conn& c, bool full_sync);
    std::unique_ptr<Conn> open_conn(bool full_sync);  // open_raw + apply_pragmas
    // Lineage check (§2.2): runs before any write (including the WAL journal
    // conversion) — app_id/ver both 0 but sqlite_master non-empty = someone else's
    // database; refuse without leaving a trace
    void check_lineage(Conn& c);
    void migrate_schema(Conn& c, int64_t ver);  // migration chain for version < current (called by check_lineage)
    void init_schema(Conn& c);
    Lease read_conn();                     // take from pool; throws InternalError after close
    void release(std::unique_ptr<Conn> c);
    Conn& wconn();                         // write connection; mu_ must be held; throws InternalError after close

    void require_bucket(Conn& c, std::string_view b);
    std::optional<std::string> object_raw(Conn& c, std::string_view b, std::string_view k);
    UploadRec require_upload_in(Conn& c, std::string_view b, std::string_view k,
                                std::string_view id);
    // Maintain refs (chunk reference table) in the same batch: add=write owner, else delete
    void write_refs(Conn& c, const DataRef& ref, bool add, std::string_view owner);
    // Maintain pack liveness accounting in the same batch (arithmetic UPDATE on the
    // pack_stats table, §2.2). Independent of write_refs: complete's refs transfer
    // (owner rewrite) must be a no-op for packs — mixing them would double-count.
    // rec_overhead: per-record header overhead (codec::pack_rec_overhead*); live_bytes
    // uses the same accounting basis as file_size (docs/archive/gaps.md §2.3a)
    void write_pack_delta(Conn& c, const DataRef& ref, int sign, int64_t rec_overhead);
    // Single-item CAS core of swap (mu_ held, runs inside the caller's transaction):
    // on successful validation writes and returns true; on mismatch returns false
    // without writing. Shared by swap_extents / swap_extents_batch
    bool apply_swap(Conn& c, std::string_view b, std::string_view k, uint64_t expect_version,
                    const DataRef& from, const DataRef& to);
    // gcq bookkeeping: seq is allocated by AUTOINCREMENT with the transaction,
    // committing/rolling back in the same batch as the business write
    void enqueue_reclaim(Conn& c, const DataRef& ref, ReclaimReason reason);
    std::vector<PartRec> scan_parts(Conn& c, std::string_view b, std::string_view k,
                                    std::string_view id);
    uint64_t alloc_id(std::string_view counter, IdRange& r, uint32_t n = 1);

    SqliteMetaOptions opt_;
    // Fail-fast enforcement of single-process exclusivity (§1; counterpart of
    // RocksDB's LOCK file): flock(LOCK_EX|LOCK_NB) on <path>.lock, acquired in the
    // constructor, released by close()
    int lock_fd_ = -1;

    // One mutex serializes all commit-class operations (transactions on wc_ always
    // run inside mu_, §3.4). The commit (including the WAL fsync when sync=true)
    // completes inside the lock, so write throughput caps at ≈ 1/fsync latency —
    // same trade-off as the RocksDB version (accepted at P1; no group commit)
    std::mutex mu_;
    std::unique_ptr<Conn> wc_;  // dedicated write connection (BEGIN IMMEDIATE txns always on it)
    // Dedicated id-segment connection, always synchronous=FULL (independent of
    // opt_.sync, §4); alloc_mu_ protects the IdRange and this connection. Lock order
    // alloc_mu_ → mu_ (mu_ is held during reservation to keep business writers out,
    // docs/archive/gaps.md §3.9); alloc is called by the data plane outside business
    // transactions, so no reverse nesting
    std::mutex alloc_mu_;
    std::unique_ptr<Conn> ac_;
    IdRange file_ids_[2];  // indexed by Extent::Kind

    std::mutex pool_mu_;
    std::vector<std::unique_ptr<Conn>> idle_;
    bool closed_ = false;

    // S4 metrics (registered at construction, visible at value 0); connections hold
    // shared_ptr copies and increment them on error paths
    std::shared_ptr<MetricCounter> m_busy_;
    std::shared_ptr<MetricCounter> m_corrupt_;

    std::function<void()> list_pause_for_test_;
};

}  // namespace lights3::storage::duostore
