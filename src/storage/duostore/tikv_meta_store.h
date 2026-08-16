// L3: TiKV implementation of IMetaStore (docs/duostore-tikv-meta.md).
// Committing operations = snapshot read (start_ts) + mutation batch assembled in C++ +
// optimistic 2PC commit (tikv_client sidecar); WriteConflict → take a new ts, re-read
// and retry (§4.1). Write skew on read-only preconditions (bucket existence, emptiness
// check, upload existence) is materialized via guard-shard Op::Lock (§4.3). 2PC
// atomicity holds across arbitrary processes — multiple gateways sharing the same PD
// share the meta, with no business-level mutex held (§4.5, only the small id-segment
// lock). Value encoding is 100% reused from codec.cc (§3.1); durability = raft
// majority, so meta_sync is meaningless (§7.1).
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include "core/metrics.h"
#include "storage/duostore/meta_store.h"
#include "storage/duostore/tikv_client.h"

namespace lights3::storage::duostore {

struct TikvMetaOptions {
    std::vector<std::string> pd_endpoints;  // required when meta=tikv
    std::string prefix = "duo:";            // prefix for all keys (multi-instance/test isolation, §3.1)
    // mTLS triple (optional; enabled only when all three are given, §9)
    std::string ca_path;
    std::string cert_path;
    std::string key_path;
    // Parameterized backoff budget (§6.1, T5): >0 overrides the library default on sidecar paths; 0 = library default
    int backoff_budget_ms = 0;
    // GC safepoint advancement (§7.3, T5): a hard requirement for long-running pure-KV
    // clusters. interval=0 disables it (directly-constructed tests / deployments shared
    // with TiDB); when enabled, a background worker advances the safepoint to
    // now − retention every interval seconds (two steps: service safepoint declaration +
    // cluster safepoint push; concurrent advancement by multiple gateways naturally
    // converges via PD's min/monotonic semantics)
    int gc_safepoint_interval_s = 0;
    int gc_retention_s = 600;  // retention window: only needs to cover the longest list/transaction duration (§7.3)
    MetricsScope metrics;  // conflict retry / safepoint counters (T5; empty scope means isolated instances)
};

class TikvMetaStore final : public IMetaStore {
public:
    // Current schema version (marker = "t" + version; existing stores are upgraded
    // along the migration chain at open, docs/gaps.md §6.1)
    static constexpr int64_t kSchemaCurrent = 1;

    explicit TikvMetaStore(TikvMetaOptions opt);
    ~TikvMetaStore() override;
    TikvMetaStore(const TikvMetaStore&) = delete;

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
    std::vector<UploadInfo> list_uploads(std::string_view b, std::string_view key_marker,
                                         std::string_view id_marker, int limit) override;
    std::string complete_upload(std::string_view b, std::string_view k, std::string_view id,
                                std::span<const PartInfo> parts) override;
    void abort_upload(std::string_view b, std::string_view k, std::string_view id) override;

    uint64_t alloc_file_run(Extent::Kind kind, uint32_t n) override;
    std::vector<std::pair<uint64_t, Reclaim>> peek_reclaims(size_t max, uint64_t min_seq = 0,
                                                            size_t max_extents = SIZE_MAX) override;
    void ack_reclaim(uint64_t seq) override;
    bool try_gc_lease(std::string_view owner, int64_t ttl_ms) override;
    void ack_reclaims(std::span<const uint64_t> seqs) override;  // batch write-off in one transaction
    std::vector<PackStat> pack_stats() override;
    void seal_pack(uint64_t pack_id, uint64_t file_size) override;
    void drop_pack_stat(uint64_t pack_id) override;
    bool swap_extents(std::string_view b, std::string_view k, uint64_t expect_version,
                      const DataRef& from, const DataRef& to) override;
    bool chunk_referenced(uint64_t file_id) override;
    void scan_refs(const std::function<void(uint64_t file_id)>& cb) override;
    void close() override;

    // Single round of GC safepoint advancement (§7.3; called by the background worker
    // each tick, and directly by tests): declare the service safepoint at
    // now − retention and get back the min across all services → push the cluster
    // safepoint with that min. Returns the cluster safepoint after the push (TSO
    // format); on failure throws a pingcap exception (the worker catches and counts
    // it, retrying next round)
    uint64_t update_gc_safepoint_once();

private:
    void migrate_schema(int64_t ver);  // migration chain for version < current (called by open schema)

    // Id-segment reservation (§5, isomorphic to the RocksDB/Redis versions): a small
    // RMW transaction on the counter key adds +kIdSegment at a time, then dispatches
    // from memory. A TiKV commit is raft-durable, so no burn-on-crash compensation
    // like the Redis version
    struct IdRange {
        uint64_t next = 0, limit = 0;
    };
    static constexpr uint64_t kIdSegment = 4096;
    // Number of guard shards (§4.3): concurrent puts collide with probability 1/16;
    // delete_bucket/complete/abort write all 16 shards to materialize the write-skew
    // conflict
    static constexpr uint32_t kGuardShards = 16;

    // Live client. The close contract = no in-flight calls (guaranteed by
    // DuoStoreBackend's close ordering); the atomic pointer makes a
    // contract-violating call after close deterministically throw InternalError
    // instead of a TOCTOU data race (same guard shape as the rocks version's db_)
    TikvClient& client();

    // ---- key construction (§3.2: prefix + one-char table tag + codec composite segment) ----
    std::string tkey(char tag, std::string_view rest) const;
    std::string bucket_key(std::string_view b) const;                  // 'B'
    std::string bucket_guard(std::string_view b, uint32_t shard) const;  // 'b'
    std::string object_key(std::string_view b, std::string_view k) const;  // 'O'
    std::string upload_key(std::string_view b, std::string_view k, std::string_view id) const;
    std::string upload_guard(std::string_view b, std::string_view k, std::string_view id,
                             uint32_t shard) const;  // 'u'
    std::string part_key(std::string_view b, std::string_view k, std::string_view id,
                         int part_no) const;                        // 'P'
    std::string refs_key(uint64_t file_id) const;                   // 'R'
    std::string gcq_key(uint64_t seq) const;                        // 'G'
    std::string counter_key(char kind) const;                       // 'C'
    // Pack liveness ledger ('S' table, §3.2): delta rows S<be64 id>d<be64 delta_id>
    // (value = le64 bytes ‖ le64 recs) + seal row S<be64 id>s (value = le64
    // file_size). Each business transaction writes a unique delta row —
    // read-modify-write of a shared ledger row would make small-object PUT prewrites
    // on the same active pack conflict with each other (the materialized solution to
    // the §3.2 warning); folding is in pack_stats()
    std::string pack_delta_key(uint64_t pack_id, uint64_t delta_id) const;
    std::string pack_seal_key(uint64_t pack_id) const;
    // [lo, hi) prefix range (bucket names / upper-layer validation guarantee the composite segment has no NUL ambiguity)
    std::pair<std::string, std::string> range_of(char tag, std::string_view rest) const;

    // ---- Transaction and read helpers (implemented in the .cc) ----
    // Optimistic retry loop (§4.1): each round takes a new start_ts → body(ts, muts)
    // reads/computes and assembles the batch → commit; empty muts = pure read / early
    // return, no transaction sent. Conflicts back off exponentially 100µs..6.4ms, 16 attempts max
    template <typename Body>
    auto txn_retry(const char* what, Body&& body);
    // Pure reads retry once (§6.4); unified translation of pingcap exceptions → S3Error(InternalError)
    template <typename Fn>
    auto guarded(const char* what, Fn&& fn);

    std::optional<std::string> snap_get(uint64_t ver, const std::string& key);
    // Batched snapshot read (one KvBatchGet round trip instead of serial per-key
    // Gets), retry semantics same as snap_get
    std::vector<std::optional<std::string>> snap_get_many(uint64_t ver,
                                                          const std::vector<std::string>& keys);
    // Paged scan of the full [lo, hi) (1024 per page); escape: callback returning false stops early
    template <typename Fn>
    void scan_range(uint64_t ver, std::string lo, const std::string& hi, Fn&& cb);

    uint64_t alloc_id(char kind, IdRange& r, uint32_t n = 1);
    // gcq ledger entry: seq is pre-dispatched (independent small transaction), so the
    // entry itself stays a pure-write mutation
    void enqueue_reclaim(std::vector<TikvMutation>& muts, const DataRef& ref,
                         ReclaimReason reason);
    void mut_refs(std::vector<TikvMutation>& muts, const DataRef& ref, bool add,
                  std::string_view owner);
    // Maintains the pack liveness ledger in the same batch (unique delta rows,
    // pure-write, no conflict). Independent of mut_refs: complete's refs transfer
    // (owner rewrite) must be a no-op for packs — mixing them would double-count.
    // rec_overhead: per-record header overhead (codec::pack_rec_overhead*); live_bytes
    // uses the same accounting basis as file_size (docs/gaps.md §2.3a)
    void mut_pack_delta(std::vector<TikvMutation>& muts, const DataRef& ref, int sign,
                        int64_t rec_overhead);
    // Full read of parts (ascending by part_no; the be16 suffix is naturally ordered)
    std::vector<PartRec> scan_parts(uint64_t ver, std::string_view b, std::string_view k,
                                    std::string_view id);

    TikvMetaOptions opt_;
    std::unique_ptr<TikvClient> client_owned_;
    std::atomic<TikvClient*> client_{nullptr};  // nulled after close (see client() comment)

    // T5 metrics (registered at construction; zero values are visible)
    std::shared_ptr<MetricCounter> m_conflict_retries_;
    std::shared_ptr<MetricCounter> m_safepoint_failures_;
    std::shared_ptr<MetricGauge> m_safepoint_ms_;  // most recently pushed cluster safepoint (physical ms)

    // safepoint worker (§7.3): the cv wait can be woken immediately for exit; close()
    // stops the worker before detaching the client (the worker gets its handle via
    // client(); detaching too early turns the normal exit path into a 500 throw)
    std::thread sp_thread_;
    std::mutex sp_mu_;
    std::condition_variable sp_cv_;
    bool sp_stop_ = false;

    // Separate small lock for id-segment dispatch (alloc is called on the data plane
    // each time a chunk opens; it must not queue behind business commits); network
    // segment renewal on exhaustion happens outside the lock (see alloc_id comment)
    std::mutex alloc_mu_;
    IdRange file_ids_[2];  // indexed by Extent::Kind
    IdRange seqs_;         // gcq seq
    IdRange pack_deltas_;  // pack ledger delta row id (uniqueness suffices, 'd' counter)
};

}  // namespace lights3::storage::duostore
