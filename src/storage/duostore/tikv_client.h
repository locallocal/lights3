// L3: TiKV client sidecar (docs/duostore-tikv-meta.md §6.3). client-c's transaction
// commit layer is test-grade (mutations are Put-only, commit error paths contain TODOs),
// so this file builds on its public transport infrastructure
// (Cluster/RegionCache/RegionClient/Backoffer/LockResolver) to implement an optimistic
// 2PC committer with ops (Put/Del/Lock/Insert) — we do not fork the submodule, keeping
// upstream pristine; this sidecar retires once upstream lands equivalent capability.
// pingcap headers (which drag in grpc/Poco) are all confined to the .cc.
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace pingcap::kv {
struct Cluster;
}

namespace lights3::storage::duostore {

struct TikvOptions {
    std::vector<std::string> pd_endpoints;  // list of "host:port"
    // mTLS triple (optional; enabled only when all three are given, docs/duostore-tikv-meta.md §9)
    std::string ca_path;
    std::string cert_path;
    std::string key_path;
    // Parameterized backoff budget (§6.1, T5): when >0, replaces the library-default
    // budget (ms) for the **sidecar paths** (2PC commit, batch_get, last_key, safepoint
    // RPCs); commit uses 2x (matching upstream's commitMaxBackoff ≈ 2x prewrite ratio).
    // 0 = library default. Backoffers built internally by upstream Snapshot/Scanner are
    // not controlled — a global per-call timeout is listed as an upstream improvement (§11 T5)
    int backoff_budget_ms = 0;
};

// mutation op (subset of kvrpcpb::Op, the four used by §4.4)
enum class TikvOp : uint8_t {
    kPut,
    kDel,
    kLock,    // placeholder lock record: materializes write-skew conflicts for read-only preconditions (§4.3 guard shards)
    kInsert,  // put + must-not-exist (create_bucket → BucketAlreadyOwnedByYou)
};

struct TikvMutation {
    TikvOp op;
    std::string key;
    std::string value;  // always empty for kDel/kLock
};

// ---- Commit outcome classification (§4.1/§4.6; the meta store decides retry/mapping by category) ----
// WriteConflict / prewrite lock contention over budget: definitely not committed;
// take a new start_ts, re-read and retry
struct TikvConflict {
    std::string what;
};
// Op::Insert hit an already-existing key: definitely not committed (sole call site: create_bucket)
struct TikvAlreadyExist {
    std::string key;
};
// primary commit outcome unknown (blind-retry ban, §4.6): always rethrown as InternalError
struct TikvUndetermined {
    std::string what;
};

class TikvClient {
public:
    explicit TikvClient(const TikvOptions& opt);
    ~TikvClient();
    TikvClient(const TikvClient&) = delete;
    TikvClient& operator=(const TikvClient&) = delete;

    // PD TSO (transaction start_ts / list snapshot version)
    uint64_t get_ts();

    // Single-key snapshot read; returns nullopt when absent (client-c Get signals
    // absence with an empty string; codec values are never empty so this
    // disambiguation is lossless, §3.1)
    std::optional<std::string> get(uint64_t version, const std::string& key);

    // Batched snapshot read (KvBatchGet, grouped by region); returns a value array of
    // the same length and order as keys. Locked keys fall back to single-key Get
    // (which resolves locks internally)
    std::vector<std::optional<std::string>> batch_get(uint64_t version,
                                                      const std::vector<std::string>& keys);

    // Range scan [begin, end), at most limit entries (empty end = no upper bound).
    // limit is pushed down exactly as the scan batch size — an existence probe
    // (limit=1) fetches just 1 entry, no implicit over-fetch. Multiple calls at the
    // same version form a consistent view (MVCC, §3.3)
    std::vector<std::pair<std::string, std::string>> scan(uint64_t version,
                                                          const std::string& begin,
                                                          const std::string& end, size_t limit);

    // Last key within [lo, hi); nullopt if none. Key-only reverse scan: region lookup
    // goes through the cache + typically 1 RPC — the O(1) primitive for list group-tail
    // tokens (§3.3, counterpart of the RocksDB version's SeekForPrev; client-c Scanner
    // does not wrap reverse, so raw KvScan is used here)
    std::optional<std::string> last_key(uint64_t version, const std::string& lo,
                                        const std::string& hi);

    // Optimistic 2PC commit (§4). muts must be non-empty; primary = muts[0].key.
    // Multiple mutations on the same key merge in order of appearance, last one wins
    // (matching WriteBatch's in-order-overwrite semantics — Percolator allows only one
    // op per key). Exceptions:
    //   TikvConflict      — write-write conflict / lock contention, safe to retry
    //   TikvAlreadyExist  — Insert hit an existing key, safe to fail
    //   TikvUndetermined  — primary commit outcome unknown, blind retry forbidden
    //   pingcap::Exception — everything else (network/cluster); those from the
    //                        prewrite phase are definitely not committed
    void commit(uint64_t start_ts, const std::vector<TikvMutation>& muts);

    // ---- GC safepoint (§7.3; direct PD RPC sidecar — client-c does not wrap these three) ----
    // Register/renew this service's service safepoint (TTL in seconds; PD removes it
    // automatically on expiry) and return the minimum across all services. Semantics:
    // declare "this service no longer reads versions before safe_point"
    uint64_t update_service_gc_safepoint(const std::string& service_id, int64_t ttl_s,
                                         uint64_t safe_point);
    // Advance the cluster GC safepoint (monotonic forward-only on the PD side; a
    // lagging value just returns the current value unchanged). The argument must
    // always be the min returned by update_service_gc_safepoint — going past any live
    // service's declaration breaks its snapshot
    uint64_t update_gc_safepoint(uint64_t safe_point);
    uint64_t get_gc_safepoint();

    // Test hook: exposes the underlying cluster (splitRegion to create multiple regions etc., §10)
    pingcap::kv::Cluster* cluster();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace lights3::storage::duostore
