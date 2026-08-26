// L3: TiKV client sidecar implementation (docs/duostore-tikv-meta.md §6.3).
// The committer skeleton is adapted from client-c's kv/2pc.cc (Apache-2.0, tikv/client-c@78a557e):
// region grouping/batching, prewrite lock resolution, and region-error retry stay isomorphic;
// differences —
//   1. mutations carry an op (Put/Del/Lock/Insert); upstream is always Put;
//   2. AlreadyExist / WriteConflict surface as structured exceptions (upstream lumps them into
//      LogicalError/Unknown);
//   3. commit has two explicit branches: primary explicitly rejected = already rolled back (safe
//      to retry), RPC-layer exception = result unknown (TikvUndetermined, §4.6 no-blind-retry
//      rule); upstream leaves this as a TODO;
//   4. best-effort BatchRollback lock cleanup after prewrite failure (upstream TODO), shortening
//      concurrent parties' wait on residual locks (a failed commit-point TSO fetch is covered
//      too); failure is harmless — residual locks converge via TTL / readers' LockResolver;
//   5. commit ts discipline: in the primary phase, refetch the TSO before region retries and
//      retry in place with a fresh ts on CommitTsExpired (the normal path when readers push up
//      min_commit_ts); secondaries always use the commit_ts frozen by the primary — upstream
//      refetches the TSO for secondaries too, which forks primary/secondary commit_ts; not
//      followed;
//   6. multiple mutations on the same key merge at construction (last one wins, matching
//      WriteBatch's in-order overwrite semantics);
//   7. "live lock held by a newer optimistic txn" is classified as a conflict early at the
//      prewrite error site, without relying on upstream resolveLocksForWrite's bare
//      Exception("write conflict") message string (string matching kept as defense in depth);
//   8. adds two read primitives, batch_get (KvBatchGet) and last_key (key-only reverse scan),
//      which upstream Snapshot/Scanner do not cover.
#include "storage/duostore/tikv_client.h"

#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Poco/URI.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <kvproto/pdpb.grpc.pb.h>
#include <pingcap/Exception.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/LockResolver.h>
#include <pingcap/kv/RegionClient.h>
#include <pingcap/kv/Scanner.h>
#include <pingcap/kv/Snapshot.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <mutex>
#include <tuple>
#include <unordered_map>

#include "core/log.h"

namespace lights3::storage::duostore {

namespace {

using pingcap::Exception;
using pingcap::kv::Backoffer;
using pingcap::kv::Cluster;
using pingcap::kv::RegionClient;
using pingcap::kv::RegionVerID;

// Matches client-c txnCommitBatchSize: per-region-batch upper bound on key+value bytes
constexpr uint64_t kTxnCommitBatchSize = 16 * 1024;

// lock_ttl scaling cap (upstream 2pc.cc managedLockTTL, copied since it is not exported): large
// transactions send prewrite batches serially and take longer, so the TTL scales up accordingly
// — otherwise readers would judge a still-prewriting primary as dead and roll it back
// (§6.3 ten-thousand-part complete case)
constexpr uint64_t kManagedLockTTL = 20000;

// Transaction lock_ttl (sidecar version of upstream txnLockTTL): small transactions always use
// defaultLockTTL(3s); those exceeding the per-batch byte bound scale by ttlFactor·√MiB, clamped
// to [3s, 20s]. Difference from upstream: no elapsed term — we commit immediately after
// batching, with no TiDB-style "accumulate a buffer, then commit" gap
uint64_t txn_lock_ttl(uint64_t mutation_bytes) {
    if (mutation_bytes < kTxnCommitBatchSize) return pingcap::kv::defaultLockTTL;
    uint64_t mb = std::max<uint64_t>(mutation_bytes >> 20, 1);
    auto ttl = uint64_t(double(pingcap::kv::ttlFactor) * std::sqrt(double(mb)));
    return std::clamp(ttl, pingcap::kv::defaultLockTTL, kManagedLockTTL);
}

// Poco log bridge (§6.2, T5): client-c logs everything through Poco::Logger, whose default
// ConsoleChannel carries its own format, disjoint from spdlog. After swapping the root logger to
// this bridge, all other pingcap.* loggers funnel along the inheritance chain into the unified
// stderr format (the source name is kept as a message prefix). Must be installed before the
// first pingcap logger is created (= first Cluster construction) — Poco child loggers inherit
// the channel at creation time
class PocoSpdlogChannel : public Poco::Channel {
public:
    void log(const Poco::Message& m) override {
        switch (m.getPriority()) {
            case Poco::Message::PRIO_FATAL:
            case Poco::Message::PRIO_CRITICAL:
            case Poco::Message::PRIO_ERROR:
                LOG_ERROR("{}: {}", m.getSource(), m.getText());
                break;
            case Poco::Message::PRIO_WARNING:
                LOG_WARN("{}: {}", m.getSource(), m.getText());
                break;
            case Poco::Message::PRIO_NOTICE:
            case Poco::Message::PRIO_INFORMATION:
                LOG_INFO("{}: {}", m.getSource(), m.getText());
                break;
            default:
                LOG_DEBUG("{}: {}", m.getSource(), m.getText());
        }
    }
};

void bridge_poco_logs_once() {
    static std::once_flag once;
    std::call_once(once, [] {
        Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::Channel>(new PocoSpdlogChannel));
        // Start at information: pingcap's debug noise is cut off at the source (saving format
        // cost); information and above still pass a second filter, the global log level, once in spdlog
        Poco::Logger::root().setLevel(Poco::Message::PRIO_INFORMATION);
    });
}

::kvrpcpb::Op to_pb_op(TikvOp op) {
    switch (op) {
        case TikvOp::kPut: return ::kvrpcpb::Put;
        case TikvOp::kDel: return ::kvrpcpb::Del;
        case TikvOp::kLock: return ::kvrpcpb::Lock;
        case TikvOp::kInsert: return ::kvrpcpb::Insert;
    }
    __builtin_unreachable();
}

struct Batch {
    RegionVerID region;
    std::vector<std::string> keys;
};

// ---- Transaction capability boundary (docs/archive/gaps.md §6.1 assessment) ----
// This sidecar does optimistic 2PC only; the four "missing" items are deliberate narrowing
// argued from the workload, not debt:
//   Pessimistic transactions: meta transactions are all short (a single-object put touches a few
//     keys, swap is a single-key CAS); conflicts converge via txn_retry's 16 rounds of
//     exponential backoff; client-c's pessimistic path (PessimisticLock + TTL manager) has no
//     mature C++ implementation, and adopting it for nonexistent long transactions is negative value.
//   Lock TTL renewal (TxnHeartBeat): renewal serves large transactions whose locks might expire
//     during prewrite. This store's transaction size is bounded by construction — object values
//     capped at 6MiB (kMaxObjectValueBytes), gcq entries split at 4096 extents
//     (kReclaimMaxExtents), ten-thousand-part complete measured at worst ~a few dozen batches —
//     txn_lock_ttl's √MiB scaling (20s cap) already covers this; no heartbeat thread needed.
//   Sharded commit for large transactions: same as above — transaction boundedness is enforced
//     at the entrance, so no transaction ever needs sharding.
//   Inter-batch concurrency: prewrite/commit run batch by batch, serially. Concurrency would
//     require making the shared Backoffer/RegionCache thread-safe (upstream itself is serial +
//     test-grade), and the benefit only reaches low-frequency wide transactions
//     (complete/delete_bucket) — not worth it. If wide transactions ever become the norm, split
//     the transaction first rather than parallelize
class Committer {
public:
    Committer(Cluster* cluster, uint64_t start_ts, const std::vector<TikvMutation>& muts,
              int backoff_budget_ms)
        : cluster_(cluster), start_ts_(start_ts) {
        keys_.reserve(muts.size());
        uint64_t bytes = 0;
        for (const auto& m : muts) {
            // Multiple mutations on the same key: last one wins (WriteBatch in-order overwrite
            // semantics; Percolator sends one op per key). keys_ dedups in first-seen order —
            // primary is still the first key
            auto [it, inserted] = by_key_.try_emplace(m.key, &m);
            if (inserted)
                keys_.push_back(m.key);
            else
                it->second = &m;
        }
        for (auto& [k, m] : by_key_) bytes += k.size() + m->value.size();
        lock_ttl_ = txn_lock_ttl(bytes);
        primary_ = keys_.front();
        // Budget parameterization (§6.1, T5): commit uses 2× to match upstream's commit:prewrite ≈ 2:1
        prewrite_budget_ = backoff_budget_ms > 0 ? backoff_budget_ms
                                                 : pingcap::kv::prewriteMaxBackoff;
        commit_budget_ = backoff_budget_ms > 0 ? 2 * backoff_budget_ms
                                               : pingcap::kv::commitMaxBackoff;
    }

    void execute() {
        try {
            Backoffer bo(prewrite_budget_);
            prewrite_keys(bo, keys_);
            // The commit-point TSO fetch is also under lock-cleanup protection: PD failure =
            // definitively not committed, and residual locks must be cleaned (otherwise the
            // guard locks of wide transactions like delete_bucket would stall concurrent
            // same-bucket writes for a whole TTL)
            commit_ts_ = cluster_->pd_client->getTS();
        } catch (...) {
            cleanup_locks();
            throw;
        }
        // ---- Commit point: primary in its own batch (§4.6) ----
        try {
            Backoffer bo(commit_budget_);
            commit_keys(bo, {primary_}, /*primary_phase=*/true);
        } catch (TikvConflict&) {
            // TiKV's commit is idempotent for a committed transaction and returns ok; an explicit rejection = already rolled back — safe to retry
            throw;
        } catch (Exception& e) {
            throw TikvUndetermined{e.displayText()};
        }
        // ---- Once the primary lands, the transaction has succeeded; secondary failures only
        // delay convergence (readers check back on the primary via LockResolver) — swallow and warn ----
        if (keys_.size() > 1) {
            try {
                Backoffer bo(commit_budget_);
                commit_keys(bo, {keys_.begin() + 1, keys_.end()}, /*primary_phase=*/false);
            } catch (const TikvConflict& c) {
                LOG_WARN("tikv: secondary commit rejected (resolves lazily): {}", c.what);
            } catch (const Exception& e) {
                LOG_WARN("tikv: secondary commit failed (resolves lazily): {}", e.displayText());
            }
        }
    }

private:
    // Region grouping + byte-bounded batching (mirrors the shape of client-c doActionOnKeys)
    std::vector<Batch> make_batches(Backoffer& bo, const std::vector<std::string>& keys,
                                    bool with_values) {
        auto [groups, first_region] = cluster_->region_cache->groupKeysByRegion(bo, keys);
        std::ignore = first_region;
        std::vector<Batch> batches;
        for (auto& [region, group_keys] : groups) {
            uint64_t size = 0;
            std::vector<std::string> sub;
            for (auto& k : group_keys) {
                uint64_t s = k.size() + (with_values ? by_key_.at(k)->value.size() : 0);
                if (!sub.empty() && size + s > kTxnCommitBatchSize) {
                    batches.push_back(Batch{region, std::move(sub)});
                    sub.clear();
                    size = 0;
                }
                sub.push_back(k);
                size += s;
            }
            if (!sub.empty()) batches.push_back(Batch{region, std::move(sub)});
        }
        return batches;
    }

    // Explicit work stack instead of recursion (docs/archive/gaps.md §4): redo triggered by region
    // split/migration used to recurse (prewrite_batch → prewrite_keys → …); the backoff budget
    // bounds time, not stack depth, which is unbounded under frequent region churn. false =
    // this batch hit a region error; its keys go back on the stack to re-resolve routing and redo
    void prewrite_keys(Backoffer& bo, const std::vector<std::string>& keys) {
        std::vector<std::vector<std::string>> work;
        work.push_back(keys);
        while (!work.empty()) {
            auto group = std::move(work.back());
            work.pop_back();
            for (auto& b : make_batches(bo, group, /*with_values=*/true))
                if (!prewrite_batch(bo, b)) work.push_back(std::move(b.keys));
        }
    }

    bool prewrite_batch(Backoffer& bo, const Batch& batch) {
        for (;;) {
            ::kvrpcpb::PrewriteRequest req;
            for (auto& k : batch.keys) {
                const TikvMutation& m = *by_key_.at(k);
                auto* mu = req.add_mutations();
                mu->set_op(to_pb_op(m.op));
                mu->set_key(k);
                if (m.op == TikvOp::kPut || m.op == TikvOp::kInsert) mu->set_value(m.value);
            }
            req.set_primary_lock(primary_);
            req.set_start_version(start_ts_);
            req.set_lock_ttl(lock_ttl_);  // scales with transaction size (txn_lock_ttl, §6.3)
            req.set_txn_size(keys_.size());
            req.set_min_commit_ts(start_ts_ + 1);

            ::kvrpcpb::PrewriteResponse resp;
            RegionClient rc(cluster_, batch.region);
            try {
                rc.sendReqToRegion<pingcap::kv::RPC_NAME(KvPrewrite)>(bo, req, &resp);
            } catch (Exception& e) {
                // region-level error (split/migration): back off, then hand back to the caller's work stack to re-resolve routing and redo
                bo.backoff(pingcap::kv::boRegionMiss, e);
                return false;
            }
            if (resp.errors_size() == 0) return true;
            std::vector<pingcap::kv::LockPtr> locks;
            for (const auto& err : resp.errors()) {
                if (err.has_already_exist()) throw TikvAlreadyExist{err.already_exist().key()};
                if (err.has_conflict()) throw TikvConflict{err.conflict().ShortDebugString()};
                if (!err.retryable().empty()) throw TikvConflict{err.retryable()};
                auto lock = pingcap::kv::extractLockFromKeyErr(err);  // throws internally on unknown errors
                // A live lock held by a newer optimistic txn = the condition under which
                // resolveLocksForWrite must throw (upstream expresses it as a bare
                // Exception("write conflict"); LockResolver.cc carries its own TODO) — classify
                // it up front instead of betting the classification on the message string. If
                // the peer is actually dead: this round just backs off once more, and the
                // retry's fresh start_ts is necessarily greater than its txn_id (a TSO from a
                // past moment), so progress goes through the resolver cleanup path; convergence is safe
                if (lock->lock_type != ::kvrpcpb::PessimisticLock && lock->txn_id > start_ts_)
                    throw TikvConflict{"blocked by newer optimistic txn " +
                                       std::to_string(lock->txn_id)};
                locks.push_back(std::move(lock));
            }
            int64_t before_expired = 0;
            try {
                before_expired = cluster_->lock_resolver->resolveLocksForWrite(bo, start_ts_, locks);
            } catch (Exception& e) {
                // Upstream throws a bare Exception("write conflict") for "live lock held by a
                // newer txn" (LockResolver.cc carries its own TODO; no structured error code;
                // the message is stable at @78a557e) — the prewrite phase is definitively
                // uncommitted, so classify as conflict and retry
                if (e.displayText().find("write conflict") != std::string::npos)
                    throw TikvConflict{e.displayText()};
                throw;
            }
            if (before_expired > 0) {
                bo.backoffWithMaxSleep(
                    pingcap::kv::boTxnLock, static_cast<int>(before_expired),
                    Exception("prewrite blocked by " + std::to_string(locks.size()) + " locks",
                              pingcap::ErrorCodes::LockError));
            }
        }
    }

    // Work stack isomorphic to prewrite_keys (docs/archive/gaps.md §4)
    void commit_keys(Backoffer& bo, const std::vector<std::string>& keys, bool primary_phase) {
        std::vector<std::vector<std::string>> work;
        work.push_back(keys);
        while (!work.empty()) {
            auto group = std::move(work.back());
            work.pop_back();
            for (auto& b : make_batches(bo, group, /*with_values=*/false))
                if (!commit_batch(bo, b, primary_phase)) work.push_back(std::move(b.keys));
        }
    }

    bool commit_batch(Backoffer& bo, const Batch& batch, bool primary_phase) {
        // Cap on commit_ts refresh retries for the primary commit: CommitTsExpired is triggered
        // by concurrent readers pushing up min_commit_ts and dissolves with a fresh TSO; the cap
        // prevents spinning in place while the read side keeps pushing (over the cap =
        // definitively uncommitted, throw conflict and retry the whole transaction)
        constexpr int kMaxTsRefresh = 4;
        for (int ts_refresh = 0;; ++ts_refresh) {
            ::kvrpcpb::CommitRequest req;
            for (auto& k : batch.keys) req.add_keys(k);
            req.set_start_version(start_ts_);
            req.set_commit_version(commit_ts_);

            ::kvrpcpb::CommitResponse resp;
            RegionClient rc(cluster_, batch.region);
            try {
                rc.sendReqToRegion<pingcap::kv::RPC_NAME(KvCommit)>(bo, req, &resp);
            } catch (Exception& e) {
                bo.backoff(pingcap::kv::boRegionMiss, e);
                // Refetch the TSO before replaying the primary: readers may have pushed up
                // min_commit_ts during the backoff, and replaying with the old ts would hit
                // CommitTsExpired. Replay is safe: commit is idempotent per start_ts — if a
                // prior attempt already took effect, TiKV returns ok straight from the existing
                // write record, so a changed ts is harmless. Secondaries always use the
                // commit_ts_ frozen by the primary (upstream refetches the TSO for secondaries
                // too, which forks primary/secondary commit_ts — a test-grade defect of theirs; not followed)
                if (primary_phase) commit_ts_ = cluster_->pd_client->getTS();
                return false;  // hand back to the caller's work stack to re-resolve routing and redo
            }
            if (!resp.has_error()) return true;
            if (primary_phase && resp.error().has_commit_ts_expired() &&
                ts_refresh < kMaxTsRefresh) {
                commit_ts_ = cluster_->pd_client->getTS();  // the TSO fetch's own latency acts as throttling
                continue;
            }
            throw TikvConflict{resp.error().ShortDebugString()};
        }
    }

    // Best-effort lock cleanup after prewrite failure (file-header difference 4). Writing extra
    // Rollback tombstones for keys never prewritten is harmless (this start_ts is never reused)
    void cleanup_locks() noexcept {
        try {
            Backoffer bo(prewrite_budget_);
            for (auto& b : make_batches(bo, keys_, /*with_values=*/false)) {
                ::kvrpcpb::BatchRollbackRequest req;
                req.set_start_version(start_ts_);
                for (auto& k : b.keys) req.add_keys(k);
                ::kvrpcpb::BatchRollbackResponse resp;
                RegionClient rc(cluster_, b.region);
                rc.sendReqToRegion<pingcap::kv::RPC_NAME(KvBatchRollback)>(bo, req, &resp);
            }
        } catch (...) {
            // if cleanup fails, leave it to TTL / readers' LockResolver
        }
    }

    Cluster* cluster_;
    uint64_t start_ts_;
    uint64_t commit_ts_ = 0;
    uint64_t lock_ttl_ = pingcap::kv::defaultLockTTL;
    int prewrite_budget_ = pingcap::kv::prewriteMaxBackoff;
    int commit_budget_ = pingcap::kv::commitMaxBackoff;
    std::vector<std::string> keys_;
    std::unordered_map<std::string, const TikvMutation*> by_key_;
    std::string primary_;
};

}  // namespace

struct TikvClient::Impl {
    std::unique_ptr<Cluster> cluster;
    pingcap::ClusterConfig cluster_cfg;  // the direct safepoint channel reuses the same TLS config
    int backoff_budget_ms = 0;

    // ---- Direct PD-leader stub for GC safepoint (§7.3) ----
    // client-c's PDConnClient/stub are entirely private and the three safepoint calls are not
    // wrapped — the sidecar builds its own channel to getLeaderUrl() (a public interface).
    // Lazily built and cached; on an RPC error or leader change, drop the cache and rebuild
    // (the next call re-resolves the leader). Low-frequency ops path; holding the lock throughout is fine
    std::mutex pd_mu;
    std::string pd_url;
    std::unique_ptr<::pdpb::PD::Stub> pd_stub;

    template <typename Resp, typename Rpc>
    Resp pd_call(const char* what, const Rpc& rpc) {
        std::lock_guard lk(pd_mu);
        std::string url = cluster->pd_client->getLeaderUrl();
        if (!pd_stub || url != pd_url) {
            Poco::URI uri(url);  // the leader URL looks like http(s)://host:port; grpc only needs the authority
            auto creds = cluster_cfg.hasTlsConfig()
                             ? grpc::SslCredentials(cluster_cfg.getGrpcCredentials())
                             : grpc::InsecureChannelCredentials();
            pd_stub = ::pdpb::PD::NewStub(grpc::CreateChannel(uri.getAuthority(), creds));
            pd_url = url;
        }
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
        Resp resp;
        grpc::Status st = rpc(&ctx, *pd_stub, &resp);
        if (!st.ok()) {
            pd_stub.reset();
            throw Exception(std::string(what) + " failed: " + std::to_string(st.error_code()) +
                                ": " + st.error_message(),
                            pingcap::ErrorCodes::GRPCErrorCode);
        }
        if (resp.header().has_error()) {  // PD-level errors such as not-leader
            pd_stub.reset();
            throw Exception(std::string(what) + " rejected: " + resp.header().error().message(),
                            pingcap::ErrorCodes::UnknownError);
        }
        return resp;
    }

    ::pdpb::RequestHeader* pd_header() {
        auto* h = new ::pdpb::RequestHeader();  // set_allocated_* takes ownership
        h->set_cluster_id(cluster->pd_client->getClusterID());
        return h;
    }
};

TikvClient::TikvClient(const TikvOptions& opt) : impl_(std::make_unique<Impl>()) {
    bridge_poco_logs_once();  // before the first pingcap logger is created (see the comment at the bridge definition)
    pingcap::ClusterConfig cfg;
    cfg.ca_path = opt.ca_path;
    cfg.cert_path = opt.cert_path;
    cfg.key_path = opt.key_path;
    impl_->cluster_cfg = cfg;
    impl_->backoff_budget_ms = opt.backoff_budget_ms;
    impl_->cluster = std::make_unique<Cluster>(opt.pd_endpoints, cfg);
}

// Cluster's destructor stops the rpc/region cache/background threads on its own (client-c Cluster::~Cluster)
TikvClient::~TikvClient() = default;

uint64_t TikvClient::get_ts() { return impl_->cluster->pd_client->getTS(); }

std::optional<std::string> TikvClient::get(uint64_t version, const std::string& key) {
    pingcap::kv::Snapshot snap(impl_->cluster.get(), version);
    std::string v = snap.Get(key);
    if (v.empty()) return std::nullopt;  // codec values are never empty; an empty string means absent (§3.1)
    return v;
}

std::vector<std::pair<std::string, std::string>> TikvClient::scan(uint64_t version,
                                                                  const std::string& begin,
                                                                  const std::string& end,
                                                                  size_t limit) {
    pingcap::kv::Snapshot snap(impl_->cluster.get(), version);
    // Push limit down as the batch size (Snapshot::Scan is fixed at 256): a limit=1 existence probe fetches only 1 entry
    int batch = int(std::min<size_t>(std::max<size_t>(limit, 1), 1024));
    pingcap::kv::Scanner scanner(snap, begin, end, batch);
    std::vector<std::pair<std::string, std::string>> out;
    while (scanner.valid && out.size() < limit) {
        out.emplace_back(scanner.key(), scanner.value());
        scanner.next();
    }
    return out;
}

std::vector<std::optional<std::string>> TikvClient::batch_get(
    uint64_t version, const std::vector<std::string>& keys) {
    using pingcap::kv::LockPtr;
    std::unordered_map<std::string, std::string> found;
    std::vector<std::string> pending = keys;
    Backoffer bo(impl_->backoff_budget_ms > 0 ? impl_->backoff_budget_ms
                                              : pingcap::kv::GetMaxBackoff);
    while (!pending.empty()) {
        auto [groups, first_region] = impl_->cluster->region_cache->groupKeysByRegion(bo, pending);
        std::ignore = first_region;
        std::vector<std::string> retry;
        for (auto& [region, group_keys] : groups) {
            ::kvrpcpb::BatchGetRequest req;
            for (auto& k : group_keys) req.add_keys(k);
            req.set_version(version);
            ::kvrpcpb::BatchGetResponse resp;
            RegionClient rc(impl_->cluster.get(), region);
            try {
                rc.sendReqToRegion<pingcap::kv::RPC_NAME(KvBatchGet)>(bo, req, &resp);
            } catch (Exception& e) {
                bo.backoff(pingcap::kv::boRegionMiss, e);  // region changed: regroup next round
                retry.insert(retry.end(), group_keys.begin(), group_keys.end());
                continue;
            }
            if (resp.has_error()) {
                // Response-level lock error: pairs is empty; resolve, then redo the whole group (semantics per the proto comment)
                std::vector<LockPtr> locks{pingcap::kv::extractLockFromKeyErr(resp.error())};
                std::vector<uint64_t> pushed;
                auto ms = impl_->cluster->lock_resolver->resolveLocks(bo, version, locks, pushed);
                if (ms > 0)
                    bo.backoffWithMaxSleep(
                        pingcap::kv::boTxnLockFast, static_cast<int>(ms),
                        Exception("batch_get blocked by lock", pingcap::ErrorCodes::LockError));
                retry.insert(retry.end(), group_keys.begin(), group_keys.end());
                continue;
            }
            pingcap::kv::Snapshot snap(impl_->cluster.get(), version);
            for (int i = 0; i < resp.pairs_size(); ++i) {
                const auto& pair = resp.pairs(i);
                if (pair.has_error()) {
                    // Single key locked: fall back to Get (which resolves the lock internally; same trick as Scanner)
                    auto lock = pingcap::kv::extractLockFromKeyErr(pair.error());
                    found[lock->key] = snap.Get(bo, lock->key);
                } else {
                    found[pair.key()] = pair.value();
                }
            }
        }
        pending = std::move(retry);
    }
    std::vector<std::optional<std::string>> out;
    out.reserve(keys.size());
    for (auto& k : keys) {
        auto it = found.find(k);
        // Missing or empty (codec values are never empty) both count as absent — same disambiguation as get()
        if (it == found.end() || it->second.empty()) out.emplace_back(std::nullopt);
        else out.emplace_back(it->second);
    }
    return out;
}

std::optional<std::string> TikvClient::last_key(uint64_t version, const std::string& lo,
                                                const std::string& hi) {
    using pingcap::kv::KeyLocation;
    using pingcap::kv::LockPtr;
    Backoffer bo(impl_->backoff_budget_ms > 0 ? impl_->backoff_budget_ms
                                              : pingcap::kv::scanMaxBackoff);
    for (;;) {  // outer loop: restart from scratch when the region topology changes
        // Walk forward collecting the regions covering [lo, hi) (mostly cache hits, zero data
        // transfer), then reverse-scan limit=1 region by region from the tail — sidestepping
        // "locate the predecessor region by upper bound", a primitive client-c does not provide
        std::vector<KeyLocation> regions;
        std::string cur = lo;
        for (;;) {
            auto loc = impl_->cluster->region_cache->locateKey(bo, cur);
            bool covers_hi = loc.end_key.empty() || loc.end_key >= hi;
            regions.push_back(std::move(loc));
            if (covers_hi) break;
            cur = regions.back().end_key;
        }
        bool topo_changed = false;
        for (auto it = regions.rbegin(); it != regions.rend() && !topo_changed; ++it) {
            // Intersection with [lo, hi) (an empty start_key = start of the keyspace, always < lo)
            std::string r_hi = (it->end_key.empty() || it->end_key >= hi) ? hi : it->end_key;
            std::string r_lo = it->start_key < lo ? lo : it->start_key;
            for (;;) {  // lock-resolution retry for this region
                ::kvrpcpb::ScanRequest req;
                req.set_start_key(r_hi);  // reverse: scan [end_key, start_key) descending
                req.set_end_key(r_lo);
                req.set_limit(1);
                req.set_version(version);
                req.set_key_only(true);
                req.set_reverse(true);
                ::kvrpcpb::ScanResponse resp;
                RegionClient rc(impl_->cluster.get(), it->region);
                try {
                    rc.sendReqToRegion<pingcap::kv::RPC_NAME(KvScan)>(bo, req, &resp);
                } catch (Exception& e) {
                    bo.backoff(pingcap::kv::boRegionMiss, e);
                    topo_changed = true;
                    break;
                }
                if (resp.has_error()) {
                    std::vector<LockPtr> locks{pingcap::kv::extractLockFromKeyErr(resp.error())};
                    std::vector<uint64_t> pushed;
                    auto ms =
                        impl_->cluster->lock_resolver->resolveLocks(bo, version, locks, pushed);
                    if (ms > 0)
                        bo.backoffWithMaxSleep(pingcap::kv::boTxnLockFast, static_cast<int>(ms),
                                               Exception("last_key blocked by lock",
                                                         pingcap::ErrorCodes::LockError));
                    continue;
                }
                if (resp.pairs_size() == 0) break;  // no key in this region's intersection; try the previous region
                const auto& pair = resp.pairs(0);
                if (pair.has_error()) {
                    // Tail key locked (possibly an uncommitted insert): resolve, then rescan this region
                    std::vector<LockPtr> locks{pingcap::kv::extractLockFromKeyErr(pair.error())};
                    std::vector<uint64_t> pushed;
                    auto ms =
                        impl_->cluster->lock_resolver->resolveLocks(bo, version, locks, pushed);
                    if (ms > 0)
                        bo.backoffWithMaxSleep(pingcap::kv::boTxnLockFast, static_cast<int>(ms),
                                               Exception("last_key blocked by lock",
                                                         pingcap::ErrorCodes::LockError));
                    continue;
                }
                return pair.key();
            }
        }
        if (!topo_changed) return std::nullopt;
    }
}

namespace {

// Per-transaction size protection (docs/archive/gaps.md §2.12) — fail-fast before any RPC:
// - Single-value cap: TiKV raft-entry-max-size defaults to 8MiB; an over-limit prewrite is bound
//   to fail and **fails on every retry** (the object cannot be written, existing accounting
//   entries cannot be deleted). 2MiB headroom is left for proto wrapping.
// - Total/count caps: aligned with the magnitude of TiDB's txn-total-size-limit (100MB) and its
//   entry-count limit, stopping runaway batching (normal business transactions are far below).
// Throws pingcap::Exception: thrown before prewrite = definitively uncommitted; txn_retry
// callers treat it as InternalError (client-visible 500; the 400 semantics for object manifests
// are intercepted earlier by the meta layer at encoding time — this is the last line of defense)
constexpr uint64_t kMaxMutationValueBytes = 6ull << 20;
constexpr uint64_t kMaxTxnTotalBytes = 96ull << 20;
constexpr size_t kMaxTxnMutations = 300'000;

void check_txn_size(const std::vector<TikvMutation>& muts) {
    if (muts.size() > kMaxTxnMutations)
        throw Exception("tikv txn rejected: " + std::to_string(muts.size()) +
                            " mutations exceed limit " + std::to_string(kMaxTxnMutations),
                        pingcap::ErrorCodes::LogicalError);
    uint64_t total = 0;
    for (const auto& m : muts) {
        uint64_t sz = m.key.size() + m.value.size();
        if (m.value.size() > kMaxMutationValueBytes)
            throw Exception("tikv txn rejected: mutation value " +
                                std::to_string(m.value.size()) + " bytes exceeds limit " +
                                std::to_string(kMaxMutationValueBytes) +
                                " (raft entry cap; key " + m.key.substr(0, 64) + ")",
                            pingcap::ErrorCodes::LogicalError);
        total += sz;
    }
    if (total > kMaxTxnTotalBytes)
        throw Exception("tikv txn rejected: " + std::to_string(total) +
                            " total bytes exceed limit " + std::to_string(kMaxTxnTotalBytes),
                        pingcap::ErrorCodes::LogicalError);
}

}  // namespace

void TikvClient::commit(uint64_t start_ts, const std::vector<TikvMutation>& muts) {
    if (muts.empty()) return;
    check_txn_size(muts);
    Committer(impl_->cluster.get(), start_ts, muts, impl_->backoff_budget_ms).execute();
}

// ---------- GC safepoint (§7.3) ----------

uint64_t TikvClient::update_service_gc_safepoint(const std::string& service_id, int64_t ttl_s,
                                                 uint64_t safe_point) {
    ::pdpb::UpdateServiceGCSafePointRequest req;
    req.set_allocated_header(impl_->pd_header());
    req.set_service_id(service_id);
    req.set_ttl(ttl_s);
    req.set_safe_point(safe_point);
    auto resp = impl_->pd_call<::pdpb::UpdateServiceGCSafePointResponse>(
        "update_service_gc_safepoint",
        [&](grpc::ClientContext* ctx, ::pdpb::PD::Stub& stub,
            ::pdpb::UpdateServiceGCSafePointResponse* r) {
            return stub.UpdateServiceGCSafePoint(ctx, req, r);
        });
    return resp.min_safe_point();
}

uint64_t TikvClient::update_gc_safepoint(uint64_t safe_point) {
    ::pdpb::UpdateGCSafePointRequest req;
    req.set_allocated_header(impl_->pd_header());
    req.set_safe_point(safe_point);
    auto resp = impl_->pd_call<::pdpb::UpdateGCSafePointResponse>(
        "update_gc_safepoint",
        [&](grpc::ClientContext* ctx, ::pdpb::PD::Stub& stub,
            ::pdpb::UpdateGCSafePointResponse* r) { return stub.UpdateGCSafePoint(ctx, req, r); });
    return resp.new_safe_point();
}

uint64_t TikvClient::get_gc_safepoint() {
    // Bypasses client-c getGCSafePoint() (marked deprecated, and its exception path flips its
    // internal check_leader state); shares the same direct channel as the two update calls
    ::pdpb::GetGCSafePointRequest req;
    req.set_allocated_header(impl_->pd_header());
    auto resp = impl_->pd_call<::pdpb::GetGCSafePointResponse>(
        "get_gc_safepoint",
        [&](grpc::ClientContext* ctx, ::pdpb::PD::Stub& stub, ::pdpb::GetGCSafePointResponse* r) {
            return stub.GetGCSafePoint(ctx, req, r);
        });
    return resp.safe_point();
}

pingcap::kv::Cluster* TikvClient::cluster() { return impl_->cluster.get(); }

}  // namespace lights3::storage::duostore
