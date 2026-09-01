#include "storage/duostore/tikv_meta_store.h"

#include <charconv>

#include <pingcap/Exception.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <limits>
#include <map>
#include <set>
#include <thread>
#include <type_traits>

#include "core/log.h"
#include "storage/duostore/codec.h"
#include "storage/duostore/meta_util.h"
#include "storage/multipart.h"

namespace lights3::storage::duostore {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

// Counter kind chars (within the 'C' table): chunk / pack id segments, gcq seq and pack ledger delta row id
constexpr char kCtrChunk = '0';
constexpr char kCtrPack = '1';
constexpr char kCtrSeq = 'q';
constexpr char kCtrPackDelta = 'd';

// Fold threshold for pack ledger delta rows (§3.2): once a single pack has more delta
// rows than this, pack_stats() folds them into one row in passing — the low-frequency
// GC path bears the merge cost, keeping the business write path pure-write and conflict-free
constexpr size_t kPackFoldThreshold = 16;

// Delta row value: le64 live_bytes ‖ le64 live_recs (encode_counter_delta's 8B encoding x2)
std::string encode_pack_delta(int64_t bytes, int64_t recs) {
    return codec::encode_counter_delta(bytes) + codec::encode_counter_delta(recs);
}

std::pair<int64_t, int64_t> decode_pack_delta(std::string_view v) {
    if (v.size() != 16)
        throw S3Error(S3ErrorCode::InternalError, "duostore tikv meta: bad pack delta row");
    return {codec::decode_counter(v.substr(0, 8)), codec::decode_counter(v.substr(8, 8))};
}

// Conflict retry (§4.1, isomorphic to the Redis version's CAS loop): exponential
// backoff starting at 100µs, capped at 6.4ms, at most 16 attempts — exceeding that
// means pathological hotspot contention; failing loudly beats livelock
constexpr int kMaxTxnRetries = 16;

// Entries per page for paged scans (the Scanner batches 256 internally; this is our aggregation page)
constexpr size_t kScanPage = 1024;

int64_t now_ms() { return codec::to_unix_ms(std::chrono::system_clock::now()); }

void conflict_backoff(int attempt) {
    auto us = std::chrono::microseconds(100) * (1 << std::min(attempt, 6));
    std::this_thread::sleep_for(us);
}

// Single-value size guard for object manifests (docs/archive/gaps.md §2.12): a single TiKV
// value is bounded by the raft entry limit (8MiB by default); an over-limit
// PUT/complete would **fail permanently** and an existing such object could not be
// deleted. Fail-fast after encoding with EntityTooLarge (400, actionable: client
// switches to multipart / larger parts) — more honest than a 500 from prewrite
// repeatedly hitting the raft limit. The cap leaves 2MiB of headroom for proto
// wrapping; the extent-count cap blocks pathological manifests of the same magnitude
// (interleaved-id shapes that defeat run encoding, ≈ 30B/extent)
constexpr size_t kMaxObjectValueBytes = 6ull << 20;
constexpr size_t kMaxObjectExtents = 200'000;

void check_object_value(std::string_view k, size_t n_extents, size_t encoded_bytes) {
    if (n_extents <= kMaxObjectExtents && encoded_bytes <= kMaxObjectValueBytes) return;
    throw S3Error(S3ErrorCode::EntityTooLarge,
                  "duostore tikv meta: object manifest too large for a single TiKV value (" +
                      std::string(k) + ": " + std::to_string(n_extents) + " extents, " +
                      std::to_string(encoded_bytes) + " bytes)");
}

[[noreturn]] void throw_internal(const char* what, const std::string& detail) {
    LOG_ERROR("duostore tikv meta: {}: {}", what, detail);
    throw S3Error(S3ErrorCode::InternalError,
                  std::string("duostore tikv meta: ") + what + ": " + detail);
}

[[noreturn]] void throw_no_bucket(std::string_view b) {
    throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist",
                  std::string(b));
}

[[noreturn]] void throw_no_upload(std::string_view id) {
    throw S3Error(S3ErrorCode::NoSuchUpload, "The specified multipart upload does not exist.",
                  std::string(id));
}

// FNV-1a: guard shard hashing (§4.3). Only needs stable dispersion, not collision resistance
uint64_t fnv1a(std::string_view k) {
    uint64_t h = 1469598103934665603ull;
    for (unsigned char c : k) h = (h ^ c) * 1099511628211ull;
    return h;
}

}  // namespace

// ---------- Key construction (§3.2) ----------

std::string TikvMetaStore::tkey(char tag, std::string_view rest) const {
    std::string k;
    k.reserve(opt_.prefix.size() + 1 + rest.size());
    k += opt_.prefix;
    k += tag;
    k += rest;
    return k;
}

std::string TikvMetaStore::bucket_key(std::string_view b) const { return tkey('B', b); }

std::string TikvMetaStore::bucket_guard(std::string_view b, uint32_t shard) const {
    std::string rest(b);
    rest += '\0';
    rest += char(shard);
    return tkey('b', rest);
}

std::string TikvMetaStore::object_key(std::string_view b, std::string_view k) const {
    return tkey('O', codec::object_key(b, k));
}

std::string TikvMetaStore::upload_key(std::string_view b, std::string_view k,
                                      std::string_view id) const {
    return tkey('U', codec::upload_key(b, k, id));
}

std::string TikvMetaStore::upload_guard(std::string_view b, std::string_view k,
                                        std::string_view id, uint32_t shard) const {
    std::string rest = codec::upload_key(b, k, id);
    rest += '\0';
    rest += char(shard);
    return tkey('u', rest);
}

std::string TikvMetaStore::part_key(std::string_view b, std::string_view k, std::string_view id,
                                    int part_no) const {
    return tkey('P', codec::part_key(b, k, id, part_no));
}

std::string TikvMetaStore::refs_key(uint64_t file_id) const {
    return tkey('R', codec::be64_key(file_id));
}

std::string TikvMetaStore::gcq_key(uint64_t seq) const { return tkey('G', codec::be64_key(seq)); }

std::string TikvMetaStore::counter_key(char kind) const {
    return tkey('C', std::string_view(&kind, 1));
}

std::string TikvMetaStore::pack_delta_key(uint64_t pack_id, uint64_t delta_id) const {
    std::string rest = codec::be64_key(pack_id);
    rest += 'd';
    rest += codec::be64_key(delta_id);
    return tkey('S', rest);
}

std::string TikvMetaStore::pack_seal_key(uint64_t pack_id) const {
    std::string rest = codec::be64_key(pack_id);
    rest += 's';
    return tkey('S', rest);
}

std::pair<std::string, std::string> TikvMetaStore::range_of(char tag,
                                                            std::string_view rest) const {
    std::string lo = tkey(tag, rest);
    std::string hi = lo;
    codec::bump_last_byte(hi);  // last key byte is never 0xff (guaranteed by table tag / composite segment construction)
    return {std::move(lo), std::move(hi)};
}

// ---------- Transaction and read helpers ----------

// Unified exception translation (§6.4): S3Error passes through; Undetermined = outcome
// unknown (§4.6 blind-retry ban); remaining pingcap exceptions → InternalError
template <typename Fn>
auto TikvMetaStore::guarded(const char* what, Fn&& fn) {
    try {
        return fn();
    } catch (const S3Error&) {
        throw;
    } catch (const TikvUndetermined& u) {
        // Rethrown as a distinguishable type: the transaction may have taken effect;
        // callers must not fall back to physically deleting data (§4.6)
        LOG_ERROR("duostore tikv meta: {}: commit result undetermined: {}", what, u.what);
        throw UndeterminedCommit(std::string("duostore tikv meta: ") + what +
                                 ": commit result undetermined: " + u.what);
    } catch (const pingcap::Exception& e) {
        throw_internal(what, e.displayText());
    }
}

// Optimistic retry loop (§4.1): body(ts, muts) reads/computes on the start_ts snapshot
// and assembles the mutation batch; empty muts = nothing to commit (pure-read decision /
// idempotent early return). WriteConflict / prewrite lock contention over budget =
// definitely not committed → back off and retry; other exceptions are translated by guarded
template <typename Body>
auto TikvMetaStore::txn_retry(const char* what, Body&& body) {
    using R = std::invoke_result_t<Body&, uint64_t, std::vector<TikvMutation>&>;
    return guarded(what, [&]() -> R {
        for (int attempt = 0;; ++attempt) {
            uint64_t ts = client().get_ts();
            std::vector<TikvMutation> muts;
            // Commit succeeded (or pure-read decision with empty muts) → true; conflict backoff → false to retry
            auto committed = [&] {
                if (muts.empty()) return true;
                try {
                    client().commit(ts, muts);
                    return true;
                } catch (const TikvConflict& c) {
                    m_conflict_retries_->inc();  // count once per conflict retry round (T5 metric)
                    if (attempt + 1 >= kMaxTxnRetries)
                        throw_internal(what, "txn conflict storm: " + c.what);
                    conflict_backoff(attempt);
                    return false;
                }
            };
            if constexpr (std::is_void_v<R>) {
                body(ts, muts);
                if (committed()) return;
            } else {
                R r = body(ts, muts);
                if (committed()) return r;
            }
        }
    });
}

// ---------- Construction / shutdown ----------

TikvMetaStore::TikvMetaStore(TikvMetaOptions opt) : opt_(std::move(opt)) {
    // T5 metrics: registered before any network call — conflict retries during schema
    // init must count too; an empty scope returns isolated instances, so directly
    // constructed tests have zero wiring cost
    m_conflict_retries_ = opt_.metrics.counter(
        "lights3_duostore_tikv_txn_conflict_retries_total",
        "Optimistic txn retries after WriteConflict/lock contention (one per retry round)");
    m_safepoint_failures_ = opt_.metrics.counter(
        "lights3_duostore_tikv_safepoint_update_failures_total",
        "Failed GC safepoint update rounds (retried next tick)");
    m_safepoint_ms_ = opt_.metrics.gauge(
        "lights3_duostore_tikv_gc_safepoint_ms",
        "Cluster GC safepoint as of last successful push (unix ms, 0 until first push)");

    client_owned_ = std::make_unique<TikvClient>(TikvOptions{opt_.pd_endpoints, opt_.ca_path,
                                                             opt_.cert_path, opt_.key_path,
                                                             opt_.backoff_budget_ms});
    client_.store(client_owned_.get(), std::memory_order_release);
    // Schema lineage check (§3.2): Insert expresses "first creation only". Multiple
    // gateways first-starting on the same prefix is a supported, legitimate race —
    // conflict / key collision / unknown outcome (a constant idempotent write,
    // decidable by re-reading) all go through the backoff retry loop to converge;
    // one-shot-and-give-up is not allowed (other write paths get the same loop from
    // txn_retry). Version evolution (docs/archive/gaps.md §6.1): existing version < current
    // walks the migration chain (each step idempotent — the shared engine has no
    // global migration lock, and multiple new-version gateways walking the chain
    // concurrently are harmless to each other; racing stamp Puts converge to the same
    // value); > current refuses downgrade — after an upgrade, old-version gateways are
    // rejected at boot, blocking mixed deployments from writing back and corrupting
    // the new layout
    guarded("open schema", [&] {
        const std::string marker = "t" + std::to_string(kSchemaCurrent);
        std::string skey = tkey('s', {});
        for (int attempt = 0;; ++attempt) {
            auto ts = client().get_ts();
            if (auto v = snap_get(ts, skey)) {
                int64_t ver = parse_schema_marker(*v, /*lineage=*/"t", kSchemaCurrent,
                                                  "duostore tikv meta");
                if (ver == kSchemaCurrent) return;
                migrate_schema(ver);
                return;
            }
            try {
                client().commit(ts, {{TikvOp::kInsert, skey, marker}});
                return;
            } catch (const TikvAlreadyExist&) {  // concurrent first creation succeeded; read and verify next round
            } catch (const TikvConflict&) {  // concurrent first creation in progress
                m_conflict_retries_->inc();
            } catch (const TikvUndetermined&) {  // the write is a constant; re-read next round to decide
            }
            if (attempt + 1 >= kMaxTxnRetries)
                throw S3Error(S3ErrorCode::InternalError,
                              "duostore tikv meta: schema init did not converge");
            conflict_backoff(attempt);
        }
    });

    // GC safepoint worker (§7.3): pushes immediately on the first tick, then one round
    // per interval; failures are counted and retried next round (transients like PD
    // leader switches). interval=0 = disabled (directly constructed tests /
    // deployments sharing a TiDB cluster)
    if (opt_.gc_safepoint_interval_s > 0) {
        sp_thread_ = std::thread([this] {
            std::unique_lock lk(sp_mu_);
            while (!sp_stop_) {
                lk.unlock();
                try {
                    update_gc_safepoint_once();
                } catch (const std::exception& e) {
                    m_safepoint_failures_->inc();
                    LOG_WARN("duostore tikv meta: gc safepoint push failed (retry next tick): {}",
                             e.what());
                }
                lk.lock();
                sp_cv_.wait_for(lk, std::chrono::seconds(opt_.gc_safepoint_interval_s),
                                [this] { return sp_stop_; });
            }
        });
    }
}

// Migration chain (in-place transform from version n → n+1; each step idempotent —
// gateways walking the chain concurrently are harmless to each other). Register new
// layout changes here and bump kSchemaCurrent by 1 — "changing the layout without
// leaving a migration" is never allowed
void TikvMetaStore::migrate_schema(int64_t ver) {
    using MigrateFn = void (*)(TikvMetaStore&);
    static constexpr std::array<std::pair<int64_t, MigrateFn>, 0> kSchemaMigrations{
        // {{1, &migrate_v1_to_v2}}  // example: once registered, v1 stores auto-upgrade at boot
    };
    for (; ver < kSchemaCurrent; ++ver) {
        MigrateFn fn = nullptr;
        for (auto& [from, f] : kSchemaMigrations)
            if (from == ver) fn = f;
        if (!fn) throw_no_migration(ver, kSchemaCurrent, "duostore tikv meta");
        fn(*this);
        txn_retry("schema stamp", [&](uint64_t, std::vector<TikvMutation>& muts) {
            muts.push_back({TikvOp::kPut, tkey('s', {}), "t" + std::to_string(ver + 1)});
        });
        LOG_INFO("duostore tikv meta: schema migrated v{} -> v{}", ver, ver + 1);
    }
}

uint64_t TikvMetaStore::update_gc_safepoint_once() {
    // TSO = physical_ms << 18 | logical: retention is subtracted directly in the ts domain (ms << 18)
    uint64_t now = client().get_ts();
    uint64_t retention = (uint64_t(opt_.gc_retention_s) * 1000) << 18;
    uint64_t target = now > retention ? now - retention : 0;
    // 1) Register this gateway's service safepoint. The service id is shared per
    //    prefix: gateways overwriting each other is fine (all declare ~now−retention,
    //    still safe after PD takes the min); TTL is 3x interval — a gateway whose
    //    advancement stalls is auto-removed after two rounds, never dangling cluster GC
    int64_t ttl_s = std::max<int64_t>(3 * opt_.gc_safepoint_interval_s, 60);
    client().update_service_gc_safepoint("lights3-duostore:" + opt_.prefix, ttl_s, target);
    // 2) Take over TiDB's gc_worker role (the whole point of §7.3 for pure-KV
    //    deployments): PD permanently placeholds a missing gc_worker at the current
    //    cluster safepoint (infinite TTL) — without pushing it, the min stays pinned
    //    forever. PD forces infinite TTL for the gc_worker entry. Clusters shared with
    //    TiDB must disable this pusher (interval=0), otherwise it races writes with
    //    the real gc_worker (harmless under monotonic semantics but pointless)
    uint64_t min_sp = client().update_service_gc_safepoint(
        "gc_worker", std::numeric_limits<int64_t>::max(), target);
    // 3) Push the cluster safepoint with the min: the min covers all live services
    //    (including external ones like BR/CDC), never passing any service's declared
    //    snapshot; PD is monotonic forward-only and returns the current value
    //    unchanged for lagging inputs — concurrent advancement by multiple gateways
    //    naturally converges
    uint64_t cluster_sp = client().update_gc_safepoint(min_sp);
    m_safepoint_ms_->set(int64_t(cluster_sp >> 18));
    return cluster_sp;
}

TikvMetaStore::~TikvMetaStore() {
    try {
        close();
    } catch (const std::exception& e) {
        LOG_ERROR("duostore tikv meta: close in dtor failed: {}", e.what());
    }
}

void TikvMetaStore::close() {
    // Stop the safepoint worker first: it gets its handle via client(); detaching the
    // handle before stopping the thread would turn the normal exit path into a 500 throw
    {
        std::lock_guard lk(sp_mu_);
        sp_stop_ = true;
    }
    sp_cv_.notify_all();
    if (sp_thread_.joinable()) sp_thread_.join();
    // Then detach the handle, then destruct: calls after close deterministically
    // throw 500 at client() (same shape as the rocks version); Cluster destruction
    // stops background threads and must happen after there are no in-flight calls
    // (guaranteed by DuoStoreBackend's close ordering)
    TikvClient* c = client_.exchange(nullptr, std::memory_order_acq_rel);
    if (!c) return;
    client_owned_.reset();
}

TikvClient& TikvMetaStore::client() {
    TikvClient* c = client_.load(std::memory_order_acquire);
    if (!c) throw S3Error(S3ErrorCode::InternalError, "duostore tikv meta: store is closed");
    return *c;
}

std::optional<std::string> TikvMetaStore::snap_get(uint64_t ver, const std::string& key) {
    // Pure reads retry once (§6.4): client-c already backs off internally; this only catches one transient hiccup
    try {
        return client().get(ver, key);
    } catch (const pingcap::Exception& e) {
        LOG_WARN("duostore tikv meta: get retry after: {}", e.displayText());
        return client().get(ver, key);
    }
}

std::vector<std::optional<std::string>> TikvMetaStore::snap_get_many(
    uint64_t ver, const std::vector<std::string>& keys) {
    try {
        return client().batch_get(ver, keys);
    } catch (const pingcap::Exception& e) {
        LOG_WARN("duostore tikv meta: batch_get retry after: {}", e.displayText());
        return client().batch_get(ver, keys);
    }
}

template <typename Fn>
void TikvMetaStore::scan_range(uint64_t ver, std::string lo, const std::string& hi, Fn&& cb) {
    for (;;) {
        auto page = client().scan(ver, lo, hi, kScanPage);
        for (auto& kv : page)
            if (!cb(kv.first, kv.second)) return;
        if (page.size() < kScanPage) return;
        lo = page.back().first + '\0';  // byte-order successor, seamless page continuation
    }
}

void TikvMetaStore::mut_refs(std::vector<TikvMutation>& muts, const DataRef& ref, bool add,
                             std::string_view owner) {
    for (const auto& e : ref.extents) {
        if (e.kind == Extent::Kind::kPack) continue;  // pack liveness goes through the stats ledger (P2)
        if (add)
            muts.push_back({TikvOp::kPut, refs_key(e.file_id), std::string(owner)});
        else
            muts.push_back({TikvOp::kDel, refs_key(e.file_id), {}});
    }
}

void TikvMetaStore::mut_pack_delta(std::vector<TikvMutation>& muts, const DataRef& ref,
                                   int sign, int64_t rec_overhead) {
    // Aggregate multiple extents of the same pack first; one unique delta row per pack
    // (id pre-dispatched, the ledger entry is pure-write — read-modify-write of a
    // shared ledger row would make concurrent small-object PUT prewrites on the same
    // active pack conflict, §3.2)
    std::map<uint64_t, std::pair<int64_t, int64_t>> agg;  // pack_id -> (bytes, recs)
    for (const auto& e : ref.extents) {
        if (e.kind != Extent::Kind::kPack) continue;
        auto& [bytes, recs] = agg[e.file_id];
        bytes += sign * (int64_t(e.length) + rec_overhead);  // header overhead on the same accounting basis (§2.3a)
        recs += sign;
    }
    for (const auto& [id, d] : agg) {
        uint64_t delta_id = alloc_id(kCtrPackDelta, pack_deltas_);
        muts.push_back(
            {TikvOp::kPut, pack_delta_key(id, delta_id), encode_pack_delta(d.first, d.second)});
    }
}

void TikvMetaStore::enqueue_reclaim(std::vector<TikvMutation>& muts, const DataRef& ref,
                                    ReclaimReason reason) {
    if (ref.extents.empty()) return;
    // Oversized DataRefs are split into multiple entries (docs/archive/gaps.md §2.11): keeps
    // GC per-batch decode memory bounded and a single gcq value away from the raft
    // entry limit; acks are per-entry independent and unlink is idempotent, so
    // splitting does not change crash semantics
    const int64_t ts = now_ms();
    for (size_t i = 0; i < ref.extents.size(); i += kReclaimMaxExtents) {
        size_t n = std::min(kReclaimMaxExtents, ref.extents.size() - i);
        Reclaim r;
        r.extents.assign(ref.extents.begin() + i, ref.extents.begin() + i + n);
        r.reason = reason;
        uint64_t seq = alloc_id(kCtrSeq, seqs_);  // pre-dispatched (independent small txn); the ledger entry stays pure-write
        muts.push_back({TikvOp::kPut, gcq_key(seq), codec::encode_reclaim(r, ts)});
    }
}

uint64_t TikvMetaStore::alloc_id(char kind, IdRange& r, uint32_t n) {
    n = std::clamp<uint32_t>(n, 1, kMaxIdRun);  // run ≤ kMaxIdRun << kIdSegment
    {
        std::lock_guard lk(alloc_mu_);  // common path: pure in-memory next += n
        if (r.limit - r.next >= n) {
            uint64_t first = r.next;
            r.next += n;
            return first;
        }
    }
    // Segment exhausted: the counter RMW small transaction (§5) runs outside the
    // lock — TSO + 2PC + conflict retries can take tens of ms, and doing it inside
    // the lock would serialize and stall dispatch for all kinds at once. Concurrent
    // renewers are arbitrated by WriteConflict and each get disjoint segments; the
    // loser discarding its whole segment is harmless (ids only need to be unique and
    // monotonic, not contiguous). Raft-majority durable, so no crash-rollback
    // compensation like the Redis version
    std::string ck = counter_key(kind);
    uint64_t hi;
    try {
        hi = txn_retry(
            "reserve id segment",
            [&](uint64_t ts, std::vector<TikvMutation>& muts) -> uint64_t {
                uint64_t cur = 0;
                if (auto v = snap_get(ts, ck)) cur = uint64_t(codec::decode_counter(*v));
                uint64_t next_hi = cur + kIdSegment;
                muts.push_back(
                    {TikvOp::kPut, ck, codec::encode_counter_delta(int64_t(next_hi))});
                return next_hi;
            });
    } catch (const UndeterminedCommit& u) {
        // An "outcome unknown" on the id-segment counter transaction only means a
        // segment of ids may be burned (holes are harmless), while the **outer
        // business transaction is definitely not committed** — this function is
        // called during mutation assembly, before the business commit happens.
        // Passing it through as-is would make commit_or_discard treat it as
        // "business commit outcome unknown" and refuse to clean up already-written
        // data extents, creating needless orphans (docs/archive/gaps.md §3.9). Downgrade to
        // a deterministic failure
        throw S3Error(S3ErrorCode::InternalError,
                      std::string("duostore tikv meta: id segment reservation failed "
                                  "(business txn not committed): ") +
                          u.what());
    }
    std::lock_guard lk(alloc_mu_);
    if (r.limit - r.next < n) {  // if someone else renewed and it suffices, use theirs; discard ours (holes harmless, as above)
        r.limit = hi;
        r.next = hi - kIdSegment;
    }
    uint64_t first = r.next;
    r.next += n;
    return first;
}

uint64_t TikvMetaStore::alloc_file_run(Extent::Kind kind, uint32_t n) {
    // kRados and kChunk share a segment (same argument as the rocks version: refs are
    // accounted by raw file_id regardless of kind)
    if (kind == Extent::Kind::kRados) kind = Extent::Kind::kChunk;
    return alloc_id(kind == Extent::Kind::kChunk ? kCtrChunk : kCtrPack,
                    file_ids_[size_t(kind)], n);
}

// ---------- bucket ----------

void TikvMetaStore::create_bucket(std::string_view b) {
    try {
        txn_retry("create_bucket", [&](uint64_t, std::vector<TikvMutation>& muts) {
            // Insert expresses "must not exist" at the protocol level (§4.4) — no read needed; a key collision is a structured rejection
            muts.push_back({TikvOp::kInsert, bucket_key(b), codec::encode_bucket(now_ms())});
        });
    } catch (const TikvAlreadyExist&) {
        throw S3Error(S3ErrorCode::BucketAlreadyOwnedByYou, "Bucket already exists",
                      std::string(b));
    }
}

void TikvMetaStore::delete_bucket(std::string_view b) {
    txn_retry("delete_bucket", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        if (!snap_get(ts, bucket_key(b))) throw_no_bucket(b);
        // Emptiness check (same snapshot): reject if either objects or uploads is
        // non-empty (matching AWS; the uploads check also seals off "put_part reviving
        // a ghost upload after bucket deletion", same argument as the rocks version)
        for (char tag : {'O', 'U'}) {
            auto [lo, hi] = range_of(tag, std::string(b) + '\0');
            if (!client().scan(ts, lo, hi, 1).empty())
                throw S3Error(S3ErrorCode::BucketNotEmpty,
                              "The bucket you tried to delete is not empty", std::string(b));
        }
        // Del the bucket + all guard shards: forms a write-write conflict with the
        // guard Locks of concurrent put_object/create_upload, materializing the
        // emptiness check's write skew (§4.3)
        muts.push_back({TikvOp::kDel, bucket_key(b), {}});
        for (uint32_t s = 0; s < kGuardShards; ++s)
            muts.push_back({TikvOp::kLock, bucket_guard(b, s), {}});
    });
}

bool TikvMetaStore::bucket_exists(std::string_view b) {
    return guarded("bucket_exists",
                   [&] { return snap_get(client().get_ts(), bucket_key(b)).has_value(); });
}

std::vector<BucketInfo> TikvMetaStore::list_buckets() {
    return guarded("list_buckets", [&] {
        std::vector<BucketInfo> out;
        auto ts = client().get_ts();
        auto [lo, hi] = range_of('B', {});
        size_t plen = lo.size();  // prefix + 'B'
        scan_range(ts, lo, hi, [&](const std::string& k, const std::string& v) {
            out.push_back({k.substr(plen), codec::from_unix_ms(codec::decode_bucket(v))});
            return true;
        });
        return out;  // key byte order is lexicographic order
    });
}

// ---------- object ----------

std::optional<ObjectRec> TikvMetaStore::get_object(std::string_view b, std::string_view k) {
    return guarded("get_object", [&]() -> std::optional<ObjectRec> {
        auto v = snap_get(client().get_ts(), object_key(b, k));
        if (!v) return std::nullopt;
        return codec::decode_object(std::string(k), *v);
    });
}

std::optional<ObjectMeta> TikvMetaStore::head_object(std::string_view b, std::string_view k) {
    return guarded("head_object", [&]() -> std::optional<ObjectMeta> {
        auto v = snap_get(client().get_ts(), object_key(b, k));
        if (!v) return std::nullopt;
        return codec::decode_object_meta(std::string(k), *v);
    });
}

void TikvMetaStore::put_object(std::string_view b, std::string_view k, ObjectRec rec,
                               PutCondition cond) {
    txn_retry("put_object", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        std::string okey = object_key(b, k);
        auto vals = snap_get_many(ts, {bucket_key(b), okey});  // read all preconditions in one round trip
        if (!vals[0]) throw_no_bucket(b);
        std::optional<ObjectRec> old;
        if (vals[1]) old = codec::decode_object(std::string(k), *vals[1]);
        // Snapshot read + write-write conflict detection at commit: a concurrent
        // overwrite makes this transaction conflict-retry, and the retry round
        // re-checks on a fresh snapshot — check and commit are externally atomic
        // (the PutCondition contract). When this throws, muts has issued no RPC
        check_put_condition(cond, old, k);
        rec.version = old ? old->version + 1 : 1;

        // primary = the object key (semantic focus of write-write conflicts); guard
        // Lock materializes the bucket existence check (§4.3)
        std::string oval = codec::encode_object(rec);
        check_object_value(k, rec.data.extents.size(), oval.size());  // §2.12 fail-fast
        muts.push_back({TikvOp::kPut, okey, std::move(oval)});
        muts.push_back({TikvOp::kLock, bucket_guard(b, uint32_t(fnv1a(k) % kGuardShards)), {}});
        mut_refs(muts, rec.data, /*add=*/true, okey);
        const int64_t ov = codec::pack_rec_overhead(b, k);
        mut_pack_delta(muts, rec.data, +1, ov);
        if (old) {
            enqueue_reclaim(muts, old->data, ReclaimReason::kOverwrite);
            mut_refs(muts, old->data, /*add=*/false, {});
            mut_pack_delta(muts, old->data, -1, ov);
        }
    });
}

bool TikvMetaStore::delete_object(std::string_view b, std::string_view k) {
    return txn_retry("delete_object", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        std::string okey = object_key(b, k);
        auto vals = snap_get_many(ts, {bucket_key(b), okey});
        if (!vals[0]) throw_no_bucket(b);
        if (!vals[1]) return false;  // idempotent: muts empty, no transaction sent
        auto old = codec::decode_object(std::string(k), *vals[1]);
        muts.push_back({TikvOp::kDel, okey, {}});
        enqueue_reclaim(muts, old.data, ReclaimReason::kDelete);
        mut_refs(muts, old.data, /*add=*/false, {});
        mut_pack_delta(muts, old.data, -1, codec::pack_rec_overhead(b, k));
        return true;
    });
}

// §3.3: Snapshot consistent view + paged scan, algorithm copied from the rocks version
// (main doc §4.4); delimiter groups skip the whole group by bumping the last byte and
// re-seeking (one RTT per group). There is no reverse-scan primitive for the group-tail
// token, so it is deferred: only when truncation lands exactly on a group is one
// forward scan done on that group to fetch its tail key (at most once per list; groups
// are usually far smaller than the bucket)
ListResult TikvMetaStore::list_objects(std::string_view b, const ListOptions& opt) {
    return guarded("list_objects", [&] {
        ListResult out;
        uint64_t ts = client().get_ts();  // consistent view for the whole list (across pages/group skips)
        if (!snap_get(ts, bucket_key(b))) throw_no_bucket(b);
        if (opt.max_keys <= 0) return out;  // S3: max-keys=0 returns empty and not truncated

        auto [base, upper] = range_of('O', std::string(b) + '\0');
        const std::string& prefix = opt.prefix;
        const std::string& delim = opt.delimiter;

        // Simple cursor: page buffer + continuation (delimiter needs to change the seek point at will; a callback style doesn't fit)
        std::vector<std::pair<std::string, std::string>> page;
        size_t idx = 0;
        bool eof = false;
        auto seek = [&](const std::string& from) {
            page = client().scan(ts, from, upper, kScanPage);
            idx = 0;
            eof = page.empty();
        };
        auto advance = [&] {
            if (++idx < page.size()) return;
            if (page.size() < kScanPage) {
                eof = true;
                return;
            }
            seek(page.back().first + '\0');
        };

        seek(base + std::max(prefix, opt.start_after));
        if (!opt.start_after.empty() && !eof && page[idx].first == base + opt.start_after)
            advance();  // start_after matched itself, step once more

        std::string last_emitted;
        std::string last_group;  // non-empty = the most recent output was a delimiter group
        int count = 0;
        while (!eof) {
            std::string_view uk(page[idx].first);
            uk.remove_prefix(base.size());
            if (uk.compare(0, prefix.size(), prefix) != 0) break;  // stop once outside the prefix range
            if (count >= opt.max_keys) {
                out.is_truncated = true;
                if (!last_group.empty()) {
                    // Truncation landed exactly on a group: key-only reverse scan for
                    // the group tail (counterpart of the rocks version's SeekForPrev,
                    // O(1) RPCs, does not scale with group size)
                    std::string glo = base + last_group;
                    std::string ghi = glo;
                    codec::bump_last_byte(ghi);
                    if (auto tail = client().last_key(ts, glo, ghi))
                        last_emitted = tail->substr(base.size());
                }
                out.next_token = last_emitted;
                break;
            }
            if (!delim.empty()) {
                auto pos = uk.find(delim, prefix.size());
                if (pos != std::string_view::npos) {
                    std::string group(uk.substr(0, pos + delim.size()));
                    out.common_prefixes.push_back(group);
                    last_group = group;
                    ++count;
                    std::string target = base + group;
                    if (!codec::bump_last_byte(target)) break;
                    seek(target);  // skip the whole group (same snapshot, consistency intact)
                    continue;
                }
            }
            out.objects.push_back(codec::decode_object_meta(std::string(uk), page[idx].second));
            last_emitted = std::string(uk);
            last_group.clear();
            ++count;
            advance();
        }
        return out;
    });
}

// ---------- multipart ----------

std::string TikvMetaStore::create_upload(std::string_view b, std::string_view k,
                                         ObjectMeta meta) {
    UploadRec rec;
    rec.upload_id = new_upload_id();
    rec.meta = std::move(meta);
    rec.meta.key = std::string(k);
    rec.initiated_ms = now_ms();
    txn_retry("create_upload", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        if (!snap_get(ts, bucket_key(b))) throw_no_bucket(b);
        muts.push_back(
            {TikvOp::kPut, upload_key(b, k, rec.upload_id), codec::encode_upload(rec)});
        // Guard: materializes the write skew of delete_bucket's emptiness check (§4.3, same as put_object)
        muts.push_back({TikvOp::kLock, bucket_guard(b, uint32_t(fnv1a(k) % kGuardShards)), {}});
    });
    return rec.upload_id;
}

UploadRec TikvMetaStore::require_upload(std::string_view b, std::string_view k,
                                        std::string_view id) {
    return guarded("require_upload", [&] {
        if (!is_valid_upload_id(id)) throw_no_upload(id);
        auto v = snap_get(client().get_ts(), upload_key(b, k, id));
        if (!v) throw_no_upload(id);
        return codec::decode_upload(std::string(k), std::string(id), *v);
    });
}

void TikvMetaStore::put_part(std::string_view b, std::string_view k, std::string_view id,
                             PartRec p) {
    txn_retry("put_part", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        if (!is_valid_upload_id(id)) throw_no_upload(id);
        std::string pkey = part_key(b, k, id, p.part_no);
        auto vals = snap_get_many(ts, {upload_key(b, k, id), pkey});
        if (!vals[0]) throw_no_upload(id);
        std::optional<PartRec> old;
        if (vals[1]) old = codec::decode_part(p.part_no, *vals[1]);

        muts.push_back({TikvOp::kPut, pkey, codec::encode_part(p)});
        // Guard: materializes the write skew when complete/abort deletes the upload
        // (§4.3); sharded by part_no, so concurrent uploads of different part numbers
        // within the same upload don't block each other (1/16 false-collision
        // probability, retries are cheap)
        muts.push_back(
            {TikvOp::kLock, upload_guard(b, k, id, uint32_t(p.part_no) % kGuardShards), {}});
        mut_refs(muts, p.data, /*add=*/true, pkey);
        const int64_t ov = codec::pack_rec_overhead_part(b, k, id, p.part_no);
        mut_pack_delta(muts, p.data, +1, ov);
        if (old) {  // same-number re-upload is last-write-wins: the old part enters the GC ledger in the same batch
            enqueue_reclaim(muts, old->data, ReclaimReason::kPartOverwrite);
            mut_refs(muts, old->data, /*add=*/false, {});
            mut_pack_delta(muts, old->data, -1, ov);
        }
    });
}

std::vector<PartRec> TikvMetaStore::scan_parts(uint64_t ver, std::string_view b,
                                               std::string_view k, std::string_view id) {
    std::vector<PartRec> out;
    auto [lo, hi] = range_of('P', codec::parts_prefix(b, k, id));
    size_t plen = opt_.prefix.size() + 1;
    scan_range(ver, lo, hi, [&](const std::string& key, const std::string& v) {
        int no = codec::part_no_of_key(std::string_view(key).substr(plen));
        out.push_back(codec::decode_part(no, v));
        return true;
    });
    return out;  // be16 part_no guarantees ascending order
}

std::vector<PartRec> TikvMetaStore::list_parts(std::string_view b, std::string_view k,
                                               std::string_view id) {
    return guarded("list_parts", [&] {
        uint64_t ts = client().get_ts();
        if (!is_valid_upload_id(id) || !snap_get(ts, upload_key(b, k, id))) throw_no_upload(id);
        return scan_parts(ts, b, k, id);  // same snapshot: upload check consistent with parts read
    });
}

std::vector<UploadInfo> TikvMetaStore::list_uploads(std::string_view b,
                                                   std::string_view key_marker,
                                                   std::string_view id_marker, int limit,
                                                   std::string_view prefix) {
    return guarded("list_uploads", [&] {
        uint64_t ts = client().get_ts();
        if (!snap_get(ts, bucket_key(b))) throw_no_bucket(b);
        std::vector<UploadInfo> out;
        auto [lo, hi] = range_of('U', std::string(b) + '\0');
        size_t plen = lo.size();  // prefix + 'U' + b + '\0'
        // Cursor pushdown (docs/archive/gaps.md §5.1): key order is (key, upload_id) order, so
        // raising the lower bound of [lo,hi) skips the whole segment. The trailing
        // '\0' places the lower bound just past that pair; key-marker-only means
        // "key > key_marker" (keys contain no NUL, so key_marker+'\x01' is the smallest
        // greater key). The prefix raises the bound further (roadmap §3.5)
        std::string from = lo;
        if (!id_marker.empty()) {
            from += std::string(key_marker);
            from += '\0';
            from += std::string(id_marker);
            from += '\0';
        } else if (!key_marker.empty()) {
            from += std::string(key_marker);
            from += '\x01';
        }
        if (std::string pf = lo + std::string(prefix); pf > from) from = std::move(pf);
        if (from > lo) lo = std::move(from);
        scan_range(ts, lo, hi, [&](const std::string& key, const std::string& v) {
            if (limit > 0 && out.size() >= size_t(limit)) return false;
            // rest = <key>\0<upload_id>; the prefix scan is naturally sorted by (key, upload_id)
            std::string_view rest = std::string_view(key).substr(plen);
            if (rest.substr(0, prefix.size()) != prefix) return false;  // past the prefix range
            auto sep = rest.rfind('\0');
            if (sep == std::string_view::npos) return true;
            auto rec = codec::decode_upload(std::string(rest.substr(0, sep)),
                                            std::string(rest.substr(sep + 1)), v);
            out.push_back({rec.meta.key, rec.upload_id, codec::from_unix_ms(rec.initiated_ms)});
            return true;
        });
        return out;
    });
}

// §8 (main doc): complete is a pure metadata transaction, zero data movement. All
// parts enter the write set (Del one by one) → conflicts with concurrent same-number
// put_part are naturally checked by prewrite; new-number put_part is materialized by
// the Ug guard
std::string TikvMetaStore::complete_upload(std::string_view b, std::string_view k,
                                           std::string_view id,
                                           std::span<const PartInfo> parts) {
    return txn_retry("complete_upload", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        if (!is_valid_upload_id(id)) throw_no_upload(id);
        std::string okey = object_key(b, k);
        auto vals = snap_get_many(ts, {upload_key(b, k, id), okey});
        if (!vals[0]) throw_no_upload(id);
        auto up = codec::decode_upload(std::string(k), std::string(id), *vals[0]);

        std::map<int, PartRec> stored;
        for (auto& p : scan_parts(ts, b, k, id)) stored.emplace(p.part_no, std::move(p));
        std::set<int> selected;
        ObjectRec rec = assemble_completed_object(std::move(up.meta), parts, stored, selected);

        std::optional<ObjectRec> old;
        if (vals[1]) old = codec::decode_object(std::string(k), *vals[1]);
        rec.version = old ? old->version + 1 : 1;

        std::string oval = codec::encode_object(rec);
        check_object_value(k, rec.data.extents.size(), oval.size());  // §2.12 fail-fast
        muts.push_back({TikvOp::kPut, okey, std::move(oval)});  // primary
        muts.push_back({TikvOp::kDel, upload_key(b, k, id), {}});
        for (uint32_t s = 0; s < kGuardShards; ++s)
            muts.push_back({TikvOp::kLock, upload_guard(b, k, id, s), {}});
        for (const auto& [no, p] : stored) {
            muts.push_back({TikvOp::kDel, part_key(b, k, id, no), {}});
            if (selected.count(no)) {
                // refs transfer: owner rewritten to the object. Pack ledger liveness
                // is unchanged, but the accounting basis is rebalanced from part to
                // object (-part header overhead +object header overhead, recs cancel
                // out): guarantees a later object delete, deducting on the object
                // basis, zeroes the ledger exactly
                mut_refs(muts, p.data, /*add=*/true, okey);
                mut_pack_delta(muts, p.data, -1,
                               codec::pack_rec_overhead_part(b, k, id, no));
                mut_pack_delta(muts, p.data, +1, codec::pack_rec_overhead(b, k));
            } else {  // unselected parts enter the GC ledger
                enqueue_reclaim(muts, p.data, ReclaimReason::kComplete);
                mut_refs(muts, p.data, /*add=*/false, {});
                mut_pack_delta(muts, p.data, -1,
                               codec::pack_rec_overhead_part(b, k, id, no));
            }
        }
        if (old) {  // the old same-name object enters the GC ledger
            enqueue_reclaim(muts, old->data, ReclaimReason::kOverwrite);
            mut_refs(muts, old->data, /*add=*/false, {});
            mut_pack_delta(muts, old->data, -1, codec::pack_rec_overhead(b, k));
        }
        return rec.meta.etag;
    });
}

void TikvMetaStore::abort_upload(std::string_view b, std::string_view k, std::string_view id) {
    txn_retry("abort_upload", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        if (!is_valid_upload_id(id) || !snap_get(ts, upload_key(b, k, id))) throw_no_upload(id);
        muts.push_back({TikvOp::kDel, upload_key(b, k, id), {}});  // primary
        for (uint32_t s = 0; s < kGuardShards; ++s)
            muts.push_back({TikvOp::kLock, upload_guard(b, k, id, s), {}});
        for (const auto& p : scan_parts(ts, b, k, id)) {
            muts.push_back({TikvOp::kDel, part_key(b, k, id, p.part_no), {}});
            enqueue_reclaim(muts, p.data, ReclaimReason::kAbort);
            mut_refs(muts, p.data, /*add=*/false, {});
            mut_pack_delta(muts, p.data, -1,
                           codec::pack_rec_overhead_part(b, k, id, p.part_no));
        }
    });
}

// ---------- GC accounting ----------

std::vector<std::pair<uint64_t, Reclaim>> TikvMetaStore::peek_reclaims(size_t max,
                                                                       uint64_t min_seq,
                                                                       size_t max_extents) {
    return guarded("peek_reclaims", [&] {
        std::vector<std::pair<uint64_t, Reclaim>> out;
        uint64_t ts = client().get_ts();
        auto [lo, hi] = range_of('G', {});
        (void)lo;  // start from gcq_key(min_seq): with min_seq=0 that is the head of the 'G' segment, equivalent to lo
        size_t plen = opt_.prefix.size() + 1;
        size_t extents = 0;
        for (auto& [key, v] : client().scan(ts, gcq_key(min_seq), hi, max)) {
            uint64_t seq = codec::parse_be64(std::string_view(key).substr(plen));
            out.emplace_back(seq, codec::decode_reclaim(v));
            // Cumulative extent cap (gaps §2.11): returns at least 1 item (over-scanned kv discarded in place)
            extents += out.back().second.extents.size();
            if (extents >= max_extents) break;
        }
        return out;
    });
}

void TikvMetaStore::ack_reclaim(uint64_t seq) {
    txn_retry("ack_reclaim", [&](uint64_t, std::vector<TikvMutation>& muts) {
        muts.push_back({TikvOp::kDel, gcq_key(seq), {}});  // blind delete of a single key, no cross-key invariants
    });
}

// Multi-gateway GC lease (docs/archive/gaps.md §6.1): value = "<owner>\0<expiry_ms>".
// Snapshot-read decision + prewrite conflict detection acts as a CAS
// (read-then-commit, §4.1) — of two instances racing to claim, only one commit
// succeeds; the loser retries, reads the new lease, and backs out. TTL expiry is
// judged by wall clock (inter-gateway clock skew should be far smaller than the TTL;
// the lease already requires ttl ≫ a single GC round's duration)
bool TikvMetaStore::try_gc_lease(std::string_view owner, int64_t ttl_ms) {
    return txn_retry("try_gc_lease", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        std::string lk = tkey('L', "gc");
        const int64_t now = now_ms();
        if (auto v = snap_get(ts, lk)) {
            auto nul = v->find('\0');
            if (nul != std::string::npos) {
                std::string_view cur(v->data(), nul);
                int64_t expiry = 0;
                std::from_chars(v->data() + nul + 1, v->data() + v->size(), expiry);
                if (cur != owner && expiry > now) return false;  // held by someone else and not expired
            }
        }
        std::string val(owner);
        val += '\0';
        val += std::to_string(now + ttl_ms);
        muts.push_back({TikvOp::kPut, lk, std::move(val)});
        return true;
    });
}

void TikvMetaStore::ack_reclaims(std::span<const uint64_t> seqs) {
    if (seqs.empty()) return;
    txn_retry("ack_reclaims", [&](uint64_t, std::vector<TikvMutation>& muts) {
        for (uint64_t s : seqs) muts.push_back({TikvOp::kDel, gcq_key(s), {}});
    });
}

std::vector<PackStat> TikvMetaStore::pack_stats() {
    // 'S' table prefix scan (a pack's delta/seal rows are adjacent: 'd' < 's'),
    // aggregating as it scans. Folds in passing (the other half of the §3.2 delta-row
    // scheme): once a single pack's delta rows exceed the threshold they are merged
    // into one row — the low-frequency GC path bears the merge, keeping the business
    // write path pure-write and conflict-free
    struct Acc {
        PackStat ps;
        std::vector<std::string> delta_keys;  // fold candidates
    };
    std::vector<Acc> accs;
    guarded("pack_stats", [&] {
        uint64_t ts = client().get_ts();
        auto [lo, hi] = range_of('S', {});
        size_t plen = opt_.prefix.size() + 1;  // prefix + 'S'
        scan_range(ts, lo, hi, [&](const std::string& key, const std::string& v) {
            std::string_view rest = std::string_view(key).substr(plen);
            if (rest.size() < 9) return true;  // not our format, skip
            uint64_t id = codec::parse_be64(rest.substr(0, 8));
            if (accs.empty() || accs.back().ps.pack_id != id) {
                accs.emplace_back();
                accs.back().ps.pack_id = id;
            }
            Acc& a = accs.back();
            if (rest[8] == 'd') {
                auto [bytes, recs] = decode_pack_delta(v);
                a.ps.live_bytes += bytes;
                a.ps.live_recs += recs;
                a.delta_keys.push_back(key);
            } else if (rest[8] == 's') {
                a.ps.sealed = true;
                a.ps.file_size = uint64_t(codec::decode_counter(v));
            }
            return true;
        });
    });
    for (Acc& a : accs) {
        if (a.delta_keys.size() <= kPackFoldThreshold) continue;
        // Fold into one row: delete the delta rows already read + write a merged row
        // (new delta_id). Concurrent business transactions only add rows with other
        // keys, no conflict; concurrent folds (multiple gateways) are arbitrated by
        // txn_retry's write-write conflict — the loser re-reads and recomputes,
        // converging without double-counting
        try {
            txn_retry("fold pack stats", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
                int64_t bytes = 0, recs = 0;
                for (const auto& dk : a.delta_keys) {
                    auto v = snap_get(ts, dk);
                    if (!v) {  // someone else already folded: give up (clear muts to avoid a half-way commit losing ledger entries)
                        muts.clear();
                        return;
                    }
                    auto [db, dr] = decode_pack_delta(*v);
                    bytes += db;
                    recs += dr;
                    muts.push_back({TikvOp::kDel, dk, {}});
                }
                uint64_t delta_id = alloc_id(kCtrPackDelta, pack_deltas_);
                muts.push_back({TikvOp::kPut, pack_delta_key(a.ps.pack_id, delta_id),
                                encode_pack_delta(bytes, recs)});
            });
        } catch (const std::exception& e) {
            LOG_WARN("duostore tikv meta: pack {} fold skipped: {}", a.ps.pack_id, e.what());
        }
    }
    std::vector<PackStat> out;
    out.reserve(accs.size());
    for (auto& a : accs) out.push_back(a.ps);
    return out;
}

void TikvMetaStore::seal_pack(uint64_t pack_id, uint64_t file_size) {
    txn_retry("seal_pack", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        // Idempotent; file_size=0 does not overwrite an existing record (IMetaStore contract)
        std::string skey = pack_seal_key(pack_id);
        if (file_size == 0 && snap_get(ts, skey)) return;  // muts empty, no transaction sent
        muts.push_back({TikvOp::kPut, skey, codec::encode_counter_delta(int64_t(file_size))});
    });
}

void TikvMetaStore::drop_pack_stat(uint64_t pack_id) {
    txn_retry("drop_pack_stat", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        // Delete all ledger rows of this pack (delta + seal). Precondition:
        // live_recs==0 and the pack is already deleted, so no more concurrent
        // appends — what the snapshot reads is the complete set
        auto [lo, hi] = range_of('S', codec::be64_key(pack_id));
        scan_range(ts, lo, hi, [&](const std::string& key, const std::string&) {
            muts.push_back({TikvOp::kDel, key, {}});
            return true;
        });
    });
}

bool TikvMetaStore::swap_extents(std::string_view b, std::string_view k,
                                 uint64_t expect_version, const DataRef& from,
                                 const DataRef& to) {
    return txn_retry("swap_extents", [&](uint64_t ts, std::vector<TikvMutation>& muts) {
        std::string okey = object_key(b, k);
        auto v = snap_get(ts, okey);
        if (!v) return false;
        auto rec = codec::decode_object(std::string(k), *v);
        // Optimistic check: version/extent mismatch = overwritten/deleted in the
        // meantime → give up (muts empty, no transaction sent); after the check
        // passes, "read-then-commit" is guaranteed by prewrite's conflict detection
        // on okey (§4.1 natural CAS)
        if (rec.version != expect_version || rec.data.extents != from.extents) return false;
        rec.data = to;
        rec.version += 1;
        muts.push_back({TikvOp::kPut, okey, codec::encode_object(rec)});
        // refs operate on the set difference (meta_util.h refs_delta): multiple
        // mutations on the same key in TiKV are "last one wins" — add-all then
        // delete-all would wipe the refs of unmigrated chunks → live data wrongly deleted
        auto rd = refs_delta(from, to);
        mut_refs(muts, rd.added, /*add=*/true, okey);
        mut_refs(muts, rd.removed, /*add=*/false, {});
        // Compaction swaps refs: the ledger migrates with the extents (§9.2); both
        // sides use the object accounting basis (if the migrated-out old record is in
        // mpu form this slightly under-deducts, the conservative direction)
        const int64_t ov = codec::pack_rec_overhead(b, k);
        mut_pack_delta(muts, to, +1, ov);
        mut_pack_delta(muts, from, -1, ov);
        return true;
    });
}

bool TikvMetaStore::chunk_referenced(uint64_t file_id) {
    return guarded("chunk_referenced",
                   [&] { return snap_get(client().get_ts(), refs_key(file_id)).has_value(); });
}

void TikvMetaStore::scan_refs(const std::function<void(uint64_t)>& cb) {
    // Paged snapshot scan of the 'R' prefix (orphan scanning tolerates a weakly
    // consistent view); key tail = be64 file_id
    guarded("scan_refs", [&] {
        auto [lo, hi] = range_of('R', {});
        const size_t suffix = codec::be64_key(0).size();
        scan_range(client().get_ts(), lo, hi, [&](const std::string& key, const std::string&) {
            if (key.size() >= suffix)
                cb(codec::parse_be64(std::string_view(key).substr(key.size() - suffix)));
            return true;
        });
        return 0;
    });
}

}  // namespace lights3::storage::duostore
