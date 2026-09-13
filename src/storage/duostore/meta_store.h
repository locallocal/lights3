// L3: DuoStore metadata-side interface (docs/architecture/storage/duostore-design.md §3.2).
// Contract: synchronous interface, must be called on a pool thread (DuoStoreBackend
// switches to the pool uniformly at the entry point, §2.2); errors throw
// s3::S3Error; commit-type methods internally complete
// "write new + old DataRef into the GC ledger + reference/stats updates" in a
// single transaction (§4.5).
// Precondition: bucket/key/upload_id contain no NUL — '\0' is the key-encoding
// separator (§4.1), guaranteed by the shared validation layer
// validate_bucket_name/validate_object_key; the codec key builders additionally
// have defensive checks (violations throw InternalError, never silently producing
// cross-record key collisions).
#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/util/crypto.h"

#include "storage/backend.h"
#include "storage/duostore/data_ref.h"

namespace lights3::storage::duostore {

// Tiering state of an object when duostore is the local side of a TieredBackend
// (roadmap §3.6 ⑥, docs/architecture/storage/tiered-design.md §3): remote = the data lives in the cloud and
// `data` is empty (a stub), cached = local extents are a cache of the cloud replica.
// Codec object record v3; absent on older records = local
struct TierState {
    enum : uint8_t { kLocal = 0, kRemote = 1, kCached = 2 };
    uint8_t tier = kLocal;
    // cloud replica ETag (unquoted), never exposed
    std::string remote_etag;
    // iso8601 upload time
    std::string remote_at;
};

struct ObjectRec {
    // key/size/etag/content_type/last_modified/user_meta
    ObjectMeta meta;
    DataRef data;
    // +1 on every write (maintained by the implementation); optimistic check for GC compaction
    // ref swap (§9.2)
    uint64_t version = 0;
    // tiered local-side state (v3); meta.size stays the logical size even when data is empty
    TierState tier;
};

struct UploadRec {
    std::string upload_id;
    // key + content_type/user_meta (take effect at complete)
    ObjectMeta meta;
    int64_t initiated_ms = 0;
};

struct PartRec {
    int part_no = 0;
    uint64_t size = 0;
    // MD5 of the part content (unquoted hex)
    std::string etag;
    int64_t modified_ms = 0;
    DataRef data;
    // Verified part checksum (roadmap §2.2), codec part-record v2; empty = none
    std::string checksum_algorithm;
    // base64
    std::string checksum_value;
};

// gcq entry source (docs/archive/gaps.md §6.1): only with per-source bucketed counters can
// GC pinpoint whether "reclaim pressure comes from overwrites, bulk deletes, or
// abandoned mpu parts". Persisted as the reason byte of the codec gcq record
// (previously always written as 0 and discarded on decode); old entries decode to
// kUnknown
enum class ReclaimReason : uint8_t {
    // old entries enqueued before P4
    kUnknown = 0,
    // put_object / complete_upload overwriting the old version of a same-name object
    kOverwrite = 1,
    // delete_object
    kDelete = 2,
    // same-number part re-upload (last-write-wins)
    kPartOverwrite = 3,
    // abort_upload (including GC's mpu_ttl expiry cleanup)
    kAbort = 4,
    // parts not selected by complete_upload
    kComplete = 5,
};

// For metric labels and logging; unknown values always fall back to "unknown"
// (old entries / future new sources)
inline const char* reclaim_reason_name(ReclaimReason r) {
    switch (r) {
        case ReclaimReason::kOverwrite:
            return "overwrite";
        case ReclaimReason::kDelete:
            return "delete";
        case ReclaimReason::kPartOverwrite:
            return "part_overwrite";
        case ReclaimReason::kAbort:
            return "abort";
        case ReclaimReason::kComplete:
            return "complete";
        case ReclaimReason::kUnknown:
            break;
    }
    return "unknown";
}

struct Reclaim {
    // pending physical reclaim
    std::vector<Extent> extents;
    // enqueue time (unix ms); the GC consumer judges gc_grace by it (§9.1)
    int64_t enqueue_ms = 0;
    ReclaimReason reason = ReclaimReason::kUnknown;
};

struct PackStat {
    uint64_t pack_id = 0;
    // reported by the data plane at seal time; 0 = unknown (crash leftover; stat again at
    // compaction)
    uint64_t file_size = 0;
    int64_t live_bytes = 0;
    int64_t live_recs = 0;
    bool sealed = false;
};

// Single-item request of swap_extents_batch (compaction aggregates by owner, one
// per object, §9.2)
struct SwapReq {
    std::string bucket;
    std::string key;
    uint64_t expect_version = 0;
    DataRef from;
    DataRef to;
};

// Commit outcome undetermined (specific to network engines): connection dropped
// after redis EVALSHA, tikv primary commit timeout — the transaction **may have
// already taken effect**. For local engines "exception thrown ≈ not committed"
// holds; for these two it does not. The caller (commit_or_discard) must therefore
// **not** fall back to physically deleting data: if the commit actually took
// effect, what gets deleted is data already referenced by an object, producing a
// broken object pointing at deleted data; if it did not take effect, leave it for
// the orphan scan to converge. Still InternalError to the client (500, semantics
// unchanged)
struct UndeterminedCommit : s3::S3Error {
    explicit UndeterminedCommit(std::string msg) : S3Error(s3::S3ErrorCode::InternalError, std::move(msg)) {}
};

// What a gateway publishes as its lease (IMetaStore::publish_lease): start times
// (unix ms) of its oldest in-flight read and oldest in-flight write, "now" when
// idle (an idle gateway holds nothing back). oldest_write_ms is optional only on
// the consumer side (min_lease): a lease written by an older build carries no
// write field
struct LeaseInfo {
    int64_t oldest_read_ms = 0;
    std::optional<int64_t> oldest_write_ms;
};

// Read-only slice of the meta shared by IMetaStore and its point-in-time
// snapshots (roadmap §3.7 online meta dump): exactly the reads dump_meta needs.
// A snapshot implementation must make every method observe one consistent state
// KV facade records (see IMetaStore::kv_*)
struct KvItem {
    std::string key;
    std::string value;
    std::string etag;
};
struct KvPut {
    std::string key;
    std::string value;
    PutCondition cond;
};
// the ETag of a KV value: sha256 hex, first 16 characters
inline std::string kv_etag(std::string_view value) { return util::sha256_hex(value).substr(0, 16); }
// The PutCondition contract over a KV entry (the engines call it inside their atomic
// section): current = the stored value's etag, nullopt = missing
inline void check_kv_condition(const PutCondition& cond, const std::optional<std::string>& current,
                               std::string_view key) {
    if (!cond.active()) return;
    if (cond.if_none_match && current)
        throw s3::S3Error(s3::S3ErrorCode::PreconditionFailed,
                          "At least one of the pre-conditions you specified did not hold", std::string(key));
    if (cond.if_match_etag) {
        if (!current)
            throw s3::S3Error(s3::S3ErrorCode::NoSuchKey, "The specified key does not exist", std::string(key));
        if (*current != *cond.if_match_etag)
            throw s3::S3Error(s3::S3ErrorCode::PreconditionFailed,
                              "At least one of the pre-conditions you specified did not hold", std::string(key));
    }
}

struct IMetaReadView {
    virtual std::vector<BucketInfo> list_buckets() = 0;
    virtual std::optional<ObjectRec> get_object(std::string_view b, std::string_view k) = 0;
    virtual ListResult list_objects(std::string_view b, const ListOptions& opt) = 0;
    virtual std::vector<PackStat> pack_stats() = 0;
    virtual ~IMetaReadView() = default;
};

// One entry of a meta backup chain (backlog-sequence ⑧, docs/architecture/storage/duostore-core.md
// §11.1): what an engine wrote into the backup directory and how to address it
// at restore time. full=false is a delta over the previous entry of the chain
struct MetaBackupEntry {
    uint64_t id = 0;
    bool full = true;
    // wall clock at the backup point
    int64_t ts_ms = 0;
    // relative to the backup directory (empty: engine-managed, e.g. rocksdb's BackupEngine tree)
    std::string file;
    // engine-specific restore point: rocksdb backup id, sqlite WAL segment no., redis repl offset,
    // tikv TSO
    std::string marker;
    // payload written by this entry
    uint64_t bytes = 0;
};

struct IMetaStore : IMetaReadView {
    // ---- bucket ----
    // already exists -> BucketAlreadyOwnedByYou
    virtual void create_bucket(std::string_view b) = 0;
    // Missing -> NoSuchBucket; has objects or in-progress multipart -> BucketNotEmpty (aligned with AWS)
    virtual void delete_bucket(std::string_view b) = 0;
    virtual bool bucket_exists(std::string_view b) = 0;

    // ---- object ----
    // Meta only, no manifest (docs/archive/gaps.md §3.9): HEAD/precondition reads go here.
    // decode_object materializes the entire extent vector (650k extents ≈ 26MB)
    // only to discard it immediately; decode_object_meta decodes just the
    // fixed-length header
    virtual std::optional<ObjectMeta> head_object(std::string_view b, std::string_view k) = 0;
    // When cond.active(), validate the old record inside this transaction's atomic
    // section per the PutCondition contract (storage/backend.h): violations throw
    // PreconditionFailed / NoSuchKey and the transaction does not commit (shared
    // check in meta_util.h check_put_condition)
    virtual void put_object(std::string_view b, std::string_view k, ObjectRec rec, PutCondition cond = {}) = 0;
    // returns false if missing (idempotent)
    virtual bool delete_object(std::string_view b, std::string_view k) = 0;

    // ---- multipart ----
    virtual std::string create_upload(std::string_view b, std::string_view k, ObjectMeta meta) = 0;
    // missing -> NoSuchUpload
    virtual UploadRec require_upload(std::string_view b, std::string_view k, std::string_view id) = 0;
    // the old same-number part enters the GC ledger in the same batch
    virtual void put_part(std::string_view b, std::string_view k, std::string_view id, PartRec p) = 0;
    virtual std::vector<PartRec> list_parts(std::string_view b, std::string_view k, std::string_view id) = 0;
    // Pagination hint (docs/archive/gaps.md §5.1): return entries with (key, upload_id)
    // strictly greater than (key_marker, id_marker), in ascending order; with
    // limit>0 return at most limit entries. An empty id_marker with a non-empty
    // key_marker means "key > key_marker" (the whole key was paged past, S3
    // key-marker-only semantics) -- not "(key_marker, "") < (key, id)".
    // prefix (roadmap §3.5): only entries whose key starts with prefix. An engine
    // that honors limit MUST honor prefix as well (seek to it and stop past it):
    // returning `limit` entries from before the prefix range would let the
    // caller's own prefix filter empty the page and misreport end-of-list.
    // The hints may be ignored together — an engine that cannot push down can
    // just return everything, since the caller (DuoStoreBackend) always runs
    // apply_uploads_page again; semantics do not depend on whether the engine
    // pushes down.
    // Note the caller passes limit=0 when delimiter is non-empty: grouping needs
    // the full picture to determine truncation
    virtual std::vector<UploadInfo> list_uploads(std::string_view b, std::string_view key_marker = {},
                                                 std::string_view id_marker = {}, int limit = 0,
                                                 std::string_view prefix = {}) = 0;
    // returns the aggregate ETag (§8)
    virtual std::string complete_upload(std::string_view b, std::string_view k, std::string_view id,
                                        std::span<const PartInfo> parts) = 0;
    virtual void abort_upload(std::string_view b, std::string_view k, std::string_view id) = 0;

    // ---- resource allocation and GC accounting (§9) ----
    // Batch dispatch (docs/archive/gaps.md §3.9): returns the first id of a contiguous run
    // [first, first+n); durably monotonic, segment-reserved. With per-id dispatch,
    // concurrent writers interleave one object's chunk ids and the manifest's run
    // encoding becomes useless (it actually bloats 28% after encoding); writers
    // fetching runs in geometrically growing batches restore contiguity.
    // n ≤ kMaxIdRun; discarding a run's unused tail is harmless (ids only need to
    // be unique and monotonic, not contiguous)
    virtual uint64_t alloc_file_run(Extent::Kind kind, uint32_t n) = 0;
    uint64_t alloc_file_id(Extent::Kind kind) { return alloc_file_run(kind, 1); }
    // Fetch the earliest at-most-max entries with seq >= min_seq (ascending seq).
    // The GC consumer resumes scanning from the min_seq checkpoint: head entries
    // skipped by grace/pin and not yet acked cannot stall the whole round or get
    // double-counted (§9.1). max_extents = cap on cumulative extents per batch
    // (docs/archive/gaps.md §2.11: a 256-entry count-based batch can resident GB-scale in
    // the worst case): close the batch early once the cap is reached, but return at
    // least 1 entry (oversized single entries left from before splitting must still
    // be consumable)
    virtual std::vector<std::pair<uint64_t, Reclaim>> peek_reclaims(size_t max, uint64_t min_seq = 0,
                                                                    size_t max_extents = SIZE_MAX) = 0;
    // write off after successful physical deletion
    virtual void ack_reclaim(uint64_t seq) = 0;
    // Batch write-off: forwards entry by entry by default; implementations may
    // override with a single-transaction/single-batch commit. The GC consumer
    // should prefer this interface — per-entry ack cost varies wildly by
    // implementation (the SQLite version does an independent fsync per entry and
    // contends for the same write lock as business commits; the RocksDB version is
    // nearly free). Losing an ack is harmless (gcq leftovers retry, unlink is
    // idempotent), so batch semantics are safe (crash argument in main doc §9.1)
    virtual void ack_reclaims(std::span<const uint64_t> seqs) {
        for (uint64_t s : seqs) ack_reclaim(s);
    }
    // Pack liveness ledger (§9.1/§9.2): live_bytes/live_recs are incremented and
    // decremented in the same batch as commit-type transactions (pack extents do
    // not enter refs, they go through this ledger); pack_stats() (declared on
    // IMetaReadView) returns every pack with an entry (including live=0 and
    // unsealed ones — whole-file deletion of empty packs and abandonment on
    // restart both depend on seeing them)
    // Seal (called back on data-plane rotation/close; idempotent): file_size=0
    // means unknown and must not overwrite a recorded non-zero value — crash
    // leftover packs are back-sealed with 0 by DuoStoreBackend at startup
    // (abandoned on restart, §5.2)
    virtual void seal_pack(uint64_t pack_id, uint64_t file_size) = 0;
    // Write off after successfully unlinking an empty pack's whole file (same §9.1
    // ordering iron rule as ack_reclaim: physical delete first, then write off)
    virtual void drop_pack_stat(uint64_t pack_id) = 0;
    // compaction ref swap
    virtual bool swap_extents(std::string_view b, std::string_view k, uint64_t expect_version, const DataRef& from,
                              const DataRef& to) = 0;
    // Batch ref swap (docs/archive/gaps.md §2.13 batched compaction): independent CAS per
    // item, returns per-item success/failure. Forwards entry by entry by default;
    // local engines (rocks/sqlite) override with a single-batch/single-transaction
    // commit — per-entry sqlite swap is one fsync per entry and contends for the
    // same write lock as business commits. Network engines (redis/tikv) stay
    // per-entry: merging into one transaction would let a single object's CAS
    // failure take down the whole batch (all-or-nothing), while their single-entry
    // commit is already one RTT
    virtual std::vector<bool> swap_extents_batch(std::span<const SwapReq> reqs) {
        std::vector<bool> out;
        out.reserve(reqs.size());
        for (const auto& r : reqs) out.push_back(swap_extents(r.bucket, r.key, r.expect_version, r.from, r.to));
        return out;
    }
    // Multi-gateway GC lease (docs/archive/gaps.md §6.1): single-instance GC/orphan-scan
    // was previously only a gc_enabled **convention** — two machines misconfigured
    // with GC both on would unlink each other's empty-pack verdicts. Take the lease
    // before each round: shared engines (redis/tikv) implement it as an atomic CAS
    // with TTL — same owner renews and refreshes the TTL; held by someone else and
    // not expired returns false (skip this round); local engines (rocks/sqlite)
    // already guarantee exclusivity via the single-process file lock, default is
    // always true. owner is the instance identifier (randomly generated in
    // process). A crashed holder yields naturally via TTL expiry — the lease does
    // not solve the unshared pin table problem (in-process pins are invisible to
    // other gateways); that is what the read lease below covers on shared
    // engines, with gc_grace as the fallback when it is off
    virtual bool try_gc_lease(std::string_view /*owner*/, int64_t /*ttl_ms*/) { return true; }
    // Multi-gateway read / write leases (roadmap §3.7; write side:
    // docs/archive/multi-gateway-multipart-design.md §4 ①): each gateway
    // periodically publishes, under its owner id with a TTL (crashed publishers
    // yield via expiry), the start time of its oldest in-flight read and of its
    // oldest in-flight write. The GC gateway reads the min across live leases:
    //  - read floor: only gcq entries enqueued strictly before it are reclaimed —
    //    a reader holding a ref to reclaimed extents must have fetched the
    //    manifest before the deref enqueued them, so "every in-flight read
    //    started after the enqueue" proves no reader can hold the ref;
    //  - write floor: the orphan scan only unlinks unreferenced chunks whose
    //    mtime is older than it — a chunk that appeared after some in-flight
    //    write began may belong to that write (its refs commit only at the end;
    //    the in-process write pin is invisible to peers).
    // Shared engines (redis/tikv) implement both; local engines keep the no-op
    // defaults (in-process pins are already exact, publish returns false =
    // unsupported and the backend stops republishing). Clock skew between
    // gateways must stay far below gc_grace (NTP assumption, same as
    // try_gc_lease's TTL arithmetic)
    virtual bool publish_lease(std::string_view /*owner*/, const LeaseInfo& /*info*/, int64_t /*ttl_ms*/) {
        return false;
    }
    // Min across unexpired leases (field-wise); nullopt = none published / engine
    // does not support leases (the caller then falls back to gc_grace alone).
    // oldest_write_ms is nullopt when any live lease was published without it
    // (a gateway running an older build): the write floor is then unknown and
    // the consumer falls back to grace-only for that round
    virtual std::optional<LeaseInfo> min_lease() { return std::nullopt; }
    // Point-in-time read snapshot for the online meta dump (roadmap §3.7):
    // every read through the returned view observes one consistent state while
    // writes continue. nullptr = engine cannot snapshot (redis) — the caller
    // must then guarantee write quiescence for a consistent dump. The view
    // borrows this store: it must be destroyed before close()
    virtual std::unique_ptr<IMetaReadView> snapshot() { return nullptr; }

    // ---- generic KV facade (docs/architecture/s3-tables-design.md §12) ----
    // An opaque, ordered key/value space apart from the object tables (rocksdb column
    // family "tc", sqlite table tc, redis hash + lex index, tikv key tag 'T'), the
    // backing of the S3 Tables catalog when tables.catalog_backing = duostore. Values
    // are opaque bytes; the ETag of a value is kv_etag(value), computed by the engine
    // and compared inside its own atomic section for PutCondition.if_match_etag
    // (missing key → NoSuchKey, mismatch → PreconditionFailed; if_none_match on an
    // existing key → PreconditionFailed). Engines without the facade throw
    // NotImplemented (the object backing stays the default)
    virtual std::optional<KvItem> kv_get(std::string_view key) {
        (void)key;
        throw s3::S3Error(s3::S3ErrorCode::NotImplemented, "this meta engine has no KV facade");
    }
    // returns the new ETag
    virtual std::string kv_put(std::string_view key, std::string_view value, PutCondition cond = {}) {
        (void)key;
        (void)value;
        (void)cond;
        throw s3::S3Error(s3::S3ErrorCode::NotImplemented, "this meta engine has no KV facade");
    }
    // false = missing (idempotent)
    virtual bool kv_delete(std::string_view key) {
        (void)key;
        throw s3::S3Error(s3::S3ErrorCode::NotImplemented, "this meta engine has no KV facade");
    }
    // keys starting with prefix, ascending, strictly greater than `after` when non-empty,
    // at most `limit` (0 = the engine's page); values included
    virtual std::vector<KvItem> kv_scan(std::string_view prefix, std::string_view after, size_t limit) {
        (void)prefix;
        (void)after;
        (void)limit;
        throw s3::S3Error(s3::S3ErrorCode::NotImplemented, "this meta engine has no KV facade");
    }
    // all-or-nothing multi-put, every item's condition checked in the same atomic
    // section (the catalog's commit record + pointer land together); returns the new
    // ETags in input order
    virtual std::vector<std::string> kv_put_batch(std::span<const KvPut> puts) {
        (void)puts;
        throw s3::S3Error(s3::S3ErrorCode::NotImplemented, "this meta engine has no KV facade");
    }
    // ---- Incremental backup / PITR (backlog-sequence ⑧) ----
    // Engines with a gateway-side physical mechanism (sqlite: WAL segment archive;
    // rocksdb: BackupEngine) implement backup_physical: write entry `id` into dir --
    // full=true a complete copy that starts a chain, full=false the delta since the
    // chain's previous entry -- and return what to record in the manifest. Writes are
    // paused for the duration (the engine's own write mutex). Engines whose
    // incremental copies live cluster-side (redis AOF, tikv BR/CDC) return false from
    // supports_physical_backup: the caller falls back to the logical dump and records
    // restore_marker() -- the replication offset / TSO to hand to the cluster tooling
    virtual bool supports_physical_backup() const { return false; }
    virtual MetaBackupEntry backup_physical(const std::filesystem::path& /*dir*/, uint64_t /*id*/, bool /*full*/) {
        throw s3::S3Error(s3::S3ErrorCode::InvalidRequest, "this meta engine has no gateway-side physical backup");
    }
    virtual std::string restore_marker() { return ""; }
    // orphan scan
    virtual bool chunk_referenced(uint64_t file_id) = 0;
    // Orphan reverse reconciliation (§9.3): iterate every file_id in the refs table
    // (chunk/rados share the ledger; order not guaranteed). Snapshot semantics are
    // lenient: concurrent adds/removes during iteration may or may not be visible —
    // the caller (orphan scan) re-checks "file present, refs missing" with a
    // point-in-time chunk_referenced, and only warns without deleting on "refs
    // present, file missing"; both directions tolerate a weakly consistent snapshot
    virtual void scan_refs(const std::function<void(uint64_t file_id)>& cb) = 0;
    // Cross-gateway cache invalidation (backlog-sequence ⑤): an engine that can push
    // peers' commits calls on_key(bucket, key) for every object-record change it
    // observes (including this instance's own) and on_reset() whenever the feed
    // (re)connects -- messages during a gap are lost, so the subscriber drops
    // everything it holds. Returns false when the engine has no such channel (local
    // engines, tikv): the caller keeps the TTL contract. Callbacks run on the
    // engine's feed thread, must be cheap and must not call back into the store;
    // close() stops the feed and joins the thread before returning
    using InvalidationSink = std::function<void(std::string_view bucket, std::string_view key)>;
    virtual bool subscribe_invalidations(InvalidationSink /*on_key*/, std::function<void()> /*on_reset*/) {
        return false;
    }
    virtual void close() = 0;
    virtual ~IMetaStore() = default;
};

}  // namespace lights3::storage::duostore
