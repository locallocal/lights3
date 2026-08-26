// L3: DuoStore metadata-side interface (docs/duostore-backend.md §3.2).
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
#include <functional>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/backend.h"
#include "storage/duostore/data_ref.h"

namespace lights3::storage::duostore {

struct ObjectRec {
    ObjectMeta meta;       // key/size/etag/content_type/last_modified/user_meta
    DataRef data;
    uint64_t version = 0;  // +1 on every write (maintained by the implementation); optimistic check for GC compaction ref swap (§9.2)
};

struct UploadRec {
    std::string upload_id;
    ObjectMeta meta;  // key + content_type/user_meta (take effect at complete)
    int64_t initiated_ms = 0;
};

struct PartRec {
    int part_no = 0;
    uint64_t size = 0;
    std::string etag;  // MD5 of the part content (unquoted hex)
    int64_t modified_ms = 0;
    DataRef data;
};

// gcq entry source (docs/archive/gaps.md §6.1): only with per-source bucketed counters can
// GC pinpoint whether "reclaim pressure comes from overwrites, bulk deletes, or
// abandoned mpu parts". Persisted as the reason byte of the codec gcq record
// (previously always written as 0 and discarded on decode); old entries decode to
// kUnknown
enum class ReclaimReason : uint8_t {
    kUnknown = 0,        // old entries enqueued before P4
    kOverwrite = 1,      // put_object / complete_upload overwriting the old version of a same-name object
    kDelete = 2,         // delete_object
    kPartOverwrite = 3,  // same-number part re-upload (last-write-wins)
    kAbort = 4,          // abort_upload (including GC's mpu_ttl expiry cleanup)
    kComplete = 5,       // parts not selected by complete_upload
};

// For metric labels and logging; unknown values always fall back to "unknown"
// (old entries / future new sources)
inline const char* reclaim_reason_name(ReclaimReason r) {
    switch (r) {
        case ReclaimReason::kOverwrite: return "overwrite";
        case ReclaimReason::kDelete: return "delete";
        case ReclaimReason::kPartOverwrite: return "part_overwrite";
        case ReclaimReason::kAbort: return "abort";
        case ReclaimReason::kComplete: return "complete";
        case ReclaimReason::kUnknown: break;
    }
    return "unknown";
}

struct Reclaim {
    std::vector<Extent> extents;  // pending physical reclaim
    int64_t enqueue_ms = 0;       // enqueue time (unix ms); the GC consumer judges gc_grace by it (§9.1)
    ReclaimReason reason = ReclaimReason::kUnknown;
};

struct PackStat {
    uint64_t pack_id = 0;
    uint64_t file_size = 0;  // reported by the data plane at seal time; 0 = unknown (crash leftover; stat again at compaction)
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
    explicit UndeterminedCommit(std::string msg)
        : S3Error(s3::S3ErrorCode::InternalError, std::move(msg)) {}
};

struct IMetaStore {
    // ---- bucket ----
    virtual void create_bucket(std::string_view b) = 0;  // already exists -> BucketAlreadyOwnedByYou
    // Missing -> NoSuchBucket; has objects or in-progress multipart -> BucketNotEmpty (aligned with AWS)
    virtual void delete_bucket(std::string_view b) = 0;
    virtual bool bucket_exists(std::string_view b) = 0;
    virtual std::vector<BucketInfo> list_buckets() = 0;

    // ---- object ----
    virtual std::optional<ObjectRec> get_object(std::string_view b, std::string_view k) = 0;
    // Meta only, no manifest (docs/archive/gaps.md §3.9): HEAD/precondition reads go here.
    // decode_object materializes the entire extent vector (650k extents ≈ 26MB)
    // only to discard it immediately; decode_object_meta decodes just the
    // fixed-length header
    virtual std::optional<ObjectMeta> head_object(std::string_view b, std::string_view k) = 0;
    // When cond.active(), validate the old record inside this transaction's atomic
    // section per the PutCondition contract (storage/backend.h): violations throw
    // PreconditionFailed / NoSuchKey and the transaction does not commit (shared
    // check in meta_util.h check_put_condition)
    virtual void put_object(std::string_view b, std::string_view k, ObjectRec rec,
                            PutCondition cond = {}) = 0;
    virtual bool delete_object(std::string_view b, std::string_view k) = 0;  // returns false if missing (idempotent)
    virtual ListResult list_objects(std::string_view b, const ListOptions& opt) = 0;

    // ---- multipart ----
    virtual std::string create_upload(std::string_view b, std::string_view k,
                                      ObjectMeta meta) = 0;
    virtual UploadRec require_upload(std::string_view b, std::string_view k,
                                     std::string_view id) = 0;  // missing -> NoSuchUpload
    virtual void put_part(std::string_view b, std::string_view k, std::string_view id,
                          PartRec p) = 0;  // the old same-number part enters the GC ledger in the same batch
    virtual std::vector<PartRec> list_parts(std::string_view b, std::string_view k,
                                            std::string_view id) = 0;
    // Pagination hint (docs/archive/gaps.md §5.1): return entries with (key, upload_id)
    // strictly greater than (key_marker, id_marker), in ascending order; with
    // limit>0 return at most limit entries. The hint may be ignored — an engine
    // that cannot push it down can just return everything, since the caller
    // (DuoStoreBackend) always runs apply_uploads_page again; semantics do not
    // depend on whether the engine pushes down.
    // Note the caller passes limit=0 when delimiter is non-empty: grouping needs
    // the full picture to determine truncation
    virtual std::vector<UploadInfo> list_uploads(std::string_view b,
                                                 std::string_view key_marker = {},
                                                 std::string_view id_marker = {},
                                                 int limit = 0) = 0;
    virtual std::string complete_upload(std::string_view b, std::string_view k,
                                        std::string_view id,
                                        std::span<const PartInfo> parts) = 0;  // returns the aggregate ETag (§8)
    virtual void abort_upload(std::string_view b, std::string_view k,
                              std::string_view id) = 0;

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
    virtual std::vector<std::pair<uint64_t, Reclaim>> peek_reclaims(
        size_t max, uint64_t min_seq = 0, size_t max_extents = SIZE_MAX) = 0;
    virtual void ack_reclaim(uint64_t seq) = 0;    // write off after successful physical deletion
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
    // not enter refs, they go through this ledger); pack_stats() returns every pack
    // with an entry (including live=0 and unsealed ones — whole-file deletion of
    // empty packs and abandonment on restart both depend on seeing them)
    virtual std::vector<PackStat> pack_stats() = 0;
    // Seal (called back on data-plane rotation/close; idempotent): file_size=0
    // means unknown and must not overwrite a recorded non-zero value — crash
    // leftover packs are back-sealed with 0 by DuoStoreBackend at startup
    // (abandoned on restart, §5.2)
    virtual void seal_pack(uint64_t pack_id, uint64_t file_size) = 0;
    // Write off after successfully unlinking an empty pack's whole file (same §9.1
    // ordering iron rule as ack_reclaim: physical delete first, then write off)
    virtual void drop_pack_stat(uint64_t pack_id) = 0;
    virtual bool swap_extents(std::string_view b, std::string_view k, uint64_t expect_version,
                              const DataRef& from, const DataRef& to) = 0;  // compaction ref swap
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
        for (const auto& r : reqs)
            out.push_back(swap_extents(r.bucket, r.key, r.expect_version, r.from, r.to));
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
    // other gateways), so the deployment constraint gc_grace >= longest expected
    // GET duration still holds
    virtual bool try_gc_lease(std::string_view /*owner*/, int64_t /*ttl_ms*/) { return true; }
    virtual bool chunk_referenced(uint64_t file_id) = 0;  // orphan scan
    // Orphan reverse reconciliation (§9.3): iterate every file_id in the refs table
    // (chunk/rados share the ledger; order not guaranteed). Snapshot semantics are
    // lenient: concurrent adds/removes during iteration may or may not be visible —
    // the caller (orphan scan) re-checks "file present, refs missing" with a
    // point-in-time chunk_referenced, and only warns without deleting on "refs
    // present, file missing"; both directions tolerate a weakly consistent snapshot
    virtual void scan_refs(const std::function<void(uint64_t file_id)>& cb) = 0;
    virtual void close() = 0;
    virtual ~IMetaStore() = default;
};

}  // namespace lights3::storage::duostore
