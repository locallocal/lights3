// L3: pure-computation helpers shared by the meta store implementations
// (docs/duostore-backend.md §2.1: "S3 semantics duplicated across implementations
// are minimized via shared helpers"). Depends only on the record types from
// meta_store.h and the multipart utilities; no storage-engine coupling.
#pragma once

#include <charconv>
#include <chrono>
#include <map>
#include <set>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/duostore/meta_store.h"
#include "storage/multipart.h"

namespace lights3::storage::duostore {

// Per-item extent cap for gcq entries (docs/gaps.md §2.11): when deleting TB-scale
// objects (hundreds of thousands of extents), split the DataRef into multiple gcq
// items so the GC consumer's decoded memory residency per peek batch stays bounded.
// Acks are independent per item and unlink is idempotent, so splitting does not
// change crash semantics (the §9.1 argument — physical delete first, then settle
// the accounting — still holds)
inline constexpr size_t kReclaimMaxExtents = 4096;

// ---- Unified schema-marker check (docs/gaps.md §6.1: the four engines share the
// "evolve rather than hard-reject" policy) ----
// Stored marker = <lineage prefix><decimal version> (rocks prefix empty, redis "r",
// tikv "t"; sqlite's user_version is a plain integer that skips string parsing but
// follows the same comparison policy). Returns the stored version number:
//   lineage mismatch / garbage  → InternalError (wrong database; any write could destroy data);
//   version newer than build    → InternalError (running downgraded would silently corrupt the newer layout);
//   version ≤ current           → returned; for versions < current the caller runs its own migration chain.
// engine is the error-message prefix (reusing each engine's existing wording, e.g.
// "duostore redis meta")
inline int64_t parse_schema_marker(const std::string& stored, std::string_view lineage,
                                   int64_t current, const std::string& engine) {
    int64_t ver = -1;
    if (stored.size() > lineage.size() && stored.compare(0, lineage.size(), lineage) == 0) {
        const char* b = stored.data() + lineage.size();
        const char* e = stored.data() + stored.size();
        auto r = std::from_chars(b, e, ver);
        if (r.ec != std::errc() || r.ptr != e) ver = -1;
    }
    if (ver < 0)
        throw s3::S3Error(s3::S3ErrorCode::InternalError,
                          engine + ": unrecognized schema marker '" + stored + "'");
    if (ver > current)
        throw s3::S3Error(s3::S3ErrorCode::InternalError,
                          engine + ": database schema '" + stored +
                              "' is newer than this build (v" + std::to_string(current) +
                              "); refusing to run downgraded");
    return ver;
}

// Unified error for a gap in the migration chain ("changing the layout without
// leaving a migration" is a programming error; failing loudly at startup beats
// running impaired)
[[noreturn]] inline void throw_no_migration(int64_t from, int64_t current,
                                            const std::string& engine) {
    throw s3::S3Error(s3::S3ErrorCode::InternalError,
                      engine + ": no migration path from schema v" + std::to_string(from) +
                          " to v" + std::to_string(current));
}

// Atomic-section check for conditional PUT (PutCondition contract, storage/backend.h):
// the four engines call this after reading the old record inside their own
// transaction; throwing abandons the commit (rollback holds naturally for local
// engines; redis calls it before assembling the batch and tikv before filling
// mutations, so neither issues any write)
inline void check_put_condition(const PutCondition& cond, const std::optional<ObjectRec>& old,
                                std::string_view key) {
    if (!cond.active()) return;
    if (cond.if_none_match && old)
        throw s3::S3Error(s3::S3ErrorCode::PreconditionFailed,
                          "At least one of the pre-conditions you specified did not hold",
                          std::string(key));
    if (cond.if_match_etag) {
        if (!old)
            throw s3::S3Error(s3::S3ErrorCode::NoSuchKey, "The specified key does not exist",
                              std::string(key));
        if (*cond.if_match_etag != old->meta.etag)
            throw s3::S3Error(s3::S3ErrorCode::PreconditionFailed,
                              "At least one of the pre-conditions you specified did not hold",
                              std::string(key));
    }
}

// Part selection and object assembly for complete_upload (formerly a verbatim-
// identical block across the RocksDB/Redis/SQLite implementations): per-item ETag
// validation (missing/mismatched throws InvalidPart), extents concatenated in
// submission order, sizes accumulated, combined ETag and last_modified synthesized.
// selected outputs the chosen part numbers so the caller can route refs transfer
// vs. GC accounting for unselected parts. version is set separately by the caller
// after reading the old object.
inline ObjectRec assemble_completed_object(ObjectMeta meta, std::span<const PartInfo> parts,
                                           const std::map<int, PartRec>& stored,
                                           std::set<int>& selected) {
    ObjectRec rec;
    rec.meta = std::move(meta);
    std::vector<std::string> md5s;
    for (const auto& pi : parts) {
        auto sit = stored.find(pi.part_no);
        if (sit == stored.end() || sit->second.etag != strip_etag_quotes(pi.etag))
            throw s3::S3Error(s3::S3ErrorCode::InvalidPart,
                              "One or more of the specified parts could not be found or the "
                              "ETag did not match.",
                              rec.meta.key);
        md5s.push_back(sit->second.etag);
        selected.insert(pi.part_no);
        rec.meta.size += sit->second.size;
        const auto& ex = sit->second.data.extents;
        rec.data.extents.insert(rec.data.extents.end(), ex.begin(), ex.end());
    }
    rec.meta.etag = combined_etag(md5s);
    rec.meta.last_modified = std::chrono::system_clock::now();
    return rec;
}

// Refs set-difference for compaction ref swap (swap_extents, §9.2).
//
// The refs table is last-wins per file_id: for the same file_id, "Put then Delete"
// (true of all four engines — WriteBatch / Lua order / SQL order / TiKV
// latter-wins) nets out to a delete. Compaction replaces only one pack extent, and
// to shares **all un-migrated chunk extents** with from — so add(to) over the whole
// set followed by remove(from) over the whole set would erase the refs entries of
// chunks still referenced by the object, and the orphan scan would then unlink live
// data (unrecoverable data loss).
//
// Operating only on file_ids that are genuinely added (to−from) or genuinely gone
// (from−to) makes ordering irrelevant and also skips pointless mutations. Pack
// liveness accounting is additive (same-pack +1/-1 cancels naturally) and is
// unaffected, so this helper serves only the refs side; kPack extents never enter
// refs and are filtered out here as well.
struct RefsDelta {
    DataRef added;    // extents needing a refs Put
    DataRef removed;  // extents needing a refs Delete
};

inline RefsDelta refs_delta(const DataRef& from, const DataRef& to) {
    auto ref_ids = [](const DataRef& r) {
        std::set<uint64_t> ids;
        for (const auto& e : r.extents)
            if (e.kind != Extent::Kind::kPack) ids.insert(e.file_id);
        return ids;
    };
    std::set<uint64_t> from_ids = ref_ids(from), to_ids = ref_ids(to);
    RefsDelta d;
    std::set<uint64_t> seen;
    for (const auto& e : to.extents)
        if (e.kind != Extent::Kind::kPack && !from_ids.count(e.file_id) &&
            seen.insert(e.file_id).second)
            d.added.extents.push_back(e);
    seen.clear();
    for (const auto& e : from.extents)
        if (e.kind != Extent::Kind::kPack && !to_ids.count(e.file_id) &&
            seen.insert(e.file_id).second)
            d.removed.extents.push_back(e);
    return d;
}

}  // namespace lights3::storage::duostore
