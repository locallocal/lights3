// L3: DuoStore data-side interface (docs/duostore-backend.md §3.3). Coroutine Task<T>;
// each implementation decides for itself whether to hop to a pool thread internally.
#pragma once

#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <vector>

#include "core/task.h"
#include "http/model.h"
#include "storage/duostore/data_ref.h"

namespace lights3::storage::duostore {

struct WriteHint {
    std::optional<uint64_t> content_length;  // body.length(); nullopt when chunked
    // Ownership embedded in the pack record (§5.2: "bucket\0key" or "mpu\0<id>\0<part_no>"):
    // used by the compaction sequential scan for reverse liveness lookup and by
    // offline disaster-recovery salvage; engines without packs ignore it
    std::string owner;
};

struct DataWriter {
    virtual Task<void> write(std::span<const std::byte> buf) = 0;
    virtual Task<DataRef> finish() = 0;  // returns the location after persisting; destruction without finish = discard
    virtual ~DataWriter() = default;
};

// Result statistics of the P4 compaction sequential scan (docs/duostore-backend.md §9.2)
struct GcRewrite {
    uint64_t scanned = 0;    // records fully parsed (including crc pass)
    uint64_t migrated = 0;   // records confirmed live and successfully ref-swapped
    uint64_t corrupt = 0;    // records with corrupt magic/header/crc (torn tail excluded — the expected form discarded on restart)
    uint64_t file_size = 0;  // actual file size; packs left as seal(0) by a crash use this to backfill the liveness-ratio denominator
};

struct IDataStore;

// One candidate record handed by the compaction sequential scan to the migration callback (§9.2)
struct PackScanRecord {
    std::string owner;
    Extent from;
    std::vector<std::byte> payload;
};

// Compaction migration callback (§9.2, wired up by DuoStoreBackend): the data
// plane's sequential scan accumulates K records and delivers them in one call
// (docs/archive/gaps.md §2.13 batching — per-record delivery costs one fdatasync + one
// meta commit each, ≈ 1000 for a 128MiB pack, contending with business writes for
// the same write lock). The callback is responsible for owner reverse-lookup of
// liveness + batch-appending back into this store (write_batch, one persistence
// barrier) + swapping refs aggregated by owner (multiple records of the same
// object in one swap, eliminating the O(n²) rewrite of the whole manifest).
// Returns the number of successfully migrated records; dead regions and records
// that are live but temporarily unmigratable (in-flight mpu etc.) are not counted,
// and the data plane never touches the original record — pack deletion always
// goes through the "live account reaches zero + delete the whole empty pack"
// path, so a misjudgment loses no data.
// See migrate_pack_records in duostore_backend.h for the standard implementation
using PackMigrateFn = std::function<Task<uint64_t>(IDataStore& self,
                                                   std::vector<PackScanRecord>&& batch)>;

// Input item for write_batch (compaction migration only): payload is held by the
// caller until the call returns
struct PackAppendItem {
    std::string_view owner;
    std::span<const std::byte> payload;
};

// Write-side pin hooks (§9.3): the orphan scan must not reclaim chunks that are
// "mid-write with meta not yet committed" — early chunks of a slow streaming PUT
// can have mtimes far beyond gc_grace, so an mtime grace alone is insufficient.
// The writer pins when allocating a file_id and unpins if destroyed without
// finish; after finish, unpin responsibility transfers to the caller
// (DuoStoreBackend releases it after the meta commit / fallback deletion).
// **Every data store that produces chunk-like entities must hook this up**: an
// engine that misses it lets the orphan scan delete the already-landed parts of
// an in-flight large object as unreferenced files (docs/archive/gaps.md §1.2)
struct ChunkPinHooks {
    std::function<void(uint64_t)> pin;
    std::function<void(uint64_t)> unpin;

    void pin_one(uint64_t id) const {
        if (pin) pin(id);
    }
    void unpin_one(uint64_t id) const {
        if (unpin) unpin(id);
    }
};

struct IDataStore {
    virtual Task<std::unique_ptr<DataWriter>> open_writer(WriteHint hint) = 0;
    // Batch write (compaction migration only, docs/archive/gaps.md §2.13): K payloads
    // landed in one call, returns a DataRef list of the same length as the input.
    // Default is per-item open_writer (semantics unchanged); the fs implementation
    // overrides with a pack batch append using a single slot lock + a single fdatasync
    virtual Task<std::vector<DataRef>> write_batch(std::span<const PackAppendItem> items) {
        std::vector<DataRef> out;
        out.reserve(items.size());
        for (const auto& it : items) {
            auto w = co_await open_writer({it.payload.size(), std::string(it.owner)});
            co_await w->write(it.payload);
            out.push_back(co_await w->finish());
        }
        co_return out;
    }
    // [first,last] is the closed interval after resolve_range; returns a streaming BodyReader (length()=last-first+1)
    virtual Task<std::unique_ptr<http::BodyReader>> open_reader(DataRef ref, uint64_t first,
                                                               uint64_t last) = 0;
    virtual Task<void> remove(std::span<const Extent> extents) = 0;  // idempotent (ENOENT ignored)
    // Whole pack file deletion (§9.1: packs that are sealed with live_recs==0);
    // idempotent. Pure virtual: engines without pack entities write an explicit
    // no-op override (matching the rewrite_pack convention) — a silent interface
    // default would let a new engine that "has packs but forgot to implement
    // deletion" compile, with GC keeping accounts but never freeing bytes
    virtual Task<void> remove_pack(uint64_t pack_id) = 0;
    virtual Task<GcRewrite> rewrite_pack(uint64_t pack_id) = 0;      // compaction sequential scan (§9.2)
    // Age-based rotation (docs/archive/gaps.md §6.1): seals active packs whose first
    // record was written more than max_age_ms ago, returns the number sealed this
    // time. GC calls it once per round — with capacity-only sealing, an active
    // pack never rotates under low write volume, and its overwritten/deleted
    // records never enter the compaction candidate set, becoming a permanently
    // unreclaimable dead region. Engines without pack entities default to 0 (not
    // an error: no active pack means no aging problem)
    virtual Task<uint64_t> seal_aged_packs(int64_t /*max_age_ms*/) { co_return 0; }
    // Orphan-scan enumeration (§9.3): iterates all chunk-like entities on the data
    // plane (fs = the chunks/ directory, rados = namespace object listing,
    // docs/duostore-rados-data.md §8.2 — the interface was finalized in P4, the
    // rados implementation is scheduled for C4), calling back
    // (file_id, mtime_ms, size_bytes) for each. Orphan determination
    // (refs reverse-lookup/grace/pin) is the caller's (DuoStoreBackend's) job —
    // the data plane only enumerates, no liveness judgment. size feeds usage
    // metric accumulation (docs/archive/gaps.md §6.1): enumeration needs a stat anyway, so
    // one extra field adds no system calls.
    // Pure virtual (matching the remove_pack convention): engines that do not
    // support enumeration throw explicitly, never silently scan nothing and
    // falsely report "no orphans"
    virtual Task<void> scan_chunks(
        const std::function<void(uint64_t file_id, int64_t mtime_ms, uint64_t size)>& cb) = 0;
    // Reverse reconciliation enumeration of packs/ (docs/archive/gaps.md §6.1): the pack
    // file exists as soon as it is created, but the packstat row is only recorded
    // when the first record commits — a hard crash exactly in that window leaks
    // the file permanently, appearing in no account (the orphan scan previously
    // covered only chunks and could not see it). Same semantics as scan_chunks:
    // enumeration only, determination belongs to the caller. Engines without pack
    // entities default to an empty scan (not a false report — they simply have no
    // packs/ directory as a leak surface)
    virtual Task<void> scan_packs(
        const std::function<void(uint64_t pack_id, int64_t mtime_ms, uint64_t size)>& /*cb*/) {
        co_return;
    }
    // "Is this pack currently held by a live writer" (for startup catch-up
    // sealing, docs/archive/gaps.md §1.4). The fs implementation probes the active pack's
    // advisory lock; engines without pack entities or with no way to probe return
    // false (= do not block catch-up sealing, same behavior as before this
    // change). **false must be the conservative direction**: returning true only
    // postpones catch-up sealing to the next startup, while returning false could
    // seal a pack someone else is actively writing
    virtual bool pack_write_locked(uint64_t /*pack_id*/) { return false; }
    // Actual pack file size (docs/archive/gaps.md §2.3b): accounts left as seal(0) by a
    // crash use this to backfill the denominator before the GC decision, so a
    // pack with unknown file_size does not unconditionally go into full rewrite.
    // 0 = unknown/unsupported (the caller falls back to the original
    // "sequential-scan backfill" path)
    virtual uint64_t stat_pack(uint64_t /*pack_id*/) { return 0; }
    virtual Task<void> close() = 0;
    virtual ~IDataStore() = default;
};

}  // namespace lights3::storage::duostore
