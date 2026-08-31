// L3: local-filesystem implementation of IDataStore (docs/duostore-backend.md §5).
// Chunk path (fixed-size slicing + shard directories, P1) + pack aggregation
// (append-only records, P2).
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "core/thread_pool.h"
#include "storage/duostore/data_store.h"

namespace lights3::storage {
class UringEngine;  // storage/xlocalfs/uring.h; held only by shared_ptr here
}

namespace lights3::storage::duostore {

struct FsDataOptions {
    std::filesystem::path root;  // chunks/ and packs/ live underneath (§5)
    uint64_t chunk_size = 8ull << 20;
    // Chunk crc verification on the GET path (default off, §7): only applies to
    // chunks read completely from start to end — a partial read of a Range hitting
    // the middle of an extent cannot be verified; primary responsibility for
    // integrity lies with the GC/reconciliation paths.
    // pack records always verify crc (read in whole, independent of this switch)
    bool verify_chunk_crc = false;
    // Pack aggregation (§5.2): objects/parts ≤ pack_threshold go into packs;
    // 0 = disabled (everything goes through chunks).
    // Default 0 keeps the P1 behavior when FsDataStore is constructed standalone
    // (tests); DuoStoreBackend injects the real default (128KiB) from config
    uint64_t pack_threshold = 0;
    uint64_t pack_max_size = 128ull << 20;  // active pack sealing threshold
    int pack_writers = 4;                   // number of concurrent active packs
    // Age-based rotation (docs/archive/gaps.md §6.1): an active pack is sealed once its
    // first record was written longer than this ago, complementing the capacity
    // threshold. With capacity-only sealing under low write volume, an active pack
    // never rotates — its overwritten/deleted records become a dead region outside
    // the compaction candidate set (up to pack_max_size resident in the worst case).
    // 0 = disabled (seal only on capacity and close, i.e. behavior before this change)
    int pack_max_age_sec = 0;
    // Reporting of crc mismatches on the read path (P5 corruption metric; empty =
    // no reporting). Readers hold a copy of these options that escapes the store's
    // lifetime — the callback must not reference the store/backend (the assembly
    // side captures only a counter)
    std::function<void()> on_corruption;
    // io_uring data plane (roadmap §3.4 ⑤): when set, chunk writes/reads and pack
    // record reads go through the shared engine's pipelined streams and the
    // durability fdatasyncs become FSYNC SQEs; null = the original synchronous
    // path. Layout is identical either way. Readers copy these options and escape
    // the store's lifetime, so they co-own the engine via this shared_ptr.
    // Deliberately last: existing positional FsDataOptions initializers stay valid
    std::shared_ptr<UringEngine> uring;
};

class ChunkWriter;
class FsPackedWriter;

class FsDataStore final : public IDataStore {
public:
    // file_id allocation callback (persistently monotonic, provided by
    // IMetaStore::alloc_file_run): returns the first id of a contiguous run
    // [first, first+n). Writers batch-allocate with geometric growth; chunk ids of
    // the same object stay contiguous, which is what makes the manifest's run
    // encoding effective (docs/archive/gaps.md §3.9)
    using FileIdAlloc = std::function<uint64_t(Extent::Kind, uint32_t)>;
    // Pack sealing callback (IMetaStore::seal_pack): reports the final file size
    // on rotation and close. Unsealed packs left behind by a crash (callback never
    // ran) are catch-up sealed by DuoStoreBackend at startup (§5.2)
    using PackSeal = std::function<void(uint64_t pack_id, uint64_t file_size)>;

    // migrate (§9.2): compaction migration callback; empty = rewrite_pack scans
    // without migrating (statistics are still produced)
    FsDataStore(FsDataOptions opt, std::shared_ptr<ThreadPool> pool, FileIdAlloc alloc,
                PackSeal seal = {}, PackMigrateFn migrate = {}, ChunkPinHooks pins = {});
    ~FsDataStore() override;
    FsDataStore(const FsDataStore&) = delete;

    Task<std::unique_ptr<DataWriter>> open_writer(WriteHint hint) override;
    // Batch-write override (compaction migration, docs/archive/gaps.md §2.13):
    // pack-eligible items are batch-appended with one slot lock + one fdatasync;
    // items over the threshold / with packs disabled fall back to the per-item
    // open_writer path
    Task<std::vector<DataRef>> write_batch(std::span<const PackAppendItem> items) override;
    Task<std::unique_ptr<http::BodyReader>> open_reader(DataRef ref, uint64_t first,
                                                        uint64_t last) override;
    Task<void> remove(std::span<const Extent> extents) override;
    Task<void> remove_pack(uint64_t pack_id) override;
    bool pack_write_locked(uint64_t pack_id) override;
    uint64_t stat_pack(uint64_t pack_id) override;
    Task<GcRewrite> rewrite_pack(uint64_t pack_id) override;
    Task<uint64_t> seal_aged_packs(int64_t max_age_ms) override;
    Task<void> scan_chunks(
        const std::function<void(uint64_t file_id, int64_t mtime_ms, uint64_t size)>& cb)
        override;
    Task<void> scan_packs(
        const std::function<void(uint64_t pack_id, int64_t mtime_ms, uint64_t size)>& cb)
        override;
    Task<void> close() override;

    // Layout paths (§5); for test observation
    std::filesystem::path chunk_path(uint64_t file_id) const;
    std::filesystem::path pack_path(uint64_t pack_id) const;

private:
    friend class ChunkWriter;
    friend class FsPackedWriter;

    // Active pack slot (§5.2): each carries a mutex and append offset; writers
    // round-robin over slots, taking the lock to append. The lock model holds
    // because payload ≤ pack_threshold and the critical section is one small
    // pwrite + fdatasync
    struct ActivePack {
        std::mutex m;
        int fd = -1;
        uint64_t id = 0;
        uint64_t size = 0;  // current append offset = file size
        // Moment the first record landed (steady_clock, criterion for age-based
        // rotation). Monotonic clock rather than wall clock: the question is "how
        // long has this pack been open", and an NTP step must not make it never
        // rotate or rotate immediately
        std::chrono::steady_clock::time_point opened{};
    };

    // Lazily create shard directories + resident dirfd cache (for fsyncing the
    // directory at the end of a write session, §5.1)
    int shard_dirfd(unsigned shard) { return subdir_fd(chunk_dirfds_, "chunks", shard); }
    int pack_dirfd(unsigned shard) { return subdir_fd(pack_dirfds_, "packs", shard); }
    int subdir_fd(std::array<int, 256>& fds, const char* sub, unsigned shard);

    // Append one pack record (blocking pwrite on the calling pool thread; with the
    // uring engine the end-of-batch fdatasync suspends instead of blocking);
    // returns an extent pointing at the payload
    Task<Extent> append_pack_record(std::string_view owner, std::span<const std::byte> payload);
    // Batch append (write_batch's pack path): per-item pwrite inside a single slot
    // lock, one fdatasync at the end (§2.13 batching — a mid-batch rotation seal
    // first flushes the unsynced writes to disk). The slot std::mutex cannot be
    // held across a suspension point, so the pwrites and any rotation fdatasync
    // stay blocking inside the lock; only the final batch fdatasync moves off-lock
    // (on a dup of the pack fd) onto the ring (roadmap §3.4 ⑤)
    Task<std::vector<Extent>> append_pack_records(std::span<const PackAppendItem> items);

    // Sealing split in two steps (docs/archive/gaps.md §3.9): inside the lock only close
    // the fd / clear slot state and push (id,size) onto seal_retry_; seal_'s meta
    // commit (possibly a network RTT/fsync) runs outside the lock in flush_seals.
    // Previously seal_ running inside the slot mutex would block that slot; on
    // throw the fd was already -1 but size not cleared, the next write opened a
    // new pack overwriting the slot state, and the old pack was never sealed
    struct PendingSeal {
        uint64_t id = 0;
        uint64_t size = 0;
    };
    // Shard directory enumeration for chunks/ and packs/ (isomorphic layout,
    // shared by scan_chunks/scan_packs)
    Task<void> scan_shard_tree(const char* sub, const char* suffix,
                               const std::function<void(uint64_t, int64_t, uint64_t)>& cb);

    bool slot_aged(const ActivePack& slot) const;  // call with slot.m held; always false when pack_max_age_sec<=0
    void close_slot_locked(ActivePack& slot);      // call with slot.m held
    // Commit backlogged seals; failures go back onto the queue (the append path
    // warns and retries later, the close path rethrows)
    void flush_seals(bool rethrow);

    FsDataOptions opt_;
    std::shared_ptr<ThreadPool> pool_;
    FileIdAlloc alloc_;
    PackSeal seal_;
    PackMigrateFn migrate_;
    ChunkPinHooks pins_;
    std::mutex dir_mu_;
    std::array<int, 256> chunk_dirfds_;
    std::array<int, 256> pack_dirfds_;
    std::vector<std::unique_ptr<ActivePack>> packs_;  // pack_writers slots
    std::atomic<unsigned> pack_rr_{0};                // round-robin cursor
    std::mutex seal_mu_;
    std::vector<PendingSeal> seal_retry_;  // packs with fd closed but sealing not yet confirmed in meta
};

}  // namespace lights3::storage::duostore
