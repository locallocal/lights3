// L3: the local ("hot") side of TieredBackend behind an interface (roadmap §3.6 ⑥).
// TieredBackend used to be hard-bound to LocalFsBackend (object paths, sidecars, fs_util
// commit primitives). Everything tiered needs from its local side is collected here:
// the tier state of an object, per-object access records, an upload snapshot, the two
// atomic commit primitives (stub / cached), space probes, a full enumeration for
// rescans, and an optional block-level range cache. tier_local_fs.{h,cc} implements it
// over the localfs/xlocalfs disk layout, tier_local_duo.{h,cc} over duostore's meta.
#pragma once

#include <cstdint>
#include <filesystem>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "core/task.h"
#include "storage/backend.h"
#include "storage/localfs/fs_util.h"  // Tier / TierInfo are the shared vocabulary

namespace lights3::storage::tier {

using fsutil::Tier;
using fsutil::TierInfo;

// One object's tiering state as the local side sees it
struct LocalObject {
    ObjectMeta meta;           // external metadata (etag = the original, never the cloud's)
    TierInfo tier;
    uint64_t local_bytes = 0;  // object bytes actually held locally (0 for a clean stub)
    int64_t mtime = 0;         // epoch seconds of the local record (access-time fallback)
};

// Per-object access record (docs/tiered-storage.md §4.3): last access, a saturating
// access count (frequency-aware eviction, roadmap §3.6 ③) and the time-wheel slot the
// key was last enrolled in (incremental scanning, ①). Persisted by the local side
// (xattr on the data file for localfs; a resident table for duostore); the backend
// keeps only a write-behind buffer
struct AccessRec {
    int64_t atime = 0;
    uint32_t hits = 0;
    int64_t enrolled = -1;  // -1 = not enrolled
};

// Full-rescan enumeration item
struct WalkEntry {
    std::string bucket, key;
    Tier tier = Tier::kLocal;
    uint64_t size = 0;         // logical object size
    uint64_t local_bytes = 0;  // bytes held locally (stub = 0)
    int64_t mtime = 0;
};

// Pull-style enumeration of every object on the local side; next() returns batches
// (empty = done). Implementations hop to the pool themselves
class IWalker {
public:
    virtual ~IWalker() = default;
    virtual Task<std::vector<WalkEntry>> next() = 0;
};

// One whole-object cache fill in progress: bytes are pushed in as the cloud stream is
// consumed, then committed as tier=cached under the backend's per-key lock (the caller
// re-verifies the state first and passes what it read). Destruction without commit
// discards everything
class ICacheFill {
public:
    virtual ~ICacheFill() = default;
    virtual bool write(const std::byte* p, size_t n) = 0;  // false = local write failure (caller degrades)
    virtual Task<void> commit(const ObjectMeta& meta, const TierInfo& tier) = 0;
};

// Block-level partial cache of a remote object (roadmap §3.6 ⑦): a sparse file plus a
// presence bitmap, keyed to one cloud replica (remote_etag). Lives in the tier state
// directory, never in the object tree
class IRangeCache {
public:
    virtual ~IRangeCache() = default;
    virtual uint64_t block_size() const = 0;
    virtual uint64_t size() const = 0;  // object size
    virtual bool has(uint64_t first, uint64_t last) const = 0;  // every block covering [first,last] present
    virtual std::unique_ptr<http::BodyReader> open(uint64_t first, uint64_t last) = 0;
    virtual bool write(uint64_t off, const std::byte* p, size_t n) = 0;  // pwrite; false = failure
    // Persist presence of blocks [first_block, last_block] (merged with what is on disk
    // — concurrent fillers of the same key only ever add bits)
    virtual void mark_present(uint64_t first_block, uint64_t last_block) = 0;
    virtual uint64_t resident_bytes() const = 0;  // real disk usage of the cache file
};

class ITierLocal {
public:
    virtual ~ITierLocal() = default;

    virtual IStorageBackend& backend() = 0;
    // tiered's private state (gc queue, time wheel, quarantine, range cache) — must be on
    // the same filesystem as tmp_dir() (rename atomicity)
    virtual const std::filesystem::path& state_dir() const = 0;
    virtual std::filesystem::path tmp_dir() const = 0;
    virtual const char* kind() const = 0;  // "localfs" / "duostore" (logs, docs)
    virtual Task<void> close() = 0;

    // ---- state (synchronous; call on a pool thread) ----
    virtual std::optional<LocalObject> read(std::string_view bucket, std::string_view key) = 0;
    // Cheap tier-only read for the PUT/DELETE pre-check; missing object → local
    virtual TierInfo read_tier_only(std::string_view bucket, std::string_view key) = 0;

    // ---- access records (synchronous) ----
    virtual std::optional<AccessRec> load_access(std::string_view bucket, std::string_view key) = 0;
    virtual void store_access(std::string_view bucket, std::string_view key, const AccessRec& rec) = 0;
    virtual void erase_access(std::string_view bucket, std::string_view key) = 0;
    // true = records live in a resident table that needs flush_access() to persist
    virtual bool access_resident() const = 0;
    virtual void flush_access() = 0;

    // ---- data plane ----
    // Stable byte stream of the object's current local data (an inode/extent snapshot:
    // a concurrent overwrite or stubbing must not change what this stream yields)
    virtual Task<std::unique_ptr<http::BodyReader>> open_snapshot(std::string_view bucket,
                                                                  std::string_view key,
                                                                  uint64_t size) = 0;
    // Stub commit (docs/tiered-storage.md §5.2 ④): caller holds the per-key lock and has
    // verified the state; also finishes a half-done stub (remote with data still present)
    virtual Task<void> commit_stub(std::string_view bucket, std::string_view key,
                                   const ObjectMeta& meta, const TierInfo& tier) = 0;
    virtual std::unique_ptr<ICacheFill> begin_cache_fill(std::string_view bucket,
                                                         std::string_view key) = 0;  // null = cannot

    // ---- space ----
    virtual bool cache_space_ok(uint64_t size, uint64_t min_free_bytes) const = 0;
    // Used fraction and total bytes of the filesystem holding the local data (statvfs);
    // nullopt when unavailable
    virtual std::optional<std::pair<double, uint64_t>> disk_usage() const = 0;

    // ---- enumeration ----
    virtual std::unique_ptr<IWalker> walk() = 0;

    // ---- range cache (optional) ----
    virtual bool supports_range_cache() const { return false; }
    virtual std::unique_ptr<IRangeCache> open_range_cache(std::string_view, std::string_view,
                                                          const LocalObject&, uint64_t) {
        return nullptr;
    }
    virtual void drop_range_cache(std::string_view, std::string_view) {}
    virtual uint64_t range_cache_bytes(std::string_view, std::string_view) const { return 0; }
    // Housekeeping for the full rescan: drop cache entries whose object is no longer a
    // remote stub; returns the bytes still resident afterwards
    virtual uint64_t sweep_range_cache() { return 0; }
};

}  // namespace lights3::storage::tier
