// L3: local filesystem backend (see docs/storage-backend.md §3)
// Layout: <root>/<bucket>/<key path>, sidecar metadata <data>.lights3-meta,
// PUT writes via <staging>/put/<uuid> then lands atomically with rename.
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <optional>
#include <vector>

#include "core/metrics.h"
#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "core/background.h"
#include "core/timer.h"
#include "storage/backend.h"
#include "storage/localfs/fs_util.h"
#include "storage/localfs/list_cache.h"
#include "storage/meta_cache.h"

namespace lights3::storage {

// ---- Object metadata cache value (roadmap §3.8) ----
// Identity of the data inode as stat(2)/fstat(2) observed it when the metadata was read.
// Every commit path renames a fresh inode over the object (PUT / complete / copy / tier
// stub / cache fill) and the in-place tag rewrite touches ctime, so a stamp mismatch is a
// sufficient signal that the cached record may be stale -- including writes made by
// another process on the same root
struct FsMetaStamp {
    dev_t dev = 0;
    ino_t ino = 0;
    off_t size = 0;
    struct timespec mtime{};
    struct timespec ctime{};
    static FsMetaStamp of(const struct stat& st) {
        return {st.st_dev, st.st_ino, st.st_size, st.st_mtim, st.st_ctim};
    }
    bool operator==(const FsMetaStamp& o) const {
        return dev == o.dev && ino == o.ino && size == o.size &&
               mtime.tv_sec == o.mtime.tv_sec && mtime.tv_nsec == o.mtime.tv_nsec &&
               ctime.tv_sec == o.ctime.tv_sec && ctime.tv_nsec == o.ctime.tv_nsec;
    }
};
struct FsCachedMeta {
    ObjectMeta meta;
    fsutil::TierInfo tier;
    FsMetaStamp stamp;
};
using FsMetaCache = MetaCache<FsCachedMeta>;

// Inheritable by xlocalfs: the data plane (GET/PUT/parts/concatenation) is virtual, layout
// and metadata logic are reused
struct LocalFsOptions {
    // Orphaned multipart cleanup (docs/archive/gaps.md §6.3): previously kMpuTtl was hardcoded to
    // 7 days and only scanned once at startup -- a gateway running for months without a
    // restart would accumulate never-completed/aborted upload directories without bound
    int mpu_ttl_sec = 7 * 86400;          // 0 = no cleanup
    int mpu_scan_interval_sec = 6 * 3600; // 0 = scan only at startup

    // ---- roadmap §3.5 (docs/storage/localfs.md §2/§3/§6/§12) ----
    // Fail at construction (and on every write) when the root filesystem cannot store the
    // metadata xattr, instead of degrading to the two-rename sidecar consistency model
    bool require_xattr = false;
    // Sidecar write policy: sync (default, 4 fsync + 2 rename per PUT) | async (sidecar
    // written off the request path) | lazy (no sidecar while the xattr succeeds)
    fsutil::SidecarMode sidecar = fsutil::SidecarMode::kSync;
    // Pool workers sharing one listing page's stat+getxattr fan-out (<=1 = serial, capped
    // by the pool size; pages below ~32 keys per worker use fewer)
    int list_meta_concurrency = 8;
    // Per-directory sorted-entry cache budget in entries (0 = off) and the directory size
    // below which readdir is cheap enough not to bother caching
    size_t list_cache_entries = size_t(1) << 20;
    size_t list_cache_min_dir_entries = 256;
    // Orphan-sidecar sweep period (a delete crash leaves "<key>.lights3-meta" without its
    // data file; listing self-heals only the directories it visits). 0 = off
    int sidecar_scan_interval_sec = 24 * 3600;

    // ---- Object metadata cache (roadmap §3.8; docs/storage/localfs.md §5.1) ----
    // Budget in objects (0 = off). A hit spares the getxattr / sidecar read + TSV decode;
    // with meta_cache_validate a HEAD still costs one stat(2) (GET already holds an fstat)
    // and a stamp mismatch refetches, so the cache stays correct under writes made by
    // other processes on the same root. validate=false trusts the entry until the
    // backend's own write paths invalidate it or meta_cache_ttl expires -- only for roots
    // this process owns exclusively
    size_t meta_cache_entries = size_t(1) << 16;
    int meta_cache_ttl_sec = 0;  // 0 = no expiry
    bool meta_cache_validate = true;
};

// run_scrub_once knobs (roadmap §3.1); per-call rather than config — a scrub is
// an operator-invoked traversal (CLI), not a resident worker
struct FsScrubOptions {
    uint64_t max_bytes_per_sec = 0;  // 0 = unthrottled
};

// Integrity report of run_scrub_once() (roadmap §3.1). Read-only; every finding
// is a log line plus a counter here. etag_mismatches/read_errors are the "data
// is in danger" signals; the skipped_* and unverifiable buckets exist so a
// clean report can honestly say what it did not cover
struct FsScrubStats {
    uint64_t objects_scanned = 0;
    uint64_t bytes_read = 0;
    uint64_t etag_mismatches = 0;  // content MD5 no longer matches the stored ETag
    uint64_t read_errors = 0;      // open/read failures (EIO, truncation under scrub)
    uint64_t unverifiable = 0;     // no ETag recorded, or legacy multipart without part_sizes
    uint64_t skipped_stubs = 0;    // tiered stubs (data lives remote)
    uint64_t skipped_races = 0;    // object overwritten/deleted mid-verify
    uint64_t orphan_sidecars = 0;  // sidecar whose data file is gone (listing normally self-heals)
    bool aborted = false;          // backend close interrupted the scrub (stats are partial)
};

class LocalFsBackend : public IStorageBackend {
public:
    LocalFsBackend(std::filesystem::path root, std::filesystem::path staging,
                   std::shared_ptr<ThreadPool> pool, LocalFsOptions opt = {},
                   MetricsScope metrics = {});
    ~LocalFsBackend() override;
    // Cancel the periodic cleanup timer and wait for in-flight scans (xlocalfs overrides
    // must chain back to this implementation)
    Task<void> close() override;

    Task<void> create_bucket(std::string_view bucket) override;
    Task<void> delete_bucket(std::string_view bucket) override;
    Task<bool> bucket_exists(std::string_view bucket) override;
    Task<std::vector<BucketInfo>> list_buckets() override;

    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta,
                               http::BodyReader& body,
                               PutCondition cond = {}) override;
    Task<ObjectMeta> head_object(std::string_view bucket, std::string_view key) override;
    // Same-backend copy fast path (docs/archive/gaps.md §6.3): copy_file_range moves data in the
    // kernel (an O(1) clone on reflink-capable filesystems), bypassing user-space buffers.
    // Tier stubs (data not local) return nullopt to fall back to the streaming path
    Task<std::optional<PutResult>> copy_object_fast(std::string_view src_bucket,
                                                    std::string_view src_key,
                                                    std::string_view dst_bucket,
                                                    std::string_view dst_key,
                                                    ObjectMeta meta) override;
    Task<void> delete_object(std::string_view bucket, std::string_view key) override;
    // ?tagging in-place meta rewrite (roadmap §2.5): xattr + sidecar under the per-key commit lock
    Task<void> set_object_tagging(std::string_view bucket, std::string_view key,
                                  std::string tagging) override;
    Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) override;

    // multipart: parts land in <staging>/mpu/<upload_id>/part.NNNNN, complete concatenates
    // and then takes the same atomic rename commit as PUT (docs/storage-backend.md §3.2)
    Task<std::string> create_multipart(std::string_view bucket, std::string_view key,
                                       ObjectMeta meta) override;
    using IStorageBackend::upload_part;
    Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                std::string_view upload_id, int part_no, http::BodyReader& body,
                                const std::optional<PartChecksum>& checksum) override;
    Task<PutResult> complete_multipart(std::string_view bucket, std::string_view key,
                                       std::string_view upload_id,
                                       std::span<const PartInfo> parts) override;
    Task<void> abort_multipart(std::string_view bucket, std::string_view key,
                               std::string_view upload_id) override;
    Task<ListPartsResult> list_parts(std::string_view bucket, std::string_view key,
                                     std::string_view upload_id,
                                     const ListPartsOptions& opt) override;
    Task<ListUploadsResult> list_multipart_uploads(std::string_view bucket,
                                                   const ListUploadsOptions& opt) override;

    // Full-verify scrub (roadmap §3.1): walks every bucket directory, re-reads
    // each object's content and compares the recomputed MD5 against the stored
    // ETag (multipart composites are recomputed from part_sizes; legacy objects
    // without a recorded layout count as unverifiable). Strictly read-only.
    // xlocalfs shares the on-disk format and inherits this unchanged; entry
    // point is the offline `lights3 fsck` command, but it is safe against a
    // live instance — objects mutated mid-verify are re-checked and counted as
    // skipped_races instead of mismatches
    Task<FsScrubStats> run_scrub_once(FsScrubOptions opt = {});

    // Orphan-sidecar sweep (roadmap §3.5): walks every bucket and removes sidecars whose
    // data file is gone, each under the per-key commit lock so an in-flight PUT of the same
    // key can never lose its freshly written sidecar. Returns the number removed. Runs
    // periodically (sidecar_scan_interval) and is exposed for tests/tools
    Task<uint64_t> run_sidecar_sweep_once();

    // Observability hooks for tests (docs/storage/localfs.md §10)
    const fsutil::MetaXattrPolicy& xattr_policy() const { return xattr_; }
    fsutil::DirListCache::Stats list_cache_stats() const { return dir_cache_->stats(); }
    MetaCacheStats meta_cache_stats() const { return meta_cache_->stats(); }
    const LocalFsOptions& options() const { return opt_; }
    // Drop the cached record of one object. Composite backends that commit through
    // fs_util primitives directly (tiered stub / cache-fill commits) must call this after
    // their commit point; the stat stamp would catch it anyway under
    // meta_cache_validate, this keeps validate=false exact for in-process writers
    void invalidate_object_meta(std::string_view bucket, std::string_view key) {
        meta_cache_->invalidate(bucket, key);
    }

    static constexpr const char* kSidecarSuffix = fsutil::kSidecarSuffix;
    static constexpr const char* kBucketMarker = fsutil::kBucketMarker;

    // ---- Layout access needed by composite backends (tiered, docs/tiered-storage.md §2) ----
    const std::filesystem::path& root() const { return root_; }
    const std::filesystem::path& staging() const { return staging_; }
    const std::shared_ptr<ThreadPool>& pool() const { return pool_; }
    std::filesystem::path object_data_path(std::string_view bucket, std::string_view key) const {
        return object_path(bucket, key);
    }

protected:
    std::filesystem::path bucket_dir(std::string_view bucket) const;
    std::filesystem::path object_path(std::string_view bucket, std::string_view key) const;
    void require_bucket(std::string_view bucket) const;      // throws NoSuchBucket if missing
    ObjectMeta load_meta(const std::filesystem::path& data_path, std::string key) const;

    // ---- metadata cache plumbing (roadmap §3.8) ----
    // Authoritative read (xattr/sidecar) over a stat result the caller holds, filling the
    // cache with the record + that stat's stamp. tok must predate the metadata read (the
    // lookup that missed hands it out), so a write racing the read cannot leave a stale
    // record behind
    ObjectMeta meta_from_stat(std::string_view bucket, std::string_view key,
                              const std::filesystem::path& path, const struct stat& st,
                              const FsMetaCache::Token& tok,
                              fsutil::TierInfo* tier_out = nullptr) const;
    // Invalidation on frame exit, declared right before a commit section (xlocalfs's
    // ring-based commits share it); see MetaCache::InvalidateGuard
    [[nodiscard]] FsMetaCache::InvalidateGuard invalidate_on_exit(std::string_view bucket,
                                                                  std::string_view key) {
        return meta_cache_->invalidate_on_exit(bucket, key);
    }

    // ---- Data-plane accounting (docs/archive/gaps.md §7) ----
    // Enum values are the metric array indices; the data-plane methods xlocalfs overrides
    // share the same instances (overrides don't go through the base implementation, each
    // instruments at its own entry, so no double counting by construction)
    enum class Op : size_t {
        kPut, kGet, kHead, kDelete, kList, kCopy, kUploadPart, kCompleteMpu
    };
    static constexpr size_t kOpCount = 8;
    void record_op(Op op, double secs, bool ok);
    // RAII inside the coroutine frame: accounts on frame destruction (including exception
    // unwinding), ok defaults to false -- any exit path that never reached the success
    // flag (S3Error, errno exception, body disconnect) counts as an error, no need to add
    // code before every throw
    struct OpGuard {
        LocalFsBackend* self;
        Op op;
        std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();
        bool ok = false;
        ~OpGuard() {
            self->record_op(op,
                            std::chrono::duration<double>(std::chrono::steady_clock::now() -
                                                          start).count(),
                            ok);
        }
    };

    // Per-key serialization of the commit section (striped, same as tiered's key_lock):
    // sidecar and data file are two renames, so without the lock concurrent PUTs of the
    // same key can interleave into a torn object of "data=A, sidecar(etag)=B". Guards only
    // the commit section; body reads/writes remain fully concurrent; xlocalfs inherits the
    // same lock
    static constexpr size_t kLockStripes = 64;
    AsyncSemaphore& commit_lock(std::string_view bucket, std::string_view key);

    // Commit-time policy shared with xlocalfs (xattr accounting / fail-fast + sidecar mode)
    fsutil::CommitOptions commit_options() const {
        fsutil::CommitOptions co;
        co.sidecar = opt_.sidecar;
        co.xattr = &xattr_;
        return co;
    }
    // SidecarMode::kAsync: write the sidecar from a background task (bg_-tracked, so close
    // waits for it); falls back to an inline write when the backend is already closing
    void defer_sidecar(std::filesystem::path dest, ObjectMeta meta);

    std::filesystem::path root_;
    std::filesystem::path staging_;
    std::shared_ptr<ThreadPool> pool_;
    LocalFsOptions opt_;
    mutable fsutil::MetaXattrPolicy xattr_;
    std::unique_ptr<FsMetaCache> meta_cache_;  // roadmap §3.8; xlocalfs's data plane consults it too

private:
    // run_scrub_once helpers (pool thread): one bucket's ordered walk, then one
    // object's hash-and-compare; md5_range streams [off, off+len) of an open fd
    Task<void> scrub_bucket(const std::string& bucket, const std::filesystem::path& dir,
                            class ScrubThrottle& throttle, std::vector<uint8_t>& buf,
                            FsScrubStats& st);
    Task<void> scrub_object(const std::string& bucket, const std::string& key,
                            const std::filesystem::path& path, class ScrubThrottle& throttle,
                            std::vector<uint8_t>& buf, FsScrubStats& st);
    Task<std::string> md5_range(int fd, uint64_t off, uint64_t len, class ScrubThrottle& throttle,
                                std::vector<uint8_t>& buf, FsScrubStats& st);

    // list_objects helpers (roadmap §3.5 ①): the page's metadata is loaded by `stride`
    // strided workers over the pool (each key = stat + getxattr; a key deleted between
    // readdir and stat is dropped, not an error), then orphan sidecars spotted during the
    // walk are reaped under the per-key lock
    Task<void> load_page_meta(const std::filesystem::path& base,
                              const std::vector<std::string>& keys,
                              std::vector<ObjectMeta>& out);
    Task<void> load_meta_slice(const std::filesystem::path& base,
                               const std::vector<std::string>& keys, size_t first, size_t stride,
                               std::vector<std::optional<ObjectMeta>>& metas);
    Task<bool> reap_orphan_sidecar(std::string bucket, std::filesystem::path sidecar);
    Task<void> reap_orphan_sidecars(std::string bucket, std::vector<std::filesystem::path> list);

    void init_metrics(const MetricsScope& metrics);  // one-time acquisition at construction (same pattern as duostore)
    void cleanup_stale_uploads();  // remove mpu directories past mpu_ttl (startup + periodic)
    Task<void> mpu_scan_task();    // pool hop + cleanup_stale_uploads
    Task<void> sidecar_sweep_task();
    // Re-arm a periodic maintenance task after it completes (same shape as duostore's GC
    // worker): runs never overlap, a slow run just pushes back the next trigger
    void schedule_periodic(TimerQueue::Id& id, int interval_sec,
                           Task<void> (LocalFsBackend::*fn)());
    Task<void> run_periodic(TimerQueue::Id* slot, int interval_sec,
                            Task<void> (LocalFsBackend::*fn)());  // one run, then re-arm
    void shutdown_background();    // shared by close/dtor: cancel timers + wait in-flight scans

    // Instances fully pre-registered across the op dimension (acquired at construction,
    // hot path only inc/observe); the latency histogram is non-null only at the
    // put/get/list indices, record_op skips null ones
    std::array<std::shared_ptr<MetricCounter>, kOpCount> m_ops_, m_op_errors_;
    std::array<std::shared_ptr<MetricHistogram>, kOpCount> m_op_seconds_;
    std::shared_ptr<MetricCounter> m_orphans_removed_;
    std::vector<std::unique_ptr<AsyncSemaphore>> commit_locks_;
    std::unique_ptr<fsutil::DirListCache> dir_cache_;
    BackgroundTaskGroup bg_{"localfs"};
    TimerQueue::Id mpu_timer_ = 0;      // written only inside bg_.if_open; 0 = not armed
    TimerQueue::Id sidecar_timer_ = 0;  // same discipline
    std::atomic<bool> closed_{false};
};

}  // namespace lights3::storage
