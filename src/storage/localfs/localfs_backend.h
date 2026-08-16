// L3: local filesystem backend (see docs/storage-backend.md §3)
// Layout: <root>/<bucket>/<key path>, sidecar metadata <data>.lights3-meta,
// PUT writes via <staging>/put/<uuid> then lands atomically with rename.
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <filesystem>
#include <memory>
#include <vector>

#include "core/metrics.h"
#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "core/background.h"
#include "core/timer.h"
#include "storage/backend.h"
#include "storage/localfs/fs_util.h"

namespace lights3::storage {

// Inheritable by xlocalfs: the data plane (GET/PUT/parts/concatenation) is virtual, layout
// and metadata logic are reused
struct LocalFsOptions {
    // Orphaned multipart cleanup (docs/gaps.md §6.3): previously kMpuTtl was hardcoded to
    // 7 days and only scanned once at startup -- a gateway running for months without a
    // restart would accumulate never-completed/aborted upload directories without bound
    int mpu_ttl_sec = 7 * 86400;          // 0 = no cleanup
    int mpu_scan_interval_sec = 6 * 3600; // 0 = scan only at startup
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
    // Same-backend copy fast path (docs/gaps.md §6.3): copy_file_range moves data in the
    // kernel (an O(1) clone on reflink-capable filesystems), bypassing user-space buffers.
    // Tier stubs (data not local) return nullopt to fall back to the streaming path
    Task<std::optional<PutResult>> copy_object_fast(std::string_view src_bucket,
                                                    std::string_view src_key,
                                                    std::string_view dst_bucket,
                                                    std::string_view dst_key,
                                                    ObjectMeta meta) override;
    Task<void> delete_object(std::string_view bucket, std::string_view key) override;
    Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) override;

    // multipart: parts land in <staging>/mpu/<upload_id>/part.NNNNN, complete concatenates
    // and then takes the same atomic rename commit as PUT (docs/storage-backend.md §3.2)
    Task<std::string> create_multipart(std::string_view bucket, std::string_view key,
                                       ObjectMeta meta) override;
    Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                std::string_view upload_id, int part_no,
                                http::BodyReader& body) override;
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

    // ---- Data-plane accounting (docs/gaps.md §7) ----
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

    std::filesystem::path root_;
    std::filesystem::path staging_;
    std::shared_ptr<ThreadPool> pool_;

private:
    void init_metrics(const MetricsScope& metrics);  // one-time acquisition at construction (same pattern as duostore)
    void cleanup_stale_uploads();  // remove mpu directories past mpu_ttl (startup + periodic)
    void schedule_mpu_scan();      // re-arm after completion (same shape as duostore's GC worker)
    void shutdown_background();    // shared by close/dtor: cancel timer + wait in-flight scans

    LocalFsOptions opt_;
    // Instances fully pre-registered across the op dimension (acquired at construction,
    // hot path only inc/observe); the latency histogram is non-null only at the
    // put/get/list indices, record_op skips null ones
    std::array<std::shared_ptr<MetricCounter>, kOpCount> m_ops_, m_op_errors_;
    std::array<std::shared_ptr<MetricHistogram>, kOpCount> m_op_seconds_;
    std::vector<std::unique_ptr<AsyncSemaphore>> commit_locks_;
    BackgroundTaskGroup bg_{"localfs"};
    TimerQueue::Id mpu_timer_ = 0;  // written only inside bg_.if_open; 0 = not armed
    std::atomic<bool> closed_{false};
};

}  // namespace lights3::storage
