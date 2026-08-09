// L3: 本地文件系统后端（见 docs/storage-backend.md §3）
// 布局：<root>/<bucket>/<key路径>，sidecar 元数据 <data>.lights3-meta，
// PUT 经 <staging>/put/<uuid> 写入后 rename 原子落地。
#pragma once

#include <atomic>
#include <filesystem>
#include <memory>
#include <vector>

#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "core/background.h"
#include "core/timer.h"
#include "storage/backend.h"
#include "storage/localfs/fs_util.h"

namespace lights3::storage {

// 可被 xlocalfs 继承：数据面（GET/PUT/分片/拼接）为 virtual，布局与元数据逻辑复用
struct LocalFsOptions {
    // 孤儿 multipart 清理（docs/gaps.md §6.3）：此前 kMpuTtl 硬编码 7 天且只在
    // 启动时扫一次——跑数月不重启的网关会无限累积从未 complete/abort 的上传目录
    int mpu_ttl_sec = 7 * 86400;          // 0 = 不清理
    int mpu_scan_interval_sec = 6 * 3600; // 0 = 只在启动扫
};

class LocalFsBackend : public IStorageBackend {
public:
    LocalFsBackend(std::filesystem::path root, std::filesystem::path staging,
                   std::shared_ptr<ThreadPool> pool, LocalFsOptions opt = {});
    ~LocalFsBackend() override;
    // 撤销周期清理定时器并等在途扫描（xlocalfs 覆写时必须链回本实现）
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
    // 同后端 copy 快路径（docs/gaps.md §6.3）：copy_file_range 内核侧搬运（支持
    // reflink 的文件系统上是 O(1) 克隆），不经用户态缓冲。tier stub（数据不在
    // 本地）返回 nullopt 回落流式路径
    Task<std::optional<PutResult>> copy_object_fast(std::string_view src_bucket,
                                                    std::string_view src_key,
                                                    std::string_view dst_bucket,
                                                    std::string_view dst_key,
                                                    ObjectMeta meta) override;
    Task<void> delete_object(std::string_view bucket, std::string_view key) override;
    Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) override;

    // multipart：分片落 <staging>/mpu/<upload_id>/part.NNNNN，complete 拼接后
    // 走与 PUT 相同的 rename 原子提交（docs/storage-backend.md §3.2）
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

    // ---- 组合后端（tiered，docs/tiered-storage.md §2）需要的布局访问 ----
    const std::filesystem::path& root() const { return root_; }
    const std::filesystem::path& staging() const { return staging_; }
    const std::shared_ptr<ThreadPool>& pool() const { return pool_; }
    std::filesystem::path object_data_path(std::string_view bucket, std::string_view key) const {
        return object_path(bucket, key);
    }

protected:
    std::filesystem::path bucket_dir(std::string_view bucket) const;
    std::filesystem::path object_path(std::string_view bucket, std::string_view key) const;
    void require_bucket(std::string_view bucket) const;      // 不存在抛 NoSuchBucket
    ObjectMeta load_meta(const std::filesystem::path& data_path, std::string key) const;

    // 提交段的 per-key 串行化（striped，同 tiered 的 key_lock）：sidecar 与数据文件
    // 是两次 rename，不加锁时并发 PUT 同 key 可交错成"数据=A、sidecar(etag)=B"的
    // 撕裂对象。只护提交段，body 读写仍全并发；xlocalfs 继承同一把锁
    static constexpr size_t kLockStripes = 64;
    AsyncSemaphore& commit_lock(std::string_view bucket, std::string_view key);

    std::filesystem::path root_;
    std::filesystem::path staging_;
    std::shared_ptr<ThreadPool> pool_;

private:
    void cleanup_stale_uploads();  // 清理超期（mpu_ttl）的 mpu 目录（启动 + 周期）
    void schedule_mpu_scan();      // 完成后重臂（同 duostore GC worker 形态）
    void shutdown_background();    // close/dtor 共用：撤定时器 + 等在途扫描

    LocalFsOptions opt_;
    std::vector<std::unique_ptr<AsyncSemaphore>> commit_locks_;
    BackgroundTaskGroup bg_{"localfs"};
    TimerQueue::Id mpu_timer_ = 0;  // 只在 bg_.if_open 内写；0 = 未 arm
    std::atomic<bool> closed_{false};
};

}  // namespace lights3::storage
