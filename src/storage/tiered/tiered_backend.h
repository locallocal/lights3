// L3: 分层存储组合后端（docs/08-tiered-storage.md）
// 组合 local（须为 localfs/xlocalfs，共享磁盘布局）与 cloud（任意 IStorageBackend）：
// 冷对象上传云端后本地 stub 化，访问时透明回读并 Tee 缓存回本地。
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/config.h"
#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "core/timer.h"
#include "storage/backend.h"
#include "storage/localfs/localfs_backend.h"

namespace lights3::storage {

struct TieredConfig {
    int64_t cold_after_sec = 30 * 24 * 3600;  // 判冷阈值（docs/08 §5.1）
    int64_t scan_interval_sec = 3600;         // 0 = 关闭后台任务（测试用手动钩子）
    double space_high_watermark = 0.85;       // 触发空间回收
    double space_low_watermark = 0.70;        // 回收目标
    uint64_t min_free_bytes = 1ull << 30;     // 缓存回填所需最小余量（需求 3）
    bool cache_fill_on_range = true;          // Range 命中 remote 时后台整对象回迁
    int max_concurrent_transfers = 4;
    uint64_t quota_bytes = 0;                 // 0 = 不启用逻辑配额
};

class TieredBackend final : public IStorageBackend,
                            public std::enable_shared_from_this<TieredBackend> {
public:
    // StorageRegistry 两阶段构建入口：params 里的 local/cloud 以 name 引用已构造后端
    static std::shared_ptr<TieredBackend> from_config(
        const BackendConfig& cfg,
        const std::map<std::string, std::shared_ptr<IStorageBackend>>& built,
        std::shared_ptr<ThreadPool> pool);

    TieredBackend(std::shared_ptr<LocalFsBackend> local, std::shared_ptr<IStorageBackend> cloud,
                  std::shared_ptr<ThreadPool> pool, TieredConfig cfg);
    ~TieredBackend() override;

    // ---- bucket：全部委托 local（云侧 bucket 在首次下沉时惰性创建）----
    Task<void> create_bucket(std::string_view bucket) override;
    Task<void> delete_bucket(std::string_view bucket) override;
    Task<bool> bucket_exists(std::string_view bucket) override;
    Task<std::vector<BucketInfo>> list_buckets() override;

    // ---- object：tier 感知（docs/08 §6/§7）----
    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta,
                               http::BodyReader& body) override;
    Task<ObjectMeta> head_object(std::string_view bucket, std::string_view key) override;
    Task<void> delete_object(std::string_view bucket, std::string_view key) override;
    Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) override;

    // ---- multipart：全部委托 local；complete 覆盖旧云副本时入 GC ----
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
    Task<std::vector<PartMeta>> list_parts(std::string_view bucket, std::string_view key,
                                           std::string_view upload_id) override;
    Task<std::vector<UploadInfo>> list_multipart_uploads(std::string_view bucket) override;

    // 停止后台定时任务、等待在途后台协程、落盘 atime 快照。不关闭子后端
    //（它们由 registry 独立持有，可能同时被直接路由）
    Task<void> close() override;

    // ---- 后台任务与测试钩子（docs/08 §10 P1"手动触发"）----
    // 单对象下沉：local → remote（上传+stub 化）或 cached → remote（零流量 stub 化）。
    // 前置不满足（已 remote / 在途任务）静默返回；被并发写打败时云副本入 GC
    Task<void> demote_object(std::string bucket, std::string key);
    // 单对象整体回迁：remote → cached（云端 GET → staging → 校验 → 提交）；
    // Range GET 命中 remote 时由后台调用（single-flight）
    Task<void> promote_object(std::string bucket, std::string key);
    // 一轮扫描：判冷 + 空间水位回收 + 崩溃恢复（remote 但数据未回收）+ atime 快照
    Task<void> scan_once();
    // 消费一轮 GC 队列：删除孤儿云副本（校验 etag，绝不删活副本）
    Task<void> run_gc_once();

    const TieredConfig& config() const { return cfg_; }

private:
    friend class TeeCacheReader;
    friend struct InflightRelease;

    // ---- per-key 锁（docs/08 §7.3）：striped 异步互斥，只保护状态提交段 ----
    static constexpr size_t kLockStripes = 64;
    AsyncSemaphore& key_lock(std::string_view bucket, std::string_view key);

    // ---- 在途表：single-flight 缓存回填 + 保护下沉上传不被 GC 误删 ----
    bool inflight_try_begin(const std::string& ikey);
    void inflight_end(const std::string& ikey);
    bool inflight_contains(const std::string& ikey);
    static std::string make_ikey(std::string_view bucket, std::string_view key) {
        return std::string(bucket) + "/" + std::string(key);
    }

    // ---- TierIndex：atime 表（docs/08 §4.3）----
    void touch_atime(std::string_view bucket, std::string_view key);
    void erase_atime(std::string_view bucket, std::string_view key);
    int64_t atime_or(const std::string& ikey, int64_t fallback);
    void load_atime_snapshot();
    void save_atime_snapshot();

    // ---- GC 队列（docs/08 §7.2）：<staging>/tier/gc/<seq> 每项一个 TSV ----
    void enqueue_gc(std::string_view bucket, std::string_view key, std::string_view remote_etag);

    // 云侧 bucket 惰性创建（并发下 AlreadyOwned 视为成功）
    Task<void> ensure_cloud_bucket(std::string_view bucket);
    // 缓存回填提交：per-key 锁内复核 sidecar 仍为同一 remote 版本后 rename+sidecar
    Task<void> commit_cache_fill(std::string bucket, std::string key, ObjectMeta expect,
                                 fsutil::TierInfo expect_tier, fsutil::TmpFile& tmp);
    // statvfs 余量预检（docs/08 §6.2 步骤②）
    bool cache_space_ok(uint64_t size) const;

    // 后台协程管理：spawn 计数 + close() 等待归零
    void spawn_background(Task<void> t);
    void on_background_done();
    void schedule_scan();
    void schedule_snapshot();

    Task<void> demote_quiet(std::string bucket, std::string key);
    Task<void> promote_quiet(std::string bucket, std::string key);
    Task<void> scan_and_gc();
    Task<void> snapshot_task();

    std::shared_ptr<LocalFsBackend> local_;
    std::shared_ptr<IStorageBackend> cloud_;
    std::shared_ptr<ThreadPool> pool_;
    TieredConfig cfg_;
    std::filesystem::path tier_dir_;  // <staging>/tier
    std::filesystem::path gc_dir_;    // <staging>/tier/gc

    std::vector<std::unique_ptr<AsyncSemaphore>> key_locks_;
    AsyncSemaphore transfers_;  // max_concurrent_transfers 限流（docs/08 §5.1）

    std::mutex inflight_m_;
    std::set<std::string> inflight_;

    std::mutex atime_m_;
    std::unordered_map<std::string, int64_t> atime_;  // ikey → epoch 秒

    std::atomic<uint64_t> gc_seq_{0};

    std::mutex bg_m_;
    std::condition_variable bg_cv_;
    int bg_count_ = 0;
    bool closing_ = false;
    TimerQueue::Id scan_timer_ = 0;
    bool scan_timer_armed_ = false;
    TimerQueue::Id snap_timer_ = 0;  // atime 快照周期（§4.3，固定 5 min）
    bool snap_timer_armed_ = false;
};

}  // namespace lights3::storage
