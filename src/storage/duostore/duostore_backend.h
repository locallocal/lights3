// L3: DuoStore 后端门面（docs/duostore-backend.md）：S3 语义、ETag/MD5、泵送循环；
// 元数据/数据分离到 IMetaStore / IDataStore 两个可插拔接口，DataRef 为唯一耦合点。
// P1：RocksDB meta + chunk 数据路径；P3：GC 一期（gcq 消费 + pin 计数 + mpu_ttl +
// 后台 worker）；pack 聚合（P2）与压实/孤儿扫描（P4）后续引入。
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <map>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <unordered_map>
#include <vector>

#include "core/background.h"
#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "core/timer.h"
#include "storage/backend.h"
#include "storage/duostore/data_store.h"
#include "storage/duostore/meta_store.h"

namespace lights3::storage {

namespace duostore {

// 进程内 pin 计数（主文档 §7）：open_reader 把 ref 涉及的全部 file_id 注册进来，
// reader 析构解除；GC 对 pin>0 的 file 跳过本轮，防懒打开 vs unlink 的 ENOENT。
// 单进程独占 root 是既有前提，进程内计数即完全正确；shared_ptr 共享给 reader——
// ObjectStream 会随 HTTP 响应逃逸出 backend 生命周期，pin 表不得依赖 backend 存活
struct PinTable {
    // 逐 extent 计一次（同 file_id 多 extent 多次计入），pin/unpin 按同一 id 列表对称
    std::vector<uint64_t> pin(std::span<const Extent> extents);
    void unpin(const std::vector<uint64_t>& ids);
    bool any_pinned(std::span<const Extent> extents);
    bool pinned(uint64_t file_id);

private:
    // file_id 哈希分片：GET 热路径的 pin/unpin 与 GC 批量 any_pinned 不共抢一把锁
    static constexpr size_t kShards = 16;
    struct Shard {
        std::mutex m;
        std::unordered_map<uint64_t, int> refs;
    };
    Shard& shard_of(uint64_t id) { return shards_[id % kShards]; }
    std::array<Shard, kShards> shards_;
};

// run_gc_once() 的回收统计（§9.1；测试断言与后续指标接入用）
struct DuoGcStats {
    uint64_t reclaims_acked = 0;    // 销账的 gcq 项数
    uint64_t files_removed = 0;     // 物理删除的 chunk/rados extent 数（pack record 不计）
    uint64_t skipped_grace = 0;     // 未逾 gc_grace 而跳过的 gcq 项数
    uint64_t skipped_pinned = 0;    // 所涉 file 有 pin 而跳过的 gcq 项数
    uint64_t packs_removed = 0;     // 整文件删除的空 pack 数（P2 前 pack_stats 恒空）
    uint64_t uploads_expired = 0;   // mpu_ttl 过期而内部 abort 的 multipart 数
};

}  // namespace duostore

// meta 引擎选择（docs/duostore-redis-meta.md §8 / docs/duostore-sqlite-meta.md §8 /
// docs/duostore-tikv-meta.md §9）：redis / sqlite / tikv 需编译期开启对应 option，
// 否则 from_params 抛 "not compiled in"
enum class DuoMetaKind { kRocksDb, kRedis, kSqlite, kTikv };

// data 引擎选择（docs/duostore-rados-data.md §10，对偶 meta_kind）：
// rados 需编译期开启 LIGHTS3_DUOSTORE_RADOS_DATA
enum class DuoDataKind { kFs, kRados };

struct DuoStoreConfig {
    std::string name;
    std::filesystem::path root;       // 必填；meta/ chunks/ packs/ 均在其下
    std::filesystem::path meta_path;  // 默认 <root>/meta（可单独指到 SSD）
    DuoMetaKind meta_kind = DuoMetaKind::kRocksDb;
    std::string redis_uri;                // meta=redis 时必填
    std::string redis_prefix = "duo:";    // key 前缀（多实例/测试隔离）
    int redis_timeout_sec = 3;            // 建连 + 单命令超时
    int redis_pool_size = 8;              // 连接池大小
    std::filesystem::path sqlite_path;    // meta=sqlite：DB 文件，默认 <root>/meta.sqlite3
    size_t sqlite_cache = 64ull << 20;    // 页缓存（PRAGMA cache_size）
    std::vector<std::string> pd_endpoints;  // meta=tikv 时必填（docs/duostore-tikv-meta.md §9）
    std::string tikv_prefix = "duo:";       // key 前缀（多实例/测试隔离）
    std::string tikv_ca;                    // mTLS 三件套（三者同给才启用）
    std::string tikv_cert;
    std::string tikv_key;
    DuoDataKind data_kind = DuoDataKind::kFs;
    std::string rados_conf = "/etc/ceph/ceph.conf";  // data=rados 键（docs/duostore-rados-data.md §10）
    std::string rados_client = "client.admin";
    std::string rados_pool;                          // data=rados 时必填
    std::string rados_namespace;                     // pool 内逻辑隔离（多实例/测试）
    uint64_t rados_chunk_size = 8ull << 20;
    uint64_t rados_buffer_total = 256ull << 20;
    int rados_connect_timeout_sec = 5;
    int rados_op_timeout_sec = 0;                    // 0 = 不设 op 超时
    uint64_t chunk_size = 8ull << 20;
    uint64_t pack_threshold = 128 << 10;   // P2 生效
    uint64_t pack_max_size = 128ull << 20;
    int pack_writers = 4;
    double pack_gc_ratio = 0.5;
    int gc_interval_sec = 300;             // P3 生效
    int gc_grace_sec = 300;
    int orphan_scan_interval_sec = 86400;  // P4 生效
    int mpu_ttl_sec = 7 * 86400;
    bool meta_sync = true;
    bool verify_chunk_crc = false;
    size_t rocksdb_block_cache = 64ull << 20;

    // 集中解析 + 范围校验（docs/duostore-backend.md §11）；配置错误抛 std::runtime_error
    static DuoStoreConfig from_params(const std::string& name,
                                      const std::map<std::string, std::string>& params);
};

class DuoStoreBackend final : public IStorageBackend {
public:
    DuoStoreBackend(DuoStoreConfig cfg, std::shared_ptr<ThreadPool> pool);
    // 测试注入用：自组装 meta/data
    DuoStoreBackend(DuoStoreConfig cfg, std::shared_ptr<ThreadPool> pool,
                    std::unique_ptr<duostore::IMetaStore> meta,
                    std::unique_ptr<duostore::IDataStore> data);
    ~DuoStoreBackend() override;

    Task<void> create_bucket(std::string_view bucket) override;
    Task<void> delete_bucket(std::string_view bucket) override;
    Task<bool> bucket_exists(std::string_view bucket) override;
    Task<std::vector<BucketInfo>> list_buckets() override;

    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta,
                               http::BodyReader& body) override;
    Task<ObjectMeta> head_object(std::string_view bucket, std::string_view key) override;
    Task<void> delete_object(std::string_view bucket, std::string_view key) override;
    Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) override;

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

    Task<void> close() override;

    // GC 一期手动钩子（§9：测试直调，仿 tiered "手动触发下沉"先例）：
    // mpu_ttl 过期清理 + gcq 消费（grace/pin 过滤，先物理删后销账）+ 空 pack 整删。
    // 与后台 worker 经内部信号量互斥，可并发调用
    Task<duostore::DuoGcStats> run_gc_once();

private:
    void require_bucket(std::string_view bucket);  // 池线程调用
    // 取对象记录；缺失时区分 NoSuchBucket / NoSuchKey（GET/HEAD 错误语义必须一致）
    duostore::ObjectRec require_object(std::string_view bucket, std::string_view key);

    // 后台 GC 管理（§9 生命周期）：BackgroundTaskGroup 等待组 + 完成后重臂的
    // 单 worker tick；close/dtor 共用 shutdown_background（begin_close → 锁外
    // cancel 定时器[TimerQueue::cancel 阻塞等在途回调] → 等在途 GC 清零）
    void schedule_gc();
    Task<void> gc_tick();
    void shutdown_background();

    DuoStoreConfig cfg_;
    std::shared_ptr<ThreadPool> pool_;
    std::unique_ptr<duostore::IMetaStore> meta_;
    std::unique_ptr<duostore::IDataStore> data_;
    std::shared_ptr<duostore::PinTable> pins_ = std::make_shared<duostore::PinTable>();
    std::atomic<bool> closed_{false};  // close() 幂等闩；在途判定统一走 bg_

    BackgroundTaskGroup bg_{"duostore"};
    // 只在 bg_.if_open 内写、begin_close 后不变（读侧免锁）；0 = 未 arm（cancel(0) 安全）
    TimerQueue::Id gc_timer_ = 0;
    // 手动钩子与后台 worker 互斥（跨 co_await 不可持 std::mutex，用协程信号量）
    AsyncSemaphore gc_sem_{1};
};

}  // namespace lights3::storage
