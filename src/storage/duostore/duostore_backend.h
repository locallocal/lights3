// L3: DuoStore 后端门面（docs/duostore-backend.md）：S3 语义、ETag/MD5、泵送循环；
// 元数据/数据分离到 IMetaStore / IDataStore 两个可插拔接口，DataRef 为唯一耦合点。
// P1：RocksDB meta + chunk 数据路径；P2：pack 聚合（阈值分流 + 存活账 + 重启弃用）；
// P3：GC 一期（gcq 消费 + pin 计数 + mpu_ttl + 后台 worker）；压实/孤儿扫描（P4）
// 后续引入。
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
#include "core/metrics.h"
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
    // 单 id 变体（写侧 pin，§9.3）：ChunkWriter 分配即 pin，meta 提交/丢弃后解除
    void pin_id(uint64_t file_id);
    void unpin_id(uint64_t file_id);
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
    uint64_t packs_removed = 0;     // 整文件删除的空 pack 数（sealed 且 live_recs==0）
    uint64_t uploads_expired = 0;   // mpu_ttl 过期而内部 abort 的 multipart 数
    uint64_t packs_compacted = 0;   // 本轮顺扫（rewrite_pack）过的低存活 pack 数（P4 §9.2）
    uint64_t records_migrated = 0;  // 压实迁移成功换 ref 的 record 数
    uint64_t records_corrupt = 0;   // 压实顺扫检出的损坏 record 数（跳过 + 告警，不删）
};

// run_orphan_scan_once() 的对账统计（§9.3）
struct DuoOrphanStats {
    uint64_t chunks_scanned = 0;   // 数据面枚举到的 chunk 实体数
    uint64_t orphans_removed = 0;  // 无引用且逾宽限 → 已 unlink 的孤儿数
    uint64_t skipped_grace = 0;    // 无引用但 mtime 未逾 gc_grace（在途写入嫌疑）
    uint64_t skipped_pinned = 0;   // 无引用但有 pin（写侧 pin / 在途读者）
    uint64_t refs_missing = 0;     // 反向：refs 在而文件缺（数据丢失征兆，只告警不删 meta）
};

// PackMigrateFn 的标准实现（§9.2 步骤 2-3；cfg 构造与测试注入组装共用）：owner
// 反查存活（对象 "b\0k" 直查；mpu "mpu\0b\0k\0id\0no" 借 b/k 提示查 complete 后的
// 归属对象——owner 只是提示，存活判据恒为"当前 DataRef 含 from"+ swap 的 version
// 守卫，提示失效只保守不迁、绝不误删）→ payload 经 data.open_writer 追加 →
// swap_extents 乐观换 ref。竞态覆盖使 swap 失败时清理已追加的 chunk 类残留（pack
// 类为死区随压实回收）。pins 可空；须与 data 的 ChunkPinHooks 同源（追加走 chunk
// 路径时对称解 pin）
Task<bool> migrate_pack_record(IMetaStore& meta, IDataStore& data, PinTable* pins,
                               std::string_view owner, const Extent& from,
                               std::span<const std::byte> payload);

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
    uint64_t pack_threshold = 128 << 10;   // ≤ 此值进 pack；0 = 关闭（全走 chunk）
    uint64_t pack_max_size = 128ull << 20;
    int pack_writers = 4;
    double pack_gc_ratio = 0.5;            // P4 压实生效
    int gc_interval_sec = 300;
    int gc_grace_sec = 300;
    int orphan_scan_interval_sec = 86400;  // P4 生效
    int mpu_ttl_sec = 7 * 86400;
    bool meta_sync = true;
    bool verify_chunk_crc = false;
    size_t rocksdb_block_cache = 64ull << 20;
    // RocksDB 调参外露（P5，§11）；默认 = RocksDB 自身默认，行为不变
    size_t rocksdb_write_buffer = 64ull << 20;
    int rocksdb_max_write_buffers = 2;
    int rocksdb_max_background_jobs = 2;

    // 集中解析 + 范围校验（docs/duostore-backend.md §11）；配置错误抛 std::runtime_error
    static DuoStoreConfig from_params(const std::string& name,
                                      const std::map<std::string, std::string>& params);
};

class DuoStoreBackend final : public IStorageBackend {
public:
    // metrics 默认空 scope（docs/todo.md §3.1）：测试直构免装配，计数落孤立实例
    DuoStoreBackend(DuoStoreConfig cfg, std::shared_ptr<ThreadPool> pool,
                    MetricsScope metrics = {});
    // 测试注入用：自组装 meta/data
    DuoStoreBackend(DuoStoreConfig cfg, std::shared_ptr<ThreadPool> pool,
                    std::unique_ptr<duostore::IMetaStore> meta,
                    std::unique_ptr<duostore::IDataStore> data, MetricsScope metrics = {});
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

    // GC 手动钩子（§9：测试直调，仿 tiered "手动触发下沉"先例）：mpu_ttl 过期清理
    // + gcq 消费（grace/pin 过滤，先物理删后销账）+ pack 压实（P4 §9.2）+ 空 pack
    // 整删（空置逾 gc_grace 才 unlink，服务在途读者）。与后台 worker / 孤儿扫描经
    // 内部信号量互斥，可并发调用
    Task<duostore::DuoGcStats> run_gc_once();
    // 孤儿扫描手动钩子（P4 §9.3）：正向（盘上 chunk 无引用、逾 gc_grace、无 pin →
    // unlink）+ 反向（refs 在而文件缺 → 告警计数，绝不删 meta）。与 GC 同信号量
    // 互斥——反向对账"文件必先于 ref 存在"的论证依赖 gcq 的 unlink→销账窗口不并发
    Task<duostore::DuoOrphanStats> run_orphan_scan_once();

private:
    void require_bucket(std::string_view bucket);  // 池线程调用
    // 取对象记录；缺失时区分 NoSuchBucket / NoSuchKey（GET/HEAD 错误语义必须一致）
    duostore::ObjectRec require_object(std::string_view bucket, std::string_view key);
    // 重启弃用 active pack（§5.2）：把上代遗留的 unsealed pack 账补封（构造时调用）
    void abandon_stale_packs();

    // 后台 GC 管理（§9 生命周期）：BackgroundTaskGroup 等待组 + 完成后重臂的
    // 单 worker tick；close/dtor 共用 shutdown_background（begin_close → 锁外
    // cancel 定时器[TimerQueue::cancel 阻塞等在途回调] → 等在途 GC 清零）
    void schedule_gc();
    Task<void> gc_tick();
    void schedule_orphan_scan();  // 独立低频定时器（orphan_scan_interval；0 = 关）
    Task<void> orphan_tick();
    void shutdown_background();
    // GC 计数指标注册（P5 指标项的 GC 切片，docs/todo.md §1.4；两个构造共用）
    void init_metrics(const MetricsScope& metrics);

    DuoStoreConfig cfg_;
    std::shared_ptr<ThreadPool> pool_;
    std::unique_ptr<duostore::IMetaStore> meta_;
    std::unique_ptr<duostore::IDataStore> data_;
    std::shared_ptr<duostore::PinTable> pins_ = std::make_shared<duostore::PinTable>();
    // 写侧 pin 已装配（cfg 构造给 FsDataStore 注入 ChunkPinHooks 时置位）：put 路径
    // 提交/丢弃后须对称解 pin。注入构造默认无钩子——盲解会误减并发读者的 pin
    bool write_pins_ = false;
    std::atomic<bool> closed_{false};  // close() 幂等闩；在途判定统一走 bg_

    // GC 完成轮末尾一次性累计（run_gc_once 的 DuoGcStats → 单调计数器）
    std::shared_ptr<MetricCounter> m_gc_runs_, m_gc_reclaims_, m_gc_files_removed_,
        m_gc_packs_removed_, m_gc_uploads_expired_, m_gc_packs_compacted_,
        m_gc_records_migrated_, m_gc_records_corrupt_, m_orphan_runs_, m_orphan_removed_;
    // GET 读路径 crc 失配计数（P5 corruption 指标）：数据面 reader 经 on_corruption
    // 回调递增——回调只捕获此 shared_ptr，reader 逃逸出 backend 生命周期仍安全
    std::shared_ptr<MetricCounter> m_read_corruption_;
    std::shared_ptr<MetricGauge> m_orphan_refs_missing_;  // 最近一轮反向对账缺文件数

    // GC 轮内簿记（只在持 gc_sem_ 时读写，免锁）：
    // pack_empty_since_ = 空 pack 首见时刻（§9.2 步骤 4 延迟 unlink：空置逾 gc_grace
    // 才整删，服务压实/删除瞬间已持旧 ref 未及 pin 的读者；进程内读者进程内计时即
    // 正确，重启清零只是保守）。compact_blocked_ = 上轮压实未能全迁的 pack（进行中
    // mpu 分片 / 旧格式 owner / 存活损坏 record）：live 账无变化且未过冷却窗
    // （gc_grace）则跳过重扫——账有推进立即重试；冷却兜住"账不动但可归属性变了"
    // 的情形（complete_upload 的 refs 转移不动 pack 账）
    struct CompactBlocked {
        int64_t live_recs = 0;
        int64_t retry_at_ms = 0;
    };
    std::unordered_map<uint64_t, int64_t> pack_empty_since_;
    std::unordered_map<uint64_t, CompactBlocked> compact_blocked_;

    BackgroundTaskGroup bg_{"duostore"};
    // 只在 bg_.if_open 内写、begin_close 后不变（读侧免锁）；0 = 未 arm（cancel(0) 安全）
    TimerQueue::Id gc_timer_ = 0;
    TimerQueue::Id orphan_timer_ = 0;
    // 手动钩子、后台 worker 与孤儿扫描互斥（跨 co_await 不可持 std::mutex，用协程信号量）
    AsyncSemaphore gc_sem_{1};
};

}  // namespace lights3::storage
