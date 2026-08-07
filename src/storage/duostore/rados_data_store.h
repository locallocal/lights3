// L3: IDataStore 的 Ceph/RADOS 实现（docs/duostore-rados-data.md）。
// 单一路径：切片缓冲 → rados 对象一次 write_full（无 pack、无压实、无 torn tail，
// §3.3）。C3 起全 IO 走 rados_aio_*：completion 回调只把续体投递回本进程池
// executor（§6.2 纪律），写侧双缓冲流水（写第 N 片时接收 N+1，§4.2）；C4 起
// scan_chunks 孤儿枚举与 op 延迟/错误指标（§8.2/§10）。
// 编译期由 LIGHTS3_DUOSTORE_RADOS_DATA 裁剪（§9.2）。
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "core/metrics.h"
#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "storage/duostore/data_store.h"

namespace lights3::storage::duostore {

struct RadosDataOptions {
    std::string conf_path = "/etc/ceph/ceph.conf";  // mon 地址与 keyring 引用
    std::string client_name = "client.admin";       // cephx 身份
    std::string pool;                               // 必填；副本/EC 策略在 pool 级（§3.2）
    std::string ns;                                 // rados namespace（空 = 默认）
    uint64_t chunk_size = 8ull << 20;               // 切片粒度 = 单对象上限（§3.4）
    uint64_t buffer_total = 256ull << 20;           // writer 缓冲总额度（§4.2）
    int connect_timeout_sec = 5;                    // client_mount_timeout
    int op_timeout_sec = 0;                         // 0 = 不设（§6.4）
    bool verify_chunk_crc = false;                  // 语义同 fs 版（§5）
    // 读路径 crc 失配上报（P5 corruption 指标；空 = 不上报）；生命周期约束同 fs 版
    std::function<void()> on_corruption;
    // op 延迟/错误指标（C4，§10）；空 scope 即孤立实例，测试直构免装配
    MetricsScope metrics;
    // 写侧 pin（docs/gaps.md §1.2）：分配 file_id 即 pin，未 finish 即析构时解。
    // 不接的话，大对象 PUT 的早期分片会在 meta 提交前被孤儿扫描当无引用文件删掉
    ChunkPinHooks pins;
};

class RadosChunkWriter;

class RadosDataStore final : public IDataStore {
public:
    // 返回连续 run [first, first+n) 的首 id（IMetaStore::alloc_file_run，
    // docs/gaps.md §3.9：id 连续 manifest 的 run 编码才有效）
    using FileIdAlloc = std::function<uint64_t(Extent::Kind, uint32_t)>;

    // 构造即建连（fail fast，§6.1）；失败抛 std::runtime_error（配置/环境错误级）
    RadosDataStore(RadosDataOptions opt, std::shared_ptr<ThreadPool> pool, FileIdAlloc alloc);
    // 兜底（正路先 close()）：在途写清尾后释放本方 Conn 引用——弃单 writer 的
    // completion 回调引用 exec_/buffer_sem_，必须先 rados_aio_flush 等回调收尾
    ~RadosDataStore() override;
    RadosDataStore(const RadosDataStore&) = delete;

    Task<std::unique_ptr<DataWriter>> open_writer(WriteHint hint) override;
    Task<std::unique_ptr<http::BodyReader>> open_reader(DataRef ref, uint64_t first,
                                                        uint64_t last) override;
    Task<void> remove(std::span<const Extent> extents) override;
    Task<void> remove_pack(uint64_t pack_id) override;        // no-op（无 pack，§3.3）
    Task<GcRewrite> rewrite_pack(uint64_t pack_id) override;  // 恒 {}（无 pack，§3.3）
    // 孤儿扫描枚举（C4，§8.2）：rados_nobjects_list_*（ioctx 已限 namespace）+
    // rados_stat；非本店命名（非 c.<016x>）的外来对象忽略
    Task<void> scan_chunks(
        const std::function<void(uint64_t file_id, int64_t mtime_ms)>& cb) override;
    Task<void> close() override;

    // 对象命名：c.<file_id:016x>（§3.1）；测试观察用
    static std::string object_name(uint64_t file_id);

    // 连接状态引用计数共享（实现细节，公开仅为 .cc 内 reader/守卫函数可名指）：
    // reader 随 HTTP 响应逃逸出 backend 生命周期（对齐 ExtentChainReader 的自包含
    // 语义），ioctx/cluster 由最后一个持有者释放；close() 置 closed 后新 op 干净地
    // 抛 500（§6.5 守卫）
    struct Conn {
        void* cluster = nullptr;  // rados_t
        void* ioctx = nullptr;    // rados_ioctx_t
        std::atomic<bool> closed{false};
        void shutdown();  // 幂等：ioctx_destroy + rados_shutdown
        ~Conn() { shutdown(); }
    };

private:
    friend class RadosChunkWriter;

    std::shared_ptr<Conn> conn_;
    RadosDataOptions opt_;
    std::shared_ptr<ThreadPool> pool_;
    FileIdAlloc alloc_;
    ThreadPoolExecutor exec_;    // 信号量唤醒与 aio completion 续体统一经池投递（§6.2）
    AsyncSemaphore buffer_sem_;  // 许可数 = buffer_total / chunk_size（§4.2）

    // op 延迟直方图（提交 → completion 回调，含集群往返）与错误计数（C4，§10）；
    // 构造期注册 0 值可见。shared_ptr 供 reader/在途单逃逸后仍安全递增
    std::shared_ptr<MetricHistogram> m_lat_write_, m_lat_read_, m_lat_remove_;
    std::shared_ptr<MetricCounter> m_err_write_, m_err_read_, m_err_remove_, m_err_scan_;
};

}  // namespace lights3::storage::duostore
