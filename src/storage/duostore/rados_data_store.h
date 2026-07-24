// L3: IDataStore 的 Ceph/RADOS 实现（docs/duostore-rados-data.md）。
// 单一路径：切片缓冲 → rados 对象一次 write_full（无 pack、无压实、无 torn tail，
// §3.3）；C1 同步 librados 在池线程（§6.2），aio 桥接后置 C3。
// 编译期由 LIGHTS3_DUOSTORE_RADOS_DATA 裁剪（§9.2）。
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

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
};

class RadosChunkWriter;

class RadosDataStore final : public IDataStore {
public:
    using FileIdAlloc = std::function<uint64_t(Extent::Kind)>;

    // 构造即建连（fail fast，§6.1）；失败抛 std::runtime_error（配置/环境错误级）
    RadosDataStore(RadosDataOptions opt, std::shared_ptr<ThreadPool> pool, FileIdAlloc alloc);
    ~RadosDataStore() override;
    RadosDataStore(const RadosDataStore&) = delete;

    Task<std::unique_ptr<DataWriter>> open_writer(WriteHint hint) override;
    Task<std::unique_ptr<http::BodyReader>> open_reader(DataRef ref, uint64_t first,
                                                        uint64_t last) override;
    Task<void> remove(std::span<const Extent> extents) override;
    Task<GcRewrite> rewrite_pack(uint64_t pack_id) override;  // 恒 {}（无 pack，§3.3）
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
    ThreadPoolExecutor exec_;    // 信号量等待者经池唤醒，避免在释放方栈上内联跑协程链
    AsyncSemaphore buffer_sem_;  // 许可数 = buffer_total / chunk_size（§4.2）
};

}  // namespace lights3::storage::duostore
