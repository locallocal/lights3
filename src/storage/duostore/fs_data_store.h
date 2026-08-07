// L3: IDataStore 的本地文件系统实现（docs/duostore-backend.md §5）。
// chunk 路径（定长切片 + shard 目录，P1）+ pack 聚合（append-only record，P2）。
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>
#include <vector>

#include "core/thread_pool.h"
#include "storage/duostore/data_store.h"

namespace lights3::storage::duostore {

struct FsDataOptions {
    std::filesystem::path root;  // chunks/ packs/ 在其下（§5）
    uint64_t chunk_size = 8ull << 20;
    // GET 链路 chunk crc 校验（默认关，§7）：只对"从段首完整读到段尾"的 chunk
    // 生效——Range 命中中段的部分读无从校验，完整性主责在 GC/对账路径。
    // pack record 恒校验 crc（整段读入，与本开关无关）
    bool verify_chunk_crc = false;
    // pack 聚合（§5.2）：对象/分片 ≤ pack_threshold 进 pack；0 = 关闭（全走 chunk）。
    // 默认 0 保持 FsDataStore 单独构造（测试）时的 P1 行为，DuoStoreBackend 从
    // 配置注入真实默认（128KiB）
    uint64_t pack_threshold = 0;
    uint64_t pack_max_size = 128ull << 20;  // active pack 封存阈值
    int pack_writers = 4;                   // 并存 active pack 数
    // 读路径 crc 失配上报（P5 corruption 指标；空 = 不上报）。reader 持本 options
    // 拷贝逃逸出 store 生命周期——回调不得引用 store/backend（装配侧只捕获计数器）
    std::function<void()> on_corruption;
};

class ChunkWriter;
class FsPackedWriter;

class FsDataStore final : public IDataStore {
public:
    // file_id 分配回调（持久单调，由 IMetaStore::alloc_file_id 提供）
    using FileIdAlloc = std::function<uint64_t(Extent::Kind)>;
    // pack 封存回调（IMetaStore::seal_pack）：轮转与 close 时回报最终文件大小。
    // 崩溃（未回调）遗留的 unsealed pack 由 DuoStoreBackend 启动时补封（§5.2）
    using PackSeal = std::function<void(uint64_t pack_id, uint64_t file_size)>;

    // migrate（§9.2）：压实迁移回调，空 = rewrite_pack 只扫不迁（统计仍产出）
    FsDataStore(FsDataOptions opt, std::shared_ptr<ThreadPool> pool, FileIdAlloc alloc,
                PackSeal seal = {}, PackMigrateFn migrate = {}, ChunkPinHooks pins = {});
    ~FsDataStore() override;
    FsDataStore(const FsDataStore&) = delete;

    Task<std::unique_ptr<DataWriter>> open_writer(WriteHint hint) override;
    // 批量写覆写（压实迁移，docs/gaps.md §2.13）：pack 适格项一次槽锁 + 一次
    // fdatasync 批量追加；超阈值/pack 关闭的项退回逐条 open_writer 路径
    Task<std::vector<DataRef>> write_batch(std::span<const PackAppendItem> items) override;
    Task<std::unique_ptr<http::BodyReader>> open_reader(DataRef ref, uint64_t first,
                                                        uint64_t last) override;
    Task<void> remove(std::span<const Extent> extents) override;
    Task<void> remove_pack(uint64_t pack_id) override;
    bool pack_write_locked(uint64_t pack_id) override;
    uint64_t stat_pack(uint64_t pack_id) override;
    Task<GcRewrite> rewrite_pack(uint64_t pack_id) override;
    Task<void> scan_chunks(
        const std::function<void(uint64_t file_id, int64_t mtime_ms)>& cb) override;
    Task<void> close() override;

    // 布局路径（§5）；测试观察用
    std::filesystem::path chunk_path(uint64_t file_id) const;
    std::filesystem::path pack_path(uint64_t pack_id) const;

private:
    friend class ChunkWriter;
    friend class FsPackedWriter;

    // active pack 槽（§5.2）：各带互斥与追加偏移，writer 轮询取锁追加。锁模型
    // 成立的前提：payload ≤ pack_threshold，临界区是一次小 pwrite + fdatasync
    struct ActivePack {
        std::mutex m;
        int fd = -1;
        uint64_t id = 0;
        uint64_t size = 0;  // 当前追加偏移 = 文件大小
    };

    // 懒建 shard 目录 + dirfd 常驻缓存（写会话结束 fsync 目录用，§5.1）
    int shard_dirfd(unsigned shard) { return subdir_fd(chunk_dirfds_, "chunks", shard); }
    int pack_dirfd(unsigned shard) { return subdir_fd(pack_dirfds_, "packs", shard); }
    int subdir_fd(std::array<int, 256>& fds, const char* sub, unsigned shard);

    // 追加一条 pack record（阻塞 IO，须在池线程调用）；返回指向 payload 的 extent
    Extent append_pack_record(std::string_view owner, std::span<const std::byte> payload);
    // 批量追加（write_batch 的 pack 路径）：单次槽锁内逐条 pwrite、末尾一次
    // fdatasync（§2.13 批量化——中途轮转封存前会先把未同步的写落盘）
    std::vector<Extent> append_pack_records(std::span<const PackAppendItem> items);

    // 封存拆两步（docs/gaps.md §3.9）：锁内只关 fd/清槽状态并把 (id,size) 入
    // seal_retry_，seal_ 的 meta 提交（可能是网络 RTT/fsync）在 flush_seals 里
    // 锁外执行。此前 seal_ 在 slot 互斥内跑会堵死该槽；抛出时 fd 已置 -1 而
    // size 未清，下一次写会开新 pack 覆盖槽状态，旧 pack 从此永远不被封存
    struct PendingSeal {
        uint64_t id = 0;
        uint64_t size = 0;
    };
    void close_slot_locked(ActivePack& slot);  // 持 slot.m 调用
    // 提交积压的封存；失败放回队列（append 路径告警后续重试，close 路径上抛）
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
    std::vector<std::unique_ptr<ActivePack>> packs_;  // pack_writers 个槽
    std::atomic<unsigned> pack_rr_{0};                // 轮询游标
    std::mutex seal_mu_;
    std::vector<PendingSeal> seal_retry_;  // 已关 fd、meta 尚未确认封存的 pack
};

}  // namespace lights3::storage::duostore
