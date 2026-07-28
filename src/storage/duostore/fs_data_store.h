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
};

class ChunkWriter;
class FsPackedWriter;

// 写侧 pin 钩子（§9.3）：孤儿扫描不得回收"写入中尚未提交 meta"的 chunk——慢流式
// PUT 的早期 chunk mtime 可远逾 gc_grace，仅靠 mtime 宽限不充分。ChunkWriter 在
// 分配 file_id 时 pin、未 finish 即析构时解 pin；finish 之后的解 pin 责任移交
// 调用方（DuoStoreBackend 在 meta 提交 / 兜底删除之后解除）
struct ChunkPinHooks {
    std::function<void(uint64_t)> pin;
    std::function<void(uint64_t)> unpin;
};

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
    Task<std::unique_ptr<http::BodyReader>> open_reader(DataRef ref, uint64_t first,
                                                        uint64_t last) override;
    Task<void> remove(std::span<const Extent> extents) override;
    Task<void> remove_pack(uint64_t pack_id) override;
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
    void seal_slot_locked(ActivePack& slot);  // 持 slot.m 调用；关 fd + 回报封存

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
};

}  // namespace lights3::storage::duostore
