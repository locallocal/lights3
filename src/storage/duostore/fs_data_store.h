// L3: IDataStore 的本地文件系统实现（docs/duostore-backend.md §5）。
// P1 仅 chunk 路径（定长切片 + shard 目录）；pack 聚合随 P2 引入。
#pragma once

#include <array>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <memory>
#include <mutex>

#include "core/thread_pool.h"
#include "storage/duostore/data_store.h"

namespace lights3::storage::duostore {

struct FsDataOptions {
    std::filesystem::path root;  // chunks/ packs/ 在其下（§5）
    uint64_t chunk_size = 8ull << 20;
    // GET 链路 chunk crc 校验（默认关，§7）：只对"从段首完整读到段尾"的 chunk
    // 生效——Range 命中中段的部分读无从校验，完整性主责在 GC/对账路径
    bool verify_chunk_crc = false;
};

class ChunkWriter;

class FsDataStore final : public IDataStore {
public:
    // file_id 分配回调（持久单调，由 IMetaStore::alloc_file_id 提供）
    using FileIdAlloc = std::function<uint64_t(Extent::Kind)>;

    FsDataStore(FsDataOptions opt, std::shared_ptr<ThreadPool> pool, FileIdAlloc alloc);
    ~FsDataStore() override;
    FsDataStore(const FsDataStore&) = delete;

    Task<std::unique_ptr<DataWriter>> open_writer(WriteHint hint) override;
    Task<std::unique_ptr<http::BodyReader>> open_reader(DataRef ref, uint64_t first,
                                                        uint64_t last) override;
    Task<void> remove(std::span<const Extent> extents) override;
    Task<GcRewrite> rewrite_pack(uint64_t pack_id) override;
    Task<void> close() override;

    // 布局路径（§5）；测试观察用
    std::filesystem::path chunk_path(uint64_t file_id) const;
    std::filesystem::path pack_path(uint64_t pack_id) const;

private:
    friend class ChunkWriter;
    // 懒建 shard 目录 + dirfd 常驻缓存（写会话结束 fsync 目录用，§5.1）
    int shard_dirfd(unsigned shard);

    FsDataOptions opt_;
    std::shared_ptr<ThreadPool> pool_;
    FileIdAlloc alloc_;
    std::mutex dir_mu_;
    std::array<int, 256> dirfds_;
};

}  // namespace lights3::storage::duostore
