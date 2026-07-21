#include "storage/duostore/fs_data_store.h"

#include <fcntl.h>
#include <unistd.h>

#include <algorithm>
#include <bitset>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <vector>

#include "core/log.h"
#include "storage/duostore/codec.h"
#include "storage/localfs/fs_util.h"

namespace lights3::storage::duostore {

using s3::S3Error;
using s3::S3ErrorCode;
using fsutil::throw_errno;

namespace {

// ss = (file_id >> 8) 的低 8 位：id 连续分配（号段，§4.5），每 256 个连续 id 同
// 目录——一次写会话的 shard 目录 fsync 收敛到 1-2 次而非每 chunk 一个目录（§5.1）
unsigned shard_of(uint64_t id) { return unsigned((id >> 8) & 0xff); }

std::filesystem::path shard_file(const std::filesystem::path& base, uint64_t id,
                                 const char* suffix) {
    char ss[3], name[32];
    std::snprintf(ss, sizeof ss, "%02x", shard_of(id));
    std::snprintf(name, sizeof name, "%016llx%s", (unsigned long long)id, suffix);
    return base / ss / name;
}

void write_full(int fd, const std::byte* p, size_t n) {
    while (n > 0) {
        ssize_t w = ::write(fd, p, n);
        if (w < 0) throw_errno("write chunk");
        p += w;
        n -= size_t(w);
    }
}

}  // namespace

// ---------- ChunkWriter：定长切片流式写（§5.1/§6.1）----------
// 契约：write/finish 的阻塞 IO 在调用方所处线程直接执行——DuoStoreBackend 已在
// 入口统一切池线程（与 localfs 的 PUT 泵送循环同一模式）。

class ChunkWriter final : public DataWriter {
public:
    explicit ChunkWriter(FsDataStore* store) : store_(store) {}

    ~ChunkWriter() override {
        // 未 finish 即析构 = 丢弃：尽力删除已产出文件；残留落入孤儿扫描（§9.3）
        if (finished_) return;
        if (fd_ >= 0) {
            ::close(fd_);
            ::unlink(store_->chunk_path(cur_id_).c_str());
        }
        for (const auto& e : extents_) ::unlink(store_->chunk_path(e.file_id).c_str());
    }

    Task<void> write(std::span<const std::byte> buf) override {
        while (!buf.empty()) {
            if (fd_ < 0) open_next_chunk();
            size_t n = std::min<uint64_t>(store_->opt_.chunk_size - cur_len_, buf.size());
            write_full(fd_, buf.data(), n);
            cur_crc_ = codec::crc32c_update(cur_crc_, buf.first(n));
            cur_len_ += n;
            buf = buf.subspan(n);
            if (cur_len_ == store_->opt_.chunk_size) seal_chunk();
        }
        co_return;
    }

    Task<DataRef> finish() override {
        if (fd_ >= 0) seal_chunk();
        // 会话结束对涉及的 shard 目录 fsync（§5.1）
        for (unsigned s = 0; s < 256; ++s)
            if (touched_[s] && ::fsync(store_->shard_dirfd(s)) != 0)
                throw_errno("fsync chunk shard dir");
        finished_ = true;
        co_return DataRef{std::move(extents_)};
    }

private:
    void open_next_chunk() {
        cur_id_ = store_->alloc_(Extent::Kind::kChunk);
        unsigned shard = shard_of(cur_id_);
        store_->shard_dirfd(shard);  // 确保 shard 目录存在
        auto path = store_->chunk_path(cur_id_);
        fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
        if (fd_ < 0) throw_errno("open chunk");
        touched_[shard] = true;
        cur_len_ = 0;
        cur_crc_ = 0;
    }

    void seal_chunk() {
        if (::fdatasync(fd_) != 0) throw_errno("fdatasync chunk");
        ::close(fd_);
        fd_ = -1;
        extents_.push_back({Extent::Kind::kChunk, cur_id_, 0, cur_len_, cur_crc_});
    }

    FsDataStore* store_;
    std::vector<Extent> extents_;
    std::bitset<256> touched_;
    int fd_ = -1;
    uint64_t cur_id_ = 0;
    uint64_t cur_len_ = 0;
    uint32_t cur_crc_ = 0;
    bool finished_ = false;
};

// ---------- ExtentChainReader：多文件链流式读（§7）----------
// 懒打开当前 extent 的 fd（每块经池线程执行，与 localfs FdStreamReader 同模式；
// 不复用它——那是单 fd 所有权语义，这里是多文件链）。
// 自包含：持 FsDataOptions 拷贝而非 FsDataStore 指针——ObjectStream 会随 HTTP
// 响应逃逸出 backend 生命周期（驱动在 handler 返回后继续泵送），reader 不得
// 依赖 backend 存活（对齐 FdStreamReader 的自包含语义）。

namespace {

class ExtentChainReader final : public http::BodyReader {
public:
    ExtentChainReader(FsDataOptions opt, std::vector<Extent> extents, uint64_t first,
                      uint64_t last, std::shared_ptr<ThreadPool> pool)
        : opt_(std::move(opt)), extents_(std::move(extents)), remaining_(last - first + 1),
          total_(remaining_), pool_(std::move(pool)) {
        uint64_t off = first;
        while (idx_ < extents_.size() && off >= extents_[idx_].length) {
            off -= extents_[idx_].length;
            ++idx_;
        }
        cur_off_ = off;
    }

    ~ExtentChainReader() override {
        if (fd_ >= 0) ::close(fd_);
    }

    std::optional<uint64_t> length() const override { return total_; }

    Task<size_t> read(std::span<std::byte> buf) override {
        if (remaining_ == 0 || buf.empty()) co_return 0;
        co_await pool_->schedule();
        const Extent& e = extents_[idx_];
        if (fd_ < 0) {
            auto base = e.kind == Extent::Kind::kChunk ? opt_.root / "chunks"
                                                       : opt_.root / "packs";
            auto path = shard_file(base, e.file_id,
                                   e.kind == Extent::Kind::kChunk ? ".chk" : ".pak");
            fd_ = ::open(path.c_str(), O_RDONLY);
            if (fd_ < 0) {
                // refs 在而文件缺 = 数据丢失征兆（§10）；GC 并发窗口由 pin+grace 防护（P3）
                LOG_ERROR("duostore: open extent {} failed: {}", path.string(),
                          std::strerror(errno));
                throw S3Error(S3ErrorCode::InternalError, "duostore: extent file missing");
            }
            // crc 校验只在"从段首完整读到段尾"时可行（Range 命中中段无从校验，§7）
            crc_active_ = opt_.verify_chunk_crc && e.kind == Extent::Kind::kChunk &&
                          cur_off_ == 0 && remaining_ >= e.length;
            crc_acc_ = 0;
        }
        size_t want = size_t(std::min<uint64_t>({buf.size(), e.length - cur_off_, remaining_}));
        ssize_t n = ::pread(fd_, buf.data(), want, off_t(e.offset + cur_off_));
        if (n < 0) throw_errno("pread extent");
        if (n == 0)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore: extent shorter than manifest");
        if (crc_active_) crc_acc_ = codec::crc32c_update(crc_acc_, buf.first(size_t(n)));
        cur_off_ += uint64_t(n);
        remaining_ -= uint64_t(n);
        if (cur_off_ == e.length) {
            if (crc_active_ && crc_acc_ != e.crc32c) {
                LOG_ERROR("duostore: chunk {:016x} crc mismatch (stored {:08x} got {:08x})",
                          e.file_id, e.crc32c, crc_acc_);
                throw S3Error(S3ErrorCode::InternalError, "duostore: chunk crc mismatch");
            }
            ::close(fd_);
            fd_ = -1;
            ++idx_;
            cur_off_ = 0;
        }
        co_return size_t(n);
    }

private:
    FsDataOptions opt_;
    std::vector<Extent> extents_;
    size_t idx_ = 0;
    uint64_t cur_off_ = 0;
    uint64_t remaining_;
    uint64_t total_;
    int fd_ = -1;
    bool crc_active_ = false;
    uint32_t crc_acc_ = 0;
    std::shared_ptr<ThreadPool> pool_;
};

}  // namespace

// ---------- FsDataStore ----------

FsDataStore::FsDataStore(FsDataOptions opt, std::shared_ptr<ThreadPool> pool, FileIdAlloc alloc)
    : opt_(std::move(opt)), pool_(std::move(pool)), alloc_(std::move(alloc)) {
    dirfds_.fill(-1);
    std::filesystem::create_directories(opt_.root / "chunks");
}

FsDataStore::~FsDataStore() {
    for (int& fd : dirfds_)
        if (fd >= 0) {
            ::close(fd);
            fd = -1;
        }
}

std::filesystem::path FsDataStore::chunk_path(uint64_t file_id) const {
    return shard_file(opt_.root / "chunks", file_id, ".chk");
}

std::filesystem::path FsDataStore::pack_path(uint64_t pack_id) const {
    return shard_file(opt_.root / "packs", pack_id, ".pak");
}

int FsDataStore::shard_dirfd(unsigned shard) {
    std::lock_guard lk(dir_mu_);
    if (dirfds_[shard] >= 0) return dirfds_[shard];
    char ss[3];
    std::snprintf(ss, sizeof ss, "%02x", shard);
    auto dir = opt_.root / "chunks" / ss;
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if (ec) throw S3Error(S3ErrorCode::InternalError, "create shard dir: " + ec.message());
    int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) throw_errno("open shard dir");
    dirfds_[shard] = fd;
    return fd;
}

Task<std::unique_ptr<DataWriter>> FsDataStore::open_writer(WriteHint hint) {
    (void)hint;  // P2：content_length 与 pack_threshold 决定 pack/chunk 路径（§5.3）
    co_return std::make_unique<ChunkWriter>(this);
}

Task<std::unique_ptr<http::BodyReader>> FsDataStore::open_reader(DataRef ref, uint64_t first,
                                                                 uint64_t last) {
    uint64_t total = ref.total();
    if (first > last || last >= total)
        throw S3Error(S3ErrorCode::InternalError,
                      "duostore: reader range beyond manifest");  // 调用方已 resolve_range
    co_return std::make_unique<ExtentChainReader>(opt_, std::move(ref.extents), first, last,
                                                  pool_);
}

Task<void> FsDataStore::remove(std::span<const Extent> extents) {
    co_await pool_->schedule();
    for (const auto& e : extents) {
        if (e.kind != Extent::Kind::kChunk) continue;  // pack record 为死区，随压实回收（§9.1）
        if (::unlink(chunk_path(e.file_id).c_str()) != 0 && errno != ENOENT)
            throw_errno("unlink chunk");  // 幂等：ENOENT 忽略
    }
    co_return;
}

Task<GcRewrite> FsDataStore::rewrite_pack(uint64_t pack_id) {
    (void)pack_id;
    throw S3Error(S3ErrorCode::InternalError,
                  "duostore: pack compaction not implemented (P4)");
    co_return GcRewrite{};  // unreachable
}

Task<void> FsDataStore::close() {
    // P2：封存 active pack。chunk 路径无会话外状态，dirfd 由析构关闭
    co_return;
}

}  // namespace lights3::storage::duostore
