#include "storage/duostore/fs_data_store.h"

#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <bitset>
#include <cerrno>
#include <charconv>
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

// ---- pack record 头（§5.2）----
// record := header || payload
// header := magic "LP3R" | u8 ver=1 | u8 flags | u16 header_len | u64 payload_len
//         | u32 crc32c(payload) | u16 owner_len | owner
// 多字节整数小端（与 codec value 编码一致）；extent.offset 指向 payload——热路径
// 读不解析头，头信息（owner/crc 冗余）服务 P4 压实顺扫与离线打捞
constexpr char kPackMagic[4] = {'L', 'P', '3', 'R'};
constexpr size_t kPackHeaderFixed = 4 + 1 + 1 + 2 + 8 + 4 + 2;

void put_le(std::string& s, uint64_t v, size_t n) {
    for (size_t i = 0; i < n; ++i) s.push_back(char(v >> (8 * i)));
}

std::string build_pack_header(std::string_view owner, uint64_t payload_len, uint32_t crc) {
    size_t header_len = kPackHeaderFixed + owner.size();
    if (owner.size() > 0xffff || header_len > 0xffff)
        throw S3Error(S3ErrorCode::InternalError, "duostore: pack owner too long");
    std::string h;
    h.reserve(header_len);
    h.append(kPackMagic, sizeof kPackMagic);
    h.push_back(1);  // ver
    h.push_back(0);  // flags
    put_le(h, header_len, 2);
    put_le(h, payload_len, 8);
    put_le(h, crc, 4);
    put_le(h, owner.size(), 2);
    h.append(owner);
    return h;
}

}  // namespace

// ---------- ChunkWriter：定长切片流式写（§5.1/§6.1）----------
// 契约：write/finish 的阻塞 IO 在调用方所处线程直接执行——DuoStoreBackend 已在
// 入口统一切池线程（与 localfs 的 PUT 泵送循环同一模式）。

class ChunkWriter final : public DataWriter {
public:
    explicit ChunkWriter(FsDataStore* store) : store_(store) {}

    ~ChunkWriter() override {
        // 未 finish 即析构 = 丢弃：尽力删除已产出文件；残留落入孤儿扫描（§9.3）。
        // 写侧 pin 同步解除——finish 成功后 pin 所有权已移交调用方（ChunkPinHooks）
        if (finished_) return;
        if (fd_ >= 0) {
            ::close(fd_);
            ::unlink(store_->chunk_path(cur_id_).c_str());
        }
        for (const auto& e : extents_) ::unlink(store_->chunk_path(e.file_id).c_str());
        if (store_->pins_.unpin)
            for (uint64_t id : pinned_) store_->pins_.unpin(id);
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
        // 几何增长批取连续 run（docs/gaps.md §3.9）：并发写者逐个派发会让同对象
        // 的 chunk id 交错，manifest 的 run 编码失效反而膨胀。首块取 1（小对象
        // 零浪费），之后 2、4、…封顶 kMaxIdRun；run 尾部弃置无害（id 只需唯一）
        if (run_next_ == run_limit_) {
            run_len_ = run_len_ == 0 ? 1 : std::min<uint32_t>(run_len_ * 2, kMaxIdRun);
            run_next_ = store_->alloc_(Extent::Kind::kChunk, run_len_);
            run_limit_ = run_next_ + run_len_;
        }
        cur_id_ = run_next_++;
        // 先 pin 后建文件：文件一旦存在即受写侧 pin 保护，孤儿扫描无观察窗口
        if (store_->pins_.pin) {
            store_->pins_.pin(cur_id_);
            pinned_.push_back(cur_id_);
        }
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
    std::vector<uint64_t> pinned_;  // 本会话写侧 pin 的 id（finish 后所有权移交调用方）
    std::bitset<256> touched_;
    int fd_ = -1;
    uint64_t cur_id_ = 0;
    uint64_t cur_len_ = 0;
    uint32_t cur_crc_ = 0;
    uint64_t run_next_ = 0, run_limit_ = 0;  // 本会话的连续 id run（§3.9 批取）
    uint32_t run_len_ = 0;
    bool finished_ = false;
};

// ---------- FsPackedWriter：pack/chunk 分流写（§5.2/§5.3）----------
// ≤ pack_threshold 的对象先内存缓冲，EOF 时整体追加进 pack；一旦超过阈值（只可能
// 发生在长度未知的 chunked PUT）即转为 chunk 流式路径、缓冲落盘。内存上界 =
// pack_threshold × max_inflight_requests（两配置联动，§5.3）。
// 未 finish 即析构：缓冲直接丢弃（盘上无痕）；已 spill 则由 ChunkWriter 兜底。

class FsPackedWriter final : public DataWriter {
public:
    FsPackedWriter(FsDataStore* store, std::string owner)
        : store_(store), owner_(std::move(owner)) {}

    Task<void> write(std::span<const std::byte> buf) override {
        if (spill_) co_return co_await spill_->write(buf);
        if (buf_.size() + buf.size() <= store_->opt_.pack_threshold) {
            buf_.insert(buf_.end(), buf.begin(), buf.end());
            co_return;
        }
        // 超过阈值（chunked PUT 的未知长度流）：缓冲经 chunk 路径落盘后转流式
        spill_ = std::make_unique<ChunkWriter>(store_);
        co_await spill_->write(std::span<const std::byte>(buf_.data(), buf_.size()));
        buf_.clear();
        buf_.shrink_to_fit();
        co_await spill_->write(buf);
    }

    Task<DataRef> finish() override {
        if (spill_) co_return co_await spill_->finish();
        if (buf_.empty()) co_return DataRef{};  // 0 字节对象：空 DataRef
        Extent e = store_->append_pack_record(
            owner_, std::span<const std::byte>(buf_.data(), buf_.size()));
        co_return DataRef{{e}};
    }

private:
    FsDataStore* store_;
    std::string owner_;
    std::vector<std::byte> buf_;
    std::unique_ptr<ChunkWriter> spill_;
};

// ---------- ExtentChainReader：多文件链流式读（§7）----------
// 懒打开当前 extent 的 fd（每块经池线程执行，与 localfs FdStreamReader 同模式；
// 不复用它——那是单 fd 所有权语义，这里是多文件链）。
// 自包含：持 FsDataOptions 拷贝而非 FsDataStore 指针——ObjectStream 会随 HTTP
// 响应逃逸出 backend 生命周期（驱动在 handler 返回后继续泵送），reader 不得
// 依赖 backend 存活（对齐 FdStreamReader 的自包含语义）。
// pack extent：整段 payload 一次读入、恒校验 crc32c，再按 range 切片吐出（§7）。

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
        if (e.kind == Extent::Kind::kPack) co_return read_pack(e, buf);
        if (fd_ < 0) {
            fd_ = open_extent(e);
            // crc 校验只在"从段首完整读到段尾"时可行（Range 命中中段无从校验，§7）
            crc_active_ = opt_.verify_chunk_crc && cur_off_ == 0 && remaining_ >= e.length;
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
                if (opt_.on_corruption) opt_.on_corruption();
                throw S3Error(S3ErrorCode::InternalError, "duostore: chunk crc mismatch");
            }
            advance_extent();
        }
        co_return size_t(n);
    }

private:
    int open_extent(const Extent& e) {
        auto base = e.kind == Extent::Kind::kChunk ? opt_.root / "chunks" : opt_.root / "packs";
        auto path =
            shard_file(base, e.file_id, e.kind == Extent::Kind::kChunk ? ".chk" : ".pak");
        int fd = ::open(path.c_str(), O_RDONLY);
        if (fd < 0) {
            // refs 在而文件缺 = 数据丢失征兆（§10）；GC 并发窗口由 pin+grace 防护
            LOG_ERROR("duostore: open extent {} failed: {}", path.string(),
                      std::strerror(errno));
            throw S3Error(S3ErrorCode::InternalError, "duostore: extent file missing");
        }
        return fd;
    }

    // pack extent：首次触达时整段 payload 读入并恒校验 crc（payload ≤ pack_threshold
    // 有界），后续 read 从内存切片吐出
    size_t read_pack(const Extent& e, std::span<std::byte> buf) {
        if (!pack_loaded_) {
            int fd = open_extent(e);
            pack_buf_.resize(size_t(e.length));
            size_t got = 0;
            while (got < pack_buf_.size()) {
                ssize_t n = ::pread(fd, pack_buf_.data() + got, pack_buf_.size() - got,
                                    off_t(e.offset + got));
                if (n < 0) {
                    int err = errno;
                    ::close(fd);
                    errno = err;
                    throw_errno("pread pack record");
                }
                if (n == 0) {
                    ::close(fd);
                    throw S3Error(S3ErrorCode::InternalError,
                                  "duostore: pack record shorter than manifest");
                }
                got += size_t(n);
            }
            ::close(fd);
            uint32_t crc = codec::crc32c_of(
                std::span<const std::byte>(pack_buf_.data(), pack_buf_.size()));
            if (crc != e.crc32c) {
                LOG_ERROR("duostore: pack {:016x}+{} crc mismatch (stored {:08x} got {:08x})",
                          e.file_id, e.offset, e.crc32c, crc);
                if (opt_.on_corruption) opt_.on_corruption();
                throw S3Error(S3ErrorCode::InternalError, "duostore: pack record crc mismatch");
            }
            pack_loaded_ = true;
        }
        size_t n = size_t(std::min<uint64_t>({buf.size(), e.length - cur_off_, remaining_}));
        std::memcpy(buf.data(), pack_buf_.data() + cur_off_, n);
        cur_off_ += n;
        remaining_ -= n;
        if (cur_off_ == e.length) advance_extent();
        return n;
    }

    void advance_extent() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        pack_loaded_ = false;
        pack_buf_.clear();
        ++idx_;
        cur_off_ = 0;
    }

    FsDataOptions opt_;
    std::vector<Extent> extents_;
    size_t idx_ = 0;
    uint64_t cur_off_ = 0;
    uint64_t remaining_;
    uint64_t total_;
    int fd_ = -1;
    bool crc_active_ = false;
    uint32_t crc_acc_ = 0;
    bool pack_loaded_ = false;
    std::vector<std::byte> pack_buf_;
    std::shared_ptr<ThreadPool> pool_;
};

}  // namespace

// ---------- FsDataStore ----------

FsDataStore::FsDataStore(FsDataOptions opt, std::shared_ptr<ThreadPool> pool, FileIdAlloc alloc,
                         PackSeal seal, PackMigrateFn migrate, ChunkPinHooks pins)
    : opt_(std::move(opt)), pool_(std::move(pool)), alloc_(std::move(alloc)),
      seal_(std::move(seal)), migrate_(std::move(migrate)), pins_(std::move(pins)) {
    chunk_dirfds_.fill(-1);
    pack_dirfds_.fill(-1);
    std::filesystem::create_directories(opt_.root / "chunks");
    int n = std::max(opt_.pack_writers, 1);
    packs_.reserve(size_t(n));
    for (int i = 0; i < n; ++i) packs_.push_back(std::make_unique<ActivePack>());
}

FsDataStore::~FsDataStore() {
    // 兜底：不经 close() 直接析构等价于崩溃——active pack 不封存（meta 可能已先
    // 关闭，seal 回调不再可用），遗留 unsealed 项由下次启动补封（§5.2 重启弃用）
    for (auto& slot : packs_)
        if (slot->fd >= 0) {
            ::close(slot->fd);
            slot->fd = -1;
        }
    for (auto* fds : {&chunk_dirfds_, &pack_dirfds_})
        for (int& fd : *fds)
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

int FsDataStore::subdir_fd(std::array<int, 256>& fds, const char* sub, unsigned shard) {
    std::lock_guard lk(dir_mu_);
    if (fds[shard] >= 0) return fds[shard];
    char ss[3];
    std::snprintf(ss, sizeof ss, "%02x", shard);
    auto dir = opt_.root / sub / ss;
    std::error_code ec;
    std::filesystem::create_directories(dir, ec);
    if (ec) throw S3Error(S3ErrorCode::InternalError, "create shard dir: " + ec.message());
    int fd = ::open(dir.c_str(), O_RDONLY | O_DIRECTORY);
    if (fd < 0) throw_errno("open shard dir");
    fds[shard] = fd;
    return fd;
}

Task<std::unique_ptr<DataWriter>> FsDataStore::open_writer(WriteHint hint) {
    // 分流（§5.3）：已知长度 > 阈值（或 pack 关闭）直走 chunk；其余（含未知长度）
    // 进缓冲写者，EOF 时 ≤ 阈值整体进 pack、超限即转 chunk
    if (opt_.pack_threshold == 0 ||
        (hint.content_length && *hint.content_length > opt_.pack_threshold))
        co_return std::make_unique<ChunkWriter>(this);
    co_return std::make_unique<FsPackedWriter>(this, std::move(hint.owner));
}

Extent FsDataStore::append_pack_record(std::string_view owner,
                                       std::span<const std::byte> payload) {
    PackAppendItem item{owner, payload};
    return append_pack_records({&item, 1}).at(0);
}

std::vector<Extent> FsDataStore::append_pack_records(std::span<const PackAppendItem> items) {
    std::vector<Extent> out;
    if (items.empty()) return out;
    out.reserve(items.size());

    // 轮询取锁（§5.2）：先 try_lock 扫一圈摊薄排队，全忙则阻塞等在起始槽上
    const unsigned start = pack_rr_.fetch_add(1, std::memory_order_relaxed) % packs_.size();
    ActivePack* slot = nullptr;
    std::unique_lock<std::mutex> lk;
    for (size_t i = 0; i < packs_.size() && !slot; ++i) {
        auto& s = *packs_[(start + i) % packs_.size()];
        std::unique_lock<std::mutex> l(s.m, std::try_to_lock);
        if (l.owns_lock()) {
            slot = &s;
            lk = std::move(l);
        }
    }
    if (!slot) {
        slot = packs_[start].get();
        lk = std::unique_lock(slot->m);
    }

    // 批量化（docs/gaps.md §2.13）：批内逐条 pwrite，fdatasync 收敛到批末一次。
    // 崩溃丢失的只可能是"尚未返回给调用方"的记录——swap/meta 提交都在本函数返回
    // 之后，等价 torn tail（§5.2 重启弃用的预期形态）
    bool dirty = false;
    auto sync_slot = [&] {
        if (!dirty) return;
        if (::fdatasync(slot->fd) != 0) throw_errno("fdatasync pack");
        dirty = false;
    };

    for (const auto& item : items) {
        uint32_t crc = codec::crc32c_of(item.payload);
        std::string header = build_pack_header(item.owner, item.payload.size(), crc);
        const uint64_t rec_size = header.size() + item.payload.size();

        // 达到 pack_max_size 或已开启逾 pack_max_age 即封存、换新 pack_id；size>0
        // 守卫保证单条超限 record（threshold==max 的边界配置）仍可独占一个 pack
        // 落地。封存前先把本批未同步的写落盘——seal 回调回报的 file_size 必须对应
        // 已持久的字节
        if (slot->fd >= 0 && slot->size > 0 &&
            (slot->size + rec_size > opt_.pack_max_size || slot_aged(*slot))) {
            sync_slot();
            close_slot_locked(*slot);
        }
        if (slot->fd < 0) {
            slot->id = alloc_(Extent::Kind::kPack, 1);
            unsigned shard = shard_of(slot->id);
            int dirfd = pack_dirfd(shard);  // 确保 shard 目录存在
            auto path = pack_path(slot->id);
            slot->fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
            if (slot->fd < 0) throw_errno("open pack");
            // active pack 加咨询锁，随 fd 关闭自动释放（封存/进程退出/崩溃都算）。
            // 它是"这个 pack 正被某个活着的进程写"的唯一可靠信号：另一实例启动时
            // 据此区分"我上一代遗留的 pack"与"别人正在写的 pack"，避免把后者补封
            // 后当成低存活 pack 重写甚至整删（docs/gaps.md §1.4）
            if (::flock(slot->fd, LOCK_EX | LOCK_NB) != 0)
                LOG_WARN("duostore: cannot lock active pack {} ({}); concurrent-writer "
                         "detection degraded", slot->id, std::strerror(errno));
            slot->size = 0;
            slot->opened = std::chrono::steady_clock::now();  // 老化轮转起点（§6.1）
            // pack 创建低频（轮转粒度），目录立即 fsync（chunk 是会话末批量，§5.1）
            if (::fsync(dirfd) != 0) throw_errno("fsync pack shard dir");
        }

        // 一次 pwrite 头+payload（§5.2；group-commit 聚合为非目标 §6.3）
        std::string rec = std::move(header);
        rec.append(reinterpret_cast<const char*>(item.payload.data()), item.payload.size());
        size_t off = 0;
        while (off < rec.size()) {
            ssize_t w = ::pwrite(slot->fd, rec.data() + off, rec.size() - off,
                                 off_t(slot->size + off));
            if (w < 0) throw_errno("pwrite pack record");
            off += size_t(w);
        }
        dirty = true;

        out.push_back({Extent::Kind::kPack, slot->id,
                       slot->size + (rec.size() - item.payload.size()), item.payload.size(),
                       crc});
        slot->size += rec_size;
    }
    sync_slot();
    // seal 的 meta 提交在槽锁外补交（docs/gaps.md §3.9）；失败只告警，
    // (id,size) 留在队列由后续写入/close 重试——本批写入自身已安全落盘
    lk.unlock();
    flush_seals(/*rethrow=*/false);
    return out;
}

Task<std::vector<DataRef>> FsDataStore::write_batch(std::span<const PackAppendItem> items) {
    co_await pool_->schedule();
    std::vector<DataRef> out(items.size());
    // pack 适格项批量追加（单槽锁 + 单 fdatasync）；其余（超阈值/pack 关停/空
    // payload）退回逐条 writer——阈值缩小后旧 pack record 也可能超限，分流须保留
    std::vector<size_t> pack_idx;
    std::vector<PackAppendItem> pk;
    for (size_t i = 0; i < items.size(); ++i)
        if (opt_.pack_threshold > 0 && !items[i].payload.empty() &&
            items[i].payload.size() <= opt_.pack_threshold) {
            pack_idx.push_back(i);
            pk.push_back(items[i]);
        }
    if (!pk.empty()) {
        auto exts = append_pack_records(pk);
        for (size_t j = 0; j < exts.size(); ++j) out[pack_idx[j]].extents = {exts[j]};
    }
    std::vector<bool> via_pack(items.size(), false);
    for (size_t i : pack_idx) via_pack[i] = true;
    for (size_t i = 0; i < items.size(); ++i) {
        if (via_pack[i]) continue;
        auto w = co_await open_writer({items[i].payload.size(), std::string(items[i].owner)});
        co_await w->write(items[i].payload);
        out[i] = co_await w->finish();
    }
    co_return out;
}

bool FsDataStore::slot_aged(const ActivePack& slot) const {
    if (opt_.pack_max_age_sec <= 0) return false;
    return std::chrono::steady_clock::now() - slot.opened >=
           std::chrono::seconds(opt_.pack_max_age_sec);
}

// 老化轮转（docs/gaps.md §6.1）：写路径只在"有下一条 record 要写"时才检查年龄，
// 写入停止后 active pack 就永远停在那儿了——正是低写入量场景的形态。GC 每轮调
// 一次本函数补上这个缺口。
// 锁用 try_lock：拿不到说明该槽正在被写，它自己会在下一条 record 处轮转，GC 没
// 有理由为此阻塞（也不该和业务写抢锁）
Task<uint64_t> FsDataStore::seal_aged_packs(int64_t max_age_ms) {
    co_await pool_->schedule();
    uint64_t sealed = 0;
    if (max_age_ms <= 0) co_return 0;
    const auto max_age = std::chrono::milliseconds(max_age_ms);
    const auto now = std::chrono::steady_clock::now();
    for (auto& slot : packs_) {
        std::unique_lock lk(slot->m, std::try_to_lock);
        if (!lk.owns_lock()) continue;
        // 持槽锁 ⇒ 无在途 append ⇒ 槽内字节已被 append_pack_records 末尾的
        // fdatasync 持久化，封存回报的 file_size 对得上盘面
        if (slot->fd < 0 || slot->size == 0 || now - slot->opened < max_age) continue;
        close_slot_locked(*slot);
        ++sealed;
    }
    flush_seals(/*rethrow=*/false);  // 同 append 路径：失败留队列，下次写/close 重试
    co_return sealed;
}

void FsDataStore::close_slot_locked(ActivePack& slot) {
    PendingSeal ps{slot.id, slot.size};
    ::close(slot.fd);
    slot.fd = -1;
    slot.size = 0;
    std::lock_guard lk(seal_mu_);
    seal_retry_.push_back(ps);
}

void FsDataStore::flush_seals(bool rethrow) {
    std::vector<PendingSeal> todo;
    {
        std::lock_guard lk(seal_mu_);
        todo.swap(seal_retry_);
    }
    for (size_t i = 0; i < todo.size(); ++i) {
        try {
            if (seal_) seal_(todo[i].id, todo[i].size);
        } catch (...) {
            // 剩余项放回队列：封存"迟到"等价于 pack 多当一会儿 active（崩溃
            // 恢复本就要补封），丢失才是不可恢复的
            {
                std::lock_guard lk(seal_mu_);
                seal_retry_.insert(seal_retry_.end(), todo.begin() + i, todo.end());
            }
            if (rethrow) throw;
            LOG_WARN("duostore: seal_pack({}) failed; queued for retry on next write/close",
                     todo[i].id);
            return;
        }
    }
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
    size_t done = 0;
    for (const auto& e : extents) {
        if (e.kind == Extent::Kind::kPack) continue;  // pack record 为死区，随压实回收（§9.1）
        if (e.kind != Extent::Kind::kChunk) {
            // 引擎错配（fs 数据引擎收到 kRados extent）：静默跳过会让 GC 全程
            // 空转且无从察觉（docs/gaps.md §4）。告警一次，防回收循环刷屏
            static std::atomic<bool> warned{false};
            if (!warned.exchange(true))
                LOG_ERROR("duostore-fs: remove() got extent kind {} — data/meta engine "
                          "mismatch, these extents cannot be reclaimed by this engine",
                          int(e.kind));
            continue;
        }
        if (::unlink(chunk_path(e.file_id).c_str()) != 0 && errno != ENOENT)
            throw_errno("unlink chunk");  // 幂等：ENOENT 忽略
        // TB 级对象数十万 extent：定期让出，别把一个池线程占死数分钟（gaps §2.11）
        if (++done % 1024 == 0) co_await pool_->schedule();
    }
    co_return;
}

uint64_t FsDataStore::stat_pack(uint64_t pack_id) {
    // GC 判定前回填崩溃遗留 seal(0) 的分母（gaps §2.3b）：一次 stat 即得，免得
    // file_size 未知的 pack 无条件进全量顺扫重写
    struct stat sb;
    if (::stat(pack_path(pack_id).c_str(), &sb) != 0) return 0;
    return uint64_t(sb.st_size);
}

bool FsDataStore::pack_write_locked(uint64_t pack_id) {
    // 试着以非阻塞方式加锁：拿得到 ⇒ 无人持有（自己的 fd 也已随进程消失）；
    // EWOULDBLOCK ⇒ 另有活着的写者。拿到后立刻解锁，不改变任何状态
    int fd = ::open(pack_path(pack_id).c_str(), O_RDONLY);
    if (fd < 0) return false;  // 文件不存在/不可读：交给上层按原逻辑处理
    bool locked_by_other = ::flock(fd, LOCK_EX | LOCK_NB) != 0 && errno == EWOULDBLOCK;
    if (!locked_by_other) ::flock(fd, LOCK_UN);
    ::close(fd);
    return locked_by_other;
}

Task<void> FsDataStore::remove_pack(uint64_t pack_id) {
    co_await pool_->schedule();
    if (::unlink(pack_path(pack_id).c_str()) != 0 && errno != ENOENT)
        throw_errno("unlink pack");  // 幂等：ENOENT 忽略
    co_return;
}

namespace {

uint64_t get_le(const std::byte* p, size_t n) {
    uint64_t v = 0;
    for (size_t i = 0; i < n; ++i) v |= uint64_t(uint8_t(p[i])) << (8 * i);
    return v;
}

// pread 至多 n 字节（循环补齐短读）；EOF 返回实际读到的字节数
size_t pread_upto(int fd, std::byte* buf, size_t n, uint64_t off) {
    size_t got = 0;
    while (got < n) {
        ssize_t r = ::pread(fd, buf + got, n - got, off_t(off + got));
        if (r < 0) throw_errno("pread pack scan");
        if (r == 0) break;
        got += size_t(r);
    }
    return got;
}

}  // namespace

// 压实顺扫（§9.2）：逐 record 解析 → 凭 migrate 回调反查存活并换 ref。本函数从不
// 改动/删除被扫 pack——删除恒走"live 账归零 + 空 pack 整删（延迟）"路径，任何
// 误判（损坏、owner 不可反查）最多让 pack 多活，绝不丢数据。
// 损坏语义（§10）：magic/版本/头长坏 = 无法重同步 → 告警并终止顺扫（后续字节
// 不可信）；payload crc 坏 = 头可信 → 告警跳过该条继续；payload 越过文件尾 =
// torn tail（重启弃用 active pack 的预期残迹，§5.2）→ 静默止扫不计损坏
Task<GcRewrite> FsDataStore::rewrite_pack(uint64_t pack_id) {
    co_await pool_->schedule();
    GcRewrite st;
    auto path = pack_path(pack_id);
    int rfd = ::open(path.c_str(), O_RDONLY);
    if (rfd < 0) throw_errno("open pack for rewrite");
    struct FdGuard {
        int fd;
        ~FdGuard() { ::close(fd); }
    } guard{rfd};
    struct stat sb;
    if (::fstat(rfd, &sb) != 0) throw_errno("fstat pack");
    st.file_size = uint64_t(sb.st_size);

    // 迁移攒批（gaps §2.13 批量化）：K 条一次交付回调——追加侧一次 fdatasync、
    // meta 侧按 owner 聚合换 ref，替代逐条"一次 fdatasync + 一次 meta 提交"
    constexpr size_t kMigrateBatchRecs = 64;
    constexpr uint64_t kMigrateBatchBytes = 4ull << 20;
    std::vector<PackScanRecord> batch;
    uint64_t batch_bytes = 0;
    auto flush_batch = [&]() -> Task<void> {
        if (batch.empty()) co_return;
        st.migrated += co_await migrate_(*this, std::move(batch));
        batch.clear();
        batch_bytes = 0;
        co_await pool_->schedule();  // 迁移含 IO + meta 提交；批间让出池线程
    };

    std::vector<std::byte> hdr(kPackHeaderFixed);
    std::string owner;
    std::vector<std::byte> payload;
    uint64_t off = 0;
    while (off + kPackHeaderFixed <= st.file_size) {
        if (pread_upto(rfd, hdr.data(), hdr.size(), off) < hdr.size()) break;
        if (std::memcmp(hdr.data(), kPackMagic, sizeof kPackMagic) != 0 ||
            uint8_t(hdr[4]) != 1) {
            ++st.corrupt;
            LOG_WARN("duostore: pack {:016x} record at {} has bad magic/version, "
                     "aborting scan (pack kept for manual inspection)", pack_id, off);
            break;
        }
        const uint64_t header_len = get_le(hdr.data() + 6, 2);
        const uint64_t payload_len = get_le(hdr.data() + 8, 8);
        const uint32_t crc = uint32_t(get_le(hdr.data() + 16, 4));
        const uint64_t owner_len = get_le(hdr.data() + 20, 2);
        if (header_len != kPackHeaderFixed + owner_len) {
            ++st.corrupt;
            LOG_WARN("duostore: pack {:016x} record at {} has inconsistent header, "
                     "aborting scan", pack_id, off);
            break;
        }
        if (off + header_len + payload_len > st.file_size) break;  // torn tail（预期）
        owner.resize(owner_len);
        if (owner_len > 0 &&
            pread_upto(rfd, reinterpret_cast<std::byte*>(owner.data()), owner_len,
                       off + kPackHeaderFixed) < owner_len)
            break;
        payload.resize(payload_len);
        if (pread_upto(rfd, payload.data(), payload_len, off + header_len) < payload_len)
            break;
        if (codec::crc32c_of(std::span<const std::byte>(payload)) != crc) {
            ++st.corrupt;
            LOG_WARN("duostore: pack {:016x} record at {} crc mismatch, skipping record",
                     pack_id, off);
            off += header_len + payload_len;
            continue;
        }
        ++st.scanned;
        Extent from{Extent::Kind::kPack, pack_id, off + header_len, payload_len, crc};
        if (migrate_) {
            batch.push_back({owner, from, std::move(payload)});
            payload = {};  // moved-from 复位（下一条 resize 重新分配）
            batch_bytes += payload_len;
            if (batch.size() >= kMigrateBatchRecs || batch_bytes >= kMigrateBatchBytes)
                co_await flush_batch();
        }
        off += header_len + payload_len;
    }
    if (migrate_) co_await flush_batch();
    co_return st;
}

Task<void> FsDataStore::scan_chunks(
    const std::function<void(uint64_t file_id, int64_t mtime_ms)>& cb) {
    co_await pool_->schedule();
    std::error_code ec;
    std::filesystem::directory_iterator shards(opt_.root / "chunks", ec);
    if (ec) co_return;  // 目录不存在 = 无 chunk
    for (const auto& sd : shards) {
        if (!sd.is_directory(ec) || ec) continue;
        std::filesystem::directory_iterator files(sd.path(), ec);
        if (ec) continue;
        for (const auto& f : files) {
            // <file_id:016x>.chk；其余文件（临时/外来）不属本店，忽略
            std::string name = f.path().filename().string();
            if (name.size() != 20 || name.compare(16, 4, ".chk") != 0) continue;
            uint64_t id = 0;
            auto r = std::from_chars(name.data(), name.data() + 16, id, 16);
            if (r.ec != std::errc() || r.ptr != name.data() + 16) continue;
            struct stat sb;
            if (::stat(f.path().c_str(), &sb) != 0) continue;  // 并发 unlink 竞态容忍
            cb(id, int64_t(sb.st_mtim.tv_sec) * 1000 + sb.st_mtim.tv_nsec / 1000000);
        }
    }
    co_return;
}

Task<void> FsDataStore::close() {
    // 封存全部 active pack（§9 生命周期：先 data 后 meta，seal 回调仍可用）；
    // chunk 路径无会话外状态，dirfd 由析构关闭
    co_await pool_->schedule();
    for (auto& slot : packs_) {
        std::lock_guard lk(slot->m);
        if (slot->fd >= 0) close_slot_locked(*slot);
    }
    flush_seals(/*rethrow=*/true);  // 关停路径的封存失败要让调用方看见
    co_return;
}

}  // namespace lights3::storage::duostore
