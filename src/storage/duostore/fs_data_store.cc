#include "storage/duostore/fs_data_store.h"

#include "core/fault.h"

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
#include "storage/xlocalfs/uring_stream.h"

namespace lights3::storage::duostore {

using s3::S3Error;
using s3::S3ErrorCode;
using fsutil::throw_errno;

namespace {

// ss = the low 8 bits of (file_id >> 8): ids are allocated contiguously (segments,
// §4.5), so every 256 consecutive ids share a directory — shard directory fsyncs
// for one write session converge to 1-2 instead of one directory per chunk (§5.1)
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

// ---- pack record header (§5.2) ----
// record := header || payload
// header := magic "LP3R" | u8 ver=1 | u8 flags | u16 header_len | u64 payload_len
//         | u32 crc32c(payload) | u16 owner_len | owner
// Multi-byte integers are little-endian (matching the codec value encoding);
// extent.offset points at the payload — hot-path reads never parse the header;
// the header info (owner/crc redundancy) serves the P4 compaction sequential
// scan and offline salvage
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

// ---------- ChunkWriter: fixed-size-slice streaming write (§5.1/§6.1) ----------
// Contract: the blocking IO of write/finish executes directly on the caller's
// thread — DuoStoreBackend already hops to a pool thread uniformly at the entry
// point (same pattern as localfs's PUT pump loop).

class ChunkWriter final : public DataWriter {
public:
    explicit ChunkWriter(FsDataStore* store) : store_(store) {}

    ~ChunkWriter() override {
        // Destruction without finish = discard: best-effort delete of files already
        // produced; leftovers fall to the orphan scan (§9.3).
        // Write-side pins are released here too — after a successful finish, pin
        // ownership has already transferred to the caller (ChunkPinHooks)
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
            if (ws_) {
                // Pipelined chunk write (roadmap §3.4 ⑤): copy into the stream's block (a
                // registered fixed buffer when available); up to write_depth writes stay
                // in flight while the next body block is being received. The copy is
                // required by the DataWriter contract -- buf is only valid for this call
                std::span<std::byte> wb = co_await ws_->acquire();
                n = std::min(n, wb.size());
                std::memcpy(wb.data(), buf.data(), n);
                ws_->commit(n);
            } else {
                write_full(fd_, buf.data(), n);
            }
            cur_crc_ = codec::crc32c_update(cur_crc_, buf.first(n));
            cur_len_ += n;
            buf = buf.subspan(n);
            if (cur_len_ == store_->opt_.chunk_size) co_await seal_chunk();
        }
        co_return;
    }

    Task<DataRef> finish() override {
        if (fd_ >= 0) co_await seal_chunk();
        // fsync the shard directories touched by this session at its end (§5.1)
        for (unsigned s = 0; s < 256; ++s)
            if (touched_[s] && ::fsync(store_->shard_dirfd(s)) != 0)
                throw_errno("fsync chunk shard dir");
        finished_ = true;
        co_return DataRef{std::move(extents_)};
    }

private:
    void open_next_chunk() {
        // Batch-allocate contiguous runs with geometric growth (docs/archive/gaps.md §3.9):
        // handing out ids one by one to concurrent writers interleaves the chunk
        // ids of the same object, defeating the manifest's run encoding and
        // bloating it instead. The first chunk takes 1 (zero waste for small
        // objects), then 2, 4, … capped at kMaxIdRun; discarding a run's tail is
        // harmless (ids only need to be unique)
        if (run_next_ == run_limit_) {
            run_len_ = run_len_ == 0 ? 1 : std::min<uint32_t>(run_len_ * 2, kMaxIdRun);
            run_next_ = store_->alloc_(Extent::Kind::kChunk, run_len_);
            run_limit_ = run_next_ + run_len_;
        }
        cur_id_ = run_next_++;
        // Pin before creating the file: once the file exists it is protected by the write-side pin, leaving the orphan scan no observation window
        if (store_->pins_.pin) {
            store_->pins_.pin(cur_id_);
            pinned_.push_back(cur_id_);
        }
        unsigned shard = shard_of(cur_id_);
        store_->shard_dirfd(shard);  // ensure the shard directory exists
        auto path = store_->chunk_path(cur_id_);
        fd_ = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
        if (fd_ < 0) throw_errno("open chunk");
        if (store_->opt_.uring)
            ws_ = std::make_unique<UringWriteStream>(store_->opt_.uring, fd_, 0,
                                                     store_->opt_.chunk_size);
        touched_[shard] = true;
        cur_len_ = 0;
        cur_crc_ = 0;
    }

    Task<void> seal_chunk() {
        if (ws_) {
            // Final write + fdatasync as one linked chain (roadmap §3.4 ③); -EINVAL
            // (filesystem unsupported) is tolerated by the stream, matching fsync_file
            auto ws = std::move(ws_);
            co_await ws->finish(/*fdatasync=*/true);
        } else if (::fdatasync(fd_) != 0) {
            throw_errno("fdatasync chunk");
        }
        ::close(fd_);
        fd_ = -1;
        extents_.push_back({Extent::Kind::kChunk, cur_id_, 0, cur_len_, cur_crc_});
        co_return;
    }

    FsDataStore* store_;
    std::vector<Extent> extents_;
    std::vector<uint64_t> pinned_;  // ids write-side pinned by this session (ownership transfers to the caller after finish)
    std::bitset<256> touched_;
    std::unique_ptr<UringWriteStream> ws_;  // current chunk's write pipeline (uring mode)
    int fd_ = -1;
    uint64_t cur_id_ = 0;
    uint64_t cur_len_ = 0;
    uint32_t cur_crc_ = 0;
    uint64_t run_next_ = 0, run_limit_ = 0;  // this session's contiguous id run (§3.9 batch allocation)
    uint32_t run_len_ = 0;
    bool finished_ = false;
};

// ---------- FsPackedWriter: pack/chunk routing writer (§5.2/§5.3) ----------
// Objects ≤ pack_threshold buffer in memory first and are appended into a pack as
// a whole at EOF; once the threshold is exceeded (only possible for a chunked PUT
// of unknown length) it switches to the chunk streaming path and flushes the
// buffer to disk. Memory upper bound = pack_threshold × max_inflight_requests
// (the two settings are coupled, §5.3).
// Destruction without finish: the buffer is simply discarded (no trace on disk);
// if already spilled, ChunkWriter handles cleanup.

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
        // Threshold exceeded (unknown-length stream of a chunked PUT): flush the buffer via the chunk path, then go streaming
        spill_ = std::make_unique<ChunkWriter>(store_);
        co_await spill_->write(std::span<const std::byte>(buf_.data(), buf_.size()));
        buf_.clear();
        buf_.shrink_to_fit();
        co_await spill_->write(buf);
    }

    Task<DataRef> finish() override {
        if (spill_) co_return co_await spill_->finish();
        if (buf_.empty()) co_return DataRef{};  // 0-byte object: empty DataRef
        Extent e = co_await store_->append_pack_record(
            owner_, std::span<const std::byte>(buf_.data(), buf_.size()));
        co_return DataRef{{e}};
    }

private:
    FsDataStore* store_;
    std::string owner_;
    std::vector<std::byte> buf_;
    std::unique_ptr<ChunkWriter> spill_;
};

// ---------- ExtentChainReader: multi-file-chain streaming read (§7) ----------
// Lazily opens the current extent's fd (each block runs on a pool thread, same
// pattern as localfs's FdStreamReader; not reused — that one has single-fd
// ownership semantics, this is a multi-file chain).
// Self-contained: holds a copy of FsDataOptions rather than an FsDataStore
// pointer — the ObjectStream escapes the backend's lifetime along with the HTTP
// response (the driver keeps pumping after the handler returns), so the reader
// must not depend on the backend staying alive (matching FdStreamReader's
// self-contained semantics).
// pack extents: the whole payload is read in at once with crc32c always verified,
// then sliced out by range (§7).

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
        if (e.kind == Extent::Kind::kPack) co_return co_await read_pack(e, buf);
        if (fd_ < 0 && !stream_) {
            int fd = open_extent(e);
            // crc verification is only feasible when reading the full extent from start to end (a Range hitting the middle cannot be verified, §7)
            crc_active_ = opt_.verify_chunk_crc && cur_off_ == 0 && remaining_ >= e.length;
            crc_acc_ = 0;
            if (opt_.uring)
                // Read-ahead chunk stream (roadmap §3.4 ⑤①); fd ownership moves to it
                stream_ = std::make_unique<UringReadStream>(
                    opt_.uring, fd, e.offset + cur_off_,
                    std::min<uint64_t>(e.length - cur_off_, remaining_));
            else
                fd_ = fd;
        }
        size_t want = size_t(std::min<uint64_t>({buf.size(), e.length - cur_off_, remaining_}));
        ssize_t n;
        if (stream_) {
            n = ssize_t(co_await stream_->read(buf.first(want)));
        } else {
            n = ::pread(fd_, buf.data(), want, off_t(e.offset + cur_off_));
            if (n < 0) throw_errno("pread extent");
        }
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
            // refs present but file missing = sign of data loss (§10); the GC concurrency window is guarded by pin+grace
            LOG_ERROR("duostore: open extent {} failed: {}", path.string(),
                      std::strerror(errno));
            throw S3Error(S3ErrorCode::InternalError, "duostore: extent file missing");
        }
        return fd;
    }

    // pack extent: on first touch, read the whole payload in and always verify
    // crc (bounded by payload ≤ pack_threshold); subsequent reads slice from memory
    Task<size_t> read_pack(const Extent& e, std::span<std::byte> buf) {
        if (!pack_loaded_) {
            int fd = open_extent(e);
            pack_buf_.resize(size_t(e.length));
            if (opt_.uring) {
                UringReadStream rs(opt_.uring, fd, e.offset, e.length);  // owns fd
                size_t got = 0;
                while (got < pack_buf_.size()) {
                    size_t n = co_await rs.read(
                        std::span(pack_buf_.data() + got, pack_buf_.size() - got));
                    if (n == 0)
                        throw S3Error(S3ErrorCode::InternalError,
                                      "duostore: pack record shorter than manifest");
                    got += n;
                }
            } else {
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
            }
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
        co_return n;
    }

    void advance_extent() {
        if (fd_ >= 0) {
            ::close(fd_);
            fd_ = -1;
        }
        stream_.reset();
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
    std::unique_ptr<UringReadStream> stream_;  // current chunk extent's read-ahead stream (uring mode)
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
    // Fallback: destruction without close() is equivalent to a crash — active
    // packs are not sealed (meta may already be closed, so the seal callback is no
    // longer usable); leftover unsealed entries are catch-up sealed on the next
    // startup (§5.2 discard-on-restart)
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
    // Routing (§5.3): known length > threshold (or packs disabled) goes straight
    // to chunk; everything else (including unknown length) goes to the buffering
    // writer — at EOF, ≤ threshold goes into a pack as a whole, over the limit
    // switches to chunk
    if (opt_.pack_threshold == 0 ||
        (hint.content_length && *hint.content_length > opt_.pack_threshold))
        co_return std::make_unique<ChunkWriter>(this);
    co_return std::make_unique<FsPackedWriter>(this, std::move(hint.owner));
}

Task<Extent> FsDataStore::append_pack_record(std::string_view owner,
                                             std::span<const std::byte> payload) {
    PackAppendItem item{owner, payload};  // lives in this frame, valid across the co_await
    auto v = co_await append_pack_records({&item, 1});
    co_return v.at(0);
}

Task<std::vector<Extent>> FsDataStore::append_pack_records(
    std::span<const PackAppendItem> items) {
    std::vector<Extent> out;
    if (items.empty()) co_return out;
    out.reserve(items.size());

    // With the uring engine, the end-of-batch fdatasync runs off-lock on a dup of the
    // pack fd (roadmap §3.4 ⑤): the slot std::mutex cannot be held across a suspension
    // point. Correctness: a whole-file fdatasync needs no lock, and rotation/sealing
    // always run their own blocking sync inside the lock before closing the fd, so a
    // seal's reported file_size still corresponds to persisted bytes; this batch's own
    // durability is settled before the extents are returned
    int async_sync_fd = -1;
    {
        // Round-robin lock acquisition (§5.2): first sweep with try_lock to spread queuing; if all busy, block on the starting slot
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

        // Batching (docs/archive/gaps.md §2.13): per-item pwrite within the batch, fdatasync
        // converges to one at the batch end. A crash can only lose records "not yet
        // returned to the caller" — swap/meta commits all happen after this function
        // returns, equivalent to a torn tail (§5.2's expected discard-on-restart form)
        bool dirty = false;
        auto sync_slot = [&] {
            if (!dirty) return;
            if (int fe = fault::check("duostore.pack.fdatasync")) {  // roadmap §6.1
                errno = fe;
                throw_errno("fdatasync pack");
            }
            if (::fdatasync(slot->fd) != 0) throw_errno("fdatasync pack");
            dirty = false;
        };

        for (const auto& item : items) {
        uint32_t crc = codec::crc32c_of(item.payload);
        std::string header = build_pack_header(item.owner, item.payload.size(), crc);
        const uint64_t rec_size = header.size() + item.payload.size();

        // Seal and switch to a new pack_id upon reaching pack_max_size or being
        // open longer than pack_max_age; the size>0 guard ensures a single
        // over-limit record (the threshold==max edge configuration) can still land
        // in a pack of its own. Before sealing, flush this batch's unsynced writes
        // — the file_size reported by the seal callback must correspond to
        // persisted bytes
        if (slot->fd >= 0 && slot->size > 0 &&
            (slot->size + rec_size > opt_.pack_max_size || slot_aged(*slot))) {
            sync_slot();
            close_slot_locked(*slot);
        }
        if (slot->fd < 0) {
            slot->id = alloc_(Extent::Kind::kPack, 1);
            unsigned shard = shard_of(slot->id);
            int dirfd = pack_dirfd(shard);  // ensure the shard directory exists
            auto path = pack_path(slot->id);
            slot->fd = ::open(path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
            if (slot->fd < 0) throw_errno("open pack");
            // Take an advisory lock on the active pack, auto-released when the fd
            // closes (sealing / process exit / crash all count). It is the only
            // reliable signal that "this pack is being written by a live process":
            // another instance starting up uses it to distinguish "packs left by my
            // previous generation" from "packs someone else is writing", avoiding
            // catch-up sealing the latter and then rewriting or even deleting it as
            // a low-liveness pack (docs/archive/gaps.md §1.4)
            if (::flock(slot->fd, LOCK_EX | LOCK_NB) != 0)
                LOG_WARN("duostore: cannot lock active pack {} ({}); concurrent-writer "
                         "detection degraded", slot->id, std::strerror(errno));
            slot->size = 0;
            slot->opened = std::chrono::steady_clock::now();  // age-based rotation start point (§6.1)
            // pack creation is low-frequency (rotation granularity), fsync the directory immediately (chunks batch at session end, §5.1)
            if (::fsync(dirfd) != 0) throw_errno("fsync pack shard dir");
        }

        // One pwrite for header+payload (§5.2; group-commit aggregation is a non-goal §6.3)
        std::string rec = std::move(header);
        rec.append(reinterpret_cast<const char*>(item.payload.data()), item.payload.size());
        size_t off = 0;
        while (off < rec.size()) {
            if (int fe = fault::check("duostore.pack.pwrite")) {  // roadmap §6.1
                errno = fe;
                throw_errno("pwrite pack record");
            }
            ssize_t w = ::pwrite(slot->fd, rec.data() + off, rec.size() - off,
                                 off_t(slot->size + off));
            if (w < 0) throw_errno("pwrite pack record");
            off += size_t(w);
        }
        dirty = true;

            out.push_back({Extent::Kind::kPack, slot->id,
                           slot->size + (rec.size() - item.payload.size()),
                           item.payload.size(), crc});
            slot->size += rec_size;
        }
        if (dirty && opt_.uring) {
            async_sync_fd = ::dup(slot->fd);
            if (async_sync_fd < 0) sync_slot();  // dup failed: fall back to the blocking sync
        } else {
            sync_slot();
        }
    }  // slot lock released
    if (async_sync_fd >= 0) {
        int r;
        try {
            r = co_await opt_.uring->fdatasync(async_sync_fd);
        } catch (...) {
            ::close(async_sync_fd);
            throw;
        }
        ::close(async_sync_fd);
        if (r < 0 && r != -EINVAL)  // EINVAL: fs unsupported, matching fsync_file semantics
            throw S3Error(S3ErrorCode::InternalError,
                          std::string("fdatasync pack: ") + std::strerror(-r));
    }
    // The seal's meta commit is submitted outside the slot lock (docs/archive/gaps.md
    // §3.9); failure only warns, and (id,size) stays in the queue for later
    // writes/close to retry — this batch's own writes are already safely on disk
    flush_seals(/*rethrow=*/false);
    co_return out;
}

Task<std::vector<DataRef>> FsDataStore::write_batch(std::span<const PackAppendItem> items) {
    co_await pool_->schedule();
    std::vector<DataRef> out(items.size());
    // Pack-eligible items are batch-appended (one slot lock + one fdatasync); the
    // rest (over threshold / packs disabled / empty payload) fall back to the
    // per-item writer — after a threshold shrink, old pack records may also exceed
    // the limit, so the routing must remain
    std::vector<size_t> pack_idx;
    std::vector<PackAppendItem> pk;
    for (size_t i = 0; i < items.size(); ++i)
        if (opt_.pack_threshold > 0 && !items[i].payload.empty() &&
            items[i].payload.size() <= opt_.pack_threshold) {
            pack_idx.push_back(i);
            pk.push_back(items[i]);
        }
    if (!pk.empty()) {
        auto exts = co_await append_pack_records(pk);
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

// Age-based rotation (docs/archive/gaps.md §6.1): the write path only checks age when
// "there is a next record to write"; once writes stop, the active pack sits there
// forever — exactly the low-write-volume scenario. GC calls this function once
// per round to fill that gap.
// try_lock for the lock: failing to acquire means the slot is being written, and
// it will rotate itself at the next record; GC has no reason to block for it
// (nor should it contend with business writes for the lock)
Task<uint64_t> FsDataStore::seal_aged_packs(int64_t max_age_ms) {
    co_await pool_->schedule();
    uint64_t sealed = 0;
    if (max_age_ms <= 0) co_return 0;
    const auto max_age = std::chrono::milliseconds(max_age_ms);
    const auto now = std::chrono::steady_clock::now();
    for (auto& slot : packs_) {
        std::unique_lock lk(slot->m, std::try_to_lock);
        if (!lk.owns_lock()) continue;
        // Holding the slot lock ⇒ no in-flight append ⇒ the slot's bytes were
        // persisted by the fdatasync at the end of append_pack_records, so the
        // file_size reported by sealing matches the disk
        if (slot->fd < 0 || slot->size == 0 || now - slot->opened < max_age) continue;
        close_slot_locked(*slot);
        ++sealed;
    }
    flush_seals(/*rethrow=*/false);  // same as the append path: failures stay queued, retried on the next write/close
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
            // Put remaining items back on the queue: a "late" seal just means the
            // pack stays active a bit longer (crash recovery has to catch-up seal
            // anyway); only loss would be unrecoverable
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
                      "duostore: reader range beyond manifest");  // caller already ran resolve_range
    co_return std::make_unique<ExtentChainReader>(opt_, std::move(ref.extents), first, last,
                                                  pool_);
}

Task<void> FsDataStore::remove(std::span<const Extent> extents) {
    co_await pool_->schedule();
    size_t done = 0;
    for (const auto& e : extents) {
        if (e.kind == Extent::Kind::kPack) continue;  // pack records become dead regions, reclaimed via compaction (§9.1)
        if (e.kind != Extent::Kind::kChunk) {
            // Engine mismatch (fs data engine received a kRados extent): silently
            // skipping would let GC spin uselessly with no way to notice
            // (docs/archive/gaps.md §4). Warn once to keep the reclaim loop from flooding logs
            static std::atomic<bool> warned{false};
            if (!warned.exchange(true))
                LOG_ERROR("duostore-fs: remove() got extent kind {} — data/meta engine "
                          "mismatch, these extents cannot be reclaimed by this engine",
                          int(e.kind));
            continue;
        }
        if (::unlink(chunk_path(e.file_id).c_str()) != 0 && errno != ENOENT)
            throw_errno("unlink chunk");  // idempotent: ENOENT ignored
        // TB-scale objects have hundreds of thousands of extents: yield
        // periodically instead of monopolizing one pool thread for minutes (gaps §2.11)
        if (++done % 1024 == 0) co_await pool_->schedule();
    }
    co_return;
}

uint64_t FsDataStore::stat_pack(uint64_t pack_id) {
    // Backfill the denominator for crash-leftover seal(0) before the GC decision
    // (gaps §2.3b): one stat suffices, so a pack with unknown file_size does not
    // unconditionally go into a full sequential-scan rewrite
    struct stat sb;
    if (::stat(pack_path(pack_id).c_str(), &sb) != 0) return 0;
    return uint64_t(sb.st_size);
}

bool FsDataStore::pack_write_locked(uint64_t pack_id) {
    // Try to lock non-blockingly: acquiring it ⇒ nobody holds it (our own fd is
    // gone with the process too); EWOULDBLOCK ⇒ another live writer exists.
    // Unlock immediately after acquiring; no state is changed
    int fd = ::open(pack_path(pack_id).c_str(), O_RDONLY);
    if (fd < 0) return false;  // file missing/unreadable: leave it to the caller's original logic
    bool locked_by_other = ::flock(fd, LOCK_EX | LOCK_NB) != 0 && errno == EWOULDBLOCK;
    if (!locked_by_other) ::flock(fd, LOCK_UN);
    ::close(fd);
    return locked_by_other;
}

Task<void> FsDataStore::remove_pack(uint64_t pack_id) {
    co_await pool_->schedule();
    if (::unlink(pack_path(pack_id).c_str()) != 0 && errno != ENOENT)
        throw_errno("unlink pack");  // idempotent: ENOENT ignored
    co_return;
}

namespace {

uint64_t get_le(const std::byte* p, size_t n) {
    uint64_t v = 0;
    for (size_t i = 0; i < n; ++i) v |= uint64_t(uint8_t(p[i])) << (8 * i);
    return v;
}

// pread up to n bytes (looping to fill short reads); on EOF returns the byte count actually read
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

// Compaction sequential scan (§9.2): parse record by record → use the migrate
// callback to reverse-check liveness and swap refs. This function never
// modifies/deletes the scanned pack — deletion always goes through the "live
// account reaches zero + delete the whole empty pack (deferred)" path, so any
// misjudgment (corruption, owner not resolvable) at worst keeps the pack alive
// longer, never loses data.
// Corruption semantics (§10): bad magic/version/header length = cannot resync →
// warn and abort the scan (subsequent bytes untrustworthy); bad payload crc =
// header trustworthy → warn, skip the record, continue; payload past EOF =
// torn tail (expected residue of an active pack discarded on restart, §5.2) →
// stop scanning silently, not counted as corruption
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

    // Migration batching (gaps §2.13): K records delivered to the callback at
    // once — one fdatasync on the append side and owner-aggregated ref swaps on
    // the meta side, replacing the per-record "one fdatasync + one meta commit"
    constexpr size_t kMigrateBatchRecs = 64;
    constexpr uint64_t kMigrateBatchBytes = 4ull << 20;
    std::vector<PackScanRecord> batch;
    uint64_t batch_bytes = 0;
    auto flush_batch = [&]() -> Task<void> {
        if (batch.empty()) co_return;
        st.migrated += co_await migrate_(*this, std::move(batch));
        batch.clear();
        batch_bytes = 0;
        co_await pool_->schedule();  // migration involves IO + meta commits; yield the pool thread between batches
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
        if (off + header_len + payload_len > st.file_size) break;  // torn tail (expected)
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
            payload = {};  // reset moved-from state (the next record's resize reallocates)
            batch_bytes += payload_len;
            if (batch.size() >= kMigrateBatchRecs || batch_bytes >= kMigrateBatchBytes)
                co_await flush_batch();
        }
        off += header_len + payload_len;
    }
    if (migrate_) co_await flush_batch();
    co_return st;
}

// chunks/ and packs/ have isomorphic layouts (shard directory + <id:016x><suffix>), so the enumeration logic is shared
Task<void> FsDataStore::scan_shard_tree(
    const char* sub, const char* suffix,
    const std::function<void(uint64_t, int64_t, uint64_t)>& cb) {
    co_await pool_->schedule();
    std::error_code ec;
    std::filesystem::directory_iterator shards(opt_.root / sub, ec);
    if (ec) co_return;  // directory missing = no entities
    const size_t suffix_len = std::strlen(suffix);
    for (const auto& sd : shards) {
        if (!sd.is_directory(ec) || ec) continue;
        std::filesystem::directory_iterator files(sd.path(), ec);
        if (ec) continue;
        for (const auto& f : files) {
            // <file_id:016x><suffix>; other files (temporary/foreign) do not belong to this store — ignore
            std::string name = f.path().filename().string();
            if (name.size() != 16 + suffix_len || name.compare(16, suffix_len, suffix) != 0)
                continue;
            uint64_t id = 0;
            auto r = std::from_chars(name.data(), name.data() + 16, id, 16);
            if (r.ec != std::errc() || r.ptr != name.data() + 16) continue;
            struct stat sb;
            if (::stat(f.path().c_str(), &sb) != 0) continue;  // tolerate concurrent-unlink races
            cb(id, int64_t(sb.st_mtim.tv_sec) * 1000 + sb.st_mtim.tv_nsec / 1000000,
               uint64_t(sb.st_size));
        }
    }
    co_return;
}

Task<void> FsDataStore::scan_chunks(
    const std::function<void(uint64_t file_id, int64_t mtime_ms, uint64_t size)>& cb) {
    return scan_shard_tree("chunks", ".chk", cb);
}

Task<void> FsDataStore::scan_packs(
    const std::function<void(uint64_t pack_id, int64_t mtime_ms, uint64_t size)>& cb) {
    return scan_shard_tree("packs", ".pak", cb);
}

Task<void> FsDataStore::close() {
    // Seal all active packs (§9 lifecycle: data before meta, so the seal callback
    // is still usable); the chunk path has no out-of-session state, dirfds are
    // closed by the destructor
    co_await pool_->schedule();
    for (auto& slot : packs_) {
        std::lock_guard lk(slot->m);
        if (slot->fd >= 0) close_slot_locked(*slot);
    }
    flush_seals(/*rethrow=*/true);  // sealing failures on the shutdown path must be visible to the caller
    // Stop the engine's reaper threads; escaped readers keep the engine object alive via
    // their options copies, but their next submission fails with InternalError (same
    // close-ordering assumption as xlocalfs)
    if (opt_.uring) opt_.uring->shutdown();
    co_return;
}

}  // namespace lights3::storage::duostore
