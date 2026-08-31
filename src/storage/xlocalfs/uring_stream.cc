#include "storage/xlocalfs/uring_stream.h"

#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <deque>
#include <vector>

#include "core/log.h"
#include "s3/errors.h"

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;

namespace uring_detail {

namespace {

[[noreturn]] void throw_uring(const char* what, int neg_errno) {
    throw S3Error(S3ErrorCode::InternalError,
                  std::string(what) + ": " + std::strerror(-neg_errno));
}

// Register the fd in the fixed file table only when the stream is long enough for the
// saved per-IO fget/fput to outweigh the two FILES_UPDATE syscalls
constexpr uint64_t kFixedFileMinBytes = 512 * 1024;

}  // namespace

// Slot lifecycle: kIdle -(owner submits)-> kInflight -(reaper)-> kDone -(owner settles)->
// kIdle. A consumer parking on an in-flight slot swaps its coroutine handle into `sync`;
// the completion swaps kDone in and resumes whatever handle it displaced. The owner is the
// only writer of the non-atomic fields while the slot is not in flight
constexpr uintptr_t kIdle = 0;
constexpr uintptr_t kInflight = 1;
constexpr uintptr_t kDone = 2;

struct StreamState;

struct Slot final : UringEngine::Op {
    StreamState* st = nullptr;
    std::span<std::byte> mem;
    UringEngine::FixedBuf fixed;             // index<0 = heap block
    std::unique_ptr<std::byte[]> heap;
    struct iovec iov {};                     // READV/WRITEV fallback framing (pre-5.6)
    std::atomic<uintptr_t> sync{kIdle};
    int res = 0;
    uint64_t off = 0;      // file offset of the current op
    unsigned len = 0;      // bytes requested by the current op
    unsigned mem_off = 0;  // block offset (short-write resubmission)
    unsigned pos = 0;      // consumer cursor within res (read stream)
    uint32_t gen = 0;      // read stream staleness marker

    void complete(int r) noexcept override;

    struct Awaiter {
        Slot& s;
        bool await_ready() const noexcept {
            return s.sync.load(std::memory_order_acquire) == kDone;
        }
        bool await_suspend(std::coroutine_handle<> h) noexcept {
            uintptr_t expected = kInflight;
            // CAS failure = the completion already landed between ready and here: resume
            // in place instead of suspending
            return s.sync.compare_exchange_strong(expected,
                                                  reinterpret_cast<uintptr_t>(h.address()),
                                                  std::memory_order_acq_rel,
                                                  std::memory_order_acquire);
        }
        int await_resume() const noexcept { return s.res; }
    };
    Awaiter wait() { return {*this}; }
};

// All IO state shared with in-flight ops. Intrusive refcount: one reference per in-flight
// op plus one for the owning stream; the last release runs the destructor (possibly on the
// reaper thread after the owner abandoned mid-flight), which returns fixed buffers,
// unregisters the file slot and closes the fd -- nothing the kernel may still touch is
// freed earlier
struct StreamState {
    std::shared_ptr<UringEngine> eng;
    unsigned ring = 0;
    int fd = -1;
    bool own_fd = false;
    int file_slot = -1;
    unsigned block = 0;
    unsigned depth = 0;      // data slots
    int fsync_slot = -1;     // extra bufferless slot (write streams)
    std::vector<Slot> slots;
    // Write-stream bookkeeping (owner-thread only)
    std::deque<int> wq;      // in-flight write slots, oldest first
    std::vector<int> free_;
    // Read-stream bookkeeping (owner-thread only)
    uint64_t next_off = 0;
    uint64_t end_off = 0;
    uint32_t gen = 0;

    std::atomic<unsigned> refs{1};
    void ref() { refs.fetch_add(1, std::memory_order_relaxed); }
    void unref() {
        if (refs.fetch_sub(1, std::memory_order_acq_rel) == 1) delete this;
    }

    ~StreamState() {
        for (auto& s : slots)
            if (s.fixed.index >= 0) eng->release_fixed(s.fixed);
        if (file_slot >= 0) eng->unregister_file(ring, file_slot);
        if (own_fd && fd >= 0) ::close(fd);
    }

    static StreamState* make(std::shared_ptr<UringEngine> eng, int fd, bool own_fd,
                             unsigned data_slots, bool with_fsync_slot,
                             std::optional<uint64_t> expected_len) {
        auto* st = new StreamState;
        st->eng = std::move(eng);
        st->ring = st->eng->pick_ring();
        st->fd = fd;
        st->own_fd = own_fd;
        st->block = st->eng->options().block_size;
        st->depth = data_slots;
        st->slots = std::vector<Slot>(data_slots + (with_fsync_slot ? 1 : 0));
        for (unsigned i = 0; i < data_slots; ++i) {
            Slot& s = st->slots[i];
            s.st = st;
            if (!st->eng->try_acquire_fixed(st->ring, s.fixed)) {
                s.heap = std::make_unique<std::byte[]>(st->block);
                s.mem = std::span<std::byte>(s.heap.get(), st->block);
            } else {
                s.mem = s.fixed.mem;
            }
        }
        if (with_fsync_slot) {
            st->fsync_slot = int(data_slots);
            st->slots[data_slots].st = st;
        }
        if (expected_len && *expected_len >= kFixedFileMinBytes)
            st->file_slot = st->eng->register_file(st->ring, fd);
        return st;
    }

    UringEngine::Sqe make_rw(bool write, Slot& s) {
        const UringFeatures& feat = eng->features();
        UringEngine::Sqe q;
        std::byte* p = s.mem.data() + s.mem_off;
        if (s.fixed.index >= 0) {  // registration is gated on op_fixed_rw
            q.opcode = write ? uint8_t(IORING_OP_WRITE_FIXED) : uint8_t(IORING_OP_READ_FIXED);
            q.buf_index = uint16_t(s.fixed.index);
            q.addr = reinterpret_cast<uint64_t>(p);
            q.len = s.len;
        } else if (feat.op_read_write) {
            q.opcode = write ? uint8_t(IORING_OP_WRITE) : uint8_t(IORING_OP_READ);
            q.addr = reinterpret_cast<uint64_t>(p);
            q.len = s.len;
        } else {
            q.opcode = write ? uint8_t(IORING_OP_WRITEV) : uint8_t(IORING_OP_READV);
            s.iov.iov_base = p;
            s.iov.iov_len = s.len;
            q.addr = reinterpret_cast<uint64_t>(&s.iov);
            q.len = 1;
        }
        if (file_slot >= 0) {
            q.fd = file_slot;
            q.flags |= IOSQE_FIXED_FILE;
        } else {
            q.fd = fd;
        }
        q.off = s.off;
        return q;
    }

    UringEngine::Sqe make_fsync(Slot& s) {
        (void)s;
        UringEngine::Sqe q;
        q.opcode = IORING_OP_FSYNC;
        q.op_flags = IORING_FSYNC_DATASYNC;
        if (file_slot >= 0) {
            q.fd = file_slot;
            q.flags |= IOSQE_FIXED_FILE;
        } else {
            q.fd = fd;
        }
        return q;
    }

    void submit_one(Slot& s, const UringEngine::Sqe& q) {
        s.res = 0;
        s.sync.store(kInflight, std::memory_order_relaxed);
        ref();
        UringEngine::Op* ops[1] = {&s};
        try {
            eng->submit(ring, std::span<const UringEngine::Sqe>(&q, 1),
                        std::span<UringEngine::Op* const>(ops, 1));
        } catch (...) {
            s.sync.store(kIdle, std::memory_order_relaxed);
            unref();
            throw;
        }
    }

    void submit_rw(Slot& s, bool write) { submit_one(s, make_rw(write, s)); }

    // WRITE(link) -> FSYNC pushed as one contiguous chain (roadmap §3.4 ③): the fsync
    // starts only after the write completes, one submission for both
    void submit_write_fsync_chain(Slot& w, Slot& f) {
        UringEngine::Sqe qs[2] = {make_rw(true, w), make_fsync(f)};
        qs[0].flags |= IOSQE_IO_LINK;
        w.res = f.res = 0;
        w.sync.store(kInflight, std::memory_order_relaxed);
        f.sync.store(kInflight, std::memory_order_relaxed);
        ref();
        ref();
        UringEngine::Op* ops[2] = {&w, &f};
        try {
            eng->submit(ring, qs, ops);
        } catch (...) {
            w.sync.store(kIdle, std::memory_order_relaxed);
            f.sync.store(kIdle, std::memory_order_relaxed);
            unref();
            unref();
            throw;
        }
    }
};

void Slot::complete(int r) noexcept {
    StreamState* state = st;
    res = r;
    uintptr_t prev = sync.exchange(kDone, std::memory_order_acq_rel);
    if (prev != kInflight)
        // A consumer is parked on this slot (it cannot be the owner tearing the stream
        // down -- a parked consumer is suspended); hand its continuation to the pool
        state->eng->pool().post(
            [h = std::coroutine_handle<>::from_address(reinterpret_cast<void*>(prev))] {
                h.resume();
            });
    state->unref();  // may delete the state (and this slot) -- must be the last access
}

}  // namespace uring_detail

using uring_detail::kIdle;
using uring_detail::Slot;
using uring_detail::StreamState;

// ---------------------------------------------------------------------------
// UringReadStream
// ---------------------------------------------------------------------------

UringReadStream::UringReadStream(std::shared_ptr<UringEngine> eng, int fd, uint64_t off,
                                 uint64_t len, bool own_fd)
    : remaining_(len) {
    unsigned depth = eng->options().read_depth;
    // No point keeping more blocks in flight than the stream has
    uint64_t blocks = (len + eng->options().block_size - 1) / eng->options().block_size;
    depth = unsigned(std::clamp<uint64_t>(blocks, 1, depth));
    st_ = StreamState::make(std::move(eng), fd, own_fd, depth, /*with_fsync_slot=*/false,
                            len);
    st_->next_off = off;
    st_->end_off = off + len;
}

UringReadStream::~UringReadStream() { st_->unref(); }

void UringReadStream::fill() {
    StreamState& st = *st_;
    while (inflight_ < st.depth && st.next_off < st.end_off) {
        Slot& s = st.slots[(head_ + inflight_) % st.depth];
        s.off = st.next_off;
        s.len = unsigned(std::min<uint64_t>(st.block, st.end_off - st.next_off));
        s.mem_off = 0;
        s.pos = 0;
        s.gen = st.gen;
        st.submit_rw(s, /*write=*/false);
        st.next_off += s.len;
        ++inflight_;
    }
}

Task<size_t> UringReadStream::read(std::span<std::byte> out) {
    if (done_ || out.empty()) co_return 0;
    StreamState& st = *st_;
    for (;;) {
        fill();
        if (inflight_ == 0) {  // [off, end) exhausted
            done_ = true;
            co_return 0;
        }
        Slot& s = st.slots[head_];
        int r = co_await s.wait();
        if (s.gen != st.gen) {
            // Stale read-ahead ordered before a short read was noticed: its offset no
            // longer lines up with the consumer position; discard and let fill() resubmit
            s.sync.store(kIdle, std::memory_order_relaxed);
            head_ = (head_ + 1) % st.depth;
            --inflight_;
            continue;
        }
        if (r < 0) {
            done_ = true;
            uring_detail::throw_uring("io_uring read", r);
        }
        if (r == 0) {  // file truncated externally, early EOF (FdStreamReader semantics)
            done_ = true;
            remaining_ = 0;
            co_return 0;
        }
        if (s.pos == 0 && unsigned(r) < s.len && s.off + unsigned(r) < st.end_off) {
            // Short read mid-stream (truncation race): every block submitted after this
            // one targets offsets beyond the gap. Bump the generation so they complete as
            // discards and resume submission from the actual position
            ++st.gen;
            s.gen = st.gen;  // this block's bytes are still valid
            st.next_off = s.off + unsigned(r);
        }
        size_t n = std::min({out.size(), size_t(unsigned(r) - s.pos), size_t(remaining_)});
        std::memcpy(out.data(), s.mem.data() + s.pos, n);
        s.pos += unsigned(n);
        remaining_ -= n;
        if (remaining_ == 0) done_ = true;
        if (s.pos == unsigned(r) || done_) {
            s.sync.store(kIdle, std::memory_order_relaxed);
            head_ = (head_ + 1) % st.depth;
            --inflight_;
            if (!done_) fill();  // keep the pipeline full before handing bytes up
        }
        co_return n;
    }
}

// ---------------------------------------------------------------------------
// UringWriteStream
// ---------------------------------------------------------------------------

UringWriteStream::UringWriteStream(std::shared_ptr<UringEngine> eng, int fd, uint64_t off,
                                   std::optional<uint64_t> expected_len)
    : off_(off) {
    // +2: one block being filled by the caller and one held back for the linked-fsync
    // finish, on top of write_depth in flight
    unsigned depth = eng->options().write_depth + 2;
    int dfd = ::dup(fd);
    if (dfd < 0)
        throw S3Error(S3ErrorCode::InternalError,
                      std::string("dup write fd: ") + std::strerror(errno));
    st_ = StreamState::make(std::move(eng), dfd, /*own_fd=*/true, depth,
                            /*with_fsync_slot=*/true, expected_len);
    st_->free_.reserve(depth);
    for (int i = int(depth) - 1; i >= 0; --i) st_->free_.push_back(i);
}

UringWriteStream::~UringWriteStream() { st_->unref(); }

Task<void> UringWriteStream::settle_oldest() {
    StreamState& st = *st_;
    int idx = st.wq.front();
    Slot& s = st.slots[size_t(idx)];
    for (;;) {
        int r = co_await s.wait();
        if (r < 0) uring_detail::throw_uring("io_uring write", r);
        if (r == 0) throw S3Error(S3ErrorCode::InternalError, "io_uring write returned 0");
        if (unsigned(r) < s.len) {
            // The kernel may short-write; resubmit the remainder from the same block
            s.off += unsigned(r);
            s.mem_off += unsigned(r);
            s.len -= unsigned(r);
            s.sync.store(kIdle, std::memory_order_relaxed);
            st.submit_rw(s, /*write=*/true);
            continue;
        }
        break;
    }
    s.sync.store(kIdle, std::memory_order_relaxed);
    s.mem_off = 0;
    st.wq.pop_front();
    st.free_.push_back(idx);
}

Task<std::span<std::byte>> UringWriteStream::acquire() {
    StreamState& st = *st_;
    if (st.free_.empty()) co_await settle_oldest();
    held_ = st.free_.back();
    st.free_.pop_back();
    co_return st.slots[size_t(held_)].mem;
}

void UringWriteStream::commit(size_t n) {
    StreamState& st = *st_;
    int idx = held_;
    held_ = -1;
    if (n == 0) {
        st.free_.push_back(idx);
        return;
    }
    Slot& s = st.slots[size_t(idx)];
    s.off = off_;
    s.len = unsigned(n);
    s.mem_off = 0;
    off_ += n;
    written_ += n;
    // Hold-back: the freshly committed block is pushed only when the next one arrives (or
    // by finish(), linked to the fsync). The previous pending block goes out now and
    // overlaps with receiving the next body block
    if (pending_ >= 0) {
        st.submit_rw(st.slots[size_t(pending_)], /*write=*/true);
        st.wq.push_back(pending_);
    }
    pending_ = idx;
}

Task<void> UringWriteStream::finish(bool fdatasync) {
    finished_ = true;
    StreamState& st = *st_;
    if (held_ >= 0) {  // acquired but never committed
        st.free_.push_back(held_);
        held_ = -1;
    }
    while (!st.wq.empty()) co_await settle_oldest();

    const UringFeatures& feat = st.eng->features();
    bool need_fsync = fdatasync;
    if (pending_ >= 0) {
        Slot& w = st.slots[size_t(pending_)];
        pending_ = -1;
        if (need_fsync && feat.op_fsync && feat.links) {
            Slot& f = st.slots[size_t(st.fsync_slot)];
            st.submit_write_fsync_chain(w, f);
            bool wrote_short = false;
            for (;;) {
                int r = co_await w.wait();
                if (r < 0) uring_detail::throw_uring("io_uring write", r);
                if (r == 0)
                    throw S3Error(S3ErrorCode::InternalError, "io_uring write returned 0");
                if (unsigned(r) < w.len) {
                    // A short write fails the link: the fsync completes with -ECANCELED
                    // and is retried standalone below
                    wrote_short = true;
                    w.off += unsigned(r);
                    w.mem_off += unsigned(r);
                    w.len -= unsigned(r);
                    w.sync.store(kIdle, std::memory_order_relaxed);
                    st.submit_rw(w, /*write=*/true);
                    continue;
                }
                break;
            }
            w.sync.store(kIdle, std::memory_order_relaxed);
            w.mem_off = 0;
            int fr = co_await f.wait();
            f.sync.store(kIdle, std::memory_order_relaxed);
            if (fr == -ECANCELED || wrote_short) {
                st.submit_one(f, st.make_fsync(f));
                fr = co_await f.wait();
                f.sync.store(kIdle, std::memory_order_relaxed);
            }
            if (fr < 0 && fr != -EINVAL)  // EINVAL: fs unsupported, same as fsync_file
                uring_detail::throw_uring("io_uring fdatasync", fr);
            co_return;
        }
        st.submit_rw(w, /*write=*/true);
        st.wq.push_back(int(&w - st.slots.data()));
        while (!st.wq.empty()) co_await settle_oldest();
    }
    if (!need_fsync) co_return;
    if (feat.op_fsync) {
        Slot& f = st.slots[size_t(st.fsync_slot)];
        st.submit_one(f, st.make_fsync(f));
        int fr = co_await f.wait();
        f.sync.store(kIdle, std::memory_order_relaxed);
        if (fr < 0 && fr != -EINVAL) uring_detail::throw_uring("io_uring fdatasync", fr);
    } else {
        // Pre-5.1-baseline gap: no FSYNC opcode probed. finish() resumed on a pool thread
        // (continuations post there), so a blocking fdatasync is acceptable here
        if (::fdatasync(st.fd) != 0 && errno != EINVAL)
            throw S3Error(S3ErrorCode::InternalError,
                          std::string("fdatasync: ") + std::strerror(errno));
    }
}

}  // namespace lights3::storage
