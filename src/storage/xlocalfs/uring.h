// L3: minimal io_uring wrapper over raw syscalls (no liburing dependency; dependency policy
// in docs/architecture.md §6). One or more independent rings (roadmap §3.4 ④): each ring
// has its own SQ/CQ, submit-side mutex, in-flight registry and reaper thread; single ops
// are spread round-robin, multi-op streams (uring_stream.h) pin themselves to one ring
// because fixed buffer / fixed file indices are ring-scoped. Per ring, io_uring_enter is
// batched on behalf of everyone by the "on-duty flusher" (docs/archive/gaps.md §6.3); the
// reaper waits for CQEs and hands each one to its Op's completion sink -- a plain co_await
// resumes the coroutine on the thread pool, a stream slot records the result and wakes its
// consumer if one is parked on it.
#pragma once

#include <fcntl.h>
#include <linux/io_uring.h>
#include <sys/stat.h>
#include <sys/uio.h>

#include <atomic>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string>
#include <vector>

#include "core/thread_pool.h"

namespace lights3::storage {

struct UringOptions {
    unsigned entries = 256;  // SQ depth per ring
    // SQPOLL (docs/archive/gaps.md §6.3): the kernel polls the SQ, so in the common case submission
    // never enters the kernel (only a wakeup enter after the poll thread has gone to sleep).
    // The cost is a resident kernel thread, and before 5.11 it needs CAP_SYS_ADMIN -- on
    // setup failure it automatically falls back to normal mode rather than preventing the
    // process from starting
    bool sqpoll = false;
    int sqpoll_idle_ms = 100;
    // Ring sharding (roadmap §3.4 ④): a single ring means one submit mutex and one reaper
    // for the whole process, a single point on high core counts. 0 = auto
    // (hardware threads / 8, clamped to [1, 8])
    unsigned rings = 1;
    // Stream block size: the unit of read-ahead / pipelined writes, and the size of each
    // registered fixed buffer. Must be a multiple of 4096
    unsigned block_size = 64 * 1024;
    // Registered buffers per ring (IORING_REGISTER_BUFFERS, roadmap §3.4 ②): streams take
    // their blocks from this pool and use READ_FIXED/WRITE_FIXED (no per-IO page pinning);
    // when the pool is empty a stream silently falls back to heap blocks + READ/WRITE.
    // 0 = disabled. Memory: rings × fixed_buffers × block_size, resident and pinned
    unsigned fixed_buffers = 64;
    // Registered file slots per ring (sparse IORING_REGISTER_FILES + FILES_UPDATE): large
    // streams register their fd once and skip the per-IO fget/fput. 0 = disabled
    unsigned fixed_files = 256;
    // Blocks a read stream keeps in flight ahead of the consumer (roadmap §3.4 ①)
    unsigned read_depth = 4;
    // Writes a write stream keeps in flight while the next body block is being received
    unsigned write_depth = 4;
    // Route open/statx/rename/unlink through the ring when the kernel has the opcodes
    // (roadmap §3.4 ③); false = always the blocking syscall on the pool thread
    bool meta_ops = true;
};

// Kernel capability probe results (docs/archive/gaps.md §6.3): the previous implementation used
// IORING_OP_READ/WRITE unconditionally, but those opcodes only exist since 5.6 -- on
// 5.1-5.5 kernels every IO would get -EINVAL, presenting as "io_uring sets up fine but all
// reads/writes fail". After probing, old kernels fall back to READV/WRITEV (single iovec)
struct UringFeatures {
    uint32_t setup_features = 0;  // io_uring_params.features bitmap
    bool op_read_write = false;   // IORING_OP_READ / IORING_OP_WRITE (5.6+)
    bool op_fsync = false;        // IORING_OP_FSYNC (5.1+, still probed explicitly)
    bool op_fixed_rw = false;     // IORING_OP_READ_FIXED / WRITE_FIXED (5.1+, probed)
    bool op_openat = false;       // IORING_OP_OPENAT (5.6+)
    bool op_statx = false;        // IORING_OP_STATX (5.6+)
    bool op_renameat = false;     // IORING_OP_RENAMEAT (5.11+)
    bool op_unlinkat = false;     // IORING_OP_UNLINKAT (5.11+)
    bool links = false;           // IOSQE_IO_LINK usable (5.3+; tied to the probe = 5.6+)
    bool sqpoll = false;          // actually enabled (false if requested but setup failed)
    bool probed = false;          // IORING_REGISTER_PROBE available; else assume conservative 5.1 baseline
    bool fixed_buffers = false;   // IORING_REGISTER_BUFFERS succeeded on every ring
    bool fixed_files = false;     // sparse IORING_REGISTER_FILES succeeded on every ring
    unsigned rings = 1;

    std::string describe() const;
};

class UringRing;

class UringEngine {
public:
    explicit UringEngine(std::shared_ptr<ThreadPool> pool, UringOptions opt = {});
    ~UringEngine();
    UringEngine(const UringEngine&) = delete;

    // Stop every ring's reaper thread (idempotent); no submissions allowed afterwards.
    // Per ring: first reject new submissions and wait for in-flight CQEs to drain (with a
    // timeout warning) before posting the sentinel -- CQE ordering does not guarantee the
    // sentinel comes after the existing reads/writes, and munmap-ing without draining would
    // let the kernel keep writing into freed user buffers (docs/archive/gaps.md §2.9)
    void shutdown();

    const UringFeatures& features() const { return feat_; }
    const UringOptions& options() const { return opt_; }
    ThreadPool& pool() { return *pool_; }
    const std::shared_ptr<ThreadPool>& pool_ptr() const { return pool_; }

    // Completion sink of one SQE: invoked exactly once by the reaper (or by the failure
    // path with -EIO), outside every engine lock. Implementations must be noexcept and may
    // destroy themselves; the engine never touches an Op after complete() returns
    struct Op {
        virtual void complete(int res) noexcept = 0;

    protected:
        ~Op() = default;
    };

    // Plain SQE description; the fields map 1:1 onto io_uring_sqe. `off` doubles as addr2
    // (renameat's new path, statx's output struct); `op_flags` is the per-opcode flags
    // union (fsync_flags / open_flags / statx_flags / rename_flags / unlink_flags)
    struct Sqe {
        uint8_t opcode = IORING_OP_NOP;
        uint8_t flags = 0;  // IOSQE_IO_LINK / IOSQE_FIXED_FILE
        int fd = -1;
        uint64_t addr = 0;
        uint32_t len = 0;
        uint64_t off = 0;
        uint32_t op_flags = 0;
        uint16_t buf_index = 0;
    };

    unsigned ring_count() const { return feat_.rings; }
    unsigned pick_ring() { return rr_.fetch_add(1, std::memory_order_relaxed) % feat_.rings; }

    // Submit a chain of SQEs to one ring; ops[i] receives SQE i's completion. The chain is
    // pushed contiguously under the ring's submit lock (a link must never straddle a
    // submission boundary). Throws S3Error(InternalError) when the ring is stopped/failed
    // or io_uring_enter fails fatally -- none of `ops` is registered in that case, so the
    // caller treats them as never submitted (other in-flight Ops piggybacked in the same
    // batch are woken with -EIO: with batched submission there is no rolling back only
    // one's own entry)
    void submit(unsigned ring, std::span<const Sqe> chain, std::span<Op* const> ops);

    // Coroutine-resuming Op: co_await returns cqe.res; on submit failure the exception is
    // thrown from the co_await site and the coroutine is treated as never suspended
    struct CoroOp final : Op {
        UringEngine* eng = nullptr;
        std::coroutine_handle<> h;
        int res = 0;
        void complete(int r) noexcept override;
    };
    struct Awaitable {
        UringEngine& eng;
        Sqe sqe;
        int ring = -1;  // <0 = pick round-robin at suspension
        // iovec for the READV/WRITEV fallback path: lives in the coroutine frame with the
        // Awaitable, so it stays valid across suspension
        struct iovec iov {};
        CoroOp op;

        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h);
        int await_resume() const noexcept { return op.res; }
    };

    Awaitable read(int fd, std::span<std::byte> buf, uint64_t off);
    Awaitable write(int fd, std::span<const std::byte> buf, uint64_t off);
    // fsync_flags=IORING_FSYNC_DATASYNC gives fdatasync semantics (the write path only
    // needs data durability); fsync() is the full variant for directory fds
    Awaitable fdatasync(int fd);
    Awaitable fsync(int fd);
    // Metadata opcodes (roadmap §3.4 ③). Path memory must stay valid until the CQE arrives
    // (under SQPOLL the kernel copies it only when the poll thread picks the SQE up), so
    // callers keep the std::string / fs::path alive across the co_await. Results follow
    // the syscall: openat returns the fd, the rest 0 / -errno. Callers check the matching
    // features().op_* first; without it they run the blocking syscall on the pool thread
    Awaitable openat(int dirfd, const char* path, int flags, mode_t mode);
    Awaitable statx(int dirfd, const char* path, int flags, unsigned mask, struct ::statx* out);
    Awaitable renameat(int olddirfd, const char* oldpath, int newdirfd, const char* newpath);
    Awaitable unlinkat(int dirfd, const char* path, int flags);
    // Arbitrary SQE on a chosen ring (tests, streams)
    Awaitable raw(unsigned ring, Sqe sqe) { return {*this, sqe, int(ring), {}, {}}; }

    // ---- Fixed buffers (ring-scoped) ----
    struct FixedBuf {
        unsigned ring = 0;
        int index = -1;  // <0 = none
        std::span<std::byte> mem;
    };
    // Non-blocking; false when the ring has no registered buffers or the pool is exhausted
    bool try_acquire_fixed(unsigned ring, FixedBuf& out);
    void release_fixed(const FixedBuf& b);
    unsigned fixed_free(unsigned ring) const;

    // ---- Fixed files (ring-scoped) ----
    // Returns the table slot (use IOSQE_FIXED_FILE with sqe.fd = slot) or -1 when fixed
    // files are unsupported, the table is full, or the update syscall failed. The slot must
    // stay registered until every op referencing it has completed
    int register_file(unsigned ring, int fd);
    void unregister_file(unsigned ring, int slot);

private:
    std::shared_ptr<ThreadPool> pool_;
    UringOptions opt_;
    UringFeatures feat_;
    std::vector<std::unique_ptr<UringRing>> rings_;
    std::atomic<unsigned> rr_{0};
};

}  // namespace lights3::storage
