// L3: minimal io_uring wrapper over raw syscalls (no liburing dependency; dependency policy
// in docs/architecture.md §6). Single ring: a submit-side mutex serializes SQE filling, and
// io_uring_enter is batched on behalf of everyone by the "on-duty flusher"
// (docs/gaps.md §6.3); a dedicated reaper thread waits for CQEs and, on completion, posts the
// coroutine continuation to the thread pool for resumption (so subsequent synchronous
// persistence calls naturally run on pool threads).
#pragma once

#include <linux/io_uring.h>
#include <sys/uio.h>

#include <condition_variable>
#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <string>
#include <thread>
#include <unordered_set>

#include "core/thread_pool.h"

namespace lights3::storage {

struct UringOptions {
    unsigned entries = 256;  // SQ depth
    // SQPOLL (docs/gaps.md §6.3): the kernel polls the SQ, so in the common case submission
    // never enters the kernel (only a wakeup enter after the poll thread has gone to sleep).
    // The cost is a resident kernel thread, and before 5.11 it needs CAP_SYS_ADMIN -- on
    // setup failure it automatically falls back to normal mode rather than preventing the
    // process from starting
    bool sqpoll = false;
    int sqpoll_idle_ms = 100;
};

// Kernel capability probe results (docs/gaps.md §6.3): the previous implementation used
// IORING_OP_READ/WRITE unconditionally, but those opcodes only exist since 5.6 -- on
// 5.1-5.5 kernels every IO would get -EINVAL, presenting as "io_uring sets up fine but all
// reads/writes fail". After probing, old kernels fall back to READV/WRITEV (single iovec)
struct UringFeatures {
    uint32_t setup_features = 0;  // io_uring_params.features bitmap
    bool op_read_write = false;   // IORING_OP_READ / IORING_OP_WRITE (5.6+)
    bool op_fsync = false;        // IORING_OP_FSYNC (5.1+, still probed explicitly)
    bool sqpoll = false;          // actually enabled (false if requested but setup failed)
    bool probed = false;          // IORING_REGISTER_PROBE available; else assume conservative 5.1 baseline

    std::string describe() const;
};

class UringEngine {
public:
    explicit UringEngine(std::shared_ptr<ThreadPool> pool, UringOptions opt = {});
    ~UringEngine();
    UringEngine(const UringEngine&) = delete;

    // Stop the reaper thread (idempotent); no submissions allowed afterwards. First reject
    // new submissions and wait for in-flight CQEs to drain (with a timeout warning) before
    // posting the sentinel -- CQE ordering does not guarantee the sentinel comes after the
    // existing reads/writes, and munmap-ing without draining would let the kernel keep
    // writing into freed user buffers (docs/gaps.md §2.9)
    void shutdown();

    const UringFeatures& features() const { return feat_; }

    struct Op {
        std::coroutine_handle<> h;
        int res = 0;  // cqe.res: bytes read/written, <0 means -errno
    };

    // co_await returns cqe.res; on submit failure (unrecoverable enter error) the exception
    // is thrown from the co_await site and the coroutine is treated as never suspended
    // (other Ops piggybacked in the same batch are woken with -EIO -- with batched
    // submission there is no way to roll back only one's own entry)
    struct Awaitable {
        UringEngine& eng;
        uint8_t opcode;
        int fd;
        const void* addr;
        unsigned len;
        uint64_t off;
        // iovec for the READV/WRITEV fallback path: lives in the coroutine frame with the
        // Awaitable, so it stays valid across suspension
        struct iovec iov {};
        Op op;

        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            op.h = h;
            const void* a = addr;
            unsigned l = len;
            if (opcode == IORING_OP_READV || opcode == IORING_OP_WRITEV) {
                iov.iov_base = const_cast<void*>(addr);
                iov.iov_len = len;
                a = &iov;
                l = 1;
            }
            eng.submit(opcode, fd, a, l, off, &op);
        }
        int await_resume() const noexcept { return op.res; }
    };

    Awaitable read(int fd, std::span<std::byte> buf, uint64_t off) {
        return {*this, feat_.op_read_write ? uint8_t(IORING_OP_READ) : uint8_t(IORING_OP_READV),
                fd, buf.data(), static_cast<unsigned>(buf.size()), off, {}, {}};
    }
    Awaitable write(int fd, std::span<const std::byte> buf, uint64_t off) {
        return {*this,
                feat_.op_read_write ? uint8_t(IORING_OP_WRITE) : uint8_t(IORING_OP_WRITEV), fd,
                buf.data(), static_cast<unsigned>(buf.size()), off, {}, {}};
    }
    // fsync_flags=IORING_FSYNC_DATASYNC gives fdatasync semantics (the write path only
    // needs data durability)
    Awaitable fdatasync(int fd) {
        Awaitable a{*this, IORING_OP_FSYNC, fd, nullptr, IORING_FSYNC_DATASYNC, 0, {}, {}};
        return a;
    }

private:
    // user_data=0 is reserved for shutdown's NOP sentinel; op must not be null
    void submit(uint8_t opcode, int fd, const void* addr, unsigned len, uint64_t off, Op* op);
    // Only fill the SQE and advance the tail (submit_mu_ must be held; the caller has already
    // confirmed the SQ has room)
    void push_sqe_locked(uint8_t opcode, int fd, const void* addr, unsigned len, uint64_t off,
                         uint64_t user_data);
    // Hand [submitted_, sq_tail_) to the kernel (batched submission, docs/gaps.md §6.3).
    // Callers other than the on-duty flusher return immediately -- their SQEs are carried
    // along by the one on duty, saving their own io_uring_enter. Returns 0 = success;
    // >0 = unrecoverable errno (at that point failed_ is set and all in-flight Ops except
    // self have been woken with -EIO)
    int flush_locked(std::unique_lock<std::mutex>& lk, Op* self);
    // Set the error state and remove all in-flight Ops (except `except`, which the caller
    // notifies via exception); lock must be held, and the caller resumes the returned
    // handles outside the lock
    std::vector<Op*> fail_all_locked(Op* except);
    void probe_features(const io_uring_params& p);
    void reap_loop();
    void unmap_rings();

    int ring_fd_ = -1;
    std::shared_ptr<ThreadPool> pool_;
    UringFeatures feat_;

    // SQ (submit side, protected by submit_mu_)
    std::mutex submit_mu_;
    bool stopped_ = false;
    bool failed_ = false;  // reaper/submit hit an unrecoverable error; all later submissions rejected
    bool flushing_ = false;         // a thread is already running io_uring_enter (on-duty marker for batched submission)
    unsigned submitted_ = 0;        // count of SQEs handed to the kernel (same sequence as sq_tail_)
    std::condition_variable sq_cv_;  // SQ full / flusher progressed
    // In-flight op registry (protected by submit_mu_): when the reaper thread fails, wake all
    // in-flight coroutines with -EIO (otherwise GETs hang forever and connections are never
    // released); shutdown relies on it to wait for drain
    std::unordered_set<Op*> inflight_;
    std::condition_variable inflight_cv_;
    unsigned sq_entries_ = 0;
    unsigned sq_mask_ = 0;
    unsigned* sq_head_ = nullptr;
    unsigned* sq_tail_ = nullptr;
    unsigned* sq_flags_ = nullptr;  // IORING_SQ_NEED_WAKEUP for SQPOLL
    unsigned* sq_array_ = nullptr;
    io_uring_sqe* sqes_ = nullptr;

    // CQ (consumed only by the reaper thread)
    unsigned cq_mask_ = 0;
    unsigned* cq_head_ = nullptr;
    unsigned* cq_tail_ = nullptr;
    io_uring_cqe* cqes_ = nullptr;

    void* sq_ring_ptr_ = nullptr;
    size_t sq_ring_bytes_ = 0;
    void* cq_ring_ptr_ = nullptr;  // same as sq_ring_ptr_ under FEAT_SINGLE_MMAP
    size_t cq_ring_bytes_ = 0;
    size_t sqes_bytes_ = 0;

    std::thread reaper_;
};

}  // namespace lights3::storage
