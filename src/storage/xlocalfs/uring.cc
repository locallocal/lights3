#include "storage/xlocalfs/uring.h"

#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "core/log.h"
#include "s3/errors.h"

// The happens-before from submit thread -> kernel -> reaper thread flows through syscalls
// and ring barriers, which TSan cannot observe, so it would falsely report every reaper-side
// access to Op as a race; add an annotation edge on the Op pointer (release at submit,
// acquire at reap) to model this synchronization precisely
#if defined(__SANITIZE_THREAD__)
#include <sanitizer/tsan_interface.h>
#define LIGHTS3_TSAN_RELEASE(p) __tsan_release(p)
#define LIGHTS3_TSAN_ACQUIRE(p) __tsan_acquire(p)
#else
#define LIGHTS3_TSAN_RELEASE(p) (void)(p)
#define LIGHTS3_TSAN_ACQUIRE(p) (void)(p)
#endif

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

int sys_io_uring_setup(unsigned entries, io_uring_params* p) {
    return static_cast<int>(::syscall(__NR_io_uring_setup, entries, p));
}

int sys_io_uring_enter(int fd, unsigned to_submit, unsigned min_complete, unsigned flags) {
    return static_cast<int>(
        ::syscall(__NR_io_uring_enter, fd, to_submit, min_complete, flags, nullptr, 0));
}

int sys_io_uring_register(int fd, unsigned op, void* arg, unsigned nr) {
    return static_cast<int>(::syscall(__NR_io_uring_register, fd, op, arg, nr));
}

// The ring head/tail pointers live on pages mapped shared with the kernel and must be
// read/written with paired atomics
unsigned load_acquire(const unsigned* p) { return __atomic_load_n(p, __ATOMIC_ACQUIRE); }
void store_release(unsigned* p, unsigned v) { __atomic_store_n(p, v, __ATOMIC_RELEASE); }

void* ring_mmap(int fd, size_t bytes, uint64_t offset) {
    void* p = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd,
                     static_cast<off_t>(offset));
    return p == MAP_FAILED ? nullptr : p;
}

// Retryable signals from enter: EINTR (signal) / EAGAIN, EBUSY (kernel memory pressure or
// full CQ). Yield for the first few rounds (the common case of transient jitter), then back
// off exponentially, sleeping up to a 1ms cap to give up the CPU
void enter_backoff(int spin) {
    if (spin < 8) std::this_thread::yield();
    else
        std::this_thread::sleep_for(
            std::chrono::microseconds(std::min(1000, 16 << std::min(spin - 8, 6))));
}

}  // namespace

std::string UringFeatures::describe() const {
    std::string s = "io_uring: ";
    s += probed ? "probe=yes" : "probe=no(assuming 5.1 baseline)";
    s += op_read_write ? ", READ/WRITE" : ", READV/WRITEV fallback";
    s += op_fsync ? ", FSYNC" : ", no FSYNC (falling back to blocking fdatasync)";
    s += sqpoll ? ", SQPOLL on" : ", SQPOLL off";
    if (setup_features & IORING_FEAT_SINGLE_MMAP) s += ", SINGLE_MMAP";
    if (setup_features & IORING_FEAT_NODROP) s += ", NODROP";
    return s;
}

UringEngine::UringEngine(std::shared_ptr<ThreadPool> pool, UringOptions opt)
    : pool_(std::move(pool)) {
    io_uring_params p{};
    if (opt.sqpoll) {
        p.flags |= IORING_SETUP_SQPOLL;
        p.sq_thread_idle = unsigned(std::max(0, opt.sqpoll_idle_ms));
    }
    ring_fd_ = sys_io_uring_setup(opt.entries, &p);
    // SQPOLL requires CAP_SYS_ADMIN before 5.11, which containers usually lack. On setup
    // failure fall back to plain mode instead of preventing the process from starting --
    // SQPOLL is a throughput optimization, not a correctness prerequisite
    if (ring_fd_ < 0 && opt.sqpoll && (errno == EPERM || errno == EINVAL)) {
        LOG_WARN("io_uring: SQPOLL setup refused ({}); falling back to plain submission",
                 std::strerror(errno));
        p = io_uring_params{};
        ring_fd_ = sys_io_uring_setup(opt.entries, &p);
    }
    if (ring_fd_ < 0)
        throw std::runtime_error(std::string("io_uring_setup: ") + std::strerror(errno));

    sq_ring_bytes_ = p.sq_off.array + p.sq_entries * sizeof(unsigned);
    cq_ring_bytes_ = p.cq_off.cqes + p.cq_entries * sizeof(io_uring_cqe);
    bool single_mmap = p.features & IORING_FEAT_SINGLE_MMAP;
    if (single_mmap) sq_ring_bytes_ = cq_ring_bytes_ = std::max(sq_ring_bytes_, cq_ring_bytes_);
    sqes_bytes_ = p.sq_entries * sizeof(io_uring_sqe);

    sq_ring_ptr_ = ring_mmap(ring_fd_, sq_ring_bytes_, IORING_OFF_SQ_RING);
    cq_ring_ptr_ = !sq_ring_ptr_ ? nullptr
                   : single_mmap ? sq_ring_ptr_
                                 : ring_mmap(ring_fd_, cq_ring_bytes_, IORING_OFF_CQ_RING);
    sqes_ = cq_ring_ptr_ ? static_cast<io_uring_sqe*>(
                               ring_mmap(ring_fd_, sqes_bytes_, IORING_OFF_SQES))
                         : nullptr;
    if (!sqes_) {
        int saved = errno;
        unmap_rings();
        ::close(ring_fd_);
        throw std::runtime_error(std::string("io_uring mmap: ") + std::strerror(saved));
    }

    auto* sq = static_cast<uint8_t*>(sq_ring_ptr_);
    sq_head_ = reinterpret_cast<unsigned*>(sq + p.sq_off.head);
    sq_tail_ = reinterpret_cast<unsigned*>(sq + p.sq_off.tail);
    sq_flags_ = reinterpret_cast<unsigned*>(sq + p.sq_off.flags);
    sq_mask_ = *reinterpret_cast<unsigned*>(sq + p.sq_off.ring_mask);
    sq_array_ = reinterpret_cast<unsigned*>(sq + p.sq_off.array);
    sq_entries_ = p.sq_entries;
    submitted_ = *sq_tail_;

    auto* cq = static_cast<uint8_t*>(cq_ring_ptr_);
    cq_head_ = reinterpret_cast<unsigned*>(cq + p.cq_off.head);
    cq_tail_ = reinterpret_cast<unsigned*>(cq + p.cq_off.tail);
    cq_mask_ = *reinterpret_cast<unsigned*>(cq + p.cq_off.ring_mask);
    cqes_ = reinterpret_cast<io_uring_cqe*>(cq + p.cq_off.cqes);

    feat_.sqpoll = (p.flags & IORING_SETUP_SQPOLL) != 0;
    probe_features(p);
    LOG_INFO("xlocalfs {} (sq_entries={})", feat_.describe(), sq_entries_);

    reaper_ = std::thread([this] { reap_loop(); });
}

// Capability probing (docs/gaps.md §6.3): IORING_REGISTER_PROBE is available since 5.6,
// exactly the version where IORING_OP_READ/WRITE landed -- probe failure is treated as
// 5.1-5.5 and falls back to READV/WRITEV. Probe honestly instead of "try once and check for
// -EINVAL": that failure would land on some real request
void UringEngine::probe_features(const io_uring_params& p) {
    feat_.setup_features = p.features;
// IORING_REGISTER_PROBE is an enum constant, not a macro, so it cannot be tested;
// IO_URING_OP_SUPPORTED is a macro and entered the headers in the same batch (5.6) as
// io_uring_probe, so use it as the existence check
#ifdef IO_URING_OP_SUPPORTED
    // io_uring_probe is a variable-length header + ops[]; one call must cover up to IORING_OP_WRITE(23)
    constexpr unsigned kNrOps = 64;
    std::vector<uint8_t> buf(sizeof(io_uring_probe) + kNrOps * sizeof(io_uring_probe_op), 0);
    auto* probe = reinterpret_cast<io_uring_probe*>(buf.data());
    if (sys_io_uring_register(ring_fd_, IORING_REGISTER_PROBE, probe, kNrOps) == 0) {
        feat_.probed = true;
        auto supported = [&](unsigned op) {
            return op < probe->ops_len && (probe->ops[op].flags & IO_URING_OP_SUPPORTED);
        };
        feat_.op_read_write = supported(IORING_OP_READ) && supported(IORING_OP_WRITE);
        feat_.op_fsync = supported(IORING_OP_FSYNC);
        return;
    }
#endif
    // 5.1 baseline: READV/WRITEV/FSYNC/NOP are guaranteed present, READ/WRITE guaranteed absent
    feat_.probed = false;
    feat_.op_read_write = false;
    feat_.op_fsync = true;
}

UringEngine::~UringEngine() {
    shutdown();
    unmap_rings();
    if (ring_fd_ >= 0) ::close(ring_fd_);
}

void UringEngine::unmap_rings() {
    if (sqes_) ::munmap(sqes_, sqes_bytes_);
    if (cq_ring_ptr_ && cq_ring_ptr_ != sq_ring_ptr_) ::munmap(cq_ring_ptr_, cq_ring_bytes_);
    if (sq_ring_ptr_) ::munmap(sq_ring_ptr_, sq_ring_bytes_);
    sqes_ = nullptr;
    cq_ring_ptr_ = nullptr;
    sq_ring_ptr_ = nullptr;
}

void UringEngine::push_sqe_locked(uint8_t opcode, int fd, const void* addr, unsigned len,
                                  uint64_t off, uint64_t user_data) {
    unsigned tail = *sq_tail_;  // sole writer under the lock, a plain read suffices
    unsigned idx = tail & sq_mask_;
    io_uring_sqe& sqe = sqes_[idx];
    std::memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = opcode;
    sqe.fd = fd;
    sqe.addr = reinterpret_cast<uint64_t>(addr);
    sqe.off = off;
    sqe.user_data = user_data;
    // FSYNC passes DATASYNC via fsync_flags rather than len (the two are at different
    // offsets in the sqe union)
    if (opcode == IORING_OP_FSYNC) sqe.fsync_flags = len;
    else sqe.len = len;
    sq_array_[idx] = idx;
    store_release(sq_tail_, tail + 1);
}

int UringEngine::flush_locked(std::unique_lock<std::mutex>& lk, Op* self) {
    // A thread is already on duty: our SQEs fall inside its submission window and will be
    // carried along. This is where batching comes from -- under high concurrency N SQEs
    // converge into one io_uring_enter instead of N
    if (flushing_) return 0;
    flushing_ = true;
    int spin = 0;
    int fatal = 0;
    while (submitted_ != *sq_tail_) {
        unsigned n = *sq_tail_ - submitted_;
        // SQPOLL: the kernel thread fetches on its own; only when it has gone to sleep is a
        // wakeup enter needed
        if (feat_.sqpoll) {
            if (!(load_acquire(sq_flags_) & IORING_SQ_NEED_WAKEUP)) {
                submitted_ = *sq_tail_;
                break;
            }
            lk.unlock();
            int ret = sys_io_uring_enter(ring_fd_, n, 0, IORING_ENTER_SQ_WAKEUP);
            int err = errno;
            lk.lock();
            if (ret >= 0) {
                submitted_ = *sq_tail_;  // once woken, the poll thread takes everything
                break;
            }
            if (err == EINTR || err == EAGAIN || err == EBUSY) {
                enter_backoff(spin++);
                continue;
            }
            fatal = err;
            break;
        }
        lk.unlock();
        int ret = sys_io_uring_enter(ring_fd_, n, 0, 0);
        int err = errno;
        lk.lock();
        if (ret > 0) {
            submitted_ += unsigned(ret);
            spin = 0;
            sq_cv_.notify_all();  // SQ slots freed up
            continue;
        }
        if (ret == 0 || err == EINTR || err == EAGAIN || err == EBUSY) {
            enter_backoff(spin++);
            continue;
        }
        fatal = err;
        break;
    }
    flushing_ = false;
    sq_cv_.notify_all();
    if (!fatal) return 0;
    // With batched submission there is no way to roll back only one's own entry: Ops
    // piggybacked in the same batch are long suspended and their submit() has already
    // returned. A ring that got here (EBADF/EFAULT/ENXIO etc.) is unusable -- set the error
    // state, wake all in-flight with -EIO; the caller notifies self via exception
    auto orphans = fail_all_locked(self);
    lk.unlock();
    LOG_ERROR("io_uring submit failed fatally: {}; failing {} in-flight op(s) with EIO",
              std::strerror(fatal), orphans.size());
    for (Op* op : orphans) {
        op->res = -EIO;
        pool_->post([h = op->h] { h.resume(); });
    }
    lk.lock();
    inflight_cv_.notify_all();
    return fatal;
}

std::vector<UringEngine::Op*> UringEngine::fail_all_locked(Op* except) {
    failed_ = true;
    std::vector<Op*> orphans;
    orphans.reserve(inflight_.size());
    for (Op* op : inflight_)
        if (op != except) orphans.push_back(op);
    inflight_.clear();
    return orphans;
}

void UringEngine::submit(uint8_t opcode, int fd, const void* addr, unsigned len, uint64_t off,
                         Op* op) {
    std::unique_lock lk(submit_mu_);
    for (;;) {
        if (stopped_ || failed_)
            throw S3Error(S3ErrorCode::InternalError,
                          failed_ ? "io_uring engine failed" : "io_uring engine stopped");
        if (*sq_tail_ - load_acquire(sq_head_) < sq_entries_) break;
        // SQ full = the flusher lags behind filling. If we are not on duty, give it a push
        // ourselves; otherwise wait for the on-duty one to make progress (it will either
        // progress or set failed_, and both notify)
        if (!flushing_) {
            if (int err = flush_locked(lk, nullptr))
                throw S3Error(S3ErrorCode::InternalError,
                              std::string("io_uring_enter: ") + std::strerror(err));
            continue;
        }
        sq_cv_.wait(lk);
    }
    inflight_.insert(op);
    LIGHTS3_TSAN_RELEASE(op);
    push_sqe_locked(opcode, fd, addr, len, off, reinterpret_cast<uint64_t>(op));
    if (int err = flush_locked(lk, op)) {
        inflight_.erase(op);  // coroutine treated as never suspended; other in-flight already woken with -EIO
        throw S3Error(S3ErrorCode::InternalError,
                      std::string("io_uring_enter: ") + std::strerror(err));
    }
}

void UringEngine::reap_loop() {
    for (;;) {
        unsigned head = *cq_head_;  // sole consumer, a plain read suffices
        unsigned tail = load_acquire(cq_tail_);
        bool stop = false;
        while (head != tail) {
            const io_uring_cqe& cqe = cqes_[head & cq_mask_];
            if (cqe.user_data == 0) {
                stop = true;  // NOP sentinel submitted by shutdown
            } else {
                Op* op = reinterpret_cast<Op*>(static_cast<uintptr_t>(cqe.user_data));
                bool ours, drained;
                {
                    // Before touching *op, confirm in the registry that it is still in
                    // flight: if fail_all_locked has already woken this Op with -EIO, its
                    // coroutine frame (Op included) was destroyed during unwinding, and this
                    // late CQE can only be deregistered and skipped -- writing res or
                    // resuming a second time is a UAF
                    std::lock_guard lk(submit_mu_);
                    ours = inflight_.erase(op) > 0;
                    drained = stopped_ && inflight_.empty();
                }
                if (drained) inflight_cv_.notify_all();
                if (ours) {
                    LIGHTS3_TSAN_ACQUIRE(op);
                    op->res = cqe.res;
                    // Resume via the thread pool; post's internal lock makes the res write
                    // visible to the resuming thread
                    pool_->post([h = op->h] { h.resume(); });
                }
            }
            ++head;
        }
        store_release(cq_head_, head);
        if (stop) return;
        {
            // Once the engine is poisoned (fatal errno in flush_locked), stop blocking on
            // CQEs: all in-flight Ops have been woken with -EIO, so here we only poll to
            // digest the kernel's late CQEs (skipped per registry above) and exit after
            // shutdown sets stopped_ -- a blocking GETEVENTS with no further CQEs would make
            // shutdown's join wait forever
            std::unique_lock lk(submit_mu_);
            if (failed_) {
                if (stopped_) return;
                lk.unlock();
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
                continue;
            }
        }
        int ret = sys_io_uring_enter(ring_fd_, 0, 1, IORING_ENTER_GETEVENTS);
        if (ret < 0 && errno != EINTR && errno != EAGAIN && errno != EBUSY) {
            // Unrecoverable error: exiting silently would leave every in-flight co_await
            // never resumed (GETs hang, connections never released, frames leak) while
            // submit() keeps accepting new work as usual. Set the error state to reject new
            // submissions and wake all in-flight Ops with -EIO. Note the kernel could in
            // theory still complete these IOs and write into user buffers -- but any errno
            // that got us here (EBADF/EFAULT/ENXIO) means the ring is unusable, and of the
            // two evils, informing the callers is the lesser
            int err = errno;
            std::vector<Op*> orphans;
            {
                std::lock_guard lk(submit_mu_);
                orphans = fail_all_locked(nullptr);
            }
            LOG_ERROR("io_uring reaper exiting on fatal error: {}; failing {} in-flight op(s) "
                      "with EIO",
                      std::strerror(err), orphans.size());
            for (Op* op : orphans) {
                op->res = -EIO;
                pool_->post([h = op->h] { h.resume(); });
            }
            sq_cv_.notify_all();
            inflight_cv_.notify_all();
            return;
        }
    }
}

void UringEngine::shutdown() {
    std::unique_lock lk(submit_mu_);
    if (stopped_) {
        lk.unlock();
        if (reaper_.joinable()) reaper_.join();
        return;
    }
    stopped_ = true;  // reject new submissions first; only then can draining mean anything
    sq_cv_.notify_all();
    if (!failed_) {
        // Wait for in-flight CQEs to reach zero before posting the sentinel: CQE ordering
        // does not guarantee the sentinel comes after the existing reads/writes, and
        // destructing/munmap-ing without draining lets the kernel keep writing into freed
        // user buffers (UAF). On timeout only warn, never deadlock process exit -- the risk
        // can no longer be eliminated at that point, so at least leave evidence
        if (!inflight_cv_.wait_for(lk, std::chrono::seconds(10),
                                   [&] { return inflight_.empty() || failed_; }))
            LOG_ERROR("io_uring shutdown: {} op(s) still in flight after 10s; proceeding",
                      inflight_.size());
    }
    if (!failed_) {
        push_sqe_locked(IORING_OP_NOP, -1, nullptr, 0, 0, /*user_data=*/0);
        flush_locked(lk, nullptr);  // on failure it already set failed_ and woke in-flight; do not rethrow here
    }
    lk.unlock();
    if (reaper_.joinable()) reaper_.join();
}

}  // namespace lights3::storage
