#include "storage/xlocalfs/uring.h"

#include <pthread.h>
#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cstring>
#include <mutex>
#include <stdexcept>
#include <string>
#include <thread>
#include <unordered_set>
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
    s += ", rings=" + std::to_string(rings);
    s += op_read_write ? ", READ/WRITE" : ", READV/WRITEV fallback";
    s += op_fsync ? ", FSYNC" : ", no FSYNC (falling back to blocking fdatasync)";
    s += fixed_buffers ? ", fixed buffers" : ", no fixed buffers";
    s += fixed_files ? ", fixed files" : ", no fixed files";
    s += links ? ", links" : ", no links";
    std::string meta;
    if (op_openat) meta += "openat ";
    if (op_statx) meta += "statx ";
    if (op_renameat) meta += "renameat ";
    if (op_unlinkat) meta += "unlinkat ";
    if (meta.empty()) meta = "none";
    else meta.pop_back();
    s += ", meta ops: " + meta;
    s += sqpoll ? ", SQPOLL on" : ", SQPOLL off";
    if (setup_features & IORING_FEAT_SINGLE_MMAP) s += ", SINGLE_MMAP";
    if (setup_features & IORING_FEAT_NODROP) s += ", NODROP";
    return s;
}

// ---------------------------------------------------------------------------
// UringRing: one SQ/CQ pair, submit lock, in-flight registry, reaper thread and the
// ring-scoped registered resources (fixed buffers, fixed file table)
// ---------------------------------------------------------------------------

class UringRing {
public:
    using Op = UringEngine::Op;
    using Sqe = UringEngine::Sqe;

    UringRing(unsigned index, ThreadPool* pool, const UringOptions& opt, UringFeatures& feat,
              bool probe);
    ~UringRing();
    UringRing(const UringRing&) = delete;

    void start_reaper();
    void shutdown();
    void submit(std::span<const Sqe> chain, std::span<Op* const> ops);

    bool try_acquire_fixed(UringEngine::FixedBuf& out);
    void release_fixed(int index);
    unsigned fixed_free() const;
    int register_file(int fd);
    void unregister_file(int slot);

    bool has_fixed_buffers() const { return fixed_arena_ != nullptr; }
    bool has_fixed_files() const { return !file_slots_.empty() || files_registered_; }

private:
    void probe_features(const io_uring_params& p, UringFeatures& feat);
    void register_resources(const UringOptions& opt, UringFeatures& feat);
    void push_sqe_locked(const Sqe& d, uint64_t user_data);
    // Hand [submitted_, sq_tail_) to the kernel (batched submission, docs/archive/gaps.md §6.3).
    // Callers other than the on-duty flusher return immediately -- their SQEs are carried
    // along by the one on duty, saving their own io_uring_enter. Returns 0 = success;
    // >0 = unrecoverable errno (at that point failed_ is set and all in-flight Ops except
    // `self` have been woken with -EIO)
    int flush_locked(std::unique_lock<std::mutex>& lk, std::span<Op* const> self);
    // Set the error state and remove all in-flight Ops (except `except`, which the caller
    // notifies via exception); lock must be held, and the caller completes the returned
    // ops outside the lock
    std::vector<Op*> fail_all_locked(std::span<Op* const> except);
    void reap_loop();
    void unmap_rings();

    unsigned index_;
    int ring_fd_ = -1;
    ThreadPool* pool_;
    bool sqpoll_ = false;

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

    // Registered buffers: one anonymous mapping sliced into block_size blocks, each block
    // one iovec = one buf_index; free indices on a stack (fixed_mu_ is independent of
    // submit_mu_ -- a stream returning its blocks from a reaper-side completion must not
    // contend with submitters)
    mutable std::mutex fixed_mu_;
    std::byte* fixed_arena_ = nullptr;
    size_t fixed_arena_bytes_ = 0;
    unsigned fixed_block_ = 0;
    std::vector<int> fixed_free_;
    // Sparse registered file table: free slot indices; register/unregister go through
    // IORING_REGISTER_FILES_UPDATE (no quiesce since 5.6)
    std::vector<int> file_slots_;
    bool files_registered_ = false;

    std::thread reaper_;
};

UringRing::UringRing(unsigned index, ThreadPool* pool, const UringOptions& opt,
                     UringFeatures& feat, bool probe)
    : index_(index), pool_(pool) {
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
        if (index == 0)
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
        ring_fd_ = -1;
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

    sqpoll_ = (p.flags & IORING_SETUP_SQPOLL) != 0;
    if (probe) {
        feat.sqpoll = sqpoll_;
        probe_features(p, feat);
    }
    // Registration happens before the reaper starts: on pre-5.13 kernels REGISTER_BUFFERS /
    // REGISTER_FILES quiesce the ring (wait for in-flight requests), harmless only now
    register_resources(opt, feat);
}

// Capability probing (docs/archive/gaps.md §6.3): IORING_REGISTER_PROBE is available since 5.6,
// exactly the version where IORING_OP_READ/WRITE landed -- probe failure is treated as
// 5.1-5.5 and falls back to READV/WRITEV. Probe honestly instead of "try once and check for
// -EINVAL": that failure would land on some real request
void UringRing::probe_features(const io_uring_params& p, UringFeatures& feat) {
    feat.setup_features = p.features;
// IORING_REGISTER_PROBE is an enum constant, not a macro, so it cannot be tested;
// IO_URING_OP_SUPPORTED is a macro and entered the headers in the same batch (5.6) as
// io_uring_probe, so use it as the existence check
#ifdef IO_URING_OP_SUPPORTED
    // io_uring_probe is a variable-length header + ops[]; one call must cover up to
    // IORING_OP_UNLINKAT (37)
    constexpr unsigned kNrOps = 64;
    std::vector<uint8_t> buf(sizeof(io_uring_probe) + kNrOps * sizeof(io_uring_probe_op), 0);
    auto* probe = reinterpret_cast<io_uring_probe*>(buf.data());
    if (sys_io_uring_register(ring_fd_, IORING_REGISTER_PROBE, probe, kNrOps) == 0) {
        feat.probed = true;
        auto supported = [&](unsigned op) {
            return op < probe->ops_len && (probe->ops[op].flags & IO_URING_OP_SUPPORTED);
        };
        feat.op_read_write = supported(IORING_OP_READ) && supported(IORING_OP_WRITE);
        feat.op_fsync = supported(IORING_OP_FSYNC);
        feat.op_fixed_rw = supported(IORING_OP_READ_FIXED) && supported(IORING_OP_WRITE_FIXED);
        feat.op_openat = supported(IORING_OP_OPENAT);
        feat.op_statx = supported(IORING_OP_STATX);
        feat.op_renameat = supported(IORING_OP_RENAMEAT);
        feat.op_unlinkat = supported(IORING_OP_UNLINKAT);
        feat.links = true;  // IOSQE_IO_LINK is 5.3+, implied by the probe's 5.6
        return;
    }
#endif
    // 5.1 baseline: READV/WRITEV/FSYNC/NOP/READ_FIXED/WRITE_FIXED are guaranteed present,
    // everything newer guaranteed absent. Links exist since 5.3 but cannot be told apart
    // from 5.1/5.2 without the probe, so stay conservative
    feat.probed = false;
    feat.op_read_write = false;
    feat.op_fsync = true;
    feat.op_fixed_rw = true;
    feat.op_openat = feat.op_statx = feat.op_renameat = feat.op_unlinkat = false;
    feat.links = false;
}

void UringRing::register_resources(const UringOptions& opt, UringFeatures& feat) {
    // Fixed buffers: a failed registration (memlock quota on pre-5.12 kernels, EINVAL on
    // odd block sizes) only costs the optimization, never the ring. feat.fixed_buffers
    // stays true only while every ring succeeded -- streams consult the ring itself, the
    // feature bit is for the startup summary
    if (opt.fixed_buffers > 0 && feat.op_fixed_rw) {
        fixed_block_ = opt.block_size;
        fixed_arena_bytes_ = size_t(opt.fixed_buffers) * fixed_block_;
        void* p = ::mmap(nullptr, fixed_arena_bytes_, PROT_READ | PROT_WRITE,
                         MAP_PRIVATE | MAP_ANONYMOUS | MAP_POPULATE, -1, 0);
        if (p == MAP_FAILED) {
            LOG_WARN("io_uring ring {}: cannot allocate {} fixed buffers ({}); streams use "
                     "heap blocks", index_, opt.fixed_buffers, std::strerror(errno));
            feat.fixed_buffers = false;
        } else {
            std::vector<struct iovec> iov(opt.fixed_buffers);
            for (unsigned i = 0; i < opt.fixed_buffers; ++i) {
                iov[i].iov_base = static_cast<std::byte*>(p) + size_t(i) * fixed_block_;
                iov[i].iov_len = fixed_block_;
            }
            if (sys_io_uring_register(ring_fd_, IORING_REGISTER_BUFFERS, iov.data(),
                                      opt.fixed_buffers) == 0) {
                fixed_arena_ = static_cast<std::byte*>(p);
                fixed_free_.reserve(opt.fixed_buffers);
                for (int i = int(opt.fixed_buffers) - 1; i >= 0; --i) fixed_free_.push_back(i);
                if (index_ == 0) feat.fixed_buffers = true;
            } else {
                LOG_WARN("io_uring ring {}: IORING_REGISTER_BUFFERS({} x {}) refused ({}); "
                         "streams use heap blocks", index_, opt.fixed_buffers, fixed_block_,
                         std::strerror(errno));
                ::munmap(p, fixed_arena_bytes_);
                fixed_arena_bytes_ = 0;
                feat.fixed_buffers = false;
            }
        }
    } else if (index_ == 0) {
        feat.fixed_buffers = false;
    }

    // Fixed files: a sparse table (all -1) filled per stream through FILES_UPDATE. Sparse
    // sets and FILES_UPDATE are 5.5+, the non-quiescing update 5.6+ -- tie it to the probe
    if (opt.fixed_files > 0 && feat.probed) {
        std::vector<int> fds(opt.fixed_files, -1);
        if (sys_io_uring_register(ring_fd_, IORING_REGISTER_FILES, fds.data(),
                                  opt.fixed_files) == 0) {
            files_registered_ = true;
            file_slots_.reserve(opt.fixed_files);
            for (int i = int(opt.fixed_files) - 1; i >= 0; --i) file_slots_.push_back(i);
            if (index_ == 0) feat.fixed_files = true;
        } else {
            LOG_WARN("io_uring ring {}: IORING_REGISTER_FILES({}) refused ({}); streams use "
                     "plain fds", index_, opt.fixed_files, std::strerror(errno));
            feat.fixed_files = false;
        }
    } else if (index_ == 0) {
        feat.fixed_files = false;
    }
}

void UringRing::start_reaper() {
    reaper_ = std::thread([this] {
        char name[16];
        std::snprintf(name, sizeof name, "uring-reap%u", index_);
        ::pthread_setname_np(::pthread_self(), name);
        reap_loop();
    });
}

UringRing::~UringRing() {
    shutdown();
    if (fixed_arena_) ::munmap(fixed_arena_, fixed_arena_bytes_);
    unmap_rings();
    if (ring_fd_ >= 0) ::close(ring_fd_);
}

void UringRing::unmap_rings() {
    if (sqes_) ::munmap(sqes_, sqes_bytes_);
    if (cq_ring_ptr_ && cq_ring_ptr_ != sq_ring_ptr_) ::munmap(cq_ring_ptr_, cq_ring_bytes_);
    if (sq_ring_ptr_) ::munmap(sq_ring_ptr_, sq_ring_bytes_);
    sqes_ = nullptr;
    cq_ring_ptr_ = nullptr;
    sq_ring_ptr_ = nullptr;
}

bool UringRing::try_acquire_fixed(UringEngine::FixedBuf& out) {
    std::lock_guard lk(fixed_mu_);
    if (!fixed_arena_ || fixed_free_.empty()) return false;
    int idx = fixed_free_.back();
    fixed_free_.pop_back();
    out.ring = index_;
    out.index = idx;
    out.mem = std::span<std::byte>(fixed_arena_ + size_t(idx) * fixed_block_, fixed_block_);
    return true;
}

void UringRing::release_fixed(int index) {
    std::lock_guard lk(fixed_mu_);
    fixed_free_.push_back(index);
}

unsigned UringRing::fixed_free() const {
    std::lock_guard lk(fixed_mu_);
    return unsigned(fixed_free_.size());
}

int UringRing::register_file(int fd) {
    int slot;
    {
        std::lock_guard lk(fixed_mu_);
        if (!files_registered_ || file_slots_.empty()) return -1;
        slot = file_slots_.back();
        file_slots_.pop_back();
    }
    io_uring_files_update upd{};
    upd.offset = unsigned(slot);
    upd.fds = reinterpret_cast<uint64_t>(&fd);
    if (sys_io_uring_register(ring_fd_, IORING_REGISTER_FILES_UPDATE, &upd, 1) == 1) return slot;
    std::lock_guard lk(fixed_mu_);
    file_slots_.push_back(slot);
    return -1;
}

void UringRing::unregister_file(int slot) {
    int fd = -1;
    io_uring_files_update upd{};
    upd.offset = unsigned(slot);
    upd.fds = reinterpret_cast<uint64_t>(&fd);
    // A failed update leaves the kernel holding a reference to the file until the ring is
    // torn down; the slot is retired rather than handed out again with a stale file in it
    if (sys_io_uring_register(ring_fd_, IORING_REGISTER_FILES_UPDATE, &upd, 1) != 1) {
        LOG_WARN("io_uring ring {}: FILES_UPDATE(unregister slot {}) failed ({}); slot retired",
                 index_, slot, std::strerror(errno));
        return;
    }
    std::lock_guard lk(fixed_mu_);
    file_slots_.push_back(slot);
}

void UringRing::push_sqe_locked(const Sqe& d, uint64_t user_data) {
    unsigned tail = *sq_tail_;  // sole writer under the lock, a plain read suffices
    unsigned idx = tail & sq_mask_;
    io_uring_sqe& sqe = sqes_[idx];
    std::memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = d.opcode;
    sqe.flags = d.flags;
    sqe.fd = d.fd;
    sqe.addr = d.addr;
    sqe.len = d.len;
    sqe.off = d.off;
    // The per-opcode flags share one union slot (rw_flags / fsync_flags / open_flags /
    // statx_flags / rename_flags / unlink_flags); fsync_flags is its u32 spelling
    sqe.fsync_flags = d.op_flags;
    sqe.buf_index = d.buf_index;
    sqe.user_data = user_data;
    sq_array_[idx] = idx;
    store_release(sq_tail_, tail + 1);
}

int UringRing::flush_locked(std::unique_lock<std::mutex>& lk, std::span<Op* const> self) {
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
        if (sqpoll_) {
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
    LOG_ERROR("io_uring ring {} submit failed fatally: {}; failing {} in-flight op(s) with EIO",
              index_, std::strerror(fatal), orphans.size());
    for (Op* op : orphans) op->complete(-EIO);
    lk.lock();
    inflight_cv_.notify_all();
    return fatal;
}

std::vector<UringRing::Op*> UringRing::fail_all_locked(std::span<Op* const> except) {
    failed_ = true;
    std::vector<Op*> orphans;
    orphans.reserve(inflight_.size());
    for (Op* op : inflight_)
        if (std::find(except.begin(), except.end(), op) == except.end()) orphans.push_back(op);
    inflight_.clear();
    return orphans;
}

void UringRing::submit(std::span<const Sqe> chain, std::span<Op* const> ops) {
    const unsigned need = unsigned(chain.size());
    std::unique_lock lk(submit_mu_);
    for (;;) {
        if (stopped_ || failed_)
            throw S3Error(S3ErrorCode::InternalError,
                          failed_ ? "io_uring engine failed" : "io_uring engine stopped");
        if (sq_entries_ - (*sq_tail_ - load_acquire(sq_head_)) >= need) break;
        // SQ full = the flusher lags behind filling. If we are not on duty, give it a push
        // ourselves; otherwise wait for the on-duty one to make progress (it will either
        // progress or set failed_, and both notify)
        if (!flushing_) {
            if (int err = flush_locked(lk, {}))
                throw S3Error(S3ErrorCode::InternalError,
                              std::string("io_uring_enter: ") + std::strerror(err));
            continue;
        }
        sq_cv_.wait(lk);
    }
    // user_data=0 is reserved for shutdown's NOP sentinel; the Op pointer is never null
    for (size_t i = 0; i < chain.size(); ++i) {
        inflight_.insert(ops[i]);
        LIGHTS3_TSAN_RELEASE(ops[i]);
        push_sqe_locked(chain[i], reinterpret_cast<uint64_t>(ops[i]));
    }
    if (int err = flush_locked(lk, ops)) {
        for (Op* op : ops) inflight_.erase(op);  // treated as never submitted; other in-flight already woken with -EIO
        throw S3Error(S3ErrorCode::InternalError,
                      std::string("io_uring_enter: ") + std::strerror(err));
    }
}

void UringRing::reap_loop() {
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
                    op->complete(cqe.res);
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
                orphans = fail_all_locked({});
            }
            LOG_ERROR("io_uring ring {} reaper exiting on fatal error: {}; failing {} in-flight "
                      "op(s) with EIO",
                      index_, std::strerror(err), orphans.size());
            for (Op* op : orphans) op->complete(-EIO);
            sq_cv_.notify_all();
            inflight_cv_.notify_all();
            return;
        }
    }
}

void UringRing::shutdown() {
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
            LOG_ERROR("io_uring ring {} shutdown: {} op(s) still in flight after 10s; proceeding",
                      index_, inflight_.size());
    }
    if (!failed_ && reaper_.joinable()) {
        push_sqe_locked(Sqe{}, /*user_data=*/0);  // NOP sentinel
        flush_locked(lk, {});  // on failure it already set failed_ and woke in-flight; do not rethrow here
    }
    lk.unlock();
    if (reaper_.joinable()) reaper_.join();
}

// ---------------------------------------------------------------------------
// UringEngine: facade over the rings
// ---------------------------------------------------------------------------

UringEngine::UringEngine(std::shared_ptr<ThreadPool> pool, UringOptions opt)
    : pool_(std::move(pool)), opt_(opt) {
    if (opt_.block_size < 4096 || opt_.block_size % 4096 != 0)
        throw std::runtime_error("io_uring: block_size must be a multiple of 4096");
    if (opt_.read_depth == 0) opt_.read_depth = 1;
    if (opt_.write_depth == 0) opt_.write_depth = 1;
    unsigned n = opt_.rings;
    if (n == 0) n = std::clamp(std::thread::hardware_concurrency() / 8, 1u, 8u);
    feat_.rings = n;
    rings_.reserve(n);
    for (unsigned i = 0; i < n; ++i)
        rings_.push_back(std::make_unique<UringRing>(i, pool_.get(), opt_, feat_, i == 0));
    // The ring constructors ran the registrations; only now is it safe to start reaping
    for (auto& r : rings_) r->start_reaper();
    LOG_INFO("xlocalfs {} (sq_entries={}, block={}, read_depth={}, write_depth={})",
             feat_.describe(), opt_.entries, opt_.block_size, opt_.read_depth,
             opt_.write_depth);
}

UringEngine::~UringEngine() { shutdown(); }

void UringEngine::shutdown() {
    for (auto& r : rings_) r->shutdown();
}

void UringEngine::submit(unsigned ring, std::span<const Sqe> chain, std::span<Op* const> ops) {
    rings_[ring]->submit(chain, ops);
}

void UringEngine::CoroOp::complete(int r) noexcept {
    res = r;
    // Resume via the thread pool; post's internal lock makes the res write visible to the
    // resuming thread
    eng->pool_->post([h = h] { h.resume(); });
}

void UringEngine::Awaitable::await_suspend(std::coroutine_handle<> h) {
    op.eng = &eng;
    op.h = h;
    if (sqe.opcode == IORING_OP_READV || sqe.opcode == IORING_OP_WRITEV) {
        iov.iov_base = reinterpret_cast<void*>(sqe.addr);
        iov.iov_len = sqe.len;
        sqe.addr = reinterpret_cast<uint64_t>(&iov);
        sqe.len = 1;
    }
    unsigned r = ring < 0 ? eng.pick_ring() : unsigned(ring);
    Op* ops[1] = {&op};
    eng.submit(r, std::span<const Sqe>(&sqe, 1), std::span<Op* const>(ops, 1));
}

UringEngine::Awaitable UringEngine::read(int fd, std::span<std::byte> buf, uint64_t off) {
    Sqe s;
    s.opcode = feat_.op_read_write ? uint8_t(IORING_OP_READ) : uint8_t(IORING_OP_READV);
    s.fd = fd;
    s.addr = reinterpret_cast<uint64_t>(buf.data());
    s.len = static_cast<uint32_t>(buf.size());
    s.off = off;
    return {*this, s, -1, {}, {}};
}

UringEngine::Awaitable UringEngine::write(int fd, std::span<const std::byte> buf, uint64_t off) {
    Sqe s;
    s.opcode = feat_.op_read_write ? uint8_t(IORING_OP_WRITE) : uint8_t(IORING_OP_WRITEV);
    s.fd = fd;
    s.addr = reinterpret_cast<uint64_t>(buf.data());
    s.len = static_cast<uint32_t>(buf.size());
    s.off = off;
    return {*this, s, -1, {}, {}};
}

UringEngine::Awaitable UringEngine::fdatasync(int fd) {
    Sqe s;
    s.opcode = IORING_OP_FSYNC;
    s.fd = fd;
    s.op_flags = IORING_FSYNC_DATASYNC;
    return {*this, s, -1, {}, {}};
}

UringEngine::Awaitable UringEngine::fsync(int fd) {
    Sqe s;
    s.opcode = IORING_OP_FSYNC;
    s.fd = fd;
    return {*this, s, -1, {}, {}};
}

UringEngine::Awaitable UringEngine::openat(int dirfd, const char* path, int flags, mode_t mode) {
    Sqe s;
    s.opcode = IORING_OP_OPENAT;
    s.fd = dirfd;
    s.addr = reinterpret_cast<uint64_t>(path);
    s.len = mode;
    s.op_flags = uint32_t(flags);
    return {*this, s, -1, {}, {}};
}

UringEngine::Awaitable UringEngine::statx(int dirfd, const char* path, int flags, unsigned mask,
                                          struct ::statx* out) {
    Sqe s;
    s.opcode = IORING_OP_STATX;
    s.fd = dirfd;
    s.addr = reinterpret_cast<uint64_t>(path);
    s.len = mask;
    s.off = reinterpret_cast<uint64_t>(out);
    s.op_flags = uint32_t(flags);
    return {*this, s, -1, {}, {}};
}

UringEngine::Awaitable UringEngine::renameat(int olddirfd, const char* oldpath, int newdirfd,
                                             const char* newpath) {
    Sqe s;
    s.opcode = IORING_OP_RENAMEAT;
    s.fd = olddirfd;
    s.addr = reinterpret_cast<uint64_t>(oldpath);
    s.len = uint32_t(newdirfd);
    s.off = reinterpret_cast<uint64_t>(newpath);
    return {*this, s, -1, {}, {}};
}

UringEngine::Awaitable UringEngine::unlinkat(int dirfd, const char* path, int flags) {
    Sqe s;
    s.opcode = IORING_OP_UNLINKAT;
    s.fd = dirfd;
    s.addr = reinterpret_cast<uint64_t>(path);
    s.op_flags = uint32_t(flags);
    return {*this, s, -1, {}, {}};
}

bool UringEngine::try_acquire_fixed(unsigned ring, FixedBuf& out) {
    return rings_[ring]->try_acquire_fixed(out);
}

void UringEngine::release_fixed(const FixedBuf& b) {
    if (b.index >= 0) rings_[b.ring]->release_fixed(b.index);
}

unsigned UringEngine::fixed_free(unsigned ring) const { return rings_[ring]->fixed_free(); }

int UringEngine::register_file(unsigned ring, int fd) { return rings_[ring]->register_file(fd); }

void UringEngine::unregister_file(unsigned ring, int slot) {
    if (slot >= 0) rings_[ring]->unregister_file(slot);
}

}  // namespace lights3::storage
