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

// 提交线程 → 内核 → 收割线程的 happens-before 经由 syscall 与 ring barrier
// 传递，TSan 观测不到，会把收割侧对 Op 的读写全部误报成竞态；在 Op 指针上
// 补一条注解边（release 于提交、acquire 于收割）精确还原这层同步
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

// ring 头尾指针在与内核共享的映射页上，须以原子方式配对读写
unsigned load_acquire(const unsigned* p) { return __atomic_load_n(p, __ATOMIC_ACQUIRE); }
void store_release(unsigned* p, unsigned v) { __atomic_store_n(p, v, __ATOMIC_RELEASE); }

void* ring_mmap(int fd, size_t bytes, uint64_t offset) {
    void* p = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd,
                     static_cast<off_t>(offset));
    return p == MAP_FAILED ? nullptr : p;
}

// enter 的可重试信号：EINTR（信号）/EAGAIN、EBUSY（内核内存压力或 CQ 满）。
// 前几轮 yield（瞬时抖动的常见情形），随后指数睡到 1ms 封顶让出 CPU
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
    // SQPOLL 在 5.11 之前要 CAP_SYS_ADMIN，容器里通常没有。建权失败回落普通模式
    // 而不是让进程起不来——SQPOLL 是吞吐优化，不是正确性前提
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

// 能力探测（docs/gaps.md §6.3）：IORING_REGISTER_PROBE 自 5.6 起可用，正好是
// IORING_OP_READ/WRITE 落地的版本——探测失败即视为 5.1–5.5，回落 READV/WRITEV。
// 老实探测而不是"试一次看 -EINVAL"：后者的失败会落在某个真实请求头上
void UringEngine::probe_features(const io_uring_params& p) {
    feat_.setup_features = p.features;
// IORING_REGISTER_PROBE 是 enum 常量不是宏，判不了；IO_URING_OP_SUPPORTED 才是
// 宏，且与 io_uring_probe 同批（5.6）进的头文件，用它做存在性判据
#ifdef IO_URING_OP_SUPPORTED
    // io_uring_probe 是变长头 + ops[]；一次要够覆盖到 IORING_OP_WRITE(23)
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
    // 5.1 基线：READV/WRITEV/FSYNC/NOP 一定有，READ/WRITE 一定没有
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
    unsigned tail = *sq_tail_;  // 锁内唯一写者，普通读即可
    unsigned idx = tail & sq_mask_;
    io_uring_sqe& sqe = sqes_[idx];
    std::memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = opcode;
    sqe.fd = fd;
    sqe.addr = reinterpret_cast<uint64_t>(addr);
    sqe.off = off;
    sqe.user_data = user_data;
    // FSYNC 用 fsync_flags 而非 len 传 DATASYNC（两者在 sqe 的 union 里错位）
    if (opcode == IORING_OP_FSYNC) sqe.fsync_flags = len;
    else sqe.len = len;
    sq_array_[idx] = idx;
    store_release(sq_tail_, tail + 1);
}

int UringEngine::flush_locked(std::unique_lock<std::mutex>& lk, Op* self) {
    // 已有线程在班：我们的 SQE 落在它的提交窗口里，会被一并带走。这正是批量
    // 提交的来源——高并发下 N 条 SQE 收敛到一次 io_uring_enter，而不是 N 次
    if (flushing_) return 0;
    flushing_ = true;
    int spin = 0;
    int fatal = 0;
    while (submitted_ != *sq_tail_) {
        unsigned n = *sq_tail_ - submitted_;
        // SQPOLL：内核线程自己会取，只有它睡着了才需要一次 wakeup enter
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
                submitted_ = *sq_tail_;  // 唤醒后轮询线程会取走全部
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
            sq_cv_.notify_all();  // 腾出 SQ 空位
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
    // 批量提交下无法只回滚自己那一条：同批被捎带的 Op 早已挂起，submit() 已经
    // 返回过了。ring 走到这一步（EBADF/EFAULT/ENXIO 等）已不可用——置错误态、
    // 把在途全部以 -EIO 唤醒，self 由调用方以异常告知
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
        // SQ 满 = flusher 落后于填充。自己没在班就去推一把，否则等在班者推进
        // （它一定会推进或置 failed_，两者都会 notify）
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
        inflight_.erase(op);  // 协程按未挂起处理；其余在途已被 -EIO 唤醒
        throw S3Error(S3ErrorCode::InternalError,
                      std::string("io_uring_enter: ") + std::strerror(err));
    }
}

void UringEngine::reap_loop() {
    for (;;) {
        unsigned head = *cq_head_;  // 唯一消费者，普通读即可
        unsigned tail = load_acquire(cq_tail_);
        bool stop = false;
        while (head != tail) {
            const io_uring_cqe& cqe = cqes_[head & cq_mask_];
            if (cqe.user_data == 0) {
                stop = true;  // shutdown 提交的 NOP 哨兵
            } else {
                Op* op = reinterpret_cast<Op*>(static_cast<uintptr_t>(cqe.user_data));
                LIGHTS3_TSAN_ACQUIRE(op);
                op->res = cqe.res;
                bool drained;
                {
                    std::lock_guard lk(submit_mu_);
                    inflight_.erase(op);
                    drained = stopped_ && inflight_.empty();
                }
                if (drained) inflight_cv_.notify_all();
                // 经线程池恢复；post 的内部锁保证 res 写入对恢复线程可见
                pool_->post([h = op->h] { h.resume(); });
            }
            ++head;
        }
        store_release(cq_head_, head);
        if (stop) return;
        int ret = sys_io_uring_enter(ring_fd_, 0, 1, IORING_ENTER_GETEVENTS);
        if (ret < 0 && errno != EINTR && errno != EAGAIN && errno != EBUSY) {
            // 不可恢复错误：无声退出会让所有在途 co_await 永不 resume（GET 挂死、
            // 连接不释放、帧泄漏），而 submit() 还在照常收新单。置错误态拒新提交，
            // 全部在途 Op 以 -EIO 唤醒。注意内核理论上仍可能完成这些 IO 并写入
            // 用户缓冲——但走到这里的 errno（EBADF/EFAULT/ENXIO）都意味着 ring
            // 已不可用，两害相权取告知调用方
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
    stopped_ = true;  // 先拒新提交，才谈得上排空
    sq_cv_.notify_all();
    if (!failed_) {
        // 等在途 CQE 归零再投哨兵：CQE 顺序不保证哨兵排在既有读写之后，不等排空
        // 就析构 munmap，内核会继续写已释放的用户缓冲（UAF）。超时只告警不阻死
        // 进程退出——此时风险已无法消除，至少留下现场
        if (!inflight_cv_.wait_for(lk, std::chrono::seconds(10),
                                   [&] { return inflight_.empty() || failed_; }))
            LOG_ERROR("io_uring shutdown: {} op(s) still in flight after 10s; proceeding",
                      inflight_.size());
    }
    if (!failed_) {
        push_sqe_locked(IORING_OP_NOP, -1, nullptr, 0, 0, /*user_data=*/0);
        flush_locked(lk, nullptr);  // 失败已在内部置 failed_ 并唤醒在途，此处不上抛
    }
    lk.unlock();
    if (reaper_.joinable()) reaper_.join();
}

}  // namespace lights3::storage
