// L3: 原生 syscall 的最小 io_uring 封装（不依赖 liburing，docs/architecture.md §6 依赖策略）。
// 单 ring：提交侧互斥锁串行化 SQE 填充，io_uring_enter 由"当班 flusher"批量代提
// （docs/gaps.md §6.3）；独立收割线程等待 CQE，完成后把协程续体投递到线程池恢复
// （后续的同步落盘调用因此天然在池线程执行）。
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
    unsigned entries = 256;  // SQ 深度
    // SQPOLL（docs/gaps.md §6.3）：内核侧轮询 SQ，常态下提交完全不进内核（只在
    // 轮询线程休眠后补一次 wakeup enter）。代价是一个常驻内核线程，且 5.11 之前
    // 需要 CAP_SYS_ADMIN——建权失败自动回落普通模式，不让进程起不来
    bool sqpoll = false;
    int sqpoll_idle_ms = 100;
};

// 内核能力探测结果（docs/gaps.md §6.3）：此前实现无条件用 IORING_OP_READ/WRITE，
// 这两个 opcode 5.6 才有——5.1–5.5 的内核上每个 IO 都会拿到 -EINVAL，表现为
// "io_uring 装配成功但所有读写都失败"。探测后老内核回落 READV/WRITEV（单 iovec）
struct UringFeatures {
    uint32_t setup_features = 0;  // io_uring_params.features 位图
    bool op_read_write = false;   // IORING_OP_READ / IORING_OP_WRITE（5.6+）
    bool op_fsync = false;        // IORING_OP_FSYNC（5.1+，仍显式探测）
    bool sqpoll = false;          // 实际启用（请求了但建权失败时为 false）
    bool probed = false;          // IORING_REGISTER_PROBE 可用；否则取 5.1 保守基线

    std::string describe() const;
};

class UringEngine {
public:
    explicit UringEngine(std::shared_ptr<ThreadPool> pool, UringOptions opt = {});
    ~UringEngine();
    UringEngine(const UringEngine&) = delete;

    // 停止收割线程（幂等），之后不得再提交。先拒新提交、等在途 CQE 排空
    //（带超时告警）再投哨兵——CQE 顺序不保证哨兵排在既有读写之后，不等排空就
    // munmap 会让内核继续写已释放的用户缓冲（docs/gaps.md §2.9）
    void shutdown();

    const UringFeatures& features() const { return feat_; }

    struct Op {
        std::coroutine_handle<> h;
        int res = 0;  // cqe.res：读/写字节数，<0 为 -errno
    };

    // co_await 返回 cqe.res；提交失败（不可恢复的 enter 错误）时异常从 co_await
    // 处抛出，协程按未挂起处理（同批被捎带的其它 Op 以 -EIO 唤醒——批量提交下
    // 无法只回滚自己那一条）
    struct Awaitable {
        UringEngine& eng;
        uint8_t opcode;
        int fd;
        const void* addr;
        unsigned len;
        uint64_t off;
        // READV/WRITEV 回落路径的 iovec：随 Awaitable 存活于协程帧，跨挂起有效
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
    // fsync_flags=IORING_FSYNC_DATASYNC 即 fdatasync 语义（写路径只需数据持久）
    Awaitable fdatasync(int fd) {
        Awaitable a{*this, IORING_OP_FSYNC, fd, nullptr, IORING_FSYNC_DATASYNC, 0, {}, {}};
        return a;
    }

private:
    // user_data=0 保留作 shutdown 的 NOP 哨兵，op 不得为空
    void submit(uint8_t opcode, int fd, const void* addr, unsigned len, uint64_t off, Op* op);
    // 只填 SQE 并推 tail（须持 submit_mu_；调用方已确认 SQ 有空位）
    void push_sqe_locked(uint8_t opcode, int fd, const void* addr, unsigned len, uint64_t off,
                         uint64_t user_data);
    // 把 [submitted_, sq_tail_) 交给内核（批量提交，docs/gaps.md §6.3）。当班
    // flusher 之外的调用直接返回——它们的 SQE 会被在班者一并带走，省掉自己那次
    // io_uring_enter。返回 0 = 成功；>0 = 不可恢复的 errno（此时 failed_ 已置、
    // 除 self 外的在途 Op 已以 -EIO 唤醒）
    int flush_locked(std::unique_lock<std::mutex>& lk, Op* self);
    // 置错误态并摘走全部在途 Op（except 除外，由调用方以异常告知）；须持锁，
    // 返回的句柄由调用方在锁外 resume
    std::vector<Op*> fail_all_locked(Op* except);
    void probe_features(const io_uring_params& p);
    void reap_loop();
    void unmap_rings();

    int ring_fd_ = -1;
    std::shared_ptr<ThreadPool> pool_;
    UringFeatures feat_;

    // SQ（提交侧，submit_mu_ 保护）
    std::mutex submit_mu_;
    bool stopped_ = false;
    bool failed_ = false;  // 收割/提交遇不可恢复错误；后续提交一律拒绝
    bool flushing_ = false;         // 已有线程在跑 io_uring_enter（批量提交的当班标记）
    unsigned submitted_ = 0;        // 已交给内核的 SQE 计数（与 sq_tail_ 同一序列）
    std::condition_variable sq_cv_;  // SQ 满 / flusher 推进
    // 在途操作登记（submit_mu_ 保护）：收割线程失败时以 -EIO 唤醒全部在途协程
    //（否则 GET 永久挂死、连接不释放），shutdown 依它等待排空
    std::unordered_set<Op*> inflight_;
    std::condition_variable inflight_cv_;
    unsigned sq_entries_ = 0;
    unsigned sq_mask_ = 0;
    unsigned* sq_head_ = nullptr;
    unsigned* sq_tail_ = nullptr;
    unsigned* sq_flags_ = nullptr;  // SQPOLL 的 IORING_SQ_NEED_WAKEUP
    unsigned* sq_array_ = nullptr;
    io_uring_sqe* sqes_ = nullptr;

    // CQ（仅收割线程消费）
    unsigned cq_mask_ = 0;
    unsigned* cq_head_ = nullptr;
    unsigned* cq_tail_ = nullptr;
    io_uring_cqe* cqes_ = nullptr;

    void* sq_ring_ptr_ = nullptr;
    size_t sq_ring_bytes_ = 0;
    void* cq_ring_ptr_ = nullptr;  // FEAT_SINGLE_MMAP 时与 sq_ring_ptr_ 相同
    size_t cq_ring_bytes_ = 0;
    size_t sqes_bytes_ = 0;

    std::thread reaper_;
};

}  // namespace lights3::storage
