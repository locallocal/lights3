// L3: 原生 syscall 的最小 io_uring 封装（不依赖 liburing，docs/01 §6 依赖策略）。
// 单 ring：提交侧互斥锁串行化 SQE 填充与 io_uring_enter；独立收割线程等待 CQE，
// 完成后把协程续体投递到线程池恢复（后续的同步落盘调用因此天然在池线程执行）。
#pragma once

#include <linux/io_uring.h>

#include <coroutine>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>
#include <span>
#include <thread>

#include "core/thread_pool.h"

namespace lights3::storage {

class UringEngine {
public:
    // entries 为 SQ 深度；提交为逐 SQE 立即 enter，SQ 不会积压，
    // 溢出的 CQE 由内核缓存（IORING_FEAT_NODROP），深度只影响批量吞吐
    explicit UringEngine(std::shared_ptr<ThreadPool> pool, unsigned entries = 256);
    ~UringEngine();
    UringEngine(const UringEngine&) = delete;

    // 停止收割线程（幂等）；须在所有在途操作完成后调用，之后不得再提交
    void shutdown();

    struct Op {
        std::coroutine_handle<> h;
        int res = 0;  // cqe.res：读/写字节数，<0 为 -errno
    };

    // co_await 返回 cqe.res；提交失败（不可恢复的 enter 错误）时 SQE 回滚、
    // 异常从 co_await 处抛出，协程按未挂起处理
    struct Awaitable {
        UringEngine& eng;
        uint8_t opcode;
        int fd;
        const void* addr;
        unsigned len;
        uint64_t off;
        Op op;

        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            op.h = h;
            eng.submit(opcode, fd, addr, len, off, &op);
        }
        int await_resume() const noexcept { return op.res; }
    };

    Awaitable read(int fd, std::span<std::byte> buf, uint64_t off) {
        return {*this, IORING_OP_READ, fd, buf.data(),
                static_cast<unsigned>(buf.size()), off, {}};
    }
    Awaitable write(int fd, std::span<const std::byte> buf, uint64_t off) {
        return {*this, IORING_OP_WRITE, fd, buf.data(),
                static_cast<unsigned>(buf.size()), off, {}};
    }
    Awaitable fsync(int fd) { return {*this, IORING_OP_FSYNC, fd, nullptr, 0, 0, {}}; }

private:
    // user_data=0 保留作 shutdown 的 NOP 哨兵，op 不得为空
    void submit(uint8_t opcode, int fd, const void* addr, unsigned len, uint64_t off, Op* op);
    void push_and_enter(uint8_t opcode, int fd, const void* addr, unsigned len, uint64_t off,
                        uint64_t user_data);  // 须持有 submit_mu_
    void reap_loop();
    void unmap_rings();

    int ring_fd_ = -1;
    std::shared_ptr<ThreadPool> pool_;

    // SQ（提交侧，submit_mu_ 保护）
    std::mutex submit_mu_;
    bool stopped_ = false;
    unsigned sq_entries_ = 0;
    unsigned sq_mask_ = 0;
    unsigned* sq_head_ = nullptr;
    unsigned* sq_tail_ = nullptr;
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
