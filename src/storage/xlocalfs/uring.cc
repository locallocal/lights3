#include "storage/xlocalfs/uring.h"

#include <sys/mman.h>
#include <sys/syscall.h>
#include <unistd.h>

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

#include "s3/errors.h"

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

// ring 头尾指针在与内核共享的映射页上，须以原子方式配对读写
unsigned load_acquire(const unsigned* p) { return __atomic_load_n(p, __ATOMIC_ACQUIRE); }
void store_release(unsigned* p, unsigned v) { __atomic_store_n(p, v, __ATOMIC_RELEASE); }

void* ring_mmap(int fd, size_t bytes, uint64_t offset) {
    void* p = ::mmap(nullptr, bytes, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_POPULATE, fd,
                     static_cast<off_t>(offset));
    return p == MAP_FAILED ? nullptr : p;
}

}  // namespace

UringEngine::UringEngine(std::shared_ptr<ThreadPool> pool, unsigned entries)
    : pool_(std::move(pool)) {
    io_uring_params p{};
    ring_fd_ = sys_io_uring_setup(entries, &p);
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
    sq_mask_ = *reinterpret_cast<unsigned*>(sq + p.sq_off.ring_mask);
    sq_array_ = reinterpret_cast<unsigned*>(sq + p.sq_off.array);
    sq_entries_ = p.sq_entries;

    auto* cq = static_cast<uint8_t*>(cq_ring_ptr_);
    cq_head_ = reinterpret_cast<unsigned*>(cq + p.cq_off.head);
    cq_tail_ = reinterpret_cast<unsigned*>(cq + p.cq_off.tail);
    cq_mask_ = *reinterpret_cast<unsigned*>(cq + p.cq_off.ring_mask);
    cqes_ = reinterpret_cast<io_uring_cqe*>(cq + p.cq_off.cqes);

    reaper_ = std::thread([this] { reap_loop(); });
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

void UringEngine::push_and_enter(uint8_t opcode, int fd, const void* addr, unsigned len,
                                 uint64_t off, uint64_t user_data) {
    unsigned tail = *sq_tail_;  // 锁内唯一写者，普通读即可
    if (tail - load_acquire(sq_head_) >= sq_entries_)
        throw S3Error(S3ErrorCode::InternalError, "io_uring SQ full");

    unsigned idx = tail & sq_mask_;
    io_uring_sqe& sqe = sqes_[idx];
    std::memset(&sqe, 0, sizeof(sqe));
    sqe.opcode = opcode;
    sqe.fd = fd;
    sqe.addr = reinterpret_cast<uint64_t>(addr);
    sqe.len = len;
    sqe.off = off;
    sqe.user_data = user_data;
    sq_array_[idx] = idx;
    store_release(sq_tail_, tail + 1);

    for (;;) {
        int ret = sys_io_uring_enter(ring_fd_, 1, 0, 0);
        if (ret >= 1) return;
        if (ret >= 0) continue;  // 消费 0 个，重试
        if (errno == EINTR || errno == EAGAIN || errno == EBUSY) {
            std::this_thread::yield();
            continue;
        }
        // SQE 未被内核消费：回滚 tail 后抛错，协程按未挂起处理
        store_release(sq_tail_, tail);
        throw S3Error(S3ErrorCode::InternalError,
                      std::string("io_uring_enter: ") + std::strerror(errno));
    }
}

void UringEngine::submit(uint8_t opcode, int fd, const void* addr, unsigned len, uint64_t off,
                         Op* op) {
    std::lock_guard lk(submit_mu_);
    if (stopped_) throw S3Error(S3ErrorCode::InternalError, "io_uring engine stopped");
    push_and_enter(opcode, fd, addr, len, off, reinterpret_cast<uint64_t>(op));
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
                op->res = cqe.res;
                // 经线程池恢复；post 的内部锁保证 res 写入对恢复线程可见
                pool_->post([h = op->h] { h.resume(); });
            }
            ++head;
        }
        store_release(cq_head_, head);
        if (stop) return;
        int ret = sys_io_uring_enter(ring_fd_, 0, 1, IORING_ENTER_GETEVENTS);
        if (ret < 0 && errno != EINTR && errno != EAGAIN && errno != EBUSY) return;
    }
}

void UringEngine::shutdown() {
    {
        std::lock_guard lk(submit_mu_);
        if (stopped_) return;
        stopped_ = true;
        push_and_enter(IORING_OP_NOP, -1, nullptr, 0, 0, /*user_data=*/0);
    }
    if (reaper_.joinable()) reaper_.join();
}

}  // namespace lights3::storage
