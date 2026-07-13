// L4: 阻塞 IO 线程池；协程经 co_await pool.schedule() 切入池线程
// （docs/03-concurrency.md §3：有界队列 + 背压、深度/等待时长指标、§5 取消）
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <coroutine>
#include <cstdint>
#include <deque>
#include <functional>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>

#include "core/cancel.h"
#include "core/executor.h"

namespace lights3 {

class ThreadPool {
public:
    // 任务入队→开跑的等待时长直方图桶：<1ms <10ms <100ms <1s ≥1s
    static constexpr size_t kWaitBuckets = 5;

    struct Stats {
        size_t queue_depth = 0;   // 就绪队列长度
        size_t backlogged = 0;    // 队列满被背压挡在等待列表的 schedule 任务数
        uint64_t completed = 0;   // 已执行完的任务数
        std::array<uint64_t, kWaitBuckets> wait_hist{};
    };

    explicit ThreadPool(size_t threads, size_t queue_capacity = 4096);
    ~ThreadPool();
    ThreadPool(const ThreadPool&) = delete;

    // 无界入队：续体投递（executor post）不可失败也不可等待
    void post(std::function<void()> fn);
    void join();  // 停止接收新任务，排空队列并等待线程退出
    size_t size() const { return workers_.size(); }
    Stats stats() const;

    struct ScheduleAwaiter {
        // 挂起状态放独立共享块：池任务与取消回调竞争 resume，
        // 败者仍持有引用，不能指向可能已随协程恢复而销毁的 awaiter/协程帧
        struct Slot {
            std::coroutine_handle<> h;
            std::atomic<bool> claimed{false};
            bool cancelled = false;  // 仅 claim 成功者写，resume 后同线程读
            // 取消回调的注销信息：reg_id 由 on_cancel_publish 在注册临界区内落位
            std::atomic<uint64_t> reg_id{0};
            std::shared_ptr<detail::CancelState> cancel_state;
        };

        ThreadPool& pool;
        CancelToken token;
        std::shared_ptr<Slot> slot;

        bool await_ready() const noexcept { return false; }
        bool await_suspend(std::coroutine_handle<> h);
        void await_resume() {
            if (!slot) return;
            if (slot->cancel_state)
                slot->cancel_state->remove_callback(slot->reg_id.load(std::memory_order_acquire));
            if (slot->cancelled) throw OperationCancelled();
        }
    };
    // token 取消时：仍在排队的任务被以 OperationCancelled 异常 resume（docs/03 §5）；
    // 已在池线程上执行的阻塞段不被抢占，等其自然返回后由调用方检查 token
    ScheduleAwaiter schedule(CancelToken token = {}) { return {*this, std::move(token), nullptr}; }

private:
    friend struct ScheduleAwaiter;
    struct Item {
        std::function<void()> fn;
        std::chrono::steady_clock::time_point enqueued;
    };

    // schedule() 路径：队列满时挂到背压等待列表，由 worker 腾出空位后放行
    void enqueue_bounded(std::function<void()> fn);
    void worker_loop();
    static size_t wait_bucket(std::chrono::steady_clock::duration d);

    mutable std::mutex m_;
    std::condition_variable cv_;
    std::deque<Item> queue_;
    std::deque<Item> backlog_;
    size_t capacity_;
    uint64_t completed_ = 0;
    std::array<uint64_t, kWaitBuckets> wait_hist_{};
    std::vector<std::thread> workers_;
    bool stopping_ = false;
};

struct ThreadPoolExecutor final : IExecutor {
    explicit ThreadPoolExecutor(ThreadPool& pool) : pool_(pool) {}
    void post(std::coroutine_handle<> h) override {
        pool_.post([h] { h.resume(); });
    }

private:
    ThreadPool& pool_;
};

}  // namespace lights3
