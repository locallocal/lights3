// L4: blocking-IO thread pool; coroutines hop onto pool threads via
// co_await pool.schedule() (docs/concurrency.md §3: bounded queue + backpressure,
// depth/wait-time metrics, §5 cancellation)
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <concepts>
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
    // Histogram buckets for enqueue-to-start wait time: <1ms <10ms <100ms <1s >=1s
    static constexpr size_t kWaitBuckets = 5;
    // Bucket upper bounds (seconds) for Prometheus rendering (docs/archive/gaps.md §7); last bucket is +Inf
    static constexpr std::array<double, kWaitBuckets - 1> kWaitBucketBounds{0.001, 0.01, 0.1,
                                                                           1.0};

    struct Stats {
        size_t queue_depth = 0;   // ready queue length
        size_t backlogged = 0;    // schedule tasks held on the wait list by backpressure when the queue is full
        uint64_t completed = 0;   // tasks fully executed
        std::array<uint64_t, kWaitBuckets> wait_hist{};
        uint64_t wait_sum_us = 0;  // cumulative wait time (microseconds), the histogram's _sum
    };

    explicit ThreadPool(size_t threads, size_t queue_capacity = 4096);
    ~ThreadPool();
    ThreadPool(const ThreadPool&) = delete;

    // Unbounded enqueue: continuation delivery (executor post) may neither fail nor
    // wait; calling after join does not throw (noexcept consumers) — instead it logs
    // ERROR and runs in place on the calling thread
    void post(std::function<void()> fn);
    void join();  // stop accepting new tasks, drain the queue, and wait for threads to exit
    size_t size() const { return workers_.size(); }
    Stats stats() const;

    struct ScheduleAwaiter {
        // Suspension state lives in a separate shared block: the pool task and the
        // cancel callback race to resume; the loser still holds a reference, which
        // must not point into an awaiter/coroutine frame that may already be
        // destroyed once the coroutine resumes
        struct Slot {
            std::coroutine_handle<> h;
            std::atomic<bool> claimed{false};
            bool cancelled = false;  // written only by the successful claimer, read on the same thread after resume
            // Deregistration info for the cancel callback: reg_id is written by
            // on_cancel_publish inside the registration critical section
            std::atomic<uint64_t> reg_id{0};
            std::shared_ptr<detail::CancelState> cancel_state;
        };

        ThreadPool& pool;
        CancelToken token;
        std::shared_ptr<Slot> slot;

        bool await_ready() const noexcept { return false; }
        // When no token is passed explicitly, inherit from the calling coroutine's
        // promise (core/task.h's PromiseBase propagates it down the co_await chain) —
        // existing co_await pool_->schedule() call sites thus pick up request-level
        // cancellation without modification (docs/archive/gaps.md §3.1)
        template <class P>
        bool await_suspend(std::coroutine_handle<P> h) {
            if constexpr (requires {
                              { h.promise().cancel } -> std::convertible_to<CancelToken>;
                          })
                if (!token.valid()) token = h.promise().cancel;
            return suspend_impl(h);
        }
        bool suspend_impl(std::coroutine_handle<> h);
        void await_resume() {
            if (!slot) return;
            if (slot->cancel_state)
                slot->cancel_state->remove_callback(slot->reg_id.load(std::memory_order_acquire));
            if (slot->cancelled) throw OperationCancelled();
        }
    };
    // On token cancellation: tasks still queued are resumed with an
    // OperationCancelled exception (docs/concurrency.md §5); a blocking section
    // already running on a pool thread is not preempted — the caller checks the
    // token after it returns naturally
    ScheduleAwaiter schedule(CancelToken token = {}) { return {*this, std::move(token), nullptr}; }

private:
    friend struct ScheduleAwaiter;
    struct Item {
        std::function<void()> fn;
        std::chrono::steady_clock::time_point enqueued;
    };

    // schedule() path: when the queue is full, park on the backpressure wait list;
    // a worker releases it once space frees up
    void enqueue_bounded(std::function<void()> fn);
    void worker_loop();
    static size_t wait_bucket(std::chrono::steady_clock::duration d);

    mutable std::mutex m_;
    std::condition_variable cv_;
    // Continuation delivery (post) and blocking tasks (schedule) use separate queues
    // (docs/archive/gaps.md §4): post's contract is "may neither fail nor wait", and with a
    // shared 4096 queue a continuation would queue behind 4096 IOs under pressure.
    // Workers always drain the continuation queue first — continuations are existing
    // work that already yielded the thread, hence naturally higher priority
    std::deque<Item> cont_queue_;  // post: unbounded
    std::deque<Item> queue_;       // schedule: bounded by capacity_
    std::deque<Item> backlog_;
    size_t capacity_;
    // Lock-free per-task accounting (docs/archive/gaps.md §4: completed_ taking a lock per
    // task contends with scheduling)
    std::atomic<uint64_t> completed_{0};
    std::array<std::atomic<uint64_t>, kWaitBuckets> wait_hist_{};
    std::atomic<uint64_t> wait_sum_us_{0};
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
