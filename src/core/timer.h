// L4: process-wide timer thread; the foundation for timeout primitives like with_timeout (docs/concurrency.md §2/§5)
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <mutex>
#include <thread>

namespace lights3 {

class TimerQueue {
public:
    using Id = uint64_t;
    using Clock = std::chrono::steady_clock;

    TimerQueue();
    ~TimerQueue();
    TimerQueue(const TimerQueue&) = delete;

    static TimerQueue& instance();

    // Callback execution time histogram buckets: <10ms <100ms <1s <10s >=10s
    // (docs/gaps.md §7). Callbacks run serially, so a slow callback directly delays
    // subsequent timers — hence the bucket boundaries are coarser than the thread
    // pool's wait buckets
    static constexpr size_t kExecBuckets = 5;

    struct Stats {
        size_t pending = 0;        // timers not yet due
        size_t due = 0;            // timers due but still queued ahead of the callback thread
        uint64_t fired = 0;        // total callbacks fully executed
        uint64_t slow = 0;         // callbacks exceeding 1s (each accompanied by a WARN log)
        std::array<uint64_t, kExecBuckets> exec_hist{};
        uint64_t exec_sum_us = 0;  // cumulative callback execution time (microseconds)
        // Head-of-queue lag: how late the earliest due/executing callback already is
        // (seconds, 0 = no backlog). "Timers were blocked 3 seconds by some callback"
        // reads straight off this number
        double lag_seconds = 0;
    };
    Stats stats() const;

    // Call fn after delay. fn runs on a **dedicated callback thread**, separate from
    // the scheduling thread that determines expiry (docs/gaps.md §3.2): request_cancel
    // unwinds the cancelled coroutine chain in place, and running that on the
    // scheduling thread would stall every timer in the process during the unwind.
    // Callbacks are still serial with each other, so a single slow callback delays
    // subsequent callbacks (but no longer delays expiry determination); fn should
    // therefore still avoid blocking IO.
    // After shutdown (destruction already begun) returns 0 — a "valid but
    // never-firing" id is a debugging trap (docs/gaps.md §7); cancel(0) is always a
    // safe no-op, so callers need not distinguish
    Id add(Clock::duration delay, std::function<void()> fn);

    // Cancel a timer: returns true if it has not fired; false if already fired /
    // nonexistent. If the callback is **due but not yet finished** (in the pending
    // queue or currently executing), block until it settles — once cancel returns,
    // the caller can safely destroy resources captured by the callback, with no need
    // for its own lifetime guard over the "fired but not yet executed" window.
    // Self-cancellation on the callback thread (from inside a callback) does not wait
    // (prevents self-deadlock). Note: a callback must not hold locks that the
    // cancelling caller holds while waiting (the usual lock-ordering constraint for
    // cv waits)
    bool cancel(Id id);

private:
    void loop();       // scheduling thread: only determines expiry and dequeues
    void fire_loop();  // callback thread: executes due callbacks serially
    bool pending_locked(Id id) const;
    static size_t exec_bucket(Clock::duration d);

    mutable std::mutex m_;
    std::condition_variable cv_;       // scheduling thread
    std::condition_variable fire_cv_;  // callback thread
    std::condition_variable done_cv_;  // callback-finished notification (for cancel's blocking wait)
    // Pending table ordered by (deadline, id); deadlines_ provides reverse lookup by id
    std::map<std::pair<Clock::time_point, Id>, std::function<void()>> items_;
    std::map<Id, Clock::time_point> deadlines_;
    // Due, awaiting the callback thread; the deadline travels along so stats() can
    // compute head-of-queue lag
    struct DueItem {
        Id id;
        Clock::time_point deadline;
        std::function<void()> fn;
    };
    std::deque<DueItem> due_;
    Id next_id_ = 0;
    Id running_id_ = 0;  // id of the currently executing callback (0 = none)
    bool stopping_ = false;
    // Callback duration observability (docs/gaps.md §7); atomic storage, so stats()
    // reads without contending on locks with callbacks
    std::array<std::atomic<uint64_t>, kExecBuckets> exec_hist_{};
    std::atomic<uint64_t> exec_sum_us_{0};
    std::atomic<uint64_t> fired_{0};
    std::atomic<uint64_t> slow_{0};
    // Original deadline of the currently executing callback (valid when
    // running_id_ != 0; guarded by m_)
    Clock::time_point running_deadline_{};
    std::thread thread_;
    std::thread fire_thread_;
};

}  // namespace lights3
