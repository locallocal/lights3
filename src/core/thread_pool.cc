#include "core/thread_pool.h"

#include <stdexcept>

#include "core/log.h"

namespace lights3 {

using Clock = std::chrono::steady_clock;

ThreadPool::ThreadPool(size_t threads, size_t queue_capacity)
    : capacity_(queue_capacity ? queue_capacity : 1) {
    if (threads == 0) threads = 1;
    workers_.reserve(threads);
    for (size_t i = 0; i < threads; ++i)
        workers_.emplace_back([this] { worker_loop(); });
}

ThreadPool::~ThreadPool() { join(); }

void ThreadPool::post(std::function<void()> fn) {
    auto now = Clock::now();  // read the clock outside the lock: the enqueue side should not bring clock reads into the critical section either
    bool queued = false;
    {
        std::lock_guard lk(m_);
        if (!stopping_) {
            cont_queue_.push_back({std::move(fn), now});
            queued = true;
        }
    }
    if (queued) {
        cv_.notify_one();  // notify outside the lock: notifying while holding it makes the woken thread immediately collide with the lock
        return;
    }
    // Continuation delivery after join must not fail: the consumers
    // (FinalAwaiter::await_suspend, Permit's destructor) are all noexcept contexts,
    // where throwing means std::terminate. Running inline lets the remaining request
    // chain finish on the calling thread (schedule() keeps its throwing semantics,
    // as it has a co_await site to catch it)
    LOG_ERROR("ThreadPool: post after join, running task inline on caller thread");
    fn();
}

void ThreadPool::enqueue_bounded(std::function<void()> fn) {
    auto now = Clock::now();  // same as post: clock reads stay out of the critical section
    {
        std::lock_guard lk(m_);
        if (stopping_) throw std::runtime_error("ThreadPool: schedule after join");
        if (queue_.size() >= capacity_)
            backlog_.push_back({std::move(fn), now});
        else
            queue_.push_back({std::move(fn), now});
    }
    cv_.notify_one();
}

void ThreadPool::join() {
    {
        std::lock_guard lk(m_);
        if (stopping_) return;
        stopping_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_)
        if (t.joinable()) t.join();
}

ThreadPool::Stats ThreadPool::stats() const {
    Stats st;
    {
        std::lock_guard lk(m_);
        st.queue_depth = cont_queue_.size() + queue_.size();
        st.backlogged = backlog_.size();
    }
    st.completed = completed_.load(std::memory_order_relaxed);
    for (size_t i = 0; i < kWaitBuckets; ++i)
        st.wait_hist[i] = wait_hist_[i].load(std::memory_order_relaxed);
    st.wait_sum_us = wait_sum_us_.load(std::memory_order_relaxed);
    return st;
}

size_t ThreadPool::wait_bucket(Clock::duration d) {
    using namespace std::chrono;
    if (d < 1ms) return 0;
    if (d < 10ms) return 1;
    if (d < 100ms) return 2;
    if (d < 1s) return 3;
    return 4;
}

void ThreadPool::worker_loop() {
    for (;;) {
        Item item;
        {
            std::unique_lock lk(m_);
            cv_.wait(lk, [&] {
                return stopping_ || !cont_queue_.empty() || !queue_.empty() ||
                       !backlog_.empty();
            });
            if (cont_queue_.empty() && queue_.empty() && backlog_.empty())
                return;  // stopping and fully drained
            // Continuations first (§4): they are existing work that already yielded
            // the thread; queuing them behind new blocking tasks would turn
            // "suspend-resume" into "suspend-wait-in-a-long-line"
            if (!cont_queue_.empty()) {
                item = std::move(cont_queue_.front());
                cont_queue_.pop_front();
            } else {
                if (queue_.empty()) {
                    queue_.push_back(std::move(backlog_.front()));
                    backlog_.pop_front();
                }
                item = std::move(queue_.front());
                queue_.pop_front();
                // The freed slot releases one backpressure waiter (order-preserving: the wait list is FIFO too)
                if (!backlog_.empty() && queue_.size() < capacity_) {
                    queue_.push_back(std::move(backlog_.front()));
                    backlog_.pop_front();
                }
            }
        }
        // Timing and accounting both happen outside the lock (§4: wait_hist_ used to
        // read the clock while holding the lock)
        auto waited = Clock::now() - item.enqueued;
        wait_hist_[wait_bucket(waited)].fetch_add(1, std::memory_order_relaxed);
        wait_sum_us_.fetch_add(
            static_cast<uint64_t>(
                std::chrono::duration_cast<std::chrono::microseconds>(waited).count()),
            std::memory_order_relaxed);
        // Exception firewall: a task exception escaping the thread function means
        // std::terminate (coroutine continuations catch everything themselves; this
        // covers blocking tasks from bare post)
        try {
            item.fn();
        } catch (const std::exception& e) {
            LOG_ERROR("ThreadPool: task threw: {}", e.what());
        } catch (...) {
            LOG_ERROR("ThreadPool: task threw unknown exception");
        }
        completed_.fetch_add(1, std::memory_order_relaxed);
    }
}

bool ThreadPool::ScheduleAwaiter::suspend_impl(std::coroutine_handle<> h) {
    slot = std::make_shared<Slot>();
    slot->h = h;
    // Hold everything needed later in locals: once the cancel callback registers
    // successfully, the coroutine may resume on another thread at any time and
    // destroy this awaiter (this is no longer usable)
    auto s = slot;
    ThreadPool& p = pool;
    CancelToken tok = token;

    tok.on_cancel_publish(
        [s] {
            if (!s->claimed.exchange(true, std::memory_order_acq_rel)) {
                s->cancelled = true;
                // Resume in place: the resumed coroutine immediately throws
                // OperationCancelled from await_resume, doing nothing but exception
                // unwinding (no co_await is possible during unwinding — hitting a
                // suspension point hands the thread back), which is bounded work.
                // **Must not** be re-posted to the thread pool — the scenario where
                // cancellation matters most is precisely a saturated pool, where the
                // posted continuation would queue behind blocking tasks and
                // cancellation becomes an empty promise (implementing docs/gaps.md
                // §3.2's suggestion literally deadlocks; see the corresponding case
                // in test_concurrency). The firing-thread-side defense lives in
                // TimerQueue: callbacks run on a dedicated callback thread, so
                // unwinding does not stall expiry determination
                s->h.resume();
            }
        },
        s->reg_id, s->cancel_state);
    // No callback is registered on an already-cancelled token; this follow-up check
    // covers the cancelled-before-registration race
    if (tok.cancelled() && !s->claimed.exchange(true, std::memory_order_acq_rel)) {
        s->cancelled = true;
        return false;  // do not suspend; await_resume throws in place
    }
    try {
        p.enqueue_bounded([s] {
            if (!s->claimed.exchange(true, std::memory_order_acq_rel)) s->h.resume();
        });
    } catch (...) {
        // schedule after join: claim before throwing to block a second resume from
        // the cancel callback; if the cancel callback already claimed it, it will
        // resume, so treat this as suspended
        if (s->claimed.exchange(true, std::memory_order_acq_rel)) return true;
        throw;  // await_suspend throws -> the coroutine did not suspend, and the exception surfaces at the co_await
    }
    return true;
}

}  // namespace lights3
