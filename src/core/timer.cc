#include "core/timer.h"

#include "core/log.h"

namespace lights3 {

TimerQueue::TimerQueue() : thread_([this] { loop(); }), fire_thread_([this] { fire_loop(); }) {}

TimerQueue::~TimerQueue() {
    {
        std::lock_guard lk(m_);
        stopping_ = true;
        // Process teardown: due-but-unexecuted callbacks are dropped wholesale (the
        // object is being destroyed; running them would only touch dead state).
        // Callers waiting in cancel() are released accordingly
        due_.clear();
    }
    cv_.notify_all();
    fire_cv_.notify_all();
    done_cv_.notify_all();
    thread_.join();
    fire_thread_.join();
}

TimerQueue& TimerQueue::instance() {
    static TimerQueue q;
    return q;
}

TimerQueue::Id TimerQueue::add(Clock::duration delay, std::function<void()> fn) {
    Id id = 0;
    bool wake = false;
    {
        std::lock_guard lk(m_);
        // Refuse after shutdown (docs/gaps.md §7): previously this returned a
        // "valid but never-firing" id, and in destruction-time races the
        // investigator would stare at a timer that never rings. 0 closes the loop
        // with the cancel(0) no-op convention, so callers need not be aware
        if (stopping_) {
            LOG_WARN("TimerQueue: add() after shutdown, timer dropped");
            return 0;
        }
        id = ++next_id_;
        auto deadline = Clock::now() + delay;
        items_.emplace(std::make_pair(deadline, id), std::move(fn));
        deadlines_.emplace(id, deadline);
        // Only when this becomes the earliest deadline does the scheduling thread
        // need waking to recompute its wait; in all other cases it would wake only
        // to go back to sleep until the original deadline (docs/gaps.md §4:
        // previously every add did notify_all)
        wake = items_.begin()->first == std::make_pair(deadline, id);
    }
    if (wake) cv_.notify_one();  // notify outside the lock: notifying while holding it makes the woken thread immediately collide with the lock
    return id;
}

bool TimerQueue::pending_locked(Id id) const {
    if (running_id_ == id) return true;
    for (auto& item : due_)
        if (item.id == id) return true;
    return false;
}

size_t TimerQueue::exec_bucket(Clock::duration d) {
    using namespace std::chrono;
    if (d < 10ms) return 0;
    if (d < 100ms) return 1;
    if (d < 1s) return 2;
    if (d < 10s) return 3;
    return 4;
}

TimerQueue::Stats TimerQueue::stats() const {
    Stats st;
    {
        std::lock_guard lk(m_);
        st.pending = items_.size();
        st.due = due_.size();
        // Head-of-queue lag: the executing callback takes precedence (it has the
        // earliest deadline), otherwise look at the head of the pending queue
        Clock::time_point head{};
        if (running_id_ != 0) head = running_deadline_;
        else if (!due_.empty()) head = due_.front().deadline;
        if (head != Clock::time_point{}) {
            auto lag = Clock::now() - head;
            if (lag.count() > 0) st.lag_seconds = std::chrono::duration<double>(lag).count();
        }
    }
    st.fired = fired_.load(std::memory_order_relaxed);
    st.slow = slow_.load(std::memory_order_relaxed);
    st.exec_sum_us = exec_sum_us_.load(std::memory_order_relaxed);
    for (size_t i = 0; i < kExecBuckets; ++i)
        st.exec_hist[i] = exec_hist_[i].load(std::memory_order_relaxed);
    return st;
}

bool TimerQueue::cancel(Id id) {
    std::unique_lock lk(m_);
    auto it = deadlines_.find(id);
    if (it != deadlines_.end()) {
        items_.erase({it->second, id});
        deadlines_.erase(it);
        return true;
    }
    // Already fired: if still in the pending queue or currently executing, wait for
    // it to settle (except self-cancellation on the callback thread — waiting on
    // yourself is certain deadlock); this gives the caller the destruction-safety
    // guarantee that "after cancel returns, the callback never runs again" (see the
    // header comment). id 0 is invalid (allocation starts at 1, and it is also
    // running_id_'s idle value), so it is a direct no-op
    if (id != 0 && std::this_thread::get_id() != fire_thread_.get_id())
        done_cv_.wait(lk, [&] { return !pending_locked(id); });
    return false;
}

// Scheduling thread: only determines expiry and hands due items to the callback
// thread. Callbacks are never executed here — a callback may be request_cancel,
// which unwinds the whole cancelled coroutine chain in place, and running that on
// this thread would stall every timer in the process
void TimerQueue::loop() {
    std::unique_lock lk(m_);
    for (;;) {
        if (stopping_) return;
        if (items_.empty()) {
            cv_.wait(lk);
            continue;
        }
        auto first = items_.begin();
        // The deadline must be taken by value: wait_until would wait holding a
        // reference while the lock is released, and if a concurrent cancel() erases
        // the node, re-comparing the time on wakeup would read freed memory
        auto deadline = first->first.first;
        if (deadline > Clock::now()) {
            cv_.wait_until(lk, deadline);
            continue;
        }
        Id id = first->first.second;
        due_.push_back({id, deadline, std::move(first->second)});
        deadlines_.erase(id);
        items_.erase(first);
        fire_cv_.notify_one();
    }
}

void TimerQueue::fire_loop() {
    std::unique_lock lk(m_);
    for (;;) {
        fire_cv_.wait(lk, [&] { return stopping_ || !due_.empty(); });
        if (due_.empty()) {
            if (stopping_) return;
            continue;
        }
        DueItem item = std::move(due_.front());
        due_.pop_front();
        running_id_ = item.id;
        running_deadline_ = item.deadline;
        lk.unlock();
        // Executed outside the lock; a cancel(id) meanwhile blocks until this
        // returns (semantics remain false, "already fired"). Exception firewall: a
        // callback exception escaping the thread function means terminate; and the
        // running_id_ reset + notify must execute on every path, or cancel(that id)
        // blocks forever — and cancel is exactly the first step of every backend's
        // shutdown path
        auto t0 = Clock::now();
        try {
            item.fn();
        } catch (const std::exception& e) {
            LOG_ERROR("TimerQueue: callback threw: {}", e.what());
        } catch (...) {
            LOG_ERROR("TimerQueue: callback threw unknown exception");
        }
        // Duration accounting (docs/gaps.md §7): callbacks are serial, so a slow
        // callback directly delays subsequent timers — beyond 1s it is called out
        // individually on top of the histogram, so "timers were blocked 3 seconds"
        // is henceforth traceable in the logs
        auto elapsed = Clock::now() - t0;
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        exec_hist_[exec_bucket(elapsed)].fetch_add(1, std::memory_order_relaxed);
        exec_sum_us_.fetch_add(static_cast<uint64_t>(us), std::memory_order_relaxed);
        fired_.fetch_add(1, std::memory_order_relaxed);
        if (elapsed >= std::chrono::seconds(1)) {
            slow_.fetch_add(1, std::memory_order_relaxed);
            LOG_WARN("TimerQueue: callback took {:.3f}s, delaying subsequent timers",
                     std::chrono::duration<double>(elapsed).count());
        }
        lk.lock();
        running_id_ = 0;
        done_cv_.notify_all();
    }
}

}  // namespace lights3
