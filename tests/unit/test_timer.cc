// TimerQueue unit tests (docs/concurrency.md §2/§5):
// firing, expiry ordering, cancel semantics (true if not yet fired / false if fired or absent), no dangling on destruction,
// wakeup when an earlier entry is inserted ahead. Timing assertions use only loose upper bounds (cv + timed waits) to avoid flakes on slow machines.
#include <atomic>
#include <stdexcept>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "core/timer.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace std::chrono_literals;
using Id2 = TimerQueue::Id;

namespace {

struct Signal {
    std::mutex m;
    std::condition_variable cv;
    int count = 0;
    void hit() {
        // Notify while holding the lock: once the waiter's predicate is satisfied it may destroy this object, and notifying outside the lock would use a destroyed cv
        std::lock_guard lk(m);
        ++count;
        cv.notify_all();
    }
    bool wait_for_count(int n, std::chrono::milliseconds timeout) {
        std::unique_lock lk(m);
        return cv.wait_for(lk, timeout, [&] { return count >= n; });
    }
};

}  // namespace

TEST(timer_fires_after_delay) {
    TimerQueue q;
    Signal s;
    auto t0 = TimerQueue::Clock::now();
    q.add(20ms, [&] { s.hit(); });
    CHECK(s.wait_for_count(1, 5000ms));
    CHECK(TimerQueue::Clock::now() - t0 >= 20ms);  // must not fire early
}

TEST(timer_fires_in_deadline_order) {
    TimerQueue q;
    Signal s;
    std::mutex om;
    std::vector<int> order;
    q.add(80ms, [&] {
        {
            std::lock_guard lk(om);
            order.push_back(1);
        }
        s.hit();
    });
    q.add(10ms, [&] {
        {
            std::lock_guard lk(om);
            order.push_back(2);
        }
        s.hit();
    });
    CHECK(s.wait_for_count(2, 5000ms));
    std::lock_guard lk(om);
    CHECK_EQ(order.size(), size_t(2));
    CHECK_EQ(order[0], 2);
    CHECK_EQ(order[1], 1);
}

TEST(timer_cancel_prevents_fire) {
    // A far-future deadline (10s) leaves ample scheduling slack between add and cancel (no flakes on slow machines/tsan);
    // cancel returning true proves the entry was removed before firing, so it can never run afterwards -- no wait needed to verify
    TimerQueue q;
    std::atomic<bool> fired{false};
    auto id = q.add(std::chrono::seconds(10), [&] { fired = true; });
    CHECK(q.cancel(id));   // not yet fired: cancel succeeds
    CHECK(!q.cancel(id));  // no longer exists: false
    CHECK(!fired.load());
}

TEST(timer_cancel_waits_for_running_callback) {
    // cancel blocks on a callback that is currently executing: once it returns, the callback has exited and captured resources can be destroyed safely
    TimerQueue q;
    Signal started;
    std::atomic<bool> release{false};
    std::atomic<bool> done{false};
    auto id = q.add(1ms, [&] {
        started.hit();
        while (!release.load()) std::this_thread::yield();
        done = true;
    });
    CHECK(started.wait_for_count(1, 5000ms));  // callback is already executing
    release = true;                            // after release, cancel must wait until done is set before returning
    CHECK(!q.cancel(id));                      // already-fired semantics: false, but blocks until the callback returns
    CHECK(done.load());
}

TEST(timer_cancel_from_callback_no_deadlock) {
    // Self-cancel on the timer thread (inside the callback) does not wait for itself -- prevents self-deadlock
    TimerQueue q;
    Signal s;
    TimerQueue::Id id = 0;
    std::mutex idm;
    {
        std::lock_guard lk(idm);
        id = q.add(1ms, [&] {
            std::lock_guard lk2(idm);
            CHECK(!q.cancel(id));  // self-cancel: returns false immediately, no deadlock
            s.hit();
        });
    }
    CHECK(s.wait_for_count(1, 5000ms));
}

TEST(timer_cancel_after_fire_returns_false) {
    TimerQueue q;
    Signal s;
    auto id = q.add(1ms, [&] { s.hit(); });
    CHECK(s.wait_for_count(1, 5000ms));
    CHECK(!q.cancel(id));  // already fired: semantics say false
}

TEST(timer_destructor_with_pending_items_returns) {
    // Destruction does not wait for unexpired entries: returns immediately without firing callbacks
    std::atomic<bool> fired{false};
    {
        TimerQueue q;
        q.add(std::chrono::seconds(3600), [&] { fired = true; });
    }
    CHECK(!fired.load());
}

TEST(timer_add_earlier_item_preempts_wait) {
    // Insert an earlier entry while the loop is waiting on a far-future one: the cv notification path must wake it promptly
    TimerQueue q;
    Signal s;
    q.add(std::chrono::seconds(3600), [&] {});
    q.add(20ms, [&] { s.hit(); });
    CHECK(s.wait_for_count(1, 5000ms));
}

TEST(timer_callback_exception_does_not_wedge_cancel) {
    // gaps §2.2: when a callback throws, resetting running_id_ and the notify must still happen,
    // otherwise cancel(that id) blocks forever (it is the first step of every backend's shutdown path)
    TimerQueue q;
    Signal s;
    auto id = q.add(1ms, [&] {
        s.hit();
        throw std::runtime_error("timer callback failure");
    });
    CHECK(s.wait_for_count(1, 5000ms));
    CHECK(!q.cancel(id));  // already fired: returns false immediately instead of hanging
    // The queue thread is still alive; subsequent entries fire as usual
    Signal s2;
    q.add(1ms, [&] { s2.hit(); });
    CHECK(s2.wait_for_count(1, 5000ms));
}

// ---------- Callback thread separated from the scheduling thread (gaps §3.2) ----------

TEST(slow_callback_does_not_stall_deadline_tracking) {
    // A cancel callback unwinds the cancelled coroutine chain in place -- bounded but nonzero work. It must run on a dedicated callback
    // thread: if it ran on the scheduling thread, other timers expiring meanwhile could not even get their expiry check
    TimerQueue q;
    Signal slow_started;
    q.add(10ms, [&] {
        slow_started.hit();
        std::this_thread::sleep_for(300ms);
    });
    Id2 second = q.add(40ms, [] {});
    CHECK(slow_started.wait_for_count(1, 2s));
    std::this_thread::sleep_for(120ms);  // well past the second entry's expiry time
    // The slow callback is still running (300ms not yet up). If the second entry were still in the pending table, cancel would return
    // true -- meaning the scheduling thread was stuck in the callback. Returning false = it was removed on time and is merely queued for execution
    CHECK(!q.cancel(second));
}

TEST(cancel_waits_for_due_but_unstarted_callback) {
    // An entry that has expired but is still in the callback queue (not yet started): cancel must wait for it to settle,
    // otherwise the caller would destroy resources the callback still needs to access
    TimerQueue q;
    Signal first_started;
    std::atomic<bool> second_ran{false};
    q.add(10ms, [&] {
        first_started.hit();
        std::this_thread::sleep_for(200ms);
    });
    Id2 second = q.add(20ms, [&] { second_ran = true; });
    CHECK(first_started.wait_for_count(1, 2s));
    std::this_thread::sleep_for(60ms);  // the second entry has expired and is queued behind the slow callback
    CHECK(!q.cancel(second));  // returns false = already fired
    CHECK(second_ran.load());  // and by the time it returns it has indeed finished executing
}

TEST(timer_stats_track_fired_and_pending) {
    // Timer observability (docs/archive/gaps.md §7): pending/fired/latency histogram have an outlet
    TimerQueue q;
    std::atomic<int> fired{0};
    q.add(std::chrono::hours(1), [] {});  // long-hanging: permanently pending
    q.add(std::chrono::milliseconds(1), [&] { fired.fetch_add(1); });
    for (int i = 0; i < 200 && fired.load() == 0; ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    CHECK_EQ(fired.load(), 1);
    // No synchronization point between callback completion and accounting; poll briefly
    TimerQueue::Stats st;
    for (int i = 0; i < 200; ++i) {
        st = q.stats();
        if (st.fired == 1) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    CHECK_EQ(st.fired, uint64_t(1));
    CHECK_EQ(st.pending, size_t(1));
    uint64_t total = 0;
    for (auto v : st.exec_hist) total += v;
    CHECK_EQ(total, uint64_t(1));
    CHECK_EQ(st.slow, uint64_t(0));
}

TEST(timer_slow_callback_counted) {
    TimerQueue q;
    std::atomic<bool> done{false};
    q.add(std::chrono::milliseconds(1), [&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(1100));
        done.store(true);
    });
    for (int i = 0; i < 600 && !done.load(); ++i)
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    CHECK(done.load());
    TimerQueue::Stats st;
    for (int i = 0; i < 200; ++i) {
        st = q.stats();
        if (st.slow == 1) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    }
    CHECK_EQ(st.slow, uint64_t(1));
}
