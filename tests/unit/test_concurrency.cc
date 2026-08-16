// docs/concurrency.md concurrency primitives: when_all / with_timeout / cancellation / home executor / backpressure and metrics / semaphore
#include <atomic>
#include <chrono>
#include <future>
#include <numeric>
#include <stdexcept>
#include <thread>
#include <vector>

#include "core/cancel.h"
#include "core/semaphore.h"
#include "core/task.h"
#include "core/thread_pool.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace std::chrono_literals;

namespace {

Task<int> square_on(ThreadPool& pool, int v) {
    co_await pool.schedule();
    co_return v * v;
}

// Get the thread id of one of the given thread pool's workers
std::thread::id pool_thread_id(ThreadPool& pool) {
    auto t = [](ThreadPool& p) -> Task<std::thread::id> {
        co_await p.schedule();
        co_return std::this_thread::get_id();
    };
    return sync_wait(t(pool));
}

}  // namespace

// ---------- when_all ----------

TEST(when_all_collects_in_order) {
    ThreadPool pool(4);
    std::vector<Task<int>> tasks;
    for (int i = 0; i < 16; ++i) tasks.push_back(square_on(pool, i));
    auto results = sync_wait(when_all(std::move(tasks)));
    CHECK_EQ(results.size(), size_t(16));
    for (int i = 0; i < 16; ++i) CHECK_EQ(results[i], i * i);
}

TEST(when_all_empty) {
    auto results = sync_wait(when_all(std::vector<Task<int>>{}));
    CHECK(results.empty());
}

TEST(when_all_void) {
    ThreadPool pool(2);
    std::atomic<int> ran{0};
    auto t = [&](ThreadPool& p) -> Task<void> {
        co_await p.schedule();
        ++ran;
    };
    std::vector<Task<void>> tasks;
    for (int i = 0; i < 8; ++i) tasks.push_back(t(pool));
    sync_wait(when_all(std::move(tasks)));
    CHECK_EQ(ran.load(), 8);
}

TEST(when_all_rethrows_after_all_complete) {
    ThreadPool pool(2);
    std::atomic<int> completed{0};
    auto ok = [&](ThreadPool& p) -> Task<int> {
        co_await p.schedule();
        ++completed;
        co_return 1;
    };
    auto bad = [&](ThreadPool& p) -> Task<int> {
        co_await p.schedule();
        throw std::runtime_error("shard failed");
    };
    std::vector<Task<int>> tasks;
    tasks.push_back(ok(pool));
    tasks.push_back(bad(pool));
    tasks.push_back(ok(pool));
    bool thrown = false;
    try {
        sync_wait(when_all(std::move(tasks)));
    } catch (const std::runtime_error& e) {
        thrown = true;
        CHECK_EQ(std::string(e.what()), "shard failed");
    }
    CHECK(thrown);
    CHECK_EQ(completed.load(), 2);  // the exception does not interrupt the other shards
}

// ---------- Cancellation ----------

TEST(schedule_precancelled_throws) {
    ThreadPool pool(1);
    CancelSource src;
    src.request_cancel();
    auto t = [](ThreadPool& p, CancelToken tok) -> Task<int> {
        co_await p.schedule(std::move(tok));
        co_return 1;  // should not be reached
    };
    bool thrown = false;
    try {
        sync_wait(t(pool, src.token()));
    } catch (const OperationCancelled&) {
        thrown = true;
    }
    CHECK(thrown);
}

TEST(cancel_resumes_queued_task_with_exception) {
    ThreadPool pool(1);
    std::promise<void> gate;
    auto blocked = gate.get_future().share();
    pool.post([blocked] { blocked.wait(); });  // occupies the only worker

    CancelSource src;
    auto t = [](ThreadPool& p, CancelToken tok) -> Task<int> {
        co_await p.schedule(std::move(tok));
        co_return 1;
    };
    std::atomic<bool> cancelled{false};
    std::thread waiter([&] {
        try {
            sync_wait(t(pool, src.token()));
        } catch (const OperationCancelled&) {
            cancelled = true;
        }
    });
    std::this_thread::sleep_for(20ms);  // let the task enter the queue
    src.request_cancel();               // worker still occupied: the cancellation path resumes it
    waiter.join();
    CHECK(cancelled.load());
    gate.set_value();
}

TEST(token_check_in_loop) {
    CancelSource src;
    auto tok = src.token();
    CHECK(!tok.cancelled());
    src.request_cancel();
    CHECK(tok.cancelled());
    bool thrown = false;
    try {
        tok.throw_if_cancelled();
    } catch (const OperationCancelled&) {
        thrown = true;
    }
    CHECK(thrown);
    // A default token never cancels
    CancelToken none;
    CHECK(!none.cancelled());
    none.throw_if_cancelled();
}

TEST(cancel_registration_raii) {
    CancelSource src;
    std::atomic<int> fired{0};
    auto reg1 = src.token().on_cancel([&] { ++fired; });
    {
        auto reg2 = src.token().on_cancel([&] { ++fired; });
    }  // reg2 unregisters
    src.request_cancel();
    CHECK_EQ(fired.load(), 1);
    // Registering after cancellation does not invoke the callback
    auto reg3 = src.token().on_cancel([&] { ++fired; });
    CHECK_EQ(fired.load(), 1);
}

// ---------- with_timeout ----------

TEST(with_timeout_passes_result_through) {
    ThreadPool pool(2);
    CancelSource src;
    auto r = sync_wait(with_timeout(square_on(pool, 7), 5000ms, src));
    CHECK_EQ(r, 49);
    CHECK(!src.cancelled());
}

TEST(with_timeout_cancels_cooperatively) {
    ThreadPool pool(2);
    CancelSource src;
    // Simulate a chunked streaming operation: return to the pool between chunks and observe cancellation
    auto slow = [](ThreadPool& p, CancelToken tok) -> Task<int> {
        for (int i = 0; i < 1000; ++i) {
            co_await p.schedule(tok);
            std::this_thread::sleep_for(5ms);  // the "blocking segment" on a pool thread
            tok.throw_if_cancelled();
        }
        co_return 0;
    };
    auto begin = std::chrono::steady_clock::now();
    bool thrown = false;
    try {
        sync_wait(with_timeout(slow(pool, src.token()), 50ms, src));
    } catch (const OperationCancelled&) {
        thrown = true;
    }
    CHECK(thrown);
    CHECK(std::chrono::steady_clock::now() - begin < 2s);  // far earlier than 1000*5ms
}

// ---------- Switching back to the home executor (docs/concurrency.md §3) ----------

TEST(continuation_posted_back_to_home_executor) {
    ThreadPool home(1);
    ThreadPool work(2);
    ThreadPoolExecutor home_exec(home);
    auto home_id = pool_thread_id(home);

    auto parent = [](ThreadPool& wp) -> Task<std::pair<std::thread::id, std::thread::id>> {
        auto child = [](ThreadPool& p) -> Task<std::thread::id> {
            co_await p.schedule();
            co_return std::this_thread::get_id();
        };
        auto child_tid = co_await child(wp);  // the child task inherits the home executor
        co_return std::make_pair(child_tid, std::this_thread::get_id());
    };
    auto t = parent(work);
    t.via(home_exec);
    auto [child_tid, parent_tid] = sync_wait(std::move(t));
    CHECK(child_tid != home_id);      // the blocking segment runs on a pool thread
    CHECK_EQ(parent_tid, home_id);    // the continuation is posted back to home
}

TEST(resume_on_switches_executor) {
    ThreadPool pool(1);
    ThreadPoolExecutor exec(pool);
    auto tid = pool_thread_id(pool);
    auto t = [](IExecutor& ex) -> Task<std::thread::id> {
        co_await resume_on(ex);
        co_return std::this_thread::get_id();
    };
    CHECK_EQ(sync_wait(t(exec)), tid);
}

TEST(inline_executor_resumes_in_place) {
    ThreadPool pool(2);
    auto t = [](ThreadPool& p) -> Task<std::thread::id> {
        co_await p.schedule();
        co_return std::this_thread::get_id();  // inline: the continuation stays on the pool thread
    };
    auto task = t(pool);
    task.via(InlineExecutor::instance());
    CHECK(sync_wait(std::move(task)) != std::this_thread::get_id());
}

// ---------- Backpressure and metrics (docs/concurrency.md §3) ----------

TEST(bounded_queue_backpressure) {
    ThreadPool pool(1, /*queue_capacity=*/1);
    std::promise<void> gate;
    auto blocked = gate.get_future().share();
    pool.post([blocked] { blocked.wait(); });  // occupies the only worker

    std::atomic<int> done{0};
    auto t = [&](ThreadPool& p) -> Task<void> {
        co_await p.schedule();
        ++done;
    };
    std::vector<std::thread> waiters;
    for (int i = 0; i < 3; ++i)
        waiters.emplace_back([&] { sync_wait(t(pool)); });
    // Wait for all 3 tasks to be suspended: 1 enters the ready queue, 2 are held back on the wait list by backpressure
    for (int spin = 0; spin < 200; ++spin) {
        auto s = pool.stats();
        if (s.queue_depth + s.backlogged == 3) break;
        std::this_thread::sleep_for(5ms);
    }
    auto s = pool.stats();
    CHECK_EQ(s.queue_depth, size_t(1));
    CHECK_EQ(s.backlogged, size_t(2));

    gate.set_value();
    for (auto& w : waiters) w.join();
    CHECK_EQ(done.load(), 3);
    s = pool.stats();
    CHECK_EQ(s.queue_depth, size_t(0));
    CHECK_EQ(s.backlogged, size_t(0));
    CHECK_EQ(s.completed, uint64_t(4));  // the gate task + 3 schedules
    uint64_t hist_total = std::accumulate(s.wait_hist.begin(), s.wait_hist.end(), uint64_t(0));
    CHECK_EQ(hist_total, uint64_t(4));   // every dequeued task recorded its wait time
}

// ---------- AsyncSemaphore（docs/concurrency.md §6）----------

TEST(semaphore_limits_concurrency) {
    ThreadPool pool(8);
    AsyncSemaphore sem(2);
    std::atomic<int> current{0}, peak{0}, done{0};
    auto t = [&](ThreadPool& p) -> Task<void> {
        co_await p.schedule();
        auto permit = co_await sem.acquire();
        int now = ++current;
        int seen = peak.load();
        while (now > seen && !peak.compare_exchange_weak(seen, now)) {}
        std::this_thread::sleep_for(2ms);
        --current;
        ++done;
    };
    std::vector<Task<void>> tasks;
    for (int i = 0; i < 16; ++i) tasks.push_back(t(pool));
    sync_wait(when_all(std::move(tasks)));
    CHECK_EQ(done.load(), 16);
    CHECK(peak.load() <= 2);
    CHECK_EQ(sem.available(), 2L);  // all permits returned
    CHECK_EQ(sem.waiting(), size_t(0));
}

// try_acquire (the second quota slot of the rados double-buffered pipeline): non-blocking, never enters the wait
// queue; can be re-acquired after return, and Permit's bool observer distinguishes held/empty
TEST(semaphore_try_acquire_nonblocking) {
    AsyncSemaphore sem(1);
    auto p1 = sem.try_acquire();
    CHECK(p1.has_value());
    CHECK(*p1);                            // held state
    CHECK(!sem.try_acquire().has_value());  // exhausted: immediate nullopt, no queueing
    CHECK_EQ(sem.waiting(), size_t(0));
    p1->release();
    CHECK(!*p1);                           // empty after release
    auto p2 = sem.try_acquire();
    CHECK(p2.has_value());
    CHECK_EQ(sem.available(), 0L);
}

TEST(semaphore_permit_released_on_exception) {
    ThreadPool pool(2);
    AsyncSemaphore sem(1);
    auto bad = [&](ThreadPool& p) -> Task<void> {
        auto permit = co_await sem.acquire();
        co_await p.schedule();
        throw std::runtime_error("fail while holding permit");
    };
    bool thrown = false;
    try {
        sync_wait(bad(pool));
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    CHECK(thrown);
    CHECK_EQ(sem.available(), 1L);  // the exception path also returns the permit
}

// ---------- Posting during shutdown and the exception firewall (gaps §2.1/§2.2) ----------

TEST(post_after_join_runs_inline_without_throwing) {
    ThreadPool pool(2);
    pool.join();
    // The consumer of continuation posting is a noexcept context: after join it must never throw, so it executes in place instead
    std::atomic<bool> ran{false};
    pool.post([&] { ran = true; });
    CHECK(ran.load());
}

TEST(pool_task_exception_does_not_kill_worker) {
    ThreadPool pool(1);
    pool.post([] { throw std::runtime_error("escaped task exception"); });
    std::promise<void> done;
    pool.post([&] { done.set_value(); });  // the same worker thread is still alive
    CHECK(done.get_future().wait_for(5s) == std::future_status::ready);
}

// ---------- Cancellation-system wiring (gaps §3.1/§3.2/§3.9) ----------

TEST(semaphore_acquire_is_cancellable) {
    AsyncSemaphore sem(1);
    ThreadPool pool(1);
    auto hold = sem.try_acquire();  // holds the only permit
    CHECK(hold.has_value());

    CancelSource src;
    auto t = [](AsyncSemaphore& s, CancelToken tok) -> Task<int> {
        auto p = co_await s.acquire(std::move(tok));
        co_return 1;  // should not be reached
    };
    std::atomic<bool> cancelled{false};
    std::thread waiter([&] {
        try {
            sync_wait(t(sem, src.token()));
        } catch (const OperationCancelled&) {
            cancelled = true;
        }
    });
    while (sem.waiting() < 1) std::this_thread::sleep_for(1ms);  // wait for it to enter the wait queue
    CHECK_EQ(sem.waiting(), size_t(1));
    src.request_cancel();
    waiter.join();
    CHECK(cancelled.load());
    CHECK_EQ(sem.waiting(), size_t(0));  // the cancelled waiter is unlinked; the permit will not be handed to a dead waiter
    hold.reset();
    CHECK_EQ(sem.available(), 1L);
}

TEST(semaphore_close_wakes_all_waiters) {
    AsyncSemaphore sem(0);  // no permits: everyone queues
    auto t = [](AsyncSemaphore& s) -> Task<int> {
        auto p = co_await s.acquire();
        co_return 1;
    };
    std::atomic<int> cancelled{0};
    std::vector<std::thread> ws;
    for (int i = 0; i < 3; ++i)
        ws.emplace_back([&] {
            try {
                sync_wait(t(sem));
            } catch (const OperationCancelled&) {
                ++cancelled;
            }
        });
    while (sem.waiting() < 3) std::this_thread::sleep_for(1ms);
    sem.close();
    for (auto& w : ws) w.join();
    CHECK_EQ(cancelled.load(), 3);
    // No new acquires accepted after close
    bool thrown = false;
    try {
        sync_wait(t(sem));
    } catch (const OperationCancelled&) {
        thrown = true;
    }
    CHECK(thrown);
}

TEST(cancel_token_propagates_down_task_chain) {
    // A child task should inherit the token even without passing it explicitly -- the wiring style of existing co_await pool.schedule() call sites
    ThreadPool pool(1);
    std::promise<void> gate;
    auto blocked = gate.get_future().share();
    pool.post([blocked] { blocked.wait(); });  // occupies the only worker

    auto inner = [](ThreadPool& p) -> Task<int> {
        co_await p.schedule();  // no argument: the token comes from promise inheritance
        co_return 1;
    };
    auto outer = [&inner](ThreadPool& p) -> Task<int> { co_return co_await inner(p); };

    CancelSource src;
    std::atomic<bool> cancelled{false};
    std::thread waiter([&] {
        try {
            sync_wait(std::move(outer(pool).with_cancel(src.token())));
        } catch (const OperationCancelled&) {
            cancelled = true;
        }
    });
    std::this_thread::sleep_for(20ms);
    src.request_cancel();
    waiter.join();
    CHECK(cancelled.load());
    gate.set_value();
}

TEST(cancel_registration_reset_waits_for_inflight_callback) {
    // After reset() returns, the callback must no longer be running (same semantics as TimerQueue::cancel)
    CancelSource src;
    std::atomic<bool> in_cb{false};
    std::atomic<bool> cb_done{false};
    std::atomic<bool> reset_returned{false};
    auto reg = src.token().on_cancel([&] {
        in_cb = true;
        std::this_thread::sleep_for(100ms);
        cb_done = true;
    });
    std::thread trigger([&] { src.request_cancel(); });
    while (!in_cb.load()) std::this_thread::sleep_for(1ms);
    reg.reset();
    reset_returned = true;
    CHECK(cb_done.load());  // the callback finished during reset, it is not running concurrently
    trigger.join();
    CHECK(reset_returned.load());
}
