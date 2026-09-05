// Task<T> / sync_wait / ThreadPool coroutine primitives
#include <atomic>
#include <set>
#include <stdexcept>

#include "core/task.h"
#include "core/thread_pool.h"
#include "unit/mini_test.h"

using namespace lights3;

namespace {

Task<int> answer() { co_return 42; }

Task<int> add(int a, int b) {
    int x = co_await answer();
    co_return a + b + x - 42;
}

Task<int> boom() {
    throw std::runtime_error("boom");
    co_return 0;  // unreachable
}

Task<std::thread::id> on_pool(ThreadPool& pool) {
    co_await pool.schedule();
    co_return std::this_thread::get_id();
}

Task<int> hop_twice(ThreadPool& pool) {
    co_await pool.schedule();
    int a = 1;
    co_await pool.schedule();
    co_return a + 1;
}

}  // namespace

TEST(task_returns_value) { CHECK_EQ(sync_wait(answer()), 42); }

TEST(task_composes) { CHECK_EQ(sync_wait(add(1, 2)), 3); }

TEST(task_propagates_exception) {
    bool thrown = false;
    try {
        sync_wait(boom());
    } catch (const std::runtime_error& e) {
        thrown = true;
        CHECK_EQ(std::string(e.what()), "boom");
    }
    CHECK(thrown);
}

TEST(task_void_works) {
    bool ran = false;
    auto t = [&]() -> Task<void> {
        ran = true;
        co_return;
    };
    sync_wait(t());
    CHECK(ran);
}

TEST(schedule_switches_thread) {
    ThreadPool pool(2);
    auto id = sync_wait(on_pool(pool));
    CHECK(id != std::this_thread::get_id());
}

TEST(schedule_twice) {
    ThreadPool pool(2);
    CHECK_EQ(sync_wait(hop_twice(pool)), 2);
}

TEST(many_concurrent_tasks) {
    ThreadPool pool(4);
    std::atomic<int> sum{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 32; ++i) {
        threads.emplace_back([&pool, &sum, i] {
            auto t = [](ThreadPool& p, int v) -> Task<int> {
                co_await p.schedule();
                co_return v * 2;
            };
            sum += sync_wait(t(pool, i));
        });
    }
    for (auto& t : threads) t.join();
    CHECK_EQ(sum.load(), 31 * 32);  // 2 * (0+1+...+31)
}

// ---------- sync_wait_pumping / PumpExecutor（docs/archive/gaps.md §2.10）----------

TEST(pump_executor_runs_resume_on_caller_thread) {
    ThreadPool pool(2);
    PumpExecutor ex;
    auto caller = std::this_thread::get_id();
    // The task chain first hops to a pool thread, then switches back to the request thread via resume_on (the blocking-read switch path)
    auto t = [](ThreadPool& p, PumpExecutor& e,
                std::thread::id caller) -> Task<bool> {
        co_await p.schedule();
        bool on_pool = std::this_thread::get_id() != caller;
        co_await resume_on(e);
        bool back_on_caller = std::this_thread::get_id() == caller;
        co_return on_pool && back_on_caller;
    };
    CHECK(sync_wait_pumping(ex, t(pool, ex, caller)));
}

TEST(pump_executor_value_and_exception) {
    PumpExecutor ex1;
    auto v = []( ) -> Task<int> { co_return 7; };
    CHECK_EQ(sync_wait_pumping(ex1, v()), 7);
    PumpExecutor ex2;
    auto boom = []() -> Task<void> {
        throw std::runtime_error("boom");
        co_return;
    };
    bool caught = false;
    try {
        sync_wait_pumping(ex2, boom());
    } catch (const std::runtime_error&) {
        caught = true;
    }
    CHECK(caught);
}

// ---------- Started<T> (roadmap §4.3 ①, docs/concurrency.md §2.3) ----------

TEST(started_collect_by_wait) {
    ThreadPool pool(2);
    auto child = [](ThreadPool& p) -> Task<int> {
        co_await p.schedule();
        co_return 41 + 1;
    };
    Started<int> st(child(pool));
    CHECK(st.pending());
    CHECK_EQ(st.wait(), 42);
    CHECK(!st.pending());
    // Restart on the same object
    st.start(child(pool));
    CHECK_EQ(st.wait(), 42);
}

TEST(started_collect_by_co_await_and_exception) {
    ThreadPool pool(2);
    auto child = [](ThreadPool& p, bool fail) -> Task<int> {
        co_await p.schedule();
        if (fail) throw std::runtime_error("child failed");
        co_return 7;
    };
    auto outer = [&](bool fail) -> Task<int> {
        Started<int> st(child(pool, fail));
        co_return co_await st;
    };
    CHECK_EQ(sync_wait(outer(false)), 7);
    bool caught = false;
    try {
        sync_wait(outer(true));
    } catch (const std::runtime_error& e) {
        caught = std::string(e.what()) == "child failed";
    }
    CHECK(caught);
    // Already-finished child: co_await must not hang and must return the value
    auto late = [&]() -> Task<int> {
        Started<int> st(child(pool, false));
        st.wait();  // collected synchronously; a second collect is the caller's error, so restart
        st.start(child(pool, false));
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
        co_return co_await st;
    };
    CHECK_EQ(sync_wait(late()), 7);
}

TEST(started_destructor_waits_for_uncollected_child) {
    // The child writes into memory owned by the scope that owns the Started;
    // destroying the Started early must block until the child is done
    ThreadPool pool(1);
    std::atomic<bool> wrote{false};
    {
        int slot = 0;
        Started<void> st([](ThreadPool& p, int& out, std::atomic<bool>& flag) -> Task<void> {
            co_await p.schedule();
            std::this_thread::sleep_for(std::chrono::milliseconds(60));
            out = 1;
            flag.store(true);
        }(pool, slot, wrote));
        CHECK(st.pending());
    }  // ~Started blocks here
    CHECK(wrote.load());
}

TEST(pump_executor_resume_on_inline_when_running) {
    PumpExecutor ex;
    // The constructing thread is the pumping thread; any other thread is not
    CHECK(ex.running_in_this_thread());
    bool elsewhere = true;
    std::thread([&] { elsewhere = ex.running_in_this_thread(); }).join();
    CHECK(!elsewhere);
    auto t = [](PumpExecutor& e) -> Task<bool> {
        bool inside = e.running_in_this_thread();
        auto before = std::this_thread::get_id();
        co_await resume_on(e);  // fast path: already on the pumping thread
        co_return inside && std::this_thread::get_id() == before;
    };
    CHECK(sync_wait_pumping(ex, t(ex)));
}

TEST(task_moved_from_throws_not_segv) {
    // Calling on a moved-from Task used to be a null-pointer dereference (docs/archive/gaps.md §4): all four entry
    // points now throw logic_error; take_result is covered too (it has direct callers besides sync_wait)
    auto make = []() -> Task<int> { co_return 1; };
    auto count_throws = [](auto&& fn) {
        try {
            fn();
        } catch (const std::logic_error&) {
            return true;
        } catch (...) {
        }
        return false;
    };

    Task<int> a = make();
    Task<int> moved_a = std::move(a);
    CHECK(count_throws([&] { a.start(nullptr); }));
    CHECK(count_throws([&] { (void)a.take_result(); }));

    Task<void> v = []() -> Task<void> { co_return; }();
    Task<void> moved_v = std::move(v);
    CHECK(count_throws([&] { v.start(nullptr); }));
    CHECK(count_throws([&] { v.take_result(); }));

    // The copy that was not moved away still works normally
    CHECK_EQ(sync_wait(std::move(moved_a)), 1);
    sync_wait(std::move(moved_v));
}
