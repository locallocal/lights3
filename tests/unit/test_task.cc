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

// ---------- sync_wait_pumping / PumpExecutor（docs/gaps.md §2.10）----------

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

TEST(task_moved_from_throws_not_segv) {
    // Calling on a moved-from Task used to be a null-pointer dereference (docs/gaps.md §4): all four entry
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
