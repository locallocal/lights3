// docs/03 并发原语：when_all / with_timeout / 取消 / home executor / 背压与指标 / 信号量
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

// 在指定线程池上取到其某个 worker 的线程 id
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
    CHECK_EQ(completed.load(), 2);  // 异常不打断其余分片
}

// ---------- 取消 ----------

TEST(schedule_precancelled_throws) {
    ThreadPool pool(1);
    CancelSource src;
    src.request_cancel();
    auto t = [](ThreadPool& p, CancelToken tok) -> Task<int> {
        co_await p.schedule(std::move(tok));
        co_return 1;  // 不应到达
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
    pool.post([blocked] { blocked.wait(); });  // 占住唯一 worker

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
    std::this_thread::sleep_for(20ms);  // 让任务进入队列
    src.request_cancel();               // worker 仍被占住：取消路径 resume
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
    // 默认 token 永不取消
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
    }  // reg2 解除注册
    src.request_cancel();
    CHECK_EQ(fired.load(), 1);
    // 已取消后注册不回调
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
    // 模拟分块流式操作：每块之间回池并感知取消
    auto slow = [](ThreadPool& p, CancelToken tok) -> Task<int> {
        for (int i = 0; i < 1000; ++i) {
            co_await p.schedule(tok);
            std::this_thread::sleep_for(5ms);  // 池线程上的"阻塞段"
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
    CHECK(std::chrono::steady_clock::now() - begin < 2s);  // 远早于 1000*5ms
}

// ---------- home executor 切回（docs/03 §3）----------

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
        auto child_tid = co_await child(wp);  // 子任务继承 home executor
        co_return std::make_pair(child_tid, std::this_thread::get_id());
    };
    auto t = parent(work);
    t.via(home_exec);
    auto [child_tid, parent_tid] = sync_wait(std::move(t));
    CHECK(child_tid != home_id);      // 阻塞段跑在池线程
    CHECK_EQ(parent_tid, home_id);    // 续体被 post 回 home
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
        co_return std::this_thread::get_id();  // inline：续体留在池线程
    };
    auto task = t(pool);
    task.via(InlineExecutor::instance());
    CHECK(sync_wait(std::move(task)) != std::this_thread::get_id());
}

// ---------- 背压与指标（docs/03 §3）----------

TEST(bounded_queue_backpressure) {
    ThreadPool pool(1, /*queue_capacity=*/1);
    std::promise<void> gate;
    auto blocked = gate.get_future().share();
    pool.post([blocked] { blocked.wait(); });  // 占住唯一 worker

    std::atomic<int> done{0};
    auto t = [&](ThreadPool& p) -> Task<void> {
        co_await p.schedule();
        ++done;
    };
    std::vector<std::thread> waiters;
    for (int i = 0; i < 3; ++i)
        waiters.emplace_back([&] { sync_wait(t(pool)); });
    // 等 3 个任务都挂起：1 个进就绪队列，2 个被背压挡在等待列表
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
    CHECK_EQ(s.completed, uint64_t(4));  // gate 任务 + 3 个 schedule
    uint64_t hist_total = std::accumulate(s.wait_hist.begin(), s.wait_hist.end(), uint64_t(0));
    CHECK_EQ(hist_total, uint64_t(4));   // 每个出队任务都记了等待时长
}

// ---------- AsyncSemaphore（docs/03 §6）----------

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
    CHECK_EQ(sem.available(), 2L);  // 许可全部归还
    CHECK_EQ(sem.waiting(), size_t(0));
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
    CHECK_EQ(sem.available(), 1L);  // 异常路径同样归还
}
