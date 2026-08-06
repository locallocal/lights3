// TimerQueue 单测（docs/concurrency.md §2/§5；docs/todo.md §5.3 首批补齐）：
// 触发、到期顺序、撤销语义（未触发 true / 已触发或不存在 false）、析构不悬挂、
// 更早条目插队唤醒。时序断言只用宽松上界（cv + 超时等待），避免慢机 flake。
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
        // 持锁通知：等待方谓词满足即可能析构本对象，锁外 notify 会用已析构的 cv
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
    CHECK(TimerQueue::Clock::now() - t0 >= 20ms);  // 不得早触发
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
    // 远期 deadline（10s）给 add 与 cancel 之间留足调度余量（慢机/tsan 不 flake）；
    // cancel 返回 true 即证明条目在触发前被移除，之后必不执行——无需等待验证
    TimerQueue q;
    std::atomic<bool> fired{false};
    auto id = q.add(std::chrono::seconds(10), [&] { fired = true; });
    CHECK(q.cancel(id));   // 未触发：撤销成功
    CHECK(!q.cancel(id));  // 已不存在：false
    CHECK(!fired.load());
}

TEST(timer_cancel_waits_for_running_callback) {
    // cancel 对正在执行的回调阻塞等待：返回后回调必已退出，捕获的资源可安全析构
    TimerQueue q;
    Signal started;
    std::atomic<bool> release{false};
    std::atomic<bool> done{false};
    auto id = q.add(1ms, [&] {
        started.hit();
        while (!release.load()) std::this_thread::yield();
        done = true;
    });
    CHECK(started.wait_for_count(1, 5000ms));  // 回调已在执行
    release = true;                            // 放行后 cancel 须等到 done 置位才返回
    CHECK(!q.cancel(id));                      // 已触发语义 false，但阻塞至回调返回
    CHECK(done.load());
}

TEST(timer_cancel_from_callback_no_deadlock) {
    // 定时器线程上（回调内）自撤销不等待自己——防自死锁
    TimerQueue q;
    Signal s;
    TimerQueue::Id id = 0;
    std::mutex idm;
    {
        std::lock_guard lk(idm);
        id = q.add(1ms, [&] {
            std::lock_guard lk2(idm);
            CHECK(!q.cancel(id));  // 自撤销：立即返回 false，不死锁
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
    CHECK(!q.cancel(id));  // 已触发：语义为 false
}

TEST(timer_destructor_with_pending_items_returns) {
    // 析构不等未到期条目：立即返回且不触发回调
    std::atomic<bool> fired{false};
    {
        TimerQueue q;
        q.add(std::chrono::seconds(3600), [&] { fired = true; });
    }
    CHECK(!fired.load());
}

TEST(timer_add_earlier_item_preempts_wait) {
    // loop 正等远期条目时插入更早条目：cv 通知路径须及时唤醒
    TimerQueue q;
    Signal s;
    q.add(std::chrono::seconds(3600), [&] {});
    q.add(20ms, [&] { s.hit(); });
    CHECK(s.wait_for_count(1, 5000ms));
}

TEST(timer_callback_exception_does_not_wedge_cancel) {
    // gaps §2.2：回调抛出时 running_id_ 复位与 notify 必须仍然执行，
    // 否则 cancel(该 id) 永久阻塞（它是各后端关停路径的第一步）
    TimerQueue q;
    Signal s;
    auto id = q.add(1ms, [&] {
        s.hit();
        throw std::runtime_error("timer callback failure");
    });
    CHECK(s.wait_for_count(1, 5000ms));
    CHECK(!q.cancel(id));  // 已触发：立即返回 false，而非挂死
    // 队列线程仍存活，后续条目照常触发
    Signal s2;
    q.add(1ms, [&] { s2.hit(); });
    CHECK(s2.wait_for_count(1, 5000ms));
}

// ---------- 回调线程与调度线程分离（gaps §3.2）----------

TEST(slow_callback_does_not_stall_deadline_tracking) {
    // 取消回调会就地展开被取消的协程链，是有界但非零的工作。它必须跑在专用回调
    // 线程上：跑在调度线程上，期间到期的其他定时器连"到期判定"都做不了
    TimerQueue q;
    Signal slow_started;
    q.add(10ms, [&] {
        slow_started.hit();
        std::this_thread::sleep_for(300ms);
    });
    Id2 second = q.add(40ms, [] {});
    CHECK(slow_started.wait_for_count(1, 2s));
    std::this_thread::sleep_for(120ms);  // 早已过第二项的到期时刻
    // 慢回调仍在跑（300ms 未满）。此时第二项若还留在待触发表里，cancel 会返回
    // true——说明调度线程被回调卡住了。返回 false = 已被按时摘出，只是排队待执行
    CHECK(!q.cancel(second));
}

TEST(cancel_waits_for_due_but_unstarted_callback) {
    // 已到期、还排在回调队列里（尚未开始执行）的项：cancel 必须等它收敛，
    // 否则调用方会去析构回调仍要访问的资源
    TimerQueue q;
    Signal first_started;
    std::atomic<bool> second_ran{false};
    q.add(10ms, [&] {
        first_started.hit();
        std::this_thread::sleep_for(200ms);
    });
    Id2 second = q.add(20ms, [&] { second_ran = true; });
    CHECK(first_started.wait_for_count(1, 2s));
    std::this_thread::sleep_for(60ms);  // 第二项已到期，排在慢回调之后
    CHECK(!q.cancel(second));  // 返回 false = 已触发
    CHECK(second_ran.load());  // 且返回时确已执行完毕
}
