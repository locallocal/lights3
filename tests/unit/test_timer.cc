// TimerQueue 单测（docs/concurrency.md §2/§5；docs/todo.md §5.3 首批补齐）：
// 触发、到期顺序、撤销语义（未触发 true / 已触发或不存在 false）、析构不悬挂、
// 更早条目插队唤醒。时序断言只用宽松上界（cv + 超时等待），避免慢机 flake。
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <vector>

#include "core/timer.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace std::chrono_literals;

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
