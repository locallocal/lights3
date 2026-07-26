// L4: 进程级定时器线程；with_timeout 等超时原语的底座（docs/concurrency.md §2/§5）
#pragma once

#include <chrono>
#include <condition_variable>
#include <cstdint>
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

    // delay 后在定时器线程上调用 fn；fn 须轻量（典型：request_cancel / executor post）
    Id add(Clock::duration delay, std::function<void()> fn);

    // 撤销定时器：未触发返回 true；已触发/不存在返回 false。若该回调**正在执行**，
    // 阻塞等它返回——调用方 cancel 返回后即可安全析构回调捕获的资源，无需自备
    // "已触发未执行"窗口的生命期守卫。定时器线程上（回调内）自撤销不等待（防自死锁）。
    // 注意：回调内不得持有 cancel 调用方等待期间所持的锁（锁序约束同一般 cv 等待）
    bool cancel(Id id);

private:
    void loop();

    std::mutex m_;
    std::condition_variable cv_;
    std::condition_variable done_cv_;  // 回调执行完毕的通知（cancel 阻塞等待用）
    // 按 (到期时间, id) 排序的待触发表；deadlines_ 提供按 id 反查
    std::map<std::pair<Clock::time_point, Id>, std::function<void()>> items_;
    std::map<Id, Clock::time_point> deadlines_;
    Id next_id_ = 0;
    Id running_id_ = 0;  // 正在执行的回调 id（0 = 无）
    bool stopping_ = false;
    std::thread thread_;
};

}  // namespace lights3
