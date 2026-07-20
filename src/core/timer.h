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

    // 撤销未触发的定时器；已触发/不存在返回 false。不等待正在执行的回调
    bool cancel(Id id);

private:
    void loop();

    std::mutex m_;
    std::condition_variable cv_;
    // 按 (到期时间, id) 排序的待触发表；deadlines_ 提供按 id 反查
    std::map<std::pair<Clock::time_point, Id>, std::function<void()>> items_;
    std::map<Id, Clock::time_point> deadlines_;
    Id next_id_ = 0;
    bool stopping_ = false;
    std::thread thread_;
};

}  // namespace lights3
