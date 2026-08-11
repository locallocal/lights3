// L4: 进程级定时器线程；with_timeout 等超时原语的底座（docs/concurrency.md §2/§5）
#pragma once

#include <array>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <deque>
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

    // 回调执行时长直方图桶：<10ms <100ms <1s <10s ≥10s（docs/gaps.md §7）。
    // 回调串行执行，慢回调直接推迟后续定时器——桶界因此比线程池的等待桶更粗
    static constexpr size_t kExecBuckets = 5;

    struct Stats {
        size_t pending = 0;        // 未到期定时器数
        size_t due = 0;            // 已到期、还排在回调线程前面的数量
        uint64_t fired = 0;        // 已执行完的回调总数
        uint64_t slow = 0;         // 超过 1s 的回调数（每次伴随一条 WARN 日志）
        std::array<uint64_t, kExecBuckets> exec_hist{};
        uint64_t exec_sum_us = 0;  // 回调执行时长累计（微秒）
        // 队头滞后：最早的已到期/在执行回调已经迟了多久（秒，0 = 无积压）。
        // "定时器被某个回调堵了 3 秒"直接读这个数
        double lag_seconds = 0;
    };
    Stats stats() const;

    // delay 后调用 fn。fn 在**专用回调线程**上执行，与到期判定的调度线程分离
    // （docs/gaps.md §3.2）：request_cancel 会就地展开被取消的协程链，跑在调度
    // 线程上会让全进程定时器在展开期间停摆。回调之间仍是串行的，单个慢回调会
    // 推迟后续回调（但不再推迟到期判定），因此 fn 仍应避免阻塞 IO。
    // 停机后（析构已开始）返回 0——"有效但永不触发"的 id 是排查陷阱
    // （docs/gaps.md §7）；cancel(0) 恒为安全 no-op，调用方无需区分
    Id add(Clock::duration delay, std::function<void()> fn);

    // 撤销定时器：未触发返回 true；已触发/不存在返回 false。若该回调**已到期但尚未
    // 执行完**（在待执行队列里或正在执行），阻塞等它收敛——调用方 cancel 返回后即可
    // 安全析构回调捕获的资源，无需自备"已触发未执行"窗口的生命期守卫。回调线程上
    // （回调内）自撤销不等待（防自死锁）。注意：回调内不得持有 cancel 调用方等待
    // 期间所持的锁（锁序约束同一般 cv 等待）
    bool cancel(Id id);

private:
    void loop();       // 调度线程：只做到期判定与出队
    void fire_loop();  // 回调线程：串行执行到期回调
    bool pending_locked(Id id) const;
    static size_t exec_bucket(Clock::duration d);

    mutable std::mutex m_;
    std::condition_variable cv_;       // 调度线程
    std::condition_variable fire_cv_;  // 回调线程
    std::condition_variable done_cv_;  // 回调执行完毕的通知（cancel 阻塞等待用）
    // 按 (到期时间, id) 排序的待触发表；deadlines_ 提供按 id 反查
    std::map<std::pair<Clock::time_point, Id>, std::function<void()>> items_;
    std::map<Id, Clock::time_point> deadlines_;
    // 已到期、待回调线程执行；deadline 随行携带，供 stats() 计算队头滞后
    struct DueItem {
        Id id;
        Clock::time_point deadline;
        std::function<void()> fn;
    };
    std::deque<DueItem> due_;
    Id next_id_ = 0;
    Id running_id_ = 0;  // 正在执行的回调 id（0 = 无）
    bool stopping_ = false;
    // 回调耗时观测（docs/gaps.md §7）；原子存储，stats() 读取不与回调争锁
    std::array<std::atomic<uint64_t>, kExecBuckets> exec_hist_{};
    std::atomic<uint64_t> exec_sum_us_{0};
    std::atomic<uint64_t> fired_{0};
    std::atomic<uint64_t> slow_{0};
    // 正在执行回调的原定到期时刻（running_id_ != 0 时有效；guarded by m_）
    Clock::time_point running_deadline_{};
    std::thread thread_;
    std::thread fire_thread_;
};

}  // namespace lights3
