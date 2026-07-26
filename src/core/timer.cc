#include "core/timer.h"

namespace lights3 {

TimerQueue::TimerQueue() : thread_([this] { loop(); }) {}

TimerQueue::~TimerQueue() {
    {
        std::lock_guard lk(m_);
        stopping_ = true;
    }
    cv_.notify_all();
    thread_.join();
}

TimerQueue& TimerQueue::instance() {
    static TimerQueue q;
    return q;
}

TimerQueue::Id TimerQueue::add(Clock::duration delay, std::function<void()> fn) {
    std::lock_guard lk(m_);
    Id id = ++next_id_;
    auto deadline = Clock::now() + delay;
    items_.emplace(std::make_pair(deadline, id), std::move(fn));
    deadlines_.emplace(id, deadline);
    cv_.notify_all();  // 新条目可能成为最早到期者
    return id;
}

bool TimerQueue::cancel(Id id) {
    std::unique_lock lk(m_);
    auto it = deadlines_.find(id);
    if (it != deadlines_.end()) {
        items_.erase({it->second, id});
        deadlines_.erase(it);
        return true;
    }
    // 已触发：若正在执行则等它返回（回调内自撤销除外——等自己必死锁），
    // 调用方由此获得"cancel 返回后回调必不再运行"的析构安全保证（见头注）。
    // id 0 非法（分配从 1 起，也是 running_id_ 的空闲值），直接 no-op
    if (id != 0 && running_id_ == id && std::this_thread::get_id() != thread_.get_id())
        done_cv_.wait(lk, [&] { return running_id_ != id; });
    return false;
}

void TimerQueue::loop() {
    std::unique_lock lk(m_);
    for (;;) {
        if (stopping_) return;
        if (items_.empty()) {
            cv_.wait(lk);
            continue;
        }
        auto first = items_.begin();
        // deadline 必须按值取出：wait_until 持引用等待，等待期间锁已释放，
        // 并发 cancel() 删掉该节点后醒来重比时间会读已释放内存
        auto deadline = first->first.first;
        if (deadline > Clock::now()) {
            cv_.wait_until(lk, deadline);
            continue;
        }
        auto fn = std::move(first->second);
        running_id_ = first->first.second;
        deadlines_.erase(first->first.second);
        items_.erase(first);
        lk.unlock();
        fn();  // 锁外执行；期间的 cancel(id) 阻塞至此处返回（语义仍为 false"已触发"）
        lk.lock();
        running_id_ = 0;
        done_cv_.notify_all();
    }
}

}  // namespace lights3
