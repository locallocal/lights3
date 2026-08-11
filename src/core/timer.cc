#include "core/timer.h"

#include "core/log.h"

namespace lights3 {

TimerQueue::TimerQueue() : thread_([this] { loop(); }), fire_thread_([this] { fire_loop(); }) {}

TimerQueue::~TimerQueue() {
    {
        std::lock_guard lk(m_);
        stopping_ = true;
        // 进程收尾：已到期未执行的回调整体丢弃（对象正在析构，跑它们只会碰到
        // 已亡状态）。等在 cancel() 里的调用方随之放行
        due_.clear();
    }
    cv_.notify_all();
    fire_cv_.notify_all();
    done_cv_.notify_all();
    thread_.join();
    fire_thread_.join();
}

TimerQueue& TimerQueue::instance() {
    static TimerQueue q;
    return q;
}

TimerQueue::Id TimerQueue::add(Clock::duration delay, std::function<void()> fn) {
    Id id = 0;
    bool wake = false;
    {
        std::lock_guard lk(m_);
        // 停机后拒收（docs/gaps.md §7）：此前返回"有效但永不触发"的 id，析构期
        // 竞态里排查者会盯着一个永远不会响的定时器。0 与 cancel(0) 的 no-op
        // 约定闭环，调用方无需感知
        if (stopping_) {
            LOG_WARN("TimerQueue: add() after shutdown, timer dropped");
            return 0;
        }
        id = ++next_id_;
        auto deadline = Clock::now() + delay;
        items_.emplace(std::make_pair(deadline, id), std::move(fn));
        deadlines_.emplace(id, deadline);
        // 只有成为最早到期者才需要叫醒调度线程重算等待时长；其余情况它醒来也只会
        // 按原 deadline 继续睡（docs/gaps.md §4：此前每次 add 都 notify_all）
        wake = items_.begin()->first == std::make_pair(deadline, id);
    }
    if (wake) cv_.notify_one();  // 锁外唤醒：持锁 notify 会让被唤线程立刻撞上锁
    return id;
}

bool TimerQueue::pending_locked(Id id) const {
    if (running_id_ == id) return true;
    for (auto& item : due_)
        if (item.id == id) return true;
    return false;
}

size_t TimerQueue::exec_bucket(Clock::duration d) {
    using namespace std::chrono;
    if (d < 10ms) return 0;
    if (d < 100ms) return 1;
    if (d < 1s) return 2;
    if (d < 10s) return 3;
    return 4;
}

TimerQueue::Stats TimerQueue::stats() const {
    Stats st;
    {
        std::lock_guard lk(m_);
        st.pending = items_.size();
        st.due = due_.size();
        // 队头滞后：正在执行的回调优先（它到期最早），否则看待执行队头
        Clock::time_point head{};
        if (running_id_ != 0) head = running_deadline_;
        else if (!due_.empty()) head = due_.front().deadline;
        if (head != Clock::time_point{}) {
            auto lag = Clock::now() - head;
            if (lag.count() > 0) st.lag_seconds = std::chrono::duration<double>(lag).count();
        }
    }
    st.fired = fired_.load(std::memory_order_relaxed);
    st.slow = slow_.load(std::memory_order_relaxed);
    st.exec_sum_us = exec_sum_us_.load(std::memory_order_relaxed);
    for (size_t i = 0; i < kExecBuckets; ++i)
        st.exec_hist[i] = exec_hist_[i].load(std::memory_order_relaxed);
    return st;
}

bool TimerQueue::cancel(Id id) {
    std::unique_lock lk(m_);
    auto it = deadlines_.find(id);
    if (it != deadlines_.end()) {
        items_.erase({it->second, id});
        deadlines_.erase(it);
        return true;
    }
    // 已触发：若还在待执行队列里或正在执行，等它收敛（回调线程上自撤销除外——
    // 等自己必死锁），调用方由此获得"cancel 返回后回调必不再运行"的析构安全保证
    //（见头注）。id 0 非法（分配从 1 起，也是 running_id_ 的空闲值），直接 no-op
    if (id != 0 && std::this_thread::get_id() != fire_thread_.get_id())
        done_cv_.wait(lk, [&] { return !pending_locked(id); });
    return false;
}

// 调度线程：只做到期判定，到期项交给回调线程。这里绝不执行回调——回调可能是
// request_cancel，会就地展开整条被取消的协程链，跑在本线程上即全进程定时器停摆
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
        Id id = first->first.second;
        due_.push_back({id, deadline, std::move(first->second)});
        deadlines_.erase(id);
        items_.erase(first);
        fire_cv_.notify_one();
    }
}

void TimerQueue::fire_loop() {
    std::unique_lock lk(m_);
    for (;;) {
        fire_cv_.wait(lk, [&] { return stopping_ || !due_.empty(); });
        if (due_.empty()) {
            if (stopping_) return;
            continue;
        }
        DueItem item = std::move(due_.front());
        due_.pop_front();
        running_id_ = item.id;
        running_deadline_ = item.deadline;
        lk.unlock();
        // 锁外执行；期间的 cancel(id) 阻塞至此处返回（语义仍为 false"已触发"）。
        // 异常防线：回调抛出若逃逸线程函数即 terminate；且必须保证 running_id_
        // 复位 + notify 在任何路径都执行，否则 cancel(该 id) 永久阻塞——而 cancel
        // 正是各后端关停路径的第一步
        auto t0 = Clock::now();
        try {
            item.fn();
        } catch (const std::exception& e) {
            LOG_ERROR("TimerQueue: callback threw: {}", e.what());
        } catch (...) {
            LOG_ERROR("TimerQueue: callback threw unknown exception");
        }
        // 耗时记账（docs/gaps.md §7）：回调串行，慢回调直接推迟后续定时器——
        // 超过 1s 除进直方图外单独点名，"定时器被堵了 3 秒"从此有日志可查
        auto elapsed = Clock::now() - t0;
        auto us = std::chrono::duration_cast<std::chrono::microseconds>(elapsed).count();
        exec_hist_[exec_bucket(elapsed)].fetch_add(1, std::memory_order_relaxed);
        exec_sum_us_.fetch_add(static_cast<uint64_t>(us), std::memory_order_relaxed);
        fired_.fetch_add(1, std::memory_order_relaxed);
        if (elapsed >= std::chrono::seconds(1)) {
            slow_.fetch_add(1, std::memory_order_relaxed);
            LOG_WARN("TimerQueue: callback took {:.3f}s, delaying subsequent timers",
                     std::chrono::duration<double>(elapsed).count());
        }
        lk.lock();
        running_id_ = 0;
        done_cv_.notify_all();
    }
}

}  // namespace lights3
