#include "core/thread_pool.h"

#include <stdexcept>

namespace lights3 {

using Clock = std::chrono::steady_clock;

ThreadPool::ThreadPool(size_t threads, size_t queue_capacity)
    : capacity_(queue_capacity ? queue_capacity : 1) {
    if (threads == 0) threads = 1;
    workers_.reserve(threads);
    for (size_t i = 0; i < threads; ++i)
        workers_.emplace_back([this] { worker_loop(); });
}

ThreadPool::~ThreadPool() { join(); }

void ThreadPool::post(std::function<void()> fn) {
    {
        std::lock_guard lk(m_);
        if (stopping_) throw std::runtime_error("ThreadPool: post after join");
        queue_.push_back({std::move(fn), Clock::now()});
    }
    cv_.notify_one();
}

void ThreadPool::enqueue_bounded(std::function<void()> fn) {
    {
        std::lock_guard lk(m_);
        if (stopping_) throw std::runtime_error("ThreadPool: schedule after join");
        if (queue_.size() >= capacity_)
            backlog_.push_back({std::move(fn), Clock::now()});
        else
            queue_.push_back({std::move(fn), Clock::now()});
    }
    cv_.notify_one();
}

void ThreadPool::join() {
    {
        std::lock_guard lk(m_);
        if (stopping_) return;
        stopping_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_)
        if (t.joinable()) t.join();
}

ThreadPool::Stats ThreadPool::stats() const {
    std::lock_guard lk(m_);
    return {queue_.size(), backlog_.size(), completed_, wait_hist_};
}

size_t ThreadPool::wait_bucket(Clock::duration d) {
    using namespace std::chrono;
    if (d < 1ms) return 0;
    if (d < 10ms) return 1;
    if (d < 100ms) return 2;
    if (d < 1s) return 3;
    return 4;
}

void ThreadPool::worker_loop() {
    for (;;) {
        Item item;
        {
            std::unique_lock lk(m_);
            cv_.wait(lk, [&] { return stopping_ || !queue_.empty() || !backlog_.empty(); });
            if (queue_.empty() && backlog_.empty()) return;  // stopping 且已排空
            if (queue_.empty()) {
                queue_.push_back(std::move(backlog_.front()));
                backlog_.pop_front();
            }
            item = std::move(queue_.front());
            queue_.pop_front();
            // 腾出的空位放行一个背压等待者（保序：等待列表也是 FIFO）
            if (!backlog_.empty() && queue_.size() < capacity_) {
                queue_.push_back(std::move(backlog_.front()));
                backlog_.pop_front();
            }
            ++wait_hist_[wait_bucket(Clock::now() - item.enqueued)];
        }
        item.fn();
        {
            std::lock_guard lk(m_);
            ++completed_;
        }
    }
}

bool ThreadPool::ScheduleAwaiter::await_suspend(std::coroutine_handle<> h) {
    slot = std::make_shared<Slot>();
    slot->h = h;
    // 局部持有一切后续要用的东西：取消回调注册成功后，协程随时可能在
    // 别的线程恢复并销毁本 awaiter（this 不再可用）
    auto s = slot;
    ThreadPool& p = pool;
    CancelToken tok = token;

    tok.on_cancel_publish(
        [s] {
            if (!s->claimed.exchange(true, std::memory_order_acq_rel)) {
                s->cancelled = true;
                s->h.resume();  // 在取消发起线程上以 OperationCancelled resume
            }
        },
        s->reg_id, s->cancel_state);
    // 对已取消的 token 不会注册回调；补检查覆盖注册前已取消的竞态
    if (tok.cancelled() && !s->claimed.exchange(true, std::memory_order_acq_rel)) {
        s->cancelled = true;
        return false;  // 不挂起，await_resume 就地抛出
    }
    try {
        p.enqueue_bounded([s] {
            if (!s->claimed.exchange(true, std::memory_order_acq_rel)) s->h.resume();
        });
    } catch (...) {
        // join 后 schedule：先认领再抛，堵住取消回调的二次 resume；
        // 若已被取消回调认领则由它 resume，这里按已挂起处理
        if (s->claimed.exchange(true, std::memory_order_acq_rel)) return true;
        throw;  // await_suspend 抛出 → 协程未挂起，异常在 co_await 处浮出
    }
    return true;
}

}  // namespace lights3
