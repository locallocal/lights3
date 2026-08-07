#include "core/thread_pool.h"

#include <stdexcept>

#include "core/log.h"

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
        if (!stopping_) {
            cont_queue_.push_back({std::move(fn), Clock::now()});
            cv_.notify_one();
            return;
        }
    }
    // join 后的续体投递不可失败：消费方（FinalAwaiter::await_suspend、Permit 析构）
    // 都是 noexcept 上下文，抛出即 std::terminate。就地执行让残余请求链在调用方
    // 线程上收尾（schedule() 保留抛出语义，它有 co_await 承接点）
    LOG_ERROR("ThreadPool: post after join, running task inline on caller thread");
    fn();
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
    Stats st;
    {
        std::lock_guard lk(m_);
        st.queue_depth = cont_queue_.size() + queue_.size();
        st.backlogged = backlog_.size();
    }
    st.completed = completed_.load(std::memory_order_relaxed);
    for (size_t i = 0; i < kWaitBuckets; ++i)
        st.wait_hist[i] = wait_hist_[i].load(std::memory_order_relaxed);
    return st;
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
            cv_.wait(lk, [&] {
                return stopping_ || !cont_queue_.empty() || !queue_.empty() ||
                       !backlog_.empty();
            });
            if (cont_queue_.empty() && queue_.empty() && backlog_.empty())
                return;  // stopping 且已排空
            // 续体优先（§4）：它是让出过线程的既有工作，排在新阻塞任务后面
            // 会把"挂起-恢复"变成"挂起-排长队"
            if (!cont_queue_.empty()) {
                item = std::move(cont_queue_.front());
                cont_queue_.pop_front();
            } else {
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
            }
        }
        // 计时与记账都在锁外（§4：wait_hist_ 曾在持锁段内取时间）
        wait_hist_[wait_bucket(Clock::now() - item.enqueued)].fetch_add(
            1, std::memory_order_relaxed);
        // 异常防线：任务异常逃逸线程函数即 std::terminate（协程续体自身全捕获，
        // 这里兜的是裸 post 的阻塞任务）
        try {
            item.fn();
        } catch (const std::exception& e) {
            LOG_ERROR("ThreadPool: task threw: {}", e.what());
        } catch (...) {
            LOG_ERROR("ThreadPool: task threw unknown exception");
        }
        completed_.fetch_add(1, std::memory_order_relaxed);
    }
}

bool ThreadPool::ScheduleAwaiter::suspend_impl(std::coroutine_handle<> h) {
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
                // 就地 resume：恢复后立即从 await_resume 抛 OperationCancelled，
                // 做的只是异常展开（展开中不可能 co_await，遇到挂起点即交还本线程），
                // 是有界工作。**不能**改投线程池——取消最需要生效的场景正是池被占满，
                // 那时 post 的续体排在阻塞任务后面，取消变成空头支票（docs/gaps.md
                // §3.2 的建议按字面实现会死锁，见 test_concurrency 的相应用例）。
                // 触发线程侧的防线在 TimerQueue：回调跑在专用回调线程上，展开不会
                // 停摆到期判定
                s->h.resume();
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
