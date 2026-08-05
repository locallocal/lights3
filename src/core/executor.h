// L4: 协程续体的投递抽象（见 docs/concurrency.md §3）
#pragma once

#include <condition_variable>
#include <coroutine>
#include <deque>
#include <mutex>

namespace lights3 {

struct IExecutor {
    virtual void post(std::coroutine_handle<> h) = 0;
    virtual ~IExecutor() = default;
};

// 就地 resume：同步驱动（thread-per-request）使用
struct InlineExecutor final : IExecutor {
    void post(std::coroutine_handle<> h) override { h.resume(); }
    static InlineExecutor& instance() {
        static InlineExecutor e;
        return e;
    }
};

// 同步驱动（thread-per-request/connection）的请求线程 executor（docs/gaps.md
// §2.10）：请求线程不再傻等在 sync_wait 里，而是经 sync_wait_pumping（task.h）
// 运行本队列；body reader 把阻塞读经 resume_on 切回请求线程执行，慢速客户端
// 堵住的是自己的连接线程，不占共享池线程。单请求单实例
class PumpExecutor final : public IExecutor {
public:
    // 锁内 notify（同 SyncWaitEvent）：run() 一返回本对象就可能析构，notify
    // 必须先于解锁完成，否则 notify 摸到已销毁的 cv
    void post(std::coroutine_handle<> h) override {
        std::lock_guard lk(m_);
        q_.push_back(h);
        cv_.notify_one();
    }

    // 顶层任务完成时在任意线程调用；唤醒并终结 run()
    void finish() {
        std::lock_guard lk(m_);
        done_ = true;
        cv_.notify_one();
    }

    // 请求线程：循环执行投递来的续体，直至 finish() 且队列排空
    void run() {
        for (;;) {
            std::coroutine_handle<> h;
            {
                std::unique_lock lk(m_);
                cv_.wait(lk, [&] { return !q_.empty() || done_; });
                if (q_.empty()) return;
                h = q_.front();
                q_.pop_front();
            }
            h.resume();
        }
    }

private:
    std::mutex m_;
    std::condition_variable cv_;
    std::deque<std::coroutine_handle<>> q_;
    bool done_ = false;
};

// co_await resume_on(ex)：把当前协程的后续执行切换到指定 executor
struct ResumeOnAwaiter {
    IExecutor& ex;
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) { ex.post(h); }
    void await_resume() const noexcept {}
};
inline ResumeOnAwaiter resume_on(IExecutor& ex) { return {ex}; }

}  // namespace lights3
