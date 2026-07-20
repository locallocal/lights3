// L4: 协程版异步信号量（docs/concurrency.md §6）
// dispatch 入口的 max_inflight_requests 限流、multipart 分片并发控制。
// 超限的 acquire() 挂起排队（FIFO）而非拒绝。
#pragma once

#include <coroutine>
#include <deque>
#include <mutex>
#include <utility>

#include "core/executor.h"

namespace lights3 {

class AsyncSemaphore {
public:
    // resume_executor：唤醒等待者时把续体投递到该 executor 执行；
    // 为空则在 release 调用栈上就地 resume——同步驱动大量排队时会形成
    // "完成一个请求→内联跑完下一个请求"的深递归，生产路径应传池 executor
    explicit AsyncSemaphore(long permits, IExecutor* resume_executor = nullptr)
        : permits_(permits), exec_(resume_executor) {}
    AsyncSemaphore(const AsyncSemaphore&) = delete;

    // RAII 许可：析构即归还（协程帧退出时自动释放，异常路径同样覆盖）
    class Permit {
    public:
        Permit() = default;
        explicit Permit(AsyncSemaphore* s) : sem_(s) {}
        Permit(Permit&& o) noexcept : sem_(std::exchange(o.sem_, nullptr)) {}
        Permit& operator=(Permit&& o) noexcept {
            if (this != &o) {
                release();
                sem_ = std::exchange(o.sem_, nullptr);
            }
            return *this;
        }
        Permit(const Permit&) = delete;
        ~Permit() { release(); }

        void release() {
            if (auto* s = std::exchange(sem_, nullptr)) s->release_one();
        }

    private:
        AsyncSemaphore* sem_ = nullptr;
    };

    struct AcquireAwaiter {
        AsyncSemaphore& sem;
        bool await_ready() const noexcept { return false; }
        bool await_suspend(std::coroutine_handle<> h) {
            std::lock_guard lk(sem.m_);
            if (sem.permits_ > 0) {
                --sem.permits_;
                return false;  // 有许可：不挂起
            }
            sem.waiters_.push_back(h);
            return true;
        }
        Permit await_resume() const noexcept { return Permit{&sem}; }
    };

    // 用法：auto permit = co_await sem.acquire();
    AcquireAwaiter acquire() { return {*this}; }

    long available() const {
        std::lock_guard lk(m_);
        return permits_;
    }
    size_t waiting() const {
        std::lock_guard lk(m_);
        return waiters_.size();
    }

private:
    void release_one() {
        std::coroutine_handle<> next;
        {
            std::lock_guard lk(m_);
            if (waiters_.empty()) {
                ++permits_;
                return;
            }
            next = waiters_.front();  // 许可直接移交队首等待者，计数不回加
            waiters_.pop_front();
        }
        if (exec_) exec_->post(next);
        else next.resume();
    }

    mutable std::mutex m_;
    long permits_;
    IExecutor* exec_;
    std::deque<std::coroutine_handle<>> waiters_;
};

}  // namespace lights3
