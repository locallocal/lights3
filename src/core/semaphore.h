// L4: 协程版异步信号量（docs/concurrency.md §6）
// dispatch 入口的 max_inflight_requests 限流、multipart 分片并发控制。
// 超限的 acquire() 挂起排队（FIFO）而非拒绝。
#pragma once

#include <cassert>
#include <coroutine>
#include <deque>
#include <mutex>
#include <optional>
#include <utility>

#include "core/executor.h"
#include "core/log.h"

namespace lights3 {

class AsyncSemaphore {
public:
    // resume_executor：唤醒等待者时把续体投递到该 executor 执行；
    // 为空则在 release 调用栈上就地 resume——同步驱动大量排队时会形成
    // "完成一个请求→内联跑完下一个请求"的深递归，生产路径应传池 executor
    explicit AsyncSemaphore(long permits, IExecutor* resume_executor = nullptr)
        : permits_(permits), exec_(resume_executor) {}
    AsyncSemaphore(const AsyncSemaphore&) = delete;

    // 契约：析构时不得仍有等待者——resume 它们会拿到指向已亡信号量的 Permit（UAF），
    // 不 resume 则帧泄漏 + sync_wait 线程永久阻塞。持有方必须先排空在途请求再析构；
    // 违反契约时记 ERROR（debug 构建直接断言），泄漏帧是两害相权的保守选择
    ~AsyncSemaphore() {
        std::lock_guard lk(m_);
        if (!waiters_.empty())
            LOG_ERROR("AsyncSemaphore destroyed with {} waiter(s); frames leaked", waiters_.size());
        assert(waiters_.empty());
    }

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
        explicit operator bool() const { return sem_ != nullptr; }

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

    // 非阻塞获取：有许可即拿，无则返回 nullopt——绝不入等待队列。供"已持一份、
    // 还想再拿一份"的调用方（rados 双缓冲流水）使用：嵌套的阻塞 acquire 会在
    // 全员各持一份时互等死锁，try 语义把第二份降级为"拿得到就流水，拿不到就串行"
    std::optional<Permit> try_acquire() {
        std::lock_guard lk(m_);
        if (permits_ <= 0) return std::nullopt;
        --permits_;
        return Permit{this};
    }

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
