// L4: 协程版异步信号量（docs/concurrency.md §6）
// dispatch 入口的 max_inflight_requests 限流、multipart 分片并发控制。
// 超限的 acquire() 挂起排队（FIFO）而非拒绝。
#pragma once

#include <atomic>
#include <cassert>
#include <concepts>
#include <coroutine>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <utility>

#include "core/cancel.h"
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
    // 不 resume 则帧泄漏 + sync_wait 线程永久阻塞。持有方必须先排空在途请求再析构，
    // 或调用 close() 以取消语义唤醒全部等待者；违反契约时记 ERROR（debug 构建直接
    // 断言），泄漏帧是两害相权的保守选择
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

    // 等待者状态：放独立共享块（同 ThreadPool::ScheduleAwaiter::Slot）——release、
    // 取消回调、close 三方竞争 resume，败者仍持引用，不能指向可能已随协程恢复而
    // 销毁的 awaiter/协程帧
    struct Waiter {
        std::coroutine_handle<> h;
        std::atomic<bool> claimed{false};
        bool cancelled = false;  // 仅 claim 成功者写，resume 后同线程读
        std::atomic<uint64_t> reg_id{0};
        std::shared_ptr<detail::CancelState> cancel_state;
    };

    struct AcquireAwaiter {
        AsyncSemaphore& sem;
        CancelToken token;
        std::shared_ptr<Waiter> w;    // 仅在挂起或需要注册取消回调时分配
        bool immediate_fail = false;  // 未挂起即判定失败（已 close / 注册前已取消）

        bool await_ready() const noexcept { return false; }
        // 同 ThreadPool::schedule：未显式传 token 时从调用方 promise 继承
        template <class P>
        bool await_suspend(std::coroutine_handle<P> h) {
            if constexpr (requires {
                              { h.promise().cancel } -> std::convertible_to<CancelToken>;
                          })
                if (!token.valid()) token = h.promise().cancel;
            return suspend_impl(h);
        }

        bool suspend_impl(std::coroutine_handle<> h) {
            if (!token.valid()) {
                // 无取消 token：轻量路径，拿得到许可就不分配任何东西
                std::lock_guard lk(sem.m_);
                if (sem.closed_) {
                    immediate_fail = true;
                    return false;
                }
                if (sem.permits_ > 0) {
                    --sem.permits_;
                    return false;
                }
                w = std::make_shared<Waiter>();
                w->h = h;
                sem.waiters_.push_back(w);
                return true;
            }
            // 可取消路径：回调必须在入队**之前**注册完毕，否则"入队后、落位前"
            // 被 release 抢跑 resume，落位写与 await_resume 的读形成数据竞争
            w = std::make_shared<Waiter>();
            w->h = h;
            // 局部持有一切后续要用的东西（同 ThreadPool::ScheduleAwaiter）：取消
            // 回调注册成功的那一刻起，协程随时可能在别的线程被 cancel_waiter 恢复
            // 并连帧销毁——this/sem/token/w 全是帧内成员，注册后一概不可再碰
            auto* s = &sem;
            auto wp = w;
            CancelToken tok = token;
            tok.on_cancel_publish([s, wp] { s->cancel_waiter(wp); }, wp->reg_id,
                                  wp->cancel_state);
            bool pre_cancelled = tok.cancelled();  // 注册前已取消：回调不会被调用
            {
                std::lock_guard lk(s->m_);
                if (s->closed_ || pre_cancelled) {
                    // 认领成功即由本协程自行抛出；认领失败说明取消回调正要 resume 我们
                    if (wp->claimed.exchange(true, std::memory_order_acq_rel)) return true;
                    wp->cancelled = true;
                    return false;
                }
                if (s->permits_ > 0) {
                    if (wp->claimed.exchange(true, std::memory_order_acq_rel)) return true;
                    --s->permits_;
                    return false;
                }
                s->waiters_.push_back(wp);
            }
            return true;
        }

        Permit await_resume() {
            if (w) {
                if (w->cancel_state)
                    w->cancel_state->remove_callback(w->reg_id.load(std::memory_order_acquire));
                if (w->cancelled) throw OperationCancelled();
            } else if (immediate_fail) {
                throw OperationCancelled();
            }
            return Permit{&sem};
        }
    };

    // 用法：auto permit = co_await sem.acquire();
    // 取消（请求超时 / 断连 / 进程关停）时排队中的 acquire 以 OperationCancelled 浮出——
    // max_inflight_requests 的排队正是请求最可能长时间挂起的地方（docs/gaps.md §3.1）
    AcquireAwaiter acquire(CancelToken token = {}) { return {*this, std::move(token), {}, false}; }

    // 以取消语义唤醒全部等待者并拒绝后续 acquire。持有方在析构前调用即可满足
    // "析构时不得仍有等待者"的契约（docs/gaps.md §2.13 遗留项）
    void close() {
        std::deque<std::shared_ptr<Waiter>> ws;
        {
            std::lock_guard lk(m_);
            closed_ = true;
            ws.swap(waiters_);
        }
        for (auto& w : ws) wake_cancelled(w);
    }

    // 非阻塞获取：有许可即拿，无则返回 nullopt——绝不入等待队列。供"已持一份、
    // 还想再拿一份"的调用方（rados 双缓冲流水）使用：嵌套的阻塞 acquire 会在
    // 全员各持一份时互等死锁，try 语义把第二份降级为"拿得到就流水，拿不到就串行"
    std::optional<Permit> try_acquire() {
        std::lock_guard lk(m_);
        if (closed_ || permits_ <= 0) return std::nullopt;
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
        std::shared_ptr<Waiter> next;
        {
            std::lock_guard lk(m_);
            while (!waiters_.empty()) {
                auto w = std::move(waiters_.front());
                waiters_.pop_front();
                // 已被取消回调认领的等待者跳过（它自己会以 OperationCancelled 恢复），
                // 许可继续找下一个真等待者，找不到才回加计数
                if (!w->claimed.exchange(true, std::memory_order_acq_rel)) {
                    next = std::move(w);
                    break;
                }
            }
            if (!next) {
                ++permits_;
                return;
            }
        }
        if (exec_) exec_->post(next->h);
        else next->h.resume();
    }

    // 取消回调：认领 + 摘链 + 就地 resume。就地是刻意的——恢复后立即抛
    // OperationCancelled，只做异常展开这一有界工作；投给 exec_（生产上就是本 IO 池）
    // 会让"池被占满"这一最需要取消的场景等不到执行者（同 ThreadPool::ScheduleAwaiter）
    void cancel_waiter(const std::shared_ptr<Waiter>& w) {
        if (w->claimed.exchange(true, std::memory_order_acq_rel)) return;
        {
            std::lock_guard lk(m_);
            for (auto it = waiters_.begin(); it != waiters_.end(); ++it)
                if (*it == w) {
                    waiters_.erase(it);
                    break;
                }
        }
        w->cancelled = true;
        w->h.resume();
    }

    void wake_cancelled(const std::shared_ptr<Waiter>& w) {
        if (w->claimed.exchange(true, std::memory_order_acq_rel)) return;
        w->cancelled = true;
        w->h.resume();
    }

    mutable std::mutex m_;
    long permits_;
    IExecutor* exec_;
    bool closed_ = false;
    std::deque<std::shared_ptr<Waiter>> waiters_;
};

}  // namespace lights3
