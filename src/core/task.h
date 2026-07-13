// L4: Task<T> 惰性协程原语与 sync_wait / when_all / with_timeout（见 docs/03-concurrency.md）
#pragma once

#include <atomic>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <coroutine>
#include <exception>
#include <mutex>
#include <optional>
#include <type_traits>
#include <utility>
#include <variant>
#include <vector>

#include "core/cancel.h"
#include "core/executor.h"
#include "core/timer.h"

namespace lights3 {

// sync_wait 的完成事件：协程可能在任意线程完成
class SyncWaitEvent {
public:
    void set() {
        // 锁内 notify：等待方一醒来就可能析构本对象，notify 必须先于解锁完成
        std::lock_guard lk(m_);
        done_ = true;
        cv_.notify_all();
    }
    void wait() {
        std::unique_lock lk(m_);
        cv_.wait(lk, [&] { return done_; });
    }

private:
    std::mutex m_;
    std::condition_variable cv_;
    bool done_ = false;
};

namespace detail {

struct PromiseBase {
    std::coroutine_handle<> continuation;
    SyncWaitEvent* event = nullptr;
    // home executor（docs/03 §3）：设置后 final_suspend 把续体 post 过去，
    // 而非对称转移——协议逻辑由此回到 HTTP 执行环境；子任务在 co_await 时继承
    IExecutor* cont_executor = nullptr;

    struct FinalAwaiter {
        bool await_ready() const noexcept { return false; }
        template <class P>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<P> h) noexcept {
            auto& p = h.promise();
            if (p.continuation) {
                if (p.cont_executor) {
                    p.cont_executor->post(p.continuation);
                    return std::noop_coroutine();
                }
                return p.continuation;  // 对称转移回调用方
            }
            if (p.event) p.event->set();  // 顶层 sync_wait
            return std::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };

    std::suspend_always initial_suspend() noexcept { return {}; }  // 惰性启动
    FinalAwaiter final_suspend() noexcept { return {}; }
};

// co_await 时的公共挂起逻辑：记录续体并继承调用方的 home executor
template <class Promise>
std::coroutine_handle<> task_await_suspend(std::coroutine_handle<Promise> task,
                                           std::coroutine_handle<> cont,
                                           IExecutor* parent_executor) noexcept {
    auto& p = task.promise();
    p.continuation = cont;
    if (!p.cont_executor) p.cont_executor = parent_executor;
    return task;  // 对称转移启动被等待的任务
}

}  // namespace detail

template <class T>
class [[nodiscard]] Task {
public:
    struct promise_type : detail::PromiseBase {
        std::variant<std::monostate, T, std::exception_ptr> result;

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        template <class U>
        void return_value(U&& v) {
            result.template emplace<1>(std::forward<U>(v));
        }
        void unhandled_exception() { result.template emplace<2>(std::current_exception()); }
    };

    Task(Task&& o) noexcept : h_(std::exchange(o.h_, nullptr)) {}
    Task(const Task&) = delete;
    Task& operator=(Task&& o) noexcept {
        if (this != &o) {
            destroy();
            h_ = std::exchange(o.h_, nullptr);
        }
        return *this;
    }
    ~Task() { destroy(); }

    struct Awaiter {
        std::coroutine_handle<promise_type> h;
        bool await_ready() const noexcept { return false; }
        template <class P>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<P> cont) noexcept {
            IExecutor* parent = nullptr;
            if constexpr (std::derived_from<P, detail::PromiseBase>)
                parent = cont.promise().cont_executor;
            return detail::task_await_suspend(h, cont, parent);
        }
        T await_resume() {
            auto& r = h.promise().result;
            if (r.index() == 2) std::rethrow_exception(std::get<2>(r));
            return std::move(std::get<1>(r));
        }
    };
    Awaiter operator co_await() && noexcept { return {h_}; }

    // 绑定 home executor（driver 在链路起点调用）；完成后续体投递回 ex
    Task& via(IExecutor& ex) {
        h_.promise().cont_executor = &ex;
        return *this;
    }

    // sync_wait 专用：绑定事件并启动
    void start(SyncWaitEvent* ev) {
        h_.promise().event = ev;
        h_.resume();
    }
    T take_result() {
        auto& r = h_.promise().result;
        if (r.index() == 2) std::rethrow_exception(std::get<2>(r));
        return std::move(std::get<1>(r));
    }

private:
    explicit Task(std::coroutine_handle<promise_type> h) : h_(h) {}
    void destroy() {
        if (h_) h_.destroy();
    }
    std::coroutine_handle<promise_type> h_ = nullptr;
};

template <>
class [[nodiscard]] Task<void> {
public:
    struct promise_type : detail::PromiseBase {
        std::exception_ptr error;

        Task get_return_object() {
            return Task{std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        void return_void() {}
        void unhandled_exception() { error = std::current_exception(); }
    };

    Task(Task&& o) noexcept : h_(std::exchange(o.h_, nullptr)) {}
    Task(const Task&) = delete;
    Task& operator=(Task&& o) noexcept {
        if (this != &o) {
            destroy();
            h_ = std::exchange(o.h_, nullptr);
        }
        return *this;
    }
    ~Task() { destroy(); }

    struct Awaiter {
        std::coroutine_handle<promise_type> h;
        bool await_ready() const noexcept { return false; }
        template <class P>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<P> cont) noexcept {
            IExecutor* parent = nullptr;
            if constexpr (std::derived_from<P, detail::PromiseBase>)
                parent = cont.promise().cont_executor;
            return detail::task_await_suspend(h, cont, parent);
        }
        void await_resume() {
            if (h.promise().error) std::rethrow_exception(h.promise().error);
        }
    };
    Awaiter operator co_await() && noexcept { return {h_}; }

    Task& via(IExecutor& ex) {
        h_.promise().cont_executor = &ex;
        return *this;
    }

    void start(SyncWaitEvent* ev) {
        h_.promise().event = ev;
        h_.resume();
    }
    void take_result() {
        if (h_.promise().error) std::rethrow_exception(h_.promise().error);
    }

private:
    explicit Task(std::coroutine_handle<promise_type> h) : h_(h) {}
    void destroy() {
        if (h_) h_.destroy();
    }
    std::coroutine_handle<promise_type> h_ = nullptr;
};

// 阻塞当前线程直到协程完成；同步 HTTP 驱动与 L1 边界的桥
template <class T>
T sync_wait(Task<T> t) {
    SyncWaitEvent ev;
    t.start(&ev);
    ev.wait();
    return t.take_result();
}

inline void sync_wait(Task<void> t) {
    SyncWaitEvent ev;
    t.start(&ev);
    ev.wait();
    t.take_result();
}

// ---------- when_all：并发等待一组 Task（docs/03 §2/§6）----------

namespace detail {

// n 个 runner + 1 个 awaiter 共 n+1 票；最后一票的持有者恢复 when_all 协程
struct WhenAllLatch {
    std::atomic<size_t> pending;
    std::coroutine_handle<> continuation;
    explicit WhenAllLatch(size_t n) : pending(n + 1) {}
    void arrive() {
        // resume 之后不得再触碰 latch：when_all 帧可能已在 resume 内销毁
        if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1) continuation.resume();
    }
};

struct WhenAllAwaiter {
    WhenAllLatch& latch;
    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept {
        latch.continuation = h;
        // 先写 continuation 再投最后一票，保证 runner 侧看到 0 时续体已就绪
        if (latch.pending.fetch_sub(1, std::memory_order_acq_rel) == 1) return h;
        return std::noop_coroutine();
    }
    void await_resume() const noexcept {}
};

// 自销毁的包装协程：驱动一个子任务并向 latch 报到。
// 惰性启动 + 显式 resume：确保 ramp 完整返回、协程帧交接干净后才开跑，
// 否则协程迁到池线程并完成自毁时 ramp 可能仍在触碰帧（真实数据竞争）
struct WhenAllRunner {
    struct promise_type {
        WhenAllRunner get_return_object() {
            return {std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }  // 完成即自毁
        void return_void() {}
        void unhandled_exception() { std::terminate(); }  // 协程体内已全捕获
    };
    std::coroutine_handle<> h;
    void start() { h.resume(); }
};

template <class T>
WhenAllRunner when_all_run(Task<T> t, WhenAllLatch& latch, std::optional<T>& out,
                           std::exception_ptr& err) {
    try {
        out.emplace(co_await std::move(t));
    } catch (...) {
        err = std::current_exception();
    }
    latch.arrive();
}

inline WhenAllRunner when_all_run(Task<void> t, WhenAllLatch& latch, std::exception_ptr& err) {
    try {
        co_await std::move(t);
    } catch (...) {
        err = std::current_exception();
    }
    latch.arrive();
}

}  // namespace detail

// 等待全部完成后返回；有失败时在全部结束后重抛第一个异常（结果按输入顺序）
template <class T>
    requires(!std::is_void_v<T>)
Task<std::vector<T>> when_all(std::vector<Task<T>> tasks) {
    detail::WhenAllLatch latch(tasks.size());
    std::vector<std::optional<T>> results(tasks.size());
    std::vector<std::exception_ptr> errors(tasks.size());
    for (size_t i = 0; i < tasks.size(); ++i)
        detail::when_all_run(std::move(tasks[i]), latch, results[i], errors[i]).start();
    co_await detail::WhenAllAwaiter{latch};
    for (auto& e : errors)
        if (e) std::rethrow_exception(e);
    std::vector<T> out;
    out.reserve(results.size());
    for (auto& r : results) out.push_back(std::move(*r));
    co_return out;
}

inline Task<void> when_all(std::vector<Task<void>> tasks) {
    detail::WhenAllLatch latch(tasks.size());
    std::vector<std::exception_ptr> errors(tasks.size());
    for (size_t i = 0; i < tasks.size(); ++i)
        detail::when_all_run(std::move(tasks[i]), latch, errors[i]).start();
    co_await detail::WhenAllAwaiter{latch};
    for (auto& e : errors)
        if (e) std::rethrow_exception(e);
}

// ---------- with_timeout：协作式超时（docs/03 §2/§5）----------
// 到点仅触发 src.request_cancel()；task 须以 src.token() 构造并在挂起点/
// 长循环感知取消，超时表现为 OperationCancelled 从 task 内浮出。
// src 应为本次调用专用：与他人共享的 source 会在超时后殃及同请求的后续操作。
template <class T>
Task<T> with_timeout(Task<T> task, std::chrono::milliseconds timeout, CancelSource src) {
    auto& tq = TimerQueue::instance();
    auto id = tq.add(timeout, [src]() mutable { src.request_cancel(); });
    try {
        if constexpr (std::is_void_v<T>) {
            co_await std::move(task);
            tq.cancel(id);
        } else {
            T r = co_await std::move(task);
            tq.cancel(id);
            co_return r;
        }
    } catch (...) {
        tq.cancel(id);
        throw;
    }
}

}  // namespace lights3
