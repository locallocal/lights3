// L4: Task<T> 惰性协程原语与 sync_wait / when_all / with_timeout（见 docs/concurrency.md）
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
    // home executor（docs/concurrency.md §3）：设置后 final_suspend 把续体 post 过去，
    // 而非对称转移——协议逻辑由此回到 HTTP 执行环境；子任务在 co_await 时继承
    IExecutor* cont_executor = nullptr;
    // 取消 token（docs/concurrency.md §5，docs/gaps.md §3.1）：与 cont_executor 同样
    // 沿 co_await 链向下继承。请求入口用 Task::with_cancel() 挂上本请求的 token 之后，
    // 整条 L2/L3 协程链上的 co_await pool_->schedule() 会自动拿到它——挂起点因此可被
    // 取消唤醒，无需在 40+ 个调用点逐一显式传参
    CancelToken cancel;

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

// co_await 时的公共挂起逻辑：记录续体并继承调用方的 home executor 与取消 token
template <class Promise>
std::coroutine_handle<> task_await_suspend(std::coroutine_handle<Promise> task,
                                           std::coroutine_handle<> cont,
                                           IExecutor* parent_executor,
                                           const CancelToken& parent_cancel) noexcept {
    auto& p = task.promise();
    p.continuation = cont;
    if (!p.cont_executor) p.cont_executor = parent_executor;
    // 子任务自带 token（with_cancel 显式挂过）时不覆盖，否则继承调用方的
    if (!p.cancel.valid()) p.cancel = parent_cancel;
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
            CancelToken tok;
            if constexpr (std::derived_from<P, detail::PromiseBase>) {
                parent = cont.promise().cont_executor;
                tok = cont.promise().cancel;
            }
            return detail::task_await_suspend(h, cont, parent, tok);
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

    // 绑定取消 token（请求入口调用）；本任务及其 co_await 的所有子任务继承
    Task& with_cancel(CancelToken t) {
        h_.promise().cancel = std::move(t);
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
            CancelToken tok;
            if constexpr (std::derived_from<P, detail::PromiseBase>) {
                parent = cont.promise().cont_executor;
                tok = cont.promise().cancel;
            }
            return detail::task_await_suspend(h, cont, parent, tok);
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

    Task& with_cancel(CancelToken t) {
        h_.promise().cancel = std::move(t);
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

// ---------- sync_wait_pumping：请求线程边等边当 executor（docs/gaps.md §2.10）----------
// 与 sync_wait 的差异：等待期间在当前线程运行 ex 的队列，body reader 经
// resume_on(ex) 把阻塞读切回本线程执行。同步驱动（builtin/httplib）专用。

namespace detail {

// 自毁式包装协程：驱动顶层任务、把结果搬到调用方栈上，最后叫醒 pump 循环。
// out/err 先于 finish() 写入；finish() 之后不再触碰任何调用方状态
struct PumpRunner {
    struct promise_type {
        PumpRunner get_return_object() { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }  // 协程体内已全捕获
    };
};

template <class T>
PumpRunner pump_run(Task<T> t, PumpExecutor& ex, std::optional<T>& out,
                    std::exception_ptr& err) {
    try {
        out.emplace(co_await std::move(t));
    } catch (...) {
        err = std::current_exception();
    }
    ex.finish();
}

inline PumpRunner pump_run(Task<void> t, PumpExecutor& ex, std::exception_ptr& err) {
    try {
        co_await std::move(t);
    } catch (...) {
        err = std::current_exception();
    }
    ex.finish();
}

}  // namespace detail

template <class T>
T sync_wait_pumping(PumpExecutor& ex, Task<T> t) {
    std::optional<T> out;
    std::exception_ptr err;
    detail::pump_run(std::move(t), ex, out, err);
    ex.run();
    if (err) std::rethrow_exception(err);
    return std::move(*out);
}

inline void sync_wait_pumping(PumpExecutor& ex, Task<void> t) {
    std::exception_ptr err;
    detail::pump_run(std::move(t), ex, err);
    ex.run();
    if (err) std::rethrow_exception(err);
}

// ---------- when_all：并发等待一组 Task（docs/concurrency.md §2/§6）----------

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
    // runner 帧分配可能中途抛出；此时已起跑的 runner 仍引用本帧的 latch/results/
    // errors，必须补齐未起跑者的票并等它们收敛后才能让异常离开本协程
    std::exception_ptr spawn_err;
    size_t started = 0;
    for (; started < tasks.size(); ++started) {
        try {
            detail::when_all_run(std::move(tasks[started]), latch, results[started],
                                 errors[started])
                .start();
        } catch (...) {
            spawn_err = std::current_exception();
            break;
        }
    }
    if (spawn_err)
        // awaiter 的一票尚未投出，pending 不可能减到 0，无 resume 竞态
        latch.pending.fetch_sub(tasks.size() - started, std::memory_order_acq_rel);
    co_await detail::WhenAllAwaiter{latch};
    if (spawn_err) std::rethrow_exception(spawn_err);
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
    // 同上：启动中途抛出时先等已起跑 runner 收敛再重抛
    std::exception_ptr spawn_err;
    size_t started = 0;
    for (; started < tasks.size(); ++started) {
        try {
            detail::when_all_run(std::move(tasks[started]), latch, errors[started]).start();
        } catch (...) {
            spawn_err = std::current_exception();
            break;
        }
    }
    if (spawn_err)
        latch.pending.fetch_sub(tasks.size() - started, std::memory_order_acq_rel);
    co_await detail::WhenAllAwaiter{latch};
    if (spawn_err) std::rethrow_exception(spawn_err);
    for (auto& e : errors)
        if (e) std::rethrow_exception(e);
}

// ---------- with_timeout：协作式超时（docs/concurrency.md §2/§5）----------
// 到点仅触发 src.request_cancel()；task 由本函数挂上 src.token()（连同其
// co_await 的整条子任务链），超时表现为 OperationCancelled 从最近的可取消挂起点
// （pool.schedule / semaphore.acquire）浮出。已在池线程上执行的阻塞系统调用不被
// 抢占——协作式取消不追求抢占，等其自然返回后由下一个挂起点感知。
// src 应为本次调用专用：与他人共享的 source 会在超时后殃及同请求的后续操作。
// 需要"外部取消（断连/进程关停）也能打断"时，调用方把外部 token 的回调接到
// 同一个 src 上（见 S3Service::dispatch）。
template <class T>
Task<T> with_timeout(Task<T> task, std::chrono::milliseconds timeout, CancelSource src) {
    auto& tq = TimerQueue::instance();
    task.with_cancel(src.token());
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
