// L4: Task<T> 惰性协程原语与 sync_wait（见 docs/03-concurrency.md）
#pragma once

#include <condition_variable>
#include <coroutine>
#include <exception>
#include <mutex>
#include <utility>
#include <variant>

namespace lights3 {

// sync_wait 的完成事件：协程可能在任意线程完成
class SyncWaitEvent {
public:
    void set() {
        {
            std::lock_guard lk(m_);
            done_ = true;
        }
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

    struct FinalAwaiter {
        bool await_ready() const noexcept { return false; }
        template <class P>
        std::coroutine_handle<> await_suspend(std::coroutine_handle<P> h) noexcept {
            auto& p = h.promise();
            if (p.continuation) return p.continuation;  // 对称转移回调用方
            if (p.event) p.event->set();                // 顶层 sync_wait
            return std::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };

    std::suspend_always initial_suspend() noexcept { return {}; }  // 惰性启动
    FinalAwaiter final_suspend() noexcept { return {}; }
};

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
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> cont) noexcept {
            h.promise().continuation = cont;
            return h;
        }
        T await_resume() {
            auto& r = h.promise().result;
            if (r.index() == 2) std::rethrow_exception(std::get<2>(r));
            return std::move(std::get<1>(r));
        }
    };
    Awaiter operator co_await() && noexcept { return {h_}; }

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
        std::coroutine_handle<> await_suspend(std::coroutine_handle<> cont) noexcept {
            h.promise().continuation = cont;
            return h;
        }
        void await_resume() {
            if (h.promise().error) std::rethrow_exception(h.promise().error);
        }
    };
    Awaiter operator co_await() && noexcept { return {h_}; }

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

}  // namespace lights3
