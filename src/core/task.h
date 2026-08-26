// L4: lazy Task<T> coroutine primitive plus sync_wait / when_all / with_timeout (see docs/concurrency.md)
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

// Completion event for sync_wait: the coroutine may finish on any thread
class SyncWaitEvent {
public:
    void set() {
        // Notify while holding the lock: the waiter may destroy this object as
        // soon as it wakes, so notify must complete before we unlock
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
    // Home executor (docs/concurrency.md §3): when set, final_suspend posts the
    // continuation there instead of doing a symmetric transfer — protocol logic thus
    // returns to the HTTP execution context; child tasks inherit it on co_await
    IExecutor* cont_executor = nullptr;
    // Cancellation token (docs/concurrency.md §5, docs/archive/gaps.md §3.1): inherited down
    // the co_await chain just like cont_executor. Once the request entry point attaches
    // this request's token via Task::with_cancel(), every co_await pool_->schedule()
    // along the whole L2/L3 coroutine chain picks it up automatically — suspension
    // points can thus be woken by cancellation without explicitly threading the token
    // through 40+ call sites
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
                return p.continuation;  // symmetric transfer back to the caller
            }
            if (p.event) p.event->set();  // top-level sync_wait
            return std::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };

    std::suspend_always initial_suspend() noexcept { return {}; }  // lazy start
    FinalAwaiter final_suspend() noexcept { return {}; }
};

// Common suspend logic for co_await: record the continuation and inherit the
// caller's home executor and cancellation token
template <class Promise>
std::coroutine_handle<> task_await_suspend(std::coroutine_handle<Promise> task,
                                           std::coroutine_handle<> cont,
                                           IExecutor* parent_executor,
                                           const CancelToken& parent_cancel) noexcept {
    auto& p = task.promise();
    p.continuation = cont;
    if (!p.cont_executor) p.cont_executor = parent_executor;
    // Do not override a token the child task already carries (explicitly attached
    // via with_cancel); otherwise inherit the caller's
    if (!p.cancel.valid()) p.cancel = parent_cancel;
    return task;  // symmetric transfer starts the awaited task
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
    // Moved-from guard (docs/archive/gaps.md §4): keeping using an empty handle is a
    // programming error; throwing is far more diagnosable than a null-pointer
    // dereference
    Awaiter operator co_await() && {
        check_valid("co_await");
        return {h_};
    }

    // Bind the home executor (called by the driver at the start of the chain);
    // on completion the continuation is posted back to ex
    Task& via(IExecutor& ex) {
        check_valid("via");
        h_.promise().cont_executor = &ex;
        return *this;
    }

    // Bind the cancellation token (called at the request entry point); this task
    // and all child tasks it co_awaits inherit it
    Task& with_cancel(CancelToken t) {
        check_valid("with_cancel");
        h_.promise().cancel = std::move(t);
        return *this;
    }

    // For sync_wait only: bind the event and start
    void start(SyncWaitEvent* ev) {
        check_valid("start");
        h_.promise().event = ev;
        h_.resume();
    }
    T take_result() {
        check_valid("take_result");
        auto& r = h_.promise().result;
        if (r.index() == 2) std::rethrow_exception(std::get<2>(r));
        return std::move(std::get<1>(r));
    }

private:
    explicit Task(std::coroutine_handle<promise_type> h) : h_(h) {}
    void check_valid(const char* op) const {
        if (!h_)
            throw std::logic_error(std::string("Task: ") + op + " on a moved-from task");
    }
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
    // Moved-from guard, same as the primary template (docs/archive/gaps.md §4)
    Awaiter operator co_await() && {
        check_valid("co_await");
        return {h_};
    }

    Task& via(IExecutor& ex) {
        check_valid("via");
        h_.promise().cont_executor = &ex;
        return *this;
    }

    Task& with_cancel(CancelToken t) {
        check_valid("with_cancel");
        h_.promise().cancel = std::move(t);
        return *this;
    }

    void start(SyncWaitEvent* ev) {
        check_valid("start");
        h_.promise().event = ev;
        h_.resume();
    }
    void take_result() {
        check_valid("take_result");
        if (h_.promise().error) std::rethrow_exception(h_.promise().error);
    }

private:
    explicit Task(std::coroutine_handle<promise_type> h) : h_(h) {}
    void check_valid(const char* op) const {
        if (!h_)
            throw std::logic_error(std::string("Task: ") + op + " on a moved-from task");
    }
    void destroy() {
        if (h_) h_.destroy();
    }
    std::coroutine_handle<promise_type> h_ = nullptr;
};

// Block the current thread until the coroutine completes; the bridge between the
// synchronous HTTP drivers and the L1 boundary
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

// ---------- sync_wait_pumping: the request thread acts as an executor while it waits (docs/archive/gaps.md §2.10) ----------
// Difference from sync_wait: while waiting, ex's queue is run on the current thread,
// and the body reader switches blocking reads back onto this thread via
// resume_on(ex). For the synchronous drivers (builtin/httplib) only.

namespace detail {

// Self-destroying wrapper coroutine: drives the top-level task, moves the result
// onto the caller's stack, and finally wakes the pump loop. out/err are written
// before finish(); after finish() no caller state is touched anymore
struct PumpRunner {
    struct promise_type {
        PumpRunner get_return_object() { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }  // coroutine body catches everything
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

// ---------- when_all: concurrently await a set of Tasks (docs/concurrency.md §2/§6) ----------

namespace detail {

// n runners + 1 awaiter make n+1 votes; whoever casts the last vote resumes the
// when_all coroutine
struct WhenAllLatch {
    std::atomic<size_t> pending;
    std::coroutine_handle<> continuation;
    explicit WhenAllLatch(size_t n) : pending(n + 1) {}
    void arrive() {
        // Do not touch the latch after resume: the when_all frame may already have
        // been destroyed inside resume
        if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1) continuation.resume();
    }
};

struct WhenAllAwaiter {
    WhenAllLatch& latch;
    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept {
        latch.continuation = h;
        // Write the continuation before casting the last vote, so the runner side
        // sees a ready continuation when the count reaches 0
        if (latch.pending.fetch_sub(1, std::memory_order_acq_rel) == 1) return h;
        return std::noop_coroutine();
    }
    void await_resume() const noexcept {}
};

// Self-destroying wrapper coroutine: drives one child task and reports to the latch.
// Lazy start + explicit resume: guarantees the ramp has fully returned and the
// coroutine frame handoff is clean before it runs; otherwise, when the coroutine
// migrates to a pool thread and self-destructs, the ramp may still be touching the
// frame (a real data race)
struct WhenAllRunner {
    struct promise_type {
        WhenAllRunner get_return_object() {
            return {std::coroutine_handle<promise_type>::from_promise(*this)};
        }
        std::suspend_always initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }  // self-destructs on completion
        void return_void() {}
        void unhandled_exception() { std::terminate(); }  // coroutine body catches everything
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

// Returns after all tasks complete; on failure, rethrows the first exception once
// everything has finished (results are in input order)
template <class T>
    requires(!std::is_void_v<T>)
Task<std::vector<T>> when_all(std::vector<Task<T>> tasks) {
    detail::WhenAllLatch latch(tasks.size());
    std::vector<std::optional<T>> results(tasks.size());
    std::vector<std::exception_ptr> errors(tasks.size());
    // Runner frame allocation may throw partway through; runners already started
    // still reference this frame's latch/results/errors, so we must cast the votes
    // of the never-started ones and wait for the started ones to settle before
    // letting the exception leave this coroutine
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
        // The awaiter's vote has not been cast yet, so pending cannot reach 0 —
        // no resume race
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
    // Same as above: if startup throws partway, wait for already-started runners
    // to settle before rethrowing
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

// ---------- with_timeout: cooperative timeout (docs/concurrency.md §2/§5) ----------
// On expiry it only triggers src.request_cancel(); this function attaches
// src.token() to task (and to the whole chain of child tasks it co_awaits), so a
// timeout surfaces as OperationCancelled thrown from the nearest cancellable
// suspension point (pool.schedule / semaphore.acquire). A blocking syscall already
// running on a pool thread is not preempted — cooperative cancellation does not
// attempt preemption; it waits for the call to return naturally and the next
// suspension point notices.
// src should be dedicated to this call: a source shared with others would, after a
// timeout, also hit subsequent operations of the same request.
// If "external cancellation (disconnect / process shutdown) should interrupt too"
// is needed, the caller wires the external token's callback into the same src
// (see S3Service::dispatch).
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
