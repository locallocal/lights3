// L4: lazy Task<T> coroutine primitive plus sync_wait / when_all / with_timeout (see docs/architecture/concurrency.md)
#pragma once

#include <atomic>
#include <chrono>
#include <concepts>
#include <condition_variable>
#include <coroutine>
#include <exception>
#include <memory>
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

// ---------- Resume trampoline ----------
// co_await of a Task and its completion are "symmetric transfers": await_suspend
// returns the coroutine to run next. The standard promises no stack growth for
// that only when the compiler emits a tail call, which GCC does under
// optimization and not at -O0: a Debug / sanitizer build nested one C frame per
// transfer, and a body-read loop whose reads complete synchronously (the builtin
// driver's blocking socket reader feeding put_object, StreamPrefetch over an
// in-memory body) overflowed the stack at a few MiB (docs/architecture/concurrency.md §2.4).
// Transfers therefore go through a per-thread loop instead: the first transfer
// on a thread runs the loop right there (inside that await_suspend, which is
// legal -- the coroutine is already suspended), every transfer made while the
// loop runs is queued and picked up when the current resume returns. The stack
// stays flat in every build; the cost is a thread_local access and a queue
// push per transfer. Direct h.resume() calls elsewhere (executors, latches) are
// fine: the transfers they trigger enter the loop the same way
struct Trampoline {
    bool running = false;
    std::vector<std::coroutine_handle<>> queue;
};
inline Trampoline& trampoline() {
    thread_local Trampoline t;
    return t;
}

// Hand h to the running loop of this thread, or run one for it now. The caller
// must not touch its own coroutine frame afterwards (it may have completed and
// been destroyed inside the loop)
inline void transfer(std::coroutine_handle<> h) noexcept {
    auto& t = trampoline();
    if (t.running) {
        t.queue.push_back(h);
        return;
    }
    t.running = true;
    h.resume();
    while (!t.queue.empty()) {
        auto n = t.queue.back();
        t.queue.pop_back();
        n.resume();
    }
    t.running = false;
}

// Run h and everything it hands over until it all suspends, even inside a
// running loop (a private queue keeps the outer loop's pending work aside):
// for resumers whose caller then blocks on the outcome (sync_wait), which a
// deferred transfer would deadlock
inline void drive(std::coroutine_handle<> h) {
    auto& t = trampoline();
    std::vector<std::coroutine_handle<>> saved;
    saved.swap(t.queue);
    bool was = t.running;
    t.running = true;
    h.resume();
    while (!t.queue.empty()) {
        auto n = t.queue.back();
        t.queue.pop_back();
        n.resume();
    }
    t.running = was;
    t.queue.swap(saved);
}

struct PromiseBase {
    std::coroutine_handle<> continuation;
    SyncWaitEvent* event = nullptr;
    // Home executor (docs/architecture/concurrency.md §3): when set, final_suspend posts the
    // continuation there instead of doing a symmetric transfer — protocol logic thus
    // returns to the HTTP execution context; child tasks inherit it on co_await
    IExecutor* cont_executor = nullptr;
    // Cancellation token (docs/architecture/concurrency.md §5, docs/archive/gaps.md §3.1): inherited down
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
                // Back to the caller through the trampoline (the caller may destroy
                // this frame inside it: nothing of p is touched afterwards)
                transfer(p.continuation);
                return std::noop_coroutine();
            }
            // top-level sync_wait
            if (p.event) p.event->set();
            return std::noop_coroutine();
        }
        void await_resume() const noexcept {}
    };

    // lazy start
    std::suspend_always initial_suspend() noexcept { return {}; }
    FinalAwaiter final_suspend() noexcept { return {}; }
};

// Common suspend logic for co_await: record the continuation and inherit the
// caller's home executor and cancellation token
template <class Promise>
std::coroutine_handle<> task_await_suspend(std::coroutine_handle<Promise> task, std::coroutine_handle<> cont,
                                           IExecutor* parent_executor, const CancelToken& parent_cancel) noexcept {
    auto& p = task.promise();
    p.continuation = cont;
    if (!p.cont_executor) p.cont_executor = parent_executor;
    // Do not override a token the child task already carries (explicitly attached
    // via with_cancel); otherwise inherit the caller's
    if (!p.cancel.valid()) p.cancel = parent_cancel;
    // starts the awaited task (flat, see Trampoline)
    transfer(task);
    return std::noop_coroutine();
}

// co_await current_cancel(): the token the current coroutine carries (inherited or
// attached). Never suspends; used by the metered backend decorator to reach the
// request-scoped payload (roadmap §5.1)
struct CurrentCancel {
    CancelToken tok;
    bool await_ready() const noexcept { return false; }
    template <class P>
    bool await_suspend(std::coroutine_handle<P> h) noexcept {
        if constexpr (requires {
                          { h.promise().cancel } -> std::convertible_to<CancelToken>;
                      })
            tok = h.promise().cancel;
        // resume immediately
        return false;
    }
    CancelToken await_resume() noexcept { return std::move(tok); }
};

}  // namespace detail

inline detail::CurrentCancel current_cancel() { return {}; }

template <class T>
class [[nodiscard]] Task {
public:
    struct promise_type : detail::PromiseBase {
        std::variant<std::monostate, T, std::exception_ptr> result;

        Task get_return_object() { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }
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

    // For sync_wait only: bind the event and start. drive: the caller blocks on
    // the event next, so nothing may stay queued behind it on this thread
    void start(SyncWaitEvent* ev) {
        check_valid("start");
        h_.promise().event = ev;
        detail::drive(h_);
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
        if (!h_) throw std::logic_error(std::string("Task: ") + op + " on a moved-from task");
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

        Task get_return_object() { return Task{std::coroutine_handle<promise_type>::from_promise(*this)}; }
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
        detail::drive(h_);
    }
    void take_result() {
        check_valid("take_result");
        if (h_.promise().error) std::rethrow_exception(h_.promise().error);
    }

private:
    explicit Task(std::coroutine_handle<promise_type> h) : h_(h) {}
    void check_valid(const char* op) const {
        if (!h_) throw std::logic_error(std::string("Task: ") + op + " on a moved-from task");
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

// ---------- sync_wait_pumping: the request thread acts as an executor while it waits (docs/archive/gaps.md §2.10)
// ---------- Difference from sync_wait: while waiting, ex's queue is run on the current thread, and the body reader
// switches blocking reads back onto this thread via resume_on(ex). For the synchronous drivers (builtin/httplib) only.

namespace detail {

// Self-destroying wrapper coroutine: drives the top-level task, moves the result
// onto the caller's stack, and finally wakes the pump loop. out/err are written
// before finish(); after finish() no caller state is touched anymore
// Lazy start + drive(): the caller pumps ex right after, so the task and every
// transfer it makes must have run to their first real suspension before that
// (a transfer left in an enclosing trampoline loop would deadlock the pump)
struct PumpRunner {
    struct promise_type {
        PumpRunner get_return_object() { return {std::coroutine_handle<promise_type>::from_promise(*this)}; }
        std::suspend_always initial_suspend() noexcept { return {}; }
        // self-destructs on completion
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        // coroutine body catches everything
        void unhandled_exception() { std::terminate(); }
    };
    std::coroutine_handle<> h;
    void start() { drive(h); }
};

template <class T>
PumpRunner pump_run(Task<T> t, PumpExecutor& ex, std::optional<T>& out, std::exception_ptr& err) {
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
    detail::pump_run(std::move(t), ex, out, err).start();
    ex.run();
    if (err) std::rethrow_exception(err);
    return std::move(*out);
}

inline void sync_wait_pumping(PumpExecutor& ex, Task<void> t) {
    std::exception_ptr err;
    detail::pump_run(std::move(t), ex, err).start();
    ex.run();
    if (err) std::rethrow_exception(err);
}

// ---------- when_all: concurrently await a set of Tasks (docs/architecture/concurrency.md §2/§6) ----------

namespace detail {

// n runners + 1 awaiter make n+1 votes; whoever casts the last vote resumes the
// when_all coroutine
struct WhenAllLatch {
    std::atomic<size_t> pending;
    std::coroutine_handle<> continuation;
    explicit WhenAllLatch(size_t n) : pending(n + 1) {}
    void arrive() {
        // Do not touch the latch after the transfer: the when_all frame may already
        // have been destroyed inside it
        if (pending.fetch_sub(1, std::memory_order_acq_rel) == 1) transfer(continuation);
    }
};

struct WhenAllAwaiter {
    WhenAllLatch& latch;
    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept {
        latch.continuation = h;
        // Write the continuation before casting the last vote, so the runner side
        // sees a ready continuation when the count reaches 0. Everything already
        // done: resume through the trampoline (a returned handle would nest a frame
        // per synchronous round at -O0, see Trampoline)
        if (latch.pending.fetch_sub(1, std::memory_order_acq_rel) == 1) transfer(h);
        return std::noop_coroutine();
    }
    void await_resume() const noexcept {}
};

// Self-destroying wrapper coroutine: drives one child task and reports to the latch.
// Lazy start + explicit resume: guarantees the ramp has fully returned and the
// coroutine frame handoff is clean before it runs; otherwise, when the coroutine
// migrates to a pool thread and self-destructs, the ramp may still be touching the
// frame (a real data race). start() drives with a private trampoline queue: the
// child has reached its first real suspension (or completed) when start()
// returns, which a Started::wait() right after relies on
struct WhenAllRunner {
    struct promise_type {
        WhenAllRunner get_return_object() { return {std::coroutine_handle<promise_type>::from_promise(*this)}; }
        std::suspend_always initial_suspend() noexcept { return {}; }
        // self-destructs on completion
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        // coroutine body catches everything
        void unhandled_exception() { std::terminate(); }
    };
    std::coroutine_handle<> h;
    void start() { drive(h); }
};

template <class T>
WhenAllRunner when_all_run(Task<T> t, WhenAllLatch& latch, std::optional<T>& out, std::exception_ptr& err) {
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
            detail::when_all_run(std::move(tasks[started]), latch, results[started], errors[started]).start();
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
    if (spawn_err) latch.pending.fetch_sub(tasks.size() - started, std::memory_order_acq_rel);
    co_await detail::WhenAllAwaiter{latch};
    if (spawn_err) std::rethrow_exception(spawn_err);
    for (auto& e : errors)
        if (e) std::rethrow_exception(e);
}

// ---------- Started<T>: an eagerly started task collected later (docs/architecture/concurrency.md §2.3) ----------
// The building block of the drivers' double-buffered response pipeline (roadmap
// §4.3 ①): start the backend read of the *next* chunk, write the current one to
// the socket, then collect. One in-flight child per Started; the collector is
// either a coroutine (co_await, async drivers and the pumped builtin loop) or a
// plain thread (wait(), httplib's synchronous content provider). The child runs
// on whatever thread its own suspension points resume it on; co_await resumes
// the collector on the child's completing thread (or inline when the child
// already finished), so a driver switches back to its connection context
// afterwards exactly as after any co_await.
//
// Lifetime: the child references this object until it completes. Destroying a
// Started with an uncollected child blocks until the child finishes (the
// buffers it writes into are typically the owner's), so callers that bail out
// early pay the read latency once instead of freeing memory under a running read

namespace detail {

struct StartedState {
    // child + collecting coroutine; wait() never votes
    std::atomic<int> votes{2};
    std::coroutine_handle<> continuation;
    std::mutex m;
    std::condition_variable cv;
    bool done = false;

    // Child side. Everything this object is touched for happens under the
    // lock, and the continuation handle is copied out before unlocking: the
    // owner may destroy the state the moment it observes done (wait()) or is
    // resumed (co_await)
    void complete() {
        std::coroutine_handle<> resume;
        {
            std::lock_guard lk(m);
            done = true;
            if (votes.fetch_sub(1, std::memory_order_acq_rel) == 1) resume = continuation;
            cv.notify_all();
        }
        if (resume) transfer(resume);
    }
    void wait() {
        std::unique_lock lk(m);
        cv.wait(lk, [&] { return done; });
    }
    bool finished() {
        std::lock_guard lk(m);
        return done;
    }
};

struct StartedAwaiter {
    StartedState& st;
    bool await_ready() const noexcept { return false; }
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept {
        st.continuation = h;
        // Already done: through the trampoline, never a returned handle (a
        // synchronous collect-restart-collect loop must not nest, see Trampoline)
        if (st.votes.fetch_sub(1, std::memory_order_acq_rel) == 1) transfer(h);
        return std::noop_coroutine();
    }
    void await_resume() const noexcept {}
};

template <class T>
WhenAllRunner started_run(Task<T> t, StartedState& st, std::optional<T>& out, std::exception_ptr& err) {
    try {
        out.emplace(co_await std::move(t));
    } catch (...) {
        err = std::current_exception();
    }
    st.complete();
}

inline WhenAllRunner started_run(Task<void> t, StartedState& st, std::exception_ptr& err) {
    try {
        co_await std::move(t);
    } catch (...) {
        err = std::current_exception();
    }
    st.complete();
}

}  // namespace detail

template <class T>
class Started {
public:
    Started() = default;
    explicit Started(Task<T> t) { start(std::move(t)); }
    Started(const Started&) = delete;
    Started& operator=(const Started&) = delete;
    ~Started() {
        if (st_ && !collected_) st_->wait();
    }

    // Starts t; the previous child (if any) must have been collected
    void start(Task<T> t) {
        if (st_ && !collected_) st_->wait();
        st_ = std::make_unique<detail::StartedState>();
        collected_ = false;
        if constexpr (!std::is_void_v<T>) out_.reset();
        err_ = nullptr;
        if constexpr (std::is_void_v<T>)
            detail::started_run(std::move(t), *st_, err_).start();
        else
            detail::started_run(std::move(t), *st_, out_, err_).start();
    }
    // A child is running or finished but not yet collected
    bool pending() const { return st_ && !collected_; }
    bool finished() const { return pending() && st_->finished(); }

    // Synchronous collect (thread-blocking); rethrows the child's exception
    T wait() {
        st_->wait();
        return take();
    }

    // Coroutine collect
    auto operator co_await() {
        struct Awaiter {
            Started& s;
            detail::StartedAwaiter inner;
            bool await_ready() const noexcept { return false; }
            std::coroutine_handle<> await_suspend(std::coroutine_handle<> h) noexcept { return inner.await_suspend(h); }
            T await_resume() { return s.take(); }
        };
        return Awaiter{*this, detail::StartedAwaiter{*st_}};
    }

private:
    T take() {
        collected_ = true;
        if (err_) std::rethrow_exception(err_);
        if constexpr (!std::is_void_v<T>) return std::move(*out_);
    }

    std::unique_ptr<detail::StartedState> st_;
    std::conditional_t<std::is_void_v<T>, std::monostate, std::optional<T>> out_;
    std::exception_ptr err_;
    bool collected_ = true;
};

// ---------- with_timeout: cooperative timeout (docs/architecture/concurrency.md §2/§5) ----------
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
