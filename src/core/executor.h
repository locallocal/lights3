// L4: delivery abstraction for coroutine continuations (see docs/concurrency.md §3)
#pragma once

#include <condition_variable>
#include <coroutine>
#include <deque>
#include <mutex>
#include <thread>

namespace lights3 {

struct IExecutor {
    virtual void post(std::coroutine_handle<> h) = 0;
    virtual ~IExecutor() = default;
};

// Resume in place: used by the synchronous drivers (thread-per-request)
struct InlineExecutor final : IExecutor {
    void post(std::coroutine_handle<> h) override { h.resume(); }
    static InlineExecutor& instance() {
        static InlineExecutor e;
        return e;
    }
};

// Request-thread executor for the synchronous drivers (thread-per-request/
// connection) (docs/archive/gaps.md §2.10): instead of idling in sync_wait, the request
// thread runs this queue via sync_wait_pumping (task.h); the body reader switches
// blocking reads back onto the request thread via resume_on, so a slow client
// clogs only its own connection thread, not shared pool threads. One instance per
// request
class PumpExecutor final : public IExecutor {
public:
    // The constructing thread is the pumping thread: the top-level task starts
    // eagerly on it before run() is entered (sync_wait_pumping), and a
    // resume_on() hit during that ramp may continue inline just the same
    PumpExecutor() : runner_(std::this_thread::get_id()) {}

    // Notify while holding the lock (same as SyncWaitEvent): this object may be
    // destroyed as soon as run() returns, so notify must complete before unlocking
    // or it would touch a destroyed cv
    void post(std::coroutine_handle<> h) override {
        std::lock_guard lk(m_);
        q_.push_back(h);
        cv_.notify_one();
    }

    // Called from any thread when the top-level task completes; wakes and terminates run()
    void finish() {
        std::lock_guard lk(m_);
        done_ = true;
        cv_.notify_one();
    }

    // Request thread: loops executing posted continuations until finish() has been
    // called and the queue is drained
    void run() {
        runner_ = std::this_thread::get_id();
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

    // True while called from inside run() on the pumping thread: resume_on() can
    // then continue inline instead of a queue round trip (roadmap §4.3 ⑤, the
    // same fast path the beast strand / seastar shard awaiters have)
    bool running_in_this_thread() const { return runner_ == std::this_thread::get_id(); }

private:
    std::mutex m_;
    std::condition_variable cv_;
    std::deque<std::coroutine_handle<>> q_;
    bool done_ = false;
    std::thread::id runner_;
};

// co_await resume_on(ex): switches the current coroutine's subsequent execution to the given executor
struct ResumeOnAwaiter {
    IExecutor& ex;
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) { ex.post(h); }
    void await_resume() const noexcept {}
};
inline ResumeOnAwaiter resume_on(IExecutor& ex) { return {ex}; }

// PumpExecutor overload: no-op when already running on the pumping thread
struct ResumeOnPumpAwaiter {
    PumpExecutor& ex;
    bool await_ready() const noexcept { return ex.running_in_this_thread(); }
    void await_suspend(std::coroutine_handle<> h) { ex.post(h); }
    void await_resume() const noexcept {}
};
inline ResumeOnPumpAwaiter resume_on(PumpExecutor& ex) { return {ex}; }

}  // namespace lights3
