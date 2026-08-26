#include "core/background.h"

#include <chrono>
#include <utility>

#include "core/log.h"

namespace lights3 {

namespace {

// Self-destroying top-level coroutine: drives one background Task and invokes a
// callback at the end (exceptions are only logged)
struct Detached {
    struct promise_type {
        Detached get_return_object() { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }  // coroutine body catches everything
    };
};

Detached run_detached(const char* name, Task<void> t, std::function<void()> done) {
    {
        // The awaited task's coroutine frame must be destroyed **before** done():
        // once done() runs, wait_idle() releases the owner to start destroying the
        // backend/thread pool and other resources, while locals inside the task
        // frame are only just about to be destroyed and would touch those dead
        // objects (docs/archive/gaps.md §3.9). The coroutine parameter t is part of the
        // frame (destroyed with it, after done), hence moved into a local of this
        // scope first
        Task<void> task = std::move(t);
        try {
            co_await std::move(task);
        } catch (const std::exception& e) {
            LOG_WARN("{}: background task failed: {}", name, e.what());
        } catch (...) {
            LOG_WARN("{}: background task failed (unknown exception)", name);
        }
    }
    done();
}

}  // namespace

bool BackgroundTaskGroup::spawn(Task<void> t) {
    {
        std::lock_guard lk(m_);
        if (closing_) return false;
        ++count_;
    }
    try {
        run_detached(name_, std::move(t), [this] { on_done(); });
    } catch (...) {
        // Frame allocation failed: the count was already incremented and the done
        // callback will never run; without compensating, wait_idle() blocks forever
        on_done();
        throw;
    }
    return true;
}

bool BackgroundTaskGroup::enter() {
    std::lock_guard lk(m_);
    if (closing_) return false;
    ++count_;
    return true;
}

void BackgroundTaskGroup::exit() { on_done(); }

bool BackgroundTaskGroup::if_open(const std::function<void()>& fn) {
    std::lock_guard lk(m_);
    if (closing_) return false;
    fn();
    return true;
}

void BackgroundTaskGroup::begin_close() {
    std::lock_guard lk(m_);
    closing_ = true;
}

void BackgroundTaskGroup::wait_idle() {
    // Shutdown-hang diagnostics (docs/archive/gaps.md §7): the semantics are still an
    // unbounded wait (force-killing in-flight tasks would only buy a UAF), but every
    // 10 seconds "which group is stuck, how many tasks remain" is written to the log
    // — previously this was a bare cv.wait, leaving zero clues outside gdb when
    // shutdown hung
    std::unique_lock lk(m_);
    while (!cv_.wait_for(lk, std::chrono::seconds(10), [&] { return count_ == 0; }))
        LOG_WARN("{}: wait_idle still blocked, {} background task(s) in flight", name_, count_);
}

bool BackgroundTaskGroup::closing() const {
    std::lock_guard lk(m_);
    return closing_;
}

void BackgroundTaskGroup::on_done() {
    std::lock_guard lk(m_);
    --count_;
    cv_.notify_all();
}

}  // namespace lights3
