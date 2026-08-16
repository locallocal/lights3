// L4: background task wait group (docs/concurrency.md §7): self-destroying
// top-level coroutine driver + close waits for in-flight tasks to drain to zero.
// Extracted from the verbatim-duplicated private mechanisms in TieredBackend /
// DuoStoreBackend (the lifetime-critical path is maintained in one place only).
//
// Usage (the fixed shape of a backend's background worker):
//   spawn(task)            —— spawn a background coroutine from a timer callback
//                             (rejected after closing)
//   enter()/exit()         —— register a caller-driven coroutine (e.g. a manual GC
//                             hook) as in-flight; close waits for it to finish
//                             before tearing down resources; enter returning false
//                             = closing, and the caller should return immediately
//   if_open(fn)            —— run fn atomically with the closing check (typical:
//                             recording a timer id)
//   begin_close()          —— set closing; from then on spawn/enter/if_open all refuse
//   wait_idle()            —— block until in-flight tasks drain to zero (caller
//                             thread; tasks finish on pool threads)
//
// Conventional close order: begin_close() -> cancel timers outside the lock
// (TimerQueue::cancel blocks on in-flight callbacks and must not be called holding
// this group's lock — spawn/if_open inside a callback take the group lock, so
// cancelling under the lock deadlocks) -> wait_idle() -> tear down resources.
#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>

#include "core/task.h"

namespace lights3 {

class BackgroundTaskGroup {
public:
    // name prefixes background-task failure logs (must be a string with static lifetime)
    explicit BackgroundTaskGroup(const char* name) : name_(name) {}
    BackgroundTaskGroup(const BackgroundTaskGroup&) = delete;

    // Spawn a self-destroying background coroutine; returns false after closing
    // (the task is dropped, not executed). Task exceptions are caught internally
    // and logged as WARN, never propagated
    bool spawn(Task<void> t);

    // Register a caller-driven coroutine as in-flight; atomically exclusive with
    // begin_close. Pair with exit() (Scope recommended)
    bool enter();
    void exit();

    // RAII registration: ok() being false means closing is in progress and the
    // caller should return immediately
    class Scope {
    public:
        explicit Scope(BackgroundTaskGroup& g) : g_(g.enter() ? &g : nullptr) {}
        Scope(const Scope&) = delete;
        ~Scope() {
            if (g_) g_->exit();
        }
        bool ok() const { return g_ != nullptr; }

    private:
        BackgroundTaskGroup* g_;
    };

    // Run fn atomically with the closing check (when closing, fn is not run and
    // false is returned); typically for scenarios where "check not closed +
    // record the timer id" must happen in one step
    bool if_open(const std::function<void()>& fn);

    void begin_close();
    void wait_idle();
    bool closing() const;

private:
    void on_done();

    const char* name_;
    mutable std::mutex m_;
    std::condition_variable cv_;
    int count_ = 0;
    bool closing_ = false;
};

}  // namespace lights3
