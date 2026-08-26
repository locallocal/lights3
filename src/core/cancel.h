// L4: cooperative cancellation primitives (docs/concurrency.md §5)
// CancelSource triggers, CancelToken observes; cancellation surfaces as an
// OperationCancelled exception thrown from suspension points.
// No preemption is attempted: a blocking call in progress returns naturally, after
// which suspension points / long loops check the token.
#pragma once

#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <thread>
#include <utility>

namespace lights3 {

class OperationCancelled : public std::exception {
public:
    const char* what() const noexcept override { return "operation cancelled"; }
};

namespace detail {

class CancelState {
public:
    // Callback contract (docs/concurrency.md §5): **must be lightweight**. The
    // cancellation source is often TimerQueue's single timer thread, and resuming a
    // continuation directly inside a callback would run the whole request chain on
    // the timer thread, stalling every timer in the process meanwhile (docs/archive/gaps.md
    // §3.2). Consumers that need to resume coroutines always go through an executor
    // (see ThreadPool::ScheduleAwaiter / AsyncSemaphore::AcquireAwaiter)
    void request_cancel() {
        std::map<uint64_t, std::function<void()>> cbs;
        {
            std::lock_guard lk(m_);
            if (cancelled_.exchange(true, std::memory_order_acq_rel)) return;
            cbs.swap(callbacks_);
            firing_ = true;
            firing_thread_ = std::this_thread::get_id();
        }
        // Executed outside the lock, so callbacks may operate on the token again.
        // FiringGuard guarantees that every path (including a throwing callback)
        // resets firing_ and wakes deregistering callers waiting in remove_callback
        struct FiringGuard {
            CancelState* self;
            ~FiringGuard() {
                {
                    std::lock_guard lk(self->m_);
                    self->firing_ = false;
                }
                self->fired_cv_.notify_all();
            }
        } guard{this};
        for (auto& [id, fn] : cbs) fn();
    }

    bool cancelled() const { return cancelled_.load(std::memory_order_acquire); }

    // If already cancelled, does not register and returns 0; after registering, the
    // caller must check cancelled() itself to cover the race window
    uint64_t add_callback(std::function<void()> fn) {
        std::lock_guard lk(m_);
        if (cancelled_.load(std::memory_order_relaxed)) return 0;
        uint64_t id = ++next_id_;
        callbacks_.emplace(id, std::move(fn));
        return id;
    }

    // Same as above, but writes the (state, id) needed for deregistration into
    // caller-provided storage within the same critical section: request_cancel and
    // this function are mutually exclusive, guaranteeing both are in place before
    // the callback can fire. Otherwise the cancel callback could win the race and
    // resume in the window "after registration, before the writes land", making
    // those writes race with the consumer's reads
    uint64_t add_callback_publish(std::function<void()> fn, std::atomic<uint64_t>& out_id,
                                  std::shared_ptr<CancelState>& out_state,
                                  std::shared_ptr<CancelState> self) {
        std::lock_guard lk(m_);
        if (cancelled_.load(std::memory_order_relaxed)) {
            out_id.store(0, std::memory_order_release);
            return 0;
        }
        uint64_t id = ++next_id_;
        out_state = std::move(self);
        out_id.store(id, std::memory_order_release);
        callbacks_.emplace(id, std::move(fn));
        return id;
    }

    // Deregister: on return, the callback is guaranteed to never run again — if
    // cancellation is currently firing (the callback batch runs outside the lock),
    // block until the batch finishes, same semantics as TimerQueue::cancel
    // (docs/archive/gaps.md §3.9). Self-deregistration on the firing thread (from inside a
    // callback) does not wait, preventing self-deadlock; while waiting, do not hold
    // locks the callbacks need
    void remove_callback(uint64_t id) {
        if (id == 0) return;
        std::unique_lock lk(m_);
        callbacks_.erase(id);
        if (firing_ && firing_thread_ != std::this_thread::get_id())
            fired_cv_.wait(lk, [&] { return !firing_; });
    }

private:
    std::atomic<bool> cancelled_{false};
    std::mutex m_;
    std::condition_variable fired_cv_;
    uint64_t next_id_ = 0;
    bool firing_ = false;              // callback batch is executing outside the lock
    std::thread::id firing_thread_{};  // thread executing that batch
    std::map<uint64_t, std::function<void()>> callbacks_;
};

}  // namespace detail

// Registration handle for a cancel callback: deregisters on destruction/reset
// (deregistering an already-fired registration is a no-op)
class CancelRegistration {
public:
    CancelRegistration() = default;
    CancelRegistration(std::shared_ptr<detail::CancelState> s, uint64_t id)
        : state_(std::move(s)), id_(id) {}
    CancelRegistration(CancelRegistration&& o) noexcept
        : state_(std::move(o.state_)), id_(std::exchange(o.id_, 0)) {}
    CancelRegistration& operator=(CancelRegistration&& o) noexcept {
        if (this != &o) {
            reset();
            state_ = std::move(o.state_);
            id_ = std::exchange(o.id_, 0);
        }
        return *this;
    }
    CancelRegistration(const CancelRegistration&) = delete;
    ~CancelRegistration() { reset(); }

    void reset() {
        if (state_) state_->remove_callback(id_);
        state_.reset();
        id_ = 0;
    }

private:
    std::shared_ptr<detail::CancelState> state_;
    uint64_t id_ = 0;
};

// Observer side. Default construction is "never cancelled"; freely copyable and
// passed along with the RequestContext
class CancelToken {
public:
    CancelToken() = default;
    explicit CancelToken(std::shared_ptr<detail::CancelState> s) : state_(std::move(s)) {}

    bool cancelled() const { return state_ && state_->cancelled(); }
    void throw_if_cancelled() const {
        if (cancelled()) throw OperationCancelled();
    }
    // Whether a real cancellation source is bound (false for the default-constructed
    // "never cancelled" token). Used when the coroutine chain inherits tokens along
    // Tasks to decide "does this level already have a token" (core/task.h)
    bool valid() const { return state_ != nullptr; }

    // Note: if already cancelled at registration time, the callback will not be
    // invoked (an empty handle is returned) — the caller then checks cancelled() itself
    CancelRegistration on_cancel(std::function<void()> fn) const {
        if (!state_) return {};
        uint64_t id = state_->add_callback(std::move(fn));
        return id ? CancelRegistration(state_, id) : CancelRegistration{};
    }

    // For scenarios where "the callback will resume the consumer across threads":
    // the (out_state, out_id) needed for deregistration are written inside the
    // registration critical section, ready before the callback can fire. Returns
    // whether registration succeeded
    bool on_cancel_publish(std::function<void()> fn, std::atomic<uint64_t>& out_id,
                           std::shared_ptr<detail::CancelState>& out_state) const {
        if (!state_) {
            out_id.store(0, std::memory_order_release);
            return false;
        }
        return state_->add_callback_publish(std::move(fn), out_id, out_state, state_) != 0;
    }

private:
    std::shared_ptr<detail::CancelState> state_;
};

// Trigger side: client disconnect (detected by the driver), request timeout, process shutdown
class CancelSource {
public:
    CancelSource() : state_(std::make_shared<detail::CancelState>()) {}

    CancelToken token() const { return CancelToken(state_); }
    void request_cancel() { state_->request_cancel(); }
    bool cancelled() const { return state_->cancelled(); }

private:
    std::shared_ptr<detail::CancelState> state_;
};

}  // namespace lights3
