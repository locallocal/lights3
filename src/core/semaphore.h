// L4: coroutine-based async semaphore (docs/concurrency.md §6)
// Used for max_inflight_requests throttling at the dispatch entry point and for
// multipart part-level concurrency control.
// An over-limit acquire() suspends and queues (FIFO) instead of being rejected.
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
    // resume_executor: when waking a waiter, post its continuation to this executor;
    // if null, resume in place on the release call stack — with heavy queuing under
    // the synchronous drivers this builds deep recursion of "finish one request ->
    // inline-run the next request", so production paths should pass the pool executor
    explicit AsyncSemaphore(long permits, IExecutor* resume_executor = nullptr)
        : permits_(permits), capacity_(permits), exec_(resume_executor) {}
    AsyncSemaphore(const AsyncSemaphore&) = delete;

    // Contract: no waiters may remain at destruction — resuming them would hand out
    // Permits pointing at a dead semaphore (UAF), while not resuming leaks the frames
    // and blocks sync_wait threads forever. The owner must drain in-flight requests
    // before destruction, or call close() to wake all waiters with cancellation
    // semantics; on contract violation log ERROR (debug builds assert), leaking the
    // frames being the conservative lesser of two evils
    ~AsyncSemaphore() {
        std::lock_guard lk(m_);
        if (!waiters_.empty())
            LOG_ERROR("AsyncSemaphore destroyed with {} waiter(s); frames leaked", waiters_.size());
        assert(waiters_.empty());
    }

    // RAII permit: returned on destruction (released automatically when the coroutine
    // frame exits, exception paths included)
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

    // Waiter state: lives in a separate shared block (like ThreadPool::
    // ScheduleAwaiter::Slot) — release, the cancel callback, and close race to
    // resume; the losers still hold references, which must not point into an
    // awaiter/coroutine frame that may already be destroyed once the coroutine resumes
    struct Waiter {
        std::coroutine_handle<> h;
        std::atomic<bool> claimed{false};
        bool cancelled = false;  // written only by the successful claimer, read on the same thread after resume
        std::atomic<uint64_t> reg_id{0};
        std::shared_ptr<detail::CancelState> cancel_state;
    };

    struct AcquireAwaiter {
        AsyncSemaphore& sem;
        CancelToken token;
        std::shared_ptr<Waiter> w;    // allocated only when suspending or when a cancel callback must be registered
        bool immediate_fail = false;  // failed without suspending (already closed / cancelled before registration)

        bool await_ready() const noexcept { return false; }
        // Same as ThreadPool::schedule: when no token is passed explicitly, inherit
        // from the caller's promise
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
                // No cancellation token: lightweight path — if a permit is available,
                // nothing is allocated
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
            // Cancellable path: the callback must be fully registered **before**
            // enqueuing; otherwise release could win the race and resume us in the
            // window "after enqueue, before the writes land", making those writes
            // race with await_resume's reads
            w = std::make_shared<Waiter>();
            w->h = h;
            // Hold everything needed later in locals (like ThreadPool::
            // ScheduleAwaiter): from the moment the cancel callback registers
            // successfully, the coroutine may be resumed by cancel_waiter on another
            // thread and destroyed along with its frame — this/sem/token/w are all
            // frame members and must not be touched after registration
            auto* s = &sem;
            auto wp = w;
            CancelToken tok = token;
            tok.on_cancel_publish([s, wp] { s->cancel_waiter(wp); }, wp->reg_id,
                                  wp->cancel_state);
            bool pre_cancelled = tok.cancelled();  // cancelled before registration: the callback will not be invoked
            {
                std::lock_guard lk(s->m_);
                if (s->closed_ || pre_cancelled) {
                    // A successful claim means this coroutine throws by itself; a
                    // failed claim means the cancel callback is about to resume us
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

    // Usage: auto permit = co_await sem.acquire();
    // On cancellation (request timeout / disconnect / process shutdown), a queued
    // acquire surfaces as OperationCancelled — the max_inflight_requests queue is
    // exactly where a request is most likely to hang for a long time (docs/archive/gaps.md §3.1)
    AcquireAwaiter acquire(CancelToken token = {}) { return {*this, std::move(token), {}, false}; }

    // Wake all waiters with cancellation semantics and reject subsequent acquires.
    // Calling this before destruction lets the owner satisfy the "no waiters at
    // destruction" contract (docs/archive/gaps.md §2.13 leftover)
    void close() {
        std::deque<std::shared_ptr<Waiter>> ws;
        {
            std::lock_guard lk(m_);
            closed_ = true;
            ws.swap(waiters_);
        }
        for (auto& w : ws) wake_cancelled(w);
    }

    // Non-blocking acquire: take a permit if available, otherwise return nullopt —
    // never joins the wait queue. For callers that "already hold one and want a
    // second" (the rados double-buffer pipeline): a nested blocking acquire would
    // deadlock when everyone holds one each, so try semantics downgrade the second
    // permit to "pipeline if available, go serial if not"
    std::optional<Permit> try_acquire() {
        std::lock_guard lk(m_);
        if (closed_ || permits_ <= 0) return std::nullopt;
        --permits_;
        return Permit{this};
    }

    // Configured capacity (runtime.max_inflight_requests); set_capacity resizes it
    // live (config hot reload, roadmap §4.4): growing wakes queued waiters, shrinking
    // lets the extra permits drain as in-flight requests finish (available() may go
    // negative meanwhile — nothing new is admitted until it recovers)
    long capacity() const {
        std::lock_guard lk(m_);
        return capacity_;
    }
    void set_capacity(long n) {
        std::vector<std::shared_ptr<Waiter>> wake;
        {
            std::lock_guard lk(m_);
            long delta = n - capacity_;
            capacity_ = n;
            if (delta <= 0) {
                permits_ += delta;
                return;
            }
            for (long i = 0; i < delta; ++i) {
                std::shared_ptr<Waiter> next;
                while (!waiters_.empty()) {
                    auto w = std::move(waiters_.front());
                    waiters_.pop_front();
                    if (!w->claimed.exchange(true, std::memory_order_acq_rel)) {
                        next = std::move(w);
                        break;
                    }
                }
                if (next) wake.push_back(std::move(next));
                else ++permits_;
            }
        }
        for (auto& w : wake) {
            if (exec_) exec_->post(w->h);
            else w->h.resume();
        }
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
                // Skip waiters already claimed by the cancel callback (they resume
                // themselves with OperationCancelled); the permit keeps looking for
                // the next real waiter and only increments the count if none is found
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

    // Cancel callback: claim + unlink + resume in place. In place is deliberate —
    // the resumed coroutine immediately throws OperationCancelled, doing only the
    // bounded work of exception unwinding; posting to exec_ (in production, this IO
    // pool itself) would leave "the pool is saturated", the very scenario that most
    // needs cancellation, waiting for an executor (same as ThreadPool::ScheduleAwaiter)
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
    long capacity_ = 0;
    IExecutor* exec_;
    bool closed_ = false;
    std::deque<std::shared_ptr<Waiter>> waiters_;
};

}  // namespace lights3
