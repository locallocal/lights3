// L3: byte-rate pacing for scrub/fsck traversals (docs/roadmap.md §3.1).
// A scrub reads every byte a backend holds; unthrottled it competes with live
// traffic for disk bandwidth, so the read loops call pace(n) after each buffer
// and sleep whenever they run ahead of max_bytes_per_sec. Sleeping goes through
// TimerQueue in bounded slices with an abort probe in between — a backend
// close() never waits longer than one slice for a heavily throttled scrub.
#pragma once

#include <chrono>
#include <coroutine>
#include <cstdint>
#include <functional>
#include <memory>

#include "core/task.h"
#include "core/thread_pool.h"
#include "core/timer.h"

namespace lights3::storage {

class ScrubThrottle {
public:
    // bytes_per_sec = 0 disables pacing entirely. abort is probed between sleep
    // slices (typically the owner's BackgroundTaskGroup::closing) so shutdown
    // does not sit out the full pacing debt.
    ScrubThrottle(uint64_t bytes_per_sec, std::shared_ptr<ThreadPool> pool,
                  std::function<bool()> abort = {})
        : bps_(bytes_per_sec), pool_(std::move(pool)), abort_(std::move(abort)) {}

    // Account n bytes just read and sleep off any budget surplus. Resumes on a
    // pool thread (the timer callback thread must never run scrub IO).
    Task<void> pace(uint64_t n) {
        using Clock = TimerQueue::Clock;
        if (bps_ == 0 || n == 0) co_return;
        auto now = Clock::now();
        if (next_ == Clock::time_point{}) next_ = now;
        // Ideal schedule point advances by this batch's duration; after a stall
        // (slow extent, backend hiccup) allow at most one second of catch-up
        // burst instead of an unbounded one
        if (next_ < now - std::chrono::seconds(1)) next_ = now - std::chrono::seconds(1);
        next_ += std::chrono::nanoseconds(uint64_t(n * 1'000'000'000.0 / double(bps_)));
        while (true) {
            if (abort_ && abort_()) break;
            auto delay = next_ - Clock::now();
            if (delay <= Clock::duration::zero()) break;
            co_await async_sleep(std::min<Clock::duration>(delay, kSlice));
            co_await pool_->schedule();  // off the timer callback thread
        }
    }

private:
    static constexpr TimerQueue::Clock::duration kSlice = std::chrono::milliseconds(500);

    uint64_t bps_;
    std::shared_ptr<ThreadPool> pool_;
    std::function<bool()> abort_;
    TimerQueue::Clock::time_point next_{};
};

}  // namespace lights3::storage
