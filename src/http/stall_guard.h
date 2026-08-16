// L1/L2 boundary: transfer stall guard (docs/gaps.md §3.3)
//
// All four drivers reset their timeouts **per chunk**: as long as the client
// sends one byte per cycle the read timeout never fires, and a connection can
// be held indefinitely (same for both directions). Here we detect a stall as
// "less than kMinProgressBytes advanced within each window" — genuinely slow
// but progressing connections are unaffected, while drip-feed ones get cut off.
//
// Wrapped around BodyReader rather than inside each driver: one wrapper covers
// all four drivers at once, and request and response bodies share the same
// criterion. Note it only evaluates when read() returns; it does not interrupt
// a blocked syscall — connections sending no data at all are handled by the
// driver's idle_timeout.
#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <span>
#include <utility>

#include "http/model.h"
#include "s3/errors.h"

namespace lights3::http {

class StallGuardReader final : public BodyReader {
public:
    // Accumulating this many bytes within one window counts as "progress" and resets the timer
    static constexpr uint64_t kMinProgressBytes = 64 * 1024;

    StallGuardReader(std::unique_ptr<BodyReader> inner, std::chrono::seconds window)
        : inner_(std::move(inner)), window_(window), mark_(Clock::now()) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = co_await inner_->read(buf);
        if (n == 0) co_return 0;  // EOF: no stall check
        moved_ += n;
        auto now = Clock::now();
        if (moved_ >= kMinProgressBytes) {
            moved_ = 0;
            mark_ = now;
        } else if (now - mark_ > window_) {
            throw s3::S3Error(s3::S3ErrorCode::RequestTimeout,
                              "Transfer stalled: less than 64 KiB moved within the transfer "
                              "stall timeout.");
        }
        co_return n;
    }

    std::optional<uint64_t> length() const override { return inner_->length(); }

private:
    using Clock = std::chrono::steady_clock;
    std::unique_ptr<BodyReader> inner_;
    std::chrono::seconds window_;
    Clock::time_point mark_;
    uint64_t moved_ = 0;
};

// Returns the reader unchanged when window <= 0 (guard disabled)
inline std::unique_ptr<BodyReader> guard_stalls(std::unique_ptr<BodyReader> inner,
                                                std::chrono::seconds window) {
    if (!inner || window.count() <= 0) return inner;
    return std::make_unique<StallGuardReader>(std::move(inner), window);
}

}  // namespace lights3::http
