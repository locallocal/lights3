// L1/L2 边界：传输停滞守卫（docs/gaps.md §3.3）
//
// 四个驱动的超时都是**逐块重置**的：只要客户端每个周期发一个字节，读超时就永远
// 不到点，一条连接可以被无限期占住（收发两个方向同样）。这里按"每个窗口至少推进
// kMinProgressBytes 字节"判定停滞——真正慢但在传的连接不受影响，滴灌式的会被掐断。
//
// 装在 BodyReader 外层而不是各驱动内部：一处包裹即对四驱动同时生效，且请求体与
// 响应体共用同一套判据。注意它只在 read() 返回时判定，不打断已阻塞的系统调用——
// 完全不发数据的连接由驱动的 idle_timeout 负责。
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
    // 一个窗口内累计推进达到该字节数即视为"有进展"，重置计时
    static constexpr uint64_t kMinProgressBytes = 64 * 1024;

    StallGuardReader(std::unique_ptr<BodyReader> inner, std::chrono::seconds window)
        : inner_(std::move(inner)), window_(window), mark_(Clock::now()) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = co_await inner_->read(buf);
        if (n == 0) co_return 0;  // EOF：不判定
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

// window <= 0 时原样返回（关闭该保护）
inline std::unique_ptr<BodyReader> guard_stalls(std::unique_ptr<BodyReader> inner,
                                                std::chrono::seconds window) {
    if (!inner || window.count() <= 0) return inner;
    return std::make_unique<StallGuardReader>(std::move(inner), window);
}

}  // namespace lights3::http
