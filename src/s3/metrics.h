// L2: 请求指标（docs/05 §7），GET /-/metrics 以 Prometheus 文本格式输出
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <optional>
#include <string>
#include <string_view>

#include "core/thread_pool.h"

namespace lights3::s3 {

class Metrics {
public:
    // 延迟直方图桶上界（秒），末桶为 +Inf
    static constexpr std::array<double, 6> kLatencyBuckets{0.005, 0.02, 0.1, 0.5, 2.0, 10.0};

    void request_start() { inflight_.fetch_add(1, std::memory_order_relaxed); }
    void request_end(std::string_view method, int status, double seconds);
    void s3_error(const char* wire_code);

    void mpu_created() { mpu_created_.fetch_add(1, std::memory_order_relaxed); }
    void mpu_finished() { mpu_finished_.fetch_add(1, std::memory_order_relaxed); }

    // pool_stats 可空（未接线程池时省略对应指标）
    std::string render(const std::function<ThreadPool::Stats()>& pool_stats) const;

private:
    static size_t method_index(std::string_view m);

    static constexpr const char* kMethods[] = {"GET", "PUT", "POST", "DELETE", "HEAD", "other"};
    static constexpr size_t kMethodCount = 6;

    std::atomic<uint64_t> inflight_{0};
    std::atomic<uint64_t> by_method_[kMethodCount]{};
    std::atomic<uint64_t> by_status_class_[6]{};  // 1xx..5xx（下标 = 百位数）
    std::atomic<uint64_t> latency_hist_[kLatencyBuckets.size() + 1]{};
    std::atomic<uint64_t> latency_sum_us_{0};
    std::atomic<uint64_t> latency_count_{0};
    std::atomic<uint64_t> mpu_created_{0};
    std::atomic<uint64_t> mpu_finished_{0};  // complete + abort

    mutable std::mutex err_m_;
    std::map<std::string, uint64_t> errors_;  // wire code → count
};

}  // namespace lights3::s3
