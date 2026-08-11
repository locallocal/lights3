// L2: 请求指标（docs/s3-protocol.md §7），GET /-/metrics 以 Prometheus 文本格式输出
#pragma once

#include <array>
#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <mutex>
#include <string>
#include <string_view>

#include "core/thread_pool.h"
#include "core/timer.h"
#include "s3/errors.h"

namespace lights3::s3 {

// 入口限流（runtime.max_inflight_requests 信号量）的准入快照（docs/gaps.md §7）：
// 压测时"卡在准入"还是"卡在池"靠这两个数区分
struct AdmissionStats {
    long capacity = 0;   // 总许可数
    long available = 0;  // 剩余许可（capacity - available = 在途）
    size_t waiting = 0;  // 排在信号量上的请求数
};

class Metrics {
public:
    // 延迟直方图桶上界（秒），末桶为 +Inf
    static constexpr std::array<double, 6> kLatencyBuckets{0.005, 0.02, 0.1, 0.5, 2.0, 10.0};

    void request_start() { inflight_.fetch_add(1, std::memory_order_relaxed); }
    void request_end(std::string_view method, int status, double seconds);
    // 免锁（docs/gaps.md §4：此前每个错误响应抢一把全局互斥）：码集有界且与
    // 枚举同源，定长原子数组按枚举值下标
    void s3_error(S3ErrorCode code) {
        errors_[size_t(code)].fetch_add(1, std::memory_order_relaxed);
    }

    void mpu_created() { mpu_created_.fetch_add(1, std::memory_order_relaxed); }
    void mpu_finished() { mpu_finished_.fetch_add(1, std::memory_order_relaxed); }

    // 字节数与 per-bucket 维度（docs/gaps.md §7）。bucket 可为空（service 级
    // 请求只计全局）；跟踪的 bucket 数有上限，溢出并入 "_other" 防标签基数爆炸
    void add_bytes_in(std::string_view bucket, uint64_t n);
    void add_bytes_out(std::string_view bucket, uint64_t n);
    void record_bucket_request(std::string_view bucket);

    // pool_stats / admission / timer_stats 均可空（未接线时省略对应指标）
    std::string render(const std::function<ThreadPool::Stats()>& pool_stats,
                       const std::function<AdmissionStats()>& admission = {},
                       const std::function<TimerQueue::Stats()>& timer_stats = {}) const;

private:
    static size_t method_index(std::string_view m);

    static constexpr const char* kMethods[] = {"GET", "PUT", "POST", "DELETE", "HEAD", "other"};
    static constexpr size_t kMethodCount = 6;
    static constexpr size_t kMaxTrackedBuckets = 512;

    struct BucketStats {
        uint64_t requests = 0;
        uint64_t bytes_in = 0;
        uint64_t bytes_out = 0;
    };
    // 上锁返回槽位；超过上限的新 bucket 共享 "_other" 槽
    BucketStats& bucket_slot_locked(std::string_view bucket);

    std::atomic<uint64_t> inflight_{0};
    std::atomic<uint64_t> by_method_[kMethodCount]{};
    std::atomic<uint64_t> by_status_class_[6]{};  // 1xx..5xx（下标 = 百位数）
    std::atomic<uint64_t> latency_hist_[kLatencyBuckets.size() + 1]{};
    std::atomic<uint64_t> latency_sum_us_{0};
    std::atomic<uint64_t> latency_count_{0};
    std::atomic<uint64_t> mpu_created_{0};
    std::atomic<uint64_t> mpu_finished_{0};  // complete + abort
    std::atomic<uint64_t> bytes_in_{0};
    std::atomic<uint64_t> bytes_out_{0};

    // per-bucket 表：热路径每 64KiB 块一次加锁，临界区只有 map 查找 + 整数加，
    // 与请求主路径的免锁原子计数不同档但可接受（bucket 维度天然要有名字做键）
    mutable std::mutex bucket_m_;
    std::map<std::string, BucketStats, std::less<>> by_bucket_;

    std::atomic<uint64_t> errors_[kS3ErrorCodeCount]{};  // 按 S3ErrorCode 下标
};

}  // namespace lights3::s3
