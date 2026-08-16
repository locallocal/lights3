// L2: request metrics (docs/s3-protocol.md §7), emitted by GET /-/metrics in Prometheus text format
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

// Admission snapshot of ingress throttling (the runtime.max_inflight_requests semaphore) (docs/gaps.md §7):
// under load testing, these two numbers distinguish "stuck at admission" from "stuck in the pool"
struct AdmissionStats {
    long capacity = 0;   // total permits
    long available = 0;  // remaining permits (capacity - available = in flight)
    size_t waiting = 0;  // requests queued on the semaphore
};

class Metrics {
public:
    // Latency histogram bucket upper bounds (seconds); last bucket is +Inf
    static constexpr std::array<double, 6> kLatencyBuckets{0.005, 0.02, 0.1, 0.5, 2.0, 10.0};

    void request_start() { inflight_.fetch_add(1, std::memory_order_relaxed); }
    void request_end(std::string_view method, int status, double seconds);
    // Lock-free (docs/gaps.md §4: previously every error response contended on one global mutex): the code set
    // is bounded and shares its source with the enum; fixed-size atomic array indexed by enum value
    void s3_error(S3ErrorCode code) {
        errors_[size_t(code)].fetch_add(1, std::memory_order_relaxed);
    }

    void mpu_created() { mpu_created_.fetch_add(1, std::memory_order_relaxed); }
    void mpu_finished() { mpu_finished_.fetch_add(1, std::memory_order_relaxed); }

    // Byte counts and per-bucket dimension (docs/gaps.md §7). bucket may be empty (service-level
    // requests count only globally); tracked bucket count is capped, overflow folds into "_other" to prevent label cardinality explosion
    void add_bytes_in(std::string_view bucket, uint64_t n);
    void add_bytes_out(std::string_view bucket, uint64_t n);
    void record_bucket_request(std::string_view bucket);

    // pool_stats / admission / timer_stats may each be empty (corresponding metrics omitted when not wired up)
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
    // Returns the slot under lock; new buckets beyond the cap share the "_other" slot
    BucketStats& bucket_slot_locked(std::string_view bucket);

    std::atomic<uint64_t> inflight_{0};
    std::atomic<uint64_t> by_method_[kMethodCount]{};
    std::atomic<uint64_t> by_status_class_[6]{};  // 1xx..5xx (index = hundreds digit)
    std::atomic<uint64_t> latency_hist_[kLatencyBuckets.size() + 1]{};
    std::atomic<uint64_t> latency_sum_us_{0};
    std::atomic<uint64_t> latency_count_{0};
    std::atomic<uint64_t> mpu_created_{0};
    std::atomic<uint64_t> mpu_finished_{0};  // complete + abort
    std::atomic<uint64_t> bytes_in_{0};
    std::atomic<uint64_t> bytes_out_{0};

    // per-bucket table: hot path locks once per 64KiB chunk, critical section is just a map lookup + integer add;
    // a tier below the request main path's lock-free atomics but acceptable (the bucket dimension inherently needs a name key)
    mutable std::mutex bucket_m_;
    std::map<std::string, BucketStats, std::less<>> by_bucket_;

    std::atomic<uint64_t> errors_[kS3ErrorCodeCount]{};  // indexed by S3ErrorCode
};

}  // namespace lights3::s3
