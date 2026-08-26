#include "s3/metrics.h"

#include <sstream>

namespace lights3::s3 {

size_t Metrics::method_index(std::string_view m) {
    for (size_t i = 0; i + 1 < kMethodCount; ++i)
        if (m == kMethods[i]) return i;
    return kMethodCount - 1;
}

void Metrics::request_end(std::string_view method, int status, double seconds) {
    inflight_.fetch_sub(1, std::memory_order_relaxed);
    by_method_[method_index(method)].fetch_add(1, std::memory_order_relaxed);
    int cls = status / 100;
    if (cls >= 1 && cls <= 5) by_status_class_[cls].fetch_add(1, std::memory_order_relaxed);

    size_t b = 0;
    while (b < kLatencyBuckets.size() && seconds > kLatencyBuckets[b]) ++b;
    latency_hist_[b].fetch_add(1, std::memory_order_relaxed);
    latency_sum_us_.fetch_add(static_cast<uint64_t>(seconds * 1e6), std::memory_order_relaxed);
    latency_count_.fetch_add(1, std::memory_order_relaxed);
}

Metrics::BucketStats& Metrics::bucket_slot_locked(std::string_view bucket) {
    if (auto it = by_bucket_.find(bucket); it != by_bucket_.end()) return it->second;
    if (by_bucket_.size() >= kMaxTrackedBuckets) return by_bucket_["_other"];
    return by_bucket_[std::string(bucket)];
}

void Metrics::add_bytes_in(std::string_view bucket, uint64_t n) {
    if (n == 0) return;
    bytes_in_.fetch_add(n, std::memory_order_relaxed);
    if (bucket.empty()) return;
    std::lock_guard lk(bucket_m_);
    bucket_slot_locked(bucket).bytes_in += n;
}

void Metrics::add_bytes_out(std::string_view bucket, uint64_t n) {
    if (n == 0) return;
    bytes_out_.fetch_add(n, std::memory_order_relaxed);
    if (bucket.empty()) return;
    std::lock_guard lk(bucket_m_);
    bucket_slot_locked(bucket).bytes_out += n;
}

void Metrics::record_bucket_request(std::string_view bucket) {
    if (bucket.empty()) return;
    std::lock_guard lk(bucket_m_);
    bucket_slot_locked(bucket).requests += 1;
}

std::string Metrics::render(const std::function<ThreadPool::Stats()>& pool_stats,
                            const std::function<AdmissionStats()>& admission,
                            const std::function<TimerQueue::Stats()>& timer_stats) const {
    std::ostringstream os;

    os << "# TYPE lights3_requests_total counter\n";
    for (size_t i = 0; i < kMethodCount; ++i)
        os << "lights3_requests_total{method=\"" << kMethods[i] << "\"} "
           << by_method_[i].load(std::memory_order_relaxed) << "\n";

    os << "# TYPE lights3_responses_total counter\n";
    for (int cls = 2; cls <= 5; ++cls)
        os << "lights3_responses_total{class=\"" << cls << "xx\"} "
           << by_status_class_[cls].load(std::memory_order_relaxed) << "\n";

    os << "# TYPE lights3_inflight_requests gauge\n";
    os << "lights3_inflight_requests " << inflight_.load(std::memory_order_relaxed) << "\n";

    os << "# TYPE lights3_request_duration_seconds histogram\n";
    uint64_t cum = 0;
    for (size_t i = 0; i < kLatencyBuckets.size(); ++i) {
        cum += latency_hist_[i].load(std::memory_order_relaxed);
        os << "lights3_request_duration_seconds_bucket{le=\"" << kLatencyBuckets[i] << "\"} "
           << cum << "\n";
    }
    cum += latency_hist_[kLatencyBuckets.size()].load(std::memory_order_relaxed);
    os << "lights3_request_duration_seconds_bucket{le=\"+Inf\"} " << cum << "\n";
    os << "lights3_request_duration_seconds_sum "
       << latency_sum_us_.load(std::memory_order_relaxed) / 1e6 << "\n";
    os << "lights3_request_duration_seconds_count "
       << latency_count_.load(std::memory_order_relaxed) << "\n";

    os << "# TYPE lights3_s3_errors_total counter\n";
    for (size_t i = 0; i < kS3ErrorCodeCount; ++i) {
        uint64_t n = errors_[i].load(std::memory_order_relaxed);
        // Only render codes that have occurred (matches the old map behavior; avoids 25 always-zero series)
        if (n > 0)
            os << "lights3_s3_errors_total{code=\"" << wire_code(S3ErrorCode(i)) << "\"} "
               << n << "\n";
    }

    os << "# TYPE lights3_multipart_active gauge\n";
    uint64_t created = mpu_created_.load(std::memory_order_relaxed);
    uint64_t finished = mpu_finished_.load(std::memory_order_relaxed);
    os << "lights3_multipart_active " << (created > finished ? created - finished : 0) << "\n";

    // Byte counts and per-bucket dimension (docs/archive/gaps.md §7)
    os << "# TYPE lights3_bytes_total counter\n";
    os << "lights3_bytes_total{direction=\"in\"} "
       << bytes_in_.load(std::memory_order_relaxed) << "\n";
    os << "lights3_bytes_total{direction=\"out\"} "
       << bytes_out_.load(std::memory_order_relaxed) << "\n";
    {
        std::lock_guard lk(bucket_m_);
        if (!by_bucket_.empty()) {
            os << "# TYPE lights3_bucket_requests_total counter\n";
            for (auto& [name, st] : by_bucket_)
                os << "lights3_bucket_requests_total{bucket=\"" << name << "\"} " << st.requests
                   << "\n";
            os << "# TYPE lights3_bucket_bytes_total counter\n";
            for (auto& [name, st] : by_bucket_) {
                os << "lights3_bucket_bytes_total{bucket=\"" << name
                   << "\",direction=\"in\"} " << st.bytes_in << "\n";
                os << "lights3_bucket_bytes_total{bucket=\"" << name
                   << "\",direction=\"out\"} " << st.bytes_out << "\n";
            }
        }
    }

    if (pool_stats) {
        auto st = pool_stats();
        os << "# TYPE lights3_pool_queue_depth gauge\n";
        os << "lights3_pool_queue_depth " << st.queue_depth << "\n";
        os << "# TYPE lights3_pool_backlogged gauge\n";
        os << "lights3_pool_backlogged " << st.backlogged << "\n";
        os << "# TYPE lights3_pool_completed_total counter\n";
        os << "lights3_pool_completed_total " << st.completed << "\n";
        // Wait-duration histogram (docs/archive/gaps.md §7): docs/concurrency.md §3.1 defines "this histogram
        // shifting right" as the sole criterion for enabling dedicated per-backend pools; it used to be collected but never emitted
        os << "# TYPE lights3_pool_wait_seconds histogram\n";
        uint64_t wcum = 0;
        for (size_t i = 0; i < ThreadPool::kWaitBucketBounds.size(); ++i) {
            wcum += st.wait_hist[i];
            os << "lights3_pool_wait_seconds_bucket{le=\"" << ThreadPool::kWaitBucketBounds[i]
               << "\"} " << wcum << "\n";
        }
        wcum += st.wait_hist[ThreadPool::kWaitBuckets - 1];
        os << "lights3_pool_wait_seconds_bucket{le=\"+Inf\"} " << wcum << "\n";
        os << "lights3_pool_wait_seconds_sum " << st.wait_sum_us / 1e6 << "\n";
        os << "lights3_pool_wait_seconds_count " << wcum << "\n";
    }

    // Ingress throttling queue depth (docs/archive/gaps.md §7): the inflight semaphore is the process-wide sole admission gate
    if (admission) {
        auto st = admission();
        os << "# TYPE lights3_admission_capacity gauge\n";
        os << "lights3_admission_capacity " << st.capacity << "\n";
        os << "# TYPE lights3_admission_available gauge\n";
        os << "lights3_admission_available " << st.available << "\n";
        os << "# TYPE lights3_admission_waiting gauge\n";
        os << "lights3_admission_waiting " << st.waiting << "\n";
    }

    // Timer thread health (docs/archive/gaps.md §7): slow callbacks cascade into delaying tiered scans /
    // duostore GC / credential sync; head-of-queue lag and the duration histogram are the only way to detect it
    if (timer_stats) {
        auto st = timer_stats();
        os << "# TYPE lights3_timer_pending gauge\n";
        os << "lights3_timer_pending " << st.pending << "\n";
        os << "# TYPE lights3_timer_due_queue gauge\n";
        os << "lights3_timer_due_queue " << st.due << "\n";
        os << "# TYPE lights3_timer_lag_seconds gauge\n";
        os << "lights3_timer_lag_seconds " << st.lag_seconds << "\n";
        os << "# TYPE lights3_timer_slow_callbacks_total counter\n";
        os << "lights3_timer_slow_callbacks_total " << st.slow << "\n";
        os << "# TYPE lights3_timer_callback_seconds histogram\n";
        static constexpr double kExecBounds[] = {0.01, 0.1, 1.0, 10.0};
        uint64_t tcum = 0;
        for (size_t i = 0; i < 4; ++i) {
            tcum += st.exec_hist[i];
            os << "lights3_timer_callback_seconds_bucket{le=\"" << kExecBounds[i] << "\"} "
               << tcum << "\n";
        }
        tcum += st.exec_hist[TimerQueue::kExecBuckets - 1];
        os << "lights3_timer_callback_seconds_bucket{le=\"+Inf\"} " << tcum << "\n";
        os << "lights3_timer_callback_seconds_sum " << st.exec_sum_us / 1e6 << "\n";
        os << "lights3_timer_callback_seconds_count " << tcum << "\n";
    }
    return os.str();
}

}  // namespace lights3::s3
