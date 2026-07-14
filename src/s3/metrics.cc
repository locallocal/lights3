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

void Metrics::s3_error(const char* wire_code) {
    std::lock_guard lk(err_m_);
    ++errors_[wire_code];
}

std::string Metrics::render(const std::function<ThreadPool::Stats()>& pool_stats) const {
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
    {
        std::lock_guard lk(err_m_);
        for (auto& [code, n] : errors_)
            os << "lights3_s3_errors_total{code=\"" << code << "\"} " << n << "\n";
    }

    os << "# TYPE lights3_multipart_active gauge\n";
    uint64_t created = mpu_created_.load(std::memory_order_relaxed);
    uint64_t finished = mpu_finished_.load(std::memory_order_relaxed);
    os << "lights3_multipart_active " << (created > finished ? created - finished : 0) << "\n";

    if (pool_stats) {
        auto st = pool_stats();
        os << "# TYPE lights3_pool_queue_depth gauge\n";
        os << "lights3_pool_queue_depth " << st.queue_depth << "\n";
        os << "# TYPE lights3_pool_backlogged gauge\n";
        os << "lights3_pool_backlogged " << st.backlogged << "\n";
        os << "# TYPE lights3_pool_completed_total counter\n";
        os << "lights3_pool_completed_total " << st.completed << "\n";
    }
    return os.str();
}

}  // namespace lights3::s3
