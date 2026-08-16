// Cross-cutting infrastructure: backend-level metrics registration (wiring in
// docs/storage-backend.md §6). L2 request-dimension metrics remain the job of
// s3::Metrics; this file lets any component (storage backends and their meta/data
// subcomponents) register counters / gauges / histograms, appended to the
// GET /-/metrics output in Prometheus text format. Wiring path: main creates the
// MetricsRegistry -> StorageRegistry::build hands each backend a MetricsScope
// carrying the backend=<name> base label -> the backend obtains instances during
// construction and increments lock-free on the hot path.
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace lights3 {

// Label set: rendered in the order given (scope base labels first). Values are
// escaped per the Prometheus text format when rendered; keys must be valid label
// names (caller's responsibility)
using MetricLabels = std::vector<std::pair<std::string, std::string>>;

// Monotonic counter; inc is a relaxed atomic, safe to call on the hot path
class MetricCounter {
public:
    void inc(uint64_t n = 1) { v_.fetch_add(n, std::memory_order_relaxed); }
    uint64_t value() const { return v_.load(std::memory_order_relaxed); }

private:
    std::atomic<uint64_t> v_{0};
};

class MetricGauge {
public:
    void set(int64_t v) { v_.store(v, std::memory_order_relaxed); }
    void add(int64_t d) { v_.fetch_add(d, std::memory_order_relaxed); }
    void sub(int64_t d) { v_.fetch_sub(d, std::memory_order_relaxed); }
    int64_t value() const { return v_.load(std::memory_order_relaxed); }

private:
    std::atomic<int64_t> v_{0};
};

// Histogram: ascending bucket upper bounds fixed at construction (rendering adds
// the +Inf last bucket); observe is lock-free
class MetricHistogram {
public:
    explicit MetricHistogram(std::vector<double> bounds)
        : bounds_(std::move(bounds)), buckets_(bounds_.size() + 1) {}

    void observe(double v) {
        size_t b = 0;
        while (b < bounds_.size() && v > bounds_[b]) ++b;
        buckets_[b].fetch_add(1, std::memory_order_relaxed);
        sum_.fetch_add(v, std::memory_order_relaxed);
        count_.fetch_add(1, std::memory_order_relaxed);
    }

    struct Snapshot {
        std::vector<uint64_t> buckets;  // aligned with bounds + trailing overflow bucket, non-cumulative
        double sum = 0;
        uint64_t count = 0;
    };
    const std::vector<double>& bounds() const { return bounds_; }
    Snapshot snapshot() const;

private:
    std::vector<double> bounds_;
    std::vector<std::atomic<uint64_t>> buckets_;
    std::atomic<double> sum_{0};
    std::atomic<uint64_t> count_{0};
};

// Registry: get-or-create with the same name and labels returns the same instance
// (idempotent — duplicate registration from restarts/abandonment is harmless);
// the same name with a different type/bucket bounds is a wiring error and throws
// std::runtime_error. Instances are doubly held via shared_ptr — the registry for
// rendering, the caller for the hot path — so neither lifetime constrains the other
class MetricsRegistry {
public:
    std::shared_ptr<MetricCounter> counter(const std::string& name, const std::string& help,
                                           const MetricLabels& labels = {});
    std::shared_ptr<MetricGauge> gauge(const std::string& name, const std::string& help,
                                       const MetricLabels& labels = {});
    std::shared_ptr<MetricHistogram> histogram(const std::string& name, const std::string& help,
                                               std::vector<double> bounds,
                                               const MetricLabels& labels = {});
    // Callback gauge: pulls the instantaneous value at render time (queue-depth-style
    // metrics avoid a resident atomic); the callback runs on the rendering thread,
    // must be thread-safe on its own, and must not block. Later registration with
    // the same name and labels overrides the earlier one
    void gauge_callback(const std::string& name, const std::string& help,
                        std::function<double()> fn, const MetricLabels& labels = {});

    // Remove all series under a given label value (callback gauges included). Used
    // for rollback on wiring failure: an orphaned instance's callback closure holds
    // a shared_ptr to the abandoned object, and left in the table it would keep
    // rendering stale values
    void remove_labeled(const std::string& label_key, const std::string& label_value);

    // Prometheus text format; families in name order, child instances in label-string
    // order, so the output is stable and assertable
    std::string render() const;

private:
    enum class Kind { kCounter, kGauge, kHistogram };
    struct Family {
        Kind kind{};
        std::string help;
        std::vector<double> bounds;  // family-level histogram bucket bounds
        // key = normalized label string (e.g. backend="a",op="get")
        std::map<std::string, std::shared_ptr<MetricCounter>> counters;
        std::map<std::string, std::shared_ptr<MetricGauge>> gauges;
        std::map<std::string, std::shared_ptr<MetricHistogram>> histograms;
        std::map<std::string, std::function<double()>> callbacks;
    };
    Family& family_of(const std::string& name, Kind kind, const std::string& help);  // lock must be held

    mutable std::mutex m_;
    std::map<std::string, Family> families_;
};

// Registration handle from the backend's point of view: carries the registry plus
// base labels (backend=<name>); the backend appends dimension labels (op/code etc.)
// as needed. Default construction gives an empty scope — it returns unregistered,
// isolated instances, calls are harmless, and tests constructing backends directly
// need not wire up metrics
class MetricsScope {
public:
    MetricsScope() = default;
    MetricsScope(std::shared_ptr<MetricsRegistry> reg, MetricLabels base)
        : reg_(std::move(reg)), base_(std::move(base)) {}

    std::shared_ptr<MetricCounter> counter(const std::string& name, const std::string& help,
                                           const MetricLabels& extra = {}) const;
    std::shared_ptr<MetricGauge> gauge(const std::string& name, const std::string& help,
                                       const MetricLabels& extra = {}) const;
    std::shared_ptr<MetricHistogram> histogram(const std::string& name, const std::string& help,
                                               std::vector<double> bounds,
                                               const MetricLabels& extra = {}) const;
    void gauge_callback(const std::string& name, const std::string& help,
                        std::function<double()> fn, const MetricLabels& extra = {}) const;

    // Derive a child scope (e.g. the meta/data subcomponent dimension): base labels with extra appended
    MetricsScope with(const MetricLabels& extra) const;

    explicit operator bool() const { return reg_ != nullptr; }

private:
    MetricLabels merged(const MetricLabels& extra) const;

    std::shared_ptr<MetricsRegistry> reg_;
    MetricLabels base_;
};

}  // namespace lights3
