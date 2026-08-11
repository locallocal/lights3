// 横切基础设施：后端级 metrics 注册机制（装配见 docs/storage-backend.md §6）。
// L2 请求维度指标仍由 s3::Metrics 负责；本文件让任意组件（存储后端及其
// meta/data 子件）注册 counter / gauge / histogram，随 GET /-/metrics 以
// Prometheus 文本格式追加输出。装配路径：main 建 MetricsRegistry →
// StorageRegistry::build 给每个后端派发带 backend=<name> 基础标签的
// MetricsScope → 后端在构造期领取实例、热路径无锁递增。
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

// 标签集：渲染按传入序输出（scope 基础标签在前）。值渲染时按 Prometheus
// 文本格式转义，键须为合法标签名（调用方保证）
using MetricLabels = std::vector<std::pair<std::string, std::string>>;

// 单调计数器；inc 为 relaxed 原子，热路径可安全调用
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

// 直方图：构造时定升序桶上界（渲染补 +Inf 末桶），observe 无锁
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
        std::vector<uint64_t> buckets;  // 与 bounds 对齐 + 末位溢出桶，非累计
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

// 注册表：同名同标签 get-or-create 返回同一实例（幂等，重启弃用等重复注册
// 无害）；同名不同类型/桶界视为装配错误抛 std::runtime_error。实例以
// shared_ptr 双持有——注册表供渲染、调用方供热路径，二者生命期互不牵制
class MetricsRegistry {
public:
    std::shared_ptr<MetricCounter> counter(const std::string& name, const std::string& help,
                                           const MetricLabels& labels = {});
    std::shared_ptr<MetricGauge> gauge(const std::string& name, const std::string& help,
                                       const MetricLabels& labels = {});
    std::shared_ptr<MetricHistogram> histogram(const std::string& name, const std::string& help,
                                               std::vector<double> bounds,
                                               const MetricLabels& labels = {});
    // 回调 gauge：渲染时拉取瞬时值（队列深度类指标免常驻原子）；回调在渲染
    // 线程执行，须自带线程安全且不得阻塞。同名同标签后注册者覆盖前者
    void gauge_callback(const std::string& name, const std::string& help,
                        std::function<double()> fn, const MetricLabels& labels = {});

    // 撤销某个标签值下的全部序列（含回调 gauge）。用于装配失败回滚：孤儿实例的
    // 回调闭包会持着已弃用对象的 shared_ptr，留在表里会一直渲染陈旧值
    void remove_labeled(const std::string& label_key, const std::string& label_value);

    // Prometheus 文本格式；家族按名字序、子实例按标签串序，输出稳定可断言
    std::string render() const;

private:
    enum class Kind { kCounter, kGauge, kHistogram };
    struct Family {
        Kind kind{};
        std::string help;
        std::vector<double> bounds;  // histogram 家族级桶界
        // key = 规范化标签串（如 backend="a",op="get"）
        std::map<std::string, std::shared_ptr<MetricCounter>> counters;
        std::map<std::string, std::shared_ptr<MetricGauge>> gauges;
        std::map<std::string, std::shared_ptr<MetricHistogram>> histograms;
        std::map<std::string, std::function<double()>> callbacks;
    };
    Family& family_of(const std::string& name, Kind kind, const std::string& help);  // 须持锁

    mutable std::mutex m_;
    std::map<std::string, Family> families_;
};

// 后端视角的注册句柄：携带注册表 + 基础标签（backend=<name>），后端内部再按需
// 追加维度标签（op/code 等）。默认构造为空 scope——返回未注册的孤立实例，
// 调用无害，测试直构后端无需装配 metrics
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

    // 派生子 scope（如 meta/data 子件维度）：基础标签追加 extra
    MetricsScope with(const MetricLabels& extra) const;

    explicit operator bool() const { return reg_ != nullptr; }

private:
    MetricLabels merged(const MetricLabels& extra) const;

    std::shared_ptr<MetricsRegistry> reg_;
    MetricLabels base_;
};

}  // namespace lights3
