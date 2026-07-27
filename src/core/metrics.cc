#include "core/metrics.h"

#include <sstream>
#include <stdexcept>

namespace lights3 {

namespace {

// Prometheus 文本格式的标签值转义：\ → \\、" → \"、换行 → \n
void append_escaped(std::string& out, const std::string& v) {
    for (char c : v) {
        if (c == '\\')
            out += "\\\\";
        else if (c == '"')
            out += "\\\"";
        else if (c == '\n')
            out += "\\n";
        else
            out += c;
    }
}

// 规范化标签串（无花括号）：backend="a",op="get"；空标签集为空串
std::string label_str(const MetricLabels& labels) {
    std::string s;
    for (const auto& [k, v] : labels) {
        if (!s.empty()) s += ',';
        s += k;
        s += "=\"";
        append_escaped(s, v);
        s += '"';
    }
    return s;
}

// 序列名 + 标签：name{...}；直方图桶行需在已有标签后续接 le，走 extra 形参
std::string series(const std::string& name, const std::string& labels,
                   const std::string& extra = "") {
    std::string all = labels;
    if (!extra.empty()) {
        if (!all.empty()) all += ',';
        all += extra;
    }
    return all.empty() ? name : name + "{" + all + "}";
}

const char* kind_str(int k) {
    switch (k) {
        case 0: return "counter";
        case 1: return "gauge";
        default: return "histogram";
    }
}

}  // namespace

MetricHistogram::Snapshot MetricHistogram::snapshot() const {
    Snapshot s;
    s.buckets.reserve(buckets_.size());
    for (const auto& b : buckets_) s.buckets.push_back(b.load(std::memory_order_relaxed));
    s.sum = sum_.load(std::memory_order_relaxed);
    s.count = count_.load(std::memory_order_relaxed);
    return s;
}

MetricsRegistry::Family& MetricsRegistry::family_of(const std::string& name, Kind kind,
                                                    const std::string& help) {
    auto [it, inserted] = families_.try_emplace(name);
    if (inserted) {
        it->second.kind = kind;
        it->second.help = help;
    } else if (it->second.kind != kind) {
        throw std::runtime_error("metric '" + name + "' re-registered as " +
                                 kind_str(int(kind)) + ", was " +
                                 kind_str(int(it->second.kind)));
    }
    return it->second;
}

std::shared_ptr<MetricCounter> MetricsRegistry::counter(const std::string& name,
                                                        const std::string& help,
                                                        const MetricLabels& labels) {
    std::lock_guard lk(m_);
    auto& c = family_of(name, Kind::kCounter, help).counters[label_str(labels)];
    if (!c) c = std::make_shared<MetricCounter>();
    return c;
}

std::shared_ptr<MetricGauge> MetricsRegistry::gauge(const std::string& name,
                                                    const std::string& help,
                                                    const MetricLabels& labels) {
    std::lock_guard lk(m_);
    auto& g = family_of(name, Kind::kGauge, help).gauges[label_str(labels)];
    if (!g) g = std::make_shared<MetricGauge>();
    return g;
}

std::shared_ptr<MetricHistogram> MetricsRegistry::histogram(const std::string& name,
                                                            const std::string& help,
                                                            std::vector<double> bounds,
                                                            const MetricLabels& labels) {
    std::lock_guard lk(m_);
    auto& fam = family_of(name, Kind::kHistogram, help);
    if (fam.histograms.empty() && fam.callbacks.empty())
        fam.bounds = bounds;
    else if (fam.bounds != bounds)
        throw std::runtime_error("metric '" + name + "' re-registered with different buckets");
    auto& h = fam.histograms[label_str(labels)];
    if (!h) h = std::make_shared<MetricHistogram>(std::move(bounds));
    return h;
}

void MetricsRegistry::gauge_callback(const std::string& name, const std::string& help,
                                     std::function<double()> fn, const MetricLabels& labels) {
    std::lock_guard lk(m_);
    family_of(name, Kind::kGauge, help).callbacks[label_str(labels)] = std::move(fn);
}

std::string MetricsRegistry::render() const {
    // 回调 gauge 在锁外求值：回调可能耗时甚至反过来触碰注册表（锁内调用会死锁）。
    // 先持锁抄回调清单 → 锁外求值 → 再持锁渲染，同名样本紧跟其 # TYPE 行
    // （Prometheus 文本格式的家族分组要求）。两次持锁间新注册的回调本轮缺值，
    // 跳过样本、下轮补上
    std::vector<std::tuple<std::string, std::string, std::function<double()>>> cbs;
    {
        std::lock_guard lk(m_);
        for (const auto& [name, fam] : families_)
            for (const auto& [ls, fn] : fam.callbacks) cbs.emplace_back(name, ls, fn);
    }
    std::map<std::pair<std::string, std::string>, double> cb_vals;
    for (auto& [name, ls, fn] : cbs) cb_vals[{name, ls}] = fn();

    std::ostringstream os;
    std::lock_guard lk(m_);
    for (const auto& [name, fam] : families_) {
        if (!fam.help.empty()) os << "# HELP " << name << " " << fam.help << "\n";
        os << "# TYPE " << name << " " << kind_str(int(fam.kind)) << "\n";
        for (const auto& [ls, c] : fam.counters)
            os << series(name, ls) << " " << c->value() << "\n";
        for (const auto& [ls, g] : fam.gauges)
            os << series(name, ls) << " " << g->value() << "\n";
        for (const auto& [ls, fn] : fam.callbacks) {
            auto it = cb_vals.find({name, ls});
            if (it != cb_vals.end()) os << series(name, ls) << " " << it->second << "\n";
        }
        for (const auto& [ls, h] : fam.histograms) {
            auto snap = h->snapshot();
            uint64_t cum = 0;
            for (size_t i = 0; i < fam.bounds.size(); ++i) {
                cum += snap.buckets[i];
                std::ostringstream le;
                le << "le=\"" << fam.bounds[i] << "\"";
                os << series(name + "_bucket", ls, le.str()) << " " << cum << "\n";
            }
            cum += snap.buckets[fam.bounds.size()];
            os << series(name + "_bucket", ls, "le=\"+Inf\"") << " " << cum << "\n";
            os << series(name + "_sum", ls) << " " << snap.sum << "\n";
            os << series(name + "_count", ls) << " " << snap.count << "\n";
        }
    }
    return os.str();
}

MetricLabels MetricsScope::merged(const MetricLabels& extra) const {
    MetricLabels out = base_;
    out.insert(out.end(), extra.begin(), extra.end());
    return out;
}

std::shared_ptr<MetricCounter> MetricsScope::counter(const std::string& name,
                                                     const std::string& help,
                                                     const MetricLabels& extra) const {
    if (!reg_) return std::make_shared<MetricCounter>();
    return reg_->counter(name, help, merged(extra));
}

std::shared_ptr<MetricGauge> MetricsScope::gauge(const std::string& name,
                                                 const std::string& help,
                                                 const MetricLabels& extra) const {
    if (!reg_) return std::make_shared<MetricGauge>();
    return reg_->gauge(name, help, merged(extra));
}

std::shared_ptr<MetricHistogram> MetricsScope::histogram(const std::string& name,
                                                         const std::string& help,
                                                         std::vector<double> bounds,
                                                         const MetricLabels& extra) const {
    if (!reg_) return std::make_shared<MetricHistogram>(std::move(bounds));
    return reg_->histogram(name, help, std::move(bounds), merged(extra));
}

void MetricsScope::gauge_callback(const std::string& name, const std::string& help,
                                  std::function<double()> fn, const MetricLabels& extra) const {
    if (!reg_) return;
    reg_->gauge_callback(name, help, std::move(fn), merged(extra));
}

MetricsScope MetricsScope::with(const MetricLabels& extra) const {
    return MetricsScope(reg_, merged(extra));
}

}  // namespace lights3
