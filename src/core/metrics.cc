#include "core/metrics.h"

#include <charconv>
#include <cmath>
#include <sstream>
#include <stdexcept>

#include "core/log.h"

namespace lights3 {

namespace {

// Render floats in shortest round-trip form (docs/gaps.md §4): ostream's default
// 6 significant digits would render bucket bounds >=1e6 as "1.04858e+06", and when
// nearby bounds collapse into duplicate le sequences Prometheus rejects the entire
// target outright
std::string fmt_double(double v) {
    // Non-finite values must use Prometheus's spelling: to_chars would emit
    // "inf"/"-inf"/"nan", and the scraper likewise ends up scrapping the whole
    // target — the same failure mode as bucket-bound collapse
    if (std::isnan(v)) return "NaN";
    if (std::isinf(v)) return v > 0 ? "+Inf" : "-Inf";
    char buf[32];
    auto [p, ec] = std::to_chars(buf, buf + sizeof(buf), v);
    return std::string(buf, p);
}

// # HELP text escaping (docs/gaps.md §4: label values were escaped, help
// previously was not): the spec requires \ -> \\ and newline -> \n
std::string escape_help(const std::string& v) {
    std::string out;
    out.reserve(v.size());
    for (char c : v) {
        if (c == '\\')
            out += "\\\\";
        else if (c == '\n')
            out += "\\n";
        else
            out += c;
    }
    return out;
}

// Label value escaping for the Prometheus text format: \ -> \\, " -> \", newline -> \n
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

// Normalized label string (no braces): backend="a",op="get"; empty label set yields an empty string
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

// Series name + labels: name{...}; histogram bucket lines append le after the
// existing labels via the extra parameter
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
    } else if (!help.empty() && it->second.help != help) {
        // Keep the first help (a family renders only one # HELP line), but no longer
        // silently (docs/gaps.md §4)
        LOG_WARN("metric '{}' re-registered with different help text; keeping the first",
                 name);
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

void MetricsRegistry::remove_labeled(const std::string& label_key,
                                     const std::string& label_value) {
    std::string needle = label_key + "=\"";
    append_escaped(needle, label_value);
    needle += '"';
    auto hit = [&](const std::string& ls) {
        // Label strings look like a="1",backend="x": a match must land on separator
        // boundaries, so backend="x" cannot hit prefix look-alikes such as backend="xy"
        for (size_t at = ls.find(needle); at != std::string::npos;
             at = ls.find(needle, at + 1)) {
            bool left_ok = at == 0 || ls[at - 1] == ',';
            size_t end = at + needle.size();
            bool right_ok = end == ls.size() || ls[end] == ',';
            if (left_ok && right_ok) return true;
        }
        return false;
    };
    std::lock_guard lk(m_);
    for (auto& [name, fam] : families_) {
        (void)name;
        auto prune = [&](auto& map) {
            for (auto it = map.begin(); it != map.end();)
                it = hit(it->first) ? map.erase(it) : std::next(it);
        };
        prune(fam.counters);
        prune(fam.gauges);
        prune(fam.histograms);
        prune(fam.callbacks);
    }
}

std::string MetricsRegistry::render() const {
    // Callback gauges are evaluated outside the lock: a callback may be slow or even
    // touch the registry in turn (calling under the lock would deadlock). First copy
    // the callback list under the lock -> evaluate outside -> render under the lock
    // again, so same-name samples follow directly after their # TYPE line (the
    // family-grouping requirement of the Prometheus text format). Callbacks newly
    // registered between the two lock holds lack a value this round: skip the
    // sample, pick it up next round
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
        if (!fam.help.empty()) os << "# HELP " << name << " " << escape_help(fam.help) << "\n";
        os << "# TYPE " << name << " " << kind_str(int(fam.kind)) << "\n";
        for (const auto& [ls, c] : fam.counters)
            os << series(name, ls) << " " << c->value() << "\n";
        for (const auto& [ls, g] : fam.gauges)
            os << series(name, ls) << " " << g->value() << "\n";
        for (const auto& [ls, fn] : fam.callbacks) {
            auto it = cb_vals.find({name, ls});
            if (it != cb_vals.end())
                os << series(name, ls) << " " << fmt_double(it->second) << "\n";
        }
        for (const auto& [ls, h] : fam.histograms) {
            auto snap = h->snapshot();
            uint64_t cum = 0;
            for (size_t i = 0; i < fam.bounds.size(); ++i) {
                cum += snap.buckets[i];
                os << series(name + "_bucket", ls, "le=\"" + fmt_double(fam.bounds[i]) + "\"")
                   << " " << cum << "\n";
            }
            cum += snap.buckets[fam.bounds.size()];
            os << series(name + "_bucket", ls, "le=\"+Inf\"") << " " << cum << "\n";
            os << series(name + "_sum", ls) << " " << fmt_double(snap.sum) << "\n";
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
