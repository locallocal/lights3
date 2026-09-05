// Unit tests for the backend-level metrics registration mechanism (core/metrics.h): instance reuse/type conflicts/label escaping/
// histogram rendering/callback gauges/harmless empty scope/concurrent-increment smoke test
#include <limits>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "core/metrics.h"
#include "core/timer.h"
#include "s3/metrics.h"
#include "unit/mini_test.h"

using namespace lights3;

namespace {

bool contains(const std::string& s, const std::string& sub) {
    return s.find(sub) != std::string::npos;
}

}  // namespace

TEST(metrics_counter_gauge_render) {
    MetricsRegistry reg;
    auto c = reg.counter("lights3_test_ops_total", "ops", {{"backend", "b1"}});
    c->inc();
    c->inc(41);
    auto g = reg.gauge("lights3_test_depth", "depth");
    g->set(7);
    g->add(5);
    g->sub(2);

    auto out = reg.render();
    CHECK(contains(out, "# HELP lights3_test_ops_total ops\n"));
    CHECK(contains(out, "# TYPE lights3_test_ops_total counter\n"));
    CHECK(contains(out, "lights3_test_ops_total{backend=\"b1\"} 42\n"));
    CHECK(contains(out, "# TYPE lights3_test_depth gauge\n"));
    CHECK(contains(out, "lights3_test_depth 10\n"));
}

TEST(metrics_get_or_create_identity) {
    MetricsRegistry reg;
    // Same name and labels -> same instance (repeated registration is idempotent); same name, different labels -> new child instance within the family
    auto a = reg.counter("lights3_test_total", "", {{"k", "a"}});
    auto a2 = reg.counter("lights3_test_total", "", {{"k", "a"}});
    auto b = reg.counter("lights3_test_total", "", {{"k", "b"}});
    CHECK(a.get() == a2.get());
    CHECK(a.get() != b.get());
    a->inc(3);
    b->inc(1);
    auto out = reg.render();
    CHECK(contains(out, "lights3_test_total{k=\"a\"} 3\n"));
    CHECK(contains(out, "lights3_test_total{k=\"b\"} 1\n"));
    // # TYPE emitted only once per family
    CHECK_EQ(out.find("# TYPE lights3_test_total"),
             out.rfind("# TYPE lights3_test_total"));

    // Same name with different types = assembly error
    bool thrown = false;
    try {
        reg.gauge("lights3_test_total", "");
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    CHECK(thrown);
}

TEST(metrics_label_escaping) {
    MetricsRegistry reg;
    reg.counter("lights3_test_esc_total", "", {{"v", "a\"b\\c\nd"}})->inc();
    auto out = reg.render();
    CHECK(contains(out, "lights3_test_esc_total{v=\"a\\\"b\\\\c\\nd\"} 1\n"));
}

TEST(metrics_histogram_render) {
    MetricsRegistry reg;
    auto h = reg.histogram("lights3_test_seconds", "lat", {0.1, 1.0}, {{"op", "get"}});
    h->observe(0.05);
    h->observe(0.05);
    h->observe(0.5);
    h->observe(30);  // overflow bucket
    auto out = reg.render();
    CHECK(contains(out, "# TYPE lights3_test_seconds histogram\n"));
    CHECK(contains(out, "lights3_test_seconds_bucket{op=\"get\",le=\"0.1\"} 2\n"));
    CHECK(contains(out, "lights3_test_seconds_bucket{op=\"get\",le=\"1\"} 3\n"));
    CHECK(contains(out, "lights3_test_seconds_bucket{op=\"get\",le=\"+Inf\"} 4\n"));
    CHECK(contains(out, "lights3_test_seconds_count{op=\"get\"} 4\n"));
    CHECK(contains(out, "lights3_test_seconds_sum{op=\"get\"} 30.6\n"));

    // Same-name histograms with inconsistent bucket bounds = assembly error
    bool thrown = false;
    try {
        reg.histogram("lights3_test_seconds", "lat", {0.5, 5.0}, {{"op", "put"}});
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    CHECK(thrown);
}

TEST(metrics_nonfinite_render) {
    // Non-finite values must use the Prometheus spelling (docs/archive/gaps.md §4): the raw to_chars output
    // "inf"/"nan" would make the scraper reject the entire target, same failure mode as bucket-bound collapsing
    MetricsRegistry reg;
    reg.gauge_callback("lights3_test_nf_pos", "cb",
                       [] { return std::numeric_limits<double>::infinity(); });
    reg.gauge_callback("lights3_test_nf_neg", "cb",
                       [] { return -std::numeric_limits<double>::infinity(); });
    reg.gauge_callback("lights3_test_nf_nan", "cb",
                       [] { return std::numeric_limits<double>::quiet_NaN(); });
    auto out = reg.render();
    CHECK(contains(out, "lights3_test_nf_pos +Inf\n"));
    CHECK(contains(out, "lights3_test_nf_neg -Inf\n"));
    CHECK(contains(out, "lights3_test_nf_nan NaN\n"));
    CHECK(!contains(out, "inf\n"));
    CHECK(!contains(out, "nan\n"));
}

TEST(metrics_large_bucket_bounds_render) {
    // Bucket bounds >= 1e6 were once rendered with 6 significant digits as "1.04858e+06", and nearby bounds could even collapse
    // into duplicate le sequences -- Prometheus rejects the entire target on a duplicate le
    MetricsRegistry reg;
    auto h = reg.histogram("lights3_test_bytes", "sz", {1048576.0, 1048577.0, 1e9});
    h->observe(1.0);
    auto out = reg.render();
    CHECK(contains(out, "le=\"1048576\""));
    CHECK(contains(out, "le=\"1048577\""));  // must not collapse into the same le as the previous bucket
    CHECK(!contains(out, "e+06"));
    // Shortest round-trip allows exponent notation (1e+09 reads back exactly, Prometheus accepts it); the key point is no precision loss
    CHECK(contains(out, "le=\"1e+09\""));
}

TEST(metrics_gauge_callback) {
    MetricsRegistry reg;
    int depth = 3;
    reg.gauge_callback("lights3_test_cb_depth", "cb", [&] { return double(depth); },
                       {{"backend", "b1"}});
    CHECK(contains(reg.render(), "lights3_test_cb_depth{backend=\"b1\"} 3\n"));
    depth = 9;  // instantaneous value pulled at render time
    CHECK(contains(reg.render(), "lights3_test_cb_depth{backend=\"b1\"} 9\n"));
    // With the same name and labels, the later registrant overrides
    reg.gauge_callback("lights3_test_cb_depth", "cb", [] { return 1.0; },
                       {{"backend", "b1"}});
    CHECK(contains(reg.render(), "lights3_test_cb_depth{backend=\"b1\"} 1\n"));
}

TEST(metrics_scope_base_labels) {
    auto reg = std::make_shared<MetricsRegistry>();
    MetricsScope scope(reg, {{"backend", "duo1"}});
    scope.counter("lights3_test_scoped_total", "", {{"op", "gc"}})->inc(5);
    // A derived child scope appends dimensions
    scope.with({{"engine", "redis"}}).counter("lights3_test_engine_total", "")->inc();
    auto out = reg->render();
    CHECK(contains(out, "lights3_test_scoped_total{backend=\"duo1\",op=\"gc\"} 5\n"));
    CHECK(contains(out, "lights3_test_engine_total{backend=\"duo1\",engine=\"redis\"} 1\n"));
    // Scope and direct registry lookup with the same name and labels -> same instance
    auto direct = reg->counter("lights3_test_scoped_total", "",
                               {{"backend", "duo1"}, {"op", "gc"}});
    CHECK_EQ(direct->value(), uint64_t(5));
}

TEST(metrics_empty_scope_harmless) {
    // Empty scope (default path for directly constructed backends in tests): instance usable but not registered, not rendered
    MetricsScope scope;
    CHECK(!scope);
    auto c = scope.counter("lights3_test_orphan_total", "");
    c->inc(3);
    CHECK_EQ(c->value(), uint64_t(3));
    scope.gauge("lights3_test_orphan", "")->set(1);
    scope.histogram("lights3_test_orphan_seconds", "", {1.0})->observe(0.5);
    scope.gauge_callback("lights3_test_orphan_cb", "", [] { return 0.0; });
    scope.with({{"k", "v"}}).counter("lights3_test_orphan2_total", "")->inc();
}

TEST(metrics_concurrent_smoke) {
    MetricsRegistry reg;
    auto c = reg.counter("lights3_test_conc_total", "");
    auto h = reg.histogram("lights3_test_conc_seconds", "", {0.5});
    std::vector<std::thread> ts;
    for (int t = 0; t < 4; ++t)
        ts.emplace_back([&, t] {
            for (int i = 0; i < 10000; ++i) {
                c->inc();
                h->observe(t % 2 ? 0.1 : 1.0);
                // Concurrent get-or-create and rendering must not tear either
                reg.counter("lights3_test_conc_total", "")->value();
                if (i % 2000 == 0) reg.render();
            }
        });
    for (auto& t : ts) t.join();
    CHECK_EQ(c->value(), uint64_t(40000));
    CHECK_EQ(h->snapshot().count, uint64_t(40000));
}

// ---------- §7 increment for L2 request metrics (docs/archive/gaps.md §7) ----------

TEST(s3_metrics_renders_pool_wait_histogram) {
    // The dedicated-pool criterion of concurrency.md §3.1 depends on this histogram; once collected it must be readable from /-/metrics
    s3::Metrics m;
    ThreadPool::Stats st;
    st.wait_hist = {5, 3, 1, 0, 1};
    st.wait_sum_us = 2'500'000;
    auto out = m.render([st] { return st; });
    CHECK(out.find("# TYPE lights3_pool_wait_seconds histogram") != std::string::npos);
    CHECK(out.find("lights3_pool_wait_seconds_bucket{le=\"0.001\"} 5") != std::string::npos);
    CHECK(out.find("lights3_pool_wait_seconds_bucket{le=\"+Inf\"} 10") != std::string::npos);
    CHECK(out.find("lights3_pool_wait_seconds_sum 2.5") != std::string::npos);
    CHECK(out.find("lights3_pool_wait_seconds_count 10") != std::string::npos);
}

TEST(s3_metrics_renders_admission_and_timer) {
    s3::Metrics m;
    auto out = m.render(
        {}, [] { return s3::AdmissionStats{1024, 1000, 3}; },
        [] {
            TimerQueue::Stats st;
            st.pending = 7;
            st.due = 2;
            st.fired = 100;
            st.slow = 1;
            st.exec_hist = {90, 8, 1, 1, 0};
            st.exec_sum_us = 1'000'000;
            st.lag_seconds = 3.5;
            return st;
        });
    CHECK(out.find("lights3_admission_capacity 1024") != std::string::npos);
    CHECK(out.find("lights3_admission_available 1000") != std::string::npos);
    CHECK(out.find("lights3_admission_waiting 3") != std::string::npos);
    CHECK(out.find("lights3_timer_pending 7") != std::string::npos);
    CHECK(out.find("lights3_timer_lag_seconds 3.5") != std::string::npos);
    CHECK(out.find("lights3_timer_slow_callbacks_total 1") != std::string::npos);
    CHECK(out.find("lights3_timer_callback_seconds_bucket{le=\"+Inf\"} 100") !=
          std::string::npos);
}

TEST(s3_metrics_bytes_and_per_bucket) {
    s3::Metrics m;
    m.record_bucket_request("photos");
    m.add_bytes_in("photos", 1000);
    m.add_bytes_out("photos", 2000);
    m.add_bytes_out("", 50);  // service-level request: counts only toward the global
    auto out = m.render({});
    CHECK(out.find("lights3_bytes_total{direction=\"in\"} 1000") != std::string::npos);
    CHECK(out.find("lights3_bytes_total{direction=\"out\"} 2050") != std::string::npos);
    CHECK(out.find("lights3_bucket_requests_total{bucket=\"photos\"} 1") != std::string::npos);
    CHECK(out.find("lights3_bucket_bytes_total{bucket=\"photos\",direction=\"in\"} 1000") !=
          std::string::npos);
    CHECK(out.find("lights3_bucket_bytes_total{bucket=\"photos\",direction=\"out\"} 2000") !=
          std::string::npos);
}

TEST(s3_metrics_split_totals_and_bucket_batches) {
    // The streaming decorator feeds the lock-free totals per chunk and the
    // per-bucket slot in batches (roadmap §4.3 ⑦); both halves must render
    s3::Metrics m;
    m.add_bytes_out_total(64 * 1024);
    m.add_bytes_out_total(64 * 1024);
    m.add_bytes_in_total(10);
    m.add_bucket_bytes("photos", 10, 128 * 1024);
    m.add_bucket_bytes("", 5, 5);  // no bucket: nothing to attribute
    auto out = m.render({});
    CHECK(out.find("lights3_bytes_total{direction=\"out\"} 131072") != std::string::npos);
    CHECK(out.find("lights3_bytes_total{direction=\"in\"} 10") != std::string::npos);
    CHECK(out.find("lights3_bucket_bytes_total{bucket=\"photos\",direction=\"out\"} 131072") !=
          std::string::npos);
    CHECK(out.find("lights3_bucket_bytes_total{bucket=\"photos\",direction=\"in\"} 10") !=
          std::string::npos);
}

TEST(s3_metrics_bucket_cardinality_capped) {
    // A malicious/runaway client scanning bucket names must not inflate /-/metrics into GiB-scale label cardinality
    s3::Metrics m;
    for (int i = 0; i < 600; ++i) m.record_bucket_request("bkt-" + std::to_string(i));
    auto out = m.render({});
    CHECK(out.find("lights3_bucket_requests_total{bucket=\"_other\"}") != std::string::npos);
}

// roadmap §5.3: exact status codes and website-plane events
TEST(s3_metrics_status_codes_and_website_events) {
    s3::Metrics m;
    for (int st : {200, 206, 304, 304, 404}) {
        m.request_start();
        m.request_end("GET", st, 0.001);
    }
    m.website(s3::WebsiteEvent::IndexRewrite);
    m.website(s3::WebsiteEvent::IndexRewrite);
    m.website(s3::WebsiteEvent::Redirect);
    auto out = m.render({});
    CHECK(out.find("lights3_responses_by_status_total{status=\"200\"} 1\n") != std::string::npos);
    CHECK(out.find("lights3_responses_by_status_total{status=\"206\"} 1\n") != std::string::npos);
    CHECK(out.find("lights3_responses_by_status_total{status=\"304\"} 2\n") != std::string::npos);
    CHECK(out.find("lights3_responses_by_status_total{status=\"404\"} 1\n") != std::string::npos);
    CHECK(out.find("status=\"500\"") == std::string::npos);  // sparse: never occurred
    CHECK(out.find("lights3_responses_total{class=\"3xx\"} 2") != std::string::npos);
    CHECK(out.find("lights3_website_events_total{event=\"index_rewrite\"} 2\n") != std::string::npos);
    CHECK(out.find("lights3_website_events_total{event=\"redirect\"} 1\n") != std::string::npos);
    CHECK(out.find("lights3_website_events_total{event=\"anon_read\"} 0\n") != std::string::npos);
    CHECK(out.find("lights3_website_events_total{event=\"error_document\"} 0\n") != std::string::npos);
    CHECK(out.find("lights3_website_events_total{event=\"throttled\"} 0\n") != std::string::npos);
}

// roadmap §5.3: admission wait histogram / queue / cancellation / stall cuts and the
// added L1 series (requests, TLS handshakes, parse errors)
TEST(s3_metrics_renders_admission_counters_and_l1_extras) {
    s3::Metrics m;
    auto out = m.render(
        {},
        [] {
            s3::AdmissionStats st{64, 60, 1};
            st.counters = true;
            st.wait_hist = {5, 1, 0, 0, 0, 1};
            st.wait_sum_us = 2'500'000;
            st.wait_count = 7;
            st.queued = 2;
            st.cancelled = 1;
            st.stalls_in = 3;
            st.stalls_out = 4;
            return st;
        },
        {},
        [] {
            http::ConnStats c;
            c.accepted = 3;
            c.requests = 12;
            c.tls_handshakes_ok = 2;
            c.tls_handshakes_failed = 1;
            c.parse_errors = 5;
            return c;
        });
    CHECK(out.find("lights3_admission_wait_seconds_bucket{le=\"0.001\"} 5\n") != std::string::npos);
    CHECK(out.find("lights3_admission_wait_seconds_bucket{le=\"0.01\"} 6\n") != std::string::npos);
    CHECK(out.find("lights3_admission_wait_seconds_bucket{le=\"+Inf\"} 7\n") != std::string::npos);
    CHECK(out.find("lights3_admission_wait_seconds_sum 2.5\n") != std::string::npos);
    CHECK(out.find("lights3_admission_wait_seconds_count 7\n") != std::string::npos);
    CHECK(out.find("lights3_admission_queued_total 2\n") != std::string::npos);
    CHECK(out.find("lights3_admission_cancelled_total 1\n") != std::string::npos);
    CHECK(out.find("lights3_transfer_stalls_total{direction=\"in\"} 3\n") != std::string::npos);
    CHECK(out.find("lights3_transfer_stalls_total{direction=\"out\"} 4\n") != std::string::npos);
    CHECK(out.find("lights3_http_requests_total 12\n") != std::string::npos);
    CHECK(out.find("lights3_http_tls_handshakes_total{result=\"ok\"} 2\n") != std::string::npos);
    CHECK(out.find("lights3_http_tls_handshakes_total{result=\"failed\"} 1\n") != std::string::npos);
    CHECK(out.find("lights3_http_parse_errors_total 5\n") != std::string::npos);
    // Without the cumulative counters wired (counters=false) only the three gauges appear
    auto bare = m.render({}, [] { return s3::AdmissionStats{1, 1, 0}; });
    CHECK(bare.find("lights3_admission_capacity 1") != std::string::npos);
    CHECK(bare.find("lights3_admission_wait_seconds") == std::string::npos);
}
