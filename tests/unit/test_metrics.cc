// 后端级 metrics 注册机制单测（docs/todo.md §3.1）：实例复用/类型冲突/标签转义/
// 直方图渲染/回调 gauge/空 scope 无害/并发递增冒烟
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "core/metrics.h"
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
    // 同名同标签 → 同实例（重复注册幂等）；同名异标签 → 家族内新子实例
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
    // # TYPE 每家族只出一次
    CHECK_EQ(out.find("# TYPE lights3_test_total"),
             out.rfind("# TYPE lights3_test_total"));

    // 同名不同类型 = 装配错误
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
    h->observe(30);  // 溢出桶
    auto out = reg.render();
    CHECK(contains(out, "# TYPE lights3_test_seconds histogram\n"));
    CHECK(contains(out, "lights3_test_seconds_bucket{op=\"get\",le=\"0.1\"} 2\n"));
    CHECK(contains(out, "lights3_test_seconds_bucket{op=\"get\",le=\"1\"} 3\n"));
    CHECK(contains(out, "lights3_test_seconds_bucket{op=\"get\",le=\"+Inf\"} 4\n"));
    CHECK(contains(out, "lights3_test_seconds_count{op=\"get\"} 4\n"));
    CHECK(contains(out, "lights3_test_seconds_sum{op=\"get\"} 30.6\n"));

    // 同名直方图桶界不一致 = 装配错误
    bool thrown = false;
    try {
        reg.histogram("lights3_test_seconds", "lat", {0.5, 5.0}, {{"op", "put"}});
    } catch (const std::runtime_error&) {
        thrown = true;
    }
    CHECK(thrown);
}

TEST(metrics_gauge_callback) {
    MetricsRegistry reg;
    int depth = 3;
    reg.gauge_callback("lights3_test_cb_depth", "cb", [&] { return double(depth); },
                       {{"backend", "b1"}});
    CHECK(contains(reg.render(), "lights3_test_cb_depth{backend=\"b1\"} 3\n"));
    depth = 9;  // 渲染时拉取瞬时值
    CHECK(contains(reg.render(), "lights3_test_cb_depth{backend=\"b1\"} 9\n"));
    // 同名同标签后注册者覆盖
    reg.gauge_callback("lights3_test_cb_depth", "cb", [] { return 1.0; },
                       {{"backend", "b1"}});
    CHECK(contains(reg.render(), "lights3_test_cb_depth{backend=\"b1\"} 1\n"));
}

TEST(metrics_scope_base_labels) {
    auto reg = std::make_shared<MetricsRegistry>();
    MetricsScope scope(reg, {{"backend", "duo1"}});
    scope.counter("lights3_test_scoped_total", "", {{"op", "gc"}})->inc(5);
    // 派生子 scope 追加维度
    scope.with({{"engine", "redis"}}).counter("lights3_test_engine_total", "")->inc();
    auto out = reg->render();
    CHECK(contains(out, "lights3_test_scoped_total{backend=\"duo1\",op=\"gc\"} 5\n"));
    CHECK(contains(out, "lights3_test_engine_total{backend=\"duo1\",engine=\"redis\"} 1\n"));
    // scope 与直连注册表取同名同标签 → 同实例
    auto direct = reg->counter("lights3_test_scoped_total", "",
                               {{"backend", "duo1"}, {"op", "gc"}});
    CHECK_EQ(direct->value(), uint64_t(5));
}

TEST(metrics_empty_scope_harmless) {
    // 空 scope（测试直构后端的默认径）：实例可用但不注册、不渲染
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
                // 并发 get-or-create 与渲染也不得撕裂
                reg.counter("lights3_test_conc_total", "")->value();
                if (i % 2000 == 0) reg.render();
            }
        });
    for (auto& t : ts) t.join();
    CHECK_EQ(c->value(), uint64_t(40000));
    CHECK_EQ(h->snapshot().count, uint64_t(40000));
}
