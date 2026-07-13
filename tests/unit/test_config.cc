// YAML 子集解析与类型化配置
#include <cstdlib>

#include "core/config.h"
#include "unit/mini_test.h"

using namespace lights3;

namespace {
const char* kSample = R"(
# 注释行
http:
  driver: builtin
  port: 9100
  max_header_size: 16KiB
  idle_timeout: 60s

runtime:
  io_threads: 8

auth:
  credentials:
    - access_key: AK1
      secret_key: ${LIGHTS3_TEST_SECRET}
  region: us-west-2

backends:
  - name: localdata
    type: localfs
    root: /tmp/l3data          # 行内注释
  - name: mem
    type: memory

buckets:
  default_backend: localdata
  rules:
    - match: "cache-*"
      backend: mem
)";
}  // namespace

TEST(config_parses_sample) {
    setenv("LIGHTS3_TEST_SECRET", "sekrit", 1);
    auto cfg = Config::from_string(kSample);

    CHECK_EQ(cfg.http.driver, "builtin");
    CHECK_EQ(static_cast<int>(cfg.http.port), 9100);
    CHECK_EQ(cfg.http.max_header_size, size_t(16 * 1024));
    CHECK_EQ(cfg.http.idle_timeout_sec, 60);
    CHECK_EQ(cfg.runtime.io_threads, 8);

    CHECK_EQ(cfg.auth.credentials.size(), size_t(1));
    CHECK_EQ(cfg.auth.credentials[0].access_key, "AK1");
    CHECK_EQ(cfg.auth.credentials[0].secret_key, "sekrit");  // ${ENV} 展开
    CHECK_EQ(cfg.auth.region, "us-west-2");

    CHECK_EQ(cfg.backends.size(), size_t(2));
    CHECK_EQ(cfg.backends[0].name, "localdata");
    CHECK_EQ(cfg.backends[0].type, "localfs");
    CHECK_EQ(cfg.backends[0].params.at("root"), "/tmp/l3data");
    CHECK_EQ(cfg.backends[1].type, "memory");

    CHECK_EQ(cfg.buckets.default_backend, "localdata");
    CHECK_EQ(cfg.buckets.rules.size(), size_t(1));
    CHECK_EQ(cfg.buckets.rules[0].match, "cache-*");
    CHECK_EQ(cfg.buckets.rules[0].backend, "mem");
}

TEST(config_rejects_unknown_backend_ref) {
    const char* bad = R"(
backends:
  - name: a
    type: memory
buckets:
  default_backend: nonexistent
)";
    bool thrown = false;
    try {
        Config::from_string(bad);
    } catch (const std::exception&) {
        thrown = true;
    }
    CHECK(thrown);
}

TEST(config_defaults) {
    auto cfg = Config::from_string("backends:\n  - name: m\n    type: memory\n");
    CHECK_EQ(cfg.http.driver, "builtin");
    CHECK_EQ(static_cast<int>(cfg.http.port), 9000);
    CHECK_EQ(cfg.buckets.default_backend, "m");  // 缺省取第一个后端
    CHECK(cfg.auth.credentials.empty());
}

TEST(parse_size_and_duration) {
    CHECK_EQ(parse_size("16KiB"), size_t(16384));
    CHECK_EQ(parse_size("2MiB"), size_t(2 * 1024 * 1024));
    CHECK_EQ(parse_size("123"), size_t(123));
    CHECK_EQ(parse_duration_sec("60s"), 60);
    CHECK_EQ(parse_duration_sec("5m"), 300);
}
