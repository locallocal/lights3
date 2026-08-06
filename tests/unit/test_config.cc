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

namespace {
template <class F>
bool throws(F&& fn) {
    try {
        fn();
    } catch (const std::exception&) {
        return true;
    }
    return false;
}
}  // namespace

TEST(parse_size_rejects_negative_and_overflow) {
    CHECK(throws([] { parse_size("-1"); }));            // stoull 回绕成 2^64-1
    CHECK(throws([] { parse_size("20000000000G"); }));  // << 30 回绕
    CHECK_EQ(parse_size("0"), size_t(0));
}

TEST(parse_duration_rejects_negative_and_overflow) {
    CHECK(throws([] { parse_duration_sec("30000000h"); }));  // int 乘法溢出
    CHECK(throws([] { parse_duration_sec("-5s"); }));
    CHECK_EQ(parse_duration_sec("24h"), 86400);
    CHECK_EQ(parse_duration_sec("2d"), 172800);
}

TEST(yaml_rejects_unexpected_indent) {
    // 列表项参数多缩进两格：此前被静默丢弃，现在报错
    CHECK(throws([] {
        yaml_parse("backends:\n  - name: a\n      type: memory\n");
    }));
    // 列表项参数少缩进：同样报错而非丢弃
    CHECK(throws([] {
        yaml_parse("backends:\n  - name: a\n type: memory\n");
    }));
    // 正常嵌套不受影响
    auto n = yaml_parse("backends:\n  - name: a\n    type: memory\n");
    CHECK_EQ(n.find("backends")->list.size(), size_t(1));
}

TEST(config_rejects_out_of_range_values) {
    const char* backends = "backends:\n  - name: m\n    type: memory\n";
    CHECK(throws([&] {
        Config::from_string(std::string("http:\n  port: 70000\n") + backends);
    }));
    CHECK(throws([&] {
        Config::from_string(std::string("http:\n  port: -1\n") + backends);
    }));
    // port 0 合法：内核分配空闲端口
    CHECK_EQ(static_cast<int>(
                 Config::from_string(std::string("http:\n  port: 0\n") + backends).http.port),
             0);
    CHECK(throws([&] {
        Config::from_string(std::string("runtime:\n  max_inflight_requests: 0\n") + backends);
    }));
    CHECK(throws([&] {
        Config::from_string(std::string("runtime:\n  io_threads: -2\n") + backends);
    }));
    CHECK(throws([&] {
        Config::from_string(std::string("http:\n  io_threads: 0\n") + backends);
    }));
}

// ---------- 校验缺口（gaps §3.9）----------

TEST(config_rejects_absurd_thread_counts) {
    const char* backends = "backends:\n  - name: m\n    type: memory\n";
    // 上界与 per-backend 的同名参数取齐 [1,1024]：手滑的 100000 会在启动时把进程拖垮
    CHECK(throws([&] {
        Config::from_string(std::string("runtime:\n  io_threads: 100000\n") + backends);
    }));
    CHECK(throws([&] {
        Config::from_string(std::string("http:\n  io_threads: 2048\n") + backends);
    }));
    CHECK_EQ(Config::from_string(std::string("runtime:\n  io_threads: 1024\n") + backends)
                 .runtime.io_threads,
             1024);
}

TEST(config_int_errors_name_the_key_and_value) {
    const char* backends = "backends:\n  - name: m\n    type: memory\n";
    std::string msg;
    try {
        Config::from_string(std::string("runtime:\n  io_threads: eight\n") + backends);
    } catch (const std::exception& e) {
        msg = e.what();
    }
    // 裸 stoi 只会报 "stoi"：既没有键名也没有原值
    CHECK(msg.find("runtime.io_threads") != std::string::npos);
    CHECK(msg.find("eight") != std::string::npos);
    // 尾随垃圾不再被当成合法前缀
    CHECK(throws([&] {
        Config::from_string(std::string("runtime:\n  io_threads: 8x\n") + backends);
    }));
}

TEST(config_rejects_duplicate_backend_name) {
    CHECK(throws([] {
        Config::from_string(
            "backends:\n  - name: dup\n    type: memory\n  - name: dup\n    type: memory\n");
    }));
}

TEST(config_keeps_hash_inside_quotes) {
    setenv("LIGHTS3_TEST_SECRET", "sekrit", 1);
    auto cfg = Config::from_string(
        "auth:\n  credentials:\n    - access_key: AK\n      secret_key: \"a #b c\"\n"
        "backends:\n  - name: m\n    type: memory\n");
    // 引号内的 " #" 不是注释：裸 find(" #") 会把密钥静默截短
    CHECK_EQ(cfg.auth.credentials[0].secret_key, "a #b c");
    // 引号外的行内注释仍照常剥离
    auto n = yaml_parse("a: b   # trailing\n");
    CHECK_EQ(n.get("a"), "b");
}

TEST(config_undefined_env_is_an_error_unless_defaulted) {
    unsetenv("LIGHTS3_DEFINITELY_UNSET");
    const char* backends = "backends:\n  - name: m\n    type: memory\n";
    // 静默展开成空串会把"变量名写错"变成"值悄悄变空"
    CHECK(throws([&] {
        Config::from_string(std::string("log:\n  level: ${LIGHTS3_DEFINITELY_UNSET}\n") + backends);
    }));
    // 确实可选的写 ${VAR:-默认值}
    auto cfg = Config::from_string(
        std::string("log:\n  level: ${LIGHTS3_DEFINITELY_UNSET:-debug}\n") + backends);
    CHECK_EQ(cfg.log_level, "debug");
}
