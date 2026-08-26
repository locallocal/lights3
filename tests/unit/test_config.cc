// YAML-subset parsing and typed configuration
#include <cstdlib>

#include "core/config.h"
#include "unit/mini_test.h"

using namespace lights3;

namespace {
const char* kSample = R"(
# comment line
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
    root: /tmp/l3data          # inline comment
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
    CHECK_EQ(cfg.auth.credentials[0].secret_key, "sekrit");  // ${ENV} expansion
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

TEST(config_website_buckets) {
    auto cfg = Config::from_string(
        "backends:\n  - name: m\n    type: memory\nwebsite:\n"
        "  - bucket: site-a\n"
        "  - bucket: site-b\n    index_suffix: home.htm\n    error_key: errors/404.html\n");
    CHECK_EQ(cfg.website.buckets.size(), size_t(2));
    CHECK_EQ(cfg.website.buckets[0].bucket, "site-a");
    CHECK_EQ(cfg.website.buckets[0].index_suffix, "index.html");  // default
    CHECK_EQ(cfg.website.buckets[0].error_key, "");
    CHECK_EQ(cfg.website.buckets[1].bucket, "site-b");
    CHECK_EQ(cfg.website.buckets[1].index_suffix, "home.htm");
    CHECK_EQ(cfg.website.buckets[1].error_key, "errors/404.html");

    // Startup errors: empty bucket, duplicates, index_suffix with '/', empty
    // index_suffix, error_key with a leading '/'
    for (const char* bad :
         {"backends:\n  - name: m\n    type: memory\nwebsite:\n  - bucket: \"\"\n",
          "backends:\n  - name: m\n    type: memory\nwebsite:\n"
          "  - bucket: dup\n  - bucket: dup\n",
          "backends:\n  - name: m\n    type: memory\nwebsite:\n"
          "  - bucket: b\n    index_suffix: sub/index.html\n",
          "backends:\n  - name: m\n    type: memory\nwebsite:\n"
          "  - bucket: b\n    index_suffix: \"\"\n",
          "backends:\n  - name: m\n    type: memory\nwebsite:\n"
          "  - bucket: b\n    error_key: /error.html\n"}) {
        bool thrown = false;
        try {
            Config::from_string(bad);
        } catch (const std::exception&) {
            thrown = true;
        }
        CHECK(thrown);
    }
}

TEST(config_defaults) {
    auto cfg = Config::from_string("backends:\n  - name: m\n    type: memory\n");
    CHECK_EQ(cfg.http.driver, "builtin");
    CHECK_EQ(static_cast<int>(cfg.http.port), 9000);
    CHECK_EQ(cfg.buckets.default_backend, "m");  // defaults to the first backend
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
    CHECK(throws([] { parse_size("-1"); }));            // stoull wraps around to 2^64-1
    CHECK(throws([] { parse_size("20000000000G"); }));  // << 30 wraps around
    CHECK_EQ(parse_size("0"), size_t(0));
}

TEST(parse_duration_rejects_negative_and_overflow) {
    CHECK(throws([] { parse_duration_sec("30000000h"); }));  // int multiplication overflow
    CHECK(throws([] { parse_duration_sec("-5s"); }));
    CHECK_EQ(parse_duration_sec("24h"), 86400);
    CHECK_EQ(parse_duration_sec("2d"), 172800);
}

TEST(yaml_rejects_unexpected_indent) {
    // A list-item parameter indented two spaces too far: previously silently dropped, now an error
    CHECK(throws([] {
        yaml_parse("backends:\n  - name: a\n      type: memory\n");
    }));
    // A list-item parameter under-indented: also an error rather than being dropped
    CHECK(throws([] {
        yaml_parse("backends:\n  - name: a\n type: memory\n");
    }));
    // Normal nesting is unaffected
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
    // port 0 is valid: the kernel assigns a free port
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

// ---------- Validation gaps (gaps §3.9) ----------

TEST(config_rejects_absurd_thread_counts) {
    const char* backends = "backends:\n  - name: m\n    type: memory\n";
    // Upper bound aligned with the per-backend parameter of the same name [1,1024]: a fat-fingered 100000 would drag the process down at startup
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
    // Bare stoi would only report "stoi": neither the key name nor the original value
    CHECK(msg.find("runtime.io_threads") != std::string::npos);
    CHECK(msg.find("eight") != std::string::npos);
    // Trailing garbage is no longer treated as a valid prefix
    CHECK(throws([&] {
        Config::from_string(std::string("runtime:\n  io_threads: 8x\n") + backends);
    }));
}

// ---------- Validation gaps, second round (docs/roadmap.md §1.2) ----------

TEST(config_port_rejects_trailing_garbage) {
    const char* backends = "backends:\n  - name: m\n    type: memory\n";
    // port was the last field going through bare stoi: "9000abc" silently became 9000
    CHECK(throws([&] {
        Config::from_string(std::string("http:\n  port: 9000abc\n") + backends);
    }));
    std::string msg;
    try {
        Config::from_string(std::string("http:\n  port: nine\n") + backends);
    } catch (const std::exception& e) {
        msg = e.what();
    }
    CHECK(msg.find("http.port") != std::string::npos);
    CHECK(msg.find("nine") != std::string::npos);
}

TEST(config_max_header_size_bounded) {
    const char* backends = "backends:\n  - name: m\n    type: memory\n";
    // 4GiB truncates to 0 in beast's parser.header_limit(uint32_t) and rejects everything
    CHECK(throws([&] {
        Config::from_string(std::string("http:\n  max_header_size: 4GiB\n") + backends);
    }));
    CHECK(throws([&] {
        Config::from_string(std::string("http:\n  max_header_size: 512\n") + backends);
    }));
    CHECK_EQ(Config::from_string(std::string("http:\n  max_header_size: 1MiB\n") + backends)
                 .http.max_header_size,
             size_t(1024 * 1024));
}

TEST(config_idle_timeout_rejects_zero) {
    const char* backends = "backends:\n  - name: m\n    type: memory\n";
    // 0 means "never" to builtin but "expire immediately" to beast — rejected rather
    // than silently meaning opposite things per driver
    CHECK(throws([&] {
        Config::from_string(std::string("http:\n  idle_timeout: 0s\n") + backends);
    }));
    CHECK(throws([&] {
        Config::from_string(std::string("http:\n  idle_timeout: 2d\n") + backends);
    }));
    CHECK_EQ(Config::from_string(std::string("http:\n  idle_timeout: 24h\n") + backends)
                 .http.idle_timeout_sec,
             86400);
}

TEST(config_log_level_rejects_typos) {
    const char* backends = "backends:\n  - name: m\n    type: memory\n";
    // "warning" used to silently downgrade to info — the operator thinks the level took effect
    CHECK(throws([&] {
        Config::from_string(std::string("log:\n  level: warning\n") + backends);
    }));
    for (const char* ok : {"debug", "info", "warn", "error"})
        CHECK_EQ(Config::from_string(std::string("log:\n  level: ") + ok + "\n" + backends)
                     .log_level,
                 ok);
}

TEST(config_bucket_rule_rejects_empty_fields) {
    const char* backends = "backends:\n  - name: m\n    type: memory\n";
    // An empty glob can never match a bucket name; an empty backend surfaced as unknown backend ""
    CHECK(throws([&] {
        Config::from_string(std::string(backends) +
                            "buckets:\n  rules:\n    - match: \"\"\n      backend: m\n");
    }));
    CHECK(throws([&] {
        Config::from_string(std::string(backends) +
                            "buckets:\n  rules:\n    - match: \"a-*\"\n      backend: \"\"\n");
    }));
    CHECK(throws([&] {
        Config::from_string(std::string(backends) + "buckets:\n  rules:\n    - match: \"a-*\"\n");
    }));
}

TEST(config_stall_timeout_must_not_exceed_request_timeout) {
    const char* backends = "backends:\n  - name: m\n    type: memory\n";
    // request_timeout always fires first, so a longer stall window can never take effect
    CHECK(throws([&] {
        Config::from_string(std::string("http:\n  request_timeout: 60s\n"
                                        "  transfer_stall_timeout: 120s\n") +
                            backends);
    }));
    // Equal is fine, and a disabled request_timeout (0) lifts the constraint
    CHECK_EQ(Config::from_string(std::string("http:\n  request_timeout: 60s\n"
                                             "  transfer_stall_timeout: 60s\n") +
                                 backends)
                 .http.transfer_stall_timeout_sec,
             60);
    CHECK_EQ(Config::from_string(std::string("http:\n  request_timeout: 0s\n"
                                             "  transfer_stall_timeout: 3600s\n") +
                                 backends)
                 .http.transfer_stall_timeout_sec,
             3600);
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
    // " #" inside quotes is not a comment: a bare find(" #") would silently truncate the secret
    CHECK_EQ(cfg.auth.credentials[0].secret_key, "a #b c");
    // Inline comments outside quotes are still stripped as usual
    auto n = yaml_parse("a: b   # trailing\n");
    CHECK_EQ(n.get("a"), "b");
}

TEST(config_undefined_env_is_an_error_unless_defaulted) {
    unsetenv("LIGHTS3_DEFINITELY_UNSET");
    const char* backends = "backends:\n  - name: m\n    type: memory\n";
    // Silently expanding to an empty string would turn "misspelled variable name" into "value quietly becomes empty"
    CHECK(throws([&] {
        Config::from_string(std::string("log:\n  level: ${LIGHTS3_DEFINITELY_UNSET}\n") + backends);
    }));
    // Genuinely optional values are written ${VAR:-default}
    auto cfg = Config::from_string(
        std::string("log:\n  level: ${LIGHTS3_DEFINITELY_UNSET:-debug}\n") + backends);
    CHECK_EQ(cfg.log_level, "debug");
}

TEST(config_tls_requires_both_cert_and_key) {
    // Providing only one half is necessarily a misconfiguration (docs/archive/gaps.md §7): silently ignoring it would let
    // an instance that "thinks TLS is on" run in plaintext
    auto one_sided = R"(
http:
  tls_cert: /etc/lights3/server.crt
backends:
  - name: d
    type: memory
)";
    bool threw = false;
    try {
        Config::from_string(one_sided);
    } catch (const std::exception& e) {
        threw = std::string(e.what()).find("tls") != std::string::npos;
    }
    CHECK(threw);

    auto both = R"(
http:
  tls_cert: /etc/lights3/server.crt
  tls_key: /etc/lights3/server.key
backends:
  - name: d
    type: memory
)";
    auto cfg = Config::from_string(both);
    CHECK_EQ(cfg.http.tls_cert, "/etc/lights3/server.crt");
    CHECK_EQ(cfg.http.tls_key, "/etc/lights3/server.key");
}

TEST(config_shutdown_backpressure_knobs) {
    // Shutdown/backpressure boundaries (docs/archive/gaps.md §7): defaults + explicit overrides + range validation
    auto cfg = Config::from_string(R"(
backends:
  - name: d
    type: memory
)");
    CHECK_EQ(cfg.http.drain_limit, 4u * 1024 * 1024);
    CHECK_EQ(cfg.http.trailer_max_size, size_t(16 * 1024));
    CHECK_EQ(cfg.http.io_chunk_size, size_t(64 * 1024));
    CHECK_EQ(cfg.http.body_queue_cap, size_t(256 * 1024));
    CHECK_EQ(cfg.http.shutdown_grace_sec, 10);
    CHECK_EQ(cfg.http.shutdown_force_wait_sec, 5);
    CHECK(!cfg.http.io_threads_set);

    auto tuned = Config::from_string(R"(
http:
  io_threads: 4
  drain_limit: 1MiB
  trailer_max_size: 8KiB
  io_chunk_size: 128KiB
  body_queue_cap: 512KiB
  shutdown_grace: 3s
  shutdown_force_wait: 1s
backends:
  - name: d
    type: memory
)");
    CHECK_EQ(tuned.http.drain_limit, 1u * 1024 * 1024);
    CHECK_EQ(tuned.http.trailer_max_size, size_t(8 * 1024));
    CHECK_EQ(tuned.http.io_chunk_size, size_t(128 * 1024));
    CHECK_EQ(tuned.http.body_queue_cap, size_t(512 * 1024));
    CHECK_EQ(tuned.http.shutdown_grace_sec, 3);
    CHECK_EQ(tuned.http.shutdown_force_wait_sec, 1);
    // Set when io_threads is configured explicitly (the builtin driver uses this to WARN instead of silently ignoring it)
    CHECK(tuned.http.io_threads_set);

    bool threw = false;
    try {
        Config::from_string(R"(
http:
  io_chunk_size: 1KiB
backends:
  - name: d
    type: memory
)");
    } catch (const std::exception& e) {
        threw = std::string(e.what()).find("io_chunk_size") != std::string::npos;
    }
    CHECK(threw);
}
