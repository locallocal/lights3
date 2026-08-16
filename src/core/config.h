// L4: config loading. Includes a built-in YAML-subset parser covering the shapes this
// project's config uses: nested maps, "- " lists, scalars, comments, ${ENV} expansion.
// No flow style / anchors / multi-line scalars.
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "core/util/crypto.h"

namespace lights3 {

// ---------- YAML subset node ----------
struct YamlNode {
    enum class Type { Scalar, Map, List };
    Type type = Type::Scalar;
    std::string scalar;
    std::vector<std::pair<std::string, YamlNode>> map;  // order-preserving
    std::vector<YamlNode> list;

    const YamlNode* find(const std::string& key) const;
    // Get a child scalar; returns def if absent
    std::string get(const std::string& key, const std::string& def = "") const;
};

YamlNode yaml_parse(const std::string& text);  // throws std::runtime_error on syntax errors

// ---------- Typed configuration ----------
struct HttpConfig {
    std::string driver = "builtin";
    std::string bind = "0.0.0.0";
    uint16_t port = 9000;
    int io_threads = 4;
    size_t max_header_size = 16 * 1024;
    int idle_timeout_sec = 60;
    // Per-request timeout (docs/gaps.md §3.3): the clock starts when the handler
    // begins executing; on expiry the request is interrupted via cooperative
    // cancellation (suspension points throw OperationCancelled -> 503 SlowDown,
    // retryable by SDKs). idle_timeout only covers socket syscalls, not the
    // handler execution window. 0 = disabled
    int request_timeout_sec = 300;
    // Minimum multipart part size in bytes (docs/gaps.md §5.7): AWS fixes it at
    // 5MiB, 0 = no limit. Relax it when a toolchain that ignores this rule sits in
    // front, or when this instance is merely a proxy for another lights3
    uint64_t min_part_size = 5ull * 1024 * 1024;
    // Transfer stall limit: the total no-progress duration allowed for streaming
    // send/receive **as a whole**. All four drivers reset their per-chunk timeouts
    // chunk by chunk, so a client sending 1 byte every 59 seconds could hold a
    // connection indefinitely. 0 = disabled
    int transfer_stall_timeout_sec = 300;
    // Hard cap on concurrent connections (uniform across the four drivers; httplib
    // is implicitly constrained by its thread pool): new connections are rejected
    // above the limit; without one, per-connection threads/coroutine frames/buffers
    // can exhaust memory
    int max_connections = 4096;
    std::string base_domain;  // non-empty enables virtual-host style (docs/s3-protocol.md §2)
    // TLS (docs/gaps.md §7): HTTPS is enabled when both cert and key are given.
    // SigV4's UNSIGNED-PAYLOAD integrity relies on transport-layer encryption, and
    // this covers the inbound direction. Only the httplib/beast drivers support it;
    // builtin/seastar error out at startup if TLS is configured — never
    // "configured but silently running plaintext"
    std::string tls_cert;  // path to PEM certificate chain
    std::string tls_key;   // path to PEM private key
    // The builtin driver is thread-per-connection, so io_threads is meaningless for
    // it; when explicitly configured, WARN at startup instead of silently ignoring
    // (docs/gaps.md §7). Set by the parser
    bool io_threads_set = false;
    // ---- Shutdown/backpressure knobs (docs/gaps.md §7): formerly hard-coded once per driver ----
    uint64_t drain_limit = 4 * 1024 * 1024;   // max request body drained before returning an error
    size_t trailer_max_size = 16 * 1024;      // chunked trailer section limit (builtin/seastar)
    size_t io_chunk_size = 64 * 1024;         // streaming read/write chunk size
    size_t body_queue_cap = 256 * 1024;       // push-to-pull body queue capacity (httplib only, i.e. the backpressure watermark)
    int shutdown_grace_sec = 10;              // grace period waiting for in-flight requests on shutdown
    int shutdown_force_wait_sec = 5;          // second wait after forced disconnect
};

struct RuntimeConfig {
    int io_threads = 16;
    int max_inflight_requests = 1024;
};

struct Credential {
    std::string access_key;
    util::SecretString secret_key;  // wiped on destruction (docs/gaps.md §4)
};

struct AuthConfig {
    std::vector<Credential> credentials;  // empty disables auth (for demos/tests)
    std::string region = "us-east-1";
    std::string service = "s3";
    // Credential management phase 2 (docs/credential-management.md §10)
    std::string credentials_file;          // external credentials file (JSON, hot-reloaded); empty = disabled
    int credentials_file_reload_sec = 30;  // file mtime polling period; 0 = load at startup only
    int sync_interval_sec = 0;             // multi-instance: periodic incremental reload of .sys; 0 = disabled
};

struct BackendConfig {
    std::string name;
    std::string type;                            // localfs | memory | ...
    std::map<std::string, std::string> params;   // root/staging/endpoint/... interpreted by each backend
};

struct BucketRule {
    std::string match;  // glob
    std::string backend;
};

struct BucketsConfig {
    std::string default_backend;
    std::vector<BucketRule> rules;
};

struct Config {
    HttpConfig http;
    RuntimeConfig runtime;
    AuthConfig auth;
    std::vector<BackendConfig> backends;
    BucketsConfig buckets;
    std::string log_level = "info";

    static Config load(const std::string& path);
    static Config from_string(const std::string& yaml_text);
};

// Parsing helpers for values like "16KiB" / "1MB" / "60s" / "true"
size_t parse_size(const std::string& s);
int parse_duration_sec(const std::string& s);
bool parse_bool(const std::string& s);  // true/1/yes/on | false/0/no/off; anything else throws runtime_error

}  // namespace lights3
