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
    // order-preserving
    std::vector<std::pair<std::string, YamlNode>> map;
    std::vector<YamlNode> list;

    // Special members out of line: the recursive pair<string, YamlNode> member
    // makes clang instantiate them while the type is still incomplete when they
    // are defaulted in-class (GCC accepts it); defined in config.cc
    YamlNode();
    explicit YamlNode(Type t) : type(t) {}
    ~YamlNode();
    YamlNode(const YamlNode&);
    YamlNode(YamlNode&&) noexcept;
    YamlNode& operator=(const YamlNode&);
    YamlNode& operator=(YamlNode&&) noexcept;

    const YamlNode* find(const std::string& key) const;
    // Get a child scalar; returns def if absent
    std::string get(const std::string& key, const std::string& def = "") const;
};

// throws std::runtime_error on syntax errors
YamlNode yaml_parse(const std::string& text);

// ---------- Typed configuration ----------
// Extra certificate served by SNI (roadmap §4.1, docs/tls.md §2.3): hosts is a
// comma-separated list of exact names or "*.example.com" wildcards
struct TlsSniEntry {
    std::string hosts;
    std::string cert;
    std::string key;
};

struct HttpConfig {
    std::string driver = "builtin";
    std::string bind = "0.0.0.0";
    uint16_t port = 9000;
    // Separate admin listener (backlog-sequence ②): when admin_port is set (>= 0;
    // 0 = kernel-picked, like port) a second server of the same driver serves the
    // /-/ face (metrics, admin API) and the data-plane port answers 404 for it;
    // probes (/-/healthz, /-/readyz) stay on both. -1 = no admin listener (every
    // face on the data-plane port). admin_bind empty = same address as bind
    std::string admin_bind;
    int admin_port = -1;
    int io_threads = 4;
    // Validated to [1KiB, 1MiB]: beast passes it into parser.header_limit(uint32_t),
    // where an unbounded value like 4GiB would truncate to 0 and reject every request
    size_t max_header_size = 16 * 1024;
    // Timeout family (roadmap §4.2, docs/http-adapter.md §2.1). All validated to
    // [1s, 86400s]; 0 is rejected: drivers disagree on its meaning (builtin's
    // SO_RCVTIMEO 0 = never time out, beast's expires_after(0s) = expire
    // immediately), so "no timeout" is not a supported configuration
    // keep-alive: waiting for the next request on an idle connection
    int idle_timeout_sec = 60;
    // a new connection's request line + header block (slowloris bound)
    int header_timeout_sec = 30;
    // inactivity bound on one request-body read (slow uploader)
    int body_timeout_sec = 60;
    // inactivity bound on one response write (slow downloader)
    int write_timeout_sec = 60;
    // Requests served over one keep-alive connection before the server answers
    // Connection: close (lets load balancers re-balance long-lived connections);
    // 0 = unlimited. httplib had a hard-coded 1024 before, the other three had none
    int max_requests_per_connection = 1024;
    // Per-request timeout (docs/archive/gaps.md §3.3): the clock starts when the handler
    // begins executing; on expiry the request is interrupted via cooperative
    // cancellation (suspension points throw OperationCancelled -> 503 SlowDown,
    // retryable by SDKs). idle_timeout only covers socket syscalls, not the
    // handler execution window. 0 = disabled
    int request_timeout_sec = 300;
    // Minimum multipart part size in bytes (docs/archive/gaps.md §5.7): AWS fixes it at
    // 5MiB, 0 = no limit. Relax it when a toolchain that ignores this rule sits in
    // front, or when this instance is merely a proxy for another lights3
    uint64_t min_part_size = 5ull * 1024 * 1024;
    // Transfer stall limit: the total no-progress duration allowed for streaming
    // send/receive **as a whole**. All four drivers reset their per-chunk timeouts
    // chunk by chunk, so a client sending 1 byte every 59 seconds could hold a
    // connection indefinitely. 0 = disabled. Must not exceed request_timeout when
    // both are enabled — the request timeout would always fire first and the stall
    // guard could never take effect (rejected at load)
    int transfer_stall_timeout_sec = 300;
    // Hard cap on concurrent connections (uniform across the four drivers; httplib
    // is implicitly constrained by its thread pool): new connections are rejected
    // above the limit; without one, per-connection threads/coroutine frames/buffers
    // can exhaust memory
    int max_connections = 4096;
    // /-/metrics exposure (roadmap §5.3): anonymous (default, classic scrape) or
    // root (a statically configured credential must sign the GET). Hot-reloadable
    std::string metrics_access = "anonymous";
    // non-empty enables virtual-host style (docs/s3-protocol.md §2)
    std::string base_domain;
    // TLS (docs/archive/gaps.md §7): HTTPS is enabled when both cert and key are given.
    // SigV4's UNSIGNED-PAYLOAD integrity relies on transport-layer encryption, and
    // this covers the inbound direction. Only the httplib/beast drivers support it;
    // builtin/seastar error out at startup if TLS is configured — never
    // "configured but silently running plaintext"
    // path to PEM certificate chain
    std::string tls_cert;
    // path to PEM private key
    std::string tls_key;
    // TLS knobs (roadmap §4.1, docs/tls.md): all four drivers honor them (seastar
    // maps versions/client auth onto GnuTLS, see docs/tls.md §4)
    // PEM CA bundle for client certificates (mTLS); empty = none
    std::string tls_client_ca;
    // off | optional | require (the latter two need tls_client_ca)
    std::string tls_client_auth = "off";
    // 1.2 | 1.3
    std::string tls_min_version = "1.2";
    // OpenSSL cipher list for TLS <= 1.2; empty = library default
    std::string tls_ciphers;
    // OpenSSL TLS 1.3 suites; empty = library default
    std::string tls_ciphersuites;
    // certificate file polling period (hot reload); 0 = off
    int tls_reload_interval_sec = 60;
    // extra certificates selected by SNI
    std::vector<TlsSniEntry> tls_sni;
    // The builtin driver is thread-per-connection, so io_threads is meaningless for
    // it; when explicitly configured, WARN at startup instead of silently ignoring
    // (docs/archive/gaps.md §7). Set by the parser
    bool io_threads_set = false;
    // ---- Shutdown/backpressure knobs (docs/archive/gaps.md §7): formerly hard-coded once per driver ----
    // max request body drained before returning an error
    uint64_t drain_limit = 4 * 1024 * 1024;
    // chunked trailer section limit (builtin/seastar)
    size_t trailer_max_size = 16 * 1024;
    // streaming read/write chunk size
    size_t io_chunk_size = 64 * 1024;
    // sendfile(2) for file-backed fixed-length plaintext responses (roadmap §4.3 ④;
    // builtin driver; TLS / chunked / non-file bodies always take the read() path)
    bool sendfile = true;
    // push-to-pull body queue capacity (httplib only, i.e. the backpressure watermark)
    size_t body_queue_cap = 256 * 1024;
    // grace period waiting for in-flight requests on shutdown
    int shutdown_grace_sec = 10;
    // second wait after forced disconnect
    int shutdown_force_wait_sec = 5;
};

struct RuntimeConfig {
    int io_threads = 16;
    int max_inflight_requests = 1024;
};

// Per-client rate limits (roadmap §4.2, docs/http-adapter.md §2.3): token bucket +
// concurrency cap per source IP (decided before signature verification) and per
// access key (after it). 0 = that limit off. Over the limit answers 503 SlowDown
struct RateLimitConfig {
    // sustained requests/second per client IP
    int per_ip_rps = 0;
    // bucket size; 0 = same as rps
    int per_ip_burst = 0;
    // concurrent requests per client IP
    int per_ip_max_inflight = 0;
    // sustained requests/second per access key
    int per_ak_rps = 0;
    int per_ak_burst = 0;
    int per_ak_max_inflight = 0;
    // distinct keys kept per table (LRU beyond that)
    int max_tracked = 10000;
};

struct Credential {
    std::string access_key;
    // wiped on destruction (docs/archive/gaps.md §4)
    util::SecretString secret_key;
};

struct AuthConfig {
    // empty disables auth (for demos/tests)
    std::vector<Credential> credentials;
    std::string region = "us-east-1";
    std::string service = "s3";
    // Credential management phase 2 (docs/credential-management.md §10)
    // external credentials file (JSON, hot-reloaded); empty = disabled
    std::string credentials_file;
    // file mtime polling period; 0 = load at startup only
    int credentials_file_reload_sec = 30;
    // multi-instance: periodic incremental reload of .sys; 0 = disabled
    int sync_interval_sec = 0;
    // mTLS identity mapping (backlog-sequence ⑥, docs/tls.md §2.1): which field of a
    // verified client certificate names the identity looked up in
    // .sys/tls-identities/. off = certificates stay transport admission only.
    // Needs http.tls_client_auth optional|require
    // off | subject-cn | san-uri
    std::string tls_identity = "off";
};

struct BackendConfig {
    std::string name;
    // localfs | memory | ...
    std::string type;
    // root/staging/endpoint/... interpreted by each backend
    std::map<std::string, std::string> params;
};

struct BucketRule {
    // glob
    std::string match;
    std::string backend;
};

// Static website hosting (docs/static-website.md): buckets listed here accept
// anonymous GET/HEAD object reads. Exact names only, no globs — a pattern typo
// must not silently make extra buckets public.
// RoutingRules entry (roadmap §2.3, AWS WebsiteConfiguration shape). Both condition
// fields optional (both empty = matches everything); redirect fields empty = keep the
// request's value. Managed via the ?website XML API; not exposed in YAML (static
// sites needing rules can be created dynamically)
struct WebsiteRoutingRule {
    // Condition
    std::string key_prefix_equals;
    // 0 = unset; else matched against the error status
    int http_error_code_equals = 0;
    // Redirect
    // "http"/"https"; "" = request scheme
    std::string protocol;
    // "" = this gateway
    std::string host_name;
    // exclusive with replace_key_with
    std::optional<std::string> replace_key_prefix_with;
    std::optional<std::string> replace_key_with;
    int http_redirect_code = 301;

    bool operator==(const WebsiteRoutingRule&) const = default;
};

struct WebsiteBucket {
    std::string bucket;
    // Appended when the anonymous key is empty or ends with '/' (phase ②). Must not
    // contain '/' (AWS rule: a slash would map "docs/" outside that directory)
    std::string index_suffix = "index.html";
    // Object served as the body of anonymous 4xx/5xx responses, keeping the original
    // status code; empty = built-in minimal HTML page
    std::string error_key;
    // RedirectAllRequestsTo (roadmap §2.3): every anonymous request answers 301 to
    // <protocol>://<host><path>; exclusive with index/error/rules (AWS shape).
    // Non-empty host enables it; empty protocol follows the request scheme
    std::string redirect_all_host;
    std::string redirect_all_protocol;
    std::vector<WebsiteRoutingRule> routing_rules;
    // Anonymous request rate limit, requests/second (roadmap §2.3: anonymous GET has
    // no signature cost — a public bucket is otherwise a free bandwidth amplifier).
    // 0 = unlimited. YAML/JSON only; the AWS XML shape has no such field
    uint32_t max_rps = 0;

    bool operator==(const WebsiteBucket&) const = default;
};

struct WebsiteConfig {
    std::vector<WebsiteBucket> buckets;
};

// Lifecycle enforcement (roadmap §2.4): how often the runner walks the rule table.
// 0 = enforcement disabled (the ?lifecycle API still works; rules just never fire)
struct LifecycleConfig {
    int scan_interval_sec = 3600;
};

// S3 Tables / Iceberg REST catalog (docs/s3-tables-design.md §10): off by default,
// every key is restart-only. path_prefix carries no "/v1" (appended by the router)
struct TablesConfig {
    bool enabled = false;
    std::string path_prefix = "/iceberg";
    // optional MinIO-style alias such as "/_iceberg" (design §14 ⑥); empty = off
    std::string compat_prefix;
    bool accept_s3tables_signing = true;
    // reserved prefix inside table buckets; always ends with '/'
    std::string reserved_prefix = ".lights3-table/";
    size_t metadata_max_size = 50 * 1024 * 1024;
    size_t request_max_size = 1024 * 1024;
    int metadata_log_keep = 100;
    int max_page_size = 1000;
    int validate_concurrency = 16;
    bool credential_vending = false;
    int credential_ttl_sec = 900;
    // where the catalog state lives (docs/s3-tables-design.md §12): "object" = .sys objects
    // of the default backend; "duostore" = the default backend's meta engine KV facade
    // (default backend must be duostore; one transaction per commit)
    std::string catalog_backing = "object";
    struct Maintenance {
        int scan_interval_sec = 0;
        int safety_window_sec = 900;
        int retain_recent_metadata_files = 10;
        bool delete_enabled = false;
        int tombstone_ttl_sec = 86400;
        bool operator==(const Maintenance&) const = default;
    } maintenance;
    bool operator==(const TablesConfig&) const = default;
};

struct BucketsConfig {
    std::string default_backend;
    std::vector<BucketRule> rules;
};

// Bucket usage accounting (roadmap §3.9 ①): per-bucket object/byte counters kept at
// L2, updated at every write commit, persisted to .sys/usage/<bucket> and reconciled
// by a periodic full listing (docs/multi-tenancy.md §2)
struct UsageConfig {
    // false = no counters, no quota enforcement (skips the pre-write HEAD)
    bool enabled = true;
    // persist dirty counters; 0 = only at shutdown / after a scan
    int flush_interval_sec = 60;
    // full re-count of every bucket; 0 = never (manual rescan only)
    int reconcile_interval_sec = 86400;
    // false = non-designated instance in a multi-gateway setup
    bool reconcile = true;
};

// Audit log (roadmap §3.9 ④): JSON lines to a rotating file; empty path = off
// (control-plane events still go to the regular log at INFO)
struct AuditConfig {
    std::string path;
    // also record one line per data-plane request
    bool data_plane = false;
    // rotate above this many bytes
    uint64_t max_size = 64 * 1024 * 1024;
    // rotated files kept
    int max_files = 10;
};

// Operational log (roadmap §5.2). Every knob but level / slow_request_threshold is
// fixed at startup (the sink and the formatter are built once; docs/config-reload.md §4)
struct LogConfig {
    // debug | info | warn | error (hot-reloadable)
    std::string level = "info";
    // text = one human-readable line; json = one JSON object per line
    std::string format = "text";
    // empty = stderr; otherwise a size-rotated file
    std::string file;
    // rotate the file above this many bytes
    uint64_t max_size = 64 * 1024 * 1024;
    // rotated files kept
    int max_files = 10;
    // true = a dedicated writer thread; request threads only enqueue
    bool async = false;
    // queue capacity (records)
    int async_queue = 8192;
    // block = the caller waits on a full queue; drop = overwrite the oldest
    std::string async_overflow = "block";
    // Requests taking at least this long are logged at WARN with per-stage timings
    // (auth / handler / backend / TTFB / total); 0 = off (hot-reloadable)
    int slow_request_threshold_ms = 0;
};

struct Config {
    HttpConfig http;
    RuntimeConfig runtime;
    AuthConfig auth;
    std::vector<BackendConfig> backends;
    BucketsConfig buckets;
    WebsiteConfig website;
    LifecycleConfig lifecycle;
    TablesConfig tables;
    UsageConfig usage;
    AuditConfig audit;
    RateLimitConfig ratelimit;
    LogConfig log;

    static Config load(const std::string& path);
    static Config from_string(const std::string& yaml_text);
};

// Outcome of a configuration hot reload (roadmap §4.4, docs/config-reload.md):
// what was applied at runtime, what changed but needs a restart, or why the new
// file was refused (the old configuration then stays in force, untouched)
struct ConfigReloadReport {
    bool ok = false;
    // parse/validation failure (nothing applied)
    std::string error;
    // "log.level: info -> debug"
    std::vector<std::string> applied;
    // "http.port" ...
    std::vector<std::string> requires_restart;
};

// S3 Tables deployment sanity (docs/s3-tables-design.md §5.5, docs/s3-tables/step-5-multi-gateway-docs.md
// §3): the catalog state lives in the default backend's .sys, so tables only converge
// across gateways when that backend is shared (cloudproxy, duostore with redis / tikv
// meta). tables.enabled on a single-gateway default backend together with any
// multi-gateway signal (an explicit read_lease > 0, gc_enabled: false, usage.reconcile:
// false) yields the warning text; nullopt = nothing to say. Startup logs it, --check-config
// prints it
std::optional<std::string> tables_deployment_warning(const Config& cfg);

// Parsing helpers for values like "16KiB" / "1MB" / "60s" / "true"
size_t parse_size(const std::string& s);
int parse_duration_sec(const std::string& s);
// same units plus "ms"; a bare number is seconds
int parse_duration_ms(const std::string& s);
// true/1/yes/on | false/0/no/off; anything else throws runtime_error
bool parse_bool(const std::string& s);

}  // namespace lights3
