#include "core/config.h"

#include <climits>
#include <cstdlib>
#include <fstream>
#include <limits>
#include <optional>
#include <set>
#include <sstream>
#include <stdexcept>

namespace lights3 {

// ---------------- YAML subset parsing ----------------
namespace {

struct Line {
    int indent = 0;
    int lineno = 0;    // original line number (1-based), for error reporting
    std::string text;  // content with indentation and comments stripped
};

// ${VAR} -> environment variable value. Undefined is an error: silently expanding
// to an empty string turns "misspelled env var name" into "credential/path quietly
// went empty", and the failure resurfaces in a different guise long after startup
// (docs/archive/gaps.md §3.9). For genuinely optional values write ${VAR:-default}
std::string expand_env(const std::string& s) {
    std::string out;
    for (size_t i = 0; i < s.size();) {
        if (s[i] == '$' && i + 1 < s.size() && s[i + 1] == '{') {
            auto end = s.find('}', i + 2);
            if (end != std::string::npos) {
                std::string expr = s.substr(i + 2, end - i - 2);
                std::string var = expr;
                std::optional<std::string> def;
                if (auto d = expr.find(":-"); d != std::string::npos) {
                    var = expr.substr(0, d);
                    def = expr.substr(d + 2);
                }
                if (const char* v = getenv(var.c_str())) out += v;
                else if (def) out += *def;
                else
                    throw std::runtime_error("config: undefined environment variable ${" + var +
                                             "} (use ${" + var + ":-default} if optional)");
                i = end + 1;
                continue;
            }
        }
        out.push_back(s[i++]);
    }
    return out;
}

std::string trim(const std::string& s) {
    size_t b = s.find_first_not_of(" \t");
    if (b == std::string::npos) return "";
    size_t e = s.find_last_not_of(" \t");
    return s.substr(b, e - b + 1);
}

std::string unquote(const std::string& s) {
    if (s.size() >= 2 && ((s.front() == '"' && s.back() == '"') ||
                          (s.front() == '\'' && s.back() == '\'')))
        return s.substr(1, s.size() - 2);
    return s;
}

std::vector<Line> to_lines(const std::string& text) {
    std::vector<Line> lines;
    std::istringstream is(text);
    std::string raw;
    int lineno = 0;
    while (std::getline(is, raw)) {
        ++lineno;
        if (!raw.empty() && raw.back() == '\r') raw.pop_back();
        size_t indent = 0;
        while (indent < raw.size() && raw[indent] == ' ') ++indent;
        if (indent < raw.size() && raw[indent] == '\t')
            throw std::runtime_error("yaml: tab indentation not supported");
        std::string content = raw.substr(indent);
        // Comments: a leading # or an unquoted " #". A " #" inside quotes is not a
        // comment — a naive find(" #") would truncate secret_key: "a #b" to "a,
        // silently shortening the key (docs/archive/gaps.md §3.9)
        if (!content.empty() && content[0] == '#') {
            content.clear();
        } else {
            char quote = 0;
            for (size_t j = 0; j < content.size(); ++j) {
                char c = content[j];
                if (quote) {
                    if (c == quote) quote = 0;
                } else if (c == '"' || c == '\'') {
                    quote = c;
                } else if (c == '#' && j > 0 && (content[j - 1] == ' ' || content[j - 1] == '\t')) {
                    content = content.substr(0, j);
                    break;
                }
            }
        }
        content = trim(content);
        if (content.empty()) continue;
        lines.push_back({static_cast<int>(indent), lineno, std::move(content)});
    }
    return lines;
}

class Parser {
public:
    explicit Parser(std::vector<Line> lines) : lines_(std::move(lines)) {}

    YamlNode parse() {
        if (lines_.empty()) return YamlNode{YamlNode::Type::Map, {}, {}, {}};
        YamlNode root = parse_block(lines_[0].indent, 0);
        // A line with mismatched indentation makes every block loop exit instead of
        // consuming it; without this check it is silently dropped (a lost optional
        // parameter fails without a sound), so always report an error here
        if (i_ < lines_.size())
            throw std::runtime_error("yaml: unexpected indent at line " +
                                     std::to_string(lines_[i_].lineno));
        return root;
    }

private:
    static constexpr int kMaxDepth = 64;  // guards against stack overflow on pathologically deep input

    // Parse the block starting at the current line whose indentation is exactly `indent`
    YamlNode parse_block(int indent, int depth) {
        if (depth > kMaxDepth)
            throw std::runtime_error("yaml: nesting too deep at line " +
                                     std::to_string(lines_[i_].lineno));
        YamlNode node;
        if (i_ < lines_.size() && lines_[i_].text.rfind("- ", 0) == 0) {
            node.type = YamlNode::Type::List;
            while (i_ < lines_.size() && lines_[i_].indent == indent &&
                   lines_[i_].text.rfind("- ", 0) == 0) {
                // Treat "- xxx" as a line indented at indent+2; together with
                // following lines at the same indentation it forms one item
                lines_[i_].text = lines_[i_].text.substr(2);
                lines_[i_].indent = indent + 2;
                node.list.push_back(parse_block(indent + 2, depth + 1));
            }
            return node;
        }
        node.type = YamlNode::Type::Map;
        while (i_ < lines_.size() && lines_[i_].indent == indent &&
               lines_[i_].text.rfind("- ", 0) != 0) {
            auto& text = lines_[i_].text;
            auto colon = text.find(':');
            if (colon == std::string::npos)
                throw std::runtime_error("yaml: expected 'key:' at '" + text + "'");
            std::string key = trim(text.substr(0, colon));
            std::string val = trim(text.substr(colon + 1));
            ++i_;
            if (!val.empty()) {
                YamlNode child;
                child.type = YamlNode::Type::Scalar;
                child.scalar = expand_env(unquote(val));
                node.map.emplace_back(std::move(key), std::move(child));
            } else {
                // Nested block: use the next line's indentation (must be deeper);
                // no content means an empty map
                if (i_ < lines_.size() && lines_[i_].indent > indent) {
                    node.map.emplace_back(std::move(key),
                                          parse_block(lines_[i_].indent, depth + 1));
                } else {
                    node.map.emplace_back(std::move(key),
                                          YamlNode{YamlNode::Type::Map, {}, {}, {}});
                }
            }
        }
        return node;
    }

    std::vector<Line> lines_;
    size_t i_ = 0;
};

}  // namespace

const YamlNode* YamlNode::find(const std::string& key) const {
    for (auto& [k, v] : map)
        if (k == key) return &v;
    return nullptr;
}

std::string YamlNode::get(const std::string& key, const std::string& def) const {
    auto* n = find(key);
    return (n && n->type == Type::Scalar) ? n->scalar : def;
}

YamlNode yaml_parse(const std::string& text) { return Parser(to_lines(text)).parse(); }

// ---------------- Sizes / durations ----------------

size_t parse_size(const std::string& s) {
    // stoull accepts "-1" and wraps it to 2^64-1, so reject the minus sign explicitly
    if (s.find('-') != std::string::npos)
        throw std::runtime_error("negative size not allowed: " + s);
    size_t pos = 0;
    unsigned long long num = std::stoull(s, &pos);
    std::string unit = trim(s.substr(pos));
    int shift = 0;
    if (unit.empty() || unit == "B") shift = 0;
    else if (unit == "KiB" || unit == "KB" || unit == "K" || unit == "k") shift = 10;
    else if (unit == "MiB" || unit == "MB" || unit == "M" || unit == "m") shift = 20;
    else if (unit == "GiB" || unit == "GB" || unit == "G" || unit == "g") shift = 30;
    else throw std::runtime_error("bad size unit: " + s);
    if (shift && num > (std::numeric_limits<size_t>::max() >> shift))
        throw std::runtime_error("size out of range: " + s);
    return static_cast<size_t>(num) << shift;
}

int parse_duration_sec(const std::string& s) {
    size_t pos = 0;
    long long num = std::stoll(s, &pos);
    std::string unit = trim(s.substr(pos));
    long long mult = 0;
    if (unit.empty() || unit == "s") mult = 1;
    else if (unit == "m") mult = 60;
    else if (unit == "h") mult = 3600;
    else if (unit == "d") mult = 86400;  // tiered storage's cold_after (docs/tiered-storage.md §8)
    else throw std::runtime_error("bad duration unit: " + s);
    if (num < 0 || num > INT_MAX / mult)
        throw std::runtime_error("duration out of range: " + s);
    return static_cast<int>(num * mult);
}

bool parse_bool(const std::string& s) {
    if (s == "true" || s == "1" || s == "yes" || s == "on") return true;
    if (s == "false" || s == "0" || s == "no" || s == "off") return false;
    throw std::runtime_error("bad bool value: " + s);
}

// ---------------- Typed configuration ----------------

namespace {

// Integer parsing with context: bare stoi throws a bare std::invalid_argument("stoi")
// on bad input, so the error the operator sees carries neither the key name nor the
// original value (docs/archive/gaps.md §3.9). Also rejects trailing garbage ("8x" is no
// longer treated as 8) and out-of-range values
int to_int(const std::string& key, const std::string& s, int def) {
    if (s.empty()) return def;
    size_t pos = 0;
    long long v = 0;
    try {
        v = std::stoll(s, &pos);
    } catch (const std::exception&) {
        throw std::runtime_error("config: " + key + " is not an integer: '" + s + "'");
    }
    if (trim(s.substr(pos)) != "")
        throw std::runtime_error("config: " + key + " has trailing garbage: '" + s + "'");
    if (v < INT_MIN || v > INT_MAX)
        throw std::runtime_error("config: " + key + " out of range: '" + s + "'");
    return static_cast<int>(v);
}

void check_range(const std::string& key, int v, int lo, int hi) {
    if (v < lo || v > hi)
        throw std::runtime_error("config: " + key + " must be in [" + std::to_string(lo) + "," +
                                 std::to_string(hi) + "], got " + std::to_string(v));
}

// Size-typed parameters (parse_size returns 64 bits): the int overload would
// truncate before comparing, and after a huge value wraps to negative the reported
// "got" would be wrong
void check_range(const std::string& key, long long v, long long lo, long long hi) {
    if (v < lo || v > hi)
        throw std::runtime_error("config: " + key + " must be in [" + std::to_string(lo) + "," +
                                 std::to_string(hi) + "], got " + std::to_string(v));
}

}  // namespace

Config Config::from_string(const std::string& text) {
    Config cfg;
    YamlNode root = yaml_parse(text);

    if (auto* http = root.find("http")) {
        cfg.http.driver = http->get("driver", cfg.http.driver);
        cfg.http.bind = http->get("bind", cfg.http.bind);
        if (auto v = http->get("port"); !v.empty()) {
            int p = to_int("http.port", v, cfg.http.port);
            // 0 is legal: let the kernel pick a free port (the e2e/unit-test fixtures rely on this convention)
            check_range("http.port", p, 0, 65535);
            cfg.http.port = static_cast<uint16_t>(p);
        }
        if (auto v = http->get("io_threads"); !v.empty()) {
            cfg.http.io_threads = to_int("http.io_threads", v, cfg.http.io_threads);
            cfg.http.io_threads_set = true;  // the builtin driver WARNs based on this (docs/archive/gaps.md §7)
        }
        cfg.http.base_domain = http->get("base_domain", cfg.http.base_domain);
        if (auto v = http->get("max_header_size"); !v.empty())
            cfg.http.max_header_size = parse_size(v);
        if (auto v = http->get("idle_timeout"); !v.empty())
            cfg.http.idle_timeout_sec = parse_duration_sec(v);
        // Per-request timeout and transfer stall limit (docs/archive/gaps.md §3.3): 0 = disabled
        if (auto v = http->get("request_timeout"); !v.empty())
            cfg.http.request_timeout_sec = parse_duration_sec(v);
        // Minimum multipart part size (docs/archive/gaps.md §5.7): 0 = no limit
        if (auto v = http->get("min_part_size"); !v.empty())
            cfg.http.min_part_size = parse_size(v);
        if (auto v = http->get("transfer_stall_timeout"); !v.empty())
            cfg.http.transfer_stall_timeout_sec = parse_duration_sec(v);
        cfg.http.max_connections =
            to_int("http.max_connections", http->get("max_connections"), cfg.http.max_connections);
        check_range("http.max_connections", cfg.http.max_connections, 1, 1'000'000);
        // TLS (docs/archive/gaps.md §7): enabled only when both are given; giving just one is surely a misconfiguration
        cfg.http.tls_cert = http->get("tls_cert", cfg.http.tls_cert);
        cfg.http.tls_key = http->get("tls_key", cfg.http.tls_key);
        if (cfg.http.tls_cert.empty() != cfg.http.tls_key.empty())
            throw std::runtime_error(
                "config: http.tls_cert and http.tls_key must be set together");
        // Shutdown/backpressure knobs (docs/archive/gaps.md §7)
        if (auto v = http->get("drain_limit"); !v.empty())
            cfg.http.drain_limit = parse_size(v);
        if (auto v = http->get("trailer_max_size"); !v.empty())
            cfg.http.trailer_max_size = parse_size(v);
        if (auto v = http->get("io_chunk_size"); !v.empty())
            cfg.http.io_chunk_size = parse_size(v);
        if (auto v = http->get("body_queue_cap"); !v.empty())
            cfg.http.body_queue_cap = parse_size(v);
        if (auto v = http->get("shutdown_grace"); !v.empty())
            cfg.http.shutdown_grace_sec = parse_duration_sec(v);
        if (auto v = http->get("shutdown_force_wait"); !v.empty())
            cfg.http.shutdown_force_wait_sec = parse_duration_sec(v);
    }
    if (auto* rt = root.find("runtime")) {
        cfg.runtime.io_threads =
            to_int("runtime.io_threads", rt->get("io_threads"), cfg.runtime.io_threads);
        cfg.runtime.max_inflight_requests = to_int(
            "runtime.max_inflight_requests", rt->get("max_inflight_requests"),
            cfg.runtime.max_inflight_requests);
    }
    if (auto* auth = root.find("auth")) {
        cfg.auth.region = auth->get("region", cfg.auth.region);
        cfg.auth.service = auth->get("service", cfg.auth.service);
        cfg.auth.credentials_file = auth->get("credentials_file", cfg.auth.credentials_file);
        if (auto v = auth->get("credentials_file_reload"); !v.empty())
            cfg.auth.credentials_file_reload_sec = parse_duration_sec(v);
        if (auto v = auth->get("sync_interval"); !v.empty())
            cfg.auth.sync_interval_sec = parse_duration_sec(v);
        if (auto* creds = auth->find("credentials"); creds && creds->type == YamlNode::Type::List) {
            for (auto& c : creds->list) {
                Credential cr{c.get("access_key"), c.get("secret_key")};
                if (cr.access_key.empty() || cr.secret_key.empty())
                    throw std::runtime_error("config: credential needs access_key + secret_key");
                cfg.auth.credentials.push_back(std::move(cr));
            }
        }
    }
    if (auto* bs = root.find("backends"); bs && bs->type == YamlNode::Type::List) {
        for (auto& b : bs->list) {
            BackendConfig bc;
            for (auto& [k, v] : b.map) {
                if (k == "name") bc.name = v.scalar;
                else if (k == "type") bc.type = v.scalar;
                else if (v.type == YamlNode::Type::Scalar) bc.params[k] = v.scalar;
            }
            if (bc.name.empty() || bc.type.empty())
                throw std::runtime_error("config: backend needs name + type");
            // Duplicate backend names: the registry keys by name, so the later one
            // silently overwrites the earlier one and which one bucket routing hits
            // depends on insertion order — error out at startup
            for (auto& prev : cfg.backends)
                if (prev.name == bc.name)
                    throw std::runtime_error("config: duplicate backend name '" + bc.name + "'");
            cfg.backends.push_back(std::move(bc));
        }
    }
    if (auto* bk = root.find("buckets")) {
        cfg.buckets.default_backend = bk->get("default_backend");
        if (auto* rules = bk->find("rules"); rules && rules->type == YamlNode::Type::List) {
            for (auto& r : rules->list) {
                BucketRule rule{r.get("match"), r.get("backend")};
                // An empty glob can never match a valid bucket name, and an empty
                // backend would surface as 'unknown backend ""' — both mean the rule
                // is mangled, so name the actual problem
                if (rule.match.empty() || rule.backend.empty())
                    throw std::runtime_error("config: buckets.rules entry needs match + backend");
                cfg.buckets.rules.push_back(std::move(rule));
            }
        }
    }
    if (auto* web = root.find("website"); web && web->type == YamlNode::Type::List) {
        for (auto& w : web->list) {
            WebsiteBucket wb;
            wb.bucket = w.get("bucket");
            wb.index_suffix = w.get("index_suffix", wb.index_suffix);
            wb.error_key = w.get("error_key");
            if (wb.bucket.empty()) throw std::runtime_error("config: website entry needs bucket");
            // AWS rule: the index suffix is non-empty and contains no '/' — with a slash,
            // "docs/" would map to a key outside that directory
            if (wb.index_suffix.empty() || wb.index_suffix.find('/') != std::string::npos)
                throw std::runtime_error(
                    "config: website index_suffix must be non-empty and contain no '/'");
            // Keys never start with '/' (the path form "/bucket/key" strips it); a leading
            // slash here would make the error object silently unreachable
            if (!wb.error_key.empty() && wb.error_key.front() == '/')
                throw std::runtime_error("config: website error_key must not start with '/'");
            // RedirectAllRequestsTo (roadmap §2.3): host enables it, protocol optional.
            // A protocol without a host is a half-configured redirect — reject rather
            // than silently ignoring it
            wb.redirect_all_host = w.get("redirect_all_host");
            wb.redirect_all_protocol = w.get("redirect_all_protocol");
            if (!wb.redirect_all_protocol.empty() && wb.redirect_all_protocol != "http" &&
                wb.redirect_all_protocol != "https")
                throw std::runtime_error(
                    "config: website redirect_all_protocol must be http or https");
            if (!wb.redirect_all_protocol.empty() && wb.redirect_all_host.empty())
                throw std::runtime_error(
                    "config: website redirect_all_protocol requires redirect_all_host");
            // Anonymous rate limit (roadmap §2.3): 0 = unlimited
            int rps = to_int("website.max_rps", w.get("max_rps"), 0);
            check_range("website.max_rps", rps, 0, 1'000'000);
            wb.max_rps = static_cast<uint32_t>(rps);
            // Duplicates rejected like backend names: a copy-pasted entry usually means
            // the second one was meant to be a different bucket
            for (auto& prev : cfg.website.buckets)
                if (prev.bucket == wb.bucket)
                    throw std::runtime_error("config: duplicate website bucket '" + wb.bucket +
                                             "'");
            cfg.website.buckets.push_back(std::move(wb));
        }
    }
    if (auto* log = root.find("log")) cfg.log_level = log->get("level", cfg.log_level);
    // parse_level in app.cc maps anything unknown to Info, so a misspelled level
    // ("warning", "trace") would silently downgrade — the operator believes debug
    // logging is on while it is not. Reject it here instead
    if (cfg.log_level != "debug" && cfg.log_level != "info" && cfg.log_level != "warn" &&
        cfg.log_level != "error")
        throw std::runtime_error("config: log.level must be one of debug|info|warn|error, got '" +
                                 cfg.log_level + "'");

    // Consistency checks. Thread-count upper bounds align with the per-backend
    // parameters of the same name ([1,1024]): without an upper bound, a fat-fingered
    // io_threads: 100000 would bring the process down right at startup
    check_range("http.io_threads", cfg.http.io_threads, 1, 1024);
    check_range("runtime.io_threads", cfg.runtime.io_threads, 1, 1024);
    // <= 0 would leave the very first request suspended on the semaphore forever;
    // must fail at startup instead of silently hanging
    check_range("runtime.max_inflight_requests", cfg.runtime.max_inflight_requests, 1, 1'000'000);
    // beast feeds this into parser.header_limit(uint32_t): an unbounded value like
    // 4GiB truncates to 0 there and every request is rejected with "header too big".
    // The upper bound also guards a slipped unit (KiB written as GiB); the lower
    // bound keeps room for a request line plus SigV4 auth headers
    check_range("http.max_header_size", static_cast<long long>(cfg.http.max_header_size),
                1024LL, 1'048'576LL);
    // 0 means "never time out" to builtin (SO_RCVTIMEO of 0 disables the timeout)
    // but "already expired" to beast (expires_after(0s)) — rather than silently
    // picking one meaning per driver, reject it: an idle timeout must be positive
    check_range("http.idle_timeout", cfg.http.idle_timeout_sec, 1, 86400);
    check_range("http.request_timeout", cfg.http.request_timeout_sec, 0, 86400);
    check_range("http.transfer_stall_timeout", cfg.http.transfer_stall_timeout_sec, 0, 86400);
    // Cross-item consistency: the per-request timeout always fires first, so a stall
    // window longer than it is a knob that looks configured but can never take effect
    if (cfg.http.request_timeout_sec > 0 &&
        cfg.http.transfer_stall_timeout_sec > cfg.http.request_timeout_sec)
        throw std::runtime_error(
            "config: http.transfer_stall_timeout (" +
            std::to_string(cfg.http.transfer_stall_timeout_sec) +
            "s) exceeds http.request_timeout (" + std::to_string(cfg.http.request_timeout_sec) +
            "s) — the stall guard would never fire; lower it or set it to 0 to disable");
    // Shutdown/backpressure knobs (docs/archive/gaps.md §7). Lower bounds guard against
    // "configured to 0 -> write loop spins / never drains"; upper bounds guard
    // against a slipped unit (MiB written as GiB) eating all memory outright
    check_range("http.drain_limit", static_cast<long long>(cfg.http.drain_limit),
                64LL * 1024, 1'073'741'824LL);
    check_range("http.trailer_max_size", static_cast<long long>(cfg.http.trailer_max_size),
                1024LL, 1'048'576LL);
    check_range("http.io_chunk_size", static_cast<long long>(cfg.http.io_chunk_size),
                4096LL, 8LL * 1'048'576);
    check_range("http.body_queue_cap", static_cast<long long>(cfg.http.body_queue_cap),
                4096LL, 1'073'741'824LL);
    check_range("http.shutdown_grace", cfg.http.shutdown_grace_sec, 0, 300);
    check_range("http.shutdown_force_wait", cfg.http.shutdown_force_wait_sec, 0, 300);
    if (cfg.backends.empty()) throw std::runtime_error("config: no backends configured");
    if (cfg.buckets.default_backend.empty()) cfg.buckets.default_backend = cfg.backends[0].name;
    auto has_backend = [&](const std::string& n) {
        for (auto& b : cfg.backends)
            if (b.name == n) return true;
        return false;
    };
    if (!has_backend(cfg.buckets.default_backend))
        throw std::runtime_error("config: unknown default_backend " + cfg.buckets.default_backend);
    for (auto& r : cfg.buckets.rules)
        if (!has_backend(r.backend))
            throw std::runtime_error("config: rule references unknown backend " + r.backend);
    return cfg;
}

Config Config::load(const std::string& path) {
    std::ifstream f(path);
    if (!f) throw std::runtime_error("cannot open config file: " + path);
    std::ostringstream buf;
    buf << f.rdbuf();
    return from_string(buf.str());
}

}  // namespace lights3
