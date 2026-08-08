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

// ---------------- YAML 子集解析 ----------------
namespace {

struct Line {
    int indent = 0;
    int lineno = 0;    // 原始行号（1 起），用于报错定位
    std::string text;  // 去掉缩进与注释后的内容
};

// ${VAR} → 环境变量值。未定义即报错：静默展开成空串会把"环境变量名写错"变成
// "凭证/路径悄悄变空"，错误在启动后很久才以别的面貌出现（docs/gaps.md §3.9）。
// 确实可选的值写 ${VAR:-默认值}
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
        // 注释：行首 # 或引号外的 " #"。引号内的 " #" 不是注释——裸 find(" #")
        // 会把 secret_key: "a #b" 截成 "a，密钥静默变短（docs/gaps.md §3.9）
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
        // 缩进不匹配的行会让各层块循环全部退出而非被消费；
        // 不检查则静默丢弃（可选参数丢了配置无声失效），这里一律报错
        if (i_ < lines_.size())
            throw std::runtime_error("yaml: unexpected indent at line " +
                                     std::to_string(lines_[i_].lineno));
        return root;
    }

private:
    static constexpr int kMaxDepth = 64;  // 病态深嵌套输入防栈溢出

    // 解析从当前行开始、缩进恰为 indent 的块
    YamlNode parse_block(int indent, int depth) {
        if (depth > kMaxDepth)
            throw std::runtime_error("yaml: nesting too deep at line " +
                                     std::to_string(lines_[i_].lineno));
        YamlNode node;
        if (i_ < lines_.size() && lines_[i_].text.rfind("- ", 0) == 0) {
            node.type = YamlNode::Type::List;
            while (i_ < lines_.size() && lines_[i_].indent == indent &&
                   lines_[i_].text.rfind("- ", 0) == 0) {
                // 把 "- xxx" 视为缩进 indent+2 的一行，与后续同缩进行组成 item
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
                // 嵌套块：取下一行缩进（须更深），无内容则视为空 map
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

// ---------------- 尺寸/时长 ----------------

size_t parse_size(const std::string& s) {
    // stoull 接受 "-1" 并回绕成 2^64-1，须显式拒绝负号
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
    else if (unit == "d") mult = 86400;  // tiered 的 cold_after（docs/tiered-storage.md §8）
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

// ---------------- 类型化配置 ----------------

namespace {

// 带上下文的整数解析：裸 stoi 对非法值抛的是无参 std::invalid_argument("stoi")，
// 运维拿到的报错里既没有键名也没有原值（docs/gaps.md §3.9）。同时拒绝尾随垃圾
// （"8x" 不再被当成 8）与越界
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

}  // namespace

Config Config::from_string(const std::string& text) {
    Config cfg;
    YamlNode root = yaml_parse(text);

    if (auto* http = root.find("http")) {
        cfg.http.driver = http->get("driver", cfg.http.driver);
        cfg.http.bind = http->get("bind", cfg.http.bind);
        if (auto v = http->get("port"); !v.empty()) {
            int p = std::stoi(v);
            // 0 合法：让内核分配空闲端口（e2e/单测夹具即用此约定）
            if (p < 0 || p > 65535)
                throw std::runtime_error("config: http.port out of range: " + v);
            cfg.http.port = static_cast<uint16_t>(p);
        }
        cfg.http.io_threads =
            to_int("http.io_threads", http->get("io_threads"), cfg.http.io_threads);
        cfg.http.base_domain = http->get("base_domain", cfg.http.base_domain);
        if (auto v = http->get("max_header_size"); !v.empty())
            cfg.http.max_header_size = parse_size(v);
        if (auto v = http->get("idle_timeout"); !v.empty())
            cfg.http.idle_timeout_sec = parse_duration_sec(v);
        // 请求级超时与传输停滞上限（docs/gaps.md §3.3）：0 = 关闭
        if (auto v = http->get("request_timeout"); !v.empty())
            cfg.http.request_timeout_sec = parse_duration_sec(v);
        // multipart 最小分片（docs/gaps.md §5.7）：0 = 不限制
        if (auto v = http->get("min_part_size"); !v.empty())
            cfg.http.min_part_size = parse_size(v);
        if (auto v = http->get("transfer_stall_timeout"); !v.empty())
            cfg.http.transfer_stall_timeout_sec = parse_duration_sec(v);
        cfg.http.max_connections =
            to_int("http.max_connections", http->get("max_connections"), cfg.http.max_connections);
        check_range("http.max_connections", cfg.http.max_connections, 1, 1'000'000);
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
            // 重名后端：注册表按名建表，后者静默覆盖前者，桶路由指向的是哪一个
            // 取决于插入顺序——启动即报错
            for (auto& prev : cfg.backends)
                if (prev.name == bc.name)
                    throw std::runtime_error("config: duplicate backend name '" + bc.name + "'");
            cfg.backends.push_back(std::move(bc));
        }
    }
    if (auto* bk = root.find("buckets")) {
        cfg.buckets.default_backend = bk->get("default_backend");
        if (auto* rules = bk->find("rules"); rules && rules->type == YamlNode::Type::List) {
            for (auto& r : rules->list)
                cfg.buckets.rules.push_back({r.get("match"), r.get("backend")});
        }
    }
    if (auto* log = root.find("log")) cfg.log_level = log->get("level", cfg.log_level);

    // 一致性检查。线程数上界与 per-backend 的同名参数取齐（[1,1024]）：
    // 没有上界时一个手滑的 io_threads: 100000 会在启动时直接把进程拖垮
    check_range("http.io_threads", cfg.http.io_threads, 1, 1024);
    check_range("runtime.io_threads", cfg.runtime.io_threads, 1, 1024);
    // <= 0 会让第一个请求在信号量上永久挂起，须启动时报错而非静默挂死
    check_range("runtime.max_inflight_requests", cfg.runtime.max_inflight_requests, 1, 1'000'000);
    check_range("http.request_timeout", cfg.http.request_timeout_sec, 0, 86400);
    check_range("http.transfer_stall_timeout", cfg.http.transfer_stall_timeout_sec, 0, 86400);
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
