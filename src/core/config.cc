#include "core/config.h"

#include <climits>
#include <cstdlib>
#include <fstream>
#include <limits>
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

// ${VAR} → 环境变量值；未定义展开为空串
std::string expand_env(const std::string& s) {
    std::string out;
    for (size_t i = 0; i < s.size();) {
        if (s[i] == '$' && i + 1 < s.size() && s[i + 1] == '{') {
            auto end = s.find('}', i + 2);
            if (end != std::string::npos) {
                std::string var = s.substr(i + 2, end - i - 2);
                if (const char* v = getenv(var.c_str())) out += v;
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
        // 注释：行首 # 或 " #"（简化处理；引号内含 " #" 的值不支持）
        if (!content.empty() && content[0] == '#') content.clear();
        auto pos = content.find(" #");
        if (pos != std::string::npos) content = content.substr(0, pos);
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
int to_int(const std::string& s, int def) { return s.empty() ? def : std::stoi(s); }
}  // namespace

Config Config::from_string(const std::string& text) {
    Config cfg;
    YamlNode root = yaml_parse(text);

    if (auto* http = root.find("http")) {
        cfg.http.driver = http->get("driver", cfg.http.driver);
        cfg.http.bind = http->get("bind", cfg.http.bind);
        if (auto v = http->get("port"); !v.empty()) {
            int p = std::stoi(v);
            if (p < 1 || p > 65535)
                throw std::runtime_error("config: http.port out of range: " + v);
            cfg.http.port = static_cast<uint16_t>(p);
        }
        cfg.http.io_threads = to_int(http->get("io_threads"), cfg.http.io_threads);
        cfg.http.base_domain = http->get("base_domain", cfg.http.base_domain);
        if (auto v = http->get("max_header_size"); !v.empty())
            cfg.http.max_header_size = parse_size(v);
        if (auto v = http->get("idle_timeout"); !v.empty())
            cfg.http.idle_timeout_sec = parse_duration_sec(v);
    }
    if (auto* rt = root.find("runtime")) {
        cfg.runtime.io_threads = to_int(rt->get("io_threads"), cfg.runtime.io_threads);
        cfg.runtime.max_inflight_requests =
            to_int(rt->get("max_inflight_requests"), cfg.runtime.max_inflight_requests);
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

    // 一致性检查
    if (cfg.http.io_threads < 1)
        throw std::runtime_error("config: http.io_threads must be >= 1");
    if (cfg.runtime.io_threads < 1)
        throw std::runtime_error("config: runtime.io_threads must be >= 1");
    // <= 0 会让第一个请求在信号量上永久挂起，须启动时报错而非静默挂死
    if (cfg.runtime.max_inflight_requests < 1)
        throw std::runtime_error("config: runtime.max_inflight_requests must be >= 1");
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
