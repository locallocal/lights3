// L4: 配置加载。内置一个覆盖本项目配置形态的 YAML 子集解析器：
// 嵌套 map、"- " 列表、标量、注释、${ENV} 展开。不支持 flow style/锚点/多行标量。
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

namespace lights3 {

// ---------- YAML 子集节点 ----------
struct YamlNode {
    enum class Type { Scalar, Map, List };
    Type type = Type::Scalar;
    std::string scalar;
    std::vector<std::pair<std::string, YamlNode>> map;  // 保序
    std::vector<YamlNode> list;

    const YamlNode* find(const std::string& key) const;
    // 取子标量，不存在返回 def
    std::string get(const std::string& key, const std::string& def = "") const;
};

YamlNode yaml_parse(const std::string& text);  // 语法错误抛 std::runtime_error

// ---------- 类型化配置 ----------
struct HttpConfig {
    std::string driver = "builtin";
    std::string bind = "0.0.0.0";
    uint16_t port = 9000;
    int io_threads = 4;
    size_t max_header_size = 16 * 1024;
    int idle_timeout_sec = 60;
    std::string base_domain;  // 非空时启用 virtual-host style（docs/05 §2）
};

struct RuntimeConfig {
    int io_threads = 16;
    int max_inflight_requests = 1024;
};

struct Credential {
    std::string access_key;
    std::string secret_key;
};

struct AuthConfig {
    std::vector<Credential> credentials;  // 为空则关闭认证（demo/测试用）
    std::string region = "us-east-1";
    std::string service = "s3";
};

struct BackendConfig {
    std::string name;
    std::string type;                            // localfs | memory | ...
    std::map<std::string, std::string> params;   // root/staging/endpoint/... 由各后端解释
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

// "16KiB" / "1MB" / "60s" 之类的解析辅助
size_t parse_size(const std::string& s);
int parse_duration_sec(const std::string& s);

}  // namespace lights3
