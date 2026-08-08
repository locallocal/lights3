// L4: 配置加载。内置一个覆盖本项目配置形态的 YAML 子集解析器：
// 嵌套 map、"- " 列表、标量、注释、${ENV} 展开。不支持 flow style/锚点/多行标量。
#pragma once

#include <cstdint>
#include <map>
#include <optional>
#include <string>
#include <vector>

#include "core/util/crypto.h"

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
    // 请求级超时（docs/gaps.md §3.3）：从 handler 开始执行起计时，到点以协作式取消
    // 打断请求（挂起点抛 OperationCancelled → 504）。idle_timeout 只覆盖 socket
    // 系统调用，覆盖不到 handler 执行期。0 = 关闭
    int request_timeout_sec = 300;
    // multipart 最小分片字节数（docs/gaps.md §5.7）：AWS 恒 5MiB，0 = 不限制。
    // 前面挂着不守这条规则的工具链、或本实例只是另一个 lights3 的代理时可放开
    uint64_t min_part_size = 5ull * 1024 * 1024;
    // 传输停滞上限：流式收发**整体**允许的无进展时长。四驱动的分块超时都是逐块
    // 重置的，每 59 秒发 1 字节的客户端可无限期占住连接。0 = 关闭
    int transfer_stall_timeout_sec = 300;
    // 并发连接硬上限（四驱动统一；httplib 由其线程池隐式约束）：超限拒绝新连接，
    // 无上限时每连接的线程/协程帧/缓冲可耗尽内存
    int max_connections = 4096;
    std::string base_domain;  // 非空时启用 virtual-host style（docs/s3-protocol.md §2）
};

struct RuntimeConfig {
    int io_threads = 16;
    int max_inflight_requests = 1024;
};

struct Credential {
    std::string access_key;
    util::SecretString secret_key;  // 析构即擦除（docs/gaps.md §4）
};

struct AuthConfig {
    std::vector<Credential> credentials;  // 为空则关闭认证（demo/测试用）
    std::string region = "us-east-1";
    std::string service = "s3";
    // 凭证管理二期（docs/credential-management.md §10）
    std::string credentials_file;          // 外部凭证文件（JSON，热加载）；空 = 不启用
    int credentials_file_reload_sec = 30;  // 文件 mtime 轮询周期；0 = 仅启动时加载
    int sync_interval_sec = 0;             // 多实例：定期增量 reload .sys；0 = 关闭
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

// "16KiB" / "1MB" / "60s" / "true" 之类的解析辅助
size_t parse_size(const std::string& s);
int parse_duration_sec(const std::string& s);
bool parse_bool(const std::string& s);  // true/1/yes/on | false/0/no/off，其余抛 runtime_error

}  // namespace lights3
