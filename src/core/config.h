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
    // 打断请求（挂起点抛 OperationCancelled → 503 SlowDown，SDK 可重试）。
    // idle_timeout 只覆盖 socket 系统调用，覆盖不到 handler 执行期。0 = 关闭
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
    // TLS（docs/gaps.md §7）：cert/key 都给出即启用 HTTPS。SigV4 的 UNSIGNED-PAYLOAD
    // 完整性依赖传输层加密，入站方向由这里承接。仅 httplib/beast 驱动支持；
    // builtin/seastar 配置了 TLS 会在启动时报错——绝不"配了但静默跑明文"
    std::string tls_cert;  // PEM 证书链路径
    std::string tls_key;   // PEM 私钥路径
    // builtin 驱动是 thread-per-connection，io_threads 对它无意义；显式配置时
    // 启动 WARN 而不是静默忽略（docs/gaps.md §7）。解析器置位
    bool io_threads_set = false;
    // ---- 停机/背压边界（docs/gaps.md §7）：曾是四驱动各写一份的硬编码 ----
    uint64_t drain_limit = 4 * 1024 * 1024;   // 回错前排空请求体上限
    size_t trailer_max_size = 16 * 1024;      // chunked trailer 区上限（builtin/seastar）
    size_t io_chunk_size = 64 * 1024;         // 流式读写块大小
    size_t body_queue_cap = 256 * 1024;       // 推转拉体队列容量（仅 httplib，即背压水位）
    int shutdown_grace_sec = 10;              // 停机等在途请求的宽限
    int shutdown_force_wait_sec = 5;          // 强制断开后的二次等待
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
