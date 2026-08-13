// per-credential policy（docs/credential-management.md §10.4）。
// 独立成头：验签层（sigv4.h）要在 verify 时刻带出 policy 快照（docs/gaps.md §3.7），
// 而 credential_store.h 依赖 sigv4.h——放回去就成环了。
#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

namespace lights3::s3 {

// 动作粒度（docs/gaps.md §5.10）：此前只有 readonly 一个开关，于是"能写"必然
// "能删"——给不出备份场景最常见的那条策略（只许写入、不许删除）。
// 归类按"造成的后果"而非 HTTP 方法：DeleteObjects 是 POST，却显然属于 Delete
enum class Action { Read, Write, Delete };

const char* action_name(Action a);
// 解析 JSON 里的动作名；未知名字返回 nullopt（调用方转 InvalidRequest）
std::optional<Action> action_from_name(std::string_view s);

// 缺省（nullopt）= 无限制
struct CredentialPolicy {
    std::vector<std::string> buckets;   // bucket glob 白名单；空 = 全部
    std::vector<std::string> prefixes;  // key 前缀白名单；空 = 全部（§5.10）
    bool readonly = false;              // 等价于 actions = [read]，保留兼容
    std::vector<Action> actions;        // 空 = 由 readonly 决定

    // bucket 为空表示账户级操作（ListBuckets）。key 为空表示"本次判定与具体
    // 对象无关"（建桶、列桶内对象等），此时不校验 prefixes
    bool allows(std::string_view bucket, std::string_view key, Action action) const;
    bool allows_action(Action a) const;
    bool allows_bucket(std::string_view bucket) const;
    // 列举结果的 prefix 过滤（多租户共桶时 prefixes 是隔离边界）：
    // allows_key = key 落在某白名单前缀内；prefix_may_contain = 列举返回的
    // CommonPrefixes 分组下**可能**存在白名单内的 key（双向前缀关系任一成立）
    bool allows_key(std::string_view key) const;
    bool prefix_may_contain(std::string_view group_prefix) const;
};

// policy 的 JSON 字段约定
// （{"buckets": [...], "prefixes": [...], "readonly": bool, "actions": [...]}），
// admin handler、落盘对象与 credentials_file 共用一套；实现在 credential_store.cc
//（nlohmann 不进头文件）。字段非法/未知抛 S3Error(InvalidRequest)
CredentialPolicy parse_policy_json(const std::string& text);
std::string policy_to_json(const CredentialPolicy& p);

}  // namespace lights3::s3
