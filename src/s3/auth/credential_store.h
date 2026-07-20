// L2: 动态凭证管理（docs/credential-management.md）：AK/SK 的生成/查询/吊销 + IStorageBackend 持久化。
// 两级模型：静态凭证（配置文件）= root，动态凭证（本类生成）仅数据面。
#pragma once

#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include "core/config.h"
#include "core/task.h"
#include "s3/auth/sigv4.h"
#include "storage/backend.h"

namespace lights3::s3 {

struct CredentialInfo {
    std::string access_key;
    std::string secret_key;
    bool is_static = false;  // 静态（配置文件）来源 = root
    std::string comment;
    std::chrono::system_clock::time_point created;
};

// 保留系统 bucket 与对象键前缀（docs/credential-management.md §4.1）
inline constexpr std::string_view kSysBucket = ".sys";
inline constexpr std::string_view kCredPrefix = "credentials/";

class CredentialStore final : public ICredentialProvider {
public:
    // 启动时全量加载 .sys/credentials/*，与静态表合并（同 AK 静态优先并告警）。
    // .sys 不存在视为空表；单个对象损坏跳过并告警，不阻塞启动
    static Task<std::shared_ptr<CredentialStore>> load(
        std::shared_ptr<storage::IStorageBackend> backend, const AuthConfig& static_cfg);

    // ---- ICredentialProvider（验签热路径，读锁） ----
    std::optional<std::string> secret_for(std::string_view ak) const override;
    bool has_credentials() const override;

    bool is_root(std::string_view ak) const;

    // ---- 管理面（docs/credential-management.md §5.1）----
    // 先写 storage 成功再改内存（write-through）：崩溃时以 storage 为准，
    // 内存顶多"少"不会"多"。注意不在持锁状态下 co_await（协程可能换线程恢复，
    // std::mutex 跨线程解锁是 UB）；并发 generate 的唯一性由 AK 随机空间保证
    Task<CredentialInfo> generate(std::string comment);
    // 不存在 → InvalidAccessKeyId；静态凭证 → MethodNotAllowed（归配置文件管）
    Task<void> remove(std::string_view ak);

    std::optional<CredentialInfo> find(std::string_view ak) const;
    std::vector<CredentialInfo> list() const;  // 按 AK 排序

private:
    CredentialStore() = default;

    std::shared_ptr<storage::IStorageBackend> backend_;
    mutable std::shared_mutex mu_;
    std::map<std::string, CredentialInfo, std::less<>> creds_;
    std::atomic<bool> sys_bucket_ready_{false};  // 惰性 create_bucket(".sys") 只做一次
};

}  // namespace lights3::s3
