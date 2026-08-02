// L2: 动态凭证管理（docs/credential-management.md）：AK/SK 的生成/查询/吊销 + IStorageBackend 持久化。
// 三来源模型：静态凭证（配置文件）= root；文件凭证（credentials_file 热加载）与
// 动态凭证（本类生成）仅数据面，可携带 per-credential policy（§10）。
#pragma once

#include <atomic>
#include <chrono>
#include <filesystem>
#include <map>
#include <memory>
#include <optional>
#include <shared_mutex>
#include <string>
#include <string_view>
#include <vector>

#include "core/background.h"
#include "core/config.h"
#include "core/task.h"
#include "core/thread_pool.h"
#include "core/timer.h"
#include "core/util/crypto.h"
#include "s3/auth/sigv4.h"
#include "storage/backend.h"

namespace lights3::s3 {

enum class CredSource { kStatic, kFile, kDynamic };

// per-credential policy（docs/credential-management.md §10.4）。缺省（nullopt）= 无限制
struct CredentialPolicy {
    std::vector<std::string> buckets;  // bucket glob 白名单；空 = 全部
    bool readonly = false;             // true: 仅允许 GET/HEAD
    bool allows(std::string_view bucket, bool is_write) const;
};

// policy 的 JSON 字段约定（{"buckets": [...], "readonly": bool}），admin handler、
// 落盘对象与 credentials_file 共用一套；实现在 .cc（nlohmann 不进头文件）。
// 字段非法/未知抛 S3Error(InvalidRequest)
CredentialPolicy parse_policy_json(const std::string& text);
std::string policy_to_json(const CredentialPolicy& p);

struct CredentialInfo {
    std::string access_key;
    std::string secret_key;
    CredSource source = CredSource::kDynamic;
    std::string comment;
    std::chrono::system_clock::time_point created;
    std::optional<CredentialPolicy> policy;  // 仅 file/dynamic；静态凭证恒无限制

    bool is_static() const { return source == CredSource::kStatic; }
};

// 保留系统 bucket 与对象键前缀（docs/credential-management.md §4.1）
inline constexpr std::string_view kSysBucket = ".sys";
inline constexpr std::string_view kCredPrefix = "credentials/";

// SK at-rest 加密的 master key 环境变量（§10.1）：64 个 hex 字符（32 字节）。
// 设置后新写入的凭证对象为 version=2（AES-256-GCM），load 时 v1 对象自动升级
inline constexpr const char* kMasterKeyEnv = "LIGHTS3_MASTER_KEY";

class CredentialStore final : public ICredentialProvider {
public:
    // 启动时全量加载 .sys/credentials/* 与 credentials_file，与静态表合并
    //（同 AK 优先级 static > file > dynamic，并告警）。.sys 不存在视为空表；
    // 单个对象 JSON 损坏跳过并告警；v2 对象无 master key / 解密失败则抛错
    //（配置错误应当拦下启动，而非静默丢凭证把用户锁在门外）
    static Task<std::shared_ptr<CredentialStore>> load(
        std::shared_ptr<storage::IStorageBackend> backend, const AuthConfig& cfg);

    ~CredentialStore() { shutdown_background(); }

    // ---- ICredentialProvider（验签热路径，读锁） ----
    std::optional<std::string> secret_for(std::string_view ak) const override;
    bool has_credentials() const override;

    bool is_root(std::string_view ak) const;

    // per-credential policy 执行（§10.4）：验签通过后、路由前调用；
    // 静态凭证/无 policy 恒通过，拒绝抛 S3Error(AccessDenied)。
    // ak 为空（认证关闭）或查不到（在途吊销竞态，§7 语义）也放行
    void authorize(std::string_view ak, std::string_view bucket, bool is_write) const;

    // ---- 管理面（docs/credential-management.md §5.1）----
    // 先写 storage 成功再改内存（write-through）：崩溃时以 storage 为准，
    // 内存顶多"少"不会"多"。注意不在持锁状态下 co_await（协程可能换线程恢复，
    // std::mutex 跨线程解锁是 UB）；并发 generate 的唯一性由 AK 随机空间保证
    Task<CredentialInfo> generate(std::string comment,
                                  std::optional<CredentialPolicy> policy = std::nullopt);
    // 不存在 → InvalidAccessKeyId；静态/文件凭证 → MethodNotAllowed（归配置/文件管）
    Task<void> remove(std::string_view ak);

    std::optional<CredentialInfo> find(std::string_view ak) const;
    std::vector<CredentialInfo> list() const;  // 按 AK 排序

    // ---- 后台任务（§10.2 文件热加载轮询 / §10.3 多实例定期增量同步）----
    // main 装配后调用一次；shutdown_background 须在线程池 join 前调用（幂等）
    void start_background(std::shared_ptr<ThreadPool> pool);
    void shutdown_background();

    // 手动钩子（测试/运维）：
    // 立即重读 credentials_file（忽略 mtime）；解析失败告警并保留旧表
    void reload_file_now();
    // 立即做一次 .sys 增量同步：新增 AK 拉取入表，消失的动态凭证移除
    Task<void> sync_now();

    // 防 fail-open 的清空保护被触发过（README §1.2）：热加载/同步试图把非空表
    // 清成空表时保留旧表并置位；/-/readyz 据此转 503。成功的下一轮加载会复位
    bool degraded() const { return degraded_.load(std::memory_order_relaxed); }

private:
    CredentialStore() = default;

    void apply_file_credentials(std::vector<CredentialInfo> creds);
    Task<void> file_tick();
    Task<void> sync_tick();
    void schedule_file_reload();
    void schedule_sync();

    std::shared_ptr<storage::IStorageBackend> backend_;
    AuthConfig cfg_;
    std::optional<util::Aes256Key> master_key_;

    mutable std::shared_mutex mu_;
    std::map<std::string, CredentialInfo, std::less<>> creds_;
    // 本实例刚吊销的 AK → 吊销时刻（mu_ 保护）。sync_now 的新增分支跳过近期
    // tombstone：remove 的 delete_object 与 sync 的 list 交错时，list 可能仍见到
    // 已吊销对象，无 tombstone 会把它重新拉回内存复活一个同步周期
    std::map<std::string, std::chrono::steady_clock::time_point, std::less<>> tombstones_;
    std::atomic<bool> degraded_{false};
    std::atomic<bool> sys_bucket_ready_{false};  // 惰性 create_bucket(".sys") 只做一次

    // credentials_file 的 mtime 快照（轮询变更检测；仅后台/手动 reload 线程触碰）
    std::filesystem::file_time_type file_mtime_{};

    std::shared_ptr<ThreadPool> pool_;
    BackgroundTaskGroup bg_{"credential-store"};
    TimerQueue::Id file_timer_ = 0;
    TimerQueue::Id sync_timer_ = 0;
};

}  // namespace lights3::s3
