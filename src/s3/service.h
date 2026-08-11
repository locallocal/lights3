// L2 入口：S3Service::dispatch（认证 → 路由 → handler → 错误映射）
#pragma once

#include <chrono>
#include <span>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "core/cancel.h"
#include "core/metrics.h"
#include "core/semaphore.h"
#include "core/task.h"
#include "core/thread_pool.h"
#include "http/model.h"
#include "s3/auth/policy.h"
#include "s3/auth/sigv4.h"
#include "s3/metrics.h"
#include "storage/bucket_router.h"
#include "storage/multipart.h"

namespace lights3::s3 {

class CredentialStore;  // auth/credential_store.h（仅 admin handler 的 .cc 需要完整定义）

struct RequestContext {
    std::string request_id;
    // x-amz-id-2 / <HostId>（docs/gaps.md §5.9）：AWS 支持工单要的两个 id 之一，
    // 客户端只会转述它看到的这一对，日志侧必须能对上
    std::string host_id;
    // 取消信号：客户端断连（driver 发现）、请求超时、进程 shutdown（docs/concurrency.md §5）；
    // 默认"永不取消"。长循环（流式读写每块之间）与 pool.schedule() 感知它
    CancelToken cancel;
};

class S3Service {
public:
    // base_domain 非空时启用 virtual-host style 寻址（docs/s3-protocol.md §2）
    S3Service(storage::BucketRouter router, SigV4Authenticator auth,
              std::string base_domain = "")
        : router_(std::move(router)),
          auth_(std::move(auth)),
          base_domain_(std::move(base_domain)) {
        // Host 匹配统一按小写进行（resolve_address），配置侧同样归一化
        for (char& c : base_domain_)
            if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    }

    // 顶层入口：内部捕获一切异常并映射为 S3 错误响应，不向 L1 抛出
    Task<http::HttpResponse> dispatch(http::HttpRequest req);

    // /-/metrics 的线程池指标来源（可选，main 装配时注入）
    void set_pool_stats(std::function<ThreadPool::Stats()> fn) { pool_stats_ = std::move(fn); }

    // 入口限流准入快照（docs/gaps.md §7，可选，main 装配时注入）
    void set_admission_stats(std::function<AdmissionStats()> fn) {
        admission_stats_ = std::move(fn);
    }

    // 定时器线程健康度（docs/gaps.md §7，可选）
    void set_timer_stats(std::function<TimerQueue::Stats()> fn) {
        timer_stats_ = std::move(fn);
    }

    // 后端级指标注册表（可选）：渲染追加在 L2 请求指标之后
    void set_backend_metrics(std::shared_ptr<MetricsRegistry> m) {
        backend_metrics_ = std::move(m);
    }

    // 动态凭证管理（docs/credential-management.md）：未注入时 /-/admin/credentials 一律 AccessDenied
    void set_credential_store(std::shared_ptr<CredentialStore> s) {
        cred_store_ = std::move(s);
    }

    // 请求级超时（docs/gaps.md §3.3）：0 = 关闭。到点以协作式取消打断整条 handler
    // 链，挂起点抛 OperationCancelled → 503
    void set_request_timeout(std::chrono::milliseconds t) { request_timeout_ = t; }

    // multipart 最小分片（docs/gaps.md §5.7）：默认 AWS 的 5MiB，0 = 不限制。
    // 做成旋钮而非硬编码，是因为网关前面挂的工具链未必守这条规则（把小分片
    // 上传打回去比让运维改工具更贵），也让"proxy 到另一个 lights3"的部署形态
    // 不至于被两层各判一次
    void set_min_part_size(uint64_t n) { min_part_size_ = n; }
    uint64_t min_part_size() const { return min_part_size_; }

    // 验签结果沿分派链传给 handler（docs/gaps.md §5.10）：ListBuckets 要按 policy
    // 过滤结果，而 policy 此前只活在 dispatch 的局部变量里
    struct RequestAuth {
        std::string_view access_key;             // 认证关闭时为空
        const CredentialPolicy* policy = nullptr;  // nullptr = 无限制
    };

    // 显式分派表（docs/s3-protocol.md §2）：(method, scope, query-flag) → handler，声明序匹配
    enum class Scope { Service, Bucket, Object };
    using Handler = Task<http::HttpResponse> (*)(S3Service&, http::HttpRequest&, std::string,
                                                 std::string, const RequestAuth&);
    struct Route {
        std::string_view method;
        Scope scope;
        std::string_view flag;  // ""=兜底；"k" 按 query 存在匹配；"k=v" 按值匹配
        // query 白名单（docs/gaps.md §3.5）：本路由额外允许的 query key（空格分隔）。
        // flag 键与 presigned 签名参数天然允许；出现名单外的 key → 501。
        // 黑名单兜底的结构性问题是任何遗漏都静默降级成"读/写整对象"——
        // ?attributes 回整个对象体、?partNumber 回整个对象、response-* 被吞
        std::string_view extra_query;
        // 本路由对应的动作（docs/gaps.md §5.10）：授权按它判定，而不是按 HTTP
        // 方法猜——DeleteObjects 是 POST 却显然是删除，CreateMultipartUpload 同为
        // POST 却是写入，方法维度根本分不开这两件事
        Action action;
        Handler fn;
    };

    // 分派表匹配（授权需在调用 handler 之前就知道动作，故与 route 分开）
    static std::span<const Route> route_table();
    const Route* match_route(const http::HttpRequest& req, Scope scope) const;

private:
    Task<http::HttpResponse> route(http::HttpRequest& req, std::string bucket, std::string key,
                                   const RequestAuth& auth);

    // handlers/buckets.cc
    Task<http::HttpResponse> list_buckets(const RequestAuth& auth);
    Task<http::HttpResponse> create_bucket(http::HttpRequest& req, std::string bucket);
    Task<http::HttpResponse> head_bucket(std::string bucket);
    Task<http::HttpResponse> delete_bucket(std::string bucket);
    Task<http::HttpResponse> get_bucket_location(std::string bucket);
    // handlers/objects.cc
    Task<http::HttpResponse> put_object(http::HttpRequest& req, std::string bucket,
                                        std::string key);
    Task<http::HttpResponse> copy_object(http::HttpRequest& req, std::string bucket,
                                         std::string key);
    Task<http::HttpResponse> get_object(http::HttpRequest& req, std::string bucket,
                                        std::string key, bool head_only);
    Task<http::HttpResponse> delete_object(std::string bucket, std::string key);
    Task<http::HttpResponse> delete_objects(http::HttpRequest& req, std::string bucket);
    // handlers/list_objects.cc
    Task<http::HttpResponse> list_objects(http::HttpRequest& req, std::string bucket);
    // handlers/multipart.cc
    Task<http::HttpResponse> create_multipart(http::HttpRequest& req, std::string bucket,
                                              std::string key);
    Task<http::HttpResponse> upload_part(http::HttpRequest& req, std::string bucket,
                                         std::string key);
    Task<http::HttpResponse> complete_multipart(http::HttpRequest& req, std::string bucket,
                                                std::string key);
    Task<http::HttpResponse> abort_multipart(http::HttpRequest& req, std::string bucket,
                                             std::string key);
    Task<http::HttpResponse> list_parts(http::HttpRequest& req, std::string bucket,
                                        std::string key);
    Task<http::HttpResponse> list_multipart_uploads(http::HttpRequest& req, std::string bucket);

    Task<http::HttpResponse> readyz();

    // handlers/admin_credentials.cc（docs/credential-management.md §2）：内部完成验签与 root 判定，
    // 错误渲染成 JSON 体；access_key 出参供访问日志
    Task<http::HttpResponse> admin_credentials(http::HttpRequest& req,
                                               std::string& access_key);

    // virtual-host style：Host 匹配 *.base_domain 时把 bucket 前置到路径解析。
    // vhost 标记供内部端点分流用（docs/gaps.md §3.8）：vhost 下 req.path 是 key，
    // "/-/metrics" 可能是 mybucket 里的合法对象键，不得被内部端点遮蔽
    struct Address {
        std::string bucket, key;
        bool vhost = false;
    };
    Address resolve_address(const http::HttpRequest& req) const;

    storage::BucketRouter router_;
    SigV4Authenticator auth_;
    std::string base_domain_;
    Metrics metrics_;
    std::function<ThreadPool::Stats()> pool_stats_;
    std::function<AdmissionStats()> admission_stats_;
    std::function<TimerQueue::Stats()> timer_stats_;
    std::chrono::milliseconds request_timeout_{0};
    uint64_t min_part_size_ = storage::kMinPartSize;
    std::shared_ptr<MetricsRegistry> backend_metrics_;
    std::shared_ptr<CredentialStore> cred_store_;

    // /-/readyz 结果短缓存（匿名可达，探测对每个后端发真实调用：不加缓存
    // 可被匿名循环放大成对上游的计费/限流调用）
    std::mutex readyz_mu_;
    std::chrono::steady_clock::time_point readyz_at_{};
    int readyz_status_ = 0;  // 0 = 尚无结果
    std::string readyz_body_;
    bool readyz_inflight_ = false;
};

}  // namespace lights3::s3
