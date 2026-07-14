// L2 入口：S3Service::dispatch（认证 → 路由 → handler → 错误映射）
#pragma once

#include <functional>
#include <memory>
#include <string>

#include "core/cancel.h"
#include "core/task.h"
#include "core/thread_pool.h"
#include "http/model.h"
#include "s3/auth/sigv4.h"
#include "s3/metrics.h"
#include "storage/bucket_router.h"

namespace lights3::s3 {

struct RequestContext {
    std::string request_id;
    // 取消信号：客户端断连（driver 发现）、请求超时、进程 shutdown（docs/03 §5）；
    // 默认"永不取消"。长循环（流式读写每块之间）与 pool.schedule() 感知它
    CancelToken cancel;
};

class S3Service {
public:
    // base_domain 非空时启用 virtual-host style 寻址（docs/05 §2）
    S3Service(storage::BucketRouter router, SigV4Authenticator auth,
              std::string base_domain = "")
        : router_(std::move(router)),
          auth_(std::move(auth)),
          base_domain_(std::move(base_domain)) {}

    // 顶层入口：内部捕获一切异常并映射为 S3 错误响应，不向 L1 抛出
    Task<http::HttpResponse> dispatch(http::HttpRequest req);

    // /-/metrics 的线程池指标来源（可选，main 装配时注入）
    void set_pool_stats(std::function<ThreadPool::Stats()> fn) { pool_stats_ = std::move(fn); }

    // 显式分派表（docs/05 §2）：(method, scope, query-flag) → handler，声明序匹配
    enum class Scope { Service, Bucket, Object };
    using Handler = Task<http::HttpResponse> (*)(S3Service&, http::HttpRequest&, std::string,
                                                 std::string);
    struct Route {
        std::string_view method;
        Scope scope;
        std::string_view flag;  // ""=兜底；"k" 按 query 存在匹配；"k=v" 按值匹配
        Handler fn;
    };

private:
    Task<http::HttpResponse> route(http::HttpRequest& req, std::string bucket, std::string key);

    // handlers/buckets.cc
    Task<http::HttpResponse> list_buckets();
    Task<http::HttpResponse> create_bucket(std::string bucket);
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

    // virtual-host style：Host 匹配 *.base_domain 时把 bucket 前置到路径解析
    std::pair<std::string, std::string> resolve_address(const http::HttpRequest& req) const;

    storage::BucketRouter router_;
    SigV4Authenticator auth_;
    std::string base_domain_;
    Metrics metrics_;
    std::function<ThreadPool::Stats()> pool_stats_;
};

}  // namespace lights3::s3
