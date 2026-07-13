// L2 入口：S3Service::dispatch（认证 → 路由 → handler → 错误映射）
#pragma once

#include <string>

#include "core/cancel.h"
#include "core/task.h"
#include "http/model.h"
#include "s3/auth/sigv4.h"
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
    S3Service(storage::BucketRouter router, SigV4Authenticator auth)
        : router_(std::move(router)), auth_(std::move(auth)) {}

    // 顶层入口：内部捕获一切异常并映射为 S3 错误响应，不向 L1 抛出
    Task<http::HttpResponse> dispatch(http::HttpRequest req);

private:
    Task<http::HttpResponse> route(http::HttpRequest& req, std::string bucket, std::string key);

    // handlers/buckets.cc
    Task<http::HttpResponse> list_buckets();
    Task<http::HttpResponse> create_bucket(std::string bucket);
    Task<http::HttpResponse> head_bucket(std::string bucket);
    Task<http::HttpResponse> delete_bucket(std::string bucket);
    // handlers/objects.cc
    Task<http::HttpResponse> put_object(http::HttpRequest& req, std::string bucket,
                                        std::string key);
    Task<http::HttpResponse> get_object(http::HttpRequest& req, std::string bucket,
                                        std::string key, bool head_only);
    Task<http::HttpResponse> delete_object(std::string bucket, std::string key);
    // handlers/list_objects.cc
    Task<http::HttpResponse> list_objects(http::HttpRequest& req, std::string bucket);

    storage::BucketRouter router_;
    SigV4Authenticator auth_;
};

}  // namespace lights3::s3
