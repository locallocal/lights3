// bucket 级与 service 级 handler
#include "core/util/time.h"
#include "s3/service.h"
#include "s3/xml.h"

namespace lights3::s3 {

Task<http::HttpResponse> S3Service::list_buckets() {
    // 聚合各后端；同名去重（首个后端优先）
    std::vector<storage::BucketInfo> all;
    for (auto& [_, backend] : router_.backends()) {
        auto part = co_await backend->list_buckets();
        for (auto& b : part) {
            if (!b.name.empty() && b.name.front() == '.') continue;  // 内部保留（docs/06 §4.1）
            bool dup = false;
            for (auto& e : all)
                if (e.name == b.name) dup = true;
            if (!dup) all.push_back(b);
        }
    }
    std::sort(all.begin(), all.end(),
              [](const auto& a, const auto& b) { return a.name < b.name; });

    XmlWriter w;
    w.open("ListAllMyBucketsResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    w.open("Owner");
    w.element("ID", "lights3");
    w.element("DisplayName", "lights3");
    w.close();
    w.open("Buckets");
    for (auto& b : all) {
        w.open("Bucket");
        w.element("Name", b.name);
        w.element("CreationDate", util::iso8601(b.created));
        w.close();
    }
    w.close();
    w.close();

    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = w.str();
    co_return resp;
}

Task<http::HttpResponse> S3Service::create_bucket(std::string bucket) {
    co_await router_.resolve(bucket).create_bucket(bucket);
    http::HttpResponse resp;
    resp.headers.set("Location", "/" + bucket);
    co_return resp;
}

Task<http::HttpResponse> S3Service::head_bucket(std::string bucket) {
    bool exists = co_await router_.resolve(bucket).bucket_exists(bucket);
    if (!exists)
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist", bucket);
    co_return http::HttpResponse{};
}

Task<http::HttpResponse> S3Service::delete_bucket(std::string bucket) {
    co_await router_.resolve(bucket).delete_bucket(bucket);
    http::HttpResponse resp;
    resp.status = 204;
    co_return resp;
}

// GetBucketLocation：回显配置 region（docs/05 §1：LocationConstraint 无 region 约束）
Task<http::HttpResponse> S3Service::get_bucket_location(std::string bucket) {
    bool exists = co_await router_.resolve(bucket).bucket_exists(bucket);
    if (!exists)
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist", bucket);
    XmlWriter w;
    // us-east-1 按 S3 惯例返回空 LocationConstraint
    const std::string& region = auth_.region();
    w.open("LocationConstraint", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    if (region != "us-east-1") w.text(region);
    w.close();
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = w.str();
    co_return resp;
}

}  // namespace lights3::s3
