// bucket 级与 service 级 handler
#include "core/util/time.h"
#include "s3/handlers/common.h"
#include "s3/service.h"
#include "s3/xml.h"

namespace lights3::s3 {

Task<http::HttpResponse> S3Service::list_buckets() {
    // 聚合各后端；同名去重（首个后端优先）
    std::vector<storage::BucketInfo> all;
    for (auto& [_, backend] : router_.backends()) {
        auto part = co_await backend->list_buckets();
        for (auto& b : part) {
            // 内部保留名不出现在用户可见的列表里（docs/credential-management.md §4.1）
            if (b.name == storage::kSysBucketName) continue;
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
    w.element("ID", std::string(handlers::kOwnerId));
    w.element("DisplayName", std::string(handlers::kOwnerId));
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

// CreateBucket：解析 CreateBucketConfiguration/LocationConstraint（docs/gaps.md §5.4）。
// 此前请求体从不读，跨 region 的建桶静默成功、随后 GetBucketLocation 回显的却是本地
// region——客户端据此认定数据落在别处。空 body 与空 LocationConstraint 均按 us-east-1
// 处理（S3 惯例：该 region 不写约束）
Task<http::HttpResponse> S3Service::create_bucket(http::HttpRequest& req, std::string bucket) {
    std::string body = co_await handlers::read_body(req);
    if (!body.empty()) {
        XmlNode root = xml_parse(body);
        if (root.name != "CreateBucketConfiguration")
            throw S3Error(S3ErrorCode::MalformedXML,
                          "The XML you provided was not well-formed or did not validate.");
        std::string want = root.get("LocationConstraint");
        const std::string& region = auth_.region();
        // 空约束 = us-east-1；本实现只服务单一 region，不符即拒而非静默改写
        if (want.empty()) want = "us-east-1";
        if (want != region)
            throw S3Error(S3ErrorCode::InvalidLocationConstraint,
                          "The specified location-constraint '" + want +
                              "' is not valid for this endpoint (region '" + region + "').",
                          bucket);
    }
    co_await router_.resolve(bucket).create_bucket(bucket);
    http::HttpResponse resp;
    resp.headers.set("Location", "/" + bucket);
    resp.headers.set("x-amz-bucket-region", auth_.region());
    co_return resp;
}

Task<http::HttpResponse> S3Service::head_bucket(std::string bucket) {
    bool exists = co_await router_.resolve(bucket).bucket_exists(bucket);
    if (!exists)
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist", bucket);
    // boto3 的跨区重定向依赖这个头（docs/gaps.md §5.9）
    http::HttpResponse resp;
    resp.headers.set("x-amz-bucket-region", auth_.region());
    co_return resp;
}

Task<http::HttpResponse> S3Service::delete_bucket(std::string bucket) {
    co_await router_.resolve(bucket).delete_bucket(bucket);
    http::HttpResponse resp;
    resp.status = 204;
    co_return resp;
}

// GetBucketLocation：回显配置 region（docs/s3-protocol.md §1：LocationConstraint 无 region 约束）
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
