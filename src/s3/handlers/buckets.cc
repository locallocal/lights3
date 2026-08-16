// Bucket-level and service-level handlers
#include "core/util/time.h"
#include "s3/handlers/common.h"
#include "s3/service.h"
#include "s3/xml.h"

namespace lights3::s3 {

Task<http::HttpResponse> S3Service::list_buckets(const RequestAuth& auth) {
    // Aggregate across backends; deduplicate by name (first backend wins)
    std::vector<storage::BucketInfo> all;
    for (auto& [_, backend] : router_.backends()) {
        auto part = co_await backend->list_buckets();
        for (auto& b : part) {
            // Internal reserved names never appear in the user-visible list (docs/credential-management.md §4.1)
            if (b.name == storage::kSysBucketName) continue;
            // Filter by policy (docs/gaps.md §5.10): not filtering was previously a documented trade-off
            // ("only bucket names leak"), but bucket names are exactly step one of an attack chain -- restricted
            // credentials should not see that buckets outside their allowlist exist
            if (auth.policy && !auth.policy->allows_bucket(b.name)) continue;
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

// CreateBucket: parses CreateBucketConfiguration/LocationConstraint (docs/gaps.md §5.4).
// Previously the request body was never read, so cross-region bucket creation silently succeeded while a later
// GetBucketLocation echoed the local region -- leading clients to conclude the data lived elsewhere. Empty body
// and empty LocationConstraint are both treated as us-east-1 (S3 convention: that region writes no constraint)
Task<http::HttpResponse> S3Service::create_bucket(http::HttpRequest& req, std::string bucket) {
    std::string body = co_await handlers::read_body(req);
    if (!body.empty()) {
        XmlNode root = xml_parse(body);
        if (root.name != "CreateBucketConfiguration")
            throw S3Error(S3ErrorCode::MalformedXML,
                          "The XML you provided was not well-formed or did not validate.");
        std::string want = root.get("LocationConstraint");
        const std::string& region = auth_.region();
        // Empty constraint = us-east-1; this implementation serves a single region, mismatches are rejected rather than silently rewritten
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
    // boto3's cross-region redirect depends on this header (docs/gaps.md §5.9)
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

// GetBucketLocation: echoes the configured region (docs/s3-protocol.md §1: LocationConstraint carries no region constraint)
Task<http::HttpResponse> S3Service::get_bucket_location(std::string bucket) {
    bool exists = co_await router_.resolve(bucket).bucket_exists(bucket);
    if (!exists)
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist", bucket);
    XmlWriter w;
    // us-east-1 returns an empty LocationConstraint per S3 convention
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
