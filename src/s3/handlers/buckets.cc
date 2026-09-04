// Bucket-level and service-level handlers
#include "core/log.h"
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
            // Filter by policy (docs/archive/gaps.md §5.10): not filtering was previously a documented trade-off
            // ("only bucket names leak"), but bucket names are exactly step one of an attack chain -- restricted
            // credentials should not see that buckets outside their allowlist exist
            if (auth.policy && !auth.policy->allows_bucket(b.name)) continue;
            // Tenant credentials see only their tenant's buckets (docs/multi-tenancy.md §4.3)
            if (!auth.tenant.empty() && tenants_ && tenants_->owner_of(b.name) != auth.tenant)
                continue;
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
    // Owner: the tenant for tenant credentials, the single legacy identity otherwise
    std::string owner_id(handlers::kOwnerId), owner_name(handlers::kOwnerId);
    if (!auth.tenant.empty() && tenants_) {
        owner_id = std::string(auth.tenant);
        auto t = tenants_->find(owner_id);
        owner_name = t ? t->display_name : owner_id;
    }
    w.open("Owner");
    w.element("ID", owner_id);
    w.element("DisplayName", owner_name);
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

// CreateBucket: parses CreateBucketConfiguration/LocationConstraint (docs/archive/gaps.md §5.4).
// Previously the request body was never read, so cross-region bucket creation silently succeeded while a later
// GetBucketLocation echoed the local region -- leading clients to conclude the data lived elsewhere. Empty body
// and empty LocationConstraint are both treated as us-east-1 (S3 convention: that region writes no constraint)
Task<http::HttpResponse> S3Service::create_bucket(http::HttpRequest& req, std::string bucket,
                                                  const RequestAuth& auth) {
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
    auto& backend = router_.resolve(bucket);
    // Tenant credential (docs/multi-tenancy.md §4.3): the bucket becomes the tenant's.
    // An existing unowned name must not be claimable — dispatch admitted the request
    // because no owner record exists, so the existence check happens here
    if (!auth.tenant.empty() && tenants_) {
        auto t = tenants_->find(std::string(auth.tenant));
        if (!t)
            throw S3Error(S3ErrorCode::AccessDenied,
                          "The credential's tenant no longer exists.", bucket);
        if (t->quota.max_buckets &&
            tenants_->buckets_of(t->id).size() >= t->quota.max_buckets) {
            if (usage_) usage_->quota_rejected(true);
            AuditEvent e;
            e.event = "quota.reject";
            e.actor = auth.access_key;
            e.tenant = auth.tenant;
            e.request_id = auth.request_id;
            e.bucket = bucket;
            e.detail = "buckets " + std::to_string(t->quota.max_buckets) + " reached";
            audit(e);
            throw S3Error(S3ErrorCode::QuotaExceeded,
                          "The tenant has reached its bucket limit (" +
                              std::to_string(t->quota.max_buckets) + ").",
                          bucket);
        }
        if (co_await backend.bucket_exists(bucket))
            throw S3Error(S3ErrorCode::BucketAlreadyExists,
                          "The requested bucket name is not available.", bucket);
    }
    co_await backend.create_bucket(bucket);
    if (!auth.tenant.empty() && tenants_) {
        std::exception_ptr err;
        try {
            co_await tenants_->assign(bucket, std::string(auth.tenant),
                                      std::string(auth.access_key), /*force=*/true);
        } catch (...) {
            err = std::current_exception();
        }
        if (err) {
            // The bucket exists but its ownership could not be recorded: undo, or the
            // tenant would have created a bucket it can never touch again (co_await is
            // not allowed inside a catch handler, hence the exception_ptr detour)
            LOG_ERROR("bucket {}: ownership record failed, rolling back creation", bucket);
            try {
                co_await backend.delete_bucket(bucket);
            } catch (...) {
            }
            std::rethrow_exception(err);
        }
    }
    // A fresh bucket starts with a known-empty counter (listed by the usage API right
    // away; the bootstrap scan still counts it later as it carries no scan stamp)
    note_usage(bucket, 0, 0);
    {
        AuditEvent e;
        e.event = "bucket.create";
        e.actor = auth.access_key;
        e.tenant = auth.tenant;
        e.request_id = auth.request_id;
        e.bucket = bucket;
        audit(e);
    }
    http::HttpResponse resp;
    resp.headers.set("Location", "/" + bucket);
    resp.headers.set("x-amz-bucket-region", auth_.region());
    co_return resp;
}

Task<http::HttpResponse> S3Service::head_bucket(std::string bucket) {
    bool exists = co_await router_.resolve(bucket).bucket_exists(bucket);
    if (!exists)
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist", bucket);
    // boto3's cross-region redirect depends on this header (docs/archive/gaps.md §5.9)
    http::HttpResponse resp;
    resp.headers.set("x-amz-bucket-region", auth_.region());
    co_return resp;
}

Task<http::HttpResponse> S3Service::delete_bucket(std::string bucket,
                                                  const RequestAuth& auth) {
    co_await router_.resolve(bucket).delete_bucket(bucket);
    // Per-bucket records this feature set owns die with the bucket. Failures here
    // only leave a stale record (harmless: an owner entry for a missing bucket is
    // ignored, and a re-created bucket by root gets a fresh assignment if needed)
    auto forget = [&](Task<void> t, const char* what) -> Task<void> {
        try {
            co_await std::move(t);
        } catch (const std::exception& e) {
            LOG_WARN("bucket {}: could not drop {} record: {}", bucket, what, e.what());
        }
    };
    if (tenants_) co_await forget(tenants_->unassign(bucket), "ownership");
    if (quota_store_) co_await forget(quota_store_->remove(bucket), "quota");
    if (usage_) co_await forget(usage_->remove(bucket), "usage");
    {
        AuditEvent e;
        e.event = "bucket.delete";
        e.actor = auth.access_key;
        e.tenant = auth.tenant;
        e.request_id = auth.request_id;
        e.bucket = bucket;
        audit(e);
    }
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
