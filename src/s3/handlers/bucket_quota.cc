// L2: ?quota subresource (roadmap §3.9 ②, docs/multi-tenancy.md §3). No AWS
// equivalent — the XML shape is this gateway's own. GET is open to any credential
// dispatch admitted to the bucket (a tenant may read its own limit); PUT/DELETE are
// operator decisions and stay root-only like ?cors / ?lifecycle / ?website.
#include "core/log.h"
#include "s3/auth/credential_store.h"
#include "s3/handlers/common.h"
#include "s3/quota.h"
#include "s3/service.h"

namespace lights3::s3 {

namespace {

void require_root_for_quota(bool root) {
    if (!root)
        throw S3Error(S3ErrorCode::AccessDenied,
                      "Setting a bucket quota requires a root (statically configured) "
                      "credential.");
}

}  // namespace

Task<http::HttpResponse> S3Service::get_bucket_quota(std::string bucket,
                                                     const RequestAuth& auth) {
    (void)auth;
    auto snap = quota_store_ ? quota_store_->snapshot() : QuotaStore::Snapshot{};
    const auto* q = QuotaStore::find(snap, bucket);
    if (!q)
        throw S3Error(S3ErrorCode::NoSuchQuotaConfiguration,
                      "The bucket has no quota configuration", bucket);
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = quota_xml(*q);
    co_return resp;
}

Task<http::HttpResponse> S3Service::put_bucket_quota(http::HttpRequest& req, std::string bucket,
                                                     const RequestAuth& auth) {
    require_root_for_quota(is_root(auth.access_key));
    if (!quota_store_)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "Bucket quotas are not available on this deployment.");
    if (!usage_ || !usage_->enabled())
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "Bucket quotas require usage accounting (usage.enabled).");
    if (!co_await router_.resolve(bucket).bucket_exists(bucket))
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist", bucket);
    auto body = co_await handlers::read_body(req);
    BucketQuota q = parse_quota_xml(body);
    co_await quota_store_->put(bucket, q);
    std::string detail = "max_bytes=" + std::to_string(q.max_bytes) +
                         " max_objects=" + std::to_string(q.max_objects);
    LOG_INFO("quota: bucket {} set by {}: {}", bucket, std::string(auth.access_key), detail);
    AuditEvent e;
    e.event = "quota.set";
    e.actor = auth.access_key;
    e.request_id = auth.request_id;
    e.bucket = bucket;
    e.detail = detail;
    audit(e);
    co_return http::HttpResponse{};
}

Task<http::HttpResponse> S3Service::delete_bucket_quota(std::string bucket,
                                                        const RequestAuth& auth) {
    require_root_for_quota(is_root(auth.access_key));
    if (!quota_store_)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "Bucket quotas are not available on this deployment.");
    co_await quota_store_->remove(bucket);
    LOG_INFO("quota: bucket {} cleared by {}", bucket, std::string(auth.access_key));
    AuditEvent e;
    e.event = "quota.clear";
    e.actor = auth.access_key;
    e.request_id = auth.request_id;
    e.bucket = bucket;
    audit(e);
    http::HttpResponse resp;
    resp.status = 204;
    co_return resp;
}

}  // namespace lights3::s3
