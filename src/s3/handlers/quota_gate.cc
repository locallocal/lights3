// L2: usage / quota / tenancy helpers shared by the write handlers (roadmap §3.9,
// docs/multi-tenancy.md §2–§4). Kept out of the individual handler files so the
// accounting rules live in one place:
//   - existing_size: the HEAD before a write that tells us what a PUT replaces or a
//     DELETE removes (skipped entirely when accounting is off);
//   - check_quota: bucket limit first, then the owner tenant's aggregate limit —
//     pure in-memory arithmetic on the tracker's counters, decided before any body
//     byte is streamed;
//   - note_usage: the post-commit delta;
//   - require_tenant_bucket: the ownership gate dispatch applies to tenant credentials.
#include "core/log.h"
#include "s3/auth/credential_store.h"
#include "s3/handlers/common.h"
#include "s3/service.h"

namespace lights3::s3 {

bool S3Service::is_root(std::string_view access_key) const {
    return cred_store_ && cred_store_->is_root(access_key);
}

Task<std::optional<uint64_t>> S3Service::existing_size(storage::IStorageBackend& backend,
                                                       const std::string& bucket,
                                                       const std::string& key) {
    if (!usage_ || !usage_->enabled()) co_return std::nullopt;
    try {
        auto meta = co_await backend.head_object(bucket, key);
        co_return meta.size;
    } catch (const S3Error& e) {
        if (e.code == S3ErrorCode::NoSuchKey) co_return std::nullopt;
        throw;
    }
}

void S3Service::note_usage(const std::string& bucket, int64_t d_objects, int64_t d_bytes,
                           int64_t d_mpu_bytes) {
    if (usage_) usage_->apply(bucket, d_objects, d_bytes, d_mpu_bytes);
}

Task<uint64_t> S3Service::upload_parts_bytes(storage::IStorageBackend& backend,
                                             const std::string& bucket, const std::string& key,
                                             const std::string& upload_id) {
    if (!usage_ || !usage_->enabled()) co_return 0;
    storage::ListPartsOptions opt;
    opt.max_parts = storage::kMaxParts;
    auto parts = co_await backend.list_parts(bucket, key, upload_id, opt);
    uint64_t total = 0;
    for (auto& p : parts.parts) total += p.size;
    co_return total;
}

void S3Service::check_quota(const std::string& bucket, int64_t add_bytes, int64_t add_objects,
                            const RequestAuth& auth) {
    if (!usage_ || !usage_->enabled()) return;
    auto reject = [&](bool tenant_scope, const std::string& scope_name, const char* axis,
                      int64_t have, uint64_t limit) {
        usage_->quota_rejected(tenant_scope);
        std::string detail = std::string(axis) + " " + std::to_string(have) + " + request > " +
                             std::to_string(limit) + " (" + scope_name + ")";
        AuditEvent e;
        e.event = "quota.reject";
        e.actor = auth.access_key;
        e.tenant = auth.tenant;
        e.request_id = auth.request_id;
        e.bucket = bucket;
        e.detail = detail;
        audit(e);
        throw S3Error(S3ErrorCode::QuotaExceeded,
                      "The request would exceed the " + scope_name + " quota: " + detail + ".",
                      bucket);
    };
    BucketUsage u = usage_->get(bucket).value_or(BucketUsage{});
    if (quota_store_) {
        auto snap = quota_store_->snapshot();
        if (const auto* q = QuotaStore::find(snap, bucket)) {
            if (q->max_bytes && u.total_bytes() + add_bytes > static_cast<int64_t>(q->max_bytes))
                reject(false, "bucket", "bytes", u.total_bytes(), q->max_bytes);
            if (q->max_objects && u.objects + add_objects > static_cast<int64_t>(q->max_objects))
                reject(false, "bucket", "objects", u.objects, q->max_objects);
        }
    }
    if (tenants_) {
        std::string owner = tenants_->owner_of(bucket);
        if (owner.empty()) return;
        auto t = tenants_->find(owner);
        if (!t || (!t->quota.max_bytes && !t->quota.max_objects)) return;
        BucketUsage tu = usage_->sum(tenants_->buckets_of(owner));
        std::string scope = "tenant '" + owner + "'";
        if (t->quota.max_bytes &&
            tu.total_bytes() + add_bytes > static_cast<int64_t>(t->quota.max_bytes))
            reject(true, scope, "bytes", tu.total_bytes(), t->quota.max_bytes);
        if (t->quota.max_objects &&
            tu.objects + add_objects > static_cast<int64_t>(t->quota.max_objects))
            reject(true, scope, "objects", tu.objects, t->quota.max_objects);
    }
}

Task<void> S3Service::require_tenant_bucket(const std::string& bucket, std::string_view tenant,
                                            bool creating) {
    std::string owner = tenants_->owner_of(bucket);
    if (owner == tenant) co_return;
    if (creating && owner.empty()) co_return;  // create_bucket records ownership on success
    // Unowned or foreign: a missing bucket stays a 404 (SDK existence probes rely on
    // it); anything that exists is invisible to this tenant beyond "forbidden"
    bool exists = co_await router_.resolve(bucket).bucket_exists(bucket);
    if (!exists)
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist", bucket);
    throw S3Error(S3ErrorCode::AccessDenied,
                  "Access denied: bucket is not owned by tenant '" + std::string(tenant) + "'.",
                  bucket);
}

}  // namespace lights3::s3
