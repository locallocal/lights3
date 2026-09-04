// L2: tenant entity + bucket ownership (roadmap §3.9 ③, docs/multi-tenancy.md §4).
// A tenant is a named account that owns buckets and credentials. The model stays
// entirely at L2 — nothing in the storage layer or the meta schema knows tenants:
//   .sys/tenants/<id>   the tenant record (display name, quota, creation time)
//   .sys/owners/<bucket> which tenant owns a bucket (written at CreateBucket by a
//                        tenant credential, or assigned by root through the admin API)
// Both are SysConfigStore instantiations (write-through + tombstone sync). Buckets
// without an owner record are "unowned": visible to root and legacy (tenant-less)
// credentials exactly as before this feature, invisible to tenant credentials.
#pragma once

#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "s3/sys_config_store.h"

namespace lights3::s3 {

// Tenant-level limits: 0 = unlimited. Bytes/objects aggregate over every bucket the
// tenant owns (in-flight multipart bytes included); buckets caps CreateBucket
struct TenantQuota {
    uint64_t max_bytes = 0;
    uint64_t max_objects = 0;
    uint64_t max_buckets = 0;
    bool operator==(const TenantQuota&) const = default;
    bool unlimited() const { return !max_bytes && !max_objects && !max_buckets; }
};

struct Tenant {
    std::string id;            // [a-z0-9][a-z0-9._-]{0,63}; doubles as the .sys object key
    std::string display_name;  // shown as Owner/DisplayName; defaults to id
    std::chrono::system_clock::time_point created;
    TenantQuota quota;
    uint64_t rev = 1;          // edit counter (sync change detection, mirrors credentials)
    bool operator==(const Tenant&) const = default;
};

// Throws S3Error(InvalidRequest) for an ill-formed id; the id becomes an object key
// under .sys and an Owner/ID value, so the charset is deliberately tight
void validate_tenant_id(std::string_view id);

struct TenantTraits {
    using Entry = Tenant;
    static constexpr std::string_view kPrefix = "tenants/";
    static constexpr const char* kName = "tenant";
    static std::string serialize(const Entry& t);
    static std::optional<Entry> deserialize(const std::string& id, const std::string& body);
    static bool differs(const Entry& a, const Entry& b) { return !(a == b); }
};
using TenantStore = SysConfigStore<TenantTraits>;

struct BucketOwner {
    std::string tenant;
    std::string assigned_by;  // access key that created the bucket / assigned the owner
    std::chrono::system_clock::time_point assigned;
    bool operator==(const BucketOwner&) const = default;
};

struct OwnerTraits {
    using Entry = BucketOwner;
    static constexpr std::string_view kPrefix = "owners/";
    static constexpr const char* kName = "bucket-owner";
    static std::string serialize(const Entry& o);
    static std::optional<Entry> deserialize(const std::string& bucket, const std::string& body);
    static bool differs(const Entry& a, const Entry& b) { return !(a == b); }
};
using OwnerStore = SysConfigStore<OwnerTraits>;

// Convenience view over the two stores used by dispatch and the handlers. All
// lookups run on immutable snapshots taken per call; the request path never
// co_awaits with a pointer into a snapshot it did not itself keep alive
class TenantRegistry {
public:
    TenantRegistry(std::shared_ptr<TenantStore> tenants, std::shared_ptr<OwnerStore> owners)
        : tenants_(std::move(tenants)), owners_(std::move(owners)) {}

    // Owner tenant id of a bucket, empty when unowned
    std::string owner_of(const std::string& bucket) const;
    std::optional<Tenant> find(const std::string& id) const;
    std::vector<Tenant> list() const;                              // sorted by id
    std::vector<std::string> buckets_of(const std::string& id) const;  // sorted

    // Ownership mutation (write-through). assign refuses a bucket already owned by
    // another tenant unless force; unassign of an unowned bucket is a no-op
    Task<void> assign(std::string bucket, std::string tenant, std::string by, bool force);
    Task<void> unassign(const std::string& bucket);

    TenantStore& tenants() { return *tenants_; }
    OwnerStore& owners() { return *owners_; }

private:
    std::shared_ptr<TenantStore> tenants_;
    std::shared_ptr<OwnerStore> owners_;
};

}  // namespace lights3::s3
