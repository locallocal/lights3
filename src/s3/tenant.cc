#include "s3/tenant.h"

#include <algorithm>
#include <nlohmann/json.hpp>

#include "core/util/time.h"
#include "s3/errors.h"

namespace lights3::s3 {

using nlohmann::json;

void validate_tenant_id(std::string_view id) {
    auto bad = [&](const char* why) {
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "Invalid tenant id '" + std::string(id) + "': " + why);
    };
    if (id.empty() || id.size() > 64) bad("must be 1-64 characters");
    auto lower_alnum = [](char c) { return (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'); };
    if (!lower_alnum(id.front())) bad("must start with a lowercase letter or digit");
    for (char c : id)
        if (!lower_alnum(c) && c != '-' && c != '_' && c != '.')
            bad("allowed characters are a-z 0-9 . _ -");
    if (id == "." || id == "..") bad("reserved");
}

namespace {

int64_t to_unix(std::chrono::system_clock::time_point t) {
    return std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count();
}
std::chrono::system_clock::time_point from_unix(int64_t s) {
    return std::chrono::system_clock::time_point(std::chrono::seconds(s));
}

}  // namespace

std::string TenantTraits::serialize(const Entry& t) {
    json j;
    j["id"] = t.id;
    j["display_name"] = t.display_name;
    j["created"] = util::iso8601(t.created);
    j["created_unix"] = to_unix(t.created);
    j["rev"] = t.rev;
    json q = json::object();
    if (t.quota.max_bytes) q["max_bytes"] = t.quota.max_bytes;
    if (t.quota.max_objects) q["max_objects"] = t.quota.max_objects;
    if (t.quota.max_buckets) q["max_buckets"] = t.quota.max_buckets;
    j["quota"] = std::move(q);
    return j.dump(2) + "\n";
}

std::optional<Tenant> TenantTraits::deserialize(const std::string& id, const std::string& body) {
    try {
        json j = json::parse(body);
        Tenant t;
        t.id = id;
        // The key is authoritative; a body naming another id is a corrupt object
        if (j.value("id", id) != id) return std::nullopt;
        t.display_name = j.value("display_name", id);
        t.created = from_unix(j.value("created_unix", int64_t{0}));
        t.rev = j.value("rev", uint64_t{1});
        if (auto it = j.find("quota"); it != j.end() && it->is_object()) {
            t.quota.max_bytes = it->value("max_bytes", uint64_t{0});
            t.quota.max_objects = it->value("max_objects", uint64_t{0});
            t.quota.max_buckets = it->value("max_buckets", uint64_t{0});
        }
        return t;
    } catch (const json::exception&) {
        return std::nullopt;
    }
}

std::string OwnerTraits::serialize(const Entry& o) {
    json j;
    j["tenant"] = o.tenant;
    j["assigned_by"] = o.assigned_by;
    j["assigned"] = util::iso8601(o.assigned);
    j["assigned_unix"] = to_unix(o.assigned);
    return j.dump(2) + "\n";
}

std::optional<BucketOwner> OwnerTraits::deserialize(const std::string&, const std::string& body) {
    try {
        json j = json::parse(body);
        BucketOwner o;
        o.tenant = j.at("tenant").get<std::string>();
        if (o.tenant.empty()) return std::nullopt;
        o.assigned_by = j.value("assigned_by", "");
        o.assigned = from_unix(j.value("assigned_unix", int64_t{0}));
        return o;
    } catch (const json::exception&) {
        return std::nullopt;
    }
}

// ---------- TenantRegistry ----------

std::string TenantRegistry::owner_of(const std::string& bucket) const {
    auto snap = owners_->snapshot();
    const auto* o = OwnerStore::find(snap, bucket);
    return o ? o->tenant : std::string();
}

std::optional<Tenant> TenantRegistry::find(const std::string& id) const {
    auto snap = tenants_->snapshot();
    const auto* t = TenantStore::find(snap, id);
    if (!t) return std::nullopt;
    return *t;
}

std::vector<Tenant> TenantRegistry::list() const {
    std::vector<Tenant> out;
    auto snap = tenants_->snapshot();
    if (snap)
        for (auto& [_, t] : *snap) out.push_back(t);  // std::map: already sorted by id
    return out;
}

std::vector<std::string> TenantRegistry::buckets_of(const std::string& id) const {
    std::vector<std::string> out;
    auto snap = owners_->snapshot();
    if (snap)
        for (auto& [bucket, o] : *snap)
            if (o.tenant == id) out.push_back(bucket);
    return out;
}

Task<void> TenantRegistry::assign(std::string bucket, std::string tenant, std::string by,
                                  bool force) {
    if (!force) {
        std::string cur = owner_of(bucket);
        if (!cur.empty() && cur != tenant)
            throw S3Error(S3ErrorCode::BucketAlreadyExists,
                          "Bucket " + bucket + " is owned by tenant '" + cur + "'.", bucket);
    }
    BucketOwner o;
    o.tenant = std::move(tenant);
    o.assigned_by = std::move(by);
    o.assigned = std::chrono::system_clock::now();
    co_await owners_->put(std::move(bucket), std::move(o));
}

Task<void> TenantRegistry::unassign(const std::string& bucket) {
    co_await owners_->remove(bucket);
}

}  // namespace lights3::s3
