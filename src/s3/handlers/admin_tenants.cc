// L2: /-/admin/tenants and /-/admin/usage (roadmap §3.9 ①③, docs/multi-tenancy.md
// §6). Tiered admin model:
//   root (static credential)            everything
//   tenant admin (role=admin)           read its own tenant, its buckets' usage,
//                                       trigger rescans of its own buckets
//   anyone else                         AccessDenied
// Same JSON conventions as /-/admin/credentials; errors never reach the XML path.
#include "core/log.h"
#include "core/util/time.h"
#include "s3/auth/credential_store.h"
#include "s3/handlers/admin_json.h"
#include "s3/service.h"

namespace lights3::s3 {

using namespace handlers;
using nlohmann::json;

namespace {

json quota_json(const TenantQuota& q) {
    json j = json::object();
    j["max_bytes"] = q.max_bytes;
    j["max_objects"] = q.max_objects;
    j["max_buckets"] = q.max_buckets;
    return j;
}

json usage_json(const BucketUsage& u) {
    json j;
    j["objects"] = u.objects;
    j["bytes"] = u.bytes;
    j["mpu_bytes"] = u.mpu_bytes;
    j["scanned"] = u.scanned();
    if (u.scanned()) j["scanned_at"] = util::iso8601(u.scanned_at);
    return j;
}

// {"max_bytes"?, "max_objects"?, "max_buckets"?}; unknown fields are refused
TenantQuota parse_tenant_quota(const json& j, TenantQuota base) {
    if (!j.is_object())
        throw S3Error(S3ErrorCode::InvalidRequest, "quota must be a JSON object.");
    for (auto& [k, v] : j.items()) {
        if (k == "max_bytes") base.max_bytes = json_u64(v, "quota.max_bytes");
        else if (k == "max_objects") base.max_objects = json_u64(v, "quota.max_objects");
        else if (k == "max_buckets") base.max_buckets = json_u64(v, "quota.max_buckets");
        else throw S3Error(S3ErrorCode::InvalidRequest, "unknown quota field '" + k + "'.");
    }
    return base;
}

// Splits "/-/admin/<group>/a/b/c" into ["a","b","c"]; empty segments are malformed
std::vector<std::string> path_segments(const std::string& path, std::string_view base) {
    std::vector<std::string> out;
    std::string rest = path.substr(base.size());
    if (rest.empty()) return out;
    if (rest.front() != '/') throw S3Error(S3ErrorCode::InvalidRequest, "Malformed admin API path.");
    rest.erase(0, 1);
    size_t pos = 0;
    while (pos <= rest.size()) {
        size_t slash = rest.find('/', pos);
        if (slash == std::string::npos) slash = rest.size();
        std::string seg = rest.substr(pos, slash - pos);
        if (seg.empty()) throw S3Error(S3ErrorCode::InvalidRequest, "Malformed admin API path.");
        out.push_back(std::move(seg));
        pos = slash + 1;
    }
    return out;
}

}  // namespace

Task<http::HttpResponse> S3Service::admin_tenancy(http::HttpRequest& req,
                                                  std::string& access_key,
                                                  const RequestContext& ctx) {
    try {
        auto ident = auth_.verify(req);
        access_key = ident.access_key;
        bool root = is_root(access_key);
        // Tenant admins reach this plane for their own tenant only; sessions never do
        // (mint_session drops the admin role, and root is decided on the live table)
        if (!root && !ident.tenant_admin)
            throw S3Error(S3ErrorCode::AccessDenied,
                          "Admin API requires a root credential or a tenant admin.");
        if (!tenants_ || !usage_)
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "Tenancy and usage accounting are not available on this deployment.");
        const std::string& own = ident.tenant;  // empty for root
        auto require_root = [&](const char* what) {
            if (!root)
                throw S3Error(S3ErrorCode::AccessDenied,
                              std::string(what) + " requires a root credential.");
        };
        auto may_see_tenant = [&](const std::string& id) { return root || id == own; };
        auto may_see_bucket = [&](const std::string& bucket) {
            return root || tenants_->owner_of(bucket) == own;
        };
        auto tenant_json = [&](const Tenant& t) {
            json j;
            j["id"] = t.id;
            j["display_name"] = t.display_name;
            j["created_at"] = util::iso8601(t.created);
            j["rev"] = t.rev;
            j["quota"] = quota_json(t.quota);
            auto buckets = tenants_->buckets_of(t.id);
            j["buckets"] = buckets;
            j["usage"] = usage_json(usage_->sum(buckets));
            j["credentials"] = cred_store_ ? cred_store_->list_tenant(t.id).size() : 0;
            return j;
        };
        auto bucket_usage_json = [&](const std::string& bucket) {
            json j = usage_json(usage_->get(bucket).value_or(BucketUsage{}));
            j["bucket"] = bucket;
            std::string owner = tenants_->owner_of(bucket);
            if (!owner.empty()) j["tenant"] = owner;
            if (quota_store_) {
                auto snap = quota_store_->snapshot();
                if (const auto* q = QuotaStore::find(snap, bucket)) {
                    j["quota"] = {{"max_bytes", q->max_bytes}, {"max_objects", q->max_objects}};
                }
            }
            return j;
        };
        auto audit_event = [&](std::string_view event, std::string_view target,
                               std::string_view bucket, std::string detail) {
            AuditEvent e;
            e.event = event;
            e.actor = access_key;
            e.tenant = own;
            e.request_id = ctx.request_id;
            e.target = target;
            e.bucket = bucket;
            e.detail = detail;
            audit(e);
        };

        // ---------- /-/admin/tenants ----------
        if (req.path.rfind("/-/admin/tenants", 0) == 0) {
            auto seg = path_segments(req.path, "/-/admin/tenants");
            if (seg.empty() && req.method == "POST") {
                require_root("Creating a tenant");
                json body = co_await read_json_object(req, /*allow_empty=*/false);
                Tenant t;
                for (auto& [k, v] : body.items()) {
                    if (k == "id") {
                        if (!v.is_string())
                            throw S3Error(S3ErrorCode::InvalidRequest, "id must be a string.");
                        t.id = v.get<std::string>();
                    } else if (k == "display_name") {
                        if (!v.is_string())
                            throw S3Error(S3ErrorCode::InvalidRequest,
                                          "display_name must be a string.");
                        t.display_name = v.get<std::string>();
                    } else if (k == "quota") {
                        t.quota = parse_tenant_quota(v, {});
                    } else {
                        throw S3Error(S3ErrorCode::InvalidRequest, "unknown field '" + k + "'.");
                    }
                }
                validate_tenant_id(t.id);
                if (t.display_name.empty()) t.display_name = t.id;
                if (t.display_name.size() > 256)
                    throw S3Error(S3ErrorCode::InvalidRequest, "display_name too long.");
                if (tenants_->find(t.id))
                    throw S3Error(S3ErrorCode::TenantAlreadyExists,
                                  "Tenant '" + t.id + "' already exists.");
                t.created = std::chrono::system_clock::now();
                co_await tenants_->tenants().put(t.id, t);
                LOG_INFO("tenant: {} created by {}", t.id, access_key);
                audit_event("tenant.create", t.id, "", "display_name=" + t.display_name);
                co_return json_response(201, tenant_json(t));
            }
            if (seg.empty() && req.method == "GET") {
                json j;
                j["tenants"] = json::array();
                for (auto& t : tenants_->list())
                    if (may_see_tenant(t.id)) j["tenants"].push_back(tenant_json(t));
                co_return json_response(200, j);
            }
            if (seg.size() == 1) {
                const std::string& id = seg[0];
                if (!may_see_tenant(id))
                    throw S3Error(S3ErrorCode::AccessDenied,
                                  "Tenant admins may only access their own tenant.");
                auto t = tenants_->find(id);
                if (!t)
                    throw S3Error(S3ErrorCode::NoSuchTenant,
                                  "The specified tenant does not exist.");
                if (req.method == "GET") co_return json_response(200, tenant_json(*t));
                if (req.method == "PUT") {
                    require_root("Updating a tenant");
                    json body = co_await read_json_object(req, /*allow_empty=*/false);
                    for (auto& [k, v] : body.items()) {
                        if (k == "display_name") {
                            if (!v.is_string() || v.get<std::string>().empty() ||
                                v.get<std::string>().size() > 256)
                                throw S3Error(S3ErrorCode::InvalidRequest,
                                              "display_name must be a non-empty string.");
                            t->display_name = v.get<std::string>();
                        } else if (k == "quota") {
                            // Replace semantics: axes absent from the body are cleared,
                            // so a PUT is the complete new quota, never a merge
                            t->quota = parse_tenant_quota(v, {});
                        } else {
                            throw S3Error(S3ErrorCode::InvalidRequest,
                                          "unknown field '" + k + "'.");
                        }
                    }
                    ++t->rev;
                    co_await tenants_->tenants().put(id, *t);
                    LOG_INFO("tenant: {} updated by {} (rev {})", id, access_key, t->rev);
                    audit_event("tenant.update", id, "", body.dump());
                    co_return json_response(200, tenant_json(*t));
                }
                if (req.method == "DELETE") {
                    require_root("Deleting a tenant");
                    auto buckets = tenants_->buckets_of(id);
                    if (!buckets.empty())
                        throw S3Error(S3ErrorCode::TenantNotEmpty,
                                      "Tenant '" + id + "' still owns " +
                                          std::to_string(buckets.size()) +
                                          " bucket(s); reassign or delete them first.");
                    if (cred_store_ && !cred_store_->list_tenant(id).empty())
                        throw S3Error(S3ErrorCode::TenantNotEmpty,
                                      "Tenant '" + id +
                                          "' still has credentials; revoke them first.");
                    co_await tenants_->tenants().remove(id);
                    LOG_INFO("tenant: {} deleted by {}", id, access_key);
                    audit_event("tenant.delete", id, "", "");
                    http::HttpResponse resp;
                    resp.status = 204;
                    co_return resp;
                }
            }
            if (seg.size() == 3 && seg[1] == "buckets") {
                require_root("Changing bucket ownership");
                const std::string& id = seg[0];
                const std::string& bucket = seg[2];
                storage::validate_bucket_name(bucket);
                if (!tenants_->find(id))
                    throw S3Error(S3ErrorCode::NoSuchTenant,
                                  "The specified tenant does not exist.");
                if (req.method == "PUT") {
                    if (!co_await router_.resolve(bucket).bucket_exists(bucket))
                        throw S3Error(S3ErrorCode::NoSuchBucket,
                                      "The specified bucket does not exist", bucket);
                    bool force = req.query_get("force").value_or("") == "true";
                    co_await tenants_->assign(bucket, id, access_key, force);
                    LOG_INFO("tenant: bucket {} assigned to {} by {}", bucket, id, access_key);
                    audit_event("tenant.assign_bucket", id, bucket, force ? "force" : "");
                    json j;
                    j["bucket"] = bucket;
                    j["tenant"] = id;
                    co_return json_response(200, j);
                }
                if (req.method == "DELETE") {
                    if (tenants_->owner_of(bucket) != id)
                        throw S3Error(S3ErrorCode::InvalidRequest,
                                      "Bucket " + bucket + " is not owned by tenant '" + id +
                                          "'.");
                    co_await tenants_->unassign(bucket);
                    LOG_INFO("tenant: bucket {} detached from {} by {}", bucket, id,
                             access_key);
                    audit_event("tenant.unassign_bucket", id, bucket, "");
                    http::HttpResponse resp;
                    resp.status = 204;
                    co_return resp;
                }
            }
            throw S3Error(S3ErrorCode::MethodNotAllowed,
                          "The specified method is not allowed against this resource.");
        }

        // ---------- /-/admin/usage ----------
        auto seg = path_segments(req.path, "/-/admin/usage");
        if (seg.empty() && req.method == "GET") {
            std::string filter = req.query_get("tenant").value_or("");
            if (!root) filter = own;
            json j;
            j["buckets"] = json::array();
            for (auto& [bucket, u] : usage_->all()) {
                (void)u;
                if (!filter.empty() && tenants_->owner_of(bucket) != filter) continue;
                j["buckets"].push_back(bucket_usage_json(bucket));
            }
            co_return json_response(200, j);
        }
        if (!seg.empty()) {
            const std::string& bucket = seg[0];
            storage::validate_bucket_name(bucket);
            if (!may_see_bucket(bucket))
                throw S3Error(S3ErrorCode::AccessDenied,
                              "Tenant admins may only inspect their own buckets.");
            if (seg.size() == 1 && req.method == "GET") {
                if (!usage_->get(bucket) &&
                    !co_await router_.resolve(bucket).bucket_exists(bucket))
                    throw S3Error(S3ErrorCode::NoSuchBucket,
                                  "The specified bucket does not exist", bucket);
                co_return json_response(200, bucket_usage_json(bucket));
            }
            if (seg.size() == 2 && seg[1] == "rescan" && req.method == "POST") {
                if (!usage_->enabled())
                    throw S3Error(S3ErrorCode::InvalidRequest,
                                  "Usage accounting is disabled (usage.enabled).");
                if (!co_await router_.resolve(bucket).bucket_exists(bucket))
                    throw S3Error(S3ErrorCode::NoSuchBucket,
                                  "The specified bucket does not exist", bucket);
                auto u = co_await usage_->rescan(bucket);
                audit_event("usage.rescan", "", bucket,
                            "objects=" + std::to_string(u.objects) +
                                " bytes=" + std::to_string(u.bytes));
                co_return json_response(200, bucket_usage_json(bucket));
            }
        }
        throw S3Error(S3ErrorCode::MethodNotAllowed,
                      "The specified method is not allowed against this resource.");
    } catch (const S3Error& e) {
        metrics_.s3_error(e.code);
        co_return admin_error(e, req);
    } catch (const std::exception& e) {
        LOG_ERROR("admin api {} {} internal error: {}", req.method, req.path, e.what());
        metrics_.s3_error(S3ErrorCode::InternalError);
        co_return admin_error(S3Error(S3ErrorCode::InternalError, e.what()), req);
    }
}

}  // namespace lights3::s3
