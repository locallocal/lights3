// L2: ?lifecycle subresource (roadmap §2.4) — minimal subset: Expiration.Days +
// AbortIncompleteMultipartUpload.DaysAfterInitiation, prefix filters only. Root
// (static credential) only: lifecycle rules delete data, an operator decision. Every
// unsupported element of the full AWS surface answers 501 rather than being silently
// dropped (a swallowed Transition would claim archival that never happens).
#include <charconv>

#include "core/log.h"
#include "s3/auth/credential_store.h"
#include "s3/handlers/common.h"
#include "s3/lifecycle.h"
#include "s3/service.h"
#include "s3/xml.h"

namespace lights3::s3 {

namespace {

std::string lifecycle_xml(const std::vector<LifecycleRule>& rules) {
    XmlWriter x;
    x.open("LifecycleConfiguration", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    for (auto& r : rules) {
        x.open("Rule");
        if (!r.id.empty()) x.element("ID", r.id);
        x.open("Filter");
        if (!r.prefix.empty()) x.element("Prefix", r.prefix);
        x.close();
        x.element("Status", r.enabled ? "Enabled" : "Disabled");
        if (r.expiration_days) {
            x.open("Expiration");
            x.element("Days", static_cast<uint64_t>(r.expiration_days));
            x.close();
        }
        if (r.abort_incomplete_days) {
            x.open("AbortIncompleteMultipartUpload");
            x.element("DaysAfterInitiation", static_cast<uint64_t>(r.abort_incomplete_days));
            x.close();
        }
        x.close();
    }
    x.close();
    return x.str();
}

int parse_days(const std::string& v, const char* what) {
    int out = 0;
    auto [p, ec] = std::from_chars(v.data(), v.data() + v.size(), out);
    if (ec != std::errc() || p != v.data() + v.size() || out < 1)
        throw S3Error(S3ErrorCode::MalformedXML,
                      std::string(what) + " must be a positive integer.");
    return out;
}

std::vector<LifecycleRule> parse_lifecycle_xml(const std::string& body) {
    auto root = xml_parse(body);
    if (root.name != "LifecycleConfiguration")
        throw S3Error(S3ErrorCode::MalformedXML,
                      "The XML you provided was not well-formed or did not validate "
                      "against our published schema.");
    std::vector<LifecycleRule> rules;
    for (auto& rn : root.children) {
        if (rn.name != "Rule") continue;
        if (rules.size() >= 1000)
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "The lifecycle configuration may contain at most 1000 rules.");
        // Explicit 501s for the rest of the AWS surface — silently dropping any of
        // these would claim semantics this gateway does not provide
        for (const char* unsupported :
             {"Transition", "NoncurrentVersionExpiration", "NoncurrentVersionTransition"})
            if (rn.find(unsupported))
                throw S3Error(S3ErrorCode::NotImplemented,
                              std::string(unsupported) + " lifecycle rules are not "
                              "implemented (only Expiration.Days and "
                              "AbortIncompleteMultipartUpload).");
        LifecycleRule r;
        r.id = rn.get("ID");
        std::string status = rn.get("Status");
        if (status == "Enabled") r.enabled = true;
        else if (status == "Disabled") r.enabled = false;
        else
            throw S3Error(S3ErrorCode::MalformedXML,
                          "Each Rule requires a Status of Enabled or Disabled.");
        if (auto* f = rn.find("Filter")) {
            for (const char* unsupported :
                 {"Tag", "And", "ObjectSizeGreaterThan", "ObjectSizeLessThan"})
                if (f->find(unsupported))
                    throw S3Error(S3ErrorCode::NotImplemented,
                                  "Lifecycle filters other than Prefix are not implemented.");
            r.prefix = f->get("Prefix");
        } else {
            r.prefix = rn.get("Prefix");  // legacy top-level Prefix form
        }
        if (auto* e = rn.find("Expiration")) {
            for (const char* unsupported : {"Date", "ExpiredObjectDeleteMarker"})
                if (e->find(unsupported))
                    throw S3Error(S3ErrorCode::NotImplemented,
                                  "Only Expiration.Days is implemented.");
            r.expiration_days = parse_days(e->get("Days"), "Expiration.Days");
        }
        if (auto* a = rn.find("AbortIncompleteMultipartUpload"))
            r.abort_incomplete_days = parse_days(a->get("DaysAfterInitiation"),
                                                 "DaysAfterInitiation");
        if (!r.expiration_days && !r.abort_incomplete_days)
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "Each Rule must specify Expiration and/or "
                          "AbortIncompleteMultipartUpload.");
        rules.push_back(std::move(r));
    }
    if (rules.empty())
        throw S3Error(S3ErrorCode::MalformedXML,
                      "The lifecycle configuration must contain at least one Rule.");
    return rules;
}

// Same root gate as ?website/?cors: lifecycle rules delete data
void require_root(const std::shared_ptr<CredentialStore>& store, std::string_view access_key) {
    if (!store || !store->is_root(access_key))
        throw S3Error(S3ErrorCode::AccessDenied,
                      "The lifecycle configuration API requires a root (statically "
                      "configured) credential.");
}

}  // namespace

Task<http::HttpResponse> S3Service::get_bucket_lifecycle(std::string bucket,
                                                         const RequestAuth& auth) {
    require_root(cred_store_, auth.access_key);
    auto snap = lifecycle_store_ ? lifecycle_store_->snapshot() : LifecycleStore::Snapshot{};
    const auto* rules = LifecycleStore::find(snap, bucket);
    if (!rules)
        throw S3Error(S3ErrorCode::NoSuchLifecycleConfiguration,
                      "The lifecycle configuration does not exist", bucket);
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = lifecycle_xml(*rules);
    co_return resp;
}

Task<http::HttpResponse> S3Service::put_bucket_lifecycle(http::HttpRequest& req,
                                                         std::string bucket,
                                                         const RequestAuth& auth) {
    require_root(cred_store_, auth.access_key);
    if (!lifecycle_store_)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "Dynamic lifecycle configuration is not available on this deployment.");
    if (!co_await router_.resolve(bucket).bucket_exists(bucket))
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist", bucket);
    auto body = co_await handlers::read_body(req);
    auto rules = parse_lifecycle_xml(body);
    co_await lifecycle_store_->put(bucket, std::move(rules));
    LOG_INFO("lifecycle: configuration for bucket {} set by {}", bucket,
             std::string(auth.access_key));
    co_return http::HttpResponse{};
}

Task<http::HttpResponse> S3Service::delete_bucket_lifecycle(std::string bucket,
                                                            const RequestAuth& auth) {
    require_root(cred_store_, auth.access_key);
    if (!lifecycle_store_)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "Dynamic lifecycle configuration is not available on this deployment.");
    co_await lifecycle_store_->remove(bucket);
    LOG_INFO("lifecycle: configuration for bucket {} deleted by {}", bucket,
             std::string(auth.access_key));
    http::HttpResponse resp;
    resp.status = 204;
    co_return resp;
}

}  // namespace lights3::s3
