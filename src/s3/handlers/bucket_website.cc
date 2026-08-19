// L2: ?website subresource — Get/Put/DeleteBucketWebsite (docs/static-website.md
// phase ③). Root (static credential) only, same two-tier model as the admin plane:
// website configuration makes a bucket anonymously readable, which is an operator
// decision, not a tenant one. Dynamic entries persist to .sys/website/<bucket>
// (WebsiteStore); statically configured buckets refuse mutation (config file owns them).
#include "core/log.h"
#include "s3/auth/credential_store.h"
#include "s3/handlers/common.h"
#include "s3/service.h"
#include "s3/website_store.h"
#include "s3/xml.h"

namespace lights3::s3 {

namespace {

// AWS XML shape: <WebsiteConfiguration><IndexDocument><Suffix>…</Suffix></IndexDocument>
// [<ErrorDocument><Key>…</Key></ErrorDocument>]</WebsiteConfiguration>
std::string website_xml(const WebsiteBucket& w) {
    XmlWriter x;
    x.open("WebsiteConfiguration", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    x.open("IndexDocument");
    x.element("Suffix", w.index_suffix);
    x.close();
    if (!w.error_key.empty()) {
        x.open("ErrorDocument");
        x.element("Key", w.error_key);
        x.close();
    }
    x.close();
    return x.str();
}

// Same shape rules as the YAML config side (config.cc): one validation story
// regardless of which plane the entry came through
WebsiteBucket parse_website_xml(const std::string& body, std::string bucket) {
    auto root = xml_parse(body);
    if (root.name != "WebsiteConfiguration")
        throw S3Error(S3ErrorCode::MalformedXML,
                      "The XML you provided was not well-formed or did not validate "
                      "against our published schema.");
    // Silently accepting these would break the site's routing expectations — refuse
    // loudly (same philosophy as the header/subresource 501 lists)
    if (root.find("RoutingRules") || root.find("RedirectAllRequestsTo"))
        throw S3Error(S3ErrorCode::NotImplemented,
                      "RoutingRules and RedirectAllRequestsTo are not implemented.");
    WebsiteBucket w;
    w.bucket = std::move(bucket);
    auto* idx = root.find("IndexDocument");
    if (!idx)
        throw S3Error(S3ErrorCode::InvalidArgument, "IndexDocument is required.");
    w.index_suffix = idx->get("Suffix");
    if (w.index_suffix.empty() || w.index_suffix.find('/') != std::string::npos)
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "IndexDocument.Suffix must be non-empty and must not contain '/'.");
    if (auto* err = root.find("ErrorDocument")) {
        w.error_key = err->get("Key");
        if (w.error_key.empty() || w.error_key.front() == '/')
            throw S3Error(S3ErrorCode::InvalidArgument,
                          "ErrorDocument.Key must be non-empty and must not start with '/'.");
    }
    return w;
}

}  // namespace

// Root gate shared by the three handlers (mirrors admin_credentials): with auth
// disabled verify yields an empty ak, which also lands here as AccessDenied
static void require_root(const std::shared_ptr<CredentialStore>& store,
                         std::string_view access_key) {
    if (!store || !store->is_root(access_key))
        throw S3Error(S3ErrorCode::AccessDenied,
                      "The website configuration API requires a root (statically "
                      "configured) credential.");
}

Task<http::HttpResponse> S3Service::get_bucket_website(std::string bucket,
                                                       const RequestAuth& auth) {
    require_root(cred_store_, auth.access_key);
    auto snap = website_store_ ? website_store_->snapshot() : WebsiteStore::Snapshot{};
    const WebsiteBucket* w = WebsiteStore::find(snap, bucket);
    if (!w)
        throw S3Error(S3ErrorCode::NoSuchWebsiteConfiguration,
                      "The specified bucket does not have a website configuration", bucket);
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = website_xml(*w);
    co_return resp;
}

Task<http::HttpResponse> S3Service::put_bucket_website(http::HttpRequest& req,
                                                       std::string bucket,
                                                       const RequestAuth& auth) {
    require_root(cred_store_, auth.access_key);
    if (!website_store_)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "Dynamic website configuration is not available on this deployment.");
    // AWS parity: configuring a website on a bucket that does not exist is NoSuchBucket
    if (!co_await router_.resolve(bucket).bucket_exists(bucket))
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist",
                      bucket);
    auto body = co_await handlers::read_body(req);
    auto entry = parse_website_xml(body, bucket);
    co_await website_store_->put(std::move(entry));
    LOG_INFO("website: configuration for bucket {} set by {}", bucket,
             std::string(auth.access_key));
    co_return http::HttpResponse{};
}

Task<http::HttpResponse> S3Service::delete_bucket_website(std::string bucket,
                                                          const RequestAuth& auth) {
    require_root(cred_store_, auth.access_key);
    if (!website_store_)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "Dynamic website configuration is not available on this deployment.");
    // Idempotent like DeleteObject: removing a configuration that is not there is 204 too
    co_await website_store_->remove(bucket);
    LOG_INFO("website: configuration for bucket {} deleted by {}", bucket,
             std::string(auth.access_key));
    http::HttpResponse resp;
    resp.status = 204;
    co_return resp;
}

}  // namespace lights3::s3
