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

// AWS XML shape: <WebsiteConfiguration> with either <RedirectAllRequestsTo> alone, or
// <IndexDocument> [+ <ErrorDocument>] [+ <RoutingRules>] (roadmap §2.3)
std::string website_xml(const WebsiteBucket& w) {
    XmlWriter x;
    x.open("WebsiteConfiguration", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    if (!w.redirect_all_host.empty()) {
        x.open("RedirectAllRequestsTo");
        x.element("HostName", w.redirect_all_host);
        if (!w.redirect_all_protocol.empty()) x.element("Protocol", w.redirect_all_protocol);
        x.close();
        x.close();
        return x.str();
    }
    x.open("IndexDocument");
    x.element("Suffix", w.index_suffix);
    x.close();
    if (!w.error_key.empty()) {
        x.open("ErrorDocument");
        x.element("Key", w.error_key);
        x.close();
    }
    if (!w.routing_rules.empty()) {
        x.open("RoutingRules");
        for (auto& r : w.routing_rules) {
            x.open("RoutingRule");
            if (!r.key_prefix_equals.empty() || r.http_error_code_equals) {
                x.open("Condition");
                if (!r.key_prefix_equals.empty())
                    x.element("KeyPrefixEquals", r.key_prefix_equals);
                if (r.http_error_code_equals)
                    x.element("HttpErrorCodeReturnedEquals",
                              static_cast<uint64_t>(r.http_error_code_equals));
                x.close();
            }
            x.open("Redirect");
            if (!r.protocol.empty()) x.element("Protocol", r.protocol);
            if (!r.host_name.empty()) x.element("HostName", r.host_name);
            if (r.replace_key_prefix_with)
                x.element("ReplaceKeyPrefixWith", *r.replace_key_prefix_with);
            if (r.replace_key_with) x.element("ReplaceKeyWith", *r.replace_key_with);
            if (r.http_redirect_code != 301)
                x.element("HttpRedirectCode", static_cast<uint64_t>(r.http_redirect_code));
            x.close();
            x.close();
        }
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
    WebsiteBucket w;
    w.bucket = std::move(bucket);
    // RedirectAllRequestsTo is exclusive with everything else (AWS shape): a config
    // carrying both has ambiguous intent and is rejected rather than half-honored
    if (auto* ra = root.find("RedirectAllRequestsTo")) {
        if (root.find("IndexDocument") || root.find("ErrorDocument") ||
            root.find("RoutingRules"))
            throw S3Error(S3ErrorCode::InvalidArgument,
                          "RedirectAllRequestsTo cannot be combined with IndexDocument, "
                          "ErrorDocument or RoutingRules.");
        w.redirect_all_host = ra->get("HostName");
        if (w.redirect_all_host.empty())
            throw S3Error(S3ErrorCode::InvalidArgument,
                          "RedirectAllRequestsTo.HostName is required.");
        w.redirect_all_protocol = ra->get("Protocol");
        if (!w.redirect_all_protocol.empty() && w.redirect_all_protocol != "http" &&
            w.redirect_all_protocol != "https")
            throw S3Error(S3ErrorCode::InvalidArgument,
                          "RedirectAllRequestsTo.Protocol must be http or https.");
        return w;
    }
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
    if (auto* rules = root.find("RoutingRules")) {
        for (auto& rr : rules->children) {
            if (rr.name != "RoutingRule") continue;
            if (w.routing_rules.size() >= 50)  // AWS quota
                throw S3Error(S3ErrorCode::InvalidArgument,
                              "RoutingRules may contain at most 50 rules.");
            WebsiteRoutingRule r;
            if (auto* cond = rr.find("Condition")) {
                r.key_prefix_equals = cond->get("KeyPrefixEquals");
                std::string code = cond->get("HttpErrorCodeReturnedEquals");
                if (!code.empty()) {
                    try {
                        r.http_error_code_equals = std::stoi(code);
                    } catch (...) {
                        throw S3Error(S3ErrorCode::InvalidArgument,
                                      "Invalid HttpErrorCodeReturnedEquals value.");
                    }
                    if (r.http_error_code_equals < 400 || r.http_error_code_equals > 599)
                        throw S3Error(S3ErrorCode::InvalidArgument,
                                      "HttpErrorCodeReturnedEquals must be a 4xx or 5xx "
                                      "status code.");
                }
            }
            auto* red = rr.find("Redirect");
            if (!red)
                throw S3Error(S3ErrorCode::InvalidArgument,
                              "Each RoutingRule must contain a Redirect.");
            r.protocol = red->get("Protocol");
            if (!r.protocol.empty() && r.protocol != "http" && r.protocol != "https")
                throw S3Error(S3ErrorCode::InvalidArgument,
                              "Redirect.Protocol must be http or https.");
            r.host_name = red->get("HostName");
            if (auto* n = red->find("ReplaceKeyPrefixWith")) r.replace_key_prefix_with = n->text;
            if (auto* n = red->find("ReplaceKeyWith")) r.replace_key_with = n->text;
            if (r.replace_key_prefix_with && r.replace_key_with)
                throw S3Error(S3ErrorCode::InvalidArgument,
                              "ReplaceKeyPrefixWith and ReplaceKeyWith are mutually "
                              "exclusive.");
            std::string code = red->get("HttpRedirectCode");
            if (!code.empty()) {
                try {
                    r.http_redirect_code = std::stoi(code);
                } catch (...) {
                    throw S3Error(S3ErrorCode::InvalidArgument,
                                  "Invalid HttpRedirectCode value.");
                }
                if (r.http_redirect_code < 300 || r.http_redirect_code > 399)
                    throw S3Error(S3ErrorCode::InvalidArgument,
                                  "HttpRedirectCode must be a 3xx status code.");
            }
            // A Redirect with no effect at all (no host/protocol/key change) loops back
            // to the same resource — reject the no-op
            if (r.protocol.empty() && r.host_name.empty() && !r.replace_key_prefix_with &&
                !r.replace_key_with)
                throw S3Error(S3ErrorCode::InvalidArgument,
                              "Redirect must specify at least one of Protocol, HostName, "
                              "ReplaceKeyPrefixWith or ReplaceKeyWith.");
            w.routing_rules.push_back(std::move(r));
        }
        if (w.routing_rules.empty())
            throw S3Error(S3ErrorCode::InvalidArgument,
                          "RoutingRules must contain at least one RoutingRule.");
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
