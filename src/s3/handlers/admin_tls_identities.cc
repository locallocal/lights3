// L2: mTLS identity mapping (backlog-sequence ⑥, docs/tls.md §2.1,
// docs/multi-tenancy.md §4.2) -- two halves in one file:
//   1. verify_identity / enforce_tls_tenant: the dispatch-side rules. A verified
//      client certificate whose selected subject (auth.tls_identity) is bound to
//      a credential stands in for the SigV4 signature on an unsigned request
//      (the request runs as that credential: policy, tenant, role); on a signed
//      request the certificate and the signing credential must belong to the
//      same tenant (root is exempt -- an operator's certificate may front any
//      credential, and root has no tenant to compare). An unbound certificate
//      is refused on both paths: enabling the mode means "every certificate is
//      accounted for", so a stray one is a configuration error, not anonymity;
//   2. /-/admin/tls-identities: the binding table's admin API (root only, JSON
//      conventions of /-/admin/credentials).
#include "core/log.h"
#include "core/util/time.h"
#include "core/util/uri.h"
#include "s3/auth/credential_store.h"
#include "s3/handlers/admin_json.h"
#include "s3/service.h"

namespace lights3::s3 {

using namespace handlers;
using nlohmann::json;

namespace {

constexpr std::string_view kBase = "/-/admin/tls-identities";

json to_json(const std::string& subject, const TlsBinding& b) {
    json j;
    j["subject"] = subject;
    j["access_key"] = b.access_key;
    if (!b.comment.empty()) j["comment"] = b.comment;
    j["created_at"] = util::iso8601(b.created);
    if (!b.created_by.empty()) j["created_by"] = b.created_by;
    return j;
}

const char* mode_name(S3Service::TlsIdentityMode m) {
    switch (m) {
        case S3Service::TlsIdentityMode::Off: return "off";
        case S3Service::TlsIdentityMode::SubjectCn: return "subject-cn";
        case S3Service::TlsIdentityMode::SanUri: return "san-uri";
    }
    return "off";
}

bool has_signature(const http::HttpRequest& req) {
    return req.headers.has("Authorization") || req.query_has("X-Amz-Algorithm");
}

}  // namespace

// ---------- dispatch-side rules ----------

S3Service::TlsIdentityMode S3Service::parse_tls_identity_mode(const std::string& s) {
    if (s == "off" || s.empty()) return TlsIdentityMode::Off;
    if (s == "subject-cn") return TlsIdentityMode::SubjectCn;
    if (s == "san-uri") return TlsIdentityMode::SanUri;
    throw std::runtime_error("auth.tls_identity must be off|subject-cn|san-uri, got '" + s + "'");
}

std::optional<std::string> S3Service::tls_subject_of(const http::HttpRequest& req) const {
    if (tls_mode_ == TlsIdentityMode::Off || !req.tls_identity) return std::nullopt;
    const std::string& v = tls_mode_ == TlsIdentityMode::SubjectCn ? req.tls_identity->subject_cn
                                                                    : req.tls_identity->san_uri;
    // A subject the binding table could never hold is no identity (and never matches)
    if (!valid_tls_subject(v)) return std::nullopt;
    return v;
}

bool S3Service::tls_identity_bound(const http::HttpRequest& req) const {
    auto subject = tls_subject_of(req);
    if (!subject || !tls_store_) return false;
    return TlsIdentityStore::find(tls_store_->snapshot(), *subject) != nullptr;
}

void S3Service::enforce_tls_tenant(const http::HttpRequest& req,
                                   const VerifiedIdentity& ident) const {
    auto subject = tls_subject_of(req);
    if (!subject) return;
    if (ident.access_key.empty()) return;        // auth disabled: nothing to compare
    if (is_root(ident.access_key)) return;       // an operator's certificate fronts anything
    const TlsBinding* b = tls_store_ ? TlsIdentityStore::find(tls_store_->snapshot(), *subject)
                                     : nullptr;
    if (!b)
        throw S3Error(S3ErrorCode::AccessDenied,
                      "The client certificate '" + *subject + "' is not bound to a credential.");
    if (b->access_key == ident.access_key) return;
    auto bound = cred_store_ ? cred_store_->lookup(b->access_key) : std::nullopt;
    if (!bound)
        throw S3Error(S3ErrorCode::AccessDenied,
                      "The credential bound to the client certificate no longer exists.");
    // Legacy (tenant-less) credentials compare as one tenant: "" == ""
    if (bound->tenant != ident.tenant)
        throw S3Error(S3ErrorCode::AccessDenied,
                      "The client certificate belongs to a different tenant than the "
                      "signing credential.");
}

VerifiedIdentity S3Service::verify_identity(http::HttpRequest& req) const {
    auto subject = tls_subject_of(req);
    if (!subject || has_signature(req)) {
        auto ident = auth_.verify(req);
        if (subject) enforce_tls_tenant(req, ident);
        return ident;
    }
    // Unsigned request from a verified certificate: the binding is the signature
    if (!auth_.enabled()) return {};
    const TlsBinding* b = tls_store_ ? TlsIdentityStore::find(tls_store_->snapshot(), *subject)
                                     : nullptr;
    if (!b)
        throw S3Error(S3ErrorCode::AccessDenied,
                      "The client certificate '" + *subject + "' is not bound to a credential.");
    // Same single-lookup snapshot rule as a signed request (docs/archive/gaps.md
    // §3.7): policy / tenant / role are taken with the credential, never re-read
    auto bound = cred_store_ ? cred_store_->lookup(b->access_key) : std::nullopt;
    if (!bound)
        throw S3Error(S3ErrorCode::AccessDenied,
                      "The credential bound to the client certificate no longer exists.");
    VerifiedIdentity ident;
    ident.access_key = b->access_key;
    ident.policy = std::move(bound->policy);
    ident.tenant = std::move(bound->tenant);
    ident.tenant_admin = bound->tenant_admin;
    return ident;
}

// ---------- admin API ----------

Task<http::HttpResponse> S3Service::admin_tls_identities(http::HttpRequest& req,
                                                         std::string& access_key,
                                                         const RequestContext& ctx) {
    try {
        auto ident = verify_identity(req);
        access_key = ident.access_key;
        if (!is_root(access_key))
            throw S3Error(S3ErrorCode::AccessDenied,
                          "Managing certificate bindings requires a root (statically "
                          "configured) credential.");
        if (!tls_store_)
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "Certificate bindings are not available on this deployment.");
        // Subject: the path remainder (percent-decoded by the driver, may contain
        // '/' -- a URI SAN does), or ?subject= for callers whose HTTP stack
        // normalizes paths
        std::string subject;
        if (req.path.size() > kBase.size()) subject = req.path.substr(kBase.size() + 1);
        if (subject.empty()) subject = req.query_get("subject").value_or("");
        auto audit_event = [&](std::string_view event, std::string detail) {
            AuditEvent e;
            e.event = event;
            e.actor = access_key;
            e.request_id = ctx.request_id;
            e.target = subject;
            e.detail = detail;
            audit(e);
        };

        if (req.method == "GET" && subject.empty()) {
            json j;
            j["mode"] = mode_name(tls_mode_);
            j["identities"] = json::array();
            auto snap = tls_store_->snapshot();
            for (auto& [s, b] : *snap) j["identities"].push_back(to_json(s, b));
            co_return json_response(200, j);
        }
        if (subject.empty())
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "Usage: /-/admin/tls-identities/<subject> (or ?subject=).");
        if (!valid_tls_subject(subject))
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "Subject must be 1-" + std::to_string(kMaxTlsSubjectLen) +
                              " bytes of printable text without surrounding whitespace.");
        if (req.method == "GET") {
            auto snap = tls_store_->snapshot();
            const TlsBinding* b = TlsIdentityStore::find(snap, subject);
            if (!b)
                throw S3Error(S3ErrorCode::NoSuchKey,
                              "No credential is bound to certificate subject '" + subject + "'.");
            co_return json_response(200, to_json(subject, *b));
        }
        if (req.method == "PUT") {
            auto body = co_await read_json_object(req, /*allow_empty=*/false);
            TlsBinding b;
            for (auto& [k, v] : body.items()) {
                if (k == "access_key") {
                    if (!v.is_string())
                        throw S3Error(S3ErrorCode::InvalidRequest, "access_key must be a string.");
                    b.access_key = v.get<std::string>();
                } else if (k == "comment") {
                    if (!v.is_string())
                        throw S3Error(S3ErrorCode::InvalidRequest, "comment must be a string.");
                    b.comment = v.get<std::string>();
                } else {
                    throw S3Error(S3ErrorCode::InvalidRequest, "unknown field '" + k + "'.");
                }
            }
            if (b.access_key.empty())
                throw S3Error(S3ErrorCode::InvalidRequest, "access_key is required.");
            // Sessions are short-lived and never root: a binding to one would
            // outlive it and silently stop working
            if (b.access_key.rfind(kSessionAkPrefix, 0) == 0)
                throw S3Error(S3ErrorCode::InvalidRequest,
                              "STS session credentials cannot be bound to a certificate.");
            if (!cred_store_ || !cred_store_->find(b.access_key))
                throw S3Error(S3ErrorCode::InvalidAccessKeyId,
                              "The specified access key does not exist.");
            b.created = std::chrono::system_clock::now();
            b.created_by = access_key;
            bool replaced = TlsIdentityStore::find(tls_store_->snapshot(), subject) != nullptr;
            co_await tls_store_->put(subject, b);
            LOG_INFO("admin: certificate subject '{}' bound to {} by {}", subject, b.access_key,
                     access_key);
            audit_event("tls.bind", "access_key=" + b.access_key);
            co_return json_response(replaced ? 200 : 201, to_json(subject, b));
        }
        if (req.method == "DELETE") {
            co_await tls_store_->remove(subject);
            audit_event("tls.unbind", "");
            http::HttpResponse resp;
            resp.status = 204;
            co_return resp;
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
