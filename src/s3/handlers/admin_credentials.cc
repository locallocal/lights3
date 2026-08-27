// L2: /-/admin/credentials -- dynamic credential management API (docs/credential-management.md §2).
// Unlike the data plane: responses and errors are both JSON; errors are caught and rendered inside this handler,
// never taking dispatch's outer S3 XML error path.
#include <nlohmann/json.hpp>

#include "core/log.h"
#include "core/util/time.h"
#include "s3/auth/credential_store.h"
#include "s3/service.h"

namespace lights3::s3 {

namespace {

using nlohmann::json;

http::HttpResponse json_response(int status, const json& j) {
    http::HttpResponse resp;
    resp.status = status;
    resp.small_body = j.dump(2) + "\n";
    resp.headers.set("Content-Type", "application/json");
    return resp;
}

// SK masking (docs/archive/gaps.md §5.10): keep only the first 4 characters. The previous "first 4 + last 4" leaked
// 8 of 40 characters, and static-credential SKs are hand-picked by operators with possibly insufficient entropy --
// leaking both ends is entirely unnecessary
std::string mask(const std::string& sk) {
    if (sk.size() < 8) return "****";
    return sk.substr(0, 4) + "****";
}

const char* source_name(CredSource s) {
    switch (s) {
        case CredSource::kStatic: return "static";
        case CredSource::kFile: return "file";
        case CredSource::kDynamic: return "dynamic";
    }
    return "dynamic";
}

json to_json(const CredentialInfo& c, bool with_secret) {
    json j;
    j["access_key"] = c.access_key;
    // The plaintext SK of static (root) credentials is never returned via the admin API (docs/archive/gaps.md §5.10):
    // it comes from a config file/environment variable, and retrieving it would downgrade the "can read config"
    // trust boundary to a single HTTP GET -- and the root SK is exactly the one that cannot be revoked via the
    // admin API; if leaked, the only remedy is changing config and restarting
    if (with_secret && !c.is_static())
        j["secret_key"] = static_cast<const std::string&>(c.secret_key);
    else
        j["secret_key_masked"] = mask(c.secret_key);
    j["source"] = source_name(c.source);
    if (c.source == CredSource::kDynamic) {
        j["created_at"] = util::iso8601(c.created);
        j["rev"] = c.rev;  // edit counter (roadmap §2.5)
    }
    if (!c.is_static()) {
        if (!c.comment.empty()) j["comment"] = c.comment;
        if (c.policy) j["policy"] = json::parse(policy_to_json(*c.policy));
    }
    return j;
}

// POST body (optional): {"comment": "...", "policy": {"buckets": [...], "readonly": bool}}
// Unknown top-level fields and invalid policy are strictly rejected (InvalidRequest)
struct CreateRequest {
    std::string comment;
    std::optional<CredentialPolicy> policy;
};

Task<CreateRequest> parse_create_body(http::HttpRequest& req) {
    CreateRequest out;
    out.comment = req.query_get("comment").value_or("");
    if (!req.body) co_return out;
    std::string text;
    std::byte buf[16 * 1024];
    for (;;) {
        size_t n = co_await req.body->read(std::span(buf));
        if (n == 0) break;
        if (text.size() + n > 64 * 1024)
            throw S3Error(S3ErrorCode::InvalidRequest, "Request body too large.");
        text.append(reinterpret_cast<const char*>(buf), n);
    }
    if (text.empty()) co_return out;
    json j;
    try {
        j = json::parse(text);
    } catch (const json::exception&) {
        throw S3Error(S3ErrorCode::InvalidRequest, "Request body is not valid JSON.");
    }
    if (!j.is_object())
        throw S3Error(S3ErrorCode::InvalidRequest, "Request body must be a JSON object.");
    for (auto& [k, v] : j.items()) {
        if (k == "comment") {
            if (!v.is_string())
                throw S3Error(S3ErrorCode::InvalidRequest, "comment must be a string.");
            out.comment = v.get<std::string>();  // body takes precedence over ?comment=
        } else if (k == "policy") {
            out.policy = parse_policy_json(v.dump());
        } else {
            throw S3Error(S3ErrorCode::InvalidRequest, "unknown field '" + k + "'.");
        }
    }
    co_return out;
}

}  // namespace

Task<http::HttpResponse> S3Service::admin_credentials(http::HttpRequest& req,
                                                      std::string& access_key) {
    constexpr std::string_view kBase = "/-/admin/credentials";
    try {
        access_key = auth_.verify(req).access_key;
        // Two-tier model (docs/credential-management.md §3): only root (static credentials) may use this; with auth
        // disabled, verify returns an empty ak, which also falls to AccessDenied -- no root, no admin plane
        if (!cred_store_ || !cred_store_->is_root(access_key))
            throw S3Error(S3ErrorCode::AccessDenied,
                          "Admin API requires a root (statically configured) credential.");

        // Subpath: /-/admin/credentials[/{ak}]
        std::string rest = req.path.substr(kBase.size());
        if (!rest.empty() && rest.front() == '/') rest.erase(0, 1);
        if (rest.find('/') != std::string::npos)
            throw S3Error(S3ErrorCode::InvalidRequest, "Malformed admin API path.");

        if (req.method == "POST" && rest.empty()) {
            auto body = co_await parse_create_body(req);
            auto c = co_await cred_store_->generate(std::move(body.comment),
                                                    std::move(body.policy));
            co_return json_response(201, to_json(c, /*with_secret=*/true));
        }
        if (req.method == "GET" && rest.empty()) {
            json j;
            j["credentials"] = json::array();
            for (auto& c : cred_store_->list()) j["credentials"].push_back(to_json(c, false));
            co_return json_response(200, j);
        }
        if (req.method == "GET") {
            auto c = cred_store_->find(rest);
            if (!c)
                throw S3Error(S3ErrorCode::InvalidAccessKeyId,
                              "The specified access key does not exist.");
            bool show = req.query_get("show-secret").value_or("") == "true";
            // Retrieving the plaintext SK is highly sensitive: leave an audit trail whether or not it is granted
            if (show)
                LOG_WARN("admin: plaintext secret requested for {} by root {}{}", c->access_key,
                         access_key, c->is_static() ? " (static credential — refused)" : "");
            co_return json_response(200, to_json(*c, show));
        }
        if (req.method == "PUT" && !rest.empty()) {
            // Update a dynamic credential in place (roadmap §2.5): fields present in the
            // body are replaced — "policy": null clears the policy, an absent field is
            // kept. Multi-instance propagation via the sync rev/ETag comparison
            std::string text;
            if (req.body) {
                std::byte buf[16 * 1024];
                for (;;) {
                    size_t n = co_await req.body->read(std::span(buf));
                    if (n == 0) break;
                    if (text.size() + n > 64 * 1024)
                        throw S3Error(S3ErrorCode::InvalidRequest, "Request body too large.");
                    text.append(reinterpret_cast<const char*>(buf), n);
                }
            }
            if (text.empty())
                throw S3Error(S3ErrorCode::InvalidRequest,
                              "Update requires a JSON body with comment and/or policy.");
            json j;
            try {
                j = json::parse(text);
            } catch (const json::exception&) {
                throw S3Error(S3ErrorCode::InvalidRequest, "Request body is not valid JSON.");
            }
            if (!j.is_object())
                throw S3Error(S3ErrorCode::InvalidRequest,
                              "Request body must be a JSON object.");
            CredentialStore::Update upd;
            for (auto& [k, v] : j.items()) {
                if (k == "comment") {
                    if (!v.is_string())
                        throw S3Error(S3ErrorCode::InvalidRequest, "comment must be a string.");
                    upd.comment = v.get<std::string>();
                } else if (k == "policy") {
                    upd.set_policy = true;
                    if (!v.is_null()) upd.policy = parse_policy_json(v.dump());
                } else {
                    throw S3Error(S3ErrorCode::InvalidRequest, "unknown field '" + k + "'.");
                }
            }
            if (!upd.comment && !upd.set_policy)
                throw S3Error(S3ErrorCode::InvalidRequest,
                              "Update requires at least one of comment or policy.");
            auto c = co_await cred_store_->update(rest, std::move(upd));
            LOG_INFO("admin: credential {} updated by root {}", rest, access_key);
            co_return json_response(200, to_json(c, /*with_secret=*/false));
        }
        if (req.method == "DELETE" && !rest.empty()) {
            co_await cred_store_->remove(rest);
            http::HttpResponse resp;
            resp.status = 204;
            co_return resp;
        }
        throw S3Error(S3ErrorCode::MethodNotAllowed,
                      "The specified method is not allowed against this resource.");
    } catch (const S3Error& e) {
        metrics_.s3_error(e.code);
        json j;
        j["code"] = wire_code(e.code);
        // Raw internal error text may contain backend topology: log only; the response uses fixed wording
        if (e.code == S3ErrorCode::InternalError) {
            LOG_ERROR("admin api {} {} internal error: {}", req.method, req.path, e.message);
            j["message"] = "We encountered an internal error.";
        } else {
            j["message"] = e.message;
        }
        co_return json_response(http_status(e.code), j);
    } catch (const std::exception& e) {
        // runtime_error such as getentropy/put failures from generate(): this handler promises all errors
        // render as JSON and must never escape to the outer S3 XML path
        LOG_ERROR("admin api {} {} internal error: {}", req.method, req.path, e.what());
        metrics_.s3_error(S3ErrorCode::InternalError);
        json j;
        j["code"] = "InternalError";
        j["message"] = "We encountered an internal error.";
        co_return json_response(500, j);
    }
}

}  // namespace lights3::s3
