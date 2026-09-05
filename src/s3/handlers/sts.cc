// L2: STS AssumeRole endpoint (roadmap §2.6) — POST / with a form body, signed SigV4
// with service scope "sts" (exactly how SDKs call a custom STS endpoint). Mints
// short-lived session credentials (AK/SK/token + TTL) that inherit the caller's
// policy: this implementation has no role catalog, so RoleArn is accepted and echoed
// but a session can never exceed the identity that minted it. Errors render in the
// STS ErrorResponse XML shape (not the S3 one) so STS SDK clients parse them.
#include "core/log.h"
#include "core/util/crypto.h"
#include "core/util/time.h"
#include "core/util/uri.h"
#include "s3/auth/credential_store.h"
#include "s3/handlers/common.h"
#include "s3/service.h"
#include "s3/xml.h"

namespace lights3::s3 {

namespace {

// application/x-www-form-urlencoded: '+' means space, then percent-decoding
std::map<std::string, std::string> parse_form(const std::string& body) {
    std::map<std::string, std::string> out;
    size_t pos = 0;
    while (pos < body.size()) {
        size_t amp = body.find('&', pos);
        if (amp == std::string::npos) amp = body.size();
        std::string item = body.substr(pos, amp - pos);
        auto eq = item.find('=');
        auto decode = [](std::string s) {
            for (char& c : s)
                if (c == '+') c = ' ';
            return util::percent_decode(s);
        };
        std::string k = decode(eq == std::string::npos ? item : item.substr(0, eq));
        std::string v = eq == std::string::npos ? "" : decode(item.substr(eq + 1));
        if (!k.empty()) out[std::move(k)] = std::move(v);
        pos = amp + 1;
    }
    return out;
}

http::HttpResponse sts_error(const S3Error& e, const std::string& request_id) {
    XmlWriter x;
    x.open("ErrorResponse", R"(xmlns="https://sts.amazonaws.com/doc/2011-06-15/")");
    x.open("Error");
    x.element("Type", "Sender");
    x.element("Code", wire_code(e.code));
    x.element("Message", e.message);
    x.close();
    x.element("RequestId", request_id);
    x.close();
    http::HttpResponse resp;
    resp.status = http_status(e.code);
    resp.headers.set("Content-Type", "text/xml");
    resp.small_body = x.str();
    return resp;
}

}  // namespace

Task<http::HttpResponse> S3Service::sts_endpoint(http::HttpRequest& req,
                                                 const RequestContext& ctx,
                                                 std::string& access_key) {
    try {
        // The form body is read BEFORE verification: generic (non-S3) SigV4 carries the
        // payload hash only inside the canonical request, so verify needs it up front
        std::string body = co_await handlers::read_body(req, 64 * 1024);
        auto ident = auth_.verify_sts(req, util::sha256_hex(body));
        access_key = ident.access_key;
        if (!auth_.enabled())
            throw S3Error(S3ErrorCode::AccessDenied,
                          "STS requires authentication to be enabled.");

        auto params = parse_form(body);
        std::string action = params.count("Action") ? params["Action"] : "";
        if (action != "AssumeRole")
            throw S3Error(S3ErrorCode::NotImplemented,
                          "Only the AssumeRole action is implemented.");
        int duration = 3600;
        if (auto it = params.find("DurationSeconds"); it != params.end()) {
            try {
                duration = std::stoi(it->second);
            } catch (...) {
                throw S3Error(S3ErrorCode::InvalidArgument, "Invalid DurationSeconds.");
            }
            // AWS bounds for AssumeRole
            if (duration < 900 || duration > 43200)
                throw S3Error(S3ErrorCode::InvalidArgument,
                              "DurationSeconds must be between 900 and 43200.");
        }
        std::string role_arn = params.count("RoleArn") ? params["RoleArn"] : "";
        std::string session_name =
            params.count("RoleSessionName") ? params["RoleSessionName"] : "session";

        if (!cred_store_)
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "STS is not available on this deployment.");
        auto sc = co_await cred_store_->mint_session(access_key, duration);
        LOG_INFO("sts: session {} minted for {} ({}s, role '{}')", sc.access_key, access_key,
                 duration, role_arn);
        {
            AuditEvent e;
            e.event = "sts.assume_role";
            e.actor = access_key;
            e.tenant = ident.tenant;
            e.request_id = ctx.request_id;
            e.target = sc.access_key;
            std::string detail = "duration=" + std::to_string(duration) + " role=" + role_arn +
                                 " session=" + session_name;
            e.detail = detail;
            audit(e);
        }

        XmlWriter x;
        x.open("AssumeRoleResponse", R"(xmlns="https://sts.amazonaws.com/doc/2011-06-15/")");
        x.open("AssumeRoleResult");
        x.open("AssumedRoleUser");
        // No role catalog: the ARN names this gateway and the caller so audits can
        // trace the session back; it is not an IAM role reference
        x.element("AssumedRoleId", sc.access_key + ":" + session_name);
        x.element("Arn", "arn:lights3:sts::assumed-role/" + std::string(access_key) + "/" +
                             session_name);
        x.close();
        x.open("Credentials");
        x.element("AccessKeyId", sc.access_key);
        x.element("SecretAccessKey", static_cast<const std::string&>(sc.secret_key));
        x.element("SessionToken", sc.token);
        x.element("Expiration", util::iso8601(sc.expires));
        x.close();
        x.close();
        x.open("ResponseMetadata");
        x.element("RequestId", ctx.request_id);
        x.close();
        x.close();
        http::HttpResponse resp;
        resp.headers.set("Content-Type", "text/xml");
        resp.small_body = x.str();
        co_return resp;
    } catch (const S3Error& e) {
        metrics_.s3_error(e.code);
        co_return sts_error(e, ctx.request_id);
    } catch (const std::exception& e) {
        LOG_ERROR("sts: internal error: {}", e.what());
        metrics_.s3_error(S3ErrorCode::InternalError);
        co_return sts_error(S3Error(S3ErrorCode::InternalError,
                                    "We encountered an internal error."),
                            ctx.request_id);
    }
}

}  // namespace lights3::s3
