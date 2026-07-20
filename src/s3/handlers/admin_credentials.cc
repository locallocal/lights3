// L2: /-/admin/credentials —— 动态凭证管理 API（docs/credential-management.md §2）。
// 与数据面不同：响应与错误均为 JSON；错误在本 handler 内捕获渲染，
// 不走 dispatch 外层的 S3 XML 错误路径。
#include <nlohmann/json.hpp>

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

// SK 掩码：前 4 + **** + 后 4（短 SK 全掩）
std::string mask(const std::string& sk) {
    if (sk.size() < 12) return "****";
    return sk.substr(0, 4) + "****" + sk.substr(sk.size() - 4);
}

json to_json(const CredentialInfo& c, bool with_secret) {
    json j;
    j["access_key"] = c.access_key;
    if (with_secret) j["secret_key"] = c.secret_key;
    else j["secret_key_masked"] = mask(c.secret_key);
    j["source"] = c.is_static ? "static" : "dynamic";
    if (!c.is_static) {
        j["created_at"] = util::iso8601(c.created);
        if (!c.comment.empty()) j["comment"] = c.comment;
    }
    return j;
}

}  // namespace

Task<http::HttpResponse> S3Service::admin_credentials(http::HttpRequest& req,
                                                      std::string& access_key) {
    constexpr std::string_view kBase = "/-/admin/credentials";
    try {
        access_key = auth_.verify(req);
        // 两级模型（docs/credential-management.md §3）：仅 root（静态凭证）可用；认证关闭时
        // verify 返回空 ak，同样落到 AccessDenied——没有 root 就没有管理面
        if (!cred_store_ || !cred_store_->is_root(access_key))
            throw S3Error(S3ErrorCode::AccessDenied,
                          "Admin API requires a root (statically configured) credential.");

        // 子路径：/-/admin/credentials[/{ak}]
        std::string rest = req.path.substr(kBase.size());
        if (!rest.empty() && rest.front() == '/') rest.erase(0, 1);
        if (rest.find('/') != std::string::npos)
            throw S3Error(S3ErrorCode::InvalidRequest, "Malformed admin API path.");

        if (req.method == "POST" && rest.empty()) {
            auto c = co_await cred_store_->generate(
                std::string(req.query_get("comment").value_or("")));
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
            co_return json_response(200, to_json(*c, show));
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
        metrics_.s3_error(wire_code(e.code));
        json j;
        j["code"] = wire_code(e.code);
        j["message"] = e.message;
        co_return json_response(http_status(e.code), j);
    }
}

}  // namespace lights3::s3
