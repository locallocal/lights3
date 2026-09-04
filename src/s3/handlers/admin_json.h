// Shared helpers for the JSON admin plane (/-/admin/*): response shaping, bounded
// body reads, and the error rendering contract (docs/credential-management.md §2 —
// admin errors are JSON bodies with the S3 wire code, never the XML path)
#pragma once

#include <nlohmann/json.hpp>
#include <span>
#include <string>

#include "core/log.h"
#include "core/task.h"
#include "http/model.h"
#include "s3/errors.h"

namespace lights3::s3::handlers {

inline http::HttpResponse json_response(int status, const nlohmann::json& j) {
    http::HttpResponse resp;
    resp.status = status;
    resp.small_body = j.dump(2) + "\n";
    resp.headers.set("Content-Type", "application/json");
    return resp;
}

// Reads a JSON object body (64KiB cap). Empty body -> empty object when allow_empty,
// else InvalidRequest. Non-object / malformed -> InvalidRequest
inline Task<nlohmann::json> read_json_object(http::HttpRequest& req, bool allow_empty) {
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
    if (text.empty()) {
        if (allow_empty) co_return nlohmann::json::object();
        throw S3Error(S3ErrorCode::InvalidRequest, "Request requires a JSON body.");
    }
    nlohmann::json j;
    try {
        j = nlohmann::json::parse(text);
    } catch (const nlohmann::json::exception&) {
        throw S3Error(S3ErrorCode::InvalidRequest, "Request body is not valid JSON.");
    }
    if (!j.is_object())
        throw S3Error(S3ErrorCode::InvalidRequest, "Request body must be a JSON object.");
    co_return j;
}

inline http::HttpResponse admin_error(const S3Error& e, const http::HttpRequest& req) {
    nlohmann::json j;
    j["code"] = wire_code(e.code);
    if (e.code == S3ErrorCode::InternalError) {
        LOG_ERROR("admin api {} {} internal error: {}", req.method, req.path, e.message);
        j["message"] = "We encountered an internal error.";
    } else {
        j["message"] = e.message;
    }
    return json_response(http_status(e.code), j);
}

inline uint64_t json_u64(const nlohmann::json& v, const char* field) {
    if (!v.is_number_unsigned() && !(v.is_number_integer() && v.get<int64_t>() >= 0))
        throw S3Error(S3ErrorCode::InvalidRequest,
                      std::string(field) + " must be a non-negative integer.");
    return v.get<uint64_t>();
}

}  // namespace lights3::s3::handlers
