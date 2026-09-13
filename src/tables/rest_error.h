// Iceberg REST error model (docs/architecture/s3-tables-design.md §6.6): the catalog raises RestError
// internally and renders it as {"error":{"message","type","code"}} at the REST boundary
// only. Storage-layer S3Errors are translated once, in from_s3_error
#pragma once

#include <exception>
#include <nlohmann/json.hpp>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "s3/errors.h"

namespace lights3::tables {

struct RestError : std::exception {
    int status;
    std::string type;
    std::string message;
    // Retry-After and friends
    std::vector<std::pair<std::string, std::string>> headers;

    RestError(int s, std::string t, std::string m) : status(s), type(std::move(t)), message(std::move(m)) {}
    RestError& with_header(std::string k, std::string v) {
        headers.emplace_back(std::move(k), std::move(v));
        return *this;
    }
    const char* what() const noexcept override { return message.c_str(); }
};

inline RestError bad_request(std::string m) { return {400, "BadRequestException", std::move(m)}; }
inline RestError forbidden(std::string m) { return {403, "ForbiddenException", std::move(m)}; }
inline RestError not_authorized(std::string m) { return {401, "NotAuthorizedException", std::move(m)}; }
inline RestError not_found_ns(std::string m) { return {404, "NoSuchNamespaceException", std::move(m)}; }
inline RestError not_found_table(std::string m) { return {404, "NoSuchTableException", std::move(m)}; }
inline RestError not_found_view(std::string m) { return {404, "NoSuchViewException", std::move(m)}; }
inline RestError not_found_resource(std::string m) { return {404, "NoSuchResourceException", std::move(m)}; }
inline RestError already_exists(std::string m) { return {409, "AlreadyExistsException", std::move(m)}; }
inline RestError commit_failed(std::string m) { return {409, "CommitFailedException", std::move(m)}; }
inline RestError ns_not_empty(std::string m) { return {409, "NamespaceNotEmptyException", std::move(m)}; }
inline RestError unsupported(std::string m) { return {406, "UnsupportedOperationException", std::move(m)}; }
inline RestError unprocessable(std::string m) { return {422, "UnprocessableEntityException", std::move(m)}; }
inline RestError internal(std::string m) { return {500, "RESTException", std::move(m)}; }
inline RestError commit_state_unknown(std::string m) { return {500, "CommitStateUnknownException", std::move(m)}; }
inline RestError unavailable(std::string m) {
    RestError e{503, "ServiceUnavailableException", std::move(m)};
    e.with_header("Retry-After", "1");
    return e;
}

inline nlohmann::json to_json(const RestError& e) {
    nlohmann::json j;
    j["error"]["message"] = e.message;
    j["error"]["type"] = e.type;
    j["error"]["code"] = e.status;
    return j;
}

// Storage / auth layer error -> REST error (design §6.6). path picks the 404 flavour
inline RestError from_s3_error(const s3::S3Error& e, std::string_view path) {
    using s3::S3ErrorCode;
    switch (e.code) {
        case S3ErrorCode::AccessDenied:
        case S3ErrorCode::SignatureDoesNotMatch:
        case S3ErrorCode::RequestTimeTooSkewed:
        case S3ErrorCode::AuthorizationHeaderMalformed:
            return forbidden(e.message);
        case S3ErrorCode::InvalidAccessKeyId:
        case S3ErrorCode::InvalidToken:
        case S3ErrorCode::ExpiredToken:
            return not_authorized(e.message);
        case S3ErrorCode::PreconditionFailed:
            return commit_failed(e.message);
        case S3ErrorCode::NoSuchKey:
        case S3ErrorCode::NoSuchBucket:
            return path.find("/tables/") != std::string_view::npos ? not_found_table(e.message)
                                                                   : not_found_ns(e.message);
        case S3ErrorCode::SlowDown:
            return unavailable(e.message);
        case S3ErrorCode::QuotaExceeded:
            return commit_failed("QuotaExceeded: " + e.message);
        case S3ErrorCode::InvalidArgument:
        case S3ErrorCode::InvalidRequest:
        case S3ErrorCode::InvalidBucketName:
        case S3ErrorCode::KeyTooLongError:
        case S3ErrorCode::EntityTooLarge:
            return bad_request(e.message);
        case S3ErrorCode::NotImplemented:
            return unsupported(e.message);
        default:
            return internal("internal error");
    }
}

}  // namespace lights3::tables
