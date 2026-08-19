// Single source of truth for S3 error codes: code -> (HTTP status, wire code) (docs/s3-protocol.md §5)
// The L3 storage layer also throws S3Error (convention in docs/storage-backend.md §1), so this header has no HTTP model dependency.
#pragma once

#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace lights3::s3 {

// X-macro single source of truth: X(enum name, HTTP status). The wire code is the enum name string.
// The enum, http_status(), wire_code(), and code_from_wire() are all derived from it -- adding an
// error code touches only this one place, so forward and reverse mappings cannot drift. Keep alphabetical order.
#define LIGHTS3_S3_ERROR_CODES(X)        \
    X(AccessDenied, 403)                 \
    X(AuthorizationHeaderMalformed, 400) \
    X(BadDigest, 400)                    \
    X(BucketAlreadyExists, 409)          \
    X(BucketAlreadyOwnedByYou, 409)      \
    X(BucketNotEmpty, 409)               \
    X(EntityTooLarge, 400)               \
    X(EntityTooSmall, 400)               \
    X(ExpiredToken, 400)                 \
    X(InternalError, 500)                \
    X(InvalidAccessKeyId, 403)           \
    X(InvalidArgument, 400)              \
    X(InvalidBucketName, 400)            \
    X(InvalidDigest, 400)                \
    X(InvalidLocationConstraint, 400)    \
    X(InvalidObjectState, 403)           \
    X(InvalidPart, 400)                  \
    X(InvalidPartOrder, 400)             \
    X(InvalidRange, 416)                 \
    X(InvalidRequest, 400)               \
    X(InvalidToken, 400)                 \
    X(KeyTooLongError, 400)              \
    X(MalformedXML, 400)                 \
    X(MethodNotAllowed, 405)             \
    X(MissingContentLength, 411)         \
    X(NoSuchBucket, 404)                 \
    X(NoSuchKey, 404)                    \
    X(NoSuchWebsiteConfiguration, 404)   \
    X(NoSuchUpload, 404)                 \
    X(NotImplemented, 501)               \
    X(PermanentRedirect, 301)            \
    X(PreconditionFailed, 412)           \
    X(RequestTimeout, 400)               \
    X(RequestTimeTooSkewed, 403)         \
    X(SignatureDoesNotMatch, 403)        \
    X(SlowDown, 503)                     \
    X(TooManyBuckets, 400)               \
    X(XAmzContentSHA256Mismatch, 400)

enum class S3ErrorCode {
#define LIGHTS3_S3_ERROR_ENUM(name, status) name,
    LIGHTS3_S3_ERROR_CODES(LIGHTS3_S3_ERROR_ENUM)
#undef LIGHTS3_S3_ERROR_ENUM
};

// Code table size (same source as the enum): error counters use a fixed-size atomic array indexed by enum value, lock-free
inline constexpr size_t kS3ErrorCodeCount = 0
#define LIGHTS3_S3_ERROR_COUNT(name, status) +1
    LIGHTS3_S3_ERROR_CODES(LIGHTS3_S3_ERROR_COUNT)
#undef LIGHTS3_S3_ERROR_COUNT
    ;

struct S3Error : std::exception {
    S3ErrorCode code;
    std::string message;
    std::string resource;
    // A few errors are only complete with specific response headers per RFC/S3: Allow for 405 (RFC 9110 §15.5.6),
    // x-amz-bucket-region for redirect-class errors. Carried on the exception; L2 attaches them to the response during unified rendering
    std::vector<std::pair<std::string, std::string>> headers;

    S3Error(S3ErrorCode c, std::string msg, std::string res = "")
        : code(c), message(std::move(msg)), resource(std::move(res)) {}
    S3Error& with_header(std::string k, std::string v) {
        headers.emplace_back(std::move(k), std::move(v));
        return *this;
    }
    const char* what() const noexcept override { return message.c_str(); }
};

int http_status(S3ErrorCode code);
const char* wire_code(S3ErrorCode code);
// wire code -> enum reverse lookup (used by cloudproxy to pass through remote errors, docs/cloudproxy-backend.md §5.1); returns nullopt for unknown codes
std::optional<S3ErrorCode> code_from_wire(std::string_view wire);

// Standard S3 error response XML. Empty host_id omits <HostId> (the driver's fallback response has no L2 context,
// only a request id to offer) -- AWS's x-amz-id-2 equals HostId, and support tickets use it to locate the node
std::string error_xml(const S3Error& e, const std::string& request_id,
                      const std::string& host_id = "");

}  // namespace lights3::s3
