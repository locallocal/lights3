// S3 错误码单一事实来源：code → (HTTP status, wire code)（docs/05 §5）
// L3 存储层也抛 S3Error（约定见 docs/04 §1），因此本头文件不含 HTTP 模型依赖。
#pragma once

#include <exception>
#include <string>

namespace lights3::s3 {

enum class S3ErrorCode {
    AccessDenied,
    AuthorizationHeaderMalformed,
    BucketAlreadyOwnedByYou,
    BucketNotEmpty,
    EntityTooLarge,
    InternalError,
    InvalidAccessKeyId,
    InvalidArgument,
    InvalidBucketName,
    InvalidPart,
    InvalidRange,
    InvalidRequest,
    KeyTooLongError,
    MethodNotAllowed,
    NoSuchBucket,
    NoSuchKey,
    NoSuchUpload,
    NotImplemented,
    PreconditionFailed,
    RequestTimeTooSkewed,
    SignatureDoesNotMatch,
    SlowDown,
    XAmzContentSHA256Mismatch,
};

struct S3Error : std::exception {
    S3ErrorCode code;
    std::string message;
    std::string resource;

    S3Error(S3ErrorCode c, std::string msg, std::string res = "")
        : code(c), message(std::move(msg)), resource(std::move(res)) {}
    const char* what() const noexcept override { return message.c_str(); }
};

int http_status(S3ErrorCode code);
const char* wire_code(S3ErrorCode code);

// 标准 S3 错误响应 XML
std::string error_xml(const S3Error& e, const std::string& request_id);

}  // namespace lights3::s3
