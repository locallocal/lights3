// S3 错误码单一事实来源：code → (HTTP status, wire code)（docs/05 §5）
// L3 存储层也抛 S3Error（约定见 docs/04 §1），因此本头文件不含 HTTP 模型依赖。
#pragma once

#include <exception>
#include <optional>
#include <string>
#include <string_view>

namespace lights3::s3 {

// X-macro 单一事实来源：X(枚举名, HTTP 状态)。wire code 即枚举名字符串。
// 枚举、http_status()、wire_code()、code_from_wire() 全部由此派生——新增
// 错误码只改这一处，正反向映射不可能漂移。保持字母序。
#define LIGHTS3_S3_ERROR_CODES(X)        \
    X(AccessDenied, 403)                 \
    X(AuthorizationHeaderMalformed, 400) \
    X(BucketAlreadyOwnedByYou, 409)      \
    X(BucketNotEmpty, 409)               \
    X(EntityTooLarge, 400)               \
    X(InternalError, 500)                \
    X(InvalidAccessKeyId, 403)           \
    X(InvalidArgument, 400)              \
    X(InvalidBucketName, 400)            \
    X(InvalidPart, 400)                  \
    X(InvalidRange, 416)                 \
    X(InvalidRequest, 400)               \
    X(KeyTooLongError, 400)              \
    X(MalformedXML, 400)                 \
    X(MethodNotAllowed, 405)             \
    X(NoSuchBucket, 404)                 \
    X(NoSuchKey, 404)                    \
    X(NoSuchUpload, 404)                 \
    X(NotImplemented, 501)               \
    X(PreconditionFailed, 412)           \
    X(RequestTimeTooSkewed, 403)         \
    X(SignatureDoesNotMatch, 403)        \
    X(SlowDown, 503)                     \
    X(XAmzContentSHA256Mismatch, 400)

enum class S3ErrorCode {
#define LIGHTS3_S3_ERROR_ENUM(name, status) name,
    LIGHTS3_S3_ERROR_CODES(LIGHTS3_S3_ERROR_ENUM)
#undef LIGHTS3_S3_ERROR_ENUM
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
// wire code → enum 反查（cloudproxy 透传远端错误用，docs/09 §5.1）；未知返回 nullopt
std::optional<S3ErrorCode> code_from_wire(std::string_view wire);

// 标准 S3 错误响应 XML
std::string error_xml(const S3Error& e, const std::string& request_id);

}  // namespace lights3::s3
