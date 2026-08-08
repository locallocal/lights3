// S3 错误码单一事实来源：code → (HTTP status, wire code)（docs/s3-protocol.md §5）
// L3 存储层也抛 S3Error（约定见 docs/storage-backend.md §1），因此本头文件不含 HTTP 模型依赖。
#pragma once

#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

namespace lights3::s3 {

// X-macro 单一事实来源：X(枚举名, HTTP 状态)。wire code 即枚举名字符串。
// 枚举、http_status()、wire_code()、code_from_wire() 全部由此派生——新增
// 错误码只改这一处，正反向映射不可能漂移。保持字母序。
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

// 码表大小（与枚举同源）：错误计数用定长原子数组按枚举值下标，免锁
inline constexpr size_t kS3ErrorCodeCount = 0
#define LIGHTS3_S3_ERROR_COUNT(name, status) +1
    LIGHTS3_S3_ERROR_CODES(LIGHTS3_S3_ERROR_COUNT)
#undef LIGHTS3_S3_ERROR_COUNT
    ;

struct S3Error : std::exception {
    S3ErrorCode code;
    std::string message;
    std::string resource;
    // 少数错误按 RFC/S3 规定必须带响应头才算完整：405 的 Allow（RFC 9110 §15.5.6）、
    // 重定向类的 x-amz-bucket-region。放在异常上，L2 统一渲染时贴回响应
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
// wire code → enum 反查（cloudproxy 透传远端错误用，docs/cloudproxy-backend.md §5.1）；未知返回 nullopt
std::optional<S3ErrorCode> code_from_wire(std::string_view wire);

// 标准 S3 错误响应 XML。host_id 为空则省略 <HostId>（驱动兜底响应无 L2 上下文，
// 只有 request id 可给）——AWS 的 x-amz-id-2 与 HostId 同值，支持工单据此定位节点
std::string error_xml(const S3Error& e, const std::string& request_id,
                      const std::string& host_id = "");

}  // namespace lights3::s3
