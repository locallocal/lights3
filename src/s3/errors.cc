#include "s3/errors.h"

#include "s3/xml.h"

namespace lights3::s3 {

namespace {
struct Entry {
    int status;
    const char* code;
};

Entry entry(S3ErrorCode c) {
    switch (c) {
        case S3ErrorCode::AccessDenied:                 return {403, "AccessDenied"};
        case S3ErrorCode::AuthorizationHeaderMalformed: return {400, "AuthorizationHeaderMalformed"};
        case S3ErrorCode::BucketAlreadyOwnedByYou:      return {409, "BucketAlreadyOwnedByYou"};
        case S3ErrorCode::BucketNotEmpty:               return {409, "BucketNotEmpty"};
        case S3ErrorCode::EntityTooLarge:               return {400, "EntityTooLarge"};
        case S3ErrorCode::InternalError:                return {500, "InternalError"};
        case S3ErrorCode::InvalidAccessKeyId:           return {403, "InvalidAccessKeyId"};
        case S3ErrorCode::InvalidArgument:              return {400, "InvalidArgument"};
        case S3ErrorCode::InvalidBucketName:            return {400, "InvalidBucketName"};
        case S3ErrorCode::InvalidPart:                  return {400, "InvalidPart"};
        case S3ErrorCode::InvalidRange:                 return {416, "InvalidRange"};
        case S3ErrorCode::InvalidRequest:               return {400, "InvalidRequest"};
        case S3ErrorCode::KeyTooLongError:              return {400, "KeyTooLongError"};
        case S3ErrorCode::MethodNotAllowed:             return {405, "MethodNotAllowed"};
        case S3ErrorCode::NoSuchBucket:                 return {404, "NoSuchBucket"};
        case S3ErrorCode::NoSuchKey:                    return {404, "NoSuchKey"};
        case S3ErrorCode::NoSuchUpload:                 return {404, "NoSuchUpload"};
        case S3ErrorCode::NotImplemented:               return {501, "NotImplemented"};
        case S3ErrorCode::PreconditionFailed:           return {412, "PreconditionFailed"};
        case S3ErrorCode::RequestTimeTooSkewed:         return {403, "RequestTimeTooSkewed"};
        case S3ErrorCode::SignatureDoesNotMatch:        return {403, "SignatureDoesNotMatch"};
        case S3ErrorCode::SlowDown:                     return {503, "SlowDown"};
        case S3ErrorCode::XAmzContentSHA256Mismatch:    return {400, "XAmzContentSHA256Mismatch"};
    }
    return {500, "InternalError"};
}
}  // namespace

int http_status(S3ErrorCode code) { return entry(code).status; }
const char* wire_code(S3ErrorCode code) { return entry(code).code; }

std::string error_xml(const S3Error& e, const std::string& request_id) {
    XmlWriter w;
    w.open("Error");
    w.element("Code", wire_code(e.code));
    w.element("Message", e.message);
    if (!e.resource.empty()) w.element("Resource", e.resource);
    w.element("RequestId", request_id);
    w.close();
    return w.str();
}

}  // namespace lights3::s3
