#include "storage/backend.h"

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;

void validate_bucket_name(std::string_view b) {
    auto fail = [&] {
        throw S3Error(S3ErrorCode::InvalidBucketName,
                      "The specified bucket is not valid.", std::string(b));
    };
    if (b.size() < 3 || b.size() > 63) fail();
    for (char c : b)
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.')) fail();
    if (b.front() == '-' || b.front() == '.' || b.back() == '-' || b.back() == '.') fail();
    if (b.find("..") != std::string_view::npos) fail();
}

void validate_object_key(std::string_view k) {
    if (k.empty() || k.size() > 1024)
        throw S3Error(S3ErrorCode::KeyTooLongError, "Object key is empty or too long.");
    if (k.front() == '/')
        throw S3Error(S3ErrorCode::InvalidArgument, "Object key must not start with '/'.");
    if (k.find('\0') != std::string_view::npos)
        throw S3Error(S3ErrorCode::InvalidArgument, "Object key contains NUL.");
    // 拒绝路径逃逸段与空段（LocalFs 直接映射为路径）
    size_t start = 0;
    while (start <= k.size()) {
        size_t end = k.find('/', start);
        if (end == std::string_view::npos) end = k.size();
        std::string_view seg = k.substr(start, end - start);
        if (seg.empty() || seg == "." || seg == "..")
            throw S3Error(S3ErrorCode::InvalidArgument,
                          "Object key contains invalid path segment.");
        // LocalFs 直接映射为路径，单段超过文件名上限（255B）无法落盘；
        // 统一在共享校验层拒绝，保证各后端行为一致（docs/04 §3.1）
        if (seg.size() > 255)
            throw S3Error(S3ErrorCode::KeyTooLongError,
                          "A single path segment of the key exceeds 255 bytes.");
        if (end == k.size()) break;
        start = end + 1;
    }
}

}  // namespace lights3::storage
