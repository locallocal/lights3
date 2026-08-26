#include "storage/backend.h"

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

// Four decimal segments like 192.168.0.1 -- AWS explicitly rejects these (they would be
// confused with IP endpoints under path-style addressing). Only recognize "four segments,
// each 1-3 digits and <=255"; no leniency beyond leading zeros
bool looks_like_ipv4(std::string_view b) {
    int segs = 0;
    size_t pos = 0;
    while (pos <= b.size()) {
        size_t dot = b.find('.', pos);
        if (dot == std::string_view::npos) dot = b.size();
        std::string_view s = b.substr(pos, dot - pos);
        if (s.empty() || s.size() > 3) return false;
        unsigned v = 0;
        for (char c : s) {
            if (c < '0' || c > '9') return false;
            v = v * 10 + unsigned(c - '0');
        }
        if (v > 255) return false;
        ++segs;
        if (dot == b.size()) break;
        pos = dot + 1;
    }
    return segs == 4;
}

}  // namespace

void validate_bucket_name(std::string_view b, bool allow_reserved) {
    auto fail = [&] {
        throw S3Error(S3ErrorCode::InvalidBucketName,
                      "The specified bucket is not valid.", std::string(b));
    };
    // Internal reserved bucket (docs/credential-management.md §4.1, credential
    // persistence): only callers that explicitly pass allow_reserved (CredentialStore) may
    // use it. Previously this was unconditionally admitted, i.e. the backend layer offered
    // no protection at all for the reserved name and relied entirely on L2's '.'-prefix
    // heuristic -- a defense that vhost addressing can bypass (the bucket is determined
    // entirely by Host)
    if (b == kSysBucketName) {
        if (allow_reserved) return;
        fail();
    }
    if (b.size() < 3 || b.size() > 63) fail();
    for (char c : b)
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.')) fail();
    if (b.front() == '-' || b.front() == '.' || b.back() == '-' || b.back() == '.') fail();
    if (b.find("..") != std::string_view::npos) fail();
    // The following three rules were the previously missing parts of AWS naming rules
    // (docs/archive/gaps.md §6.3). They are not pedantry: buckets admitted here could not be
    // created on real S3, so the "get it working on lights3 first, migrate to S3 later"
    // path would break at the very last moment
    if (b.find(".-") != std::string_view::npos || b.find("-.") != std::string_view::npos) fail();
    if (looks_like_ipv4(b)) fail();
    // Reserved prefixes/suffixes: xn-- is IDNA punycode, sthree-* and amzn-s3-demo-* are
    // for AWS internal use, -s3alias / --ol-s3 / --x-s3 / .mrap are alias suffixes of
    // access points and multi-region access points
    static constexpr std::string_view kReservedPrefixes[] = {"xn--", "sthree-",
                                                             "amzn-s3-demo-"};
    static constexpr std::string_view kReservedSuffixes[] = {"-s3alias", "--ol-s3", "--x-s3",
                                                             ".mrap"};
    for (auto p : kReservedPrefixes)
        if (b.size() >= p.size() && b.compare(0, p.size(), p) == 0) fail();
    for (auto s : kReservedSuffixes)
        if (b.size() >= s.size() && b.compare(b.size() - s.size(), s.size(), s) == 0) fail();
}

// The shared validation layer keeps only the AWS constraints that hold for every backend
// (docs/archive/gaps.md §6.3): previously it also rejected the AWS-legal leading '/', empty
// segments ("a//b", directory marker "folder/"), and the 255B single-segment cap -- three
// rules born from localfs's path mapping that stripped memory/duostore/cloudproxy of
// compatibility along with it (the S3 console's "create folder" and the directory
// semantics of s3fs/goofys/rclone all depend on "folder/").
// They have been pushed down into validate_fs_object_key, called only by backends that map
// keys directly to paths.
// '.' and '..' segments are the exception and stay in the shared layer: they are not just
// a local-path problem -- forwarding backends splice the key into a URL path, and RFC
// 3986's remove_dot_segments lets any proxy/server along the way normalize "a/./b" into
// "a/b", rewriting the object's identity itself
void validate_object_key(std::string_view k) {
    if (k.empty() || k.size() > 1024)
        throw S3Error(S3ErrorCode::KeyTooLongError, "Object key is empty or too long.");
    // Reject control characters (including NUL): XML 1.0 cannot represent 0x00-0x1F (except
    // \t\n\r) even with numeric entities, so a single such object would make the entire
    // ListObjects response unparseable to a conforming parser
    for (unsigned char c : k)
        if (c < 0x20 || c == 0x7f)
            throw S3Error(S3ErrorCode::InvalidArgument,
                          "Object key contains control characters.");
    size_t start = 0;
    while (start <= k.size()) {
        size_t end = k.find('/', start);
        if (end == std::string_view::npos) end = k.size();
        std::string_view seg = k.substr(start, end - start);
        if (seg == "." || seg == "..")
            throw S3Error(S3ErrorCode::InvalidArgument,
                          "Object key must not contain '.' or '..' path segments.");
        if (end == k.size()) break;
        start = end + 1;
    }
}

// Additional constraints for path-mapping backends (localfs / xlocalfs). The keys the
// shared layer admits cannot land on disk here: an empty segment has no corresponding file
// name, a leading '/' escapes the bucket directory, and a segment beyond NAME_MAX fails
// with ENAMETOOLONG. **A trailing '/' is the only exception** -- directory-marker objects
// are represented by localfs with a reserved marker file inside the directory
// (fsutil::kDirMarker); this is genuine support, not admitting them only to fail later
void validate_fs_object_key(std::string_view k) {
    if (k.front() == '/')
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Object key must not start with '/' on a filesystem backend.");
    // Strip the trailing '/' first, then check segment by segment: something must remain
    // after stripping (a key of exactly "/" is already blocked by the previous rule)
    std::string_view body = k.ends_with('/') ? k.substr(0, k.size() - 1) : k;
    if (body.empty())
        throw S3Error(S3ErrorCode::InvalidArgument, "Object key must not be only '/'.");
    size_t start = 0;
    while (start <= body.size()) {
        size_t end = body.find('/', start);
        if (end == std::string_view::npos) end = body.size();
        std::string_view seg = body.substr(start, end - start);
        if (seg.empty())
            throw S3Error(S3ErrorCode::InvalidArgument,
                          "Object key contains an empty path segment.");
        // A single segment beyond the file-name limit (255B) cannot land on disk
        // (docs/storage-backend.md §3.1)
        if (seg.size() > 255)
            throw S3Error(S3ErrorCode::KeyTooLongError,
                          "A single path segment of the key exceeds 255 bytes.");
        if (end == body.size()) break;
        start = end + 1;
    }
}

}  // namespace lights3::storage
