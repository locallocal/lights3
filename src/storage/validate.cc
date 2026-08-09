#include "storage/backend.h"

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

// 形如 192.168.0.1 的四段十进制——AWS 明确拒绝（会与 path-style 寻址的 IP 端点
// 混淆）。只认"四段、每段 1–3 位数字且 ≤255"，不做前导零之外的宽松处理
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
    // 内部保留 bucket（docs/credential-management.md §4.1，凭证持久化）：只有显式
    // 传 allow_reserved 的调用方（CredentialStore）能用。此前是无条件放行，等于
    // 后端层对保留名毫无保护，全指望 L2 的 '.' 前缀启发式——而那道防线在 vhost
    // 寻址下可被绕过（bucket 完全由 Host 决定）
    if (b == kSysBucketName) {
        if (allow_reserved) return;
        fail();
    }
    if (b.size() < 3 || b.size() > 63) fail();
    for (char c : b)
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.')) fail();
    if (b.front() == '-' || b.front() == '.' || b.back() == '-' || b.back() == '.') fail();
    if (b.find("..") != std::string_view::npos) fail();
    // 以下三条是 AWS 命名规则里此前漏掉的部分（docs/gaps.md §6.3）。它们不是
    // 洁癖：放行后建出的桶在真实 S3 上创建不了，"先在 lights3 上跑通、再迁到
    // S3"的迁移路径会在最后一刻断掉
    if (b.find(".-") != std::string_view::npos || b.find("-.") != std::string_view::npos) fail();
    if (looks_like_ipv4(b)) fail();
    // 保留前后缀：xn-- 是 IDNA punycode，sthree-* 与 amzn-s3-demo-* 归 AWS 内部用，
    // -s3alias / --ol-s3 / --x-s3 / .mrap 是接入点与多区域接入点的别名后缀
    static constexpr std::string_view kReservedPrefixes[] = {"xn--", "sthree-",
                                                             "amzn-s3-demo-"};
    static constexpr std::string_view kReservedSuffixes[] = {"-s3alias", "--ol-s3", "--x-s3",
                                                             ".mrap"};
    for (auto p : kReservedPrefixes)
        if (b.size() >= p.size() && b.compare(0, p.size(), p) == 0) fail();
    for (auto s : kReservedSuffixes)
        if (b.size() >= s.size() && b.compare(b.size() - s.size(), s.size(), s) == 0) fail();
}

// 共享校验层只保留 AWS 对所有后端都成立的约束（docs/gaps.md §6.3）：此前这里还
// 拒绝了 AWS 合法的前导 '/'、空段（"a//b"、目录标记 "folder/"）与 255B 单段上限
// ——那三条源自 localfs 的路径映射，却让 memory/duostore/cloudproxy 一并丧失了
// 兼容性（S3 控制台"新建文件夹"、s3fs/goofys/rclone 的目录语义都依赖 "folder/"）。
// 已下沉为 validate_fs_object_key，只有直接把 key 映射成路径的后端才调。
// '.' 与 '..' 段是例外，留在共享层：它们不只是本地路径问题——转发型后端把 key
// 拼进 URL 路径，RFC 3986 的 remove_dot_segments 会让沿途任一代理/服务端把
// "a/./b" 归一成 "a/b"，改写的是对象身份本身
void validate_object_key(std::string_view k) {
    if (k.empty() || k.size() > 1024)
        throw S3Error(S3ErrorCode::KeyTooLongError, "Object key is empty or too long.");
    // 控制字符（含 NUL）拒绝：XML 1.0 连数值实体都无法表示 0x00–0x1F（除 \t\n\r），
    // 允许的话一个对象就能让整个 ListObjects 响应对合规解析器不可解析
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

// 路径映射型后端（localfs / xlocalfs）的补充约束。共享层放行的那几种 key 在这里
// 无法落盘：空段没有对应的文件名、前导 '/' 会跳出 bucket 目录、单段超过 NAME_MAX
// 直接 ENAMETOOLONG。**末尾 '/' 是唯一的例外**——目录标记对象由 localfs 以目录内
// 的保留标记文件承载（fsutil::kDirMarker），是真正支持而不是放行后出错
void validate_fs_object_key(std::string_view k) {
    if (k.front() == '/')
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Object key must not start with '/' on a filesystem backend.");
    // 末尾 '/' 先剥掉再逐段检查：剥掉之后不能什么都不剩（key 恰为 "/" 已被上一条挡）
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
        // 单段超过文件名上限（255B）无法落盘（docs/storage-backend.md §3.1）
        if (seg.size() > 255)
            throw S3Error(S3ErrorCode::KeyTooLongError,
                          "A single path segment of the key exceeds 255 bytes.");
        if (end == body.size()) break;
        start = end + 1;
    }
}

}  // namespace lights3::storage
