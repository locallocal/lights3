// handler 间共享的小工具（仅 handlers/ 内部使用）
#pragma once

#include <chrono>
#include <string>
#include <utility>

#include "core/task.h"
#include "core/util/time.h"
#include "core/util/uri.h"
#include "http/model.h"
#include "s3/errors.h"
#include "storage/backend.h"

namespace lights3::s3::handlers {

inline std::string quote_etag(const std::string& etag) { return "\"" + etag + "\""; }

// 单一所有者：本实现无多租户 owner 概念，ListAllMyBuckets 与 ListObjectsV2 的
// fetch-owner 用同一个身份，免得两处各写一份字面量后漂移
inline constexpr std::string_view kOwnerId = "lights3";

inline std::string strip_quotes(std::string s) {
    if (s.size() >= 2 && s.front() == '"' && s.back() == '"') return s.substr(1, s.size() - 2);
    return s;
}

// TSV 与头部都以行为单位：元数据值里的 CR/LF 会撕开 sidecar 记录，也是响应头
// 注入面。一等字段与 user-meta 同样拒收（docs/gaps.md §5.2）
inline void reject_control_chars(std::string_view name, const std::string& v) {
    if (v.find('\n') != std::string::npos || v.find('\r') != std::string::npos)
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Header '" + std::string(name) + "' must not contain line breaks.");
}

// PutObject / CreateMultipartUpload 共用：提取 Content-Type、x-amz-meta-*，以及
// 六个 S3 一等元数据字段（docs/gaps.md §5.2）
inline storage::ObjectMeta meta_from_headers(const http::HttpRequest& req) {
    storage::ObjectMeta meta;
    if (auto ct = req.headers.get("Content-Type")) meta.content_type = *ct;
    for (auto& f : storage::kStdMetaFields) {
        if (auto v = req.headers.get(f.header)) {
            reject_control_chars(f.header, *v);
            meta.*f.field = *v;
        }
    }
    for (auto& [k, v] : req.headers.items()) {
        std::string lk;
        for (char c : k) lk.push_back(http::HeaderMap::lower(c));
        if (lk.rfind("x-amz-meta-", 0) == 0) {
            reject_control_chars(k, v);
            meta.user_meta[lk.substr(11)] = v;
        }
    }
    return meta;
}

// HTTP 时间头按秒粒度比较（Last-Modified 序列化即秒精度）
inline int64_t to_epoch_sec(util::SysTime t) {
    return std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count();
}

// CopyObject / UploadPartCopy 共用的源条件（x-amz-copy-source-if-*）：任一不满足即 412
inline void check_copy_preconditions(const http::HttpRequest& req,
                                     const storage::ObjectMeta& src) {
    auto fail = [] {
        throw S3Error(S3ErrorCode::PreconditionFailed,
                      "At least one of the pre-conditions you specified did not hold");
    };
    if (auto v = req.headers.get("x-amz-copy-source-if-match"))
        if (strip_quotes(*v) != src.etag) fail();
    if (auto v = req.headers.get("x-amz-copy-source-if-none-match"))
        if (strip_quotes(*v) == src.etag) fail();
    if (auto v = req.headers.get("x-amz-copy-source-if-unmodified-since")) {
        auto t = util::parse_http_date(*v);
        if (t && to_epoch_sec(src.last_modified) > to_epoch_sec(*t)) fail();
    }
    if (auto v = req.headers.get("x-amz-copy-source-if-modified-since")) {
        auto t = util::parse_http_date(*v);
        if (t && to_epoch_sec(src.last_modified) <= to_epoch_sec(*t)) fail();
    }
}

// "x-amz-copy-source: [/]bucket/key"（percent-encoded）；?versionId → NotImplemented
inline std::pair<std::string, std::string> parse_copy_source(const std::string& raw) {
    std::string s = raw;
    if (auto q = s.find('?'); q != std::string::npos) {
        if (s.find("versionId=", q) != std::string::npos)
            throw S3Error(S3ErrorCode::NotImplemented, "Versioning is not implemented.");
        s.resize(q);
    }
    s = util::percent_decode(s);
    if (!s.empty() && s.front() == '/') s.erase(0, 1);
    auto slash = s.find('/');
    if (slash == std::string::npos || slash == 0 || slash + 1 >= s.size())
        throw S3Error(S3ErrorCode::InvalidArgument, "Invalid x-amz-copy-source header.");
    std::string bucket = s.substr(0, slash);
    // copy-source 走 header，不经 dispatch 的 bucket 闸门，须在此独立校验——否则
    // CopyObject 能把 .sys 里的凭证对象拷进用户可读的对象。用与 dispatch 同一个
    // 校验函数（此前是第三份独立的 '.' 前缀启发式，三份副本各自演化是漂移源头）
    storage::validate_bucket_name(bucket);
    return {std::move(bucket), s.substr(slash + 1)};
}

// 读整个请求体（XML 请求限 1MiB，docs/s3-protocol.md §4）；超限抛 MalformedXML
inline Task<std::string> read_body(http::HttpRequest& req, size_t max_size = 1024 * 1024) {
    std::string out;
    if (!req.body) co_return out;
    std::byte buf[16 * 1024];
    for (;;) {
        size_t n = co_await req.body->read(std::span(buf));
        if (n == 0) break;
        if (out.size() + n > max_size)
            throw S3Error(S3ErrorCode::MalformedXML, "Request body exceeds the size limit.");
        out.append(reinterpret_cast<const char*>(buf), n);
    }
    co_return out;
}

}  // namespace lights3::s3::handlers
