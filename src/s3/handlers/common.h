// handler 间共享的小工具（仅 handlers/ 内部使用）
#pragma once

#include <string>

#include "core/task.h"
#include "http/model.h"
#include "s3/errors.h"
#include "storage/backend.h"

namespace lights3::s3::handlers {

inline std::string quote_etag(const std::string& etag) { return "\"" + etag + "\""; }

inline std::string strip_quotes(std::string s) {
    if (s.size() >= 2 && s.front() == '"' && s.back() == '"') return s.substr(1, s.size() - 2);
    return s;
}

// PutObject / CreateMultipartUpload 共用：从请求头提取 Content-Type 与 x-amz-meta-*
inline storage::ObjectMeta meta_from_headers(const http::HttpRequest& req) {
    storage::ObjectMeta meta;
    if (auto ct = req.headers.get("Content-Type")) meta.content_type = *ct;
    for (auto& [k, v] : req.headers.items()) {
        std::string lk;
        for (char c : k) lk.push_back(http::HeaderMap::lower(c));
        if (lk.rfind("x-amz-meta-", 0) == 0) meta.user_meta[lk.substr(11)] = v;
    }
    return meta;
}

// 读整个请求体（XML 请求限 1MiB，docs/05 §4）；超限抛 MalformedXML
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
