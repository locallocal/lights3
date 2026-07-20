// L1: 各 HTTP 驱动共享的适配辅助（不属于 L1/L2 边界，仅驱动内部复用）
#pragma once

#include <chrono>
#include <string>
#include <string_view>

#include "core/util/time.h"
#include "core/util/uri.h"
#include "http/model.h"
#include "s3/errors.h"

namespace lights3::http::driver {

// 把请求行的 target（"/a%2Fb?x=1&y"）拆成中立模型的四个字段：
// raw_path / raw_query 保留原文（SigV4 需要），path / query 为解码结果（保序）
inline void parse_target(std::string_view target, HttpRequest& req) {
    auto qpos = target.find('?');
    req.raw_path = std::string(qpos == std::string_view::npos ? target : target.substr(0, qpos));
    req.raw_query = qpos == std::string_view::npos ? "" : std::string(target.substr(qpos + 1));
    req.path = util::percent_decode(req.raw_path);
    size_t start = 0;
    while (start < req.raw_query.size()) {
        auto amp = req.raw_query.find('&', start);
        if (amp == std::string::npos) amp = req.raw_query.size();
        std::string kv = req.raw_query.substr(start, amp - start);
        if (!kv.empty()) {
            auto eq = kv.find('=');
            if (eq == std::string::npos)
                req.query.emplace_back(util::percent_decode(kv), "");
            else
                req.query.emplace_back(util::percent_decode(kv.substr(0, eq)),
                                       util::percent_decode(kv.substr(eq + 1)));
        }
        start = amp + 1;
    }
}

// 契约 2（docs/http-adapter.md §4）：handler 逃逸异常时驱动统一回 500 + S3 InternalError XML
inline HttpResponse internal_error_response() {
    s3::S3Error err(s3::S3ErrorCode::InternalError, "We encountered an internal error.");
    HttpResponse resp;
    resp.status = s3::http_status(err.code);
    resp.small_body = s3::error_xml(err, "");
    resp.headers.set("Content-Type", "application/xml");
    return resp;
}

inline const char* reason_phrase(int status) {
    switch (status) {
        case 100: return "Continue";
        case 200: return "OK";
        case 204: return "No Content";
        case 206: return "Partial Content";
        case 304: return "Not Modified";
        case 400: return "Bad Request";
        case 403: return "Forbidden";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 409: return "Conflict";
        case 412: return "Precondition Failed";
        case 416: return "Range Not Satisfiable";
        case 500: return "Internal Server Error";
        case 501: return "Not Implemented";
        case 503: return "Service Unavailable";
        default: return "Unknown";
    }
}

// 自己拼 HTTP/1.1 报文的驱动（builtin/seastar）共用的响应头渲染。
// body 形态在此统一决定：定长走 Content-Length，流式无长度走 chunked。
struct ResponseHead {
    std::string text;      // 状态行 + 全部头部 + 空行
    bool chunked = false;  // body 需按 chunked 编码写出
};

inline ResponseHead render_response_head(const HttpResponse& resp, bool keep_alive) {
    bool no_body_status = resp.status == 204 || resp.status == 304 || resp.status < 200;
    ResponseHead out;
    out.text = "HTTP/1.1 " + std::to_string(resp.status) + " " + reason_phrase(resp.status) +
               "\r\n";
    for (auto& [k, v] : resp.headers.items()) out.text += k + ": " + v + "\r\n";
    if (!resp.headers.has("Date"))
        out.text += "Date: " + util::http_date(std::chrono::system_clock::now()) + "\r\n";
    if (!no_body_status) {
        if (resp.stream_body && !resp.content_length) {
            out.chunked = true;
            out.text += "Transfer-Encoding: chunked\r\n";
        } else {
            uint64_t len = resp.content_length.value_or(resp.small_body.size());
            out.text += "Content-Length: " + std::to_string(len) + "\r\n";
        }
    }
    out.text += keep_alive ? "Connection: keep-alive\r\n" : "Connection: close\r\n";
    out.text += "\r\n";
    return out;
}

}  // namespace lights3::http::driver
