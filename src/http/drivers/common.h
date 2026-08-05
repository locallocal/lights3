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
                req.query.emplace_back(util::percent_decode_query(kv), "");
            else
                req.query.emplace_back(util::percent_decode_query(kv.substr(0, eq)),
                                       util::percent_decode_query(kv.substr(eq + 1)));
        }
        start = amp + 1;
    }
}

// ---------- HTTP/1.1 消息边界（framing）辅助：builtin/seastar 手写解析器共用 ----------

// Content-Length：1*DIGIT，拒空/符号/前导空白/尾部垃圾/溢出
// （stoull 会接受 "-1" 回绕成 2^64-1、"5abc" 截成 5，都是走私/挂死向量）
inline bool parse_content_length(std::string_view s, uint64_t& out) {
    if (s.empty()) return false;
    uint64_t v = 0;
    for (char c : s) {
        if (c < '0' || c > '9') return false;
        if (v > (UINT64_MAX - static_cast<uint64_t>(c - '0')) / 10) return false;
        v = v * 10 + static_cast<uint64_t>(c - '0');
    }
    out = v;
    return true;
}

// chunk-size 行：1*HEXDIG，之后只允许 ";ext"（忽略扩展内容）；拒空/符号/空白/溢出
inline bool parse_chunk_size(std::string_view line, uint64_t& out) {
    size_t i = 0;
    uint64_t v = 0;
    for (; i < line.size(); ++i) {
        char c = line[i];
        int d;
        if (c >= '0' && c <= '9') d = c - '0';
        else if (c >= 'a' && c <= 'f') d = c - 'a' + 10;
        else if (c >= 'A' && c <= 'F') d = c - 'A' + 10;
        else break;
        if (v > (UINT64_MAX >> 4)) return false;
        v = (v << 4) | static_cast<uint64_t>(d);
    }
    if (i == 0) return false;                             // 无 hex 数字
    if (i < line.size() && line[i] != ';') return false;  // 尾部垃圾
    out = v;
    return true;
}

// 请求 body 边界判定（RFC 9112 §6.1/§6.3）。以下情形一律 valid=false（调用方
// 应拒绝请求并关连接，宽松放行任何一种都是请求走私前置条件）：
//  - Transfer-Encoding 与 Content-Length 同现（CL.TE 走私）
//  - 多个 TE 头、或 TE 值不是全等 "chunked"（本实现不解码其他编码，
//    放过则 chunked body 会被当作下一个请求行解析）
//  - 多个 Content-Length 且值不一致；或 CL 值非纯数字
struct BodyFraming {
    bool valid = false;
    bool chunked = false;
    std::optional<uint64_t> content_length;
};

inline BodyFraming parse_body_framing(const HeaderMap& headers) {
    BodyFraming f;
    std::optional<std::string> te, cl;
    int te_count = 0;
    for (auto& [k, v] : headers.items()) {
        if (HeaderMap::ieq(k, "Transfer-Encoding")) {
            ++te_count;
            te = v;
        } else if (HeaderMap::ieq(k, "Content-Length")) {
            if (cl && *cl != v) return f;  // 两个不同长度：断帧分歧
            cl = v;
        }
    }
    if (te) {
        if (cl) return f;
        if (te_count > 1 || !HeaderMap::ieq(*te, "chunked")) return f;
        f.valid = f.chunked = true;
        return f;
    }
    if (cl) {
        uint64_t v = 0;
        if (!parse_content_length(*cl, v)) return f;
        f.content_length = v;
    }
    f.valid = true;
    return f;
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

// 消息边界违规（CL/TE 冲突、重复 CL、坏 chunk 等）：RFC 9112 §6.1 要求 400 或
// 关连接。手写解析器的驱动两者都做：回 400 再关，避免客户端把关连接读成截断响应
inline HttpResponse bad_request_response(const char* why) {
    s3::S3Error err(s3::S3ErrorCode::InvalidRequest, why);
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

// 出站头能否原样写入报文：头名须是合法 token 片段（无空白/冒号/CR/LF），
// 头值不得含 CR/LF（否则是响应拆分注入面）。不合规的头直接丢弃
inline bool header_emittable(const std::string& k, const std::string& v) {
    if (k.empty()) return false;
    for (char c : k)
        if (c == '\r' || c == '\n' || c == ':' || c == ' ' || c == '\t') return false;
    for (char c : v)
        if (c == '\r' || c == '\n') return false;
    return true;
}

// 出站头统一过滤（四驱动同一套规则，契约 5）：长度/编码/连接管理是驱动的职责
// 由各驱动自行追加，放行会产生重复 Content-Length 等断帧漏洞；不可安全写入报文
// 的头（CR/LF 注入面）直接丢弃。set(k, v) 由驱动适配自己的响应对象
template <class SetFn>
inline void emit_headers(const HeaderMap& headers, SetFn&& set) {
    for (auto& [k, v] : headers.items()) {
        if (HeaderMap::ieq(k, "Content-Length") || HeaderMap::ieq(k, "Transfer-Encoding") ||
            HeaderMap::ieq(k, "Connection") || HeaderMap::ieq(k, "Keep-Alive"))
            continue;
        if (!header_emittable(k, v)) continue;
        set(k, v);
    }
}

inline ResponseHead render_response_head(const HttpResponse& resp, bool keep_alive) {
    bool no_body_status = resp.status == 204 || resp.status == 304 || resp.status < 200;
    ResponseHead out;
    out.text = "HTTP/1.1 " + std::to_string(resp.status) + " " + reason_phrase(resp.status) +
               "\r\n";
    emit_headers(resp.headers, [&](const std::string& k, const std::string& v) {
        out.text += k + ": " + v + "\r\n";
    });
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
