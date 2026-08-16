// L1: adapter helpers shared by the HTTP drivers (not part of the L1/L2 boundary; driver-internal reuse only)
#pragma once

#include <chrono>
#include <cstdio>
#include <random>
#include <string>
#include <string_view>

#include "core/log.h"
#include "core/util/time.h"
#include "core/util/uri.h"
#include "http/model.h"
#include "s3/errors.h"

namespace lights3::http::driver {

// ---------- Driver-internal buffer constants ----------
// Shutdown/backpressure bounds (drain cap, trailer cap, chunk size, shutdown
// grace) have been promoted to HttpConfig options (docs/gaps.md §7), with
// defaults consolidated in config.h; only purely internal values remain here
inline constexpr size_t kIoChunkBytes = 64 * 1024;  // Streaming read/write chunk size (default of http.io_chunk_size)
inline constexpr size_t kScratchBytes = 16 * 1024;  // Scratch buffer for draining, line parsing, etc.

// Splits the request-line target ("/a%2Fb?x=1&y") into the neutral model's
// four fields: raw_path / raw_query keep the original text (needed for SigV4),
// path / query are the decoded results (order-preserving)
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

// ---------- HTTP/1.1 message framing helpers: shared by the builtin/seastar hand-written parsers ----------

// Content-Length: 1*DIGIT; rejects empty/signs/leading whitespace/trailing garbage/overflow
// (stoull would accept "-1" wrapping to 2^64-1 and truncate "5abc" to 5 — both smuggling/hang vectors)
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

// chunk-size line: 1*HEXDIG, followed only by an optional ";ext" (extension content ignored); rejects empty/signs/whitespace/overflow
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
    if (i == 0) return false;                             // No hex digits
    if (i < line.size() && line[i] != ';') return false;  // Trailing garbage
    out = v;
    return true;
}

// Request body framing determination (RFC 9112 §6.1/§6.3). All of the
// following yield valid=false (the caller should reject the request and close
// the connection; leniently allowing any of them is a request-smuggling
// precondition):
//  - Transfer-Encoding together with Content-Length (CL.TE smuggling)
//  - Multiple TE headers, or a TE value that is not exactly "chunked" (this
//    implementation does not decode other encodings; letting it through would
//    make the chunked body get parsed as the next request line)
//  - Multiple Content-Length headers with differing values; or a non-numeric CL value
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
            if (cl && *cl != v) return f;  // Two differing lengths: framing disagreement
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

// Request id for driver fallback responses (docs/gaps.md §4): 400/500 are
// precisely the two error classes that most need log correlation, yet
// previously carried neither an x-amz-request-id header nor a RequestId in
// the XML. L2 dispatch never ran at this point, so the id can only be
// generated on the driver side (not security-sensitive, a PRNG suffices)
inline std::string fallback_request_id() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    char buf[17];
    std::snprintf(buf, sizeof(buf), "%016llX",
                  static_cast<unsigned long long>(rng()));
    return std::string(buf, 16);
}

// Contract 2 (docs/http-adapter.md §4): when an exception escapes the handler,
// every driver uniformly returns 500 + S3 InternalError XML. The log is
// written here rather than at the caller: the client only holds the request
// id, and only a single place that writes both the id and the context makes
// that slip of paper redeemable for logs — so none of the four drivers has to
// log its own copy
inline HttpResponse internal_error_response(std::string_view detail = {}) {
    s3::S3Error err(s3::S3ErrorCode::InternalError, "We encountered an internal error.");
    std::string rid = fallback_request_id();
    LOG_ERROR("handler escaped exception (request id {}): {}", rid,
              detail.empty() ? std::string_view("<no detail>") : detail);
    HttpResponse resp;
    resp.status = s3::http_status(err.code);
    resp.small_body = s3::error_xml(err, rid);
    resp.headers.set("Content-Type", "application/xml");
    resp.headers.set("x-amz-request-id", rid);
    return resp;
}

// Message framing violations (CL/TE conflict, duplicate CL, bad chunk, etc.):
// RFC 9112 §6.1 requires 400 or closing the connection. Drivers with
// hand-written parsers do both: return 400 then close, so the client does not
// read the closed connection as a truncated response
inline HttpResponse bad_request_response(const char* why) {
    s3::S3Error err(s3::S3ErrorCode::InvalidRequest, why);
    std::string rid = fallback_request_id();
    LOG_WARN("rejected malformed request (request id {}): {}", rid, why);
    HttpResponse resp;
    resp.status = s3::http_status(err.code);
    resp.small_body = s3::error_xml(err, rid);
    resp.headers.set("Content-Type", "application/xml");
    resp.headers.set("x-amz-request-id", rid);
    return resp;
}

// Error responses produced by the upstream driver itself (httplib routing/header rejections, etc.): still need an id to be traceable
inline HttpResponse upstream_error_response(s3::S3ErrorCode code, const char* why) {
    s3::S3Error err(code, why);
    std::string rid = fallback_request_id();
    LOG_WARN("rejected by the HTTP layer (request id {}): {}", rid, why);
    HttpResponse resp;
    resp.status = s3::http_status(err.code);
    resp.small_body = s3::error_xml(err, rid);
    resp.headers.set("Content-Type", "application/xml");
    resp.headers.set("x-amz-request-id", rid);
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

// Response-head rendering shared by drivers that assemble HTTP/1.1 messages
// themselves (builtin/seastar). The body form is decided uniformly here:
// fixed length uses Content-Length, streaming without a length uses chunked.
struct ResponseHead {
    std::string text;      // Status line + all headers + blank line
    bool chunked = false;  // Body must be written with chunked encoding
};

// Whether an outbound header can be written into the message as-is: the name
// must be a valid token fragment (no whitespace/colon/CR/LF), and the value
// must not contain CR/LF (otherwise a response-splitting injection surface).
// Non-conforming headers are simply dropped
inline bool header_emittable(const std::string& k, const std::string& v) {
    if (k.empty()) return false;
    for (char c : k)
        if (c == '\r' || c == '\n' || c == ':' || c == ' ' || c == '\t') return false;
    for (char c : v)
        if (c == '\r' || c == '\n') return false;
    return true;
}

// Unified outbound-header filtering (one rule set for all four drivers,
// contract 5): length/encoding/connection management is the driver's job and
// appended by each driver itself — letting these through would create framing
// holes like duplicate Content-Length; headers that cannot be safely written
// into the message (CR/LF injection surface) are dropped. set(k, v) is how
// each driver adapts to its own response object
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

// Framing headers for HEAD responses (contract 6, uniform across the four
// drivers): HEAD writes no body, therefore
//  - Length known -> write Content-Length (the value the corresponding GET
//    would return)
//  - Length unknown (streaming, no content_length) -> write **neither** and
//    close the connection. Writing Content-Length: 0 is a lie (GET does not
//    return 0 bytes; old beast/httplib behavior); writing Transfer-Encoding:
//    chunked would require chunk frames to follow, but HEAD sends no body
//    (old builtin/seastar behavior). L2's HEAD path always provides a length,
//    so this is only a fallback — but all four drivers must give the same answer
inline bool head_length_known(const HttpResponse& resp) {
    return resp.content_length.has_value() || !resp.stream_body;
}

inline ResponseHead render_response_head(const HttpResponse& resp, bool keep_alive,
                                         bool head_request = false) {
    bool no_body_status = resp.status == 204 || resp.status == 304 || resp.status < 200;
    if (head_request && !head_length_known(resp)) keep_alive = false;
    ResponseHead out;
    out.text = "HTTP/1.1 " + std::to_string(resp.status) + " " + reason_phrase(resp.status) +
               "\r\n";
    emit_headers(resp.headers, [&](const std::string& k, const std::string& v) {
        out.text += k + ": " + v + "\r\n";
    });
    if (!resp.headers.has("Date"))
        out.text += "Date: " + util::http_date(std::chrono::system_clock::now()) + "\r\n";
    if (!no_body_status) {
        if (head_request) {
            if (head_length_known(resp))
                out.text += "Content-Length: " +
                            std::to_string(resp.content_length.value_or(resp.small_body.size())) +
                            "\r\n";
        } else if (resp.stream_body && !resp.content_length) {
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
