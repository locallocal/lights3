// Small utilities shared between handlers (internal to handlers/ only)
#pragma once

#include <chrono>
#include <memory>
#include <optional>
#include <string>
#include <utility>

#include "core/task.h"
#include "core/util/time.h"
#include "core/util/uri.h"
#include "http/model.h"
#include "s3/checksum_guard.h"
#include "s3/errors.h"
#include "storage/backend.h"

namespace lights3::s3::handlers {

inline std::string quote_etag(const std::string& etag) { return "\"" + etag + "\""; }

// Single owner: this implementation has no multi-tenant owner concept; ListAllMyBuckets and ListObjectsV2's
// fetch-owner use the same identity, so two hand-written literals cannot drift apart
inline constexpr std::string_view kOwnerId = "lights3";

inline std::string strip_quotes(std::string s) {
    if (s.size() >= 2 && s.front() == '"' && s.back() == '"') return s.substr(1, s.size() - 2);
    return s;
}

// Both TSV and headers are line-oriented: CR/LF in metadata values would tear sidecar records apart, and is
// also a response-header injection surface. First-class fields are rejected just like user-meta (docs/archive/gaps.md §5.2)
inline void reject_control_chars(std::string_view name, const std::string& v) {
    if (v.find('\n') != std::string::npos || v.find('\r') != std::string::npos)
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Header '" + std::string(name) + "' must not contain line breaks.");
}

// Shared by PutObject / CreateMultipartUpload: extracts Content-Type, x-amz-meta-*, and
// the six first-class S3 metadata fields (docs/archive/gaps.md §5.2)
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
    // AWS constraint on the redirect target (docs/static-website.md phase ③): the value is
    // served verbatim as a Location header on the anonymous website plane, so free-form
    // schemes (javascript:, data:) must never get in
    if (!meta.website_redirect.empty() && meta.website_redirect.front() != '/' &&
        meta.website_redirect.rfind("http://", 0) != 0 &&
        meta.website_redirect.rfind("https://", 0) != 0)
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "x-amz-website-redirect-location must start with '/', 'http://' or "
                      "'https://'.");
    return meta;
}

// True when the request carries any response-* override parameter (docs/archive/gaps.md §5.3;
// the list lives next to apply_response_overrides in objects.cc). dispatch uses this to
// refuse overrides on anonymous website reads — on a public bucket a crafted link could
// otherwise hang an arbitrary Content-Disposition off the bucket's domain.
bool has_response_override(const http::HttpRequest& req);

// HTTP time headers compare at second granularity (Last-Modified serializes at second precision)
inline int64_t to_epoch_sec(util::SysTime t) {
    return std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count();
}

// Source conditions shared by CopyObject / UploadPartCopy (x-amz-copy-source-if-*): any unmet condition yields 412
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
    // copy-source arrives via header and bypasses dispatch's bucket gate, so it must be validated here independently --
    // otherwise CopyObject could copy credential objects from .sys into user-readable objects. Uses the same
    // validation function as dispatch (previously a third independent '.'-prefix heuristic; three copies evolving separately was a drift source)
    storage::validate_bucket_name(bucket);
    return {std::move(bucket), s.substr(slash + 1)};
}

// Attach the request's declared checksum to the outgoing meta (roadmap §2.2): the
// header form fills the value up front (verification happens at body EOF, before any
// backend commit); the trailer form installs a DigestCaptureReader plus a pending slot
// the backend serializes once the body drains
inline void attach_request_checksum(http::HttpRequest& req, storage::ObjectMeta& meta) {
    auto rc = request_checksum(req);
    if (!rc) return;
    meta.checksum_algorithm = checksum_wire_name(*rc->spec);
    meta.checksum_type = "FULL_OBJECT";
    if (!rc->trailer) {
        meta.checksum_value = rc->value;
        return;
    }
    auto slot = std::make_shared<std::string>();
    meta.checksum_pending = slot;
    if (!req.body) req.body = std::make_unique<http::StringBodyReader>("");
    req.body =
        std::make_unique<DigestCaptureReader>(std::move(req.body), rc->spec->algo, slot);
}

// UploadPart variant: same extraction, result travels as the upload_part checksum
// parameter and lands in the part record
inline std::optional<storage::PartChecksum> extract_part_checksum(http::HttpRequest& req) {
    auto rc = request_checksum(req);
    if (!rc) return std::nullopt;
    storage::PartChecksum pc;
    pc.algorithm = checksum_wire_name(*rc->spec);
    if (!rc->trailer) {
        pc.value = rc->value;
        return pc;
    }
    auto slot = std::make_shared<std::string>();
    pc.pending = slot;
    if (!req.body) req.body = std::make_unique<http::StringBodyReader>("");
    req.body =
        std::make_unique<DigestCaptureReader>(std::move(req.body), rc->spec->algo, slot);
    return pc;
}

// GET/HEAD checksum echo (roadmap §2.2): only under an explicit
// x-amz-checksum-mode: ENABLED, and only for full-object responses (AWS omits the
// checksum on ranged GETs — a full-object digest against partial bytes would mislead)
inline void apply_checksum_echo(const http::HttpRequest& req, const storage::ObjectMeta& meta,
                                http::HttpResponse& resp) {
    if (resp.status == 206) return;
    auto v = req.headers.get("x-amz-checksum-mode");
    if (!v || !http::HeaderMap::ieq(*v, "ENABLED")) return;
    if (meta.checksum_value.empty() || meta.checksum_algorithm.empty()) return;
    std::string h = "x-amz-checksum-";
    for (char c : meta.checksum_algorithm) h.push_back(http::HeaderMap::lower(c));
    resp.headers.set(h, meta.checksum_value);
    resp.headers.set("x-amz-checksum-type",
                     meta.checksum_type.empty() ? "FULL_OBJECT" : meta.checksum_type);
}

// PutObject/UploadPart require a request framing that carries a body (roadmap §2.5): a PUT
// with neither Content-Length nor Transfer-Encoding has no body per RFC 9112 §6.3 --
// silently committing an empty object out of a client bug is data loss, 411 is the honest
// answer (MissingContentLength was previously dead code in the error table)
inline void require_content_length(const http::HttpRequest& req) {
    if (!req.headers.has("Content-Length") && !req.headers.has("Transfer-Encoding"))
        throw S3Error(S3ErrorCode::MissingContentLength,
                      "You must provide the Content-Length HTTP header.");
}

// Read the entire request body (XML requests capped at 1MiB, docs/s3-protocol.md §4); over the cap throws MalformedXML
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
