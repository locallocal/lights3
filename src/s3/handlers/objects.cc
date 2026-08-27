// Object-level handlers: Put/Get/Head/Delete/Copy/DeleteObjects and conditional requests (docs/s3-protocol.md §1/§6)
#include <charconv>

#include "core/log.h"
#include "core/util/time.h"
#include "core/util/uri.h"
#include "s3/handlers/common.h"
#include "s3/service.h"
#include "s3/xml.h"

namespace lights3::s3 {

using namespace handlers;

namespace {

// "bytes=a-b" / "bytes=a-" / "bytes=-n"; ignored when malformed (S3 behavior)
std::optional<storage::ByteRange> parse_range_header(const std::string& v) {
    if (v.rfind("bytes=", 0) != 0) return std::nullopt;
    std::string spec = v.substr(6);
    if (spec.find(',') != std::string::npos) return std::nullopt;  // multi-range not supported
    auto dash = spec.find('-');
    if (dash == std::string::npos) return std::nullopt;
    auto to_u64 = [](std::string_view s) -> std::optional<uint64_t> {
        uint64_t out = 0;
        auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
        if (ec != std::errc() || p != s.data() + s.size()) return std::nullopt;
        return out;
    };
    storage::ByteRange r;
    std::string_view a = std::string_view(spec).substr(0, dash);
    std::string_view b = std::string_view(spec).substr(dash + 1);
    if (a.empty() && b.empty()) return std::nullopt;
    if (!a.empty()) {
        r.first = to_u64(a);
        if (!r.first) return std::nullopt;
    }
    if (!b.empty()) {
        r.last = to_u64(b);
        if (!r.last) return std::nullopt;
    }
    // "bytes=5-3" is syntactically invalid (RFC 9110 §14.1.1 requires last >= first): the whole header is
    // ignored as invalid and 200 with the full object returned -- previously it fell into resolve_range and became 416 (docs/archive/gaps.md §4)
    if (r.first && r.last && *r.last < *r.first) return std::nullopt;
    return r;
}

// response-* override parameters (docs/archive/gaps.md §5.3): the most common use in presigned download links is
// response-content-disposition ("clicking downloads it under this filename").
// Premise: AWS honors these only for authenticated requests -- if anonymously readable objects allowed overrides,
// a single link could hang an arbitrary Content-Disposition off the bucket's domain. With auth enabled here, any
// request reaching a handler has already been verified; with auth disabled, that boundary does not exist. So no
// extra identity check is needed, but values must be filtered for CR/LF:
// query values are attacker-controlled, and inserting them straight into response headers is response splitting
struct ResponseOverride {
    const char* param;
    const char* header;
};
constexpr ResponseOverride kResponseOverrides[] = {
    {"response-content-type", "Content-Type"},
    {"response-content-language", "Content-Language"},
    {"response-expires", "Expires"},
    {"response-cache-control", "Cache-Control"},
    {"response-content-disposition", "Content-Disposition"},
    {"response-content-encoding", "Content-Encoding"},
};

void apply_response_overrides(const http::HttpRequest& req, http::HttpResponse& resp) {
    for (auto& o : kResponseOverrides) {
        if (auto v = req.query_get(o.param)) {
            reject_control_chars(o.param, *v);
            resp.headers.set(o.header, *v);
        }
    }
}

// Header set for 304 (RFC 9110 §15.4.5, docs/archive/gaps.md §5.9): cache-validation headers that a 200 would send
// must also be sent on 304, otherwise clients refreshing a cache entry drop the Last-Modified
void fill_not_modified_headers(http::HttpResponse& resp, const storage::ObjectMeta& meta) {
    resp.status = 304;
    resp.headers.set("ETag", quote_etag(meta.etag));
    resp.headers.set("Last-Modified", util::http_date(meta.last_modified));
    if (!meta.cache_control.empty()) resp.headers.set("Cache-Control", meta.cache_control);
}

void fill_object_headers(http::HttpResponse& resp, const storage::ObjectMeta& meta) {
    resp.headers.set("ETag", quote_etag(meta.etag));
    resp.headers.set("Content-Type", meta.content_type);
    resp.headers.set("Last-Modified", util::http_date(meta.last_modified));
    resp.headers.set("Accept-Ranges", "bytes");
    // First-class metadata echoed verbatim (docs/archive/gaps.md §5.2); empty = unset, header not sent
    for (auto& f : storage::kStdMetaFields)
        if (!(meta.*f.field).empty()) resp.headers.set(f.header, meta.*f.field);
    for (auto& [k, v] : meta.user_meta) resp.headers.set("x-amz-meta-" + k, v);
}

// GET/HEAD conditional requests (docs/s3-protocol.md §6, precedence follows RFC 7232:
// If-Match > If-Unmodified-Since；If-None-Match > If-Modified-Since）
void check_read_preconditions(const http::HttpRequest& req, const storage::ObjectMeta& meta,
                              bool& not_modified) {
    if (auto v = req.headers.get("If-Match")) {
        if (*v != "*" && strip_quotes(*v) != meta.etag)
            throw S3Error(S3ErrorCode::PreconditionFailed,
                          "At least one of the pre-conditions you specified did not hold");
    } else if (auto ius = req.headers.get("If-Unmodified-Since")) {
        auto t = util::parse_http_date(*ius);
        if (t && to_epoch_sec(meta.last_modified) > to_epoch_sec(*t))
            throw S3Error(S3ErrorCode::PreconditionFailed,
                          "At least one of the pre-conditions you specified did not hold");
    }
    if (auto v = req.headers.get("If-None-Match")) {
        if (*v == "*" || strip_quotes(*v) == meta.etag) not_modified = true;
    } else if (auto ims = req.headers.get("If-Modified-Since")) {
        auto t = util::parse_http_date(*ims);
        if (t && to_epoch_sec(meta.last_modified) <= to_epoch_sec(*t)) not_modified = true;
    }
}

bool has_read_preconditions(const http::HttpRequest& req) {
    return req.headers.has("If-Match") || req.headers.has("If-None-Match") ||
           req.headers.has("If-Modified-Since") || req.headers.has("If-Unmodified-Since");
}

// If-Range (RFC 7233 §3.2): Range takes effect only when the validator (strong ETag, or HTTP-date exactly
// matching Last-Modified) hits; otherwise Range is ignored and the full object returned
bool if_range_matches(const http::HttpRequest& req, const storage::ObjectMeta& meta) {
    auto v = req.headers.get("If-Range");
    if (!v) return true;
    if (!v->empty() && v->front() == '"') return strip_quotes(*v) == meta.etag;
    auto t = util::parse_http_date(*v);
    return t && to_epoch_sec(meta.last_modified) == to_epoch_sec(*t);
}

}  // namespace

namespace handlers {
bool has_response_override(const http::HttpRequest& req) {
    for (auto& o : kResponseOverrides)
        if (req.query_has(o.param)) return true;
    return false;
}
}  // namespace handlers

Task<http::HttpResponse> S3Service::put_object(http::HttpRequest& req, std::string bucket,
                                               std::string key) {
    require_content_length(req);  // 411 (roadmap §2.5); CopyObject is body-less and exempt
    auto& backend = router_.resolve(bucket);

    // PUT conditional requests (docs/s3-protocol.md §6): If-None-Match:* prevents overwrite, If-Match is optimistic concurrency.
    // "Check + commit" is done by the backend at its atomic commit point (PutCondition contract, backend.h) -- here
    // only one lock-free head precheck is done, so obviously failing requests get their 412/404 without uploading the full body.
    // The precheck is non-atomic and carries no correctness burden; the old L2 striped lock spanned the entire body
    // upload, so 64 slow connections could block all conditional writes gateway-wide, and it could never hold in multi-instance deployments anyway
    storage::PutCondition cond;
    if (auto v = req.headers.get("If-None-Match")) {
        if (*v != "*")
            throw S3Error(S3ErrorCode::NotImplemented,
                          "PUT If-None-Match only supports '*'.");
        cond.if_none_match = true;
        bool exists = true;
        try {
            co_await backend.head_object(bucket, key);
        } catch (const S3Error& e) {
            if (e.code != S3ErrorCode::NoSuchKey) throw;
            exists = false;
        }
        if (exists)
            throw S3Error(S3ErrorCode::PreconditionFailed,
                          "At least one of the pre-conditions you specified did not hold");
    } else if (auto v2 = req.headers.get("If-Match")) {
        cond.if_match_etag = strip_quotes(*v2);
        auto cur = co_await backend.head_object(bucket, key);  // missing -> NoSuchKey(404)
        if (*cond.if_match_etag != cur.etag)
            throw S3Error(S3ErrorCode::PreconditionFailed,
                          "At least one of the pre-conditions you specified did not hold");
    }

    // Declared checksum persists with the object (roadmap §2.2): header form is known
    // now; trailer form resolves through the pending slot once the body drains
    storage::ObjectMeta meta = meta_from_headers(req);
    attach_request_checksum(req, meta);
    std::string cs_algo = meta.checksum_algorithm;
    std::string cs_value = meta.checksum_value;
    auto cs_pending = meta.checksum_pending;

    http::StringBodyReader empty{""};
    http::BodyReader& body = req.body ? *req.body : static_cast<http::BodyReader&>(empty);
    auto result = co_await backend.put_object(bucket, key, std::move(meta), body, cond);

    http::HttpResponse resp;
    resp.headers.set("ETag", quote_etag(result.etag));
    // Response echo (AWS PutObject behavior): the body is drained by now, so the
    // trailer-form value is readable from the capture slot
    if (cs_value.empty() && cs_pending) cs_value = *cs_pending;
    if (!cs_algo.empty() && !cs_value.empty()) {
        std::string h = "x-amz-checksum-";
        for (char c : cs_algo) h.push_back(http::HeaderMap::lower(c));
        resp.headers.set(h, cs_value);
        resp.headers.set("x-amz-checksum-type", "FULL_OBJECT");
    }
    co_return resp;
}

Task<http::HttpResponse> S3Service::copy_object(http::HttpRequest& req, std::string bucket,
                                                std::string key) {
    auto [src_bucket, src_key] = parse_copy_source(*req.headers.get("x-amz-copy-source"));
    auto& src_backend = router_.resolve(src_bucket);

    auto src_meta = co_await src_backend.head_object(src_bucket, src_key);
    check_copy_preconditions(req, src_meta);

    std::string directive = req.headers.get("x-amz-metadata-directive").value_or("COPY");
    if (directive != "COPY" && directive != "REPLACE")
        throw S3Error(S3ErrorCode::InvalidArgument, "Invalid x-amz-metadata-directive.");
    if (src_bucket == bucket && src_key == key && directive == "COPY")
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "This copy request is illegal because it is trying to copy an object "
                      "to itself without changing metadata.");

    storage::ObjectMeta meta;
    if (directive == "REPLACE") {
        meta = meta_from_headers(req);
        // The bytes are unchanged by a copy, so the source's checksum and part layout
        // still describe the new object (roadmap §2.2/§2.5); REPLACE only swaps the
        // user-editable metadata
        meta.checksum_algorithm = src_meta.checksum_algorithm;
        meta.checksum_value = src_meta.checksum_value;
        meta.checksum_type = src_meta.checksum_type;
        meta.part_sizes = src_meta.part_sizes;
    } else {
        // COPY: the whole metadata set travels with the object. Field-by-field copying once missed newly added
        // first-class fields (§5.2); here only the three items bound to the new object (key/size/etag) are left for the backend to recompute
        meta = src_meta;
        meta.key.clear();
        meta.size = 0;
        meta.etag.clear();
    }

    // Same-backend fast path (docs/archive/gaps.md §6.2/§6.3): localfs uses kernel copy_file_range,
    // cloudproxy uses remote server-side COPY -- both skip "read into the gateway then write back". nullopt = the
    // backend has no fast path or it is unavailable this time (tier stub, cross-device); falls back to the streaming path, semantically equivalent
    auto& dst_backend = router_.resolve(bucket);
    storage::PutResult result;
    std::optional<storage::PutResult> fast;
    if (&src_backend == &dst_backend)
        fast = co_await dst_backend.copy_object_fast(src_bucket, src_key, bucket, key, meta);
    if (fast) {
        result = std::move(*fast);
    } else {
        auto stream = co_await src_backend.get_object(src_bucket, src_key, std::nullopt);
        result = co_await dst_backend.put_object(bucket, key, std::move(meta), *stream.body);
    }

    XmlWriter w;
    w.open("CopyObjectResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    w.element("LastModified", util::iso8601(std::chrono::system_clock::now()));
    w.element("ETag", quote_etag(result.etag));
    w.close();
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = w.str();
    co_return resp;
}

Task<http::HttpResponse> S3Service::get_object(http::HttpRequest& req, std::string bucket,
                                               std::string key, bool head_only) {
    auto& backend = router_.resolve(bucket);

    std::optional<storage::ByteRange> range;
    if (auto v = req.headers.get("Range")) range = parse_range_header(*v);

    http::HttpResponse resp;
    if (head_only) {
        auto meta = co_await backend.head_object(bucket, key);
        // Preconditions are decided before Range (RFC 7232 precedence: 412/304 beats 416)
        bool not_modified = false;
        check_read_preconditions(req, meta, not_modified);
        if (not_modified) {
            fill_not_modified_headers(resp, meta);
            co_return resp;
        }
        fill_object_headers(resp, meta);
        apply_response_overrides(req, resp);
        if (range && !if_range_matches(req, meta)) range.reset();
        if (range) {  // aligned with GET: 206 + Content-Range, no body, length only
            auto [f, l] = storage::resolve_range(*range, meta.size);  // unsatisfiable -> 416
            resp.status = 206;
            resp.headers.set("Content-Range", "bytes " + std::to_string(f) + "-" +
                                                  std::to_string(l) + "/" +
                                                  std::to_string(meta.size));
            resp.content_length = l - f + 1;
        } else {
            resp.content_length = meta.size;  // no body, the driver sends only Content-Length
        }
        apply_checksum_echo(req, meta, resp);  // §2.2 (no-op on 206)
        co_return resp;
    }

    // When conditional headers are present, decide via head before opening the stream: 412/304 comes before
    // range's 416 (RFC 7232), and it also avoids backends like cloudproxy pulling the object from upstream only to discard it all
    if (has_read_preconditions(req) || (range && req.headers.has("If-Range"))) {
        auto meta = co_await backend.head_object(bucket, key);
        bool not_modified = false;
        check_read_preconditions(req, meta, not_modified);
        if (not_modified) {
            fill_not_modified_headers(resp, meta);
            co_return resp;
        }
        if (range && !if_range_matches(req, meta)) range.reset();
    }

    auto stream = co_await backend.get_object(bucket, key, range);

    fill_object_headers(resp, stream.meta);
    // Overrides are applied before 206/Content-Range: those two are determined by this transfer and cannot be client-specified
    apply_response_overrides(req, resp);
    uint64_t len = stream.meta.size;
    if (stream.range) {
        // Backend contract: the returned range must have both ends resolved; leaving one unset is a backend defect, no UB dereference
        if (!stream.range->first || !stream.range->last)
            throw S3Error(S3ErrorCode::InternalError,
                          "storage backend returned an unresolved range");
        uint64_t f = *stream.range->first, l = *stream.range->last;
        len = l - f + 1;
        resp.status = 206;
        resp.headers.set("Content-Range", "bytes " + std::to_string(f) + "-" + std::to_string(l) +
                                              "/" + std::to_string(stream.meta.size));
    }
    resp.content_length = len;
    resp.stream_body = std::move(stream.body);
    apply_checksum_echo(req, stream.meta, resp);  // §2.2 (no-op on 206)
    co_return resp;
}

Task<http::HttpResponse> S3Service::delete_object(std::string bucket, std::string key) {
    co_await router_.resolve(bucket).delete_object(bucket, key);
    http::HttpResponse resp;
    resp.status = 204;
    co_return resp;
}

namespace {

// Single-key deletion with exceptions folded into a result value (docs/archive/gaps.md §3.9): no key failure may abort
// the batch -- already-deleted keys must appear in the response, or clients cannot tell which deletions succeeded.
// A standalone function rather than a capturing lambda: the lambda temporary is destroyed while the coroutine is suspended, so captures would dangle
Task<std::optional<S3Error>> delete_one(storage::IStorageBackend& backend,
                                        const std::string& bucket, const std::string& key) {
    try {
        co_await backend.delete_object(bucket, key);
        co_return std::nullopt;
    } catch (const S3Error& e) {
        co_return e;
    } catch (const std::exception& e) {
        // Non-S3 exceptions (backend transport/storage errors): raw text goes to the log only
        LOG_ERROR("DeleteObjects: key {} failed: {}", key, e.what());
        co_return S3Error(S3ErrorCode::InternalError, "We encountered an internal error.");
    }
}

}  // namespace

// DeleteObjects batch deletion (POST /bucket?delete, request XML <= 1MiB, at most 1000 keys)
Task<http::HttpResponse> S3Service::delete_objects(http::HttpRequest& req, std::string bucket,
                                                   const RequestAuth& auth) {
    // AWS **requires** an integrity header for this operation (docs/archive/gaps.md §5.6): batch deletion is the one
    // operation where "a rewritten request body silently deletes extra objects"; absence is 400. The digest itself
    // is verified while reading the body by the ChecksumVerifyingReader that dispatch installs; here only "is one declared" is checked
    constexpr std::string_view kChecksumPrefix = "x-amz-checksum-";
    bool has_digest = req.headers.has("Content-MD5");
    if (!has_digest)
        for (auto& [k, v] : req.headers.items())
            if (k.size() > kChecksumPrefix.size() &&
                http::HeaderMap::ieq(std::string_view(k).substr(0, kChecksumPrefix.size()),
                                     kChecksumPrefix))
                has_digest = true;
    if (!has_digest)
        throw S3Error(S3ErrorCode::InvalidRequest,
                      "Missing required header for this request: Content-MD5 "
                      "(or a x-amz-checksum-* header).");
    std::string body = co_await read_body(req);
    XmlNode root = xml_parse(body);
    if (root.name != "Delete")
        throw S3Error(S3ErrorCode::MalformedXML, "Expected <Delete> root element.");
    bool quiet = root.get("Quiet") == "true";

    std::vector<std::string> keys;
    for (auto& child : root.children) {
        if (child.name != "Object") continue;
        // Missing <Key>/empty Key is a malformed request, the whole batch is rejected (matching AWS) -- not per-key errors
        std::string k = child.get("Key");
        if (k.empty())
            throw S3Error(S3ErrorCode::MalformedXML,
                          "Each <Object> must contain a non-empty <Key>.");
        // Silently ignoring <VersionId> would turn "delete a specific version" into "delete the current object" --
        // far more dangerous than erroring (docs/archive/gaps.md §3.9)
        if (!child.get("VersionId").empty())
            throw S3Error(S3ErrorCode::NotImplemented, "Versioning is not implemented.");
        keys.push_back(std::move(k));
    }
    if (keys.empty())
        throw S3Error(S3ErrorCode::MalformedXML, "Delete must contain at least one <Object>.");
    if (keys.size() > 1000)
        throw S3Error(S3ErrorCode::MalformedXML, "DeleteObjects accepts at most 1000 keys.");

    auto& backend = router_.resolve(bucket);
    std::vector<std::optional<S3Error>> outcome(keys.size());
    // Re-check policy per key: dispatch authorizes this route at Bucket scope (key empty, prefix check skipped
    // entirely), so every Key in the XML must be re-judged here with the same decision as single delete, otherwise
    // a prefix-restricted credential could use the batch interface to delete objects outside its allowlist
    if (auth.policy)
        for (size_t i = 0; i < keys.size(); ++i)
            if (!auth.policy->allows(bucket, keys[i], Action::Delete))
                outcome[i] =
                    S3Error(S3ErrorCode::AccessDenied, "Access denied by credential policy.");
    // Bounded concurrency (docs/archive/gaps.md §3.9): serial co_await on cloudproxy/duostore means
    // 1000 sequential RTTs. The batch size caps the concurrency hit on a single backend; batches still proceed in order
    constexpr size_t kBatch = 32;
    std::vector<size_t> pending;
    pending.reserve(keys.size());
    for (size_t i = 0; i < keys.size(); ++i)
        if (!outcome[i]) pending.push_back(i);
    for (size_t base = 0; base < pending.size(); base += kBatch) {
        size_t n = std::min(kBatch, pending.size() - base);
        std::vector<Task<std::optional<S3Error>>> batch;
        batch.reserve(n);
        for (size_t i = 0; i < n; ++i)
            batch.push_back(delete_one(backend, bucket, keys[pending[base + i]]));
        auto res = co_await when_all(std::move(batch));
        for (size_t i = 0; i < n; ++i) outcome[pending[base + i]] = std::move(res[i]);
    }

    XmlWriter w;
    w.open("DeleteResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    for (size_t i = 0; i < keys.size(); ++i) {
        if (!outcome[i]) {
            if (!quiet) {
                w.open("Deleted");
                w.element("Key", keys[i]);
                w.close();
            }
        } else {
            w.open("Error");
            w.element("Key", keys[i]);
            w.element("Code", wire_code(outcome[i]->code));
            w.element("Message", outcome[i]->message);
            w.close();
        }
    }
    w.close();

    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = w.str();
    co_return resp;
}

}  // namespace lights3::s3
