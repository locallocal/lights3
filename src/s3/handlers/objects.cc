// object 级 handler：Put/Get/Head/Delete/Copy/DeleteObjects 与条件请求（docs/s3-protocol.md §1/§6）
#include <charconv>

#include "core/util/time.h"
#include "core/util/uri.h"
#include "s3/handlers/common.h"
#include "s3/service.h"
#include "s3/xml.h"

namespace lights3::s3 {

using namespace handlers;

namespace {

// "bytes=a-b" / "bytes=a-" / "bytes=-n"；malformed 时忽略（S3 行为）
std::optional<storage::ByteRange> parse_range_header(const std::string& v) {
    if (v.rfind("bytes=", 0) != 0) return std::nullopt;
    std::string spec = v.substr(6);
    if (spec.find(',') != std::string::npos) return std::nullopt;  // 多段 range 不支持
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
    return r;
}

void fill_object_headers(http::HttpResponse& resp, const storage::ObjectMeta& meta) {
    resp.headers.set("ETag", quote_etag(meta.etag));
    resp.headers.set("Content-Type", meta.content_type);
    resp.headers.set("Last-Modified", util::http_date(meta.last_modified));
    resp.headers.set("Accept-Ranges", "bytes");
    for (auto& [k, v] : meta.user_meta) resp.headers.set("x-amz-meta-" + k, v);
}

// HTTP 时间头按秒粒度比较（Last-Modified 序列化即秒精度）
int64_t to_epoch_sec(util::SysTime t) {
    return std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count();
}

// GET/HEAD 条件请求（docs/s3-protocol.md §6，优先级遵循 RFC 7232：
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

// CopyObject 的源条件（x-amz-copy-source-if-*）：任一不满足即 412
void check_copy_preconditions(const http::HttpRequest& req, const storage::ObjectMeta& src) {
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
std::pair<std::string, std::string> parse_copy_source(const std::string& raw) {
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
    return {s.substr(0, slash), s.substr(slash + 1)};
}

}  // namespace

Task<http::HttpResponse> S3Service::put_object(http::HttpRequest& req, std::string bucket,
                                               std::string key) {
    auto& backend = router_.resolve(bucket);

    // PUT 条件请求（docs/s3-protocol.md §6）：If-None-Match:* 防覆盖，If-Match 乐观并发
    if (auto v = req.headers.get("If-None-Match")) {
        if (*v != "*")
            throw S3Error(S3ErrorCode::NotImplemented,
                          "PUT If-None-Match only supports '*'.");
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
        auto cur = co_await backend.head_object(bucket, key);  // 缺失 → NoSuchKey(404)
        if (strip_quotes(*v2) != cur.etag)
            throw S3Error(S3ErrorCode::PreconditionFailed,
                          "At least one of the pre-conditions you specified did not hold");
    }

    http::StringBodyReader empty{""};
    http::BodyReader& body = req.body ? *req.body : static_cast<http::BodyReader&>(empty);
    auto result = co_await backend.put_object(bucket, key, meta_from_headers(req), body);

    http::HttpResponse resp;
    resp.headers.set("ETag", quote_etag(result.etag));
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
    } else {
        meta.content_type = src_meta.content_type;
        meta.user_meta = src_meta.user_meta;
    }

    auto stream = co_await src_backend.get_object(src_bucket, src_key, std::nullopt);
    auto result =
        co_await router_.resolve(bucket).put_object(bucket, key, std::move(meta), *stream.body);

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
        bool not_modified = false;
        check_read_preconditions(req, meta, not_modified);
        if (not_modified) {
            resp.status = 304;
            resp.headers.set("ETag", quote_etag(meta.etag));
            co_return resp;
        }
        fill_object_headers(resp, meta);
        resp.content_length = meta.size;  // 无 body，驱动只发 Content-Length
        co_return resp;
    }

    auto stream = co_await backend.get_object(bucket, key, range);
    bool not_modified = false;
    check_read_preconditions(req, stream.meta, not_modified);
    if (not_modified) {
        resp.status = 304;
        resp.headers.set("ETag", quote_etag(stream.meta.etag));
        co_return resp;
    }

    fill_object_headers(resp, stream.meta);
    uint64_t len = stream.meta.size;
    if (stream.range) {
        uint64_t f = *stream.range->first, l = *stream.range->last;
        len = l - f + 1;
        resp.status = 206;
        resp.headers.set("Content-Range", "bytes " + std::to_string(f) + "-" + std::to_string(l) +
                                              "/" + std::to_string(stream.meta.size));
    }
    resp.content_length = len;
    resp.stream_body = std::move(stream.body);
    co_return resp;
}

Task<http::HttpResponse> S3Service::delete_object(std::string bucket, std::string key) {
    co_await router_.resolve(bucket).delete_object(bucket, key);
    http::HttpResponse resp;
    resp.status = 204;
    co_return resp;
}

// DeleteObjects 批量删除（POST /bucket?delete，请求 XML ≤ 1MiB，至多 1000 key）
Task<http::HttpResponse> S3Service::delete_objects(http::HttpRequest& req, std::string bucket) {
    std::string body = co_await read_body(req);
    XmlNode root = xml_parse(body);
    if (root.name != "Delete")
        throw S3Error(S3ErrorCode::MalformedXML, "Expected <Delete> root element.");
    bool quiet = root.get("Quiet") == "true";

    std::vector<std::string> keys;
    for (auto& child : root.children)
        if (child.name == "Object") keys.push_back(child.get("Key"));
    if (keys.size() > 1000)
        throw S3Error(S3ErrorCode::MalformedXML, "DeleteObjects accepts at most 1000 keys.");

    auto& backend = router_.resolve(bucket);
    XmlWriter w;
    w.open("DeleteResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    for (auto& key : keys) {
        try {
            if (key.empty())
                throw S3Error(S3ErrorCode::InvalidArgument, "Object key must not be empty.");
            co_await backend.delete_object(bucket, key);
            if (!quiet) {
                w.open("Deleted");
                w.element("Key", key);
                w.close();
            }
        } catch (const S3Error& e) {
            w.open("Error");
            w.element("Key", key);
            w.element("Code", wire_code(e.code));
            w.element("Message", e.message);
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
