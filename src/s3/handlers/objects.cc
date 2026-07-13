// object 级 handler：PutObject / GetObject / HeadObject / DeleteObject
#include <charconv>

#include "core/util/time.h"
#include "s3/service.h"

namespace lights3::s3 {

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

std::string quote_etag(const std::string& etag) { return "\"" + etag + "\""; }

void fill_object_headers(http::HttpResponse& resp, const storage::ObjectMeta& meta) {
    resp.headers.set("ETag", quote_etag(meta.etag));
    resp.headers.set("Content-Type", meta.content_type);
    resp.headers.set("Last-Modified", util::http_date(meta.last_modified));
    resp.headers.set("Accept-Ranges", "bytes");
    for (auto& [k, v] : meta.user_meta) resp.headers.set("x-amz-meta-" + k, v);
}

// If-Match / If-None-Match（弱比较不支持；etag 参数为未加引号形式）
void check_preconditions(const http::HttpRequest& req, const std::string& etag,
                         bool& not_modified) {
    auto strip = [](std::string s) {
        if (s.size() >= 2 && s.front() == '"' && s.back() == '"') return s.substr(1, s.size() - 2);
        return s;
    };
    if (auto v = req.headers.get("If-Match")) {
        if (*v != "*" && strip(*v) != etag)
            throw S3Error(S3ErrorCode::PreconditionFailed,
                          "At least one of the pre-conditions you specified did not hold");
    }
    if (auto v = req.headers.get("If-None-Match")) {
        if (*v == "*" || strip(*v) == etag) not_modified = true;
    }
}

}  // namespace

Task<http::HttpResponse> S3Service::put_object(http::HttpRequest& req, std::string bucket,
                                               std::string key) {
    storage::ObjectMeta meta;
    if (auto ct = req.headers.get("Content-Type")) meta.content_type = *ct;
    for (auto& [k, v] : req.headers.items()) {
        std::string lk;
        for (char c : k) lk.push_back(http::HeaderMap::lower(c));
        if (lk.rfind("x-amz-meta-", 0) == 0) meta.user_meta[lk.substr(11)] = v;
    }

    http::StringBodyReader empty{""};
    http::BodyReader& body = req.body ? *req.body : static_cast<http::BodyReader&>(empty);
    auto result = co_await router_.resolve(bucket).put_object(bucket, key, std::move(meta), body);

    http::HttpResponse resp;
    resp.headers.set("ETag", quote_etag(result.etag));
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
        check_preconditions(req, meta.etag, not_modified);
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
    check_preconditions(req, stream.meta.etag, not_modified);
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

}  // namespace lights3::s3
