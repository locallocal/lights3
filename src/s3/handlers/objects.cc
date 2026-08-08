// object 级 handler：Put/Get/Head/Delete/Copy/DeleteObjects 与条件请求（docs/s3-protocol.md §1/§6）
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
    // "bytes=5-3" 是语法非法（RFC 9110 §14.1.1 要求 last >= first）：整个头按
    // 无效忽略、回 200 整对象——此前落到 resolve_range 变成 416（docs/gaps.md §4）
    if (r.first && r.last && *r.last < *r.first) return std::nullopt;
    return r;
}

// response-* 覆盖参数（docs/gaps.md §5.3）：presigned 下载链接最常用的就是
// response-content-disposition（"点开就下载成这个文件名"）。
// 前提：AWS 只对已认证请求生效——匿名可读对象若允许覆盖，一条链接就能把任意
// Content-Disposition 挂到桶的域名下。本实现开启认证时，能走到 handler 的请求
// 必然已验签；关闭认证时不存在这条边界。故无需再判身份，但值必须过滤 CR/LF：
// query 值是攻击者可控的，直接塞进响应头就是响应拆分
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

// 304 的头集合（RFC 9110 §15.4.5，docs/gaps.md §5.9）：200 会发的缓存校验类头
// 在 304 上必须照发，否则客户端刷新缓存条目时会把 Last-Modified 丢掉
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
    // 一等元数据原样回显（docs/gaps.md §5.2）；空 = 未设置，不发该头
    for (auto& f : storage::kStdMetaFields)
        if (!(meta.*f.field).empty()) resp.headers.set(f.header, meta.*f.field);
    for (auto& [k, v] : meta.user_meta) resp.headers.set("x-amz-meta-" + k, v);
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

bool has_read_preconditions(const http::HttpRequest& req) {
    return req.headers.has("If-Match") || req.headers.has("If-None-Match") ||
           req.headers.has("If-Modified-Since") || req.headers.has("If-Unmodified-Since");
}

// If-Range（RFC 7233 §3.2）：验证器（强 ETag 或 HTTP-date 精确匹配 Last-Modified）
// 命中才生效 Range，否则忽略 Range 回整对象
bool if_range_matches(const http::HttpRequest& req, const storage::ObjectMeta& meta) {
    auto v = req.headers.get("If-Range");
    if (!v) return true;
    if (!v->empty() && v->front() == '"') return strip_quotes(*v) == meta.etag;
    auto t = util::parse_http_date(*v);
    return t && to_epoch_sec(meta.last_modified) == to_epoch_sec(*t);
}

}  // namespace

Task<http::HttpResponse> S3Service::put_object(http::HttpRequest& req, std::string bucket,
                                               std::string key) {
    auto& backend = router_.resolve(bucket);

    // PUT 条件请求（docs/s3-protocol.md §6）：If-None-Match:* 防覆盖，If-Match 乐观并发。
    // "检查 + 提交"由后端在其原子提交点完成（PutCondition 契约，backend.h）——此处
    // 只做一次无锁 head 预检，让明显失败的请求不必上传完整 body 就能拿到 412/404。
    // 预检非原子，不承担正确性；曾经的 L2 条带锁跨整个 body 上传，64 条慢速连接
    // 即可堵死全网关条件写，且多实例部署下本就守不住
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
        auto cur = co_await backend.head_object(bucket, key);  // 缺失 → NoSuchKey(404)
        if (*cond.if_match_etag != cur.etag)
            throw S3Error(S3ErrorCode::PreconditionFailed,
                          "At least one of the pre-conditions you specified did not hold");
    }

    http::StringBodyReader empty{""};
    http::BodyReader& body = req.body ? *req.body : static_cast<http::BodyReader&>(empty);
    auto result = co_await backend.put_object(bucket, key, meta_from_headers(req), body, cond);

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
        // COPY：整份元数据随对象走。逐字段抄写曾经漏掉新增的一等字段（§5.2），
        // 这里只把与新对象绑定的三项（key/size/etag）留给后端重算
        meta = src_meta;
        meta.key.clear();
        meta.size = 0;
        meta.etag.clear();
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
        // 前置条件先于 Range 判定（RFC 7232 优先级：412/304 压过 416）
        bool not_modified = false;
        check_read_preconditions(req, meta, not_modified);
        if (not_modified) {
            fill_not_modified_headers(resp, meta);
            co_return resp;
        }
        fill_object_headers(resp, meta);
        apply_response_overrides(req, resp);
        if (range && !if_range_matches(req, meta)) range.reset();
        if (range) {  // 与 GET 对齐：206 + Content-Range，无 body 只报长度
            auto [f, l] = storage::resolve_range(*range, meta.size);  // 不可满足 → 416
            resp.status = 206;
            resp.headers.set("Content-Range", "bytes " + std::to_string(f) + "-" +
                                                  std::to_string(l) + "/" +
                                                  std::to_string(meta.size));
            resp.content_length = l - f + 1;
        } else {
            resp.content_length = meta.size;  // 无 body，驱动只发 Content-Length
        }
        co_return resp;
    }

    // 条件头存在时先 head 判定再建流：412/304 先于 range 的 416（RFC 7232），
    // 也避免 cloudproxy 之类后端先向上游拉取对象再整个丢弃
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
    // 覆盖在 206/Content-Range 之前应用：那两个由本次传输决定，不接受客户端指定
    apply_response_overrides(req, resp);
    uint64_t len = stream.meta.size;
    if (stream.range) {
        // 后端契约：返回的 range 须两端都已解析；漏填是后端缺陷，不能 UB 解引用
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
    co_return resp;
}

Task<http::HttpResponse> S3Service::delete_object(std::string bucket, std::string key) {
    co_await router_.resolve(bucket).delete_object(bucket, key);
    http::HttpResponse resp;
    resp.status = 204;
    co_return resp;
}

namespace {

// 单 key 删除，异常收敛为结果值（docs/gaps.md §3.9）：批内任何 key 失败都不得
// 中断整批——已删的 key 必须出现在响应里，否则客户端无从得知哪些删成了。
// 独立函数而非捕获 lambda：协程挂起期间 lambda 临时对象已析构，捕获即悬垂
Task<std::optional<S3Error>> delete_one(storage::IStorageBackend& backend,
                                        const std::string& bucket, const std::string& key) {
    try {
        co_await backend.delete_object(bucket, key);
        co_return std::nullopt;
    } catch (const S3Error& e) {
        co_return e;
    } catch (const std::exception& e) {
        // 非 S3 异常（后端传输/存储错误）：原始文案只进日志
        LOG_ERROR("DeleteObjects: key {} failed: {}", key, e.what());
        co_return S3Error(S3ErrorCode::InternalError, "We encountered an internal error.");
    }
}

}  // namespace

// DeleteObjects 批量删除（POST /bucket?delete，请求 XML ≤ 1MiB，至多 1000 key）
Task<http::HttpResponse> S3Service::delete_objects(http::HttpRequest& req, std::string bucket) {
    std::string body = co_await read_body(req);
    XmlNode root = xml_parse(body);
    if (root.name != "Delete")
        throw S3Error(S3ErrorCode::MalformedXML, "Expected <Delete> root element.");
    bool quiet = root.get("Quiet") == "true";

    std::vector<std::string> keys;
    for (auto& child : root.children) {
        if (child.name != "Object") continue;
        // 缺 <Key>/空 Key 是畸形请求，整批拒绝（与 AWS 一致）——不是逐 key 报错
        std::string k = child.get("Key");
        if (k.empty())
            throw S3Error(S3ErrorCode::MalformedXML,
                          "Each <Object> must contain a non-empty <Key>.");
        // <VersionId> 静默忽略的话，"删指定版本"会变成"删当前对象"——比报错
        // 危险得多（docs/gaps.md §3.9）
        if (!child.get("VersionId").empty())
            throw S3Error(S3ErrorCode::NotImplemented, "Versioning is not implemented.");
        keys.push_back(std::move(k));
    }
    if (keys.empty())
        throw S3Error(S3ErrorCode::MalformedXML, "Delete must contain at least one <Object>.");
    if (keys.size() > 1000)
        throw S3Error(S3ErrorCode::MalformedXML, "DeleteObjects accepts at most 1000 keys.");

    auto& backend = router_.resolve(bucket);
    // 有界并发（docs/gaps.md §3.9）：串行 co_await 在 cloudproxy/duostore 上是
    // 1000 次串行 RTT。批大小压住对单后端的并发冲击，批间仍顺序推进
    constexpr size_t kBatch = 32;
    std::vector<std::optional<S3Error>> outcome(keys.size());
    for (size_t base = 0; base < keys.size(); base += kBatch) {
        size_t n = std::min(kBatch, keys.size() - base);
        std::vector<Task<std::optional<S3Error>>> batch;
        batch.reserve(n);
        for (size_t i = 0; i < n; ++i)
            batch.push_back(delete_one(backend, bucket, keys[base + i]));
        auto res = co_await when_all(std::move(batch));
        for (size_t i = 0; i < n; ++i) outcome[base + i] = std::move(res[i]);
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
