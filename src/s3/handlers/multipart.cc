// multipart handler：Create/UploadPart/Complete/Abort/ListParts/ListMultipartUploads
// （docs/s3-protocol.md §1；存储层语义见 docs/storage-backend.md §3.2）
#include <charconv>
#include <algorithm>
#include <map>

#include "core/util/time.h"
#include "s3/handlers/common.h"
#include "storage/multipart.h"
#include "s3/service.h"
#include "s3/xml.h"

namespace lights3::s3 {

using namespace handlers;

namespace {

int parse_part_number(const http::HttpRequest& req) {
    auto v = req.query_get("partNumber");
    int no = 0;
    if (v) {
        auto [p, ec] = std::from_chars(v->data(), v->data() + v->size(), no);
        if (ec != std::errc() || p != v->data() + v->size()) no = 0;
    }
    if (no < 1 || no > 10000)
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Part number must be an integer between 1 and 10000.");
    return no;
}

std::string require_upload_id(const http::HttpRequest& req) {
    auto v = req.query_get("uploadId");
    if (!v || v->empty())
        throw S3Error(S3ErrorCode::InvalidArgument, "Missing uploadId query parameter.");
    return *v;
}

// query 里的整数参数：缺省用 def，非法一律 400（静默当默认值会让客户端以为
// 自己的分页生效了）
int parse_int_param(const http::HttpRequest& req, const char* name, int def) {
    auto v = req.query_get(name);
    if (!v || v->empty()) return def;
    int out = 0;
    auto [p, ec] = std::from_chars(v->data(), v->data() + v->size(), out);
    if (ec != std::errc() || p != v->data() + v->size())
        throw S3Error(S3ErrorCode::InvalidArgument, std::string("Invalid ") + name + " value");
    return out;
}

// max-* 与 ListObjects 的 max-keys 同款：负值 400，超出上限静默钳制——不钳制的话
// max=INT_MAX 会把整表一次构造进内存
int parse_max(const http::HttpRequest& req, const char* name, int cap) {
    int v = parse_int_param(req, name, cap);
    if (v < 0) throw S3Error(S3ErrorCode::InvalidArgument, std::string("Invalid ") + name + " value");
    return std::min(v, cap);
}

// "scheme://host"：Location 要完整 URL（docs/gaps.md §5.7）。scheme 只能靠反代
// 转述——直连时本实现是明文 HTTP，TLS 由前置代理终结（docs/s3-protocol.md）
std::string request_base_url(const http::HttpRequest& req) {
    std::string scheme = "http";
    if (auto p = req.headers.get("X-Forwarded-Proto"); p && !p->empty()) scheme = *p;
    std::string host = req.headers.get("Host").value_or("");
    return scheme + "://" + host;
}

// 最小分片校验（docs/gaps.md §5.7）：末片除外。分片尺寸只有存储层知道，
// complete 之前先列一次；缺失的分片不在这里报错，交给后端的 InvalidPart
Task<void> check_min_part_sizes(storage::IStorageBackend& backend, const std::string& bucket,
                                const std::string& key, const std::string& upload_id,
                                const std::vector<storage::PartInfo>& parts, uint64_t min_size) {
    if (min_size == 0) co_return;      // 旋钮关闭
    if (parts.size() <= 1) co_return;  // 单片上传不受最小尺寸约束
    // 取全量：分片数由协议封顶 10000，且这里要按分片号查表而非翻页
    storage::ListPartsOptions all_opt;
    all_opt.max_parts = storage::kMaxParts;
    auto have = co_await backend.list_parts(bucket, key, upload_id, all_opt);
    std::map<int, uint64_t> size_of;
    for (auto& p : have.parts) size_of[p.part_no] = p.size;
    for (size_t i = 0; i + 1 < parts.size(); ++i) {  // 末片不校验
        auto it = size_of.find(parts[i].part_no);
        if (it == size_of.end()) continue;
        if (it->second < min_size)
            throw S3Error(S3ErrorCode::EntityTooSmall,
                          "Your proposed upload is smaller than the minimum allowed size. "
                          "Part " + std::to_string(parts[i].part_no) + " is " +
                              std::to_string(it->second) + " bytes; the minimum is " +
                              std::to_string(min_size) + " bytes.");
    }
}

// "bytes=first-last"：两端必填、闭区间（AWS UploadPartCopy 语义，比 GET Range 的
// 宽松解析严格——suffix/开区间形式在这里都是 InvalidArgument）
storage::ByteRange parse_copy_source_range(const std::string& v, uint64_t src_size) {
    auto bad = [] {
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "The x-amz-copy-source-range value must be of the form bytes=first-last "
                      "where first and last are the zero-based offsets to copy.");
    };
    if (v.rfind("bytes=", 0) != 0) bad();
    std::string_view spec = std::string_view(v).substr(6);
    auto dash = spec.find('-');
    if (dash == std::string_view::npos || dash == 0 || dash + 1 >= spec.size()) bad();
    auto to_u64 = [&](std::string_view s) -> uint64_t {
        uint64_t out = 0;
        auto [p, ec] = std::from_chars(s.data(), s.data() + s.size(), out);
        if (ec != std::errc() || p != s.data() + s.size()) bad();
        return out;
    };
    uint64_t first = to_u64(spec.substr(0, dash)), last = to_u64(spec.substr(dash + 1));
    if (first > last) bad();
    if (last >= src_size)
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Range specified is not valid for source object of size: " +
                          std::to_string(src_size));
    storage::ByteRange r;
    r.first = first;
    r.last = last;
    return r;
}

}  // namespace

Task<http::HttpResponse> S3Service::create_multipart(http::HttpRequest& req, std::string bucket,
                                                     std::string key) {
    auto upload_id =
        co_await router_.resolve(bucket).create_multipart(bucket, key, meta_from_headers(req));
    metrics_.mpu_created();

    XmlWriter w;
    w.open("InitiateMultipartUploadResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    w.element("Bucket", bucket);
    w.element("Key", key);
    w.element("UploadId", upload_id);
    w.close();
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = w.str();
    co_return resp;
}

Task<http::HttpResponse> S3Service::upload_part(http::HttpRequest& req, std::string bucket,
                                                std::string key) {
    int part_no = parse_part_number(req);
    std::string upload_id = require_upload_id(req);

    // UploadPartCopy（docs/s3-protocol.md §1）：源经 head 校验条件头后按 range 流式
    // 读出，作为 part body 写入目标 upload；源/目标可在不同后端（同 CopyObject）
    if (auto src_hdr = req.headers.get("x-amz-copy-source")) {
        auto [src_bucket, src_key] = parse_copy_source(*src_hdr);
        auto& src_backend = router_.resolve(src_bucket);
        auto src_meta = co_await src_backend.head_object(src_bucket, src_key);
        check_copy_preconditions(req, src_meta);
        std::optional<storage::ByteRange> range;
        if (auto r = req.headers.get("x-amz-copy-source-range"))
            range = parse_copy_source_range(*r, src_meta.size);

        auto stream = co_await src_backend.get_object(src_bucket, src_key, range);
        auto result = co_await router_.resolve(bucket).upload_part(bucket, key, upload_id,
                                                                   part_no, *stream.body);

        XmlWriter w;
        w.open("CopyPartResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
        w.element("LastModified", util::iso8601(std::chrono::system_clock::now()));
        w.element("ETag", quote_etag(result.etag));
        w.close();
        http::HttpResponse resp;
        resp.headers.set("Content-Type", "application/xml");
        resp.small_body = w.str();
        co_return resp;
    }

    http::StringBodyReader empty{""};
    http::BodyReader& body = req.body ? *req.body : static_cast<http::BodyReader&>(empty);
    auto result =
        co_await router_.resolve(bucket).upload_part(bucket, key, upload_id, part_no, body);

    http::HttpResponse resp;
    resp.headers.set("ETag", quote_etag(result.etag));
    co_return resp;
}

Task<http::HttpResponse> S3Service::complete_multipart(http::HttpRequest& req,
                                                       std::string bucket, std::string key) {
    std::string upload_id = require_upload_id(req);
    std::string body = co_await read_body(req);
    XmlNode root = xml_parse(body);
    if (root.name != "CompleteMultipartUpload")
        throw S3Error(S3ErrorCode::MalformedXML, "Expected <CompleteMultipartUpload> root.");

    std::vector<storage::PartInfo> parts;
    for (auto& child : root.children) {
        if (child.name != "Part") continue;
        // 分片数上界（docs/gaps.md §5.7）：此前无限追加，一份构造好的 XML 就能
        // 让本请求把任意长的列表读进内存
        if (parts.size() >= size_t(storage::kMaxParts))
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "The upload contains more than the maximum number of allowed parts.");
        storage::PartInfo p;
        std::string no = child.get("PartNumber");
        auto [ptr, ec] = std::from_chars(no.data(), no.data() + no.size(), p.part_no);
        if (ec != std::errc() || ptr != no.data() + no.size())
            throw S3Error(S3ErrorCode::MalformedXML, "Invalid PartNumber.");
        // 上界复核：upload 时校验过，complete 的列表是另一份输入
        storage::validate_part_number(p.part_no);
        p.etag = child.get("ETag");
        parts.push_back(std::move(p));
    }
    storage::validate_part_order(parts);  // 乱序 → InvalidPartOrder，先于后端判定

    auto& backend = router_.resolve(bucket);
    // 最小分片 5MiB（末片除外）：不校验则 10000 个 1 字节分片也能提交，complete
    // 逐个 open/read/write 拼接是廉价的放大面。尺寸只有存储层知道，故先列一次
    co_await check_min_part_sizes(backend, bucket, key, upload_id, parts, min_part_size_);

    auto result = co_await backend.complete_multipart(bucket, key, upload_id, parts);
    metrics_.mpu_finished();

    XmlWriter w;
    w.open("CompleteMultipartUploadResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    // Location 回完整 URL（部分 Java SDK 直接当 URL 用）。用 req.path 重建可同时
    // 覆盖 path-style 与 vhost 两种寻址——vhost 下 req.path 本就只有 key
    w.element("Location", request_base_url(req) + req.path);
    w.element("Bucket", bucket);
    w.element("Key", key);
    w.element("ETag", quote_etag(result.etag));
    w.close();
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = w.str();
    co_return resp;
}

Task<http::HttpResponse> S3Service::abort_multipart(http::HttpRequest& req, std::string bucket,
                                                    std::string key) {
    co_await router_.resolve(bucket).abort_multipart(bucket, key, require_upload_id(req));
    metrics_.mpu_finished();
    http::HttpResponse resp;
    resp.status = 204;
    co_return resp;
}

Task<http::HttpResponse> S3Service::list_parts(http::HttpRequest& req, std::string bucket,
                                               std::string key) {
    std::string upload_id = require_upload_id(req);
    // 此前 max-parts / part-number-marker 一个都不读，还恒报 IsTruncated=false
    //（docs/gaps.md §5.1）
    storage::ListPartsOptions opt;
    opt.max_parts = parse_max(req, "max-parts", 1000);
    opt.part_number_marker = parse_int_param(req, "part-number-marker", 0);
    if (opt.part_number_marker < 0)
        throw S3Error(S3ErrorCode::InvalidArgument, "Invalid part-number-marker value");

    auto res = co_await router_.resolve(bucket).list_parts(bucket, key, upload_id, opt);

    XmlWriter w;
    w.open("ListPartsResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    w.element("Bucket", bucket);
    w.element("Key", key);
    w.element("UploadId", upload_id);
    w.element("StorageClass", "STANDARD");
    w.element("PartNumberMarker", static_cast<uint64_t>(opt.part_number_marker));
    if (res.is_truncated)
        w.element("NextPartNumberMarker", static_cast<uint64_t>(res.next_part_number_marker));
    w.element("MaxParts", static_cast<uint64_t>(opt.max_parts));
    w.element("IsTruncated", res.is_truncated ? "true" : "false");
    for (auto& p : res.parts) {
        w.open("Part");
        w.element("PartNumber", static_cast<uint64_t>(p.part_no));
        w.element("LastModified", util::iso8601(p.last_modified));
        w.element("ETag", quote_etag(p.etag));
        w.element("Size", p.size);
        w.close();
    }
    w.close();
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = w.str();
    co_return resp;
}

Task<http::HttpResponse> S3Service::list_multipart_uploads(http::HttpRequest& req,
                                                           std::string bucket,
                                                           const RequestAuth& auth) {
    // 此前 `(void)req;`——prefix/delimiter/三个 marker/max-uploads 一个都不读，
    // 却硬编码 MaxUploads=1000 与 IsTruncated=false 后返回全部 upload
    storage::ListUploadsOptions opt;
    opt.prefix = req.query_get("prefix").value_or("");
    opt.delimiter = req.query_get("delimiter").value_or("");
    opt.max_uploads = parse_max(req, "max-uploads", 1000);
    opt.key_marker = req.query_get("key-marker").value_or("");
    opt.upload_id_marker = req.query_get("upload-id-marker").value_or("");
    // upload-id-marker 单独出现无意义（游标是二元组），AWS 同样按 400 处理
    if (opt.key_marker.empty() && !opt.upload_id_marker.empty())
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "upload-id-marker requires key-marker to be specified.");
    if (!opt.delimiter.empty() && opt.delimiter != "/")
        throw S3Error(S3ErrorCode::NotImplemented,
                      "Only '/' is supported as a delimiter.");

    auto res = co_await router_.resolve(bucket).list_multipart_uploads(bucket, opt);

    // policy prefix 过滤：同 list_objects——否则 prefix 受限凭证可枚举他租户
    // 进行中 upload 的 key
    if (auth.policy && !auth.policy->prefixes.empty()) {
        std::erase_if(res.uploads,
                      [&](const auto& u) { return !auth.policy->allows_key(u.key); });
        std::erase_if(res.common_prefixes, [&](const std::string& p) {
            return !auth.policy->prefix_may_contain(p);
        });
    }

    XmlWriter w;
    w.open("ListMultipartUploadsResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    w.element("Bucket", bucket);
    w.element("KeyMarker", opt.key_marker);
    w.element("UploadIdMarker", opt.upload_id_marker);
    if (res.is_truncated) {
        w.element("NextKeyMarker", res.next_key_marker);
        w.element("NextUploadIdMarker", res.next_upload_id_marker);
    }
    if (!opt.prefix.empty()) w.element("Prefix", opt.prefix);
    if (!opt.delimiter.empty()) w.element("Delimiter", opt.delimiter);
    w.element("MaxUploads", static_cast<uint64_t>(opt.max_uploads));
    w.element("IsTruncated", res.is_truncated ? "true" : "false");
    for (auto& u : res.uploads) {
        w.open("Upload");
        w.element("Key", u.key);
        w.element("UploadId", u.upload_id);
        w.element("Initiated", util::iso8601(u.initiated));
        w.element("StorageClass", "STANDARD");
        w.close();
    }
    for (auto& p : res.common_prefixes) {
        w.open("CommonPrefixes");
        w.element("Prefix", p);
        w.close();
    }
    w.close();
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = w.str();
    co_return resp;
}

}  // namespace lights3::s3
