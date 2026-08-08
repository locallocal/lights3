// ListObjectsV2（GET /bucket?list-type=2；V1 请求按 V2 语义降级处理）
#include <algorithm>

#include "core/util/checksum.h"
#include "core/util/time.h"
#include "core/util/uri.h"
#include "s3/handlers/common.h"
#include "s3/service.h"
#include "s3/xml.h"

namespace lights3::s3 {

using handlers::kOwnerId;

namespace {

// V2 continuation-token 的不透明化（docs/gaps.md §4）：V1 的 marker 语义上就是
// key（响应会回显），V2 的 token 规范是不透明串——此前 V1 做了 URL 编码而 V2
// 明文透传，两版本不一致且把内部键序直接暴露成 API。base64 一层对齐 AWS 形态
std::string token_encode(const std::string& in) { return util::base64_encode(in); }
std::optional<std::string> token_decode(const std::string& in) {
    return util::base64_decode(in);
}

}  // namespace

Task<http::HttpResponse> S3Service::list_objects(http::HttpRequest& req, std::string bucket) {
    storage::ListOptions opt;
    opt.prefix = req.query_get("prefix").value_or("");
    opt.delimiter = req.query_get("delimiter").value_or("");
    if (auto v = req.query_get("max-keys")) {
        try {
            opt.max_keys = std::stoi(*v);
        } catch (...) {
            throw S3Error(S3ErrorCode::InvalidArgument, "Invalid max-keys value");
        }
        if (opt.max_keys < 0)
            throw S3Error(S3ErrorCode::InvalidArgument, "Invalid max-keys value");
        // S3 语义：上限 1000，超出静默钳制——不钳制时 max-keys=INT_MAX 会把
        // 整桶列表一次性构造进内存（数百 MB XML，单请求 OOM 面）
        opt.max_keys = std::min(opt.max_keys, 1000);
    }
    // encoding-type=url（S3 语义）：响应里的 key/prefix 等经 URL 编码返回；
    // 其他取值拒绝
    bool encode_url = false;
    if (auto et = req.query_get("encoding-type")) {
        if (*et != "url")
            throw S3Error(S3ErrorCode::InvalidArgument,
                          "Invalid Encoding Method specified in Request");
        encode_url = true;
    }
    auto enc = [&](const std::string& s) {
        return encode_url ? util::aws_uri_encode(s, /*encode_slash=*/false) : s;
    };
    // V2（?list-type=2）与 V1 的差异：KeyCount/ContinuationToken vs Marker
    bool v2 = req.query_get("list-type").value_or("") == "2";
    // 三种 marker 各归各版本（docs/gaps.md §5.5）：此前塌缩成同一个 start_after，
    // 于是 V1 请求带 start-after 也生效、且响应回显出客户端从未发过的 <Marker>。
    // V2 认 continuation-token（本实现签发的不透明串）与 start-after；V1 只认
    // marker，两者都是"从此 key 之后开始"的明文语义
    std::optional<std::string> start_after_param;  // 仅 V2，需原样回显
    if (v2) {
        if (auto tok = req.query_get("continuation-token")) {
            auto key = token_decode(*tok);
            if (!key)
                throw S3Error(S3ErrorCode::InvalidArgument,
                              "The continuation token provided is incorrect.");
            opt.start_after = std::move(*key);
            // AWS：两者同时出现时 continuation-token 胜出，start-after 被忽略
            start_after_param = req.query_get("start-after");
        } else if (auto sa = req.query_get("start-after")) {
            opt.start_after = *sa;
            start_after_param = *sa;
        }
    } else {
        opt.start_after = req.query_get("marker").value_or("");
    }
    // fetch-owner=true（V2）：本实现只有单一所有者，与 ListAllMyBuckets 同源
    bool fetch_owner = v2 && req.query_get("fetch-owner").value_or("") == "true";

    auto result = co_await router_.resolve(bucket).list_objects(bucket, opt);

    XmlWriter w;
    w.open("ListBucketResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    w.element("Name", bucket);
    w.element("Prefix", enc(opt.prefix));
    if (encode_url) w.element("EncodingType", "url");
    if (!opt.delimiter.empty()) w.element("Delimiter", enc(opt.delimiter));
    w.element("MaxKeys", static_cast<uint64_t>(opt.max_keys));
    if (v2) {
        w.element("KeyCount",
                  static_cast<uint64_t>(result.objects.size() + result.common_prefixes.size()));
        if (auto tok = req.query_get("continuation-token"))
            w.element("ContinuationToken", *tok);
        if (start_after_param) w.element("StartAfter", enc(*start_after_param));
    } else {
        w.element("Marker", enc(opt.start_after));
    }
    w.element("IsTruncated", result.is_truncated ? "true" : "false");
    if (result.is_truncated)
        w.element(v2 ? "NextContinuationToken" : "NextMarker",
                  v2 ? token_encode(result.next_token) : enc(result.next_token));
    for (auto& o : result.objects) {
        w.open("Contents");
        w.element("Key", enc(o.key));
        w.element("LastModified", util::iso8601(o.last_modified));
        w.element("ETag", "\"" + o.etag + "\"");
        w.element("Size", o.size);
        w.element("StorageClass", "STANDARD");
        if (fetch_owner) {
            w.open("Owner");
            w.element("ID", std::string(kOwnerId));
            w.element("DisplayName", std::string(kOwnerId));
            w.close();
        }
        w.close();
    }
    for (auto& p : result.common_prefixes) {
        w.open("CommonPrefixes");
        w.element("Prefix", enc(p));
        w.close();
    }
    w.close();

    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = w.str();
    co_return resp;
}

}  // namespace lights3::s3
