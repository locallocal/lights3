// ListObjectsV2（GET /bucket?list-type=2；V1 请求按 V2 语义降级处理）
#include <algorithm>

#include "core/util/time.h"
#include "core/util/uri.h"
#include "s3/service.h"
#include "s3/xml.h"

namespace lights3::s3 {

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
    // token 即 "start after this key"（V2 continuation-token / start-after，V1 marker）
    opt.start_after = req.query_get("continuation-token")
                          .value_or(req.query_get("start-after")
                                        .value_or(req.query_get("marker").value_or("")));

    auto result = co_await router_.resolve(bucket).list_objects(bucket, opt);

    // V2（?list-type=2）与 V1 的响应差异：KeyCount/ContinuationToken vs Marker
    bool v2 = req.query_get("list-type").value_or("") == "2";

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
    } else {
        w.element("Marker", enc(opt.start_after));
    }
    w.element("IsTruncated", result.is_truncated ? "true" : "false");
    if (result.is_truncated)
        w.element(v2 ? "NextContinuationToken" : "NextMarker",
                  v2 ? result.next_token : enc(result.next_token));
    for (auto& o : result.objects) {
        w.open("Contents");
        w.element("Key", enc(o.key));
        w.element("LastModified", util::iso8601(o.last_modified));
        w.element("ETag", "\"" + o.etag + "\"");
        w.element("Size", o.size);
        w.element("StorageClass", "STANDARD");
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
