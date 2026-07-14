// multipart handler：Create/UploadPart/Complete/Abort/ListParts/ListMultipartUploads
// （docs/05 §1；存储层语义见 docs/04 §3.2）
#include <charconv>

#include "core/util/time.h"
#include "s3/handlers/common.h"
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
    if (req.headers.has("x-amz-copy-source"))
        throw S3Error(S3ErrorCode::NotImplemented, "UploadPartCopy is not implemented.");
    int part_no = parse_part_number(req);
    std::string upload_id = require_upload_id(req);

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
        storage::PartInfo p;
        std::string no = child.get("PartNumber");
        auto [ptr, ec] = std::from_chars(no.data(), no.data() + no.size(), p.part_no);
        if (ec != std::errc() || ptr != no.data() + no.size())
            throw S3Error(S3ErrorCode::MalformedXML, "Invalid PartNumber.");
        p.etag = child.get("ETag");
        parts.push_back(std::move(p));
    }

    auto result =
        co_await router_.resolve(bucket).complete_multipart(bucket, key, upload_id, parts);
    metrics_.mpu_finished();

    XmlWriter w;
    w.open("CompleteMultipartUploadResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    w.element("Location", "/" + bucket + "/" + key);
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
    auto parts = co_await router_.resolve(bucket).list_parts(bucket, key, upload_id);

    XmlWriter w;
    w.open("ListPartsResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    w.element("Bucket", bucket);
    w.element("Key", key);
    w.element("UploadId", upload_id);
    w.element("StorageClass", "STANDARD");
    w.element("MaxParts", uint64_t(10000));
    w.element("IsTruncated", "false");
    for (auto& p : parts) {
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
                                                           std::string bucket) {
    (void)req;
    auto uploads = co_await router_.resolve(bucket).list_multipart_uploads(bucket);

    XmlWriter w;
    w.open("ListMultipartUploadsResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    w.element("Bucket", bucket);
    w.element("MaxUploads", uint64_t(1000));
    w.element("IsTruncated", "false");
    for (auto& u : uploads) {
        w.open("Upload");
        w.element("Key", u.key);
        w.element("UploadId", u.upload_id);
        w.element("Initiated", util::iso8601(u.initiated));
        w.element("StorageClass", "STANDARD");
        w.close();
    }
    w.close();
    http::HttpResponse resp;
    resp.headers.set("Content-Type", "application/xml");
    resp.small_body = w.str();
    co_return resp;
}

}  // namespace lights3::s3
