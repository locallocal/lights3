// multipart handler: Create/UploadPart/Complete/Abort/ListParts/ListMultipartUploads
// (docs/s3-protocol.md §1; storage-layer semantics in docs/storage-backend.md §3.2)
#include <charconv>
#include <algorithm>
#include <map>

#include "core/util/time.h"
#include "core/util/uri.h"
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

// Integer query parameters: default to def when absent, any invalid value is 400 (silently falling back to the
// default would let clients believe their pagination took effect)
int parse_int_param(const http::HttpRequest& req, const char* name, int def) {
    auto v = req.query_get(name);
    if (!v || v->empty()) return def;
    int out = 0;
    auto [p, ec] = std::from_chars(v->data(), v->data() + v->size(), out);
    if (ec != std::errc() || p != v->data() + v->size())
        throw S3Error(S3ErrorCode::InvalidArgument, std::string("Invalid ") + name + " value");
    return out;
}

// max-* mirrors ListObjects' max-keys: negative is 400, above the cap is silently clamped -- without clamping,
// max=INT_MAX would build the whole table in memory at once
int parse_max(const http::HttpRequest& req, const char* name, int cap) {
    int v = parse_int_param(req, name, cap);
    if (v < 0) throw S3Error(S3ErrorCode::InvalidArgument, std::string("Invalid ") + name + " value");
    return std::min(v, cap);
}

// "scheme://host": Location must be a full URL (docs/archive/gaps.md §5.7). The scheme can only be relayed by the reverse
// proxy -- on direct connections this implementation is plaintext HTTP, TLS is terminated by a front proxy (docs/s3-protocol.md)
std::string request_base_url(const http::HttpRequest& req) {
    std::string scheme = "http";
    if (auto p = req.headers.get("X-Forwarded-Proto"); p && !p->empty()) scheme = *p;
    std::string host = req.headers.get("Host").value_or("");
    return scheme + "://" + host;
}

// Pre-complete validation over one listing: minimum part size (docs/archive/gaps.md §5.7,
// last part exempt) plus declared-checksum cross-check (roadmap §2.2 — the complete XML's
// Checksum values are client claims and must match the stored, upload-time-verified
// ones; a mismatch is BadDigest, a claim against a checksum-less part is InvalidPart).
// Missing parts are not reported here but left to the backend's InvalidPart
Task<void> check_parts_before_complete(storage::IStorageBackend& backend,
                                       const std::string& bucket, const std::string& key,
                                       const std::string& upload_id,
                                       const std::vector<storage::PartInfo>& parts,
                                       uint64_t min_size) {
    bool need_sizes = min_size > 0 && parts.size() > 1;
    bool need_checksums = false;
    for (auto& p : parts)
        if (!p.checksum_value.empty()) need_checksums = true;
    if (!need_sizes && !need_checksums) co_return;
    // Fetch everything: the protocol caps part count at 10000, and this needs lookup by part number, not paging
    storage::ListPartsOptions all_opt;
    all_opt.max_parts = storage::kMaxParts;
    auto have = co_await backend.list_parts(bucket, key, upload_id, all_opt);
    std::map<int, const storage::PartMeta*> by_no;
    for (auto& p : have.parts) by_no[p.part_no] = &p;
    for (size_t i = 0; i < parts.size(); ++i) {
        auto it = by_no.find(parts[i].part_no);
        if (it == by_no.end()) continue;
        if (need_sizes && i + 1 < parts.size() && it->second->size < min_size)  // last part exempt
            throw S3Error(S3ErrorCode::EntityTooSmall,
                          "Your proposed upload is smaller than the minimum allowed size. "
                          "Part " + std::to_string(parts[i].part_no) + " is " +
                              std::to_string(it->second->size) + " bytes; the minimum is " +
                              std::to_string(min_size) + " bytes.");
        if (!parts[i].checksum_value.empty()) {
            if (it->second->checksum_value.empty())
                throw S3Error(S3ErrorCode::InvalidPart,
                              "Part " + std::to_string(parts[i].part_no) +
                                  " was uploaded without a checksum but the complete "
                                  "request declares one.");
            if (it->second->checksum_algorithm != parts[i].checksum_algorithm ||
                it->second->checksum_value != parts[i].checksum_value)
                throw S3Error(S3ErrorCode::BadDigest,
                              "The Checksum" + parts[i].checksum_algorithm + " of part " +
                                  std::to_string(parts[i].part_no) +
                                  " did not match what we received.");
        }
    }
}

// "bytes=first-last": both ends required, closed interval (AWS UploadPartCopy semantics, stricter than GET Range's
// lenient parsing -- suffix/open-ended forms are all InvalidArgument here)
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
    auto meta = meta_from_headers(req);
    // Declared checksum algorithm survives create→complete (roadmap §2.2). Only the
    // COMPOSITE form is implemented: CRC64NVME (full-object only per AWS) and an
    // explicit FULL_OBJECT request get an honest 501 instead of a silently absent
    // checksum at complete time
    for (std::string_view h : {"x-amz-checksum-algorithm", "x-amz-sdk-checksum-algorithm"}) {
        auto v = req.headers.get(h);
        if (!v) continue;  // unknown values were already rejected by the dispatch guard
        std::string algo;
        for (char c : *v) algo.push_back(char(toupper(static_cast<unsigned char>(c))));
        if (algo == "CRC64NVME")
            throw S3Error(S3ErrorCode::NotImplemented,
                          "Full-object multipart checksums (CRC64NVME) are not implemented; "
                          "use CRC32, CRC32C, SHA1 or SHA256 (composite).");
        meta.checksum_algorithm = algo;
        meta.checksum_type = "COMPOSITE";
    }
    if (auto t = req.headers.get("x-amz-checksum-type")) {
        if (http::HeaderMap::ieq(*t, "FULL_OBJECT"))
            throw S3Error(S3ErrorCode::NotImplemented,
                          "Full-object multipart checksums are not implemented; only "
                          "COMPOSITE is supported.");
        if (!http::HeaderMap::ieq(*t, "COMPOSITE"))
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "Invalid x-amz-checksum-type value: " + *t);
    }
    auto upload_id =
        co_await router_.resolve(bucket).create_multipart(bucket, key, std::move(meta));
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

    // UploadPartCopy (docs/s3-protocol.md §1): after conditional headers are checked via head, the source is
    // streamed out by range and written into the target upload as the part body; source/target may be on
    // different backends (same as CopyObject)
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

    require_content_length(req);  // 411 (roadmap §2.5); UploadPartCopy above is body-less and exempt
    // Declared part checksum persists with the part record (roadmap §2.2)
    auto part_checksum = extract_part_checksum(req);
    http::StringBodyReader empty{""};
    http::BodyReader& body = req.body ? *req.body : static_cast<http::BodyReader&>(empty);
    auto result = co_await router_.resolve(bucket).upload_part(bucket, key, upload_id, part_no,
                                                               body, part_checksum);

    http::HttpResponse resp;
    resp.headers.set("ETag", quote_etag(result.etag));
    if (part_checksum) {  // body drained: trailer-form value is resolvable now
        std::string v = part_checksum->resolved();
        if (!v.empty()) {
            std::string h = "x-amz-checksum-";
            for (char c : part_checksum->algorithm) h.push_back(http::HeaderMap::lower(c));
            resp.headers.set(h, v);
        }
    }
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
        // Part count upper bound (docs/archive/gaps.md §5.7): previously unbounded appends, so one crafted XML could
        // make this request read an arbitrarily long list into memory
        if (parts.size() >= size_t(storage::kMaxParts))
            throw S3Error(S3ErrorCode::InvalidRequest,
                          "The upload contains more than the maximum number of allowed parts.");
        storage::PartInfo p;
        std::string no = child.get("PartNumber");
        auto [ptr, ec] = std::from_chars(no.data(), no.data() + no.size(), p.part_no);
        if (ec != std::errc() || ptr != no.data() + no.size())
            throw S3Error(S3ErrorCode::MalformedXML, "Invalid PartNumber.");
        // Re-check the upper bound: validated at upload time, but complete's list is a separate input
        storage::validate_part_number(p.part_no);
        p.etag = child.get("ETag");
        // Optional per-part checksum claims (roadmap §2.2): cross-checked against the
        // stored values before the backend commit
        for (std::string_view a : {"CRC32", "CRC32C", "CRC64NVME", "SHA1", "SHA256"}) {
            std::string v = child.get("Checksum" + std::string(a));
            if (v.empty()) continue;
            if (!p.checksum_value.empty())
                throw S3Error(S3ErrorCode::MalformedXML,
                              "A Part may declare at most one checksum value.");
            p.checksum_algorithm = std::string(a);
            p.checksum_value = std::move(v);
        }
        parts.push_back(std::move(p));
    }
    storage::validate_part_order(parts);  // out of order -> InvalidPartOrder, decided before the backend

    auto& backend = router_.resolve(bucket);
    // Minimum part size 5MiB (last part exempt): without the check, 10000 one-byte parts could be committed, and
    // complete's per-part open/read/write concatenation is a cheap amplification surface. Only the storage layer
    // knows sizes, hence the upfront listing; declared part checksums cross-check in the same pass
    co_await check_parts_before_complete(backend, bucket, key, upload_id, parts,
                                         min_part_size_);

    auto result = co_await backend.complete_multipart(bucket, key, upload_id, parts);
    metrics_.mpu_finished();

    XmlWriter w;
    w.open("CompleteMultipartUploadResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    // Location returns a full URL (some Java SDKs use it directly as a URL). Rebuilding from req.path covers both
    // path-style and vhost addressing -- under vhost, req.path already contains only the key
    w.element("Location", request_base_url(req) + req.path);
    w.element("Bucket", bucket);
    w.element("Key", key);
    w.element("ETag", quote_etag(result.etag));
    // Composite checksum echo (roadmap §2.2): present when every part carried one
    if (!result.checksum_value.empty()) {
        w.element("Checksum" + result.checksum_algorithm, result.checksum_value);
        w.element("ChecksumType",
                  result.checksum_type.empty() ? "COMPOSITE" : result.checksum_type);
    }
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
    // Previously neither max-parts nor part-number-marker was read, and IsTruncated=false was always reported
    //（docs/archive/gaps.md §5.1）
    storage::ListPartsOptions opt;
    opt.max_parts = parse_max(req, "max-parts", 1000);
    opt.part_number_marker = parse_int_param(req, "part-number-marker", 0);
    if (opt.part_number_marker < 0)
        throw S3Error(S3ErrorCode::InvalidArgument, "Invalid part-number-marker value");

    // encoding-type=url (roadmap §2.5): SDKs occasionally pass it through; before it entered
    // the allowlist the whole request was 501. Same semantics as the two bucket listings
    bool encode_url = false;
    if (auto et = req.query_get("encoding-type")) {
        if (*et != "url")
            throw S3Error(S3ErrorCode::InvalidArgument,
                          "Invalid Encoding Method specified in Request");
        encode_url = true;
    }

    auto res = co_await router_.resolve(bucket).list_parts(bucket, key, upload_id, opt);

    XmlWriter w;
    w.open("ListPartsResult", R"(xmlns="http://s3.amazonaws.com/doc/2006-03-01/")");
    w.element("Bucket", bucket);
    w.element("Key", encode_url ? util::aws_uri_encode(key, /*encode_slash=*/false) : key);
    if (encode_url) w.element("EncodingType", "url");
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
        // Stored per-part checksum (roadmap §2.2)
        if (!p.checksum_value.empty() && !p.checksum_algorithm.empty())
            w.element("Checksum" + p.checksum_algorithm, p.checksum_value);
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
    // Previously `(void)req;` -- none of prefix/delimiter/the three markers/max-uploads was read,
    // yet MaxUploads=1000 and IsTruncated=false were hardcoded and every upload returned
    storage::ListUploadsOptions opt;
    opt.prefix = req.query_get("prefix").value_or("");
    opt.delimiter = req.query_get("delimiter").value_or("");
    opt.max_uploads = parse_max(req, "max-uploads", 1000);
    opt.key_marker = req.query_get("key-marker").value_or("");
    opt.upload_id_marker = req.query_get("upload-id-marker").value_or("");
    // upload-id-marker alone is meaningless (the cursor is a pair); AWS likewise treats it as 400
    if (opt.key_marker.empty() && !opt.upload_id_marker.empty())
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "upload-id-marker requires key-marker to be specified.");
    // encoding-type=url: same semantics as list_objects -- previously the parameter was accepted but nothing
    // was ever encoded, a silent wrong answer
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

    auto res = co_await router_.resolve(bucket).list_multipart_uploads(bucket, opt);

    // Policy prefix filtering: same as list_objects -- otherwise a prefix-restricted credential could enumerate
    // other tenants' in-progress upload keys
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
    w.element("KeyMarker", enc(opt.key_marker));
    w.element("UploadIdMarker", opt.upload_id_marker);
    if (res.is_truncated) {
        w.element("NextKeyMarker", enc(res.next_key_marker));
        w.element("NextUploadIdMarker", res.next_upload_id_marker);
    }
    if (!opt.prefix.empty()) w.element("Prefix", enc(opt.prefix));
    if (!opt.delimiter.empty()) w.element("Delimiter", enc(opt.delimiter));
    if (encode_url) w.element("EncodingType", "url");
    w.element("MaxUploads", static_cast<uint64_t>(opt.max_uploads));
    w.element("IsTruncated", res.is_truncated ? "true" : "false");
    for (auto& u : res.uploads) {
        w.open("Upload");
        w.element("Key", enc(u.key));
        w.element("UploadId", u.upload_id);
        w.element("Initiated", util::iso8601(u.initiated));
        w.element("StorageClass", "STANDARD");
        w.close();
    }
    for (auto& p : res.common_prefixes) {
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
