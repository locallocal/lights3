// ListObjectsV2 (GET /bucket?list-type=2; V1 requests are handled degraded to V2 semantics)
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

// Opacifying the V2 continuation-token (docs/archive/gaps.md §4): V1's marker is semantically a key
// (echoed in the response), while the V2 token is by spec an opaque string -- previously V1 was URL-encoded
// but V2 passed through in plaintext, inconsistent across versions and exposing the internal key order as API.
// A base64 layer aligns with the AWS shape
std::string token_encode(const std::string& in) { return util::base64_encode(in); }
std::optional<std::string> token_decode(const std::string& in) {
    return util::base64_decode(in);
}

}  // namespace

Task<http::HttpResponse> S3Service::list_objects(http::HttpRequest& req, std::string bucket,
                                                 const RequestAuth& auth) {
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
        // S3 semantics: cap at 1000, silently clamp beyond it -- without clamping, max-keys=INT_MAX would
        // build the whole bucket listing in memory at once (hundreds of MB of XML, a single-request OOM surface)
        opt.max_keys = std::min(opt.max_keys, 1000);
    }
    // encoding-type=url (S3 semantics): key/prefix etc. in the response are returned URL-encoded;
    // other values are rejected
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
    // V2 (?list-type=2) vs V1 differences: KeyCount/ContinuationToken vs Marker.
    // Any other list-type value (e.g. a future V3) is InvalidArgument (roadmap §2.5) --
    // silently serving it with V1 semantics would answer a request shape we do not speak
    auto list_type = req.query_get("list-type");
    if (list_type && *list_type != "2")
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Invalid List Type specified in Request");
    bool v2 = list_type.value_or("") == "2";
    // The three markers each belong to their own version (docs/archive/gaps.md §5.5): previously they collapsed into a
    // single start_after, so a V1 request with start-after took effect and the response echoed a <Marker> the
    // client never sent. V2 accepts continuation-token (the opaque string this implementation issues) plus
    // start-after; V1 accepts only marker; both carry the plaintext "start after this key" semantics
    std::optional<std::string> start_after_param;  // V2 only, must be echoed verbatim
    if (v2) {
        if (auto tok = req.query_get("continuation-token")) {
            auto key = token_decode(*tok);
            if (!key)
                throw S3Error(S3ErrorCode::InvalidArgument,
                              "The continuation token provided is incorrect.");
            opt.start_after = std::move(*key);
            // AWS: when both are present, continuation-token wins and start-after is ignored
            start_after_param = req.query_get("start-after");
        } else if (auto sa = req.query_get("start-after")) {
            opt.start_after = *sa;
            start_after_param = *sa;
        }
    } else {
        opt.start_after = req.query_get("marker").value_or("");
    }
    // fetch-owner=true (V2): this implementation has a single owner, same source as ListAllMyBuckets
    bool fetch_owner = v2 && req.query_get("fetch-owner").value_or("") == "true";

    auto result = co_await router_.resolve(bucket).list_objects(bucket, opt);

    // Policy prefix filtering: with Bucket-scope authorization the key is empty and the prefix check is skipped,
    // while opt.prefix is fully client-controlled -- without filtering here, a prefix-restricted credential
    // (multi-tenant shared bucket) could enumerate every key in the bucket via GET /bucket with no prefix.
    // The pagination cursor remains the backend's (a page may hold fewer entries than max-keys, allowed by S3 semantics)
    if (auth.policy && !auth.policy->prefixes.empty()) {
        std::erase_if(result.objects,
                      [&](const auto& o) { return !auth.policy->allows_key(o.key); });
        std::erase_if(result.common_prefixes, [&](const std::string& p) {
            return !auth.policy->prefix_may_contain(p);
        });
    }

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
    // Owner (fetch-owner): the bucket's tenant when it has one, else the legacy identity
    std::string owner_id(kOwnerId), owner_name(kOwnerId);
    if (tenants_) {
        if (std::string t = tenants_->owner_of(bucket); !t.empty()) {
            owner_id = t;
            auto rec = tenants_->find(t);
            owner_name = rec ? rec->display_name : t;
        }
    }
    for (auto& o : result.objects) {
        w.open("Contents");
        w.element("Key", enc(o.key));
        w.element("LastModified", util::iso8601(o.last_modified));
        w.element("ETag", "\"" + o.etag + "\"");
        w.element("Size", o.size);
        w.element("StorageClass", "STANDARD");
        if (fetch_owner) {
            w.open("Owner");
            w.element("ID", owner_id);
            w.element("DisplayName", owner_name);
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
