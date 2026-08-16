// L2 pure-logic tests: mock HttpRequest + memory backend through the full dispatch (docs/architecture.md §2)
#include <set>

#include "core/semaphore.h"
#include "core/util/checksum.h"
#include "core/util/crypto.h"
#include "s3/service.h"
#include "storage/memory/memory_backend.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::s3;

namespace {

storage::BucketRouter make_router() {
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends;
    backends["mem"] = std::make_shared<storage::MemoryBackend>();
    BucketsConfig cfg;
    cfg.default_backend = "mem";
    return storage::BucketRouter::build(cfg, std::move(backends));
}

S3Service make_service_noauth() {
    return S3Service(make_router(), SigV4Authenticator::build(AuthConfig{}));
}

http::HttpRequest make_req(std::string method, std::string path, std::string body = "",
                           std::vector<std::pair<std::string, std::string>> query = {}) {
    http::HttpRequest req;
    req.method = std::move(method);
    req.raw_path = path;
    req.path = std::move(path);
    req.query = query;
    for (auto& [k, v] : query) {
        if (!req.raw_query.empty()) req.raw_query += "&";
        req.raw_query += k + "=" + v;
    }
    req.headers.add("Host", "localhost");
    if (!body.empty()) req.body = std::make_unique<http::StringBodyReader>(std::move(body));
    return req;
}

// DeleteObjects requires an integrity header (§5.6): tests share this builder so each call
// site doesn't have to compute it
http::HttpRequest make_delete_req(std::string path, std::string body,
                                  std::vector<std::pair<std::string, std::string>> query) {
    auto req = make_req("POST", std::move(path), body, std::move(query));
    util::HashStream h(util::HashStream::Algo::Md5);
    h.update(std::span(reinterpret_cast<const uint8_t*>(body.data()), body.size()));
    auto d = h.final_bytes();
    req.headers.add("Content-MD5", util::base64_encode(std::span(d.data(), d.size())));
    return req;
}

std::string body_of(http::HttpResponse& resp) {
    if (!resp.stream_body) return resp.small_body;
    std::string out;
    std::byte buf[8192];
    for (;;) {
        size_t n = sync_wait(resp.stream_body->read(std::span(buf)));
        if (n == 0) break;
        out.append(reinterpret_cast<const char*>(buf), n);
    }
    return out;
}

bool contains(const std::string& s, const std::string& sub) {
    return s.find(sub) != std::string::npos;
}

// A body that never yields data but can be cancelled: read() parks on a semaphore with no
// permits; the request timeout's cooperative cancellation breaks it out of this suspension
// point (the cancellation wiring of docs/concurrency.md §5)
class HangingReader final : public http::BodyReader {
public:
    Task<size_t> read(std::span<std::byte>) override {
        auto p = co_await sem_.acquire();  // token propagates down automatically via the Task promise
        co_return 0;
    }
    std::optional<uint64_t> length() const override { return std::nullopt; }

private:
    AsyncSemaphore sem_{0};
};

// Extract the first <tag>…</tag> text from response XML (test-only; fine for shallow structures)
std::string xelem(const std::string& xml, const std::string& tag) {
    auto open = "<" + tag + ">", close = "</" + tag + ">";
    auto b = xml.find(open);
    if (b == std::string::npos) return "";
    b += open.size();
    auto e = xml.find(close, b);
    return e == std::string::npos ? "" : xml.substr(b, e - b);
}

}  // namespace

TEST(service_put_get_roundtrip) {
    auto svc = make_service_noauth();

    auto create = sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    CHECK_EQ(create.status, 200);

    auto put = sync_wait(svc.dispatch(make_req("PUT", "/bkt/dir/hello.txt", "hi lights3")));
    CHECK_EQ(put.status, 200);
    CHECK(put.headers.has("ETag"));
    CHECK(put.headers.has("x-amz-request-id"));

    auto get = sync_wait(svc.dispatch(make_req("GET", "/bkt/dir/hello.txt")));
    CHECK_EQ(get.status, 200);
    CHECK_EQ(body_of(get), "hi lights3");
    CHECK_EQ(*get.headers.get("ETag"), *put.headers.get("ETag"));

    auto head = sync_wait(svc.dispatch(make_req("HEAD", "/bkt/dir/hello.txt")));
    CHECK_EQ(head.status, 200);
    CHECK_EQ(*head.content_length, uint64_t(10));
    CHECK(!head.stream_body);
}

TEST(service_range_request) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "0123456789")));

    auto req = make_req("GET", "/bkt/k");
    req.headers.add("Range", "bytes=2-5");
    auto resp = sync_wait(svc.dispatch(std::move(req)));
    CHECK_EQ(resp.status, 206);
    CHECK_EQ(body_of(resp), "2345");
    CHECK_EQ(*resp.headers.get("Content-Range"), "bytes 2-5/10");
}

TEST(service_error_xml) {
    auto svc = make_service_noauth();
    auto resp = sync_wait(svc.dispatch(make_req("GET", "/nobucket/k")));
    CHECK_EQ(resp.status, 404);
    CHECK(contains(resp.small_body, "<Code>NoSuchBucket</Code>"));
    CHECK(contains(resp.small_body, "<RequestId>"));

    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto nk = sync_wait(svc.dispatch(make_req("GET", "/bkt/missing")));
    CHECK_EQ(nk.status, 404);
    CHECK(contains(nk.small_body, "<Code>NoSuchKey</Code>"));
}

TEST(service_list_objects_v2) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/a/1.txt", "x")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/a/2.txt", "y")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/b.txt", "z")));

    auto resp = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"list-type", "2"}, {"delimiter", "/"}})));
    CHECK_EQ(resp.status, 200);
    CHECK(contains(resp.small_body, "<Key>b.txt</Key>"));
    CHECK(contains(resp.small_body, "<Prefix>a/</Prefix>"));
    CHECK(contains(resp.small_body, "<KeyCount>2</KeyCount>"));

    auto buckets = sync_wait(svc.dispatch(make_req("GET", "/")));
    CHECK(contains(buckets.small_body, "<Name>bkt</Name>"));
}

TEST(service_delete_semantics) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "v")));

    auto del = sync_wait(svc.dispatch(make_req("DELETE", "/bkt/k")));
    CHECK_EQ(del.status, 204);
    auto again = sync_wait(svc.dispatch(make_req("DELETE", "/bkt/k")));
    CHECK_EQ(again.status, 204);  // idempotent

    auto delb = sync_wait(svc.dispatch(make_req("DELETE", "/bkt")));
    CHECK_EQ(delb.status, 204);
    auto headb = sync_wait(svc.dispatch(make_req("HEAD", "/bkt")));
    CHECK_EQ(headb.status, 404);
    CHECK_EQ(headb.small_body, "");  // HEAD error responses carry no body
}

TEST(service_not_implemented_apis) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    // Explicitly unsupported subresources (docs/s3-protocol.md §1) get an explicit 501 instead of
    // falling into the List/Get catch-all
    for (auto* sub : {"acl", "policy", "versioning", "lifecycle", "tagging"}) {
        auto resp = sync_wait(svc.dispatch(make_req("GET", "/bkt", "", {{sub, ""}})));
        CHECK_EQ(resp.status, 501);
        CHECK(contains(resp.small_body, "NotImplemented"));
    }
    // versioning expressed via the copy-source query is also 501
    auto upc = make_req("PUT", "/bkt/k", "", {{"partNumber", "1"}, {"uploadId", "x"}});
    upc.headers.add("x-amz-copy-source", "/bkt/other?versionId=abc");
    auto resp = sync_wait(svc.dispatch(std::move(upc)));
    CHECK_EQ(resp.status, 501);
}

TEST(service_upload_part_copy) {
    auto svc = make_service_noauth();
    svc.set_min_part_size(0);  // this case tests the flow, not the 5MiB rule (see service_multipart_constraints)
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/src.bin", "0123456789")));

    auto init = sync_wait(svc.dispatch(make_req("POST", "/bkt/dst.bin", "", {{"uploads", ""}})));
    std::string uid = xelem(body_of(init), "UploadId");

    auto copy_part = [&](int no, std::vector<std::pair<std::string, std::string>> hdrs) {
        auto req = make_req("PUT", "/bkt/dst.bin", "",
                            {{"partNumber", std::to_string(no)}, {"uploadId", uid}});
        req.headers.add("x-amz-copy-source", "/bkt/src.bin");
        for (auto& [k, v] : hdrs) req.headers.add(k, v);
        return sync_wait(svc.dispatch(std::move(req)));
    };

    // Full copy + range copy; CopyPartResult carries an ETag; content is correct after complete
    auto p1 = copy_part(1, {});
    CHECK_EQ(p1.status, 200);
    std::string etag1 = xelem(body_of(p1), "ETag");
    CHECK(!etag1.empty());
    auto p2 = copy_part(2, {{"x-amz-copy-source-range", "bytes=2-4"}});
    CHECK_EQ(p2.status, 200);
    std::string etag2 = xelem(body_of(p2), "ETag");

    std::string cxml = "<CompleteMultipartUpload>"
                       "<Part><PartNumber>1</PartNumber><ETag>" + etag1 + "</ETag></Part>"
                       "<Part><PartNumber>2</PartNumber><ETag>" + etag2 + "</ETag></Part>"
                       "</CompleteMultipartUpload>";
    auto done = sync_wait(svc.dispatch(make_req("POST", "/bkt/dst.bin", cxml, {{"uploadId", uid}})));
    CHECK_EQ(done.status, 200);
    auto get = sync_wait(svc.dispatch(make_req("GET", "/bkt/dst.bin")));
    CHECK_EQ(body_of(get), "0123456789234");

    // Error paths: malformed / out-of-bounds range (both InvalidArgument per AWS semantics),
    // unmet source precondition 412, missing source 404
    auto init2 = sync_wait(svc.dispatch(make_req("POST", "/bkt/dst2.bin", "", {{"uploads", ""}})));
    uid = xelem(body_of(init2), "UploadId");
    // helper reuses uid, with the key switched to dst2
    auto copy_part2 = [&](std::vector<std::pair<std::string, std::string>> hdrs,
                          std::string src = "/bkt/src.bin") {
        auto req = make_req("PUT", "/bkt/dst2.bin", "",
                            {{"partNumber", "1"}, {"uploadId", uid}});
        req.headers.add("x-amz-copy-source", std::move(src));
        for (auto& [k, v] : hdrs) req.headers.add(k, v);
        return sync_wait(svc.dispatch(std::move(req)));
    };
    auto bad_form = copy_part2({{"x-amz-copy-source-range", "bytes=2-"}});
    CHECK_EQ(bad_form.status, 400);
    CHECK(contains(bad_form.small_body, "InvalidArgument"));
    auto oob = copy_part2({{"x-amz-copy-source-range", "bytes=5-100"}});
    CHECK_EQ(oob.status, 400);
    auto src_etag = *sync_wait(svc.dispatch(make_req("HEAD", "/bkt/src.bin"))).headers.get("ETag");
    CHECK_EQ(copy_part2({{"x-amz-copy-source-if-none-match", src_etag}}).status, 412);
    CHECK_EQ(copy_part2({{"x-amz-copy-source-if-match", src_etag}}).status, 200);
    CHECK_EQ(copy_part2({}, "/bkt/nope.bin").status, 404);
}

TEST(service_copy_source_cannot_reach_reserved_bucket) {
    // A bucket starting with '.' in the copy-source header bypasses the dispatch-path check and
    // must be rejected separately (otherwise CopyObject/UploadPartCopy could read .sys credential
    // objects)
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto cp = make_req("PUT", "/bkt/leak");
    cp.headers.add("x-amz-copy-source", "/.sys/credentials/SOMEAK");
    auto resp = sync_wait(svc.dispatch(std::move(cp)));
    CHECK_EQ(resp.status, 400);
    CHECK(contains(resp.small_body, "InvalidBucketName"));
}

TEST(service_aws_aligned_edge_semantics) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "0123456789")));

    // Multipart Range: AWS doesn't support it; the whole header is ignored → 200 full object
    // (docs/s3-protocol.md §6)
    auto multi = make_req("GET", "/bkt/k");
    multi.headers.add("Range", "bytes=0-1,3-4");
    auto resp = sync_wait(svc.dispatch(std::move(multi)));
    CHECK_EQ(resp.status, 200);
    CHECK_EQ(body_of(resp), "0123456789");

    // PUT If-None-Match other than '*': AWS also returns 501 (conditional writes only support *)
    auto put = make_req("PUT", "/bkt/k", "new");
    put.headers.add("If-None-Match", "\"someetag\"");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(put))).status, 501);
}

TEST(service_with_auth) {
    AuthConfig acfg;
    acfg.credentials = {{"TESTAK", "test-sk"}};
    auto auth = SigV4Authenticator::build(acfg);
    S3Service svc(make_router(), auth);

    // Unsigned → 403
    auto denied = sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    CHECK_EQ(denied.status, 403);
    CHECK(contains(denied.small_body, "AccessDenied"));

    // Correct signature → accepted (generated by the same signing side)
    auto req = make_req("PUT", "/bkt");
    auth.sign(req, acfg.credentials[0]);
    auto ok = sync_wait(svc.dispatch(std::move(req)));
    CHECK_EQ(ok.status, 200);

    // Signature valid but body hash mismatched → caught in transit
    auto put = make_req("PUT", "/bkt/k", "tampered body");
    auth.sign(put, acfg.credentials[0], util::sha256_hex("original body"));
    auto resp = sync_wait(svc.dispatch(std::move(put)));
    CHECK_EQ(resp.status, 400);
    CHECK(contains(resp.small_body, "XAmzContentSHA256Mismatch"));

    // healthz is exempt from auth
    auto hz = sync_wait(svc.dispatch(make_req("GET", "/-/healthz")));
    CHECK_EQ(hz.status, 200);
}

// ---------- Additional coverage for docs/s3-protocol.md ----------


TEST(service_multipart_flow) {
    auto svc = make_service_noauth();
    svc.set_min_part_size(0);  // this case tests the flow, not the 5MiB rule (see service_multipart_constraints)
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));

    // Create → UploadId
    auto init = sync_wait(svc.dispatch(make_req("POST", "/bkt/mp.bin", "", {{"uploads", ""}})));
    CHECK_EQ(init.status, 200);
    std::string uid = xelem(body_of(init), "UploadId");
    CHECK(!uid.empty());

    // Two parts
    auto p1 = sync_wait(svc.dispatch(
        make_req("PUT", "/bkt/mp.bin", "hello ", {{"partNumber", "1"}, {"uploadId", uid}})));
    CHECK_EQ(p1.status, 200);
    std::string etag1 = *p1.headers.get("ETag");
    auto p2 = sync_wait(svc.dispatch(
        make_req("PUT", "/bkt/mp.bin", "world", {{"partNumber", "2"}, {"uploadId", uid}})));
    std::string etag2 = *p2.headers.get("ETag");

    // ListParts / ListMultipartUploads
    auto lp = sync_wait(svc.dispatch(make_req("GET", "/bkt/mp.bin", "", {{"uploadId", uid}})));
    CHECK_EQ(lp.status, 200);
    auto lp_body = body_of(lp);
    CHECK(contains(lp_body, "<PartNumber>1</PartNumber>"));
    CHECK(contains(lp_body, "<PartNumber>2</PartNumber>"));
    auto lu = sync_wait(svc.dispatch(make_req("GET", "/bkt", "", {{"uploads", ""}})));
    CHECK(contains(body_of(lu), "<UploadId>" + uid + "</UploadId>"));

    // Complete (XML request body)
    std::string cxml = "<CompleteMultipartUpload>"
                       "<Part><PartNumber>1</PartNumber><ETag>" + etag1 + "</ETag></Part>"
                       "<Part><PartNumber>2</PartNumber><ETag>" + etag2 + "</ETag></Part>"
                       "</CompleteMultipartUpload>";
    auto done = sync_wait(svc.dispatch(
        make_req("POST", "/bkt/mp.bin", cxml, {{"uploadId", uid}})));
    CHECK_EQ(done.status, 200);
    CHECK(contains(xelem(body_of(done), "ETag"), "-2"));  // composite ETag rule

    auto get = sync_wait(svc.dispatch(make_req("GET", "/bkt/mp.bin")));
    CHECK_EQ(body_of(get), "hello world");

    // Abort path + upload gone after completion
    auto again = sync_wait(svc.dispatch(
        make_req("POST", "/bkt/mp.bin", cxml, {{"uploadId", uid}})));
    CHECK_EQ(again.status, 404);  // NoSuchUpload
    auto init2 = sync_wait(svc.dispatch(make_req("POST", "/bkt/mp.bin", "", {{"uploads", ""}})));
    std::string uid2 = xelem(body_of(init2), "UploadId");
    auto ab = sync_wait(svc.dispatch(
        make_req("DELETE", "/bkt/mp.bin", "", {{"uploadId", uid2}})));
    CHECK_EQ(ab.status, 204);
}

TEST(service_delete_objects_batch) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    for (auto* k : {"a", "b", "c"})
        sync_wait(svc.dispatch(make_req("PUT", std::string("/bkt/") + k, "x")));

    std::string xml = "<Delete><Object><Key>a</Key></Object>"
                      "<Object><Key>b</Key></Object></Delete>";
    auto resp = sync_wait(svc.dispatch(make_delete_req("/bkt", xml, {{"delete", ""}})));
    CHECK_EQ(resp.status, 200);
    auto body = body_of(resp);
    CHECK(contains(body, "<Deleted><Key>a</Key></Deleted>"));
    CHECK(contains(body, "<Deleted><Key>b</Key></Deleted>"));

    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/a"))).status, 404);
    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/c"))).status, 200);

    // Bad XML → MalformedXML
    auto bad = sync_wait(svc.dispatch(make_delete_req("/bkt", "<oops>", {{"delete", ""}})));
    CHECK_EQ(bad.status, 400);
    CHECK(contains(bad.small_body, "MalformedXML"));
}

TEST(service_copy_object) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto put = make_req("PUT", "/bkt/src.txt", "copy me");
    put.headers.add("Content-Type", "text/plain");
    put.headers.add("x-amz-meta-color", "red");
    auto put_resp = sync_wait(svc.dispatch(std::move(put)));
    std::string src_etag = *put_resp.headers.get("ETag");

    // COPY (the default): data and metadata are copied together
    auto cp = make_req("PUT", "/bkt/dst.txt");
    cp.headers.add("x-amz-copy-source", "/bkt/src.txt");
    auto cp_resp = sync_wait(svc.dispatch(std::move(cp)));
    CHECK_EQ(cp_resp.status, 200);
    CHECK(contains(body_of(cp_resp), "CopyObjectResult"));
    auto got = sync_wait(svc.dispatch(make_req("GET", "/bkt/dst.txt")));
    CHECK_EQ(body_of(got), "copy me");
    CHECK_EQ(*got.headers.get("Content-Type"), "text/plain");
    CHECK_EQ(*got.headers.get("x-amz-meta-color"), "red");

    // REPLACE: swap in new metadata
    auto rp = make_req("PUT", "/bkt/dst2.txt");
    rp.headers.add("x-amz-copy-source", "/bkt/src.txt");
    rp.headers.add("x-amz-metadata-directive", "REPLACE");
    rp.headers.add("Content-Type", "application/json");
    sync_wait(svc.dispatch(std::move(rp)));
    auto got2 = sync_wait(svc.dispatch(make_req("HEAD", "/bkt/dst2.txt")));
    CHECK_EQ(*got2.headers.get("Content-Type"), "application/json");
    CHECK(!got2.headers.has("x-amz-meta-color"));

    // Unmet copy-source precondition → 412; self-copy with COPY → 400
    auto cond = make_req("PUT", "/bkt/dst3.txt");
    cond.headers.add("x-amz-copy-source", "/bkt/src.txt");
    cond.headers.add("x-amz-copy-source-if-none-match", src_etag);
    CHECK_EQ(sync_wait(svc.dispatch(std::move(cond))).status, 412);
    auto self = make_req("PUT", "/bkt/src.txt");
    self.headers.add("x-amz-copy-source", "/bkt/src.txt");
    auto self_resp = sync_wait(svc.dispatch(std::move(self)));
    CHECK_EQ(self_resp.status, 400);
    CHECK(contains(self_resp.small_body, "InvalidRequest"));
}

TEST(service_conditional_requests) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/c.txt", "v1")));

    // If-Modified-Since in the future → 304; in the past → 200
    auto ims = make_req("GET", "/bkt/c.txt");
    ims.headers.add("If-Modified-Since", "Fri, 01 Jan 2100 00:00:00 GMT");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(ims))).status, 304);
    auto ims2 = make_req("GET", "/bkt/c.txt");
    ims2.headers.add("If-Modified-Since", "Mon, 01 Jan 2001 00:00:00 GMT");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(ims2))).status, 200);

    // If-Unmodified-Since in the past → 412
    auto ius = make_req("HEAD", "/bkt/c.txt");
    ius.headers.add("If-Unmodified-Since", "Mon, 01 Jan 2001 00:00:00 GMT");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(ius))).status, 412);

    // PUT If-None-Match:* prevents overwrites (docs/s3-protocol.md §6)
    auto pin = make_req("PUT", "/bkt/c.txt", "v2");
    pin.headers.add("If-None-Match", "*");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(pin))).status, 412);
    auto pin2 = make_req("PUT", "/bkt/new.txt", "v1");
    pin2.headers.add("If-None-Match", "*");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(pin2))).status, 200);

    // PUT If-Match: etag mismatch → 412
    auto pim = make_req("PUT", "/bkt/c.txt", "v2");
    pim.headers.add("If-Match", "\"deadbeef\"");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(pim))).status, 412);
}

TEST(service_bucket_location_and_list_v1v2) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k1", "x")));

    auto loc = sync_wait(svc.dispatch(make_req("GET", "/bkt", "", {{"location", ""}})));
    CHECK_EQ(loc.status, 200);
    CHECK(contains(body_of(loc), "LocationConstraint"));

    // V1: Marker, no KeyCount; V2: KeyCount
    auto v1 = sync_wait(svc.dispatch(make_req("GET", "/bkt")));
    auto v1b = body_of(v1);
    CHECK(contains(v1b, "<Marker>"));
    CHECK(!contains(v1b, "KeyCount"));
    auto v2 = sync_wait(svc.dispatch(make_req("GET", "/bkt", "", {{"list-type", "2"}})));
    CHECK(contains(body_of(v2), "<KeyCount>1</KeyCount>"));
}

TEST(service_virtual_host_style) {
    S3Service svc(make_router(), SigV4Authenticator::build(AuthConfig{}), "s3.local");

    auto create = make_req("PUT", "/");
    create.headers.set("Host", "vbkt.s3.local");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(create))).status, 200);

    auto put = make_req("PUT", "/dir/a.txt", "vh data");
    put.headers.set("Host", "vbkt.s3.local:9000");  // port is stripped
    CHECK_EQ(sync_wait(svc.dispatch(std::move(put))).status, 200);

    auto get = make_req("GET", "/dir/a.txt");
    get.headers.set("Host", "vbkt.s3.local");
    auto resp = sync_wait(svc.dispatch(std::move(get)));
    CHECK_EQ(resp.status, 200);
    CHECK_EQ(body_of(resp), "vh data");

    // A Host that doesn't match base_domain still goes path-style
    auto ps = sync_wait(svc.dispatch(make_req("GET", "/vbkt/dir/a.txt")));
    CHECK_EQ(body_of(ps), "vh data");
}

TEST(service_observability_endpoints) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));

    auto ready = sync_wait(svc.dispatch(make_req("GET", "/-/readyz")));
    CHECK_EQ(ready.status, 200);
    CHECK(contains(ready.small_body, "ok"));

    auto metrics = sync_wait(svc.dispatch(make_req("GET", "/-/metrics")));
    CHECK_EQ(metrics.status, 200);
    CHECK(contains(metrics.small_body, "lights3_requests_total"));
    CHECK(contains(metrics.small_body, "lights3_request_duration_seconds_bucket"));
    CHECK(contains(metrics.small_body, "lights3_inflight_requests"));
}

// Backend-level metrics registry: once injected, /-/metrics renders it appended after the L2
// request metrics; without injection (previous case) only the L2 part appears
TEST(service_backend_metrics_appended) {
    auto svc = make_service_noauth();
    auto reg = std::make_shared<MetricsRegistry>();
    reg->counter("lights3_duostore_gc_runs_total", "", {{"backend", "duo1"}})->inc(2);
    svc.set_backend_metrics(reg);

    auto metrics = sync_wait(svc.dispatch(make_req("GET", "/-/metrics")));
    CHECK_EQ(metrics.status, 200);
    CHECK(contains(metrics.small_body, "lights3_requests_total"));
    CHECK(contains(metrics.small_body,
                   "lights3_duostore_gc_runs_total{backend=\"duo1\"} 2\n"));
}

// ---------- Regression cases found during review ----------

// Explicit rejection of ?versionId: DELETE ?versionId= must not silently delete the current object
TEST(service_version_id_rejected) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "keep me")));

    auto del = sync_wait(
        svc.dispatch(make_req("DELETE", "/bkt/k", "", {{"versionId", "abc"}})));
    CHECK_EQ(del.status, 501);
    auto get = sync_wait(svc.dispatch(make_req("GET", "/bkt/k")));
    CHECK_EQ(get.status, 200);  // object was not deleted by mistake
    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/k", "",
                                             {{"versionId", "abc"}}))).status, 501);
}

// HEAD + Range: returns 206/Content-Range, aligned with GET; unsatisfiable → 416
TEST(service_head_range) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "0123456789")));

    auto head = make_req("HEAD", "/bkt/k");
    head.headers.add("Range", "bytes=2-5");
    auto resp = sync_wait(svc.dispatch(std::move(head)));
    CHECK_EQ(resp.status, 206);
    CHECK_EQ(*resp.headers.get("Content-Range"), "bytes 2-5/10");
    CHECK_EQ(*resp.content_length, uint64_t(4));

    auto bad = make_req("HEAD", "/bkt/k");
    bad.headers.add("Range", "bytes=99-");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(bad))).status, 416);
}

// RFC 7232 precedence: preconditions (412) are evaluated before Range (416)
TEST(service_precondition_beats_range) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "0123456789")));

    auto req = make_req("GET", "/bkt/k");
    req.headers.add("If-Match", "\"wrong-etag\"");
    req.headers.add("Range", "bytes=99-");  // would 416 on its own
    CHECK_EQ(sync_wait(svc.dispatch(std::move(req))).status, 412);
}

// If-Range: Range only takes effect when the validator matches; otherwise return the full object
TEST(service_if_range) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto put = sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "0123456789")));
    std::string etag = *put.headers.get("ETag");

    auto hit = make_req("GET", "/bkt/k");
    hit.headers.add("Range", "bytes=2-5");
    hit.headers.add("If-Range", etag);
    auto r1 = sync_wait(svc.dispatch(std::move(hit)));
    CHECK_EQ(r1.status, 206);
    CHECK_EQ(body_of(r1), "2345");

    auto miss = make_req("GET", "/bkt/k");
    miss.headers.add("Range", "bytes=2-5");
    miss.headers.add("If-Range", "\"stale-etag\"");
    auto r2 = sync_wait(svc.dispatch(std::move(miss)));
    CHECK_EQ(r2.status, 200);
    CHECK_EQ(body_of(r2), "0123456789");
}

// max-keys is clamped to 1000 (S3 clamps silently); encoding-type only accepts "url"
TEST(service_max_keys_clamp_and_encoding_type) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/a b.txt", "x")));

    auto big = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"list-type", "2"}, {"max-keys", "2147483647"}})));
    CHECK_EQ(big.status, 200);
    CHECK(contains(body_of(big), "<MaxKeys>1000</MaxKeys>"));

    auto enc = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"list-type", "2"}, {"encoding-type", "url"}})));
    auto encb = body_of(enc);
    CHECK(contains(encb, "<EncodingType>url</EncodingType>"));
    CHECK(contains(encb, "<Key>a%20b.txt</Key>"));

    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt", "",
                                             {{"encoding-type", "zzz"}}))).status, 400);
}

// Control characters in object keys are rejected (0x01 makes the ListObjects XML unparseable
// for conforming parsers)
TEST(service_key_control_chars_rejected) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto resp = sync_wait(svc.dispatch(make_req("PUT", "/bkt/a\x01b", "x")));
    CHECK_EQ(resp.status, 400);
    CHECK(contains(resp.small_body, "InvalidArgument"));
}

// /-/admin/credentials prefix boundary: credentialsXYZ must not reach the admin plane (it should
// go to the data plane → XML error)
TEST(service_admin_prefix_boundary) {
    auto svc = make_service_noauth();
    auto resp = sync_wait(svc.dispatch(make_req("GET", "/-/admin/credentialsXYZ")));
    CHECK(*resp.headers.get("Content-Type") == "application/xml");
    CHECK(resp.status == 400 || resp.status == 404);
}

// An IPv6 literal Host is not mangled by rfind(':') (still goes path-style under a vhost config)
TEST(service_ipv6_host_literal) {
    S3Service svc(make_router(), SigV4Authenticator::build(AuthConfig{}), "s3.local");
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto req = make_req("PUT", "/bkt/k", "v6 data");
    req.headers.set("Host", "[::1]:9000");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(req))).status, 200);
    auto get = make_req("GET", "/bkt/k");
    get.headers.set("Host", "[::1]:9000");
    auto resp = sync_wait(svc.dispatch(std::move(get)));
    CHECK_EQ(body_of(resp), "v6 data");
}

// ---------- P0 §1.1: unified bucket name validation ----------

// Under vhost addressing the Host prefix is the bucket; names containing '/', '..' or control
// characters must be rejected at L2 -- otherwise localfs's root_/bucket/key concatenation
// escapes root (fs::path replaces the whole path when given an absolute one), enabling
// arbitrary file reads
TEST(service_vhost_bucket_name_validated) {
    S3Service svc(make_router(), SigV4Authenticator::build(AuthConfig{}), "s3.local");
    for (const char* host : {"/etc.s3.local", "b/../.sys.s3.local", "b/x.s3.local",
                             "ab.s3.local"}) {
        auto req = make_req("GET", "/passwd");
        req.headers.set("Host", host);
        auto resp = sync_wait(svc.dispatch(std::move(req)));
        CHECK_EQ(resp.status, 400);
        CHECK(contains(resp.small_body, "InvalidBucketName"));
    }
    // Domain names are case-insensitive (RFC 4343, docs/gaps.md §2.13): an uppercase Host
    // normalizes to the same bucket as lowercase ("upper" doesn't exist → 404), rather than
    // falling back to path-style or rejecting the uppercase bucket name -- either would make
    // the same URL point to different resources depending on case
    auto req = make_req("GET", "/k");
    req.headers.set("Host", "UPPER.s3.local");
    auto resp = sync_wait(svc.dispatch(std::move(req)));
    CHECK_EQ(resp.status, 404);
    CHECK(contains(resp.small_body, "NoSuchBucket"));
}

// path-style: after decoding, %00 makes the first character NUL rather than '.'; the old
// first-character heuristic would let it through
TEST(service_path_style_bucket_name_validated) {
    auto svc = make_service_noauth();
    for (const char* path : {"/.sys/credentials/x", "/\x01bkt/k", "/AB C/k"}) {
        auto resp = sync_wait(svc.dispatch(make_req("GET", path)));
        CHECK_EQ(resp.status, 400);
        CHECK(contains(resp.small_body, "InvalidBucketName"));
    }
    // Leading NUL: after decoding, bucket.front() == '\0'; the character-set rule rejects it too
    http::HttpRequest nul;
    nul.method = "GET";
    nul.raw_path = "/%00.sys/credentials/x";
    nul.path = std::string("/\0.sys/credentials/x", 20);
    nul.headers.add("Host", "localhost");
    auto resp = sync_wait(svc.dispatch(std::move(nul)));
    CHECK_EQ(resp.status, 400);
}

// copy-source arrives via header and bypasses the dispatch gate; it must be intercepted
// independently with the same validation function
TEST(service_copy_source_bucket_validated) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    for (const char* src : {"/.sys/credentials/AK", "/b\x01d/k", "/A/k"}) {
        auto req = make_req("PUT", "/bkt/copy");
        req.headers.add("x-amz-copy-source", src);
        auto resp = sync_wait(svc.dispatch(std::move(req)));
        CHECK(resp.status == 400);
    }
}

// Valid bucket names are unaffected (reads and writes work under both vhost and path-style)
TEST(service_valid_bucket_still_works_after_validation) {
    S3Service svc(make_router(), SigV4Authenticator::build(AuthConfig{}), "s3.local");
    auto create = make_req("PUT", "/");
    create.headers.set("Host", "my-bkt.s3.local");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(create))).status, 200);
    auto put = make_req("PUT", "/dir/a.txt", "vh data");
    put.headers.set("Host", "my-bkt.s3.local");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(put))).status, 200);
    auto get = sync_wait(svc.dispatch(make_req("GET", "/my-bkt/dir/a.txt")));
    CHECK_EQ(body_of(get), "vh data");
}

// ---------- gaps §3.4: unsupported request headers must 501, not be silently swallowed ----------
// Silent acceptance is more dangerous than an error: on a 200 the client assumes the object
// was encrypted/tagged/locked
TEST(service_unsupported_headers_rejected) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));

    auto try_put = [&](std::string header, std::string value) {
        auto req = make_req("PUT", "/bkt/k", "data");
        req.headers.add(std::move(header), std::move(value));
        return sync_wait(svc.dispatch(std::move(req)));
    };
    for (const char* h : {"x-amz-server-side-encryption",
                          "x-amz-server-side-encryption-customer-algorithm", "x-amz-tagging",
                          "x-amz-object-lock-mode", "x-amz-grant-read"}) {
        auto resp = try_put(h, "whatever");
        CHECK_EQ(resp.status, 501);
        CHECK(contains(resp.small_body, "<Code>NotImplemented</Code>"));
    }
    // x-amz-acl: private matches this implementation's actual semantics, so it passes; all other
    // ACL values are 501
    CHECK_EQ(try_put("x-amz-acl", "private").status, 200);
    CHECK_EQ(try_put("x-amz-acl", "public-read").status, 501);
}

// ---------- gaps §3.5: query whitelist; unknown params get 501 instead of a silent wrong answer ----------
TEST(service_query_whitelist) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "0123456789")));

    // Exact gaps from the blacklist era: each silently degraded to "read the whole object".
    // response-* was implemented in §5.3 and is no longer listed here -- unimplemented
    // subresources must still 501
    for (auto q : {std::pair{"attributes", ""}, std::pair{"partNumber", "1"}}) {
        auto resp = sync_wait(svc.dispatch(make_req("GET", "/bkt/k", "", {{q.first, q.second}})));
        CHECK_EQ(resp.status, 501);
        CHECK(contains(resp.small_body, "<Code>NotImplemented</Code>"));
    }

    // Whitelisted params are unaffected; fetch-owner is implemented (§5.5), no longer "allowed
    // but ignored"
    auto ls = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"list-type", "2"}, {"prefix", ""}, {"fetch-owner", "true"}})));
    CHECK_EQ(ls.status, 200);
    CHECK(contains(ls.small_body, "<Owner>"));

    // The presigned-signature parameter family is allowed globally (values aren't validated here;
    // the SigV4 layer owns that)
    auto pre = sync_wait(svc.dispatch(make_req("GET", "/bkt/k", "", {{"X-Amz-Algorithm", "AWS4-HMAC-SHA256"}})));
    CHECK_EQ(pre.status, 200);
}

// ---------- gaps §3.8: /-/ internal endpoints must not shadow legitimate object keys under vhost ----------
TEST(service_internal_endpoints_not_shadowing_vhost_keys) {
    S3Service svc(make_router(), SigV4Authenticator::build(AuthConfig{}), "s3.local");
    auto create = make_req("PUT", "/");
    create.headers.set("Host", "vbkt.s3.local");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(create))).status, 200);

    // Under vhost, "/-/metrics" is the object key "-/metrics" in vbkt: PUT must actually write it
    // (old behavior: hit the anonymous metrics endpoint and returned 200 without writing the
    // object -- silent data loss)
    auto put = make_req("PUT", "/-/metrics", "real object data");
    put.headers.set("Host", "vbkt.s3.local");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(put))).status, 200);

    auto get = make_req("GET", "/-/metrics");
    get.headers.set("Host", "vbkt.s3.local");
    auto resp = sync_wait(svc.dispatch(std::move(get)));
    CHECK_EQ(resp.status, 200);
    CHECK_EQ(body_of(resp), "real object data");  // object content, not Prometheus text

    // Path-style internal endpoints still work, but only for GET/HEAD: PUT is 405 rather than
    // "200 with data loss"
    auto m = sync_wait(svc.dispatch(make_req("GET", "/-/metrics")));
    CHECK_EQ(m.status, 200);
    CHECK(contains(m.small_body, "lights3_requests_total"));
    auto pm = sync_wait(svc.dispatch(make_req("PUT", "/-/metrics", "x")));
    CHECK_EQ(pm.status, 405);
    auto hh = sync_wait(svc.dispatch(make_req("HEAD", "/-/healthz")));
    CHECK_EQ(hh.status, 200);  // health probes commonly use HEAD
}

// ---------- gaps §4: syntactically invalid Range ignored → 200; V2 token opaque round-trip ----------
TEST(service_malformed_range_ignored) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "0123456789")));

    // "bytes=5-3" (last < first) is syntactically invalid: RFC 9110 requires the whole header
    // to be ignored as invalid, returning 200 with the full object -- previously this wrongly
    // answered 416
    auto req = make_req("GET", "/bkt/k");
    req.headers.add("Range", "bytes=5-3");
    auto resp = sync_wait(svc.dispatch(std::move(req)));
    CHECK_EQ(resp.status, 200);
    CHECK_EQ(body_of(resp), "0123456789");

    // A genuinely unsatisfiable range is still 416
    auto req2 = make_req("GET", "/bkt/k");
    req2.headers.add("Range", "bytes=99-");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(req2))).status, 416);
}

TEST(service_v2_token_opaque_roundtrip) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    for (auto* k : {"a", "b", "c"})
        sync_wait(svc.dispatch(make_req("PUT", std::string("/bkt/") + k, "x")));

    auto p1 = sync_wait(
        svc.dispatch(make_req("GET", "/bkt", "", {{"list-type", "2"}, {"max-keys", "2"}})));
    auto b1 = body_of(p1);
    CHECK(contains(b1, "<IsTruncated>true</IsTruncated>"));
    std::string tok = xelem(b1, "NextContinuationToken");
    CHECK(!tok.empty());
    CHECK(tok != "b");  // opaque (base64), no longer a plaintext key

    auto p2 = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"list-type", "2"}, {"continuation-token", tok}})));
    auto b2 = body_of(p2);
    CHECK(contains(b2, "<Key>c</Key>"));
    CHECK(!contains(b2, "<Key>b</Key>"));

    // An undecodable token → InvalidArgument, rather than being silently used as a plaintext key
    auto bad = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"list-type", "2"}, {"continuation-token", "!!!"}})));
    CHECK_EQ(bad.status, 400);
    CHECK(contains(bad.small_body, "InvalidArgument"));
}

// ---------- gaps §3.9: DeleteObjects malformed inputs and versioned deletes ----------
TEST(service_delete_objects_malformed_inputs) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/a", "x")));

    // An empty list and a missing <Key> are both malformed requests: the whole batch is
    // MalformedXML, not a 200 with an empty result
    auto empty = sync_wait(
        svc.dispatch(make_delete_req("/bkt", "<Delete></Delete>", {{"delete", ""}})));
    CHECK_EQ(empty.status, 400);
    CHECK(contains(empty.small_body, "MalformedXML"));

    auto nokey = sync_wait(svc.dispatch(
        make_delete_req("/bkt", "<Delete><Object></Object></Delete>", {{"delete", ""}})));
    CHECK_EQ(nokey.status, 400);
    CHECK(contains(nokey.small_body, "MalformedXML"));

    // Silently ignoring <VersionId> would turn "delete this version" into "delete the current
    // object": 501, and the object stays
    auto ver = sync_wait(svc.dispatch(make_delete_req(
        "/bkt", "<Delete><Object><Key>a</Key><VersionId>v1</VersionId></Object></Delete>",
        {{"delete", ""}})));
    CHECK_EQ(ver.status, 501);
    CHECK(contains(ver.small_body, "NotImplemented"));
    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/a"))).status, 200);
}

// ---- docs/gaps.md §5.2 / §5.3 / §5.5 / §5.9 ----

TEST(service_first_class_object_metadata) {
    // First-class metadata (§5.2): previously all of it was dropped on PUT and never returned by
    // GET/HEAD. Losing Content-Encoding doesn't just mean "one header fewer" -- the browser
    // gets a byte stream it can't decompress
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));

    auto put = make_req("PUT", "/bkt/o.bin", "payload");
    put.headers.add("Cache-Control", "max-age=42");
    put.headers.add("Content-Disposition", "attachment; filename=\"r.bin\"");
    put.headers.add("Content-Encoding", "gzip");
    put.headers.add("Content-Language", "zh-CN");
    put.headers.add("Expires", "Wed, 21 Oct 2026 07:28:00 GMT");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(put))).status, 200);

    for (auto method : {"GET", "HEAD"}) {
        auto r = sync_wait(svc.dispatch(make_req(method, "/bkt/o.bin")));
        CHECK_EQ(r.status, 200);
        CHECK_EQ(*r.headers.get("Cache-Control"), "max-age=42");
        CHECK_EQ(*r.headers.get("Content-Disposition"), "attachment; filename=\"r.bin\"");
        CHECK_EQ(*r.headers.get("Content-Encoding"), "gzip");
        CHECK_EQ(*r.headers.get("Content-Language"), "zh-CN");
        CHECK_EQ(*r.headers.get("Expires"), "Wed, 21 Oct 2026 07:28:00 GMT");
    }

    // CopyObject's COPY directive must carry the whole set (field-by-field copying once missed
    // newly added fields)
    auto cp = make_req("PUT", "/bkt/copy.bin");
    cp.headers.add("x-amz-copy-source", "/bkt/o.bin");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(cp))).status, 200);
    auto cg = sync_wait(svc.dispatch(make_req("GET", "/bkt/copy.bin")));
    CHECK_EQ(*cg.headers.get("Content-Encoding"), "gzip");
    CHECK_EQ(*cg.headers.get("Cache-Control"), "max-age=42");

    // CR/LF in header values tears the sidecar record apart and is also a response-header
    // injection surface
    auto bad = make_req("PUT", "/bkt/bad.bin", "x");
    bad.headers.add("Cache-Control", "a\r\nX-Injected: 1");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(bad))).status, 400);

    // STANDARD is the only storage class: accepting GLACIER and echoing it back would be lying
    // on the storage layer's behalf
    auto sc = make_req("PUT", "/bkt/sc.bin", "x");
    sc.headers.add("x-amz-storage-class", "GLACIER");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(sc))).status, 501);
    auto sc2 = make_req("PUT", "/bkt/sc2.bin", "x");
    sc2.headers.add("x-amz-storage-class", "STANDARD");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(sc2))).status, 200);
}

TEST(service_response_override_params) {
    // §5.3: the family most used by presigned download links; previously it neither took effect
    // nor errored
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto put = make_req("PUT", "/bkt/o.bin", "payload");
    put.headers.add("Content-Type", "application/octet-stream");
    sync_wait(svc.dispatch(std::move(put)));

    auto r = sync_wait(svc.dispatch(make_req(
        "GET", "/bkt/o.bin", "",
        {{"response-content-type", "text/plain"},
         {"response-content-disposition", "attachment; filename=\"x.txt\""},
         {"response-cache-control", "no-store"}})));
    CHECK_EQ(r.status, 200);
    CHECK_EQ(*r.headers.get("Content-Type"), "text/plain");  // overrides the object's own value
    CHECK_EQ(*r.headers.get("Content-Disposition"), "attachment; filename=\"x.txt\"");
    CHECK_EQ(*r.headers.get("Cache-Control"), "no-store");
    CHECK_EQ(body_of(r), "payload");  // headers change, body doesn't

    // Query values are attacker-controlled: CR/LF must be blocked before they reach response
    // headers (response splitting)
    auto inj = sync_wait(svc.dispatch(
        make_req("GET", "/bkt/o.bin", "", {{"response-content-type", "t\r\nX-Injected: 1"}})));
    CHECK_EQ(inj.status, 400);
}

TEST(service_list_marker_semantics) {
    // §5.5: three kinds of markers previously collapsed into a single start_after
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    for (auto k : {"a", "b", "c"}) sync_wait(svc.dispatch(make_req("PUT", std::string("/bkt/") + k, "x")));

    // V1 only honors marker: start-after on a V1 request must not take effect, and the echoed
    // <Marker> must be empty
    auto v1 = sync_wait(svc.dispatch(make_req("GET", "/bkt", "", {{"start-after", "b"}})));
    CHECK_EQ(v1.status, 200);
    CHECK(contains(v1.small_body, "<Key>a</Key>"));      // start-after did not take effect
    CHECK(contains(v1.small_body, "<Marker></Marker>"));  // don't echo a value the client never sent

    auto v1m = sync_wait(svc.dispatch(make_req("GET", "/bkt", "", {{"marker", "b"}})));
    CHECK(!contains(v1m.small_body, "<Key>a</Key>"));
    CHECK(contains(v1m.small_body, "<Key>c</Key>"));
    CHECK(contains(v1m.small_body, "<Marker>b</Marker>"));

    // V2 honors start-after and echoes <StartAfter>
    auto v2 = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"list-type", "2"}, {"start-after", "a"}})));
    CHECK(!contains(v2.small_body, "<Key>a</Key>"));
    CHECK(contains(v2.small_body, "<StartAfter>a</StartAfter>"));
    CHECK(!contains(v2.small_body, "<Marker>"));

    // V2 omits Owner by default; only fetch-owner=true returns it
    CHECK(!contains(v2.small_body, "<Owner>"));
}

TEST(service_response_protocol_details) {
    // §5.9: HostId/x-amz-id-2, Allow, Last-Modified on 304, region on HeadBucket
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto put = sync_wait(svc.dispatch(make_req("PUT", "/bkt/o.bin", "payload")));

    auto err = sync_wait(svc.dispatch(make_req("GET", "/bkt/missing")));
    CHECK(contains(err.small_body, "<HostId>"));
    CHECK(err.headers.has("x-amz-id-2"));

    auto hb = sync_wait(svc.dispatch(make_req("HEAD", "/bkt")));
    CHECK_EQ(hb.status, 200);
    CHECK(hb.headers.has("x-amz-bucket-region"));

    // 405 must carry Allow (RFC 9110 §15.5.6)
    auto na = sync_wait(svc.dispatch(make_req("PATCH", "/bkt/o.bin")));
    CHECK_EQ(na.status, 405);
    CHECK(na.headers.has("Allow"));
    CHECK(contains(*na.headers.get("Allow"), "GET"));

    // A 304 with only an ETag isn't enough; cache entries would lose Last-Modified
    auto nm = make_req("GET", "/bkt/o.bin");
    nm.headers.add("If-None-Match", *put.headers.get("ETag"));
    auto r304 = sync_wait(svc.dispatch(std::move(nm)));
    CHECK_EQ(r304.status, 304);
    CHECK(r304.headers.has("ETag"));
    CHECK(r304.headers.has("Last-Modified"));
}

TEST(service_create_bucket_location_constraint) {
    // §5.4: previously the request body was never read; cross-region bucket creation silently
    // succeeded
    auto svc = make_service_noauth();
    auto ok = sync_wait(svc.dispatch(make_req(
        "PUT", "/loc1", "<CreateBucketConfiguration><LocationConstraint></LocationConstraint>"
                      "</CreateBucketConfiguration>")));
    CHECK_EQ(ok.status, 200);  // empty constraint = us-east-1 = this implementation's default region

    auto bad = sync_wait(svc.dispatch(make_req(
        "PUT", "/loc2", "<CreateBucketConfiguration><LocationConstraint>eu-west-1"
                      "</LocationConstraint></CreateBucketConfiguration>")));
    CHECK_EQ(bad.status, 400);
    CHECK(contains(bad.small_body, "<Code>InvalidLocationConstraint</Code>"));
    // after rejection the bucket must not exist
    CHECK_EQ(sync_wait(svc.dispatch(make_req("HEAD", "/loc2"))).status, 404);

    // no body still passes via the old path
    CHECK_EQ(sync_wait(svc.dispatch(make_req("PUT", "/loc3"))).status, 200);
}

TEST(service_content_md5_and_checksums) {
    // §5.6: Content-MD5 / x-amz-checksum-* previously had no handling anywhere in the repo --
    // request bodies rewritten by middleboxes were accepted wholesale
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));

    auto md5_b64 = [](std::string_view body) {
        util::HashStream h(util::HashStream::Algo::Md5);
        h.update(std::span(reinterpret_cast<const uint8_t*>(body.data()), body.size()));
        auto d = h.final_bytes();
        return util::base64_encode(std::span(d.data(), d.size()));
    };

    auto ok = make_req("PUT", "/bkt/a.bin", "hello");
    ok.headers.add("Content-MD5", md5_b64("hello"));
    CHECK_EQ(sync_wait(svc.dispatch(std::move(ok))).status, 200);

    // Digest mismatch → BadDigest, and the object must not be persisted (the backend doesn't
    // commit when body.read throws)
    auto bad = make_req("PUT", "/bkt/b.bin", "hello");
    bad.headers.add("Content-MD5", md5_b64("goodbye"));
    auto bad_resp = sync_wait(svc.dispatch(std::move(bad)));
    CHECK_EQ(bad_resp.status, 400);
    CHECK(contains(bad_resp.small_body, "<Code>BadDigest</Code>"));
    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/b.bin"))).status, 404);

    // A malformed digest and a "digest mismatch" must be distinct: clients need to tell whether
    // they miscomputed or the transport rewrote the body
    auto junk = make_req("PUT", "/bkt/c.bin", "hello");
    junk.headers.add("Content-MD5", "not-base64!!");
    auto junk_resp = sync_wait(svc.dispatch(std::move(junk)));
    CHECK_EQ(junk_resp.status, 400);
    CHECK(contains(junk_resp.small_body, "<Code>InvalidDigest</Code>"));

    // A length mismatch (4 bytes received for a 16-byte MD5) is likewise InvalidDigest
    auto shortd = make_req("PUT", "/bkt/d.bin", "hello");
    shortd.headers.add("Content-MD5", util::base64_encode(std::string_view("abcd")));
    CHECK(contains(sync_wait(svc.dispatch(std::move(shortd))).small_body, "InvalidDigest"));

    // x-amz-checksum-*: previously accepted silently, never verified
    auto crc = make_req("PUT", "/bkt/e.bin", "hello");
    uint32_t v = util::crc32c_of(std::string_view("hello"));
    std::string be;
    for (int s = 24; s >= 0; s -= 8) be.push_back(char((v >> s) & 0xff));
    crc.headers.add("x-amz-checksum-crc32c", util::base64_encode(be));
    CHECK_EQ(sync_wait(svc.dispatch(std::move(crc))).status, 200);

    auto crc_bad = make_req("PUT", "/bkt/f.bin", "tampered");
    crc_bad.headers.add("x-amz-checksum-crc32c", util::base64_encode(be));
    CHECK_EQ(sync_wait(svc.dispatch(std::move(crc_bad))).status, 400);
}

TEST(service_delete_objects_requires_digest) {
    // Batch delete is the only operation where a rewritten request body silently deletes extra
    // objects; AWS requires an integrity header
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/a", "x")));

    std::string xml = "<Delete><Object><Key>a</Key></Object></Delete>";
    auto missing = sync_wait(svc.dispatch(make_req("POST", "/bkt", xml, {{"delete", ""}})));
    CHECK_EQ(missing.status, 400);
    CHECK(contains(missing.small_body, "Content-MD5"));
    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/a"))).status, 200);  // not deleted

    // With the correct digest it goes through
    CHECK_EQ(sync_wait(svc.dispatch(make_delete_req("/bkt", xml, {{"delete", ""}}))).status, 200);
    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/a"))).status, 404);
}

TEST(service_multipart_constraints) {
    // §5.7: none of AWS's hard constraints existed before
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));

    auto init = sync_wait(svc.dispatch(make_req("POST", "/bkt/mp.bin", "", {{"uploads", ""}})));
    std::string uid = xelem(body_of(init), "UploadId");
    auto put_part = [&](int no, const std::string& data) {
        auto r = sync_wait(svc.dispatch(make_req(
            "PUT", "/bkt/mp.bin", data, {{"partNumber", std::to_string(no)}, {"uploadId", uid}})));
        std::string e = *r.headers.get("ETag");  // strip quotes: the complete XML carries ETags unquoted
        if (e.size() >= 2 && e.front() == '"') e = e.substr(1, e.size() - 2);
        return e;
    };
    std::string e1 = put_part(1, "small");                          // 5 bytes, not the last part
    std::string e2 = put_part(2, "tail");
    auto complete_xml = [](std::vector<std::pair<int, std::string>> ps) {
        std::string x = "<CompleteMultipartUpload>";
        for (auto& [n, e] : ps)
            x += "<Part><PartNumber>" + std::to_string(n) + "</PartNumber><ETag>" + e +
                 "</ETag></Part>";
        return x + "</CompleteMultipartUpload>";
    };

    // A non-final part under 5MiB → EntityTooSmall (otherwise 10000 one-byte parts could be
    // committed)
    auto small = sync_wait(svc.dispatch(make_req("POST", "/bkt/mp.bin", complete_xml({{1, e1}, {2, e2}}),
                                                 {{"uploadId", uid}})));
    CHECK_EQ(small.status, 400);
    CHECK(contains(small.small_body, "<Code>EntityTooSmall</Code>"));

    // Out of order → InvalidPartOrder (previously InvalidPart, which would send clients off to
    // re-upload parts)
    auto unordered = sync_wait(svc.dispatch(make_req(
        "POST", "/bkt/mp.bin", complete_xml({{2, e2}, {1, e1}}), {{"uploadId", uid}})));
    CHECK_EQ(unordered.status, 400);
    CHECK(contains(unordered.small_body, "<Code>InvalidPartOrder</Code>"));

    // Out-of-range part numbers must be re-checked on the complete side too (the upload side
    // validates a different input)
    auto oob = sync_wait(svc.dispatch(make_req(
        "POST", "/bkt/mp.bin", complete_xml({{99999, e1}}), {{"uploadId", uid}})));
    CHECK_EQ(oob.status, 400);

    // The last part is exempt from the minimum size: a single-part upload succeeds as usual,
    // and Location is a full URL
    auto one = sync_wait(svc.dispatch(
        make_req("POST", "/bkt/mp.bin", complete_xml({{1, e1}}), {{"uploadId", uid}})));
    CHECK_EQ(one.status, 200);
    CHECK(contains(one.small_body, "<Location>http://localhost/bkt/mp.bin</Location>"));
}

TEST(service_multipart_listing_pagination) {
    // §5.1: ListParts/ListMultipartUploads previously always reported IsTruncated=false, which
    // clients take as "end of list" -- with 5000 active uploads only the first page would ever
    // be seen, with no hint anything is missing
    auto svc = make_service_noauth();
    svc.set_min_part_size(0);
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));

    auto init = sync_wait(svc.dispatch(make_req("POST", "/bkt/mp.bin", "", {{"uploads", ""}})));
    std::string uid = xelem(body_of(init), "UploadId");
    for (int i = 1; i <= 3; ++i)
        sync_wait(svc.dispatch(make_req("PUT", "/bkt/mp.bin", "x",
                                        {{"partNumber", std::to_string(i)}, {"uploadId", uid}})));

    // ListParts: max-parts takes effect, truncation is reported truthfully, and the cursor resumes
    auto p1 = sync_wait(svc.dispatch(
        make_req("GET", "/bkt/mp.bin", "", {{"uploadId", uid}, {"max-parts", "2"}})));
    std::string b1 = body_of(p1);
    CHECK(contains(b1, "<IsTruncated>true</IsTruncated>"));
    CHECK(contains(b1, "<MaxParts>2</MaxParts>"));
    CHECK(contains(b1, "<NextPartNumberMarker>2</NextPartNumberMarker>"));
    CHECK(contains(b1, "<PartNumber>1</PartNumber>"));
    CHECK(!contains(b1, "<PartNumber>3</PartNumber>"));

    auto p2 = sync_wait(svc.dispatch(make_req(
        "GET", "/bkt/mp.bin", "",
        {{"uploadId", uid}, {"max-parts", "2"}, {"part-number-marker", "2"}})));
    std::string b2 = body_of(p2);
    CHECK(contains(b2, "<IsTruncated>false</IsTruncated>"));
    CHECK(contains(b2, "<PartNumber>3</PartNumber>"));
    CHECK(!contains(b2, "<PartNumber>1</PartNumber>"));

    // ListMultipartUploads: three uploads, paged by (key, upload_id) with no duplicates or gaps
    std::vector<std::string> keys{"a.bin", "b.bin", "c.bin"};
    for (auto& k : keys)
        sync_wait(svc.dispatch(make_req("POST", "/bkt/" + k, "", {{"uploads", ""}})));

    std::set<std::string> seen;
    std::string km, im;
    int pages = 0;
    for (;;) {
        std::vector<std::pair<std::string, std::string>> q{{"uploads", ""}, {"max-uploads", "2"}};
        if (!km.empty()) {
            q.push_back({"key-marker", km});
            q.push_back({"upload-id-marker", im});
        }
        auto page = sync_wait(svc.dispatch(make_req("GET", "/bkt", "", q)));
        CHECK_EQ(page.status, 200);
        std::string body = body_of(page);
        size_t pos = 0;
        while ((pos = body.find("<Key>", pos)) != std::string::npos) {
            size_t end = body.find("</Key>", pos);
            CHECK(seen.insert(body.substr(pos + 5, end - pos - 5)).second);  // no duplicates
            pos = end;
        }
        if (++pages > 10) break;  // defensive: don't page forever if the cursor stops advancing
        if (!contains(body, "<IsTruncated>true</IsTruncated>")) break;
        km = xelem(body, "NextKeyMarker");
        im = xelem(body, "NextUploadIdMarker");
        CHECK(!km.empty());
    }
    CHECK_EQ(seen.size(), size_t(4));  // a/b/c + mp.bin
    CHECK(pages > 1);                  // actually paged

    // prefix filtering and delimiter grouping
    auto pref = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"uploads", ""}, {"prefix", "a."}})));
    CHECK(contains(body_of(pref), "<Key>a.bin</Key>"));
    CHECK(!contains(body_of(pref), "<Key>b.bin</Key>"));

    // upload-id-marker on its own is meaningless (the cursor is a pair)
    auto bad = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"uploads", ""}, {"upload-id-marker", "x"}})));
    CHECK_EQ(bad.status, 400);

    // encoding-type=url (docs/issues.md T13): previously the parameter was accepted but never
    // encoded -- a silent wrong answer
    sync_wait(svc.dispatch(make_req("POST", "/bkt/enc me.bin", "", {{"uploads", ""}})));
    auto encp = sync_wait(svc.dispatch(make_req(
        "GET", "/bkt", "", {{"uploads", ""}, {"encoding-type", "url"}, {"prefix", "enc "}})));
    CHECK(contains(body_of(encp), "<EncodingType>url</EncodingType>"));
    CHECK(contains(body_of(encp), "<Key>enc%20me.bin</Key>"));
    CHECK(contains(body_of(encp), "<Prefix>enc%20</Prefix>"));
    auto bad_enc = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"uploads", ""}, {"encoding-type", "zzz"}})));
    CHECK_EQ(bad_enc.status, 400);
}

// Per-request timeout (config.h request_timeout_sec · docs/issues.md T10): a timeout during
// handler execution is broken by cooperative cancellation and converges to 503 SlowDown
// (retryable) -- this contract previously had zero tests
TEST(service_request_timeout_cancels_and_returns_503) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    svc.set_request_timeout(std::chrono::seconds(1));

    auto req = make_req("PUT", "/bkt/hung");
    req.body = std::make_unique<HangingReader>();  // body never yields: the timeout must be able to break it
    auto t0 = std::chrono::steady_clock::now();
    auto resp = sync_wait(svc.dispatch(std::move(req)));
    CHECK_EQ(resp.status, 503);
    CHECK(contains(body_of(resp), "<Code>SlowDown</Code>"));
    // Converges at the deadline (1s timeout + slack), not via some other fallback timeout
    CHECK(std::chrono::steady_clock::now() - t0 < std::chrono::seconds(5));

    // The interrupted PUT has no side effects
    auto head = sync_wait(svc.dispatch(make_req("HEAD", "/bkt/hung")));
    CHECK_EQ(head.status, 404);

    // With the timeout disabled (0), the same shape of request is unaffected -- a finite body
    // completes normally
    svc.set_request_timeout(std::chrono::seconds(0));
    auto ok = sync_wait(svc.dispatch(make_req("PUT", "/bkt/fine", "payload")));
    CHECK_EQ(ok.status, 200);
}
