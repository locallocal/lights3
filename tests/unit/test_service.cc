// L2 纯逻辑测试：mock HttpRequest + memory 后端走完整 dispatch（docs/01 §2）
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
    CHECK_EQ(again.status, 204);  // 幂等

    auto delb = sync_wait(svc.dispatch(make_req("DELETE", "/bkt")));
    CHECK_EQ(delb.status, 204);
    auto headb = sync_wait(svc.dispatch(make_req("HEAD", "/bkt")));
    CHECK_EQ(headb.status, 404);
    CHECK_EQ(headb.small_body, "");  // HEAD 错误响应不带 body
}

TEST(service_not_implemented_apis) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    // 明确不支持的子资源（docs/05 §1）显式 501，不落入 List/Get 兜底
    for (auto* sub : {"acl", "policy", "versioning", "lifecycle", "tagging"}) {
        auto resp = sync_wait(svc.dispatch(make_req("GET", "/bkt", "", {{sub, ""}})));
        CHECK_EQ(resp.status, 501);
        CHECK(contains(resp.small_body, "NotImplemented"));
    }
    auto upc = make_req("PUT", "/bkt/k", "", {{"partNumber", "1"}, {"uploadId", "x"}});
    upc.headers.add("x-amz-copy-source", "/bkt/other");
    auto resp = sync_wait(svc.dispatch(std::move(upc)));
    CHECK_EQ(resp.status, 501);  // UploadPartCopy 二期
}

TEST(service_with_auth) {
    AuthConfig acfg;
    acfg.credentials = {{"TESTAK", "test-sk"}};
    auto auth = SigV4Authenticator::build(acfg);
    S3Service svc(make_router(), auth);

    // 未签名 → 403
    auto denied = sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    CHECK_EQ(denied.status, 403);
    CHECK(contains(denied.small_body, "AccessDenied"));

    // 正确签名 → 通过（用同一套签名端生成）
    auto req = make_req("PUT", "/bkt");
    auth.sign(req, acfg.credentials[0]);
    auto ok = sync_wait(svc.dispatch(std::move(req)));
    CHECK_EQ(ok.status, 200);

    // 签名对但 body hash 不符 → 传输中检出
    auto put = make_req("PUT", "/bkt/k", "tampered body");
    auth.sign(put, acfg.credentials[0], util::sha256_hex("original body"));
    auto resp = sync_wait(svc.dispatch(std::move(put)));
    CHECK_EQ(resp.status, 400);
    CHECK(contains(resp.small_body, "XAmzContentSHA256Mismatch"));

    // healthz 免认证
    auto hz = sync_wait(svc.dispatch(make_req("GET", "/-/healthz")));
    CHECK_EQ(hz.status, 200);
}

// ---------- docs/05 新增覆盖 ----------

namespace {
// 从响应 XML 中抽取首个 <tag>…</tag> 文本（测试用，够浅结构使用）
std::string xelem(const std::string& xml, const std::string& tag) {
    auto open = "<" + tag + ">", close = "</" + tag + ">";
    auto b = xml.find(open);
    if (b == std::string::npos) return "";
    b += open.size();
    auto e = xml.find(close, b);
    return e == std::string::npos ? "" : xml.substr(b, e - b);
}
}  // namespace

TEST(service_multipart_flow) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));

    // Create → UploadId
    auto init = sync_wait(svc.dispatch(make_req("POST", "/bkt/mp.bin", "", {{"uploads", ""}})));
    CHECK_EQ(init.status, 200);
    std::string uid = xelem(body_of(init), "UploadId");
    CHECK(!uid.empty());

    // 两个分片
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

    // Complete（XML 请求体）
    std::string cxml = "<CompleteMultipartUpload>"
                       "<Part><PartNumber>1</PartNumber><ETag>" + etag1 + "</ETag></Part>"
                       "<Part><PartNumber>2</PartNumber><ETag>" + etag2 + "</ETag></Part>"
                       "</CompleteMultipartUpload>";
    auto done = sync_wait(svc.dispatch(
        make_req("POST", "/bkt/mp.bin", cxml, {{"uploadId", uid}})));
    CHECK_EQ(done.status, 200);
    CHECK(contains(xelem(body_of(done), "ETag"), "-2"));  // 拼接 ETag 规则

    auto get = sync_wait(svc.dispatch(make_req("GET", "/bkt/mp.bin")));
    CHECK_EQ(body_of(get), "hello world");

    // Abort 路径 + 完成后 upload 消失
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
    auto resp = sync_wait(svc.dispatch(make_req("POST", "/bkt", xml, {{"delete", ""}})));
    CHECK_EQ(resp.status, 200);
    auto body = body_of(resp);
    CHECK(contains(body, "<Deleted><Key>a</Key></Deleted>"));
    CHECK(contains(body, "<Deleted><Key>b</Key></Deleted>"));

    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/a"))).status, 404);
    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/c"))).status, 200);

    // 坏 XML → MalformedXML
    auto bad = sync_wait(svc.dispatch(make_req("POST", "/bkt", "<oops>", {{"delete", ""}})));
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

    // COPY（默认）：数据与元数据一同复制
    auto cp = make_req("PUT", "/bkt/dst.txt");
    cp.headers.add("x-amz-copy-source", "/bkt/src.txt");
    auto cp_resp = sync_wait(svc.dispatch(std::move(cp)));
    CHECK_EQ(cp_resp.status, 200);
    CHECK(contains(body_of(cp_resp), "CopyObjectResult"));
    auto got = sync_wait(svc.dispatch(make_req("GET", "/bkt/dst.txt")));
    CHECK_EQ(body_of(got), "copy me");
    CHECK_EQ(*got.headers.get("Content-Type"), "text/plain");
    CHECK_EQ(*got.headers.get("x-amz-meta-color"), "red");

    // REPLACE：换元数据
    auto rp = make_req("PUT", "/bkt/dst2.txt");
    rp.headers.add("x-amz-copy-source", "/bkt/src.txt");
    rp.headers.add("x-amz-metadata-directive", "REPLACE");
    rp.headers.add("Content-Type", "application/json");
    sync_wait(svc.dispatch(std::move(rp)));
    auto got2 = sync_wait(svc.dispatch(make_req("HEAD", "/bkt/dst2.txt")));
    CHECK_EQ(*got2.headers.get("Content-Type"), "application/json");
    CHECK(!got2.headers.has("x-amz-meta-color"));

    // copy-source 条件不满足 → 412；自复制且 COPY → 400
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

    // If-Modified-Since 未来时间 → 304；过去时间 → 200
    auto ims = make_req("GET", "/bkt/c.txt");
    ims.headers.add("If-Modified-Since", "Fri, 01 Jan 2100 00:00:00 GMT");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(ims))).status, 304);
    auto ims2 = make_req("GET", "/bkt/c.txt");
    ims2.headers.add("If-Modified-Since", "Mon, 01 Jan 2001 00:00:00 GMT");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(ims2))).status, 200);

    // If-Unmodified-Since 过去时间 → 412
    auto ius = make_req("HEAD", "/bkt/c.txt");
    ius.headers.add("If-Unmodified-Since", "Mon, 01 Jan 2001 00:00:00 GMT");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(ius))).status, 412);

    // PUT If-None-Match:* 防覆盖（docs/05 §6）
    auto pin = make_req("PUT", "/bkt/c.txt", "v2");
    pin.headers.add("If-None-Match", "*");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(pin))).status, 412);
    auto pin2 = make_req("PUT", "/bkt/new.txt", "v1");
    pin2.headers.add("If-None-Match", "*");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(pin2))).status, 200);

    // PUT If-Match：etag 不符 → 412
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

    // V1：Marker，无 KeyCount；V2：KeyCount
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
    put.headers.set("Host", "vbkt.s3.local:9000");  // 端口剥离
    CHECK_EQ(sync_wait(svc.dispatch(std::move(put))).status, 200);

    auto get = make_req("GET", "/dir/a.txt");
    get.headers.set("Host", "vbkt.s3.local");
    auto resp = sync_wait(svc.dispatch(std::move(get)));
    CHECK_EQ(resp.status, 200);
    CHECK_EQ(body_of(resp), "vh data");

    // 未命中 base_domain 的 Host 仍走 path-style
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
