// L2 纯逻辑测试：mock HttpRequest + memory 后端走完整 dispatch（docs/architecture.md §2）
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
    // 明确不支持的子资源（docs/s3-protocol.md §1）显式 501，不落入 List/Get 兜底
    for (auto* sub : {"acl", "policy", "versioning", "lifecycle", "tagging"}) {
        auto resp = sync_wait(svc.dispatch(make_req("GET", "/bkt", "", {{sub, ""}})));
        CHECK_EQ(resp.status, 501);
        CHECK(contains(resp.small_body, "NotImplemented"));
    }
    // versioning 经 copy-source query 表达也是 501
    auto upc = make_req("PUT", "/bkt/k", "", {{"partNumber", "1"}, {"uploadId", "x"}});
    upc.headers.add("x-amz-copy-source", "/bkt/other?versionId=abc");
    auto resp = sync_wait(svc.dispatch(std::move(upc)));
    CHECK_EQ(resp.status, 501);
}

TEST(service_upload_part_copy) {
    auto svc = make_service_noauth();
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

    // 全量 copy + range copy，CopyPartResult 带 ETag，complete 后内容正确
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

    // 错误路径：range 形式非法 / 越界（AWS 语义均为 InvalidArgument）、
    // 源条件不满足 412、源缺失 404
    auto init2 = sync_wait(svc.dispatch(make_req("POST", "/bkt/dst2.bin", "", {{"uploads", ""}})));
    uid = xelem(body_of(init2), "UploadId");
    // helper 复用 uid，key 换成 dst2
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
    // '.' 开头 bucket 走 copy-source header 不经 dispatch 路径拦截，须单独拒绝
    //（否则 CopyObject/UploadPartCopy 能读 .sys 凭证对象）
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

    // 多段 Range：AWS 不支持，整个头忽略 → 200 全量（docs/todo.md §4）
    auto multi = make_req("GET", "/bkt/k");
    multi.headers.add("Range", "bytes=0-1,3-4");
    auto resp = sync_wait(svc.dispatch(std::move(multi)));
    CHECK_EQ(resp.status, 200);
    CHECK_EQ(body_of(resp), "0123456789");

    // PUT If-None-Match 非 '*'：AWS 同样 501（conditional writes 仅支持 *）
    auto put = make_req("PUT", "/bkt/k", "new");
    put.headers.add("If-None-Match", "\"someetag\"");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(put))).status, 501);
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

// ---------- docs/s3-protocol.md 新增覆盖 ----------


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

    // PUT If-None-Match:* 防覆盖（docs/s3-protocol.md §6）
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

// 后端级 metrics 注册表（docs/todo.md §3.1）：注入后 /-/metrics 在 L2 请求指标
// 之后追加渲染；未注入（上一用例）则只有 L2 部分
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

// ---------- 评审发现的回归用例 ----------

// ?versionId 显式拒绝：DELETE ?versionId= 不得静默删当前对象
TEST(service_version_id_rejected) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "keep me")));

    auto del = sync_wait(
        svc.dispatch(make_req("DELETE", "/bkt/k", "", {{"versionId", "abc"}})));
    CHECK_EQ(del.status, 501);
    auto get = sync_wait(svc.dispatch(make_req("GET", "/bkt/k")));
    CHECK_EQ(get.status, 200);  // 对象未被误删
    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/k", "",
                                             {{"versionId", "abc"}}))).status, 501);
}

// HEAD + Range：与 GET 对齐返回 206/Content-Range；不可满足 → 416
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

// RFC 7232 优先级：前置条件（412）判定先于 Range（416）
TEST(service_precondition_beats_range) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "0123456789")));

    auto req = make_req("GET", "/bkt/k");
    req.headers.add("If-Match", "\"wrong-etag\"");
    req.headers.add("Range", "bytes=99-");  // 本身会 416
    CHECK_EQ(sync_wait(svc.dispatch(std::move(req))).status, 412);
}

// If-Range：验证器命中才生效 Range，否则回整对象
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

// max-keys 钳制到 1000（S3 语义静默钳制）；encoding-type 只认 url
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

// 对象键控制字符拒绝（0x01 会让 ListObjects XML 对合规解析器不可解析）
TEST(service_key_control_chars_rejected) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto resp = sync_wait(svc.dispatch(make_req("PUT", "/bkt/a\x01b", "x")));
    CHECK_EQ(resp.status, 400);
    CHECK(contains(resp.small_body, "InvalidArgument"));
}

// /-/admin/credentials 前缀边界：credentialsXYZ 不得进管理面（应走数据面 → XML 错误）
TEST(service_admin_prefix_boundary) {
    auto svc = make_service_noauth();
    auto resp = sync_wait(svc.dispatch(make_req("GET", "/-/admin/credentialsXYZ")));
    CHECK(*resp.headers.get("Content-Type") == "application/xml");
    CHECK(resp.status == 400 || resp.status == 404);
}

// IPv6 字面量 Host 不被 rfind(':') 截坏（vhost 配置下仍正常走 path-style）
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

// ---------- P0 §1.1：bucket 名校验统一收口 ----------

// vhost 寻址下 Host 前缀是 bucket，含 '/'、'..' 或控制字符时必须在 L2 就被拒——
// 否则 localfs 的 root_/bucket/key 拼接会逃出 root（fs::path 遇绝对路径替换整条
// 路径），造成任意文件读
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
    // 域名大小写不敏感（RFC 4343，docs/gaps.md §2.13）：大写 Host 归一化后与
    // 小写指向同一 bucket（"upper" 不存在 → 404），而不是降级 path-style 或按
    // 大写桶名拒绝——后者会让同一 URL 在两种大小写下指向不同资源
    auto req = make_req("GET", "/k");
    req.headers.set("Host", "UPPER.s3.local");
    auto resp = sync_wait(svc.dispatch(std::move(req)));
    CHECK_EQ(resp.status, 404);
    CHECK(contains(resp.small_body, "NoSuchBucket"));
}

// path-style：%00 解码后首字符是 NUL 而非 '.'，旧的首字符启发式会放行
TEST(service_path_style_bucket_name_validated) {
    auto svc = make_service_noauth();
    for (const char* path : {"/.sys/credentials/x", "/\x01bkt/k", "/AB C/k"}) {
        auto resp = sync_wait(svc.dispatch(make_req("GET", path)));
        CHECK_EQ(resp.status, 400);
        CHECK(contains(resp.small_body, "InvalidBucketName"));
    }
    // NUL 开头：解码后 bucket.front() == '\0'，字符集规则同样拒绝
    http::HttpRequest nul;
    nul.method = "GET";
    nul.raw_path = "/%00.sys/credentials/x";
    nul.path = std::string("/\0.sys/credentials/x", 20);
    nul.headers.add("Host", "localhost");
    auto resp = sync_wait(svc.dispatch(std::move(nul)));
    CHECK_EQ(resp.status, 400);
}

// copy-source 走 header 不经 dispatch 闸门，须用同一校验函数独立拦截
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

// 合法桶名不受影响（vhost 与 path-style 都能正常读写）
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
