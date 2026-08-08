// L2 纯逻辑测试：mock HttpRequest + memory 后端走完整 dispatch（docs/architecture.md §2）
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

// DeleteObjects 要求完整性头（§5.6）：测试里统一走这个构造器，免得每处各算一遍
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
    svc.set_min_part_size(0);  // 本用例测流程，不测 5MiB 规则（见 service_multipart_constraints）
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
    svc.set_min_part_size(0);  // 本用例测流程，不测 5MiB 规则（见 service_multipart_constraints）
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
    auto resp = sync_wait(svc.dispatch(make_delete_req("/bkt", xml, {{"delete", ""}})));
    CHECK_EQ(resp.status, 200);
    auto body = body_of(resp);
    CHECK(contains(body, "<Deleted><Key>a</Key></Deleted>"));
    CHECK(contains(body, "<Deleted><Key>b</Key></Deleted>"));

    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/a"))).status, 404);
    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/c"))).status, 200);

    // 坏 XML → MalformedXML
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

// ---------- gaps §3.4：不支持的请求头必须 501，而不是静默吞掉 ----------
// 静默接受比报错危险：客户端会据 200 认为对象已加密/已打标/已锁定
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
    // x-amz-acl: private 是本实现的实际语义，放行；其余 ACL 值 501
    CHECK_EQ(try_put("x-amz-acl", "private").status, 200);
    CHECK_EQ(try_put("x-amz-acl", "public-read").status, 501);
}

// ---------- gaps §3.5：query 白名单，未知参数 501 而非静默误答 ----------
TEST(service_query_whitelist) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "0123456789")));

    // 黑名单时代的确切漏项：都曾静默降级为"读整个对象"。response-* 已于 §5.3
    // 实现，不再在此列——未实现的子资源仍须 501
    for (auto q : {std::pair{"attributes", ""}, std::pair{"partNumber", "1"}}) {
        auto resp = sync_wait(svc.dispatch(make_req("GET", "/bkt/k", "", {{q.first, q.second}})));
        CHECK_EQ(resp.status, 501);
        CHECK(contains(resp.small_body, "<Code>NotImplemented</Code>"));
    }

    // 白名单内的参数不受影响；fetch-owner 已实现（§5.5），不再是"允许但忽略"
    auto ls = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"list-type", "2"}, {"prefix", ""}, {"fetch-owner", "true"}})));
    CHECK_EQ(ls.status, 200);
    CHECK(contains(ls.small_body, "<Owner>"));

    // presigned 签名参数族全局放行（值不在此校验，SigV4 层负责）
    auto pre = sync_wait(svc.dispatch(make_req("GET", "/bkt/k", "", {{"X-Amz-Algorithm", "AWS4-HMAC-SHA256"}})));
    CHECK_EQ(pre.status, 200);
}

// ---------- gaps §3.8：/-/ 内部端点不得遮蔽 vhost 下的合法对象键 ----------
TEST(service_internal_endpoints_not_shadowing_vhost_keys) {
    S3Service svc(make_router(), SigV4Authenticator::build(AuthConfig{}), "s3.local");
    auto create = make_req("PUT", "/");
    create.headers.set("Host", "vbkt.s3.local");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(create))).status, 200);

    // vhost 下 "/-/metrics" 是 vbkt 里的对象键 "-/metrics"：PUT 必须真的写进去
    //（旧行为：命中匿名 metrics 端点，返回 200 但对象没写——静默丢数据）
    auto put = make_req("PUT", "/-/metrics", "real object data");
    put.headers.set("Host", "vbkt.s3.local");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(put))).status, 200);

    auto get = make_req("GET", "/-/metrics");
    get.headers.set("Host", "vbkt.s3.local");
    auto resp = sync_wait(svc.dispatch(std::move(get)));
    CHECK_EQ(resp.status, 200);
    CHECK_EQ(body_of(resp), "real object data");  // 对象内容，不是 Prometheus 文本

    // path-style 下内部端点照常工作，但只认 GET/HEAD：PUT 是 405 而不是"200 且丢数据"
    auto m = sync_wait(svc.dispatch(make_req("GET", "/-/metrics")));
    CHECK_EQ(m.status, 200);
    CHECK(contains(m.small_body, "lights3_requests_total"));
    auto pm = sync_wait(svc.dispatch(make_req("PUT", "/-/metrics", "x")));
    CHECK_EQ(pm.status, 405);
    auto hh = sync_wait(svc.dispatch(make_req("HEAD", "/-/healthz")));
    CHECK_EQ(hh.status, 200);  // 探活器常用 HEAD
}

// ---------- gaps §4：语法非法 Range 忽略回 200；V2 token 不透明往返 ----------
TEST(service_malformed_range_ignored) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/k", "0123456789")));

    // "bytes=5-3"（last < first）语法非法：RFC 9110 要求整个头按无效忽略，
    // 回 200 整对象——此前误答 416
    auto req = make_req("GET", "/bkt/k");
    req.headers.add("Range", "bytes=5-3");
    auto resp = sync_wait(svc.dispatch(std::move(req)));
    CHECK_EQ(resp.status, 200);
    CHECK_EQ(body_of(resp), "0123456789");

    // 真正不可满足的 range 仍是 416
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
    CHECK(tok != "b");  // 不透明（base64），不再是明文 key

    auto p2 = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"list-type", "2"}, {"continuation-token", tok}})));
    auto b2 = body_of(p2);
    CHECK(contains(b2, "<Key>c</Key>"));
    CHECK(!contains(b2, "<Key>b</Key>"));

    // 解不开的 token → InvalidArgument，而不是被当明文 key 静默使用
    auto bad = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"list-type", "2"}, {"continuation-token", "!!!"}})));
    CHECK_EQ(bad.status, 400);
    CHECK(contains(bad.small_body, "InvalidArgument"));
}

// ---------- gaps §3.9：DeleteObjects 的畸形输入与版本删除 ----------
TEST(service_delete_objects_malformed_inputs) {
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/a", "x")));

    // 空列表与缺 <Key> 都是畸形请求：整批 MalformedXML，而不是 200 空结果
    auto empty = sync_wait(
        svc.dispatch(make_delete_req("/bkt", "<Delete></Delete>", {{"delete", ""}})));
    CHECK_EQ(empty.status, 400);
    CHECK(contains(empty.small_body, "MalformedXML"));

    auto nokey = sync_wait(svc.dispatch(
        make_delete_req("/bkt", "<Delete><Object></Object></Delete>", {{"delete", ""}})));
    CHECK_EQ(nokey.status, 400);
    CHECK(contains(nokey.small_body, "MalformedXML"));

    // <VersionId> 静默忽略会把"删指定版本"变成"删当前对象"：501 且对象未删
    auto ver = sync_wait(svc.dispatch(make_delete_req(
        "/bkt", "<Delete><Object><Key>a</Key><VersionId>v1</VersionId></Object></Delete>",
        {{"delete", ""}})));
    CHECK_EQ(ver.status, 501);
    CHECK(contains(ver.small_body, "NotImplemented"));
    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/a"))).status, 200);
}

// ---- docs/gaps.md §5.2 / §5.3 / §5.5 / §5.9 ----

TEST(service_first_class_object_metadata) {
    // 一等元数据（§5.2）：此前 PUT 时全丢、GET/HEAD 也不回。丢 Content-Encoding
    // 的后果不是"少个头"，是浏览器拿到无法解压的字节流
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

    // CopyObject 的 COPY 指令必须整份带走（逐字段抄写曾漏掉新增字段）
    auto cp = make_req("PUT", "/bkt/copy.bin");
    cp.headers.add("x-amz-copy-source", "/bkt/o.bin");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(cp))).status, 200);
    auto cg = sync_wait(svc.dispatch(make_req("GET", "/bkt/copy.bin")));
    CHECK_EQ(*cg.headers.get("Content-Encoding"), "gzip");
    CHECK_EQ(*cg.headers.get("Cache-Control"), "max-age=42");

    // 头值里的 CR/LF 会撕开 sidecar 记录，也是响应头注入面
    auto bad = make_req("PUT", "/bkt/bad.bin", "x");
    bad.headers.add("Cache-Control", "a\r\nX-Injected: 1");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(bad))).status, 400);

    // 只有 STANDARD 一种存储类：收下 GLACIER 再回显等于替存储层撒谎
    auto sc = make_req("PUT", "/bkt/sc.bin", "x");
    sc.headers.add("x-amz-storage-class", "GLACIER");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(sc))).status, 501);
    auto sc2 = make_req("PUT", "/bkt/sc2.bin", "x");
    sc2.headers.add("x-amz-storage-class", "STANDARD");
    CHECK_EQ(sync_wait(svc.dispatch(std::move(sc2))).status, 200);
}

TEST(service_response_override_params) {
    // §5.3：presigned 下载链接最常用的一族，此前既不生效也不报错
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
    CHECK_EQ(*r.headers.get("Content-Type"), "text/plain");  // 覆盖对象自身的值
    CHECK_EQ(*r.headers.get("Content-Disposition"), "attachment; filename=\"x.txt\"");
    CHECK_EQ(*r.headers.get("Cache-Control"), "no-store");
    CHECK_EQ(body_of(r), "payload");  // 只改头，不改体

    // query 值是攻击者可控的：塞进响应头前必须挡住 CR/LF（响应拆分）
    auto inj = sync_wait(svc.dispatch(
        make_req("GET", "/bkt/o.bin", "", {{"response-content-type", "t\r\nX-Injected: 1"}})));
    CHECK_EQ(inj.status, 400);
}

TEST(service_list_marker_semantics) {
    // §5.5：三种 marker 此前塌缩成同一个 start_after
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    for (auto k : {"a", "b", "c"}) sync_wait(svc.dispatch(make_req("PUT", std::string("/bkt/") + k, "x")));

    // V1 只认 marker：带 start-after 的 V1 请求不得生效，且回显的 <Marker> 必须是空
    auto v1 = sync_wait(svc.dispatch(make_req("GET", "/bkt", "", {{"start-after", "b"}})));
    CHECK_EQ(v1.status, 200);
    CHECK(contains(v1.small_body, "<Key>a</Key>"));      // start-after 未生效
    CHECK(contains(v1.small_body, "<Marker></Marker>"));  // 不回显客户端没发过的值

    auto v1m = sync_wait(svc.dispatch(make_req("GET", "/bkt", "", {{"marker", "b"}})));
    CHECK(!contains(v1m.small_body, "<Key>a</Key>"));
    CHECK(contains(v1m.small_body, "<Key>c</Key>"));
    CHECK(contains(v1m.small_body, "<Marker>b</Marker>"));

    // V2 认 start-after 并回显 <StartAfter>
    auto v2 = sync_wait(svc.dispatch(
        make_req("GET", "/bkt", "", {{"list-type", "2"}, {"start-after", "a"}})));
    CHECK(!contains(v2.small_body, "<Key>a</Key>"));
    CHECK(contains(v2.small_body, "<StartAfter>a</StartAfter>"));
    CHECK(!contains(v2.small_body, "<Marker>"));

    // V2 缺省不回 Owner；fetch-owner=true 才回
    CHECK(!contains(v2.small_body, "<Owner>"));
}

TEST(service_response_protocol_details) {
    // §5.9：HostId/x-amz-id-2、Allow、304 的 Last-Modified、HeadBucket 的 region
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    auto put = sync_wait(svc.dispatch(make_req("PUT", "/bkt/o.bin", "payload")));

    auto err = sync_wait(svc.dispatch(make_req("GET", "/bkt/missing")));
    CHECK(contains(err.small_body, "<HostId>"));
    CHECK(err.headers.has("x-amz-id-2"));

    auto hb = sync_wait(svc.dispatch(make_req("HEAD", "/bkt")));
    CHECK_EQ(hb.status, 200);
    CHECK(hb.headers.has("x-amz-bucket-region"));

    // 405 必须带 Allow（RFC 9110 §15.5.6）
    auto na = sync_wait(svc.dispatch(make_req("PATCH", "/bkt/o.bin")));
    CHECK_EQ(na.status, 405);
    CHECK(na.headers.has("Allow"));
    CHECK(contains(*na.headers.get("Allow"), "GET"));

    // 304 只带 ETag 不够，缓存条目会丢掉 Last-Modified
    auto nm = make_req("GET", "/bkt/o.bin");
    nm.headers.add("If-None-Match", *put.headers.get("ETag"));
    auto r304 = sync_wait(svc.dispatch(std::move(nm)));
    CHECK_EQ(r304.status, 304);
    CHECK(r304.headers.has("ETag"));
    CHECK(r304.headers.has("Last-Modified"));
}

TEST(service_create_bucket_location_constraint) {
    // §5.4：此前请求体从不读，跨 region 建桶静默成功
    auto svc = make_service_noauth();
    auto ok = sync_wait(svc.dispatch(make_req(
        "PUT", "/loc1", "<CreateBucketConfiguration><LocationConstraint></LocationConstraint>"
                      "</CreateBucketConfiguration>")));
    CHECK_EQ(ok.status, 200);  // 空约束 = us-east-1 = 本实现默认 region

    auto bad = sync_wait(svc.dispatch(make_req(
        "PUT", "/loc2", "<CreateBucketConfiguration><LocationConstraint>eu-west-1"
                      "</LocationConstraint></CreateBucketConfiguration>")));
    CHECK_EQ(bad.status, 400);
    CHECK(contains(bad.small_body, "<Code>InvalidLocationConstraint</Code>"));
    // 拒绝之后桶不得存在
    CHECK_EQ(sync_wait(svc.dispatch(make_req("HEAD", "/loc2"))).status, 404);

    // 无 body 仍按老路径放行
    CHECK_EQ(sync_wait(svc.dispatch(make_req("PUT", "/loc3"))).status, 200);
}

TEST(service_content_md5_and_checksums) {
    // §5.6：Content-MD5 / x-amz-checksum-* 此前全仓无处理——被中间设备改写的
    // 请求体会被照单全收
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

    // 摘要不符 → BadDigest，且对象不得落盘（body.read 抛异常时后端不提交）
    auto bad = make_req("PUT", "/bkt/b.bin", "hello");
    bad.headers.add("Content-MD5", md5_b64("goodbye"));
    auto bad_resp = sync_wait(svc.dispatch(std::move(bad)));
    CHECK_EQ(bad_resp.status, 400);
    CHECK(contains(bad_resp.small_body, "<Code>BadDigest</Code>"));
    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/b.bin"))).status, 404);

    // 格式非法与"摘要不符"必须分开：客户端要能分辨是自己算错还是链路改写
    auto junk = make_req("PUT", "/bkt/c.bin", "hello");
    junk.headers.add("Content-MD5", "not-base64!!");
    auto junk_resp = sync_wait(svc.dispatch(std::move(junk)));
    CHECK_EQ(junk_resp.status, 400);
    CHECK(contains(junk_resp.small_body, "<Code>InvalidDigest</Code>"));

    // 长度对不上（16 字节的 MD5 收到 4 字节）同样是 InvalidDigest
    auto shortd = make_req("PUT", "/bkt/d.bin", "hello");
    shortd.headers.add("Content-MD5", util::base64_encode(std::string_view("abcd")));
    CHECK(contains(sync_wait(svc.dispatch(std::move(shortd))).small_body, "InvalidDigest"));

    // x-amz-checksum-*：此前静默接受、从不校验
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
    // 批量删除是唯一"请求体被改写即静默多删对象"的操作，AWS 要求完整性头
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));
    sync_wait(svc.dispatch(make_req("PUT", "/bkt/a", "x")));

    std::string xml = "<Delete><Object><Key>a</Key></Object></Delete>";
    auto missing = sync_wait(svc.dispatch(make_req("POST", "/bkt", xml, {{"delete", ""}})));
    CHECK_EQ(missing.status, 400);
    CHECK(contains(missing.small_body, "Content-MD5"));
    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/a"))).status, 200);  // 未删

    // 带上正确摘要即放行
    CHECK_EQ(sync_wait(svc.dispatch(make_delete_req("/bkt", xml, {{"delete", ""}}))).status, 200);
    CHECK_EQ(sync_wait(svc.dispatch(make_req("GET", "/bkt/a"))).status, 404);
}

TEST(service_multipart_constraints) {
    // §5.7：AWS 的几条硬约束此前一条都没有
    auto svc = make_service_noauth();
    sync_wait(svc.dispatch(make_req("PUT", "/bkt")));

    auto init = sync_wait(svc.dispatch(make_req("POST", "/bkt/mp.bin", "", {{"uploads", ""}})));
    std::string uid = xelem(body_of(init), "UploadId");
    auto put_part = [&](int no, const std::string& data) {
        auto r = sync_wait(svc.dispatch(make_req(
            "PUT", "/bkt/mp.bin", data, {{"partNumber", std::to_string(no)}, {"uploadId", uid}})));
        std::string e = *r.headers.get("ETag");  // 去引号：complete 的 XML 里不带引号
        if (e.size() >= 2 && e.front() == '"') e = e.substr(1, e.size() - 2);
        return e;
    };
    std::string e1 = put_part(1, "small");                          // 5 字节，非末片
    std::string e2 = put_part(2, "tail");
    auto complete_xml = [](std::vector<std::pair<int, std::string>> ps) {
        std::string x = "<CompleteMultipartUpload>";
        for (auto& [n, e] : ps)
            x += "<Part><PartNumber>" + std::to_string(n) + "</PartNumber><ETag>" + e +
                 "</ETag></Part>";
        return x + "</CompleteMultipartUpload>";
    };

    // 非末片小于 5MiB → EntityTooSmall（否则 10000 个 1 字节分片也能提交）
    auto small = sync_wait(svc.dispatch(make_req("POST", "/bkt/mp.bin", complete_xml({{1, e1}, {2, e2}}),
                                                 {{"uploadId", uid}})));
    CHECK_EQ(small.status, 400);
    CHECK(contains(small.small_body, "<Code>EntityTooSmall</Code>"));

    // 乱序 → InvalidPartOrder（此前是 InvalidPart，会让客户端去重传分片）
    auto unordered = sync_wait(svc.dispatch(make_req(
        "POST", "/bkt/mp.bin", complete_xml({{2, e2}, {1, e1}}), {{"uploadId", uid}})));
    CHECK_EQ(unordered.status, 400);
    CHECK(contains(unordered.small_body, "<Code>InvalidPartOrder</Code>"));

    // 分片号越界在 complete 侧也要复核（upload 侧校验的是另一份输入）
    auto oob = sync_wait(svc.dispatch(make_req(
        "POST", "/bkt/mp.bin", complete_xml({{99999, e1}}), {{"uploadId", uid}})));
    CHECK_EQ(oob.status, 400);

    // 末片不受最小尺寸约束：单片上传照常成功，Location 是完整 URL
    auto one = sync_wait(svc.dispatch(
        make_req("POST", "/bkt/mp.bin", complete_xml({{1, e1}}), {{"uploadId", uid}})));
    CHECK_EQ(one.status, 200);
    CHECK(contains(one.small_body, "<Location>http://localhost/bkt/mp.bin</Location>"));
}
