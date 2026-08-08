// 后端一致性套件（docs/storage-backend.md §6）：同一组用例参数化跑所有 IStorageBackend 实现。
// 自 test_storage.cc 提取，test_cloudproxy.cc 复用（docs/cloudproxy-backend.md §10）。
#pragma once

#include <unistd.h>

#include <filesystem>
#include <string>

#include "storage/backend.h"
#include "unit/mini_test.h"

namespace backend_suite {

using namespace lights3;
using namespace lights3::storage;

// 各后端测试共用的临时目录（析构即清理）
struct TmpDir {
    std::filesystem::path path;
    explicit TmpDir(std::string_view prefix = "lights3-test-") {
        path = std::filesystem::temp_directory_path() /
               (std::string(prefix) + std::to_string(::getpid()) + "-" +
                std::to_string(reinterpret_cast<uintptr_t>(this)));
        std::filesystem::create_directories(path);
    }
    ~TmpDir() {
        std::error_code ec;
        std::filesystem::remove_all(path, ec);
    }
};

inline std::string read_all(http::BodyReader& r) {
    std::string out;
    std::byte buf[8192];
    for (;;) {
        size_t n = sync_wait(r.read(std::span(buf)));
        if (n == 0) break;
        out.append(reinterpret_cast<const char*>(buf), n);
    }
    return out;
}

inline PutResult put(IStorageBackend& b, const std::string& bkt, const std::string& key,
                     const std::string& data, ObjectMeta meta = {}) {
    http::StringBodyReader body(data);
    return sync_wait(b.put_object(bkt, key, std::move(meta), body));
}

// 读到一半就抛的 body（Content-MD5 不符、客户端断连、传输停滞都是这个形态）。
// backend.h 契约：body.read 抛异常时后端不得留下任何写入痕迹
struct ThrowingBodyReader final : http::BodyReader {
    explicit ThrowingBodyReader(std::string prefix, uint64_t declared)
        : prefix_(std::move(prefix)), declared_(declared) {}
    Task<size_t> read(std::span<std::byte> buf) override {
        if (sent_ < prefix_.size()) {
            size_t n = std::min(buf.size(), prefix_.size() - sent_);
            memcpy(buf.data(), prefix_.data() + sent_, n);
            sent_ += n;
            co_return n;
        }
        throw s3::S3Error(s3::S3ErrorCode::BadDigest, "injected mid-body failure");
    }
    std::optional<uint64_t> length() const override { return declared_; }

    std::string prefix_;
    uint64_t declared_;
    size_t sent_ = 0;
};

// 对一个后端实例跑完整一致性用例
inline void run_backend_suite(IStorageBackend& b) {
    using s3::S3ErrorCode;

    // bucket 生命周期
    CHECK(!sync_wait(b.bucket_exists("suite-bkt")));
    CHECK_THROWS_S3(sync_wait(b.list_objects("suite-bkt", {})), S3ErrorCode::NoSuchBucket);
    sync_wait(b.create_bucket("suite-bkt"));
    CHECK(sync_wait(b.bucket_exists("suite-bkt")));
    CHECK_THROWS_S3(sync_wait(b.create_bucket("suite-bkt")),
                    S3ErrorCode::BucketAlreadyOwnedByYou);

    // PUT / GET 往返，ETag = 内容 MD5
    ObjectMeta meta;
    meta.content_type = "text/plain";
    meta.user_meta["color"] = "red";
    auto r = put(b, "suite-bkt", "dir/a.txt", "hello world", meta);
    CHECK_EQ(r.etag, "5eb63bbbe01eeed093cb22bb8f5acdc3");  // md5("hello world")

    auto got = sync_wait(b.get_object("suite-bkt", "dir/a.txt", std::nullopt));
    CHECK_EQ(got.meta.size, uint64_t(11));
    CHECK_EQ(got.meta.etag, r.etag);
    CHECK_EQ(got.meta.content_type, "text/plain");
    CHECK_EQ(got.meta.user_meta.at("color"), "red");
    CHECK_EQ(read_all(*got.body), "hello world");

    // body 读到一半抛异常：异常须原样传出，且不得留下半个对象（backend.h 契约）。
    // 这是 Content-MD5 不符（docs/gaps.md §5.6）与客户端断连共用的形态
    {
        ThrowingBodyReader bad("partial-", 64);
        CHECK_THROWS_S3(sync_wait(b.put_object("suite-bkt", "torn.bin", ObjectMeta{}, bad)),
                        S3ErrorCode::BadDigest);
        CHECK_THROWS_S3(sync_wait(b.head_object("suite-bkt", "torn.bin")),
                        S3ErrorCode::NoSuchKey);
        // 覆盖写失败也不得破坏既有对象
        ThrowingBodyReader bad2("partial-", 64);
        CHECK_THROWS_S3(sync_wait(b.put_object("suite-bkt", "dir/a.txt", ObjectMeta{}, bad2)),
                        S3ErrorCode::BadDigest);
        CHECK_EQ(sync_wait(b.head_object("suite-bkt", "dir/a.txt")).etag, r.etag);
    }

    // Range：中段 / 开区间 / 后缀 / 越界
    auto mid = sync_wait(b.get_object("suite-bkt", "dir/a.txt", ByteRange{6, 10}));
    CHECK_EQ(read_all(*mid.body), "world");
    auto tail = sync_wait(b.get_object("suite-bkt", "dir/a.txt",
                                       ByteRange{std::nullopt, uint64_t(5)}));
    CHECK_EQ(read_all(*tail.body), "world");
    auto open_end = sync_wait(b.get_object("suite-bkt", "dir/a.txt",
                                           ByteRange{uint64_t(6), std::nullopt}));
    CHECK_EQ(read_all(*open_end.body), "world");
    CHECK_THROWS_S3(sync_wait(b.get_object("suite-bkt", "dir/a.txt", ByteRange{99, 100})),
                    S3ErrorCode::InvalidRange);

    // 覆盖写 last-write-wins
    put(b, "suite-bkt", "dir/a.txt", "v2");
    auto v2 = sync_wait(b.get_object("suite-bkt", "dir/a.txt", std::nullopt));
    CHECK_EQ(read_all(*v2.body), "v2");

    // 条件 PUT（PutCondition 契约，storage/backend.h）：检查在后端提交点原子完成，
    // 失败不得留下写入痕迹
    {
        auto put_if = [&](const std::string& key, const std::string& data, PutCondition cond) {
            http::StringBodyReader body(data);
            return sync_wait(b.put_object("suite-bkt", key, ObjectMeta{}, body, cond));
        };
        PutCondition none_match;
        none_match.if_none_match = true;
        CHECK_THROWS_S3(put_if("dir/a.txt", "clobber", none_match),
                        S3ErrorCode::PreconditionFailed);
        auto created = put_if("cond/new.txt", "fresh", none_match);  // 不存在 → 创建
        PutCondition match_ok;
        match_ok.if_match_etag = created.etag;
        put_if("cond/new.txt", "fresh2", match_ok);  // etag 相符 → 覆盖
        PutCondition match_stale;
        match_stale.if_match_etag = created.etag;  // 已被上一步覆盖，etag 过期
        CHECK_THROWS_S3(put_if("cond/new.txt", "x", match_stale),
                        S3ErrorCode::PreconditionFailed);
        PutCondition match_absent;
        match_absent.if_match_etag = created.etag;
        CHECK_THROWS_S3(put_if("cond/absent.txt", "x", match_absent), S3ErrorCode::NoSuchKey);
        // 条件失败的路径均未污染现场
        auto cur = sync_wait(b.get_object("suite-bkt", "cond/new.txt", std::nullopt));
        CHECK_EQ(read_all(*cur.body), "fresh2");
        auto keep = sync_wait(b.get_object("suite-bkt", "dir/a.txt", std::nullopt));
        CHECK_EQ(read_all(*keep.body), "v2");
        CHECK_THROWS_S3(sync_wait(b.get_object("suite-bkt", "cond/absent.txt", std::nullopt)),
                        S3ErrorCode::NoSuchKey);
        sync_wait(b.delete_object("suite-bkt", "cond/new.txt"));
    }

    // 错误路径
    CHECK_THROWS_S3(sync_wait(b.get_object("suite-bkt", "missing", std::nullopt)),
                    S3ErrorCode::NoSuchKey);
    CHECK_THROWS_S3(sync_wait(b.get_object("no-such-bkt", "k", std::nullopt)),
                    S3ErrorCode::NoSuchBucket);
    CHECK_THROWS_S3(put(b, "suite-bkt", "../escape", "x"), S3ErrorCode::InvalidArgument);
    CHECK_THROWS_S3(put(b, "suite-bkt", "a/../b", "x"), S3ErrorCode::InvalidArgument);
    // 单段超过文件名上限（255B）统一拒绝（docs/storage-backend.md §3.1）
    CHECK_THROWS_S3(put(b, "suite-bkt", "a/" + std::string(300, 'x'), "x"),
                    S3ErrorCode::KeyTooLongError);

    // list：prefix / delimiter / 分页
    put(b, "suite-bkt", "photos/2026/a.jpg", "1");
    put(b, "suite-bkt", "photos/2026/b.jpg", "2");
    put(b, "suite-bkt", "photos/2027/c.jpg", "3");
    put(b, "suite-bkt", "readme.md", "4");

    ListOptions all;
    auto la = sync_wait(b.list_objects("suite-bkt", all));
    CHECK_EQ(la.objects.size(), size_t(5));
    CHECK(!la.is_truncated);
    CHECK_EQ(la.objects[0].key, "dir/a.txt");  // 字典序

    ListOptions pre;
    pre.prefix = "photos/2026/";
    auto lp = sync_wait(b.list_objects("suite-bkt", pre));
    CHECK_EQ(lp.objects.size(), size_t(2));

    ListOptions delim;
    delim.delimiter = "/";
    auto ld = sync_wait(b.list_objects("suite-bkt", delim));
    CHECK_EQ(ld.objects.size(), size_t(1));  // readme.md
    CHECK_EQ(ld.common_prefixes.size(), size_t(2));  // dir/ photos/
    CHECK_EQ(ld.common_prefixes[0], "dir/");
    CHECK_EQ(ld.common_prefixes[1], "photos/");

    // 分页：max_keys=2，续传后无重复无遗漏
    ListOptions page;
    page.max_keys = 2;
    auto p1 = sync_wait(b.list_objects("suite-bkt", page));
    CHECK_EQ(p1.objects.size(), size_t(2));
    CHECK(p1.is_truncated);
    page.start_after = p1.next_token;
    auto p2 = sync_wait(b.list_objects("suite-bkt", page));
    CHECK_EQ(p2.objects.size(), size_t(2));
    CHECK(p2.is_truncated);
    page.start_after = p2.next_token;
    auto p3 = sync_wait(b.list_objects("suite-bkt", page));
    CHECK_EQ(p3.objects.size(), size_t(1));
    CHECK(!p3.is_truncated);
    CHECK(p1.objects[1].key < p2.objects[0].key);

    // multipart：分片上传-拼接-总 ETag 规则（docs/storage-backend.md §1/§3.2）
    ObjectMeta mmeta;
    mmeta.content_type = "application/x-mpu";
    mmeta.user_meta["origin"] = "suite";
    auto uid = sync_wait(b.create_multipart("suite-bkt", "mp/joined.bin", mmeta));
    CHECK(!uid.empty());
    CHECK_THROWS_S3(sync_wait(b.create_multipart("no-such-bkt", "k", {})),
                    S3ErrorCode::NoSuchBucket);

    auto upload = [&](const std::string& id, int no, const std::string& data) {
        http::StringBodyReader body(data);
        return sync_wait(b.upload_part("suite-bkt", "mp/joined.bin", id, no, body));
    };
    auto r1 = upload(uid, 1, "hello ");
    CHECK_EQ(r1.etag, "f814893777bcc2295fff05f00e508da6");  // md5("hello ")
    auto r2 = upload(uid, 2, "world");
    CHECK_EQ(r2.etag, "7d793037a0760186574b0282f2f435e7");  // md5("world")
    auto r1b = upload(uid, 1, "hello ");  // 同号重传 last-write-wins
    CHECK_EQ(r1b.etag, r1.etag);

    // 分片号越界 / 未知 upload id
    CHECK_THROWS_S3(upload(uid, 0, "x"), S3ErrorCode::InvalidArgument);
    CHECK_THROWS_S3(upload(uid, 10001, "x"), S3ErrorCode::InvalidArgument);
    CHECK_THROWS_S3(upload("00000000000000000000000000000000", 1, "x"),
                    S3ErrorCode::NoSuchUpload);

    auto complete = [&](const std::string& id, std::vector<PartInfo> parts) {
        return sync_wait(b.complete_multipart("suite-bkt", "mp/joined.bin", id, parts));
    };
    // 乱序 / ETag 不匹配 / 缺分片 / 空 parts。乱序有专属码（docs/gaps.md §5.7）：
    // InvalidPart 会让客户端去重传分片，实际要做的是把列表排好序
    CHECK_THROWS_S3(complete(uid, {{2, r2.etag}, {1, r1.etag}}), S3ErrorCode::InvalidPartOrder);
    CHECK_THROWS_S3(complete(uid, {{1, "deadbeef"}}), S3ErrorCode::InvalidPart);
    CHECK_THROWS_S3(complete(uid, {{1, r1.etag}, {3, r2.etag}}), S3ErrorCode::InvalidPart);
    CHECK_THROWS_S3(complete(uid, {}), S3ErrorCode::InvalidPart);
    // key 与 upload 不匹配 → NoSuchUpload
    CHECK_THROWS_S3(sync_wait(b.complete_multipart("suite-bkt", "other.bin", uid,
                                                   std::vector<PartInfo>{{1, r1.etag}})),
                    S3ErrorCode::NoSuchUpload);

    // list_parts / list_multipart_uploads（docs/s3-protocol.md ListParts 支撑）
    auto lparts = sync_wait(b.list_parts("suite-bkt", "mp/joined.bin", uid, {}));
    CHECK_EQ(lparts.parts.size(), size_t(2));
    CHECK(!lparts.is_truncated);
    CHECK_EQ(lparts.parts[0].part_no, 1);
    CHECK_EQ(lparts.parts[0].etag, r1.etag);
    CHECK_EQ(lparts.parts[0].size, uint64_t(6));
    CHECK_EQ(lparts.parts[1].part_no, 2);
    auto lups = sync_wait(b.list_multipart_uploads("suite-bkt", {}));
    CHECK_EQ(lups.uploads.size(), size_t(1));
    CHECK(!lups.is_truncated);
    CHECK_EQ(lups.uploads[0].key, "mp/joined.bin");
    CHECK_EQ(lups.uploads[0].upload_id, uid);
    CHECK_THROWS_S3(sync_wait(b.list_parts("suite-bkt", "mp/joined.bin",
                                           "00000000000000000000000000000000", {})),
                    S3ErrorCode::NoSuchUpload);

    // 分页（docs/gaps.md §5.1）：此前恒报 IsTruncated=false，客户端据此判定已到尾
    {
        ListPartsOptions po;
        po.max_parts = 1;
        auto page1 = sync_wait(b.list_parts("suite-bkt", "mp/joined.bin", uid, po));
        CHECK_EQ(page1.parts.size(), size_t(1));
        CHECK_EQ(page1.parts[0].part_no, 1);
        CHECK(page1.is_truncated);
        CHECK_EQ(page1.next_part_number_marker, 1);
        // 用回传的游标续traversal，第二页到尾
        po.part_number_marker = page1.next_part_number_marker;
        auto page2 = sync_wait(b.list_parts("suite-bkt", "mp/joined.bin", uid, po));
        CHECK_EQ(page2.parts.size(), size_t(1));
        CHECK_EQ(page2.parts[0].part_no, 2);
        CHECK(!page2.is_truncated);
        // max=0 必须是"空且未截断"：空游标 + truncated 会让循环续传的客户端死循环
        po.part_number_marker = 0;
        po.max_parts = 0;
        auto page0 = sync_wait(b.list_parts("suite-bkt", "mp/joined.bin", uid, po));
        CHECK_EQ(page0.parts.size(), size_t(0));
        CHECK(!page0.is_truncated);
    }
    {
        // 再开一个 upload 才能翻页；两个 upload 的 (key, upload_id) 序稳定
        std::string uid2 = sync_wait(b.create_multipart("suite-bkt", "mp/other.bin", {}));
        ListUploadsOptions uo;
        uo.max_uploads = 1;
        auto up1 = sync_wait(b.list_multipart_uploads("suite-bkt", uo));
        CHECK_EQ(up1.uploads.size(), size_t(1));
        CHECK(up1.is_truncated);
        CHECK(!up1.next_key_marker.empty());
        uo.key_marker = up1.next_key_marker;
        uo.upload_id_marker = up1.next_upload_id_marker;
        auto up2 = sync_wait(b.list_multipart_uploads("suite-bkt", uo));
        CHECK_EQ(up2.uploads.size(), size_t(1));
        CHECK(!up2.is_truncated);
        CHECK(up2.uploads[0].key != up1.uploads[0].key);  // 不重复
        // prefix 过滤
        ListUploadsOptions fo;
        fo.prefix = "mp/other";
        auto filtered = sync_wait(b.list_multipart_uploads("suite-bkt", fo));
        CHECK_EQ(filtered.uploads.size(), size_t(1));
        CHECK_EQ(filtered.uploads[0].key, "mp/other.bin");
        sync_wait(b.abort_multipart("suite-bkt", "mp/other.bin", uid2));
    }

    // ETag 允许带引号；总 ETag = md5(分片 md5 拼接)-N
    auto done = complete(uid, {{1, "\"" + r1.etag + "\""}, {2, r2.etag}});
    CHECK_EQ(done.etag, "e09e4fd6265b36115fe3db32df945d84-2");
    auto mo = sync_wait(b.get_object("suite-bkt", "mp/joined.bin", std::nullopt));
    CHECK_EQ(read_all(*mo.body), "hello world");
    CHECK_EQ(mo.meta.etag, done.etag);
    CHECK_EQ(mo.meta.content_type, "application/x-mpu");
    CHECK_EQ(mo.meta.user_meta.at("origin"), "suite");

    // 完成后 upload 即消失；abort 后同理
    CHECK_THROWS_S3(complete(uid, {{1, r1.etag}}), S3ErrorCode::NoSuchUpload);
    CHECK_THROWS_S3(sync_wait(b.abort_multipart("suite-bkt", "mp/joined.bin", uid)),
                    S3ErrorCode::NoSuchUpload);
    auto uid2 = sync_wait(b.create_multipart("suite-bkt", "mp/joined.bin", {}));
    upload(uid2, 1, "zzz");
    sync_wait(b.abort_multipart("suite-bkt", "mp/joined.bin", uid2));
    CHECK_THROWS_S3(upload(uid2, 2, "x"), S3ErrorCode::NoSuchUpload);

    // 删除：幂等 + 目录清理；空 bucket 才能删
    CHECK_THROWS_S3(sync_wait(b.delete_bucket("suite-bkt")), S3ErrorCode::BucketNotEmpty);
    for (auto& k : {"dir/a.txt", "photos/2026/a.jpg", "photos/2026/b.jpg",
                    "photos/2027/c.jpg", "readme.md", "mp/joined.bin"})
        sync_wait(b.delete_object("suite-bkt", k));
    sync_wait(b.delete_object("suite-bkt", "dir/a.txt"));  // 再删不报错
    auto empty = sync_wait(b.list_objects("suite-bkt", {}));
    CHECK_EQ(empty.objects.size(), size_t(0));
    sync_wait(b.delete_bucket("suite-bkt"));
    CHECK(!sync_wait(b.bucket_exists("suite-bkt")));
}

}  // namespace backend_suite
