// Backend conformance suite (docs/storage-backend.md §6): the same set of cases runs parameterized over all IStorageBackend implementations.
// Extracted from test_storage.cc, reused by test_cloudproxy.cc (docs/cloudproxy-backend.md §10).
#pragma once

#include <unistd.h>

#include <filesystem>
#include <string>

#include "storage/backend.h"
#include "unit/mini_test.h"

namespace backend_suite {

using namespace lights3;
using namespace lights3::storage;

// Temporary directory shared by backend tests (cleaned up on destruction)
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

// A body that throws mid-read (Content-MD5 mismatch, client disconnect, and stalled transfer all take this shape).
// backend.h contract: when body.read throws, the backend must not leave behind any trace of the write
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

// Run the full conformance cases against one backend instance
inline void run_backend_suite(IStorageBackend& b) {
    using s3::S3ErrorCode;

    // Bucket lifecycle
    CHECK(!sync_wait(b.bucket_exists("suite-bkt")));
    CHECK_THROWS_S3(sync_wait(b.list_objects("suite-bkt", {})), S3ErrorCode::NoSuchBucket);
    sync_wait(b.create_bucket("suite-bkt"));
    CHECK(sync_wait(b.bucket_exists("suite-bkt")));
    CHECK_THROWS_S3(sync_wait(b.create_bucket("suite-bkt")),
                    S3ErrorCode::BucketAlreadyOwnedByYou);

    // PUT / GET round trip, ETag = MD5 of content
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

    // Body throws mid-read: the exception must propagate as-is, and no partial object may be left behind (backend.h contract).
    // This is the shape shared by Content-MD5 mismatch (docs/gaps.md §5.6) and client disconnect
    {
        ThrowingBodyReader bad("partial-", 64);
        CHECK_THROWS_S3(sync_wait(b.put_object("suite-bkt", "torn.bin", ObjectMeta{}, bad)),
                        S3ErrorCode::BadDigest);
        CHECK_THROWS_S3(sync_wait(b.head_object("suite-bkt", "torn.bin")),
                        S3ErrorCode::NoSuchKey);
        // A failed overwrite must not corrupt the existing object either
        ThrowingBodyReader bad2("partial-", 64);
        CHECK_THROWS_S3(sync_wait(b.put_object("suite-bkt", "dir/a.txt", ObjectMeta{}, bad2)),
                        S3ErrorCode::BadDigest);
        CHECK_EQ(sync_wait(b.head_object("suite-bkt", "dir/a.txt")).etag, r.etag);
    }

    // Range: middle segment / open-ended / suffix / out of bounds
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

    // Overwrite is last-write-wins
    put(b, "suite-bkt", "dir/a.txt", "v2");
    auto v2 = sync_wait(b.get_object("suite-bkt", "dir/a.txt", std::nullopt));
    CHECK_EQ(read_all(*v2.body), "v2");

    // Conditional PUT (PutCondition contract, storage/backend.h): the check completes atomically at the backend's commit point;
    // a failure must not leave behind any trace of the write
    {
        auto put_if = [&](const std::string& key, const std::string& data, PutCondition cond) {
            http::StringBodyReader body(data);
            return sync_wait(b.put_object("suite-bkt", key, ObjectMeta{}, body, cond));
        };
        PutCondition none_match;
        none_match.if_none_match = true;
        CHECK_THROWS_S3(put_if("dir/a.txt", "clobber", none_match),
                        S3ErrorCode::PreconditionFailed);
        auto created = put_if("cond/new.txt", "fresh", none_match);  // absent -> create
        PutCondition match_ok;
        match_ok.if_match_etag = created.etag;
        put_if("cond/new.txt", "fresh2", match_ok);  // etag matches -> overwrite
        PutCondition match_stale;
        match_stale.if_match_etag = created.etag;  // overwritten by the previous step, etag is stale
        CHECK_THROWS_S3(put_if("cond/new.txt", "x", match_stale),
                        S3ErrorCode::PreconditionFailed);
        PutCondition match_absent;
        match_absent.if_match_etag = created.etag;
        CHECK_THROWS_S3(put_if("cond/absent.txt", "x", match_absent), S3ErrorCode::NoSuchKey);
        // None of the failed-condition paths polluted the state
        auto cur = sync_wait(b.get_object("suite-bkt", "cond/new.txt", std::nullopt));
        CHECK_EQ(read_all(*cur.body), "fresh2");
        auto keep = sync_wait(b.get_object("suite-bkt", "dir/a.txt", std::nullopt));
        CHECK_EQ(read_all(*keep.body), "v2");
        CHECK_THROWS_S3(sync_wait(b.get_object("suite-bkt", "cond/absent.txt", std::nullopt)),
                        S3ErrorCode::NoSuchKey);
        sync_wait(b.delete_object("suite-bkt", "cond/new.txt"));
    }

    // Error paths
    CHECK_THROWS_S3(sync_wait(b.get_object("suite-bkt", "missing", std::nullopt)),
                    S3ErrorCode::NoSuchKey);
    CHECK_THROWS_S3(sync_wait(b.get_object("no-such-bkt", "k", std::nullopt)),
                    S3ErrorCode::NoSuchBucket);
    CHECK_THROWS_S3(put(b, "suite-bkt", "../escape", "x"), S3ErrorCode::InvalidArgument);
    CHECK_THROWS_S3(put(b, "suite-bkt", "a/../b", "x"), S3ErrorCode::InvalidArgument);
    // The 255B per-segment limit has been pushed down to localfs only (docs/gaps.md §6.3 validate_fs_object_key);
    // the shared layer here only guarantees the 1024B total-length limit still holds
    CHECK_THROWS_S3(put(b, "suite-bkt", std::string(1100, 'x'), "x"),
                    S3ErrorCode::KeyTooLongError);

    // Directory marker objects (docs/gaps.md §6.3): the S3 console's "create folder" and the directory
    // semantics of s3fs/goofys/rclone all depend on them. All backends support them uniformly -- localfs
    // carries them as a marker file inside the directory, the other backends are flat key spaces anyway
    {
        auto pr = put(b, "suite-bkt", "folder/", "");
        (void)pr;
        auto head = sync_wait(b.head_object("suite-bkt", "folder/"));
        CHECK_EQ(head.size, uint64_t(0));
        auto g = sync_wait(b.get_object("suite-bkt", "folder/", std::nullopt));
        CHECK_EQ(read_all(*g.body), "");
        // Visible in listings, and coexists with real objects beneath it
        put(b, "suite-bkt", "folder/file.txt", "in-folder");
        ListOptions lo;
        lo.prefix = "folder/";
        auto lr = sync_wait(b.list_objects("suite-bkt", lo));
        CHECK_EQ(lr.objects.size(), size_t(2));
        CHECK_EQ(lr.objects[0].key, "folder/");  // lexicographic order: the directory marker precedes its contents
        CHECK_EQ(lr.objects[1].key, "folder/file.txt");
        sync_wait(b.delete_object("suite-bkt", "folder/file.txt"));
        sync_wait(b.delete_object("suite-bkt", "folder/"));
        CHECK_THROWS_S3(sync_wait(b.head_object("suite-bkt", "folder/")),
                        S3ErrorCode::NoSuchKey);
    }

    // list: prefix / delimiter / pagination
    put(b, "suite-bkt", "photos/2026/a.jpg", "1");
    put(b, "suite-bkt", "photos/2026/b.jpg", "2");
    put(b, "suite-bkt", "photos/2027/c.jpg", "3");
    put(b, "suite-bkt", "readme.md", "4");

    ListOptions all;
    auto la = sync_wait(b.list_objects("suite-bkt", all));
    CHECK_EQ(la.objects.size(), size_t(5));
    CHECK(!la.is_truncated);
    CHECK_EQ(la.objects[0].key, "dir/a.txt");  // lexicographic order

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

    // Pagination: max_keys=2, no duplicates and no gaps after resuming
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

    // multipart: part upload - assembly - overall ETag rule (docs/storage-backend.md §1/§3.2)
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
    auto r1b = upload(uid, 1, "hello ");  // re-upload with the same part number is last-write-wins
    CHECK_EQ(r1b.etag, r1.etag);

    // Part number out of range / unknown upload id
    CHECK_THROWS_S3(upload(uid, 0, "x"), S3ErrorCode::InvalidArgument);
    CHECK_THROWS_S3(upload(uid, 10001, "x"), S3ErrorCode::InvalidArgument);
    CHECK_THROWS_S3(upload("00000000000000000000000000000000", 1, "x"),
                    S3ErrorCode::NoSuchUpload);

    auto complete = [&](const std::string& id, std::vector<PartInfo> parts) {
        return sync_wait(b.complete_multipart("suite-bkt", "mp/joined.bin", id, parts));
    };
    // Out of order / ETag mismatch / missing part / empty parts. Out-of-order has its own code (docs/gaps.md §5.7):
    // InvalidPart would make clients re-upload parts, when what is actually needed is sorting the list
    CHECK_THROWS_S3(complete(uid, {{2, r2.etag}, {1, r1.etag}}), S3ErrorCode::InvalidPartOrder);
    CHECK_THROWS_S3(complete(uid, {{1, "deadbeef"}}), S3ErrorCode::InvalidPart);
    CHECK_THROWS_S3(complete(uid, {{1, r1.etag}, {3, r2.etag}}), S3ErrorCode::InvalidPart);
    CHECK_THROWS_S3(complete(uid, {}), S3ErrorCode::InvalidPart);
    // key does not match the upload -> NoSuchUpload
    CHECK_THROWS_S3(sync_wait(b.complete_multipart("suite-bkt", "other.bin", uid,
                                                   std::vector<PartInfo>{{1, r1.etag}})),
                    S3ErrorCode::NoSuchUpload);

    // list_parts / list_multipart_uploads (backing docs/s3-protocol.md ListParts)
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

    // Pagination (docs/gaps.md §5.1): this used to always report IsTruncated=false, which clients take to mean the end was reached
    {
        ListPartsOptions po;
        po.max_parts = 1;
        auto page1 = sync_wait(b.list_parts("suite-bkt", "mp/joined.bin", uid, po));
        CHECK_EQ(page1.parts.size(), size_t(1));
        CHECK_EQ(page1.parts[0].part_no, 1);
        CHECK(page1.is_truncated);
        CHECK_EQ(page1.next_part_number_marker, 1);
        // Resume the traversal with the returned cursor, the second page reaches the end
        po.part_number_marker = page1.next_part_number_marker;
        auto page2 = sync_wait(b.list_parts("suite-bkt", "mp/joined.bin", uid, po));
        CHECK_EQ(page2.parts.size(), size_t(1));
        CHECK_EQ(page2.parts[0].part_no, 2);
        CHECK(!page2.is_truncated);
        // max=0 must be "empty and not truncated": an empty cursor + truncated would put loop-resuming clients into an infinite loop
        po.part_number_marker = 0;
        po.max_parts = 0;
        auto page0 = sync_wait(b.list_parts("suite-bkt", "mp/joined.bin", uid, po));
        CHECK_EQ(page0.parts.size(), size_t(0));
        CHECK(!page0.is_truncated);
    }
    {
        // Open a second upload so there is something to page over; the (key, upload_id) order of the two uploads is stable
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
        CHECK(up2.uploads[0].key != up1.uploads[0].key);  // no duplicates
        // prefix filtering
        ListUploadsOptions fo;
        fo.prefix = "mp/other";
        auto filtered = sync_wait(b.list_multipart_uploads("suite-bkt", fo));
        CHECK_EQ(filtered.uploads.size(), size_t(1));
        CHECK_EQ(filtered.uploads[0].key, "mp/other.bin");
        sync_wait(b.abort_multipart("suite-bkt", "mp/other.bin", uid2));
    }

    // ETags may be quoted; overall ETag = md5(concatenation of part md5s)-N
    auto done = complete(uid, {{1, "\"" + r1.etag + "\""}, {2, r2.etag}});
    CHECK_EQ(done.etag, "e09e4fd6265b36115fe3db32df945d84-2");
    auto mo = sync_wait(b.get_object("suite-bkt", "mp/joined.bin", std::nullopt));
    CHECK_EQ(read_all(*mo.body), "hello world");
    CHECK_EQ(mo.meta.etag, done.etag);
    CHECK_EQ(mo.meta.content_type, "application/x-mpu");
    CHECK_EQ(mo.meta.user_meta.at("origin"), "suite");

    // The upload disappears once completed; same after abort
    CHECK_THROWS_S3(complete(uid, {{1, r1.etag}}), S3ErrorCode::NoSuchUpload);
    CHECK_THROWS_S3(sync_wait(b.abort_multipart("suite-bkt", "mp/joined.bin", uid)),
                    S3ErrorCode::NoSuchUpload);
    auto uid2 = sync_wait(b.create_multipart("suite-bkt", "mp/joined.bin", {}));
    upload(uid2, 1, "zzz");
    sync_wait(b.abort_multipart("suite-bkt", "mp/joined.bin", uid2));
    CHECK_THROWS_S3(upload(uid2, 2, "x"), S3ErrorCode::NoSuchUpload);

    // Delete: idempotent + directory cleanup; only an empty bucket can be deleted
    CHECK_THROWS_S3(sync_wait(b.delete_bucket("suite-bkt")), S3ErrorCode::BucketNotEmpty);
    for (auto& k : {"dir/a.txt", "photos/2026/a.jpg", "photos/2026/b.jpg",
                    "photos/2027/c.jpg", "readme.md", "mp/joined.bin"})
        sync_wait(b.delete_object("suite-bkt", k));
    sync_wait(b.delete_object("suite-bkt", "dir/a.txt"));  // deleting again does not error
    auto empty = sync_wait(b.list_objects("suite-bkt", {}));
    CHECK_EQ(empty.objects.size(), size_t(0));
    sync_wait(b.delete_bucket("suite-bkt"));
    CHECK(!sync_wait(b.bucket_exists("suite-bkt")));
}

}  // namespace backend_suite
