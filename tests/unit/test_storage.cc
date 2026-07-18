// 后端一致性套件：同一组用例参数化跑 memory / localfs / xlocalfs（docs/04 §5）
#include <filesystem>

#include "core/thread_pool.h"
#include "storage/localfs/localfs_backend.h"
#include "storage/memory/memory_backend.h"
#include "storage/tiered/tiered_backend.h"
#include "storage/xlocalfs/xlocalfs_backend.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::storage;
namespace fs = std::filesystem;

namespace {

std::string read_all(http::BodyReader& r) {
    std::string out;
    std::byte buf[8192];
    for (;;) {
        size_t n = sync_wait(r.read(std::span(buf)));
        if (n == 0) break;
        out.append(reinterpret_cast<const char*>(buf), n);
    }
    return out;
}

PutResult put(IStorageBackend& b, const std::string& bkt, const std::string& key,
              const std::string& data, ObjectMeta meta = {}) {
    http::StringBodyReader body(data);
    return sync_wait(b.put_object(bkt, key, std::move(meta), body));
}

struct TmpDir {
    fs::path path;
    TmpDir() {
        path = fs::temp_directory_path() /
               ("lights3-test-" + std::to_string(::getpid()) + "-" +
                std::to_string(reinterpret_cast<uintptr_t>(this)));
        fs::create_directories(path);
    }
    ~TmpDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

// 对一个后端实例跑完整一致性用例
void run_backend_suite(IStorageBackend& b) {
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

    // 错误路径
    CHECK_THROWS_S3(sync_wait(b.get_object("suite-bkt", "missing", std::nullopt)),
                    S3ErrorCode::NoSuchKey);
    CHECK_THROWS_S3(sync_wait(b.get_object("no-such-bkt", "k", std::nullopt)),
                    S3ErrorCode::NoSuchBucket);
    CHECK_THROWS_S3(put(b, "suite-bkt", "../escape", "x"), S3ErrorCode::InvalidArgument);
    CHECK_THROWS_S3(put(b, "suite-bkt", "a/../b", "x"), S3ErrorCode::InvalidArgument);
    // 单段超过文件名上限（255B）统一拒绝（docs/04 §3.1）
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

    // multipart：分片上传-拼接-总 ETag 规则（docs/04 §1/§3.2）
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
    // 乱序 / ETag 不匹配 / 缺分片 / 空 parts
    CHECK_THROWS_S3(complete(uid, {{2, r2.etag}, {1, r1.etag}}), S3ErrorCode::InvalidPart);
    CHECK_THROWS_S3(complete(uid, {{1, "deadbeef"}}), S3ErrorCode::InvalidPart);
    CHECK_THROWS_S3(complete(uid, {{1, r1.etag}, {3, r2.etag}}), S3ErrorCode::InvalidPart);
    CHECK_THROWS_S3(complete(uid, {}), S3ErrorCode::InvalidPart);
    // key 与 upload 不匹配 → NoSuchUpload
    CHECK_THROWS_S3(sync_wait(b.complete_multipart("suite-bkt", "other.bin", uid,
                                                   std::vector<PartInfo>{{1, r1.etag}})),
                    S3ErrorCode::NoSuchUpload);

    // list_parts / list_multipart_uploads（docs/05 ListParts 支撑）
    auto lparts = sync_wait(b.list_parts("suite-bkt", "mp/joined.bin", uid));
    CHECK_EQ(lparts.size(), size_t(2));
    CHECK_EQ(lparts[0].part_no, 1);
    CHECK_EQ(lparts[0].etag, r1.etag);
    CHECK_EQ(lparts[0].size, uint64_t(6));
    CHECK_EQ(lparts[1].part_no, 2);
    auto lups = sync_wait(b.list_multipart_uploads("suite-bkt"));
    CHECK_EQ(lups.size(), size_t(1));
    CHECK_EQ(lups[0].key, "mp/joined.bin");
    CHECK_EQ(lups[0].upload_id, uid);
    CHECK_THROWS_S3(sync_wait(b.list_parts("suite-bkt", "mp/joined.bin",
                                           "00000000000000000000000000000000")),
                    S3ErrorCode::NoSuchUpload);

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

}  // namespace

TEST(memory_backend_suite) {
    MemoryBackend b;
    run_backend_suite(b);
}

TEST(localfs_backend_suite) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    run_backend_suite(b);
}

TEST(xlocalfs_backend_suite) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    XLocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    run_backend_suite(b);
    sync_wait(b.close());
}

// tiered 对 L2 仍是普通后端（docs/08 §2）：全 local 态跑同一套一致性用例
TEST(tiered_backend_suite) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto local = std::make_shared<LocalFsBackend>(tmp.path / "data", tmp.path / "staging", pool);
    TieredConfig cfg;
    cfg.scan_interval_sec = 0;  // 单测不开后台任务
    auto b = std::make_shared<TieredBackend>(local, std::make_shared<MemoryBackend>(), pool, cfg);
    run_backend_suite(*b);
    sync_wait(b->close());
}

// 跨多个 64KiB 数据块的读写路径：io_uring 流式写入与带偏移读取
TEST(xlocalfs_large_object_roundtrip) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    XLocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));

    std::string data(1 << 20, '\0');  // 1 MiB 伪随机内容
    uint32_t x = 0x12345678;
    for (auto& c : data) {
        x = x * 1664525 + 1013904223;
        c = static_cast<char>(x >> 24);
    }
    auto pr = put(b, "bkt", "big/blob.bin", data);

    auto whole = sync_wait(b.get_object("bkt", "big/blob.bin", std::nullopt));
    CHECK_EQ(whole.meta.size, uint64_t(data.size()));
    CHECK_EQ(whole.meta.etag, pr.etag);
    CHECK(read_all(*whole.body) == data);

    // 跨块边界的 Range
    auto mid = sync_wait(b.get_object("bkt", "big/blob.bin",
                                      ByteRange{uint64_t(65530), uint64_t(65545)}));
    CHECK(read_all(*mid.body) == data.substr(65530, 16));

    // multipart：两个跨块分片经 io_uring 拼接
    auto uid = sync_wait(b.create_multipart("bkt", "big/joined.bin", {}));
    std::string p1 = data.substr(0, 300 * 1024), p2 = data.substr(300 * 1024);
    http::StringBodyReader b1(p1), b2(p2);
    auto r1 = sync_wait(b.upload_part("bkt", "big/joined.bin", uid, 1, b1));
    auto r2 = sync_wait(b.upload_part("bkt", "big/joined.bin", uid, 2, b2));
    sync_wait(b.complete_multipart("bkt", "big/joined.bin", uid,
                                   std::vector<PartInfo>{{1, r1.etag}, {2, r2.etag}}));
    auto joined = sync_wait(b.get_object("bkt", "big/joined.bin", std::nullopt));
    CHECK(read_all(*joined.body) == data);
    sync_wait(b.close());
}

TEST(localfs_atomic_layout) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));
    put(b, "bkt", "x/y.bin", "payload");

    // 磁盘布局符合 docs/04 §3.1：数据文件 + sidecar，staging 无残留
    CHECK(fs::exists(tmp.path / "data/bkt/x/y.bin"));
    CHECK(fs::exists(tmp.path / "data/bkt/x/y.bin.lights3-meta"));
    size_t staging_leftover = 0;
    for (auto& e : fs::recursive_directory_iterator(tmp.path / "staging"))
        if (e.is_regular_file()) ++staging_leftover;
    CHECK_EQ(staging_leftover, size_t(0));

    // 内部保留名不可作为 key
    CHECK_THROWS_S3(put(b, "bkt", "x/y.bin.lights3-meta", "z"),
                    lights3::s3::S3ErrorCode::InvalidArgument);
}

TEST(localfs_multipart_layout_and_cleanup) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    LocalFsBackend b(tmp.path / "data", tmp.path / "staging", pool);
    sync_wait(b.create_bucket("bkt"));

    // 分片落 <staging>/mpu/<id>/，complete 后目录清理、对象原子落地（docs/04 §3.2）
    auto uid = sync_wait(b.create_multipart("bkt", "big.bin", {}));
    http::StringBodyReader part("data");
    auto pr = sync_wait(b.upload_part("bkt", "big.bin", uid, 1, part));
    fs::path mpu = tmp.path / "staging/mpu" / uid;
    CHECK(fs::exists(mpu / "manifest"));
    CHECK(fs::exists(mpu / "part.00001"));
    sync_wait(b.complete_multipart("bkt", "big.bin", uid,
                                   std::vector<PartInfo>{{1, pr.etag}}));
    CHECK(!fs::exists(mpu));
    CHECK(fs::exists(tmp.path / "data/bkt/big.bin"));
    CHECK(fs::exists(tmp.path / "data/bkt/big.bin.lights3-meta"));

    // 超期（>7 天）孤儿上传在新实例启动时被清理
    auto stale = sync_wait(b.create_multipart("bkt", "stale.bin", {}));
    fs::path stale_dir = tmp.path / "staging/mpu" / stale;
    fs::last_write_time(stale_dir / "manifest",
                        fs::file_time_type::clock::now() - std::chrono::hours(24 * 8));
    LocalFsBackend b2(tmp.path / "data", tmp.path / "staging", pool);
    CHECK(!fs::exists(stale_dir));
}
