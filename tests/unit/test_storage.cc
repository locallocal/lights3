// 后端一致性套件：同一组用例参数化跑 memory 与 localfs（docs/04 §5）
#include <filesystem>

#include "core/thread_pool.h"
#include "storage/localfs/localfs_backend.h"
#include "storage/memory/memory_backend.h"
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

    // 删除：幂等 + 目录清理；空 bucket 才能删
    CHECK_THROWS_S3(sync_wait(b.delete_bucket("suite-bkt")), S3ErrorCode::BucketNotEmpty);
    for (auto& k : {"dir/a.txt", "photos/2026/a.jpg", "photos/2026/b.jpg",
                    "photos/2027/c.jpg", "readme.md"})
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
