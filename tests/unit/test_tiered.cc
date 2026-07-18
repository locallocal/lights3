// 分层存储后端单测（docs/08 §10 P1–P4 验收）：
// 一致性套件、tier 状态机、覆盖/删除入 GC、scanner 判冷与崩溃恢复、空间兜底。
// 云侧用 MemoryBackend 充当（经计数包装断言云端调用次数）。
#include <atomic>
#include <filesystem>
#include <fstream>

#include "core/thread_pool.h"
#include "storage/localfs/fs_util.h"
#include "storage/memory/memory_backend.h"
#include "storage/registry.h"
#include "storage/tiered/tiered_backend.h"
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
               ("lights3-tier-" + std::to_string(::getpid()) + "-" +
                std::to_string(reinterpret_cast<uintptr_t>(this)));
        fs::create_directories(path);
    }
    ~TmpDir() {
        std::error_code ec;
        fs::remove_all(path, ec);
    }
};

// 计数包装：断言 tiered 何时真正触碰云端
class CountingCloud final : public IStorageBackend {
public:
    std::shared_ptr<MemoryBackend> inner = std::make_shared<MemoryBackend>();
    std::atomic<int> puts{0}, gets{0}, heads{0}, deletes{0};

    Task<void> create_bucket(std::string_view b) override {
        co_return co_await inner->create_bucket(b);
    }
    Task<void> delete_bucket(std::string_view b) override {
        co_return co_await inner->delete_bucket(b);
    }
    Task<bool> bucket_exists(std::string_view b) override {
        co_return co_await inner->bucket_exists(b);
    }
    Task<std::vector<BucketInfo>> list_buckets() override {
        co_return co_await inner->list_buckets();
    }
    Task<ObjectStream> get_object(std::string_view b, std::string_view k,
                                  std::optional<ByteRange> r) override {
        ++gets;
        co_return co_await inner->get_object(b, k, r);
    }
    Task<PutResult> put_object(std::string_view b, std::string_view k, ObjectMeta m,
                               http::BodyReader& body) override {
        ++puts;
        co_return co_await inner->put_object(b, k, std::move(m), body);
    }
    Task<ObjectMeta> head_object(std::string_view b, std::string_view k) override {
        ++heads;
        co_return co_await inner->head_object(b, k);
    }
    Task<void> delete_object(std::string_view b, std::string_view k) override {
        ++deletes;
        co_return co_await inner->delete_object(b, k);
    }
    Task<ListResult> list_objects(std::string_view b, const ListOptions& o) override {
        co_return co_await inner->list_objects(b, o);
    }
    Task<std::string> create_multipart(std::string_view b, std::string_view k,
                                       ObjectMeta m) override {
        co_return co_await inner->create_multipart(b, k, std::move(m));
    }
    Task<PutResult> upload_part(std::string_view b, std::string_view k, std::string_view id,
                                int no, http::BodyReader& body) override {
        co_return co_await inner->upload_part(b, k, id, no, body);
    }
    Task<PutResult> complete_multipart(std::string_view b, std::string_view k,
                                       std::string_view id,
                                       std::span<const PartInfo> parts) override {
        co_return co_await inner->complete_multipart(b, k, id, parts);
    }
    Task<void> abort_multipart(std::string_view b, std::string_view k,
                               std::string_view id) override {
        co_return co_await inner->abort_multipart(b, k, id);
    }
    Task<std::vector<PartMeta>> list_parts(std::string_view b, std::string_view k,
                                           std::string_view id) override {
        co_return co_await inner->list_parts(b, k, id);
    }
    Task<std::vector<UploadInfo>> list_multipart_uploads(std::string_view b) override {
        co_return co_await inner->list_multipart_uploads(b);
    }
};

struct Fixture {
    TmpDir tmp;
    std::shared_ptr<ThreadPool> pool = std::make_shared<ThreadPool>(4);
    std::shared_ptr<LocalFsBackend> local;
    std::shared_ptr<CountingCloud> cloud = std::make_shared<CountingCloud>();
    std::shared_ptr<TieredBackend> tiered;

    explicit Fixture(TieredConfig cfg = {}) {
        cfg.scan_interval_sec = 0;  // 关闭后台定时任务，用手动钩子驱动
        local = std::make_shared<LocalFsBackend>(tmp.path / "data", tmp.path / "staging", pool);
        tiered = std::make_shared<TieredBackend>(local, cloud, pool, cfg);
    }
    ~Fixture() { sync_wait(tiered->close()); }

    fs::path data_path(const std::string& bkt, const std::string& key) const {
        return tmp.path / "data" / bkt / key;
    }
    fsutil::TierInfo tier_of(const std::string& bkt, const std::string& key) const {
        fsutil::TierInfo t;
        fsutil::load_object_meta(data_path(bkt, key), key, &t);
        return t;
    }
    uint64_t disk_size(const std::string& bkt, const std::string& key) const {
        return fs::file_size(data_path(bkt, key));
    }
    size_t gc_entries() const {
        size_t n = 0;
        for (auto& e : fs::directory_iterator(tmp.path / "staging/tier/gc"))
            if (e.is_regular_file()) ++n;
        return n;
    }
};

std::string make_data(size_t n) {
    std::string data(n, '\0');
    uint32_t x = 0xabcd1234;
    for (auto& c : data) {
        x = x * 1664525 + 1013904223;
        c = static_cast<char>(x >> 24);
    }
    return data;
}

}  // namespace

// 状态机：local → remote（下沉）→ cached（GET 回迁）→ remote（二次判冷零上传）
TEST(tiered_demote_get_cache_cycle) {
    Fixture f;
    std::string data = make_data(200 * 1024);
    sync_wait(f.tiered->create_bucket("bkt"));
    auto pr = put(*f.tiered, "bkt", "dir/cold.bin", data);

    // 下沉：本地变 0 长度 stub，sidecar tier=remote，云端恰好一份
    sync_wait(f.tiered->demote_object("bkt", "dir/cold.bin"));
    CHECK_EQ(f.disk_size("bkt", "dir/cold.bin"), uint64_t(0));
    CHECK(f.tier_of("bkt", "dir/cold.bin").tier == fsutil::Tier::kRemote);
    CHECK_EQ(f.cloud->puts.load(), 1);

    // HEAD 完全本地完成：size/etag 为原始值，不触碰云端
    int heads_before = f.cloud->heads.load();
    auto hm = sync_wait(f.tiered->head_object("bkt", "dir/cold.bin"));
    CHECK_EQ(hm.size, uint64_t(data.size()));
    CHECK_EQ(hm.etag, pr.etag);
    CHECK_EQ(f.cloud->heads.load(), heads_before);

    // List 识别 stub 的 size
    auto lr = sync_wait(f.tiered->list_objects("bkt", {}));
    CHECK_EQ(lr.objects.size(), size_t(1));
    CHECK_EQ(lr.objects[0].size, uint64_t(data.size()));

    // 透明回读：数据/ETag 与下沉前一致；EOF 后 Tee 缓存提交为 cached
    auto got = sync_wait(f.tiered->get_object("bkt", "dir/cold.bin", std::nullopt));
    CHECK_EQ(got.meta.etag, pr.etag);
    CHECK(read_all(*got.body) == data);
    CHECK_EQ(f.cloud->gets.load(), 1);
    CHECK(f.tier_of("bkt", "dir/cold.bin").tier == fsutil::Tier::kCached);
    CHECK_EQ(f.disk_size("bkt", "dir/cold.bin"), uint64_t(data.size()));

    // cached 命中本地，不再触碰云端
    auto again = sync_wait(f.tiered->get_object("bkt", "dir/cold.bin", std::nullopt));
    CHECK(read_all(*again.body) == data);
    CHECK_EQ(f.cloud->gets.load(), 1);

    // cached 再判冷：校验云副本后直接 stub 化，零上传流量
    sync_wait(f.tiered->demote_object("bkt", "dir/cold.bin"));
    CHECK(f.tier_of("bkt", "dir/cold.bin").tier == fsutil::Tier::kRemote);
    CHECK_EQ(f.disk_size("bkt", "dir/cold.bin"), uint64_t(0));
    CHECK_EQ(f.cloud->puts.load(), 1);
}

// Range GET 命中 remote：直接透传云端，不产生部分缓存
TEST(tiered_range_get_passthrough) {
    TieredConfig cfg;
    cfg.cache_fill_on_range = false;
    Fixture f(cfg);
    std::string data = make_data(100 * 1024);
    sync_wait(f.tiered->create_bucket("bkt"));
    put(*f.tiered, "bkt", "r.bin", data);
    sync_wait(f.tiered->demote_object("bkt", "r.bin"));

    auto mid = sync_wait(f.tiered->get_object("bkt", "r.bin",
                                              ByteRange{uint64_t(1000), uint64_t(2999)}));
    CHECK(read_all(*mid.body) == data.substr(1000, 2000));
    CHECK(mid.range.has_value());
    CHECK_EQ(mid.meta.size, uint64_t(data.size()));  // 206 的总长取本地 meta
    CHECK(f.tier_of("bkt", "r.bin").tier == fsutil::Tier::kRemote);
    CHECK_EQ(f.disk_size("bkt", "r.bin"), uint64_t(0));

    // 手动整对象回迁（Range 后台回迁走同一路径）
    sync_wait(f.tiered->promote_object("bkt", "r.bin"));
    CHECK(f.tier_of("bkt", "r.bin").tier == fsutil::Tier::kCached);
    auto whole = sync_wait(f.tiered->get_object("bkt", "r.bin", std::nullopt));
    CHECK(read_all(*whole.body) == data);
}

// multipart 对象（etag 带 -N）：下沉按字节数校验，回读后 ETag 恒为 -N 形式
TEST(tiered_multipart_object_demote) {
    Fixture f;
    sync_wait(f.tiered->create_bucket("bkt"));
    std::string p1 = make_data(300 * 1024), p2 = make_data(123 * 1024);
    auto uid = sync_wait(f.tiered->create_multipart("bkt", "mp.bin", {}));
    http::StringBodyReader b1(p1), b2(p2);
    auto r1 = sync_wait(f.tiered->upload_part("bkt", "mp.bin", uid, 1, b1));
    auto r2 = sync_wait(f.tiered->upload_part("bkt", "mp.bin", uid, 2, b2));
    auto cr = sync_wait(f.tiered->complete_multipart(
        "bkt", "mp.bin", uid, std::vector<PartInfo>{{1, r1.etag}, {2, r2.etag}}));
    CHECK(cr.etag.find('-') != std::string::npos);

    sync_wait(f.tiered->demote_object("bkt", "mp.bin"));
    auto t = f.tier_of("bkt", "mp.bin");
    CHECK(t.tier == fsutil::Tier::kRemote);
    CHECK(t.remote_etag != cr.etag);  // 云端 etag 独立记录，不外泄

    auto got = sync_wait(f.tiered->get_object("bkt", "mp.bin", std::nullopt));
    CHECK_EQ(got.meta.etag, cr.etag);  // 对外 ETag 恒等原则
    CHECK(read_all(*got.body) == p1 + p2);
    CHECK(f.tier_of("bkt", "mp.bin").tier == fsutil::Tier::kCached);
}

// PUT 覆盖 remote：tier 回 local，旧云副本入 GC 并被异步删除
TEST(tiered_overwrite_and_delete_gc) {
    Fixture f;
    sync_wait(f.tiered->create_bucket("bkt"));
    put(*f.tiered, "bkt", "o.bin", make_data(64 * 1024));
    sync_wait(f.tiered->demote_object("bkt", "o.bin"));
    CHECK_EQ(f.gc_entries(), size_t(0));

    // PUT 覆盖：写回本地，云副本成孤儿
    std::string v2 = make_data(32 * 1024);
    put(*f.tiered, "bkt", "o.bin", v2);
    CHECK(f.tier_of("bkt", "o.bin").tier == fsutil::Tier::kLocal);
    CHECK_EQ(f.gc_entries(), size_t(1));
    sync_wait(f.tiered->run_gc_once());
    CHECK_EQ(f.gc_entries(), size_t(0));
    CHECK_THROWS_S3(sync_wait(f.cloud->inner->get_object("bkt", "o.bin", std::nullopt)),
                    s3::S3ErrorCode::NoSuchKey);  // 孤儿已删

    // DELETE remote：本地立即删（不等云端），云副本经 GC 删除
    sync_wait(f.tiered->demote_object("bkt", "o.bin"));
    sync_wait(f.tiered->delete_object("bkt", "o.bin"));
    CHECK(!fs::exists(f.data_path("bkt", "o.bin")));
    CHECK_EQ(f.gc_entries(), size_t(1));
    sync_wait(f.tiered->run_gc_once());
    CHECK_EQ(f.gc_entries(), size_t(0));
    CHECK_THROWS_S3(sync_wait(f.cloud->inner->get_object("bkt", "o.bin", std::nullopt)),
                    s3::S3ErrorCode::NoSuchKey);
}

// GC 绝不删活副本：同 key 重新下沉后，旧的过期条目直接作废
TEST(tiered_gc_never_deletes_live_copy) {
    Fixture f;
    sync_wait(f.tiered->create_bucket("bkt"));
    std::string data = make_data(16 * 1024);
    put(*f.tiered, "bkt", "live.bin", data);
    sync_wait(f.tiered->demote_object("bkt", "live.bin"));
    put(*f.tiered, "bkt", "live.bin", data);  // 覆盖（同内容）→ 旧副本入 GC
    CHECK_EQ(f.gc_entries(), size_t(1));
    sync_wait(f.tiered->demote_object("bkt", "live.bin"));  // 再下沉：云端又是活副本

    sync_wait(f.tiered->run_gc_once());
    CHECK_EQ(f.gc_entries(), size_t(0));
    // 活副本未被误删，对象仍可读
    auto got = sync_wait(f.tiered->get_object("bkt", "live.bin", std::nullopt));
    CHECK(read_all(*got.body) == data);
}

// scanner：cold_after=0 全量判冷下沉；崩溃恢复（remote 但数据未回收）补做 stub 化
TEST(tiered_scanner_cold_and_crash_recovery) {
    TieredConfig cfg;
    cfg.cold_after_sec = 0;
    Fixture f(cfg);
    sync_wait(f.tiered->create_bucket("bkt"));
    std::string d1 = make_data(50 * 1024), d2 = make_data(60 * 1024);
    put(*f.tiered, "bkt", "a/x.bin", d1);
    put(*f.tiered, "bkt", "b/y.bin", d2);

    sync_wait(f.tiered->scan_once());
    CHECK(f.tier_of("bkt", "a/x.bin").tier == fsutil::Tier::kRemote);
    CHECK(f.tier_of("bkt", "b/y.bin").tier == fsutil::Tier::kRemote);
    CHECK_EQ(f.cloud->puts.load(), 2);

    // 模拟 §5.2 b/c 之间崩溃：GET 回迁成 cached 后，把 sidecar 手动改回 remote
    //（等价于 sidecar 已提交 remote、数据文件还是全量的状态）
    auto got = sync_wait(f.tiered->get_object("bkt", "a/x.bin", std::nullopt));
    CHECK(read_all(*got.body) == d1);
    CHECK(f.tier_of("bkt", "a/x.bin").tier == fsutil::Tier::kCached);
    fs::path sc = f.data_path("bkt", "a/x.bin").string() + fsutil::kSidecarSuffix;
    std::string content;
    {
        std::ifstream in(sc, std::ios::binary);
        content.assign(std::istreambuf_iterator<char>(in), {});
    }
    auto pos = content.find("tier\tcached");
    CHECK(pos != std::string::npos);
    content.replace(pos, 11, "tier\tremote");
    {
        std::ofstream out(sc, std::ios::binary | std::ios::trunc);
        out << content;
    }
    CHECK(f.disk_size("bkt", "a/x.bin") > 0);

    sync_wait(f.tiered->scan_once());  // 特征"remote 但 stat size>0" → 补回收
    CHECK_EQ(f.disk_size("bkt", "a/x.bin"), uint64_t(0));
    CHECK(f.tier_of("bkt", "a/x.bin").tier == fsutil::Tier::kRemote);
    CHECK_EQ(f.cloud->puts.load(), 2);  // 恢复不重传

    // 数据仍可读（云端为准）
    auto again = sync_wait(f.tiered->get_object("bkt", "a/x.bin", std::nullopt));
    CHECK(read_all(*again.body) == d1);
}

// 空间兜底（需求 3）：余量不足时 GET 纯透传，不缓存、不失败
TEST(tiered_space_fallback_passthrough) {
    TieredConfig cfg;
    cfg.min_free_bytes = ~uint64_t(0) / 2;  // 永远"空间不足"
    Fixture f(cfg);
    std::string data = make_data(80 * 1024);
    sync_wait(f.tiered->create_bucket("bkt"));
    put(*f.tiered, "bkt", "p.bin", data);
    sync_wait(f.tiered->demote_object("bkt", "p.bin"));

    auto got = sync_wait(f.tiered->get_object("bkt", "p.bin", std::nullopt));
    CHECK(read_all(*got.body) == data);  // 读路径不因缓存失败而失败
    CHECK(f.tier_of("bkt", "p.bin").tier == fsutil::Tier::kRemote);  // 未缓存
    CHECK_EQ(f.disk_size("bkt", "p.bin"), uint64_t(0));

    // 回迁同样放弃，但对象保持可读
    sync_wait(f.tiered->promote_object("bkt", "p.bin"));
    CHECK(f.tier_of("bkt", "p.bin").tier == fsutil::Tier::kRemote);
}

// quota 水位回收：先 cached（零上传）再 local，降到低水位即停
TEST(tiered_quota_watermark_eviction) {
    TieredConfig cfg;
    cfg.cold_after_sec = 1 << 30;      // 判冷不触发，只测水位
    cfg.quota_bytes = 100 * 1024;      // 高水位 85KiB，低水位 70KiB
    Fixture f(cfg);
    sync_wait(f.tiered->create_bucket("bkt"));
    std::string a = make_data(40 * 1024), b = make_data(50 * 1024);
    put(*f.tiered, "bkt", "hot.bin", a);   // 40K local
    put(*f.tiered, "bkt", "warm.bin", b);  // 50K local → 合计 90K > 85K
    // 先把 warm 变 cached：下沉后读回（此时 warm atime 最新）
    sync_wait(f.tiered->demote_object("bkt", "warm.bin"));
    auto got = sync_wait(f.tiered->get_object("bkt", "warm.bin", std::nullopt));
    CHECK(read_all(*got.body) == b);
    CHECK(f.tier_of("bkt", "warm.bin").tier == fsutil::Tier::kCached);

    int puts_before = f.cloud->puts.load();
    sync_wait(f.tiered->scan_once());
    // 90K→85K 超限，需回收到 70K：cached 的 warm(50K) 首选牺牲，回收后 40K 达标
    CHECK(f.tier_of("bkt", "warm.bin").tier == fsutil::Tier::kRemote);
    CHECK(f.tier_of("bkt", "hot.bin").tier == fsutil::Tier::kLocal);  // local 无需上传
    CHECK_EQ(f.cloud->puts.load(), puts_before);  // 零上传流量
}

// registry 两阶段构建：tiered 引用叶子后端；循环/未知引用与非法 local 报错
TEST(tiered_registry_two_phase_build) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    std::vector<BackendConfig> cfgs;
    cfgs.push_back({"localdata", "localfs",
                    {{"root", (tmp.path / "d").string()}, {"staging", (tmp.path / "s").string()}}});
    cfgs.push_back({"mem", "memory", {}});
    cfgs.push_back({"tier", "tiered",
                    {{"local", "localdata"}, {"cloud", "mem"}, {"scan_interval", "0s"},
                     {"cold_after", "30d"}, {"space_high_watermark", "85%"},
                     {"min_free_bytes", "1GiB"}}});
    auto out = StorageRegistry::build(cfgs, pool);
    CHECK_EQ(out.size(), size_t(3));
    auto tiered = std::dynamic_pointer_cast<TieredBackend>(out.at("tier"));
    CHECK(tiered != nullptr);
    CHECK_EQ(tiered->config().cold_after_sec, int64_t(30) * 86400);
    CHECK(tiered->config().space_high_watermark > 0.84 &&
          tiered->config().space_high_watermark < 0.86);
    sync_wait(tiered->close());

    // 未知引用
    std::vector<BackendConfig> bad1 = {
        {"t", "tiered", {{"local", "nope"}, {"cloud", "mem"}}}};
    bool threw = false;
    try {
        StorageRegistry::build(bad1, pool);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);

    // local 必须为 localfs 系
    std::vector<BackendConfig> bad2 = {
        {"mem", "memory", {}},
        {"mem2", "memory", {}},
        {"t", "tiered", {{"local", "mem"}, {"cloud", "mem2"}}}};
    threw = false;
    try {
        StorageRegistry::build(bad2, pool);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);

    // tiered 互相引用（循环）
    std::vector<BackendConfig> bad3 = {
        {"a", "tiered", {{"local", "b"}, {"cloud", "b"}}},
        {"b", "tiered", {{"local", "a"}, {"cloud", "a"}}}};
    threw = false;
    try {
        StorageRegistry::build(bad3, pool);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
}
