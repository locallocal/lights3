// Tiered storage backend unit tests (docs/tiered-storage.md §10 P1-P4 acceptance):
// consistency suite, tier state machine, overwrite/delete entering GC, scanner cold detection and crash recovery, space fallback.
// The cloud side is played by MemoryBackend (wrapped with counters to assert the number of cloud calls).
#include <sys/xattr.h>

#include <atomic>
#include <cmath>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <thread>

#include "core/thread_pool.h"
#include "storage/localfs/fs_util.h"
#include "storage/memory/memory_backend.h"
#include "storage/bucket_router.h"
#include "storage/registry.h"
#include "storage/tiered/tiered_backend.h"
#ifdef LIGHTS3_DUOSTORE
#include "storage/duostore/duostore_backend.h"
#include "storage/tiered/tier_local_duo.h"
#endif
#include "unit/backend_suite.h"
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

using TmpDir = backend_suite::TmpDir;

// Counting wrapper: asserts when tiered actually touches the cloud; fail_cloud simulates an unreachable cloud (injection for the
// GC backoff / reconciliation failure paths)
class CountingCloud final : public IStorageBackend {
public:
    std::shared_ptr<MemoryBackend> inner = std::make_shared<MemoryBackend>();
    std::atomic<int> puts{0}, gets{0}, heads{0}, deletes{0};
    std::atomic<bool> fail_cloud{false};

    void require_up() const {
        if (fail_cloud)
            throw s3::S3Error(s3::S3ErrorCode::InternalError, "injected: cloud unreachable");
    }

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
                               http::BodyReader& body, PutCondition cond = {}) override {
        ++puts;
        co_return co_await inner->put_object(b, k, std::move(m), body, cond);
    }
    Task<ObjectMeta> head_object(std::string_view b, std::string_view k) override {
        ++heads;
        require_up();
        co_return co_await inner->head_object(b, k);
    }
    Task<void> delete_object(std::string_view b, std::string_view k) override {
        ++deletes;
        require_up();
        co_return co_await inner->delete_object(b, k);
    }
    Task<ListResult> list_objects(std::string_view b, const ListOptions& o) override {
        require_up();
        co_return co_await inner->list_objects(b, o);
    }
    Task<std::string> create_multipart(std::string_view b, std::string_view k,
                                       ObjectMeta m) override {
        co_return co_await inner->create_multipart(b, k, std::move(m));
    }
    using IStorageBackend::upload_part;
    Task<PutResult> upload_part(std::string_view b, std::string_view k, std::string_view id,
                                int no, http::BodyReader& body,
                                const std::optional<PartChecksum>& checksum) override {
        co_return co_await inner->upload_part(b, k, id, no, body, checksum);
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
    Task<ListPartsResult> list_parts(std::string_view b, std::string_view k,
                                     std::string_view id,
                                     const ListPartsOptions& opt) override {
        co_return co_await inner->list_parts(b, k, id, opt);
    }
    Task<ListUploadsResult> list_multipart_uploads(std::string_view b,
                                                   const ListUploadsOptions& opt) override {
        co_return co_await inner->list_multipart_uploads(b, opt);
    }
};

struct Fixture {
    TmpDir tmp;
    std::shared_ptr<ThreadPool> pool = std::make_shared<ThreadPool>(4);
    std::shared_ptr<LocalFsBackend> local;
    std::shared_ptr<CountingCloud> cloud = std::make_shared<CountingCloud>();
    std::shared_ptr<TieredBackend> tiered;

    explicit Fixture(TieredConfig cfg = {}) {
        cfg.scan_interval_sec = 0;  // disable the background periodic task, drive via the manual hook
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

// State machine: local -> remote (demotion) -> cached (GET promotion) -> remote (second cold pass with zero upload)
TEST(tiered_demote_get_cache_cycle) {
    Fixture f;
    std::string data = make_data(200 * 1024);
    sync_wait(f.tiered->create_bucket("bkt"));
    auto pr = put(*f.tiered, "bkt", "dir/cold.bin", data);

    // Demotion: local becomes a 0-length stub, sidecar tier=remote, exactly one copy in the cloud
    sync_wait(f.tiered->demote_object("bkt", "dir/cold.bin"));
    CHECK_EQ(f.disk_size("bkt", "dir/cold.bin"), uint64_t(0));
    CHECK(f.tier_of("bkt", "dir/cold.bin").tier == fsutil::Tier::kRemote);
    CHECK_EQ(f.cloud->puts.load(), 1);

    // HEAD completes fully locally: size/etag are the original values, cloud untouched
    int heads_before = f.cloud->heads.load();
    auto hm = sync_wait(f.tiered->head_object("bkt", "dir/cold.bin"));
    CHECK_EQ(hm.size, uint64_t(data.size()));
    CHECK_EQ(hm.etag, pr.etag);
    CHECK_EQ(f.cloud->heads.load(), heads_before);

    // List recognizes the stub's size
    auto lr = sync_wait(f.tiered->list_objects("bkt", {}));
    CHECK_EQ(lr.objects.size(), size_t(1));
    CHECK_EQ(lr.objects[0].size, uint64_t(data.size()));

    // Transparent read-back: data/ETag identical to before demotion; after EOF the Tee cache commits as cached
    auto got = sync_wait(f.tiered->get_object("bkt", "dir/cold.bin", std::nullopt));
    CHECK_EQ(got.meta.etag, pr.etag);
    CHECK(read_all(*got.body) == data);
    CHECK_EQ(f.cloud->gets.load(), 1);
    CHECK(f.tier_of("bkt", "dir/cold.bin").tier == fsutil::Tier::kCached);
    CHECK_EQ(f.disk_size("bkt", "dir/cold.bin"), uint64_t(data.size()));

    // cached hits locally, cloud no longer touched
    auto again = sync_wait(f.tiered->get_object("bkt", "dir/cold.bin", std::nullopt));
    CHECK(read_all(*again.body) == data);
    CHECK_EQ(f.cloud->gets.load(), 1);

    // cached judged cold again: after verifying the cloud copy, stub directly with zero upload traffic
    sync_wait(f.tiered->demote_object("bkt", "dir/cold.bin"));
    CHECK(f.tier_of("bkt", "dir/cold.bin").tier == fsutil::Tier::kRemote);
    CHECK_EQ(f.disk_size("bkt", "dir/cold.bin"), uint64_t(0));
    CHECK_EQ(f.cloud->puts.load(), 1);
}

// Range GET hitting remote: passed straight through from the cloud, no partial cache is created
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
    CHECK_EQ(mid.meta.size, uint64_t(data.size()));  // the 206 total length comes from local meta
    CHECK(f.tier_of("bkt", "r.bin").tier == fsutil::Tier::kRemote);
    CHECK_EQ(f.disk_size("bkt", "r.bin"), uint64_t(0));

    // Manual whole-object promotion (background promotion after Range takes the same path)
    sync_wait(f.tiered->promote_object("bkt", "r.bin"));
    CHECK(f.tier_of("bkt", "r.bin").tier == fsutil::Tier::kCached);
    auto whole = sync_wait(f.tiered->get_object("bkt", "r.bin", std::nullopt));
    CHECK(read_all(*whole.body) == data);
}

// multipart object (etag with -N): demotion verifies by byte count, ETag stays in -N form after read-back
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
    CHECK(t.remote_etag != cr.etag);  // the cloud etag is recorded separately, never leaks out

    auto got = sync_wait(f.tiered->get_object("bkt", "mp.bin", std::nullopt));
    CHECK_EQ(got.meta.etag, cr.etag);  // externally the ETag is invariant
    CHECK(read_all(*got.body) == p1 + p2);
    CHECK(f.tier_of("bkt", "mp.bin").tier == fsutil::Tier::kCached);
}

// PUT overwriting remote: tier returns to local, the old cloud copy enters GC and is deleted asynchronously
TEST(tiered_overwrite_and_delete_gc) {
    Fixture f;
    sync_wait(f.tiered->create_bucket("bkt"));
    put(*f.tiered, "bkt", "o.bin", make_data(64 * 1024));
    sync_wait(f.tiered->demote_object("bkt", "o.bin"));
    CHECK_EQ(f.gc_entries(), size_t(0));

    // PUT overwrite: written back locally, the cloud copy becomes an orphan
    std::string v2 = make_data(32 * 1024);
    put(*f.tiered, "bkt", "o.bin", v2);
    CHECK(f.tier_of("bkt", "o.bin").tier == fsutil::Tier::kLocal);
    CHECK_EQ(f.gc_entries(), size_t(1));
    sync_wait(f.tiered->run_gc_once());
    CHECK_EQ(f.gc_entries(), size_t(0));
    CHECK_THROWS_S3(sync_wait(f.cloud->inner->get_object("bkt", "o.bin", std::nullopt)),
                    s3::S3ErrorCode::NoSuchKey);  // orphan deleted

    // DELETE remote: local deleted immediately (without waiting for the cloud), the cloud copy is deleted via GC
    sync_wait(f.tiered->demote_object("bkt", "o.bin"));
    sync_wait(f.tiered->delete_object("bkt", "o.bin"));
    CHECK(!fs::exists(f.data_path("bkt", "o.bin")));
    CHECK_EQ(f.gc_entries(), size_t(1));
    sync_wait(f.tiered->run_gc_once());
    CHECK_EQ(f.gc_entries(), size_t(0));
    CHECK_THROWS_S3(sync_wait(f.cloud->inner->get_object("bkt", "o.bin", std::nullopt)),
                    s3::S3ErrorCode::NoSuchKey);
}

// GC never deletes a live copy: after the same key is demoted again, the old expired entry is simply voided
TEST(tiered_gc_never_deletes_live_copy) {
    Fixture f;
    sync_wait(f.tiered->create_bucket("bkt"));
    std::string data = make_data(16 * 1024);
    put(*f.tiered, "bkt", "live.bin", data);
    sync_wait(f.tiered->demote_object("bkt", "live.bin"));
    put(*f.tiered, "bkt", "live.bin", data);  // overwrite (same content) -> old copy enters GC
    CHECK_EQ(f.gc_entries(), size_t(1));
    sync_wait(f.tiered->demote_object("bkt", "live.bin"));  // demote again: the cloud copy is live again

    sync_wait(f.tiered->run_gc_once());
    CHECK_EQ(f.gc_entries(), size_t(0));
    // The live copy was not deleted by mistake, the object is still readable
    auto got = sync_wait(f.tiered->get_object("bkt", "live.bin", std::nullopt));
    CHECK(read_all(*got.body) == data);
}

// GC with the cloud unreachable: entries back off exponentially (attempts/retry_at persisted in the TSV, not reset on restart),
// and resume liquidation once due (todo §3.4; docs/tiered-storage.md §9)
TEST(tiered_gc_retry_exponential_backoff) {
    Fixture f;
    sync_wait(f.tiered->create_bucket("bkt"));
    put(*f.tiered, "bkt", "g.bin", make_data(8 * 1024));
    sync_wait(f.tiered->demote_object("bkt", "g.bin"));
    sync_wait(f.tiered->delete_object("bkt", "g.bin"));  // cloud copy enters GC
    CHECK_EQ(f.gc_entries(), size_t(1));

    f.cloud->fail_cloud = true;
    auto st1 = sync_wait(f.tiered->run_gc_once());
    CHECK_EQ(st1.failed, uint64_t(1));
    CHECK_EQ(f.gc_entries(), size_t(1));  // entry retained

    fs::path entry;
    for (auto& e : fs::directory_iterator(f.tmp.path / "staging/tier/gc")) entry = e.path();
    auto read_entry = [&] {
        std::map<std::string, std::string> kv;
        for (auto& [k, v] : fsutil::read_tsv(entry)) kv[k] = v;
        return kv;
    };
    auto kv1 = read_entry();
    CHECK_EQ(kv1["attempts"], "1");
    int64_t now = ::time(nullptr);
    int64_t ra1 = std::stoll(kv1["retry_at"]);
    CHECK(ra1 - now >= 50 && ra1 - now <= 70);  // base=60s

    // Not due yet: skipped this round, zero cloud accesses
    int heads_before = f.cloud->heads.load();
    auto st2 = sync_wait(f.tiered->run_gc_once());
    CHECK_EQ(st2.deferred, uint64_t(1));
    CHECK_EQ(f.cloud->heads.load(), heads_before);

    // Manually dial the clock past the due point (simulating the backoff window elapsing), still failing -> attempts=2, backoff doubles (~=120s)
    auto rewind = [&] {
        auto kv = read_entry();
        std::ofstream out(entry, std::ios::trunc);
        out << "bucket\t" << kv["bucket"] << "\nkey\t" << kv["key"] << "\netag\t" << kv["etag"]
            << "\nattempts\t" << kv["attempts"] << "\nretry_at\t0\n";
    };
    rewind();
    auto st3 = sync_wait(f.tiered->run_gc_once());
    CHECK_EQ(st3.failed, uint64_t(1));
    auto kv2 = read_entry();
    CHECK_EQ(kv2["attempts"], "2");
    now = ::time(nullptr);
    int64_t ra2 = std::stoll(kv2["retry_at"]);
    CHECK(ra2 - now >= 110 && ra2 - now <= 130);

    // Cloud recovers + due -> liquidation converges
    f.cloud->fail_cloud = false;
    rewind();
    auto st4 = sync_wait(f.tiered->run_gc_once());
    CHECK_EQ(st4.removed_cloud, uint64_t(1));
    CHECK_EQ(st4.resolved, uint64_t(1));
    CHECK_EQ(f.gc_entries(), size_t(0));
}

// Reconciliation forward direction (docs/tiered-storage.md §9): a manually mis-deleted stub is rebuilt from the lights3-* redundant headers;
// foreign objects without the redundant headers are skipped untouched
TEST(tiered_reconcile_rebuilds_lost_stub) {
    Fixture f;
    sync_wait(f.tiered->create_bucket("bkt"));
    ObjectMeta meta;
    meta.content_type = "text/x-test";
    meta.user_meta["color"] = "blue";
    std::string data = make_data(8 * 1024);
    put(*f.tiered, "bkt", "dir/lost.bin", data, meta);
    auto orig = sync_wait(f.tiered->head_object("bkt", "dir/lost.bin"));
    sync_wait(f.tiered->demote_object("bkt", "dir/lost.bin"));

    // Manually mis-delete the local stub + sidecar (the failure row of §9)
    fs::remove(f.data_path("bkt", "dir/lost.bin"));
    fs::remove(fs::path(f.data_path("bkt", "dir/lost.bin").string() + fsutil::kSidecarSuffix));
    // Foreign object: written directly into the cloud bucket, no lights3 redundant headers
    {
        http::StringBodyReader b2("foreign");
        sync_wait(f.cloud->inner->put_object("bkt", "foreign.bin", {}, b2));
    }

    auto st = sync_wait(f.tiered->run_reconcile_once());
    CHECK_EQ(st.cloud_objects, uint64_t(2));
    CHECK_EQ(st.stubs_rebuilt, uint64_t(1));
    CHECK_EQ(st.orphans_skipped, uint64_t(1));  // foreign.bin
    CHECK_EQ(st.orphans_deleted, uint64_t(0));
    CHECK_EQ(st.refs_missing, uint64_t(0));

    // The rebuilt stub: meta fully restored, readable via the cloud
    CHECK(f.tier_of("bkt", "dir/lost.bin").tier == fsutil::Tier::kRemote);
    auto m = sync_wait(f.tiered->head_object("bkt", "dir/lost.bin"));
    CHECK_EQ(m.etag, orig.etag);
    CHECK_EQ(m.size, orig.size);
    CHECK_EQ(m.content_type, "text/x-test");
    CHECK_EQ(m.user_meta.at("color"), "blue");
    auto got = sync_wait(f.tiered->get_object("bkt", "dir/lost.bin", std::nullopt));
    CHECK(read_all(*got.body) == data);
    got.body.reset();
    // The foreign object is preserved as is, and no local object was created
    CHECK_EQ(sync_wait(f.cloud->inner->head_object("bkt", "foreign.bin")).size, uint64_t(7));
    CHECK_THROWS_S3(sync_wait(f.tiered->head_object("bkt", "foreign.bin")),
                    s3::S3ErrorCode::NoSuchKey);
    // Convergence: another round rebuilds nothing
    auto st2 = sync_wait(f.tiered->run_reconcile_once());
    CHECK_EQ(st2.stubs_rebuilt, uint64_t(0));
}

// Reconciliation delete mode + GC pending entries do not resurrect + cleanup of stale cloud copies for local-tier objects (§9)
TEST(tiered_reconcile_delete_mode_and_stale_copy) {
    TieredConfig cfg;
    cfg.reconcile_delete_orphans = true;
    Fixture f(cfg);
    sync_wait(f.tiered->create_bucket("bkt"));
    std::string data = make_data(4 * 1024);
    put(*f.tiered, "bkt", "o.bin", data);
    sync_wait(f.tiered->demote_object("bkt", "o.bin"));
    sync_wait(f.tiered->delete_object("bkt", "o.bin"));  // GC queued, cloud copy still present for now
    CHECK_EQ(f.gc_entries(), size_t(1));

    // A pending GC entry exists -> reconciliation neither rebuilds nor deletes (prevents resurrecting a just-DELETEd object), waits for GC to liquidate
    auto st1 = sync_wait(f.tiered->run_reconcile_once());
    CHECK_EQ(st1.cloud_objects, uint64_t(1));
    CHECK_EQ(st1.stubs_rebuilt + st1.orphans_deleted, uint64_t(0));
    sync_wait(f.tiered->run_gc_once());

    // A pure orphan (no GC entry): in delete mode it is deleted even with lights3 headers
    {
        http::StringBodyReader b2("orphan");
        ObjectMeta om;
        om.user_meta["lights3-etag"] = "deadbeef";
        sync_wait(f.cloud->inner->put_object("bkt", "orphan.bin", std::move(om), b2));
    }
    auto st2 = sync_wait(f.tiered->run_reconcile_once());
    CHECK_EQ(st2.orphans_deleted, uint64_t(1));
    CHECK_THROWS_S3(sync_wait(f.cloud->inner->head_object("bkt", "orphan.bin")),
                    s3::S3ErrorCode::NoSuchKey);

    // A stale cloud copy whose local object is back to local (the GC lost-entry shape): deletion is always safe (local holds the full data)
    put(*f.tiered, "bkt", "s.bin", data);
    sync_wait(f.tiered->demote_object("bkt", "s.bin"));
    put(*f.tiered, "bkt", "s.bin", make_data(2 * 1024));  // overwrite -> old copy enters GC
    for (auto& e : fs::directory_iterator(f.tmp.path / "staging/tier/gc"))
        fs::remove(e.path());  // simulate a lost GC entry
    auto st3 = sync_wait(f.tiered->run_reconcile_once());
    CHECK_EQ(st3.orphans_deleted, uint64_t(1));
    CHECK(f.tier_of("bkt", "s.bin").tier == fsutil::Tier::kLocal);  // local untouched
    auto got = sync_wait(f.tiered->get_object("bkt", "s.bin", std::nullopt));
    CHECK(read_all(*got.body) == make_data(2 * 1024));
}

// Reconciliation reverse direction (§9): local remote, nothing in the cloud -> alarm counter, never delete the stub
TEST(tiered_reconcile_reverse_alarm_keeps_stub) {
    Fixture f;
    sync_wait(f.tiered->create_bucket("bkt"));
    put(*f.tiered, "bkt", "r.bin", make_data(4 * 1024));
    sync_wait(f.tiered->demote_object("bkt", "r.bin"));
    sync_wait(f.cloud->inner->delete_object("bkt", "r.bin"));  // delete the cloud copy bypassing GC

    auto st = sync_wait(f.tiered->run_reconcile_once());
    CHECK_EQ(st.refs_missing, uint64_t(1));
    CHECK(f.tier_of("bkt", "r.bin").tier == fsutil::Tier::kRemote);  // stub retained
    auto lr = sync_wait(f.tiered->list_objects("bkt", {}));
    CHECK_EQ(lr.objects.size(), size_t(1));  // for manual intervention
}

// Reconciliation/backoff config parsing: valid parameters land in config; invalid reconcile_orphans / gc_retry ranges error out
TEST(tiered_reconcile_config_validation) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    auto try_build = [&](std::map<std::string, std::string> extra) {
        std::vector<BackendConfig> cfgs = {
            {"l", "localfs",
             {{"root", (tmp.path / "d").string()}, {"staging", (tmp.path / "s").string()}}},
            {"m", "memory", {}}};
        extra.emplace("local", "l");
        extra.emplace("cloud", "m");
        extra.emplace("scan_interval", "0s");
        cfgs.push_back({"t", "tiered", std::move(extra)});
        return StorageRegistry::build(cfgs, pool);
    };
    auto out = try_build({{"reconcile_interval", "12h"},
                          {"reconcile_orphans", "delete"},
                          {"gc_retry_base", "30s"},
                          {"gc_retry_cap", "10m"}});
    auto t = std::dynamic_pointer_cast<TieredBackend>(out.at("t"));
    CHECK_EQ(t->config().reconcile_interval_sec, int64_t(12 * 3600));
    CHECK(t->config().reconcile_delete_orphans);
    CHECK_EQ(t->config().gc_retry_base_sec, int64_t(30));
    CHECK_EQ(t->config().gc_retry_cap_sec, int64_t(600));
    sync_wait(t->close());
    for (const auto& bad : std::vector<std::map<std::string, std::string>>{
             {{"reconcile_orphans", "maybe"}},
             {{"gc_retry_base", "0s"}},
             {{"gc_retry_base", "2h"}, {"gc_retry_cap", "1h"}}}) {
        bool threw = false;
        try {
            try_build(bad);
        } catch (const std::runtime_error&) {
            threw = true;
        }
        CHECK(threw);
    }
}

// scanner: cold_after=0 demotes everything as cold; crash recovery (remote but data not reclaimed) finishes the stub conversion
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

    // Simulate a crash between §5.2 b/c: after a GET promotes to cached, manually flip the sidecar back to remote
    // (equivalent to the state where the sidecar committed remote but the data file is still full)
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

    sync_wait(f.tiered->scan_once());  // signature "remote but stat size>0" -> finish the reclamation
    CHECK_EQ(f.disk_size("bkt", "a/x.bin"), uint64_t(0));
    CHECK(f.tier_of("bkt", "a/x.bin").tier == fsutil::Tier::kRemote);
    CHECK_EQ(f.cloud->puts.load(), 2);  // recovery does not re-upload

    // Data still readable (cloud is authoritative)
    auto again = sync_wait(f.tiered->get_object("bkt", "a/x.bin", std::nullopt));
    CHECK(read_all(*again.body) == d1);
}

// Space fallback (requirement 3): with insufficient headroom, GET is pure passthrough -- no caching, no failure
TEST(tiered_space_fallback_passthrough) {
    TieredConfig cfg;
    cfg.min_free_bytes = ~uint64_t(0) / 2;  // permanently "out of space"
    Fixture f(cfg);
    std::string data = make_data(80 * 1024);
    sync_wait(f.tiered->create_bucket("bkt"));
    put(*f.tiered, "bkt", "p.bin", data);
    sync_wait(f.tiered->demote_object("bkt", "p.bin"));

    auto got = sync_wait(f.tiered->get_object("bkt", "p.bin", std::nullopt));
    CHECK(read_all(*got.body) == data);  // the read path does not fail because caching failed
    CHECK(f.tier_of("bkt", "p.bin").tier == fsutil::Tier::kRemote);  // not cached
    CHECK_EQ(f.disk_size("bkt", "p.bin"), uint64_t(0));

    // Promotion likewise gives up, but the object stays readable
    sync_wait(f.tiered->promote_object("bkt", "p.bin"));
    CHECK(f.tier_of("bkt", "p.bin").tier == fsutil::Tier::kRemote);
}

// Quota watermark reclamation: cached first (zero upload) then local, stops once below the low watermark
TEST(tiered_quota_watermark_eviction) {
    TieredConfig cfg;
    cfg.cold_after_sec = 1 << 30;      // cold detection never triggers, testing watermarks only
    cfg.quota_bytes = 100 * 1024;      // high watermark 85KiB, low watermark 70KiB
    Fixture f(cfg);
    sync_wait(f.tiered->create_bucket("bkt"));
    std::string a = make_data(40 * 1024), b = make_data(50 * 1024);
    put(*f.tiered, "bkt", "hot.bin", a);   // 40K local
    put(*f.tiered, "bkt", "warm.bin", b);  // 50K local -> 90K total > 85K
    // First turn warm into cached: demote then read back (warm's atime is now the freshest)
    sync_wait(f.tiered->demote_object("bkt", "warm.bin"));
    auto got = sync_wait(f.tiered->get_object("bkt", "warm.bin", std::nullopt));
    CHECK(read_all(*got.body) == b);
    CHECK(f.tier_of("bkt", "warm.bin").tier == fsutil::Tier::kCached);

    int puts_before = f.cloud->puts.load();
    sync_wait(f.tiered->scan_once());
    // 90K->85K exceeded, must reclaim down to 70K: cached warm (50K) is the preferred victim; after reclaiming, 40K passes
    CHECK(f.tier_of("bkt", "warm.bin").tier == fsutil::Tier::kRemote);
    CHECK(f.tier_of("bkt", "hot.bin").tier == fsutil::Tier::kLocal);  // local needs no upload
    CHECK_EQ(f.cloud->puts.load(), puts_before);  // zero upload traffic
}

// Local-tier capacity gauges (backlog-sequence ①): what the space watermark sees,
// exported per backend and readable after the backend is gone
TEST(tiered_local_capacity_gauges) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    auto metrics = std::make_shared<MetricsRegistry>();
    std::vector<BackendConfig> cfgs;
    cfgs.push_back({"localdata", "localfs",
                    {{"root", (tmp.path / "d").string()}, {"staging", (tmp.path / "s").string()}}});
    cfgs.push_back({"mem", "memory", {}});
    cfgs.push_back({"tier", "tiered",
                    {{"local", "localdata"}, {"cloud", "mem"}, {"scan_interval", "0s"},
                     {"cold_after", "30d"}, {"space_high_watermark", "85%"},
                     {"quota_bytes", "64MiB"}}});
    auto out = StorageRegistry::build(cfgs, pool, metrics);
    auto tiered = std::dynamic_pointer_cast<TieredBackend>(out.at("tier"));
    CHECK(tiered != nullptr);

    auto gauge = [&](const char* name) -> double {
        auto text = metrics->render();
        std::string key = std::string(name) + "{backend=\"tier\"} ";
        auto at = text.find(key);
        CHECK(at != std::string::npos);
        return at == std::string::npos ? -1.0 : std::strtod(text.c_str() + at + key.size(), nullptr);
    };
    double used = gauge("lights3_tiered_local_used_bytes");
    double total = gauge("lights3_tiered_local_total_bytes");
    double high = gauge("lights3_tiered_local_high_watermark_bytes");
    CHECK(total > 0);
    CHECK(used > 0 && used <= total);
    CHECK(std::fabs(high - 0.85 * total) <= 1e-6 * total);
    CHECK_EQ(gauge("lights3_tiered_local_quota_bytes"), double(64) * 1024 * 1024);
    // The books estimate starts uncalibrated (rendered as 0); a scan calibrates it to
    // the resident object bytes
    CHECK_EQ(gauge("lights3_tiered_local_cached_bytes"), 0.0);
    sync_wait(tiered->create_bucket("bkt"));
    backend_suite::put(*tiered, "bkt", "k", std::string(1 << 20, 'x'));
    sync_wait(tiered->scan_once());
    CHECK(gauge("lights3_tiered_local_cached_bytes") >= double(1 << 20));
    // The used-bytes probe agrees with the adapter's own view of the same filesystem
    auto s = tiered->local().space_usage();
    CHECK(s.has_value());
    CHECK_EQ(s->used_bytes + s->avail_bytes, s->total_bytes);

    sync_wait(tiered->close());
    tiered.reset();
    out.clear();
    // The registry outlives the backend: the callbacks hold the adapter and the shared
    // estimate, not the backend, so rendering stays valid
    CHECK(gauge("lights3_tiered_local_total_bytes") > 0);
}

// Two-phase registry construction: tiered references leaf backends; cycles/unknown references and an invalid local error out
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

    // Watermark parsing (gaps §3.9): "1%" means 1% -- the old implementation dropped the "%" and then 1.0 skipped the /100,
    // parsing as 100%; (used-low) went negative, wrapped around, and demoted the entire bucket
    std::vector<BackendConfig> pct = cfgs;
    pct[2].params["space_high_watermark"] = "5%";
    pct[2].params["space_low_watermark"] = "1%";
    auto out2 = StorageRegistry::build(pct, pool);
    auto t2 = std::dynamic_pointer_cast<TieredBackend>(out2.at("tier"));
    CHECK(t2->config().space_low_watermark > 0.009 && t2->config().space_low_watermark < 0.011);
    CHECK(t2->config().space_high_watermark > 0.049 &&
          t2->config().space_high_watermark < 0.051);
    sync_wait(t2->close());

    // Out-of-range values error at startup instead of wrapping at runtime
    std::vector<BackendConfig> oob = cfgs;
    oob[2].params["space_high_watermark"] = "150%";
    bool oob_threw = false;
    try {
        StorageRegistry::build(oob, pool);
    } catch (const std::exception&) {
        oob_threw = true;
    }
    CHECK(oob_threw);

    // Unknown reference
    std::vector<BackendConfig> bad1 = {
        {"t", "tiered", {{"local", "nope"}, {"cloud", "mem"}}}};
    bool threw = false;
    try {
        StorageRegistry::build(bad1, pool);
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);

    // local must be of the localfs family
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

    // tiered referencing each other (a cycle)
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

// Per-backend dedicated IO pool (docs/concurrency.md §3.1): the io_threads parameter builds a dedicated pool for that backend
// (a generic key for any type), pool observability metrics carry the backend label; backends without it share the global pool
// (no such metric); invalid values error at configuration time
TEST(registry_per_backend_thread_pool) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    auto metrics = std::make_shared<MetricsRegistry>();
    std::vector<BackendConfig> cfgs;
    cfgs.push_back({"fast", "localfs",
                    {{"root", (tmp.path / "d").string()},
                     {"staging", (tmp.path / "s").string()},
                     {"io_threads", "3"}}});
    cfgs.push_back({"mem", "memory", {}});
    auto out = StorageRegistry::build(cfgs, pool, metrics);
    CHECK_EQ(out.size(), size_t(2));

    // Smoke: the dedicated-pool backend reads and writes normally (all IO goes through the dedicated pool)
    auto& fast = *out.at("fast");
    sync_wait(fast.create_bucket("bkt"));
    backend_suite::put(fast, "bkt", "k", "hello dedicated pool");
    auto got = sync_wait(fast.get_object("bkt", "k", std::nullopt));
    CHECK_EQ(backend_suite::read_all(*got.body), "hello dedicated pool");

    auto text = metrics->render();
    CHECK(text.find("lights3_backend_pool_threads{backend=\"fast\"} 3") != std::string::npos);
    CHECK(text.find("lights3_backend_pool_queue_depth{backend=\"fast\"}") !=
          std::string::npos);
    CHECK(text.find("lights3_backend_pool_completed{backend=\"fast\"}") != std::string::npos);
    // The shared-pool backend has no **pool** metrics (the memory backend's own usage gauge is not among these)
    CHECK(text.find("lights3_backend_pool_threads{backend=\"mem\"}") == std::string::npos);
    CHECK(text.find("lights3_memory_backend_used_bytes{backend=\"mem\"}") != std::string::npos);

    // Invalid io_threads: non-integer / 0 both error at build time (fail fast)
    for (const char* bad : {"0", "many"}) {
        std::vector<BackendConfig> bc = {{"m", "memory", {{"io_threads", bad}}}};
        bool t = false;
        try {
            StorageRegistry::build(bc, pool);
        } catch (const std::runtime_error&) {
            t = true;
        }
        CHECK(t);
    }
}

// Incremental quota maintenance (docs/archive/gaps.md §6.3): PUT accumulates the estimate in place and kicks an early scan round when
// over the watermark -- previously a quota breach between two scans (default 1 hour) was completely invisible
TEST(tiered_quota_incremental_kicks_early_scan) {
    TieredConfig cfg;
    cfg.cold_after_sec = 1 << 30;
    cfg.quota_bytes = 100 * 1024;  // high watermark 85KiB, low watermark 70KiB
    Fixture f(cfg);
    sync_wait(f.tiered->create_bucket("bkt"));
    put(*f.tiered, "bkt", "seed.bin", make_data(10 * 1024));
    sync_wait(f.tiered->scan_once());  // calibrate the estimate ledger (no increments recorded before the first round)

    // Three 30K PUTs push the estimate past 85K; the last PUT should trigger an early scan (in the background)
    // that demotes the cold end below the low watermark
    put(*f.tiered, "bkt", "a.bin", make_data(30 * 1024));
    put(*f.tiered, "bkt", "b.bin", make_data(30 * 1024));
    put(*f.tiered, "bkt", "c.bin", make_data(30 * 1024));
    // The early round is a background coroutine: poll until it brings local usage down (no assertion on who gets demoted --
    // atimes are extremely close, the choice is an implementation detail)
    bool converged = false;
    for (int i = 0; i < 100 && !converged; ++i) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        uint64_t local = 0;
        for (auto& e : fs::recursive_directory_iterator(f.tmp.path / "data")) {
            if (!e.is_regular_file()) continue;
            std::string n = e.path().filename().string();
            if (n == fsutil::kBucketMarker || n.ends_with(fsutil::kSidecarSuffix)) continue;
            local += fs::file_size(e.path());
        }
        converged = local <= 70 * 1024;
    }
    CHECK(converged);
}

// ---------- roadmap §3.6 ----------

namespace {

std::string getx(const fs::path& p, const char* name) {
    char buf[128];
    ssize_t n = ::getxattr(p.c_str(), name, buf, sizeof(buf));
    return n > 0 ? std::string(buf, size_t(n)) : std::string();
}

fsutil::TierInfo tier_of_path(const fs::path& p) {
    fsutil::TierInfo t;
    fsutil::load_object_meta(p, p.filename().string(), &t);
    return t;
}

size_t wheel_files(const Fixture& f) {
    size_t n = 0;
    std::error_code ec;
    for (auto& e : fs::directory_iterator(f.tmp.path / "staging/tier/wheel", ec))
        if (e.is_regular_file()) ++n;
    return n;
}

}  // namespace

// ⑤ Access records leave the process: after a flush the record sits in the data file's
// xattr (atime + hits + wheel slot), and a restarted backend reads it back — the
// coldness verdict no longer depends on a resident table
TEST(tiered_access_record_in_xattr_survives_restart) {
    if (fsutil::probe_meta_xattr(fs::temp_directory_path()) != 0) {
        printf("       [SKIP] temp filesystem has no xattr support\n");
        return;
    }
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto cloud = std::make_shared<CountingCloud>();
    TieredConfig cfg;
    cfg.scan_interval_sec = 0;
    cfg.cold_after_sec = 3600;
    int64_t atime = 0;
    {
        auto local = std::make_shared<LocalFsBackend>(tmp.path / "data", tmp.path / "staging", pool);
        auto t = std::make_shared<TieredBackend>(local, cloud, pool, cfg);
        sync_wait(t->create_bucket("bkt"));
        put(*t, "bkt", "k.bin", make_data(4 * 1024));
        for (int i = 0; i < 3; ++i) read_all(*sync_wait(t->get_object("bkt", "k.bin", std::nullopt)).body);
        sync_wait(t->flush_access());
        std::string rec = getx(tmp.path / "data/bkt/k.bin", "user.lights3.access");
        CHECK(!rec.empty());
        long long a = 0, hits = 0, slot = 0;
        CHECK_EQ(sscanf(rec.c_str(), "%lld %lld %lld", &a, &hits, &slot), 3);
        atime = a;
        CHECK(a >= ::time(nullptr) - 5);
        CHECK_EQ(hits, 4LL);  // PUT + 3 GETs
        CHECK_EQ(slot, (a + 3600) / 3600);  // enrolled at its deadline slot
        sync_wait(t->close());
    }
    // No atime.tsv is written in xattr mode
    CHECK(!fs::exists(tmp.path / "staging/tier/atime.tsv"));
    {
        auto local = std::make_shared<LocalFsBackend>(tmp.path / "data", tmp.path / "staging", pool);
        auto t = std::make_shared<TieredBackend>(local, cloud, pool, cfg);
        // Backdate the file's mtime far into the past: only the xattr record keeps it hot
        fs::last_write_time(tmp.path / "data/bkt/k.bin",
                            fs::file_time_type::clock::now() - std::chrono::hours(24 * 30));
        auto st = sync_wait(t->scan_once());
        CHECK(st.full);
        CHECK_EQ(st.cold_picked, uint64_t(0));
        CHECK(tier_of_path(tmp.path / "data/bkt/k.bin").tier == fsutil::Tier::kLocal);
        sync_wait(t->close());
    }
    (void)atime;
}

// ① Incremental rounds: after the bootstrap full scan, later rounds consume only the
// due time-wheel slots. A key touched after enrollment is re-enrolled further out
// instead of demoted; deleted keys are dropped as stale
TEST(tiered_incremental_scan_consumes_wheel) {
    TieredConfig cfg;
    cfg.cold_after_sec = 1;
    Fixture f(cfg);
    sync_wait(f.tiered->create_bucket("bkt"));
    put(*f.tiered, "bkt", "a.bin", make_data(8 * 1024));
    put(*f.tiered, "bkt", "gone.bin", make_data(8 * 1024));
    auto st1 = sync_wait(f.tiered->scan_once());  // bootstrap: full, nothing cold yet (just written)
    CHECK(st1.full);
    CHECK_EQ(st1.cold_picked, uint64_t(0));
    CHECK(wheel_files(f) >= 1);  // enrolled by the pre-scan access flush (PUT touches)
    sync_wait(f.tiered->delete_object("bkt", "gone.bin"));

    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    auto st2 = sync_wait(f.tiered->scan_once());  // wheel round: a.bin is due and cold
    CHECK(!st2.full);
    CHECK_EQ(st2.cold_picked, uint64_t(1));
    CHECK_EQ(st2.stale, uint64_t(1));  // gone.bin's enrollment
    CHECK(f.tier_of("bkt", "a.bin").tier == fsutil::Tier::kRemote);
    CHECK_EQ(wheel_files(f), size_t(0));  // consumed slots are removed

    // A hot key is pushed out, not demoted: PUT b, touch it after enrollment, then a due round
    put(*f.tiered, "bkt", "b.bin", make_data(8 * 1024));
    sync_wait(f.tiered->flush_access());
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    read_all(*sync_wait(f.tiered->get_object("bkt", "b.bin", std::nullopt)).body);  // fresh touch (pending)
    auto st3 = sync_wait(f.tiered->scan_once());
    CHECK(!st3.full);
    CHECK_EQ(st3.cold_picked, uint64_t(0));
    CHECK(f.tier_of("bkt", "b.bin").tier == fsutil::Tier::kLocal);
    std::this_thread::sleep_for(std::chrono::milliseconds(1200));
    auto st4 = sync_wait(f.tiered->scan_once());  // now genuinely cold
    CHECK_EQ(st4.cold_picked, uint64_t(1));
    CHECK(f.tier_of("bkt", "b.bin").tier == fsutil::Tier::kRemote);
}

// ② Prefix rules: per-prefix cold_after, "never" pins (neither cold demotion nor
// watermark eviction touches pinned objects); first match wins
TEST(tiered_prefix_rules_and_pinning) {
    TieredConfig cfg;
    cfg.cold_after_sec = 1 << 30;
    cfg.rules = {{"bkt/pin/*", -1}, {"bkt/fast/*", 0}, {"other/*", 0}};
    cfg.quota_bytes = 100 * 1024;  // 85K high / 70K low
    Fixture f(cfg);
    sync_wait(f.tiered->create_bucket("bkt"));
    put(*f.tiered, "bkt", "pin/a.bin", make_data(60 * 1024));
    put(*f.tiered, "bkt", "fast/b.bin", make_data(10 * 1024));
    put(*f.tiered, "bkt", "slow/c.bin", make_data(30 * 1024));
    CHECK_EQ(f.tiered->cold_after_for("bkt", "pin/x"), int64_t(-1));
    CHECK_EQ(f.tiered->cold_after_for("bkt", "fast/x"), int64_t(0));
    CHECK_EQ(f.tiered->cold_after_for("bkt", "slow/x"), int64_t(1 << 30));

    auto st = sync_wait(f.tiered->scan_once());
    CHECK(f.tier_of("bkt", "fast/b.bin").tier == fsutil::Tier::kRemote);  // per-prefix threshold
    CHECK(f.tier_of("bkt", "pin/a.bin").tier == fsutil::Tier::kLocal);    // pinned
    // Quota: 100K local > 85K; after fast/b (10K) 90K remain > 70K → the only evictable
    // candidate is slow/c (30K); pin/a must stay even though it is the biggest and coldest
    CHECK(f.tier_of("bkt", "slow/c.bin").tier == fsutil::Tier::kRemote);
    CHECK(f.tier_of("bkt", "pin/a.bin").tier == fsutil::Tier::kLocal);
    CHECK(st.evicted >= 1);
}

// ③ Eviction score: with frequency weighting a frequently read object survives a
// colder-by-age but rarely read one; size weighting prefers big victims
TEST(tiered_evict_score_weights) {
    auto run = [&](double size_w, double freq_w, int hot_gets, const char* expect_evicted) {
        TieredConfig cfg;
        cfg.cold_after_sec = 1 << 30;
        cfg.quota_bytes = 100 * 1024;  // need 20K once 90K are local
        cfg.evict_size_weight = size_w;
        cfg.evict_frequency_weight = freq_w;
        Fixture f(cfg);
        sync_wait(f.tiered->create_bucket("bkt"));
        put(*f.tiered, "bkt", "a-big.bin", make_data(60 * 1024));
        std::this_thread::sleep_for(std::chrono::milliseconds(1100));  // a-big is older
        put(*f.tiered, "bkt", "b-small.bin", make_data(30 * 1024));
        for (int i = 0; i < hot_gets; ++i)
            read_all(*sync_wait(f.tiered->get_object("bkt", "a-big.bin", std::nullopt)).body);
        sync_wait(f.tiered->scan_once());
        bool big_remote = f.tier_of("bkt", "a-big.bin").tier == fsutil::Tier::kRemote;
        bool small_remote = f.tier_of("bkt", "b-small.bin").tier == fsutil::Tier::kRemote;
        CHECK(big_remote != small_remote);  // exactly one victim covers the 20K deficit
        CHECK_EQ(big_remote ? "a-big" : "b-small", std::string(expect_evicted));
    };
    run(0.0, 0.0, 0, "a-big");     // pure LRU: the older object goes
    run(0.0, 1.0, 20, "b-small");  // a-big read 20×: frequency keeps it, the younger small one goes
    run(1.0, 0.0, 0, "a-big");     // size weight only reinforces the LRU pick here
}

// ⑦ Range cache: a Range GET on a remote object fills block-aligned blocks into the
// sparse cache; later ranges inside those blocks are served locally, ranges outside
// fetch again; a PUT drops the cache
TEST(tiered_range_cache_blocks) {
    TieredConfig cfg;
    cfg.cache_fill_on_range = false;
    cfg.range_cache = true;
    cfg.range_cache_block = 64 * 1024;
    Fixture f(cfg);
    std::string data = make_data(300 * 1024);
    sync_wait(f.tiered->create_bucket("bkt"));
    put(*f.tiered, "bkt", "r.bin", data);
    sync_wait(f.tiered->demote_object("bkt", "r.bin"));

    auto get_range = [&](uint64_t a, uint64_t b) {
        auto os = sync_wait(f.tiered->get_object("bkt", "r.bin", ByteRange{a, b}));
        CHECK(os.range.has_value());
        CHECK_EQ(os.meta.size, uint64_t(data.size()));
        return read_all(*os.body);
    };
    int gets0 = f.cloud->gets.load();
    CHECK(get_range(70000, 149999) == data.substr(70000, 80000));  // blocks 1..2 fetched
    CHECK_EQ(f.cloud->gets.load(), gets0 + 1);
    fs::path rc = f.tmp.path / "staging/tier/rcache/data/bkt/r.bin";
    CHECK(fs::exists(rc));
    CHECK_EQ(fs::file_size(rc), uint64_t(data.size()));  // sparse container of the object's size
    CHECK(fs::exists(f.tmp.path / "staging/tier/rcache/map/bkt/r.bin"));
    CHECK(get_range(70000, 149999) == data.substr(70000, 80000));  // hit
    CHECK(get_range(100000, 120000) == data.substr(100000, 20001));  // inside cached blocks: hit
    CHECK(get_range(65536, 196607) == data.substr(65536, 131072));   // exactly the two blocks: hit
    CHECK_EQ(f.cloud->gets.load(), gets0 + 1);
    CHECK(get_range(0, 9) == data.substr(0, 10));  // block 0: fill
    CHECK_EQ(f.cloud->gets.load(), gets0 + 2);
    CHECK(get_range(0, 9) == data.substr(0, 10));
    CHECK(get_range(299000, 307199) == data.substr(299000, 8200));  // tail block (short last block)
    CHECK_EQ(f.cloud->gets.load(), gets0 + 3);
    CHECK(get_range(299000, 307199) == data.substr(299000, 8200));
    CHECK_EQ(f.cloud->gets.load(), gets0 + 3);
    CHECK(f.tier_of("bkt", "r.bin").tier == fsutil::Tier::kRemote);  // still a stub
    CHECK_EQ(f.disk_size("bkt", "r.bin"), uint64_t(0));

    // Whole-object promotion replaces the partial cache
    sync_wait(f.tiered->promote_object("bkt", "r.bin"));
    CHECK(f.tier_of("bkt", "r.bin").tier == fsutil::Tier::kCached);
    CHECK(!fs::exists(rc));
    // Demote again, fill, then a PUT overwrite drops the (now stale) cache
    sync_wait(f.tiered->demote_object("bkt", "r.bin"));
    CHECK(get_range(0, 9) == data.substr(0, 10));
    CHECK(fs::exists(rc));
    put(*f.tiered, "bkt", "r.bin", make_data(1024));
    CHECK(!fs::exists(rc));
}

// ④ Quarantine ledger: a refs_missing finding alerts once, later rounds only bump the
// count; it resolves by itself when the copy comes back, `forget` drops it, `purge`
// deletes the dead stub after re-verification; foreign cloud objects are quarantined too
TEST(tiered_quarantine_ledger) {
    Fixture f;
    sync_wait(f.tiered->create_bucket("bkt"));
    std::string data = make_data(4 * 1024);
    put(*f.tiered, "bkt", "r.bin", data);
    sync_wait(f.tiered->demote_object("bkt", "r.bin"));
    sync_wait(f.cloud->inner->delete_object("bkt", "r.bin"));  // copy lost behind GC's back

    auto st1 = sync_wait(f.tiered->run_reconcile_once());
    CHECK_EQ(st1.refs_missing, uint64_t(1));
    CHECK_EQ(st1.quarantined_new, uint64_t(1));
    auto q1 = f.tiered->quarantine_list();
    CHECK_EQ(q1.size(), size_t(1));
    CHECK_EQ(q1[0].kind, std::string("refs_missing"));
    CHECK_EQ(q1[0].key, std::string("r.bin"));
    CHECK_EQ(q1[0].count, uint64_t(1));
    auto st2 = sync_wait(f.tiered->run_reconcile_once());
    CHECK_EQ(st2.refs_missing, uint64_t(1));
    CHECK_EQ(st2.quarantined_new, uint64_t(0));  // already known: no re-alert
    CHECK_EQ(f.tiered->quarantine_list()[0].count, uint64_t(2));

    // The copy comes back (same bytes → same etag): the finding resolves on the next round
    {
        http::StringBodyReader b(data);
        sync_wait(f.cloud->inner->put_object("bkt", "r.bin", {}, b));
    }
    auto st3 = sync_wait(f.tiered->run_reconcile_once());
    CHECK_EQ(st3.refs_missing, uint64_t(0));
    CHECK_EQ(st3.quarantined_resolved, uint64_t(1));
    CHECK(f.tiered->quarantine_list().empty());

    // Lost again → purge deletes the dead stub; a present copy makes purge refuse
    sync_wait(f.cloud->inner->delete_object("bkt", "r.bin"));
    sync_wait(f.tiered->run_reconcile_once());
    CHECK_EQ(f.tiered->quarantine_list().size(), size_t(1));
    CHECK(!sync_wait(f.tiered->quarantine_purge("bkt", "nope.bin")));  // not quarantined
    CHECK(sync_wait(f.tiered->quarantine_purge("bkt", "r.bin")));
    CHECK_THROWS_S3(sync_wait(f.tiered->head_object("bkt", "r.bin")), s3::S3ErrorCode::NoSuchKey);
    CHECK(f.tiered->quarantine_list().empty());

    // Foreign object: quarantined as `foreign`, forget drops it, next round re-adds it
    {
        http::StringBodyReader b("foreign");
        sync_wait(f.cloud->inner->put_object("bkt", "foreign.bin", {}, b));
    }
    auto st4 = sync_wait(f.tiered->run_reconcile_once());
    CHECK_EQ(st4.orphans_skipped, uint64_t(1));
    CHECK_EQ(st4.quarantined_new, uint64_t(1));
    CHECK_EQ(f.tiered->quarantine_list()[0].kind, std::string("foreign"));
    CHECK(f.tiered->quarantine_forget("bkt", "foreign.bin"));
    CHECK(f.tiered->quarantine_list().empty());
    CHECK(!f.tiered->quarantine_forget("bkt", "foreign.bin"));
    auto st5 = sync_wait(f.tiered->run_reconcile_once());
    CHECK_EQ(st5.quarantined_new, uint64_t(1));
}

// rules parsing through the registry (config.cc flattens `rules:` into rules.N.*)
TEST(tiered_rules_config_parsing) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    std::vector<BackendConfig> cfgs = {
        {"l", "localfs", {{"root", (tmp.path / "d").string()}, {"staging", (tmp.path / "s").string()}}},
        {"m", "memory", {}},
        {"t", "tiered",
         {{"local", "l"}, {"cloud", "m"}, {"scan_interval", "0s"},
          {"rules.0.match", "arch-*/raw/*"}, {"rules.0.cold_after", "7d"},
          {"rules.1.match", "arch-*/keep/*"}, {"rules.1.cold_after", "never"},
          {"full_scan_interval", "12h"}, {"evict_frequency_weight", "0.5"},
          {"range_cache", "true"}, {"range_cache_block", "256KiB"}}}};
    auto out = StorageRegistry::build(cfgs, pool);
    auto t = std::dynamic_pointer_cast<TieredBackend>(out.at("t"));
    CHECK_EQ(t->config().rules.size(), size_t(2));
    CHECK_EQ(t->cold_after_for("arch-1", "raw/x"), int64_t(7 * 86400));
    CHECK_EQ(t->cold_after_for("arch-1", "keep/x"), int64_t(-1));
    CHECK_EQ(t->cold_after_for("arch-1", "other/x"), t->config().cold_after_sec);
    CHECK_EQ(t->config().full_scan_interval_sec, int64_t(12 * 3600));
    CHECK(t->config().range_cache);
    CHECK_EQ(t->config().range_cache_block, uint64_t(256 * 1024));
    sync_wait(t->close());
    for (const auto& bad : std::vector<std::map<std::string, std::string>>{
             {{"rules.0.match", "x/*"}},                     // missing cold_after
             {{"evict_size_weight", "-1"}},
             {{"range_cache_block", "1KiB"}}}) {
        std::vector<BackendConfig> c2 = cfgs;
        c2[2].params = {{"local", "l"}, {"cloud", "m"}, {"scan_interval", "0s"}};
        for (auto& [k, v] : bad) c2[2].params[k] = v;
        bool threw = false;
        try {
            StorageRegistry::build(c2, pool);
        } catch (const std::runtime_error&) {
            threw = true;
        }
        CHECK(threw);
    }
}

#ifdef LIGHTS3_DUOSTORE
// ⑥ duostore as the local side: tier state lives in the object record, a stub is a
// record without extents (the old extents go to duostore's gcq), read-back caches into
// new extents; scan/GC/reconcile run unchanged over the ITierLocal interface
TEST(tiered_duostore_local_side) {
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    DuoStoreConfig dc;
    dc.name = "duo-local";
    dc.root = tmp.path / "duo";
    dc.meta_path = dc.root / "meta";
    dc.gc_interval_sec = 0;
    dc.orphan_scan_interval_sec = 0;
    fs::create_directories(dc.meta_path);
    auto duo = std::make_shared<DuoStoreBackend>(dc, pool);
    auto cloud = std::make_shared<CountingCloud>();
    TieredConfig cfg;
    cfg.scan_interval_sec = 0;
    cfg.cold_after_sec = 1 << 30;
    auto t = std::make_shared<TieredBackend>(std::make_shared<tier::DuoStoreTierLocal>(duo),
                                             cloud, pool, cfg);
    CHECK_EQ(std::string(t->local().kind()), std::string("duostore"));
    sync_wait(t->create_bucket("bkt"));
    std::string data = make_data(200 * 1024);
    auto pr = put(*t, "bkt", "dir/cold.bin", data);

    sync_wait(t->demote_object("bkt", "dir/cold.bin"));
    auto rec = duo->tier_read("bkt", "dir/cold.bin");
    CHECK(rec.has_value());
    CHECK_EQ(int(rec->tier.tier), int(duostore::TierState::kRemote));
    CHECK(rec->data.extents.empty());          // local data released to the gcq
    CHECK_EQ(rec->meta.size, uint64_t(data.size()));  // logical size kept
    CHECK_EQ(cloud->puts.load(), 1);
    // HEAD/list local, GET goes to the cloud and caches back into fresh extents
    CHECK_EQ(sync_wait(t->head_object("bkt", "dir/cold.bin")).etag, pr.etag);
    CHECK_EQ(sync_wait(t->list_objects("bkt", {})).objects[0].size, uint64_t(data.size()));
    // Direct GET on the stub through duostore itself is refused loudly, not served empty
    CHECK_THROWS_S3(sync_wait(duo->get_object("bkt", "dir/cold.bin", std::nullopt)),
                    s3::S3ErrorCode::InternalError);
    auto got = sync_wait(t->get_object("bkt", "dir/cold.bin", std::nullopt));
    CHECK_EQ(got.meta.etag, pr.etag);
    CHECK(read_all(*got.body) == data);
    CHECK_EQ(cloud->gets.load(), 1);
    rec = duo->tier_read("bkt", "dir/cold.bin");
    CHECK_EQ(int(rec->tier.tier), int(duostore::TierState::kCached));
    CHECK(!rec->data.extents.empty());
    CHECK(read_all(*sync_wait(t->get_object("bkt", "dir/cold.bin", std::nullopt)).body) == data);
    CHECK_EQ(cloud->gets.load(), 1);  // served from the cached extents
    // cached → remote again with zero upload
    sync_wait(t->demote_object("bkt", "dir/cold.bin"));
    CHECK_EQ(cloud->puts.load(), 1);
    CHECK_EQ(int(duo->tier_read("bkt", "dir/cold.bin")->tier.tier), int(duostore::TierState::kRemote));

    // Scan (full bootstrap) over the meta-driven walker + coldness with a 0 threshold
    put(*t, "bkt", "hot.bin", make_data(8 * 1024));
    auto st = sync_wait(t->scan_once());
    CHECK(st.full);
    CHECK_EQ(st.walked, uint64_t(2));
    CHECK_EQ(st.cold_picked, uint64_t(0));
    // Overwrite + delete route the old cloud copies through GC
    put(*t, "bkt", "dir/cold.bin", make_data(1024));
    CHECK_EQ(int(duo->tier_read("bkt", "dir/cold.bin")->tier.tier), int(duostore::TierState::kLocal));
    auto gs = sync_wait(t->run_gc_once());
    CHECK_EQ(gs.removed_cloud, uint64_t(1));
    // Reconcile is a no-op on a consistent pair
    auto rs = sync_wait(t->run_reconcile_once());
    CHECK_EQ(rs.stubs_rebuilt + rs.orphans_deleted + rs.refs_missing, uint64_t(0));
    // duostore's own GC reclaims the released extents without complaint
    sync_wait(duo->run_gc_once());
    sync_wait(t->close());
}
#endif

// bucket_router build-time validation (docs/archive/gaps.md §6.3): bad globs / unreachable rules error at startup,
// negation rules take effect
TEST(bucket_router_validation_and_negation) {
    auto mem1 = std::make_shared<MemoryBackend>();
    auto mem2 = std::make_shared<MemoryBackend>();
    std::map<std::string, std::shared_ptr<IStorageBackend>> backends{{"m1", mem1}, {"m2", mem2}};

    auto build = [&](std::vector<BucketRule> rules) {
        BucketsConfig cfg;
        cfg.default_backend = "m1";
        cfg.rules = std::move(rules);
        return BucketRouter::build(cfg, backends);
    };
    auto throws = [&](std::vector<BucketRule> rules) {
        try {
            build(std::move(rules));
        } catch (const std::runtime_error&) {
            return true;
        }
        return false;
    };

    // Unclosed character class; literal characters impossible in bucket names (uppercase/underscore) -- rules that silently never match
    CHECK(throws({{"log[a-z", "m2"}}));
    CHECK(throws({{"Logs-*", "m2"}}));
    CHECK(throws({{"my_bucket", "m2"}}));
    // Unreachable: rules after a catch-all; duplicate rules
    CHECK(throws({{"*", "m2"}, {"logs-*", "m1"}}));
    CHECK(throws({{"logs-*", "m2"}, {"logs-*", "m1"}}));

    // Normal routing + negation rule: "!logs-*" = everything except logs-*
    auto r = build({{"logs-*", "m2"}});
    CHECK(&r.resolve("logs-app") == mem2.get());
    CHECK(&r.resolve("data") == mem1.get());
    auto rn = build({{"!logs-*", "m2"}});
    CHECK(&rn.resolve("data") == mem2.get());
    CHECK(&rn.resolve("logs-app") == mem1.get());
    // A negated fixed string is itself a catch-all; rules after it are unreachable
    CHECK(throws({{"!onlyone", "m2"}, {"other-*", "m1"}}));
}
