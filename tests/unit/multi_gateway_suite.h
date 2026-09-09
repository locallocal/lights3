// Multi-gateway multipart suite (docs/archive/multi-gateway-multipart-design.md §4 ②):
// two DuoStoreBackend instances ("gateways" A and B) over ONE shared meta target and
// ONE shared data engine object, parameterized over the shared meta engines
// (RedisMetaStore / TikvMetaStore — each engine file wires its own factory and SKIPs
// without an external instance). The data plane is a single FsDataStore shared at
// object level: what a rados data plane gives every gateway (P3/P4) without the
// flock problem of a shared fs root, enough to exercise the backend orchestration
// layer (P1/P2/P5/P6) that the design audits.
//
// Factory convention: every call opens a NEW connection to the SAME shared meta
// (same key prefix); the suite closes what it opens.
#pragma once

#include <unistd.h>

#include <algorithm>
#include <chrono>
#include <cstring>
#include <filesystem>
#include <functional>
#include <memory>
#include <optional>
#include <semaphore>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "core/thread_pool.h"
#include "http/model.h"
#include "storage/duostore/data_store.h"
#include "storage/duostore/duostore_backend.h"
#include "storage/duostore/fs_data_store.h"
#include "storage/duostore/meta_store.h"
#include "storage/multipart.h"
#include "unit/backend_suite.h"
#include "unit/mini_test.h"

namespace multi_gateway_suite {

namespace fs = std::filesystem;
using namespace lights3;
using namespace lights3::storage;
using namespace lights3::storage::duostore;
using backend_suite::read_all;
using backend_suite::TmpDir;

using MetaFactory = std::function<std::unique_ptr<IMetaStore>()>;

// Forwarding IDataStore over one shared engine object. DuoStoreBackend owns its
// data store by unique_ptr, so sharing one engine between two backends needs a
// thin proxy; close() is a no-op here — the suite closes the engine once, after
// both gateways are down (a gateway restart must not tear down its peer's data plane)
struct SharedDataStore final : IDataStore {
    std::shared_ptr<IDataStore> inner;
    explicit SharedDataStore(std::shared_ptr<IDataStore> d) : inner(std::move(d)) {}

    Task<std::unique_ptr<DataWriter>> open_writer(WriteHint hint) override {
        return inner->open_writer(std::move(hint));
    }
    Task<std::vector<DataRef>> write_batch(std::span<const PackAppendItem> items) override {
        return inner->write_batch(items);
    }
    Task<std::unique_ptr<http::BodyReader>> open_reader(DataRef ref, uint64_t first,
                                                       uint64_t last) override {
        return inner->open_reader(std::move(ref), first, last);
    }
    Task<void> remove(std::span<const Extent> extents) override { return inner->remove(extents); }
    Task<void> remove_pack(uint64_t pack_id) override { return inner->remove_pack(pack_id); }
    Task<GcRewrite> rewrite_pack(uint64_t pack_id) override { return inner->rewrite_pack(pack_id); }
    Task<uint64_t> seal_aged_packs(int64_t max_age_ms) override {
        return inner->seal_aged_packs(max_age_ms);
    }
    Task<void> scan_chunks(
        const std::function<void(uint64_t, int64_t, uint64_t)>& cb) override {
        return inner->scan_chunks(cb);
    }
    Task<void> scan_packs(const std::function<void(uint64_t, int64_t, uint64_t)>& cb) override {
        return inner->scan_packs(cb);
    }
    bool pack_write_locked(uint64_t pack_id) override { return inner->pack_write_locked(pack_id); }
    uint64_t stat_pack(uint64_t pack_id) override { return inner->stat_pack(pack_id); }
    Task<void> close() override { co_return; }
};

// Gateable body: emits `first`, then blocks until release() before emitting `rest`
// — a long "bytes landed, meta not yet committed" window in which the peer acts
class GatedReader final : public http::BodyReader {
public:
    GatedReader(std::string first, std::string rest)
        : first_(std::move(first)), rest_(std::move(rest)),
          total_(first_.size() + rest_.size()) {}

    std::optional<uint64_t> length() const override { return total_; }

    Task<size_t> read(std::span<std::byte> buf) override {
        if (stage_ == 0) {
            size_t n = std::min(buf.size(), first_.size() - off_);
            std::memcpy(buf.data(), first_.data() + off_, n);
            off_ += n;
            if (off_ == first_.size()) {
                stage_ = 1;
                off_ = 0;
            }
            co_return n;
        }
        if (stage_ == 1) {
            gate_.acquire();  // parks a pool thread; the pool has spare threads for the peer
            stage_ = 2;
        }
        size_t n = std::min(buf.size(), rest_.size() - off_);
        std::memcpy(buf.data(), rest_.data() + off_, n);
        off_ += n;
        co_return n;
    }

    void release() { gate_.release(); }

private:
    std::string first_, rest_;
    uint64_t total_;
    size_t off_ = 0;
    int stage_ = 0;
    std::binary_semaphore gate_{0};
};

inline std::string pattern_bytes(size_t n, char base = 'a') {
    std::string s(n, '\0');
    for (size_t i = 0; i < n; ++i) s[i] = char(base + i % 26);
    return s;
}

inline size_t count_chunk_files(const fs::path& root) {
    size_t n = 0;
    std::error_code ec;
    fs::recursive_directory_iterator it(root / "chunks", ec), end;
    for (; !ec && it != end; it.increment(ec))
        if (it->is_regular_file() && it->path().extension() == ".chk") ++n;
    return n;
}

inline PutResult upload(DuoStoreBackend& gw, const std::string& bkt, const std::string& key,
                        const std::string& id, int part_no, const std::string& data) {
    http::StringBodyReader body(data);
    return sync_wait(gw.upload_part(bkt, key, id, part_no, body));
}

inline PartInfo part_info(int part_no, const std::string& etag) {
    PartInfo p;
    p.part_no = part_no;
    p.etag = etag;
    return p;
}

inline std::vector<PartMeta> parts_of(DuoStoreBackend& gw, const std::string& bkt,
                                      const std::string& key, const std::string& id) {
    return sync_wait(gw.list_parts(bkt, key, id, {})).parts;
}

inline std::vector<UploadInfo> uploads_of(DuoStoreBackend& gw, const std::string& bkt) {
    return sync_wait(gw.list_multipart_uploads(bkt, {})).uploads;
}

// Two gateways over one meta target + one data engine. 4 KiB chunks force multi-chunk
// parts; packs disabled (rados has no pack entity, and the chunk path is what the
// object-level sharing models); gc_grace 0 and manual hooks only, so every reclaim
// decision in the suite is driven by refs / pins / leases rather than by time;
// read_lease on (the multi-gateway deployment requirement, §4 ③), object cache off
// (the shared-engine default of from_params)
struct Cluster {
    TmpDir tmp;
    std::shared_ptr<ThreadPool> pool = std::make_shared<ThreadPool>(6);
    DuoStoreConfig cfg;
    std::unique_ptr<IMetaStore> alloc_meta;  // file-id allocation for the shared data plane
    std::shared_ptr<FsDataStore> data;       // the ONE data engine both gateways share
    std::shared_ptr<DuoStoreBackend> a, b;

    // Both gateways publish their leases right before the peer consumes them —
    // the timer publishes too, but on its own cadence; the manual hook makes the
    // floors deterministic (idle gateway → floor = now). The floors are unix-ms and
    // inclusive (an entry enqueued in the same ms as the floor is deferred), so let
    // a few ms pass after the last business op before publishing
    void publish_leases() {
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
        CHECK(sync_wait(a->publish_lease_once()));
        CHECK(sync_wait(b->publish_lease_once()));
    }

    void close() {
        if (a) sync_wait(a->close());
        if (b) sync_wait(b->close());
        a.reset();
        b.reset();
        if (data) sync_wait(data->close());
        data.reset();
        if (alloc_meta) alloc_meta->close();
        alloc_meta.reset();
    }
    ~Cluster() { close(); }
};

inline std::shared_ptr<DuoStoreBackend> make_gateway(Cluster& c, const MetaFactory& make_meta,
                                                     const char* name) {
    auto cfg = c.cfg;
    cfg.name = name;
    return std::make_shared<DuoStoreBackend>(cfg, c.pool, make_meta(),
                                             std::make_unique<SharedDataStore>(c.data));
}

inline std::unique_ptr<Cluster> make_cluster(const MetaFactory& make_meta, DuoMetaKind kind,
                                             const std::function<void(DuoStoreConfig&)>& tweak = {}) {
    auto c = std::make_unique<Cluster>();
    c->cfg.root = c->tmp.path / "duo";
    c->cfg.meta_kind = kind;
    c->cfg.chunk_size = 4096;
    c->cfg.pack_threshold = 0;
    c->cfg.meta_sync = false;
    c->cfg.gc_interval_sec = 0;
    c->cfg.orphan_scan_interval_sec = 0;
    c->cfg.gc_grace_sec = 0;
    c->cfg.read_lease_sec = 1;
    c->cfg.meta_cache_entries = 0;
    if (tweak) tweak(c->cfg);
    fs::create_directories(c->cfg.root);
    c->alloc_meta = make_meta();
    IMetaStore* mp = c->alloc_meta.get();
    FsDataOptions fopt;
    fopt.root = c->cfg.root;
    fopt.chunk_size = c->cfg.chunk_size;
    fopt.verify_chunk_crc = c->cfg.verify_chunk_crc;
    fopt.pack_threshold = c->cfg.pack_threshold;
    c->data = std::make_shared<FsDataStore>(
        std::move(fopt), c->pool,
        [mp](Extent::Kind k, uint32_t n) { return mp->alloc_file_run(k, n); },
        [mp](uint64_t id, uint64_t sz) { mp->seal_pack(id, sz); });
    c->a = make_gateway(*c, make_meta, "gw-a");
    c->b = make_gateway(*c, make_meta, "gw-b");
    return c;
}

// Wait until the first chunk of an in-flight write is on the shared data plane
inline void wait_chunks_at_least(const fs::path& root, size_t n) {
    for (int i = 0; i < 250 && count_chunk_files(root) < n; ++i) usleep(20 * 1000);
    CHECK(count_chunk_files(root) >= n);
}

// ① A create → B upload_part ×2 → A complete → B get: the ETag is the S3 combined
// ETag over the parts B recorded, the body is byte-identical through B's data plane
inline void cross_gateway_multipart(const MetaFactory& make_meta, DuoMetaKind kind) {
    auto c = make_cluster(make_meta, kind);
    sync_wait(c->a->create_bucket("bkt"));
    const std::string p1 = pattern_bytes(6000, 'a'), p2 = pattern_bytes(5000, 'n');
    auto id = sync_wait(c->a->create_multipart("bkt", "obj", {}));
    auto e1 = upload(*c->b, "bkt", "obj", id, 1, p1).etag;
    auto e2 = upload(*c->b, "bkt", "obj", id, 2, p2).etag;
    CHECK_EQ(parts_of(*c->a, "bkt", "obj", id).size(), size_t(2));  // B's parts visible from A
    std::vector<PartInfo> parts{part_info(1, e1), part_info(2, e2)};
    auto done = sync_wait(c->a->complete_multipart("bkt", "obj", id, parts));
    CHECK_EQ(done.etag, combined_etag({e1, e2}));
    CHECK(uploads_of(*c->b, "bkt").empty());  // complete on A retired the upload for B too
    auto h = sync_wait(c->b->head_object("bkt", "obj"));
    CHECK_EQ(h.etag, done.etag);
    CHECK_EQ(h.size, uint64_t(p1.size() + p2.size()));
    {
        auto g = sync_wait(c->b->get_object("bkt", "obj", std::nullopt));
        CHECK_EQ(read_all(*g.body), p1 + p2);
    }
    {
        auto g = sync_wait(c->a->get_object("bkt", "obj", std::nullopt));
        CHECK_EQ(read_all(*g.body), p1 + p2);
    }
    // No stray data: parts became the object's extents, nothing to reclaim
    c->publish_leases();
    auto st = sync_wait(c->a->run_orphan_scan_once());
    CHECK_EQ(st.orphans_removed, uint64_t(0));
    CHECK_EQ(st.refs_missing, uint64_t(0));
    CHECK_EQ(count_chunk_files(c->cfg.root), size_t(4));  // 2 + 2 chunks at 4 KiB
    sync_wait(c->a->delete_object("bkt", "obj"));
    sync_wait(c->b->delete_bucket("bkt"));
    c->close();
}

// ② A upload_part still pumping → B abort: A's put_part surfaces NoSuchUpload, the
// chunks A already landed are discarded by commit_or_discard, and the peer's orphan
// scan finds nothing left to reclaim
inline void abort_while_peer_pumps(const MetaFactory& make_meta, DuoMetaKind kind) {
    auto c = make_cluster(make_meta, kind);
    sync_wait(c->a->create_bucket("bkt"));
    auto id = sync_wait(c->b->create_multipart("bkt", "obj", {}));
    const std::string data = pattern_bytes(9000);
    GatedReader body(data.substr(0, 5000), data.substr(5000));
    std::exception_ptr err;
    std::thread writer([&] {
        try {
            sync_wait(c->a->upload_part("bkt", "obj", id, 1, body));
        } catch (...) {
            err = std::current_exception();
        }
    });
    wait_chunks_at_least(c->cfg.root, 1);  // A's first chunk is on the shared plane
    sync_wait(c->b->abort_multipart("bkt", "obj", id));
    body.release();
    writer.join();
    CHECK(bool(err));
    CHECK_THROWS_S3(std::rethrow_exception(err), s3::S3ErrorCode::NoSuchUpload);
    CHECK_EQ(count_chunk_files(c->cfg.root), size_t(0));  // discarded by A itself
    CHECK(uploads_of(*c->a, "bkt").empty());
    c->publish_leases();
    auto st = sync_wait(c->b->run_orphan_scan_once());
    CHECK_EQ(st.chunks_scanned, uint64_t(0));
    CHECK_EQ(st.orphans_removed, uint64_t(0));
    CHECK_EQ(st.refs_missing, uint64_t(0));
    auto gc = sync_wait(c->b->run_gc_once());
    CHECK_EQ(gc.files_removed, uint64_t(0));  // the abort had no committed parts to book
    sync_wait(c->b->delete_bucket("bkt"));
    c->close();
}

// ③ The same part number uploaded concurrently from A and B: exactly one wins
// (last commit), its content is what complete assembles and both gateways read;
// the loser's extents were booked into the gcq by the overwriting commit and are
// physically reclaimed by the GC gateway
inline void same_part_concurrent(const MetaFactory& make_meta, DuoMetaKind kind) {
    auto c = make_cluster(make_meta, kind);
    sync_wait(c->a->create_bucket("bkt"));
    auto id = sync_wait(c->a->create_multipart("bkt", "obj", {}));
    const std::string da = pattern_bytes(6000, 'a'), db = pattern_bytes(6000, 'h');
    GatedReader ba(da.substr(0, 5000), da.substr(5000)), bb(db.substr(0, 5000), db.substr(5000));
    PutResult ra, rb;
    std::exception_ptr ea, eb;
    std::thread ta([&] {
        try {
            ra = sync_wait(c->a->upload_part("bkt", "obj", id, 1, ba));
        } catch (...) {
            ea = std::current_exception();
        }
    });
    std::thread tb([&] {
        try {
            rb = sync_wait(c->b->upload_part("bkt", "obj", id, 1, bb));
        } catch (...) {
            eb = std::current_exception();
        }
    });
    // A gated writer has at most 2 files down (chunk 0 sealed, chunk 1 open), so 3
    // means both writers are past their first chunk and parked on the gate
    wait_chunks_at_least(c->cfg.root, 3);
    ba.release();
    bb.release();
    ta.join();
    tb.join();
    if (ea) std::rethrow_exception(ea);
    if (eb) std::rethrow_exception(eb);
    CHECK(ra.etag != rb.etag);

    auto listed = parts_of(*c->b, "bkt", "obj", id);
    CHECK_EQ(listed.size(), size_t(1));
    CHECK_EQ(listed[0].part_no, 1);
    const bool a_won = listed[0].etag == ra.etag;
    CHECK(a_won || listed[0].etag == rb.etag);
    const std::string& winner = a_won ? da : db;
    CHECK_EQ(parts_of(*c->a, "bkt", "obj", id)[0].etag, listed[0].etag);  // same view from A

    std::vector<PartInfo> parts{part_info(1, listed[0].etag)};
    auto done = sync_wait(c->b->complete_multipart("bkt", "obj", id, parts));
    CHECK_EQ(done.etag, combined_etag({listed[0].etag}));
    for (auto* gw : {c->a.get(), c->b.get()}) {
        auto g = sync_wait(gw->get_object("bkt", "obj", std::nullopt));
        CHECK_EQ(read_all(*g.body), winner);
    }
    // 4 chunks landed (2 per writer); the loser's 2 are in the gcq and come off the
    // shared plane in the GC gateway's round
    CHECK_EQ(count_chunk_files(c->cfg.root), size_t(4));
    c->publish_leases();
    auto gc = sync_wait(c->a->run_gc_once());
    CHECK_EQ(gc.reclaims_acked, uint64_t(1));
    CHECK_EQ(gc.files_removed, uint64_t(2));
    CHECK_EQ(gc.skipped_leased, uint64_t(0));
    CHECK_EQ(count_chunk_files(c->cfg.root), size_t(2));
    {
        auto g = sync_wait(c->b->get_object("bkt", "obj", std::nullopt));  // winner intact
        CHECK_EQ(read_all(*g.body), winner);
    }
    sync_wait(c->a->delete_object("bkt", "obj"));
    sync_wait(c->a->delete_bucket("bkt"));
    c->close();
}

// ④ mpu_ttl cleanup runs on one gateway only: the GC lease (P6) makes the second
// instance's round a no-op while the first holds it — whichever order the expired
// upload is created and swept in
inline void mpu_ttl_single_executor(const MetaFactory& make_meta, DuoMetaKind kind) {
    auto c = make_cluster(make_meta, kind, [](DuoStoreConfig& cfg) { cfg.mpu_ttl_sec = 1; });
    sync_wait(c->a->create_bucket("bkt"));
    auto id1 = sync_wait(c->a->create_multipart("bkt", "one", {}));
    upload(*c->b, "bkt", "one", id1, 1, pattern_bytes(6000));  // 2 chunks
    CHECK_EQ(count_chunk_files(c->cfg.root), size_t(2));
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));  // past the 1 s ttl

    c->publish_leases();
    auto sa = sync_wait(c->a->run_gc_once());  // A takes the GC lease
    CHECK_EQ(sa.uploads_expired, uint64_t(1));
    // The abort inside the round enqueued the parts after the peer's published
    // read floor: deferred this round (a reader on B that started before the
    // abort could still hold the ref), reclaimed once the floor moves past it
    CHECK_EQ(sa.skipped_leased, uint64_t(1));
    CHECK_EQ(sa.files_removed, uint64_t(0));
    auto sb = sync_wait(c->b->run_gc_once());  // lease held by A: B does nothing
    CHECK_EQ(sb.uploads_expired, uint64_t(0));
    CHECK_EQ(sb.reclaims_acked, uint64_t(0));
    CHECK(uploads_of(*c->b, "bkt").empty());
    c->publish_leases();
    sa = sync_wait(c->a->run_gc_once());
    CHECK_EQ(sa.reclaims_acked, uint64_t(1));
    CHECK_EQ(sa.files_removed, uint64_t(2));
    CHECK_EQ(count_chunk_files(c->cfg.root), size_t(0));

    // A second expired upload: B still cannot sweep it (A's lease outlives the
    // round), A does
    auto id2 = sync_wait(c->b->create_multipart("bkt", "two", {}));
    upload(*c->a, "bkt", "two", id2, 1, pattern_bytes(3000));  // 1 chunk
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    c->publish_leases();
    sb = sync_wait(c->b->run_gc_once());
    CHECK_EQ(sb.uploads_expired, uint64_t(0));
    CHECK_EQ(uploads_of(*c->a, "bkt").size(), size_t(1));  // untouched
    sa = sync_wait(c->a->run_gc_once());
    CHECK_EQ(sa.uploads_expired, uint64_t(1));
    c->publish_leases();
    sa = sync_wait(c->a->run_gc_once());
    CHECK_EQ(sa.files_removed, uint64_t(1));
    CHECK(uploads_of(*c->a, "bkt").empty());
    CHECK(uploads_of(*c->b, "bkt").empty());
    CHECK_EQ(count_chunk_files(c->cfg.root), size_t(0));
    {
        http::StringBodyReader part("x");
        CHECK_THROWS_S3(sync_wait(c->b->upload_part("bkt", "two", id2, 2, part)),
                        s3::S3ErrorCode::NoSuchUpload);
    }
    sync_wait(c->a->delete_bucket("bkt"));
    c->close();
}

// ⑤ list_parts / list_uploads are one shared view: uploads created and parts
// recorded on either gateway list identically from both, and an abort on one
// retires the upload for the other
inline void listings_are_shared(const MetaFactory& make_meta, DuoMetaKind kind) {
    auto c = make_cluster(make_meta, kind);
    sync_wait(c->b->create_bucket("bkt"));
    auto u1 = sync_wait(c->a->create_multipart("bkt", "k1", {}));
    auto u2 = sync_wait(c->b->create_multipart("bkt", "k1", {}));  // second upload on the same key
    auto u3 = sync_wait(c->b->create_multipart("bkt", "k2", {}));
    upload(*c->a, "bkt", "k1", u1, 1, pattern_bytes(5000, 'a'));
    upload(*c->b, "bkt", "k1", u1, 2, pattern_bytes(4500, 'b'));
    upload(*c->a, "bkt", "k1", u1, 3, pattern_bytes(100, 'c'));
    upload(*c->b, "bkt", "k2", u3, 1, pattern_bytes(6000, 'd'));

    auto la = uploads_of(*c->a, "bkt"), lb = uploads_of(*c->b, "bkt");
    CHECK_EQ(la.size(), size_t(3));
    CHECK_EQ(lb.size(), size_t(3));
    for (size_t i = 0; i < la.size(); ++i) {
        CHECK_EQ(la[i].key, lb[i].key);
        CHECK_EQ(la[i].upload_id, lb[i].upload_id);
    }
    std::set<std::string> ids{u1, u2, u3};
    for (const auto& u : la) CHECK(ids.count(u.upload_id) == 1);

    const std::vector<std::tuple<std::string, std::string, size_t>> expect{
        {"k1", u1, 3}, {"k1", u2, 0}, {"k2", u3, 1}};
    for (const auto& [key, id, n] : expect) {
        auto pa = parts_of(*c->a, "bkt", key, id), pb = parts_of(*c->b, "bkt", key, id);
        CHECK_EQ(pa.size(), n);
        CHECK_EQ(pb.size(), n);
        for (size_t i = 0; i < pa.size(); ++i) {
            CHECK_EQ(pa[i].part_no, pb[i].part_no);
            CHECK_EQ(pa[i].size, pb[i].size);
            CHECK_EQ(pa[i].etag, pb[i].etag);
        }
    }

    // Abort on one side, gone on both; the aborted parts reclaim on the GC gateway
    sync_wait(c->b->abort_multipart("bkt", "k1", u1));
    CHECK_EQ(uploads_of(*c->a, "bkt").size(), size_t(2));
    CHECK_THROWS_S3(parts_of(*c->a, "bkt", "k1", u1), s3::S3ErrorCode::NoSuchUpload);
    sync_wait(c->a->abort_multipart("bkt", "k1", u2));
    sync_wait(c->a->abort_multipart("bkt", "k2", u3));
    CHECK(uploads_of(*c->b, "bkt").empty());
    c->publish_leases();
    auto gc = sync_wait(c->a->run_gc_once());
    CHECK_EQ(gc.files_removed, uint64_t(7));  // u1: 2 + 2 + 1 chunks, u3: 2 chunks
    CHECK_EQ(count_chunk_files(c->cfg.root), size_t(0));
    sync_wait(c->a->delete_bucket("bkt"));
    c->close();
}

}  // namespace multi_gateway_suite
