// Meta store conformance suite (docs/storage/duostore-meta-redis-design.md §9): the same set of cases runs
// parameterized over all IMetaStore implementations (RocksMetaStore always, RedisMetaStore conditionally); both
// implementations share the same semantic baseline. Extracted from the meta cases of test_duostore.cc.
// Factory convention: each call opens a new instance on the same underlying storage ("restart" semantics);
// scenarios within the suite are isolated by distinct bucket names, and instances open/close serially (RocksDB
// single-process lock).
#pragma once

#include <algorithm>
#include <functional>
#include <memory>
#include <optional>
#include <string>
#include <vector>

#include "storage/duostore/codec.h"
#include "storage/duostore/meta_store.h"
// kReclaimMaxExtents (gcq split cap)
#include "storage/duostore/meta_util.h"
#include "unit/mini_test.h"

namespace meta_store_suite {

using namespace lights3::storage;
using namespace lights3::storage::duostore;

using MetaFactory = std::function<std::unique_ptr<IMetaStore>()>;

inline Extent chunk_extent(uint64_t id, uint64_t len, uint32_t crc = 0) {
    return {Extent::Kind::kChunk, id, 0, len, crc};
}

inline Extent pack_extent(uint64_t pack_id, uint64_t off, uint64_t len, uint32_t crc = 0) {
    return {Extent::Kind::kPack, pack_id, off, len, crc};
}

inline std::optional<PackStat> find_pack(IMetaStore& m, uint64_t pack_id) {
    for (const auto& ps : m.pack_stats())
        if (ps.pack_id == pack_id) return ps;
    return std::nullopt;
}

// Drain the gcq at the end of a case (so later cases' assertions on queue state are not affected by leftovers)
inline void drain_gcq(IMetaStore& m) {
    for (;;) {
        auto rs = m.peek_reclaims(256);
        if (rs.empty()) return;
        std::vector<uint64_t> seqs;
        for (auto& [seq, rc] : rs) seqs.push_back(seq);
        m.ack_reclaims(seqs);
    }
}

inline ObjectRec make_rec(std::string key, std::vector<Extent> extents) {
    ObjectRec rec;
    rec.meta.key = std::move(key);
    rec.meta.etag = "deadbeef";
    rec.meta.last_modified = std::chrono::system_clock::now();
    rec.data.extents = std::move(extents);
    rec.meta.size = rec.data.total();
    return rec;
}

// GC accounting for overwrite/delete (main doc §4.5 same-batch invariants): gcq entries, refs added/removed, version
// incremented
inline void case_gc_accounting(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-gc");

    uint64_t id1 = m->alloc_file_id(Extent::Kind::kChunk);
    m->put_object("ms-gc", "k", make_rec("k", {chunk_extent(id1, 3)}));
    CHECK(m->chunk_referenced(id1));
    CHECK_EQ(m->peek_reclaims(10).size(), size_t(0));
    CHECK_EQ(m->get_object("ms-gc", "k")->version, uint64_t(1));

    // Overwrite: old extent enters gcq, old refs removed, new refs established
    uint64_t id2 = m->alloc_file_id(Extent::Kind::kChunk);
    m->put_object("ms-gc", "k", make_rec("k", {chunk_extent(id2, 5)}));
    auto rs = m->peek_reclaims(10);
    CHECK_EQ(rs.size(), size_t(1));
    CHECK_EQ(rs[0].second.extents.at(0).file_id, id1);
    // enqueue timestamp returned (GC consumer checks gc_grace, §9.1)
    CHECK(rs[0].second.enqueue_ms > 0);
    // The entry's origin is persisted with the record (docs/archive/gaps.md §6.1): GC buckets its counts by it
    CHECK(rs[0].second.reason == ReclaimReason::kOverwrite);
    CHECK(!m->chunk_referenced(id1));
    CHECK(m->chunk_referenced(id2));
    CHECK_EQ(m->get_object("ms-gc", "k")->version, uint64_t(2));

    // Delete: enters gcq; an idempotent second delete returns false
    CHECK(m->delete_object("ms-gc", "k"));
    CHECK(!m->delete_object("ms-gc", "k"));
    CHECK(!m->chunk_referenced(id2));
    auto rs2 = m->peek_reclaims(10);
    CHECK_EQ(rs2.size(), size_t(2));
    CHECK(rs2[1].second.reason == ReclaimReason::kDelete);
    // min_seq resumable scan (§9.1): peeking from the second item's seq sees only what follows; past the tail it is
    // empty
    auto tail = m->peek_reclaims(10, rs2[1].first);
    CHECK_EQ(tail.size(), size_t(1));
    CHECK_EQ(tail[0].first, rs2[1].first);
    CHECK_EQ(m->peek_reclaims(10, rs2[1].first + 1).size(), size_t(0));

    // gcq is empty after acking: exercise both the per-item and batch interfaces (batch is the GC consumer's preferred
    // form)
    m->ack_reclaim(rs2[0].first);
    std::vector<uint64_t> rest;
    for (size_t i = 1; i < rs2.size(); ++i) rest.push_back(rs2[i].first);
    m->ack_reclaims(rest);
    CHECK_EQ(m->peek_reclaims(10).size(), size_t(0));
    m->delete_bucket("ms-gc");
    m->close();
}

// gcq entry origins (docs/archive/gaps.md §6.1): each of the six origins records its own reason so GC can tell
// whether reclaim pressure comes from overwrites, bulk deletes, or abandoned mpu parts. Assert per item
// rather than by count -- the suite's cases share the underlying storage, and this case only looks at the entries it
// created
inline void case_reclaim_reasons(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-reason");
    drain_gcq(*m);

    uint64_t pid = m->alloc_file_id(Extent::Kind::kPack);
    lights3::storage::ObjectMeta meta;
    auto id = m->create_upload("ms-reason", "k", meta);
    auto part = [&](int no, uint64_t off, uint64_t len) {
        PartRec p;
        p.part_no = no;
        p.size = len;
        p.etag = "deadbeef";
        p.data.extents = {pack_extent(pid, off, len)};
        return p;
    };
    // Build up step by step: same-number re-upload -> not selected at complete -> complete overwrites same-named object
    // -> abort -> delete
    m->put_object("ms-reason", "k", make_rec("k", {pack_extent(pid, 900, 10)}));
    // the put above only exists so complete has an old object to overwrite; not part of the assertions
    drain_gcq(*m);

    m->put_part("ms-reason", "k", id, part(1, 0, 100));
    // same-number re-upload
    m->put_part("ms-reason", "k", id, part(1, 100, 100));
    m->put_part("ms-reason", "k", id, part(2, 200, 50));
    std::vector<PartInfo> sel1 = {{1, "deadbeef"}};
    // part2 not selected + overwrites the old object
    m->complete_upload("ms-reason", "k", id, sel1);

    auto id2 = m->create_upload("ms-reason", "k2", meta);
    m->put_part("ms-reason", "k2", id2, part(1, 400, 30));
    m->abort_upload("ms-reason", "k2", id2);
    CHECK(m->delete_object("ms-reason", "k"));

    bool part_ovw = false, complete = false, overwrite = false, abort = false, del = false;
    for (const auto& [seq, rc] : m->peek_reclaims(256)) {
        (void)seq;
        switch (rc.reason) {
            case ReclaimReason::kPartOverwrite:
                part_ovw = true;
                break;
            case ReclaimReason::kComplete:
                complete = true;
                break;
            case ReclaimReason::kOverwrite:
                overwrite = true;
                break;
            case ReclaimReason::kAbort:
                abort = true;
                break;
            case ReclaimReason::kDelete:
                del = true;
                break;
            case ReclaimReason::kUnknown:
                CHECK(false);
                // new entries must not record unknown
                break;
        }
    }
    CHECK(part_ovw);
    CHECK(complete);
    CHECK(overwrite);
    CHECK(abort);
    CHECK(del);

    drain_gcq(*m);
    m->delete_bucket("ms-reason");
    m->close();
}

// file_id segments: no rollback after restart (only uniqueness and monotonicity are required, not contiguity --
// the absolute value is an implementation detail: RocksDB starts at 0, Redis burns the first segment and starts at
// kIdSegment, docs/storage/duostore-meta-redis-design.md §4)
inline void case_alloc_monotonic_across_reopen(const MetaFactory& make) {
    uint64_t last = 0;
    {
        auto m = make();
        for (int i = 0; i < 10; ++i) last = m->alloc_file_id(Extent::Kind::kChunk);
        m->close();
    }
    {
        auto m = make();
        CHECK(m->alloc_file_id(Extent::Kind::kChunk) > last);
        // pack and chunk counters are independent and each monotonic
        uint64_t p1 = m->alloc_file_id(Extent::Kind::kPack);
        CHECK_EQ(m->alloc_file_id(Extent::Kind::kPack), p1 + 1);
        m->close();
    }
}

// delete_bucket: refused while a multipart upload is in progress (matches AWS; prevents permanent refs leaks and
// ghost-upload resurrection)
inline void case_delete_bucket_blocks_on_mpu(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-mpu");
    lights3::storage::ObjectMeta meta;
    auto id = m->create_upload("ms-mpu", "k", meta);
    CHECK_THROWS_S3(m->delete_bucket("ms-mpu"), lights3::s3::S3ErrorCode::BucketNotEmpty);
    m->abort_upload("ms-mpu", "k", id);
    // deletable after abort
    m->delete_bucket("ms-mpu");
    CHECK(!m->bucket_exists("ms-mpu"));
    m->close();
}

// max-keys=0: S3 semantics are an empty result + IsTruncated=false (otherwise the empty token puts clients into an
// infinite loop)
inline void case_list_max_keys_zero(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-mk0");
    m->put_object("ms-mk0", "k", make_rec("k", {}));
    ListOptions opt;
    opt.max_keys = 0;
    auto r = m->list_objects("ms-mk0", opt);
    CHECK_EQ(r.objects.size(), size_t(0));
    CHECK(!r.is_truncated);
    m->delete_object("ms-mk0", "k");
    m->delete_bucket("ms-mk0");
    m->close();
}

// list: skip-iteration over delimiter groups + pagination token lands at the group tail (main doc §4.4 / redis version
// §2.3)
inline void case_list_delimiter_paging(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-page");
    for (auto k : {"a", "b/1", "b/2", "b/3", "c"}) m->put_object("ms-page", k, make_rec(k, {}));

    ListOptions opt;
    opt.delimiter = "/";
    opt.max_keys = 2;
    auto p1 = m->list_objects("ms-page", opt);
    CHECK_EQ(p1.objects.size(), size_t(1));
    CHECK_EQ(p1.objects[0].key, "a");
    CHECK_EQ(p1.common_prefixes.size(), size_t(1));
    CHECK_EQ(p1.common_prefixes[0], "b/");
    CHECK(p1.is_truncated);
    // token semantics (start after) must land at the group tail
    CHECK_EQ(p1.next_token, "b/3");

    opt.start_after = p1.next_token;
    auto p2 = m->list_objects("ms-page", opt);
    CHECK_EQ(p2.objects.size(), size_t(1));
    CHECK_EQ(p2.objects[0].key, "c");
    CHECK(p2.common_prefixes.empty());
    CHECK(!p2.is_truncated);

    // prefix + delimiter: grouping at the sub-level
    ListOptions sub;
    sub.prefix = "b/";
    sub.delimiter = "/";
    auto ps = m->list_objects("ms-page", sub);
    CHECK_EQ(ps.objects.size(), size_t(3));
    CHECK(ps.common_prefixes.empty());
    for (auto k : {"a", "b/1", "b/2", "b/3", "c"}) m->delete_object("ms-page", k);
    m->delete_bucket("ms-page");
    m->close();
}

// Pack liveness accounting (main doc §9.1/P2): commit-type transactions add/subtract in the same batch, seal is
// idempotent and 0 does not overwrite a known size, swap accounting migrates with the extent, live=0 entries stay
// visible, drop clears the entry
inline void case_pack_stats_accounting(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-pk");
    uint64_t pid = m->alloc_file_id(Extent::Kind::kPack);
    // no entry until something is written
    CHECK(!find_pack(*m, pid).has_value());

    // Each record counts a 29B header (22 fixed + "ms-pk\0<k>" owner, same accounting basis as §2.3a)
    m->put_object("ms-pk", "a", make_rec("a", {pack_extent(pid, 30, 100)}));
    m->put_object("ms-pk", "b", make_rec("b", {pack_extent(pid, 160, 50)}));
    auto ps = find_pack(*m, pid);
    CHECK(ps.has_value());
    CHECK_EQ(ps->live_bytes, int64_t(150 + 2 * 29));
    CHECK_EQ(ps->live_recs, int64_t(2));
    CHECK(!ps->sealed);

    // Compaction swaps the ref: accounting migrates with the extent (−pack +chunk)
    uint64_t cid = m->alloc_file_id(Extent::Kind::kChunk);
    CHECK(m->swap_extents("ms-pk", "a", /*expect_version=*/1, DataRef{{pack_extent(pid, 30, 100)}},
                          DataRef{{chunk_extent(cid, 100)}}));
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(50 + 29));
    CHECK_EQ(ps->live_recs, int64_t(1));

    // Seal: idempotent, file_size=0 does not overwrite a known value
    m->seal_pack(pid, 4096);
    m->seal_pack(pid, 0);
    ps = find_pack(*m, pid);
    CHECK(ps->sealed);
    CHECK_EQ(ps->file_size, uint64_t(4096));

    // Overwrite (pack -> pack): old decremented, new incremented
    uint64_t pid2 = m->alloc_file_id(Extent::Kind::kPack);
    m->put_object("ms-pk", "b", make_rec("b", {pack_extent(pid2, 0, 70)}));
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(0));
    CHECK_EQ(ps->live_recs, int64_t(0));
    // a sealed pack with live=0 stays visible -- candidate for whole-pack deletion of empty packs
    // (§9.1)
    CHECK(ps->sealed);
    auto ps2 = find_pack(*m, pid2);
    CHECK_EQ(ps2->live_bytes, int64_t(70 + 29));

    // Clear the entry (after whole-pack deletion of the empty pack)
    m->drop_pack_stat(pid);
    CHECK(!find_pack(*m, pid).has_value());

    // Delete the object: pid2 drops to zero
    CHECK(m->delete_object("ms-pk", "b"));
    ps2 = find_pack(*m, pid2);
    CHECK_EQ(ps2->live_bytes, int64_t(0));
    CHECK_EQ(ps2->live_recs, int64_t(0));
    m->drop_pack_stat(pid2);

    CHECK(m->delete_object("ms-pk", "a"));
    drain_gcq(*m);
    m->delete_bucket("ms-pk");
    m->close();
}

// Pack accounting for multipart: put_part counts in, same-number re-upload swaps the entry, parts selected at
// complete are not double-counted (refs transfer != liveness change), unselected parts are deducted, abort deducts
// everything
inline void case_pack_stats_multipart(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-pmpu");
    uint64_t pid = m->alloc_file_id(Extent::Kind::kPack);
    lights3::storage::ObjectMeta meta;
    auto id = m->create_upload("ms-pmpu", "k", meta);

    auto part = [&](int no, uint64_t off, uint64_t len) {
        PartRec p;
        p.part_no = no;
        p.size = len;
        p.etag = "deadbeef";
        p.data.extents = {pack_extent(pid, off, len)};
        return p;
    };
    // Part record header overhead (same accounting basis as §2.3a): 22 fixed + "mpu\0<b>\0<k>\0<id>\0<no>"
    const int64_t pov = codec::pack_rec_overhead_part("ms-pmpu", "k", id, 1);
    const int64_t oov = codec::pack_rec_overhead("ms-pmpu", "k");
    m->put_part("ms-pmpu", "k", id, part(1, 0, 100));
    m->put_part("ms-pmpu", "k", id, part(2, 130, 60));
    m->put_part("ms-pmpu", "k", id, part(3, 220, 40));
    auto ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(200) + 3 * pov);
    CHECK_EQ(ps->live_recs, int64_t(3));

    // Same-number re-upload (last-write-wins): old 60 deducted, new 80 counted
    m->put_part("ms-pmpu", "k", id, part(2, 300, 80));
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(220) + 3 * pov);
    CHECK_EQ(ps->live_recs, int64_t(3));

    // complete selects 1/2: selected parts' liveness is unchanged but the accounting basis is rebalanced
    // (-part header +object header, so a later object delete deducted on the object basis lands exactly at zero);
    // unselected part3 deducts 40+header
    std::vector<PartInfo> sel = {{1, "deadbeef"}, {2, "deadbeef"}};
    m->complete_upload("ms-pmpu", "k", id, sel);
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(180) + 2 * oov);
    CHECK_EQ(ps->live_recs, int64_t(2));

    // Abort path: all pack parts of the other upload are deducted
    auto id2 = m->create_upload("ms-pmpu", "k2", meta);
    const int64_t pov2 = codec::pack_rec_overhead_part("ms-pmpu", "k2", id2, 1);
    m->put_part("ms-pmpu", "k2", id2, part(1, 400, 30));
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(210) + 2 * oov + pov2);
    m->abort_upload("ms-pmpu", "k2", id2);
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(180) + 2 * oov);
    CHECK_EQ(ps->live_recs, int64_t(2));

    // Delete the object produced by complete -> drops to zero
    CHECK(m->delete_object("ms-pmpu", "k"));
    ps = find_pack(*m, pid);
    CHECK_EQ(ps->live_bytes, int64_t(0));
    CHECK_EQ(ps->live_recs, int64_t(0));
    m->drop_pack_stat(pid);
    drain_gcq(*m);
    m->delete_bucket("ms-pmpu");
    m->close();
}

// refs traversal (for reverse reconciliation, P4 §9.3): writes create refs, scan_refs sees them, they disappear after
// delete; consistent with chunk_referenced point lookups. Assertions use containment semantics -- the suite's cases
// share the underlying storage, only this case's ids are checked
inline void case_scan_refs(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-scanrefs");
    uint64_t a = m->alloc_file_id(Extent::Kind::kChunk);
    uint64_t b = m->alloc_file_id(Extent::Kind::kChunk);
    uint64_t c = m->alloc_file_id(Extent::Kind::kChunk);
    m->put_object("ms-scanrefs", "k1", make_rec("k1", {chunk_extent(a, 10)}));
    m->put_object("ms-scanrefs", "k2", make_rec("k2", {chunk_extent(b, 20), chunk_extent(c, 30)}));

    auto collect = [&] {
        std::vector<uint64_t> ids;
        m->scan_refs([&](uint64_t id) { ids.push_back(id); });
        return ids;
    };
    auto has = [](const std::vector<uint64_t>& ids, uint64_t id) {
        for (uint64_t x : ids)
            if (x == id) return true;
        return false;
    };
    auto ids = collect();
    CHECK(has(ids, a));
    CHECK(has(ids, b));
    CHECK(has(ids, c));

    CHECK(m->delete_object("ms-scanrefs", "k1"));
    ids = collect();
    // delete clears the ref in the same batch (§4.5)
    CHECK(!has(ids, a));
    CHECK(has(ids, b));
    CHECK(has(ids, c));
    // point lookup consistent with traversal
    CHECK(!m->chunk_referenced(a));
    CHECK(m->chunk_referenced(b));

    CHECK(m->delete_object("ms-scanrefs", "k2"));
    ids = collect();
    CHECK(!has(ids, b));
    CHECK(!has(ids, c));
    drain_gcq(*m);
    m->delete_bucket("ms-scanrefs");
    m->close();
}

// gcq splitting (gaps §2.11): an oversized DataRef is split into multiple entries by kReclaimMaxExtents on enqueue;
// peek's max_extents cap closes the batch early but returns at least 1 item; after full consumption the extent total is
// conserved
inline void case_reclaim_split_and_capped_peek(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-split");
    // splits into 2 entries: 4096 + 700
    const size_t total = kReclaimMaxExtents + 700;
    std::vector<Extent> big;
    big.reserve(total);
    for (size_t i = 0; i < total; ++i) big.push_back(chunk_extent(m->alloc_file_id(Extent::Kind::kChunk), 1));
    m->put_object("ms-split", "big", make_rec("big", std::move(big)));
    CHECK(m->delete_object("ms-split", "big"));

    // Capped peek: the first batch closes once it accumulates kReclaimMaxExtents (1 item)
    auto capped = m->peek_reclaims(100, 0, kReclaimMaxExtents);
    CHECK_EQ(capped.size(), size_t(1));
    CHECK_EQ(capped[0].second.extents.size(), kReclaimMaxExtents);

    // Full view: exactly 2 items, extent total conserved
    auto all = m->peek_reclaims(100, 0);
    CHECK_EQ(all.size(), size_t(2));
    size_t seen = 0;
    std::vector<uint64_t> seqs;
    for (auto& [seq, rc] : all) {
        seen += rc.extents.size();
        seqs.push_back(seq);
    }
    CHECK_EQ(seen, total);
    m->ack_reclaims(seqs);
    m->delete_bucket("ms-split");
    m->close();
}

// swap_extents_batch (gaps §2.13 batched compaction): per-item CAS is independent -- an item with a mismatched
// version fails without writing, the rest take effect as usual (rocks/sqlite override with a single-batch commit,
// redis/tikv use the default per-item forwarding)
inline void case_swap_extents_batch(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-swapb");
    uint64_t pid = m->alloc_file_id(Extent::Kind::kPack);
    m->put_object("ms-swapb", "a", make_rec("a", {pack_extent(pid, 30, 100)}));
    m->put_object("ms-swapb", "b", make_rec("b", {pack_extent(pid, 160, 50)}));

    uint64_t ca = m->alloc_file_id(Extent::Kind::kChunk);
    uint64_t cb = m->alloc_file_id(Extent::Kind::kChunk);
    std::vector<SwapReq> reqs;
    reqs.push_back({"ms-swapb", "a", /*expect_version=*/1, DataRef{{pack_extent(pid, 30, 100)}},
                    DataRef{{chunk_extent(ca, 100)}}});
    // version mismatch: this item must fail
    reqs.push_back(
        {"ms-swapb", "b", /*expect_version=*/7, DataRef{{pack_extent(pid, 160, 50)}}, DataRef{{chunk_extent(cb, 50)}}});
    auto ok = m->swap_extents_batch(reqs);
    CHECK_EQ(ok.size(), size_t(2));
    CHECK(ok[0]);
    CHECK(!ok[1]);

    auto ra = m->get_object("ms-swapb", "a");
    CHECK(ra->data.extents[0].kind == Extent::Kind::kChunk);
    // successful item's version increments
    CHECK_EQ(ra->version, uint64_t(2));
    auto rb = m->get_object("ms-swapb", "b");
    // failed item untouched
    CHECK(rb->data.extents[0].kind == Extent::Kind::kPack);
    CHECK_EQ(rb->version, uint64_t(1));
    // refs migrate with the successful item
    CHECK(m->chunk_referenced(ca));
    // failed item writes nothing
    CHECK(!m->chunk_referenced(cb));

    CHECK(m->delete_object("ms-swapb", "a"));
    CHECK(m->delete_object("ms-swapb", "b"));
    drain_gcq(*m);
    m->drop_pack_stat(pid);
    m->delete_bucket("ms-swapb");
    m->close();
}

// list_uploads pushdown hints (roadmap §3.5): engines that honor `limit` must also honor the
// prefix and the key-marker-only cursor, otherwise the caller's own filtering can empty a page
// and misreport end-of-list. Every engine must at least return a superset in (key, id) order
inline void case_list_uploads_hints(const MetaFactory& make) {
    auto m = make();
    m->create_bucket("ms-uplist");
    lights3::storage::ObjectMeta meta;
    std::vector<std::string> a_ids, b_ids;
    for (int i = 0; i < 5; ++i) a_ids.push_back(m->create_upload("ms-uplist", "a/k", meta));
    for (int i = 0; i < 3; ++i) b_ids.push_back(m->create_upload("ms-uplist", "b/k", meta));
    std::sort(a_ids.begin(), a_ids.end());
    std::sort(b_ids.begin(), b_ids.end());
    auto ordered = [](const std::vector<UploadInfo>& v) {
        for (size_t i = 1; i < v.size(); ++i)
            if (!(std::pair(v[i - 1].key, v[i - 1].upload_id) < std::pair(v[i].key, v[i].upload_id))) return false;
        return true;
    };
    // prefix + limit: the page must come from inside the prefix range
    auto pb = m->list_uploads("ms-uplist", {}, {}, 2, "b/");
    CHECK(ordered(pb));
    CHECK(!pb.empty());
    for (auto& u : pb) CHECK(u.key.compare(0, 2, "b/") == 0);
    CHECK_EQ(pb[0].upload_id, b_ids[0]);
    // key-marker only: everything of that key is already paged past
    auto pk = m->list_uploads("ms-uplist", "a/k", {}, 2);
    CHECK(ordered(pk));
    CHECK(!pk.empty());
    for (auto& u : pk) CHECK(u.key != "a/k");
    CHECK_EQ(pk[0].upload_id, b_ids[0]);
    // composite marker: strictly after (key, id)
    auto pc = m->list_uploads("ms-uplist", "a/k", a_ids[2], 0);
    CHECK(ordered(pc));
    CHECK(pc.size() >= size_t(2 + 3));
    CHECK_EQ(pc[0].key, "a/k");
    CHECK_EQ(pc[0].upload_id, a_ids[3]);
    // no prefix match → empty, not "the first entries of the table"
    auto pn = m->list_uploads("ms-uplist", {}, {}, 2, "zz/");
    CHECK(pn.empty());
    for (auto& id : a_ids) m->abort_upload("ms-uplist", "a/k", id);
    for (auto& id : b_ids) m->abort_upload("ms-uplist", "b/k", id);
    m->delete_bucket("ms-uplist");
    m->close();
}

// KV facade (docs/s3-tables/step-6-optional.md §5): opaque values, etag = sha256[:16],
// PutCondition inside the engine's atomic section, ordered prefix scans with an
// exclusive `after`, all-or-nothing batches, survival across reopen
inline void case_kv_facade(const MetaFactory& make) {
    auto m = make();
    CHECK(!m->kv_get("kv/a").has_value());
    CHECK(m->kv_scan("kv/", "", 0).empty());
    std::string e1 = m->kv_put("kv/a", "one");
    CHECK_EQ(e1, kv_etag("one"));
    auto got = m->kv_get("kv/a");
    CHECK(got && got->value == "one" && got->etag == e1);
    // if_none_match on an existing key, if_match on a wrong / missing etag
    PutCondition none;
    none.if_none_match = true;
    bool failed = false;
    try {
        m->kv_put("kv/a", "two", none);
    } catch (const lights3::s3::S3Error& e) {
        failed = e.code == lights3::s3::S3ErrorCode::PreconditionFailed;
    }
    CHECK(failed);
    PutCondition wrong;
    wrong.if_match_etag = "0000000000000000";
    failed = false;
    try {
        m->kv_put("kv/a", "two", wrong);
    } catch (const lights3::s3::S3Error& e) {
        failed = e.code == lights3::s3::S3ErrorCode::PreconditionFailed;
    }
    CHECK(failed);
    failed = false;
    try {
        m->kv_put("kv/missing", "x", wrong);
    } catch (const lights3::s3::S3Error& e) {
        failed = e.code == lights3::s3::S3ErrorCode::NoSuchKey;
    }
    CHECK(failed);
    CHECK_EQ(m->kv_get("kv/a")->value, "one");
    // a matching etag swaps the value
    PutCondition match;
    match.if_match_etag = e1;
    std::string e2 = m->kv_put("kv/a", "two", match);
    CHECK_EQ(m->kv_get("kv/a")->value, "two");
    CHECK_EQ(m->kv_get("kv/a")->etag, e2);
    // ordered scan with prefix / after / limit; the empty value is a value
    m->kv_put("kv/b", "");
    m->kv_put("kv/c", "three");
    m->kv_put("kw/z", "other");
    auto all = m->kv_scan("kv/", "", 0);
    CHECK_EQ(all.size(), size_t(3));
    CHECK_EQ(all[0].key, "kv/a");
    CHECK_EQ(all[1].key, "kv/b");
    CHECK_EQ(all[1].value, "");
    CHECK_EQ(all[2].key, "kv/c");
    auto page = m->kv_scan("kv/", "", 2);
    CHECK_EQ(page.size(), size_t(2));
    auto rest = m->kv_scan("kv/", page.back().key, 2);
    CHECK_EQ(rest.size(), size_t(1));
    CHECK_EQ(rest[0].key, "kv/c");
    CHECK(m->kv_scan("kv/", "kv/c", 2).empty());
    CHECK_EQ(m->kv_scan("", "", 0).size(), size_t(4));
    // batch: all or nothing
    std::vector<KvPut> batch;
    batch.push_back({"kv/d", "four", {}});
    KvPut bad{"kv/a", "clobber", {}};
    bad.cond.if_none_match = true;
    batch.push_back(bad);
    failed = false;
    try {
        m->kv_put_batch(batch);
    } catch (const lights3::s3::S3Error& e) {
        failed = e.code == lights3::s3::S3ErrorCode::PreconditionFailed;
    }
    CHECK(failed);
    CHECK(!m->kv_get("kv/d").has_value());
    CHECK_EQ(m->kv_get("kv/a")->value, "two");
    batch[1].cond = match;
    batch[1].cond.if_match_etag = e2;
    auto etags = m->kv_put_batch(batch);
    CHECK_EQ(etags.size(), size_t(2));
    CHECK_EQ(m->kv_get("kv/d")->value, "four");
    CHECK_EQ(m->kv_get("kv/a")->value, "clobber");
    // delete is idempotent
    CHECK(m->kv_delete("kv/b"));
    CHECK(!m->kv_delete("kv/b"));
    CHECK(!m->kv_get("kv/b").has_value());
    m->close();
    // reopen: the space is durable
    auto m2 = make();
    CHECK_EQ(m2->kv_get("kv/a")->value, "clobber");
    CHECK_EQ(m2->kv_scan("kv/", "", 0).size(), size_t(3));
    m2->kv_delete("kv/a");
    m2->kv_delete("kv/c");
    m2->kv_delete("kv/d");
    m2->kv_delete("kw/z");
    m2->close();
}

inline void run_meta_store_suite(const MetaFactory& make) {
    case_kv_facade(make);
    case_gc_accounting(make);
    case_reclaim_reasons(make);
    case_alloc_monotonic_across_reopen(make);
    case_delete_bucket_blocks_on_mpu(make);
    case_list_uploads_hints(make);
    case_list_max_keys_zero(make);
    case_list_delimiter_paging(make);
    case_pack_stats_accounting(make);
    case_pack_stats_multipart(make);
    case_scan_refs(make);
    case_reclaim_split_and_capped_peek(make);
    case_swap_extents_batch(make);
}

}  // namespace meta_store_suite
