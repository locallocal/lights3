#include "storage/duostore/meta_dump.h"

#include <algorithm>
#include <cstring>
#include <istream>
#include <ostream>
#include <span>
#include <string>

#include "core/log.h"
#include "storage/duostore/codec.h"

namespace lights3::storage::duostore {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

// Stream format: after the magic come per-record tag + fixed-length /
// length-prefixed fields (all little-endian), terminated by 'E':
//   'B' u32 len | bucket
//   'O' u32 blen u32 klen u32 vlen | bucket key value(codec::encode_object)
//   'S' u64 pack_id u64 file_size          (sealed packs only)
//   'E' u64 n_buckets u64 n_objects u64 n_packs u32 crc
// crc32c covers every byte after the magic and before the crc field (including
// 'E' and the three counts)
constexpr char kMagic[] = "L3DUOMETA1\n";
constexpr size_t kMagicLen = sizeof(kMagic) - 1;
// Per-field length cap (defensive: with the wrong file the length field is random
// bytes; don't feed the allocator 4GiB). The legitimate upper bound for an object
// value is about 26MiB (a 650k-extent manifest, meta_store.h §3.9)
constexpr uint32_t kMaxFieldLen = 256u << 20;

struct CrcWriter {
    std::ostream& out;
    uint32_t crc = 0;

    void raw(const void* p, size_t n) {
        out.write(static_cast<const char*>(p), std::streamsize(n));
        if (!out)
            throw S3Error(S3ErrorCode::InternalError, "duostore meta dump: write failed");
    }
    void body(const void* p, size_t n) {
        crc = codec::crc32c_update(crc,
                                   std::span(static_cast<const std::byte*>(p), n));
        raw(p, n);
    }
    void u8(uint8_t v) { body(&v, 1); }
    void u32(uint32_t v) {
        uint8_t b[4] = {uint8_t(v), uint8_t(v >> 8), uint8_t(v >> 16), uint8_t(v >> 24)};
        body(b, 4);
    }
    void u64(uint64_t v) {
        uint8_t b[8];
        for (int i = 0; i < 8; ++i) b[i] = uint8_t(v >> (8 * i));
        body(b, 8);
    }
    void str32(std::string_view s) {
        u32(uint32_t(s.size()));
        body(s.data(), s.size());
    }
};

struct CrcReader {
    std::istream& in;
    uint32_t crc = 0;

    void raw(void* p, size_t n) {
        in.read(static_cast<char*>(p), std::streamsize(n));
        if (size_t(in.gcount()) != n)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore meta load: truncated archive");
    }
    void body(void* p, size_t n) {
        raw(p, n);
        crc = codec::crc32c_update(crc, std::span(static_cast<const std::byte*>(p), n));
    }
    uint8_t u8() {
        uint8_t v;
        body(&v, 1);
        return v;
    }
    uint32_t u32() {
        uint8_t b[4];
        body(b, 4);
        return uint32_t(b[0]) | uint32_t(b[1]) << 8 | uint32_t(b[2]) << 16 |
               uint32_t(b[3]) << 24;
    }
    uint64_t u64() {
        uint8_t b[8];
        body(b, 8);
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v |= uint64_t(b[i]) << (8 * i);
        return v;
    }
    std::string str32() {
        uint32_t n = u32();
        if (n > kMaxFieldLen)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore meta load: implausible field length (corrupt archive?)");
        std::string s(n, '\0');
        body(s.data(), n);
        return s;
    }
};

}  // namespace

MetaDumpStats dump_meta(IMetaStore& src, std::ostream& out) {
    MetaDumpStats st;
    CrcWriter w{out};
    w.raw(kMagic, kMagicLen);
    for (const auto& b : src.list_buckets()) {
        w.u8('B');
        w.str32(b.name);
        ++st.buckets;
        ListOptions opt;
        opt.max_keys = 1000;
        for (;;) {
            auto res = src.list_objects(b.name, opt);
            for (const auto& om : res.objects) {
                auto rec = src.get_object(b.name, om.key);
                if (!rec) continue;  // writes-stopped is the operational contract; a concurrent delete is skipped only defensively
                auto val = codec::encode_object(*rec);
                w.u8('O');
                w.str32(b.name);
                w.str32(om.key);
                w.str32(val);
                ++st.objects;
            }
            if (!res.is_truncated) break;
            opt.start_after = res.next_token;
        }
    }
    for (const auto& ps : src.pack_stats()) {
        if (!ps.sealed) continue;  // unsealed packs' ledger is rebuilt by object replay; the ledger gets sealed on restart
        w.u8('S');
        w.u64(ps.pack_id);
        w.u64(ps.file_size);
        ++st.sealed_packs;
    }
    w.u8('E');
    w.u64(st.buckets);
    w.u64(st.objects);
    w.u64(st.sealed_packs);
    uint32_t crc = w.crc;
    uint8_t b[4] = {uint8_t(crc), uint8_t(crc >> 8), uint8_t(crc >> 16), uint8_t(crc >> 24)};
    w.raw(b, 4);
    out.flush();
    if (!out) throw S3Error(S3ErrorCode::InternalError, "duostore meta dump: flush failed");
    return st;
}

MetaDumpStats load_meta(IMetaStore& dst, std::istream& in) {
    char magic[kMagicLen];
    in.read(magic, std::streamsize(kMagicLen));
    if (size_t(in.gcount()) != kMagicLen || std::memcmp(magic, kMagic, kMagicLen) != 0)
        throw S3Error(S3ErrorCode::InternalError,
                      "duostore meta load: not a duostore meta archive");
    CrcReader r{in};
    MetaDumpStats st;
    // Largest file_id seen (kChunk and kRados share the id segment, see the
    // alloc_file_run implementations); "seen at all" is expressed via a +1
    // sentinel: 0 is also a valid id
    uint64_t next_chunk = 0, next_pack = 0;
    for (;;) {
        uint8_t tag = r.u8();
        if (tag == 'B') {
            std::string b = r.str32();
            try {
                dst.create_bucket(b);
            } catch (const S3Error& e) {
                if (e.code != S3ErrorCode::BucketAlreadyOwnedByYou) throw;
                // Idempotent: a load rerun after interruption lands on an already-created bucket
            }
            ++st.buckets;
        } else if (tag == 'O') {
            std::string b = r.str32();
            std::string k = r.str32();
            std::string v = r.str32();
            ObjectRec rec = codec::decode_object(k, v);
            for (const auto& e : rec.data.extents) {
                uint64_t& next =
                    e.kind == Extent::Kind::kPack ? next_pack : next_chunk;
                next = std::max(next, e.file_id + 1);
            }
            dst.put_object(b, k, std::move(rec));
            ++st.objects;
        } else if (tag == 'S') {
            uint64_t id = r.u64();
            uint64_t fsz = r.u64();
            next_pack = std::max(next_pack, id + 1);
            dst.seal_pack(id, fsz);
            ++st.sealed_packs;
        } else if (tag == 'E') {
            uint64_t nb = r.u64(), no = r.u64(), np = r.u64();
            uint32_t want = r.crc;  // accumulated value after the count fields, before the crc field
            uint8_t b[4];
            r.raw(b, 4);
            uint32_t got = uint32_t(b[0]) | uint32_t(b[1]) << 8 | uint32_t(b[2]) << 16 |
                           uint32_t(b[3]) << 24;
            if (got != want)
                throw S3Error(S3ErrorCode::InternalError,
                              "duostore meta load: archive crc mismatch");
            if (nb != st.buckets || no != st.objects || np != st.sealed_packs)
                throw S3Error(S3ErrorCode::InternalError,
                              "duostore meta load: record count mismatch");
            break;
        } else {
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore meta load: unknown record tag " + std::to_string(tag));
        }
    }
    // Counter raising: post-restore new writes must not allocate file numbers <=
    // the largest id seen (the data-side files already exist; an id collision =
    // silent mutual overwrite). Raised by burning id segments through the
    // interface — alloc is monotonically increasing, so the loop necessarily
    // terminates
    for (auto [kind, floor] : {std::pair{Extent::Kind::kChunk, next_chunk},
                               std::pair{Extent::Kind::kPack, next_pack}}) {
        if (floor == 0) continue;  // no extent of this kind was ever seen
        for (;;) {
            uint64_t got = dst.alloc_file_run(kind, 1);
            if (got >= floor) break;
            uint64_t gap = floor - got - 1;
            while (gap > 0) {
                uint32_t step = uint32_t(std::min<uint64_t>(gap, 1u << 20));
                dst.alloc_file_run(kind, step);
                gap -= step;
            }
        }
    }
    LOG_INFO("duostore meta load: {} buckets, {} objects, {} sealed packs restored",
             st.buckets, st.objects, st.sealed_packs);
    return st;
}

}  // namespace lights3::storage::duostore
