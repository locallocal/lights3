#include "storage/duostore/codec.h"

#include "core/util/checksum.h"

#include <array>

#include "s3/errors.h"

namespace lights3::storage::duostore::codec {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

[[noreturn]] void corrupt(const char* what) {
    throw S3Error(S3ErrorCode::InternalError,
                  std::string("duostore: corrupt meta value: ") + what);
}

// '\0' 分隔编码成立的前提（§4.1）：共享校验层已拒绝 key 含 NUL、bucket 名限
// [a-z0-9.-]。这里是防御纵深——任何含 NUL 的段进入 key 编码都意味着上游校验被
// 绕过，继续编码会产生跨记录 key 碰撞（静默数据损坏），必须响亮失败。
void require_no_nul(std::string_view part) {
    if (part.find('\0') != std::string_view::npos)
        throw S3Error(S3ErrorCode::InternalError,
                      "duostore: key component contains NUL (validation bypassed)");
}

// ---- 小端整数与带长度前缀字符串 ----

void put_u8(std::string& s, uint8_t v) { s.push_back(char(v)); }
void put_u16(std::string& s, uint16_t v) {
    for (int i = 0; i < 2; ++i) s.push_back(char(v >> (8 * i)));
}
void put_u32(std::string& s, uint32_t v) {
    for (int i = 0; i < 4; ++i) s.push_back(char(v >> (8 * i)));
}
void put_u64(std::string& s, uint64_t v) {
    for (int i = 0; i < 8; ++i) s.push_back(char(v >> (8 * i)));
}
// 编码侧超限是**请求**问题而非库损坏（docs/gaps.md §4）：用户提交超长
// user-meta 应得到 400，而不是 500 "corrupt meta value"
[[noreturn]] void too_large(const char* what) {
    throw S3Error(S3ErrorCode::InvalidArgument,
                  std::string("Metadata field too large: ") + what);
}

void put_str(std::string& s, std::string_view v) {
    if (v.size() > 0xffff) too_large("string field exceeds 64KiB");
    put_u16(s, uint16_t(v.size()));
    s.append(v);
}

struct Cursor {
    std::string_view s;
    size_t pos = 0;

    void need(size_t n) {
        if (s.size() - pos < n) corrupt("truncated");
    }
    uint8_t u8() {
        need(1);
        return uint8_t(s[pos++]);
    }
    uint16_t u16() {
        need(2);
        uint16_t v = 0;
        for (int i = 0; i < 2; ++i) v |= uint16_t(uint8_t(s[pos++])) << (8 * i);
        return v;
    }
    uint32_t u32() {
        need(4);
        uint32_t v = 0;
        for (int i = 0; i < 4; ++i) v |= uint32_t(uint8_t(s[pos++])) << (8 * i);
        return v;
    }
    uint64_t u64() {
        need(8);
        uint64_t v = 0;
        for (int i = 0; i < 8; ++i) v |= uint64_t(uint8_t(s[pos++])) << (8 * i);
        return v;
    }
    std::string_view str() {
        size_t n = u16();
        need(n);
        auto v = s.substr(pos, n);
        pos += n;
        return v;
    }
    void skip(size_t n) {
        need(n);
        pos += n;
    }
    void done() {
        if (pos != s.size()) corrupt("trailing bytes");
    }
};

void check_ver(Cursor& c, uint8_t expect) {
    if (c.u8() != expect) corrupt("unsupported value version");
}

// 版本兼容读（docs/gaps.md §5.2）：v1 记录无一等元数据段，v2 起有。严格相等的
// check_ver 让旧值一律变成 500 "corrupt"，升级得停机重写全量元数据
uint8_t read_ver(Cursor& c, uint8_t max) {
    uint8_t v = c.u8();
    if (v == 0 || v > max) corrupt("unsupported value version");
    return v;
}

// ---- extent run 编解码（§4.3）----
// run = { u8 kind, u64 first_file_id, u32 count, u64 chunk_len, u64 last_len,
//         u64 pack_offset, u32 crc[count] }
// 合并条件：同 kind（chunk/rados，二者形态同构）连续 file_id 且前一段为满长
// （run 中除末段外长度必须一致）；pack extent 不合并（count 恒 1）。

void append_extent_runs(std::string& out, const std::vector<Extent>& extents) {
    struct Run {
        Extent::Kind kind;
        uint64_t first_id;
        uint32_t count;
        uint64_t chunk_len;
        uint64_t last_len;
        uint64_t pack_offset;
        std::vector<uint32_t> crcs;
    };
    std::vector<Run> runs;
    for (const auto& e : extents) {
        if (!runs.empty()) {
            Run& r = runs.back();
            if (r.kind == e.kind && e.kind != Extent::Kind::kPack &&
                e.file_id == r.first_id + r.count && e.offset == 0 &&
                r.last_len == r.chunk_len) {
                ++r.count;
                r.last_len = e.length;
                r.crcs.push_back(e.crc32c);
                continue;
            }
        }
        runs.push_back({e.kind, e.file_id, 1, e.length, e.length, e.offset, {e.crc32c}});
    }
    put_u32(out, uint32_t(runs.size()));
    for (const auto& r : runs) {
        put_u8(out, uint8_t(r.kind));
        put_u64(out, r.first_id);
        put_u32(out, r.count);
        put_u64(out, r.chunk_len);
        put_u64(out, r.last_len);
        put_u64(out, r.pack_offset);
        for (uint32_t c : r.crcs) put_u32(out, c);
    }
}

// 算术跳过 runs 段（run 头定长 33B + 4B×count 的 crc 数组），不物化 Extent
void skip_extent_runs(Cursor& c) {
    uint32_t n_runs = c.u32();
    if (size_t(n_runs) * 33 > c.s.size() - c.pos) corrupt("run count beyond payload");
    for (uint32_t i = 0; i < n_runs; ++i) {
        c.u8();   // kind
        c.u64();  // first_file_id
        uint32_t count = c.u32();
        c.u64();  // chunk_len
        c.u64();  // last_len
        c.u64();  // pack_offset
        c.skip(size_t(count) * 4);
    }
}

std::vector<Extent> read_extent_runs(Cursor& c) {
    uint32_t n_runs = c.u32();
    // n_runs 是裸 u32：每个 run 至少占 33B 头，先按剩余载荷卡掉离谱值。否则
    // 损坏的长度字段要先把 out 撑到几十万条，才轮到 need() 在半路抛（§4）
    if (size_t(n_runs) * 33 > c.s.size() - c.pos) corrupt("run count beyond payload");
    std::vector<Extent> out;
    out.reserve(n_runs);
    for (uint32_t i = 0; i < n_runs; ++i) {
        uint8_t kind = c.u8();
        if (kind > uint8_t(Extent::Kind::kRados)) corrupt("unknown extent kind");
        uint64_t first_id = c.u64();
        uint32_t count = c.u32();
        if (count == 0) corrupt("empty run");
        // 编码约定 pack 不合并（count 恒 1）；count 还必须被剩余 crc 数组字节
        // 覆盖——损坏值不得解出一串假 extent（docs/gaps.md §4）
        if (kind == uint8_t(Extent::Kind::kPack) && count != 1) corrupt("pack run count");
        if (size_t(count) * 4 > c.s.size() - c.pos) corrupt("run count beyond payload");
        uint64_t chunk_len = c.u64();
        uint64_t last_len = c.u64();
        uint64_t pack_offset = c.u64();
        for (uint32_t j = 0; j < count; ++j) {
            Extent e;
            e.kind = Extent::Kind(kind);
            e.file_id = first_id + j;
            e.offset = e.kind == Extent::Kind::kPack ? pack_offset : 0;
            e.length = (j + 1 < count) ? chunk_len : last_len;
            e.crc32c = c.u32();
            out.push_back(e);
        }
    }
    return out;
}

void put_user_meta(std::string& s, const std::map<std::string, std::string>& m) {
    if (m.size() > 0xffff) too_large("too many user metadata entries");
    put_u16(s, uint16_t(m.size()));
    for (const auto& [k, v] : m) {
        put_str(s, k);
        put_str(s, v);
    }
}

std::map<std::string, std::string> read_user_meta(Cursor& c) {
    std::map<std::string, std::string> m;
    uint16_t n = c.u16();
    for (uint16_t i = 0; i < n; ++i) {
        std::string k(c.str());
        m[std::move(k)] = std::string(c.str());
    }
    return m;
}

// 一等元数据段（v2 起）：自描述的 u16 n + (str k, str v)*，只写非空项。做成
// kv 而非六个定长槽位，是为了让下次加字段不必再动版本号——未知键读到就丢
void put_std_meta(std::string& s, const ObjectMeta& m) {
    uint16_t n = 0;
    for (auto& f : kStdMetaFields)
        if (!(m.*f.field).empty()) ++n;
    put_u16(s, n);
    for (auto& f : kStdMetaFields)
        if (!(m.*f.field).empty()) {
            put_str(s, f.store_key);
            put_str(s, m.*f.field);
        }
}

void read_std_meta(Cursor& c, ObjectMeta& m) {
    uint16_t n = c.u16();
    for (uint16_t i = 0; i < n; ++i) {
        std::string k(c.str());
        std::string v(c.str());
        for (auto& f : kStdMetaFields)
            if (k == f.store_key) m.*f.field = std::move(v);
    }
}

}  // namespace

// ---- crc32c ----
// 实现已上提到 core/util/checksum.h（S3 的 x-amz-checksum-crc32c 同用一份），
// 这里保留 duostore 命名空间下的转发，调用点不动
uint32_t crc32c_update(uint32_t crc, std::span<const std::byte> data) {
    return util::crc32c_update(crc, data);
}

// ---- key 编码 ----

std::string object_key(std::string_view bucket, std::string_view key) {
    require_no_nul(bucket);
    require_no_nul(key);
    std::string s;
    s.reserve(bucket.size() + 1 + key.size());
    s.append(bucket);
    s.push_back('\0');
    s.append(key);
    return s;
}

std::string upload_key(std::string_view bucket, std::string_view key, std::string_view id) {
    require_no_nul(id);
    std::string s = object_key(bucket, key);
    s.push_back('\0');
    s.append(id);
    return s;
}

std::string parts_prefix(std::string_view bucket, std::string_view key, std::string_view id) {
    std::string s = upload_key(bucket, key, id);
    s.push_back('\0');
    return s;
}

std::string part_key(std::string_view bucket, std::string_view key, std::string_view id,
                     int part_no) {
    std::string s = parts_prefix(bucket, key, id);
    s.push_back(char(uint8_t(part_no >> 8)));  // big-endian：升序即 part_no 升序
    s.push_back(char(uint8_t(part_no)));
    return s;
}

int part_no_of_key(std::string_view parts_cf_key) {
    if (parts_cf_key.size() < 2) corrupt("parts key too short");
    return int(uint8_t(parts_cf_key[parts_cf_key.size() - 2])) << 8 |
           int(uint8_t(parts_cf_key.back()));
}

std::string be64_key(uint64_t v) {
    std::string s(8, '\0');
    for (int i = 0; i < 8; ++i) s[i] = char(v >> (8 * (7 - i)));
    return s;
}

uint64_t parse_be64(std::string_view k) {
    if (k.size() != 8) corrupt("be64 key size");
    uint64_t v = 0;
    for (int i = 0; i < 8; ++i) v = v << 8 | uint8_t(k[i]);
    return v;
}

// ---- extent 数组（供测试观察 run 压缩）----

std::string encode_extents(const std::vector<Extent>& extents) {
    std::string s;
    append_extent_runs(s, extents);
    return s;
}

std::vector<Extent> decode_extents(std::string_view v) {
    Cursor c{v};
    auto out = read_extent_runs(c);
    c.done();
    return out;
}

// ---- bucket ----

std::string encode_bucket(int64_t created_ms) {
    std::string s;
    put_u8(s, 1);
    put_u64(s, uint64_t(created_ms));
    return s;
}

int64_t decode_bucket(std::string_view v) {
    Cursor c{v};
    check_ver(c, 1);
    int64_t ms = int64_t(c.u64());
    c.done();
    return ms;
}

// ---- object：u8 ver | u64 size | u64 mtime_ms | u64 version | str etag
//              | str content_type | u16 n_meta (str k, str v)*
//              | [v2] u16 n_std (str k, str v)* | runs（§4.2）----
// v1 = 无一等元数据段；读侧两版都认，写侧恒 v2（存量记录原地可读，无需重写）

constexpr uint8_t kObjectVer = 2;
constexpr uint8_t kUploadVer = 2;

std::string encode_object(const ObjectRec& rec) {
    std::string s;
    put_u8(s, kObjectVer);
    put_u64(s, rec.meta.size);
    put_u64(s, uint64_t(to_unix_ms(rec.meta.last_modified)));
    put_u64(s, rec.version);
    put_str(s, rec.meta.etag);
    put_str(s, rec.meta.content_type);
    put_user_meta(s, rec.meta.user_meta);
    put_std_meta(s, rec.meta);
    append_extent_runs(s, rec.data.extents);
    return s;
}

ObjectMeta decode_object_meta(std::string key, std::string_view v) {
    Cursor c{v};
    uint8_t ver = read_ver(c, kObjectVer);
    ObjectMeta m;
    m.key = std::move(key);
    m.size = c.u64();
    m.last_modified = from_unix_ms(int64_t(c.u64()));
    c.u64();  // version
    m.etag = std::string(c.str());
    m.content_type = std::string(c.str());
    m.user_meta = read_user_meta(c);
    if (ver >= 2) read_std_meta(c, m);
    skip_extent_runs(c);  // list 不需要定位信息，免物化大对象的 Extent 数组（§4.4）
    c.done();
    return m;
}

ObjectRec decode_object(std::string key, std::string_view v) {
    Cursor c{v};
    uint8_t ver = read_ver(c, kObjectVer);
    ObjectRec rec;
    rec.meta.key = std::move(key);
    rec.meta.size = c.u64();
    rec.meta.last_modified = from_unix_ms(int64_t(c.u64()));
    rec.version = c.u64();
    rec.meta.etag = std::string(c.str());
    rec.meta.content_type = std::string(c.str());
    rec.meta.user_meta = read_user_meta(c);
    if (ver >= 2) read_std_meta(c, rec.meta);
    rec.data.extents = read_extent_runs(c);
    c.done();
    return rec;
}

// ---- upload：u8 ver | i64 initiated_ms | str content_type | u16 n_meta kv*
//              | [v2] u16 n_std kv* ----

std::string encode_upload(const UploadRec& rec) {
    std::string s;
    put_u8(s, kUploadVer);
    put_u64(s, uint64_t(rec.initiated_ms));
    put_str(s, rec.meta.content_type);
    put_user_meta(s, rec.meta.user_meta);
    put_std_meta(s, rec.meta);
    return s;
}

UploadRec decode_upload(std::string key, std::string upload_id, std::string_view v) {
    Cursor c{v};
    uint8_t ver = read_ver(c, kUploadVer);
    UploadRec rec;
    rec.upload_id = std::move(upload_id);
    rec.meta.key = std::move(key);
    rec.initiated_ms = int64_t(c.u64());
    rec.meta.content_type = std::string(c.str());
    rec.meta.user_meta = read_user_meta(c);
    if (ver >= 2) read_std_meta(c, rec.meta);
    c.done();
    return rec;
}

// ---- part：u8 ver | u64 size | str md5 | i64 modified_ms | runs ----

std::string encode_part(const PartRec& rec) {
    std::string s;
    put_u8(s, 1);
    put_u64(s, rec.size);
    put_str(s, rec.etag);
    put_u64(s, uint64_t(rec.modified_ms));
    append_extent_runs(s, rec.data.extents);
    return s;
}

PartRec decode_part(int part_no, std::string_view v) {
    Cursor c{v};
    check_ver(c, 1);
    PartRec rec;
    rec.part_no = part_no;
    rec.size = c.u64();
    rec.etag = std::string(c.str());
    rec.modified_ms = int64_t(c.u64());
    rec.data.extents = read_extent_runs(c);
    c.done();
    return rec;
}

// ---- gcq：u8 ver | u8 reason | i64 enqueue_ms | runs ----

std::string encode_reclaim(const Reclaim& r, int64_t enqueue_ms) {
    std::string s;
    put_u8(s, 1);
    put_u8(s, 0);  // reason：预留（覆盖/删除/abort，暂未区分）
    put_u64(s, uint64_t(enqueue_ms));
    append_extent_runs(s, r.extents);
    return s;
}

Reclaim decode_reclaim(std::string_view v, int64_t* enqueue_ms) {
    Cursor c{v};
    check_ver(c, 1);
    c.u8();  // reason
    int64_t ms = int64_t(c.u64());
    if (enqueue_ms) *enqueue_ms = ms;
    Reclaim r{read_extent_runs(c), ms};
    c.done();
    return r;
}

// ---- stats 计数器 ----

std::string encode_counter_delta(int64_t d) {
    std::string s;
    put_u64(s, uint64_t(d));
    return s;
}

int64_t decode_counter(std::string_view v) {
    Cursor c{v};
    int64_t d = int64_t(c.u64());
    c.done();
    return d;
}

}  // namespace lights3::storage::duostore::codec
