#include "tables/iceberg/avro_reader.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <optional>
#include <vector>

#include "tables/rest_error.h"

#ifdef LIGHTS3_TABLES_ZLIB
#include <zlib.h>
#endif

namespace lights3::tables::iceberg::avro {

using nlohmann::json;

namespace {

[[noreturn]] void fail(const std::string& what) { throw commit_failed("invalid avro: " + what); }

}  // namespace

bool deflate_supported() {
#ifdef LIGHTS3_TABLES_ZLIB
    return true;
#else
    return false;
#endif
}

// ---------- schema ----------

struct Schema {
    enum class Kind { Null, Boolean, Int, Long, Float, Double, Bytes, String, Record, Enum, Array, Map, Union, Fixed };
    Kind kind = Kind::Null;
    std::string name;
    std::vector<std::pair<std::string, std::shared_ptr<Schema>>> fields;
    std::vector<std::string> symbols;
    std::shared_ptr<Schema> items;
    std::vector<std::shared_ptr<Schema>> branches;
    size_t size = 0;
};

namespace {

using Kind = Schema::Kind;
using Named = std::map<std::string, std::shared_ptr<Schema>>;

std::optional<Kind> primitive_kind(std::string_view t) {
    if (t == "null") return Kind::Null;
    if (t == "boolean") return Kind::Boolean;
    if (t == "int") return Kind::Int;
    if (t == "long") return Kind::Long;
    if (t == "float") return Kind::Float;
    if (t == "double") return Kind::Double;
    if (t == "bytes") return Kind::Bytes;
    if (t == "string") return Kind::String;
    return std::nullopt;
}

std::string full_name(const json& j, const std::string& enclosing_ns, std::string& own_ns) {
    if (!j.contains("name") || !j["name"].is_string()) fail("named type without a name");
    std::string name = j["name"].get<std::string>();
    if (name.find('.') != std::string::npos) {
        own_ns = name.substr(0, name.rfind('.'));
        return name;
    }
    own_ns = enclosing_ns;
    if (j.contains("namespace") && j["namespace"].is_string()) own_ns = j["namespace"].get<std::string>();
    return own_ns.empty() ? name : own_ns + "." + name;
}

std::shared_ptr<Schema> parse_schema(const json& j, Named& named, const std::string& ns, size_t depth) {
    if (depth > Reader::kMaxDepth) fail("schema nests too deep");
    if (j.is_string()) {
        std::string t = j.get<std::string>();
        if (auto k = primitive_kind(t)) {
            auto s = std::make_shared<Schema>();
            s->kind = *k;
            return s;
        }
        // named reference: qualified as written, then in the enclosing namespace
        if (auto it = named.find(t); it != named.end()) return it->second;
        if (!ns.empty())
            if (auto it = named.find(ns + "." + t); it != named.end()) return it->second;
        fail("unknown type '" + t + "'");
    }
    if (j.is_array()) {
        auto s = std::make_shared<Schema>();
        s->kind = Kind::Union;
        for (auto& b : j) s->branches.push_back(parse_schema(b, named, ns, depth + 1));
        if (s->branches.empty()) fail("empty union");
        return s;
    }
    if (!j.is_object() || !j.contains("type")) fail("schema node without a type");
    const json& t = j["type"];
    if (!t.is_string()) return parse_schema(t, named, ns, depth + 1);
    std::string type = t.get<std::string>();
    if (auto k = primitive_kind(type)) {
        // {"type":"long","logicalType":"timestamp-micros"}: the logical type is ignored
        auto s = std::make_shared<Schema>();
        s->kind = *k;
        return s;
    }
    auto s = std::make_shared<Schema>();
    if (type == "record" || type == "error") {
        s->kind = Kind::Record;
        std::string own_ns;
        s->name = full_name(j, ns, own_ns);
        // registered before the fields so recursive references resolve
        named[s->name] = s;
        if (!j.contains("fields") || !j["fields"].is_array()) fail("record '" + s->name + "' without fields");
        for (auto& f : j["fields"]) {
            if (!f.is_object() || !f.contains("name") || !f["name"].is_string() || !f.contains("type"))
                fail("malformed field in record '" + s->name + "'");
            s->fields.emplace_back(f["name"].get<std::string>(), parse_schema(f["type"], named, own_ns, depth + 1));
        }
        return s;
    }
    if (type == "enum") {
        s->kind = Kind::Enum;
        std::string own_ns;
        s->name = full_name(j, ns, own_ns);
        if (!j.contains("symbols") || !j["symbols"].is_array()) fail("enum '" + s->name + "' without symbols");
        for (auto& sym : j["symbols"]) {
            if (!sym.is_string()) fail("enum symbol is not a string");
            s->symbols.push_back(sym.get<std::string>());
        }
        named[s->name] = s;
        return s;
    }
    if (type == "fixed") {
        s->kind = Kind::Fixed;
        std::string own_ns;
        s->name = full_name(j, ns, own_ns);
        if (!j.contains("size") || !j["size"].is_number_unsigned()) fail("fixed '" + s->name + "' without a size");
        s->size = j["size"].get<size_t>();
        if (s->size > Reader::kMaxRecordBytes) fail("fixed '" + s->name + "' is too large");
        named[s->name] = s;
        return s;
    }
    if (type == "array") {
        s->kind = Kind::Array;
        if (!j.contains("items")) fail("array without items");
        s->items = parse_schema(j["items"], named, ns, depth + 1);
        return s;
    }
    if (type == "map") {
        s->kind = Kind::Map;
        if (!j.contains("values")) fail("map without values");
        s->items = parse_schema(j["values"], named, ns, depth + 1);
        return s;
    }
    fail("unknown type '" + type + "'");
}

// ---------- binary decoding ----------

struct Cursor {
    const uint8_t* p;
    const uint8_t* end;

    size_t remaining() const { return static_cast<size_t>(end - p); }
    void need(size_t n) const {
        if (remaining() < n) fail("truncated data");
    }
    int64_t read_long() {
        uint64_t v = 0;
        int shift = 0;
        for (;;) {
            need(1);
            uint8_t b = *p++;
            if (shift >= 64) fail("varint is too long");
            v |= static_cast<uint64_t>(b & 0x7f) << shift;
            if (!(b & 0x80)) break;
            shift += 7;
        }
        return static_cast<int64_t>(v >> 1) ^ -static_cast<int64_t>(v & 1);
    }
    int32_t read_int() {
        int64_t v = read_long();
        if (v < INT32_MIN || v > INT32_MAX) fail("int out of range");
        return static_cast<int32_t>(v);
    }
    // non-negative length that fits what is left
    size_t read_len() {
        int64_t n = read_long();
        if (n < 0) fail("negative length");
        need(static_cast<size_t>(n));
        return static_cast<size_t>(n);
    }
    std::string_view read_bytes(size_t n) {
        need(n);
        std::string_view v(reinterpret_cast<const char*>(p), n);
        p += n;
        return v;
    }
};

json binary_of(std::string_view v) { return json::binary(std::vector<uint8_t>(v.begin(), v.end())); }

json decode(const Schema& s, Cursor& c, size_t depth);

// The count of an array / map block: a negative count is followed by the byte size of
// the block (Spark writes these); a run of empty items is bounded so a hostile count
// cannot allocate unboundedly
size_t block_count(Cursor& c, const Schema& item) {
    int64_t n = c.read_long();
    if (n < 0) {
        if (n == INT64_MIN) fail("block count overflow");
        n = -n;
        int64_t size = c.read_long();
        if (size < 0) fail("negative block size");
        c.need(static_cast<size_t>(size));
    }
    if (item.kind == Kind::Null) {
        if (n > (1 << 20)) fail("null block is too long");
    } else if (static_cast<uint64_t>(n) > c.remaining()) {
        fail("block count exceeds the data");
    }
    return static_cast<size_t>(n);
}

json decode(const Schema& s, Cursor& c, size_t depth) {
    if (depth > Reader::kMaxDepth) fail("value nests too deep");
    switch (s.kind) {
        case Kind::Null:
            return nullptr;
        case Kind::Boolean: {
            c.need(1);
            uint8_t b = *c.p++;
            if (b > 1) fail("boolean is not 0 or 1");
            return b == 1;
        }
        case Kind::Int:
            return c.read_int();
        case Kind::Long:
            return c.read_long();
        case Kind::Float: {
            c.need(4);
            float f;
            std::memcpy(&f, c.p, 4);
            c.p += 4;
            return f;
        }
        case Kind::Double: {
            c.need(8);
            double d;
            std::memcpy(&d, c.p, 8);
            c.p += 8;
            return d;
        }
        case Kind::Bytes:
            return binary_of(c.read_bytes(c.read_len()));
        case Kind::String:
            return std::string(c.read_bytes(c.read_len()));
        case Kind::Fixed:
            return binary_of(c.read_bytes(s.size));
        case Kind::Enum: {
            int32_t idx = c.read_int();
            if (idx < 0 || static_cast<size_t>(idx) >= s.symbols.size()) fail("enum index out of range");
            return s.symbols[static_cast<size_t>(idx)];
        }
        case Kind::Union: {
            int64_t idx = c.read_long();
            if (idx < 0 || static_cast<size_t>(idx) >= s.branches.size()) fail("union index out of range");
            return decode(*s.branches[static_cast<size_t>(idx)], c, depth + 1);
        }
        case Kind::Record: {
            json o = json::object();
            for (auto& [name, fs] : s.fields) o[name] = decode(*fs, c, depth + 1);
            return o;
        }
        case Kind::Array: {
            json a = json::array();
            for (;;) {
                size_t n = block_count(c, *s.items);
                if (n == 0) break;
                for (size_t i = 0; i < n; ++i) a.push_back(decode(*s.items, c, depth + 1));
            }
            return a;
        }
        case Kind::Map: {
            json o = json::object();
            for (;;) {
                size_t n = block_count(c, *s.items);
                if (n == 0) break;
                for (size_t i = 0; i < n; ++i) {
                    std::string key(c.read_bytes(c.read_len()));
                    o[key] = decode(*s.items, c, depth + 1);
                }
            }
            return o;
        }
    }
    fail("unknown schema kind");
}

#ifdef LIGHTS3_TABLES_ZLIB
std::vector<uint8_t> inflate_raw(std::string_view in) {
    z_stream z{};
    if (inflateInit2(&z, -15) != Z_OK) fail("zlib init failed");
    std::vector<uint8_t> out;
    out.resize(std::min(Reader::kMaxBlockBytes, std::max<size_t>(in.size() * 4, 64 * 1024)));
    z.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(in.data()));
    z.avail_in = static_cast<uInt>(in.size());
    size_t produced = 0;
    int rc = Z_OK;
    while (rc != Z_STREAM_END) {
        if (produced == out.size()) {
            if (out.size() >= Reader::kMaxBlockBytes) {
                inflateEnd(&z);
                fail("deflate block exceeds the size limit");
            }
            out.resize(std::min(Reader::kMaxBlockBytes, out.size() * 2));
        }
        z.next_out = out.data() + produced;
        z.avail_out = static_cast<uInt>(out.size() - produced);
        rc = inflate(&z, Z_NO_FLUSH);
        produced = out.size() - z.avail_out;
        if (rc != Z_OK && rc != Z_STREAM_END && rc != Z_BUF_ERROR) {
            inflateEnd(&z);
            fail("deflate block is corrupt");
        }
        if (rc == Z_BUF_ERROR && z.avail_in == 0 && z.avail_out != 0) {
            inflateEnd(&z);
            fail("deflate block is truncated");
        }
    }
    inflateEnd(&z);
    out.resize(produced);
    return out;
}
#endif

}  // namespace

// ---------- reader ----------

Reader::Reader(std::string_view bytes) : bytes_(bytes) {
    Cursor c{reinterpret_cast<const uint8_t*>(bytes.data()),
             reinterpret_cast<const uint8_t*>(bytes.data()) + bytes.size()};
    if (c.read_bytes(4) != std::string_view("Obj\x01", 4)) fail("bad magic");
    Schema str;
    str.kind = Kind::String;
    for (;;) {
        size_t n = block_count(c, str);
        if (n == 0) break;
        for (size_t i = 0; i < n; ++i) {
            std::string k(c.read_bytes(c.read_len()));
            std::string v(c.read_bytes(c.read_len()));
            meta_[k] = std::move(v);
        }
    }
    sync_ = std::string(c.read_bytes(16));
    data_pos_ = static_cast<size_t>(c.p - reinterpret_cast<const uint8_t*>(bytes.data()));
    auto sit = meta_.find("avro.schema");
    if (sit == meta_.end()) fail("header has no avro.schema");
    try {
        schema_json_ = json::parse(sit->second);
    } catch (const json::exception& e) {
        fail(std::string("avro.schema is not JSON: ") + e.what());
    }
    Named named;
    schema_ = parse_schema(schema_json_, named, "", 0);
    auto cit = meta_.find("avro.codec");
    codec_ = cit == meta_.end() || cit->second.empty() ? "null" : cit->second;
}

Reader::~Reader() = default;

void Reader::for_each(const std::function<void(json&&)>& fn, size_t max_records) {
    bool deflate = codec_ == "deflate";
    if (codec_ != "null" && !deflate) throw UnsupportedCodec("avro codec '" + codec_ + "' is not supported");
    if (deflate && !deflate_supported()) throw UnsupportedCodec("avro codec 'deflate' needs zlib, not built in");
    Cursor file{reinterpret_cast<const uint8_t*>(bytes_.data()) + data_pos_,
                reinterpret_cast<const uint8_t*>(bytes_.data()) + bytes_.size()};
    size_t records = 0;
    std::vector<uint8_t> inflated;
    while (file.remaining() > 0) {
        int64_t count = file.read_long();
        if (count < 0) fail("negative record count");
        size_t size = file.read_len();
        std::string_view raw = file.read_bytes(size);
        if (file.read_bytes(16) != sync_) fail("sync marker mismatch");
        if (records + static_cast<size_t>(count) > max_records) fail("more records than allowed");
        std::string_view data = raw;
        if (deflate) {
#ifdef LIGHTS3_TABLES_ZLIB
            inflated = inflate_raw(raw);
            data = std::string_view(reinterpret_cast<const char*>(inflated.data()), inflated.size());
#endif
        }
        Cursor block{reinterpret_cast<const uint8_t*>(data.data()),
                     reinterpret_cast<const uint8_t*>(data.data()) + data.size()};
        for (int64_t i = 0; i < count; ++i) {
            const uint8_t* start = block.p;
            json rec = decode(*schema_, block, 0);
            if (static_cast<size_t>(block.p - start) > kMaxRecordBytes) fail("record exceeds the size limit");
            ++records;
            fn(std::move(rec));
        }
        if (block.remaining() != 0) fail("block has trailing bytes");
    }
}

}  // namespace lights3::tables::iceberg::avro
