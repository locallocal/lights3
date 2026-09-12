// Shared helpers for the S3 Tables step ③ tests: the PyIceberg-written Avro fixtures under
// tests/fixtures/tables (scripts/tables/gen_fixtures.py) and a minimal Avro OCF encoder for
// hand-built containers (codec headers, malformed files)
#pragma once

#include <cstdint>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

namespace tables_fixtures {

inline std::filesystem::path dir() {
    return std::filesystem::path(__FILE__).parent_path().parent_path() / "fixtures" / "tables";
}

inline std::string slurp(const std::string& name) {
    std::ifstream in(dir() / name, std::ios::binary);
    if (!in.good()) throw std::runtime_error("missing fixture " + name);
    return std::string((std::istreambuf_iterator<char>(in)), {});
}

inline void put_long(std::string& out, int64_t v) {
    uint64_t z = (static_cast<uint64_t>(v) << 1) ^ static_cast<uint64_t>(v >> 63);
    do {
        uint8_t b = z & 0x7f;
        z >>= 7;
        if (z) b |= 0x80;
        out.push_back(static_cast<char>(b));
    } while (z);
}

inline void put_bytes(std::string& out, std::string_view s) {
    put_long(out, static_cast<int64_t>(s.size()));
    out.append(s);
}

// One-block OCF with the given schema text, codec name and pre-encoded records
inline std::string ocf(const std::string& schema, const std::string& codec, const std::vector<std::string>& records,
                       bool corrupt_sync = false) {
    std::string out("Obj\x01", 4);
    put_long(out, 2);
    put_bytes(out, "avro.schema");
    put_bytes(out, schema);
    put_bytes(out, "avro.codec");
    put_bytes(out, codec);
    put_long(out, 0);
    std::string sync = "0123456789abcdef";
    out += sync;
    std::string block;
    for (auto& r : records) block += r;
    put_long(out, static_cast<int64_t>(records.size()));
    put_bytes(out, block);
    if (corrupt_sync) sync[0] = 'X';
    out += sync;
    return out;
}

// A manifest list the reader cannot decode (snappy codec) but whose header is well formed
inline std::string snappy_manifest_list() {
    std::string schema =
        R"({"type":"record","name":"manifest_file","fields":[{"name":"manifest_path","type":"string"},{"name":"manifest_length","type":"long"}]})";
    std::string rec;
    put_bytes(rec, "s3://tbk/n/t/metadata/none.avro");
    put_long(rec, 1);
    return ocf(schema, "snappy", {rec});
}

}  // namespace tables_fixtures
