// Read-only Avro Object Container File decoder (docs/architecture/s3-tables-design.md
// §7.4): the subset an Iceberg manifest-list /
// manifest needs -- OCF header (magic, metadata, sync marker), schema-driven decoding of
// every Avro type, codecs "null" and "deflate" (zlib, when built with LIGHTS3_TABLES_ZLIB).
// The whole file is in memory (the caller bounds the read); records come back as JSON.
// Malformed input is a 409 CommitFailedException (a manifest that cannot be read cannot
// be validated); an unknown codec is UnsupportedCodec so the caller can degrade
#pragma once

#include <cstddef>
#include <functional>
#include <map>
#include <memory>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <string_view>

namespace lights3::tables::iceberg::avro {

struct UnsupportedCodec : std::runtime_error {
    using std::runtime_error::runtime_error;
};

// true when the binary can inflate "deflate" blocks (zlib compiled in)
bool deflate_supported();

struct Schema;

class Reader {
public:
    // Parses the header; throws RestError(409) on a malformed file. `bytes` must outlive
    // the reader
    explicit Reader(std::string_view bytes);
    ~Reader();
    Reader(const Reader&) = delete;
    Reader& operator=(const Reader&) = delete;

    // the avro.schema text, parsed
    const nlohmann::json& schema_json() const { return schema_json_; }
    // "null" | "deflate" | whatever the writer put in avro.codec ("null" when absent)
    std::string_view codec() const { return codec_; }
    // every header entry (Iceberg stores its table schema, partition spec, ... here)
    const std::map<std::string, std::string>& meta() const { return meta_; }

    // Decodes every block and hands each record to fn as JSON: records → objects keyed by
    // field name, arrays → arrays, maps → objects, unions → the branch value, enums → the
    // symbol, bytes / fixed → binary. Stops after max_records (throws 409 when the file
    // holds more). Throws UnsupportedCodec before touching any data when the codec is not
    // decodable; RestError(409) on corruption (bad sync, truncation, depth > 64, a record
    // over 16 MiB)
    void for_each(const std::function<void(nlohmann::json&&)>& fn, size_t max_records);

    static constexpr size_t kMaxDepth = 64;
    static constexpr size_t kMaxRecordBytes = 16u << 20;
    // decompressed size of one block
    static constexpr size_t kMaxBlockBytes = 128u << 20;

private:
    std::string_view bytes_;
    std::map<std::string, std::string> meta_;
    nlohmann::json schema_json_;
    std::shared_ptr<Schema> schema_;
    std::string codec_;
    std::string sync_;
    // first block
    size_t data_pos_ = 0;
};

}  // namespace lights3::tables::iceberg::avro
