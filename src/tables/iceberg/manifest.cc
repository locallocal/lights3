#include "tables/iceberg/manifest.h"

#include "tables/rest_error.h"

namespace lights3::tables::iceberg {

using nlohmann::json;

namespace {

[[noreturn]] void bad(const char* what, const std::string& field) {
    throw commit_failed(std::string("invalid ") + what + ": field '" + field + "' is missing or has the wrong type");
}

int64_t need_long(const json& rec, const char* what, const char* field) {
    auto it = rec.find(field);
    if (it == rec.end() || !it->is_number_integer()) bad(what, field);
    return it->get<int64_t>();
}

// absent or null → 0 (v1 files), present but not an integer → 409
int64_t opt_long(const json& rec, const char* what, const char* field) {
    auto it = rec.find(field);
    if (it == rec.end() || it->is_null()) return 0;
    if (!it->is_number_integer()) bad(what, field);
    return it->get<int64_t>();
}

std::optional<int64_t> nullable_long(const json& rec, const char* what, const char* field) {
    auto it = rec.find(field);
    if (it == rec.end() || it->is_null()) return std::nullopt;
    if (!it->is_number_integer()) bad(what, field);
    return it->get<int64_t>();
}

std::string need_string(const json& rec, const char* what, const char* field) {
    auto it = rec.find(field);
    if (it == rec.end() || !it->is_string()) bad(what, field);
    return it->get<std::string>();
}

int small_int(int64_t v, const char* what, const char* field) {
    if (v < 0 || v > 1000000) bad(what, field);
    return static_cast<int>(v);
}

}  // namespace

std::vector<ManifestFile> parse_manifest_list(avro::Reader& reader, size_t max_records) {
    std::vector<ManifestFile> out;
    reader.for_each(
        [&](json&& rec) {
            if (!rec.is_object()) throw commit_failed("invalid manifest list: record is not a manifest_file");
            ManifestFile m;
            m.path = need_string(rec, "manifest list", "manifest_path");
            m.length = need_long(rec, "manifest list", "manifest_length");
            m.spec_id = small_int(opt_long(rec, "manifest list", "partition_spec_id"), "manifest list",
                                  "partition_spec_id");
            m.content = small_int(opt_long(rec, "manifest list", "content"), "manifest list", "content");
            m.sequence_number = opt_long(rec, "manifest list", "sequence_number");
            m.min_sequence_number = opt_long(rec, "manifest list", "min_sequence_number");
            m.added_snapshot_id = opt_long(rec, "manifest list", "added_snapshot_id");
            if (m.length < 0) bad("manifest list", "manifest_length");
            out.push_back(std::move(m));
        },
        max_records);
    return out;
}

std::vector<DataFile> parse_manifest(avro::Reader& reader, size_t max_records,
                                     std::optional<int64_t> inherited_sequence_number) {
    std::vector<DataFile> out;
    reader.for_each(
        [&](json&& rec) {
            if (!rec.is_object()) throw commit_failed("invalid manifest: record is not a manifest_entry");
            DataFile f;
            f.status = small_int(need_long(rec, "manifest", "status"), "manifest", "status");
            if (f.status > 2) bad("manifest", "status");
            f.snapshot_id = nullable_long(rec, "manifest", "snapshot_id");
            f.sequence_number = nullable_long(rec, "manifest", "sequence_number");
            // Iceberg inheritance: a null sequence number is the manifest's for ADDED entries
            // (and for every entry of a sequence-0 manifest, v1 → v2 upgrades)
            if (!f.sequence_number && inherited_sequence_number && (f.status == 1 || *inherited_sequence_number == 0))
                f.sequence_number = inherited_sequence_number;
            auto df = rec.find("data_file");
            if (df == rec.end() || !df->is_object()) bad("manifest", "data_file");
            f.path = need_string(*df, "manifest", "file_path");
            f.format = need_string(*df, "manifest", "file_format");
            f.content = small_int(opt_long(*df, "manifest", "content"), "manifest", "content");
            if (f.content > 2) bad("manifest", "content");
            f.size_bytes = need_long(*df, "manifest", "file_size_in_bytes");
            f.record_count = need_long(*df, "manifest", "record_count");
            if (f.size_bytes < 0 || f.record_count < 0) bad("manifest", "file_size_in_bytes");
            out.push_back(std::move(f));
        },
        max_records);
    return out;
}

}  // namespace lights3::tables::iceberg
