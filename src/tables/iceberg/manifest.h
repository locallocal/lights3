// Iceberg manifest-list / manifest records (docs/architecture/s3-tables-design.md
// §7.4): the fields the deep snapshot check needs, lifted from the Avro JSON the reader
// produces. Field names follow the Iceberg spec; v1 files lacking the v2 fields read as 0.
// A missing or mistyped required field is a 409 CommitFailedException
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <vector>

#include "tables/iceberg/avro_reader.h"

namespace lights3::tables::iceberg {

struct ManifestFile {
    std::string path;
    int64_t length = 0;
    int spec_id = 0;
    // 0 data, 1 deletes
    int content = 0;
    int64_t sequence_number = 0;
    int64_t min_sequence_number = 0;
    int64_t added_snapshot_id = 0;
};

struct DataFile {
    // 0 existing, 1 added, 2 deleted
    int status = 0;
    std::string path;
    // 0 data, 1 position deletes, 2 equality deletes
    int content = 0;
    std::string format;
    int64_t size_bytes = 0;
    int64_t record_count = 0;
    std::optional<int64_t> snapshot_id;
    // data_file.sort_order_id (v2; absent → 0)
    int sort_order_id = 0;
    // null in the file for ADDED entries: inherited from the manifest when the caller
    // passes the manifest's sequence number
    std::optional<int64_t> sequence_number;
};

// Bounded by max_records (more → 409)
std::vector<ManifestFile> parse_manifest_list(avro::Reader& reader, size_t max_records);
std::vector<DataFile> parse_manifest(avro::Reader& reader, size_t max_records,
                                     std::optional<int64_t> inherited_sequence_number = std::nullopt);

}  // namespace lights3::tables::iceberg
