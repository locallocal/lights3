// gzip (RFC 1952) decompression for table metadata files (docs/architecture/s3-tables-design.md
// §7.5): Iceberg writers with `write.metadata.compression-codec=gzip` (Spark's default
// in some distributions) name their metadata `<n>-<uuid>.gz` and store it gzip-framed.
// The catalog only ever *writes* uncompressed metadata; this is the read side, so
// register / LoadTable work against tables an engine wrote that way.
// Separate from the Avro reader's raw-deflate path (avro_reader.cc): OCF blocks are
// headerless zlib streams, a metadata file is a gzip member with its own header.
#pragma once

#include <cstddef>
#include <string>
#include <string_view>

namespace lights3::tables::iceberg {

// true when the binary was built with zlib (LIGHTS3_TABLES_ZLIB)
bool gzip_supported();

// A metadata key that is gzip-framed by its name (".gz", the Iceberg convention);
// the content is not sniffed -- the writer's own naming decides
bool is_gzip_key(std::string_view key);

// Inflates one gzip member. Throws RestError(406) when the build has no zlib, and
// RestError(400) when the input is corrupt, truncated, or inflates past max_out
// (the same ceiling the uncompressed path applies, so a compression bomb cannot
// outgrow tables.metadata_max_size)
std::string gunzip(std::string_view in, size_t max_out);

}  // namespace lights3::tables::iceberg
