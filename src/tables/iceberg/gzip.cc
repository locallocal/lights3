#include "tables/iceberg/gzip.h"

#include <algorithm>

#include "tables/rest_error.h"

#ifdef LIGHTS3_TABLES_ZLIB
#include <zlib.h>
#endif

namespace lights3::tables::iceberg {

bool gzip_supported() {
#ifdef LIGHTS3_TABLES_ZLIB
    return true;
#else
    return false;
#endif
}

bool is_gzip_key(std::string_view key) { return key.size() > 3 && key.substr(key.size() - 3) == ".gz"; }

std::string gunzip([[maybe_unused]] std::string_view in, [[maybe_unused]] size_t max_out) {
#ifndef LIGHTS3_TABLES_ZLIB
    throw unsupported("this build cannot read gzip-compressed metadata files (no zlib)");
#else
    if (in.empty()) throw bad_request("gzip-compressed metadata file is empty");
    z_stream z{};
    // 15 window bits + 16 = expect a gzip header (not a zlib/raw stream)
    if (inflateInit2(&z, 15 + 16) != Z_OK) throw internal("zlib init failed");
    std::string out;
    // One byte of headroom over the ceiling, so a file that inflates to exactly
    // max_out still fits (the uncompressed path accepts that size too) and anything
    // larger is caught by the check after the loop
    const size_t cap = max_out + 1;
    // Start at 4x the compressed size: metadata is JSON, which gzip shrinks roughly
    // that much, so the common file inflates without a single reallocation
    out.resize(std::min(cap, std::max<size_t>(in.size() * 4, 64 * 1024)));
    z.next_in = reinterpret_cast<Bytef*>(const_cast<char*>(in.data()));
    z.avail_in = static_cast<uInt>(in.size());
    size_t produced = 0;
    int rc = Z_OK;
    auto fail = [&](const char* what) {
        inflateEnd(&z);
        throw bad_request(std::string("gzip-compressed metadata file ") + what);
    };
    while (rc != Z_STREAM_END) {
        if (produced == out.size()) {
            if (out.size() >= cap) fail("exceeds the metadata size limit once inflated");
            out.resize(std::min(cap, out.size() * 2));
        }
        z.next_out = reinterpret_cast<Bytef*>(out.data()) + produced;
        z.avail_out = static_cast<uInt>(out.size() - produced);
        rc = inflate(&z, Z_NO_FLUSH);
        produced = out.size() - z.avail_out;
        if (rc != Z_OK && rc != Z_STREAM_END && rc != Z_BUF_ERROR) fail("is corrupt");
        // no progress possible and input exhausted: the member ends mid-stream
        if (rc == Z_BUF_ERROR && z.avail_in == 0 && z.avail_out != 0) fail("is truncated");
    }
    inflateEnd(&z);
    if (produced > max_out)
        throw bad_request("gzip-compressed metadata file exceeds the metadata size limit once inflated");
    out.resize(produced);
    return out;
#endif
}

}  // namespace lights3::tables::iceberg
