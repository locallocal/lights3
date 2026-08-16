// L3: shared pagination/delimiter logic for ListObjects (reused by memory and localfs)
#pragma once

#include <functional>
#include <string>
#include <vector>

#include "storage/backend.h"

namespace lights3::storage {

// sorted_keys must already be sorted lexicographically; fetch is called only for keys that
// end up in the result
ListResult apply_listing(const std::vector<std::string>& sorted_keys, const ListOptions& opt,
                         const std::function<ObjectMeta(const std::string&)>& fetch);

// Pagination semantics for the two multipart listings (docs/gaps.md §5.1): one shared
// implementation for all backends, so the "truncation rules" don't grow six different
// shapes across six backends.
// Inputs must already be sorted per their contracts (parts ascending by part_no, uploads
// ascending by (key,upload_id)); backends that can push the marker down to the engine
// (sqlite/rocks/tikv) may still push down first, fetch max+1 entries, and then hand off
// here -- the semantics are identical.
// max<=0 always returns empty with is_truncated=false -- an empty marker + truncated would
// send clients that loop on IsTruncated into an infinite loop (same as apply_listing's
// max_keys=0)
ListPartsResult apply_parts_page(std::vector<PartMeta> sorted, const ListPartsOptions& opt);
ListUploadsResult apply_uploads_page(std::vector<UploadInfo> sorted,
                                     const ListUploadsOptions& opt);

// Marker comparison: the uploads cursor is the (key, upload_id) pair
inline bool upload_after_marker(const UploadInfo& u, const ListUploadsOptions& opt) {
    if (opt.key_marker.empty() && opt.upload_id_marker.empty()) return true;
    if (u.key != opt.key_marker) return u.key > opt.key_marker;
    // Same key: an empty upload_id_marker means the whole key has already been paged past
    return opt.upload_id_marker.empty() ? false : u.upload_id > opt.upload_id_marker;
}

}  // namespace lights3::storage
