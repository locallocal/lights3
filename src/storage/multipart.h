// L3: backend-agnostic multipart helpers (upload_id generation, combined ETag, parts validation)
#pragma once

#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "storage/backend.h"

namespace lights3::storage {

// 32 random hex characters; used as the upload_id (and as localfs's mpu directory name)
std::string new_upload_id();
bool is_valid_upload_id(std::string_view id);

// S3 multipart combined-ETag rule: hex of md5(concatenation of each part's binary md5) + "-N"
std::string combined_etag(const std::vector<std::string>& part_md5_hex);

// Composite object checksum (roadmap §2.2): base64(H(concatenation of each part's raw
// digest)) + "-N", H selected by the uppercase wire name (CRC32/CRC32C/SHA1/SHA256).
// nullopt when the algorithm is unknown/unsupported for composites (CRC64NVME is
// full-object only) or any part value fails to base64-decode — callers then record no
// checksum rather than a wrong one
std::optional<std::string> composite_checksum(std::string_view algorithm,
                                              const std::vector<std::string>& part_values_b64);

// Shared complete-time hook: when every part carries a verified checksum under one
// algorithm, fill meta.checksum_* (COMPOSITE) and the result echo; otherwise leave both
// untouched. part_checksums[i] pairs with part i of the completed list
struct PartDigest {
    std::string algorithm;
    std::string value;  // base64
};
void apply_composite_checksum(const std::vector<PartDigest>& parts, ObjectMeta& meta,
                              PutResult& result);

// Strip the quotes around an ETag (clients may send the W3C quoted form)
std::string_view strip_etag_quotes(std::string_view etag);

// Pre-checks for complete: empty parts throws InvalidPart, non-strictly-increasing part
// numbers throw InvalidPartOrder
void validate_part_order(std::span<const PartInfo> parts);

// part_no ∈ [1,kMaxParts], otherwise throws InvalidArgument
void validate_part_number(int part_no);

// AWS hard limits (docs/archive/gaps.md §5.7). The minimum part size is only judged at complete,
// and it is an S3 protocol rule rather than a storage rule -- so the check lives at L2
// (handlers/multipart.cc) and the storage layer imposes no limit: callers using the
// backend API directly (including each backend's consistency suite) are not bound by 5MiB
inline constexpr int kMaxParts = 10000;
inline constexpr uint64_t kMinPartSize = 5ull * 1024 * 1024;

}  // namespace lights3::storage
