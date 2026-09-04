// L2: bucket quota configuration (roadmap §3.9 ②, docs/multi-tenancy.md §3).
// Persisted as .sys/quota/<bucket> through SysConfigStore; managed by the ?quota
// subresource (root credential) and read by the enforcement gate in the write
// handlers. Enforcement itself needs live counters and therefore lives next to
// UsageTracker (S3Service::check_quota); this header is only the stored shape.
#pragma once

#include <cstdint>
#include <optional>
#include <string>
#include <string_view>

#include "s3/sys_config_store.h"

namespace lights3::s3 {

// 0 = unlimited on that axis; an entry with both zero is refused at PUT (delete it instead)
struct BucketQuota {
    uint64_t max_bytes = 0;    // committed object bytes + in-flight multipart part bytes
    uint64_t max_objects = 0;  // committed objects
    bool operator==(const BucketQuota&) const = default;
};

struct QuotaTraits {
    using Entry = BucketQuota;
    static constexpr std::string_view kPrefix = "quota/";
    static constexpr const char* kName = "quota";
    static std::string serialize(const Entry& q);
    static std::optional<Entry> deserialize(const std::string& bucket, const std::string& body);
    static bool differs(const Entry& a, const Entry& b) { return !(a == b); }
};
using QuotaStore = SysConfigStore<QuotaTraits>;

// XML shape of the ?quota subresource (no AWS equivalent; RGW/MinIO expose quotas
// through their own admin APIs). Parse throws MalformedXML / InvalidRequest
std::string quota_xml(const BucketQuota& q);
BucketQuota parse_quota_xml(const std::string& body);

}  // namespace lights3::s3
