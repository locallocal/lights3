// L2: audit log (roadmap §3.9 ④). One JSON object per line, written to a dedicated
// rotating file separate from the operational log, so it can be shipped to a
// compliance sink without filtering. Two classes of records:
//   - control plane (always when the log is configured): credential lifecycle,
//     STS sessions, tenant/quota/ownership changes, bucket create/delete, quota
//     rejections, usage rescans. Each record is flushed immediately — these are
//     rare and are the ones an investigation needs to be durable.
//   - data plane (audit.data_plane = true): one record per S3 request with the
//     actor, tenant, bucket, key, action, status and bytes. Flushed periodically.
// Field names are stable (docs/multi-tenancy.md §5); unknown values are omitted,
// never rendered as empty strings, so consumers can rely on presence checks.
#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

#include "core/config.h"

namespace spdlog {
class logger;
}

namespace lights3::s3 {

struct AuditEvent {
    std::string_view event;       // e.g. "cred.create", "quota.reject", "access"
    std::string_view actor;       // access key (empty = anonymous / auth disabled)
    std::string_view tenant;      // actor's tenant (empty = none / root)
    std::string_view request_id;
    std::string_view trace_id;    // W3C trace id of the request (roadmap §5.4), data plane
    std::string_view bucket;
    std::string_view key;
    std::string_view target;      // object of the action: another AK, a tenant id, ...
    std::string_view action;      // read / write / delete for data-plane records
    std::string_view detail;      // free text (what changed, why refused)
    std::string_view method;      // data plane only
    std::string_view path;        // data plane only
    int status = 0;               // HTTP status (0 = not applicable)
    int64_t bytes = -1;           // -1 = not applicable
};

class AuditLog {
public:
    // nullptr when cfg.path is empty (callers treat a null log as "off")
    static std::shared_ptr<AuditLog> open(const AuditConfig& cfg);
    ~AuditLog();

    // Control-plane record: written and flushed synchronously
    void record(const AuditEvent& e);
    // Data-plane record: no-op unless audit.data_plane is on; not flushed per line
    void access(const AuditEvent& e);
    bool data_plane() const { return data_plane_; }

    // Test hook: force buffered lines to disk
    void flush();

private:
    AuditLog() = default;
    void write(const AuditEvent& e, bool flush_now);

    std::shared_ptr<spdlog::logger> logger_;
    bool data_plane_ = false;
};

}  // namespace lights3::s3
