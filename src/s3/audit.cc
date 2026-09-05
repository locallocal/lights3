#include "s3/audit.h"

#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/spdlog.h>

#include <chrono>
#include <nlohmann/json.hpp>

#include "core/log.h"
#include "core/util/time.h"

namespace lights3::s3 {

using nlohmann::json;

std::shared_ptr<AuditLog> AuditLog::open(const AuditConfig& cfg) {
    if (cfg.path.empty()) return nullptr;
    auto log = std::shared_ptr<AuditLog>(new AuditLog());
    // Own logger + own sink: the operational logger's level/pattern must not
    // apply here (an operator raising log.level to warn must not silence audit)
    auto sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
        cfg.path, cfg.max_size, static_cast<size_t>(cfg.max_files));
    log->logger_ = std::make_shared<spdlog::logger>("lights3-audit", std::move(sink));
    log->logger_->set_pattern("%v");  // the line is a complete JSON document
    log->logger_->set_level(spdlog::level::info);
    log->data_plane_ = cfg.data_plane;
    LOG_INFO("audit: writing to {} (data plane {})", cfg.path,
             cfg.data_plane ? "on" : "off");
    return log;
}

AuditLog::~AuditLog() {
    if (logger_) logger_->flush();
}

void AuditLog::record(const AuditEvent& e) { write(e, /*flush_now=*/true); }

void AuditLog::access(const AuditEvent& e) {
    if (!data_plane_) return;
    write(e, /*flush_now=*/false);
}

void AuditLog::flush() {
    if (logger_) logger_->flush();
}

void AuditLog::write(const AuditEvent& e, bool flush_now) {
    json j;
    j["ts"] = util::iso8601(std::chrono::system_clock::now());
    j["event"] = std::string(e.event);
    auto put = [&](const char* k, std::string_view v) {
        if (!v.empty()) j[k] = std::string(v);
    };
    put("actor", e.actor);
    put("tenant", e.tenant);
    put("request_id", e.request_id);
    put("trace_id", e.trace_id);
    put("bucket", e.bucket);
    put("key", e.key);
    put("target", e.target);
    put("action", e.action);
    put("method", e.method);
    put("path", e.path);
    put("detail", e.detail);
    if (e.status) j["status"] = e.status;
    if (e.bytes >= 0) j["bytes"] = e.bytes;
    logger_->info(j.dump());
    if (flush_now) logger_->flush();
}

}  // namespace lights3::s3
