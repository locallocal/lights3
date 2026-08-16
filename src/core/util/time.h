// L4: time formatting/parsing (HTTP Date, ISO8601, x-amz-date)
#pragma once

#include <chrono>
#include <optional>
#include <string>

namespace lights3::util {

using SysTime = std::chrono::system_clock::time_point;

// "Tue, 14 Jul 2026 08:00:00 GMT"（HTTP Date / Last-Modified）
std::string http_date(SysTime t);
// Parse IMF-fixdate (conditional headers like If-Modified-Since); returns nullopt when unparseable
std::optional<SysTime> parse_http_date(const std::string& s);

// "2026-07-14T08:00:00.000Z"（S3 XML LastModified）
std::string iso8601(SysTime t);
// Parse ISO8601 (fractional seconds optional, truncated); returns nullopt when unparseable
std::optional<SysTime> parse_iso8601(const std::string& s);

// "20260714T080000Z" (x-amz-date basic format)
std::string amz_date(SysTime t);
std::optional<SysTime> parse_amz_date(const std::string& s);

}  // namespace lights3::util
