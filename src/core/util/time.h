// L4: 时间格式化/解析（HTTP Date、ISO8601、x-amz-date）
#pragma once

#include <chrono>
#include <optional>
#include <string>

namespace lights3::util {

using SysTime = std::chrono::system_clock::time_point;

// "Tue, 14 Jul 2026 08:00:00 GMT"（HTTP Date / Last-Modified）
std::string http_date(SysTime t);
// 解析 IMF-fixdate（If-Modified-Since 等条件头）；无法解析返回 nullopt
std::optional<SysTime> parse_http_date(const std::string& s);

// "2026-07-14T08:00:00.000Z"（S3 XML LastModified）
std::string iso8601(SysTime t);
// 解析 ISO8601（小数秒可选、按截断处理）；无法解析返回 nullopt
std::optional<SysTime> parse_iso8601(const std::string& s);

// "20260714T080000Z"（x-amz-date basic 格式）
std::string amz_date(SysTime t);
std::optional<SysTime> parse_amz_date(const std::string& s);

}  // namespace lights3::util
