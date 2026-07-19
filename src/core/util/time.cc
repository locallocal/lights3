#include "core/util/time.h"

#include <cstdio>
#include <ctime>
#include <string_view>

namespace lights3::util {

namespace {
std::tm to_utc_tm(SysTime t) {
    auto tt = std::chrono::system_clock::to_time_t(t);
    std::tm tm{};
    gmtime_r(&tt, &tm);
    return tm;
}
}  // namespace

std::string http_date(SysTime t) {
    auto tm = to_utc_tm(t);
    char buf[64];
    strftime(buf, sizeof(buf), "%a, %d %b %Y %H:%M:%S GMT", &tm);
    return buf;
}

std::optional<SysTime> parse_http_date(const std::string& s) {
    static const char* months[] = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
    char mon[4] = {0};
    std::tm tm{};
    // "Tue, 14 Jul 2026 08:00:00 GMT"（星期与 GMT 后缀不严格校验）
    if (sscanf(s.c_str(), "%*3s, %2d %3s %4d %2d:%2d:%2d", &tm.tm_mday, mon, &tm.tm_year,
               &tm.tm_hour, &tm.tm_min, &tm.tm_sec) != 6)
        return std::nullopt;
    tm.tm_mon = -1;
    for (int i = 0; i < 12; ++i)
        if (std::string_view(mon) == months[i]) tm.tm_mon = i;
    if (tm.tm_mon < 0) return std::nullopt;
    tm.tm_year -= 1900;
    time_t tt = timegm(&tm);
    if (tt == static_cast<time_t>(-1)) return std::nullopt;
    return std::chrono::system_clock::from_time_t(tt);
}

std::string iso8601(SysTime t) {
    auto tm = to_utc_tm(t);
    char buf[48];
    snprintf(buf, sizeof(buf), "%04d-%02d-%02dT%02d:%02d:%02d.000Z", tm.tm_year + 1900,
             tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
    return buf;
}

std::optional<SysTime> parse_iso8601(const std::string& s) {
    std::tm tm{};
    // "2026-07-14T08:00:00[.sss]Z"（小数秒与 Z 后缀不严格校验）
    if (sscanf(s.c_str(), "%4d-%2d-%2dT%2d:%2d:%2d", &tm.tm_year, &tm.tm_mon, &tm.tm_mday,
               &tm.tm_hour, &tm.tm_min, &tm.tm_sec) != 6)
        return std::nullopt;
    tm.tm_year -= 1900;
    tm.tm_mon -= 1;
    time_t tt = timegm(&tm);
    if (tt == static_cast<time_t>(-1)) return std::nullopt;
    return std::chrono::system_clock::from_time_t(tt);
}

std::string amz_date(SysTime t) {
    auto tm = to_utc_tm(t);
    char buf[40];
    snprintf(buf, sizeof(buf), "%04d%02d%02dT%02d%02d%02dZ", tm.tm_year + 1900, tm.tm_mon + 1,
             tm.tm_mday, tm.tm_hour, tm.tm_min, tm.tm_sec);
    return buf;
}

std::optional<SysTime> parse_amz_date(const std::string& s) {
    std::tm tm{};
    if (sscanf(s.c_str(), "%4d%2d%2dT%2d%2d%2dZ", &tm.tm_year, &tm.tm_mon, &tm.tm_mday,
               &tm.tm_hour, &tm.tm_min, &tm.tm_sec) != 6)
        return std::nullopt;
    tm.tm_year -= 1900;
    tm.tm_mon -= 1;
    time_t tt = timegm(&tm);
    if (tt == static_cast<time_t>(-1)) return std::nullopt;
    return std::chrono::system_clock::from_time_t(tt);
}

}  // namespace lights3::util
