// L4: logging facade — implemented by spdlog (roadmap §5.2: stderr or a size-rotated
// file, optional writer thread, text or JSON lines, UTC timestamps, fmt-style
// placeholders). The access log is a second logger sharing the same sink so a
// line-oriented consumer sees one stream, while the JSON formatter can render
// its records as top-level fields instead of a "msg" string
#pragma once

#include <spdlog/spdlog.h>

#include <memory>
#include <string_view>

namespace lights3 {

struct LogConfig;

enum class LogLevel { Debug = 0, Info = 1, Warn = 2, Error = 3 };

class Logger {
public:
    // Builds the sink described by cfg (stderr / rotating file), the formatter
    // (text / JSON) and, with cfg.async, the writer thread. Safe to call again
    // (tests, embedding): the previous queue is drained first
    static void init(const LogConfig& cfg);
    // Same wiring over a caller-supplied sink (tests capture the rendered lines)
    static void init(const LogConfig& cfg, std::shared_ptr<spdlog::sinks::sink> sink);
    // Runtime change (config hot reload, roadmap §4.4)
    static void set_level(LogLevel lv);
    static LogLevel parse_level(std::string_view s);  // unknown -> Info (config.cc rejects those first)
    // Drains the async queue and drops the writer thread; logging keeps working
    // afterwards, synchronously, on the same sink. Idempotent
    static void shutdown();

    // The access-log logger ("lights3.access"): never null, falls back to a
    // stderr logger when init() was never called (unit tests)
    static spdlog::logger& access();
    // true when access lines must be JSON objects (log.format = json): the JSON
    // formatter splices such a payload into the record instead of quoting it
    static bool json();

    static constexpr std::string_view kAccessLoggerName = "lights3.access";
};

#define LOG_DEBUG(...) ::spdlog::debug(__VA_ARGS__)
#define LOG_INFO(...)  ::spdlog::info(__VA_ARGS__)
#define LOG_WARN(...)  ::spdlog::warn(__VA_ARGS__)
#define LOG_ERROR(...) ::spdlog::error(__VA_ARGS__)

}  // namespace lights3
