// L4: 极简结构化日志（stderr），够用即可
#pragma once

#include <chrono>
#include <cstdio>
#include <mutex>
#include <sstream>
#include <string>

namespace lights3 {

enum class LogLevel { Debug = 0, Info = 1, Warn = 2, Error = 3 };

class Logger {
public:
    static void init(LogLevel lv) { level_ref() = lv; }
    static LogLevel level() { return level_ref(); }

    static void write(LogLevel lv, const std::string& msg) {
        if (lv < level_ref()) return;
        static const char* names[] = {"DEBUG", "INFO", "WARN", "ERROR"};
        auto now = std::chrono::system_clock::now();
        auto t = std::chrono::system_clock::to_time_t(now);
        auto ms = std::chrono::duration_cast<std::chrono::milliseconds>(
                      now.time_since_epoch()).count() % 1000;
        std::tm tm{};
        gmtime_r(&t, &tm);
        char ts[48];
        snprintf(ts, sizeof(ts), "%04d-%02d-%02dT%02d:%02d:%02d.%03dZ",
                 tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour,
                 tm.tm_min, tm.tm_sec, static_cast<int>(ms));
        static std::mutex m;
        std::lock_guard lk(m);
        fprintf(stderr, "%s %-5s %s\n", ts, names[static_cast<int>(lv)], msg.c_str());
    }

private:
    static LogLevel& level_ref() {
        static LogLevel lv = LogLevel::Info;
        return lv;
    }
};

namespace detail {
template <class... Args>
std::string log_format(Args&&... args) {
    std::ostringstream os;
    (os << ... << args);
    return os.str();
}
}  // namespace detail

#define LOG_DEBUG(...) ::lights3::Logger::write(::lights3::LogLevel::Debug, ::lights3::detail::log_format(__VA_ARGS__))
#define LOG_INFO(...)  ::lights3::Logger::write(::lights3::LogLevel::Info,  ::lights3::detail::log_format(__VA_ARGS__))
#define LOG_WARN(...)  ::lights3::Logger::write(::lights3::LogLevel::Warn,  ::lights3::detail::log_format(__VA_ARGS__))
#define LOG_ERROR(...) ::lights3::Logger::write(::lights3::LogLevel::Error, ::lights3::detail::log_format(__VA_ARGS__))

}  // namespace lights3
