// L4: 日志门面 —— 由 spdlog 实现（stderr sink，UTC 时间戳，fmt 风格占位符）
#pragma once

#include <spdlog/sinks/stdout_color_sinks.h>
#include <spdlog/spdlog.h>

namespace lights3 {

enum class LogLevel { Debug = 0, Info = 1, Warn = 2, Error = 3 };

class Logger {
public:
    static void init(LogLevel lv) {
        auto logger = std::make_shared<spdlog::logger>(
            "lights3", std::make_shared<spdlog::sinks::stderr_color_sink_mt>());
        spdlog::set_default_logger(std::move(logger));
        spdlog::set_pattern("%Y-%m-%dT%H:%M:%S.%eZ %-5!l %v", spdlog::pattern_time_type::utc);
        spdlog::set_level(to_spdlog(lv));
    }

private:
    static spdlog::level::level_enum to_spdlog(LogLevel lv) {
        switch (lv) {
            case LogLevel::Debug: return spdlog::level::debug;
            case LogLevel::Warn: return spdlog::level::warn;
            case LogLevel::Error: return spdlog::level::err;
            default: return spdlog::level::info;
        }
    }
};

#define LOG_DEBUG(...) ::spdlog::debug(__VA_ARGS__)
#define LOG_INFO(...)  ::spdlog::info(__VA_ARGS__)
#define LOG_WARN(...)  ::spdlog::warn(__VA_ARGS__)
#define LOG_ERROR(...) ::spdlog::error(__VA_ARGS__)

}  // namespace lights3
