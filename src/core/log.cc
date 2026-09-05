#include "core/log.h"

#include <spdlog/async.h>
#include <spdlog/details/registry.h>
#include <spdlog/formatter.h>
#include <spdlog/sinks/rotating_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

#include <chrono>
#include <cstdio>
#include <ctime>
#include <atomic>
#include <mutex>
#include <string>
#include <vector>

#include "core/config.h"

namespace lights3 {

namespace {

constexpr const char* kMainLoggerName = "lights3";
constexpr const char* kTextPattern = "%Y-%m-%dT%H:%M:%S.%eZ %-5!l %v";

std::mutex g_mu;
std::shared_ptr<spdlog::logger> g_access;  // the registered access logger (null before init)
std::atomic<bool> g_json{false};
bool g_async = false;

void append(spdlog::memory_buf_t& dest, std::string_view s) {
    dest.append(s.data(), s.data() + s.size());
}

// JSON string escaping per RFC 8259: the two mandatory escapes plus control
// characters; everything else (UTF-8 included) passes through
void append_json_string(spdlog::memory_buf_t& dest, std::string_view s) {
    dest.push_back('"');
    for (unsigned char c : s) {
        switch (c) {
            case '"': append(dest, "\\\""); break;
            case '\\': append(dest, "\\\\"); break;
            case '\n': append(dest, "\\n"); break;
            case '\r': append(dest, "\\r"); break;
            case '\t': append(dest, "\\t"); break;
            default:
                if (c < 0x20) {
                    char buf[8];
                    std::snprintf(buf, sizeof(buf), "\\u%04x", c);
                    append(dest, buf);
                } else {
                    dest.push_back(static_cast<char>(c));
                }
        }
    }
    dest.push_back('"');
}

// The config vocabulary (log.level: debug|info|warn|error) rather than spdlog's
// "warning"/"err", so a consumer filters on the names the operator configured
std::string_view level_name(spdlog::level::level_enum lv) {
    switch (lv) {
        case spdlog::level::trace: return "trace";
        case spdlog::level::debug: return "debug";
        case spdlog::level::info: return "info";
        case spdlog::level::warn: return "warn";
        case spdlog::level::err: return "error";
        case spdlog::level::critical: return "critical";
        default: return "off";
    }
}

// One JSON object per line: {"ts":"<UTC, ms>","level":"info","thread":<id>,"msg":"..."}.
// Records of the access logger arrive as a complete JSON object in the payload
// (S3Service renders them with the fields it has); the object's members are
// spliced in after the envelope so the consumer sees flat top-level fields
class JsonFormatter final : public spdlog::formatter {
public:
    void format(const spdlog::details::log_msg& msg, spdlog::memory_buf_t& dest) override {
        auto ms_total = std::chrono::duration_cast<std::chrono::milliseconds>(
                            msg.time.time_since_epoch())
                            .count();
        std::time_t secs = static_cast<std::time_t>(ms_total / 1000);
        int ms = static_cast<int>(ms_total % 1000);
        std::tm tm{};
        gmtime_r(&secs, &tm);
        char buf[64];
        std::snprintf(buf, sizeof(buf), "{\"ts\":\"%04d-%02d-%02dT%02d:%02d:%02d.%03dZ\",",
                      tm.tm_year + 1900, tm.tm_mon + 1, tm.tm_mday, tm.tm_hour, tm.tm_min,
                      tm.tm_sec, ms);
        append(dest, buf);
        append(dest, "\"level\":\"");
        append(dest, level_name(msg.level));
        append(dest, "\",\"thread\":");
        append(dest, std::to_string(msg.thread_id));
        std::string_view name(msg.logger_name.data(), msg.logger_name.size());
        std::string_view payload(msg.payload.data(), msg.payload.size());
        if (name == Logger::kAccessLoggerName && payload.size() >= 2 && payload.front() == '{' &&
            payload.back() == '}') {
            append(dest, ",\"msg\":\"access\"");
            if (payload.size() > 2) {
                dest.push_back(',');
                append(dest, payload.substr(1));  // members + closing brace
            } else {
                dest.push_back('}');
            }
        } else {
            append(dest, ",\"msg\":");
            append_json_string(dest, payload);
            dest.push_back('}');
        }
        dest.push_back('\n');
    }
    std::unique_ptr<spdlog::formatter> clone() const override {
        return std::make_unique<JsonFormatter>();
    }
};

spdlog::level::level_enum to_spdlog(LogLevel lv) {
    switch (lv) {
        case LogLevel::Debug: return spdlog::level::debug;
        case LogLevel::Warn: return spdlog::level::warn;
        case LogLevel::Error: return spdlog::level::err;
        default: return spdlog::level::info;
    }
}

void apply_format(spdlog::logger& l, bool json) {
    if (json) l.set_formatter(std::make_unique<JsonFormatter>());
    else l.set_pattern(kTextPattern, spdlog::pattern_time_type::utc);
}

// Replaces the registered pair (main + access) with synchronous loggers over the
// given sinks and drops the writer thread: its destructor drains the queue and
// joins, so every record enqueued before the call reaches the sink. Caller holds g_mu
void install_sync(std::vector<spdlog::sink_ptr> sinks, bool json,
                  spdlog::level::level_enum level) {
    auto main = std::make_shared<spdlog::logger>(kMainLoggerName, sinks.begin(), sinks.end());
    auto access = std::make_shared<spdlog::logger>(std::string(Logger::kAccessLoggerName),
                                                   sinks.begin(), sinks.end());
    spdlog::drop(std::string(Logger::kAccessLoggerName));
    spdlog::set_default_logger(main);
    spdlog::register_logger(access);
    g_access = access;
    spdlog::details::registry::instance().set_tp(nullptr);  // joins the old writer (if any)
    g_async = false;
    for (auto& l : {main, access}) {
        apply_format(*l, json);
        l->set_level(level);
        l->flush_on(spdlog::level::warn);
    }
}

}  // namespace

LogLevel Logger::parse_level(std::string_view s) {
    if (s == "debug") return LogLevel::Debug;
    if (s == "warn") return LogLevel::Warn;
    if (s == "error") return LogLevel::Error;
    return LogLevel::Info;
}

void Logger::init(const LogConfig& cfg) {
    spdlog::sink_ptr sink;
    if (cfg.file.empty()) {
        sink = std::make_shared<spdlog::sinks::stderr_color_sink_mt>();
    } else {
        sink = std::make_shared<spdlog::sinks::rotating_file_sink_mt>(
            cfg.file, cfg.max_size, static_cast<size_t>(cfg.max_files));
        // File output sits in the stdio buffer otherwise; a periodic flush keeps
        // `tail -f` honest without paying a syscall per line
        spdlog::flush_every(std::chrono::seconds(1));
    }
    init(cfg, std::move(sink));
}

void Logger::init(const LogConfig& cfg, std::shared_ptr<spdlog::sinks::sink> sink) {
    std::lock_guard<std::mutex> lk(g_mu);
    bool json = cfg.format == "json";
    auto level = to_spdlog(parse_level(cfg.level));
    // A previous incarnation (re-init) may still hold queued records: drain it
    // through its own sinks before the new pair takes over
    if (g_async) install_sync(spdlog::default_logger()->sinks(), g_json, level);
    g_json = json;
    if (!cfg.async) {
        install_sync({sink}, json, level);
        return;
    }
    spdlog::init_thread_pool(static_cast<size_t>(cfg.async_queue), 1);
    auto policy = cfg.async_overflow == "drop" ? spdlog::async_overflow_policy::overrun_oldest
                                               : spdlog::async_overflow_policy::block;
    auto main = std::make_shared<spdlog::async_logger>(kMainLoggerName, sink,
                                                       spdlog::thread_pool(), policy);
    auto access = std::make_shared<spdlog::async_logger>(std::string(kAccessLoggerName), sink,
                                                         spdlog::thread_pool(), policy);
    spdlog::drop(std::string(kAccessLoggerName));
    spdlog::set_default_logger(main);
    spdlog::register_logger(access);
    g_access = access;
    g_async = true;
    for (auto& l : {main, access}) {
        apply_format(*l, json);
        l->set_level(level);
        l->flush_on(spdlog::level::warn);
    }
}

void Logger::set_level(LogLevel lv) { spdlog::set_level(to_spdlog(lv)); }

void Logger::shutdown() {
    std::lock_guard<std::mutex> lk(g_mu);
    if (!g_async) return;
    install_sync(spdlog::default_logger()->sinks(), g_json, spdlog::default_logger()->level());
}

spdlog::logger& Logger::access() {
    std::lock_guard<std::mutex> lk(g_mu);
    if (!g_access) {
        auto def = spdlog::default_logger();
        auto l = std::make_shared<spdlog::logger>(std::string(kAccessLoggerName),
                                                  def->sinks().begin(), def->sinks().end());
        l->set_level(def->level());
        spdlog::drop(std::string(kAccessLoggerName));
        spdlog::register_logger(l);
        g_access = l;
    }
    return *g_access;
}

bool Logger::json() { return g_json; }

}  // namespace lights3
