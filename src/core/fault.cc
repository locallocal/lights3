#include "core/fault.h"

#include <cerrno>
#include <cstdlib>
#include <map>
#include <mutex>
#include <stdexcept>
#include <vector>

namespace lights3::fault {

namespace detail {
std::atomic<int> g_armed{0};
}

namespace {

struct Armed {
    int remaining;  // -1 = unlimited
    int err;
};

std::mutex g_mu;
std::map<std::string, Armed, std::less<>> g_points;

int errno_of(std::string_view s) {
    static const std::map<std::string_view, int> kNames = {
        {"EIO", EIO},         {"ENOSPC", ENOSPC},   {"EDQUOT", EDQUOT},
        {"EACCES", EACCES},   {"EROFS", EROFS},     {"ETIMEDOUT", ETIMEDOUT},
        {"ECONNRESET", ECONNRESET}, {"ECONNREFUSED", ECONNREFUSED}, {"EPIPE", EPIPE},
        {"ENOENT", ENOENT},   {"EAGAIN", EAGAIN},   {"EMFILE", EMFILE},
        {"ENOMEM", ENOMEM},   {"EINVAL", EINVAL},   {"EBUSY", EBUSY},
    };
    if (auto it = kNames.find(s); it != kNames.end()) return it->second;
    char* end = nullptr;
    std::string tmp(s);
    long v = std::strtol(tmp.c_str(), &end, 10);
    if (end && *end == '\0' && v > 0 && v < 4096) return static_cast<int>(v);
    throw std::runtime_error("fault: unknown errno '" + tmp + "'");
}

bool known_point(std::string_view p) {
    for (auto k : kPoints)
        if (k == p) return true;
    return false;
}

}  // namespace

void arm(std::string_view spec) {
    std::vector<std::pair<std::string, Armed>> parsed;
    size_t pos = 0;
    while (pos <= spec.size()) {
        size_t comma = spec.find(',', pos);
        std::string_view item = spec.substr(pos, comma == std::string_view::npos ? std::string_view::npos : comma - pos);
        pos = comma == std::string_view::npos ? spec.size() + 1 : comma + 1;
        // trim
        while (!item.empty() && (item.front() == ' ' || item.front() == '\t')) item.remove_prefix(1);
        while (!item.empty() && (item.back() == ' ' || item.back() == '\t')) item.remove_suffix(1);
        if (item.empty()) continue;
        size_t c1 = item.find(':');
        std::string_view name = item.substr(0, c1);
        Armed a{1, EIO};
        if (c1 != std::string_view::npos) {
            std::string_view rest = item.substr(c1 + 1);
            size_t c2 = rest.find(':');
            std::string_view count = rest.substr(0, c2);
            if (!count.empty()) {
                std::string tmp(count);
                char* end = nullptr;
                long n = std::strtol(tmp.c_str(), &end, 10);
                if (!end || *end != '\0' || n < 0)
                    throw std::runtime_error("fault: bad count in '" + std::string(item) + "'");
                a.remaining = n == 0 ? -1 : static_cast<int>(n);
            }
            if (c2 != std::string_view::npos) a.err = errno_of(rest.substr(c2 + 1));
        }
        if (!known_point(name))
            throw std::runtime_error("fault: unknown point '" + std::string(name) + "'");
        parsed.emplace_back(std::string(name), a);
    }
    std::lock_guard lk(g_mu);
    for (auto& [name, a] : parsed) g_points[name] = a;
    detail::g_armed.store(static_cast<int>(g_points.size()), std::memory_order_release);
}

void arm_from_env() {
    if (const char* v = std::getenv("LIGHTS3_FAULTS"); v && *v) arm(v);
}

void reset() {
    std::lock_guard lk(g_mu);
    g_points.clear();
    detail::g_armed.store(0, std::memory_order_release);
}

int detail::check_slow(std::string_view point) {
    std::lock_guard lk(g_mu);
    auto it = g_points.find(point);
    if (it == g_points.end()) return 0;
    int err = it->second.err;
    if (it->second.remaining > 0 && --it->second.remaining == 0) {
        g_points.erase(it);
        g_armed.store(static_cast<int>(g_points.size()), std::memory_order_release);
    }
    return err;
}

std::string describe() {
    std::lock_guard lk(g_mu);
    std::string out;
    for (auto& [name, a] : g_points) {
        if (!out.empty()) out += ", ";
        out += name + ":" + (a.remaining < 0 ? "*" : std::to_string(a.remaining)) + ":" +
               std::to_string(a.err);
    }
    return out;
}

}  // namespace lights3::fault
