#include "core/trace.h"

#include <cstdint>
#include <random>

namespace lights3 {

namespace {

std::mt19937_64& trace_rng() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    return rng;
}

std::string random_hex(size_t bytes) {
    static constexpr char kHex[] = "0123456789abcdef";
    std::string out;
    out.reserve(bytes * 2);
    while (out.size() < bytes * 2) {
        uint64_t v = trace_rng()();
        for (int i = 0; i < 8 && out.size() < bytes * 2; ++i) {
            unsigned char b = static_cast<unsigned char>(v >> (i * 8));
            out.push_back(kHex[b >> 4]);
            out.push_back(kHex[b & 0xf]);
        }
    }
    return out;
}

// Non-empty span/trace ids must be non-zero: a zero id is the spec's "invalid" marker
bool lower_hex_nonzero(std::string_view s) {
    bool nonzero = false;
    for (char c : s) {
        bool hex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        if (!hex) return false;
        if (c != '0') nonzero = true;
    }
    return nonzero;
}

bool lower_hex(std::string_view s) {
    for (char c : s)
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
    return true;
}

}  // namespace

std::string TraceContext::traceparent() const {
    return "00-" + trace_id + "-" + span_id + (sampled ? "-01" : "-00");
}

std::optional<TraceContext> TraceContext::parse(std::string_view tp, std::string_view ts) {
    // version(2)-trace_id(32)-span_id(16)-flags(2); a future version may append
    // "-..." after the flags, which is ignored
    if (tp.size() < 55) return std::nullopt;
    if (tp[2] != '-' || tp[35] != '-' || tp[52] != '-') return std::nullopt;
    std::string_view version = tp.substr(0, 2), trace = tp.substr(3, 32), span = tp.substr(36, 16),
                     flags = tp.substr(53, 2);
    if (!lower_hex(version) || version == "ff") return std::nullopt;
    if (version == "00" && tp.size() != 55) return std::nullopt;
    if (tp.size() > 55 && tp[55] != '-') return std::nullopt;
    if (!lower_hex_nonzero(trace) || !lower_hex_nonzero(span) || !lower_hex(flags))
        return std::nullopt;
    TraceContext c;
    c.trace_id = std::string(trace);
    c.parent_span_id = std::string(span);
    c.span_id = random_hex(8);  // this hop's own span
    c.sampled = (flags[1] == '1' || flags[1] == '3' || flags[1] == '5' || flags[1] == '7' ||
                 flags[1] == '9' || flags[1] == 'b' || flags[1] == 'd' || flags[1] == 'f');
    c.tracestate = std::string(ts);
    c.inherited = true;
    return c;
}

TraceContext TraceContext::start() {
    TraceContext c;
    c.trace_id = random_hex(16);
    c.span_id = random_hex(8);
    return c;
}

TraceContext TraceContext::from_headers(std::optional<std::string> tp,
                                        std::optional<std::string> ts) {
    if (tp)
        if (auto c = parse(*tp, ts.value_or(""))) return *c;
    return start();
}

}  // namespace lights3
