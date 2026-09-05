// L4: W3C Trace Context (roadmap §5.4, lightweight tier). The gateway accepts an
// incoming `traceparent` (+ `tracestate`, passed through verbatim), starts a trace
// of its own when none is present or the header is malformed, stamps the ids on
// the request's log lines / access record / audit record, and forwards its own
// span to every outbound hop (cloudproxy) so the next instance logs this span as
// its parent — a multi-hop deployment correlates on one trace id and the parent
// links land in the logs. No spans are exported (no intermediate per-hop spans
// either: an unlogged span id would link nothing); otel-cpp instrumentation
// stays a long-term item
#pragma once

#include <optional>
#include <string>
#include <string_view>

namespace lights3 {

struct TraceContext {
    std::string trace_id;        // 32 lowercase hex, never all zeros
    std::string span_id;         // 16 lowercase hex: this hop's span
    std::string parent_span_id;  // the caller's span (empty when this hop started the trace)
    bool sampled = true;         // trace-flags bit 0
    std::string tracestate;      // vendor state, opaque, passed through unchanged
    bool inherited = false;      // true = trace_id came from the client

    bool valid() const { return !trace_id.empty(); }
    // "00-<trace_id>-<span_id>-<flags>": the header sent to the next hop
    std::string traceparent() const;

    // Strict W3C parse: version 00 (any other version is accepted with its first
    // four fields, ff is rejected), lowercase hex, non-zero ids. nullopt = malformed
    static std::optional<TraceContext> parse(std::string_view traceparent,
                                             std::string_view tracestate = {});
    // A fresh trace (new trace id + span id, sampled)
    static TraceContext start();
    // The request's context: parsed when valid, else a fresh trace
    static TraceContext from_headers(std::optional<std::string> traceparent,
                                     std::optional<std::string> tracestate);
};

}  // namespace lights3
