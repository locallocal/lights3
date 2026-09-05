// roadmap §5.4: W3C Trace Context primitive — strict parse, fresh start, child spans
#include "core/trace.h"
#include "unit/mini_test.h"

using namespace lights3;

namespace {
bool lower_hex(const std::string& s, size_t n) {
    if (s.size() != n) return false;
    for (char c : s)
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
    return true;
}
}  // namespace

TEST(trace_parse_valid_traceparent) {
    auto c = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
                                 "vendor=abc,other=1");
    CHECK(c.has_value());
    CHECK_EQ(c->trace_id, "4bf92f3577b34da6a3ce929d0e0e4736");
    CHECK_EQ(c->parent_span_id, "00f067aa0ba902b7");
    CHECK(lower_hex(c->span_id, 16));               // this hop gets its own span
    CHECK(c->span_id != c->parent_span_id);
    CHECK(c->sampled);
    CHECK(c->inherited);
    CHECK_EQ(c->tracestate, "vendor=abc,other=1");
    CHECK_EQ(c->traceparent(), "00-4bf92f3577b34da6a3ce929d0e0e4736-" + c->span_id + "-01");
    // Unsampled flag is preserved on the outgoing header
    auto u = TraceContext::parse("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-00");
    CHECK(u && !u->sampled);
    CHECK(u->traceparent().ends_with("-00"));
    // A future version with an extra trailing field parses by its first four fields
    auto v = TraceContext::parse("01-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-extra");
    CHECK(v && v->trace_id == "4bf92f3577b34da6a3ce929d0e0e4736");
}

TEST(trace_parse_rejects_malformed) {
    for (const char* bad : {
             "",
             "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7",          // missing flags
             "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01-x",     // v00 with trailer
             "00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01",       // uppercase
             "00-00000000000000000000000000000000-00f067aa0ba902b7-01",       // zero trace id
             "00-4bf92f3577b34da6a3ce929d0e0e4736-0000000000000000-01",       // zero span id
             "ff-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",       // version ff
             "00_4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",       // bad separator
             "00-4bf92f3577b34da6a3ce929d0e0e473g-00f067aa0ba902b7-01",       // non-hex
             "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-0z",       // non-hex flags
         })
        CHECK(!TraceContext::parse(bad).has_value());
    // from_headers falls back to a fresh trace rather than failing the request
    auto fresh = TraceContext::from_headers(std::string("garbage"), std::nullopt);
    CHECK(fresh.valid() && !fresh.inherited && fresh.parent_span_id.empty());
    auto none = TraceContext::from_headers(std::nullopt, std::nullopt);
    CHECK(none.valid() && !none.inherited);
}

TEST(trace_start_and_next_hop) {
    auto a = TraceContext::start();
    auto b = TraceContext::start();
    CHECK(lower_hex(a.trace_id, 32) && lower_hex(a.span_id, 16));
    CHECK(a.trace_id != b.trace_id);
    CHECK(a.sampled && !a.inherited && a.parent_span_id.empty());
    // The wire form names this hop's span; the next hop parses it as its parent
    auto next = TraceContext::parse(a.traceparent());
    CHECK(next.has_value());
    CHECK_EQ(next->trace_id, a.trace_id);
    CHECK_EQ(next->parent_span_id, a.span_id);
    CHECK(next->span_id != a.span_id);
}
