// L3: request-scoped payload that travels on the cancellation token
// (CancelSource::set_data at dispatch, CancelToken::data<RequestBackendStats>()
// down the handler chain, docs/concurrency.md §5). Two consumers:
//   - the metering decorator (metered_backend.h) accumulates the backend share of
//     the request's latency into it for the access log (roadmap §5.1);
//   - outbound hops (cloudproxy) read the request's trace context from it and
//     forward it as traceparent (roadmap §5.4)
#pragma once

#include <atomic>
#include <cstdint>

#include "core/trace.h"

namespace lights3::storage {

struct RequestBackendStats {
    std::atomic<int64_t> nanos{0};
    std::atomic<uint32_t> calls{0};
    std::atomic<uint32_t> errors{0};
    TraceContext trace;  // set once at dispatch, read-only afterwards
    double millis() const { return static_cast<double>(nanos.load(std::memory_order_relaxed)) / 1e6; }
};

}  // namespace lights3::storage
