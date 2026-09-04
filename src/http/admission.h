// L1/L2 boundary: dispatch-entry admission control assembly (docs/concurrency.md §6)
//
// This used to be inlined in the main.cc assembly layer — the whole
// lifetime-sensitive path (queueing, tying the Permit into the streaming
// response body, returning it on disconnect, 503 when cancelled while queued)
// was unreachable from unit tests (docs/archive/issues.md T11). Leaking a single
// Permit permanently loses one slot; in production that shows up as a
// site-wide hang once the quota is exhausted, so this class of regression
// must be caught by behavioral tests. Extracted into a standalone header so
// main.cc and the unit tests assemble the same code.
#pragma once

#include <atomic>
#include <chrono>
#include <memory>
#include <utility>

#include "core/cancel.h"
#include "core/semaphore.h"
#include "http/server.h"
#include "http/stall_guard.h"
#include "s3/errors.h"

namespace lights3::http {

// Ties the admission-control Permit to the streaming response body's lifetime
// (docs/archive/gaps.md part 3, concurrency.md §6): if the permit only lived in the
// handler coroutine frame, it would be returned **before** the response body
// starts transferring — N large-object GETs could all acquire and all release
// their permits yet still concurrently consume bandwidth and backend IO, so
// `max_inflight_requests` would not constrain the request's main lifetime;
// shutdown draining that uses available() to count in-flight work would
// likewise miss requests mid-stream. When the driver finishes reading or
// discards the response body, this reader is destroyed and the permit is
// returned with it (destruction may happen on a driver thread, with the same
// semantics as the coroutine-frame path: waiters are woken via the pool
// executor, not inlined on the releasing call stack)
class PermitBodyReader final : public BodyReader {
public:
    PermitBodyReader(std::unique_ptr<BodyReader> inner, AsyncSemaphore::Permit permit)
        : inner_(std::move(inner)), permit_(std::move(permit)) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        co_return co_await inner_->read(buf);
    }
    std::optional<uint64_t> length() const override { return inner_->length(); }

private:
    std::unique_ptr<BodyReader> inner_;
    AsyncSemaphore::Permit permit_;
};

// Assembles the driver handler with admission control + transfer stall guard:
// - Requests over the limit queue on inflight (FIFO) instead of being
//   rejected; cancellation while queued (shutdown broadcast / request timeout
//   / driver disconnect) resolves to 503 SlowDown, which SDKs can retry;
// - If the driver already attached this connection's token, it is kept;
//   otherwise the shutdown_src cancel source is attached;
// - Streaming responses hand the permit to the response body (outermost
//   wrapper): it is only returned when the driver finishes reading or drops
//   it on disconnect, so admission control covers the entire response
//   transfer. Small responses (small_body) return the permit at co_return —
//   the driver's time to write out a chunk of memory is bounded, not worth
//   threading the permit into the driver for;
// - The transfer stall guard (docs/archive/gaps.md §3.3) wraps both directions,
//   installed at the L1/L2 boundary so it covers all four drivers at once;
//   disabled when stall <= 0
// stall_sec is read per request so a config hot reload (roadmap §4.4) can change
// the transfer stall bound without rebuilding the handler chain
inline Handler make_admission_handler(std::shared_ptr<AsyncSemaphore> inflight,
                                      std::shared_ptr<std::atomic<long>> stall_sec,
                                      std::shared_ptr<CancelSource> shutdown_src,
                                      Handler dispatch,
                                      uint64_t stall_min_progress =
                                          StallGuardReader::kMinProgressBytes) {
    return [inflight, stall_sec, shutdown_src, stall_min_progress,
            dispatch = std::move(dispatch)](HttpRequest req) -> Task<HttpResponse> {
        if (!req.cancel.valid()) req.cancel = shutdown_src->token();
        CancelToken tok = req.cancel;
        std::chrono::seconds stall(stall_sec->load(std::memory_order_relaxed));
        try {
            auto permit = co_await inflight->acquire(tok);
            req.body = guard_stalls(std::move(req.body), stall, stall_min_progress);
            auto resp = co_await dispatch(std::move(req));
            resp.stream_body = guard_stalls(std::move(resp.stream_body), stall, stall_min_progress);
            if (resp.stream_body)
                resp.stream_body = std::make_unique<PermitBodyReader>(
                    std::move(resp.stream_body), std::move(permit));
            co_return resp;
        } catch (const OperationCancelled&) {
            HttpResponse r;
            r.status = 503;
            r.headers.set("Content-Type", "application/xml");
            r.small_body = s3::error_xml(
                s3::S3Error(s3::S3ErrorCode::SlowDown,
                            "Request cancelled while queued (server shutting down or "
                            "request timed out)."),
                "-");
            co_return r;
        }
    };
}

// Fixed-stall convenience (tests / static assemblies)
inline Handler make_admission_handler(std::shared_ptr<AsyncSemaphore> inflight,
                                      std::chrono::seconds stall,
                                      std::shared_ptr<CancelSource> shutdown_src,
                                      Handler dispatch,
                                      uint64_t stall_min_progress =
                                          StallGuardReader::kMinProgressBytes) {
    return make_admission_handler(std::move(inflight),
                                  std::make_shared<std::atomic<long>>(stall.count()),
                                  std::move(shutdown_src), std::move(dispatch),
                                  stall_min_progress);
}

}  // namespace lights3::http
