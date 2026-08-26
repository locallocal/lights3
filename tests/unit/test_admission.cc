// Behavioral tests for the ingress admission-control assembly (docs/concurrency.md §6 · docs/archive/issues.md T11):
// queueing when over the limit, the Permit tied into the streaming response body (returned only when fully read
// or dropped on disconnect), and a cancelled queued request returning 503. The unit under test is
// http/admission.h -- the same code main.cc assembles; previously this lifetime-sensitive path had no unit-test
// coverage at all, so Permit-leak regressions (quota exhaustion hanging the whole site) were undetectable.
// Also covers the transfer stall guard's (stall_guard.h) decision behavior.
#include <atomic>
#include <chrono>
#include <thread>

#include "core/thread_pool.h"
#include "http/admission.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::http;
using namespace std::chrono_literals;

namespace {

// Fixed byte stream: a streaming response body of controllable length
class ZeroReader final : public BodyReader {
public:
    explicit ZeroReader(uint64_t size) : size_(size) {}
    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = static_cast<size_t>(std::min<uint64_t>(buf.size(), size_ - pos_));
        std::fill_n(buf.data(), n, std::byte{0});
        pos_ += n;
        co_return n;
    }
    std::optional<uint64_t> length() const override { return size_; }

private:
    uint64_t size_, pos_ = 0;
};

// Assemble an admission handler in the same shape as main.cc (cap permits + pool executor wakeups)
struct AdmissionEnv {
    std::shared_ptr<ThreadPool> pool = std::make_shared<ThreadPool>(2);
    std::shared_ptr<ThreadPoolExecutor> exec = std::make_shared<ThreadPoolExecutor>(*pool);
    std::shared_ptr<AsyncSemaphore> inflight;
    std::shared_ptr<CancelSource> shutdown_src = std::make_shared<CancelSource>();
    Handler handler;

    AdmissionEnv(long cap, Handler dispatch, std::chrono::seconds stall = 0s)
        : inflight(std::make_shared<AsyncSemaphore>(cap, exec.get())),
          handler(make_admission_handler(inflight, stall, shutdown_src, std::move(dispatch))) {}

    HttpResponse call(HttpRequest req = {}) { return sync_wait(handler(std::move(req))); }
};

// Spin-wait until the predicate holds (same correct pattern as test_concurrency.cc, no fixed sleeps)
template <class F>
bool eventually(F&& pred, std::chrono::milliseconds limit = 5000ms) {
    auto deadline = std::chrono::steady_clock::now() + limit;
    while (!pred()) {
        if (std::chrono::steady_clock::now() > deadline) return false;
        std::this_thread::sleep_for(1ms);
    }
    return true;
}

}  // namespace

// Small response (small_body): the permit is returned when the handler co_returns, and does not travel into the driver
TEST(admission_small_body_releases_permit_on_return) {
    AdmissionEnv env(1, [](HttpRequest) -> Task<HttpResponse> {
        HttpResponse r;
        r.small_body = "ok";
        co_return r;
    });
    auto resp = env.call();
    CHECK_EQ(resp.status, 200);
    CHECK_EQ(env.inflight->available(), 1L);
}

// Streaming response: after the handler returns, the permit is still held by the response body (this is exactly the
// point of "the limit covers the whole response transfer"); reading to EOF does not return it, only destruction (driver finished reading / dropped on disconnect) does
TEST(admission_streaming_body_holds_permit_until_dropped) {
    AdmissionEnv env(1, [](HttpRequest) -> Task<HttpResponse> {
        HttpResponse r;
        r.stream_body = std::make_unique<ZeroReader>(128);
        co_return r;
    });
    auto resp = env.call();
    CHECK(resp.stream_body != nullptr);
    CHECK_EQ(env.inflight->available(), 0L);  // the permit is held by the response body

    std::byte buf[256];
    CHECK_EQ(sync_wait(resp.stream_body->read(std::span(buf))), size_t{128});
    CHECK_EQ(sync_wait(resp.stream_body->read(std::span(buf))), size_t{0});  // EOF
    CHECK_EQ(env.inflight->available(), 0L);  // still not returned after reading everything: the return point is destruction

    resp.stream_body.reset();  // driver drops the response body (finished reading and disconnect share this path)
    CHECK(eventually([&] { return env.inflight->available() == 1; }));
}

// Over-limit queueing: the second request queues on the semaphore (FIFO) instead of being rejected, and only gets
// a permit to run after the first one's response body is dropped (disconnect semantics). If disconnect did not
// return the permit this would hang forever -- exactly the leak-type regression shape T11 calls out
TEST(admission_over_limit_queues_until_streaming_peer_disconnects) {
    std::atomic<int> dispatched{0};
    AdmissionEnv env(1, [&](HttpRequest) -> Task<HttpResponse> {
        ++dispatched;
        HttpResponse r;
        r.stream_body = std::make_unique<ZeroReader>(64);
        co_return r;
    });

    auto first = env.call();  // holds the only permit
    CHECK_EQ(dispatched.load(), 1);

    std::atomic<bool> second_done{false};
    std::thread second([&] {
        auto resp = env.call();
        CHECK_EQ(resp.status, 200);
        second_done = true;
        resp.stream_body.reset();
    });
    // The second request should be stuck queueing: dispatch has not been called
    CHECK(eventually([&] { return env.inflight->waiting() == 1; }));
    CHECK_EQ(dispatched.load(), 1);
    CHECK(!second_done.load());

    first.stream_body.reset();  // simulate client disconnect / driver drop -> permit returned
    second.join();
    CHECK_EQ(dispatched.load(), 2);
    CHECK(second_done.load());
    // The second response body has also been destroyed, the full quota is back (no leak)
    CHECK(eventually([&] { return env.inflight->available() == 1; }));
}

// Cancelled while queued (shutdown broadcast / request timeout / disconnect): resolves to 503 SlowDown, and takes no quota
TEST(admission_queued_request_cancelled_returns_503) {
    AdmissionEnv env(1, [](HttpRequest) -> Task<HttpResponse> {
        HttpResponse r;
        r.stream_body = std::make_unique<ZeroReader>(64);
        co_return r;
    });
    auto first = env.call();  // holds the only permit

    CancelSource src;
    std::atomic<bool> got_503{false};
    std::thread second([&] {
        HttpRequest req;
        req.cancel = src.token();  // the connection token the driver attached should be kept and used
        auto resp = env.call(std::move(req));
        got_503 = resp.status == 503 &&
                  resp.small_body.find("<Code>SlowDown</Code>") != std::string::npos;
    });
    CHECK(eventually([&] { return env.inflight->waiting() == 1; }));
    src.request_cancel();
    second.join();
    CHECK(got_503.load());
    CHECK_EQ(env.inflight->waiting(), size_t{0});

    // The cancelled requester takes no quota: the permit is still held by the first response body, and fully returns once dropped
    first.stream_body.reset();
    CHECK(eventually([&] { return env.inflight->available() == 1; }));
}

// Shutdown-source fallback: a request without a token is attached to shutdown_src, and the shutdown broadcast
// surfaces queued requests as 503 (the premise for main.cc's shutdown using available() to detect in-flight work and close() to wake the queued)
TEST(admission_shutdown_broadcast_cancels_queued) {
    AdmissionEnv env(1, [](HttpRequest) -> Task<HttpResponse> {
        HttpResponse r;
        r.stream_body = std::make_unique<ZeroReader>(64);
        co_return r;
    });
    auto first = env.call();

    std::atomic<bool> got_503{false};
    std::thread second([&] {
        auto resp = env.call();  // no token -> attached to shutdown_src
        got_503 = resp.status == 503;
    });
    CHECK(eventually([&] { return env.inflight->waiting() == 1; }));
    env.shutdown_src->request_cancel();
    second.join();
    CHECK(got_503.load());
    first.stream_body.reset();
    CHECK(eventually([&] { return env.inflight->available() == 1; }));
}

// dispatch throws: the permit does not leak (RAII covers the exception path)
TEST(admission_dispatch_exception_releases_permit) {
    AdmissionEnv env(1, [](HttpRequest) -> Task<HttpResponse> {
        throw std::runtime_error("dispatch boom");
        co_return HttpResponse{};
    });
    bool threw = false;
    try {
        env.call();
    } catch (const std::runtime_error&) {
        threw = true;
    }
    CHECK(threw);
    CHECK_EQ(env.inflight->available(), 1L);
}

// ---------- Transfer stall guard (stall_guard.h, the transfer_stall contract of docs/archive/issues.md T10) ----------

namespace {

// A "drip" reader yielding 1 byte per read -- the attack shape called out in the comments (1 byte every 59 seconds)
class DripReader final : public BodyReader {
public:
    Task<size_t> read(std::span<std::byte> buf) override {
        if (buf.empty()) co_return 0;
        buf[0] = std::byte{'x'};
        co_return 1;
    }
    std::optional<uint64_t> length() const override { return std::nullopt; }
};

}  // namespace

TEST(stall_guard_kills_dripping_transfer) {
    // 1-second window: drip-style progress (far below 64KiB per window) must be cut off once the window passes
    auto guarded = guard_stalls(std::make_unique<DripReader>(), 1s);
    std::byte b[1];
    CHECK_EQ(sync_wait(guarded->read(std::span(b))), size_t{1});  // no false kill within the window
    std::this_thread::sleep_for(1100ms);
    CHECK_THROWS_S3(sync_wait(guarded->read(std::span(b))), s3::S3ErrorCode::RequestTimeout);
}

TEST(stall_guard_progress_resets_window) {
    // Progress of >= 64KiB within the window resets the timer: genuinely slow but still-transferring connections are unaffected
    auto guarded = guard_stalls(std::make_unique<ZeroReader>(256 * 1024), 1s);
    std::vector<std::byte> buf(StallGuardReader::kMinProgressBytes);
    CHECK_EQ(sync_wait(guarded->read(std::span(buf))), buf.size());  // one read fills a full window's worth
    std::this_thread::sleep_for(1100ms);
    // The previous read reset the timer; this read's window counts from the reset point -- not killed for absolute elapsed time
    CHECK_EQ(sync_wait(guarded->read(std::span(buf))), buf.size());
}

TEST(stall_guard_min_progress_follows_chunk_size) {
    // roadmap §1.4: with io_chunk_size at its 4KiB lower bound a single read can never
    // reach the 64KiB default, so every window would misjudge a healthy connection.
    // Under the default threshold, 4KiB reads are killed once the window passes...
    auto strict = guard_stalls(std::make_unique<ZeroReader>(64 * 1024), 1s);
    std::vector<std::byte> buf(4096);
    CHECK_EQ(sync_wait(strict->read(std::span(buf))), buf.size());
    std::this_thread::sleep_for(1100ms);
    CHECK_THROWS_S3(sync_wait(strict->read(std::span(buf))), s3::S3ErrorCode::RequestTimeout);
    // ...while the clamped threshold (min(64KiB, io_chunk_size)) counts them as progress
    auto clamped = guard_stalls(std::make_unique<ZeroReader>(64 * 1024), 1s, 4096);
    CHECK_EQ(sync_wait(clamped->read(std::span(buf))), buf.size());
    std::this_thread::sleep_for(1100ms);
    CHECK_EQ(sync_wait(clamped->read(std::span(buf))), buf.size());
}

TEST(stall_guard_eof_and_disabled_passthrough) {
    // EOF is not judged a stall
    auto guarded = guard_stalls(std::make_unique<ZeroReader>(0), 1s);
    std::byte b[8];
    CHECK_EQ(sync_wait(guarded->read(std::span(b))), size_t{0});
    // window <= 0 = disabled: returned as-is, not wrapped
    auto raw = guard_stalls(std::make_unique<DripReader>(), 0s);
    CHECK(dynamic_cast<StallGuardReader*>(raw.get()) == nullptr);
    // A null reader passes through as-is (the no-body response path)
    CHECK(guard_stalls(nullptr, 1s) == nullptr);
}
