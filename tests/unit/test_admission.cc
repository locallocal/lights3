// 入口限流装配的行为测试（docs/concurrency.md §6 · docs/issues.md T11）：
// 超限排队、Permit 系进流式响应体（读完/断连丢弃才归还）、排队被取消回 503。
// 被测对象是 http/admission.h——与 main.cc 装配的同一份代码；此前这条
// 生命周期敏感路径完全不被单测触达，Permit 泄漏类回归（额度耗尽全站 hang）
// 无从检出。另含传输停滞守卫（stall_guard.h）的判定行为。
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

// 固定字节流：可控长度的流式响应体
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

// 装配一套与 main.cc 相同形态的准入 handler（cap 个许可 + 池 executor 唤醒）
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

// 自旋等待谓词成立（同 test_concurrency.cc 的正确范式，不用固定 sleep）
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

// 小响应（small_body）：permit 随 handler co_return 归还，不穿进驱动
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

// 流式响应：handler 返回后 permit 仍被响应体占用（这正是"约束覆盖响应传输
// 全程"的要点），读到 EOF 不归还，析构（驱动读完/断连丢弃）才归还
TEST(admission_streaming_body_holds_permit_until_dropped) {
    AdmissionEnv env(1, [](HttpRequest) -> Task<HttpResponse> {
        HttpResponse r;
        r.stream_body = std::make_unique<ZeroReader>(128);
        co_return r;
    });
    auto resp = env.call();
    CHECK(resp.stream_body != nullptr);
    CHECK_EQ(env.inflight->available(), 0L);  // permit 在响应体手里

    std::byte buf[256];
    CHECK_EQ(sync_wait(resp.stream_body->read(std::span(buf))), size_t{128});
    CHECK_EQ(sync_wait(resp.stream_body->read(std::span(buf))), size_t{0});  // EOF
    CHECK_EQ(env.inflight->available(), 0L);  // 读完仍未归还：归还点是析构

    resp.stream_body.reset();  // 驱动丢弃响应体（读完或断连同一路径）
    CHECK(eventually([&] { return env.inflight->available() == 1; }));
}

// 超限排队：第二个请求在信号量上排队（FIFO）而非拒绝，第一个的响应体被丢弃
// （断连语义）后它才拿到许可跑完。断连不归还 permit 的话这里永久 hang——
// 正是 T11 点名的泄漏型回归形态
TEST(admission_over_limit_queues_until_streaming_peer_disconnects) {
    std::atomic<int> dispatched{0};
    AdmissionEnv env(1, [&](HttpRequest) -> Task<HttpResponse> {
        ++dispatched;
        HttpResponse r;
        r.stream_body = std::make_unique<ZeroReader>(64);
        co_return r;
    });

    auto first = env.call();  // 占住唯一许可
    CHECK_EQ(dispatched.load(), 1);

    std::atomic<bool> second_done{false};
    std::thread second([&] {
        auto resp = env.call();
        CHECK_EQ(resp.status, 200);
        second_done = true;
        resp.stream_body.reset();
    });
    // 第二个请求应停在排队处：dispatch 未被调用
    CHECK(eventually([&] { return env.inflight->waiting() == 1; }));
    CHECK_EQ(dispatched.load(), 1);
    CHECK(!second_done.load());

    first.stream_body.reset();  // 模拟客户端断连/驱动丢弃 → permit 归还
    second.join();
    CHECK_EQ(dispatched.load(), 2);
    CHECK(second_done.load());
    // 第二个响应体也已析构，额度完整归位（无泄漏）
    CHECK(eventually([&] { return env.inflight->available() == 1; }));
}

// 排队期间被取消（关停广播/请求超时/断连）：503 SlowDown 收敛，且不占额度
TEST(admission_queued_request_cancelled_returns_503) {
    AdmissionEnv env(1, [](HttpRequest) -> Task<HttpResponse> {
        HttpResponse r;
        r.stream_body = std::make_unique<ZeroReader>(64);
        co_return r;
    });
    auto first = env.call();  // 占住唯一许可

    CancelSource src;
    std::atomic<bool> got_503{false};
    std::thread second([&] {
        HttpRequest req;
        req.cancel = src.token();  // 驱动挂上的连接 token 应被保留使用
        auto resp = env.call(std::move(req));
        got_503 = resp.status == 503 &&
                  resp.small_body.find("<Code>SlowDown</Code>") != std::string::npos;
    });
    CHECK(eventually([&] { return env.inflight->waiting() == 1; }));
    src.request_cancel();
    second.join();
    CHECK(got_503.load());
    CHECK_EQ(env.inflight->waiting(), size_t{0});

    // 取消者不占额度：许可仍在第一个响应体手里，丢弃后完整归位
    first.stream_body.reset();
    CHECK(eventually([&] { return env.inflight->available() == 1; }));
}

// 关停源兜底：请求未带 token 时接上 shutdown_src，关停广播让排队者以 503 浮出
//（main.cc 关停用 available() 判在途、close() 唤醒排队者的前提）
TEST(admission_shutdown_broadcast_cancels_queued) {
    AdmissionEnv env(1, [](HttpRequest) -> Task<HttpResponse> {
        HttpResponse r;
        r.stream_body = std::make_unique<ZeroReader>(64);
        co_return r;
    });
    auto first = env.call();

    std::atomic<bool> got_503{false};
    std::thread second([&] {
        auto resp = env.call();  // 不带 token → 接 shutdown_src
        got_503 = resp.status == 503;
    });
    CHECK(eventually([&] { return env.inflight->waiting() == 1; }));
    env.shutdown_src->request_cancel();
    second.join();
    CHECK(got_503.load());
    first.stream_body.reset();
    CHECK(eventually([&] { return env.inflight->available() == 1; }));
}

// dispatch 抛异常：permit 不泄漏（RAII 覆盖异常路径）
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

// ---------- 传输停滞守卫（stall_guard.h，docs/issues.md T10 的 transfer_stall 契约）----------

namespace {

// 每次 read 给 1 字节的"滴灌"reader——注释点名的攻击形态（每 59 秒 1 字节）
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
    // 窗口 1 秒：滴灌式推进（远低于窗口内 64KiB）在窗口过后必须被掐断
    auto guarded = guard_stalls(std::make_unique<DripReader>(), 1s);
    std::byte b[1];
    CHECK_EQ(sync_wait(guarded->read(std::span(b))), size_t{1});  // 窗口内不误杀
    std::this_thread::sleep_for(1100ms);
    CHECK_THROWS_S3(sync_wait(guarded->read(std::span(b))), s3::S3ErrorCode::RequestTimeout);
}

TEST(stall_guard_progress_resets_window) {
    // 窗口内推进 ≥ 64KiB 即重置计时：真正慢但在传的连接不受影响
    auto guarded = guard_stalls(std::make_unique<ZeroReader>(256 * 1024), 1s);
    std::vector<std::byte> buf(StallGuardReader::kMinProgressBytes);
    CHECK_EQ(sync_wait(guarded->read(std::span(buf))), buf.size());  // 一次推满一个窗口量
    std::this_thread::sleep_for(1100ms);
    // 上一读已重置计时，这一读的窗口从重置点起算——不因绝对时长被杀
    CHECK_EQ(sync_wait(guarded->read(std::span(buf))), buf.size());
}

TEST(stall_guard_eof_and_disabled_passthrough) {
    // EOF 不判定停滞
    auto guarded = guard_stalls(std::make_unique<ZeroReader>(0), 1s);
    std::byte b[8];
    CHECK_EQ(sync_wait(guarded->read(std::span(b))), size_t{0});
    // window <= 0 = 关闭：原样返回，不包裹
    auto raw = guard_stalls(std::make_unique<DripReader>(), 0s);
    CHECK(dynamic_cast<StallGuardReader*>(raw.get()) == nullptr);
    // 空 reader 原样透传（响应无 body 的路径）
    CHECK(guard_stalls(nullptr, 1s) == nullptr);
}
