// L1/L2 边界：dispatch 入口限流装配（docs/concurrency.md §6）
//
// 曾内联在 main.cc 装配层——排队、Permit 系进流式响应体、断连归还、排队被取消
// 回 503 这整条生命周期敏感路径不被单测触达（docs/issues.md T11）。Permit 泄漏
// 一个就永久少一个额度，生产表现为额度耗尽后全站 hang；这类回归必须有行为测试
// 兜住。抽成独立头让 main.cc 与单测装配同一份代码。
#pragma once

#include <chrono>
#include <memory>
#include <utility>

#include "core/cancel.h"
#include "core/semaphore.h"
#include "http/server.h"
#include "http/stall_guard.h"
#include "s3/errors.h"

namespace lights3::http {

// 把入口限流的 Permit 系进流式响应体的生命期（docs/gaps.md 第三部分 ·
// concurrency.md §6）：permit 若只活在 handler 协程帧内，会在响应体开始传输
// **之前**就归还——N 个大对象 GET 全部拿到 permit 又全部归还后仍在并发占用
// 带宽与后端 IO，`max_inflight_requests` 约束不到请求的主要生命期；关停排空
// 用 available() 判在途，同样会漏数流式传输中的请求。驱动读完/丢弃响应体时
// 析构本 reader，permit 随之归还（析构可发生在驱动线程，与协程帧路径同语义：
// 等待者经池 executor 唤醒，不在释放方调用栈上内联展开）
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

// 组装带准入限流 + 传输停滞守卫的驱动 handler：
// - 超限的请求在 inflight 上排队（FIFO）而非拒绝；排队中被取消（关停广播 /
//   请求超时 / 驱动断连）以 503 SlowDown 收敛，SDK 可重试；
// - 驱动已挂上本连接 token 时保留，否则接上 shutdown_src 的关停源；
// - 流式响应把 permit 交给响应体（最外层包装）：驱动读完/断连丢弃时才归还，
//   限流因此覆盖响应传输全程。小响应（small_body）随 co_return 归还——驱动
//   写出一段内存的耗时有界，不值得为它把 permit 也穿进驱动；
// - 传输停滞守卫（docs/gaps.md §3.3）收发两个方向都包一层，装在 L1/L2 交界处
//   四驱动一次性生效；stall <= 0 时关闭
inline Handler make_admission_handler(std::shared_ptr<AsyncSemaphore> inflight,
                                      std::chrono::seconds stall,
                                      std::shared_ptr<CancelSource> shutdown_src,
                                      Handler dispatch) {
    return [inflight, stall, shutdown_src,
            dispatch = std::move(dispatch)](HttpRequest req) -> Task<HttpResponse> {
        if (!req.cancel.valid()) req.cancel = shutdown_src->token();
        CancelToken tok = req.cancel;
        try {
            auto permit = co_await inflight->acquire(tok);
            req.body = guard_stalls(std::move(req.body), stall);
            auto resp = co_await dispatch(std::move(req));
            resp.stream_body = guard_stalls(std::move(resp.stream_body), stall);
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

}  // namespace lights3::http
