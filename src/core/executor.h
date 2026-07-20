// L4: 协程续体的投递抽象（见 docs/concurrency.md §3）
#pragma once

#include <coroutine>

namespace lights3 {

struct IExecutor {
    virtual void post(std::coroutine_handle<> h) = 0;
    virtual ~IExecutor() = default;
};

// 就地 resume：同步驱动（thread-per-request）使用
struct InlineExecutor final : IExecutor {
    void post(std::coroutine_handle<> h) override { h.resume(); }
    static InlineExecutor& instance() {
        static InlineExecutor e;
        return e;
    }
};

// co_await resume_on(ex)：把当前协程的后续执行切换到指定 executor
struct ResumeOnAwaiter {
    IExecutor& ex;
    bool await_ready() const noexcept { return false; }
    void await_suspend(std::coroutine_handle<> h) { ex.post(h); }
    void await_resume() const noexcept {}
};
inline ResumeOnAwaiter resume_on(IExecutor& ex) { return {ex}; }

}  // namespace lights3
