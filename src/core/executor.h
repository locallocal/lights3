// L4: 协程续体的投递抽象（见 docs/03-concurrency.md §3）
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

}  // namespace lights3
