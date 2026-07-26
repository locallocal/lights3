#include "core/background.h"

#include <utility>

#include "core/log.h"

namespace lights3 {

namespace {

// 自销毁的顶层协程：驱动一个后台 Task 并在结束时回调（异常只记日志）
struct Detached {
    struct promise_type {
        Detached get_return_object() { return {}; }
        std::suspend_never initial_suspend() noexcept { return {}; }
        std::suspend_never final_suspend() noexcept { return {}; }
        void return_void() {}
        void unhandled_exception() { std::terminate(); }  // 协程体内已全捕获
    };
};

Detached run_detached(const char* name, Task<void> t, std::function<void()> done) {
    try {
        co_await std::move(t);
    } catch (const std::exception& e) {
        LOG_WARN("{}: background task failed: {}", name, e.what());
    } catch (...) {
        LOG_WARN("{}: background task failed (unknown exception)", name);
    }
    done();
}

}  // namespace

bool BackgroundTaskGroup::spawn(Task<void> t) {
    {
        std::lock_guard lk(m_);
        if (closing_) return false;
        ++count_;
    }
    run_detached(name_, std::move(t), [this] { on_done(); });
    return true;
}

bool BackgroundTaskGroup::enter() {
    std::lock_guard lk(m_);
    if (closing_) return false;
    ++count_;
    return true;
}

void BackgroundTaskGroup::exit() { on_done(); }

bool BackgroundTaskGroup::if_open(const std::function<void()>& fn) {
    std::lock_guard lk(m_);
    if (closing_) return false;
    fn();
    return true;
}

void BackgroundTaskGroup::begin_close() {
    std::lock_guard lk(m_);
    closing_ = true;
}

void BackgroundTaskGroup::wait_idle() {
    std::unique_lock lk(m_);
    cv_.wait(lk, [&] { return count_ == 0; });
}

bool BackgroundTaskGroup::closing() const {
    std::lock_guard lk(m_);
    return closing_;
}

void BackgroundTaskGroup::on_done() {
    std::lock_guard lk(m_);
    --count_;
    cv_.notify_all();
}

}  // namespace lights3
