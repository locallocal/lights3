// L4: 阻塞 IO 线程池；协程经 co_await pool.schedule() 切入池线程
#pragma once

#include <condition_variable>
#include <coroutine>
#include <deque>
#include <functional>
#include <mutex>
#include <thread>
#include <vector>

#include "core/executor.h"

namespace lights3 {

class ThreadPool {
public:
    explicit ThreadPool(size_t threads);
    ~ThreadPool();
    ThreadPool(const ThreadPool&) = delete;

    void post(std::function<void()> fn);
    void join();  // 停止接收新任务，排空队列并等待线程退出
    size_t size() const { return workers_.size(); }

    struct ScheduleAwaiter {
        ThreadPool& pool;
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            pool.post([h] { h.resume(); });
        }
        void await_resume() const noexcept {}
    };
    ScheduleAwaiter schedule() { return {*this}; }

private:
    void worker_loop();

    std::mutex m_;
    std::condition_variable cv_;
    std::deque<std::function<void()>> queue_;
    std::vector<std::thread> workers_;
    bool stopping_ = false;
};

struct ThreadPoolExecutor final : IExecutor {
    explicit ThreadPoolExecutor(ThreadPool& pool) : pool_(pool) {}
    void post(std::coroutine_handle<> h) override {
        pool_.post([h] { h.resume(); });
    }

private:
    ThreadPool& pool_;
};

}  // namespace lights3
