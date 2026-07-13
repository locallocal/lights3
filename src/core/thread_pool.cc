#include "core/thread_pool.h"

#include <stdexcept>

namespace lights3 {

ThreadPool::ThreadPool(size_t threads) {
    if (threads == 0) threads = 1;
    workers_.reserve(threads);
    for (size_t i = 0; i < threads; ++i)
        workers_.emplace_back([this] { worker_loop(); });
}

ThreadPool::~ThreadPool() { join(); }

void ThreadPool::post(std::function<void()> fn) {
    {
        std::lock_guard lk(m_);
        if (stopping_) throw std::runtime_error("ThreadPool: post after join");
        queue_.push_back(std::move(fn));
    }
    cv_.notify_one();
}

void ThreadPool::join() {
    {
        std::lock_guard lk(m_);
        if (stopping_) return;
        stopping_ = true;
    }
    cv_.notify_all();
    for (auto& t : workers_)
        if (t.joinable()) t.join();
}

void ThreadPool::worker_loop() {
    for (;;) {
        std::function<void()> fn;
        {
            std::unique_lock lk(m_);
            cv_.wait(lk, [&] { return stopping_ || !queue_.empty(); });
            if (queue_.empty()) return;  // stopping 且已排空
            fn = std::move(queue_.front());
            queue_.pop_front();
        }
        fn();
    }
}

}  // namespace lights3
