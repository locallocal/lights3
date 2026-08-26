// Shared by L1/L3: push-model <-> pull-model inversion components
// (docs/cloudproxy-backend.md §3.1, extracted from the httplib driver).
// BlockQueue: bounded buffer capped by bytes, single-producer/single-consumer;
// the capacity is the backpressure. Producer push returning false means the
// consumer has cancelled; consumer pop returning 0 means EOF, and pop after
// close(ok=false) propagates as an exception (matching the "peer failed
// mid-transfer" contract).
#pragma once

#include <condition_variable>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>

#include "core/executor.h"
#include "http/model.h"

namespace lights3::http {

class BlockQueue {
public:
    explicit BlockQueue(size_t cap_bytes) : cap_(cap_bytes) {}

    // Producer; false means the consumer has cancelled (the pusher aborts the transfer on false)
    bool push(const char* data, size_t n) {
        std::unique_lock lk(m_);
        cv_push_.wait(lk, [&] { return bytes_ < cap_ || cancelled_; });
        if (cancelled_) return false;
        blocks_.emplace_back(data, n);
        bytes_ += n;
        cv_pop_.notify_one();
        return true;
    }

    void close(bool ok) {
        std::lock_guard lk(m_);
        closed_ = true;
        ok_ = ok;
        cv_pop_.notify_all();
    }

    // Consumer; 0 = EOF; throws if the producer failed mid-transfer, or the queue was cancelled with nothing left
    size_t pop(std::span<std::byte> buf) {
        std::unique_lock lk(m_);
        cv_pop_.wait(lk, [&] { return !blocks_.empty() || closed_ || cancelled_; });
        if (blocks_.empty()) {
            // After cancel we must not keep waiting for the producer (it stops
            // pushing once it sees cancelled), nor report a normal EOF
            if (cancelled_ && !closed_)
                throw std::runtime_error("http body: transfer cancelled");
            if (!ok_) throw std::runtime_error("http body: peer disconnected mid-body");
            return 0;
        }
        auto& front = blocks_.front();
        size_t n = std::min(buf.size(), front.size() - front_pos_);
        std::memcpy(buf.data(), front.data() + front_pos_, n);
        front_pos_ += n;
        bytes_ -= n;
        if (front_pos_ == front.size()) {
            blocks_.pop_front();
            front_pos_ = 0;
        }
        cv_push_.notify_one();
        return n;
    }

    void cancel() {
        std::lock_guard lk(m_);
        cancelled_ = true;
        cv_push_.notify_all();
        // Consumers blocked in pop must be woken too: this is a shared component
        // (cloudproxy uses it as well), and cancel-before-pop (or cancel from
        // another thread) must not leave the consumer blocked forever
        cv_pop_.notify_all();
    }

private:
    std::mutex m_;
    std::condition_variable cv_push_, cv_pop_;
    std::deque<std::string> blocks_;
    size_t front_pos_ = 0;
    size_t bytes_ = 0;
    size_t cap_;
    bool closed_ = false;
    bool ok_ = true;
    bool cancelled_ = false;
};

class QueueBodyReader : public BodyReader {
public:
    // When request_thread is non-null (httplib driver, docs/archive/gaps.md §2.10): the
    // cv-blocking pop first switches back to the request's own thread (which is
    // idling in sync_wait_pumping), so it does not occupy a shared pool thread;
    // when null, it blocks in place (cloudproxy's pump direction already yields
    // per block on a pool thread, see the caller's comment)
    QueueBodyReader(std::shared_ptr<BlockQueue> q, std::optional<uint64_t> len,
                    IExecutor* request_thread = nullptr)
        : q_(std::move(q)), len_(len), exec_(request_thread) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        if (eof_) co_return 0;
        if (exec_) co_await resume_on(*exec_);
        size_t n = q_->pop(buf);
        if (n == 0) eof_ = true;
        co_return n;
    }
    std::optional<uint64_t> length() const override { return len_; }

private:
    std::shared_ptr<BlockQueue> q_;
    std::optional<uint64_t> len_;
    IExecutor* exec_;
    bool eof_ = false;
};

}  // namespace lights3::http
