// L1/L3 共享：推模型 ↔ 拉模型的翻转组件（docs/09 §3.1，自 httplib 驱动提取）。
// BlockQueue：按字节限容的有界缓冲，单生产者/单消费者；容量即背压。
// 生产者 push 返回 false 表示消费方已 cancel；消费者 pop 返回 0 表示 EOF，
// close(ok=false) 后的 pop 以异常传播（对应"对端中途失败"契约）。
#pragma once

#include <condition_variable>
#include <cstring>
#include <deque>
#include <memory>
#include <mutex>
#include <span>
#include <stdexcept>
#include <string>

#include "http/model.h"

namespace lights3::http {

class BlockQueue {
public:
    explicit BlockQueue(size_t cap_bytes) : cap_(cap_bytes) {}

    // 生产者；返回 false 表示消费方已取消（推送方收到 false 即中止传输）
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

    // 消费者；0 = EOF；生产方中途失败以异常传播
    size_t pop(std::span<std::byte> buf) {
        std::unique_lock lk(m_);
        cv_pop_.wait(lk, [&] { return !blocks_.empty() || closed_; });
        if (blocks_.empty()) {
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
    QueueBodyReader(std::shared_ptr<BlockQueue> q, std::optional<uint64_t> len)
        : q_(std::move(q)), len_(len) {}

    Task<size_t> read(std::span<std::byte> buf) override {
        if (eof_) co_return 0;
        size_t n = q_->pop(buf);
        if (n == 0) eof_ = true;
        co_return n;
    }
    std::optional<uint64_t> length() const override { return len_; }

private:
    std::shared_ptr<BlockQueue> q_;
    std::optional<uint64_t> len_;
    bool eof_ = false;
};

}  // namespace lights3::http
