// L4: 协作式取消原语（docs/03-concurrency.md §5）
// CancelSource 触发、CancelToken 观察；取消以 OperationCancelled 异常从挂起点浮出。
// 不追求抢占：正在执行的阻塞调用等其自然返回，返回后由挂起点/长循环检查 token。
#pragma once

#include <atomic>
#include <cstdint>
#include <exception>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <utility>

namespace lights3 {

class OperationCancelled : public std::exception {
public:
    const char* what() const noexcept override { return "operation cancelled"; }
};

namespace detail {

class CancelState {
public:
    void request_cancel() {
        std::map<uint64_t, std::function<void()>> cbs;
        {
            std::lock_guard lk(m_);
            if (cancelled_.exchange(true, std::memory_order_acq_rel)) return;
            cbs.swap(callbacks_);
        }
        for (auto& [id, fn] : cbs) fn();  // 锁外执行，回调内可再操作 token
    }

    bool cancelled() const { return cancelled_.load(std::memory_order_acquire); }

    // 已取消则不注册并返回 0；调用方注册后须自查 cancelled() 补上竞态窗口
    uint64_t add_callback(std::function<void()> fn) {
        std::lock_guard lk(m_);
        if (cancelled_.load(std::memory_order_relaxed)) return 0;
        uint64_t id = ++next_id_;
        callbacks_.emplace(id, std::move(fn));
        return id;
    }

    // 同上，但在同一临界区内把注销所需的 (state, id) 写入调用方提供的存储：
    // request_cancel 与本函数互斥，保证回调可被触发之前两者均已落位。
    // 否则"注册后、落位前"被取消回调抢跑 resume，落位写与使用方读形成数据竞争
    uint64_t add_callback_publish(std::function<void()> fn, std::atomic<uint64_t>& out_id,
                                  std::shared_ptr<CancelState>& out_state,
                                  std::shared_ptr<CancelState> self) {
        std::lock_guard lk(m_);
        if (cancelled_.load(std::memory_order_relaxed)) {
            out_id.store(0, std::memory_order_release);
            return 0;
        }
        uint64_t id = ++next_id_;
        out_state = std::move(self);
        out_id.store(id, std::memory_order_release);
        callbacks_.emplace(id, std::move(fn));
        return id;
    }

    void remove_callback(uint64_t id) {
        if (id == 0) return;
        std::lock_guard lk(m_);
        callbacks_.erase(id);
    }

private:
    std::atomic<bool> cancelled_{false};
    std::mutex m_;
    uint64_t next_id_ = 0;
    std::map<uint64_t, std::function<void()>> callbacks_;
};

}  // namespace detail

// 取消回调的注册句柄：析构/reset 时解除注册（对已触发的注册解除是空操作）
class CancelRegistration {
public:
    CancelRegistration() = default;
    CancelRegistration(std::shared_ptr<detail::CancelState> s, uint64_t id)
        : state_(std::move(s)), id_(id) {}
    CancelRegistration(CancelRegistration&& o) noexcept
        : state_(std::move(o.state_)), id_(std::exchange(o.id_, 0)) {}
    CancelRegistration& operator=(CancelRegistration&& o) noexcept {
        if (this != &o) {
            reset();
            state_ = std::move(o.state_);
            id_ = std::exchange(o.id_, 0);
        }
        return *this;
    }
    CancelRegistration(const CancelRegistration&) = delete;
    ~CancelRegistration() { reset(); }

    void reset() {
        if (state_) state_->remove_callback(id_);
        state_.reset();
        id_ = 0;
    }

private:
    std::shared_ptr<detail::CancelState> state_;
    uint64_t id_ = 0;
};

// 观察端。默认构造为"永不取消"，可自由拷贝随 RequestContext 传递
class CancelToken {
public:
    CancelToken() = default;
    explicit CancelToken(std::shared_ptr<detail::CancelState> s) : state_(std::move(s)) {}

    bool cancelled() const { return state_ && state_->cancelled(); }
    void throw_if_cancelled() const {
        if (cancelled()) throw OperationCancelled();
    }

    // 注意：若注册时已取消，回调不会被调用（返回空句柄）——调用方随后自查 cancelled()
    CancelRegistration on_cancel(std::function<void()> fn) const {
        if (!state_) return {};
        uint64_t id = state_->add_callback(std::move(fn));
        return id ? CancelRegistration(state_, id) : CancelRegistration{};
    }

    // 供"回调会跨线程 resume 使用方"的场景：注销所需的 (out_state, out_id)
    // 在注册临界区内落位，回调可被触发前即已就绪。返回是否注册成功
    bool on_cancel_publish(std::function<void()> fn, std::atomic<uint64_t>& out_id,
                           std::shared_ptr<detail::CancelState>& out_state) const {
        if (!state_) {
            out_id.store(0, std::memory_order_release);
            return false;
        }
        return state_->add_callback_publish(std::move(fn), out_id, out_state, state_) != 0;
    }

private:
    std::shared_ptr<detail::CancelState> state_;
};

// 触发端：客户端断连（driver 发现）、请求超时、进程 shutdown
class CancelSource {
public:
    CancelSource() : state_(std::make_shared<detail::CancelState>()) {}

    CancelToken token() const { return CancelToken(state_); }
    void request_cancel() { state_->request_cancel(); }
    bool cancelled() const { return state_->cancelled(); }

private:
    std::shared_ptr<detail::CancelState> state_;
};

}  // namespace lights3
