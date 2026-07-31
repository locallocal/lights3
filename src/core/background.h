// L4: 后台任务等待组（docs/concurrency.md §7）：自销毁顶层协程驱动 + close 时
// 等待在途任务清零。自 TieredBackend / DuoStoreBackend 的逐字重复私有机制提炼
// （生命期关键路径只维护一份）。
//
// 用法（backend 后台 worker 的固定形态）：
//   spawn(task)            —— 定时器回调里派生后台协程（closing 后拒绝）
//   enter()/exit()         —— 调用方驱动的协程（如手动 GC 钩子）登记为在途，
//                             close 会等它结束后才拆资源；enter 返回 false = 正在
//                             关闭，调用方应立即返回
//   if_open(fn)            —— 与 closing 判定原子地执行 fn（典型：登记定时器 id）
//   begin_close()          —— 置 closing；此后 spawn/enter/if_open 全部拒绝
//   wait_idle()            —— 阻塞等在途任务清零（调用方线程；任务在池线程收尾）
//
// close 惯例序：begin_close() → 锁外 cancel 定时器（TimerQueue::cancel 阻塞等
// 在途回调，不得持本组锁调用——回调内 spawn/if_open 要拿组锁，持锁 cancel 即死锁）
// → wait_idle() → 拆资源。
#pragma once

#include <condition_variable>
#include <functional>
#include <mutex>

#include "core/task.h"

namespace lights3 {

class BackgroundTaskGroup {
public:
    // name 用于后台任务失败日志的前缀（须为静态生存期字符串）
    explicit BackgroundTaskGroup(const char* name) : name_(name) {}
    BackgroundTaskGroup(const BackgroundTaskGroup&) = delete;

    // 派生自销毁后台协程；closing 后返回 false（任务被丢弃不执行）。
    // 任务异常在内部捕获并记 WARN，不外抛
    bool spawn(Task<void> t);

    // 调用方驱动的协程登记为在途；与 begin_close 原子互斥。配对 exit()（建议 Scope）
    bool enter();
    void exit();

    // RAII 登记：ok() 为 false 表示正在关闭，调用方应立即返回
    class Scope {
    public:
        explicit Scope(BackgroundTaskGroup& g) : g_(g.enter() ? &g : nullptr) {}
        Scope(const Scope&) = delete;
        ~Scope() {
            if (g_) g_->exit();
        }
        bool ok() const { return g_ != nullptr; }

    private:
        BackgroundTaskGroup* g_;
    };

    // 与 closing 判定原子地执行 fn（closing 时不执行返回 false）；典型用于
    // "检查未关闭 + 登记定时器 id"须一步完成的场景
    bool if_open(const std::function<void()>& fn);

    void begin_close();
    void wait_idle();
    bool closing() const;

private:
    void on_done();

    const char* name_;
    mutable std::mutex m_;
    std::condition_variable cv_;
    int count_ = 0;
    bool closing_ = false;
};

}  // namespace lights3
