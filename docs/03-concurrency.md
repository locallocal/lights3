# 03 并发模型：协程 + 线程池

## 1. 总体思路

业务逻辑（L2/L3 接口）**只有一种写法**：C++20 协程，返回 `Task<T>`。
执行环境有两类，通过统一抽象衔接：

| 执行环境 | 谁提供 | 跑什么 |
| --- | --- | --- |
| HTTP 执行环境 | driver（asio io_context 线程，或同步库的请求线程） | 协议逻辑、协程的默认续体 |
| 阻塞 IO 线程池 | core/ThreadPool，进程内一个共享实例 | posix 文件 IO、云 SDK 同步调用、SHA256 大块计算等一切可能阻塞的事 |

规则一句话：**协程里不许直接调用可能阻塞的函数；先 `co_await pool.schedule()`
切过去，干完再切回来。**

```cpp
Task<ObjectStream> LocalFsBackend::get_object(...) {
    co_await pool_->schedule();          // 之后代码运行在池线程
    int fd = ::open(path.c_str(), O_RDONLY);   // 阻塞 OK
    auto meta = read_sidecar(path);
    co_return ObjectStream{meta, make_fd_reader(fd, *pool_)};
}   // 调用方 co_await 处 resume——resume 发生在池线程，
    // 由调用侧的 continuation executor 决定是否切回（见 §3）
```

## 2. Task<T>：轻量惰性协程

```cpp
// src/core/task.h（示意，省略引用限定与异常细节）
template <class T>
class Task {
public:
    struct promise_type {
        std::variant<std::monostate, T, std::exception_ptr> result_;
        std::coroutine_handle<> continuation_;
        IExecutor* cont_executor_ = nullptr;      // resume 续体时投递到哪

        Task get_return_object();
        std::suspend_always initial_suspend() noexcept { return {}; }  // 惰性
        FinalAwaiter final_suspend() noexcept;    // 对称转移或投递到 executor
        void return_value(T v);
        void unhandled_exception();
    };
    // co_await task 时：记录当前协程为 continuation，启动 task
    Awaiter operator co_await() &&;
private:
    std::coroutine_handle<promise_type> h_;
};
```

要点：

- **惰性启动**（initial_suspend = suspend_always）：`Task` 只是描述，
  `co_await` 或 `sync_wait` 才执行。便于组合（`when_all`）与超时包装。
- **对称转移**：`final_suspend` 默认直接 `symmetric transfer` 到 continuation，
  避免栈增长；仅当设置了 `cont_executor_` 时改为 `executor->post(continuation)`。
- 配套原语：
  - `sync_wait(Task<T>) -> T`：阻塞当前线程等待完成（同步 driver 的桥）。
  - `when_all(vector<Task<T>>)`：并发等待（CloudProxy 并发分片上传用）。
  - `with_timeout(Task<T>, duration)`：基于定时器 executor 的超时取消。

## 3. Executor 抽象与线程切换

```cpp
// src/core/executor.h
struct IExecutor {
    virtual void post(std::coroutine_handle<> h) = 0;   // 异步投递并 resume
    virtual ~IExecutor() = default;
};
```

三个实现：

1. **ThreadPoolExecutor**：包装 core/ThreadPool。`pool.schedule()` 返回一个
   awaiter，`await_suspend` 里把 handle post 进池 → 协程在池线程 resume。
2. **AsioExecutor**（beast driver 内）：`post` = `asio::post(io_context, resume)`。
3. **InlineExecutor**：就地 resume，同步 driver 用。

**切回策略**：每个请求协程链在启动时绑定一个 `home executor`（由 driver 提供：
beast 是它的 io_context，httplib 是 inline）。`Task::promise` 记录它；被
`co_await` 的子任务在池线程完成后，final_suspend 把续体 post 回 home executor。
这样 L2 的协议逻辑始终回到 HTTP 执行环境运行，池线程只承担阻塞段本身：

```text
[asio 线程]  handler 逻辑 ── co_await get_object() ──┐
[池线程]                          open/read/meta ◄──┘  完成
[asio 线程]  ◄── post 续体回 home ── 组装响应，继续写 socket
```

实现上通过一个 awaiter 包装器 `resume_on(home)` 自动完成，业务代码不写切换。

### ThreadPool 本体

固定大小（配置 `runtime.io_threads`），MPMC 任务队列 + condvar；带两个护栏：

- **有界队列 + 背压**：队列满时 `schedule()` 的 awaiter 挂到等待列表，
  相当于把背压传导回请求协程乃至 socket 读取，防止阻塞任务无限堆积。
- **指标**：队列深度、任务等待时长直方图，是容量调优的主要信号。

LocalFs 与 CloudProxy 共享此池即可起步；如出现"云端慢请求占满池饿死本地盘"
的情况，再按 backend 配置独立池（Registry 构造时注入，接口已预留）。

## 4. 两类 HTTP 驱动的统一

### 4.1 异步驱动（Beast/asio）

driver 的连接会话本身是 asio 协程。衔接点是让 asio 协程能 `co_await` 我们的
`Task`：提供 `task_to_asio(Task<T>)` 适配器，内部用 `asio::async_initiate`，
在 Task 完成回调里触发 asio 的 completion handler。home executor 设为该连接
所在 io_context。整条链路无线程阻塞，一个 io 线程可承载大量并发连接。

### 4.2 同步驱动（httplib/CivetWeb）

请求线程直接 `sync_wait(handler(req))`。请求线程会在等待期间阻塞——这正是
thread-per-request 模型的语义，没有额外损失。home executor 为 inline：
子任务在池线程完成后就地跑续体，续体推进到下一个 `co_await` 或结束，
`sync_wait` 的 event 被唤醒。

注意事项：inline 续体意味着 L2 逻辑可能在池线程上执行，因此 L2 代码要求
**线程亲和无关**（不使用 TLS 缓存可变状态），这一点写入编码规范并靠
一致性测试覆盖。

## 5. 取消与超时

- 取消源：客户端断连（driver 发现）、请求超时、进程 shutdown。
- 机制：轻量 `CancelToken`（shared_ptr<atomic_bool> + 回调列表）随
  `RequestContext` 传入 handler；长循环（流式读写的每块之间）检查 token；
  `pool.schedule()` 的 awaiter 在挂起时注册取消回调，取消即以
  `OperationCancelled` 异常 resume。
- 不追求抢占式取消：正在执行的阻塞 syscall/SDK 调用等它自然返回，
  返回后看 token 决定终止。这与线程池模型是自洽的。

## 6. 并发控制与限流

- `runtime.max_inflight_requests`：信号量（协程版 async semaphore）在
  dispatch 入口 `co_await acquire()`，超限的请求排队而非拒绝（可配拒绝阈值）。
- 每连接串行处理（HTTP/1.1 pipeline 不并行执行），由 driver 保证。
- Multipart 的分片并发上传由 handler 内 `when_all` + 分片信号量控制，
  不额外占用全局配额之外的资源。
