# 并发模型：协程 + 线程池

> 状态：已实现。本文既是设计文档也是 L4 并发原语的实现参考，
> 代码以 `src/core/` 为准：

| 文件 | 内容 |
| --- | --- |
| `core/task.h` | `Task<T>` 惰性协程、`sync_wait`、`when_all`、`with_timeout` |
| `core/executor.h` | `IExecutor` 投递抽象、`InlineExecutor`、`resume_on` |
| `core/thread_pool.h/.cc` | 阻塞 IO 线程池、`schedule()` awaiter、`ThreadPoolExecutor` |
| `core/semaphore.h` | `AsyncSemaphore` 协程版异步信号量 |
| `core/cancel.h` | `CancelSource` / `CancelToken` 协作式取消 |
| `core/timer.h/.cc` | `TimerQueue` 进程级定时器线程 |

## 1. 总体思路

业务逻辑（L2/L3 接口）**只有一种写法**：C++20 协程，返回 `Task<T>`。
执行环境有两类，通过统一抽象衔接：

| 执行环境 | 谁提供 | 跑什么 |
| --- | --- | --- |
| HTTP 执行环境 | driver（asio io_context / seastar shard / 同步库的请求线程） | HTTP 解析、socket 读写 |
| 阻塞 IO 线程池 | `core/ThreadPool`，进程内一个共享实例 | posix 文件 IO、cloudproxy 的远端 HTTP 调用等一切可能阻塞的事 |

规则一句话：**协程里不许直接调用可能阻塞的函数；先 `co_await pool.schedule()`
切过去，干完再切回来。**

```cpp
Task<ObjectStream> LocalFsBackend::get_object(...) {
    co_await pool_->schedule(ctx.cancel);      // 之后代码运行在池线程
    int fd = ::open(path.c_str(), O_RDONLY);   // 阻塞 OK
    auto meta = read_sidecar(path);
    co_return ObjectStream{meta, make_fd_reader(fd, *pool_)};
}   // 调用方在 co_await 处 resume——默认发生在池线程上（见 §3 切回策略）
```

推论：**L2/L3 代码必须线程亲和无关**——续体可能在池线程、driver 线程或取消
发起线程上运行，不得依赖 TLS 或"始终在同一线程"的假设。需要回到特定执行
环境的地方（beast 碰 socket）由 driver 自己重新锚定（§4.1）。

## 2. `Task<T>`：轻量惰性协程

`Task<T>` 是唯一的协程返回类型。骨架（完整实现见 `core/task.h`）：

- promise 持有 `variant<monostate, T, exception_ptr>`：值与异常统一经
  `await_resume()` 交付，异常在 `co_await` 处原样重抛；
- **惰性启动**（`initial_suspend = suspend_always`）：`Task` 只是描述，
  `co_await` / `sync_wait` 才执行，便于组合（`when_all`、`with_timeout`
  都要求"先拿到 Task 再决定怎么跑"）；
- **对称转移**：`co_await task` 时记录当前协程为 continuation，直接
  `return task_handle` 转移执行权；`final_suspend` 再对称转移回来，
  全程不增长调用栈；
- **move-only**，`operator co_await` 仅限右值（`std::move(t)` 或临时值）：
  一个 Task 只能被消费一次，析构时销毁未完成的协程帧。

三个顶层入口 / 组合器：

### 2.1 sync_wait

```cpp
T sync_wait(Task<T> t);   // 阻塞当前线程直至协程完成
```

同步驱动与 L1 边界的桥（§4.2）。内部用 `SyncWaitEvent`（mutex + condvar）：
`start()` 把事件指针挂进 promise 并 resume，`final_suspend` 发现没有
continuation 但有 event 时 `set()`。event 的 `notify` 必须在锁内完成——
等待方一醒来就可能析构事件对象。

### 2.2 when_all

```cpp
Task<std::vector<T>> when_all(std::vector<Task<T>> tasks);
Task<void>           when_all(std::vector<Task<void>> tasks);
```

并发等待一组 Task，结果按输入顺序返回；任一失败时**等全部结束**后重抛
第一个异常（不提前返回，避免尚在飞行的任务引用已析构的输入）。

实现要点（都是真实数据竞争的教训，动代码前先读注释）：

- **计票**：`WhenAllLatch` 持有 `n + 1` 票——n 个 runner 各一票，
  awaiter 挂起时投第 n+1 票；最后一票的持有者 resume `when_all` 协程。
  awaiter 先写 continuation 再投票，保证 runner 侧看到票数归零时续体已就绪；
- **runner 是自销毁包装协程**（`final_suspend = suspend_never`）：驱动一个
  子任务、捕获其异常、向 latch 报到即消亡。报到（`arrive()`）之后不得再
  触碰 latch——`when_all` 帧可能已在 resume 内销毁；
- runner **惰性启动 + 显式 `start()`**：若在 ramp（协程创建函数）里直接
  开跑，子任务迁到池线程并完成自毁时，ramp 可能仍在触碰协程帧。

生产消费方：tiered 后端的 TierScanner 并发下沉一批冷对象。

### 2.3 with_timeout

```cpp
Task<T> with_timeout(Task<T> task, std::chrono::milliseconds timeout, CancelSource src);
```

协作式超时：向 `TimerQueue`（§5.3）注册一个到点回调，回调只做一件事——
`src.request_cancel()`。task 须以 `src.token()` 构造，在挂起点/长循环感知
取消；超时表现为 `OperationCancelled` 从 task 内部浮出，正常/异常完成都会
撤销定时器。`src` 应为本次调用专用：与他人共享的 source 会在超时后殃及
同请求的后续操作。

## 3. Executor 抽象与线程切换

```cpp
// src/core/executor.h
struct IExecutor {
    virtual void post(std::coroutine_handle<> h) = 0;   // 投递并（异步）resume
};
```

两个实现 + 一个切换原语：

1. **InlineExecutor**：就地 resume。进程单例，也是"不切换"的显式表达；
2. **ThreadPoolExecutor**：`post` = 把 resume 包成任务无界入队线程池。
   `main.cc` 用它做信号量唤醒的投递目标（§6）；
3. `co_await resume_on(ex)`：把当前协程的后续执行切到指定 executor，
   业务代码需要明确落点时使用。

**切回策略**：`Task::promise` 带一个 `cont_executor`（home executor）指针。
设置后 `final_suspend` 不做对称转移，改为 `executor->post(continuation)`；
子任务在 `co_await` 时**继承**调用方的 home executor，因此在链路起点
`task.via(ex)` 一次即可作用于整条协程链。

当前生产路径**不绑定 home executor**（`via()` 由单测覆盖，作为驱动可选
能力保留）：续体默认在完成线程上内联推进——池线程完成阻塞段后顺势跑完
L2 逻辑，直到下一个挂起点。这省掉一次线程切换，代价就是 §1 的
"线程亲和无关"纪律；需要回到特定执行环境的位置由 driver 显式重锚（§4.1）。

### 3.1 ThreadPool 本体

固定大小（配置 `runtime.io_threads`），mutex + condvar 保护的 FIFO 队列。
两条入队路径，语义刻意不同：

| 入口 | 容量 | 用途 |
| --- | --- | --- |
| `post(fn)` | 无界 | 续体投递（executor post）：不可失败也不可等待，丢失续体等于挂死请求 |
| `co_await schedule(token)` | 有界（默认 4096） | 业务切入池线程：队列满时任务进 backlog 等待列表，worker 腾出空位后按 FIFO 放行 |

- **背压**：backlog 中的任务不占队列容量，但 `schedule()` 的协程保持挂起
  ——相当于把压力传导回请求协程乃至 socket 读取，防止阻塞任务无限堆积；
- **指标**（`stats()`，经 `/-/metrics` 暴露，见 [s3-protocol.md](s3-protocol.md) §7）：
  队列深度、backlog 长度、完成计数、入队→开跑等待时长直方图
  （<1ms / <10ms / <100ms / <1s / ≥1s），是容量调优的主要信号；
- `join()`：停止接收新任务，**排空队列**（含 backlog）后等待线程退出；
  join 后 `post/schedule` 抛异常。

localfs / xlocalfs / tiered / cloudproxy 共享此池。如出现云端慢请求占满池
饿死本地盘，再按 backend 配置独立池（Registry 构造时注入，接口已预留）。

### 3.2 schedule() 的取消竞态

`schedule(token)` 返回的 awaiter 要面对两个并发的 resume 来源：池任务
（正常路径）与取消回调（token 被触发，§5）。约束是**恰好 resume 一次**，
且败者手里的引用不能指向已销毁的对象。实现（`ScheduleAwaiter`）：

- 挂起状态放进独立的 `shared_ptr<Slot>` 共享块（handle、`claimed` 原子位、
  取消注销信息），池任务与取消回调各持一份——败者摸到的是共享块，
  而不是可能已随协程恢复而销毁的 awaiter/协程帧；
- 两边先 `claimed.exchange(true)` 认领，胜者 resume：取消回调胜出时置
  `cancelled` 标志，`await_resume` 里注销回调并抛 `OperationCancelled`；
- 取消回调用 `on_cancel_publish` 注册（§5.1）：注销所需的 `(state, id)`
  在注册临界区内落位，堵住"注册后、落位前被回调抢跑 resume"的窗口；
- 注册后补查 `token.cancelled()`：覆盖"注册前已取消，回调根本不会注册"
  的路径，此时不挂起、就地抛出；
- `join()` 后 `schedule`：先认领再抛（堵住取消回调的二次 resume），
  异常从 `co_await` 处浮出。

## 4. 两类 HTTP 驱动的统一

驱动如何把自己的执行模型接到 `Task` 上，详细契约见
[http-adapter.md](http-adapter.md) §4；这里只讲协程衔接点。

### 4.1 异步驱动（beast / seastar）

连接会话本身就是一个 `Task<void>` 协程，挂在驱动的事件循环上：
handler 直接 `resp = co_await handler_(std::move(req))`，全程无线程阻塞。

关键点是**执行环境的重新锚定**：handler 的续体可能在池线程上 resume
（§3 默认策略），而 asio 的 socket/stream 非线程安全。beast 驱动在每个
要碰 socket 的位置插入 `co_await ResumeOn{stream.get_executor()}`
（driver 内部的 awaiter，`asio::post` 到连接自己的 strand）：
handler 返回后、`BeastBodyReader::read` 入口处均如此。效果上等价于
"home executor 只作用于 L1 自己的代码段"，L2/L3 则维持线程亲和无关。

### 4.2 同步驱动（builtin / httplib）

thread-per-request：请求线程直接 `resp = sync_wait(handler_(std::move(req)))`，
等待期间阻塞——这正是该模型的语义，没有额外损失。子任务在池线程完成后
就地推进续体到下一个挂起点或结束，`sync_wait` 的 event 被唤醒。

流式响应同理：驱动的 content provider 每块 `sync_wait(body->read(buf))`。
httplib 的请求 body 是推模型，由 pump 线程经有界缓冲队列翻转成拉模型
（`http/pushpull.h`），队列容量即背压。

## 5. 取消与超时

取消源有三：客户端断连（driver 发现）、请求超时、进程 shutdown。
哲学：**协作式，不追求抢占**——正在执行的阻塞 syscall 等它自然返回，
返回后由挂起点/长循环检查 token 决定终止。这与线程池模型自洽。

### 5.1 CancelSource / CancelToken（core/cancel.h）

- `CancelSource`：触发端，持有 `CancelState`（原子标志 + 回调表）；
  `request_cancel()` 幂等，回调**取出后在锁外执行**（回调内可再操作 token）；
- `CancelToken`：观察端，可自由拷贝，随 `RequestContext` 传入 handler 直到
  L3。默认构造为"永不取消"，让不关心取消的调用方零成本；
- `token.on_cancel(fn)` 返回 `CancelRegistration`（RAII，析构即注销）。
  注册时已取消则**不注册回调**并返回空句柄——调用方随后必须自查
  `cancelled()` 补上竞态窗口；
- `on_cancel_publish(fn, out_id, out_state)`：供"回调会跨线程 resume
  使用方"的场景（§3.2）。与 `request_cancel` 互斥，保证回调可被触发前
  注销信息已落位；
- 取消的表现形式统一为 `OperationCancelled` 异常从挂起点浮出，
  L2 兜底层把它映射为断连/499 语义（见 [http-adapter.md](http-adapter.md) §4 契约）。

### 5.2 感知点

- `pool.schedule(token)`：排队中被取消 → 立即以异常 resume（§3.2）；
- 流式读写的每块之间：`token.throw_if_cancelled()`；
- cloudproxy 的远端流：reader 析构即中止远端传输
  （见 [cloudproxy-backend.md](cloudproxy-backend.md) §3.1）。

### 5.3 TimerQueue（core/timer.h）

进程级单例、单线程的定时器：`add(delay, fn)` 返回 id，`cancel(id)` 撤销
未触发的定时器（不等待正在执行的回调）。回调在定时器线程上执行，
**必须轻量**——典型只做 `request_cancel()` 或 executor post。
`with_timeout`（§2.3）与请求级超时都以它为底座。

## 6. 并发控制与限流

`AsyncSemaphore`（core/semaphore.h）：协程版信号量，超限的 `acquire()`
挂起排队（FIFO）而非拒绝。

```cpp
auto permit = co_await sem.acquire();   // Permit 是 RAII 许可
// ... 协程帧退出（含异常路径）时自动归还
```

实现要点：

- **许可直接移交**：`release` 时若有等待者，许可不回加计数、直接交给队首
  ——先来先服务，不会被新来的 `acquire` 插队；
- **唤醒投递**：构造时可传 `resume_executor`。为空则在 release 调用栈上
  就地 resume 等待者——同步驱动大量排队时会形成"完成一个请求→内联跑完
  下一个请求"的深递归，生产路径应传池 executor。

三个生产消费方：

| 位置 | 用途 |
| --- | --- |
| `main.cc` dispatch 入口 | `runtime.max_inflight_requests` 全局限流，超限请求排队而非拒绝；等待者经池 executor 唤醒 |
| tiered `transfers_` | `max_concurrent_transfers`：下沉/回迁的并发传输上限（见 [tiered-storage.md](tiered-storage.md) §5.1） |
| tiered `key_locks_` | permits=1 当异步互斥用：striped per-key 锁，只保护状态提交段（见 [tiered-storage.md](tiered-storage.md) §7.3） |

此外：每连接串行处理（HTTP/1.1 pipelining 不并行执行）由 driver 保证；
线程池的有界队列 + backlog（§3.1）是最底层的第二道闸门。
