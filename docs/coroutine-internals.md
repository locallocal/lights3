# 协程实现内幕

> 本文总结 lights3 中 C++20 协程机制的**实现**：`Task<T>` 的 promise 布局与对称转移、
> 顶层驱动方式、组合子、取消的竞态协议。设计动机与使用规范见
> [concurrency.md](concurrency.md)，此处以"代码为什么长这样"为主线。

涉及文件：

| 文件 | 内容 |
| --- | --- |
| `core/task.h` | `Task<T>`/`Task<void>`、`sync_wait`、`sync_wait_pumping`、`when_all`、`with_timeout` |
| `core/executor.h` | `IExecutor`、`InlineExecutor`、`PumpExecutor`、`resume_on` |
| `core/thread_pool.h/.cc` | `ThreadPool`、`schedule()` 的 `ScheduleAwaiter`、`ThreadPoolExecutor` |
| `core/semaphore.h` | `AsyncSemaphore` 的 `AcquireAwaiter`（与 schedule 同款竞态协议） |
| `core/cancel.h` | `CancelState`/`CancelSource`/`CancelToken`/`CancelRegistration` |
| `core/timer.h/.cc` | `TimerQueue` 双线程定时器（`with_timeout` 的底座） |
| `core/background.h/.cc` | `BackgroundTaskGroup`：自毁式顶层协程 + 等待组 |

## 1. `Task<T>`：惰性协程的 promise 布局

`Task<T>` 是唯一的业务协程类型。三个关键决定：

1. **惰性启动**：`initial_suspend()` 返回 `suspend_always`。协程体在 `co_await`/
   `start()` 之前不执行任何代码，因此 `Task` 可以先被 `via()`/`with_cancel()`
   打标记、再被移动传递，不存在"已经跑起来才绑定"的竞态。
2. **帧所有权 = RAII**：`Task` 独占 `coroutine_handle`，析构时 `destroy()`。
   moved-from 的 `Task` 上任何操作都抛 `std::logic_error`（比空指针解引用可诊断，
   docs/archive/gaps.md §4）。
3. **结果内联在 promise 里**：`Task<T>::promise_type` 用
   `std::variant<monostate, T, exception_ptr>` 存结果，`await_resume()`/
   `take_result()` 取值时 rethrow 异常——异常沿 co_await 链自然传播，无需错误码。

所有 promise 共享基类 `detail::PromiseBase`，它承载了三条"沿 co_await 链继承"的
信息：

```cpp
struct PromiseBase {
    std::coroutine_handle<> continuation;  // 谁在等我
    SyncWaitEvent* event;                  // 顶层 sync_wait 的完成事件
    IExecutor* cont_executor;              // home executor（§3）
    CancelToken cancel;                    // 取消令牌（§5）
};
```

### 1.1 co_await 的对称转移

`co_await task` 走 `Task::Awaiter::await_suspend` → `detail::task_await_suspend`：

- 把调用者 handle 记进子任务的 `continuation`；
- 子任务**没有**自己的 `cont_executor`/`cancel` 时，从父 promise 继承（有则保留，
  `via`/`with_cancel` 显式绑定的优先）；
- 返回子任务 handle → **对称转移**启动子任务，不增长栈。

完成路径在 `PromiseBase::FinalAwaiter::await_suspend`：

- 有 `continuation` 且有 `cont_executor` → `post` 到 executor（回到 home 执行环境，
  例如 beast 的连接 strand），返回 `noop_coroutine`；
- 有 `continuation` 无 executor → 对称转移直接恢复调用者；
- 都没有（顶层）→ `event->set()` 唤醒 `sync_wait`。

`operator co_await` 只对右值开放（`&&`）：一个 `Task` 只能被 await 一次，结果被
move 走。

### 1.2 继承机制的意义

`cont_executor` 与 `cancel` 的按需继承是整个设计的支点：请求入口处一次
`task.with_cancel(token)`（见 `S3Service::dispatch`），之后 L2/L3 全链条的
`co_await pool_->schedule()`、`co_await sem.acquire()` 都自动感知取消，**不需要把
token 穿透 40+ 个函数签名**（docs/archive/gaps.md §3.1）。awaiter 侧靠 `requires` 约束探测
调用者 promise 是否携带 token：

```cpp
if constexpr (requires { { h.promise().cancel } -> std::convertible_to<CancelToken>; })
    if (!token.valid()) token = h.promise().cancel;
```

## 2. 顶层驱动：一个 Task 如何被"跑起来"

`Task` 是惰性的，必须有顶层驱动者。四种方式对应四类场景：

| 驱动方式 | 场景 | 机制 |
| --- | --- | --- |
| `sync_wait(task)` | 启动/关闭路径、测试 | `SyncWaitEvent`（mutex+cv）阻塞当前线程 |
| `sync_wait_pumping(ex, task)` | 同步驱动（builtin/httplib）的请求线程 | 请求线程化身 executor 泵队列 |
| fire-and-forget `Detached` | 异步驱动（beast）、后台任务 | 自毁式包装协程 |
| `when_all` 的 `WhenAllRunner` | 并发子任务 | 同上，外加计数栅栏 |

### 2.1 sync_wait 与通知时序

`SyncWaitEvent::set()` **持锁 notify**：等待方一醒来就可能销毁事件对象（栈上），
notify 必须在解锁前完成，否则会触碰已析构的 cv。`PumpExecutor::post/finish` 同理。

### 2.2 sync_wait_pumping：请求线程兼职 executor

同步驱动下如果用 `sync_wait`，请求线程在整个请求期间闲置阻塞。
`sync_wait_pumping`（docs/archive/gaps.md §2.10）改为：

1. `detail::pump_run`（自毁式包装协程，`initial/final_suspend` 均 `suspend_never`）
   立即启动顶层 task，结果/异常写进调用者栈上的 `out/err`，最后 `ex.finish()`；
2. 调用者线程进入 `PumpExecutor::run()` 循环，执行被 `post` 过来的续体——
   body 读取等阻塞点用 `co_await resume_on(ex)` 把阻塞切回请求线程，慢客户端只
   拖垮自己的连接线程，不占共享池线程。

### 2.3 自毁式包装协程的公共形状

`PumpRunner` / `WhenAllRunner` / `background.cc` 和 beast 驱动里的 `Detached`
都是同一形状：`final_suspend` 返回 `suspend_never` → 协程体跑完帧自动销毁，
`unhandled_exception` 直接 `std::terminate()`（协程体内已 catch 一切）。两个易错点
在注释里反复强调：

- **`WhenAllRunner` 必须惰性启动 + 显式 `start()`**：若 eager 启动，协程迁移到池
  线程并自毁时，ramp（编译器生成的启动代码）可能还在触碰帧——真实数据竞争。
- **`background.cc::run_detached` 里 task 帧必须先于 `done()` 销毁**：`done()` 一旦
  执行，`wait_idle()` 就放行 owner 去拆后端/线程池；协程参数 `t` 属于帧、销毁在
  `done()` 之后，所以先 move 进内层作用域的局部变量（docs/archive/gaps.md §3.9）。

## 3. Executor 抽象与线程切换

`IExecutor` 只有一个 `post(coroutine_handle<>)`。实现：

- `InlineExecutor`：就地 resume（同步驱动的简单路径）；
- `PumpExecutor`：投递到请求线程的泵队列（§2.2）；
- `ThreadPoolExecutor`：投递到 IO 线程池（`AsyncSemaphore` 唤醒等待者用它，避免
  释放方的调用栈上内联跑完下一个请求）；
- beast 驱动内部用 asio strand 直接充当执行环境（`ResumeOn` awaiter 把续体
  `asio::post` 回连接 strand）。

`co_await resume_on(ex)` 是显式线程切换原语；`co_await pool.schedule()` 则是
"切到池线程"+"可取消挂起点"二合一（§6）。

## 4. 组合子

### 4.1 when_all：n+1 票的栅栏

`when_all(vector<Task<T>>)` 为每个子任务起一个 `WhenAllRunner`，结果写进
when_all 自己帧上的 `results/errors` 数组。同步靠 `WhenAllLatch`：**n 个 runner +
1 个 awaiter 共 n+1 票**，`fetch_sub` 到 0 的那一方恢复 when_all 协程——awaiter 先
写 `continuation` 再投票，保证 runner 侧读到就绪的续体；投完最后一票后不许再碰
latch（when_all 帧可能已在 resume 里销毁）。

启动循环中途 runner 帧分配失败时：已启动的 runner 仍引用本帧的 latch/results，
所以先替未启动者补投票、`co_await` 等已启动者收敛，再把异常抛出去——绝不能带着
在途 runner 提前离开本帧。语义：全部完成后才返回；有失败则 rethrow 第一个异常，
结果按输入序。

### 4.2 with_timeout：定时器只负责点火

`with_timeout(task, ms, src)` = `task.with_cancel(src.token())` +
`TimerQueue::add(ms, [src]{ src.request_cancel(); })`。到期只触发取消；超时表现为
链上最近的可取消挂起点抛 `OperationCancelled`。正常完成/异常路径都 `tq.cancel(id)`
——`TimerQueue::cancel` 会阻塞到"已到期未执行完"的回调收敛，因此回调捕获的 `src`
副本不需要额外生命期保护。已在池线程上跑着的阻塞 syscall 不被抢占（协作式取消，
等它自然返回后由下一个挂起点感知）。

## 5. 取消的实现：CancelState

`CancelSource`（触发方）/`CancelToken`（观察方）共享 `detail::CancelState`：

- `request_cancel()`：置位后把回调表 **swap 出来在锁外执行**（回调里允许再操作
  token）；`firing_/firing_thread_` + `FiringGuard` 保证任何路径（包括回调抛异常）
  都复位状态并唤醒等待者。
- `remove_callback(id)`：返回即保证回调不会再跑——若回调批次正在锁外执行，阻塞到
  批次结束（与 `TimerQueue::cancel` 同语义）；**回调线程上的自注销不等待**，防自
  死锁。
- `add_callback_publish`：为"回调会跨线程恢复消费者"的场景准备——把注销所需的
  `(state, id)` 写进调用者提供的存储，**与 `request_cancel` 在同一临界区内互斥**。
  否则回调可能赢得竞争、在"注册完成但 id 尚未写回"的窗口 resume 协程，写回与
  消费者的读构成竞争。

回调契约：**必须轻量**。取消源常是 TimerQueue 的回调线程，回调里内联 resume 整条
请求链会拖住后续所有定时器（docs/archive/gaps.md §3.2）。

## 6. 可取消挂起点的竞态协议（Slot/Waiter claim）

`ThreadPool::ScheduleAwaiter` 与 `AsyncSemaphore::AcquireAwaiter` 是仅有的两个
可取消挂起点，共用同一协议：

```
共享块（shared_ptr<Slot/Waiter>）：
    h            要恢复的协程
    claimed      atomic<bool>，恢复权：谁 exchange 到 false→true 谁 resume
    cancelled    仅由胜者写，resume 后同线程读
    reg_id / cancel_state   注销回调所需（on_cancel_publish 写入）
```

要点：

1. **状态独立于协程帧**。正常唤醒方（池任务 / release）与取消回调竞争 resume，
   败者手里仍握着引用——那块内存不能指向可能已随协程恢复而销毁的 awaiter/帧，
   所以放进独立的 `shared_ptr` 块。
2. **注册回调之后不许再碰帧成员**。`suspend_impl` 把之后要用的一切先拷进局部变量
   （`this`、`pool`、`token` 都是帧成员）；回调注册成功那一刻起，协程随时可能在
   别的线程被恢复并连帧销毁。
3. **取消路径就地 resume 是有意为之**：被恢复的协程立刻从 `await_resume` 抛
   `OperationCancelled`，只做异常展开这种有界工作。若 post 回线程池，"池饱和"这个
   最需要取消的场景里，取消通知反而排在阻塞任务后面——照 archive/gaps.md §3.2 字面实现会
   死锁（见 test_concurrency 对应用例）。点火线程侧的防护在 TimerQueue：回调跑在
   专职回调线程，展开不阻塞到期判定。
4. **正常唤醒方跳过已被取消认领的等待者**：`AsyncSemaphore::release_one` 弹出
   waiter 时先 `claimed.exchange`，输了就找下一个，全输才 `++permits_`。
5. **先注册回调、再入队/检查**，配合注册后的一次 `cancelled()` 复查，覆盖
   "注册前已取消（回调不会被调用）"的窗口。

`ThreadPool` 侧还有两点配套实现：`post`（续体投递）与 `schedule`（阻塞任务）用
**分离队列**，worker 优先清空续体队列——续体是已让出线程的存量工作；`schedule`
队列有界，满了进 backlog 由 worker 腾位释放（背压）。`join` 后 `post` 不抛
（消费者多为 noexcept 路径），记 ERROR 并就地执行；`schedule` 则抛异常，awaiter
里先 claim 再 throw，堵住取消回调的二次 resume。

## 7. TimerQueue：双线程分工

调度线程 `loop()` 只判到期、把 `DueItem` 挪进 `due_` 队列；专职回调线程
`fire_loop()` 串行执行回调。分工的原因：`request_cancel` 会就地展开被取消的协程
链，若跑在调度线程上，展开期间全进程定时器的**到期判定**都停摆。回调之间仍串行
（慢回调拖后续回调，有 `slow`/`lag_seconds` 指标兜底观测）。

`cancel(id)` 的阻塞语义（回调"已到期未执行完"则等它结束）让调用方无需为
"fired 但未执行"窗口自备生命期保护；回调线程上自取消不等待。进程关闭后 `add`
返回 0（永不点火的"有效" id 是调试陷阱），`cancel(0)` 恒为安全 no-op。

## 8. 生命期守则速查

实现/评审协程相关代码时的检查单，均有上文对应的事故原型：

1. 协程帧内的东西，在"可能被别的线程恢复"之后一律不许碰——先拷局部变量（§6.2）。
2. 唤醒用的共享状态放独立分配的块里，不放 awaiter/帧内（§6.1）。
3. 事件/泵对象 notify 必须持锁完成，等待方醒来即可销毁它（§2.1）。
4. 自毁式包装协程：要么惰性启动保证 ramp 退出后再跑（`WhenAllRunner`），要么确认
   eager 启动安全；被 await 的 task 参数先 move 出帧再等（`run_detached`）。
5. 投完最后一票/触发 resume 之后，不许再读写共享的 latch/slot（§4.1、§6）。
6. 取消回调轻量化；需要恢复协程时，要么确认展开是有界工作（就地），要么走
   executor（`AsyncSemaphore` 正常唤醒），不要往可能饱和的池里 post 取消通知。
7. `AsyncSemaphore` 析构时不许有等待者——owner 先排空或 `close()`；
   `ThreadPool::join` 前后端必须先 `close()`（`Application::shutdown` 的顺序即由
   这些约束推导，见 `src/app/app.cc`）。
