# Coroutine Internals

> This document summarizes the **implementation** of the C++20 coroutine
> machinery in lights3: the promise layout and symmetric transfer of `Task<T>`,
> top-level drivers, combinators, and the cancellation race protocols. Design
> rationale and usage rules live in [concurrency.md](concurrency.md); the focus
> here is "why the code looks the way it does".

Files involved:

| File | Contents |
| --- | --- |
| `core/task.h` | `Task<T>`/`Task<void>`, `sync_wait`, `sync_wait_pumping`, `when_all`, `with_timeout` |
| `core/executor.h` | `IExecutor`, `InlineExecutor`, `PumpExecutor`, `resume_on` |
| `core/thread_pool.h/.cc` | `ThreadPool`, the `ScheduleAwaiter` behind `schedule()`, `ThreadPoolExecutor` |
| `core/semaphore.h` | `AsyncSemaphore`'s `AcquireAwaiter` (same race protocol as schedule) |
| `core/cancel.h` | `CancelState`/`CancelSource`/`CancelToken`/`CancelRegistration` |
| `core/timer.h/.cc` | `TimerQueue`, the two-thread timer underlying `with_timeout` |
| `core/background.h/.cc` | `BackgroundTaskGroup`: self-destroying top-level coroutine + wait group |

## 1. `Task<T>`: promise layout of a lazy coroutine

`Task<T>` is the only business-logic coroutine type. Three key decisions:

1. **Lazy start**: `initial_suspend()` returns `suspend_always`. The body runs
   nothing before `co_await`/`start()`, so a `Task` can first be tagged with
   `via()`/`with_cancel()` and moved around — there is no "already running
   before binding" race.
2. **Frame ownership = RAII**: `Task` exclusively owns the
   `coroutine_handle` and `destroy()`s it in the destructor. Any operation on a
   moved-from `Task` throws `std::logic_error` (far more diagnosable than a
   null-pointer dereference, docs/gaps.md §4).
3. **The result lives inline in the promise**: `Task<T>::promise_type` stores a
   `std::variant<monostate, T, exception_ptr>`; `await_resume()`/`take_result()`
   rethrow on the exception alternative — exceptions propagate naturally along
   the co_await chain, no error codes needed.

All promises share the base `detail::PromiseBase`, which carries the three
pieces of state inherited down the co_await chain:

```cpp
struct PromiseBase {
    std::coroutine_handle<> continuation;  // who awaits me
    SyncWaitEvent* event;                  // completion event for top-level sync_wait
    IExecutor* cont_executor;              // home executor (§3)
    CancelToken cancel;                    // cancellation token (§5)
};
```

### 1.1 Symmetric transfer on co_await

`co_await task` goes through `Task::Awaiter::await_suspend` →
`detail::task_await_suspend`:

- record the caller's handle as the child's `continuation`;
- if the child has **no** `cont_executor`/`cancel` of its own, inherit from the
  parent promise (an explicitly bound one via `via`/`with_cancel` wins);
- return the child's handle → **symmetric transfer** starts the awaited task
  without growing the stack.

The completion path is `PromiseBase::FinalAwaiter::await_suspend`:

- continuation present and `cont_executor` set → `post` the continuation to the
  executor (returning to the home execution context, e.g. beast's connection
  strand) and return `noop_coroutine`;
- continuation present, no executor → symmetric transfer straight back to the
  caller;
- neither (top level) → `event->set()` wakes `sync_wait`.

`operator co_await` is rvalue-only (`&&`): a `Task` can be awaited exactly
once; the result is moved out.

### 1.2 Why the inheritance matters

The on-demand inheritance of `cont_executor` and `cancel` is the linchpin: one
`task.with_cancel(token)` at the request entry point (see
`S3Service::dispatch`) makes every `co_await pool_->schedule()` and
`co_await sem.acquire()` across the whole L2/L3 chain cancellation-aware —
**without threading the token through 40+ signatures** (docs/gaps.md §3.1). The
awaiters probe the caller's promise with a `requires` constraint:

```cpp
if constexpr (requires { { h.promise().cancel } -> std::convertible_to<CancelToken>; })
    if (!token.valid()) token = h.promise().cancel;
```

## 2. Top-level drivers: how a Task gets to run

A `Task` is lazy; something must drive it at the top. Four drivers for four
scenarios:

| Driver | Scenario | Mechanism |
| --- | --- | --- |
| `sync_wait(task)` | startup/shutdown paths, tests | `SyncWaitEvent` (mutex+cv) blocks the calling thread |
| `sync_wait_pumping(ex, task)` | request threads of the synchronous drivers (builtin/httplib) | the request thread doubles as an executor and pumps a queue |
| fire-and-forget `Detached` | async drivers (beast), background tasks | self-destroying wrapper coroutine |
| `when_all`'s `WhenAllRunner` | concurrent child tasks | same, plus a counting latch |

### 2.1 sync_wait and notify ordering

`SyncWaitEvent::set()` **notifies while holding the lock**: the waiter may
destroy the (stack-allocated) event the moment it wakes, so notify must
complete before unlocking or it would touch a destroyed cv.
`PumpExecutor::post/finish` follow the same rule.

### 2.2 sync_wait_pumping: the request thread doubles as an executor

Under the synchronous drivers, plain `sync_wait` would leave the request thread
idle-blocked for the whole request. `sync_wait_pumping` (docs/gaps.md §2.10)
instead:

1. `detail::pump_run` (a self-destroying wrapper coroutine with
   `suspend_never` initial/final suspend) starts the top-level task
   immediately, writes result/exception into `out/err` on the caller's stack,
   and finally calls `ex.finish()`;
2. the calling thread enters the `PumpExecutor::run()` loop, executing
   continuations `post`ed to it — blocking points such as body reads switch
   back onto the request thread via `co_await resume_on(ex)`, so a slow client
   clogs only its own connection thread, never the shared pool threads.

### 2.3 The common shape of self-destroying wrapper coroutines

`PumpRunner` / `WhenAllRunner` / `Detached` in `background.cc` and the beast
driver all share one shape: `final_suspend` returns `suspend_never` → the frame
self-destructs when the body finishes; `unhandled_exception` calls
`std::terminate()` (the body already catches everything). Two hazards the
comments stress repeatedly:

- **`WhenAllRunner` must be lazily started with an explicit `start()`**: with
  an eager start, when the coroutine migrates to a pool thread and
  self-destructs, the ramp (compiler-generated startup code) may still be
  touching the frame — a real data race.
- **In `background.cc::run_detached` the task frame must be destroyed before
  `done()`**: once `done()` runs, `wait_idle()` releases the owner to start
  tearing down the backend/thread pool, while the coroutine parameter `t`
  belongs to the frame and would be destroyed after `done()` — hence it is
  moved into a local of an inner scope first (docs/gaps.md §3.9).

## 3. The executor abstraction and thread switching

`IExecutor` has a single `post(coroutine_handle<>)`. Implementations:

- `InlineExecutor`: resume in place (simple path of the synchronous drivers);
- `PumpExecutor`: post to the request thread's pump queue (§2.2);
- `ThreadPoolExecutor`: post to the IO thread pool (used by `AsyncSemaphore` to
  wake waiters, so the next request does not run inline on the releasing call
  stack);
- the beast driver uses asio strands directly as its execution context (its
  `ResumeOn` awaiter `asio::post`s the continuation back onto the connection
  strand).

`co_await resume_on(ex)` is the explicit thread-switch primitive;
`co_await pool.schedule()` combines "hop onto a pool thread" with "cancellable
suspension point" (§6).

## 4. Combinators

### 4.1 when_all: a latch with n+1 votes

`when_all(vector<Task<T>>)` starts one `WhenAllRunner` per child; results are
written into `results/errors` arrays on when_all's own frame. Synchronization
is the `WhenAllLatch`: **n runners + 1 awaiter make n+1 votes**; whoever
`fetch_sub`s to 0 resumes the when_all coroutine. The awaiter writes
`continuation` before casting its vote so the runner side always sees a ready
continuation; after casting the final vote nobody may touch the latch again
(the when_all frame may already be destroyed inside resume).

If runner frame allocation throws partway through the start loop: runners
already started still reference this frame's latch/results, so the votes of
the never-started ones are cast on their behalf, the awaiter waits for the
started ones to settle, and only then is the exception rethrown — never leave
the frame with runners in flight. Semantics: returns only after everything
completes; on failure rethrows the first exception; results are in input order.

### 4.2 with_timeout: the timer only pulls the trigger

`with_timeout(task, ms, src)` = `task.with_cancel(src.token())` +
`TimerQueue::add(ms, [src]{ src.request_cancel(); })`. Expiry only requests
cancellation; a timeout surfaces as `OperationCancelled` thrown from the
nearest cancellable suspension point in the chain. Both the success and the
exception path call `tq.cancel(id)` — and since `TimerQueue::cancel` blocks
until a due-but-unfinished callback settles, the `src` copy captured by the
callback needs no extra lifetime guard. A blocking syscall already running on
a pool thread is not preempted (cooperative cancellation: it returns naturally
and the next suspension point notices).

## 5. Cancellation internals: CancelState

`CancelSource` (trigger side) and `CancelToken` (observer side) share a
`detail::CancelState`:

- `request_cancel()`: sets the flag, then **swaps the callback map out and runs
  it outside the lock** (callbacks may operate on the token again);
  `firing_/firing_thread_` plus the `FiringGuard` guarantee that every path
  (including a throwing callback) resets the state and wakes waiters.
- `remove_callback(id)`: on return the callback is guaranteed never to run
  again — if a callback batch is currently executing outside the lock, block
  until it finishes (same semantics as `TimerQueue::cancel`);
  **self-deregistration on the firing thread does not wait**, preventing
  self-deadlock.
- `add_callback_publish`: for the "the callback will resume the consumer on
  another thread" scenario — it writes the `(state, id)` needed for
  deregistration into caller-provided storage **inside the same critical
  section that excludes `request_cancel`**. Otherwise the callback could win
  the race and resume the coroutine in the window "registered but id not yet
  published", making that write race with the consumer's reads.

Callback contract: **must be lightweight**. The cancellation source is often
TimerQueue's callback thread; resuming a whole request chain inline there would
stall every subsequent timer (docs/gaps.md §3.2).

## 6. The race protocol of cancellable suspension points (Slot/Waiter claim)

`ThreadPool::ScheduleAwaiter` and `AsyncSemaphore::AcquireAwaiter` are the only
two cancellable suspension points and share one protocol:

```
shared block (shared_ptr<Slot/Waiter>):
    h            the coroutine to resume
    claimed      atomic<bool>; the right to resume: whoever exchanges false→true resumes
    cancelled    written only by the successful claimer, read on the same thread after resume
    reg_id / cancel_state   deregistration info (written by on_cancel_publish)
```

Key points:

1. **The state lives outside the coroutine frame.** The normal waker (pool task
   / release) races the cancel callback to resume; the loser still holds a
   reference — which must not point into an awaiter/frame that may already be
   destroyed once the coroutine resumes. Hence the separately allocated
   `shared_ptr` block.
2. **After registering the callback, frame members are off-limits.**
   `suspend_impl` copies everything it needs into locals first (`this`,
   `pool`, `token` are all frame members); from the moment registration
   succeeds, the coroutine may be resumed on another thread and destroyed
   together with its frame at any time.
3. **Resuming in place on the cancel path is deliberate**: the resumed
   coroutine immediately throws `OperationCancelled` from `await_resume`,
   doing only the bounded work of exception unwinding. Posting to the pool
   instead would make the cancellation notice queue behind blocking tasks in
   exactly the scenario that needs it most — a saturated pool; implementing
   docs/gaps.md §3.2's suggestion literally deadlocks (see the corresponding
   case in test_concurrency). The firing-thread-side defense lives in
   TimerQueue: callbacks run on a dedicated callback thread, so unwinding does
   not stall expiry determination.
4. **Normal wakers skip waiters already claimed by cancellation**:
   `AsyncSemaphore::release_one` `claimed.exchange`s each popped waiter, moves
   on if it lost, and only increments the permit count if no real waiter is
   found.
5. **Register the callback first, then enqueue/check**, plus one `cancelled()`
   recheck after registration to cover the "cancelled before registration (the
   callback will never be invoked)" window.

Two supporting details on the `ThreadPool` side: `post` (continuation
delivery) and `schedule` (blocking tasks) use **separate queues**, and workers
always drain the continuation queue first — continuations are existing work
that already yielded a thread. The `schedule` queue is bounded; overflow goes
to a backlog released by workers as space frees up (backpressure). After
`join`, `post` does not throw (its consumers are noexcept paths) — it logs
ERROR and runs inline; `schedule` throws, and the awaiter claims before
throwing to block a second resume from the cancel callback.

## 7. TimerQueue: two threads, two jobs

The scheduling thread `loop()` only determines expiry and moves `DueItem`s
into the `due_` queue; the dedicated callback thread `fire_loop()` executes
callbacks serially. The split exists because `request_cancel` unwinds the
cancelled coroutine chain in place: running that on the scheduling thread
would stall **expiry determination** for every timer in the process. Callbacks
remain serial with each other (a slow callback delays the next; the
`slow`/`lag_seconds` metrics keep that observable).

The blocking semantics of `cancel(id)` (wait for a due-but-unfinished callback
to settle) spare callers a lifetime guard over the "fired but not yet
executed" window; self-cancellation on the callback thread does not wait.
After shutdown, `add` returns 0 (a valid-but-never-firing id is a debugging
trap) and `cancel(0)` is always a safe no-op.

## 8. Lifetime rules — quick checklist

A review checklist for coroutine-adjacent code; each rule has an incident
prototype above:

1. Anything living in a coroutine frame is off-limits once the coroutine may be
   resumed by another thread — copy into locals first (§6.2).
2. Wake-up state goes into a separately allocated shared block, never into the
   awaiter/frame (§6.1).
3. Event/pump objects must notify while holding the lock; the waiter may
   destroy them the moment it wakes (§2.1).
4. Self-destroying wrapper coroutines: either start lazily so the ramp has
   fully returned (`WhenAllRunner`), or prove the eager start safe; move an
   awaited task parameter out of the frame before awaiting it
   (`run_detached`).
5. After casting the final vote / triggering a resume, never touch the shared
   latch/slot again (§4.1, §6).
6. Keep cancel callbacks lightweight; when one must resume a coroutine, either
   confirm the unwind is bounded work (in place) or go through an executor
   (`AsyncSemaphore`'s normal wake-up) — never post cancellation notices into
   a possibly saturated pool.
7. No waiters may remain when an `AsyncSemaphore` is destroyed — the owner
   drains or `close()`s first; backends must `close()` before
   `ThreadPool::join` (the ordering in `Application::shutdown` is derived from
   exactly these constraints, see `src/app/app.cc`).
