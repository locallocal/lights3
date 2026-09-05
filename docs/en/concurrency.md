# Concurrency Model: Coroutines + Thread Pool

> English translation of [../concurrency.md](../concurrency.md). The Chinese original is authoritative; section numbering matches.

> Status: implemented. This document is both the design doc and the
> implementation reference for the L4 concurrency primitives;
> the code under `src/core/` is authoritative:

| File | Contents |
| --- | --- |
| `core/task.h` | `Task<T>` lazy coroutine, `sync_wait`, `when_all`, `with_timeout` |
| `core/executor.h` | `IExecutor` posting abstraction, `InlineExecutor`, `resume_on` |
| `core/thread_pool.h/.cc` | blocking IO thread pool, `schedule()` awaiter, `ThreadPoolExecutor` |
| `core/semaphore.h` | `AsyncSemaphore` coroutine-flavored async semaphore |
| `core/cancel.h` | `CancelSource` / `CancelToken` cooperative cancellation |
| `core/timer.h/.cc` | `TimerQueue` process-level timer thread |
| `core/background.h/.cc` | `BackgroundTaskGroup` background task wait group |

## 1. Overall Approach

Business logic (the L2/L3 interfaces) is written in **exactly one style**:
C++20 coroutines returning `Task<T>`. There are two kinds of execution
environments, bridged by a unified abstraction:

| Execution environment | Provided by | Runs what |
| --- | --- | --- |
| HTTP execution environment | the driver (asio io_context / seastar shard / the request thread of a synchronous library) | HTTP parsing, socket reads/writes |
| Blocking IO thread pool | `core/ThreadPool`; one shared instance by default, a backend may configure `io_threads` for a dedicated pool (§3.1) | posix file IO, cloudproxy's remote HTTP calls, and anything else that may block |

The rule in one sentence: **never call a potentially blocking function directly
inside a coroutine; `co_await pool.schedule()` over first, do the work, then
switch back.**

```cpp
Task<ObjectStream> LocalFsBackend::get_object(...) {
    co_await pool_->schedule(ctx.cancel);      // code below runs on a pool thread
    int fd = ::open(path.c_str(), O_RDONLY);   // blocking is OK here
    auto meta = read_sidecar(path);
    co_return ObjectStream{meta, make_fd_reader(fd, *pool_)};
}   // the caller resumes at its co_await — by default on the pool thread (see §3, resume policy)
```

Corollary: **L2/L3 code must be thread-affinity agnostic** — the continuation
may run on a pool thread, a driver thread, or the thread that requested
cancellation; it must not rely on TLS or a "always on the same thread"
assumption. Places that need to return to a specific execution environment
(beast touching its socket) are re-anchored by the driver itself (§4.1).

## 2. `Task<T>`: a Lightweight Lazy Coroutine

`Task<T>` is the only coroutine return type. Skeleton (full implementation in
`core/task.h`):

- the promise holds `variant<monostate, T, exception_ptr>`: values and
  exceptions are delivered uniformly through `await_resume()`, and exceptions
  are rethrown as-is at the `co_await` point;
- **lazy start** (`initial_suspend = suspend_always`): a `Task` is just a
  description; only `co_await` / `sync_wait` executes it. This makes
  composition easy (`when_all` and `with_timeout` both require "get the Task
  first, decide how to run it later");
- **symmetric transfer**: `co_await task` records the current coroutine as the
  continuation and directly `return task_handle` to transfer execution;
  `final_suspend` symmetrically transfers back — the call stack never grows;
- **move-only**, `operator co_await` is rvalue-only (`std::move(t)` or a
  temporary): a Task can be consumed only once, and its destructor destroys an
  unfinished coroutine frame.

Three top-level entry points / combinators:

### 2.1 sync_wait

```cpp
T sync_wait(Task<T> t);   // blocks the current thread until the coroutine completes
```

The bridge between synchronous drivers and the L1 boundary (§4.2). Internally
uses `SyncWaitEvent` (mutex + condvar): `start()` hooks the event pointer into
the promise and resumes; when `final_suspend` finds no continuation but an
event, it calls `set()`. The event's `notify` must happen while holding the
lock — the waiter may destroy the event object the moment it wakes up.

### 2.2 when_all

```cpp
Task<std::vector<T>> when_all(std::vector<Task<T>> tasks);
Task<void>           when_all(std::vector<Task<void>> tasks);
```

Awaits a set of Tasks concurrently; results are returned in input order. If any
fails, it **waits for all to finish** and then rethrows the first exception
(no early return — that would let still-flying tasks reference destroyed
inputs).

Implementation notes (each one a lesson from a real data race — read the
comments before touching the code):

- **Vote counting**: `WhenAllLatch` holds `n + 1` votes — one per runner, and
  the awaiter casts the (n+1)-th when it suspends; whoever casts the last vote
  resumes the `when_all` coroutine. The awaiter writes the continuation before
  voting, guaranteeing the runner side sees the continuation ready when the
  count hits zero;
- **runners are self-destroying wrapper coroutines**
  (`final_suspend = suspend_never`): each drives one child task, captures its
  exception, reports to the latch, and dies. After reporting (`arrive()`) it
  must never touch the latch again — the `when_all` frame may already have been
  destroyed inside the resume;
- runners are **lazily started + explicitly `start()`ed**: if they started
  running right inside the ramp (the coroutine-creating function), a child task
  might migrate to a pool thread and self-destruct while the ramp is still
  touching the coroutine frame.

Production consumer: the tiered backend's TierScanner sinking a batch of cold
objects concurrently.

### 2.3 Started

```cpp
template <class T> class Started {
    explicit Started(Task<T> t);   // starts immediately
    void start(Task<T> t);         // reuse the object (the previous child must be collected)
    bool pending() const;          // a child is in flight or finished-but-uncollected
    T wait();                      // thread-blocking collect (httplib's sync content provider)
    auto operator co_await();      // coroutine collect (beast / seastar / builtin's pumping loop)
    ~Started();                    // blocks until the child completes if uncollected
};
```

The "start now, collect later" single-child primitive behind the drivers'
double-buffered response pipeline ([http-adapter.md §2.4](http-adapter.md) ①):
issue the backend read of the next chunk, go write the current chunk to the
socket, then collect. Same self-destroying runner coroutine as `when_all` plus
a two-vote latch (one vote for the child, one for `co_await`; `wait()` does not
vote, it just waits on the condition variable). The child completes on whatever
thread its own suspension points chose; `co_await` resumes the collector on
that **completing thread** (inline when already finished), after which the
driver switches back to its connection context (strand / shard / pump) as usual.

- **The completing side's last touch is under the lock**: `complete()` sets the
  flag, casts its vote and copies the continuation out while holding the mutex,
  then unlocks and resumes -- the collector may destroy the object the moment it
  wakes (`wait()` returns or it is resumed);
- **The destructor waits**: the child writes into the owner's buffers
  (`StreamPrefetch`'s two chunks); a driver abandoning a response pays one read
  latency instead of freeing memory under a running read.

### 2.4 with_timeout

```cpp
Task<T> with_timeout(Task<T> task, std::chrono::milliseconds timeout, CancelSource src);
```

Cooperative timeout: registers a deadline callback with `TimerQueue` (§5.3),
and the callback does exactly one thing — `src.request_cancel()`. The task must
be constructed with `src.token()` and observe cancellation at suspension points
/ in long loops; a timeout surfaces as `OperationCancelled` bubbling out of the
task. Both normal and exceptional completion retract the timer. `src` should be
dedicated to this call: a source shared with others would, after a timeout,
also hit subsequent operations of the same request.

## 3. The Executor Abstraction and Thread Switching

```cpp
// src/core/executor.h
struct IExecutor {
    virtual void post(std::coroutine_handle<> h) = 0;   // post and (asynchronously) resume
};
```

Two implementations + one switching primitive:

1. **InlineExecutor**: resumes in place. A process singleton, and the explicit
   expression of "no switch";
2. **ThreadPoolExecutor**: `post` = wrap the resume into a task and enqueue it
   unbounded into the thread pool. `main.cc` uses it as the posting target for
   semaphore wakeups (§6);
3. `co_await resume_on(ex)`: switches the rest of the current coroutine onto
   the given executor; used when business code needs an explicit landing spot.

**Resume policy**: `Task::promise` carries a `cont_executor` (home executor)
pointer. When set, `final_suspend` does not do symmetric transfer but
`executor->post(continuation)` instead; a child task **inherits** the caller's
home executor at `co_await`, so a single `task.via(ex)` at the head of the
chain covers the entire coroutine chain.

The current production path **does not bind a home executor** (`via()` is
covered by unit tests and kept as an optional driver capability): by default
the continuation advances inline on the completing thread — after a pool thread
finishes the blocking section it keeps going through the L2 logic until the
next suspension point. This saves one thread switch, at the price of the
"thread-affinity agnostic" discipline of §1; positions that must return to a
specific execution environment are explicitly re-anchored by the driver (§4.1).

### 3.1 The ThreadPool Itself

Fixed size (config `runtime.io_threads`), a FIFO queue guarded by
mutex + condvar. Two enqueue paths with deliberately different semantics:

| Entry | Capacity | Purpose |
| --- | --- | --- |
| `post(fn)` | unbounded | continuation posting (executor post): must neither fail nor wait — losing a continuation means a hung request |
| `co_await schedule(token)` | bounded (default 4096) | business code switching onto a pool thread: when the queue is full the task goes onto a backlog wait list, released FIFO as workers free up slots |

- **Backpressure**: backlogged tasks do not consume queue capacity, but the
  `schedule()` coroutine stays suspended — effectively propagating the pressure
  back to the request coroutine and ultimately the socket read, preventing
  unbounded pile-up of blocking tasks;
- **Metrics** (`stats()`, see [s3-protocol.md](s3-protocol.md) §7):
  queue depth, backlog length, and completion counts are exposed via
  `/-/metrics` and are the main signals for capacity tuning; the
  enqueue-to-start wait-time histogram (<1ms / <10ms / <100ms / <1s / ≥1s)
  is output as the `lights3_pool_wait_seconds` histogram — the direct data
  source for the dedicated-pool criterion below;
- `join()`: stops accepting new tasks, **drains the queue** (including the
  backlog), then waits for the threads to exit; `post/schedule` after join
  throws.

localfs / xlocalfs / tiered / cloudproxy / duostore share this pool by default.
**Per-backend dedicated pools** (landed): any backend configured
with `io_threads: N` (a generic key, [1,1024]) gets its own ThreadPool — when
slow cloud requests fill the shared pool and starve the local-disk path,
per-backend isolation keeps them out of each other's way (the Registry injects
by parameter at construction time, `backend_pool` in `storage/registry.cc`).
The shared default is the right choice for most deployments; isolation is a
targeted measure to enable "after confirming starvation symptoms (the wait-time
histogram shifting right)" — read the criterion straight off
`lights3_pool_wait_seconds` in `/-/metrics` (a rightward shift is the
starvation symptom); cloudproxy
additionally has a local workaround with its private pump threads, see
[cloudproxy-backend.md](cloudproxy-backend.md) §2.3. A dedicated pool lives and
dies with the backend's shared_ptr (destruction joins it); the observability
metrics `lights3_backend_pool_{threads,queue_depth,backlogged,completed}` carry
a backend label and are exposed via the backend registry, in a namespace
distinct from the global pool's unlabeled `lights3_pool_*`.

### 3.2 The Cancellation Race in schedule()

The awaiter returned by `schedule(token)` faces two concurrent resume sources:
the pool task (normal path) and the cancel callback (token fired, §5). The
constraint is **exactly one resume**, and the loser's reference must not point
at a destroyed object. Implementation (`ScheduleAwaiter`):

- the suspension state lives in a standalone `shared_ptr<Slot>` shared block
  (handle, `claimed` atomic flag, cancel-deregistration info), with the pool
  task and the cancel callback each holding a copy — what the loser touches is
  the shared block, not an awaiter/coroutine frame that may already have been
  destroyed by the coroutine resuming;
- both sides claim via `claimed.exchange(true)` first; the winner resumes. If
  the cancel callback wins it sets the `cancelled` flag, and `await_resume`
  deregisters the callback and throws `OperationCancelled`;
- the cancel callback is registered with `on_cancel_publish` (§5.1): the
  `(state, id)` needed for deregistration is put in place inside the
  registration critical section, closing the window "callback races ahead to
  resume after registration but before the info lands";
- after registering, re-check `token.cancelled()`: covers the path "already
  cancelled before registration, so the callback will never be registered" —
  in that case do not suspend, throw in place;
- `schedule` after `join()`: claim first, then throw (blocking a second resume
  from the cancel callback); the exception surfaces at the `co_await`.

## 4. Unifying the Two Kinds of HTTP Drivers

How a driver hooks its own execution model up to `Task` — the detailed contract
is in [http-adapter.md](http-adapter.md) §4; here only the coroutine seams.

### 4.1 Asynchronous Drivers (beast / seastar)

The connection session itself is a `Task<void>` coroutine hanging off the
driver's event loop: the handler is simply
`resp = co_await handler_(std::move(req))`, with no thread ever blocked.

The key point is **re-anchoring the execution environment**: the handler's
continuation may resume on a pool thread (the default policy of §3), while
asio's socket/stream is not thread-safe. The beast driver inserts
`co_await ResumeOn{stream.get_executor()}` (a driver-internal awaiter,
`asio::post` onto the connection's own strand) at every spot that touches the
socket: after the handler returns, and at the entry of
`BeastBodyReader::read`. The effect is equivalent to "the home executor applies
only to L1's own code segments", while L2/L3 remain thread-affinity agnostic.

### 4.2 Synchronous Drivers (builtin / httplib)

Thread-per-request: the request thread directly runs
`resp = sync_wait(handler_(std::move(req)))` and blocks while waiting — that is
exactly this model's semantics, with no extra cost. When a child task completes
on a pool thread, the continuation is advanced in place to the next suspension
point or to completion, and the `sync_wait` event is woken.

Streaming responses work the same way: the driver's content provider does
`sync_wait(body->read(buf))` per chunk. httplib's request body is a push model,
inverted into a pull model by a pump thread through a bounded buffer queue
(`http/pushpull.h`); the queue capacity is the backpressure.

## 5. Cancellation and Timeouts

Two cancellation sources are wired: **request timeout**
(`http.request_timeout`, `with_timeout` inside dispatch) and **process
shutdown** (main.cc's shutdown source); both feed one request-level
CancelSource that propagates down the Task promise chain automatically.
**Client disconnect is deliberately not an independent cancellation source**:
disconnects are sensed wherever the socket is touched — body reads throw,
response writes fail and the result is discarded, and L2/L3 unwind via RAII; a
long handler that never touches the socket (e.g. the metadata phase of a GET on
a slow backend) is bounded by the request timeout. Independent detection would
mean one extra watcher thread per connection on the synchronous drivers
(builtin/httplib), and on async drivers a concurrent half-close watch that must
not swallow the next pipelined request's bytes — on both sides the cost
outweighs "runs at most until request_timeout".
Philosophy: **cooperative, no preemption** — a
blocking syscall in progress is left to return naturally, after which
suspension points / long loops check the token and decide to stop. This is
self-consistent with the thread-pool model.

### 5.1 CancelSource / CancelToken (core/cancel.h)

- `CancelSource`: the triggering end, holds the `CancelState` (atomic flag +
  callback table); `request_cancel()` is idempotent, and callbacks are
  **taken out and run outside the lock** (a callback may itself operate on
  tokens);
- `CancelToken`: the observing end, freely copyable, passed with
  `RequestContext` into the handler and down to L3. Default-constructed means
  "never cancelled", making it free for callers who don't care;
- `token.on_cancel(fn)` returns a `CancelRegistration` (RAII; destruction
  deregisters). If already cancelled at registration time, the callback is
  **not registered** and an empty handle is returned — the caller must then
  check `cancelled()` itself to close the race window;
- `on_cancel_publish(fn, out_id, out_state)`: for the scenario where "the
  callback will resume the user across threads" (§3.2). Mutually exclusive with
  `request_cancel`, guaranteeing the deregistration info is in place before the
  callback can fire;
- cancellation uniformly manifests as an `OperationCancelled` exception
  surfacing from a suspension point; the L2 catch-all maps it to
  disconnect/499 semantics (see the contract in [http-adapter.md](http-adapter.md) §4).

### 5.2 Observation Points

- `pool.schedule(token)`: cancelled while queued → resumes immediately with the
  exception (§3.2);
- between chunks of streaming reads/writes: `token.throw_if_cancelled()`;
- cloudproxy's remote stream: destroying the reader aborts the remote transfer
  (see [cloudproxy-backend.md](cloudproxy-backend.md) §3.1).

### 5.3 TimerQueue (core/timer.h)

A process-level, single-threaded timer: `add(delay, fn)` returns an id,
`cancel(id)` retracts a not-yet-fired timer (does not wait for a callback in
progress). Callbacks run on the timer thread and **must be lightweight** —
typically just `request_cancel()` or an executor post. Both `with_timeout`
(§2.3) and request-level timeouts are built on it.

## 6. Concurrency Control and Throttling

`AsyncSemaphore` (core/semaphore.h): a coroutine-flavored semaphore; an
over-limit `acquire()` suspends and queues (FIFO) instead of rejecting.

```cpp
auto permit = co_await sem.acquire();   // Permit is an RAII grant
// ... automatically released when the coroutine frame exits (including exception paths)
auto maybe = sem.try_acquire();         // non-blocking: nullopt immediately if no permit, never queues
```

`try_acquire` serves callers that "already hold one and want one more" (the
rados double-buffered pipeline, duostore-rados-data.md §4.2): a nested blocking
acquire would deadlock when everyone holds one each and waits for a second; the
try semantics degrade the second permit to "pipeline if available, serialize if
not".

Implementation notes:

- **Direct permit hand-off**: on `release`, if there are waiters the permit is
  not added back to the count but handed straight to the queue head —
  first-come-first-served, never overtaken by a freshly arriving `acquire`;
- **Wakeup posting**: a `resume_executor` may be passed at construction. If
  empty, waiters are resumed in place on the `release` call stack — with heavy
  queuing under a synchronous driver this forms deep recursion of "finish one
  request → inline-run the next request"; production paths should pass the pool
  executor.

Production consumers:

| Site | Purpose |
| --- | --- |
| `main.cc` dispatch entry | `runtime.max_inflight_requests` global throttling; over-limit requests queue rather than being rejected; waiters woken via the pool executor. For streaming responses the Permit is tied to `stream_body` (outermost wrapper) and released only when the driver finishes or discards the body — throttling covers the whole response transfer, not just the handler coroutine frame; shutdown draining judges in-flight via `available()`, so it counts mid-stream requests too |
| tiered `transfers_` | `max_concurrent_transfers`: concurrency cap on sink/recall transfers (see [tiered-storage.md](tiered-storage.md) §5.1) |
| tiered `key_locks_` | permits=1 used as an async mutex: striped per-key locks, protecting only the state-commit section (see [tiered-storage.md](tiered-storage.md) §7.3) |
| rados `buffer_sem_` | `rados_buffer_total` overall write-buffer budget: first share via blocking acquire (backpressure), second share of the double buffer via try_acquire (see [duostore-rados-data.md](duostore-rados-data.md) §4.2) |

In addition: per-connection serial processing (HTTP/1.1 pipelining is not
executed in parallel) is guaranteed by the driver; the thread pool's bounded
queue + backlog (§3.1) is the bottom-most second gate.

## 7. Lifetime of Background Tasks

`BackgroundTaskGroup` (core/background.h/.cc): a background task wait group,
extracted from the verbatim-duplicated private mechanisms of
TieredBackend / DuoStoreBackend (the lifetime-critical path is maintained in
exactly one place), now shared by duostore, tiered, and CredentialStore
(credential-management.md §10.3). The problem it solves: timer-driven
background coroutines (GC / scans / credential sync) racing against their
owner's destruction — on close, resources may be torn down only after
guaranteeing "no new tasks are produced + in-flight tasks have drained to
zero".

Three registration entry points (each atomically mutually exclusive with the
closing check; all rejected once closing):

- `spawn(task)`: from a timer callback, derives a **self-destroying top-level
  coroutine** to drive the task; exceptions are caught internally and logged as
  WARN, never propagated; after closing it returns false and the task is
  dropped without running;
- `enter()/exit()` (prefer the RAII `Scope`): registers a caller-driven
  coroutine (e.g. a manual GC hook) as in-flight; close waits for it to finish
  before tearing down resources; `enter`/`Scope::ok()` returning false means
  closing is in progress and the caller should return immediately;
- `if_open(fn)`: runs fn atomically with the closing check; typically for
  scenarios where "check not-closed + record the timer id" must happen in one
  step.

The iron rule of closing (the conventional sequence, per the header comment of
`background.h`): **`begin_close()` → cancel timers outside the lock →
`wait_idle()` → tear down resources**. `begin_close` sets closing, after which
spawn/enter/if_open are all rejected; `TimerQueue::cancel` blocks waiting for
in-flight callbacks and must not be called while holding this group's lock —
callbacks call spawn/if_open which take the group lock, so cancelling under the
lock deadlocks; `wait_idle()` blocks the calling thread until in-flight tasks
drain to zero (tasks wrap up on pool threads), and only then comes the resource
teardown.
