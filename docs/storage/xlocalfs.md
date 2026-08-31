# xlocalfs 后端实现（io_uring 数据面）

xlocalfs 是 localfs 的 **io_uring 数据面变体**：磁盘布局、sidecar 元数据、
multipart 目录结构与原子提交协议与 localfs 完全一致（继承
`src/storage/localfs/localfs_backend.h:LocalFsBackend` 并共享 `fs_util` 落盘原语），
只把「数据字节搬运」——GET 流式读、PUT/分片流式写、complete 拼接、提交期
fdatasync——换成 io_uring 异步提交：磁盘等待期间**不占用任何线程**，完成后协程
续体投递回线程池继续执行。roadmap §3.4 之后进一步兑现了单 ring 单在途时代
留在桌上的收益：单流多在途流水线（§5）、注册缓冲/注册文件（§4）、linked SQE
与元数据 opcode（§5.2/§6）、多 ring 分片（§3）。

本文只讲 xlocalfs 特有的部分；共享的布局 / 提交 / 元数据语义见
[./localfs.md](./localfs.md)，总体定位见
[../storage-backend.md](../storage-backend.md) §3.3，协程与线程池模型见
[../concurrency.md](../concurrency.md)。duostore 的 fs 数据面复用同一引擎与
流（`fs_uring: true`），见 [./duostore-data-fs.md](./duostore-data-fs.md) §9。

## 1. 复用与替换边界

| 面 | 实现 | 说明 |
| --- | --- | --- |
| 磁盘布局 / sidecar / xattr | 复用 localfs | 同一 on-disk 格式，两种后端可互换指向同一 root |
| bucket 管理、HEAD/LIST、copy_object_fast、mpu 管理面 | 复用 localfs（线程池同步 IO） | io_uring 没有 getdents 等目录原语；元数据操作耗时短，异步化收益低 |
| GET 流式读 | **替换**：`xlocalfs_backend.cc:UringStreamBodyReader` → `uring_stream.h:UringReadStream` | read-ahead 流水线（§5.1），Range 天然支持 |
| PUT / UploadPart 流式写 | **替换**：`xlocalfs_backend.h:XLocalFsBackend::drain_to_tmp` → `UringWriteStream` | body 直读进流水线块（注册缓冲可用时零弹跳拷贝），hold-back 写流水线（§5.2） |
| complete 分片拼接 | **替换**：`XLocalFsBackend::complete_multipart` | 每分片一条 read-ahead 流喂共享写流水线；分片存在性 STATX 上 ring |
| 提交期数据持久化 | **替换**：`UringWriteStream::finish` | 末块 WRITE→FSYNC 链式一次提交（§5.2） |
| 提交 rename / 目录 fsync | **替换**：`XLocalFsBackend::commit_prepared` | RENAMEAT SQE + 目录 fd 的 FSYNC SQE（内核支持时，§6.2） |
| DELETE | **替换**：`XLocalFsBackend::delete_object` | UNLINKAT SQE（大文件 unlink 是真实盘上工作）；内核无 opcode 时整体落回基类 |
| per-key commit_lock、sidecar 写、PutCondition 检查 | 复用 localfs | 仍在池线程同步执行 |
| 指标 | 复用 localfs 的 `lights3_localfs_*` | on-disk 语义相同，靠 backend label 区分实例，构造时直传基类（`xlocalfs_backend.cc:XLocalFsBackend::XLocalFsBackend`） |

## 2. UringEngine：原生 syscall 上的最小封装

依赖政策（见 [../architecture.md](../architecture.md) §6）不引入 liburing：
`src/storage/xlocalfs/uring.cc` 直接以 `syscall(__NR_io_uring_setup/enter/register)`
封装三个系统调用（`uring.cc:sys_io_uring_setup` 等），只用内核 UAPI 头
`<linux/io_uring.h>` 里的结构体。引擎由 **1..N 个独立 ring**（§3）组成，每个
ring（`uring.cc:UringRing`）自带 SQ/CQ、提交互斥锁、在途表与专职收割线程：

- 提交侧：per-ring `submit_mu_` 互斥锁串行化 SQE 填充；`io_uring_enter` 由
  「值班 flusher」代表同批全部提交者批量执行（见 §2.3）。
- 完成侧：per-ring reaper 线程阻塞等待 CQE，把每条 CQE 交给其 Op 的完成
  接收器（`uring.h:UringEngine::Op::complete`）——普通 `co_await` 的
  `CoroOp` 把协程续体 `pool_->post` 回线程池恢复；流的 Slot（§5）则就地记录
  结果、仅在有消费者停靠时才唤醒。

### 2.1 建环与 SQ/CQ mmap 布局

`uring.cc:UringRing::UringRing`（每 ring 一次）：

1. `io_uring_setup(entries, &params)` 返回 ring fd，内核在 `params` 的
   `sq_off`/`cq_off` 中回填各字段在共享页内的偏移。请求 SQPOLL 失败
   （5.11 前需要 CAP_SYS_ADMIN，容器里常无）时以 EPERM/EINVAL 识别并**自动
   回退普通模式重建**——SQPOLL 是吞吐优化而非正确性前提。
2. 三段 `mmap`（`MAP_SHARED | MAP_POPULATE`，`uring.cc:ring_mmap`）：
   - SQ ring：`IORING_OFF_SQ_RING`，长度 `sq_off.array + sq_entries * 4`
     （head/tail/mask/flags + 索引数组）；
   - CQ ring：`IORING_OFF_CQ_RING`，长度 `cq_off.cqes + cq_entries * sizeof(io_uring_cqe)`；
     内核有 `IORING_FEAT_SINGLE_MMAP` 时 SQ/CQ 共用一段映射（取两者较大长度）；
   - SQE 数组：`IORING_OFF_SQES`，`sq_entries * sizeof(io_uring_sqe)`。
3. 按偏移解出裸指针存进成员（`sq_head_/sq_tail_/sq_flags_/sq_array_/sqes_/
   cq_head_/cq_tail_/cqes_` 等，见 `uring.cc:UringRing`）。这些指针指向与内核
   共享的页面，head/tail 一律用 `uring.cc:load_acquire`/`store_release`
   （`__atomic_load_n/store_n`）成对访问，充当 liburing 的 smp_load_acquire /
   smp_store_release。
4. 能力探测（§2.2，仅 ring 0 执行、全体共享）与注册资源（§4，逐 ring 执行，
   pre-5.13 内核的 REGISTER_* 会 quiesce ring，故必须在 reaper 启动前做完）。
   全部 ring 建好后统一启动 reaper 并打印一行
   `uring.cc:UringFeatures::describe` 的能力摘要日志。

`entries`（配置项 `queue_depth`，默认 256）即每 ring 请求的 SQ 深度；内核向上
取 2 的幂并通常给出 2 倍深度的 CQ。任何 mmap 失败都会解除已建映射、关 fd 并抛
`std::runtime_error`——构造失败由注册工厂兜底（§8）。

### 2.2 内核能力探测与降级

`uring.cc:UringRing::probe_features`，结果存 `uring.h:UringFeatures`：

- 用 `IORING_REGISTER_PROBE` 查询 opcode 支持位图。该注册项 5.6 才有，恰与
  `IORING_OP_READ/WRITE` 同版本落地：探测失败即按 5.1 基线保守假设——
  READV/WRITEV/FSYNC/NOP/READ_FIXED/WRITE_FIXED 必有，READ/WRITE、全部元数据
  opcode 与 link（5.3 有但无 probe 无从区分）必无。
- `op_read_write=false` 时读写降级为 **READV/WRITEV 单 iovec**：普通
  `co_await` 用 Awaitable 自带的 `iovec` 成员（与协程帧同生命周期），流用
  Slot 自带的（§5）。
- 元数据 opcode 逐个探测：`op_openat`/`op_statx`（5.6+）、
  `op_renameat`/`op_unlinkat`（5.11+）；无某项时对应调用点回退池线程阻塞
  syscall（§6）。`links`（IOSQE_IO_LINK 可用）与 probe 绑定。
- `op_fsync=false` 时 `UringWriteStream::finish` 回退池内阻塞 fdatasync。
- 探测「诚实」而非「先试一次看 -EINVAL」：否则第一发失败会落在真实请求上。

### 2.3 提交流程：批量 enter 与值班 flusher

一次 `co_await uring_->read/write/...` 的展开（`uring.h:UringEngine::Awaitable`）
落到所在 ring 的 `uring.cc:UringRing::submit`，它接受**一条 SQE 链**
（`std::span<const Sqe>`，流水线的 WRITE→FSYNC 链一次进来，见 §5.2）：

1. 持 `submit_mu_`：`stopped_/failed_` 直接抛 `InternalError`（引擎已停/已坏）。
2. **SQ 剩余槽位不足整条链**时的背压：若当前无人值班就自己执行一轮
   `flush_locked` 推进提交；否则在 `sq_cv_` 上等值班者腾出槽位（值班者要么
   推进要么置 `failed_`，两者都会 notify）。链必须完整落在同一提交窗口内
   （link 不得跨提交边界）。
3. 逐条把 `Op*` 登记进在途表 `inflight_`，`uring.cc:UringRing::push_sqe_locked`
   填 SQE：`user_data = Op*`，per-opcode flags（fsync_flags/open_flags/statx_flags/
   rename_flags/unlink_flags）共用 sqe union 的同一槽位，最后 `store_release`
   推进 `sq_tail_`。
4. 调 `uring.cc:UringRing::flush_locked` 提交。它是**批量提交**的核心
   （docs/archive/gaps.md §6.3）：若已有线程在 `io_uring_enter` 中（`flushing_`
   为真），本次直接返回——自己的 SQE 落在值班者的提交窗口 `[submitted_, sq_tail_)`
   内被捎带，高并发下 N 个 SQE 合并成一次 enter。值班者循环 enter 直到
   `submitted_` 追平 tail，enter 期间放锁；EINTR/EAGAIN/EBUSY 按
   `uring.cc:enter_backoff` 退避（前 8 轮 yield，之后指数退避、上限 1ms 睡眠）。

SQPOLL 模式下内核线程自取 SQE：仅当 `sq_flags_` 带 `IORING_SQ_NEED_WAKEUP`
（poll 线程睡了）才需要一次 `IORING_ENTER_SQ_WAKEUP` 的 enter，常态提交
**零 syscall**。

enter 返回不可恢复 errno（EBADF/EFAULT/ENXIO 等）即该 ring 报废：批量提交无法
只回滚自己的条目（同批捎带的 Op 早已挂起、其 submit 已返回），于是
`uring.cc:UringRing::fail_all_locked` 置 `failed_`、清空在途表，除提交者本人的
链外全部以 `res = -EIO` 唤醒；提交者本人由 `submit` 抛异常通知——普通
`co_await` 视作**从未挂起**，异常从 `co_await` 处正常传播；流的提交封装在
异常路径上撤销 Slot 状态与引用票（§5）。

### 2.4 完成流程：reaper 线程与 Op 完成接收器

`uring.cc:UringRing::reap_loop`（per-ring CQ 唯一消费者，读 `cq_head_` 无需加锁）：

1. 消费 `[head, tail)` 内的 CQE。`user_data == 0` 是 shutdown 的 NOP 哨兵
   （§7.2）；否则把 `user_data` 还原为 `Op*`。
2. **先持 `submit_mu_` 在 `inflight_` 中确认并摘除**，再触碰 `*op`：若
   `fail_all_locked` 已用 -EIO 唤醒过该 Op，其宿主（协程帧或流状态块）可能已
   销毁，这条迟到 CQE 只能跳过——再写结果或二次唤醒都是 UAF。
3. 确认在途后调 `op->complete(cqe.res)`（负值即 `-errno`，锁外执行）：
   - `uring.h:UringEngine::CoroOp`（普通 `co_await`）写 `res` 后
     `pool_->post([h]{ h.resume(); })` 把续体交回线程池，post 内部锁使 `res`
     写对恢复线程可见；
   - 流的 Slot（`uring_stream.cc:Slot::complete`）以原子交换置完成态，仅当
     消费者已停靠在该槽时才 post 唤醒（§5），随后归还一票引用——弃流后的
     末票会在这里（reaper 侧）释放整个流状态块。
4. `store_release(cq_head_, head)` 归还 CQE 槽位后，
   `io_uring_enter(fd, 0, 1, IORING_ENTER_GETEVENTS)` 阻塞等下一批。

两处特殊状态：GETEVENTS 遇不可恢复 errno 时同样 `fail_all_locked` + 全员 -EIO
唤醒后退出——静默退出会让所有在途 `co_await` 永不恢复（GET 悬死、连接不释放、
协程帧泄漏）而 `submit` 还在照常收新活。ring 已 `failed_` 后 reaper 不再阻塞等
CQE，改为 1ms 轮询，只为消化内核的迟到 CQE（按第 2 步登记表跳过）并在
`stopped_` 后退出——否则 shutdown 的 join 会在一个永远不来的 CQE 上等死。

跨线程可见性：提交线程 → 内核 → reaper 的 happens-before 经由 syscall 与 ring
屏障传递，TSan 观测不到，故在 Op 指针上加 `__tsan_release/acquire` 注解边
（`uring.cc` 头部的 `LIGHTS3_TSAN_RELEASE/ACQUIRE`）精确建模这条同步链。

关于 CQ 溢出：CQ 默认 2 倍于 SQ 深度；多在途流使单 ring 在途 op 数不再受
「每帧一发」约束，但仍受「流数 × 流深度 + 散 op」与 SQ 背压共同限制。5.5+ 内核
的 `IORING_FEAT_NODROP` 保证 CQE 不丢（溢出转入内核侧缓存）；更老内核本就
没有任何新 opcode，实际在途量极小，风险可忽略。

## 3. 多 ring 分片（roadmap §3.4 ④）

单 ring 意味着全进程一把提交锁、一个 reaper 线程——高核数下的单点。
`uring.h:UringOptions::rings`（配置项 `rings`，默认 1，`0 = auto =
hardware_concurrency/8`，钳制在 [1,8]）把引擎切成 N 个互相独立的
`UringRing`，`UringEngine` 只是它们之上的门面：

- **散 op**（普通 `co_await uring_->read/openat/...`）按
  `UringEngine::pick_ring` 轮询选 ring；
- **流**（§5）构造时选定一个 ring 并把全部 op 钉在上面——注册缓冲的
  `buf_index` 与注册文件表的槽位都是 **ring 作用域**的索引，跨 ring 无意义；
- 注册资源（§4）逐 ring 各一份，容量 = `rings × fixed_buffers × block_size`
  常驻锁页内存；
- 能力探测只在 ring 0 做一次（同一内核），`describe()` 摘要含 `rings=N`。

## 4. 注册缓冲与注册文件（roadmap §3.4 ②）

均为 ring 作用域资源，建环时注册（`uring.cc:UringRing::register_resources`），
失败只损失优化、绝不影响 ring 可用性（WARN 一行、相应 feature 位清零）：

- **fixed buffers**：一整块匿名映射按 `block_size` 切成 `fixed_buffers` 个块，
  每块一个 iovec 经 `IORING_REGISTER_BUFFERS` 注册，空闲索引栈式管理
  （`fixed_mu_` 独立于提交锁——流在 reaper 侧完成回收时不与提交者争锁）。
  流构造时 `UringEngine::try_acquire_fixed` 逐槽领取：领到的块直接作为流水线
  缓冲，IO 走 READ_FIXED/WRITE_FIXED——内核跳过每次 IO 的页 pin/unpin；池尽或
  注册被拒时**静默退化为堆块 + 普通 READ/WRITE**，两种块可在同一条流内共存。
- **fixed files**：`IORING_REGISTER_FILES` 注册全 -1 的**稀疏表**（5.5+，
  与 probe 绑定），流按需经 `IORING_REGISTER_FILES_UPDATE`（5.6+ 不 quiesce）
  把自己的 fd 写进一个空槽（`UringEngine::register_file`），此后 SQE 以
  `IOSQE_FIXED_FILE + fd=槽位` 引用，内核跳过每次 IO 的 fget/fput。仅
  预期长度 ≥ 512KiB（`uring_stream.cc:kFixedFileMinBytes`）的流才注册——
  两次 FILES_UPDATE syscall 要靠省下的 per-IO 开销摊回来。流结束（末票释放）
  时反注册；反注册失败则槽位退役不再发放（内核持引用直到 ring 拆除，
  绝不能带着陈旧文件再发出去）。

## 5. 多在途流：UringReadStream / UringWriteStream（roadmap §3.4 ①③）

引擎原始纪律是「每协程帧至多一个在途 op」——析构安全不言自明，但
queue_depth 在单请求内完全用不上：GET/PUT 每 64KiB 都要付一次完整的
提交→CQE 往返。`src/storage/xlocalfs/uring_stream.h` 在**多在途**下重建同等的
析构安全，靠一条所有权规则：

> 全部 IO 状态（块缓冲、fd、fixed 槽位）住在堆上的侵入式引用计数块
> `uring_stream.cc:StreamState` 里；**每个在途 op 持一票**，流本体持一票。
> 中途弃流（客户端断连、异常展开）不发 IORING_OP_ASYNC_CANCEL——磁盘 IO
> 在有界时间内必然完成，弃置的 op 自然完成、逐票归还，末票（可能在 reaper
> 线程上）执行析构：归还 fixed 缓冲、反注册文件槽、关 fd。内核仍在写的内存
> 绝不会被提前释放。

槽位（`uring_stream.cc:Slot`）自身就是 `UringEngine::Op`：完成接收器用单原子
字 `sync` 走 `kIdle → kInflight → kDone` 状态机，消费者停靠时把协程句柄 CAS
进 `sync`，完成侧交换出句柄再 post 唤醒——无锁、无 condvar。

### 5.1 UringReadStream：read-ahead 环形缓冲

`read_depth`（默认 4，按流长钳制）个块槽组成环：`fill()` 把空闲槽全部填上
超前于消费位置的块读；`read()` 等最老的槽完成后 memcpy 给调用方（64KiB 的
拷贝相对省下的往返是噪声，且这正是块可以用注册缓冲的原因），槽耗尽即回环
再填。串行消费者（HTTP 响应泵）因此与磁盘延迟重叠，而不是每块付一次往返。

EOF 语义与 `fs_util.h:FdStreamReader` 一致：短读/零读（文件被外部截断）提前
收尾。**中途短读**多一层考虑：晚于该块提交的 read-ahead 都瞄着「空洞之后」
的偏移，与消费位置对不上号——流上的世代号 `gen` 递增使它们完成后按「陈旧」
丢弃，提交位置回拨到实际短读处重灌流水线。

### 5.2 UringWriteStream：hold-back 写流水线 + 链式 fsync

`acquire()` 发一个空闲块（满时先结算最老在途写，短写透明重提交），调用方
直接把 body 读进块里再 `commit(n)`。**hold-back**：第 k 块的写 SQE 在第 k+1
块 commit 时才推出——`finish()` 执行时最后一块必然还未提交，可与 FSYNC
（fdatasync 语义）组成 `IOSQE_IO_LINK` 链**一次提交**：常见小对象「写 + 持久化」
只花一次提交、一次唤醒。中间块推出后与接收下一个 body 块重叠，在途至多
`write_depth`（默认 4）个。

finish 的边角：短写破坏 link（fsync 以 -ECANCELED 完成）时重提交剩余字节、
fsync 单独重发；内核无 FSYNC opcode 时回退池内阻塞 fdatasync；`-EINVAL`
（文件系统不支持）与 `fsutil::fsync_file` 同样容忍。流构造时 `dup()` fd：
SQPOLL 下内核在 poll 线程取件时才解析 SQE 的 fd，调用方（TmpFile 展开）先关
fd 不能使弃置的在途写踩到复用的 fd 号。

## 6. 数据面路径与 localfs 的差异

元数据 opcode（roadmap §3.4 ③）的通用形状：`xlocalfs_backend.cc:uring_open/
uring_rename/uring_unlink/uring_exists` 先查 `features().op_*` 与配置开关
`meta_ops`，具备才上 ring，否则原地阻塞 syscall（返回值统一 syscall 约定）。
路径字符串由调用方保活跨 `co_await`（SQPOLL 下内核延迟取件才拷路径）。

### 6.1 GET

open（冷 dentry/inode 查找是真实盘上工作）走 OPENAT SQE；fstat 紧随 open
之后是纯内存操作，保持普通 syscall。`UringStreamBodyReader` 把 fd 所有权交给
`UringReadStream`（§5.1）按 `[off, off+len)` 窗口 read-ahead，Range 天然成立。
meta 取**已打开 fd 的 fstat** 防并发覆盖错位；tier stub 竞态抛 `StubRace`
交给 tiered 走云端（见 [../tiered-storage.md](../tiered-storage.md) §7.3）；
延迟指标止于流句柄就绪。

### 6.2 PUT / UploadPart

`XLocalFsBackend::drain_to_tmp`：循环「`ws.acquire()` → `body.read` 直读进
流水线块（注册缓冲可用时省一次弹跳拷贝）→ MD5 更新（池线程用户态）→
`ws.commit(n)`」。提交次序保持 localfs 原样——`fsutil::set_meta_xattr` 先写，
然后 `ws.finish(fsync_enabled)` 送出「末块写 + 链式 fdatasync」。

之后的提交阶段换成 `XLocalFsBackend::commit_prepared`：与
`fs_util.h:commit_object_file(prepared=true)` 同序（数据 rename → 目录
fsync → sidecar），前两步上 ring——RENAMEAT SQE 与目录 fd 的 FSYNC SQE
（`XLocalFsBackend::sync_dir`，沿用 `fsutil::fsync_dir` 的静默失败语义）。
per-key `commit_lock` 与 PutCondition 检查在同一把锁内，协议不变。

`drain_to_tmp` 用**出参**而非返回 `pair<uint64_t,string>`：`body.read` 抛异常
（Content-MD5 不匹配、客户端断连）时，GCC 会对**从未构造**的绑定目标跑析构，
表现为 put 路径 double free / SEGV（docs/archive/gaps.md §5.6 的测试用例正是
此形状）；出参在 `co_await` 前已完整构造，展开销毁的是真实对象。

`upload_part` 同理：流水线写分片临时文件 → finish 链式持久化 → RENAMEAT 落位
→ 目录 FSYNC SQE。落盘次序与 localfs 一致——**先 rename 数据、后写 `.md5`**
（同号重传 last-write-wins；反序在断电后可能留下「`.md5` 有效但数据零块」）；
rename 失败时区分「上传已被并发 abort（目录没了）→ NoSuchUpload」与真实 IO
错误。

### 6.3 complete_multipart

分片校验（`.md5` 比对 ETag）与 localfs 相同，存在性检查换 STATX SQE
（`uring_exists`）；拼接阶段每分片开一条 read-ahead 流（OPENAT 上 ring、fd
归流所有）喂给共享的写流水线——分片读超前于写、写与读重叠，之后与 PUT 走
同一 `finish` + `commit_prepared` 提交，最后删除 mpu 目录。总 ETag 规则
（`md5(各分片二进制 md5 拼接)-N`）复用基类。

### 6.4 DELETE

内核有 UNLINKAT 且 `meta_ops` 开启时整个覆写基类：对象与 sidecar 各一发
UNLINKAT SQE（幂等语义同基类——ENOENT 不是错误，真实 EACCES/EIO 必须上抛，
静默 204 会骗客户端删除已发生；key 落在既有前缀目录上的 -EISDIR 按基类
`fs::remove` 语义处理），空父目录逐级清理不变。任一前提不满足则整体走基类。

## 7. 错误处理、关闭与并发/取消语义

### 7.1 错误处理

- **单次 IO 失败**：`cqe.res < 0` 由调用点转成 `S3Error(InternalError)` 并带
  `strerror(-res)`（`xlocalfs_backend.cc:throw_uring`、流内
  `uring_stream.cc:throw_uring`）；读到 0 视为外部截断的早 EOF（GET 流正常收尾）。
- **提交失败**：`submit` 抛出的异常从 `co_await` 处传播（协程未挂起过）或从
  流的 acquire/commit/finish 传播，正常展开——`TmpFile` RAII 清理 staging
  残留，`OpGuard` 计一次错误，流析构走弃流路径（§5）。
- **ring 报废**（enter/GETEVENTS 不可恢复 errno）：`failed_` 置位后该 ring
  拒绝一切新提交，在途 Op 全部以 -EIO 唤醒（表现为各调用点抛 InternalError）；
  注意内核理论上仍可能完成这些 IO 并写用户缓冲，但走到这一步的 errno 意味着
  ring 已不可用，两害相权取「通知调用者」。
- **构造失败**：见 §8 的注册工厂回退。

### 7.2 关闭

`XLocalFsBackend::close` = `uring_->shutdown()` + 链回基类 `close()`
（取消 mpu 周期清理定时器）。`uring.cc:UringRing::shutdown`（幂等，析构也调，
引擎 shutdown 逐 ring 执行）分三步，次序是安全性的关键：

1. **先置 `stopped_` 拒绝新提交**——否则「排空」无从谈起；
2. **等在途 CQE 清零**（`inflight_cv_`，上限 10s）：CQE 完成序**不保证** NOP
   哨兵排在既有读写之后，不排空就 munmap 会让内核继续往已释放的用户缓冲写
   （UAF，docs/archive/gaps.md §2.9）。超时只告警不死等——进程退出不可因此死锁，
   风险此刻已无法消除，至少留下证据；
3. 提交 `user_data=0` 的 **NOP 哨兵**唤醒并终止 reaper，join 之。ring 已
   `failed_` 时跳过 2/3 中的等待与哨兵（reaper 处于轮询退出路径）。

### 7.3 并发模型与取消

per-ring `submit_mu_` 保护 SQ 生产端 + 在途表 + 状态位；CQ 只有本 ring 的
reaper 一个消费者。**没有单请求取消**：引擎不用 `IORING_OP_ASYNC_CANCEL`。
散 op 的用户缓冲住在协程帧里、`co_await` 必等到自己的 CQE 才恢复，帧在恢复前
不销毁；流的缓冲住在引用计数状态块里，弃流后由在途 op 的末票释放（§5）。
客户端断连的取消表现为：PUT 侧 `body.read` 抛异常 → 流析构弃写；GET 侧上层
不再调 `read()` → `UringStreamBodyReader` 析构弃读——两者都不存在「内存已还
而 IO 在途」的窗口。shutdown 的排空覆盖进程退出路径。

## 8. 配置、注册与观测

`src/storage/registry.cc:ensure_registered` 注册 `type: xlocalfs`，参数在
localfs（root/staging/mpu_ttl/mpu_scan_interval）之上增加（映射
`uring.h:UringOptions`）：

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `queue_depth` | 256 | 每 ring 的 SQ 深度（`UringOptions::entries`） |
| `sqpoll` | false | 内核 SQ 轮询线程（常态提交零 syscall，代价是常驻内核线程） |
| `sqpoll_idle` | 100ms | poll 线程空转多久入睡（`sq_thread_idle`） |
| `rings` | 1 | 独立 ring 数（§3）；`0` = auto（核数/8，钳制 [1,8]） |
| `block_size` | 64KiB | 流水线块大小 = 注册缓冲粒度；须为 4096 的倍数 |
| `fixed_buffers` | 64 | 每 ring 注册缓冲块数（§4）；`0` 关闭。常驻锁页内存 `rings × fixed_buffers × block_size` |
| `fixed_files` | 256 | 每 ring 稀疏注册文件表槽数（§4）；`0` 关闭 |
| `read_depth` | 4 | 读流 read-ahead 深度（§5.1） |
| `write_depth` | 4 | 写流在途写深度（§5.2） |
| `meta_ops` | true | open/statx/rename/unlink 上 ring（§6）；`false` 一键回退同步 syscall |

构造抛异常（老内核、容器 seccomp 拦 `io_uring_setup`、memlock 配额不足）时工厂
**回退为 LocalFsBackend**：两者 on-disk 布局与语义完全相同，回退无损、只失去
异步 IO；除启动告警日志外，置常驻 gauge `lights3_xlocalfs_uring_fallback=1`，
让「以为在跑异步 IO 实际已回退」在监控面上持续可见。

启动成功则打印一行能力摘要（`UringFeatures::describe`）：probe 是否可用、
ring 数、READ/WRITE 还是 READV/WRITEV 回退、FSYNC 有无、fixed buffers/files
是否注册成功、link 可用性、各元数据 opcode、SQPOLL 是否真实启用、
SINGLE_MMAP/NODROP 特性位。数据面指标沿用 `lights3_localfs_*` 命名空间
（见 [./localfs.md](./localfs.md)），以 backend label 区分实例。

duostore 复用同一引擎的 `fs_uring*` 配置与回退 gauge
`lights3_duostore_uring_fallback` 见
[./duostore-data-fs.md](./duostore-data-fs.md) §9。
