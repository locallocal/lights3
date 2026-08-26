# xlocalfs 后端实现（io_uring 数据面）

xlocalfs 是 localfs 的 **io_uring 数据面变体**：磁盘布局、sidecar 元数据、
multipart 目录结构与原子提交协议与 localfs 完全一致（继承
`src/storage/localfs/localfs_backend.h:LocalFsBackend` 并共享 `fs_util` 落盘原语），
只把「数据字节搬运」——GET 流式读、PUT/分片流式写、complete 拼接、提交期
fdatasync——换成 io_uring 异步提交：磁盘等待期间**不占用任何线程**，完成后协程
续体投递回线程池继续执行。

本文只讲 xlocalfs 特有的部分；共享的布局 / 提交 / 元数据语义见
[./localfs.md](./localfs.md)，总体定位见
[../storage-backend.md](../storage-backend.md) §3.3，协程与线程池模型见
[../concurrency.md](../concurrency.md)。

## 1. 复用与替换边界

| 面 | 实现 | 说明 |
| --- | --- | --- |
| 磁盘布局 / sidecar / xattr | 复用 localfs | 同一 on-disk 格式，两种后端可互换指向同一 root |
| bucket 管理、HEAD/DELETE/LIST、copy_object_fast、mpu 管理面 | 复用 localfs（线程池同步 IO） | io_uring 没有 getdents 等目录原语；元数据操作耗时短，异步化收益低 |
| GET 流式读 | **替换**：`xlocalfs_backend.cc:UringBodyReader` | 带偏移 pread 语义，Range 天然支持 |
| PUT / UploadPart 流式写 | **替换**：`xlocalfs_backend.h:XLocalFsBackend::drain_to_tmp` | 读 body → io_uring 写 staging 临时文件 |
| complete 分片拼接 | **替换**：`XLocalFsBackend::complete_multipart` | 分片读与拼接写都走 ring |
| 提交期数据持久化 | **替换**：`XLocalFsBackend::sync_fd`（FSYNC SQE） | 原 fdatasync 是提交路径上最长的阻塞点 |
| 提交（rename + sidecar）、fsync_dir、per-key commit_lock | 复用 localfs | 仍在池线程同步执行；见 [§4](#4-数据面路径与-localfs-的差异) |
| 指标 | 复用 localfs 的 `lights3_localfs_*` | on-disk 语义相同，靠 backend label 区分实例，构造时直传基类（`xlocalfs_backend.cc:XLocalFsBackend::XLocalFsBackend`） |

## 2. UringEngine：原生 syscall 上的最小封装

依赖政策（见 [../architecture.md](../architecture.md) §6）不引入 liburing：
`src/storage/xlocalfs/uring.cc` 直接以 `syscall(__NR_io_uring_setup/enter/register)`
封装三个系统调用（`uring.cc:sys_io_uring_setup` 等），只用内核 UAPI 头
`<linux/io_uring.h>` 里的结构体。整个引擎是**单 ring + 单收割线程**：

- 提交侧：`submit_mu_` 互斥锁串行化 SQE 填充；`io_uring_enter` 由「值班
  flusher」代表同批全部提交者批量执行（见 §2.3）。
- 完成侧：专职 reaper 线程阻塞等待 CQE，把完成的协程续体 `pool_->post`
  回线程池恢复——后续的同步落盘调用（rename/sidecar/fsync_dir）天然回到池线程。

### 2.1 建环与 SQ/CQ mmap 布局

`uring.cc:UringEngine::UringEngine`：

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
   cq_head_/cq_tail_/cqes_` 等，见 `uring.h:UringEngine`）。这些指针指向与内核
   共享的页面，head/tail 一律用 `uring.cc:load_acquire`/`store_release`
   （`__atomic_load_n/store_n`）成对访问，充当 liburing 的 smp_load_acquire /
   smp_store_release。
4. 能力探测（§2.2）后启动 reaper 线程，并打印一行
   `uring.cc:UringFeatures::describe` 的能力摘要日志。

`entries`（配置项 `queue_depth`，默认 256）即请求的 SQ 深度；内核向上取 2 的幂
并通常给出 2 倍深度的 CQ。任何 mmap 失败都会解除已建映射、关 fd 并抛
`std::runtime_error`——构造失败由注册工厂兜底（§7）。

### 2.2 内核能力探测与降级

`uring.cc:UringEngine::probe_features`，结果存 `uring.h:UringFeatures`：

- 用 `IORING_REGISTER_PROBE` 查询 opcode 支持位图。该注册项 5.6 才有，恰与
  `IORING_OP_READ/WRITE` 同版本落地：探测失败即按 5.1 基线保守假设——
  READV/WRITEV/FSYNC/NOP 必有，READ/WRITE 必无。编译期以
  `#ifdef IO_URING_OP_SUPPORTED`（与 probe 同批进头文件的宏）判断头文件新旧。
- `op_read_write=false` 时读写降级为 **READV/WRITEV 单 iovec**：
  `uring.h:UringEngine::Awaitable::await_suspend` 现场把 `(addr,len)` 装进
  Awaitable 自带的 `iovec` 成员——它与 Awaitable 同住协程帧，挂起期间保持有效。
- `op_fsync=false` 时 `sync_fd` 回退池内阻塞 fdatasync（§4.2）。
- 探测「诚实」而非「先试一次看 -EINVAL」：否则第一发失败会落在真实请求上。

### 2.3 提交流程：批量 enter 与值班 flusher

一次 `co_await uring_->read/write/fdatasync(...)` 的展开
（`uring.h:UringEngine::Awaitable`）：

1. `await_suspend` 记下协程句柄后调 `uring.cc:UringEngine::submit`；
2. `submit` 持 `submit_mu_`：
   - `stopped_/failed_` 直接抛 `InternalError`（引擎已停/已坏）；
   - **SQ 满**（`*sq_tail_ - head >= sq_entries_`）时的背压：若当前无人值班就
     自己执行一轮 `flush_locked` 推进提交；否则在 `sq_cv_` 上等值班者腾出
     槽位（值班者要么推进要么置 `failed_`，两者都会 notify）；
   - 把 `Op*` 登记进在途表 `inflight_`，随后
     `uring.cc:UringEngine::push_sqe_locked` 填 SQE：`user_data = Op*`，FSYNC 的
     DATASYNC 标志写 `sqe.fsync_flags` 而非 `len`（两者在 sqe union 的不同偏移），
     最后 `store_release` 推进 `sq_tail_`；
   - 调 `uring.cc:UringEngine::flush_locked` 提交。
3. `flush_locked` 是**批量提交**的核心（docs/archive/gaps.md §6.3）：若已有线程在
   `io_uring_enter` 中（`flushing_` 为真），本次直接返回——自己的 SQE 落在值班者
   的提交窗口 `[submitted_, sq_tail_)` 内被捎带，高并发下 N 个 SQE 合并成一次
   enter。值班者循环 enter 直到 `submitted_` 追平 tail，enter 期间放锁；
   EINTR/EAGAIN/EBUSY 按 `uring.cc:enter_backoff` 退避（前 8 轮 yield，之后指数
   退避、上限 1ms 睡眠）。
4. SQPOLL 模式下内核线程自取 SQE：仅当 `sq_flags_` 带 `IORING_SQ_NEED_WAKEUP`
   （poll 线程睡了）才需要一次 `IORING_ENTER_SQ_WAKEUP` 的 enter，常态提交
   **零 syscall**。

enter 返回不可恢复 errno（EBADF/EFAULT/ENXIO 等）即引擎报废：批量提交无法只回滚
自己的条目（同批捎带的 Op 早已挂起、其 submit 已返回），于是
`uring.cc:UringEngine::fail_all_locked` 置 `failed_`、清空在途表，除提交者本人外
全部以 `res = -EIO` 经 `pool_->post` 唤醒；提交者本人由 `submit` 抛异常通知——
此时协程视作**从未挂起**，异常从 `co_await` 处正常传播。

### 2.4 完成流程：reaper 线程与协程恢复

`uring.cc:UringEngine::reap_loop`（CQ 唯一消费者，读 `cq_head_` 无需加锁）：

1. 消费 `[head, tail)` 内的 CQE。`user_data == 0` 是 shutdown 的 NOP 哨兵
   （§6）；否则把 `user_data` 还原为 `Op*`。
2. **先持 `submit_mu_` 在 `inflight_` 中确认并摘除**，再触碰 `*op`：若
   `fail_all_locked` 已用 -EIO 唤醒过该 Op，其协程帧（连同 Op）可能已在异常
   展开中销毁，这条迟到 CQE 只能跳过——再写 `res` 或二次 resume 都是 UAF。
3. 确认在途后写 `op->res = cqe.res`（负值即 `-errno`），
   `pool_->post([h]{ h.resume(); })` 把续体交回线程池：post 内部锁使 `res` 写
   对恢复线程可见；`co_await` 的返回值就是 `await_resume` 读出的 `res`。
4. `store_release(cq_head_, head)` 归还 CQE 槽位后，
   `io_uring_enter(fd, 0, 1, IORING_ENTER_GETEVENTS)` 阻塞等下一批。

两处特殊状态：GETEVENTS 遇不可恢复 errno 时同样 `fail_all_locked` + 全员 -EIO
唤醒后退出——静默退出会让所有在途 `co_await` 永不恢复（GET 悬死、连接不释放、
协程帧泄漏）而 `submit` 还在照常收新活。引擎已 `failed_` 后 reaper 不再阻塞等
CQE，改为 1ms 轮询，只为消化内核的迟到 CQE（按第 2 步登记表跳过）并在
`stopped_` 后退出——否则 shutdown 的 join 会在一个永远不来的 CQE 上等死。

跨线程可见性：提交线程 → 内核 → reaper 的 happens-before 经由 syscall 与 ring
屏障传递，TSan 观测不到，故在 Op 指针上加 `__tsan_release/acquire` 注解边
（`uring.cc` 头部的 `LIGHTS3_TSAN_RELEASE/ACQUIRE`）精确建模这条同步链。

## 3. queue_depth 与背压

`uring.h:UringOptions::entries`（`queue_depth`，默认 256）限定同时在环上的 SQE
数。提交侧在 SQ 满时于 `submit` 内自旋/等待（§2.3），协程所在的池线程会短暂
阻塞在 `sq_cv_` 上——但这只在「flusher 落后于填充」的瞬时发生；稳态并发度由上层
HTTP 连接数与线程池深度共同约束，256 深度对 64KiB 块的流式读写绰绰有余。
`IORING_FEAT_NODROP` 之外的老内核 CQ 溢出会丢 CQE，本实现依靠 CQ 默认 2 倍于
SQ 深度加上「每协程同时至多一个在途 SQE」的使用模式规避（所有调用点都是
`co_await` 串行发起，绝无一帧多发）。

## 4. 数据面路径与 localfs 的差异

### 4.1 GET

localfs 的 `fs_util.h:FdStreamReader` 每次 `read()` 经 `pool_->schedule()`
在池线程做阻塞 `pread`——磁盘等待期间**占住一个池线程**。xlocalfs 的
`xlocalfs_backend.cc:UringBodyReader` 结构相同（fd + offset + remaining，析构关
fd，早 EOF 语义处理外部截断），但 `read()` 变成
`co_await eng_->read(fd, buf, offset)`：等待期间零线程占用，CQE 到来后在池线程
恢复。带偏移读让 Range 请求天然成立，无需 lseek。

`XLocalFsBackend::get_object` 的控制面与 localfs 逐行同构：open/fstat 仍是池线程
同步调用（open 无法走 ring 收益也小）；meta 取**已打开 fd 的 fstat** 防并发覆盖
错位；tier stub 竞态抛 `StubRace` 交给 tiered 走云端（见
[../tiered-storage.md](../tiered-storage.md) §7.3）；延迟指标止于流句柄就绪。

### 4.2 PUT / UploadPart

localfs 在池线程内联「`body.read(64KiB)` → 阻塞 write 循环 → 增量 MD5」；
xlocalfs 抽成 `XLocalFsBackend::drain_to_tmp`：MD5 更新仍在（恢复后的）池线程
用户态完成，写盘换成 `XLocalFsBackend::write_all`——内核可能短写，循环
`co_await uring_->write` 直到写完，返回 0 或负值抛 `InternalError`。

`drain_to_tmp` 用**出参**而非返回 `pair<uint64_t,string>`：`body.read` 抛异常
（Content-MD5 不匹配、客户端断连）时，GCC 会对**从未构造**的绑定目标跑析构，
表现为 put 路径 double free / SEGV（docs/archive/gaps.md §5.6 的测试用例正是此形状）；
出参在 `co_await` 前已完整构造，展开销毁的是真实对象。

提交期差异集中在持久化一步：`XLocalFsBackend::sync_fd` 用 FSYNC SQE
（`fsync_flags = IORING_FSYNC_DATASYNC`，即 fdatasync 语义）替换阻塞
fdatasync；内核无 FSYNC opcode（按探测）时回退池内 `fsutil::fsync_file`，
`LIGHTS3_FSYNC=0` 时整体 no-op，`-EINVAL`（文件系统不支持）被容忍。为此
`put_object` 自己按原顺序做完 `fsutil::set_meta_xattr` → `sync_fd`，再以
`prepared=true` 调 `fs_util.h:commit_object_file` 跳过其内部的阻塞持久化，
其余提交协议不变：同一把 `LocalFsBackend::commit_lock` 分条纹 per-key 锁内做
条件检查（PutCondition 合同）+ 双 rename，锁唤醒后 `pool_->schedule()` 回池线程
再碰盘。

`upload_part` 同理：ring 写分片临时文件 → FSYNC SQE → 池线程 rename。落盘次序
与 localfs 一致——**先 rename 数据、后写 `.md5`**（同号重传 last-write-wins；
反序在断电后可能留下「`.md5` 有效但数据零块」）；rename 失败时区分「上传已被
并发 abort（目录没了）→ NoSuchUpload」与真实 IO 错误。`fsync_dir` 仍是池线程
同步调用（io_uring 的 FSYNC 用在数据 fd 上，目录持久化保持原样）。

### 4.3 complete_multipart

分片校验（`.md5` 比对 ETag）与 localfs 相同；拼接阶段读写都换 ring：256KiB
缓冲循环 `co_await uring_->read` 分片 → `write_all` 追加进最终临时文件，之后与
PUT 走同一 `prepared=true` 提交（xattr → FSYNC SQE → commit_lock 内 rename），
最后删除 mpu 目录。总 ETag 规则（`md5(各分片二进制 md5 拼接)-N`）复用基类。

## 5. 错误处理

- **单次 IO 失败**：`cqe.res < 0` 由调用点的
  `xlocalfs_backend.cc:throw_uring` 转成 `S3Error(InternalError)` 并带
  `strerror(-res)`；读到 0 视为外部截断的早 EOF（GET 流正常收尾）。
- **提交失败**：`submit` 抛出的异常从 `co_await` 处传播，协程未挂起过，
  正常展开——`TmpFile` RAII 清理 staging 残留，`OpGuard` 计一次错误。
- **引擎报废**（enter/GETEVENTS 不可恢复 errno）：`failed_` 置位后拒绝一切新
  提交，在途协程全部以 -EIO 恢复（表现为各调用点抛 InternalError）；注意内核
  理论上仍可能完成这些 IO 并写用户缓冲，但走到这一步的 errno 意味着 ring 已
  不可用，两害相权取「通知调用者」。
- **构造失败**：见 §7 的注册工厂回退。

## 6. 关闭与并发/取消语义

`XLocalFsBackend::close` = `uring_->shutdown()` + 链回基类 `close()`
（取消 mpu 周期清理定时器）。`uring.cc:UringEngine::shutdown`（幂等，析构也调）
分三步，次序是安全性的关键：

1. **先置 `stopped_` 拒绝新提交**——否则「排空」无从谈起；
2. **等在途 CQE 清零**（`inflight_cv_`，上限 10s）：CQE 完成序**不保证** NOP
   哨兵排在既有读写之后，不排空就 munmap 会让内核继续往已释放的用户缓冲写
   （UAF，docs/archive/gaps.md §2.9）。超时只告警不死等——进程退出不可因此死锁，风险
   此刻已无法消除，至少留下证据；
3. 提交 `user_data=0` 的 **NOP 哨兵**唤醒并终止 reaper，join 之。引擎已
   `failed_` 时跳过 2/3 中的等待与哨兵（reaper 处于轮询退出路径）。

并发模型一句话：`submit_mu_` 保护 SQ 生产端 + 在途表 + 状态位；CQ 只有 reaper
一个消费者；用户缓冲都住在协程帧里，帧在 CQE 恢复前不销毁，shutdown 的排空
保证覆盖了进程退出路径。

**没有单请求取消**：引擎不用 `IORING_OP_ASYNC_CANCEL`，一次 `co_await` 必然等到
自己的 CQE（或引擎失效的 -EIO）才恢复。客户端断连的取消表现为 `body.read` 在两
次 ring 操作**之间**抛异常：此时无在途 SQE，展开安全；GET 侧则是上层不再调
`read()`、`UringBodyReader` 在两次读之间析构关 fd——同样不存在「fd 已关而 IO 在
途」的窗口。这是「每帧至多一个在途 op、且 co_await 到底」的使用纪律换来的简单性。

## 7. 配置、注册与观测

`src/storage/registry.cc:ensure_registered` 注册 `type: xlocalfs`，参数在
localfs（root/staging/mpu_ttl/mpu_scan_interval）之上增加：

| 参数 | 默认 | 含义 |
| --- | --- | --- |
| `queue_depth` | 256 | SQ 深度（`UringOptions::entries`） |
| `sqpoll` | false | 内核 SQ 轮询线程（常态提交零 syscall，代价是常驻内核线程） |
| `sqpoll_idle` | 100ms | poll 线程空转多久入睡（`sq_thread_idle`） |

构造抛异常（老内核、容器 seccomp 拦 `io_uring_setup`、memlock 配额不足）时工厂
**回退为 LocalFsBackend**：两者 on-disk 布局与语义完全相同，回退无损、只失去
异步 IO；除启动告警日志外，置常驻 gauge `lights3_xlocalfs_uring_fallback=1`，
让「以为在跑异步 IO 实际已回退」在监控面上持续可见。

启动成功则打印一行能力摘要（`UringFeatures::describe`）：probe 是否可用、
READ/WRITE 还是 READV/WRITEV 回退、FSYNC 有无、SQPOLL 是否真实启用、
SINGLE_MMAP/NODROP 特性位。数据面指标沿用 `lights3_localfs_*` 命名空间
（见 [./localfs.md](./localfs.md)），以 backend label 区分实例。
