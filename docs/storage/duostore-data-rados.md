# RadosDataStore：DuoStore RADOS 数据面

本文是 `RadosDataStore` 的实现级文档，展开
[../duostore-rados-data.md](../duostore-rados-data.md)（设计与选型调研，本文引其
章节记作"设计文档 §N"）在代码中的具体落法。装配与驱动侧见
[./duostore-core.md](./duostore-core.md)；本地 fs 数据面对照见
[./duostore-data-fs.md](./duostore-data-fs.md)。涉及文件：

| 文件 | 内容 |
| --- | --- |
| `src/storage/duostore/rados_data_store.h/.cc` | `IDataStore` 的 Ceph/RADOS 实现（librados C API，编译门 `LIGHTS3_DUOSTORE_RADOS_DATA`） |
| `src/storage/duostore/data_store.h` | 数据面 SPI |

## 1. SPI 覆盖矩阵

单一路径（切片缓冲 → 每片一个 rados 对象），无 pack 实体，接口覆盖如下：

| 接口 | 实现 |
| --- | --- |
| `open_writer` | `RadosChunkWriter`：切片缓冲 + aio write_full 双缓冲流水（§4） |
| `open_reader` | `RadosExtentReader`：逐对象按名 aio 读（§5） |
| `remove` | 窗口化并发 aio remove（§6） |
| `write_batch` | 未覆盖——接口默认逐条 open_writer 语义即正确（无 pack 批量追加可省） |
| `remove_pack` / `rewrite_pack` | 显式 no-op / 恒 `{}`：meta 永无 kRados 的 pack 账，压实候选恒空，实际不会被调用 |
| `seal_aged_packs` / `scan_packs` / `pack_write_locked` / `stat_pack` | 接口默认（0/空扫/false/0）——无 pack 实体即无老化/泄漏面 |
| `scan_chunks` | namespace 内全量列举 + stat（§7） |
| `close` | `rados_aio_flush` 排干在途写 + 置 closed（§8） |

无 pack 是**设计边界而非欠账**（`rados_data_store.cc` remove_pack 前的长注释）：
pack 解决的是本地 fs 的 inode/目录开销，RADOS 侧小对象代价由 BlueStore
`min_alloc_size` 与 pool 副本策略承担；网关再叠 pack 层只会引入跨对象压实与
读改写放大（rados 对象也无法打洞）。

`rados_data_store.h:RadosDataOptions` 关键项：`pool`（必填，副本/EC 策略在
pool 级）、`ns`（namespace 逻辑隔离）、`chunk_size`（默认 8MiB = 切片粒度 =
单对象上限）、`buffer_total`（默认 256MiB，writer 缓冲总额度）、
`op_timeout_sec`（0 = 不设）、`verify_chunk_crc`/`on_corruption`（语义同 fs
版）、`metrics`（op 延迟/错误指标）、`pins`（写侧 pin 钩子，backend 从同一
`PinTable` 注入）。

## 2. 对象模型

- **chunk → rados 对象**：对象名 `c.<file_id:016x>`
  （`rados_data_store.cc:RadosDataStore::object_name`）；`Extent{kind=kRados,
  file_id, offset=0, length, crc32c}`——对象即切片，无段内偏移。file_id 经
  `RadosDataStore::FileIdAlloc`（= `IMetaStore::alloc_file_run`）分配，与
  kChunk **共号段**（同一 meta 上切换 data 引擎不产生 id 碰撞）；
- **id 号段几何增长**：与 fs 版 ChunkWriter 同策略，1→2→4…上限
  `data_ref.h:kMaxIdRun`，保住 manifest run 编码；
- **owner xattr**：`kOwnerXattr` = `lights3.owner`，`WriteHint.owner` 原样存进
  每个切片对象的 xattr——meta 全失时离线打捞凭 getxattr 反查归属。与数据同一
  write_op 提交（§4），不存在"有对象无归属"的中间态。

## 3. 连接生命周期：`Conn`

`rados_data_store.h:RadosDataStore::Conn` 用 `shared_ptr` 引用计数共享
`rados_t cluster` + `rados_ioctx_t ioctx`：reader 随 HTTP 响应逃逸出 backend
生命周期，各自持一份 Conn 引用，最后一个持有者析构时 `Conn::shutdown`
（幂等：`rados_ioctx_destroy` + `rados_shutdown`）。

构造序列（`rados_data_store.cc:RadosDataStore::RadosDataStore`，失败即构造
失败——配置/环境错误 fail fast，抛 `std::runtime_error` 而非 S3Error）：
`rados_create2` → `rados_conf_read_file` → `rados_conf_set
client_mount_timeout`（+ 可选 `rados_osd_op_timeout`）→ `rados_connect` →
`rados_ioctx_create` → `rados_ioctx_set_namespace`。此后 ioctx 属性恒不变——
单 ioctx 进程级并发共享的线程安全前提。

**close 守卫**：`rados_data_store.cc:io(conn)` 是所有 op 的取 ioctx 入口，
`closed` 置位后任何调用干净地抛 `InternalError`(500) 而非崩溃（仿 rocks 版
`db()` 守卫）。

## 4. aio → 协程桥：`AioPending` / `AioAwait`

全部 IO 走 `rados_aio_*`，completion 与协程的会合点是
`rados_data_store.cc:AioPending`（堆分配的在途单）：

- **状态机**：`state` 原子取值 `0 →（等待者 handle 地址）→ kDone`。
  `AioAwait::await_suspend` 用 CAS 把协程 handle 写入（若回调已先到 CAS 失败
  → 不挂起就地续行）；`AioPending::on_complete` 用 exchange 置 kDone，读到
  非零旧值即把停车的 handle `ex->post(...)` 投回本进程池 executor。**纪律**：
  回调运行在 librados finisher 线程，只记结果 + 投递，绝不在 ceph 线程续跑
  业务逻辑（阻塞它会反压 librados 内部管线）；
- **双引用弃单安全**：`refs=2`（发起方/回调各一），`unref` 归零才
  `rados_aio_release` + delete。写侧流水允许"提交后不等待"，writer 未 finish
  即析构时，在途单携带的缓冲（`data`）与信号量额度（`permit`）都活到回调落地
  ——librados 正在读的内存恒有效，额度记账与实际驻留内存恒一致；
- **提交失败**（`rados_aio_*` 返回负值）：回调永不会来，调用方连续两次
  `unref` 回收（`start_flush`/`read`/`remove` 三处一致）；
- **延迟指标**：`make_pending` 记 `t0`，回调里 observe 到
  `lights3_duostore_rados_op_duration_seconds`（提交 → 回调，含集群往返）。

`await_resume` 只返回 rados 返回值，语义处置（-ENOENT 等）归调用方。

## 5. 写路径：`RadosChunkWriter` 双缓冲流水

### 5.1 缓冲与背压

首次 `write` 时 `co_await buffer_sem_.acquire()` 取第一份额度
（`AsyncSemaphore`，许可数 = `buffer_total / chunk_size`）；耗尽即挂起，背压
沿协程链传导回 socket 读循环。`write` 循环填满 `chunk_size` 触发
`flush_chunk`。

### 5.2 流水线与 try_acquire 二份额度

`rados_data_store.cc:RadosChunkWriter::flush_chunk` 维持**至多 1 单在途**
（extent 顺序天然保序）：先收割上一单（`harvest_pending`）→ 提交本片
（`start_flush`，不等待）→ 为下一片备缓冲，三选一：

1. 有 `spare_permit_`（上次收割回收的缓冲+额度）→ 直接复用；
2. 否则 `buffer_sem_.try_acquire()` **非阻塞**取第二份额度——**决不排队等**：
   每个 writer 已各持一份额度时嵌套阻塞等待会互等死锁；
3. 拿不到 → 就地 `harvest_pending` 等本片落地回收缓冲——退化为单缓冲串行
  （C1 行为），背压语义不变，只失去接收/写入重叠。

### 5.3 `start_flush`：提交一片

1. `co_await pool_->schedule()`：crc 计算（CPU）不占 HTTP 驱动线程；
2. 号段分配 + `pins.pin_one(file_id)`——**必须在 `make_pending` 之前**：
   alloc/pin 可抛，此刻尚无 completion 需要回收。pin 挡住孤儿扫描删掉"对象已
   落、整个 PUT 的 meta 还没提交"的切片（早期切片的 Δ 可远超 gc_grace）；
3. 缓冲与额度的所有权转移进在途单（`p->data = std::move(buf_)` 等）；
4. 提交：owner 非空时组 `rados_create_write_op`（`write_full` + `setxattr`
   同一 op，单对象 op 原子——数据与归属要么同在要么同无）走
   `rados_aio_write_op_operate`；否则裸 `rados_aio_write_full`。提交失败双
   `unref` + 置 `failed_` + 计错 + `throw_rados`。

write_full 的回执语义（设计文档 §4.3）：全部副本/EC 条带已持久化，等价且强于
fs 版"fdatasync + 目录 fsync"；单对象 op 原子 ⇒ **无 torn chunk**。

### 5.4 收割、finish 与丢弃

- `harvest_pending`：`co_await AioAwait{p}` 等落地 → 回收缓冲为 `spare_`、
  额度为 `spare_permit_` → 成功 push `Extent{kRados, id, 0, len, crc}`；失败
  置 `failed_`、计错、`throw_rados`——**不重试不复用**（`require_usable` 在
  finish/failed 后拒绝任何调用；-ETIMEDOUT 结果不明只产生孤儿、不产生错数据）；
- `finish`：收割在途单 → 尾片（未满 chunk_size 的退化情形，小对象即"缓冲到
  EOF 一片写完"）同步 start_flush + harvest → 清空 `pinned_`（**pin 归属随
  DataRef 转移给调用方**，backend 用 `duostore_backend.cc:WritePinRelease` 在
  meta 提交/兜底删除后释放）→ 返回 DataRef；
- **未 finish 即析构 = 丢弃**：只 `pending_->unref()`（不等在途单，缓冲/额度
  随回调落地归还）+ 就地释放本 writer 的 pin；已写出对象成无主对象，由
  `duostore_backend.cc:commit_or_discard` 的 remove 兜底或孤儿扫描回收。
  析构函数内**无网络 IO**。

## 6. 读路径：`RadosExtentReader` 与 `remove`

`rados_data_store.cc:RadosExtentReader` 结构对照 fs 版 ExtentChainReader，
**自持化**：持 Conn 的 shared_ptr + 自己的 `ThreadPoolExecutor`（reader 逃逸
出 store 生命周期后 aio 恢复仍有投递目标）。每次 `read`：

- 逐段按名 `rados_aio_read`，**读进在途单自有的缓冲**再 memcpy 给调用方——
  若直写调用方 buf，读超时/取消销毁协程帧连带 buf 后，远端 completion 仍会
  写穿已释放内存；
- `co_await AioAwait{p}` 停车，completion 经 executor 恢复到池线程；
- 返回值分治：`-ENOENT` = refs 在而对象缺（数据丢失征兆或 pin/grace 失效）
  → LOG_ERROR + 500；其余负值 `throw_rados`；0 = 比 manifest 短 → 500；
- crc 语义同 fs 版：仅"从段首完整读到段尾"才校验，失配 →
  `on_corruption` + 500。

**pin 是唯一防线**：rados 没有 POSIX"已打开 fd 不受 unlink 影响"的兜底，
GET 与 GC 的竞态完全靠 `PinnedReader` 的 pin + gc_grace（设计文档 §8.1）。

`RadosDataStore::remove`：窗口化并发（`kWindow`=16 单在途）aio remove——TiB
级 manifest 的 GC 不无界压集群；`-ENOENT` 幂等忽略；其余负值**先收齐整批再抛
首个**（collect before throw，不留悬空在途单）；非 kRados 的 extent 跳过
（数据引擎切换遗留，不归本店）。

## 7. `scan_chunks`：孤儿枚举

`rados_data_store.cc:RadosDataStore::scan_chunks`：
`rados_nobjects_list_open/next`（ioctx 已限 namespace，多实例互不可见）逐名
遍历 → `parse_object_name` 严格匹配 `c.<016x>`（外来命名忽略，对齐 fs 版对非
`.chk` 文件的处置）→ `rados_stat` 取 size/mtime（秒级足够；stat 失败 = 并发
remove 竞态，容忍跳过）→ 回调。列举/统计无 aio 版，低频（默认 1/d）驻池线程
同步阻塞可接受。列举以 `-ENOENT` 正常结束，其余负值计错上抛。孤儿判定
（refs 反查/grace/pin）在调用方
`duostore_backend.cc:DuoStoreBackend::run_orphan_scan_once`——注意该处删除的
extent kind 随 `data_kind` 取 kRados（硬编码 kChunk 会被本店 `remove` 跳过、
孤儿删除静默空转）。

## 8. flush / close / 析构

- `RadosDataStore::close()`：切池线程后 `rados_aio_flush`——**连 completion
  回调一起等完**，弃单 writer 的缓冲/额度归还随之落地，此后 ceph 线程不再触碰
  `exec_`/`buffer_sem_`；再置 `closed`（新 op 干净抛 500）、`conn_.reset()`。
  ioctx/cluster 的真正销毁由最后一个 Conn 持有者执行（在途读被逃逸 reader
  自身的 Conn 引用兜住）；
- 析构兜底（未走 close）：同样先 `rados_aio_flush` 再放 Conn 引用——否则
  成员先于回调销毁；
- backend 侧顺序不变：先停 GC → `data_->close()` → `meta_->close()`。

## 9. 错误映射、崩溃与重试边界

统一出口 `rados_data_store.cc:throw_rados`（LOG_ERROR 含 errno 名 + 抛
`InternalError`）。要点：

| 情形 | 处置 |
| --- | --- |
| 构造期失败（connect/ioctx） | `std::runtime_error`，启动失败（fail fast） |
| write_full/read/remove 负值 | 500 + `lights3_duostore_rados_op_errors_total` 计数（remove 的幂等 -ENOENT 不计） |
| -ETIMEDOUT（配置了 op 超时） | op 结果不明：writer 进 `failed_` 态，不重试不复用；孤儿由扫描收敛 |
| 应用层重试 | **不做**：write_full 幂等且 librados 对暂时不可达的 OSD/PG 内部排队重试（默认不设 op 超时，挂起优于误报） |

崩溃窗口（对照 fs 版大幅收窄）：write_full 单对象原子 ⇒ 无半截对象；网关
崩溃只留无主对象（refs 无记录）→ 孤儿扫描回收；无 pack ⇒ 无 torn tail、无
补封存逻辑、无 packs/ 泄漏面。数据先落、meta 后提交的总不变量
（[../duostore-backend.md](../duostore-backend.md) §6）逐条成立。

