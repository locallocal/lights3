# FsDataStore：DuoStore 本地文件系统数据面

本文是 `FsDataStore` 的实现级文档，展开 [../duostore-backend.md](../duostore-backend.md)
§5–§9 的数据面设计在代码中的具体落法。装配与驱动侧（GC worker、pin 表、泵送循环）见
[./duostore-core.md](./duostore-core.md)；RADOS 数据面的对照实现见
[./duostore-data-rados.md](./duostore-data-rados.md)。涉及文件：

| 文件 | 内容 |
| --- | --- |
| `src/storage/duostore/fs_data_store.h/.cc` | `IDataStore` 的本地文件系统实现（chunk 切片 + pack 聚合） |
| `src/storage/duostore/data_store.h` | 数据面 SPI（`IDataStore` / `DataWriter` / `ChunkPinHooks` 等） |
| `src/storage/duostore/data_ref.h` | `Extent` / `DataRef`：meta 与 data 的唯一耦合点 |

## 1. 在 SPI 中的位置与装配

`data_store.h:IDataStore` 全接口由 `fs_data_store.h:FsDataStore` 实现，所有阻塞
IO 都直接跑在调用线程上——契约是 DuoStoreBackend 在入口统一
`co_await pool_->schedule()` 切池线程（`fs_data_store.cc:ChunkWriter` 头注释）；
后台类方法（`remove`/`rewrite_pack`/`scan_*` 等）自带一次 `pool_->schedule()`。

构造注入四组回调（`duostore_backend.cc` 的 cfg 构造分支装配）：

| 回调 | 装配来源 | 作用 |
| --- | --- | --- |
| `FsDataStore::FileIdAlloc` | `IMetaStore::alloc_file_run` | 分配连续 file_id 号段（chunk/pack 共用，§4.2） |
| `FsDataStore::PackSeal` | `IMetaStore::seal_pack` | 封存回执：pack 轮转/close 时上报最终 file_size |
| `data_store.h:PackMigrateFn` | `duostore_backend.cc:migrate_pack_records` | 压实迁移回调（反查存活 + 批量回写 + 换 ref） |
| `data_store.h:ChunkPinHooks` | `PinTable::pin_id/unpin_id` | 写侧 pin：孤儿扫描不回收"写了一半、meta 未提交"的 chunk |

`fs_data_store.h:FsDataOptions` 关键项：`chunk_size`（默认 8MiB）、
`pack_threshold`（0 = 关 pack，backend 注入实际默认 128KiB）、`pack_max_size`
（128MiB）、`pack_writers`（4 个 active pack 槽）、`pack_max_age_sec`（0 = 只按
容量封存）、`verify_chunk_crc`（GET 链路 chunk 校验开关）、`on_corruption`
（读路径 crc 失配上报钩子——reader 持 options 副本逃逸出 store 生命周期，
回调只允许捕获计数器，backend 侧以 `[c = m_read_corruption_]` 捕获兑现）。

## 2. 磁盘布局

```text
<root>/
  chunks/<ss>/<file_id:016x>.chk    # 大对象定长切片，一次写成后不可变
  packs/<ss>/<pack_id:016x>.pak     # 小对象 append-only 聚合文件
```

- **shard 目录**：`fs_data_store.cc:shard_of` 取 `(id >> 8) & 0xff`，256 个两位
  hex 子目录。号段分配使连续 256 个 id 落同一目录——一次写会话的目录 fsync
  收敛到 1–2 次而非每 chunk 一次；
- **dirfd 常驻缓存**：`fs_data_store.cc:FsDataStore::subdir_fd` 惰性建目录并缓存
  256 个 dirfd（chunks/packs 各一组，`dir_mu_` 保护），供会话末尾
  `fsync(dirfd)`；析构统一关闭；
- 文件名恒 `%016llx` + 后缀，`chunk_path`/`pack_path` 为测试观察点；
  `scan_shard_tree` 枚举时对不符合命名的文件（临时/外来）直接忽略。

## 3. Pack 文件格式（字节布局）

pack 文件是 record 顺序追加的 append-only 文件，无文件头。record 布局
（`fs_data_store.cc:build_pack_header`，多字节整数一律小端）：

| 偏移 | 长度 | 字段 |
| --- | --- | --- |
| 0 | 4 | magic `"LP3R"`（`kPackMagic`） |
| 4 | 1 | ver = 1 |
| 5 | 1 | flags = 0 |
| 6 | 2 | header_len（= 22 + owner_len，`kPackHeaderFixed` = 22） |
| 8 | 8 | payload_len |
| 16 | 4 | crc32c(payload) |
| 20 | 2 | owner_len |
| 22 | owner_len | owner（`"bucket\0key"` 或 `"mpu\0…"`，形态见 `codec.h:parse_pack_owner`） |
| header_len | payload_len | payload |

`Extent{kind=kPack, file_id=pack_id, offset, length=payload_len, crc}` 的
`offset` 指向 **payload 起始**而非 record 起始——热路径 GET 不解析头；头里的
owner/crc 冗余专供压实顺扫（§7.3）与离线打捞。owner 或 header 超 0xffff 抛
`InternalError`（`build_pack_header` 前置校验）。

## 4. 写路径

### 4.1 路由：`open_writer`

`fs_data_store.cc:FsDataStore::open_writer` 按 `WriteHint` 分流：
`pack_threshold == 0`（关 pack）或已知长度 > 阈值 → `ChunkWriter`；其余
（含 chunked PUT 的未知长度）→ `FsPackedWriter`。驱动方是
`duostore_backend.cc:pump_body`：64KiB 缓冲循环 `body.read → md5.update →
writer->write`，EOF 后 `finish()` 拿 `DataRef`。

### 4.2 ChunkWriter：定长切片流式写

`fs_data_store.cc:ChunkWriter` 逐块落 `chunks/`：

- **id 号段几何增长**：`open_next_chunk` 用完当前 run 时按 1→2→4…（上限
  `data_ref.h:kMaxIdRun`=64）调 `alloc_`。并发写者若逐个取 id，同一对象的
  chunk id 会交错，manifest 的 run 编码失效；首块只取 1 个，小对象零浪费，
  run 尾未用的 id 直接丢弃（id 只需唯一）；
- **先 pin 后建文件**：分配 id 后立即 `pins_.pin(id)` 再 `open(O_CREAT|O_EXCL)`
  ——文件一旦存在即已受写侧 pin 保护，孤儿扫描没有观察窗口；
- `write` 循环填满 `chunk_size` 即 `seal_chunk`：`fdatasync(fd)` + close +
  push `Extent{kChunk, id, 0, len, crc}`（crc 随写增量累计
  `codec::crc32c_update`）；
- `finish`：封存尾块后，对本会话 `touched_` 位图标记的 shard 目录逐个
  `fsync(dirfd)`（目录项持久化点），返回 DataRef。**pin 的归属随 finish 转移
  给调用方**——backend 用 `duostore_backend.cc:WritePinRelease` 在 meta 提交或
  兜底删除之后释放；
- **未 finish 即析构 = 丢弃**：best-effort unlink 已产出文件 + 释放本会话全部
  pin；残留交给孤儿扫描。

### 4.3 FsPackedWriter：pack/chunk 双态缓冲

`fs_data_store.cc:FsPackedWriter` 先在内存累积；一旦累计超过
`pack_threshold`（只可能发生在未知长度流），构造内嵌 `ChunkWriter`（spill），
把缓冲整体刷下去后转为纯 chunk 流式。`finish`：已 spill 则委托 spill；缓冲为空
返回空 DataRef（0 字节对象）；否则 `append_pack_record` 整体追加进 pack。未
finish 即析构时缓冲直接丢弃，盘上无痕迹（已 spill 则由 ChunkWriter 清理）。
内存上界 = `pack_threshold × max_inflight_requests`（与主文档 §5.3 联动声明）。

### 4.4 active pack 追加：`append_pack_records`

单条 `append_pack_record` 是批量版的 n=1 退化。批量路径要点：

1. **槽位轮询取锁**：`pack_rr_` 原子游标定起点，先对 `pack_writers` 个
   `ActivePack` 槽 try_lock 扫一圈摊薄排队，全忙则阻塞在起点槽；
2. **轮转封存**：写前判 `slot->size + rec_size > pack_max_size` 或
   `slot_aged`（steady_clock 计龄，防 NTP 跳变）。封存前先把本批未 sync 的
   写 `fdatasync` 落盘——seal 回执报告的 file_size 必须对应已持久化字节。
   `size > 0` 守卫保证"单条恰好超限"的边界配置仍能独占一个 pack；
3. **新 pack 创建**：`alloc_(kPack, 1)` 取 id → `O_CREAT|O_EXCL` 建文件 →
   对 fd 取 `flock(LOCK_EX|LOCK_NB)` 咨询锁（"活进程正在写此 pack"的唯一可靠
   信号，fd 关闭/进程退出自动释放，供启动补封存与孤儿扫描探测）→ 立即
   `fsync(pack dirfd)`（pack 创建是轮转粒度的低频事件，不像 chunk 攒到会话尾）；
4. **追加**：header+payload 拼成一条后单次 `pwrite`（循环补短写）；批内逐条
   pwrite、`fdatasync` 收敛到批尾一次。崩溃只可能丢"尚未返回给调用方"的
   record——ref 换入/meta 提交都在本函数返回之后，等价于 torn tail；
5. **封存两段式**：`close_slot_locked` 在槽锁内只做 close(fd) + 清槽 + 把
   `(id,size)` 推入 `seal_retry_` 队列；`flush_seals` 在**槽锁外**提交
   `seal_`（可能是网络 RTT/fsync）。追加路径 `flush_seals(false)` 失败仅告警、
   留队列由后续写/close 重试；close 路径 `flush_seals(true)` 失败上抛。
   历史教训（头注释）：seal 在锁内抛出会留下 fd=-1 但 size 未清的坏槽。

### 4.5 `write_batch`：压实迁移的批量落地

`fs_data_store.cc:FsDataStore::write_batch` 覆盖接口默认实现：合 pack 条件的
（非空且 ≤ 阈值）收集后一次 `append_pack_records`（单槽锁 + 单 fdatasync）；
超限/关 pack 的逐条回落 `open_writer` 路径（阈值缩小后旧 record 可能超限，
路由必须保留）。仅由 `duostore_backend.cc:migrate_pack_records` 调用。

## 5. fsync 点与崩溃窗口

| 持久化点 | 位置 | 时机 |
| --- | --- | --- |
| chunk 内容 | `ChunkWriter::seal_chunk` 的 `fdatasync(fd)` | 每 chunk 写满/尾块 |
| chunk 目录项 | `ChunkWriter::finish` 的 `fsync(shard_dirfd)` | 会话末尾，仅 touched shard |
| pack record | `append_pack_records` 的 `sync_slot`（`fdatasync`） | 每批一次；轮转封存前强制 |
| pack 目录项 | 新 pack 创建后立即 `fsync(pack_dirfd)` | 每次轮转（低频） |
| pack 封存账 | `seal_`（meta 提交，可能 WAL fsync） | 槽锁外异步，失败进 `seal_retry_` |

崩溃窗口矩阵（数据先落、meta 后提交的总不变量见主文档 §6）：

| 崩溃点 | 盘上残留 | 收敛路径 |
| --- | --- | --- |
| chunk 已落、meta 未提交 | 无 refs 的 .chk 文件 | 孤儿扫描（写侧 pin 已随进程消失，mtime 逾 grace 后 unlink） |
| pack record 已追加、未返回调用方 | 尾部完整但无账 record | 天然死区，压实回收 |
| pwrite 中途断电 | torn tail（半条 record） | 重启弃用 active pack（新号段 + O_EXCL 开新文件），顺扫遇 payload 越界即静默停止 |
| pack 文件已建、首条 record 未提交 | 有文件无 packstat 账 | `scan_packs` 反向对账（mtime 逾 grace + 无 pin + 无写锁 → 删） |
| seal 回执未达（崩溃/失败） | packstat 停留 unsealed | 启动期 `duostore_backend.cc:DuoStoreBackend::abandon_stale_packs` 补 `seal_pack(id, 0)`；file_size=0 由 GC 轮 `stat_pack` 回填分母 |

析构未经 `close()` 等价崩溃：`FsDataStore::~FsDataStore` 只关 fd 不 seal
（meta 可能已先关闭），留待下次启动补封存。补封存前先
`pack_write_locked` 探测咨询锁——滚动重启/误配共享 root 时不能把**别的活进程
正在写**的 pack 封掉（封了会立刻进压实候选、账清零后甚至被整删）。

## 6. 读路径：ExtentChainReader

`fs_data_store.cc:ExtentChainReader` 是 `http::BodyReader`，由
`FsDataStore::open_reader` 构造（入参 [first,last] 闭区间已经 `resolve_range`，
越界抛 500）。**自持化**：持 `FsDataOptions` 副本而非 store 指针——reader 随
HTTP 响应逃逸出 backend 生命周期（驱动器在 handler 返回后继续泵送）。

- 构造时把 first 折算到起始 extent 下标 + 段内偏移；每次 `read` 先
  `co_await pool_->schedule()`（open/pread 都是阻塞 IO）；
- **chunk extent**：懒 open 当前段 fd，`pread` 流式吐出，段读尽关 fd 推进。
  crc 仅当 `verify_chunk_crc` 开且"从段首完整读到段尾"（`cur_off_==0 &&
  remaining_ >= e.length`）才累计校验——Range 命中段中部无从校验；
- **pack extent**：`read_pack` 首次触达把整段 payload（≤ pack_threshold）一次
  读入内存、**恒校验 crc32c**（与开关无关），再按 range 从内存切片；
- **失配处置**：chunk/pack crc 失配都是 LOG_ERROR + 调 `opt_.on_corruption`
  （backend 侧落 `lights3_duostore_read_corruption_total` 计数器）+ 抛 500；
- open ENOENT 而 refs 在 = 数据丢失征兆，LOG_ERROR + 抛 500——正常并发窗口
  由 pin+grace 挡掉（GET 侧 `duostore_backend.cc:PinnedReader` 先 pin 后开
  reader，析构解 pin）；pread 提前读到 EOF 抛 "extent shorter than manifest"。

## 7. 删除与 GC 支撑面

### 7.1 `remove` / `remove_pack` / `stat_pack` / `pack_write_locked`

- `FsDataStore::remove`：只 unlink kChunk（ENOENT 幂等忽略）；kPack 直接跳过
  ——pack record 变死区由压实回收；异种 kind（如 kRados）= data/meta 引擎错配，
  全局告警一次后跳过（静默跳过会让 GC 永久空转无从察觉）。每 1024 个 extent
  `co_await pool_->schedule()` 让出池线程（TB 级对象数十万 extent）；
- `remove_pack`：整文件 unlink，幂等。GC 只对 sealed 且 live_recs==0 且逾
  grace 且无 pin 的 pack 调用（`duostore_backend.cc` GC 第 4 步）；
- `stat_pack`：一次 stat 返回真实文件大小，供崩溃遗留 seal(0) 的账在 GC 决策前
  回填分母（避免无条件进全量顺扫重写）；失败返回 0（= 未知）；
- `pack_write_locked`：只读 open + `flock(LOCK_EX|LOCK_NB)` 探测：EWOULDBLOCK
  ⇒ 别的活进程持锁；拿到则立即解锁，不改任何状态。**false 是保守方向**——
  误报 true 只推迟补封存，误报 false 会封掉别人正在写的 pack。

### 7.2 `seal_aged_packs`：低写量下的年龄轮转

写路径只在"有下一条要写"时检查年龄；写停了 active pack 会永远挂着，其被
覆盖/删除的 record 成为压实候选集之外的死区。GC 每轮调
`FsDataStore::seal_aged_packs` 补位：对每个槽 try_lock（拿不到 = 正在写、
下一条自会轮转，GC 不与业务争锁），fd 有效、size>0 且超龄则
`close_slot_locked`。持有槽锁 ⇒ 无在途追加 ⇒ 上一批批尾的 fdatasync 已把
槽内字节落盘，seal 报的 file_size 与磁盘一致。

### 7.3 `rewrite_pack`：压实顺扫与损坏语义

`fs_data_store.cc:FsDataStore::rewrite_pack` 逐条解析 record，本函数**从不
修改/删除被扫的 pack**——删除恒走"存活账清零 + 整空 pack 延迟删"路径，任何
误判（损坏、owner 解析不了）至多让 pack 多活一阵，绝不丢数据。三类异常分治：

| 异常 | 判定 | 处置 | 计数 |
| --- | --- | --- | --- |
| magic/版本/header_len 不符 | 无法重新同步流 | 告警 + **中止整个扫描**（后续字节不可信），pack 留待人工 | `GcRewrite::corrupt` |
| payload crc 失配 | header 可信（长度已知） | 告警 + 跳过该条继续 | `corrupt` |
| payload 越过 EOF | torn tail（重启弃用 active pack 的预期残留） | 静默停止 | 不计损坏 |

存活迁移按批交付：`kMigrateBatchRecs`=64 条或 `kMigrateBatchBytes`=4MiB 攒一批
调 `migrate_`（gaps §2.13：逐条交付每条一次 fdatasync + 一次 meta 提交，
128MiB pack ≈ 千次）；批间 `pool_->schedule()` 让出池线程。回调标准实现
`duostore_backend.cc:migrate_pack_records`：按 owner 聚组 → `get_object` 反查
存活（extent 逐位配对）→ `write_batch` 批量回写 → `swap_extents_batch` 乐观
换 ref（version 失配 = 期间被覆盖/删除，放弃该组并清理新写的 chunk 残留）。
`GcRewrite::file_size` 供无 `stat_pack` 支持的引擎回填账面分母。

### 7.4 `scan_chunks` / `scan_packs`：孤儿对账枚举

两者共享 `fs_data_store.cc:FsDataStore::scan_shard_tree`（chunks/ 与 packs/
布局同构）：遍历 shard 目录 → 校验 `<016x><suffix>` 命名（异名文件忽略）→
`stat` 取 mtime/size 回调（stat 失败 = 并发 unlink 竞态，容忍跳过）。**数据面
只枚举，不做存活判定**——refs 反查/grace/pin 全在调用方
`duostore_backend.cc:DuoStoreBackend::run_orphan_scan_once`；size 顺带喂用量
指标（枚举本来就要 stat，零额外系统调用）。

## 8. 并发模型小结

| 共享状态 | 保护 | 说明 |
| --- | --- | --- |
| `ActivePack` 槽（fd/size/opened） | 每槽 `std::mutex` | 临界区 = 一次小 pwrite（+批尾 fdatasync）；`pack_rr_` 原子游标轮询分散 |
| dirfd 缓存 | `dir_mu_` | 惰性建目录 + open，之后只读 |
| `seal_retry_` | `seal_mu_` | 封存回执与追加/close/GC 多方并发入队出队 |
| ChunkWriter / FsPackedWriter | 无锁 | 单请求私有，串行协程使用 |
| ExtentChainReader | 无锁 | 自持 options 副本，与 store 生命周期解耦 |

`close()`：逐槽加锁封存 active pack → `flush_seals(true)`（失败上抛，关闭路径
必须可见）→ 若挂了 uring 引擎则 `shutdown()` 其收割线程（§9）。backend 的
close 顺序保证 data 先于 meta 关闭，seal 回调此时仍可用。

## 9. io_uring 变体（roadmap §3.4 ⑤）

`fs_uring: true`（键表见 [../duostore-backend.md](../duostore-backend.md) §11）
时，DuoStoreBackend 构造一个与 xlocalfs 同源的
`storage/xlocalfs/uring.h:UringEngine` 注入 `FsDataOptions::uring`；引擎建失败
（老内核 / seccomp / memlock 配额）LOG_WARN 后回退同步路径，并置常驻 gauge
`lights3_duostore_uring_fallback=1`。**磁盘布局与两种模式完全一致**，可随时
开关切换。替换面：

| 路径 | 同步模式 | uring 模式 |
| --- | --- | --- |
| chunk 写（`ChunkWriter`） | 池线程 `write` 循环 + 封存时阻塞 fdatasync | 每 chunk 一条 `uring_stream.h:UringWriteStream` 写流水线（拷入流块，至多 `write_depth` 在途）；封存 = 末块 WRITE→FSYNC 链一次提交 |
| chunk 读（`ExtentChainReader`） | 池线程 pread 逐块 | 每 extent 一条 `UringReadStream`（read-ahead），crc 校验条件不变 |
| pack 记录读 | pread 循环整读 payload | `UringReadStream` 整读，crc 恒校验不变 |
| pack 追加（`append_pack_records`） | 槽锁内 pwrite + 批尾 fdatasync | pwrite 仍在槽锁内同步（临界区小；`std::mutex` 不能跨挂起点），**批尾 fdatasync 经 `dup(fd)` 出锁后以 FSYNC SQE 挂起等待**——追加路径最长的阻塞点离开池线程。正确性：封存/轮转仍在锁内先做阻塞 sync 再 close，seal 上报的 file_size 恒对应已持久化字节；本批自身的持久化在返回 extents 前完成 |
| 轮转中途 sync / dirfd fsync | 阻塞 | 不变（锁内 / 低频路径） |

读者（`ExtentChainReader`）持 options 副本逃逸 store 生命周期的老约束照旧成立：
`FsDataOptions::uring` 是 shared_ptr，读者共同持有引擎对象；`close()` 停掉
收割线程后，逃逸读者的下一次提交以 InternalError 收场（与 xlocalfs 的关闭
时序假设一致）。多在途 op 的析构安全性由 uring_stream 的引用计数共享块承担
（见 [./xlocalfs.md](./xlocalfs.md) §5）。

