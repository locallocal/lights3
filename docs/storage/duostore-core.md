# DuoStore 引擎核心实现

> 本文是 [../duostore-backend.md](../duostore-backend.md)（设计文档）的实现级对照，
> 覆盖 DuoStore 的**引擎核心**：编排层 `DuoStoreBackend`、元数据编解码 `codec`、
> 运维 dump/load 路径 `meta_dump`，以及两个 SPI 接口 `IMetaStore` / `IDataStore`
> 的方法级契约。各插件实现的细节见同目录文档：
> [./duostore-meta-rocksdb.md](./duostore-meta-rocksdb.md)、
> [./duostore-meta-redis.md](./duostore-meta-redis.md)、
> [./duostore-meta-sqlite.md](./duostore-meta-sqlite.md)、
> [./duostore-meta-tikv.md](./duostore-meta-tikv.md)、
> [./duostore-data-fs.md](./duostore-data-fs.md)、
> [./duostore-data-rados.md](./duostore-data-rados.md)。代码位置
> `src/storage/duostore/`，符号引用格式为 `文件:符号`。

## 1. 架构：元数据/数据分离的落地形态

`duostore_backend.h:DuoStoreBackend` 实现 `backend.h:IStorageBackend` 全接口，
内部把职责拆到两个可插拔接口上，唯一耦合点是 `data_ref.h:DataRef`：

```text
DuoStoreBackend（S3 语义、ETag/MD5、泵送循环、pin 表、GC/孤儿扫描编排）
    │                                │
IMetaStore（同步，池线程调用）    IDataStore（协程 Task<T>）
rocksdb / redis / sqlite / tikv    fs（chunk+pack）/ rados
    └────────── DataRef（值语义定位信息）──────────┘
```

两侧刻意不对称（设计论证见主文档 §2.2）：

- **IMetaStore 同步**：候选客户端库全是阻塞式；`DuoStoreBackend` 在每个入口统一
  `co_await pool_->schedule()` 切到池线程后才调用 meta，复合元数据操作单跳完成。
- **IDataStore 协程**：数据面要与 `http::BodyReader` 的协程读循环交织流式写；
  各实现自行决定内部是否再切池线程。

`data_ref.h:Extent` 是定位信息的最小单元：`kind`（kChunk=0 / kPack=1 / kRados=2）、
`file_id`（全局单调分配）、`offset`（仅 pack 有意义，指向 payload 而非 record 头）、
`length`、`crc32c`。`DataRef.extents` 为空即 0 字节对象。

引擎选择（`duostore_backend.h:DuoMetaKind` / `DuoDataKind`）在
`duostore_backend.cc:DuoStoreConfig::from_params` 中集中解析：未编译进的引擎抛
"not compiled in"；不属于所选引擎的配置键按 key→ownership 表逐条 WARN。
构造函数有两个：cfg 构造自装配引擎并注入回调（seal 回调、压实迁移回调、
写侧 pin 钩子）；测试注入构造接受自装配的 meta/data（此时
`write_pins_ = false`，不做写侧 pin 的盲释放）。

## 2. codec：元数据记录的字节布局

`codec.h` / `codec.cc` 定义所有持久化编码。通则：

- **key 编码**：`'\0'` 分隔。共享校验层已拒绝含 NUL 的 key，
  `codec.cc:require_no_nul` 再做纵深防御——违规抛 InternalError，绝不静默产生
  跨记录 key 碰撞。
- **value 编码**：手写小端二进制，首字节为版本号。`codec.cc:read_ver` 做
  版本容忍读（v ∈ [1, max] 接受，读旧写新，升级不需要停机重写）；
  `codec.cc:check_ver` 用于尚无多版本的记录（严格相等）。
- 损坏值统一经 `codec.cc:corrupt` 抛 `S3Error(InternalError)`；编码侧超限
  （如 user-meta 超 64KiB）经 `too_large` 抛 `InvalidArgument`（400）——请求
  问题不是库损坏。
- 解码用 `codec.cc:Cursor` 游标：`need()` 越界检查、`done()` 拒绝尾随字节；
  run 数量、crc 数组长度都先对剩余 payload 做合理性检查，防止损坏的长度字段
  在 `need()` 抛出前膨胀出几十万个假 extent。

### 2.1 key 编码

| 函数 | 布局 | 说明 |
| --- | --- | --- |
| `codec:object_key` | `<bucket>\0<key>` | 字节序 = S3 字典序，直接支撑 list |
| `codec:upload_key` | `<bucket>\0<key>\0<upload_id>` | |
| `codec:parts_prefix` | `upload_key + '\0'` | 前缀扫描 |
| `codec:part_key` | `parts_prefix + be16(part_no)` | big-endian 保证 part_no 升序，`part_no_of_key` 反解尾 2 字节 |
| `codec:be64_key` | 8 字节 big-endian | refs / gcq 的键（`parse_be64` 反解） |

`codec.h:bump_last_byte` 是 delimiter 分组跳跃的后继 seek 点：末个非 0xff 字节
+1 并截断，各 meta 实现共享。

### 2.2 extent run 编码（`codec.cc:append_extent_runs` / `read_extent_runs`）

```text
u32 n_runs | run*
run = { u8 kind, u64 first_file_id, u32 count,
        u64 chunk_len, u64 last_len, u64 pack_offset, u32 crc[count] }
```

合并条件：同 kind（chunk/rados 形状同构）、file_id 连续、前一段满长
（run 内除末段外各段等长）；**pack extent 永不合并**（count 恒 1，解码强校验）。
单次 PUT 的 65 万 chunk 压成 1 个 run（crc 数组 4B/chunk 保留）。
`codec.cc:skip_extent_runs` 按 33B 固定 run 头 + 4B×count 算术跳过整段，
不物化 Extent 数组——这是 `decode_object_meta` 的基础。

### 2.3 value 布局

| 记录 | 版本 | 布局（str = u16 len + bytes） |
| --- | --- | --- |
| bucket（`encode_bucket`） | v1 | `u8 ver \| u64 created_ms` |
| object（`encode_object`） | 写 v2 读 v1/v2 | `u8 ver \| u64 size \| u64 mtime_ms \| u64 version \| str etag \| str content_type \| u16 n_meta (str k, str v)* \| [v2] u16 n_std (str k, str v)* \| runs` |
| upload（`encode_upload`） | 写 v2 读 v1/v2 | `u8 ver \| u64 initiated_ms \| str content_type \| u16 n_meta kv* \| [v2] u16 n_std kv*` |
| part（`encode_part`） | v1 | `u8 ver \| u64 size \| str md5 \| u64 modified_ms \| runs` |
| gcq（`encode_reclaim`） | v1 | `u8 ver \| u8 reason \| u64 enqueue_ms \| runs` |
| stats 计数器（`encode_counter_delta`） | — | 8B 小端 i64（merge 增量与全值同格式） |

要点：

- object/part 的 key、upload_id、part_no **不进 value**（已在 CF key 里）；
  解码函数由调用方回填。
- v2 的一等元数据段（Cache-Control 等五个标准头，`backend.h:kStdMetaFields`）
  是自描述 kv 而非固定槽位：下次加字段不需要 bump 版本，未知 key 读时丢弃
  （`codec.cc:put_std_meta` / `read_std_meta`）。
- `codec:decode_object_meta` 只解码定长头 + 跳过 runs，供 HEAD/list 使用
  （65 万 extent ≈ 26MiB 的 manifest 不再白白物化）；`decode_object` 才完整
  物化 `ObjectRec`。
- gcq 的 `reason` 字节是 P4 前就存在的保留位（旧值恒 0），解码时未知值回落
  `ReclaimReason::kUnknown`，新旧条目同队列共存无需版本升级。

### 2.4 pack owner 规范解析与记账口径

`codec:parse_pack_owner` 是 pack record 内嵌 owner 的唯一解析入口（三种历史
形态收敛于此，离线取证工具复用）：

- 对象：`"b\0k"` → `kObject`；
- 分片（P4 起）：`"mpu\0b\0k\0id\0no"` → `kPart`（b/k 可反查归属对象）；
- 遗留分片（P4 前）：`"mpu\0id\0no"` → `kLegacyPart`（无 b/k，归属性丢失，
  是压实"保守不迁移"的判据）；其余 `kUnknown`。

`codec.h:pack_rec_overhead` / `pack_rec_overhead_part`：pack 存活账
（live_bytes）必须**含 record 头**（22B 固定头 + owner），与 file_size 同口径
——只记 payload 会让全小对象的 pack 即使 100% 存活也永远低于 `pack_gc_ratio`，
压实陷入永久重写循环。complete 把选中分片的记账从 part 口径转到 object 口径
（盘上 record 仍是 mpu 形态，头长差是轻微低估的保守方向）。

## 3. IMetaStore 契约（`meta_store.h:IMetaStore`）

总契约：同步接口、必须在池线程调用、错误抛 `s3::S3Error`；**提交类方法内部
单事务完成"写新 + 旧 DataRef 入 GC 账 + 引用/统计更新"**。记录类型：
`ObjectRec`（meta + DataRef + `version`，每次写 +1，压实换 ref 的乐观校验）、
`UploadRec`、`PartRec`、`Reclaim`（extents + enqueue_ms + reason）、`PackStat`。

### 3.1 提交事务的同批不变量

| 方法 | 同一事务内完成 | 错误语义 |
| --- | --- | --- |
| `create_bucket` | 写 bucket | 已存在 → BucketAlreadyOwnedByYou |
| `delete_bucket` | 空检查（objects **与** uploads）+ 删 | 缺 → NoSuchBucket；非空/有进行中 mpu → BucketNotEmpty |
| `put_object` | 条件检查（`meta_util.h:check_put_condition`，在原子区内）+ 写 objects + 新 refs + 旧 DataRef 入 gcq(kOverwrite) + 删旧 refs + pack 账增减 | 条件违背抛 PreconditionFailed/NoSuchKey 且不提交 |
| `delete_object` | 删 objects + 旧 DataRef 入 gcq(kDelete) + 删 refs + pack 账扣减 | 缺返回 false（幂等） |
| `put_part` | 写 parts + 新 refs + 同号旧分片入 gcq(kPartOverwrite) | upload 缺 → NoSuchUpload |
| `complete_upload` | 装配对象（`meta_util.h:assemble_completed_object`）+ 删 uploads/parts + 未选中分片入 gcq(kComplete) + 旧同名对象入 gcq(kOverwrite) + refs 转移 + pack 账 part→object 口径转移 | ETag 不符 → InvalidPart；返回合成 ETag |
| `abort_upload` | 删 uploads/parts + 全部分片入 gcq(kAbort) | 缺 → NoSuchUpload |

`head_object` 是纯 meta 读（走 `decode_object_meta`，不物化 manifest）；
`get_object` 返回 `optional`（由 backend 区分 NoSuchBucket/NoSuchKey）；
`list_objects` / `list_parts` / `list_uploads` 为纯读。`list_uploads` 的
(key_marker, id_marker, limit) 是**下推提示**，引擎可忽略（调用方总会再跑
`apply_uploads_page`）；delimiter 非空时调用方传 limit=0（分组需全貌）。

### 3.2 资源分配、GC 记账与守卫式提交

- `alloc_file_run(kind, n)`：返回连续段 `[first, first+n)` 的首 id；持久单调、
  号段预留（引擎内 kIdSegment=4096），n ≤ `data_ref.h:kMaxIdRun`(64)。批量派发
  保住单对象 chunk id 的连续性（否则并发写者交错，run 编码反而膨胀 28%）；
  弃用段尾无害（id 只需唯一单调）。
- `peek_reclaims(max, min_seq, max_extents)`：按 seq 升序取最早的至多 max 条；
  min_seq 是消费者的续扫检查点（队头被 grace/pin 跳过的条目不卡整轮、不重复
  计数）；max_extents 限制单批累计 extent 数（防 GB 级驻留），但至少返回 1 条。
- `ack_reclaim` / `ack_reclaims`：**物理删除成功之后**销账。批量版默认逐条转发，
  本地引擎覆写为单事务提交（sqlite 逐条 ack 是每条一次 fsync 且与业务提交争
  同一把写锁）。丢 ack 无害：gcq 残留重试，unlink 幂等。
- pack 存活账：`pack_stats()` 返回所有有账的 pack（含 live=0 与未封存——整
  空 pack 删除与重启弃用都依赖看到它们）；`seal_pack(id, file_size)` 幂等，
  **file_size=0 表示未知且不得覆盖已记录的非零值**；`drop_pack_stat` 在整文件
  unlink 成功后销账（与 ack_reclaim 同一顺序铁律）。
- `swap_extents(b, k, expect_version, from, to)`：压实换 ref 的乐观 CAS——
  version 或当前 DataRef 与 from 不符即返回 false。refs 增删走
  `meta_util.h:refs_delta` 的集合差（to−from 才 Put、from−to 才 Delete）：
  整段 add+remove 会误删仍被引用的 chunk refs，孤儿扫描随后 unlink 活数据。
  `swap_extents_batch` 每项独立 CAS、返回逐项成败；本地引擎覆写为单批提交，
  网络引擎保持逐项（合并成单事务会让一个 CAS 失败拖垮全批）。
- `try_gc_lease(owner, ttl_ms)`：多网关 GC 租约。共享引擎（redis/tikv）实现为
  带 TTL 的原子 CAS（同 owner 续租刷新 TTL）；本地引擎默认恒 true（单进程文件
  锁已保证独占）。租约不解决进程内 pin 表不共享的问题——部署约束
  `gc_grace ≥ 最长预期 GET 时长`仍然成立。
- `chunk_referenced(file_id)` / `scan_refs(cb)`：孤儿扫描的正/反向查询。
  `scan_refs` 快照语义宽松（迭代期间的并发增删可见与否不保证）——调用方用
  时点性 `chunk_referenced` 复查后才删，反向只告警不删 meta。

### 3.3 UndeterminedCommit：提交结局未定

`meta_store.h:UndeterminedCommit`（网络引擎特有）：redis EVALSHA 后掉连接、
tikv primary 提交超时——事务**可能已生效**。本地引擎"抛异常 ≈ 未提交"成立，
这两者不成立。调用方（`duostore_backend.cc:commit_or_discard`）遇到它**不做**
数据兜底删除：若提交实际生效，删掉的是对象已引用的数据（坏对象指向已删数据）；
若未生效，留给孤儿扫描收敛。对客户端仍是 InternalError(500)。

## 4. IDataStore 契约（`data_store.h:IDataStore`）

- `open_writer(WriteHint)` → `DataWriter`：hint 携带 `content_length`
  （chunked 时 nullopt，决定 pack/chunk 路径判定时机）与 `owner`（嵌入
  pack record 头，供压实反查与离线打捞；无 pack 的引擎忽略）。
  `DataWriter::finish()` 持久化后返回 DataRef；**未 finish 即析构 = 丢弃**。
- `write_batch(items)`：仅压实迁移使用。K 个 payload 一次落盘、返回等长
  DataRef 列表；默认逐项 open_writer，fs 实现覆写为单槽锁 + 单次 fdatasync
  的 pack 批量追加。
- `open_reader(ref, first, last)`：`[first,last]` 为 `resolve_range` 后的
  闭区间，返回流式 `BodyReader`（length()=last−first+1）。
- `remove(extents)`：幂等（ENOENT 忽略）；pack extent 内部跳过（死区归压实）。
- `remove_pack(pack_id)`：整 pack 文件删除。**纯虚**——无 pack 实体的引擎
  必须写显式 no-op 覆写；静默接口默认会让"有 pack 但忘实现删除"的新引擎
  编译通过、GC 记账却永不释放字节。
- `rewrite_pack(pack_id)`：压实顺扫，返回 `GcRewrite{scanned, migrated,
  corrupt, file_size}`；file_size 供崩溃遗留 seal(0) 的账回填分母。
  顺扫累积 K 条 `PackScanRecord{owner, from, payload}` 后一次交给
  `PackMigrateFn` 回调（逐条交付是每条一次 fdatasync + 一次 meta 提交）。
  数据面从不动原 record——pack 删除总走"存活账归零 + 整空 pack 删除"路径，
  误判不丢数据。
- `seal_aged_packs(max_age_ms)`：按龄封存 active pack（GC 每轮调一次；仅按
  容量封存则低写入量下 pack 永不轮转，死区永不进压实候选集）。默认返回 0。
- `scan_chunks(cb)` / `scan_packs(cb)`：孤儿扫描枚举，回调
  `(id, mtime_ms, size)`。**只枚举、不判活**（判定归调用方）。`scan_chunks`
  纯虚（不支持枚举的引擎显式抛，绝不静默假报"无孤儿"）；`scan_packs` 默认空扫
  （无 pack 实体即无泄漏面）。
- `pack_write_locked(pack_id)`：启动补封存与孤儿扫描的"活写者探测"（fs 探
  advisory lock）。**false 必须是保守方向不成立的一侧的反面**——返回 true 只
  推迟补封存，返回 false 可能封掉别人正在写的 pack。
- `stat_pack(pack_id)`：pack 实际文件大小，0 = 未知/不支持（回落顺扫回填）。
- `ChunkPinHooks`：写侧 pin 钩子。writer 分配 file_id 时 pin、未 finish 析构
  时 unpin；finish 后 unpin 责任转移给调用方（meta 提交/兜底删除后释放）。
  **凡产出 chunk 类实体的数据引擎都必须接上**——漏接会让孤儿扫描把慢速流式
  PUT 已落盘的前段当无引用文件删掉（mtime 宽限对长上传不够）。

## 5. PinTable：进程内引用计数

`duostore_backend.h:PinTable`——GC 与在途读/写的并发防线（主文档 §7）：

- pin 键是 **(is-pack, file_id) 二元组**：chunk/rados 共享 id 号段而 pack 用
  独立计数器，两空间数值必然重叠，单表会把"某 chunk 在读"误读成"同号 pack
  被 pin"。分表无假阳性。
- 16 个 shard 按 `file_id % 16` 分片锁：GET 热路径的 pin/unpin 与 GC 的批量
  `any_pinned` 不争一把锁。
- 读侧：`pin(extents)`/`unpin(handles)` 按 extent 逐段计数（同 file 多段计多
  次，对称释放）。写侧：`pin_id`/`unpin_id` 恒在 chunk 空间（写侧只产出
  chunk/rados）。`shared_ptr` 与 reader 共享——ObjectStream 随 HTTP 响应逃逸
  出 backend 生命周期，pin 表不能依赖 backend 存活。

## 6. put_object 写路径（`duostore_backend.cc:DuoStoreBackend::put_object`）

```text
① 调用方线程：validate_bucket_name / validate_object_key
② co_await pool_->schedule()；require_bucket 预检（权威检查在提交事务内复查）
③ pump_body：open_writer({length, "bucket\0key"})，循环
   body.read → md5.update → writer->write（流式，无整对象缓冲）
④ writer->finish() → DataRef；构造 WritePinRelease（写侧 pin 的对称释放守卫）
⑤ 组装 ObjectRec（size=ref.total()、etag=md5、version 由实现维护）
⑥ commit_or_discard：meta_->put_object(b, k, rec, cond)
   —— 提交点。条件检查在 meta 事务原子区内；旧 DataRef 同批入 gcq
```

- `duostore_backend.cc:pump_body`：PUT 与 upload_part 共用的泵送循环，64KiB
  栈缓冲，边写边算 MD5（哈希是 S3 语义，不进数据面接口）。chunk/pack 路由、
  active pack 轮转与封存都在数据面内部完成（见
  [./duostore-data-fs.md](./duostore-data-fs.md)）。
- `duostore_backend.cc:commit_or_discard`：提交抛异常时兜底
  `data.remove(ref.extents)`（co_await 不能进 catch，经 exception_ptr 移出）；
  兜底失败也无害——落入孤儿扫描/死区。**例外**：`UndeterminedCommit` 不删
  （§3.3）。
- `duostore_backend.cc:WritePinRelease`：协程帧退出（提交或兜底删除之后）
  释放写侧 pin；仅 `write_pins_` 时生效（注入构造无钩子，盲释放会错误递减
  并发读者的 pin）。

崩溃窗口矩阵（数据先落、meta 后提交，任何时刻崩溃都不产生"meta 指向不存在
数据"）：

| 崩溃点 | 后果 | 回收路径 |
| --- | --- | --- |
| ③④ 之间 / ④⑥ 之间（chunk） | chunk 在盘、refs 无记录 | 孤儿扫描（无 refs + mtime 逾宽限 + 无 pin → unlink） |
| 同上（pack） | record 已追加、无 live 记账 | 天然死区，压实回收 |
| pack append 中途 | 尾部 torn record | 重启弃用 active pack（§10），死区随压实回收 |
| ⑥ 之后 | 一切一致 | 旧数据在 gcq，GC 照常 |

## 7. get_object 读路径

`duostore_backend.cc:DuoStoreBackend::get_object`：

1. 池线程上 `require_object`（缺失时再查 bucket 以区分 NoSuchBucket/NoSuchKey，
   与 HEAD 的错误语义一致）；
2. `resolve_range` 解出闭区间 `[first,last]`；len==0 走空 body 短路；
3. **只 pin 命中区间的 extent**——按 manifest 顺序累加偏移，落在
   `[first,last]` 内的段进 hit 列表（Range GET 不为整对象 manifest 付 pin
   成本）；
4. `pins_->pin(hit)` **先于** `open_reader`（meta 读与 pin 之间的微窗口由
   gc_grace 覆盖）；open_reader 抛异常时对称 unpin 后重抛；
5. 内层 reader 包进 `duostore_backend.cc:PinnedReader`：析构时 unpin，经
   shared_ptr 持有 pin 表，reader 随 HTTP 响应逃逸后仍安全。

`head_object` 走 `meta_->head_object`（`decode_object_meta`，零 manifest
物化）。`delete_object` 是纯 meta 事务（幂等），物理回收全部异步由 GC 变现。

## 8. GC 与孤儿扫描

### 8.1 run_gc_once（`duostore_backend.cc:DuoStoreBackend::run_gc_once`）

入口序：池线程 → `BackgroundTaskGroup::Scope` 注册在途（close 等待）→
`gc_sem_` 互斥（手动钩子/后台 worker/孤儿扫描/dump/load 共一把协程信号量）→
`try_gc_lease`（多网关；TTL = max(2×gc_interval, 10min)）。四步：

1. **mpu_ttl 过期清理**：遍历 buckets × uploads，initiated 早于 `mpu_ttl` 的
   内部 `abort_upload`（分片入 gcq，本轮第 2 步即变现）。ttl ≤ 0 = 关闭。
   单桶 list 失败只跳过该桶，绝不让异常吞掉后续三步。
2. **gcq 消费**：`peek_reclaims` 批取（kGcBatch=256 条 / kGcBatchExtents=32768
   段封批）；逾 `gc_grace` 且 `pins_->any_pinned` 为假的项：
   `data_->remove(extents)` **物理删先行** → `ack_reclaims` 批量销账。
   顺序铁律：反序在删与销之间崩溃产生永久账外孤儿；正序只留 gcq 残留，重试
   幂等。跨轮水位线 `gcq_hi_`/`gcq_skips_`：无跳过条目到期的轮次从上轮高水位
   续扫（队头 grace 积压不再每轮重复 peek+decode；grace 跳过的重试时间为
   enqueue+grace，pin/删除失败为"本轮"）。纯内存优化，重启回落全扫。
3. **按龄封存 + pack 压实**：先 `seal_aged_packs`（本轮封的本轮即可评估）。
   候选 = sealed 且 live_recs>0 且（存活率 < `pack_gc_ratio` 或 file_size
   未知）；崩溃遗留 seal(0) 先用 `stat_pack` 回填分母。候选按可回收字节降序
   （未知 size 者垫底、同分按 pack_id 稳定排序防轮间震荡），按
   `gc_compact_max_packs`/`gc_compact_max_bytes` 预算截断（首个候选恒放行，
   否则超预算的单个大 pack 永远无法推进），余者计入 `packs_compact_deferred`
   下轮继续。`rewrite_pack` 顺扫触发迁移回调（§8.2）。`compact_blocked_`：
   上轮没迁干净的 pack（在途 mpu 分片/遗留 owner/存活损坏记录），live_recs
   无变化且冷却窗（gc_grace）未过则跳过重扫。
4. **整空 pack 删除**：sealed 且 live_recs==0、**首次见空起逾 gc_grace**
   （`pack_empty_since_` 记账；服务压实/删除瞬间已读到旧 ref 但尚未 pin 的
   读者）且无 pin → `remove_pack` → `drop_pack_stat`（同一顺序铁律）。
   两张簿记表按现存账目剪枝防无界增长。

### 8.2 压实迁移（`duostore_backend.cc:migrate_pack_records`）

`PackMigrateFn` 的标准实现，五步：

1. 按 owner 聚组（`parse_pack_owner`；kLegacyPart/kUnknown 保守不迁移）——
   同对象多条 record 一次 get_object + 一次换 ref，消掉逐条重写 manifest 的
   O(n²)；
2. 逐组反查存活：from 与当前 manifest 逐位置配对（对象缺失 = 全死区/在途
   mpu，保守不迁移；owner 只是提示，存活判据永远是"当前 DataRef 含 from" +
   换 ref 的 version 守卫）；
3. 存活 payload 经 `data.write_batch` 一次落盘（fs 实现单次 fdatasync），
   新 record 的 owner 统一转对象形态；
4. 逐组装配新 manifest → `meta.swap_extents_batch` 批量 CAS；
5. 失败组清理：chunk 类残留显式 `remove`（refs 从未建立，删之干净），
   pack 类成死区待下次压实；无论成败对称释放写侧 pin。返回成功迁移数。

### 8.3 孤儿扫描（`duostore_backend.cc:DuoStoreBackend::run_orphan_scan_once`）

同持 `gc_sem_` 与 GC 租约（反向对账的论证依赖 gcq 的 unlink→销账窗口不并发）。

- **refs 快照先行**：先 `scan_refs` 收集 R（排序去重的 u64 向量 + 命中位图，
  峰值内存较三张哈希表方案低一个量级），再枚举盘面——"文件先于 refs 落盘"
  的不变量保证 R 中 id 的文件在枚举开始前必已在盘，miss 即真丢失。
- **正向**：`scan_chunks` 枚举中内联分类——无 refs、mtime 逾 gc_grace、无
  pin → 候选；删除前再做时点性 `chunk_referenced` 复查（扫描间隙新提交的
  引用）。unlink 的 extent kind 跟随 `data_kind`（rados 引擎只认 kRados，
  硬编码 kChunk 会让 rados 孤儿删除静默空转）。
- **反向**：refs 有而文件缺 → **告警 + 计数（refs_missing），绝不删 meta**
  （数据丢失征兆，留人工介入）。
- **packs/ 双向对账**：pack 文件先创建、首条 record 提交时才写 packstat 行
  ——恰在该窗口硬崩会永久泄漏无账文件。无账 pack 过三道闸（mtime 逾宽限、
  无 pin、`pack_write_locked` 为假——锁探测在枚举回调外做，因为它要开 fd
  取 flock）才 `remove_pack`；反向 packstat 在而文件缺同样只告警。
- 顺带积累用量指标：chunk_bytes / pack_bytes（枚举反正要 stat）。

调度：`schedule_gc` / `schedule_orphan_scan` 用 `TimerQueue` 单发定时器，
`gc_tick`/`orphan_tick` **完成后重臂**（轮次永不重叠堆积）；`gc_enabled=false`
（多网关非指定实例）不排程但保留手动钩子。

### 8.4 scrub（`run_scrub_once`，roadmap §3.1）

`duostore_backend.cc:DuoStoreBackend::run_scrub_once` 是深度完整性巡检——
孤儿扫描只查**存在性**，scrub 读**内容**。**纯只读**：一切发现只落
LOG + `DuoScrubStats` 计数，绝不修复、绝不删除。CLI 入口
`lights3 fsck <backend>`（[../cli.md](../cli.md) §2.3）。

以 **meta 为驱动**而非盘面驱动，这是布局决定的：chunk 文件无头无尾，
crc32c 只存在 manifest 的 extent 数组里（§4.2），`scan_chunks` 枚举只给
`(id, mtime, size)`——盘面驱动无从校验。遍历 = `list_buckets` →
`list_objects` 分页 → `get_object` 取全量 manifest（与 dump 同构），加
`list_uploads × list_parts` 覆盖进行中 multipart 分片（分片持有 refs 与
活数据，跳过会把它们误报成 stale）。对每个 extent：

- **读回 + 重算 crc**：单 extent 构造 DataRef 走 `open_reader` 排空，边读边
  `crc32c_update` 与 manifest 对照——**独立于 `verify_chunk_crc` 开关**（该
  开关只管 GET 热路径）。读失败/短读 → `unreadable_extents`（pack 侧 crc
  失配由 reader 自身拒绝交付，也落在这一类）；读通但 crc 不符 →
  `corrupt_extents`。逐 extent 打点使报告能精确到 file_id。
- **refs 正向对账**（chunk/rados；pack 走 packstat 不进 refs）：巡检开始时
  先做 refs 快照（排序向量 + 命中位图，同孤儿扫描的内存取舍），manifest
  引用的 id 不在快照 → 时点性 `chunk_referenced` 复查 → 再重取 manifest 确
  认未被并发覆盖/删除，三关都过才报 `refs_missing`——这是"孤儿扫描可能删
  掉活数据"的危险信号，LOG_ERROR。
- **refs 反向对账**：快照中从未被任何 manifest 命中的 id，复查仍在 →
  `refs_stale`（泄漏嫌疑：孤儿扫描永远不会回收它的文件）。只 LOG_WARN——
  巡检中途 complete 的 MPU 会把分片 refs 转给一个已走过的列表位置，造成
  暂态误报，复跑确认。

并发边界：全程持 `gc_sem_`（自身 GC/孤儿扫描停摆 ⇒ 巡检期间没有任何
unlink，业务删除只入 gcq 不动数据，读回无需 pin）；GC 租约 best-effort 续
（失败只 WARN 不跳过——scrub 不删东西，对端 GC 并发至多造成 unreadable
误报）。限速经 `storage/scrub_throttle.h:ScrubThrottle`：按字节预算在
TimerQueue 上分片睡眠（≤500ms/片，片间探测 `bg_.closing()`），close 不被
长限速拖住；`DuoScrubOptions::max_bytes_per_sec` 每次调用传入而非配置项
（scrub 是运维触发的遍历，不是常驻 worker）。`bg_.closing()` 在对象与
extent 粒度探测，中断置 `aborted`（统计为部分结果）。

## 9. Multipart

- `create_multipart`：纯 meta 写（upload_id 由引擎生成）。
- `upload_part`：与 PUT 同一条泵送管线；owner 为
  `"mpu\0<b>\0<k>\0<id>\0<no>"`（P4 起带 b/k，complete 后压实仍能反查归属）；
  同号重传 last-write-wins，旧分片同批入 gcq。提交失败走同一兜底删除。
- `complete_multipart`：**纯元数据事务、零数据搬运**——各 part 的 extent runs
  按提交序拼接成对象 DataRef（`meta_util.h:assemble_completed_object`），
  O(#parts) 对比 localfs 串接式的 O(总字节)。
- `abort_multipart`：meta 事务批删 + 分片全部入 gcq。
- `list_parts`：协议上限 10000，全物化后 `apply_parts_page` 分页；
  `list_multipart_uploads`：游标/条数下推引擎（delimiter 非空时 limit=0），
  再统一 `apply_uploads_page`。
- 过期兜底：GC 第 1 步的 mpu_ttl 清理（§8.1）。

## 10. 启动恢复与关闭

**启动**（两个构造函数尾部）：`abandon_stale_packs` 对上一代遗留的未封存
pack 账补封存 `seal_pack(id, 0)`（数据面重启从不复用旧 active pack——新号段
+ O_EXCL，旧尾部 torn record 成死区；file_size=0 表示未知，压实时 stat/顺扫
回填）。两道闸防误封：`gc_enabled=false` 的从网关整体跳过；主网关对每个未
封存 pack 先 `pack_write_locked` 探测——误封他人（滚动重启重叠进程/共 root
误配）正在写的 pack 会让其进压实、账归零后被 remove_pack，对方 fd 写进已删
inode = 静默丢数据。封存失败仅 WARN 不阻启动。随后生成随机 `gc_owner_`
（租约身份，重启换新、旧租约靠 TTL 让位）并排程 GC/孤儿扫描。

**关闭**（`duostore_backend.cc:DuoStoreBackend::close`）：`closed_` 原子闩
幂等 → `shutdown_background`（`bg_.begin_close` → 组锁外 cancel 两个定时器
[TimerQueue::cancel 阻塞等在途回调] → `bg_.wait_idle` 等在途 GC 归零）→
`data_->close()`（封存 active pack）→ 切池线程 `meta_->close()`。析构函数
兜底：未 close 时同序执行 `shutdown_background` + `meta_->close()`（防回调
用已释放的 this；RocksDB 干净落盘）。

## 11. meta dump/load（`meta_dump.h` / `meta_dump.cc`）

经 IMetaStore 中介的逻辑 dump/load：dump 写全部 bucket/object 记录（含
extent manifest）与已封存 pack 账；load 逐条经 `put_object` 重放——记录级
重放天然兼作四引擎间的 meta 迁移工具，value 布局差异被接口吸收。backend 层
入口 `run_meta_dump` / `run_meta_load` 持 `gc_sem_`（写静默由运维保证：
`lights3 duostore dump|load <backend> <file> [--config=<path>]` 入口在服务启动前运行）。

**流格式**（`meta_dump.cc:dump_meta`）：magic `"L3DUOMETA1\n"` 后按 tag：

```text
'B' u32 len | bucket
'O' u32 blen u32 klen u32 vlen | bucket key value(codec::encode_object)
'S' u64 pack_id u64 file_size        （仅 sealed pack）
'E' u64 n_buckets u64 n_objects u64 n_packs u32 crc
```

crc32c 覆盖 magic 之后、crc 字段之前的全部字节。对象经 list_objects 分页 +
get_object 逐条导出（并发删除仅防御性跳过）。

**load**（`meta_dump.cc:load_meta`）的不变量：

- magic / 截断 / 逐字段长度上限（kMaxFieldLen=256MiB，防坏文件的随机长度字段
  喂给分配器）/ 末尾 crc / 三个计数全部校验，失败抛 InternalError；
- bucket 重放幂等（BucketAlreadyOwnedByYou 吞掉，支持中断后重跑）；
- 对象重放同时追踪所见最大 file_id（chunk/rados 共号段、pack 独立），末尾
  **通过接口烧号段抬高计数器**至 floor 之上——恢复后的新写绝不分配 ≤ 已存在
  文件的 id（碰撞 = 静默互相覆盖）；
- 'S' 记录恢复 sealed 状态与 file_size；未封存 pack 的账由对象重放以增量行
  重建；全死 pack（无对象引用）恢复为 live=0 的 sealed 账 → 下一轮 GC 整删。

**刻意不入档**（靠既有机制收敛）：进行中 multipart（upload_id 由目标引擎
生成，分片数据在恢复侧无引用 → 孤儿扫描回收）、gcq 待回收账（接口无入队
原语，同上回落）、bucket 创建时间（create_bucket 重打）、对象 version
（仅运行时压实 CAS 使用）。运维顺序契约：备份 = 先数据后 dump（"meta 引用
的数据必在备份内"）；恢复 = 先放回数据 → load → **强制孤儿扫描**
（`run_meta_load` 内建：释放信号量后调 `run_orphan_scan_once`，回收备份窗口
内数据侧多出的文件）。

## 12. 指标

`duostore_backend.cc:DuoStoreBackend::init_metrics` 注册（两个构造函数共用；
测试直构时落到隔离实例）：

| 类型 | 指标（前缀 `lights3_duostore_`） | 说明 |
| --- | --- | --- |
| counter | `gc_runs_total` / `gc_reclaims_total` / `gc_files_removed_total` / `gc_packs_removed_total` / `gc_uploads_expired_total` / `gc_packs_compacted_total` / `gc_packs_sealed_aged_total` / `gc_records_migrated_total` / `pack_corrupt_records_total` | 完整 GC 轮结束时一次性累加（`DuoGcStats` → 单调计数器） |
| counter | `gc_reclaims_by_reason_total{reason}` | 按 gcq reason 字节分桶（六个值全预注册，缺桶在 Prometheus 里读作"无数据"而非 0） |
| counter | `orphan_scans_total` / `orphan_chunks_removed_total` / `orphan_packs_removed_total` | 孤儿扫描 |
| counter | `read_corruption_total` | GET 链路 crc 不符；数据面经 on_corruption 回调递增，回调只捕获 shared_ptr（reader 逃逸 backend 生命周期仍安全） |
| gauge | `gc_skipped_grace` / `gc_skipped_pinned` / `gc_compact_deferred` | 每轮观测值而非累计（grace/pin 跳过每轮重计，单调计数器会虚增） |
| gauge | `gcq_depth` / `gcq_oldest_age_seconds` | 仅**全量轮**刷新（增量轮从高水位续扫看不到队头积压，刷新会周期性误报 0） |
| gauge | `chunk_bytes` / `pack_bytes` | 盘面用量，随孤儿扫描节奏刷新 |
| gauge | `pack_accounted_bytes` / `pack_live_bytes` / `packs` | accounted/live 之比 = 空间放大系数（留给查询侧计算，保精度） |
| gauge | `orphan_refs_missing` / `orphan_packstats_missing` | 数据丢失信号 |
| histogram | `gc_round_seconds` | 单轮 GC 墙钟时间 |
