# RocksMetaStore：DuoStore 元数据的 RocksDB 实现

本文是 `RocksMetaStore` 的实现级文档，展开
[../duostore-backend.md](../duostore-backend.md)（下称"主文档"）§4 的设计在代码中的
具体落法。DuoStoreBackend 主体与双接口拆分见 [./duostore-core.md](./duostore-core.md)；
姊妹实现见 [./duostore-meta-sqlite.md](./duostore-meta-sqlite.md) 与
[../duostore-redis-meta.md](../duostore-redis-meta.md) /
[../duostore-tikv-meta.md](../duostore-tikv-meta.md)。涉及文件：

| 文件 | 内容 |
| --- | --- |
| `src/storage/duostore/rocks_meta_store.h/.cc` | `IMetaStore` 的 RocksDB 实现（默认 meta 引擎） |
| `src/storage/duostore/meta_store.h` | `IMetaStore` SPI（同步契约、错误抛 `s3::S3Error`） |
| `src/storage/duostore/codec.h/.cc` | key/value 编解码（四实现共享 value 格式） |
| `src/storage/duostore/meta_util.h` | 跨实现共享的纯计算 helper（分片拼装、refs 差集、schema 标记解析） |

契约要点（`meta_store.h:IMetaStore` 头注释）：同步接口、必须在池线程调用
（DuoStoreBackend 在入口统一 `co_await pool->schedule()`，主文档 §2.2）；提交类方法
内部单事务完成"写新 + 旧 DataRef 入 GC 账 + 引用/统计更新"（主文档 §4.5）。

## 1. Key 空间设计

八个 column family（`rocks_meta_store.h:RocksMetaStore::Cf` 枚举，打开顺序即索引），
key 编码集中在 `codec.cc`（`\0` 分隔安全性由共享校验层保证，`codec.cc:require_no_nul`
再做纵深防御——违反即抛 InternalError，绝不静默产生跨记录 key 碰撞）：

| CF | key | value | 说明 |
| --- | --- | --- | --- |
| `default` | `"schema"` | 十进制版本号字符串 | 打开时校验/迁移（§4） |
| `default` | `"instance"` | uuid（`new_upload_id()` 生成） | 实例标识，首次建库写入 |
| `buckets` | `<bucket>` | `codec.cc:encode_bucket`（`u8 ver=1 \| u64 created_ms`） | bucket 名限 `[a-z0-9.-]`，字节序即字典序 |
| `objects` | `codec.cc:object_key` = `<bucket>\0<key>` | `codec.cc:encode_object`（ObjectVal v2，§2） | 字节序 = S3 字典序，直接支撑 list（§5） |
| `uploads` | `codec.cc:upload_key` = `<bucket>\0<key>\0<upload_id>` | `codec.cc:encode_upload` | 前缀扫 `<bucket>\0` 天然按 (key, upload_id) 排序 |
| `parts` | `codec.cc:part_key` = `<bucket>\0<key>\0<id>\0<be16 part_no>` | `codec.cc:encode_part` | big-endian 尾缀保证 part_no 升序；`codec.cc:part_no_of_key` 反解 |
| `refs` | `codec.cc:be64_key(chunk_file_id)` | owner 简述（objects/parts 的 CF key，调试用） | chunk/rados 存活引用表；`chunk_referenced` O(1) 点查 |
| `gcq` | `be64_key(seq)` | `codec.cc:encode_reclaim`（`u8 ver \| u8 reason \| u64 enqueue_ms \| runs`） | 待回收队列，seq 升序即入账序 |
| `stats` | 见下 | 8B 小端 i64 计数器 | 唯一挂 merge operator 的 CF |

`stats` CF 的 key 细分（`rocks_meta_store.cc` 匿名空间常量与 `pack_stat_key`）：

| key | 写法 | 含义 |
| --- | --- | --- |
| `"c0"` / `"c1"` | Merge（`kCounterChunk` / `kCounterPack`） | chunk / pack 的 file_id 号段计数器（§6） |
| `"q"` | Merge（`kCounterSeq`） | gcq seq 号段计数器 |
| `p<be64 pack_id>b` | Merge | pack 存活字节账 live_bytes |
| `p<be64 pack_id>r` | Merge | pack 存活记录数 live_recs |
| `p<be64 pack_id>s` | 普通 Put | 封存标记，value = 8B 小端 file_size；**存在即 sealed** |

同一 pack 的 `b/r/s` 三个子 key 共享前缀相邻存放，`pack_stats()` 一次前缀扫
（`Seek("p")`，key 长恒 10 = `'p'+be64+field`）边扫边聚合，无需三次点查。

merge operator（`rocks_meta_store.cc:CounterMerge`，`AssociativeMergeOperator`）：
操作数与全值同格式（`codec.cc:encode_counter_delta` / `decode_counter`，8B 小端 i64），
累加语义——号段预留与 pack 增量记账都免读改写，负增量天然支持。

## 2. Value 编码要点

全部复用 `codec.cc`（四实现字节级同格式，roundtrip 测试共享），首字节为版本号：

- **版本容错读**（`codec.cc:read_ver`）：object/upload 当前写 v2（一等元数据 kv 段），
  读侧接受 v1/v2——升级不需要停机重写存量记录；bucket/part/gcq 恒 v1
  （`codec.cc:check_ver` 严格相等）。
- **extent run 编码**（`codec.cc:append_extent_runs` / `read_extent_runs`）：连续
  file_id、等长 chunk 压成一个 run（主文档 §4.3）；pack extent 永不合并（count 恒 1）。
  解码前先做 `n_runs × 33B` 与剩余 payload 的合法性预检，坏长度字段不会膨胀出几十万
  条假 extent。
- **`codec.cc:decode_object_meta`**：list/HEAD 专用，算术跳过 runs 段
  （`codec.cc:skip_extent_runs`）不物化 Extent 数组——650k extent 的 5TiB 对象
  manifest（≈26MB）在 `head_object` / `list_objects` 上零成本。
- 损坏值统一抛 `InternalError`（`codec.cc:corrupt`）；编码侧超限（user-meta 字段
  >64KiB）抛 `InvalidArgument`（400，用户问题非库损坏）。

## 3. 打开、选项与调优

`rocks_meta_store.h:RocksMetaOptions`：

| 字段 | 默认 | 落点 |
| --- | --- | --- |
| `path` | — | `DB::Open` 目录（配置 `meta_path`，默认 `<root>/meta`） |
| `sync` | true | 每次 `commit()` 的 `WriteOptions::sync`（= 配置 `meta_sync`，主文档 §6.3） |
| `block_cache_bytes` | 64MiB | `NewLRUCache` → 全 CF 共享 `BlockBasedTableOptions::block_cache` |
| `write_buffer_bytes` | 64MiB | 每 CF `write_buffer_size`（memtable 容量） |
| `max_write_buffers` | 2 | 每 CF `max_write_buffer_number` |
| `max_background_jobs` | 2 | flush/compaction 后台线程总数 |
| `metrics` | 空 scope | 空 scope = 隔离实例，测试直接构造零接线 |

构造函数（`rocks_meta_store.cc:RocksMetaStore::RocksMetaStore`）要点：

- `compression = kNoCompression` 恒关、不外露（主文档 §13.3：元数据体量小，换零外部
  依赖）；`create_if_missing` / `create_missing_column_families` 开启，八个 CF 一次
  `DB::Open` 打开；只有 `stats` CF 的 options 额外挂 `CounterMerge`。
- **RocksDB 层无自定义 compaction/GC 钩子**：不设 compaction filter、不设 TTL——记录
  删除全部走业务 WriteBatch 的显式 Delete/DeleteRange，SST 空间由 RocksDB 默认
  compaction 回收；对象级 GC（gcq/压实/孤儿扫描）是 DuoStoreBackend 层的事（§7）。
- 观测指标（P5，`opt_.metrics.gauge_callback`）：`estimate_num_keys` /
  `block_cache_usage_bytes` / `sst_bytes` / `memtable_bytes` 四个 gauge，渲染时经
  `GetAggregatedIntProperty` 现读；db 关闭后经 `db_` 原子空判返回 0（弱引用语义）。
- 单进程独占由 RocksDB 自带 LOCK 文件保证（SQLite 版需自建 flock，对比见姊妹文档
  §2）；`try_gc_lease` 用接口默认实现恒 true——本地引擎的排他性已由文件锁兑现。

## 4. Schema 版本与迁移

`default` CF 的 `"schema"` 键存十进制版本号（当前
`rocks_meta_store.h:RocksMetaStore::kSchemaCurrent` = 1）。打开时：

- 键缺失（新库）→ 单 WriteBatch 写入 `schema` + `instance`；
- 存在 → `rocks_meta_store.cc:RocksMetaStore::migrate_schema`：
  `validate_schema_marker`（转发共享的 `meta_util.h:parse_schema_marker`，谱系前缀为
  空串）解析出版本——解析失败或**版本超前本构建**抛 InternalError（降级运行会静默
  写坏新布局）；版本落后则沿 `kSchemaMigrations` 迁移链逐级升级，每步迁移后单独提交
  新版本号戳（中途崩溃重启从断点续走）。链目前为空——记录级演进走 codec 的
  `read_ver` 容错读，此链保留给未来 CF/key 布局变更；缺迁移路径经
  `meta_util.h:throw_no_migration` 响亮失败。

构造中 Open 成功后的任何异常必须先走 `close()` 再重抛（构造抛出时析构不运行，否则
DB 句柄与 LOCK 文件泄漏）。

## 5. 读路径：迭代器与列举

纯读不加 `mu_`（get 幂等安全；迭代器/快照自带一致视图）：

- **`get_object` / `head_object`**：`objects` CF 点查（`get_raw`），后者用
  `decode_object_meta` 免物化 manifest（docs/archive/gaps.md §3.9）。
- **`list_objects`**（`rocks_meta_store.cc:RocksMetaStore::list_objects`，主文档 §4.4）：
  固定 `GetSnapshot()`（RAII `SnapshotGuard` 释放）+ `iterate_upper_bound` =
  `<bucket>\x01`（bucket 名无 NUL，`'\0'+1` 即桶域上界）。起点
  `Seek(bucket\0 + max(prefix, start_after))`，start_after 命中自身再 Next 一次；
  delimiter 命中归组后 **`Seek(组尾字节+1)` 整组跳过**（`codec.h:bump_last_byte`
  构造后继，四实现共享）——恰好收满时 token 须落组尾，故 Seek 后 `Prev()` 取组内
  最后一个 key 更新 `last_emitted` 再 `Next()` 归位；Seek 落到 `!Valid()`（组是桶尾）
  时 `last_emitted` 保持旧值但循环必然不再走截断分支，旧值不会被读到。多收一条判
  `is_truncated`，`next_token` = 最后发出的 key。`max_keys<=0` 直接返回空且不截断
  （S3 语义）。
- **`list_uploads`**：游标下推（docs/archive/gaps.md §5.1）——key 编码已按 (key, upload_id)
  有序，seek 点 = `<bucket>\0<key_marker>\0<id_marker>\0`（尾 `\0` 使落点恰在该
  (key,id) 之后），一条被跳过的记录都不读；`limit>0` 时收满即断。
- **`list_parts` / `scan_parts`**：`codec.cc:parts_prefix` 前缀扫，be16 尾缀保证
  升序，无需排序。
- **`peek_reclaims`**：`gcq` CF 从 `be64_key(min_seq)` 起顺扫，双上限——条数 `max`
  与累计 extent 数 `max_extents`（docs/archive/gaps.md §2.11：防按条数取批时 GB 级驻留），
  至少返回 1 条（拆分前遗留的超大单条仍可消费）。
- **`scan_refs`**：`refs` CF 全扫回调 file_id，不取 `mu_`——孤儿扫描按接口契约容忍
  弱一致快照。

## 6. 写路径：互斥 + 单 WriteBatch 提交

**提交模型**（主文档 §4.5）：一把 `rocks_meta_store.h:RocksMetaStore::mu_` 序列化全部
提交类操作，锁内"读旧值 → 校验 → 攒 WriteBatch → `commit()`"。`commit()` =
`Write(WriteOptions{sync=opt_.sync}, &batch)`，这就是本实现的守卫式提交
（guarded commit）形态：与 Redis 版的 Lua 前置条件脚本不同，本地引擎靠互斥让
"读-校验-写"零窗口，提交点单一且失败即未提交（无 `UndeterminedCommit` 问题）。锁内
含 WAL fsync，写吞吐上限 ≈ 1/fsync 延迟且 RocksDB group commit 失效——P1 明确接受，
升级路径 TransactionDB 仅注明不做。

同批记账三件套（提交类方法共用）：

| helper | 作用 |
| --- | --- |
| `rocks_meta_store.cc:RocksMetaStore::batch_refs` | chunk/rados extent 逐个写/删 `refs`（kPack 跳过——pack 走存活账） |
| `rocks_meta_store.cc:RocksMetaStore::batch_pack_delta` | 同 pack extent 先聚合再对 `p…b`/`p…r` 各一次 Merge；字节数按 payload+record 头开销计（`codec.h:pack_rec_overhead*`，与 file_size 同口径，docs/archive/gaps.md §2.3a） |
| `rocks_meta_store.cc:RocksMetaStore::enqueue_reclaim_locked` | 旧 DataRef 入 `gcq`；超过 `meta_util.h:kReclaimMaxExtents`（4096）按段拆成多条（每条独立 ack，unlink 幂等故拆分不改崩溃语义） |

逐方法同批内容：

| 方法 | 校验（锁内、批外读） | WriteBatch 内容 |
| --- | --- | --- |
| `put_object` | `require_bucket_locked`；读旧对象；`meta_util.h:check_put_condition`（条件 PUT 违反即抛，批不提交）；version = 旧+1 | Put objects + 新 refs + pack 账 +1；旧对象：gcq(kOverwrite) + 删 refs + pack 账 −1 |
| `delete_object` | 读旧对象，缺即返回 false（幂等，无写） | Delete objects + gcq(kDelete) + 删 refs + pack 账 −1 |
| `put_part` | `require_upload`；读旧同号分片 | Put parts + 新 refs + pack 账 +1；旧分片：gcq(kPartOverwrite) + 删 refs + pack 账 −1（last-write-wins） |
| `complete_upload` | `require_upload` + `require_bucket_locked`；锁内 `scan_parts` 重扫（parts 一致性）；`meta_util.h:assemble_completed_object` 选片拼装 | Put objects + Delete uploads + **DeleteRange 整个 parts 前缀**（免逐 key 重建、万分片不膨批）；选中分片 refs 转移（owner 改写为对象）+ pack 账从分片口径换记到对象口径（−part 头 +object 头，recs 一减一增抵消——保证日后按对象口径删除恰好清零）；未选中分片 gcq(kComplete) + 删 refs + 账 −1；旧同名对象 gcq(kOverwrite) + 删 refs + 账 −1 |
| `abort_upload` | `require_upload` | Delete uploads + DeleteRange parts + 全部分片 gcq(kAbort) + 删 refs + 账 −1 |
| `create_bucket` / `delete_bucket` | 存在性 / objects+uploads 双 CF 空检查（进行中 MPU 即 BucketNotEmpty，对齐 AWS） | 单 Put / 单 Delete |

**压实换 ref**（主文档 §9.2）：单项 CAS 内核
`rocks_meta_store.cc:RocksMetaStore::stage_swap_locked`——读对象、比对
`expect_version` 与 from.extents（不符返回 false 不碰批），符合则 version+1、
DataRef=to，refs 按 **`meta_util.h:refs_delta` 差集**操作（to/from 共享未迁移
chunk，全量先加后删在 last-wins 下净效果是删，会抹掉存活数据的 refs），pack 账
±object 口径。`swap_extents` 单项单批；`swap_extents_batch`（gaps §2.13 批量压实）
整批一个 WriteBatch 一次提交，逐项 CAS 独立——失败项不进批、不影响其余，全败则
不提交。

**GC 销账**不取 `mu_`（无跨 key 不变量的盲删，不排在业务 fsync 之后）：`ack_reclaim`
单条删；`ack_reclaims` 覆写接口默认的逐条转发为单 WriteBatch（RocksDB 版销账近乎
免费，对比 SQLite 版逐条 ack 各含一次 fsync）。`seal_pack` 例外地取 `mu_`：读改写
（file_size=0 不覆盖已知值的幂等语义需要先读 `p…s`）。`drop_pack_stat` 盲删三个
子 key，不取锁。

## 7. file_id 号段分配

`rocks_meta_store.cc:RocksMetaStore::alloc_id`（主文档 §4.5）：独立小锁 `alloc_mu_`
（数据面每开一个 chunk 都要调，不得排在业务提交的 fsync 后；锁序恒 `mu_ → alloc_mu_`
无环），常态是纯内存 `next += n`。段耗尽时对 stats 计数器 Merge `+kIdSegment`(4096)
并**恒 `sync=true` 提交**（独立于 `meta_sync`——崩溃丢预留会重发已用 file_id，与已
落盘 chunk 的 O_EXCL 冲突；§6.3"关 sync 仍自洽"依赖这里的无条件 fsync），再读回新
上界派发 `[hi−4096, hi)`。批量 run（`alloc_file_run`，n ≤ `data_ref.h:kMaxIdRun`=64）
保证一次写会话的 chunk id 连续、run 编码有效；换段丢弃的尾巴无害（id 只需唯一单调）。
`kRados` 与 `kChunk` 共用计数器——refs 表按裸 file_id 记账不分 kind，分开计数会在
同一 meta 上切换数据引擎（fs↔rados）时跨 kind 撞号。gcq seq 也走号段（`"q"` 计数器，
`enqueue_reclaim_locked` 内逐条取号）。

## 8. 恢复行为与生命周期

- **崩溃恢复即 RocksDB WAL 回放**，无实现侧修复逻辑：`sync=true` 时提交即持久；
  `sync=false` 崩溃丢最近数秒元数据但库自洽——duostore"数据先落、meta 后提交"顺序
  保证丢 meta 只产生孤儿数据，走孤儿扫描回收（主文档 §6.2 崩溃矩阵）；号段计数器
  由 §7 的无条件 fsync 单独兜底。
- **`close()`**：先 `db_.exchange(nullptr)` 摘除句柄（此后一切调用在
  `rocks_meta_store.cc:RocksMetaStore::db` 干净抛 InternalError 500，误用不
  段错误——契约仍是在途请求完成后才关），再销毁 CF 句柄、`db->Close()`。析构函数
  兜底调 close 并吞异常。
- 错误映射（主文档 §10）：非 ok `Status` 统一经 `rocks_meta_store.cc:throw_status`
  转 `InternalError` + error 日志；`IsNotFound` 不是错误，由语义层转
  `NoSuchKey/NoSuchBucket/NoSuchUpload`；损坏 value 由 codec 抛 InternalError。
