# SqliteMetaStore：DuoStore 元数据的 SQLite 实现

本文是 `SqliteMetaStore` 的实现级文档，展开
[../duostore-sqlite-meta.md](../duostore-sqlite-meta.md)（下称"设计文档"）在代码中的
具体落法；主设计脉络（CF 布局、崩溃模型、GC）见
[../duostore-backend.md](../duostore-backend.md)（下称"主文档"）。DuoStoreBackend
主体见 [./duostore-core.md](./duostore-core.md)，RocksDB 版对照见
[./duostore-meta-rocksdb.md](./duostore-meta-rocksdb.md)。涉及文件：

| 文件 | 内容 |
| --- | --- |
| `src/storage/duostore/sqlite_meta_store.h/.cc` | `IMetaStore` 的 SQLite 实现（编译开关 `LIGHTS3_DUOSTORE_SQLITE_META`） |
| `src/storage/duostore/meta_store.h` | `IMetaStore` SPI |
| `src/storage/duostore/codec.h/.cc` | value 编解码（100% 复用，key 构造器不用——key 是列） |
| `src/storage/duostore/meta_util.h` | 共享 helper（分片拼装、refs 差集、条件 PUT 检查） |

**阻塞调用与线程模型**：`IMetaStore` 是同步接口，契约要求在池线程调用——
DuoStoreBackend 在每个入口统一 `co_await pool->schedule()` 后才进入本类（主文档
§2.2），因此所有阻塞的 sqlite3 调用（含 COMMIT 的 fsync、busy_timeout 等待）都发生
在池线程上，不会卡住事件循环；本实现内部不再做任何线程切换。

## 1. Schema：表、索引与谱系标记

建表 DDL 全量在 `sqlite_meta_store.cc:kSchemaDdl`（`init_schema` 在一个事务里建表 +
种子 + 盖章）：

```sql
CREATE TABLE buckets(name BLOB PRIMARY KEY, val BLOB NOT NULL) WITHOUT ROWID, STRICT;
CREATE TABLE objects(bucket BLOB NOT NULL, key BLOB NOT NULL, val BLOB NOT NULL,
                     PRIMARY KEY(bucket, key)) WITHOUT ROWID, STRICT;
CREATE TABLE uploads(bucket BLOB NOT NULL, key BLOB NOT NULL, id BLOB NOT NULL,
                     val BLOB NOT NULL,
                     PRIMARY KEY(bucket, key, id)) WITHOUT ROWID, STRICT;
CREATE TABLE parts(bucket BLOB NOT NULL, key BLOB NOT NULL, id BLOB NOT NULL,
                   part_no INTEGER NOT NULL, val BLOB NOT NULL,
                   PRIMARY KEY(bucket, key, id, part_no)) WITHOUT ROWID, STRICT;
CREATE TABLE refs(file_id INTEGER PRIMARY KEY, owner BLOB NOT NULL) STRICT;
CREATE TABLE gcq(seq INTEGER PRIMARY KEY AUTOINCREMENT, val BLOB NOT NULL) STRICT;
CREATE TABLE counters(name BLOB PRIMARY KEY, val INTEGER NOT NULL) WITHOUT ROWID, STRICT;
CREATE TABLE pack_stats(pack_id INTEGER PRIMARY KEY, file_size INTEGER NOT NULL DEFAULT 0,
                       live_bytes INTEGER NOT NULL DEFAULT 0,
                       live_recs INTEGER NOT NULL DEFAULT 0,
                       sealed INTEGER NOT NULL DEFAULT 0) STRICT;
```

- **无任何二级索引**：主键即聚簇 B-tree（WITHOUT ROWID），全部查询按主键序命中
  （设计文档 §2.1 原则 3）；`refs`/`gcq`/`pack_stats` 的 `INTEGER PRIMARY KEY`
  即 rowid，数值有序。
- key 列一律 BLOB 绑定（memcmp 序 = S3 字典序），`STRICT` 强制列型；value 列存
  `codec.cc` 编码的 BLOB（`encode_object/encode_upload/encode_part/encode_reclaim/
  encode_bucket`），与 RocksDB 版字节级同格式。
- `counters` 种子行 `'chunk'/'pack'`（`kCtrChunk`/`kCtrPack`）由 `init_schema` 用
  绑定语句写入（`kCtrSeed`），不写进 DDL——常量单一来源。gcq 的 seq 不占计数器，
  走 `AUTOINCREMENT`（§5）。
- **谱系标记零表实现**：`PRAGMA application_id` = `sqlite_meta_store.cc:kAppId`
  （`0x4C335351`，"L3SQ"），`PRAGMA user_version` = `kSchemaVersion`（当前 1）。

## 2. 连接模型：读连接池 + 单写连接 + 独立号段连接

四类连接全部是 `sqlite_meta_store.cc:SqliteMetaStore::Conn`（`sqlite3*` + 以 SQL
字面量地址为键的常驻 prepared-statement 缓存 + 指标计数器 shared_ptr；头文件不泄漏
sqlite3 类型）：

| 连接 | 数量 | 同步档位 | 保护 |
| --- | --- | --- | --- |
| 写连接 `wc_` | 1 | `opt_.sync` → FULL/NORMAL | `mu_`（事务恒在其内） |
| 号段连接 `ac_` | 1 | **恒 FULL**（独立于 meta_sync，§5） | `alloc_mu_` |
| 读池 `idle_` | ≤ `pool_size`（默认 8） | NORMAL（只读无关） | `pool_mu_`；RAII `Lease` 取还 |

打开序列（`open_raw` → `check_lineage`（仅首连接）→ `apply_pragmas`）：

- `open_raw`：`sqlite3_open_v2(READWRITE|CREATE)` + `busy_timeout`（默认 5000ms，
  纵深防御——进程内本不该长 BUSY）。**谱系校验先于任何写入**：`check_lineage` 在
  裸连接上跑——app_id/ver 全 0 但 `sqlite_master` 非空 = 别人的库，拒绝且不留痕
  （连 `journal_mode=WAL` 的文件头改写都还没发生）；app_id 不符或版本超前也拒绝。
- `apply_pragmas`：`journal_mode=WAL`（库的持久属性）、`synchronous=FULL|NORMAL`、
  `cache_size` 按连接摊分（`cache_bytes / (pool_size+2)`，下限 256KiB——配置语义是
  进程级总预算，对齐 rocksdb_block_cache 的角色）、`journal_size_limit=4MiB`
  （auto-checkpoint 搬空后把 -wal 截回上限，防常驻历史高水位）、`temp_store=MEMORY`、
  `foreign_keys=OFF`（跨表不变量由事务保证，不引入隐式删除序）。
- **单进程独占 fail-fast**：构造函数对 `<path>.lock` 取 `flock(LOCK_EX|LOCK_NB)`
  （`lock_fd_`），第二实例直接拒绝启动——RocksDB LOCK 文件的对等物；不用
  `PRAGMA locking_mode=EXCLUSIVE`（连接级锁会与自身读池互斥）。`try_gc_lease` 用
  接口默认恒 true，排他性已由 flock 兑现。
- 读池取还：`read_conn` 弹出空闲连接或新开；新开后**重检 `closed_`**（TOCTOU——
  开连接期间 close 可能已完成 checkpoint+truncate，此时丢弃新连接失败返回）。
  `release` 归还前检查 `sqlite3_get_autocommit`：残留开放事务（Txn 回滚也失败的极端
  路径）先补 ROLLBACK——成功恢复 autocommit 即可安全回池，仍失败才销毁（否则裸读
  永远读冻结 snapshot、事务方法撞嵌套 BEGIN）。

## 3. 语句与事务原语

- `sqlite_meta_store.cc:SqliteMetaStore::Stmt`：单次使用 RAII——bind → step*，析构
  `sqlite3_reset + clear_bindings`。绑定恒 `SQLITE_TRANSIENT` 拷贝（免与 reseek/
  异常路径纠缠生命周期；meta 值小拷贝可忽略）；空串必须传非空指针
  （`bind_blob(nullptr)` 是 SQL NULL 不是零长 BLOB）。全部 SQL 是具名字面量常量
  （`kObjGet`/`kObjPut`/…），参数 `?N` 绑定，**禁止拼接 SQL**（注入面 + BLOB 截断源）。
  `step_busy()` 是号段路径专用的 BUSY 容忍变体：SQLITE_BUSY 返回 nullopt（单语句
  autocommit 事务取锁失败即确定未执行，重试安全）并 +1 busy 计数。
- `sqlite_meta_store.cc:SqliteMetaStore::Txn`：事务 RAII——构造发 `BEGIN IMMEDIATE`
  （写；进程内已被 `mu_` 序列化，永不 BUSY）或 `BEGIN`（读；WAL snapshot 一致视图），
  `commit()` 显式提交，**析构未提交即 ROLLBACK**——语义错误抛 `S3Error` 穿出方法时
  自动回滚，杜绝半程残留。
- `wconn()` 防御：取写连接前若发现残留开放事务（COMMIT 与兜底 ROLLBACK 相继失败）
  先补一次 ROLLBACK，仍失败即抛 500——绝不带着悬挂事务进入新提交（写连接唯一且
  从不重建，"事务套事务"会永久化）。

## 4. 写路径：守卫式提交 = mu_ + 单 SQL 事务

每个提交类方法 = 锁 `mu_` → `wconn()` → `Txn` → 事务内"读-校验-写"→ `commit()`。
这是三本地引擎中与接口契约字面对齐度最高的守卫式提交：读与写在同一隔离域内自由
交错，零窗口、无 CAS 重试、无 Redis 版的 parts sha1 指纹（`complete_upload` 事务内
`scan_parts` 即最新）。COMMIT 的 fsync 在锁内，写吞吐上限 ≈ 1/fsync 延迟——与
RocksDB 版同一取舍。

同批记账三件套（对照 RocksDB 版 `batch_*`）：

| helper | 实现 |
| --- | --- |
| `sqlite_meta_store.cc:SqliteMetaStore::write_refs` | chunk/rados extent 逐个 `kRefPut`（INSERT OR REPLACE）/ `kRefDel`；kPack 跳过 |
| `sqlite_meta_store.cc:SqliteMetaStore::write_pack_delta` | 同 pack 聚合后每 pack 一条 `kPackDelta`（`INSERT … ON CONFLICT DO UPDATE SET live_bytes=live_bytes+…` 算术 upsert）；字节按 payload+记录头开销计（`codec.h:pack_rec_overhead*`，与 file_size 同口径） |
| `sqlite_meta_store.cc:SqliteMetaStore::enqueue_reclaim` | `kGcqPut` 插入，**seq = AUTOINCREMENT rowid 随业务事务分配**——同批提交/回滚，回滚不产生账外 seq，重启不回退不重发（sqlite_sequence 共享业务事务），省一个号段计数器；超 `meta_util.h:kReclaimMaxExtents`(4096) 拆多条 |

逐方法事务内容与 RocksDB 版逐行对应（详表见设计文档 §3.3），差异点：

- `put_object`：条件 PUT 经 `meta_util.h:check_put_condition` 在事务内校验，抛出即
  Txn 析构回滚；owner 记 `b + '/' + k`（refs 调试列，非 codec 的 pack owner 格式）。
- `delete_object`：无行直接 return false——到此为止只读，Txn 回滚是纯 no-op。
- `complete_upload`：`kPartDelAll` 一条语句整前缀删（对应 RocksDB DeleteRange）；
  选中分片 refs 转移 + pack 账从分片口径换记对象口径（−part 头 +object 头，保证日后
  对象删除恰好清零）；未选中分片入 gcq(kComplete)。
- `swap_extents` / `swap_extents_batch`：单项 CAS 内核
  `sqlite_meta_store.cc:SqliteMetaStore::apply_swap`——version/extents 不符返回
  false 不落写；refs 走 `meta_util.h:refs_delta` 差集（全量先加后删会抹掉 to/from
  共享 chunk 的 refs，孤儿扫描会误删存活数据）。batch 版整批一个事务一次 fsync
  （gaps §2.13），逐项独立，全败不提交。
- **单写连接的后果**：`ack_reclaim`/`seal_pack`/`drop_pack_stat` 这类盲写也必须持
  `mu_`（否则语句会混进别的线程的开放事务）——RocksDB 版"GC 销账不排队"的性质在
  SQLite 单写者约束下保不住。因此 GC 消费端应走批量 `ack_reclaims`（覆写为单事务
  单 fsync；逐条 ack 每条一次独立提交含 fsync，且与业务写争同一把锁）。
  `seal_pack` 是单条幂等 upsert（`kPackSeal` 的 CASE：file_size=0 不覆盖已知非零值）。

## 5. 号段分配：独立 FULL 连接 + 确定性互斥

`sqlite_meta_store.cc:SqliteMetaStore::alloc_id`（与 RocksDB 版同构，kIdSegment=4096，
run ≤ `data_ref.h:kMaxIdRun`=64；kRados 与 kChunk 共用计数器防跨 kind 撞号）：

- 预留 = `ac_` 连接上的单语句 `kCtrReserve`（`UPDATE counters SET val=val+?
  RETURNING val`）。`ac_` **恒 `synchronous=FULL`**、独立于 `opt_.sync`——SQLite 的
  synchronous 是连接级 PRAGMA，正好让号段单独保 FULL 而业务连接可降 NORMAL（比
  RocksDB 版按次覆写 sync 标志更干净）；崩溃丢预留会重发已用 file_id、与已落盘
  chunk 的 O_EXCL 冲突，故必须先持久后派发。
- **锁序 `alloc_mu_ → mu_`**：预留期间持 `mu_` 把进程内唯一写者挡在门外
  （docs/archive/gaps.md §3.9）——否则号段 UPDATE 与业务事务在 db 级写锁上打 busy_timeout
  彩票（busy handler 不公平排队，写热点下连输 4 轮 = 20s，正常 PUT 变 500）。有界
  重试保留（`step_busy` ≤4 轮）但只针对绕过 flock 的进程外来客（裸 sqlite3 工具），
  超限抛 InternalError"id reservation starved"。alloc 由数据面在业务事务之外调用，
  不存在反向嵌套。

## 6. 读路径与列举查询

纯读走读池（`Lease`），单语句在 WAL 下自带一致快照；多语句的 `list_objects` 包一个
读事务：

- **`list_objects`**（`sqlite_meta_store.cc:SqliteMetaStore::list_objects`）：算法与
  RocksDB 版逐行对应，迭代原语换成范围 SELECT `kObjScanGe`
  （`key>=?2 ORDER BY key`）。整个循环包在 `Txn(c, immediate=false)` 读事务里——
  WAL snapshot 保证一致视图且不阻塞写者。起点：`max(prefix, start_after)`，
  start_after 生效时**追加 `\0` 构造后继**（BLOB memcmp 下 `key > s ⇔ key >= s+'\0'`，
  一条 `>=` 语句覆盖，对应 RocksDB 版"命中自身多 Next 一次"）；delimiter 命中归组后
  `codec.h:bump_last_byte` 构造后继 re-seek（重执行带新 `?2` 的同一 prepared
  statement，一次 B-tree 定位），token 落组尾用反向点查 `kObjPrev`
  （`key<?2 ORDER BY key DESC LIMIT 1`，对应 RocksDB SeekForPrev）；语句先于事务
  结束 finalize（`it.reset()` 在 `t.commit()` 前）。测试钩子
  `sqlite_meta_store.h:SqliteMetaStore::set_list_pause_for_test` 在发出首条后回调
  一次，供并发提交注入验证 snapshot 岿然不动（设计文档 §9 S4）。
- **`list_uploads`**：`kUpList` 用**行值比较** `(key,id) > (?2,?3)` 做复合游标下推
  （恰为主键序；docs/archive/gaps.md §5.1），`LIMIT ?4`（≤0 绑 −1 = 不限）。
- `list_parts`/`scan_parts`：`kPartScan` 按 `part_no` 数值序；`list_buckets`：
  `kBucketList` 主键序免费排序。
- `peek_reclaims`：`kGcqPeek`（`seq>=? ORDER BY seq LIMIT ?`）+ 累计 extent 上限
  `max_extents`（至少返回 1 条）。`chunk_referenced` = `kRefGet` 点查；`scan_refs`
  = `kRefScan` 全扫回调（弱一致快照，孤儿扫描契约允许）。
- `require_upload`/`bucket_exists`/`get_object`/`head_object`：读池单语句；
  `head_object` 用 `codec.cc:decode_object_meta` 免物化 manifest。

## 7. Schema 版本策略

`check_lineage`（§2）识别三态：真空库 → `init_schema` 建表盖章（app_id + user_version
一个事务）；版本超前本构建 → 拒绝降级运行；版本落后 →
`sqlite_meta_store.cc:SqliteMetaStore::migrate_schema` 沿 `kSchemaMigrations` 迁移链
逐级升级——**每步一个事务，含 `PRAGMA user_version` 盖章**，中途崩溃重启从断点续走；
缺迁移路径经 `meta_util.h:throw_no_migration` 响亮失败。策略与其余三引擎统一
（docs/archive/gaps.md §6.1）；记录级演进仍走 codec 的 `read_ver` 容错读，此链保留给表/列
布局变更。

## 8. 错误映射与损坏指标

集中在 `Conn::raise`（LOG_ERROR + 抛 `InternalError`）与 `Conn::classify_error`：

| 来源 | 处理 |
| --- | --- |
| `SQLITE_BUSY`（busy_timeout 耗尽） | `m_busy_`（`lights3_duostore_sqlite_busy_total`）+1 + InternalError——进程内理论不可达，出现即进程外有人碰库或号段饥饿 |
| `SQLITE_CORRUPT` / `SQLITE_NOTADB` | `m_corrupt_`（`lights3_duostore_sqlite_corruption_total`）+1 + 专项 ERROR 日志（数据丢失征兆）+ InternalError |
| step 无行 | 非错误——语义层转 `NoSuchKey/NoSuchBucket/NoSuchUpload` |
| 其余非 OK（FULL/IOERR/CONSTRAINT…） | InternalError(500) + `sqlite3_errmsg` 日志 |

两个计数器在构造函数**任何连接建立之前**注册（打开路径的 NOTADB——拿错文件——也要
计入；空 MetricsScope 返回隔离实例，测试零接线），`Conn` 持 shared_ptr 副本在错误
路径自增，值 0 即可见。

## 9. 关闭与冷备

`close()` → `sqlite_meta_store.cc:SqliteMetaStore::shutdown(graceful=true)`：三锁齐持、
置 `closed_`、清读池与 `ac_`；写连接先 `PRAGMA optimize`（失败仅 WARN），再
`sqlite3_wal_checkpoint_v2(TRUNCATE)` 把 WAL 合并回主文件并截断——**不用
`PRAGMA wal_checkpoint`**（被读者阻塞时经 sqlite3_exec 静默返回 OK，busy 标志只在被
丢弃的结果行里；v2 的返回码 + 残留帧数可检测），未截干净必须 WARN"冷备需带 -wal
文件"；最后释放 flock。干净关闭后目录只剩单个 DB 文件，拷贝即完整冷备（设计文档
§6.3）。`graceful=false` 供构造失败清理——库未曾干净打开，跳过收尾（必然失败且会
重复计 corruption）。close 后一切调用经 `wconn()`/`read_conn()` 的守卫干净抛
InternalError（500 而非崩溃）；析构兜底调 close 并吞异常。
