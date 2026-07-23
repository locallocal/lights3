# SqliteMetaStore：基于 SQLite 的 DuoStore 元数据存储

> 状态：S1-S3 已实现（`SqliteMetaStore` 全接口 + meta/backend 套件 + e2e，
> 代码在 `src/storage/duostore/sqlite_meta_store.{h,cc}`，编译开关
> `LIGHTS3_DUOSTORE_SQLITE_META` 默认 OFF）；S4 未开始（§10）。兑现
> [duostore-backend.md](duostore-backend.md)
> §12 "SQLite（单文件部署）"的演进承诺：meta 侧换 SQLite，实现
> `IMetaStore`（`src/storage/duostore/meta_store.h`），元数据收敛为**单个
> 数据库文件**，备份/迁移 = 拷一个文件。引擎源码为
> `third_party/sqlite` submodule（https://github.com/sqlite/sqlite.git，
> §7）。本文中"主文档"指 duostore-backend.md，"Redis 版文档"指
> [duostore-redis-meta.md](duostore-redis-meta.md)，`§N` 不带前缀时指本
> 文档章节。

## 1. 目标与非目标

| 目标 | 说明 |
| --- | --- |
| 实现 `IMetaStore` 全接口 | bucket / object / list / multipart / GC 记账全套，行为与 `RocksMetaStore` 语义等价——同一 meta store 测试套件全绿（§9） |
| 单文件部署 | 元数据 = 一个 `.sqlite3` 文件（+运行期 WAL/SHM 伴随文件），无目录树、无 manifest 集合；冷备 = 干净 close 后拷贝单文件（§6.3） |
| 零新增编解码 | value 编码 100% 复用 `codec.cc`（§2.1），三实现（RocksDB / Redis / SQLite）字节级同格式，roundtrip 测试共享 |
| 事务表达力最强的实现 | 复合不变量直接落在 SQLite 原生事务里：读-校验-写同事务零窗口，无 Redis 版的 CAS 重试、无 parts sha1 指纹（§3.4）——这是主文档 §2.1 选语义级接口的又一次兑现 |
| 测试零外部依赖 | 不需要外部 server（对比 Redis 版需探测 redis-server），单测/e2e 自洽，无 SKIP 路径 |

非目标（显式声明）：

- **多进程共享 meta 不支持**。SQLite 文件锁本身允许多进程，但数据面的
  pin 表 / GC 是单进程语义（主文档 §7、§12），且 NFS 等网络文件系统的
  锁不可靠——与 RocksDB 版同一前提：单进程独占。且与 RocksDB 的 LOCK
  文件对等地 **fail-fast enforcement**：构造时对 `<path>.lock` 取
  `flock(LOCK_EX|LOCK_NB)`，第二个进程（或同进程第二个实例）直接拒绝
  启动，绝不静默双开（`PRAGMA locking_mode=EXCLUSIVE` 不可用——连接级
  锁会与自身连接池互斥）。多网关共享 meta 的正路是 Redis 版；
- 不用 SQL 表达业务查询：SQLite 在此只是"带事务的有序 KV +几个计数
  器"，不做关系建模（§2.1 原则 1 的推论）；
- 不启用 FTS / json1 / rtree 等扩展，不加载运行时扩展（编译期
  `SQLITE_OMIT_LOAD_EXTENSION`，§5.1）；
- 分库分表 / 在线备份 API（`sqlite3_backup`）首期不做，列为演进（§10 S4）。

## 2. 数据模型

### 2.1 总原则

1. **value 编码 100% 复用 `codec.cc`**（`encode_object / encode_upload /
   encode_part / encode_reclaim / encode_bucket`），存 BLOB 列。收益与
   Redis 版同一论证：零新增编解码；三实现磁盘格式字节级相同，codec
   roundtrip 专项测试直接共享；GC 压实换 ref、版本递增等逻辑不因存储引擎
   分叉。

   否决的备选：全关系建模（size/etag/mtime/user_meta/extents 拆列拆表）
   ——SQL 可查询单字段是唯一收益，但 `IMetaStore` 没有任何按字段查询的
   接口需求；extent 嵌套数组关系化后 complete_upload 的拼接要写一遍
   行搬运；等于用 SQL DDL 重写一份 codec，维护两份格式代码。与 Redis 版
   "Lua 不解析 value"是同一条铁律的 SQLite 形态；
2. **key 列一律 BLOB 绑定**（`sqlite3_bind_blob`，绝不 bind_text）。
   SQLite 的 BLOB 比较即 memcmp = S3 字典序，且绕开两个坑：TEXT 存任意
   字节序列有 UTF-8 假设风险；SQLite 类型序 TEXT < BLOB，同列混存两型会
   破坏排序。建表全部 `STRICT`，列型 `BLOB` 由引擎强制——误 bind 直接
   报错，不静默混型；
3. **主键即索引：WITHOUT ROWID 聚簇表**。`objects` 以 `(bucket, key)`
   为主键的 WITHOUT ROWID 表，B-tree 按主键有序存储，`key > ?` 范围扫
   即 list 的有序迭代原语（§2.3），与 RocksDB objects CF 的 key 布局
   同构。注：超大 manifest（万分片 multipart 的 ObjectVal 可达数百 KiB）
   行会走 overflow 页——但 list 本就要读 val 解码 meta，聚簇反而省一次
   回表；普通 rowid 表 + 二级索引没有优势。

### 2.2 表布局

与主文档 §4.1 的 CF 表逐行对照：

```sql
-- 打开时若 user_version=0 则建表；全部 STRICT（§2.1 原则 2）
CREATE TABLE buckets(
  name  BLOB PRIMARY KEY,          -- <bucket>
  val   BLOB NOT NULL              -- encode_bucket
) WITHOUT ROWID, STRICT;

CREATE TABLE objects(
  bucket BLOB NOT NULL, key BLOB NOT NULL,
  val    BLOB NOT NULL,            -- encode_object（含 version / extent runs）
  PRIMARY KEY(bucket, key)
) WITHOUT ROWID, STRICT;

CREATE TABLE uploads(
  bucket BLOB NOT NULL, key BLOB NOT NULL, id BLOB NOT NULL,
  val    BLOB NOT NULL,            -- encode_upload
  PRIMARY KEY(bucket, key, id)
) WITHOUT ROWID, STRICT;

CREATE TABLE parts(
  bucket BLOB NOT NULL, key BLOB NOT NULL, id BLOB NOT NULL,
  part_no INTEGER NOT NULL,
  val    BLOB NOT NULL,            -- encode_part
  PRIMARY KEY(bucket, key, id, part_no)
) WITHOUT ROWID, STRICT;

CREATE TABLE refs(
  file_id INTEGER PRIMARY KEY,     -- chunk 引用表；owner 调试用
  owner   BLOB NOT NULL
) STRICT;

CREATE TABLE gcq(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,  -- 随业务事务分配，见下表 gcq 行
  val BLOB NOT NULL                       -- encode_reclaim
) STRICT;

CREATE TABLE counters(
  name BLOB PRIMARY KEY,           -- 'chunk' / 'pack'（gcq seq 不走计数器，见下）
  val  INTEGER NOT NULL
) WITHOUT ROWID, STRICT;

CREATE TABLE pack_stats(
  pack_id    INTEGER PRIMARY KEY,  -- 原生数值列：SQL 算术 UPDATE 即增量记账
  file_size  INTEGER NOT NULL DEFAULT 0,
  live_bytes INTEGER NOT NULL DEFAULT 0,
  live_recs  INTEGER NOT NULL DEFAULT 0,
  sealed     INTEGER NOT NULL DEFAULT 0
) STRICT;
```

| RocksDB CF | SQLite 对应 | 差异说明 |
| --- | --- | --- |
| `default`（schema/instance） | `PRAGMA application_id` = `0x4C335351`（"L3SQ"）+ `PRAGMA user_version` = 1 | SQLite 自带的文件谱系机制，零表实现；打开时 application_id 不符 → 拒绝（拿错文件），user_version 超前 → 拒绝（新版格式）。**app_id=0 且 ver=0（野生 SQLite 库的常态）时再查 `sqlite_master`：有表即别人的库，拒绝**——校验先于任何写入（含 WAL journal 转换），异库文件不留痕；只有真空库才允许建表盖章。否决专门 meta 表：多余 |
| `buckets` / `objects` / `uploads` / `parts` | 同名表 | RocksDB 用 `\0` 拼复合 key；SQLite 直接多列复合主键，`\0` 分隔符问题整个消失（bucket/key 无 NUL 的前提仍由共享校验层保证，但本实现不再依赖它做 key 编码） |
| `parts` 的 `<be16 part_no>` 尾缀 | `part_no INTEGER` 列 | 数值列天然升序，无需 big-endian 技巧 |
| `refs` / `gcq` 的 `<be64>` key | `INTEGER PRIMARY KEY` | 同上；`INTEGER PRIMARY KEY` 即 rowid，B-tree 按数值有序。**gcq 的 seq 不走号段计数器而走 `AUTOINCREMENT`**：随业务事务分配、同批提交/回滚——事务回滚不产生账外 seq，且避开"开放写事务内不能再碰号段连接"的单写者互斥（§4）；seq 64 位整型，无 Redis 版 2^53 约束 |
| `stats`（号段计数器） | `counters` 表（仅 chunk / pack） | `UPDATE … SET val = val + ? … RETURNING val` 一条语句完成预留（§4） |
| `stats`（pack 存活账，merge operator） | `pack_stats` 原生列 | `INSERT … ON CONFLICT DO UPDATE SET live_bytes = live_bytes + ?` 即增量记账，事务内与业务写同批；随 pack 聚合（主文档 P2）启用，当前 `pack_stats()` 返回空——三实现行为一致 |

codec 复用范围与 Redis 版相同：取全部 value 编解码 + crc32c；**不取**
CF key 构造器（`be64_key`、`part_key` 尾缀、`\0` 拼接）——SQLite 侧
key 是列，不是编码问题。

### 2.3 list_objects：主键范围扫 + 单读事务

算法照搬 RocksDB 版（主文档 §4.4）：seek 起点 =
`max(prefix, start_after 的后继)`；delimiter 命中时归组、组末字节 +1
构造后继 seek 点跳过整组；多取一条判 `is_truncated`。迭代原语换成：

```sql
SELECT key, val FROM objects
 WHERE bucket = ?1 AND key >= ?2 AND (?3 IS NULL OR key < ?3)  -- ?3 = prefix 上界
 ORDER BY key LIMIT ?4;
```

- 每次 re-seek（delimiter 跳组）= 重新执行带新 `?2` 的同一 prepared
  statement——一次 B-tree 定位，纯进程内开销。Redis 版为省 RTT 把循环
  塞进 Lua 脚本的动机在这里不存在，**循环留在 C++**，与 RocksDB 版代码
  形态一致；
- **一致视图**：整个 list 循环包在一个读事务里（`BEGIN DEFERRED` …
  `COMMIT`）。WAL 模式下读事务持有打开瞬间的 snapshot，且不阻塞写者
  （§3.1）——语义与 RocksDB 固定 snapshot、Redis 单脚本一致视图对齐；
- delimiter 组收尾且恰好收满时 `next_token` 需落组尾：
  `SELECT key FROM objects WHERE bucket=?1 AND key<?2 ORDER BY key DESC
  LIMIT 1`（对应 RocksDB `SeekForPrev` / Redis `ZREVRANGEBYLEX`）；
- 解码同两实现：`codec::decode_object_meta` 跳过 extent runs 不物化。

`list_buckets` / `list_uploads` / `list_parts` 皆为单条
`ORDER BY` 查询（uploads 按 `key, id`、parts 按 `part_no` 数值序），
排序由主键 B-tree 免费提供，无需客户端排序。

## 3. 事务与并发

### 3.1 连接模型：读连接池 + 单写连接

SQLite 同库同刻只允许一个写事务；WAL 模式下读写互不阻塞。据此：

| 路线 | 评价 |
| --- | --- |
| 单连接 + 互斥 | 最简，但纯读也排队——长 list 扫描会卡住提交，放弃 RocksDB 版"纯读走 snapshot 不加锁"的既有性质，出局 |
| thread_local 每线程一连接 | `close()` 时机、线程退出清理难管（Redis 版 §5.2 同一论证），且写连接仍需全局唯一，出局 |
| **读连接池 + 专用写连接（已定）** | 写侧：一条连接 + 一把 `std::mutex`，形态与 RocksMetaStore 的 `mu_` 完全对应（锁 = 事务边界）；读侧：小连接池（默认 ≈ 线程池大小），RAII 取还，WAL 下与写并行 |

全部连接打开同一 DB 文件；编译期 `SQLITE_THREADSAFE=1`（serialized，
默认值）——我们的借出协议已保证每连接同刻单线程使用，串行化互斥是防御
纵深，本场景的性能差异可忽略，换绝对安全（§5.1）。

### 3.2 写事务形态：RAII guard

每个提交类方法 = 锁 `mu_` → 写连接上一个事务：

```text
BEGIN IMMEDIATE            -- 立即取写锁；进程内已被 mu_ 序列化，永不 BUSY
  读旧值 / 前置校验          -- bucket 存在性、旧记录、parts 集合……
  （校验失败 → ROLLBACK → 抛对应 S3Error）
  全部写：INSERT/UPDATE/DELETE（对象 + refs 增删 + gcq 入账 + pack 账）
COMMIT                     -- 提交点；synchronous 档位见 §6
```

C++ 侧提供 `Txn` RAII guard：构造 `BEGIN IMMEDIATE`，`commit()` 显式
提交，**析构时未 commit 即 ROLLBACK**——语义错误抛 `S3Error` 穿出方法
时事务自动回滚，杜绝半程状态残留。这是 SQLite 版对 WriteBatch /
RedisBatch 的替身，且表达力更强：WriteBatch 只能"先读后攒批"，这里
读与写在同一隔离域内自由交错。

### 3.3 逐方法流水

对照主文档 §4.5 同批内容表（省略号段/纯读方法；每行 = 一个 §3.2 事务）：

| 操作 | 事务内校验 | 事务内写 |
| --- | --- | --- |
| put_object | bucket 存在；`SELECT val` 读旧对象（算 version 与 gcq 账） | `INSERT OR REPLACE objects` 新 ObjectVal（version = 旧 +1）+ 新 chunk `INSERT refs` + 旧 DataRef 入 `gcq` + `DELETE` 旧 refs + pack 账负增量 |
| delete_object | `SELECT val` 读旧对象（无行 → 直接 ROLLBACK 返回 false，幂等） | `DELETE objects` + 旧 DataRef 入 `gcq` + `DELETE refs` + pack 账负增量 |
| create_upload | bucket 存在 | `INSERT uploads`（id 由 `storage/multipart.h::new_upload_id` 生成） |
| put_part | upload 存在；`SELECT` 旧同号分片 | `INSERT OR REPLACE parts` + 新 refs + 旧分片入 `gcq` + 删旧 refs（同号重传 last-write-wins） |
| complete_upload | upload 存在；事务内 `SELECT … ORDER BY part_no` 读全部 parts，ETag 逐项比对 / `validate_part_order` / `combined_etag`（复用 `storage/multipart.h`）；读旧同名对象 | `INSERT OR REPLACE objects` + `DELETE uploads` + `DELETE parts WHERE …`（整前缀一条语句，对应 RocksDB 范围删）+ 未选中分片入 `gcq` + 旧同名对象入 `gcq` + refs 转移 + pack 账 |
| abort_upload | upload 存在 | `DELETE uploads` + `DELETE parts` + 全部分片入 `gcq` + 删 refs |
| delete_bucket | bucket 存在；`EXISTS objects` / `EXISTS uploads` 空检查（覆盖进行中 multipart，对齐 AWS，与两实现同一论证） | `DELETE buckets` |
| swap_extents | `SELECT val` 解码，比对 expect_version 与 from extents（不符 → ROLLBACK 返回 false） | `UPDATE objects` 新 ObjectVal（version+1、DataRef=to）+ pack 账迁移 |
| ack_reclaim / ack_reclaims | — | `DELETE gcq WHERE seq=?`（refs 已在业务事务同批删除；物理 unlink 之后调用，主文档 §9.1 顺序铁律不变）。注意本实现的逐条 ack = mu_ 内一次独立提交（sync=true 时含 fsync），与业务写争同一把写锁——**GC 消费端应走接口的批量 `ack_reclaims`**（本实现覆写为单事务单 fsync；RocksDB 版覆写为单 WriteBatch；接口默认逐条转发，丢 ack 无害故批量语义安全） |

- `create_bucket` = 事务内 `SELECT` 存在性检查 + `INSERT buckets`——`mu_`
  已序列化写者，显式检查与 RocksDB 版逐行对应，不依赖"约束冲突转错误码"
  的隐式路径；
- 纯读（bucket_exists / get_object / require_upload / list_* /
  peek_reclaims / chunk_referenced / pack_stats）走读池连接，单语句自带
  一致性，多语句的 list 见 §2.3 读事务。`peek_reclaims` =
  `SELECT seq, val FROM gcq ORDER BY seq LIMIT ?`；`chunk_referenced` =
  `EXISTS refs`。

### 3.4 三实现原子性对比

| | RocksDB 版 | Redis 版 | SQLite 版 |
| --- | --- | --- | --- |
| 原子提交原语 | WriteBatch | Lua guarded-commit 脚本 | SQL 事务 |
| 读-校验-写零窗口 | 靠进程内 `mu_`（读在锁内、批外） | 靠前置条件字节比对 + CAS 重试 | **事务内直接读**，天然零窗口 |
| 冲突处理 | 无（互斥串行） | 脚本返 0 → 重读重试（指数退避） | 无（互斥串行 + BEGIN IMMEDIATE） |
| complete 的 parts 一致性 | 锁内重扫 | sha1 指纹传脚本比对 | 事务内 SELECT 即最新 |
| 多进程原子性 | 无 | 有（服务端脚本） | 无（§1 非目标） |

SQLite 版是三者中**事务机制与接口契约（"提交类方法内部单事务完成"）
字面对齐度最高**的实现——不变量代码最少、无重试路径、无指纹技巧。互斥
仍保留（与 RocksDB 版同款 `mu_`）：SQLite 层面 BEGIN IMMEDIATE 已互斥，
进程内再加 mutex 是把"撞 SQLITE_BUSY 后重试"变成"排队"，路径更简单
确定。锁内含 COMMIT 的 fsync（`meta_sync=true` 时），写吞吐上限 ≈
1/fsync 延迟——与 RocksDB 版同一取舍，P1 级接受，注明不做 group commit。

## 4. alloc_file_id：counters 号段

与 RocksDB 版同构（主文档 §4.5）：`IdRange` 结构、独立 `alloc_mu_`
小锁（锁序恒 `mu_` → `alloc_mu_`）、`kIdSegment = 4096` 照搬。预留 =
**专用 alloc 连接**上的单语句事务：

```sql
UPDATE counters SET val = val + 4096 WHERE name = ?1 RETURNING val;  -- 返回新上界 hi
```

内存派发 `[hi−4096, hi)`。gcq 的 seq **不走号段**：业务事务持有 wc_ 的写
锁期间再写号段连接会撞 SQLite 单写者互斥（busy 等待直至自锁），而 seq 只
需"live 行内唯一 + 单调"——`AUTOINCREMENT` rowid 随业务事务分配、同批
提交/回滚，正好（§2.2）。`RETURNING` 需 SQLite ≥3.35，submodule 固定在
近期 release tag（§7.1），远高于此。

**持久化警示**（对应 RocksDB 版"号段预留恒 WAL fsync"）：崩溃回滚
计数器会重发已用 file_id，与已落盘 chunk 冲突。SQLite 的 `synchronous`
是**连接级** PRAGMA——所以 alloc 用独立连接、恒 `synchronous=FULL`，
不随 `meta_sync=false` 降档；业务写连接的档位不受影响。这比 RocksDB 版
（同一 DB 按次覆写 sync 标志）更干净。兜底与两实现相同：数据面 chunk
创建走 `O_EXCL`，撞已有文件即响亮报错。

**写锁竞争**：预留 UPDATE 与业务事务争 SQLite 单写者锁，先由
busy_timeout(5s) 吸收；写热点下 busy handler 不公平排队、可能连续输掉
竞争——SQLITE_BUSY 即单语句明确未执行，**有界重试（≤4 轮，每轮自带 5s
等待）而非立刻 500**，超限才抛 InternalError。

## 5. SQLite 接入

### 5.1 编译期选项

amalgamation（§7.2）以固定选项编译进 `lights3_core`：

```text
SQLITE_THREADSAFE=1            # serialized（§3.1：防御纵深，性能差异可忽略）
SQLITE_OMIT_LOAD_EXTENSION     # 不加载扩展，去 dlopen 依赖
SQLITE_DQS=0                   # 双引号字符串按标准 SQL 处理（防笔误）
SQLITE_DEFAULT_MEMSTATUS=0     # 免全局内存统计锁
SQLITE_LIKE_DOESNT_MATCH_BLOBS # 本方案 key 全 BLOB，LIKE 不参与
SQLITE_MAX_EXPR_DEPTH=0
SQLITE_OMIT_DEPRECATED
SQLITE_OMIT_SHARED_CACHE       # shared-cache 与 WAL/连接池模型无关且有害
SQLITE_USE_ALLOCA
```

（即官方"Recommended Compile-time Options"裁剪。不定义
`SQLITE_OMIT_AUTOINIT`——省一处初始化契约，收益微小。）

### 5.2 打开序列

每条连接 `sqlite3_open_v2(path, READWRITE|CREATE|NOMUTEX 不设*)` 后：

```sql
PRAGMA journal_mode = WAL;       -- 持久属性，见 §6
PRAGMA synchronous  = FULL|NORMAL;  -- 按 meta_sync（§6）；alloc 连接恒 FULL（§4）
PRAGMA busy_timeout = 5000;      -- 防御纵深：进程内本不该长 BUSY（§3.2/§4）
PRAGMA cache_size   = -<KiB>;    -- sqlite_cache / (pool_size+2)：cache 按连接生效，
                                 -- 配置语义 = 进程级总预算，摊到全部连接（§8）
PRAGMA temp_store   = MEMORY;
PRAGMA foreign_keys = OFF;       -- 不用外键：跨表不变量由事务保证（§3.3），不引入隐式删除顺序
```

谱系校验（§2.2）在上述 PRAGMA **之前**、以裸连接（仅 busy_timeout）执行
——`journal_mode=WAL` 是持久的文件头修改，异库文件必须在被碰过之前拒绝。

首连接负责建库：`application_id` / `user_version` 校验与建表
（§2.2）——放在一个写事务里，多连接并发打开也安全。

### 5.3 语句与资源 RAII

- **prepared statement 常驻缓存**：SQL 全部为具名字面量常量，每连接以
  字面量地址为键缓存（首次使用 `sqlite3_prepare_v2`，之后复用）；使用处
  RAII（bind → step → 析构 `sqlite3_reset` + `clear_bindings`），杜绝
  运行期拼 SQL 与重复编译。参数一律 `?N` 占位符绑定、`SQLITE_TRANSIENT`
  拷贝（免与 reseek/异常路径纠缠生命周期）——**禁止字符串拼接 SQL**（与
  Redis 版禁 `%s` 格式化同级红线：既是注入面也是 BLOB 截断源）；
- **连接卫生**：归还读池与复用写连接前检查 `sqlite3_get_autocommit`——
  COMMIT 与兜底 ROLLBACK 相继失败（Txn 析构吞异常）的极端路径会残留开放
  事务，带它回池 = 裸读永远读冻结 snapshot（静默陈旧）、事务方法撞嵌套
  BEGIN。残留读连接直接销毁；写连接先补 ROLLBACK、失败则响亮 500；
- `close()`：读池与号段连接直接关；写连接 `PRAGMA optimize` 后走
  `sqlite3_wal_checkpoint_v2(TRUNCATE)` 把 WAL 合并回主文件再最后关，
  然后释放 `.lock` 文件锁——干净关闭后目录里只剩单个 DB 文件，可直接
  拷贝冷备（§6.3）。**不用 `PRAGMA wal_checkpoint`**：被读者阻塞时它经
  sqlite3_exec 返回 OK、busy 标志只在被丢弃的结果行里，失败完全静默；
  v2 API 的返回码 + 残留帧数可检测，未截干净必须告警（提示冷备需带
  -wal 文件）。之后任何调用干净地抛 `InternalError`（仿 RocksDB 版
  `db()` 守卫；read_conn 在建连后重检 closed_，堵 TOCTOU 窗口）。

### 5.4 错误映射

统一经 `throw_sqlite(what, rc, conn)`（仿 `throw_status`：LOG_ERROR +
抛 `s3::S3Error`）：

| 来源 | 处理 |
| --- | --- |
| `SQLITE_CONSTRAINT_*` | 不预期进语义层（create_bucket 走显式检查，§3.3）→ `InternalError` + 日志（不变量被绕过的征兆） |
| step 无行 / SELECT 空 | 不是错误——语义层转 `NoSuchKey` / `NoSuchBucket` / `NoSuchUpload`，与 RocksDB NotFound 处理同构（主文档 §10） |
| `SQLITE_BUSY` | 理论不可达（§3.2）；busy_timeout 耗尽仍 BUSY → `InternalError` + 告警（进程外有人碰库，违反 §1 前提） |
| `SQLITE_CORRUPT` / `SQLITE_NOTADB` | `InternalError`(500) + corruption 告警（数据丢失征兆，对齐主文档 §10） |
| `SQLITE_FULL` / `SQLITE_IOERR` 等其余非 OK | `InternalError`(500) + error 日志（含 `sqlite3_errmsg`） |

进程内嵌库无连接/超时/结果不明问题——Redis 版 §3.5 的盲重试禁令在此
无对应物，COMMIT 返回即结果确定。

## 6. 持久化与一致性声明

提交点 = 写事务 COMMIT（WAL 追加）。与主文档 §6.3 对照：

| RocksDB 版 | SQLite 对应 | 语义 |
| --- | --- | --- |
| `meta_sync: true`（默认，每提交 WAL fsync） | WAL + `synchronous=FULL` | 提交即持久 |
| `meta_sync: false` | WAL + `synchronous=NORMAL` | 断电丢最近若干提交但**库仍自洽**（WAL 回放到最后一个完整 checkpoint 边界）：duostore"数据先落、meta 后提交"顺序（主文档 §6）保证丢 meta 只产生孤儿数据，走孤儿扫描回收——§6.2 崩溃矩阵原样成立。唯一例外 file_id 计数器，由 §4 独立 FULL 连接覆盖 |
| — | `journal_mode=DELETE`（回滚日志） | **不用**：每次提交整页 fsync 两次且写者阻塞全部读者，两项都劣于 WAL |

- WAL 是**库的持久属性**，首连接设置一次即写进文件头；
- checkpoint 用默认自动策略（WAL ≥1000 页时借道提交线程搬运）；后台
  worker 不做主动 checkpoint，首期不调——WAL 文件上界 ≈ 高峰未搬运量，
  meta 记录小，可接受；
- **冷备路径**：干净 `close()` 后（§5.3 checkpoint TRUNCATE）拷贝单
  文件即完整备份；运行中拷贝不保证一致（在线备份 API 列 §10 S4）。

## 7. 构建接入

### 7.1 sqlite submodule

`.gitmodules` 增：

```text
[submodule "third_party/sqlite"]
    path = third_party/sqlite
    url = https://github.com/sqlite/sqlite.git
    shallow = true
```

`shallow = true` 同 rocksdb（fossil 镜像全史很大）；submodule 固定到
近期 release tag（如 `version-3.50.x`）。`build.sh` 的 `LIGHT_MODULES`
始终 init（同 hiredis 策略——option OFF 也先拉，惰性拉取只留给系统级
重依赖的 seastar）。

**关键差异：git 树不含现成 amalgamation**。canonical 源码树需先生成
`sqlite3.c/sqlite3.h`（`./configure && make sqlite3.c`）；生成脚本跑在
tclsh 上——本机已有 `/usr/bin/tclsh` 8.6（无 sudo 环境满足），且新树
（≥3.49，autosetup 构建系统）无系统 tclsh 时能以内置 jimsh 自举，
配置探测为准、不硬依赖。

否决的备选：系统 libsqlite3（无 sudo 环境版本不可控、编译选项不可控，
§5.1 的选项集拿不到）；amalgamation 快照直接入库（脱离 submodule 惯例、
升级要手工搬文件、体量 9MB+ 的生成物进 git 历史）。

### 7.2 CMake 预设（仿 rocksdb/hiredis 模板，主文档 §13.3）

新增 option **`LIGHTS3_DUOSTORE_SQLITE_META`，默认 OFF**，依赖
`LIGHTS3_DUOSTORE`。与 Redis 版同为可选 meta 后端不进默认构建面；差异
在动机——Redis OFF 是因为测试要外部 server，这里是 amalgamation 生成
引入 tclsh 工具链依赖 + 可选件不背默认构建成本。特性腐化由 CI 可选矩阵
覆盖。

```cmake
if(LIGHTS3_DUOSTORE_SQLITE_META)
    # 生成 amalgamation（out-of-tree configure，产物在 build 目录）
    set(SQLITE_GEN_DIR ${CMAKE_BINARY_DIR}/sqlite-amalgamation)
    add_custom_command(
        OUTPUT ${SQLITE_GEN_DIR}/sqlite3.c ${SQLITE_GEN_DIR}/sqlite3.h
        COMMAND ${CMAKE_COMMAND} -E make_directory ${SQLITE_GEN_DIR}
        COMMAND ${CMAKE_COMMAND} -E chdir ${SQLITE_GEN_DIR}
                ${CMAKE_SOURCE_DIR}/third_party/sqlite/configure
        COMMAND ${CMAKE_COMMAND} -E chdir ${SQLITE_GEN_DIR} make sqlite3.c
        DEPENDS ${CMAKE_SOURCE_DIR}/third_party/sqlite/VERSION
        COMMENT "Generating SQLite amalgamation" VERBATIM)
    enable_language(C)  # 项目本体 CXX-only，amalgamation 是唯一 C 编译单元
    add_library(lights3_sqlite3 STATIC ${SQLITE_GEN_DIR}/sqlite3.c)
    target_include_directories(lights3_sqlite3 PUBLIC ${SQLITE_GEN_DIR})
    target_compile_definitions(lights3_sqlite3 PRIVATE
        SQLITE_THREADSAFE=1 SQLITE_OMIT_LOAD_EXTENSION SQLITE_DQS=0
        SQLITE_DEFAULT_MEMSTATUS=0 SQLITE_LIKE_DOESNT_MATCH_BLOBS
        SQLITE_MAX_EXPR_DEPTH=0 SQLITE_OMIT_DEPRECATED
        SQLITE_OMIT_SHARED_CACHE SQLITE_USE_ALLOCA)
    target_compile_options(lights3_sqlite3 PRIVATE -w)
    target_sources(lights3_core PRIVATE src/storage/duostore/sqlite_meta_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_SQLITE_META)
    target_link_libraries(lights3_core PRIVATE lights3_sqlite3)
endif()
```

（amalgamation 单编译单元即官方推荐形态，比 add_subdirectory 整个
autosetup 树可控得多；`DEPENDS VERSION` 使 submodule 升级触发重生成。）
OFF 时配置 `meta: sqlite` 在 `from_params` 抛 "not compiled in" 的
`std::runtime_error`（同 Redis 版形态）。

### 7.3 组件关系与复用

- 新文件仅 `sqlite_meta_store.{h,cc}`；`DuoStoreBackend`、`FsDataStore`、
  GC、S3 语义层零改动；
- 复用：`codec.{h,cc}` 全部 value 编解码与 crc32c（§2.1）及
  `bump_last_byte`（delimiter 跳组后继，与 RocksDB 版共用一份）、
  `storage/duostore/meta_util.h`（`assemble_completed_object`——
  complete_upload 的分片选择与对象拼装，三实现共用，兑现主文档 §2.1 的
  "共享 helper"承诺）、`storage/validate.cc`、`storage/multipart.h`
  （new_upload_id / validate_part_order / combined_etag）；
- **不复用**：`codec` 的 CF key 构造器（§2.2 表）；`core/util/uri`
  （无 URI，路径即配置）。

## 8. 配置

`DuoStoreConfig::from_params` 新增键（YAML 标量自动收入
`BackendConfig.params`，惯例同主文档 §11）；`DuoMetaKind` 增 `kSqlite`：

```yaml
backends:
  - name: duodata
    type: duostore
    root: ./data/duostore
    meta: sqlite                     # rocksdb（默认）/ redis / sqlite
    # sqlite_path: /ssd/duo-meta.sqlite3   # 可选：默认 <root>/meta.sqlite3
    sqlite_cache: 64MiB
    meta_sync: true                  # 沿用：映射 synchronous FULL/NORMAL（§6）
    # 其余 duostore 键（chunk_size / pack_* / gc_* / mpu_ttl …）不变
```

| 键 | 默认 | 说明 |
| --- | --- | --- |
| meta | `rocksdb` | 增合法值 `sqlite`；未编译 option 时选 sqlite → 配置错误 |
| sqlite_path | `<root>/meta.sqlite3` | DB 文件路径（对应 RocksDB 的 meta_path 可指 SSD 的用法）；父目录由 store 自建 |
| sqlite_cache | 64MiB | 页缓存**进程级总预算**（校验 ≥1MiB）：SQLite 的 cache_size 按连接生效，实现将预算摊到全部连接（1 写 + 1 alloc + pool_size 读，§5.2）——语义对齐 rocksdb_block_cache 的单一预算角色，不随连接数放大 |
| meta_sync | true | **沿用而非忽略**（对比 meta=redis 时被忽略）：SQLite 与 RocksDB 同为本地引擎，持久化档位仍归本进程管（§6） |

meta 引擎专属键（meta_path / rocksdb_* / redis_* / sqlite_*）出现但不属
于选中引擎时一律 WARN——`from_params` 内单一"键→归属引擎"表驱动，新增
引擎加表行即可，不存在各分支互相漏列对方键的问题。
`SqliteMetaOptions{path, sync, cache_bytes, pool_size}` 对齐
`RocksMetaOptions` 形态；读池大小复用默认 ≈ 线程池规模，不单独开配置键
（需要时再加，避免键膨胀）。

## 9. 测试策略

1. **meta store 一致性套件**：`tests/unit/meta_store_suite.h` 已接口化
   （Redis 版 R1 完成的前置重构直接受益）——`run_meta_store_suite`
   增 SqliteMetaStore factory，`#ifdef LIGHTS3_DUOSTORE_SQLITE_META`
   条件编译但**无条件运行**（无外部依赖，不存在 Redis 版的探测/SKIP
   路径）。factory 的"同一底层存储重开新实例"约定 = 同一临时 DB 文件
   反复 open/close，天然覆盖重启语义（号段不回退、schema 校验）；
2. **组合与 e2e**：注入构造
   `DuoStoreBackend(cfg, pool, SqliteMetaStore, FsDataStore)` 跑
   `run_backend_suite`；`run_e2e.sh` 增 `duostore-sqlite` 分支，CMake
   注册于 `if(LIGHTS3_DUOSTORE_SQLITE_META AND LIGHTS3_DRIVER_BUILTIN)`；
3. **SQLite 专项**（`tests/unit/test_duostore_sqlite.cc`）：
   - BLOB key 排序：含 0x01/0x7F/0xFF/非 UTF-8 字节的 key 的 list 顺序
     与分页 token（memcmp 序 = S3 序的落地验证）；
   - 重开持久性：对象/version/refs/gcq 账跨 close-reopen 原样保留（号段
     单调已由共享套件覆盖）；
   - 冷备：close 后单文件拷贝到新路径重开，数据完整、WAL/SHM 不残留；
   - swap_extents 乐观放弃路径（version / extents 不符 → false 不落写）；
   - 文件谱系：非 SQLite 文件、以及 app_id=0 但已有表的"别人的库"→ 响亮
     拒绝且不留痕（不建表、不盖章、journal 不转 WAL）；
   - 单进程独占：第二个实例被 `.lock` flock 拒绝，close 释放后可重开；
   - create_bucket 重复 → BucketAlreadyOwnedByYou；close 后调用 → 500。

   崩溃模拟（kill 后 WAL 回放对账）与 list 迭代中途并发写的一致视图注入
   属 S4 打磨项（需进程级 harness / 内部钩子）。

## 10. 实施拆分

| 阶段 | 内容 | 可独立验收 | 状态 |
| --- | --- | --- | --- |
| S1 | sqlite submodule + amalgamation 生成 + CMake option + build.sh；连接/语句 RAII、打开序列与 schema 建表校验、错误映射；`counters` 与 alloc_file_id（独立 FULL 连接）；bucket 四方法；套件接入 factory | RocksDB 套件不回归；S1 用例（bucket + 号段 + schema/重开）绿 | 已完成 |
| S2 | `Txn` guard + object 四方法（含 list 读事务与 delimiter 跳组）+ refs / gcq / swap_extents / chunk_referenced / peek_reclaims / ack_reclaim / pack_stats | meta store 套件三实现全绿 + BLOB 排序/冷备专项 | 已完成 |
| S3 | multipart 全套（create / put_part / list_parts / list_uploads / complete / abort）；注入组合跑 `run_backend_suite`；`e2e_duostore_sqlite` | 后端一致性套件 + e2e 绿 | 已完成 |
| S4 | 打磨：崩溃模拟（kill 后 WAL 回放对账）与一致视图注入专项、`sqlite3_backup` 在线备份评估、`PRAGMA optimize`/checkpoint 调优、指标（BUSY/corruption 计数）、文档状态头更新 | 全 ctest 矩阵绿 | 未开始 |

S1 先做 bucket 的理由同 Redis 版：bucket 四方法覆盖"约束冲突原子性
（create）+ 纯读（exists/list）+ 最简复合事务（delete 的空检查）"三种
形态，把连接层、语句缓存、事务 guard 的地基打完，S2 起是纯业务翻译——
且相对 Redis 版少整整一层（无脚本机制），S2/S3 的预期体量更小。
