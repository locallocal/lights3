# RedisMetaStore：DuoStore 元数据的 Redis 实现

本文是 `RedisMetaStore` 的实现级文档，展开
[../duostore-redis-meta.md](../duostore-redis-meta.md)（下称"设计文档"）的方案在代码中的
具体落法——选型论证、构建接入、配置与测试策略见设计文档，本文只讲代码层的契约与算法。
DuoStoreBackend 主体见 [./duostore-core.md](./duostore-core.md)；姊妹实现见
[./duostore-meta-rocksdb.md](./duostore-meta-rocksdb.md)、
[./duostore-meta-sqlite.md](./duostore-meta-sqlite.md) 与
[./duostore-meta-tikv.md](./duostore-meta-tikv.md)。涉及文件：

| 文件 | 内容 |
| --- | --- |
| `src/storage/duostore/redis_meta_store.h/.cc` | `IMetaStore` 的 Redis 实现（编译开关 `LIGHTS3_DUOSTORE_REDIS_META`） |
| `src/storage/duostore/meta_store.h` | `IMetaStore` SPI（同步契约、错误抛 `s3::S3Error`、`UndeterminedCommit` 类型） |
| `src/storage/duostore/codec.h/.cc` | value 编解码 100% 复用（四实现字节级同格式） |
| `src/storage/duostore/meta_util.h` | 共享纯计算 helper（`check_put_condition` / `assemble_completed_object` / `refs_delta` / schema 标记解析） |

契约要点（`meta_store.h:IMetaStore` 头注释）：同步接口、必须在池线程调用（hiredis
阻塞调用恰在此发生）；提交类方法单事务完成"写新 + 旧 DataRef 入 GC 账 + 引用/统计
更新"。头文件不泄漏 hiredis 类型：`redisReply` 前向声明 +
`redis_meta_store.h:RedisReplyDeleter`（`freeReplyObject`）包装出
`RedisReplyPtr`，连接结构 `RedisMetaStore::Conn` 定义在 `.cc`。

## 1. Key 空间设计

全部 key = 可配置前缀（`RedisMetaOptions::prefix`，默认 `duo:`）+ 后缀，构造集中在
`redis_meta_store.h` 声明的一组 `*_key()` 方法（复合段复用 `codec.cc` 的 `\0` 分隔
键构造器，合法性由共享校验层 + codec 纵深防御保证）。与 RocksDB 版 CF 的逐行对照
见设计文档 §2.2，此处按代码符号列全（略去前缀）：

| key | 结构 | 构造符号 | 内容与访问方式 |
| --- | --- | --- | --- |
| `schema` | STRING | `redis_meta_store.cc` 构造函数 | `"r1"`（`kSchemaValue`）；`SET NX` 抢首建，已存在则 `parse_schema_marker` 校验谱系并走迁移链（§8） |
| `buckets` | HASH | `buckets_key` | field=桶名，value=`codec::encode_bucket`；`create_bucket` = 单命令 `HSETNX` |
| `o:<b>` | HASH | `objects_key` | field=对象 key，value=`codec::encode_object`；点查 `HGET` |
| `oz:<b>` | ZSET | `zindex_key` | 全员 score=0 的字典序索引，member=对象 key；list 走 `ZRANGEBYLEX`（§5） |
| `up:<b>` | HASH | `uploads_key` | field=`<key>\0<id>`，value=`codec::encode_upload` |
| `uz:<b>` | ZSET（score 恒 0） | `uploads_zkey` | member=`<key>\0<id>`，`up:<b>` 的词序索引（roadmap §3.5），与 `oz:<b>` 同构；create/complete/abort 与 HASH 同脚本同批维护，delete_bucket 一并 DEL |
| `pt:<b>\0<key>\0<id>` | HASH | `parts_key`（内部拼 `codec::upload_key`） | field=十进制 part_no，value=`codec::encode_part`；complete/abort 整键 `DEL` |
| `refs` | HASH | `refs_key` | field=十进制 file_id，value=owner 简述；`chunk_referenced` = `HEXISTS` |
| `gcq` | ZSET | `gcq_key` | score=seq，member=`be64(seq) ‖ encode_reclaim`（be64 前缀保 member 唯一且自含 seq） |
| `ctr:chunk` / `ctr:pack` / `ctr:seq` | STRING 整数 | `kCounterChunk/Pack/Seq` 常量 | `INCRBY` 号段计数器（§6） |
| `pack:<id>` | HASH | `pack_key` | live_bytes / live_recs / file_size / sealed 四 field，`HINCRBY` 增量记账 |
| `gc_lease` | STRING | `try_gc_lease` 内 `key("gc_lease")` | 多网关 GC 租约，PX 过期（§7） |

约束声明：gcq 的 score 为 double，要求 seq < 2^53（`ack_reclaim` 注释），号段
速率下可用万年量级；`pack_stats()` 对 `pack:` 做 `SCAN MATCH` 时会转义 glob
元字符（前缀可含任意字节）。

## 2. 连接层：hiredis 同步调用与连接池

- **建连**（`redis_meta_store.cc:RedisMetaStore::make_conn`）：URI 在构造函数解析一次
  （`redis://[user][:pass]@host[:port][/db]` 或 `unix://<path>`，非法即抛
  `std::runtime_error`）；`redisConnectWithTimeout` / `redisConnectUnixWithTimeout`
  建连 + `redisSetTimeout` 命令超时（同一 `timeout_ms`，默认 3s）。重连状态按序补：
  `AUTH`（有密码时）→ `SELECT`（非 0 库）——脚本是 server 级状态，无需逐连接补载。
- **连接池**（`acquire` / `release`）：mutex 保护的空闲栈 `idle_`，取空则新建；归还时
  超过 `pool_size` 或已 `close()` 则直接 `redisFree`。`close()` 置 `closed_` 并清空
  池，之后 `acquire` 干净地抛 InternalError（防御纵深，误用变 500 而非崩溃）。
- **命令执行**（`exec`）：一律 `redisCommandArgv`（argc/argv/argvlen，二进制安全——
  key/field/value 均可含 `\0`；`run_on` 封装单连接执行与 `ctx->err` 上报）。
  `read_retry=true`（纯读）时连接级失败换新连接重试一次并计
  `m_reconnects_`；仍失败抛 InternalError。`read_retry=false`（提交类）失败 =
  **结果不明**，经 `throw_undetermined` 抛 `meta_store.h:UndeterminedCommit`
  ——调用方（DuoStoreBackend 的 commit_or_discard）据此**不得**回退物理删数据（§9）。
- **WAIT**（`wait_for_replicas`）：`wait_replicas > 0` 时，提交类命令成功后在**同一
  连接**上追加 `WAIT <n> <timeout/2>`（WAIT 只覆盖本连接此前的写；超时取命令超时一半
  保证 server 先于客户端读超时返回）。副本数不足/被拒**仅 WARN**——写已在主上生效，
  报错会误导 S3 客户端重试；WAIT 自身连接失败则该连接不回池（写本身已成功）。

## 3. guarded-commit：一个 Lua 脚本承载全部提交事务

提交类操作的原子性由 `redis_meta_store.cc:kCommitScript`（构造时 `SCRIPT LOAD`，
SHA 存 `sha_commit_`）保证：脚本在 Redis 单线程内原子执行，**脚本原子性即全局
原子性**，多网关共享同一 redis 即共享 meta，进程内不持业务互斥（仅号段小锁
`alloc_mu_`）。为什么必须 Lua 而非 MULTI/WATCH：提交都是"读-校验-写"复合事务，
MULTI 表达不了条件放弃，WATCH 是连接级状态与连接池冲突（设计文档 §3.1）。
分工铁律：**读与计算在 C++，Lua 只做前置条件字节级比对 + 批量写**，value 对脚本
全程不透明（否则等于用 Lua 重写 codec）。

脚本输入是扁平化四元组序列：`ARGV = n_checks | (type,key_idx,field,expected)* |
n_ops | (kind,key_idx,a,b)*`，key 经 KEYS 表按 1-based 下标引用（`RedisBatch::key_idx`
去重）。任一 check 失败立即 `return 0` 不写任何 op；全过则顺序执行 op `return 1`。

| check 类型 | 语义 |
| --- | --- |
| `eq` | `HGET` 后 `redis.sha1hex(v) == expected`——CAS 见证传 **SHA1 指纹**而非旧值原文（大 manifest 从 MB 级降到 40B，重试轮次不再重传全值；碰撞需对两个既存合法编码值做选择前缀攻击、收益只是打败自己的 CAS，不属防护目标） |
| `absent` / `exists` | `HGET` 为空 / `HEXISTS` 非 0 |
| `hlen0` / `zcard0` | 整 HASH / ZSET 为空（delete_bucket 空检查） |
| `sha1`（缺省分支） | `HGETALL` 整 hash，field 按数值序排序后拼接 value，`redis.sha1hex == expected`——complete/abort 的 parts 集合指纹；单次 `HGETALL` 取代早期"HKEYS + 逐 field HGET"（万分片 complete 曾在原子执行内打满整个实例） |

op 类型：`hset / hdel / hincr(HINCRBY) / zadd / zrem / del / set`。

C++ 侧组装器 `redis_meta_store.cc:RedisBatch`（`friend`，镜像 RocksDB 版 WriteBatch
的追加接口）：`expect_eq/expect_absent/expect_exists/expect_hlen0/expect_sha1` 追加
check，`hset/hdel/hincr/zadd/zrem/del` 追加 op，`commit()` 即一次
`EVALSHA`，返回 0 表示有并发修改。调用方的 CAS 循环形态统一为：

```text
for attempt in 0..kMaxCasRetries(16):
    cas_backoff(attempt)      # attempt>0 计 m_cas_retries_，指数退避 100µs..6.4ms
    重读旧值 → C++ 计算 → 组 RedisBatch → commit()
    成功 return；失败进下一轮
超限 → InternalError（病态热点，响亮失败优于活锁）
```

退避（`cas_backoff`）必不可少：无退避的紧循环会被对端网关的连续提交流饿死。

**EVALSHA 自愈**（`eval`）：收到 `NOSCRIPT`（server 重启 / SCRIPT FLUSH）时脚本
**明确未执行**，重新 `SCRIPT LOAD` 后重发是安全的；SHA 内容寻址，重载值不变。

## 4. 逐方法提交流水

每个提交方法的结构 = 读旧值 → `meta_util.h` 共享计算 → 组 batch → CAS 循环。
三个 batch 辅助件贯穿全部提交（同批原子落账）：

- `batch_refs`：按 extent 逐个 `HSET refs <file_id> <owner>` / `HDEL`（kPack 跳过
  ——pack 存活走 stats 账）；
- `batch_pack_delta`：同 pack 的 extent 先聚合，再对 `pack:<id>` 各两条 `HINCRBY`
  （live_bytes/live_recs）；`rec_overhead` 用 `codec::pack_rec_overhead*`，与
  file_size 同口径（记录头也计入）；
- `enqueue_reclaim`：seq 预派发（`alloc_id`，使脚本保持纯写确定性；CAS 重试浪费 seq
  无害），DataRef 超 `meta_util.h:kReclaimMaxExtents`(4096) 时切成多条 gcq 条目，
  member = `be64(seq) ‖ codec::encode_reclaim`。

| 方法 | 前置 check | 要点 |
| --- | --- | --- |
| `put_object` | bucket exists；旧 `o:<b>[k]` eq（或 absent） | 先 `check_put_condition`（条件 PUT 在本轮 CAS 见证的旧值上校验，check 与 commit 对外原子）；version = 旧+1；新 refs/pack 账 + 旧 DataRef 入 gcq（kOverwrite） |
| `delete_object` | 旧值 eq | 不存在直接 return false（幂等，不发脚本）；HDEL+ZREM+gcq（kDelete） |
| `create_upload` | bucket exists | id 出 `storage/multipart.h:new_upload_id` |
| `put_part` | upload exists；旧 part eq（或 absent） | 同号重传 last-write-wins，旧片入 gcq（kPartOverwrite）；commit 失败后若 upload 已消失（并发 complete/abort 赢了）改抛 NoSuchUpload |
| `complete_upload` | bucket + upload exists；parts 整表 sha1 指纹；旧对象 eq/absent | `scan_parts` 取原始 value 拼指纹；`assemble_completed_object` 选片；选中片 refs 转移至对象 owner、pack 账从 part 口径换记到对象口径（-part 头 +object 头，recs 相抵）；未选中片入 gcq（kComplete）；旧同名对象入 gcq（kOverwrite） |
| `abort_upload` | upload exists；parts sha1 指纹（防并发 put_part 漏账） | 全部片入 gcq（kAbort），整键 DEL parts |
| `delete_bucket` | bucket exists；`o:<b>` hlen0；`up:<b>` hlen0 | 池外先 HLEN 预检查给出精确错误码，原子性靠脚本内 hlen0 复查；空检查覆盖进行中 MPU（对齐 AWS） |
| `swap_extents` | 旧对象**整段原始字节** eq | 天然蕴含 expect_version 与 from 校验（C++ 先解码确认）；失败不重试——语义同版本失配，放弃本条；refs 走 `meta_util.h:refs_delta` 差集（全加全删会抹掉未迁移 chunk 的 refs，孤儿扫描会误删在引数据） |

`create_bucket`（`HSETNX`）与 `seal_pack` / `drop_pack_stat` / `ack_reclaim` 为单命令
即原子，不走脚本；`seal_pack` 恒 `HSET sealed 1`，file_size=0 走 `HSETNX`（不覆盖
已知值的契约）。

## 5. 列举路径

### 5.1 list_objects：kListScript 单脚本 1 RTT

`redis_meta_store.cc:kListScript`（SHA 存 `sha_list_`）把整个 list 循环放进一次原子
执行：delimiter 每组一次 re-seek，客户端驱动 = 每组 1 RTT；脚本内收敛为 1 RTT，且
原子执行天然等价固定 snapshot（一致视图）。算法与 RocksDB 版逐行同构：

- seek 下界：`start_after >= prefix` 时取 `'(' .. start_after`（排他）否则
  `'[' .. prefix`；
- **分页扫描**：`ZRANGEBYLEX oz LIMIT 0 200` 每页 200 条驱动（早期逐 key
  `HGET`，千 key 列举 = 2000 次 redis.call 打满实例）；跳组时弃页重 seek；
- delimiter 命中：组前缀入 groups，`bump`（末非 0xff 字节 +1，与
  `codec.h:bump_last_byte` 同法）构造组后继 seek 点跳过整组；同时
  `ZREVRANGEBYLEX` 取组尾 key 备作 next_token（截断恰落在组上时 token 须是组尾，
  对应 RocksDB 的 SeekForPrev）；
- 收满 max_keys 置 truncated，`next_token = last_emitted`；
- 收尾对命中 keys 分批 `HMGET`（每批 500，`unpack` 受 Lua C 栈限制）取 value。

C++ 侧 `list_objects` 解包四元组返回值，value 经 `codec::decode_object_meta` 解码
（算术跳过 extent runs 不物化大 manifest）；`max_keys <= 0` 直接返回空（S3 语义）。

### 5.2 其余列举

- `list_buckets`：`HGETALL buckets` + 客户端按名排序（桶数小）；
- `list_uploads`（roadmap §3.5）：先 `ZCARD uz:<b>` 与 `HLEN up:<b>` 对账。相等
  → 索引路径：`ZRANGEBYLEX uz:<b> <min> + LIMIT 0 <page>` 从游标起按 (key,
  upload_id) 序分页取 member，再 `HMGET up:<b>` 取值；`<min>` 取"marker 之后"
  与"prefix 起点"二者较大者（复合 marker 用 `(key\0id`；仅 key-marker 用
  `[key\x01`——key 不含 NUL，故这是严格大于该 key 的最小串），member 越过
  prefix 即停，`limit` 下推。HMGET 为 nil 的 member（写入与重建竞态）跳过并
  顺手 `ZREM`。不等 → 兼容路径：`HSCAN up:<b> COUNT 512` 全表分批（老版本网
  关或建索引前写入的表），`std::map` 去重排序返回全部，并**重建索引**
  （ZREM 多余 member、ZADD 缺失 field，普通命令分批），下一次列举即走索引；
- `scan_parts` / `list_parts`：`HGETALL pt:...` + 客户端按 part_no 数值排序，同时保留
  原始 value（供 complete/abort 的 sha1 指纹）；
- `pack_stats`：`SCAN MATCH <prefix>pack:* COUNT 512` 游标迭代（不阻塞 server）+
  逐 key `HGETALL`（GC 低频路径，逐 key RTT 可接受）；返回含 live=0 与未 seal 的
  条目（空 pack 整文件删除、重启放弃都依赖看见它们）；
- `scan_refs`：`HSCAN refs COUNT 512`，弱一致游标由孤儿扫描的双向复核契约容忍
  （`meta_store.h:IMetaStore::scan_refs` 注释）。

## 6. alloc_file_run：INCRBY 号段 + 空烧补偿

`redis_meta_store.cc:alloc_id`：`alloc_mu_` 小锁内派发内存区间 `[next, limit)`，耗尽
则 `INCRBY ctr:* kIdSegment(4096)` 预留新段。批量 run（`alloc_file_run(kind, n)`，
n ≤ `kMaxIdRun`）要求段内连续，换段丢弃残尾无害（id 只需唯一单调）。kRados 与
kChunk 共段（refs 不分 kind，防跨 kind id 碰撞）。

Redis 特有补偿（`IdRange::burned`）：**首次预留空烧一整段**（取 2×kIdSegment）——
跳过 AOF everysec 崩溃丢失窗口内可能已派发却随计数器回滚的 id；崩溃浪费号段无害。
计数器 INCRBY 走 `read_retry=false`：预留失败结果不明时抛 Undetermined 是可接受的
（号段烧洞无害，但此处发生在业务提交之前，S3 客户端重试即可）。

## 7. GC 记账接口

- `peek_reclaims`：`ZRANGEBYSCORE gcq <min_seq> +inf LIMIT 0 max`（score=seq 精确），
  member 前 8 字节 `codec::parse_be64` 反解 seq；累计 extents 达 `max_extents` 提前
  收批但至少返回 1 条（超大历史条目仍可消费）；
- `ack_reclaim`：`ZREMRANGEBYSCORE gcq seq seq` 盲删单命令；`ack_reclaims` 覆写为
  内嵌小脚本一次 RTT 批量删（丢 ack 无害——gcq 残留会重试、unlink 幂等，故脚本
  无需任何守卫）；
- `try_gc_lease`：内嵌小脚本 `GET` 判 owner + `SET PX ttl` 续期，原子实现
  "SET NX + 同 owner 续租"；TTL 由 Redis 过期承担，崩溃持有者自然让出；
- 顺序铁律不变：ack 类销账**必须**发生在物理删除成功之后（SPI 契约）。

## 8. schema 与多网关共享语义

构造函数（`redis_meta_store.cc:RedisMetaStore::RedisMetaStore`）：

1. 解析 URI → 建首连（不可达即响亮失败）→ `SCRIPT LOAD` 两脚本；
2. `SET schema "r1" NX` 抢首建；已存在则 `GET` 后
   `meta_util.h:parse_schema_marker`（谱系 `"r"`）校验：低于当前版本走
   `kSchemaMigrations` 迁移链（每步必须幂等——共享引擎无全局迁移锁，多个新版网关
   并发走链互不伤害），高于当前版本拒绝降级运行（防混部写坏新布局）；
3. `CONFIG GET appendonly` 探测 AOF，非 AOF 仅 WARN（托管 Redis 可能禁 CONFIG，
   降级为提示）——持久化语义论证见设计文档 §6。

多网关论证：所有跨 key 复合不变量都在单脚本内原子执行，因此多个网关进程指向同一
redis_uri 即共享 meta，无需任何进程间协调；进程内仅剩 `alloc_mu_`（号段）与
`pool_mu_`（连接池）两把资源锁，无业务锁。

## 9. 失败语义汇总

| 场景 | 行为 |
| --- | --- |
| 纯读命令连接失败 | 换新连接重试一次（`m_reconnects_`）；再失败 InternalError |
| 提交类命令连接失败/超时 | **结果不明**：抛 `UndeterminedCommit`（InternalError 子类）——提交可能已生效，调用方不得物理删数据回滚，孤儿留给扫描收敛；盲重放会把刚写入的 DataRef 记入 gcq、GC 回收在引数据 |
| `REDIS_REPLY_ERROR` | InternalError 携带 server 错误文本（`check_reply_error`） |
| NOSCRIPT | 重载脚本后重发（明确未执行，安全） |
| guarded-commit 返回 0 | 明确未提交：CAS 重试（指数退避，16 次上限） |
| WAIT 副本不足 | 仅 WARN（写已生效，报错会误导客户端重试） |
| close 后调用 | `acquire` 抛 InternalError（500 而非崩溃） |

指标（构造期注册，0 值可见；空 `MetricsScope` 返回游离实例便于测试直构）：
`lights3_duostore_redis_cas_retries_total`、`lights3_duostore_redis_reconnects_total`。
