# RedisMetaStore：基于 Redis 的 DuoStore 元数据存储

> 状态：R1-R3 已实现（`RedisMetaStore` 全接口 + guarded-commit 脚本 + 测试
> 套件接口化 + e2e，代码在 `src/storage/duostore/redis_meta_store.{h,cc}`，
> 编译开关 `LIGHTS3_DUOSTORE_REDIS_META` 默认 OFF）；R4 未开始（§10）。兑现
> [duostore-backend.md](duostore-backend.md) §12 的演进承诺：meta 侧换
> Redis，实现 `IMetaStore`（`src/storage/duostore/meta_store.h`），供多网关
> 共享同一份元数据。客户端库 hiredis（`third_party/hiredis` submodule，§7）。
> 本文中"主文档"指 duostore-backend.md，`§N` 不带前缀时指本文档章节。

## 1. 目标与非目标

| 目标 | 说明 |
| --- | --- |
| 实现 `IMetaStore` 全接口 | bucket / object / list / multipart / GC 记账全套，行为与 `RocksMetaStore` 语义等价——同一 meta store 测试套件全绿（§9） |
| 多网关共享 meta | 事务原子性由 Redis 服务端 Lua 脚本保证（§3.4），不依赖进程内互斥——这是相对 RocksDB 版的语义增强，也是引入 Redis 的动机 |
| 零新增编解码 | value 编码 100% 复用 `codec.cc`（§2.1），两实现字节级同格式 |
| 实现结构与 RocksDB 版逐行对应 | `RedisBatch` 镜像 WriteBatch 的提交形态（§3.2），压缩两实现的语义漂移面 |

非目标（显式声明）：

- **Redis Cluster 不支持**。复合事务（Lua 脚本）跨多个 key——bucket 表、
  对象 HASH、字典序 ZSET、refs、gcq、计数器——无法归入同一 hash slot；
  且对象 key 可含 `{}`，会形成意外 hash tag。逃生通道：全部 key 加统一
  hash tag（如 `{duo}`）可在 Cluster 上运行但退化为单 slot，不推荐、不
  测试。支持范围 = standalone / Sentinel 主从；
- **TLS 首期不启用**（hiredis 的 `hiredis_ssl` 为可选组件，项目已链
  OpenSSL，未来开启成本低，§5.5）；
- RESP3 / client-side caching 不用，RESP2 足够；
- 多网关共享 meta 时**数据面**的跨进程问题（主文档 §7 pin 表、§12 已注明
  需要租约/分布式宽限）不在本文档解决——本文只保证 meta 侧正确。

## 2. Redis 数据模型

### 2.1 总原则

1. **value 编码 100% 复用 `codec.cc`**（`encode_object / encode_upload /
   encode_part / encode_reclaim / encode_bucket / encode_extents`）。Redis
   的 key、value、HASH field 全部二进制安全，hiredis 用 `redisCommandArgv`
   显式传长度即可携带 `\0`（§5.1）。收益：零新增编解码代码；RocksDB /
   Redis 两实现磁盘（内存）格式字节级相同，codec roundtrip 专项测试直接
   共享；
2. **key 命名 = 可配置前缀（默认 `duo:`）+ `\0` 分隔的复合段**，即
   `codec` 键构造器的风格直接拼进 Redis key 名。分隔符合法性与主文档
   §4.1 同一论证：共享校验层已拒绝 object key 含 NUL、bucket 名限
   `[a-z0-9.-]`，`\0` 无歧义。前缀的作用：多个 backend 实例 / 多套测试
   共享一个 redis-server 而互不污染（§8、§9）；
3. **对象一桶两键：HASH 存本体 + ZSET 存字典序索引**。ZSET 全员
   score=0——同分时 Redis 按 member 字典序排序，`ZRANGEBYLEX` 即有序
   迭代原语，天然支撑 prefix / marker（§2.3）；元数据本体放同桶 HASH
   （field = 对象 key，value = `encode_object`），点查 `HGET` O(1)。
   两键在同一事务内同增同删（§3.3）。

   否决的备选：每对象一个顶层 STRING key——顶层 key 数爆炸、
   delete_bucket 空检查退化为 SCAN、与 ZSET 成员失配时难对账；元数据塞
   进 ZSET member——member 即身份，改元数据 = 换 member，不可行。

### 2.2 key 布局

与主文档 §4.1 的 CF 表逐行对照（下表 key 均略去 `duo:` 前缀）：

| RocksDB CF | Redis key | 结构 | 说明 |
| --- | --- | --- | --- |
| `default` | `schema` | STRING | 打开时 `SET NX` 写 `"r1"`，已存在则读出校验；谱系与 RocksDB 的 schema 区分。不设 `instance`——meta 本就为多网关共享，不绑实例 |
| `buckets` | `buckets` | HASH：field=`<bucket>`，value=`encode_bucket` | `create_bucket` = `HSETNX` 单命令即原子，返回 0 → BucketAlreadyOwnedByYou，无需脚本；`list_buckets` = `HGETALL` + 客户端按名排序（桶数小） |
| `objects` | `o:<b>` + `oz:<b>` | HASH + ZSET（§2.1 原则 3） | 点查 `HGET o:<b> <key>`；迭代走 `oz:<b>` |
| `uploads` | `up:<b>` | HASH：field=`<key>\0<id>`，value=`encode_upload` | `list_uploads` = `HGETALL` + 客户端按 field 字节序排序，即得 (key, upload_id) 序（与 RocksDB 前缀扫同序）；量受 mpu_ttl 约束，HGETALL 可接受（超大时的演进是 HSCAN 分批） |
| `parts` | `pt:<b>\0<key>\0<id>` | HASH：field=十进制 `part_no`，value=`encode_part` | 每 upload 一个 HASH；`complete/abort` 整键 `DEL`（对应 RocksDB 的范围删）；≤1 万 field，`HGETALL` + 客户端数值排序 |
| `refs` | `refs` | HASH：field=十进制 `file_id`，value=owner 简述 | `chunk_referenced` = `HEXISTS`，O(1) |
| `gcq` | `gcq` | ZSET：score=`seq`，member=`be64(seq) ‖ encode_reclaim(...)` | be64 前缀保证 member 唯一且自含 seq；`peek_reclaims` = `ZRANGEBYSCORE gcq -inf +inf LIMIT 0 max`（seq 从 member 前 8 字节精确解析）；`ack_reclaim` = `ZREMRANGEBYSCORE gcq seq seq`。约束：score 为 double，要求 seq < 2^53——每秒 1 万次删除可用 2.8 万年，声明即可 |
| `stats`（号段计数器） | `ctr:chunk` / `ctr:pack` / `ctr:seq` | STRING（整数） | `INCRBY` 号段预留（§4） |
| `stats`（pack 存活账） | `pack:<id>` | HASH：live_bytes / live_recs / file_size / sealed | `HINCRBY` 即增量记账（替代 RocksDB merge operator）；随 pack 聚合（主文档 P2）引入，当前无 pack 记录，`pack_stats()` 返回空——两实现行为一致 |

### 2.3 list_objects：ZRANGEBYLEX + 单 Lua 脚本

算法照搬 RocksDB 版（主文档 §4.4）：seek 起点 =
`max(prefix, start_after 的后继)`；delimiter 命中时归组、**组末字节 +1
构造后继 seek 点跳过整组**；多取一条判 `is_truncated`。迭代原语从
RocksDB Iterator 换成 `ZRANGEBYLEX oz:<b> [<seek> + LIMIT 0 <batch>`。

**整个 list 循环放进一个 Lua 脚本**，理由：

1. delimiter 列举每组一次 re-seek——客户端驱动 = 每组一个 RTT，千组即
   千次网络往返；脚本内循环收敛为 **1 RTT**；
2. Redis 脚本执行期间不插入其他命令，单脚本天然等价于 RocksDB 的固定
   snapshot——单次调用一致视图，语义与主文档 §4.4 对齐；
3. 工作量有上界：迭代次数 ≤ max_keys + 组数（每次 O(log n)），max_keys
   上限 1000，不会长阻塞 server。

脚本内对命中的 key 逐个 `HGET o:<b>` 取 value 一并返回；解码在 C++ 侧
（`codec::decode_object_meta`，跳过 extent runs 不物化，与 RocksDB 版
同一优化）。delimiter 组收尾且恰好收满时，`next_token` 需落在组尾：脚本
内 `ZREVRANGEBYLEX oz:<b> (<组后继> - LIMIT 0 1` 取组内最后一条 key
（对应 RocksDB Iterator 的 `SeekForPrev`）。

否决的备选：客户端多轮 ZRANGEBYLEX 驱动——省一个脚本，但 delimiter RTT
放大 + 跨轮无一致视图（列举中途的并发写可能漏/重），两点都输。

## 3. 事务与不变量

### 3.1 路线选择：Lua 守卫式提交，不用 MULTI/WATCH

`IMetaStore` 的提交类方法都是"读-校验-写"复合事务（bucket 存在性复查、
读旧记录算 version 与 gcq 账、complete 的 ETag 比对）。Redis 的三种原子
性原语对比：

| 路线 | 评价 |
| --- | --- |
| MULTI/EXEC | 只有无条件批量写，表达不了"校验失败则放弃"，出局 |
| WATCH + MULTI | 能做乐观事务，但 WATCH 是**连接级状态**——与连接池模型（§5.2）冲突（取到的连接可能带着残留 WATCH），且校验逻辑散在客户端两次往返之间，形态笨重 |
| **Lua 脚本（EVALSHA，已定）** | 服务端单线程原子执行；check 与写在同一脚本内零窗口；对连接无状态要求 |

**分工铁律：读与计算在 C++，Lua 只做「前置条件字节级比对 + 批量写」。**
value 是 `codec.cc` 的二进制格式，若在 Lua 里解析它，等于用 Lua 重写一遍
codec、维护两份格式代码——否决。因此 value 对脚本全程不透明：C++ 先读
后算，把"我读到的原始字节"作为前置条件传给脚本，脚本比对通过才落写；
比对失败即有并发修改，C++ 重读重试。

### 3.2 通用守卫式提交脚本（guarded-commit）

一个脚本服务所有提交类操作——这是 Redis 版对 WriteBatch 的替身：

```text
输入：KEYS = 本次涉及的全部 key
      ARGV = n_checks | check*        check := type, key_idx, field, expected
           | n_ops    | op*           op    := kind, key_idx, field, value, [score]
check 类型：eq（HGET == expected 原始字节）/ absent（HGET 为空）
           / exists（HEXISTS）/ hlen0（HLEN == 0）/ zcard0（ZCARD == 0）
           / sha1（HGETALL 按 field 数值序拼接后 redis.sha1hex == expected，§3.3）
op 类型：  hset / hdel / zadd / zrem / del / set / incrby / hincrby
语义：     任一 check 失败 → 立即 return 0，不执行任何 op；
           全部通过 → 顺序执行 op，return 1
```

C++ 侧提供 `RedisBatch`：`hset()/hdel()/zadd()/…` 镜像 WriteBatch 的
追加接口，另有 `expect_eq()/expect_absent()/…` 追加前置条件；`commit()`
即一次 `EVALSHA`。每个 IMetaStore 方法的实现结构与
`rocks_meta_store.cc` 逐行对应（读旧值 → 组 batch → 提交），仅"锁内
WriteBatch"换成"CAS 重试循环"：脚本返回 0 → 重读、重建 batch、重试，
重试前**指数退避**（100µs 起、上限 6.4ms——无退避的紧循环会被对端的连续
提交流饿死），上限 16 次，超限抛 `InternalError`（意味着病态热点竞争，
响亮失败优于活锁）。

### 3.3 逐方法流水

对照主文档 §4.5 的同批内容表（省略号段/纯读方法）：

| 操作 | 前置 check | 同批 op |
| --- | --- | --- |
| put_object | bucket exists；`o:<b>[k]` eq 旧原始字节（或 absent） | HSET `o` 新 ObjectVal（version = 旧 +1）+ ZADD `oz` + 新 chunk HSET `refs` + 旧 DataRef 入 `gcq` + HDEL 旧 `refs` + pack 账 HINCRBY 负增量 |
| delete_object | `o:<b>[k]` eq 读到的旧字节（absent 则直接返回 false，不发脚本） | HDEL `o` + ZREM `oz` + 旧 DataRef 入 `gcq` + HDEL `refs` + pack 账负增量 |
| create_upload | bucket exists | HSET `up`（id 由 `storage/multipart.h::new_upload_id` 生成） |
| put_part | `up:<b>[key\0id]` exists；`pt:…[part_no]` eq 旧字节（或 absent） | HSET `pt` + 新 `refs` + 旧分片入 `gcq` + HDEL 旧 `refs`（同号重传 last-write-wins） |
| complete_upload | `up:<b>[key\0id]` eq；`pt:…` **sha1 指纹**；`o:<b>[k]` eq 旧字节（或 absent） | HSET `o` + ZADD `oz` + HDEL `up` + DEL `pt:…` + 未选中分片入 `gcq` + 旧同名对象入 `gcq` + `refs` 转移 + pack 账 |
| abort_upload | `up:<b>[key\0id]` eq | HDEL `up` + DEL `pt:…` + 全部分片入 `gcq` + HDEL `refs` |
| delete_bucket | `buckets[b]` exists；`o:<b>` hlen0；`up:<b>` hlen0 | HDEL `buckets` + DEL `oz:<b>`（空检查覆盖进行中 multipart，对齐 AWS，与 RocksDB 版同一论证） |
| swap_extents | `o:<b>[k]` eq 旧对象**整段原始字节**（天然蕴含 expect_version 与 from 校验，C++ 先解码确认再提交） | HSET `o` 新 ObjectVal（version+1、DataRef=to）+ pack 账迁移 |
| GC 销账（ack_reclaim） | —（单命令即原子） | ZREMRANGEBYSCORE `gcq`（refs 已在业务事务同批删除，与 RocksDB 版一致；物理 unlink 之后调用，主文档 §9.1 顺序铁律不变） |

- **complete_upload 的 parts 指纹**：ETag 逐项比对、`combined_etag`、
  extent runs 拼接全在 C++ 完成（复用 `storage/multipart.h`，与 RocksDB
  版同一套 helper）；前置条件需要"parts 集合在读取后未变"，但把上万
  part 的原文再传回脚本比对太浪费——C++ 把读到的各 field 原始 value 按
  part_no 数值序拼接后取 sha1，脚本内 `redis.sha1hex` 对当前内容重算
  比对，O(parts) 服务端计算换 O(1) 网络传输；
- **create_bucket / bucket_exists / get_object / require_upload /
  list_* / peek_reclaims / chunk_referenced / pack_stats** 为单命令或
  纯读，不走脚本。纯读天然一致（单命令原子）；list 的一致视图由 §2.3
  脚本保证。

### 3.4 多网关下的原子性论证

RocksDB 版用一把 `std::mutex` 序列化复合不变量（主文档 §4.5）——它只
序列化**本进程**。Redis 版的对应物是 Lua 脚本在服务端单线程原子执行：
**脚本原子性即全局原子性**，对多进程、多网关同样成立。因此
RedisMetaStore **不持业务互斥**（仅保留号段派发的内存小锁，§4），锁语义
被乐观 CAS 重试取代；gcq 的 seq、file_id 经 `INCRBY` 分配，全局单调。
这是本实现相对 RocksDB 版的语义增强：多个网关进程指向同一 redis_uri 即
共享 meta，无需额外协调（数据面前提见 §1 非目标）。

### 3.5 脚本管理与盲重试禁令

- **脚本管理**：Lua 源码以 C++ raw string 常量内嵌（不部署 .lua 文件）；
  连接建立时 `SCRIPT LOAD` 记住 SHA，调用一律 `EVALSHA`；收到 NOSCRIPT
  错误则回退 `EVAL` 并重新 LOAD（server 重启 / SCRIPT FLUSH 自愈）；
- **盲重试禁令**：提交类脚本发出后连接超时/断开 = **结果不明**——上次
  可能已生效。此时若内部盲重试，重放的 put_object 会把"刚写入的同一
  DataRef"当旧值记入 gcq，GC 随后回收在引数据，破坏根本不变量。因此
  结果不明一律抛 `InternalError`，交由 S3 客户端重试——那时会重传数据、
  分配新 file_id，无此风险。允许自动重连重试的只有两类：纯读命令、
  发送前就失败（连接从池中取出即坏）的请求（§5.4）。CAS 返回 0 的重试
  不在此列——0 是明确结果，安全。

## 4. alloc_file_id：INCRBY 号段

与 RocksDB 版同构（主文档 §4.5）：`INCRBY ctr:chunk 4096` 返回新上界
hi，内存派发 `[hi−4096, hi)`；`IdRange` 结构、独立 `alloc_mu_` 小锁、
`kIdSegment = 4096` 常量照搬（alloc 在数据面每个 chunk 打开时调用，不能
排在业务提交之后）。seq 计数器同法走 `ctr:seq`——预派发使 gcq 入账成为
纯写 op，守卫式脚本保持确定性（Lua 内不产生新 id）。

**持久化警示**（对应 RocksDB 版"号段预留恒 WAL fsync"）：Redis 崩溃
回滚 INCRBY 会重发已使用的 file_id，与已落盘 chunk 文件冲突。三层缓解：

1. 严格正确性要求 `appendfsync always`（§6）；
2. 廉价缓解：**进程启动及每次重连后的首次预留额外空烧一个号段**——
   跳过 everysec 模式下 ≤1s 丢失窗口内可能已派发的 id（崩溃浪费号段
   无害，file_id 只需唯一单调，不需连续，与 RocksDB 版同一论证）；
3. 兜底检测：数据面 chunk 创建走 `O_EXCL`，撞已有文件即响亮报错，绝不
   静默覆盖。

## 5. hiredis 接入（同步客户端）

hiredis 同步 API 在池线程调用，正是主文档 §2.2 为 IMetaStore 选同步接口
时预判的模式（与 cloudproxy 在池线程跑同步 httplib 同构）。

### 5.1 命令构造

一律 `redisCommandArgv`（argc + argv[] + argvlen[]，二进制安全）——本
方案的 key、field、value 都可能含 `\0`。**禁止 `redisCommand` 的 `%s`
格式化**（按 C 字符串截断，是静默数据损坏，代码评审红线）。协议 RESP2
默认即可。

### 5.2 连接模型：小连接池

mutex 保护的空闲连接栈，规模默认 ≈ 线程池大小（`redis_pool_size`，
§8）；每次 IMetaStore 调用 RAII 取出/归还。否决 thread_local 单连接：
`close()` 时机、线程退出清理、坏连接重建都更难管；且本方案无跨命令会话
状态（无 MULTI/WATCH，§3.1），同一逻辑操作内换连接无害，池化最简。

### 5.3 reply 生命周期与错误映射

`redisReply` 统一以 `std::unique_ptr<redisReply, FreeReplyDeleter>`
包裹。错误分层，统一经 `throw_reply(what, ...)`（仿 RocksDB 版
`throw_status`：LOG_ERROR + 抛 `s3::S3Error`）：

| 来源 | 处理 |
| --- | --- |
| `ctx->err`（IO / EOF / 协议 / 超时） | 丢弃连接；发送前失败或纯读 → 重连重试一次；提交类结果不明 → `InternalError`（§3.5 禁令） |
| `REDIS_REPLY_ERROR` | `InternalError`(500)，携带 server 错误文本 |
| NOSCRIPT | 重新 `SCRIPT LOAD` 后重发（明确未执行，安全） |
| 语义性缺失（HGET 空等） | 不是错误——由 C++ 语义层转 `NoSuchKey` / `NoSuchBucket` / `NoSuchUpload`，与 RocksDB 版 NotFound 处理同构（主文档 §10） |

### 5.4 超时与重连

`redisConnectWithTimeout` 建连 + `redisSetTimeout` 命令超时（默认 3s，
`redis_timeout`）；连接失效即从池中丢弃、新建。重连后按序补状态：
`AUTH`（若 uri 带密码）→ `SELECT`（若非 0 号库）→ `SCRIPT LOAD`。重试
边界严格遵守 §3.5。

### 5.5 TLS 与 close

TLS 走 hiredis 的独立组件 `hiredis_ssl`（编译期 `ENABLE_SSL`），首期
不启用以减小构建面；项目已链 OpenSSL，列为可选演进。`close()`：排空
连接池逐一 `redisFree`，之后任何调用干净地抛 `InternalError`（仿
RocksDB 版 `db()` 守卫——防御纵深，误用变 500 而非崩溃）。

## 6. 持久化与一致性声明

Redis 默认持久化（RDB 快照）对本方案**不成立**——整库回档意味着 meta
指向已被 GC 的数据，破坏"meta 即真相"的根本不变量。部署要求 AOF，与
主文档 §6.3 的 meta_sync 对照：

| RocksDB 版 | Redis 对应 | 语义 |
| --- | --- | --- |
| `meta_sync: true`（每提交 WAL fsync） | `appendonly yes` + `appendfsync always` | 提交即持久 |
| `meta_sync: false` | `appendfsync everysec`（**推荐默认**） | 崩溃丢最近 ≤1s 元数据但**仍自洽**：duostore"数据先落、meta 后提交"的顺序（主文档 §6）保证丢 meta 只产生孤儿数据，走孤儿扫描回收——§6.2 崩溃矩阵的论证原样成立。唯一例外是 file_id 计数器回滚，由 §4 三层缓解覆盖 |
| — | 仅 RDB / 关持久化 | **不支持**（整库回档，见上） |

- 打开时 `CONFIG GET appendonly` 探测，非 AOF 打 WARN 日志（托管 Redis
  可能禁用 CONFIG 命令——降级为无法探测的提示，不拒绝启动）；
- **WAIT（可选）**：Sentinel 主从部署下，提交类脚本后可追加
  `WAIT <n> <timeout>` 等待 n 个副本确认，缩小 failover 丢写窗口（注意
  WAIT 只保证复制送达、不保证副本 fsync）。配置 `redis_wait_replicas`
  默认 0（不等待），不进首期实现（§10 R4）。

## 7. 构建接入与组件关系

### 7.1 hiredis submodule

`.gitmodules` 增：

```text
[submodule "third_party/hiredis"]
    path = third_party/hiredis
    url = https://github.com/redis/hiredis.git
```

不设 `shallow`（仓库很小，与 rocksdb 的处理不同）；`build.sh` 的
`LIGHT_MODULES` **始终 init**——纯 C、零系统级依赖、体量小，与 rocksdb
同策略（主文档 §13.2），不做惰性拉取。无 sudo 环境无额外要求。

### 7.2 CMake 预设（仿 rocksdb 模板，主文档 §13.3）

新增 option **`LIGHTS3_DUOSTORE_REDIS_META`，默认 OFF**，依赖
`LIGHTS3_DUOSTORE`。与 rocksdb 默认 ON 的理由相反：本实现的单测/e2e 需要
外部 redis-server 在场（§9），且是可选后端——日常构建不背这个包袱；
特性腐化风险由 CI 的可选矩阵覆盖，而非默认构建。

```cmake
if(LIGHTS3_DUOSTORE_REDIS_META)
    set(DISABLE_TESTS ON CACHE BOOL "" FORCE)
    set(ENABLE_SSL OFF CACHE BOOL "" FORCE)        # §5.5：首期不启用 TLS
    set(ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE) # 静态链接，与全仓惯例一致
    if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.25)
        add_subdirectory(third_party/hiredis EXCLUDE_FROM_ALL SYSTEM)
    else()
        add_subdirectory(third_party/hiredis EXCLUDE_FROM_ALL)
    endif()
    target_sources(lights3_core PRIVATE src/storage/duostore/redis_meta_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_REDIS_META)
    target_link_libraries(lights3_core PRIVATE hiredis::hiredis_static)
endif()
```

（hiredis ≥1.1 提供 `hiredis::hiredis_static` alias；更老版本退回
`hiredis_static` 目标名。）OFF 时配置 `meta: redis` 在 `from_params`
抛 "not compiled in" 的 `std::runtime_error`。

### 7.3 组件关系与复用

- 新文件仅 `redis_meta_store.{h,cc}`（+ 内嵌 Lua 常量），实现
  `IMetaStore`；`DuoStoreBackend`、`FsDataStore`、GC、S3 语义层零改动
  ——这正是主文档 §2.1 选语义级接口的兑现；
- 复用：`codec.{h,cc}` 全部 value 编解码与 crc32c（§2.1）、
  `storage/validate.cc`、`storage/multipart.h`（new_upload_id /
  validate_part_order / combined_etag）、`core/util/uri`（redis_uri
  解析，§8）；
- **不复用**：`codec` 的 CF key 构造器中 RocksDB 特有部分（`be64_key`
  的 refs/gcq key、`part_key` 的 be16 尾缀——Redis 侧 field 用十进制，
  §2.2），只取所需。

## 8. 配置

`DuoStoreConfig::from_params` 新增键（全为 YAML 标量，自动收入
`BackendConfig.params`，零解析器改动，惯例同主文档 §11）：

```yaml
backends:
  - name: duodata
    type: duostore
    root: ./data/duostore
    meta: redis                        # 默认 rocksdb
    redis_uri: redis://:pass@127.0.0.1:6379/0   # 或 unix:///path/to/redis.sock
    redis_prefix: "duo:"
    redis_timeout: 3s
    redis_pool_size: 8
    # 其余 duostore 键（chunk_size / pack_* / gc_* / mpu_ttl …）不变
```

| 键 | 默认 | 说明 |
| --- | --- | --- |
| meta | `rocksdb` | `rocksdb` / `redis`；未编译 `LIGHTS3_DUOSTORE_REDIS_META` 时选 redis → 配置错误 |
| redis_uri | —（meta=redis 时必填） | `redis://[:pass@]host:port[/db]` 或 `unix://<path>`，复用 `core/util/uri` 解析 |
| redis_prefix | `duo:` | 全部 key 的前缀（多实例/测试隔离，§2.1） |
| redis_timeout | 3s | 建连 + 单命令超时（`parse_duration_sec`） |
| redis_pool_size | 8 | 连接池大小（§5.2），建议 ≈ 线程池规模 |
| redis_wait_replicas | 0 | 提交后 `WAIT` 的副本数（§6，R4 才实现） |

`meta: redis` 时 `meta_path` / `rocksdb_block_cache` / `meta_sync` 被
忽略并打 WARN（持久化语义改由 Redis 侧 AOF 配置承担，§6）。
`DuoStoreConfig` 增 `meta_kind` 枚举与上述字段；`DuoStoreBackend`
构造函数按 `meta_kind` 分支构造（`#ifdef LIGHTS3_DUOSTORE_REDIS_META`
包裹 Redis 分支），测试注入构造函数不动。

## 9. 测试策略

1. **meta store 套件接口化**（前置重构）：`tests/unit/test_duostore.cc`
   的 meta 用例目前直接实例化 `RocksMetaStore`——先抽成
   `run_meta_store_suite(factory)`（仿 `backend_suite.h` 模式），
   RocksMetaStore 恒跑、RedisMetaStore 条件跑，两实现共享同一语义基线
   （GC 记账、号段单调、delete_bucket 挡进行中 MPU、list 分页/delimiter
   等既有用例全部继承）；
2. **真实 redis 的获取**（不假设 docker 可用）：测试启动时探测
   `redis-server` 可执行文件——找到则以随机端口 + 临时目录拉起私有实例
   （`redis-server --port <N> --save '' --appendonly no --dir <tmp>`，
   teardown 时 kill），找不到则**显式 SKIP 并打印原因**（不算失败）。
   `LIGHTS3_TEST_REDIS_URI` 环境变量可覆盖为外部实例（隔离靠每用例唯一的
   `redis_prefix`——pid + 计数器，不同运行互不碰撞）。否决
   miniredis 类假实现：C++ 生态无成熟件，且本方案的核心（Lua 脚本语义）
   假实现测不到；
3. **组合与 e2e**：注入构造
   `DuoStoreBackend(cfg, pool, RedisMetaStore, FsDataStore)` 跑
   `run_backend_suite`（与 memory/localfs/duostore 同一语义基线）；
   `run_e2e.sh` 增 `duostore-redis` 分支（同样探测 redis-server、缺则
   skip），CMake 注册于
   `if(LIGHTS3_DUOSTORE_REDIS_META AND LIGHTS3_DRIVER_BUILTIN)`；
4. **Redis 专项**：guarded-commit 冲突路径（并发写触发 check 失败 →
   重试收敛）；NOSCRIPT 自愈（SCRIPT FLUSH 后操作照常）；杀连接后纯读
   自动重连、提交类抛 InternalError（§3.5 边界）；swap_extents CAS
   放弃路径；list 的 delimiter 跳组与组尾 token；schema 校验与前缀隔离
   （两个不同 prefix 的 store 共用一个 server 互不可见）。

## 10. 实施拆分

| 阶段 | 内容 | 可独立验收 | 状态 |
| --- | --- | --- | --- |
| R1 | hiredis submodule + CMake option + build.sh；连接池 / reply RAII / 错误映射 / 脚本加载器；`ctr:*` 计数器与 alloc_file_id；bucket 四方法 + schema 校验；meta 测试套件接口化 + redis-server 探测/skip 机制 | RocksDB 套件重构后全绿；redis 在场时 R1 用例绿 | 未开始 |
| R2 | 通用 guarded-commit 脚本 + `RedisBatch`；object 四方法（含 list_objects Lua）+ refs / gcq / swap_extents / chunk_referenced / peek_reclaims / ack_reclaim | meta store 套件两实现全绿 + 冲突重试/CAS 专项 | 未开始 |
| R3 | multipart 全套（create / put_part / list_parts / list_uploads / complete / abort，含 parts sha1 指纹）；注入组合跑 `run_backend_suite`；`e2e_duostore_redis` | 后端一致性套件 + e2e 绿 | 未开始 |
| R4 | 打磨：AOF 探测告警、`redis_wait_replicas`、指标（CAS 重试 / 重连计数）、TLS 评估、文档状态头更新 | 全 ctest 矩阵（含 skip 路径）绿 | 未开始 |

R1 先做 bucket 而非 object：bucket 方法覆盖"单命令原子（HSETNX）+ 纯读
+ 最简脚本（delete_bucket 的空检查）"三种形态，把连接层与脚本机制的
地基打完，R2 的 guarded-commit 才是纯粹的业务翻译。
