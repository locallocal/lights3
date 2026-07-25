# TikvMetaStore：基于 TiKV 的 DuoStore 元数据存储

> 状态：T1-T4 已实现（`TikvMetaStore` 全接口 + 带 op 的 2PC 侧车提交器 +
> 守卫分片 + 测试套件/e2e 接线，代码在 `src/storage/duostore/tikv_client.{h,cc}`
> 与 `tikv_meta_store.{h,cc}`，编译开关 `LIGHTS3_DUOSTORE_TIKV_META` 默认
> OFF）；T5 未开始（§11）。submodule `third_party/client-c` 锁定 `78a557e`
> （2026-07-21，上游无 release tag 只能按 commit 锁定）。实施偏差：上游扩展
> 未走 fork，改为 in-tree 侧车（§6.3）。兑现
> [duostore-backend.md](duostore-backend.md)
> §12 "TiKV（多网关共 meta）"的演进承诺：实现 `IMetaStore`
> （`src/storage/duostore/meta_store.h`），meta 侧获得**水平扩展 + 多副本
> 高可用**，补齐 Redis 版（单点/主从）之上的最后一级。客户端库
> [tikv/client-c](https://github.com/tikv/client-c)。本文中"主文档"指
> duostore-backend.md，`§N` 不带前缀时指本文档章节。
>
> **先读结论**：client-c 的传输基建（PD/TSO、region cache、重试退避、
> lock 解析）经 TiFlash 生产验证，但其**事务提交层是 test-grade**——
> `Txn` 头文件自注 "only used for TEST"，mutation 只支持 Put（无
> Del/Insert/Lock）。引入它 = 引入一项**必须先扩展 2PC 层**的前置工程
> （§6.3），这是整个方案的主要工作量与风险点，实施拆分把它放在 T1。

## 1. 目标与非目标

| 目标 | 说明 |
| --- | --- |
| 实现 `IMetaStore` 全接口 | bucket / object / list / multipart / GC 记账全套，与 RocksDB / SQLite / Redis 版同一测试套件全绿（§10） |
| 多网关共享 meta，且 meta 自身分布式 | 事务原子性由 TiKV 乐观事务（Percolator 2PC）保证（§4）；相对 Redis 版的增量：meta 存储本身多副本（raft 多数派）、可水平扩展、无单点——这是引入 TiKV 的动机 |
| 零新增编解码 | value 编码 100% 复用 `codec.cc`（§3.1），与 RocksDB/Redis/SQLite 实现字节级同格式 |
| 回归有序 KV 模型 | TiKV 是全局有序事务 KV——key 布局、list_objects 算法可逐行对照 RocksDB 版（主文档 §4.1/§4.4），是四种 meta 实现中与 RocksDB 同构度最高的（§3.2） |

非目标（显式声明）：

- **不做 client-c 的全面加固**——只扩展本方案必需的最小面（2PC 的
  mutation op、Get 的 not_found，§6.3），其余（async commit、
  pessimistic 事务、coprocessor）不碰；
- **TiKV RawKV 模式不用**：master 分支的 client-c 无 RawKV 客户端
  （api-v2 分支有但 2022 年后停更）；且 IMetaStore 的复合事务本就需要
  TxnKV，Raw/Txn 混用同一集群是 TiKV 明确禁忌；
- **TiDB 不引入**：只依赖 PD + TiKV。代价是 MVCC GC safepoint 需自行
  运维（§7.3）；
- keyspace / API V2 多租户首期不启用（`ClusterConfig.api_version` 恒
  V1；前缀隔离用 `tikv_prefix`，§3.2）；
- 数据面跨进程问题（主文档 §7 pin 表）与 Redis 版同一声明：不在本文档
  解决，meta 侧正确性独立成立。

## 2. 接入路线选型（调研结论）

### 2.1 候选客户端对比

| 路线 | 评价 |
| --- | --- |
| **A. tikv/client-c（已定）** | 官方 C++ 客户端，Apache-2.0，CMake + C++17，`add_subdirectory` 可直接消费（§8）。传输层生产级：TiFlash 以它做 region 读与 lock 解析，2026-07 仍在活跃修正确性 bug（#243/#244/#247 均为 lock/snapshot 语义修复）。事务写路径 test-grade，需自行扩展（§6.3） |
| B. [tikv/client-cpp](https://github.com/tikv/client-cpp)（cxx 包装 client-rust） | 官方路线里功能上限最高（client-rust 是与 Go 并列的最成熟客户端，raw + txn + pessimistic 全有），但 client-cpp 自述 "proof-of-concept and under heavy development"（37 个 commit），且引入 Rust 工具链进构建链；成熟度低于 client-c 的传输层而构建面更重 |
| C. 裸 kvproto + 自研客户端 | grpc 直连可行，但 region cache、路由重试、backoff、lock resolver、TSO 缓存全要重写——恰是 client-c 里经过生产验证的那 60%，重造无价值 |
| D. 经 TiDB（MySQL 协议） | 变成"SQL meta store 第二版"，SqliteMetaStore 的模式可搬，但部署面从 PD+TiKV 膨胀到 PD+TiKV+TiDB，且丢掉有序 KV 直通（list 退化为 SQL 范围查询）。若未来放弃 B/C 类路线，这是兜底 |

选 A。判据：唯一"官方 + 纯 C++ + 可 submodule"的组合；其缺口
（§2.2）集中在 2PC 层一处，工作量可控且有明确的上游回馈路径。

### 2.2 client-c 现状清单（源码核实，@78a557e）

| 面 | 现状 | 对本方案的影响 |
| --- | --- | --- |
| 集群接入 | `pingcap::kv::Cluster(pd_addrs, ClusterConfig)`；ClusterConfig 内建 TLS（ca/cert/key 三件套） | 直接可用；TLS 首期即可透传（对比 hiredis 需额外组件） |
| 读 | `Snapshot(cluster)` 取 TSO 定版本；`Get(key)` / `Scan(begin, end)`（`Scanner` 每批 256、region 边界自动衔接）；自动 resolve 残留锁 | 直接可用。缺口：`Get` 丢弃了 `not_found` 标志、返回空串——本方案靠 codec 值恒非空消歧（§3.1），并列为上游修复项 |
| 写 | `Txn`（buffer 缓冲 + `commit()` 走 `TwoPhaseCommitter`）。**mutation 仅 key→value，op 恒为 Put**：无 Del/Insert/Lock（上游 issue #82 "delete key" 长期开放）；async commit 仅测试注入启用；`commit()` 异常路径自注 TODO | **不可直接用**。必须扩展 op 支持（§6.3）；kvproto 协议侧 `Op` 枚举完备（Put/Del/Lock/Rollback/Insert/PessimisticLock/CheckNotExists），缺口只在客户端封装 |
| 悲观事务 | 无 | 不需要（§4 全走乐观 + 冲突重试） |
| PD | getTS（TSO）、region 路由、`getGCSafePoint`（只读） | 够用；**无推进 safepoint 的封装**（§7.3 运维接管） |
| 线程模型 | 全同步阻塞（grpc 同步 stub）；`Cluster` 自带后台线程（TSO oracle 2s 刷新、region cache、MPPProber） | 同步阻塞恰合 IMetaStore 池线程契约（主文档 §2.2 预判"TiKV 客户端"属此列）。MPPProber 是 TiFlash MPP 探活，对纯 KV 是死重但无害，列为可选上游开关 |
| 错误模型 | `pingcap::Exception : Poco::Exception` + ErrorCodes 枚举；region/lock 类错误在库内 Backoffer 重试，超预算才抛出 | 映射规则见 §6.4 |
| 构建 | CMake ≥3.10、C++17，导出 `kv_client` 静态库；**无 install/export 规则、无 tag/release**——add_subdirectory 是唯一消费方式（TiFlash 即如此，vendor 在 contrib/）；依赖 gRPC + protobuf（构建期需 protoc + grpc_cpp_plugin 生成 kvproto）、Poco（Foundation/Net/JSON/Util，`Poco::Logger` 在公共头，是硬依赖）、abseil；自带 submodule：kvproto / libfiu（恒编译进 kv_client）/ abseil-cpp（系统缺失时兜底）/ googletest（仅测试） | 依赖面显著重于 hiredis/sqlite，接入策略见 §8 |

## 3. 数据模型

### 3.1 总原则

1. **value 编码 100% 复用 `codec.cc`**——TiKV key/value 均二进制安全。
   附加收益：codec 值**恒非空**（首字节为版本号），而 client-c 的
   `Snapshot::Get` 以空串表示 not found（丢弃了 kvrpcpb 的 not_found
   标志）——值恒非空使"空串 = 不存在"无歧义，缺口被格式不变量吸收；
2. **key = 可配置前缀（默认 `duo:`）+ 单字符表标签 + `\0` 分隔复合段**，
   分隔符合法性与主文档 §4.1 同一论证。前缀作用同 Redis 版：多实例/
   多套测试共享一个集群互不污染（未来切 API V2 keyspace 时前缀退役）；
3. **一表一前缀，回归 RocksDB 的 CF 心智**。TiKV 全局有序，
   `O<bucket>\0<key>` 的字节序即 S3 字典序，list 无需二级索引——对比
   Redis 版被迫的 HASH+ZSET 双键结构，这里单键即可（§3.3）。

### 3.2 key 布局

与主文档 §4.1 的 CF 表逐行对照（下表 key 均略去 `duo:` 前缀）：

表标签恒为**单字符**（守卫表用小写对应字母）——两字符标签（如 `Bg`）
会与桶名首字符歧义（桶名含 `[a-z]`，`B` + `g...` 撞 `Bg` + `...`）：

| RocksDB CF | TiKV key | value | 说明 |
| --- | --- | --- | --- |
| `default` | `s` | `"t1"` | schema 谱系标识，打开时读出校验，缺失则事务内 Insert（§4.4） |
| `buckets` | `B<bucket>` | `encode_bucket` | create 用 Op::Insert 表达"必须不存在"（§4.4） |
| —（新增） | `b<bucket>\0<u8 shard>` | 无值（Op::Lock 占位） | **桶守卫分片**，shard ∈ [0,16)：物化 put/create_upload 与 delete_bucket 的写偏斜冲突（§4.3） |
| `objects` | `O<bucket>\0<key>` | `encode_object` | 字节序 = S3 字典序，直接支撑 list（§3.3） |
| `uploads` | `U<bucket>\0<key>\0<id>` | `encode_upload` | 前缀扫即按 (key, id) 排序，与 RocksDB 同构 |
| —（新增） | `u<bucket>\0<key>\0<id>\0<u8 shard>` | 无值（Op::Lock 占位） | **上传守卫分片**：物化 put_part 与 complete/abort 的写偏斜冲突（§4.3） |
| `parts` | `P<bucket>\0<key>\0<id>\0<be16 part_no>` | `encode_part` | big-endian part_no 保证升序 |
| `refs` | `R<be64 file_id>` | owner 简述 | `chunk_referenced` = 单 key Get |
| `gcq` | `G<be64 seq>` | `encode_reclaim` | `peek_reclaims` = 前缀 Scan limit max；seq 即 key 序 |
| `stats` 计数器 | `C<kind>`（kind ∈ {`0`,`1`,`q`}） | 8B 小端 i64（复用 codec 计数器格式） | 号段预留（§5） |
| `stats` pack 账 | `S<be64 pack_id>` | 计数结构 | 随 pack 聚合（P2）引入；当前 `pack_stats()` 返回空，与其余实现一致。预警：事务内读改写 pack 账会使同 active-pack 的小对象 PUT 互相冲突，P2 接入时的演进是 delta 行 + 后台折叠，此处仅立此存照 |

### 3.3 list_objects：Snapshot + Scanner，算法照搬 RocksDB 版

RocksDB 版（主文档 §4.4）的迭代结构原样成立，迭代原语换成
`Snapshot::Scan`：

- 打开一个 `Snapshot`（TSO 定版本）作为整次 list 的一致视图——**跨
  Scanner 重建全程有效**，这是 MVCC 的直接红利：Redis 版要靠"整个循环
  塞进一个 Lua 脚本"才买到的单次调用一致性（duostore-redis-meta.md
  §2.3），这里免费；
- seek 起点 = `max(prefix, start_after 的后继)`；delimiter 命中归组后
  **组末字节 +1 构造后继 seek 点**，在同一 Snapshot 上新建 Scanner 跳过
  整组——每组一次 gRPC 往返（LAN 亚毫秒级）。Redis 版否决"客户端多轮
  驱动"的两条理由（RTT 放大 + 跨轮无一致视图）在这里只剩前者，且量级
  与 RocksDB 版的每组一次 Seek 同构，可接受；不走 coprocessor 下推
  ——那是 SQL/TiFlash 的领域，为 list 引入执行框架不成比例；
- 多取一条判 `is_truncated`；截断恰落在 delimiter 组上时 next_token 须
  取组内最后一条 key（组已被跳过、未逐条经过）：反向扫描原语 client-c
  未封装，改为**延迟前向扫描**——仅在真正截断且末项是组时，对该组做一次
  分页前向扫取尾 key。每次 list 至多触发一次，组大小通常远小于桶；这是
  与 RocksDB 版（SeekForPrev）唯一的实现差异；
- 版本可见性约束：Snapshot 早于 GC safepoint 会被拒（§7.3）；list 单
  调用秒级完成，距 safepoint 水位（分钟-小时级）余量充足，声明即可。

## 4. 事务与不变量

### 4.1 路线：乐观 2PC + 冲突重试，不引入悲观锁

`IMetaStore` 的提交类方法都是"读-校验-写"：映射为**在 start_ts 的
Snapshot 上读、在 C++ 侧算、组 mutation 批、一次 2PC 提交**。TiKV
prewrite 对写集内每个 key 检查"是否存在 commit_ts > start_ts 的写记录
或他人锁"——即**凡是本事务要写的 key，"读到的值未被并发修改"由协议
免费保证**（WriteConflict / KeyIsLocked → 重试）。对比：

- Redis 版要把"读到的原始字节"作为前置条件传给 Lua 脚本逐一比对
  （duostore-redis-meta.md §3.2）；TiKV 版对**写集内**的 key 天然 CAS，
  前置条件绝大部分消失；
- 例外是"只读不写"的前置条件（bucket 存在性、delete_bucket 空检查、
  put_part 的 upload 存在性）——乐观 2PC 不校验只读键，构成写偏斜
  （write-skew）窗口，用守卫键物化（§4.3）。

重试循环与 Redis 版 §3.2 同构：WriteConflict / KeyIsLocked（库内
backoff 超预算后抛出）→ 取新 start_ts、重读、重组批、重提交；指数退避
100µs 起、上限 6.4ms、最多 16 次，超限抛 `InternalError`（病态热点，
响亮失败优于活锁）。

### 4.2 悲观事务否决理由

client-c 无悲观封装（kvproto 协议有）；补齐它 = 引入锁生命周期管理
（TTL 续期、死锁检测交互），工作量数倍于 §6.3 的 op 扩展。而本负载的
冲突面经守卫分片摊薄后极窄（§4.3），乐观重试的均摊代价可忽略。若未来
出现病态热点（同 key 高频覆盖写），升级路径才是悲观锁——列为演进，
不进首期。

### 4.3 写偏斜的物化：守卫分片（guard shards）

问题定式：事务 T1 读 key X 校验（不写 X），事务 T2 写 X——乐观 2PC
不检测 T1 的读集，两者并发提交则 T1 的校验失效。本接口共三处：

| 只读校验方 | 并发写方 | 后果（若不物化） |
| --- | --- | --- |
| put_object / create_upload 读 `B<bucket>` 校验桶存在 | delete_bucket 删 `B<bucket>` | 桶已删而对象/上传写入成功：refs 永久泄漏、重建桶复活幽灵（主文档 §4.5 明确要防） |
| delete_bucket 扫 `O<b>` / `U<b>` 空检查 | put_object 新建对象 | 同上（对称面） |
| put_part 读 `U<...>` 校验上传存在 | complete/abort 删 `U<...>` | 孤儿 part 行 + refs 泄漏 |

**解法：让"只读方"也写一个双方共同的 key，把读写冲突物化为写写冲突。**
直接写 `B<bucket>` 本身会让同桶所有 PUT 互相冲突（热点），故引入
**守卫分片**：

- put_object / create_upload：对 `b<bucket>\0<hash(key) % 16>` 附加一个
  **Op::Lock** mutation（占位锁记录，不写值）；delete_bucket：写
  `B<bucket>`（Del）+ 全部 16 个 `b` 分片（Op::Lock）。任何 put 与
  delete_bucket 并发必撞分片；两个 put 相撞概率 1/16，重试廉价；
- put_part：对 `u<...>\0<part_no % 16>` 附加 Op::Lock；complete /
  abort：写 `U<...>`（Del）+ 全部 16 个 `u` 分片。同上传并发传不同号
  分片互不阻塞（S3 客户端并行传分片是常态，不可串行化）；
- **complete_upload / put_part(同号重传) / swap_extents / delete_object
  无需桶守卫**：它们的前置对象（upload 行、object 行）在对方快照里
  可见——delete_bucket 的空检查扫得到 `U`/`O` 行即拒绝，冲突由既有
  数据行天然物化，无需额外 key。

Op::Lock 的优点：不产生 value、不占存储，只在 write 列留冲突记录（TiDB
`LOCK KEYS` 同机制）。**T1 验证项**：Lock 型写记录被后续 prewrite 冲突
检测计入的语义需冒烟确认（TiKV 版本间对 Lock/Rollback 记录的冲突判定有
过调整）；若语义不符，回退方案 = Op::Put 写回原值（等价物化，仅多版本
垃圾），接口不变。

### 4.4 逐方法流水

对照 Redis 版 §3.3（省略纯读与号段方法；"锁" = Op::Lock 守卫）：

| 操作 | 读集（start_ts 快照） | mutation 批 |
| --- | --- | --- |
| create_bucket | — | **Insert** `B`（AlreadyExist 错误 → BucketAlreadyOwnedByYou，协议级表达"必须不存在"） |
| delete_bucket | `B` 存在；`O<b>` / `U<b>` 前缀扫空检查 | Del `B` + Lock 全部 `b` 分片 |
| put_object | `B` 存在；旧 `O`（算 version 与 gcq 账） | Put `O`(version+1) + Lock `b` 分片 + Put 新 `R` + Del 旧 `R` + Put `G`(旧 DataRef) |
| delete_object | 旧 `O`（absent → 返回 false，不发事务） | Del `O` + Put `G` + Del `R` |
| create_upload | `B` 存在 | Put `U` + Lock `b` 分片 |
| put_part | `U` 存在；旧 `P`（同号重传账） | Put `P` + Lock `u` 分片 + refs/gcq 同 put_object |
| complete_upload | `U`；全部 `P` 前缀扫；旧同名 `O` | Put `O` + Del `U` + Del 全部 `P` + Lock 全部 `u` 分片 + 未选中分片入 `G` + 旧对象入 `G` + refs 转移 |
| abort_upload | `U`；全部 `P` | Del `U` + Del 全部 `P` + Lock 全部 `u` 分片 + 全部分片入 `G` + Del `R` |
| swap_extents | `O`（C++ 解码校验 expect_version 与 from，不符 → 返回 false） | Put `O`(version+1, DataRef=to) |
| ack_reclaim(s) | — | Del `G<seq>`（可多 seq 同批——`ack_reclaims` 覆写为单事务，接口默认逐条的升级点） |

读集里被本事务**写**的 key（旧 `O`、`U`、`P`）由 prewrite 天然校验；
读集里只读的 key 全部落在上表的守卫/Insert 机制内——两者并集覆盖
Redis 版 §3.3 的全部前置 check，无遗漏（complete 的 parts sha1 指纹
不再需要：全部 `P` 都被 Del，即全部在写集内受 CAS 保护）。

### 4.5 多网关下的原子性论证

RocksDB 版靠进程内 mutex，Redis 版靠服务端 Lua 单线程；TiKV 版的对应物
是 **Percolator 2PC 本身**——原子性由协议对任意客户端、任意进程成立，
且事务参与 key 可跨 region/跨节点（primary key 定提交点，secondary 异步
落地，读者遇 secondary 残留锁经 LockResolver 回查 primary，client-c
读路径已内建此逻辑且是 TiFlash 生产在用的部分）。TikvMetaStore
**不持业务互斥**（仅号段派发内存小锁，§5），与 Redis 版同型。

### 4.6 盲重试禁令与结果不明

规则与 Redis 版 §3.5 逐字继承：提交请求发出后连接/超时故障 = 结果不明
——**一律抛 `InternalError`，决不盲重放**（重放会把刚写入的 DataRef 记
入 gcq，GC 回收在引数据）。TiKV 版的特有细节：

- 2PC 的提交点 = primary key 的 commit 落地。client-c 在 commit 阶段
  异常时的行为自注 TODO（`2pc.cc` "TODO: Process commit exception"）
  ——T1 扩展时必须把"primary commit 后 secondary 失败 = 已提交成功"
  与"primary commit 结果不明 = InternalError"两条路径显式化（§6.3）；
- 演进（不进首期）：结果不明时可用 `CheckTxnStatus`（kvproto 已有，
  client-c LockResolver 内部在用）回查 primary 消歧，把一部分
  InternalError 变成确定答案；
- CAS/WriteConflict 重试不在此列——冲突是明确结果，安全（同 Redis 版）。

## 5. alloc_file_id：计数器 RMW 号段

`C<kind>` 计数器 key 上的读改写小事务：读旧值 → Put 旧值+4096 → 提交；
WriteConflict → 重试（多网关抢号段，胜者拿走区间）。`IdRange` 结构、
独立 `alloc_mu_`、`kIdSegment = 4096` 照搬 RocksDB 版（主文档 §4.5）。
seq 同法走 `C<seq>` 预派发，gcq 入账保持纯写。

Redis 版 §4 的"崩溃回滚重发已用 id"三层缓解**在此不需要**：TiKV 提交
即 raft 多数派持久，无 everysec 类丢失窗口；号段跨网关全局唯一由事务
保证。保留兜底第 3 层（数据面 `O_EXCL` 撞文件响亮报错）——它在数据面，
本就恒在。

## 6. client-c 接入与必要扩展

### 6.1 生命周期与线程模型

- 每 store 一个 `pingcap::kv::Cluster`（内含 grpc channel 池、region
  cache、TSO oracle 后台线程）；同步阻塞调用在池线程发生，恰是主文档
  §2.2 为 IMetaStore 选同步接口时点名预判的模式。`Cluster` 方法线程
  安全（TiFlash 多线程共用单 Cluster 是其设计用法），**无需连接池**
  ——对比 hiredis 的自建池，这层复杂度消失；
- `close()`：置关闭标志后析构 Cluster（其析构已依次 stop rpc/prober/
  region cache/线程池）；之后任何调用干净地抛 `InternalError`（防御
  纵深惯例）。析构在无在途调用后进行——由 DuoStoreBackend 的 close
  顺序保证（主文档 §9 生命周期）；
- 超时：client-c 无全局"单调用超时"参数，各操作的 Backoffer 预算是
  库内常量（如 GetMaxBackoff/prewriteMaxBackoff）。首期接受库默认；
  外层兜底是 S3 请求超时。参数化列为上游改进项（T5）。

### 6.2 Poco 依赖的处置

`Poco::Logger`/`Poco::Exception` 出现在 client-c 公共头，Poco
Foundation 是硬依赖（find_poco 还要求 Net/JSON/Util）。不做剥离
（改动面深入上游全部源文件），接受依赖；日志桥接：Poco 默认输出与
spdlog 各行其道，T5 可配置 Poco root logger 的 channel 收敛到统一
stderr 格式——纯运维项，不阻塞功能。

### 6.3 2PC 扩展：in-tree 侧车（已实现，`tikv_client.{h,cc}`）

原计划 fork（`locallocal/client-c`）后在 fork 上扩展；实施时改为
**in-tree 侧车**：`src/storage/duostore/tikv_client.cc` 以 client-c 公开
的传输基建（`Cluster` / `RegionCache` / `RegionClient` / `Backoffer` /
`LockResolver`）为地基，改写其 `kv/2pc.cc`（Apache-2.0，来源注明于文件
头）实现自有提交器。改判理由：fork 指针会让 submodule 指向不可复现的
非上游 commit，且 fork 的维护/同步成本落在本仓；侧车让 submodule 保持
pristine 上游锁定，升级 = 换指针 + 回归。上游 PR 仍列为 T5 事项，合入
后侧车退役。侧车相对上游 2pc.cc 的差异（文件头逐条注明）：

1. **mutation 带 op**（Put / Del / Lock / Insert，§4.4）：prewrite 组包
   `mut->set_op(...)`，kvproto 协议侧本就完备；
2. **结构化错误**：`already_exist` → `TikvAlreadyExist`（create_bucket
   转 `BucketAlreadyOwnedByYou`）、`write_conflict`/`retryable` →
   `TikvConflict`（重试循环消费）——上游笼统 LogicalError/Unknown。
   含一处对上游裸异常的补分类：`resolveLocksForWrite` 遇"更新事务的
   活锁"抛 `Exception("write conflict")`（上游 TODO 未给错误码），
   prewrite 阶段明确未提交，按消息归入 `TikvConflict`（守卫分片专项
   `duostore_tikv_write_skew_guard` 在真实集群上暴露此路径）；
3. **commit 异常路径显式化**（§4.6 两分支）：primary 明确拒绝 = 已回滚
   （TiKV 对已提交事务的 commit 幂等返回 ok）→ 安全重试；RPC 层异常 =
   `TikvUndetermined` → 上抛 InternalError。替换上游 TODO；
4. **prewrite 失败后 best-effort `BatchRollback` 清锁**（上游 TODO）：
   缩短并发方在残锁上的等待；清不掉无害（TTL/读者 LockResolver 收敛）；
5. **commit 重试不换 commit_ts**：同 (start_ts, commit_ts) 的 commit
   幂等可重放；上游区域重试会重取 TSO，引入不必要的语义分叉。

Get 的 not_found 重载未做（codec 值恒非空已消歧，§3.1），与
`Snapshot`/`Scanner` 一样直接复用上游。

明确不做：pessimistic（§4.2）、async commit、TTLManager 加固（首期
事务 mutation 数 ≤ 数十 + 16 守卫，单批远低于 `txnCommitBatchSize =
16KiB` 键量级，无长事务）。**大 complete 例外**：万级分片的
complete_upload 事务含数万 mutation，prewrite 耗时可能越过默认
lock_ttl——T4 验收项含万分片 complete 专项，超时则接入 client-c 已有
的 `TTLManager` 心跳（机制在库内现成，只是 Txn 未接线）。

### 6.4 错误映射

统一经 `throw_tikv(what, e)`（仿 `throw_status`/`throw_reply`：
LOG_ERROR + 抛 `s3::S3Error`）：

| 来源 | 处理 |
| --- | --- |
| WriteConflict / KeyIsLocked（重试预算内） | 库内 backoff，对上不可见 |
| 同上（超预算抛出） | §4.1 重试循环；16 次超限 → `InternalError` |
| AlreadyExist（Op::Insert） | `BucketAlreadyOwnedByYou`（唯一使用点） |
| RegionUnavailable / 网络 / TimeoutError | 纯读 → 重试一次后 `InternalError`；提交类结果不明 → `InternalError`（§4.6 禁令） |
| 语义性缺失（Get 空串） | 不是错误——语义层转 `NoSuchKey` / `NoSuchBucket` / `NoSuchUpload`，与各实现同构 |
| 其余 `pingcap::Exception` | `InternalError`(500) + 错误码/displayText 入日志 |

## 7. 持久化、一致性与 MVCC GC

### 7.1 持久化声明

提交 = raft 多数派落盘（TiKV 5.0 起 sync-log 恒开，不可关）。对照主
文档 §6.3：`meta_sync` 配置对本实现**无意义（恒等效 true）**，设置时
打 WARN 忽略——四种实现里持久化语义最强的一档，Redis 版 §6 的 AOF
论证矩阵在此整体退役。

### 7.2 集群侧故障

- 少数副本故障：透明（raft 重选主 + client region cache 刷新重试）；
- 多数派故障 / PD 全挂：meta 不可用，S3 层表现为 5xx——不产生不一致，
  duostore"数据先落、meta 后提交"的崩溃矩阵（主文档 §6.2）按"提交未
  完成"分支原样成立；
- TSO 时钟：单调性由 PD 保证，网关本地时钟不参与提交序——多网关下
  无时钟偏斜问题。

### 7.3 MVCC GC safepoint：无 TiDB 部署的运维责任

TiKV 的多版本垃圾靠 GC safepoint 推进回收；**标准部署里推进者是
TiDB**。本方案不引入 TiDB（§1），则：

| 部署形态 | safepoint 策略 |
| --- | --- |
| 集群同时服务 TiDB（共用） | 无事可做，TiDB 照常推进 |
| 纯 KV 集群 + 长期运行 | **必须自行推进**：周期调用 PD 的 UpdateServiceGCSafePoint（kvproto 有、client-c 未封装——上游扩展或 pd-ctl/HTTP API 旁路脚本）。推进为 now − 保留窗口（如 10 分钟，只需覆盖最长 list/事务时长） |
| 测试/短期集群 | 不推进无碍（垃圾积累但正确性无损） |

首期（T1-T4）按测试形态处理；**T5 交付推进方案**（优先在 fork 的
pd::Client 上加 updateServiceGCSafePoint——与 §6.3 同一 fork 流程）。
不推进的后果是空间放大与 scan 变慢，不是正确性问题，故可后置。

## 8. 构建接入

### 8.1 submodule（已完成）与初始化策略

`.gitmodules` 已增：

```text
[submodule "third_party/client-c"]
    path = third_party/client-c
    url = https://github.com/tikv/client-c.git
```

不设 shallow（本体 <1MiB；含 kvproto 检出共 ~9MiB）。上游无 tag，按
commit 锁定（当前 `78a557e`）。client-c 自带 4 个 submodule，只需其 2：
kvproto（协议，必需）、libfiu（恒编译进 kv_client，必需）。abseil-cpp
不拉——统一用系统/解包 absl（与 grpc 所链同一 ABI，避免双份共存）；
googletest 不拉（ENABLE_TESTS 恒 OFF，上游 #104 ON 已知构建断裂）。

`build.sh`：**不进 `LIGHT_MODULES`**——与 rocksdb/hiredis"零系统级
依赖、始终 init"相反，本件需要系统级 gRPC/Poco 工具链（§8.2），走
seastar 式惰性拉取：新增 `--tikv` 开关时

```bash
git submodule update --init third_party/client-c
git -C third_party/client-c submodule update --init \
    third_party/kvproto third_party/libfiu
```

并追加 `-DLIGHTS3_DUOSTORE_TIKV_META=ON`（粘性语义同 `--seastar`，建议
配 `-B build-tikv` 与常规构建隔离）。

### 8.2 依赖矩阵与无 sudo 环境路线

| 依赖 | 用途 | 有 sudo（部署/CI 机） | 本开发机（无 sudo） |
| --- | --- | --- | --- |
| gRPC（库 + `grpc_cpp_plugin`） | kvproto codegen + 传输 | `libgrpc++-dev protobuf-compiler-grpc` | dpkg -x 提取（下） |
| protobuf（库 + `protoc`） | 同上 | `libprotobuf-dev protobuf-compiler` | 同 |
| Poco Foundation/Net/JSON/Util | client-c 公共头/日志 | `libpoco-dev` | 同 |
| abseil | grpc/kvproto | `libabsl-dev`（或走 client-c 内建 submodule） | 同 |

本机现状（已核实并落地）：grpc/protobuf/Poco/protoc/grpc_cpp_plugin
系统层面全缺。路线沿用 librados 先例：**apt-get download + dpkg -x 整链
提取到 `~/.local/opt/tikv-deps`**（23 个 deb：libgrpc++-dev/libgrpc-dev
及 runtime、protobuf-compiler[-grpc]/libprotobuf-dev/libprotoc-dev 及
runtime、libabsl-dev/libabsl20260107、libpoco-dev 及 Foundation/Net/
JSON/Util/XML runtime、libre2/libc-ares；openssl/zlib 系统已有）。
CMake 侧不走 `CMAKE_PREFIX_PATH` 泛探测，而是 **`LIGHTS3_TIKV_DEPS_ROOT`
指向解包前缀后逐变量预置**（Protobuf_*/gRPC_*/Poco_*_LIBRARY——client-c
的 find 均带 `if(NOT ...)` 守卫，父作用域预置即整体短路）；解包的
protoc / grpc_cpp_plugin 运行需同树 so，configure 期生成 LD_LIBRARY_PATH
wrapper 供 kvproto codegen 调用。`~/.local/opt/tikv-deps/usr` 存在时
自动探测，无需手工传参。专用构建目录 **`build-tikv`**（惯例同
build-redis / build-rados）。运行期链接：全树一个 libdir，
`--disable-new-dtags` + rpath/rpath-link（RUNPATH 不传递到依赖树深处，
librados 同款论证）。

版本一致性约束（上游未文档化，靠我方纪律保证）：kvproto 生成代码必须与
链接的 protobuf runtime 同版（经典单 protobuf per binary 约束），protoc /
grpc_cpp_plugin / 库三者同源同版——同一发行版快照的 deb 链天然满足；
本仓无其他 protobuf 使用者，无第二冲突源。client-c 唯一被完整测试过的
配置是 TiFlash 的 vendored 工具链，我方组合属未测领土，T1 冒烟即为此
钉桩。

否决的备选：gRPC 以 submodule 源码构建——克隆与构建体量巨大（分钟级
变十分钟级 clean build），且 protoc 自举后仍要处理宿主/目标一致性；
依赖自洽的收益撑不起这个成本，dpkg -x 已验证可行（librados 先例）。

### 8.3 CMake 预设（仿 hiredis 模板）

新增 option **`LIGHTS3_DUOSTORE_TIKV_META`，默认 OFF**（依赖
`LIGHTS3_DUOSTORE`）——同 redis/sqlite 的理由：测试需外部集群在场
（§10），且依赖面重，不进日常构建。

实现落在顶层 `CMakeLists.txt` 的 `LIGHTS3_DUOSTORE_TIKV_META` 块（依赖
预置 + wrapper 生成 + `add_subdirectory(third_party/client-c
EXCLUDE_FROM_ALL SYSTEM)`），target 接线：

```cmake
target_sources(lights3_core PRIVATE
  src/storage/duostore/tikv_client.cc
  src/storage/duostore/tikv_meta_store.cc)
target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_TIKV_META)
target_link_libraries(lights3_core PRIVATE kv_client)
```

要点：

- `kv_client` 的 include 目录是 PUBLIC 传递的（pingcap 头 → grpc/
  kvproto 生成头），我方 `.cc` 直接 include `<pingcap/kv/...>` 即可；
  Poco include 与 Net/JSON/Util 链接是**上游欠传/欠链**（其源码实际用
  到，TiFlash 环境里恰好全局可见），由我方在 add_subdirectory 后补齐；
- client-c 顶层 `set(CMAKE_CXX_STANDARD 17)` / `-Wno-narrowing` 只作用
  于其子目录作用域，不污染全仓 C++20 标志；同工具链下 C++17 静态库链入
  C++20 目标 ABI 无虞（rocksdb 同款先例）；kv_client/kvproto/fiu 追加
  `-w`（第三方源码不受 -Wall -Wextra 约束，sqlite 同款惯例）；
- kvproto codegen 在 client-c 内部以 custom command 接好（
  `Protobuf_PROTOC_EXECUTABLE` / `gRPC_CPP_PLUGIN` 即我方预置的
  wrapper），无需额外步骤；
- OFF 时配置 `meta: tikv` 在 `from_params` 抛 "not compiled in"（惯例）。

### 8.4 组件关系与复用

- 新文件仅 `tikv_meta_store.{h,cc}`；`DuoStoreBackend` 构造分支加
  `#ifdef LIGHTS3_DUOSTORE_TIKV_META` 一段（`DuoMetaKind::kTikv`），
  数据面/GC/S3 层零改动——语义级接口承诺的第四次兑现；
- 复用：`codec.{h,cc}` 全部 value 编解码（§3.1）、`be64_key`/`part_key`
  的 be 编码 helper（key 布局与 RocksDB 同构，复用面大于 Redis 版）、
  `storage/validate.cc`、`storage/multipart.h`、meta 测试套件
  `tests/unit/meta_store_suite.h`。

## 9. 配置

`DuoStoreConfig::from_params` 新增键（YAML 标量惯例同主文档 §11）：

```yaml
backends:
  - name: duodata
    type: duostore
    root: ./data/duostore
    meta: tikv                          # rocksdb / redis / sqlite / tikv
    pd_endpoints: "10.0.0.1:2379,10.0.0.2:2379,10.0.0.3:2379"
    tikv_prefix: "duo:"
    # tikv_ca / tikv_cert / tikv_key: TLS 三件套（可选，透传 ClusterConfig）
    # 其余 duostore 键（chunk_size / pack_* / gc_* / mpu_ttl …）不变
```

| 键 | 默认 | 说明 |
| --- | --- | --- |
| meta | `rocksdb` | 增合法值 `tikv`；未编译时选 tikv → 配置错误 |
| pd_endpoints | —（meta=tikv 时必填） | 逗号分隔 PD 地址列表 |
| tikv_prefix | `duo:` | 全部 key 前缀（多实例/测试隔离，§3.1） |
| tikv_ca / tikv_cert / tikv_key | 空（明文） | mTLS 证书路径，三者同时给定才启用 |

`meta: tikv` 时 `meta_path` / `rocksdb_block_cache` / `meta_sync` 忽略
并打 WARN（§7.1）。

## 10. 测试策略

1. **套件复用**：`meta_store_suite` factory 注册 TikvMetaStore 条件跑
   ——与 sqlite/redis 同一语义基线全量继承；组合面注入构造
   `DuoStoreBackend(cfg, pool, TikvMetaStore, FsDataStore)` 跑
   `run_backend_suite`；`run_e2e.sh` 增 `duostore-tikv` 分支；
2. **真实集群的获取**：环境变量 `LIGHTS3_TEST_PD_ADDR` 指向外部集群
   （tiup playground / 既有测试集群），**缺则显式 SKIP**——librados
   先例（本机无集群恒 SKIP）；隔离靠每用例唯一 `tikv_prefix`
   （pid+计数器，同 Redis 策略）。不做进程内 mock：client-c 的
   MockPDClient 只 mock PD，纯内存假 TiKV 无处买，且本方案核心
   （2PC 冲突语义）mock 测不到；
3. **TiKV 专项**：§4.3 守卫物化——双 store 并发 put_object vs
   delete_bucket / put_part vs abort，验证必有一方冲突重试或拒绝，无
   幽灵残留；Op::Lock 冲突语义冒烟（T1 验证项）；WriteConflict 重试
   收敛与 16 次上限路径；`splitRegion` 测试钩子制造多 region 后跑
   list 分页与跨 region 事务（2PC primary/secondary 分属不同 region）；
   万分片 complete_upload 的 prewrite 时长与 lock_ttl 余量（§6.3）；
   残留锁恢复——事务提交中途 kill 网关进程，另一网关读同 key 经
   LockResolver 正常解锁推进；
4. **上游锁定**：fork 指针变更（§6.3）经 submodule commit 锁定，CI
   不追 master——升级 = 显式换指针 + 全套件回归。

## 11. 实施拆分

| 阶段 | 内容 | 可独立验收 | 状态 |
| --- | --- | --- | --- |
| T0 | 调研 + 本文档 + `third_party/client-c` submodule 引入（锁定 78a557e） | 文档评审 | **已完成** |
| T1 | 依赖矩阵落地（dpkg -x 链 + build-tikv）；CMake option + build.sh `--tikv`；带 op 的 2PC 侧车（Put/Del/Lock/Insert + commit 异常路径 + 清锁，§6.3） | 全量构建绿；集群冒烟（含 Op::Lock 守卫语义专项 `duostore_tikv_write_skew_guard`） | **已完成** |
| T2 | `TikvMetaStore` 骨架：错误映射 / 重试循环 / close 守卫；`C<kind>` 号段 alloc_file_id；bucket 四方法（含 Insert 语义）+ schema 校验；meta 套件接线 + PD 探测/SKIP 机制 | 集群在场时用例绿；无集群 SKIP 路径绿 | **已完成** |
| T3 | object 四方法（list = Snapshot+Scanner，§3.3）+ 守卫分片 + refs / gcq / swap_extents / chunk_referenced / peek_reclaims / ack_reclaims 批量覆写 | meta 套件全绿 + 写偏斜/冲突专项 | **已完成** |
| T4 | multipart 全套（含 `u` 守卫）；注入组合 `run_backend_suite`；`e2e_duostore_tikv` | 后端一致性套件 + e2e 绿 | **已完成** |
| T5 | 打磨：GC safepoint 推进方案（§7.3）、Poco 日志收敛、超时参数化、指标（冲突重试计数）、万分片 complete 专项与 TTLManager 评估（§6.3）、上游 PR（op 扩展回馈）与指针升级 | 全 ctest 矩阵（含 skip 路径）绿 | 未开始 |
