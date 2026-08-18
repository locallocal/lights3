# TikvMetaStore：DuoStore 元数据的 TiKV 实现

本文是 `TikvMetaStore` 与其 2PC 侧车 `TikvClient` 的实现级文档，展开
[../duostore-tikv-meta.md](../duostore-tikv-meta.md)（下称"设计文档"）的方案在代码中的
具体落法——client-c 选型调研、构建/依赖矩阵、配置与测试策略见设计文档，本文只讲
代码层。DuoStoreBackend 主体见 [./duostore-core.md](./duostore-core.md)；姊妹实现见
[./duostore-meta-rocksdb.md](./duostore-meta-rocksdb.md)、
[./duostore-meta-sqlite.md](./duostore-meta-sqlite.md) 与
[./duostore-meta-redis.md](./duostore-meta-redis.md)。涉及文件：

| 文件 | 内容 |
| --- | --- |
| `src/storage/duostore/tikv_meta_store.h/.cc` | `IMetaStore` 的 TiKV 实现（编译开关 `LIGHTS3_DUOSTORE_TIKV_META`） |
| `src/storage/duostore/tikv_client.h/.cc` | 侧车客户端：乐观 2PC 提交器（带 op）、batch_get / last_key 读原语、GC safepoint 直连 PD RPC；pingcap 头（拖 grpc/Poco）全部封在 `.cc` |
| `src/storage/duostore/meta_store.h` | `IMetaStore` SPI（同步契约、`UndeterminedCommit`） |
| `src/storage/duostore/codec.h/.cc` | value 编解码 100% 复用（四实现字节级同格式） |
| `src/storage/duostore/meta_util.h` | 共享纯计算 helper（`check_put_condition` / `assemble_completed_object` / `refs_delta` 等） |

侧车存在的理由（`tikv_client.h` 头注释）：client-c 的事务提交层是 test-grade
（mutation 仅 Put、commit 异常路径留 TODO），故基于其**公开传输基建**
（`Cluster`/`RegionCache`/`RegionClient`/`Backoffer`/`LockResolver`）自建提交器，
不 fork submodule，上游补齐后侧车退役。

## 1. Key 编码

key = 前缀（`TikvMetaOptions::prefix`，默认 `duo:`）+ **单字符表标签** + codec 复合段
（`tikv_meta_store.cc:TikvMetaStore::tkey`）。TiKV 全局有序，`O` 表字节序即 S3
字典序，list 无需二级索引。构造符号与布局：

| 标签 | 构造符号 | key 形态 | value |
| --- | --- | --- | --- |
| `s` | 构造函数内 `tkey('s', {})` | schema 标记 | `"t1"`（谱系 `"t"` + `kSchemaCurrent`） |
| `B` | `bucket_key` | `B<bucket>` | `codec::encode_bucket` |
| `b` | `bucket_guard` | `b<bucket>\0<u8 shard>` | 无值（Op::Lock 守卫分片，§4） |
| `O` | `object_key`（内部 `codec::object_key`） | `O<b>\0<key>` | `codec::encode_object` |
| `U` | `upload_key` | `U<b>\0<key>\0<id>` | `codec::encode_upload` |
| `u` | `upload_guard` | `u<b>\0<key>\0<id>\0<u8 shard>` | 无值（Op::Lock） |
| `P` | `part_key` | `P<b>\0<key>\0<id>\0<be16 no>` | `codec::encode_part`（be16 尾缀天然升序） |
| `R` | `refs_key` | `R<be64 file_id>` | owner 简述 |
| `G` | `gcq_key` | `G<be64 seq>` | `codec::encode_reclaim` |
| `C` | `counter_key` | `C<kind>`，kind ∈ {`0` chunk, `1` pack, `q` seq, `d` pack-delta} | 8B 小端 i64（codec 计数器格式） |
| `S` | `pack_delta_key` / `pack_seal_key` | `S<be64 id>d<be64 delta_id>` / `S<be64 id>s` | delta = le64 bytes‖le64 recs（`encode_pack_delta`）；seal = le64 file_size |
| `L` | `try_gc_lease` 内 `tkey('L', "gc")` | GC 租约 | `<owner>\0<expiry_ms>` |

前缀范围由 `range_of` 生成：`[lo, hi)`，hi = lo 经 `codec::bump_last_byte`（末非
0xff 字节 +1）。pack 账采用**唯一 delta 行**而非共享账行：每次业务事务写一条新
delta 行（id 出 `d` 号段），纯写无冲突——共享行的读改写会让同一 active pack 上的
并发小对象 PUT prewrite 互撞；折叠合并推迟到低频的 `pack_stats()`（§7）。

## 2. TikvClient：集群接入与读原语

- **构造**（`tikv_client.cc:TikvClient::TikvClient`）：先
  `bridge_poco_logs_once()`——把 Poco root logger 的 channel 换成
  `PocoSpdlogChannel`（client-c 全部经 Poco 记日志；须先于首个 pingcap logger
  创建，Poco 子 logger 在创建时继承 channel；information 起送，spdlog 侧再按全局
  级别二次过滤）；随后以 `pd_endpoints` + `pingcap::ClusterConfig` 建
  `pingcap::kv::Cluster`（内含 grpc channel 池、region cache、TSO oracle 后台线程；
  线程安全，无需连接池）。**mTLS**：ca/cert/key 三路径透传 `ClusterConfig`，三者
  齐备才启用（PD 直连 stub 复用同一 TLS 配置）。
- **backoff 预算**（`TikvOptions::backoff_budget_ms`）：>0 时替换侧车路径（2PC、
  batch_get、last_key）的库默认预算，commit 用 2×（对齐上游 commit:prewrite ≈ 2:1）；
  上游 Snapshot/Scanner 内建的 Backoffer 不受控（列为上游改进项）。
- `get_ts`：PD TSO（事务 start_ts / 列举快照版本）。
- `get`：`Snapshot::Get` 单 key 快照读；client-c 以空串表示不存在，codec 值恒非空
  （首字节版本号），"空串 = absent" 消歧无损。
- `batch_get`：自实现 KvBatchGet——`groupKeysByRegion` 分组、region 错误回退重组、
  响应级锁错误 resolve 后整组重做、**单 key 锁错误回退单 key `Get`**（其内部解锁）；
  返回与 keys 同序同长的 optional 数组。
- `scan`：`Snapshot` + `Scanner`，limit **精确下推为批大小**（存在性探测 limit=1 只取
  1 条，无隐式超读）；同版本多次调用构成一致视图（MVCC）。
- `last_key`：key-only **反向扫描** limit=1（client-c Scanner 不封装 reverse，直接组
  `KvScan` 请求）——先经 region cache 前向收集覆盖 `[lo, hi)` 的 region 列表（基本
  纯缓存命中），再从尾 region 逐个反向探测；region 拓扑变化整体重来；锁错误经
  LockResolver 解决后重扫。它是 list 组尾 token 的 O(1) 原语（对应 RocksDB 版
  SeekForPrev）。

## 3. 乐观 2PC 侧车：Committer

`tikv_client.cc:Committer` 改写自 client-c `kv/2pc.cc`（Apache-2.0，@78a557e；
八条差异逐条注明于 `tikv_client.cc` 文件头）。mutation 为
`tikv_client.h:TikvMutation`{op, key, value}，op ∈ {kPut, kDel, kLock, kInsert}
（`kvrpcpb::Op` 子集）：kLock 是占位锁记录，用于把只读前置条件的写偏斜物化为
写写冲突；kInsert 是"put + 必须不存在"（唯一使用点 create_bucket）。

**事务构成**（构造函数）：

- 同 key 多 mutation 按出现序合并、后者胜（对齐 WriteBatch 的按序覆写语义；
  Percolator 每 key 只发一个 op）；keys 按首现序去重，**primary = muts[0].key**
  （调用方把语义焦点 key 放首位）；
- `txn_lock_ttl`：小事务恒 defaultLockTTL(3s)；超单批字节上界
  （`kTxnCommitBatchSize` = 16KiB）的按 `ttlFactor·√MiB` 放大，夹在 [3s, 20s]
  （`kManagedLockTTL`）——大事务 prewrite 分批串行发送耗时更长，TTL 不放大会被
  读者判死回滚（万分片 complete 场景）。

**prewrite**（`prewrite_keys` / `prewrite_batch`）：

- `make_batches`：`groupKeysByRegion` 按 region 分组，再按 key+value 字节 16KiB
  切批（镜像上游 doActionOnKeys 形态）；
- **显式工作栈代替递归**：region 分裂/迁移触发的重做原本会递归
  （prewrite_batch → prewrite_keys → …），退避预算只约束时间不约束栈深；批处理
  失败（region 级错误）把 keys 压回栈重解析路由重做；
- 请求携带 primary_lock / start_ts / lock_ttl / txn_size / min_commit_ts=start_ts+1；
- 错误分类（`tikv_client.h` 三个结构化异常，替代上游笼统 LogicalError）：
  `already_exist` → `TikvAlreadyExist`；`conflict` / `retryable` → `TikvConflict`
  （明确未提交，安全重试）；其余提取锁——**新乐观事务持有的活锁提前归类为
  TikvConflict**（不赌上游 `resolveLocksForWrite` 的裸 `Exception("write conflict")`
  消息串，但消息匹配保留为纵深防御；若对端实际已死，本轮多退避一次后，重试的新
  start_ts 必大于其 txn_id，走 resolver 清锁路径收敛）；活锁未过期则
  `boTxnLock` 退避后重试本批；
- **prewrite 阶段任何异常 = 明确未提交**：`execute` 捕获后 best-effort
  `cleanup_locks`（BatchRollback 清残锁，上游 TODO；清不掉无害，TTL/读者
  LockResolver 收敛），commit 点 TSO 获取失败也在此保护内。

**commit 点与两阶段落地**（`execute` / `commit_keys` / `commit_batch`）：

- primary **单独成批**先提交——这是事务的提交点。primary 明确拒绝 = 已回滚
  （TiKV 对已提交事务的 commit 幂等返回 ok）→ `TikvConflict` 安全重试；RPC 层
  异常 = **结果不明** → `TikvUndetermined`（盲重试禁令，§8）；
- primary 阶段 region 重试前**重取 TSO**（读者可能在退避期间推高 min_commit_ts，
  旧 ts 重放会撞 CommitTsExpired；commit 对同 start_ts 幂等，换 ts 无害）；
  `commit_batch` 内 CommitTsExpired 最多原地刷新 `kMaxTsRefresh`(4) 次 TSO，超限
  按冲突抛出整事务重试；
- primary 落地后事务已成功：secondaries 以 primary 冻结的 commit_ts 提交
  （上游给 secondary 也重取 TSO 会分叉 commit_ts，不跟随），失败只延迟收敛
  （读者经 LockResolver 回查 primary）——**吞掉并 WARN**。

**入口护栏**（`check_txn_size`，`TikvClient::commit` 前置）：单 value ≤ 6MiB
（raft entry 8MiB 上限留 proto 余量——超限 prewrite 每次重试必败）、总量 ≤ 96MB、
mutation 数 ≤ 30 万；违反抛 pingcap 异常（prewrite 前 = 明确未提交）。

## 4. 事务组装：txn_retry 与守卫分片

`tikv_meta_store.cc:TikvMetaStore::txn_retry(what, body)` 是全部提交方法的骨架：

```text
for attempt in 0..kMaxTxnRetries(16):
    ts = client().get_ts()               # 新 start_ts
    body(ts, muts)                       # 快照读 + C++ 计算 + 组 mutation 批
    muts 为空 → 纯读决策/幂等早退，直接返回（不发事务）
    client().commit(ts, muts) 成功 → 返回
    TikvConflict → m_conflict_retries_++，conflict_backoff（100µs..6.4ms 指数）重试
超限 → InternalError（"txn conflict storm"）
```

写集内的 key（旧对象、upload、parts）由 prewrite 的写写冲突检测天然 CAS——
"读到的值未被并发修改"由协议免费保证；**只读前置条件**（bucket 存在、空检查、
upload 存在）不在写集内，构成写偏斜窗口，用 **Op::Lock 守卫分片**物化
（`kGuardShards` = 16，FNV-1a 散列选片）：

- `put_object` / `create_upload`：附加 Lock `b<bucket>\0<fnv1a(key)%16>`；
  `delete_bucket`：Del `B` + Lock **全部 16 个** `b` 分片——任何并发 put 必撞，
  两个 put 相撞概率 1/16、重试廉价；
- `put_part`：Lock `u<...>\0<part_no%16>`（按 part_no 分片，同上传并发传不同号
  互不阻塞）；`complete_upload` / `abort_upload`：Del `U` + Lock 全部 16 个 `u`
  分片。

逐方法流水（与 Redis 版同一套 `meta_util.h` 计算 helper；mutation 辅助件
`mut_refs` / `mut_pack_delta` / `enqueue_reclaim` 与 Redis 版的 batch_* 同构，
gcq seq 与 pack delta id 均预派发保持事务纯写）：

| 方法 | 读集（start_ts 快照） | mutation 批要点 |
| --- | --- | --- |
| `create_bucket` | — | 单 **Insert** `B`；`TikvAlreadyExist` → BucketAlreadyOwnedByYou |
| `delete_bucket` | `B`；`O`/`U` 前缀 scan limit=1 空检查 | Del `B` + Lock 16 `b` 分片 |
| `put_object` | `snap_get_many({B, O})` 一次往返；`check_put_condition` 在快照旧值上校验（冲突重试轮会重查，对外原子） | Put `O`(version+1，`check_object_value` 先行 §5) 为 primary + Lock `b` 分片 + refs/pack/gcq 同批 |
| `delete_object` | `{B, O}` | 不存在 → muts 空 return false（幂等）；Del `O` + gcq(kDelete) + refs/pack 负账 |
| `put_part` | `{U, P}` | Put `P` + Lock `u` 分片；同号重传旧片入 gcq(kPartOverwrite) |
| `complete_upload` | `{U, O}` + `scan_parts` 全量 | Put `O` 为 primary + Del `U` + Lock 16 `u` 分片 + **逐片 Del `P`**（全部 parts 入写集，受 prewrite CAS 保护——Redis 版的 sha1 指纹在此不需要）+ 选中片 refs 转移/pack 口径重记、未选中片入 gcq(kComplete) + 旧同名对象入 gcq(kOverwrite) |
| `abort_upload` | `U` + `scan_parts` | Del `U`(primary) + Lock 16 `u` 分片 + 逐片 Del + gcq(kAbort) |
| `swap_extents` | `O` | version/from 失配 → muts 空 return false（放弃）；Put `O` 后由 prewrite 对 okey 的冲突检测充当 CAS；refs 走 `refs_delta` 差集（TiKV 同 key 后者胜，全加全删会抹掉未迁移 chunk 的 refs） |
| `ack_reclaim(s)` | — | 盲 Del `G<seq>`；`ack_reclaims` 单事务批量 |

纯读方法（`bucket_exists` / `get_object` / `head_object` / `require_upload` /
`chunk_referenced` 等）经 `guarded` 包装：`snap_get` / `snap_get_many` 对 pingcap
异常重试一次（client-c 内部已退避，这里只兜一次瞬时抖动）。

## 5. 对象 manifest 体量护栏

`tikv_meta_store.cc:check_object_value`（put_object / complete_upload 编码后即查）：
extents ≤ 20 万且编码 ≤ 6MiB（`kMaxObjectValueBytes`，raft entry 8MiB 上限留
2MiB proto 余量），超限抛 **EntityTooLarge(400)**——比 prewrite 反复撞 raft 上限的
500 诚实（客户端可改走 multipart/更大分片）；且若放行，超限对象一旦写入连删除
事务都会永久失败。`tikv_client.cc:check_txn_size` 是同一护栏在客户端层的最后防线。

## 6. 列举路径

- `list_objects`：一个 TSO 定版本作全程一致视图（跨分页/跳组均在同快照上，MVCC
  免费买到 Redis 版靠单 Lua 脚本才有的一致性）；页缓冲游标（`kScanPage` = 1024）+
  `advance`/`seek`，算法与 RocksDB 版逐行同构：seek 起点
  `base + max(prefix, start_after)`，delimiter 命中归组后 `bump_last_byte` 构造组
  后继重 seek 跳过整组（每组 1 RTT）；截断恰落在组上时用 `TikvClient::last_key`
  反向取组尾 key 作 next_token（O(1) RPC，不随组大小扩展）；`max_keys<=0` 返回空；
- `list_buckets`：`B` 前缀 `scan_range`（分页回调，页尾 key + `'\0'` 无缝续页），
  key 字节序即字典序；
- `list_uploads`：`U<b>\0` 前缀扫天然按 (key, upload_id) 排序；**分页 hint 完整
  下推**——marker 拼成 `lo + key_marker + '\0' + id_marker + '\0'` 抬高扫描下界，
  limit 在回调内截断（对比 Redis 版只能忽略 hint）；
- `scan_parts` / `list_parts`：`P` 前缀扫，be16 尾缀天然升序，`codec::part_no_of_key`
  反解；list_parts 的 upload 校验与 parts 读取同快照；
- `scan_refs`：`R` 前缀分页扫，key 尾 8 字节反解 file_id（弱一致由孤儿扫描契约容忍）。

## 7. 号段分配与 GC 记账

**alloc_id**（`tikv_meta_store.cc:TikvMetaStore::alloc_id`）：常规路径 `alloc_mu_`
锁内纯内存派发；段耗尽时计数器 RMW 小事务（读旧值 → Put +4096）**在锁外执行**
——TSO+2PC+冲突重试可达数十 ms，锁内会串死全部 kind 的派发；并发续段由
WriteConflict 仲裁、各拿不相交段，输家整段丢弃无害（id 只需唯一单调）。TiKV 提交
即 raft 多数派持久，**无 Redis 版的崩溃空烧补偿**。特殊处理：计数器事务的
`UndeterminedCommit` **降级为确定性 InternalError**——号段可能烧洞无害，而外层
业务事务此刻必然未提交，原样上抛会让 commit_or_discard 误判"业务提交结果不明"
而拒绝清理已写数据 extent，凭空制造孤儿。

**pack 账两半**：业务侧 `mut_pack_delta` 写唯一 delta 行（§1）；`pack_stats()` 做
`S` 前缀扫边扫边聚合（同 pack 的 `d` 行与 `s` 行相邻），单 pack delta 行超
`kPackFoldThreshold`(16) 时**顺带折叠**——独立事务删已读行 + 写一条合并行，并发
业务只会新增其他 key 不冲突；他人先折叠（行已消失）则清空 muts 放弃（半程提交会
丢账）；多网关并发折叠由写写冲突仲裁收敛。`seal_pack`：file_size=0 且已有 seal 行
则不覆盖（muts 空早退，SPI 契约）；`drop_pack_stat`：删该 pack 全部 `S` 行
（前提 live=0 且 pack 已删，快照读到的即完整集合）。

**gcq**：`peek_reclaims` = 从 `gcq_key(min_seq)` 起的 `G` 前缀 scan limit=max，
累计 extents 达上限提前收批但至少 1 条；`ack_reclaims` 覆写为单事务批量盲删。
**try_gc_lease**：value = `<owner>\0<expiry_ms>`，快照读判"他人持有且未过期"则
返回 false，否则 Put 续期——读后提交经 prewrite 冲突检测充当 CAS，两实例抢租约
只有一个提交成功；过期用墙钟判断（网关间时钟偏斜远小于 TTL 的部署前提）。

## 8. 错误映射、盲重试禁令与结果不明

`guarded` 统一翻译：`S3Error` 透传；`TikvUndetermined` →
`meta_store.h:UndeterminedCommit`（primary commit 结果不明——事务**可能已生效**，
调用方不得物理删数据回滚，客户端见 500；盲重放会把刚写入的 DataRef 记入 gcq、GC
回收在引数据）；其余 `pingcap::Exception` → InternalError。分类总表：

| 来源 | 结果 |
| --- | --- |
| WriteConflict / retryable / 新乐观活锁 | `TikvConflict`：明确未提交，txn_retry 退避重试（16 次上限） |
| Insert 撞已有 key | `TikvAlreadyExist`：明确未提交（create_bucket → 409 语义） |
| prewrite 阶段任何异常 | 明确未提交 + best-effort 清锁 |
| primary commit RPC 异常 | `TikvUndetermined` → `UndeterminedCommit` |
| secondary commit 失败 | 吞掉 WARN（事务已成功，延迟收敛） |
| 纯读 pingcap 异常 | 重试一次后 InternalError |
| close 后调用 | `client()` 原子指针判空抛 InternalError |

## 9. schema、GC safepoint 与水平扩展语义

**schema 首建竞争**（构造函数）：读到标记则 `parse_schema_marker`（谱系 `"t"`）校验
——低于当前版本走 `migrate_schema` 迁移链（每步幂等，共享引擎无全局迁移锁，多个
新版网关并发走链无害），高于则拒绝降级；缺失则 `Insert` 抢首建，AlreadyExist /
Conflict / Undetermined（常量幂等写，重读可判）都进退避循环收敛——多网关同时
首启是受支持的合法竞争。

**GC safepoint**（`update_gc_safepoint_once` + 后台 worker）：纯 KV 集群无 TiDB
推进者，MVCC 垃圾会永久积累。每 `gc_safepoint_interval_s` 一轮三步：①注册本服务
safepoint = now − retention（TSO 域直接减 `ms << 18`；TTL 3×interval，停摆网关两轮
自动摘除）；②以无限 TTL 顶替 `gc_worker` 服务项（PD 对缺失的 gc_worker 永久占位，
不推它 min 恒被钉死）；③以 PD 返回的全服务 min 推进集群 safepoint（不越过 BR/CDC
等外部服务的声明；PD 单调只进，多网关并发推进天然收敛）。三个 PD RPC client-c 未
封装，`tikv_client.cc:TikvClient::Impl::pd_call` 经 `getLeaderUrl()` 自建 grpc stub
直连 leader（懒建缓存，报错/换主弃缓存重建；TLS 复用 ClusterConfig）。共 TiDB
集群须 interval=0 关闭。`close()` 先停 worker 再摘 `client_` 原子指针再析构
Cluster（顺序颠倒会让 worker 正常退出路径变成 500 抛出）。

**水平扩展论证**：原子性由 Percolator 2PC 对任意进程成立（primary 定提交点、
读者经 LockResolver 回查），TikvMetaStore 不持任何业务互斥（仅 `alloc_mu_` 号段
小锁）；meta 存储自身多副本（raft 多数派）可水平扩展——`meta_sync` 配置无意义
（恒等效 true）。多个网关指向同一 PD + 同一前缀即共享 meta；号段、schema、GC
租约、safepoint 推进、pack 折叠五处多写者竞争各自经事务冲突或 PD 单调语义收敛。

指标（构造期注册，0 值可见）：`lights3_duostore_tikv_txn_conflict_retries_total`、
`lights3_duostore_tikv_safepoint_update_failures_total`、
`lights3_duostore_tikv_gc_safepoint_ms`（gauge，最近推进值物理 ms）。
