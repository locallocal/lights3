# 多网关共享存储下的 Multipart：现状核对与补齐步骤

> 状态：**设计 / 待实施**（2026-09-08 核对代码得出）。本文回答一个问题：
> 多个 lights3 网关指向同一份共享存储时，一个 multipart 上传的
> create / upload_part / complete / abort 能否落在**不同网关**上。结论先行：
> 只有 **duostore（redis / tikv meta + rados data）** 与 **cloudproxy** 在设计上
> 支持；duostore 的这条组合还有一个会损坏数据的缺口（§3.2 写侧在途保护）与
> 零端到端测试，补齐步骤见 §4。其余后端不是多网关设计，§2 逐一说明。
>
> 相关：[duostore-core.md](duostore-core.md) §3 / §8 / §9、
> [duostore-data-rados-design.md](duostore-data-rados-design.md) §8.3、
> [duostore-meta-redis.md](duostore-meta-redis.md) §8、
> [duostore-meta-tikv.md](duostore-meta-tikv.md) §9、[localfs.md](localfs.md) §7。

## 1. 场景定义

"共享存储 + 多网关"指：N 个 `lights3` 进程（通常不同主机、前置负载均衡、
无会话粘连）挂同一个逻辑后端；S3 客户端的 multipart 各步请求可被均衡到任意
网关。要成立，须同时满足：

| 前提 | 含义 |
| --- | --- |
| P1 元数据共享且原子 | upload / part 记录对所有网关可见，put_part / complete / abort 的复合不变量跨进程原子 |
| P2 upload_id 全局唯一 | 任一网关生成的 id 不与他网关冲突 |
| P3 分片数据共享可读 | 网关 B 能读到网关 A 落盘的分片字节（complete 是零拷贝装配时尤其关键） |
| P4 数据面 id 全局唯一 | chunk / pack 号段跨网关不重叠 |
| P5 在途保护跨网关 | 网关 A 尚未提交的分片数据，不被网关 B 的 GC / 孤儿扫描当作垃圾 |
| P6 后台任务单执行者 | mpu_ttl 过期清理、GC、孤儿扫描只由一个实例跑，或互相协调 |

## 2. 各后端现状

| 后端 | 状态 | 依据 |
| --- | --- | --- |
| memory | 不适用 | 进程内存 |
| localfs / xlocalfs | **非多网关设计** | 分片状态全在 `<staging>/mpu/<upload_id>/`（[localfs.md](localfs.md) §7），无进程内 upload 注册表，理论上共享 POSIX 文件系统（NFS）下能"碰巧工作"；但仓库从未把共享 root 当作支持的部署：rename / fsync / xattr 在 NFS 上的语义未论证，`commit_lock` 是进程内 per-key 互斥（两网关同 key 并发 complete 各自 rename，后者胜），`next_tmp_name` = pid + steady_clock + 进程内序号（跨主机可撞名，靠 `O_EXCL` 兜成报错），过期清理每个网关各跑一遍（`remove_all` 幂等，无害） |
| tiered | 同 localfs | multipart 四步全部委托 local 侧 `LocalFsBackend`（`tiered_backend.cc:740-773`） |
| cloudproxy | **支持** | 纯透传：upload_id 是远端 S3 的，网关无本地状态（`cloudproxy_backend.cc:1104-1200`）；任意网关处理任意一步 |
| duostore + rocksdb / sqlite meta | 不可能 | 本地引擎持文件锁，单进程独占 |
| duostore + redis / tikv meta + **fs data** | **不支持**（且与 multipart 无关） | meta 共享但数据在各网关本地盘：A 写的 chunk / pack 在 B 上不存在，B 的 GET 直接缺 extent；B 的 GC 对 A 的 extent `remove` 得 ENOENT 幂等销账 → A 盘上永久泄漏；B 启动的 `abandon_stale_packs` 只能探测本机 flock，会误封 A 的 active pack。文档把共享 root 明列为误配（[duostore-data-fs.md](duostore-data-fs.md) §5 "误配共享 root"）。这条组合只能作单网关部署（共享 meta 仅换来 meta 侧高可用） |
| duostore + redis / tikv meta + **rados data** | **设计上支持，有缺口** | 见 §3 |

## 3. duostore（redis / tikv + rados）逐项核对

### 3.1 已满足

| 前提 | 现状 |
| --- | --- |
| P1 | `create_upload` / `put_part` / `complete_upload` / `abort_upload` 各为单事务（[duostore-core.md](duostore-core.md) §3.1）；redis 为服务端 Lua 脚本（[duostore-meta-redis.md](duostore-meta-redis.md) §8），tikv 为 2PC + 写写冲突重试（[duostore-meta-tikv.md](duostore-meta-tikv.md) §9）。跨网关的竞态全部收敛：同号分片两网关并发上传 = last-write-wins，败者入 gcq；A 在泵送分片时 B abort/complete → A 的 `put_part` 抛 NoSuchUpload，`commit_or_discard` 删已落数据；`UndeterminedCommit` 不删、留给孤儿扫描 |
| P2 | `multipart.cc:new_upload_id` = `getentropy` 128 bit 随机，与进程无关 |
| P3 | rados 无 pack 层，全部 extent 为 `kRados` 对象，同 pool + namespace 内全网关可读；complete 是纯 meta 装配（§9），任一网关装配、任一网关读 |
| P4 | `alloc_file_run` 走共享 meta 的号段分配（redis INCRBY / tikv 计数器 RMW），单测 `duostore_redis_multi_gateway_shared_meta` / `duostore_tikv_multi_gateway_shared_meta` 覆盖 |
| P6 | `try_gc_lease` 保证 GC 轮与孤儿扫描单执行者；mpu_ttl 清理是 GC 轮第 1 步，随租约走；非指定网关 `gc_enabled: false` |
| 读侧 | read-lease（[duostore-core.md](duostore-core.md) §8.5）已把"A 在读、B 在回收"的 pin 表跨进程问题覆盖 |
| 元数据缓存 | 分片记录不进缓存；complete 在本网关 `invalidate_on_exit`，redis 经 pub/sub 广播、tikv 靠 `meta_cache_ttl` 有界陈旧（§7.1），对 multipart 无额外问题 |

### 3.2 缺口 G1：写侧在途保护是进程内的（会损坏数据）

`run_orphan_scan_once`（`duostore_backend.cc:2047` 起）的正向判定：**无 refs +
mtime 早于 `gc_grace` + 本进程 pin 表无 pin** → unlink。代码注释明说
"write-side pin covers very long streaming PUTs, for which the mtime grace alone
is insufficient"——即单网关下超过 `gc_grace` 的长上传靠**写侧 pin** 保护。
写侧 pin（`ChunkPinHooks` / `write_pins_`）与读侧一样是进程内表，但读侧有
read-lease 把它搬到共享介质，**写侧没有对应物**（`ReadClock` 只在
`PinnedReader` 注册，`pump_body` 不注册）。

多网关下的失效序列：

1. 网关 A 接收一个大分片（或大 PUT），`RadosChunkWriter` 按 `rados_chunk_size`
   （默认 8 MiB）逐块 `write_full`，第一块的 mtime 是上传开始时刻；
2. 上传持续超过 `gc_grace`（默认 300 s；5 GiB 分片在 10 MB/s 客户端上需 500 s）；
3. 网关 B（`gc_enabled: true`）跑孤儿扫描：`scan_chunks` 枚举整个 namespace，
   A 的前若干块无 refs（分片尚未提交）、mtime 逾 grace、B 的 pin 表当然没有 →
   `chunk_referenced` 复查仍无 refs → **删除**；
4. A 泵送完毕，`put_part` 提交成功，refs 指向已删对象；complete 后对象存在、
   GET 该区间 500（缺 extent），`lights3 fsck` 才会发现。

触发条件是"孤儿扫描轮 ∩ 逾 grace 的在途写"。默认 `orphan_scan_interval: 1d`
使概率不高，但 `lights3 duostore scan <backend>` 手动轮随时可触发，且
rados 数据面把每一块都写成独立对象，块越多越容易被扫到。gcq 路径不受影响
（只有曾被引用的 extent 才入 gcq，且已被 read-lease 门控）。

**现有唯一规避**：把 `gc_grace` 调到 ≥ 最长预期分片/对象上传时长（同时会拖慢
所有正常回收），或非指定网关之外关闭孤儿扫描。两者都是运维口头约定，代码不校验。

### 3.3 缺口 G2：零端到端验证

现有多网关测试只在 IMetaStore 层（两个 `RedisMetaStore` / `TikvMetaStore`
实例共前缀：号段唯一、CAS 收敛、GC 租约、read-lease），**没有**两个
`DuoStoreBackend` 共享同一 meta + data 的用例，multipart 跨实例的四步从未被
执行过；e2e（`tests/e2e/run_e2e.sh`）与 compose profile 都是单网关。

### 3.4 缺口 G3：文档与配置无承载

- [duostore-data-rados-design.md](duostore-data-rados-design.md) §8.3 的前提表
  没有 multipart / 写侧在途一行；[duostore-core.md](duostore-core.md) §9 无多网关
  说明；`config/lights3.yaml` 未列 `read_lease`，`gc_enabled` 注释未提写侧约束。
- `redis/tikv meta + fs data` 这条**不支持**的组合启动时无任何告警，误配只在
  跨网关 GET 失败时暴露。

## 4. 补齐步骤

按依赖顺序；① 是正确性前提，②③ 让它可验证、可运维，④ 是误配防线。

### ① 写侧租约（write-lease）：把写侧在途保护搬到共享介质

与 read-lease 同构，复用其发布/消费骨架，不引入新表：

1. **登记**：`pump_body`（PUT 与 upload_part 共用）开始前向 `WriteClock`
   （`ReadClock` 同一实现，独立实例）取 ticket，`WritePinRelease` 析构时注销。
   登记先于第一块落盘，故发布值必然早于任何在途块的 mtime。
2. **发布**：`lease_tick` 同时发布 `oldest_read_ms` 与 `oldest_write_ms`
   （无在途写发 now）。接口从
   `publish_read_lease(owner, oldest_ms, ttl)` / `min_read_lease()` 扩为
   `publish_lease(owner, LeaseInfo{oldest_read_ms, oldest_write_ms}, ttl)` /
   `min_lease() -> optional<LeaseInfo>`；redis 值 `<read>\0<write>`、tikv `'L'`
   表 `r<owner>` 行值追加一段。**兼容**：解析到旧格式（缺 write 字段）视该
   网关 write 下限未知 → 消费侧对该轮回落 grace-only（等价现状，不更糟）。
3. **消费**：孤儿扫描的 chunk 候选条件加一项：`mtime_ms < write_floor − skew`，
   `write_floor = min` 全体存活租约的 `oldest_write_ms`。含义：任何在途写开始
   之后才出现的块都可能属于它，跳过；发布延迟只让 floor 偏旧 ⇒ 只会少删。
   `skew` 取 `gc_grace` 的一个固定分数（如 min(gc_grace, 60 s)）吸收 OSD 与
   网关的时钟差（rados 对象 mtime 由 OSD 打，不是网关时钟）。取租约失败 →
   WARN + grace-only（与 gcq 路径一致，绝不停摆）。指标：`skipped_leased`
   在孤儿扫描统计里新增一项，日志一并打印。
4. **本地引擎**：`publish_lease` 返回 unsupported → 发布器停摆（现行为），
   写侧 pin 仍精确。
5. **pack 路径不改**：rados 无 pack；fs 数据面不在多网关矩阵内（§2）。

替代方案评估：在 `alloc_file_run` 时把号段登记进共享"在途 id"表并 TTL 续租，
孤儿扫描跳过表内 id——精确但每次分配多一次共享写、需要续租线程，且
`alloc_file_run` 是热路径；write-lease 一个网关一行、每 `read_lease` 秒一次
写，与现有骨架同构，胜出。

### ② 测试：两个 backend 实例共享 meta + data

单测（`tests/unit/test_duostore_redis.cc` / `test_duostore_tikv.cc`，外部实例缺席
时 SKIP）：用现有注入构造（`duostore_backend.h` "For test injection:
self-assembled meta/data"）建两个 `DuoStoreBackend`，共享**同一个**
`IMetaStore` 连接目标与**同一个** `IDataStore` 对象（本机无 rados 时以一个
`FsDataStore` 实例在两侧共享——对象级共享绕开了"共享 root 误配"的 flock
问题，足以验证 backend 编排层）：

| 用例 | 断言 |
| --- | --- |
| A create → B upload_part ×2 → A complete → B get | ETag = `combined_etag`，内容逐字节相等 |
| A upload_part 泵送中 → B abort | A 的 put_part 抛 NoSuchUpload，A 落盘的 chunk 被 `commit_or_discard` 删除，孤儿扫描无残留 |
| 同号分片 A/B 并发 | 胜者内容可读，败者 extent 入 gcq 并被 GC 回收 |
| **长写 vs 对端孤儿扫描**（G1 回归） | A 用慢 BodyReader 持有在途写，`gc_grace=0`，B 跑 `run_orphan_scan_once`：无 write-lease 时 A 的块被删（先写出失败用例证明缺口），接线后 `skipped_leased>0` 且块保留 |
| mpu_ttl 清理 | 只有持租约的实例 abort 过期 upload；另一实例 `uploads_expired=0` |
| list_parts / list_uploads 跨网关 | 任一网关列出的集合一致 |

e2e：compose 增加 profile `multi`（两个 `lights3` + redis + rados，前置一个
nginx 轮询），`run_e2e.sh` 用 aws cli 跑 5 分片 multipart 并校验 ETag；本机无
docker daemon，归入 [../todo.md](../todo.md) §2 待验证。

### ③ 文档与配置

- [duostore-data-rados-design.md](duostore-data-rados-design.md) §8.3 前提表加
  "写侧在途 vs 他网关孤儿扫描：write-lease（§4 ①）"与"multipart 跨网关：已验证
  （§4 ②）"两行；[duostore-core.md](duostore-core.md) §9 加多网关小节，§8.5 改题
  为"读写租约"。
- `config/lights3.yaml` duostore 段补 `read_lease: 5s`（多网关必开，0 = 关）
  注释，`gc_enabled` 注释指向本文。
- [../deployment.md](../deployment.md) 新增"多网关部署"小节：支持矩阵（§2）、
  必要配置（`gc_enabled` 单实例、`read_lease` 开启、NTP、`meta_cache_ttl` 约束）、
  负载均衡无需粘连。

### ④ 误配防线

`DuoStoreBackend` 构造末尾：meta 为 redis / tikv 且 data 为 fs 时
`LOG_WARN("… shared meta with local fs data: single-gateway only, objects written
by other gateways are unreadable here")`；`--check-config` 同步打印。不做硬拒绝
（单网关用共享 meta 是合法的）。

## 5. 不做

- localfs / tiered 的共享文件系统多网关：需要 NFS 语义论证 + 跨主机 per-key
  锁，与 duostore 的正路重复；明确不做，文档保持"单网关"。
- fs 数据面的跨网关共享（共享 root）：见 §2，误配。
- 分片级别的分布式 pin（逐 extent 上报）：read-lease 的评估结论同样适用
  （[duostore-data-rados-design.md](duostore-data-rados-design.md) §8.3 候选表），
  粗粒度租约足够。
