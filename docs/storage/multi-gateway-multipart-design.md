# 多网关共享存储下的 Multipart：现状核对与补齐步骤

> 状态：**§4 ① 写侧租约（2026-09-08）、② 双实例测试与 ③ 文档配置（2026-09-09）已实现，④ 待做**。本文回答一个问题：
> 多个 lights3 网关指向同一份共享存储时，一个 multipart 上传的
> create / upload_part / complete / abort 能否落在**不同网关**上。结论先行：
> 只有 **duostore（redis / tikv meta + rados data）** 与 **cloudproxy** 在设计上
> 支持；duostore 的这条组合曾有一个会损坏数据的缺口（§3.2 写侧在途保护，
> 已由 §4 ① 的写侧租约关闭）与零端到端测试，补齐步骤见 §4。其余后端不是
> 多网关设计，§2 逐一说明。
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

### 3.2 缺口 G1（已关闭）：写侧在途保护曾是进程内的

`run_orphan_scan_once`（`duostore_backend.cc`）的正向判定原为：**无 refs +
mtime 早于 `gc_grace` + 本进程 pin 表无 pin** → unlink。代码注释明说
"write-side pin covers very long streaming PUTs, for which the mtime grace alone
is insufficient"——即单网关下超过 `gc_grace` 的长上传靠**写侧 pin** 保护。
写侧 pin（`ChunkPinHooks` / `write_pins_`）与读侧一样是进程内表，读侧有
read-lease 把它搬到共享介质，写侧当时**没有对应物**。

多网关下的失效序列（单测 `duostore_orphan_scan_defers_to_peer_write_lease`
第一阶段以 `read_lease: 0` 复现）：

1. 网关 A 接收一个大分片（或大 PUT），`RadosChunkWriter` 按 `rados_chunk_size`
   （默认 8 MiB）逐块 `write_full`，第一块的 mtime 是上传开始时刻；
2. 上传持续超过 `gc_grace`（默认 300 s；5 GiB 分片在 10 MB/s 客户端上需 500 s）；
3. 网关 B（`gc_enabled: true`）跑孤儿扫描：`scan_chunks` 枚举整个 namespace，
   A 的前若干块无 refs（分片尚未提交）、mtime 逾 grace、B 的 pin 表当然没有 →
   `chunk_referenced` 复查仍无 refs → **删除**；
4. A 泵送完毕，`put_part` 提交成功，refs 指向已删对象；complete 后对象存在、
   GET 该区间 500（缺 extent），`lights3 fsck` 才会发现。

触发条件是"孤儿扫描轮 ∩ 逾 grace 的在途写"。默认 `orphan_scan_interval: 1d`
使概率不高，但 `lights3 duostore scan <backend>` 手动轮随时可触发。gcq 路径
不受影响（只有曾被引用的 extent 才入 gcq，且已被 read-lease 门控）。

**现状**：§4 ① 的写侧租约已落地——租约同时携带最老在途写开始时间，孤儿扫描
只删 mtime 早于该下限的无 refs chunk。`read_lease: 0` 关闭租约时仍回落到
"`gc_grace` ≥ 最长预期分片/对象上传时长"的运维约定（代码不校验）。

### 3.3 缺口 G2（已关闭）：曾经零端到端验证

现有多网关测试只在 IMetaStore 层（两个 `RedisMetaStore` / `TikvMetaStore`
实例共前缀：号段唯一、CAS 收敛、GC 租约、read-lease），**没有**两个
`DuoStoreBackend` 共享同一 meta + data 的用例，multipart 跨实例的四步从未被
执行过；e2e（`tests/e2e/run_e2e.sh`）与 compose profile 都是单网关。

**现状**：§4 ② 已落地——`tests/unit/multi_gateway_suite.h`、`run_e2e.sh` 双网关段、
compose `multi` profile。

### 3.4 缺口 G3（文档配置已关闭，误配告警归 ④）：曾经无承载

- [duostore-data-rados-design.md](duostore-data-rados-design.md) §8.3 的前提表
  没有 multipart / 写侧在途一行；[duostore-core.md](duostore-core.md) §9 无多网关
  说明；`config/lights3.yaml` 未列 `read_lease`，`gc_enabled` 注释未提写侧约束。
- `redis/tikv meta + fs data` 这条**不支持**的组合启动时无任何告警，误配只在
  跨网关 GET 失败时暴露。

**现状**：第一条随 §4 ③ 关闭（前提表、§9.1、`read_lease` 样例、
[../deployment.md §5](../deployment.md) 多网关小节）；第二条待 §4 ④。

## 4. 补齐步骤

按依赖顺序；① 是正确性前提，②③ 让它可验证、可运维，④ 是误配防线。

### ① 写侧租约（write-lease）：把写侧在途保护搬到共享介质 —— 已实现

与 read-lease 同构，复用其发布/消费骨架，不引入新表（实现细节与安全论证
汇总在 [duostore-core.md §8.5](duostore-core.md)）：

1. **登记**：`put_object` / `upload_part` / `tier_commit_cached` 在 `pump_body`
   之前构造 `WriteTicket`（向 `write_clock_`——与读侧同一实现 `InFlightClock`
   的独立实例——取 ticket），协程帧退出（提交或兜底删除之后）注销。登记先于
   第一块落盘，故发布值必然早于任何在途块的 mtime。
2. **发布**：`lease_tick` 同时发布读、写两个下限。接口由
   `publish_read_lease(owner, oldest_ms, ttl)` / `min_read_lease()` 改为
   `publish_lease(owner, LeaseInfo{oldest_read_ms, oldest_write_ms}, ttl)` /
   `min_lease() -> optional<LeaseInfo>`（`meta_store.h`）；redis 值
   `<read> <write>`、tikv 'L' 表 `r<owner>` 行值 `<read>\0<expiry>\0<write>`。
   **兼容**：解析到旧格式（缺 write 字段）视该网关写侧下限未知 →
   `min_lease().oldest_write_ms` 为 nullopt，消费侧对该轮回落 grace-only
   （等价现状，不更糟）。`publish_lease_once()` 为手动发布钩子。
3. **消费**：孤儿扫描枚举前取 `min_lease()`，
   `write_floor = oldest_write_ms − clamp(gc_grace, 1 s, 60 s)`；无 refs 的
   chunk 仅当 `mtime < write_floor` 才成为候选，否则计入新增统计
   `skipped_leased`（admin JSON / `lights3 duostore scan` 日志同步输出）。
   余量吸收 OSD 与网关的时钟差（rados 对象 mtime 由 OSD 打）与粗粒度文件时间
   戳，1 s 下限让 `gc_grace: 0` 的测试配置也能容忍一个 jiffy。取租约失败 →
   WARN + grace-only（与 gcq 路径一致，绝不停摆）。
4. **本地引擎**：`publish_lease` 返回 unsupported → 发布器停摆（原行为），
   写侧 pin 仍精确。
5. **pack 路径不改**：rados 无 pack；fs 数据面不在多网关矩阵内（§2）。

测试（已随实现落地）：

| 用例 | 覆盖 |
| --- | --- |
| `duostore_orphan_scan_defers_to_peer_write_lease`（test_duostore.cc） | 两个 `DuoStoreBackend` 共享一个 RocksMetaStore + 租约板 + 同一 chunk 目录。阶段 1 `read_lease: 0`：对端扫描删掉在途 PUT 的块、提交后 `refs_missing`（复现缺口）；阶段 2 租约开启：`skipped_leased ≥ 1`、块保留、对象从对端可读；早于下限的真孤儿仍被删 |
| `duostore_redis_read_lease` / `duostore_tikv_read_lease` | 双字段逐字段取最小、TTL 过期、旧格式值 → 写侧下限未知 |
| `duostore_redis_meta_cache_bounded_staleness` | 读侧回拨 TTL、写侧不回拨 |

替代方案评估：在 `alloc_file_run` 时把号段登记进共享"在途 id"表并 TTL 续租，
孤儿扫描跳过表内 id——精确但每次分配多一次共享写、需要续租线程，且
`alloc_file_run` 是热路径；write-lease 一个网关一行、每 `read_lease` 秒一次
写，与现有骨架同构，胜出。

### ② 测试：两个 backend 实例共享 meta + data —— 已实现

单测套件 `tests/unit/multi_gateway_suite.h`，由 `test_duostore_redis.cc` /
`test_duostore_tikv.cc` 各自带工厂实例化（外部实例缺席时 SKIP）：用现有注入构造
（`duostore_backend.h` "For test injection: self-assembled meta/data"）建两个
`DuoStoreBackend`，共享**同一个** `IMetaStore` 连接目标（同一 key 前缀，各自连接）
与**同一个** `IDataStore` 对象（一个 `FsDataStore` 经转发壳 `SharedDataStore`
在两侧共享——对象级共享绕开了"共享 root 误配"的 flock 问题，也正是 rados 数据面
给每个网关的东西，足以验证 backend 编排层）。两侧 `read_lease: 1s`、对象缓存关、
`gc_grace: 0`、只走手动钩子，回收判断全由 refs / pin / 租约驱动而非时间：

| 用例（`duostore_{redis,tikv}_multi_gateway_*`） | 断言 |
| --- | --- |
| `multipart`：A create → B upload_part ×2 → A complete → B get | ETag = `combined_etag`，A/B 读回内容逐字节相等，孤儿扫描零动作 |
| `abort_while_peer_pumps`：A upload_part 泵送中 → B abort | A 的 put_part 抛 NoSuchUpload，A 落盘的 chunk 被 `commit_or_discard` 删除，B 的孤儿扫描 `chunks_scanned=0` |
| `same_part_concurrent`：同号分片 A/B 并发 | 恰好一个胜者，两网关读到胜者内容；败者 extent 入 gcq，A 的 GC 回收 2 块 |
| **长写 vs 对端孤儿扫描**（G1 回归） | 已随 ① 落地（`duostore_orphan_scan_defers_to_peer_write_lease`） |
| `mpu_ttl_single_executor`：mpu_ttl 清理 | 只有持 GC 租约的 A abort 过期 upload；B 整轮空转 `uploads_expired=0`；轮内 abort 入 gcq 的分片晚于对端读下限，本轮 `skipped_leased`，下一轮回收 |
| `listings_shared`：list_parts / list_uploads 跨网关 | 任一网关列出的 (key, upload_id) 与 (part_no, size, etag) 序列一致；一侧 abort 另一侧立即不可见 |

实现时发现的两个可测性细节：读租约下限是 unix-ms 且**含等号**（与下限同一毫秒
入队的 gcq 项被推迟），套件在发布租约前让几毫秒过去；GC 轮内 mpu_ttl abort 入队
的分片必然晚于轮前发布的下限，只能在下一轮回收——这是设计行为（对端可能有早于
abort 的在途读），不是缺口。

e2e 两层：

- `run_e2e.sh` 的 `duostore-redis` 变体新增"multi-gateway multipart"段：同一
  redis meta + 同一数据根起两个网关，create 在 A、5 个分片 B/A 交替、两侧
  ListParts、B complete、A GET；期望 ETag 由脚本用各分片 ETag 独立算出
  （md5(拼接二进制 md5)-5）。本机可跑，已通过。
- compose profile `multi`：`lights3-multi-a` / `-b`（redis meta + rados data，
  `read_lease: 5s`，`LIGHTS3_GC_ENABLED` 仅 a 为 true）+ `nginx-multi`
  （无粘连逐请求轮询，:9004）+ `e2e-multi`（`deploy/docker/e2e-multi.sh`：经
  nginx 跑 5 分片 multipart，校验合成 ETag、GET 字节、两网关请求计数都在动；
  用 curl `--aws-sigv4` 而非 aws cli，与仓库 e2e 工具链一致）。
  `docker compose --profile multi config` 通过；本机无 docker daemon，实际拉起归入
  [../todo.md](../todo.md) §2 待验证。

### ③ 文档与配置 —— 已实现

- [duostore-data-rados-design.md](duostore-data-rados-design.md) §8.3 前提表：
  "写侧 pin vs 他网关孤儿扫描"行随 ① 加入，本步补"multipart 跨网关：已验证
  （§4 ②）"行，并把"暂不实现租约式"的结语改为已落地的粗粒度租约；
  [duostore-core.md](duostore-core.md) §8.5 已随 ① 改题为"多网关读写租约"，
  本步加 §9.1 多网关小节（全局唯一、竞态收敛、在途保护、单执行者、用例、
  fs data 不在矩阵内）。
- `config/lights3.yaml` duostore 段补 `read_lease: 5s`（多网关必开，`0s` = 关，
  关则 `gc_grace` 须盖过最长 GET 与上传）注释，`gc_enabled` 注释指向本文与
  deployment.md §5。
- [../deployment.md](../deployment.md) §5 "多网关部署"（+en）：支持矩阵（§2）、
  必要配置表（`gc_enabled` 单实例、`read_lease` 开启、NTP、`meta_cache_ttl`
  约束、前缀 / namespace 一致、实例级后台任务单开）、负载均衡无需粘连
  （透传 `Host`、关请求缓冲）。原 §5 / §6 顺延为 §6 / §7。

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
