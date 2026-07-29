# TODO — 项目待办清单

> 生成时间：2026-07-26。基于全部设计文档的路线图章节与源码扫描（TODO 注释、
> NotImplemented 路径、工厂注册、测试覆盖、CMake/build.sh 开关）交叉核对得出。
> 各条目附文档小节号或 `文件:行号` 引用；依赖关系用「前置」标注。

## 0. 现状快照

已完成（有 commit 佐证）：四层架构 + 四个 HTTP driver + SigV4（含 chunked/presigned）+
凭证管理一期；存储侧 localfs / xlocalfs / memory / tiered（P1-P5）/ cloudproxy（P1-P5）/
duostore P1-P5 全部（RocksDB meta + chunk/pack data + GC 一二期 + 打磨），以及 duostore 可插拔件：Redis meta（R1-R3）、
SQLite meta（S1-S3）、RADOS data（C1-C2）、TiKV meta（T1-T4）。

未完成阶段总览：

| 阶段 | 出处 | 状态 |
| --- | --- | --- |
| DuoStore P2（pack 聚合） | duostore-backend.md §15 | ✅ 已完成（2026-07-26） |
| DuoStore P3（GC 一期） | duostore-backend.md §15 | ✅ 已完成（2026-07-26） |
| DuoStore P4（GC 二期：压实/孤儿） | duostore-backend.md §15 | ✅ 已完成（2026-07-29） |
| DuoStore P5（打磨/指标/e2e_tiered_duostore） | duostore-backend.md §15 | ✅ 已完成（2026-07-29） |
| Redis meta R4（打磨） | duostore-redis-meta.md §10 | ✅ 已完成（2026-07-30） |
| SQLite meta S4（打磨） | duostore-sqlite-meta.md §10 | ✅ 已完成（2026-07-30） |
| RADOS data C3（aio 桥接）/ C4（孤儿+多网关） | duostore-rados-data.md §12 | 未开始 |
| TiKV meta T5（打磨） | duostore-tikv-meta.md §11 | ✅ 已完成（2026-07-30，上游 PR 回馈除外） |
| tiered 对账工具（P4 剩余） | tiered-storage.md §9/§10 | 未做 |
| cloudproxy 指标 + control_in_pump（P4 剩余） | cloudproxy-backend.md §8.2/§2.3 | 未做 |
| 凭证管理二期 | credential-management.md §9 | 未开始 |

## 1. 主线：DuoStore P2–P5（✅ 已全部完成，2026-07-29）

### 1.1 P3 · GC 一期（✅ 已完成，2026-07-26）

全部条目落地，覆盖 4 meta × 2 data 组合（经 IMetaStore/IDataStore 接口天然生效）：

- ✅ gcq 消费 worker：`run_gc_once()` 批取 `peek_reclaims`（256/批）→ grace/pin 过滤
  → 先物理删后批量销账（`ack_reclaims`，§9.1 顺序铁律）；`Reclaim` 增 `enqueue_ms`
  回传入队时刻供判 grace
- ✅ chunk/rados unlink 与整 pack 删除（`IDataStore::remove_pack`，fs 实现 unlink，
  其余默认 no-op；packstat 销账接口随 P2 存活账落地）
- ✅ pin 计数（`duostore::PinTable`，GET reader 持 pin、析构解除）+ `gc_grace`
- ✅ `run_gc_once()` 测试钩子 + 后台 worker（TimerQueue 周期；`gc_interval` 0 可关；
  `src/core/timer` 首批单测同步补齐，§5.3 已销）
- ✅ mpu_ttl 过期 multipart 清理（内部 abort，分片同轮变现）
- ✅ close() 补齐：撤销 GC 定时器、等待在途 GC 协程；dtor 兜底同路径（定时器回调
  生命期由 GcGuard 防悬垂 this）
- ✅ rados C2 遗留「GC 变现 / pin 竞态」专项测试（无集群 SKIP）
- 验收：GC 收敛/grace/pin/mpu/worker/close 专项 + 并发 GET vs GC 无 ENOENT 全绿；
  asan 10 连跑、tsan 4 连跑零告警（顺带修复 tsan 检出的 `closed_` 数据竞争）

### 1.2 P2 · pack 聚合（✅ 已完成，2026-07-26）

全部条目落地，覆盖 4 meta × fs data（rados 无 pack 实体，天然不涉）：

- ✅ `open_writer()` pack/chunk 分流：已知长度 > 阈值直走 chunk；其余进
  `FsPackedWriter` 缓冲，EOF ≤ 阈值整体进 pack、超限转 chunk 流式（§5.3
  chunked 缓冲；`WriteHint` 增 owner 供 record 内嵌）
- ✅ 多 active pack 并发追加（`pack_writers` 槽 + try_lock 轮询）、record 格式
  （"LP3R" 头 + crc32c + owner）、达 `pack_max_size` 轮转封存、close() 封存、
  重启弃用（构造时 `abandon_stale_packs` 补封上代 unsealed 账）
- ✅ GET 侧 pack record 整段读入恒校验 crc（§7）
- ✅ 四个 meta store 的 pack 存活账接通：`IMetaStore` 增 `seal_pack` /
  `drop_pack_stat`；rocks = stats CF 子 key merge、redis = `pack:<id>` HASH
  HINCRBY（脚本增 hincr op）、sqlite = pack_stats 表 upsert、tikv = **delta 行 +
  pack_stats() 顺带折叠**（§3.2 预警的物化解法，业务写保持纯写无冲突）；
  complete 的 refs 转移不动 pack 账（防双计）
- ✅ GC 空 pack 整删后 `drop_pack_stat` 销账（P3 预铺路径接通）
- ✅ 测试：`duostore_backend_suite_all_pack`（强制全 pack + 小 pack_max_size 高频
  轮转）与默认/多 chunk 变体同套件全绿；meta_store_suite 增 pack 账两用例
  （rocks/redis/sqlite/tikv 共享）；专项覆盖分流/chunked 缓冲/轮转封存/record
  格式落盘/crc 位腐/空 pack 整删 vs pin/重启弃用
- 验收：默认/redis/sqlite/rados/tikv 五种构建单测全绿（redis 真实 server、
  tikv 真实 tiup playground 集群，rados 无集群恒 SKIP）；e2e_duostore ×
  {rocksdb,redis,sqlite,tikv} 全绿；asan 3 连跑、tsan 2 连跑零告警（顺带修复
  P3 改 `peek_reclaims` 签名后 test_duostore_tikv 三处具体类调用的编译遗留）

### 1.3 P4 · GC 二期（✅ 已完成，2026-07-29）

全部条目落地：

- ✅ pack 压实：`FsDataStore::rewrite_pack()` 顺扫（record 解析；损坏语义分级——
  magic/头坏告警止扫、payload crc 坏告警跳过、torn tail 静默止扫不计损坏）+
  `PackMigrateFn` 迁移回调（标准实现 `migrate_pack_record`：owner 反查存活 →
  payload 追加回 active pack → `swap_extents` 乐观换 ref，swap 失败清理 chunk 残留；
  误判恒保守不迁，pack 删除只走"live 归零 + 空 pack 整删"，绝不丢数据）
- ✅ 压实调度进 `run_gc_once()`：`pack_gc_ratio` 存活率阈值分选 + 崩溃遗留 seal(0)
  的 file_size 回填；`compact_blocked_` 记账 + gc_grace 冷却窗防无效重扫（进行中
  mpu 分片/旧格式 owner/存活损坏 record，账有推进立即重试）；mpu owner 增 b/k
  （"mpu\0b\0k\0id\0no"，complete 后分片仍可反查；P4 前旧格式保守搁置）
- ✅ 空 pack 整删改延迟 unlink（空置逾 gc_grace 且无 pin 才删，服务压实/删除瞬间
  已持旧 ref 未及 pin 的读者）
- ✅ 孤儿扫描：接口定形 `IDataStore::scan_chunks(cb)`（fs 实现目录顺扫；rados 显式
  抛错留 C4，绝不静默空扫谎报）+ `IMetaStore::scan_refs(cb)`（4 meta 全实现，弱一致
  快照）；正向 = 无引用 + mtime 逾 grace + 无 pin + `chunk_referenced` 现点复查 →
  unlink；反向 = refs 在而文件缺 → 告警计数绝不删 meta（与 GC 同信号量互斥保
  "文件先于 ref 存在"论证）；独立低频定时器 `orphan_scan_interval`（默认 1d，0 关）
  + `run_orphan_scan_once()` 手动钩子
- ✅ 写侧 pin（`ChunkPinHooks`）：ChunkWriter 分配 file_id 即 pin，meta 提交/兜底
  删除后调用方对称解除（`WritePinRelease`）——慢流式 PUT 的早期 chunk mtime 可远逾
  grace，仅靠 mtime 宽限不充分
- ✅ 崩溃注入测试：mini_test 增子进程模式（execv /proc/self/exe + SIGKILL），4 专项
  ——提交后崩溃全恢复 / PUT 中途崩溃孤儿扫描无垃圾 / 删除后崩溃 GC 收敛 / 随机
  kill -9 已报告提交全保
- ✅ 指标：压实 3 项（packs_compacted/records_migrated/pack_corrupt_records）+
  孤儿 3 项（scans/chunks_removed/refs_missing gauge）
- 验收：默认/redis/rados/tikv/asan/tsan 六种构建单测全绿（redis 真实 server、tikv
  真实 tiup playground 集群 9 用例、rados 无集群恒 SKIP）；asan 3 连跑、tsan 2 连跑
  零告警；e2e × {rocksdb,redis,sqlite,tikv} 各 49 用例全绿

### 1.4 P5 · 打磨（✅ 已完成，2026-07-29）

全部条目落地：

- ✅ RocksDB 调参外露：`rocksdb_write_buffer` / `rocksdb_max_write_buffers` /
  `rocksdb_max_background_jobs` 三键（默认 = RocksDB 自身默认，既有部署行为不变；
  压缩恒关 §13.3 不外露），from_params 范围校验（≥1）+ 引擎归属 WARN 表 +
  lights3.yaml 样例与 §11 配置表同步
- ✅ corruption 计数指标：`lights3_duostore_read_corruption_total`——GET 读路径
  crc 失配经 `on_corruption` 回调递增，fs chunk / fs pack / rados 三处接入；回调
  只捕获计数器 shared_ptr，reader（持 options 拷贝）逃逸出 backend 生命周期仍安全。
  GC 顺扫损坏计数（pack_corrupt_records_total）已随 P4 落地
- ✅ `e2e_tiered_duostore`：run_e2e.sh 增 tiered-duostore 场景（localfs local +
  duostore cloud + tiered 组合），CMake 注册；复证 §13.1「duostore 可作 tiered
  cloud 侧」（cloud 只经 IStorageBackend 抽象）
- ✅ 文档状态头更新：duostore-backend.md 状态头（P1-P5 全部完成）+ §11 配置表 +
  §15 表；storage-backend.md §5 去掉「P2-P4 规划」措辞
- 验收：默认 ctest 矩阵 12/12 全绿（含新 e2e_tiered_duostore）；默认/redis/rados/
  tikv/asan/tsan 六种构建单测全绿（新增 corruption 指标 + 调参解析 2 用例；tikv
  真实 tiup playground 集群、rados 无集群恒 SKIP）；asan 3 连跑、tsan 2 连跑零
  告警；e2e × {rocksdb,redis,sqlite,tikv} 各 49 用例全绿

## 2. 各插拔引擎的打磨尾巴

### 2.1 Redis meta · R4（duostore-redis-meta.md §10）（✅ 已完成，2026-07-30）

- ✅ AOF 探测告警：已随 R1 构造函数在场（`CONFIG GET appendonly` 非 AOF 打
  WARN、CONFIG 不可用降级提示），本期核对无需改动
- ✅ `redis_wait_replicas`（默认 0，范围 [0,256]）：>0 时提交类命令成功后同
  连接追加 `WAIT <n> <timeout/2>`；副本不足仅 WARN 不报错（写已在主上生效，
  报错会误导客户端重试）——§6 语义说明同步
- ✅ 指标：`redis_cas_retries_total` / `redis_reconnects_total` 经
  MetricsScope 接入（RedisMetaOptions 增 metrics 字段，构造期注册 0 值可见）
- ✅ `list_uploads` HGETALL → HSCAN 分批（COUNT 512，游标弱一致可接受，
  map 去重 + 排序）；TLS 评估结论落 §5.5（维持不启用，开启路径已记录）
- ✅ 文档：状态头 R1-R4 完成、§10 表 R4 行、§8 配置表/样例、lights3.yaml
  样例（顺修 `redis_uri: tcp://` 应为 `redis://` 的样例笔误）。§10 表中
  R1/R2/R3 状态此前已修正为「已完成」，原记录的矛盾不复存在
- 验收：redis 构建 181 用例全绿（新增 4 专项：wait_replicas 容忍/配置校验、
  CLIENT KILL 重连计数 + 提交类 InternalError 边界、HSCAN 跨批完整性）；
  默认构建全绿；e2e duostore-redis 全绿

### 2.2 SQLite meta · S4（duostore-sqlite-meta.md §10）（✅ 已完成，2026-07-30）

- ✅ 崩溃模拟专项：子进程（execv 自身，sync=true）逐提交回报、父进程随机 SIGKILL；
  重启后回报过的提交必在、refs↔objects 双向对账收敛、gcq 无幻账、号段不回退、
  integrity_check 干净（进程级 harness 复用 mini_test ChildRegistrar）
- ✅ list 一致视图注入：`set_list_pause_for_test` 钩子中途并发提交（插入/删除/
  覆盖），本次 list 的 WAL snapshot 不动、下次 list 见新态
- ✅ `sqlite3_backup` 在线备份评估：维持不做——WAL 下外部只读连接（sqlite3 CLI
  `VACUUM INTO`/`.backup`）即可在线取一致快照，结论落 §6.3
- ✅ `PRAGMA optimize` / checkpoint 调优：自动策略不加码，补 `journal_size_limit=4MiB`
  截断 WAL 高水位；optimize 维持仅 close() 收尾跑（结论落 §6）
- ✅ 指标：`sqlite_busy_total` / `sqlite_corruption_total` 经 MetricsScope 接入
  （SqliteMetaOptions 增 metrics / busy_timeout_ms 字段，构造期注册 0 值可见；
  含打开路径 NOTADB）
- ✅ 残留开放事务连接改「先补 ROLLBACK、成功回池」（原保守直接销毁）；构造失败
  清理走非优雅 shutdown（不跑 optimize/checkpoint，避免重复计 corruption）
- 验收：sqlite 构建 184 用例全绿（新增 4 专项）；默认构建全绿；
  e2e_duostore_sqlite 全绿；崩溃专项 5 连跑稳定

### 2.3 RADOS data · C3 / C4（duostore-rados-data.md §12）

- C3：aio 协程桥接（completion → 先 reschedule 回本进程 executor/池，再继续业务逻辑——
  §6.2 纪律）+ 双缓冲流水（写第 N 片时接收 N+1）；对象级 read-ahead 评估
- C4：孤儿扫描——接口已随主线 P4 定形（`IDataStore::scan_chunks`，rados 侧目前
  显式抛错），待实现 rados_nobjects_list_* + rados_stat 枚举；多网关 GC 单实例执行配置
  与分布式 pin 方案评估（租约 / rados_lock / watch-notify，§8.3）；op 延迟/错误指标
- ~~C2 遗留：GC 变现 / pin 竞态专项测试（前置：主线 P3）~~ ✅ 已随 P3 补齐
  （test_duostore_rados.cc，无集群 SKIP）

### 2.4 TiKV meta · T5（duostore-tikv-meta.md §11）（✅ 已完成，2026-07-30）

- ✅ **GC safepoint 推进**（§7.3）：侧车封装 PD RPC 三件（UpdateServiceGCSafePoint /
  UpdateGCSafePoint / GetGCSafePoint，经 getLeaderUrl 直连 leader 自建 stub）；
  TikvMetaStore 后台 worker 每 `tikv_gc_interval`（默认 60s，0=关）三步推进：注册
  本网关 service safepoint（now−`tikv_gc_retention`）→ 以无限 TTL 接管 `gc_worker`
  角色（PD 对缺失 gc_worker 以 0 永久占位，不推它 min 恒被钉死——实测坐实）→
  以全服务 min 推进集群 safepoint；多网关并发经 PD min/单调语义收敛
- ✅ 万分片 complete 专项：`duostore_tikv_bulk_complete_10k_parts`（10000 分片、
  8 线程残差类并行上传、complete 期间并发读者驱动 TTL 判定），实测 ~0.5s；
  侧车 lock_ttl 按上游 txnLockTTL 语义伸缩（ttlFactor·√MiB，[3s,20s]）；
  TTLManager 心跳评估结论：不接线（事务规模有上界，S3 万分片顶格），结论落 §6.3。
  核实结果：T4 未跑过此专项，本期补齐
- ✅ Poco 日志收敛：`PocoSpdlogChannel` 桥接 root logger 到统一 stderr 格式
  （源名前缀保留；information 起送）；超时参数化：`tikv_backoff_ms` 覆盖侧车
  路径退避预算（上游 Snapshot/Scanner 内部不受控，全局参数化仍列上游项）
- ✅ 指标：`tikv_txn_conflict_retries_total` / `tikv_safepoint_update_failures_total` /
  `tikv_gc_safepoint_ms` 经 MetricsScope 接入（TikvMetaOptions 增 metrics 字段，
  构造期注册 0 值可见）
- ✅ 文档措辞核实：§7.3/§10.4 的 fork 描述此前已同步为 in-tree 侧车，无残留
- 尾巴（依赖上游流程，无法本地销）：上游 PR——2PC mutation op 扩展回馈 +
  `Snapshot::Get` not_found 重载 + 指针升级（当前靠 `@78a557e` 错误消息字面量
  匹配兜底，升级指针须复查，duostore-tikv-meta.md §11 末注）
- 验收：tikv 构建 181 用例全绿 ×3（新增 3 专项）+ 无集群 SKIP 路径绿；
  build-tikv ctest 11/11（含 e2e_duostore_tikv）；默认构建 ctest 12/12

## 3. 横切基础设施（一处未做卡住多处）

### 3.1 后端级 metrics 注册机制（✅ 已完成，2026-07-28）

~~现有 Metrics 只有 L2 请求维度，无后端级注册（cloudproxy-backend.md §8.2 明确指出需先
扩展框架）。~~框架已落地（`src/core/metrics.{h,cc}`）：

- `MetricsRegistry`：counter / gauge / histogram / 回调 gauge 四类件，
  同名同标签 get-or-create（幂等）、同名异类型/异桶界抛装配错误；
  Prometheus 文本渲染（家族分组、# HELP/# TYPE、标签转义、输出序稳定）
- `MetricsScope`：per-backend 注册句柄，`StorageRegistry::build` 给每个后端派发
  `backend=<name>` 基础标签的 scope（`BackendFactory` 增第三参），`with()` 可派生
  子件维度；默认空 scope 返回孤立实例——测试直构后端零装配成本
- 装配：main 建注册表 → build 注入 → `S3Service::set_backend_metrics`，
  `GET /-/metrics` 在 L2 请求指标之后追加渲染（未注入则行为不变）
- 示范消费者：duostore GC 计数 5 项（runs/reclaims/files_removed/packs_removed/
  uploads_expired，P5 指标项的 GC 切片）经 scope 注册，构造期即注册故 0 值可见
- 验收：test_metrics.cc 8 用例 + service/duostore 端到端断言；默认/asan/tsan/
  rados/redis 五种构建单测全绿，e2e duostore 全绿，真实进程 /-/metrics 实测输出

它解锁以下指标项（各随所属阶段排期，接入方式见 storage-backend.md §6）：

- cloudproxy P4 指标（远端请求计数/时延、重试、错误映射、ETag 校验失败、ClientPool 等待）
- ~~duostore P5（corruption 计数；GC 计数已随本项示范落地）~~（✅ 已随 P5 落地：
  read_corruption_total）、redis R4、sqlite S4、rados C4、tikv T5 的各自指标项
- s3-protocol.md §7 规划的「按后端分维度、后端错误率」

### 3.2 per-backend 独立 ThreadPool

concurrency.md §3.1 预留（Registry 构造注入接口已留）：云端慢请求占满共享池会饿死
本地盘路径；cloudproxy 目前只用私有 pump 线程局部规避。可与 §3.1 一起作为独立特性。

### 3.3 cloudproxy P4 剩余

- §8.2 指标（见上）
- §2.3 `control_in_pump: true` 配置项 + 压测定默认值
- `force_path_style: false`（virtual-hosted style）：目前配置加载期直接报错
  （`src/storage/cloudproxy/remote_client.cc:88-91`），低优先
- 已知权衡记录：create_multipart 重试可能留空孤儿 upload（建议远端配生命周期规则，§5.2）

### 3.4 tiered 剩余

- §9 对账工具（P4 剩余）：默认每日双向 diff——云端有本地无 → 重建 stub 或删除；
  本地 remote 云端无 → 告警
- GC 重试指数退避（目前简化为按轮周期重试）
- 演进项：抽象 local 侧接口以支持 duostore 作 local 侧（目前绑定 localfs 磁盘布局，
  duostore-backend.md §13.1）

### 3.5 凭证管理二期（credential-management.md §9）

- SK at-rest 加密（master key 来自环境变量，AES-256-GCM；`version` 字段已预留升级路径）
- 外部 IdP / 文件热加载 provider
- 多实例失效同步（定期增量 reload 或管理面广播，§7）
- per-credential policy

## 4. S3 协议层缺口（s3-protocol.md）

明确列为二期/不支持、返回 NotImplemented 的（集中拒绝表见 `src/s3/service.cc:36-52`，
27 个子资源）：

- **UploadPartCopy**（§1 明确「二期」，`src/s3/handlers/multipart.cc:59`）——协议层最近的一个
- versioning（含 CopyObject ?versionId、ListObjectVersions）、ACL/policy、website、
  lifecycle、tagging、CORS、SSE-C/KMS、Object Lock、replication、notification 等
- PUT `If-None-Match` 仅支持 `*`（`src/s3/handlers/objects.cc:122-125`）
- 多段 Range（含逗号）不支持（`objects.cc:20`）
- presigned 未做 15min 时钟偏移检查（`src/s3/auth/sigv4.cc:421`）
- §8 测试策略提到跑 MinIO mint 兼容集作回归门槛——未见落地，需核对/排期

## 5. 工程与测试缺口

### 5.1 配置样例严重滞后（✅ 已完成，2026-07-26）

~~`config/lights3.yaml` 仅 26 行、只示范 localfs。duostore（meta/data 及各引擎参数）、
cloudproxy、tiered 完全缺失，用户只能从 `tests/e2e/run_e2e.sh` 反查写法。~~
已补全带注释的全后端样例：duostore 四种 meta（rocksdb/redis/sqlite/tikv）与两种
data（fs/rados）引擎全参数、cloudproxy、tiered、buckets.rules 均以注释形式给出
默认值与取值范围；样例已实测可解析启动（localfs 默认 + 全后端取消注释两种形态）。

### 5.2 build.sh 开关不同步（✅ 已完成，2026-07-26）

~~CMake 有 10 个 `LIGHTS3_*` 选项，build.sh 只暴露 `--seastar` 与 `--tikv`。
缺 `--redis` / `--sqlite` / `--rados`；rados 需系统 librados 也无提示。~~
已补 `--redis` / `--sqlite` / `--rados` 三开关（粘性语义同 `--seastar`），usage 注明
rados 需系统 librados（librados-dev 或 `LIGHTS3_RADOS_ROOT`）、建议 `-B build-rados`
隔离；`--rados` 已在 build-rados 目录实测构建通过。

### 5.3 测试覆盖缺口

- **3 个未验证组合**：redis×rados、sqlite×rados、tikv×rados（配置可写、构造可跑，
  但零单测零 e2e；`run_e2e.sh` 的 duostore-rados 场景恒用默认 rocksdb meta）
- 零单测模块：~~`src/core/timer`~~（✅ 已随 P3 补齐，test_timer.cc）、
  `src/storage/bucket_router`、`src/storage/listing`、`src/storage/validate.cc`、
  `src/s3/handlers/admin_credentials.cc`、`src/http/pushpull.h`、
  `src/storage/xlocalfs/uring`
- ~~`pack_stats()` 未进共享套件~~（✅ 已随 P2 进 meta_store_suite）；~~`rewrite_pack()`
  仍未进~~（✅ 已随 P4 补：scan_refs 进 meta_store_suite，rewrite_pack/压实/孤儿/
  崩溃注入专项进 test_duostore.cc）
- 环境依赖用例：rados 8 个 + tikv 9 个在裸环境恒 SKIP，redis 8 个取决于
  redis-server——共约 17% 用例默认不执行，CI 若要覆盖需专门环境
- tiered 隐性耦合：tiered 走两阶段构建、不在 registry map 内，"unknown type" 检查
  对它不生效（`src/storage/registry.cc:93-125`），可顺手加固

### 5.4 其他代码内已注明的技术债（列为「注明不做」，勿误当 bug）

- RocksDB/SQLite meta：锁内 fsync，写吞吐上限 ≈ 1/fsync 延迟；升级路径分别为
  TransactionDB / group commit，均「仅注明不做」（`rocks_meta_store.h:96`、
  `sqlite_meta_store.h:117`）
- Redis Cluster 不支持（`redis_meta_store.h:5`，明确非目标）
- YAML parser 简化版：不支持 tab 缩进/flow style/锚点/多行标量；引号内 `" #"` 解析错误
  （`src/core/config.cc:59,61`）——如踩到再升级
- codec `reason` 字段预留，覆盖/删除/abort 暂未区分（`src/storage/duostore/codec.cc:406`）
- cloudproxy 无 content-length 上行不做 TRAILER 组帧，恒 NotImplemented
  （`cloudproxy_backend.cc:404-407`）

## 6. 文档一致性修复（✅ 已全部完成，2026-07-26）

1. ✅ `docs/README.zh-CN.md` 文末「未实现」5 项（Multipart、CopyObject、DeleteObjects、
   cloudproxy、aws-chunked）实为已实现——已改为真实缺口（UploadPartCopy/versioning 等）；
   驱动（补 seastar）、认证（补 aws-chunked/凭证管理）、存储与 S3 API 列表已补全
2. ✅ `docs/README.md` 索引已补 4 篇 duostore 子文档、todo.md 与 README.zh-CN.md 指引；
   去掉 tiered/duostore「（设计稿）」；「首期两种后端」与架构图（CivetWeb 等）已更新
3. ✅ `docs/duostore-redis-meta.md` §10 表 R1-R3 状态改「已完成」
4. ✅ `docs/duostore-tikv-meta.md` §7.3 / §10.4 fork 措辞改 in-tree 侧车
5. ✅ `docs/architecture.md`：§1 目标表补「duostore 可选外部 meta/data 服务」、非目标
   区分网关单实例与外部系统扩展；§6 目录树补 duostore、CMake 开关补 `LIGHTS3_DUOSTORE*`
6. ✅ `docs/storage-backend.md` §5 去「设计稿」，补 P1/P2-P4 状态与 4 篇子文档链接
7. ✅ `docs/concurrency.md` §3.1 per-backend 池预留已回填指向本清单 §3.2
8. ✅（顺带）根目录 `README.md`：存储列表补 CloudProxy/Tiered/DuoStore，
   「Not implemented」去掉已实现的 cloudproxy、补 UploadPartCopy

## 7. 建议推进顺序

1. ~~**文档一致性修复（§6）+ 配置样例（§5.1）+ build.sh 开关（§5.2）**——半天级，先清零~~ ✅ 已全部完成（2026-07-26）
2. ~~**DuoStore P3 GC 一期（§1.1）**——生产可用性硬阻塞；先补 `src/core/timer` 单测~~ ✅ 已完成（2026-07-26）
3. ~~**DuoStore P2 pack 聚合（§1.2）**——解锁四个 meta store 的 pack 账与全 pack 测试变体~~ ✅ 已完成（2026-07-26）
4. ~~**后端级 metrics 框架（§3.1）**——一次解锁六处指标项~~ ✅ 已完成（2026-07-28）
5. ~~**DuoStore P4（§1.3）**~~ ✅ 已完成（2026-07-29；枚举接口已定形）→ **rados C4
   孤儿扫描**（`RadosDataStore::scan_chunks` 实现）可随 C3/C4 排期
6. 各引擎打磨（~~R4~~ ✅ 2026-07-30 / S4 / C3 / T5）与 tiered 对账、凭证二期按需排期
   （~~P5~~ ✅ 已完成，2026-07-29）
