# 优化与功能规划（Roadmap）

> 基于 2026-08-25 的全仓走读（源码 + 设计文档 + git 历史）。此前的评审底账
> `gaps.md` / `issues.md` 已全部清零并归档至 [archive/](archive/gaps.md)，
> 本文是接续它们的新规划底账：
> 只收录"尚未做"与"值得做"的事，不重复已落地能力（能力边界见根
> README「Current scope」与 [s3-protocol.md](s3-protocol.md) §1）。
>
> 每项标注 **价值**（高/中/低）与 **难度**（低/中/高）。条目按主题分节，
> 末节给出跨主题的优先级梯队。文中行号为写作时快照，仅作定位提示。

## 目录

1. [立即修：bug 级问题与文档漂移](#1-立即修bug-级问题与文档漂移)
2. [S3 协议层](#2-s3-协议层)
3. [存储层](#3-存储层)
4. [运行时与 HTTP 层](#4-运行时与-http-层)
5. [可观测性](#5-可观测性)
6. [测试 / 工具链 / 分发](#6-测试--工具链--分发)
7. [明确不建议近期做的事](#7-明确不建议近期做的事)
8. [优先级梯队](#8-优先级梯队)

---

## 1. 立即修：bug 级问题与文档漂移

这一节的条目要么是可复现的行为缺陷，要么违反了项目自己确立的工程原则
（"静默吞掉即撒谎，宁可 501"，见 `src/s3/service.cc` 拒绝面注释），
成本都很低，应最先处理。

| # | 条目 | 现状 | 价值 | 难度 | 入口 |
| --- | --- | --- | --- | --- | --- |
| 1.1 | ~~aws-chunked trailer 校验和完全不验~~ **已完成（2026-08-26）** | -TRAILER 两变体的 trailer 段现按行严格解析并与 `x-amz-trailer` 声明双向对照，声明的校验和对解码后 payload 校验，signed 变体验 `x-amz-trailer-signature`（见 [s3-protocol.md](s3-protocol.md) §3.2/§3.3） | — | — | `src/s3/auth/sigv4.cc`、`src/s3/checksum_guard.h` |
| 1.2 | ~~配置校验缺口簇~~ **已完成（2026-08-27）** | ① `max_header_size` 限 `[1KiB,1MiB]`（远低于 beast `header_limit(uint32_t)` 截断阈值）；② `idle_timeout` 限 `[1s,86400s]`，`0` 因驱动语义相反直接拒绝；③ `http.port` 改走 `to_int`（尾随垃圾报错）；④ `log.level` 白名单 debug\|info\|warn\|error，拼错启动即报错；⑤ `buckets.rules[].match/backend` 空值拒绝；⑥ 跨项检查：`transfer_stall_timeout > request_timeout`（且后者启用）启动报错 | — | — | `src/core/config.cc`、`tests/unit/test_config.cc` |
| 1.3 | ~~`X-Amz-Security-Token` 静默忽略~~ **已完成（2026-08-27）** | query/header 两侧现均在 verify 之前被 `reject_sts_token` 显式拒绝（501 NotImplemented，报文点名 STS 不支持），token 已从 `kCommonQueryKeys` 白名单移除；真做 STS 见 §2.8 | — | — | `src/s3/service.cc`（`reject_sts_token`） |
| 1.4 | ~~stall guard 与 `io_chunk_size` 下界耦合可误杀~~ **已完成（2026-08-27）** | 进度阈值参数化为 `min(kMinProgressBytes, io_chunk_size)`（app.cc 装配处收敛），`io_chunk_size` 配到 4KiB 时不再每 window 误判停滞 | — | — | `src/http/stall_guard.h`、`src/http/admission.h`、`src/app/app.cc` |
| 1.5 | ~~请求延迟直方图上界 10s，P99 失真~~ **已完成（2026-08-27）** | `kLatencyBuckets` 增加 30/60/300 三桶，覆盖到 `request_timeout` 默认值（300s）的大对象区间 | — | — | `src/s3/metrics.h` |
| 1.6 | ~~文档漂移~~ **已完成（2026-08-27）** | ① 中英 README 的"明确不支持"清单已移除 website 并补上静态网站托管能力项；② `gaps.md` / `issues.md` 已从 git 历史恢复归档为 `docs/archive/`，全仓 380+ 处引用批量改为 `docs/archive/<name>.md` 路径；③ `cli.md` 中英两版 `add_conn_opts` 笔误已改为 `add_conn_flags` | — | — | `docs/archive/`、`README.md`、`docs/README.zh-CN.md`、`docs/cli.md` |
| 1.7 | ~~`x-amz-checksum-algorithm` 头静默忽略~~ **已完成（2026-08-26）** | `x-amz-checksum-algorithm` / `x-amz-sdk-checksum-algorithm` 现强制校验：未知算法或"声明了却没提供对应摘要"→ InvalidRequest（无 body 的声明放行） | — | — | `src/s3/checksum_guard.h` |

## 2. S3 协议层

**本节全部条目已完成（2026-08-28）。** 现有拒绝面（子资源黑名单 + 头前缀
拒绝 + per-route query 白名单反转）结构健全，未支持能力都会正确回 501 而
非静默错误，扩展时保持了这一纪律。

### 2.1 ~~高价值：CORS（website 的头号配套缺口）~~ **已完成（2026-08-28）**

`?cors` 三 handler + XML 编解码 + OPTIONS 预检（验签之前分派）+ 实际请求
响应头注入均已落地；持久化经由从 WebsiteStore 模式提取的
`SysConfigStore<Traits>` 模板（`.sys/cors/<bucket>` write-through +
tombstone 同步，lifecycle 复用同一模板）。root 专属，同 ?website 两级模型。
入口：`src/s3/handlers/bucket_cors.cc`、`src/s3/cors_store.*`、
`src/s3/sys_config_store.h`。

### 2.2 checksum 闭环（配合 §1.1 一次做完）

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| ~~`crc64nvme` 算法~~ **已完成（2026-08-26）** | 头部与 trailer 两种形态均支持（`util::crc64nvme_update` + `checksum_spec` 表项） | — | — |
| ~~校验和持久化 + 回显~~ **已完成（2026-08-28）** | `ObjectMeta` 增 checksum_algorithm/value/type + part_sizes（trailer 形态经 pending 槽在 body 读尽后落值）；六后端序列化齐备（duostore 走自描述 kv 零迁移、part 记录 v2）；GET/HEAD 认 `x-amz-checksum-mode: ENABLED`；multipart 复合 `-N` 由已验证分片值计算（CRC64NVME/FULL_OBJECT → 501） | — | — |

### 2.3 网站托管收尾（[static-website.md](static-website.md) §"未尽事项"）

| 条目 | 价值 | 难度 |
| --- | --- | --- |
| ~~`GET /prefix` 无尾斜杠时 302 补斜杠~~ **已完成（2026-08-28）**（`prefix/<index>` 存在时 302，否则保留原错误） | — | — |
| ~~`RedirectAllRequestsTo` / `RoutingRules`~~ **已完成（2026-08-28）**（XML/JSON 全量；prefix 规则取对象前评估、错误码规则先于 error 文档；见 [static-website.md](static-website.md) §6） | — | — |
| ~~per-bucket 限速~~ **已完成（2026-08-28）**（`website[].max_rps` 令牌桶，超限轻量 XML 503，签名请求不受限） | — | — |
| ~~website 匿名面的专项单测~~ **已完成（2026-08-28）**（签名材料检测、路由限定、`response-*` 拒绝、302/重定向规则/限速全覆盖，`test_service.cc`） | — | — |

### 2.4 ~~Lifecycle 最小子集~~ **已完成（2026-08-28）**

`?lifecycle` API（root 专属）+ `LifecycleRunner` 周期扫描
（`lifecycle.scan_interval`，默认 1h）：`Expiration.Days` 过期删除 +
`AbortIncompleteMultipartUpload.DaysAfterInitiation` 清僵尸 MPU（prefix 过
滤）；Transition/tag 过滤/Date 形态 → 501。入口：`src/s3/lifecycle.*`、
`src/s3/handlers/bucket_lifecycle.cc`。

### 2.5 其他中小项

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| ~~Object Tagging~~ **已完成（2026-08-28）** | `x-amz-tagging` 写入即持久化（canonical 编码走 kStdMetaFields 非回显行）；`?tagging` 三 API + `x-amz-tagging-count`；就地改标签 duostore 诚实 501（无 meta 原地更新原语），其余后端可用 | — | — |
| ~~policy 创建后不可改~~ **已完成（2026-08-28）** | `PUT /-/admin/credentials/<ak>` 改 policy/comment（`policy: null` 清除）；落盘对象带 `rev` 计数 + 实例侧 storage ETag，sync 据此传播编辑 | — | — |
| ~~ListParts 缺 `encoding-type`~~ **已完成（2026-08-28）** | 白名单放行 + Key URL 编码 + EncodingType 回显 | — | — |
| ~~`list-type=3` 静默当 V1~~ **已完成（2026-08-28）** | 未知 list-type → InvalidArgument | — | — |
| ~~`GET object?partNumber=N`~~ **已完成（2026-08-28）** | complete 记录 `part_sizes`；206 + Content-Range + `x-amz-mp-parts-count`；cloudproxy 远端 HEAD ?partNumber 解析；旧 multipart 对象（无布局）诚实 501 | — | — |
| ~~`MissingContentLength`(411) 死码~~ **已完成（2026-08-28）** | PutObject/UploadPart 缺 Content-Length/Transfer-Encoding → 411 | — | — |
| ~~ListMultipartUploads 仅支持 `delimiter="/"`~~ **已完成（2026-08-28）** | 分组本就是通用 substring 实现，撤掉 L2 的 501 限制 | — | — |

### 2.6 ~~长期：STS 会话凭证~~ **已完成（2026-08-28）**

`AssumeRole` 端点（`POST /` form、service scope `sts`）+ 带 TTL 的内存会
话凭证表（L3SA 前缀，policy 继承调用者、session 不能再 assume）；数据面
token 验证（不符 InvalidToken / 过期 ExpiredToken），
`X-Amz-Security-Token` 回归 query 白名单。会话表单实例内存态（会话短命，
多实例共享留作后续）。入口：`src/s3/handlers/sts.cc`、
`src/s3/auth/credential_store.*`、`sigv4.*`。

## 3. 存储层

存储层是全项目完成度最高的部分（duostore GC/压实/孤儿扫描/租约矩阵齐全），
以下是残留设计边界与新增可优化面。

### 3.1 ~~数据完整性：scrub / fsck（最大的单项运维缺口）~~ **已完成（2026-08-28）**

① duostore `run_scrub_once`：meta 驱动（chunk 无头无尾、crc 只在 manifest，
盘面驱动无从校验）读回全部对象与进行中 mpu 分片的 extent、独立于
`verify_chunk_crc` 重算 crc32c，refs 台账双向对账（三关复查防并发误报）；
② localfs/xlocalfs `run_scrub_once`：ETag 当校验和全量 verify（multipart
按 part_sizes 重算复合 ETag，无布局存量对象诚实 unverifiable）；③ CLI 两
面：离线 `lights3 fsck <backend>`（照 dump/load 模式，发现 → 退出码 1）+
在线 `s3adm fsck <bucket>`（S3 API 端到端，GET ?partNumber 逐分片）。均纯
只读，`--max-mbps` 限速（`scrub_throttle.h`，TimerQueue 分片睡眠）。admin
端点未做（CLI 已覆盖，触发面留观）。见 [cli.md](cli.md) §2.3/§3.5、
[storage/duostore-core.md](storage/duostore-core.md) §8.4、
[storage/localfs.md](storage/localfs.md) §11。

### 3.2 ~~后台任务 CLI 化（钩子现成，接线即可）~~ **已完成（2026-08-28）**

`lights3 duostore gc|scan <backend>` 与 `lights3 tier scan|gc|reconcile
<backend>`（tiered 的 run_gc_once 一并接上），照 dump/load 离线模式跑一轮即
退，统计进日志、退出码 0/1（完整性裁决归 `lights3 fsck`）。本地 meta 引擎
持文件锁须停服；redis/tikv 可与在线网关并行（GC 租约协调）。见
[cli.md](cli.md) §2.4。

### 3.3 ~~cloudproxy 网络面~~ **已完成（2026-08-28，六项全部）**

① 退避协程化：控制面重试环重构为 `retry_io` 协程模板（每 attempt 经
control_io 阻塞发送，轮间 `core/timer.h:async_sleep` + 回池，退避不再睡池
线程；pump 私有线程保留阻塞退避）；② 连接池卫生：空闲项带时间戳
（`pool_idle_timeout` 逾期绝不复用 + TimerQueue reaper 静默期回收）+
`pool_max_lifetime` 按龄退休，`total_` 随之收缩；③ `Retry-After`（整秒/
HTTP-date 两形态，钳 [0,60s]）覆盖公式退避；④ 熔断器（连续 N 次传输/5xx
失败开闸快败 SlowDown + 半开单探针，`breaker_threshold/cooldown`）+
`op_deadline_ms`（只裁剪重试环，不掐在飞传输）；⑤ `acquire_async`（池满
waiter 挂起等 release 交接，TimerQueue 兑现超时契约，不停池线程）；
⑥ AWS 凭证链（AK/SK 未配置时：环境变量 → ECS/EKS 容器端点 → EC2 IMDSv2，
会话 token 进签名、到期前 5min 续期、负缓存 60s，`imds_endpoint` 可指测试
桩）。COPY 顺带纳入重试（幂等 PUT）。见
[storage/cloudproxy.md](storage/cloudproxy.md) §2.3.1/§2.4/§7.2、
[cloudproxy-backend.md](cloudproxy-backend.md) §5.2/§7/§8.1。

### 3.4 ~~xlocalfs：io_uring 的未兑现收益~~ **已完成（2026-09-01，五项全部）**

① 单流多在途流水线：新增 `storage/xlocalfs/uring_stream.h` ——
`UringReadStream`（read-ahead 环形缓冲，`read_depth` 默认 4）与
`UringWriteStream`（hold-back 写流水线，`write_depth` 默认 4）；多在途下的
析构安全用"引用计数共享块（缓冲 + fd + fixed 槽），每个在途 op 持一票"重建：
中途弃流不做取消，弃置的 op 自然完成后由末票释放资源。② fixed
buffers/files：建环时 `IORING_REGISTER_BUFFERS`（每 ring
`fixed_buffers`×`block_size`，流块直接取注册缓冲走 READ/WRITE_FIXED，池尽或
注册被拒静默退堆块）与稀疏 `IORING_REGISTER_FILES` + FILES_UPDATE
（≥512KiB 的流临时注册 fd）。③ linked SQE + 元数据 opcode：写流 finish 时
末块 WRITE→FSYNC 链一次提交（短写自动回退独立 fsync）；GET 的 open、提交期
rename + 目录 fsync、part rename、DELETE 的 unlink、complete 分片存在性
statx 全部上 ring（`meta_ops: false` 一键回退同步）。④ 多 ring 分片：
`rings`（0=auto=核数/8）个独立 SQ/CQ/收割线程，fixed 资源按 ring 作用域，
流固定单 ring、散 op 轮询。⑤ duostore io_uring 版 FsDataStore：
`fs_uring: true`（chunk 读写走流水线 + 封存链式 fdatasync；pack 批尾
fdatasync 经 dup fd 出锁上 ring；引擎建失败回退同步路径 + 常驻 gauge
`lights3_duostore_uring_fallback`），布局不变。见
[storage/xlocalfs.md](storage/xlocalfs.md)、
[storage/duostore-data-fs.md](storage/duostore-data-fs.md) §9。

### 3.5 ~~localfs / listing~~ **已完成（2026-09-01，六项全部）**

① listing 元数据并行：遍历只收集本页 key，随后按 `list_meta_concurrency`
（默认 8）条带分给池线程 `when_all` 并行 stat+getxattr；readdir 与 stat 之
间被删的 key 从本页消失而非让整个 LIST 报 NoSuchKey。② 分页：新增
`storage/localfs/list_cache.h:DirListCache`——每个目录的排序条目表按目录
inode+mtime/ctime 缓存（一次 stat 校验、2s racy 窗口不入缓存、LRU 预算
`list_cache_entries`），翻页起点 `partition_point` 二分，深页成本不再随页码
上升；孤儿 sidecar 判定改用同一次 readdir 的名字集合（省掉每 sidecar 一次
stat）。③ xattr 降级可见：构造期 `probe_meta_xattr`，常驻 gauge
`lights3_localfs_xattr_fallback` + 计数器 `…_xattr_write_failures_total`，
`require_xattr: true` 探测失败即拒绝启动、写失败在 rename 前抛错。④ 写路径：
`sidecar: sync|async|lazy`——async 把 sidecar 挪到后台补写、lazy 在 xattr 成
功时不写（关键路径 2 fsync + 1 rename），xattr 失败时两者退回同步写；
xlocalfs 的 ring 提交同享。⑤ 孤儿 sidecar 定期扫描：
`run_sidecar_sweep_once` 挂 `schedule_periodic`（默认 1d），删除在该 key 的
commit_lock 下复查后进行（listing 自愈同路径），杜绝误删刚落地对象的 sidecar。
⑥ redis：`uz:<b>` 词序 ZSET 索引，`ZRANGEBYLEX` 下推游标与 prefix、`HMGET`
取值，`ZCARD≠HLEN` 时回退全表 HSCAN 并重建索引；顺带修正四个 meta 引擎
`list_uploads` 的下推契约（SPI 增 `prefix` 提示 + 仅 key-marker 的"整 key 已翻
过"语义），此前 rocks/sqlite/tikv 在 prefix 列举下可能误报列举结束。见
[storage/localfs.md](storage/localfs.md) §2/§3/§6/§12、
[storage/duostore-meta-redis.md](storage/duostore-meta-redis.md) §5.2。

### 3.6 ~~tiered~~ **已完成（2026-09-02，七项全部）**

⑥ 先行：local 侧抽象为 `storage/tiered/tier_local.h:ITierLocal`（读状态 /
访问记录 / 上传快照 / stub 与缓存回填两条提交原语 / 空间探针 / 全量枚举 /
可选块缓存），`LocalFsTierLocal`（localfs/xlocalfs）与 `DuoStoreTierLocal`
（tier 状态进对象记录 v3，stub = 无 extent 记录，提交 = `PutCondition`
CAS 元数据事务，旧 extent 同事务入 gcq）两个适配器，`from_config` 按类型选
择；`DuoStoreBackend::get_object` 对 stub 抛 `StubRace`。① 访问记录改随对象
持久化（localfs xattr `user.lights3.access`，无 xattr / duostore 退常驻表），
后端只留写后缓冲；`<state>/wheel/<hour>` 时间轮按 `atime+cold_after` 登记，
常态扫描只消费到期槽（成本正比活动量），`full_scan_interval`（默认 1d）全量
兜底（未登记对象/崩溃恢复/配额校准/块缓存清理）。② `rules[]`（`bucket/key`
glob → `cold_after`，`never` 钉住，config.cc 把后端下的 map 列表拍平成
`rules.N.*`）。③ 淘汰候选来自时间轮（≈LRU，读到 4×缺口即止），排序键
`(rank, score)`，`score = age × (1 + size_w·log2(1+MiB)) / (1 + freq_w·hits)`。
④ 对账隔离区 `<state>/quarantine/`（refs_missing / foreign 首次告警、之后只累
计、完整轮后自动销账），`lights3 tier quarantine list|forget|purge`，gauge
`lights3_tiered_quarantine_entries{kind}`。⑤ 即 ① 的访问记录改造（常驻表仅作
无 xattr 与 duostore 的兜底）。⑦ `range_cache`：`<state>/rcache/` 稀疏文件 +
位图，Range 命中块对齐超集回填、全命中本地服务、尾块同 read 吸完，
PUT/回填/换代失效，水位淘汰当 rank 0 残留。见
[tiered-storage.md](tiered-storage.md) §4.3/§5.1/§6.3/§9、
[storage/tiered.md](storage/tiered.md) §3/§4.1/§5.4/§11。

### 3.7 duostore 残留 **已完成（2026-09-03，全部五项）**

| 条目 | 落地 |
| --- | --- |
| ~~`DuoGcStats` 接入 metrics~~ | 早在 gaps §6.1 已全量接线（`lights3_duostore_gc_*` 计数器/gauge 族，完整轮末折算），本表此前未更新；本次仅清掉陈旧头注释 |
| ~~损坏 pack 隔离区~~ | 连续 3 次"corrupt>0 且零迁移且账目不动"入持久化隔离账本（`<root>/quarantine/`），停冷却重扫；账目一动自动释放；CLI `lights3 duostore quarantine list\|release\|purge` + `packs_quarantined` gauge（duostore-core.md §8.6） |
| ~~TiKV 事务层生产化~~ | "自建 2PC 层"选项早已由 tikv_client sidecar 落地（乐观 2PC、Put/Del/Lock/Insert 四 op、kvrpcpb 结构化冲突分类＋字符串匹配仅作纵深、Undetermined 禁盲重试），头注释里 "test-grade" 指上游 client-c 而非本仓实现，本表此前误读；向上游贡献结构化错误码仍是可选项（duostore-tikv-meta.md：上游 PR 列为 T5 遗留） |
| ~~多网关 read-lease~~ | 各网关周期发布"最老在途读开始时间"（redis SET PX / tikv 'L' 表，TTL 崩溃自愈；本地引擎 unsupported 即停摆），GC 只回收 `enqueue_ms < 全体最小租约` 的 gcq 项与见空早于下限的空 pack；孤儿扫描改为跳过 gcq 在途 chunk 以关闭同一竞态（duostore-core.md §8.5） |
| ~~meta 在线备份~~ | dump 走 `IMetaStore::snapshot()` 一致性视图（rocksdb Snapshot / sqlite WAL 读事务 / tikv 固定 TSO），写不停机；redis 无 MVCC 保持停写契约并 WARN；load 契约不变（duostore-core.md §11）。增量/PITR 仍为未做的后续方向 |

### 3.8 横切：元数据缓存层 **已完成（2026-09-04，单实例层 + 共享 meta 的有界陈旧模式）**

~~全仓无任何对象/元数据缓存：每次 HEAD/GET 都是一次 stat+getxattr（localfs）
或一次 meta 引擎 RTT（duostore+redis/tikv）。单实例可做 per-backend 分片
LRU + 写路径 invalidate；多网关共享 meta 时需失效协议，可分期。~~

落地：`storage/meta_cache.h:MetaCache<V>`——按 (bucket,key) 分片（默认 64
条带）的 LRU，可选 TTL，**回填令牌**（miss 时取分片失效代，insert 时代已
变则丢弃）关闭"读旧记录→被覆盖写抢先提交并失效→回填旧记录"的经典竞态；
指标 `lights3_meta_cache_lookups_total{result}` / `_invalidations_total` /
`_entries`（backend 标签区分实例）。两个后端各自接入：

- **localfs/xlocalfs**：缓存 `ObjectMeta + tier + stat 戳`（dev/ino/size/
  mtime/ctime）。HEAD 命中默认仍做一次 stat 校验戳（省掉 getxattr / sidecar
  读 + TSV 解码；`meta_cache_validate=false` 则零 syscall），GET 用已持有
  fd 的 fstat 校验（免费），戳不符即失效重读——因此**外部进程改写同一
  root 也不会被喂陈旧记录**。put/copy/complete/delete/tagging 与 xlocalfs
  的 ring 提交在提交点后经 RAII 守卫失效；tiered 的 stub/回填提交调
  `invalidate_object_meta`。默认 64K 条。
- **duostore**：缓存整条 `ObjectRec`（HEAD 只填 meta 段，GET 升级为含
  manifest 的整记录；>256 extent 的记录不缓存），命中的 GET/HEAD **零 meta
  RTT**。本地引擎（rocksdb/sqlite）默认开、精确失效（put/delete/complete/
  tier 提交守卫；压实 swap 后整表清空；dump load 后清空）。共享引擎
  （redis/tikv）默认关：开启必须 `0 < meta_cache_ttl < gc_grace`（对端网关的
  写在 TTL 内不可见，且陈旧 manifest 必须早于其 extent 可被对端 GC 回收前
  过期），read-lease 发布值再回拨一个 TTL，使对端 GC 不回收本网关缓存
  manifest 仍可能引用的 extent。

**分期保留**：跨网关失效协议（redis pub/sub / tikv 无等价物）未做，共享
meta 场景以 TTL 有界陈旧为契约。见 [storage/localfs.md](storage/localfs.md)
§5.1、[storage/duostore-core.md](storage/duostore-core.md) §7.1。

### 3.9 链条：用量统计 → 配额 → 多租户 **已完成（2026-09-04，四项全部）**

~~三者是同一条依赖链，越往后越重，建议按序分期：~~ 落地文档
[multi-tenancy.md](multi-tenancy.md)，全部实现在 L2，存储层与 meta schema
零改动：

1. ~~**用量统计**（价值高/难度中）：`IStorageBackend` 无任何 usage 接口，
   想知道桶的对象数/字节数只能全量 List 累加；指标只到后端粒度。方案二选
   一：meta 侧增量计数器（四个 IMetaStore 实现都要动、正确性成本高）或离
   线聚合扫描（可与 §3.1 scrub 同一次遍历产出）。~~ 选了"离线聚合"路线的
   改良版：`UsageTracker`（`src/s3/usage.{h,cc}`）在每条写路径提交后做增量
   （覆盖/删除前 HEAD 取旧尺寸，body 实际字节计数），周期 flush 到
   `.sys/usage/<bucket>`，`usage.reconcile_interval` 全量列举校准 + 启动
   bootstrap + `s3adm usage --rescan`；多网关只采纳更新的扫描。精度契约
   "两次扫描之间近似"（multi-tenancy.md §2.4）。指标
   `lights3_bucket_usage_{bytes,objects}{bucket}`。
2. ~~**桶级/租户级配额**（中-高/高）：现唯一的 "quota" 是 tiered 本地盘水位，
   语义完全不同。依赖 ①，还需定超限错误码与 MPU 半途语义。~~ `?quota`
   子资源（`.sys/quota/<bucket>`，root 管理）+ 租户记录里的聚合配额
   （bytes/objects/buckets）；错误码 **`QuotaExceeded`(403)**；MPU 语义：
   分片计入 `mpu_bytes` 受限，Complete 仅在完成后仍放不下时拒绝且上传保留
   （multi-tenancy.md §3.4）。
3. ~~**租户实体化**（中-高/高）：现模型是"一个 root + 多个受限 AK"（policy
   glob 隔离已含列举过滤），无桶归属、无 Owner、无分级 admin。适合内部部
   署，不适合真多租户 SaaS；牵动 meta schema 与全部 IMetaStore，动之前先
   想清目标场景。~~ 目标场景定为"叠加而非替换"：`.sys/tenants/<id>` +
   `.sys/owners/<bucket>`，凭证加 `tenant`/`role`，租户凭证只见本租户所有
   桶（dispatch 归属判定 + ListBuckets 过滤 + 真实 `Owner`），无归属记录的
   桶对 root/legacy 凭证行为不变；分级 admin（租户 admin 管本租户凭证/用量）；
   `/-/admin/tenants`、`s3adm tenant`。meta schema 未动。
4. ~~**审计日志**（高/中）：现全仓审计面只有"请求明文 SK 时一条 WARN"。凭证
   生命周期事件 + 数据面结构化访问日志（见 §5.2）是合规前置，也是 ① 的数
   据源之一。~~ `audit.path` 独立 rotating JSON 行文件：凭证/租户/配额/归属
   生命周期、STS、建删桶、配额拒绝、rescan；`audit.data_plane` 每请求一条
   `access`（§5.2 的运行日志异步化与慢日志已于 2026-09-05 完成）。

## 4. 运行时与 HTTP 层

### 4.1 TLS（部署硬伤级）**已完成（2026-09-05，四项全部）**

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| ~~**builtin / seastar 无 TLS**~~ | 已完成：builtin 在连接线程用 OpenSSL 阻塞 I/O 包裹 socket；seastar 经 `seastar::tls` 包裹每 shard 的 listener（[tls.md](tls.md) §3/§4） | — | — |
| ~~证书热重载~~ | 已完成：`tls_reload_interval` 轮询证书文件，变更即整套重载、新握手生效、失败保留旧素材；seastar 用可重载凭证（自带文件监视） | — | — |
| ~~TLS 旋钮~~ | 已完成：`tls_client_ca` + `tls_client_auth`（mTLS，暂不映射身份）、`tls_min_version`、`tls_ciphers` / `tls_ciphersuites`、`tls_sni` 多证书（seastar 不支持 SNI，构造期抛错）——三个 OpenSSL 驱动共用一个证书回调层 `src/http/tls.h` | — | — |
| ~~短期替代~~ | 已完成：[tls.md](tls.md) §6 nginx / caddy 反代终结样例（透传 Host、`X-Forwarded-Proto`、关请求体缓冲） | — | — |

**分期保留**：mTLS 客户端证书 → 凭证/租户身份映射；SIGHUP 触发重载（轮询已够）。

### 4.2 超时与连接治理 **已完成（2026-09-05，五项落地，末项按原计划维持）**

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| ~~**超时体系拆分**~~ | 已完成：`header_timeout` / `idle_timeout` / `body_timeout` / `write_timeout` 四类，四驱动各接一遍（builtin 阶段边界重设 socket 超时、beast 逐操作 `expires_after`、seastar 按阶段武装定时器、httplib 头部阶段由 body_timeout 约束），超时按阶段计入 `lights3_http_timeouts_total{phase}`（[http-adapter.md §2.2](http-adapter.md)） | — | — |
| ~~keep-alive 每连接请求数上限~~ | 已完成：`http.max_requests_per_connection`（默认 1024，0=不限），第 N 个响应带 `Connection: close`；指标 `lights3_http_keepalive_closes_total` | — | — |
| ~~连接上限观测~~ | 已完成：`IHttpServer::stats()` → `lights3_http_connections_total{result=accepted\|rejected_limit}`、`lights3_http_connections_active`（httplib 跑上游 accept 循环，报零） | — | — |
| ~~per-IP / per-AK 限流~~ | 已完成：`ratelimit.*`（令牌桶 + 并发上限，LRU 有界表），per-IP 在验签前、per-AK 在验签后，超限 `503 SlowDown` + `Retry-After: 1`，指标 `lights3_ratelimit_rejections_total{scope}`（[http-adapter.md §2.3](http-adapter.md)） | — | — |
| ~~`io_threads` 语义漂移~~ | 已完成（文档化矩阵路线）：[http-adapter.md §2.2](http-adapter.md) 的四驱动矩阵 + 各驱动启动日志打印实际含义 | — | — |
| 客户端断连独立取消源 | 文档明示的刻意取舍（长 handler 靠 `request_timeout` 兜底），**维持** | 中 | 高 |

### 4.3 数据面性能（按投入产出排序）

1. **流式响应双缓冲/预取**（高/中）：现读后端与写 socket 严格交替，吞吐
   ≈ 1/(读延迟+写延迟)；rados 数据面已有双缓冲流水范式可抄。注意
   `BodyReader` 契约要求串行单消费者，需在驱动内做而非改契约。
2. **缓冲区池化**（高/中）：每个流式响应现场 `std::vector` 分配并**零初始
   化** 64KiB，四驱动各一份；thread_local 池即可（builtin 是
   thread-per-connection，天然适配），同时是 §3.4 fixed buffers 的前置。
3. ~~**异步日志 sink**~~（高/低）**已完成（2026-09-05）**：见 §5.2——访问日志现是热路径上每请求一次同
   步 stderr write。
4. **sendfile 零拷贝**（高/中-高）：`http-adapter.md` §1 已预留
   `try_as_file()` 设计出口，接口未加实现未做；localfs 大对象 GET 收益显
   著，TLS 路径需保留回退。
5. **builtin 流式写纳入 pumping**（中/中）：`write_response` 现每 64KiB 一
   次裸 `sync_wait`（condvar + 双向线程跳转），1GiB 对象 = 16384 次；请求
   body 侧已用 `sync_wait_pumping`，补齐对称面。
6. **beast `ResumeOn` 快速路径**（中/低）：已在同一 strand 时跳过
   `asio::post`。
7. **per-bucket 指标去锁**（中/中）：`add_bytes_in/out` 每 64KiB 进一次全
   局 mutex（注释自认"a tier below lock-free"）；sharded map 或
   per-request 聚合后一次提交。
8. `HeaderMap` 线性扫描、`BlockQueue` 双拷贝：绝对量小，低优先。

另：**性能基线缺失**（高/低）——仓内无任何存档的 benchmark 数据，"beast
是性能路径"是断言而非实测。跑一轮 `s3adm bench` × 4 驱动 × TLS 开关矩阵，
结果入库 `docs/`，后续优化才有对照（`bench` 需先支持 `--output=json`，见
§6.3）。

### 4.4 配置热重载 **已完成（2026-09-05，安全子集 + bucket 路由规则）**

~~现状：全项目唯一的热更新是凭证文件与 website 条目；`SIGHUP` 零命中，改日
志级别/超时/限流/路由规则都要重启。~~ 落地（[config-reload.md](config-reload.md)）：
`SIGHUP` / `POST /-/admin/config/reload` / `s3adm reload` 三入口共用
`Application::reload_config`——重新 `Config::load` 做与启动相同的全量校验，失败
整体拒绝；通过后应用可热更新子集（`log.level`、`request_timeout`、
`transfer_stall_timeout`、`max_inflight_requests`（`AsyncSemaphore::set_capacity`）、
`min_part_size`、`ratelimit.*`、TLS 证书素材）与 `buckets.rules`
（`BucketRouter::update` 原子换代，各持有者共享一张表）；其余改动逐项列入
`requires_restart` 并 WARN。driver/后端/`default_backend`/`auth.*` 明确不可重载；
后端实例增删仍"另议"。

### 4.5 其他 **前两项已完成（2026-09-05）；HTTP/2 维持长期**

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| ~~排空死线硬编码~~ | 已完成：许可排空死线即 `http.shutdown_grace`（与驱动连接排空同一个量），`AsyncSemaphore::wait_drained` 条件变量等待替代 20ms 轮询 | — | — |
| ~~关停失败退出码~~ | 已完成：后端 `close()` / 线程池 join 失败或在途请求超过排空死线 → `run()` 返回 `3`（[cli.md §2.1](cli.md)），进程管理器可察觉 | — | — |
| HTTP/2 | 全仓零命中；S3 SDK 主流仍 HTTP/1.1，CDN/L7 前置场景才需要——**维持长期**，前置代理终结 h2 见 [tls.md §6](tls.md) | 中 | 高（长期） |

## 5. 可观测性

### 5.1 API × 后端维度指标 **已完成（2026-09-05，两处挂账兑现）**

~~现延迟直方图全局单条（PutObject 的 P99 被 HeadBucket 稀释），访问日志只有
总耗时无后端耗时——无法回答"是哪个 API / 哪个后端在劣化、是网关排队慢还
是后端慢"。~~ 落地：`Route` 加 `name`（32 条路由各有 S3 API 名，
CopyObject/UploadPartCopy 按 `x-amz-copy-source` 细分）→
`lights3_api_requests_total{api,backend,class}` +
`lights3_api_request_duration_seconds{api,backend}`（backend 取自
`BucketRouter::backend_name`）；路由后端外包一层 `storage::MeteredBackend`
计时装饰器 → `lights3_backend_op_seconds{backend,op}` 直方图 +
`lights3_backend_errors_total{backend,op}`（后端错误率），并经请求取消令牌
上的 `RequestBackendStats` 把后端耗时回填到访问日志尾部
`api=… backend=<name>:<ms>`（[s3-protocol.md §7](s3-protocol.md)）。

### 5.2 日志体系 **已完成（2026-09-05，三项全部）**

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| ~~**异步 sink + 轮转**~~ | 已完成：`log.file`（空 = stderr，否则 `rotating_file_sink` 按 `max_size`/`max_files` 轮转，每秒定时 flush）、`log.async` + `async_queue` + `async_overflow`（block/drop）走 `async_logger`，`Logger::shutdown` 在应用停机末尾排空队列；`core/log.cc` | 高 | 低 |
| ~~慢请求日志~~ | 已完成：`log.slow_request_threshold`（可热更新）——总耗时达阈值的访问行升 WARN 并附 `auth/handler/backend/ttfb/total` 阶段耗时；`log.level: warn` 下仍可见 | 高 | 低 |
| ~~结构化访问日志~~ | 已完成：文本行 `path`/UA 加引号转义，新增 `remote/bucket/ttfb/ua` 槽；`log.format: json` 时整条运行日志每行一个对象，访问记录平铺 `request_id/remote/ak/method/path/query/bucket/key/status/bytes/ms/ttfb_ms/auth_ms/handler_ms/backend_ms/backend_calls/api/backend/ua`；流式响应在响应体读尽时写行（实际字节数 + 含传输的总耗时，中断标 `truncated`）；[s3-protocol.md §7](s3-protocol.md) | 中 | 低-中 |

### 5.3 指标补口

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| L1 层零指标 | 连接数、accept/拒绝、keep-alive 复用率、TLS 握手、解析失败全缺；`MetricsScope` 现成 | 高 | 低 |
| admission/stall 指标 | 排队时长 histogram、停滞掐断计数均无（现仅三个瞬时 gauge） | 中 | 低 |
| 精确状态码 | 现仅 2xx-5xx 大类，200/204/206/304 不可分（206/304 比例是网站/CDN 场景关键指标） | 中 | 极低 |
| website/匿名面指标 | 匿名读、index 改写、error 回退、301 全无计数 | 中 | 极低 |
| `/-/metrics` 匿名可达 | 含 bucket 名等业务信息；加 root gate 或独立 admin 端口 | 中 | 低 |

### 5.4 分布式 trace

全仓无 traceparent/otel 痕迹，只有自生成 `x-amz-request-id`。多跳架构
（网关→cloudproxy 远端 / tiered / duostore meta+data）下排障刚需。轻量方
案先行：接受并透传 W3C `traceparent` + 注入日志关联字段；otel-cpp 全量埋
点作为长期项。
**价值：高；难度：低（透传）/ 高（otel）。**

### 5.5 监控消费侧（零 C++ 改动）

`/-/metrics` 已是规范 Prometheus 文本格式、`lights3_*` 命名统一，但一张
dashboard、一条告警规则都没有。补 `deploy/grafana/lights3.json` +
`deploy/prometheus/{scrape.yml,lights3.rules.yml}`（5xx 率、P99、GC 未收
敛、tiered 水位、cloudproxy 重试率、`xlocalfs_uring_fallback=1` 等）。
**价值：高；难度：低——投入产出比最高的单项。**

## 6. 测试 / 工具链 / 分发

注：本项目已明确不使用 GitHub Actions CI，回归全靠脚本手工触发；下面的
"自动化"均指本地脚本/ctest 层面。

### 6.1 测试缺口

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| **mint 挂进 ctest** | `run_mint.sh` 已写好（无 docker 自动 SKIP），但 CMake 无对应 `add_test`——"写完了没挂上门禁"。以 `s3cmd awscli` 子集起步并记录基线通过率 | 高 | 低 |
| **website e2e 零覆盖** | `run_e2e.sh` 中 website 零命中；匿名可读开关这种安全敏感面完全无回归保护（同 §2.3 单测项） | 高 | 低 |
| `s3adm` 零测试 | e2e 全用 curl 手工签名，s3adm 的自签客户端与服务端是两份实现、无交叉验证；替换部分 e2e 步骤为 s3adm 调用即可一石二鸟 | 中-高 | 低 |
| fuzz | 零 fuzz 目标；高价值面全是无认证可触达的解析器：SigV4/aws-chunked、XML、URI、builtin HTTP 解析、duostore codec。asan 基建已有，libFuzzer harness 每个 20 行 | 高 | 低-中 |
| 故障注入体系化 | 真崩溃注入只在 duostore 单测内；磁盘满/EIO/redis 断连/rados 超时/TiKV region error 无系统性注入（依赖树里已有 libfiu 可用） | 高 | 中 |
| 压力/长稳 | 无带阈值断言的性能回归、无数小时 soak（内存增长/fd 泄漏/pack 空间放大/GC 收敛都是长时间才暴露的） | 高 | 中 |
| coverage / ubsan | build.sh 有 asan/tsan，无 `--coverage`、无 ubsan（几乎零成本） | 中 | 极低 |
| 一键矩阵脚本 | 6 个构建目录 × 14 个 e2e 变体靠人记得轮转；加 `scripts/check-all.sh` 串起增量构建 + ctest 矩阵 | 中 | 低 |

### 6.2 s3adm / 运维命令扩展

| 条目 | 说明 | 价值 | 难度 |
| --- | --- | --- | --- |
| ~~`s3adm fsck/scrub`~~ **已完成（2026-08-28）** | 见 §3.1（`s3adm fsck` + `lights3 fsck`） | — | — |
| ~~`lights3 duostore gc/scan`、`tier scan/reconcile`~~ **已完成（2026-08-28）** | 见 §3.2（含 `tier gc`） | — | — |
| `s3adm object inspect` | 打印对象内部布局（pack/chunk/offset/CRC/tier 归属）；现排障只能读日志或 hexdump | 中-高 | 低-中 |
| `s3adm mpu list/abort` | 清理僵尸 MPU；服务端 API 已有，纯 CLI 包装 | 中 | 低 |
| `lights3 --check-config` | 校验逻辑已完整（test_config.cc 345 行），只差不 open backend 的 dry-run 出口 | 中 | 极低 |
| `s3adm bench --output=json` | 现只有人读表格，无法做基线比对（§4.3 性能基线的前置） | 中 | 低 |
| ~~`s3adm usage`~~ **已完成（2026-09-04）** | 见 §3.9①（`s3adm usage [bucket] [--rescan]`，另有 `quota`/`tenant` 命令组） | — | — |

### 6.3 构建与分发

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| **`--version` / 版本号** | `project()` 无 VERSION、二进制无版本输出、无 git commit 嵌入——现场无法确认在跑哪个构建 | 中 | 极低 |
| CMake install target | `install()` 零命中，`cmake --install` 不可用；是打包的前置 | 中-高 | 低 |
| CPack deb/rpm | 现分发 = clone + 全套构建依赖；install.sh 已手工实现的 user/unit/conffile 逻辑可迁给 deb 钩子 | 中-高 | 中 |
| Dockerfile + compose | 零容器化（而 run_mint.sh 自己都依赖 docker）。compose 一箭双雕：上手 demo + 让 redis/tikv/rados 三条**长期被 SKIP 的 e2e 路径**真正跑起来 | 高 | 低-中 |
| uninstall/回滚脚本 | install.sh 原子替换但不保留上一版 | 低-中 | 极低 |

## 7. 明确不建议近期做的事

| 条目 | 理由 |
| --- | --- |
| Versioning 全量实现 | 架构级变更（六后端 key 布局/List 语义/delete marker/GC 全动）。若未来要做，**从 duostore 侧切入成本最低**（meta 是 KV，加 version 维度即可），localfs 的 key→路径映射天然容纳不下多版本 |
| Object Lock / Legal Hold | 无 versioning 地基，WORM 语义无法成立 |
| Bucket Policy（IAM 语言） | per-credential policy 已覆盖多租户隔离，匿名公开桶已由 website 面解决；IAM 求值器是独立子系统，投入不成比例 |
| SigV2 | AWS 已停用，客户端基本绝迹 |
| presigned POST | 需先写半个 multipart/form-data 流式解析器；CORS + presigned PUT 是更现代的替代路径 |
| cloudproxy 出方向 streaming 签名上传 | 文档已论证：复杂度高、收益仅是明文 HTTP 下的完整性 |
| rados 数据面 pack 层 | 代码内长注释已论证为设计边界（小对象放大交给 BlueStore `min_alloc_size`） |
| CivetWeb 等新 HTTP 驱动 | 文档明示不在计划内，四驱动已覆盖设计空间 |
| GitHub Actions CI | 项目已明确移除、不使用；自动化投入放在本地脚本矩阵（§6.1） |

## 8. 优先级梯队

### P0 — 立即（bug 修复与文档一致性，合计约一周）

1. ~~trailer 校验和空洞 + `crc64nvme` + `x-amz-checksum-algorithm`（§1.1/§2.2/§1.7，一次闭合新版 SDK 上传路径）~~ **已完成（2026-08-26）**
2. ~~配置校验缺口簇（§1.2）+ stall guard 误杀（§1.4）+ 延迟桶上界（§1.5）~~ **已完成（2026-08-27）**
3. ~~`X-Amz-Security-Token` 显式 501（§1.3）~~ **已完成（2026-08-27）**
4. ~~文档漂移三件套：README website、gaps.md 断链归档恢复、cli.md 笔误（§1.6）~~ **已完成（2026-08-27）**
5. `--version`、`--check-config` 极低成本小项（ListParts encoding-type 已随 §2.5 完成，2026-08-28）

### P1 — 近期（高价值 / 低-中难度）

1. Grafana dashboard + Prometheus 告警规则（§5.5，零 C++）
2. mint 挂 ctest + website e2e/单测 + s3adm 进 e2e（§6.1）
3. ~~API×后端分维指标 + 后端耗时（§5.1）~~ **已完成（2026-09-05）**；~~异步日志 + 慢日志（§5.2）~~ **已完成（2026-09-05）**
4. ~~CORS + OPTIONS 预检（§2.1）；网站 302 补斜杠（§2.3）~~ **已完成（2026-08-28，§2.3 全项一并）**
5. ~~cloudproxy 协程化退避 + 连接池回收 + Retry-After（§3.3，含熔断/deadline/异步 acquire/凭证链）~~ **已完成（2026-08-28）**
6. ~~后台任务 CLI 化（§3.2）~~ **已完成（2026-08-28）**；~~DuoGcStats 接指标（§3.7）~~ **已完成（早于 2026-09，gaps §6.1 时接线）**；~~xattr 降级 gauge（§3.5）~~ **已完成（2026-09-01）**
7. fuzz 起步（XML/SigV4/URI 三个 harness）+ ubsan/coverage（§6.1）
8. Dockerfile + compose（§6.3）
9. 性能基线入库（§4.3；前置 bench --json）

### P2 — 中期（需设计，一个方向一个迭代）

1. ~~scrub/fsck（§3.1）~~ **已完成（2026-08-28）**；~~顺带的用量统计（§3.9①）未做~~ **已完成（2026-09-04，L2 增量 + 全量列举校准，未复用 scrub 遍历）**
2. ~~超时体系拆分 + L1 指标 + 连接治理（§4.2/§5.3）~~ **§4.2 已完成（2026-09-05）；§5.3 的 L1 连接/超时指标随之落地，其余指标补口仍待做**
3. 缓冲池化 + 流式双缓冲 + sendfile（§4.3；与 §3.4 fixed buffers 联动）
4. ~~builtin/seastar TLS + 证书热重载（§4.1）~~ **已完成（2026-09-05，含 mTLS/SNI/cipher 旋钮与反代样例）**
5. ~~配置热重载安全子集（§4.4）~~ **已完成（2026-09-05，含 bucket 路由规则）**
6. ~~Lifecycle 最小子集（MPU 自动清理）+ Object Tagging（§2.4/§2.5）~~ **已完成（2026-08-28）**
7. ~~localfs listing 优化（§3.5）~~ **已完成（2026-09-01）**；元数据缓存层（§3.8）
8. ~~tiered 扫描增量化 + prefix 策略（§3.6）~~ **已完成（2026-09-02，含全部七项）**
9. 故障注入体系 + soak（§6.1）；traceparent 透传（§5.4）
10. install target + CPack（§6.3）；~~审计日志（§3.9④）~~ **已完成（2026-09-04）**

### P3 — 长期 / 架构级（先想清目标场景再动）

1. xlocalfs 单流多在途流水线 + fixed buffers（§3.4）
2. ~~配额 → 租户实体化（§3.9②③）~~ **已完成（2026-09-04，§3.9 四项全部）**
3. ~~STS 会话凭证（§2.6）~~ **已完成（2026-08-28）**
4. ~~TiKV 事务层生产化 / 多网关 read-lease / meta 在线备份（§3.7）~~ **已完成（2026-09-03，§3.7 全部五项）**；meta 增量备份/PITR 留作后续
5. versioning（duostore 侧切入）、SSE-C/SSE-S3（§2.2/§7）
6. tiered local 侧抽象化容纳 duostore、Range 部分缓存（§3.6）
7. OpenTelemetry 全量埋点（§5.4）、HTTP/2（§4.5）
