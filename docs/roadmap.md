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

现有拒绝面（子资源黑名单 + 头前缀拒绝 + per-route query 白名单反转）结构
健全，未支持能力都会正确回 501 而非静默错误，扩展时保持这一纪律。

### 2.1 高价值：CORS（website 的头号配套缺口）

静态网站托管已完整落地，但 `?cors` 子资源 501、且 **`OPTIONS` 方法在分派表
中无条目**（落到 405 而非预检应答）——浏览器 JS/SPA 直连网关必然失败，也堵
死了 web 直传 presigned PUT 的路径。实现面：`?cors` 三个 handler + XML 编解
码 + OPTIONS 预检路由 + 响应头注入；持久化可完全复用 `WebsiteStore` 的
`.sys` write-through + tombstone 同步模式。
**价值：高；难度：中。** 入口：`src/s3/service.cc`、新增
`src/s3/handlers/bucket_cors.cc`、`src/s3/website_store.*`（作模板）。

### 2.2 checksum 闭环（配合 §1.1 一次做完）

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| ~~`crc64nvme` 算法~~ **已完成（2026-08-26）** | 头部与 trailer 两种形态均支持（`util::crc64nvme_update` + `checksum_spec` 表项） | — | — |
| 校验和持久化 + 回显 | `ObjectMeta` 无 checksum 字段；GET/HEAD 不回 `x-amz-checksum-*`，不支持 `x-amz-checksum-mode`，multipart 无 composite（`-N`）形式——"上传时校验、读取时可复核"的闭环只做了一半 | 中 | 中（扩 `ObjectMeta` + 各后端序列化；duostore codec 自描述 kv 段已有扩展先例） |

### 2.3 网站托管收尾（[static-website.md](static-website.md) §"未尽事项"）

| 条目 | 价值 | 难度 |
| --- | --- | --- |
| `GET /prefix` 无尾斜杠时 302 补斜杠到 `/prefix/`（AWS 行为；现直接进 error 文档，是站点最常见的体感差异） | 中 | 低 |
| `RedirectAllRequestsTo`（SPA 常用）/ `RoutingRules` | 低-中 | 低 / 中 |
| per-bucket 限速（匿名 GET 无签名成本，公开桶=免费带宽放大面；文档已自陈留待后续） | 中 | 中 |
| website 匿名面的专项单测（签名材料检测、路由限定、`response-*` 拒绝、index 改写各闸门；当前 `tests/` 无一条 website 用例） | 高 | 低 |

### 2.4 Lifecycle 最小子集

完整 Lifecycle 价值一般，但 **`AbortIncompleteMultipartUpload` 规则最实用
——当前僵尸 MPU 的暂存垃圾永不自动回收**。可先只做它 + `Expiration`，复用
`TimerQueue` + `BackgroundTaskGroup`。
**价值：中；难度：中。** 入口：`src/storage/multipart.*`、`src/core/timer.h`。

### 2.5 其他中小项

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| Object Tagging（`?tagging` + `x-amz-tagging`） | 501；是 Lifecycle 按 tag 过滤/成本分摊的前置 | 中 | 中 |
| policy 创建后不可改（无 update API） | 需先给落盘凭证对象加版本位使多实例同步能传播编辑（`WebsiteStore::sync_now` 有现成范式） | 中 | 中 |
| ListParts 缺 `encoding-type`（会被白名单打成 501） | SDK 偶发透传即 501 | 低-中 | 极低 |
| `list-type=3` 被静默当 V1 处理 | 应回 InvalidArgument | 低 | 极低 |
| `GET object?partNumber=N` + `x-amz-mp-parts-count` | 大对象并行下载的 SDK 优化路径 | 低-中 | 中 |
| `MissingContentLength`(411) 是死码 | 错误码表有、无人抛 | 低 | 极低 |
| ListMultipartUploads 仅支持 `delimiter="/"` | 其余 501 | 低 | 低 |

### 2.6 长期：STS 会话凭证

真实现（非仅 501）的价值在 EKS IRSA / assume-role 默认路径：`AssumeRole`
端点 + 带 TTL 的会话凭证表。`CredentialStore` 的三来源模型和
`ExpiredToken`/`InvalidToken` 错误码已就位，地基比看上去好。
**价值：中；难度：中。**

## 3. 存储层

存储层是全项目完成度最高的部分（duostore GC/压实/孤儿扫描/租约矩阵齐全），
以下是残留设计边界与新增可优化面。

### 3.1 数据完整性：scrub / fsck（最大的单项运维缺口）

现状拼图：duostore pack extent 读取恒校验 crc32c，但 chunk 的
`verify_chunk_crc` **默认关**且 Range 命中中段时无法校验；localfs/xlocalfs
**读路径零校验**（ETag 写时算、读时从不复核，静默位翻转不可发现）；孤儿扫
描只查存在性、不读内容；`s3adm`/`lights3` 均无 fsck/scrub/verify 子命令。
**当前网关无法自证数据完好。**

分步走：① duostore 侧成本最低——`scan_chunks` 枚举原语已存在，加"读 + 验
crc + 对账 refs"的遍历与限速；② localfs 侧把 ETag 当校验和用做全量 verify；
③ 挂到 CLI 与（可选的）admin 端点。
**价值：高；难度：中。** 入口：`src/storage/duostore/data_store.h`、
`fs_data_store.cc`、新增 `src/tools/s3adm_fsck.cc`。

### 3.2 后台任务 CLI 化（钩子现成，接线即可）

`run_gc_once` / `run_orphan_scan_once` / `run_reconcile_once`（tiered 对账）/
`scan_once`（tiered 判冷）目前只被定时器和单测调用，运维想立即回收空间只能
等定时器（GC 默认 5min、孤儿扫描 1 天）。照 `lights3 duostore dump|load` 的
模式加 `lights3 duostore gc|scan <backend>`、`lights3 tier scan|reconcile
<backend>`。
**价值：高；难度：低。** 入口：`src/main.cc`、
`src/storage/duostore/duostore_backend.h`、`src/storage/tiered/tiered_backend.h`。

### 3.3 cloudproxy 网络面

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| **退避改协程定时器** | `backoff()` 用 `std::this_thread::sleep_for` 阻塞池线程，最坏 700ms/请求；远端抖动时并发退避成片吃掉池容量。换 `co_await` TimerQueue 即可 | 高 | 低 |
| 连接池空闲回收 | `release()` 只 push 不带时间戳，无 reaper、无 max-lifetime；远端/NAT 关掉空闲连接后表现为周期性偶发重试尖峰；`total_` 只增不减 | 中 | 低 |
| 解析 `Retry-After` | 现为固定公式 + jitter | 中 | 低 |
| 熔断器 / per-op 总 deadline | 远端整体挂掉时每请求走满 `retry_max+1` 轮；GET 重试环最坏 `(retry_max+1)×60s` | 中 | 中 |
| `ClientPool::acquire` 异步化 | 现用 `cv_.wait_until` 同步等（有 histogram 观测，属自觉取舍）；可换 `AsyncSemaphore` | 中 | 中 |
| IMDS/STS 凭证链 | 只支持静态 AK/SK；EC2/EKS 上部署几乎必需 | 中-高 | 中 |

入口：`src/storage/cloudproxy/remote_client.{h,cc}`、`cloudproxy_backend.cc`。

### 3.4 xlocalfs：io_uring 的未兑现收益

当前只用了数据面最基本形态。按收益排序：

1. **单流多在途 op 流水线**（价值高/难度中-高）：现纪律是"每协程帧至多一个
   在途 op"，单个大对象 GET/PUT 严格 64KiB 串行、每块等一次 CQE——
   `queue_depth=256` 只在多请求并发时被填满。预提交 N 个后续读（read-ahead
   环形缓冲）是 io_uring 相对 pread 最大的收益点，但需重新论证多在途 op 的
   取消/析构安全性（当前简单性正是靠该纪律换来的，见
   [storage/xlocalfs.md](storage/xlocalfs.md)）。
2. **fixed buffers / fixed files**（中-高/高）：现无 `IORING_REGISTER_BUFFERS/FILES`，
   且缓冲住在协程帧栈上、不适合注册——依赖先引入缓冲池（与 §4.3 缓冲池化
   同一件事）。
3. **linked SQE + 元数据面 opcode**（中/中）：write+fdatasync 链一次提交；
   open/statx/rename/unlink 均有 uring opcode（文档中"无目录原语"仅对
   getdents 成立），现全走线程池同步调用。
4. **多 ring 分片**（中/中）：单 ring + `submit_mu_` 一把锁在高核数下是单点。
5. duostore 的 **io_uring 版 FsDataStore**（中/中）：文档明确"未实现"，
   `uring.h` 引擎可复用、布局不变。

入口：`src/storage/xlocalfs/uring.{h,cc}`、`xlocalfs_backend.cc`、
`src/storage/duostore/fs_data_store.cc`。

### 3.5 localfs / listing

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| **listing 每 key 一次 stat+getxattr** | `max_keys=1000` 一页 2000+ 次系统调用、串行在单个池线程。可并行化 `load_meta` 或加 per-directory 缓存（文档已把后者写作"留作后续"） | 高 | 中 |
| 分页无索引 | 翻页需沿路径重走 readdir+排序，深目录百万对象下成本随页码上升 | 中 | 中 |
| xattr 降级不可见 | 不支持 xattr 的 fs（NFS/部分 overlayfs）上退化为两步提交、留下"新数据+旧 sidecar"崩溃窗口，但只有一行启动 WARN。照 `xlocalfs_uring_fallback` 先例提升为常驻 gauge，或提供 fail-fast 开关 | 中 | 低 |
| 写路径 4 次 fsync + 2 次 rename | 小对象负载下元数据开销大于数据；xattr 已是权威读源，可考虑 sidecar 惰性/异步化选项 | 中 | 中 |
| 孤儿 sidecar 仅 listing 顺带清理 | 从不被 list 的目录永久残留；可挂进现有 MPU 孤儿清理定时器 | 低 | 低 |
| redis 侧 list_multipart_uploads 全表 HSCAN | 可加词序 ZSET 索引下推游标 | 中 | 低 |

### 3.6 tiered

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| **后台扫描全量遍历增量化** | `scan_once` 每周期完整 readdir+stat 整个 local root（超水位再来第二遍）。正路是持久化候选索引/时间轮，或直接用 atime 表产出候选 | 高 | 中-高 |
| prefix 级分层策略 | 现策略最细到 bucket（文档明示的取舍）；在 `scan_once` 判冷分支加 prefix 策略表即可，不动磁盘布局 | 中 | 中 |
| 冷热判定多维化 | 现仅"最后访问时间"单维；加大小加权/访问频次到 `Evict` 排序键即可 | 中 | 低 |
| 对账 quarantine | `orphans_skipped`/`refs_missing` 两类条目每轮重复告警，无隔离区、无人工处置入口 | 中 | 低 |
| atime 表全量常驻内存 + 全量 TSV 快照 | 千万级热对象下内存与快照成本可观 | 中 | 中 |
| local 侧支持 duostore | 现 `dynamic_cast<LocalFsBackend>` 硬绑；"duostore 热层 + 云冷层"需为 tiered 抽象 local 侧接口（stub 表示/tier 字段/原子提交原语） | 中 | 高 |
| Range GET 部分缓存 | 现读 1MB 可能触发整对象回迁；需块级索引+稀疏文件，文档已论证复杂度 | 中 | 高 |

### 3.7 duostore 残留

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| `DuoGcStats` 接入 metrics | 头注释自认 "for later metric wiring"；压实/孤儿统计现只能看日志 | 中 | 低 |
| 损坏 pack 隔离区 | 损坏记录使 pack 进 `compact_blocked_` 反复冷却重试、永久无法回收，无运维出口 | 中 | 中 |
| TiKV 事务层生产化 | `tikv_client.h` 自认 "commit layer is test-grade"（Put-only、错误路径含上游 TODO、写冲突靠字符串分类）。要么向上游 client-c 贡献结构化错误码，要么自建 2PC 层 | 高（若 TiKV 组合要投产） | 高 |
| 多网关 read-lease | 跨网关 GC 已有 `try_gc_lease`（redis/tikv），但"A 网关在读的 extent，B 网关 GC 看不到 A 的 pin"仍只靠 `gc_grace` 兜底；文档列为设计前提 | 中 | 高 |
| meta 在线备份 | dump/load 需停写；无增量/PITR | 中 | 高 |

### 3.8 横切：元数据缓存层

全仓无任何对象/元数据缓存：每次 HEAD/GET 都是一次 stat+getxattr（localfs）
或一次 meta 引擎 RTT（duostore+redis/tikv）。单实例可做 per-backend 分片
LRU + 写路径 invalidate；多网关共享 meta 时需失效协议，可分期。
**价值：高（小对象高 QPS）；难度：中。**

### 3.9 链条：用量统计 → 配额 → 多租户

三者是同一条依赖链，越往后越重，建议按序分期：

1. **用量统计**（价值高/难度中）：`IStorageBackend` 无任何 usage 接口，
   想知道桶的对象数/字节数只能全量 List 累加；指标只到后端粒度。方案二选
   一：meta 侧增量计数器（四个 IMetaStore 实现都要动、正确性成本高）或离
   线聚合扫描（可与 §3.1 scrub 同一次遍历产出）。
2. **桶级/租户级配额**（中-高/高）：现唯一的 "quota" 是 tiered 本地盘水位，
   语义完全不同。依赖 ①，还需定超限错误码与 MPU 半途语义。
3. **租户实体化**（中-高/高）：现模型是"一个 root + 多个受限 AK"（policy
   glob 隔离已含列举过滤），无桶归属、无 Owner、无分级 admin。适合内部部
   署，不适合真多租户 SaaS；牵动 meta schema 与全部 IMetaStore，动之前先
   想清目标场景。
4. **审计日志**（高/中）：现全仓审计面只有"请求明文 SK 时一条 WARN"。凭证
   生命周期事件 + 数据面结构化访问日志（见 §5.2）是合规前置，也是 ① 的数
   据源之一。

## 4. 运行时与 HTTP 层

### 4.1 TLS（部署硬伤级）

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| **builtin / seastar 无 TLS** | 配置 TLS 构造期直接抛错（刻意防静默明文），但**默认驱动 builtin 恰好是不支持的那个**，性能路径 seastar 同样没有 | 高 | 中（builtin 接 OpenSSL BIO；seastar 用 `seastar::tls`） |
| 证书热重载 | 证书轮换必须重启进程 | 高 | 中 |
| TLS 旋钮 | 仅 `tls_cert`+`tls_key` 两项：无 mTLS、无 cipher 选择、无 SNI 多证书、无 min_version | 中 | 中 |
| 短期替代 | 文档补 nginx/caddy 反代终结样例，成本极低 | 中 | 低 |

### 4.2 超时与连接治理

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| **超时体系拆分** | 单个 `idle_timeout` 撑起 header 读/body 读/写/keep-alive 空闲四类语义，"空闲连接 5s 回收"与"慢客户端 body 允许 300s"无法分开配置 | 高 | 中（拆三项、四驱动各接一遍） |
| keep-alive 每连接请求数上限 | 仅 httplib 有（1024）；其余三驱动 `while(keep_alive)` 无上限，妨碍 LB 重分片 | 中 | 低 |
| 连接上限观测 | `max_connections` 四驱动已统一，但拒绝时只有 WARN 无计数器 | 中 | 低 |
| per-IP / per-AK 限流 | 只有全局闸门，单客户端可占满全部配额 | 中 | 中 |
| `io_threads` 语义漂移 | 同一配置键在四驱动四种语义（beast=IO 线程 / httplib=池下限 / seastar=shard 数 / builtin=忽略） | 中 | 低（文档化矩阵或拆键） |
| 客户端断连独立取消源 | 文档明示的刻意取舍（长 handler 靠 `request_timeout` 兜底），暂维持 | 中 | 高 |

### 4.3 数据面性能（按投入产出排序）

1. **流式响应双缓冲/预取**（高/中）：现读后端与写 socket 严格交替，吞吐
   ≈ 1/(读延迟+写延迟)；rados 数据面已有双缓冲流水范式可抄。注意
   `BodyReader` 契约要求串行单消费者，需在驱动内做而非改契约。
2. **缓冲区池化**（高/中）：每个流式响应现场 `std::vector` 分配并**零初始
   化** 64KiB，四驱动各一份；thread_local 池即可（builtin 是
   thread-per-connection，天然适配），同时是 §3.4 fixed buffers 的前置。
3. **异步日志 sink**（高/低）：见 §5.2——访问日志现是热路径上每请求一次同
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

### 4.4 配置热重载

现状：全项目唯一的热更新是凭证文件与 website 条目；`SIGHUP` 零命中，改日
志级别/超时/限流/路由规则都要重启。`Config` 是纯值类型、`load()` 无状态，
地基良好；`CredentialStore::schedule_file_reload` 是现成范式。分期：先做
安全子集（`log.level`、`request_timeout`、`transfer_stall_timeout`、
`max_inflight_requests`、TLS 证书），bucket 路由规则次之（`BucketRouter`
可 `shared_ptr` 原子换代，后端实例增删涉及生命周期另议），driver/后端不可
重载。
**价值：高；难度：中。** 入口：`src/app/app.cc`、`src/core/config.cc`。

### 4.5 其他

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| 排空死线硬编码 | 关停排空是 20ms 轮询 + `constexpr 10s` 死线，不可配且与 `http.shutdown_grace` 是两个量 | 中 | 低 |
| 关停失败退出码 | backend close 失败只 LOG_ERROR、进程仍 0 退出 | 中 | 低 |
| HTTP/2 | 全仓零命中；S3 SDK 主流仍 HTTP/1.1，CDN/L7 前置场景才需要 | 中 | 高（长期） |

## 5. 可观测性

### 5.1 API × 后端维度指标（文档挂账两处"待接入"）

现延迟直方图全局单条（PutObject 的 P99 被 HeadBucket 稀释），访问日志只有
总耗时无后端耗时——无法回答"是哪个 API / 哪个后端在劣化、是网关排队慢还
是后端慢"。分派表的 `Route` 结构天然是 API 名的单一来源：加 `name` 字段 →
按 `(api, backend)` 分维直方图 + 访问日志加后端耗时槽，一次改动兑现两处挂
账。标签基数 = API 数 × 后端数，可控。
**价值：高；难度：中。** 入口：`src/s3/metrics.{h,cc}`、`service.cc`
（Route 表）、`src/storage/bucket_router.cc`。

### 5.2 日志体系

| 条目 | 现状 | 价值 | 难度 |
| --- | --- | --- | --- |
| **异步 sink + 轮转** | `Logger::init` 硬编码同步 `stderr_color_sink_mt`——高 QPS 下每条访问日志一次锁 + write syscall；spdlog 的 `async_logger` + `rotating_file_sink` 现成，配置项化即可 | 高 | 低 |
| 慢请求日志 | 无阈值通道：生产开 info 被淹没，开 warn 就没有访问日志。加 `log.slow_request_threshold`，超限升 WARN 并附阶段耗时 | 高 | 低 |
| 结构化访问日志 | 固定七字段空格分隔，`path` 未转义（含空格的 key 破坏切分），缺 remote_addr / UA / bucket / 后端名 / TTFB；无 JSON 选项，不利接 Loki/ELK；同时是审计（§3.9）与用量统计的数据源 | 中-高 | 低-中 |

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
| `s3adm fsck/scrub` | 见 §3.1 | 高 | 中 |
| `lights3 duostore gc/scan`、`tier scan/reconcile` | 见 §3.2 | 高 | 低 |
| `s3adm object inspect` | 打印对象内部布局（pack/chunk/offset/CRC/tier 归属）；现排障只能读日志或 hexdump | 中-高 | 低-中 |
| `s3adm mpu list/abort` | 清理僵尸 MPU；服务端 API 已有，纯 CLI 包装 | 中 | 低 |
| `lights3 --check-config` | 校验逻辑已完整（test_config.cc 345 行），只差不 open backend 的 dry-run 出口 | 中 | 极低 |
| `s3adm bench --output=json` | 现只有人读表格，无法做基线比对（§4.3 性能基线的前置） | 中 | 低 |
| `s3adm usage` | 依赖 §3.9 ① | 高 | 中 |

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
5. `--version`、`--check-config`、ListParts encoding-type 等极低成本小项

### P1 — 近期（高价值 / 低-中难度）

1. Grafana dashboard + Prometheus 告警规则（§5.5，零 C++）
2. mint 挂 ctest + website e2e/单测 + s3adm 进 e2e（§6.1）
3. API×后端分维指标 + 后端耗时（§5.1）；异步日志 + 慢日志（§5.2）
4. CORS + OPTIONS 预检（§2.1）；网站 302 补斜杠（§2.3）
5. cloudproxy 协程化退避 + 连接池回收 + Retry-After（§3.3）
6. 后台任务 CLI 化（§3.2）；DuoGcStats 接指标（§3.7）；xattr 降级 gauge（§3.5）
7. fuzz 起步（XML/SigV4/URI 三个 harness）+ ubsan/coverage（§6.1）
8. Dockerfile + compose（§6.3）
9. 性能基线入库（§4.3；前置 bench --json）

### P2 — 中期（需设计，一个方向一个迭代）

1. scrub/fsck（§3.1）→ 顺带产出用量统计（§3.9①）
2. 超时体系拆分 + L1 指标 + 连接治理（§4.2/§5.3）
3. 缓冲池化 + 流式双缓冲 + sendfile（§4.3；与 §3.4 fixed buffers 联动）
4. builtin/seastar TLS + 证书热重载（§4.1）
5. 配置热重载安全子集（§4.4）
6. Lifecycle 最小子集（MPU 自动清理）+ Object Tagging（§2.4/§2.5）
7. localfs listing 优化 + 元数据缓存层（§3.5/§3.8）
8. tiered 扫描增量化 + prefix 策略（§3.6）
9. 故障注入体系 + soak（§6.1）；traceparent 透传（§5.4）
10. install target + CPack（§6.3）；审计日志（§3.9④）

### P3 — 长期 / 架构级（先想清目标场景再动）

1. xlocalfs 单流多在途流水线 + fixed buffers（§3.4）
2. 配额 → 租户实体化（§3.9②③）
3. STS 会话凭证（§2.6）
4. TiKV 事务层生产化 / 多网关 read-lease / meta 在线备份（§3.7）
5. versioning（duostore 侧切入）、SSE-C/SSE-S3（§2.2/§7）
6. tiered local 侧抽象化容纳 duostore、Range 部分缓存（§3.6）
7. OpenTelemetry 全量埋点（§5.4）、HTTP/2（§4.5）
