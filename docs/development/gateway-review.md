# 网关整体扫描（2026-09-15）

一次性的全量走查快照，**不是**新的待办底账：每条确认要做的，要么直接修掉，要么
搬进 [todo.md](todo.md)，搬走后从本文删除对应行；全部清空后本文连同索引行一起删除
（与 gaps/issues/roadmap 三份归档底账的处置方式一致）。

## 1. 范围与方法

覆盖 L1 HTTP 适配层（`src/http/`）、L2 S3 协议层（`src/s3/`）、L4 运行时
（`src/core/`），L3 存储层（`src/storage/`）只走了 localfs 写路径与公共约定。
读码为主，逐条落地时都补了回归用例，多数还在活网关上实测过（见各条后面的用例名）。

已修复并删除的条目，编号留空不再复用：R1（auth 关闭时 aws-chunked 不解帧，回归用例
`sigv4_disabled_*` 三条）、R2（`x-amz-decoded-content-length` 解析过宽，回归用例
`parse_content_length_is_strict` 与 `sigv4_decoded_content_length_parsed_strictly`）、
R3（用户元数据无总量上限，现为 `http.max_user_metadata_size`，回归用例
`service_user_metadata_size_capped` / `config_max_user_metadata_size_bounded`）、
R4（请求尾部的进程级锁与默认同步日志，`log.async` 已默认开，实测见
[performance-baseline.md](performance-baseline.md) §4.0–4.2；回归用例
`http_driver_shutdown_cuts_idle_keepalive_at_once` 与 `ratelimit_*` 两条）、
R5（分块解帧改直读，实测见同文 §4.3，解帧开销 +18% → 持平；回归用例
`sigv4_chunked_direct_read_*` 与 `sigv4_chunked_zero_length_read_is_not_a_truncation`）、
R6（路由只解析一次；**性能上是阴性结果**，见同文 §4.4，保留是为了把授权与执行取同一条
路由变成结构保证，由 `service_website_*` 三条用例守着）、R7（桶维度指标表改 LRU 淘汰 +
不存在的桶不开系列，回归用例 `s3_metrics_bucket_table_evicts_coldest` 与
`service_unknown_bucket_opens_no_metric_series`）、R8（httplib 泵线程改固定池，
PUT +20%，见同文 §4.5；回归用例 `http_driver_many_concurrent_bodies`）、R9（线程池
"有界队列"的说法改成实话：有界的是**就绪数**、超出的被推迟而非拒绝，硬上限在准入闸门；
`enqueue_bounded` 更名 `enqueue_deferrable`，回归用例
`schedule_overflow_is_deferred_not_rejected`）、R10（builtin/seastar 以 400 拒绝
obs-fold；**原来不只是"碰巧安全"** —— 带冒号的续行会让重复 Content-Length 逃过框架
检查，其后的字节被当作下一个请求应答，回归用例 `http_driver_obs_fold_*` 两条）、
R11（builtin 关连接前补 `shutdown(SHUT_WR)`；**原判"客户端读不到 4xx"不成立** ——
实测 Linux 会先交付对端缓冲里的数据再报 reset，真正的差别是客户端分不清正常结束与
截断；半关一句就够，不需要 lingering 排空。回归用例
`http_driver_unconsumed_body_ends_the_connection_in_order`）、R12（presigned 采信签名
覆盖之内的 payload 哈希承诺：`X-Amz-Content-Sha256` query 参数，或出现在 SignedHeaders
里的同名头；没签的头不看。回归用例 `sigv4_presigned_honours_a_committed_payload_hash`
与 `sigv4_presigned_payload_hash_cannot_be_forged`）、R13（`/-/metrics` 匿名暴露在非回环
监听上时启动 WARN，并把"`/-/` 面放私网监听"写成推荐形态；回归用例
`config_metrics_exposure_predicate`）。

**风险项（R 系列）已全部清零**，本文只剩 §3 的纯性能项 O1–O6 与 §6 的顺带发现。

等级：高＝可能损坏数据或绕过约束；中＝可被外部输入放大，或明显偏离 AWS 语义；
低＝加固/一致性问题。

## 2. 结论摘要

风险项 R1–R13 与纯性能项 O1–O6 **全部完成**（去向见 §1）。本文只剩 §5 的两条顺带
发现；那两条各自了结后，本文连同 docs/README 的索引行一起删除。

## 3. 复现方法

条目已清空。各条的实测方法与数据都在
[performance-baseline.md](performance-baseline.md) §4；早先几条用过的"无凭证起服"模板
（localfs + builtin + `auth:` 只留 region）见 git 历史里本文的旧版本。

## 4. 留给后来者

1. 只剩 R13。O1–O6 是纯性能项，先照 §4.4 的办法做交错 A/B，别预设它们一定
   测得出来。

## 5. 顺带发现（不在原清单里）

- `http_driver_many_concurrent_bodies`（R8 时新加）对 httplib 是 flaky 的：24 个客户端
  同时 connect 会打爆 cpp-httplib 上游那个 5 的 listen backlog，SYN 被丢弃重传，表现为
  "服务端从没读过的连接"。**与泵线程池无关** —— 改 R8 之前的版本同样 2/12 失败，把连接
  建立错峰 10ms 后 27/27 通过（上传本身仍然重叠，每个 200 KB 远长于 10ms）。已在
  R11 这轮修掉；记在这里是因为我一开始误判成 R8 的回归，多跑几轮才排除。

- **ASan 下全量单测泄漏约 63 MB / 252 处**，全部是 `PipelinedMd5::make_buffers`
  （`duostore_backend.cc` 的 `pump_body`、localfs 的 put 路径）：put 在中途被放弃时协程帧
  没被销毁，每次漏掉一对 256 KiB 缓冲。最初只在
  `duostore_pack_chunked_put_buffer_and_spill` 上看到 512 KiB，跑全量才看出规模。**在未
  改动的树上同样复现**（O 系列改前 252 处 / 63,439,920 字节，改后 253 处 / 63,702,064
  字节，即与这些改动无关），未深查。复现：`./build-asan/unit_tests`，看末尾的
  LeakSanitizer 汇总。

## 6. 走查中确认无问题的点

避免后来者重复排查，记几条读过并确认站得住的：

- 请求走私前置条件（CL/TE 冲突、重复 CL、非 chunked 的 TE）在四驱动统一拒绝
  （`http/drivers/common.h:293`）；
- 响应头 CR/LF 注入在 `emit_headers` 统一过滤（同上 450），beast 的直插路径也已
  收口；
- 准入 permit 绑在流式响应体的生命周期上（`http/admission.h:39`），HEAD 与小响应
  的归还路径都对；
- 取消（超时/断连/关停）的竞态协议（claim + 单次 resume）在 `ThreadPool::
  ScheduleAwaiter`、`AsyncSemaphore::Waiter`、`CancelState` 三处写法一致；
- SigV4 的 host 必签、scope/日期一致性、STS token 与 AK 的绑定关系、presigned 的
  未来时间上限都做了（`sigv4.cc:713-783`）；
- 桶名校验是唯一权威闸门且 copy-source 单独复用同一函数
  （`s3/handlers/common.h:172`）；
- 指标标签基数有上限（api 与 bucket 两个维度都有），限流表有 LRU 上限；
- `Range: bytes=-0` 返回 416（实测），`bytes=5-3` 按无效头忽略（`objects.cc:52`），
  与 RFC 9110 §14.1.1 / AWS 一致；
- 工作树（含未提交改动）Debug 增量构建通过且无编译告警
  （`cmake --build build -j 16`，半数核）。
