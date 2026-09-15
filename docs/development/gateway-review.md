# 网关整体扫描（2026-09-15）

一次性的全量走查快照，**不是**新的待办底账：每条确认要做的，要么直接修掉，要么
搬进 [todo.md](todo.md)，搬走后从本文删除对应行；全部清空后本文连同索引行一起删除
（与 gaps/issues/roadmap 三份归档底账的处置方式一致）。

## 1. 范围与方法

覆盖 L1 HTTP 适配层（`src/http/`）、L2 S3 协议层（`src/s3/`）、L4 运行时
（`src/core/`），L3 存储层（`src/storage/`）只走了 localfs 写路径与公共约定。
读码为主，其中 R11 与 §7 的 Range 行为用一个**无凭证**的 builtin 实例现场跑过
（复现步骤见 §5）。标注 **实测** 的结论有现场证据，其余是读码推断，落地前请各自补一
个用例。

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
`http_driver_unconsumed_body_ends_the_connection_in_order`）。

等级：高＝可能损坏数据或绕过约束；中＝可被外部输入放大，或明显偏离 AWS 语义；
低＝加固/一致性问题。

## 2. 结论摘要

| 编号 | 位置 | 等级 | 一句话 |
| --- | --- | --- | --- |
| R12 | `s3/auth/sigv4.cc:792` | 低 | presigned 一律按 `UNSIGNED-PAYLOAD` 计签 |
| R13 | `config/lights3.yaml` | 低 | `/-/metrics` 默认匿名，暴露桶名与后端拓扑 |
| O1–O6 | 见 §4 | — | 纯性能项（SigV4 规范化、header 访问、id 生成、fsync、beast 每请求系统调用） |

## 3. 风险项

### R12（低）presigned 一律按 UNSIGNED-PAYLOAD 计签

`sigv4.cc:791-792`：只要是 presigned 就把 payload_hash 固定成 `UNSIGNED-PAYLOAD`。
`X-Amz-Content-Sha256` 在通用查询白名单里（`service.cc:370`）却不参与这个选择，
所以用真实 payload hash 预签的客户端会拿到 SignatureDoesNotMatch。与 AWS 主流行为
一致，但既然放行了这个查询参数，就应该在它出现时采用它的值。

### R13（低）`/-/metrics` 默认匿名

默认 `metrics_access: anonymous`，而指标里带桶名、每桶请求量/字节、后端名与拓扑。
生产部署应当 `metrics_access: root` 或用 `http.admin_port` 把管理面分到单独监听。
现有文档提到了这两个开关，但没有在部署清单里作为**推荐默认**出现。

## 4. 纯性能项

| 编号 | 位置 | 内容 |
| --- | --- | --- |
| O1 | `s3/auth/sigv4.cc:609-623` | canonical headers 是 O(signed × headers) 的 `ieq` 扫描，且 `split` 每次分配一串 `std::string`；`canonical_query`（66-85）对 raw query 做 decode→encode→sort，又是一轮分配。小对象高 QPS 下 SigV4 是 CPU 大头之一，值得先建一次小索引再扫 |
| O2 | 全仓 | `headers.get()` 用了 52 处，`headers.find()` 只有 1 处 —— 而 `http/model.h:43` 的注释明确写着"存在性判断/比较请用 find，get 每次拷贝值"。改造是机械的，每请求能省十几次小分配 |
| O3 | `s3/service.cc:35-52` | 每请求生成 16 字符 request-id + 48 字符 host-id（两次 `to_hex` + 两次分配）。`x-amz-id-2` 可以退化成"每进程前缀 + 每连接计数"，它只是给日志关联用的 |
| O4 | `storage/localfs/fs_util.cc:87-94` | 对象提交走 `fsync_path`（按路径重新 `open` 再 `fdatasync` 再 `close`），而 upload_part 走的是 fd 版 `fsync_file`（`localfs_backend.cc:1134`）。PUT 路径每次多一对 open/close 系统调用，两条路径的写法也不一致 |
| O5 | `http/drivers/beast/beast_server.cc:762-766` | 每请求一次 `remote_endpoint()` 系统调用 + `to_string()` 分配；keep-alive 连接上这是常量，缓存到 `Session` 即可 |
| O6 | `http/drivers/common.h:213-233` | `parse_target` 对每个 query 参数做 `substr` + 两次 percent-decode，全部落成 `std::string`；配合 O1/R6 一起改成 string_view 视图 + 延迟解码收益更整齐 |

## 5. 复现方法

剩下的 R12 / R13 都不需要跑服务：R12 读 `sigv4.cc` 的 presigned 分支即可判断，R13 是
配置默认值。早先几条用过的"无凭证起服"模板（localfs + builtin + `auth:` 只留 region）
见 git 历史里本文的旧版本，或直接照 `config/lights3.yaml` 删掉 credentials 一节。

## 6. 建议的推进顺序

1. 按等级顺延（R12 起）。O1–O6 是纯性能项，先照 §4.4 的办法做交错 A/B，别预设它们一定
   测得出来。

## 6.5 顺带发现（不在原清单里）

- `http_driver_many_concurrent_bodies`（R8 时新加）对 httplib 是 flaky 的：24 个客户端
  同时 connect 会打爆 cpp-httplib 上游那个 5 的 listen backlog，SYN 被丢弃重传，表现为
  "服务端从没读过的连接"。**与泵线程池无关** —— 改 R8 之前的版本同样 2/12 失败，把连接
  建立错峰 10ms 后 27/27 通过（上传本身仍然重叠，每个 200 KB 远长于 10ms）。已在
  R11 这轮修掉；记在这里是因为我一开始误判成 R8 的回归，多跑几轮才排除。

- `duostore_pack_chunked_put_buffer_and_spill` 在 ASan 下报 512KiB 泄漏（2 次
  `PipelinedMd5::make_buffers`，`duostore_backend.cc:1157` 的 `pump_body`）：put 在中途
  被放弃时协程帧没被销毁。在未改动的树上同样复现，与 R4/R5 无关，未深查。复现：
  `LIGHTS3_TEST_FILTER=duostore_pack_chunked ./build-asan/unit_tests`。

## 7. 走查中确认无问题的点

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
