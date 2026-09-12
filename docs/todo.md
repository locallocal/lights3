# 待办与规划（TODO）

接替 [archive/backlog.md](archive/backlog.md)（2026-09-05 起的待办底账，其 §1 十个
分期保留项已按 [archive/backlog-sequence.md](archive/backlog-sequence.md) 于
2026-09-06 全部完成，两份文件随之归档，源码注释里的 `backlog §N` /
`backlog-sequence ①…⑩` 指归档文件的对应章节）。本文**只列尚未做的事**：
外部依赖的收尾、代码已落地但本机无法验证的项、基线跑出的新问题、长期项与
明确不做清单。做完一项就从本文删除，实现细节写进对应设计文档，不保留划线
历史。每条标 **价值**（高/中/低）与 **难度**（低/中/高）。

## 1. 外部依赖的收尾

| 条目 | 出处 | 现状与剩余步骤 | 价值 | 难度 |
| --- | --- | --- | --- | --- |
| client-c 结构化错误码上游合入 | backlog-sequence ⑨，[duostore-meta-tikv-design.md](storage/duostore-meta-tikv-design.md) | 本仓侧已完成（2026-09-06）：补丁在 `third_party/patches/client-c`（README 含 PR 描述与合入后步骤），sidecar 编译期按链接到的库选择按码 / 按消息串。剩余：开上游 PR → 合入后升子模块指针、删消息串分支与补丁目录 | 低 | 低 |

## 2. 待验证（代码已落地，本机环境验证不了）

| 条目 | 出处 | 需要 |
| --- | --- | --- |
| Docker 镜像构建与 compose 四个 profile（默认 / redis / tikv / rados / e2e） | roadmap §6.3，[deployment.md §4](deployment.md) | 有 docker daemon 的机器：`docker compose build`，`docker compose --profile e2e run --rm e2e`（把 redis / tikv / rados 三条 SKIP 的 e2e 路径真正跑一次） |
| CPack RPM | roadmap §6.3，[deployment.md §3.2](deployment.md) | 有 `rpmbuild` 的机器：`cpack -G RPM`，`rpm -qp --scripts` 核对 scriptlet，安装/升级/卸载各走一遍 |
| `unit_tests` 偶发 `terminate called without an active exception` | 2026-09-05 本机 5 次全量运行中 2 次，均发生在 `timer_stats_track_fired_and_pending` 通过之后、`timer_slow_callback_counted` 的 1.1s 慢回调期间（日志先打 "callback took 1.100s"），gdb 下未复现；与业务改动无关 | 有空档时排查：怀疑 TimerQueue 或测试夹具里某个 joinable `std::thread` 在负载下的析构次序；先用 `catch throw`/`ulimit -c` 抓栈 |
| 多网关 multipart 容器 e2e | [archive/multi-gateway-multipart-design.md](archive/multi-gateway-multipart-design.md) §4 ② | compose `multi` profile（两 lights3 + redis + rados + nginx 轮询）与 `deploy/docker/e2e-multi.sh` 已落地（2026-09-09，`docker compose --profile multi config` 通过）；有 docker 的机器：`docker compose --profile multi run --rm e2e-multi`。单测与本机 e2e（`run_e2e.sh` duostore-redis 段）已通过 |
| mint 兼容基线 | roadmap §6.1，[testing.md §6](testing.md) | 有 docker 的机器跑 `ctest -R mint -V`，把每套件 PASS/FAIL/NA 计数记入 testing.md §6 |
| S3 Tables 的 Spark / Trino 人工验证 | [s3-tables-design.md §13](s3-tables-design.md) | 本机只有 PyIceberg / DuckDB 冒烟通过（testing.md §6），无 Spark / Trino：按 §13 模板配 `rest.sigv4-enabled` 走一遍建表 / append / `rewrite_data_files`（Spark）与 SIGV4 读写（Trino），结果记 testing.md §6；顺带把 Spark 写出的 manifest（负块计数）作为固件入 `tests/fixtures/tables/`（现有固件全部由 PyIceberg 生成） |

## 3. 性能基线跑出的新问题（[performance-baseline.md](performance-baseline.md)）

| 条目 | 现象 | 入口 | 价值 | 难度 |
| --- | --- | --- | --- | --- |
| beast 的 TLS GET 明显落后 | 4 MiB GET 明文 4.6k ops/s、TLS 仅 1.5k，其他三驱动 TLS 在 3.0k 左右 | `src/http/drivers/beast/beast_server.cc` 的 `TlsStream` 写路径：asio ssl 的 record 切分与每块一次 strand 跳转；先用 `strace -c` 对比明文/TLS 的 syscall 计数 | 中 | 中 |
| 请求体路径未做对称优化 | PUT 各驱动持平，只有 beast 因读粒度 bug 修复而大幅提升；backlog-sequence ⑩ 的队列块整形只让 httplib 4 MiB PUT 提升约 3% | 请求体是 pull 模型且要保留背压，预取需谨慎；候选：builtin `SocketBodyReader` 大块 recv、beast `expires_after` 每块重设定时器的开销 | 中 | 中 |

## 4. S3 Tables 收尾项（[s3-tables-design.md](s3-tables-design.md) ①–⑥ 已实现，以下是实现记录里留下的缺口）

| 条目 | 出处 | 现状与入口 | 价值 | 难度 |
| --- | --- | --- | --- | --- |
| view 的诊断与 fsck 对账 | [s3-tables/step-6-optional.md §7](s3-tables/step-6-optional.md) | `rename_view` 是"先写目标、再把源改墓碑"两步且无 intent，中间崩溃留下源与目标同时 Active 的双份；`reconcile_catalog`（`tables/fsck.cc`）与 `catalog/diagnostics` 都不看 `view/` 目录。入口：fsck 对 view 条目做 `tables.malformed_entry` 与"同一 view uuid 出现两次"两项发现，diagnostics 对 view 至少核对 metadata 指针存在 | 中 | 低 |
| gzip 压缩的 metadata.json | [s3-tables/step-1-catalog-core.md §9](s3-tables/step-1-catalog-core.md)（① 留给 ③） | `.metadata.json.gz` 仍回 406 `compressed metadata files are not supported`（③ 引入的 zlib 只用于 Avro deflate 块）；Spark `write.metadata.compression-codec=gzip` 写出的表无法 register / LoadTable。入口：`Catalog` 读 metadata 处在 `LIGHTS3_TABLES_ZLIB` 下 inflate（仍受 50 MiB 上限），写侧保持不压缩 | 中 | 低 |
| 设计 §13 两个指标未加 | [s3-tables-design.md §13](s3-tables-design.md) | `lights3_tables_maintenance_deleted_bytes_total`（run 作业 `stats` 里已有 `deleted_bytes`）与 `lights3_tables_finalization_gaps` gauge（diagnostics 结果里已有计数）没接 `MetricsScope`；告警组 `lights3.tables` 只用 commits / requests / validation。入口：`maintenance.cc` 的 run 收尾与 `diagnostics.cc` 的汇总各加一处，`gen_dashboard.py` 加面板 | 低 | 低 |
| `DuoMetaCatalogStore` 的 KV 调用同步阻塞 | [s3-tables/step-6-optional.md §7](s3-tables/step-6-optional.md) | redis / tikv 引擎的网络往返在调用线程（HTTP 工作线程）上执行；目录写路径量小且按表串行，暂可接受。入口：与 `DuoStoreBackend` 一致，经 `pool->schedule()` 挪到池线程，或给 `IMetaStore` 加异步 KV 面 | 低 | 中 |

## 5. 长期 / 架构级（先想清目标场景再动）

| 条目 | 说明 |
| --- | --- |
| Versioning | 架构级（六后端 key 布局 / List 语义 / delete marker / GC 全动）；若做，**从 duostore 侧切入成本最低**（meta 是 KV，加 version 维度即可），localfs 的 key→路径映射容纳不下多版本 |
| SSE-C / SSE-S3 | 服务端加密；需先定密钥来源与 ETag/校验和语义 |
| OpenTelemetry 全量埋点 | 轻量 trace 层已做（W3C traceparent 透传、每请求一 span、日志关联，[s3-protocol.md §7](s3-protocol.md)）；otel-cpp 导出 span 是长期项 |
| HTTP/2 | S3 SDK 主流仍 HTTP/1.1，CDN / L7 前置场景才需要；前置代理终结 h2 见 [tls.md §6](tls.md) |
| 客户端断连独立取消源 | 刻意取舍：长 handler 靠 `request_timeout` 兜底，驱动在下一次 socket 操作时发现断连（[http-adapter.md §2.3](http-adapter.md)） |
| Iceberg 多表事务 `/transactions/commit` | [s3-tables-design.md §15](s3-tables-design.md) 原写"duostore-meta 后备落地后再议"，⑥ 已落地：`kv_put_batch` 一批可写多张表的指针 + 记录，原子性条件已满足；缺的是端点本身、跨表 requirements 的组合校验、对象后备（做不到）下的拒绝方式（406）与引擎侧开关的对接（DuckDB 模板里的 `DISABLE_MULTI_TABLE_COMMIT`）。先确认有引擎真的需要再动 |

## 6. 明确不做

| 条目 | 理由 |
| --- | --- |
| Object Lock / Legal Hold | 无 versioning 地基，WORM 语义无法成立 |
| Bucket Policy（IAM 语言） | per-credential policy 已覆盖多租户隔离，匿名公开桶由 website 面解决；IAM 求值器是独立子系统，投入不成比例 |
| SigV2 | AWS 已停用，客户端基本绝迹 |
| presigned POST | 需先写半个 multipart/form-data 流式解析器；CORS + presigned PUT 是更现代的替代路径 |
| cloudproxy 出方向 streaming 签名上传 | 复杂度高、收益仅是明文 HTTP 下的完整性（[cloudproxy-design.md](storage/cloudproxy-design.md)） |
| rados 数据面 pack 层 | 代码内长注释已论证为设计边界（小对象放大交给 BlueStore `min_alloc_size`） |
| CivetWeb 等新 HTTP 驱动 | 四驱动已覆盖设计空间（[http-adapter.md §3.4](http-adapter.md)） |
| GitHub Actions CI | 项目已明确移除、不使用；自动化投入放在本地脚本矩阵（`scripts/check-all.sh`，[testing.md §8](testing.md)） |

## 7. 维护约定

- 新条目须给出**出处 / 入口 / 价值 / 难度**；做完即删，实现写进对应设计文档。
- 源码注释继续用 `roadmap §N`、`backlog §N`、`backlog-sequence ①…⑩` 引用归档文件的
  论证；本文的条目以 `todo §N` 引用。
- 历史底账：[archive/gaps.md](archive/gaps.md)、[archive/issues.md](archive/issues.md)、
  [archive/roadmap.md](archive/roadmap.md)、[archive/backlog.md](archive/backlog.md)、
  [archive/backlog-sequence.md](archive/backlog-sequence.md)，只读。
