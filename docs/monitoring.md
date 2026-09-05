# 监控消费侧：Prometheus 规则与 Grafana dashboard（roadmap §5.5）

`GET /-/metrics` 已是规范 Prometheus 文本格式、`lights3_*` 命名统一
（[s3-protocol.md §7](s3-protocol.md)），本篇是它的消费侧：零 C++ 改动，
`deploy/` 下三份现成资产 + 一个把资产和源码指标目录对账的测试。

## 1. 资产一览

| 文件 | 内容 |
| --- | --- |
| `deploy/prometheus/scrape.yml` | 抓取配置：`job_name: lights3`、`metrics_path: /-/metrics`、加载规则文件；可单独运行也可并入既有 prometheus.yml |
| `deploy/prometheus/lights3.rules.yml` | 9 组 48 条：7 条 recording（5xx 率、P99、后端错误率、cloudproxy 重试率、keep-alive 复用率）+ 41 条告警 |
| `deploy/grafana/lights3.json` | dashboard（uid `lights3-overview`，63 面板 / 9 行），变量 `DS` / `instance` / `backend` |
| `deploy/grafana/gen_dashboard.py` | dashboard 的生成脚本——面板定义的唯一来源，改完重跑生成 JSON |
| `tests/monitoring/check_assets.py` | 资产校验（§5），ctest 名 `monitoring_assets` |

## 2. 接入

```bash
prometheus --config.file=deploy/prometheus/scrape.yml       # 单网关实验环境
# 或把 scrape_configs 段并入既有配置，rule_files 指向 lights3.rules.yml
```

Grafana：Dashboards → Import → 上传 `lights3.json`，选 Prometheus 数据源
绑定到 `DS` 变量。多网关按 `instance` 变量切换/聚合；后端级面板按
`backend` 变量过滤（取自 `lights3_backend_op_seconds_count` 的 backend 标签）。

**`/-/metrics` 的访问面**：Prometheus 不会做 SigV4 签名，因此抓取要求
`http.metrics_access: anonymous`（默认）。配置了 `http.admin_port` 时 `/-/metrics`
只在 admin 端口上（数据面端口答 404），抓取目标写 `host:admin_port`
（[http-adapter.md §2.1](http-adapter.md)）。开了 `root` 门禁的部署，要么用
会签名的旁路代理抓取，要么把监听放在私网并保持匿名。TLS 部署改
`scheme: https` 并配置 `tls_config`。

## 3. 告警目录

阈值是单网关起点，按流量形态调 `for` 与比率。`severity` 三级：
critical（需即时处理）/ warning（需当天处理）/ info（趋势提示）。

| 组 | 告警 | 触发 | 级别 |
| --- | --- | --- | --- |
| availability | `Lights3Down` | `up == 0` 持续 1m | critical |
| | `Lights3High5xxRate` | 5xx 比率 > 2% 且 > 1 req/s，5m | critical |
| | `Lights3HighP99Latency` | 请求 P99（响应头就绪）> 2s，10m | warning |
| | `Lights3ApiP99Latency` | 元数据类 API P99 > 1s（排除传输体的 Get/Put/UploadPart/Copy/Complete） | warning |
| capacity | `Lights3AdmissionSaturated` | 许可耗尽且有排队，5m | warning |
| | `Lights3AdmissionQueueWait` | 准入等待 P99 > 1s | warning |
| | `Lights3AdmissionCancellations` | 10m 内 >10 个排队中被取消（503） | warning |
| | `Lights3PoolBacklogged` | 共享池积压 5m（[concurrency.md §3.1](concurrency.md) 的分池判据） | warning |
| | `Lights3TimerLag` | 定时器队头滞后 > 5s | warning |
| | `Lights3TransferStalls` | 10m 内停滞守卫掐断 > 20 次 | info |
| http | `Lights3ConnectionLimit` | `max_connections` 拒绝 | warning |
| | `Lights3ParseErrorSpike` | 畸形请求 > 5/s | info |
| | `Lights3TlsHandshakeFailures` | 握手失败 > 1/s | info |
| | `Lights3RateLimitRejections` | 限流拒绝 > 10/s，10m | info |
| backends | `Lights3BackendErrors` | 后端 5xx/传输错误率 > 1%（4xx 不计） | critical |
| | `Lights3XlocalfsUringFallback` | `lights3_xlocalfs_uring_fallback == 1` | warning |
| | `Lights3LocalfsXattrFallback` | `lights3_localfs_xattr_fallback == 1` | info |
| | `Lights3MetaCacheStale` | 元数据缓存 stale 命中 > 5% | info |
| duostore | `Lights3DuostoreGcNotConverging` | GC 有轮次但队头 > 1h 未处理（**GC 未收敛**） | warning |
| | `Lights3DuostoreGcStalled` | 有积压但 1h 无 GC 轮次 | warning |
| | `Lights3DuostoreGcRoundSlow` | GC 轮次 P99 > 10min | info |
| | `Lights3DuostoreCorruption` | 读 CRC 不符 / pack 记录损坏 | critical |
| | `Lights3DuostorePacksQuarantined` | 隔离 pack > 0 | warning |
| | `Lights3DuostoreCompactionDeferred` | 压缩连续推迟 6h | info |
| | `Lights3DuostoreSqliteBusy` / `RedisReconnects` / `TikvSafepoint` / `RadosErrors` | 各 meta/data 引擎的健康信号 | info–critical |
| tiered | `Lights3TieredGcFailures` | 云端删除持续失败 30m | warning |
| | `Lights3TieredGcBacklog` | 推迟的云端删除 > 100，1h | warning |
| | `Lights3TieredQuarantine` | 隔离条目 > 0（refs_missing = 数据丢失信号） | critical |
| | `Lights3TieredEvictionPressure` | 每轮扫描都在逐出，1h（**水位**信号，见 §4） | info |
| | `Lights3TieredCloudReadHeavy` | >50% 读回源云端，30m | info |
| cloudproxy | `Lights3CloudproxyRetryRatio` | 重试比 > 10%，10m | warning |
| | `Lights3CloudproxyRemoteErrors` | 远端 5xx/传输错误 > 0.5/s | critical |
| | `Lights3CloudproxyEtagMismatch` | 上传 ETag 不符 | critical |
| | `Lights3CloudproxyPoolWait` | 连接池等待 P99 > 1s | warning |
| tenancy | `Lights3QuotaRejections` | 1h 内有配额拒绝 | info |
| | `Lights3UsageScanStale` | 用量全量校准超过 2 天未跑 | info |

Recording 规则（供 dashboard 与告警共用）：`lights3:requests:rate5m`、
`lights3:responses_5xx_ratio:rate5m`、`lights3:request_duration_seconds:p99_5m`、
`lights3:api_request_duration_seconds:p99_5m`、`lights3:backend_error_ratio:rate5m`、
`lights3:cloudproxy_retry_ratio:rate5m`、`lights3:keepalive_reuse:rate5m`。

## 4. Dashboard 行

| 行 | 面板要点 |
| --- | --- |
| Overview | 四个 stat（req/s、5xx 率、P99、在途）；按 method / class / 精确状态码分布、S3 错误码、字节吞吐 |
| APIs | api × backend 的速率、5xx、P99（roadmap §5.1） |
| HTTP layer | 连接数、keep-alive 复用率（requests ÷ accepted）、超时分相、畸形请求 / TLS 握手、限流拒绝 |
| Admission & pools | 许可容量/可用/排队、准入等待 P50/P99、停滞掐断、共享池与后端池、定时器线程 |
| Storage backends | 后端 op P99 / 错误 / 速率、元数据缓存、**降级旗标**（uring / xattr fallback）、localfs op 错误 |
| Buckets / usage / website | top bucket、用量字节、配额拒绝、网站面事件、活跃 multipart |
| DuoStore | GC 队列深度与队头年龄（收敛判据）、轮次/回收/耗时、跳过原因、pack live vs total、隔离、损坏与孤儿、四种 meta 引擎信号 |
| Tiered | 读来源、降冷/回热/逐出、本地层容量（文件系统已用 vs 高水位、账面本地字节 vs 配额）、云端 GC、扫描与隔离、range cache、op 错误 |
| CloudProxy | 远端请求 P99、重试比、错误码、连接池等待与 ETag 不符 |

**tiered 水位**：`lights3_tiered_local_used_bytes` / `_total_bytes` /
`_high_watermark_bytes`（statvfs，正是水位逻辑看到的量）与
`lights3_tiered_local_cached_bytes` / `_quota_bytes`（账面本地字节与逻辑配额）
五个 gauge 直接给出位置；告警 `Lights3TieredLocalAboveHighWatermark`（30 分钟
仍在高水位之上 = 逐出追不上）与 `Lights3TieredQuotaNearlyFull`（账面超过配额
90%）。逐出速率 + 扫描轮次的 `Lights3TieredEvictionPressure` 保留为"每轮都在逐出"
的补充信号。

## 5. 测试

`tests/monitoring/check_assets.py`（ctest `monitoring_assets`，需要 python3；
PyYAML 缺失时规则/抓取配置的结构校验降级为失败提示）：

1. 规则文件解析；每条是 record 或 alert；alert 必有 `severity`
   （critical|warning|info）、`summary`、`description`；名字唯一并遵循
   `Lights3*` / `lights3:<a>:<b>` 约定；本机装了 promtool 则再跑
   `promtool check rules/config`。
2. 抓取配置含 `lights3` job、`/-/metrics` 路径并加载规则文件。
3. `lights3.json` 与 `gen_dashboard.py` 的渲染结果**逐字节一致**（防手改
   JSON 与生成器漂移）；面板都有 target、id 唯一、三个模板变量齐全。
4. 规则与 dashboard 表达式引用的每个 `lights3_*` 指标都能在 `src/` 的指标
   目录（对 `lights3_[a-z0-9_]+` 字面量的 grep）中找到，直方图允许
   `_bucket/_sum/_count` 后缀——改名或删指标会让测试失败，资产不会悄悄失效。
5. 起一个 memory 后端网关，打几条请求后抓 `/-/metrics`：每行符合 exposition
   格式、每个族有 `# TYPE`，且资产引用的所有**非后端专属**指标族都出现在
   输出里（后端专属前缀 duostore/tiered/cloudproxy/localfs/… 只做目录对账）。

改动流程：改 `gen_dashboard.py` → `python3 deploy/grafana/gen_dashboard.py` →
`ctest -R monitoring_assets`。
