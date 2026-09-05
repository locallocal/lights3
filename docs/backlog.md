# 待办与后续规划（Backlog）

接替 [archive/roadmap.md](archive/roadmap.md)（2026-08-25 走读的规划底账，
2026-09-05 全部条目收口后归档；源码注释里的 `roadmap §N` 指该归档文件的
章节）。本文**只列尚未做的事**：分期保留的设计、代码已落地但本机无法验证的
项、基线跑出的新问题、长期项与明确不做清单。做完一项就从本文删除，实现细节
写进对应设计文档——不再像 roadmap 那样保留划线历史。每条标 **价值**
（高/中/低）与 **难度**（低/中/高）。

## 1. 分期保留（设计入口已定，随需求触发）

实施先后顺序、各项范围与验收见 [backlog-sequence.md](backlog-sequence.md)。

| 条目 | 出处 | 现状与入口 | 价值 | 难度 |
| --- | --- | --- | --- | --- |
| STS 会话表多实例共享 | roadmap §2.6 | `AssumeRole` 会话为单实例内存态（`src/s3/auth/credential_store.*`，会话短命）；多网关共享需落到 `.sys` 或 meta 引擎，可复用凭证 `sync_interval` 的同步机制 | 中 | 中 |
| scrub / fsck 的 admin 端点 | roadmap §3.1 | 离线 `lights3 fsck` 与在线 `s3adm fsck` 已覆盖，触发面留观；若要做，照 `POST /-/admin/config/reload` 的 root 端点模式 | 低 | 低 |
| duostore meta 增量备份 / PITR | roadmap §3.7 | `dump` 已是一致性全量快照（`IMetaStore::snapshot()`）；增量需 WAL 级导出，四个引擎各不相同 | 中 | 高 |
| client-c 结构化错误码上游贡献 | roadmap §3.7（tikv T5） | sidecar 以 kvrpcpb 结构化冲突分类为主、字符串匹配作纵深，功能不受影响；上游 PR 可选 | 低 | 中 |
| 跨网关元数据缓存失效协议 | roadmap §3.8 | 共享 meta（redis/tikv）以 `meta_cache_ttl` 有界陈旧为契约（[storage/duostore-core.md §7.1](storage/duostore-core.md)）；redis 可走 pub/sub，tikv 无等价物 | 中 | 高 |
| mTLS 客户端证书 → 凭证 / 租户身份映射 | roadmap §4.1 | `tls_client_auth` 已验证证书链但不映射身份（[tls.md](tls.md)）；映射需定证书字段与凭证的绑定规则 | 中 | 中 |
| 后端实例增删热重载 | roadmap §4.4 | `reload_config` 只应用安全子集 + `buckets.rules`；driver / 后端 / `default_backend` / `auth.*` 明确需重启（[config-reload.md](config-reload.md)） | 中 | 高 |
| 独立 admin 端口 | roadmap §5.3 | 现由 `http.metrics_access: root` 与 root 专属 admin 路由把关；端口层隔离交部署侧反代（[tls.md §6](tls.md)） | 低 | 低 |
| tiered 本地层已用字节 gauge | roadmap §5.5 | dashboard 以逐出速率表达水位；`ITierLocal` 已有空间探针，导出为 gauge 是小改 C++ + 面板生成器 | 中 | 低 |
| `HeaderMap` 线性扫描 / `BlockQueue` 双拷贝 | roadmap §4.3 ⑧ | 绝对量小；有 profile 证据再动 | 低 | 低 |

## 2. 待验证（代码已落地，本机环境验证不了）

| 条目 | 出处 | 需要 |
| --- | --- | --- |
| Docker 镜像构建与 compose 四个 profile（默认 / redis / tikv / rados / e2e） | roadmap §6.3，[deployment.md §4](deployment.md) | 有 docker daemon 的机器：`docker compose build`，`docker compose --profile e2e run --rm e2e`（把 redis / tikv / rados 三条 SKIP 的 e2e 路径真正跑一次） |
| CPack RPM | roadmap §6.3，[deployment.md §3.2](deployment.md) | 有 `rpmbuild` 的机器：`cpack -G RPM`，`rpm -qp --scripts` 核对 scriptlet，安装/升级/卸载各走一遍 |
| mint 兼容基线 | roadmap §6.1，[testing.md §6](testing.md) | 有 docker 的机器跑 `ctest -R mint -V`，把每套件 PASS/FAIL/NA 计数记入 testing.md §6 |

## 3. 性能基线跑出的新问题（[performance-baseline.md](performance-baseline.md)）

| 条目 | 现象 | 入口 | 价值 | 难度 |
| --- | --- | --- | --- | --- |
| beast 的 TLS GET 明显落后 | 4 MiB GET 明文 4.6k ops/s、TLS 仅 1.5k，其他三驱动 TLS 在 3.0k 左右 | `src/http/drivers/beast/beast_server.cc` 的 `TlsStream` 写路径：asio ssl 的 record 切分与每块一次 strand 跳转；先用 `strace -c` 对比明文/TLS 的 syscall 计数 | 中 | 中 |
| 请求体路径未做对称优化 | PUT 各驱动持平，只有 beast 因读粒度 bug 修复而大幅提升 | 请求体是 pull 模型且要保留背压，预取需谨慎；候选：builtin `SocketBodyReader` 大块 recv、beast `expires_after` 每块重设定时器的开销 | 中 | 中 |

## 4. 长期 / 架构级（先想清目标场景再动）

| 条目 | 说明 |
| --- | --- |
| Versioning | 架构级（六后端 key 布局 / List 语义 / delete marker / GC 全动）；若做，**从 duostore 侧切入成本最低**（meta 是 KV，加 version 维度即可），localfs 的 key→路径映射容纳不下多版本 |
| SSE-C / SSE-S3 | 服务端加密；需先定密钥来源与 ETag/校验和语义 |
| OpenTelemetry 全量埋点 | 轻量 trace 层已做（W3C traceparent 透传、每请求一 span、日志关联，[s3-protocol.md §7](s3-protocol.md)）；otel-cpp 导出 span 是长期项 |
| HTTP/2 | S3 SDK 主流仍 HTTP/1.1，CDN / L7 前置场景才需要；前置代理终结 h2 见 [tls.md §6](tls.md) |
| 客户端断连独立取消源 | 刻意取舍：长 handler 靠 `request_timeout` 兜底，驱动在下一次 socket 操作时发现断连（[http-adapter.md §2.3](http-adapter.md)） |

## 5. 明确不做

| 条目 | 理由 |
| --- | --- |
| Object Lock / Legal Hold | 无 versioning 地基，WORM 语义无法成立 |
| Bucket Policy（IAM 语言） | per-credential policy 已覆盖多租户隔离，匿名公开桶由 website 面解决；IAM 求值器是独立子系统，投入不成比例 |
| SigV2 | AWS 已停用，客户端基本绝迹 |
| presigned POST | 需先写半个 multipart/form-data 流式解析器；CORS + presigned PUT 是更现代的替代路径 |
| cloudproxy 出方向 streaming 签名上传 | 复杂度高、收益仅是明文 HTTP 下的完整性（[cloudproxy-backend.md](cloudproxy-backend.md)） |
| rados 数据面 pack 层 | 代码内长注释已论证为设计边界（小对象放大交给 BlueStore `min_alloc_size`） |
| CivetWeb 等新 HTTP 驱动 | 四驱动已覆盖设计空间（[http-adapter.md §3.4](http-adapter.md)） |
| GitHub Actions CI | 项目已明确移除、不使用；自动化投入放在本地脚本矩阵（`scripts/check-all.sh`，[testing.md §8](testing.md)） |

## 6. 维护约定

- 新条目须给出**出处 / 入口 / 价值 / 难度**；做完即删，实现写进对应设计文档。
- 源码注释继续用 `roadmap §N` 引用归档文件的论证；本文的条目以 `backlog §N` 引用。
- 历史底账：[archive/gaps.md](archive/gaps.md)、[archive/issues.md](archive/issues.md)、
  [archive/roadmap.md](archive/roadmap.md)，只读。
