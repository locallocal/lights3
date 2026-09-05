# LightS3 —— 基于 C++ 的 S3 协议网关设计文档

LightS3 是一个用 C++20 实现的 S3 协议网关（Gateway）。它对外暴露标准 S3 REST API，
对内将请求路由到可插拔的存储后端。设计上强调三点：

1. **HTTP 协议库可插拔** —— 核心业务逻辑不依赖任何具体 HTTP 库，通过适配层可以在
   builtin（POSIX socket）、Boost.Beast、cpp-httplib、Seastar 等驱动之间切换。
2. **协程 + 线程池双执行模型** —— 请求处理链路以 C++20 协程表达，阻塞型操作
   （磁盘 IO、远端 S3 调用）卸载到专用线程池，两种模型通过统一的 Executor 抽象衔接。
3. **多存储后端** —— 后端以 `IStorageBackend` 接口抽象，按 bucket 粒度路由。
   已实现：本地文件系统（LocalFs / XLocalFs）、内存（Memory，测试用）、
   公有云代理（CloudProxy）、冷热分层组合（Tiered）、元数据/数据分离引擎
   （DuoStore，meta 可选 RocksDB/Redis/SQLite/TiKV，data 可选本地 fs/RADOS）。

## 文档目录

| 文档 | 内容 |
| --- | --- |
| [architecture.md](architecture.md) | 总体架构、分层设计、请求生命周期、代码目录规划 |
| [config-reload.md](config-reload.md) | 配置热重载：SIGHUP / admin API / `s3adm reload`，整体校验、可热更新子集与"需重启"报告（roadmap §4.4） |
| [tls.md](tls.md) | TLS：四驱动 HTTPS、证书热重载、mTLS / cipher / 最低版本 / SNI 多证书、反向代理终结样例（roadmap §4.1） |
| [http-adapter.md](http-adapter.md) | HTTP 协议库插拔层：中立请求/响应模型、流式 Body、适配器实现要点 |
| [concurrency.md](concurrency.md) | 并发模型：Task 协程、Executor 抽象、线程池、同步/异步 HTTP 库的统一 |
| [coroutine-internals.md](coroutine-internals.md) | 协程实现内幕：Task promise 布局与对称转移、顶层驱动方式、when_all/with_timeout、取消的竞态协议与生命期守则 |
| [storage-backend.md](storage-backend.md) | 存储后端抽象、LocalFs/XLocalFs、DuoStore 概览与新增后端指南、bucket 路由 |
| [storage/](storage/README.md) | 各存储实现的实现级详解（13 篇）：数据结构、磁盘/键空间布局、读写流程、并发与崩溃一致性 |
| [s3-protocol.md](s3-protocol.md) | S3 协议实现：API 范围、SigV4 认证（含 presigned 与时钟偏移）、Multipart Upload、错误码映射、mint 兼容集 |
| [credential-management.md](credential-management.md) | 凭证管理：AK/SK 生成/查询/吊销 API、三来源模型（静态 root / 文件 / 动态）、`.sys` 持久化；二期：SK at-rest 加密、凭证文件热加载、多实例同步、per-credential policy |
| [multi-tenancy.md](multi-tenancy.md) | 用量统计、桶/租户配额、租户实体与桶归属、分级管理面、审计日志（roadmap §3.9 全链） |
| [testing.md](testing.md) | 测试体系：ctest 矩阵与标签、website/s3adm/故障注入 e2e、fuzz harness、故障注入门面、性能门禁与 soak、mint、ubsan/coverage、一键矩阵脚本（roadmap §6.1） |
| [deployment.md](deployment.md) | 构建与分发：`--version` / git commit 嵌入、`cmake --install` 安装树、CPack deb/rpm、Dockerfile + compose（含 redis/tikv/rados e2e profile）、`install.sh` 升级的回滚与卸载（roadmap §6.3） |
| [performance-baseline.md](performance-baseline.md) | 性能基线：`scripts/bench_matrix.sh` 的 4 驱动 × TLS 开关 × put/get 矩阵，§4.3 数据面优化前后对照，复现方法（roadmap §4.3） |
| [monitoring.md](monitoring.md) | 监控消费侧：`deploy/` 下的 Prometheus 抓取配置与告警/recording 规则、Grafana dashboard 及其生成器、资产对账测试（roadmap §5.5，零 C++） |
| [object-read-write-flow.md](object-read-write-flow.md) | 对象读写流程：三层代码路径串联、BodyReader 包装链、staging 原子提交、fd 快照读 |
| [tiered-storage.md](tiered-storage.md) | 分层存储：冷数据下沉公有云、stub 元数据、透明回读与缓存回填 |
| [cloudproxy-backend.md](cloudproxy-backend.md) | CloudProxy 后端：自签 SigV4 + httplib 直连远端 S3、双向流式泵、错误映射与重试 |
| [duostore-backend.md](duostore-backend.md) | DuoStore 后端：元数据/数据分离引擎，RocksDB 元数据 + chunk 切片/pack 聚合/GC |
| [duostore-redis-meta.md](duostore-redis-meta.md) | DuoStore 的 Redis IMetaStore：hiredis + Lua guarded-commit，多网关共享 meta |
| [duostore-sqlite-meta.md](duostore-sqlite-meta.md) | DuoStore 的 SQLite IMetaStore：amalgamation 内嵌，WAL + 读池/单写连接 |
| [duostore-rados-data.md](duostore-rados-data.md) | DuoStore 的 RADOS IDataStore：librados 直连，chunk → rados 对象 |
| [duostore-tikv-meta.md](duostore-tikv-meta.md) | DuoStore 的 TiKV IMetaStore：client-c + 2PC 侧车，meta 水平扩展 |
| [cli.md](cli.md) | 命令行工具：`lights3` 启动、`duostore dump/load/gc/scan`、`tier scan/gc/reconcile`、`fsck` 离线巡检、`s3adm` 的 cred/website/bench/fsck/quota/tenant/usage 命令、ccmd 选项语义与退出码 |
| [backlog.md](backlog.md) | 待办与后续规划：分期保留的设计、代码已落地但本机无法验证的项、基线跑出的新问题、长期项、明确不做清单；做完即删 |
| [backlog-sequence.md](backlog-sequence.md) | backlog §1 十个分期保留项的实施顺序：五个阶段、依赖关系、各项范围 / 入口 / 验收 / 预估与记账表 |
| [archive/](archive/gaps.md) | 已收口的历史底账（gaps.md / issues.md / roadmap.md），只读归档——源码注释中 `docs/archive/<name>.md §N` 与 `roadmap §N` 引用的论证出处 |

*另有中文项目介绍 [README.zh-CN.md](README.zh-CN.md)（构建/运行/当前实现范围）；
全部设计文档的英文翻译在 [en/](en/README.md)，章节编号与中文版一一对应，
代码注释中的 `docs/<name>.md §N` 引用两边通用。*

## 一页纸架构图

```text
                ┌────────────────────────────────────────────────┐
                │                  HTTP Adapter 层                │
                │  Builtin / Beast / Httplib / Seastar 驱动       │
                │        (实现 IHttpServer, 编译期/运行期可选)      │
                └───────────────────────┬────────────────────────┘
                                        │ HttpRequest / HttpResponse (中立模型)
                ┌───────────────────────▼────────────────────────┐
                │                  S3 Protocol 层                 │
                │  Router → SigV4 Auth → policy 授权 → Handler    │
                │  XML 编解码 / 错误码映射 / Multipart 状态机       │
                └───────────────────────┬────────────────────────┘
                                        │ IStorageBackend (异步流式接口)
                ┌───────────────────────▼────────────────────────┐
                │                  Storage 层                     │
                │  LocalFs/XLocalFs · Memory · CloudProxy         │
                │  Tiered(组合) · DuoStore(meta/data 可插拔)      │
                └────────────────────────────────────────────────┘
                          ▲ 横切：Executor(协程调度) / ThreadPool /
                            Config / Logging / Metrics
```

## 关键取舍摘要

- **C++20 协程作为一等公民**：Handler、存储接口全部返回 `Task<T>`；同步 HTTP 库
  通过 `sync_wait` 桥接，异步库通过 io_context 集成，业务代码只写一份。
- **中立 HTTP 模型 + 流式 Body**：请求/响应体不落地为完整内存缓冲，以
  `BodyReader`/`BodyWriter` 拉/推接口传递，支撑大对象上传下载与 SigV4
  chunked 签名校验。
- **bucket 级路由而非 object 级**：路由规则简单、可静态配置，避免元数据服务；
  object 级分层已按此思路以组合后端形式叠加实现（见 [tiered-storage.md](tiered-storage.md)）。
- **元数据 sidecar 而非嵌入数据文件**：LocalFs 后端用 `.lights3-meta` sidecar
  （TSV，`fs_util.h:kSidecarSuffix`）存储 Content-Type、ETag、自定义元数据，
  保持数据文件与普通文件系统工具兼容（xattr 同批提交为主，sidecar 兼作外部
  工具可读与回落，见 [storage-backend.md](storage-backend.md) §3.1）。
