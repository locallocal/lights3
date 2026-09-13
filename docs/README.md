# LightS3 文档

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

文档按读者分三组：**架构设计**（系统怎么构成、为什么这样做）、**使用**（怎么部署、
配置、运维）、**开发**（怎么构建、测试、贡献）。中文是原文，全部文档的英文翻译在
[en/](en/README.md)，章节编号与中文版一一对应，代码注释中的
`docs/<group>/<name>.md §N` 引用两边通用。

### 架构设计（[architecture/](architecture/)）

| 文档 | 内容 |
| --- | --- |
| [overview.md](architecture/overview.md) | 总体架构、分层设计、请求生命周期、进程装配、源码目录 |
| [http-adapter.md](architecture/http-adapter.md) | HTTP 协议库插拔层：中立请求/响应模型、流式 Body、四个驱动的实现要点 |
| [concurrency.md](architecture/concurrency.md) | 并发模型：Task 协程、Executor 抽象、线程池、同步/异步 HTTP 库的统一 |
| [coroutine-internals.md](architecture/coroutine-internals.md) | 协程实现内幕：Task promise 布局与对称转移、顶层驱动方式、when_all/with_timeout、取消的竞态协议与生命期守则 |
| [object-read-write-flow.md](architecture/object-read-write-flow.md) | 对象读写流程：三层代码路径串联、BodyReader 包装链、staging 原子提交、fd 快照读 |
| [s3-protocol.md](architecture/s3-protocol.md) | S3 协议实现：API 范围、SigV4 认证（含 presigned、STS 与时钟偏移）、Multipart Upload、错误码映射、可观测性、mint 兼容集 |
| [credential-management.md](architecture/credential-management.md) | 凭证管理：AK/SK 管理 API、三来源模型（静态 root / 文件 / 动态）、`.sys` 持久化、SK at-rest 加密、凭证文件热加载、多实例同步、per-credential policy、STS 会话 |
| [multi-tenancy.md](architecture/multi-tenancy.md) | 用量统计、桶/租户配额、租户实体与桶归属、分级管理面、审计日志 |
| [static-website.md](architecture/static-website.md) | 静态网站托管的设计：匿名判定与双重授权闸门、请求处理顺序、WebsiteStore 持久化与多实例同步、错误页渲染、重定向 Location 生成、放大面与限速 |
| [s3-tables-design.md](architecture/s3-tables-design.md) | S3 Tables / Apache Iceberg REST Catalog：建在 `.sys` 上的目录状态与 CAS 提交协议、REST 端点与错误模型、Avro 深校验与诊断/恢复、表桶守卫与凭证下发、维护作业、多网关矩阵、views、duostore-meta 目录后备 |
| [storage/](architecture/storage/README.md) | 存储层全部文档：设计层——`storage-backend.md`（接口抽象、bucket 路由、LocalFs/XLocalFs、新增后端指南）与 tiered / cloudproxy / duostore（含 redis / sqlite / tikv meta、rados data 引擎）各自的 `*-design.md`；实现层——13 篇实现级详解（数据结构、磁盘/键空间布局、读写流程、并发与崩溃一致性，只有中文） |

### 使用（[usage/](usage/)）

| 文档 | 内容 |
| --- | --- |
| [deployment.md](usage/deployment.md) | 构建与分发：`--version` / git commit 嵌入、`cmake --install` 安装树、deb/rpm 包、`docker/` 下的 Dockerfile + compose（含 redis/tikv/rados/multi/e2e profile）、多网关部署、升级回滚与卸载 |
| [cli.md](usage/cli.md) | 命令行工具：`lights3` 启动与 `duostore` / `tier` / `fsck` / `tables` 子命令，`lights3-ctl` 的 cred/website/bench/fsck/quota/tenant/usage/tables 等命令、选项语义与退出码 |
| [config-reload.md](usage/config-reload.md) | 配置热重载：SIGHUP / admin API / `lights3-ctl reload`，整体校验、可热更新子集与"需重启"报告 |
| [tls.md](usage/tls.md) | TLS：四驱动 HTTPS、证书热重载、mTLS / cipher / 最低版本 / SNI 多证书、反向代理终结样例 |
| [monitoring.md](usage/monitoring.md) | 监控消费侧：`deploy/` 下的 Prometheus 抓取配置与告警/recording 规则、Grafana dashboard 及其生成器 |
| [static-website.md](usage/static-website.md) | 静态网站托管使用手册：启用方式（YAML / `?website` API / `lights3-ctl`）、配置项对照、匿名访问范围、index / error 文档、重定向、限速、指标与排错 |

配置键的完整说明以 [config/lights3.yaml](../config/lights3.yaml) 的注释为准；项目
介绍（构建 / 运行 / 当前实现范围）见 [README.zh-CN.md](README.zh-CN.md)。

### 开发（[development/](development/)）

| 文档 | 内容 |
| --- | --- |
| [contributing.md](development/contributing.md) | 开发指南：仓库布局、构建变体、验证套路、代码与文档约定、分支与提交 |
| [testing.md](development/testing.md) | 测试体系：ctest 矩阵与标签、e2e 各段、fuzz harness、故障注入、性能门禁与 soak、mint、ubsan/coverage、一键矩阵脚本、Makefile |
| [performance-baseline.md](development/performance-baseline.md) | 性能基线：`scripts/bench_matrix.sh` 的 4 驱动 × TLS 开关 × put/get 矩阵、数据面优化前后对照、2026-09-13 复测（beast TLS 与请求体 MD5 流水化）、复现方法 |
| [todo.md](development/todo.md) | 待办与规划：待验证项、S3 Tables 收尾项、长期项、明确不做清单；做完即删 |

### 归档（[archive/](archive/)）

已收口的历史底账（gaps.md / issues.md / roadmap.md / backlog.md / backlog-sequence.md）
与做完的设计步骤文档（multi-gateway-multipart-design.md），只读、不再更新；源码注释中
`docs/archive/<name>.md §N`、`roadmap §N`、`backlog §N`、`backlog-sequence ①…⑩`、
`multi-gateway-multipart §4 ①…④` 引用的论证出处，不要删除。

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
  object 级分层已按此思路以组合后端形式叠加实现（见 [tiered-design.md](architecture/storage/tiered-design.md)）。
- **元数据 sidecar 而非嵌入数据文件**：LocalFs 后端用 `.lights3-meta` sidecar
  （TSV，`fs_util.h:kSidecarSuffix`）存储 Content-Type、ETag、自定义元数据，
  保持数据文件与普通文件系统工具兼容（xattr 同批提交为主，sidecar 兼作外部
  工具可读与回落，见 [storage/storage-backend.md](architecture/storage/storage-backend.md) §3.1）。
