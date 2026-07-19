# LightS3 —— 基于 C++ 的 S3 协议网关设计文档

LightS3 是一个用 C++20 实现的 S3 协议网关（Gateway）。它对外暴露标准 S3 REST API，
对内将请求路由到可插拔的存储后端。设计上强调三点：

1. **HTTP 协议库可插拔** —— 核心业务逻辑不依赖任何具体 HTTP 库，通过适配层可以在
   Boost.Beast、cpp-httplib、CivetWeb 等实现之间切换。
2. **协程 + 线程池双执行模型** —— 请求处理链路以 C++20 协程表达，阻塞型操作
   （磁盘 IO、云 SDK 调用）卸载到专用线程池，两种模型通过统一的 Executor 抽象衔接。
3. **多存储后端** —— 后端以 `IStorageBackend` 接口抽象，首期实现两种：
   本地文件系统（LocalFs）与映射到公有云对象存储的代理后端（CloudProxy），
   按 bucket 粒度路由。

## 文档目录

| 文档 | 内容 |
| --- | --- |
| [01-architecture.md](01-architecture.md) | 总体架构、分层设计、请求生命周期、代码目录规划 |
| [02-http-adapter.md](02-http-adapter.md) | HTTP 协议库插拔层：中立请求/响应模型、流式 Body、适配器实现要点 |
| [03-concurrency.md](03-concurrency.md) | 并发模型：Task 协程、Executor 抽象、线程池、同步/异步 HTTP 库的统一 |
| [04-storage-backend.md](04-storage-backend.md) | 存储后端抽象、LocalFs 后端、CloudProxy 后端、bucket 路由 |
| [05-s3-protocol.md](05-s3-protocol.md) | S3 协议实现：API 范围、SigV4 认证、Multipart Upload、错误码映射 |
| [06-credential-management.md](06-credential-management.md) | 凭证管理：AK/SK 生成/查询 API、两级权限、`.sys` 存储持久化 |
| [07-object-read-write-flow.md](07-object-read-write-flow.md) | 对象读写流程：三层代码路径串联、BodyReader 包装链、staging 原子提交、fd 快照读 |
| [08-tiered-storage.md](08-tiered-storage.md) | 分层存储（设计稿）：冷数据下沉公有云、stub 元数据、透明回读与缓存回填 |
| [09-cloudproxy-backend.md](09-cloudproxy-backend.md) | CloudProxy 后端：自签 SigV4 + httplib 直连远端 S3、双向流式泵、错误映射与重试 |

## 一页纸架构图

```text
                ┌────────────────────────────────────────────────┐
                │                  HTTP Adapter 层                │
                │  BeastServer / HttplibServer / CivetWebServer   │
                │        (实现 IHttpServer, 编译期/运行期可选)      │
                └───────────────────────┬────────────────────────┘
                                        │ HttpRequest / HttpResponse (中立模型)
                ┌───────────────────────▼────────────────────────┐
                │                  S3 Protocol 层                 │
                │  Router → SigV4 Auth → S3 Handler (协程)        │
                │  XML 编解码 / 错误码映射 / Multipart 状态机       │
                └───────────────────────┬────────────────────────┘
                                        │ IStorageBackend (异步流式接口)
                ┌───────────────────────▼────────────────────────┐
                │                  Storage 层                     │
                │   LocalFsBackend        CloudProxyBackend       │
                │   (posix + 线程池)      (SigV4 签名直接转发)     │
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
  后续如需 object 级分层可在此之上叠加。
- **元数据 sidecar 而非嵌入数据文件**：LocalFs 后端用 `.meta` sidecar JSON 存储
  Content-Type、ETag、自定义元数据，保持数据文件与普通文件系统工具兼容。
