# 01 总体架构

## 1. 设计目标

| 目标 | 说明 |
| --- | --- |
| S3 兼容 | 兼容主流 S3 客户端（aws cli、s3cmd、boto3、MinIO SDK），实现常用 API 子集 |
| HTTP 库可插拔 | 更换 HTTP 库不触碰协议层与存储层代码 |
| 高吞吐大对象 | 全链路流式传输，内存占用与对象大小无关 |
| 后端可扩展 | 新增存储后端只需实现一个接口并注册工厂 |
| 部署简单 | 单二进制 + 一个 YAML 配置文件，无外部元数据服务依赖 |

非目标（首期不做）：多节点集群与数据分片、纠删码、bucket versioning、
Object Lock、事件通知。

## 2. 分层架构

自上而下四层，依赖方向严格单向（上层依赖下层的接口，不依赖实现）：

```text
┌──────────────────────────────────────────────────────┐
│ L1  HTTP Adapter 层                                   │
│     职责：网络监听、HTTP 解析、把请求转成中立模型         │
│     产物：IHttpServer 的各实现（Beast/httplib/CivetWeb） │
├──────────────────────────────────────────────────────┤
│ L2  S3 Protocol 层                                    │
│     职责：URL 路由、SigV4 认证、S3 语义、XML 编解码、     │
│           Multipart 状态机、错误码映射                   │
├──────────────────────────────────────────────────────┤
│ L3  Storage 层                                        │
│     职责：对象读写删列、multipart 落地                   │
│     产物：IStorageBackend 的各实现 + BucketRouter        │
├──────────────────────────────────────────────────────┤
│ L4  Core / Runtime 层（横切）                          │
│     职责：Task<T> 协程原语、Executor、ThreadPool、       │
│           Config、Log、Metrics、工具类(hex/hmac/uri)     │
└──────────────────────────────────────────────────────┘
```

核心解耦点有两个接口：

- `IHttpServer` / `HttpRequest` / `HttpResponse`：L1 与 L2 之间的边界（见 02 篇）。
- `IStorageBackend`：L2 与 L3 之间的边界（见 04 篇）。

L2 是纯逻辑层：不含任何 socket、epoll、具体 HTTP 库或存储 SDK 的头文件，
可以在单元测试中用 mock 的 Http 模型和内存后端完整覆盖。

## 3. 请求生命周期

以 `GET /mybucket/dir/a.bin`（bucket 路由到 LocalFs）为例：

```text
 client ──► [L1] HTTP 库 accept + 解析首部
              │  构造 HttpRequest{method,path,query,headers,BodyReader}
              ▼
            [L2] Router::dispatch(req)                     ← 协程入口 Task<void>
              │  1. 解析 (bucket, key)，支持 path-style 与 virtual-host style
              │  2. SigV4Authenticator::verify(req)        ← 查 AK/SK、验签
              │  3. 分派到 GetObjectHandler
              ▼
            [L2] GetObjectHandler::handle
              │  1. router.resolve("mybucket") → LocalFsBackend
              │  2. co_await backend.get_object(bucket,key,range)
              ▼
            [L3] LocalFsBackend::get_object
              │  1. co_await pool_.schedule()              ← 切换到 IO 线程池
              │  2. open + fstat + 读 .meta sidecar
              │  3. 返回 ObjectStream{meta, BodyReader}
              ▼
            [L2] 组装 HttpResponse{200, headers(ETag/Content-Type/...), BodyReader}
              ▼
            [L1] 循环 co_await body.read(buf) → 写 socket   ← 流式，64KB 块
 client ◄── 响应完成，归还连接
```

要点：

- **全链路流式**：L3 返回的是可分块拉取的 `BodyReader`，L1 边读边写 socket；
  PUT 方向对称，L1 的 `BodyReader` 一路透传到 L3 写文件/转发云端。
- **线程模型**：协议层逻辑跑在 HTTP 库的 IO 执行环境里；凡是可能阻塞的调用
  （posix IO、云 SDK）都先 `co_await pool.schedule()` 切到线程池，完成后
  切回（细节见 03 篇）。

## 4. 进程结构与启动流程

```cpp
int main(int argc, char** argv) {
    auto cfg      = Config::load(cli_args(argc, argv));   // YAML + 命令行覆盖
    Logger::init(cfg.log);
    auto pool     = std::make_shared<ThreadPool>(cfg.runtime.io_threads);
    auto registry = StorageRegistry::build(cfg.backends, pool);  // 构造各后端
    auto router   = BucketRouter::build(cfg.buckets, registry);  // bucket→后端
    auto auth     = SigV4Authenticator::build(cfg.auth);         // AK/SK 凭证表
    S3Service service{router, auth};                             // L2 入口

    // 按配置选择 HTTP 实现（也可编译期用 CMake 选项裁剪）
    auto server = HttpServerFactory::create(cfg.http.driver);    // "beast"|"httplib"|...
    server->set_handler([&](HttpRequest req) -> Task<HttpResponse> {
        co_return co_await service.dispatch(std::move(req));
    });
    server->listen(cfg.http.bind, cfg.http.port);
    server->run();          // 阻塞直至收到 SIGTERM/SIGINT
}
```

优雅退出：信号触发 `server->shutdown()` → 停止 accept → 等待在途请求完成
（带超时）→ 各 backend `close()`（冲刷 multipart 暂存）→ 线程池 join。

## 5. 配置文件示例

```yaml
http:
  driver: beast            # beast | httplib | civetweb
  bind: 0.0.0.0
  port: 9000
  io_threads: 4            # 异步驱动的 io_context 线程数
  max_header_size: 16KiB
  idle_timeout: 60s

runtime:
  io_threads: 16           # 阻塞 IO 线程池大小（LocalFs、云 SDK 共用）
  max_inflight_requests: 1024

auth:
  credentials:
    - access_key: AKIDEXAMPLE
      secret_key: ${LIGHTS3_SECRET_1}     # 支持环境变量引用
  region: us-east-1

backends:
  - name: localdata
    type: localfs
    root: /var/lib/lights3/data
    staging: /var/lib/lights3/staging     # multipart 暂存，需与 root 同文件系统
  - name: aws-archive
    type: cloudproxy
    endpoint: https://s3.us-west-2.amazonaws.com
    region: us-west-2
    access_key: AKIA...
    secret_key: ${AWS_SECRET}
    bucket_prefix: corp-archive-          # 远端真实 bucket = 前缀 + 本地 bucket 名

buckets:
  default_backend: localdata
  rules:
    - match: "archive-*"                  # glob，按声明顺序匹配
      backend: aws-archive
```

## 6. 源码目录规划

```text
lights3/
├── CMakeLists.txt
├── docs/
├── src/
│   ├── core/                 # L4：与业务无关的基础设施
│   │   ├── task.h            #   Task<T>, sync_wait, when_all
│   │   ├── executor.h        #   IExecutor, InlineExecutor
│   │   ├── thread_pool.h/.cc
│   │   ├── config.h/.cc
│   │   ├── log.h  metrics.h
│   │   └── util/             #   hex, sha256/hmac(vendored or OpenSSL), uri, time
│   ├── http/                 # L1
│   │   ├── model.h           #   HttpRequest/HttpResponse/BodyReader/BodyWriter
│   │   ├── server.h          #   IHttpServer, HttpServerFactory
│   │   └── drivers/
│   │       ├── beast/        #   Boost.Beast + asio 实现
│   │       ├── httplib/      #   cpp-httplib 实现（线程池模型）
│   │       └── civetweb/
│   ├── s3/                   # L2
│   │   ├── service.h/.cc     #   S3Service::dispatch
│   │   ├── router.h/.cc      #   URL → (bucket,key,操作) 解析
│   │   ├── auth/sigv4.h/.cc
│   │   ├── handlers/         #   get_object.cc put_object.cc list_objects.cc ...
│   │   ├── xml.h/.cc         #   S3 XML 编解码（自写小型生成器/解析器）
│   │   └── errors.h/.cc      #   S3ErrorCode ↔ HTTP status ↔ XML body
│   └── storage/              # L3
│       ├── backend.h         #   IStorageBackend, ObjectMeta, 各 Options 结构
│       ├── registry.h/.cc    #   type 字符串 → 工厂
│       ├── bucket_router.h/.cc
│       ├── localfs/
│       └── cloudproxy/
├── tests/
│   ├── unit/                 # L2/L3 纯逻辑测试（mock http + 内存后端）
│   └── e2e/                  # 起真实进程，用 aws cli / boto3 打请求
└── third_party/
```

依赖策略：核心（core/s3/storage-localfs）只依赖标准库 + OpenSSL（SigV4 需要
SHA256/HMAC）；各 HTTP driver 与 cloudproxy 的 SDK 依赖通过 CMake 选项
（`LIGHTS3_DRIVER_BEAST=ON` 等）隔离，未启用则不参与编译。
