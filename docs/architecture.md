# 总体架构

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
│     产物：IHttpServer 的各实现                          │
│           （builtin/Beast/httplib/seastar）             │
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
│           定时器/取消/信号量、Config、Log、              │
│           工具类(hex/crypto/uri/time)                   │
└──────────────────────────────────────────────────────┘
```

核心解耦点有两个接口：

- `IHttpServer` / `HttpRequest` / `HttpResponse`：L1 与 L2 之间的边界
  （见 [http-adapter.md](http-adapter.md)）。
- `IStorageBackend`：L2 与 L3 之间的边界（见 [storage-backend.md](storage-backend.md)）。

L2 是纯逻辑层：不含任何 socket、epoll、具体 HTTP 库或存储 SDK 的头文件，
可以在单元测试中用 mock 的 Http 模型和内存后端完整覆盖。

## 3. 请求生命周期

以 `GET /mybucket/dir/a.bin`（bucket 路由到 LocalFs）为例：

```text
 client ──► [L1] HTTP 库 accept + 解析首部
              │  构造 HttpRequest{method,path,query,headers,BodyReader}
              ▼
            [L2] S3Service::dispatch(req)                  ← 协程入口
              │  1. 解析 (bucket, key)：path-style；配置 base_domain 后
              │     支持 virtual-host style
              │  2. SigV4Authenticator::verify(req)        ← 查 AK/SK、验签
              │  3. 显式分派表 → GetObject handler（handlers/objects.cc）
              ▼
            [L2] GetObject handler
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
  （posix IO、cloudproxy 的远端 HTTP 调用）都先 `co_await pool.schedule()`
  切到线程池，完成后切回（细节见 [concurrency.md](concurrency.md)）。

## 4. 进程结构与启动流程

`src/main.cc` 的装配流程（略去日志与错误处理）：

```cpp
int main(int argc, char** argv) {
    // gflags 解析 --config，Config::load 读 YAML
    auto cfg      = Config::load(FLAGS_config);
    Logger::init(parse_level(cfg.log_level));
    auto pool     = std::make_shared<ThreadPool>(cfg.runtime.io_threads);
    auto backends = StorageRegistry::build(cfg.backends, pool);   // 构造各后端
    auto router   = BucketRouter::build(cfg.buckets, std::move(backends));
    auto auth     = SigV4Authenticator::build(cfg.auth);          // 静态凭证表
    // 动态凭证（docs/credential-management.md）：从默认后端加载并替换静态查表
    auto cred_store = sync_wait(CredentialStore::load(router.default_backend(), cfg.auth));
    auth.set_provider(cred_store);
    auto service  = std::make_shared<S3Service>(std::move(router), std::move(auth),
                                                cfg.http.base_domain);      // L2 入口

    // 按配置选择 HTTP 驱动（可用 CMake 选项在编译期裁剪）
    auto server = HttpServerFactory::create(cfg.http.driver, cfg.http);
    // dispatch 入口限流（docs/concurrency.md §6）：超限请求在信号量上排队
    auto inflight = std::make_shared<AsyncSemaphore>(cfg.runtime.max_inflight_requests, ...);
    server->set_handler([=](HttpRequest req) -> Task<HttpResponse> {
        auto permit = co_await inflight->acquire();
        co_return co_await service->dispatch(std::move(req));
    });
    server->listen(cfg.http.bind, cfg.http.port);
    server->run();          // 阻塞直至收到 SIGTERM/SIGINT
}
```

优雅退出：信号处理函数触发 `server->shutdown()`（仅 async-signal-safe 操作）
→ 停止 accept、等待在途请求完成 → `run()` 返回 → 线程池 `join()`。

## 5. 配置文件示例

```yaml
http:
  driver: builtin          # builtin | beast | httplib | seastar（需对应 CMake 选项编译进来）
  bind: 0.0.0.0
  port: 9000
  io_threads: 4            # 异步驱动的 io_context 线程数
  max_header_size: 16KiB
  idle_timeout: 60s
  # base_domain: s3.local  # 非空时启用 virtual-host style 寻址

runtime:
  io_threads: 16           # 阻塞 IO 线程池大小（localfs、cloudproxy 共用）
  max_inflight_requests: 1024

auth:
  credentials:
    - access_key: AKIDEXAMPLE
      secret_key: ${LIGHTS3_SECRET_1}     # 支持环境变量引用；留空则拒绝所有请求
  region: us-east-1

backends:
  - name: localdata
    type: localfs                         # localfs | xlocalfs | memory | tiered | cloudproxy
    root: /var/lib/lights3/data
    staging: /var/lib/lights3/staging     # multipart 暂存，需与 root 同文件系统
  - name: aws-archive
    type: cloudproxy                      # 见 cloudproxy-backend.md
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

log:
  level: info
```

可运行的最小示例见仓库根的 `config/lights3.yaml`。

## 6. 源码目录规划

```text
lights3/
├── CMakeLists.txt
├── build.sh
├── config/lights3.yaml       # 可运行的最小示例配置
├── docs/
├── src/
│   ├── core/                 # L4：与业务无关的基础设施
│   │   ├── task.h            #   Task<T>, sync_wait, when_all, with_timeout
│   │   ├── executor.h        #   IExecutor, ThreadPoolExecutor
│   │   ├── thread_pool.h/.cc #   有界队列 + 背压
│   │   ├── semaphore.h       #   AsyncSemaphore（入口限流）
│   │   ├── timer.h/.cc       #   定时器线程（with_timeout 底座）
│   │   ├── cancel.h          #   协作式取消原语
│   │   ├── config.h/.cc      #   YAML 解析 + 类型化配置
│   │   ├── log.h             #   spdlog 门面
│   │   └── util/             #   hex, crypto(OpenSSL SHA256/HMAC), uri, time
│   ├── http/                 # L1
│   │   ├── model.h           #   HttpRequest/HttpResponse/BodyReader
│   │   ├── server.h/.cc      #   IHttpServer, HttpServerFactory
│   │   ├── pushpull.h        #   推模型 ↔ 拉模型翻转组件
│   │   └── drivers/
│   │       ├── common.h      #   驱动共享的契约实现
│   │       ├── builtin/      #   POSIX socket 同步驱动（零依赖）
│   │       ├── beast/        #   Boost.Beast + asio 异步驱动
│   │       ├── httplib/      #   cpp-httplib 同步驱动（thread-per-request）
│   │       └── seastar/      #   Seastar shard-per-core 驱动（重依赖，默认关）
│   ├── s3/                   # L2
│   │   ├── service.h/.cc     #   S3Service::dispatch + 显式分派表
│   │   ├── router.h/.cc      #   URL → (bucket, key) 解析
│   │   ├── auth/             #   sigv4.h/.cc, credential_store.h/.cc（动态凭证）
│   │   ├── handlers/         #   objects.cc buckets.cc list_objects.cc
│   │   │                     #   multipart.cc admin_credentials.cc
│   │   ├── xml.h/.cc         #   S3 XML 编解码（自写小型生成器/解析器）
│   │   ├── errors.h/.cc      #   S3ErrorCode ↔ HTTP status ↔ XML body
│   │   └── metrics.h/.cc     #   Prometheus 文本格式指标
│   └── storage/              # L3
│       ├── backend.h         #   IStorageBackend, ObjectMeta, 各 Options 结构
│       ├── registry.h/.cc    #   type 字符串 → 工厂（两阶段构建组合后端）
│       ├── bucket_router.h/.cc
│       ├── validate.cc  listing.h/.cc  multipart.h/.cc   # 各后端共享逻辑
│       ├── memory/           #   内存后端（测试用）
│       ├── localfs/          #   本地文件系统后端
│       ├── xlocalfs/         #   localfs 的 io_uring 数据面变体
│       ├── tiered/           #   分层存储组合后端（见 tiered-storage.md）
│       └── cloudproxy/       #   公有云代理后端（见 cloudproxy-backend.md）
├── tests/
│   ├── unit/                 # L2/L3 纯逻辑测试（mock http + 内存后端）
│   └── e2e/                  # 起真实进程，用 aws cli 打请求
└── third_party/              # httplib/gflags/spdlog/json 等子模块
```

依赖策略：核心（core/s3/storage）依赖标准库 + OpenSSL（SigV4 需要
SHA256/HMAC）+ spdlog（日志）+ gflags（命令行）+ nlohmann/json（admin
凭证 API，不进公共头）；各 HTTP driver 与 cloudproxy 后端通过 CMake 选项
（`LIGHTS3_DRIVER_BEAST`、`LIGHTS3_CLOUDPROXY` 等）隔离，未启用则不参与
编译。cloudproxy 不引入云 SDK，用 vendored httplib 自签 SigV4 直连远端。
