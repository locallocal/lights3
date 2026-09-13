# 总体架构

## 1. 设计目标

| 目标 | 说明 |
| --- | --- |
| S3 兼容 | 兼容主流 S3 客户端（aws cli、s3cmd、boto3、MinIO SDK），实现常用 API 子集 |
| HTTP 库可插拔 | 更换 HTTP 库不触碰协议层与存储层代码 |
| 高吞吐大对象 | 全链路流式传输，内存占用与对象大小无关 |
| 后端可扩展 | 新增存储后端只需实现一个接口并注册工厂 |
| 部署简单 | 单二进制 + 一个 YAML 配置文件；默认形态无外部服务依赖（duostore 后端可选接入外部 meta/data 服务：Redis/TiKV/Ceph，见 [storage/storage-backend.md](storage/storage-backend.md) §5） |

非目标（首期不做）：纠删码、bucket versioning、Object Lock、事件通知。
多网关（多个 `lights3` 进程共享同一份存储、前置负载均衡）**不是**非目标而是
按后端分级支持：凭证/网站/配额等控制面经 `auth.sync_interval` 定期同步
（[credential-management.md](credential-management.md) §10.3）；数据面在
duostore（redis / tikv meta + rados data）与 cloudproxy 上支持多网关（含跨网关
multipart，`read_lease` / `gc_enabled` 协同），localfs / xlocalfs / tiered 及
rocksdb / sqlite meta 仍是单实例，支持矩阵见
[deployment.md](../usage/deployment.md) §5。元数据/数据侧的多副本与水平
扩展不由网关实现，而是经 duostore 的 TiKV meta / RADOS data 插拔件
借外部系统获得。

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
- `IStorageBackend`：L2 与 L3 之间的边界（见 [storage/storage-backend.md](storage/storage-backend.md)）。

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

`src/main.cc` 只负责 ccmd 命令树（无子命令即启动服务；`lights3 duostore|tier|fsck|tables …`
为运维入口，见 [cli.md](../usage/cli.md)）；装配与启停顺序集中在
`lights3::Application`（`src/app/app.h/.cc`），由 `cli/cli_server.cc` 依次调用
`open_storage()` → `start_server()` → `run()`（略去日志与错误处理）：

```cpp
// Application::Application(config_path)：Config::load 读 YAML，Logger::init(cfg.log)
// open_storage()：线程池 + 指标 + 后端（--check-config / 离线运维命令只到这一步，不监听端口）
pool_     = std::make_shared<ThreadPool>(cfg_.runtime.io_threads);
metrics_  = std::make_shared<MetricsRegistry>();                       // 后端级指标注册表
backends_ = StorageRegistry::build(cfg_.backends, pool_, metrics_);    // 构造各后端

// start_server()：L2 装配 + 准入 + HTTP 监听
metered_  = meter_backends(backends_, metrics_);                       // per-backend 操作直方图装饰器
auto router = BucketRouter::build(cfg_.buckets, metered_);
auto auth   = SigV4Authenticator::build(cfg_.auth);                    // 静态凭证表
// 动态凭证（credential-management.md）：从默认后端加载并替换静态查表
cred_store_ = sync_wait(CredentialStore::load(router.default_backend(), cfg_.auth));
auth.set_provider(cred_store_);
// 同样从默认后端 .sys 加载：website / cors / tls_identity / lifecycle / quota / tenant / owner
// / usage（以及 tables.enabled 时的表桶目录），随后各自 start_background(pool_, sync_interval)
service_ = std::make_shared<S3Service>(std::move(router), std::move(auth), cfg_.http.base_domain);
service_->set_backend_metrics(metrics_);      // /-/metrics 追加后端级指标
service_->set_credential_store(cred_store_);  // per-credential policy 执行点
cred_store_->start_background(pool_);         // credentials_file 热加载轮询 + 多实例增量同步

// 按配置选择 HTTP 驱动（可用 CMake 选项在编译期裁剪）
server_   = HttpServerFactory::create(cfg_.http.driver, cfg_.http);
// dispatch 入口准入（concurrency.md §6，http/admission.h）：超限请求在信号量上排队，
// 等待者经池 executor 唤醒；Permit 系在流式响应体上，stall guard 与关停取消源也在此接入
pool_exec_ = std::make_shared<ThreadPoolExecutor>(*pool_);
inflight_  = std::make_shared<AsyncSemaphore>(cfg_.runtime.max_inflight_requests, pool_exec_.get());
server_->set_handler(make_admission_handler(inflight_, stall_sec_, shutdown_src_,
    [service = service_](HttpRequest req) { return service->dispatch(std::move(req)); }, ...));
server_->listen(cfg_.http.bind, cfg_.http.port);
// http.admin_port 配置时再起一个同驱动的 admin_server_（http-adapter.md §2.1）

// run()：装信号处理，阻塞直至 SIGTERM/SIGINT（SIGHUP = reload_config()），返回进程退出码
server_->run();
```

优雅退出（`Application::run()` → `shutdown()`）：信号经 self-pipe 唤醒看门狗线程调用
`server_->shutdown()`（信号处理函数本身仅做 async-signal-safe 的写管道）→ 停止 accept、
等待在途请求完成 → `run()` 返回 → `shutdown_src_->request_cancel()` + 等许可归还
（`http.shutdown_grace`，超时即退出码 3）→ 各 store / runner 的 `shutdown_background()`
（撤定时器并等在途后台任务收尾，**必须先于**后端 close 与线程池 join，否则后台协程
可能投递到已关闭的池）→ 后端 `close()` → 线程池 `join()`。

## 5. 配置文件示例

```yaml
http:
  driver: builtin          # builtin | beast | httplib | seastar（需对应 CMake 选项编译进来）
  bind: 0.0.0.0
  port: 9000
  io_threads: 4            # 异步驱动的 io_context 线程数
  max_header_size: 16KiB
  idle_timeout: 60s
  min_part_size: 5MiB      # multipart 非末片最小分片；0 = 不限制
  # base_domain: s3.local  # 非空时启用 virtual-host style 寻址

runtime:
  io_threads: 16           # 阻塞 IO 线程池大小（各后端默认共用；后端可配
                           # 同名键建专属池隔离，concurrency.md §3.1）
  max_inflight_requests: 1024

auth:
  credentials:
    - access_key: AKIDEXAMPLE
      secret_key: ${LIGHTS3_SECRET_1}     # 支持环境变量引用；留空则拒绝所有请求
  region: us-east-1
  # 凭证管理二期（credential-management.md §10）；动态凭证 SK 的 at-rest 加密
  # 不走配置：设环境变量 LIGHTS3_MASTER_KEY（openssl rand -hex 32）即启用
  # credentials_file: /etc/lights3/creds.json  # 外部凭证文件（热加载，仅数据面）
  # credentials_file_reload: 30s               # 文件 mtime 轮询周期；0s = 仅启动时加载
  # sync_interval: 0s                          # 多实例定期增量 reload .sys 凭证；0s = 关闭

backends:
  - name: localdata
    type: localfs                         # localfs | xlocalfs | memory | tiered | cloudproxy | duostore
    root: /var/lib/lights3/data
    staging: /var/lib/lights3/staging     # multipart 暂存，需与 root 同文件系统
  - name: aws-archive
    type: cloudproxy                      # 见 cloudproxy-design.md
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
  # format: json                        # 每行一个 JSON 对象（默认 text）
  # file: /var/log/lights3/lights3.log  # 空 = stderr；否则按大小轮转
  # async: true                         # 独立写线程
  # slow_request_threshold: 500ms       # 慢请求升 WARN 并附阶段耗时
```

可运行的最小示例见仓库根的 `config/lights3.yaml`。

## 6. 源码目录规划

```text
lights3/
├── CMakeLists.txt  Makefile  build.sh
├── config/lights3.yaml       # 可运行的最小示例配置
├── docs/                     # architecture/ usage/ development/ archive/ + en/ 镜像
├── docker/                   # Dockerfile、compose 与各后端示例配置
├── deploy/                   # grafana/ prometheus/ 监控素材
├── packaging/  scripts/      # deb/rpm 打包；构建/检查/基准/安装脚本
├── src/
│   ├── main.cc               # ccmd 命令树入口（无子命令即启动服务）
│   ├── app/                  # Application 装配与生命期（§4）、admin 后台作业
│   ├── cli/                  # 子命令：server / duostore / tier / fsck / tables
│   ├── tools/                # lights3-ctl 运维客户端（cli.md §3）
│   ├── core/                 # L4：与业务无关的基础设施
│   │   ├── task.h            #   Task<T>, sync_wait(_pumping), when_all, Started, with_timeout
│   │   ├── executor.h        #   IExecutor, InlineExecutor, PumpExecutor, resume_on
│   │   ├── thread_pool.h/.cc #   有界队列 + 背压, ThreadPoolExecutor
│   │   ├── semaphore.h       #   AsyncSemaphore（入口限流）
│   │   ├── timer.h/.cc       #   定时器线程（with_timeout 底座）
│   │   ├── cancel.h          #   协作式取消原语
│   │   ├── background.h/.cc  #   后台任务等待组（concurrency.md §7）
│   │   ├── config.h/.cc      #   YAML 解析 + 类型化配置
│   │   ├── metrics.h/.cc     #   后端级 metrics 注册表 + scope（backend=<name> 标签派发）
│   │   ├── log.h/.cc  trace  fault  version   # spdlog 门面；请求追踪；故障注入；构建标识
│   │   └── util/             #   hex, crypto(OpenSSL SHA256/HMAC), checksum, uri, time
│   ├── http/                 # L1
│   │   ├── model.h           #   HttpRequest/HttpResponse/BodyReader/HeaderMap
│   │   ├── server.h/.cc      #   IHttpServer, HttpServerFactory
│   │   ├── admission.h  stall_guard.h   # 入口准入（Permit 随流式体）、传输停滞守卫
│   │   ├── tls.h/.cc         #   OpenSSL 证书/SNI/mTLS/热重载（tls.md）
│   │   ├── pushpull.h        #   推模型 ↔ 拉模型翻转组件
│   │   └── drivers/
│   │       ├── common.h      #   驱动共享的契约实现（StreamPrefetch、IoBuffer、计数器）
│   │       ├── builtin/      #   POSIX socket 同步驱动（零依赖）
│   │       ├── beast/        #   Boost.Beast + asio 异步驱动
│   │       ├── httplib/      #   cpp-httplib 同步驱动（thread-per-request）
│   │       └── seastar/      #   Seastar shard-per-core 驱动（重依赖，默认关）
│   ├── s3/                   # L2
│   │   ├── service.h/.cc     #   S3Service::dispatch + 显式分派表
│   │   ├── router.h/.cc      #   URL → (bucket, key) 解析
│   │   ├── auth/             #   sigv4, credential_store（动态凭证）, policy
│   │   ├── handlers/         #   objects buckets list_objects multipart sts
│   │   │                     #   bucket_{cors,lifecycle,quota,website} quota_gate admin_*
│   │   ├── ratelimit  quota  usage  tenant  audit  lifecycle   # 限流、配额、用量、租户、审计、生命周期
│   │   ├── cors_store  website_store  tls_identity_store  sys_config_store.h  # .sys 持久化的运行期配置
│   │   ├── xml.h/.cc         #   S3 XML 编解码（自写小型生成器/解析器）
│   │   ├── errors.h/.cc      #   S3ErrorCode ↔ HTTP status ↔ XML body
│   │   └── metrics.h/.cc  checksum_guard.h   # Prometheus 文本格式指标；x-amz-checksum 校验
│   ├── storage/              # L3
│   │   ├── backend.h         #   IStorageBackend, ObjectMeta, 各 Options 结构
│   │   ├── registry.h/.cc    #   type 字符串 → 工厂（两阶段构建组合后端、per-backend 池）
│   │   ├── bucket_router.h/.cc
│   │   ├── metered_backend.h/.cc  meta_cache.h  request_stats.h  scrub_throttle.h   # 装饰器与共享设施
│   │   ├── validate.cc  listing.h/.cc  multipart.h/.cc   # 各后端共享逻辑
│   │   ├── memory/           #   内存后端（测试用）
│   │   ├── localfs/          #   本地文件系统后端
│   │   ├── xlocalfs/         #   localfs 的 io_uring 数据面变体
│   │   ├── tiered/           #   分层存储组合后端（见 storage/tiered-design.md）
│   │   ├── cloudproxy/       #   公有云代理后端（见 storage/cloudproxy-design.md）
│   │   └── duostore/         #   元数据/数据分离引擎（见 storage/duostore-design.md）
│   └── tables/               # S3 Tables / Iceberg REST catalog（s3-tables-design.md）
│       └── iceberg/          #   Iceberg 元数据、快照、manifest、Avro 读取
├── tests/
│   ├── unit/                 # L2/L3 纯逻辑测试（mock http + 内存后端）+ 驱动一致性测试
│   ├── e2e/                  # 起真实进程，用 aws cli / mint 打请求
│   ├── fuzz/  fixtures/      # libFuzzer 目标；测试素材
│   └── monitoring/  packaging/   # 监控规则与打包产物校验
└── third_party/              # httplib/ccmd/spdlog/json + rocksdb/sqlite/hiredis/client-c/seastar 子模块
```

依赖策略：核心（core/s3/storage）依赖标准库 + OpenSSL（SigV4 需要
SHA256/HMAC）+ spdlog（日志）+ ccmd（命令行，含 cflag）+ nlohmann/json（admin
凭证 API，不进公共头）；各 HTTP driver 与 cloudproxy、duostore 后端通过
CMake 选项（`LIGHTS3_DRIVER_BEAST`、`LIGHTS3_CLOUDPROXY`、`LIGHTS3_DUOSTORE`
及其子开关 `LIGHTS3_DUOSTORE_REDIS_META` / `LIGHTS3_DUOSTORE_SQLITE_META` /
`LIGHTS3_DUOSTORE_RADOS_DATA` / `LIGHTS3_DUOSTORE_TIKV_META` 等）隔离，
未启用则不参与编译。cloudproxy 不引入云 SDK，用 vendored httplib 自签
SigV4 直连远端。
