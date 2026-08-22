# LightS3（中文介绍）

基于 C++20 的 S3 协议网关。对外暴露 S3 REST API，对内可插拔 HTTP 驱动与存储后端。
设计文档见[本目录索引](README.md)（英文翻译在 [en/](en/README.md)），
当前实现对应 [architecture.md](architecture.md) 的架构。

*English version: [../README.md](../README.md)*

## 架构

四层单向依赖，两个插拔边界是 `IHttpServer`（L1/L2）与 `IStorageBackend`（L2/L3）：

```text
              S3 客户端 (aws cli / boto3 / curl --aws-sigv4)
                                  │ HTTP/1.1
┌─ L1 · HTTP Adapter ─────────────▼─────────────────────────────────────┐
│ HttpServerFactory → IHttpServer，driver 运行期选择                     │
│   builtin : POSIX socket，thread-per-connection                       │
│   beast   : Boost.Asio 异步，N 个 io 线程，每连接会话协程               │
│   httplib : cpp-httplib 同步，thread-per-request                      │
│   seastar : shard-per-core reactor，进程级引擎（可选编译）              │
│ 中立 HttpRequest/HttpResponse 模型，流式 BodyReader body               │
└─────────────────────────────────┬─────────────────────────────────────┘
                                  ▼
┌─ L2 · S3 Protocol ────────────────────────────────────────────────────┐
│ S3Service::dispatch                                                   │
│   ├─ /-/healthz · /-/metrics · /-/readyz          （匿名端点）         │
│   ├─ /-/admin/credentials → admin handler（JSON，仅 root）            │
│   │        └─ CredentialStore ──(ICredentialProvider)──┐              │
│   └─ SigV4Authenticator.verify ◄───────────────────────┘              │
│        └─ per-credential policy 授权（bucket glob / readonly）         │
│             └─ 分派表（method + scope + query 标志）                   │
│                  └─ handlers: buckets / objects / list / multipart    │
│ XML 编解码 · S3Error 映射 · Metrics · 访问日志                         │
└─────────────────────────────────┬─────────────────────────────────────┘
                   IStorageBackend（Task<T>，流式）
┌─ L3 · Storage ──────────────────▼─────────────────────────────────────┐
│ BucketRouter：glob 规则 → 后端；".sys" 为凭证保留桶                     │
│   localfs  : sidecar .meta JSON，staging+rename 原子写                 │
│   xlocalfs : io_uring 数据面（原生 syscall），reaper 线程              │
│   memory   : 内存后端（测试用）                                        │
│ 共享：listing · multipart 状态 · 名称校验                              │
└─────────────────────────────────┬─────────────────────────────────────┘
                                  ▼
┌─ L4 · Core（横切） ────────────────────────────────────────────────────┐
│ Task<T> 惰性协程 · sync_wait / when_all · ThreadPool                  │
│ AsyncSemaphore（在途限流） · TimerQueue · YAML 配置 · spdlog           │
│ util: crypto（OpenSSL EVP）/ uri / time / hex                         │
└───────────────────────────────────────────────────────────────────────┘
```

一句话请求链路：driver 解析 HTTP 后把中立请求交给 `S3Service::dispatch`，
验签（SigV4，凭证经 `ICredentialProvider` 解析）并执行 per-credential policy
授权，再按 method/scope/query 分派到 handler 协程，与 `BucketRouter` 选中的
后端流式收发数据；各层都跑在共享 `ThreadPool` 调度的 `Task<T>` 协程上。

## 构建与测试

依赖：g++ ≥ 13（C++20 协程）、CMake ≥ 3.20、OpenSSL；
beast 驱动需要 Boost 头文件（≥ 1.75，header-only，无需编译库；
找不到系统 Boost 时可用 `BOOST_ROOT` 指向头文件目录，或 `-DLIGHTS3_DRIVER_BEAST=OFF` 裁剪）。
ccmd、spdlog、httplib、nlohmann/json、rocksdb、hiredis、sqlite 以 git 子模块
置于 `third_party/`，首次构建前需初始化（rocksdb 为必需——DuoStore 后端默认
开启；hiredis/sqlite 供其可选 meta 引擎）。

```bash
./build.sh --test        # 子模块 + cmake + ninja + ctest 一步完成
```

或手动：

```bash
git submodule update --init third_party/spdlog \
    third_party/httplib third_party/json third_party/rocksdb \
    third_party/hiredis third_party/sqlite
git submodule update --init --recursive third_party/ccmd   # 内嵌 cflag
cmake -B build
cmake --build build -j
ctest --test-dir build --output-on-failure   # 单测 + 每驱动/每后端 e2e（e2e 需要 curl ≥ 7.75）
```

seastar 驱动默认关闭（依赖重），`./build.sh --seastar` 开启。可选后端开关：
`--redis` / `--sqlite`（DuoStore meta 引擎）、`--tikv`（需系统 gRPC/Poco，
惰性拉取 client-c 子模块）、`--rados`（需 librados，或 `-DLIGHTS3_RADOS_ROOT`
指向解包目录）。这些 CMake 开关写入构建缓存后是粘性的——要关掉请配合
`--clean` 或独立的 `-B build-x` 目录。Sanitizer 构建：`./build.sh --asan` /
`--tsan`。

MinIO mint 兼容集为手动门槛（未接入 ctest；依赖 docker，无 docker 自动
SKIP，详见 [s3-protocol.md](s3-protocol.md) §8）：

```bash
tests/e2e/run_mint.sh build/lights3 s3cmd awscli
```

## 运行

```bash
export LIGHTS3_SECRET_1=my-secret
# 可选：动态凭证 SK 落盘 AES-256-GCM 加密；启用后缺 key/错 key 会启动失败
export LIGHTS3_MASTER_KEY=$(openssl rand -hex 32)
./build/lights3 --config=config/lights3.yaml
```

运维 CLI `s3adm`（凭证、桶网站配置、压测）与 `lights3` 完整命令树见 [cli.md](cli.md)。

用任意 S3 客户端访问（示例用 curl 的 SigV4 支持）：

```bash
alias s3curl='curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "AKIDEXAMPLE:$LIGHTS3_SECRET_1"'
s3curl -X PUT http://127.0.0.1:9000/mybucket                      # CreateBucket
s3curl -X PUT --data-binary @file.bin http://127.0.0.1:9000/mybucket/file.bin
s3curl http://127.0.0.1:9000/mybucket?list-type=2                 # ListObjectsV2
s3curl -r 0-99 http://127.0.0.1:9000/mybucket/file.bin            # Range 下载
```

或使用 aws cli：`aws --endpoint-url http://127.0.0.1:9000 s3 ls`。

## 作为 systemd 服务安装

先构建二进制，再以 root 权限运行安装脚本：

```bash
./build.sh -DLIGHTS3_BUILD_TESTS=OFF
sudo ./scripts/install.sh
sudo /usr/local/sbin/lights3ctl status
```

安装脚本会创建专用的 `lights3` 系统用户，把服务安装到 `/usr/local/bin`，
并以 `/var/lib/lights3` 为工作目录读取 `/etc/lights3/lights3.yaml`。首次安装
还会在 `/etc/lights3/lights3.env` 中生成随机凭证；升级安装不会覆盖已有的
配置和凭证。可通过 `lights3ctl help` 查看启停、重启、状态及日志命令；若需
先调整配置再启动，请向安装脚本传入 `--no-start`。

## 当前实现范围

- **架构**：四层（HTTP Adapter / S3 Protocol / Storage / Core），依赖单向；
  `IHttpServer` 与 `IStorageBackend` 两个插拔边界均已落地
- **HTTP 驱动**：四个驱动全部落地，运行期由 `http.driver` 切换、编译期由
  CMake 选项裁剪，并共享同一套驱动一致性测试（[http-adapter.md](http-adapter.md) §4 契约）：
  - `builtin` —— 零依赖 POSIX socket，thread-per-connection；
  - `beast` —— Boost.Beast/Asio 异步驱动（默认性能路径）：N 线程共跑一个
    io_context，每连接一个 strand 上的会话协程，延迟 100-continue；
  - `httplib` —— cpp-httplib 同步驱动（thread-per-request，功能验证用），
    推模型 body 经有界队列翻转为拉模型；
  - `seastar` —— shard-per-core reactor 驱动（编译期可选，
    `-DLIGHTS3_DRIVER_SEASTAR=ON`），进程级引擎单例，会话协程把
    `seastar::future` 桥接进项目的 `Task<T>`
- **并发**：自研 `Task<T>` 惰性协程 + `ThreadPool`，阻塞 IO 经
  `co_await pool.schedule()` 下沉池线程，同步驱动经 `sync_wait` 桥接
- **认证**：SigV4 自实现（头签名 + presigned query），流式 payload SHA256 校验
  与 aws-chunked 逐 chunk 签名链，单测覆盖 AWS 官方测试向量；presigned URL
  双向约束（过去侧 `X-Amz-Expires`，未来侧 15min 时钟偏移拒绝）
- **凭证管理**（[credential-management.md](credential-management.md)）：
  运行期生成/查询/吊销 AK/SK 经 `/-/admin/credentials`，持久化到存储；
  三来源模型（静态配置 = root、外部凭证文件、动态），仅静态凭证可调 admin API；
  SK at-rest AES-256-GCM 加密（`LIGHTS3_MASTER_KEY`）；外部凭证文件热加载
  （`auth.credentials_file`）；多实例定期增量同步（`auth.sync_interval`）；
  per-credential policy（bucket glob 白名单 + readonly）
- **存储**：LocalFs（sidecar 元数据、staging+rename 原子写）、
  XLocalFs（io_uring 数据面，原生 syscall 实现，无需 liburing）、Memory（测试用）、
  CloudProxy（自签 SigV4 直连远端 S3，[cloudproxy-backend.md](cloudproxy-backend.md)）、
  Tiered（冷数据下沉云端的组合后端，[tiered-storage.md](tiered-storage.md)）、
  DuoStore（元数据/数据分离引擎，meta 可选 RocksDB/Redis/SQLite/TiKV、
  data 可选本地 fs/RADOS，[duostore-backend.md](duostore-backend.md)）；
  bucket 级 glob 路由
- **S3 API**：ListBuckets、Create/Head/DeleteBucket、Put/Get/Head/DeleteObject
  （含 Range 与条件请求）、CopyObject、DeleteObjects 批量、
  ListObjectsV2（prefix/delimiter/分页）、Multipart Upload
  （create/upload/upload-part-copy/list/complete/abort；UploadPartCopy 支持
  `x-amz-copy-source-range` 与 copy-source 条件头，源与目标可在不同后端）

设计上明确不支持（返回 NotImplemented，见 [s3-protocol.md](s3-protocol.md) §1）：
versioning、ACL 细粒度（只认 private）、bucket policy、website、lifecycle、
tagging/CORS、SSE-C/KMS、Object Lock、presigned POST。

## 文档

设计文档中文原文在 [docs/](README.md)，英文翻译在 [docs/en/](en/README.md)，
章节编号与中文版一致（源码注释以 `docs/<name>.md §N` 形式引用章节）。

| 文档（[中文](README.md) · [en](en/README.md)） | 内容 |
| --- | --- |
| [architecture](architecture.md) | 总体架构、分层、请求生命周期、代码布局 |
| [http-adapter](http-adapter.md) | HTTP 插拔层：中立请求/响应模型、流式 body、各驱动要点 |
| [concurrency](concurrency.md) | Task 协程、Executor 抽象、线程池、同步/异步驱动桥接 |
| [storage-backend](storage-backend.md) | `IStorageBackend`、LocalFs/XLocalFs、bucket 路由、新增后端指南 |
| [s3-protocol](s3-protocol.md) | API 范围、SigV4（含 presigned 与时钟偏移）、XML 编解码、错误、mint 门禁 |
| [credential-management](credential-management.md) | AK/SK 管理 API、三来源模型、`.sys` 持久化、at-rest 加密、policy |
| [object-read-write-flow](object-read-write-flow.md) | 端到端读写路径、BodyReader 链、staging 提交、fd 快照读 |
| [tiered-storage](tiered-storage.md) | 冷数据下沉云端、stub 元数据、透明回读 |
| [cloudproxy-backend](cloudproxy-backend.md) | 自签 SigV4 转发远端 S3、流式泵、重试 |
| [duostore-backend](duostore-backend.md) | 元数据/数据分离引擎：RocksDB meta、chunk/pack、GC |
| [duostore-redis-meta](duostore-redis-meta.md) | Redis IMetaStore：hiredis + Lua 守卫提交 |
| [duostore-sqlite-meta](duostore-sqlite-meta.md) | SQLite IMetaStore：内嵌 amalgamation、WAL、读连接池 |
| [duostore-rados-data](duostore-rados-data.md) | RADOS IDataStore：librados，chunk → rados 对象 |
| [duostore-tikv-meta](duostore-tikv-meta.md) | TiKV IMetaStore：client-c + 2PC 侧车 |
| [cli](cli.md) | `lights3` / `s3adm` 命令参考：启动、duostore dump/load、cred/website/bench |
