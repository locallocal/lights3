# LightS3（中文介绍）

基于 C++20 的 S3 协议网关。对外暴露 S3 REST API，对内可插拔 HTTP 驱动与存储后端。
设计文档见[本目录索引](README.md)，当前实现对应 [architecture.md](architecture.md) 的架构。

*English version: [../README.md](../README.md)*

## 构建与测试

依赖：g++ ≥ 13（C++20 协程）、CMake ≥ 3.20、OpenSSL；
beast 驱动需要 Boost 头文件（≥ 1.75，header-only，无需编译库；
找不到系统 Boost 时可用 `BOOST_ROOT` 指向头文件目录，或 `-DLIGHTS3_DRIVER_BEAST=OFF` 裁剪）。
httplib、spdlog、gflags、nlohmann/json 等以 git 子模块置于 `third_party/`，
首次构建前需初始化（推荐直接 `./build.sh --test` 一步完成）。

```bash
git submodule update --init
cmake -B build
cmake --build build -j
ctest --test-dir build --output-on-failure   # 单测 + 每驱动 e2e（e2e 需要 curl ≥ 7.75）
```

## 运行

```bash
export LIGHTS3_SECRET_1=my-secret
./build/lights3 --config config/lights3.yaml
```

用任意 S3 客户端访问（示例用 curl 的 SigV4 支持）：

```bash
alias s3curl='curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "AKIDEXAMPLE:$LIGHTS3_SECRET_1"'
s3curl -X PUT http://127.0.0.1:9000/mybucket                      # CreateBucket
s3curl -X PUT --data-binary @file.bin http://127.0.0.1:9000/mybucket/file.bin
s3curl http://127.0.0.1:9000/mybucket?list-type=2                 # ListObjectsV2
s3curl -r 0-99 http://127.0.0.1:9000/mybucket/file.bin            # Range 下载
```

或使用 aws cli：`aws --endpoint-url http://127.0.0.1:9000 s3 ls`。

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
  与 aws-chunked 逐 chunk 签名链，单测覆盖 AWS 官方测试向量；运行期凭证管理
  （生成/查询/吊销 AK/SK，持久化到存储）经 `/-/admin/credentials`
  （[credential-management.md](credential-management.md)）
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
  （create/upload/list/complete/abort）

未实现（返回 NotImplemented，见 [s3-protocol.md](s3-protocol.md) 规划）：
UploadPartCopy、versioning、ACL/policy、lifecycle、SSE、Object Lock 等
扩展子资源；完整待办见 [todo.md](todo.md)。
