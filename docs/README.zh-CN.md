# LightS3（中文介绍）

基于 C++20 的 S3 协议网关。对外暴露 S3 REST API，对内可插拔 HTTP 驱动与存储后端。
设计文档见[本目录索引](README.md)，当前实现对应 [architecture.md](architecture.md) 的架构。

*English version: [../README.md](../README.md)*

## 构建与测试

依赖：g++ ≥ 13（C++20 协程）、CMake ≥ 3.20、OpenSSL；
beast 驱动需要 Boost 头文件（≥ 1.75，header-only，无需编译库；
找不到系统 Boost 时可用 `BOOST_ROOT` 指向头文件目录，或 `-DLIGHTS3_DRIVER_BEAST=OFF` 裁剪）。
httplib、spdlog、gflags 以 git 子模块置于 `third_party/`，首次构建前需初始化。

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
- **HTTP 驱动**：三个驱动全部落地，运行期由 `http.driver` 切换、编译期由
  CMake 选项裁剪，并共享同一套驱动一致性测试（[http-adapter.md](http-adapter.md) §4 契约）：
  - `builtin` —— 零依赖 POSIX socket，thread-per-connection；
  - `beast` —— Boost.Beast/Asio 异步驱动（默认性能路径）：N 线程共跑一个
    io_context，每连接一个 strand 上的会话协程，延迟 100-continue；
  - `httplib` —— cpp-httplib 同步驱动（thread-per-request，功能验证用），
    推模型 body 经有界队列翻转为拉模型
- **并发**：自研 `Task<T>` 惰性协程 + `ThreadPool`，阻塞 IO 经
  `co_await pool.schedule()` 下沉池线程，同步驱动经 `sync_wait` 桥接
- **认证**：SigV4 自实现（头签名 + presigned query），流式 payload SHA256 校验，
  单测覆盖 AWS 官方测试向量
- **存储**：LocalFs（sidecar 元数据、staging+rename 原子写）、
  XLocalFs（io_uring 数据面，原生 syscall 实现，无需 liburing）、Memory（测试用）；
  bucket 级 glob 路由
- **S3 API**：ListBuckets、Create/Head/DeleteBucket、Put/Get/Head/DeleteObject
  （含 Range 与条件请求）、ListObjectsV2（prefix/delimiter/分页）

未实现（返回 NotImplemented，见 [s3-protocol.md](s3-protocol.md) 规划）：
Multipart Upload、CopyObject、DeleteObjects 批量、cloudproxy 后端、aws-chunked 流式签名。
