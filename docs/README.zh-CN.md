# LightS3（中文介绍）

基于 C++20 的 S3 协议网关。对外暴露 S3 REST API，对内可插拔 HTTP 驱动与存储后端；
同一监听端口上还提供 Apache Iceberg REST Catalog（S3 Tables），PyIceberg / DuckDB /
Spark / Trino 可以把任意桶当作湖仓目录使用。
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
│                  └─ /iceberg/v1/... → tables::RestApi（S3 Tables：    │
│                       Iceberg REST catalog，namespace/表/view）       │
│ XML 编解码 · S3Error 映射 · Metrics · 访问日志                         │
└─────────────────────────────────┬─────────────────────────────────────┘
                   IStorageBackend（Task<T>，流式）
┌─ L3 · Storage ──────────────────▼─────────────────────────────────────┐
│ BucketRouter：glob 规则 → 后端；".sys" 存凭证与表目录状态             │
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
make release             # build-rel 下的 Release 构建（make debug → build 下的 Debug；make package → deb/rpm/tgz）
make test                # Debug 构建后跑 ctest 快速集（除性能门禁、soak、mint 之外的全部）
make coverage            # build-cov 的 -O0 --coverage 构建 + 单测 + 行覆盖率报告（scripts/coverage.sh）
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
`--clean` 或独立的 `-B build-x` 目录。Sanitizer / 分析构建：`./build.sh --asan` /
`--tsan` / `--ubsan` / `--coverage` / `--fuzz`（libFuzzer，clang）。
`scripts/check-all.sh` 对每个已存在的构建目录跑增量构建 + ctest 矩阵；
`scripts/coverage.sh` 出行覆盖率；`scripts/bench_gate.sh` 与 `scripts/soak.sh`
是性能门禁与长稳跑——见 [testing.md](testing.md)。

ctest 还带 fuzz 语料回放（`fuzz_regression_*`）、监控资产检查、3 秒 bench 门禁与
30 秒 soak（标签 `perf`/`soak`）、S3 Tables 客户端冒烟（`tables_smoke`，需要
`LIGHTS3_TABLES_SMOKE=1` 与 PyIceberg / DuckDB，否则 SKIP），以及 MinIO mint 兼容集
（`s3cmd awscli` 子集；依赖 docker，无 docker 报 SKIP）：

```bash
ctest --test-dir build -LE "perf|mint"      # 快速集
ctest --test-dir build -R mint -V           # 有 docker 的机器上跑 mint
```

## 运行

```bash
export LIGHTS3_SECRET_1=my-secret
# 可选：动态凭证 SK 落盘 AES-256-GCM 加密；启用后缺 key/错 key 会启动失败
export LIGHTS3_MASTER_KEY=$(openssl rand -hex 32)
./build/lights3 --config=config/lights3.yaml
```

运维 CLI `lights3-ctl`（凭证、桶网站配置、表桶、压测）与 `lights3` 完整命令树见 [cli.md](cli.md)。

用任意 S3 客户端访问（示例用 curl 的 SigV4 支持）：

```bash
alias s3curl='curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "AKIDEXAMPLE:$LIGHTS3_SECRET_1"'
s3curl -X PUT http://127.0.0.1:9000/mybucket                      # CreateBucket
s3curl -X PUT --data-binary @file.bin http://127.0.0.1:9000/mybucket/file.bin
s3curl http://127.0.0.1:9000/mybucket?list-type=2                 # ListObjectsV2
s3curl -r 0-99 http://127.0.0.1:9000/mybucket/file.bin            # Range 下载
```

或使用 aws cli：`aws --endpoint-url http://127.0.0.1:9000 s3 ls`。

`lights3 --version`（以及 `lights3-ctl --version`）打印版本号、构建时嵌入的 git
commit、构建类型与编译进来的驱动 / 后端；同一身份也写在启动日志首行，并以
`lights3_build_info` 指标导出。

## S3 Tables（Apache Iceberg REST Catalog）

在 S3 API 之外，lights3 还提供 Iceberg REST Catalog。*表桶*里的 Iceberg 元数据与数据文件
就是普通对象；目录状态（namespace、表指针、提交记录）以 JSON 对象放在 `.sys` 桶，每次
元数据指针切换都是存储后端上的一次条件写（`If-Match` / `If-None-Match: *`）。因此同一套
保证在所有后端上成立，共享一份存储的多个网关也成立；元数据由网关生成并校验，引擎只需
标准的 REST catalog 配置。

打开开关并建一个表桶：

```yaml
tables:
  enabled: true                 # 同一监听端口上的 /iceberg/v1/...
  credential_vending: true      # 可选：给引擎下发按表收窄的 STS 会话
  maintenance:
    scan_interval: 0s           # 0 = 只在请求时做维护（lights3-ctl / REST）
```

```bash
./build/lights3-ctl tables enable lake     # root 凭证：PUT /iceberg/v1/buckets/lake
```

把引擎指过来（SigV4 签名的 REST 调用，signing name 为 `s3`）：

```python
from pyiceberg.catalog import load_catalog
cat = load_catalog("lake", **{
    "uri": "http://127.0.0.1:9000/iceberg", "warehouse": "lake",
    "rest.sigv4-enabled": "true", "rest.signing-name": "s3", "rest.signing-region": "us-east-1",
    "s3.endpoint": "http://127.0.0.1:9000", "s3.path-style-access": "true", "s3.region": "us-east-1",
    "s3.access-key-id": "AKIDEXAMPLE", "s3.secret-access-key": "my-secret",
})
cat.create_namespace("sales")
```

```sql
-- DuckDB（iceberg + httpfs 扩展）
CREATE SECRET s3s (TYPE s3, PROVIDER config, KEY_ID 'AKIDEXAMPLE', SECRET 'my-secret',
                   REGION 'us-east-1', ENDPOINT '127.0.0.1:9000', URL_STYLE 'path', USE_SSL false);
ATTACH 'lake' AS lake (TYPE iceberg, ENDPOINT 'http://127.0.0.1:9000/iceberg',
                       AUTHORIZATION_TYPE 'sigv4', SECRET s3s, SIGV4_REGION 'us-east-1', SIGV4_SERVICE 's3');
SELECT * FROM lake.sales.orders LIMIT 10;
```

目录提供的能力：

- **标准 REST catalog 面**（`/iceberg/v1`，可选 `/_iceberg` 别名）：`/config`、namespace
  全部端点、表的 create / load / commit / register / rename / drop 与 `metadata-location`、
  views、`reportMetrics`、Iceberg JSON 错误信封；format v1/v2 元数据的 requirements 与
  updates 全部在服务端应用
- **安全提交**：单表 CAS + 按 commit id 幂等重放、有据可查的崩溃窗口矩阵与
  `catalog/diagnostics` / `catalog/recovery` 端点、任一网关都能接续的两阶段 rename、
  接受快照前的深校验（manifest-list → manifest → 数据文件，Avro null / deflate）；
  LoadTable 带 `ETag`，支持 `If-None-Match`
- **跟随对象模型的访问控制**：per-credential policy 按 (bucket, `<namespace>/<table>`)
  判定、租户隔离、接受 `s3tables` 签名名、按表前缀收窄的下发凭证
  （`X-Iceberg-Access-Delegation: vended-credentials`）；保留前缀在 S3 面只读，
  lifecycle 跳过表桶
- **以管理作业形式的维护**：`plan`（元数据保留、快照过期、孤儿文件、compaction 候选）/
  `run` / `purge`，带安全窗口与指针复核，由 `lights3-ctl tables …`、REST 端点或可选的
  周期 runner 驱动；`fsck` 对账目录状态与表桶
- **部署选择**：任意后端做对象存储；共享默认后端的网关共享同一份目录；
  `tables.catalog_backing: duostore` 把目录放进 DuoStore meta 引擎
  （RocksDB / SQLite / Redis / TiKV）并以原子事务提交，`lights3 tables export|import`
  在两种后备间迁移

已验证客户端：PyIceberg 0.12 与 DuckDB 1.5（ctest `tables_smoke`）；Spark / Trino 用同一套
REST 配置（模板在设计文档里，本机尚未验证）。设计与实现记录：
[s3-tables-design.md](s3-tables-design.md)；命令：[cli.md](cli.md) §2.6 与 §3.13；
端点清单：[s3-protocol.md](s3-protocol.md) §1。

## 安装、打包、容器化

三条渠道，细节见 [deployment.md](deployment.md)：

```bash
# 1. /usr/local 下的 systemd 服务（先构建）
./build.sh -DLIGHTS3_BUILD_TESTS=OFF
sudo ./scripts/install.sh                    # 可升级：保留配置/密钥，旧二进制留作 *.prev
sudo /usr/local/sbin/lights3ctl status
sudo ./scripts/rollback.sh                   # 换回上一版二进制
sudo ./scripts/uninstall.sh [--purge]

# 2. 安装树 / 包
sudo cmake --install build                   # 或 --prefix / DESTDIR
cmake --build build --target package         # build/packages/lights3_<ver>_<arch>.deb（有 rpmbuild 则出 rpm）
make package                                 # 同上，但从 Release 树出包：build-rel/packages/
sudo apt install ./build/packages/lights3_0.1.0_amd64.deb

# 3. Docker / compose
docker compose up -d                         # localfs demo，:9000（AKIDEXAMPLE / lights3-demo-secret）
docker compose --profile redis up -d         # duostore + redis meta，:9001（另有 tikv / rados profile）
docker compose --profile e2e run --rm e2e    # 开发机上 SKIP 的 redis / tikv / rados e2e 路径
```

各渠道都会创建 `lights3` 系统用户，把配置放在 `/etc/lights3/lights3.yaml`
（deb conffile / rpm `%config(noreplace)`；重复安装不覆盖），首次安装在
`/etc/lights3/lights3.env` 生成随机凭证，并在重启服务前先用
`lights3 --check-config` 校验现有配置。`lights3ctl help` 列出启停、重启、
状态与日志命令。

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
  CloudProxy（自签 SigV4 直连远端 S3，[cloudproxy-design.md](storage/cloudproxy-design.md)）、
  Tiered（冷数据下沉云端的组合后端，[tiered-design.md](storage/tiered-design.md)）、
  DuoStore（元数据/数据分离引擎，meta 可选 RocksDB/Redis/SQLite/TiKV、
  data 可选本地 fs/RADOS，[duostore-design.md](storage/duostore-design.md)）；
  bucket 级 glob 路由
- **S3 API**：ListBuckets、Create/Head/DeleteBucket、Put/Get/Head/DeleteObject
  （含 Range 与条件请求）、CopyObject、DeleteObjects 批量、
  ListObjectsV2（prefix/delimiter/分页）、Multipart Upload
  （create/upload/upload-part-copy/list/complete/abort；UploadPartCopy 支持
  `x-amz-copy-source-range` 与 copy-source 条件头，源与目标可在不同后端）；
  静态网站托管（显式列出的 bucket 匿名 GET/HEAD + index/error 文档、
  RedirectAllRequestsTo/RoutingRules、无尾斜杠 302、按桶匿名限速，
  [static-website.md](static-website.md)）；CORS（`?cors` + OPTIONS 预检 +
  响应头注入）；对象标签（`?tagging` + `x-amz-tagging` +
  `x-amz-tagging-count`）；Lifecycle 最小子集（Expiration.Days +
  AbortIncompleteMultipartUpload，周期执行扫描）；校验和持久化与回显
  （`x-amz-checksum-*` 随对象存储、GET/HEAD `x-amz-checksum-mode: ENABLED`、
  multipart 复合 `-N` 校验和）；`GET ?partNumber` + `x-amz-mp-parts-count`；
  STS AssumeRole 会话凭证（SigV4 `sts` scope，数据面带 token 验证与 TTL）
- **用量 / 配额 / 多租户 / 审计**（[multi-tenancy.md](multi-tenancy.md)）：
  桶级用量计数器（增量 + 周期全量校准，`/-/admin/usage`）；`?quota` 桶配额与
  租户聚合配额（`QuotaExceeded` 403，MPU 分片计入）；租户实体与桶归属
  （凭证 `tenant`/`role`，租户只见自己的桶，分级管理面）；JSON 行审计日志
- **S3 Tables / Iceberg REST Catalog**（[s3-tables-design.md](s3-tables-design.md)）：
  表桶、`.sys` 上的目录状态与条件写提交 + 幂等重放、Avro 深校验、诊断 / 恢复、
  凭证下发、维护作业、views、多网关、可选的 DuoStore-meta 目录后备（见上文专节）

设计上明确不支持（返回 NotImplemented，见 [s3-protocol.md](s3-protocol.md) §1）：
versioning、ACL 细粒度（只认 private）、bucket policy、lifecycle
Transition/按 tag 过滤、SSE-C/KMS、Object Lock、presigned POST。

## 文档

设计文档中文原文在 [docs/](README.md)，英文翻译在 [docs/en/](en/README.md)，
章节编号与中文版一致（源码注释以 `docs/<name>.md §N` 形式引用章节）。

| 文档（[中文](README.md) · [en](en/README.md)） | 内容 |
| --- | --- |
| [architecture](architecture.md) | 总体架构、分层、请求生命周期、代码布局 |
| [config-reload](config-reload.md) | 配置热重载：SIGHUP / admin API / `lights3-ctl reload` |
| [tls](tls.md) | 四驱动 HTTPS、证书热重载、mTLS / cipher / SNI、反代终结样例 |
| [http-adapter](http-adapter.md) | HTTP 插拔层：中立请求/响应模型、流式 body、各驱动要点 |
| [concurrency](concurrency.md) | Task 协程、Executor 抽象、线程池、同步/异步驱动桥接 |
| [storage-backend](storage/storage-backend.md) | `IStorageBackend`、LocalFs/XLocalFs、bucket 路由、新增后端指南 |
| [s3-protocol](s3-protocol.md) | API 范围、SigV4（含 presigned 与时钟偏移）、XML 编解码、错误、mint 门禁 |
| [credential-management](credential-management.md) | AK/SK 管理 API、三来源模型、`.sys` 持久化、at-rest 加密、policy |
| [multi-tenancy](multi-tenancy.md) | 用量统计、桶/租户配额、租户与桶归属、分级管理面、审计日志 |
| [s3-tables-design](s3-tables-design.md) | S3 Tables：建在 `.sys` 上的 Iceberg REST Catalog、CAS 提交协议、深校验、诊断 / 恢复、凭证、维护、DuoStore-meta 后备 |
| [object-read-write-flow](object-read-write-flow.md) | 端到端读写路径、BodyReader 链、staging 提交、fd 快照读 |
| [tiered-storage](storage/tiered-design.md) | 冷数据下沉云端、stub 元数据、透明回读 |
| [cloudproxy-backend](storage/cloudproxy-design.md) | 自签 SigV4 转发远端 S3、流式泵、重试 |
| [duostore-backend](storage/duostore-design.md) | 元数据/数据分离引擎：RocksDB meta、chunk/pack、GC |
| [duostore-redis-meta](storage/duostore-meta-redis-design.md) | Redis IMetaStore：hiredis + Lua 守卫提交 |
| [duostore-sqlite-meta](storage/duostore-meta-sqlite-design.md) | SQLite IMetaStore：内嵌 amalgamation、WAL、读连接池 |
| [duostore-rados-data](storage/duostore-data-rados-design.md) | RADOS IDataStore：librados，chunk → rados 对象 |
| [duostore-tikv-meta](storage/duostore-meta-tikv-design.md) | TiKV IMetaStore：client-c + 2PC 侧车 |
| [performance-baseline](performance-baseline.md) | 驱动 × TLS 压测矩阵，数据面优化前后对照 |
| [deployment](deployment.md) | 版本标识、`cmake --install`、deb/rpm 包、Dockerfile + compose、回滚 / 卸载 |
| [cli](cli.md) | `lights3` / `lights3-ctl` 命令参考：启动、duostore dump/load/backup/restore、`tables export/import`、cred/website/bench/quota/tenant/usage/tables |
