# 测试体系：矩阵、e2e 覆盖、fuzz、故障注入、压测/长稳、覆盖率（roadmap §6.1）

单元测试与 e2e 的基础形态见 [s3-protocol.md §8](../architecture/s3-protocol.md)；本篇是 roadmap
§6.1 补齐的八项：ctest 清单与标签、website / lights3-ctl / 故障注入的 e2e 段、fuzz
harness、故障注入门面、性能门禁与 soak、mint 挂 ctest、ubsan/coverage 构建、
一键矩阵脚本。

## 1. ctest 清单与标签

| 测试 | 内容 | 标签 |
| --- | --- | --- |
| `unit_tests` | 全部单元用例（含 `test_fault.cc`）；`LIGHTS3_TEST_FILTER=子串1,子串2` 只跑名字含任一子串的用例（sanitizer 构建在某个文件上中止时仍能覆盖其它文件） | — |
| `e2e_<driver>` / `e2e_<backend>` | 同一套 `run_e2e.sh` 按驱动 × 后端参数化（16 变体），含 S3 Tables 段（配置 `tables.enabled: true`） | — |
| `fuzz_regression_<target>` | 6 个 harness 各自回放语料（§3） | `fuzz` |
| `monitoring_assets` | 监控资产对账（[monitoring.md §5](../usage/monitoring.md)） | — |
| `bench_gate` | 3 秒吞吐/延迟门禁（§5） | `perf` |
| `soak_smoke` | 30 秒 soak：RSS / fd / 泄漏断言（§5） | `perf` `soak` |
| `mint` | MinIO mint 的 s3cmd + awscli 子集；无 docker 显式 SKIP（§6） | `mint` |
| `install_tree` | `cmake --install` 进临时 prefix：布局、unit 搬迁、保留已有配置、`--version` 格式、维护脚本 `sh -n`（[deployment.md §2](../usage/deployment.md)） | — |
| `tables_smoke` | S3 Tables 客户端冒烟：`run_tables_smoke.sh` 起 memory 网关，跑 `scripts/tables/pyiceberg_smoke.py` 与 `duckdb_smoke.py`；`LIGHTS3_TABLES_SMOKE=1` 才跑，否则 SKIP（§6） | `tables-smoke` |

负载敏感或需要外部依赖的项按标签排除：`ctest -LE "perf|mint"`（`tables-smoke` 默认
自行 SKIP；`check-all.sh --with-tables-smoke` 打开）。双网关的 tables 用例
（`tables_multi_gateway_suite.h`）在 `unit_tests` 里：memory 恒跑，redis / tikv 变体随
`duostore_redis_*` / `duostore_tikv_*` 的外部依赖探测 SKIP。

`e2e_duostore_redis` / `_tikv` / `_rados` 各自探测外部依赖，缺则显式 SKIP：
redis 找 `redis-server` 自起私有实例，或 `LIGHTS3_TEST_REDIS_URI=redis://host:port`
指向外部实例（每次运行唯一 `redis_prefix`）；tikv 看 `LIGHTS3_TEST_PD_ADDR`；
rados 看 `LIGHTS3_TEST_RADOS_CONF` + `_POOL`（`LIGHTS3_TEST_RADOS_CLIENT` 选客户端名，默认 `client.admin`）。`docker compose --profile e2e run --rm e2e`
把三套依赖拉起来一次跑完（[deployment.md §4.3](../usage/deployment.md)）。

## 2. e2e 新增段（`tests/e2e/run_e2e.sh`）

- **静态网站**（此前零覆盖，匿名可读是安全敏感面）：配置里的静态条目
  `e2esite` + `?website` API 动态条目 `dynsite`。匿名 GET/HEAD 对象、桶根与目录
  key 的 index 改写、`/prefix` 无斜杠 302、error 文档 404、
  `x-amz-website-redirect-location` 301、匿名 listing / `?uploads` / 写 / 删 /
  非网站桶一律拒绝、静态条目 API 不可改（405）、非 root 不能 Put 配置、删除配置后
  匿名面立即关闭、`lights3_website_events_total` 计数。
- **S3 Tables 步骤 ①**（[s3-tables-design.md §13](../architecture/s3-tables-design.md)）：
  `/iceberg/v1/config` → 启用表桶（非 root 403）→ namespace / 表 → 保留前缀下的
  metadata.json 可读不可写 → 手工 CommitTable（add-snapshot 指向预放的 manifest-list）
  → 同 commit-id 重放幂等 → 陈旧 requirement 409 → 缺 manifest 409 → rename → 非空
  表桶 DeleteBucket 409 → `purgeRequested=maybe` 400 → drop → 桶名 `iceberg` 400 →
  `lights3_tables_commits_total` 计数；步骤 ②：`s3tables` 签名名在目录面通过、S3 面 400，
  `X-Iceberg-Access-Delegation` 下发的会话凭证在表前缀内 PUT 200 / 前缀外 403 / 读 metadata
  200 / AssumeRole 403，表桶上的 lifecycle 规则被接受但 WARN。单测：`test_tables_iceberg.cc`（纯函数）、
  `test_tables_catalog.cc`（提交协议、幂等重放、`tables.commit.after_stage|after_cas`
  故障点的崩溃窗口、rename）、`test_tables_rest.cc`（端点、错误模型、policy 与列表过滤、租户门、`s3tables` 签名、
  凭证下发、lifecycle 跳过、守卫、`/config.endpoints` 与路由表一致）、`test_credentials.cc`
  的 `policy_narrowing_for_vended_sessions`。步骤 ③：
  e2e 段改用 PyIceberg 写的 manifest 固件（`tests/fixtures/tables/`），深校验通过报
  `lights3.snapshot-validation: deep`、缺数据文件 409、坏 Avro 409、`If-None-Match` 304、
  `catalog/diagnostics` 报 `Committed` 且无未引用文件、`catalog/recovery` 无事可做、只读凭证
  diagnostics 200 / recovery 403。单测：`test_tables_avro.cc`（手工 OCF 每种类型、截断 / sync /
  深度 / 未知 codec、PyIceberg 固件逐字段对照）、`test_tables_catalog.cc` 追加深校验六种 409
  与 codec 两态、诊断五态与 `recover` 指针不动、rename 五个故障点由另一实例恢复 + Prepared
  超时回滚、`test_tables_rest.cc` 的 diagnostics / recovery / 304 / `skipped-codec`、
  `test_admin_jobs.cc` 的 fsck 扩展合并与目录对账三类 finding。步骤 ⑥：
  e2e 段加 `/_iceberg` 别名（/config 报 `lights3.catalog-compat-prefix`、桶名 `_iceberg` 400）、views
  建 / 列 / HEAD / 同名表 409 / replace 版本 +1 / 陈旧 uuid 409 / rename / drop、`reportMetrics` 204、plan 的
  `compaction-candidates`。单测：`test_tables_optional.cc`（`catalog_store_suite.h` 对 object 与
  duostore-rocksdb 后备、views 生命周期两种后备、view 元数据模型、compaction 候选、`catalog_backing` 配置、
  duostore 后备的原子提交与 20 并发单胜者 + export/import）、`test_duostore_sqlite|redis|tikv.cc` 的
  `*_tables_catalog_store`、`meta_store_suite.h` 的 `case_kv_facade`（四引擎）、
  `test_tables_rest.cc` 的 `tables_rest_views_compat_prefix_and_metrics`（审计文件里的 `tables.metrics`）。步骤 ④：
  e2e 段加 `maintenance/config` 默认与表级设置、plan 作业绑定 version token 且安全窗口内无候选、
  run 作业不删文件、只读凭证 plan 403、管理面 `/-/admin/tables/...` 202、`lights3-ctl tables
  status|list|plan|run|diagnose|recover|purge`（purge 无 `--yes` 退出 2，purge 后 metadata 404、
  目录 404）。单测：`test_tables_maintenance.cc`（保留集与安全窗口、快照过期与 tag / ref 规则 /
  `remove-snapshots` 提交、孤儿 fail-closed、StalePlan / `delete_enabled` / 删前复核、purge、
  runner 经作业框架跳过忙表 + 墓碑 TTL + 后台 tick）、`test_tables_rest.cc` 的
  `tables_rest_maintenance_endpoints`（端点、权限、管理面、`purgeRequested=true` 作业）。
- **lights3-ctl 交叉验证**：curl 用 libcurl 的 SigV4，lights3-ctl 用自实现签名，两套客户端
  打同一服务端。`cred create/list/get --show-secret/delete`（lights3-ctl 铸的凭证 curl
  能签、吊销后 curl 403）、`website set/get/delete`（curl 读回 lights3-ctl 写的配置）、
  `bench put/get` 零错误、`fsck` 对 bench 对象零 mismatch；原有 `usage/quota/
  tenant/reload` 保留。
- **多网关 multipart**（`duostore-redis` 变体，
  [archive/multi-gateway-multipart-design.md §4 ②](../archive/multi-gateway-multipart-design.md)）：
  同一 redis meta + 同一数据根起两个网关，create 在 A、5 个分片 B/A 交替上传、
  两侧 ListParts / ListMultipartUploads 一致、B complete、A HEAD/GET；合成 ETag 由
  脚本从各分片 ETag 独立算出再比对。单测侧同一场景矩阵在
  `tests/unit/multi_gateway_suite.h`（redis / tikv 各实例化一次，缺实例 SKIP）；
  compose `multi` profile 的容器版见 [deployment.md §4.2](../usage/deployment.md)。
- **故障注入**（localfs / xlocalfs / tiered 变体）：以
  `LIGHTS3_FAULTS=localfs.write:1:EIO,xlocalfs.write:1:EIO` 再起一个实例：首个 PUT
  500、对象不存在、重试成功、`lights3_backend_errors_total{op="put_object"}` 与
  `lights3_responses_by_status_total{status="500"}` 各计 1。

这些段落各抓到一个真实缺陷：httplib 驱动把"空 body 的 ≥400 响应"（HEAD 404、
流式 error 文档）误改写成 405——已修（错误处理器看到 L2 的
`x-amz-request-id`/`Server` 头即放行）。

## 3. fuzz

`tests/fuzz/fuzz_<target>.cc` 各一个 `LLVMFuzzerTestOneInput`，全部是无认证即可
触达的解析器：

| target | 入口 | 语料 |
| --- | --- | --- |
| `xml` | `s3::xml_parse`（Complete/DeleteObjects/?website 请求体） | `corpus/xml/` |
| `uri` | `percent_decode` / `percent_decode_query` / `aws_uri_encode` 往返不变量 | `corpus/uri/` |
| `http_parse` | `http/drivers/common.h`：`parse_target` / `parse_content_length` / `parse_chunk_size` / `parse_body_framing`（首字节选路由） | `corpus/http_parse/` |
| `sigv4` | `SigV4Authenticator::verify`：Authorization 头与 presigned 查询（"头块\n\n查询"） | `corpus/sigv4/` |
| `aws_chunked` | 签名正确的请求头 + fuzz 字节作 body，经 `verify()` 装上的 aws-chunked 解帧读取器读尽（unsigned-trailer / signed / signed+trailer 三种） | `corpus/aws_chunked/` |
| `duostore_codec` | `codec::decode_*`（共享 meta 引擎里别的网关写的记录） | `corpus/duostore_codec/` |

两种构建：默认（任何编译器）链接 `fuzz_driver.cc`，二进制回放语料目录 + 单字节
扫描，作为 ctest `fuzz_regression_*` 崩溃回归门槛；`./build.sh --fuzz`（切到
clang，`-DLIGHTS3_FUZZ_LIBFUZZER=ON`，默认叠 ASan）链接 libFuzzer 真正变异：

```bash
./build.sh --fuzz -B build-fuzz
mkdir -p build-fuzz/corpus-xml
build-fuzz/fuzz_xml build-fuzz/corpus-xml tests/fuzz/corpus/xml -max_total_time=600
```

libFuzzer 把新发现的有趣输入写进**第一个**语料目录：把工作目录放在 build-fuzz
下，`tests/fuzz/corpus/<target>/` 只放手写种子；发现的崩溃输入挑出来放回种子目录
即成为永久回归。libFuzzer 模式要求整棵树能
用 clang 编译——为此把 `YamlNode` 的特殊成员移到类外（递归的 `pair<string,
YamlNode>` 成员在类内 default 时 clang 会以不完整类型实例化）并修了一处窄化；
clang 21 + ASan 下核心库、六个 harness、unit_tests 全部编过；GCC 15 + ASan 的 `build-asan` 全量 unit_tests 521 项通过（2026-09-06，`Task` 恢复蹦床落地之后——此前 -O0 下同步完成的读链会在 `test_http_drivers` 栈溢出中止整次运行，[concurrency.md §2](../architecture/concurrency.md)）。
本机各 harness 空跑 5–10 秒：xml 15 万次、uri 340 万次、http_parse 247 万次、
sigv4 42 万次、aws_chunked 1.4 万次、duostore_codec 19 万次，均无崩溃。

## 4. 故障注入

`core/fault.h`：命名注入点 + 环境变量/程序化武装，同一二进制生产可用（未武装时
热路径一次 relaxed 原子读）。语法 `point[:count][:errno]` 逗号分隔，count 默认 1、
0 = 持续到 reset，errno 符号名或数字（默认 EIO）：

```bash
LIGHTS3_FAULTS="localfs.write:1:EIO,duostore.pack.fdatasync:0:ENOSPC" lights3 --config ...
```

| 注入点 | 位置 | 表现 |
| --- | --- | --- |
| `localfs.write` | staging tmp 的 `::write`（put / upload_part / complete 拼接） | InternalError，无残留对象与 tmp |
| `localfs.rename` | 对象 / 缓存数据的提交 rename | InternalError（附 errno 文本） |
| `localfs.fsync` | staged 文件的 fdatasync（`fsync_file` 与 `fsync_path`） | InternalError；顺带修正：`fsync_path` 原先静默吞掉真实 fdatasync 错误，现按 200 的持久化承诺抛错 |
| `xlocalfs.write` | io_uring 写管线（`drain_to_tmp`） | InternalError |
| `duostore.pack.pwrite` / `duostore.pack.fdatasync` | pack 记录追加 / 持久化 | InternalError，先前数据完好 |
| `redis.command` | hiredis 命令层：模拟连接级失败 | 读命令换连接重试一次（`reconnects` 计数），写命令 InternalError |
| `rados.submit` | `rados_aio_*` 提交：返回 `-errno` | 走既有 rados 错误路径 |
| `tables.commit.after_stage` / `tables.commit.after_cas` | 表目录提交（[s3-tables-design.md §5.4](../architecture/s3-tables-design.md)）：STAGED 记录写入之后、指针 CAS 之前 / 指针 CAS 之后、记录 finalize 之前 | 崩溃窗口由 `catalog/diagnostics` 报出并可恢复（`test_tables_catalog.cc`） |
| `tables.rename.after_prepare` / `after_fence` / `after_destination` / `after_tombstone` / `before_cleanup` | 表 rename（设计 §5.6）五步各自之后、下一步之前：intent 已写 / 源已 fence / 目标已写 / 源已墓碑 / intent 即将删除 | 另一实例可接续恢复，Prepared 超时回滚（`test_tables_catalog.cc`） |

`fault::kPoints` 是唯一清单；`test_fault.cc` 会 grep 源码确认每个点都接了线（表里
有、代码里没有的点会让单测失败）。单测覆盖 localfs 三点、duostore 两点，redis 点在
`test_duostore_redis.cc`（有实例才跑），rados/tikv 无本机集群仅编译验证。libfiu
只在 client-c 的嵌套子模块里，不作全仓依赖。

## 5. 性能门禁与 soak

- `scripts/bench_matrix.sh <lights3> <lights3-ctl> [--drivers a,b] [--tls on|off|both]
  [--duration N] [--concurrency N] [--size SZ] [--objects N] [--modes put,get]
  [--io-threads N] [--json FILE] [--label TEXT] [--keep-log]`：性能基线矩阵（roadmap §4.3）——每个（驱动 × TLS）格起一个
  localfs 网关跑 `lights3-ctl bench put/get`，输出 Markdown 表 + 每格一行 JSON；
  驱动清单默认取 `lights3 --version` 的 `drivers:` 行。结果入库
  [performance-baseline.md](performance-baseline.md)。

- `scripts/bench_gate.sh <lights3> <lights3-ctl> [--duration N] [--concurrency N] [--size SZ]
  [--min-put-ops N] [--min-get-ops N] [--max-p99-ms N] [--keep-log]`：memory 后端网关 + `lights3-ctl bench put/get`，
  解析 `--output=json` 的汇总对象断言吞吐下限（默认 300 ops/s）与 p99 上限（默认 500 ms）；环境变量
  `LIGHTS3_BENCH_DURATION` / `_CONCURRENCY` / `_SIZE` / `_MIN_PUT_OPS` / `_MIN_GET_OPS` / `_MAX_P99_MS` 同名覆盖。ctest `bench_gate` 用 3 秒。
- `scripts/soak.sh <lights3> <lights3-ctl> [--seconds N] [--backend localfs|duostore|memory]
  [--concurrency N] [--max-rss-growth PCT] [--max-fd-growth N] [--keep-log]`（环境变量
  `LIGHTS3_SOAK_SECONDS` / `_BACKEND` / `_CONCURRENCY` / `_MAX_RSS_GROWTH` / `_MAX_FD_GROWTH` 同名覆盖）：轮转 put/get/stat/list/删池 五种
  轮次，每轮采样 RSS、fd 数、`lights3_duostore_gcq_depth`、`lights3_multipart_active`；
  结束断言 RSS 相对暖机后增长 < 25%、fd ≤ 暖机 + 16、multipart 无残留、duostore
  GC 队列归零、日志无 ERROR。ctest `soak_smoke` 30 秒；数小时 soak：
  `scripts/soak.sh build/lights3 build/lights3-ctl --seconds 7200 --backend duostore`。

门禁上线即抓到一个真实问题：lights3-ctl 客户端未开 TCP_NODELAY，小 PUT 因 Nagle +
延迟 ACK 每次卡约 40 ms（三个驱动一致、256K 以上正常），已修
（`lights3_ctl_common.cc`），16K PUT 从 98 ops/s 到约 2 万 ops/s。

## 6. mint

`run_mint.sh` 挂为 ctest `mint`（`s3cmd awscli` 子集，`SKIP_RETURN_CODE 77`，无
docker 时显示 Not Run 而非通过）。跑完从 `log.json` 打印每套件 PASS/FAIL/NA 计数
作为基线记录。本机 docker daemon 不可达，**基线尚未记录**：在有权限的机器上
`ctest -R mint -V` 一次，把汇总粘到本节即可。

**S3 Tables 客户端冒烟**（ctest `tables_smoke`，[s3-tables-design.md §13](../architecture/s3-tables-design.md)）：
2026-09-12 本机通过——PyIceberg 0.12.0（pyarrow 22.0，boto3 走 SigV4，`rest.signing-name=s3`）
16/16：启用表桶、建表、append ×2、重载 scan、陈旧句柄提交由 PyIceberg 自动刷新重试、同
commit-id 幂等重放、维护 plan/run、diagnostics 全 Committed、purge；DuckDB 1.5.5（iceberg
扩展，`ATTACH … (TYPE iceberg, AUTHORIZATION_TYPE 'sigv4', SECRET …, SIGV4_REGION …,
SIGV4_SERVICE 's3')`）5/5：attach、列 namespace、scan。依赖装在 `pip --target` 目录、
`PYTHONPATH` 指过去即可：`LIGHTS3_TABLES_SMOKE=1 PYTHONPATH=… ctest -R tables_smoke`。

## 7. ubsan / coverage

- `./build.sh --ubsan`（build-ubsan，`-fsanitize=undefined`）；`check-all.sh` 以
  `UBSAN_OPTIONS=halt_on_error=1` 运行使发现即失败。
- `./build.sh --coverage`（build-cov，`-O0 --coverage`）；`scripts/coverage.sh
  [--e2e] [--no-build] [--no-test] [-j N] [-B build-cov]`（即 `make coverage`，§9）构建、跑测、报告：有 gcovr 出 HTML，有 lcov 出
  HTML，都没有则 `scripts/coverage_aggregate.py` 解析 `gcov --json-format` 输出、按
  (文件, 行号) 求并集给出 `src/` 行覆盖率（写入 `build-cov/coverage/summary.txt`；
  gcov 的文本汇总对模板实例化重复计行，不可直接相加）。**已知限制**：GCC 的 gcov
  基本不给协程体插桩（只有 ramp 函数），协程密集的文件分母极小
  （如 `xlocalfs_backend.cc` 只有 28 行被计入），单测跑完的口径是 88% / 13.7k
  行；gcovr/lcov 同样受此限制。

## 8. 一键矩阵

`scripts/check-all.sh [--only build,build-asan,...] [--configure] [--with-perf]
[--with-soak] [--with-tables-smoke] [-j N] [--ctest-args "..."]`：对存在的构建目录（build / asan / tsan / ubsan / cov /
sqlite / redis / rados / tikv / seastar / fuzz）逐个增量构建 + `ctest -LE
"mint|perf|soak"`（按旗标放开；`--ctest-args` 追加 ctest 参数），sanitizer 目录带 `*SAN_OPTIONS` 使发现即失败，
末尾打印汇总表；`--configure` 用 `build.sh` 创建缺失目录。

## 9. Makefile：构建与代码格式

根目录 `Makefile` 是 `build.sh`、ctest、`scripts/coverage.sh` 与 CPack 的薄封装：
`make release`（Release，`build-rel/`）、`make debug`（Debug，`build/`）、
`make test`（先 debug，再在 `build/` 跑 §1 的快速集 `ctest -LE "perf|soak|mint"`，与
`check-all.sh` 同一过滤；`CTEST_ARGS="-R tables"` / `CTEST_ARGS="-L perf"` 追加筛选；
ctest 串行跑——e2e 段绑定固定端口）、`make coverage`（`scripts/coverage.sh`：`build-cov/`
的 `-O0 --coverage` 构建 + 单测与 fuzz 回放 + §7 的行覆盖率报告；
`COVERAGE_ARGS="--e2e"` / `"--no-build"` / `"--no-test"` 透传）、`make package`（先
release，再 CPack，产物在 `build-rel/packages/`，生成器按机器上有的 dpkg-deb / rpmbuild
选，否则 TGZ）、`make clean`（只删 `build-rel/`、`build/`、`build-cov/` 三个目录，其余
`build-*` 变体不动）。`JOBS=` 定并发（默认核数的一半），`BUILD_ARGS="--redis --sqlite"`
透传 build.sh 旗标，`RELEASE_DIR=` / `DEBUG_DIR=` / `COVERAGE_DIR=` 改目录。`make help`
列出全部目标。

### 9.1 代码格式

`make format` 先跑 `scripts/check_comments.py --fix` 把行尾 `//` 注释挪到所在语句
的上一行（以 `{` 结尾的行挪进块内第一行；列表元素、标签、预处理行放在自身上一行；
`}  // namespace x`、`#endif  // X`、`// NOLINT`、`// clang-format off` 这几类收尾
注释保留），再用仓库根目录的 `.clang-format`（Google 风格 + 4 空格缩进 + 120 列，
其余偏离项在文件内逐条注明）就地重排 `src/`、`tests/` 下全部受 git 跟踪的 `.h` /
`.cc`；`make format-check` 列出行尾注释与会被 clang-format 改动的文件并以 1 退出，
供提交前与 CI 用。
`CLANG_FORMAT=clang-format-23 make format` 可指定二进制；Google 预设在 LLVM 大版本
之间有细微漂移，团队应钉住一个主版本。全仓已于 2026-09-09 按此格式化过一次，
之后的 PR 不应再夹带格式噪音。
