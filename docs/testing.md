# 测试体系：矩阵、e2e 覆盖、fuzz、故障注入、压测/长稳、覆盖率（roadmap §6.1）

单元测试与 e2e 的基础形态见 [s3-protocol.md §8](s3-protocol.md)；本篇是 roadmap
§6.1 补齐的八项：ctest 清单与标签、website / s3adm / 故障注入的 e2e 段、fuzz
harness、故障注入门面、性能门禁与 soak、mint 挂 ctest、ubsan/coverage 构建、
一键矩阵脚本。

## 1. ctest 清单与标签

| 测试 | 内容 | 标签 |
| --- | --- | --- |
| `unit_tests` | 全部单元用例（含 `test_fault.cc`） | — |
| `e2e_<driver>` / `e2e_<backend>` | 同一套 `run_e2e.sh` 按驱动 × 后端参数化（13 变体） | — |
| `fuzz_regression_<target>` | 6 个 harness 各自回放语料（§3） | `fuzz` |
| `monitoring_assets` | 监控资产对账（[monitoring.md §5](monitoring.md)） | — |
| `bench_gate` | 3 秒吞吐/延迟门禁（§5） | `perf` |
| `soak_smoke` | 30 秒 soak：RSS / fd / 泄漏断言（§5） | `perf` `soak` |
| `mint` | MinIO mint 的 s3cmd + awscli 子集；无 docker 显式 SKIP（§6） | `mint` |

负载敏感或需要外部依赖的项按标签排除：`ctest -LE "perf|mint"`。

## 2. e2e 新增段（`tests/e2e/run_e2e.sh`）

- **静态网站**（此前零覆盖，匿名可读是安全敏感面）：配置里的静态条目
  `e2esite` + `?website` API 动态条目 `dynsite`。匿名 GET/HEAD 对象、桶根与目录
  key 的 index 改写、`/prefix` 无斜杠 302、error 文档 404、
  `x-amz-website-redirect-location` 301、匿名 listing / `?uploads` / 写 / 删 /
  非网站桶一律拒绝、静态条目 API 不可改（405）、非 root 不能 Put 配置、删除配置后
  匿名面立即关闭、`lights3_website_events_total` 计数。
- **s3adm 交叉验证**：curl 用 libcurl 的 SigV4，s3adm 用自实现签名，两套客户端
  打同一服务端。`cred create/list/get --show-secret/delete`（s3adm 铸的凭证 curl
  能签、吊销后 curl 403）、`website set/get/delete`（curl 读回 s3adm 写的配置）、
  `bench put/get` 零错误、`fsck` 对 bench 对象零 mismatch；原有 `usage/quota/
  tenant/reload` 保留。
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
| `http_parse` | `drivers/common.h`：`parse_target` / `parse_content_length` / `parse_chunk_size` / `parse_body_framing`（首字节选路由） | `corpus/http_parse/` |
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
clang 21 + ASan 下核心库、六个 harness、unit_tests 全部编过，unit_tests 484 项通过。
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

`fault::kPoints` 是唯一清单；`test_fault.cc` 会 grep 源码确认每个点都接了线（表里
有、代码里没有的点会让单测失败）。单测覆盖 localfs 三点、duostore 两点，redis 点在
`test_duostore_redis.cc`（有实例才跑），rados/tikv 无本机集群仅编译验证。libfiu
只在 client-c 的嵌套子模块里，不作全仓依赖。

## 5. 性能门禁与 soak

- `scripts/bench_gate.sh <lights3> <s3adm> [--duration N] [--min-put-ops N]
  [--min-get-ops N] [--max-p99-ms N]`：memory 后端网关 + `s3adm bench put/get`，
  解析 `--output=json` 的汇总对象断言吞吐下限（默认 300 ops/s）与 p99 上限（默认 500 ms）；环境变量
  `LIGHTS3_BENCH_*` 同名覆盖。ctest `bench_gate` 用 3 秒。
- `scripts/soak.sh <lights3> <s3adm> [--seconds N] [--backend localfs|duostore|memory]
  [--max-rss-growth PCT] [--max-fd-growth N]`：轮转 put/get/stat/list/删池 五种
  轮次，每轮采样 RSS、fd 数、`lights3_duostore_gcq_depth`、`lights3_multipart_active`；
  结束断言 RSS 相对暖机后增长 < 25%、fd ≤ 暖机 + 16、multipart 无残留、duostore
  GC 队列归零、日志无 ERROR。ctest `soak_smoke` 30 秒；数小时 soak：
  `scripts/soak.sh build/lights3 build/s3adm --seconds 7200 --backend duostore`。

门禁上线即抓到一个真实问题：s3adm 客户端未开 TCP_NODELAY，小 PUT 因 Nagle +
延迟 ACK 每次卡约 40 ms（三个驱动一致、256K 以上正常），已修
（`s3adm_common.cc`），16K PUT 从 98 ops/s 到约 2 万 ops/s。

## 6. mint

`run_mint.sh` 挂为 ctest `mint`（`s3cmd awscli` 子集，`SKIP_RETURN_CODE 77`，无
docker 时显示 Not Run 而非通过）。跑完从 `log.json` 打印每套件 PASS/FAIL/NA 计数
作为基线记录。本机 docker daemon 不可达，**基线尚未记录**：在有权限的机器上
`ctest -R mint -V` 一次，把汇总粘到本节即可。

## 7. ubsan / coverage

- `./build.sh --ubsan`（build-ubsan，`-fsanitize=undefined`）；`check-all.sh` 以
  `UBSAN_OPTIONS=halt_on_error=1` 运行使发现即失败。
- `./build.sh --coverage`（build-cov，`-O0 --coverage`）；`scripts/coverage.sh
  [--e2e] [--no-build] [--no-test]` 构建、跑测、报告：有 gcovr 出 HTML，有 lcov 出
  HTML，都没有则 `scripts/coverage_aggregate.py` 解析 `gcov --json-format` 输出、按
  (文件, 行号) 求并集给出 `src/` 行覆盖率（写入 `build-cov/coverage/summary.txt`；
  gcov 的文本汇总对模板实例化重复计行，不可直接相加）。**已知限制**：GCC 的 gcov
  基本不给协程体插桩（只有 ramp 函数），协程密集的文件分母极小
  （如 `xlocalfs_backend.cc` 只有 28 行被计入），单测跑完的口径是 88% / 13.7k
  行；gcovr/lcov 同样受此限制。

## 8. 一键矩阵

`scripts/check-all.sh [--only build,build-asan,...] [--configure] [--with-perf]
[--with-soak] [-j N]`：对存在的构建目录（build / asan / tsan / ubsan / cov /
sqlite / redis / rados / tikv / seastar / fuzz）逐个增量构建 + `ctest -LE
"mint|perf|soak"`（按旗标放开），sanitizer 目录带 `*SAN_OPTIONS` 使发现即失败，
末尾打印汇总表；`--configure` 用 `build.sh` 创建缺失目录。
