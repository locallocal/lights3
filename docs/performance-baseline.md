# 性能基线：驱动 × TLS × put/get 矩阵

roadmap §4.3 末项"性能基线缺失"的兑现：仓内首份存档的 benchmark 数据，同时是
§4.3 数据面优化（[http-adapter.md §2.4](http-adapter.md)）的前后对照。数字只对
本机有意义，**用于相对比较与回归对照，不是产品指标**；换机器请用 §3 的命令
重新生成并替换本文表格。

## 1. 环境与方法

| 项 | 值 |
| --- | --- |
| 机器 | Intel i9-14900KF（32 线程）、30 GiB、Linux 7.0.0-31、g++ 15.2 |
| 数据目录 | `/tmp`（tmpfs）——localfs 后端的对象在内存文件系统上，测的是 HTTP 层 + 拷贝路径，**不含磁盘 IO** |
| 客户端 | `s3adm bench`（httplib 同步客户端，每 worker 一条 keep-alive 连接），与网关同机 loopback |
| 网关 | localfs 后端；`http.io_threads: 8`（beast/httplib/seastar），`runtime.io_threads: 16`；TLS 为 openssl 自签 P-256 证书，客户端 `--insecure` |
| 大对象档 | 4 MiB × 8 workers × 8 s × 32 键 |
| 小对象档 | 16 KiB × 16 workers × 8 s × 256 键 |
| "前" | main 21968cf（含 #89），`Release`、`-DLIGHTS3_DUOSTORE=OFF -DLIGHTS3_CLOUDPROXY=OFF`（只裁掉无关后端，驱动与 localfs 路径不变）；seastar 用 `build-seastar`（RelWithDebInfo，改动前的树） |
| "后" | 分支 `feat/dataplane-perf`，同一套选项；seastar 同一 `build-seastar` 增量重建 |
| 脚本 | `scripts/bench_matrix.sh`（[testing.md §5](testing.md)），每格起一个新网关，顺序执行，机器空闲 |

延迟列来自 `s3adm bench` 的直方图分位数（桶边界离散，p50 出现 6.15、12.29 这类
"整数"是桶宽所致）。单次 8 s 的抖动约 ±5%：**±5% 以内的差异视为噪声**。

## 2. 结果

### 2.1 4 MiB 对象（8 workers）

| 驱动 | TLS | 模式 | ops/s 前 | ops/s 后 | Δ | MiB/s 后 | p50 ms 前→后 | p99 ms 前→后 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| builtin | off | put | 1074 | 1065 | **-1%** | 4259 | 7.10→7.20 | 16.12→16.14 |
| builtin | off | get | 4219 | 5709 | **+35%** | 22837 | 1.79→1.52 | 4.03→2.05 |
| builtin | on | put | 930 | 927 | **-0%** | 3707 | 7.57→7.62 | 16.18→16.19 |
| builtin | on | get | 2425 | 3076 | **+27%** | 12306 | 3.24→2.80 | 7.90→4.08 |
| beast | off | put | 91 | 914 | **+907%** | 3656 | 98.30→10.67 | 130.35→16.27 |
| beast | off | get | 3399 | 4568 | **+34%** | 18270 | 3.07→1.58 | 4.08→3.84 |
| beast | on | put | 207 | 727 | **+252%** | 2910 | 49.15→12.29 | 65.20→16.31 |
| beast | on | get | 1304 | 1511 | **+16%** | 6045 | 6.15→6.15 | 8.16→8.15 |
| httplib | off | put | 1084 | 1082 | **-0%** | 4328 | 7.26→7.37 | 16.15→16.16 |
| httplib | off | get | 4000 | 4561 | **+14%** | 18243 | 1.99→1.71 | 4.05→4.03 |
| httplib | on | put | 1068 | 879 | **-18%** | 3516 | 7.42→10.01 | 16.17→16.33 |
| httplib | on | get | 2245 | 3005 | **+34%** | 12020 | 3.69→2.71 | 8.09→4.40 |
| seastar | off | put | 1109 | 1031 | **-7%** | 4124 | 6.19→6.77 | 12.51→16.03 |
| seastar | off | get | 3995 | 6070 | **+52%** | 24279 | 1.83→1.55 | 4.04→3.11 |
| seastar | on | put | 934 | 880 | **-6%** | 3518 | 11.47→12.24 | 16.29→16.31 |
| seastar | on | get | 2234 | 3005 | **+34%** | 12019 | 3.09→3.13 | 5.21→7.50 |

### 2.2 16 KiB 对象（16 workers）

| 驱动 | TLS | 模式 | ops/s 前 | ops/s 后 | Δ | MiB/s 后 | p50 ms 前→后 | p99 ms 前→后 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| builtin | off | put | 80477 | 80364 | **-0%** | 1256 | 0.20→0.20 | 0.47→0.47 |
| builtin | off | get | 148727 | 151198 | **+2%** | 2362 | 0.10→0.10 | 0.25→0.25 |
| builtin | on | put | 70889 | 70845 | **-0%** | 1107 | 0.21→0.21 | 0.50→0.50 |
| builtin | on | get | 120613 | 124612 | **+3%** | 1947 | 0.14→0.13 | 0.25→0.25 |
| beast | off | put | 21845 | 77474 | **+255%** | 1211 | 0.77→0.20 | 1.02→0.48 |
| beast | off | get | 110778 | 127500 | **+15%** | 1992 | 0.18→0.11 | 0.25→0.25 |
| beast | on | put | 35439 | 65572 | **+85%** | 1025 | 0.41→0.22 | 0.99→0.60 |
| beast | on | get | 107445 | 112165 | **+4%** | 1753 | 0.18→0.17 | 0.25→0.26 |
| httplib | off | put | 42115 | 41920 | **-0%** | 655 | 0.19→0.19 | 0.47→0.47 |
| httplib | off | get | 91136 | 93214 | **+2%** | 1456 | 0.09→0.09 | 0.13→0.13 |
| httplib | on | put | 33893 | 33409 | **-1%** | 522 | 0.23→0.23 | 0.51→0.51 |
| httplib | on | get | 66405 | 67307 | **+1%** | 1052 | 0.12→0.12 | 0.25→0.25 |
| seastar | off | put | 83815 | 82243 | **-2%** | 1285 | 0.19→0.19 | 0.37→0.43 |
| seastar | off | get | 152357 | 155199 | **+2%** | 2425 | 0.10→0.10 | 0.24→0.23 |
| seastar | on | put | 71330 | 71420 | **+0%** | 1116 | 0.20→0.20 | 0.49→0.50 |
| seastar | on | get | 122783 | 121683 | **-1%** | 1901 | 0.13→0.13 | 0.25→0.26 |

### 2.3 读法

- **GET 大对象**是 §4.3 的目标场景：seastar +52%（预取把后端读与 shard 上的
  socket 写完全重叠，收益最大）、builtin +35%（sendfile + 预取，p99 减半）、
  beast +34%、httplib +14%；TLS 下没有 sendfile，只剩预取与缓冲池，仍有 +16～34%
  （httplib/seastar TLS +34% 是双缓冲把加密与后端读重叠的收益）。
- **beast PUT 4 MiB 从 91 到 914 ops/s（10×）、16 KiB 从 21.8k 到 77.5k（3.5×）**：
  基线跑出来的意外发现。beast 的 `flat_buffer` 不预留容量时，`read_size = max(512,
  capacity − size)` 让每次 socket 读只取 **512 字节**，4 MiB 请求体 = 8192 次
  `recvmsg` + 8198 次 `timerfd_settime` + 7.7 万次 futex（`strace -c` 实测），单次
  4 MiB PUT 40 ms 对 builtin 6 ms。修复是一行 `buffer.reserve(io_chunk_size)`
  （[http-adapter.md §2.4](http-adapter.md) ⑨）。这印证了 roadmap 的判断——
  "beast 是性能路径"此前只是断言。
- **小对象**：ops/s 由请求开销主导，§4.3 的改动对它基本中性（±5%），beast GET +14%
  来自 `ResumeOn` 快路径省掉的 `asio::post`；beast PUT 的 3.5× 同样来自读粒度修复。
- **PUT 其他驱动持平**：请求体路径本轮未动。表中 httplib TLS PUT 的 −18% 是单次
  运行的抖动：机器空闲后各复测两次，前 1051 / 1074 ops/s，后 1055 / 1055 ops/s
  （响应侧改动不经过 PUT 的 small_body 路径）；seastar PUT 的 −6～7% 同理，
  改动后复测两次为 1121 / 1085（明文）与 911 / 909（TLS）ops/s，与改动前持平。
- 四驱动横向：明文 GET 大对象 seastar > builtin（sendfile）> beast ≈ httplib；TLS 下
  builtin ≈ seastar ≈ httplib > beast。**beast 的 TLS 明显落后**（GET 1.5k 对 3.0k），
  值得单独排查（asio ssl 的 record 处理与 strand 跳转）——留作后续项，本轮未处理。

## 3. 复现

```bash
./build.sh -B build-rel -DCMAKE_BUILD_TYPE=Release -DLIGHTS3_DUOSTORE=OFF -DLIGHTS3_CLOUDPROXY=OFF -DLIGHTS3_BUILD_TESTS=OFF
scripts/bench_matrix.sh build-rel/lights3 build-rel/s3adm --duration 8 --size 4M --json 4m.jsonl --label "$(git rev-parse --short HEAD)"
scripts/bench_matrix.sh build-rel/lights3 build-rel/s3adm --duration 8 --size 16K --concurrency 16 --objects 256 --json 16k.jsonl
scripts/bench_matrix.sh build-seastar/lights3 build-rel/s3adm --drivers seastar --duration 8 --size 4M   # seastar 变体单跑
```

脚本每格打印一行进度到 stderr，stdout 是 Markdown 表；`--json` 每格一行
`{label, version, driver, tls, mode, size, concurrency, duration_s, result}`，
`result` 就是 `s3adm bench --output=json` 的对象。跑之前确认机器空闲、
没有残留的 `lights3` 进程（`pgrep -x lights3`）。

## 4. 历史

| 日期 | 变更 | 摘要 |
| --- | --- | --- |
| 2026-09-05 | §4.3 数据面优化（预取、缓冲池、sendfile、pumping、ResumeOn 快路径、per-bucket 指标去锁、beast 读缓冲预留） | 大对象 GET +14～52%，beast PUT 3.5～10× |
