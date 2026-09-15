# 性能基线：驱动 × TLS × put/get 矩阵

补上此前缺失的性能基线：仓内首份存档的 benchmark 数据，同时是数据面优化
（[http-adapter.md §2.4](../architecture/http-adapter.md)）的前后对照。数字只对
本机有意义，**用于相对比较与回归对照，不是产品指标**；换机器请用 §3 的命令
重新生成并替换本文表格。

## 1. 环境与方法

| 项 | 值 |
| --- | --- |
| 机器 | Intel i9-14900KF（32 线程）、30 GiB、Linux 7.0.0-31、g++ 15.2 |
| 数据目录 | `/tmp`（tmpfs）——localfs 后端的对象在内存文件系统上，测的是 HTTP 层 + 拷贝路径，**不含磁盘 IO** |
| 客户端 | `lights3-ctl bench`（httplib 同步客户端，每 worker 一条 keep-alive 连接），与网关同机 loopback |
| 网关 | localfs 后端；`http.io_threads: 8`（beast/httplib/seastar），`runtime.io_threads: 16`；TLS 为 openssl 自签 P-256 证书，客户端 `--insecure` |
| 大对象档 | 4 MiB × 8 workers × 8 s × 32 键 |
| 小对象档 | 16 KiB × 16 workers × 8 s × 256 键 |
| "前" | main 21968cf（含 #89），`Release`、`-DLIGHTS3_DUOSTORE=OFF -DLIGHTS3_CLOUDPROXY=OFF`（只裁掉无关后端，驱动与 localfs 路径不变）；seastar 用 `build-seastar`（RelWithDebInfo，改动前的树） |
| "后" | 分支 `feat/dataplane-perf`，同一套选项；seastar 同一 `build-seastar` 增量重建 |
| 脚本 | `scripts/bench_matrix.sh`（[testing.md §5](testing.md)），每格起一个新网关，顺序执行，机器空闲 |

延迟列来自 `lights3-ctl bench` 的直方图分位数（桶边界离散，p50 出现 6.15、12.29 这类
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
  （[http-adapter.md §2.4](../architecture/http-adapter.md) ⑨）。这印证了此前的判断——
  "beast 是性能路径"在此之前只是断言。
- **小对象**：ops/s 由请求开销主导，§4.3 的改动对它基本中性（±5%），beast GET +14%
  来自 `ResumeOn` 快路径省掉的 `asio::post`；beast PUT 的 3.5× 同样来自读粒度修复。
- **PUT 其他驱动持平**：请求体路径本轮未动。表中 httplib TLS PUT 的 −18% 是单次
  运行的抖动：机器空闲后各复测两次，前 1051 / 1074 ops/s，后 1055 / 1055 ops/s
  （响应侧改动不经过 PUT 的 small_body 路径）；seastar PUT 的 −6～7% 同理，
  改动后复测两次为 1121 / 1085（明文）与 911 / 909（TLS）ops/s，与改动前持平。
- 四驱动横向：明文 GET 大对象 seastar > builtin（sendfile）> beast ≈ httplib；TLS 下
  builtin ≈ seastar ≈ httplib > beast。**beast 的 TLS 明显落后**（GET 1.5k 对 3.0k），
  值得单独排查（asio ssl 的 record 处理与 strand 跳转）——已在 §3 收口（2026-09-13）。

## 3. 2026-09-13 复测：基线跑出的两个问题的收口

§2.3 留下的两项后续（beast 的 TLS GET 落后、请求体路径未做对称优化）已做完，实现见
[http-adapter.md §2.4 ⑩–⑬](../architecture/http-adapter.md)。方法与 §1 相同（同一台
机器、同一脚本、`Release` + `-DLIGHTS3_DUOSTORE=OFF -DLIGHTS3_CLOUDPROXY=OFF`，seastar 用
`build-seastar` 增量重建），"前"取 §2 的"后"列（2026-09-05 的树），"后"是本轮。

### 3.1 定位过程

- **beast TLS GET**：`strace -c` 一次 4 MiB TLS GET，beast 约 655 次 `futex`、258 次
  `sendmsg`（每条 16 KiB TLS 记录一次）、105 次 `epoll_wait`；builtin 同一请求 260 次
  `write` 与约 220 次 `futex`（后者来自线程池的每块调度，四驱动共有）。beast 多出的
  futex 是 8 个 io 线程共跑一个 `io_context` 的代价：每条记录的完成回调都要经全局队列
  唤醒另一个线程。改成每线程一个 `io_context` 后 TLS GET 1560 → 2406 ops/s，再看
  `strace`：剩 263 次 `timerfd_settime`（`beast::basic_stream` 每次 socket 操作武装并取消
  一次定时器）与每记录一次组合操作。会话级看门狗去掉定时器操作（吞吐在噪声内，
  p99 14.4 → 8.1 ms）；每 op CPU 仍比 builtin 多 0.5 ms（3.95 对 3.44 ms），根源是
  `asio::ssl::stream` 的 17 KiB 缓冲把每个 64 KiB 块拆成 4 轮异步操作，自写内存 BIO 的
  `TlsStream` 一次加密整块后 TLS GET 2469 → 3069 ops/s，与 builtin 持平。
- **PUT**：`openssl speed -evp md5` 单流 1.26 GB/s，4 MiB 约 3.3 ms；网关在 8 worker 下
  每个 4 MiB PUT 消耗 6.15 ms CPU、6.6 核忙，说明 PUT 是串行的 recv + MD5 + write 的
  CPU 路径封顶，驱动侧的"预取"帮不上（读的下一块无处并行）。真正对称的优化是把
  MD5 与写盘、收下一块流水化：`PipelinedMd5` 后 p50 7.2 → 6.2 ms，每 op CPU 升到
  7.1 ms（多了线程跳转），128 KiB 与 256 KiB 块无差别（跳转不是剩余开销来源）。剩余
  时间是内核拷贝（recv、tmpfs 写）与提交路径，MD5 单流 3.3 ms 是无法再降的下限。

### 3.2 4 MiB 对象（8 workers）

| 驱动 | TLS | 模式 | ops/s 前 | ops/s 后 | Δ | MiB/s 后 | p50 ms 前→后 | p99 ms 前→后 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| builtin | off | put | 1065 | 1221 | **+15%** | 4883 | 7.20→6.18 | 16.14→11.27 |
| builtin | off | get | 5709 | 5876 | **+3%** | 23503 | 1.52→1.50 | 2.05→2.04 |
| builtin | on | put | 927 | 1073 | **+16%** | 4293 | 7.62→6.38 | 16.19→15.59 |
| builtin | on | get | 3076 | 3067 | **-0%** | 12267 | 2.80→2.83 | 4.08→4.08 |
| beast | off | put | 914 | 1202 | **+32%** | 4808 | 10.67→6.16 | 16.27→8.18 |
| beast | off | get | 4568 | 5547 | **+21%** | 22188 | 1.58→1.52 | 3.84→2.60 |
| beast | on | put | 727 | 1127 | **+55%** | 4509 | 12.29→6.26 | 16.31→14.78 |
| beast | on | get | 1511 | 2913 | **+93%** | 11651 | 6.15→2.75 | 8.15→7.15 |
| httplib | off | put | 1082 | 1209 | **+12%** | 4837 | 7.37→6.15 | 16.16→8.17 |
| httplib | off | get | 4561 | 5709 | **+25%** | 22837 | 1.71→1.51 | 4.03→2.04 |
| httplib | on | put | 879 | 1202 | **+37%** | 4807 | 10.01→6.20 | 16.33→13.33 |
| httplib | on | get | 3005 | 2882 | **-4%** | 11528 | 2.71→2.87 | 4.40→6.59 |
| seastar | off | put | 1031 | 1220 | **+18%** | 4881 | 6.77→6.15 | 16.03→8.17 |
| seastar | off | get | 6070 | 6183 | **+2%** | 24731 | 1.55→1.54 | 3.11→2.04 |
| seastar | on | put | 880 | 1034 | **+17%** | 4135 | 12.24→6.36 | 16.31→15.51 |
| seastar | on | get | 3005 | 2940 | **-2%** | 11762 | 3.13→3.14 | 7.50→7.60 |

### 3.3 16 KiB 对象（16 workers）

| 驱动 | TLS | 模式 | ops/s 前 | ops/s 后 | Δ | MiB/s 后 | p50 ms 前→后 | p99 ms 前→后 |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| builtin | off | put | 80364 | 77626 | **-3%** | 1213 | 0.20→0.20 | 0.47→0.48 |
| builtin | off | get | 151198 | 142186 | **-6%** | 2222 | 0.10→0.10 | 0.25→0.25 |
| builtin | on | put | 70845 | 67801 | **-4%** | 1059 | 0.21→0.21 | 0.50→0.50 |
| builtin | on | get | 124612 | 115981 | **-7%** | 1812 | 0.13→0.15 | 0.25→0.26 |
| beast | off | put | 77474 | 75710 | **-2%** | 1183 | 0.20→0.20 | 0.48→0.49 |
| beast | off | get | 127500 | 136053 | **+7%** | 2126 | 0.11→0.11 | 0.25→0.25 |
| beast | on | put | 65572 | 63523 | **-3%** | 993 | 0.22→0.24 | 0.60→0.51 |
| beast | on | get | 112165 | 111144 | **-1%** | 1737 | 0.17→0.14 | 0.26→0.29 |
| httplib | off | put | 41920 | 39433 | **-6%** | 616 | 0.19→0.20 | 0.47→0.49 |
| httplib | off | get | 93214 | 89673 | **-4%** | 1401 | 0.09→0.09 | 0.13→0.13 |
| httplib | on | put | 33409 | 33407 | **-0%** | 522 | 0.23→0.23 | 0.51→0.51 |
| httplib | on | get | 67307 | 64591 | **-4%** | 1009 | 0.12→0.12 | 0.25→0.25 |
| seastar | off | put | 82243 | 80632 | **-2%** | 1260 | 0.19→0.19 | 0.43→0.40 |
| seastar | off | get | 155199 | 151413 | **-2%** | 2366 | 0.10→0.10 | 0.23→0.24 |
| seastar | on | put | 71420 | 69156 | **-3%** | 1081 | 0.20→0.20 | 0.50→0.50 |
| seastar | on | get | 121683 | 116146 | **-5%** | 1815 | 0.13→0.13 | 0.26→0.26 |

### 3.4 读法

- **beast TLS GET 与其他驱动持平**（3.0k 左右），TLS PUT 从垫底追到并列；明文 GET 仍是
  seastar > builtin（sendfile）> beast ≈ httplib，但 beast 与 builtin 的差距缩小到 sendfile
  的份额。
- **PUT 4 MiB 四驱动同步提升 12～18%**（httplib TLS 与 beast 更多：前者的 2026-09-05 数字
  是抖动偏低的一次，后者叠加了驱动侧改动）：这是 `PipelinedMd5` 的收益，与驱动无关；
  上限由 MD5 单流速度决定。
- **小对象**：16 KiB 全部在 `PipelinedMd5` 的内联阈值内（256 KiB），请求体路径不变。本轮
  16 KiB 整体比 2026-09-05 低 2～7%，未改动的 seastar 与 builtin GET 同样如此，是当日的
  噪声底而非回归；beast 明文 GET +7%、TLS 与 asio 版持平（内存 BIO 流最初逐缓冲封记录时
  曾慢 15%，合并小缓冲后消除，见 http-adapter.md §2.4 ⑫）。
- 不需要再留后续项：todo 中"性能基线跑出的新问题"一节已随之删除。

## 4. 2026-09-15：走查条目的性能收口

### 4.0 R4：请求尾部的固定开销（访问日志与进程级锁）

四处"每请求都付"的开销：`Logger::access()` 的进程级互斥、默认**同步**
的访问日志、限流器的键拷贝与第二次加锁、builtin 每请求两次全局锁 + 一次 `std::set`
节点分配。前三项在 16 并发下都落在噪声里；**真正的大头是同步日志**。

### 4.1 隔离测量（builtin + memory 后端 + 16 KiB，Release）

用 `log.level` 与 `log.async` 三种组合跑同一负载，`warn` 一档不产生访问行，是"日志
完全不要钱"的上限：

| 并发 | 配置 | PUT ops/s | GET ops/s | PUT p99 | GET p99 |
| --- | --- | --- | --- | --- | --- |
| 16 | info + 同步（改前默认） | 154.3k | 201.9k | 0.255 ms | 0.239 ms |
| 16 | info + 异步 | 158.5k | 211.4k | 0.252 ms | 0.238 ms |
| 64 | info + 同步（改前默认） | 203.0k | 254.4k | 1.02 ms | 1.01 ms |
| 64 | info + 异步 | 218.5k | 304.4k | 0.99 ms | 0.87 ms |
| 64 | warn（无访问行，上限） | 224.1k | 318.1k | 0.98 ms | 0.80 ms |

64 并发下同步日志吃掉 **PUT 9%、GET 20%** 的吞吐；换成异步后分别只差上限的 2.5% 与
4.3%。p99 也跟着从 1.01ms 降到 0.87ms。于是 `log.async` 的默认值改为 `true`
（代价：硬崩溃时最多丢 `async_queue` 条记录；warn/error 仍逐条 flush）。

### 4.2 改前 / 改后（`scripts/bench_gate.sh`，16 并发 ×10s ×3 轮）

| | PUT ops/s | GET ops/s | GET p99 |
| --- | --- | --- | --- |
| 改前 | 156.2 / 157.5 / 159.6k | 204.7 / 203.5 / 205.3k | 0.226 ms |
| 改后 | 157.9 / 160.1 / 160.4k | 215.1 / 214.8 / 215.7k | 0.219 ms |

16 并发 GET +5.2%，PUT +1.1%（在噪声边缘）；64 并发见上表，PUT +8.1%、GET +19.2%。
去锁三项（`Logger::access` 无锁读、限流器异构查找 + 无锁 release、builtin 的
per-connection 原子位）单独测不出来 —— 限流默认关闭时根本不进那段代码，另两处每请求
各几百纳秒，对 ~6 µs 的请求是噪声。它们照样改了：代价为零，且争用随核数与并发增长。

### 4.3 R5：aws-chunked 解帧的中转缓冲

`ChunkedSigV4BodyReader` 过去把**全部** body 先读进自己的 16KiB 栈缓冲再 memcpy 给调用
方，于是单次 `read()` 最多吐 16KiB —— 驱动一次要 64KiB（`io_chunk_size`），一 MiB 的
body 要跑四倍的协程往返，每个字节还多一次 memcpy 加一次 `erase(0,n)` 的 memmove。改成
chunk 数据直接读进调用方的 span，只有框架（chunk 头、trailer 段）还走暂存区。

方法：同一台机器、memory 后端 + builtin + auth 关闭，同样 128MiB 的 body 各传 6 次，
一次普通 body、一次 aws-chunked（64KiB 一个 chunk）；比的是**同一轮内**两者的比值，
避开机器漂移。

| | 普通 body（中位） | aws-chunked（中位） | 分块/普通 |
| --- | --- | --- | --- |
| 改前 | 0.198 s | 0.234 s | **1.185** |
| 改后 | 0.204 s | 0.196 s | **0.96**（噪声内持平） |

分块路径吞吐 575 → 686 MB/s（**+19%**），解帧开销从 +18% 降到量不出来。

"buf_ 改读游标去掉 erase" 的另一半**没做**：直读之后 `buf_` 只剩每个 chunk 头那次
fill 的尾巴（≤16KiB），下一次 read 通常一次取空，`erase(0, 全部)` 本就是 O(1)；上表
显示剩余开销已在噪声内，再加一层游标是拿复杂度换不出东西。

### 4.4 R6：每请求多次路由表全扫描 —— 量不出来

走查条目 R6 把"一个请求扫五六遍 35 条的分派表"列为性能项。改成只解析一次之后，用改前 /
改后两个二进制**交错**跑（同一轮内 A/B/A/B，规避机器漂移），并发 64、16 KiB、各 3 轮：

| | PUT ops/s（三轮） | GET ops/s（三轮） |
| --- | --- | --- |
| 改前 | 224.1 / 217.9 / 218.0k | 303.8 / 303.4 / 304.1k |
| 改后 | 221.0 / 221.0 / 221.8k | 300.5 / 304.2 / 284.6k |

**没有可测差异**（PUT 中位 +1.4%、GET 中位 −1.1%，都在本机噪声内；16 并发同样如此）。
表扫描在 method 比较上就短路掉绝大多数条目，五遍加起来也压不过测量噪声。

改动仍然保留，但理由不是性能：它把"授权判定与真正执行的 handler 取到同一条路由"从
巧合变成结构，见 [s3-protocol.md §2](../architecture/s3-protocol.md)。**后来者不必再
测一遍。**

### 4.5 R8：httplib 每个带 body 的请求一个 `std::thread`

httplib 的推转拉需要第二个线程驱动 `ContentReader`（请求线程正在跑 handler）。这个线程
原本是**每请求现起一个** `std::thread`：一次创建 + 一次 join，外加默认 8MiB 栈虚存。改成
驱动内的固定 `PumpPool`，大小取请求线程数 `max(io_threads, 8)` —— 一个请求线程同时只驱动
一个泵，泵永不排队等 worker。

改前/改后两个二进制交错跑，`bench_matrix.sh --drivers httplib`，16 KiB、16 并发、10s、
4 轮：

| | PUT ops/s（四轮） | 均值延迟 | p99 |
| --- | --- | --- | --- |
| 改前 | 38.8 / 37.0 / 38.7 / 38.6k | 0.412–0.431 ms | ~0.50 ms |
| 改后 | 47.0 / 45.2 / 46.6 / 46.4k | 0.340–0.353 ms | 0.26–0.34 ms |

**PUT +20%**（中位 38.6 → 46.5k），均值延迟 −17%，p99 −40%。GET 无 body、不走泵，作为
对照未变。注意 httplib 的定位仍是功能验证而非性能路径，这条只是把一处纯粹的浪费去掉。

## 5. 复现

```bash
./build.sh -B build-rel -DCMAKE_BUILD_TYPE=Release -DLIGHTS3_DUOSTORE=OFF -DLIGHTS3_CLOUDPROXY=OFF -DLIGHTS3_BUILD_TESTS=OFF
scripts/bench_matrix.sh build-rel/lights3 build-rel/lights3-ctl --duration 8 --size 4M --json 4m.jsonl --label "$(git rev-parse --short HEAD)"
scripts/bench_matrix.sh build-rel/lights3 build-rel/lights3-ctl --duration 8 --size 16K --concurrency 16 --objects 256 --json 16k.jsonl
scripts/bench_matrix.sh build-seastar/lights3 build-rel/lights3-ctl --drivers seastar --duration 8 --size 4M   # seastar 变体单跑
```

脚本每格打印一行进度到 stderr，stdout 是 Markdown 表；`--json` 每格一行
`{label, version, driver, tls, mode, size, concurrency, duration_s, result}`，
`result` 就是 `lights3-ctl bench --output=json` 的对象。跑之前确认机器空闲、
没有残留的 `lights3` 进程（`pgrep -x lights3`）。

## 6. 历史

| 日期 | 变更 | 摘要 |
| --- | --- | --- |
| 2026-09-05 | §4.3 数据面优化（预取、缓冲池、sendfile、pumping、ResumeOn 快路径、per-bucket 指标去锁、beast 读缓冲预留） | 大对象 GET +14～52%，beast PUT 3.5～10× |
| 2026-09-13 | beast 每线程 io_context、会话看门狗、内存 BIO TlsStream；PipelinedMd5 请求体 MD5 流水化（http-adapter.md §2.4 ⑩–⑬） | beast TLS GET 4 MiB +93%（与其他驱动持平），4 MiB PUT 四驱动 +12～55%，p50 7.2 → 6.2 ms |
| 2026-09-15 | 请求尾部去锁 + `log.async` 默认开（§4.0–4.2）；aws-chunked 解帧直读（§4.3）；路由只解析一次（§4.4，性能上是阴性结果）；httplib 泵线程池（§4.5） | 64 并发 16 KiB：PUT +8.1%、GET +19.2%，GET p99 1.01 → 0.87 ms；128 MiB 分块 PUT +19%，与普通 body 持平 |
