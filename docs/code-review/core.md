# core（L4）评审明细

> 覆盖 `src/core/`：task / executor / thread_pool / timer / background / semaphore /
> cancel / config / metrics / log / util（crypto/uri/time/hex）。跨模块问题（后台
> 协程自毁竞态、post-after-join、`percent_decode` 的 `+`）见 [README.md](README.md) §1。

**模块级评价**：并发原语的核心时序（sync_wait 报到、when_all 正常计票、对称转移栈
深、TimerQueue cancel 契约、AsyncSemaphore 许可移交、BackgroundTaskGroup 关闭铁律）
经逐条核对**正确**。缺陷集中在两处：后台协程启动模式（见 README §1.6）、以及 config
解析对运营者输入几乎不做校验（一簇静默错误配置）。

> **2026-08-01 更新**：本文件所列问题除 thread_pool post 无界（文档化取舍，保留）
> 外已全部修复，各条目标注 ✅已修复；跨模块问题（README §1）不在本轮范围。

## 高

### [高 · ✅已修复] when_all 启动循环中途抛异常 → 已起跑 runner 引用即将销毁的 when_all 帧
`src/core/task.h:303-316, 318-326`。`when_all_run(...).start()` 每次分配一个 runner 帧。若第 k 次分配抛 `bad_alloc`：异常进入 when_all promise，when_all 以异常"完成"，但前 k-1 个 runner 仍在飞行且持有指向 when_all 帧内 `latch/results/errors` 的引用；调用方 `co_await` 收到异常后销毁临时 Task（=销毁 when_all 帧）→ 在飞 runner 写已释放内存 → UAF/堆损坏。正常计票路径（RMW 链 acq_rel）核对无误，问题只在 OOM 异常路径。**修复**（2026-08-01）：启动循环 try 包裹并计数，异常时补齐未起跑者的票（awaiter 自身一票未投出，pending 不会减到 0，补票无 resume 竞态）、`co_await` latch 等已起跑 runner 收敛后再重抛；两个重载同改。

## 中

### [中 · ✅已修复] `parse_duration_sec` 乘法有符号溢出（UB），坏配置产出负时长
`src/core/config.cc:156-165`。`num * 3600` / `num * 86400` 用 `int`。`cold_after: 30000000h` → 3e7×3600 > INT_MAX → 有符号溢出 UB → 典型回绕成负数 → tiered 冷判阈值为负 → 全部对象立即判冷批量下沉。运营者 typo 一个数量级即命中，无报错。**修复**（2026-08-01）：`stoll` 64 位中间量，负数或乘积超 INT_MAX 抛 `runtime_error`；单测覆盖。

### [中 · ✅已修复] `parse_size` 负数经 `stoull` 回绕、移位溢出静默回绕
`src/core/config.cc:145-154`。`max_header_size: -1` → `stoull` 接受负号回绕成 2^64-1；`20000000000G` → `<<30` 无符号回绕成错误小值。限额类配置拿到荒谬值无诊断。**修复**（2026-08-01）：含 `-` 直接拒绝；移位前对 `size_t` 上界（`max >> shift`）校验，超界抛 `runtime_error`；单测覆盖。

### [中 · ✅已修复] YAML 子集解析器缩进不匹配的行被静默丢弃，parse 结束不校验消费完
`src/core/config.cc:76-126`。`parse_block` 以 `indent == 当前` 为继续条件，不匹配即退出；`parse()` 结束不检查 `i_ == lines_.size()`。列表项参数多/少缩进两格、或首行误缩进后的整个剩余文件，都会被静默丢弃——若丢的是可选参数（`io_threads`/`cold_after`）则配置无声失效。**修复**（2026-08-01）：`Line` 记录原始行号，`parse()` 末尾校验全部行已消费，否则抛 "unexpected indent at line N"（不匹配行会让各层块循环全部退出而非被中层吞掉，单一末尾校验即覆盖）；单测覆盖多/少缩进两种情形，仓库示例配置回归通过。

### [中 · ✅已修复] YAML 解析递归深度 = 嵌套层数，病态输入可栈溢出
`src/core/config.cc:83-122`（`parse_block` 递归 + `YamlNode` 同深度析构）。k 层需 k²/2 字节缩进，打爆 8MB 栈约需 800MB 文件——配置属运营者输入非网络面，评为理论边界。**修复**（2026-08-01）：`parse_block` 加深度上限 64（`kMaxDepth`），超限抛错；`YamlNode` 析构深度随之受限。日后复用于非信任输入无需再改。

## 低（简列）

| 位置 | 问题 | 状态 |
| --- | --- | --- |
| `config.cc` port 解析 | `to_int` → uint16 截断，`port: 70000` 静默截成 4464，无范围校验 | ✅已修复：范围限 0–65535 超界抛错（2026-08-02 调整：0 合法=内核分配空闲端口，单测/e2e 夹具依赖此约定） |
| `config.cc:194-198` | `max_inflight_requests <= 0` 无校验 → 第一个请求就在信号量上永久挂起（静默挂死非启动报错）；`io_threads` 负数则 `reserve(size_t(-n))` 抛 length_error（至少 fail-fast） | ✅已修复：`http.io_threads`/`runtime.io_threads`/`max_inflight_requests` < 1 一律启动时抛错 |
| `thread_pool.cc` post 无界 | `post` 队列无上限（docs 已声明取舍，记录以备量化背压时参考） | 保留（文档化取舍，不改） |
| util/time `%4d` | `parse_amz_date`/`parse_http_date` 接受 9999 年，下游 chrono 溢出（详见 s3.md 的 amz-date 条） | ✅已修复：三个 parse（http/iso8601/amz）年份限 1970–2200，超界返回 nullopt |

**核对无恙**（备查）：getentropy 返回值 / GCM nonce 随机不复用 / 恒定时间比较、Slot 三方竞争（先注册后补查、败者经共享块触摸、`(state,id)` 注册临界区内落位）、TimerQueue cancel"阻塞等在途 + 定时器线程自撤销不等待"、AsyncSemaphore 双缓冲防死锁、crypto/hex 各类畸形输入安全返回。
