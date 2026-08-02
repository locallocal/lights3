# LightS3 全项目代码评审（2026-08-01）

> 本目录记录一次覆盖全部 22k 行源码的只读评审发现，按**框架 / 模块 / 文件**三个
> 粒度组织。方法：按子系统并行深读 + 交叉印证，每条发现标注 `文件:行/函数`、
> 触发场景、严重度与验证状态。评审只记录问题，不改动源码——修复另行排期。

## 验证状态说明

- **✅ 已复核**：评审后由人工独立读代码复现了机制/触发链，确认为真。
- **confirmed**：给出了确定的代码路径与触发方式（agent 判定，未逐条人工复核）。
- **plausible**：机制成立，但触发需特定时序、后端实现或客户端行为配合。

## 严重度分布

| 子系统 | 严重(P0) | 高 | 中 | 低 | 明细 |
| --- | --- | --- | --- | --- | --- |
| 框架级（跨模块） | 1 | 5 | 1 | — | 本文 §1 |
| core（L4） | — | 2 | 4 | 7 | [core.md](core.md) |
| http（L1） | — | 1（+走私簇） | 6 | 6 | [http.md](http.md) |
| s3（L2） | — | 3 | 14 | 8 | [s3.md](s3.md) |
| storage 通用/localfs 系（L3） | — | 1 | 4 | 5 | [storage.md](storage.md) |
| duostore / cloudproxy（L3） | — | 2 | 2 | 3 | [storage.md](storage.md) |

> 注：跨模块问题（`percent_decode` 的 `+`、认证 fail-open、后端 close 等）被多个子系统
> 独立发现，为避免重复，统一收敛到本文 §1，各模块文档只留指针。

## 总体评价

工程质量整体偏高：分层依赖单向无违例（282 条跨模块 include 全部核对）、协程取消/
定时器/后台任务生命期的并发推理严谨、duostore 四引擎记账语义对齐良好、SigV4 恒定
时间比较与 `.sys` 保留桶的多数入口都做对了。**风险集中在三处**：(1) virtual-host
寻址这一条路径缺少 path-style 已有的 bucket 校验，形成一个严重的越权读取面；(2) 一
批客户端可控输入未设上限/未严格校验（HTTP framing、max-keys、amz-date 年份、
Content-Length），其中 builtin/seastar 手写解析器的消息边界处理是最大的协议风险面；
(3) 若干"契约靠约定而非代码保证"的地方在特定后端/关停时序下失效（cloudproxy 跳过
payload 校验、后端 close 不被调用、请求取消是死代码）。

---

## 1. 框架级（跨模块）发现

这些问题跨越多个模块或属于影响全局的横切基础设施缺陷，按严重度排列。

### 1.1 [严重 · ✅已复核] virtual-host 寻址的 bucket 名从不校验 → 任意文件读 + `.sys` 凭证泄露

**位置**：`src/s3/service.cc:60-76`（`resolve_address` vhost 分支）、`:107-109`（唯一拦截，只挡 `.` 前缀）；`src/storage/localfs/localfs_backend.cc:162`（`get_object` 只 `validate_object_key`，**独缺** `validate_bucket_name`）、`:44-49`（`object_path = root_ / fs::path(bucket) / key`）；xlocalfs 同构。

**机制**：path-style 经 `parse_bucket_key` 在首个 `/` 切分，bucket 不可能含 `/`；`..` 单段被 `.` 前缀检查挡下。**唯独 vhost 路径**直接把 `Host` 头前缀当 bucket，全链路无字符校验，且 bucket 可含 `/` 甚至以 `/` 开头。`std::filesystem::operator/` 遇绝对路径右操作数直接替换整条路径（`root_ / "/etc"` == `/etc`）。localfs `get_object` 又不校验 bucket 名（对比同文件 `put_object:119` 是校验的）。

**触发场景**（前提：`http.base_domain` 已配置，即启用 S3 标准 vhost 寻址）：
- 任意持合法凭证者（含被 policy 限制的普通凭证）`GET /passwd` + `Host: /etc.example.com` → bucket=`/etc` → 读出 `/etc/passwd`。签名用攻击者自己的 SK，验签正常通过。
- `Host: b/../.sys.example.com`（`b` 为任意已存在的桶）→ bucket=`b/../.sys` 绕过 `.sys` 封锁 → `ListObjects`/`GET credentials/{AK}` 列举并读取**全部动态凭证的 SK**（未设 `LIGHTS3_MASTER_KEY` 时为明文）→ 横向越权到任意凭证、绕过所有 per-credential policy。
- 认证关闭时上述均匿名可达。

**影响后端**：localfs / xlocalfs（默认后端）及以它们为 local 层的 tiered。duostore/cloudproxy 因 bucket 走 KV/远端命名空间，表现为 NoSuchBucket。

**建议**：`resolve_address` 解析出 vhost bucket 后立即 `validate_bucket_name`；并在各 localfs/xlocalfs 数据面入口补 `validate_bucket_name(bucket)`（get/head/list/delete 与 put 对齐），作为纵深防御。

### 1.2 [高 · ✅已复核 · ✅已修复] 认证 fail-open：凭证表运行期变空 → 整个端点匿名开放

> **2026-08-02 已随 s3 评审修复落地**（详见 [s3.md](s3.md) 高危第三条）：`enabled()`
> 改为只升不降的闩锁（表变空后 fail-closed）；`apply_file_credentials`/`sync_now`
> 拒绝把非空表清成空表（保留旧表 + LOG_ERROR + `degraded` → `/-/readyz` 503）。

**位置**：`src/s3/auth/sigv4.cc:388-389`（`verify()` 首行 `if (!enabled()) return ""`）、`sigv4.h:33`（`enabled() = provider_->has_credentials()`）；`src/s3/auth/credential_store.cc:344`（`has_credentials() = !creds_.empty()`）、`apply_file_credentials()`（先清空 kFile 再插入）、`sync_now()` 吊销分支。

**机制**：认证开关不是配置项，而是"当前内存凭证表是否非空"这个**运行期可变**量。部署若不含静态（config）凭证（文档 §3 明确支持的"纯 credentials_file / 纯动态"形态），把文件误编辑成 `{"credentials": []}`（合法 JSON，解析成功）→ 下一个 mtime 轮询 → `creds_` 变空 → 此后 `verify()` 对每个请求返回空 AK、`authorize()` 因 `ak.empty()` 直接放行 → **匿名可读可写**，且只有启动时判过一次的 "authentication is DISABLED" 告警，运行期静默。`sync_now()` 在 `.sys` 被外部清空时同效。

**建议**：把"是否要求认证"固化为启动时确定的布尔；`apply_file_credentials()`/`sync_now()` 拒绝把表清空到 0（保留旧表 + `LOG_ERROR` + `/-/readyz` 转 503）。

### 1.3 [高 · ✅已复核] `percent_decode` 把 `+` 解码为空格，被用于 URL path / copy-source → 对象键静默损坏

**位置**：`src/core/util/uri.cc:32-35`（无模式参数，无条件 `+`→空格）；path 调用点 `src/http/drivers/common.h:21`（`req.path`，即对象 key）、`src/s3/handlers/common.h:70`（`x-amz-copy-source`）。

**机制**：`+`→空格是 `application/x-www-form-urlencoded` 规则，不适用于 URL path 与 S3 对象键。`PUT /bucket/a+b` 存成 key `a b`；`x-amz-copy-source: /b/report+2026.csv` 拷贝的是 `report 2026.csv`。三个评审（core/http/s3）独立发现，确定性数据面损坏且全程无报错。query 侧调用点（`common.h:30-33`）用它才是对的。

**建议**：`percent_decode(s, bool plus_is_space)`，path / copy-source / SigV4 canonical query 传 false，仅真正的 form 解析传 true。

### 1.4 [高 · ✅已复核] 后端 `close()` 生产环境从不调用；关停顺序 close 晚于 `pool->join()`

**位置**：`src/main.cc:90-96`（`server->run()` 返回 → `cred_store->shutdown_background()` → `pool->join()`，注释称"各后端冲刷"实际只有 join）；backends 由 service/router 持有，只在 main 返回（join 之后）走析构兜底。`DuoStoreBackend::~`（跳过 `data_->close()` 的 active pack 封存/rados flush）、`TieredBackend::~`（少 `save_atime_snapshot()`）。

**机制**：`IStorageBackend::close()` 全仓无生产调用点（只有测试）。析构兜底 ≠ close：SIGTERM 滚动重启时 duostore active pack 未封存（重启靠 `abandon_stale_packs` 以 live=0 补封，GC 统计从零起算）、tiered 每次丢 atime 快照（判冷退化为 mtime，重启后一轮误判热对象为冷并下沉）。更危险：`pool->join()` 在后端析构**之前**，析构期后台收尾若再 `co_await pool->schedule()` 会命中 join 后的池抛异常（见 §1.7）。

**建议**：`main` 在 `pool->join()` **之前**遍历 backends `co_await close()`，再 join。

### 1.5 [高 · confirmed] 请求级取消是死代码：超时/断连取消整条链路未接线

**位置**：`src/s3/service.h:20-26`、`service.cc:81`（`RequestContext.cancel` 是默认"永不取消" token，且 `ctx` 未传给 `route()`/handler/后端）；后端一律 `co_await pool_->schedule()` 不带 token；`with_timeout` 全仓无生产调用点。

**机制**：docs/concurrency.md §5 声称取消源有三（客户端断连/请求超时/进程 shutdown），实际三者均未接线。后果：无任何请求级超时。`max_inflight_requests`（默认 1024）的许可在 dispatch 前获取、随协程存活到请求结束。慢客户端每 59s（< idle_timeout 60s）发一字节的 PUT 可无限期持有许可；1024 条这样的连接即让所有新请求在 `AsyncSemaphore` 上永久排队（信号量是排队非拒绝），网关对外无响应且无机制踢除。cloudproxy 远端黑洞时同理占住池线程+许可。

**建议**：把 `ctx.cancel` 接进 handler/后端签名，dispatch 挂上 `with_timeout`（请求超时）与驱动的断连信号；至少给 inflight 许可加超时上界。

### 1.6 [高 · ✅已复核] 后台协程 `run_detached` 急切启动 → ramp 与自毁竞态（每个后台 tick 都在触发窗口）

**位置**：`src/core/background.cc:12-31`（`Detached` 用 `suspend_never` initial_suspend，`run_detached` 被 `spawn()` 同步调用）；对照 `src/core/task.h:261-263`（`WhenAllRunner` 为同款自毁模式特意改用 `suspend_always` + 显式 `start()`，注释明写"否则协程迁到池线程并完成自毁时 ramp 可能仍在触碰帧（真实数据竞争）"）。

**机制**：定时器回调（TimerQueue 线程）`spawn(task)` → ramp 在定时器线程直接跑协程体 → task 在首个 `co_await pool.schedule()` 迁到池线程 → 池线程完成任务、`done()`、经 `final_suspend=suspend_never` **销毁帧**；与此同时定时器线程的 ramp 尚未从 `run_detached` 返回，若 ramp 尾部仍触碰帧（正是 when_all 注释记录的实测行为）即 use-after-free。GC / orphan scan / 凭证 file_tick / sync_tick 每次触发都在掷骰子。这是项目自己在 when_all 已修、却在 background 漏修的同款缺陷。

**建议**：与 `WhenAllRunner` 对齐——`suspend_always` initial + 返回 handle，ramp 返回后再 `start()`。

### 1.7 [中 · ✅已复核] `ThreadPool::post` 在 join 后抛异常，却被 noexcept 上下文调用 → 关停期 `std::terminate`

**位置**：抛出点 `src/core/thread_pool.cc:22`（`if (stopping_) throw runtime_error("post after join")`）；noexcept 调用点 `src/core/task.h:55`（`FinalAwaiter::await_suspend(...) noexcept` 内 `cont_executor->post`）、`src/core/semaphore.h:88`（`release_one` 内 `exec_->post`，源头是 `Permit::~Permit` 隐式 noexcept 析构）。

**机制**：docs §3.1 说 post"不可失败"，但 join 后它就是会抛，且抛点落在不许抛的上下文。生产路径即可触发：main 用 `ThreadPoolExecutor` 作 inflight 信号量的唤醒 executor；builtin 优雅退出带在途连接直接返回 → `pool->join()` → 某在途请求完成 → `Permit` 析构 → `release_one` → `post` 抛 → 逸出 noexcept 析构 → **terminate**，"clean exit" 变 abort。与 §1.4（close 晚于 join）叠加放大。

**建议**：post 对 stopping 改为"接受并排入收尾"，或在 noexcept 消费点吞掉/降级；配合 §1.4 修正关停顺序。

---

各模块的模块级与文件级明细见：[core.md](core.md) · [http.md](http.md) · [s3.md](s3.md) · [storage.md](storage.md)。
