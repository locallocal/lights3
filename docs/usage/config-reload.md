# 配置热重载（roadmap §4.4）

> 状态：已落地（2026-09-05）。代码：`Application::reload_config`（`src/app/app.cc`）、
> `storage::BucketRouter::update`、`AsyncSemaphore::set_capacity`、
> `POST /-/admin/config/reload`（`src/s3/handlers/admin_tenants.cc`）、`lights3-ctl reload`。
> 单测 `tests/unit/test_reload.cc`，e2e `run_e2e.sh` 的 "roadmap §4.4" 一节。

## 1. 触发方式

| 方式 | 说明 |
| --- | --- |
| `kill -HUP <pid>` | 信号经自管道交给看门狗线程执行（信号处理函数只写一个字节），与 SIGINT/SIGTERM 同一套机制；systemd 单元可配 `ExecReload=/bin/kill -HUP $MAINPID` |
| `POST /-/admin/config/reload` | root 静态凭证；返回 JSON 报告（下文）；写一条 `config.reload` 审计记录 |
| `lights3-ctl reload` | 上一条的 CLI 包装，退出码 0/1 对应 `ok` |

两条路径共用同一把锁，串行执行；重载不阻塞请求路径（读文件与应用都在看门狗
线程或 admin 请求的协程里）。

## 2. 语义：整体校验、子集应用、其余报告

1. 重新 `Config::load(path)`——**与启动完全相同的解析与校验**。任何错误（语法、
   范围、交叉一致性、bucket 规则引用未知后端）都让本次重载整体拒绝，运行中的
   配置一字不改，报告 `ok=false` + `error`。
2. 校验通过后，只应用**可热更新子集**（§3），逐项写 INFO `config reload: applied …`；
3. 子集之外、但磁盘上已改变的键列入 `requires_restart` 并逐项 WARN——运维能立刻
   看到"改了但没生效"的项，而不是发现不了。

报告形状（admin API / `lights3-ctl reload` 输出）：

```json
{
  "ok": true,
  "applied": ["log.level: info -> debug", "http.request_timeout: 300 -> 120",
              "buckets.rules: 0 -> 1 rule(s)", "http.tls: certificate material re-read"],
  "requires_restart": ["http.max_connections"]
}
```

## 3. 可热更新子集

| 键 | 生效方式 |
| --- | --- |
| `log.level` | spdlog 全局级别即时切换 |
| `log.slow_request_threshold` | 下一请求起生效（dispatch 结束时读原子值；流式响应在响应体读尽时判定） |
| `http.request_timeout` | 下一请求起生效（dispatch 每次读原子值） |
| `http.transfer_stall_timeout` | 下一请求起生效（准入处理器每请求读原子值） |
| `http.min_part_size` | 下一次 CompleteMultipartUpload 起生效 |
| `http.metrics_access` | 下一次 `GET /-/metrics` 起生效（dispatch 读原子值） |
| `runtime.max_inflight_requests` | `AsyncSemaphore::set_capacity`：调大立即唤醒排队请求；调小则等在途请求归还许可（`available` 可短暂为负，期间不再放行新请求） |
| `ratelimit.per_ip_* / per_ak_*` | 重建限流器并原子替换；在途请求持有旧实例直到结束，不会悬空（`max_tracked` 除外：仅重启） |
| `buckets.rules` | `BucketRouter::update` 原子换代规则表；`S3Service`、lifecycle runner、usage tracker 的路由副本共享同一张表；在途请求继续用它解析时的表 |
| `backends[]` **新增条目** | backlog-sequence ⑦：按配置 `StorageRegistry::build`（新的 tiered 条目可引用已在运行的后端）→ `meter_backends` 包装 → 与规则表**同一快照**换入路由器（规则可以立刻指向新后端）；维护 job 表（fsck 与 duostore / tier 轮次，[cli.md §3.12](cli.md)）、`/-/metrics` 的 `backend=` 标签随之出现。构建失败（如 localfs 缺 root）整体拒绝，已建好的实例回滚关闭 |
| `backends[]` **删除条目** | 条件：不是 `default_backend`、文件里没有 tiered 条目再引用它、没有维护 job（fsck / duostore / tier 轮次）在跑（后两者违反 = 整体拒绝，前者 = 延后进 requires_restart）。删除先从路由表摘除（新请求立刻按剩余规则走），随后在一条**退役线程**上等该后端的在途租约归零（`MeteredBackend::wait_idle`，每次调用与每个打开的 GET 流各持一租约）再 `close()`，最后删掉它的指标序列；日志 `backend <name> removed: closed after in-flight requests drained`。等待不设上限（每 10s 记一行仍在等），进程关停时中止等待直接关闭 |
| `backends[]` 已有条目改参数 | **不应用**：实例带状态，重建等于重启；逐条列入 requires_restart（`backends (<name>: type/parameters changed …)`），运行实例保持启动时的配置。条目顺序变化不算变化（按名字匹配） |
| TLS 证书素材 | 每次重载强制 `Holder::reload_now()`（不等 `tls_reload_interval` 轮询）；seastar 由其可重载凭证自行监视文件 |

## 4. 明确不可热更新（列入 requires_restart）

- `http.driver / bind / port / admin_bind / admin_port / io_threads / max_header_size`，四类连接超时与
  `max_requests_per_connection`、`max_connections`（驱动构造期固化）；
- TLS 的**路径与旋钮**（`tls_cert/tls_key` 路径、`tls_client_*`、`tls_min_version`、
  cipher、`tls_sni`、`tls_reload_interval`）——证书**内容**热更新，参数不；
- `backends[]` 已有条目的参数变更（见 §3：增删可以，改不行）与
  `buckets.default_backend`（承载 `.sys` 与从它加载的各 store；连带"删除默认后端"也只延后报告）；
- `auth.*`（静态 root 凭证、凭证文件路径、同步周期、`tls_identity` 模式）——动态
  凭证、凭证文件与证书绑定表本就有各自的热加载 / 同步通道；
- `website` 静态条目（动态条目走 `?website` API）、`lifecycle.scan_interval`、
  `usage.*`、`audit.*`、`ratelimit.max_tracked`、停机/背压边界；
- `tables.*`（S3 Tables / Iceberg REST catalog，[s3-tables-design.md §10](../architecture/s3-tables-design.md)）——
  目录路由与守卫在装配期固化，整节 restart-only；
- `log.format / file / max_size / max_files / async*`——sink 与格式器在
  `Logger::init` 一次性构建（roadmap §5.2）。

## 5. 分期保留

- 后端实例热增删已落地（§3，2026-09-06）；已有实例**改参数**仍需重启——重建带
  状态的实例（duostore 的 meta 句柄、tiered 的降冷表、cloudproxy 连接池）与重启
  没有本质区别；
- 文件 mtime 自动轮询未做：SIGHUP/admin API 已足够且更可控，避免半写文件被
  误应用。

## 6. 测试

- `test_reload.cc`：信号量扩缩容（扩容唤醒排队者、缩容负可用度恢复）；路由表
  原子换代（副本共享、未知后端/换默认后端被拒且旧表保留）；`Application` 级
  端到端（无变化空报告；子集逐项 applied + startup-only 键进 requires_restart；
  坏文件整体拒绝且运行值不变；引用未知后端的规则在任何应用之前被拒）；admin
  端点（未签名/非 root 403、GET 405、报告 JSON、失败 400）。后端热增删（⑦）：
  路由器规则 + 后端集合一次换代、旧快照不受影响、默认后端不可丢；
  `MeteredBackend` 租约（打开的 GET 流计入在途，`wait_idle` 在最后一个租约归还
  时醒来）；`AdminJobs` 动态集合；`Application` 级——加 memory 后端并路由过去
  （HTTP 真请求落在新后端）、规则仍引用时删除被整体拒绝、改参数只报告、流打开
  时删除立刻生效而实例等流结束才关、删默认后端延后、tiered 引用被删后端 /
  构建失败整体拒绝。
- e2e：改 `log.level` 后 `SIGHUP` 看日志；`?request_timeout` 经 admin API 与
  `lights3-ctl reload` 应用；非 root 403；非法配置 400；热加 memory 后端 `hot` 并路由
  `hot-*`（PUT 落在新后端、指标出现 `backend="hot"`、`lights3-ctl object inspect` 看到
  它），限速 GET 流进行中删除——reload 立刻返回 removed、`hot-*` 立刻改走默认
  后端、close 日志在流结束后才出现、指标标签消失；删默认后端只延后报告。
