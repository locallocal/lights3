# 配置热重载（roadmap §4.4）

> 状态：已落地（2026-09-05）。代码：`Application::reload_config`（`src/app/app.cc`）、
> `storage::BucketRouter::update`、`AsyncSemaphore::set_capacity`、
> `POST /-/admin/config/reload`（`src/s3/handlers/admin_tenants.cc`）、`s3adm reload`。
> 单测 `tests/unit/test_reload.cc`，e2e `run_e2e.sh` 的 "roadmap §4.4" 一节。

## 1. 触发方式

| 方式 | 说明 |
| --- | --- |
| `kill -HUP <pid>` | 信号经自管道交给看门狗线程执行（信号处理函数只写一个字节），与 SIGINT/SIGTERM 同一套机制；systemd 单元可配 `ExecReload=/bin/kill -HUP $MAINPID` |
| `POST /-/admin/config/reload` | root 静态凭证；返回 JSON 报告（下文）；写一条 `config.reload` 审计记录 |
| `s3adm reload` | 上一条的 CLI 包装，退出码 0/1 对应 `ok` |

两条路径共用同一把锁，串行执行；重载不阻塞请求路径（读文件与应用都在看门狗
线程或 admin 请求的协程里）。

## 2. 语义：整体校验、子集应用、其余报告

1. 重新 `Config::load(path)`——**与启动完全相同的解析与校验**。任何错误（语法、
   范围、交叉一致性、bucket 规则引用未知后端）都让本次重载整体拒绝，运行中的
   配置一字不改，报告 `ok=false` + `error`。
2. 校验通过后，只应用**可热更新子集**（§3），逐项写 INFO `config reload: applied …`；
3. 子集之外、但磁盘上已改变的键列入 `requires_restart` 并逐项 WARN——运维能立刻
   看到"改了但没生效"的项，而不是发现不了。

报告形状（admin API / `s3adm reload` 输出）：

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
| TLS 证书素材 | 每次重载强制 `Holder::reload_now()`（不等 `tls_reload_interval` 轮询）；seastar 由其可重载凭证自行监视文件 |

## 4. 明确不可热更新（列入 requires_restart）

- `http.driver / bind / port / io_threads / max_header_size`，四类连接超时与
  `max_requests_per_connection`、`max_connections`（驱动构造期固化）；
- TLS 的**路径与旋钮**（`tls_cert/tls_key` 路径、`tls_client_*`、`tls_min_version`、
  cipher、`tls_sni`、`tls_reload_interval`）——证书**内容**热更新，参数不；
- `backends`（后端实例增删/参数变更牵动生命周期与数据）与
  `buckets.default_backend`（承载 `.sys` 与从它加载的各 store）；
- `auth.*`（静态 root 凭证、凭证文件路径、同步周期）——动态凭证与凭证文件
  本就有各自的热加载通道；
- `website` 静态条目（动态条目走 `?website` API）、`lifecycle.scan_interval`、
  `usage.*`、`audit.*`、`ratelimit.max_tracked`、停机/背压边界；
- `log.format / file / max_size / max_files / async*`——sink 与格式器在
  `Logger::init` 一次性构建（roadmap §5.2）。

## 5. 分期保留

- 后端实例热增删：涉及 `StorageRegistry` 生命周期与 `.sys` 归属，先想清目标
  场景再动（roadmap 原文"另议"）；
- 文件 mtime 自动轮询未做：SIGHUP/admin API 已足够且更可控，避免半写文件被
  误应用。

## 6. 测试

- `test_reload.cc`：信号量扩缩容（扩容唤醒排队者、缩容负可用度恢复）；路由表
  原子换代（副本共享、未知后端/换默认后端被拒且旧表保留）；`Application` 级
  端到端（无变化空报告；子集逐项 applied + startup-only 键进 requires_restart；
  坏文件整体拒绝且运行值不变；引用未知后端的规则在任何应用之前被拒）；admin
  端点（未签名/非 root 403、GET 405、报告 JSON、失败 400）。
- e2e：改 `log.level` 后 `SIGHUP` 看日志；`?request_timeout` 经 admin API 与
  `s3adm reload` 应用；非 root 403；非法配置 400。
