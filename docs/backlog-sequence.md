# 分期保留项的实施顺序

对 [backlog.md §1](backlog.md) 十个"分期保留"条目给出先后顺序。排序依据只有
三条：**依赖关系**（谁给谁铺路）、**投入产出**（价值 ÷ 难度）、**同一主题一次
做完**（少切换上下文、共用测试脚手架）。每项写明范围、入口、验收与预估工作量；
预估以"一个人、熟悉代码"为准，含文档（中英）与测试。做完一项就把它从
backlog.md §1 删掉，本文对应行划掉并记日期。

## 0. 顺序总览

| 序 | 阶段 | 条目 | 价值 / 难度 | 预估 | 为什么排在这里 |
| --- | --- | --- | --- | --- | --- |
| 1 | A 速赢 | ~~tiered 本地层已用字节 gauge~~ **已完成（2026-09-05）** | 中 / 低 | 0.5 天 | 零风险小改，监控面板立刻少一个"用逐出速率猜水位"的绕路 |
| 2 | A 速赢 | 独立 admin 端口 | 低 / 低 | 1 天 | 后面每一个新增 admin 端点都会落到这个端口上，先把地基铺好 |
| 3 | A 速赢 | scrub / fsck 的 admin 端点 | 低 / 低 | 0.5 天 | 紧跟 ②：第一个用上 admin 端口的新端点，顺手验证端口隔离 |
| 4 | B 多实例一致性 | STS 会话表多实例共享 | 中 / 中 | 2–3 天 | 多网关部署下唯一"换个实例就失效"的功能面；复用凭证同步机制，是 ⑤ 的热身 |
| 5 | B 多实例一致性 | 跨网关元数据缓存失效协议 | 中 / 高 | 4–5 天 | 与 ④ 同一主题（多网关共享状态），共用两实例 e2e 脚手架；解除 redis 共享 meta 下"缓存默认关"的限制 |
| 6 | C 身份 | mTLS 客户端证书 → 凭证 / 租户身份映射 | 中 / 中 | 2–3 天 | 独立于 B；租户模型与 tls_client_auth 都已就位，只差绑定规则 |
| 7 | D 运维纵深 | 后端实例增删热重载 | 中 / 高 | 4–5 天 | 动 Application 生命周期与路由表换代，风险最高，放在多实例与身份两条线稳定之后 |
| 8 | D 运维纵深 | duostore meta 增量备份 / PITR | 中 / 高 | 5+ 天（按引擎分期） | 四个引擎各不相同，天然可拆成四个小迭代；不阻塞任何其他项 |
| 9 | E 机会项 | client-c 结构化错误码上游贡献 | 低 / 中 | 1–2 天 + 上游周期 | 外部依赖节奏，任何空档都能做；功能不受影响 |
| 10 | E 机会项 | `HeaderMap` 线性扫描 / `BlockQueue` 双拷贝 | 低 / 低 | 视 profile | 有 profile 证据才动；没有证据就一直排在最后 |

依赖只有两条实线：② → ③（端点落到新端口上）、④ → ⑤（共用多实例 e2e 脚手架
与 `.sys` 共享状态的写法）。其余各项相互独立，A/B/C 三个阶段可以并行推进；
D 阶段建议等 B 落地后再动 ⑦。

## 1. 阶段 A：速赢（合计约 2 天）

### ① tiered 本地层已用字节 gauge

- **范围**：`ITierLocal` 增加 `space_usage()`（已用 / 总量 / 可用，`std::optional`，
  探针失败返回空），`LocalFsTierLocal` 用现成的 `statvfs`
  （`src/storage/tiered/tier_local_fs.cc` 已在 `cache_space_ok` 里调用），
  `DuoStoreTierLocal` 汇总本地 extent 字节。`TieredBackend::init_metrics` 注册
  `lights3_tiered_local_used_bytes` / `_total_bytes` 两个 `gauge_callback`，
  再加 `lights3_tiered_local_high_watermark_bytes`（配置换算，方便面板画线）。
- **入口**：`src/storage/tiered/tier_local.h`、`tier_local_fs.cc`、`tier_local_duo.cc`、
  `tiered_backend.cc`；`deploy/grafana/gen_dashboard.py`（tiered 行的水位面板改为
  直接画 used / high_watermark，逐出速率面板保留）；`deploy/prometheus/lights3.rules.yml`
  加一条"接近高水位"告警。
- **验收**：`test_tiered.cc` 断言 gauge 在场且写入后单调；ctest `monitoring_assets`
  通过（dashboard 必须由生成器重新生成，逐字节一致）；[monitoring.md](monitoring.md)、
  [tiered-storage.md](tiered-storage.md) 各补一行。

### ② 独立 admin 端口

- **范围**：`http.admin_bind` / `http.admin_port`（默认空 = 维持现状，所有面共用
  数据面端口）。配置了就再起一个同驱动的 `IHttpServer` 实例，只挂 `/-/` 路由；
  数据面端口上的 `/-/admin/*` 与 `/-/metrics` 改回 404（`/-/healthz|readyz` 两边都留）。
  TLS 素材共用同一 `tls::Holder`。`metrics_access: root` 语义不变。
- **入口**：`src/app/app.cc`（第二个 `server_`，关停顺序与 `shutdown_grace` 共用）、
  `src/s3/service.cc` 分派处的 `internal` 判定（加"本监听是否 admin 面"的标志）、
  `src/core/config.{h,cc}` 校验（admin 端口不得与数据面相同）。
- **验收**：单测起两端口，断言数据面 404 / admin 面 200；`config-reload` 不覆盖
  admin 端口（列入 `requires_restart`）；`s3adm` 的 `--endpoint` 文档说明 admin 命令
  改指 admin 端口；[http-adapter.md §2.1](http-adapter.md)、[cli.md §3.1](cli.md)。

### ③ scrub / fsck 的 admin 端点

- **范围**：`POST /-/admin/fsck/<backend>?max_mbps=N`（root），后台跑一轮
  `run_scrub_once`，立即返回 job id；`GET /-/admin/fsck/<backend>` 查进度与上次
  结果（发现数、耗时、是否在跑）。同一后端同一时刻只允许一个 job。`s3adm fsck
  --offline <backend>` 包装这两个端点（现有在线 `s3adm fsck <bucket>` 语义不变）。
- **入口**：`src/s3/service.cc` 的 admin 路由表、`src/storage/*/run_scrub_once`、
  `src/tools/s3adm_fsck.cc`；限速沿用 `scrub_throttle.h`。
- **验收**：e2e 起 localfs 网关触发一轮、轮询到完成、篡改一个对象后再跑能报出
  发现；并发第二次触发返回 409；[cli.md §3.5](cli.md)、[storage/localfs.md §11](storage/localfs.md)。

## 2. 阶段 B：多实例一致性（合计约 1.5 周）

### ④ STS 会话表多实例共享

- **范围**：会话记录从 `CredentialStore::sessions_`（进程内存）改为写穿到默认后端
  `.sys/sts/<session-ak>`（含 token 哈希、TTL、继承的 policy、发起者），本实例保留
  内存缓存；其他实例在数据面首次碰到未知 session AK 时按需回源读一次（负缓存
  60s，避免暴力枚举打穿后端），`auth.sync_interval` 的周期增量同步顺带拉取新会话与
  删除过期条目。过期由各实例惰性清理 + 周期扫描。
- **入口**：`src/s3/auth/credential_store.{h,cc}`（`sessions_` 与同步任务）、
  `src/s3/handlers/sts.cc`；持久化格式沿用凭证对象的 JSON 与 `rev` 计数。
- **验收**：单测——实例 A 签发、实例 B 验证通过、过期后两边都拒绝；e2e 增"两网关
  共享 localfs root 的 STS"段（复用 cloudproxy 双实例脚手架的写法）；
  [credential-management.md](credential-management.md) §STS 与 [s3-protocol.md](s3-protocol.md) 更新。

### ⑤ 跨网关元数据缓存失效协议

- **范围**：只做 redis 引擎：`RedisMetaStore` 在每次写提交后 `PUBLISH duo:inv
  <bucket>\0<key>`，各网关一条订阅连接把消息喂给本地 `MetaCache::invalidate`。
  订阅断线期间整表清空再重连（保守）。有失效协议后 redis 共享 meta 允许
  `meta_cache_ttl` 放宽到 `gc_grace` 之内的更大值，默认仍关。tikv 无发布订阅，
  维持 TTL 有界陈旧契约，文档明写。
- **入口**：`src/storage/duostore/redis_meta_store.cc`（发布点 = 各 commit 之后）、
  `src/storage/meta_cache.h`（已有分片失效代与回填令牌，无需改）、
  `duostore_backend.cc` 的缓存接线与配置校验。
- **验收**：`test_duostore_redis.cc`（有实例才跑）——两个 backend 实例共享一个 redis，
  A 写 B 读命中新值；订阅连接被 kill 后 B 清缓存并恢复；e2e `duostore-redis`
  段增两实例用例；[storage/duostore-core.md §7.1](storage/duostore-core.md)、
  [duostore-redis-meta.md](duostore-redis-meta.md)。

## 3. 阶段 C：身份（约 2–3 天）

### ⑥ mTLS 客户端证书 → 凭证 / 租户身份映射

- **范围**：`auth.tls_identity: off|subject-cn|san-uri`。开启后，`tls_client_auth:
  require` 验证通过的连接把证书主体（CN 或首个 URI SAN）作为身份候选：请求**未签名**
  时按 `.sys/tls-identities/<subject>` 映射到某个凭证（继承其 policy / tenant /
  role）视同该凭证签名；请求已签名时证书身份必须与签名凭证同租户，否则 403。
  映射由 root 经 `/-/admin/tls-identities` 与 `s3adm cred bind-cert` 维护。
- **入口**：`src/http/tls.h`（握手后取 peer 证书主体，四驱动把它放进
  `HttpRequest.remote_addr` 旁的新字段 `tls_identity`）、`src/s3/service.cc` 验签前
  的分支、`credential_store` 的映射表（`SysConfigStore` 模板直接可用）。
- **验收**：`test_tls.cc` + `test_service.cc`：未签名 + 有效证书通过、无证书 401、
  证书租户与签名租户不符 403；e2e 用 openssl 生成客户端证书跑一遍 builtin/beast；
  [tls.md](tls.md) §mTLS、[multi-tenancy.md](multi-tenancy.md)。

## 4. 阶段 D：运维纵深（合计约 2 周，可拆）

### ⑦ 后端实例增删热重载

- **范围**：`reload_config` 允许 `backends[]` 新增条目与删除**未被任何路由规则和
  `default_backend` 引用**的条目；已有条目的参数变更仍需重启。新增：按配置构建 →
  `meter_backends` 包装 → `BucketRouter::update` 原子换代；删除：先从路由表摘除，
  等在途请求排空（复用 `AsyncSemaphore::wait_drained` 思路，每后端一个在途计数），
  再 `close()`。tiered 的 local/cloud 引用与 cloudproxy 连接池在删除路径上要正确关闭。
- **入口**：`src/app/app.cc` `reload_config` / `diff_config`、`storage/registry.h`、
  `storage/bucket_router.h`、`storage/metered_backend.h`。
- **验收**：`test_reload.cc`：增一个 memory 后端并路由过去、删掉它、删被引用的返回
  `requires_restart`；e2e 在流式 GET 进行中删后端，请求完成后才 close；
  [config-reload.md](config-reload.md) 的可热更新矩阵更新。

### ⑧ duostore meta 增量备份 / PITR

- **范围**：按引擎分四个小迭代，先易后难：sqlite（WAL 归档 + `dump --since`
  用 checkpoint 序号）→ rocksdb（`BackupEngine` 增量 + `CreateCheckpoint`）→
  redis（依赖 AOF 归档，网关侧只做 `dump --since <offset>` 的一致性说明）→ tikv
  （BR/CDC 属集群侧，网关只导出"当前 TSO"作恢复点）。统一 CLI：`lights3 duostore
  backup <backend> --incremental --to <dir>`、`restore --to-ts`。
- **入口**：`src/storage/duostore/*_meta_store.cc` 的 `snapshot()` 旁增 `export_since`、
  `src/main.cc` duostore 子命令、[storage/duostore-core.md §11](storage/duostore-core.md)。
- **验收**：每引擎一条"全量 + 两次增量 + 恢复到中间点"的单测（有实例才跑的按既有
  SKIP 约定）；对应 meta 文档各加一节。

## 5. 阶段 E：机会项

### ⑨ client-c 结构化错误码上游贡献

- **范围**：把 `tikv_client` sidecar 里"kvrpcpb 结构化冲突分类 + 字符串匹配纵深"的
  分类逻辑整理成上游 PR（`third_party/client-c`），让 `Backoffer`/`RegionCache`
  抛带枚举码的异常。合入后本仓删掉字符串匹配分支。
- **验收**：上游合入 + 子模块指针更新 + `test_duostore_tikv.cc` 的冲突用例仍过。

### ⑩ `HeaderMap` 线性扫描 / `BlockQueue` 双拷贝

- **触发条件**：`perf` 火焰图里 `HeaderMap::find` 或 `BlockQueue::push/pop` 合计
  超过请求 CPU 的 2%。没有这个证据就不动。
- **若做**：`HeaderMap` 保持 vector 但加小写键的 8 位哈希预筛；`BlockQueue::push`
  接受 `std::string&&` 让 httplib 的 content receiver 直接移交。

## 6. 记账

| 序 | 条目 | 状态 | 日期 / 分支 |
| --- | --- | --- | --- |
| ① | tiered 本地层已用字节 gauge | 已完成 | 2026-09-05 / `feat/tiered-local-usage-gauge` |
| ② | 独立 admin 端口 | 未开始 | |
| ③ | scrub / fsck 的 admin 端点 | 未开始 | |
| ④ | STS 会话表多实例共享 | 未开始 | |
| ⑤ | 跨网关元数据缓存失效协议 | 未开始 | |
| ⑥ | mTLS 身份映射 | 未开始 | |
| ⑦ | 后端实例增删热重载 | 未开始 | |
| ⑧ | duostore meta 增量备份 / PITR | 未开始 | |
| ⑨ | client-c 上游贡献 | 未开始 | |
| ⑩ | HeaderMap / BlockQueue | 等 profile 证据 | |
