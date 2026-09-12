# 步骤 ④：维护（plan / run / purge）与 CLI

> 状态：**已实现（2026-09-12，分支 feat/s3-tables-step4）**。对应设计 §5.7、§9、§14 ④。依赖 ①③。
> 实现与本稿的差异见文末 §10。
> 完成后：每张表可做元数据文件保留、快照过期、孤儿清理的规划与执行；drop 支持
> `purgeRequested=true`；可选周期 runner；`lights3-ctl tables …` 全套子命令；
> 墓碑按 `tombstone_ttl` 清理。

## 1. 目标与验收

| 验收项 | 判据 |
| --- | --- |
| plan | 对有 5 个 metadata 文件、`retain_recent_metadata_files: 2` 的表，报告候选 = 最旧的、不在 metadata-log、且 mtime 早于安全窗口的文件 |
| run | plan 后指针变化 → `StalePlan` 失败，不删任何文件；`delete_enabled: false` 时只提交快照过期；`true` 时删候选并复核 mtime |
| 快照过期 | `max_snapshot_age_ms` 之外且非 ref 引用的快照被 `remove-snapshots` 提交移除，`generation +1`，commit log 有记录 |
| 孤儿 | `location/data/` 下未被任何保留 metadata 可达的文件被删；manifest 解析失败时零删除 |
| purge | `DELETE …/tables/{t}?purgeRequested=true` → 204 + 作业；作业结束后保留目录与 `location` 前缀为空、墓碑删除 |
| 周期 | `tables.maintenance.scan_interval: 1s` 的单测：runner 一轮后状态与手动 plan+run 相同 |
| CLI | `lights3-ctl tables enable|disable|status|list|plan|run|purge|diagnose|recover` 退出码与 JSON 输出符合 `docs/cli.md` 约定 |

## 2. 文件清单

```text
src/tables/maintenance.{h,cc}        新增：Planner / Runner / PurgeJob
src/tables/maintenance_runner.{h,cc} 新增：周期 runner（LifecycleRunner 同款）
src/app/admin_jobs.{h,cc}            扩展：资源级作业（§5）
src/s3/service.cc                    /-/admin/tables/… 分支
src/tables/rest_api.cc               …/maintenance/{config,plan,run,jobs/{id}}、purgeRequested
src/tools/lights3_ctl_tables.{h,cc}  新增子命令
src/cli/cli_common.cc                lights3 主程序无新子命令（离线维护不做：目录状态需要在线 CAS）
tests/unit/test_tables_maintenance.cc
docs/cli.md                          tables 子命令段
```

## 3. 维护配置

表级配置对象 `.sys/tables-catalog/<bucket>/ns/<ns>/tbl/<t>.maint.json`（与指针并列，
`ObjectCatalogStore` 加 `get/put_maintenance_config`）：

```json
{"version":1,"retain_recent_metadata_files":10,"delete_enabled":false,
 "max_snapshot_age_ms":432000000,"min_snapshots_to_keep":1,"orphan_cleanup":true}
```

解析顺序：表级对象 → `tables.maintenance` 配置默认值。表属性
`history.expire.max-snapshot-age-ms` / `history.expire.min-snapshots-to-keep`
（Iceberg 标准属性）优先于两者；表属性与表级对象都设置且不同 → plan 标
`manual_review = true`。REST：`GET/PUT …/tables/{t}/maintenance/config`（Read / Write）。

## 4. Planner（`maintenance.h`）

```cpp
struct MaintenancePlan {
    std::string version_token;                       // plan 绑定的指针
    std::vector<std::string> metadata_candidates;    // 保留目录下可删的 metadata 文件 key
    std::vector<int64_t> expire_snapshots;           // 要 remove-snapshots 的 id
    nlohmann::json expire_updates, expire_requirements;   // 直接可提交的 updates/requirements
    std::vector<std::string> orphan_candidates;      // location 下可删的数据/删除文件 key
    bool manual_review = false; std::vector<std::string> notes;
    int64_t planned_unix;
};
struct PlannerOptions { int retain_recent; int safety_window_sec; std::optional<int64_t> max_snapshot_age_ms; std::optional<int> min_snapshots_to_keep; bool orphan_cleanup; DeepCheckOptions deep; };
Task<MaintenancePlan> plan_table(Catalog&, ITableCatalogStore&, storage::IStorageBackend& bucket_backend, bucket, levels, name, const PlannerOptions&);
```

流程：

```text
1 (entry, etag) = get_table；Active 否则 400；cur = 读当前 metadata
2 元数据保留集 R：
   - entry.metadata_location
   - cur["metadata-log"][].metadata-file
   - 保留目录 metadata/ 按文件名序号最近 retain_recent 个（列举 prefix "<reserved>/<ns>/<t>/metadata/"）
   - 诊断（③）中每条 COMMITTED 记录的 new_metadata_location
   candidates = 目录下 ∉ R 且 last_modified ≤ now − safety_window 的 *.metadata.json
3 快照过期（有 age 或 keep 配置时）：
   watermark = now − max_snapshot_age_ms；按 timestamp-ms 降序，保留 min_snapshots_to_keep 个；
   保留 current-snapshot-id、任一 refs[*].snapshot-id、以及作为保留快照 parent 链上的祖先（Iceberg 语义允许删中间快照，但为简单保留链）；
   refs 里非 main 的 ref 带 max-ref-age-ms / min-snapshots-to-keep 字段 → manual_review
   expire_updates = [{"action":"remove-snapshots","snapshot-ids":[...]}]
   expire_requirements = [{"type":"assert-ref-snapshot-id","ref":"main","snapshot-id":<current>}]
4 孤儿（orphan_cleanup）：
   reach = 从 R 中每个 metadata 的每个快照出发，走 ③ 的 manifest-list → manifest → 文件路径（复用 parse_*；UnsupportedCodec 或任何解析失败 → manual_review，orphan_candidates 清空）
   listed = 列举 "<location 相对前缀>/data/" 与 ".../delete/"（若表 location 在保留前缀之外还要列保留目录下的 data/——本设计 data 不在保留目录，跳过）
   orphan_candidates = listed ∖ reach，且 last_modified ≤ now − safety_window
5 plan.version_token = entry.version_token
```

复杂度：`reach` 对每个保留 metadata 的每个快照都读 manifest；对 1000 快照的表这是
上万次对象读——`PlannerOptions.deep.max_manifests` 是上限，超过即 `manual_review`
并提示先缩小 `retain_recent` / 先过期快照。

## 5. Runner 与作业框架

`AdminJobs` 现在按后端名 + `JobOp` 建槽位并调 `run_job(op, backend, bps)`。扩展为
资源级自定义作业（不改既有语义）：

```cpp
// admin_jobs.h
enum class JobOp { Fsck, DuoGc, DuoScan, TierScan, TierGc, TierReconcile, TablePlan, TableRun, TablePurge };
// 资源键任意字符串（"tables:<bucket>/<ns-path>/<t>"），fn 在专用线程里同步执行（内部 sync_wait 协程）
uint64_t start_custom(const std::string& resource, JobOp op, std::function<JobOutcome()> fn);
nlohmann::json status(const std::string& resource, JobOp op) const;   // 既有签名复用：resource 即 backend 名
```

`job_group_name(TablePlan|TableRun|TablePurge) = "tables"`，`job_op_name = "plan"|"run"|"purge"`；
`parse_job_op("tables", ...)`。`JobOutcome.stats` 放 plan JSON（plan）/ 删除计数（run）。
"每表同一时刻一个作业" = `AdminJobs` 既有的每资源单作业规则（`JobInProgress` 409）。

`run_table(plan)`：

```text
1 (entry, etag) = get_table；entry.version_token != plan.version_token → 抛 S3Error(InvalidRequest, "StalePlan: table changed since planning")
2 若 plan.expire_snapshots 非空：Catalog::commit_table(bucket, levels, name, {commit_id="maint-"+uuid, plan.expire_requirements, plan.expire_updates})
   → 409 也视为 StalePlan（并发写者）；成功后 entry 更新，plan.version_token 失效——因此顺序是先提交快照过期，再删文件时**重新 head 复核**而不再比对 token
3 若 delete_enabled：
   for k in metadata_candidates ∪ orphan_candidates：head_object；仍存在且 last_modified ≤ now − safety_window 且 k 不是当前指针 → delete_object（限速：每删 N 个 co_await pool->schedule() 让出）
   删前再读一次指针：metadata_location == k → 跳过（极端并发下保护）
4 usage：每删一个 note_usage(bucket, -1, -size)（经 hook）
5 JobOutcome{stats: {deleted_metadata, deleted_orphans, expired_snapshots, skipped}}
```

`purge_table`（drop 时 `purgeRequested=true`）：

```text
1 drop_table（墓碑）——已由 REST 层做完，作业只清数据
2 列举并删除 保留目录 "<reserved>/<ns>/<t>/" 全部对象；列举并删除 location 相对前缀下全部对象（分页，每页 delete）
3 删 commits/<table_id>/*、<t>.maint.json、墓碑 tbl/<t>.json
```

REST：`POST …/tables/{t}/maintenance/plan` → 202 `{"job_id"}`（Write）；
`POST …/maintenance/run` `{"plan": <plan JSON>}` 或 `{"job_id": <plan 作业 id>}`（Write）；
`GET …/maintenance/jobs/{id}` → AdminJobs status JSON；`DELETE …/tables/{t}?purgeRequested=true`
→ 先墓碑再 `start_custom(TablePurge)`，204 + 头 `x-lights3-job-id`。管理面
`POST /-/admin/tables/<bucket>/<ns-path>/<t>/<plan|run|purge>`（root）走同一 `job_start_` hook（group `tables`，
resource 由路径拼出），`service.cc` 的 `/-/admin/duostore|tier` 分支加 `tables`。

## 6. 周期 runner（`maintenance_runner.h`）

```cpp
class MaintenanceRunner {   // LifecycleRunner 同款骨架：TimerQueue + BackgroundTaskGroup + scan_tick 不重叠
public:
    MaintenanceRunner(std::shared_ptr<Catalog>, std::shared_ptr<ITableCatalogStore>, std::shared_ptr<TableBucketStore>, storage::BucketRouter, TablesConfig::Maintenance);
    Task<PassStats> run_once();          // 每个表桶 → 每个 namespace → 每张 Active 表：plan → run；墓碑（Deleted 且 updated_unix + tombstone_ttl < now）→ 删条目；过期 Renaming intent → 交给 ③ 的 recover_renames
    void start_background(std::shared_ptr<ThreadPool>, int scan_interval_sec);
    void shutdown_background();
};
```

多网关：与 lifecycle 同策略（各实例都跑，删除幂等，安全窗口保护在途提交），或只在一台
配置 `scan_interval > 0`；文档写明。每张表的 plan+run 经 `AdminJobs::start_custom`
排队，与手动作业互斥（`Busy` 则跳过本轮）。

## 7. CLI（`lights3_ctl_tables.cc`）

ccmd 子命令 `tables`（仿 `lights3_ctl_quota.cc` 的 HTTP + JSON 输出）：

| 子命令 | 请求 |
| --- | --- |
| `enable <bucket>` / `disable <bucket>` / `status <bucket>` | PUT / DELETE / GET `<prefix>/v1/buckets/<bucket>` |
| `list <bucket> [--namespace=a.b]` | GET namespaces / tables，表格输出 |
| `plan <bucket> <ns.table>` | POST `/-/admin/tables/<bucket>/<ns-path>/<t>/plan`，随后轮询 status 直到完成，打印 plan |
| `run <bucket> <ns.table> [--plan-job=<id>] [--yes]` | `delete_enabled` 为真时无 `--yes` 拒绝执行（本地防呆） |
| `purge <bucket> <ns.table> --yes` | DELETE `…?purgeRequested=true` |
| `diagnose` / `recover <bucket> <ns.table> [--prune]` | ③ 的端点 |

通用选项沿用 `lights3_ctl_common.h`（`--endpoint` `--access-key` `--secret-key` `--json`）。
签名 service 固定 `s3`。退出码：0 成功、1 请求失败、2 用法错误（`docs/cli.md` 既有约定）。

## 8. 单测清单（`test_tables_maintenance.cc`）

- 保留集：构造 5 个 metadata（用真实 commit 产生）+ 手工旧文件，`retain_recent=2`，
  断言候选集合；把 `last_modified` 用 `MemoryBackend` 的测试 hook 回拨（若无则新增
  `set_mtime_for_tests`）验证安全窗口。
- 快照过期：3 个快照 + 一个 tag ref 指向中间快照 → 只过期最旧；带 `max-ref-age-ms` 的 ref → `manual_review`。
- 孤儿：`data/` 下放 3 个文件，manifest 只引用 2 个 → 候选 1；manifest 损坏 → 候选空 + `manual_review`。
- run：StalePlan；`delete_enabled=false` 只提交；`true` 删除后 head 404；删前指针换到候选文件（人为）→ 跳过。
- purge：作业后前缀为空、墓碑不存在、`list_tables` 不含。
- runner：`scan_interval=1` 跑一轮 == 手动；`Busy` 时跳过不报错。
- AdminJobs：`start_custom` 与既有 `start` 在同一资源上互斥；status JSON 形态。

## 9. 陷阱

- **安全窗口以对象 `last_modified` 为准**：cloudproxy 的远端 mtime 与本机时钟可能偏差，
  `safety_window` 默认 15 min 已含余量；文档提示。
- **快照过期后 metadata-log 仍引用旧 metadata**：这是 Iceberg 语义（time travel 元数据），
  由 `metadata_log_keep` 裁剪后才成为候选。
- **`remove-snapshots` 与引擎并发写**：走标准 commit，冲突即 409 → StalePlan，重来即可。
- **purge 与仍在读的引擎**：Iceberg 无读锁，purge 是运维决定；CLI 必须 `--yes`。
- **AdminJobs 线程里跑协程**：`sync_wait` 顶层驱动（仓库约定：`Started::wait` 必须走 drive）。

## 10. 实现记录（2026-09-12）

- **维护配置对象**放在 `tables-catalog/<bucket>/maint/<ns-path>/<t>.json`（`ObjectCatalogStore::maint_key`），
  不与指针并列：`tbl/` 下多出 `<t>.maint.json` 会被 `list_tables` / `namespace_has_children` /
  fsck 当成表名。五个字段全部可选（`MaintenanceConfig`），未设即回落；`GET …/maintenance/config`
  返回 `{"effective": {...}, "table-config": <对象或 null>, "defaults": {...}}`，`PUT` 整体替换，
  键名与类型校验（400）。解析顺序按 §3：Iceberg 属性 `history.expire.*` > 表对象 >
  `tables.maintenance`；属性与表对象不一致 → `effective.conflict = true`，plan 标 `manual_review`。
  **快照过期只在配置了 `max_snapshot_age_ms` 时做**（无默认 5 天）；`min_snapshots_to_keep`
  默认 1。`purge` 时删配置对象；`drop`（不 purge）保留。
- **Planner**（`tables/maintenance.{h,cc}`）：签名简化为 `plan_table(Catalog&, bucket, levels, name,
  PlannerOptions, now_unix)`（store / backend 经 `Catalog::store()` / `bucket_backend()` 取）。
  保留集按 §4 第 2 步；注意每条 **COMMITTED 记录**都把它的 `new_metadata_location` 留在保留集里，
  因此正常提交产生的 metadata 永远不会成为候选，候选只会是"无记录"的文件（建表的 gen 1、
  crash 后 prune 掉记录的、手工放入的）——这是设计 §9 第 1 条的字面结果，测试按此断言。
  非 Active 表由 `load_table` 报 404 / 503（不是稿子里的 400）。快照过期的 requirements：有
  `main` ref 用 `assert-ref-snapshot-id`，否则 `assert-table-uuid`。孤儿可达集用
  `iceberg::reachable_files`（③ 的遍历，任何 status 的条目都算可达；manifest 上限、解析失败、
  codec 不可读一律 `manual_review` + 候选清空）。
- **Runner**：`run_table(Catalog&, plan, RunOptions, now_unix)`；StalePlan 抛
  `S3Error(InvalidRequest, "StalePlan: …")`（作业文档 `error` 字段可见）。过期提交的 commit-id 为
  `maint-<uuid>`。删文件前逐个 HEAD 复核 mtime，metadata 候选另读一次指针；每 64 个删除
  `co_await pool->schedule()`。`purge_table` 要求墓碑（否则 InvalidRequest）。
- **作业框架**：`AdminJobs` 加 `JobOp::TablePlan|TableRun|TablePurge`（组 `tables`）、
  `start_custom(resource, op, fn)`、`status_by_id(id)`；`status()` 对 `tables` 组不再要求资源是
  后端名；`start` / `start_custom` 共用 `launch`。目录侧只见 `tables/jobs.h` 的 `JobHooks`
  （start / status / status_by_id），由 `app.cc` 用 `AdminJobs` 实现并注入 `RestApi::set_job_hooks`
  与 `MaintenanceRunner::set_job_hooks`——tables 层不依赖 `app/`。作业的 `fn` 在作业线程里
  `sync_wait` 协程，结果放 `JobOutcome{kind:"tables", stats}`；抛异常即 `error`。
- **REST**：`GET/PUT …/maintenance/config`、`POST …/maintenance/plan`（202）、
  `POST …/maintenance/run`（body `{"plan":{…}}` / `{"job_id":N}` / 空 = 该表最近一次 plan 作业；
  `manual-review` 的 plan 400）、`GET …/maintenance/jobs/{id}`（须属于该表，否则 404）；
  `DELETE …?purgeRequested=true` → 204 + `x-lights3-job-id`（无作业框架时 406）。CommitTable 响应
  与 plan 文档都是 JSON；plan 作业的 `stats` 即 plan 文档，外加 `effective`。管理面
  `POST/GET /-/admin/tables/<bucket>/<ns 各级>/<t>/<plan|run|purge>`（root，`service.cc` 新分支
  → `RestApi::admin_job`），审计 `tables.<op>.start`。
- **周期 runner**（`tables/maintenance_runner.{h,cc}`）：构造只要 `Catalog` 与 `TablesConfig`；
  装了 `JobHooks` 时每张表的 plan+run 作为 `run` 作业排队，**pass 逐表等待作业结束**（用
  `std::future` 在 pass 所在的池线程上阻塞，同一时刻只有一张表在跑），`JobInProgress` → 跳过计
  `skipped_busy`；`manual_review` 的表只出 plan 不 run。墓碑清理按 `tombstone_ttl`（≤ 0 关），
  每桶先 `recover_renames`。`tables.maintenance.scan_interval > 0` 时 `app.cc` 启动。
- **CLI** `lights3-ctl tables`：`enable|disable|status|list|config|plan|run|purge|diagnose|recover`，
  比稿子多 `config`（run 的 `--yes` 防呆要先读 `effective.delete_enabled`）。`--catalog-prefix`
  默认 `/iceberg`；`run --plan-job=<id>`；`purge --yes` 必填并轮询 `…/maintenance/jobs/<id>`。
- **测试**：`test_tables_maintenance.cc`（保留集、快照过期 + 真实 `remove-snapshots` 提交、
  孤儿与 fail-closed、StalePlan / 删除门 / 安全窗口复核 / 指针换到候选、purge、runner 经作业框架
  跳过忙表 + 墓碑 + 后台 tick）、`test_tables_rest.cc` 追加端点 / 管理面 / purge 一例；e2e 段加
  settings / plan / run / 管理面 / `lights3-ctl tables` 全套；`MemoryBackend::set_mtime_for_tests`
  新增。
