# 步骤 ⑤：多网关验证、误配防线与文档转正

> 状态：**未实现**（实施稿 2026-09-11）。对应设计 §5.5、§14 ⑤。依赖 ①–④。
> 完成后：两个网关共享同一份目录状态时提交冲突收敛、rename 恢复可由对端驱动；
> 非共享默认后端上启用 tables 会得到明确 WARN；设计文档转为实现文档并同步英文版。

## 1. 目标与验收

| 验收项 | 判据 |
| --- | --- |
| 双网关单测 | `multi_gateway_suite.h` 新用例：两个 `S3Service`（各自的 `Catalog` / `ObjectCatalogStore`）共享一个后端，并发 commit 恰一胜；A 上 rename 中断由 B 的写操作恢复 |
| redis/tikv 变体 | 同用例在 `build-redis` / `build-tikv`（duostore 共享 meta）下通过；无集群时 SKIP |
| 误配 | `tables.enabled: true` + 默认后端 localfs/xlocalfs/rocksdb/sqlite + 任一多网关信号（`read_lease > 0` 或 `usage.reconcile: false` 或 `gc_enabled: false`）→ 启动 WARN 与 `--check-config` 同文 |
| 文档 | `deployment.md §5.1` 表加"表目录"列；`s3-tables-design.md` 顶部状态改为"已实现"，§14 表每行标日期；`docs/en/s3-tables-design.md` 同步；`docs/README.md` 描述更新 |

## 2. 双网关用例（`tests/unit/multi_gateway_suite.h`）

现有 suite 以"两个后端实例共享介质"参数化；追加一组 `tables_*` 用例，参数为
两个 `IStorageBackend`（同一介质）：

```cpp
template <class MakeBackends>   // 返回 pair<shared_ptr<IStorageBackend>, shared_ptr<IStorageBackend>>
void run_tables_multi_gateway_suite(MakeBackends make) {
    auto [b1, b2] = make();
    auto svc1 = make_tables_service(b1), svc2 = make_tables_service(b2);   // 各自 TableBucketStore / Catalog
    // 1 A 启用表桶、建 namespace/table；B 的 store sync_now 后可见（或直接读 .sys：ObjectCatalogStore 无缓存，TableBucketStore 需 sync）
    // 2 并发：A、B 各发 10 个 commit（同 expected token）→ 恰 1 成功，其余 409
    // 3 A 在 fault 点 "tables.rename.after_stage2" 中断 rename；B 的 create_table 触发 recover_renames → 终态：dst Active、src Deleted
    // 4 A drop，B load → 404
}
```

`MemoryBackend` 变体：两个 `MemoryBackend` 无法共享，用同一个实例作 b1 = b2（进程内两套
`Catalog` 已足够验证 CAS 收敛与恢复驱动）。redis / tikv 变体沿用现有 `duostore_*_multi_gateway_shared_meta`
的构造方式（同前缀两个 `DuoStoreBackend` + rados 或 fs data；fs data 场景目录状态在
`.sys` 里全是小对象，两网关各自的 fs data **不共享**，因此只在 rados data 或
`MemoryBackend` 上跑；文档注明这与 multi-gateway-multipart §2 的结论一致）。

**`TableBucketStore` 的跨实例可见性**：`SysConfigStore` 靠 `sync_interval` 拉取；
B 上对未同步的桶回 404 `NoSuchNamespaceException`——与凭证"写穿先于生效、SDK 重试"
同一模型，文档写明；用例里显式 `sync_now()`。

## 3. `--check-config` 误配 WARN（`config.cc` / `app.cc`）

在 `Application` 启动的既有多网关误配检查（multi-gateway-multipart §4 ④）旁追加：

```cpp
if (cfg_.tables.enabled) {
    auto& def = backend_config_of(cfg_.buckets.default_backend);
    bool shared = def.type == "cloudproxy" || (def.type == "duostore" && (def.meta == "redis" || def.meta == "tikv"));
    if (!shared && multi_gateway_signals(cfg_))
        LOG_WARN("tables: catalog state lives on default backend '{}' ({}) which is single-gateway; "
                 "multi-gateway signals present -- table commits will not converge across gateways "
                 "(docs/s3-tables-design.md §5.5)", def.name, def.type);
}
```

`--check-config` 输出同一句（`cli_server.cc` 的 `check_config` 调同一函数）。

## 4. deployment.md §5.1 矩阵

加一列"表目录（tables）"：cloudproxy ✔、duostore redis/tikv + rados ✔、
duostore redis/tikv + fs ✖（目录状态可共享但表数据不共享——与该行既有结论一致）、
其余 单网关。§5.2 加一段"表目录多网关：`tables.enabled` 各实例一致；
`auth.sync_interval` 决定表桶启用的可见延迟；`tables.maintenance.scan_interval`
只在一台开或全开"。

## 5. 文档转正

- `docs/s3-tables-design.md`：顶部状态 → "已实现（①–⑤ 日期）"；§14 表每行改
  "已实现 + 日期 + PR 号"；实现中偏离设计的点在对应章节改正文（不留划线）。
- `docs/en/s3-tables-design.md` 同步（章节编号不变）。
- `docs/s3-tables/step-N.md` 顶部状态改"已实现"；实现细节若与实施稿不同，改实施稿。
- `docs/README.md` 与 `docs/en/README.md` 的行去掉"（未实现）"。
- `docs/s3-protocol.md §1` 加 "Tables" 行指向设计文档；`docs/cli.md` 加 `tables`
  子命令段；`docs/testing.md §1/§2` 加 `tables` 标签与 e2e 段；`docs/config-reload.md §4`
  加 `tables.*`；`docs/monitoring.md` 加 tables 指标（若 dashboard 生成器加面板，先改
  `gen_dashboard.py` 再生成）。
- `docs/todo.md §4` 的 S3 Tables 行删除（做完即删），未做的 ⑥ 项若有价值则移入 §4 新行。

## 6. 验收脚本

`scripts/check-all.sh` 加 tables 段：`LIGHTS3_TEST_FILTER=tables unit_tests`、
`ctest -R e2e_.*` 已含 tables 段、`ctest -L tables-smoke`（默认 SKIP，`LIGHTS3_TABLES_SMOKE=1`
时跑 `scripts/tables/pyiceberg_smoke.py` 与 `duckdb_smoke.py`，脚本随 ① 的
冒烟一起入库，此步纳入 ctest）。

## 7. 陷阱

- `multi_gateway_suite.h` 的既有用例已很长，tables 用例单独放 `tables_multi_gateway_suite.h`
  由三个测试文件（memory / redis / tikv）包含，避免重复编译大文件。
- worktree 里勿跑 `submodule update`（仓库约定）；变体构建用增量 `cmake --build build-redis`。
- redis reconnect 类用例在普通 shell 下死于 SIGPIPE（既有），tables 用例不涉及断连，
  但同一进程里跑时注意 `LIGHTS3_TEST_FILTER`。
