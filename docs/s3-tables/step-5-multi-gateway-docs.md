# 步骤 ⑤：多网关验证、误配防线与文档转正

> 状态：**已实现（2026-09-12，分支 feat/s3-tables-step5）**。对应设计 §5.5、§14 ⑤。依赖 ①–④。
> 实现与本稿的差异见文末 §8。
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

## 8. 实现记录（2026-09-12）

- **双网关用例**放在 `tests/unit/tables_multi_gateway_suite.h`（§7 的建议），入参是两个
  `IStorageBackend` 句柄；用例直接在 `Catalog` 层跑（各自 `TableBucketStore` /
  `ObjectCatalogStore` / `Catalog`），不再各起一个 `S3Service`——收敛与恢复都在目录层决定，
  REST 面另有覆盖。接入点：`test_tables_multi_gateway.cc`（memory，同一实例作两个句柄）、
  `test_duostore_redis.cc` / `test_duostore_tikv.cc`（`multi_gateway_suite::make_cluster` 的
  两个 `DuoStoreBackend`：共享 meta + 一个 `SharedDataStore`，`#ifdef LIGHTS3_TABLES`，无实例
  SKIP）。步骤 ③ 的中断点用 `tables.rename.after_destination`（稿子写的
  `after_stage2` 不存在）；触发恢复的是 B 对**被 fence 的表**的写（writer 路径强制先
  `recover_renames`），而不是任意 `create_table`（无关写只每 32 次调用扫一遍，用例里不确定）。
  memory 与 redis 变体本机通过；tikv 变体无集群 SKIP。
- **误配 WARN**：纯函数 `tables_deployment_warning(const Config&)`（`core/config.{h,cc}`），
  `Application::start_server` 与 `--check-config`（`cli_server.cc`）各调一次。"多网关信号"按
  **显式配置**判定：任一 duostore 后端 `read_lease` 显式 > 0、`gc_enabled: false`、或
  `usage.reconcile: false`；`read_lease` 的默认值 5s 不算（否则每个 tables + localfs 的
  单机配置都会被误报）。单测 `config_tables_deployment_warning`。
- **SigV4 兼容修正**（冒烟时发现）：PyIceberg / Spark `rest.sigv4-enabled` 这类通用 SigV4
  客户端把 payload 哈希算进 canonical request 但**不发 `x-amz-content-sha256` 头**，S3 面的
  校验器把它当必需头 → 400。`RestApi::dispatch` 对无该头、非 presigned 的目录请求先把 body
  读满（≤ `tables.request_max_size`）、算 sha256 填进该头再验签（客户端签的就是这个值）。
  管理面 `/-/admin/...` 不变（`lights3-ctl` 一直带头）。
- **冒烟脚本**入库并挂 ctest：`scripts/tables/pyiceberg_smoke.py`（PyIceberg 走 boto3 默认
  凭证链，脚本把 `LIGHTS3_AK/SK` 放进 `AWS_ACCESS_KEY_ID/SECRET`；管理面调用用 botocore
  自签）、`duckdb_smoke.py`（DuckDB 1.5.5 的 iceberg 扩展 ATTACH 须给
  `SIGV4_REGION` + `SIGV4_SERVICE 's3'`，否则它试图从主机名解析 region / service）、
  `tests/e2e/run_tables_smoke.sh`（起 memory 网关，`LIGHTS3_SMOKE_KEEP=1` 先留表给 DuckDB
  再 purge）。ctest `tables_smoke`（标签 `tables-smoke`，`SKIP_RETURN_CODE 77`），
  `check-all.sh --with-tables-smoke` 置 `LIGHTS3_TABLES_SMOKE=1`。本机结果：PyIceberg 0.12.0
  16/16、DuckDB 1.5.5 5/5（依赖装在临时目录，`PYTHONPATH` 指过去）。
- **监控资产**：`lights3.rules.yml` 加 `lights3.tables` 组 4 条告警，dashboard 加
  "S3 Tables" 行 5 面板（`gen_dashboard.py` 生成），`check_assets.py` 把 `lights3_tables_`
  列为特性相关（memory 网关不开 tables 时不在 `/-/metrics` 里）。
- **文档**：deployment §5.1 加"表目录"列与 §5.2 段；设计文档中英文顶部状态改"已实现"、
  §14 各行标 PR 号、§5.5 / §13 回写；`docs/README.md` 与 `docs/en/README.md` 去掉"未实现"；
  testing.md §1 加 `tables_smoke` 行、§6 记客户端版本；monitoring.md 加 tables 组 / 行；
  todo §4 的 S3 Tables 行改为 ⑥ 可选项。`docs/s3-protocol.md`、`docs/cli.md`、
  `docs/config-reload.md` 在 ①–④ 已改，本步只改状态措辞。
