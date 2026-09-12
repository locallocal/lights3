# 步骤 ⑥：可选项

> 状态：**已实现（2026-09-12，分支 feat/s3-tables-step6，全部五项）**。对应设计 §6.3 views 行、§9 末段、
> §12、§14 ⑥。各项独立；实现记录见 §7，设计文档对应章节已回写。

## 1. Iceberg views

价值：Trino / Spark 的 `CREATE VIEW` 走 REST catalog；PyIceberg 0.9 只读 view。
实现量小于表（无快照图）。

- `ViewEntry`：与 `TableEntry` 同形（`view_id` / `view_uuid` / `metadata_location` /
  `version_token` / `generation`，format-version 1），`.sys` 键
  `…/ns/<ns>/view/<name>.json`；保留目录 `<reserved>/<ns>/<name>/view-metadata/`。
  表与 view 同名互斥（create / rename 时各查一次）。
- REST：`GET/POST …/views`、`GET/HEAD/POST/DELETE …/views/{v}`、`POST /{w}/views/rename`；
  请求 `{"name","location?","schema","view-version":{"representations":[{"type":"sql","sql","dialect"}],"default-namespace","summary?"},"properties"}`；
  replace（POST）的 `requirements` 只认 `assert-view-uuid`，`updates` 认
  `assign-uuid|upgrade-format-version|add-schema|add-view-version|set-current-view-version|set-location|set-properties|remove-properties`。
- 错误类型 `NoSuchViewException`；`/config.endpoints` 含 view 端点。

## 2. `/_iceberg/v1` 别名

`tables.compat_prefix` 非空时 dispatch 两个前缀都匹配；`GET /config` 的
`defaults["lights3.catalog-compat-prefix"]`；CreateBucket 同时保留两个前缀的首段。
签名名在 ② 已放开；冒烟脚本用 `LIGHTS3_TABLES_PREFIX=/_iceberg` 切换。价值：MinIO AIStor
迁移用户的配置零改动。

## 3. `reportMetrics` 落审计

`POST …/tables/{t}/metrics` 解析 `{"report-type":"scan-report"|"commit-report", ...}` 写审计事件
`tables.metrics`（`detail` = 精简后的 JSON：`report-type`、`table-name`、`snapshot-id`、`filter`、
`projected-field-names`、`metrics` 各计数 / 计时、`metadata`）。上限 64 KiB，超出或不是 JSON
仍 204 但不记录。

## 4. compaction 候选规划输出

`plan` 报告的 `compaction-candidates`：读当前快照的 manifest（③ 的遍历），把
`content == 0`、`size_bytes ≤ small_file_ratio × target` 的数据文件按
`(桶内目录前缀, sort_order_id)` 分组，组内按 `target_file_size_bytes` 首次适应递减装箱，
输出 `[{"partition","sort-order-id","files":[…],"bytes","row-level-required"}]`（≥ 2 个文件的箱才
输出）；不执行重写。目标大小取表属性 `write.target-file-size-bytes`，默认 512 MiB，
`small_file_ratio` 0.75；有 delete 文件的分区标 `row-level-required`。

## 5. duostore-meta 后备（设计 §12）

`DuoMetaCatalogStore : ITableCatalogStore`，条件：默认后端是 duostore，
`tables.catalog_backing: duostore`。

- `IMetaStore` 通用 KV 面：`kv_get / kv_put(key, value, PutCondition) / kv_delete / kv_scan(prefix, after, limit) /
  kv_put_batch`（rocksdb 新 column family `tc`、sqlite 表 `tc`、redis hash `tc` + etag hash `tce` +
  lex zset `tcz`、tikv 键标签 `T`）；etag = `sha256(value)` 前 16 字节 hex，由引擎算并在自己的
  原子区内比对（`check_kv_condition`）。
- 键布局与 `ObjectCatalogStore` 完全相同，因此 `lights3 tables export|import` 在两种后备之间
  逐字节搬运；提交的记录与指针在 `commit_atomic` 一批落地（`kv_put_batch`），无 STAGED 记录、
  无 finalization gap。
- 单测：`catalog_store_suite.h` 对 ObjectCatalogStore(memory) 与 DuoMetaCatalogStore(rocksdb /
  sqlite / redis / tikv) 跑同一组用例；`meta_store_suite.h` 的 `case_kv_facade` 对四个引擎跑 KV 面。

## 6. 不做（重申设计 §15）

多表事务、`/plan` `/tasks` `/sign`、compaction 执行、durable-strong 单快照、Delta/Hudi、
跨区域双活写。

## 7. 实现记录（2026-09-12）

- **views**：`iceberg/view_metadata.{h,cc}`（初始元数据、校验、requirements / updates）；
  `Catalog::create_view / load_view / view_exists / list_views / replace_view / rename_view / drop_view`；
  rename 是"先写目标、再把源改墓碑"两步（无 intent：view 没有在途读者要 fence）；replace 复用表提交
  的骨架但不写 commit 记录（view 无快照图，诊断只针对表）。`ObjectCatalogStore` /
  `DuoMetaCatalogStore` 的 `namespace_has_children` / `bucket_state_empty` / 子 namespace 列表都认得
  `view/` 目录；fsck 的对账不覆盖 view。view 的 `location` 默认 `s3://<bucket>/<ns>/<name>`。
- **别名**：`RestApi::matched_prefix` 决定 dispatch 跳过的段数；`TableBucketGuard` 在 ① 就已保留
  两个前缀的首段。
- **reportMetrics**：`RestApi::report_metrics` 解析后经 `Hooks::audit` 记 `tables.metrics`。
- **compaction**：`iceberg::live_files_of_snapshot`（当前快照的活跃条目）；`DataFile` 多了
  `sort_order_id`；分区键用桶内目录（不是 URI）。
- **duostore-meta**：`kv_*` 加在 `IMetaStore` 上带默认 `NotImplemented`（测试替身不受影响）；
  `DuoStoreBackend::meta()` 暴露引擎；`app.cc` 取**原始**后端实例（不是计量装饰器）做
  `dynamic_cast`；`Catalog::commit_table` 按 `store_->supports_atomic_commit()` 跳过 STAGED 写与
  两个 `tables.commit.*` 故障点。`--check-config` 对 `catalog_backing: duostore` + 非 duostore
  默认后端报错。tikv 变体只做了编译检查（无集群）。`DuoMetaCatalogStore` 的 KV 调用在调用线程
  同步执行（redis / tikv 是网络往返；与 DuoStoreBackend 在池线程上跑 meta 的做法不同，
  目录写路径本就串行且量小）。
- **文档**：设计文档中英文 §6.3 / §9 / §10 / §12 / §14 回写；cli.md §2.6；s3-protocol；testing；
  README；todo 删去 ⑥ 行。
