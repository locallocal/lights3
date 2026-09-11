# 步骤 ⑥：可选项

> 状态：**未实现，按需**（实施稿 2026-09-11）。对应设计 §6.3 views 行、§9 末段、
> §12、§14 ⑥。每项独立，无相互依赖；做哪项由需求决定，做完把该项从本文删除并
> 在设计文档对应章节写明。

## 1. Iceberg views

价值：Trino / Spark 的 `CREATE VIEW` 走 REST catalog；PyIceberg 0.9 只读 view。
实现量小于表（无快照图）。

- `ViewEntry`：与 `TableEntry` 同形（`view_id` / `view_uuid` / `metadata_location` /
  `version_token` / `generation`，`format_version = 1`），`.sys` 键
  `…/ns/<ns>/view/<name>.json`；保留目录 `<reserved>/<ns>/<name>/view-metadata/`。
  表与 view 同名互斥（create 时各查一次）。
- REST：`GET/POST …/views`、`GET/HEAD/POST/DELETE …/views/{v}`、`POST /{w}/views/rename`；
  请求 `{"name","location?","schema","view-version":{"version-id","schema-id","timestamp-ms","summary","representations":[{"type":"sql","sql","dialect"}],"default-namespace"},"properties"}`；
  replace（POST）的 `requirements` 只认 `assert-view-uuid`，`updates` 认
  `assign-uuid|upgrade-format-version|add-schema|add-view-version|set-current-view-version|set-location|set-properties|remove-properties`。
- 复用 `Catalog::commit_table` 的骨架（去掉快照校验），错误类型 `NoSuchViewException`。
- `/config.endpoints` 追加 view 端点。
- 单测：create / load / replace（版本 +1）/ rename / drop；与表同名 409。

## 2. `/_iceberg/v1` 别名与 `s3tables` 默认签名

`tables.compat_prefix` 非空时 dispatch 两个前缀都匹配；`GET /config` 的
`defaults["lights3.catalog-compat-prefix"]`；CreateBucket 同时保留 `_iceberg` 名。
签名名在 ② 已放开，这里只是文档与冒烟脚本的 `--compat` 开关。价值：MinIO AIStor
迁移用户的配置零改动。

## 3. `reportMetrics` 落审计

`POST …/tables/{t}/metrics`（① 已 204 丢弃）改为解析
`{"report-type":"scan-report"|"commit-report", ...}` 写审计事件 `tables.metrics`
（`detail` = 精简后的 JSON：`filter`、`projected-field-names`、`metrics.result-data-files`
等计数）。上限 64 KiB，超出仍 204 但不记录。价值：查询审计与热表统计；零风险。

## 4. compaction 候选规划输出

`plan` 报告增加 `compaction_candidates`：读当前快照的 manifest（③ 已有），把
`content == 0`、`size_bytes ≤ small_file_threshold` 的数据文件按
`(partition 目录前缀, sort_order_id)` 分组，组内按 `target_file_size` 贪心装箱，
输出 `[{"partition":"…","sort-order-id":0,"files":[…],"bytes":N}]`；不执行重写
（交给 Spark `rewrite_data_files` 或引擎自带 compaction）。`delete` 文件存在的分区
标 `row_level_required`。价值：给运维/引擎一个"该合并什么"的机器可读清单。

## 5. duostore-meta 后备（设计 §12）

`DuoMetaCatalogStore : ITableCatalogStore`，条件：默认后端是 duostore。

- `IMetaStore` 增加通用 KV 面：`kv_get(prefix,key) / kv_put(prefix,key,value,PutCondition) / kv_delete / kv_scan(prefix, after, limit)`
  （新 column family `tc`，redis 用 hash+Lua、tikv 用事务、rocksdb 用 WriteBatch、sqlite 用表）；
  `PutCondition.if_match_etag` 的 etag = `sha256(value)` 前 16 字节 hex，由 KV 层计算并在事务内比对。
- 提交协议的 7–9 步合成一个事务（`kv_put` commit + `kv_put` table 同批），崩溃窗口矩阵退化为一行。
- 配置 `tables.catalog_backing: object | duostore`（默认 object）；两种后备之间迁移用
  `lights3 tables export|import`（离线，JSON 行）。
- 单测：`backend_suite` 风格的 `catalog_store_suite.h` 对两个实现跑同一组用例。

价值：单表提交少三次对象写、跨网关靠 meta 事务而非对象 CAS；难度高（四种 meta 各一份 KV 面），
只有 duostore 部署且提交 QPS 成为瓶颈时才值得。

## 6. 不做（重申设计 §15）

多表事务、`/plan` `/tasks` `/sign`、compaction 执行、durable-strong 单快照、Delta/Hudi、
跨区域双活写。
