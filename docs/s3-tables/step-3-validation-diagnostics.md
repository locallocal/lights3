# 步骤 ③：Avro 深校验、诊断与恢复

> 状态：**已实现（2026-09-12，分支 feat/s3-tables-step3）**。对应设计 §5.4、§5.6、§7.4、§14 ③。
> 实现与本稿的差异见文末 §12。
> 依赖 ①。完成后：CommitTable 会读 manifest-list / manifest 核对每个数据文件存在；
> post-CAS 的 finalization gap 可被诊断与修复；rename 中断可由任何写者恢复；
> LoadTable 支持 `ETag` / `If-None-Match`；`fsck` 报告目录状态与表桶的不一致。

## 1. 目标与验收

| 验收项 | 判据 |
| --- | --- |
| Avro | 读取器对 PyIceberg 0.9 / Spark 1.6 生成的 manifest-list 与 manifest 固件（null 与 deflate 编解码）解析结果与 PyIceberg 打印一致 |
| 深校验 | 提交引用不存在的数据文件 → 409；`file_size_in_bytes` 不符 → 409；`manifest_length` 不符 → 409；重复 ADD 活跃文件 → 409 |
| 诊断/恢复 | 注入 `tables.commit.after_cas` 故障后 `GET …/catalog/diagnostics` 报 `FinalizationRequired`，`POST …/catalog/recovery` 后为 `Committed`，指针不变 |
| rename 恢复 | 五步各注入一次中断，另一实例（第二个 `Catalog`）的任一写操作把它推进到完成或回滚 |
| ETag | LoadTable 带 `If-None-Match: <etag>` → 304 无 body |
| fsck | `lights3 fsck` 对"指针指向不存在的 metadata"与"孤儿 `.sys/tables-catalog/<bucket>/`"各报一条 finding |

## 2. 文件清单

```text
src/tables/iceberg/avro_reader.{h,cc}      新增：Avro OCF 只读解码器
src/tables/iceberg/manifest.{h,cc}         新增：manifest-list / manifest 记录模型（从 Avro Json 到结构体）
src/tables/iceberg/snapshots.{h,cc}        扩展：check_new_snapshots_deep、冲突复核
src/tables/diagnostics.{h,cc}              新增：提交记录分类、恢复、rename 恢复驱动
src/tables/catalog.cc                      接入深校验、rename 恢复、ETag
src/tables/rest_api.cc                     GET …/catalog/diagnostics、POST …/catalog/recovery、If-None-Match
src/cli/cli_fsck.cc + src/app/admin_jobs.cc   fsck 增加目录对账项
CMakeLists.txt                             find_package(ZLIB)（可选）→ LIGHTS3_TABLES_ZLIB
tests/unit/test_tables_avro.cc             新增
tests/fixtures/tables/*.avro               PyIceberg / Spark 生成的固件
```

## 3. Avro OCF 读取器（`avro_reader.h`）

只实现 Iceberg manifest 需要的子集，输入整段字节（≤128 MiB，调用方先读满）。

```cpp
namespace lights3::tables::iceberg::avro {
struct Schema;                              // 解析后的 schema 树（shared_ptr 节点，named types 表）
struct Reader {
    // 抛 RestError(409, "CommitFailedException", "invalid avro: ...")——manifest 损坏等价于提交不可验证
    explicit Reader(std::string_view bytes);
    const nlohmann::json& schema_json() const;      // avro.schema 原文
    std::string_view codec() const;                 // "null" | "deflate" | 其他
    const std::map<std::string, std::string>& meta() const;   // OCF 头元数据（Iceberg 放 schema / partition-spec 等）
    // 逐块解码，每条记录回调 Json；codec 不支持 → 抛 UnsupportedCodec（调用方决定降级）
    void for_each(const std::function<void(nlohmann::json&&)>& fn, size_t max_records);
};
struct UnsupportedCodec : std::runtime_error { using std::runtime_error::runtime_error; };
}
```

实现要点：

- 头：magic `Obj\x01`、`meta` map（`avro.schema`、`avro.codec`）、16 字节 sync；块：
  `long count`、`long size`、数据、sync 校验。
- 解码：zigzag varint（int/long）、float/double 小端、bytes/string（长度前缀）、
  boolean、null、fixed、enum（index）、array/map（块计数可为负 = 带字节长度）、
  union（index + 分支）、record（按 schema 字段序）。命名类型引用查表；
  `logicalType` 忽略（Iceberg manifest 的 `timestamp`、`decimal` 以原始类型处理）。
- 递归深度上限 64，单条记录大小上限 16 MiB，块内记录数与 `max_records` 累计。
- deflate：`LIGHTS3_TABLES_ZLIB` 时用 zlib `inflateInit2(-15)`（raw deflate，Avro 规范）；
  未编译 zlib 或 codec 为 snappy/zstd/bzip2 → `UnsupportedCodec`。

## 4. manifest 模型（`manifest.h`）

```cpp
struct ManifestFile { std::string path; int64_t length; int spec_id; int content /*0 data,1 deletes*/;
                      int64_t sequence_number, min_sequence_number, added_snapshot_id; };
struct DataFile { int status /*0 existing,1 added,2 deleted*/; std::string path; int content;
                  std::string format; int64_t size_bytes, record_count; std::optional<int64_t> sequence_number; };
std::vector<ManifestFile> parse_manifest_list(avro::Reader&);   // 字段名按 Iceberg 规范：manifest_path, manifest_length, partition_spec_id, content, sequence_number, min_sequence_number, added_snapshot_id
std::vector<DataFile>     parse_manifest(avro::Reader&);        // manifest_entry：status, snapshot_id, sequence_number, data_file{content,file_path,file_format,file_size_in_bytes,record_count}
```

v1 manifest 缺 `content` / `sequence_number` 时按 0 处理；字段缺失或类型不符 → 409。

## 5. 深校验（`snapshots.h` 扩展）

```cpp
struct DeepCheckOptions { int concurrency = 16; size_t max_avro = 128 << 20; size_t max_manifests = 10000; size_t max_files = 1000000; bool allow_unsupported_codec = true; };
struct DeepCheckReport { size_t manifests = 0, files = 0; bool skipped_codec = false; };
Task<DeepCheckReport> check_new_snapshots_deep(const SnapshotCheckContext&, const Json& current, const Json& next, const DeepCheckOptions&);
```

流程（每个新增快照）：

```text
1 mlist = read_all(get_object(manifest-list))（上限 max_avro）；Reader；UnsupportedCodec → 若 allow: report.skipped_codec=true, 跳过该快照；否则 409
2 for m in parse_manifest_list: 路径校验（桶内、非保留前缀）；head_object 大小 == m.length（否则 409）；累计 ≤ max_manifests
3 分批 when_all（concurrency 个）：读 manifest → parse_manifest → 对 status ∈ {0,1} 的文件：路径校验 + head_object；
  size_bytes > 0 时比对 ObjectMeta.size（否则 409 "data file size mismatch"）；累计 ≤ max_files
4 冲突复核（设计 §7.4）：parent = snapshots[next.parent-snapshot-id]；活跃集 = 遍历 parent 的 manifest-list 全部 status ∈ {0,1} 文件路径（缓存于本次调用）；
  新快照 status=1 的路径 ∈ 活跃集 → 409 "re-adding a live file"；status=2 的路径 ∉ 活跃集 → 409 "deleting a non-live file"；
  summary.operation == "append" 且存在 status=2 或 content=1 的条目 → 409
5 statistics / partition-statistics：head_object 存在 + 读前 4 字节比对 "PFA1" / "PAR1"（Range GET 0-3）
```

`head_object` 走表桶所在后端（`router.resolve(bucket)`），每个都是独立协程，用
`when_all` 分批（`core/task.h` 的 `when_all`）。cloudproxy 后端上这是远端 HEAD，
16 并发是保守值。`Catalog::commit_table` 的 f 步改调深校验；
`report.skipped_codec` 时 LoadTable / commit 响应的 `config["lights3.snapshot-validation"] = "skipped-codec"`
（否则 `"deep"`）。`register_table` 同样走深校验（`current` 为空，所有快照都新增）。

## 6. 诊断与恢复（`diagnostics.h`）

```cpp
enum class CommitState { Committed, StagedBeforeTableUpdate, FinalizationRequired, Superseded, ManualReview };
struct CommitDiagnosis { CommitRecord record; CommitState state; std::string note; };
struct TableDiagnostics { TableEntry entry; std::vector<CommitDiagnosis> commits; std::vector<std::string> unreferenced_metadata; };
Task<TableDiagnostics> diagnose_table(ITableCatalogStore&, storage::IStorageBackend& bucket_backend, bucket, levels, name);
// 只补记录、不动指针：FinalizationRequired → 写 COMMITTED；Superseded / StagedBeforeTableUpdate → 删记录（可选 --prune）
struct RecoveryReport { int finalized = 0, pruned = 0, manual = 0; };
Task<RecoveryReport> recover_table(ITableCatalogStore&, bucket, levels, name, bool prune);
```

分类规则（对每条 `list_commits`）：

| 条件 | 状态 |
| --- | --- |
| `status == COMMITTED` | Committed |
| STAGED 且 `entry.version_token == new_token` | FinalizationRequired |
| STAGED 且 `entry.version_token == expected_token` | StagedBeforeTableUpdate |
| STAGED 且 `entry.generation` 对应的历史（沿 `prev_metadata_location` 链回溯 COMMITTED 记录）能证明 `new_metadata_location` 在链上 | FinalizationRequired |
| STAGED 其余 | Superseded |
| 记录字段缺失 / 链有环 | ManualReview |

`unreferenced_metadata`：列保留目录 `metadata/`，减去当前指针、`metadata-log`、
所有记录的 `new_metadata_location`（供 ④ 的维护规划复用）。

REST：`GET …/tables/{t}/catalog/diagnostics` → JSON
`{"table":{...entry},"commits":[{"commit-id","status","state","note"}],"unreferenced-metadata":[...]}`
（Read 权限）；`POST …/catalog/recovery` `{"prune": false}` → `RecoveryReport`（Write 权限，审计 `tables.recovery`）。
`lights3-ctl tables diagnose|recover <bucket> <ns.table>` 走同一端点（④ 一起加 CLI）。

## 7. rename 恢复驱动（`diagnostics.cc`）

```cpp
// 设计 §5.6：从 intent.stage 继续；每步 CAS 失败即重读判断已完成与否（幂等）
Task<void> drive_rename(ITableCatalogStore&, std::string_view bucket, RenameIntent intent);
Task<void> recover_renames(ITableCatalogStore&, std::string_view bucket);   // list_renames 逐个 drive
```

接入：`Catalog` 的每个**写**入口（create / register / commit / drop / rename /
metadata-location PUT）在读到 `Renaming` 状态或每 N 次调用时先
`recover_renames(bucket)`；读入口遇 `Renaming` 仍 503。超过 `tombstone_ttl`
仍处于 `Prepared` 阶段的 intent 视为放弃：回滚（源改回 Active，若目标已写则删目标）。

## 8. LoadTable 的 ETag（`rest_api.cc`）

响应头 `ETag: "<entry etag>"`；请求 `If-None-Match` 等于（去引号后）→ 304，无 body，
仍带 `ETag`。`Catalog::load_table` 先 `get_table` 拿 etag，命中 304 时**不读 metadata**
（省一次对象读）。`config` 里加 `"lights3.catalog-etag"` 便于调试（可选）。

## 9. fsck 对账（`cli_fsck.cc` / `admin_jobs.cc`）

`run_scrub` 之后追加"tables 对账"（仅当扫描的后端是默认后端、`.sys/tables/` 非空）：

- 每个表桶：桶存在？否 → finding `tables.orphan_state`。
- 每个 `tbl/*.json`：`metadata_location` 对象存在？否 → `tables.dangling_pointer`；
  `state == Renaming` 且 intent 缺失 → `tables.stale_renaming`。
- `renames/` 中 intent 的源/目标条目一致性。

findings 计入 `JobOutcome.findings`，`kind = "tables"` 明细进 `stats`。不修复（修复走
`catalog/recovery`）。

## 10. 单测清单

- `test_tables_avro.cc`：手工构造的最小 OCF（每种基本类型一条）；PyIceberg 固件
  （manifest-list + manifest，null 与 deflate 各一）逐字段断言；截断文件 / sync 不符 /
  超深递归 → 抛；`UnsupportedCodec`（构造 `avro.codec: snappy` 头）。
- `test_tables_catalog.cc` 追加：深校验 6 种 409（缺数据文件、大小不符、manifest 长度不符、
  越桶路径、重复 ADD、append 带 delete）；`allow_unsupported_codec` 两态；
  诊断分类五态各一（用 fault 点制造）；`recover_table` 后指针 etag 不变；
  rename 五步中断 × 由另一个 `Catalog` 实例的 `create_table` 触发恢复 → 终态正确；
  Prepared 超时回滚。
- `test_tables_rest.cc` 追加：diagnostics / recovery 端点形态与权限；
  `If-None-Match` 304；`skipped-codec` 标记。
- `test_admin_jobs.cc` 追加：fsck 三种 finding。

## 11. 陷阱

- **Avro 的 map/array 负块计数**：负数表示后面跟着字节长度，常见于 Spark 写出；必须实现。
- **`manifest_length` 与 `file_size_in_bytes` 可为 0**：0 视为"未知"，不比对。
- **`when_all` 的异常传播**：任一 head 失败即整体失败，并发未完成的协程要在取消后
  收尾；用现有 `when_all` 语义（首个异常抛出，其余等待完成）即可，注意不要在
  catch 内 `co_await`。
- **cloudproxy 上的 HEAD 成本**：1000 个文件 = 1000 次远端 HEAD；`validate_concurrency`
  与 `max_files` 是唯一闸门，文档写明。
- **zlib 可选依赖**：`find_package(ZLIB)` 找不到时编译仍通过，`avro_reader.cc` 用
  `#ifdef LIGHTS3_TABLES_ZLIB`；`--version` 输出加 `tables: deflate=yes|no`。

## 12. 实现记录（2026-09-12）

- **Avro 读取器**（`iceberg/avro_reader.{h,cc}`）：按 §3 实现；`Reader` 持有输入
  `string_view`（调用方先读满，`DeepCheckOptions::max_avro` 封顶）。bytes / fixed 解码成
  `json::binary`，union 返回分支值本身（`["null", T]` 的 null 分支即 `null`），
  `logicalType` 一律忽略（Iceberg 的 map 以 `logicalType: map` 的 record 数组落地，按数组
  读）。深度上限 64、单条记录 16 MiB、单块解压 128 MiB；负块计数（Spark 写法）已实现并有
  单测。deflate 走 zlib raw inflate（`LIGHTS3_TABLES_ZLIB`，CMake `find_package(ZLIB QUIET)`），
  `avro::deflate_supported()` 供测试与 `--version`（输出 `tables:   deflate=yes|no`）；
  `features` 行同时多了 `tables`。
- **manifest 模型**（`iceberg/manifest.{h,cc}`）：`DataFile` 多了 `snapshot_id`；
  `parse_manifest` 接受 manifest 的 `sequence_number` 做继承（null 且 status=ADDED，或
  manifest 序号为 0），与 PyIceberg 读回的值逐字段一致（固件 `<name>.json` 即 PyIceberg 的
  读回结果，由 `scripts/tables/gen_fixtures.py` 生成）。
- **深校验**（`check_new_snapshots_deep`）：流程按 §5，两处与稿子不同：
  1. 冲突复核只看**归属于新快照**的条目（`entry.snapshot_id == 新快照 id`，为空则看
     manifest 的 `added_snapshot_id`）。Iceberg 写者 append 时原样复用父快照的 manifest
     文件（其中的 ADDED 条目属于旧快照），按稿子的写法每次 append 都会被判成"重复 ADD"。
     `append` 不得带删除的规则同样只作用于归属条目，并扩展到 `content=1` 的 manifest。
  2. manifest 长度不走单独 HEAD：GET 返回的 `ObjectMeta.size` 直接比对（少一次往返）。
  父快照的活跃集按 `parent-snapshot-id` 在 `next` 里找，找不到（已过期）或父快照的
  codec 不可读则跳过复核并记 `skipped_codec`。统计文件按 §5 第 5 步做 Range GET 魔数比对。
  指标：`lights3_tables_validation_files_total`、`lights3_tables_validation_skipped_total`。
  `LoadedTable.validation`（`"deep"` / `"skipped-codec"`）进 LoadTable / CommitTable 响应的
  `config["lights3.snapshot-validation"]`（CommitTable 响应因此多了 `config` 对象，含
  `lights3.catalog-etag`）。register / metadata-location PUT 同走深校验。
  **配置不新增键**：并发度仍是 `tables.validate_concurrency`，其余上限用 `DeepCheckOptions`
  默认值（10 000 manifest / 1 000 000 文件 / 128 MiB）。
- **诊断与恢复**（`diagnostics.{h,cc}`）：`diagnose_table` 比稿子多一个 `metadata_dir`
  参数（保留目录由 `Catalog` 算好传入）；`unreferenced_metadata` 的"已引用"集合额外含每条
  记录的 `prev_metadata_location`；`ITableCatalogStore` 新增 `delete_commit`（prune 用）。
  REST：`GET …/catalog/diagnostics`（Read）/ `POST …/catalog/recovery`（Write，审计
  `tables.recovery`），不进 `/config` 的 `endpoints`；`table` 字段是目录条目 JSON 加
  `etag`，`metadata_location` 改成 `s3://` 形式。`lights3-ctl tables diagnose|recover`
  留给 ④。
- **rename 恢复**（`drive_rename` / `recover_renames`）：`Catalog::rename_table` 写完
  intent 后就调用同一个 `drive_rename`，因此正常路径与恢复路径是同一段代码。每一步对实体
  CAS，**每次阶段推进对 intent 本身 CAS**（`put_rename` 改为返回 ETag），多个驱动者并发时
  输家重读判断"已完成"或退出（`Contended`）。Prepared 阶段：源 etag 已变 → 删 intent
  （`Abandoned`）；超过 `tables.maintenance.tombstone_ttl` 仍未 fence → 同样放弃；已被本
  intent fence 则继续（不回滚）。目标已被占用 → 源改回 Active、删 intent
  （`DestinationTaken`，409）。回滚只动"被本 intent fence 的源"。写入口（create / register /
  commit / drop / rename / metadata-location PUT）：读到 `Renaming` 时强制先恢复再重读；
  否则每桶每 32 次调用（进程内第一次算）扫一遍 `renames/`。读入口仍 503。故障点五个：
  `tables.rename.after_prepare|after_fence|after_destination|after_tombstone|before_cleanup`。
- **ETag**：`Catalog::load_table(..., if_none_match)`，接受带引号 / `W/` / 逗号列表 / `*`；
  命中回 304 无 body 带 `ETag`，不读 metadata。`config["lights3.catalog-etag"]` 已加。
- **fsck**：对账逻辑放在 `tables/fsck.{h,cc}`（`reconcile_catalog(sys_backend, router)`），
  不在 `admin_jobs.cc` 里；`AdminJobs::set_fsck_extension` 让在线 `POST /-/admin/fsck/<默认后端>`
  与离线 `lights3 fsck <默认后端>` 都把结果并进结论（`findings` 累加，明细在
  `stats.tables`）。发现种类：`tables.orphan_state`、`tables.dangling_pointer`、
  `tables.stale_renaming`、`tables.inconsistent_rename`（intent 与源/目标条目不符）、
  `tables.malformed_entry`。墓碑不查指针。
- **固件**：`tests/fixtures/tables/`，PyIceberg 0.12.0 生成（本机无 Spark，Spark 固件未入
  库；PyIceberg 与 Spark 的 Avro 写法差异只在块计数符号，负块计数已用手工构造覆盖）；
  路径写死在桶 `tbk`（表 `n/t`）与 `tbe2e`（e2e）。单测：`test_tables_avro.cc`（新）、
  `test_tables_catalog.cc` 追加 4 例、`test_tables_rest.cc` 追加 1 例、`test_admin_jobs.cc`
  追加 2 例；e2e 段改用真固件并加深校验 409 / 304 / diagnostics / recovery / 只读凭证。
