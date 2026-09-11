# S3 Tables：Apache Iceberg REST Catalog（调研 RustFS 后的设计）

> 状态：**设计稿（2026-09-11），尚未实现**。本文先回答"RustFS 是怎么做 S3 Tables
> 的"（§2，源码核实 @853ae63，2026-09-11），再给出 lights3 的方案（§3–§13）与
> 实施拆分（§14）。代码落地后，本文按仓库惯例保留为设计层文档，实现细节写进
> 对应实现文档；源码注释用 `docs/s3-tables-design.md §N` 引用本文。
>
> 一句话结论：**lights3 把表目录做成"建在 `.sys` 桶上的 Iceberg REST Catalog"**——
> 目录实体每个一个 JSON 对象，用已经存在于全部后端的原子条件写
> `PutCondition`（`If-None-Match: *` / `If-Match: <etag>`，`storage/backend.h`）
> 做元数据指针的 CAS 提交；REST 面挂在 `/iceberg/v1` 路径前缀上，复用 SigV4、
> per-credential policy、STS 会话与 AdminJobs；Iceberg 元数据由**服务端**生成与
> 校验，客户端（PyIceberg / DuckDB / Spark / Trino）只需标准 REST catalog 配置。
> 不追求 AWS S3 Tables 控制面 API 的逐字兼容（RustFS 也不追求）。

相关：[s3-protocol.md](s3-protocol.md)（路由表、SigV4、条件请求）、
[credential-management.md](credential-management.md) §10（policy 与 STS 会话）、
[multi-tenancy.md](multi-tenancy.md)（配额与审计钩子）、
[storage/storage-backend.md](storage/storage-backend.md) §1（`PutCondition` 契约）、
[deployment.md](deployment.md) §5（多网关支持矩阵）、[cli.md](cli.md)（AdminJobs 与 `lights3-ctl`）。

## 1. 目标与非目标

**目标**：

1. **表桶（table bucket）**：普通桶经显式启用后成为表桶，作为 Iceberg REST
   catalog 的 warehouse；桶内表数据仍是普通 S3 对象，引擎用现有 S3 数据面读写。
2. **Iceberg REST Catalog**：实现规范中的 namespace / table 端点子集（§6.3），
   `GET /v1/config` 只广播已实现的端点；PyIceberg 与 DuckDB 端到端可用，Spark /
   Trino 的 REST 配置可用（读写与只读）。
3. **原子单表提交**：CommitTable 的"校验 requirements → 生成新 metadata.json →
   CAS 指针"在多网关共享 `.sys` 时仍然是一次成功、其余 409 可重试。
4. **表感知的数据面**：目录保留前缀不可被普通 S3 写删；表桶排除在 lifecycle
   过期之外；DeleteBucket 对非空表桶拒绝；可选按表前缀下发短期凭证。
5. **维护**：元数据文件保留、快照过期、孤儿清理，作为运维触发的 AdminJobs
   作业，可选周期执行；一切删除都受安全窗口与当前指针复核保护。

**非目标（明确不做，见 §15 的理由）**：AWS S3 Tables 控制面 API 逐字兼容、
多表事务（`/transactions/commit`）、服务端扫描规划（`/plan`、`/tasks`）、
remote-signing、行级/删除文件合并的执行（compaction 执行）、Delta / Hudi、
内置 SQL、跨区域双活写。

## 2. RustFS 的实现（调研结论）

RustFS（Rust，Apache-2.0）在 2026 年把 S3 Tables 作为"Iceberg REST Catalog +
表桶"直接内建到对象存储核心（`rustfs/src/table_catalog/`、
`rustfs/src/admin/handlers/table_catalog/`，合计约 7 万行含测试）。它的官方支持
矩阵（`docs/architecture/s3-tables-support-matrix.md`）明确写着"不声称与 AWS
S3 Tables 控制面 API 对等"，可声称的是：PyIceberg 与 DuckDB 自动化冒烟通过、
表感知的数据面策略、受控维护、提交恢复诊断、Spark / Trino 的人工 harness。

### 2.1 总体形态

| 维度 | RustFS 的选择 |
| --- | --- |
| 协议面 | Iceberg REST Catalog；前缀 `/iceberg/v1`（MinIO AIStor 风格别名 `/_iceberg/v1`）；`{prefix}` 即桶名 |
| 路由 | 两个前缀走 **admin 路由器**（`is_admin_path` 包含 `is_table_catalog_path`），在 S3 路由之前被截获；未保留桶名，桶名恰为 `iceberg` 且 key 以 `v1/` 开头会被遮蔽 |
| 认证 | SigV4；credential scope 的 service 接受 `s3` 与 `s3tables`（`sig_v4_allowed_services`），不按 service 分流 |
| 授权 | 自定义 `admin:*Table*` 动作族（`admin:CreateTable`、`admin:CommitTable`、`admin:GetTableMetadata`、`admin:SetTableMetadata`…），资源为 `arn:aws:s3:::<bucket>/namespaces/<ns>/tables/<t>` |
| 状态落点 | **系统桶** `.rustfs.sys` 下 `s3tables/catalog/table-buckets/<sha256(bucket)>/…`；用户桶只放标记 `table-bucket.json` 与保留前缀 `.rustfs-table/` 下的 Iceberg 文件 |
| 一致性原语 | 对象 ETag 的条件写（`If-None-Match: *` / `If-Match`），外加分布式命名空间锁串行化每个目录对象的写者 |
| 后备模式 | `object`（默认）与 `durable-strong`（整份目录一个 ≤64 MiB 的快照对象，单锁 + ETag CAS） |
| 元数据生产者 | **服务端**：CreateTable / CommitTable 由服务端生成并写 metadata.json；另保留"legacy pointer"模式（客户端自写 metadata 后只换指针） |
| 维护 | 规划与执行分离；无内置周期调度，全部由运维触发端点驱动 |

### 2.2 状态模型与持久化

- 实体：`TableBucketEntry`、`NamespaceEntry`（多级，`a.b.c`，存储 id 用 `/` 连接）、
  `TableEntry`（`table_id` 与 Iceberg `table_uuid` 是两个独立 uuid；
  `metadata_location` 桶内相对 key；`version_token = "token-<uuid>"` 每次提交
  换新；`generation` 单调递增；`state ∈ Active|Renaming|Deleting|Deleted`）、
  `ViewEntry`、`CommitLogEntry`（`Staged|Committed|Failed`）、`TableRenameIntent`、
  反向索引 `TableWarehouseIndexEntry`（数据前缀 → 所属表，供数据面授权）。
- 所有持久化结构 `deny_unknown_fields` + `version == 1` 严检：旧二进制遇到新
  字段/新状态**失败关闭**，`Renaming` 状态就是刻意利用这一点让老读者拒绝。
- 每次读取都校验"实体身份字段重算出的路径 == 读到它的对象路径"，复制/挪动过
  的 JSON 被拒绝。
- 元数据文件名 `{generation:05}-{uuid}.metadata.json`，始终在保留前缀
  `.rustfs-table/warehouses/default/namespaces/<ns>/tables/<t>/metadata/` 下，
  **与表的 `location` 无关**。
- 标识符：段 `[a-z0-9_-]`、首尾字母数字、≤64 字节；namespace 全长 ≤512；禁 `.`
  `/` `\` `%`（`%1F` 由 handler 先解码再切分）。
- 分页 token：base64url 的 `{version, context: sha256(resource\0warehouse\0ns), cursor}`，
  换一个列表操作重放即 400；默认与上限 pageSize 1000。

### 2.3 提交协议（单表 CAS）

```text
1 取表锁 + 发布栅栏（表桶级 publication.lock，表级 publication.lock）
2 读 (TableEntry, etag0)，须 Active
3 幂等查找：commits/<h(table_id)>/<h(commit_id)>.json、commit-idempotency/<h(key)>.json
   已 Committed 或"Staged 但指针已前进/可由提交链证明"→ 重新 finalize 并幂等返回
   payload 不同 → 409
4 version_token / metadata_location 与 expected 相符，否则 409
5 读新 metadata（≤50 MiB），可选 assert-rustfs-metadata-sha256
6 STAGED：commit log（If-None-Match:*）+ 幂等索引
7 CAS：put table-entry.json If-Match: etag0     ← 唯一原子点
8 finalize：commit log → COMMITTED（结果刻意忽略："CAS 已成功，staged 记录就是
   持久的恢复源；finalize 失败不能把已对外提交的指针变成失败响应"）
```

"finalization gap"由 `catalog/diagnostics` 分类
（`Committed | StagedBeforeTableUpdate | FinalizationRequired |
IdempotencyIndexRepairRequired | ManualReview`）、`catalog/recovery` 修复，
修复**永不移动指针**。rename 是带持久化 intent 的两阶段：source→`Renaming`
→ dest 写入 → source 改写为 `Deleted` 墓碑（"用条件替换的墓碑而不是无条件
删除"）→ 反向索引改指 → dest `Active` → intent `Completed`；期间表桶
`active_rename_id` 非空，所有读者 503、所有写者先跑恢复。

### 2.4 REST 面要点

- `GET /v1/config`：`defaults.warehouse = "default"`（常量，不是桶名）、
  `overrides["namespace-separator"] = "%1F"`、`endpoints` 列出 21 个标准端点；
  扩展端点（refs、metadata-location、maintenance、catalog/{export,import,
  diagnostics,recovery,rollback,migration}、buckets/{w}）**不**列进 `endpoints`。
- CreateTable：服务端重排字段 id（schema-id 0、spec-id 0、partition field id 从
  1000 起）、生成初始 metadata（v1/v2，v3 → 406）、`If-None-Match:*` 写入、
  注册；响应 200 的 `LoadTableResult`；`config` 里**没有** `s3.endpoint` /
  `s3.path-style-access` 之类，只有 `rustfs.credential-*` 说明键。`stage-create`
  → 406。
- CommitTable：`requirements` 支持全部 8 种 `assert-*`；`updates` 支持规范全集
  （含 `set-statistics`、`remove-partition-specs`、`remove-schemas`），
  `add-encryption-key` 等 v3 → 406；**服务端**应用 updates 生成新 metadata，做迁移
  不变量校验（`last-column-id` / `last-partition-id` / `last-sequence-number`
  单调、既有 schema/spec/snapshot 逐字节不变），再读 manifest-list / manifest
  Avro 校验每个新快照引用的数据/删除文件**存在**且在桶内，重放父快照活跃文件集
  拒绝重复加入/删除不存在的文件；写入 `{gen+1:05}-<token>.metadata.json`
  （token = commit-id）。请求 `commit-id` / `idempotency-key` 是 **body 字段**，
  重试时重放 updates 必须逐字节复现已提交对象。
- 错误：`{"error":{"message","type","code"}}`；`PreconditionFailed`/412 →
  409 `CommitFailedException`；404 按路径选 `NoSuchNamespaceException` /
  `NoSuchTableException` / `NoSuchViewException`；`Unsupported` → 406
  `UnsupportedOperationException`；`Unavailable` → 503。
- 凭证下发：仅当 `X-Iceberg-Access-Delegation` 含 `vended-credentials` 且服务端
  开关打开；一份 STS 临时用户带内嵌会话策略（表前缀下的 Get/Put/Delete/
  AbortMultipart + metadata 对象只读）；TTL 60–3600 s；响应
  `Cache-Control: no-store, private`。

### 2.5 数据面集成

- 保留前缀 `.rustfs-table/`：表桶内对它的 Put / Copy 目标 / Delete / Restore /
  multipart 各步全部拒绝（`InvalidRequest "Object key is reserved for the table
  catalog"`），读放行；桶元数据读不到时**默认拒绝**。
- "表感知策略桥"：普通 S3 对象操作被 IAM 放行后，再按反向索引找出对象所属的
  表，追加一次 `admin:GetTableMetadata` / `admin:SetTableMetadata` 检查；匿名
  访问表前缀一律拒绝；内容变更还要持有发布栅栏的共享读锁，提交不能在数据写
  途中发布。
- lifecycle：表桶整体排除在过期之外（含已排队的工作）；只有目录维护能删表文件。
- DeleteBucket：`.rustfs-table` 下仍有对象 → `BucketNotEmpty`；删桶时顺带清理
  系统桶下的目录状态。

### 2.6 维护

配置（`retain-recent-metadata-files`、`delete-enabled`、`worker-lease-timeout-seconds`、
重试退避、quarantine…）三级解析（表 → 表桶 → 默认）。规划：元数据保留集 =
当前指针 ∪ `metadata-log` ∪ 最近 N 个 ∪ 受保护 ref 可达；可达性清理读
manifest-list → manifest → 数据/删除文件，**任一无法解析即全体进入人工复核**；
删除只针对 mtime 早于 15 min 安全窗口且复核当前指针后仍是候选的对象。快照
过期先出 plan，再经标准 commit 的 `remove-snapshots` 提交，plan 过期即失败关闭。
作业状态机 `NotYetRun|Queued|Running|Successful|Failed|Disabled|Paused`，
带调度器租约 / worker 租约 / 心跳 / 退避重试；没有进程内周期调度器。

### 2.7 测试

服务端约 630 个单测跑在带故障注入的内存对象后端上（按尝试序号注入读/写/
删失败、写后失败、暂停栅栏、无 ETag 对象）；`scripts/table-catalog/` 的
PyIceberg 与 DuckDB 冒烟脚本是"可声称支持"的唯一依据；`failure_coverage.py`
生成故障探针计划（陈旧 token 冲突、post-CAS finalization gap、单胜者 CAS、
拒绝时无数据面旁路、陈旧维护计划拒绝…）。

### 2.8 借鉴与不照搬

| 借鉴（照做） | 不照搬（lights3 另选） | 理由 |
| --- | --- | --- |
| 目录状态放系统桶、每实体一对象、ETag 条件写做 CAS | 分布式命名空间锁 | lights3 无锁服务；`PutCondition` 的原子性由各后端在提交点保证，跨网关靠 CAS 收敛即可（§5.5） |
| staged commit log 先于指针 CAS；finalize 失败不回滚响应 | 幂等索引单独一份对象 | 幂等 key 直接编进 commit log 的对象名，少一次写（§5.3） |
| 服务端生成/校验 metadata.json；requirements / updates 全集；迁移不变量 | legacy pointer 模式 | 指针只由服务端写，S3 面对保留前缀只读，攻击面更小；register 走"读取→校验→复制入保留目录"（§6.5） |
| 保留前缀不可写、lifecycle 排除、DeleteBucket 守卫 | 反向索引 + 第二道 IAM 检查 | lights3 policy 是 bucket glob + key prefix 白名单，表路径 = key 路径，**天然**按前缀限权，无需第二套动作族（§6.2） |
| 分页 token 绑定上下文；标识符字符集 | `defaults.warehouse = "default"` | lights3 回桶名，客户端配置更直观 |
| 维护"规划 / 执行分离 + 安全窗口 + 指针复核" | 租约 / 心跳 / quarantine 状态机 | lights3 已有 AdminJobs（每后端单作业、`JobInProgress`）与可选周期 runner，够用（§9） |
| `X-Iceberg-Access-Delegation` 协商的凭证下发 | IAM 临时用户 | 复用 STS 会话（`mint_session`）加"收窄 policy"参数（§8.4） |
| Avro 读 manifest 做文件存在性校验 | 引入 avro-cpp | 树内最小 Avro OCF 读取器（只读、null/deflate 编解码）（§7.4、§11） |
| 不实现 stage-create / purge / v3 时回 406 而非静默 | `/_iceberg/v1` 别名与 `s3tables` 签名名 | 作为可选项放 §14 ⑥，默认关闭 |
| durable-strong 单快照后备 | — | 不做：lights3 若需强后备，走 duostore meta（KV 事务）实现 `ITableCatalogStore`（§12） |

## 3. 架构定位

新增 **Tables 子系统**（`src/tables/`），位于 L2 之内、与 S3 handler 平行；
对下只依赖 `IStorageBackend`（经 `BucketRouter`）与 `.sys` 默认后端，对上经
`S3Service::dispatch` 的路径前缀分派接入。不新增存储接口。

```text
                 HttpRequest (path-style)
                          │
        ┌─────────────────▼──────────────────┐
        │ S3Service::dispatch                │
        │  /-/…      → admin 面              │
        │  /iceberg/v1/… → tables::RestApi   │  ← 新增分支（§6.1）
        │  其余       → S3 路由表             │
        └───────┬───────────────────┬────────┘
                │                   │ 表桶守卫（§8.1）：保留前缀只读、
                │                   │ lifecycle 排除、DeleteBucket 守卫
   ┌────────────▼─────────────┐     │
   │ tables::RestApi           │     │
   │  认证(SigV4 s3|s3tables)  │     │
   │  授权(CredentialPolicy)   │     │
   │  JSON 编解码 / 错误模型    │     │
   └──┬───────────────┬───────┘     │
      │               │             │
┌─────▼──────┐ ┌──────▼──────────┐  │
│ Catalog    │ │ iceberg::       │  │
│ (§4,§5)    │ │  Metadata 模型   │  │
│ ITableCata-│ │  requirements/  │  │
│ logStore   │ │  updates/迁移校验│  │
│ Object 实现 │ │  Avro 读取器     │  │
└─────┬──────┘ └──────┬──────────┘  │
      │ .sys 默认后端   │ 表桶所在后端  │
      ▼               ▼             ▼
        IStorageBackend（BucketRouter 解析）
```

三条边界：

- **`ITableCatalogStore`**（§4.2）：目录实体的读/列/条件写。首期实现
  `ObjectCatalogStore`（`.sys` 对象 + `PutCondition`）；预留 duostore-meta 实现。
- **`iceberg::TableMetadata`**（§7）：纯函数式的 JSON 模型、校验与 update 应用，
  不感知存储，可单测穷举。
- **`TableBucketGuard`**（§8）：给 S3 面的守卫钩子，只读 `TableBucketStore`
  快照，零额外 IO。

## 4. 数据模型与键空间布局

### 4.1 表桶

`TableBucketStore = SysConfigStore<TableBucketTraits>`（`kPrefix = "tables/"`），
一桶一对象 `.sys/tables/<bucket>`：

```json
{"version":1,"enabled":true,"reserved_prefix":".lights3-table/",
 "properties":{},"created_unix":1757548800}
```

启用是 root 专属（与 `?website` / `?cors` 同两级模型）：REST 扩展
`PUT /iceberg/v1/buckets/{bucket}`、管理面 `PUT /-/admin/tables/buckets/<bucket>`、
CLI `lights3-ctl tables enable <bucket>` 三个入口同一实现。启用前提：桶存在、
桶内**没有**以保留前缀开头的对象、桶不是 `.sys`。禁用（`DELETE`）前提：目录
中无 namespace。表桶快照随请求钉住（`SysConfigStore::snapshot()`），守卫查表
为 O(log n) 内存查找。

### 4.2 目录实体与 `ITableCatalogStore`

```cpp
// src/tables/catalog_store.h
struct NamespaceEntry { int version=1; std::vector<std::string> levels; std::map<std::string,std::string> properties;
                        int64_t created_unix, updated_unix; };
enum class TableState { Active, Renaming, Deleted };
struct TableEntry {
    int version = 1;
    std::string table_id;            // uuid v4，存储身份（路径、日志键）
    std::string table_uuid;          // Iceberg table-uuid（register 时采用外来值）
    std::string location;            // s3://<bucket>/<ns-path>/<table>
    std::string metadata_location;   // 桶内相对 key，恒在保留前缀下（§4.4）
    std::string version_token;       // "t-" + 22 字符 base64url 随机；每次提交换新
    uint64_t    generation = 1;      // 单调递增，也是 metadata 文件名的序号
    int         format_version = 2;
    TableState  state = TableState::Active;
    std::string rename_id;           // Renaming 时的 intent id
    int64_t     created_unix, updated_unix;
};
struct CommitRecord {                // 提交日志（§5.3）
    std::string commit_id, table_id, expected_token, new_token,
                prev_metadata_location, new_metadata_location;
    std::string status;              // STAGED | COMMITTED
    nlohmann::json request_digest;   // sha256(规范化 requirements+updates)，重放比对用
    int64_t created_unix;
};
struct Versioned<T> { T value; std::string etag; };

struct ITableCatalogStore {
    virtual Task<std::optional<Versioned<NamespaceEntry>>> get_namespace(bucket, levels) = 0;
    virtual Task<ListPage<NamespaceEntry>>  list_namespaces(bucket, parent_levels, PageCursor) = 0;
    virtual Task<void> put_namespace(bucket, NamespaceEntry, PutCondition) = 0;
    virtual Task<void> delete_namespace(bucket, levels) = 0;             // 空校验由上层做
    virtual Task<std::optional<Versioned<TableEntry>>> get_table(bucket, levels, name) = 0;
    virtual Task<ListPage<std::string>>     list_tables(bucket, levels, PageCursor) = 0;
    virtual Task<std::string>  put_table(bucket, levels, name, TableEntry, PutCondition) = 0;  // → 新 etag
    virtual Task<void> delete_table(bucket, levels, name) = 0;
    virtual Task<std::optional<Versioned<CommitRecord>>> get_commit(bucket, table_id, commit_id) = 0;
    virtual Task<void> put_commit(bucket, table_id, CommitRecord, PutCondition) = 0;
    virtual Task<std::vector<CommitRecord>> list_commits(bucket, table_id) = 0;   // 诊断/恢复用
    // rename intent（§5.6）
    virtual Task<std::optional<Versioned<RenameIntent>>> get_rename(bucket, id) = 0;
    virtual Task<void> put_rename(bucket, RenameIntent, PutCondition) = 0;
    virtual Task<void> delete_rename(bucket, id) = 0;
};
```

接口粒度是**语义级**（实体，而非裸 KV）——与 duostore 的 `IMetaStore`
同一取舍（[storage/duostore-design.md](storage/duostore-design.md) §2.1），
这样 duostore-meta 实现可以把"列表"落到有序迭代、把 CAS 落到事务，而不用
模拟对象语义。

### 4.3 `.sys` 键布局（`ObjectCatalogStore`）

```text
.sys/tables/<bucket>                                    表桶标记（§4.1，SysConfigStore）
.sys/tables-catalog/<bucket>/ns/<l1>/<l2>/_ns.json      NamespaceEntry（多级 namespace 每级一段）
.sys/tables-catalog/<bucket>/ns/<l1>/<l2>/tbl/<table>.json    TableEntry（当前指针）
.sys/tables-catalog/<bucket>/commits/<table_id>/<commit_id>.json   CommitRecord
.sys/tables-catalog/<bucket>/renames/<rename_id>.json   RenameIntent
```

- `_ns.json` 与 `tbl/` 都是保留段，标识符规则（§4.5）保证用户名字撞不上。
- 列子 namespace = `list_objects(prefix="…/ns/<l1>/", delimiter="/")` 的
  `common_prefixes` 去掉 `tbl/`；列表 = `prefix="…/tbl/"`。两者都是现有
  `ListOptions` 的 prefix + delimiter + start_after，天然有序、天然分页。
- **namespace 存在性按证据判定**（借鉴 RustFS）：`_ns.json` 存在，或其前缀下有
  任何表/子 namespace，都算存在；`create_namespace a.b` 不要求 `a` 有显式记录。
- 每个实体对象的 body 里带 `bucket` / `levels` / `name`，读取时与对象 key 重算
  比对，不符即当损坏（防止 `duostore dump/load` 之类工具搬错位置）。
- 目录状态跟其他 `.sys` 状态一样落在**默认后端**；表桶本身可路由到任何后端。
  `.sys` 对象受 `validate_bucket_name(allow_reserved=false)` 保护，S3 面碰不到。

### 4.4 表桶内布局（保留前缀）

```text
<bucket>/
├── .lights3-table/                                   保留前缀：S3 面只读（§8.1）
│   └── <l1>/<l2>/<table>/metadata/
│       ├── 00001-<uuid>.metadata.json                CreateTable 生成
│       ├── 00002-<uuid>.metadata.json                每次 CommitTable +1
│       └── …
└── <l1>/<l2>/<table>/                                 表 location（默认）
    ├── data/…parquet                                  引擎经 S3 面写
    └── delete/…                                       v2 删除文件
```

- 元数据**永远**在保留前缀下，与 `location` 解耦：引擎只能通过目录换指针，
  S3 面无法伪造/覆盖 metadata.json；`metadata-log[].metadata-file` 与
  `metadata-location` 对客户端回 `s3://<bucket>/.lights3-table/…`，引擎经 S3
  GET 读取（读放行）。
- 默认 `location = s3://<bucket>/<l1>/<l2>/<table>`：人类可浏览的 Hive 风格
  布局。CreateTable 可显式给 `location`（须在本桶内、不在保留前缀下、不与
  既有表的 location 互为前缀）。rename 不搬数据，也不改 location（Iceberg 语义）。
- 与 RustFS 的差别：RustFS 默认 `s3://<bucket>/tables/<table_id>`（不透明）。
  这里选可读路径是因为 lights3 的 policy `prefixes` 就是 key 前缀，
  `prefixes: ["sales/"]` 同时限住 namespace `sales` 下的目录操作与数据对象（§6.2）。

### 4.5 标识符规则

与 RustFS 一致（对齐 AWS S3 Tables 的字符集，避免 URL 与 key 编码歧义）：
段 `^[a-z0-9]([a-z0-9_-]{0,62}[a-z0-9])?$`（1–64 字节），namespace 多级用
`%1F` 分隔（URL）/ `levels[]`（JSON），总长 ≤512；表名单段；保留段 `_ns.json`
/ `tbl` 不可能命中（含 `.`）。违反 → 400 `BadRequestException`。

### 4.6 分页 token

`pageToken` = base64url（无 padding）的
`{"v":1,"ctx":"<sha256(op\0bucket\0ns)前16字节hex>","after":"<start_after key>"}`，
≤4 KiB；`ctx` 不符 → 400；`pageSize` 缺省与上限 1000（`tables.max_page_size`）；
两者都缺省时返回全量（与 RustFS 同）。

## 5. 提交协议（单表 CAS）

### 5.1 原语

`IStorageBackend::put_object(..., PutCondition)`：`if_none_match`（不存在才
写，否则 `PreconditionFailed`）与 `if_match_etag`（ETag 相等才写，不等
`PreconditionFailed`、不存在 `NoSuchKey`），契约要求"检查与提交在后端自己的
原子点内、失败不留痕"（`storage/backend.h`），六个后端均已实现且有
`backend_suite` 覆盖。目录只用这一原语，**不引入锁服务**。

进程内再加一层 per-(bucket, table) 的 `AsyncMutex` 作为快路径：同一网关上的并发
提交串行化，避免无谓的 CAS 失败与 metadata 对象堆积；跨网关正确性仍只靠 CAS。

### 5.2 CommitTable 流程

```text
 0 授权：Action::Write on (bucket, "<ns-path>/<table>")（§6.2）
 1 读指针 (entry, etag0)；state != Active → 404（Deleted）/ 503（Renaming，§5.6）
 2 幂等（§5.3）：get_commit(table_id, commit_id) 命中 → 按状态重放或 409
 3 读当前 metadata.json（≤ tables.metadata_max_size，默认 50 MiB）
 4 iceberg::check_requirements(current, requirements)   失败 → 409 CommitFailedException
 5 next = iceberg::apply_updates(current, updates)       非法 → 400 / 406
   iceberg::check_transition(current, next)              失败 → 409
   iceberg::check_snapshots(next - current, bucket)      缺文件/越界 → 409（§7.4）
 6 new_loc = .lights3-table/<ns>/<t>/metadata/{gen+1:05}-<commit_id 或新 uuid>.metadata.json
   put_object(bucket, new_loc, json(next), if_none_match)   ← 幂等重放会撞到，此时读回比对
 7 put_commit(CommitRecord{STAGED, expected_token=entry.version_token, new_token, …}, if_none_match)
   已存在 → 回到 2（并发的同 commit_id）
 8 next_entry = entry{metadata_location=new_loc, version_token=new_token, generation+1, updated}
   put_table(..., next_entry, if_match_etag=etag0)         ← 唯一原子点
   PreconditionFailed → 409 CommitFailedException（best-effort 删 new_loc，删不掉留给维护）
 9 put_commit(record{COMMITTED}, {})                       结果忽略（见 §5.4）
10 响应 {metadata-location: s3://…/new_loc, metadata: next}
```

步骤 3–5 是纯计算（线程池上做 JSON 与 Avro 解析），6–9 是四次对象写，其中
只有 8 决定成败。`version_token` 由 `getentropy` 16 字节生成（与 upload_id 同源，
`storage/multipart.cc`），不可枚举。

### 5.3 幂等与重试

客户端（PyIceberg 的 `commit-id`、DuckDB 的重试）可能对同一提交重放。规则：

| 已有 CommitRecord | 指针状态 | 处理 |
| --- | --- | --- |
| COMMITTED，`request_digest` 相同 | 任意 | 幂等成功：回当时的 `new_metadata_location` 与其 metadata |
| STAGED，指针的 `version_token == record.new_token` | 已前进 | finalize（写 COMMITTED），幂等成功 |
| STAGED，指针的 `version_token == record.expected_token` | 未前进 | 上次死在 7–8 之间：继续从 8 走（new_loc 已存在则读回比对内容） |
| STAGED，指针两者都不是 | 已被别的提交超过 | 409：这次 STAGED 记录是死记录，诊断可清理 |
| 任意，`request_digest` 不同 | — | 409 `CommitFailedException "commit-id reused with a different payload"` |

`request_digest` = sha256(规范化 JSON 的 requirements + updates)，不存原文。
`idempotency-key`（body 字段，与 RustFS 同）与 `commit-id` 等价处理：有
`commit-id` 用它做记录名，否则用 `sha256(idempotency-key)`，两者都没有则生成
uuid（此时不可重放，与规范一致）。

### 5.4 崩溃窗口矩阵

| 崩溃点 | 现场 | 后果与自愈 |
| --- | --- | --- |
| 6 后 | 多一个无人引用的 metadata 文件 | 维护的元数据保留规则视为孤儿，过安全窗口后删（§9） |
| 7 后 | STAGED 记录 + 未引用 metadata | 重放走 §5.3 第 3 行；无人重放则诊断列为 `StagedBeforeTableUpdate`，可清理 |
| 8 后 9 前 | 指针已前进，记录仍 STAGED | **对外已提交**。重放走 §5.3 第 2 行；`GET …/catalog/diagnostics` 报 `FinalizationRequired`，`POST …/catalog/recovery` 只补写记录、不动指针 |
| 8 的 PreconditionFailed 后删 new_loc 失败 | 冗余 metadata 文件 | 同第 1 行 |

与 RustFS 的关键一致点：**指针 CAS 之后的一切失败都不能把成功变成失败**。

### 5.5 多网关

目录状态在默认后端的 `.sys`，其原子性 = 该后端 `PutCondition` 的跨进程原子性。
按 [deployment.md §5.1](deployment.md) 的矩阵：duostore（redis / tikv meta +
rados data）与 cloudproxy 做默认后端时跨网关成立；localfs / xlocalfs /
rocksdb / sqlite 默认后端只能单网关。`--check-config` 在 `tables.enabled` 且
多网关标志（`read_lease` 等）出现在非共享默认后端上时 WARN，与
multi-gateway-multipart §4 ④ 同一防线。表桶自身可在任何后端（数据面无跨网关
状态）。

### 5.6 rename 两阶段

指针按名字寻址，rename 本质是"在 CAS 世界里搬一个对象"。照搬 RustFS 的
intent 方案，去掉反向索引那一步：

```text
① put_rename(intent{Prepared, src, dst, src_etag}, if_none_match)
② src.state = Renaming, rename_id           put_table(src, if_match src_etag)
③ dst = src 的拷贝 {state=Active}           put_table(dst, if_none_match)    已存在 → 回滚 ②，409 AlreadyExists
④ src.state = Deleted 墓碑                  put_table(src, if_match)
⑤ delete_rename(intent)
```

读者遇到 `Renaming` 回 503（`Retry-After: 1`），写者先驱动未完成的 intent
（幂等，从 intent 记录的阶段继续）。墓碑对象保留到维护清理（默认 24h），期间
同名 create 用 `if_match(tombstone_etag)` 覆盖。表在两个 namespace 之间 rename
需要目标 namespace 存在（按证据判定）。

### 5.7 drop

`DELETE …/tables/{t}`：写 `Deleted` 墓碑（`if_match`），不删 metadata 与数据
（`purgeRequested=false`，规范默认）。`purgeRequested=true`：首期 406
`UnsupportedOperationException`；§14 ④ 落地维护后改为"先墓碑，再入队 purge
作业删保留目录与 `location` 前缀"。`DELETE …/namespaces/{ns}`：证据判定为非空
→ 409 `NamespaceNotEmptyException`。

## 6. REST 接口

### 6.1 路由与寻址

- 只在 **path-style** 下识别：`req.path` 以 `tables.path_prefix + "/v1/"`
  （默认 `/iceberg/v1/`）开头，且未启用 vhost 寻址命中 → 进入
  `tables::RestApi::dispatch`。分派点在 `S3Service::dispatch` 的 `/-/` 内部面
  分支之后、STS `POST /` 之前（`service.cc` 的路径阶梯）。
- **桶名保留**：`tables.enabled` 时 CreateBucket 拒绝名字等于前缀首段
  （`iceberg`，及 `_iceberg` 若开了别名）的桶（`InvalidBucketName`），启动时
  已存在同名桶 → WARN 并说明该桶 `v1/` 前缀被遮蔽。RustFS 未处理这一点。
- `{prefix}` = 桶名（客户端 `warehouse`/`prefix` 都填桶名），`GET /v1/config?warehouse=<bucket>`
  回 `overrides.prefix = <bucket>`。
- 监听器：走数据监听器（引擎与数据面同一端点）；`http.admin_port` 分离时
  表桶启用的管理面入口在 admin 监听器，REST 面不受影响。
- JSON 请求体上限 `tables.request_max_size`（默认 1 MiB，CommitTable 的
  updates 可含整份 schema），复用 `handlers::read_json_object` 的形态但独立上限。

### 6.2 认证与授权

- SigV4：`SigV4Authenticator::verify_impl(req, service, …)` 已按参数校验
  service，目录路径接受 `s3`（默认）与 `s3tables`（`tables.accept_s3tables_signing`，
  默认 true）；payload 三形态照旧（PyIceberg 走 botocore 带
  `x-amz-content-sha256`，DuckDB 走 `UNSIGNED-PAYLOAD`）。会话凭证
  （`L3SA`）照常验 token。
- 授权复用 `CredentialPolicy::allows(bucket, key, action)`，把目录操作映射为
  S3 三元组，**不新增动作族**：

| 目录操作 | (bucket, key) | Action |
| --- | --- | --- |
| GET /config、GET buckets/{w} | (w, "") | Read |
| list/get/exists namespace | (w, "<ns-path>/") | Read |
| create/update/drop namespace | (w, "<ns-path>/") | Write / Write / Delete |
| list tables | (w, "<ns-path>/") | Read |
| load / exists table、refs GET、metadata-location GET | (w, "<ns-path>/<t>") | Read |
| create / register / commit、metadata-location PUT | (w, "<ns-path>/<t>") | Write |
| drop table | (w, "<ns-path>/<t>") | Delete |
| rename | 源 Delete + 目标 Write | |
| PUT/DELETE buckets/{w}（启用/禁用表桶） | root 专属 | |

于是 `prefixes: ["sales/"]` 的凭证只能操作 namespace `sales`（及子级）与其
数据对象；`readonly: true` 的凭证只能 load / list / 读数据。租户凭证只见本
租户的表桶（`require_tenant_bucket`）。每个目录请求进审计日志，`api_name` 为
`Iceberg.<Op>`。

### 6.3 端点表

标准端点（进 `GET /v1/config` 的 `endpoints`）：

| 方法 路径（`/iceberg/v1` 之后） | 语义 | 阶段 |
| --- | --- | --- |
| GET `/config` | CatalogConfig | ① |
| GET / POST `/{w}/namespaces` | 列（`parent`、分页）/ 建 namespace | ① |
| GET / HEAD / DELETE `/{w}/namespaces/{ns}` | 读 / 存在性(204) / 删 | ① |
| POST `/{w}/namespaces/{ns}/properties` | `{removals, updates}` → `{updated, removed, missing}` | ① |
| GET / POST `/{w}/namespaces/{ns}/tables` | 列（分页）/ CreateTable | ① |
| POST `/{w}/namespaces/{ns}/register` | RegisterTable | ① |
| GET / HEAD / POST / DELETE `/{w}/namespaces/{ns}/tables/{t}` | LoadTable / 存在性 / CommitTable / DropTable | ① |
| POST `/{w}/tables/rename` | RenameTable | ① |
| GET `/{w}/namespaces/{ns}/tables/{t}/credentials` | LoadCredentials | ② |
| GET/POST/HEAD/DELETE `…/views…`、POST `/{w}/views/rename` | Iceberg views（format v1） | ⑥ |

扩展端点（不进 `endpoints`，与 RustFS / AWS 语义对齐）：

| 方法 路径 | 语义 | 阶段 |
| --- | --- | --- |
| PUT / GET / DELETE `/{w}/buckets/{bucket}` | 启用 / 查看 / 禁用表桶（root） | ① |
| GET / PUT `…/tables/{t}/metadata-location` | AWS `GetTableMetadataLocation` / `UpdateTableMetadataLocation` 形态：`{metadataLocation, versionToken}`；PUT 只换指针，仍走 §5.2 的 3–5 校验（metadata 须已在保留目录下，即只对 register 后的服务端拷贝或维护产物有意义） | ① |
| GET `…/tables/{t}/catalog/diagnostics`、POST `…/catalog/recovery` | §5.4 的诊断与修复 | ③ |
| GET/PUT `…/tables/{t}/maintenance/config`、POST `…/maintenance/{plan,run}`、GET `…/maintenance/jobs/{id}` | §9 | ④ |

不实现即 406 `UnsupportedOperationException`（而非 501 XML）：`stage-create`、
`purgeRequested=true`（④ 前）、`overwrite: true` 的 register、format-version 3、
`/transactions/commit`、`/plan`、`/tasks`、`/sign`、`/metrics`（`reportMetrics`
接收后丢弃回 204，PyIceberg 会调用）、`/oauth/tokens`。

### 6.4 `GET /v1/config`

```json
{"defaults":{"warehouse":"<bucket 或空>","lights3.catalog-prefix":"/iceberg/v1"},
 "overrides":{"namespace-separator":"%1F","prefix":"<bucket>"},
 "endpoints":["GET /v1/{prefix}/namespaces", "...仅已实现端点..."]}
```

`overrides.prefix` 让客户端在只给 `warehouse=<bucket>` 时自动得到 `{prefix}`
（PyIceberg / Spark 的标准行为）。不广播 `idempotency-key-lifetime`。

### 6.5 CreateTable / RegisterTable / LoadTable

- **CreateTable** `{name, location?, schema, partition-spec?, write-order?, stage-create?, properties?}`：
  服务端重排 id（schema-id 0、字段 id 从 1、spec-id 0、分区字段 id 从 1000、
  sort-order 0/1）、`properties["format-version"]` 取出（默认 2，1 可、3 → 406）、
  生成初始 metadata（`snapshots: []`、`refs: {}`、v2 加 `last-sequence-number: 0`；
  v1 另同步 `schema` / `partition-spec` 镜像字段）、写 `00001-<uuid>.metadata.json`
  （`if_none_match`）、`put_table(if_none_match 或 if_match 墓碑)`。已存在 →
  409 `AlreadyExistsException`。响应 200 `LoadTableResult`。
- **RegisterTable** `{name, metadata-location, overwrite?}`：`metadata-location`
  须为 `s3://<w>/…` 本桶内 key（可在保留前缀外，即引擎自己写的旧表）；服务端读
  取（≤上限，`.json`/`.json.gz`）、`validate_supported_metadata`、`check_snapshots`
  全量校验，然后**复制**为保留目录下 `00001-<uuid>.metadata.json` 并以此为指针
  （原文件不动），`table_uuid` 采用原值，`location` 采用 metadata 的 `location`。
  这就是"指针只指保留目录"不变量的来源；AWS 的 register 亦要求 metadata 在表桶内。
- **LoadTable**：`?snapshots=all|refs`（`refs` 裁剪到 refs 与 current 引用的
  快照）；响应 `{metadata-location, metadata, config}`；`config` 给引擎数据面
  提示：`{"s3.path-style-access":"true","s3.region":"<auth.region>",
  "lights3.credential-vending":"disabled|supported","lights3.table-location":"<location>"}`
  （`s3.endpoint` 不给：网关不知道自己的对外地址，客户端本就要配）。
  支持 `ETag`（= 指针 etag）与 `If-None-Match` → 304，规范可选项，PyIceberg 0.9+
  会用，省一次 metadata 读取。

### 6.6 错误模型

`tables::RestError{status, type, message}` → `{"error":{"message","type","code"}}`，
`Content-Type: application/json`。映射：

| 来源 | status / type |
| --- | --- |
| 标识符 / JSON 形态 / 未知 requirement 或 update | 400 `BadRequestException` |
| SigV4 失败 | 401 `NotAuthorizedException`（`InvalidAccessKeyId`）/ 403 `ForbiddenException`（其余） |
| policy / 租户拒绝 | 403 `ForbiddenException` |
| namespace / table / view 不存在 | 404 `NoSuchNamespaceException` / `NoSuchTableException` / `NoSuchViewException` |
| 非表桶 | 404 `NoSuchNamespaceException`，message 说明 "bucket is not table-enabled"（RustFS 用 400；这里选 404 让 PyIceberg 的 `namespace_exists` 语义正确） |
| create 撞名 | 409 `AlreadyExistsException` |
| drop 非空 namespace | 409 `NamespaceNotEmptyException` |
| requirement 失败 / CAS 失败 / 迁移不变量失败 / 幂等 payload 不同 | 409 `CommitFailedException` |
| `Renaming` 中、后端 `SlowDown` | 503 `ServiceUnavailableException` + `Retry-After` |
| 不支持 | 406 `UnsupportedOperationException` |
| 存储层其他错误 | 500 `RESTException`；**CAS 之后**的错误一律 200（§5.4） |
| 步骤 8 结果不明（超时/连接断） | 500 `CommitStateUnknownException`：客户端不得盲目重试写，应 load 后判断 |

错误经 `public_error` 同款脱敏（`InternalError` 不带内部文本）。

## 7. Iceberg 元数据模型与校验（`src/tables/iceberg/`）

纯函数模块，输入输出都是 `nlohmann::json`（保持字段顺序无关；序列化用
`dump()` 稳定输出以便重放比对）。

### 7.1 requirements

全部 8 种：`assert-create`（表已存在恒 409）、`assert-table-uuid`、
`assert-ref-snapshot-id`（`snapshot-id: null` 表示 ref 须不存在）、
`assert-last-assigned-field-id`（对 `last-column-id`）、`assert-current-schema-id`、
`assert-last-assigned-partition-id`、`assert-default-spec-id`、
`assert-default-sort-order-id`。未知 type → 400。

### 7.2 updates

| action | 处理要点 |
| --- | --- |
| `assign-uuid` | 与现值不同 → 409 |
| `upgrade-format-version` | 1→2 允许（给快照补 `sequence-number: 0`）；降级 400；3 → 406 |
| `add-schema` | 服务端**重分配** `schema-id` 为下一个 id；新字段 id 不得 ≤ 旧 `last-column-id`；类型提升只允许 int→long、float→double、decimal 放宽精度 |
| `set-current-schema` | `-1` = 最近加入的 |
| `add-spec` / `set-default-spec` | 服务端分配 spec-id 与分区字段 id；源字段须在当前 schema |
| `add-sort-order` / `set-default-sort-order` | 空 fields → order-id 0 |
| `add-snapshot` | v2 拒绝 `manifests`（须 `manifest-list`）；`sequence-number` > `last-sequence-number`；id 唯一；parent 存在；`summary.operation ∈ {append, overwrite, delete, replace}` |
| `set-snapshot-ref` | `main` 同步 `current-snapshot-id` 与 `snapshot-log` |
| `remove-snapshots` / `remove-snapshot-ref` | 顺带清悬空 ref 与统计；删掉 main 指向的快照则 `current-snapshot-id = -1` |
| `set-location` | 须在本桶内、不在保留前缀 |
| `set-properties` / `remove-properties` | 值只能是字符串；`format-version` 不可经属性改 |
| `set-statistics` / `remove-statistics` / `set-partition-statistics` / `remove-partition-statistics` | 记录；文件存在性在 §7.4 校验 |
| `remove-partition-specs` / `remove-schemas` | 不得删当前/默认 |
| `add-encryption-key` / `remove-encryption-key` | 406（v3） |

应用完成后：裁剪 `snapshot-log` 中不再存在的快照、追加 `metadata-log`
（`{timestamp-ms, metadata-file: 旧位置}`，保留最近 `tables.metadata_log_keep`
条，默认 100，对应 Iceberg `write.metadata.previous-versions-max`）、更新
`last-updated-ms`。

### 7.3 迁移不变量（current → next）

`table-uuid` 不变；`format-version` 不降；`last-column-id` / `last-partition-id`
/ `last-sequence-number` 非递减；已有 schema / partition-spec / sort-order /
snapshot 逐字节相同（只能增删，不能改）；`current-schema-id` 等引用的 id 必须
存在。失败 409（这是并发写者绕过 requirements 时的最后防线）。

### 7.4 快照图校验与 Avro

对 `next` 新增的每个快照：读 `manifest-list`（Avro OCF）→ 每个 manifest
（Avro OCF）→ 每个 `status ∈ {ADDED(1), EXISTING(0)}` 的数据/删除文件：
`head_object` 存在、`file_size_in_bytes` 相符（有值时）、路径在本桶且不在保留
前缀。manifest 的 `manifest_length` 与对象大小相符。统计文件读头 4 字节验
`PFA1` / `PAR1` 魔数。并发度 `tables.validate_concurrency`（默认 16，`when_all`
分批）；总上限：manifest ≤ 10 000 / 文件引用 ≤ 1 000 000 / 单 Avro ≤ 128 MiB。

提交冲突复核（借鉴 RustFS）：以父快照的活跃文件集为基线，新快照重复 ADD 同一
文件或 DELETE 非活跃文件 → 409；`append` 不得携带删除文件或删数据文件。

**Avro 读取器**：树内实现 `avro_reader.{h,cc}`（只读）：OCF 头（magic、
`avro.schema`、`avro.codec`、sync marker）、schema JSON（nlohmann）驱动的解码
（null/boolean/int/long/float/double/bytes/string/record/array/map/union/
fixed/enum，Iceberg manifest 只用这些）、codec `null` 与 `deflate`（zlib，
`find_package(ZLIB)` 缺失时 deflate → 不校验、记 WARN 并在响应 `config` 里报
`lights3.snapshot-validation: "skipped-codec"`）；`snappy` / `zstd` 同样降级。
不引入 avro-cpp（依赖 Boost 与 fmt，且只用到读的十分之一）。

阶段 ① 先做"manifest-list 对象存在 + 大小"的浅校验，Avro 深校验在 ③ 补齐。

## 8. 数据面集成

### 8.1 表桶守卫（`TableBucketGuard`）

挂在 `S3Service::dispatch` 授权点之后（bucket / key / `r->action` 已知），
`tables.enabled` 且桶在 `TableBucketStore` 快照中时生效：

| 请求 | 规则 |
| --- | --- |
| PUT / DELETE / POST(DeleteObjects 的每个 key) / CopyObject 目标 / 全部 multipart 写步骤，key 在保留前缀下 | 400 `InvalidRequest "Object key is reserved for the table catalog"` |
| GET / HEAD / List 保留前缀 | 放行（引擎读 metadata.json 与列 metadata 目录） |
| PutObjectTagging 等就地改 meta 到保留前缀 | 同第一行 |
| DeleteBucket | 目录非空（有 namespace）或保留前缀下有对象 → 409 `BucketNotEmpty`；空表桶删除时顺带删 `.sys/tables/<bucket>` 与 `.sys/tables-catalog/<bucket>/`（先删目录状态再删桶，崩溃只留孤儿状态，`fsck` 报告） |
| 表桶被禁用 | 守卫解除；保留前缀下对象变普通对象 |

DeleteObjects 的批内混合：保留 key 逐条回 `Error` 而非整批失败（与 AWS
逐条语义一致）。守卫是内存查表，不增加 IO。

### 8.2 lifecycle 排除

`LifecycleRunner::run_once` 迭代桶时跳过表桶（`Expiration` 与
`AbortIncompleteMultipartUpload` 都跳过——后者是刻意的：引擎的大文件 multipart
在慢链路上可能超过天级），启动与 `PutBucketLifecycle` 时对表桶 WARN
"lifecycle rules are ignored on table buckets"。表文件的删除只由 §9 维护做。

### 8.3 用量、配额、审计

表数据对象走既有 PutObject 路径，用量/配额照常；目录写的 metadata.json 也经
`put_object`，用量按普通对象计入表桶（AWS 也把 metadata 计费在表桶）；
`QuotaExceeded` 时 CommitTable 回 409 `CommitFailedException`（message 带
`QuotaExceeded`），客户端会重试——因此在步骤 6 之前先做一次 `check_quota`
预检以早失败。审计事件 `tables.<op>`，与现有 `AuditEvent` 同形。

### 8.4 凭证下发（阶段 ②）

`X-Iceberg-Access-Delegation` 含 `vended-credentials` 且 `tables.credential_vending: true`：
`CredentialStore::mint_session(parent, ttl, narrowed_policy)` 新增第三参数——
会话 policy = 父 policy ∩ `{buckets:[w], prefixes:["<location 相对前缀>/", ".lights3-table/<ns>/<t>/metadata/"], readonly: 父只读或请求只读}`；
持久化格式已有 `policy` 字段无需改。响应 `storage-credentials: [{prefix: "<location>", config: {s3.access-key-id, s3.secret-access-key, s3.session-token, expiration-ms}}]`
与 `config` 同款键（PyIceberg 两处都认），`Cache-Control: no-store, private`。
TTL `tables.credential_ttl`（默认 900 s，钳 60–3600）。会话不能再下发、不能
AssumeRole（既有规则）。`GET …/credentials` 同一实现。

## 9. 维护（阶段 ④）

复用 `AdminJobs`：新增 `JobOp::{TablePlan, TableRun, TablePurge}`，路径
`POST /-/admin/tables/<bucket>/<ns-path>/<table>/<op>`（root）与 REST 扩展
`POST …/maintenance/{plan,run}`（Write 权限），CLI
`lights3-ctl tables plan|run <bucket> <ns.table>`；每表同一时刻一个作业
（`JobInProgress`）；作业在专用线程跑（AdminJobs 既有模型），状态
`GET` 同路径。

**plan**（只读，输出 JSON 报告）：

1. 元数据保留集 = 当前指针 ∪ `metadata-log` ∪ 保留目录下最近
   `retain_recent_metadata_files` 个 ∪ 受保护快照 ref 可达；其余为候选，
   候选还须 `last_modified ≤ now − safety_window`（默认 15 min）。
2. 快照过期：`max_snapshot_age_ms` / `min_snapshots_to_keep`（表属性
   `history.expire.*` 优先，冲突则人工复核）；ref 引用的、带 ref 级保留字段的
   → 人工复核。产出一份 `remove-snapshots` 的 updates 与其对应的
   `assert-ref-snapshot-id` requirements（plan 与指针绑定）。
3. 孤儿：`location` 下 `data/`、`delete/` 与保留目录列举，减去从**保留集内全部
   metadata**可达的文件（§7.4 的 Avro 遍历）；任一 manifest 解析失败 → 本轮不删
   任何数据文件（fail-closed）。

**run**：重读指针，与 plan 时的 `version_token` 不符 → 失败 `StalePlan`；
快照过期以 §5.2 的标准提交执行（因此也进 commit log）；然后按 plan 删元数据
候选与孤儿，每个删除前再 `head` 复核 mtime；限速 `max_bytes_per_sec` 沿用
AdminJobs 参数。`tables.maintenance.delete_enabled: false`（默认）时 run 只做
快照过期提交、不删文件。

**periodic**：`tables.maintenance.scan_interval`（默认 0 = 关）时，一个
`LifecycleRunner` 同款 runner 对所有表桶顺序 plan+run；多网关下与 lifecycle
一样各实例都跑（删除幂等，安全窗口挡在途提交），或按 `gc_enabled` 约定只在一台
开。**purge**（drop 后）：删保留目录 + `location` 前缀 + 墓碑，走同一作业框架。

不做 compaction 执行（需 Parquet 读写）；plan 可输出 binpack 候选组供外部引擎
（Spark `rewrite_data_files`）使用，属 ⑥ 可选。

## 10. 配置

```yaml
tables:
  enabled: false                   # 总开关；关闭时 /iceberg/v1 落回 S3 路由（桶名 iceberg 不保留）
  path_prefix: /iceberg            # REST 前缀（+ /v1）
  compat_prefix: ""                # 可选别名，如 /_iceberg（⑥）
  accept_s3tables_signing: true    # 目录路径接受 credential scope service = s3tables
  reserved_prefix: .lights3-table/ # 表桶内保留前缀（启用后不可改）
  metadata_max_size: 50MiB
  request_max_size: 1MiB
  metadata_log_keep: 100
  max_page_size: 1000
  validate_concurrency: 16
  credential_vending: false
  credential_ttl: 900s
  maintenance:
    scan_interval: 0s              # 0 = 只手动
    safety_window: 15m
    retain_recent_metadata_files: 10
    delete_enabled: false
    tombstone_ttl: 24h
```

`Config` 新增 `TablesConfig`，`config.cc` 的 `root.find("tables")` 块 +
`check_range`；全部 **restart-only**（进 `docs/config-reload.md §4` 表与
`app.cc` 的 `requires_restart` 比对）。`--check-config` 校验：前缀以 `/` 开头
且不含 `/v1`、`reserved_prefix` 以 `/` 结尾且不以 `.sys` 开头、多网关误配 WARN（§5.5）。

## 11. 构建接入与依赖

- 源码：`src/tables/{catalog_store.h, object_catalog_store.cc, catalog.cc,
  rest_api.cc, rest_error.h, bucket_guard.cc, maintenance.cc,
  iceberg/{metadata.cc, requirements.cc, updates.cc, snapshots.cc, avro_reader.cc}}`，
  编入 `lights3_core`，CMake 选项 `LIGHTS3_TABLES`（默认 ON；OFF 时不编译、
  配置 `tables.enabled: true` 报错），仿 `LIGHTS3_CLOUDPROXY` 的门控。
- 依赖：nlohmann/json（已有）、OpenSSL sha256（已有 `core/util/crypto.h`）、
  `getentropy`（已有）、zlib（可选，`find_package(ZLIB)`，缺失则 deflate Avro
  降级）。**不新增子模块**。
- CLI：`lights3-ctl tables enable|disable|status <bucket>`、`tables list <bucket>`、
  `tables plan|run|purge`；`lights3 fsck` 增加"目录状态与表桶对账"检查项
  （孤儿 `.sys/tables-catalog/<bucket>/` 、指针指向不存在的 metadata）。

## 12. 可插拔演进：duostore-meta 后备

`ITableCatalogStore` 的第二实现把实体放进 duostore 的 `IMetaStore`（新增
column family / 键前缀 `tc/`），CAS 落到 meta 事务（redis Lua / tikv 2PC /
rocksdb WriteBatch），列表落到有序迭代；单表提交在一个事务里完成 7–9 三步，
崩溃窗口矩阵退化为单行。适用于默认后端就是 duostore 的部署，配置
`tables.catalog_backing: object | duostore`。不做 RustFS 的"整份目录一个快照
对象"模式（64 MiB 上限与单锁不是 lights3 的路线）。

## 13. 可观测性与测试

指标（`MetricsScope{feature=tables}`）：`lights3_tables_requests_total{op,status}`、
`lights3_tables_commit_seconds{result=ok|conflict|error}`、
`lights3_tables_commit_conflicts_total`、`lights3_tables_validation_files_total`、
`lights3_tables_maintenance_deleted_bytes_total`、`lights3_tables_finalization_gaps`
（gauge，诊断扫描时更新）。访问日志 `api_name = Iceberg.<Op>`，慢请求阈值照旧。

测试（[testing.md](testing.md) 体系内）：

| 层 | 内容 |
| --- | --- |
| `tests/unit/test_tables_iceberg.cc` | §7 纯函数：8 种 requirement × 通过/失败；每种 update 的合法/非法样例；迁移不变量；Avro 读取器对 PyIceberg 生成的 manifest 固件（二进制固件入库，≤100 KiB） |
| `tests/unit/test_tables_catalog.cc` | `ObjectCatalogStore` on `MemoryBackend`：namespace 证据判定、分页、rename 五步逐点崩溃（复用 `core/fault.h` 门面按尝试序号注入 put/delete 失败）、§5.4 四个崩溃窗口 + 重放矩阵 §5.3、并发 100 提交单胜者 |
| `tests/unit/test_tables_rest.cc` | 进程内 `S3Service`：端点形态、错误模型、policy 前缀限权、租户隔离、`s3tables` 签名、表桶守卫（保留前缀 PUT 400 / GET 200、DeleteBucket 409、lifecycle 跳过）、`/config` 的 `endpoints` 与路由表一致性（表驱动，防漂移） |
| `tests/unit/multi_gateway_suite.h` 追加 | 两个 `S3Service` 共享 `MemoryBackend`（或 redis/tikv duostore）：跨网关提交冲突收敛、rename 恢复由另一网关驱动 |
| `tests/e2e/run_e2e.sh` 新段 | bash + curl：启用表桶 → 建 namespace/table → 手工 CommitTable（add-snapshot 指向预置固件）→ 冲突 409 → drop；六驱动矩阵照跑 |
| `scripts/tables/pyiceberg_smoke.py`、`duckdb_smoke.py`（opt-in） | 借鉴 RustFS 脚本序列：建表 → append 2 行 → 重载 scan → 冲突/幂等探针 → 维护 plan/run → drop；依赖 `pyiceberg[pyarrow]` / `duckdb`，由 `LIGHTS3_TABLES_SMOKE=1` 触发，ctest 标签 `tables-smoke`，默认 SKIP（与 tikv 的 `LIGHTS3_TEST_PD_ADDR` 同一模式）；通过后把客户端版本记入 [testing.md §6](testing.md) |
| Spark / Trino | 只给配置模板（§13 附录），人工验证记录进 testing.md；不声称自动化 |

客户端配置模板（写进用户文档，与 RustFS 脚本一致的键）：

```text
PyIceberg:  type=rest uri=http://gw:9000/iceberg warehouse=<bucket>
            rest.sigv4-enabled=true rest.signing-name=s3 rest.signing-region=<region>
            s3.endpoint=http://gw:9000 s3.path-style-access=true s3.access-key-id/... s3.region
DuckDB:     CREATE SECRET (TYPE s3, PROVIDER config, ENDPOINT 'gw:9000', URL_STYLE 'path', USE_SSL false, ...)
            ATTACH '<bucket>' AS c (TYPE iceberg, ENDPOINT 'http://gw:9000/iceberg',
                                    AUTHORIZATION_TYPE 'sigv4', SECRET ..., SIGV4_SERVICE 's3',
                                    STAGE_CREATE_TABLES false, DISABLE_MULTI_TABLE_COMMIT true)
Spark:      spark.sql.catalog.c=org.apache.iceberg.spark.SparkCatalog  .type=rest  .uri=http://gw:9000/iceberg
            .warehouse=<bucket>  .io-impl=org.apache.iceberg.aws.s3.S3FileIO  .s3.endpoint=…  .s3.path-style-access=true
            .rest.sigv4-enabled=true  .rest.signing-name=s3  .rest.signing-region=<region>
Trino:      iceberg.catalog.type=rest  iceberg.rest-catalog.uri=…  .warehouse=<bucket>  .security=SIGV4
            .signing-name=s3  fs.native-s3.enabled=true  s3.endpoint=…  s3.path-style-access=true
```

## 14. 实施拆分

按依赖顺序；每步独立可合并、有单测，做完把本节对应行改为"已实现 + 日期"。

| 步骤 | 内容 | 验收 |
| --- | --- | --- |
| ① 目录核心 + REST 最小集 | `TablesConfig`；`TableBucketStore`；`ITableCatalogStore` + `ObjectCatalogStore`；§7.1–7.3 的 metadata 模型（浅快照校验）；§5.2/5.3 提交协议；端点：config / buckets / namespaces 全部 / tables list-create-load-commit-drop-exists-rename-register / metadata-location；错误模型；dispatch 分支与桶名保留；表桶守卫的保留前缀只读与 DeleteBucket 守卫；审计与指标 | `test_tables_iceberg` / `test_tables_catalog` / `test_tables_rest` 通过；e2e 新段通过；PyIceberg 冒烟（本机人工）建表 + append + scan 通过 |
| ② 权限与凭证 | policy 三元组映射表（§6.2）、租户隔离、`s3tables` 签名名、`mint_session` 收窄参数与 `vended-credentials` 协商、`GET …/credentials`、lifecycle 排除 | 前缀限权与只读凭证用例；下发凭证在前缀内 Put/Get/Delete 通过、前缀外 403 |
| ③ 深校验与诊断 | Avro 读取器；§7.4 快照图与冲突复核；`catalog/diagnostics` / `recovery`；`fsck` 对账项；ETag/If-None-Match on LoadTable | manifest 固件用例；崩溃窗口矩阵用例；rename 恢复用例 |
| ④ 维护 | `JobOp::Table*`、plan/run/purge、`purgeRequested=true`、周期 runner、CLI、`tombstone_ttl` 清理 | 保留集/安全窗口/StalePlan 用例；DuckDB 冒烟（本机人工） |
| ⑤ 多网关与文档 | `multi_gateway_suite` 追加；`--check-config` 误配 WARN；deployment.md §5 矩阵加"表目录"列；本文转成实现文档 + `docs/en/` 同步；README 索引 | 双网关用例；文档评审 |
| ⑥ 可选 | views；`/_iceberg/v1` 别名；`reportMetrics` 落审计；compaction 候选规划输出；duostore-meta 后备（§12） | 按需 |

## 15. 明确不做

| 条目 | 理由 |
| --- | --- |
| AWS S3 Tables 控制面 API（`s3tables.<region>` 端点、ARN 寻址、`CreateTableBucket` 等 50 个操作） | 引擎生态全部走 Iceberg REST；AWS 自己也让引擎经 REST 端点访问表桶。RustFS 同样不声称。只借用 `GetTableMetadataLocation` / `UpdateTableMetadataLocation` 的语义与字段名 |
| 多表事务 `/transactions/commit` | 需要跨对象原子性，对象 CAS 做不到；duostore-meta 后备（§12）落地后再议 |
| 服务端扫描规划 `/plan` `/tasks`、`remote-signing` `/sign` | 引擎本地规划已足够；remote-signing 需要网关替客户端签 S3 请求，与 policy 模型冲突 |
| compaction / 删除文件重写的执行 | 需要 Parquet 读写与行级语义，交给引擎（Spark `rewrite_data_files`） |
| durable-strong 单快照后备 | 见 §12 |
| Delta Lake / Hudi、内置 SQL | 超出对象存储网关范围 |
| 跨区域双活写 | 单表单写者是 Iceberg 的前提；多网关只在共享同一份目录状态时成立（§5.5） |
