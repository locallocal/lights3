# 步骤 ①：目录核心 + REST 最小集

> 状态：**已实现（2026-09-12，分支 feat/s3-tables-step1）**。对应设计 §3–§7.3、§8.1、§10、§11、§14 ①。
> 实现与本稿的差异见文末 §18。
> 完成后 PyIceberg 能建 namespace / 表、append、reload、scan；DuckDB 能 ATTACH
> 后建表插入（深校验在 ③ 补，此步只做快照的浅校验）。

## 1. 目标与验收

| 验收项 | 判据 |
| --- | --- |
| 单测 | `test_tables_iceberg.cc`、`test_tables_catalog.cc`、`test_tables_rest.cc` 全绿，进 `unit_tests` |
| e2e | `run_e2e.sh` 新段 `tables`：启用表桶 → namespace → 建表 → 手工 CommitTable（add-snapshot 指向预置固件）→ 陈旧 token 409 → drop |
| 冒烟 | 本机人工：PyIceberg `create_table` + `append` 2 行 + `scan` 2 行；`--check-config` 通过 |
| 不回归 | 六驱动 e2e、`unit_tests` 既有用例、`tables.enabled: false` 时行为与现在逐字节一致（默认关闭） |

## 2. 文件清单

新增（全部编入 `lights3_core`，`if(LIGHTS3_TABLES)` 门控）：

```text
src/tables/config.h                 TablesConfig（也可放 core/config.h，见 §4）
src/tables/identifier.{h,cc}        标识符校验、namespace levels ⇄ 路径 / %1F
src/tables/catalog_store.h          实体结构 + ITableCatalogStore
src/tables/object_catalog_store.{h,cc}   .sys 对象实现
src/tables/table_bucket_store.{h,cc}     TableBucketTraits / TableBucketStore
src/tables/catalog.{h,cc}           Catalog：业务层（namespace / table 生命周期、提交协议）
src/tables/rest_error.h             RestError + to_json
src/tables/rest_api.{h,cc}          RestApi：路由、JSON 编解码、handler
src/tables/bucket_guard.{h,cc}      TableBucketGuard（S3 面守卫）
src/tables/iceberg/metadata.{h,cc}  TableMetadata 读写、初始 metadata、版本字段同步
src/tables/iceberg/requirements.{h,cc}
src/tables/iceberg/updates.{h,cc}
src/tables/iceberg/transition.{h,cc}
src/tables/iceberg/snapshots.{h,cc} 此步只有浅校验（manifest-list 存在 + 大小）
tests/unit/test_tables_iceberg.cc
tests/unit/test_tables_catalog.cc
tests/unit/test_tables_rest.cc
tests/fixtures/tables/               PyIceberg 生成的 metadata.json / manifest-list 固件
```

修改：

| 文件 | 改动 |
| --- | --- |
| `CMakeLists.txt` | `option(LIGHTS3_TABLES "..." ON)`；`target_sources` + `LIGHTS3_TABLES` 宏；`unit_tests` 加三个测试文件；e2e 段 |
| `src/core/config.h/.cc` | `TablesConfig` + `Config::tables` + `root.find("tables")` 解析与 `check_range` |
| `src/app/app.h/.cc` | 装配 `TableBucketStore` / `ObjectCatalogStore` / `Catalog` / `RestApi` / `TableBucketGuard`，`set_tables(...)`；reload 比对加 `tables` 为 restart-only |
| `src/s3/service.h/.cc` | `set_tables_api(std::shared_ptr<tables::RestApi>)`、`set_table_bucket_guard(...)`；dispatch 阶梯加 `/iceberg/v1/` 分支；授权点之后加守卫调用 |
| `src/s3/handlers/buckets.cc` | CreateBucket 拒绝保留桶名；DeleteBucket 前调守卫 + 成功后删目录状态 |
| `src/s3/handlers/objects.cc` `multipart.cc` | 无改动（守卫在 dispatch 统一做；DeleteObjects 的逐 key 判定见 §9） |
| `src/s3/errors.h` | 无新码（目录错误不走 X-macro，见 §8） |
| `docs/*` | 见 README 的"文档回写" |

## 3. 标识符（`identifier.h`）

```cpp
namespace lights3::tables {
// 设计 §4.5：段 ^[a-z0-9]([a-z0-9_-]{0,62}[a-z0-9])?$，namespace 总长 ≤ 512
constexpr size_t kMaxSegment = 64, kMaxNamespace = 512;
bool valid_segment(std::string_view s);
// 解析 URL 路径段：percent-decode 后按 U+001F 切分；无 %1F 且含 '.' 时按 '.' 切分（PyIceberg 旧行为）。
// 抛 RestError(400, "BadRequestException")
std::vector<std::string> parse_namespace_path(std::string_view raw_segment);
// JSON "namespace": ["a","b"] → levels，逐段校验
std::vector<std::string> parse_namespace_json(const nlohmann::json& arr);
std::string ns_path(const std::vector<std::string>& levels);      // "a/b"（key 路径与 .sys 路径共用）
std::string ns_display(const std::vector<std::string>& levels);   // "a.b"（日志/审计）
}
```

`valid_segment` 顺带排除 `_ns.json` / `tbl`（含 `.` 或作为保留词），实现里直接
`if (s == "tbl") return false;`。

## 4. 配置（`TablesConfig`）

放在 `src/core/config.h`（与 `LifecycleConfig` 并列，避免 core 依赖 tables 头）：

```cpp
struct TablesConfig {
    bool enabled = false;
    std::string path_prefix = "/iceberg";        // 须以 '/' 开头、不含 "/v1"、不以 '/' 结尾
    std::string compat_prefix;                   // ⑥
    bool accept_s3tables_signing = true;         // ②
    std::string reserved_prefix = ".lights3-table/";   // 以 '/' 结尾，不以 ".sys" 开头
    size_t metadata_max_size = 50 * 1024 * 1024;
    size_t request_max_size = 1024 * 1024;
    int metadata_log_keep = 100;
    int max_page_size = 1000;
    int validate_concurrency = 16;               // ③
    bool credential_vending = false;             // ②
    int credential_ttl_sec = 900;                // ②
    struct Maintenance {                         // ④
        int scan_interval_sec = 0;
        int safety_window_sec = 900;
        int retain_recent_metadata_files = 10;
        bool delete_enabled = false;
        int tombstone_ttl_sec = 86400;
    } maintenance;
    bool operator==(const TablesConfig&) const = default;
};
```

`config.cc` 在 `root.find("lifecycle")` 块之后加 `root.find("tables")` 块：
`parse_bool` / `parse_size` / `parse_duration_sec` + `check_range`
（`metadata_max_size` 1 MiB–512 MiB、`max_page_size` 1–10000、
`metadata_log_keep` 1–10000、`credential_ttl` 60–3600）。校验规则
（抛 `std::runtime_error("config: tables.path_prefix ...")`）：

- `path_prefix` 非空、首字符 `/`、末字符非 `/`、不含 `/v1`；
- `reserved_prefix` 非空、末字符 `/`、不以 `.sys` 开头、不含 `..`；
- `enabled && !LIGHTS3_TABLES`（编译期宏缺失）→ 报错 "built without tables support"。

`app.cc` 的 reload 比对（`cmp(...)` 列表）加 `cmp(a.tables != b.tables, "tables")`
→ requires_restart；`docs/config-reload.md §4` 加一行。

## 5. 表桶标记（`table_bucket_store.h`）

```cpp
struct TableBucketEntry {
    int version = 1;
    bool enabled = true;
    std::string reserved_prefix;                 // 启用时从配置快照，之后不随配置改
    std::map<std::string, std::string> properties;
    int64_t created_unix = 0;
    bool operator==(const TableBucketEntry&) const = default;
};
struct TableBucketTraits {
    using Entry = TableBucketEntry;
    static constexpr std::string_view kPrefix = "tables/";
    static constexpr const char* kName = "tables";
    static std::string serialize(const Entry&);
    static std::optional<Entry> deserialize(const std::string& bucket, const std::string& body);
    static bool differs(const Entry& a, const Entry& b) { return a != b; }
};
using TableBucketStore = s3::SysConfigStore<TableBucketTraits>;
```

装配与 `cors_store_` 完全同款：`sync_wait(TableBucketStore::load(router.default_backend()))`、
`start_background(pool_, cfg_.auth.sync_interval_sec)`；`tables.enabled: false`
时**不加载**（store 指针为空 = 功能关闭，dispatch 分支与守卫都以空指针短路）。

## 6. 目录实体与 `ITableCatalogStore`（`catalog_store.h`）

结构体按设计 §4.2，补齐两个：

```cpp
struct RenameIntent {                       // §5.6，本步只写 intent 与五步，恢复驱动在 ③
    int version = 1;
    std::string rename_id;
    std::vector<std::string> src_levels, dst_levels;
    std::string src_name, dst_name;
    std::string src_etag;                   // 阶段 ② 用的 if_match
    enum class Stage { Prepared, SourceFenced, DestinationWritten, SourceTombstoned } stage;
    int64_t created_unix = 0;
};
template <class T> struct ListPage { std::vector<T> items; std::string next_after; };  // next_after 空 = 结束
struct PageCursor { std::string after; int limit = 1000; };
```

接口签名（`Task<>` 全部）：

```cpp
struct ITableCatalogStore {
    virtual ~ITableCatalogStore() = default;
    virtual Task<std::optional<Versioned<NamespaceEntry>>> get_namespace(std::string_view bucket, const Levels&) = 0;
    virtual Task<ListPage<std::string>> list_child_namespaces(std::string_view bucket, const Levels& parent, PageCursor) = 0;
    virtual Task<bool> namespace_has_children(std::string_view bucket, const Levels&) = 0;   // 证据判定：任一子 ns / 表
    virtual Task<void> put_namespace(std::string_view bucket, const NamespaceEntry&, storage::PutCondition) = 0;
    virtual Task<void> delete_namespace(std::string_view bucket, const Levels&) = 0;
    virtual Task<std::optional<Versioned<TableEntry>>> get_table(std::string_view bucket, const Levels&, std::string_view name) = 0;
    virtual Task<ListPage<std::string>> list_tables(std::string_view bucket, const Levels&, PageCursor) = 0;
    virtual Task<std::string> put_table(std::string_view bucket, const Levels&, std::string_view name, const TableEntry&, storage::PutCondition) = 0;
    virtual Task<void> delete_table(std::string_view bucket, const Levels&, std::string_view name) = 0;
    virtual Task<std::optional<Versioned<CommitRecord>>> get_commit(std::string_view bucket, std::string_view table_id, std::string_view commit_id) = 0;
    virtual Task<void> put_commit(std::string_view bucket, std::string_view table_id, const CommitRecord&, storage::PutCondition) = 0;
    virtual Task<std::vector<CommitRecord>> list_commits(std::string_view bucket, std::string_view table_id) = 0;
    virtual Task<std::optional<Versioned<RenameIntent>>> get_rename(std::string_view bucket, std::string_view id) = 0;
    virtual Task<std::vector<RenameIntent>> list_renames(std::string_view bucket) = 0;
    virtual Task<void> put_rename(std::string_view bucket, const RenameIntent&, storage::PutCondition) = 0;
    virtual Task<void> delete_rename(std::string_view bucket, std::string_view id) = 0;
    virtual Task<void> delete_bucket_state(std::string_view bucket) = 0;    // DeleteBucket 后清 tables-catalog/<bucket>/
};
```

### 6.1 `ObjectCatalogStore`

```cpp
class ObjectCatalogStore final : public ITableCatalogStore {
public:
    explicit ObjectCatalogStore(std::shared_ptr<storage::IStorageBackend> sys_backend);   // router.default_backend()
    // 键（设计 §4.3）
    static std::string ns_key(bucket, levels)      // tables-catalog/<b>/ns/<a>/<b>/_ns.json
    static std::string tbl_key(bucket, levels, name) // tables-catalog/<b>/ns/<a>/<b>/tbl/<name>.json
    static std::string commit_key(bucket, table_id, commit_id)
    static std::string rename_key(bucket, id)
private:
    Task<std::optional<std::pair<std::string, std::string>>> read(std::string_view key);   // {body, etag}；NoSuchKey → nullopt
    Task<std::string> write(std::string_view key, std::string body, storage::PutCondition);  // → etag；PreconditionFailed 原样抛
    Task<void> ensure_sys_bucket();   // 同 SysConfigStore
};
```

实现要点：

- `read`：`get_object(kSysBucketName, key, nullopt)` + `read_all`（上限 1 MiB）；
  `stream.meta.etag` 即 CAS 用的 etag。body 反序列化后校验身份字段
  （`bucket` / `levels` / `name`）与 key 重算一致，不一致抛 `S3Error(InternalError,
  "catalog entry at <key> does not describe itself")`。
- `write`：`put_object(kSysBucketName, key, meta{content_type=application/json}, StringBodyReader, cond)`，
  返回 `PutResult.etag`。**不**吞 `PreconditionFailed`，由 `Catalog` 决定语义。
- `list_child_namespaces(parent)`：`ListOptions{prefix = "<ns-dir>/", delimiter = "/", start_after = cursor.after, max_keys = limit + 2}`；
  取 `common_prefixes`，去掉尾部 `/`，过滤 `tbl`；`next_after` = 最后一个 common prefix
  （`is_truncated` 时）。注意 `_ns.json` 本身出现在 `objects` 里，忽略。
- `namespace_has_children`：`ListOptions{prefix="<ns-dir>/", max_keys=2}`，结果里除
  `_ns.json` 外还有任何 key 即 true。
- `list_tables`：`prefix = "<ns-dir>/tbl/"`，key 去前缀与 `.json` 后缀。
- `delete_bucket_state`：分页 list `tables-catalog/<bucket>/` 逐个 `delete_object`。
- `list_commits`：`prefix = "tables-catalog/<b>/commits/<table_id>/"`，全量读（③ 诊断用；
  提交数按维护清理有界）。

### 6.2 序列化

每个实体一个 `to_json` / `from_json`（`.cc` 内），字段名 snake_case，`version` 必须
为 1，未知字段拒绝（`from_json` 里遍历 keys 比对白名单，与 RustFS 的
`deny_unknown_fields` 同义）。`TableEntry.state` 序列化为 `"ACTIVE" | "RENAMING" | "DELETED"`。

## 7. Iceberg 元数据模块（`src/tables/iceberg/`）

纯函数，无 IO；异常统一 `RestError`。

### 7.1 `metadata.h`

```cpp
namespace lights3::tables::iceberg {
using Json = nlohmann::json;
struct MetadataLimits { int metadata_log_keep = 100; };
// 解析 + 结构校验（format-version ∈ {1,2}，必需字段齐全，schemas/specs/orders 非空且 id 唯一，
// current-schema-id 等引用存在，snapshots id 唯一）。抛 RestError(400/406)
Json parse_and_validate(std::string_view text, size_t max_size);
struct CreateTableInput { std::string name; Json schema; std::optional<Json> partition_spec, write_order;
                          std::map<std::string, std::string> properties; int format_version = 2;
                          std::string location; std::string table_uuid; int64_t now_ms; };
// 设计 §6.5：重排 id、生成初始 metadata（含 v1 镜像字段）
Json initial_metadata(const CreateTableInput&);
// v1: 同步 schema / partition-spec 镜像；v2: 删除镜像字段
void synchronize_version_fields(Json& md);
// 追加 metadata-log 与 last-updated-ms、裁剪 snapshot-log（apply_updates 之后调用）
void finish_transition(Json& md, std::string_view prev_metadata_location, int64_t now_ms, const MetadataLimits&);
// 稳定序列化（键排序，无空白）——重放比对与 sha256 都用它
std::string canonical(const Json&);
int64_t current_snapshot_id(const Json&);   // -1 = 无
}
```

`initial_metadata` 的 id 分配规则（设计 §6.5）：schema 字段 id 深度优先从 1 起
（struct / list / map 的嵌套字段也分配，`element-id` / `key-id` / `value-id`）、
`last-column-id` = 最大 id；`schema-id: 0`；spec：`spec-id: 0`，`field-id` 从 1000
起，`source-id` 须存在于 schema，`transform` 只认
`identity|bucket[N]|truncate[N]|year|month|day|hour|void`；`last-partition-id`
= 999 或最大 field-id；sort-order：`fields` 空 → `order-id: 0`，否则 1，
`default-sort-order-id` 同值。

### 7.2 `requirements.h`

```cpp
// 设计 §7.1；失败抛 RestError(409, "CommitFailedException", "<why>")；未知 type 抛 400
void check_requirements(const Json& current, const Json& requirements, bool table_exists);
```

`assert-ref-snapshot-id`：`ref` 缺失 → 400；`snapshot-id` 为 null 时要求 `refs[ref]`
不存在；否则要求 `refs[ref].snapshot-id` 相等。

### 7.3 `updates.h`

```cpp
struct ApplyOptions { std::string bucket; std::string reserved_prefix; };
// 设计 §7.2：返回新 metadata（拷贝后就地改）；非法 400，v3/加密 406，冲突（assign-uuid 不同、快照 id 重复、parent 不存在）409
Json apply_updates(const Json& current, const Json& updates, const ApplyOptions&);
```

逐 action 的实现顺序：先 `assign-uuid` / `upgrade-format-version`，再按数组顺序
处理其余（Iceberg 规范要求按序）。`add-schema` 的 `last-column-id` 用请求里的
`last-column-id`（若给）与新 schema 最大 id 取大者；`add-snapshot` 若 v2 且缺
`sequence-number` → 400。`set-snapshot-ref` 目标快照必须存在（409）。
`set-location` 调 `validate_location(bucket, reserved_prefix, loc)`
（`identifier.h` 里的共用函数：`s3://<bucket>/` 前缀、不含 `..`、不以保留前缀开头）。

### 7.4 `transition.h`

```cpp
// 设计 §7.3；失败 409
void check_transition(const Json& current, const Json& next);
```

比较"既有对象逐字节不变"用 `canonical()` 后的字符串比对，按 id 建 map。

### 7.5 `snapshots.h`（本步：浅校验）

```cpp
struct SnapshotCheckContext { storage::IStorageBackend& backend; std::string bucket; std::string reserved_prefix; };
// 对 next 中新增的每个 snapshot：manifest-list 路径在桶内且不在保留前缀下，head_object 存在；
// 缺失 → 409 CommitFailedException "manifest list <path> does not exist"
Task<void> check_new_snapshots_shallow(const SnapshotCheckContext&, const Json& current, const Json& next);
```

路径 → key：接受 `s3://<bucket>/<key>` 与 `s3a://`；其他 scheme 或其他桶 → 409。

## 8. 错误模型（`rest_error.h`）

```cpp
struct RestError : std::exception {
    int status; std::string type; std::string message;
    std::vector<std::pair<std::string, std::string>> headers;   // Retry-After 等
    RestError(int s, std::string t, std::string m) : status(s), type(std::move(t)), message(std::move(m)) {}
    const char* what() const noexcept override { return message.c_str(); }
};
// 工厂，避免到处写字符串
RestError bad_request(std::string m);        // 400 BadRequestException
RestError not_found_ns(std::string m);       // 404 NoSuchNamespaceException
RestError not_found_table(std::string m);    // 404 NoSuchTableException
RestError already_exists(std::string m);     // 409 AlreadyExistsException
RestError commit_failed(std::string m);      // 409 CommitFailedException
RestError ns_not_empty(std::string m);       // 409 NamespaceNotEmptyException
RestError unsupported(std::string m);        // 406 UnsupportedOperationException
RestError unavailable(std::string m);        // 503 ServiceUnavailableException + Retry-After: 1
RestError commit_state_unknown(std::string m); // 500 CommitStateUnknownException
nlohmann::json to_json(const RestError&);    // {"error":{"message","type","code"}}
// s3::S3Error → RestError（存储层错误；设计 §6.6 表）
RestError from_s3_error(const s3::S3Error&, std::string_view path);
```

`from_s3_error`：`AccessDenied|SignatureDoesNotMatch|RequestTimeTooSkewed` → 403
`ForbiddenException`；`InvalidAccessKeyId|InvalidToken|ExpiredToken` → 401
`NotAuthorizedException`；`PreconditionFailed` → 409 `CommitFailedException`；
`NoSuchKey|NoSuchBucket` → 404（按 path 含 `/tables/` 选类型）；`SlowDown` → 503；
`QuotaExceeded` → 409 `CommitFailedException`（message 前缀 `QuotaExceeded:`）；
`InternalError` 与其余 → 500 `RESTException`（message 脱敏为 "internal error"）。

## 9. 业务层 `Catalog`（`catalog.h`）

```cpp
class Catalog {
public:
    Catalog(std::shared_ptr<ITableCatalogStore> store, std::shared_ptr<TableBucketStore> buckets,
            storage::BucketRouter router, std::shared_ptr<ThreadPool> pool, const TablesConfig& cfg,
            MetricsScope metrics);

    // 表桶（root 专属，权限在 RestApi / admin 判）
    Task<TableBucketEntry> enable_bucket(std::string_view bucket);
    Task<void> disable_bucket(std::string_view bucket);
    const TableBucketEntry* table_bucket(const TableBucketStore::Snapshot&, std::string_view bucket) const;

    // namespaces
    Task<NamespaceEntry> create_namespace(bucket, Levels, props);
    Task<std::optional<NamespaceEntry>> load_namespace(bucket, Levels);   // 证据判定：无 _ns.json 但有子项 → 合成空属性条目
    Task<bool> namespace_exists(bucket, Levels);
    Task<ListPage<Levels>> list_namespaces(bucket, parent, PageCursor);
    Task<NamespacePropsResult> update_namespace_properties(bucket, Levels, removals, updates);
    Task<void> drop_namespace(bucket, Levels);

    // tables
    struct LoadedTable { TableEntry entry; std::string etag; nlohmann::json metadata; };
    Task<LoadedTable> create_table(bucket, Levels, CreateTableRequest);
    Task<LoadedTable> register_table(bucket, Levels, name, std::string metadata_location);
    Task<LoadedTable> load_table(bucket, Levels, name);
    Task<bool> table_exists(bucket, Levels, name);
    Task<ListPage<std::string>> list_tables(bucket, Levels, PageCursor);
    struct CommitRequest { std::string commit_id; nlohmann::json requirements, updates; };
    Task<LoadedTable> commit_table(bucket, Levels, name, CommitRequest);
    Task<TableEntry> update_metadata_location(bucket, Levels, name, std::string new_location, std::string expected_token);
    Task<void> rename_table(bucket, Levels src, name, Levels dst, dst_name);
    Task<void> drop_table(bucket, Levels, name);
    Task<bool> catalog_empty(bucket);        // DeleteBucket 守卫用
    Task<void> forget_bucket(bucket);        // DeleteBucket 成功后清状态
private:
    AsyncSemaphore& table_mutex(bucket, table_id);   // 进程内快路径（设计 §5.1）：map<string, unique_ptr<AsyncSemaphore(1)>>，mutex 保护
    std::string metadata_key(const TableBucketEntry&, Levels, name, uint64_t gen, std::string_view token);
    static std::string new_token();          // "t-" + base64url(getentropy 16B)
    static std::string new_uuid();           // uuid v4 文本，getentropy
};
```

### 9.1 `create_table`

```text
1 tb = table_bucket(snapshot, bucket)，无 → not_found_ns("bucket is not table-enabled")
2 namespace_exists(levels) 否 → not_found_ns
3 校验 name；get_table 存在且 Active → already_exists；Deleted 墓碑 → cond = if_match(etag)；否则 cond = if_none_match
4 input.location = req.location 或 "s3://<bucket>/<ns-path>/<name>"；validate_location；
  与既有表 location 互为前缀的检查：list_tables 同 namespace 逐个 get_table 比较（同 ns 内的表才可能撞；跨 ns 用户显式给 location 时不查，文档说明）
5 md = iceberg::initial_metadata(...)；co_await pool->schedule() 包住 JSON 工作
6 key = "<reserved>/<ns-path>/<name>/metadata/00001-<table_id>.metadata.json"
  put_object(bucket 所在后端, key, json, if_none_match)；PreconditionFailed → already_exists（并发建同名）
7 entry{table_id, table_uuid, location, metadata_location=key, version_token=new_token(), generation=1, ...}
  etag = store->put_table(..., cond)；PreconditionFailed → 删 key（best effort）→ already_exists
8 co_return {entry, etag, md}
```

### 9.2 `commit_table`（设计 §5.2 逐步对应）

```text
 a  tb / namespace / (entry, etag0) = get_table；无或 Deleted → not_found_table；Renaming → unavailable
 b  auto permit = co_await table_mutex(bucket, entry.table_id).acquire()
 c  幂等：if (!commit_id.empty()) rec = get_commit(table_id, commit_id) → 按设计 §5.3 表处理（见 9.3）
 d  cur = parse_and_validate(read_all(get_object(bucket, entry.metadata_location)))
    entry.table_uuid != cur["table-uuid"] → RESTException 500 "persisted metadata does not match catalog entry"
 e  pool->schedule(); check_requirements(cur, req.requirements, /*exists=*/true);
    next = apply_updates(cur, req.updates, {bucket, reserved}); check_transition(cur, next);
    finish_transition(next, entry.metadata_location, now_ms, limits)
 f  co_await check_new_snapshots_shallow(ctx, cur, next)
 g  quota 预检（②：此步先留 hook `quota_check_` 函数指针，为空则跳过）
 h  token = commit_id 为 uuid 格式 ? commit_id : new_uuid()
    new_key = metadata_key(tb, levels, name, entry.generation + 1, token)
    put_object(bucket 后端, new_key, canonical(next), if_none_match)
      PreconditionFailed → 读回比对 canonical 相同 → 继续（重放）；不同 → commit_failed("generated metadata location already contains a different commit")
 i  record{commit_id 或 sha256(idempotency) 或 token, STAGED, expected_token=entry.version_token, new_token=new_token(), prev=entry.metadata_location, new=new_key, request_digest}
    put_commit(if_none_match)；PreconditionFailed → 回到 c（并发同 commit_id）
 j  next_entry = entry; next_entry.metadata_location = new_key; version_token = record.new_token; generation++; updated_unix
    etag1 = put_table(..., if_match_etag = etag0)
      PreconditionFailed → best-effort delete_object(new_key)；commit_failed("table was updated concurrently")
      其他异常（超时/断连）→ commit_state_unknown（不删 new_key）
 k  record.status = COMMITTED; put_commit({})——异常只 LOG_WARN，不影响返回
 l  metrics.commit_seconds{ok}；co_return {next_entry, etag1, next}
```

`idempotency-key` 有而 `commit-id` 无：`commit_id = "ik-" + sha256_hex(key).substr(0, 32)`。
两者都无：`commit_id = token`（不可重放，但记录仍写，诊断可见）。

### 9.3 幂等重放（c 步）

```cpp
if (rec) {
    bool same = rec->value.request_digest == digest(req);
    if (!same) throw commit_failed("commit-id reused with a different payload");
    if (rec->value.status == "COMMITTED") co_return load_from_record(*rec);            // 读 rec.new_metadata_location
    // STAGED
    if (entry.version_token == rec->value.new_token) { finalize(*rec); co_return load_from_record(*rec); }
    if (entry.version_token == rec->value.expected_token) { /* 从 h 步继续，token 取 rec.new 的文件名 */ }
    else throw commit_failed("staged commit was superseded");
}
```

### 9.4 `register_table`

`metadata_location` → key（本桶内，任意位置）；`get_object` 读取（`.gz` 后缀：本步
不支持 gzip → 406 "compressed metadata is not supported"；③ 引入 zlib 后未放开，记 [todo §4](../todo.md)）；
`parse_and_validate`；`check_new_snapshots_shallow(ctx, /*current=*/empty, md)`（全部
快照都算新增）；写保留目录 `00001-<table_id>.metadata.json`（内容 = `canonical(md)`），
其余同 `create_table` 6–8；`table_uuid = md["table-uuid"]`、`location = md["location"]`
（须在本桶；不在 → 400）。

### 9.5 `rename_table`（五步，设计 §5.6）

本步写全五步与 intent；**恢复驱动**（读到 `Renaming` 的写者先驱动未完成 intent）
在 ③。此步的行为：读者遇 `Renaming` → `unavailable`；写者遇 `Renaming` → 也
`unavailable`（③ 改为驱动恢复）。

### 9.6 `drop_table` / `drop_namespace`

`drop_table`：`entry.state = Deleted; updated_unix = now; put_table(if_match etag)`；
PreconditionFailed → 重读一次再试一次（并发提交刚换了指针），仍失败 → `unavailable`。
`drop_namespace`：`namespace_has_children` → `ns_not_empty`；否则 `delete_namespace`
（无 `_ns.json` 且无子项 → `not_found_ns`）。**注意**：`Deleted` 墓碑算子项
（`namespace_has_children` 会看到 `tbl/x.json`），因此 drop 表后立刻 drop namespace
会 409——④ 的墓碑清理（`tombstone_ttl`）之前是预期行为；文档与错误 message 要说明
（"namespace still holds dropped-table tombstones"）。

## 10. REST 层（`rest_api.h`）

```cpp
class RestApi {
public:
    RestApi(std::shared_ptr<Catalog>, const TablesConfig&, MetricsScope);
    // 由 S3Service::dispatch 调用：req.path 已确认以 "<path_prefix>/v1/" 开头
    // verify 回调：dispatch 注入 verify_identity 与 is_root（RestApi 不依赖 service.h）
    struct Hooks {
        std::function<s3::VerifiedIdentity(http::HttpRequest&)> verify;
        std::function<bool(std::string_view ak)> is_root;
        std::function<void(const s3::AuditEvent&)> audit;
    };
    Task<http::HttpResponse> dispatch(http::HttpRequest& req, const s3::RequestContext& ctx, Hooks&, std::string& access_key, std::string_view& api_name);
    static std::vector<std::string> advertised_endpoints();   // 与 route 表同源（单测断言）
private:
    struct Route { std::string_view method; std::vector<std::string_view> pattern; /* "namespaces","{ns}","tables","{t}" */
                   s3::Action action; std::string_view name; Handler fn; };
    static std::span<const Route> routes();
    // 路径匹配：按 '/' 切段后与 pattern 逐段比对，"{x}" 捕获；捕获值 percent-decode
};
```

处理链（每个请求）：

```text
1 strip "<path_prefix>/v1" → segs；GET /config 单独处理（无 warehouse 段）
2 match routes()；无 → 404 {"error":{"type":"NoSuchResourceException"}}（规范无明确类型，沿 RustFS）；方法不匹配 → 405
3 ident = hooks.verify(req)（②：service 可为 s3|s3tables）；access_key = ident.access_key
4 授权：bucket = segs[0]；key = 见设计 §6.2 表；if (ident.policy && !policy->allows(bucket, key, r.action)) → 403
   buckets/{w} 的 PUT/DELETE：!is_root → 403
5 body：read_json_object 变体（上限 tables.request_max_size；空 body 按路由决定是否允许）
6 handler → HttpResponse{status, json}
7 catch RestError → to_json；catch S3Error → from_s3_error；catch std::exception → 500 RESTException（LOG_ERROR）
8 api_name = "Iceberg." + r.name；metrics.requests_total{op, status}；audit（写操作）
```

响应头：`Content-Type: application/json`；HEAD 存在性路由无 body（204/404）。

### 10.1 端点 → handler 对照（本步实现的）

| 路由 | handler | 请求/响应 JSON |
| --- | --- | --- |
| GET `/config` | `get_config` | 设计 §6.4；`?warehouse=` 出现即 `overrides.prefix` |
| PUT/GET/DELETE `/{w}/buckets/{w2}` | `enable/get/disable_bucket` | `w == w2` 否则 400；响应 `{"table-bucket","enabled","reserved-prefix","warehouse-location":"s3://<w>/","catalog-uri":"<path_prefix>/v1/<w>","properties"}` |
| GET `/{w}/namespaces` | `list_namespaces` | `?parent=`（%1F 切分）、`pageToken`、`pageSize` → `{"namespaces":[["a","b"]],"next-page-token":null}` |
| POST `/{w}/namespaces` | `create_namespace` | `{"namespace":[…],"properties":{}}` → 200 同形 |
| GET/HEAD/DELETE `/{w}/namespaces/{ns}` | `load/exists/drop_namespace` | GET → `{"namespace":[…],"properties":{}}`；HEAD 204；DELETE 204 |
| POST `…/{ns}/properties` | `update_ns_props` | `{"removals":[],"updates":{}}` → `{"updated":[],"removed":[],"missing":[]}`；同 key 出现在两侧 → 422 `UnprocessableEntityException` |
| GET `…/{ns}/tables` | `list_tables` | `{"identifiers":[{"namespace":[…],"name":"t"}],"next-page-token":null}` |
| POST `…/{ns}/tables` | `create_table` | 设计 §6.5；`stage-create: true` → 406 |
| POST `…/{ns}/register` | `register_table` | `{"name","metadata-location","overwrite"}`；`overwrite: true` → 406 |
| GET `…/tables/{t}` | `load_table` | `?snapshots=all|refs`；`{"metadata-location","metadata","config":{...}}` |
| HEAD `…/tables/{t}` | `table_exists` | 204 / 404 |
| POST `…/tables/{t}` | `commit_table` | 设计 §5.2；`identifier` 若给须与 URL 一致；`requirements`/`updates` 数组各 ≤ 1024 |
| DELETE `…/tables/{t}` | `drop_table` | `?purgeRequested=true` → 406 |
| POST `/{w}/tables/rename` | `rename_table` | `{"source":{"namespace","name"},"destination":{...}}` → 204 |
| GET/PUT `…/tables/{t}/metadata-location` | `get/put_metadata_location` | GET → `{"metadataLocation":"s3://…","versionToken":"…"}`；PUT `{"metadataLocation","versionToken"}` → 同形；token 不符 409 |
| POST `…/tables/{t}/metrics` | `report_metrics` | 读 body 后丢弃 → 204 |

`LoadTable.config`（设计 §6.5）本步固定输出
`{"s3.path-style-access":"true","s3.region":"<auth.region>","lights3.credential-vending":"disabled","lights3.table-location":"<location>"}`；
`region` 由 dispatch 通过 Hooks 传入（`auth_.region()`）。

`metadata-location` / `metadata-log[].metadata-file` 对外统一改写为 `s3://<bucket>/<key>`
（`Catalog::to_client_location`）。

### 10.2 分页 token

```cpp
struct PageToken { int v = 1; std::string ctx; std::string after; };
std::string encode_page_token(std::string_view op, std::string_view bucket, const Levels&, std::string_view after);
// 解析失败 / ctx 不符 → bad_request("pageToken does not match this list operation")
std::string decode_page_token(std::string_view token, std::string_view op, std::string_view bucket, const Levels&);
```

base64url 用 `util::base64_encode` 后做 `+/`→`-_` 替换并去 `=`（写在 `identifier.cc`
里的小函数，附单测）。

## 11. dispatch 接入（`service.cc`）

在 `else if (!addr.vhost && req.path == "/" && req.method == "POST")`（STS）**之前**加：

```cpp
} else if (tables_api_ && !addr.vhost && tables_prefix_match(req.path)) {
    // Iceberg REST catalog (docs/s3-tables-design.md §6.1): path-style only
    RestApi::Hooks hooks{[this](http::HttpRequest& r) { return verify_identity(r); },
                         [this](std::string_view ak) { return is_root(ak); },
                         [this](const AuditEvent& e) { audit(e); }};
    resp = co_await tables_api_->dispatch(req, ctx, hooks, access_key, api_name);
}
```

`tables_prefix_match`：`req.path` 等于 `<prefix>/v1` 或以 `<prefix>/v1/` 开头
（边界落在 `/`，与 `/-/admin/credentials/` 的注释同理）。`tables_api_` 为空 =
功能关闭，走既有路径。

`S3Service` 新成员：`std::shared_ptr<tables::RestApi> tables_api_;
std::shared_ptr<tables::TableBucketGuard> table_guard_; std::string tables_prefix_;`
与 `set_tables(api, guard, prefix)`。

**守卫调用点**：`RequestAuth auth{...}` 构造之后、`require_tenant_bucket` 之前：

```cpp
if (table_guard_ && !bucket.empty()) {
    const Route* r = match_route(req, key.empty() ? Scope::Bucket : Scope::Object);
    if (r) table_guard_->check(bucket, key, r->name, req);   // 同步，抛 S3Error
}
```

## 12. 表桶守卫（`bucket_guard.h`）

```cpp
class TableBucketGuard {
public:
    TableBucketGuard(std::shared_ptr<TableBucketStore> store, std::shared_ptr<Catalog> catalog);
    // 设计 §8.1 表：写类路由 + key 在保留前缀下 → S3Error(InvalidRequest, "Object key is reserved for the table catalog")
    // 判定依据是 Route::name（PutObject/CopyObject/DeleteObject/CreateMultipartUpload/UploadPart/UploadPartCopy/
    // CompleteMultipartUpload/AbortMultipartUpload/PutObjectTagging/DeleteObjectTagging）；GET/HEAD/List 放行
    void check(std::string_view bucket, std::string_view key, std::string_view route_name, const http::HttpRequest& req) const;
    // DeleteObjects 逐 key：true = 该 key 被保留（handler 生成 <Error><Code>InvalidRequest</Code>）
    bool reserved_key(std::string_view bucket, std::string_view key) const;
    // DeleteBucket 前：目录非空或保留前缀下有对象 → S3Error(BucketNotEmpty)
    Task<void> check_delete_bucket(std::string_view bucket, storage::IStorageBackend&) const;
    // CreateBucket 前：名字等于 path_prefix 首段（"iceberg"）→ S3Error(InvalidBucketName)
    void check_create_bucket(std::string_view bucket) const;
};
```

`delete_objects`（`objects.cc:569` 附近 `delete_one`）：循环前取 `table_guard_`，
对 `reserved_key(bucket, k)` 为真的 key 直接压入 `<Error>`（Code `InvalidRequest`），
不调后端。`delete_bucket`（`buckets.cc:155`）：第一行前 `if (table_guard_) co_await table_guard_->check_delete_bucket(bucket, backend)`；
成功后在 `forget` 列表里加 `catalog_->forget_bucket(bucket)`（顺序：先目录状态、
再 `.sys/tables/<bucket>`）。`create_bucket`：`validate_bucket_name` 之后
`if (table_guard_) table_guard_->check_create_bucket(bucket)`。

## 13. 装配（`app.cc`）

`start_server()` 内 `lifecycle_store_` 之后：

```cpp
#ifdef LIGHTS3_TABLES
if (cfg_.tables.enabled) {
    table_bucket_store_ = sync_wait(tables::TableBucketStore::load(router.default_backend()));
    auto cat_store = std::make_shared<tables::ObjectCatalogStore>(router.default_backend());
    tables_catalog_ = std::make_shared<tables::Catalog>(cat_store, table_bucket_store_, router, pool_, cfg_.tables,
                                                        MetricsScope(metrics_, {{"feature", "tables"}}));
    tables_api_ = std::make_shared<tables::RestApi>(tables_catalog_, cfg_.tables, ...);
    table_guard_ = std::make_shared<tables::TableBucketGuard>(table_bucket_store_, tables_catalog_);
    // 启动 WARN：已存在名为 <prefix 首段> 的桶（遮蔽）
}
#endif
...
service_->set_tables(tables_api_, table_guard_, cfg_.tables.path_prefix);
...
if (table_bucket_store_) table_bucket_store_->start_background(pool_, cfg_.auth.sync_interval_sec);
```

关闭顺序：`table_bucket_store_->shutdown_background()` 在线程池 join 之前（与其他
store 同位置）。`Catalog` 本步无后台任务。

## 14. 指标与审计

`MetricsScope{feature=tables}`：`lights3_tables_requests_total{op,status}`（counter）、
`lights3_tables_commit_seconds{result}`（histogram，bounds 与 backend 直方图同款）、
`lights3_tables_commit_conflicts_total`、`lights3_tables_entities{kind=namespace|table}`
（gauge_callback，本步可省）。审计：`event = "tables.<op>"`（create_namespace /
create_table / commit / drop_* / rename / enable_bucket…），`bucket`、`key = "<ns-path>/<t>"`、
`detail` 带 `generation` 与 `commit_id`。

## 15. 单测清单

`test_tables_iceberg.cc`（纯函数，无协程）：

- `initial_metadata`：v1/v2 各一；嵌套 struct/list/map 字段 id 分配；spec field-id 从 1000；空 sort order → 0。
- `check_requirements` 8 种 × 通过 / 失败；未知 type → 400；`assert-ref-snapshot-id` null 语义。
- `apply_updates`：每个 action 的合法样例 + 至少一个非法样例；v3 升级 406；`add-snapshot` 缺 sequence-number（v2）400；`set-snapshot-ref` 不存在的快照 409；`set-location` 保留前缀 400。
- `check_transition`：改既有 schema → 409；`last-column-id` 回退 → 409；删当前 spec → 409。
- `finish_transition`：`metadata-log` 追加与裁剪（keep=2）；`snapshot-log` 裁剪。
- 固件回归：`tests/fixtures/tables/pyiceberg-v2.metadata.json` 解析通过、`canonical` 幂等。

`test_tables_catalog.cc`（`ObjectCatalogStore` on `MemoryBackend`，`sync_wait`）：

- namespace：多级创建、证据判定（有子表无 `_ns.json` → exists）、分页（pageSize=1 三页）、drop 非空 409。
- create_table 并发：两个 `create_table` 同名 `when_all` → 恰一个成功。
- commit：正常路径 generation 1→2→3；陈旧 token 409；并发 20 个 commit（不同 commit_id，同 expected）→ 恰一个成功，其余 `CommitFailedException`。
- 幂等矩阵（设计 §5.3 五行）：用 fault 门面在 `put_commit` 后 / `put_table` 后注入异常，再重放同 commit_id。
  fault 点：新增 `"tables.commit.after_stage"`、`"tables.commit.after_cas"`（加入 `fault::kPoints`，`check()` 放在 Catalog 对应位置）。
- 崩溃窗口矩阵（设计 §5.4）：同上四种现场，各自断言"指针与 metadata 文件的最终状态"。
- 实体身份校验：把 `tbl/a.json` 的 body 复制到 `tbl/b.json` → `get_table(b)` 抛 InternalError。
- `delete_bucket_state` 清空。

`test_tables_rest.cc`（进程内 `S3Service`，`tables.enabled`，noauth 与 signed 各若干）：

- `/config` 的 `endpoints` == `RestApi::advertised_endpoints()` == 路由表中标准端点（表驱动，三者一致）。
- 未启用表桶：任一 namespace 路由 404 `NoSuchNamespaceException`。
- 全流程：PUT buckets → POST namespaces → POST tables → GET tables/{t}（`metadata-location` 以 `s3://` 开头且含保留前缀）→ POST commit（`add-snapshot` + `set-snapshot-ref`，manifest-list 预先 PUT 到桶）→ 陈旧 token 409 → GET metadata-location / PUT 错 token 409 → rename → DELETE。
- 错误模型：每类错误的 `type` 与 status；`stage-create` / `purgeRequested=true` / `overwrite` / v3 → 406；`identifier` 与 URL 不符 400；pageToken 换操作重放 400。
- 守卫：表桶内 PUT `.lights3-table/x` → 400；GET 同 key → 404（不存在）而非 400；DeleteObjects 混合批逐 key Error；DeleteBucket 非空目录 409；CreateBucket `iceberg` → 400；`tables.enabled: false` 时以上全部不生效。
- 桶名遮蔽：启用后 `PUT /iceberg/v1/...` 不落到 S3 面（无 `iceberg` 桶时 404 JSON 而非 XML NoSuchBucket）。

## 16. e2e 段（`run_e2e.sh`）

新增 `tables` 段（六驱动矩阵都跑；`LIGHTS3_TABLES` OFF 的构建跳过）：配置
`tables.enabled: true`；用 `s3curl`（SigV4，service `s3`）依次：`PUT /iceberg/v1/buckets/tb`
→ `POST …/namespaces {"namespace":["e2e"]}` → `POST …/tables {schema…}` →
`PUT /tb/e2e/t/data/f.parquet`（任意字节）与 `PUT /tb/e2e/t/metadata/snap-1.avro`
（固件 `tests/fixtures/tables/manifest-list-1.avro`）→ `POST …/tables/t` 带
`add-snapshot{manifest-list: s3://tb/e2e/t/metadata/snap-1.avro}` + `set-snapshot-ref main`
→ 断言 200 且 `generation` 2 → 再发同 body 带旧 requirements（`assert-ref-snapshot-id main null`）
→ 断言 409 → `PUT /tb/.lights3-table/x` 断言 400 → `DELETE …/tables/t` 204。

## 17. 陷阱与注意

- **CAS 的 etag 来源**：`get_object` 的 `stream.meta.etag`；memory/localfs 的 etag 是内容 MD5，
  两次写入同内容 etag 相同——`put_table` 的 `updated_unix` 与 `version_token` 保证内容必变。
- **`If-Match` 语义**：对象不存在时后端抛 `NoSuchKey`（不是 PreconditionFailed），
  `Catalog` 里两者都当冲突处理（drop 与 commit 竞争时会遇到）。
- **协程 catch 内不能 co_await**：j 步失败后的 best-effort delete 要先记
  `std::exception_ptr`，出 catch 再 `co_await delete_object`。
- **GCC15 `co_await` 成员指针 ICE**：先存局部变量再 `co_await`（仓库既有约定）。
- **路径解码**：`req.path` 是驱动解码后的路径还是原始路径？按 `router.cc` 的
  `parse_bucket_key` 现状（用 `req.path`），`%1F` 已被解码成 U+001F；用
  `req.raw_path` 切段再自行 `percent_decode` 更稳（vhost 分支不涉及）。
- **`Content-Length: 0` 的 POST**：`read_json_object(req, allow_empty=true)` 用于
  DELETE/HEAD/rename 等；`commit` 必须有 body。
- **测试固件**：manifest-list Avro 固件此步只需"存在"，内容用 PyIceberg 真生成的文件，
  ③ 的深校验直接复用。
- **`tables.enabled: false` 必须零影响**：所有新成员为空指针即短路；`test_service.cc`
  既有用例不加任何配置即验证了这一点。

## 18. 实现记录（2026-09-12）

按本稿落地，差异与补充如下（设计文档对应章节已同步）：

- **表桶端点路径**：`PUT|GET|DELETE /iceberg/v1/buckets/{bucket}`（无 warehouse 段，
  与 RustFS 一致），不是 `/{w}/buckets/{w2}`。
- **墓碑对"空"的判定**：`namespace_has_children` / `bucket_state_empty` 只认活跃对象，
  drop 表留下的墓碑不算子项，drop namespace 时顺带删除其下墓碑；否则 PyIceberg
  的"drop table → drop namespace"序列会 409。设计 §5.7 / §9.6 的表述随之修正。
- **幂等重放读回文件**：STAGED 记录重放时不比对重算出的 metadata（`last-updated-ms`
  必然不同），直接采用记录指向的文件；COMMITTED 重放同样读文件。
- **表级锁**：仓库没有 `AsyncMutex`，用 `AsyncSemaphore(1)`；锁表按 `<bucket>/<table_id>`
  只增不删（有界于表数）。
- **`LoadTable.config`**：额外给出 `lights3.version-token`（省一次 metadata-location 调用）
  与 `lights3.snapshot-validation: "shallow"`（③ 改为 `deep`）。
- **指标**：`lights3_tables_commits_total{result}`、`lights3_tables_commit_seconds`、
  `lights3_tables_requests_total`、`lights3_tables_requests_by_op_total{op,status}`。
- **配额预检**（§9.2 g 步）留给 ②：`CommitHooks.quota_check` 已预留，dispatch 目前只注入
  `note_usage`。
- **GCC 15**：`co_await (this->*fn)(...)` 触发 ICE，先存成局部 `Task` 再 `co_await`。
- **e2e**：`run_e2e.sh` 主配置打开 `tables.enabled`，六驱动矩阵全部跑 tables 段；
  没有独立固件，manifest-list 用任意字节（浅校验只看存在性）。
