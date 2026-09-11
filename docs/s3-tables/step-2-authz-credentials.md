# 步骤 ②：权限、租户、签名名与凭证下发

> 状态：**已实现（2026-09-12，分支 feat/s3-tables-step2）**。对应设计 §6.2、§8.2–§8.4、§14 ②。
> 实现与本稿的差异见文末 §12。
> 依赖 ①。完成后：带 `prefixes` / `readonly` 的凭证在目录面按前缀限权；租户凭证
> 只见本租户表桶；`s3tables` 签名名可用；`X-Iceberg-Access-Delegation: vended-credentials`
> 得到按表前缀收窄的会话凭证；表桶不再被 lifecycle 扫描。

## 1. 目标与验收

| 验收项 | 判据 |
| --- | --- |
| policy | `prefixes:["sales/"]` 凭证：`sales` 下建表 200、`hr` 下 403；`readonly` 凭证 load/list 200、create/commit 403 |
| 租户 | 租户 A 凭证访问租户 B 的表桶 → 403（`require_tenant_bucket` 同款 message） |
| 签名名 | 同一请求用 credential scope `…/s3tables/aws4_request` 签名 → 通过；`tables.accept_s3tables_signing: false` → 403 |
| 凭证下发 | LoadTable 带头 → `storage-credentials[0].config` 含 AK/SK/token；用它 PUT `<location>/data/x` 200、PUT `<location>/../y` 403、PUT `.lights3-table/…` 400、AssumeRole 403 |
| lifecycle | 表桶配置 Expiration 规则后 `LifecycleRunner::run_once` 不删任何对象，日志有 WARN |

## 2. 文件清单

| 文件 | 改动 |
| --- | --- |
| `src/tables/rest_api.cc` | 授权映射表（§3）；`X-Iceberg-Access-Delegation` 解析；`GET …/tables/{t}/credentials` |
| `src/tables/catalog.{h,cc}` | `quota_check_` hook 接入；`vend_credentials(...)` |
| `src/s3/auth/sigv4.h/.cc` | `verify_any(req, services)` 或 `verify_impl` 的 service 集合重载 |
| `src/s3/auth/credential_store.h/.cc` | `mint_session(parent, duration, std::optional<CredentialPolicy> narrow)` |
| `src/s3/service.h/.cc` | Hooks 增加 `verify_catalog`（按配置选 service 集合）、`tenant_gate`、`quota_gate` |
| `src/s3/lifecycle.cc` | `run_once` 跳过表桶；`bucket_lifecycle.cc` PUT 时 WARN |
| `src/s3/handlers/quota_gate.cc` | 无改动（`check_quota` 被 Catalog 经 hook 调用） |
| `tests/unit/test_tables_rest.cc` `test_credentials.cc` `test_tenancy.cc` | 新用例 |
| `docs/credential-management.md §10` `docs/multi-tenancy.md §4.3` | 各加一小节 |

## 3. 授权映射（`rest_api.cc`）

路由表的每一行带 `Action` 与一个 `key_of(captures)`：

```cpp
enum class KeyKind { None, Namespace, Table };
struct Route { ...; s3::Action action; KeyKind key_kind; bool root_only = false; };
// key_kind → policy 检查用的 key
//   None      → ""                          （/config、buckets/{w} GET）
//   Namespace → ns_path(levels) + "/"       （namespace 各操作、list tables）
//   Table     → ns_path(levels) + "/" + t   （table 各操作）
```

`rename`：两次检查——源 `(w, src_key, Delete)` 与目标 `(w, dst_key, Write)`。
`buckets/{w}` 的 PUT / DELETE：`root_only = true`，`!hooks.is_root(ak)` → 403
`ForbiddenException "enabling a table bucket requires a root credential"`。
policy 为 `nullptr`（不受限）时跳过。所有 403 走 `RestError(403, "ForbiddenException", "Access denied by credential policy.")`。

**列表结果过滤**（与 ListObjects 的 `allows_key` / `prefix_may_contain` 同理）：
`list_namespaces` 的每个子 namespace 用 `policy->prefix_may_contain(ns_path + "/")`
过滤；`list_tables` 用 `policy->allows_key(ns_path + "/" + t)`。过滤在分页之后
做（可能出现空页但 `next-page-token` 非空，规范允许）。

## 4. 租户隔离

`Hooks` 增加 `std::function<Task<void>(std::string_view bucket, std::string_view tenant)> tenant_gate`，
dispatch 注入 `[this](b, t) { return require_tenant_bucket(b, t, /*creating=*/false); }`。
`RestApi::dispatch` 第 4 步之后：`if (!ident.tenant.empty()) co_await hooks.tenant_gate(bucket, ident.tenant)`。
`S3Error(AccessDenied)` 经 `from_s3_error` → 403。`GET /config` 无桶时不检查。

## 5. `s3tables` 签名名（`sigv4.h`）

`verify_impl(req, service, payload_hash)` 已按参数比对 service。新增：

```cpp
// 目录面：credential scope 的 service 允许集合中的任一个（设计 §6.2）
VerifiedIdentity verify_services(http::HttpRequest& req, std::span<const std::string_view> services) const;
```

实现：先 `parse_authorization` 得到 `f.service`（现有 `verify_impl` 内部已解析；
抽出一个 `peek_service(req)` 或在 `verify_impl` 增加"service 集合"参数——取后者，
`verify_impl(req, std::span<const std::string_view>, hash)`，旧接口包一层
`std::array{service_}`）。错误消息保持
"credential scope does not match this endpoint (<region>/<s3|s3tables>)"。
dispatch 的 `Hooks.verify` 对目录路径改为
`verify_services(req, cfg.accept_s3tables_signing ? {"s3","s3tables"} : {"s3"})`。
payload：目录请求 body 小，`x-amz-content-sha256` 三形态照 `verify_impl` 既有逻辑
（PyIceberg 走 botocore 带十六进制摘要；DuckDB `UNSIGNED-PAYLOAD`），无需特判。

## 6. `mint_session` 收窄（`credential_store.h`）

```cpp
// narrow: 与父 policy 求交后作为会话 policy（nullopt = 继承父，现有行为）。
// 交集规则：buckets = narrow.buckets ∩ 父允许（逐个 allows_bucket 过滤）；
// prefixes = narrow.prefixes 中被父 allows_key 覆盖者（父 prefixes 为空 = 全部保留）；
// readonly = 父.readonly || narrow.readonly；actions = 父 ∩ narrow（空 = 由 readonly 决定）。
// 交集为空（无桶或无前缀可用）→ S3Error(AccessDenied, "requested session scope exceeds the caller's policy")
Task<SessionCredential> mint_session(std::string_view parent_ak, int duration_sec,
                                     std::optional<CredentialPolicy> narrow = std::nullopt);
```

实现改动集中在 `credential_store.cc:690-729`：构造 `SessionEntry` 时
`policy = narrow ? intersect(parent.policy, *narrow) : parent.policy`；`parent.policy`
为 nullopt（root/不受限）时 `policy = narrow`。持久化格式已有 `policy` 字段，
`.sys/sts/<ak>` 无需改；旧实例读到收窄 policy 也能正确执行（policy JSON 形态未变）。
`sts.cc` 的 AssumeRole 不传第三参数（`Policy` 表单参数仍不支持，保持现状）。

## 7. 凭证下发（`Catalog::vend_credentials`）

```cpp
struct VendedCredentials { std::string prefix;            // s3://<bucket>/<location 相对前缀>/
                           std::string ak, sk, token; int64_t expires_unix; };
// 设计 §8.4。caller 为发起 LoadTable 的 AK（会话 AK 直接拒绝：S3Error AccessDenied "sessions cannot vend"）
Task<VendedCredentials> vend_credentials(std::string_view caller_ak, const TableEntry&, bool readonly);
```

收窄 policy：

```cpp
CredentialPolicy p;
p.buckets  = {bucket};
p.prefixes = {location_relative_prefix + "/",                       // 数据
              reserved_prefix + ns_path + "/" + name + "/metadata/"}; // metadata.json 只读（写受守卫拦截，policy 不用区分）
p.readonly = readonly;   // 请求方 policy readonly 或 X-Iceberg-Access-Delegation 只要求读（本步：等于调用者 readonly）
```

`RestApi::load_table`：

```text
delegation = 头 "X-Iceberg-Access-Delegation" 按 ',' 切分、trim、小写
want = 含 "vended-credentials"
if (!want)                → config["lights3.credential-vending"] = cfg.credential_vending ? "supported" : "disabled"
else if (!cfg.credential_vending) → config[...] = "disabled", config["lights3.credential-vending-reason"] = "credential-vending-disabled"
else try vend → 响应加 "storage-credentials":[{"prefix": v.prefix, "config": {"s3.access-key-id","s3.secret-access-key","s3.session-token","expiration-ms"}}]
                并把同四键并入 "config"；头 Cache-Control: no-store, private
     catch AccessDenied → config[...] = "disabled", reason = "credential-vending-not-authorized"（表照常返回）
```

`GET …/tables/{t}/credentials`：同一实现，只返回 `{"storage-credentials":[...]}`；
未开启 → 406 `UnsupportedOperationException`。TTL = `cfg.credential_ttl_sec`。
审计 `tables.vend_credentials`，`target` = 会话 AK。

`Hooks` 增加 `std::function<Task<VendedCredentials>(std::string_view caller, CredentialPolicy, int ttl)> mint`
（dispatch 注入 `cred_store_->mint_session`），`RestApi` 仍不依赖 `credential_store.h`。

## 8. 配额预检（`quota_check_` hook）

`Catalog` 的 `std::function<void(std::string_view bucket, int64_t add_bytes, int64_t add_objects)> quota_check_`，
dispatch 注入 `[this, &auth](...) { check_quota(bucket, add_bytes, add_objects, auth); }`
（`auth` 需要 `RequestAuth`，因此 hook 在 `RestApi::dispatch` 每请求构造后传给
`Catalog::commit_table` 作参数，而不是 Catalog 成员）。提交前以
`canonical(next).size()` 作 `add_bytes`、`add_objects = 1` 预检；`QuotaExceeded` →
409 `CommitFailedException "QuotaExceeded: ..."`。metadata.json 写入后
`note_usage(bucket, 1, size)`（经同一 hook 结构再加一个 `note_usage` 函数）。

## 9. lifecycle 排除（`lifecycle.cc`）

`LifecycleRunner` 增加 `set_table_bucket_store(std::shared_ptr<tables::TableBucketStore>)`；
`run_once` 循环开头：

```cpp
if (table_buckets_ && tables::TableBucketStore::find(tb_snap, bucket)) {
    LOG_WARN("lifecycle: bucket {} is a table bucket, rules are ignored (docs/s3-tables-design.md §8.2)", bucket);
    continue;
}
```

`put_bucket_lifecycle`（`bucket_lifecycle.cc`）：表桶时照常保存但响应后 LOG_WARN
（不拒绝：AWS 也接受配置）。`docs/s3-protocol.md §1` 的 Lifecycle 行补一句。

## 10. 单测清单

- `test_tables_rest.cc`：
  - 三种凭证（不受限 / `prefixes:["sales/"]` / `readonly`）× 每个路由的期望 status（表驱动）。
  - list 过滤：两个 namespace，前缀凭证只见一个；`identifiers` 过滤。
  - `s3tables` 签名通过；关闭开关后 403；错 region 仍 403。
  - 下发：`storage-credentials` 形态；用下发 AK 走 S3 面：前缀内 PUT/GET/DELETE 200、前缀外 403、保留前缀 400、`POST /` AssumeRole 403；过期后 `ExpiredToken`；`Cache-Control` 头存在；`GET …/credentials` 未开启 406。
  - 未授权下发：调用者 policy 不含该桶 → 表照常返回且 `reason = credential-vending-not-authorized`。
- `test_credentials.cc`：`mint_session` 交集规则（父 `prefixes:["a/"]` + narrow `["a/b/", "c/"]` → `["a/b/"]`；交集空 → AccessDenied；父 readonly 传播）。
- `test_tenancy.cc`：租户 A 对租户 B 表桶的 namespace 列表 403。
- `test_service.cc`（lifecycle）：表桶上的 Expiration 规则 `run_once` 后对象仍在。

## 11. 陷阱

- `RequestAuth::policy` 是 verify 时快照的指针，`RestApi::dispatch` 内跨 `co_await`
  使用是安全的（`VerifiedIdentity` 在栈上活到请求结束）——但 hook 里不要缓存它。
- 收窄 policy 的 `prefixes` 是 key 前缀而非 glob：`location` 相对前缀必须以 `/` 结尾，
  否则 `sales/t` 会放行 `sales/t2/`。
- 会话凭证签名验证要求 `X-Amz-Security-Token`，PyIceberg 用 `s3.session-token` 会自动带；
  DuckDB SECRET 需 `SESSION_TOKEN` 字段——写进 `docs/s3-tables-design.md §13` 的模板。
- `verify_services` 只在目录路径使用；S3 面继续只认 `s3`，否则等于全局放宽签名名。

## 12. 实现记录（2026-09-12）

- **签名名集合**：`SigV4Authenticator::verify_any(req, span<string_view>)`，`verify_impl`
  改为接受 service 集合；`sign()` 加第四参数 `service` 供测试签 `s3tables`。目录路径按
  `tables.accept_s3tables_signing` 选 `{s3, s3tables}` 或 `{s3}`；S3 面不变。scope 不符仍是
  `AuthorizationHeaderMalformed`（400），目录面经 `from_s3_error` 变 403 `ForbiddenException`。
  绑定了 mTLS 证书且未签名的请求仍走 `verify_identity`。
- **授权粒度调整**（与本稿 §3 的差异）：namespace 的**读**类路由（load / exists / list tables）
  按 `prefix_may_contain(ns + "/")` 判定——限在 `sales/orders/` 的凭证要能列出 namespace
  `sales`，否则 PyIceberg 的 `list_tables` 不可用；写类路由仍要求 `allows(bucket, ns + "/")`。
  表类路由同时接受 `<ns>/<t>` 与 `<ns>/<t>/` 两种 key（`RestApi::allows_table`），下发的会话
  凭证（前缀 `<ns>/<t>/`）因此能 LoadTable。rename 用同一个 helper。
- **列表过滤**：`list_namespaces` 用 `prefix_may_contain`，`list_tables` 用 `allows_key`，
  分页之后过滤（可能出现空页而 token 非空，规范允许）。
- **`narrow_policy(parent, narrow)`**（`s3/auth/policy.h`）：buckets / prefixes 逐项被父放行者
  保留，`readonly` 取或，actions 取交；交集为空抛 `AccessDenied`。`mint_session` 第三参数
  `std::optional<CredentialPolicy> narrow`；持久化格式未变。
- **凭证下发**：`Catalog::vending_policy` 给出 `{bucket, prefixes: [<location>/, <reserved>/<ns>/<t>/metadata/], readonly}`；
  `RestApi::vend` 经 `Hooks::mint` 铸会话；会话凭证再请求下发时 `mint_session` 拒绝 →
  `credential-vending-not-authorized`（表照常返回）。响应 `storage-credentials[0].config`
  含 `s3.access-key-id / s3.secret-access-key / s3.session-token / expiration-ms`，同四键并入
  `config`，加 `lights3.credential-mode`；`Cache-Control: no-store, private`。`GET …/credentials`
  未开启 → 406，未授权 → 403。
- **租户门**：`Hooks::tenant_gate` 在 ① 已接线；本步补单测（租户 A 访问租户 B 的表桶 403）。
- **配额预检**：`Hooks::commit.quota_check` 在 dispatch 里用 `hooks.access_key / policy / tenant`
  构造 `RequestAuth` 调 `check_quota`；`QuotaExceeded` 经 `from_s3_error` → 409
  `CommitFailedException "QuotaExceeded: …"`。
- **lifecycle 排除**：`LifecycleRunner::set_skip_predicate`（不让 lifecycle 依赖 tables 头），
  app 注入 `TableBucketGuard::is_table_bucket`；`PutBucketLifecycle` 对表桶照常保存并 WARN。
- **e2e**：`tables.credential_vending: true`；`s3tables` 签名（curl `aws:amz:<region>:s3tables`）、
  下发凭证在前缀内 PUT 200 / 前缀外 403 / 读 metadata 200 / AssumeRole 403、lifecycle WARN。
