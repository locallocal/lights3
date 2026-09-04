# 用量统计、配额与多租户（roadmap §3.9）

> 状态：四项全部落地（2026-09-04）。代码：`src/s3/usage.{h,cc}`、
> `src/s3/quota.{h,cc}`、`src/s3/tenant.{h,cc}`、`src/s3/audit.{h,cc}`、
> `src/s3/handlers/{quota_gate,bucket_quota,admin_tenants}.cc`；CLI
> `src/tools/s3adm_{usage,quota,tenant}.cc`。单测 `tests/unit/test_tenancy.cc`，
> e2e `tests/e2e/run_e2e.sh` 的 "roadmap §3.9" 一节。

## 1. 目标与边界

roadmap §3.9 把四件事定义为一条依赖链：**用量统计 → 配额 → 租户实体化 →
审计日志**。本文按链条顺序描述各层的设计与契约。一个统一的原则贯穿全部：
**全部实现在 L2（S3 服务层），存储层与 meta schema 零改动**——

- 用量计数器由网关维护、以 `.sys/usage/<bucket>` 持久化，用普通
  `IStorageBackend` 列举做校准；四个 IMetaStore 与六个后端都不知道计数器
  的存在；
- 配额、租户、桶归属都是 `.sys` 下的 JSON 记录（复用 `SysConfigStore` 的
  write-through + tombstone sync 模式，与 cors/lifecycle 同一套）；
- 租户凭证只是多带两个字段（`tenant`、`role`）的动态/文件凭证，验签路径
  不变。

代价是：计数器的精度是"两次全量校准之间近似"（§2.4），而不是 meta 事务内
的强一致——这是 roadmap 给出的两条路线里的"离线聚合"路线，理由见 §2.5。

## 2. 用量统计

### 2.1 数据模型

每个桶三个计数器（`BucketUsage`，`src/s3/usage.h`）：

| 字段 | 含义 |
| --- | --- |
| `objects` | 已提交对象数 |
| `bytes` | 已提交对象字节数 |
| `mpu_bytes` | 进行中 multipart 分片的字节数（**计入配额**，见 §3.4） |
| `scanned_at` | 最近一次全量计数的时刻；epoch = 从未扫描（计数器从零起算，标"近似"） |

### 2.2 增量维护

每条写路径在后端提交成功后应用增量（`S3Service::note_usage`）：

| 操作 | objects | bytes | mpu_bytes |
| --- | --- | --- | --- |
| PutObject / CopyObject | 新键 +1 | +实际写入 − 被覆盖对象大小 | — |
| DeleteObject / DeleteObjects | 存在则 −1 | −被删对象大小 | — |
| UploadPart / UploadPartCopy | — | — | +分片大小 |
| CompleteMultipartUpload | 新键 +1 | +请求所列分片之和 − 被覆盖对象大小 | −该 upload 下全部已存分片之和 |
| AbortMultipartUpload | — | — | −全部已存分片之和 |
| Lifecycle 过期 / 清理僵尸 MPU | 同上两行 | | |
| CreateBucket / DeleteBucket | 建立零计数 / 删除记录 | | |

"被覆盖/被删对象大小"来自写前的一次 `head_object`（`existing_size`）。
有 §3.8 元数据缓存的后端上这是一次 stat 或缓存命中；cloudproxy 上是一次
远端 HEAD。`usage.enabled=false` 时整条链路（HEAD、计数、配额）全部跳过。
PutObject/UploadPart 的实际写入字节由 `ByteCountingReader` 在 body 上计数
（后端契约要求读到 EOF），不信任客户端声明的长度。

### 2.3 持久化与多网关

- **flush**：脏计数器每 `usage.flush_interval`（默认 60s）落一次
  `.sys/usage/<bucket>`；进程退出时最后 flush 一次。重启后从记录恢复，
  没有记录的桶由启动时的 **bootstrap 扫描**（§2.4）补齐。
- **多网关**：每个实例只看到自己的写增量。`auth.sync_interval` 开启时，
  实例周期性重读 `.sys/usage/*`，**只采纳 `scanned_at` 比本地更新的记录**
  （对端的一次 flush 只是它的片面视图，不比本地的更好；一次全量扫描才是）。
  跨网关的漂移由 reconcile 收敛。

### 2.4 全量校准（reconcile）

`UsageTracker::rescan(bucket)` 用普通后端 API 全量列举：`list_objects` 分页
累加对象数与字节，`list_multipart_uploads` + `list_parts` 累加进行中分片；
结果整体替换该桶的计数器并立即落盘。三个触发点：

| 触发 | 说明 |
| --- | --- |
| 启动 bootstrap | 存在但没有扫描记录的桶各扫一次（`usage.reconcile=true` 的实例） |
| 周期 | `usage.reconcile_interval`（默认 1d，0=关）全桶重扫；多网关只在一个实例开 `usage.reconcile`（同 duostore `gc_enabled` 的指定实例语义） |
| 按需 | `POST /-/admin/usage/<bucket>/rescan`、`s3adm usage <bucket> --rescan` |

**精度契约**：扫描完成的瞬间精确；两次扫描之间的误差来源只有 (a) 同键并发
覆盖写各自减去同一个旧尺寸，(b) 同 upload 重传同一分片号的过计（complete/abort
按实际存量结算），(c) 多网关下对端的写。三者都被下次扫描抹平；计数器下钳
于零。配额执行读取的就是这套计数器，继承同一契约。

扫描期间落地的写可能被计入也可能不被计入：`scanned_at` 取扫描**开始**时刻，
不会声称覆盖了它们。同一桶的扫描单飞（并发 rescan 得 `SlowDown`）。

### 2.5 为什么不做 meta 侧计数器

roadmap 列了两条路：meta 事务内计数器（四个 IMetaStore 各写一份，tikv 还得
用 §3.7 那种追加式 delta 行避免热点冲突）或离线聚合扫描。选后者的理由：
(1) 六个后端里 localfs/xlocalfs/cloudproxy/tiered 根本没有事务性 meta，
meta 侧方案只覆盖 duostore；(2) 配额场景对"近似 + 周期校准"的容忍度很高
（RGW 的 bucket index 统计、MinIO 的 data-usage crawler 都是这种口径）；
(3) 零后端改动意味着新后端自动获得用量统计。scrub（§3.1）的遍历没有直接
复用——它不覆盖 cloudproxy，而 `list_objects` 对所有后端一致。

### 2.6 观测

| 指标 | 说明 |
| --- | --- |
| `lights3_bucket_usage_bytes{bucket}` / `_objects{bucket}` | 回调 gauge，最多 512 个桶（超出不再注册，防标签爆炸） |
| `lights3_usage_scans_total` / `lights3_usage_last_scan_timestamp_seconds` | 全量计数次数 / 最近一次时间 |
| `lights3_quota_rejections_total{scope=bucket\|tenant}` | 配额拒绝次数 |

## 3. 配额

### 3.1 桶级配额

`.sys/quota/<bucket>` 记录 `{max_bytes, max_objects}`（0 = 该轴不限；两轴皆
0 的记录不合法，删除即"无配额"）。管理面是 `?quota` 子资源（S3 无对应
API，XML 是本实现自定义形状）：

```text
GET    /bucket?quota   任何被 dispatch 放行到该桶的凭证（租户可读自己桶的限额）；无配置 404 NoSuchQuotaConfiguration
PUT    /bucket?quota   root 专属；桶须存在；usage.enabled=false 时拒绝（无法执行的限额不允许配置）
DELETE /bucket?quota   root 专属，幂等 204
```

```xml
<QuotaConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <MaxBytes>53687091200</MaxBytes>
  <MaxObjects>1000000</MaxObjects>
</QuotaConfiguration>
```

### 3.2 租户级配额

租户记录（§4.1）里的 `quota{max_bytes, max_objects, max_buckets}`：前两轴对
租户**全部所有桶**的用量求和（含 `mpu_bytes`），`max_buckets` 在 CreateBucket
时按归属记录计数。桶级与租户级同时生效，先判桶后判租户。

### 3.3 执行点与错误码

`S3Service::check_quota(bucket, add_bytes, add_objects)` 只做内存算术
（计数器 + 本次增量 > 上限 → 拒绝），在 body 流入之前决定：

| 操作 | add_bytes | add_objects |
| --- | --- | --- |
| PutObject | 声明长度（aws-chunked 用解码长度）− 被覆盖对象大小 | 新键 1 |
| CopyObject | 源大小 − 被覆盖对象大小 | 新键 1 |
| CreateMultipartUpload | 0（已超限的桶直接拒绝开新上传） | 0 |
| UploadPart / UploadPartCopy | 分片大小 | 0 |
| CompleteMultipartUpload | 所列分片之和 − 该 upload 全部已存分片之和 − 被覆盖对象大小 | 新键 1 |

拒绝返回 **`QuotaExceeded`（HTTP 403）**——与 Ceph RGW 的选择一致（AWS S3
没有可对照的错误码；MinIO 同样用 403）。消息形如
`The request would exceed the bucket quota: bytes 12 + request > 20 (bucket).`，
租户级带 `tenant '<id>'`。每次拒绝计一次指标并写一条审计记录（§5）。

### 3.4 MPU 半途语义

roadmap 要求"定 MPU 半途语义"。契约：

1. **分片占用配额**。UploadPart 时按分片大小判定并计入 `mpu_bytes`——
   分片实实在在占着磁盘，不计入等于给了绕过配额的通道；
2. **Complete 只在"完成后的对象仍放不下"时拒绝**。因为分片已在 UploadPart
   时被逐一放行，Complete 的净增量是"所列分片 − 全部分片 − 被覆盖对象"
   （通常 ≤ 0），正常情况必然通过；只有在分片上传后配额被调低、或对端网关
   的写把桶挤满时才会 403；
3. **被拒的 upload 原样保留**，客户端可 Abort（释放 `mpu_bytes`）或等
   lifecycle 的 `AbortIncompleteMultipartUpload` 清理。不做"拒绝即自动
   abort"——那会让一次瞬时超限吞掉客户端已经付费上传的分片。

### 3.5 已知取舍

- 计数器近似（§2.4）意味着配额也近似：多网关下一次扫描周期内可能超限一个
  周期的漂移量。要更紧就缩短 `usage.reconcile_interval`。
- 未扫描的桶（无记录）按零用量放行——可用性优先于误拒；启动 bootstrap 扫描
  会很快补齐。
- 声明长度未知（无 `x-amz-decoded-content-length` 的 chunked 请求）时预检
  按 0 判定，写完后按实际字节计数；下一次请求即受限。

## 4. 租户

### 4.1 模型与存储

```text
.sys/tenants/<id>    {id, display_name, created, quota{max_bytes,max_objects,max_buckets}, rev}
.sys/owners/<bucket> {tenant, assigned_by, assigned}
凭证对象 / credentials_file 条目：新增 "tenant": "<id>", "role": "user"|"admin"
```

- 租户 id：`[a-z0-9][a-z0-9._-]{0,63}`，同时是 `.sys` 对象键和
  `Owner/ID` 的值；
- **归属**在两个地方写入：租户凭证 CreateBucket 成功后自动记为该租户
  （记录失败则回滚删桶——不能留下租户再也碰不到的桶），或 root 经
  `PUT /-/admin/tenants/<id>/buckets/<bucket>` 指派（已属他租户的桶须
  `?force=true`）；DeleteBucket 连带删除归属、配额与用量记录；
- **没有归属记录的桶 = 未归属（legacy）桶**：root 与不带 tenant 的凭证看到
  的世界与本功能落地前完全一致，租户凭证则完全看不到它们。这是向后兼容
  的关键：现有部署不做任何迁移，租户是叠加而非替换。

### 4.2 凭证的三种身份

```text
静态凭证（config）                      = root：一切
tenant 为空的动态/文件凭证              = legacy：数据面全部桶（受 policy 约束），同以前
tenant=<id> 的动态/文件凭证             = 租户凭证：仅本租户所有桶（再受 policy 约束）
    role=admin                          + 本租户的管理面（§4.4）
STS 会话                                 继承父凭证的 tenant，永不是 admin
```

`tenant`/`role` 随 policy 一起在验签时刻快照（`VerifiedIdentity`，
docs/archive/gaps.md §3.7 的同一理由），在途请求不受并发改动影响。
`role` 只认 `user`/`admin`，其它值 `InvalidRequest`（文件里的拼错是启动错误，
不会静默降级）。

### 4.3 数据面授权

dispatch 在 policy 判定之后追加归属判定（`require_tenant_bucket`），仅对
租户凭证、且请求带桶名时生效：

| 情形 | 结果 |
| --- | --- |
| 桶归本租户 | 放行 |
| CreateBucket 且桶无归属记录 | 放行；handler 再查桶是否已存在——存在即 `BucketAlreadyExists`(409)，租户不能"认领"已有的未归属桶 |
| 其它：桶不存在 | `NoSuchBucket`(404)——SDK 的 HeadBucket 探测依赖它 |
| 其它：桶存在但归属他人 / 未归属 | `AccessDenied`(403) |
| CopyObject / UploadPartCopy 的源桶 | 同样判定 |

ListBuckets 只列本租户的桶，`Owner` 输出租户 id 与 display_name（root/legacy
仍是 `lights3`）；ListObjectsV2 `fetch-owner` 输出桶归属租户。

### 4.4 分级管理面

| 操作 | root | 租户 admin | 租户 user |
| --- | --- | --- | --- |
| `/-/admin/credentials` 增删改查 | 全部 | 仅本租户的凭证（POST 的 tenant 被固定为本租户；他租户的 AK 一律 `InvalidAccessKeyId`，不可枚举；不能改 `tenant` 字段） | 403 |
| `/-/admin/tenants` | 全部 | `GET` 本租户 | 403 |
| `/-/admin/usage` | 全部 | 本租户桶的查询与 rescan | 403 |
| `?quota` | GET/PUT/DELETE | GET | GET |
| `?cors` / `?lifecycle` / `?website` | 全部 | 403（不变） | 403 |

root 的判定仍是"静态来源"的实时查表（`is_root`），租户 admin 的判定用验签
快照；两者并存的理由与 credential-management.md §7 一致。

### 4.5 分期保留

- 租户 admin 不能自行配置桶配额——限额是运营决策；
- 无 bucket policy / ACL 语义，租户内的细粒度仍靠 per-credential policy；
- 会话表仍是单实例内存态（§2.6 既有约束）。

## 5. 审计日志

`audit.path` 非空即开启：独立的 spdlog rotating 文件（`audit.max_size` ×
`audit.max_files`），与运行日志分离（改 `log.level` 不影响审计），每行一个
JSON 对象，字段缺省时**省略而非空串**：

```json
{"ts":"2026-09-04T12:00:00Z","event":"quota.reject","actor":"L3AK...","tenant":"acme",
 "request_id":"...","bucket":"logs","detail":"bytes 12 + request > 20 (bucket)"}
```

| 事件 | 触发 | 关键字段 |
| --- | --- | --- |
| `cred.create` / `cred.update` / `cred.delete` | 管理面凭证生命周期 | `target`=AK，`detail`=tenant/role 或更新体 |
| `cred.show_secret` | `?show-secret=true`（无论是否给出） | `detail`=granted / refused (static) |
| `sts.assume_role` | 铸造会话 | `target`=会话 AK |
| `tenant.create` / `tenant.update` / `tenant.delete` | 租户生命周期 | `target`=租户 id |
| `tenant.assign_bucket` / `tenant.unassign_bucket` | 归属变更 | `target`=租户，`bucket` |
| `quota.set` / `quota.clear` | `?quota` | `bucket`，`detail`=上限 |
| `quota.reject` | 任何配额拒绝 | `bucket`，`detail`=超限的轴 |
| `bucket.create` / `bucket.delete` | 数据面建删桶 | `bucket` |
| `usage.rescan` | 按需扫描 | `bucket`，`detail`=结果 |
| `access` | **仅 `audit.data_plane=true`**：每个请求一条 | `method`/`path`/`status`/`bytes`/`bucket`/`key`/`tenant` |

控制面事件逐条 flush（少而关键），`access` 记录不逐条 flush（高 QPS 下由
sink 缓冲）。`access` 是 roadmap §5.2 "结构化访问日志"的审计侧版本；§5.2 的
运行日志异步化与慢日志仍是独立事项。

## 6. 管理 API 参考

与 `/-/admin/credentials` 同一套 JSON 约定（错误体 `{"code","message"}`，
HTTP 状态取自 S3 错误表）。

| 方法与路径 | 权限 | 说明 |
| --- | --- | --- |
| `POST /-/admin/tenants` | root | `{"id","display_name"?,"quota"?:{max_bytes,max_objects,max_buckets}}` → 201；重名 `TenantAlreadyExists`(409) |
| `GET /-/admin/tenants` | root / 本租户 admin | 列表，每项含 `buckets`、聚合 `usage`、`credentials` 计数 |
| `GET /-/admin/tenants/{id}` | root / 本租户 admin | 单个；不存在 `NoSuchTenant`(404) |
| `PUT /-/admin/tenants/{id}` | root | `display_name` / `quota`（**整体替换**，未给的轴变为不限） |
| `DELETE /-/admin/tenants/{id}` | root | 仍拥有桶或凭证时 `TenantNotEmpty`(409) |
| `PUT /-/admin/tenants/{id}/buckets/{bucket}[?force=true]` | root | 指派归属；桶须存在；他租户所有且无 force → `BucketAlreadyExists`(409) |
| `DELETE /-/admin/tenants/{id}/buckets/{bucket}` | root | 解除归属（须当前归该租户） |
| `GET /-/admin/usage[?tenant=id]` | root / 本租户 admin | 全部（或按归属过滤）桶的计数器；admin 固定为本租户 |
| `GET /-/admin/usage/{bucket}` | root / 归属租户 admin | `{bucket,tenant?,objects,bytes,mpu_bytes,scanned,scanned_at?,quota?}` |
| `POST /-/admin/usage/{bucket}/rescan` | root / 归属租户 admin | 同步全量计数并返回结果；并发扫描 `SlowDown`(503) |
| `POST /-/admin/credentials` | root / 租户 admin | body 新增 `"tenant"`（须存在，`NoSuchTenant`）与 `"role"` |
| `PUT /-/admin/credentials/{ak}` | root / 租户 admin | 新增 `"tenant"`（root 专属，`null` 解除）与 `"role"` |

## 7. 配置

```yaml
usage:
  enabled: true              # false = 不计数、不执行配额（也免去写前 HEAD）
  flush_interval: 60s        # 脏计数器落盘周期；0 = 仅退出时
  reconcile_interval: 1d     # 全量重扫周期；0 = 只按需
  reconcile: true            # 多网关时只在一个实例为 true（负责 bootstrap 与周期重扫）
audit:
  path: ""                   # 空 = 关；例：/var/log/lights3/audit.log
  data_plane: false          # true = 每个数据面请求一条 access 记录（需 path）
  max_size: 64MiB            # 轮转阈值 [64KiB, 64GiB]
  max_files: 10              # 保留的轮转文件数 [1, 1000]
```

多实例的记录同步复用 `auth.sync_interval`（quota/tenants/owners 三个存储
与 usage 的采纳都挂在它上面）。

## 8. CLI

`s3adm quota get|set|clear <bucket>`、`s3adm tenant list|get|create|update|
delete|assign|unassign`、`s3adm usage [bucket] [--rescan] [--tenant=]`、
`s3adm cred create --tenant= --role=`，详见 [cli.md §3.6–§3.8](cli.md)。

## 9. 测试

- 单测 `tests/unit/test_tenancy.cc`：每条写路径的计数增量（含 MPU 三态）、
  重启恢复 + bootstrap、禁用开关、admin usage API；桶配额两轴 + 覆盖写抵扣 +
  copy 门；MPU 半途语义（分片受限、调低后 complete 拒绝、abort 释放、满桶
  拒开新上传）；租户生命周期 API、数据面隔离（外租户 403 / 不存在 404 /
  不能认领未归属桶 / copy 源 / ListBuckets 与 Owner）、租户聚合配额与桶数
  上限、租户 admin 作用域（凭证面、租户面、用量面、?quota）、会话继承租户但
  非 admin、凭证文件的 tenant/role、lifecycle 过期扣减、审计文件的事件与
  数据面记录、配置解析与校验。
- e2e：`run_e2e.sh` 对全部后端变体跑同一段 §3.9 用例（usage API、?quota
  往返与 403、租户建桶/桶数上限/隔离/ListBuckets、租户删除守卫、`s3adm
  usage/quota/tenant`）。
