# Usage Accounting, Quotas and Multi-Tenancy (roadmap §3.9)

> Status: all four items landed (2026-09-04). Code: `src/s3/usage.{h,cc}`,
> `src/s3/quota.{h,cc}`, `src/s3/tenant.{h,cc}`, `src/s3/audit.{h,cc}`,
> `src/s3/handlers/{quota_gate,bucket_quota,admin_tenants}.cc`; CLI
> `src/tools/s3adm_{usage,quota,tenant}.cc`. Unit tests in
> `tests/unit/test_tenancy.cc`, e2e in the "roadmap §3.9" section of
> `tests/e2e/run_e2e.sh`.

## 1. Goals and Boundaries

Roadmap §3.9 defines four items as one dependency chain: **usage accounting →
quotas → tenant entities → audit log**. This document follows the chain. One
principle runs through all of it: **everything lives at L2 (the S3 service
layer); the storage layer and the meta schema are untouched** —

- usage counters are maintained by the gateway, persisted as
  `.sys/usage/<bucket>`, and calibrated with plain `IStorageBackend` listings;
  none of the four IMetaStores or six backends knows they exist;
- quotas, tenants and bucket ownership are JSON records under `.sys` (the
  `SysConfigStore` write-through + tombstone-sync pattern shared with
  cors/lifecycle);
- a tenant credential is just a dynamic/file credential carrying two more
  fields (`tenant`, `role`); the verification path is unchanged.

The price: counters are "approximate between two full recounts" (§2.4) rather
than strongly consistent inside a meta transaction — the "offline aggregation"
option of the two the roadmap offered; §2.5 gives the reasoning.

## 2. Usage Accounting

### 2.1 Data Model

Three counters per bucket (`BucketUsage`, `src/s3/usage.h`):

| Field | Meaning |
| --- | --- |
| `objects` | committed objects |
| `bytes` | committed object bytes |
| `mpu_bytes` | bytes of in-flight multipart parts (**counted toward quotas**, §3.4) |
| `scanned_at` | time of the last full count; epoch = never scanned (counters started from zero, flagged "approximate") |

### 2.2 Incremental Maintenance

Every write path applies its delta after the backend commit
(`S3Service::note_usage`):

| Operation | objects | bytes | mpu_bytes |
| --- | --- | --- | --- |
| PutObject / CopyObject | +1 for a new key | +bytes actually written − replaced object's size | — |
| DeleteObject / DeleteObjects | −1 if it existed | −deleted object's size | — |
| UploadPart / UploadPartCopy | — | — | +part size |
| CompleteMultipartUpload | +1 for a new key | +sum of the parts named in the request − replaced size | −sum of every stored part of the upload |
| AbortMultipartUpload | — | — | −sum of every stored part |
| Lifecycle expiry / stale-MPU abort | as the two rows above | | |
| CreateBucket / DeleteBucket | zero counter created / record dropped | | |

The "replaced/deleted size" comes from one `head_object` before the write
(`existing_size`). With the §3.8 metadata cache that is one stat or a cache
hit; on cloudproxy it is one remote HEAD. `usage.enabled=false` skips the
whole chain (HEAD, counters, quotas). The bytes PutObject/UploadPart actually
wrote are counted by a `ByteCountingReader` on the body (backends must read to
EOF by contract); the client's declared length is never trusted for the delta.

### 2.3 Persistence and Multiple Gateways

- **flush**: dirty counters are written to `.sys/usage/<bucket>` every
  `usage.flush_interval` (default 60s) and once more at shutdown. A restart
  resumes from the records; buckets without one are counted by the startup
  **bootstrap scan** (§2.4).
- **multi-gateway**: each instance sees only its own deltas. With
  `auth.sync_interval` on, instances periodically re-read `.sys/usage/*` and
  **adopt only records whose `scanned_at` is newer than their own** (a peer's
  flush is just its partial view, never better than ours; a full scan is).
  Cross-gateway drift converges through reconcile.

### 2.4 Full Recount (reconcile)

`UsageTracker::rescan(bucket)` lists through the ordinary backend API:
`list_objects` pages accumulate object count and bytes,
`list_multipart_uploads` + `list_parts` accumulate in-flight parts; the result
replaces the bucket's counters wholesale and is persisted immediately. Three
triggers:

| Trigger | Notes |
| --- | --- |
| startup bootstrap | every existing bucket without a scan record, once (instances with `usage.reconcile=true`) |
| periodic | `usage.reconcile_interval` (default 1d, 0 = off) recounts every bucket; in a multi-gateway setup enable `usage.reconcile` on one instance only (the designated-instance semantics of duostore's `gc_enabled`) |
| on demand | `POST /-/admin/usage/<bucket>/rescan`, `s3adm usage <bucket> --rescan` |

**Accuracy contract**: exact at the moment a scan completes; between scans the
only error sources are (a) concurrent overwrites of the same key each
subtracting the same old size, (b) over-counting a re-uploaded part number
within one upload (complete/abort settle against the stored parts), (c) writes
by peer gateways. All three are erased by the next scan; counters clamp at
zero. Quota enforcement reads these counters and inherits the contract.

Writes landing during a scan may or may not be counted: `scanned_at` is the
scan's **start** time, so it never claims them. Scans of one bucket are
single-flight (a concurrent rescan gets `SlowDown`).

### 2.5 Why Not Meta-Side Counters

The roadmap listed two routes: counters inside the meta transaction (one
implementation per IMetaStore, with tikv needing the §3.7-style append-only
delta rows to avoid hot-row conflicts) or an offline aggregation scan. The
latter won because (1) of the six backends, localfs/xlocalfs/cloudproxy/tiered
have no transactional meta at all — the meta route would only cover duostore;
(2) quota use cases tolerate "approximate + periodic calibration" well (RGW's
bucket index stats and MinIO's data-usage crawler are the same kind of
number); (3) zero backend changes means every new backend gets accounting for
free. The scrub traversal (§3.1) was not reused directly — it does not cover
cloudproxy, while `list_objects` is uniform across backends.

### 2.6 Observability

| Metric | Notes |
| --- | --- |
| `lights3_bucket_usage_bytes{bucket}` / `_objects{bucket}` | callback gauges, at most 512 buckets (no new series beyond that — label-cardinality guard) |
| `lights3_usage_scans_total` / `lights3_usage_last_scan_timestamp_seconds` | full counts completed / time of the last one |
| `lights3_quota_rejections_total{scope=bucket\|tenant}` | quota refusals |

## 3. Quotas

### 3.1 Bucket Quota

`.sys/quota/<bucket>` holds `{max_bytes, max_objects}` (0 = that axis
unlimited; a record with both zero is invalid — delete it to mean "no quota").
The management surface is the `?quota` subresource (no AWS equivalent; the XML
shape is this implementation's own):

```text
GET    /bucket?quota   any credential dispatch admitted to the bucket (a tenant may read its own limit); none set -> 404 NoSuchQuotaConfiguration
PUT    /bucket?quota   root only; the bucket must exist; refused when usage.enabled=false (an unenforceable limit must not be configurable)
DELETE /bucket?quota   root only, idempotent 204
```

```xml
<QuotaConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <MaxBytes>53687091200</MaxBytes>
  <MaxObjects>1000000</MaxObjects>
</QuotaConfiguration>
```

### 3.2 Tenant Quota

The tenant record (§4.1) carries `quota{max_bytes, max_objects, max_buckets}`:
the first two axes sum the usage of **every bucket the tenant owns** (including
`mpu_bytes`); `max_buckets` is counted from ownership records at CreateBucket.
Bucket and tenant limits apply together, bucket first.

### 3.3 Enforcement Points and Error Code

`S3Service::check_quota(bucket, add_bytes, add_objects)` is pure in-memory
arithmetic (counters + this request's delta > limit → refuse), decided before
any body byte streams:

| Operation | add_bytes | add_objects |
| --- | --- | --- |
| PutObject | declared length (decoded length for aws-chunked) − replaced object's size | 1 for a new key |
| CopyObject | source size − replaced size | 1 for a new key |
| CreateMultipartUpload | 0 (a bucket already over its limit refuses new uploads outright) | 0 |
| UploadPart / UploadPartCopy | part size | 0 |
| CompleteMultipartUpload | sum of the named parts − sum of every stored part of the upload − replaced size | 1 for a new key |

A refusal returns **`QuotaExceeded` (HTTP 403)** — Ceph RGW's choice (AWS S3
has no comparable code; MinIO also answers 403). The message reads like
`The request would exceed the bucket quota: bytes 12 + request > 20 (bucket).`,
tenant-level ones carry `tenant '<id>'`. Every refusal increments the metric and
writes an audit record (§5).

### 3.4 Mid-Flight Multipart Semantics

The roadmap asked for the multipart mid-flight semantics to be defined:

1. **Parts consume quota.** UploadPart is judged by the part's size and adds it
   to `mpu_bytes` — the part really occupies disk; not counting it would be a
   bypass.
2. **Complete refuses only when the finished object still would not fit.** The
   parts were admitted one by one at UploadPart, and Complete's net delta is
   "named parts − all parts − replaced object" (normally ≤ 0), so the normal
   case always passes; only a quota lowered after the parts were uploaded, or a
   peer gateway filling the bucket, yields a 403.
3. **A refused upload stays in place** for the client to Abort (which releases
   `mpu_bytes`) or for lifecycle's `AbortIncompleteMultipartUpload` to clean
   up. No "refuse = auto-abort": a transient overrun must not swallow parts
   the client already paid to upload.

### 3.5 Known Trade-offs

- Approximate counters (§2.4) make quotas approximate: in a multi-gateway
  setup a bucket may exceed its limit by one scan period's drift. Shorten
  `usage.reconcile_interval` for a tighter bound.
- A bucket that was never scanned (no record) is admitted as zero usage —
  availability over false refusals; the startup bootstrap scan closes the gap
  quickly.
- With an unknown declared length (chunked without
  `x-amz-decoded-content-length`) the pre-check judges 0 and the actual bytes
  are counted after the write; the next request is bounded.

## 4. Tenants

### 4.1 Model and Storage

```text
.sys/tenants/<id>    {id, display_name, created, quota{max_bytes,max_objects,max_buckets}, rev}
.sys/owners/<bucket> {tenant, assigned_by, assigned}
credential objects / credentials_file entries: new "tenant": "<id>", "role": "user"|"admin"
```

- Tenant id: `[a-z0-9][a-z0-9._-]{0,63}`; it doubles as the `.sys` object
  key and the `Owner/ID` value.
- **Ownership** is written in two places: automatically after a tenant
  credential's CreateBucket succeeds (if the record cannot be written the
  bucket is rolled back — a tenant must never be left with a bucket it can no
  longer touch), or by root through
  `PUT /-/admin/tenants/<id>/buckets/<bucket>` (a bucket owned by another
  tenant needs `?force=true`). DeleteBucket drops the ownership, quota and
  usage records with it.
- **A bucket without an ownership record is unowned (legacy)**: root and
  tenant-less credentials see exactly the world they saw before this feature;
  tenant credentials never see it. This is the backward-compatibility key:
  existing deployments migrate nothing — tenancy is layered on, not swapped in.

### 4.2 Three Kinds of Identity

```text
static credential (config)                  = root: everything
dynamic/file credential without tenant      = legacy: every bucket on the data plane (subject to policy), as before
dynamic/file credential with tenant=<id>    = tenant credential: only its tenant's buckets (then policy)
    role=admin                              + its tenant's admin plane (§4.4)
STS session                                 inherits the parent's tenant, never admin
```

`tenant`/`role` are snapshotted at verify time together with the policy
(`VerifiedIdentity`, the same reason as docs/archive/gaps.md §3.7), so in-flight
requests are unaffected by concurrent edits. `role` accepts only
`user`/`admin`; anything else is `InvalidRequest` (a typo in the credentials
file is a startup error, never a silent downgrade).

### 4.3 Data-Plane Authorization

After the policy decision, dispatch adds the ownership decision
(`require_tenant_bucket`) for tenant credentials on bucket-addressed requests:

| Case | Result |
| --- | --- |
| bucket owned by this tenant | admitted |
| CreateBucket and no ownership record | admitted; the handler then checks existence — an existing bucket is `BucketAlreadyExists` (409): a tenant cannot "claim" an existing unowned bucket |
| otherwise: bucket does not exist | `NoSuchBucket` (404) — SDK HeadBucket probes rely on it |
| otherwise: exists but foreign / unowned | `AccessDenied` (403) |
| CopyObject / UploadPartCopy source bucket | judged the same way |

ListBuckets lists only the tenant's buckets and reports the tenant id and
display name as `Owner` (root/legacy still get `lights3`); ListObjectsV2
`fetch-owner` reports the bucket's owner tenant.

### 4.4 Tiered Admin Plane

| Operation | root | tenant admin | tenant user |
| --- | --- | --- | --- |
| `/-/admin/credentials` CRUD | all | own tenant's credentials only (POST's tenant is pinned to its own; foreign AKs read as `InvalidAccessKeyId`, never enumerable; cannot change `tenant`) | 403 |
| `/-/admin/tenants` | all | `GET` of its own tenant | 403 |
| `/-/admin/usage` | all | query + rescan of its own buckets | 403 |
| `?quota` | GET/PUT/DELETE | GET | GET |
| `?cors` / `?lifecycle` / `?website` | all | 403 (unchanged) | 403 |

Root is still decided by a live "static source" lookup (`is_root`); tenant
admin by the verify-time snapshot — the two coexist for the reasons in
credential-management.md §7.

### 4.5 Deferred

- A tenant admin cannot set bucket quotas itself — limits are an operator
  decision.
- No bucket policy / ACL semantics; fine-grained control inside a tenant is
  still per-credential policy.
- The session table stays single-instance in-memory (the existing §2.6
  constraint).

## 5. Audit Log

A non-empty `audit.path` enables it: a dedicated spdlog rotating file
(`audit.max_size` × `audit.max_files`), separate from the operational log
(changing `log.level` does not affect it), one JSON object per line; absent
fields are **omitted, never empty strings**:

```json
{"ts":"2026-09-04T12:00:00Z","event":"quota.reject","actor":"L3AK...","tenant":"acme",
 "request_id":"...","bucket":"logs","detail":"bytes 12 + request > 20 (bucket)"}
```

| Event | Trigger | Key fields |
| --- | --- | --- |
| `cred.create` / `cred.update` / `cred.delete` | admin-plane credential lifecycle | `target`=AK, `detail`=tenant/role or the update body |
| `cred.show_secret` | `?show-secret=true` (granted or not) | `detail`=granted / refused (static) |
| `sts.assume_role` | session minted | `target`=session AK |
| `tenant.create` / `tenant.update` / `tenant.delete` | tenant lifecycle | `target`=tenant id |
| `tenant.assign_bucket` / `tenant.unassign_bucket` | ownership change | `target`=tenant, `bucket` |
| `quota.set` / `quota.clear` | `?quota` | `bucket`, `detail`=limits |
| `quota.reject` | any quota refusal | `bucket`, `detail`=the exceeded axis |
| `bucket.create` / `bucket.delete` | data-plane bucket create/delete | `bucket` |
| `usage.rescan` | on-demand scan | `bucket`, `detail`=result |
| `access` | **only with `audit.data_plane=true`**: one per request | `method`/`path`/`status`/`bytes`/`bucket`/`key`/`tenant`/`trace_id` (roadmap §5.4) |

Control-plane events are flushed one by one (rare and critical); `access`
records are not (buffered by the sink under high QPS). `access` is the audit
counterpart of roadmap §5.2's "structured access log"; §5.2's async operational
logging and slow-request log remain separate items.

## 6. Admin API Reference

Same JSON conventions as `/-/admin/credentials` (error body
`{"code","message"}`, HTTP status from the S3 error table).

| Method and path | Who | Notes |
| --- | --- | --- |
| `POST /-/admin/tenants` | root | `{"id","display_name"?,"quota"?:{max_bytes,max_objects,max_buckets}}` → 201; duplicate `TenantAlreadyExists` (409) |
| `GET /-/admin/tenants` | root / own tenant admin | list; each entry carries `buckets`, aggregate `usage`, `credentials` count |
| `GET /-/admin/tenants/{id}` | root / own tenant admin | one tenant; missing `NoSuchTenant` (404) |
| `PUT /-/admin/tenants/{id}` | root | `display_name` / `quota` (**replaced as a whole**; axes not given become unlimited) |
| `DELETE /-/admin/tenants/{id}` | root | `TenantNotEmpty` (409) while it still owns buckets or has credentials |
| `PUT /-/admin/tenants/{id}/buckets/{bucket}[?force=true]` | root | assign ownership; the bucket must exist; owned by another tenant without force → `BucketAlreadyExists` (409) |
| `DELETE /-/admin/tenants/{id}/buckets/{bucket}` | root | detach (must currently belong to that tenant) |
| `GET /-/admin/usage[?tenant=id]` | root / own tenant admin | counters of every bucket (or filtered by owner); an admin is pinned to its tenant |
| `GET /-/admin/usage/{bucket}` | root / owner tenant's admin | `{bucket,tenant?,objects,bytes,mpu_bytes,scanned,scanned_at?,quota?}` |
| `POST /-/admin/usage/{bucket}/rescan` | root / owner tenant's admin | synchronous full count, returns the result; concurrent scan `SlowDown` (503) |
| `POST /-/admin/credentials` | root / tenant admin | body gains `"tenant"` (must exist, else `NoSuchTenant`) and `"role"` |
| `PUT /-/admin/credentials/{ak}` | root / tenant admin | gains `"tenant"` (root only, `null` detaches) and `"role"` |

## 7. Configuration

```yaml
usage:
  enabled: true              # false = no counters, no quota enforcement (also skips the pre-write HEAD)
  flush_interval: 60s        # dirty-counter persistence period; 0 = only at shutdown
  reconcile_interval: 1d     # full recount period; 0 = on demand only
  reconcile: true            # multi-gateway: true on one instance only (bootstrap + periodic recounts)
audit:
  path: ""                   # empty = off; e.g. /var/log/lights3/audit.log
  data_plane: false          # true = one access record per data-plane request (needs path)
  max_size: 64MiB            # rotation threshold [64KiB, 64GiB]
  max_files: 10              # rotated files kept [1, 1000]
```

Multi-instance record sync reuses `auth.sync_interval` (the quota/tenant/owner
stores and usage adoption all hang off it).

## 8. CLI

`s3adm quota get|set|clear <bucket>`, `s3adm tenant list|get|create|update|
delete|assign|unassign`, `s3adm usage [bucket] [--rescan] [--tenant=]`,
`s3adm cred create --tenant= --role=`; see [cli.md §3.6–§3.8](cli.md).

## 9. Tests

- Unit (`tests/unit/test_tenancy.cc`): deltas on every write path (incl. the
  three multipart states), restart recovery + bootstrap, the disable switch,
  the admin usage API; both bucket-quota axes + overwrite netting + the copy
  gate; multipart mid-flight semantics (parts bounded, complete refused after
  lowering, abort releases, a full bucket refuses new uploads); tenant
  lifecycle API, data-plane isolation (foreign 403 / missing 404 / no claiming
  of unowned buckets / copy source / ListBuckets and Owner), tenant aggregate
  quota and bucket cap, tenant-admin scoping (credential, tenant and usage
  planes, ?quota), sessions inheriting the tenant but never admin,
  credentials-file tenant/role, lifecycle expiry deltas, audit file events and
  data-plane records, config parsing and validation.
- e2e: `run_e2e.sh` runs the same §3.9 section against every backend variant
  (usage API, ?quota round trip and 403, tenant bucket creation / bucket cap /
  isolation / ListBuckets, tenant deletion guard, `s3adm usage/quota/tenant`).
