# S3 Tables: Apache Iceberg REST Catalog (design after studying RustFS)

> Status: **design draft (2026-09-11), not implemented**. The document first answers
> "how does RustFS do S3 Tables" (§2, verified against source @853ae63 on
> 2026-09-11), then gives the lights3 plan (§3–§13) and the implementation steps
> (§14). Once code lands this file stays as the design-level document per repo
> convention and implementation details move into an implementation document;
> source comments cite it as `docs/s3-tables-design.md §N`.
>
> One-line conclusion: **lights3 builds the table catalog as "an Iceberg REST
> Catalog on top of the `.sys` bucket"** — one JSON object per catalog entity, the
> atomic conditional write `PutCondition` that every backend already implements
> (`If-None-Match: *` / `If-Match: <etag>`, `storage/backend.h`) serves as the CAS
> for the metadata-pointer commit; the REST surface hangs off the `/iceberg/v1`
> path prefix and reuses SigV4, per-credential policy, STS sessions and AdminJobs;
> Iceberg metadata is produced and validated **server-side**, so clients
> (PyIceberg / DuckDB / Spark / Trino) only need a standard REST catalog
> configuration. Verbatim parity with the AWS S3 Tables control-plane API is not a
> goal (it is not one for RustFS either).

Related: [s3-protocol.md](s3-protocol.md) (route table, SigV4, conditional requests),
[credential-management.md](credential-management.md) §10 (policy and STS sessions),
[multi-tenancy.md](multi-tenancy.md) (quota and audit hooks),
[storage/storage-backend.md](storage/storage-backend.md) §1 (the `PutCondition` contract),
[deployment.md](deployment.md) §5 (multi-gateway support matrix), [cli.md](cli.md) (AdminJobs and `lights3-ctl`).

## 1. Goals and non-goals

**Goals**:

1. **Table buckets**: an ordinary bucket becomes a table bucket after explicit
   enablement and serves as the warehouse of the Iceberg REST catalog; table data
   inside it stays ordinary S3 objects that engines read and write through the
   existing S3 data plane.
2. **Iceberg REST Catalog**: the namespace / table endpoint subset of the spec
   (§6.3); `GET /v1/config` advertises only implemented endpoints; PyIceberg and
   DuckDB work end to end, Spark / Trino REST configurations work (read-write and
   read-only).
3. **Atomic single-table commit**: CommitTable's "validate requirements → produce
   the new metadata.json → CAS the pointer" yields exactly one winner and retryable
   409s for the rest, even with several gateways sharing `.sys`.
4. **Table-aware data plane**: the reserved catalog prefix cannot be written or
   deleted by ordinary S3 requests; table buckets are excluded from lifecycle
   expiration; DeleteBucket refuses a non-empty table bucket; short-lived
   credentials scoped to a table prefix can optionally be vended.
5. **Maintenance**: metadata-file retention, snapshot expiration and orphan cleanup
   as operator-triggered AdminJobs, optionally periodic; every deletion is guarded
   by a safety window and a re-check of the current pointer.

**Non-goals (deliberately not done, reasons in §15)**: verbatim AWS S3 Tables
control-plane parity, multi-table transactions (`/transactions/commit`),
server-side scan planning (`/plan`, `/tasks`), remote signing, execution of
row-level / delete-file compaction, Delta / Hudi, built-in SQL, active-active
multi-region writes.

## 2. How RustFS does it (findings)

RustFS (Rust, Apache-2.0) built S3 Tables in 2026 directly into the object-store
core as "Iceberg REST Catalog + table buckets" (`rustfs/src/table_catalog/`,
`rustfs/src/admin/handlers/table_catalog/`, about 70k lines including tests). Its
official support matrix (`docs/architecture/s3-tables-support-matrix.md`) states
explicitly that it "does not claim parity with the AWS S3 Tables control-plane
API"; what it does claim is automated PyIceberg and DuckDB smoke coverage,
table-aware data-plane policy, controlled maintenance, commit-recovery diagnostics
and manual harnesses for Spark / Trino.

### 2.1 Overall shape

| Dimension | RustFS's choice |
| --- | --- |
| Protocol surface | Iceberg REST Catalog; prefix `/iceberg/v1` (MinIO AIStor-style alias `/_iceberg/v1`); `{prefix}` is the bucket name |
| Routing | both prefixes go through the **admin router** (`is_admin_path` includes `is_table_catalog_path`) and are claimed before S3 routing; no bucket name is reserved, so a bucket literally named `iceberg` has keys under `v1/` shadowed |
| Authentication | SigV4; the credential-scope service accepts `s3` and `s3tables` (`sig_v4_allowed_services`), no routing by service |
| Authorization | a custom `admin:*Table*` action family (`admin:CreateTable`, `admin:CommitTable`, `admin:GetTableMetadata`, `admin:SetTableMetadata`…) on resources `arn:aws:s3:::<bucket>/namespaces/<ns>/tables/<t>` |
| Where state lives | the **system bucket** `.rustfs.sys` under `s3tables/catalog/table-buckets/<sha256(bucket)>/…`; the user bucket only holds the marker `table-bucket.json` and Iceberg files under the reserved prefix `.rustfs-table/` |
| Consistency primitive | conditional writes on object ETags (`If-None-Match: *` / `If-Match`), plus distributed namespace locks serializing writers per catalog object |
| Backing modes | `object` (default) and `durable-strong` (the whole catalog in one ≤64 MiB snapshot object, single lock + ETag CAS) |
| Who produces metadata | **the server**: CreateTable / CommitTable generate and write metadata.json; a "legacy pointer" mode (client writes metadata, server only swaps the pointer) is kept |
| Maintenance | planning separated from execution; no built-in periodic scheduler, everything is operator-triggered |

### 2.2 State model and persistence

- Entities: `TableBucketEntry`, `NamespaceEntry` (multi-level, `a.b.c`, storage
  id joined with `/`), `TableEntry` (`table_id` and the Iceberg `table_uuid` are
  two independent uuids; `metadata_location` is a bucket-relative key;
  `version_token = "token-<uuid>"` renewed on every commit; `generation`
  monotonic; `state ∈ Active|Renaming|Deleting|Deleted`), `ViewEntry`,
  `CommitLogEntry` (`Staged|Committed|Failed`), `TableRenameIntent`, and the
  reverse index `TableWarehouseIndexEntry` (data prefix → owning table, used for
  data-plane authorization).
- Every persisted struct is `deny_unknown_fields` with a strict `version == 1`
  check: an older binary that meets a new field or state **fails closed**; the
  `Renaming` state deliberately exploits this so old readers reject it.
- Every read verifies that "the path recomputed from the entity's identity fields
  == the object path it was read from", so a copied or moved JSON is rejected.
- Metadata file names are `{generation:05}-{uuid}.metadata.json`, always under
  the reserved prefix `.rustfs-table/warehouses/default/namespaces/<ns>/tables/<t>/metadata/`,
  **independent of the table's `location`**.
- Identifiers: segments `[a-z0-9_-]`, first/last alphanumeric, ≤64 bytes;
  namespace total ≤512; `.` `/` `\` `%` forbidden (`%1F` is decoded by the handler
  before splitting).
- Page tokens: base64url of `{version, context: sha256(resource\0warehouse\0ns), cursor}`;
  replaying one against another list operation is a 400; default and maximum
  pageSize 1000.

### 2.3 Commit protocol (single-table CAS)

```text
1 take the table lock + publication fences (bucket-level publication.lock, table-level publication.lock)
2 read (TableEntry, etag0); must be Active
3 idempotency lookup: commits/<h(table_id)>/<h(commit_id)>.json, commit-idempotency/<h(key)>.json
   already Committed, or "Staged but the pointer advanced / provable from the commit chain"
   → re-finalize and return idempotently; different payload → 409
4 version_token / metadata_location must equal the expected ones, else 409
5 read the new metadata (≤50 MiB), optional assert-rustfs-metadata-sha256
6 STAGED: commit log (If-None-Match:*) + idempotency index
7 CAS: put table-entry.json If-Match: etag0          ← the only atomic point
8 finalize: commit log → COMMITTED (result deliberately ignored: "after the table CAS
   succeeds the staged record is the durable recovery source; a finalization failure
   must not turn an externally committed pointer into a failed response")
```

The "finalization gap" is classified by `catalog/diagnostics`
(`Committed | StagedBeforeTableUpdate | FinalizationRequired |
IdempotencyIndexRepairRequired | ManualReview`) and repaired by
`catalog/recovery`, which **never moves the pointer**. Rename is a two-phase
protocol with a persisted intent: source→`Renaming` → destination written →
source rewritten as a `Deleted` tombstone ("a conditional-replacement tombstone
instead of relying on an unconditional delete") → reverse index re-pointed →
destination `Active` → intent `Completed`; while the bucket's `active_rename_id`
is set every reader gets 503 and every writer first runs recovery.

### 2.4 REST surface highlights

- `GET /v1/config`: `defaults.warehouse = "default"` (a constant, not the bucket),
  `overrides["namespace-separator"] = "%1F"`, `endpoints` lists the 21 standard
  endpoints; extensions (refs, metadata-location, maintenance,
  catalog/{export,import,diagnostics,recovery,rollback,migration}, buckets/{w})
  are **not** advertised in `endpoints`.
- CreateTable: the server reassigns ids (schema-id 0, spec-id 0, partition field
  ids from 1000), produces the initial metadata (v1/v2, v3 → 406), writes it with
  `If-None-Match:*`, registers; responds 200 with a `LoadTableResult`; `config`
  contains **no** `s3.endpoint` / `s3.path-style-access`, only
  `rustfs.credential-*` descriptive keys. `stage-create` → 406.
- CommitTable: all 8 `assert-*` `requirements`; the full spec set of `updates`
  (including `set-statistics`, `remove-partition-specs`, `remove-schemas`),
  `add-encryption-key` and other v3 items → 406; the **server** applies updates,
  checks transition invariants (`last-column-id` / `last-partition-id` /
  `last-sequence-number` monotonic, existing schemas/specs/snapshots byte-identical),
  then reads manifest-list / manifest Avro and checks that every data/delete file
  a new snapshot references **exists** and lies inside the bucket, and replays the
  parent snapshot's live-file set to reject re-adding or deleting non-live files;
  writes `{gen+1:05}-<token>.metadata.json` (token = commit-id). `commit-id` /
  `idempotency-key` are **body fields**; a retry must reproduce the committed
  object byte for byte.
- Errors: `{"error":{"message","type","code"}}`; `PreconditionFailed`/412 →
  409 `CommitFailedException`; 404 picks `NoSuchNamespaceException` /
  `NoSuchTableException` / `NoSuchViewException` by path; `Unsupported` → 406
  `UnsupportedOperationException`; `Unavailable` → 503.
- Credential vending: only when `X-Iceberg-Access-Delegation` contains
  `vended-credentials` and the server switch is on; one STS temporary user with an
  embedded session policy (Get/Put/Delete/AbortMultipart under the table prefix +
  read-only on the metadata object); TTL 60–3600 s; `Cache-Control: no-store, private`.

### 2.5 Data-plane integration

- Reserved prefix `.rustfs-table/`: in a table bucket Put / Copy destination /
  Delete / Restore / every multipart step on it is rejected (`InvalidRequest
  "Object key is reserved for the table catalog"`), reads pass; when bucket
  metadata cannot be loaded the guard **defaults to reject**.
- "Table-aware policy bridge": after IAM allows an ordinary S3 object operation,
  the owning table is looked up through the reverse index and a second
  `admin:GetTableMetadata` / `admin:SetTableMetadata` check is applied; anonymous
  access to table prefixes is always denied; content mutations additionally hold
  shared read locks on the publication fences so a commit cannot publish in the
  middle of a data write.
- Lifecycle: table buckets are excluded from expiration wholesale (including
  already queued work); only catalog maintenance deletes table files.
- DeleteBucket: objects remaining under `.rustfs-table` → `BucketNotEmpty`;
  deleting a bucket also purges its catalog state in the system bucket.

### 2.6 Maintenance

Configuration (`retain-recent-metadata-files`, `delete-enabled`,
`worker-lease-timeout-seconds`, retry backoff, quarantine…) resolves at three
levels (table → table bucket → default). Planning: the metadata retained set =
current pointer ∪ `metadata-log` ∪ newest N ∪ reachable from protected refs;
reachability cleanup walks manifest-list → manifest → data/delete files and **any
unparsable item puts everything into manual review**; deletion only targets
objects whose mtime predates the 15-minute safety window and that are still
candidates after re-checking the current pointer. Snapshot expiration produces a
plan first and then commits through a standard `remove-snapshots` commit; a stale
plan fails closed. Job state machine
`NotYetRun|Queued|Running|Successful|Failed|Disabled|Paused` with scheduler lease /
worker lease / heartbeat / backoff retries; no in-process periodic scheduler.

### 2.7 Tests

About 630 server unit tests run on an in-memory object backend with fault
injection (read/put/delete failures by attempt index, failure after put, pause
barriers, etag-less objects); the PyIceberg and DuckDB smoke scripts under
`scripts/table-catalog/` are the only basis for "supported" claims;
`failure_coverage.py` generates fault-probe plans (stale-token conflict, post-CAS
finalization gap, single CAS winner, no data-plane bypass on denial, stale
maintenance plan rejected…).

### 2.8 Borrowed vs. not copied

| Borrowed (done the same way) | Not copied (lights3 chooses otherwise) | Reason |
| --- | --- | --- |
| catalog state in the system bucket, one object per entity, ETag conditional writes as CAS | distributed namespace locks | lights3 has no lock service; `PutCondition` atomicity is guaranteed by each backend at its commit point, cross-gateway convergence via CAS suffices (§5.5) |
| staged commit log before the pointer CAS; finalization failure never rolls back the response | a separate idempotency index object | the idempotency key is folded into the commit-log object name, one write less (§5.3) |
| server-side metadata.json production/validation; full requirement/update sets; transition invariants | legacy pointer mode | the pointer is written only by the server and the S3 plane is read-only on the reserved prefix, smaller attack surface; register goes "read → validate → copy into the reserved directory" (§6.5) |
| reserved prefix read-only, lifecycle exclusion, DeleteBucket guard | reverse index + second IAM check | lights3's policy is a bucket-glob + key-prefix allowlist and the table path *is* the key path, so prefix scoping comes **for free** without a second action family (§6.2) |
| context-bound page tokens; identifier character set | `defaults.warehouse = "default"` | lights3 returns the bucket name, which is more intuitive to configure |
| maintenance "plan / execute split + safety window + pointer re-check" | lease / heartbeat / quarantine state machine | lights3 already has AdminJobs (one job per backend, `JobInProgress`) and an optional periodic runner, which is enough (§9) |
| `X-Iceberg-Access-Delegation`-negotiated credential vending | IAM temporary users | reuse STS sessions (`mint_session`) with a "narrowed policy" parameter (§8.4) |
| Avro reading of manifests for file-existence validation | pulling in avro-cpp | a minimal in-tree Avro OCF reader (read-only, null/deflate codecs) (§7.4, §11) |
| 406 rather than silence for stage-create / purge / v3 | the `/_iceberg/v1` alias and the `s3tables` signing name | optional, in §14 ⑥, off by default |
| durable-strong single-snapshot backing | — | not done: if lights3 needs a strong backing it implements `ITableCatalogStore` on duostore meta (KV transactions) (§12) |

## 3. Architectural position

A new **Tables subsystem** (`src/tables/`) inside L2, parallel to the S3
handlers; downward it depends only on `IStorageBackend` (via `BucketRouter`) and
the `.sys` default backend, upward it plugs into `S3Service::dispatch` through a
path-prefix branch. No new storage interface.

```text
                 HttpRequest (path-style)
                          │
        ┌─────────────────▼──────────────────┐
        │ S3Service::dispatch                │
        │  /-/…      → admin plane           │
        │  /iceberg/v1/… → tables::RestApi   │  ← new branch (§6.1)
        │  otherwise → S3 route table        │
        └───────┬───────────────────┬────────┘
                │                   │ table-bucket guard (§8.1): reserved prefix
                │                   │ read-only, lifecycle exclusion, DeleteBucket guard
   ┌────────────▼─────────────┐     │
   │ tables::RestApi           │     │
   │  auth (SigV4 s3|s3tables) │     │
   │  authz (CredentialPolicy) │     │
   │  JSON codec / error model │     │
   └──┬───────────────┬───────┘     │
      │               │             │
┌─────▼──────┐ ┌──────▼──────────┐  │
│ Catalog    │ │ iceberg::       │  │
│ (§4,§5)    │ │  Metadata model │  │
│ ITableCata-│ │  requirements/  │  │
│ logStore   │ │  updates/transi-│  │
│ Object impl│ │  tion, Avro rdr │  │
└─────┬──────┘ └──────┬──────────┘  │
      │ .sys default   │ backend of  │
      │ backend        │ the bucket  ▼
        IStorageBackend (resolved by BucketRouter)
```

Three boundaries:

- **`ITableCatalogStore`** (§4.2): read / list / conditional write of catalog
  entities. First implementation `ObjectCatalogStore` (`.sys` objects +
  `PutCondition`); a duostore-meta implementation is reserved.
- **`iceberg::TableMetadata`** (§7): a purely functional JSON model with
  validation and update application, storage-agnostic, exhaustively unit-testable.
- **`TableBucketGuard`** (§8): the guard hook for the S3 plane, reads only the
  `TableBucketStore` snapshot, zero extra IO.

## 4. Data model and key layout

### 4.1 Table buckets

`TableBucketStore = SysConfigStore<TableBucketTraits>` (`kPrefix = "tables/"`),
one object per bucket `.sys/tables/<bucket>`:

```json
{"version":1,"enabled":true,"reserved_prefix":".lights3-table/",
 "properties":{},"created_unix":1757548800}
```

Enablement is root-only (the same two-tier model as `?website` / `?cors`): the
REST extension `PUT /iceberg/v1/buckets/{bucket}`, the admin plane
`PUT /-/admin/tables/buckets/<bucket>` and the CLI
`lights3-ctl tables enable <bucket>` share one implementation. Preconditions:
the bucket exists, contains **no** object under the reserved prefix, and is not
`.sys`. Disabling (`DELETE`) requires an empty catalog (no namespaces). The
table-bucket snapshot is pinned per request (`SysConfigStore::snapshot()`), so
the guard's lookup is an O(log n) in-memory check.

### 4.2 Catalog entities and `ITableCatalogStore`

```cpp
// src/tables/catalog_store.h
struct NamespaceEntry { int version=1; std::vector<std::string> levels; std::map<std::string,std::string> properties;
                        int64_t created_unix, updated_unix; };
enum class TableState { Active, Renaming, Deleted };
struct TableEntry {
    int version = 1;
    std::string table_id;            // uuid v4, storage identity (paths, log keys)
    std::string table_uuid;          // Iceberg table-uuid (adopted from register)
    std::string location;            // s3://<bucket>/<ns-path>/<table>
    std::string metadata_location;   // bucket-relative key, always under the reserved prefix (§4.4)
    std::string version_token;       // "t-" + 22 base64url random chars; renewed per commit
    uint64_t    generation = 1;      // monotonic, also the metadata file-name ordinal
    int         format_version = 2;
    TableState  state = TableState::Active;
    std::string rename_id;           // intent id while Renaming
    int64_t     created_unix, updated_unix;
};
struct CommitRecord {                // commit log (§5.3)
    std::string commit_id, table_id, expected_token, new_token,
                prev_metadata_location, new_metadata_location;
    std::string status;              // STAGED | COMMITTED
    nlohmann::json request_digest;   // sha256(canonical requirements+updates), compared on replay
    int64_t created_unix;
};
struct Versioned<T> { T value; std::string etag; };

struct ITableCatalogStore {
    virtual Task<std::optional<Versioned<NamespaceEntry>>> get_namespace(bucket, levels) = 0;
    virtual Task<ListPage<NamespaceEntry>>  list_namespaces(bucket, parent_levels, PageCursor) = 0;
    virtual Task<void> put_namespace(bucket, NamespaceEntry, PutCondition) = 0;
    virtual Task<void> delete_namespace(bucket, levels) = 0;             // emptiness checked above
    virtual Task<std::optional<Versioned<TableEntry>>> get_table(bucket, levels, name) = 0;
    virtual Task<ListPage<std::string>>     list_tables(bucket, levels, PageCursor) = 0;
    virtual Task<std::string>  put_table(bucket, levels, name, TableEntry, PutCondition) = 0;  // → new etag
    virtual Task<void> delete_table(bucket, levels, name) = 0;
    virtual Task<std::optional<Versioned<CommitRecord>>> get_commit(bucket, table_id, commit_id) = 0;
    virtual Task<void> put_commit(bucket, table_id, CommitRecord, PutCondition) = 0;
    virtual Task<std::vector<CommitRecord>> list_commits(bucket, table_id) = 0;   // diagnostics / recovery
    // rename intents (§5.6)
    virtual Task<std::optional<Versioned<RenameIntent>>> get_rename(bucket, id) = 0;
    virtual Task<void> put_rename(bucket, RenameIntent, PutCondition) = 0;
    virtual Task<void> delete_rename(bucket, id) = 0;
};
```

The interface is **semantic** (entities, not raw KV) — the same trade-off as
duostore's `IMetaStore` ([storage/duostore-design.md](storage/duostore-design.md) §2.1),
so a duostore-meta implementation can map listing onto ordered iteration and CAS
onto transactions instead of emulating object semantics.

### 4.3 `.sys` key layout (`ObjectCatalogStore`)

```text
.sys/tables/<bucket>                                    table-bucket marker (§4.1, SysConfigStore)
.sys/tables-catalog/<bucket>/ns/<l1>/<l2>/_ns.json      NamespaceEntry (one segment per level)
.sys/tables-catalog/<bucket>/ns/<l1>/<l2>/tbl/<table>.json    TableEntry (the current pointer)
.sys/tables-catalog/<bucket>/commits/<table_id>/<commit_id>.json   CommitRecord
.sys/tables-catalog/<bucket>/renames/<rename_id>.json   RenameIntent
```

- `_ns.json` and `tbl/` are reserved segments; the identifier rules (§4.5)
  guarantee user names cannot collide with them.
- Listing child namespaces = `list_objects(prefix="…/ns/<l1>/", delimiter="/")`
  minus `tbl/` from `common_prefixes`; listing tables = `prefix="…/tbl/"`. Both
  are the existing `ListOptions` prefix + delimiter + start_after: naturally
  ordered, naturally paginated.
- **Namespace existence is evidence-based** (borrowed from RustFS): an
  `_ns.json`, or any table / child namespace under its prefix, counts as
  existence; `create_namespace a.b` does not require an explicit record for `a`.
- Every entity body carries `bucket` / `levels` / `name`, recomputed and compared
  against the object key on read; a mismatch counts as corruption (protects
  against tools like `duostore dump/load` moving things around).
- Catalog state lives on the **default backend** like every other `.sys` state;
  the table bucket itself can be routed to any backend. `.sys` objects are shielded
  by `validate_bucket_name(allow_reserved=false)`, the S3 plane cannot reach them.

### 4.4 Layout inside the table bucket (reserved prefix)

```text
<bucket>/
├── .lights3-table/                                   reserved prefix: S3 plane read-only (§8.1)
│   └── <l1>/<l2>/<table>/metadata/
│       ├── 00001-<uuid>.metadata.json                produced by CreateTable
│       ├── 00002-<uuid>.metadata.json                +1 per CommitTable
│       └── …
└── <l1>/<l2>/<table>/                                 table location (default)
    ├── data/…parquet                                  written by engines via the S3 plane
    └── delete/…                                       v2 delete files
```

- Metadata **always** lives under the reserved prefix, decoupled from
  `location`: engines can only swap the pointer through the catalog, the S3 plane
  cannot forge or overwrite metadata.json; `metadata-log[].metadata-file` and
  `metadata-location` are returned as `s3://<bucket>/.lights3-table/…` and engines
  read them with S3 GET (reads pass).
- Default `location = s3://<bucket>/<l1>/<l2>/<table>`: a human-browsable
  Hive-style layout. CreateTable may pass an explicit `location` (inside this
  bucket, not under the reserved prefix, not a prefix of / prefixed by another
  table's location). Rename moves no data and does not change `location`
  (Iceberg semantics).
- Difference from RustFS: RustFS defaults to `s3://<bucket>/tables/<table_id>`
  (opaque). The readable path is chosen because lights3's policy `prefixes` are
  key prefixes: `prefixes: ["sales/"]` scopes both the catalog operations under
  namespace `sales` and its data objects (§6.2).

### 4.5 Identifier rules

Same as RustFS (aligned with the AWS S3 Tables character set to avoid URL / key
encoding ambiguity): segment `^[a-z0-9]([a-z0-9_-]{0,62}[a-z0-9])?$` (1–64 bytes),
multi-level namespaces separated by `%1F` (URL) / `levels[]` (JSON), total ≤512;
table names are single segments; the reserved segments `_ns.json` / `tbl` cannot
be matched (they contain `.`). Violations → 400 `BadRequestException`.

### 4.6 Page tokens

`pageToken` = base64url (no padding) of
`{"v":1,"ctx":"<first 16 hex bytes of sha256(op\0bucket\0ns)>","after":"<start_after key>"}`,
≤4 KiB; `ctx` mismatch → 400; `pageSize` default and maximum 1000
(`tables.max_page_size`); when both are absent the full list is returned (same as
RustFS).

## 5. Commit protocol (single-table CAS)

### 5.1 Primitive

`IStorageBackend::put_object(..., PutCondition)`: `if_none_match` (write only if
absent, else `PreconditionFailed`) and `if_match_etag` (write only if the ETag
matches; mismatch `PreconditionFailed`, missing `NoSuchKey`); the contract
requires "check and commit inside the backend's own atomic point, no trace on
failure" (`storage/backend.h`); all six backends implement it and `backend_suite`
covers it. The catalog uses only this primitive and **introduces no lock
service**.

In-process, a per-(bucket, table) `AsyncMutex` is added as a fast path:
concurrent commits on the same gateway are serialized to avoid needless CAS
failures and piling up metadata objects; cross-gateway correctness still rests on
CAS alone.

### 5.2 CommitTable flow

```text
 0 authorize: Action::Write on (bucket, "<ns-path>/<table>") (§6.2)
 1 read pointer (entry, etag0); state != Active → 404 (Deleted) / 503 (Renaming, §5.6)
 2 idempotency (§5.3): get_commit(table_id, commit_id) hit → replay by state or 409
 3 read current metadata.json (≤ tables.metadata_max_size, default 50 MiB)
 4 iceberg::check_requirements(current, requirements)   failure → 409 CommitFailedException
 5 next = iceberg::apply_updates(current, updates)       invalid → 400 / 406
   iceberg::check_transition(current, next)              failure → 409
   iceberg::check_snapshots(next - current, bucket)      missing file / out of bucket → 409 (§7.4)
 6 new_loc = .lights3-table/<ns>/<t>/metadata/{gen+1:05}-<commit_id or fresh uuid>.metadata.json
   put_object(bucket, new_loc, json(next), if_none_match)   ← an idempotent replay collides here; read back and compare
 7 put_commit(CommitRecord{STAGED, expected_token=entry.version_token, new_token, …}, if_none_match)
   already exists → back to 2 (a concurrent replay of the same commit_id)
 8 next_entry = entry{metadata_location=new_loc, version_token=new_token, generation+1, updated}
   put_table(..., next_entry, if_match_etag=etag0)         ← the only atomic point
   PreconditionFailed → 409 CommitFailedException (best-effort delete of new_loc; leftovers go to maintenance)
 9 put_commit(record{COMMITTED}, {})                       result ignored (see §5.4)
10 respond {metadata-location: s3://…/new_loc, metadata: next}
```

Steps 3–5 are pure computation (JSON and Avro parsing on the thread pool), 6–9 are
four object writes of which only 8 decides the outcome. `version_token` comes
from 16 `getentropy` bytes (the same source as upload ids,
`storage/multipart.cc`) and is not enumerable.

### 5.3 Idempotency and retries

Clients (PyIceberg's `commit-id`, DuckDB retries) may replay the same commit.
Rules:

| Existing CommitRecord | Pointer state | Handling |
| --- | --- | --- |
| COMMITTED, same `request_digest` | any | idempotent success: return the recorded `new_metadata_location` and its metadata |
| STAGED, pointer's `version_token == record.new_token` | advanced | finalize (write COMMITTED), idempotent success |
| STAGED, pointer's `version_token == record.expected_token` | not advanced | the previous attempt died between 7 and 8: continue from 8 (if new_loc exists, read back and compare) |
| STAGED, pointer matches neither | overtaken by another commit | 409: this STAGED record is dead, diagnostics can clean it |
| any, different `request_digest` | — | 409 `CommitFailedException "commit-id reused with a different payload"` |

`request_digest` = sha256 of the canonical JSON of requirements + updates; the
original is not stored. `idempotency-key` (a body field, as in RustFS) is treated
like `commit-id`: with a `commit-id` the record is named after it, otherwise
after `sha256(idempotency-key)`; with neither a uuid is generated (not replayable,
consistent with the spec).

### 5.4 Crash-window matrix

| Crash point | Leftover | Consequence and self-healing |
| --- | --- | --- |
| after 6 | one unreferenced metadata file | the maintenance retention rule treats it as an orphan and deletes it after the safety window (§9) |
| after 7 | STAGED record + unreferenced metadata | a replay takes row 3 of §5.3; without a replay diagnostics list it as `StagedBeforeTableUpdate`, cleanable |
| after 8, before 9 | pointer advanced, record still STAGED | **externally committed**. A replay takes row 2 of §5.3; `GET …/catalog/diagnostics` reports `FinalizationRequired`, `POST …/catalog/recovery` only rewrites the record and never touches the pointer |
| delete of new_loc fails after 8's PreconditionFailed | redundant metadata file | same as row 1 |

The key agreement with RustFS: **nothing that fails after the pointer CAS may
turn success into failure**.

### 5.5 Multi-gateway

Catalog state is in the default backend's `.sys`, so its atomicity equals that
backend's cross-process `PutCondition` atomicity. Per the matrix in
[deployment.md §5.1](deployment.md): duostore (redis / tikv meta + rados data)
and cloudproxy as the default backend hold across gateways; localfs / xlocalfs /
rocksdb / sqlite default backends are single-gateway only. `--check-config` WARNs
when `tables.enabled` and multi-gateway flags (`read_lease` etc.) appear on a
non-shared default backend, the same defence line as multi-gateway-multipart
§4 ④. The table bucket itself can live on any backend (the data plane carries no
cross-gateway state).

### 5.6 Two-phase rename

Pointers are addressed by name, so a rename is "moving an object in a CAS
world". The RustFS intent scheme is adopted minus the reverse-index step:

```text
① put_rename(intent{Prepared, src, dst, src_etag}, if_none_match)
② src.state = Renaming, rename_id           put_table(src, if_match src_etag)
③ dst = copy of src {state=Active}          put_table(dst, if_none_match)    exists → roll back ②, 409 AlreadyExists
④ src.state = Deleted tombstone             put_table(src, if_match)
⑤ delete_rename(intent)
```

Readers that meet `Renaming` get 503 (`Retry-After: 1`); writers first drive any
unfinished intent (idempotent, resuming from the phase recorded in the intent).
The tombstone object is kept until maintenance cleans it (default 24h); meanwhile
a create with the same name overwrites it with `if_match(tombstone_etag)`.
Renaming across namespaces requires the destination namespace to exist
(evidence-based).

### 5.7 Drop

`DELETE …/tables/{t}`: writes a `Deleted` tombstone (`if_match`), deletes neither
metadata nor data (`purgeRequested=false`, the spec default).
`purgeRequested=true`: 406 `UnsupportedOperationException` at first; after §14 ④
lands it becomes "tombstone first, then enqueue a purge job that deletes the
reserved directory and the `location` prefix". `DELETE …/namespaces/{ns}`:
non-empty by evidence → 409 `NamespaceNotEmptyException`.

## 6. REST interface

### 6.1 Routing and addressing

- Recognized only under **path-style**: `req.path` starts with
  `tables.path_prefix + "/v1/"` (default `/iceberg/v1/`) and no vhost match →
  `tables::RestApi::dispatch`. The branch sits in `S3Service::dispatch` after the
  `/-/` internal-plane branch and before STS `POST /` (the path ladder in
  `service.cc`).
- **Bucket-name reservation**: with `tables.enabled`, CreateBucket rejects a bucket
  named after the first prefix segment (`iceberg`, plus `_iceberg` if the alias
  is on) with `InvalidBucketName`; an already existing bucket of that name at
  startup → WARN explaining that its `v1/` prefix is shadowed. RustFS does not
  handle this.
- `{prefix}` = the bucket name (clients set both `warehouse` and `prefix` to the
  bucket); `GET /v1/config?warehouse=<bucket>` returns `overrides.prefix = <bucket>`.
- Listener: the data listener (engines use one endpoint for catalog and data);
  with `http.admin_port` split, the admin-plane entry for enabling table buckets is
  on the admin listener, the REST surface is unaffected.
- JSON request-body cap `tables.request_max_size` (default 1 MiB, CommitTable
  updates can carry whole schemas), the same shape as `handlers::read_json_object`
  with its own limit.

### 6.2 Authentication and authorization

- SigV4: `SigV4Authenticator::verify_impl(req, service, …)` already checks the
  service by parameter; catalog paths accept `s3` (default) and `s3tables`
  (`tables.accept_s3tables_signing`, default true); the three payload forms are
  unchanged (PyIceberg goes through botocore with `x-amz-content-sha256`, DuckDB
  uses `UNSIGNED-PAYLOAD`). Session credentials (`L3SA`) verify their token as usual.
- Authorization reuses `CredentialPolicy::allows(bucket, key, action)` by mapping
  catalog operations onto S3 triples, **no new action family**:

| Catalog operation | (bucket, key) | Action |
| --- | --- | --- |
| GET /config, GET buckets/{w} | (w, "") | Read |
| list/get/exists namespace | (w, "<ns-path>/") | Read |
| create/update/drop namespace | (w, "<ns-path>/") | Write / Write / Delete |
| list tables | (w, "<ns-path>/") | Read |
| load / exists table, refs GET, metadata-location GET | (w, "<ns-path>/<t>") | Read |
| create / register / commit, metadata-location PUT | (w, "<ns-path>/<t>") | Write |
| drop table | (w, "<ns-path>/<t>") | Delete |
| rename | source Delete + destination Write | |
| PUT/DELETE buckets/{w} (enable/disable table bucket) | root only | |

Hence a credential with `prefixes: ["sales/"]` can only touch namespace `sales`
(and children) and their data objects; a `readonly: true` credential can only
load / list / read data. Tenant credentials see only their tenant's table buckets
(`require_tenant_bucket`). Every catalog request goes to the audit log with
`api_name` `Iceberg.<Op>`.

### 6.3 Endpoint table

Standard endpoints (advertised in `GET /v1/config` `endpoints`):

| Method path (after `/iceberg/v1`) | Semantics | Step |
| --- | --- | --- |
| GET `/config` | CatalogConfig | ① |
| GET / POST `/{w}/namespaces` | list (`parent`, pagination) / create namespace | ① |
| GET / HEAD / DELETE `/{w}/namespaces/{ns}` | load / exists (204) / drop | ① |
| POST `/{w}/namespaces/{ns}/properties` | `{removals, updates}` → `{updated, removed, missing}` | ① |
| GET / POST `/{w}/namespaces/{ns}/tables` | list (pagination) / CreateTable | ① |
| POST `/{w}/namespaces/{ns}/register` | RegisterTable | ① |
| GET / HEAD / POST / DELETE `/{w}/namespaces/{ns}/tables/{t}` | LoadTable / exists / CommitTable / DropTable | ① |
| POST `/{w}/tables/rename` | RenameTable | ① |
| GET `/{w}/namespaces/{ns}/tables/{t}/credentials` | LoadCredentials | ② |
| GET/POST/HEAD/DELETE `…/views…`, POST `/{w}/views/rename` | Iceberg views (format v1) | ⑥ |

Extensions (not in `endpoints`, aligned with RustFS / AWS semantics):

| Method path | Semantics | Step |
| --- | --- | --- |
| PUT / GET / DELETE `/{w}/buckets/{bucket}` | enable / inspect / disable table bucket (root) | ① |
| GET / PUT `…/tables/{t}/metadata-location` | AWS `GetTableMetadataLocation` / `UpdateTableMetadataLocation` shape: `{metadataLocation, versionToken}`; PUT swaps only the pointer but still runs checks 3–5 of §5.2 (the metadata must already be in the reserved directory, i.e. only meaningful for the server copy made by register or maintenance output) | ① |
| GET `…/tables/{t}/catalog/diagnostics`, POST `…/catalog/recovery` | diagnostics and repair of §5.4 | ③ |
| GET/PUT `…/tables/{t}/maintenance/config`, POST `…/maintenance/{plan,run}`, GET `…/maintenance/jobs/{id}` | §9 | ④ |

Unimplemented items answer 406 `UnsupportedOperationException` (not a 501 XML):
`stage-create`, `purgeRequested=true` (before ④), register with `overwrite: true`,
format-version 3, `/transactions/commit`, `/plan`, `/tasks`, `/sign`, `/metrics`
(`reportMetrics` is accepted and discarded with 204 because PyIceberg calls it),
`/oauth/tokens`.

### 6.4 `GET /v1/config`

```json
{"defaults":{"warehouse":"<bucket or empty>","lights3.catalog-prefix":"/iceberg/v1"},
 "overrides":{"namespace-separator":"%1F","prefix":"<bucket>"},
 "endpoints":["GET /v1/{prefix}/namespaces", "...implemented endpoints only..."]}
```

`overrides.prefix` lets a client that only sets `warehouse=<bucket>` obtain
`{prefix}` automatically (standard PyIceberg / Spark behaviour).
`idempotency-key-lifetime` is not advertised.

### 6.5 CreateTable / RegisterTable / LoadTable

- **CreateTable** `{name, location?, schema, partition-spec?, write-order?, stage-create?, properties?}`:
  the server reassigns ids (schema-id 0, field ids from 1, spec-id 0, partition
  field ids from 1000, sort-order 0/1), extracts `properties["format-version"]`
  (default 2, 1 allowed, 3 → 406), produces the initial metadata (`snapshots: []`,
  `refs: {}`, `last-sequence-number: 0` for v2; v1 additionally mirrors `schema` /
  `partition-spec`), writes `00001-<uuid>.metadata.json` (`if_none_match`), then
  `put_table(if_none_match, or if_match on a tombstone)`. Exists → 409
  `AlreadyExistsException`. Responds 200 `LoadTableResult`.
- **RegisterTable** `{name, metadata-location, overwrite?}`: `metadata-location`
  must be an `s3://<w>/…` key inside this bucket (it may be outside the reserved
  prefix, i.e. an old table written by an engine); the server reads it (≤ cap,
  `.json`/`.json.gz`), runs `validate_supported_metadata` and the full
  `check_snapshots`, then **copies** it to `00001-<uuid>.metadata.json` under the
  reserved directory and points there (the original is untouched); `table_uuid`
  is adopted, `location` is taken from the metadata. This is where the invariant
  "the pointer only ever points into the reserved directory" comes from; AWS's
  register also requires the metadata to be inside the table bucket.
- **LoadTable**: `?snapshots=all|refs` (`refs` prunes to snapshots referenced by
  refs and current); response `{metadata-location, metadata, config}`; `config`
  carries data-plane hints for engines:
  `{"s3.path-style-access":"true","s3.region":"<auth.region>",
  "lights3.credential-vending":"disabled|supported","lights3.table-location":"<location>"}`
  (`s3.endpoint` is not given: the gateway does not know its external address and
  clients must configure it anyway). `ETag` (= pointer etag) and `If-None-Match`
  → 304 are supported, an optional spec feature PyIceberg 0.9+ uses to save a
  metadata read.

### 6.6 Error model

`tables::RestError{status, type, message}` → `{"error":{"message","type","code"}}`,
`Content-Type: application/json`. Mapping:

| Source | status / type |
| --- | --- |
| identifier / JSON shape / unknown requirement or update | 400 `BadRequestException` |
| SigV4 failure | 401 `NotAuthorizedException` (`InvalidAccessKeyId`) / 403 `ForbiddenException` (others) |
| policy / tenant denial | 403 `ForbiddenException` |
| namespace / table / view missing | 404 `NoSuchNamespaceException` / `NoSuchTableException` / `NoSuchViewException` |
| bucket not table-enabled | 404 `NoSuchNamespaceException`, message "bucket is not table-enabled" (RustFS uses 400; 404 keeps PyIceberg's `namespace_exists` semantics right) |
| create name clash | 409 `AlreadyExistsException` |
| drop of a non-empty namespace | 409 `NamespaceNotEmptyException` |
| requirement failure / CAS failure / transition invariant failure / idempotent payload mismatch | 409 `CommitFailedException` |
| `Renaming` in progress, backend `SlowDown` | 503 `ServiceUnavailableException` + `Retry-After` |
| unsupported | 406 `UnsupportedOperationException` |
| other storage errors | 500 `RESTException`; errors **after the CAS** are always 200 (§5.4) |
| step 8 outcome unknown (timeout / connection loss) | 500 `CommitStateUnknownException`: the client must not blindly retry the write but load and decide |

Errors are scrubbed like `public_error` (no internal text on `InternalError`).

## 7. Iceberg metadata model and validation (`src/tables/iceberg/`)

A purely functional module whose inputs and outputs are `nlohmann::json`
(field-order independent; serialization uses stable `dump()` output so replays
can be compared).

### 7.1 Requirements

All 8: `assert-create` (always 409 when the table exists), `assert-table-uuid`,
`assert-ref-snapshot-id` (`snapshot-id: null` means the ref must not exist),
`assert-last-assigned-field-id` (against `last-column-id`),
`assert-current-schema-id`, `assert-last-assigned-partition-id`,
`assert-default-spec-id`, `assert-default-sort-order-id`. Unknown type → 400.

### 7.2 Updates

| action | Handling |
| --- | --- |
| `assign-uuid` | differs from the current value → 409 |
| `upgrade-format-version` | 1→2 allowed (snapshots get `sequence-number: 0`); downgrade 400; 3 → 406 |
| `add-schema` | the server **reassigns** `schema-id` to the next id; new field ids must exceed the old `last-column-id`; type promotion only int→long, float→double, decimal precision widening |
| `set-current-schema` | `-1` = the most recently added |
| `add-spec` / `set-default-spec` | server assigns spec-id and partition field ids; source fields must exist in the current schema |
| `add-sort-order` / `set-default-sort-order` | empty fields → order-id 0 |
| `add-snapshot` | v2 rejects `manifests` (requires `manifest-list`); `sequence-number` > `last-sequence-number`; unique id; parent exists; `summary.operation ∈ {append, overwrite, delete, replace}` |
| `set-snapshot-ref` | `main` also updates `current-snapshot-id` and `snapshot-log` |
| `remove-snapshots` / `remove-snapshot-ref` | dangling refs and statistics are pruned; removing main's snapshot sets `current-snapshot-id = -1` |
| `set-location` | must be inside this bucket and outside the reserved prefix |
| `set-properties` / `remove-properties` | string values only; `format-version` cannot be changed through properties |
| `set-statistics` / `remove-statistics` / `set-partition-statistics` / `remove-partition-statistics` | recorded; file existence checked in §7.4 |
| `remove-partition-specs` / `remove-schemas` | the current / default ones cannot be removed |
| `add-encryption-key` / `remove-encryption-key` | 406 (v3) |

After application: prune `snapshot-log` entries whose snapshots no longer exist,
append to `metadata-log` (`{timestamp-ms, metadata-file: old location}`, keeping
the newest `tables.metadata_log_keep`, default 100, matching Iceberg's
`write.metadata.previous-versions-max`), update `last-updated-ms`.

### 7.3 Transition invariants (current → next)

`table-uuid` unchanged; `format-version` never lowered; `last-column-id` /
`last-partition-id` / `last-sequence-number` non-decreasing; existing schemas /
partition specs / sort orders / snapshots byte-identical (add or remove only,
never modify); ids referenced by `current-schema-id` etc. must exist. Failure →
409 (the last line of defence when concurrent writers bypass requirements).

### 7.4 Snapshot-graph validation and Avro

For every snapshot added in `next`: read the `manifest-list` (Avro OCF) → every
manifest (Avro OCF) → every data/delete file with `status ∈ {ADDED(1), EXISTING(0)}`:
`head_object` exists, `file_size_in_bytes` matches (when present), the path is
inside this bucket and outside the reserved prefix. A manifest's `manifest_length`
must match the object size. Statistics files get a 4-byte magic check
(`PFA1` / `PAR1`). Concurrency `tables.validate_concurrency` (default 16,
`when_all` in batches); caps: manifests ≤ 10 000 / file references ≤ 1 000 000 /
one Avro ≤ 128 MiB.

Commit-conflict re-check (borrowed from RustFS): with the parent snapshot's live
file set as baseline, a new snapshot that re-ADDs a live file or DELETEs a non-live
one → 409; an `append` may carry neither delete files nor data-file deletions.

**Avro reader**: an in-tree `avro_reader.{h,cc}` (read-only): OCF header (magic,
`avro.schema`, `avro.codec`, sync marker), schema-JSON-driven decoding (nlohmann;
null/boolean/int/long/float/double/bytes/string/record/array/map/union/fixed/enum,
all that Iceberg manifests use), codecs `null` and `deflate` (zlib; when
`find_package(ZLIB)` is missing deflate → validation skipped, WARN logged and the
response `config` reports `lights3.snapshot-validation: "skipped-codec"`);
`snappy` / `zstd` degrade the same way. avro-cpp is not pulled in (depends on
Boost and fmt, and only a tenth of it — reading — would be used).

Step ① does the shallow "manifest-list object exists + size" check; the deep
Avro validation comes with ③.

## 8. Data-plane integration

### 8.1 Table-bucket guard (`TableBucketGuard`)

Hooked after the authorization point in `S3Service::dispatch` (bucket / key /
`r->action` known), active when `tables.enabled` and the bucket is in the
`TableBucketStore` snapshot:

| Request | Rule |
| --- | --- |
| PUT / DELETE / POST (each key of DeleteObjects) / CopyObject destination / every multipart write step with a key under the reserved prefix | 400 `InvalidRequest "Object key is reserved for the table catalog"` |
| GET / HEAD / List on the reserved prefix | pass (engines read metadata.json and list the metadata directory) |
| PutObjectTagging and other in-place meta updates on the reserved prefix | as the first row |
| DeleteBucket | non-empty catalog (any namespace) or objects under the reserved prefix → 409 `BucketNotEmpty`; deleting an empty table bucket also deletes `.sys/tables/<bucket>` and `.sys/tables-catalog/<bucket>/` (catalog state first, then the bucket; a crash leaves only orphaned state, reported by `fsck`) |
| table bucket disabled | the guard lifts; objects under the reserved prefix become ordinary objects |

Mixed DeleteObjects batches: reserved keys get a per-key `Error` rather than
failing the whole batch (AWS per-key semantics). The guard is an in-memory lookup
and adds no IO.

### 8.2 Lifecycle exclusion

`LifecycleRunner::run_once` skips table buckets while iterating (both
`Expiration` and `AbortIncompleteMultipartUpload` — the latter deliberately: an
engine's large-file multipart on a slow link may exceed days); at startup and on
`PutBucketLifecycle` a table bucket gets a WARN "lifecycle rules are ignored on
table buckets". Table files are deleted only by §9 maintenance.

### 8.3 Usage, quota, audit

Table data objects go through the existing PutObject path, so usage and quota
apply unchanged; metadata.json written by the catalog also goes through
`put_object` and counts as ordinary objects of the table bucket (AWS bills
metadata to the table bucket too); on `QuotaExceeded` CommitTable answers 409
`CommitFailedException` (message carries `QuotaExceeded`), which clients retry —
hence a `check_quota` pre-check before step 6 to fail early. Audit events
`tables.<op>`, same shape as the existing `AuditEvent`.

### 8.4 Credential vending (step ②)

When `X-Iceberg-Access-Delegation` contains `vended-credentials` and
`tables.credential_vending: true`: `CredentialStore::mint_session(parent, ttl, narrowed_policy)`
gains a third parameter — the session policy = parent policy ∩
`{buckets:[w], prefixes:["<location relative prefix>/", ".lights3-table/<ns>/<t>/metadata/"], readonly: parent readonly or request readonly}`;
the persisted format already has a `policy` field, no change needed. The response
carries `storage-credentials: [{prefix: "<location>", config: {s3.access-key-id, s3.secret-access-key, s3.session-token, expiration-ms}}]`
and the same keys in `config` (PyIceberg honours both), with
`Cache-Control: no-store, private`. TTL `tables.credential_ttl` (default 900 s,
clamped 60–3600). Sessions can neither vend again nor AssumeRole (existing
rules). `GET …/credentials` is the same implementation.

## 9. Maintenance (step ④)

Reuses `AdminJobs`: new `JobOp::{TablePlan, TableRun, TablePurge}`, paths
`POST /-/admin/tables/<bucket>/<ns-path>/<table>/<op>` (root) and the REST
extension `POST …/maintenance/{plan,run}` (Write permission), CLI
`lights3-ctl tables plan|run <bucket> <ns.table>`; one job per table at a time
(`JobInProgress`); jobs run on a dedicated thread (the existing AdminJobs model),
status via `GET` on the same path.

**plan** (read-only, JSON report):

1. Metadata retained set = current pointer ∪ `metadata-log` ∪ the newest
   `retain_recent_metadata_files` under the reserved directory ∪ reachable from
   protected snapshot refs; everything else is a candidate, and candidates must
   also satisfy `last_modified ≤ now − safety_window` (default 15 min).
2. Snapshot expiration: `max_snapshot_age_ms` / `min_snapshots_to_keep` (table
   properties `history.expire.*` win; conflicts → manual review); snapshots
   referenced by refs or refs carrying their own retention fields → manual
   review. Produces a `remove-snapshots` update set with matching
   `assert-ref-snapshot-id` requirements (the plan is bound to the pointer).
3. Orphans: list `data/`, `delete/` under `location` and the reserved directory,
   minus files reachable from **all metadata in the retained set** (the Avro walk
   of §7.4); any manifest parse failure → no data file is deleted this round
   (fail-closed).

**run**: re-read the pointer; a `version_token` different from planning time →
fails with `StalePlan`; snapshot expiration is executed as a standard commit of
§5.2 (so it enters the commit log too); then metadata candidates and orphans are
deleted per plan, each with a `head` re-check of mtime before deletion; throttling
via the AdminJobs `max_bytes_per_sec` parameter. With
`tables.maintenance.delete_enabled: false` (default) run only commits snapshot
expiration and deletes no files.

**periodic**: with `tables.maintenance.scan_interval` (default 0 = off) a
`LifecycleRunner`-style runner plans+runs every table bucket in sequence; across
gateways every instance runs it like lifecycle (deletion is idempotent, the safety
window shields in-flight commits), or by convention only one gateway enables it
(`gc_enabled`). **purge** (after drop): deletes the reserved directory + the
`location` prefix + the tombstone, on the same job framework.

Compaction execution is not done (needs Parquet read/write); plan may emit
binpack candidate groups for external engines (Spark `rewrite_data_files`),
optional in ⑥.

## 10. Configuration

```yaml
tables:
  enabled: false                   # master switch; off → /iceberg/v1 falls through to S3 routing (bucket name iceberg not reserved)
  path_prefix: /iceberg            # REST prefix (+ /v1)
  compat_prefix: ""                # optional alias such as /_iceberg (⑥)
  accept_s3tables_signing: true    # catalog paths accept credential-scope service = s3tables
  reserved_prefix: .lights3-table/ # reserved prefix inside table buckets (immutable once enabled)
  metadata_max_size: 50MiB
  request_max_size: 1MiB
  metadata_log_keep: 100
  max_page_size: 1000
  validate_concurrency: 16
  credential_vending: false
  credential_ttl: 900s
  maintenance:
    scan_interval: 0s              # 0 = manual only
    safety_window: 15m
    retain_recent_metadata_files: 10
    delete_enabled: false
    tombstone_ttl: 24h
```

`Config` gains `TablesConfig`, a `root.find("tables")` block in `config.cc` with
`check_range`; everything is **restart-only** (listed in `docs/config-reload.md §4`
and in the `requires_restart` comparison in `app.cc`). `--check-config`
validates: the prefix starts with `/` and does not contain `/v1`,
`reserved_prefix` ends with `/` and does not start with `.sys`, multi-gateway
misconfiguration WARN (§5.5).

## 11. Build integration and dependencies

- Sources: `src/tables/{catalog_store.h, object_catalog_store.cc, catalog.cc,
  rest_api.cc, rest_error.h, bucket_guard.cc, maintenance.cc,
  iceberg/{metadata.cc, requirements.cc, updates.cc, snapshots.cc, avro_reader.cc}}`,
  compiled into `lights3_core`; CMake option `LIGHTS3_TABLES` (default ON; OFF
  compiles nothing and `tables.enabled: true` becomes a config error), gated like
  `LIGHTS3_CLOUDPROXY`.
- Dependencies: nlohmann/json (present), OpenSSL sha256 (present in
  `core/util/crypto.h`), `getentropy` (present), zlib (optional,
  `find_package(ZLIB)`; when missing deflate Avro degrades). **No new submodule**.
- CLI: `lights3-ctl tables enable|disable|status <bucket>`, `tables list <bucket>`,
  `tables plan|run|purge`; `lights3 fsck` gains a "catalog state vs. table
  buckets" reconciliation item (orphaned `.sys/tables-catalog/<bucket>/`, pointers
  to missing metadata).

## 12. Pluggable evolution: a duostore-meta backing

A second `ITableCatalogStore` implementation stores the entities in duostore's
`IMetaStore` (a new column family / key prefix `tc/`), maps CAS onto meta
transactions (redis Lua / tikv 2PC / rocksdb WriteBatch) and listing onto ordered
iteration; the single-table commit performs steps 7–9 in one transaction and the
crash-window matrix collapses to one row. For deployments whose default backend is
duostore, configured as `tables.catalog_backing: object | duostore`. RustFS's
"whole catalog in one snapshot object" mode is not adopted (a 64 MiB cap and a
single lock are not lights3's route).

## 13. Observability and tests

Metrics (`MetricsScope{feature=tables}`): `lights3_tables_requests_total{op,status}`,
`lights3_tables_commit_seconds{result=ok|conflict|error}`,
`lights3_tables_commit_conflicts_total`, `lights3_tables_validation_files_total`,
`lights3_tables_maintenance_deleted_bytes_total`, `lights3_tables_finalization_gaps`
(gauge, updated by diagnostic scans). Access log `api_name = Iceberg.<Op>`, slow
request threshold unchanged.

Tests (within the [testing.md](testing.md) system):

| Layer | Content |
| --- | --- |
| `tests/unit/test_tables_iceberg.cc` | the pure functions of §7: 8 requirements × pass/fail; valid/invalid samples of every update; transition invariants; the Avro reader on manifests produced by PyIceberg (binary fixtures in the repo, ≤100 KiB) |
| `tests/unit/test_tables_catalog.cc` | `ObjectCatalogStore` on `MemoryBackend`: evidence-based namespaces, pagination, rename crashed at each of the five steps (put/delete failures injected by attempt index through the `core/fault.h` facade), the four crash windows of §5.4 + the replay matrix of §5.3, 100 concurrent commits with a single winner |
| `tests/unit/test_tables_rest.cc` | in-process `S3Service`: endpoint shapes, error model, prefix-scoped policy, tenant isolation, `s3tables` signing, table-bucket guard (reserved-prefix PUT 400 / GET 200, DeleteBucket 409, lifecycle skip), consistency between `/config` `endpoints` and the route table (table-driven, prevents drift) |
| `tests/unit/multi_gateway_suite.h` additions | two `S3Service`s sharing a `MemoryBackend` (or redis/tikv duostore): cross-gateway commit conflicts converge, rename recovery driven by the other gateway |
| new segment in `tests/e2e/run_e2e.sh` | bash + curl: enable table bucket → create namespace/table → hand-written CommitTable (add-snapshot pointing at a bundled fixture) → conflict 409 → drop; runs on the six-driver matrix |
| `scripts/tables/pyiceberg_smoke.py`, `duckdb_smoke.py` (opt-in) | the RustFS script sequence: create table → append 2 rows → reload and scan → conflict/idempotency probes → maintenance plan/run → drop; needs `pyiceberg[pyarrow]` / `duckdb`, triggered by `LIGHTS3_TABLES_SMOKE=1`, ctest label `tables-smoke`, SKIP by default (the same pattern as tikv's `LIGHTS3_TEST_PD_ADDR`); on success record the client versions in [testing.md §6](testing.md) |
| Spark / Trino | configuration templates only (appendix of §13); manual verification recorded in testing.md; no automation claimed |

Client configuration templates (for user docs, the same keys as the RustFS scripts):

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

## 14. Implementation steps

In dependency order; each step is independently mergeable with unit tests; when
done, update the row to "implemented + date". The implementation-level document of
each step (file list, signatures, flows, integration points, unit-test list) lives
under `docs/s3-tables/` (Chinese only, like the other implementation-level docs):
`step-1-catalog-core.md` … `step-6-optional.md`.

| Step | Content | Acceptance |
| --- | --- | --- |
| ① catalog core + minimal REST | `TablesConfig`; `TableBucketStore`; `ITableCatalogStore` + `ObjectCatalogStore`; the metadata model of §7.1–7.3 (shallow snapshot check); commit protocol §5.2/5.3; endpoints: config / buckets / all namespace ops / tables list-create-load-commit-drop-exists-rename-register / metadata-location; error model; dispatch branch and bucket-name reservation; the guard's reserved-prefix read-only rule and DeleteBucket guard; audit and metrics | `test_tables_iceberg` / `test_tables_catalog` / `test_tables_rest` pass; new e2e segment passes; PyIceberg smoke (manual, local) create + append + scan passes |
| ② permissions and credentials | the policy triple mapping of §6.2, tenant isolation, the `s3tables` signing name, the `mint_session` narrowing parameter and `vended-credentials` negotiation, `GET …/credentials`, lifecycle exclusion | prefix-scoped and read-only credential cases; vended credentials Put/Get/Delete inside the prefix pass, outside 403 |
| ③ deep validation and diagnostics | Avro reader; snapshot graph and conflict re-check of §7.4; `catalog/diagnostics` / `recovery`; `fsck` reconciliation item; ETag/If-None-Match on LoadTable | manifest fixture cases; crash-window matrix cases; rename recovery cases |
| ④ maintenance | `JobOp::Table*`, plan/run/purge, `purgeRequested=true`, periodic runner, CLI, `tombstone_ttl` cleanup | retained set / safety window / StalePlan cases; DuckDB smoke (manual, local) |
| ⑤ multi-gateway and docs | `multi_gateway_suite` additions; `--check-config` misconfiguration WARN; a "table catalog" column in deployment.md §5; turn this document into an implementation document + `docs/en/` sync; README index | two-gateway cases; doc review |
| ⑥ optional | views; the `/_iceberg/v1` alias; `reportMetrics` into audit; compaction candidate planning output; duostore-meta backing (§12) | as needed |

## 15. Deliberately not done

| Item | Reason |
| --- | --- |
| the AWS S3 Tables control-plane API (`s3tables.<region>` endpoint, ARN addressing, `CreateTableBucket` and ~50 operations) | the engine ecosystem goes through Iceberg REST; AWS itself has engines reach table buckets through the REST endpoint. RustFS makes no such claim either. Only the semantics and field names of `GetTableMetadataLocation` / `UpdateTableMetadataLocation` are borrowed |
| multi-table transactions `/transactions/commit` | needs cross-object atomicity, which object CAS cannot give; revisit once the duostore-meta backing (§12) exists |
| server-side scan planning `/plan` `/tasks`, remote signing `/sign` | engine-local planning suffices; remote signing would have the gateway sign S3 requests on the client's behalf, conflicting with the policy model |
| execution of compaction / delete-file rewrites | needs Parquet read/write and row-level semantics; left to engines (Spark `rewrite_data_files`) |
| durable-strong single-snapshot backing | see §12 |
| Delta Lake / Hudi, built-in SQL | outside the scope of an object-storage gateway |
| active-active multi-region writes | single writer per table is an Iceberg premise; multiple gateways are only valid when they share one catalog state (§5.5) |
