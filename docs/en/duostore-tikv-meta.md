# TikvMetaStore: TiKV-Based DuoStore Metadata Store

> English translation of [../duostore-tikv-meta.md](../duostore-tikv-meta.md). The Chinese original is authoritative; section numbering matches.

> Status: T0-T5 all complete (2026-07-30; full `TikvMetaStore` interface + 2PC
> sidecar committer with ops + guard shards + test suite/e2e wiring + T5 polish —
> GC safepoint advancement worker, Poco log bridge, backoff-budget
> parameterization, conflict-retry metrics, ten-thousand-part complete special;
> code in `src/storage/duostore/tikv_client.{h,cc}` and
> `tikv_meta_store.{h,cc}`, build option `LIGHTS3_DUOSTORE_TIKV_META` default
> OFF). Submodule `third_party/client-c` pinned to `78a557e`
> (2026-07-21; upstream has no release tag, so pinning by commit is the only
> option). Implementation deviation: the upstream extension did not go through a
> fork; it became an in-tree sidecar instead (§6.3). Delivers the evolution
> promise of [duostore-backend.md](duostore-backend.md)
> §12 "TiKV (multi-gateway shared meta)": implement `IMetaStore`
> (`src/storage/duostore/meta_store.h`); the meta side gains **horizontal scaling
> + multi-replica high availability**, completing the final tier above the Redis
> version (single node / primary-replica). Client library:
> [tikv/client-c](https://github.com/tikv/client-c). In this document "main doc"
> refers to duostore-backend.md, and a bare `§N` refers to sections of this
> document.
>
> **Read this first**: client-c's transport infrastructure (PD/TSO, region cache,
> retry backoff, lock resolution) is production-proven by TiFlash, but its
> **transaction commit layer is test-grade** — the `Txn` header annotates itself
> "only used for TEST", and mutations only support Put (no
> Del/Insert/Lock). Adopting it = adopting a prerequisite engineering task of
> **extending the 2PC layer first** (§6.3); this is the main workload and risk of
> the whole plan, and the implementation phasing puts it at T1.

## 1. Goals and Non-Goals

| Goal | Description |
| --- | --- |
| Implement the full `IMetaStore` interface | The whole set of bucket / object / list / multipart / GC accounting; the same test suite as the RocksDB / SQLite / Redis versions passes all green (§10) |
| Multi-gateway shared meta, with meta itself distributed | Transaction atomicity guaranteed by TiKV optimistic transactions (Percolator 2PC) (§4); the increment over the Redis version: meta storage itself is multi-replica (raft majority), horizontally scalable, no single point — this is the motivation for adopting TiKV |
| Zero new codecs | Value encoding 100% reuses `codec.cc` (§3.1), byte-identical format with the RocksDB/Redis/SQLite implementations |
| Return to the ordered KV model | TiKV is a globally ordered transactional KV — the key layout and the list_objects algorithm can be checked line-by-line against the RocksDB version (main doc §4.1/§4.4); of the four meta implementations, this is the one most isomorphic to RocksDB (§3.2) |

Non-goals (explicitly declared):

- **No comprehensive hardening of client-c** — extend only the minimal surface
  this plan requires (2PC mutation op, Get's not_found, §6.3); the rest
  (async commit, pessimistic transactions, coprocessor) is untouched;
- **TiKV RawKV mode is not used**: the master branch of client-c has no RawKV
  client (the api-v2 branch has one but stalled after 2022); besides, IMetaStore's
  compound transactions need TxnKV anyway, and mixing Raw/Txn on the same cluster
  is an explicit TiKV taboo;
- **TiDB is not introduced**: only PD + TiKV are depended on. The cost is that the
  MVCC GC safepoint must be operated by ourselves (§7.3);
- keyspace / API V2 multi-tenancy is not enabled in the first phase
  (`ClusterConfig.api_version` stays V1; prefix isolation uses `tikv_prefix`,
  §3.2);
- Data-plane cross-process issues (main doc §7 pin table) — same declaration as
  the Redis version: not solved in this document; meta-side correctness stands
  independently.

## 2. Integration Route Selection (Research Conclusions)

### 2.1 Candidate Client Comparison

| Route | Assessment |
| --- | --- |
| **A. tikv/client-c (decided)** | The official C++ client, Apache-2.0, CMake + C++17, directly consumable via `add_subdirectory` (§8). Transport layer is production-grade: TiFlash uses it for region reads and lock resolution, and correctness bugs were still being actively fixed as of 2026-07 (#243/#244/#247 are all lock/snapshot semantics fixes). The transactional write path is test-grade and needs our own extension (§6.3) |
| B. [tikv/client-cpp](https://github.com/tikv/client-cpp) (cxx wrapper around client-rust) | The highest feature ceiling among official routes (client-rust is the most mature client alongside Go: raw + txn + pessimistic all present), but client-cpp self-describes as "proof-of-concept and under heavy development" (37 commits), and pulls the Rust toolchain into the build chain; maturity below client-c's transport layer with a heavier build surface |
| C. Bare kvproto + homegrown client | Direct grpc is feasible, but region cache, routing retries, backoff, lock resolver, and TSO caching would all have to be rewritten — precisely the production-proven 60% inside client-c; reinventing it has no value |
| D. Via TiDB (MySQL protocol) | Becomes "SQL meta store, second edition"; the SqliteMetaStore pattern could be transplanted, but the deployment surface balloons from PD+TiKV to PD+TiKV+TiDB, and the ordered-KV pass-through is lost (list degrades to SQL range queries). If the B/C class routes are ever abandoned, this is the fallback |

A is chosen. Criteria: the only "official + pure C++ + submodule-able"
combination; its gap (§2.2) is concentrated in one place — the 2PC layer — with a
controllable workload and a clear upstream-contribution path.

### 2.2 client-c Status Inventory (Source-Verified, @78a557e)

| Area | Status | Impact on this plan |
| --- | --- | --- |
| Cluster access | `pingcap::kv::Cluster(pd_addrs, ClusterConfig)`; ClusterConfig has built-in TLS (ca/cert/key trio) | Directly usable; TLS can be passed through from the first phase (contrast hiredis, which needs an extra component) |
| Reads | `Snapshot(cluster)` takes a TSO to pin the version; `Get(key)` / `Scan(begin, end)` (`Scanner` batches of 256, region boundaries stitched automatically); residual locks auto-resolved | Directly usable. Gap: `Get` discards the `not_found` flag and returns an empty string — this plan disambiguates via codec values being always non-empty (§3.1), and lists it as an upstream fix item |
| Writes | `Txn` (buffer + `commit()` via `TwoPhaseCommitter`). **Mutations are key→value only, op fixed to Put**: no Del/Insert/Lock (upstream issue #82 "delete key" long open); async commit only enabled by test injection; `commit()` exception path self-annotated TODO | **Not directly usable.** Op support must be extended (§6.3); the kvproto protocol-side `Op` enum is complete (Put/Del/Lock/Rollback/Insert/PessimisticLock/CheckNotExists), the gap is only in the client wrapper |
| Pessimistic transactions | None | Not needed (§4 goes all-optimistic + conflict retry) |
| PD | getTS (TSO), region routing, `getGCSafePoint` (read-only) | Sufficient; **no wrapper for advancing the safepoint** (§7.3 operations takeover) |
| Threading model | Fully synchronous blocking (grpc sync stubs); `Cluster` has its own background threads (TSO oracle 2s refresh, region cache, MPPProber) | Synchronous blocking exactly fits the IMetaStore pool-thread contract (main doc §2.2 named "a TiKV client" as belonging to this category). MPPProber is TiFlash MPP liveness probing — dead weight for pure KV but harmless, listed as an optional upstream switch |
| Error model | `pingcap::Exception : Poco::Exception` + ErrorCodes enum; region/lock class errors are retried in-library by the Backoffer, thrown only past budget | Mapping rules in §6.4 |
| Build | CMake ≥3.10, C++17, exports the `kv_client` static library; **no install/export rules, no tag/release** — add_subdirectory is the only consumption mode (TiFlash does exactly this, vendored in contrib/); depends on gRPC + protobuf (build-time protoc + grpc_cpp_plugin to generate kvproto), Poco (Foundation/Net/JSON/Util; `Poco::Logger` is in public headers, a hard dependency), abseil; bundles its own submodules: kvproto / libfiu (always compiled into kv_client) / abseil-cpp (fallback when the system lacks it) / googletest (tests only) | Dependency surface markedly heavier than hiredis/sqlite; integration strategy in §8 |

## 3. Data Model

### 3.1 General Principles

1. **Value encoding 100% reuses `codec.cc`** — TiKV keys/values are both
   binary-safe. Bonus benefit: codec values are **always non-empty** (first byte
   is the version number), whereas client-c's `Snapshot::Get` signals not found
   with an empty string (discarding kvrpcpb's not_found flag) — always-non-empty
   values make "empty string = does not exist" unambiguous; the gap is absorbed by
   a format invariant;
2. **key = configurable prefix (default `duo:`) + single-character table tag +
   `\0`-separated composite segments**; separator legality follows the same
   argument as main doc §4.1. The prefix serves the same purpose as in the Redis
   version: multiple instances / multiple test sets share one cluster without
   polluting each other (the prefix retires once API V2 keyspace is adopted);
3. **One table, one prefix — back to the RocksDB CF mental model.** TiKV is
   globally ordered; the byte order of `O<bucket>\0<key>` is the S3 lexicographic
   order, so list needs no secondary index — contrast the Redis version's forced
   HASH+ZSET double-key structure; here a single key suffices (§3.3).

### 3.2 Key Layout

Row-by-row correspondence with the CF table in main doc §4.1 (keys below omit the
`duo:` prefix):

Table tags are always **single characters** (guard tables use the corresponding
lowercase letter) — two-character tags (e.g. `Bg`) would be ambiguous with bucket
names' first characters (bucket names contain `[a-z]`, so `B` + `g...` collides
with `Bg` + `...`):

| RocksDB CF | TiKV key | value | Description |
| --- | --- | --- | --- |
| `default` | `s` | `"t1"` | Schema lineage marker, read and validated on open; if missing, Insert within a transaction (§4.4) |
| `buckets` | `B<bucket>` | `encode_bucket` | create uses Op::Insert to express "must not exist" (§4.4) |
| — (new) | `b<bucket>\0<u8 shard>` | no value (Op::Lock placeholder) | **Bucket guard shards**, shard ∈ [0,16): materialize the write-skew conflict between put/create_upload and delete_bucket (§4.3) |
| `objects` | `O<bucket>\0<key>` | `encode_object` | Byte order = S3 lexicographic order, directly supporting list (§3.3) |
| `uploads` | `U<bucket>\0<key>\0<id>` | `encode_upload` | A prefix scan yields (key, id) order, isomorphic to RocksDB |
| — (new) | `u<bucket>\0<key>\0<id>\0<u8 shard>` | no value (Op::Lock placeholder) | **Upload guard shards**: materialize the write-skew conflict between put_part and complete/abort (§4.3) |
| `parts` | `P<bucket>\0<key>\0<id>\0<be16 part_no>` | `encode_part` | big-endian part_no guarantees ascending order |
| `refs` | `R<be64 file_id>` | owner summary | `chunk_referenced` = single-key Get |
| `gcq` | `G<be64 seq>` | `encode_reclaim` | `peek_reclaims` = prefix Scan limit max; seq is the key order |
| `stats` counters | `C<kind>` (kind ∈ {`0`,`1`,`q`}) | 8B little-endian i64 (reusing the codec counter format) | Segment reservation (§5) |
| `stats` pack accounting | `S<be64 id>d<be64 delta_id>` (delta rows) / `S<be64 id>s` (seal row) | delta = le64 bytes‖le64 recs; seal = le64 file_size | Lands with pack aggregation (P2) as **delta rows + folding**: each business transaction writes a unique delta row (id from the 'd' segment counter), pure writes without conflict — a read-modify-write on a shared account row would make small-object PUT prewrites on the same active pack collide (the materialized solution to the original warning); `pack_stats()` aggregates via prefix scan, and when a single pack exceeds 16 delta rows it folds them into one row in passing (delete old rows + write the merged row, no conflict with concurrent additions) |

### 3.3 list_objects: Snapshot + Scanner, Algorithm Copied from the RocksDB Version

The iteration structure of the RocksDB version (main doc §4.4) holds verbatim;
the iteration primitive becomes `Snapshot::Scan`:

- Open one `Snapshot` (TSO-pinned version) as the consistent view for the entire
  list — **valid across Scanner rebuilds**, a direct dividend of MVCC: the
  single-invocation consistency that the Redis version had to buy by "stuffing the
  whole loop into one Lua script" (duostore-redis-meta.md §2.3) is free here;
- Seek start = `max(prefix, successor of start_after)`; after a delimiter hit
  groups, **construct the successor seek point by +1 on the group's last byte**
  and open a new Scanner on the same Snapshot to skip the whole group — one gRPC
  round trip per group (sub-millisecond on a LAN). Of the Redis version's two
  reasons for rejecting "client-driven multi-round" (RTT amplification + no
  consistent view across rounds), only the former remains here, and its magnitude
  is isomorphic to the RocksDB version's one Seek per group; acceptable. No
  coprocessor pushdown — that is the domain of SQL/TiFlash; introducing an
  execution framework for list is out of proportion;
- Fetch one extra row to decide `is_truncated`; when truncation lands exactly on a
  delimiter group, next_token must take the group's last key (the group was
  skipped, not traversed): client-c does not wrap a reverse-scan primitive, so use
  **deferred forward scanning** instead — only when genuinely truncated with the
  last item being a group, do one paged forward scan over that group to fetch the
  tail key. At most once per list; group sizes are usually far smaller than the
  bucket; this is the only implementation difference from the RocksDB version
  (SeekForPrev);
- Version-visibility constraint: a Snapshot older than the GC safepoint is
  rejected (§7.3); a list call completes in seconds, leaving ample margin against
  the safepoint watermark (minutes-to-hours scale); a statement suffices.

## 4. Transactions and Invariants

### 4.1 Route: Optimistic 2PC + Conflict Retry, No Pessimistic Locks

`IMetaStore`'s commit-class methods are all "read-validate-write": mapped to
**read on the start_ts Snapshot, compute on the C++ side, assemble the mutation
batch, commit once via 2PC**. TiKV prewrite checks, for every key in the write
set, "whether a write record with commit_ts > start_ts or someone else's lock
exists" — i.e. **for every key this transaction writes, "the value read was not
concurrently modified" is guaranteed for free by the protocol**
(WriteConflict / KeyIsLocked → retry). Comparison:

- The Redis version has to pass "the raw bytes read" to the Lua script as
  preconditions for one-by-one comparison (duostore-redis-meta.md §3.2); the TiKV
  version gets natural CAS for keys **in the write set**, and most preconditions
  vanish;
- The exception is read-only preconditions (bucket existence, delete_bucket's
  emptiness check, put_part's upload existence) — optimistic 2PC does not
  validate read-only keys, forming a write-skew window, materialized with guard
  keys (§4.3).

The retry loop is isomorphic to the Redis version's §3.2:
WriteConflict / KeyIsLocked (thrown after the in-library backoff exceeds budget)
→ take a new start_ts, re-read, re-assemble the batch, re-commit; exponential
backoff starting at 100µs, capped at 6.4ms, at most 16 attempts; past the limit
throw `InternalError` (pathological hotspot; failing loudly beats livelock).

### 4.2 Why Pessimistic Transactions Were Rejected

client-c has no pessimistic wrapper (the kvproto protocol has it); filling it in
= introducing lock lifecycle management (TTL renewal, deadlock-detection
interplay), a workload several times the op extension of §6.3. Meanwhile this
workload's conflict surface, thinned by guard sharding, is extremely narrow
(§4.3), and the amortized cost of optimistic retries is negligible. If a
pathological hotspot ever appears (high-frequency overwrites of the same key),
the upgrade path is pessimistic locking — listed as evolution, not in the first
phase.

### 4.3 Materializing Write Skew: Guard Shards

The problem pattern: transaction T1 reads key X to validate (without writing X),
transaction T2 writes X — optimistic 2PC does not detect T1's read set, so if
both commit concurrently T1's validation is voided. This interface has three such
spots:

| Read-only validator | Concurrent writer | Consequence (if not materialized) |
| --- | --- | --- |
| put_object / create_upload read `B<bucket>` to validate the bucket exists | delete_bucket deletes `B<bucket>` | Bucket already deleted yet the object/upload write succeeds: refs leak permanently, recreating the bucket resurrects ghosts (main doc §4.5 explicitly requires preventing this) |
| delete_bucket scans `O<b>` / `U<b>` for emptiness | put_object creates a new object | Same as above (the symmetric face) |
| put_part reads `U<...>` to validate the upload exists | complete/abort deletes `U<...>` | Orphan part rows + refs leak |

**Solution: make the "read-only side" also write a key shared by both parties,
materializing the read-write conflict into a write-write conflict.** Writing
`B<bucket>` itself would make all PUTs on the same bucket conflict with each
other (a hotspot), hence **guard shards**:

- put_object / create_upload: attach an **Op::Lock** mutation to
  `b<bucket>\0<hash(key) % 16>` (a placeholder lock record, no value written);
  delete_bucket: write `B<bucket>` (Del) + all 16 `b` shards (Op::Lock). Any put
  concurrent with delete_bucket must collide on a shard; two puts collide with
  probability 1/16, and the retry is cheap;
- put_part: attach Op::Lock to `u<...>\0<part_no % 16>`; complete / abort: write
  `U<...>` (Del) + all 16 `u` shards. Concurrent uploads of different part
  numbers on the same upload do not block each other (S3 clients uploading parts
  in parallel is the norm; it must not be serialized);
- **complete_upload / put_part (same-number re-upload) / swap_extents /
  delete_object need no bucket guard**: their precondition objects (the upload
  row, the object row) are visible in the counterpart's snapshot —
  delete_bucket's emptiness scan sees the `U`/`O` rows and refuses; the conflict
  is naturally materialized by existing data rows, no extra key needed.

Op::Lock's virtues: it produces no value, occupies no storage, and only leaves a
conflict record in the write column (the same mechanism as TiDB's `LOCK KEYS`).
**T1 verification item**: the semantics of Lock-type write records being counted
by subsequent prewrite conflict detection need smoke confirmation (TiKV has
adjusted conflict adjudication of Lock/Rollback records between versions); if the
semantics do not hold, the fallback = Op::Put writing back the original value
(equivalent materialization, just extra version garbage), interface unchanged.

### 4.4 Per-Method Flows

Against the Redis version's §3.3 (pure-read and segment methods omitted;
"Lock" = Op::Lock guard):

| Operation | Read set (start_ts snapshot) | Mutation batch |
| --- | --- | --- |
| create_bucket | — | **Insert** `B` (AlreadyExist error → BucketAlreadyOwnedByYou, protocol-level expression of "must not exist") |
| delete_bucket | `B` exists; `O<b>` / `U<b>` prefix-scan emptiness checks | Del `B` + Lock all `b` shards |
| put_object | `B` exists; old `O` (compute version and gcq accounting) | Put `O`(version+1) + Lock `b` shard + Put new `R` + Del old `R` + Put `G`(old DataRef) |
| delete_object | old `O` (absent → return false, no transaction issued) | Del `O` + Put `G` + Del `R` |
| create_upload | `B` exists | Put `U` + Lock `b` shard |
| put_part | `U` exists; old `P` (same-number re-upload accounting) | Put `P` + Lock `u` shard + refs/gcq as in put_object |
| complete_upload | `U`; all `P` via prefix scan; old same-name `O` | Put `O` + Del `U` + Del all `P` + Lock all `u` shards + unselected parts into `G` + old object into `G` + refs transfer |
| abort_upload | `U`; all `P` | Del `U` + Del all `P` + Lock all `u` shards + all parts into `G` + Del `R` |
| swap_extents | `O` (C++-side decode validating expect_version and from; mismatch → return false) | Put `O`(version+1, DataRef=to) |
| ack_reclaim(s) | — | Del `G<seq>` (multiple seqs may share a batch — `ack_reclaims` overridden as a single transaction, the upgrade point over the interface's per-item default) |

Keys in the read set that this transaction **writes** (old `O`, `U`, `P`) are
naturally validated by prewrite; read-only keys in the read set all fall under
the guard/Insert mechanisms in the table above — the union of the two covers all
precondition checks of the Redis version's §3.3, with no omission (complete's
parts sha1 fingerprint is no longer needed: all `P` are Del'ed, i.e. all in the
write set and CAS-protected).

### 4.5 Atomicity Argument Under Multiple Gateways

The RocksDB version relies on an in-process mutex, the Redis version on
server-side single-threaded Lua; the TiKV version's counterpart is **Percolator
2PC itself** — atomicity holds by protocol for any client, any process, and the
transaction's participating keys may span regions/nodes (the primary key defines
the commit point, secondaries land asynchronously; a reader hitting a residual
secondary lock consults the primary via LockResolver — client-c's read path has
this logic built in and it is the part TiFlash uses in production).
TikvMetaStore **holds no business mutex** (only the small in-memory lock for
segment hand-out, §5), same shape as the Redis version.

### 4.6 Blind-Retry Ban and Indeterminate Results

The rule is inherited word-for-word from the Redis version's §3.5: after a commit
request has been sent, a connection/timeout failure = indeterminate result —
**always throw `InternalError`, never blindly replay** (a replay would enter the
just-written DataRef into gcq, and GC would reclaim data still referenced). TiKV
version specifics:

- 2PC's commit point = the primary key's commit landing. client-c's behavior on
  exceptions during the commit phase is self-annotated TODO (`2pc.cc` "TODO:
  Process commit exception") — the T1 extension must make explicit the two paths
  "secondary failure after primary commit = already committed successfully" and
  "primary commit result indeterminate = InternalError" (§6.3);
- Evolution (not in the first phase): on an indeterminate result,
  `CheckTxnStatus` (already in kvproto; used internally by client-c's
  LockResolver) can consult the primary to disambiguate, turning part of the
  InternalErrors into definite answers;
- CAS/WriteConflict retries are not in this category — a conflict is a definite
  result, safe (same as the Redis version).

## 5. alloc_file_id: Counter RMW Segments

A small read-modify-write transaction on the `C<kind>` counter key: read the old
value → Put old value+4096 → commit; WriteConflict → retry (multiple gateways
race for segments; the winner takes the interval). The `IdRange` struct, the
separate `alloc_mu_`, and `kIdSegment = 4096` are copied from the RocksDB version
(main doc §4.5). seq works the same way, pre-dispensed via `C<seq>`; gcq
bookkeeping stays pure-write.

The Redis version §4's three-layer mitigation for "crash rollback re-issuing used
ids" is **not needed here**: a TiKV commit is raft-majority durable, with no
everysec-style loss window; cross-gateway global uniqueness of segments is
guaranteed by the transaction. The backstop layer 3 is kept (data-plane `O_EXCL`
failing loudly on file collision) — it lives in the data plane and is always
there anyway.

## 6. client-c Integration and Necessary Extensions

### 6.1 Lifecycle and Threading Model

- One `pingcap::kv::Cluster` per store (containing the grpc channel pool, region
  cache, TSO oracle background thread); synchronous blocking calls happen on pool
  threads — exactly the pattern main doc §2.2 predicted by name when choosing a
  synchronous interface for IMetaStore. `Cluster` methods are thread-safe
  (TiFlash sharing a single Cluster across threads is its designed usage), **no
  connection pool needed** — contrast hiredis's homegrown pool; that layer of
  complexity disappears;
- `close()`: set the closed flag, then destroy the Cluster (its destructor stops
  rpc/prober/region cache/thread pool in order); any call afterwards cleanly
  throws `InternalError` (defense-in-depth convention). Destruction happens after
  no calls are in flight — guaranteed by DuoStoreBackend's close ordering (main
  doc §9 lifecycle);
- Timeouts: client-c has no global "per-call timeout" parameter; each operation's
  Backoffer budget is an in-library constant (e.g. GetMaxBackoff /
  prewriteMaxBackoff). Since T5, sidecar paths (2PC commit, batch_get, last_key,
  safepoint RPCs) have their budgets parameterized via `tikv_backoff_ms` (0 =
  library default; commit uses 2× to match the upstream commit:prewrite ratio,
  §9); Backoffers built internally by upstream Snapshot/Scanner remain
  uncontrolled — global parameterization stays an upstream improvement item. The
  outer backstop is the S3 request timeout.

### 6.2 Handling the Poco Dependency

`Poco::Logger`/`Poco::Exception` appear in client-c public headers; Poco
Foundation is a hard dependency (find_poco also requires Net/JSON/Util). No
stripping (the change would cut deep into all upstream source files); the
dependency is accepted. The log bridge landed with T5 (`PocoSpdlogChannel` in
`tikv_client.cc`): when the first TikvClient is constructed, the Poco root
logger's channel is swapped for the spdlog bridge (must precede the creation of
any pingcap logger — Poco child loggers inherit the channel at creation);
`pingcap.*` logs enter the unified stderr format as "source name: message"; the
Poco side sends from information level up, and after entering spdlog they are
filtered a second time by the global level.

### 6.3 2PC Extension: In-Tree Sidecar (Implemented, `tikv_client.{h,cc}`)

The original plan was to fork (`locallocal/client-c`) and extend on the fork;
during implementation this became an **in-tree sidecar**:
`src/storage/duostore/tikv_client.cc` builds on client-c's public transport
infrastructure (`Cluster` / `RegionCache` / `RegionClient` / `Backoffer` /
`LockResolver`) and adapts its `kv/2pc.cc` (Apache-2.0, provenance noted in the
file header) into our own committer. Rationale for the change: a fork pointer
would make the submodule point at a non-reproducible, non-upstream commit, and
the fork's maintenance/sync cost would land on this repo; the sidecar keeps the
submodule a pristine upstream pin — an upgrade = swap the pointer + regression.
The upstream PR remains a T5 item; the sidecar retires once merged. Sidecar
differences from upstream 2pc.cc (itemized in the file header):

1. **Mutations carry an op** (Put / Del / Lock / Insert, §4.4): prewrite
   assembly calls `mut->set_op(...)`; the kvproto protocol side is complete
   already;
2. **Structured errors**: `already_exist` → `TikvAlreadyExist` (create_bucket
   converts to `BucketAlreadyOwnedByYou`), `write_conflict`/`retryable` →
   `TikvConflict` (consumed by the retry loop) — upstream is a vague
   LogicalError/Unknown. Includes one reclassification of an upstream bare
   exception: `resolveLocksForWrite`, when hitting "a live lock of a newer
   transaction", throws `Exception("write conflict")` (upstream TODO, no error
   code); the prewrite phase is definitively uncommitted, so it is classified as
   `TikvConflict` by message (the guard-shard special
   `duostore_tikv_write_skew_guard` exposes this path on a real cluster);
3. **Commit exception paths made explicit** (the two §4.6 branches): a definite
   primary rejection = already rolled back (TiKV's commit is idempotent-ok for an
   already-committed transaction) → safe to retry; an RPC-layer exception =
   `TikvUndetermined` → InternalError upwards. Replaces the upstream TODO;
4. **Best-effort `BatchRollback` lock cleanup after prewrite failure** (upstream
   TODO): shortens concurrent parties' waits on residual locks; failing to clean
   is harmless (TTL/reader LockResolver converge);
5. **Commit retries do not change commit_ts**: a commit with the same
   (start_ts, commit_ts) is idempotently replayable; upstream's region retry
   re-fetches a TSO, introducing an unnecessary semantic fork.

The Get not_found overload was not done (codec values being always non-empty
already disambiguates, §3.1); `Snapshot`/`Scanner` are likewise reused from
upstream as-is.

Explicitly not done: pessimistic (§4.2), async commit, TTLManager hardening
(a regular transaction has ≤ tens of mutations + 16 guards, a single batch far
below the `txnCommitBatchSize = 16KiB` key volume; no long transactions).
**Handling of large completes (decided in T5)**: a complete_upload with
ten-thousand-scale parts carries about 20k mutations; prewrite sends batches
serially, and the duration may approach the default lock_ttl(3s) — since T5 the
sidecar scales per upstream `txnLockTTL` semantics: once the transaction's bytes
exceed the single-batch bound, lock_ttl is enlarged by `ttlFactor·√MiB`, clamped
to [3s, 20s] (`tikv_client.cc` txn_lock_ttl). Ten-thousand-part special measured
(`duostore_tikv_bulk_complete_10k_parts`, with a concurrent reader driving
LockResolver's TTL adjudication against the in-flight primary throughout): a
10000-part complete takes ~0.5s end to end, a 6x margin below the 3s default
floor, wider still after scaling. `TTLManager` heartbeats stay unwired —
transaction size is bounded (the S3 ten-thousand-part maximum = the largest
transaction), no "unknown-duration" long transactions; if a >20s prewrite shape
ever appears, wire it then (the machinery is readily in the library).

### 6.4 Error Mapping

Uniformly via `throw_tikv(what, e)` (mirroring `throw_status`/`throw_reply`:
LOG_ERROR + throw `s3::S3Error`):

| Source | Handling |
| --- | --- |
| WriteConflict / KeyIsLocked (within retry budget) | In-library backoff, invisible upwards |
| Same (thrown past budget) | §4.1 retry loop; past 16 attempts → `InternalError` |
| AlreadyExist (Op::Insert) | `BucketAlreadyOwnedByYou` (the only use site) |
| RegionUnavailable / network / TimeoutError | Pure reads → retry once then `InternalError`; commit-class indeterminate → `InternalError` (§4.6 ban) |
| Semantic absence (empty Get) | Not an error — the semantic layer converts to `NoSuchKey` / `NoSuchBucket` / `NoSuchUpload`, isomorphic across implementations |
| Other `pingcap::Exception` | `InternalError`(500) + error code/displayText into the log |

## 7. Durability, Consistency, and MVCC GC

### 7.1 Durability Statement

Commit = raft majority persisted to disk (sync-log always on since TiKV 5.0, not
disableable). Against main doc §6.3: the `meta_sync` config is **meaningless for
this implementation (always effectively true)**; if set, WARN and ignore — the
strongest durability tier of the four implementations; the Redis version §6's AOF
argument matrix retires wholesale here.

### 7.2 Cluster-Side Failures

- Minority replica failure: transparent (raft re-election + client region cache
  refresh and retry);
- Majority failure / all PDs down: meta unavailable, surfacing as 5xx at the S3
  layer — no inconsistency is produced; duostore's "data lands first, meta
  commits after" crash matrix (main doc §6.2) holds verbatim under the "commit
  did not complete" branch;
- TSO clock: monotonicity is guaranteed by PD; gateway local clocks play no part
  in the commit order — no clock-skew problem across gateways.

### 7.3 MVCC GC Safepoint: The Operational Duty of a TiDB-less Deployment

TiKV's multi-version garbage is reclaimed by advancing the GC safepoint; **in a
standard deployment the advancer is TiDB**. This plan introduces no TiDB (§1),
therefore:

| Deployment shape | Safepoint strategy |
| --- | --- |
| Cluster also serving TiDB (shared) | Nothing to do; TiDB advances as usual |
| Pure-KV cluster + long-running | **Must advance ourselves**: periodically call PD's UpdateServiceGCSafePoint (kvproto has it, client-c does not wrap it — upstream extension or a pd-ctl/HTTP API side script). Advance to now − retention window (e.g. 10 minutes; it only needs to cover the longest list/transaction duration) |
| Test/short-lived cluster | Not advancing is fine (garbage accumulates but correctness is unharmed) |

The advancement scheme landed with T5 (the in-tree sidecar wraps the PD RPC trio:
`UpdateServiceGCSafePoint` / `UpdateGCSafePoint` / `GetGCSafePoint`;
`tikv_client.cc` builds its own stub straight to the leader via `getLeaderUrl()`
— client-c's PD stubs are all private; on leader change/error the cache is
dropped and rebuilt). The TikvMetaStore background worker advances one round
every `tikv_gc_interval` (default 60s, 0 = off), three steps per round
(`update_gc_safepoint_once`, directly callable by tests):

1. Register this gateway's service safepoint = now − `tikv_gc_retention`
   (default 10 minutes; TTL 3×interval, so a gateway whose advancement stalls is
   auto-removed after two rounds);
2. **Stand in for TiDB's gc_worker role**: for a missing `gc_worker` service
   entry, PD permanently placeholds it at the current cluster safepoint (infinite
   TTL) — without pushing it, the min stays pinned forever, which is precisely
   the materialized form of "nobody advances on a pure-KV cluster". Update the
   `gc_worker` entry to the same target value with infinite TTL;
3. Advance the cluster safepoint to the all-services min returned by step 2 —
   never overtaking the declared snapshot of any live service (external tools of
   the BR/CDC kind); PD-side monotonic advance-only, so concurrent advancement by
   multiple gateways naturally converges.

Clusters shared with TiDB must set `tikv_gc_interval: 0` (first table row,
"nothing to do"); otherwise it races writes with the real gc_worker (harmless
under monotonic semantics but pointless). Failures count into
`safepoint_update_failures_total` and are retried next round; the latest advanced
value lands in the `gc_safepoint_ms` gauge.
The consequence of not advancing is space amplification and slower scans, not a
correctness problem.

## 8. Build Integration

### 8.1 Submodule (Done) and Initialization Strategy

`.gitmodules` already adds:

```text
[submodule "third_party/client-c"]
    path = third_party/client-c
    url = https://github.com/tikv/client-c.git
```

No shallow (the repo itself is <1MiB; ~9MiB with the kvproto checkout). Upstream
has no tags; pinned by commit (currently `78a557e`). client-c bundles 4
submodules, of which only 2 are needed: kvproto (protocol, required), libfiu
(always compiled into kv_client, required). abseil-cpp is not pulled — use the
system/unpacked absl uniformly (same ABI as the one grpc links against, avoiding
two coexisting copies); googletest is not pulled (ENABLE_TESTS always OFF;
upstream #104: ON is known build-broken).

`build.sh`: **not in `LIGHT_MODULES`** — opposite to rocksdb/hiredis's
"zero system-level dependencies, always init"; this component needs a
system-level gRPC/Poco toolchain (§8.2), so it follows the seastar-style lazy
fetch: with the new `--tikv` switch

```bash
git submodule update --init third_party/client-c
git -C third_party/client-c submodule update --init \
    third_party/kvproto third_party/libfiu
```

and append `-DLIGHTS3_DUOSTORE_TIKV_META=ON` (sticky semantics like `--seastar`;
`-B build-tikv` recommended to isolate from regular builds).

### 8.2 Dependency Matrix and the No-Sudo Environment Route

| Dependency | Purpose | With sudo (deploy/CI machine) | This dev machine (no sudo) |
| --- | --- | --- | --- |
| gRPC (library + `grpc_cpp_plugin`) | kvproto codegen + transport | `libgrpc++-dev protobuf-compiler-grpc` | dpkg -x extraction (below) |
| protobuf (library + `protoc`) | Same | `libprotobuf-dev protobuf-compiler` | Same |
| Poco Foundation/Net/JSON/Util | client-c public headers/logging | `libpoco-dev` | Same |
| abseil | grpc/kvproto | `libabsl-dev` (or via client-c's bundled submodule) | Same |

Local status (verified and in place): grpc/protobuf/Poco/protoc/grpc_cpp_plugin
are all absent at the system level. The route follows the librados precedent:
**apt-get download + dpkg -x, extracting the whole chain into
`~/.local/opt/tikv-deps`** (23 debs: libgrpc++-dev/libgrpc-dev
and runtimes, protobuf-compiler[-grpc]/libprotobuf-dev/libprotoc-dev and
runtimes, libabsl-dev/libabsl20260107, libpoco-dev and Foundation/Net/
JSON/Util/XML runtimes, libre2/libc-ares; openssl/zlib already on the system).
The CMake side does not do `CMAKE_PREFIX_PATH` blanket probing; instead
**`LIGHTS3_TIKV_DEPS_ROOT` points at the unpack prefix and each variable is
preset** (Protobuf_*/gRPC_*/Poco_*_LIBRARY — client-c's finds are all guarded
with `if(NOT ...)`, so parent-scope presets short-circuit them wholesale); the
unpacked protoc / grpc_cpp_plugin need same-tree .so files to run, so
configure-time-generated LD_LIBRARY_PATH wrappers serve the kvproto codegen.
`~/.local/opt/tikv-deps/usr` is auto-detected when present, no manual argument
needed. Dedicated build directory **`build-tikv`** (convention like
build-redis / build-rados). Runtime linking: one libdir for the whole tree,
`--disable-new-dtags` + rpath/rpath-link (RUNPATH does not propagate deep into
the dependency tree — same argument as librados).

Version-consistency constraint (undocumented upstream, upheld by our own
discipline): kvproto-generated code must match the linked protobuf runtime
version (the classic one-protobuf-per-binary constraint), and protoc /
grpc_cpp_plugin / library must be same-origin same-version — a deb chain from
one distro snapshot satisfies this naturally; this repo has no other protobuf
user, no second conflict source. The only fully tested configuration of client-c
is TiFlash's vendored toolchain; our combination is untested territory, and the
T1 smoke test stakes exactly this down.

Rejected alternative: building gRPC from a source submodule — clone and build
volume is huge (minutes-scale becomes tens-of-minutes clean build), and after
protoc bootstrapping the host/target consistency still must be handled; the
benefit of dependency self-containment does not carry that cost, and dpkg -x is
proven feasible (the librados precedent).

### 8.3 CMake Wiring (After the hiredis Template)

New option **`LIGHTS3_DUOSTORE_TIKV_META`, default OFF** (depends on
`LIGHTS3_DUOSTORE`) — same reasons as redis/sqlite: tests need an external
cluster present (§10), and the dependency surface is heavy; not in the daily
build.

The implementation lands in the top-level `CMakeLists.txt`'s
`LIGHTS3_DUOSTORE_TIKV_META` block (dependency presets + wrapper generation +
`add_subdirectory(third_party/client-c EXCLUDE_FROM_ALL SYSTEM)`), with target
wiring:

```cmake
target_sources(lights3_core PRIVATE
  src/storage/duostore/tikv_client.cc
  src/storage/duostore/tikv_meta_store.cc)
target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_TIKV_META)
target_link_libraries(lights3_core PRIVATE kv_client)
```

Key points:

- `kv_client`'s include directories are PUBLIC-propagated (pingcap headers →
  grpc/kvproto generated headers), so our `.cc` can include `<pingcap/kv/...>`
  directly; the Poco include and Net/JSON/Util linkage are **under-propagated /
  under-linked upstream** (its sources actually use them; in the TiFlash
  environment they happen to be globally visible), supplemented by us after
  add_subdirectory;
- client-c's top-level `set(CMAKE_CXX_STANDARD 17)` / `-Wno-narrowing` apply
  only within its subdirectory scope and do not pollute the repo-wide C++20
  flags; under the same toolchain, linking a C++17 static library into a C++20
  target is ABI-safe (same precedent as rocksdb); kv_client/kvproto/fiu get `-w`
  appended (third-party sources are not bound by -Wall -Wextra, same convention
  as sqlite);
- kvproto codegen is wired inside client-c as a custom command
  (`Protobuf_PROTOC_EXECUTABLE` / `gRPC_CPP_PLUGIN` are exactly our preset
  wrappers), no extra step needed;
- When OFF, configuring `meta: tikv` throws "not compiled in" in `from_params`
  (convention).

### 8.4 Component Relations and Reuse

- New files are only `tikv_meta_store.{h,cc}`; the `DuoStoreBackend` constructor
  gains one `#ifdef LIGHTS3_DUOSTORE_TIKV_META` branch (`DuoMetaKind::kTikv`);
  data plane/GC/S3 layer untouched — the fourth delivery of the semantic-level
  interface promise;
- Reused: `codec.{h,cc}` for all value codecs (§3.1), the `be64_key`/`part_key`
  be-encoding helpers (key layout isomorphic to RocksDB, a larger reuse surface
  than the Redis version), `storage/validate.cc`, `storage/multipart.h`, the
  meta test suite `tests/unit/meta_store_suite.h`.

## 9. Configuration

`DuoStoreConfig::from_params` adds new keys (YAML scalar convention as in main
doc §11):

```yaml
backends:
  - name: duodata
    type: duostore
    root: ./data/duostore
    meta: tikv                          # rocksdb / redis / sqlite / tikv
    pd_endpoints: "10.0.0.1:2379,10.0.0.2:2379,10.0.0.3:2379"
    tikv_prefix: "duo:"
    # tikv_ca / tikv_cert / tikv_key: TLS trio (optional, passed through to ClusterConfig)
    # tikv_gc_interval: 60s               # GC safepoint advancement period (0 = off, §7.3)
    # tikv_gc_retention: 10m              # safepoint retention window (now − retention)
    # tikv_backoff_ms: 0                  # sidecar-path backoff budget (0 = library default, §6.1)
    # remaining duostore keys (chunk_size / pack_* / gc_* / mpu_ttl ...) unchanged
```

| Key | Default | Description |
| --- | --- | --- |
| meta | `rocksdb` | Adds legal value `tikv`; selecting tikv when not compiled in → configuration error |
| pd_endpoints | — (required when meta=tikv) | Comma-separated PD address list |
| tikv_prefix | `duo:` | Prefix for all keys (multi-instance/test isolation, §3.1) |
| tikv_ca / tikv_cert / tikv_key | empty (plaintext) | mTLS certificate paths; enabled only when all three are given |
| tikv_gc_interval | `60s` | GC safepoint advancement period; `0` disables (cluster shared with TiDB, §7.3); duration unit same as gc_interval |
| tikv_gc_retention | `10m` | Safepoint retention window, must be positive; only needs to cover the longest list/transaction duration |
| tikv_backoff_ms | `0` | Backoff budget for sidecar paths (2PC/batch_get/last_key/safepoint); `0` = client-c library default (§6.1) |

With `meta: tikv`, `meta_path` / `rocksdb_block_cache` / `meta_sync` are ignored
with a WARN (§7.1).

## 10. Test Strategy

1. **Suite reuse**: the `meta_store_suite` factory registers TikvMetaStore to run
   conditionally — the same semantic baseline as sqlite/redis inherited in full;
   on the composition side, injection-construct
   `DuoStoreBackend(cfg, pool, TikvMetaStore, FsDataStore)` and run
   `run_backend_suite`; `run_e2e.sh` gains a `duostore-tikv` branch;
2. **Obtaining a real cluster**: the environment variable `LIGHTS3_TEST_PD_ADDR`
   points at an external cluster (tiup playground / an existing test cluster);
   **absent → explicit SKIP** — the librados precedent (no cluster on this
   machine, always SKIP); isolation via a per-case unique `tikv_prefix`
   (pid+counter, same strategy as Redis). No in-process mock: client-c's
   MockPDClient mocks only PD, a pure in-memory fake TiKV is nowhere to be had,
   and this plan's core (2PC conflict semantics) is untestable by mock;
3. **TiKV specials**: §4.3 guard materialization — two stores concurrently
   put_object vs delete_bucket / put_part vs abort, verifying that one side must
   conflict-retry or be refused, with no ghost residue; Op::Lock conflict
   semantics smoke (T1 verification item); WriteConflict retry convergence and
   the 16-attempt limit path; the `splitRegion` test hook creates multiple
   regions, then run list pagination and cross-region transactions (2PC
   primary/secondary in different regions);
   ten-thousand-part complete_upload prewrite duration and lock_ttl margin
   (§6.3, ✅ T5: `duostore_tikv_bulk_complete_10k_parts`, 10000 parts + a
   concurrent reader driving TTL adjudication, measured ~0.5s);
   residual-lock recovery — kill the gateway process mid-commit; another gateway
   reading the same key unlocks and progresses normally via LockResolver;
   T5 additional specials: conflict-retry metric counting (bounded rounds must
   occur under hotspot contention + zero value visible), GC safepoint
   single-round advancement (return value >0, monotonic across rounds, gauge in
   sync) and worker mode (interval=1s advances on the first tick, close stops
   cleanly);
4. **Upstream pinning**: the submodule pointer pins an upstream commit (the §6.3
   sidecar does not modify upstream sources); CI does not chase master — an
   upgrade = explicit pointer swap + full-suite regression.

## 11. Implementation Phases

| Phase | Content | Independently acceptable | Status |
| --- | --- | --- | --- |
| T0 | Research + this document + `third_party/client-c` submodule introduction (pinned to 78a557e) | Document review | **Done** |
| T1 | Dependency matrix in place (dpkg -x chain + build-tikv); CMake option + build.sh `--tikv`; 2PC sidecar with ops (Put/Del/Lock/Insert + commit exception paths + lock cleanup, §6.3) | Full build green; cluster smoke (incl. the Op::Lock guard-semantics special `duostore_tikv_write_skew_guard`) | **Done** |
| T2 | `TikvMetaStore` skeleton: error mapping / retry loop / close guard; `C<kind>` segment alloc_file_id; the four bucket methods (incl. Insert semantics) + schema validation; meta suite wiring + PD probe/SKIP mechanism | Cases green with a cluster present; SKIP path green without one | **Done** |
| T3 | The four object methods (list = Snapshot+Scanner, §3.3) + guard shards + refs / gcq / swap_extents / chunk_referenced / peek_reclaims / batched ack_reclaims override | Meta suite all green + write-skew/conflict specials | **Done** |
| T4 | Full multipart set (incl. `u` guards); injected composition `run_backend_suite`; `e2e_duostore_tikv` | Backend consistency suite + e2e green | **Done** |
| T5 | Polish: GC safepoint advancement scheme (§7.3 worker + gc_worker role takeover), Poco log convergence (§6.2 bridge), timeout parameterization (`tikv_backoff_ms`, §6.1), metrics (conflict retry/safepoint counts), ten-thousand-part complete special and TTLManager evaluation (§6.3 lock_ttl scaling, heartbeat left unwired) | Full ctest matrix (incl. skip paths) green | Done (2026-07-30; except upstream PR contribution and pointer upgrade — dependent on the upstream process, see the §11 endnote) |

**§11 endnote — upstream contribution item (the only unclosed T5 sub-item,
dependent on the upstream process)**: contribute to tikv/client-c the 2PC
mutation op extension and the `Snapshot::Get` not_found overload; once merged,
upgrade the submodule pointer and retire the sidecar accordingly. Until then the
sidecar's two message-string couplings to `@78a557e` stay in force
(`tikv_client.cc`'s fallback match on resolveLocksForWrite's bare
`Exception("write conflict")`, with a preceding structured classification that
lowers the dependency, file-header difference 7) — **that message string must be
re-checked whenever the pointer is upgraded**. Tracked as a todo.md §2.4
loose end.
