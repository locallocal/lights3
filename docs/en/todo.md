> English translation of [../todo.md](../todo.md). The Chinese original is authoritative; section numbering matches. This is a point-in-time snapshot of a living document; check the Chinese original for the latest state.

# TODO — Project Backlog

> Generated: 2026-07-26. Derived by cross-checking the roadmap sections of all
> design documents against a source scan (TODO comments, NotImplemented paths,
> factory registrations, test coverage, CMake/build.sh switches). Each item
> cites a document section number or a `file:line` reference; dependencies are
> marked with "prerequisite".

## 0. Status Snapshot

Completed (with commits as evidence): four-layer architecture + four HTTP
drivers + SigV4 (incl. chunked/presigned) + credential management phases 1
and 2 + S3 protocol gap closure (UploadPartCopy / presigned skew, §4); on
the storage side localfs / xlocalfs / memory / tiered (P1-P5) / cloudproxy
(P1-P5) / all of duostore P1-P5 (RocksDB meta + chunk/pack data + GC phases
1-2 + polish), plus the duostore pluggables: Redis meta (R1-R3),
SQLite meta (S1-S3), RADOS data (C1-C4), TiKV meta (T1-T4).

Overview of incomplete phases:

| Phase | Source | Status |
| --- | --- | --- |
| DuoStore P2 (pack aggregation) | duostore-backend.md §15 | ✅ Done (2026-07-26) |
| DuoStore P3 (GC phase 1) | duostore-backend.md §15 | ✅ Done (2026-07-26) |
| DuoStore P4 (GC phase 2: compaction/orphans) | duostore-backend.md §15 | ✅ Done (2026-07-29) |
| DuoStore P5 (polish/metrics/e2e_tiered_duostore) | duostore-backend.md §15 | ✅ Done (2026-07-29) |
| Redis meta R4 (polish) | duostore-redis-meta.md §10 | ✅ Done (2026-07-30) |
| SQLite meta S4 (polish) | duostore-sqlite-meta.md §10 | ✅ Done (2026-07-30) |
| RADOS data C3 (aio bridging) / C4 (orphans + multi-gateway) | duostore-rados-data.md §12 | ✅ Done (2026-07-30; throughput comparison data deferred to a cluster environment) |
| TiKV meta T5 (polish) | duostore-tikv-meta.md §11 | ✅ Done (2026-07-30, except upstream PR contributions) |
| tiered reconciliation tool (P4 remainder) | tiered-storage.md §9/§10 | ✅ Done (2026-07-31, incl. GC exponential backoff) |
| cloudproxy metrics + control_in_pump (P4 remainder) | cloudproxy-backend.md §8.2/§2.3 | ✅ Done (2026-07-31, incl. vhost) |
| Credential management phase 2 | credential-management.md §9 | ✅ Done (2026-07-31, §3.5) |
| S3 protocol gap closure (UploadPartCopy etc.) | s3-protocol.md / §4 of this doc | ✅ Done (2026-07-31) |

## 1. Mainline: DuoStore P2–P5 (✅ all done, 2026-07-29)

### 1.1 P3 · GC phase 1 (✅ done, 2026-07-26)

All items landed, covering the 4 meta × 2 data combinations (naturally
effective through the IMetaStore/IDataStore interfaces):

- ✅ gcq consumer worker: `run_gc_once()` batch-fetches `peek_reclaims`
  (256/batch) → grace/pin filtering → physical deletion first, then batch
  settlement (`ack_reclaims`, the §9.1 ordering iron rule); `Reclaim` gains
  `enqueue_ms` returning the enqueue time for grace evaluation
- ✅ chunk/rados unlink and whole-pack deletion (`IDataStore::remove_pack`; the
  fs implementation unlinks, the rest default to no-op; the packstat
  settlement interface lands with P2's liveness accounting)
- ✅ pin counting (`duostore::PinTable`; GET readers hold a pin, released on
  destruction) + `gc_grace`
- ✅ `run_gc_once()` test hook + background worker (TimerQueue periodic;
  `gc_interval` 0 disables; the first `src/core/timer` unit tests added
  alongside, closing that §5.3 item)
- ✅ mpu_ttl expired-multipart cleanup (internal abort; parts reclaimed in the
  same round)
- ✅ close() completion: cancel the GC timer, wait for in-flight GC coroutines;
  the dtor falls back to the same path (timer-callback lifetime guarded
  against a dangling this by GcGuard)
- ✅ The rados C2 leftover "GC reclamation / pin race" special tests (SKIP
  without a cluster)
- Acceptance: GC convergence/grace/pin/mpu/worker/close specials + concurrent
  GET vs GC with no ENOENT, all green; 10 consecutive asan runs and 4 tsan runs
  with zero warnings (incidentally fixed the tsan-detected `closed_` data race)

### 1.2 P2 · pack aggregation (✅ done, 2026-07-26)

All items landed, covering 4 meta × fs data (rados has no pack entity,
naturally out of scope):

- ✅ `open_writer()` pack/chunk split: known length > threshold goes straight to
  chunk; the rest buffer in `FsPackedWriter`; at EOF, ≤ threshold goes wholly
  into a pack, over the limit switches to chunk streaming (§5.3 chunked
  buffering; `WriteHint` gains owner for embedding in the record)
- ✅ Multiple active packs with concurrent append (`pack_writers` slots +
  try_lock polling), record format ("LP3R" header + crc32c + owner),
  rotate-and-seal at `pack_max_size`, close() sealing, restart abandonment
  (constructor-time `abandon_stale_packs` closes out the previous generation's
  unsealed accounting)
- ✅ The GET side reads a pack record in full and always verifies the crc (§7)
- ✅ Pack liveness accounting wired into all four meta stores: `IMetaStore`
  gains `seal_pack` / `drop_pack_stat`; rocks = stats CF sub-key merge, redis =
  `pack:<id>` HASH HINCRBY (the script gains an hincr op), sqlite = pack_stats
  table upsert, tikv = **delta rows + folding inside pack_stats()** (the
  materialization solution forewarned in §3.2; business writes stay pure
  writes with no conflicts); complete's ref transfer does not touch pack
  accounting (prevents double counting)
- ✅ `drop_pack_stat` settlement after GC deletes an empty pack wholesale (the
  path pre-laid in P3, now wired up)
- ✅ Tests: `duostore_backend_suite_all_pack` (force all-pack + a small
  pack_max_size for high-frequency rotation) green in the same suite alongside
  the default/multi-chunk variants; meta_store_suite gains two pack-accounting
  cases (shared by rocks/redis/sqlite/tikv); specials cover the split/chunked
  buffering/rotation sealing/record format on disk/crc bit rot/empty-pack
  wholesale delete vs pin/restart abandonment
- Acceptance: unit tests green in all five builds default/redis/sqlite/rados/
  tikv (real redis server, real tikv tiup playground cluster; rados always
  SKIPs without a cluster); e2e_duostore × {rocksdb,redis,sqlite,tikv} all
  green; 3 consecutive asan runs and 2 tsan runs with zero warnings
  (incidentally fixed the compile leftovers in three concrete-class calls in
  test_duostore_tikv after P3 changed the `peek_reclaims` signature)

### 1.3 P4 · GC phase 2 (✅ done, 2026-07-29)

All items landed:

- ✅ Pack compaction: `FsDataStore::rewrite_pack()` sequential scan (record
  parsing; graded corruption semantics — a bad magic/header alerts and stops
  the scan, a bad payload crc alerts and skips, a torn tail silently stops the
  scan without counting as corruption) + the `PackMigrateFn` migration callback
  (standard implementation `migrate_pack_record`: owner reverse-lookup of
  liveness → payload appended back to the active pack → `swap_extents`
  optimistic ref swap, with chunk-residue cleanup on swap failure;
  misjudgments always conservatively skip migration, and pack deletion goes
  only through "live count reaches zero + empty-pack wholesale delete" — never
  losing data)
- ✅ Compaction scheduling folded into `run_gc_once()`: `pack_gc_ratio`
  liveness-ratio threshold selection + backfilling file_size for
  crash-leftover seal(0); `compact_blocked_` bookkeeping + a gc_grace cooldown
  window against futile rescans (in-progress mpu parts / old-format owners /
  live corrupted records; retried immediately once the accounting advances);
  mpu owner gains b/k ("mpu\0b\0k\0id\0no", so parts remain
  reverse-lookupable after complete; the pre-P4 old format is conservatively
  shelved)
- ✅ Empty-pack wholesale delete switched to deferred unlink (deleted only when
  empty beyond gc_grace with no pin, serving readers that hold an old ref at
  the instant of compaction/deletion but have not yet pinned)
- ✅ Orphan scanning: interface finalized as `IDataStore::scan_chunks(cb)` (the
  fs implementation does a sequential directory scan; rados explicitly throws,
  left to C4 — never silently scanning empty and lying) +
  `IMetaStore::scan_refs(cb)` (implemented by all 4 metas, weakly-consistent
  snapshot); forward = unreferenced + mtime beyond grace + no pin +
  `chunk_referenced` point-in-time re-check → unlink; reverse = ref present but
  file missing → alert and count, never delete meta (mutually exclusive with
  GC via the same semaphore to preserve the "file exists before the ref"
  argument); an independent low-frequency timer `orphan_scan_interval`
  (default 1d, 0 = off) + the `run_orphan_scan_once()` manual hook
- ✅ Write-side pin (`ChunkPinHooks`): ChunkWriter pins as soon as a file_id is
  allocated; released symmetrically by the caller after the meta commit /
  fallback deletion (`WritePinRelease`) — with slow streaming PUTs the early
  chunks' mtime can be far beyond grace, so the mtime grace alone is
  insufficient
- ✅ Crash-injection tests: mini_test gains a child-process mode
  (execv /proc/self/exe + SIGKILL), 4 specials — full recovery after a
  post-commit crash / no garbage from the orphan scan after a mid-PUT crash /
  GC converges after a post-delete crash / random kill -9 preserves all
  reported commits
- ✅ Metrics: 3 compaction items (packs_compacted/records_migrated/
  pack_corrupt_records) + 3 orphan items (scans/chunks_removed/refs_missing
  gauge)
- Acceptance: unit tests green in six builds default/redis/rados/tikv/asan/
  tsan (real redis server, real tikv tiup playground cluster with 9 cases,
  rados always SKIPs without a cluster); 3 consecutive asan runs and 2 tsan
  runs with zero warnings; e2e × {rocksdb,redis,sqlite,tikv} 49 cases each,
  all green

### 1.4 P5 · polish (✅ done, 2026-07-29)

All items landed:

- ✅ RocksDB tuning exposed: the three keys `rocksdb_write_buffer` /
  `rocksdb_max_write_buffers` / `rocksdb_max_background_jobs` (defaults =
  RocksDB's own defaults, so existing deployments behave unchanged;
  compression stays permanently off per §13.3, not exposed); from_params range
  validation (≥1) + the engine-ownership WARN table + the lights3.yaml sample
  and the §11 config table synced
- ✅ Corruption counter metric: `lights3_duostore_read_corruption_total` — GET
  read-path crc mismatches increment through the `on_corruption` callback,
  wired at the three sites fs chunk / fs pack / rados; the callback captures
  only the counter shared_ptr, so a reader (holding an options copy) escaping
  the backend's lifetime remains safe. GC sequential-scan corruption counting
  (pack_corrupt_records_total) already landed with P4
- ✅ `e2e_tiered_duostore`: run_e2e.sh gains a tiered-duostore scenario
  (localfs local + duostore cloud + tiered composition), registered in CMake;
  re-proves §13.1 "duostore can serve as the tiered cloud side" (the cloud
  goes only through the IStorageBackend abstraction)
- ✅ Doc status headers updated: duostore-backend.md status header (P1-P5 all
  done) + §11 config table + §15 table; storage-backend.md §5 drops the
  "P2-P4 planned" wording
- Acceptance: default ctest matrix 12/12 green (incl. the new
  e2e_tiered_duostore); unit tests green in six builds default/redis/rados/
  tikv/asan/tsan (2 new cases: corruption metric + tuning parsing; real tikv
  tiup playground cluster, rados always SKIPs without a cluster); 3
  consecutive asan runs and 2 tsan runs with zero warnings; e2e ×
  {rocksdb,redis,sqlite,tikv} 49 cases each, all green

## 2. Polish Tails of the Pluggable Engines

### 2.1 Redis meta · R4 (duostore-redis-meta.md §10) (✅ done, 2026-07-30)

- ✅ AOF detection warning: already present since the R1 constructor
  (`CONFIG GET appendonly` non-AOF logs a WARN, with a degradation hint when
  CONFIG is unavailable); verified this round, no change needed
- ✅ `redis_wait_replicas` (default 0, range [0,256]): when >0, after a
  commit-class command succeeds, append `WAIT <n> <timeout/2>` on the same
  connection; insufficient replicas only WARNs, no error (the write already
  took effect on the primary; erroring would mislead clients into retrying) —
  the §6 semantics note synced
- ✅ Metrics: `redis_cas_retries_total` / `redis_reconnects_total` wired via
  MetricsScope (RedisMetaOptions gains a metrics field; registered at
  construction so 0 values are visible)
- ✅ `list_uploads` HGETALL → HSCAN batching (COUNT 512; cursor weak
  consistency acceptable; map dedup + sort); the TLS evaluation conclusion
  recorded in §5.5 (stays disabled, the enable path is documented)
- ✅ Docs: status header R1-R4 done, the §10 table R4 row, the §8 config
  table/sample, the lights3.yaml sample (incidentally fixed the sample typo
  `redis_uri: tcp://` which should be `redis://`). The R1/R2/R3 states in the
  §10 table had already been corrected to "done" earlier; the previously
  recorded contradiction no longer exists
- Acceptance: redis build, 181 cases all green (4 new specials:
  wait_replicas tolerance/config validation, CLIENT KILL reconnect counting +
  the commit-class InternalError boundary, HSCAN cross-batch completeness);
  default build all green; e2e duostore-redis all green

### 2.2 SQLite meta · S4 (duostore-sqlite-meta.md §10) (✅ done, 2026-07-30)

- ✅ Crash-simulation special: a child process (execv self, sync=true) reports
  back per commit, the parent randomly SIGKILLs; after restart every reported
  commit must exist, refs↔objects bidirectional reconciliation converges, no
  phantom gcq entries, sequence segments never regress, integrity_check clean
  (the process-level harness reuses mini_test ChildRegistrar)
- ✅ list consistent-view injection: the `set_list_pause_for_test` hook with
  concurrent commits midway (insert/delete/overwrite); the current list's WAL
  snapshot is untouched, the next list sees the new state
- ✅ `sqlite3_backup` online-backup evaluation: stays unimplemented — under
  WAL, an external read-only connection (sqlite3 CLI `VACUUM INTO`/`.backup`)
  can already take a consistent online snapshot; conclusion recorded in §6.3
- ✅ `PRAGMA optimize` / checkpoint tuning: no extra automatic policy; added
  `journal_size_limit=4MiB` to truncate the WAL high-water mark; optimize
  stays as a close()-only final run (conclusion recorded in §6)
- ✅ Metrics: `sqlite_busy_total` / `sqlite_corruption_total` wired via
  MetricsScope (SqliteMetaOptions gains metrics / busy_timeout_ms fields;
  registered at construction so 0 values are visible; includes open-path
  NOTADB)
- ✅ Connections left with an open transaction changed to "try a ROLLBACK
  first, return to the pool on success" (previously conservatively
  destroyed); construction-failure cleanup goes through non-graceful shutdown
  (skipping optimize/checkpoint to avoid double-counting corruption)
- Acceptance: sqlite build, 184 cases all green (4 new specials); default
  build all green; e2e_duostore_sqlite all green; the crash special stable
  over 5 consecutive runs

### 2.3 RADOS data · C3 / C4 (duostore-rados-data.md §12) (✅ done, 2026-07-30)

- ✅ C3 aio coroutine bridging: write_full/read/remove all switched to
  `rados_aio_*`; completion callbacks only post the continuation back to the
  pool executor (the §6.2 discipline; `AioPending` CAS rendezvous +
  double-reference abandonment safety — if the writer is destroyed before
  finish, in-flight buffers/semaphore quota stay alive until the callback
  lands); write-side double-buffered pipelining (at most 1 op in flight; the
  second quota acquired non-blocking via `AsyncSemaphore::try_acquire`,
  degrading to C1 serial when unavailable, with no nested-wait deadlock
  shape); windowed concurrent remove (16/batch); close/dtor `rados_aio_flush`
  drains the tail. Object-level read-ahead evaluation conclusion: not
  implemented (~11% boundary bubble vs the new buffer-accounting coupling;
  the quantified argument recorded in §5); throughput comparison data needs a
  real cluster, deferred to cluster e2e
- ✅ C4 orphan scan: `scan_chunks` = nobjects_list (namespace-scoped, foreign
  names ignored) + rados_stat (race-tolerant); incidentally fixed the backend
  orphan unlink hard-coding kChunk, which made rados silently no-op (the kind
  now follows `data_kind`)
- ✅ C4 multi-gateway: `gc_enabled` config (default true; false stops only the
  background scheduling, manual hooks kept); the distributed-pin evaluation
  conclusion recorded in §8.3 — lease-style deferred deletion wins
  (self-healing / no per-object lock RTT); rados_lock and watch/notify
  rejected; not implemented until a real multi-gateway need appears
- ✅ C4 metrics: `lights3_duostore_rados_op_duration_seconds` (histogram with
  op label, submission → completion) /
  `lights3_duostore_rados_op_errors_total` wired via MetricsScope
- ✅ Tests: pipeline roundtrip (incl. serial degradation) / orphan
  forward+reverse+grace+foreign objects / op metrics (SKIP without a
  cluster); `gc_enabled` gating and `try_acquire` always-run cases
- ~~C2 leftover: GC reclamation / pin race special tests (prerequisite:
  mainline P3)~~ ✅ completed with P3 (test_duostore_rados.cc, SKIP without a
  cluster)

### 2.4 TiKV meta · T5 (duostore-tikv-meta.md §11) (✅ done, 2026-07-30)

- ✅ **GC safepoint advancement** (§7.3): the sidecar wraps the three PD RPCs
  (UpdateServiceGCSafePoint / UpdateGCSafePoint / GetGCSafePoint, connecting
  straight to the leader via getLeaderUrl with a self-built stub); the
  TikvMetaStore background worker advances in three steps every
  `tikv_gc_interval` (default 60s, 0=off): register this gateway's service
  safepoint (now−`tikv_gc_retention`) → take over the `gc_worker` role with an
  infinite TTL (PD permanently placeholds a missing gc_worker at 0; unless it
  is pushed, the min stays pinned — empirically confirmed) → advance the
  cluster safepoint to the min across all services; multi-gateway concurrency
  converges via PD's min/monotonic semantics
- ✅ 10k-part complete special: `duostore_tikv_bulk_complete_10k_parts` (10000
  parts, 8-thread residue-class parallel upload, concurrent readers during
  complete driving the TTL evaluation), measured ~0.5s; the sidecar lock_ttl
  scales per the upstream txnLockTTL semantics (ttlFactor·√MiB, [3s,20s]);
  TTLManager heartbeat evaluation conclusion: not wired (transaction sizes
  are bounded; S3's 10k parts is the ceiling), recorded in §6.3. Verified: T4
  never ran this special; added this round
- ✅ Poco log convergence: `PocoSpdlogChannel` bridges the root logger to the
  unified stderr format (source-name prefix kept; information level and up);
  timeout parameterization: `tikv_backoff_ms` covers the sidecar path's
  backoff budget (the upstream Snapshot/Scanner internals are uncontrolled;
  global parameterization remains an upstream item)
- ✅ Metrics: `tikv_txn_conflict_retries_total` /
  `tikv_safepoint_update_failures_total` / `tikv_gc_safepoint_ms` wired via
  MetricsScope (TikvMetaOptions gains a metrics field; registered at
  construction so 0 values are visible)
- ✅ Doc wording verified: the fork descriptions in §7.3/§10.4 were already
  synced to the in-tree sidecar; no residue
- Tail (depends on the upstream process, cannot be closed locally): upstream
  PRs — contributing back the 2PC mutation op extension + a `Snapshot::Get`
  not_found overload + a pointer bump (currently guarded by literal matching
  of the `@78a557e` error messages; bumping the pointer requires re-checking,
  see the note at the end of duostore-tikv-meta.md §11)
- Acceptance: tikv build, 181 cases all green ×3 (3 new specials) + the
  no-cluster SKIP path green; build-tikv ctest 11/11 (incl.
  e2e_duostore_tikv); default build ctest 12/12

## 3. Cross-Cutting Infrastructure (one missing piece blocks many)

### 3.1 Backend-level metrics registration mechanism (✅ done, 2026-07-28)

~~The existing Metrics has only the L2 request dimension and no backend-level
registration (cloudproxy-backend.md §8.2 explicitly notes the framework must be
extended first).~~ The framework has landed (`src/core/metrics.{h,cc}`):

- `MetricsRegistry`: four kinds of instruments — counter / gauge / histogram /
  callback gauge; get-or-create for the same name and labels (idempotent);
  same name with a different type/bucket bounds throws an assembly error;
  Prometheus text rendering (family grouping, # HELP/# TYPE, label escaping,
  stable output order)
- `MetricsScope`: the per-backend registration handle; `StorageRegistry::build`
  hands each backend a scope with the `backend=<name>` base label
  (`BackendFactory` gains a third parameter); `with()` can derive
  sub-instrument dimensions; the default empty scope returns isolated
  instances — backends constructed directly in tests have zero assembly cost
- Assembly: main creates the registry → build injects it →
  `S3Service::set_backend_metrics`; `GET /-/metrics` appends the rendering
  after the L2 request metrics (behavior unchanged if not injected)
- Demonstration consumer: 5 duostore GC counters (runs/reclaims/files_removed/
  packs_removed/uploads_expired — the GC slice of the P5 metric items)
  registered via the scope; registered at construction so 0 values are visible
- Acceptance: test_metrics.cc 8 cases + service/duostore end-to-end
  assertions; unit tests green in five builds default/asan/tsan/rados/redis;
  e2e duostore all green; real-process /-/metrics output verified

It unlocks the following metric items (each scheduled with its owning phase;
integration approach in storage-backend.md §6):

- cloudproxy P4 metrics (remote request counts/latency, retries, error
  mapping, ETag verification failures, ClientPool waits)
- ~~duostore P5 (corruption counter; the GC counters landed with this
  demonstration item)~~ (✅ landed with P5: read_corruption_total), plus the
  respective metric items of redis R4, sqlite S4, rados C4, tikv T5
- The "per-backend dimensions, backend error rate" planned in s3-protocol.md §7

### 3.2 Per-backend dedicated ThreadPool (✅ done, 2026-07-30)

~~Reserved in concurrency.md §3.1 (the Registry constructor-injection
interface is already in place)~~ Landed (`storage/registry.cc`
`backend_pool`):

- Generic config key `io_threads` ([1,1024], effective for any type; defaults
  to the shared global pool) — the Registry injects a dedicated ThreadPool per
  the parameter before calling the factory; factories/backends need zero
  changes and stay unaware; the tiered composite backend is supported likewise
- Lifecycle: the backend holds a shared_ptr, joined on destruction; the
  metrics callback holds another copy, so stats() reads stay safe after join
- Observability: `lights3_backend_pool_{threads,queue_depth,backlogged,completed}`
  gauge callbacks carry the backend label, kept apart from the unlabeled
  global-pool `lights3_pool_*` namespace (avoids duplicate TYPE lines under
  one name)
- Acceptance: registry_per_backend_thread_pool (dedicated-pool read/write
  smoke + metric assertions + illegal-value fail fast); cloudproxy's private
  pump-thread local workaround kept as-is

### 3.3 cloudproxy P4 remainder (✅ done, 2026-07-31)

- ✅ §8.2 metrics: `RemoteMetrics` single-point wrapper for five pieces — a
  request latency histogram (op label; each retry attempt recorded once; data
  plane = the whole transfer) / a retry counter / an error-mapping counter
  (wire code; unparsable falls into `http_<status>`, the network layer into
  `transport`) / ETag mismatches / a ClientPool wait histogram; op/code
  dimensions get-or-create on demand with local caching
- ✅ §2.3 `control_in_pump` config item: the `control_io` helper — false uses
  the shared pool (original behavior), true spins a one-shot private thread to
  run the blocking section (incl. retry backoff), with the continuation
  resumed via the pool executor; the complete_multipart retry loop refactored
  into a thread-runnable pure blocking section. Benchmark (in-process
  loopback, 300 HEADs): pool≈210µs/op, pump≈294µs/op — the ~80µs fixed
  thread-creation cost is a net loss at low RTT, so **default false**; set
  true when remote RTT reaches several ms or the pool-wait histogram shifts
  right (conclusion recorded in §2.3)
- ✅ `force_path_style: false` (virtual-hosted style): single-point `Target`
  addressing (path prefix + Host); TCP/SNI always points at the endpoint, and
  only the Host/signature/path vary per bucket (httplib self-sets Host only
  when absent; the pipeline always carries the signed Host explicitly) —
  avoids per-bucket connection pools; vhost tested with the full suite (a
  remote lights3 configured with base_domain)
- ✅ Known trade-off recorded: create_multipart retries may leave an empty
  orphan upload — recorded in §5.2 (recommending an
  AbortIncompleteMultipartUpload lifecycle rule on the remote), with a
  matching comment at the code site
- Acceptance: 202 cases all green (new vhost suite / control_in_pump suite +
  benchmark / metric assertions / config parsing); full build matrix + e2e
  green

### 3.4 tiered remainder (✅ done, 2026-07-31; except the evolution item)

- ✅ §9 reconciliation tool: `run_reconcile_once` bidirectional diff (manual
  hook + an independent `reconcile_interval` timer, default 1d) — forward
  orphans are by default rebuilt as stubs from the lights3-* redundant
  headers (`reconcile_orphans: delete` optionally deletes; foreign objects
  without the redundant headers alert and skip); three guards against false
  positives: a GC-queue snapshot (prevents resurrecting just-DELETEd
  objects) / the inflight table / an in-lock re-check + point-in-time cloud
  etag verification; stale copies of locally-local objects (lost GC entries)
  are always safe to delete and are deleted in both modes; reverse
  refs_missing re-checks with HEAD before alerting, never deleting the stub
- ✅ GC retry exponential backoff: failed entries persist `attempts`/`retry_at`
  in the TSV (not reset on restart; old entries default to immediately
  eligible, backward compatible); delay = `gc_retry_base`(60s) × 2^n clamped
  to `gc_retry_cap`(1h); entries not yet due incur zero cloud access;
  `run_gc_once` returns TierGcStats for assertions
- Acceptance: backoff (fail → doubling → recovery reclamation) /
  reconciliation (rebuild/delete modes/anti-resurrection/reverse
  alert/config validation) specials, 5 cases + the full 207-case run green;
  consecutive asan/tsan runs with zero warnings
- Evolution item (a separate feature, not this round): abstract the
  local-side interface so duostore can serve as the local side (currently
  bound to the localfs disk layout, duostore-backend.md §13.1)

### 3.5 Credential management phase 2 (credential-management.md §9) (✅ done, 2026-07-31)

~~- SK at-rest encryption (master key from an environment variable, AES-256-GCM; the `version` field already reserves the upgrade path)~~
~~- External IdP / file hot-reload provider~~
~~- Multi-instance invalidation sync (periodic incremental reload or control-plane broadcast, §7)~~
~~- per-credential policy~~
All four landed (design in credential-management.md §10): `LIGHTS3_MASTER_KEY`
triggers v2 encrypted persistence and in-place upgrade of existing v1 objects
(missing/wrong key fails fast); the `auth.credentials_file` JSON file
provider (mtime-polling hot reload, data plane only); `auth.sync_interval`
periodic incremental `.sys` reload (snapshot taken before list to prevent
deletion races); per-credential policy (bucket glob allowlist + readonly,
carried in the POST body / file entries; unified authorize in dispatch). 8 new
unit tests + 12 new e2e checks all green.

## 4. S3 Protocol-Layer Gaps (s3-protocol.md) (✅ done, 2026-07-31)

All actionable items handled; versioning (incl. CopyObject ?versionId,
ListObjectVersions), ACL/policy, website, lifecycle, tagging, CORS, SSE-C/KMS,
Object Lock, replication, notification, etc. remain **explicitly unsupported by
design** (centralized rejection table in `src/s3/service.cc`, 27 subresources,
staying NotImplemented):

- ~~**UploadPartCopy**~~ implemented (`multipart.cc`): x-amz-copy-source-if-*
  conditional headers, x-amz-copy-source-range (strict bytes=first-last
  parsing, out-of-range InvalidArgument), cross-backend sources,
  CopyPartResult XML; incidentally fixed a pre-existing security hole —
  copy-source via header bypassed dispatch's `.`-prefix interception,
  allowing reads of `.sys` credential objects, and policy credentials could
  use copy-source to read buckets outside their allowlist; both sealed at the
  parse/authorize sites with tests pinning them
- ~~PUT `If-None-Match` supports only `*`~~ Verification conclusion:
  consistent with AWS (official conditional writes likewise support only
  `*`; with an ETag it is also 501); an assertion test added to pin it; not a
  gap
- ~~Multi-range Range unsupported~~ Verification conclusion: AWS does not
  support multi-range either; behavior aligned (ignore the whole Range
  header, return 200 with the full body); an assertion test added to pin it;
  not a gap
- ~~presigned lacked the 15-min clock-skew check~~ Added: X-Amz-Date more than
  15 min in the future → AccessDenied "Request is not valid yet" (the past
  side remains constrained by X-Amz-Expires)
- ~~mint compatibility-set verification/scheduling~~ Landed as
  `tests/e2e/run_mint.sh` (SKIPs when the docker probe fails, same pattern as
  rados/tikv); the local docker socket lacks permission so it cannot run
  here; classified as a manual CI gate; recommend starting with the
  s3cmd/awscli subset (see s3-protocol.md §8)

## 5. Engineering and Test Gaps

### 5.1 Config sample severely outdated (✅ done, 2026-07-26)

~~`config/lights3.yaml` was just 26 lines demonstrating only localfs. duostore
(meta/data and per-engine parameters), cloudproxy, and tiered were entirely
missing; users could only reverse-engineer the syntax from
`tests/e2e/run_e2e.sh`.~~ A fully commented all-backend sample has been added:
full parameters for duostore's four metas (rocksdb/redis/sqlite/tikv) and two
datas (fs/rados), cloudproxy, tiered, and buckets.rules, with defaults and
value ranges given in comments; the sample was verified to parse and boot
(both forms: the localfs default, and all backends uncommented).

### 5.2 build.sh switches out of sync (✅ done, 2026-07-26)

~~CMake has 10 `LIGHTS3_*` options; build.sh exposed only `--seastar` and
`--tikv`. Missing `--redis` / `--sqlite` / `--rados`; no hint that rados needs
the system librados either.~~ The three switches `--redis` / `--sqlite` /
`--rados` have been added (sticky semantics like `--seastar`); usage notes
that rados needs the system librados (librados-dev or `LIGHTS3_RADOS_ROOT`)
and recommends `-B build-rados` for isolation; `--rados` was verified to
build in the build-rados directory.

### 5.3 Test coverage gaps

- **3 unverified combinations**: redis×rados, sqlite×rados, tikv×rados (the
  config is writable and construction runs, but zero unit tests and zero
  e2e; the duostore-rados scenario in `run_e2e.sh` always uses the default
  rocksdb meta)
- Zero-unit-test modules: ~~`src/core/timer`~~ (✅ closed with P3,
  test_timer.cc), `src/storage/bucket_router`, `src/storage/listing`,
  `src/storage/validate.cc`, `src/s3/handlers/admin_credentials.cc`,
  `src/http/pushpull.h`, `src/storage/xlocalfs/uring`
- ~~`pack_stats()` not in the shared suite~~ (✅ entered meta_store_suite with
  P2); ~~`rewrite_pack()` still not~~ (✅ closed with P4: scan_refs entered
  meta_store_suite; the rewrite_pack/compaction/orphan/crash-injection
  specials entered test_duostore.cc)
- Environment-dependent cases: 8 rados + 9 tikv always SKIP in a bare
  environment, and 8 redis depend on redis-server — about 17% of cases do not
  run by default; CI coverage requires a dedicated environment
- tiered implicit coupling: tiered goes through the two-phase build and is
  not in the registry map, so the "unknown type" check does not apply to it
  (`src/storage/registry.cc:93-125`); could be hardened in passing

### 5.4 Other technical debt noted in code (listed as "noted, won't do" — do not mistake for bugs)

- RocksDB/SQLite meta: fsync inside the lock, capping write throughput at
  ≈ 1/fsync latency; the upgrade paths are TransactionDB / group commit
  respectively, both "noted only, not done" (`rocks_meta_store.h:96`,
  `sqlite_meta_store.h:117`)
- Redis Cluster unsupported (`redis_meta_store.h:5`, an explicit non-goal)
- Simplified YAML parser: no tab indentation/flow style/anchors/multi-line
  scalars; `" #"` inside quotes parses incorrectly (`src/core/config.cc:59,61`)
  — upgrade if it ever bites
- The codec `reason` field is reserved; overwrite/delete/abort are not yet
  distinguished (`src/storage/duostore/codec.cc:406`)
- cloudproxy does no TRAILER framing for uploads without a content-length,
  always NotImplemented (`cloudproxy_backend.cc:404-407`)

## 6. Documentation Consistency Fixes (✅ all done, 2026-07-26)

1. ✅ The 5 "not implemented" items at the end of `docs/README.zh-CN.md`
   (Multipart, CopyObject, DeleteObjects, cloudproxy, aws-chunked) were in
   fact implemented — replaced with the real gaps (UploadPartCopy/versioning
   etc.); the drivers (adding seastar), auth (adding aws-chunked/credential
   management), storage and S3 API lists completed
2. ✅ The `docs/README.md` index now includes the 4 duostore sub-documents plus
   pointers to todo.md and README.zh-CN.md; the "(draft)" tag removed from
   tiered/duostore; "two backends in the first phase" and the architecture
   diagram (CivetWeb etc.) updated
3. ✅ `docs/duostore-redis-meta.md` §10 table: R1-R3 states changed to "done"
4. ✅ `docs/duostore-tikv-meta.md` §7.3 / §10.4 fork wording changed to the
   in-tree sidecar
5. ✅ `docs/architecture.md`: the §1 goals table adds "duostore optional
   external meta/data services"; the non-goals distinguish gateway
   single-instance from external-system scaling; the §6 directory tree adds
   duostore; CMake switches add `LIGHTS3_DUOSTORE*`
6. ✅ `docs/storage-backend.md` §5 drops "(draft)", adds P1/P2-P4 status and
   links to the 4 sub-documents
7. ✅ The `docs/concurrency.md` §3.1 per-backend pool reservation backfilled
   to point at §3.2 of this list
8. ✅ (In passing) the root `README.md`: the storage list adds
   CloudProxy/Tiered/DuoStore; "Not implemented" drops the
   already-implemented cloudproxy and adds UploadPartCopy

## 7. Suggested Order of Work

1. ~~**Documentation consistency fixes (§6) + config sample (§5.1) + build.sh
   switches (§5.2)** — half-day scale, clear these to zero first~~ ✅ all done
   (2026-07-26)
2. ~~**DuoStore P3 GC phase 1 (§1.1)** — a hard blocker for production
   readiness; add the `src/core/timer` unit tests first~~ ✅ done (2026-07-26)
3. ~~**DuoStore P2 pack aggregation (§1.2)** — unlocks pack accounting in the
   four meta stores and the all-pack test variants~~ ✅ done (2026-07-26)
4. ~~**Backend-level metrics framework (§3.1)** — unlocks six metric sites at
   once~~ ✅ done (2026-07-28)
5. ~~**DuoStore P4 (§1.3)**~~ ✅ done (2026-07-29; the enumeration interfaces
   are finalized) → the **rados C4 orphan scan**
   (`RadosDataStore::scan_chunks` implementation) can be scheduled with C3/C4
6. Per-engine polish (~~R4~~ ✅ 2026-07-30 / S4 / C3 / T5) plus tiered
   reconciliation and credential phase 2 as needed (~~P5~~ ✅ done, 2026-07-29)
