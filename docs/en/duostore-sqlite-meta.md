# SqliteMetaStore: SQLite-Based DuoStore Metadata Store

> English translation of [../duostore-sqlite-meta.md](../duostore-sqlite-meta.md). The Chinese original is authoritative; section numbering matches.

> Status: S1-S4 all complete (2026-07-30; full `SqliteMetaStore` interface + meta/backend
> suites + e2e + S4 polish — crash-replay/consistent-view specials, BUSY/corruption
> metrics, checkpoint tuning and online-backup evaluation; code in
> `src/storage/duostore/sqlite_meta_store.{h,cc}`, build option
> `LIGHTS3_DUOSTORE_SQLITE_META` default OFF). Delivers the evolution promise of
> [duostore-backend.md](duostore-backend.md)
> §12 "SQLite (single-file deployment)": switch the meta side to SQLite, implement
> `IMetaStore` (`src/storage/duostore/meta_store.h`), and converge metadata into a
> **single database file** — backup/migration = copying one file. Engine source is the
> `third_party/sqlite` submodule (https://github.com/sqlite/sqlite.git,
> §7). In this document "main doc" refers to duostore-backend.md, "the Redis doc"
> refers to [duostore-redis-meta.md](duostore-redis-meta.md), and a bare `§N` refers
> to sections of this document.

## 1. Goals and Non-Goals

| Goal | Description |
| --- | --- |
| Implement the full `IMetaStore` interface | The whole set of bucket / object / list / multipart / GC accounting; behavior semantically equivalent to `RocksMetaStore` — the same meta store test suite passes all green (§9) |
| Single-file deployment | Metadata = one `.sqlite3` file (+ runtime WAL/SHM companion files), no directory tree, no manifest collection; cold backup = copy the single file after a clean close (§6.3) |
| Zero new codecs | Value encoding 100% reuses `codec.cc` (§2.1); the three implementations (RocksDB / Redis / SQLite) are byte-identical on disk format, roundtrip tests are shared |
| The implementation with the strongest transactional expressiveness | Compound invariants land directly in SQLite native transactions: read-validate-write in one transaction with zero window, no CAS retries as in the Redis version, no parts sha1 fingerprint (§3.4) — yet another payoff of the semantic-level interface chosen in main doc §2.1 |
| Zero external test dependencies | No external server required (contrast: the Redis version must probe for redis-server); unit tests/e2e are self-contained, no SKIP path |

Non-goals (explicitly declared):

- **Multi-process shared meta is not supported.** SQLite file locking itself allows
  multiple processes, but the data plane's pin table / GC have single-process
  semantics (main doc §7, §12), and locks on NFS and other network filesystems are
  unreliable — same premise as the RocksDB version: single-process exclusive. And
  with **fail-fast enforcement** on par with RocksDB's LOCK file: at construction,
  take `flock(LOCK_EX|LOCK_NB)` on `<path>.lock`; a second process (or a second
  instance in the same process) is refused at startup outright, never a silent
  double open (`PRAGMA locking_mode=EXCLUSIVE` is unusable — connection-level
  locking would mutually exclude our own connection pool). The right road for
  multi-gateway shared meta is the Redis version;
- No SQL for business queries: SQLite here is merely "an ordered KV with
  transactions plus a few counters"; no relational modeling (a corollary of
  §2.1 principle 1);
- No FTS / json1 / rtree etc.; no runtime extension loading (compile-time
  `SQLITE_OMIT_LOAD_EXTENSION`, §5.1);
- No sharding/partitioning; the in-process online backup API (`sqlite3_backup`)
  remains not-done after the S4 evaluation — in WAL mode an external tool can take
  a consistent snapshot online (evaluation conclusion in §6.3).

## 2. Data Model

### 2.1 General Principles

1. **Value encoding 100% reuses `codec.cc`** (`encode_object / encode_upload /
   encode_part / encode_reclaim / encode_bucket`), stored in BLOB columns. Benefits
   are the same argument as the Redis version: zero new codecs; the three
   implementations are byte-identical on disk format, the codec roundtrip special
   tests are directly shared; logic like GC compaction ref-swapping and version
   bumping does not fork per storage engine.

   Rejected alternative: full relational modeling (splitting
   size/etag/mtime/user_meta/extents into columns and tables) — SQL queryability of
   single fields is the only benefit, but `IMetaStore` has no interface requirement
   to query by field; once the nested extent array is relationalized,
   complete_upload's assembly requires writing row-shuffling code; it amounts to
   rewriting the codec in SQL DDL and maintaining two copies of the format code.
   This is the SQLite form of the same iron law as the Redis version's "Lua never
   parses values";
2. **Key columns are always bound as BLOB** (`sqlite3_bind_blob`, never bind_text).
   SQLite's BLOB comparison is memcmp = S3 lexicographic order, and it sidesteps two
   pitfalls: storing arbitrary byte sequences as TEXT risks UTF-8 assumptions;
   SQLite's type ordering is TEXT < BLOB, so mixing both types in one column would
   break ordering. All tables are `STRICT` with column type `BLOB` enforced by the
   engine — a wrong bind errors out immediately instead of silently mixing types;
3. **Primary key is the index: WITHOUT ROWID clustered tables.** `objects` is a
   WITHOUT ROWID table with `(bucket, key)` as primary key; the B-tree stores rows
   in primary-key order, and a `key > ?` range scan is the ordered iteration
   primitive for list (§2.3), isomorphic to the key layout of the RocksDB objects
   CF. Note: very large manifests (an ObjectVal for a ten-thousand-part multipart
   can reach hundreds of KiB) put rows onto overflow pages — but list must read val
   to decode meta anyway, so clustering actually saves a lookup; a plain rowid
   table + secondary index has no advantage.

### 2.2 Table Layout

Row-by-row correspondence with the CF table in main doc §4.1:

```sql
-- Create tables on open if user_version=0; all STRICT (§2.1 principle 2)
CREATE TABLE buckets(
  name  BLOB PRIMARY KEY,          -- <bucket>
  val   BLOB NOT NULL              -- encode_bucket
) WITHOUT ROWID, STRICT;

CREATE TABLE objects(
  bucket BLOB NOT NULL, key BLOB NOT NULL,
  val    BLOB NOT NULL,            -- encode_object (incl. version / extent runs)
  PRIMARY KEY(bucket, key)
) WITHOUT ROWID, STRICT;

CREATE TABLE uploads(
  bucket BLOB NOT NULL, key BLOB NOT NULL, id BLOB NOT NULL,
  val    BLOB NOT NULL,            -- encode_upload
  PRIMARY KEY(bucket, key, id)
) WITHOUT ROWID, STRICT;

CREATE TABLE parts(
  bucket BLOB NOT NULL, key BLOB NOT NULL, id BLOB NOT NULL,
  part_no INTEGER NOT NULL,
  val    BLOB NOT NULL,            -- encode_part
  PRIMARY KEY(bucket, key, id, part_no)
) WITHOUT ROWID, STRICT;

CREATE TABLE refs(
  file_id INTEGER PRIMARY KEY,     -- chunk reference table; owner for debugging
  owner   BLOB NOT NULL
) STRICT;

CREATE TABLE gcq(
  seq INTEGER PRIMARY KEY AUTOINCREMENT,  -- allocated within the business txn, see gcq row below
  val BLOB NOT NULL                       -- encode_reclaim
) STRICT;

CREATE TABLE counters(
  name BLOB PRIMARY KEY,           -- 'chunk' / 'pack' (gcq seq does not use a counter, see below)
  val  INTEGER NOT NULL
) WITHOUT ROWID, STRICT;

CREATE TABLE pack_stats(
  pack_id    INTEGER PRIMARY KEY,  -- native numeric columns: SQL arithmetic UPDATE = incremental accounting
  file_size  INTEGER NOT NULL DEFAULT 0,
  live_bytes INTEGER NOT NULL DEFAULT 0,
  live_recs  INTEGER NOT NULL DEFAULT 0,
  sealed     INTEGER NOT NULL DEFAULT 0
) STRICT;
```

| RocksDB CF | SQLite counterpart | Differences |
| --- | --- | --- |
| `default` (schema/instance) | `PRAGMA application_id` = `0x4C335351` ("L3SQ") + `PRAGMA user_version` = 1 | SQLite's built-in file-lineage mechanism, zero-table implementation; on open, application_id mismatch → refuse (wrong file grabbed), user_version ahead of us → refuse (newer format). **When app_id=0 and ver=0 (the norm for wild SQLite databases), additionally check `sqlite_master`: any table means it is someone else's database — refuse** — validation precedes any write (including the WAL journal conversion), so foreign files are left untouched; only a genuinely empty database may have tables created and be stamped. Dedicated meta table rejected: redundant |
| `buckets` / `objects` / `uploads` / `parts` | Same-named tables | RocksDB joins composite keys with `\0`; SQLite uses multi-column composite primary keys directly, so the `\0` separator issue disappears entirely (the premise that bucket/key contain no NUL is still guaranteed by the shared validation layer, but this implementation no longer relies on it for key encoding) |
| `<be16 part_no>` suffix of `parts` | `part_no INTEGER` column | Numeric columns are naturally ascending; no big-endian trick needed |
| `<be64>` keys of `refs` / `gcq` | `INTEGER PRIMARY KEY` | Same as above; `INTEGER PRIMARY KEY` is the rowid, the B-tree is numerically ordered. **gcq's seq uses `AUTOINCREMENT` instead of the segment counter**: allocated within the business transaction, committed/rolled back with the same batch — a rolled-back transaction produces no off-book seq, and it avoids the single-writer exclusion of "cannot touch the segment connection while a write transaction is open" (§4); seq is a 64-bit integer, without the Redis version's 2^53 constraint |
| `stats` (segment counters) | `counters` table (chunk / pack only) | `UPDATE … SET val = val + ? … RETURNING val` completes a reservation in one statement (§4) |
| `stats` (pack liveness accounting, merge operator) | Native `pack_stats` columns | `INSERT … ON CONFLICT DO UPDATE SET live_bytes = live_bytes + ?` is the incremental accounting, batched with business writes inside the transaction; enabled along with pack aggregation (main doc P2): `kPackDelta` incremental upsert, `seal_pack` idempotent sealing (file_size=0 does not overwrite a known value), `drop_pack_stat` clears the account after an empty pack is wholly deleted |

The codec reuse scope is the same as the Redis version: take all value codecs +
crc32c; do **not** take the CF key constructors (`be64_key`, the `part_key` suffix,
`\0` joining) — on the SQLite side keys are columns, not an encoding problem.

### 2.3 list_objects: Primary-Key Range Scan + Single Read Transaction

The algorithm is copied from the RocksDB version (main doc §4.4): seek start =
`max(prefix, successor of start_after)`; on a delimiter hit, group and construct
the successor seek point by +1 on the group's last byte to skip the whole group;
fetch one extra row to decide `is_truncated`. The iteration primitive becomes:

```sql
SELECT key, val FROM objects
 WHERE bucket = ?1 AND key >= ?2 AND (?3 IS NULL OR key < ?3)  -- ?3 = prefix upper bound
 ORDER BY key LIMIT ?4;
```

- Each re-seek (delimiter group skip) = re-executing the same prepared statement
  with a new `?2` — one B-tree positioning, purely in-process cost. The Redis
  version's motivation of stuffing the loop into a Lua script to save RTTs does not
  exist here; **the loop stays in C++**, matching the code shape of the RocksDB
  version;
- **Consistent view**: the whole list loop is wrapped in one read transaction
  (`BEGIN DEFERRED` … `COMMIT`). In WAL mode a read transaction holds the snapshot
  of the instant it opened, and does not block writers (§3.1) — semantics aligned
  with the RocksDB pinned snapshot and the Redis single-script consistent view;
- When a delimiter group closes the page exactly at capacity, `next_token` must
  land on the group's tail:
  `SELECT key FROM objects WHERE bucket=?1 AND key<?2 ORDER BY key DESC
  LIMIT 1` (corresponding to RocksDB `SeekForPrev` / Redis `ZREVRANGEBYLEX`);
- Decoding is the same as in the other two implementations:
  `codec::decode_object_meta` skips extent runs without materializing them.

`list_buckets` / `list_uploads` / `list_parts` are each a single
`ORDER BY` query (uploads by `key, id`, parts by numeric `part_no` order);
ordering is provided for free by the primary-key B-tree, no client-side sorting.

## 3. Transactions and Concurrency

### 3.1 Connection Model: Read Pool + Single Write Connection

SQLite allows only one write transaction per database at a time; in WAL mode reads
and writes do not block each other. Accordingly:

| Route | Assessment |
| --- | --- |
| Single connection + mutex | Simplest, but even pure reads queue up — a long list scan would stall commits, abandoning the RocksDB version's existing property "pure reads go through a snapshot without locking"; out |
| thread_local connection per thread | `close()` timing and thread-exit cleanup are hard to manage (same argument as the Redis doc §5.2), and the write connection still must be globally unique; out |
| **Read connection pool + dedicated write connection (decided)** | Write side: one connection + one `std::mutex`, exactly matching RocksMetaStore's `mu_` in shape (lock = transaction boundary); read side: a small connection pool (default ≈ thread pool size), RAII checkout/return, running in parallel with writes under WAL |

All connections open the same DB file; compile-time `SQLITE_THREADSAFE=1`
(serialized, the default) — our checkout protocol already guarantees each
connection is used by a single thread at a time, so serialized mutexes are defense
in depth; the performance difference is negligible in this scenario, traded for
absolute safety (§5.1).

### 3.2 Write Transaction Shape: RAII Guard

Each commit-class method = lock `mu_` → one transaction on the write connection:

```text
BEGIN IMMEDIATE            -- take the write lock now; serialized in-process by mu_, never BUSY
  read old values / preconditions   -- bucket existence, old records, parts set...
  (validation failure → ROLLBACK → throw the corresponding S3Error)
  all writes: INSERT/UPDATE/DELETE (object + refs add/del + gcq entries + pack accounting)
COMMIT                     -- commit point; synchronous level, see §6
```

The C++ side provides a `Txn` RAII guard: constructor runs `BEGIN IMMEDIATE`,
`commit()` commits explicitly, and **the destructor issues ROLLBACK if not
committed** — when a semantic error throws an `S3Error` out of a method, the
transaction rolls back automatically, eliminating half-done residual state. This
is the SQLite version's stand-in for WriteBatch / RedisBatch, with stronger
expressiveness: WriteBatch can only "read first, then accumulate the batch",
whereas here reads and writes interleave freely in the same isolation domain.

### 3.3 Per-Method Flows

Against the same-batch content table in main doc §4.5 (segment/pure-read methods
omitted; each row = one §3.2 transaction):

| Operation | In-txn validation | In-txn writes |
| --- | --- | --- |
| put_object | bucket exists; `SELECT val` reads old object (compute version and gcq accounting) | `INSERT OR REPLACE objects` new ObjectVal (version = old +1) + `INSERT refs` for new chunks + old DataRef into `gcq` + `DELETE` old refs + negative pack-accounting delta |
| delete_object | `SELECT val` reads old object (no row → ROLLBACK directly and return false, idempotent) | `DELETE objects` + old DataRef into `gcq` + `DELETE refs` + negative pack-accounting delta |
| create_upload | bucket exists | `INSERT uploads` (id generated by `storage/multipart.h::new_upload_id`) |
| put_part | upload exists; `SELECT` old same-number part | `INSERT OR REPLACE parts` + new refs + old part into `gcq` + delete old refs (same-number re-upload is last-write-wins) |
| complete_upload | upload exists; in-txn `SELECT … ORDER BY part_no` reads all parts, per-item ETag comparison / `validate_part_order` / `combined_etag` (reusing `storage/multipart.h`); read old same-name object | `INSERT OR REPLACE objects` + `DELETE uploads` + `DELETE parts WHERE …` (whole prefix in one statement, corresponding to RocksDB range delete) + unselected parts into `gcq` + old same-name object into `gcq` + refs transfer + pack accounting |
| abort_upload | upload exists | `DELETE uploads` + `DELETE parts` + all parts into `gcq` + delete refs |
| delete_bucket | bucket exists; `EXISTS objects` / `EXISTS uploads` emptiness checks (covers in-flight multipart, aligned with AWS, same argument as the other two implementations) | `DELETE buckets` |
| swap_extents | `SELECT val` decode, compare expect_version and from extents (mismatch → ROLLBACK and return false) | `UPDATE objects` new ObjectVal (version+1, DataRef=to) + pack-accounting migration |
| ack_reclaim / ack_reclaims | — | `DELETE gcq WHERE seq=?` (refs were already deleted in the same batch of the business transaction; called after the physical unlink, main doc §9.1 ordering iron law unchanged). Note: in this implementation a per-item ack = one standalone commit inside mu_ (incl. fsync when sync=true), competing with business writes for the same write lock — **the GC consumer should use the interface's batched `ack_reclaims`** (this implementation overrides it as one transaction, one fsync; the RocksDB version overrides it as one WriteBatch; the interface default forwards item-by-item; a lost ack is harmless, so the batched semantics are safe) |

- `create_bucket` = in-txn `SELECT` existence check + `INSERT buckets` — `mu_`
  already serializes writers; the explicit check corresponds line-for-line with the
  RocksDB version, not relying on the implicit "constraint violation to error code"
  path;
- Pure reads (bucket_exists / get_object / require_upload / list_* /
  peek_reclaims / chunk_referenced / pack_stats) use pool connections; a single
  statement is consistent by itself, and multi-statement list uses the §2.3 read
  transaction. `peek_reclaims` =
  `SELECT seq, val FROM gcq ORDER BY seq LIMIT ?`; `chunk_referenced` =
  `EXISTS refs`.

### 3.4 Atomicity Across the Three Implementations

| | RocksDB version | Redis version | SQLite version |
| --- | --- | --- | --- |
| Atomic commit primitive | WriteBatch | Lua guarded-commit script | SQL transaction |
| Read-validate-write zero window | Via in-process `mu_` (read inside lock, outside batch) | Via precondition byte comparison + CAS retry | **Read directly inside the transaction**, naturally zero window |
| Conflict handling | None (mutex-serialized) | Script returns 0 → re-read and retry (exponential backoff) | None (mutex-serialized + BEGIN IMMEDIATE) |
| parts consistency in complete | Rescan inside lock | sha1 fingerprint passed to script for comparison | In-txn SELECT is the latest by definition |
| Multi-process atomicity | No | Yes (server-side script) | No (§1 non-goal) |

The SQLite version is, of the three, **the one whose transaction machinery aligns
most literally with the interface contract ("commit-class methods complete in a
single internal transaction")** — least invariant code, no retry path, no
fingerprint trick. The mutex is still kept (same `mu_` as the RocksDB version):
BEGIN IMMEDIATE is already mutually exclusive at the SQLite level; adding the
in-process mutex turns "hit SQLITE_BUSY then retry" into "queue up", a simpler and
more deterministic path. The lock encompasses COMMIT's fsync (when
`meta_sync=true`), so the write throughput ceiling ≈ 1/fsync latency — same
trade-off as the RocksDB version, accepted at P1, noting that group commit is not
done.

## 4. alloc_file_id: counters Segments

Isomorphic to the RocksDB version (main doc §4.5): the `IdRange` struct, the
separate `alloc_mu_` small lock (lock order always `mu_` → `alloc_mu_`),
`kIdSegment = 4096` copied verbatim. A reservation = a single-statement
transaction on the **dedicated alloc connection**:

```sql
UPDATE counters SET val = val + 4096 WHERE name = ?1 RETURNING val;  -- returns the new upper bound hi
```

Memory hands out `[hi−4096, hi)`. gcq's seq does **not** use segments: writing on
the segment connection while the business transaction holds wc_'s write lock would
hit SQLite's single-writer exclusion (busy-waiting until self-deadlock), and seq
only needs "unique among live rows + monotonic" — the `AUTOINCREMENT` rowid,
allocated within the business transaction and committed/rolled back with the same
batch, fits exactly (§2.2). `RETURNING` requires SQLite ≥3.35; the submodule is
pinned to a recent release tag (§7.1), far above that.

**Durability warning** (corresponding to the RocksDB version's "segment
reservations always WAL-fsync"): a crash that rolls back the counter would re-issue
already-used file_ids, colliding with chunks already on disk. SQLite's
`synchronous` is a **per-connection** PRAGMA — hence alloc uses a separate
connection, always `synchronous=FULL`, never downgraded by `meta_sync=false`; the
business write connection's level is unaffected. This is cleaner than the RocksDB
version (per-write sync flag override on the same DB). The backstop is the same as
in both other implementations: data-plane chunk creation uses `O_EXCL`, so hitting
an existing file fails loudly.

**Write-lock contention**: the reservation UPDATE competes with business
transactions for SQLite's single-writer lock, absorbed first by busy_timeout(5s);
under write hotspots the busy handler queues unfairly and may lose the race
repeatedly — SQLITE_BUSY means the single statement definitively did not execute,
so **bounded retries (≤4 rounds, each with its own 5s wait) instead of an
immediate 500**; only past the limit is InternalError thrown.

## 5. SQLite Integration

### 5.1 Compile-Time Options

The amalgamation (§7.2) is compiled into `lights3_core` with fixed options:

```text
SQLITE_THREADSAFE=1            # serialized (§3.1: defense in depth, negligible perf difference)
SQLITE_OMIT_LOAD_EXTENSION     # no extension loading, drops the dlopen dependency
SQLITE_DQS=0                   # double-quoted strings treated as standard SQL (typo-proofing)
SQLITE_DEFAULT_MEMSTATUS=0     # avoids the global memory-stats lock
SQLITE_LIKE_DOESNT_MATCH_BLOBS # all keys are BLOB in this design, LIKE is not involved
SQLITE_MAX_EXPR_DEPTH=0
SQLITE_OMIT_DEPRECATED
SQLITE_OMIT_SHARED_CACHE       # shared-cache is irrelevant and harmful to the WAL/pool model
SQLITE_USE_ALLOCA
```

(A trimmed version of the official "Recommended Compile-time Options".
`SQLITE_OMIT_AUTOINIT` is not defined — saves one initialization contract, minimal
benefit.)

### 5.2 Open Sequence

After each connection's `sqlite3_open_v2(path, READWRITE|CREATE, NOMUTEX not set)`:

```sql
PRAGMA journal_mode = WAL;       -- persistent property, see §6
PRAGMA synchronous  = FULL|NORMAL;  -- per meta_sync (§6); alloc connection always FULL (§4)
PRAGMA busy_timeout = 5000;      -- defense in depth: no long BUSY should occur in-process (§3.2/§4)
PRAGMA cache_size   = -<KiB>;    -- sqlite_cache / (pool_size+2): cache is per-connection,
                                 -- config semantics = process-wide total budget, spread across all connections (§8)
PRAGMA temp_store   = MEMORY;
PRAGMA foreign_keys = OFF;       -- no foreign keys: cross-table invariants are held by transactions (§3.3), no implicit delete ordering
```

Lineage validation (§2.2) runs **before** the PRAGMAs above, on a bare connection
(busy_timeout only) — `journal_mode=WAL` is a persistent file-header modification;
a foreign file must be refused before it is ever touched.

The first connection is responsible for database creation: `application_id` /
`user_version` validation and table creation (§2.2) — inside one write
transaction, so concurrent multi-connection opens are safe too.

### 5.3 Statement and Resource RAII

- **Resident prepared-statement cache**: all SQL is named literal constants; each
  connection caches by literal address (first use `sqlite3_prepare_v2`, reused
  afterwards); at the use site, RAII (bind → step → destructor `sqlite3_reset` +
  `clear_bindings`), eliminating runtime SQL assembly and recompilation. Parameters
  are always `?N` placeholder bindings with `SQLITE_TRANSIENT` copies (avoiding
  lifetime entanglement with reseek/exception paths) — **string-concatenated SQL is
  forbidden** (a red line of the same rank as the Redis version's ban on `%s`
  formatting: it is both an injection surface and a BLOB truncation source);
- **Connection hygiene**: before returning to the read pool or reusing the write
  connection, check `sqlite3_get_autocommit` — the extreme path where COMMIT and
  the fallback ROLLBACK fail in succession (the Txn destructor swallows exceptions)
  leaves an open transaction; bringing it back to the pool = bare reads forever
  reading a frozen snapshot (silently stale), and transactional methods hitting
  nested BEGIN. A tainted read connection first gets a make-up ROLLBACK — success
  restores autocommit and it may safely return to the pool (since S4; before that
  it was conservatively destroyed, and the evaluation saved one rebuild); only if
  that still fails is it destroyed; the write connection gets the make-up ROLLBACK
  first, and on failure fails loudly with 500;
- `close()`: read pool and segment connections close directly; the write connection
  runs `PRAGMA optimize`, then `sqlite3_wal_checkpoint_v2(TRUNCATE)` to merge the
  WAL back into the main file, closes last, then releases the `.lock` file lock —
  after a clean close the directory holds only the single DB file, directly
  copyable for cold backup (§6.3). **Do not use `PRAGMA wal_checkpoint`**: when
  blocked by a reader it returns OK through sqlite3_exec, the busy flag lives only
  in the discarded result row, and the failure is entirely silent; the v2 API's
  return code + residual frame count are detectable, and an incomplete truncation
  must be alerted on (warning that cold backup needs the -wal file). Any call
  afterwards cleanly throws `InternalError` (mirroring the RocksDB version's
  `db()` guard; read_conn rechecks closed_ after establishing the connection,
  plugging the TOCTOU window).

### 5.4 Error Mapping

Uniformly via `throw_sqlite(what, rc, conn)` (mirroring `throw_status`:
LOG_ERROR + throw `s3::S3Error`):

| Source | Handling |
| --- | --- |
| `SQLITE_CONSTRAINT_*` | Not expected to reach the semantic layer (create_bucket uses the explicit check, §3.3) → `InternalError` + log (a symptom of a bypassed invariant) |
| step with no row / empty SELECT | Not an error — the semantic layer converts to `NoSuchKey` / `NoSuchBucket` / `NoSuchUpload`, isomorphic to RocksDB NotFound handling (main doc §10) |
| `SQLITE_BUSY` | Theoretically unreachable (§3.2); still BUSY after busy_timeout is exhausted → `InternalError` + alert (someone outside the process is touching the database, violating the §1 premise) |
| `SQLITE_CORRUPT` / `SQLITE_NOTADB` | `InternalError`(500) + corruption alert (a symptom of data loss, aligned with main doc §10) |
| `SQLITE_FULL` / `SQLITE_IOERR` and other non-OK | `InternalError`(500) + error log (incl. `sqlite3_errmsg`) |

An in-process embedded library has no connection/timeout/indeterminate-result
problems — the Redis doc §3.5 blind-retry ban has no counterpart here; once
COMMIT returns, the result is determinate.

## 6. Durability and Consistency Statement

Commit point = write transaction COMMIT (WAL append). Against main doc §6.3:

| RocksDB version | SQLite counterpart | Semantics |
| --- | --- | --- |
| `meta_sync: true` (default, WAL fsync per commit) | WAL + `synchronous=FULL` | Committed = durable |
| `meta_sync: false` | WAL + `synchronous=NORMAL` | Power loss drops the most recent commits but **the database stays self-consistent** (WAL replays to the last complete checkpoint boundary): duostore's "data lands first, meta commits after" ordering (main doc §6) guarantees lost meta only produces orphan data, reclaimed by the orphan scan — the §6.2 crash matrix holds unchanged. The sole exception is the file_id counter, covered by the §4 dedicated FULL connection |
| — | `journal_mode=DELETE` (rollback journal) | **Not used**: every commit fsyncs whole pages twice and writers block all readers; both worse than WAL |

- WAL is a **persistent property of the database**; the first connection sets it
  once and it is written into the file header;
- Checkpointing uses the default automatic policy (piggybacked on committing
  threads when the WAL exceeds 1000 pages); the background worker does no active
  checkpointing — the WAL file's upper bound ≈ peak unmoved backlog; meta records
  are small, acceptable. S4 tuning conclusion: no additions to the automatic
  policy, only `journal_size_limit=4MiB` added — after auto-checkpoint empties the
  WAL, it truncates the `-wal` file back to the limit; otherwise its size would sit
  forever at the historical high-water mark (the default -1 never shrinks).
  `PRAGMA optimize` runs only once at `close()` (§5.3): the statements are all
  primary-key point lookups/prefix scans with naturally stable plans; periodic
  runtime execution has zero benefit;
- **Cold backup path** (this section is the reference target of §6.3): after a
  clean `close()` (§5.3 checkpoint TRUNCATE), copying the single file is a complete
  backup; a direct `cp` while running guarantees nothing. **Online backup
  evaluation conclusion (S4)**: the in-process `sqlite3_backup` API remains
  not-done — WAL mode allows concurrent external read-only connections (the
  `.lock` flock only blocks a second instance of ours, not backup tools; readers
  do not block writers); operations can use the sqlite3 CLI's `VACUUM INTO
  'backup.sqlite3'` (or `.backup`; both are based on a consistent snapshot) to take
  a consistent snapshot while running. An in-process implementation would only save
  one external tool, at the cost of a new API/config/lifecycle-management trio.
  Caveat: during the backup, that read transaction pins the WAL reclamation point,
  so the WAL may grow temporarily (recovering once the backup completes); the meta
  database is small, the duration negligible.

## 7. Build Integration

### 7.1 sqlite Submodule

`.gitmodules` adds:

```text
[submodule "third_party/sqlite"]
    path = third_party/sqlite
    url = https://github.com/sqlite/sqlite.git
    shallow = true
```

`shallow = true` as with rocksdb (the fossil mirror's full history is large); the
submodule is pinned to a recent release tag (e.g. `version-3.50.x`). `build.sh`'s
`LIGHT_MODULES` always inits it (same policy as hiredis — pulled even when the
option is OFF; lazy fetching is reserved for seastar with its system-level heavy
dependencies).

**Key difference: the git tree does not contain a ready-made amalgamation.** The
canonical source tree must first generate `sqlite3.c/sqlite3.h`
(`./configure && make sqlite3.c`); the generation scripts run on tclsh — this
machine has `/usr/bin/tclsh` 8.6 (satisfied in the no-sudo environment), and new
trees (≥3.49, autosetup build system) can bootstrap with the built-in jimsh when
no system tclsh exists; the configure probe decides, no hard dependency.

Rejected alternatives: system libsqlite3 (version uncontrollable in a no-sudo
environment, compile options uncontrollable, the §5.1 option set unattainable);
committing an amalgamation snapshot directly (breaks the submodule convention,
upgrades require manually moving files, 9MB+ of generated artifacts entering git
history).

### 7.2 CMake Wiring (After the rocksdb/hiredis Template, Main Doc §13.3)

New option **`LIGHTS3_DUOSTORE_SQLITE_META`, default OFF**, depends on
`LIGHTS3_DUOSTORE`. Like the Redis version, it is an optional meta backend outside
the default build surface; the motivation differs — Redis is OFF because tests need
an external server, here it is that amalgamation generation introduces a tclsh
toolchain dependency + an optional component should not burden the default build.
Feature rot is covered by the optional CI matrix.

```cmake
if(LIGHTS3_DUOSTORE_SQLITE_META)
    # Generate the amalgamation (out-of-tree configure; artifacts in the build dir)
    set(SQLITE_GEN_DIR ${CMAKE_BINARY_DIR}/sqlite-amalgamation)
    add_custom_command(
        OUTPUT ${SQLITE_GEN_DIR}/sqlite3.c ${SQLITE_GEN_DIR}/sqlite3.h
        COMMAND ${CMAKE_COMMAND} -E make_directory ${SQLITE_GEN_DIR}
        COMMAND ${CMAKE_COMMAND} -E chdir ${SQLITE_GEN_DIR}
                ${CMAKE_SOURCE_DIR}/third_party/sqlite/configure
        COMMAND ${CMAKE_COMMAND} -E chdir ${SQLITE_GEN_DIR} make sqlite3.c
        DEPENDS ${CMAKE_SOURCE_DIR}/third_party/sqlite/VERSION
        COMMENT "Generating SQLite amalgamation" VERBATIM)
    enable_language(C)  # the project itself is CXX-only; the amalgamation is the sole C translation unit
    add_library(lights3_sqlite3 STATIC ${SQLITE_GEN_DIR}/sqlite3.c)
    target_include_directories(lights3_sqlite3 PUBLIC ${SQLITE_GEN_DIR})
    target_compile_definitions(lights3_sqlite3 PRIVATE
        SQLITE_THREADSAFE=1 SQLITE_OMIT_LOAD_EXTENSION SQLITE_DQS=0
        SQLITE_DEFAULT_MEMSTATUS=0 SQLITE_LIKE_DOESNT_MATCH_BLOBS
        SQLITE_MAX_EXPR_DEPTH=0 SQLITE_OMIT_DEPRECATED
        SQLITE_OMIT_SHARED_CACHE SQLITE_USE_ALLOCA)
    target_compile_options(lights3_sqlite3 PRIVATE -w)
    target_sources(lights3_core PRIVATE src/storage/duostore/sqlite_meta_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_SQLITE_META)
    target_link_libraries(lights3_core PRIVATE lights3_sqlite3)
endif()
```

(The single-translation-unit amalgamation is the officially recommended form, far
more controllable than add_subdirectory of the whole autosetup tree;
`DEPENDS VERSION` makes a submodule upgrade trigger regeneration.)
When OFF, configuring `meta: sqlite` throws a "not compiled in"
`std::runtime_error` in `from_params` (same shape as the Redis version).

### 7.3 Component Relations and Reuse

- New files are only `sqlite_meta_store.{h,cc}`; `DuoStoreBackend`, `FsDataStore`,
  GC, and the S3 semantic layer are untouched;
- Reused: `codec.{h,cc}` for all value codecs and crc32c (§2.1) plus
  `bump_last_byte` (delimiter group-skip successor, one copy shared with the
  RocksDB version), `storage/duostore/meta_util.h`
  (`assemble_completed_object` — complete_upload's part selection and object
  assembly, shared by the three implementations, delivering the main doc §2.1
  "shared helper" promise), `storage/validate.cc`, `storage/multipart.h`
  (new_upload_id / validate_part_order / combined_etag);
- **Not reused**: `codec`'s CF key constructors (§2.2 table); `core/util/uri`
  (no URI; the path is the config).

## 8. Configuration

`DuoStoreConfig::from_params` adds new keys (YAML scalars automatically collected
into `BackendConfig.params`, convention as in main doc §11); `DuoMetaKind` gains
`kSqlite`:

```yaml
backends:
  - name: duodata
    type: duostore
    root: ./data/duostore
    meta: sqlite                     # rocksdb (default) / redis / sqlite
    # sqlite_path: /ssd/duo-meta.sqlite3   # optional: default <root>/meta.sqlite3
    sqlite_cache: 64MiB
    meta_sync: true                  # carried over: maps to synchronous FULL/NORMAL (§6)
    # remaining duostore keys (chunk_size / pack_* / gc_* / mpu_ttl ...) unchanged
```

| Key | Default | Description |
| --- | --- | --- |
| meta | `rocksdb` | Adds legal value `sqlite`; selecting sqlite when the option is not compiled in → configuration error |
| sqlite_path | `<root>/meta.sqlite3` | DB file path (corresponds to the RocksDB meta_path usage of pointing at an SSD); the parent directory is created by the store |
| sqlite_cache | 64MiB | Page-cache **process-wide total budget** (validated ≥1MiB): SQLite's cache_size is per-connection; the implementation spreads the budget across all connections (1 write + 1 alloc + pool_size reads, §5.2) — semantics aligned with rocksdb_block_cache's single-budget role, not amplified by connection count |
| meta_sync | true | **Carried over, not ignored** (contrast: ignored when meta=redis): SQLite, like RocksDB, is a local engine; the durability level is still owned by this process (§6) |

Engine-specific meta keys (meta_path / rocksdb_* / redis_* / sqlite_*) that appear
but do not belong to the selected engine always WARN — `from_params` uses a single
"key → owning engine" table; adding an engine adds table rows, so there is no
problem of branches each missing the other's keys.
`SqliteMetaOptions{path, sync, cache_bytes, pool_size}` aligns with the
`RocksMetaOptions` shape; the read-pool size reuses the default ≈ thread pool
size, without a dedicated config key (add one when needed, avoiding key bloat).

## 9. Test Strategy

1. **Meta store consistency suite**: `tests/unit/meta_store_suite.h` is already
   interface-ized (a direct beneficiary of the prerequisite refactor completed in
   the Redis version's R1) — `run_meta_store_suite` gains a SqliteMetaStore
   factory, `#ifdef LIGHTS3_DUOSTORE_SQLITE_META` conditionally compiled but
   **unconditionally run** (no external dependency, none of the Redis version's
   probe/SKIP paths). The factory convention "reopen a new instance on the same
   underlying storage" = repeatedly open/close the same temporary DB file,
   naturally covering restart semantics (segments never regress, schema
   validation);
2. **Composition and e2e**: injection-construct
   `DuoStoreBackend(cfg, pool, SqliteMetaStore, FsDataStore)` and run
   `run_backend_suite`; `run_e2e.sh` gains a `duostore-sqlite` branch, CMake
   registration under `if(LIGHTS3_DUOSTORE_SQLITE_META AND LIGHTS3_DRIVER_BUILTIN)`;
3. **SQLite specials** (`tests/unit/test_duostore_sqlite.cc`):
   - BLOB key ordering: list order and pagination tokens for keys containing
     0x01/0x7F/0xFF/non-UTF-8 bytes (grounded verification that memcmp order = S3
     order);
   - Reopen durability: objects/version/refs/gcq accounts preserved verbatim
     across close-reopen (segment monotonicity already covered by the shared
     suite);
   - Cold backup: after close, copy the single file to a new path and reopen; data
     intact, no WAL/SHM leftovers;
   - swap_extents optimistic-abandon path (version / extents mismatch → false, no
     write lands);
   - File lineage: a non-SQLite file, and an app_id=0 "someone else's database"
     with existing tables → loud refusal leaving no trace (no tables created, no
     stamp, journal not converted to WAL);
   - Single-process exclusivity: a second instance is refused by the `.lock`
     flock; reopenable after close releases it;
   - Duplicate create_bucket → BucketAlreadyOwnedByYou; calls after close → 500.

   S4 polish specials (same file):
   - Crash simulation: a child process (execv of self, `sync=true`) loops
     "alloc_file_id + put_object" and reports line-by-line after each COMMIT; the
     parent SIGKILLs at a random window; after restart every reported commit must
     exist (durability contract), refs↔objects bidirectional reconciliation
     converges, gcq has no phantom entries, segments never regress, and
     `integrity_check` is clean after a clean close — the full acceptance of WAL
     replay;
   - Consistent-view injection: the `set_list_pause_for_test` hook commits
     concurrently from the write connection after list emits its first entry
     (inserts/deletes in unvisited ranges + overwrite of an already-visited key) —
     this list's WAL snapshot stands unmoved; the next list sees the new state;
   - Metrics: an external bare connection holding the write lock plays the
     "outside visitor" → BUSY counting (single statement +1, segment reservation
     starved 4 rounds +4, recovering after the lock is released); a corrupted file
     header reopened → NOTADB on the open path counts as corruption and is loudly
     refused.

## 10. Implementation Phases

| Phase | Content | Independently acceptable | Status |
| --- | --- | --- | --- |
| S1 | sqlite submodule + amalgamation generation + CMake option + build.sh; connection/statement RAII, open sequence with schema creation/validation, error mapping; `counters` and alloc_file_id (dedicated FULL connection); the four bucket methods; suite factory wiring | RocksDB suite does not regress; S1 cases (bucket + segments + schema/reopen) green | Done |
| S2 | `Txn` guard + the four object methods (incl. list read transaction and delimiter group skip) + refs / gcq / swap_extents / chunk_referenced / peek_reclaims / ack_reclaim / pack_stats | Meta store suite green across the three implementations + BLOB ordering/cold-backup specials | Done |
| S3 | Full multipart set (create / put_part / list_parts / list_uploads / complete / abort); injected composition runs `run_backend_suite`; `e2e_duostore_sqlite` | Backend consistency suite + e2e green | Done |
| S4 | Polish: crash simulation (post-kill WAL replay reconciliation) and consistent-view injection specials, `sqlite3_backup` online-backup evaluation (remains not-done, §6.3), `PRAGMA optimize`/checkpoint tuning (`journal_size_limit`, §6), metrics (BUSY/corruption counts), doc status header update | Full ctest matrix green | Done (2026-07-30) |

The rationale for doing bucket first in S1 is the same as the Redis version: the
four bucket methods cover the three shapes "constraint-conflict atomicity
(create) + pure read (exists/list) + the simplest compound transaction (delete's
emptiness check)", laying the full foundation of the connection layer, statement
cache, and transaction guard; from S2 on it is pure business translation — and
compared with the Redis version there is one whole layer less (no script
machinery), so the expected size of S2/S3 is smaller.
