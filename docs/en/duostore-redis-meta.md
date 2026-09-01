# RedisMetaStore: Redis-Based DuoStore Metadata Store

> English translation of [../duostore-redis-meta.md](../duostore-redis-meta.md). The Chinese original is authoritative; section numbering matches.

> Status: R1-R4 all complete (full `RedisMetaStore` interface + guarded-commit
> script + test-suite interfacing + e2e + R4 polish: AOF probe warning /
> `redis_wait_replicas` / CAS retry and reconnect metrics / `list_uploads`
> HSCAN batching / TLS evaluated and kept disabled; code in
> `src/storage/duostore/redis_meta_store.{h,cc}`, compile switch
> `LIGHTS3_DUOSTORE_REDIS_META` default OFF). This delivers the evolution
> promise of [duostore-backend.md](duostore-backend.md) §12: swap the meta side
> to Redis, implementing `IMetaStore` (`src/storage/duostore/meta_store.h`) so
> multiple gateways can share the same metadata. Client library: hiredis
> (`third_party/hiredis` submodule, §7). In this document, "main doc" refers to
> duostore-backend.md, and unprefixed `§N` refers to sections of this document.

## 1. Goals and Non-Goals

| Goal | Notes |
| --- | --- |
| Implement the full `IMetaStore` interface | The complete bucket / object / list / multipart / GC accounting set, semantically equivalent to `RocksMetaStore`—the same meta store test suite goes all green (§9) |
| Multi-gateway shared meta | Transaction atomicity is guaranteed by server-side Redis Lua scripts (§3.4), with no reliance on in-process mutexes—this is a semantic upgrade over the RocksDB version, and the motivation for introducing Redis |
| Zero new encoding/decoding | Value encoding reuses `codec.cc` 100% (§2.1); the two implementations are byte-identical in format |
| Implementation structure corresponds line-by-line to the RocksDB version | `RedisBatch` mirrors WriteBatch's commit shape (§3.2), shrinking the semantic-drift surface between the two implementations |

Non-goals (explicitly declared):

- **Redis Cluster is not supported.** The composite transactions (Lua scripts)
  span multiple keys—the bucket table, object HASH, lexicographic ZSET, refs,
  gcq, counters—which cannot be placed in the same hash slot; moreover object
  keys may contain `{}`, forming accidental hash tags. Escape hatch: giving all
  keys a uniform hash tag (e.g. `{duo}`) would run on Cluster but degrade to a
  single slot—not recommended, not tested. Supported scope = standalone /
  Sentinel primary-replica;
- **TLS is not enabled in the first phase** (hiredis's `hiredis_ssl` is an
  optional component; the project already links OpenSSL, so enabling it later
  is cheap, §5.5);
- RESP3 / client-side caching not used; RESP2 suffices;
- The **data-plane** cross-process issues of multi-gateway shared meta (the pin
  table of main doc §7; §12 already notes the need for leases/distributed grace
  periods) are not solved in this document—this document only guarantees
  meta-side correctness.

## 2. Redis Data Model

### 2.1 General Principles

1. **Value encoding reuses `codec.cc` 100%** (`encode_object / encode_upload /
   encode_part / encode_reclaim / encode_bucket / encode_extents`). Redis keys,
   values, and HASH fields are all binary-safe; hiredis carries `\0` fine via
   `redisCommandArgv` with explicit lengths (§5.1). Benefit: zero new codec
   code; the RocksDB / Redis implementations have byte-identical on-disk
   (in-memory) formats, and the codec roundtrip targeted tests are shared
   directly;
2. **Key naming = configurable prefix (default `duo:`) + `\0`-separated
   composite segments**, i.e. the style of the `codec` key builders spliced
   directly into Redis key names. Separator legality follows the same argument
   as main doc §4.1: the shared validation layer already rejects object keys
   containing NUL and restricts bucket names to `[a-z0-9.-]`, so `\0` is
   unambiguous. The prefix's purpose: multiple backend instances / multiple
   test suites can share one redis-server without polluting each other (§8,
   §9);
3. **Two keys per bucket for objects: a HASH for the payload + a ZSET for the
   lexicographic index**. All ZSET members have score=0—with equal scores
   Redis orders by member lexicographically, so `ZRANGEBYLEX` is the ordered
   iteration primitive, naturally supporting prefix / marker (§2.3); the
   metadata payload lives in the same bucket's HASH (field = object key, value
   = `encode_object`), point lookup `HGET` O(1). The two keys are added and
   removed together within the same transaction (§3.3).

   Rejected alternatives: one top-level STRING key per object—top-level key
   count explosion, delete_bucket's emptiness check degrades to SCAN, hard to
   reconcile when out of sync with ZSET members; stuffing metadata into the
   ZSET member—the member is the identity, so changing metadata = changing
   the member; not viable.

### 2.2 Key Layout

Row-by-row correspondence with the CF table of main doc §4.1 (keys below omit
the `duo:` prefix):

| RocksDB CF | Redis key | Structure | Notes |
| --- | --- | --- | --- |
| `default` | `schema` | STRING | On open, `SET NX` writes `"r1"`; if already present, read and validate; the lineage is distinct from the RocksDB schema. No `instance` key—meta is inherently shared by multiple gateways, not bound to an instance |
| `buckets` | `buckets` | HASH: field=`<bucket>`, value=`encode_bucket` | `create_bucket` = `HSETNX`, atomic as a single command; return 0 → BucketAlreadyOwnedByYou, no script needed; `list_buckets` = `HGETALL` + client-side sort by name (bucket count is small) |
| `objects` | `o:<b>` + `oz:<b>` | HASH + ZSET (§2.1 principle 3) | Point lookup `HGET o:<b> <key>`; iteration via `oz:<b>` |
| `uploads` | `up:<b>` + `uz:<b>` | HASH: field=`<key>\0<id>`, value=`encode_upload`; ZSET (score 0) whose members are the same fields (§2.1 principle 3, roadmap §3.5) | `list_uploads` = `ZRANGEBYLEX uz:<b>` paged from the cursor/prefix + `HMGET up:<b>` for the values — cursor and prefix are pushed down (same order and cost shape as the RocksDB prefix scan). When `ZCARD≠HLEN` (a table written before the index existed, or by an older gateway) it falls back to the `HSCAN COUNT 512` full scan and rebuilds the index on the way |
| `parts` | `pt:<b>\0<key>\0<id>` | HASH: field=decimal `part_no`, value=`encode_part` | One HASH per upload; `complete/abort` deletes the whole key with `DEL` (corresponding to RocksDB's range delete); ≤10k fields, `HGETALL` + client-side numeric sort |
| `refs` | `refs` | HASH: field=decimal `file_id`, value=owner summary | `chunk_referenced` = `HEXISTS`, O(1) |
| `gcq` | `gcq` | ZSET: score=`seq`, member=`be64(seq) ‖ encode_reclaim(...)` | The be64 prefix keeps members unique and self-contained with seq; `peek_reclaims` = `ZRANGEBYSCORE gcq -inf +inf LIMIT 0 max` (seq parsed exactly from the member's first 8 bytes); `ack_reclaim` = `ZREMRANGEBYSCORE gcq seq seq`. Constraint: score is a double, requiring seq < 2^53—at 10k deletes per second that lasts 28,000 years; declaring it suffices |
| `stats` (segment counters) | `ctr:chunk` / `ctr:pack` / `ctr:seq` | STRING (integer) | `INCRBY` segment reservation (§4) |
| `stats` (pack liveness accounting) | `pack:<id>` | HASH: live_bytes / live_recs / file_size / sealed | `HINCRBY` gives incremental accounting (replacing the RocksDB merge operator; the commit script adds an `hincr` op batched with business writes); `pack_stats()` = SCAN MATCH `pack:*` + per-key HGETALL (a low-frequency GC path); `seal_pack` with file_size=0 uses HSETNX so a known value is not overwritten |

### 2.3 list_objects: ZRANGEBYLEX + a Single Lua Script

The algorithm copies the RocksDB version (main doc §4.4): seek start =
`max(prefix, successor of start_after)`; on a delimiter hit, group and
**construct the successor seek point by incrementing the group's last byte to
skip the whole group**; fetch one extra entry to determine `is_truncated`. The
iteration primitive changes from the RocksDB Iterator to
`ZRANGEBYLEX oz:<b> [<seek> + LIMIT 0 <batch>`.

**The whole list loop goes into one Lua script**, because:

1. Delimiter listing re-seeks once per group—client-driven means one RTT per
   group, so a thousand groups mean a thousand network round trips; the
   in-script loop collapses to **1 RTT**;
2. Redis interleaves no other command during script execution, so a single
   script is naturally equivalent to RocksDB's pinned snapshot—one consistent
   view per call, semantics aligned with main doc §4.4;
3. The work is bounded: iteration count ≤ max_keys + number of groups (each
   O(log n)), and max_keys is capped at 1000, so the server is never blocked
   for long.

Inside the script, each hit key gets an `HGET o:<b>` and the values are
returned together; decoding happens on the C++ side
(`codec::decode_object_meta`, skipping extent runs without materializing
them—the same optimization as the RocksDB version). When a delimiter group
closes the page exactly full, `next_token` must land at the group's tail: the
script runs `ZREVRANGEBYLEX oz:<b> (<group successor> - LIMIT 0 1` to fetch the
group's last key (corresponding to the RocksDB Iterator's `SeekForPrev`).

Rejected alternative: client-driven multi-round ZRANGEBYLEX—saves one script,
but loses on both delimiter RTT amplification and the lack of a consistent
view across rounds (concurrent writes mid-listing may cause misses/dups).

## 3. Transactions and Invariants

### 3.1 Route Choice: Lua Guarded Commit, Not MULTI/WATCH

`IMetaStore`'s commit-class methods are all "read-validate-write" composite
transactions (re-checking bucket existence, reading the old record to compute
version and gcq accounting, complete's ETag comparison). Comparing Redis's
three atomicity primitives:

| Route | Verdict |
| --- | --- |
| MULTI/EXEC | Only unconditional batched writes; cannot express "abort if validation fails"; out |
| WATCH + MULTI | Can do optimistic transactions, but WATCH is **connection-level state**—conflicting with the connection-pool model (§5.2) (a pooled connection may carry residual WATCH), and the validation logic is scattered across two client round trips; clumsy shape |
| **Lua script (EVALSHA, chosen)** | Atomic execution on the server's single thread; check and write in the same script with zero window; no state requirements on the connection |

**Iron rule of division of labor: reads and computation in C++; Lua does only
"byte-level precondition comparison + batched writes".** Values are in
`codec.cc`'s binary format; parsing them in Lua would mean rewriting the codec
in Lua and maintaining two copies of the format code—rejected. So values are
fully opaque to the script: C++ reads first and computes, passes "the raw bytes
I read" to the script as preconditions, and the script writes only if the
comparison passes; a failed comparison means a concurrent modification, and C++
re-reads and retries.

### 3.2 The Generic Guarded-Commit Script

One script serves all commit-class operations—this is the Redis version's
stand-in for WriteBatch:

```text
Input: KEYS = all keys involved in this commit
       ARGV = n_checks | check*        check := type, key_idx, field, expected
            | n_ops    | op*           op    := kind, key_idx, field, value, [score]
check types: eq (HGET == expected raw bytes) / absent (HGET is nil)
            / exists (HEXISTS) / hlen0 (HLEN == 0) / zcard0 (ZCARD == 0)
            / sha1 (HGETALL concatenated in numeric field order, then
              redis.sha1hex == expected, §3.3)
op types:   hset / hdel / zadd / zrem / del / set / incrby / hincrby
Semantics:  any check fails → return 0 immediately, executing no op;
            all pass → execute ops in order, return 1
```

The C++ side provides `RedisBatch`: `hset()/hdel()/zadd()/…` mirror
WriteBatch's append interface, plus `expect_eq()/expect_absent()/…` to append
preconditions; `commit()` is one `EVALSHA`. Each IMetaStore method's
implementation structure corresponds line-by-line to `rocks_meta_store.cc`
(read old values → build batch → commit), with only "WriteBatch under the
lock" replaced by a "CAS retry loop": script returns 0 → re-read, rebuild the
batch, retry, with **exponential backoff** before each retry (starting at
100µs, capped at 6.4ms—a tight loop without backoff can be starved by a
peer's continuous commit stream), capped at 16 attempts, beyond which
`InternalError` is thrown (meaning pathological hot-spot contention; failing
loudly beats a livelock).

### 3.3 Per-Method Flows

Against the same-batch content table of main doc §4.5 (segment-allocation and
read-only methods omitted):

| Operation | Precondition checks | Same-batch ops |
| --- | --- | --- |
| put_object | bucket exists; `o:<b>[k]` eq old raw bytes (or absent) | HSET `o` new ObjectVal (version = old +1) + ZADD `oz` + HSET `refs` for new chunks + old DataRef into `gcq` + HDEL old `refs` + pack accounting HINCRBY negative deltas |
| delete_object | `o:<b>[k]` eq the old bytes read (if absent, return false directly without sending the script) | HDEL `o` + ZREM `oz` + old DataRef into `gcq` + HDEL `refs` + negative pack accounting |
| create_upload | bucket exists | HSET `up` (id generated by `storage/multipart.h::new_upload_id`) |
| put_part | `up:<b>[key\0id]` exists; `pt:…[part_no]` eq old bytes (or absent) | HSET `pt` + new `refs` + old part into `gcq` + HDEL old `refs` (same-number re-upload is last-write-wins) |
| complete_upload | `up:<b>[key\0id]` eq; `pt:…` **sha1 fingerprint**; `o:<b>[k]` eq old bytes (or absent) | HSET `o` + ZADD `oz` + HDEL `up` + DEL `pt:…` + unselected parts into `gcq` + old same-name object into `gcq` + `refs` transfer + pack accounting |
| abort_upload | `up:<b>[key\0id]` eq | HDEL `up` + DEL `pt:…` + all parts into `gcq` + HDEL `refs` |
| delete_bucket | `buckets[b]` exists; `o:<b>` hlen0; `up:<b>` hlen0 | HDEL `buckets` + DEL `oz:<b>` (the emptiness check covers in-progress multipart, aligned with AWS, same argument as the RocksDB version) |
| swap_extents | `o:<b>[k]` eq the old object's **entire raw bytes** (naturally subsuming expect_version and the from check; C++ decodes and confirms first, then commits) | HSET `o` new ObjectVal (version+1, DataRef=to) + pack accounting migration |
| GC settlement (ack_reclaim) | — (a single command is atomic) | ZREMRANGEBYSCORE `gcq` (refs were already deleted in the same batch as the business transaction, consistent with the RocksDB version; called after the physical unlink, the ordering iron rule of main doc §9.1 unchanged) |

- **complete_upload's parts fingerprint**: per-item ETag comparison,
  `combined_etag`, and extent-run splicing all happen in C++ (reusing
  `storage/multipart.h`, the same helper set as the RocksDB version); the
  precondition needs "the parts set is unchanged since it was read", but
  sending tens of thousands of part originals back to the script for comparison
  is too wasteful—C++ concatenates the raw values of the fields it read in
  numeric part_no order and takes the sha1; the script recomputes it over the
  current content with `redis.sha1hex` and compares: O(parts) server-side
  compute traded for O(1) network transfer;
- **create_bucket / bucket_exists / get_object / require_upload /
  list_* / peek_reclaims / chunk_referenced / pack_stats** are single-command
  or read-only and do not use the script. Pure reads are naturally consistent
  (single-command atomicity); list's consistent view is provided by the §2.3
  script.

### 3.4 The Atomicity Argument under Multiple Gateways

The RocksDB version serializes composite invariants with one `std::mutex`
(main doc §4.5)—which only serializes **this process**. The Redis version's
counterpart is the Lua script's atomic execution on the server's single
thread: **script atomicity is global atomicity**, holding equally across
processes and gateways. Hence RedisMetaStore **holds no business mutex** (only
the small in-memory lock for segment dispatch remains, §4); lock semantics are
replaced by optimistic CAS retry; gcq's seq and file_id are allocated via
`INCRBY`, globally monotonic. This is the semantic upgrade of this
implementation over the RocksDB version: multiple gateway processes pointing at
the same redis_uri share the meta with no extra coordination (for the
data-plane precondition see §1 non-goals).

### 3.5 Script Management and the Blind-Retry Ban

- **Script management**: the Lua source is embedded as a C++ raw string
  constant (no .lua files deployed); on connection establishment, `SCRIPT
  LOAD` and remember the SHA; calls always use `EVALSHA`; on a NOSCRIPT error,
  fall back to `EVAL` and re-LOAD (self-healing after server restart / SCRIPT
  FLUSH);
- **Blind-retry ban**: a connection timeout/drop after a commit-class script
  was sent = **indeterminate outcome**—the previous attempt may have taken
  effect. Blindly retrying internally at that point would make the replayed
  put_object record "the very DataRef just written" as the old value into gcq,
  and GC would then reclaim referenced data, breaking the fundamental
  invariant. Therefore an indeterminate outcome always throws `InternalError`,
  deferring to the S3 client's retry—which re-sends the data and allocates a
  new file_id, carrying no such risk. Only two categories may auto-reconnect
  and retry: read-only commands, and requests that failed before sending (the
  connection was already dead when taken from the pool) (§5.4). A CAS return of
  0 is not in this category—0 is a definite outcome, safe.

## 4. alloc_file_id: INCRBY Segments

Isomorphic to the RocksDB version (main doc §4.5): `INCRBY ctr:chunk 4096`
returns the new upper bound hi; dispatch `[hi−4096, hi)` from memory; the
`IdRange` struct, the separate `alloc_mu_` small lock, and the
`kIdSegment = 4096` constant carry over unchanged (alloc is called on the data
plane every time a chunk is opened and cannot be sequenced after the business
commit). The seq counter works the same way via `ctr:seq`—pre-dispatch makes
gcq entry a pure write op, keeping the guarded script deterministic (no new ids
generated inside Lua).

**Durability warning** (corresponding to the RocksDB version's "segment
reservation always WAL-fsyncs"): a Redis crash rolling back INCRBY would
re-issue already-used file_ids, colliding with chunk files already on disk.
Three layers of mitigation:

1. Strict correctness requires `appendfsync always` (§6);
2. Cheap mitigation: **on process start and on the first reservation after
   every reconnect, burn one extra segment**—skipping ids that may have been
   dispatched within the ≤1s loss window of everysec mode (wasting a segment
   on a crash is harmless; file_id only needs to be unique and monotonic, not
   contiguous—the same argument as the RocksDB version);
3. Backstop detection: data-plane chunk creation uses `O_EXCL`; hitting an
   existing file fails loudly, never silently overwriting.

## 5. hiredis Integration (Synchronous Client)

Calling the synchronous hiredis API on pool threads is exactly the pattern
anticipated by main doc §2.2 when choosing a synchronous interface for
IMetaStore (isomorphic to cloudproxy running synchronous httplib on pool
threads).

### 5.1 Command Construction

Always `redisCommandArgv` (argc + argv[] + argvlen[], binary-safe)—in this
scheme keys, fields, and values may all contain `\0`. **`redisCommand`'s `%s`
formatting is forbidden** (it truncates at C-string boundaries—silent data
corruption; a code-review red line). Default RESP2 protocol is fine.

### 5.2 Connection Model: A Small Connection Pool

A mutex-protected idle-connection stack, default size ≈ thread-pool size
(`redis_pool_size`, §8); each IMetaStore call takes/returns via RAII.
thread_local single connections rejected: `close()` timing, thread-exit
cleanup, and dead-connection rebuilds are all harder to manage; and this scheme
has no cross-command session state (no MULTI/WATCH, §3.1), so switching
connections within one logical operation is harmless—pooling is simplest.

### 5.3 Reply Lifetime and Error Mapping

`redisReply` is uniformly wrapped in
`std::unique_ptr<redisReply, FreeReplyDeleter>`. Errors are layered, uniformly
via `throw_reply(what, ...)` (modeled on the RocksDB version's
`throw_status`: LOG_ERROR + throw `s3::S3Error`):

| Source | Handling |
| --- | --- |
| `ctx->err` (IO / EOF / protocol / timeout) | Discard the connection; failed before sending, or read-only → reconnect and retry once; commit-class indeterminate → `InternalError` (§3.5 ban) |
| `REDIS_REPLY_ERROR` | `InternalError` (500), carrying the server's error text |
| NOSCRIPT | Re-`SCRIPT LOAD` then resend (definitively not executed, safe) |
| Semantic absence (empty HGET etc.) | Not an error—the C++ semantic layer converts to `NoSuchKey` / `NoSuchBucket` / `NoSuchUpload`, isomorphic to the RocksDB version's NotFound handling (main doc §10) |

### 5.4 Timeouts and Reconnection

`redisConnectWithTimeout` for connection + `redisSetTimeout` for command
timeout (default 3s, `redis_timeout`); a dead connection is dropped from the
pool and a new one created. After reconnecting, restore state in order: `AUTH`
(if the uri carries a password) → `SELECT` (if not db 0) → `SCRIPT LOAD`.
Retry boundaries strictly follow §3.5.

### 5.5 TLS and close

TLS goes through hiredis's separate `hiredis_ssl` component (compile-time
`ENABLE_SSL`); not enabled in the first phase to keep the build surface small;
the project already links OpenSSL, so it is listed as an optional evolution.

**R4 evaluation conclusion: keep it disabled.** (1) The supported scope is
standalone / Sentinel primary-replica (§1), whose typical deployment shape is a
same-host unix socket or intranet TCP—no concrete demand; (2) enabling it is
a deterministic small job—rebuild hiredis with `ENABLE_SSL=ON` to get the
`hiredis_ssl` library, handshake via `redisInitiateSSLWithContext` after
connecting, add the `rediss://` URI dialect plus three config keys for
CA/certificate/private key; the project already links OpenSSL so no new
dependency—enable on demand when the need arises; (3) in the meantime the
alternative across untrusted networks is stunnel / network-layer encryption.

`close()`: drain the connection pool, `redisFree` each one; any call afterward
cleanly throws `InternalError` (modeled on the RocksDB version's `db()`
guard—defense in depth: misuse becomes a 500, not a crash).

## 6. Durability and Consistency Statement

Redis's default persistence (RDB snapshots) is a **non-starter** for this
scheme—a whole-database rollback means meta pointing at data already
reclaimed by GC, breaking the fundamental "meta is truth" invariant.
Deployment requires AOF, contrasted with main doc §6.3's meta_sync:

| RocksDB version | Redis counterpart | Semantics |
| --- | --- | --- |
| `meta_sync: true` (WAL fsync per commit) | `appendonly yes` + `appendfsync always` | Durable on commit |
| `meta_sync: false` | `appendfsync everysec` (**recommended default**) | A crash loses the most recent ≤1s of metadata but **stays self-consistent**: duostore's "data lands first, meta commits after" ordering (main doc §6) ensures lost meta only produces orphan data, reclaimed by the orphan scan—the §6.2 crash matrix argument holds verbatim. The sole exception is file_id counter rollback, covered by §4's three-layer mitigation |
| — | RDB only / persistence off | **Unsupported** (whole-database rollback, see above) |

- On open, probe with `CONFIG GET appendonly`; if not AOF, log a WARN (managed
  Redis may disable the CONFIG command—degrade to a "cannot probe" notice,
  do not refuse to start);
- **WAIT (implemented in R4)**: shrinks the failover lost-write window under
  Sentinel primary-replica deployments. When `redis_wait_replicas` (default 0
  = no waiting) > 0, after each commit-class command succeeds, append
  `WAIT <n> <timeout>` on the **same connection** (WAIT only covers this
  connection's prior writes; timeout is half the command timeout, guaranteeing
  the server returns before the client's read timeout). Insufficient replicas
  or a rejected WAIT is **WARN-only, not an error**—the write has already
  taken effect on the primary, and erroring would mislead the S3 client into
  retrying (complete-class retries would even get a spurious NoSuchUpload).
  Note WAIT only guarantees replication delivery, not replica fsync.

## 7. Build Integration and Component Relationships

### 7.1 hiredis Submodule

`.gitmodules` gains:

```text
[submodule "third_party/hiredis"]
    path = third_party/hiredis
    url = https://github.com/redis/hiredis.git
```

No `shallow` (the repo is tiny, unlike the rocksdb treatment); `build.sh`'s
`LIGHT_MODULES` **always inits it**—pure C, zero system-level dependencies,
small footprint, same policy as rocksdb (main doc §13.2), no lazy fetching. No
extra requirements in a sudo-less environment.

### 7.2 CMake Preset (Modeled on the rocksdb Template, Main Doc §13.3)

New option **`LIGHTS3_DUOSTORE_REDIS_META`, default OFF**, depending on
`LIGHTS3_DUOSTORE`. The rationale is the opposite of rocksdb's default ON: this
implementation's unit tests/e2e need an external redis-server present (§9), and
it is an optional backend—everyday builds should not carry this burden;
feature rot is covered by CI's optional matrix rather than the default build.

```cmake
if(LIGHTS3_DUOSTORE_REDIS_META)
    set(DISABLE_TESTS ON CACHE BOOL "" FORCE)
    set(ENABLE_SSL OFF CACHE BOOL "" FORCE)        # §5.5: TLS not enabled in the first phase
    set(ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
    set(BUILD_SHARED_LIBS OFF CACHE BOOL "" FORCE) # static linking, consistent with repo convention
    if(CMAKE_VERSION VERSION_GREATER_EQUAL 3.25)
        add_subdirectory(third_party/hiredis EXCLUDE_FROM_ALL SYSTEM)
    else()
        add_subdirectory(third_party/hiredis EXCLUDE_FROM_ALL)
    endif()
    target_sources(lights3_core PRIVATE src/storage/duostore/redis_meta_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_REDIS_META)
    target_link_libraries(lights3_core PRIVATE hiredis::hiredis_static)
endif()
```

(hiredis ≥1.1 provides the `hiredis::hiredis_static` alias; older versions
fall back to the `hiredis_static` target name.) When OFF, configuring
`meta: redis` makes `from_params` throw a "not compiled in"
`std::runtime_error`.

### 7.3 Component Relationships and Reuse

- New files are only `redis_meta_store.{h,cc}` (+ the embedded Lua constant),
  implementing `IMetaStore`; `DuoStoreBackend`, `FsDataStore`, GC, and the S3
  semantic layer are untouched—exactly what main doc §2.1's choice of a
  semantic-level interface promised;
- Reused: all value encoding/decoding and crc32c of `codec.{h,cc}` (§2.1),
  `storage/validate.cc`, `storage/multipart.h` (new_upload_id /
  validate_part_order / combined_etag), `core/util/uri` (redis_uri parsing,
  §8);
- **Not reused**: the RocksDB-specific parts of `codec`'s CF key builders (the
  refs/gcq keys of `be64_key`, the be16 suffix of `part_key`—the Redis side
  uses decimal fields, §2.2); take only what is needed.

## 8. Configuration

New keys in `DuoStoreConfig::from_params` (all YAML scalars, automatically
collected into `BackendConfig.params`, zero parser changes, same convention as
main doc §11):

```yaml
backends:
  - name: duodata
    type: duostore
    root: ./data/duostore
    meta: redis                        # default rocksdb
    redis_uri: redis://:pass@127.0.0.1:6379/0   # or unix:///path/to/redis.sock
    redis_prefix: "duo:"
    redis_timeout: 3s
    redis_pool_size: 8
    redis_wait_replicas: 0
    # the remaining duostore keys (chunk_size / pack_* / gc_* / mpu_ttl ...) unchanged
```

| Key | Default | Notes |
| --- | --- | --- |
| meta | `rocksdb` | `rocksdb` / `redis`; choosing redis without `LIGHTS3_DUOSTORE_REDIS_META` compiled in → configuration error |
| redis_uri | — (required when meta=redis) | `redis://[:pass@]host:port[/db]` or `unix://<path>`, parsed by reusing `core/util/uri` |
| redis_prefix | `duo:` | Prefix for all keys (multi-instance/test isolation, §2.1) |
| redis_timeout | 3s | Connection + per-command timeout (`parse_duration_sec`) |
| redis_pool_size | 8 | Connection pool size (§5.2), recommended ≈ thread-pool size |
| redis_wait_replicas | 0 | Replica count for `WAIT` after commit-class commands, range [0,256] (§6; 0 = no waiting; insufficient replicas is WARN-only, not an error) |

With `meta: redis`, `meta_path` / `rocksdb_block_cache` / `meta_sync` are
ignored with a WARN (durability semantics are taken over by the Redis-side AOF
configuration, §6). `DuoStoreConfig` gains the `meta_kind` enum and the fields
above; the `DuoStoreBackend` constructor branches on `meta_kind` (the Redis
branch wrapped in `#ifdef LIGHTS3_DUOSTORE_REDIS_META`); the test-injection
constructor is untouched.

## 9. Testing Strategy

1. **Interfacing the meta store suite** (prerequisite refactor): the meta cases
   in `tests/unit/test_duostore.cc` currently instantiate `RocksMetaStore`
   directly—first extract them into `run_meta_store_suite(factory)` (modeled
   on the `backend_suite.h` pattern); RocksMetaStore always runs,
   RedisMetaStore runs conditionally, and both implementations share the same
   semantic baseline (all existing cases inherited: GC accounting, segment
   monotonicity, delete_bucket blocking in-progress MPU, list
   pagination/delimiter, etc.);
2. **Obtaining a real redis** (docker availability not assumed): at test
   startup, probe for the `redis-server` executable—if found, launch a
   private instance on a random port with a temp directory
   (`redis-server --port <N> --save '' --appendonly no --dir <tmp>`, killed at
   teardown); if not found, **SKIP explicitly with the reason printed** (not a
   failure). The `LIGHTS3_TEST_REDIS_URI` environment variable can override
   with an external instance (isolation via a per-case unique
   `redis_prefix`—pid + counter, so different runs never collide). miniredis
   style fake implementations rejected: the C++ ecosystem has no mature one,
   and the core of this scheme (Lua script semantics) is untestable against a
   fake;
3. **Combination and e2e**: injection-construct
   `DuoStoreBackend(cfg, pool, RedisMetaStore, FsDataStore)` and run
   `run_backend_suite` (same semantic baseline as memory/localfs/duostore);
   `run_e2e.sh` gains a `duostore-redis` branch (also probing for
   redis-server, skipping if absent), registered in CMake under
   `if(LIGHTS3_DUOSTORE_REDIS_META AND LIGHTS3_DRIVER_BUILTIN)`;
4. **Redis-specific tests**: guarded-commit conflict path (concurrent writes
   trigger a check failure → retry converges); NOSCRIPT self-healing
   (operations proceed normally after SCRIPT FLUSH); after killing connections,
   read-only auto-reconnects while commit-class throws InternalError (§3.5
   boundary, injected via CLIENT KILL); the swap_extents CAS abandonment path;
   list's delimiter group skipping and group-tail token; schema validation and
   prefix isolation (two stores with different prefixes sharing one server are
   mutually invisible). R4 additions: metrics registered with 0 at
   construction for visibility + reconnect counter increment assertion;
   `redis_wait_replicas` tolerated on standalone (0 replicas) (WARN, no
   error); `list_uploads` completeness and ordering across HSCAN batches;
   `redis_wait_replicas` config parsing and range validation.

## 10. Implementation Phases

| Phase | Content | Independently verifiable | Status |
| --- | --- | --- | --- |
| R1 | hiredis submodule + CMake option + build.sh; connection pool / reply RAII / error mapping / script loader; `ctr:*` counters and alloc_file_id; the four bucket methods + schema validation; meta test suite interfacing + redis-server probe/skip mechanism | RocksDB suite all green after the refactor; R1 cases green when redis is present | Done |
| R2 | Generic guarded-commit script + `RedisBatch`; the four object methods (including list_objects Lua) + refs / gcq / swap_extents / chunk_referenced / peek_reclaims / ack_reclaim | Meta store suite green on both implementations + conflict-retry/CAS targeted tests | Done |
| R3 | Full multipart set (create / put_part / list_parts / list_uploads / complete / abort, including the parts sha1 fingerprint); injection combination running `run_backend_suite`; `e2e_duostore_redis` | Backend consistency suite + e2e green | Done |
| R4 | Polish: AOF probe warning (landed with the constructor since R1), `redis_wait_replicas`, metrics (CAS retries / reconnect count, wired into the §3.1 framework), `list_uploads` HSCAN batching, TLS evaluation (§5.5, kept disabled), doc status header update | Full ctest matrix (including skip paths) green | Done |

R1 does buckets before objects: the bucket methods cover all three shapes—
"single-command atomic (HSETNX) + read-only + simplest script (delete_bucket's
emptiness check)"—laying the full foundation of the connection layer and
script machinery, so that R2's guarded-commit is pure business translation.
