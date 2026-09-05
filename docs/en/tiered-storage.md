> English translation of [../tiered-storage.md](../tiered-storage.md). The Chinese original is authoritative; section numbering matches.

# Tiered Storage: Demoting Cold Data to Public Cloud

> Status: P1–P5 all implemented (`src/storage/tiered/`; remaining P4 items wrapped
> up 2026-07-31: §9 reconciliation tool + exponential backoff for failed GC
> entries). The cloud side is integrated via the `IStorageBackend` abstraction,
> and CI uses MemoryBackend as the cloud for full coverage (unit tests
> `test_tiered.cc` + `e2e_tiered`); the real CloudProxyBackend of P5 is described
> in [cloudproxy-backend.md](cloudproxy-backend.md), with the combined scenario
> accepted by `e2e_tiered_cloudproxy`.

## 1. Goals and Non-Goals

Goals (mapping to the requirements):

1. **Cold demotion**: when bucket data has gone unaccessed for a long time, or
   local space runs low, upload object data to public cloud and keep only
   metadata (a stub) locally;
2. **Transparent read-back**: when a demoted object is accessed, fetch it from
   the cloud and return it to the client, caching it back locally at the same time;
3. **Space fallback**: when local space cannot be obtained, do not cache — pass
   through only; the read path never fails because caching failed.

Non-goals (first phase):

- No object-level multi-replica/erasure coding; the cloud always holds exactly one copy;
- No transparent compression/encryption (relying on cloud-side SSE is enough);
- In-progress multipart parts do not participate in demotion (after completion
  they become ordinary objects and then enter the lifecycle).

The first-phase "no prefix-granularity policy" was lifted in P6 (roadmap §3.6):
`rules[]` gives a per-prefix `cold_after` by `bucket/key` glob (`never` pins),
see §5.1/§8.

## 2. Architectural Position: the Composite TieredBackend

docs/storage-backend.md §2 deliberately stops routing at bucket granularity and
reserves "object-level tiering is implemented as an overlay, without changing the
`IStorageBackend` interface". This design fulfills that reservation: a new
composite backend `type: tiered` (`src/storage/tiered/`) that is still an
ordinary `IStorageBackend` to L2, internally composing two existing backends:

```text
                        BucketRouter
                              │
                     ┌────────▼────────┐
                     │  TieredBackend  │  type: tiered
                     │  ┌───────────┐  │
  state machine /    │  │ TierIndex │  │  atime table + demote/
  policy / promote   │  └───────────┘  │  promote/GC tasks
                     └──┬───────────┬──┘
       local (hot tier) │           │ cloud (cold tier)
    ┌───────────────────▼─┐      ┌──▼─────────────────────┐
    │ ITierLocal          │      │ any IStorageBackend    │
    │  ├ LocalFsTierLocal │      │ (CloudProxy initially; │
    │  │  (localfs/xlocalfs)│    │  Memory in unit tests) │
    │  └ DuoStoreTierLocal│      └────────────────────────┘
    └─────────────────────┘
```

The coupling on the two sides is deliberately asymmetric:

- **The cloud side goes only through the `IStorageBackend` abstraction** —
  upload/download/delete are all standard put/get/delete, so the cloud can be
  CloudProxy, another localfs (for tests), or any future backend;
- **The local side goes through `tier::ITierLocal`**
  (`src/storage/tiered/tier_local.h`, P6): everything tiered needs from the hot
  tier is one narrow interface — read tier state, load/store access records, an
  upload snapshot, the two atomic commit primitives (stub / cache fill), space
  probes, a full enumeration, an optional block cache. Two implementations:
  `LocalFsTierLocal` (localfs/xlocalfs, sharing the `fs_util` disk layout — what
  §4 describes) and `DuoStoreTierLocal` (tier state inside the object record, a
  stub = record without extents, commits = CAS meta transactions; see
  [storage/tiered.md §11](../storage/tiered.md)). `local` may name a
  localfs/xlocalfs or a duostore backend; any other type is still a config error.

The configuration references the two existing backend instances by name;
`StorageRegistry::build` becomes two-phase (construct all leaf backends first,
then the composite backends), and circular references are treated as
configuration errors.

## 3. Object State Model

Each object is in one of three states, recorded in the sidecar `tier` field
(default `local`):

```text
             scanner deems cold: upload to cloud + stub-ify
   ┌────────────────────────────────────────────┐
   │                                            ▼
 local ◄──────────── PUT overwrite ──────────── remote
 (data local only)               (local stub only; data in cloud)
   ▲                                            │
   │ PUT overwrite                              │ GET promote (cache fill succeeded)
   │                                            ▼
   └──────────────────────────────────────────  cached
        (one copy local + one in cloud; when deemed cold again no upload is
         needed — verify, then stub-ify directly)
```

- `local`: an ordinary object, exactly matching the status quo (existing data is
  compatible with zero migration);
- `remote`: the local side is a **stub** = 0-length data file + extended
  sidecar; the data lives in the cloud;
- `cached`: one copy of the data locally and one in the cloud; the local copy is
  a cache — the **preferred victim** during space reclamation (stub-ified
  directly, zero upload traffic).

## 4. Disk and Metadata Layout

### 4.1 Stub: a 0-length file rather than a separate index

The stub keeps a 0-length data file as a placeholder, rather than deleting the
data file and building a separate index. Rationale:

- list (directory traversal), key/prefix conflict detection, delete's
  empty-directory cleanup, etc. are **all reused as-is**, with no "merge two
  views" code whatsoever;
- The rename atomic-commit semantics are preserved: stub-ification is just
  "rename a 0-length file over the data file", the same primitive as a PUT
  overwrite.

Cost: `load_meta` can no longer trust stat for the size. The sidecar gains a
`size` field; when `tier != local` the sidecar is authoritative (`local` objects
keep using stat, compatible with existing data).

### 4.2 Sidecar extension fields (TSV, backward-compatible append)

```text
etag            <original local etag, never changed by demotion>
content_type    ...
meta.*          ...
tier            remote | cached          # absent means local
size            <byte count, effective when tier != local>
remote.etag     <etag returned by the cloud>  # multipart objects get a different etag once uploaded; recorded separately
remote.at       <iso8601 upload time>
```

**Externally-visible ETag invariance**: the ETag the client sees is always the
original local value (including the multipart `-N` form). After an object is
uploaded to the cloud via a single-stream PUT its cloud etag changes; it is
stored only in `remote.etag` for verification and never leaked — demotion and
promotion are fully transparent to clients.

The cloud object also carries a redundant copy of the original meta (etag/
content_type/user_meta) in `x-amz-meta-lights3-*`; if the local stub is
accidentally lost, reconciliation can rebuild it (§9).

### 4.3 TierIndex: access records, the time wheel and the space ledger

- **Access records** (P6, roadmap §3.6 ⑤): one `AccessRec{atime, hits, enrolled}`
  per object, **persisted with the object** rather than held resident — the
  localfs side writes it into a second xattr on the data file
  (`user.lights3.access`; no fsync, travels with the inode, vanishes naturally
  when a PUT/stubbing swaps the inode and the mtime fallback takes over); the
  duostore side and filesystems without xattrs fall back to a resident table +
  `<state>/atime.tsv` snapshot (memory grows with the object count — a
  deliberate fallback). TieredBackend keeps only a **write-behind buffer**
  (`ikey → newest atime + hit delta`): GET/HEAD/PUT update it, and every 5 min
  or once it holds `access_buffer_max` keys (default 100k) it is flushed (read
  the stored record → merge → write back + wheel enrollment). A coldness verdict
  reads the buffer first, then the stored record, then the mtime fallback, so
  touches inside the flush window are never misjudged. A crash loses at most one
  flush period, affecting only precision; filesystem atime (relatime) is not used.
- **Time wheel** (roadmap §3.6 ①): append-only files `<state>/wheel/<slot>`
  bucketed by hour, one `bucket\tkey` line per enrollment; a key is enrolled in
  the slot its `atime + cold_after` falls into (`enrolled` remembers the slot, a
  later touch appends only when the slot changes). Due slots are consumed by the
  scan and deleted; keys still hot are re-enrolled further out. It is the sole
  source of coldness and eviction candidates; the full enumeration runs only every
  `full_scan_interval` (default 1d) as the safety net (untracked objects, crash
  recovery, quota calibration), see §5.1.
- **Space ledger**: real-time `statvfs` readings are authoritative (the local
  disk may be shared with other processes); an optional `quota_bytes` adds a
  logical quota on top (calibrated by the full scan, maintained incrementally by
  PUT/DELETE).

## 5. Demotion Flow (demote)

### 5.1 Triggers

Background **TierScanner**: triggered periodically by `TimerQueue` (default 1h)
→ posted to the thread pool to run the scan coroutine. A round first flushes the
access buffer, then produces candidates by mode (P6, roadmap §3.6 ①):

- **Incremental round (the norm)**: reads only the time-wheel slots that are due
  (`slot ≤ now/1h`) and re-checks each key's current access record — still-hot
  keys are re-enrolled further out, deleted keys dropped, cold ones demoted. The
  cost is proportional to **activity**, not to the object count;
- **Full round**: the first round after startup, then every `full_scan_interval`
  (default 1d; `0` = every round full, the previous behavior) —
  `ITierLocal::walk` enumerates every object: untracked ones are enrolled,
  "remote but data still present" crash residue gets its stub finished, the
  quota ledger is recalibrated, stale range-cache entries are swept.

Two trigger conditions:

1. **Cold detection**: `local`/`cached` objects with
   `now - atime ≥ cold_after(bucket, key)`. The threshold comes from `rules[]`
   first (`bucket/key` glob, first match wins, `never` pins: neither cold
   demotion nor watermark eviction), else the global `cold_after`;
2. **Space watermark**: when `statvfs` usage > `space_high_watermark` (default
   85%), reclaim until usage drops to `space_low_watermark` (default 70%).
   Candidates come from the wheel in ascending slot order (≈ LRU) and reading
   stops once the collected candidate bytes reach 4× the deficit; the sort key
   is `(rank, score)` — `cached` objects and range-cache residue of remote
   objects rank 0 (zero upload), `local` rank 1;
   `score = age × (1 + size_weight × log2(1 + size/1MiB)) / (1 + frequency_weight × hits)`,
   both weights 0 by default = pure LRU (roadmap §3.6 ③).

Concurrency is throttled by `core/semaphore.h` (`max_concurrent_transfers`,
default 4) to avoid saturating uplink bandwidth and the thread pool.

### 5.2 Per-object demotion steps

```text
Precondition check inside the per-key lock (§7): tier==local and no in-flight multipart
① open(data) for an fd snapshot; read the sidecar and record etag₀
② cloud.put_object(remote_bucket, key, meta + redundant headers, FdBodyReader(fd))
     —— streaming upload, no extra memory; on failure skip this object and retry
        next round after backoff
③ Verify: the etag returned by the cloud matches the local content (single-part =
     direct MD5 comparison; multipart objects instead recompute the MD5 during
     upload and verify the byte count independently of the sidecar)
④ Commit inside the per-key lock:
     a. Re-check sidecar etag == etag₀ (if a PUT overwrote it in the meantime,
        abort: the cloud copy goes into the GC queue)
     b. Write the new sidecar (tier=remote, size, remote.etag, remote.at)  ← tmp+rename
     c. rename(0-length tmp → data)                                       ← stub-ification commit point
```

Key semantics:

- **In-flight readers are unaffected**: step c is a rename-over; a reader in the
  middle of `pread` holds the old inode's fd and keeps reading the complete old
  data (naturally protected by the fd-snapshot semantics of
  docs/object-read-write-flow.md §3.2);
- **Crash between b and c**: the sidecar already says remote but the data file
  is still full — GET follows the sidecar `tier` and goes to the cloud
  (correct: the cloud copy exists); the data file's space is reclaimed next
  scanner round, which redoes step c for entries matching "tier=remote but
  stat size>0";
- **Any failure after ②**: at most one extra orphan cloud copy, idempotent —
  the next round re-uploads over the same key, or the reconciliation task
  cleans it up (§9).

`cached → remote` stub-ification performs only step ④ (verifying `remote.etag`
is still valid), with zero traffic.

## 6. Read Flow (GET/HEAD hitting remote)

### 6.1 HEAD / conditional requests

The stub's sidecar information is complete (size/etag/meta); HEAD and If-*
evaluation are **completed entirely locally**, never touching the cloud and
never triggering promotion.

### 6.2 Full GET: Tee pass-through + cache-as-you-download

```text
① cloud.get_object(remote_bucket, key, range=nullopt) → cloud stream
② Space precheck: statvfs free > size + min_free_bytes ?
     No → skip caching; the cloud stream is passed straight through as
          resp.stream_body (requirement 3)
③ Yes → wrap in a TeeBodyReader returned to L2:
     each read(): cloud stream → passed through toward the client, while writing
     the staging tmp + incremental MD5
     - Write failure (ENOSPC etc.): silently degrade to pure pass-through,
       delete the tmp; the client notices nothing
     - Client disconnect: the coroutine chain unwinds; TmpFile RAII discards the
       partial cache
     At EOF: MD5 == local etag (single-part) / byte count == size (multipart)
       → commit inside the per-key lock: rename(tmp → data) first, then write
         sidecar tier=cached
         (opposite order to stub-ification: on a crash in between, the sidecar
          is still remote and reads go to the cloud — correct; the full data
          file is reclaimed by the scanner under "remote but size>0")
       → verification fails: discard the tmp (cloud data anomaly; alert counter)
```

Under the Tee scheme, cache backfill costs **zero extra cloud traffic** (half
the traffic of "pass through once, pull again in the background"), and the
commit still goes through staging+rename, with failure modes exactly isomorphic
to PUT.

### 6.3 Range GET

By default Range requests pass the range straight through to the cloud
(`IStorageBackend::get_object` already takes a range); the response follows
docs/object-read-write-flow.md §3.1 as a normal 206. Configurable
`cache_fill_on_range` (default on): on a hit, submit a single-flight
whole-object promotion task to the background (independent cloud GET → cache
fill → commit as cached); likewise give up outright if space is insufficient.

**Block-level partial cache** (P6, roadmap §3.6 ⑦, `range_cache: true`, localfs
side only): a Range hit on a remote object first consults the block cache under
`<state>/rcache/` — a sparse data file of the object's length plus a bitmap file
of present blocks, bound to one replica by `remote.etag`. If every block is
present the range is served locally; otherwise the cloud is asked for the
**block-aligned superset** `[af, al]` and `RangeTeeReader` `pwrite`s bytes into
the sparse file as they arrive while handing only the client's window on; once
the client window is exhausted the tail (at most one block) is drained inside
the same `read()`, and the bitmap is merged at EOF. Local write failures degrade
to passthrough; concurrent fills of the same key are single-flight. PUT/DELETE/
complete overwrites, a whole-object fill commit, and the full scan finding the
object no longer remote (or the replica changed) drop the cache; watermark
eviction treats cache residue as a rank-0 victim (the file is simply removed).
`range_cache_block` defaults to 1MiB.

### 6.4 single-flight

Concurrent GETs on the same remote object: each request independently opens a
cloud stream and passes it through (no waiting on each other, best latency),
but **only one writes the cache to disk** — if the per-key in-flight table
already holds a tee/promotion task, later requests are pure pass-through. This
avoids N staging copies of the same object being written to disk concurrently.

## 7. Write/Delete Paths and Concurrency Control

### 7.1 PUT / multipart complete overwrite

Goes through the local backend's staging+rename as usual (write-back: new data
lands only locally, `tier` naturally returns to `local`, and the scanner decides
when to upload again). When overwriting a `remote`/`cached` object, the old
cloud copy becomes an orphan → after a successful commit, append
`(remote_bucket, key, remote.etag)` to the **GC queue** for asynchronous
deletion.

### 7.2 DELETE

Local deletion executes immediately (idempotent, current semantics); if the
sidecar is `remote`/`cached`, the cloud copy is likewise enqueued for GC. **The
client response does not wait for the cloud**: delete latency is unaffected by
cloud RTT; cloud-side failures are retried until success.

GC queue persistence: `<staging>/tier/gc/<seq>`, one TSV file per entry (written
via tmp+rename, unlinked after successful deletion), crash-safe; a background
task consumes it periodically with exponential backoff.

### 7.3 per-key locks

TieredBackend maintains striped mutexes (bucketed by key hash,
coroutine-aware async locks). **They protect only the state-commit section**
(the few metadata operations of sidecar+rename, microsecond-scale); data
movement (upload/download/tee) is all streamed outside the lock. Conflict
matrix:

| Contenders | Outcome |
| --- | --- |
| PUT vs demotion commit | Demotion commit fails the etag re-check → abandons the stub, cloud copy goes to GC; PUT wins |
| PUT vs cache commit | Cache commit re-checks that the sidecar is still remote with the etag unchanged, otherwise discards the tmp; PUT wins |
| DELETE vs either commit | Sidecar already gone at commit time → discard; DELETE wins |
| GET (old data) vs stub-ification | fd snapshot; finishes reading the old inode; no conflict |
| GET (open after rename) vs stub-ification | The fd is the new 0-length inode while the sidecar claims size>0 → localfs throws `StubRace`; tiered catches it, re-reads the tier, and goes to the cloud |
| Two cache backfills | single-flight guarantees only one |

Principle: **user write operations always beat background tasks**; the cost of
a failed background task is only wasted traffic plus one cloud copy awaiting
GC — never correctness.

## 8. Configuration

```yaml
backends:
  - name: localdata
    type: localfs
    root: ./data/objects
    staging: ./data/staging
  - name: aws
    type: cloudproxy                  # docs/cloudproxy-backend.md
    endpoint: https://s3.us-east-1.amazonaws.com
    bucket_prefix: lights3-tier-
    # cloud credentials...
  - name: tiered
    type: tiered
    local: localdata                  # must be localfs/xlocalfs
    cloud: aws                        # any backend
    cold_after: 30d                   # cold-detection threshold
    scan_interval: 1h
    space_high_watermark: 85%         # triggers space reclamation
    space_low_watermark: 70%          # reclamation target
    min_free_bytes: 1GiB              # minimum headroom required for cache backfill (requirement 3's "cannot obtain space" criterion)
    cache_fill_on_range: true         # whether a Range GET triggers background whole-object promotion
    max_concurrent_transfers: 4
    # quota_bytes: 500GiB             # optional logical quota, layered on top of statvfs
    gc_retry_base: 60s                # backoff base for failed GC entries (delay = base × 2^n, §9)
    gc_retry_cap: 1h                  # backoff cap
    reconcile_interval: 1d            # bidirectional reconciliation period (§9); 0 = off (everything stops when scan_interval=0)
    reconcile_orphans: rebuild        # cloud-orphan handling: rebuild (default, rebuild the stub) | delete
    # ---- P6 (roadmap §3.6) ----
    full_scan_interval: 1d            # full-enumeration safety net; 0 = every round full (old behavior)
    evict_size_weight: 0              # size weighting in the eviction score; 0 = ignore size
    evict_frequency_weight: 0         # access-frequency weighting; 0 = pure LRU
    access_buffer_max: 100000         # write-behind access buffer cap (keys); flush early when reached
    range_cache: false                # block-level partial cache for Range GETs (localfs side)
    range_cache_block: 1MiB           # block size [64KiB, 1GiB]
    rules:                            # prefix policies: bucket/key glob, first match wins
      - match: "archive-*/raw/*"
        cold_after: 7d
      - match: "archive-*/keep/*"
        cold_after: never             # pinned: never demoted, never evicted

buckets:
  default_backend: localdata
  rules:
    - match: "archive-*"              # the tiering policy is just bucket routing: point buckets you want tiered at tiered
      backend: tiered
```

The tiering on/off granularity = the bucket routing granularity; differences
below the bucket are expressed with `rules[]` (they apply with a duostore
`local` too); differences across instances can still be met by declaring
multiple tiered instances.

## 9. Failure Matrix and Reconciliation

| Failure | Behavior |
| --- | --- |
| Cloud unreachable (GET remote) | Pass through the mapped cloud error (aligned with cloudproxy: remote 5xx → 502/503 S3 error codes); local `local`/`cached` objects are entirely unaffected |
| Cloud unreachable (scanner/GC) | Skip this round; failed GC entries are rescheduled with exponential backoff (`attempts`/`retry_at` persisted in the entry TSV, not reset on restart; delay = `gc_retry_base` × 2^n clamped to `gc_retry_cap`); entries not yet due incur zero cloud access. A cold-detection backlog has no side effects |
| Crash after upload, stub not committed | Orphan cloud copy; idempotently overwritten next round or cleaned up by reconciliation |
| Crash halfway through the stub commit (between §5.2 b/c) | The sidecar is authoritative and reads go to the cloud; the data file is reclaimed next round |
| Cache backfill disconnect/ENOSPC | TmpFile discarded / degrade to pass-through; the client notices nothing |
| Local stub lost (accidental manual deletion) | The reconciliation tool rebuilds the sidecar from the cloud's `x-amz-meta-lights3-*` redundant headers |

**Reconciliation task** (implemented in the P4 wrap-up; `run_reconcile_once`
manual hook + an independent low-frequency timer, default daily): per bucket,
enumerate the cloud and local object sets and take a bidirectional diff —

- **Forward (in cloud, not local)**: by default rebuild the stub from the
  cloud's `x-amz-meta-lights3-*` redundant headers (original etag/content-type/
  user meta fully restored); with `reconcile_orphans: delete`, delete the cloud
  copy instead to reclaim storage cost. Objects without the redundant headers
  are treated as foreign (the remote bucket may be shared with other parties):
  alert and skip, never touch. **Three guards against false positives**: a
  GC-queue snapshot (copies pending deletion are not orphans — rebuilding would
  resurrect a just-DELETEd object), the inflight table (uploads in the middle of
  demotion are not orphans), and re-checking the local state + cloud etag inside
  the per-key lock before acting;
- **Stale cloud copies for objects locally back at local** (the lost-GC-entry
  shape): the full data is in hand locally, so deletion is always safe; deleted
  outright in both modes;
- **Reverse (locally remote, missing/etag-mismatched in the cloud)**: first
  re-check with a point-in-time HEAD (the listing snapshot races with concurrent
  demotion); once confirmed missing, alert and count (a data-loss signal), and
  **never silently delete the stub** — the object stays in listings for manual
  intervention; an invalidated cached reference only degrades to an alert (the
  data is still local and is re-uploaded at the next cold detection).

**Quarantine ledger** (P6, roadmap §3.6 ④): two kinds of findings —
`refs_missing` (a stub whose referenced cloud copy is gone) and `foreign` (a
cloud orphan without lights3 redundant headers) — enter the ledger under
`<state>/quarantine/` (one TSV each: kind/bucket/key/etag/first and last
sighting/count). **Only the first sighting logs at ERROR/WARN**; later rounds
just bump the count (DEBUG) instead of re-alerting. After a reconcile round
**runs to completion**, entries that did not reproduce this round are resolved
automatically (INFO). Operator entry point:
`lights3 tier quarantine list|forget|purge <backend> …`
([cli.md §2.4](cli.md)): `forget` drops the ledger entry only; `purge` targets
refs_missing — it HEADs the cloud once more and, if the copy is still gone,
deletes the dead local stub (acknowledged data loss; if the copy is back the
stub stays and the entry is dropped). The gauge
`lights3_tiered_quarantine_entries{kind}` shows the ledger size at all times. Local-tier
capacity has its own five callback gauges,
`lights3_tiered_local_{used,total,high_watermark,cached,quota}_bytes`
([monitoring.md](monitoring.md) "Tiered watermark", [storage/tiered.md](../storage/tiered.md) metric table).

## 10. Implementation Phases

| Phase | Content | Independently acceptable via | Status |
| --- | --- | --- | --- |
| P1 | Sidecar extension fields + stub read/write paths (GET/HEAD/List recognize tier), MemoryBackend on the cloud side; test hooks for manually triggering demotion/promotion | Backend consistency suite all green + tier state-machine unit tests | ✅ |
| P2 | TierScanner (cold detection + watermarks), TierIndex persistence, per-key locks and conflict-matrix tests | Concurrent PUT/GET/demotion stress with no dirty data | ✅ |
| P3 | Tee cache backfill + space-fallback degradation + single-flight | Disconnect/ENOSPC injection tests | ✅ |
| P4 | GC queue + reconciliation tool | Reconciliation converges after crash injection | ✅ Fully landed (reconciliation tool + GC exponential backoff wrapped up 2026-07-31; stub-loss rebuild / delete mode / anti-resurrection / reverse alert / backoff-recovery specials all green) |
| P5 | Integrate the real CloudProxyBackend (itself an independent feature, see docs/cloudproxy-backend.md) | End-to-end against public cloud | ✅ (`e2e_tiered_cloudproxy` two-instance composition) |
| P6 | roadmap §3.6: `ITierLocal` abstraction + duostore hot tier, xattr access records + time-wheel incremental scanning, prefix rules, multi-dimensional eviction score, reconcile quarantine, Range block cache | Dedicated unit tests for incremental rounds / rules / score / block cache / quarantine / duostore hot tier | ✅ (2026-09-02) |

P1–P4 depend on no cloud SDK at all; the `tiered` + `memory` combination gives
full coverage in CI — a direct dividend of the decision to couple the local
side to concrete types while the cloud side goes through the abstract
interface.
