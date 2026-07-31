# RadosDataStore: DuoStore Data Storage on Ceph/RADOS

> English translation of [../duostore-rados-data.md](../duostore-rados-data.md). The Chinese original is authoritative; section numbering matches.

> Status: C1-C4 implemented (`src/storage/duostore/rados_data_store.{h,cc}`,
> CMake option `LIGHTS3_DUOSTORE_RADOS_DATA`, all completed 2026-07-30, §12).
> Delivers the evolution promise of
> [duostore-backend.md](duostore-backend.md) §12: swap the data side to Ceph by
> implementing `IDataStore` (`src/storage/duostore/data_store.h`); the data
> plane moves from a single-node filesystem to a RADOS distributed object
> pool, with replication/EC, rebalancing on expansion, and self-healing all
> handled by Ceph. Client library: the librados **C API** (discovered as a
> system package, not a submodule, §9). In this document "the main document"
> means duostore-backend.md, and an unprefixed `§N` refers to sections of this
> document.

## 1. Goals and Non-Goals

| Goal | Description |
| --- | --- |
| Implement the full `IDataStore` interface | open_writer / open_reader / remove / rewrite_pack / close; the injected combination runs `run_backend_suite` all green (§11) |
| Distributed data plane | Redundancy (replication or EC), capacity scaling, and failure self-healing are all delegated to RADOS; the gateway side no longer manages disks |
| Greatly simplified layout | The fs version's dual chunk/pack paths converge to a **single rados-object path** (§3.3): no packs, no compaction, no torn tail, no directory fsync |
| DataRef extension point delivered | Add `Extent::Kind::kRados` with near-zero meta-layer changes (only: refs accounting treats kRados like kChunk, and alloc shares the id segments, §3.1) — direct validation of the design promise in the main document §3.1 |

Non-goals (explicitly declared):

- **Not managing the Ceph cluster itself**: pool creation, replica count / EC
  profile, quotas, and placement rules are deployment-side responsibilities;
  the backend only consumes an existing pool (§10);
- **No libradosstriper** (route B rejected with rationale in §2);
- No rados-level read cache/prefetch (evaluated in C3; conclusion and
  quantified rationale in §5);
- No distributed pin/lease for a multi-gateway data plane — running GC on a
  single instance (`gc_enabled`) is a deployment precondition; a complete
  scheme has been evaluated (C4, conclusion in §8.3).

## 2. Integration Route Selection (Investigation Conclusions)

Laying out every feasible route for "lights3 storing data on Ceph":

| Route | Approach | Trade-offs |
| --- | --- | --- |
| **A. librados direct (chosen)** | implement `IDataStore`, chunk → rados object | Closest fit to the interface: the DataRef/Extent model maps one-to-one onto rados objects, location info fully self-contained; single-object writes are atomic (§7.1), an ack means multi-replica durability (§4.3) — a stronger consistency model than the fs version; the dependency surface is a single C library |
| B. libradosstriper | the striper library does the slicing for us; one logical object name in and out | Slice location info is hidden inside the striper (xattrs on the first object), conflicting with the Extent/run encoding model — DataRef degenerates to a single name string, and Range locating plus multipart zero-copy concatenation (main document §8) all break; the striper has internal shared-lock overhead; low community maintenance. Ruled out |
| C. RGW (S3 gateway) in front | run RGW on Ceph; lights3 connects via the cloudproxy backend | Not an IDataStore — a whole S3 gateway layer of duplicated semantics (auth, multipart done twice), doubling latency and deployment surface. **Already usable today**: point cloudproxy at an RGW endpoint, zero code; as a duostore data plane it is pointless. Not part of this design, listed for deployment-choice reference |
| D. CephFS mount + FsDataStore | point root at a CephFS mount point, zero code | 100% reuse, but: the MDS becomes an extra metadata bottleneck (duostore already has its own meta — pure waste); mounting needs privileges (no sudo on this machine — falls at the first deployment step); the "single process owns root exclusively" premise is nominal at best on a shared fs; directory fsync / rename semantics are weakest on a network fs. Emergency route only; not designed, not tested |
| E. RBD block device + local fs | mount a block device and treat it as a local disk | Still needs the whole local-fs machinery, a single mount point with no sharing — combines the drawbacks of both A and D. Ruled out |

A is chosen. B/E are ruled out; C/D are "no-code deployment alternatives"
rather than implementations of this interface.

## 3. Data Model

### 3.1 Extent mapping

```cpp
// delivering on the existing "extensible: kRados…" comment in data_ref.h
enum class Kind : uint8_t { kChunk = 0, kPack = 1, kRados = 2 };
```

| Extent field | kRados semantics |
| --- | --- |
| file_id | rados object name = `c.<file_id:016x>`; allocated by `IMetaStore::alloc_file_id(kRados)`, segment mechanism carried over as-is (main document §4.5). In the implementation kRados **shares segments** with kChunk: the refs table accounts by bare file_id regardless of kind, and an independent counter would produce cross-kind id collisions when switching data engines (fs↔rados) on the same meta |
| offset | always 0 (the object is the slice; no intra-pack offset) |
| length | object byte count |
| crc32c | computed at write time as before; read-verification semantics in §5 |

The run encoding (main document §4.3) needs no change: the kind field is
already in the run header; file_ids of one write session are contiguous
(segments), so 650k chunks still compress into 1 run; `pack_offset` is always
0. multipart complete remains pure meta concatenation (main document §8) — a
kRados run is isomorphic to a kChunk run, and **the O(#parts) zero-data-
movement payoff is preserved intact**.

### 3.2 pool and namespace

- **pool** (`rados_pool`, required): the boundary of physical placement
  policy — replica count, EC profile, crush rule, and quotas are all decided
  at the pool level by the deployment side. This implementation works
  identically on replicated / EC pools (it only uses write_full / read /
  remove / stat; EC does not need the overwrite feature enabled, §4.1);
- **namespace** (`rados_namespace`, default empty): cheap logical isolation
  inside a pool — multiple backend instances / multiple test runs share one
  pool without seeing each other; its role corresponds to redis-meta's
  `redis_prefix` ([duostore-redis-meta.md](duostore-redis-meta.md) §2.1), and
  test isolation relies on it (§11).

### 3.3 A single path: pack aggregation dropped

The fs version introduced packs to dodge the inode/directory overhead of
massive small-file counts (main document §1). RADOS has no such problem: no
directory hierarchy, an object's metadata is one record in BlueStore's
embedded RocksDB, and a small object's cost = `min_alloc_size` alignment
(default 4KiB) + one metadata record — no aggregation needed. Therefore:

- **All objects (including ≤128KiB small ones) uniformly take the single
  "slice → rados object" path**; `pack_threshold` / `pack_max_size` /
  `pack_writers` / `pack_gc_ratio` are ignored under `data: rados` with a
  WARN (§10);
- `rewrite_pack` always returns `{}`. In practice it is never called: meta's
  `pack_stats()` never has pack records for kRados, the compaction candidate
  set is always empty, and the GC compaction path naturally never triggers;
- The dual-state logic of the main document §5.3 ("unknown length buffers as
  pack first, converts to chunk past the threshold") disappears — unknown-
  length streams and known-length streams share the same buffer-and-slice
  path (§4.2).

Rejected alternative: rebuilding packs on RADOS (appending to shared
objects). Three objections: appends on EC pools require stripe alignment
(`rados_ioctx_pool_requires_alignment2`), complicating the path; compaction
would need cross-object moves + `swap_extents`, racing with business writes
under multi-gateway; the gain is merely saving BlueStore's few-KiB per-object
overhead. Ruled out on complexity/benefit ratio.

### 3.4 chunk size

`rados_chunk_size` defaults to **8MiB**, the same value as the main document.
RADOS-side reference points: RGW stripes 4MiB, RBD objects 4MiB,
`osd_max_object_size` default 128MiB (config validation upper bound). Each
chunk = one write_full round trip: too small and RTT amplification dominates
throughput; too large and buffering memory (§4.2) plus single-write tail
latency rise. 8MiB sits in the comfortable range on both sides, and matches
the fs version's DataRef shape (the same object has a manifest of the same
order of magnitude in both data stores).

## 4. Write Path

### 4.1 DataWriter: slice buffering + write_full

| Scheme | Assessment |
| --- | --- |
| **Whole-chunk buffering + `rados_write_full` (chosen)** | Buffer up to chunk_size (or EOF) → one write_full produces one immutable object, same semantics as the fs version's "chunk is immutable once written"; RADOS single-object ops are atomic — **no torn chunks** (§7.1); write_full is idempotent (a replay converges to the same content); works on replicated / EC pools alike, EC needs no overwrite feature |
| Segmented streaming out via `rados_append` | Small buffers, but: EC pools require stripe alignment for all but the last segment; **an append with unknown outcome cannot be retried** (it might append twice = data corruption, the same shape as redis-meta §3.5's blind-retry ban); a crash leaves a half-written object needing tail-repair logic — precisely the torn-tail problem §3.3 just discarded, coming back from the dead. Ruled out |

### 4.2 Memory bound and backpressure

Each active writer holds 1-2 `chunk_size` buffers (one receiving + one
in-flight, the C3 double-buffer pipeline). The total is throttled by a
`core/semaphore.h` semaphore at **`rados_buffer_total` (default 256MiB)**: a
writer acquires its first quota **blockingly** on its first `write()`; when
the semaphore is exhausted it suspends via `co_await` — backpressure
propagates along the coroutine chain back to the socket read loop, so the
main document's "end-to-end streaming" is realized here as "**bounded
buffering**" rather than "zero buffering". The second quota (for pipelining)
is acquired only via non-blocking `try_acquire`: with every writer already
holding one quota, nested blocking waits would deadlock on each other;
failing to get it degrades to single-buffer serial operation (issue, then
wait for completion immediately), with correctness and backpressure semantics
unchanged — only the overlap is lost. On abandonment (destroyed without
finish), in-flight buffers and their quotas live on with the aio op until the
completion lands, so accounting always matches actually resident memory
(§6.2). Compare the existing fs-side precedent: the main document §5.3's
chunked buffering is likewise a coupled "buffer cap × concurrency"
declaration; this implementation merely generalizes it to the whole write
path and caps it explicitly with a semaphore.

Small objects (total length < chunk_size, including unknown-length streams
that never exceed the limit): the degenerate case of the same path — buffer
until EOF, one write_full of a single object, object length < chunk_size.

### 4.3 finish and durability semantics

What a librados write ack means: **all replicas (or the EC k+m stripes) have
been committed to durable storage** — the ack comes only after the BlueStore
transaction is on disk (since Luminous, ack means safe;
`rados_aio_wait_for_safe` is deprecated). Hence a write_full ack (aio
completion returning 0) is equivalent to the fs version's "fdatasync + shard
dir fsync", and stronger (multi-replica). There is no directory-fsync
counterpart to worry about. `finish()` = after collecting in-flight ops and
landing the tail-slice write_full, assemble and return the DataRef; destroyed
without finish = the objects already written become ownerless, cleaned up by
the caller's remove fallback or reclaimed by the orphan scan (§8.2) —
isomorphic to the fallback path of the main document §6.1 ⑤.

### 4.4 Failure handling and the retry boundary

- write_full is idempotent, and librados itself queues and retries internally
  against temporarily unreachable OSDs/PGs (no op timeout by default, §6.4) —
  **the application layer does no retry loop**;
- with `rados_op_timeout` configured, after -ETIMEDOUT returns the op may
  still take effect in flight (unknown outcome). Handling: the writer enters
  a failed state, the current request throws `InternalError`, and the writer
  is **neither retried nor reused**; already-written objects go through the
  remove fallback / orphan scan. Unknown outcomes only produce orphans, never
  wrong data — same reasoning as redis-meta §3.5's ban, but the cost here is
  lighter (the data plane has no accounting side effects).

## 5. Read Path

`open_reader(ref, first, last)` returns `RadosExtentReader : http::BodyReader`,
structured after the fs version's `ExtentChainReader` (main document §7):

- construction maps [first,last] to the starting run/extent, O(#runs),
  algorithm unchanged;
- `read(buf)`: after `co_await pool_->schedule()`,
  `rados_read(ioctx, oid, buf, min(buf.size, extent remainder), extent cursor)`
  (synchronous in C1, §6.2); when an extent is exhausted, advance to the next
  object. There is no fd concept — the fs version's "lazily open an fd"
  becomes "read by name each time", but **the GC race window is isomorphic**:
  the object may be removed by GC mid-read, the -ENOENT exposure matches the
  fs version's lazy open, and the pin table + gc_grace protection applies
  as-is (§8.1). Note the fs version's POSIX backstop — "an open fd is
  unaffected by unlink" — does not exist on rados: the pin table is promoted
  from "defense in depth" to "the only line of defense", with gc_grace still
  as the second line;
- single-object reads are linearizable (RADOS reads from the PG primary), no
  snapshot semantics needed — objects are immutable, so whatever is read is
  correct;
- `verify_chunk_crc` semantics carried over: verify crc32c only for extents
  "read completely from start to end"; a Range hitting the middle is not
  verified (same argument and default as the main document §7).

Each `read(buf)` is one RTT (aio from C3 on: issue then park, resume on
completion — pool threads no longer block on the network), with granularity =
the pump loop's buf size. The read amplification is acceptable (sequential
reads are partly masked by librados's internal pipelining).

**Object-level read-ahead evaluation (C3, conclusion: not implemented).**
Pre-issuing the aio for N+1 while reading N only removes the one
non-overlapped RTT at object boundaries — once per `rados_chunk_size`
(default 8MiB). Quantified: at RTT 1ms and single-stream 1GiB/s, the bubble
share ≈ 1/(8+1) ≈ 11%, and the larger the chunk the lower the share. The cost
is structural: each reader permanently holds ≥1 extra chunk_size buffer,
which must be accounted into `rados_buffer_total` to stay honest — reads and
writes competing for the quota makes GET latency depend on write concurrency
(a new coupling); early Range termination / abandoned reads turn the prefetch
into wasted reads; and GC pin semantics must also cover objects being
prefetched. By contrast, enlarging the HTTP pump buffer or `rados_chunk_size`
achieves the same direction of gain at lower cost. If cluster-environment e2e
measurements show object-boundary bubbles dominate, tune parameters first;
read-ahead remains a candidate on record, not entering the implementation.

## 6. librados Integration

### 6.1 C API and connection lifecycle

Use the **C API (`rados/librados.h`)** rather than the C++ API: the C ABI is
stable across versions; the C++ headers drag in ceph-internal types (boost
etc.) and their ABI drifts with versions — incompatible with the
"system-package discovery" integration approach (§9).

Construction sequence (executed at backend construction; failure means
construction failure — fail fast, config/environment errors surface at
startup):

```text
rados_create2(&cluster, "ceph", rados_client, 0)
→ rados_conf_read_file(cluster, rados_conf)        # ceph.conf + keyring
→ rados_conf_set(...)                              # client_mount_timeout / optional op timeout
→ rados_connect(cluster)                           # bounded by client_mount_timeout
→ rados_ioctx_create(cluster, rados_pool, &ioctx)
→ rados_ioctx_set_namespace(ioctx, rados_namespace)
```

A single `rados_t` + single `rados_ioctx_t` shared process-wide: both are
thread-safe for concurrent IO, provided the ioctx attributes (namespace /
locator / snap) never change after creation — constant in this
implementation, a declaration suffices. librados has its own internal
messenger network thread pool, so multiple pool threads issuing synchronous
ops concurrently are naturally parallel — no connection pool needed (contrast
hiredis pooling, §5.2 in redis-meta — that exists because a redis connection
is ordered and simplex; rados has no such constraint).

### 6.2 Threading model: aio bridging (implemented in C3)

C1 initially honored the Task<T> interface with "pool threads + synchronous
librados" (the minimal implementation within the evolution space reserved by
the main document §2.2); from C3 on, the data path fully switched to
`rados_aio_*` with zero interface changes — a purely internal replacement,
which is exactly the payoff of choosing Task<T> for IDataStore.

- **Discipline**: completion callbacks run on librados finisher threads;
  **only record the result and post the parked continuation back to this
  process's pool executor — never continue business logic on a ceph
  thread** — running business on ceph-internal threads is borrowing a thread
  for private work, and blocking it backpressures librados's internal
  pipeline. Implemented as the `AioPending` rendezvous (an atomic state
  machine: empty → waiter handle → done; callback and waiter meet via CAS,
  and whoever arrives second performs the resume/continuation);
- **Abandonment safety**: `AioPending` is heap-allocated with dual references
  (issuer / callback). The write-side pipeline allows "issue without waiting
  yet"; when a writer is destroyed without finish (discard semantics, §4.3),
  in-flight buffers and their semaphore quotas live on with the op until the
  callback lands — memory librados is reading stays valid, and quota
  accounting always matches actually resident memory;
- **Write-side double-buffer pipeline** (§4.2): at most 1 op in flight
  (extent order is naturally preserved); while writing slice N, keep
  receiving N+1. The second buffer quota is acquired via non-blocking
  `try_acquire` — with every writer already holding one, nested blocking
  waits would deadlock; failing to get it degrades to single-buffer serial
  (C1 behavior), backpressure semantics unchanged;
- **Windowed concurrent remove**: at most 16 in-flight aio removes per
  batch — GC of a large manifest (hundreds of thousands of extents for a
  TiB-scale object) does not pressure the cluster unboundedly;
- **Read side**: per-extent aio reads by name, issue then park; crc
  accumulation and business continuation on pool threads. Listing/stat
  (`scan_chunks`) has no aio version; being low-frequency (default 1/d) it
  blocks synchronously on a pool thread, matching the fs version's
  full-scan precedent.

### 6.3 Error mapping

librados returns negative errno values, uniformly routed through
`throw_rados(what, ret)` (modeled on the fs/rocks versions' `throw_status`:
LOG_ERROR + throw `s3::S3Error`):

| Source | Mapping |
| --- | --- |
| read -ENOENT while refs present (GET) | `InternalError`(500) + alert (sign of data loss — or pin/grace failure; same as main document §10) |
| remove -ENOENT | idempotently ignored (interface contract) |
| -ENOSPC / -EDQUOT (pool full / quota) | `InternalError`(500); already-produced objects go through the remove fallback (corresponds to the ENOSPC row of main document §10) |
| -ETIMEDOUT (with op timeout configured) | `InternalError`(500); writer enters failed state (§4.4) |
| rados_connect / ioctx_create failure | construction throws `std::runtime_error` (configuration-error class, startup-time failure) |
| other negative values (-EIO / -EPERM …) | `InternalError`(500) + error log (with the errno name) |

### 6.4 Timeouts

- connection: `client_mount_timeout` (via conf_set, default 5s;
  `rados_connect` failure means startup failure);
- op timeout defaults to **unset** (`rados_op_timeout: 0`): librados hanging
  and waiting for recovery against temporarily unreachable OSDs is the
  expected behavior of distributed storage — hanging beats false failures,
  and request-level duration judgment is left to the S3 client / HTTP layer;
  deployments needing a hard cap can set `rados_op_timeout` (mapped to
  `rados_osd_op_timeout`), at the cost of the unknown-outcome handling of
  §4.4 taking effect.

### 6.5 close

`close()`: wait for in-flight writes to wind down (`rados_aio_flush`,
including the completion callbacks — buffer/quota returns of abandoned
writers land with them; after this, ceph threads no longer touch store
members; the destructor fallback takes the same path) →
`rados_ioctx_destroy` → `rados_shutdown` (the last two performed by the final
Conn holder; in-flight reads are covered by the escaped reader's own Conn
reference). After close, any call throws `InternalError` cleanly (a guard,
modeled on the rocks version's `db()` / redis version §5.5 — defense in
depth: misuse becomes a 500 rather than a crash). The lifecycle hooks into
the existing order of the main document §9: backend close stops GC first,
then `data_->close()`, then `meta_->close()` — zero changes.

## 7. Consistency and Crash Model

### 7.1 The commit-point invariant holds as-is

The fundamental invariant of the main document §6 — **data lands first, meta
commits after** — checked item by item:

| Crash point (vs main document §6.2) | kRados consequence | Reclaim path |
| --- | --- | --- |
| data written, meta not committed | object in the pool, no refs record | orphan scan: no refs and mtime beyond grace → remove (§8.2) |
| chunk half-written | **no half-written object exists**: write_full is single-object atomic — either the whole new content is visible or none is | completed earlier objects become orphans, as above |
| after meta commit | everything consistent | old DataRef in gcq, GC as usual |

Two **strengthenings** over the fs version: no torn chunks (single-object op
atomicity); no "abandon active pack on restart" logic (no packs). All
pack-related rows of the fs version's crash matrix disappear.

### 7.2 Cluster-side failures

- OSD failure / network partition: librados blocks while the PG is degraded,
  requests hang (the trade-off of §6.4); RADOS replication is strongly
  consistent — **no "whole-store rollback" exists** — contrast redis-meta §6
  which must plug the durability window with AOF; the data plane has no
  corresponding risk on the Ceph side;
- monitor majority lost: new connections fail (startup failure), existing
  ops hang;
- gateway crash: objects already written by in-flight writers become orphans
  → reclaimed by the scan; no local state needs recovery (the fs version's
  segment-fsync caveat belongs to the meta side, irrelevant here).

### 7.3 The GC ordering iron rule

Main document §9.1 unchanged: gcq consumption performs `rados_remove` on
kRados extents (-ENOENT idempotent) → `ack_reclaim` settles after success —
the "physically delete first, settle after" argument holds as-is (a
reverse-order crash produces off-ledger orphan objects; the orphan scan can
catch them, but the iron rule is not relaxed because a backstop exists). The
"whole-pack delete when live_recs==0" branch never triggers (§3.3).

## 8. GC Details and Multi-Gateway

### 8.1 The pin table

The in-process pin table + `gc_grace` carry over from the main document §7.
As noted in §5: rados has no POSIX "open fd unaffected by unlink" backstop,
so the pin table is the read side's only line of defense — under
single-gateway deployment, in-process counting is fully correct (the same
argument as the main document).

### 8.2 Orphan scan (implemented in C4)

`scan_chunks` (interface finalized with mainline P4):
`rados_nobjects_list_open/next` (the ioctx is already namespace-scoped)
enumerates all objects of this instance → parse the file_id from the object
name (foreign objects not named `c.<016x>` are not ours — ignored, matching
the fs version's handling of non-*.chk files) → `rados_stat` for the mtime
(second precision suffices; grace judgments are minute-scale; a stat failure
= a concurrent-remove race, tolerated and skipped). Orphan determination
(refs back-check / grace / pin) is the business of the caller's
`run_orphan_scan_once`; the data plane only enumerates. Reverse
reconciliation (refs present but object missing) is as in the main document
§9.3: alert counter, never silently delete meta. Enumeration cost =
O(objects in the namespace); low-frequency (`orphan_scan_interval` default
1d) is acceptable.

Integration fix made while landing this: the backend's orphan-unlink extent
kind was previously hardcoded to kChunk, while the rados `remove` only
accepts kRados (foreign-kind extents are treated as data-engine-switch
leftovers and skipped) — the kind is now derived from `data_kind`, otherwise
rados orphan deletion would silently do nothing.

### 8.3 Multi-gateway combination

RedisMetaStore + RadosDataStore = the realization of "fully distributed
gateways" in the main document §12 combination matrix. Checking premises and
gaps item by item:

| Premise | Status |
| --- | --- |
| file_id globally unique | Satisfied: segment allocation on the shared meta (INCRBY, redis-meta §4) is naturally monotonic across gateways |
| meta transactions globally atomic | Satisfied: Lua scripts are server-side atomic (redis-meta §3.4) |
| read-side pin vs another gateway's GC | **Gap**: the pin table is per-process; during a long read on gateway A, gateway B's GC may remove the object → read -ENOENT. First-phase constraint + mitigation below |
| who runs GC / the orphan scan | **Must be a single instance** (configuration designates which gateway runs GC), otherwise concurrent compaction/scans trample each other |

Deployment constraint (carried by configuration since C4): **with multiple
gateways, GC runs only on the single designated instance, and `gc_grace` ≥
the longest expected GET duration** (pulling probabilistic correctness up to
engineering acceptability). Non-designated gateways set
**`gc_enabled: false`** (a duostore-wide key, main document §11): it only
stops the scheduling of the background GC worker and orphan scan; the manual
hooks (test/ops channel) remain; an INFO log at startup leaves a trace
against "forgot which box runs GC".

**Distributed pin scheme evaluation (C4, conclusion: not implemented for
now; preferred direction = lease-based)**:

| Candidate | Evaluation |
| --- | --- |
| Lease-based delayed deletion | GET start writes a file_id→deadline lease to shared media (a meta-side table or object xattrs); GC skips unexpired entries; extra-long reads renew. Cost = one extra shared write on the GET hot path; advantages = lock-free, crash self-healing (leases expire on their own), isomorphic to the existing gcq/grace/pin shapes (the in-process pin table moved to shared media). **Winning candidate** |
| `rados_lock_shared/exclusive` | Readers take shared locks, GC tries exclusive — strongest object-level correctness; but every GET adds lock+unlock, two RTTs; locks left by crashed readers are broken by timeout (small = hurts long reads, large = slows GC), and GC must try-lock candidates one by one, O(n) RTTs. Rejected |
| watch/notify broadcast pins | Notification fan-out = number of gateways, not objects (good); but reply aggregation is a homegrown protocol, an unresponsive gateway makes notify wait out its timeout (default 30s) slowing every GC round, and under partition "no reply" is indistinguishable from "not reading" — grace backstop still required on top. Rejected |

Implement the lease-based scheme when a real multi-gateway shared-data
deployment need arrives; for now the `gc_enabled` + grace constraints have
narrowed the risk to engineering acceptability. The main document §12 warning
"the pin table is no longer sufficient when meta is shared across gateways"
lands here as a concrete item.

## 9. Build Integration

### 9.1 Obtaining librados: system-package discovery, not a submodule

The Ceph repository cannot be submoduled: multi-GiB size, hour-scale builds,
and a dependency surface (bundled boost/fmt/…) risking conflict with this
repo's `~/.local/opt/boost-1.90` — the exact opposite of the "small and
self-contained" premise of rocksdb/hiredis. Ruled out. librados comes from:

1. system packages: `librados-dev` (deb) / `librados2-devel` (rpm);
2. a user prefix: in no-sudo environments, self-install to
   `~/.local/opt/ceph` (same precedent as this machine's Boost), pointed at
   by the CMake hint variable `LIGHTS3_RADOS_ROOT`.

Discovery order: `pkg_check_modules(RADOS rados)` first, falling back to
`find_path(rados/librados.h)` + `find_library(rados)` (with the
`LIGHTS3_RADOS_ROOT` prefix hint). Version requirements are loose: every API
used in this document (create2 / conf / write_full / read / remove / stat /
nobjects_list / set_namespace) has been available since Luminous (v12,
2017) — no version special-casing.

### 9.2 CMake

New option **`LIGHTS3_DUOSTORE_RADOS_DATA`, default OFF**, depending on
`LIGHTS3_DUOSTORE`. The rationale for default OFF matches redis-meta §7.2 and
is stronger: an external-service dependency (tests need a real cluster, §11),
and **this machine may not have librados at all** — default ON would chain
the main build to the system package being present. When OFF, configuring
`data: rados` throws "not compiled in" from `from_params`.

```cmake
if(LIGHTS3_DUOSTORE_RADOS_DATA)
    find_package(PkgConfig QUIET)
    # pkg-config first, falling back to find_path/find_library (LIGHTS3_RADOS_ROOT hint)…
    target_sources(lights3_core PRIVATE src/storage/duostore/rados_data_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_RADOS_DATA)
    target_link_libraries(lights3_core PRIVATE ${RADOS_LIBRARIES})
endif()
```

New files are only `rados_data_store.{h,cc}`; `DuoStoreBackend`, the meta
stores, and the S3 semantic layer are unchanged — yet another payoff of the
dual-interface decoupling of the main document §2.1/§3.1 (redis-meta swapped
the meta side without touching data; this document swaps the data side
without touching meta).

## 10. Configuration

New keys in `DuoStoreConfig::from_params` (YAML scalars automatically
collected into `BackendConfig.params`, zero parser changes, same convention
as the main document §11):

```yaml
backends:
  - name: duodata
    type: duostore
    root: ./data/duostore          # meta/ still lives under it; with data: rados, chunks/ packs/ are no longer produced
    data: rados                    # default fs
    rados_conf: /etc/ceph/ceph.conf
    rados_client: client.lights3   # cephx identity; keyring designated via ceph.conf
    rados_pool: lights3-data       # required; replication/EC policy decided at pool level by the deployment side
    rados_namespace: ""            # multi-instance/test isolation
    rados_chunk_size: 8MiB
    rados_buffer_total: 256MiB
    rados_connect_timeout: 5s
    rados_op_timeout: 0            # 0 = unset (default, §6.4)
    # meta-side keys (meta_path / meta: redis …) and GC keys (gc_* / mpu_ttl) unchanged
```

| Key | Default | Description |
| --- | --- | --- |
| data | `fs` | `fs` / `rados`; choosing rados without `LIGHTS3_DUOSTORE_RADOS_DATA` compiled in → configuration error |
| rados_conf | `/etc/ceph/ceph.conf` | ceph.conf path (with mon addresses and the keyring reference) |
| rados_client | `client.admin` | cephx user; production should use a dedicated least-privilege user (rwx on the pool) |
| rados_pool | — (required when data=rados) | data pool |
| rados_namespace | `""` | logical isolation inside the pool (§3.2) |
| rados_chunk_size | 8MiB | slicing granularity; validated ≤ osd_max_object_size (§3.4) |
| rados_buffer_total | 256MiB | total writer buffer quota = max concurrent streaming PUTs × chunk_size (§4.2) |
| rados_connect_timeout | 5s | connection timeout (client_mount_timeout) |
| rados_op_timeout | 0 | per-op hard timeout; when non-zero, mind the unknown-outcome semantics of §4.4 |

New GC key `gc_enabled` (a duostore-wide key, default true; ownership table
in the main document §11): set false on non-designated instances in
multi-gateway deployments, semantics in §8.3.

**Op metrics (C4)**: `RadosDataOptions` carries a `MetricsScope` (passed in
at backend assembly; direct test construction defaults to an empty scope,
i.e. an isolated instance), registered at construction so zero values are
visible: `lights3_duostore_rados_op_duration_seconds` (histogram, label
op=write_full/read/remove, from submission to the completion callback,
including the cluster round trip) and
`lights3_duostore_rados_op_errors_total` (counter; op additionally includes
scan; remove's idempotent -ENOENT does not count as an error).

Handling of `chunk_size` / `pack_*` / `verify_chunk_crc` under `data: rados`:
`verify_chunk_crc` is kept (semantics per §5); `chunk_size` is superseded by
`rados_chunk_size`, and `pack_*` are all ignored with a WARN (§3.3).
`DuoStoreConfig` gains a `data_kind` enum (the dual of redis-meta §8's
`meta_kind`); the `DuoStoreBackend` constructor branches on `data_kind`
(wrapped in `#ifdef`), the test injection constructor untouched.

## 11. Testing Strategy

1. **Obtaining a real cluster** (much heavier than redis — stated honestly):
   redis-server is a single-process single binary that tests can spin up on
   the spot; a Ceph cluster needs mon + osd + cephx keys, and even
   vstart/micro-osd depends on the full ceph binary set being present —
   **this machine (no sudo, no librados) always SKIPs**. Probe protocol: run
   only when the environment variables `LIGHTS3_TEST_RADOS_CONF` +
   `LIGHTS3_TEST_RADOS_POOL` are both set; otherwise SKIP explicitly with a
   printed reason (not a failure; same mechanism as redis-meta §9.2). The CI
   matrix fills the coverage with a containerized single-mon single-osd
   (ceph demo image);
2. **Isolation**: each run generates a unique `rados_namespace` (pid +
   counter, the redis_prefix technique); teardown enumerates and deletes all
   objects of this namespace — multiple test runs can share one pool;
3. **Combination and e2e**: injected construction
   `DuoStoreBackend(cfg, pool, RocksMetaStore, RadosDataStore)` runs
   `run_backend_suite` (the same semantic baseline as
   memory/localfs/duostore); `run_e2e.sh` adds a `duostore-rados` branch
   (same probe/skip), registered in CMake under
   `if(LIGHTS3_DUOSTORE_RADOS_DATA AND LIGHTS3_DRIVER_BUILTIN)`;
4. **rados specials**: multi-chunk large-object roundtrip and Range across
   extent boundaries; unknown-length streaming (one case each with EOF on
   either side of chunk_size); remove idempotence (double delete) and
   `run_gc_once()` convergence after overwrite/delete (objects gone, gcq
   empty); GC skipping while a concurrent GET holds a pin (the pin is the
   only line of defense, §8.1 — must-test); buffer semaphore backpressure
   (concurrent PUTs > quota count without deadlock, all completing); orphan
   paths (write objects without committing meta → scan reclaims; foreign
   names ignored; untouched within grace); the alert path for refs present
   but object missing (injected by manually deleting the rados object); C3
   double-buffer pipeline roundtrip (including the serial degradation at
   buffer_total = chunk_size); op metrics recorded. The `gc_enabled` gating
   and `try_acquire` semantics need no cluster and live in
   test_duostore.cc / test_concurrency.cc, always run.

## 12. Implementation Phasing

| Phase | Content | Independently verifiable by | Status |
| --- | --- | --- | --- |
| C1 | CMake option + librados discovery; the `kRados` enum and alloc wiring; connection lifecycle / error mapping / throw_rados; write path (slice buffering + write_full + semaphore), read path, and remove; test probe/skip mechanism | with a cluster present, the injected combination passes `run_backend_suite` all green; without one, all SKIP with no red | Done |
| C2 | unknown-length streaming wrap-up; full configuration (validation/WARN semantics); `e2e_duostore_rados`; rados-special unit tests (§11.4 except orphans) | e2e + specials green | Done (GC materialization / pin-race specials were completed along with mainline P3) |
| C3 | aio coroutine bridging (completion → executor reschedule) + double-buffer pipeline (receive N+1 while writing slice N); read-side object-level read-ahead evaluation | same suite all green + throughput comparison data | Done (§6.2 landing discipline and abandonment safety; read-ahead conclusion §5; the throughput comparison needs a real cluster, deferred to cluster-environment e2e — always SKIP on this machine, §11.1) |
| C4 | orphan scan (interface finalized with mainline P4); multi-gateway GC constraint landing (single-instance execution config) and distributed pin scheme evaluation (§8.3); metrics (op latency/error counts); doc status header update | orphan/reconciliation specials + full ctest matrix green | Done (scan_chunks §8.2; gc_enabled and the lease-based evaluation conclusion §8.3; op metrics §10) |

C1 already includes full read/write: the rados version has no incremental
steps like pack/GC compaction — the single path reaches usable in one step;
GC consumption (gcq → rados_remove) is carried by mainline P3's GC worker
(P3 already landed); the earlier transitional state of "deletion only
accounted, not reclaimed" matched the fs version's P1/P2 and is now history.
