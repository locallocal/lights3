# Multipart Across Gateways on Shared Storage: Audit and Gap-Closing Steps

> Status: **§4 ① write lease implemented (2026-09-08); ②③④ pending**. Chinese original: [../../storage/multi-gateway-multipart-design.md](../../storage/multi-gateway-multipart-design.md).
> The question: when several lights3 gateways point at the same shared storage,
> can the create / upload_part / complete / abort steps of one multipart upload
> land on **different gateways**? Short answer: only **duostore (redis / tikv
> meta + rados data)** and **cloudproxy** support it by design; the duostore
> combination had one data-corrupting gap (§3.2, write-side in-flight
> protection — closed by the write lease of §4 ①) and still has zero
> end-to-end tests. The steps to close them are in §4.
> The other backends are not multi-gateway designs; §2 goes through them.
>
> Related: [duostore-core.md](../../storage/duostore-core.md) §3 / §8 / §9,
> [duostore-data-rados-design.md](duostore-data-rados-design.md) §8.3,
> [duostore-meta-redis.md](../../storage/duostore-meta-redis.md) §8,
> [duostore-meta-tikv.md](../../storage/duostore-meta-tikv.md) §9,
> [localfs.md](../../storage/localfs.md) §7.

## 1. Scenario

"Shared storage + multiple gateways" means: N `lights3` processes (usually on
different hosts, behind a load balancer, no session affinity) mount the same
logical backend; the S3 client's multipart steps may be balanced to any
gateway. For this to hold, all of the following are required:

| Premise | Meaning |
| --- | --- |
| P1 shared, atomic metadata | upload / part records are visible to every gateway; the compound invariants of put_part / complete / abort are atomic across processes |
| P2 globally unique upload_id | an id minted by any gateway never collides with another's |
| P3 shared, readable part data | gateway B can read the part bytes gateway A landed (critical since complete is a zero-copy assembly) |
| P4 globally unique data-plane ids | chunk / pack id segments never overlap across gateways |
| P5 in-flight protection across gateways | data gateway A has landed but not yet committed is not treated as garbage by gateway B's GC / orphan scan |
| P6 single executor for background tasks | mpu_ttl expiry, GC and the orphan scan run on one instance, or coordinate |

## 2. Per-backend status

| Backend | Status | Evidence |
| --- | --- | --- |
| memory | n/a | process memory |
| localfs / xlocalfs | **not a multi-gateway design** | all part state lives under `<staging>/mpu/<upload_id>/` ([localfs.md](../../storage/localfs.md) §7) with no in-process upload registry, so it may "happen to work" on a shared POSIX filesystem (NFS); but the repository never treats a shared root as a supported deployment: rename / fsync / xattr semantics on NFS are unargued, `commit_lock` is an in-process per-key mutex (two gateways completing the same key both rename, last wins), `next_tmp_name` is pid + steady_clock + in-process sequence (can collide across hosts, caught by `O_EXCL` as an error), and the stale-upload sweep runs on every gateway (`remove_all` is idempotent, harmless) |
| tiered | same as localfs | all four multipart steps delegate to the local `LocalFsBackend` (`tiered_backend.cc:740-773`) |
| cloudproxy | **supported** | pure pass-through: the upload_id is the remote S3's, the gateway keeps no local state (`cloudproxy_backend.cc:1104-1200`); any gateway may handle any step |
| duostore + rocksdb / sqlite meta | impossible | local engines hold a file lock; single-process exclusive |
| duostore + redis / tikv meta + **fs data** | **unsupported** (and unrelated to multipart) | meta is shared but data sits on each gateway's local disk: chunks / packs written by A do not exist on B, so B's GET fails with a missing extent; B's GC `remove` on A's extents gets ENOENT, acks idempotently, and A's disk leaks forever; B's startup `abandon_stale_packs` can only probe local flocks and will mis-seal A's active pack. The docs explicitly list a shared root as a misconfiguration ([duostore-data-fs.md](../../storage/duostore-data-fs.md) §5). This combination is single-gateway only (shared meta buys meta-side HA, nothing more) |
| duostore + redis / tikv meta + **rados data** | **supported by design, with gaps** | see §3 |

## 3. duostore (redis / tikv + rados), premise by premise

### 3.1 Satisfied

| Premise | Status |
| --- | --- |
| P1 | `create_upload` / `put_part` / `complete_upload` / `abort_upload` are each one transaction ([duostore-core.md](../../storage/duostore-core.md) §3.1); redis runs them as server-side Lua scripts ([duostore-meta-redis.md](../../storage/duostore-meta-redis.md) §8), tikv as 2PC with write-write conflict retries ([duostore-meta-tikv.md](../../storage/duostore-meta-tikv.md) §9). Every cross-gateway race converges: the same part number uploaded concurrently from two gateways is last-write-wins with the loser entering the gcq; B aborts / completes while A is still pumping a part → A's `put_part` throws NoSuchUpload and `commit_or_discard` deletes the landed data; `UndeterminedCommit` deletes nothing and leaves it to the orphan scan |
| P2 | `multipart.cc:new_upload_id` = 128 random bits from `getentropy`, independent of the process |
| P3 | rados has no pack layer; every extent is a `kRados` object readable by every gateway within the same pool + namespace; complete is a pure meta assembly (§9), any gateway assembles, any gateway reads |
| P4 | `alloc_file_run` allocates segments through the shared meta (redis INCRBY / tikv counter RMW); covered by the unit tests `duostore_redis_multi_gateway_shared_meta` / `duostore_tikv_multi_gateway_shared_meta` |
| P6 | `try_gc_lease` makes the GC round and the orphan scan single-executor; mpu_ttl cleanup is step 1 of the GC round and follows the lease; non-designated gateways set `gc_enabled: false` |
| read side | the read lease ([duostore-core.md](../../storage/duostore-core.md) §8.5) already covers the cross-process pin-table problem for "A reads while B reclaims" |
| meta cache | part records are never cached; complete runs `invalidate_on_exit` locally, redis broadcasts over pub/sub, tikv relies on `meta_cache_ttl` bounded staleness (§7.1); nothing multipart-specific |

### 3.2 Gap G1 (closed): write-side in-flight protection was process-local

The forward pass of `run_orphan_scan_once` (`duostore_backend.cc`) used to
unlink a chunk when: **no refs + mtime older than `gc_grace` + no pin in this
process's pin table**. The code comment says it outright: "write-side pin
covers very long streaming PUTs, for which the mtime grace alone is
insufficient" — on a single gateway, uploads longer than `gc_grace` are
protected by the **write-side pin**. That pin (`ChunkPinHooks` / `write_pins_`)
is an in-process table just like the read-side one, but where the read side
had the read lease to move it onto the shared medium, the write side had
**no counterpart**.

Failure sequence across gateways (reproduced by phase 1 of the unit test
`duostore_orphan_scan_defers_to_peer_write_lease` with `read_lease: 0`):

1. Gateway A receives a large part (or a large PUT); `RadosChunkWriter` issues
   one `write_full` per `rados_chunk_size` (default 8 MiB), so the first
   chunk's mtime is the upload's start time;
2. the upload lasts longer than `gc_grace` (default 300 s; a 5 GiB part from a
   10 MB/s client takes 500 s);
3. gateway B (`gc_enabled: true`) runs an orphan scan: `scan_chunks` lists the
   whole namespace, A's first chunks have no refs (the part is not committed
   yet), their mtime is beyond the grace, B's pin table naturally has no entry
   → the `chunk_referenced` recheck still finds nothing → **deleted**;
4. A finishes pumping, `put_part` commits successfully with refs pointing at
   deleted objects; after complete the object exists, a GET over that range
   returns 500 (missing extent), and only `lights3 fsck` notices.

The trigger is "an orphan-scan round ∩ an in-flight write older than the
grace". The default `orphan_scan_interval: 1d` keeps the probability low, but
`lights3 duostore scan <backend>` can fire a round at any time. The gcq path
is unaffected (only once-referenced extents enter the gcq, and that path is
already gated by the read lease).

**Now**: the write lease of §4 ① is in place — the lease also carries the
oldest in-flight write start, and the orphan scan only unlinks unreferenced
chunks older than that floor. With `read_lease: 0` the operational rule
"`gc_grace` ≥ the longest expected part/object upload" still applies (the code
does not check it).

### 3.3 Gap G2: zero end-to-end verification

The existing multi-gateway tests stop at the IMetaStore layer (two
`RedisMetaStore` / `TikvMetaStore` instances sharing a prefix: unique
segments, CAS convergence, GC lease, read lease). There is **no** test with
two `DuoStoreBackend` instances sharing the same meta + data; the four
multipart steps have never been executed across instances; e2e
(`tests/e2e/run_e2e.sh`) and the compose profiles are all single-gateway.

### 3.4 Gap G3: nothing in docs or config carries it

- The premise table in [duostore-data-rados-design.md](duostore-data-rados-design.md)
  §8.3 has no row for multipart / write-side in-flight data;
  [duostore-core.md](../../storage/duostore-core.md) §9 says nothing about
  multiple gateways; `config/lights3.yaml` does not list `read_lease`, and the
  `gc_enabled` comment does not mention the write-side constraint.
- The **unsupported** `redis/tikv meta + fs data` combination starts without a
  warning; the misconfiguration surfaces only as failing cross-gateway GETs.

## 4. Steps to close the gaps

In dependency order; ① is the correctness prerequisite, ② and ③ make it
verifiable and operable, ④ is the misconfiguration guard.

### ① Write lease: move write-side in-flight protection onto the shared medium — implemented

Isomorphic to the read lease, reusing its publish/consume skeleton with no new
table (implementation details and the safety argument are collected in
[duostore-core.md §8.5](../../storage/duostore-core.md)):

1. **Register**: `put_object` / `upload_part` / `tier_commit_cached` construct a
   `WriteTicket` before `pump_body` (a ticket from `write_clock_`, a separate
   instance of the same `InFlightClock` the read side uses); it is released
   when the coroutine frame exits, after the commit or the discard.
   Registration precedes the first chunk landing, so the published value is
   necessarily older than the mtime of any in-flight chunk.
2. **Publish**: `lease_tick` publishes both floors. The interface changed from
   `publish_read_lease(owner, oldest_ms, ttl)` / `min_read_lease()` to
   `publish_lease(owner, LeaseInfo{oldest_read_ms, oldest_write_ms}, ttl)` /
   `min_lease() -> optional<LeaseInfo>` (`meta_store.h`); the redis value is
   `<read> <write>`, the tikv `'L'` table row `r<owner>` holds
   `<read>\0<expiry>\0<write>`. **Compatibility**: an old-format value (write
   field missing) marks that gateway's write floor unknown →
   `min_lease().oldest_write_ms` is nullopt and the consumer falls back to
   grace-only for that round (equal to the old behaviour, never worse).
   `publish_lease_once()` is the manual publish hook.
3. **Consume**: before enumerating, the orphan scan fetches `min_lease()` and
   sets `write_floor = oldest_write_ms − clamp(gc_grace, 1 s, 60 s)`; an
   unreferenced chunk becomes a candidate only when `mtime < write_floor`,
   otherwise it is counted in the new `skipped_leased` statistic (also in the
   admin JSON and the `lights3 duostore scan` log line). The margin absorbs
   OSD-vs-gateway clock offsets (rados object mtimes are stamped by the OSD)
   and coarse filesystem timestamps; the 1 s floor lets `gc_grace: 0` test
   setups tolerate a jiffy. A failed lease fetch → WARN + grace-only (as on the
   gcq path, never stall).
4. **Local engines**: `publish_lease` returns unsupported → the publisher
   stands down (previous behaviour); the write-side pin stays exact.
5. **Pack path untouched**: rados has no packs; the fs data plane is outside
   the multi-gateway matrix (§2).

Tests (landed with the implementation):

| Case | Coverage |
| --- | --- |
| `duostore_orphan_scan_defers_to_peer_write_lease` (test_duostore.cc) | two `DuoStoreBackend`s sharing one RocksMetaStore + a lease board + one chunk directory. Phase 1 with `read_lease: 0`: the peer scan deletes the in-flight PUT's chunk and the commit leaves `refs_missing` (reproduces the gap); phase 2 with the lease on: `skipped_leased ≥ 1`, the chunk survives, the object is readable from the peer; a genuine orphan older than the floor is still reclaimed |
| `duostore_redis_read_lease` / `duostore_tikv_read_lease` | field-wise min over both fields, TTL expiry, legacy value → write floor unknown |
| `duostore_redis_meta_cache_bounded_staleness` | the read floor is backdated by the cache TTL, the write floor is not |

Alternative evaluated: register each `alloc_file_run` segment in a shared
"in-flight ids" table with TTL renewal and have the orphan scan skip listed
ids — exact, but one extra shared write per allocation plus a renewal thread,
and `alloc_file_run` is a hot path; the write lease is one row per gateway,
one write per `read_lease` seconds, isomorphic to the existing skeleton. It wins.

### ② Tests: two backend instances sharing meta + data

Unit tests (`tests/unit/test_duostore_redis.cc` / `test_duostore_tikv.cc`,
SKIP without the external instance): use the existing injection constructor
(`duostore_backend.h` "For test injection: self-assembled meta/data") to build
two `DuoStoreBackend`s sharing **the same** `IMetaStore` target and **the same**
`IDataStore` object (without rados on the machine, one `FsDataStore` instance
shared by both sides — object-level sharing sidesteps the shared-root flock
misconfiguration and is enough to verify the backend orchestration layer):

| Case | Assertion |
| --- | --- |
| A create → B upload_part ×2 → A complete → B get | ETag = `combined_etag`, content byte-identical |
| B aborts while A is pumping a part | A's put_part throws NoSuchUpload, the chunks A landed are removed by `commit_or_discard`, the orphan scan finds no residue |
| same part number concurrently from A / B | the winner's content is readable, the loser's extents enter the gcq and are reclaimed by GC |
| **long write vs peer orphan scan** (G1 regression) | landed with ① (`duostore_orphan_scan_defers_to_peer_write_lease`) |
| mpu_ttl cleanup | only the lease holder aborts the expired upload; the other instance reports `uploads_expired=0` |
| list_parts / list_uploads across gateways | both gateways list the same set |

e2e: add a compose profile `multi` (two `lights3` + redis + rados behind a
round-robin nginx); `run_e2e.sh` runs a 5-part multipart with the aws cli and
checks the ETag. No docker daemon on this machine, so it goes to
[../../todo.md](../../todo.md) §2 pending verification.

### ③ Docs and config

- [duostore-data-rados-design.md](duostore-data-rados-design.md) §8.3 premise
  table: add rows "write-side in-flight vs peer orphan scan: write lease (§4 ①)"
  and "multipart across gateways: verified (§4 ②)";
  [duostore-core.md](../../storage/duostore-core.md) §9 gains a multi-gateway
  subsection, §8.5 is retitled "read / write leases".
- `config/lights3.yaml` duostore section: add a `read_lease: 5s` comment
  (mandatory with multiple gateways, 0 = off); the `gc_enabled` comment points
  at this document.
- [../deployment.md](../deployment.md) gains a "multi-gateway deployment"
  section: support matrix (§2), required configuration (`gc_enabled` on one
  instance, `read_lease` on, NTP, the `meta_cache_ttl` constraint), no load
  balancer affinity needed.

### ④ Misconfiguration guard

At the end of the `DuoStoreBackend` constructor, when meta is redis / tikv and
data is fs: `LOG_WARN("… shared meta with local fs data: single-gateway only,
objects written by other gateways are unreadable here")`; `--check-config`
prints the same. No hard rejection (shared meta on a single gateway is legal).

## 5. Out of scope

- Multi-gateway localfs / tiered over a shared filesystem: needs an NFS
  semantics argument plus cross-host per-key locking, duplicating duostore's
  proper path; explicitly not done, the docs stay "single gateway".
- Sharing the fs data plane across gateways (shared root): §2, a misconfiguration.
- Per-extent distributed pins: the read-lease evaluation applies unchanged
  ([duostore-data-rados-design.md](duostore-data-rados-design.md) §8.3
  candidate table); coarse leases suffice.
