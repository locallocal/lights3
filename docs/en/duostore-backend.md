# DuoStore: A Storage Engine Backend with Metadata/Data Separation

> English translation of [../duostore-backend.md](../duostore-backend.md). The Chinese original is authoritative; section numbering matches.

> Status: P1-P5 all complete (dual interfaces + full RocksMetaStore + chunk/pack
> data path + GC phases one and two (compaction/orphan scan/crash injection) +
> polish (tuning knobs exposed / corruption metrics / `e2e_tiered_duostore`);
> code in `src/storage/duostore/`). The metadata engine is RocksDB
> (`third_party/rocksdb` submodule, swappable with redis/sqlite/tikv); the data
> engine is the local filesystem (chunk slicing + pack aggregation + GC,
> swappable with rados). Implements the interface contract of
> [storage-backend.md](storage-backend.md) §1 and the extension guide of its §5;
> implementation phasing in §15.

## 1. Goals and Non-Goals

| Goal | Description |
| --- | --- |
| Implement the full `IStorageBackend` interface | bucket CRUD, object data plane, list, the complete multipart set (as defined by `src/storage/backend.h`); passes the backend consistency suite |
| Metadata/data separation | Internally split into two **pluggable** interfaces, IMetaStore / IDataStore; default RocksDB + local fs; meta now also supports redis/sqlite/tikv, data now also supports rados (§12) |
| Large-object slicing | Fixed-size chunks (default 8MiB), O(1) locating for Range reads; multipart complete with zero data movement (§8) |
| Small-object aggregation | Objects ≤ threshold (default 128KiB) are appended into append-only pack files, avoiding the inode/directory overhead of massive numbers of small files |
| GC | Garbage from delete/overwrite/abort is reclaimable: delayed chunk unlink, pack compaction by liveness ratio, orphan reconciliation (§9) |
| End-to-end streaming | GET/PUT never buffer a whole object; backpressure propagates (buffering upper bound for chunked PUT in §5.3) |

Non-goals (not in the first phase):

- content dedup / chunk sharing (a chunk always has exactly 0/1 references);
- group-commit aggregation for pack writes (one fdatasync per record, see §6.3);
- multiple processes/gateways sharing the same root (single-process exclusive,
  same premise as localfs);
- TransactionDB / distributed transactions for meta (compound invariants use an
  in-store mutex, §4.5);
- serving as the local side of tiered (§13.1);
- RocksDB compression (metadata volume is small; traded for zero external
  dependencies, §13.3).

## 2. Architecture and Route Decisions

```text
                 DuoStoreBackend : IStorageBackend
        (S3 semantics, ETag/MD5, validation, pump loop, GC worker)
                    │                                   │
  IMetaStore (sync, pool-thread calls)     IDataStore (coroutine Task<T>)
                    │                                   │
             RocksMetaStore                        FsDataStore
          (RocksDB WriteBatch)             (chunk files + pack files)
                    └───────────── DataRef ─────────────┘
          (value-semantic, serializable location info — the only
                    coupling point between the two sides)
```

### 2.1 Granularity of the internal interfaces: semantic level vs raw KV

| Route | Approach | Trade-offs |
| --- | --- | --- |
| A. Semantic level (chosen) | IMetaStore exposes S3 metadata transactions such as create_bucket / put_object / complete_upload | Transaction atomicity is expressed with each implementation's native primitives (RocksDB WriteBatch, SQL transactions, TiKV transactions); no "ordered KV" assumption leaks upward, so SQL/redis-style implementations are not awkward; the same-batch invariant of "GC accounting with metadata commit" is encapsulated inside each implementation |
| B. Raw KV level | IMetaStore only has get/put/scan/batch; S3 semantics written once in the layer above | S3 semantic code is written only once; but the implementation space is locked into ordered KV, and batch expressiveness cannot cover "read-modify-write + commit" style transactions |

A is chosen. The cost is that some S3 validation logic may be slightly
duplicated across implementations, minimized with shared helpers
(`storage/validate.cc`, `storage/multipart.h`).

### 2.2 Sync or coroutine: deliberately asymmetric

- **IMetaStore is synchronous** (contract: must be called on pool threads): the
  client APIs of the candidate implementations — RocksDB, SQLite, redis
  (hiredis), the TiKV client — are all synchronous and blocking; wrapping them
  in Task would only make every implementation write its own
  `co_await pool->schedule()` boilerplate. DuoStoreBackend switches to a pool
  thread uniformly at the entry point (same convention as localfs: validation on
  the caller thread → `co_await pool_->schedule()` → pool thread all the way
  after that), so compound meta operations complete in a single hop. Network
  meta making synchronous calls on pool threads is the same pattern as
  cloudproxy running synchronous httplib on pool threads
  ([concurrency.md](concurrency.md) §1).
- **IDataStore is coroutine-based** (Task<T>): the data plane must interleave
  streaming writes with the coroutine read loop of `http::BodyReader`; and the
  foreseeable alternative implementations (an io_uring version mirroring
  xlocalfs, the Ceph librados async API) are natively asynchronous — a
  synchronous interface would close off that path. Each implementation decides
  for itself whether to hop to pool threads internally.

## 3. The Dual Interfaces and DataRef

### 3.1 DataRef: the only coupling point between the two sides

```cpp
// src/storage/duostore/data_ref.h
struct Extent {
    enum class Kind : uint8_t { kChunk = 0, kPack = 1 };  // extensible: kRados…
    Kind kind;
    uint64_t file_id;   // chunk / pack file number (globally monotonic allocation, §4.5)
    uint64_t offset;    // payload start offset within a pack; always 0 for chunks
    uint64_t length;    // byte count of this extent
    uint32_t crc32c;    // checksum of this extent's content
};
struct DataRef {
    std::vector<Extent> extents;   // empty = 0-byte object; persisted via run encoding (§4.3)
    uint64_t total() const;        // Σ length
};
```

meta stores and returns DataRef as **opaque location info**; data only reads
and writes according to it. The `Kind` enum is the data plane's extension
point: the Ceph implementation adds `kRados` (file_id maps to a rados object
name) with zero changes to the meta layer (§12).

MD5/ETag are computed by DuoStoreBackend in the pump loop with
`util::HashStream` — hashing is S3 semantics and does not enter the data-plane
interface.

### 3.2 IMetaStore (synchronous, called on pool threads, errors throw `s3::S3Error`)

```cpp
struct ObjectRec {
    ObjectMeta meta;      // key/size/etag/content_type/last_modified/user_meta
    DataRef    data;
    uint64_t   version;   // +1 per write; optimistic check for GC compaction ref swap (§9.2)
};
struct UploadRec { std::string upload_id; ObjectMeta meta; int64_t initiated_ms; };
struct PartRec   { int part_no; uint64_t size; std::string etag;
                   int64_t modified_ms; DataRef data; };
struct Reclaim   { std::vector<Extent> extents; };   // pending physical reclaim
struct PackStat  { uint64_t pack_id; uint64_t file_size;
                   int64_t live_bytes; int64_t live_recs; bool sealed; };

struct IMetaStore {
    // bucket
    virtual void create_bucket(std::string_view b) = 0;   // exists → BucketAlreadyOwnedByYou
    virtual void delete_bucket(std::string_view b) = 0;   // missing → NoSuchBucket; non-empty → BucketNotEmpty
    virtual bool bucket_exists(std::string_view b) = 0;
    virtual std::vector<BucketInfo> list_buckets() = 0;

    // object: commit-type methods internally do "write new + old DataRef into the
    // GC ledger + reference/stat updates" in a single transaction
    virtual std::optional<ObjectRec> get_object(std::string_view b, std::string_view k) = 0;
    virtual void put_object(std::string_view b, std::string_view k, ObjectRec rec) = 0;
    virtual bool delete_object(std::string_view b, std::string_view k) = 0;  // returns false if missing (idempotent)
    virtual ListResult list_objects(std::string_view b, const ListOptions& opt) = 0;

    // multipart
    virtual std::string create_upload(std::string_view b, std::string_view k, ObjectMeta meta) = 0;
    virtual UploadRec require_upload(std::string_view b, std::string_view k,
                                     std::string_view id) = 0;               // missing → NoSuchUpload
    virtual void put_part(std::string_view b, std::string_view k, std::string_view id,
                          PartRec p) = 0;                                    // old same-number part enters GC ledger in the same batch
    virtual std::vector<PartRec> list_parts(std::string_view b, std::string_view k,
                                            std::string_view id) = 0;
    virtual std::vector<UploadInfo> list_uploads(std::string_view b) = 0;
    virtual std::string complete_upload(std::string_view b, std::string_view k,
                                        std::string_view id,
                                        std::span<const PartInfo> parts) = 0;  // §8
    virtual void abort_upload(std::string_view b, std::string_view k, std::string_view id) = 0;

    // resource allocation and GC accounting (§9)
    virtual uint64_t alloc_file_id(Extent::Kind kind) = 0;    // durably monotonic, segment reservation
    virtual std::vector<std::pair<uint64_t, Reclaim>> peek_reclaims(size_t max) = 0;
    virtual void ack_reclaim(uint64_t seq) = 0;               // settle after physical deletion succeeds
    virtual std::vector<PackStat> pack_stats() = 0;           // compaction candidates
    virtual bool swap_extents(std::string_view b, std::string_view k, uint64_t expect_version,
                              const DataRef& from, const DataRef& to) = 0;  // compaction ref swap
    virtual bool chunk_referenced(uint64_t file_id) = 0;      // orphan scan
    virtual void close() = 0;
};
```

### 3.3 IDataStore (coroutines)

```cpp
struct WriteHint { std::optional<uint64_t> content_length; };  // body.length(); nullopt when chunked

struct DataWriter {
    virtual Task<void> write(std::span<const std::byte> buf) = 0;
    virtual Task<DataRef> finish() = 0;   // returns location after persist+fsync; destroyed without finish = discard
    virtual ~DataWriter() = default;
};

struct IDataStore {
    virtual Task<std::unique_ptr<DataWriter>> open_writer(WriteHint hint) = 0;
    // [first,last] is the closed interval after resolve_range; returns a streaming
    // BodyReader (length() = last-first+1)
    virtual Task<std::unique_ptr<http::BodyReader>> open_reader(DataRef ref,
                                                    uint64_t first, uint64_t last) = 0;
    virtual Task<void> remove(std::span<const Extent> extents) = 0;  // idempotent (ENOENT ignored)
    virtual Task<GcRewrite> rewrite_pack(uint64_t pack_id) = 0;      // sequential compaction scan (§9.2)
    virtual Task<void> close() = 0;
};
```

## 4. RocksDB Metadata Model (RocksMetaStore)

### 4.1 Column family layout

| CF | key encoding | value | Notes |
| --- | --- | --- | --- |
| `default` | `"schema"` / `"instance"` | version / uuid | schema version verified on open |
| `buckets` | `<bucket>` | `{v1, created_ms}` | |
| `objects` | `<bucket>\0<key>` | ObjectVal (§4.3) | byte order = S3 lexicographic order, directly supports list |
| `uploads` | `<bucket>\0<key>\0<upload_id>` | `{v1, initiated_ms, content_type, user_meta}` | a prefix scan of `<bucket>\0` is naturally sorted by (key, upload_id) |
| `parts` | `<bucket>\0<key>\0<id>\0<be16 part_no>` | `{v1, size, md5, modified_ms, extent_runs}` | big-endian part_no guarantees ascending order; list_parts is just a prefix scan |
| `refs` | `<be64 chunk_file_id>` | brief owner description (for debugging) | chunk live-reference table; O(1) orphan determination |
| `gcq` | `<be64 seq>` | `{extents, reason, enqueue_ms}` | pending-reclaim queue |
| `stats` | `p<be64 pack_id>` / `c<kind>` | merge counters | pack liveness ledger (incremental merge of live_bytes/live_recs); file_id/seq segments |

Separator legality: `validate_object_key` already rejects keys containing NUL
in the shared validation layer (`src/storage/validate.cc`), and bucket names
are limited to `[a-z0-9.-]` — the `\0` separator is safe, no escape encoding
needed.

### 4.2 value encoding: hand-written little-endian binary

| Scheme | Assessment |
| --- | --- |
| Hand-written binary (chosen) | values are dominated by byte-exact integers (offset/length/crc); the extent array is tiny after run encoding; the on-disk format has zero third-party dependencies |
| JSON | easy to encode/decode, but for a 5TiB object (≈650k extents) manifest bloat and parse cost grow linearly with object size; the on-disk format would be tied to a third-party library (by convention nlohmann_json is used only inside admin/credential) |
| TSV (the sidecar convention) | unsuitable for nested arrays and not binary-safe |

The first byte is the version number. ObjectVal v1 layout:

```text
u8 ver | u64 size | u64 mtime_ms | u64 version | str etag | str content_type
| u16 n_meta | (str k, str v)* | u32 n_runs | run*        (str = u16 len + bytes)
```

v2 appends the first-class metadata section `u16 n_std | (str k, str v)*` after
n_meta; v3 (roadmap §3.6 ⑥) appends `u8 tier | str remote_etag | str remote_at`
— the object's state when duostore serves as a tiered hot tier (a stub is
tier=remote with no runs), see [storage/tiered.md §11](../storage/tiered.md).
Readers accept v1–v3, writers always emit v3.

### 4.3 extent run encoding

chunk file_ids allocated within one write session are contiguous (§4.5
segments), hence:

```text
run = { u8 kind, u64 first_file_id, u32 count,
        u64 chunk_len, u64 last_len, u64 pack_offset, u32 crc[count] }
```

The 650k chunks of a single PUT compress into 1 run (the 4B/chunk crc array is
still kept); after multipart complete, run count = O(number of parts).

### 4.4 list_objects: native ordered iteration

**Does not reuse `apply_listing` from `storage/listing.h`** — it requires
collecting the full sorted key set first, defeating the purpose of adopting an
ordered KV. Iterate directly on the `objects` CF:

- start: `Seek(bucket + '\0' + max(prefix, successor of start_after))`; when
  start_after hits itself, Next once more;
- termination: the key no longer has `bucket\0prefix` as a prefix, or max_keys
  are collected (fetch one extra to decide `is_truncated`; `next_token` = the
  last key, consistent with the semantics of the existing backends);
- delimiter="/": a key that contains "/" after stripping the prefix → grouped
  into common_prefix `p`, then **`Seek(last byte of p + 1)` skips the whole
  group** — delimiter listing complexity drops from O(keys in the bucket) to
  O(returned entries), the substantive advantage over localfs directory walks;
- iteration holds a fixed snapshot + `iterate_upper_bound`; each call sees a
  consistent view.

### 4.5 WriteBatch atomic transactions and mutual exclusion

All commit-type operations form a single WriteBatch inside RocksMetaStore; one
`Write(WriteOptions{sync = meta_sync}, &batch)` is the commit point:

| Operation | Same-batch contents |
| --- | --- |
| put_object | write objects + write refs for new chunks + old DataRef into gcq + delete old refs + negative stats merge |
| delete_object | delete objects + old DataRef into gcq + delete refs + negative stats merge |
| put_part (same-number retransmit) | write parts + new refs + old part into gcq + delete old refs |
| complete_upload | write objects + delete uploads/all parts + unselected parts into gcq + old same-name object into gcq + refs transfer + stats merge |
| abort_upload | delete uploads/parts + all parts into gcq + delete refs |
| GC settlement | delete gcq entries + delete refs (after physical unlink succeeds, §9.1) |

Cross-key compound invariants (bucket existence check + object commit,
delete_bucket emptiness check, complete validation + commit) are serialized by
**a single `std::mutex`** inside the store; pure reads (get/list, via
snapshot) take no lock. Note: read-modify-write transactions require the
commit (including the WAL fsync when meta_sync=true) to finish inside the lock
too, so write-path throughput is capped at ≈ 1/fsync-latency and RocksDB group
commit is defeated — P1 explicitly accepts this cost; the upgrade path once
contention becomes the bottleneck is RocksDB `TransactionDB` (not done, noted
only). delete_bucket's emptiness check covers both objects and uploads (an
in-progress multipart also yields BucketNotEmpty, matching AWS — otherwise
put_part could still write after the bucket is deleted, refs would leak
permanently, and recreating the bucket would resurrect ghost uploads).

`alloc_file_id` uses **segment reservation**: a single merge of +4096 on the
`stats` counter, then dispensed from memory, holding its own small lock (not
queued behind business commits' fsync); the reservation commit is **always
WAL-fsynced** (independent of meta_sync) — otherwise a crash that loses the
reservation would reissue already-used file_ids after restart, colliding via
O_EXCL with chunk files already on disk. Wasting a segment on crash is
harmless (file_id only needs to be unique and monotonic, not contiguous).

## 5. Data Layout (FsDataStore)

```text
<root>/
  meta/                             # RocksDB (meta_path may point it at an SSD separately)
  chunks/<ss>/<file_id:016x>.chk    # ss = low 8 bits of (file_id >> 8) in hex, 256 shard dirs
  packs/<ss>/<pack_id:016x>.pak     # consecutive ids share a dir per 256: dir fsyncs of one write session converge to 1-2
```

### 5.1 chunk (large-object slicing)

- `chunk_size` defaults to **8MiB**: sequential IO large enough to amortize
  open/seek overhead; a 5TiB max-size object ≈650k chunks still has a
  constant-scale manifest after run encoding; the same order of magnitude as
  mainstream object-store stripes. Fixed length (except the last chunk) makes
  Range→(chunk_idx, in-chunk offset) an O(1) division.
- A chunk file is **immutable** once written, and is referenced by exactly 0 or
  1 manifest (no dedup). `fdatasync(fd)` after writing; at session end,
  `fsync(dirfd)` on the shard directories involved (256 dirfds kept in a
  resident cache).

### 5.2 pack (small-object aggregation)

- Criterion: an object (or part) with length ≤ `pack_threshold` (default
  **128KiB**) goes into a pack, otherwise it takes the chunk path.
- Pack files are **append-only**, with record format:

```text
record := header || payload
header := magic u32 "LP3R" | u8 ver=1 | u8 flags | u16 header_len
        | u64 payload_len | u32 crc32c(payload)
        | u16 owner_len | owner ("bucket\0key" or "mpu\0<id>\0<part_no>")
Extent{ kind=kPack, file_id=pack_id, offset=payload start, length=payload_len }
```

  Purpose of the embedded owner: liveness back-lookup during GC compaction's
  sequential scan (§9.2) and offline salvage for disaster recovery, at a cost
  of a few dozen bytes per record. offset points at the payload, not the
  header — the hot read path never parses headers.
- **active pack rotation**: `pack_writers` (default 4) active packs coexist,
  each with a mutex and an append offset; writers poll for a lock and append
  (append = one `pwrite` of header+payload + `fdatasync`). Reaching
  `pack_max_size` (default **128MiB**) seals the pack and switches to a new
  pack_id. The premise that makes the simple lock model hold: payload ≤128KiB,
  the critical section is one small write + sync, and multiple writers
  amortize queueing.
- **Old active packs are not reused across restarts**: a new file is opened
  directly; a possible torn record at the old tail becomes dead space
  reclaimed by compaction — saving tail-validation repair logic at restart.

### 5.3 Streaming PUT of unknown length (chunked encoding)

`open_writer` receives `WriteHint.content_length` to pick the path; with
unknown length, the writer first **buffers in memory**: if the total stays
≤ pack_threshold, the whole thing is appended into a pack at EOF; once it
exceeds the threshold, a chunk file_id is allocated, the buffer is flushed to
disk, and the writer switches to the streaming chunk path. Memory cost upper
bound = `pack_threshold × max_inflight_requests` (default 128KiB × 1024 =
128MiB) — the two settings are coupled; watch the product when raising either.

## 6. Write Consistency and Crash Model

**Commit point = the RocksDB WriteBatch write (WAL). Data lands first, meta
commits after — a crash at any moment never produces "meta pointing at
nonexistent/incomplete data."**

### 6.1 Full PUT flow (upload_part identical)

```text
① caller thread: validate_bucket_name / validate_object_key
② co_await pool_->schedule(); bucket existence pre-check (the authoritative check
   is redone inside the commit transaction)
③ writer = co_await data_->open_writer({body.length()})
   loop: n = co_await body.read(buf); md5.update; co_await writer->write(...)
④ ref = co_await writer->finish()      # chunk: fdatasync + shard dir fsync
                                        # pack: record pwrite + fdatasync
⑤ meta_->put_object(...)               # WriteBatch commit point; old DataRef into gcq in the same batch
   if ⑤ throws: catch, then co_await data_->remove(ref) as best-effort cleanup;
   a failed cleanup is also harmless — falls to the orphan scan / dead space (§9)
```

### 6.2 Crash-window matrix

| Crash point | Consequence | Reclaim path |
| --- | --- | --- |
| between ③ and ④ / between ④ and ⑤ (chunk) | chunk on disk, no refs record | orphan scan: no refs and mtime beyond grace → unlink |
| ditto (pack) | record appended but no live accounting | natural dead space, automatically reclaimed at compaction, no scan needed |
| mid pack-append | torn record at the tail, unreferenced | restart abandons the active pack; dead space reclaimed with compaction |
| after ⑤ | everything consistent | old data is in gcq, GC proceeds as usual |

Overwrite (PUT to the same key): after the new data has fully landed, the
commit transaction atomically does "new rec takes effect + old DataRef into
gcq"; physical deletion **must be asynchronous and delayed** (serving
concurrent reads, §7), never a synchronous unlink on the PUT path.

### 6.3 fsync policy summary

| Plane | Policy |
| --- | --- |
| chunk | per-file `fdatasync` + shard-dir fsync |
| pack | `fdatasync` per record (a group-commit aggregation window is a non-goal) |
| meta | `meta_sync: true` (default) = WAL fsync per commit; when off, a crash loses the last few seconds of metadata yet stays self-consistent (the data becomes orphans and is reclaimed) — semantics aligned with the localfs status quo (rename without a disk barrier), trade off as needed |

## 7. GET Streaming Reads

`open_reader(ref, first, last)` returns `ExtentChainReader : http::BodyReader`:

- construction maps [first,last] to the starting run/extent: chunks within a
  run are fixed-length, `idx = (first − run_base) / chunk_len`, O(#runs)
  locating;
- `read(buf)`: **lazily opens** the current extent's fd (open + pread after
  `co_await pool_->schedule()`; the pool-thread-hop pattern matches localfs's
  `FdStreamReader`, which is not reused directly — that is single-fd-ownership
  semantics, this is a multi-file chain); when an extent is exhausted, the fd
  is closed and the reader advances to the next; `length()` returns
  last−first+1;
- pack extent: the whole payload (≤128KiB) is read in one go and **always
  crc32c-verified**, then sliced by range; chunk extents pread and stream
  directly, crc unverified by default (`verify_chunk_crc: false`); when
  enabled, only chunks "read completely from extent start to end" are
  verified — a Range hitting mid-chunk has nothing to verify against, and
  integrity responsibility lies mainly with the GC/reconciliation paths.

**Safety versus concurrent delete/GC**: POSIX already guarantees an open fd is
unaffected by unlink (same as the localfs fd snapshot); the risk is only in
**lazy opening**: after an object is DELETEd and GC unlinks immediately, the
reader opening the next chunk gets ENOENT.

| Scheme | Assessment |
| --- | --- |
| Pre-open all fds | tens of thousands of fds for a large object; infeasible |
| Delayed deletion only (grace) | simple, but a slow client reading a large object can exceed the window; only probabilistically correct |
| In-process reference counting (chosen) | `open_reader` registers all file_ids in the ref into a pin table (`unordered_map<file_id,int>` + mutex); reader destruction unregisters; GC skips files with pin>0 for the round. Single-process-exclusive root is an existing premise, so in-process counting is fully correct |

`gc_grace` (default 5m) is layered on top as defense in depth against
implementation defects. Pack compaction is equally safe for readers: old packs
are never modified in place (§9.2), an already-open fd reads the old inode,
and lazy opens are protected by pin + grace.

## 8. Multipart

- `create_multipart`: `new_upload_id()` (reusing `storage/multipart.h`) +
  write to the uploads CF;
- `upload_part`: the same data pipeline as PUT (a part goes to pack/chunk by
  its own size; regular ≥5MiB parts naturally all take chunks); PartRec
  records the part's own DataRef and MD5; a same-number retransmit = swap the
  record within the transaction, old part into gcq (last-write-wins);
- `complete_multipart`: **a pure metadata transaction, zero data movement** —
  after `validate_part_order` + per-item ETag comparison
  (`strip_etag_quotes`), concatenate each part's extent runs in order into the
  object DataRef, synthesize the overall ETag with `combined_etag()`, and
  commit in a single WriteBatch (§4.5). Contrast localfs's concatenating
  complete: **O(#parts) vs O(total bytes)** — the direct payoff of the
  manifest-based layout;
- `abort_multipart`: batch-delete uploads/parts + all parts into gcq; an
  unknown id throws NoSuchUpload (per the interface contract);
- expiry cleanup: the GC worker also scans the uploads CF; uploads initiated
  earlier than `mpu_ttl` (default 7d) go through an internal abort (the same
  mechanism as localfs's `kMpuTtl`).

## 9. GC

All accounting is produced in the same batch as business transactions (§4.5
table): the `gcq` pending-reclaim queue, the `stats` pack liveness ledger
(merge-operator increments, no read-modify-write), and the `refs` chunk
reference table.

Triggering: a background single-coroutine worker (`TimerQueue` period,
`gc_interval` default 5m) + **a public manual hook
`Task<GcStats> run_gc_once()`** (called directly by tests, following the
tiered P1 "manual sink trigger" precedent); the orphan scan is independent and
low-frequency (`orphan_scan_interval` default 1d) + the same style of hook. GC
shares `pool_` with business traffic, throttled via `core/semaphore.h`;
compaction copies proceed in slices so pool threads are not held for long.

Lifecycle: `DuoStoreBackend::close()` cancels the timers, waits for in-flight
GC coroutines to finish, then calls `data_->close()` (seals active packs) and
`meta_->close()` (clean RocksDB shutdown) in order; same close semantics as
tiered (must be called after in-flight requests complete).

### 9.1 gcq consumption (materializing deletions)

`peek_reclaims` takes a batch; for entries past `gc_grace` whose files carry
no pin (§7):

- chunk extent → `unlink` (ENOENT ignored idempotently) → on success
  `ack_reclaim` (same batch deletes gcq + refs). **Iron rule of ordering:
  physically delete first, settle the ledger after** — in the reverse order, a
  crash between deleting and settling produces permanently orphaned off-ledger
  files; in the correct order a crash merely leaves gcq residue, and retrying
  unlink is idempotent;
- pack extent → the liveness ledger was already decremented in the business
  transaction, so settle directly; along the way, a pack with
  `live_recs == 0` and sealed → unlink the whole file + delete its packstat.

### 9.2 pack compaction

`pack_stats()` picks packs that are sealed with
`live_bytes / file_size < pack_gc_ratio` (default 0.5) and live>0:

1. `rewrite_pack` **sequentially scans** all records (magic mismatch / bad crc
   → skip + alert, never delete silently, §10);
2. use the embedded owner to look back into meta: if the owner's current
   DataRef still points at this pack and offset → live, append the payload to
   an active pack;
3. `swap_extents(b, k, expect_version, from, to)` optimistically swaps the
   ref — a version or extent mismatch = overwritten/deleted in the meantime →
   abandon that entry (the new write does its own accounting);
4. after the full scan, the whole pack enters gcq (delayed unlink, serving
   in-flight readers).

"Sequential scan + embedded owner" was chosen over maintaining a pack→owner
reverse-index CF: GC is a low-frequency path, sequentially scanning 128MiB is
acceptable, and it saves an index that would have to be maintained in the same
batch as every business transaction.

### 9.3 Orphan scan (reconciliation)

- walk `chunks/`: `chunk_referenced(file_id) == false` and mtime beyond
  `gc_grace` → unlink;
- the reverse direction: refs present but file missing → **alert counter,
  never silently delete meta** (a sign of data loss, left for human
  intervention);
- packs need no orphan scan: an unaccounted record is dead space, naturally
  reclaimed by compaction.

## 10. Error Mapping

| Source | Mapping |
| --- | --- |
| RocksDB non-ok Status (IOError/Corruption/…) | `InternalError`(500) + error log (with the Status description) |
| RocksDB NotFound | not mapped directly — the semantic layer converts it to `NoSuchKey` / `NoSuchBucket` / `NoSuchUpload`(404) |
| bucket already exists / not empty | `BucketAlreadyOwnedByYou` / `BucketNotEmpty`(409) |
| part validation failure | `InvalidPart` / `InvalidArgument`(400), reusing the multipart.h helpers |
| Range unsatisfiable | `resolve_range` throws `InvalidRange`(416) (shared implementation) |
| pack record crc / magic error (GET) | `InternalError`(500) + corruption counter + alert |
| chunk open ENOENT while refs present (GET) | `InternalError`(500) + alert (sign of data loss) |
| GC compaction hits a corrupt record | skip + alert, keep the original pack undeleted (human intervention); the remaining live records migrate as usual |
| ENOSPC (data plane) | `InternalError`(500); already-produced data goes through remove cleanup |

## 11. Configuration

```yaml
backends:
  - name: duodata
    type: duostore
    root: ./data/duostore         # required; meta/ chunks/ packs/ all live under it
    # meta_path: /ssd/duo-meta    # optional: place the RocksDB directory separately
    chunk_size: 8MiB
    pack_threshold: 128KiB
    pack_max_size: 128MiB
    pack_writers: 4
    pack_gc_ratio: "0.5"
    gc_interval: 5m
    gc_grace: 5m
    orphan_scan_interval: 1d
    mpu_ttl: 7d
    meta_sync: true
    verify_chunk_crc: false
    rocksdb_block_cache: 64MiB

buckets:
  default_backend: duodata
```

All keys are YAML scalars automatically collected into `BackendConfig.params`;
no config-parser changes needed. `DuoStoreConfig::from_params(name, params)`
parses centrally with range validation (modeled on cloudproxy); `parse_size` /
`parse_duration_sec` are directly usable.

| Key | Default | Description |
| --- | --- | --- |
| root | required | data root directory |
| meta_path | `<root>/meta` | RocksDB directory |
| chunk_size | 8MiB | large-object slicing granularity |
| pack_threshold | 128KiB | ≤ this value goes into packs; also the buffering cap for chunked PUT (§5.3) |
| pack_max_size | 128MiB | active-pack sealing threshold (= compaction rewrite unit) |
| pack_writers | 4 | number of concurrent active packs |
| pack_gc_ratio | 0.5 | compaction triggers when the liveness ratio falls below this |
| gc_enabled | true | master switch for the background GC worker + orphan-scan scheduling; set false on non-designated instances in multi-gateway deployments (single-instance execution constraint, duostore-rados-data.md §8.3); manual hooks are not gated |
| gc_interval / gc_grace | 5m / 5m | reclaim period / delayed-deletion grace |
| read_lease | 5s | multi-gateway read-lease publish period (roadmap §3.7, storage/duostore-core.md §8.5): every gateway publishes its oldest in-flight read start time to the shared meta (redis/tikv); GC only reclaims entries every peer's in-flight read provably cannot reference; 0 = off; local engines (rocksdb/sqlite) stand the publisher down automatically at no cost |
| meta_cache_entries | 64K (rocksdb/sqlite) / 0 (redis/tikv) | object metadata cache budget (roadmap §3.8, storage/duostore-core.md §7.1): a GET/HEAD hit costs no meta round trip; 0 = off. Exact invalidation on local engines; shared engines need `meta_cache_ttl` to enable it |
| meta_cache_ttl | 0 (never) | cache entry expiry; on shared engines (redis/tikv) it must satisfy `0 < ttl < gc_grace` (a peer gateway's write stays invisible for up to one TTL; the published read lease is backdated by the TTL) |
| orphan_scan_interval | 1d | chunk orphan reconciliation period |
| mpu_ttl | 7d | expiry cleanup of incomplete multiparts; 0 = off (matching gc_interval's 0 semantics) |
| meta_sync | true | whether RocksDB commits are WAL-fsynced (§6.3) |
| verify_chunk_crc | false | chunk crc verification on the GET path (packs are always verified) |
| rocksdb_block_cache | 64MiB | RocksDB block cache capacity |
| rocksdb_write_buffer | 64MiB | memtable capacity per CF (P5 tuning exposure; default same as RocksDB) |
| rocksdb_max_write_buffers | 2 | max memtables per CF (≥1) |
| rocksdb_max_background_jobs | 2 | total flush/compaction background threads (≥1) |

## 12. Pluggable Evolution: Distributed Implementations

The design goal of the dual interfaces + DataRef decoupling is precisely to
let the two sides be replaced independently:

| Side | Implementation | Integration |
| --- | --- | --- |
| meta | redis / TiKV (meta shared across gateways; both implemented) | implement IMetaStore: the synchronous client is simply called on pool threads (§2.2); transaction invariants are expressed with each system's primitives (redis MULTI/Lua, TiKV transactions) — the semantic-level interface assumes no ordered KV, which is exactly why §2.1 chose A. Detailed Redis design in [duostore-redis-meta.md](duostore-redis-meta.md), detailed TiKV investigation in [duostore-tikv-meta.md](duostore-tikv-meta.md) |
| meta | SQLite (single-file deployment, implemented) | same as above, with SQL transactions. Detailed design in [duostore-sqlite-meta.md](duostore-sqlite-meta.md) |
| data | Ceph / RADOS (implemented) | implement IDataStore: add `Extent::Kind::kRados` (file_id maps to a rados object name), zero meta-layer changes; the librados async API fits the coroutine interface naturally. Detailed investigation in [duostore-rados-data.md](duostore-rados-data.md) |
| data | io_uring FsDataStore (not implemented) | following xlocalfs's approach: swap the data plane's IO engine, layout unchanged |

Combination matrix: RocksDB+local disk (first phase), TiKV+Ceph (fully
distributed gateways), and RocksDB+Ceph (local index + remote data) are all
valid combinations. Note: when meta is shared across gateways, the in-process
pin table of §7 is no longer sufficient — leases / distributed grace are
needed; listed as a design precondition of those implementations, not expanded
in this document.

## 13. Relationship to Existing Components and Build Integration

### 13.1 Component relationships

- **Cannot serve as tiered's local side**: `TieredBackend` does
  `dynamic_pointer_cast<LocalFsBackend>` on the local side; its
  stub/sidecar/fd-snapshot semantics are bound to the localfs disk layout,
  which duostore does not have. If support is wanted in the future, the proper
  way is to abstract a local-side interface for tiered — that is tiered's
  evolution, outside the scope of this design;
- **Can serve as tiered's cloud side**: the cloud side goes only through the
  `IStorageBackend` abstraction and works directly (P5 acceptance via the
  `e2e_tiered_duostore` combination);
- **bucket_router routes it normally**: a leaf backend; single-phase
  construction in the registry (not in tiered's deferred list);
- Reused: `validate_*`, the whole `multipart.h` set, `util::HashStream`,
  `resolve_range`, `core/semaphore.h`, `TimerQueue`; **not reused**:
  `apply_listing` (§4.4), `FdStreamReader` (§7), the fs_util sidecar system.

### 13.2 RocksDB submodule

`.gitmodules` adds `third_party/rocksdb` (`shallow = true`, the repository is
large); build.sh's `LIGHT_MODULES` adds rocksdb, **always init** — unlike
seastar's on-demand fetch: with compression fully off, RocksDB has zero
system-level dependencies and builds self-contained, so no `--xxx` switch for
lazy fetching is needed.

### 13.3 CMake presets (modeled on the gflags/seastar template)

```cmake
set(WITH_GFLAGS OFF CACHE BOOL "" FORCE)      # only rocksdb tools need gflags; the repo does not ship it
set(WITH_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_ALL_TESTS OFF CACHE BOOL "" FORCE)
set(WITH_BENCHMARK_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_CORE_TOOLS OFF CACHE BOOL "" FORCE)
set(WITH_TRACE_TOOLS OFF CACHE BOOL "" FORCE)
set(ROCKSDB_BUILD_SHARED OFF CACHE BOOL "" FORCE)
set(WITH_SNAPPY OFF CACHE BOOL "" FORCE)      # compression fully off: metadata volume is small, traded for zero external dependencies
set(WITH_LZ4 OFF CACHE BOOL "" FORCE)
set(WITH_ZLIB OFF CACHE BOOL "" FORCE)
set(WITH_ZSTD OFF CACHE BOOL "" FORCE)
set(WITH_LIBURING OFF CACHE BOOL "" FORCE)    # this machine has no liburing dev headers (same comment as seastar)
set(PORTABLE ON CACHE BOOL "" FORCE)          # do not use -march=native
set(USE_RTTI 1 CACHE BOOL "" FORCE)           # lights3 uses exceptions + RTTI throughout
set(FAIL_ON_WARNINGS OFF CACHE BOOL "" FORCE)
add_subdirectory(third_party/rocksdb EXCLUDE_FROM_ALL SYSTEM)
```

**`LIGHTS3_DUOSTORE` defaults to ON.** Comparison: default OFF (like seastar)
could save a few minutes of clean build, but the real reason seastar defaults
to OFF is heavy system-level dependencies (compiled Boost, ragel), which
RocksDB does not have; and default OFF would mean duostore's backend_suite/e2e
are absent from daily builds — the feature would inevitably rot. The trimming
template follows cloudproxy: inside `if(LIGHTS3_DUOSTORE)`, `target_sources`
(four .cc files) +
`target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE)` +
`target_link_libraries(lights3_core PRIVATE rocksdb)`; registration in
registry.cc is wrapped in `#ifdef LIGHTS3_DUOSTORE`.

## 14. Testing Strategy

1. **Consistency suite**: `tests/unit/test_storage.cc` adds suite cases under
   `#ifdef LIGHTS3_DUOSTORE`, building a DuoStoreBackend in a temp directory
   and running `run_backend_suite` — the same semantic baseline as
   memory/localfs/xlocalfs/tiered. **Three layout variants** all green on the
   same suite: default parameters (mixed), `pack_threshold` enlarged (forcing
   all-pack), `chunk_size` shrunk to 4KiB (forcing many-chunk manifests);
2. **Dedicated unit tests** (`tests/unit/test_duostore.cc`): key/value codec
   roundtrip (including run boundaries); list's delimiter Seek jumps and
   pagination tokens; pack record parsing / torn tail / crc corruption
   injection; after overwrite and delete, `run_gc_once()` → files gone,
   packstat zeroed, gcq emptied; compaction (low-liveness pack rewrite,
   concurrent overwrite during compaction triggering the version-check abandon
   path); GC skipping while a concurrent GET holds a pin; restart simulation
   (close backend → reopen the same root: active pack abandoned, orphan chunks
   reclaimed, id segments do not regress); mpu_ttl expiry abort;
3. **e2e**: `run_e2e.sh` adds a backend-type `duostore` branch (config block
   per §11); CMake registers `e2e_duostore` under
   `if(LIGHTS3_DUOSTORE AND LIGHTS3_DRIVER_BUILTIN)`; P5 adds
   `e2e_tiered_duostore` (duostore as tiered's cloud side, re-verifying
   §13.1).

## 15. Implementation Phasing

| Phase | Content | Independently verifiable by | Status |
| --- | --- | --- | --- |
| P1 | rocksdb submodule + CMake/build.sh integration; DataRef/encoding; the dual interfaces; full RocksMetaStore (bucket/object/list/multipart transactions); FsDataStore chunk path only (`pack_threshold=0`, everything via chunks); deletion only accounted, not reclaimed | `duostore_backend_suite` all green + `e2e_duostore` + codec/list specials | Done |
| P2 | pack aggregation: threshold decision (incl. chunked buffering), concurrent appends across multiple active packs, record format and crc, active-pack abandonment on restart | all-pack/mixed layout suite variants all green + record/torn-tail specials | Done |
| P3 | GC phase one: gcq consumption, chunk unlink and whole-pack deletion, pin counting + gc_grace, the `run_gc_once()` hook, mpu_ttl cleanup, background worker | GC convergence specials after overwrite/delete/abort + concurrent GET vs GC with no ENOENT | Done |
| P4 | GC phase two: pack compaction (sequential scan + owner back-lookup + swap_extents), orphan scan and reverse refs reconciliation alerts, crash injection (kill -9 restart convergence) | low-liveness compaction + crash-injection specials all green | Done |
| P5 | polish: RocksDB tuning exposure, s3/metrics metrics (corruption/GC counters), the `e2e_tiered_duostore` combination, doc status header update | full ctest matrix incl. the new e2e all green | Done |

P1 already includes multipart: `run_backend_suite` is a single-entry
full-semantics suite that cannot be bypassed; besides, duostore's complete is
pure meta concatenation anyway (§8), so multipart is actually a low-cost item
under this architecture. Deferring GC to P3 is safe: P1/P2's deletion
semantics are already correct (meta is the truth) — space is just not yet
reclaimed while accounting runs throughout; P3 only "materializes the old
ledger".
