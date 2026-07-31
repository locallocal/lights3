# Storage Backend

> English translation of [../storage-backend.md](../storage-backend.md). The Chinese original is authoritative; section numbering matches.

## 1. The IStorageBackend Interface

The sole boundary between L2 and storage. The interface is designed around S3
semantics rather than file semantics; everything returns `Task<T>`, and the data
plane goes through the streaming `BodyReader` (excerpt below):

```cpp
// src/storage/backend.h
namespace lights3::storage {

struct ObjectMeta {
    uint64_t    size = 0;
    std::string etag;                       // typically hex of the content MD5
    std::string content_type;
    std::chrono::system_clock::time_point last_modified;
    std::map<std::string,std::string> user_meta;   // x-amz-meta-*
};

struct ObjectStream {                       // returned by GET
    ObjectMeta meta;
    std::unique_ptr<http::BodyReader> body; // already trimmed to the range
};

struct PutResult { std::string etag; };

struct ListResult {
    std::vector<ObjectMeta /* includes key */> objects;
    std::vector<std::string> common_prefixes;
    bool is_truncated = false;
    std::string next_token;
};

struct IStorageBackend {
    // ---- bucket ----
    virtual Task<void> create_bucket(std::string_view bucket) = 0;
    virtual Task<void> delete_bucket(std::string_view bucket) = 0;   // must be empty
    virtual Task<bool> bucket_exists(std::string_view bucket) = 0;

    // ---- object data plane ----
    virtual Task<ObjectStream> get_object(std::string_view bucket,
                                          std::string_view key,
                                          std::optional<ByteRange> range) = 0;
    virtual Task<PutResult>    put_object(std::string_view bucket,
                                          std::string_view key,
                                          ObjectMeta meta,             // desired CT/user_meta
                                          http::BodyReader& body) = 0;
    virtual Task<ObjectMeta>   head_object(std::string_view bucket,
                                           std::string_view key) = 0;
    virtual Task<void>         delete_object(std::string_view bucket,
                                             std::string_view key) = 0;
    virtual Task<ListResult>   list_objects(std::string_view bucket,
                                            const ListOptions& opt) = 0;  // prefix/delimiter/max_keys/token

    // ---- multipart ----
    virtual Task<std::string>  create_multipart(std::string_view bucket,
                                                std::string_view key,
                                                ObjectMeta meta) = 0;      // → upload_id
    virtual Task<PutResult>    upload_part(std::string_view bucket, std::string_view key,
                                           std::string_view upload_id, int part_no,
                                           http::BodyReader& body) = 0;
    virtual Task<PutResult>    complete_multipart(std::string_view bucket, std::string_view key,
                                                  std::string_view upload_id,
                                                  std::span<const PartInfo> parts) = 0;
    virtual Task<void>         abort_multipart(std::string_view bucket, std::string_view key,
                                               std::string_view upload_id) = 0;

    virtual Task<void> close() = 0;    // flush/cleanup on graceful shutdown
    virtual ~IStorageBackend() = default;
};

} // namespace
```

(The snapshot is an excerpt: backend.h also has `list_buckets()` / `list_parts()` /
`list_multipart_uploads()`; `close()` has a default empty implementation rather
than being pure virtual.)

Error convention: backends throw `StorageError{S3ErrorCode, message}` (NoSuchKey,
NoSuchBucket, EntityTooLarge, ...); L2's errors module uniformly maps them to HTTP
responses. Backends are unaware of HTTP.

## 2. BucketRouter

```text
resolve(bucket) → IStorageBackend&
```

- Configuration-driven: glob matching in declaration order (`archive-*` →
  aws-archive); no match falls through to `default_backend`.
- Purely static, lock-free reads; config hot reload is out of scope for phase 1
  (takes effect on restart).
- ListBuckets semantics: aggregate each backend's `list_buckets` results,
  annotated with ownership.
- **Bucket granularity** is a deliberate choice: routing needs no metadata service
  and has no consistency issues. Object-level tiering/migration is an explicit
  later feature that can be layered on with "shadow bucket + replication job"
  without changing the interface.

## 3. LocalFsBackend (Local Filesystem)

### 3.1 Disk Layout

```text
<root>/
├── mybucket/                          # bucket = top-level directory
│   ├── .lights3-bucket                # bucket marker and attributes (creation time etc.)
│   ├── dir/a.bin                      # object data file; the key is the relative path
│   └── dir/a.bin.lights3-meta         # sidecar: TSV of ObjectMeta
<staging>/                             # same filesystem as root (rename atomicity)
├── put/<pid>-<ts>-<seq>               # temp file for an in-progress PUT
└── mpu/<upload_id>/
    ├── manifest                       # bucket/key/meta/creation time (TSV)
    └── part.00001 ... part.NNNNN      # each part has a part.NNNNN.md5 sidecar alongside
```

Key decisions:

- **key → path mapping**: `/` maps directly to directory separators on disk,
  keeping things human-readable and workable with ordinary tools. Escape handling:
  keys with a segment >255B, containing `.`/`..`, or with empty segments are all
  rejected in the shared validation layer (`src/storage/validate.cc`), so all
  backends behave consistently.
  Key/directory conflicts (PUT `a` when `a/b` already exists) return an
  S3-compatible error.
- **Sidecar rather than xattr**: xattrs have size limits and are easily lost by
  scp/rsync; sidecar TSV is reliable and inspectable. Listing filters out
  sidecars by suffix.
- **Write atomicity**: PUT writes entirely to a staging temp file (computing MD5
  as the ETag while writing; SHA256 verification belongs to L2's signature
  decorator, not the backend), then `rename()`s to the final path once
  verification passes (meta first, then data; the read side treats data as
  authoritative); every failure path unlinks the temp file. Concurrent PUTs on
  the same key use last-write-wins (consistent with S3 semantics); rename
  atomicity guarantees readers never see partial data.

### 3.2 Implementation Notes per Operation

- All posix calls execute after `co_await pool.schedule()`
  (see [concurrency.md](concurrency.md) §3).
- **GET**: open + fstat + read sidecar; `FdBodyReader` performs `pread` through
  the pool on every `read()` (with offset, naturally supporting Range); the fd is
  held by RAII and closed automatically on cancellation/disconnect.
- **PUT**: loop `body.read(64KiB)` → in-pool write + incremental MD5 → rename.
  ETag = MD5 hex, matching S3 single-part upload.
- **LIST**: recursive directory walk + prefix pruning (when the prefix contains
  `/`, jump straight to the starting directory); with delimiter=`/`, a directory
  is a common prefix and its contents need not be expanded — naturally efficient.
  Pagination token = last key returned (directory order is lexicographic order;
  the traversal must be a sorted traversal).
  No index in phase 1; optimizations for very large buckets (e.g. per-directory
  caching) are left for later.
- **Multipart**: parts land in `staging/mpu/<id>/part.N`; complete concatenates
  the parts in order into a final temp file and then renames (computing the total
  ETag along the way: `md5(concatenated part MD5s)-N`, per S3 rules); abort
  deletes the directory. On startup, the mpu directory is scanned to clean up
  expired (default 7 days) orphan uploads.

### 3.3 XLocalFsBackend (xlocalfs, io_uring data-plane variant)

Disk layout, metadata, and multipart logic are **fully reused** from
LocalFsBackend (inheritance + shared `fs_util` on-disk primitives); only the
data-plane byte shoveling is swapped for io_uring:

- **Wrapper**: `storage/xlocalfs/uring.h` implements a minimal wrapper using raw
  syscalls (io_uring_setup/enter + mmap SQ/CQ), introducing no liburing dependency.
  Single ring: a submit-side mutex serializes SQE filling and enters one by one
  (no SQ backlog); a dedicated reaper thread waits for CQEs and, on completion,
  posts the coroutine continuation back to the thread pool — no thread is occupied
  while waiting on disk, and subsequent synchronous on-disk calls (rename/sidecar)
  naturally return to pool threads.
- **Coverage**: GET streaming read (`UringBodyReader`, with offset naturally
  supporting Range), PUT/UploadPart streaming write, complete's part
  concatenation. Directory traversal and metadata operations still go through the
  thread pool (io_uring has no directory primitives such as getdents).
- **Configuration**: `type: xlocalfs`, parameters same as localfs (root/staging),
  plus optional `queue_depth` (SQ depth, default 256).
- **Lifecycle**: `close()` stops the reaper thread; must be called after in-flight
  requests complete (same assumption as ThreadPool::join).

## 4. CloudProxyBackend (Mapping to Public Cloud)

Maps local buckets to public-cloud object storage (AWS S3 / S3-compatible OSS,
COS, MinIO, etc.); the gateway acts as a proxy with local authentication. Full
design in [cloudproxy-backend.md](cloudproxy-backend.md); this section retains
the overview and route decision.

### 4.1 Two Implementation Routes

| Route | Approach | Trade-offs |
| --- | --- | --- |
| A. SDK wrapper | Call the remote via aws-sdk-cpp (or the lightweight aws-c-s3) | Correctness taken care of: retries, region, TLS, multipart all come ready-made; the SDK's synchronous API plugs into the coroutine model by calling it on the thread pool |
| B. Direct forwarding (decided) | Construct HTTP requests ourselves + SigV4-sign against the remote, forward via an HTTP client | Zero SDK dependency, truly streaming forwarding; but retries and per-cloud differences must be handled ourselves |

Early design leaned toward route A; **the detailed design phase reversed to route B**
(rationale in docs/cloudproxy-backend.md §2.1): outbound signing
`SigV4Authenticator::sign()` was implemented long ago alongside verification and
reserved for cloudproxy; the vendored httplib has streaming client capability;
and aws-sdk-cpp's dependency footprint conflicts with this project's
"fully vendored submodules" build constraint.

Key points (expanded in the corresponding sections of docs/cloudproxy-backend.md):

- **Credential isolation**: clients authenticate with the gateway's local AK/SK;
  the gateway accesses the remote with its own cloud credentials. Client
  credentials are never passed through; cloud credentials exist only in the
  gateway configuration.
- **Streaming**: in the GET direction, a pump thread + bounded queue turns
  httplib's push model into the `BodyReader` pull model; the PUT direction is the
  symmetric inverse. Avoids whole-object buffering (docs/cloudproxy-backend.md §3).
- **Multipart pass-through**: upload_id and parts map directly to the remote's
  same-named concepts; the gateway never lands parts on disk.
- **Timeouts and retries**: connect/request timeouts, exponential backoff 3 times;
  remote 5xx maps to the S3 error codes corresponding to gateway 500/503, remote
  4xx semantics pass through (NoSuchKey etc.) (docs/cloudproxy-backend.md §5).
- **Name mapping**: `bucket_prefix` resolves conflicts between local bucket names
  and the remote's global namespace; keys are not transformed.
- Thread occupancy: a synchronous HTTP client holds a thread for the entire
  request duration — the data-plane pump uses cloudproxy-private pump threads
  rather than the shared pool (docs/cloudproxy-backend.md §2.3).
  An independent mechanism is the generic per-backend `io_threads` pool, landed
  as a generic key configurable on any backend
  (see [concurrency.md](concurrency.md) §3.1).

## 5. DuoStoreBackend (Metadata/Data-Split Engine)

A storage engine internally split into two pluggable implementations — metadata
(IMetaStore) and data (IDataStore) — with DataRef as the sole coupling point:
by default metadata uses RocksDB (submodule) and data uses the local filesystem —
large objects are sliced into fixed-length chunks, small objects are aggregated
into append-only packs, and deletes/overwrites are reclaimed by GC (deferred
unlink + pack compaction + orphan reconciliation; P1-P5 all implemented).
Multipart complete is pure metadata concatenation (O(#parts), zero data
movement). Full design in [duostore-backend.md](duostore-backend.md).

Both the meta and data sides already have optional replacement implementations
(each with its own document; compile switches default to OFF):

- meta: Redis ([duostore-redis-meta.md](duostore-redis-meta.md)),
  SQLite ([duostore-sqlite-meta.md](duostore-sqlite-meta.md)),
  TiKV ([duostore-tikv-meta.md](duostore-tikv-meta.md));
- data: Ceph/RADOS ([duostore-rados-data.md](duostore-rados-data.md)).

Note: duostore cannot serve as tiered's local side (tiered is bound to the
localfs disk layout); it can serve as its cloud side or stand alone.

## 6. Steps to Add a Backend (Extension Guide)

1. Implement `IStorageBackend` (place it in `src/storage/<name>/`).
2. Register it in `registry.cc`'s `ensure_registered()` by calling
   `StorageRegistry::register_backend("<type>", factory)`; the factory signature is
   `(const BackendConfig&, shared_ptr<ThreadPool>, MetricsScope)
   → shared_ptr<IStorageBackend>`.
3. Backend-level metrics (optional, todo.md §3.1): the `MetricsScope` the factory
   receives already carries the `backend=<name>` base label; pass it through to
   the backend constructor and claim instances at construction time
   (`scope.counter/gauge/histogram/gauge_callback`, `with()` derives
   sub-dimensions); hot-path increments are lock-free, and `GET /-/metrics`
   appends the output automatically. If not consuming metrics, simply ignore the
   parameter; tests constructing backends directly pass a default empty scope —
   counts land on isolated instances with no registry wiring needed.
4. Reference it via `backends[].type` in the config; accept it through the generic
   **backend conformance test suite** (the same set of cases runs parameterized
   against all backends: CRUD, range, list pagination, multipart, concurrent PUT
   on the same key, abnormal keys).
5. Generic key `io_threads` (optional, todo.md §3.2): configuring it on any
   backend gives it a dedicated IO thread pool rather than the shared global pool
   (the Registry injects it per the parameter before calling the factory; factory
   and backend are unaware) — an isolation lever for when a slow backend (cloud)
   saturates the shared pool and starves fast backends (local disk); see
   [concurrency.md](concurrency.md) §3.1.
