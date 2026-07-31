# Object Read/Write Flow

> English translation of [../object-read-write-flow.md](../object-read-write-flow.md). The Chinese original is authoritative; section numbering matches.

This document follows the perspective of "one byte traveling from the socket to
disk (write), then from disk back to the socket (read)", threading together the
actual code paths of the HTTP Adapter layer (L1), the S3 Protocol layer (L2),
and the Storage layer (L3). Layer responsibilities are in
[architecture.md](architecture.md); per-layer internals are in
[http-adapter.md](http-adapter.md)/[storage-backend.md](storage-backend.md)/[s3-protocol.md](s3-protocol.md).

There is exactly one core abstraction running through this document: the
**`http::BodyReader` streaming pull interface** (`src/http/model.h`). Both
request and response bodies are passed between the three layers as one;
`read(span) → byte count (0 = EOF)`, so uploads/downloads of objects of any
size never land in memory as a whole (except MemoryBackend, see §3.4).

## 1. Common Front Chain (shared by reads and writes)

Taking the builtin driver (`src/http/drivers/builtin/builtin_server.cc`) as the
example:

```text
socket byte stream
  │ ① the driver parses the request line/headers and builds the neutral HttpRequest
  │    the body is not pre-read: wrapped as SocketBodyReader (fixed Content-Length
  │    or chunked de-framing)
  ▼
S3Service::dispatch()                       src/s3/service.cc
  │ ② generate x-amz-request-id; metrics counting starts here
  │ ③ /-/healthz /-/metrics /-/readyz /-/admin/* short-circuit return
  │ ④ SigV4 verification auth_.verify(req)   src/s3/auth/sigv4.cc
  │    when payload verification is needed, req.body gets one more wrapper (see §2.1)
  │ ⑤ resolve_address(): virtual-host or path-style resolves (bucket, key)
  │    buckets starting with '.' are internal reserved names, uniformly rejected
  │    (docs/credential-management.md §4.1)
  │ ⑥ per-credential policy authorization: cred_store->authorize(ak, bucket, is_write)
  │    (GET/HEAD count as reads, everything else as writes; CopyObject/UploadPartCopy
  │    additionally run one read authorization against the source bucket,
  │    see docs/credential-management.md §10.4)
  ▼
S3Service::route()                          explicit dispatch table (docs/s3-protocol.md §2)
  │ ⑦ first reject unsupported subresources (?acl etc. → 501)
  │ ⑧ match a table entry by (method, scope, query-flag) → concrete handler coroutine
  ▼
object handler                              src/s3/handlers/objects.cc
  ▼
BucketRouter::resolve(bucket)               src/storage/bucket_router.cc
  │ ⑨ pick a backend by glob rules (fnmatch); no match falls to default
  ▼
IStorageBackend                             src/storage/backend.h
```

The whole chain is one `Task<HttpResponse>` coroutine chain; synchronous
drivers (builtin/httplib) `sync_wait` on the driver thread, asynchronous
drivers hang it on their own event loop (docs/concurrency.md).
`dispatch()` catches uniformly: `S3Error` → corresponding status code + error
XML, any other exception → 500 `InternalError`; finally it adds the
`x-amz-request-id`/`Server` headers and prints one access-log line.

Object-level routing table entries (the data-plane part of `kRoutes` in
`service.cc`):

| Request | handler |
| --- | --- |
| `PUT /b/k` (no `x-amz-copy-source`) | `put_object` |
| `PUT /b/k` + `x-amz-copy-source` | `copy_object` |
| `GET /b/k` | `get_object(head_only=false)` |
| `HEAD /b/k` | `get_object(head_only=true)` |
| `DELETE /b/k` | `delete_object` |
| `PUT /b/k?partNumber&uploadId` | `upload_part` (multipart, see §2.4) |
| `PUT /b/k?partNumber&uploadId` + `x-amz-copy-source` | the copy branch of `upload_part` (UploadPartCopy, `multipart.cc:upload_part`) |

## 2. Write Path (PutObject)

### 2.1 The Three Wrapping Layers of the Request Body

The `BodyReader` that reaches the backend may be a nested onion, from the
inside out:

1. **Driver layer**: `SocketBodyReader` — reads a fixed-length body from the
   connection buffer, or does HTTP chunked de-framing;
   `Expect: 100-continue` is answered lazily at the first `read()`, so an
   authentication failure can reject without receiving the body.
2. **Auth layer** (`sigv4.cc`, optional wrapper depending on the value of
   `x-amz-content-sha256`):
   - hex digest → `Sha256VerifyingReader`: passes data through while streaming
     the SHA256; on EOF, a mismatch throws `XAmzContentSHA256Mismatch`;
   - `STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]` → `ChunkedSigV4BodyReader`:
     aws-chunked de-framing + per-chunk signature-chain verification;
   - `STREAMING-UNSIGNED-PAYLOAD-TRAILER` → de-framing only;
   - `UNSIGNED-PAYLOAD` → no wrapper.
3. **The backend** only faces the outermost reader and just loops `read()`,
   completely oblivious to all of the above.

### 2.2 Handler: Conditional PUT and Metadata Extraction

`S3Service::put_object` (`objects.cc:put_object`):

1. `router_.resolve(bucket)` picks the backend;
2. conditional requests (docs/s3-protocol.md §6): `If-None-Match: *` first
   probes existence with `head_object`, 412 if present (prevents
   overwrite-on-create); `If-Match: <etag>` compares against the current ETag
   (optimistic concurrency), 404 if the object is missing;
3. `meta_from_headers()` (`handlers/common.h`) extracts `Content-Type` and the
   `x-amz-meta-*` user metadata;
4. `backend.put_object(bucket, key, meta, body)`; if there is no body, an empty
   `StringBodyReader` serves as the fallback;
5. the response carries only the `ETag` header, no body.

### 2.3 LocalFs Backend: Staging + Atomic Commit

`LocalFsBackend::put_object` (`localfs_backend.cc:put_object`):

```text
validate bucket/key legality, reject reserved names (.lights3-meta suffix / bucket marker)
co_await pool_->schedule()          ← switch to the disk IO thread pool; blocking IO from here on
require_bucket()                    ← no marker → NoSuchBucket

① stream-write to staging:
   tmp = <staging>/put/<pid>-<ts>-<seq>   (O_CREAT|O_EXCL)
   loop: body.read(64KiB) → md5.update → write(tmp)
   —— MD5 is computed while writing; the object never resides in memory as a whole

② commit_object_file()              fs_util.cc:commit_object_file, the atomic-commit primitive
   create_directories(parent dirs)  failure → key conflicts with an existing object path (InvalidArgument)
   target is a directory → key conflicts with an existing prefix (InvalidArgument)
   write the sidecar first: <data>.lights3-meta (suffix fs_util.h:kSidecarSuffix;
   TSV: etag/content_type/meta.*, itself also tmp+rename)
   then rename(tmp → final path)    ← the commit point; on rename failure, roll back and delete the sidecar
```

Key conventions:

- **ETag = MD5 of the whole content** (hex, stored without quotes; quotes are
  added uniformly at the exits).
- **The sidecar lands before the data file**: the moment the data file appears,
  complete metadata is already readable — the read side never sees a
  "data without meta" window; when an old object is overwritten, the sidecar
  being replaced first only means the meta briefly runs ahead.
- `TmpFile` is RAII: if any step throws (including a client disconnect making
  `body.read` throw), the destructor removes the staging residue; only after
  `committed = true` is it exempt from deletion.
- Client disconnects are thrown upward by the driver-layer reader as exceptions
  (contract in docs/http-adapter.md §4); the whole coroutine chain unwinds and
  no half-written object is produced — the file at the final path is either the
  old one or the new one.

### 2.4 Variants

- **XLocalFs** (`xlocalfs_backend.cc:put_object`): same staging/commit path,
  only the data-plane `write` is replaced with io_uring (`drain_to_tmp()`:
  `body.read` → `uring_->write`); completion continuations are resumed via the
  thread pool, and no thread is occupied while flushing to disk; the commit
  still runs synchronously back on a pool thread.
- **Memory** (`memory_backend.cc:put_object`): first, without holding the lock,
  streams the whole body into a `std::string` while computing the MD5, then
  locks and inserts into the map (the object resides wholly in memory — mainly
  for testing).
- **CopyObject** (`objects.cc:copy_object`): a server-side pipe — the source
  backend's `get_object()` stream is used directly as the body of the target
  backend's `put_object()`; cross-backend copies are likewise free of
  whole-object buffering; `x-amz-copy-source-if-*` is validated before copying.
- **Multipart** (details in docs/storage-backend.md §3.2 and
  docs/s3-protocol.md §1): `upload_part` is fully isomorphic to PUT
  (staged streaming write + per-part MD5, write `part.NNNNN.md5` first then
  rename the data file; re-uploading the same part number is last-write-wins);
  `complete_multipart` verifies each part's ETag, concatenates them into a new
  tmp in the declared order, the total ETag = `md5(concatenation of the parts'
  binary md5s)-N`, and finally goes through the same `commit_object_file`
  atomic commit.

## 3. Read Path (GetObject / HeadObject)

### 3.1 Handler: Range and Conditional Requests

`S3Service::get_object` (`objects.cc:get_object`):

1. parse `Range: bytes=a-b / a- / -n`; a malformed Range is **ignored** rather
   than an error (S3 behavior); multi-range is unsupported and likewise treated
   as no Range;
2. **HEAD**: `backend.head_object()` fetches metadata only → conditional
   request evaluation (`If-Match`/`If-Unmodified-Since` → 412,
   `If-None-Match`/`If-Modified-Since` → 304, precedence per RFC 7232) → fill
   `ETag`/`Content-Type`/`Last-Modified`/`x-amz-meta-*`,
   `content_length = size` but no body;
3. **GET**: `backend.get_object(bucket, key, range)` returns
   `ObjectStream{meta, body, range}` (`storage/backend.h:ObjectStream`); the
   body is already trimmed to the range, and `range` is the actual effective
   closed interval after resolution;
4. the conditional evaluation happens after obtaining the stream (on 304 just
   drop the stream);
5. Range hit → `206` + `Content-Range: bytes f-l/size`,
   `content_length = l-f+1`; otherwise `200`, `content_length = size`;
   `resp.stream_body = move(stream.body)`.

### 3.2 LocalFs Backend: Open Is a Snapshot

`LocalFsBackend::get_object` (`localfs_backend.cc:get_object`):

```text
co_await pool_->schedule()
open(O_RDONLY); on failure, require_bucket() first to distinguish NoSuchBucket/NoSuchKey
fstat to confirm a regular file
load_meta(): stat (size/mtime) + read the <data>.lights3-meta sidecar (etag/content_type/meta.*)
resolve_range(range, size): resolve a-b/a-/-n into a closed interval; unsatisfiable → InvalidRange(416)
construct FdBodyReader(fd, offset=f, remaining=len)   ← fd ownership moves into the reader
```

`FdBodyReader.read()` does `co_await pool_->schedule()` first each time, then
`pread`s one chunk (the response loop uses a 64KiB buffer); blocking IO never
occupies the HTTP execution environment. Because it holds the fd and `pread`s
by offset, **even if the object is overwritten (renamed) or deleted mid
transfer, the complete snapshot of the old file can still be read to the end**;
if the file is truncated externally, EOF comes early. The reader closes the fd
on destruction.

### 3.3 Variants

- **XLocalFs**: `UringBodyReader` replaces `pread` with io_uring offset reads
  (Range comes naturally); no thread is occupied while waiting for completion.
- **Memory**: under the lock, `substr`-copies the hit interval out and returns
  it wrapped in a `StringBodyReader`.

## 4. Writing the Response Back (driver layer)

An `HttpResponse` body is one of two (`http/model.h:HttpResponse`): small
responses use `small_body` (a string), large responses use `stream_body`
(a BodyReader) + `content_length`. The builtin driver's write-back
(`builtin_server.cc:write_response`):

1. `content_length` set → send `Content-Length`; stream without a length →
   chunked;
2. HEAD and 204/304 send headers only;
3. streaming response loop: `sync_wait(stream_body->read(64KiB))` → `send`,
   until EOF;
4. **`read` throws after the response headers have gone out** (e.g. a backend
   disk error): the status code cannot be changed anymore; the only option is
   to drop the connection, and the client perceives the failure as a short byte
   count / unterminated chunked encoding;
5. before keep-alive reuse, the request-body residue must be `drain()`ed (the
   handler may not have read it all — e.g. rejected right at signature
   failure); if the residue is too large or 100-continue was never answered,
   the connection is closed outright.

## 5. One-Diagram Summary

```text
 Write (PUT /b/k)                              Read (GET /b/k)
 ───────────────                               ───────────────
 socket ──SocketBodyReader──┐                  ┌──FdBodyReader/UringBodyReader── fd
   (chunked de-framing/     │                  │   (pread per chunk, pool thread/io_uring)
    100-continue)           │                  │
        Sha256Verifying /   │                  │  ObjectStream{meta, body, range}
        ChunkedSigV4 reader │  ◄─ auth wrapper │
                            ▼                  ▼
                    backend.put_object   backend.get_object
                            │                  │
        staging tmp streamed write+MD5   resp.stream_body
        sidecar → rename atomic commit         │
                            │                  ▼
                            ▼          driver pulls 64KiB chunks and writes back
                       ETag response     (Content-Length / chunked)
```

Design points recap:

- streaming across the whole pipeline: memory footprint is independent of
  object size (O(64KiB) buffer per request);
- write path: staging + sidecar-first + rename; crashes/disconnects leave no
  half-written objects;
- read path: fd snapshot; concurrent reads and writes neither block nor
  pollute each other;
- blocking IO is always isolated from the HTTP execution environment via
  `pool_->schedule()` or io_uring.
