# CloudProxyBackend: A Proxy Backend Mapping Public Cloud Storage

> English translation of [../cloudproxy-backend.md](../cloudproxy-backend.md). The Chinese original is authoritative; section numbering matches.

> Status: P1–P5 fully implemented (`src/storage/cloudproxy/`; remaining P4 items
> wrapped up on 2026-07-31: §8.2 metrics, §2.3 `control_in_pump`, §7
> `force_path_style: false` vhost). Unit tests bootstrap a dual in-process stack
> and run the consistency suite (`test_cloudproxy.cc`, including the
> vhost/control_in_pump suites and metrics assertions); the e2e dual-instance
> scenarios `e2e_cloudproxy` / `e2e_tiered_cloudproxy` are all green. This
> carries forward the overview in docs/storage-backend.md §4 and the reservation
> in docs/tiered-storage.md §10 P5 (hooking tiered's cloud side up to a real
> cloud). This document settles the implementation route as **in-house SigV4
> signing + direct connection via vendored httplib** (route B of
> docs/storage-backend.md §4.1).

## 1. Goals and Non-Goals

Goals:

| Goal | Notes |
| --- | --- |
| 1:1 mapping of a remote S3-compatible store | Local bucket ↔ remote bucket (`bucket_prefix` prefix mapping); the remote may be AWS S3, MinIO, an S3-compatible endpoint of OSS/COS, or even another lights3 instance |
| Implement the full `IStorageBackend` interface | Including bucket CRUD, the object data plane, list, and the complete multipart set (docs/storage-backend.md §1, with `src/storage/backend.h` as the source of truth) |
| End-to-end streaming | GET/PUT never buffer a whole object in memory; backpressure propagates down to the TCP layer (continuing the principle of docs/storage-backend.md §4) |
| Credential isolation | Clients authenticate with the gateway's local AK/SK; the gateway accesses the remote with its own cloud credentials; the two are never mixed or passed through |
| Support tiered P5 | Serves as the cloud-side backend of docs/tiered-storage.md's TieredBackend (the primary consumer); acceptance checklist in §9 |

Non-goals (first phase):

- ~~No automatic acquisition of IAM Role / IMDS / STS temporary credentials~~
  **implemented (roadmap §3.3, 2026-08-28)**: with no static AK/SK configured
  the credential chain runs (environment → container endpoint → EC2 IMDSv2,
  §7); static AK/SK (`${ENV}` expansion) remains the explicit-config shape;
- No multi-endpoint load balancing/failover; one backend instance maps to one
  remote endpoint;
- No caching of remote data—caching is the responsibility of TieredBackend
  (docs/tiered-storage.md §6); separation of concerns;
- No proxying of remote extension APIs such as ACL / policy / versioning /
  lifecycle; coverage is limited to the object semantics expressed by
  `IStorageBackend`.

## 2. Architectural Position and Route Decision

### 2.1 Route Reversal: A (SDK) → B (In-House Signing, Direct Connection)

docs/storage-backend.md §4.1 originally preferred route A (wrapping
aws-sdk-cpp). This design **reverses that to route B**, for these reasons:

1. **Outbound signing is already in place**: `SigV4Authenticator::sign()`
   (`src/s3/auth/sigv4.h`) was implemented alongside signature verification; its
   header comment already states "the signing side is for unit tests and later
   reuse by cloudproxy forwarding"; the underlying crypto primitives
   (HMAC-SHA256 and incremental MD5/SHA256 in `src/core/util/crypto.h`) are all
   present. The hardest part of route B (signing) is effectively zero new code.
2. **The HTTP client is already vendored**: `httplib::Client` of
   `third_party/httplib` 0.20.0 supports streaming download (ContentReceiver)
   and streaming upload (ContentProvider); HTTPS only needs the
   `CPPHTTPLIB_OPENSSL_SUPPORT` compile macro plus linking `OpenSSL::SSL`
   (§8.3).
3. **The cost of pulling in the SDK is unacceptable**: aws-sdk-cpp has a huge
   dependency tree (libcurl etc.), conflicting with this project's build
   constraint of "all dependencies via vendored submodules, zero system-package
   dependencies".
4. **A controllable threading model**: a self-built implementation can be fused
   precisely with the project's coroutine/ThreadPool model; the SDK's sync/async
   APIs would require a second adaptation layer instead
   (docs/storage-backend.md §4 already pointed out the thread-hogging bottleneck
   of a synchronous SDK).

Costs (already listed as §1 non-goals): no automatic credential chain; S3
protocol corner cases (such as §4.4's 200-with-error-body on complete) must be
handled ourselves—fortunately the coverage surface is only the
`IStorageBackend` interface layer, and remote interaction can be regressed with
the consistency suite against lights3 itself (§10).

### 2.2 Class Structure and the Common Request Pipeline

```text
src/storage/cloudproxy/
├── cloudproxy_backend.h/.cc    # CloudProxyBackend : IStorageBackend
└── remote_client.h/.cc         # ClientPool (httplib::Client connection pool)
                                # + RemoteRequest common pipeline + error mapping
(reused) s3/auth/sigv4           # outbound signing
(reused) s3/xml                  # remote XML response parsing / request body generation
(reused) core/util/{crypto,uri,time}
(reused, extracted in P1) http/pushpull  # BlockQueue / QueueBodyReader (§3.1)
```

Every operation goes through the same pipeline:

```text
① Build a neutral http::HttpRequest (for signature computation only):
     method / raw_path / raw_query / headers (including host and business x-amz-*)
② authenticator_.sign(req, cred, payload_hash)
     —— fills in x-amz-date / x-amz-content-sha256 / Authorization
③ Move req.headers into httplib::Headers, take a connection from ClientPool and send
④ Response: 2xx → parse headers/XML and assemble the return value;
   otherwise map_remote_error() throws s3::S3Error
```

**How signing is wired in**: `sign()` modifies the `HttpRequest` in place; its
canonical computation depends only on `method`, `raw_path` (i.e. the canonical
URI), `raw_query` (sorted internally), and `headers` (SignedHeaders
automatically takes host + all `x-amz-*`). Hence the choice of "build a minimal
HttpRequest solely for signing, then move the headers over"—the alternative
(rewriting the canonical computation directly against httplib::Headers) would
amount to duplicating the signing logic, defeating the point of reuse. Key
points:

- `raw_path = "/" + aws_uri_encode(remote_bucket + "/" + key, /*encode_slash=*/false)`
  (readily available in `core/util/uri.h`); `raw_query` is assembled per
  operation (`uploads`, `list-type=2&prefix=...`, etc.), values encoded with
  `aws_uri_encode(v, true)`;
- **Host consistency trap**: the `Host` header httplib actually sends is
  `host[:port]` (port included for non-default ports). The `host` preset in the
  signing-side HttpRequest must match it **byte for byte** (`sign()` does not
  fill it in), otherwise the remote reports SignatureDoesNotMatch. The
  implementation parses a canonical host string from the endpoint and shares
  one value between the two places;
- Business headers (`x-amz-meta-*`, `Range`, `Content-Type`) are put in before
  ②. `x-amz-*` automatically enters SignedHeaders; `Range`/`Content-Type` are
  not part of the signature (SignedHeaders contains only host + x-amz-*, which
  S3 accepts);
- Authenticator instance: one per backend,
  `SigV4Authenticator::build(AuthConfig{cloud credentials, remote region, "s3"})`,
  with region independent from the region used by local L2 verification.

### 2.3 Threading Model: Private Pump Threads, No Shared-Pool Occupation

`httplib::Client` is a synchronous blocking API: `Get/Put` block until the
entire transfer finishes. Putting those calls into the shared ThreadPool risks
a **global deadlock**: once the number of concurrent proxy requests ≥ pool
size, all pool threads block in the pump (waiting for the queue consumer of
§3), while the consumer—the handler coroutine—needs a pool thread to
resume. Therefore:

- **Data-plane pumps run on CloudProxyBackend's private threads** (one
  `std::thread` per in-flight transfer, capped at `max_connections`, naturally
  bounded by ClientPool capacity);
- Control-plane short requests (HEAD/DELETE/LIST etc., small response bodies)
  by default do `co_await pool->schedule()` and then call synchronously on a
  pool thread—a single occupation is on the same order as one localfs disk
  operation, which is acceptable; when high remote RTT makes pool occupation
  significant, set **`control_in_pump: true`** so the control plane also uses a
  one-shot private thread (the `control_io` helper: the private thread runs the
  blocking section including retry backoff, and on completion the continuation
  resumes via the pool executor—same family as the data-plane pump).
  **Load-test conclusion (P4, default false)**: against an in-process loopback
  remote, HEAD costs pool≈210µs vs pump≈294µs per operation—the ~80µs fixed
  cost of one thread creation per control request is a net loss at low RTT;
  when remote RTT ≥ several ms (public Internet/cross-region) that overhead
  drops below 5% while the freed pool-thread occupation spans the whole
  RTT+backoff, making true worthwhile. Criterion: enable it when the pool wait
  histograms (`lights3_pool_*` / `lights3_backend_pool_*`) shift right and
  cloudproxy control-plane op latency dominates.

The generic per-backend dedicated pool reserved in docs/concurrency.md §3.1
(the `io_threads` config key) has landed separately and can give cloudproxy its
own pool; the private pump thread set here is a local workaround for
data-plane blocking sections—two different things, kept as is.

## 3. Data-Plane Streaming Design

### 3.1 GET: Push-to-Pull (Reusing the BlockQueue Precedent)

httplib's ContentReceiver is a push-model callback, while `get_object` must
return a pull-model `BodyReader`. The project has already solved exactly this
problem in the httplib **server** driver:
`src/http/drivers/httplib/httplib_server.cc`'s `BlockQueue` (a byte-capped
bounded buffer where push returning false means the consumer canceled, pop
returning 0 means EOF, and an exception means mid-stream failure) +
`QueueBodyReader`. **P1 extracts these two classes from that file's anonymous
namespace into shared components (`src/http/pushpull.h`), shared by the server
driver and cloudproxy.**

```text
get_object(bucket, key, range):
① Take a connection from ClientPool, start the pump thread, then block waiting
     for the "headers ready" signal (promise/condition variable, timeout =
     request_timeout; the calling coroutine would be waiting for the first byte
     at this point anyway; it sits in the shared pool, the pump on a private
     thread—no mutual wait)
② Pump thread: client.Get(path, headers, ResponseHandler, ContentReceiver)
     - ResponseHandler (called as soon as response headers arrive):
         2xx → assemble ObjectMeta from headers (header mapping of §4.1)
               + parse Content-Range, hand back to ① via the promise;
         error status → keep receiving the error body, do the §5 mapping,
               hand the exception back via the promise
     - ContentReceiver: loop queue->push(data, n); if push returns false,
       return false to abort the transfer
     - Finish: queue->close(ok)
③ Once ① has the meta: co_return ObjectStream{meta,
       make_unique<CancelOnDropReader>(QueueBodyReader(queue, len)), range}
```

- **Backpressure**: BlockQueue capacity (default 1 MiB, configurable) → push
  blocks → httplib stops recv → TCP window tightens → the remote slows
  down. Same mechanism as the server side; propagates naturally;
- **Cancellation**: when the client disconnects or the handler throws, the
  reader's destructor calls `cancel()` on the queue → push returns false →
  ContentReceiver returns false → httplib aborts the transfer. A "cancel on
  destruction" wrapper is needed around QueueBodyReader (the server-side
  precedent is inbound and has no such need). Noted cost: the aborted
  connection is invalidated; after being returned to ClientPool, httplib
  automatically reconnects on the next request (one keep-alive lost).

### 3.2 PUT / upload_part: Pull-to-Pull, Decoupling Threads via the Queue

The `BodyReader&` parameter is a pull model and httplib's ContentProvider is
also a pull model, but the two **cannot be joined on the same thread**: the
Provider is a synchronous callback and cannot drive `co_await body.read()`
inside itself; moreover BodyReader's contract is that it is driven only by the
handler coroutine chain. Symmetrically reuse BlockQueue with the direction
reversed:

```text
put_object(bucket, key, meta, body):
① Start the pump thread: client.Put(path, headers, length, ContentProvider, ct)
     The Provider pops from the queue and writes into the DataSink; ends after
     popping EOF
② The calling coroutine (staying in the shared pool) loops:
     co_await body.read(64KiB buffer) → HashStream(Md5) incremental update
       → queue->push(); after EOF, queue->close(ok=true)
     body.read() throws (client disconnected) → queue->close(ok=false) →
       Provider returns false to abort the upload
③ join the pump, take the remote response: 2xx → verify ETag (§6)
   → co_return PutResult
```

**Payload hash decision: `UNSIGNED-PAYLOAD`.** Trade-offs among the three
candidates:

| Candidate | Trade-off |
| --- | --- |
| Exact SHA256 | Requires reading the entire body before sending the first byte—violates the streaming principle; rejected |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` | Requires implementing outbound aws-chunked framing plus a per-chunk signature chain; high complexity for a benefit that only matters over plaintext HTTP integrity—not done |
| `UNSIGNED-PAYLOAD` (chosen) | Accepted by AWS/MinIO/lights3 itself; over HTTPS integrity is guaranteed by TLS, further compensated by §6's end-to-end ETag comparison |

Documented convention: **the combination of plaintext HTTP + unsigned payload
is recommended only for intranet/testing**; production should use HTTPS.

**Two Content-Length cases**:

- `body.length()` has a value (the vast majority: client sends Content-Length;
  aws-chunked input streams also have `x-amz-decoded-content-length` as a
  fallback) → fixed-length `client.Put(...)`, best compatibility;
- nullopt (true chunked with no length): AWS S3 does not accept bare
  `Transfer-Encoding: chunked` (it requires fixed length or aws-chunked). P4
  decision: keep `NotImplemented`; do not implement
  `STREAMING-UNSIGNED-PAYLOAD-TRAILER` outbound framing—a rare path whose
  benefit does not justify the complexity (recorded as explicitly
  won't-do).

### 3.3 Range GET Pass-Through

The `range` parameter is formatted as `Range: bytes=...` (the three forms
`first-last` / `first-` / `-suffix` passed through as is):

- Remote 206: parse total from `Content-Range: bytes a-b/total` into
  `meta.size` (the interface contract says size is the full object length); the
  effective interval goes into `ObjectStream.range`;
- Remote 416 → `InvalidRange`;
- Remote anomalously returns 200 (a non-conforming endpoint ignoring Range):
  treat as full-object, clear `range`—L2 responds with a full 200, semantics
  still correct.

## 4. Control Plane

### 4.1 head / delete and Header Mapping

- **head_object**: HEAD request → assemble `ObjectMeta` from response headers:
  `ETag` stripped of quotes and stored as hex (`backend.h` convention is
  unquoted), `Content-Length` → size, `Content-Type`, `Last-Modified` via the
  existing `util::parse_http_date`, `x-amz-meta-*` stripped of the prefix into
  `user_meta`. 404 → `NoSuchKey`.
- **delete_object**: DELETE; both remote 204 and 404 count as success (S3
  idempotent delete semantics, consistent with local backends).

### 4.2 list: ListObjectsV2 with Always-start-after Pagination

The local `ListOptions.start_after` token semantics is "the last key of the
previous page", while V2's `continuation-token` is an opaque remote
string—the two do not match. Solution: **send on every page**
`?list-type=2&start-after=<token>&prefix=&delimiter=&max-keys=`. How
`next_token` is derived: if the last element is a plain key, it is that key; if
it is a common-prefix group, take `group_skip_token(prefix)` = the prefix
padded with 0xff up to the key length limit (1024)—under the exclusive
semantics all keys inside the group are skipped, and not a single successor key
outside the group (including a literal key equal to "last char +1") is missed.
`start-after` is valid on any page and stateless (correctness takes priority
over the potential server-side optimization of continuation-token).

- Response parsing uses the existing `s3::xml_parse` (`src/s3/xml.h`, a
  shallow-structure parser that skips attributes—ListBucketResult only has an
  xmlns attribute, so it suffices);
- **Utility gap**: `LastModified` in the response is ISO8601, and
  `core/util/time.h` currently only has the `iso8601()` formatter—P1 adds
  `parse_iso8601()`;
- **list_buckets**: GET / → parse ListAllMyBucketsResult, keep only names
  carrying the `bucket_prefix` prefix, strip the prefix and return (the rest
  are unrelated buckets under the remote account).

### 4.3 Bucket Operations: Direct Mapping to the Remote

No "local virtual buckets"—tiered P5 needs remote buckets to really exist,
and virtualization would introduce local state, contradicting the stateless
proxy positioning.

- Name mapping: `remote = bucket_prefix + local`; the concatenation is
  validated by S3 rules (length ≤ 63 etc., reusing the existing bucket-name
  validation); violations are reported at **config load time**;
- `create_bucket` → PUT bucket (with CreateBucketConfiguration XML generated
  by `XmlWriter` when region ≠ us-east-1); 409 already-exists →
  `BucketAlreadyOwnedByYou` passed through;
- `bucket_exists` → HEAD bucket: 200 → true; 404 → false; **403 → true**
  (matching AWS HeadBucket semantics: an existing bucket without permission
  also returns 403; treat as existing and log a warn). This ambiguity means
  callers must not use the bucket_exists result to decide whether to create a
  bucket—tiered's `ensure_cloud_bucket` therefore goes straight to create and
  treats 409 as already-exists, without an exists check;
- `delete_bucket` → DELETE; 409 → `BucketNotEmpty`.

### 4.4 Multipart Pass-Through

- `create_multipart` → POST `?uploads` (the meta's content_type / user_meta go
  onto headers at this point) → parse InitiateMultipartUploadResult for the
  UploadId; **upload_id passed through verbatim**, the gateway keeps no local
  state whatsoever;
- `upload_part` → PUT `?partNumber=N&uploadId=...`, streaming as in §3.2, ETag
  passed through;
- `complete_multipart` → POST, body generated with `XmlWriter`
  (CompleteMultipartUpload/Part/PartNumber/ETag; the ETag must get its quotes
  back). **Must handle S3's special "200 OK with an `<Error>` body"**: when
  complete takes long, AWS first returns 200 and then reports the error inside
  the body (the most famous pitfall of the self-implementation route)—after
  receiving the full body, discriminate by root node name between
  CompleteMultipartUploadResult / Error, the latter going through §5 error
  mapping;
- `abort_multipart` → DELETE `?uploadId=...`, 404 → `NoSuchUpload`;
- `list_parts` / `list_multipart_uploads` → GET with the corresponding query
  strings, XML parsing as in §4.2.

## 5. Error Mapping Matrix and Retry

### 5.1 Mapping Matrix

Remote response → local `s3::S3Error` (single-point implementation in
`map_remote_error()`):

| Remote behavior | Local behavior |
| --- | --- |
| 4xx + parsable XML `<Error><Code>` | Reverse-lookup `S3ErrorCode` by wire code and pass through verbatim (NoSuchKey / NoSuchBucket / NoSuchUpload / InvalidRange / InvalidPart / BucketNotEmpty / BucketAlreadyOwnedByYou / EntityTooLarge…); `errors.cc` needs a wire code → enum reverse lookup added |
| Remote 403 (proxy credential/permission fault) | **`InternalError`, do not pass AccessDenied through**—the client has already passed local authentication; the 403 is a gateway configuration fault, and passing it through would mislead the client into debugging its own credentials; log warn with the remote's original code (exception: the HEAD 403 of `bucket_exists`, see §4.3) |
| 404 with unparsable body | Fill in NoSuchKey / NoSuchBucket from the operation context |
| 429 / 503 / SlowDown | `SlowDown` (local 503; the client may back off and retry) |
| 500 / 502 / 504, 5xx with unparsable body | `InternalError` (local 500). **No 502 introduced**: the S3 error vocabulary has no BadGateway; standard S3 clients treat 500/503 as retryable; stay protocol-faithful (the original "502/503" wording in docs/storage-backend.md §4 is revised along with this document) |
| Connection refused / DNS failure / timeout (after retries exhausted) | `InternalError`, message containing the endpoint and the underlying cause (httplib `Result.error()` enum rendered as text) |

### 5.2 Retry Policy

- Retryable conditions: network-layer errors, 5xx, SlowDown. Exponential
  backoff `base × 2^n + jitter` (defaults: base 100ms, 3 attempts;
  `retry_max` / `retry_base_ms` configurable with range validation at load
  time; a single backoff is clamped to 60s); **a `Retry-After` header on
  429/503 (integer seconds or HTTP-date) overrides the formula, clamped to
  [0, 60s] (roadmap §3.3)**;
- **Backoff never sleeps on a pool thread** (roadmap §3.3 rework): the
  control-plane retry loop is driven at coroutine level (`retry_io`) with
  awaitable TimerQueue sleeps between attempts, and connection leases are
  asynchronous too (`ClientPool::acquire_async` — at capacity a waiter
  suspends for a handoff instead of parking in a cv). Data-plane pumps are
  private per-transfer threads, where a blocking backoff is harmless;
- **Circuit breaker**: `breaker_threshold` consecutive transport/5xx failures
  (429 is neutral; default 10) open it for `breaker_cooldown_ms` (default
  10s) — requests fail fast with SlowDown, then a single half-open probe
  decides. A decidedly-down remote no longer costs every request the full
  `(retry_max+1)` rounds;
- **Per-op deadline** (`op_deadline_ms`, default 0 = off): a total budget for
  one operation's whole retry loop; it only trims retries and never cuts an
  in-flight transfer;
- Applies in full to idempotent operations: GET / HEAD / LIST / DELETE / abort
  / bucket operations / create_multipart / complete_multipart. Note:
  create_multipart is strictly speaking non-idempotent—retrying after a lost
  response leaves an empty orphan upload on the remote (standard industry
  practice; the AWS SDK behaves the same); recommend configuring an
  AbortIncompleteMultipartUpload lifecycle rule on the remote account;
- **Ambiguity of complete retries**: receiving `NoSuchUpload` after a retry may
  mean the previous attempt actually succeeded (the upload is gone)—in that
  case degrade to a HEAD of the target object to verify; if it exists and the
  ETag has the `-N` form, treat as success;
- **PUT / upload_part must not be blindly retried**: the body is a one-shot
  `BodyReader` that cannot be replayed once the first byte has been
  pumped—retry only when the failure occurred during **connection
  establishment** (httplib returns a Connection / SSLConnection class error and
  the Provider was never invoked, determined via a flag).

## 6. ETag Semantics and End-to-End Verification

- Always **pass through the remote ETag** (quotes stripped, stored as hex);
- Single-part PUT / upload_part: remote ETag = content MD5. §3.2 already
  computes it incrementally with `util::HashStream(Md5)` while pumping; when
  the response arrives, compare against the remote ETag and throw
  `InternalError` ("upload corrupted in transit") on mismatch—this both
  compensates for the integrity of UNSIGNED-PAYLOAD and directly satisfies
  docs/tiered-storage.md §5.2's dependency of "verify the cloud-returned etag
  against local content";
- The multipart overall ETag `hex-N` rule matches the local implementation
  (`md5(concatenation of part md5s)-N`), so tiered's comparison of the cloud
  etag against the local sidecar is semantically self-consistent;
- Exception: with SSE-KMS / SSE-C enabled on the remote, the ETag is not the
  content MD5—`verify_etag: false` is provided to disable the comparison
  (enabled by default).

## 7. Configuration

```yaml
backends:
  - name: aws
    type: cloudproxy
    endpoint: "https://s3.ap-east-1.amazonaws.com"   # or http://127.0.0.1:19100 (testing)
    region: ap-east-1
    access_key: "${CLOUD_AK}"        # the config parser already supports ${ENV} expansion
    secret_key: "${CLOUD_SK}"
    bucket_prefix: "lights3-"        # remote bucket = prefix + local name; default empty
    force_path_style: true           # default true, see below
    control_in_pump: false           # control plane on private threads (§2.3 load-test default false)
    tls_verify: true                 # can be off for self-signed remotes; optional ca_cert: /path/to/ca.pem
    connect_timeout_ms: 5000
    request_timeout_ms: 60000        # httplib read/write timeout (per recv/send call)
    retry_max: 3
    retry_base_ms: 100
    op_deadline_ms: 0                # total budget for one op's retry loop; 0 = no cap (§5.2)
    breaker_threshold: 10            # circuit-breaker threshold (consecutive failures); 0 = off (§5.2)
    breaker_cooldown_ms: 10000
    max_connections: 16              # ClientPool cap = pump concurrency cap
    pool_idle_timeout_ms: 60000      # idle-connection expiry/reaping (NAT protection, §8.1); 0 = keep forever
    pool_max_lifetime_ms: 0          # retire connections by age; 0 = unlimited
    queue_cap: 1MiB                  # data-plane BlockQueue capacity (backpressure watermark, §3.1)
    verify_etag: true                # §6; turn off when the remote uses SSE-KMS
    # Leave BOTH access_key/secret_key empty to use the AWS credential chain (roadmap §3.3):
    # environment → container endpoint (ECS/EKS) → EC2 IMDSv2, with session credentials
    # refreshed ahead of expiry; imds_endpoint: http://169.254.169.254 is overridable for tests
```

All keys are collected automatically via `BackendConfig::params` (scalar keys
other than name/type under a yaml backend entry all go into params); **no
config-parser change needed**.

**`force_path_style` defaults to true**: MinIO / self-hosted endpoints /
lights3 as the remote are all naturally path-style, and AWS still accepts
path-style to this day. Explicitly turning it off means **virtual-hosted style
(implemented in P4)**: Host and signature become
`<remote_bucket>.<endpoint-host>[:port]` and the path no longer contains the
bucket segment (single point of `Target` resolution, `RemoteContext::target`).
Implementation key points: **the TCP connection and TLS SNI always point at the
endpoint itself**; only the Host header/signature/path vary per bucket—httplib
only sets Host when it is absent, and this pipeline always carries the signed
Host explicitly, so ClientPool needs no per-bucket differentiation (rejecting
the earlier "per-bucket connection pool" idea: with wildcard DNS resolving to
the same frontend group the two are equivalent, while direct endpoint
connection avoids per-bucket connection amplification). Deployment-side
constraint: the remote must accept the vhost Host under the endpoint
certificate/frontend (the common shape of AWS regional endpoints and
S3-compatible gateways; lights3 as the remote just needs `http.base_domain`
configured); bucket names containing `.` will fail TLS wildcard certificate
matching—a deployment-side responsibility, not intercepted by the gateway.

## 8. Connection Management and Build Changes

### 8.1 ClientPool

`httplib::Client` is not thread-safe (a single socket reused sequentially) →
a mutex-protected **idle-queue connection pool**: acquire pops an idle instance
(creating one if none, total capped at `max_connections`; at the cap, wait with
a timeout — pump threads block on a cv, the coroutine control plane suspends in
`acquire_async` for a direct handoff instead of parking a pool thread), returned
via RAII guard. Per-thread clients rejected: pumps run on private threads and
the control plane on arbitrary pool threads, so thread_local would make the
connection count uncontrollable.

Connection hygiene (roadmap §3.3): idle entries carry timestamps — anything
idle beyond `pool_idle_timeout` is never reused and a light reaper closes it (a
remote/NAT silently dropping idle sockets no longer shows up as periodic
first-request retry spikes); `pool_max_lifetime` retires connections by age at
release; `total_` shrinks accordingly.

Each Client is uniformly configured at creation: `set_connection_timeout` /
`set_read_timeout` / `set_write_timeout`, `set_keep_alive(true)`, TLS
verification switches (`enable_server_certificate_verification` /
`set_ca_cert_path`). Canceled transfers (§3.1) invalidate the connection; after
return, httplib reconnects automatically.

### 8.2 Metrics (Implemented in P4)

Wired in via MetricsScope (the factory's third parameter),
encapsulated at a single point in `RemoteMetrics`:

| Metric | Type/labels | Semantics |
| --- | --- | --- |
| `lights3_cloudproxy_remote_request_seconds` | histogram, op | One observation per remote round trip (each attempt of a retried call recorded separately); data plane (get/put/upload_part) = the whole transfer duration |
| `lights3_cloudproxy_retries_total` | counter, op | Number of retries that actually backed off |
| `lights3_cloudproxy_remote_errors_total` | counter, code | Remote failures mapped to local errors: wire code preferred (NoSuchKey…), unparsable bodies fall under `http_<status>`, network layer under `transport`—a bounded code set |
| `lights3_cloudproxy_etag_mismatch_total` | counter | Remote ETag ≠ local incremental MD5 (in-transit corruption signal, §6); registered with 0 at construction for visibility |
| `lights3_cloudproxy_pool_wait_seconds` | histogram | ClientPool acquire wait (including the SlowDown timeout path); rightward shift = `max_connections` tuning signal |

op label values: control plane create_bucket / delete_bucket / head_bucket /
list_buckets / head / delete / list / create_multipart / complete_multipart /
abort_multipart / list_parts / list_uploads; data plane get / put /
upload_part. op/code-dimension instances are registered on demand through a
mutex-protected cache (get-or-create idempotent). Warn logs (403 mapping, ETag
fallback) remain unchanged.

### 8.3 CMake

- `CPPHTTPLIB_OPENSSL_SUPPORT` must be defined at the **lights3_core target
  level**—having it on in one of the two TUs (httplib_server.cc and
  cloudproxy) and off in the other is an ODR violation;
- Linking extended from `OpenSSL::Crypto` to also include `OpenSSL::SSL`;
- New option `LIGHTS3_CLOUDPROXY` (default ON); `registry.cc` already registers
  the cloudproxy factory (reading the §7 keys from `BackendConfig::params`).

## 9. Acceptance for Integration with TieredBackend (docs/tiered-storage.md P5)

As tiered's cloud-side backend, the acceptance checklist:

1. After `put_object` uploads with `user_meta` (the `x-amz-meta-lights3-*`
   redundancy headers, docs/tiered-storage.md §4.2), `head_object` /
   `get_object` retrieve it verbatim;
2. The etags returned by put / upload_part / complete are non-empty;
   single-part = content MD5 (the verification dependency of
   docs/tiered-storage.md §5.2 step ③);
3. All three Range GET forms are correct (the pass-through dependency of
   docs/tiered-storage.md §6.3);
4. head returns size / etag / last_modified in full (the conditional-request
   dependency of docs/tiered-storage.md §6.1);
5. `list_objects` is usable for the docs/tiered-storage.md §9 reconciliation
   traversal;
6. When the remote is unreachable, throw `InternalError` / `SlowDown` rather
   than hanging (docs/tiered-storage.md §9's fault matrix relies on
   predictable exceptions).

## 10. Testing Strategy

**Unit tests: in-process dual-stack bootstrap, no mocking of httplib.** The
test starts lights3's own `HttplibServer + S3Service + MemoryBackend` as the
"remote" (127.0.0.1 with a random port and one static credential), constructs a
CloudProxyBackend pointing at it, and directly runs the
`run_backend_suite()` consistency suite of `tests/unit/test_storage.cc`. Bonus:
this simultaneously covers interoperability between our own `sign()` and local
`verify()` (mirror images of each other—a free regression). P1 must verify
whether HttpConfig supports port=0 auto-selection; if not, the test probes for
a free port itself.

Targeted tests: error mapping (a bare httplib::Server returning constructed
error XML / mid-stream disconnect), GET canceled midway (reader destroyed
early, asserting the remote stream is aborted), the three Range forms, the ETag
verification failure path, retry counting (a fake endpoint with injectable
failures), and complete's 200-with-error-body.

**e2e**: `tests/e2e/run_e2e.sh` gains a dual-instance scenario—start instance
B (localfs backend) acting as the "cloud", configure instance A with cloudproxy
pointing at B, and run the existing curl --aws-sigv4 case set; then add a
`tiered(cloud=cloudproxy→B)` combination smoke test (a P5 rehearsal). No
external dependencies like MinIO/docker throughout.

## 11. Implementation Phases

| Phase | Content | Independently verifiable | Status |
| --- | --- | --- | --- |
| P1 | CMake (OPENSSL_SUPPORT + OpenSSL::SSL + option); extract BlockQueue/QueueBodyReader into `http/pushpull.h` (server driver switched over in the same change); config parsing and validation; ClientPool; signing pipeline; control-plane head/delete/bucket CRUD; error-mapping skeleton; `parse_iso8601`; registry registration | Control-plane cases pass against the in-process remote; no regression in the existing full unit-test set | ✅ |
| P2 | GET data plane (pump + ResponseHandler + BlockQueue, Range, cancellation); list_objects / list_buckets XML parsing | run_backend_suite read/list paths pass; cancellation targeted test passes | ✅ |
| P3 | PUT / upload_part streaming (pull-to-pull + UNSIGNED-PAYLOAD + MD5 verification); full multipart set (including 200-with-error-body handling) | `run_backend_suite(CloudProxyBackend)` all green | ✅ |
| P4 | Retry/backoff, timeout refinement, metrics, logging; decision on the length-less body path (NotImplemented or TRAILER framing); `control_in_pump` default set by load test | Fault-injection targeted tests pass | ✅ All landed (2026-07-31): metrics in §8.2; `control_in_pump` load test set default false (§2.3); `force_path_style: false` vhost implemented along the way (§7) |
| P5 | e2e dual-instance script; tiered integration (§9 checklist); docs/tiered-storage.md P5 status update | e2e passes; tiered + cloudproxy smoke test passes | ✅ (`e2e_cloudproxy` + `e2e_tiered_cloudproxy`) |
