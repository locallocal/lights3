# S3 Protocol Implementation

> English translation of [../s3-protocol.md](../s3-protocol.md). The Chinese original is authoritative; section numbering matches.

## 1. API Scope

The first phase covers the subset needed for day-to-day operations of mainstream clients (aws cli, boto3, s3cmd, MinIO SDK):

| Category | API | Notes |
| --- | --- | --- |
| Service | ListBuckets | Aggregates across backends |
| Bucket | CreateBucket / DeleteBucket / HeadBucket | Single region: a CreateBucket LocationConstraint that disagrees with the configured region is `InvalidLocationConstraint`; HeadBucket/CreateBucket return `x-amz-bucket-region` |
| Object | PutObject / GetObject / HeadObject / DeleteObject / DeleteObjects (batch) / CopyObject | Get supports Range, conditional requests (If-Match/If-None-Match/If-Modified-Since), the six `response-*` override parameters and `?partNumber` (206 + `x-amz-mp-parts-count`; the part layout is recorded in `part_sizes` at complete); Cache-Control/Content-Disposition/Content-Encoding/Content-Language/Expires are persisted and echoed back; Content-MD5 and `x-amz-checksum-*` (crc32/crc32c/crc64nvme/sha1/sha256, in header or aws-chunked trailer form) verify the request body **and persist with the object** — GET/HEAD echo them under `x-amz-checksum-mode: ENABLED` (with `x-amz-checksum-type`; not on ranged/partNumber 206); DeleteObjects **requires** an integrity header |
| Tagging | GetObjectTagging / PutObjectTagging / DeleteObjectTagging | `x-amz-tagging` at write time persists on every backend; `x-amz-tagging-count` echoed; in-place tag mutation works on memory/localfs/xlocalfs/tiered/cloudproxy, duostore answers an honest 501 (no meta-only update primitive) |
| CORS | GetBucketCors / PutBucketCors / DeleteBucketCors + OPTIONS preflight | Root only (same two-tier model as ?website); rules persist to `.sys/cors/<bucket>`; preflight is dispatched **before** signature verification (browsers never sign OPTIONS); actual requests (success and error responses alike) get Allow-Origin/Expose-Headers/Vary injected |
| Lifecycle | GetBucketLifecycle / PutBucketLifecycle / DeleteBucketLifecycle | Root only; minimal subset: Expiration.Days + AbortIncompleteMultipartUpload.DaysAfterInitiation (prefix filters); enforced periodically (`lifecycle.scan_interval`, default 1h, 0 = off); Transitions/tag filters/Date forms → 501 |
| Quota / tenancy | GetBucketQuota / PutBucketQuota / DeleteBucketQuota (`?quota`, this implementation's own XML) | Bucket-level `MaxBytes`/`MaxObjects` (PUT/DELETE root only); usage counters are gateway-maintained and periodically recounted; writes over the limit get `QuotaExceeded` (403), multipart parts count toward usage and Complete is refused only when the finished object still would not fit (the upload is kept); tenant credentials (`tenant`/`role` fields) see only their tenant's buckets and ListBuckets reports the tenant as `Owner`; admin plane `/-/admin/tenants`, `/-/admin/usage`; JSON-lines audit log. See [multi-tenancy.md](multi-tenancy.md) |
| STS | AssumeRole | `POST /` (path-style deployments) with a form body, SigV4 service scope `sts`; session credentials (L3SA-prefixed AK/SK/token, TTL 900–43200s) inherit the **caller's** policy (no role catalog — a session can never exceed the identity that minted it); in-memory single-instance, sessions cannot assume again |
| List | ListObjectsV2 (with V1 compatibility) | prefix / delimiter / max-keys / continuation-token / start-after / fetch-owner; V1 honours only marker, V2 only continuation-token and start-after |
| Multipart | CreateMultipartUpload / UploadPart / UploadPartCopy / CompleteMultipartUpload / AbortMultipartUpload / ListParts / ListMultipartUploads | UploadPartCopy supports x-amz-copy-source-if-* and x-amz-copy-source-range (bytes=first-last, both ends required); source/destination may be on different backends; ListParts/ListMultipartUploads are **truly paginated** (marker + max-*, honest IsTruncated; both accept encoding-type, and uploads accepts arbitrary delimiters); non-final parts must be at least 5MiB (`http.min_part_size`, 0 disables), out-of-order parts return `InvalidPartOrder`; per-part checksums persist with the part records, complete computes the composite (`-N`) checksum from **verified** stored values (COMPOSITE; CRC64NVME/explicit FULL_OBJECT → 501) and cross-checks any Checksum* claims in the XML (mismatch → BadDigest) |

Static website hosting **is supported** (docs/static-website.md): per-bucket
anonymous GET/HEAD object reads, index/error documents, the root-only
`?website` configuration API, and `x-amz-website-redirect-location`.

Explicitly unsupported (returns `NotImplemented`): versioning, fine-grained ACL
(only private is accepted), bucket policy, lifecycle transitions/tag filters,
SSE-C/KMS, Object Lock, storage-class (only STANDARD is accepted), presigned
POST (query signing for presigned GET/PUT **is supported**, see §3.4). The
rejection surface covers both query subresources (inverted whitelist; anything
off-list → 501) and **request headers** (`x-amz-server-side-encryption*` /
`x-amz-object-lock-*` / `x-amz-grant-*` etc. → 501 when present, no longer
silently swallowed with a 200). PutObject/UploadPart without
Content-Length/Transfer-Encoding → 411 `MissingContentLength`; `list-type=3` →
`InvalidArgument`.

## 2. Routing and Addressing

- **path-style** (default): `GET /{bucket}/{key...}`.
- **virtual-host style**: `Host: {bucket}.gw.example.com`, enabled once
  `http.base_domain` is configured; both are supported simultaneously.
- Operation identification = method + path shape + query flags (such as `?uploads`,
  `?uploadId=`, `?delete`, `?list-type=2`). The Router uses an explicit dispatch
  table rather than regexes, keeping it readable and testable:

```cpp
// (method, scope, query-flag) → handler
{ "GET",  Scope::Bucket, "list-type=2" } → ListObjectsV2Handler
{ "POST", Scope::Object, "uploads"     } → CreateMultipartHandler
{ "PUT",  Scope::Object, "partNumber"  } → UploadPartHandler
{ "PUT",  Scope::Object, {}            } → PutObjectHandler   // fallback
...
```

## 3. AWS Signature V4 Authentication

Implemented in-house (the protocol is public and stable; avoids pulling in a whole SDK just for signature verification); code lives in `src/s3/auth/`.

### 3.1 Verification Flow

```text
1. Parse the Authorization header (or query parameters, in the presigned case)
   → access_key, date, region/service, SignedHeaders, Signature
2. Look up access_key in the local credential table → secret_key (not found → InvalidAccessKeyId)
3. Rebuild the CanonicalRequest:
   method + canonical_uri(undecoded path re-encoded per SigV4 rules)
          + canonical_query(sorted + encoded)
          + canonical_headers(headers listed in SignedHeaders)
          + hashed_payload
4. StringToSign = "AWS4-HMAC-SHA256" + timestamp + scope + sha256(CanonicalRequest)
5. Derive the signing key (HMAC chain: date→region→service→"aws4_request")
   → compute the signature, constant-time comparison (guards against timing side channels)
6. Check clock skew: |x-amz-date - now| > 15min → RequestTimeTooSkewed
```

### 3.2 The Three Forms of Payload Verification

| `x-amz-content-sha256` | Handling |
| --- | --- |
| Hex digest | Compute SHA256 incrementally while streaming the body; compare when done; mismatch → XAmzContentSHA256Mismatch. Note: **a 2xx may only be issued after the body is fully consumed**, so PUT's success response naturally comes after verification |
| `UNSIGNED-PAYLOAD` | Skip body verification (common under HTTPS); only the header signature is verified |
| `STREAMING-AWS4-HMAC-SHA256-PAYLOAD` | aws-chunked encoding: L2 provides a `ChunkedSigV4BodyReader` decorator that strips the framing chunk by chunk and verifies the chunk signature chain, exposing a pure data stream downstream. Placed in L2 rather than the driver, so all drivers get support for free |
| The two `STREAMING-*-TRAILER` variants | The default upload shape of post-2025 SDKs: `STREAMING-UNSIGNED-PAYLOAD-TRAILER` (unsigned chunks, integrity carried by the trailing checksum) and `STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER` (chunk signature chain + trailer signature). The trailer section is parsed strictly line by line and matched against the `x-amz-trailer` declaration in both directions (an undeclared trailer is rejected, and so is a declared one that never arrives); declared `x-amz-checksum-*` trailers are verified against the full decoded payload (malformed → InvalidDigest, mismatch → BadDigest); the signed variant additionally verifies `x-amz-trailer-signature` (the `AWS4-HMAC-SHA256-TRAILER` string-to-sign over the hash of the canonicalized trailers, chained onto the final chunk signature). The trailer section is capped at 16KiB |

### 3.3 Interplay with the Streaming Model (incl. trailing checksums)

Signature verification uses the decorator pattern: `Sha256VerifyingReader` wraps the
raw `BodyReader`, passing data through while accumulating the digest; the storage
layer consumes this decorated reader. On verification failure, `complete()` throws
→ the handler takes the error path → LocalFs's staging temp file is cleaned up
by RAII, leaving no half-written object behind.

Division of labor for trailing checksums: header-declared `Content-MD5` /
`x-amz-checksum-*` are verified by `ChecksumVerifyingReader` (checksum_guard.h)
**outside** the chunked de-framing decorator; for checksums declared via
`x-amz-trailer` the expected value only arrives after the payload, so
`ChunkedSigV4BodyReader` accumulates digests over the decoded stream **inside**
and compares once the trailer is parsed. Both share one algorithm table
(crc32 / crc32c / crc64nvme / sha1 / sha256, `checksum_spec`) and the
`StreamingDigest` incremental digester.
The `x-amz-checksum-algorithm` / `x-amz-sdk-checksum-algorithm` declarations are
enforced too: an unknown algorithm is InvalidRequest; a request with a body that
declares an algorithm but supplies neither the matching header nor a matching
trailer declaration is InvalidRequest (a body-less declaration, e.g.
CreateMultipartUpload, is accepted — each part carries and is verified against
its own checksum).

### 3.4 Presigned URL

`X-Amz-Signature` and friends appear in the query: same canonical algorithm, payload
treated as `UNSIGNED-PAYLOAD`, with an additional `X-Amz-Expires` check. Expiry only
constrains the past side; an `X-Amz-Date` more than 15min ahead of the server is
likewise rejected (AccessDenied "Request is not valid yet"), preventing future
timestamps from extending the validity window indefinitely.

### 3.5 Credential Management

A static AK/SK table in the config file (secrets may reference environment variables)
is the phase-1 form; phase 2 has landed the three-source model
(static=root / file / dynamic), hot reload of the credential file, and lightweight
per-credential policy. The scheme for generating/querying AK/SK at runtime and
persisting them to storage, plus the phase-2 design, is detailed in
[credential-management.md](credential-management.md) §10.

## 4. XML Encoding/Decoding

S3's XML structures are simple and schema-fixed; no large XML library is introduced:

- **Generation**: a small writer (escaping + nesting stack), one pure function per
  response type: `to_xml(const ListResult&) → std::string`.
- **Parsing**: only three places need to parse request XML — CompleteMultipartUpload,
  DeleteObjects, CreateBucket (LocationConstraint). All structures are shallow;
  handled by a small home-grown parser (`src/s3/xml.cc`), with request XML
  limited to ≤ 1MiB.

## 5. Error Handling

Single source of truth in `src/s3/errors.h`:

```cpp
enum class S3ErrorCode { NoSuchBucket, NoSuchKey, AccessDenied,
                         InvalidAccessKeyId, SignatureDoesNotMatch,
                         BucketNotEmpty, EntityTooLarge, InvalidPart,
                         NoSuchUpload, PreconditionFailed, NotImplemented,
                         SlowDown, InternalError, ... };

struct S3Error : std::exception {   // L2/L3 uniformly throw this
    S3ErrorCode code; std::string message; std::string resource;
};
// Table-driven: code → (http_status, wire_code_string)
```

The response body is standard S3 error XML (with `Code/Message/Resource/RequestId`).
`RequestId` is generated per request (timestamp + random), written to both the
access log and the `x-amz-request-id` response header, serving as the correlation
key for end-to-end troubleshooting.

Unknown exceptions → `InternalError` (500); the log records the full stack trace,

Error codes specific to this implementation (no AWS counterpart): `QuotaExceeded`
(403, quotas), `NoSuchQuotaConfiguration` (404), `NoSuchTenant` (404),
`TenantAlreadyExists` (409), `TenantNotEmpty` (409); semantics in
[multi-tenancy.md](multi-tenancy.md).
and the response leaks no internal information.

## 6. Consistency and Semantics (Promises to Clients)

- **Read after PUT**: LocalFs provides strong read-after-write consistency via
  rename atomicity; CloudProxy inherits the remote's consistency (modern S3 is
  strongly consistent across the board).
- **Concurrent PUT on the same key**: last-write-wins, no version retention.
- **ETag**: single part = content MD5; multipart = `md5(concatenated part MD5s)-N`;
  both backends agree.
- **Conditional requests**: GET/HEAD/PUT support If-Match / If-None-Match;
  CopyObject / UploadPartCopy support x-amz-copy-source-if-*.
  Two boundaries aligned verbatim with AWS: PUT `If-None-Match` only supports `*`
  (AWS conditional writes likewise only support `*`; with an ETag → 501); AWS does
  not support multi-range GET (with commas), and the behavior matches AWS — the
  entire Range header is ignored and a full 200 response is returned.
  **Atomicity scope**: the conditional check and the commit share the backend's
  own atomic commit point (localfs commit-section per-key lock, duostore
  metadata transaction, cloudproxy passes the conditional headers upstream); L2
  only does a lock-free fast-fail precheck. Cross-instance the guarantee holds
  for duostore/cloudproxy; localfs/xlocalfs mutual exclusion is
  **process-local**, and multi-instance deployments sharing one filesystem are
  out of scope ([architecture.md](architecture.md) non-goal: the data plane
  assumes a single instance).
- **Security boundary of copy-source**: `x-amz-copy-source` does not pass through
  dispatch's path interception; two checks live in two places: reserved buckets
  starting with `.` (`.sys`) are rejected in `parse_copy_source()`
  (src/s3/handlers/common.h) (InvalidBucketName); the per-credential policy's
  "read" authorization on the source bucket is executed separately at the dispatch
  entry point (src/s3/service.cc) (AccessDenied).

## 7. Observability

L1 connection and rate-limit metrics (roadmap §4.2): `lights3_http_connections_total{result}`,
`lights3_http_connections_active`, `lights3_http_keepalive_closes_total`,
`lights3_http_timeouts_total{phase=idle|header|body|write}`,
`lights3_ratelimit_rejections_total{scope=ip|ak}`; see [http-adapter.md §2.2–§2.3](http-adapter.md).

- **Access log** (roadmap §5.2): one line per request through a dedicated
  `lights3.access` logger sharing the operational log's sink. Text format
  (`log.format: text`): `access <request_id> <AK|-> <method> "<path>" <status>
  <bytes> <total>ms api=<Route name> backend=<backend>:<backend ms>ms
  remote=<client address> bucket=<bucket|-> ttfb=<ms>ms ua="<User-Agent>"` — the
  first seven fields follow the S3 server access log, `path` and the UA are
  quoted and escaped (`"`, `\`, control characters) so a key with a space no
  longer breaks field splitting; the backend time comes from the metering
  decorator wrapped around the routed backends (`storage/metered_backend.h`),
  fed back through the request's cancellation token, so "gateway queueing or
  backend" is answered by one line. **Streaming responses** (GetObject and the
  like) write their line at end of body: `bytes` is what actually went out, the
  total includes the transfer, `ttfb` is the headers-ready moment (the quantity
  the metric histograms record); a client leaving mid-transfer gets the line
  from the decorator's destructor, flagged `truncated=1`. In JSON format
  (`log.format: json`) the whole operational log is one object per line
  `{"ts","level","thread","msg"}` and access records carry flat fields in the
  same object: `request_id / remote / ak / method / path / query / bucket / key /
  status / bytes / ms / ttfb_ms / auth_ms / handler_ms / backend_ms /
  backend_calls / api / backend / ua`; absent values are omitted rather than
  rendered empty (`ak` disappears for anonymous requests), ready for Loki/ELK.
- **Slow-request channel**: `log.slow_request_threshold` (e.g. `500ms`, 0 = off,
  hot-reloadable) — an access line whose total reaches the threshold is logged
  at WARN with the per-stage timings (text: `slow=1 auth=<ms> handler=<ms>
  backend_calls=<n>`; JSON: `"slow":true` plus those fields), so a deployment
  running at `log.level: warn` still sees the requests worth looking at. auth =
  dispatch entry to verified identity, handler = the whole route handler
  (backend time is a subset).
- **Distributed trace (lightweight tier, roadmap §5.4, `core/trace.h`)**:
  accepts W3C `traceparent` (strict: version 00, lowercase hex, non-zero ids; a
  malformed header counts as absent) and `tracestate` (passed through verbatim);
  without one the gateway starts a trace. Every request owns its span, the
  caller's span is recorded as parent. Where it lands: the access line's text
  slot `trace=<trace_id>/<span_id> [parent=<span>]`, the JSON fields `trace_id /
  span_id / parent_span_id`, dispatch's cancellation / internal-error WARN/ERROR
  lines (`trace=`), the audit data-plane record (`trace_id`); the response
  header `traceresponse: 00-<trace_id>-<span_id>-<flags>` (W3C draft) lets a
  client without a tracer quote it. Outbound: every cloudproxy request carries
  the gateway's own span in `traceparent` (the remote logs it as parent, so the
  link lands in both logs) plus `tracestate`
  ([cloudproxy-backend.md §2.2](cloudproxy-backend.md)), so multi-hop setups
  (gateway → remote lights3 behind cloudproxy / tiered cold layer) correlate on
  one trace id; background work (tiered demotion, GC) has no request context
  and sends no trace headers. Nothing is exported: otel-cpp instrumentation
  stays a long-term item.
- **Sink and async**: an empty `log.file` writes to stderr, otherwise to a
  size-rotated file (`max_size` / `max_files`, flushed every second and
  immediately at WARN and above); with `log.async: true` request threads only
  enqueue (capacity `async_queue`) and a dedicated writer thread drains, a full
  queue blocking or overwriting the oldest record per `async_overflow`;
  `Logger::shutdown` drains the queue before the process exits. The audit log
  ([multi-tenancy.md §5](multi-tenancy.md)) stays a separate file, untouched by
  these knobs.
- **Metrics** (Prometheus text format, `GET /-/metrics`, on the data-plane
  listener; `http.metrics_access: anonymous` (default, the classic scrape) or
  `root` — the latter requires the GET to be signed by a root credential, since
  the output carries business information such as bucket names and backend
  topology; hot-reloadable, moot when authentication is disabled altogether;
  `/-/healthz|readyz` stay anonymous):
  request counts / latency histograms (global and per `(api, backend)`:
  `lights3_api_requests_total{api,backend,class}`,
  `lights3_api_request_duration_seconds{api,backend}`; api is the dispatch
  table's `Route::name`, CopyObject/UploadPartCopy refined on
  `x-amz-copy-source`), in-flight requests, thread-pool queue depth, active
  multipart uploads. Backend-level metrics registered through the
  `core/metrics.h` registry (`backend=<name>` label) follow the L2 request
  metrics; among them `lights3_backend_op_seconds{backend,op}` histograms and
  `lights3_backend_errors_total{backend,op}` (5xx / transport exceptions count,
  4xx do not) are produced uniformly for all six backends by the metering
  decorator (roadmap §5.1).
  roadmap §5.3 additions: **exact status codes**
  `lights3_responses_by_status_total{status}` (sparse — a code gets a line once
  it occurred; the 206/304 share is the key website/CDN quantity); the
  **admission gate** `lights3_admission_wait_seconds` histogram (one sample per
  request, an immediate permit lands in the first bucket),
  `lights3_admission_queued_total` (requests that had to wait),
  `lights3_admission_cancelled_total` (cancelled while queued → 503),
  `lights3_transfer_stalls_total{direction=in|out}` (stall-guard cuts); **L1**
  `lights3_http_requests_total` (requests parsed at L1; ÷ accepted = keep-alive
  reuse factor), `lights3_http_tls_handshakes_total{result=ok|failed}`,
  `lights3_http_parse_errors_total` (malformed request line / headers /
  framing; see [http-adapter.md §2.2](http-adapter.md)); the **website plane**
  `lights3_website_events_total{event=anon_read|index_rewrite|error_document|redirect|throttled}`
  ([static-website.md §6](static-website.md)).
- **Health checks**: `GET /-/healthz` (process liveness) and `GET /-/readyz`
  (per-backend probing: `co_await list_buckets()` uniformly against all backends;
  any failure returns 503 with the failing backend names reported in the body).
  The three read endpoints accept only GET/HEAD; other methods get 405.
  The `/-/` prefix cannot collide with a legal bucket name (S3 bucket naming
  disallows that shape) — this claim holds **only under path-style addressing**,
  so the internal-endpoint branch is gated on "not vhost": under vhost
  addressing `req.path` is the object key in its entirety, `/-/metrics` is a
  legal key name and is read/written as an ordinary object, never shadowed by
  the internal endpoints.

## 8. Testing Strategy

1. **Unit tests**: SigV4 replays the full official AWS test-vector suite; XML,
   the routing dispatch table, and error mapping are covered item by item.
2. **Backend conformance suite**: the same set of cases runs parameterized against
   LocalFs / CloudProxy (against MinIO) / in-memory mock.
3. **Driver conformance suite**: the same set of HTTP behavior cases runs against
   every compiled-in driver.
4. **End-to-end**: CI starts the gateway (LocalFs backend) and runs real operation
   scripts with aws cli and boto3 (including 100MB-scale multipart, Range download,
   presigned URLs); additionally runs MinIO's `mint` compatibility test suite as a
   regression gate — `tests/e2e/run_mint.sh` has landed (starts lights3 + a
   `minio/mint` container against each other; explicit SKIP when docker is
   unavailable). mint requires docker daemon privileges and usually cannot run on
   local dev machines; since roadmap §6.1 it is registered as ctest `mint`
   (Not Run without docker) and prints a per-suite PASS/FAIL baseline
   ([testing.md §6](testing.md)); it is positioned as a gate for CI/privileged
   environments. The full suite includes explicitly unsupported APIs such as
   versioning/tagging, so starting with the `s3cmd` and `awscli` subsets is
   recommended (`run_mint.sh <bin> s3cmd awscli`).
