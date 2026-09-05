# Backlog: open items and future plans

Successor of [archive/roadmap.md](../archive/roadmap.md) (the planning ledger
from the 2026-08-25 walkthrough, archived on 2026-09-05 once every entry was
closed out; `roadmap §N` in source comments refers to that archived file's
sections). This document lists **only what is not done**: designs kept for a
later phase, code that is in place but could not be verified on the development
box, new issues found by the performance baseline, long-term items, and the
explicit not-planned list. Delete an item when it is done and write the
implementation into the relevant design document -- no struck-through history
here, unlike the roadmap. Each entry carries **value** (high/medium/low) and
**difficulty** (low/medium/high).

## 1. Kept for a later phase (design entry points settled, triggered by demand)

Implementation order, scope and acceptance per item: [backlog-sequence.md](backlog-sequence.md).

| Item | Source | State and entry point | Value | Difficulty |
| --- | --- | --- | --- | --- |
| Multi-instance STS session table | roadmap §2.6 | `AssumeRole` sessions are single-instance in-memory state (`src/s3/auth/credential_store.*`, sessions are short-lived); sharing across gateways means persisting to `.sys` or the meta engine, reusing the credential `sync_interval` mechanism | medium | medium |
| Admin endpoint for scrub / fsck | roadmap §3.1 | The offline `lights3 fsck` and online `s3adm fsck` cover it; an endpoint would follow the root-only `POST /-/admin/config/reload` pattern | low | low |
| duostore meta incremental backup / PITR | roadmap §3.7 | `dump` is already a consistent full snapshot (`IMetaStore::snapshot()`); incremental needs WAL-level export, different for each of the four engines | medium | high |
| Structured error codes upstream in client-c | roadmap §3.7 (tikv T5) | The sidecar classifies conflicts from kvrpcpb structures first and string-matches only as defense in depth; an upstream PR is optional | low | medium |
| Cross-gateway meta cache invalidation | roadmap §3.8 | Shared meta (redis/tikv) runs under the bounded-staleness contract of `meta_cache_ttl` ([storage/duostore-core.md §7.1](../storage/duostore-core.md)); redis could use pub/sub, tikv has no equivalent | medium | high |
| mTLS client certificate → credential / tenant identity | roadmap §4.1 | `tls_client_auth` verifies the chain but maps no identity ([tls.md](tls.md)); needs a rule binding certificate fields to credentials | medium | medium |
| Hot add / remove of backend instances | roadmap §4.4 | `reload_config` applies the safe subset plus `buckets.rules`; driver / backends / `default_backend` / `auth.*` explicitly need a restart ([config-reload.md](config-reload.md)) | medium | high |
| Separate admin port | roadmap §5.3 | Guarded today by `http.metrics_access: root` and root-only admin routes; port-level isolation is left to a fronting proxy ([tls.md §6](tls.md)) | low | low |
| `HeaderMap` linear scan / `BlockQueue` double copy | roadmap §4.3 ⑧ | Small in absolute terms; touch only with profile evidence | low | low |

## 2. Pending verification (implemented, not verifiable on the development box)

| Item | Source | Needs |
| --- | --- | --- |
| Docker image build and the compose profiles (default / redis / tikv / rados / e2e) | roadmap §6.3, [deployment.md §4](deployment.md) | A machine with a docker daemon: `docker compose build`, then `docker compose --profile e2e run --rm e2e` (runs the redis / tikv / rados e2e paths that SKIP locally) |
| CPack RPM | roadmap §6.3, [deployment.md §3.2](deployment.md) | A machine with `rpmbuild`: `cpack -G RPM`, check the scriptlets with `rpm -qp --scripts`, walk through install / upgrade / remove |
| mint compatibility baseline | roadmap §6.1, [testing.md §6](testing.md) | A machine with docker: `ctest -R mint -V`, record the per-suite PASS/FAIL/NA counts in testing.md §6 |

## 3. Found by the performance baseline ([performance-baseline.md](performance-baseline.md))

| Item | Symptom | Entry point | Value | Difficulty |
| --- | --- | --- | --- | --- |
| beast's TLS GET clearly lags | 4 MiB GET at 4.6k ops/s plaintext but 1.5k under TLS, while the other three drivers sit around 3.0k under TLS | The `TlsStream` write path in `src/http/drivers/beast/beast_server.cc`: asio ssl record splitting and one strand hop per chunk; start with an `strace -c` comparison of plaintext vs TLS syscall counts | medium | medium |
| Request-body path not optimized symmetrically | PUT is flat across drivers; only beast improved, through the read-granularity bug fix | The request body is a pull model that must keep backpressure, so prefetch needs care; candidates: larger recv calls in builtin's `SocketBodyReader`, beast's per-chunk `expires_after` timer re-arm | medium | medium |

## 4. Long-term / architectural (settle the target scenario first)

| Item | Notes |
| --- | --- |
| Versioning | Architecture-level (key layout of six backends / List semantics / delete markers / GC all move); if ever, **start from duostore** (meta is a KV, add a version dimension), localfs's key→path mapping cannot hold multiple versions |
| SSE-C / SSE-S3 | Server-side encryption; key sourcing and the ETag / checksum semantics have to be settled first |
| Full OpenTelemetry instrumentation | The lightweight trace layer exists (W3C traceparent pass-through, one span per request, log correlation, [s3-protocol.md §7](s3-protocol.md)); exporting spans through otel-cpp is long-term |
| HTTP/2 | Mainstream S3 SDKs still speak HTTP/1.1; only CDN / L7 fronting needs it; terminating h2 at a fronting proxy is in [tls.md §6](tls.md) |
| Independent cancellation source on client disconnect | A deliberate trade-off: long handlers are bounded by `request_timeout`, drivers notice the disconnect at the next socket operation ([http-adapter.md §2.3](http-adapter.md)) |

## 5. Explicitly not planned

| Item | Reason |
| --- | --- |
| Object Lock / Legal Hold | No versioning foundation, WORM semantics cannot hold |
| Bucket Policy (IAM language) | Per-credential policy already covers tenant isolation, anonymous public buckets are handled by the website face; an IAM evaluator is a subsystem of its own, out of proportion |
| SigV2 | Retired by AWS, clients have all but disappeared |
| presigned POST | Needs half a streaming multipart/form-data parser first; CORS + presigned PUT is the more modern path |
| cloudproxy outbound streaming-signed uploads | High complexity for integrity over plaintext HTTP only ([cloudproxy-backend.md](cloudproxy-backend.md)) |
| A pack layer on the rados data plane | Argued as a design boundary in the code (small-object amplification is left to BlueStore `min_alloc_size`) |
| CivetWeb or other new HTTP drivers | The four drivers cover the design space ([http-adapter.md §3.4](http-adapter.md)) |
| GitHub Actions CI | Deliberately removed; automation investment goes into the local script matrix (`scripts/check-all.sh`, [testing.md §8](testing.md)) |

## 6. Maintenance rules

- A new entry states its **source / entry point / value / difficulty**; delete
  it when done and write the implementation into the design document.
- Source comments keep citing the archived reasoning as `roadmap §N`; entries
  here are cited as `backlog §N`.
- Historical ledgers, read-only: [archive/gaps.md](../archive/gaps.md),
  [archive/issues.md](../archive/issues.md), [archive/roadmap.md](../archive/roadmap.md).
