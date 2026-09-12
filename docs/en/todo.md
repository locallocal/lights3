# TODO: open items and plans

Successor of `docs/archive/backlog.md` (the open-items ledger since 2026-09-05;
its ten deferred items of §1 were all completed by 2026-09-06 in the order of
`docs/archive/backlog-sequence.md`, and both files were archived -- Chinese only,
like the other archived ledgers; `backlog §N` / `backlog-sequence ①…⑩` in source
comments refer to their sections). This document lists **only what is not
done**: follow-ups that wait on an external party, code that is in place but
could not be verified on the development box, new issues found by the
performance baseline, long-term items, and the explicit not-planned list.
Delete an item when it is done and write the implementation into the relevant
design document -- no struck-through history here. Each entry carries
**value** (high/medium/low) and **difficulty** (low/medium/high).

## 1. Follow-ups waiting on an external party

| Item | Source | State and remaining steps | Value | Difficulty |
| --- | --- | --- | --- | --- |
| Structured error codes merged upstream in client-c | backlog-sequence ⑨, [duostore-meta-tikv-design.md](storage/duostore-meta-tikv-design.md) | Done on our side (2026-09-06): patch in `third_party/patches/client-c` (README has the PR text and the post-merge steps); the sidecar picks by-code / by-message at compile time from the linked library. Remaining: open the upstream PR → after the merge bump the submodule pointer, delete the message branch and the patch directory | low | low |

## 2. Pending verification (implemented, not verifiable on the development box)

| Item | Source | Needs |
| --- | --- | --- |
| Docker image build and the compose profiles (default / redis / tikv / rados / e2e) | roadmap §6.3, [deployment.md §4](deployment.md) | A machine with a docker daemon: `docker compose build`, then `docker compose --profile e2e run --rm e2e` (runs the redis / tikv / rados e2e paths that SKIP locally) |
| CPack RPM | roadmap §6.3, [deployment.md §3.2](deployment.md) | A machine with `rpmbuild`: `cpack -G RPM`, check the scriptlets with `rpm -qp --scripts`, walk through install / upgrade / remove |
| `unit_tests` intermittently dies with `terminate called without an active exception` | 2 of 5 full runs on 2026-09-05 on this box, always right after `timer_stats_track_fired_and_pending` passed, during the 1.1 s slow callback of `timer_slow_callback_counted` (the "callback took 1.100s" line prints first); not reproducible under gdb; unrelated to feature work | Investigate in a gap: suspect the destruction order of a joinable `std::thread` in TimerQueue or the test fixture under load; capture a stack with `ulimit -c` / `catch throw` first |
| Multi-gateway multipart container e2e | [../archive/multi-gateway-multipart-design.md](../archive/multi-gateway-multipart-design.md) §4 ② | The compose `multi` profile (two lights3 + redis + rados + nginx round robin) and `deploy/docker/e2e-multi.sh` are in place (2026-09-09, `docker compose --profile multi config` passes); a machine with docker: `docker compose --profile multi run --rm e2e-multi`. The unit suite and the local e2e (the duostore-redis segment of `run_e2e.sh`) pass |
| mint compatibility baseline | roadmap §6.1, [testing.md §6](testing.md) | A machine with docker: `ctest -R mint -V`, record the per-suite PASS/FAIL/NA counts in testing.md §6 |

## 3. Found by the performance baseline ([performance-baseline.md](performance-baseline.md))

| Item | Symptom | Entry point | Value | Difficulty |
| --- | --- | --- | --- | --- |
| beast's TLS GET clearly lags | 4 MiB GET at 4.6k ops/s plaintext but 1.5k under TLS, while the other three drivers sit around 3.0k under TLS | The `TlsStream` write path in `src/http/drivers/beast/beast_server.cc`: asio ssl record splitting and one strand hop per chunk; start with an `strace -c` comparison of plaintext vs TLS syscall counts | medium | medium |
| Request-body path not optimized symmetrically | PUT is flat across drivers; only beast improved, through the read-granularity bug fix; the queue block shaping of backlog-sequence ⑩ gained httplib's 4 MiB PUT only about 3% | The request body is a pull model that must keep backpressure, so prefetch needs care; candidates: larger recv calls in builtin's `SocketBodyReader`, beast's per-chunk `expires_after` timer re-arm | medium | medium |

## 4. Long-term / architectural (settle the target scenario first)

| Item | Notes |
| --- | --- |
| Versioning | Architecture-level (key layout of six backends / List semantics / delete markers / GC all move); if ever, **start from duostore** (meta is a KV, add a version dimension), localfs's key→path mapping cannot hold multiple versions |
| S3 Tables ⑥ optional items | ①–⑤ are implemented and merged ([s3-tables-design.md](s3-tables-design.md)). What remains is optional: Iceberg views, the `/_iceberg/v1` alias (`tables.compat_prefix` is parsed but not wired), `reportMetrics` into the audit log, compaction candidate planning output, a duostore-meta catalog backing (`docs/s3-tables/step-6-optional.md`); pick up when a real client needs one |
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
| cloudproxy outbound streaming-signed uploads | High complexity for integrity over plaintext HTTP only ([cloudproxy-design.md](storage/cloudproxy-design.md)) |
| A pack layer on the rados data plane | Argued as a design boundary in the code (small-object amplification is left to BlueStore `min_alloc_size`) |
| CivetWeb or other new HTTP drivers | The four drivers cover the design space ([http-adapter.md §3.4](http-adapter.md)) |
| GitHub Actions CI | Deliberately removed; automation investment goes into the local script matrix (`scripts/check-all.sh`, [testing.md §8](testing.md)) |

## 6. Maintenance rules

- A new entry states its **source / entry point / value / difficulty**; delete
  it when done and write the implementation into the design document.
- Source comments keep citing the archived reasoning as `roadmap §N`,
  `backlog §N` and `backlog-sequence ①…⑩`; entries here are cited as `todo §N`.
- Historical ledgers, read-only (Chinese): `docs/archive/gaps.md`, `issues.md`,
  `roadmap.md`, `backlog.md`, `backlog-sequence.md`.
