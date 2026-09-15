# TODO: open items and plans

Successor of `docs/archive/backlog.md` (the open-items ledger since 2026-09-05;
its ten deferred items of §1 were all completed by 2026-09-06 and the file was
archived -- Chinese only; `backlog §N` in source comments refers to its
sections). This document lists **only what is not
done**: follow-ups that wait on an external party, code that is in place but
could not be verified on the development box, long-term items, and the explicit
not-planned list.
Delete an item when it is done and write the implementation into the relevant
design document -- no struck-through history here. Each entry carries
**value** (high/medium/low) and **difficulty** (low/medium/high).

## 1. Follow-ups waiting on an external party

| Item | Source | State and remaining steps | Value | Difficulty |
| --- | --- | --- | --- | --- |
| Structured error codes merged upstream in client-c | [duostore-meta-tikv-design.md](../architecture/storage/duostore-meta-tikv-design.md) | Done on our side (2026-09-06): patch in `third_party/patches/client-c` (README has the PR text and the post-merge steps); the sidecar picks by-code / by-message at compile time from the linked library. Remaining: open the upstream PR → after the merge bump the submodule pointer, delete the message branch and the patch directory | low | low |

## 2. Pending verification (implemented, not verifiable on the development box)

| Item | Source | Needs |
| --- | --- | --- |
| Docker image build and the compose profiles (default / redis / tikv / rados / e2e; `multi` is the row below) | [deployment.md §4](../usage/deployment.md) | A machine with a docker daemon: `docker compose build`, then `docker compose --profile e2e run --rm e2e` (runs the redis / tikv / rados e2e paths that SKIP locally) |
| CPack RPM | [deployment.md §3.2](../usage/deployment.md) | A machine with `rpmbuild`: `cpack -G RPM`, check the scriptlets with `rpm -qp --scripts`, walk through install / upgrade / remove |
| `unit_tests` intermittently dies with `terminate called without an active exception` | 2 of 5 full runs on 2026-09-05 on this box, always right after `timer_stats_track_fired_and_pending` passed, during the 1.1 s slow callback of `timer_slow_callback_counted` (the "callback took 1.100s" line prints first); not reproducible under gdb; unrelated to feature work. A second spot, seen on 2026-09-12 during `duostore_orphan_scan_defers_to_peer_write_lease`, was diagnosed and fixed the same day: the phase-2 wait loop `chunk_files_on_disk < 1` was satisfied at once by the tail chunk left over from phase 1, so the scan finished before A's new chunk reached disk, the CHECK on `skipped_leased` being 0 threw, and the destructor of a joinable `std::thread` turned the failure into a terminate; the loop now waits relative to the file count before the start, and the writer thread is released and joined by an RAII guard. An aborted process flushes no gcov counters, so `make coverage` then reports a low figure and WARNs at the end | Only the timer spot remains: the destruction order during a slow TimerQueue callback under load; capture a stack with `catch throw` / `ulimit -c` first |
| Multi-gateway multipart container e2e | [deployment.md §4](../usage/deployment.md) | The compose `multi` profile (two lights3 + redis + rados + nginx round robin) and `docker/e2e-multi.sh` are in place (2026-09-09, `docker compose --profile multi config` passes); a machine with docker: `cd docker && docker compose --profile multi run --rm e2e-multi`. The unit suite and the local e2e (the duostore-redis segment of `run_e2e.sh`) pass |
| mint compatibility baseline | [testing.md §6](testing.md) | A machine with docker: `ctest -R mint -V`, record the per-suite PASS/FAIL/NA counts in testing.md §6 |
| S3 Tables manual verification with Spark / Trino | [s3-tables-design.md §13](../architecture/s3-tables-design.md) | Only the PyIceberg / DuckDB smoke passed here (testing.md §6); no Spark / Trino on this box: configure `rest.sigv4-enabled` from the §13 templates, walk through create / append / `rewrite_data_files` (Spark) and SIGV4 read-write (Trino), record the result in testing.md §6; also add a Spark-written manifest (negative block counts) to `tests/fixtures/tables/` (every fixture there is PyIceberg-generated) |

## 3. Long-term / architectural (settle the target scenario first)

| Item | Notes |
| --- | --- |
| Versioning | Architecture-level (key layout of six backends / List semantics / delete markers / GC all move); if ever, **start from duostore** (meta is a KV, add a version dimension), localfs's key→path mapping cannot hold multiple versions |
| SSE-C / SSE-S3 | Server-side encryption; key sourcing and the ETag / checksum semantics have to be settled first |
| Full OpenTelemetry instrumentation | The lightweight trace layer exists (W3C traceparent pass-through, one span per request, log correlation, [s3-protocol.md §7](../architecture/s3-protocol.md)); exporting spans through otel-cpp is long-term |
| HTTP/2 | Mainstream S3 SDKs still speak HTTP/1.1; only CDN / L7 fronting needs it; terminating h2 at a fronting proxy is in [tls.md §6](../usage/tls.md) |
| Independent cancellation source on client disconnect | A deliberate trade-off: long handlers are bounded by `request_timeout`, drivers notice the disconnect at the next socket operation ([http-adapter.md §2.3](../architecture/http-adapter.md)) |
| Iceberg multi-table transactions `/transactions/commit` | [s3-tables-design.md §15](../architecture/s3-tables-design.md) said "revisit once the duostore-meta backing exists"; ⑥ delivered it: one `kv_put_batch` can write the pointers + records of several tables, so atomicity is available. Missing: the endpoint itself, combined validation of requirements across tables, the refusal on the object backing (406, which cannot do it) and the engine-side switches (`DISABLE_MULTI_TABLE_COMMIT` in the DuckDB template). Confirm an engine actually needs it before starting |

## 4. Explicitly not planned

| Item | Reason |
| --- | --- |
| Object Lock / Legal Hold | No versioning foundation, WORM semantics cannot hold |
| Bucket Policy (IAM language) | Per-credential policy already covers tenant isolation, anonymous public buckets are handled by the website face; an IAM evaluator is a subsystem of its own, out of proportion |
| SigV2 | Retired by AWS, clients have all but disappeared |
| presigned POST | Needs half a streaming multipart/form-data parser first; CORS + presigned PUT is the more modern path |
| cloudproxy outbound streaming-signed uploads | High complexity for integrity over plaintext HTTP only ([cloudproxy-design.md](../architecture/storage/cloudproxy-design.md)) |
| A pack layer on the rados data plane | Argued as a design boundary in the code (small-object amplification is left to BlueStore `min_alloc_size`) |
| CivetWeb or other new HTTP drivers | The four drivers cover the design space ([http-adapter.md §3.4](../architecture/http-adapter.md)) |
| GitHub Actions CI | Deliberately removed; automation investment goes into the local script matrix (`scripts/check-all.sh`, [testing.md §8](testing.md)) |

## 5. Maintenance rules

- A new entry states its **source / entry point / value / difficulty**; delete
  it when done and write the implementation into the design document.
- Source comments keep citing the archived reasoning as `backlog §N`; entries
  here are cited as `todo §N`.
- Historical ledger, read-only (Chinese): `docs/archive/backlog.md`.
