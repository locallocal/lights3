# Testing: Matrix, e2e Coverage, Fuzzing, Fault Injection, Bench/Soak, Coverage (roadmap §6.1)

The basic shape of unit tests and e2e is in [s3-protocol.md §8](../architecture/s3-protocol.md);
this document covers the eight items roadmap §6.1 filled in: the ctest inventory
and labels, the website / lights3-ctl / fault-injection e2e sections, the fuzz
harnesses, the fault-injection facade, the performance gate and soak, mint in
ctest, ubsan/coverage builds, and the one-shot matrix script.

## 1. ctest inventory and labels

| Test | Content | Labels |
| --- | --- | --- |
| `unit_tests` | every unit case (including `test_fault.cc`); `LIGHTS3_TEST_FILTER=sub1,sub2` runs only the cases whose name contains one of the substrings (a sanitizer build that dies in one file can still cover another) | — |
| `e2e_<driver>` / `e2e_<backend>` | one `run_e2e.sh` parameterized by driver × backend (16 variants), including the S3 Tables section (configured with `tables.enabled: true`) | — |
| `fuzz_regression_<target>` | the 6 harnesses replaying their corpora (§3) | `fuzz` |
| `monitoring_assets` | monitoring-asset reconciliation ([monitoring.md §5](../usage/monitoring.md)) | — |
| `bench_gate` | 3-second throughput / latency gate (§5) | `perf` |
| `soak_smoke` | 30-second soak: RSS / fd / leak assertions (§5) | `perf` `soak` |
| `mint` | MinIO mint's s3cmd + awscli subset; explicit SKIP without docker (§6) | `mint` |
| `install_tree` | `cmake --install` into a scratch prefix: layout, unit relocation, config preservation, `--version` format, `sh -n` on the maintainer scripts ([deployment.md §2](../usage/deployment.md)) | — |
| `tables_smoke` | S3 Tables client smoke: `run_tables_smoke.sh` starts a memory gateway and runs `scripts/tables/pyiceberg_smoke.py` and `duckdb_smoke.py`; runs only with `LIGHTS3_TABLES_SMOKE=1`, SKIP otherwise (§6) | `tables-smoke` |

Load-sensitive or externally dependent items are excluded by label:
`ctest -LE "perf|mint"` (`tables-smoke` SKIPs on its own by default;
`check-all.sh --with-tables-smoke` turns it on). The two-gateway tables cases
(`tables_multi_gateway_suite.h`) live in `unit_tests`: memory always runs, the
redis / tikv variants SKIP together with the external-dependency probes of
`duostore_redis_*` / `duostore_tikv_*`.

`e2e_duostore_redis` / `_tikv` / `_rados` each probe an external dependency and
SKIP explicitly without it: redis looks for `redis-server` and spawns a private
instance, or `LIGHTS3_TEST_REDIS_URI=redis://host:port` points at an external
one (unique `redis_prefix` per run); tikv reads `LIGHTS3_TEST_PD_ADDR`; rados
reads `LIGHTS3_TEST_RADOS_CONF` + `_POOL` (`LIGHTS3_TEST_RADOS_CLIENT` selects the
client name, default `client.admin`). `docker compose --profile e2e run --rm e2e`
brings all three up and runs them in one go ([deployment.md §4.3](../usage/deployment.md)).

## 2. New e2e sections (`tests/e2e/run_e2e.sh`)

- **Static website** (zero coverage before; anonymous reads are the
  security-sensitive face): the static entry `e2esite` from the config plus the
  dynamic `dynsite` through the `?website` API. Anonymous GET/HEAD of objects,
  index rewrite for bucket root and directory keys, `/prefix` slash 302, the
  error document with 404, `x-amz-website-redirect-location` 301, anonymous
  listing / `?uploads` / writes / deletes / non-website buckets all refused,
  static entries immutable through the API (405), non-root cannot Put the
  configuration, the anonymous plane closes as soon as the configuration is
  deleted, `lights3_website_events_total` counts.
- **S3 Tables step ①** ([s3-tables-design.md §13](../architecture/s3-tables-design.md)):
  `/iceberg/v1/config` → enable a table bucket (non-root 403) → namespace / table →
  metadata.json under the reserved prefix readable but not writable → a manual
  CommitTable (add-snapshot pointing at a pre-placed manifest-list) → replay with the
  same commit-id is idempotent → stale requirement 409 → missing manifest 409 → rename →
  DeleteBucket on a non-empty table bucket 409 → `purgeRequested=maybe` 400 → drop →
  bucket name `iceberg` 400 → `lights3_tables_commits_total` counts; step ②: the
  `s3tables` signing name passes on the catalog face and answers 400 on the S3 face,
  the session credential vended through `X-Iceberg-Access-Delegation` gets PUT 200
  inside the table prefix / 403 outside / metadata read 200 / AssumeRole 403, a
  lifecycle rule on a table bucket is accepted with a WARN. Unit tests:
  `test_tables_iceberg.cc` (pure functions), `test_tables_catalog.cc` (commit
  protocol, idempotent replay, the crash windows of the
  `tables.commit.after_stage|after_cas` fault points, rename), `test_tables_rest.cc`
  (endpoints, error model, policy and list filtering, tenant gate, `s3tables`
  signing, credential vending, lifecycle skip, guards, `/config.endpoints`
  consistent with the route table), `policy_narrowing_for_vended_sessions` in
  `test_credentials.cc`. Step ③: the e2e section switches to PyIceberg-written
  manifest fixtures (`tests/fixtures/tables/`); deep validation passes with
  `lights3.snapshot-validation: deep`, missing data file 409, corrupt Avro 409,
  `If-None-Match` 304, `catalog/diagnostics` reports `Committed` with no unreferenced
  files, `catalog/recovery` has nothing to do, a read-only credential gets
  diagnostics 200 / recovery 403. Unit tests: `test_tables_avro.cc` (hand-built OCF
  for every type, truncation / sync / depth / unknown codec, field-by-field comparison
  against the PyIceberg fixtures), `test_tables_catalog.cc` adds the six deep-validation
  409s and both codec states, the five diagnostics states with `recover` leaving the
  pointer alone, the five rename fault points recovered by another instance + the
  Prepared timeout rollback, `test_tables_rest.cc` diagnostics / recovery / 304 /
  `skipped-codec`, `test_admin_jobs.cc` fsck extension merge and the three catalog
  reconciliation findings. Step ⑥: the e2e section adds the `/_iceberg` alias
  (/config reports `lights3.catalog-compat-prefix`, bucket name `_iceberg` 400), views
  create / list / HEAD / same-name table 409 / replace bumps the version / stale uuid
  409 / rename / drop, `reportMetrics` 204, `compaction-candidates` in plan. Unit
  tests: `test_tables_optional.cc` (`catalog_store_suite.h` against the object and
  duostore-rocksdb backings, the view lifecycle on both backings, the view metadata
  model, compaction candidates, the `catalog_backing` setting, atomic commits on the
  duostore backing with a single winner out of 20 concurrent + export/import),
  `*_tables_catalog_store` in `test_duostore_sqlite|redis|tikv.cc`, `case_kv_facade`
  of `meta_store_suite.h` (four engines), `tables_rest_views_compat_prefix_and_metrics`
  in `test_tables_rest.cc` (`tables.metrics` in the audit file). Step ④: the e2e
  section adds `maintenance/config` defaults and per-table settings, a plan job bound
  to the version token with no candidates inside the safety window, a run job that
  deletes no file, plan 403 for a read-only credential, the admin plane
  `/-/admin/tables/...` 202, `lights3-ctl tables
  status|list|plan|run|diagnose|recover|purge` (purge without `--yes` exits 2, after
  purge metadata 404 and catalog 404). Unit tests: `test_tables_maintenance.cc`
  (retention set and safety window, snapshot expiry with the tag / ref rules /
  `remove-snapshots` commit, orphans fail-closed, StalePlan / `delete_enabled` /
  re-check before delete, purge, the runner skipping busy tables through the job
  framework + tombstone TTL + background tick), `tables_rest_maintenance_endpoints`
  in `test_tables_rest.cc` (endpoints, permissions, admin plane, the
  `purgeRequested=true` job).
- **lights3-ctl cross-validation**: curl signs with libcurl's SigV4, lights3-ctl with its
  own implementation, both against one server. `cred create/list/get
  --show-secret/delete` (curl signs with the credential lights3-ctl minted, 403 after
  revocation), `website set/get/delete` (curl reads back what lights3-ctl wrote),
  `bench put/get` error-free, `fsck` with zero mismatches over the bench objects;
  the existing `usage/quota/tenant/reload` cases stay.
- **Multi-gateway multipart** (`duostore-redis` variant,
  [archive/multi-gateway-multipart-design.md §4 ②](../../archive/multi-gateway-multipart-design.md)):
  two gateways on the same redis meta + the same data root; create on A, 5 parts
  uploaded alternating B/A, ListParts / ListMultipartUploads consistent on both sides,
  complete on B, HEAD/GET on A; the composite ETag is computed independently by the
  script from the part ETags and compared. The same scenario matrix on the unit side
  is `tests/unit/multi_gateway_suite.h` (instantiated once each for redis / tikv, SKIP
  without an instance); the container version with the compose `multi` profile is in
  [deployment.md §4.2](../usage/deployment.md).
- **Fault injection** (localfs / xlocalfs / tiered variants): a second instance
  started with `LIGHTS3_FAULTS=localfs.write:1:EIO,xlocalfs.write:1:EIO` — the
  first PUT answers 500, the object does not exist, the retry succeeds,
  `lights3_backend_errors_total{op="put_object"}` and
  `lights3_responses_by_status_total{status="500"}` each count 1.

Each section caught a real defect: the httplib driver rewrote "≥400 responses
with an empty body" (HEAD 404, streamed error documents) into 405 — fixed (the
error handler leaves any response carrying L2's `x-amz-request-id` / `Server`
headers alone).

## 3. Fuzzing

`tests/fuzz/fuzz_<target>.cc`, one `LLVMFuzzerTestOneInput` each, all parsers
reachable without authentication:

| target | entry | corpus |
| --- | --- | --- |
| `xml` | `s3::xml_parse` (Complete/DeleteObjects/?website bodies) | `corpus/xml/` |
| `uri` | `percent_decode` / `percent_decode_query` / `aws_uri_encode` round-trip invariant | `corpus/uri/` |
| `http_parse` | `http/drivers/common.h`: `parse_target` / `parse_content_length` / `parse_chunk_size` / `parse_body_framing` (first byte selects the routine) | `corpus/http_parse/` |
| `sigv4` | `SigV4Authenticator::verify`: Authorization header and presigned query ("header block\n\nquery") | `corpus/sigv4/` |
| `aws_chunked` | a correctly signed header set + the fuzz bytes as body, drained through the aws-chunked de-framing reader `verify()` installs (unsigned-trailer / signed / signed+trailer) | `corpus/aws_chunked/` |
| `duostore_codec` | `codec::decode_*` (records another gateway wrote into a shared meta engine) | `corpus/duostore_codec/` |

Two build modes: the default (any compiler) links `fuzz_driver.cc` — the binary
replays the corpus directory plus a single-byte sweep, the ctest
`fuzz_regression_*` crash-regression gate; `./build.sh --fuzz` (switches to
clang, `-DLIGHTS3_FUZZ_LIBFUZZER=ON`, ASan on by default) links libFuzzer and
really mutates:

```bash
./build.sh --fuzz -B build-fuzz
mkdir -p build-fuzz/corpus-xml
build-fuzz/fuzz_xml build-fuzz/corpus-xml tests/fuzz/corpus/xml -max_total_time=600
```

libFuzzer writes newly interesting inputs into the **first** corpus directory:
keep that working directory under build-fuzz and leave
`tests/fuzz/corpus/<target>/` to hand-written seeds; a crashing input picked
out and dropped into the seed directory becomes a permanent regression. libFuzzer mode needs the whole tree to compile with clang — for
that `YamlNode`'s special members moved out of line (clang instantiates them
with the recursive `pair<string, YamlNode>` member still incomplete when they
are defaulted in-class) and one narrowing was fixed; under clang 21 + ASan the
core library, the six harnesses and unit_tests all build; the GCC 15 + ASan
`build-asan` runs the full unit_tests, 521 cases passing (2026-09-06, once the
`Task` resume trampoline landed — before it, a synchronously completing read
chain overflowed the stack in `test_http_drivers` at -O0 and aborted the whole
run, [concurrency.md §2](../architecture/concurrency.md)). Idle 5–10 second runs on this machine: xml 150k executions, uri
3.4M, http_parse 2.47M, sigv4 420k, aws_chunked 14k, duostore_codec 190k, no
crashes.

## 4. Fault injection

`core/fault.h`: named points, armed from the environment or programmatically,
in the same binary that ships (one relaxed atomic load on the hot path while
nothing is armed). Grammar `point[:count][:errno]`, comma separated; count
defaults to 1, 0 = until reset; errno by symbolic name or number (default EIO):

```bash
LIGHTS3_FAULTS="localfs.write:1:EIO,duostore.pack.fdatasync:0:ENOSPC" lights3 --config ...
```

| Point | Where | Effect |
| --- | --- | --- |
| `localfs.write` | `::write` into the staging tmp (put / upload_part / complete stitching) | InternalError, no leftover object or tmp |
| `localfs.rename` | the commit rename of an object / cached data | InternalError (with the errno text) |
| `localfs.fsync` | fdatasync of a staged file (`fsync_file` and `fsync_path`) | InternalError; side fix: `fsync_path` used to swallow real fdatasync errors, it now throws per the durability promise of the 200 |
| `xlocalfs.write` | the io_uring write pipeline (`drain_to_tmp`) | InternalError |
| `duostore.pack.pwrite` / `duostore.pack.fdatasync` | pack record append / durability sync | InternalError, earlier data intact |
| `redis.command` | the hiredis command layer: simulated connection failure | reads retry once on a fresh connection (`reconnects` counted), writes InternalError |
| `rados.submit` | `rados_aio_*` submission: returns `-errno` | the existing rados error path |
| `tables.commit.after_stage` / `tables.commit.after_cas` | table catalog commit ([s3-tables-design.md §5.4](../architecture/s3-tables-design.md)): after the STAGED record, before the pointer CAS / after the pointer CAS, before the record is finalized | crash windows reported by `catalog/diagnostics` and recoverable (`test_tables_catalog.cc`) |
| `tables.rename.after_prepare` / `after_fence` / `after_destination` / `after_tombstone` / `before_cleanup` | table rename (design §5.6), after each of the five steps and before the next: intent written / source fenced / destination written / source tombstoned / intent about to be deleted | another instance can take over the recovery, Prepared times out and rolls back (`test_tables_catalog.cc`) |

`fault::kPoints` is the single list; `test_fault.cc` greps the sources to
confirm every point is wired (a point in the table without code fails the
test). Unit tests cover the three localfs points and the two duostore points;
the redis point lives in `test_duostore_redis.cc` (runs with an instance);
rados/tikv have no local cluster and are compile-verified only. libfiu exists
only as client-c's nested submodule and is not a repository-wide dependency.

## 5. Performance gate and soak

- `scripts/bench_matrix.sh <lights3> <lights3-ctl> [--drivers a,b] [--tls on|off|both]
  [--duration N] [--concurrency N] [--size SZ] [--objects N] [--modes put,get]
  [--io-threads N] [--json FILE] [--label TEXT] [--keep-log]`: the performance baseline matrix (roadmap §4.3) -- one
  localfs gateway per (driver × TLS) cell running `lights3-ctl bench put/get`, output
  as a Markdown table plus one JSON line per cell; the driver list defaults to
  the `drivers:` line of `lights3 --version`. Results are kept in
  [performance-baseline.md](performance-baseline.md).

- `scripts/bench_gate.sh <lights3> <lights3-ctl> [--duration N] [--concurrency N] [--size SZ]
  [--min-put-ops N] [--min-get-ops N] [--max-p99-ms N] [--keep-log]`: a memory-backend gateway + `lights3-ctl bench
  put/get --output=json`; the JSON summary is parsed and a throughput floor (default 300
  ops/s) and a p99 ceiling (default 500 ms) asserted; `LIGHTS3_BENCH_DURATION` / `_CONCURRENCY` / `_SIZE` / `_MIN_PUT_OPS` / `_MIN_GET_OPS` /
  `_MAX_P99_MS` override the same-named settings. ctest `bench_gate` uses 3 seconds.
- `scripts/soak.sh <lights3> <lights3-ctl> [--seconds N] [--backend
  localfs|duostore|memory] [--concurrency N] [--max-rss-growth PCT] [--max-fd-growth N]
  [--keep-log]` (`LIGHTS3_SOAK_SECONDS` / `_BACKEND` / `_CONCURRENCY` / `_MAX_RSS_GROWTH` /
  `_MAX_FD_GROWTH` override the same-named settings): rotates
  put/get/stat/list/delete-pool rounds, sampling RSS, fd count,
  `lights3_duostore_gcq_depth` and `lights3_multipart_active` per round; at the
  end asserts RSS growth since warm-up < 25%, fds ≤ warm-up + 16, no multipart
  leftovers, duostore GC queue back at 0, no ERROR lines. ctest `soak_smoke`
  runs 30 seconds; for hours: `scripts/soak.sh build/lights3 build/lights3-ctl
  --seconds 7200 --backend duostore`.

The gate caught a real issue the day it went in: the lights3-ctl client had no
TCP_NODELAY, so every small PUT stalled ~40 ms on Nagle + delayed ACK (identical
across all three drivers, fine from 256K up) — fixed in `lights3_ctl_common.cc`, 16K
PUTs went from 98 ops/s to ~20k ops/s.

## 6. mint

`run_mint.sh` is registered as ctest `mint` (the `s3cmd awscli` subset,
`SKIP_RETURN_CODE 77`, so a docker-less box shows Not Run rather than a pass).
After a run it prints per-suite PASS/FAIL/NA counts from `log.json` as the
baseline record. The docker daemon is unreachable on this machine, so **the
baseline is not recorded yet**: run `ctest -R mint -V` once on a privileged
machine and paste the summary here.

**S3 Tables client smoke** (ctest `tables_smoke`, [s3-tables-design.md §13](../architecture/s3-tables-design.md)):
passed on this machine on 2026-09-12 -- PyIceberg 0.12.0 (pyarrow 22.0, boto3 signing
with SigV4, `rest.signing-name=s3`) 16/16: enable the table bucket, create a table,
append ×2, reload and scan, a commit from a stale handle retried automatically by
PyIceberg after refresh, idempotent replay with the same commit-id, maintenance
plan/run, diagnostics all Committed, purge; DuckDB 1.5.5 (iceberg extension,
`ATTACH … (TYPE iceberg, AUTHORIZATION_TYPE 'sigv4', SECRET …, SIGV4_REGION …,
SIGV4_SERVICE 's3')`) 5/5: attach, list namespaces, scan. Install the dependencies
into a `pip --target` directory and point `PYTHONPATH` at it:
`LIGHTS3_TABLES_SMOKE=1 PYTHONPATH=… ctest -R tables_smoke`.

## 7. ubsan / coverage

- `./build.sh --ubsan` (build-ubsan, `-fsanitize=undefined`); `check-all.sh` runs
  it with `UBSAN_OPTIONS=halt_on_error=1` so a finding fails the run.
- `./build.sh --coverage` (build-cov, `-O0 --coverage`); `scripts/coverage.sh
  [--e2e] [--no-build] [--no-test] [-j N] [-B build-cov]` (`make coverage`, §9) builds, tests and reports: HTML through
  gcovr or lcov when installed, otherwise `scripts/coverage_aggregate.py`
  parses `gcov --json-format` output and unions by (file, line number) for
  `src/` line coverage (written to `build-cov/coverage/summary.txt`; gcov's
  text summary double-counts template instantiations and cannot be summed).
  **Known limitation**: GCC's gcov barely instruments coroutine bodies (only the
  ramp function), so coroutine-heavy files have tiny denominators
  (`xlocalfs_backend.cc` counts 28 lines); after the unit tests the figure is
  88% of 13.7k lines; gcovr/lcov share the limitation.

## 8. One-shot matrix

`scripts/check-all.sh [--only build,build-asan,...] [--configure] [--with-perf]
[--with-soak] [--with-tables-smoke] [-j N] [--ctest-args "..."]`: for every existing build directory (build / asan / tsan /
ubsan / cov / sqlite / redis / rados / tikv / seastar / fuzz) an incremental
build + `ctest -LE "mint|perf|soak"` (opened up by the flags; `--ctest-args` appends
ctest arguments), sanitizer
directories under `*SAN_OPTIONS` so findings fail, and a summary table at the
end; `--configure` creates missing directories through `build.sh`.
## 9. Makefile: builds and code formatting

The top-level `Makefile` is a thin wrapper over `build.sh`, ctest,
`scripts/coverage.sh` and CPack: `make release` (Release, `build-rel/`), `make debug`
(Debug, `build/`), `make test` (debug, then the quick set of §1 in `build/`:
`ctest -LE "perf|soak|mint"`, the same filter `check-all.sh` uses;
`CTEST_ARGS="-R tables"` / `CTEST_ARGS="-L perf"` add filters; ctest runs serially
because the e2e sections bind fixed ports), `make coverage` (`scripts/coverage.sh`:
the `-O0 --coverage` build in `build-cov/`, unit tests + fuzz replays, the
line-coverage report of §7; `COVERAGE_ARGS="--e2e"` / `"--no-build"` / `"--no-test"`
are forwarded), `make package` (release, then CPack; output in `build-rel/packages/`,
the generator picked from the host's dpkg-deb / rpmbuild, TGZ otherwise),
`make clean` (removes only `build-rel/`, `build/` and `build-cov/`, every other
`build-*` variant stays). `JOBS=` sets the parallelism (default: half the cores),
`BUILD_ARGS="--redis --sqlite"` forwards build.sh flags, `RELEASE_DIR=` /
`DEBUG_DIR=` / `COVERAGE_DIR=` move the directories. `make help` lists every target.

### 9.1 Code formatting

`make format` first runs `scripts/check_comments.py --fix`, which moves every
trailing `//` comment onto its own line above the statement (a line ending in `{`
gets it as the first line inside the block; list elements, labels and
preprocessor lines get it right above themselves; the closers `}  // namespace x`,
`#endif  // X`, `// NOLINT` and `// clang-format off` stay), then rewrites every
git-tracked `.h` / `.cc` under `src/` and `tests/` in place with the repository's
`.clang-format` (Google style + 4-space indent + 120 columns; every other
deviation is commented in the file); `make format-check` lists trailing comments
and the files clang-format would change, and exits 1, for pre-commit hooks and CI.
`CLANG_FORMAT=clang-format-23 make format` selects the binary; the Google preset
drifts slightly between LLVM major versions, so pin one. The whole tree was
formatted once on 2026-09-09; later PRs should carry no formatting noise.
