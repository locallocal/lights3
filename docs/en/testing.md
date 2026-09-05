# Testing: Matrix, e2e Coverage, Fuzzing, Fault Injection, Bench/Soak, Coverage (roadmap §6.1)

The basic shape of unit tests and e2e is in [s3-protocol.md §8](s3-protocol.md);
this document covers the eight items roadmap §6.1 filled in: the ctest inventory
and labels, the website / s3adm / fault-injection e2e sections, the fuzz
harnesses, the fault-injection facade, the performance gate and soak, mint in
ctest, ubsan/coverage builds, and the one-shot matrix script.

## 1. ctest inventory and labels

| Test | Content | Labels |
| --- | --- | --- |
| `unit_tests` | every unit case (including `test_fault.cc`) | — |
| `e2e_<driver>` / `e2e_<backend>` | one `run_e2e.sh` parameterized by driver × backend (13 variants) | — |
| `fuzz_regression_<target>` | the 6 harnesses replaying their corpora (§3) | `fuzz` |
| `monitoring_assets` | monitoring-asset reconciliation ([monitoring.md §5](monitoring.md)) | — |
| `bench_gate` | 3-second throughput / latency gate (§5) | `perf` |
| `soak_smoke` | 30-second soak: RSS / fd / leak assertions (§5) | `perf` `soak` |
| `mint` | MinIO mint's s3cmd + awscli subset; explicit SKIP without docker (§6) | `mint` |

Load-sensitive or externally dependent items are excluded by label:
`ctest -LE "perf|mint"`.

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
- **s3adm cross-validation**: curl signs with libcurl's SigV4, s3adm with its
  own implementation, both against one server. `cred create/list/get
  --show-secret/delete` (curl signs with the credential s3adm minted, 403 after
  revocation), `website set/get/delete` (curl reads back what s3adm wrote),
  `bench put/get` error-free, `fsck` with zero mismatches over the bench objects;
  the existing `usage/quota/tenant/reload` cases stay.
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
| `http_parse` | `drivers/common.h`: `parse_target` / `parse_content_length` / `parse_chunk_size` / `parse_body_framing` (first byte selects the routine) | `corpus/http_parse/` |
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
build-fuzz/fuzz_xml tests/fuzz/corpus/xml -max_total_time=600
```

A crashing input dropped back into `corpus/<target>/` becomes a permanent
regression. libFuzzer mode needs the whole tree to compile with clang — for
that `YamlNode`'s special members moved out of line (clang instantiates them
with the recursive `pair<string, YamlNode>` member still incomplete when they
are defaulted in-class) and one narrowing was fixed; under clang 21 + ASan the
core library, the six harnesses and unit_tests all build, unit_tests passing
484 cases. Idle 5–10 second runs on this machine: xml 150k executions, uri
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

`fault::kPoints` is the single list; `test_fault.cc` greps the sources to
confirm every point is wired (a point in the table without code fails the
test). Unit tests cover the three localfs points and the two duostore points;
the redis point lives in `test_duostore_redis.cc` (runs with an instance);
rados/tikv have no local cluster and are compile-verified only. libfiu exists
only as client-c's nested submodule and is not a repository-wide dependency.

## 5. Performance gate and soak

- `scripts/bench_gate.sh <lights3> <s3adm> [--duration N] [--min-put-ops N]
  [--min-get-ops N] [--max-p99-ms N]`: a memory-backend gateway + `s3adm bench
  put/get`; the summary line is parsed and a throughput floor (default 300
  ops/s) and a p99 ceiling (default 500 ms) asserted; `LIGHTS3_BENCH_*`
  variables override. ctest `bench_gate` uses 3 seconds.
- `scripts/soak.sh <lights3> <s3adm> [--seconds N] [--backend
  localfs|duostore|memory] [--max-rss-growth PCT] [--max-fd-growth N]`: rotates
  put/get/stat/list/delete-pool rounds, sampling RSS, fd count,
  `lights3_duostore_gcq_depth` and `lights3_multipart_active` per round; at the
  end asserts RSS growth since warm-up < 25%, fds ≤ warm-up + 16, no multipart
  leftovers, duostore GC queue back at 0, no ERROR lines. ctest `soak_smoke`
  runs 30 seconds; for hours: `scripts/soak.sh build/lights3 build/s3adm
  --seconds 7200 --backend duostore`.

The gate caught a real issue the day it went in: the s3adm client had no
TCP_NODELAY, so every small PUT stalled ~40 ms on Nagle + delayed ACK (identical
across all three drivers, fine from 256K up) — fixed in `s3adm_common.cc`, 16K
PUTs went from 98 ops/s to ~20k ops/s.

## 6. mint

`run_mint.sh` is registered as ctest `mint` (the `s3cmd awscli` subset,
`SKIP_RETURN_CODE 77`, so a docker-less box shows Not Run rather than a pass).
After a run it prints per-suite PASS/FAIL/NA counts from `log.json` as the
baseline record. The docker daemon is unreachable on this machine, so **the
baseline is not recorded yet**: run `ctest -R mint -V` once on a privileged
machine and paste the summary here.

## 7. ubsan / coverage

- `./build.sh --ubsan` (build-ubsan, `-fsanitize=undefined`); `check-all.sh` runs
  it with `UBSAN_OPTIONS=halt_on_error=1` so a finding fails the run.
- `./build.sh --coverage` (build-cov, `-O0 --coverage`); `scripts/coverage.sh
  [--e2e] [--no-build] [--no-test]` builds, tests and reports: HTML through
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
[--with-soak] [-j N]`: for every existing build directory (build / asan / tsan /
ubsan / cov / sqlite / redis / rados / tikv / seastar / fuzz) an incremental
build + `ctest -LE "mint|perf|soak"` (opened up by the flags), sanitizer
directories under `*SAN_OPTIONS` so findings fail, and a summary table at the
end; `--configure` creates missing directories through `build.sh`.
