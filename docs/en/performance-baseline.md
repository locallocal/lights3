# Performance baseline: the driver × TLS × put/get matrix

The last item of roadmap §4.3 ("no performance baseline"): the first benchmark
data kept in the repository, doubling as the before/after comparison for the
§4.3 data-plane work ([http-adapter.md §2.4](http-adapter.md)). The numbers
are meaningful only on this machine -- **relative comparison and regression
reference, not product figures**; regenerate with the commands in §3 on another
box and replace the tables.

## 1. Environment and method

| Item | Value |
| --- | --- |
| Machine | Intel i9-14900KF (32 threads), 30 GiB, Linux 7.0.0-31, g++ 15.2 |
| Data directory | `/tmp` (tmpfs) -- the localfs objects live on a memory filesystem, so this measures the HTTP layer plus the copy path, **no disk IO** |
| Client | `s3adm bench` (httplib synchronous client, one keep-alive connection per worker), loopback on the same machine |
| Gateway | localfs backend; `http.io_threads: 8` (beast/httplib/seastar), `runtime.io_threads: 16`; TLS with an openssl self-signed P-256 certificate, client `--insecure` |
| Large objects | 4 MiB × 8 workers × 8 s × 32 keys |
| Small objects | 16 KiB × 16 workers × 8 s × 256 keys |
| "before" | main 21968cf (includes #89), `Release`, `-DLIGHTS3_DUOSTORE=OFF -DLIGHTS3_CLOUDPROXY=OFF` (only unrelated backends trimmed; drivers and the localfs path unchanged); seastar from `build-seastar` (RelWithDebInfo, pre-change tree) |
| "after" | branch `feat/dataplane-perf`, same options; seastar the same `build-seastar` rebuilt incrementally |
| Script | `scripts/bench_matrix.sh` ([testing.md §5](testing.md)), a fresh gateway per cell, run sequentially on an idle machine |

Latency columns are histogram percentiles from `s3adm bench` (discrete bucket
edges: "round" p50 values such as 6.15 or 12.29 are bucket widths). A single
8 s run jitters by about ±5%: **differences within ±5% are noise**.

## 2. Results

### 2.1 4 MiB objects (8 workers)

| driver | TLS | mode | ops/s before | ops/s after | Δ | MiB/s after | p50 ms before→after | p99 ms before→after |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| builtin | off | put | 1074 | 1065 | **-1%** | 4259 | 7.10→7.20 | 16.12→16.14 |
| builtin | off | get | 4219 | 5709 | **+35%** | 22837 | 1.79→1.52 | 4.03→2.05 |
| builtin | on | put | 930 | 927 | **-0%** | 3707 | 7.57→7.62 | 16.18→16.19 |
| builtin | on | get | 2425 | 3076 | **+27%** | 12306 | 3.24→2.80 | 7.90→4.08 |
| beast | off | put | 91 | 914 | **+907%** | 3656 | 98.30→10.67 | 130.35→16.27 |
| beast | off | get | 3399 | 4568 | **+34%** | 18270 | 3.07→1.58 | 4.08→3.84 |
| beast | on | put | 207 | 727 | **+252%** | 2910 | 49.15→12.29 | 65.20→16.31 |
| beast | on | get | 1304 | 1511 | **+16%** | 6045 | 6.15→6.15 | 8.16→8.15 |
| httplib | off | put | 1084 | 1082 | **-0%** | 4328 | 7.26→7.37 | 16.15→16.16 |
| httplib | off | get | 4000 | 4561 | **+14%** | 18243 | 1.99→1.71 | 4.05→4.03 |
| httplib | on | put | 1068 | 879 | **-18%** | 3516 | 7.42→10.01 | 16.17→16.33 |
| httplib | on | get | 2245 | 3005 | **+34%** | 12020 | 3.69→2.71 | 8.09→4.40 |
| seastar | off | put | 1109 | 1031 | **-7%** | 4124 | 6.19→6.77 | 12.51→16.03 |
| seastar | off | get | 3995 | 6070 | **+52%** | 24279 | 1.83→1.55 | 4.04→3.11 |
| seastar | on | put | 934 | 880 | **-6%** | 3518 | 11.47→12.24 | 16.29→16.31 |
| seastar | on | get | 2234 | 3005 | **+34%** | 12019 | 3.09→3.13 | 5.21→7.50 |

### 2.2 16 KiB objects (16 workers)

| driver | TLS | mode | ops/s before | ops/s after | Δ | MiB/s after | p50 ms before→after | p99 ms before→after |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| builtin | off | put | 80477 | 80364 | **-0%** | 1256 | 0.20→0.20 | 0.47→0.47 |
| builtin | off | get | 148727 | 151198 | **+2%** | 2362 | 0.10→0.10 | 0.25→0.25 |
| builtin | on | put | 70889 | 70845 | **-0%** | 1107 | 0.21→0.21 | 0.50→0.50 |
| builtin | on | get | 120613 | 124612 | **+3%** | 1947 | 0.14→0.13 | 0.25→0.25 |
| beast | off | put | 21845 | 77474 | **+255%** | 1211 | 0.77→0.20 | 1.02→0.48 |
| beast | off | get | 110778 | 127500 | **+15%** | 1992 | 0.18→0.11 | 0.25→0.25 |
| beast | on | put | 35439 | 65572 | **+85%** | 1025 | 0.41→0.22 | 0.99→0.60 |
| beast | on | get | 107445 | 112165 | **+4%** | 1753 | 0.18→0.17 | 0.25→0.26 |
| httplib | off | put | 42115 | 41920 | **-0%** | 655 | 0.19→0.19 | 0.47→0.47 |
| httplib | off | get | 91136 | 93214 | **+2%** | 1456 | 0.09→0.09 | 0.13→0.13 |
| httplib | on | put | 33893 | 33409 | **-1%** | 522 | 0.23→0.23 | 0.51→0.51 |
| httplib | on | get | 66405 | 67307 | **+1%** | 1052 | 0.12→0.12 | 0.25→0.25 |
| seastar | off | put | 83815 | 82243 | **-2%** | 1285 | 0.19→0.19 | 0.37→0.43 |
| seastar | off | get | 152357 | 155199 | **+2%** | 2425 | 0.10→0.10 | 0.24→0.23 |
| seastar | on | put | 71330 | 71420 | **+0%** | 1116 | 0.20→0.20 | 0.49→0.50 |
| seastar | on | get | 122783 | 121683 | **-1%** | 1901 | 0.13→0.13 | 0.25→0.26 |

### 2.3 Reading the numbers

- **Large-object GET** is the §4.3 target: seastar +52% (prefetch overlaps the
  backend read with the socket write on the shard completely -- the largest
  gain), builtin +35% (sendfile + prefetch, p99 halved), beast +34%, httplib
  +14%; under TLS there is no sendfile, prefetch and the buffer pool alone
  still give +16 to +34% (httplib/seastar TLS +34% is the double buffer
  overlapping encryption with the backend read).
- **beast PUT: 4 MiB from 91 to 914 ops/s (10×), 16 KiB from 21.8k to 77.5k
  (3.5×)** -- the baseline's surprise. Without a reserved capacity, beast's
  `flat_buffer` makes `read_size = max(512, capacity − size)` request **512
  bytes** per socket read: a 4 MiB body was 8192 `recvmsg` + 8198
  `timerfd_settime` + 77k futex calls (`strace -c`), 40 ms per 4 MiB PUT
  against 6 ms on builtin. The fix is one line, `buffer.reserve(io_chunk_size)`
  ([http-adapter.md §2.4](http-adapter.md) ⑨). It confirms the roadmap's point
  that "beast is the performance path" had only ever been asserted.
- **Small objects**: ops/s is dominated by per-request overhead; the §4.3
  changes are neutral there (±5%), beast GET +14% comes from the `ResumeOn`
  fast path skipping `asio::post`; beast PUT's 3.5× is the read-granularity fix.
- **PUT on the other drivers is flat**: the request-body path was not touched
  in this round. The −18% on httplib TLS PUT in the table is single-run jitter:
  re-measured twice each on the idle machine, before 1051 / 1074 ops/s, after
  1055 / 1055 ops/s (the response-side changes do not touch PUT's small_body
  path); likewise seastar PUT's −6 to −7%: re-measured twice after the change
  at 1121 / 1085 (plaintext) and 911 / 909 (TLS) ops/s, level with before.
- Across drivers: plaintext large GET seastar > builtin (sendfile) > beast ≈
  httplib; under TLS builtin ≈ seastar ≈ httplib > beast. **beast's TLS clearly
  lags** (GET 1.5k vs 3.0k) and deserves its own investigation (asio ssl record
  handling and strand hops) -- left as a follow-up, not addressed here.

## 3. Reproducing

```bash
./build.sh -B build-rel -DCMAKE_BUILD_TYPE=Release -DLIGHTS3_DUOSTORE=OFF -DLIGHTS3_CLOUDPROXY=OFF -DLIGHTS3_BUILD_TESTS=OFF
scripts/bench_matrix.sh build-rel/lights3 build-rel/s3adm --duration 8 --size 4M --json 4m.jsonl --label "$(git rev-parse --short HEAD)"
scripts/bench_matrix.sh build-rel/lights3 build-rel/s3adm --duration 8 --size 16K --concurrency 16 --objects 256 --json 16k.jsonl
scripts/bench_matrix.sh build-seastar/lights3 build-rel/s3adm --drivers seastar --duration 8 --size 4M   # the seastar variant on its own
```

The script prints one progress line per cell to stderr and the Markdown table
to stdout; `--json` writes one line per cell, `{label, version, driver, tls,
mode, size, concurrency, duration_s, result}`, where `result` is the
`s3adm bench --output=json` object. Make sure the machine is idle and no stray
`lights3` process is around (`pgrep -x lights3`) before running.

## 4. History

| Date | Change | Summary |
| --- | --- | --- |
| 2026-09-05 | §4.3 data-plane work (prefetch, buffer pool, sendfile, pumping, ResumeOn fast path, per-bucket metrics without the lock, beast read-buffer reserve) | large-object GET +14 to +52%, beast PUT 3.5 to 10× |
