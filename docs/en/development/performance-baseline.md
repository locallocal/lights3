# Performance baseline: the driver × TLS × put/get matrix

The last item of roadmap §4.3 ("no performance baseline"): the first benchmark
data kept in the repository, doubling as the before/after comparison for the
§4.3 data-plane work ([http-adapter.md §2.4](../architecture/http-adapter.md)). The numbers
are meaningful only on this machine -- **relative comparison and regression
reference, not product figures**; regenerate with the commands in §3 on another
box and replace the tables.

## 1. Environment and method

| Item | Value |
| --- | --- |
| Machine | Intel i9-14900KF (32 threads), 30 GiB, Linux 7.0.0-31, g++ 15.2 |
| Data directory | `/tmp` (tmpfs) -- the localfs objects live on a memory filesystem, so this measures the HTTP layer plus the copy path, **no disk IO** |
| Client | `lights3-ctl bench` (httplib synchronous client, one keep-alive connection per worker), loopback on the same machine |
| Gateway | localfs backend; `http.io_threads: 8` (beast/httplib/seastar), `runtime.io_threads: 16`; TLS with an openssl self-signed P-256 certificate, client `--insecure` |
| Large objects | 4 MiB × 8 workers × 8 s × 32 keys |
| Small objects | 16 KiB × 16 workers × 8 s × 256 keys |
| "before" | main 21968cf (includes #89), `Release`, `-DLIGHTS3_DUOSTORE=OFF -DLIGHTS3_CLOUDPROXY=OFF` (only unrelated backends trimmed; drivers and the localfs path unchanged); seastar from `build-seastar` (RelWithDebInfo, pre-change tree) |
| "after" | branch `feat/dataplane-perf`, same options; seastar the same `build-seastar` rebuilt incrementally |
| Script | `scripts/bench_matrix.sh` ([testing.md §5](testing.md)), a fresh gateway per cell, run sequentially on an idle machine |

Latency columns are histogram percentiles from `lights3-ctl bench` (discrete bucket
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
  ([http-adapter.md §2.4](../architecture/http-adapter.md) ⑨). It confirms the roadmap's point
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
  handling and strand hops) -- closed in §3 (2026-09-13).

## 3. 2026-09-13 re-run: closing the two issues the baseline found

The two follow-ups left in §2.3 (beast's TLS GET lagging, the request-body path
not optimized symmetrically) are done; implementation in
[http-adapter.md §2.4 ⑩–⑬](../architecture/http-adapter.md). Method as in §1
(same machine, same script, `Release` + `-DLIGHTS3_DUOSTORE=OFF
-DLIGHTS3_CLOUDPROXY=OFF`, seastar rebuilt incrementally in `build-seastar`);
"before" is the "after" column of §2 (the 2026-09-05 tree), "after" is this run.

### 3.1 How the causes were found

- **beast TLS GET**: `strace -c` over one 4 MiB TLS GET: beast ~655 `futex`, 258
  `sendmsg` (one per 16 KiB TLS record), 105 `epoll_wait`; builtin for the same
  request 260 `write` and ~220 `futex` (the latter is the thread pool's per-chunk
  scheduling, common to all four drivers). beast's extra futex calls are the cost
  of eight io threads sharing one `io_context`: every record's completion goes
  through the global queue and wakes another thread. One `io_context` per thread
  took TLS GET from 1560 to 2406 ops/s; `strace` then showed 263 `timerfd_settime`
  left (`beast::basic_stream` arms and cancels a timer around every socket
  operation) plus one composed operation per record. The per-session watchdog
  removed the timer traffic (throughput within noise, p99 14.4 → 8.1 ms); CPU per
  op was still 0.5 ms above builtin (3.95 vs 3.44 ms), rooted in
  `asio::ssl::stream`'s 17 KiB buffers splitting every 64 KiB chunk into four
  async rounds. The driver's own memory-BIO `TlsStream`, encrypting a whole chunk
  at once, took TLS GET from 2469 to 3069 ops/s, level with builtin.
- **PUT**: `openssl speed -evp md5` gives 1.26 GB/s per stream, ~3.3 ms for 4 MiB;
  with 8 workers the gateway spent 6.15 ms CPU per 4 MiB PUT and kept 6.6 cores
  busy, so PUT is capped by the serial recv + MD5 + write CPU path and a
  driver-side "prefetch" cannot help (there is nothing to overlap the next read
  with). The symmetric optimization is pipelining MD5 against the write and the
  next read: with `PipelinedMd5` p50 went 7.2 → 6.2 ms while CPU per op rose to
  7.1 ms (the thread hops); 128 KiB and 256 KiB chunks measured the same, so the
  hops are not what remains. The rest is kernel copies (recv, tmpfs write) and the
  commit path; the 3.3 ms single-stream MD5 is a floor that cannot be lowered.

### 3.2 4 MiB objects (8 workers)

| Driver | TLS | Mode | ops/s before | ops/s after | Δ | MiB/s after | p50 ms before→after | p99 ms before→after |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| builtin | off | put | 1065 | 1221 | **+15%** | 4883 | 7.20→6.18 | 16.14→11.27 |
| builtin | off | get | 5709 | 5876 | **+3%** | 23503 | 1.52→1.50 | 2.05→2.04 |
| builtin | on | put | 927 | 1073 | **+16%** | 4293 | 7.62→6.38 | 16.19→15.59 |
| builtin | on | get | 3076 | 3067 | **-0%** | 12267 | 2.80→2.83 | 4.08→4.08 |
| beast | off | put | 914 | 1202 | **+32%** | 4808 | 10.67→6.16 | 16.27→8.18 |
| beast | off | get | 4568 | 5547 | **+21%** | 22188 | 1.58→1.52 | 3.84→2.60 |
| beast | on | put | 727 | 1127 | **+55%** | 4509 | 12.29→6.26 | 16.31→14.78 |
| beast | on | get | 1511 | 2913 | **+93%** | 11651 | 6.15→2.75 | 8.15→7.15 |
| httplib | off | put | 1082 | 1209 | **+12%** | 4837 | 7.37→6.15 | 16.16→8.17 |
| httplib | off | get | 4561 | 5709 | **+25%** | 22837 | 1.71→1.51 | 4.03→2.04 |
| httplib | on | put | 879 | 1202 | **+37%** | 4807 | 10.01→6.20 | 16.33→13.33 |
| httplib | on | get | 3005 | 2882 | **-4%** | 11528 | 2.71→2.87 | 4.40→6.59 |
| seastar | off | put | 1031 | 1220 | **+18%** | 4881 | 6.77→6.15 | 16.03→8.17 |
| seastar | off | get | 6070 | 6183 | **+2%** | 24731 | 1.55→1.54 | 3.11→2.04 |
| seastar | on | put | 880 | 1034 | **+17%** | 4135 | 12.24→6.36 | 16.31→15.51 |
| seastar | on | get | 3005 | 2940 | **-2%** | 11762 | 3.13→3.14 | 7.50→7.60 |

### 3.3 16 KiB objects (16 workers)

| Driver | TLS | Mode | ops/s before | ops/s after | Δ | MiB/s after | p50 ms before→after | p99 ms before→after |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| builtin | off | put | 80364 | 77626 | **-3%** | 1213 | 0.20→0.20 | 0.47→0.48 |
| builtin | off | get | 151198 | 142186 | **-6%** | 2222 | 0.10→0.10 | 0.25→0.25 |
| builtin | on | put | 70845 | 67801 | **-4%** | 1059 | 0.21→0.21 | 0.50→0.50 |
| builtin | on | get | 124612 | 115981 | **-7%** | 1812 | 0.13→0.15 | 0.25→0.26 |
| beast | off | put | 77474 | 75710 | **-2%** | 1183 | 0.20→0.20 | 0.48→0.49 |
| beast | off | get | 127500 | 136053 | **+7%** | 2126 | 0.11→0.11 | 0.25→0.25 |
| beast | on | put | 65572 | 63523 | **-3%** | 993 | 0.22→0.24 | 0.60→0.51 |
| beast | on | get | 112165 | 111144 | **-1%** | 1737 | 0.17→0.14 | 0.26→0.29 |
| httplib | off | put | 41920 | 39433 | **-6%** | 616 | 0.19→0.20 | 0.47→0.49 |
| httplib | off | get | 93214 | 89673 | **-4%** | 1401 | 0.09→0.09 | 0.13→0.13 |
| httplib | on | put | 33409 | 33407 | **-0%** | 522 | 0.23→0.23 | 0.51→0.51 |
| httplib | on | get | 67307 | 64591 | **-4%** | 1009 | 0.12→0.12 | 0.25→0.25 |
| seastar | off | put | 82243 | 80632 | **-2%** | 1260 | 0.19→0.19 | 0.43→0.40 |
| seastar | off | get | 155199 | 151413 | **-2%** | 2366 | 0.10→0.10 | 0.23→0.24 |
| seastar | on | put | 71420 | 69156 | **-3%** | 1081 | 0.20→0.20 | 0.50→0.50 |
| seastar | on | get | 121683 | 116146 | **-5%** | 1815 | 0.13→0.13 | 0.26→0.26 |

### 3.4 Reading the numbers

- **beast's TLS GET is level with the other drivers** (around 3.0k) and its TLS
  PUT went from last to tied; plaintext GET stays seastar > builtin (sendfile) >
  beast ≈ httplib, with beast's gap to builtin down to the sendfile share.
- **4 MiB PUT improves 12–18% on all drivers** (more for httplib TLS, whose
  2026-09-05 number was a low jitter run, and for beast, which also gets the
  driver-side changes): that is `PipelinedMd5`, independent of the driver, and
  its ceiling is single-stream MD5 speed.
- **Small objects**: 16 KiB stays under `PipelinedMd5`'s inline threshold
  (256 KiB), so the request-body path is unchanged. This run's 16 KiB numbers sit
  2–7% below 2026-09-05 across the board, including the untouched seastar and
  builtin GET cells -- the day's noise floor, not a regression; beast plaintext
  GET +7%, TLS level with the asio version (the memory-BIO stream was 15% slower
  while it closed a record per buffer; coalescing small buffers removed that, see
  http-adapter.md §2.4 ⑫).
- No follow-up remains: the "found by the performance baseline" section of the
  todo list was deleted with this.

## 4. Reproducing

```bash
./build.sh -B build-rel -DCMAKE_BUILD_TYPE=Release -DLIGHTS3_DUOSTORE=OFF -DLIGHTS3_CLOUDPROXY=OFF -DLIGHTS3_BUILD_TESTS=OFF
scripts/bench_matrix.sh build-rel/lights3 build-rel/lights3-ctl --duration 8 --size 4M --json 4m.jsonl --label "$(git rev-parse --short HEAD)"
scripts/bench_matrix.sh build-rel/lights3 build-rel/lights3-ctl --duration 8 --size 16K --concurrency 16 --objects 256 --json 16k.jsonl
scripts/bench_matrix.sh build-seastar/lights3 build-rel/lights3-ctl --drivers seastar --duration 8 --size 4M   # the seastar variant on its own
```

The script prints one progress line per cell to stderr and the Markdown table
to stdout; `--json` writes one line per cell, `{label, version, driver, tls,
mode, size, concurrency, duration_s, result}`, where `result` is the
`lights3-ctl bench --output=json` object. Make sure the machine is idle and no stray
`lights3` process is around (`pgrep -x lights3`) before running.

## 5. History

| Date | Change | Summary |
| --- | --- | --- |
| 2026-09-05 | §4.3 data-plane work (prefetch, buffer pool, sendfile, pumping, ResumeOn fast path, per-bucket metrics without the lock, beast read-buffer reserve) | large-object GET +14 to +52%, beast PUT 3.5 to 10× |
| 2026-09-13 | beast per-thread io_context, session watchdog, memory-BIO TlsStream; PipelinedMd5 request-body hashing (http-adapter.md §2.4 ⑩–⑬) | beast TLS GET 4 MiB +93% (level with the other drivers), 4 MiB PUT +12 to +55% on all drivers, p50 7.2 → 6.2 ms |
