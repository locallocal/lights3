# Command-line tools: `lights3` and `s3adm`

> English translation of [../cli.md](../cli.md). Section numbering matches.

This is the command reference for the two executables. Both are built on
`third_party/ccmd` (a header-only subcommand framework bundling `cflag` for
option parsing) and share one set of command-line semantics, spelled out once
in §1. The startup assembly flow is in [architecture.md §4](architecture.md#4-process-structure-and-startup-flow),
the credential admin plane in [credential-management.md](credential-management.md),
static websites in [static-website.md](static-website.md).

## 1. ccmd semantics

- **Command tree**: `<program> [<group> [<subcommand>]] [positionals] [options]`.
  `<program> help [<group> [<subcommand>]]`, or `-h/--help` at any level,
  prints that level's help.
- **Options do not propagate downward**: every leaf subcommand owns an
  independent option set, so options must follow the leaf
  (`s3adm cred list --endpoint=…`, not `s3adm --endpoint=… cred list`).
- **Long options take values only as `--name=value`**; `--name value` is
  rejected by cflag as a missing value. Short options accept both
  `-e http://…` and `-ehttp://…`. A bare bool option means true
  (`--insecure`, `--keep`).
  Exception: the `lights3` server folds `--config <path>` (and
  `--backend`/`--file`) into the `=` form before handing argv to ccmd, so the
  space form keeps working there (the e2e scripts and older docs use it);
  `s3adm` has no such shim.
- **`--` ends option parsing**; everything after it is positional.
- **Exit codes**: `0` success; `1` runtime failure (request refused, IO error,
  server startup failure); `2` usage error (missing positional, missing
  credentials, value out of range, bare command group). ccmd itself exits `1`
  with a stderr hint on an unknown command or option.

## 2. `lights3` — the server process

```text
lights3 [--config=<path>]                                  start the server
lights3 duostore dump <backend> <file> [--config=<path>]   export duostore meta
lights3 duostore load <backend> <file> [--config=<path>]   import duostore meta
lights3 duostore gc <backend> [--config=<path>]            run one duostore GC round now
lights3 duostore scan <backend> [--config=<path>]          run one orphan-scan round now
lights3 tier scan|gc|reconcile <backend> [--config=<path>] tiered background tasks on demand
lights3 fsck <backend> [--max-mbps=<n>] [--config=<path>]  offline integrity scrub
lights3 help [duostore [<sub>] | tier [<sub>] | fsck]
```

| Option | Applies to | Default | Meaning |
| --- | --- | --- | --- |
| `-c, --config=<path>` | all | `config/lights3.yaml` | YAML config file (format: [architecture.md §5](architecture.md#5-example-configuration-file)) |
| `--backend=<name>` | `duostore *`, `tier *`, `fsck` | — | backend name, same as the first positional |
| `--file=<path>` | `duostore dump|load` | — | dump file path, same as the second positional |
| `--max-mbps=<n>` | `fsck` | `0` | read throttle in MB/s, `0` = unthrottled |

### 2.1 Starting the server

No subcommand means start: `Application(config)` → `open_storage()` →
`start_server()` → `run()`, blocking until SIGINT/SIGTERM, then a graceful
shutdown in the order of [architecture.md §4](architecture.md#4-process-structure-and-startup-flow);
`run()`'s return value is the exit code. Any startup exception (config parse
failure, backend open failure, port in use, …) prints `fatal: …` to stderr and
exits `1`; backends already built are closed through `~Application` (duostore
seals its active pack, rados flushes).

```bash
export LIGHTS3_SECRET_1=my-secret
./build/lights3 --config=config/lights3.yaml
./build/lights3 -c /etc/lights3/lights3.yaml
```

### 2.2 `duostore dump` / `duostore load`

Logical meta backup/restore for DuoStore (stream format and invariants:
[storage/duostore-core.md §11](../storage/duostore-core.md)). Registered only
in `LIGHTS3_DUOSTORE` builds; both build every backend **without listening**,
run, and exit, so write quiescence holds trivially. `<backend>` must name a
`type: duostore` backend in the config, otherwise the command errors out.

- `dump`: writes all bucket/object records and the sealed-pack ledger of that
  backend to `<file>` (truncating).
- `load`: replays `<file>` record by record into the backend (buckets are
  idempotent, so an interrupted run can be repeated) and ends with a forced
  orphan scan.

Backup order: copy the data directory first, then `dump`; on restore put the
data back first, then `load`.

```bash
./build/lights3 duostore dump duo /backup/duo-meta.dump --config=/etc/lights3/lights3.yaml
./build/lights3 duostore load --backend=duo --file=/backup/duo-meta.dump -c /etc/lights3/lights3.yaml
```

### 2.3 `fsck`

Offline data-integrity scrub (roadmap §3.1; implementation details in
[storage/duostore-core.md §8.4](../storage/duostore-core.md) and
[storage/localfs.md §11](../storage/localfs.md)). Same pattern as dump/load:
builds every backend without listening, runs, and exits; **strictly
read-only** — every finding is a log line plus a counter, nothing is repaired.
Dispatches on the actual type of `<backend>`:

- **duostore**: meta-driven — reads back every extent of every object and every
  in-flight multipart part, recomputes crc32c per extent against the manifest
  (independent of the `verify_chunk_crc` switch), and reconciles the
  chunk/rados refs ledger against the manifests in both directions;
- **localfs / xlocalfs**: re-reads every object and compares the recomputed MD5
  with the stored ETag (multipart composites recomputed over the recorded part
  layout; legacy objects without one count as unverifiable);
- other types (memory/cloudproxy/tiered) error out.

Exit codes: `0` clean; `1` when integrity findings exist (duostore's
corrupt/unreadable/refs_missing/meta_errors, localfs's mismatches/read_errors).
Warning-grade counters (refs_stale, unverifiable, orphan sidecars) are logged
without affecting the exit code — refs_stale can be a transient artifact of an
MPU completing mid-scrub; re-run to confirm. Safe against a live instance too
(on duostore at the cost of GC standing still for the duration).

```bash
./build/lights3 fsck duodata --max-mbps=100 --config=/etc/lights3/lights3.yaml
./build/lights3 fsck localdata -c /etc/lights3/lights3.yaml && echo clean
```

### 2.4 Background tasks on demand: `duostore gc|scan`, `tier scan|gc|reconcile`

CLI exits for the background hooks (roadmap §3.2): `run_gc_once` /
`run_orphan_scan_once` / `scan_once` / tiered `run_gc_once` /
`run_reconcile_once` used to be reachable only through timers and unit tests —
an operator wanting space back *now* had to wait for the next tick (GC every
5min, orphan scan and reconciliation daily by default). Same pattern as
dump/load: build the backends, listen on nothing, run one round, exit; stats go
to the log. **Exit code is plain 0/1 (success/exception)** — loss signals like
refs_missing are LOG_ERROR'd as always but do not change the exit code; the
integrity-verdict surface is `lights3 fsck`.

- `duostore gc <backend>`: one full GC round (mpu_ttl cleanup → gcq
  consumption → aged sealing + compaction → whole-empty-pack deletion,
  [storage/duostore-core.md §8.1](../storage/duostore-core.md)). Local meta
  engines (rocksdb/sqlite) hold a file lock, so stop the server first; shared
  engines (redis/tikv) can run next to live gateways — the GC lease
  coordinates. A `gc_enabled=false` (secondary gateway) config does not gate
  the manual hooks.
- `duostore scan <backend>`: one orphan-scan round (two-way reconciliation of
  the disk against refs/packstat, §8.3); also prints chunk/pack on-disk usage.
- `tier scan <backend>`: one round of coldness detection + watermark
  reclamation + crash recovery + atime snapshot;
- `tier gc <backend>`: consume one round of the tiered GC queue (orphan cloud
  replica deletion; exponential backoff persists with each entry);
- `tier reconcile <backend>`: one bidirectional local/cloud reconciliation
  (cloud-has-it-local-doesn't → rebuild the stub; local-remote-cloud-missing →
  warn, never delete the stub).

```bash
./build/lights3 duostore gc duodata --config=/etc/lights3/lights3.yaml   # reclaim space now
./build/lights3 tier reconcile tierdata -c /etc/lights3/lights3.yaml
```

## 3. `s3adm` — ops CLI

`src/tools/s3adm*.cc`, built next to `lights3`. Three command groups plus one
leaf command: `cred` (credential admin plane), `website` (bucket static-website
configuration), `bench` (load testing), `fsck` (online object verification).
Every subcommand signs its own SigV4 requests against the lights3 HTTP
endpoint; no aws cli needed.

### 3.1 Connection and credential options (shared by every leaf)

| Option | Default | Meaning |
| --- | --- | --- |
| `-e, --endpoint=<url>` | `http://127.0.0.1:9000` | `scheme://host[:port]`; https needs a build with OpenSSL |
| `--ak=<key>` / `--sk=<key>` | env | fall back to `LIGHTS3_ADMIN_AK` / `LIGHTS3_ADMIN_SK`; prefer env for the SK (argv is visible to local `ps`) |
| `--region=<r>` | `us-east-1` | SigV4 region; must match the server's `auth.region` |
| `--insecure` | false | skip certificate verification for https (self-signed deployments) |
| `--timeout-sec=<n>` | 10 | connect/read/write timeout |

`cred` and `website` require the **root static credential** (an entry in the
config's `auth.credentials`, see [credential-management.md §3](credential-management.md));
tenant credentials get 403. `bench` works with any credential allowed on the
target bucket.

```bash
export LIGHTS3_ADMIN_AK=AKIDEXAMPLE
export LIGHTS3_ADMIN_SK=my-secret
```

### 3.2 `cred` — credential management

One subcommand per `/-/admin/credentials` endpoint; the JSON response is
printed verbatim to stdout.

```text
s3adm cred list                          list all credentials (SK masked; static/file/dynamic sources)
s3adm cred get <ak> [-s|--show-secret]   show one credential; --show-secret returns the plaintext SK
                                         (dynamic/file credentials only; the server writes an audit line)
s3adm cred create [-c|--comment=<text>] [-p|--policy=<json>|@<file>]
                                         create an AK/SK pair (the only time the full SK is returned)
s3adm cred delete <ak>                   revoke a dynamic credential (static ones belong to the config; refused)
```

`--policy` takes inline JSON or `@file`, shaped
`{"buckets":[...],"prefixes":[...],"readonly":bool,"actions":[...]}`; semantics
in [credential-management.md §11](credential-management.md).

```bash
s3adm cred create --comment=tenant-a --policy='{"buckets":["tenant-a-*"],"readonly":false}'
s3adm cred create -c ci-runner -p @policies/ci.json
s3adm cred get L3AK7Q2MXX5EIY4BJZW3 --show-secret
s3adm cred list --endpoint=https://s3.example.com --insecure
s3adm cred delete L3AK7Q2MXX5EIY4BJZW3
```

### 3.3 `website` — bucket static-website configuration

Drives the `?website` subresource ([static-website.md §4](static-website.md)).
After `set` the bucket is anonymously readable (GET/HEAD objects only) with
index/error document semantics; buckets configured statically in the YAML
refuse dynamic changes (405).

```text
s3adm website get <bucket>                            print the configuration XML (404 when unset → exit 1)
s3adm website set <bucket> [-i|--index-suffix=<s>] [-k|--error-key=<key>]
                                                      enable/replace; index-suffix defaults to index.html and
                                                      must not contain '/'; empty error-key = built-in error page
s3adm website delete <bucket>                         remove the configuration (idempotent); no longer anonymous
```

```bash
s3adm website set my-site --index-suffix=index.html --error-key=404.html
s3adm website get my-site
s3adm website delete my-site
```

### 3.4 `bench` — load testing

Closed-loop benchmarks of the data plane (put/get) and non-IO APIs
(stat/list/list-buckets): `--concurrency` workers, one connection each, loop
over a pool of `--objects` keys under `--prefix` for `--duration-sec` seconds;
one interval stats line per second, then a summary (ops, ops/s, MiB/s,
avg/p50/p90/p99/max latency).

```text
s3adm bench put           upload (round-robin overwrite across the pool)
s3adm bench get           download (uploads the pool first)
s3adm bench stat          HeadObject (uploads the pool first)
s3adm bench list          ListObjectsV2 (uploads the pool first; --max-keys per page)
s3adm bench list-buckets  ListBuckets (no --bucket needed)
```

| Option | Default | Range / meaning |
| --- | --- | --- |
| `-b, --bucket=<name>` | — | target bucket, created if missing; required except for `list-buckets` |
| `-j, --concurrency=<n>` | 4 | 1–256 |
| `-d, --duration-sec=<n>` | 10 | 1–86400 |
| `-n, --objects=<n>` | 64 | 1–1000000, key pool size |
| `-s, --size=<sz>` | put/get `1M`, stat/list `4K` | bytes or K/M/G suffix, max 1G |
| `--prefix=<p>` | `s3adm-bench/` | key prefix |
| `--max-keys=<n>` | 100 | `list` only |
| `--keep` | false | keep the objects instead of deleting the pool at the end |

The first error is printed to stderr (`s3adm: bench: first error: …`); later
ones only increment the err counter. A failure in the prepare phase (bucket
creation / pre-upload) exits `1` immediately.

```bash
s3adm bench put --bucket=test --size=4M --concurrency=8 --duration-sec=30
s3adm bench get -b test -s 4M -j 8 -d 30 --keep
s3adm bench stat -b test -j 16
s3adm bench list -b test -n 10000 --max-keys=1000
s3adm bench list-buckets -j 16
```

### 3.5 `fsck` — online object verification

The online complement of `lights3 fsck` (§2.3): end-to-end verification through
the S3 API — ListObjectsV2 page by page, then a streaming GET per object with
the MD5 recomputed client-side and compared against the ETag, which also
exercises the gateway read path itself. Multipart composite ETags are
recomputed part by part via `GET ?partNumber=i` (objects the server has no
recorded layout for return 501 and count as UNVERIFIABLE, never MISMATCH).
Read-only; any credential that can read the bucket works. The cost is pulling
every byte over HTTP — the deep check (duostore crc/refs reconciliation) still
needs server-side `lights3 fsck`.

```text
s3adm fsck <bucket> [-p|--prefix=<p>] [--max-mbps=<n>]
```

| Option | Default | Meaning |
| --- | --- | --- |
| `-p, --prefix=<p>` | — | only verify keys under this prefix |
| `--max-mbps=<n>` | `0` | download throttle in MB/s, `0` = unthrottled |

Prints one `MISMATCH <key>` line per finding (stdout) / transport errors
(stderr), then a summary line (objects/bytes/mismatches/errors/unverifiable/
skipped; skipped = objects deleted between list and GET). Exit codes: `0`
clean; `1` mismatches or errors.

```bash
s3adm fsck my-bucket --endpoint=https://s3.example.com
s3adm fsck my-bucket --prefix=photos/ --max-mbps=50
```

## 4. Conventions for adding subcommands

- One source file per command group (`s3adm_<group>.cc/.h`, `make_<group>()`
  returns the group root), added in `s3adm.cc` via `add_subcommand`;
  connection options are reused through `add_conn_flags` / `read_conn_opts`
  in `s3adm_common.h`.
- Callbacks return nothing; the exit code travels through `s3adm::g_exit`
  following the 0/1/2 convention in §1; positionals are read from `c->args()`
  and count-checked by the command itself.
- Server-side ops entry points live in the command tree of `src/main.cc`
  (e.g. `duostore`), registered only inside their compile-time switch so that
  trimmed builds never expose an unusable command.
