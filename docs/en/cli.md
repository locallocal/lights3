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
lights3 duostore quarantine list|release|purge <backend> [<pack_id>] corrupt-pack quarantine
lights3 tier scan|gc|reconcile <backend> [--config=<path>] tiered background tasks on demand
lights3 tier quarantine list|forget|purge <backend> [<bucket> <key>] tiered reconcile quarantine ledger
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
run, and exit. `<backend>` must name a `type: duostore` backend in the config,
otherwise the command errors out. When run next to live gateways on a shared
meta engine: `dump` is online-consistent on rocksdb/sqlite/tikv via an engine
snapshot (roadmap §3.7); redis has no MVCC, so a dump is only consistent with
writes stopped (the entry point warns). `load` always requires target-side
write quiescence.

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

### 2.4 Background tasks on demand: `duostore gc|scan|quarantine`, `tier scan|gc|reconcile|quarantine`

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
- `duostore quarantine list <backend>`: print the corrupt-pack quarantine
  ledger (pack id / live and corrupt record counts / entry time / purged,
  [storage/duostore-core.md §8.6](../storage/duostore-core.md));
  `duostore quarantine release <backend> <pack_id>` drops the entry so
  compaction retries (use after restoring the pack file from backup);
  `duostore quarantine purge <backend> <pack_id>` deletes the pack file while
  keeping the accounting (accepting the loss of its remaining corrupt records;
  refused while an in-flight reader pins the pack) — delete the owning objects
  afterwards and regular GC retires the rest. Pack ids accept the 16-digit hex
  the logs print, 0x-prefixed hex, or decimal.
- `tier scan <backend>`: one scan round (the first after startup and every
  `full_scan_interval` is a full enumeration, the rest are time-wheel
  incremental rounds): coldness detection + watermark reclamation + crash
  recovery + access-record flush; the round's `TierScanStats` go to the log;
- `tier gc <backend>`: consume one round of the tiered GC queue (orphan cloud
  replica deletion; exponential backoff persists with each entry);
- `tier reconcile <backend>`: one bidirectional local/cloud reconciliation
  (cloud-has-it-local-doesn't → rebuild the stub; local-remote-cloud-missing →
  warn, never delete the stub); findings that repeat go to the quarantine
  ledger and alert once;
- `tier quarantine list <backend>`: print the quarantine ledger (kind / bucket /
  key / etag / first and last sighting / count);
  `tier quarantine forget <backend> <bucket> <key>` drops the entry only (it
  returns next round if the finding still reproduces);
  `tier quarantine purge <backend> <bucket> <key>` resolves a `refs_missing`
  finding: after a HEAD confirms the cloud copy is still gone it deletes the
  dead local stub (acknowledged data loss, the object leaves listings; if the
  copy is back the stub stays, the entry is dropped and the exit code is 1).
  See [tiered-storage.md §9](tiered-storage.md).

```bash
./build/lights3 duostore gc duodata --config=/etc/lights3/lights3.yaml   # reclaim space now
./build/lights3 tier reconcile tierdata -c /etc/lights3/lights3.yaml
./build/lights3 tier quarantine list tierdata -c /etc/lights3/lights3.yaml
./build/lights3 tier quarantine purge tierdata archive photos/2024/a.jpg -c /etc/lights3/lights3.yaml
```

## 3. `s3adm` — ops CLI

`src/tools/s3adm*.cc`, built next to `lights3`. Command groups: `cred`
(credential admin plane), `website` (bucket static-website configuration),
`bench` (load testing), `fsck` (online object verification), `quota` (bucket
quotas), `tenant` (tenants and bucket ownership), `usage` (usage counters;
roadmap §3.9, see [multi-tenancy.md](multi-tenancy.md)). Every subcommand
signs its own SigV4 requests against the lights3 HTTP endpoint; no aws cli
needed.

### 3.1 Connection and credential options (shared by every leaf)

| Option | Default | Meaning |
| --- | --- | --- |
| `-e, --endpoint=<url>` | `http://127.0.0.1:9000` | `scheme://host[:port]`; https needs a build with OpenSSL |
| `--ak=<key>` / `--sk=<key>` | env | fall back to `LIGHTS3_ADMIN_AK` / `LIGHTS3_ADMIN_SK`; prefer env for the SK (argv is visible to local `ps`) |
| `--region=<r>` | `us-east-1` | SigV4 region; must match the server's `auth.region` |
| `--insecure` | false | skip certificate verification for https (self-signed deployments) |
| `--timeout-sec=<n>` | 10 | connect/read/write timeout |

`website`, `quota set/clear` and the mutating `tenant` commands require the
**root static credential** (an entry in the config's `auth.credentials`, see
[credential-management.md §3](credential-management.md)); `cred`,
`tenant list/get` and `usage` also accept a **tenant admin** (scoped to its
own tenant, [multi-tenancy.md §4.4](multi-tenancy.md)); other credentials get
403. `bench` works with any credential allowed on the target bucket.

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
s3adm cred create [-c|--comment=<text>] [-p|--policy=<json>|@<file>] [-t|--tenant=<id>] [-r|--role=user|admin]
                                         create an AK/SK pair (the only time the full SK is returned); --tenant sets the
                                         owning tenant (pinned server-side when a tenant admin calls, so it may be omitted),
                                         --role=admin grants that tenant's admin plane
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
s3adm cred create --tenant=acme --role=admin --comment='acme operator'
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

### 3.6 `quota` — bucket quotas

Drives the `?quota` subresource ([multi-tenancy.md §3](multi-tenancy.md)).
`set` replaces the limit as a whole and needs at least one axis > 0; `get`
works for any credential allowed on the bucket, `set`/`clear` are root only.
Writes over the limit get `QuotaExceeded` (403).

```text
s3adm quota get <bucket>                                    print the quota XML (none set: 404 -> exit 1)
s3adm quota set <bucket> [-b|--max-bytes=<sz>] [-o|--max-objects=<n>]
                                                            set/replace; sz accepts KiB/MiB/GiB suffixes, 0 = that axis unlimited
s3adm quota clear <bucket>                                  remove the quota (idempotent)
```

```bash
s3adm quota set logs --max-bytes=50GiB --max-objects=1000000
s3adm quota get logs
s3adm quota clear logs
```

### 3.7 `tenant` — tenants and bucket ownership

Drives `/-/admin/tenants` ([multi-tenancy.md §6](multi-tenancy.md)).
Mutations are root only; `list`/`get` also work for a tenant admin on its own
tenant. The JSON response is printed verbatim.

```text
s3adm tenant list                                            list tenants (quota, owned buckets, aggregate usage, credential count)
s3adm tenant get <id>                                        one tenant
s3adm tenant create <id> [--display-name=<s>] [--max-bytes=<sz>] [--max-objects=<n>] [--max-buckets=<n>]
                                                             create; id matches [a-z0-9][a-z0-9._-]{0,63}
s3adm tenant update <id> [--display-name=<s>] [quota flags | --clear-quota]
                                                             the quota is replaced as a whole: axes not given become unlimited
s3adm tenant delete <id>                                     refused (409) while it still owns buckets or has credentials
s3adm tenant assign <id> <bucket> [--force]                  make an existing bucket the tenant's; --force to take it from another tenant
s3adm tenant unassign <id> <bucket>                          detach (the bucket becomes unowned)
```

```bash
s3adm tenant create acme --display-name='ACME Corp' --max-bytes=1TiB --max-buckets=20
s3adm tenant assign acme legacy-logs
s3adm tenant get acme
```

### 3.8 `usage` — usage counters

Reads `/-/admin/usage` ([multi-tenancy.md §2](multi-tenancy.md)). Root sees
every bucket, a tenant admin its own tenant's; `--rescan` runs a synchronous
full count of one bucket and prints the result (refused when
`usage.enabled=false`).

```text
s3adm usage [bucket] [-r|--rescan] [-t|--tenant=<id>]
```

```bash
s3adm usage                       # every bucket: objects / bytes / mpu_bytes / scanned_at
s3adm usage --tenant=acme         # only buckets owned by acme (root)
s3adm usage logs --rescan         # recount the logs bucket now
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
