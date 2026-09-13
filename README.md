# LightS3

An S3-protocol gateway written in C++20. It exposes the standard S3 REST API on
the outside, with pluggable HTTP drivers and storage backends on the inside, and
serves an Apache Iceberg REST catalog (S3 Tables) on the same listener so that
PyIceberg, DuckDB, Spark or Trino can use any bucket as a lakehouse catalog.
Design documents live in [docs/](docs/README.md) (Chinese originals, English
translations under [docs/en/](docs/en/README.md)); the current implementation
follows the architecture described in
[docs/en/architecture/overview.md](docs/en/architecture/overview.md).

*中文介绍见 [docs/README.zh-CN.md](docs/README.zh-CN.md)。*

## Architecture

Four layers with one-way, top-down dependencies; the two pluggable
boundaries are `IHttpServer` (L1/L2) and `IStorageBackend` (L2/L3):

```text
              S3 clients (aws cli / boto3 / curl --aws-sigv4)
                                  │ HTTP/1.1
┌─ L1 · HTTP Adapter ─────────────▼─────────────────────────────────────┐
│ HttpServerFactory → IHttpServer, driver picked at runtime             │
│   builtin : POSIX sockets, thread-per-connection                      │
│   beast   : Boost.Asio async, N io threads, per-connection coroutine  │
│   httplib : cpp-httplib sync, thread-per-request                      │
│   seastar : shard-per-core reactor, process-wide engine (optional)    │
│ neutral HttpRequest/HttpResponse model, streaming BodyReader bodies   │
└─────────────────────────────────┬─────────────────────────────────────┘
                                  ▼
┌─ L2 · S3 Protocol ────────────────────────────────────────────────────┐
│ S3Service::dispatch                                                   │
│   ├─ /-/healthz · /-/metrics · /-/readyz          (anonymous)         │
│   ├─ /-/admin/credentials → admin handler (JSON, root-only)           │
│   │        └─ CredentialStore ──(ICredentialProvider)──┐              │
│   └─ SigV4Authenticator.verify ◄───────────────────────┘              │
│        └─ per-credential policy authorize (bucket glob / readonly)    │
│             └─ route table (method + scope + query flag)              │
│                  └─ handlers: buckets / objects / list / multipart    │
│                  └─ /iceberg/v1/... → tables::RestApi (S3 Tables:     │
│                       Iceberg REST catalog, namespaces/tables/views)  │
│ XML codec · S3Error mapping · Metrics · access log                    │
└─────────────────────────────────┬─────────────────────────────────────┘
                   IStorageBackend (Task<T>, streaming)
┌─ L3 · Storage ──────────────────▼─────────────────────────────────────┐
│ BucketRouter: glob rules → backend; ".sys": credentials + catalog     │
│   localfs  : sidecar .meta JSON, atomic staging+rename                │
│   xlocalfs : io_uring data plane (raw syscalls), reaper thread        │
│   memory   : in-memory backend for tests                              │
│ shared: listing · multipart state · name validation                   │
└─────────────────────────────────┬─────────────────────────────────────┘
                                  ▼
┌─ L4 · Core (cross-cutting) ───────────────────────────────────────────┐
│ Task<T> lazy coroutines · sync_wait / when_all · ThreadPool           │
│ AsyncSemaphore (inflight limit) · TimerQueue · YAML config · spdlog   │
│ util: crypto (OpenSSL EVP) / uri / time / hex                         │
└───────────────────────────────────────────────────────────────────────┘
```

Request lifecycle in one line: driver parses HTTP and hands a neutral
request to `S3Service::dispatch`, which authenticates (SigV4, credentials
resolved through `ICredentialProvider`), routes by method/scope/query to a
handler coroutine, which streams data to/from the backend chosen by
`BucketRouter`; every layer runs on `Task<T>` coroutines scheduled onto the
shared `ThreadPool`.

## Build and test

Requirements: g++ ≥ 13 (C++20 coroutines), CMake ≥ 3.20, OpenSSL.
The beast driver needs Boost headers (≥ 1.75, header-only, no compiled
libraries; if system Boost is not found, point `BOOST_ROOT` at the header
directory, or disable the driver with `-DLIGHTS3_DRIVER_BEAST=OFF`).
ccmd, spdlog, httplib, nlohmann/json, rocksdb, hiredis and sqlite are git
submodules under `third_party/` and must be initialized before the first
build (rocksdb is required — the DuoStore backend is on by default; hiredis
and sqlite serve its optional meta engines).

```bash
./build.sh --test        # submodules + cmake + ninja + ctest in one go
make release             # Release build in build-rel (make debug -> Debug in build; make package -> deb/rpm/tgz)
make test                # Debug build, then the quick ctest set (everything but the perf gate, soak and mint)
make coverage            # -O0 --coverage build in build-cov, unit tests, line-coverage report (scripts/coverage.sh)
```

or manually:

```bash
git submodule update --init third_party/spdlog \
    third_party/httplib third_party/json third_party/rocksdb \
    third_party/hiredis third_party/sqlite
git submodule update --init --recursive third_party/ccmd   # nests cflag
cmake -B build
cmake --build build -j
ctest --test-dir build --output-on-failure   # unit tests + per-driver and per-backend e2e
                                             # (e2e needs curl ≥ 7.75)
```

The seastar driver is off by default (heavy dependencies); enable with
`./build.sh --seastar`. Optional backend switches: `--redis` / `--sqlite`
(DuoStore meta engines), `--tikv` (needs system gRPC/Poco, lazily pulls the
client-c submodule), `--rados` (needs librados, or `-DLIGHTS3_RADOS_ROOT`).
These CMake options are sticky in the build cache — combine with `--clean`
or a separate `-B build-x` directory to switch them off. Sanitizer / analysis
builds: `./build.sh --asan` / `--tsan` / `--ubsan` / `--coverage` / `--fuzz`
(libFuzzer, clang). `scripts/check-all.sh` runs the incremental build + ctest
matrix over every build directory present; `scripts/coverage.sh` reports line
coverage; `scripts/bench_gate.sh` and `scripts/soak.sh` are the performance
gate and the long-stability run — see [docs/en/development/testing.md](docs/en/development/testing.md).

ctest also carries the fuzz corpus replays (`fuzz_regression_*`), the
monitoring-asset check, a 3-second bench gate and a 30-second soak (labels
`perf`/`soak`), the S3 Tables client smoke (`tables_smoke`; needs
`LIGHTS3_TABLES_SMOKE=1` plus PyIceberg / DuckDB, SKIP otherwise), and the
MinIO mint compatibility suite (`s3cmd awscli` subset; needs docker and
reports SKIP without it):

```bash
ctest --test-dir build -LE "perf|mint"      # the quick set
ctest --test-dir build -R mint -V           # mint, on a machine with docker
```

`lights3 --version` (and `lights3-ctl --version`) prints the version, the git commit
stamped at build time, the build type and the compiled-in drivers / backends;
the same identity is logged at startup and exported as `lights3_build_info`.

## Run

```bash
export LIGHTS3_SECRET_1=my-secret
# optional: encrypt dynamically generated secret keys at rest (AES-256-GCM).
# Once enabled, starting without the key (or with a wrong one) fails fast.
export LIGHTS3_MASTER_KEY=$(openssl rand -hex 32)
./build/lights3 --config=config/lights3.yaml
```

The ops CLI `lights3-ctl` (credentials, bucket websites, table buckets, benchmarks)
and the full `lights3` command tree are documented in [docs/en/usage/cli.md](docs/en/usage/cli.md).

Access it with any S3 client (the examples below use curl's SigV4 support):

```bash
alias s3curl='curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "AKIDEXAMPLE:$LIGHTS3_SECRET_1"'
s3curl -X PUT http://127.0.0.1:9000/mybucket                      # CreateBucket
s3curl -X PUT --data-binary @file.bin http://127.0.0.1:9000/mybucket/file.bin
s3curl http://127.0.0.1:9000/mybucket?list-type=2                 # ListObjectsV2
s3curl -r 0-99 http://127.0.0.1:9000/mybucket/file.bin            # Range download
```

Or use the aws cli: `aws --endpoint-url http://127.0.0.1:9000 s3 ls`.

## S3 Tables (Apache Iceberg REST catalog)

Next to the S3 API, lights3 serves an Iceberg REST catalog. A *table bucket*
keeps Iceberg metadata and data files as ordinary objects; the catalog state
(namespaces, table pointers, commit records) lives as JSON objects in the `.sys`
bucket, and every metadata-pointer swap is one conditional write
(`If-Match` / `If-None-Match: *`) on the storage backend. The same guarantees
therefore hold on every backend and across several gateways that share one
store, and the gateway generates and validates the Iceberg metadata itself, so
engines need nothing beyond their stock REST-catalog settings.

Enable it and create a table bucket:

```yaml
tables:
  enabled: true                 # /iceberg/v1/... on the same listener
  credential_vending: true      # optional: hand engines table-scoped STS sessions
  maintenance:
    scan_interval: 0s           # 0 = maintenance only on request (lights3-ctl / REST)
```

```bash
./build/lights3-ctl tables enable lake     # root credential: PUT /iceberg/v1/buckets/lake
```

Point an engine at it (SigV4-signed REST calls, signing name `s3`):

```python
from pyiceberg.catalog import load_catalog
cat = load_catalog("lake", **{
    "uri": "http://127.0.0.1:9000/iceberg", "warehouse": "lake",
    "rest.sigv4-enabled": "true", "rest.signing-name": "s3", "rest.signing-region": "us-east-1",
    "s3.endpoint": "http://127.0.0.1:9000", "s3.path-style-access": "true", "s3.region": "us-east-1",
    "s3.access-key-id": "AKIDEXAMPLE", "s3.secret-access-key": "my-secret",
})
cat.create_namespace("sales")
```

```sql
-- DuckDB (iceberg + httpfs extensions)
CREATE SECRET s3s (TYPE s3, PROVIDER config, KEY_ID 'AKIDEXAMPLE', SECRET 'my-secret',
                   REGION 'us-east-1', ENDPOINT '127.0.0.1:9000', URL_STYLE 'path', USE_SSL false);
ATTACH 'lake' AS lake (TYPE iceberg, ENDPOINT 'http://127.0.0.1:9000/iceberg',
                       AUTHORIZATION_TYPE 'sigv4', SECRET s3s, SIGV4_REGION 'us-east-1', SIGV4_SERVICE 's3');
SELECT * FROM lake.sales.orders LIMIT 10;
```

What the catalog provides:

- **The standard REST catalog surface** on `/iceberg/v1` (optional `/_iceberg`
  alias): `/config`, namespaces, tables (create / load / commit / register /
  rename / drop, `metadata-location`), views, `reportMetrics`, Iceberg JSON
  errors; format v1/v2 metadata with every requirement and update applied
  server-side
- **Safe commits**: single-table CAS with idempotent replay by commit id, a
  documented crash-window matrix with `catalog/diagnostics` and
  `catalog/recovery` endpoints, two-phase rename recoverable from any gateway,
  and deep snapshot validation (manifest-list → manifests → data files, Avro
  null / deflate) before a snapshot is accepted; LoadTable carries an `ETag`
  and honors `If-None-Match`
- **Access control that follows the object model**: per-credential policy on
  (bucket, `<namespace>/<table>`), tenant isolation, the `s3tables` signing
  name, table-scoped vended credentials
  (`X-Iceberg-Access-Delegation: vended-credentials`); the reserved prefix is
  read-only on the S3 side and lifecycle rules skip table buckets
- **Maintenance as admin jobs**: `plan` (metadata retention, snapshot expiry,
  orphan files, compaction candidates) / `run` / `purge` with a safety window
  and pointer re-check, driven by `lights3-ctl tables …`, the REST endpoints or
  an optional periodic runner; `fsck` reconciles the catalog with the buckets
- **Deployment choices**: any backend as the object store; gateways that share
  the default backend share the catalog; `tables.catalog_backing: duostore`
  moves the catalog into the DuoStore meta engine (RocksDB / SQLite / Redis /
  TiKV) with atomic commits, and `lights3 tables export|import` migrates
  between the two backings

Verified clients: PyIceberg 0.12 and DuckDB 1.5 (ctest `tables_smoke`); Spark
and Trino use the same REST settings (templates in the design doc, not yet
verified here). Design and implementation notes:
[docs/en/architecture/s3-tables-design.md](docs/en/architecture/s3-tables-design.md); commands:
[docs/en/usage/cli.md](docs/en/usage/cli.md) §2.6 and §3.13; endpoint list:
[docs/en/architecture/s3-protocol.md](docs/en/architecture/s3-protocol.md) §1.

## Install, package, containerize

Three channels, all documented in [docs/en/usage/deployment.md](docs/en/usage/deployment.md):

```bash
# 1. systemd service under /usr/local (build first)
./build.sh -DLIGHTS3_BUILD_TESTS=OFF
sudo ./scripts/install.sh                    # upgrade-safe: keeps config/secrets, old binary -> *.prev
sudo /usr/local/sbin/lights3ctl status
sudo ./scripts/rollback.sh                   # swap back to the previous binary
sudo ./scripts/uninstall.sh [--purge]

# 2. install tree / packages
sudo cmake --install build                   # or --prefix / DESTDIR
cmake --build build --target package         # build/packages/lights3_<ver>_<arch>.deb (rpm with rpmbuild)
make package                                 # same from a Release tree: build-rel/packages/
sudo apt install ./build/packages/lights3_0.1.0_amd64.deb

# 3. Docker / compose (docker/: Dockerfile, docker-compose.yml, image configs)
cd docker
docker compose up -d                         # localfs demo on :9000 (AKIDEXAMPLE / lights3-demo-secret)
docker compose --profile redis up -d         # duostore + redis meta on :9001 (tikv / rados profiles too)
docker compose --profile e2e run --rm e2e    # the redis / tikv / rados e2e paths that SKIP on a dev box
```

Every channel creates the `lights3` system user, keeps the configuration in
`/etc/lights3/lights3.yaml` (a deb conffile / rpm `%config(noreplace)`; never
overwritten by a re-install), generates random credentials into
`/etc/lights3/lights3.env` on the first install, and validates the live config
with `lights3 --check-config` before restarting the service. `lights3ctl help`
lists the start / stop / restart / status / journal commands.

## Current scope

- **Architecture**: four layers (HTTP Adapter / S3 Protocol / Storage / Core)
  with one-way dependencies; both pluggable boundaries — `IHttpServer` and
  `IStorageBackend` — are in place
- **HTTP drivers**: all four drivers are implemented, selected at runtime via
  `http.driver` and trimmed at compile time via CMake options; they share one
  driver-conformance test suite (the contract in
  [docs/architecture/http-adapter.md](docs/architecture/http-adapter.md) §4):
  - `builtin` — zero-dependency POSIX sockets, thread-per-connection;
  - `beast` — asynchronous Boost.Beast/Asio driver (the default performance
    path): N threads share one io_context, one per-connection session
    coroutine on a strand, deferred 100-continue;
  - `httplib` — synchronous cpp-httplib driver (thread-per-request, for
    functional verification); its push-model body is flipped to a pull model
    through a bounded queue;
  - `seastar` — shard-per-core reactor driver (compile-time optional,
    `-DLIGHTS3_DRIVER_SEASTAR=ON`); process-wide engine singleton, session
    coroutines bridge `seastar::future` into the project's `Task<T>`
- **Concurrency**: home-grown lazy `Task<T>` coroutines + `ThreadPool`;
  blocking IO is moved onto pool threads via `co_await pool.schedule()`,
  synchronous drivers bridge through `sync_wait`
- **Auth**: SigV4 implemented from scratch (header signing + presigned query),
  streaming payload SHA256 verification and aws-chunked per-chunk signature
  chains, unit tests cover the official AWS test vectors; presigned URLs are
  bounded on both sides (`X-Amz-Expires` for the past, a 15-minute clock-skew
  limit against future-dated `X-Amz-Date`)
- **Credential management**
  ([docs/en/architecture/credential-management.md](docs/en/architecture/credential-management.md)):
  runtime generate/query/revoke of AK/SK via `/-/admin/credentials`, persisted
  in storage; three credential sources (static config = root, external
  credentials file, dynamic) — only static credentials may call the admin API;
  at-rest AES-256-GCM encryption of secret keys via `LIGHTS3_MASTER_KEY`;
  hot-reloaded external credentials file (`auth.credentials_file`);
  periodic multi-instance sync (`auth.sync_interval`); per-credential policy
  (bucket glob whitelist + readonly)
- **Storage**: LocalFs (sidecar metadata, atomic writes via staging+rename),
  XLocalFs (io_uring data plane using raw syscalls, no liburing required),
  Memory (for tests), CloudProxy (self-signed SigV4 proxy to a remote S3,
  [docs/architecture/storage/cloudproxy-design.md](docs/architecture/storage/cloudproxy-design.md)), Tiered (cold-data
  tiering combinator, [docs/architecture/storage/tiered-design.md](docs/architecture/storage/tiered-design.md)),
  DuoStore (split metadata/data engine — meta: RocksDB/Redis/SQLite/TiKV,
  data: local fs/RADOS, [docs/architecture/storage/duostore-design.md](docs/architecture/storage/duostore-design.md));
  bucket-level glob routing
- **S3 API**: ListBuckets, Create/Head/DeleteBucket, Put/Get/Head/DeleteObject
  (including Range and conditional requests), CopyObject, batch DeleteObjects,
  ListObjectsV2 (prefix/delimiter/pagination), Multipart Upload
  (create/upload/upload-part-copy/list/complete/abort; UploadPartCopy supports
  `x-amz-copy-source-range` and copy-source conditional headers, and the
  source may live on a different backend than the destination); static website
  hosting for explicitly listed buckets (anonymous GET/HEAD with index/error
  documents, RedirectAllRequestsTo/RoutingRules, trailing-slash 302, per-bucket
  anonymous rate limiting, [docs/en/usage/static-website.md](docs/en/usage/static-website.md));
  CORS (`?cors` + OPTIONS preflight + response header injection); object tagging
  (`?tagging` + `x-amz-tagging` + `x-amz-tagging-count`); lifecycle minimal
  subset (Expiration.Days + AbortIncompleteMultipartUpload with a periodic
  enforcement scan); checksum persistence and echo (`x-amz-checksum-*` stored
  with the object, `x-amz-checksum-mode: ENABLED` on GET/HEAD, composite `-N`
  multipart checksums); `GET ?partNumber` with `x-amz-mp-parts-count`; STS
  AssumeRole session credentials (SigV4 `sts` scope, token-verified data-plane
  requests with TTL)
- **Usage / quotas / multi-tenancy / audit**
  ([docs/en/architecture/multi-tenancy.md](docs/en/architecture/multi-tenancy.md)): per-bucket usage
  counters (incremental + periodic full recount, `/-/admin/usage`); `?quota`
  bucket quotas and aggregate tenant quotas (`QuotaExceeded` 403, multipart
  parts counted); tenant entities with bucket ownership (credential
  `tenant`/`role`, tenants see only their own buckets, tiered admin plane);
  JSON-lines audit log
- **S3 Tables / Iceberg REST catalog**
  ([docs/en/architecture/s3-tables-design.md](docs/en/architecture/s3-tables-design.md)): table buckets,
  catalog state on `.sys` with conditional-write commits and idempotent replay,
  deep Avro validation, diagnostics / recovery, credential vending, maintenance
  jobs, views, multi-gateway operation, optional DuoStore-meta catalog backing
  (see the section above)

Not supported by design (returns NotImplemented; see
[docs/en/architecture/s3-protocol.md](docs/en/architecture/s3-protocol.md) §1): versioning, fine-grained
ACL (only "private" is accepted), bucket policy, lifecycle
transitions/tag filters, SSE-C/KMS, Object Lock, and presigned POST.

## Documentation

Documents are grouped by reader under [docs/](docs/README.md) (Chinese
originals) and [docs/en/](docs/en/README.md) (English mirror with identical
section numbering; source comments reference sections as
`docs/<group>/<name>.md §N`). Both indexes carry one-line summaries of every
document.

| Group ([en](docs/en/README.md) · [中文](docs/README.md)) | Documents |
| --- | --- |
| **Architecture** — what the system is made of and why | [overview](docs/en/architecture/overview.md), [http-adapter](docs/en/architecture/http-adapter.md), [concurrency](docs/en/architecture/concurrency.md), [coroutine-internals](docs/en/architecture/coroutine-internals.md), [object-read-write-flow](docs/en/architecture/object-read-write-flow.md), [s3-protocol](docs/en/architecture/s3-protocol.md), [credential-management](docs/en/architecture/credential-management.md), [multi-tenancy](docs/en/architecture/multi-tenancy.md), [s3-tables-design](docs/en/architecture/s3-tables-design.md), [storage/](docs/en/architecture/storage/README.md) (storage-backend, tiered, cloudproxy, duostore and its redis / sqlite / tikv meta and rados data engines; 13 implementation-level documents in Chinese) |
| **Usage** — deploying, configuring, operating | [deployment](docs/en/usage/deployment.md), [cli](docs/en/usage/cli.md), [config-reload](docs/en/usage/config-reload.md), [tls](docs/en/usage/tls.md), [monitoring](docs/en/usage/monitoring.md), [static-website](docs/en/usage/static-website.md); every config key is documented in [config/lights3.yaml](config/lights3.yaml) |
| **Development** — building, testing, contributing | [contributing](docs/en/development/contributing.md), [testing](docs/en/development/testing.md), [performance-baseline](docs/en/development/performance-baseline.md), [todo](docs/en/development/todo.md) |
| **Archive** — closed ledgers, read-only | `docs/archive/` (roadmap, gaps, issues, backlog; the targets of `roadmap §N` / `backlog §N` in source comments) |
