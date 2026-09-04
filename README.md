# LightS3

An S3-protocol gateway written in C++20. It exposes the standard S3 REST API on
the outside, with pluggable HTTP drivers and storage backends on the inside.
Design documents live in [docs/](docs/README.md) (Chinese originals, English
translations under [docs/en/](docs/en/README.md)); the current implementation
follows the architecture described in
[docs/en/architecture.md](docs/en/architecture.md).

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
│ XML codec · S3Error mapping · Metrics · access log                    │
└─────────────────────────────────┬─────────────────────────────────────┘
                   IStorageBackend (Task<T>, streaming)
┌─ L3 · Storage ──────────────────▼─────────────────────────────────────┐
│ BucketRouter: glob rules → backend; ".sys" reserved for credentials   │
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
or a separate `-B build-x` directory to switch them off. Sanitizer builds:
`./build.sh --asan` / `--tsan`.

The MinIO mint compatibility suite is a manual gate (not wired into ctest;
needs docker and skips cleanly without it — see
[docs/en/s3-protocol.md](docs/en/s3-protocol.md) §8):

```bash
tests/e2e/run_mint.sh build/lights3 s3cmd awscli
```

## Run

```bash
export LIGHTS3_SECRET_1=my-secret
# optional: encrypt dynamically generated secret keys at rest (AES-256-GCM).
# Once enabled, starting without the key (or with a wrong one) fails fast.
export LIGHTS3_MASTER_KEY=$(openssl rand -hex 32)
./build/lights3 --config=config/lights3.yaml
```

The ops CLI `s3adm` (credentials, bucket websites, benchmarks) and the full
`lights3` command tree are documented in [docs/en/cli.md](docs/en/cli.md).

Access it with any S3 client (the examples below use curl's SigV4 support):

```bash
alias s3curl='curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "AKIDEXAMPLE:$LIGHTS3_SECRET_1"'
s3curl -X PUT http://127.0.0.1:9000/mybucket                      # CreateBucket
s3curl -X PUT --data-binary @file.bin http://127.0.0.1:9000/mybucket/file.bin
s3curl http://127.0.0.1:9000/mybucket?list-type=2                 # ListObjectsV2
s3curl -r 0-99 http://127.0.0.1:9000/mybucket/file.bin            # Range download
```

Or use the aws cli: `aws --endpoint-url http://127.0.0.1:9000 s3 ls`.

## Install as a systemd service

Build the binaries, then run the installer as root:

```bash
./build.sh -DLIGHTS3_BUILD_TESTS=OFF
sudo ./scripts/install.sh
sudo /usr/local/sbin/lights3ctl status
```

The installer creates a dedicated `lights3` system user, installs the server
under `/usr/local/bin`, and runs it with `/etc/lights3/lights3.yaml` from the
`/var/lib/lights3` working directory. On the first install it also writes random
credentials to `/etc/lights3/lights3.env`; both that file and an existing YAML
configuration are preserved on upgrades. Use `lights3ctl help` for start, stop,
restart, status, and journal commands. Pass `--no-start` if the configuration
must be adjusted before the first launch.

## Current scope

- **Architecture**: four layers (HTTP Adapter / S3 Protocol / Storage / Core)
  with one-way dependencies; both pluggable boundaries — `IHttpServer` and
  `IStorageBackend` — are in place
- **HTTP drivers**: all four drivers are implemented, selected at runtime via
  `http.driver` and trimmed at compile time via CMake options; they share one
  driver-conformance test suite (the contract in
  [docs/http-adapter.md](docs/http-adapter.md) §4):
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
  ([docs/en/credential-management.md](docs/en/credential-management.md)):
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
  [docs/cloudproxy-backend.md](docs/cloudproxy-backend.md)), Tiered (cold-data
  tiering combinator, [docs/tiered-storage.md](docs/tiered-storage.md)),
  DuoStore (split metadata/data engine — meta: RocksDB/Redis/SQLite/TiKV,
  data: local fs/RADOS, [docs/duostore-backend.md](docs/duostore-backend.md));
  bucket-level glob routing
- **S3 API**: ListBuckets, Create/Head/DeleteBucket, Put/Get/Head/DeleteObject
  (including Range and conditional requests), CopyObject, batch DeleteObjects,
  ListObjectsV2 (prefix/delimiter/pagination), Multipart Upload
  (create/upload/upload-part-copy/list/complete/abort; UploadPartCopy supports
  `x-amz-copy-source-range` and copy-source conditional headers, and the
  source may live on a different backend than the destination); static website
  hosting for explicitly listed buckets (anonymous GET/HEAD with index/error
  documents, RedirectAllRequestsTo/RoutingRules, trailing-slash 302, per-bucket
  anonymous rate limiting, [docs/en/static-website.md](docs/en/static-website.md));
  CORS (`?cors` + OPTIONS preflight + response header injection); object tagging
  (`?tagging` + `x-amz-tagging` + `x-amz-tagging-count`); lifecycle minimal
  subset (Expiration.Days + AbortIncompleteMultipartUpload with a periodic
  enforcement scan); checksum persistence and echo (`x-amz-checksum-*` stored
  with the object, `x-amz-checksum-mode: ENABLED` on GET/HEAD, composite `-N`
  multipart checksums); `GET ?partNumber` with `x-amz-mp-parts-count`; STS
  AssumeRole session credentials (SigV4 `sts` scope, token-verified data-plane
  requests with TTL)
- **Usage / quotas / multi-tenancy / audit**
  ([docs/en/multi-tenancy.md](docs/en/multi-tenancy.md)): per-bucket usage
  counters (incremental + periodic full recount, `/-/admin/usage`); `?quota`
  bucket quotas and aggregate tenant quotas (`QuotaExceeded` 403, multipart
  parts counted); tenant entities with bucket ownership (credential
  `tenant`/`role`, tenants see only their own buckets, tiered admin plane);
  JSON-lines audit log

Not supported by design (returns NotImplemented; see
[docs/en/s3-protocol.md](docs/en/s3-protocol.md) §1): versioning, fine-grained
ACL (only "private" is accepted), bucket policy, lifecycle
transitions/tag filters, SSE-C/KMS, Object Lock, and presigned POST.

## Documentation

Design docs are written in Chinese under [docs/](docs/README.md); English
translations live in [docs/en/](docs/en/README.md) and mirror the Chinese
section numbering (source comments reference sections as `docs/<name>.md §N`).

| Document ([en](docs/en/README.md) · [中文](docs/README.md)) | Contents |
| --- | --- |
| [architecture](docs/en/architecture.md) | Overall architecture, layering, request lifecycle, code layout |
| [tls](docs/en/tls.md) | HTTPS on all drivers, certificate hot reload, mTLS / ciphers / SNI, reverse-proxy termination |
| [http-adapter](docs/en/http-adapter.md) | Pluggable HTTP layer: neutral request/response model, streaming bodies, driver notes |
| [concurrency](docs/en/concurrency.md) | Task coroutines, Executor abstraction, thread pool, sync/async driver bridging |
| [storage-backend](docs/en/storage-backend.md) | `IStorageBackend`, LocalFs/XLocalFs, bucket routing, new-backend guide |
| [s3-protocol](docs/en/s3-protocol.md) | API scope, SigV4 (incl. presigned & clock skew), XML codec, errors, mint gate |
| [credential-management](docs/en/credential-management.md) | AK/SK admin API, three credential sources, `.sys` persistence, at-rest encryption, policy |
| [multi-tenancy](docs/en/multi-tenancy.md) | Usage accounting, bucket/tenant quotas, tenants and bucket ownership, tiered admin plane, audit log |
| [object-read-write-flow](docs/en/object-read-write-flow.md) | End-to-end read/write paths, BodyReader chains, staging commit, fd-snapshot reads |
| [tiered-storage](docs/en/tiered-storage.md) | Cold-data tiering to cloud, stub metadata, transparent read-back |
| [cloudproxy-backend](docs/en/cloudproxy-backend.md) | Self-signed SigV4 proxy to remote S3, streaming pumps, retries |
| [duostore-backend](docs/en/duostore-backend.md) | Split meta/data engine: RocksDB meta, chunk/pack, GC |
| [duostore-redis-meta](docs/en/duostore-redis-meta.md) | Redis IMetaStore: hiredis + Lua guarded-commit |
| [duostore-sqlite-meta](docs/en/duostore-sqlite-meta.md) | SQLite IMetaStore: embedded amalgamation, WAL, read pool |
| [duostore-rados-data](docs/en/duostore-rados-data.md) | RADOS IDataStore: librados, chunk → rados objects |
| [duostore-tikv-meta](docs/en/duostore-tikv-meta.md) | TiKV IMetaStore: client-c + 2PC sidecar |
| [cli](docs/en/cli.md) | `lights3` / `s3adm` command reference: startup, duostore dump/load, cred/website/bench/quota/tenant/usage |
