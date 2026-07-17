# LightS3

An S3-protocol gateway written in C++20. It exposes the standard S3 REST API on
the outside, with pluggable HTTP drivers and storage backends on the inside.
Design documents live in [docs/](docs/README.md) (in Chinese); the current
implementation follows the architecture described in
[docs/01-architecture.md](docs/01-architecture.md).

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
│        └─ route table (method + scope + query flag)                   │
│             └─ handlers: buckets / objects / list_objects / multipart │
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
httplib, spdlog, gflags and nlohmann/json are git submodules under
`third_party/` and must be initialized before the first build.

```bash
./build.sh --test        # submodules + cmake + ninja + ctest in one go
```

or manually:

```bash
git submodule update --init third_party/gflags third_party/spdlog \
    third_party/httplib third_party/json
cmake -B build
cmake --build build -j
ctest --test-dir build --output-on-failure   # unit tests + per-driver e2e (e2e needs curl ≥ 7.75)
```

The seastar driver is off by default (heavy dependencies); enable with
`./build.sh --seastar`. Sanitizer builds: `./build.sh --asan` / `--tsan`.

## Run

```bash
export LIGHTS3_SECRET_1=my-secret
./build/lights3 --config config/lights3.yaml
```

Access it with any S3 client (the examples below use curl's SigV4 support):

```bash
alias s3curl='curl -s --aws-sigv4 "aws:amz:us-east-1:s3" --user "AKIDEXAMPLE:$LIGHTS3_SECRET_1"'
s3curl -X PUT http://127.0.0.1:9000/mybucket                      # CreateBucket
s3curl -X PUT --data-binary @file.bin http://127.0.0.1:9000/mybucket/file.bin
s3curl http://127.0.0.1:9000/mybucket?list-type=2                 # ListObjectsV2
s3curl -r 0-99 http://127.0.0.1:9000/mybucket/file.bin            # Range download
```

Or use the aws cli: `aws --endpoint-url http://127.0.0.1:9000 s3 ls`.

## Current scope

- **Architecture**: four layers (HTTP Adapter / S3 Protocol / Storage / Core)
  with one-way dependencies; both pluggable boundaries — `IHttpServer` and
  `IStorageBackend` — are in place
- **HTTP drivers**: all four drivers are implemented, selected at runtime via
  `http.driver` and trimmed at compile time via CMake options; they share one
  driver-conformance test suite (the contract in
  [docs/02-http-adapter.md](docs/02-http-adapter.md) §4):
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
  chains, unit tests cover the official AWS test vectors; runtime credential
  management (generate/query/revoke AK/SK, persisted in storage) via
  `/-/admin/credentials` ([docs/06](docs/06-credential-management.md))
- **Storage**: LocalFs (sidecar metadata, atomic writes via staging+rename),
  XLocalFs (io_uring data plane using raw syscalls, no liburing required),
  Memory (for tests); bucket-level glob routing
- **S3 API**: ListBuckets, Create/Head/DeleteBucket, Put/Get/Head/DeleteObject
  (including Range and conditional requests), CopyObject, batch DeleteObjects,
  ListObjectsV2 (prefix/delimiter/pagination), Multipart Upload
  (create/upload/list/complete/abort)

Not implemented yet (returns NotImplemented; see
[docs/05-s3-protocol.md](docs/05-s3-protocol.md) for the roadmap): the
cloudproxy backend, versioning, ACL/policy, lifecycle, SSE, and Object Lock.
