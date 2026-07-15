# LightS3

An S3-protocol gateway written in C++20. It exposes the standard S3 REST API on
the outside, with pluggable HTTP drivers and storage backends on the inside.
Design documents live in [docs/](docs/README.md) (in Chinese); the current
implementation follows the architecture described in
[docs/01-architecture.md](docs/01-architecture.md).

*中文介绍见 [docs/README.zh-CN.md](docs/README.zh-CN.md)。*

## Build and test

Requirements: g++ ≥ 13 (C++20 coroutines), CMake ≥ 3.20, OpenSSL.
The beast driver needs Boost headers (≥ 1.75, header-only, no compiled
libraries; if system Boost is not found, point `BOOST_ROOT` at the header
directory, or disable the driver with `-DLIGHTS3_DRIVER_BEAST=OFF`).
httplib, spdlog and gflags are git submodules under `third_party/` and must be
initialized before the first build.

```bash
git submodule update --init
cmake -B build
cmake --build build -j
ctest --test-dir build --output-on-failure   # unit tests + per-driver e2e (e2e needs curl ≥ 7.75)
```

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
- **HTTP drivers**: all three drivers are implemented, selected at runtime via
  `http.driver` and trimmed at compile time via CMake options; they share one
  driver-conformance test suite (the contract in
  [docs/02-http-adapter.md](docs/02-http-adapter.md) §4):
  - `builtin` — zero-dependency POSIX sockets, thread-per-connection;
  - `beast` — asynchronous Boost.Beast/Asio driver (the default performance
    path): N threads share one io_context, one per-connection session
    coroutine on a strand, deferred 100-continue;
  - `httplib` — synchronous cpp-httplib driver (thread-per-request, for
    functional verification); its push-model body is flipped to a pull model
    through a bounded queue
- **Concurrency**: home-grown lazy `Task<T>` coroutines + `ThreadPool`;
  blocking IO is moved onto pool threads via `co_await pool.schedule()`,
  synchronous drivers bridge through `sync_wait`
- **Auth**: SigV4 implemented from scratch (header signing + presigned query),
  streaming payload SHA256 verification, unit tests cover the official AWS
  test vectors
- **Storage**: LocalFs (sidecar metadata, atomic writes via staging+rename),
  XLocalFs (io_uring data plane using raw syscalls, no liburing required),
  Memory (for tests); bucket-level glob routing
- **S3 API**: ListBuckets, Create/Head/DeleteBucket, Put/Get/Head/DeleteObject
  (including Range and conditional requests), ListObjectsV2
  (prefix/delimiter/pagination)

Not implemented yet (returns NotImplemented; see
[docs/05-s3-protocol.md](docs/05-s3-protocol.md) for the roadmap): Multipart
Upload, CopyObject, batch DeleteObjects, the cloudproxy backend, and
aws-chunked streaming signatures.
