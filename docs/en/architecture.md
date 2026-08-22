# Overall Architecture

> English translation of [../architecture.md](../architecture.md). The Chinese original is authoritative; section numbering matches.

## 1. Design Goals

| Goal | Description |
| --- | --- |
| S3 compatible | Compatible with mainstream S3 clients (aws cli, s3cmd, boto3, MinIO SDK); implements the commonly used API subset |
| Pluggable HTTP library | Swapping the HTTP library touches neither the protocol-layer nor the storage-layer code |
| High-throughput large objects | Streaming across the whole pipeline; memory footprint is independent of object size |
| Extensible backends | Adding a storage backend only requires implementing one interface and registering a factory |
| Simple deployment | Single binary + one YAML config file; the default form has no external service dependencies (the duostore backend can optionally attach external meta/data services: Redis/TiKV/Ceph, see [storage-backend.md](storage-backend.md) §5) |

Non-goals (not in the first phase): multi-node gateway cluster (the credential
plane already has periodic sync via `auth.sync_interval`, see
[credential-management.md](credential-management.md) §10.3;
the data plane still assumes a single instance), erasure coding,
bucket versioning, Object Lock, event notifications. Multi-replica and
horizontal scaling on the metadata/data side are not implemented by the
gateway; they are obtained from external systems through duostore's
TiKV meta / RADOS data plugins.

## 2. Layered Architecture

Four layers from top to bottom; the dependency direction is strictly one-way
(upper layers depend on lower layers' interfaces, not their implementations):

```text
┌──────────────────────────────────────────────────────────┐
│ L1  HTTP Adapter layer                                   │
│     Duties: network listening, HTTP parsing,             │
│             converting requests into the neutral model   │
│     Deliverables: the IHttpServer implementations        │
│                   (builtin/Beast/httplib/seastar)        │
├──────────────────────────────────────────────────────────┤
│ L2  S3 Protocol layer                                    │
│     Duties: URL routing, SigV4 authentication, S3        │
│             semantics, XML encode/decode, multipart      │
│             state machine, error-code mapping            │
├──────────────────────────────────────────────────────────┤
│ L3  Storage layer                                        │
│     Duties: object read/write/delete/list, multipart     │
│             materialization                              │
│     Deliverables: IStorageBackend implementations        │
│                   + BucketRouter                         │
├──────────────────────────────────────────────────────────┤
│ L4  Core / Runtime layer (cross-cutting)                 │
│     Duties: Task<T> coroutine primitives, Executor,      │
│             ThreadPool, timers/cancellation/semaphore,   │
│             Config, Log,                                 │
│             utilities (hex/crypto/uri/time)              │
└──────────────────────────────────────────────────────────┘
```

There are two interfaces at the core decoupling points:

- `IHttpServer` / `HttpRequest` / `HttpResponse`: the boundary between L1 and L2
  (see [http-adapter.md](http-adapter.md)).
- `IStorageBackend`: the boundary between L2 and L3 (see [storage-backend.md](storage-backend.md)).

L2 is a pure-logic layer: it contains no socket, epoll, concrete HTTP library,
or storage SDK headers, and can be fully covered in unit tests with mocked
Http models and the in-memory backend.

## 3. Request Lifecycle

Take `GET /mybucket/dir/a.bin` (bucket routed to LocalFs) as an example:

```text
 client ──► [L1] HTTP library accept + parse headers
              │  build HttpRequest{method,path,query,headers,BodyReader}
              ▼
            [L2] S3Service::dispatch(req)                  ← coroutine entry
              │  1. Parse (bucket, key): path-style; virtual-host style
              │     is supported once base_domain is configured
              │  2. SigV4Authenticator::verify(req)        ← look up AK/SK, verify signature
              │  3. explicit dispatch table → GetObject handler (handlers/objects.cc)
              ▼
            [L2] GetObject handler
              │  1. router.resolve("mybucket") → LocalFsBackend
              │  2. co_await backend.get_object(bucket,key,range)
              ▼
            [L3] LocalFsBackend::get_object
              │  1. co_await pool_.schedule()              ← switch to the IO thread pool
              │  2. open + fstat + read the .meta sidecar
              │  3. return ObjectStream{meta, BodyReader}
              ▼
            [L2] assemble HttpResponse{200, headers(ETag/Content-Type/...), BodyReader}
              ▼
            [L1] loop co_await body.read(buf) → write socket   ← streaming, 64KB chunks
 client ◄── response complete, connection returned
```

Key points:

- **Streaming across the whole pipeline**: L3 returns a `BodyReader` that can be
  pulled chunk by chunk, and L1 writes to the socket while reading; the PUT
  direction is symmetric — L1's `BodyReader` is passed straight through to L3
  to write files / forward to the cloud.
- **Threading model**: protocol-layer logic runs in the HTTP library's IO
  execution environment; any potentially blocking call (posix IO, cloudproxy's
  remote HTTP calls) first does `co_await pool.schedule()` to switch to the
  thread pool, then switches back afterwards (details in [concurrency.md](concurrency.md)).

## 4. Process Structure and Startup Flow

The assembly flow in `src/main.cc` (logging and error handling omitted):

```cpp
int main(int argc, char** argv) {
    // ccmd parses --config (no subcommand = start the server; `lights3 duostore dump|load` is the ops entry), Config::load reads the YAML
    auto cfg      = Config::load(FLAGS_config);
    Logger::init(parse_level(cfg.log_level));
    auto pool     = std::make_shared<ThreadPool>(cfg.runtime.io_threads);
    auto metrics  = std::make_shared<MetricsRegistry>();          // backend-level metrics registry
    auto backends = StorageRegistry::build(cfg.backends, pool, metrics);  // construct each backend
    auto router   = BucketRouter::build(cfg.buckets, std::move(backends));
    auto auth     = SigV4Authenticator::build(cfg.auth);          // static credential table
    // Dynamic credentials (docs/credential-management.md): load from the default
    // backend and replace the static lookup table
    auto cred_store = sync_wait(CredentialStore::load(router.default_backend(), cfg.auth));
    auth.set_provider(cred_store);
    auto service  = std::make_shared<S3Service>(std::move(router), std::move(auth),
                                                cfg.http.base_domain);      // L2 entry point
    service->set_backend_metrics(metrics);      // /-/metrics appends backend-level metrics
    service->set_credential_store(cred_store);  // per-credential policy enforcement point
    // Credential phase-2 background tasks (credential-management.md §10.2/§10.3):
    // credentials_file hot-reload polling + periodic multi-instance incremental
    // sync (both gated by config)
    cred_store->start_background(pool);

    // Choose the HTTP driver by config (can be trimmed at compile time via CMake options)
    auto server = HttpServerFactory::create(cfg.http.driver, cfg.http);
    // dispatch entry throttling (docs/concurrency.md §6): over-limit requests
    // queue on the semaphore
    auto inflight = std::make_shared<AsyncSemaphore>(cfg.runtime.max_inflight_requests, ...);
    server->set_handler([=](HttpRequest req) -> Task<HttpResponse> {
        auto permit = co_await inflight->acquire();
        co_return co_await service->dispatch(std::move(req));
    });
    server->listen(cfg.http.bind, cfg.http.port);
    server->run();          // blocks until SIGTERM/SIGINT
}
```

Graceful shutdown: the signal handler triggers `server->shutdown()`
(async-signal-safe operations only) → stop accepting, wait for in-flight
requests to finish → `run()` returns →
`cred_store->shutdown_background()` (retract timers and wait for in-flight
syncs to wrap up; this **must precede** the thread-pool join, otherwise
background coroutines may post to an already-closed pool) → thread pool `join()`.

## 5. Example Configuration File

```yaml
http:
  driver: builtin          # builtin | beast | httplib | seastar (must be compiled in via the matching CMake option)
  bind: 0.0.0.0
  port: 9000
  io_threads: 4            # io_context thread count for async drivers
  max_header_size: 16KiB
  idle_timeout: 60s
  min_part_size: 5MiB      # minimum non-final multipart part; 0 disables
  # base_domain: s3.local  # non-empty enables virtual-host style addressing

runtime:
  io_threads: 16           # blocking IO thread pool size (shared by all backends by
                           # default; a backend may set the same key to get a dedicated
                           # isolated pool, concurrency.md §3.1)
  max_inflight_requests: 1024

auth:
  credentials:
    - access_key: AKIDEXAMPLE
      secret_key: ${LIGHTS3_SECRET_1}     # environment variable references supported; if empty, all requests are rejected
  region: us-east-1
  # Credential management phase 2 (credential-management.md §10); at-rest encryption
  # of dynamic-credential SKs is not configured here: set the environment variable
  # LIGHTS3_MASTER_KEY (openssl rand -hex 32) to enable it
  # credentials_file: /etc/lights3/creds.json  # external credentials file (hot reload, data plane only)
  # credentials_file_reload: 30s               # file mtime polling period; 0s = load at startup only
  # sync_interval: 0s                          # multi-instance periodic incremental reload of .sys credentials; 0s = off

backends:
  - name: localdata
    type: localfs                         # localfs | xlocalfs | memory | tiered | cloudproxy | duostore
    root: /var/lib/lights3/data
    staging: /var/lib/lights3/staging     # multipart staging, must be on the same filesystem as root
  - name: aws-archive
    type: cloudproxy                      # see cloudproxy-backend.md
    endpoint: https://s3.us-west-2.amazonaws.com
    region: us-west-2
    access_key: AKIA...
    secret_key: ${AWS_SECRET}
    bucket_prefix: corp-archive-          # real remote bucket = prefix + local bucket name

buckets:
  default_backend: localdata
  rules:
    - match: "archive-*"                  # glob, matched in declaration order
      backend: aws-archive

log:
  level: info
```

A runnable minimal example lives at `config/lights3.yaml` in the repository root.

## 6. Source Tree Layout

```text
lights3/
├── CMakeLists.txt
├── build.sh
├── config/lights3.yaml       # runnable minimal example config
├── docs/
├── src/
│   ├── core/                 # L4: business-agnostic infrastructure
│   │   ├── task.h            #   Task<T>, sync_wait, when_all, with_timeout
│   │   ├── executor.h        #   IExecutor, ThreadPoolExecutor
│   │   ├── thread_pool.h/.cc #   bounded queue + backpressure
│   │   ├── semaphore.h       #   AsyncSemaphore (entry throttling)
│   │   ├── timer.h/.cc       #   timer thread (foundation of with_timeout)
│   │   ├── cancel.h          #   cooperative cancellation primitives
│   │   ├── background.h/.cc  #   background task wait group (concurrency.md §7)
│   │   ├── config.h/.cc      #   YAML parsing + typed config
│   │   ├── metrics.h/.cc     #   backend-level metrics registry + scope (dispatched with a backend=<name> label)
│   │   ├── log.h             #   spdlog facade
│   │   └── util/             #   hex, crypto(OpenSSL SHA256/HMAC), uri, time
│   ├── http/                 # L1
│   │   ├── model.h           #   HttpRequest/HttpResponse/BodyReader
│   │   ├── server.h/.cc      #   IHttpServer, HttpServerFactory
│   │   ├── pushpull.h        #   push-model ↔ pull-model inversion component
│   │   └── drivers/
│   │       ├── common.h      #   contract implementations shared by drivers
│   │       ├── builtin/      #   POSIX socket synchronous driver (zero dependencies)
│   │       ├── beast/        #   Boost.Beast + asio asynchronous driver
│   │       ├── httplib/      #   cpp-httplib synchronous driver (thread-per-request)
│   │       └── seastar/      #   Seastar shard-per-core driver (heavy dependency, off by default)
│   ├── s3/                   # L2
│   │   ├── service.h/.cc     #   S3Service::dispatch + explicit dispatch table
│   │   ├── router.h/.cc      #   URL → (bucket, key) parsing
│   │   ├── auth/             #   sigv4.h/.cc, credential_store.h/.cc (dynamic credentials)
│   │   ├── handlers/         #   objects.cc buckets.cc list_objects.cc
│   │   │                     #   multipart.cc admin_credentials.cc
│   │   ├── xml.h/.cc         #   S3 XML encode/decode (small home-grown generator/parser)
│   │   ├── errors.h/.cc      #   S3ErrorCode ↔ HTTP status ↔ XML body
│   │   └── metrics.h/.cc     #   Prometheus text-format metrics
│   └── storage/              # L3
│       ├── backend.h         #   IStorageBackend, ObjectMeta, the Options structs
│       ├── registry.h/.cc    #   type string → factory (two-phase build for composite backends)
│       ├── bucket_router.h/.cc
│       ├── validate.cc  listing.h/.cc  multipart.h/.cc   # logic shared across backends
│       ├── memory/           #   in-memory backend (for tests)
│       ├── localfs/          #   local filesystem backend
│       ├── xlocalfs/         #   io_uring data-plane variant of localfs
│       ├── tiered/           #   tiered-storage composite backend (see tiered-storage.md)
│       ├── cloudproxy/       #   public-cloud proxy backend (see cloudproxy-backend.md)
│       └── duostore/         #   metadata/data split engine (see duostore-backend.md)
├── tests/
│   ├── unit/                 # L2/L3 pure-logic tests (mock http + in-memory backend)
│   └── e2e/                  # start a real process, drive requests with the aws cli
└── third_party/              # httplib/ccmd/spdlog/json etc. as submodules
```

Dependency policy: the core (core/s3/storage) depends on the standard library +
OpenSSL (SigV4 needs SHA256/HMAC) + spdlog (logging) + ccmd (command line, bundles cflag) +
nlohmann/json (admin credential API, kept out of public headers); each HTTP
driver plus the cloudproxy and duostore backends are isolated behind CMake
options (`LIGHTS3_DRIVER_BEAST`, `LIGHTS3_CLOUDPROXY`, `LIGHTS3_DUOSTORE`
and its sub-switches `LIGHTS3_DUOSTORE_REDIS_META` / `LIGHTS3_DUOSTORE_SQLITE_META` /
`LIGHTS3_DUOSTORE_RADOS_DATA` / `LIGHTS3_DUOSTORE_TIKV_META`, etc.);
when not enabled they are excluded from compilation. cloudproxy pulls in no
cloud SDK — it signs SigV4 itself with the vendored httplib and talks to the
remote end directly.
