# LightS3 documentation

> English translation of [../README.md](../README.md). Section numbering matches
> the Chinese originals one-to-one; source-code comments reference sections as
> `docs/<name>.md §N`, which applies to both language versions.

LightS3 is an S3-protocol gateway implemented in C++20. It exposes the standard
S3 REST API on the outside and routes requests to pluggable storage backends on
the inside. The design emphasizes three points:

1. **Pluggable HTTP libraries** — the core business logic depends on no
   concrete HTTP library; an adapter layer switches between builtin (POSIX
   sockets), Boost.Beast, cpp-httplib and Seastar drivers.
2. **Coroutines + thread pool as a dual execution model** — the request path is
   expressed with C++20 coroutines; blocking operations (disk IO, remote S3
   calls) are offloaded to a dedicated thread pool, and the two models meet
   through a unified Executor abstraction.
3. **Multiple storage backends** — backends are abstracted behind
   `IStorageBackend` and routed at bucket granularity. Implemented so far:
   local filesystem (LocalFs / XLocalFs), in-memory (Memory, for tests),
   public-cloud proxy (CloudProxy), hot/cold tiering combinator (Tiered), and a
   split metadata/data engine (DuoStore — meta: RocksDB/Redis/SQLite/TiKV,
   data: local fs/RADOS).

## Document index

The documents are grouped by reader: **architecture** (what the system is made
of and why), **usage** (deploying, configuring, operating), **development**
(building, testing, contributing). Chinese is the original; every document here
mirrors it with identical section numbering, so a `docs/<group>/<name>.md §N`
reference in a source comment applies to both languages.

### Architecture ([architecture/](architecture/))

| Document | Contents |
| --- | --- |
| [overview.md](architecture/overview.md) | Overall architecture, layering, request lifecycle, process assembly, source layout |
| [http-adapter.md](architecture/http-adapter.md) | Pluggable HTTP layer: neutral request/response model, streaming bodies, notes on the four drivers |
| [concurrency.md](architecture/concurrency.md) | Concurrency model: Task coroutines, Executor abstraction, thread pool, unifying sync/async HTTP libraries |
| [coroutine-internals.md](architecture/coroutine-internals.md) | Coroutine internals: Task promise layout & symmetric transfer, top-level drivers, when_all/with_timeout, cancellation race protocols and lifetime rules |
| [object-read-write-flow.md](architecture/object-read-write-flow.md) | Object read/write flow: the three-layer code path, BodyReader chains, atomic staging commit, fd-snapshot reads |
| [s3-protocol.md](architecture/s3-protocol.md) | S3 protocol: API scope, SigV4 (incl. presigned, STS and clock skew), Multipart Upload, error mapping, observability, mint compatibility gate |
| [credential-management.md](architecture/credential-management.md) | Credential management: AK/SK admin API, three credential sources (static root / file / dynamic), `.sys` persistence, at-rest SK encryption, hot-reloaded credentials file, multi-instance sync, per-credential policy, STS sessions |
| [multi-tenancy.md](architecture/multi-tenancy.md) | Usage accounting, bucket/tenant quotas, tenant entities and bucket ownership, tiered admin plane, audit log |
| [s3-tables-design.md](architecture/s3-tables-design.md) | S3 Tables / Apache Iceberg REST Catalog: catalog state on `.sys` with a CAS commit protocol, REST endpoints and error model, deep Avro validation with diagnostics / recovery, table-bucket guard and credential vending, maintenance jobs, the multi-gateway matrix, views, the duostore-meta catalog backing |
| [storage/](architecture/storage/README.md) | Storage layer, design tier: `storage-backend.md` (interface abstraction, bucket routing, LocalFs/XLocalFs, new-backend guide) and the `*-design.md` of tiered / cloudproxy / duostore (with its redis / sqlite / tikv meta and rados data engines); the 13 implementation-level documents exist in Chinese only under `docs/architecture/storage/` |

### Usage ([usage/](usage/))

| Document | Contents |
| --- | --- |
| [deployment.md](usage/deployment.md) | Build and distribution: `--version` / embedded git commit, the `cmake --install` tree, deb/rpm packages, Dockerfile + compose under `docker/` (redis/tikv/rados/multi/e2e profiles), multi-gateway deployment, upgrade rollback and uninstall |
| [cli.md](usage/cli.md) | Command-line tools: `lights3` startup and its `duostore` / `tier` / `fsck` / `tables` subcommands, the `lights3-ctl` cred/website/bench/fsck/quota/tenant/usage/tables commands, option semantics and exit codes |
| [config-reload.md](usage/config-reload.md) | Configuration hot reload: SIGHUP / admin API / `lights3-ctl reload`, whole-file validation, the hot-reloadable subset and the "requires restart" report |
| [tls.md](usage/tls.md) | TLS: HTTPS on all four drivers, certificate hot reload, mTLS / ciphers / minimum version / SNI multi-certificate, reverse-proxy termination examples |
| [monitoring.md](usage/monitoring.md) | Monitoring consumers: the Prometheus scrape config and alert/recording rules under `deploy/`, the Grafana dashboard and its generator |
| [static-website.md](usage/static-website.md) | Static website hosting: anonymous-read semantics, index / error documents, routing rules and object-level redirects, rate limiting |

Every configuration key is documented with its default in the comments of
[config/lights3.yaml](../../config/lights3.yaml); the project introduction
(build / run / current scope) is the repository root [README.md](../../README.md).

### Development ([development/](development/))

| Document | Contents |
| --- | --- |
| [contributing.md](development/contributing.md) | Development guide: repository layout, build variants, verification routine, code and documentation conventions, branches and commits |
| [testing.md](development/testing.md) | Testing: the ctest matrix and labels, the e2e sections, fuzz harnesses, fault injection, the performance gate and soak, mint, ubsan/coverage, the one-shot matrix script, the Makefile |
| [performance-baseline.md](development/performance-baseline.md) | Performance baseline: the `scripts/bench_matrix.sh` matrix of 4 drivers × TLS × put/get, before/after the data-plane work, how to reproduce |
| [todo.md](development/todo.md) | Open items and plans: pending verification, findings from the performance baseline, long-term items, the not-planned list; entries are deleted when done |

### Archive (`docs/archive/`, Chinese only)

The closed ledgers (gaps.md / issues.md / roadmap.md / backlog.md /
backlog-sequence.md) and the finished design-step document
(multi-gateway-multipart-design.md): read-only, no longer updated, and the
place source comments point at with `docs/archive/<name>.md §N`, `roadmap §N`,
`backlog §N`, `backlog-sequence ①…⑩` and `multi-gateway-multipart §4 ①…④`.

## One-page architecture

```text
                ┌────────────────────────────────────────────────┐
                │                HTTP Adapter layer              │
                │   Builtin / Beast / Httplib / Seastar drivers  │
                │  (implement IHttpServer, compile/runtime pick) │
                └───────────────────────┬────────────────────────┘
                                        │ HttpRequest / HttpResponse (neutral model)
                ┌───────────────────────▼────────────────────────┐
                │                 S3 Protocol layer              │
                │  Router → SigV4 Auth → policy auth → Handler   │
                │  XML codec / error mapping / multipart states  │
                └───────────────────────┬────────────────────────┘
                                        │ IStorageBackend (async streaming interface)
                ┌───────────────────────▼────────────────────────┐
                │                  Storage layer                 │
                │  LocalFs/XLocalFs · Memory · CloudProxy        │
                │  Tiered (combinator) · DuoStore (pluggable     │
                │  meta/data engines)                            │
                └────────────────────────────────────────────────┘
                          ▲ cross-cutting: Executor (coroutine scheduling) /
                            ThreadPool / Config / Logging / Metrics
```

## Key trade-offs at a glance

- **C++20 coroutines as first-class citizens**: handlers and storage interfaces
  all return `Task<T>`; synchronous HTTP libraries bridge via `sync_wait`,
  asynchronous ones integrate through their io_context — business code is
  written exactly once.
- **Neutral HTTP model + streaming bodies**: request/response bodies never
  materialize as full in-memory buffers; they travel through pull/push
  `BodyReader`/`BodyWriter` interfaces, supporting large-object transfers and
  SigV4 chunked signature verification.
- **Bucket-level routing rather than object-level**: routing rules stay simple
  and statically configurable, avoiding a metadata service; object-level
  tiering is layered on top as a combinator backend in the same spirit (see
  [tiered-design.md](architecture/storage/tiered-design.md)).
- **Metadata sidecar instead of embedding into data files**: the LocalFs
  backend keeps Content-Type, ETag and custom metadata in a sidecar file,
  leaving data files compatible with ordinary filesystem tools.
