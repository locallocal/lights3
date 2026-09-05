# LightS3 — Design Documents for a C++ S3-Protocol Gateway

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

| Document | Contents |
| --- | --- |
| [architecture.md](architecture.md) | Overall architecture, layering, request lifecycle, code layout |
| [config-reload.md](config-reload.md) | Configuration hot reload: SIGHUP / admin API / `s3adm reload`, whole-file validation, the hot-reloadable subset and the "requires restart" report (roadmap §4.4) |
| [tls.md](tls.md) | TLS: HTTPS on all four drivers, certificate hot reload, mTLS / ciphers / minimum version / SNI multi-certificate, reverse-proxy termination examples (roadmap §4.1) |
| [http-adapter.md](http-adapter.md) | Pluggable HTTP layer: neutral request/response model, streaming bodies, adapter notes |
| [concurrency.md](concurrency.md) | Concurrency model: Task coroutines, Executor abstraction, thread pool, unifying sync/async HTTP libraries |
| [coroutine-internals.md](coroutine-internals.md) | Coroutine internals: Task promise layout & symmetric transfer, top-level drivers, when_all/with_timeout, cancellation race protocols and lifetime rules |
| [storage-backend.md](storage-backend.md) | Storage backend abstraction, LocalFs/XLocalFs, DuoStore overview and new-backend guide, bucket routing |
| [s3-protocol.md](s3-protocol.md) | S3 protocol: API scope, SigV4 (incl. presigned & clock skew), Multipart Upload, error mapping, mint compatibility gate |
| [credential-management.md](credential-management.md) | Credential management: AK/SK generate/query/revoke API, three credential sources (static root / file / dynamic), `.sys` persistence; phase 2: at-rest SK encryption, hot-reloaded credentials file, multi-instance sync, per-credential policy |
| [multi-tenancy.md](multi-tenancy.md) | Usage accounting, bucket/tenant quotas, tenant entities and bucket ownership, tiered admin plane, audit log (the whole roadmap §3.9 chain) |
| [testing.md](testing.md) | Testing: the ctest matrix and labels, website/s3adm/fault-injection e2e, fuzz harnesses, the fault-injection facade, the performance gate and soak, mint, ubsan/coverage, the one-shot matrix script (roadmap §6.1) |
| [deployment.md](deployment.md) | Build and distribution: `--version` / embedded git commit, the `cmake --install` tree, CPack deb/rpm, Dockerfile + compose (with the redis/tikv/rados e2e profile), rollback and uninstall for `install.sh` upgrades (roadmap §6.3) |
| [performance-baseline.md](performance-baseline.md) | Performance baseline: the `scripts/bench_matrix.sh` matrix of 4 drivers × TLS × put/get, before/after the §4.3 data-plane work, how to reproduce (roadmap §4.3) |
| [backlog.md](backlog.md) | Open items and future plans: designs kept for later, implemented-but-unverified items, findings from the performance baseline, long-term items, the not-planned list; entries are deleted when done |
| [backlog-sequence.md](backlog-sequence.md) | Implementation order for the ten deferred items of backlog §1: five phases, dependencies, scope / entry points / acceptance / estimate per item, ledger |
| [monitoring.md](monitoring.md) | Monitoring consumers: the Prometheus scrape config and alert/recording rules under `deploy/`, the Grafana dashboard and its generator, the asset-reconciliation test (roadmap §5.5, zero C++) |
| [object-read-write-flow.md](object-read-write-flow.md) | Object read/write flow: the three-layer code path, BodyReader chains, atomic staging commit, fd-snapshot reads |
| [tiered-storage.md](tiered-storage.md) | Tiered storage: cold data sinking to public cloud, stub metadata, transparent read-back and cache refill |
| [cloudproxy-backend.md](cloudproxy-backend.md) | CloudProxy backend: self-signed SigV4 + httplib to a remote S3, bidirectional streaming pumps, error mapping and retries |
| [duostore-backend.md](duostore-backend.md) | DuoStore backend: split metadata/data engine, RocksDB meta + chunk slicing / pack aggregation / GC |
| [duostore-redis-meta.md](duostore-redis-meta.md) | DuoStore's Redis IMetaStore: hiredis + Lua guarded-commit, shared meta across gateways |
| [duostore-sqlite-meta.md](duostore-sqlite-meta.md) | DuoStore's SQLite IMetaStore: embedded amalgamation, WAL + read pool / single write connection |
| [duostore-rados-data.md](duostore-rados-data.md) | DuoStore's RADOS IDataStore: librados, chunk → rados objects |
| [duostore-tikv-meta.md](duostore-tikv-meta.md) | DuoStore's TiKV IMetaStore: client-c + 2PC sidecar, horizontally scalable meta |
| [cli.md](cli.md) | Command-line tools: `lights3` startup, `duostore dump/load/gc/scan`, `tier scan/gc/reconcile`, the offline `fsck` scrub, `s3adm` cred/website/bench/fsck/quota/tenant/usage commands, ccmd option semantics and exit codes |

*The project introduction (build/run/current scope) lives in the repository
root [README.md](../../README.md) (English) and
[../README.zh-CN.md](../README.zh-CN.md) (Chinese).*

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
  [tiered-storage.md](tiered-storage.md)).
- **Metadata sidecar instead of embedding into data files**: the LocalFs
  backend keeps Content-Type, ETag and custom metadata in a sidecar file,
  leaving data files compatible with ordinary filesystem tools.
