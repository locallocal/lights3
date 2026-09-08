# Storage layer documents

English translations of the storage **design documents** (goals / non-goals,
route selection, data model, consistency arguments, configuration, test
strategy); the Chinese originals live in [../../storage/](../../storage/README.md),
which also holds the 13 **implementation-level** documents (data structures,
on-disk / key-space layout, step-by-step read/write paths, concurrency and crash
consistency) -- those exist in Chinese only. Section numbering matches the
Chinese originals one to one.

| Document | Chinese original / implementation document | Contents |
| --- | --- | --- |
| [storage-backend.md](storage-backend.md) | [storage-backend.md](../../storage/storage-backend.md) / [common.md](../../storage/common.md), [localfs.md](../../storage/localfs.md), [xlocalfs.md](../../storage/xlocalfs.md) | `IStorageBackend` abstraction and error contract, BucketRouter, LocalFs layout and atomic-commit decisions, CloudProxy / DuoStore overview, the new-backend guide |
| [tiered-design.md](tiered-design.md) | [tiered-design.md](../../storage/tiered-design.md) / [tiered.md](../../storage/tiered.md) | Tiered storage: object state model, stub metadata, demotion and transparent read-back, failure matrix and reconciliation |
| [cloudproxy-design.md](cloudproxy-design.md) | [cloudproxy-design.md](../../storage/cloudproxy-design.md) / [cloudproxy.md](../../storage/cloudproxy.md) | CloudProxy: self-signed SigV4 + httplib, bidirectional streaming pumps with backpressure, error mapping and retries, end-to-end ETag check |
| [duostore-design.md](duostore-design.md) | [duostore-design.md](../../storage/duostore-design.md) / [duostore-core.md](../../storage/duostore-core.md), [duostore-data-fs.md](../../storage/duostore-data-fs.md), [duostore-meta-rocksdb.md](../../storage/duostore-meta-rocksdb.md) | DuoStore engine: the two SPIs and DataRef, RocksDB meta model, chunk/pack data layout, write consistency and crash model, GC, pluggable evolution |
| [duostore-meta-redis-design.md](duostore-meta-redis-design.md) | [duostore-meta-redis-design.md](../../storage/duostore-meta-redis-design.md) / [duostore-meta-redis.md](../../storage/duostore-meta-redis.md) | Redis IMetaStore: data model, guarded-commit transactions and invariants, persistence and consistency statement |
| [duostore-meta-sqlite-design.md](duostore-meta-sqlite-design.md) | [duostore-meta-sqlite-design.md](../../storage/duostore-meta-sqlite-design.md) / [duostore-meta-sqlite.md](../../storage/duostore-meta-sqlite.md) | SQLite IMetaStore: schema, transactions and concurrency, single-file deployment and backup |
| [duostore-meta-tikv-design.md](duostore-meta-tikv-design.md) | [duostore-meta-tikv-design.md](../../storage/duostore-meta-tikv-design.md) / [duostore-meta-tikv.md](../../storage/duostore-meta-tikv.md) | TiKV IMetaStore: integration route survey, the 2PC sidecar prerequisite, data model and transaction invariants |
| [duostore-data-rados-design.md](duostore-data-rados-design.md) | [duostore-data-rados-design.md](../../storage/duostore-data-rados-design.md) / [duostore-data-rados.md](../../storage/duostore-data-rados.md) | RADOS IDataStore: integration route, object mapping, write / read paths, consistency and multi-gateway GC |
| [multi-gateway-multipart-design.md](multi-gateway-multipart-design.md) | [multi-gateway-multipart-design.md](../../storage/multi-gateway-multipart-design.md) / — (design, pending implementation) | Multipart across gateways on shared storage: per-backend support matrix, premise-by-premise audit of duostore (redis/tikv + rados), the write-side in-flight protection gap (write lease), gap-closing steps and test list |
