# 存储层文档

本目录收齐 `src/storage/` 的全部文档，分两层：**设计文档**（目标/非目标、路线选型、
数据模型、一致性论证、配置与测试策略，文件名带 `-design` 后缀，英文翻译在
[../en/storage/](../en/storage/README.md)）与**实现级文档**（核心数据结构、磁盘/键空间
布局、读写流程逐步拆解、并发与崩溃一致性，只有中文）。两层一一对应、互不重复：
设计文档讲"为什么这样做"，实现文档讲"代码怎么落地"；各篇开头有交叉链接。接口契约
与公共辅助件从 [storage-backend.md](storage-backend.md) 与 [common.md](common.md) 读起。

## 设计文档

| 文档 | 对应实现文档 | 内容 |
| --- | --- | --- |
| [storage-backend.md](storage-backend.md) | [common.md](common.md)、[localfs.md](localfs.md)、[xlocalfs.md](xlocalfs.md) | `IStorageBackend` 接口抽象与错误约定、BucketRouter、LocalFs 布局与原子提交的设计决策、CloudProxy / DuoStore 概览、新增后端的步骤 |
| [tiered-design.md](tiered-design.md) | [tiered.md](tiered.md) | 分层存储：对象状态模型、stub 元数据、下沉与透明回读、故障矩阵与对账 |
| [cloudproxy-design.md](cloudproxy-design.md) | [cloudproxy.md](cloudproxy.md) | CloudProxy：自签 SigV4 + httplib 直连、双向流式泵与背压、错误映射与重试、ETag 端到端校验 |
| [duostore-design.md](duostore-design.md) | [duostore-core.md](duostore-core.md)、[duostore-data-fs.md](duostore-data-fs.md)、[duostore-meta-rocksdb.md](duostore-meta-rocksdb.md) | DuoStore 引擎：双接口与 DataRef、RocksDB 元数据模型、chunk/pack 数据布局、写入一致性与崩溃模型、GC、可插拔演进 |
| [duostore-meta-redis-design.md](duostore-meta-redis-design.md) | [duostore-meta-redis.md](duostore-meta-redis.md) | Redis IMetaStore：数据模型、guarded-commit 事务与不变量、持久化与一致性声明 |
| [duostore-meta-sqlite-design.md](duostore-meta-sqlite-design.md) | [duostore-meta-sqlite.md](duostore-meta-sqlite.md) | SQLite IMetaStore：schema、事务与并发、单文件部署与备份 |
| [duostore-meta-tikv-design.md](duostore-meta-tikv-design.md) | [duostore-meta-tikv.md](duostore-meta-tikv.md) | TiKV IMetaStore：接入路线调研、2PC 侧车前置工程、数据模型与事务不变量 |
| [duostore-data-rados-design.md](duostore-data-rados-design.md) | [duostore-data-rados.md](duostore-data-rados.md) | RADOS IDataStore：接入路线、对象映射、写读路径、一致性与多网关 GC |

## 实现文档

| 文档 | 覆盖源码 | 内容 |
| --- | --- | --- |
| [common.md](common.md) | `backend.h`、`registry`、`bucket_router`、`validate/listing/multipart`、`meta_cache.h` | `IStorageBackend` 逐方法契约、注册构建流程、bucket 路由、公共列举/分片算法、后端级元数据缓存组件 |
| [localfs.md](localfs.md) | `localfs/` | 磁盘布局与 xattr/sidecar 元数据、staging+rename 原子提交、fd 快照读、有序目录游走列举、multipart 落盘 |
| [xlocalfs.md](xlocalfs.md) | `xlocalfs/` | 原生 syscall 的 io_uring 引擎（SQ/CQ mmap、能力探测、SQPOLL）、与 localfs 的复用边界、关闭时序 |
| [memory.md](memory.md) | `memory/` | 全内存结构与快照隔离、容量双闸门、测试角色 |
| [tiered.md](tiered.md) | `tiered/` | stub/缓存双态提交、atime 快照、下沉与透明回读流程、GC 对账 |
| [cloudproxy.md](cloudproxy.md) | `cloudproxy/` | SigV4 自签、连接池、双向流式泵与背压、重试与错误映射单点 |
| [duostore-core.md](duostore-core.md) | `duostore/` 核心 | meta/data 分离编排、codec 字节布局、SPI 契约、读写路径与崩溃窗口矩阵、GC 四步、scrub、dump/load |
| [duostore-data-fs.md](duostore-data-fs.md) | `duostore/fs_data_store` | chunk/pack 磁盘格式（LP3R record）、active pack 槽位与封存、损坏分诊 |
| [duostore-data-rados.md](duostore-data-rados.md) | `duostore/rados_data_store` | chunk→RADOS 对象映射、AIO 状态机、双缓冲流水线、flush/close 语义 |
| [duostore-meta-rocksdb.md](duostore-meta-rocksdb.md) | `duostore/rocks_meta_store` | 8 CF 键空间、CounterMerge 计数、WriteBatch guarded-commit、快照列举 |
| [duostore-meta-sqlite.md](duostore-meta-sqlite.md) | `duostore/sqlite_meta_store` | STRICT schema 全 DDL、单写+读池连接模型、Txn RAII、WAL checkpoint 关闭 |
| [duostore-meta-redis.md](duostore-meta-redis.md) | `duostore/redis_meta_store` | 全部 key 结构、Lua guarded-commit 脚本、CAS 循环、多网关原子性 |
| [duostore-meta-tikv.md](duostore-meta-tikv.md) | `duostore/tikv_meta_store` + `tikv_client` | key 编码与守卫分片、client-c 接入、2PC 全流程、safepoint 推进 |

## 阅读路径建议

- 新增一个后端：[common.md](common.md) → [storage-backend.md](storage-backend.md) §6
  的新增后端指南 → 参考 [memory.md](memory.md)（最小实现）或
  [localfs.md](localfs.md)（完整实现）。
- 理解一个后端的取舍再看代码：先读它的 `*-design.md`，再读对应实现文档。
- 排查数据损坏/崩溃恢复：对应后端文档的「崩溃窗口」「恢复」章节；DuoStore 另见
  [duostore-core.md](duostore-core.md) 的 orphan scan 与 dump/load。
- 性能调优：各篇的并发章节 + [../concurrency.md](../concurrency.md)、
  [../coroutine-internals.md](../coroutine-internals.md)。
