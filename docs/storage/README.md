# 存储实现详解

本目录是 `src/storage/` 各存储实现的**实现级**文档：核心数据结构、磁盘/键空间布局、
读写流程逐步拆解、并发与崩溃一致性。设计动机与选型论证见 `docs/` 顶层的对应设计
文档（各篇开头有交叉链接）；接口契约与公共辅助件从 [common.md](common.md) 读起。

## 目录

| 文档 | 覆盖源码 | 内容 |
| --- | --- | --- |
| [common.md](common.md) | `backend.h`、`registry`、`bucket_router`、`validate/listing/multipart` | `IStorageBackend` 逐方法契约、注册构建流程、bucket 路由、公共列举/分片算法 |
| [localfs.md](localfs.md) | `localfs/` | 磁盘布局与 xattr/sidecar 元数据、staging+rename 原子提交、fd 快照读、有序目录游走列举、multipart 落盘 |
| [xlocalfs.md](xlocalfs.md) | `xlocalfs/` | 原生 syscall 的 io_uring 引擎（SQ/CQ mmap、能力探测、SQPOLL）、与 localfs 的复用边界、关闭时序 |
| [memory.md](memory.md) | `memory/` | 全内存结构与快照隔离、容量双闸门、测试角色 |
| [tiered.md](tiered.md) | `tiered/` | stub/缓存双态提交、atime 快照、下沉与透明回读流程、GC 对账 |
| [cloudproxy.md](cloudproxy.md) | `cloudproxy/` | SigV4 自签、连接池、双向流式泵与背压、重试与错误映射单点 |
| [duostore-core.md](duostore-core.md) | `duostore/` 核心 | meta/data 分离编排、codec 字节布局、SPI 契约、读写路径与崩溃窗口矩阵、GC 四步、dump/load |
| [duostore-data-fs.md](duostore-data-fs.md) | `duostore/fs_data_store` | chunk/pack 磁盘格式（LP3R record）、active pack 槽位与封存、损坏分诊 |
| [duostore-data-rados.md](duostore-data-rados.md) | `duostore/rados_data_store` | chunk→RADOS 对象映射、AIO 状态机、双缓冲流水线、flush/close 语义 |
| [duostore-meta-rocksdb.md](duostore-meta-rocksdb.md) | `duostore/rocks_meta_store` | 8 CF 键空间、CounterMerge 计数、WriteBatch guarded-commit、快照列举 |
| [duostore-meta-sqlite.md](duostore-meta-sqlite.md) | `duostore/sqlite_meta_store` | STRICT schema 全 DDL、单写+读池连接模型、Txn RAII、WAL checkpoint 关闭 |
| [duostore-meta-redis.md](duostore-meta-redis.md) | `duostore/redis_meta_store` | 全部 key 结构、Lua guarded-commit 脚本、CAS 循环、多网关原子性 |
| [duostore-meta-tikv.md](duostore-meta-tikv.md) | `duostore/tikv_meta_store` + `tikv_client` | key 编码与守卫分片、client-c 接入、2PC 全流程、safepoint 推进 |

## 阅读路径建议

- 新增一个后端：[common.md](common.md) → [../storage-backend.md](../storage-backend.md)
  的新增后端指南 → 参考 [memory.md](memory.md)（最小实现）或
  [localfs.md](localfs.md)（完整实现）。
- 排查数据损坏/崩溃恢复：对应后端文档的「崩溃窗口」「恢复」章节；DuoStore 另见
  [duostore-core.md](duostore-core.md) 的 orphan scan 与 dump/load。
- 性能调优：各篇的并发章节 + [../concurrency.md](../concurrency.md)、
  [../coroutine-internals.md](../coroutine-internals.md)。
