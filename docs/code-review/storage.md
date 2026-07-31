# storage（L3）评审明细

> 覆盖 `src/storage/`：通用（backend / bucket_router / registry / validate / listing /
> multipart）、localfs / xlocalfs / memory / tiered、以及 duostore / cloudproxy 两个
> 复杂后端。跨模块问题（vhost bucket 不校验、后端 close 不被调用）见
> [README.md](README.md) §1.1 / §1.4。

**模块级评价**：通用层与 tiered 组合层的并发推理反而比其所依赖的 localfs 叶子更严密
（tier 状态机、per-key 锁、GC 指数退避、双向对账、崩溃恢复矩阵都做了细致推演，注释
坦诚标注已知窗口）。duostore/cloudproxy 工程质量很高（记账与业务事务同批、四引擎语义
对齐好）。缺陷集中在：**localfs 数据面缺 per-key 串行化**（撕裂）、**全链路无 fsync**、
**duostore 压实换 ref 的顺序缺陷（数据丢失）**、**cloudproxy GET 阻塞事件循环线程**。

## localfs / xlocalfs / tiered（通用部分）

### [高 · ✅已复核] LocalFs 并发 PUT 同 key → 数据文件与 sidecar 撕裂（body 与 etag 不一致）
`localfs_backend.cc:117-158`（`put_object` 在 `co_await pool_->schedule()` 后无 per-key 锁）+ `fs_util.cc:95-113`（`commit_object_file` 两步提交：先 `write_sidecar` 再 `fs::rename(data)`，这一对不原子）。两个并发 PUT 交错可得 `write_sidecar(A)→write_sidecar(B)→rename data(B)→rename data(A)` → 数据=A、sidecar(etag)=B → GET 的 body 与响应头 ETag 不一致。MemoryBackend 锁内一次性提交无此问题，恰反证 localfs 是缺陷方；tiered 前门写直接委托 local 同样中招（tiered 的 key_lock 只保护 tier 状态迁移）。**建议**：commit 前对 (bucket,key) 取 per-key 锁（复用 tiered 的 striped `AsyncSemaphore` 下沉到 localfs），或单文件原子提交（sidecar 内联进数据文件头）。

### [中 · confirmed] GET 用 fd 旧 inode 的 body、却用 path 二次 stat 的新 size/etag
`localfs_backend.cc:160-204` + `fs_util.cc:117-149`（`load_object_meta` 对**路径**再 stat）；xlocalfs 同构。GET 先 `open(path)` 得 fd（并 fstat 拿到 `st`），随后 `load_object_meta(path)` 对路径再取 size/etag。并发覆盖写后：body=旧内容、meta.size/etag=新对象 → 新 size>旧则 pread 提前 EOF 短包、新 size<旧则 body 被截断为新长度仍是旧内容（静默损坏）。fd 已 fstat 出 size 却没用。**建议**：local tier 直接用 fd 的 fstat size/mtime，只从 sidecar 取 etag/content_type/user_meta。

### [中 · confirmed] 全链路无 fsync：已 200 应答的写掉电后可能丢失
localfs put/complete、`fs_util.cc` write_tsv/commit、xlocalfs 写路径均无 `fsync`/`fdatasync`，父目录也不 fsync。`uring.h` 定义了 `fsync()` awaitable 但 xlocalfs 从不调用（grep 0 处）。掉电可丢已确认写，或"rename 已提交但目录项未落盘"。**建议**：rename 前 fsync 数据 tmp、rename 后 fsync 父目录（sidecar 同理）；xlocalfs 用现成 `uring_->fsync(fd)`；配置项在吞吐/持久性间取舍。

### [中 · confirmed] commit_object_file 单写者崩溃窗口：覆盖写只落 sidecar，数据仍旧
`fs_util.cc:95-113`：即便无并发，覆盖已有 key 时先 `write_sidecar`（新 etag/size）再 `rename` 数据，两步间崩溃 → sidecar 新、数据旧 → 重启后 etag 与 body 不符。**建议**：单文件原子提交；或先 rename 数据后写 sidecar（对称窗口是"新数据+旧 sidecar"，危害更小）。

### [中 · plausible] 进程关停不调用后端 close()，析构晚于 pool->join()
详见 [README.md](README.md) §1.4。tiered 定时器未先 cancel、xlocalfs reaper 的 `pool_->post` 命中 join 后的池抛异常逃出 reaper 线程 → terminate（有在途流量时）。

### 低（简列）

| 位置 | 问题 |
| --- | --- |
| `tiered_backend.cc:80-149` | TeeCacheReader 直到 EOF/析构才释放 inflight，慢客户端长时间挡住该 key 的下沉/GC/对账（无超时上界） |
| `xlocalfs/uring.cc:131-143` | `push_and_enter` 在反复 EAGAIN/EBUSY 或"消费 0"时持 `submit_mu_` 无退避自旋，内核内存压力下拖住所有 io |
| `localfs_backend.cc:214-233` | delete_object 先删 data 再删 sidecar 非原子，之间崩溃遗留孤儿 sidecar（对 list 不可见地占空间） |
| `registry.cc:46-71` | build 中途失败不回滚已建 ThreadPool 与其 gauge 回调（孤儿 pool 存活、回调读陈旧 stats） |
| `tiered_backend.cc:1002` | `save_atime_snapshot` 静默丢弃含 TAB/换行的 key（防 TSV 损坏，但这些对象跨重启只能 mtime 兜底判冷） |

**核对无恙**：listing 分页/continuation-token、delimiter 分组、multipart 状态机、upload_id 路径逃逸防护、validate 的 key 各后端入口覆盖、registry 两阶段构建与环检测、空间水位回收只经"上传成功+锁内复核"的 stub 化（无误删数据）。

---

## duostore / cloudproxy

### [高 · ✅已复核] swap_extents 先加后删 refs：压实迁移抹掉混合对象中未迁移 chunk 的引用，孤儿扫描误删活数据（四引擎均中招）
`rocks_meta_store.cc:642-662`（`batch_refs(to,add=true)` 在前、`batch_refs(from,add=false)` 在后）、`redis_meta_store.cc:1098`（Lua hset→hdel）、`sqlite_meta_store.cc:1057`（REPLACE→DELETE）、`tikv` Committer"后者胜"；调用方 `duostore_backend.cc` `migrate_pack_record` 以**整个旧 DataRef** 作 `from`、**整个新 DataRef** 作 `to`。

**机制**：压实只替换一个 pack extent，`to`/`from` 共享全部未迁移的 chunk extent。四引擎的 swap_extents 都先对 `to` 逐 extent Put refs、再对 `from` 逐 extent Delete refs——同一 file_id"Put 后 Delete"在 WriteBatch/Lua 顺序/SQL 顺序/TiKV"后者胜"下**净效果全是删除** → 仍被对象引用的 chunk 失去 refs 表项 → 孤儿扫描 unlink 活数据 → GET 500 永久丢失。pack 存活账是加法语义不受影响，唯 refs 是 last-wins 中招。**触发**（可完整构造，无需竞态）：混合大小 MPU（part1=6MiB 走 chunk、part2=100KiB 走 pack）complete → 删同 pack 其他对象使存活率低触发 `rewrite_pack` → `swap_extents(from=[C,P], to=[C,P'])` 删掉 refs[C] → 下轮孤儿扫描 unlink C。**建议**：调换顺序（先删 from 再加 to），或按差集操作（`to−from` 加、`from−to` 删，同时省 TiKV 无谓 mutation）；四引擎同步改 + 补"混合对象压实后 chunk_referenced 仍 true"专项测试。

### [高 · ✅已复核] cloudproxy GET 的 QueueBodyReader 在事件循环线程上同步阻塞 pop
`http/pushpull.h:44-63`（`BlockQueue::pop` cv 阻塞）+ `QueueBodyReader::read`（无线程切换直接 pop）；`cloudproxy_backend.cc:160-178`（`PumpBodyReader::read` 直接转发）；消费点 `beast_server.cc:412` / `seastar_server.cc:436`（读协程在 io 线程/reactor shard 恢复）。项目内其余流式 reader 都遵守"阻塞段先 `co_await pool_->schedule()`"约定，`QueueBodyReader` 是唯一违约者——在调用方线程同步等 pump 从远端推来下一块。出方向（stream_upload）作者已显式规避，入方向漏掉。**触发**：远端慢于客户端（cloudproxy 常态）时队列常空，beast io 线程/seastar reactor 在整个 GET 期间大部分时间被阻塞；并发 GET ≈ io 线程数时该驱动全服时延崩塌。**建议**：`QueueBodyReader::read`（或 `PumpBodyReader::read`）pop 前 `co_await pool->schedule()`；或换成停车/投递式异步队列（对齐 rados AioAwait）。

### [中 · plausible] 网络型 meta"提交结果不明"被当失败，commit_or_discard 删掉可能已被引用的数据
`duostore_backend.cc:759-774`（meta 提交抛异常即 `data_->remove` 兜底删数据）；redis `redis_meta_store.cc:477`（EVALSHA 后断连=结果不明）、tikv `TikvUndetermined`。对本地引擎"抛异常≈未提交"成立，但 redis/tikv 存在"实已提交但抛异常"路径 → DuoStoreBackend 一律按未提交物理删数据 → 提交实已生效时产生指向已删数据的坏对象（GET 500，反向对账只告警不修）。**建议**：为"结果不明"引入可区分异常类型，commit_or_discard 遇之不删数据，留给孤儿扫描收敛。

### [中 · confirmed/影响 plausible] cloudproxy get_object 等待响应头无超时
`cloudproxy_backend.cc:449-454`（裸 `fut.get()`），对照 docs §3.1"超时 = request_timeout"。滴流远端可让单次 Get 无限期不完成；头部等待发生在共享池线程，并发 GET ≈ 池大小时池被占满长时间全局停摆。**建议**：`fut.wait_for(request_timeout)` 超时后 abort + 抛 InternalError/SlowDown。

### 低（简列）

| 位置 | 问题 |
| --- | --- |
| `duostore_backend.cc:965` | `run_gc_once` 步骤 1 的 `list_uploads` 不在 try 内，与并发 DeleteBucket 竞态抛 NoSuchBucket 逃逸 → 本轮步骤 2-4 全跳过（回收顺延一个 gc_interval，会重臂） |
| `cloudproxy_backend.cc:802` | complete_multipart 的"200 + body Error=InternalError"不重试，变成对客户端 500（客户端重试走 NoSuchUpload 歧义消解，语义最终正确但多一轮往返） |
| `duostore_backend.h:37` | PinTable 以裸 file_id 为键、chunk 与 pack 用独立计数器数值必然重叠 → GC 碰撞时假阳性 pin，仅保守顺延回收（无假阴性/误删） |

**核对无恙**：号段预留唯一性（四引擎恒 fsync/raft/独立 FULL 连接，崩溃只浪费号段）、gcq"先物理删后销账"铁律、redis Lua guarded-commit 原子性与 sha1 指纹防 write-skew、sqlite 单写连接+事务残留回滚、tikv 守卫锁物化写偏斜、fs pack append-only/fdatasync/torn-tail 处置、rados AioPending 双引用生命期与弃单缓冲所有权、cloudproxy 连接池取还/坏连接不回池/PUT 仅连接阶段重试、complete 的 NoSuchUpload 歧义消解。
