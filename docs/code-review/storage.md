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

> **2026-08-02 更新**：本文件所列问题已全部修复，各条目标注 ✅已修复。核心收敛：
> localfs 系的元数据改为**随数据文件一同原子提交**（写进数据文件的 xattr，一次
> rename 同时提交数据与元数据，sidecar 保留作外部工具/存量回落），叠加提交段
> per-key 锁与全链路 fsync；duostore 的 `swap_extents` 改按**差集**操作 refs
> （四引擎同步），并新增"结果不明"可区分异常类型阻断误删；cloudproxy 入方向
> reader 阻塞前切池线程、响应头等待加超时上界；后端 `close()` 由 `main` 在
> `pool->join()` 之前逐个调用（README §1.4 一并修复）。
> 单测新增 5 组用例（其中 localfs 撕裂与 duostore 混合对象压实两组已验证：
> 回退修复即失败）；主构建 255 / TSan 240 / sqlite 11 套件 / redis 11 套件全通过，
> 无竞争告警。README §1.1（vhost bucket 校验）不在本轮范围。

## localfs / xlocalfs / tiered（通用部分）

### [高 · ✅已修复] LocalFs 并发 PUT 同 key → 数据文件与 sidecar 撕裂（body 与 etag 不一致）
`localfs_backend.cc:117-158`（`put_object` 在 `co_await pool_->schedule()` 后无 per-key 锁）+ `fs_util.cc:95-113`（`commit_object_file` 两步提交：先 `write_sidecar` 再 `fs::rename(data)`，这一对不原子）。两个并发 PUT 交错可得 `write_sidecar(A)→write_sidecar(B)→rename data(B)→rename data(A)` → 数据=A、sidecar(etag)=B → GET 的 body 与响应头 ETag 不一致。MemoryBackend 锁内一次性提交无此问题，恰反证 localfs 是缺陷方；tiered 前门写直接委托 local 同样中招（tiered 的 key_lock 只保护 tier 状态迁移）。
**✅已修复**（两层）：(1) 元数据写进数据文件的扩展属性 `user.lights3.meta`（内容与 sidecar 同款 TSV），**一次 rename 同时提交数据与元数据**——xattr 随 inode 走，body 与 etag 结构上不可能错位，读路径 xattr 优先、缺失回落 sidecar（存量对象与不支持 xattr 的文件系统）；(2) `LocalFsBackend::commit_lock` 提交段 striped per-key 锁（64 条带，PUT/complete 共用，xlocalfs 继承），把 sidecar 也串行化到最终写者。单测 `localfs_concurrent_put_same_key_not_torn`（16 写者 × 40 轮，校验 body/etag/size 与 sidecar 四者一致）——去掉锁即失败。

### [中 · ✅已修复] GET 用 fd 旧 inode 的 body、却用 path 二次 stat 的新 size/etag
`localfs_backend.cc:160-204` + `fs_util.cc:117-149`（`load_object_meta` 对**路径**再 stat）；xlocalfs 同构。GET 先 `open(path)` 得 fd（并 fstat 拿到 `st`），随后 `load_object_meta(path)` 对路径再取 size/etag。并发覆盖写后：body=旧内容、meta.size/etag=新对象 → 新 size>旧则 pread 提前 EOF 短包、新 size<旧则 body 被截断为新长度仍是旧内容（静默损坏）。fd 已 fstat 出 size 却没用。
**✅已修复**：新增 `load_object_meta_stat(path, key, st, ...)` 复用调用方已持有的 fstat 结果，localfs/xlocalfs 的 GET 均改走它；元数据本身也来自该 inode 的 xattr，全套 meta 与 body 同源。单测 `localfs_get_meta_matches_open_inode`（持 fd 期间覆盖为更短对象，校验 size/etag/body 仍是旧值且自洽）。

### [中 · ✅已修复] 全链路无 fsync：已 200 应答的写掉电后可能丢失
localfs put/complete、`fs_util.cc` write_tsv/commit、xlocalfs 写路径均无 `fsync`/`fdatasync`，父目录也不 fsync。`uring.h` 定义了 `fsync()` awaitable 但 xlocalfs 从不调用（grep 0 处）。掉电可丢已确认写，或"rename 已提交但目录项未落盘"。
**✅已修复**：`fs_util.cc` 增加 `fsync_file/fsync_path/fsync_dir`，数据 tmp 与 sidecar tmp 在 rename **之前** fdatasync、rename **之后** fsync 父目录（`write_tsv` 与 `commit_object_file` 两条路径都覆盖，xlocalfs 共用同一提交原语）。开关 `LIGHTS3_FSYNC=0` 供吞吐优先的部署与测试夹具关闭，默认开。

### [中 · ✅已修复] commit_object_file 单写者崩溃窗口：覆盖写只落 sidecar，数据仍旧
`fs_util.cc:95-113`：即便无并发，覆盖已有 key 时先 `write_sidecar`（新 etag/size）再 `rename` 数据，两步间崩溃 → sidecar 新、数据旧 → 重启后 etag 与 body 不符。
**✅已修复**：即上面第一条的 (1)——元数据进 xattr 后，"提交数据"与"提交元数据"合并为同一次 rename，该窗口在支持 xattr 的文件系统上**不存在**；提交顺序另改为先数据后 sidecar（与 `commit_cached` 一致），使不支持 xattr 时的残留窗口是危害较小的"新数据 + 旧 sidecar"。单测 `localfs_meta_committed_with_data`（删掉 sidecar 模拟崩溃窗口，GET 的 etag 仍正确）。

### [中 · ✅已修复] 进程关停不调用后端 close()，析构晚于 pool->join()
详见 [README.md](README.md) §1.4。tiered 定时器未先 cancel、xlocalfs reaper 的 `pool_->post` 命中 join 后的池抛异常逃出 reaper 线程 → terminate（有在途流量时）。
**✅已修复**：`main.cc` 在 `pool->join()` **之前**遍历全部后端 `sync_wait(backend->close())`（单个失败只记 ERROR 不阻断其余）。各后端的 `close()` 实现本就正确（duostore 封存 active pack + 关 meta、tiered 撤定时器 + 存 atime 快照、xlocalfs 关 uring），此前只是从未被调用；析构兜底保持不变作为二道防线。

### 低（简列）

| 位置 | 问题 | 修复 |
| --- | --- | --- |
| `tiered_backend.cc:80-149` | TeeCacheReader 直到 EOF/析构才释放 inflight，慢客户端长时间挡住该 key 的下沉/GC/对账（无超时上界） | 文档化保留：inflight 只让 GC/下沉**顺延**（保守方向，无数据风险），且回填 reader 随响应生命期自然结束；加超时会引入"缓存写到一半被抢"的新窗口，收益不抵复杂度 |
| `xlocalfs/uring.cc:131-143` | `push_and_enter` 在反复 EAGAIN/EBUSY 或"消费 0"时持 `submit_mu_` 无退避自旋，内核内存压力下拖住所有 io | ✅已修复：前 8 轮 `yield`，其后指数退避睡至 1ms 封顶，不再持锁空转 |
| `localfs_backend.cc:214-233` | delete_object 先删 data 再删 sidecar 非原子，之间崩溃遗留孤儿 sidecar（对 list 不可见地占空间） | ✅已修复：`list_objects` 遍历时顺手回收"数据文件已不存在"的 sidecar（全树遍历本就在那里，best-effort 自愈）。单测 `localfs_orphan_sidecar_reaped_by_list` |
| `registry.cc:46-71` | build 中途失败不回滚已建 ThreadPool 与其 gauge 回调（孤儿 pool 存活、回调读陈旧 stats） | ✅已修复：`BuildRollback` 守卫在异常路径清空已建后端（析构即 join 池）并经新增的 `MetricsRegistry::remove_labeled("backend", name)` 撤掉本次注册的全部序列 |
| `tiered_backend.cc:1002` | `save_atime_snapshot` 静默丢弃含 TAB/换行的 key（防 TSV 损坏，但这些对象跨重启只能 mtime 兜底判冷） | 无需修改：对象键的控制字符已在共享校验层拒绝（见 [s3.md](s3.md) 对应条目），TAB/换行不可能进入 key，该分支成为纯防御 |

**核对无恙**：listing 分页/continuation-token、delimiter 分组、multipart 状态机、upload_id 路径逃逸防护、validate 的 key 各后端入口覆盖、registry 两阶段构建与环检测、空间水位回收只经"上传成功+锁内复核"的 stub 化（无误删数据）。

---

## duostore / cloudproxy

### [高 · ✅已修复] swap_extents 先加后删 refs：压实迁移抹掉混合对象中未迁移 chunk 的引用，孤儿扫描误删活数据（四引擎均中招）
`rocks_meta_store.cc:642-662`（`batch_refs(to,add=true)` 在前、`batch_refs(from,add=false)` 在后）、`redis_meta_store.cc:1098`（Lua hset→hdel）、`sqlite_meta_store.cc:1057`（REPLACE→DELETE）、`tikv` Committer"后者胜"；调用方 `duostore_backend.cc` `migrate_pack_record` 以**整个旧 DataRef** 作 `from`、**整个新 DataRef** 作 `to`。

**机制**：压实只替换一个 pack extent，`to`/`from` 共享全部未迁移的 chunk extent。四引擎的 swap_extents 都先对 `to` 逐 extent Put refs、再对 `from` 逐 extent Delete refs——同一 file_id"Put 后 Delete"在 WriteBatch/Lua 顺序/SQL 顺序/TiKV"后者胜"下**净效果全是删除** → 仍被对象引用的 chunk 失去 refs 表项 → 孤儿扫描 unlink 活数据 → GET 500 永久丢失。pack 存活账是加法语义不受影响，唯 refs 是 last-wins 中招。**触发**（可完整构造，无需竞态）：混合大小 MPU（part1=6MiB 走 chunk、part2=100KiB 走 pack）complete → 删同 pack 其他对象使存活率低触发 `rewrite_pack` → `swap_extents(from=[C,P], to=[C,P'])` 删掉 refs[C] → 下轮孤儿扫描 unlink C。
**✅已修复**：新增共享 helper `meta_util.h` 的 `refs_delta(from, to)`，只对真正新增（`to−from`）与真正消失（`from−to`）的 file_id 操作 refs——顺序随之无关，同时省掉无谓 mutation（TiKV 侧还少一批 prewrite）；四引擎（rocks/redis/sqlite/tikv）同步改用。pack 存活账保持整表 ±1（加法语义，同 pack 的 +1/−1 自然抵消）。单测 `duostore_compact_mixed_object_keeps_chunk_refs`：混合对象压实后校验 `chunk_referenced` 仍为真、孤儿扫描零删除、GET 逐字节正确——回退任一引擎的修复即失败。

### [高 · ✅已修复] cloudproxy GET 的 QueueBodyReader 在事件循环线程上同步阻塞 pop
`http/pushpull.h:44-63`（`BlockQueue::pop` cv 阻塞）+ `QueueBodyReader::read`（无线程切换直接 pop）；`cloudproxy_backend.cc:160-178`（`PumpBodyReader::read` 直接转发）；消费点 `beast_server.cc:412` / `seastar_server.cc:436`（读协程在 io 线程/reactor shard 恢复）。项目内其余流式 reader 都遵守"阻塞段先 `co_await pool_->schedule()`"约定，`QueueBodyReader` 是唯一违约者——在调用方线程同步等 pump 从远端推来下一块。出方向（stream_upload）作者已显式规避，入方向漏掉。
**✅已修复**：`PumpBodyReader::read` 在转发前 `co_await pool_->schedule()`（backend 的池，与出方向一致），阻塞恒发生在池线程；`PumpBodyReader` 随之持一份池的 shared_ptr。

### [中 · ✅已修复] 网络型 meta"提交结果不明"被当失败，commit_or_discard 删掉可能已被引用的数据
`duostore_backend.cc:759-774`（meta 提交抛异常即 `data_->remove` 兜底删数据）；redis `redis_meta_store.cc:477`（EVALSHA 后断连=结果不明）、tikv `TikvUndetermined`。对本地引擎"抛异常≈未提交"成立，但 redis/tikv 存在"实已提交但抛异常"路径 → DuoStoreBackend 一律按未提交物理删数据 → 提交实已生效时产生指向已删数据的坏对象（GET 500，反向对账只告警不修）。
**✅已修复**：`meta_store.h` 新增可区分异常 `UndeterminedCommit`（仍是 InternalError/500，对客户端语义不变）；redis 的提交类 IO 失败（`read_retry=false` 分支）与 tikv 的 `TikvUndetermined` 改抛该类型；`commit_or_discard` 遇之**不删数据**，留给孤儿扫描按"refs 无引用 + 逾宽限"自然收敛。

### [中 · ✅已修复] cloudproxy get_object 等待响应头无超时
`cloudproxy_backend.cc:449-454`（裸 `fut.get()`），对照 docs §3.1"超时 = request_timeout"。滴流远端可让单次 Get 无限期不完成；头部等待发生在共享池线程，并发 GET ≈ 池大小时池被占满长时间全局停摆。
**✅已修复**：改 `fut.wait_for(request_timeout × (retry_max+1))`（覆盖整条重试链的预算），超时即 `abort()` 打断在途 socket、cancel 队列、join pump，抛 `SlowDown` 并记 transport 错误计数。

### 低（简列）

| 位置 | 问题 | 修复 |
| --- | --- | --- |
| `duostore_backend.cc:965` | `run_gc_once` 步骤 1 的 `list_uploads` 不在 try 内，与并发 DeleteBucket 竞态抛 NoSuchBucket 逃逸 → 本轮步骤 2-4 全跳过（回收顺延一个 gc_interval，会重臂） | ✅已修复：`list_buckets`/`list_uploads` 各自包 try——单桶失败只跳过该桶，整段失败也不再吞掉后续的 gcq 消费/压实/孤儿扫描 |
| `cloudproxy_backend.cc:802` | complete_multipart 的"200 + body Error=InternalError"不重试，变成对客户端 500（客户端重试走 NoSuchUpload 歧义消解，语义最终正确但多一轮往返） | ✅已修复：200 响应体内的 Error 若映射为 InternalError/SlowDown，按可重试处置（与同名 HTTP 状态一致）；重试后的 NoSuchUpload 仍走既有歧义消解 |
| `duostore_backend.h:37` | PinTable 以裸 file_id 为键、chunk 与 pack 用独立计数器数值必然重叠 → GC 碰撞时假阳性 pin，仅保守顺延回收（无假阴性/误删） | ✅已修复：pin 键改为 (是否 pack, file_id) 双表，`pin()` 返回句柄列表；`pinned()` 拆为 `pinned_chunk()` / `pinned_pack()`，两处调用点各就各位 |

**核对无恙**：号段预留唯一性（四引擎恒 fsync/raft/独立 FULL 连接，崩溃只浪费号段）、gcq"先物理删后销账"铁律、redis Lua guarded-commit 原子性与 sha1 指纹防 write-skew、sqlite 单写连接+事务残留回滚、tikv 守卫锁物化写偏斜、fs pack append-only/fdatasync/torn-tail 处置、rados AioPending 双引用生命期与弃单缓冲所有权、cloudproxy 连接池取还/坏连接不回池/PUT 仅连接阶段重试、complete 的 NoSuchUpload 歧义消解。
