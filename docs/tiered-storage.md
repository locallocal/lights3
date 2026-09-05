# 分层存储：冷数据下沉公有云

> 状态：P1–P5 全部实现（`src/storage/tiered/`，P4 剩余项 2026-07-31 收尾：
> §9 对账工具 + GC 失败条目指数退避），cloud 侧经 `IStorageBackend`
> 抽象接入，CI 用 MemoryBackend 充当云端全覆盖（单测 `test_tiered.cc` +
> `e2e_tiered`）；P5 的真实 CloudProxyBackend 见
> [cloudproxy-backend.md](cloudproxy-backend.md)，组合场景由
> `e2e_tiered_cloudproxy` 验收。P6（2026-09-02，roadmap §3.6）：local 侧抽象为
> `ITierLocal`（localfs/xlocalfs 与 duostore 两个适配器）、访问记录落对象
> xattr + 时间轮增量扫描、prefix 策略、多维淘汰评分、对账隔离区、Range
> 块缓存。

## 1. 目标与非目标

目标（对应需求）：

1. **冷下沉**：bucket 数据长时间不访问，或本地空间不足时，把对象数据上传到
   公有云，本地仅保留元数据（stub）；
2. **透明回读**：访问已下沉对象时从云端取回并返回客户端，同时缓存回本地；
3. **空间兜底**：本地拿不到空间时不缓存，仅透传，读路径永不因缓存失败而失败。

非目标（首期）：

- 不做 object 级多副本/纠删，云端始终恰好一份；
- 不做透明压缩/加密（依赖云侧 SSE 即可）；
- 进行中的 multipart 分片不参与下沉（完成后成为普通对象再进入生命周期）。

首期的"不做 prefix 粒度策略"已在 P6 撤销：`rules[]` 按 `bucket/key` glob 给
出 per-prefix `cold_after`（`never` = 钉住），见 §5.1/§8。

## 2. 架构定位：组合后端 TieredBackend

docs/storage-backend.md §2 有意把路由停在 bucket 粒度，并预留"object 级分层用叠加实现、
不改 `IStorageBackend` 接口"。本设计兑现该预留：新增组合后端
`type: tiered`（`src/storage/tiered/`），对 L2 仍是一个普通 `IStorageBackend`，
内部组合两个既有后端：

```text
                        BucketRouter
                             │
                     ┌───────▼────────┐
                     │  TieredBackend  │  type: tiered
                     │  ┌───────────┐  │
     状态机/策略/回迁 │  │ TierIndex │  │  atime 表 + 下沉/回迁/GC 任务
                     │  └───────────┘  │
                     └──┬──────────┬───┘
              local（热层）│          │ cloud（冷层）
        ┌───────────────▼──┐    ┌──▼──────────────────┐
        │ ITierLocal        │    │ 任意 IStorageBackend │
        │  ├ LocalFsTierLocal│    │ （首期 CloudProxy；  │
        │  │ (localfs/xlocalfs)│  │  单测用 Memory）     │
        │  └ DuoStoreTierLocal│   └─────────────────────┘
        └───────────────────┘
```

两侧的耦合程度刻意不对称：

- **cloud 侧只经 `IStorageBackend` 抽象**——上传/下载/删除全是标准
  put/get/delete，因此云端可以是 CloudProxy、也可以是另一个 localfs
  （测试）或未来任何后端；
- **local 侧经 `tier::ITierLocal`（`src/storage/tiered/tier_local.h`，P6）**——
  tiered 对热层的全部依赖收敛为一个窄接口：读 tier 状态、读写访问记录、
  取上传快照、两条原子提交原语（stub 化 / 缓存回填）、空间探针、全量枚举、
  可选的块缓存。两个实现：`LocalFsTierLocal`（localfs/xlocalfs，共享
  `fs_util` 磁盘布局，本文 §4 描述的就是它）与 `DuoStoreTierLocal`（tier
  状态进对象记录、stub = 无 extent 的记录、提交 = CAS 元数据事务，见
  [storage/tiered.md §11](storage/tiered.md)）。配置里 `local` 指向
  localfs/xlocalfs 或 duostore 均可，其余类型仍为配置错误。

配置以 name 引用两个既有后端实例；`StorageRegistry::build` 改为两阶段
（先构造全部叶子后端，再构造组合后端），循环引用视为配置错误。

## 3. 对象状态模型

每个对象处于三态之一，记录在 sidecar 的 `tier` 字段（缺省 `local`）：

```text
            scanner 判冷：上传云端 + stub 化
   ┌────────────────────────────────────────────┐
   │                                            ▼
 local ◄──────────── PUT 覆盖 ─────────────── remote
（数据仅本地）                            （本地仅 stub，数据在云）
   ▲                                            │
   │ PUT 覆盖                                    │ GET 回迁（缓存成功）
   │                                            ▼
   └────────────────────────────────────────  cached
                （数据本地+云端各一份；再次判冷时无需上传，校验后直接 stub 化）
```

- `local`：普通对象，与现状完全一致（存量数据零迁移即兼容）；
- `remote`：本地是 **stub** = 0 长度数据文件 + 扩展 sidecar；数据在云端；
- `cached`：数据本地云端各一份，本地份是缓存——空间回收时的**首选牺牲品**
  （直接 stub 化，零上传流量）。

## 4. 磁盘与元数据布局

### 4.1 stub：0 长度文件而非独立索引

stub 保留 0 长度的数据文件占位，而不是删掉数据文件另建索引。理由：

- list（目录遍历）、key/前缀冲突检测、delete 清理空目录等逻辑**全部原样复用**，
  无需任何"合并两处视图"的代码；
- rename 原子提交语义保持：stub 化就是"rename 一个 0 长度文件盖过数据文件"，
  与 PUT 覆盖走同一原语。

代价：`load_meta` 的 size 不能再信 stat。sidecar 增加 `size` 字段，
`tier != local` 时以 sidecar 为准（`local` 对象继续用 stat，兼容存量）。

### 4.2 sidecar 扩展字段（TSV，向后兼容追加）

```text
etag            <本地原始 etag，永不因下沉改变>
content_type    ...
meta.*          ...
tier            remote | cached          # 缺省视为 local
size            <字节数，tier!=local 时生效>
remote.etag     <云端返回的 etag>         # multipart 对象上云后 etag 不同，独立记录
remote.at       <iso8601 上传时间>
```

**对外 ETag 恒等原则**：客户端看到的 ETag 永远是本地原始值（含 multipart 的
`-N` 形式）。对象经单流 PUT 上云后云端 etag 会变，只存在 `remote.etag`
里做校验用，绝不外泄——下沉/回迁对客户端完全透明。

云端对象同时携带 `x-amz-meta-lights3-*` 冗余一份原始 meta（etag/
content_type/user_meta），本地 stub 意外丢失时可对账重建（§9）。

### 4.3 TierIndex：访问记录、时间轮与空间账

- **访问记录**（P6，roadmap §3.6 ⑤）：每个对象一条
  `AccessRec{atime, hits, enrolled}`，**随对象持久化**而非常驻内存——
  localfs 侧写进数据文件的第二个 xattr `user.lights3.access`
  （不 fsync，随 inode 走；PUT 覆盖/stub 化换 inode 后自然消失，回落
  mtime），duostore 侧与无 xattr 的文件系统退回常驻表 +
  `<state>/atime.tsv` 快照（内存随对象数增长，是有意保留的兜底）。
  TieredBackend 只持有一个**写后缓冲**（`ikey → 最新 atime + 命中增量`）：
  GET/HEAD/PUT 命中时更新缓冲，每 5 分钟或缓冲达 `access_buffer_max`
  （默认 10 万键）即刷写（读旧记录 → 合并 → 写回 + 时间轮登记）。判冷读
  记录时缓冲优先、其次持久记录、最后 mtime 兜底，因此刷写窗口内的访问不会
  被误判。崩溃最多丢一个刷写周期，只影响判冷精度；不依赖文件系统 atime
  （relatime 不可靠）。
- **时间轮**（roadmap §3.6 ①）：`<state>/wheel/<slot>` 按小时分槽的追加文件，
  行 = `bucket\tkey`；一个键在其 `atime + cold_after` 落入的槽登记一次
  （`enrolled` 记住已登记槽，再次访问只在槽变化时追加）。到期槽被扫描消费
  后删除，仍热的键改登记到新槽。它是判冷候选与淘汰候选的唯一来源，全量
  遍历只在 `full_scan_interval`（默认 1 天）做一次兜底（未登记对象、崩溃恢
  复、配额校准），见 §5.1。
- **空间账**：`statvfs` 实时读取为准（本地盘可能被其他进程共用），
  可选 `quota_bytes` 叠加逻辑配额（全量扫描校准、PUT/DELETE 增量维护）。

## 5. 下沉流程（demote）

### 5.1 触发

后台 **TierScanner**：`TimerQueue` 周期触发（默认 1h）→ 投递到线程池跑扫描协程。
一轮先刷写访问缓冲，再按模式产出候选（P6，roadmap §3.6 ①）：

- **增量轮（常态）**：只读时间轮里已到期的槽（`slot ≤ now/1h`），逐键复核当前
  访问记录——仍热的重新登记到新槽，已删的丢弃，冷的下沉。成本正比于**活动
  量**而非对象总数；
- **全量轮**：启动后第一轮，以及此后每 `full_scan_interval`（默认 1 天，
  `0` = 每轮全量即旧行为）——`ITierLocal::walk` 枚举全部对象：未登记的对象
  登记进轮、`remote 但本地仍有数据` 的崩溃残留补完 stub、校准配额账本、清理
  失效的 Range 块缓存。

两类触发条件：

1. **判冷**：`now - atime ≥ cold_after(bucket, key)` 的 `local`/`cached` 对象。
   阈值先查 `rules[]`（`bucket/key` glob，首个命中生效，`never` 钉住：既不判冷
   也不参与水位淘汰），无命中用全局 `cold_after`；
2. **空间水位**：`statvfs` 使用率 > `space_high_watermark`（默认 85%）时进入
   回收模式，直到降至 `space_low_watermark`（默认 70%）。候选按时间轮槽升序
   （≈ LRU 顺序）取，只读到"累计候选字节 ≥ 4×缺口"即止；排序键为
   `(rank, score)`——`cached` 与 remote 对象残留的块缓存 rank 0（零上传），
   `local` rank 1；`score = age × (1 + size_weight × log2(1 + size/1MiB)) /
   (1 + frequency_weight × hits)`，两个权重默认 0 即纯 LRU（roadmap §3.6 ③）。

并发由 `core/semaphore.h` 限流（`max_concurrent_transfers`，默认 4），
避免打满上行带宽与线程池。

### 5.2 单对象下沉步骤

```text
per-key 锁（§7）内检查前置：tier==local 且非在途 multipart
① open(data) 得 fd 快照，读 sidecar 记录 etag₀
② cloud.put_object(remote_bucket, key, meta+冗余头, FdBodyReader(fd))
     —— 流式上传，不占额外内存；失败则本对象跳过，退避后下轮重试
③ 校验：云端返回 etag 与本地内容一致（单段=MD5 直接比对；
     multipart 对象改为上传时同步重算 MD5 与 sidecar 无关地校验字节数）
④ per-key 锁内提交：
     a. 复核 sidecar etag == etag₀（期间被 PUT 覆盖则放弃：云副本入 GC 队列）
     b. 写新 sidecar（tier=remote, size, remote.etag, remote.at）   ← tmp+rename
     c. rename(0 长度 tmp → data)                                  ← stub 化提交点
```

关键语义：

- **在途读者不受影响**：步骤 c 是 rename 覆盖，正在 `pread` 的读者持旧 inode
  fd，继续读完完整旧数据（docs/object-read-write-flow.md §3.2 的 fd 快照语义天然保护）；
- **b/c 之间崩溃**：sidecar 已是 remote 但数据文件还是全量——GET 以 sidecar
  `tier` 为准走云端（正确，云副本已存在），数据文件空间等下轮 scanner
  以"tier=remote 但 stat size>0"为特征补做步骤 c 回收；
- **② 之后任何失败**：至多多出一个云端孤儿副本，幂等——下轮重传会覆盖同
  key，或被对账任务清理（§9）。

`cached → remote` 的 stub 化只做步骤 ④（校验 `remote.etag` 仍有效），零流量。

## 6. 读取流程（GET/HEAD 命中 remote）

### 6.1 HEAD / 条件请求

stub 的 sidecar 信息完备（size/etag/meta），HEAD 与 If-* 判定**完全本地完成**，
不触碰云端、不触发回迁。

### 6.2 全量 GET：Tee 透传 + 边下边缓存

```text
① cloud.get_object(remote_bucket, key, range=nullopt) → 云端流
② 空间预检：statvfs 可用 > size + min_free_bytes ？
     否 → 跳过缓存，云端流直接作为 resp.stream_body 透传（需求 3）
③ 是 → 包装 TeeBodyReader 返回给 L2：
     每次 read()：云端流 → 客户端方向透传，同时写 staging tmp + 增量 MD5
     - 写盘失败（ENOSPC 等）：静默降级为纯透传，删 tmp，客户端无感知
     - 客户端断连：协程链回卷，TmpFile RAII 丢弃半截缓存
     EOF 时：MD5 == 本地 etag（单段）/ 字节数 == size（multipart）
       → per-key 锁内提交：先 rename(tmp → data) 再写 sidecar tier=cached
         （与 stub 化次序相反：中间崩溃时 sidecar 仍为 remote，读走云端正确，
          全量数据文件由 scanner 按"remote 但 size>0"回收）
       → 校验不过：丢弃 tmp（云端数据异常，告警计数）
```

Tee 方案下缓存回填**零额外云端流量**（相对"透传一遍、后台再拉一遍"省一半），
且提交仍走 staging+rename，失败模式与 PUT 完全同构。

### 6.3 Range GET

Range 请求默认把 range 直接透传给云端（`IStorageBackend::get_object` 本就带
range），响应按 docs/object-read-write-flow.md §3.1 正常走 206。可配置
`cache_fill_on_range`（默认开）：命中时向后台提交一个 single-flight 的整对象
回迁任务（独立云端 GET → 缓存回填 → 提交为 cached），空间不足同样直接放弃。

**块级部分缓存**（P6，roadmap §3.6 ⑦，`range_cache: true`，仅 localfs 侧支持）：
remote 对象的 Range 命中先查 `<state>/rcache/` 下的块缓存——数据文件是对象
等长的稀疏文件、位图文件记录已有块（按 `remote.etag` 绑定副本）。全部命中
块本地直接服务；否则向云端请求**块对齐的超集** `[af, al]`，`RangeTeeReader`
边收边 `pwrite` 进稀疏文件、只把客户端窗口透传出去，客户端窗口耗尽后在同一次
`read()` 内把尾部（至多一块）吸完，EOF 时合并写位图。写盘失败静默降级透传；
同 key 的并发填充 single-flight。PUT/DELETE/complete 覆盖、整对象回填提交、
全量扫描发现对象已非 remote/副本换代时丢弃缓存；水位回收把块缓存残留当
rank 0 牺牲品（直接删文件）。`range_cache_block` 默认 1MiB。

### 6.4 single-flight

并发 GET 同一 remote 对象：每个请求独立向云端开流透传（互不等待，延迟最优），
但 **缓存写盘者只有一个**——per-key 的 in-flight 表里已有 tee/回迁任务时，
后到请求纯透传。避免同一对象 N 份 staging 并发写盘。

## 7. 写删路径与并发控制

### 7.1 PUT / multipart complete 覆盖

照常走 local 后端的 staging+rename（write-back：新数据只落本地，`tier` 自然
回到 `local`，由 scanner 决定何时再上云）。覆盖 `remote`/`cached` 对象时，
旧云副本成为孤儿 → 提交成功后把 `(remote_bucket, key, remote.etag)` 追加进
**GC 队列**异步删除。

### 7.2 DELETE

本地删除立即执行（幂等，现状语义），若 sidecar 是 `remote`/`cached` 同样把
云副本入 GC 队列。**客户端响应不等云端**：删除延迟不受云端 RTT 影响，
云端失败重试直至成功。

GC 队列持久化：`<staging>/tier/gc/<seq>` 每项一个 TSV 文件（tmp+rename 写入，
删成功后 unlink），崩溃安全；后台任务周期消费 + 指数退避。

### 7.3 per-key 锁

TieredBackend 内维护 striped mutex（按 key 哈希分桶，协程感知的异步锁）。
**只保护状态提交段**（sidecar+rename 的几个元数据操作，微秒级），数据搬运
（上传/下载/tee）全部在锁外流式进行。冲突矩阵：

| 并发方 | 结果 |
| --- | --- |
| PUT vs 下沉提交 | 下沉提交复核 etag 失败 → 放弃 stub，云副本入 GC；PUT 胜 |
| PUT vs 缓存提交 | 缓存提交复核 sidecar 仍为 remote 且 etag 未变，否则丢弃 tmp；PUT 胜 |
| DELETE vs 任一提交 | 提交时 sidecar 已不在 → 丢弃；DELETE 胜 |
| GET(旧数据) vs stub 化 | fd 快照，读完旧 inode，不冲突 |
| GET(open 晚于 rename) vs stub 化 | fd 是 0 长度新 inode 而 sidecar 宣称 size>0 → localfs 抛 `StubRace`，tiered 捕获后重读 tier 改走云端 |
| 两个缓存回填 | single-flight 保证只有一个 |

原则：**用户写操作永远胜过后台任务**；后台任务失败的代价只是流量浪费 +
一个待 GC 的云副本，从不影响正确性。

## 8. 配置

```yaml
backends:
  - name: localdata
    type: localfs
    root: ./data/objects
    staging: ./data/staging
  - name: aws
    type: cloudproxy                  # docs/cloudproxy-backend.md
    endpoint: https://s3.us-east-1.amazonaws.com
    bucket_prefix: lights3-tier-
    # 云端凭证……
  - name: tiered
    type: tiered
    local: localdata                  # 须为 localfs/xlocalfs
    cloud: aws                        # 任意后端
    cold_after: 30d                   # 判冷阈值
    scan_interval: 1h
    space_high_watermark: 85%         # 触发空间回收
    space_low_watermark: 70%          # 回收目标
    min_free_bytes: 1GiB              # 缓存回填所需最小余量（需求 3 的"无法获取空间"判据）
    cache_fill_on_range: true         # Range GET 是否触发后台整对象回迁
    max_concurrent_transfers: 4
    # quota_bytes: 500GiB             # 可选逻辑配额，叠加在 statvfs 之上
    gc_retry_base: 60s                # GC 失败条目退避基数（delay = base × 2^n，§9）
    gc_retry_cap: 1h                  # 退避上限
    reconcile_interval: 1d            # 双向对账周期（§9）；0 = 关（scan_interval=0 时全停）
    reconcile_orphans: rebuild        # 云端孤儿处置：rebuild（默认，重建 stub）| delete
    # ---- P6（roadmap §3.6）----
    full_scan_interval: 1d            # 全量枚举兜底周期；0 = 每轮全量（旧行为）
    evict_size_weight: 0              # 淘汰评分的大小加权；0 = 忽略大小
    evict_frequency_weight: 0         # 淘汰评分的访问频次加权；0 = 纯 LRU
    access_buffer_max: 100000         # 访问记录写后缓冲上限（键数），达到即提前刷写
    range_cache: false                # Range GET 块级部分缓存（localfs 侧）
    range_cache_block: 1MiB           # 块大小 [64KiB, 1GiB]
    rules:                            # prefix 策略：bucket/key glob，首个命中生效
      - match: "archive-*/raw/*"
        cold_after: 7d
      - match: "archive-*/keep/*"
        cold_after: never             # 钉住：不判冷、不淘汰

buckets:
  default_backend: localdata
  rules:
    - match: "archive-*"              # 分层策略即 bucket 路由：想分层的 bucket 指到 tiered
      backend: tiered
```

分层的开关粒度 = bucket 路由粒度；bucket 之下的差异用 `rules[]` 表达
（`local` 为 duostore 时同样生效），跨实例的差异仍可声明多个 tiered 实例。

## 9. 故障矩阵与对账

| 故障 | 行为 |
| --- | --- |
| 云端不可达（GET remote） | 透传云端错误映射（对齐 cloudproxy：远端 5xx → 502/503 S3 错误码）；本地 `local`/`cached` 对象完全不受影响 |
| 云端不可达（scanner/GC） | 本轮跳过；GC 失败条目按指数退避重排（`attempts`/`retry_at` 持久化在条目 TSV，重启不清零；delay = `gc_retry_base` × 2^n 钳制 `gc_retry_cap`），未到点条目零云端访问。判冷积压无副作用 |
| 上传后崩溃、stub 未提交 | 云端孤儿副本，下轮幂等覆盖或对账清理 |
| stub 提交一半崩溃（§5.2 b/c 之间） | sidecar 为准走云端读，数据文件下轮补回收 |
| 缓存回填中断连/ENOSPC | TmpFile 丢弃/降级透传，客户端无感知 |
| 本地 stub 丢失（人为误删） | 对账工具从云端 `x-amz-meta-lights3-*` 冗余头重建 sidecar |

**对账任务**（P4 收尾已实现，`run_reconcile_once` 手动钩子 + 独立低频定时器，
默认每天）：逐 bucket 遍历云端与本地对象集合做双向 diff——

- **正向（云端有、本地无）**：默认从云端 `x-amz-meta-lights3-*` 冗余头重建
  stub（原始 etag/content-type/user meta 全还原）；`reconcile_orphans: delete`
  则删除云副本回收存储费。无冗余头的对象视为外来（远端 bucket 可能与他方
  共用），告警跳过、绝不动。**防误判三道守卫**：GC 队列快照（待删副本不是
  孤儿——重建会复活刚 DELETE 的对象）、inflight 表（下沉在途的上传件不是
  孤儿）、per-key 锁内复核本地现状 + 云端 etag 后才动手；
- **本地已回到 local 的陈旧云副本**（GC 丢单形态）：本地全量在手，删除恒
  安全，两种模式下都直接删；
- **反向（本地 remote、云端无/etag 不符）**：先 HEAD 现点复核（列举快照与
  并发下沉有竞态），确认缺失后告警计数（数据丢失信号），**绝不静默删
  stub**——对象保留在列表中供人工介入；cached 引用失效只降级告警（数据仍
  在本地，下轮判冷重新上传）。

**隔离区**（P6，roadmap §3.6 ④）：`refs_missing`（stub 引用的云副本已丢）与
`foreign`（无 lights3 冗余头的云端孤儿）两类发现进入
`<state>/quarantine/` 账本（每条一个 TSV：kind/bucket/key/etag/首末次发现/
次数）。**首次发现才 ERROR/WARN**，之后每轮只累加次数（DEBUG），不再刷屏；
一轮对账**完整跑完**后，本轮没再复现的条目自动销账（INFO）。人工处置入口
`lights3 tier quarantine list|forget|purge <backend> …`（[cli.md §2.4](cli.md)）：
`forget` 只删账本条目，`purge` 针对 refs_missing——先 HEAD 复核云副本仍不存在，
再删除这个已死的本地 stub（承认数据丢失；副本回来了则保留 stub、销账）。
gauge `lights3_tiered_quarantine_entries{kind}` 常驻显示账本规模。本地层容量另有
`lights3_tiered_local_{used,total,high_watermark,cached,quota}_bytes` 五个回调 gauge
（[monitoring.md](monitoring.md) "tiered 水位"、[storage/tiered.md](storage/tiered.md) 指标表）。

## 10. 实施拆分

| 阶段 | 内容 | 可独立验收 | 状态 |
| --- | --- | --- | --- |
| P1 | sidecar 扩展字段 + stub 读写路径（GET/HEAD/List 识别 tier），云侧用 MemoryBackend；手动触发下沉/回迁的测试钩子 | 后端一致性套件全绿 + tier 状态机单测 | ✅ |
| P2 | TierScanner（判冷 + 水位）、TierIndex 持久化、per-key 锁与冲突矩阵测试 | 并发 PUT/GET/下沉压测无脏数据 | ✅ |
| P3 | Tee 缓存回填 + 空间兜底降级 + single-flight | 断连/ENOSPC 注入测试 | ✅ |
| P4 | GC 队列 + 对账工具 | 崩溃注入后对账收敛 | ✅ 全部落地（对账工具 + GC 指数退避 2026-07-31 收尾；stub 丢失重建/删除模式/防复活/反向告警/退避恢复专项全绿） |
| P5 | 接入真实 CloudProxyBackend（其自身为独立特性，见 docs/cloudproxy-backend.md） | 对公有云端到端 | ✅（`e2e_tiered_cloudproxy` 双实例组合） |
| P6 | roadmap §3.6：`ITierLocal` 抽象 + duostore 热层、xattr 访问记录 + 时间轮增量扫描、prefix 策略、多维淘汰评分、对账隔离区、Range 块缓存 | 增量轮/规则/评分/块缓存/隔离区/duostore 热层专项单测 | ✅（2026-09-02） |

P1–P4 完全不依赖云 SDK，`tiered` + `memory` 组合即可在 CI 全覆盖，
这是把 local 侧耦合具体类型、cloud 侧走抽象接口这一决策换来的直接红利。
