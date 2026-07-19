# 08 分层存储：冷数据下沉公有云

> 状态：P1–P5 已实现（`src/storage/tiered/`），cloud 侧经 `IStorageBackend`
> 抽象接入，CI 用 MemoryBackend 充当云端全覆盖（单测 `test_tiered.cc` +
> `e2e_tiered`）；P5 的真实 CloudProxyBackend 见
> [09-cloudproxy-backend.md](09-cloudproxy-backend.md)，组合场景由
> `e2e_tiered_cloudproxy` 验收。§9 的对账工具尚未实现；GC 重试简化为按轮
> 周期重试（未做指数退避）。

## 1. 目标与非目标

目标（对应需求）：

1. **冷下沉**：bucket 数据长时间不访问，或本地空间不足时，把对象数据上传到
   公有云，本地仅保留元数据（stub）；
2. **透明回读**：访问已下沉对象时从云端取回并返回客户端，同时缓存回本地；
3. **空间兜底**：本地拿不到空间时不缓存，仅透传，读路径永不因缓存失败而失败。

非目标（首期）：

- 不做 object 级多副本/纠删，云端始终恰好一份；
- 不做透明压缩/加密（依赖云侧 SSE 即可）；
- 进行中的 multipart 分片不参与下沉（完成后成为普通对象再进入生命周期）；
- 不做 prefix 粒度策略（策略最细到 bucket，见 §8 配置）。

## 2. 架构定位：组合后端 TieredBackend

docs/04 §2 有意把路由停在 bucket 粒度，并预留"object 级分层用叠加实现、
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
        │ LocalFsBackend /  │    │ 任意 IStorageBackend │
        │ XLocalFsBackend   │    │ （首期 CloudProxy；  │
        │ （须为具体类型，   │    │  单测用 Memory）     │
        │  复用磁盘布局）    │    └─────────────────────┘
        └───────────────────┘
```

两侧的耦合程度刻意不对称：

- **cloud 侧只经 `IStorageBackend` 抽象**——上传/下载/删除全是标准
  put/get/delete，因此云端可以是 CloudProxy、也可以是另一个 localfs
  （测试）或未来任何后端；
- **local 侧要求具体为 localfs/xlocalfs**——stub 表示、sidecar 扩展字段、
  fd 快照语义都依赖其磁盘布局（docs/04 §3.1），TieredBackend 与它共享
  `fs_util` 落盘原语，如同 xlocalfs 之于 localfs 的关系。

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

### 4.3 TierIndex：访问时间与空间账

- **atime 表**：内存 `key → last_access` 哈希（GET/HEAD 命中时无锁更新），
  周期（默认 5 min）快照到 `<staging>/tier/atime.tsv`（tmp+rename）。
  启动时加载快照，缺失项以数据文件 mtime 兜底。崩溃最多丢一个周期的
  访问记录，只影响判冷精度，可接受；不把 atime 写 sidecar（每次 GET 一次
  fsync 不可接受），也不依赖文件系统 atime（relatime 不可靠）。
- **空间账**：`statvfs` 实时读取为准（本地盘可能被其他进程共用），
  可选 `quota_bytes` 叠加逻辑配额（遍历累计、增量维护）。

## 5. 下沉流程（demote）

### 5.1 触发

后台 **TierScanner**：`TimerQueue` 周期触发（默认 1h）→ 投递到线程池跑扫描协程。
两类触发条件，产生同一个按 atime 升序的候选队列：

1. **判冷**：`now - atime > cold_after` 的 `local`/`cached` 对象；
2. **空间水位**：`statvfs` 使用率 > `space_high_watermark`（默认 85%）时进入
   回收模式，从最冷开始处理，直到降至 `space_low_watermark`（默认 70%）。
   回收顺序：先 `cached`（stub 化零成本）、再 `local`（需上传）。

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
  fd，继续读完完整旧数据（docs/07 §3.2 的 fd 快照语义天然保护）；
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

Range 请求把 range 直接透传给云端（`IStorageBackend::get_object` 本就带
range），响应按 docs/07 §3.1 正常走 206——**不做部分缓存**（稀疏文件/分块
缓存的复杂度首期不值得）。可配置 `cache_fill_on_range`（默认开）：命中时
向后台提交一个 single-flight 的整对象回迁任务（独立云端 GET → staging →
提交为 cached），空间不足同样直接放弃。

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
    type: cloudproxy                  # docs/09
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

buckets:
  default_backend: localdata
  rules:
    - match: "archive-*"              # 分层策略即 bucket 路由：想分层的 bucket 指到 tiered
      backend: tiered
```

分层的开关粒度 = bucket 路由粒度，不引入新的策略机制；不同 `cold_after`
需求可声明多个 tiered 实例。

## 9. 故障矩阵与对账

| 故障 | 行为 |
| --- | --- |
| 云端不可达（GET remote） | 透传云端错误映射（对齐 cloudproxy：远端 5xx → 502/503 S3 错误码）；本地 `local`/`cached` 对象完全不受影响 |
| 云端不可达（scanner/GC） | 本轮跳过，指数退避；判冷积压无副作用 |
| 上传后崩溃、stub 未提交 | 云端孤儿副本，下轮幂等覆盖或对账清理 |
| stub 提交一半崩溃（§5.2 b/c 之间） | sidecar 为准走云端读，数据文件下轮补回收 |
| 缓存回填中断连/ENOSPC | TmpFile 丢弃/降级透传，客户端无感知 |
| 本地 stub 丢失（人为误删） | 对账工具从云端 `x-amz-meta-lights3-*` 冗余头重建 sidecar |

**对账任务**（低频，默认每天）：遍历云端 `remote_bucket` 与本地 stub 集合做
双向 diff——云端有、本地无 → 重建 stub 或删除（可配置）；本地 remote、
云端无 → 告警（数据丢失，绝不静默删 stub）。

## 10. 实施拆分

| 阶段 | 内容 | 可独立验收 | 状态 |
| --- | --- | --- | --- |
| P1 | sidecar 扩展字段 + stub 读写路径（GET/HEAD/List 识别 tier），云侧用 MemoryBackend；手动触发下沉/回迁的测试钩子 | 后端一致性套件全绿 + tier 状态机单测 | ✅ |
| P2 | TierScanner（判冷 + 水位）、TierIndex 持久化、per-key 锁与冲突矩阵测试 | 并发 PUT/GET/下沉压测无脏数据 | ✅ |
| P3 | Tee 缓存回填 + 空间兜底降级 + single-flight | 断连/ENOSPC 注入测试 | ✅ |
| P4 | GC 队列 + 对账工具 | 崩溃注入后对账收敛 | GC 队列 ✅；对账工具未做 |
| P5 | 接入真实 CloudProxyBackend（其自身为独立特性，见 docs/09） | 对公有云端到端 | ✅（`e2e_tiered_cloudproxy` 双实例组合） |

P1–P4 完全不依赖云 SDK，`tiered` + `memory` 组合即可在 CI 全覆盖，
这是把 local 侧耦合具体类型、cloud 侧走抽象接口这一决策换来的直接红利。
