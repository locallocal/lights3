# Tiered 后端实现：冷热分层组合器

> 本文是 `src/storage/tiered/tiered_backend.h/.cc` 的实现级文档：数据结构、
> 后台流程逐步拆解、竞态处理与指标。设计动机、状态机与方案取舍见
> [../tiered-storage.md](../tiered-storage.md)（下文以 §N 引用其章节），
> 本文不重复其内容，只讲"代码怎么落地的"。

## 1. 组合方式与构造

`TieredBackend`（`tiered_backend.h:TieredBackend`）对 L2 是一个普通
`IStorageBackend`（`src/storage/backend.h:IStorageBackend`），内部组合两个
**由 StorageRegistry 独立构建、以 name 引用**的既有后端：

- `local_`：必须是 `LocalFsBackend`（xlocalfs 亦可，同磁盘布局）。tiered 直接
  依赖其具体类型——自己拼数据文件路径（`local_->object_data_path`）、复用其
  staging 目录、调用 `fs_util` 落盘原语。`tiered_backend.cc:TieredBackend::from_config`
  里 `dynamic_pointer_cast<LocalFsBackend>` 失败即配置错误；
- `cloud_`：任意 `IStorageBackend`，只经标准 put/get/head/delete/list 抽象访问。

`from_config` 是两阶段构造的第二阶段入口：`built` 表里已含全部叶子后端，
按 `params["local"]` / `params["cloud"]` 取引用（两者不得是同一实例）。参数
解析要点：

- 水位用 `parse_pct` 解析，`"85%"`、`"85"`、`"0.85"` 三种写法等价——`%` 后缀
  必须参与判定（历史 bug：`1%` 被解析成 100%，低水位反超使用率后无符号回绕，
  整桶全量下沉，docs/gaps.md §3.9）；
- `gc_retry_base >= 1s && <= gc_retry_cap`、`low <= high` 在构造期校验，
  违反直接抛异常拒绝启动。

构造函数（`tiered_backend.cc:TieredBackend::TieredBackend`）做五件事：建
`<staging>/tier/gc` 目录；初始化 64 条 striped `AsyncSemaphore` per-key 锁；
扫描 GC 目录恢复 `gc_seq_`（取现存最大编号 +1，重启不回绕）；
`load_atime_snapshot()`；启动三个定时器（§7）。

## 2. stub 元数据：格式与存放位置

tiered 自己**不新增任何索引文件**，对象状态完全寄生在 localfs 的元数据载体里
（设计理由见 §4.1）：

- **载体**：数据文件的 xattr（`fs_util.h:kMetaXattr`）+ 同名 sidecar 文件
  `<data>.lights3-meta`（TSV，`fs_util.h:kSidecarSuffix`），读取时 xattr 优先、
  sidecar 兜底；
- **扩展字段**（`fs_util.h:TierInfo`）：`tier`（`remote`/`cached`，缺省 local）、
  `size`（tier != local 时数据文件是 0 长度，size 以元数据为准）、
  `remote.etag`（云端副本 ETag，无引号 hex，仅校验/GC 用，绝不外泄）、
  `remote.at`（iso8601 上传时间）；
- **stub 本体**：0 长度数据文件占位，list/delete/前缀冲突逻辑原样复用 localfs。

两个提交原语的写入次序刻意相反，保证任意点崩溃后 sidecar 都指向可读的一侧：

| 原语 | 次序 | 崩溃于中间的后果 |
| --- | --- | --- |
| `fs_util.h:commit_stub`（下沉） | 先写 tier=remote sidecar，再 rename 0 长度 tmp 盖数据文件 | sidecar 已 remote 但数据文件仍全量：读走云端（正确），空间由下轮 scan 补回收 |
| `fs_util.h:commit_cached`（回填） | 先 rename 数据 tmp，再写 tier=cached sidecar | sidecar 仍 remote：读继续走云端（正确），全量文件同样由 scan 按"remote 且 size>0"识别 |

轻量读取用 `tiered_backend.cc:read_tier_only`（只解析 sidecar 的三个 tier
字段，不要求数据文件存在），供 PUT/DELETE 前查旧云副本；完整读取走
`fs_util.h:load_object_meta`（顺带返回 `TierInfo`）。

下沉上传时云端对象额外携带 `x-amz-meta-lights3-etag` /
`lights3-content-type` 冗余头（`demote_object` 里填入 `cloud_meta.user_meta`），
本地 stub 意外丢失时对账可据此重建（§4.2/§9，见本文 §9）。

## 3. atime 跟踪与快照

`TierIndex`（§4.3）在代码里就是三个成员：

- `atime_`：`unordered_map<ikey, epoch秒>`，ikey = `bucket + "/" + key`
  （`tiered_backend.h:TieredBackend::make_ikey`）；`atime_m_` 普通 mutex 保护
  （临界区仅哈希表操作，微秒级，不需要异步锁）；
- `atime_dirty_`：距上次快照是否有变化——无变化则跳过整表重写 + fsync
  （空闲实例不再每 5 分钟白写一遍，docs/gaps.md §4）；
- 快照文件：`<staging>/tier/atime.tsv`，`fs_util.h:write_tsv` tmp+rename 原子写。

更新点：GET/PUT/HEAD/complete_multipart 成功后 `touch_atime`；DELETE 后
`erase_atime`；scan 里查表用 `atime_or(ikey, mtime)`——**缺失项以数据文件
mtime 兜底**，所以快照丢失只影响判冷精度，不影响正确性。

`tiered_backend.cc:TieredBackend::save_atime_snapshot` 的有界性设计：写快照
前先滚动淘汰所有 `now - atime >= cold_after` 的条目——这些对象已到判冷资格，
丢掉 atime 后 mtime 兜底得出同样结论，因此表只随"近期被访问过的 key"增长
（绕过本后端的删除不会让表无界膨胀）。含 `\t`/`\n` 的 ikey 会破坏 TSV 行格式，
跳过不写。写失败时把 `atime_dirty_` 置回 true，下轮重试，避免丢失被
"not dirty"永久掩盖。

快照时机：独立 5 分钟定时器（`kAtimeSnapshotSec`）、每轮 `scan_once` 末尾、
`close()` 收尾各一次。

## 4. 下沉（demote）后台流程

### 4.1 扫描：`scan_once`

`tiered_backend.cc:TieredBackend::scan_once` 一轮做四件事（判冷 + 崩溃恢复 +
水位回收 + atime 快照），两遍目录遍历：

**第一遍（流式，不物化候选集）**：遍历 `local_->root()` 下所有 bucket 的数据
文件（跳过 sidecar 与 marker），对每个对象：

1. 累计 `local_bytes`（配额账本，遍历完后校准 `local_bytes_est_`）；
2. `tier == remote` 且 stat size > 0 → **崩溃恢复**候选（§5.2 b/c 之间崩溃，
   stub 写了但数据文件没回收），交给 `demote_object` 补完 stub 化；
3. 否则 `now - atime_or(ikey, mtime) >= cold_after` → **判冷**候选，同时把其
   size 计入 `cold_freed`。

候选不一次性起 N 个协程：`kScanBatch = 128` 个 `demote_quiet` 攒一批
`when_all`，协程帧数量有上界（`transfers_` 信号量只限执行并发、不限帧数，
docs/gaps.md §2.13）。`chosen` 集合去重，避免同一对象两遍都被选中。

**水位判定**：第一遍结束后 `statvfs` 实测使用率 > `space_high_watermark`
则计算需释放字节数 `need`（降到 low watermark）；可选 `quota_bytes` 逻辑配额
叠加（账本 `local_bytes - cold_freed` 与配额水位比较，取两者较大的 need）。

**第二遍（仅超水位时）**：再遍历一次收集牺牲品，`multiset<Evict>` 按
`(rank, atime)` 排序——`cached` rank=0 优先（stub 化零流量），`local` rank=1。
关键的内存上界技巧：multiset 只保留"恰好覆盖 need"的最优前缀，每插入一个就
从最差端裁剪超额部分，内存正比于 need 而非对象总数。选完按序 `pick` 下沉，
每个扣减 need 直至归零。若候选耗尽 need 仍 > 0（磁盘被后端之外的东西占用），
打 WARN——此前这种情况每轮静默空转，运维完全不可见。

### 4.2 单对象下沉：`demote_object`

`tiered_backend.cc:TieredBackend::demote_object` 逐步（对应 §5.2）：

1. `transfers_.acquire()` 限流（默认 4 并发），然后 `pool_->schedule()` 切池
   线程（全部阻塞 IO 不落 HTTP 线程）；
2. 读元数据快照 `(m0, t0)`。若 `t0.tier == remote`：进**崩溃恢复分支**——数据
   文件 size > 0 则在 per-key 锁内复核后补做 `commit_stub`，然后返回（这次不
   计入 `m_demoted_`，崩溃前那次下沉已计过数，重启后重计会虚高）；
3. `inflight_try_begin(ikey)` 失败即返回（同 key 已有下沉/回填在途；in-flight
   表同时保护上传件不被 GC/对账误判为孤儿）。`InflightRelease` RAII 保证异常
   路径也释放；
4. **cached → remote 零流量路径**：先 `cloud_->head_object` 校验云端 etag 仍
   等于 sidecar 的 `remote.etag`，成立则跳过上传直接进提交；云端失效/不可达
   则退化为全量上传；
5. **全量上传**：`ensure_cloud_bucket`（直接 create、409 视为已存在——不走
   bucket_exists，因 cloudproxy 把远端 403 当"存在"，网关权限故障会让该检查
   撒谎）；`open(data)` 取 fd 快照，包 `tiered_backend.cc:HashingFdReader`
   （`FdStreamReader` 上叠加同步 MD5 + 字节计数）流式
   `cloud_->put_object`，云端 meta 附带 lights3-* 冗余头；
6. **校验**：字节数 == m0.size；单段对象 MD5 == 本地 etag；云端返回单段形态
   etag 时再与重算 MD5 比对一次。任一不过（多半是上传期间对象被覆盖）→ 云副本
   `enqueue_gc`，本轮放弃；
7. **提交（per-key 锁内）**：重读元数据复核——对象已被 DELETE（load 抛
   NoSuchKey）或 etag 变了（被 PUT 覆盖）→ 云副本入 GC，用户写胜出（§7.3）；
   已被别人 stub 化 → 直接返回；否则 `commit_stub` + `m_demoted_->inc()`。
   锁唤醒线程不确定，锁后必须再 `pool_->schedule()` 把同步文件 IO 拉回池线程。

`demote_quiet` / `promote_quiet` 是吞异常的包装：单对象失败只打 WARN 跳过，
退避到下轮 scan 重试，不让一个坏对象打断整轮。

## 5. 透明回读与缓存回填

### 5.1 GET：`get_object`

`tiered_backend.cc:TieredBackend::get_object` 在一个重试循环里（最多 3 次）：

1. `load_object_meta` 读 tier；元数据缺失或 `tier != remote` → 委托
   `local_->get_object`。若 local 抛 `fs_util.h:StubRace`（open 到的 fd 是
   0 长度新 inode 而 sidecar 宣称 size>0——open 与 stub 化 rename 竞态），
   捕获后 `continue` 重读 tier 改走云端；
2. **remote**：`cloud_->get_object` 开流。返回给客户端的 `ObjectStream.meta`
   永远是**本地 sidecar 快照 m**（对外 ETag 恒等原则，§4.2），云端流只出字节；
3. **Range 请求**：云端流直接透传，不做部分缓存（§6.3）；若
   `cache_fill_on_range` 开启则 `bg_.spawn(promote_quiet(...))` 后台整对象回迁；
4. **全量请求**：满足 `size > 0`、`cache_space_ok(size)`（statvfs 可用 >
   size + min_free_bytes）、`inflight_try_begin` 三条件才包
   `TeeCacheReader`；任一不满足纯透传（后到的并发 GET 天然被 in-flight 表挡成
   纯透传，即 single-flight，§6.4）。staging tmp 打不开时同样回退纯透传并释放
   in-flight。

HEAD 完全本地完成（stub 的 sidecar 信息完备），只额外 `touch_atime`。

### 5.2 Tee 边下边缓存：`TeeCacheReader`

`tiered_backend.cc:TeeCacheReader` 是包在云端流外的 `BodyReader`：每次
`read()` 把数据透传给客户端方向的同时写 staging tmp + 增量 MD5。关键行为：

- **写盘失败（ENOSPC 等）**：置 `degraded_`，静默降级纯透传，客户端无感知；
- **EOF 时**：字节数齐且校验过（单段比 MD5，multipart 只能比字节数，etag 是
  `-N` 形态）→ `co_await commit_cache_fill`。提交异常被 try/catch 吞掉只打
  WARN——此刻客户端已收完全部数据，异常若漏进响应收尾会被误读为传输失败；
- **in-flight 释放点**：EOF 处立即释放（reader 可能被响应链长期持有，不能一直
  卡住同 key 的下沉/回迁）；析构函数兜底（客户端中途断连时协程链回卷，
  `TmpFile` RAII 丢弃半截缓存）。

### 5.3 整对象回迁与提交

`tiered_backend.cc:TieredBackend::promote_object`（Range 触发的后台回迁 +
测试钩子）：限流 → 校验仍是 remote → 空间预检 → single-flight → 云端全量 GET
→ 写 staging tmp（256KiB 缓冲循环）→ 字节数/MD5 校验 → `commit_cache_fill`。

`tiered_backend.cc:TieredBackend::commit_cache_fill` 是两条回填路径共用的唯一
提交汇合点：per-key 锁内（锁后切回池线程——此函数被 TeeCacheReader 在客户端读
EOF 时 co_await，不切线程的话 rename + 两次 fsync 会直接落在 HTTP 响应线程，
docs/gaps.md §2.4）重读元数据复核：对象已删 / etag 变了 / tier 已非 remote /
`remote.etag` 与开流时的期望不符 → 丢弃 tmp（用户写胜出）；复核通过才
`commit_cached` + `m_promoted_->inc()`（计数器放在提交点而非 promote 出口：
只统计"数据真正回到本地"）。

## 6. 写删路径与 GC 队列

PUT/DELETE/complete_multipart 的分层逻辑都是"前查旧 tier → 委托 local →
善后"：

- `put_object`：先 `read_tier_only` 记下旧 tier 与旧 `remote.etag`，委托
  local（条件 PUT 直接透传——stub 的 sidecar 保留原始 etag，local 提交锁内的
  检查对已下沉对象同样权威），成功后若旧 tier != local 则旧云副本
  `enqueue_gc`；顺带 stat 前后差值 `note_local_delta` 维护配额账本并
  `maybe_kick_quota_scan`（账本超高水位时提前踢一轮 scan，单飞标志
  `quota_kick_inflight_` 保证同时只有一轮）；
- `delete_object`：本地删除立即返回（**响应不等云端**），云副本入 GC 异步删；
- multipart 其余接口纯委托 local（进行中的分片不参与分层）。

**GC 队列**（`tiered_backend.cc:TieredBackend::enqueue_gc`）：
`<staging>/tier/gc/<20位零填充seq>` 每项一个 TSV（tmp+rename 写入），字段
`bucket`/`key`/`etag`(+退避字段)。入队失败只打 WARN——代价仅是一个等对账清理的
云端孤儿，不影响正确性。

`tiered_backend.cc:TieredBackend::run_gc_once` 每项的处理：

1. 解析失败（缺字段）→ 删条目（corrupt）；`retry_at > now` → 退避未到点，
   跳过（deferred，零云端访问）；ikey 在 in-flight 表 → 下沉在途，下轮再看；
2. per-key 锁内复核本地 sidecar：若 tier != local 且 `remote.etag == 条目 etag`
   → **活引用**（同内容被重新下沉），条目作废删除；
3. `cloud_->head_object`：etag 匹配才 `delete_object`（不匹配说明云端已有新
   副本，条目直接作废）；NoSuchKey/NoSuchBucket → 云端本来就没有，条目删除；
4. 其他云端错误 → 指数退避重排：delay = `gc_retry_base × 2^attempts` 钳制
   `gc_retry_cap`，原地重写条目 TSV 追加 `attempts`/`retry_at`——**退避状态持久
   化在条目里，重启不清零**；旧格式条目两字段缺省 0（立即可试），向后兼容。

## 7. 后台任务编排与 close()

所有后台协程经 `BackgroundTaskGroup bg_`（core/background.h：spawn 计数 +
close 等待归零）管理，三个 `TimerQueue` 定时器：

| 定时器 | 周期 | 任务 |
| --- | --- | --- |
| `scan_timer_` | `scan_interval`（默认 1h） | `scan_and_gc()`：先 `run_gc_once` 再 `scan_once` |
| `snap_timer_` | 固定 5 min | `snapshot_task()`：atime 快照 |
| `reconcile_timer_` | `reconcile_interval`（默认 1d） | `reconcile_task()`：整轮失败只 WARN，下周期重试 |

调度模式是一次性定时器自我续期：回调里 `bg_.spawn(任务)` + 重新
`schedule_xxx()`，两者都包在 `bg_.if_open` 里——begin_close 之后回调静默不再
续期。`scan_interval = 0` 是后台任务总开关（测试手动钩子模式，三个定时器都不
启动；`reconcile_interval = 0` 单独关对账）。

`tiered_backend.cc:TieredBackend::close` 次序（析构函数是其无快照版兜底）：

1. `bg_.begin_close()`：拒绝新 spawn；
2. `TimerQueue::cancel` × 3——必须在 group 锁外调：cancel 阻塞等待在途回调返回，
   而回调会拿 group 锁（spawn/if_open），反序即死锁。begin_close 后 timer id
   不再变化，读取无需加锁（id 0 = 未启用，cancel(0) 是安全 no-op）；
3. `bg_.wait_idle()`：阻塞等全部在途后台协程结束（后台任务在池线程收尾，与
   调用方线程不互相等待）；
4. `save_atime_snapshot()`；
5. `co_await local_->close()`——取消底座 localfs 的周期 mpu 清理定时器。
   `cloud_` 不关闭：它由 registry 独立持有，可能同时被直接路由。

## 8. 故障处理与一致性保证

核心原则（§7.3）：**用户写永远胜过后台任务**；per-key striped 异步锁
（`tiered_backend.cc:TieredBackend::key_lock`，64 桶）只保护微秒级的状态提交
段，数据搬运全在锁外流式进行；后台任务失败的代价只是流量浪费 + 一个待 GC 的
云副本，从不影响正确性。

迁移中途失败的落点：

| 失败点 | 后果与收敛路径 |
| --- | --- |
| 上传后、stub 提交前崩溃/被覆盖 | 云端孤儿副本 → 入 GC（在线竞态）或对账清理（崩溃） |
| commit_stub 写完 sidecar、rename 前崩溃 | sidecar=remote 走云端读（正确），数据文件由下轮 scan 崩溃恢复分支补回收 |
| 回填提交前被 PUT/DELETE | 锁内复核不过 → 丢 tmp，用户写胜 |
| 回填断连/ENOSPC | TmpFile RAII 丢弃 / 降级纯透传，客户端无感知 |
| GC 入队失败 | 云端孤儿，等对账（rebuild 模式下会被重建为 stub——见下面已知竞态） |
| atime 快照写失败 | dirty 标记保留下轮重试；最坏丢一个周期精度 |

已知竞态与残留窗口：

- **GET open 与 stub 化 rename**：`StubRace` 异常 + 重读重试收敛（在途读者持
  旧 inode fd 不受影响，fd 快照语义天然保护）；
- **DELETE 后、GC 条目兑现前**的窗口里跑对账：云副本看似孤儿但不能重建
  （会复活刚删的对象）——靠对账开头的 **GC 队列快照** + **in-flight 表**双守卫
  排除（见 §9）；
- **配额账本漂移**：`local_bytes_est_` 由 PUT/DELETE 增量维护，demote/promote
  造成的偏差只朝保守方向（下沉释放的字节仍记在账上），每轮 scan 第一遍全量
  校准；-1 表示未校准，此前不记账（避免负账本）。

## 9. 对账：`run_reconcile_once`

`tiered_backend.cc:TieredBackend::run_reconcile_once` 逐 bucket 做本地/云端
key 集合的**双游标有序归并**（两侧都按 key 字典序分页 list，O(页) 内存，
不物化全量 key 集，docs/gaps.md §2.13），开始前先快照 GC 队列成
`"ikey\tetag"` 集合。三种归并结果：

- **两侧都有**：本地 tier != local 时校验 `remote.etag == 云端 etag`，不符则
  WARN 计 skipped 并转 `reconcile_ref_missing` 反向 HEAD 复核；本地已回到
  local（GC 丢单形态的陈旧副本）→ `reconcile_orphan(local_is_live=true)`，
  本地全量在手，删除恒安全；
- **仅云端有**（孤儿候选）：GC 快照命中或 in-flight 命中则跳过，否则
  `reconcile_orphan(local_is_live=false)`；
- **仅本地有**：tier != local 时 `reconcile_ref_missing`。

`tiered_backend.cc:TieredBackend::reconcile_orphan`：per-key 锁内重读本地现状
（listing 快照期间可能发生了 PUT/下沉/DELETE，状态变了就让位不裁决），再
HEAD 复核云端 etag 未变，然后：`local_is_live` 或 `reconcile_orphans: delete`
→ 删云副本；默认 rebuild 模式 → **只信 lights3-* 冗余头**重建 stub（原始
etag/content-type/首类元数据/user meta 全还原，`commit_stub` 落盘 +
touch_atime），无冗余头视为外来对象（远端 bucket 可能与他方共用），告警跳过
绝不动。

`tiered_backend.cc:TieredBackend::reconcile_ref_missing`（反向裁决）：先查
in-flight（下沉中间态不裁决），再 HEAD 现点复核；确认缺失后——stub 引用丢失是
**数据丢失信号**，ERROR 计 `refs_missing`，**绝不删 stub**（留给人工介入）；
cached 引用失效只 WARN（数据仍在本地，下轮判冷会重新上传）。

## 10. 指标

`tiered_backend.cc:TieredBackend::init_metrics` 预注册全部维度（缺失序列在
Prometheus 里读作"无数据"而非 0）。只在 tiered 自身有分层逻辑的四个 op
（get/put/delete/list）上计数，纯委托路径由 `local_` 的 `lights3_localfs_*`
覆盖；op 计数经 `OpGuard`（协程帧内 RAII）落账，异常回卷也计为 error。

| 指标 | 类型 | 含义 |
| --- | --- | --- |
| `lights3_tiered_ops_total{op}` / `op_errors_total{op}` | counter | 四个分层 op 的完成/出错数 |
| `lights3_tiered_get_source_total{source=local\|cloud}` | counter | GET 数据来源分流——cloud 占比上升 = 缓存命中率恶化，最核心健康信号 |
| `lights3_tiered_demoted_objects_total` | counter | stub 实际落盘的下沉数（崩溃恢复补 stub 不计） |
| `lights3_tiered_promoted_objects_total` | counter | 数据真正回到本地的回迁数（显式回迁 + Tee 回填共用提交点） |
| `lights3_tiered_scan_seconds` | histogram | 完整跑完的 scan 轮耗时（中途抛异常的轮不观测） |
| `lights3_tiered_gc_runs_total` / `gc_removed_cloud_total` / `gc_failed_total` | counter | GC 轮数 / 实删孤儿数 / 失败重排数 |
| `lights3_tiered_gc_deferred` | gauge | 本轮仍在退避中的条目数（同一条目每轮重现，累计会虚高，故用 gauge 覆盖式写入） |

GET 来源计数在**成功之后**才 +1：StubRace 重试改走云端时不会留下半截 local
计数。
