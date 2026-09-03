# LocalFs 后端实现

本文是 `LocalFsBackend` 的实现级文档，展开
[../storage-backend.md](../storage-backend.md) §3 的设计决策在代码中的具体落法。
读写主链路（三层视角）见
[../object-read-write-flow.md](../object-read-write-flow.md) §2.3/§3.2，本文只讲
L3 内部。涉及文件：

| 文件 | 内容 |
| --- | --- |
| `src/storage/localfs/localfs_backend.h/.cc` | `IStorageBackend` 的本地文件系统实现 |
| `src/storage/localfs/fs_util.h/.cc` | 落盘原语（tmp 文件、TSV、原子提交、流式读），与 xlocalfs / tiered 共享 |
| `src/storage/backend.h` | 接口与校验函数声明（`IStorageBackend` / `validate_*`） |

xlocalfs 继承本类、只覆盖数据面字节搬运（io_uring）；tiered 通过
`localfs_backend.h:LocalFsBackend::root()/staging()/object_data_path()` 直接复用同一磁盘布局
（见 [../tiered-storage.md](../tiered-storage.md)）。

## 1. 磁盘布局与 key → 路径映射

布局总览见 [../storage-backend.md](../storage-backend.md) §3.1。保留名常量集中在
`fs_util.h`：

| 常量 | 值 | 作用 |
| --- | --- | --- |
| `fs_util.h:kBucketMarker` | `.lights3-bucket` | bucket 存在性标记（目录内的空文件） |
| `fs_util.h:kSidecarSuffix` | `.lights3-meta` | 元数据 sidecar 文件后缀（`<data>.lights3-meta`） |
| `fs_util.h:kDirMarker` | `.lights3-dir` | 目录标记对象（key 以 `/` 结尾）的数据文件名 |
| `fs_util.h:kMetaXattr` | `user.lights3.meta` | 数据文件上的元数据扩展属性 |

**key 不做转义编码**：`/` 直接作目录分隔落盘（`localfs_backend.cc:object_path` 即
`bucket_dir(bucket) / key`），保持人类可读。安全性靠三层校验而非编码：

1. `backend.h:validate_object_key`（所有后端共用）：非空、≤1024B、无控制字符、
   无 `.`/`..` 段；
2. `backend.h:validate_fs_object_key`（路径映射后端附加）：无前导 `/`、无空段、
   每段 ≤255B；**允许**尾部 `/`（目录标记对象，s3fs/goofys/控制台"创建文件夹"
   依赖此形态）；
3. `fs_util.cc:reject_reserved_key`：key 含 `.lights3-bucket`/`.lights3-dir` 或以
   `.lights3-meta` 结尾一律 `InvalidArgument`，防止与内部文件同名冲突。

尾 `/` 的 key 在文件系统上没有对应文件名，`object_path` 把 `a/b/` 映射到
`<bucket>/a/b/.lights3-dir`；listing 再把该标记文件还原为 key `a/b/`（§5）。

`object_path` 还有一道纵深防御：`fs::path::operator/` 遇到绝对路径右操作数会
**整体替换**（`root_ / "/etc" == "/etc"`），因此拼接后用 `lexically_normal` 归一化并
`std::mismatch` 确认结果仍以 `root_` 为前缀，越界抛 `InvalidBucketName`——纯词法
检查，不触盘。

## 2. 元数据模型：xattr 为主、sidecar 回落

元数据序列化为 `k<TAB>v` 行格式（TSV），键集合由 `fs_util.cc:meta_kv` 生成：

| 键 | 含义 |
| --- | --- |
| `etag` | 内容 MD5 hex（无引号） |
| `content_type` | Content-Type |
| `cache_control` / `content_disposition` / `content_encoding` / `content_language` / `expires` | 五个一等元数据字段，键名来自 `backend.h:kStdMetaFields`（空值不写） |
| `meta.<k>` | `x-amz-meta-*` 用户元数据 |
| `tier` / `size` / `remote.etag` / `remote.at` | 仅 tiered 的 stub/cached 对象写入（tier=local 不写，存量 sidecar 字节不变） |

同一份 TSV 有两个载体：

- **xattr**（`fs_util.cc:set_meta_xattr`）：rename 提交前写进数据 tmp 文件的
  `user.lights3.meta`，随 inode 走，**一次 rename 同批提交数据与元数据**，
  读到的 etag 不可能描述另一个 inode 的 body。`setxattr` 失败
  （ENOTSUP/E2BIG 等）降级为纯 sidecar 语义，按 errno 种类只告警一次——并计入
  `fs_util.h:MetaXattrPolicy`（roadmap §3.5）：常驻 gauge
  `lights3_localfs_xattr_fallback`（首次失败起恒为 1）与计数器
  `lights3_localfs_xattr_write_failures_total`。构造期先用
  `fs_util.cc:probe_meta_xattr` 在 staging 探测一次（staging 与 root 同文件系
  统），不支持时 gauge 在第一个 PUT 之前就已亮起；`require_xattr: true` 时探
  测失败直接构造抛错，运行期写失败则在 rename **之前**抛 `InternalError`
  （tmp 由 RAII 丢弃，不会留下"无 xattr 的半提交对象"）。
- **sidecar**（`fs_util.cc:write_tsv` 写 `<data>.lights3-meta`）：外部工具可读、
  存量对象兼容、不支持 xattr 的文件系统上的唯一来源。自身也是 tmp+rename
  原子写，rename 前 `fsync_path` 内容、rename 后 `fsync_dir` 父目录。
  是否/何时写由 `sidecar` 模式决定（§3 末尾）。

读取入口是 `fs_util.cc:load_object_meta`（stat + 委托）与
`fs_util.cc:load_object_meta_stat`（复用调用方已持有的 `struct stat`）：
size/mtime 取 stat，然后 **xattr 优先**（`fs_util.cc:get_meta_xattr`，ERANGE 时按实际
大小重取，绝不因超长静默回落 sidecar）、缺失才读 sidecar，二者共用
`fs_util.cc:parse_meta_tsv`。`tier != local` 时 size 改用 TSV 里声明的
`size`（stub 数据文件长度为 0）。数据文件缺失或非普通文件抛 `NoSuchKey`。

## 3. put_object 流程

`localfs_backend.cc:put_object` 分两段，per-key 锁只覆盖第二段：

**① 流式写 staging（无锁、全并发）**

1. 入口校验：`validate_bucket_name` / `validate_object_key` /
   `validate_fs_object_key` / `reject_reserved_key`；
2. `co_await pool_->schedule()` 切到 IO 线程池，此后全程阻塞 IO；
   `require_bucket`（无 marker 抛 `NoSuchBucket`）；
3. 建 `fs_util.h:TmpFile`，路径 `<staging>/put/<pid>-<ts>-<seq>`
   （`fs_util.cc:next_tmp_name`，pid + steady_clock + 进程内原子序号，无碰撞），
   `O_CREAT|O_EXCL` 打开；
4. 循环 `body.read(64KiB)` → `HashStream::update`（增量 MD5）→ `write(tmp)`
   短写重试直到写完。读到 EOF（返回 0）为止——接口契约要求读穿 EOF，
   上层验签装饰器挂在 EOF 点（见 `backend.h:IStorageBackend::put_object` 注释）；
5. 填 `meta.key/size/etag/last_modified`，ETag = 全文 MD5 hex。

**② 提交段（per-key 锁内）**

6. `co_await commit_lock(bucket, key).acquire()`——64 条带 striped
   `AsyncSemaphore`（`localfs_backend.cc:commit_lock`，bucket/key 哈希混合取模）。
   锁唤醒可能在别的线程恢复协程，因此紧跟一次 `pool_->schedule()` 回池线程；
7. `fs_util.cc:check_put_condition`：条件 PUT 的判定点。`if_none_match` 要求对象
   不存在（存在→`PreconditionFailed`），`if_match_etag` 要求当前 etag 相等
   （不等→412，缺失→`NoSuchKey`）。判定读的是 xattr/sidecar 元数据，对 tier
   stub 同样权威；与后续 rename 同锁，检查与提交对并发写者原子
   （`backend.h:PutCondition` 契约）；
8. `fs_util.cc:commit_object_file` 原子提交（PUT 与 complete_multipart 共用）：
   - `create_directories(父目录)`：`not_a_directory`/`file_exists` → key 与既有
     对象路径冲突（`InvalidArgument`）；其余（ENOSPC/EACCES/EIO）→
     `InternalError`——错误映射刻意区分，磁盘满不能伪装成客户端错误；
   - 目标已是目录 → key 与既有前缀冲突（`InvalidArgument`）；
   - `set_meta_xattr(tmp)` → `fsync_path(tmp)`（数据内容先落盘）→
     `rename(tmp → dest)`（**提交点**）→ `tmp.committed = true` →
     `fsync_dir(父目录)` → 最后写 sidecar。

**先数据后 sidecar** 的顺序论证见
[../object-read-write-flow.md](../object-read-write-flow.md) §2.3：反序的崩溃窗口是
"新 etag + 旧数据"（GET 静默损坏），本序只余"新数据 + 旧 sidecar"，且支持
xattr 的文件系统上读侧根本不看 sidecar，该窗口不可见。并发撕裂（数据=A、
sidecar=B）由 per-key 锁消除，只剩单写者崩溃窗口。

失败路径统一由 `fs_util.cc:TmpFile` 析构兜底：任何一步抛异常（含
`body.read` 因客户端断连抛错），未 `committed` 的 tmp 自动 unlink，最终路径
要么旧要么新。

**sidecar 写策略**（`LocalFsOptions::sidecar`，配置键 `sidecar`，roadmap §3.5）：
小对象负载下上面第 8 步的元数据开销（sidecar 的 1 次 fdatasync + 1 次 rename +
1 次目录 fsync）大于数据本身，而 xattr 已是权威读源。
`fs_util.cc:finish_object_sidecar` 按模式收尾（`commit_object_file` 与 xlocalfs
的 `commit_prepared` 共用）：

| 模式 | 关键路径成本（每 PUT） | 行为 |
| --- | --- | --- |
| `sync`（默认） | 4 fsync + 2 rename | 与既往完全一致 |
| `async` | 2 fsync + 1 rename | 返回 `deferred=true`，后端 `defer_sidecar` 经 `BackgroundTaskGroup` 在池线程补写（`close()` 等其落地；已关闭时改为内联写）。同 key 两次 async 补写之间不保证顺序——sidecar 在此只是辅助副本 |
| `lazy` | 2 fsync + 1 rename | 不写 sidecar；顺手 `unlink` 旧 sync 时代遗留的过期 sidecar（不 fsync，崩溃复活的过期 sidecar 就是 lazy 之前的状态，无害） |

两种非 sync 模式在 **xattr 写失败**时都退回同步写 sidecar——它此时是唯一
元数据源，必须在应答前落盘。`?tagging` 的 `rewrite_object_meta` 在 lazy 下只
在 sidecar 已存在（保持一致）或 xattr 失败时才写。tiered 的 stub/cached 提交
（`commit_stub`/`commit_cached`）自带 sidecar 写入，不受此选项影响。

## 4. get_object 流程

`localfs_backend.cc:get_object` 返回"打开即快照"的流：

1. 同 PUT 的入口校验 + `pool_->schedule()` + `require_bucket`
   （bucket 存在性是无条件前置——历史实现只在 open 失败分支查 bucket，
   成功路径完全绕过校验；元数据缓存命中时跳过——记录只在 inode 戳仍匹配时
   使用，而它证明对象上次被读到时 bucket 存在，见 §5.1）；
2. `open(O_RDONLY)`，失败抛 `NoSuchKey`；`fstat` 确认普通文件；
3. `fs_util.cc:load_object_meta_stat` 用**已打开 fd 的 fstat 结果**读元数据，
   绝不对路径二次 stat：并发覆盖后路径指向新 inode，二次 stat 的 size/mtime
   会与 fd 持有的旧 body 错位（短包或截断，静默损坏）。缓存记录的戳与这次
   fstat 一致则直接采用（省掉 xattr/sidecar 读），否则失效并重读回填；
4. tiered 竞态检查：若元数据声明 `tier != local` 且 `st_size != 声明 size`，
   说明对象在 open 与读 sidecar 之间被 stub 化（fd 指着 0 长新 inode），抛
   `fs_util.h:StubRace`——`TieredBackend` 捕获后改走云端重试，独立 localfs
   误配到 tiered 布局时映射为 500；
5. `backend.h:resolve_range` 把 `a-b`/`a-`/`-n` 解析为闭区间 `[f,l]`，不可满足抛
   `InvalidRange`(416)；size=0 且无 Range 时 len=0；
6. 构造 `fs_util.h:FdStreamReader`（fd 所有权移交）。

`fs_util.cc:FdStreamReader::read` 每块先 `co_await pool_->schedule()` 再
`pread(fd, offset)`——阻塞 IO 不占 HTTP 执行环境，带偏移天然支持 Range，且
**持有 fd 意味着传输中途对象被覆盖（rename 走新 inode）或删除，仍能读完旧
文件的完整快照**；文件被外部截断（`pread` 返回 0）则提前 EOF。析构关 fd，
协程链取消/断连时 RAII 自动清理。

GET 的延迟指标只统计到流句柄就绪（open + 元数据），不含 body 传输——后者由
客户端拉取节奏主导，混入会让网络延迟淹没磁盘延迟。

## 5. head / delete / copy

- **head_object**（`localfs_backend.cc:head_object`）：先查元数据缓存（§5.1）：
  命中且戳校验通过（或关闭校验）直接返回；否则校验 + `require_bucket` +
  `load_meta_fill`（stat + xattr/sidecar 读 + 回填）。
- **delete_object**（`localfs_backend.cc:delete_object`）：幂等——`fs::remove`
  对不存在的路径返回 false 且 ec 为空，不报错；但真实失败（EACCES/EIO）必须
  上抛 500，静默 204 会让客户端误信删除已发生。顺序为先数据后 sidecar
  （崩溃中缝留下的孤儿 sidecar 由 listing 自愈，见 §6），随后自底向上删除
  已空的父目录直到 bucket 根（任一目录非空即停）。
- **copy_object_fast**（`localfs_backend.cc:copy_object_fast`）：同后端 CopyObject
  快路径。读源元数据（tier 非 local 直接返回 `nullopt` 回落流式路径）→ open 源
  → `copy_file_range` 循环拷进 staging tmp（内核内搬运，reflink 文件系统上是
  O(1) 元数据克隆）。**首字节前**失败且 errno ∈
  {EXDEV, EINVAL, ENOSYS, EOPNOTSUPP} 视为机制不可用 → `nullopt`（语义等价的
  流式路径接管）；中途失败按 IO 错误上抛；源被并发缩短（拷不满 st_size）也
  回落流式取一致快照。字节未变故 etag/size 恒等于源；REPLACE 的新
  user_meta/content_type 已由 handler 装配进 meta。提交与 PUT 完全同路
  （目标 key 的 commit_lock + `commit_object_file`）。

### 5.1 对象元数据缓存（roadmap §3.8）

`meta_cache.h:MetaCache<FsCachedMeta>`（`localfs_backend.h:FsMetaCache`）按
(bucket, key) 缓存 `ObjectMeta + TierInfo + FsMetaStamp`，戳 =
fstat/stat 观测到的 `dev/ino/size/mtime/ctime`。每条提交路径都用新 inode
rename 覆盖对象（PUT / complete / copy / tiered 的 stub 与回填），就地改标签
只动 xattr 但会推 ctime，所以**戳不符 ⇒ 记录可能陈旧**是充分条件，且对
同一 root 上另一个进程的写同样成立。

| 路径 | 行为 |
| --- | --- |
| HEAD 命中 | `meta_cache_validate=true`（默认）：先 `stat` 一次，再以"戳相等"为谓词 `lookup`——一致即返回（省掉 getxattr / sidecar 读 + TSV 解码），不一致计 `stale`、丢弃并走回填；`false`：零 syscall 直接返回 |
| GET 命中 | 恒以已打开 fd 的 `fstat` 戳为谓词（本来就要做，免费），一致直接采用，否则计 `stale` + `require_bucket` + 重读回填 |
| 未命中 | `meta_from_stat` 复用同一个 stat 结果做权威读（xattr→sidecar），读完 `insert`；回填令牌由未命中的 `lookup` 发出、**先于元数据读**，其分片在此期间发生过失效/陈旧判定则丢弃回填（关闭"读旧→写者提交并失效→回填旧记录"的竞态） |
| 写路径 | `invalidate_on_exit` RAII 守卫置于提交区起点：put / copy_object_fast / complete_multipart / delete_object / set_object_tagging，以及 xlocalfs 覆写的 put / complete / delete；协程帧局部在调用方恢复前析构，故失效先于"写已完成"被观察到，异常路径同样触发 |
| tiered 提交 | `tier_local_fs.cc` 的 `commit_stub` / `FsCacheFill::commit` 走 fs_util 原语绕过后端写路径，提交后调 `LocalFsBackend::invalidate_object_meta`（戳本就能兜住，这是让 validate=false 对进程内写者也精确） |
| 列举 | 不回填（rclone/s3fs 式全量列举会把热集冲出 LRU），只按需填充 |

配置（`registry.cc:fs_backend_opts`）：`meta_cache_entries`（默认 64K，
0 = 关）、`meta_cache_ttl`（默认 0 = 不过期）、`meta_cache_validate`
（默认 true）。`validate=false` 只适合本进程独占 root 的部署：外部进程的
覆盖在失效或 TTL 前不可见（GET 因为持 fd 校验仍会自我修复并顺带修复记录）。
指标 `lights3_meta_cache_lookups_total{result=hit|miss|stale}`（stale 是被谓词拒绝的命中，同时计入 miss）/
`lights3_meta_cache_invalidations_total` / `lights3_meta_cache_entries`
（跨后端共用族名，backend 标签区分）。测试观测口 `meta_cache_stats()`。

## 6. list_objects：排序目录遍历

`localfs_backend.cc:list_objects` 不建索引，靠**排序遍历 + 剪枝**保证输出与
"收集全部 + 全排序"逐字节一致：

- **入口剪枝**：prefix 最后一个 `/` 之前的部分直接定位起始目录
  （`<bucket>/<dir_rel>`），目录不存在即空结果；`max_keys<=0` 返回空且
  `is_truncated=false`（S3 语义）。
- **目录读取**（`localfs_backend.cc:read_dir_sorted` / `DirReader`）：对每个目录
  收集条目并按**排序键**排序——文件用文件名、子目录用 `名字+"/"`（其下所有
  key 都以该串为前缀，故中序输出等价全排序）、目录标记文件 `.lights3-dir` 的
  排序键是**空串**（还原为 key `<rel>`，恰好排在同目录其他条目之前，与全排序
  中 `a/b/ < a/b/x` 一致）。`kBucketMarker` 与 sidecar 过滤掉。
- **目录快照缓存**（`list_cache.h:DirListCache`，roadmap §3.5 ②）：翻页曾需对
  路径上每个目录重做 readdir + 全排序，页码越深越贵。目录的**名字表**由目录
  自身的 inode + mtime/ctime 唯一标识（POSIX 规定该目录内任何条目增删改名都
  更新二者），因此可缓存：读目录前先 `stat` 取 `Stamp`，命中返回共享的排序
  条目表；未命中 readdir 后插入。安全阀：只缓存 ≥ `list_cache_min_dir_entries`
  （默认 256）条的目录；总预算 `list_cache_entries`（默认 1M 条，0 关闭）LRU
  淘汰；**mtime 距 readdir 开始不足 2s 的目录不入缓存**（时间戳粒度粗时同一
  tick 内的修改无法与快照区分，即 git racy-index 规则）；Stamp 取在 readdir
  之前，读中发生的并发修改必然在下一页表现为 Stamp 失配。缓存的只有名字与类
  型，从不缓存对象元数据（元数据变化不触碰目录）。
- **翻页起点二分**（`ListWalker::first_visible`）：条目已排序，start_after 在
  本目录的位置用 `partition_point` 求得；边界前一个条目若是 start_after 的祖
  先目录（`名字/` 是其前缀）仍需下钻。命中缓存时一页的目录开销从 O(n log n)
  降到 O(log n + 页长)。
- **子树剪枝**：`ListWalker::subtree_may_match` 判断子树前缀是否与 prefix 相交、
  `start_after` 是否已大于子树内所有 key，不相交整棵不下钻；文件层面一旦排序
  越过 prefix 区间即在本目录层提前返回。
- **delimiter 分组**：回调里在 `prefix.size()` 之后找 delimiter，命中则输出
  common prefix 并设置 `skip_prefix`——组内后续 key/子树整批跳过（组前缀穿过
  子树时仍需下钻）。每个 common prefix 计 1 个配额，与 key 共享 `max_keys`。
- **截断与 next_token**：超配额时 `is_truncated=true`。若最后输出的是
  common prefix，token 取该组的**最大 key**
  （`localfs_backend.cc:max_key_with_prefix`，降序扫描第一命中即最大，同样走
  目录缓存；组被并发删空则回落组名本身），与"最后返回的 key"语义一致——下
  一页从组尾之后继续，不会重发组内 key。
- **元数据并行装载**（`LocalFsBackend::load_page_meta`，roadmap §3.5 ①）：遍历
  只收集本页 key；随后按 `list_meta_concurrency`（默认 8，受池大小与"每
  worker ≥32 key"限制）条带分给池线程 `when_all` 并行 `load_meta`（每 key 一
  次 stat + 一次 getxattr），结果按下标复原顺序。readdir 与 stat 之间被并发删
  除的 key 直接从本页消失（旧实现会让整个 LIST 报 NoSuchKey）。
- **孤儿 sidecar 自愈**：`read_dir_sorted` 用同一次 readdir 的名字集合判定
  "sidecar 有、数据文件无"（不再每个 sidecar 一次 `exists()`——那曾占遍历系
  统调用的三分之一），候选在遍历结束后交 `reap_orphan_sidecar`：**取该 key 的
  commit_lock**、复查数据仍不存在、再 unlink。PUT 的提交段（数据 rename →
  sidecar 写）持同一把锁，因此不可能误删刚落地对象的 sidecar（无 xattr 文件
  系统上那是它唯一的元数据）。剪枝与缓存命中下只覆盖真正 readdir 过的目录，
  best-effort；全量兜底见 §12。

bucket 面的三个操作也在此层：`localfs_backend.cc:create_bucket` 建目录 + 写
marker 并 **fdatasync marker、fsync bucket 目录与 root 两级目录项**（对象写
路径严格 fsync，"掉电后 bucket 消失而对象还在"的倒挂不可接受）；
`localfs_backend.cc:delete_bucket` 检查除 marker 外为空 → 删 marker → 删目录，
删目录失败说明并发写抢先落了对象，**重建 marker** 保持 bucket 可见并报
`BucketNotEmpty`（否则 bucket 从 list/exists 消失而数据残留，不可见亦不可删）；
`localfs_backend.cc:list_buckets` 扫 root 下含 marker 的目录，创建时间取 marker
的 mtime，按名排序。

## 7. Multipart

磁盘状态全部在 `<staging>/mpu/<upload_id>/`：

```text
<staging>/mpu/<upload_id>/
├── manifest            # TSV：bucket、key、content_type、一等元数据、meta.*
├── part.00001          # 分片数据（part_file_name：part.%05d）
├── part.00001.md5      # TSV：md5=<该片 ETag>；其存在 = 分片数据已持久
└── ...
```

- **create**（`localfs_backend.cc:create_multipart`）：`new_upload_id()` 建目录，
  `write_tsv` 写 manifest。一等元数据与用户元数据都进 manifest，键名与
  sidecar 同源（`kStdMetaFields`），保证 create→complete 全程存活。
- **公共校验**：`fs_util.cc:load_manifest` 先做 upload_id 格式校验
  （id 会拼进路径，格式校验兼防逃逸）再读 manifest；
  `fs_util.cc:require_upload` 核对 manifest 里的 bucket/key 与请求一致，任何
  不符一律 `NoSuchUpload`，同时把 manifest 解析回 `ObjectMeta`。
- **upload_part**（`localfs_backend.cc:upload_part`）：与 PUT 同构地流式写
  staging tmp + 增量 MD5，随后 `fsync_file(tmp)` → **先 rename 数据到
  `part.NNNNN`（同号重传 last-write-wins）→ `fsync_dir` → 后写 `.md5`**。
  顺序是关键：complete 信任 `.md5` 不重算校验和，因此 `.md5` 的存在必须蕴含
  分片数据已持久——反序在"写完 .md5 后掉电"时会留下 fsync 过的 .md5 配
  零块数据，complete 会拼出 ETag 正确、内容为零的对象。rename 失败时若目录
  已消失，说明 body 读取期间上传被并发 abort，报 `NoSuchUpload`。
- **complete**（`localfs_backend.cc:complete_multipart`）：
  1. 逐个校验声明的分片：`part.NNNNN` 存在且 `.md5` 与客户端 ETag
     （`strip_etag_quotes` 后）相等，否则 `InvalidPart`；
  2. 按声明顺序把各分片以 256KiB 块拼接进新的 staging tmp；
  3. 总 ETag = `combined_etag`（`md5(各分片 md5 二进制拼接)-N`，S3 规则），
     meta 来自 manifest；取目标 key 的 commit_lock，走与 PUT 同一个
     `commit_object_file` 原子提交；
  4. 锁外 `remove_all` mpu 目录（失败无害，超期清理兜底）。
- **abort**（`localfs_backend.cc:abort_multipart`）：`require_upload` 后
  `remove_all` 目录。
- **list_parts**（`localfs_backend.cc:list_parts`）：先只收集并排序 part 号
  （目录枚举无序），按 marker 过滤、截到 `max_parts+1`（多取一个判
  is_truncated）后才对**本页条目** stat + 读 `.md5`，分页语义统一交给
  `apply_parts_page`。
- **list_multipart_uploads**（`localfs_backend.cc:list_multipart_uploads`）：mpu 是
  全实例共享的平铺目录，归属哪个 bucket 要读 manifest 才知道，故必须全量
  扫描后按 `(key, upload_id)` 排序、`apply_uploads_page` 分页——分页省的是
  响应体与下游内存，省不掉这次扫描。
- **超期清理**（`localfs_backend.cc:cleanup_stale_uploads`）：删除 manifest
  mtime 距今超过 `LocalFsOptions::mpu_ttl_sec`（默认 7 天）的上传目录。
  构造时扫一次，之后由 `localfs_backend.cc:schedule_mpu_scan` 经 `TimerQueue`
  每 `mpu_scan_interval_sec`（默认 6h）触发一次，完成后**再重挂**——扫描
  永不重叠，慢扫描只是顺延下次触发。定时回调经 `BackgroundTaskGroup`
  spawn 协程并先 `pool_->schedule()`（目录遍历是阻塞 IO）。

## 8. 并发、取消与崩溃一致性

**线程模型**：每个操作在入口校验后第一件事就是 `co_await pool_->schedule()`
切到 IO 线程池，此后所有 posix 调用都在池线程执行（见
[../concurrency.md](../concurrency.md) §3）。协程中途唤醒点（`AsyncSemaphore`
锁获取后）会紧跟一次 `pool_->schedule()`，因为唤醒可能发生在任意线程。
GET 的 body 传输阶段每块各自 hop 一次池（`FdStreamReader::read`）。

**per-key 提交锁**：`localfs_backend.cc:commit_lock`，64 条带
`AsyncSemaphore(1)`，PUT / copy_object_fast / complete_multipart 共用。只覆盖
"条件检查 + 两次 rename"的提交段，body 读写全并发；xlocalfs 继承同一把锁。

**取消点**：`body.read` 抛异常（客户端断连、验签失败）即整个协程链回卷；
所有中间产物由 `TmpFile` RAII 兜底删除，接口契约（`backend.h`）要求
"body.read 抛错则绝不提交"在此天然成立。

**崩溃一致性小结**：

| 崩溃窗口 | 后果 |
| --- | --- |
| staging 写入中 | tmp 残留 `<staging>/put/`，无对象可见（残留文件靠部署清理，无正确性影响） |
| xattr+fsync 后、rename 前 | 同上，提交未发生 |
| rename 后、sidecar 写前（sync；async 为后台补写前） | 数据+xattr 已一致提交；仅无 xattr 文件系统上读到旧 sidecar（无 xattr 时两种模式都退回同步写，窗口同 sync） |
| lazy 模式 | 无 sidecar 可言；对象 = 数据 + xattr，单 rename 即全部 |
| delete 数据后、sidecar 删前 | 孤儿 sidecar，listing 遍历到时自愈删除，§12 的定期扫描兜底 |
| upload_part fsync 后、`.md5` 写前 | 分片数据在但无 `.md5`，complete 报 `InvalidPart`（安全方向） |
| complete 拼接中 | mpu 目录完好，可重试 complete |

持久化开关：`fs_util.cc:fsync_enabled` 读 `LIGHTS3_FSYNC`（默认开，`0`/`false`
关闭），`fsync_file`（fdatasync，失败抛错、EINVAL 忽略）、`fsync_dir`
（目录项落盘，失败静默——不该让写路径因目录不可读而失败）、`fsync_path`
全部走同一开关；xlocalfs 用 `fsync_enabled` 决定是否提交 io_uring 的 FSYNC SQE。

## 9. fs_util 原语速查

| 符号 | 作用与错误映射 |
| --- | --- |
| `fs_util.cc:next_tmp_name` | `<pid>-<steady时钟>-<原子序号>`，进程内外皆无碰撞 |
| `fs_util.h:TmpFile` | RAII：析构时关 fd、未 `committed` 则 unlink |
| `fs_util.cc:reject_reserved_key` | 保留名 → `InvalidArgument` |
| `fs_util.cc:throw_errno` | errno → `InternalError`（附 strerror） |
| `fs_util.cc:write_tsv` / `read_tsv` | TSV 原子写（tmp+fsync+rename+fsync_dir）/ 宽松读（无 TAB 行跳过） |
| `fs_util.cc:commit_object_file` | 原子提交（§3）；`CommitOptions{prepared, xattr_ok, sidecar, xattr}`，`prepared=true` 供 xlocalfs 跳过已自行完成的 xattr+落盘步骤；返回 `deferred`（async 模式由调用方补写 sidecar） |
| `fs_util.cc:finish_object_sidecar` | 按 `SidecarMode` 收尾的 sidecar 步骤（§3），xlocalfs 的 ring 提交同样调用 |
| `fs_util.h:MetaXattrPolicy` / `fs_util.cc:probe_meta_xattr` | xattr 失败计数 + fail-fast 策略 / 构造期能力探测（§2） |
| `list_cache.h:DirListCache` | 目录排序条目快照缓存（§6） |
| `meta_cache.h:MetaCache` / `localfs_backend.h:FsMetaStamp` | 对象元数据缓存 + inode 戳校验（§5.1） |
| `fs_util.cc:check_put_condition` | 条件 PUT 判定（§3 第 7 步），须持同 key 提交锁 |
| `fs_util.cc:set_meta_xattr` / `get_meta_xattr` | 元数据 xattr 写（返回是否成功；失败降级+限流告警+策略计数，`required` 下抛错）/ 读（ERANGE 重取，缺失回落 sidecar） |
| `fs_util.cc:load_object_meta`(`_stat`) | stat/复用 stat + xattr→sidecar 元数据读；缺失 → `NoSuchKey` |
| `fs_util.h:StubRace` | GET 期间对象被 stub 化的竞态信号（tiered 捕获重试） |
| `fs_util.cc:commit_stub` / `commit_cached` | tiered 的 stub 化/缓存回填提交（顺序互为镜像，见 [../tiered-storage.md](../tiered-storage.md) §5/§6） |
| `fs_util.cc:FdStreamReader` | pread 流式读（§4） |
| `fs_util.cc:part_file_name` | `part.%05d` |
| `fs_util.cc:load_manifest` / `require_upload` | mpu 校验链（§7），任何不符 → `NoSuchUpload` |

## 10. 指标与生命周期

数据面八个操作（put/get/head/delete/list/copy/upload_part/complete_mpu）在
构造期全量预注册 `lights3_localfs_ops_total` / `lights3_localfs_op_errors_total`
计数器（`localfs_backend.cc:init_metrics`；缺失序列在 Prometheus 里读作
"无数据"而非 0，全量预注册让"错误消失"与"从未有错"可区分）。延迟直方图
`lights3_localfs_op_seconds` 只挂 put/get/list——三者覆盖"盘写、盘读、目录
遍历"三种成本形态。计量用 `localfs_backend.h:LocalFsBackend::OpGuard`：协程帧内
RAII，`ok` 默认 false，任何未走到成功标记的退出路径（S3Error、errno 异常、
断连）都计为错误，无需在每个 throw 前补代码。

roadmap §3.5 追加的序列：`lights3_localfs_xattr_fallback`（gauge，§2）、
`lights3_localfs_xattr_write_failures_total`、
`lights3_localfs_orphan_sidecars_removed_total`（listing 自愈 + §12 扫描合计）、
`lights3_localfs_list_dir_cache_total{result=hit|miss}` 与
`lights3_localfs_list_dir_cache_entries`（§6 目录快照缓存）；roadmap §3.8 的
`lights3_meta_cache_lookups_total{result}` / `lights3_meta_cache_invalidations_total` /
`lights3_meta_cache_entries`（§5.1 元数据缓存，族名跨后端共用）。

`localfs_backend.cc:close` / 析构共用 `shutdown_background`：`BackgroundTaskGroup`
关门 → 锁外 cancel 两个维护定时器（mpu 清理、§12 扫描；`TimerQueue::cancel`
会等在途回调，而回调要拿组锁，顺序不能反）→ 等在途扫描与 async sidecar 补写
收尾。xlocalfs 的 override 须回链本实现。定时任务统一经
`LocalFsBackend::schedule_periodic`：完成后再重挂，永不重叠。

## 11. scrub（`run_scrub_once`，roadmap §3.1）

读路径零校验（ETag 写时算、读时从不复核）意味着静默位翻转不可发现；
`localfs_backend.cc:LocalFsBackend::run_scrub_once` 补上这一课：把 ETag 当
校验和做全量 verify。**纯只读**，发现只落 LOG + `FsScrubStats` 计数。CLI
入口 `lights3 fsck <backend>`（[../cli.md](../cli.md) §2.3）；xlocalfs 同
布局直接继承，tiered 不在覆盖面内（`skipped_stubs` 防御性兜住误配）。

遍历照 tiered `scan_once` 的先例：root 下含 bucket marker 的目录 →
`recursive_directory_iterator`，跳过 marker 与 sidecar，`.lights3-dir` 标记
文件还原为尾斜杠 key；顺带报告孤儿 sidecar（listing 会自愈但只覆盖被访问
的目录，scrub 只报不删）。每对象：

- open + fstat 后 `load_object_meta_stat` 读元数据；`tier=remote` 的 stub
  跳过（数据在云端）；无 ETag / 元数据不可读 → `unverifiable`；
- 单段 ETag：256KiB pread 循环流式重算 MD5（每块 hop 一次池线程，
  同 `FdStreamReader` 的节奏），与存储值对照；
- multipart 复合 ETag（`-N` 后缀）：按 `part_sizes` 的前缀和切段、逐段
  MD5 后过 `combined_etag` 重算——布局缺失/与 `-N` 或 st_size 不符的存量
  对象诚实计 `unverifiable`，绝不误报 mismatch；
- 失配先复查竞态：元数据按路径读而哈希用的是 fd，两者之间被并发覆盖属
  torn snapshot——重 stat 比对 inode/size/mtime，变了计 `skipped_races`，
  没变才是 `etag_mismatches`（LOG_ERROR，静默损坏实锤）。

限速与中断同 duostore scrub（`storage/scrub_throttle.h:ScrubThrottle` +
`bg_.closing()` 探测），见 [duostore-core.md §8.4](duostore-core.md)。

## 12. 孤儿 sidecar 定期扫描（`run_sidecar_sweep_once`，roadmap §3.5）

§6 的自愈只覆盖被 listing 真正 readdir 过的目录，从不被列举的目录里的残留
会永久占位。`LocalFsBackend::run_sidecar_sweep_once` 沿 mpu 超期清理的定时器
先例（`schedule_periodic`，间隔 `sidecar_scan_interval`，默认 1d，`0` 关闭）
全量遍历：root 下含 bucket marker 的目录 → `recursive_directory_iterator`，
对每个 `.lights3-meta` 判断数据文件是否存在，候选交给与 listing 同一个
`reap_orphan_sidecar`（取 key 的 commit_lock 后复查再 unlink；目录标记对象的
sidecar `<dir>/.lights3-dir.lights3-meta` 映射回 key `<dir>/` 取锁）。每 256
个条目让出一次池线程，`bg_.closing()` 时中断。删除数计入
`lights3_localfs_orphan_sidecars_removed_total`，本轮摘要 LOG_INFO。构造期不
扫（大存储上会拖慢启动），测试/工具可直接调用。
