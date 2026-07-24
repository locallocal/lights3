# RadosDataStore：基于 Ceph/RADOS 的 DuoStore 数据存储

> 状态：C1-C2 已实现（`src/storage/duostore/rados_data_store.{h,cc}`，
> CMake option `LIGHTS3_DUOSTORE_RADOS_DATA`），C3-C4 未开始（§12）。兑现
> [duostore-backend.md](duostore-backend.md) §12 的演进承诺：data 侧换
> Ceph，实现 `IDataStore`（`src/storage/duostore/data_store.h`），数据面从
> 单机文件系统换成 RADOS 分布式对象池，副本/EC、扩容再平衡、自修复由
> Ceph 承担。客户端库 librados **C API**（系统包发现，不作 submodule，
> §9）。本文中"主文档"指 duostore-backend.md，`§N` 不带前缀时指本文档章节。

## 1. 目标与非目标

| 目标 | 说明 |
| --- | --- |
| 实现 `IDataStore` 全接口 | open_writer / open_reader / remove / rewrite_pack / close，注入组合跑 `run_backend_suite` 全绿（§11） |
| 数据面分布式 | 冗余（副本或 EC）、容量扩展、故障自愈全部下沉给 RADOS；网关侧不再管盘 |
| 布局大幅简化 | fs 版的 chunk/pack 双路径收敛为**单一 rados 对象路径**（§3.3）：无 pack、无压实、无 torn tail、无目录 fsync |
| DataRef 扩展点兑现 | 新增 `Extent::Kind::kRados`，meta 层近零改动（仅 refs 记账把 kRados 与 kChunk 同待、alloc 共号段，§3.1）——主文档 §3.1 设计承诺的直接验证 |

非目标（显式声明）：

- **不管理 Ceph 集群本身**：pool 创建、副本数/EC profile、配额、placement
  rule 是部署侧职责，后端只消费既有 pool（§10）；
- **不引入 libradosstriper**（§2 路线 B 否决论证）；
- rados 层读缓存/预取首期不做（C3 评估，§5）；
- aio 协程桥接后置：C1 同步 librados 在池线程，C3 再上 aio（§6.2）；
- 多网关数据面的分布式 pin/租约不在首期——单网关跑 GC 为部署前提，
  完备方案列 C4 评估（§8.3）。

## 2. 接入路线选型（调研结论）

把"lights3 用 Ceph 存数据"的全部可行路线摆开对比：

| 路线 | 做法 | 取舍 |
| --- | --- | --- |
| **A. librados 直连（已定）** | 实现 `IDataStore`，chunk → rados 对象 | 最贴接口：DataRef/Extent 模型与 rados 对象一一对应，定位信息完全自持；单对象写原子（§7.1）、回执即多副本持久（§4.3），一致性模型比 fs 版更强；依赖面只有一个 C 库 |
| B. libradosstriper | striper 库替我们做切片，一个逻辑对象名进出 | 切片定位信息藏在 striper 内部（首对象 xattr），与 Extent/run 编码模型冲突——DataRef 退化为单名字符串，Range 定位、multipart 零拷贝拼接（主文档 §8）全部失效；striper 内部有共享锁开销；社区维护度低。出局 |
| C. RGW（S3 网关）前置 | Ceph 起 RGW，lights3 用 cloudproxy 后端对接 | 不是 IDataStore——整层 S3 网关语义重复（认证、multipart 两遍），延迟与部署面翻倍。**现状已可用**：cloudproxy 指向 RGW endpoint 即可，零代码；作为 duostore 数据面则无意义。不属本设计，但列出供部署选型参考 |
| D. CephFS 挂载 + FsDataStore | root 指向 CephFS 挂载点，零代码 | 复用 100%，但：MDS 成为额外元数据瓶颈（duostore 自己已有 meta，纯浪费）；挂载需特权（本机无 sudo 即倒在部署第一步）；"单进程独占 root"前提在共享 fs 上名存实亡；目录 fsync / rename 语义在网络 fs 上最弱。仅作应急路线，不设计不测试 |
| E. RBD 块设备 + 本地 fs | 块设备挂载后当本地盘 | 仍要本地 fs 那一套，单挂载点无共享，兼具 A/D 两者的缺点。出局 |

选 A。B/E 出局，C/D 是"不写代码的部署替代"而非本接口的实现。

## 3. 数据模型

### 3.1 Extent 映射

```cpp
// data_ref.h 既有注释"可扩展：kRados…"的兑现
enum class Kind : uint8_t { kChunk = 0, kPack = 1, kRados = 2 };
```

| Extent 字段 | kRados 语义 |
| --- | --- |
| file_id | rados 对象名 = `c.<file_id:016x>`；`IMetaStore::alloc_file_id(kRados)` 分配，号段机制照搬（主文档 §4.5）。实现上 kRados 与 kChunk **共号段**：refs 表按裸 file_id 记账不分 kind，独立计数器会在同一 meta 上切换 data 引擎（fs↔rados）时产生跨 kind id 碰撞 |
| offset | 恒 0（对象即切片，无 pack 内偏移） |
| length | 对象字节数 |
| crc32c | 写入时计算照旧；读校验语义见 §5 |

run 编码（主文档 §4.3）无需改动：kind 字段已在 run 头；一次写入会话的
file_id 连续（号段），65 万 chunk 仍压成 1 个 run；`pack_offset` 恒 0。
multipart complete 仍是纯 meta 拼接（主文档 §8）——kRados run 与 kChunk
run 同构，**O(#parts) 零数据搬运的红利原样保留**。

### 3.2 pool 与 namespace

- **pool**（`rados_pool`，必填）：物理放置策略的边界——副本数、EC
  profile、crush rule、配额全在 pool 级由部署侧决定。本实现对
  replicated / EC pool 无差别工作（只用 write_full / read / remove /
  stat，EC 不需开 overwrite 特性，§4.1）；
- **namespace**（`rados_namespace`，默认空）：pool 内的廉价逻辑隔离，
  多 backend 实例 / 多套测试共用一个 pool 而互不可见——角色对应
  redis-meta 的 `redis_prefix`（[duostore-redis-meta.md](duostore-redis-meta.md)
  §2.1），测试隔离靠它（§11）。

### 3.3 单一路径：pack 聚合取消

fs 版引入 pack 是为避开海量小文件的 inode/目录开销（主文档 §1）。RADOS
没有这个问题：无目录层级，对象元数据是 BlueStore 内嵌 RocksDB 的一条
记录，小对象代价 = `min_alloc_size`（默认 4KiB）对齐 + 一条元数据——
无需聚合。因此：

- **所有对象（含 ≤128KiB 小对象）统一走"切片 → rados 对象"一条路径**；
  `pack_threshold` / `pack_max_size` / `pack_writers` / `pack_gc_ratio`
  在 `data: rados` 下忽略并打 WARN（§10）；
- `rewrite_pack` 恒返回 `{}`。实际上不会被调用：meta 的 `pack_stats()`
  永无 kRados 的 pack 记录，压实候选恒空，GC 压实路径天然不触发；
- 主文档 §5.3 的"未知长度先按 pack 缓冲、超阈值转 chunk"双态逻辑消失
  ——未知长度流与已知长度流同一条缓冲切片路径（§4.2）。

否决的备选：在 RADOS 上重建 pack（append 到共享对象）。三重反对：EC
pool 的 append 要求 stripe 对齐（`rados_ioctx_pool_requires_alignment2`），
路径复杂化；压实要跨对象搬运 + `swap_extents`，多网关下与业务写竞态；
收益仅省 BlueStore 每对象几 KiB 开销。复杂度/收益比出局。

### 3.4 chunk 尺寸

`rados_chunk_size` 默认 **8MiB**，与主文档同值。RADOS 侧参照系：RGW
条带 4MiB、RBD 对象 4MiB、`osd_max_object_size` 默认 128MiB（配置校验
上限）。每 chunk = 一次 write_full 往返：过小则 RTT 放大主导吞吐，过大
则缓冲内存（§4.2）与单次写尾延迟上升。8MiB 落在两侧的舒适区间，且与
fs 版 DataRef 形态一致（同一对象在两种 data store 间的 manifest 规模
同量级）。

## 4. 写路径

### 4.1 DataWriter：切片缓冲 + write_full

| 方案 | 评价 |
| --- | --- |
| **整 chunk 缓冲 + `rados_write_full`（已定）** | 缓冲满 chunk_size（或 EOF）→ 一次 write_full 写成一个不可变对象，与 fs 版"chunk 一次写成后不可变"同语义；RADOS 单对象 op 原子——**无 torn chunk**（§7.1）；write_full 幂等（重放收敛为同一内容）；replicated / EC pool 通吃，EC 不需 overwrite 特性 |
| append 分段流出（`rados_append`） | 缓冲小，但：EC pool 要求除末段外按 stripe 对齐；**结果不明的 append 不可重试**（可能重复追加 = 数据损坏，同 redis-meta §3.5 盲重试禁令的形态）；崩溃遗留半截对象需要尾部修复逻辑——恰是 §3.3 刚扔掉的 torn tail 问题借尸还魂。出局 |

### 4.2 内存上界与背压

每个活跃 writer 持一个 `chunk_size` 缓冲。总量用 `core/semaphore.h`
信号量按 **`rados_buffer_total`（默认 256MiB）** 限流：writer 首次
`write()` 时获取缓冲额度，信号量耗尽则 `co_await` 挂起——背压沿协程链
传导回 socket 读循环，主文档"全链路流式"在此的兑现形式从"零缓冲"变为
"**有界缓冲**"。默认参数下 = 32 路并发流式 PUT，其余排队；每 PUT 恒持
≤1 份额度、无嵌套获取，无死锁形态。对比 fs 版既有先例：主文档 §5.3 的
chunked 缓冲同样是"缓冲上界 × 并发数"的联动声明，本实现只是把它推广到
全部写路径并用信号量显式封顶。

小对象（总长 < chunk_size，含未知长度未超限的流）：同一路径的退化情形
——缓冲至 EOF，单对象一次 write_full，对象 length < chunk_size。

### 4.3 finish 与持久化语义

librados 写回执的含义：**全部副本（或 EC k+m 条带）已提交到持久化存储**
——BlueStore 事务落盘后才 ack（Luminous 起 ack 即 safe，
`rados_aio_wait_for_safe` 已废弃）。因此同步 `rados_write_full` 返回 0
即等价于 fs 版"fdatasync + shard 目录 fsync"，且更强（多副本）。没有
目录 fsync 的对应物需要操心。`finish()` = 最后一片 write_full 返回后组
DataRef 返回；未 finish 即析构 = 已写出的对象成为无主对象，由上层
remove 兜底或孤儿扫描回收（§8.2），与主文档 §6.1 ⑤ 的兜底路径同构。

### 4.4 失败处理与重试边界

- write_full 幂等，librados 自身对暂时不可达的 OSD/PG 内部排队重试
  （默认不设 op 超时，§6.4）——**应用层不做重试循环**；
- 配置了 `rados_op_timeout` 时，-ETIMEDOUT 返回后 op 可能仍在途生效
  （结果不明）。处置：writer 进入 failed 态、当次请求抛
  `InternalError`，**不重试不复用**该 writer；已写出对象走 remove
  兜底/孤儿扫描。结果不明只产生孤儿、不产生错数据——与 redis-meta
  §3.5 的禁令同理，但此处代价更轻（数据面无记账副作用）。

## 5. 读路径

`open_reader(ref, first, last)` 返回 `RadosExtentReader : http::BodyReader`，
结构对照 fs 版 `ExtentChainReader`（主文档 §7）：

- 构造时 [first,last] → 起始 run/extent 定位，O(#runs)，算法不变；
- `read(buf)`：`co_await pool_->schedule()` 后
  `rados_read(ioctx, oid, buf, min(buf.size, extent 剩余), extent 游标)`
  （C1 同步，§6.2）；extent 读尽推进到下一个对象。无 fd 概念——fs 版
  "懒打开 fd"变成"逐次按名读"，但 **GC 竞态窗口同构**：对象可能在读
  中途被 GC remove，-ENOENT 的暴露面与 fs 版懒打开一致，pin 表 +
  gc_grace 的保护原样适用（§8.1）。注意 fs 版"已打开 fd 不受 unlink
  影响"的 POSIX 兜底在 rados 上不存在——pin 表从"防御纵深"升格为
  "唯一防线"，gc_grace 仍作二道防线；
- 单对象读线性一致（RADOS 从 PG primary 读），无需快照语义——对象
  不可变，读到即正确；
- `verify_chunk_crc` 语义照搬：只对"从段首完整读到段尾"的 extent 校验
  crc32c，Range 命中中段不校验（主文档 §7 同款论证与默认值）。

每次 `read(buf)` 一次 RTT，粒度 = 泵送循环的 buf 尺寸。读放大可接受
（顺序读被 librados 内部流水掩盖一部分）；对象级 read-ahead（读 N 时
预发 N+1 的 aio）列 C3 与 aio 桥接一并评估，不进首期。

## 6. librados 接入

### 6.1 C API 与连接生命周期

用 **C API（`rados/librados.h`）** 而非 C++ API：C ABI 跨版本稳定；
C++ 头文件拖入 ceph 内部类型（boost 等）且 ABI 随版本漂移，与"系统包
发现"的接入方式（§9）不相容。

构造序列（后端构造时执行，失败即构造失败——fail fast，配置/环境错误
在启动期暴露）：

```text
rados_create2(&cluster, "ceph", rados_client, 0)
→ rados_conf_read_file(cluster, rados_conf)        # ceph.conf + keyring
→ rados_conf_set(...)                              # client_mount_timeout / 可选 op 超时
→ rados_connect(cluster)                           # 受 client_mount_timeout 约束
→ rados_ioctx_create(cluster, rados_pool, &ioctx)
→ rados_ioctx_set_namespace(ioctx, rados_namespace)
```

单 `rados_t` + 单 `rados_ioctx_t` 进程级共享：两者对并发 IO 线程安全，
前提是 ioctx 属性（namespace / locator / snap）创建后不再变更——本实现
恒定，声明即可。librados 内部自带 messenger 网络线程池，多池线程并发
发起同步 op 即天然并行，不需要连接池（对比 hiredis 的池化，§5.2 in
redis-meta——那是因为 redis 连接是有序单工的，rados 无此约束）。

### 6.2 线程模型：C1 同步在池线程，C3 aio 桥接

- **C1**：全部 librados 调用为同步版，`co_await pool_->schedule()` 后
  在池线程阻塞等待——与 FsDataStore 的 open/pread 切池线程同构（主文档
  §7），也是主文档 §2.2 给 IDataStore 选协程接口时预留的演进空间的
  最小实现：接口是 Task<T>，内部先用"池线程 + 同步"兑付；
- **C3**：换 `rados_aio_*` + completion 回调恢复协程，池线程不再阻塞
  等网络。纪律：completion 回调运行在 librados finisher 线程，**必须
  先 reschedule 回本进程 executor/池再继续业务逻辑**——在 ceph 内部
  线程上跑业务是借线程干私活，阻塞它会反压 librados 内部管线。接口
  零改动，纯实现内替换——这正是 IDataStore 选 Task<T> 的还本。

### 6.3 错误映射

librados 返回负 errno，统一经 `throw_rados(what, ret)`（仿 fs/rocks 版
`throw_status`：LOG_ERROR + 抛 `s3::S3Error`）：

| 来源 | 映射 |
| --- | --- |
| read -ENOENT 但 refs 在（GET） | `InternalError`(500) + 告警（数据丢失征兆——或 pin/grace 失效，主文档 §10 同款） |
| remove -ENOENT | 幂等忽略（接口契约） |
| -ENOSPC / -EDQUOT（pool full / 配额） | `InternalError`(500)；已产出对象走 remove 兜底（对应主文档 §10 ENOSPC 行） |
| -ETIMEDOUT（配置了 op 超时） | `InternalError`(500)；writer 进 failed 态（§4.4） |
| rados_connect / ioctx_create 失败 | 构造抛 `std::runtime_error`（配置错误级，启动期失败） |
| 其余负值（-EIO / -EPERM …） | `InternalError`(500) + error 日志（含 errno 名） |

### 6.4 超时

- 建连：`client_mount_timeout`（经 conf_set，默认 5s，`rados_connect`
  失败即启动失败）；
- op 超时默认**不设**（`rados_op_timeout: 0`）：librados 对暂时不可达
  的 OSD 挂起等待恢复是分布式存储的期望行为——挂起优于误报，请求级
  时长裁决交给 S3 客户端/HTTP 层；需要硬上限的部署可配
  `rados_op_timeout`（映射 `rados_osd_op_timeout`），代价是 §4.4 的
  结果不明处置生效。

### 6.5 close

`close()`：等待在途 op 收尾（C3 起 `rados_aio_flush`）→
`rados_ioctx_destroy` → `rados_shutdown`。close 后任何调用干净地抛
`InternalError`（守卫，仿 rocks 版 `db()` / redis 版 §5.5——防御纵深，
误用变 500 而非崩溃）。生命周期挂接主文档 §9 既有顺序：backend close
先停 GC，再 `data_->close()`，后 `meta_->close()`，零改动。

## 7. 一致性与崩溃模型

### 7.1 提交点不变量原样成立

主文档 §6 的根本不变量——**数据先落、meta 后提交**——逐条对照：

| 崩溃点（对照主文档 §6.2） | kRados 后果 | 回收路径 |
| --- | --- | --- |
| 数据写完、meta 未提交 | 对象在池中、refs 无记录 | 孤儿扫描：无 refs 且 mtime 逾宽限 → remove（§8.2） |
| chunk 写到一半 | **不存在半截对象**：write_full 单对象原子，要么整个新内容可见要么不可见 | 已完成的前序对象成孤儿，同上 |
| meta 提交后 | 一切一致 | 旧 DataRef 在 gcq，GC 照常 |

相对 fs 版的两处**增强**：无 torn chunk（单对象 op 原子性）；无"重启
弃用 active pack"逻辑（无 pack）。fs 版崩溃矩阵里 pack 相关行全部消失。

### 7.2 集群侧故障

- OSD 故障/网络分区：PG 降级期间 librados 阻塞等待，请求挂起（§6.4 的
  取舍）；RADOS 强一致复制，**不存在"整库回档"**——对比 redis-meta §6
  必须用 AOF 堵持久化窗口，数据面在 Ceph 侧无对应风险；
- monitor 多数派丢失：新建连接失败（启动失败），存量 op 挂起；
- 网关崩溃：在途 writer 的已写对象成孤儿 → 扫描回收；无本地状态需要
  恢复（fs 版的号段 fsync 警示属 meta 侧，与本文无关）。

### 7.3 GC 顺序铁律

主文档 §9.1 照旧：gcq 消费对 kRados extent 走 `rados_remove`（-ENOENT
幂等）→ 成功后 `ack_reclaim` 销账——先物理删后销账的顺序论证原样成立
（反序崩溃产生账外孤儿对象且孤儿扫描可兜底，但铁律不因兜底放松）。
"live_recs==0 整 pack 删"分支恒不触发（§3.3）。

## 8. GC 细节与多网关

### 8.1 pin 表

进程内 pin 表 + `gc_grace` 照搬主文档 §7。注意 §5 已述：rados 无 POSIX
"已打开 fd 不受 unlink 影响"兜底，pin 表是读侧唯一防线——单网关部署下
进程内计数即完全正确（与主文档同一论证）。

### 8.2 孤儿扫描

`rados_nobjects_list_open/next`（ioctx 已限 namespace）遍历本实例全部
对象 → 从对象名解析 file_id → `chunk_referenced(file_id) == false` 且
`rados_stat` mtime 逾 `gc_grace` → remove。反向对账（refs 在而对象缺）
同主文档 §9.3：告警计数、绝不静默删 meta。列举成本 O(namespace 内对象
数)，低频（`orphan_scan_interval` 默认 1d）可接受。

接口留位：`IDataStore` 当前无枚举方法，孤儿扫描在 fs 版同样要到 P4 才
落地——届时随 P4 一并给接口定形（如 `scan_orphans(callback)`），本文
只锁定 rados 侧实现原语。

### 8.3 多网关组合

RedisMetaStore + RadosDataStore = 主文档 §12 组合矩阵里"全分布式网关"
的兑现。逐项核对前提与缺口：

| 前提 | 状态 |
| --- | --- |
| file_id 全局唯一 | 已满足：共享 meta 的号段分配（INCRBY，redis-meta §4）天然跨网关单调 |
| meta 事务全局原子 | 已满足：Lua 脚本服务端原子（redis-meta §3.4） |
| 读侧 pin vs 他网关 GC | **缺口**：pin 表进程内，网关 A 长读期间网关 B 的 GC 可 remove 对象 → 读 -ENOENT。首期约束 + 缓解见下 |
| GC/孤儿扫描的执行者 | **需单实例执行**（配置指定哪个网关跑 GC），否则并发压实/扫描互踩 |

首期部署约束：**多网关时 GC 仅由指定的单一实例执行，且
`gc_grace` ≥ 最长预期 GET 时长**（把概率正确拉到工程可接受）。完备
方案候选（C4 评估，不承诺）：租约式延迟删除（gcq 项带最近读租约）、
`rados_lock_shared/exclusive` 对象锁（读者共享锁、GC 试排他锁）、
watch/notify 广播 pin。主文档 §12 "跨网关共享 meta 时 pin 表不再充分"
的预警在此落为具体条目。

## 9. 构建接入

### 9.1 librados 的获取：系统包发现，不作 submodule

Ceph 仓库不可 submodule 化：数 GiB 体量、小时级构建、依赖面（自带
boost/fmt/…）与本仓 `~/.local/opt/boost-1.90` 冲突风险——与 rocksdb/
hiredis 的"小而自洽"前提完全相反，出局。librados 取自：

1. 系统包：`librados-dev`（deb）/ `librados2-devel`（rpm）；
2. 用户前缀：无 sudo 环境自装到 `~/.local/opt/ceph`（本机 Boost 同款
   先例），CMake 提示变量 `LIGHTS3_RADOS_ROOT` 指过去。

发现顺序：`pkg_check_modules(RADOS rados)` 优先，失败退
`find_path(rados/librados.h)` + `find_library(rados)`（带
`LIGHTS3_RADOS_ROOT` 前缀提示）。版本要求宽松：本文用到的 API
（create2 / conf / write_full / read / remove / stat / nobjects_list /
set_namespace）Luminous（v12，2017）起全部可用，不做版本特判。

### 9.2 CMake

新增 option **`LIGHTS3_DUOSTORE_RADOS_DATA`，默认 OFF**，依赖
`LIGHTS3_DUOSTORE`。默认 OFF 的理由与 redis-meta §7.2 同款且更强：
外部服务依赖（测试需真实集群，§11），且**本机可能根本没有 librados**
——默认 ON 会把主构建绑死在系统包在场上。OFF 时配置 `data: rados` 在
`from_params` 抛 "not compiled in"。

```cmake
if(LIGHTS3_DUOSTORE_RADOS_DATA)
    find_package(PkgConfig QUIET)
    # pkg-config 优先，退 find_path/find_library（LIGHTS3_RADOS_ROOT 提示）……
    target_sources(lights3_core PRIVATE src/storage/duostore/rados_data_store.cc)
    target_compile_definitions(lights3_core PUBLIC LIGHTS3_DUOSTORE_RADOS_DATA)
    target_link_libraries(lights3_core PRIVATE ${RADOS_LIBRARIES})
endif()
```

新文件仅 `rados_data_store.{h,cc}`；`DuoStoreBackend`、各 meta store、
S3 语义层零改动——主文档 §2.1/§3.1 双接口解耦的又一次兑现（redis-meta
换 meta 侧不动 data，本文换 data 侧不动 meta）。

## 10. 配置

`DuoStoreConfig::from_params` 新增键（YAML 标量，自动收入
`BackendConfig.params`，零解析器改动，惯例同主文档 §11）：

```yaml
backends:
  - name: duodata
    type: duostore
    root: ./data/duostore          # meta/ 仍在其下；data: rados 时 chunks/ packs/ 不再产生
    data: rados                    # 默认 fs
    rados_conf: /etc/ceph/ceph.conf
    rados_client: client.lights3   # cephx 身份；keyring 经 ceph.conf 指定
    rados_pool: lights3-data       # 必填；副本/EC 策略在 pool 级由部署侧决定
    rados_namespace: ""            # 多实例/测试隔离
    rados_chunk_size: 8MiB
    rados_buffer_total: 256MiB
    rados_connect_timeout: 5s
    rados_op_timeout: 0            # 0 = 不设（默认，§6.4）
    # meta 侧键（meta_path / meta: redis …）与 GC 键（gc_* / mpu_ttl）不变
```

| 键 | 默认 | 说明 |
| --- | --- | --- |
| data | `fs` | `fs` / `rados`；未编译 `LIGHTS3_DUOSTORE_RADOS_DATA` 时选 rados → 配置错误 |
| rados_conf | `/etc/ceph/ceph.conf` | ceph.conf 路径（含 mon 地址与 keyring 引用） |
| rados_client | `client.admin` | cephx 用户；生产建议专用最小权限用户（该 pool 的 rwx） |
| rados_pool | —（data=rados 时必填） | 数据 pool |
| rados_namespace | `""` | pool 内逻辑隔离（§3.2） |
| rados_chunk_size | 8MiB | 切片粒度；校验 ≤ osd_max_object_size（§3.4） |
| rados_buffer_total | 256MiB | writer 缓冲总额度 = 并发流式 PUT 上限 × chunk_size（§4.2） |
| rados_connect_timeout | 5s | 建连超时（client_mount_timeout） |
| rados_op_timeout | 0 | 单 op 硬超时；非 0 时注意 §4.4 结果不明语义 |

`data: rados` 时 `chunk_size` / `pack_*` / `verify_chunk_crc` 的处置：
`verify_chunk_crc` 保留（语义同 §5）；`chunk_size` 被 `rados_chunk_size`
取代、`pack_*` 全部忽略并打 WARN（§3.3）。`DuoStoreConfig` 增
`data_kind` 枚举（对偶 redis-meta §8 的 `meta_kind`）；`DuoStoreBackend`
构造按 `data_kind` 分支（`#ifdef` 包裹），测试注入构造函数不动。

## 11. 测试策略

1. **真实集群的获取**（比 redis 重得多，如实声明）：redis-server 是单
   进程单二进制、测试可随手拉起；Ceph 集群要 mon + osd + cephx 密钥，
   即便 vstart/micro-osd 也依赖 ceph 全套二进制在场——**本机（无 sudo、
   无 librados）恒 SKIP**。探测协议：环境变量
   `LIGHTS3_TEST_RADOS_CONF` + `LIGHTS3_TEST_RADOS_POOL` 同时设置才跑，
   否则显式 SKIP 并打印原因（不算失败，机制同 redis-meta §9.2）。CI
   矩阵用容器化单 mon 单 osd（ceph demo 镜像）补覆盖；
2. **隔离**：每次运行生成唯一 `rados_namespace`（pid + 计数器，对应
   redis_prefix 手法），teardown 列举本 namespace 全部对象删除——多套
   测试可共用一个 pool；
3. **组合与 e2e**：注入构造
   `DuoStoreBackend(cfg, pool, RocksMetaStore, RadosDataStore)` 跑
   `run_backend_suite`（与 memory/localfs/duostore 同一语义基线）；
   `run_e2e.sh` 增 `duostore-rados` 分支（同款探测/skip），CMake 注册于
   `if(LIGHTS3_DUOSTORE_RADOS_DATA AND LIGHTS3_DRIVER_BUILTIN)`；
4. **rados 专项**：多 chunk 大对象 roundtrip 与 Range 跨 extent 边界；
   未知长度流式（EOF 落在 chunk_size 两侧各一例）；remove 幂等（双删）
   与覆盖/删除后 `run_gc_once()` 收敛（对象消失、gcq 清空）；并发 GET
   持 pin 时 GC 跳过（pin 是唯一防线，§8.1，必测）；buffer 信号量背压
   （并发 PUT > 额度数不死锁、全部完成）；孤儿路径（写对象不提交 meta
   → 扫描回收）；refs 在而对象缺的告警路径（手工 rados 删对象注入）。

## 12. 实施拆分

| 阶段 | 内容 | 可独立验收 | 状态 |
| --- | --- | --- | --- |
| C1 | CMake option + librados 发现；`kRados` 枚举与 alloc 接线；连接生命周期/错误映射/throw_rados；写路径（切片缓冲 + write_full + 信号量）与读路径与 remove；测试探测/skip 机制 | 集群在场时 `run_backend_suite` 注入组合全绿；无集群时全 SKIP 不红 | 已完成 |
| C2 | 未知长度流式收口；配置全量（校验/WARN 语义）；`e2e_duostore_rados`；rados 专项单测（§11.4 除孤儿外） | e2e + 专项绿 | 已完成（GC 变现/pin 竞态专项依赖主线 P3 的 GC worker 与 pin 表，随 P3 后补） |
| C3 | aio 协程桥接（completion → executor reschedule）+ 双缓冲流水（写第 N 片时接收 N+1）；读侧对象级 read-ahead 评估 | 同套件全绿 + 吞吐对比数据 | 未开始 |
| C4 | 孤儿扫描（随主线 P4 的接口定形）；多网关 GC 约束落地（单实例执行配置）与分布式 pin 方案评估（§8.3）；指标（op 延迟/错误计数）；文档状态头更新 | 孤儿/对账专项 + 全 ctest 矩阵绿 | 未开始 |

C1 即含完整读写：rados 版没有 pack/GC 压实等增量台阶，单一路径一步到
可用；GC 消费（gcq → rados_remove）依赖主线 P3 的 GC worker——在 P3
落地前，删除只记账不回收的行为与 fs 版 P1/P2 一致，无额外风险。
