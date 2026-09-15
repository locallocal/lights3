# 网关整体扫描（2026-09-15）

一次性的全量走查快照，**不是**新的待办底账：每条确认要做的，要么直接修掉，要么
搬进 [todo.md](todo.md)，搬走后从本文删除对应行；全部清空后本文连同索引行一起删除
（与 gaps/issues/roadmap 三份归档底账的处置方式一致）。

## 1. 范围与方法

覆盖 L1 HTTP 适配层（`src/http/`）、L2 S3 协议层（`src/s3/`）、L4 运行时
（`src/core/`），L3 存储层（`src/storage/`）只走了 localfs 写路径与公共约定。
读码为主，其中 R3 / R11 与 §7 的 Range 行为用一个**无凭证**的 builtin 实例现场跑过
（复现步骤见 §5），R2 的 `stoull` 语义单独写程序确认过。标注 **实测** 的结论有现场
证据，其余是读码推断，落地前请各自补一个用例。

R1（auth 关闭时 aws-chunked 不解帧）已修复并删除，编号留空不再复用；回归用例在
`tests/unit/test_sigv4.cc` 的 `sigv4_disabled_*` 三条。

等级：高＝可能损坏数据或绕过约束；中＝可被外部输入放大，或明显偏离 AWS 语义；
低＝加固/一致性问题。

## 2. 结论摘要

| 编号 | 位置 | 等级 | 一句话 |
| --- | --- | --- | --- |
| R2 | `s3/auth/sigv4.cc:541` | 中 | `x-amz-decoded-content-length` 用 `std::stoull`，`-1` 变 2^64-1 且不抛 |
| R3 | `s3/handlers/common.h:86` | 中 | 用户元数据无总量上限（AWS 限 2KB），8KB 可写入（**实测**） |
| R4 | `core/log.cc:232` 等 | 中 | 请求尾部有 4 处进程级锁 + 默认同步日志 |
| R5 | `s3/auth/sigv4.cc:271` | 中 | 分块解帧固定 16KiB 中转缓冲，压低所有签名流式 PUT 的吞吐 |
| R6 | `s3/service.cc:903` 等 | 中 | 每请求 4–6 次路由表全扫描 |
| R7 | `s3/metrics.cc:27` | 中 | 桶维度指标表满了不淘汰，随机桶名可把真实桶挤进 `_other` |
| R8 | `http/drivers/httplib/httplib_server.cc:343` | 低中 | 每个带 body 的请求 create+join 一个 `std::thread` |
| R9 | `core/thread_pool.cc:52` | 低 | `backlog_` 无界，"有界队列 + 背压"的实际语义需要写清 |
| R10 | `http/drivers/builtin/builtin_server.cc:482` | 低 | obs-fold 折行头未按 RFC 9112 §5.2 拒绝 |
| R11 | `http/drivers/builtin/builtin_server.cc:754` | 低 | 关连接前未 `shutdown(SHUT_WR)`，极端情况客户端只看到 RST |
| R12 | `s3/auth/sigv4.cc:774` | 低 | presigned 一律按 `UNSIGNED-PAYLOAD` 计签 |
| R13 | `config/lights3.yaml` | 低 | `/-/metrics` 默认匿名，暴露桶名与后端拓扑 |
| O1–O6 | 见 §4 | — | 纯性能项（SigV4 规范化、header 访问、id 生成、fsync、beast 每请求系统调用） |

## 3. 风险项

### R2（中）`x-amz-decoded-content-length` 解析过宽

`install_chunked_body`（`sigv4.cc:541`）用 `std::stoull`。已验证的 `stoull` 语义：

```
"-1"   -> 18446744073709551615     " 5"   -> 5
"5abc" -> 5                        "+7"   -> 7
```

驱动层对 `Content-Length` 已经有严格版本 `driver::parse_content_length`
（`http/drivers/common.h:239`，注释里专门论证过"stoull 会把 -1 变成 2^64-1"），
L2 这里却没用它。后果：`BodyReader::length()` 报出天文数字，一路流进 quota 计算
（`objects.cc:167` 的 `declared` 转 `int64_t` 后是 -1）、后端的预期长度、指标，
最后才在 EOF 处以 InvalidRequest 收场。

建议：改用 `driver::parse_content_length`，失败即 InvalidRequest。仓里其它
`std::sto*` 调用点都是守住的，可以直接抄：`sts.cc:85` 有范围检查、
`admin_fsck.cc:38` 先查字符集、`tables/rest_api.cc:282` 检查 `pos` 与正负 ——
`sigv4.cc:541` 是唯一的漏网点。

### R3（中）用户元数据无总量上限

`meta_from_headers`（`s3/handlers/common.h:86-92`）把所有 `x-amz-meta-*` 收进
`user_meta`，只逐值查 CR/LF，不限条数也不限总字节；唯一的间接上限是
`http.max_header_size`（默认 16KiB）。AWS 的限制是**用户元数据总计 2KB**。

实测：两个各 4000 字节的 `x-amz-meta-*` 被接受，落盘 sidecar 8101 字节。

影响随后端不同：localfs 走 xattr 提交，而 ext4 的单个 xattr 值不能跨 block
（约 4KB），超限就退回 sidecar 双写路径（`lights3_localfs_xattr_write_failures_total`
会涨）；duostore 的 meta KV value 同步变大；List 时每个 key 的 getxattr/读 sidecar
都变慢。

建议：在 `meta_from_headers` 里加总量闸门（默认 2KiB，可配），超限返回
`MetadataTooLarge`(400)。

### R4（中）请求尾部的进程级锁与默认同步日志

每个请求在收尾阶段至少踩到四把全局锁，其中两把是纯粹可以去掉的：

| 位置 | 每请求代价 |
| --- | --- |
| `core/log.cc:232` `Logger::access()` | 进程级 `std::mutex g_mu`，只为取一个初始化后就不再变的 logger 指针 |
| `s3/metrics.cc:57` `record_bucket_request` | 全局 `bucket_m_`，临界区是一次 map 查找 |
| `s3/ratelimit.cc:15` `admit` ×1–2 | 全局 `mu_`；且 `std::string k(key)` 每请求一次拷贝，插入路径共三次 |
| `builtin_server.cc:432,439` | `sh.m` 两次 + `std::set<int>` 的节点分配/释放各一次 |

同时 `log.async` 默认 `false`（`core/config.h:354`），即每请求的 access 行在**请求
线程上**同步格式化并写 sink，默认 sink 是 stderr —— 每请求一次 `write(2)`，且被
sink 自身的互斥量串行化。

建议，按收益排序：

1. `g_access` 改成初始化后只读的 `std::atomic<spdlog::logger*>`（或
   `shared_ptr` 的 atomic load），`Logger::access()` 无锁返回；
2. 把 `log.async: true` 作为默认，或至少在 performance-baseline 里标出"默认同步
   日志的代价"，避免基准数据把日志开销算进网关本身；
3. RateLimiter 用异构查找（`std::less<>` + `string_view`）消除命中路径的拷贝，
   并按 key 哈希分片降低竞争；
4. builtin 的 idle 标记改成连接自身的原子位 + 一个计数，不再进全局 `set`。

### R5（中）分块解帧固定 16KiB 中转缓冲

`ChunkedSigV4BodyReader::fill()`（`sigv4.cc:271-277`）先把数据读进 16KiB 栈缓冲，
再 append 到 `buf_`；`read()` 从 `buf_` memcpy 出去后 `buf_.erase(0, n)`。于是：

- 无论调用方给多大的 span（数据面是 `io_chunk_size` = 64KiB），单次 `read()` 最多
  返回 16KiB → 每 MiB 多出 3 倍的协程往返；
- 每个字节多一次 memcpy，外加 `erase(0,n)` 的 memmove。

这条路径是**签名流式 PUT 的默认路径**，代价直接落在最常见的上传上。实测（64MiB PUT、
memory 后端、builtin 驱动、Debug 构建、各 3 次）：

| 请求体 | 耗时 | 吞吐 |
| --- | --- | --- |
| 普通 body | 0.095 / 0.103 / 0.103 s | ≈ 650 MB/s |
| aws-chunked（走解帧） | 0.117 / 0.122 / 0.131 s | ≈ 545 MB/s |

即解帧本身吃掉约 20%，而它做的只是"把 chunk 头摘掉"。

建议：chunk 数据段直接读进调用方的 span，只有跨 chunk 头/尾、trailer 这些边界情形
才回落到 `buf_`；`buf_` 改成"读游标 + 定期紧缩"，去掉 `erase(0,n)`。

### R6（中）每请求多次路由表全扫描

`match_route`（`service.cc:1198`）是 35 条表项的线性扫描，且每条都要
`flag_matches` → `query_has`（对 query 数组线性查找）。dispatch 里它被调用于：
api 名判定（`service.cc:903`）、匿名 website 判定（947）、policy 判定（978）、
表桶守卫（996）、租户判定（1004），`route()` 里再一次（1385），405 分支还要再
全扫一遍拼 Allow。加上 `reject_unsupported_subresource`（24 个子资源 ×
`query_has`，`service.cc:311`）和 `enforce_query_whitelist`。

单次都不贵，但这是每个请求都付、且完全冗余的固定开销。

建议：dispatch 在拿到 bucket/key 后解析一次 `const Route*`，向下传递（policy、
tenant、guard、route 复用同一个指针）；`flag_matches` 需要的 query 查找可以在
`parse_target` 时顺手建一个小索引。

### R7（中）桶维度指标表满了不淘汰

`Metrics::bucket_slot_locked`（`s3/metrics.cc:27-31`）到 `kMaxTrackedBuckets`
之后一律折进 `_other`，**没有淘汰**。而 dispatch 末尾对所有请求（含
NoSuchBucket 的 404）无条件 `record_bucket_request(bucket)`（`service.cc:1154`）。
于是外部只要用随机桶名刷一遍，表就被垃圾占满，此后真实桶的请求/字节全部进
`_other` —— 不是内存问题（有上限），是**可观测性被一次性打瞎**且不会自愈。

建议：只对"路由到存在的桶"的请求计数（或把 4xx 里的 NoSuchBucket 排除），并给桶表
加 LRU（可以直接复用 `RateLimiter::evict_locked` 的思路）。

### R8（低中）httplib 驱动每请求一个 pump 线程

`httplib_server.cc:339-346`：每个带 body 的请求 `std::thread` create + 末尾
`join`。默认 8MiB 栈虚存，创建/销毁约几十微秒，全压在 PUT 路径上。

建议：pump 作业 post 到共享 `ThreadPool`（它本来就是为阻塞型工作准备的），或维护一个
小的 pump 线程池。注意保持 `queue->cancel()` + join 的收尾语义。

### R9（低）ThreadPool 的 backlog 无界

`enqueue_bounded`（`core/thread_pool.cc:52-55`）在 `queue_` 满时把任务放进
`backlog_`，而 `backlog_` 没有上限。`capacity_` 因此不是"队列上限"，只是"推迟开始
执行的水位"；真正的上限来自 `runtime.max_inflight_requests`（默认 1024）加上后台
任务的数量。当前实现没有正确性问题，但 `thread_pool.h:2` 的"bounded queue + 背压"
和 concurrency.md §3 的表述容易被读成硬上限。

建议：要么给 backlog 一个上限（超了让 `schedule` 抛，调用点有 co_await 能接），要么
把注释和文档改成"延迟启动式背压，硬上限在准入闸门"。

### R10（低）obs-fold 折行头未按 RFC 拒绝

builtin 的头解析（`builtin_server.cc:471-489`）对以 SP/HTAB 开头的续行没有特判：
不含冒号的续行会因 `colon == npos` 被判为 malformed（安全），含冒号的会变成一个
名字带前导空格的普通头。前置代理若按 RFC 9112 §5.2 把续行折进上一个头的值，两边对
同一请求的理解就不一致 —— 目前会因签名对不上而失败，但这属于"碰巧安全"。

建议：行首为 SP/HTAB 直接 400 + 关连接（与现有 framing 违规同样处理）。

### R11（低）关连接前未 `shutdown(SHUT_WR)`

builtin 在连接线程结束后直接 `::close(fd)`（`builtin_server.cc:754`），beast 走
`shutdown(both)`（`beast_server.cc:707`）。接收缓冲里还有未读数据时，Linux 会发
RST，客户端可能读不到已经写出去的 4xx。

实测下来风险很低：延迟 100-continue（`builtin_server.cc:151`）让大多数早拒绝根本
不收 body；无 `Expect` 时 `drain_limit`（4MiB）也能吸收掉。10MB body + 非法桶名的
两种写法都稳定拿到 400。归为加固项。

### R12（低）presigned 一律按 UNSIGNED-PAYLOAD 计签

`sigv4.cc:773-774`：只要是 presigned 就把 payload_hash 固定成 `UNSIGNED-PAYLOAD`。
`X-Amz-Content-Sha256` 在通用查询白名单里（`service.cc:370`）却不参与这个选择，
所以用真实 payload hash 预签的客户端会拿到 SignatureDoesNotMatch。与 AWS 主流行为
一致，但既然放行了这个查询参数，就应该在它出现时采用它的值。

### R13（低）`/-/metrics` 默认匿名

默认 `metrics_access: anonymous`，而指标里带桶名、每桶请求量/字节、后端名与拓扑。
生产部署应当 `metrics_access: root` 或用 `http.admin_port` 把管理面分到单独监听。
现有文档提到了这两个开关，但没有在部署清单里作为**推荐默认**出现。

## 4. 纯性能项

| 编号 | 位置 | 内容 |
| --- | --- | --- |
| O1 | `s3/auth/sigv4.cc:591-605` | canonical headers 是 O(signed × headers) 的 `ieq` 扫描，且 `split` 每次分配一串 `std::string`；`canonical_query`（66-85）对 raw query 做 decode→encode→sort，又是一轮分配。小对象高 QPS 下 SigV4 是 CPU 大头之一，值得先建一次小索引再扫 |
| O2 | 全仓 | `headers.get()` 用了 52 处，`headers.find()` 只有 1 处 —— 而 `http/model.h:43` 的注释明确写着"存在性判断/比较请用 find，get 每次拷贝值"。改造是机械的，每请求能省十几次小分配 |
| O3 | `s3/service.cc:35-52` | 每请求生成 16 字符 request-id + 48 字符 host-id（两次 `to_hex` + 两次分配）。`x-amz-id-2` 可以退化成"每进程前缀 + 每连接计数"，它只是给日志关联用的 |
| O4 | `storage/localfs/fs_util.cc:87-94` | 对象提交走 `fsync_path`（按路径重新 `open` 再 `fdatasync` 再 `close`），而 upload_part 走的是 fd 版 `fsync_file`（`localfs_backend.cc:1134`）。PUT 路径每次多一对 open/close 系统调用，两条路径的写法也不一致 |
| O5 | `http/drivers/beast/beast_server.cc:762-766` | 每请求一次 `remote_endpoint()` 系统调用 + `to_string()` 分配；keep-alive 连接上这是常量，缓存到 `Session` 即可 |
| O6 | `http/drivers/common.h:213-233` | `parse_target` 对每个 query 参数做 `substr` + 两次 percent-decode，全部落成 `std::string`；配合 O1/R6 一起改成 string_view 视图 + 延迟解码收益更整齐 |

## 5. 复现方法

R3 / R11 的现场验证（不需要凭证，端口任选）：

```bash
# 内置 YAML 子集不支持 flow 风格，按缩进块写（见 config/lights3.yaml）
mkdir -p /tmp/l3data /tmp/l3stg
cat > /tmp/noauth.yaml <<'EOF'
http:
  driver: builtin
  bind: 127.0.0.1
  port: 19123
runtime:
  io_threads: 4
auth:
  region: us-east-1
backends:
  - name: fs
    type: localfs
    root: /tmp/l3data
    staging: /tmp/l3stg
buckets:
  default: fs
EOF
./build/lights3 --config /tmp/noauth.yaml &     # 启动时会 WARN: authentication is DISABLED

curl -X PUT http://127.0.0.1:19123/bkt1                       # 建桶

BIG=$(python3 -c "print('x'*4000)")
curl -X PUT -d hi -H "x-amz-meta-a: $BIG" -H "x-amz-meta-b: $BIG" \
  http://127.0.0.1:19123/bkt1/meta1                 # R3：200，sidecar 8101 字节

head -c 10000000 /dev/urandom > /tmp/big.bin        # R11：两种写法都应拿到 400
curl -sS -o /dev/null -w '%{http_code}\n' -X PUT --data-binary @/tmp/big.bin \
  http://127.0.0.1:19123/BADNAME/obj
curl -sS -o /dev/null -w '%{http_code}\n' -X PUT -H 'Expect:' \
  --data-binary @/tmp/big.bin http://127.0.0.1:19123/BADNAME/obj
```

R2 的 `stoull` 语义用一个三行程序即可确认（`-1` → 18446744073709551615，不抛）。

## 6. 建议的推进顺序

1. **R5**（下面这条实测数据摆着，且 R1 的修复让更多部署走到这条路径上）；
2. **R2 + R3**（输入校验，各自一个小闸门 + 一条用例）；
3. **R6 + R4**（请求尾部的固定开销，改完跑一次
   `scripts/bench_matrix.sh` 对照 [performance-baseline.md](performance-baseline.md)）；
4. **R7**（可观测性自愈）；
5. 其余按等级顺延；O1–O6 建议合并进第 1、3 步一起量。

## 7. 走查中确认无问题的点

避免后来者重复排查，记几条读过并确认站得住的：

- 请求走私前置条件（CL/TE 冲突、重复 CL、非 chunked 的 TE）在四驱动统一拒绝
  （`http/drivers/common.h:293`）；
- 响应头 CR/LF 注入在 `emit_headers` 统一过滤（同上 450），beast 的直插路径也已
  收口；
- 准入 permit 绑在流式响应体的生命周期上（`http/admission.h:39`），HEAD 与小响应
  的归还路径都对；
- 取消（超时/断连/关停）的竞态协议（claim + 单次 resume）在 `ThreadPool::
  ScheduleAwaiter`、`AsyncSemaphore::Waiter`、`CancelState` 三处写法一致；
- SigV4 的 host 必签、scope/日期一致性、STS token 与 AK 的绑定关系、presigned 的
  未来时间上限都做了（`sigv4.cc:696-765`）；
- 桶名校验是唯一权威闸门且 copy-source 单独复用同一函数
  （`s3/handlers/common.h:172`）；
- 指标标签基数有上限（api 与 bucket 两个维度都有），限流表有 LRU 上限；
- `Range: bytes=-0` 返回 416（实测），`bytes=5-3` 按无效头忽略（`objects.cc:52`），
  与 RFC 9110 §14.1.1 / AWS 一致；
- 工作树（含未提交改动）Debug 增量构建通过且无编译告警
  （`cmake --build build -j 16`，半数核）。
