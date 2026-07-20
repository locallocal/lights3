# 对象读写流程

本文按"一个字节从 socket 到磁盘（写），再从磁盘回到 socket（读）"的视角，串联
HTTP Adapter 层（L1）、S3 Protocol 层（L2）、Storage 层（L3）三层的实际代码路径。
分层职责见 [architecture.md](architecture.md)，各层内部细节见
[http-adapter.md](http-adapter.md)/[storage-backend.md](storage-backend.md)/[s3-protocol.md](s3-protocol.md)。

贯穿全文的核心抽象只有一个：**`http::BodyReader` 流式拉接口**
（`src/http/model.h`）。请求体和响应体在三层之间都以它传递，
`read(span) → 字节数（0 = EOF）`，因此任意大小的对象上传/下载都不会整体落入内存
（MemoryBackend 除外，见 §3.4）。

## 1. 公共前置链路（读写共用）

以 builtin 驱动（`src/http/drivers/builtin/builtin_server.cc`）为例：

```text
socket 字节流
  │ ① 驱动解析请求行/头部，构造中立 HttpRequest
  │    body 不预读：包成 SocketBodyReader（定长 Content-Length 或 chunked 剥壳）
  ▼
S3Service::dispatch()                       src/s3/service.cc
  │ ② 生成 x-amz-request-id；metrics 计数起点
  │ ③ /-/healthz /-/metrics /-/readyz /-/admin/* 短路返回
  │ ④ SigV4 验签 auth_.verify(req)           src/s3/auth/sigv4.cc
  │    需要 payload 校验时把 req.body 再包一层（见 §2.1）
  │ ⑤ resolve_address()：virtual-host 或 path-style 解出 (bucket, key)
  │    '.' 开头 bucket 为内部保留名，统一拒绝（docs/credential-management.md §4.1）
  ▼
S3Service::route()                          显式分派表（docs/s3-protocol.md §2）
  │ ⑥ 先拒绝不支持的子资源（?acl 等 → 501）
  │ ⑦ 按 (method, scope, query-flag) 匹配表项 → 具体 handler 协程
  ▼
object handler                              src/s3/handlers/objects.cc
  ▼
BucketRouter::resolve(bucket)               src/storage/bucket_router.cc
  │ ⑧ 按 glob 规则（fnmatch）选后端，无匹配走 default
  ▼
IStorageBackend                             src/storage/backend.h
```

整条链路是一个 `Task<HttpResponse>` 协程链；同步驱动（builtin/httplib）在
驱动线程上 `sync_wait`，异步驱动挂到自己的事件循环（docs/concurrency.md）。
`dispatch()` 统一 catch：`S3Error` → 对应状态码 + 错误 XML，
其他异常 → 500 `InternalError`；最后补 `x-amz-request-id`/`Server` 头并打一行访问日志。

对象级路由表项（`service.cc` 中 `kRoutes` 数据面部分）：

| 请求 | handler |
| --- | --- |
| `PUT /b/k`（无 `x-amz-copy-source`） | `put_object` |
| `PUT /b/k` + `x-amz-copy-source` | `copy_object` |
| `GET /b/k` | `get_object(head_only=false)` |
| `HEAD /b/k` | `get_object(head_only=true)` |
| `DELETE /b/k` | `delete_object` |
| `PUT /b/k?partNumber&uploadId` | `upload_part`（multipart，见 §2.4） |

## 2. 写入流程（PutObject）

### 2.1 请求体的三层包装

到达后端的 `BodyReader` 可能是一个嵌套洋葱，自内向外：

1. **驱动层**：`SocketBodyReader` —— 从连接缓冲读定长 body，或做 HTTP chunked 剥壳；
   `Expect: 100-continue` 延迟到首次 `read()` 才应答，认证失败可以不收 body 直接拒绝。
2. **认证层**（`sigv4.cc`，按 `x-amz-content-sha256` 取值可选包装）：
   - hex 摘要 → `Sha256VerifyingReader`：透传数据、流式算 SHA256，EOF 时不匹配抛
     `XAmzContentSHA256Mismatch`；
   - `STREAMING-AWS4-HMAC-SHA256-PAYLOAD[-TRAILER]` → `ChunkedSigV4BodyReader`：
     aws-chunked 剥壳 + 逐 chunk 签名链验证；
   - `STREAMING-UNSIGNED-PAYLOAD-TRAILER` → 仅剥壳；
   - `UNSIGNED-PAYLOAD` → 不包装。
3. **后端**只面对最外层 reader，循环 `read()` 即可，对以上一概无感知。

### 2.2 handler：条件 PUT 与元数据提取

`S3Service::put_object`（`objects.cc:117`）：

1. `router_.resolve(bucket)` 选后端；
2. 条件请求（docs/s3-protocol.md §6）：`If-None-Match: *` 先 `head_object` 探在，存在则 412
   （防覆盖创建）；`If-Match: <etag>` 比对当前 ETag（乐观并发），对象缺失时 404；
3. `meta_from_headers()`（`handlers/common.h`）提取 `Content-Type` 与
   `x-amz-meta-*` 用户元数据；
4. `backend.put_object(bucket, key, meta, body)`，body 无则用空 `StringBodyReader` 兜底；
5. 响应仅 `ETag` 头，无 body。

### 2.3 LocalFs 后端：staging + 原子提交

`LocalFsBackend::put_object`（`localfs_backend.cc:167`）：

```text
校验 bucket/key 合法性、拒绝保留名（.meta 后缀 / bucket marker）
co_await pool_->schedule()          ← 切到磁盘 IO 线程池，之后全程阻塞 IO
require_bucket()                    ← 无 marker 则 NoSuchBucket

① 流式写 staging：
   tmp = <staging>/put/<pid>-<ts>-<seq>   （O_CREAT|O_EXCL）
   loop: body.read(64KiB) → md5.update → write(tmp)
   —— 边写边算 MD5，对象从不整体驻留内存

② commit_object_file()              fs_util.cc:84，原子提交原语
   create_directories(父目录)       失败 → key 与既有对象路径冲突（InvalidArgument）
   目标是目录 → key 与既有前缀冲突（InvalidArgument）
   先写 sidecar：<data>.meta（TSV：etag/content_type/meta.*，自身也是 tmp+rename）
   再 rename(tmp → 最终路径)        ← 提交点；rename 失败则回滚删 sidecar
```

关键约定：

- **ETag = 整体内容 MD5**（hex，不带引号存储，出口统一加引号）。
- **sidecar 先于数据文件落位**：数据文件出现即可读到完整元数据，读侧不会
  看到"有数据无 meta"的窗口；旧对象被覆盖时 sidecar 先换新也只是 meta 短暂超前。
- `TmpFile` 为 RAII：任何一步抛异常（含客户端断连使 `body.read` 抛错），
  析构时自动删除 staging 残留，`committed = true` 后才免删。
- 客户端断连由驱动层 reader 以异常上抛（契约见 docs/http-adapter.md §4），
  整个协程链回卷，不会产生半截对象——最终路径上的文件要么旧要么新。

### 2.4 变体

- **XLocalFs**（`xlocalfs_backend.cc:107`）：同一 staging/提交路径，仅把数据面
  `write` 换成 io_uring（`drain_to_tmp()`：`body.read` → `uring_->write`），
  完成续体经线程池恢复，落盘期间不占线程；提交仍回池线程同步执行。
- **Memory**（`memory_backend.cc:54`）：先不持锁流式读完 body 到 `std::string`
  并算 MD5，再加锁插入 map（对象整体驻留内存，主要用于测试）。
- **CopyObject**（`objects.cc:152`）：服务端拼管道——源后端
  `get_object()` 得到流，直接作为目标后端 `put_object()` 的 body，
  跨后端复制同样零整体缓冲；`x-amz-copy-source-if-*` 先于复制校验。
- **Multipart**（详见 docs/storage-backend.md §3.2、docs/s3-protocol.md §4）：`upload_part` 与 PUT 完全同构
  （staging 流式写 + 分片 MD5，先写 `part.NNNNN.md5` 再 rename 数据文件，
  同号重传 last-write-wins）；`complete_multipart` 校验各分片 ETag 后按声明顺序
  拼接到新 tmp，总 ETag = `md5(各分片 md5 二进制拼接)-N`，最后走同一个
  `commit_object_file` 原子提交。

## 3. 读取流程（GetObject / HeadObject）

### 3.1 handler：Range 与条件请求

`S3Service::get_object`（`objects.cc:191`）：

1. 解析 `Range: bytes=a-b / a- / -n`；malformed 时**忽略**而非报错（S3 行为），
   多段 range 不支持，同样按无 Range 处理；
2. **HEAD**：`backend.head_object()` 只取元数据 → 条件请求判定
   （`If-Match`/`If-Unmodified-Since` → 412，`If-None-Match`/`If-Modified-Since` → 304，
   优先级遵循 RFC 7232）→ 填 `ETag`/`Content-Type`/`Last-Modified`/
   `x-amz-meta-*`，`content_length = size` 但无 body；
3. **GET**：`backend.get_object(bucket, key, range)` 返回
   `ObjectStream{meta, body, range}`（`storage/backend.h:39`），body 已按 range 裁剪，
   `range` 是解析后的实际生效闭区间；
4. 条件判定在拿到流之后做（304 时丢弃流即可）；
5. 命中 Range → `206` + `Content-Range: bytes f-l/size`，`content_length = l-f+1`；
   否则 `200`，`content_length = size`；`resp.stream_body = move(stream.body)`。

### 3.2 LocalFs 后端：打开即快照

`LocalFsBackend::get_object`（`localfs_backend.cc:210`）：

```text
co_await pool_->schedule()
open(O_RDONLY)；失败时先 require_bucket() 区分 NoSuchBucket/NoSuchKey
fstat 确认普通文件
load_meta()：stat（size/mtime）+ 读 <data>.meta sidecar（etag/content_type/meta.*）
resolve_range(range, size)：解析 a-b/a-/-n 为闭区间，不可满足 → InvalidRange(416)
构造 FdBodyReader(fd, offset=f, remaining=len)   ← fd 所有权移交 reader
```

`FdBodyReader.read()` 每次先 `co_await pool_->schedule()` 再 `pread` 一块
（响应循环用 64KiB 缓冲），阻塞 IO 不占 HTTP 执行环境。因为持有 fd + 偏移
`pread`，**对象在传输中途被覆盖（rename）或删除也能读完旧文件的完整快照**；
文件被外部截断则提前 EOF。reader 析构时关 fd。

### 3.3 变体

- **XLocalFs**：`UringBodyReader` 把 `pread` 换成 io_uring 带偏移读
  （天然支持 Range），等待完成时不占线程。
- **Memory**：加锁把命中区间 `substr` 拷出来，包成 `StringBodyReader` 返回。

## 4. 响应回写（驱动层）

`HttpResponse` 的 body 二选一（`http/model.h:97`）：小响应 `small_body`（字符串），
大响应 `stream_body`（BodyReader）+ `content_length`。builtin 驱动的回写
（`builtin_server.cc:362`）：

1. `content_length` 有值 → 发 `Content-Length`；stream 无长度 → chunked；
2. HEAD 与 204/304 只发头；
3. 流式响应循环：`sync_wait(stream_body->read(64KiB))` → `send`，直到 EOF；
4. **响应头已发出后 read 抛异常**（如后端盘错）：无法改状态码，只能断连，
   客户端以字节数不足 / chunked 未终止感知失败；
5. keep-alive 复用前必须 `drain()` 请求体残余（handler 可能没读完，
   例如验签失败即拒绝）；残余过大或从未应答过 100-continue 则直接关连接。

## 5. 一图总结

```text
 写（PUT /b/k）                                读（GET /b/k）
 ───────────────                               ───────────────
 socket ──SocketBodyReader──┐                  ┌──FdBodyReader/UringBodyReader── fd
   （chunked 剥壳/100-continue）│                  │   （pread 按块，池线程/io_uring）
                             │                  │
        Sha256Verifying /    │                  │  ObjectStream{meta, body, range}
        ChunkedSigV4 reader  │  ◄─ 验签包装      │
                             ▼                  ▼
                    backend.put_object   backend.get_object
                             │                  │
              staging tmp 流式写+MD5      resp.stream_body
              sidecar → rename 原子提交         │
                             │                  ▼
                             ▼          驱动 64KiB 拉取回写
                        ETag 响应        （Content-Length / chunked）
```

设计要点回顾：

- 全链路流式：内存占用与对象大小无关（每请求 O(64KiB) 缓冲）；
- 写路径 staging + sidecar-先行 + rename，崩溃/断连不留半截对象；
- 读路径 fd 快照，读写并发互不阻塞、互不污染；
- 阻塞 IO 一律经 `pool_->schedule()` 或 io_uring 与 HTTP 执行环境隔离。
