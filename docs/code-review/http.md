# http（L1）评审明细

> 覆盖 `src/http/`：server / model / drivers（builtin/beast/seastar/httplib）/ pushpull。
> 跨模块问题（`percent_decode` 的 `+`）见 [README.md](README.md) §1.3。

**模块级评价**：beast 驱动基本可用（每阶段 `expires_after`、strand、延迟 100-continue
都对）。**主要风险面是 builtin 与 seastar 共用的手写解析器在 HTTP/1.1 消息边界
（framing）上的一批宽松处理**——多条可导致请求走私、连接挂死或响应错位。builtin 是
默认 driver，风险直接落在默认部署上。

> **2026-08-02 更新**：本文件所列问题除 beast `body_limit(none)`（确认 L2 已有限制，
> 文档化后保留）外已全部修复，各条目标注 ✅已修复。消息边界判定统一收敛到
> `drivers/common.h` 的 `parse_body_framing`/`parse_content_length`/`parse_chunk_size`，
> 四驱动接入，接受/拒绝的请求集合一致；单测新增 11 组用例覆盖走私载荷、截断流、
> 头重复与 pushpull。跨模块问题（README §1.3 `percent_decode`）不在本轮范围。

## 高 —— 请求走私 / 消息边界（builtin + seastar 共有，✅已全部修复）

这些共用同一套逻辑（`builtin_server.cc:320-336` / `seastar_server.cc:526-548`），一一对应：

### [高 · ✅已修复] CL/TE 冲突时静默取 TE 并复用连接
`builtin_server.cc:324-336`。`if (TE==chunked) … else if (Content-Length) …`——两者同时出现时 CL 被忽略且**连接照常 keep-alive**。RFC 9112 §6.1 要求这种请求 400 拒绝或处理后强制关连接。前置代理若按 CL 断帧即经典 CL.TE 走私。**修复**（2026-08-02）：`parse_body_framing` 统一判定，CL 与 TE 同现一律 400 + 关连接；builtin/seastar/httplib/beast 四驱动全部接入（beast 自身解析对此更宽松，同样在 L1 拒）；单测 `http_driver_rejects_cl_te_conflict` 断言夹带请求不被应答。

### [高 · ✅已修复] `Content-Length` / chunk size 用 `stoull` 解析，接受负号与尾部垃圾
`builtin_server.cc:330`（CL）、`:116`（chunk size `stoull(line,nullptr,16)`）。实测 `stoull("-1")`=2^64-1、`stoull("5abc")`=5、`stoull(" 10;a=b",…,16)`=16。`Content-Length: -1` → `remaining` 变 2^64-1 → 服务端"永远等 body"（seastar 无超时则连接永久挂住）；`chunk size = -1\r\n` 合法通过 → body 长度脱离声明。**修复**（2026-08-02）：`parse_content_length`（1\*DIGIT，拒符号/空白/尾部垃圾/溢出）与 `parse_chunk_size`（1\*HEXDIG，之后只允许 `;ext`，逐位累加带溢出检查）替换 stoull；单测覆盖 `-1`、`5abc`、chunk size `-1` 三种载荷。

### [高 · ✅已修复] 末 chunk trailer 循环无上限，且截断 body 被报成正常 EOF
`builtin_server.cc:121-123` / `seastar_server.cc:322-325`：`while (read_line(t,1024) && !t.empty()) {}` 无条数/字节上限（不受 4MB drain 约束）→ 持续发 1KB trailer 行让循环永不退出；`read_line` 失败（对端断开）时仍设 `chunk_eof=true; return 0`，把截断 body 报成正常 EOF。**修复**（2026-08-02）：trailer 总量限 16KiB 超限报错；`read_line` 失败改走 `fail("client disconnected in trailers")`，不再伪装成 EOF（两驱动同改）。

### [高 · ✅已修复] `Transfer-Encoding` 只认全等 "chunked"
`HeaderMap::ieq(*te,"chunked")` + `HeaderMap::get` 只返回第一个匹配（`model.h:33-37`）。`Transfer-Encoding: gzip, chunked`、`chunked ;x`、或两个 TE 头（`identity` 在前）都不识别为 chunked → `has_body=false` → chunked body 被当作下一个请求行解析（走私）。**修复**（2026-08-02）：`parse_body_framing` 遍历全部头，TE 头多于一个、或值不是全等 "chunked" 一律 400 + 关连接（本实现不解码其他编码，静默放过即走私）；单测 `rejects_non_chunked_transfer_encoding`。

### [高 · ✅已修复] 重复 `Content-Length` 头取第一个，两个不同长度不报错
断帧分歧的经典走私前置条件。**修复**（2026-08-02）：`parse_body_framing` 收集全部 CL 头，值不一致即 400 + 关连接；单测 `rejects_duplicate_content_length`。

### [高 · ✅已修复] chunk 数据后不校验 CRLF
`builtin_server.cc:109-127`：读满 `chunk_left` 后回到循环读一行，任何"看似 hex"的垃圾被当下一个 chunk size；任意位置空行被无限跳过（`if (line.empty()) continue`）。**修复**（2026-08-02）：`BodyState` 增加 `after_chunk_data` 状态，chunk 数据后必须恰好一个 CRLF（非空行即报错），空行不再被无限跳过（builtin/seastar 同改）；单测 `rejects_missing_crlf_after_chunk`、`rejects_bad_chunk_size`。

## 中

### [中 · ✅已修复] builtin 的 `Expect: 100-continue` 是即时应答而非文档承诺的延迟应答
`builtin_server.cc:340-342` 在调 handler 前就发 100 Continue，未认证请求也能先推 body（doc §3.1 要求延迟以便认证失败时不收 body 直接拒）。配套：`BodyState::need_continue` 恒为 false（延迟分支死代码）、`body_state.fd` 从未初始化（恒 -1，一旦打开延迟分支就 `send_all(-1,…)` 失败）。beast/seastar 都正确延迟。**修复**（2026-08-02）：改为延迟应答——`need_continue` 置位、`fd` 正确初始化，首次读 body 时才发 100；handler 未读 body 且从未回过 100 时不再傻等 drain，直接关连接（客户端可能根本不发 body）。

### [中 · ✅已修复] 出站头无校验 + 不去重，跨驱动行为不一致
`drivers/common.h:82` `render_response_head` 直接拼 `k + ": " + v`，不转义、不去重（无条件追加 Content-Length/Transfer-Encoding/Connection，而前面已把 `resp.headers` 全量输出）。httplib 显式过滤了这三个头（`httplib_server.cc:164-167`），builtin/seastar 没有 → 同一 `HttpResponse` 不同驱动线上行为不同，重复 CL 即断帧漏洞（当前 L2 只用 `resp.content_length` 字段，属"待引爆"）。入站侧 `read_line` 只按 `\n` 切行剥单个 `\r`，裸 CR 可留在头名/头值（beast 会拒，两手写驱动不会）。**修复**（2026-08-02）：`render_response_head` 过滤 `resp.headers` 里的 Content-Length/Transfer-Encoding/Connection/Keep-Alive（长度/编码/连接管理归驱动，与 httplib 对齐），并经 `header_emittable` 丢弃含 CR/LF/非法头名的头（响应拆分注入面）；入站侧 builtin/seastar 拒绝含裸 CR 或空头名的头行；单测 `response_headers_not_duplicated`。

### [中 · ✅已修复] 定长流式响应不校验实际写出字节数与声明 Content-Length
`builtin_server.cc:370-392` / `seastar_server.cc:431-458`：`stream_body->read()` 提前返回 0（后端截断）时直接 break 并返回 true → **保持 keep-alive** → 客户端把下个响应头当作本次 body 剩余 → 响应错位。httplib 这点正确（n==0 断连）。**修复**（2026-08-02）：builtin/seastar 累计 `written`，短于或超出声明的 Content-Length 均记错误并强制断连；单测 `truncated_stream_closes_connection` 断言连接不可复用。

### [中 · ✅已修复] seastar 超时覆盖面严重不足（slowloris）
`seastar_server.cc:474-476`：`idle_timer` 只 arm 在请求行那次读上，随后立即 cancel——header 块、body 读、响应写全程无超时。发完请求行就停住即可长期占连接。builtin 靠 SO_RCVTIMEO/SNDTIMEO 覆盖（但那是 per-syscall 超时，慢速下载客户端会被误杀）。**修复**（2026-08-02）：`SeaConn::ArmGuard` 在每个挂起的 socket 操作（读行/头块/body、响应写、flush）期间武装定时器，到点关读写两端，挂起操作以 EOF/异常醒来收尾；会话结束置 `conn.idle = nullptr` 防触碰已销毁定时器。

### [中 · ✅已修复] builtin thread-per-connection 无上限，线程创建失败终结进程
见 README §1（框架级同类），`builtin_server.cc:201-229`：无并发上限、`std::thread` 构造抛异常穿出 `run()` → main catch → 进程退出；且异常抛出时 fd 已插入 `conns_`、`active_` 已自增 → fd 与计数双泄漏。**修复**（2026-08-02）：并发连接上限 `kMaxConnections=4096` 超限拒绝新连接；`std::thread` 构造 try 包裹，失败时回滚计数、关 fd、拒绝该连接，不再穿出 `run()`。

### [中 · ✅已修复] builtin：`run()` 返回后残余 detached 线程引用 `this` → 析构期 UAF
`builtin_server.cc:222-236`：强制断开再等 5s 仍未清零就返回，`~BuiltinServer` 一析构，残余线程还摸 `m_`/`conns_`/`active_`/`handler_`。beast 兜底是 `ioc_.stop()`（泄漏协程帧非 UAF），seastar 靠 `shared_ptr` 保命，只有 builtin 真悬空。**修复**（2026-08-02）：cfg/handler/锁/计数/连接集合收进 `ConnShared`，连接线程经 `shared_ptr<ConnShared>` 持有——server 析构后残余线程自行收尾，无悬空引用（与 seastar 同构）；TSan 构建全绿。

## 低（简列）

| 位置 | 问题 | 状态 |
| --- | --- | --- |
| builtin `accept` 循环 | 只特判 EINTR，`EMFILE/ENFILE/ECONNABORTED` 即退出监听循环永久停止 accept；seastar 同病；beast 是 warn+continue（持续 EMFILE 会忙等自旋） | ✅已修复：builtin 对 EINTR/ECONNABORTED continue、EMFILE/ENFILE 退避 100ms 重试；seastar accept 异常退避重试；beast 失败退避 100ms 后 continue |
| 三驱动 shutdown 竞态 | `shutdown()` 早于 `listen()` 时 fd 还是 -1，信号被吞，`run()` 永久阻塞 | ✅已修复：builtin `run()` 入循环前查 stopping；beast/seastar `listen()` 末尾发现 stopping 已置位则补发 eventfd 信号 |
| seastar `listen()` 快照 handler | `set_handler` 在 listen 之后调用即失效；其他驱动请求期读 `handler_`，API 时序语义不一致 | ✅已修复：`set_handler` 同步更新 `core_->handler`，listen 后调用也生效 |
| beast/seastar spawn_detached | 只 catch `const std::exception&`，非 std::exception 逃逸 → `Detached::unhandled_exception` = terminate | ✅已修复：两处补 `catch (...)` |
| httplib GET+CL>0 | 给 `StringBodyReader("")`，`length()` 报 N 但立即 EOF，与契约不符 | ✅已修复：无 ContentReader 的路由收到非零 body 请求直接 400 |
| `pushpull.h:63` cancel | 只 `notify_all(cv_push_)` 不唤醒 `cv_pop_`；共享组件（cloudproxy 也用），消费者先 cancel 再 pop 永久阻塞（httplib 当前用法侥幸安全） | ✅已修复：cancel 同时唤醒 `cv_pop_`；pop 见 cancelled 且非 closed 抛异常（不伪装成正常 EOF），closed 后 cancel 不影响 EOF 语义；单测两组 |
| beast `body_limit(none)` | 大小限制全交 L2，需确认 L2 对 PUT 有硬上限，否则无界内存/带宽入口 | 保留：已核实 L2——XML 类请求经 `read_body` 限 1MiB（s3/handlers/common.h），PUT 数据面 64KiB 块流式透传不落内存；对象大小上限未设属 S3 语义决策，代码处已注释 |

**核对无恙**：信号处理均 async-signal-safe；响应头当前可回显值（x-amz-meta-*、Content-Type）源自请求头，按行解析天然排除 LF（裸 CR 危害见"出站头"条）。
