# http（L1）评审明细

> 覆盖 `src/http/`：server / model / drivers（builtin/beast/seastar/httplib）/ pushpull。
> 跨模块问题（`percent_decode` 的 `+`）见 [README.md](README.md) §1.3。

**模块级评价**：beast 驱动基本可用（每阶段 `expires_after`、strand、延迟 100-continue
都对）。**主要风险面是 builtin 与 seastar 共用的手写解析器在 HTTP/1.1 消息边界
（framing）上的一批宽松处理**——多条可导致请求走私、连接挂死或响应错位。builtin 是
默认 driver，风险直接落在默认部署上。

## 高 —— 请求走私 / 消息边界（builtin + seastar 共有，✅已复核代表条目）

这些共用同一套逻辑（`builtin_server.cc:320-336` / `seastar_server.cc:526-548`），一一对应：

### [高 · ✅已复核] CL/TE 冲突时静默取 TE 并复用连接
`builtin_server.cc:324-336`。`if (TE==chunked) … else if (Content-Length) …`——两者同时出现时 CL 被忽略且**连接照常 keep-alive**。RFC 9112 §6.1 要求这种请求 400 拒绝或处理后强制关连接。前置代理若按 CL 断帧即经典 CL.TE 走私。

### [高 · ✅已复核] `Content-Length` / chunk size 用 `stoull` 解析，接受负号与尾部垃圾
`builtin_server.cc:330`（CL）、`:116`（chunk size `stoull(line,nullptr,16)`）。实测 `stoull("-1")`=2^64-1、`stoull("5abc")`=5、`stoull(" 10;a=b",…,16)`=16。`Content-Length: -1` → `remaining` 变 2^64-1 → 服务端"永远等 body"（seastar 无超时则连接永久挂住）；`chunk size = -1\r\n` 合法通过 → body 长度脱离声明。**建议**：全字符必须是 DIGIT/HEXDIGIT + 范围校验。

### [高 · ✅已复核] 末 chunk trailer 循环无上限，且截断 body 被报成正常 EOF
`builtin_server.cc:121-123` / `seastar_server.cc:322-325`：`while (read_line(t,1024) && !t.empty()) {}` 无条数/字节上限（不受 4MB drain 约束）→ 持续发 1KB trailer 行让循环永不退出；`read_line` 失败（对端断开）时仍设 `chunk_eof=true; return 0`，把截断 body 报成正常 EOF。

### [高] `Transfer-Encoding` 只认全等 "chunked"
`HeaderMap::ieq(*te,"chunked")` + `HeaderMap::get` 只返回第一个匹配（`model.h:33-37`）。`Transfer-Encoding: gzip, chunked`、`chunked ;x`、或两个 TE 头（`identity` 在前）都不识别为 chunked → `has_body=false` → chunked body 被当作下一个请求行解析（走私）。

### [高] 重复 `Content-Length` 头取第一个，两个不同长度不报错
断帧分歧的经典走私前置条件。

### [高] chunk 数据后不校验 CRLF
`builtin_server.cc:109-127`：读满 `chunk_left` 后回到循环读一行，任何"看似 hex"的垃圾被当下一个 chunk size；任意位置空行被无限跳过（`if (line.empty()) continue`）。

## 中

### [中 · ✅已复核] builtin 的 `Expect: 100-continue` 是即时应答而非文档承诺的延迟应答
`builtin_server.cc:340-342` 在调 handler 前就发 100 Continue，未认证请求也能先推 body（doc §3.1 要求延迟以便认证失败时不收 body 直接拒）。配套：`BodyState::need_continue` 恒为 false（延迟分支死代码）、`body_state.fd` 从未初始化（恒 -1，一旦打开延迟分支就 `send_all(-1,…)` 失败）。beast/seastar 都正确延迟。

### [中] 出站头无校验 + 不去重，跨驱动行为不一致
`drivers/common.h:82` `render_response_head` 直接拼 `k + ": " + v`，不转义、不去重（无条件追加 Content-Length/Transfer-Encoding/Connection，而前面已把 `resp.headers` 全量输出）。httplib 显式过滤了这三个头（`httplib_server.cc:164-167`），builtin/seastar 没有 → 同一 `HttpResponse` 不同驱动线上行为不同，重复 CL 即断帧漏洞（当前 L2 只用 `resp.content_length` 字段，属"待引爆"）。入站侧 `read_line` 只按 `\n` 切行剥单个 `\r`，裸 CR 可留在头名/头值（beast 会拒，两手写驱动不会）。

### [中 · ✅已复核] 定长流式响应不校验实际写出字节数与声明 Content-Length
`builtin_server.cc:370-392` / `seastar_server.cc:431-458`：`stream_body->read()` 提前返回 0（后端截断）时直接 break 并返回 true → **保持 keep-alive** → 客户端把下个响应头当作本次 body 剩余 → 响应错位。httplib 这点正确（n==0 断连）。**建议**：累计已写字节，与 content_length 不符时强制断连。

### [中] seastar 超时覆盖面严重不足（slowloris）
`seastar_server.cc:474-476`：`idle_timer` 只 arm 在请求行那次读上，随后立即 cancel——header 块、body 读、响应写全程无超时。发完请求行就停住即可长期占连接。builtin 靠 SO_RCVTIMEO/SNDTIMEO 覆盖（但那是 per-syscall 超时，慢速下载客户端会被误杀）。

### [中] builtin thread-per-connection 无上限，线程创建失败终结进程
见 README §1（框架级同类），`builtin_server.cc:201-229`：无并发上限、`std::thread` 构造抛异常穿出 `run()` → main catch → 进程退出；且异常抛出时 fd 已插入 `conns_`、`active_` 已自增 → fd 与计数双泄漏。

### [中] builtin：`run()` 返回后残余 detached 线程引用 `this` → 析构期 UAF
`builtin_server.cc:222-236`：强制断开再等 5s 仍未清零就返回，`~BuiltinServer` 一析构，残余线程还摸 `m_`/`conns_`/`active_`/`handler_`。beast 兜底是 `ioc_.stop()`（泄漏协程帧非 UAF），seastar 靠 `shared_ptr` 保命，只有 builtin 真悬空。

## 低（简列）

| 位置 | 问题 |
| --- | --- |
| builtin `accept` 循环 | 只特判 EINTR，`EMFILE/ENFILE/ECONNABORTED` 即退出监听循环永久停止 accept；seastar 同病；beast 是 warn+continue（持续 EMFILE 会忙等自旋） |
| 三驱动 shutdown 竞态 | `shutdown()` 早于 `listen()` 时 fd 还是 -1，信号被吞，`run()` 永久阻塞 |
| seastar `listen()` 快照 handler | `set_handler` 在 listen 之后调用即失效；其他驱动请求期读 `handler_`，API 时序语义不一致 |
| beast/seastar spawn_detached | 只 catch `const std::exception&`，非 std::exception 逃逸 → `Detached::unhandled_exception` = terminate |
| httplib GET+CL>0 | 给 `StringBodyReader("")`，`length()` 报 N 但立即 EOF，与契约不符 |
| `pushpull.h:63` cancel | 只 `notify_all(cv_push_)` 不唤醒 `cv_pop_`；共享组件（cloudproxy 也用），消费者先 cancel 再 pop 永久阻塞（httplib 当前用法侥幸安全） |
| beast `body_limit(none)` | 大小限制全交 L2，需确认 L2 对 PUT 有硬上限，否则无界内存/带宽入口 |

**核对无恙**：信号处理均 async-signal-safe；响应头当前可回显值（x-amz-meta-*、Content-Type）源自请求头，按行解析天然排除 LF（裸 CR 危害见"出站头"条）。
