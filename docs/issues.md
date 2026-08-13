# 问题清单（全仓库扫描 + 测试复核）

> 生成日期：2026-08-13
> 方法：并行通读 `src/core`、`src/storage`、`src/s3`、`src/http`、`src/main.cc`、`tests/` 全部源码；
> 构建并运行全部构建变体的 ctest（unit + e2e）：`build`、`build-asan`、`build-tsan`、
> `build-sqlite`、`build-redis`、`build-rados`、`build-tikv`（tikv 用 tiup playground 起真实集群）。
> 每条问题均已对照源码逐行核实，排除误报。

## 测试运行结果概览

| 构建变体 | unit | e2e | 备注 |
|---|---|---|---|
| build (RelWithDebInfo) | ✅ | ✅ 12/12 | 全绿 |
| build-tsan | ✅ | ✅ 11/11 | 全绿，无数据竞争报告 |
| build-sqlite / build-redis / build-rados | ✅ | ✅ 11/11 | 全绿 |
| build-tikv | ✅ | ✅ 11/11 | 本轮未起 tiup 集群（真实集群用例 SKIP） |
| build-asan | ✅ | ✅ 10/10 | T1 修复后全绿，T2–T9 修复后复测无报告 |

> 上表为 T1–T9 修复、T10/T11 测试补齐后（2026-08-14）的复测结果。

> mint 兼容性集（`tests/e2e/run_mint.sh`）因本机无 docker 而 SKIP，属预期。

---

## 一、确证缺陷（已逐行核实）

### ~~【高 · 已由 ASan 复现】T1. 单测 `duostore_parse_pack_owner_forms` 悬垂 string_view，ASan 构建崩溃~~ ✅ 已修复（2026-08-13）
- 位置：`tests/unit/test_duostore.cc:201-216`
- 根因：`codec::parse_pack_owner(std::string_view)` 返回的 `PackOwner` 内 `bucket/key/upload_id`
  是指向**入参字节**的 `string_view`（`src/storage/duostore/codec.h:88-92` 注释明确"指向输入串"）。
  用例把临时 `std::string(...)` 直接传入，整表达式结束时临时串析构，随后 `:203-204`
  再读 `obj.bucket/obj.key` 即访问已销毁栈对象。
- 后果：`build-asan` 下 `unit_tests` 在此处 `stack-use-after-scope` → abort，**其后所有用例不再执行**，
  ASan 回归门形同虚设。普通/ tsan 构建因不检测该类错误而"碰巧通过"。
- 修复：三处会后读 `string_view` 成员的输入串（`obj/part/legacy`）已绑到具名局部变量
  （`obj_in/part_in/legacy_in`），生命周期覆盖到断言结束。末尾几个只读 `.kind`（值成员）
  的临时串调用不构成悬垂读取，保持原样。验证：`build-asan` 下 `unit_tests` 307/307 通过。

### ~~【高】T2. DeleteObjects 批量删除绕过 per-credential prefix 策略（破坏性越权）~~ ✅ 已修复（2026-08-14）
- 位置：`src/s3/service.cc:319-333`（授权判定）+ `src/s3/handlers/objects.cc:328-407`（handler）
  + `src/s3/auth/credential_store.cc:294-304`（`allows`）
- 根因链：
  1. dispatch 按路由 scope 授权，`POST /bucket?delete` 的 `key` 为空 → `Scope::Bucket`；
  2. `CredentialPolicy::allows` 在 `key.empty()` 时**直接 `return true`，跳过 prefix 校验**
     （`credential_store.cc:300`）；
  3. `delete_objects` handler 签名 `(req, bucket)` 根本不接收 `RequestAuth`，对 XML body 里
     的每个 `<Key>` 不做任何逐 key policy 复核（`objects.cc:350-382`）。
- 后果：一个受限凭证 `{"prefixes":["logs/"],"actions":["read","delete"]}`，单对象
  `DELETE /bucket/other/secret` 会被 prefix 分支拒绝；但改用 `POST /bucket?delete` 提交
  `<Delete><Object><Key>other/secret</Key></Object></Delete>` **即可删除前缀白名单外的任意对象**。
  单删与批删授权结果不一致，构成越权删除。
- 修复：`delete_objects` 签名增加 `RequestAuth`，提交删除前对 XML 里每个 Key 走
  `auth.policy->allows(bucket, key, Action::Delete)`，拒绝的 key 在结果 XML 返回
  `<Error><Code>AccessDenied</Code>`、不执行删除。回归：单测
  `policy_prefix_batch_delete_and_listing`（test_credentials.cc）+ e2e
  "批删前缀外 key 返回 AccessDenied / 前缀外对象未被批删"。
- 严重度：**高**

### ~~【高】T3. memory 后端容量超限的 PUT 残留空指针"幽灵对象"，后续 GET 空指针崩溃~~ ✅ 已修复（2026-08-14）
- 位置：`src/storage/memory/memory_backend.cc:158-161`（put_object）、崩溃点 `:182`（get_object）；
  同型：`:274`（upload_part）、`:306-311`（complete_multipart）
- 根因：put_object 提交段先 `auto& slot = b.objects[std::string(key)];`——对新 key 会插入
  一个默认构造的 `Object`（`data` 为空 `shared_ptr`）；随后 `reserve_locked(...)` 在超过
  `max_bytes` 时抛 `SlowDown`，**幽灵条目已留在 `buckets_` 中**（`:160` 注释自认"slot 是刚插入
  的空对象"）。get_object 锁内取 `blob = it->second.data`（nullptr），锁外 `:182` 无条件
  `len = blob->size()` → 空指针解引用。
- 后果：配置 `max_bytes` 的 memory 后端达容量上限 → 对新 key 发 PUT 得 503 → 任何客户端再
  GET 该 key → **进程段错误（远程可触发）**。HEAD 会返回假元数据、list 会列出幽灵 key。
- 修复：`put_object`/`upload_part`/`complete_multipart` 改为先 `find` 算旧值、
  `reserve_locked` 通过后再 `insert_or_assign`——失败路径完全不 touch map。
  `complete_multipart` 的 `up.meta` 由 move 改为拷贝（失败时上传须保持完好可重试）。
  回归：单测 `memory_backend_capacity_no_ghost`（test_storage.cc）。
- 严重度：**高**

### ~~【中】T4. AsyncSemaphore 可取消 acquire 存在 UAF 窗口（注册取消回调后仍访问协程帧成员）~~ ✅ 已修复（2026-08-14）
- 位置：`src/core/semaphore.h:112-136`（`AcquireAwaiter::suspend_impl`），
  正确范式对照 `src/core/thread_pool.cc:139-177`
- 根因：`suspend_impl` 于 `:118` `token.on_cancel_publish(...)` 注册回调后，`:120` 起继续读取
  `token`/`sem`/`w`（均为协程帧内 awaiter 的成员/引用）。而回调 `cancel_waiter`（`:214-226`）
  一旦触发即 claim Waiter 并 `w->h.resume()`——被恢复的协程在**另一线程**从 `await_resume`
  抛 `OperationCancelled` 展开，协程帧连同 awaiter 随即可被销毁；此后原线程对帧成员的访问即 UAF。
  `ThreadPool::ScheduleAwaiter` 对同一危险有注释并把 `slot/pool/token` **全部拷入局部**后再注册回调，
  信号量版遗漏此防护。
- 后果：请求超时/断连触发的取消恰落在注册完成与后续帧访问之间（`co_await sem.acquire()` 用于
  localfs/tiered per-key commit 锁、入口限流、rados `buffer_sem_`）时内存损坏。窗口极窄，
  高并发 + 高取消率下可命中。
- 修复：比照 thread_pool 范式，注册回调前把 `&sem`/`w`/`token` 拷入局部
  （`s`/`wp`/`tok`），注册后全部走局部变量，不再触碰帧内成员。验证：ASan/TSan
  变体全量单测通过，无竞态/UAF 报告。
- 严重度：**中**（窗口窄，但属 UAF）

### ~~【中】T5. UringEngine 提交致命错误后 reaper 仍消费 CQE，解引用已销毁的 Op~~ ✅ 已修复（2026-08-14）
- 位置：`src/storage/xlocalfs/uring.cc:254-268`（`flush_locked` 致命分支 + `fail_all_locked`）、
  `:309-333`（`reap_loop`）
- 根因：`flush_locked` 遇 EINTR/EAGAIN/EBUSY 以外的 errno（如 ENOMEM）时，`fail_all_locked`
  把**全部**在途 Op（含已交内核、IO 仍在跑的）以 -EIO 唤醒，协程展开销毁其帧内 `Op`。但
  `reap_loop` 未终止：内核稍后完成真实 IO 投递 CQE，reaper `reinterpret_cast<Op*>(cqe.user_data)`
  → 写 `op->res` 并二次 `resume` 已结束协程 → 对已释放内存写入 + 二次 resume。
- 后果：xlocalfs 有在途 IO 时某次 `io_uring_enter` 返回不可重试 errno → 后续任一正常 CQE 到达 →
  UAF/崩溃。前置条件罕见。
- 修复：reaper 触碰 `*op` 之前先在锁内按 `inflight_.erase(op) > 0` 确认该 Op 仍在
  账上——被 `fail_all_locked` 唤醒过的 Op 其迟到 CQE 只摘账跳过，不写 `res`、
  不二次 resume。另：`failed_` 毒化后 reaper 不再阻塞在 GETEVENTS（改 1ms 轮询
  消化迟到 CQE），`stopped_` 置位即退出，堵住 shutdown join 等不到的路径。
- 严重度：**中**

### ~~【中】T6. memory 后端 max_bytes 闸门在全量缓冲 body 之后才检查，无法兑现 OOM 防护~~ ✅ 已修复（2026-08-14）
- 位置：`src/storage/memory/memory_backend.cc:122-131`（put_object 读 body）、`:259-267`（upload_part）
- 根因：先把整个 body 追加进内存 `std::string data`，读完 EOF 才在提交段 `reserve_locked` 判容量。
  该选项文档目的是"超限返回 503 而不是让分配器决定谁死"，但对"在途缓冲"完全不设防。
- 后果：单个超大 PUT（Content-Length 远超 max_bytes）或多个并发大 PUT，字节先全驻堆，容量检查
  尚未执行进程已被 OOM killer 杀。
- 修复：新增 `check_inflight(buffered)`，`put_object`/`upload_part` 读 body 循环内
  按"已用 + 本请求已缓冲"边读边比对 `max_bytes`，超限提前抛 SlowDown（503）。
  回归：`memory_backend_capacity_no_ghost` 用远超 max_bytes 的 body 验证提前拒绝。
- 严重度：**中**

### ~~【中】T7. dispatch 的 Bucket 级列举不按 policy prefix 过滤（租户 key 枚举）~~ ✅ 已修复（2026-08-14）
- 位置：`src/s3/handlers/list_objects.cc:27-129`、`src/s3/handlers/multipart.cc:283-334`；
  根因同 T2 的 `service.cc:319-326` + `credential_store.cc:300`
- 根因：Bucket scope 授权 `key==""` 跳过 prefix 校验；`list_objects` 用客户端可控的
  `opt.prefix = req.query_get("prefix")`，从不与 `policy.prefixes` 求交。
- 后果：prefix 受限（多租户共桶）的只读凭证发 `GET /bucket`（不带 prefix）即可枚举整桶所有 key
  （含他租户），尽管 GET 具体对象会被 prefix 拦住。`list_multipart_uploads` 同理泄露所有进行中
  upload 的 key。prefix 无法作为租户隔离边界。
- 修复：`CredentialPolicy` 新增 `allows_key`/`prefix_may_contain`；`list_objects`
  与 `list_multipart_uploads` 接收 `RequestAuth`，对结果按 policy prefixes 过滤
  （objects/uploads 逐 key，CommonPrefixes 按双向前缀关系）。分页游标仍是后端的，
  页内条目可少于 max-keys（S3 语义允许）。回归：单测
  `policy_prefix_batch_delete_and_listing` + e2e "ListObjects 不含前缀外 key"。
- 严重度：**中**

### ~~【中】T8. TLS 单测手写 joinable 线程，断言失败时栈展开析构 → std::terminate~~ ✅ 已修复（2026-08-14）
- 位置：`tests/unit/test_http_drivers.cc:955`（`http_driver_tls_round_trip`）、
  `:995`（`http_driver_tls_plaintext_client_rejected`）
- 根因：两用例 `std::thread th([&]{ srv->run(); })` 未用带析构保护的 `TestServer` 夹具。
  用例内任一 `CHECK`（含 `TlsClient` 构造里的 `SSL_connect` 检查）抛 `mini_test::Failure` 时，
  展开会析构仍 joinable 的 `th` → `std::terminate`。展开在到达 `catch(mini_test::Failure)`
  之前先析构 `th`，catch 救不了。
- 后果：任一 TLS 断言回归时不仅本例失败信息被 abort 吞掉，其后所有用例不再执行；进程 abort 使
  `RedisTestServer` 静态单例析构不运行，redis 变体下泄漏 redis-server 进程与 `/tmp/lights3-redis-test-*`。
- 修复：`TestServer` 夹具扩展可选 `tls_cert`/`tls_key` 参数，两 TLS 用例改用该
  夹具——断言失败的展开路径由析构保证 shutdown + join。
- 严重度：**中**

### ~~【中】T9. e2e "ListBuckets 按 policy 过滤"是空断言，测不到声称行为~~ ✅ 已修复（2026-08-14）
- 位置：`tests/e2e/run_e2e.sh:511-518`
- 根因：断言 `polcurl "$BASE/"` 输出不含 `<Name>otherbkt</Name>`，但 `otherbkt` 从未被创建
  （`:511` 只是对不存在的桶 GET 拿 403）。即便 ListBuckets policy 过滤整个失效，该检查也恒通过
  （唯一存在的桶 `credbkt` 恰在白名单内，白名单外无任何真实桶）。
- 后果：e2e 层对该行为的回归保护是虚假的（单测 `admin_api_policy_flow` 有真实覆盖，故不升高）。
- 修复：先用 root 建真实的 `otherbkt` 再断言受限凭证看不到，并补"白名单内可见"
  的对照断言；顺带在 e2e 补了 T2/T7 的行为断言（批删越权、列举 prefix 过滤）。
- 严重度：**中**

---

## 二、测试覆盖盲区（对照 docs/ 声称行为）

### ~~【中】T10. 四驱动的超时 / 连接上限契约完全无测试~~ ✅ 已补齐（2026-08-14）
- 对照：`src/core/config.h:38-50`（`request_timeout_sec`、`transfer_stall_timeout_sec`、
  `max_connections` 拒新连、`idle_timeout_sec` 空闲关连）与 `docs/http-adapter.md:201`
  （shutdown 须在"在途完成**或超时**"后返回）
- 盲区：`grep` 全 tests/ 对 `request_timeout` / `transfer_stall` / `max_connections` **零命中**；
  `idle_timeout` 只在夹具里被设置从未被断言；shutdown 只测了快乐路径。而这组行为的要点恰是
  "四驱动一致"（每驱动各自实现，最易各行其是）。慢客户端佯攻（每 59 秒 1 字节）正是注释点名的攻击形态。
- 已补（test_http_drivers.cc / test_admission.cc / test_service.cc）：
  - `http_driver_idle_timeout_closes_idle_connection`：空闲 keep-alive 连接在
    idle_timeout 后被关，四驱动同断言；
  - `http_driver_max_connections_rejects_excess`：超限第三连拿不到响应、已建立
    连接不受影响（builtin/beast/seastar；httplib 上限由其线程池隐式约束，
    config.h 明示语义不同，跳过）；
  - `http_driver_shutdown_waits_for_inflight_within_grace`（宽限内等在途完成、
    响应完整送达）+ `http_driver_shutdown_grace_bounds_return`（超宽限强关、
    run() 有界返回，四驱动）+ `http_driver_shutdown_force_deadline_builtin`
    （handler 睡过整个 grace+force 窗口 run() 仍到点返回；仅 builtin——
    beast/seastar 强关路径遗弃未完成 session 帧，属进程退出场景的有界泄漏，
    ASan 下不可复现执行，已在用例注释记录）；
  - `stall_guard_kills_dripping_transfer` 等 3 例：滴灌式传输在窗口后被掐断、
    窗口内推进 ≥64KiB 重置计时、EOF 不判定、window≤0 关闭；
  - `service_request_timeout_cancels_and_returns_503`：handler 执行期超时由协作式
    取消打断 → 503 SlowDown、无副作用、0 = 关闭。注：config.h 原注释声称
    "→504"与实现（503 SlowDown，SDK 可重试）不符，注释已修正。
- 严重度：**中**

### ~~【中】T11. `runtime.max_inflight_requests` 入口限流（含 Permit 系进流式响应体）无行为测试~~ ✅ 已补齐（2026-08-14）
- 对照：`docs/concurrency.md:281`（全局限流、超限排队、Permit 系在 `stream_body`、读完/断连才归还、
  关停按 `available()` 判在途）；实现在 `src/main.cc` 装配层
- 盲区：现有覆盖仅 config 解析（`test_config.cc:154`）与 metrics 渲染桩（假数据）。限流本体——
  超限排队、流式 GET 未读完时许可是否仍被占、断连时许可是否归还（泄漏一个就永久少一个额度）——
  零测试。这还是最近提交 `9b9edba` 刚改过的生命周期敏感逻辑，`main.cc` 装配路径整体不被单测触达。
- 已补：准入装配从 main.cc 抽到 `src/http/admission.h`
  （`PermitBodyReader` + `make_admission_handler`，main.cc 与单测装配同一份代码），
  新增 `tests/unit/test_admission.cc` 六例行为测试：
  - 小响应随 co_return 归还 permit；流式响应 handler 返回后 permit 仍被响应体
    占用、读到 EOF 不归还、析构（读完/断连丢弃）才归还；
  - 超限第二请求在信号量排队（dispatch 未执行），第一响应体丢弃后才放行，
    额度完整归位（断连不归还即永久 hang——点名的泄漏形态）；
  - 排队中被连接 token / 关停广播取消 → 503 SlowDown、不占额度；
  - dispatch 抛异常 permit 不泄漏（RAII 覆盖异常路径）。
  ASan/TSan 变体下全部通过，无竞态/泄漏报告。
- 严重度：**中**（新改动 + 失败模式是整体 DoS，建议优先补）

---

## 三、轻微问题

| 编号 | 位置 | 问题 | 严重度 |
|---|---|---|---|
| T12 | `src/s3/xml.cc:174-177` | 数字字符引用长度上限 `>8` 误拒 6 位十六进制补充平面码点（`&#x1F600;`），合法 XML 被 400 MalformedXML | 低 |
| T13 | `src/s3/handlers/multipart.cc:283-334` | `list_multipart_uploads` 放行了 `encoding-type` 却从不读取/编码输出（`list_objects` 是 honor 的），`encoding-type=url` 静默误答 | 低 |
| T14 | `src/storage/localfs/localfs_backend.cc:449-452` | `delete_object` 不检查 `fs::remove` 的 `ec`，EACCES/EIO 等真实失败也返回 204，且 sidecar 可能删一半致数据/元数据不一致 | 低 |
| T15 | `src/storage/tiered/tiered_backend.cc:114-129` | `TeeCacheReader` EOF 分支 `commit_cache_fill` 未捕获异常（取消/ENOSPC），客户端已收完全部数据却在收尾处得到失败，违背同函数的降级设计 | 低 |
| T16 | `src/storage/duostore/rados_data_store.cc:306-313` | `RadosChunkWriter::start_flush` 在 `make_pending()` 之后才 `alloc_`，号段分配抛异常时泄漏 `AioPending` 与 rados completion（每次失败泄漏一份） | 低 |
| T17 | `tests/unit/test_concurrency.cc:385-386` | `semaphore_acquire_is_cancellable` 用固定 20ms sleep 替代条件等待，TSan/慢机下 flake（同文件 `:411` 有正确自旋范式） | 低 |
| T18 | `tests/unit/test_credentials.cc:411,478` | 固定 `/tmp/lights3-test-creds.json` 无 pid/随机量，并行跑多个构建变体的 unit_tests 会互相覆盖 | 低 |
| T19 | `tests/unit/test_duostore_redis.cc:249,371,377` | `SCRIPT FLUSH` / `CLIENT KILL TYPE normal` 是服务器全局操作，与"外部实例靠 key 前缀隔离"约定矛盾，指向共享 redis 时打断他人并使 `reconnects_total==1` 精确断言失真 | 低 |
| T20 | `build.sh:117` | `set -u` 下空数组 `"${CMAKE_ARGS[@]}"` 在 bash < 4.4 报 unbound（同行 `CMAKE_EXTRA` 已做保护，只护了一半），老发行版可移植性隐患 | 低 |

---

## 建议处理顺序

1. ~~**T2 / T3**：可被外部触发的越权删除与远程崩溃，优先。~~ ✅
2. ~~**T1**：修好后 ASan 回归门才恢复有效，能兜住 T4/T5 这类 UAF 的将来回归。~~ ✅
3. ~~**T4 / T5**：UAF，按 thread_pool 的既有范式对齐。~~ ✅
4. ~~**T6 / T7 / T8 / T9**：DoS 面、租户隔离、测试可靠性。~~ ✅
5. ~~**T10 / T11**：补关键契约的行为测试（超时/上限、入口限流生命周期）。~~ ✅
6. **T12–T20**：择机清理。
