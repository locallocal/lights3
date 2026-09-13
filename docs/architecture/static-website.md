# 静态网站托管：匿名读面的设计

> 代码：`src/s3/service.cc`（dispatch 的匿名分支、`website_error_page`、重定向
> 辅助函数、`website_rate_admit`）、`src/s3/website_store.{h,cc}`（配置存储）、
> `src/s3/handlers/bucket_website.cc`（`?website` XML 编解码与校验）、
> `src/core/config.h` 的 `WebsiteBucket` / `WebsiteRoutingRule`。
> 使用手册：[usage/static-website.md](../usage/static-website.md)，本文章节编号与之
> 无对应关系。

## 1. 目标与非目标

**目标**

- 把桶作为静态站点直接对浏览器提供服务，不经过任何签名客户端；
- 对齐 AWS 网站端点的可感知行为：index / error 文档、目录补斜杠、
  `x-amz-website-redirect-location`、`RedirectAllRequestsTo`、`RoutingRules`；
- 匿名面**只能**放大"读一个对象"这一种能力，其余一切与签名请求完全一致；
- 配置可静态（YAML）也可动态（`?website` API），多网关共享后端时自动收敛。

**非目标**

- 独立的网站域名 / 双端点模型（§9）；
- bucket policy / ACL 式的通用匿名授权：匿名面是 website 特性的副产品，不是通用
  公共读；
- 服务端渲染或内容改写：网关只负责路由、状态码与 Location，对象体原样下发。

## 2. 匿名判定与授权链

对象读路径早已"网站就绪"（Content-Type 持久化、ETag/304、Range/206、Cache-Control
回显，见 [s3-protocol.md](s3-protocol.md)），所以本特性没有新的数据面：它只在
dispatch 的**验签之前**加一个判定，把符合条件的请求换成一个合成身份，然后交给和
签名请求完全相同的授权链。

**判定条件**（`S3Service::anonymous_website_read`）全部满足才算匿名：

1. website 表非空、桶名非空且在表中（服务级请求永远不匿名，ListBuckets 不能公开）；
2. 方法是 GET 或 HEAD；
3. 请求**没有任何签名材料**：无 `Authorization` 头，也无 `X-Amz-Algorithm` /
   `X-Amz-Signature` / `X-Amz-Credential` 任一 query 参数；
4. 鉴权全局开启；
5. 没有绑定到凭证的 TLS 客户端证书。

为什么在 verify 之前决定：verify 把缺失的 Authorization 视为 `AccessDenied`，匿名
必须绕开它；而鉴权关闭时 verify 放行一切，此时匿名分支不会被走到，合成的只读
policy 只会比"无限制"更严，所以"鉴权关闭本特性不参与"是自然结果，不需要特判。

**为什么带残缺签名也走验签**：一条带了过期 `X-Amz-Signature` 的 presigned 链接如果
降级成匿名成功，客户端配置错误会被掩盖，过期链接"看起来能用"。规则是：只要请求
表达了"我有身份"，就必须按身份处理，坏签名保持 `SignatureDoesNotMatch`。

**为什么绑定证书优先**：mTLS 下已绑定到凭证的证书是一个身份，不是匿名读者；更具体
的身份优先，该凭证无权读此桶就是 403。这样运维可以先铺证书绑定再切模式，不会被
匿名面绕过（[tls.md](../usage/tls.md)）。

**双重闸门**。匿名请求拿到的身份是 `VerifiedIdentity{}` 加一份合成 policy
（`buckets = {bucket}`，`readonly = true`），后面的 policy 块用与所有凭证相同的
`allows()` 路径再判一次。但 policy 不是唯一的防线：在设置 policy 之前，dispatch 用
**路由匹配**硬性限定匿名只能命中 `flag == ""` 且 `Action::Read` 的对象级路由。
两者缺一不可：

- 只靠 policy，`?uploadId`（ListParts，Action::Read）这类带 query flag 的只读操作会
  漏过去；
- 只靠路由，将来新增的对象级读路由默认就对匿名开放，而 policy 把"只此一桶、只读"
  写成了显式数据。

匿名列举在构造上不可能：空 key 先被 index 改写成对象读（§3），带 `?list-type=2`
的请求则落进 GetObject 的 query 白名单反转拿到 501，两条路都到不了 ListObjects。
`response-*` 覆盖参数对匿名一律 `InvalidRequest`：公开桶上一条构造链接就能在桶
域名下挂任意 Content-Disposition，AWS 也拒绝。

## 3. 请求处理顺序

匿名面在 dispatch 中的决策点，按发生顺序：

| 阶段 | 决策 | 结果 |
| --- | --- | --- |
| 限速 | `max_rps` 令牌桶不放行 | 503 `SlowDown`，XML，不计 `anon_read` |
| 进入匿名面 | 记 `anon_read`，保存原始 key | — |
| 前置重定向 | `RedirectAllRequestsTo`；否则第一条**无错误码条件**且前缀命中的 RoutingRule | 3xx，跳过路由与 policy |
| index 改写 | key 为空或以 `/` 结尾 | 追加 `index_suffix`，记 `index_rewrite` |
| 路由闸门 | 非裸 GET/HEAD 对象路由 | 403 |
| `response-*` | 存在覆盖参数 | 400 |
| policy | 合成只读 policy 再判一次 | 403 |
| route() | 正常对象读 | 200/206/304 … |
| 对象级重定向 | 200/206 响应带 `x-amz-website-redirect-location` | 301 + Location，丢弃对象体 |
| 错误阶段 ① | 第一条 `HttpErrorCodeReturnedEquals` 等于本次状态码且前缀命中的 RoutingRule | 3xx |
| 错误阶段 ② | `NoSuchKey` 且原始 key 非空、不以 `/` 结尾、`<key>/<index_suffix>` 存在 | 302 到 `<key>/` |
| 错误阶段 ③ | 其余错误 | error 文档或内置页，状态码不变 |

几个顺序上的取舍：

- **前置规则按原始 key 评估**，在 index 改写之前：AWS 的 `KeyPrefixEquals` 语义是
  针对用户请求的路径，不是改写后的对象名。错误阶段的规则同样用原始 key。
- **错误码规则先于补斜杠与 error 文档**：显式配置优先于默认行为，否则 SPA 常见的
  "404 → index.html" 规则会被补斜杠抢走。
- **补斜杠只对 `NoSuchKey`**，并且要真的探到目录索引存在（一次 `head_object`）才
  跳转：探测失败的任何异常都保留原错误，不把一次瞬时后端错误放大成误导性的 302。
- **对象级重定向在 route() 之后**：值是对象元数据的一部分，只有读到对象才知道；
  签名请求走同一 route()，但不进这一步，拿到对象体并回显该头，与 AWS 一致。
- **取消/超时路径保持 XML**：503 `SlowDown` 是给 SDK 的重试信号，不是页面；限速
  同理，而且限速拒绝若还去取 error 文档，就花掉了限速器本来要保护的那次后端读。

## 4. 配置存储：WebsiteStore

`WebsiteStore` 照抄 `CredentialStore` 的三段式（静态 / 动态 / 周期同步，见
[credential-management.md §10.3](credential-management.md)），只删掉不适用的部分。

- **静态条目**来自 YAML，启动时经 `validate_bucket_name` 校验（`.sys` 等保留名直接
  启动失败），经 API 修改回 405：配置文件是它们的唯一真相源。
- **动态条目**经 `PUT ?website` 写入 `.sys/website/<bucket>`（JSON），
  **write-through**：先存储后内存，崩溃时以存储为准。同名冲突静态优先并 WARN。
- **快照**：`snapshot()` 返回不可变的 `shared_ptr<const vector<WebsiteBucket>>`，
  dispatch 在请求开始时取一份并持有到结束，`anon_site` 指针指向快照内部；并发的
  PUT/DELETE 只是换掉当前快照，不会让进行中的请求悬垂。
- **周期同步**：`auth.sync_interval` 开启后重列 `.sys/website/`，新增/变更拉入，
  存储里消失的动态条目从内存移除；本地刚删除的条目记 tombstone，避免与列举交错时
  复活。与凭证同步的差别是**没有空表保护**：website 表清空只会关闭匿名访问，不存在
  把所有人锁在外面的风险。
- **启动容错**：`.sys` 缺失视为空表；某个 JSON 对象损坏则跳过并 WARN，最坏是一个
  站点回 403，不能因此阻止进程启动（凭证损坏才值得启动失败）。
- **无后端装配**（单测 / 纯静态部署）用 `make_static`，动态 API 回 `InvalidRequest`。

`?website` 只对 root（静态配置的凭证）开放，与 admin 面同一套两级模型：把桶公开
是运维决策，租户凭证即便拥有该桶也不能自行公开。校验逻辑在 XML 与 YAML 两侧
是同一套规则（`index_suffix` 非空且不含 `/`、`RedirectAllRequestsTo` 与其他元素
互斥、RoutingRules ≤ 50 条且每条 Redirect 至少改变一项），不管配置从哪个面进来
都同一条校验故事。

## 5. 错误页渲染

`website_error_page` 把匿名请求抛出的 `S3Error` 变成页面：

- **保留原状态码**。用 200 包装 404 会污染 CDN 与浏览器缓存、误导爬虫，AWS 也保留。
  错误自带的响应头（如 405 的 `Allow`）原样带上。
- **直接读后端，不重入 dispatch**：error 对象经 `router_.resolve(bucket)` 直接
  `get_object`，不再经过路由、policy 与重定向判定，因此不会递归，也不会被自身的
  RoutingRules 再次改写。
- **回退内置页**：error 对象缺失或读失败时 WARN 并下发极简 HTML，站长配错不能把
  404 变成 500。
- **XSS 转义**：内置页嵌入 `S3Error::message`，而部分消息会引用请求输入（query 参数
  名、key），不转义就是桶域名下的反射型 XSS。`html_escape` 处理 `& < > "`。
- **HEAD**：保留状态码与头部，不发响应体，Content-Length 取 error 对象的大小。
- **在 catch 之外取文档**：协程 catch 块内不能 `co_await`，所以 catch 只记录错误，
  出了 catch 再走错误阶段的三步（§3）。

## 6. 重定向的 Location 生成

所有 3xx 的 Location 由 `website_location` 统一生成，规则：

- HostName 与 Protocol 都缺省 → 相对路径，留在本网关。**path-style 下带 `/<bucket>`
  前缀**，vhost 下就是 `/<key>`。这是本实现与 AWS 唯一的可感知差异来源：AWS 网站
  端点恒为 vhost，`/` 就是桶根；本实现共用 REST 端点，path-style 下 `/` 是主机根。
- 任一非空 → 绝对 URL。Host 缺省取请求的 `Host` 头（回到本网关时保持本网关的
  寻址风格），Protocol 缺省取 `X-Forwarded-Proto`，直连为 http（scheme 只能由反向
  代理转达，直连是明文）。
- key 走 `aws_uri_encode`（不编码 `/`）：它要进响应头，不能原样带控制字符。

`x-amz-website-redirect-location` 不经上述函数，值原样进 Location，因此在 PUT 时
就限定必须以 `/`、`http://`、`https://` 开头：自由 scheme（`javascript:`、
`data:`）是注入面。该头随 `kStdMetaFields` 存储与回显，元数据序列化是自描述 kv，
存量数据零迁移。

## 7. 放大面与限速

匿名 GET 没有签名成本，一个公开桶就是免费的带宽放大器。防线分三层：

1. 全局 `runtime.max_inflight_requests` 准入闸门，所有请求一视同仁；
2. `website[].max_rps` 按桶令牌桶（容量 = 速率，首次请求满桶），只对匿名请求计数，
   签名请求不受影响；
3. 限速拒绝在进入匿名面之前判定，回轻量 XML 503，不取 error 文档，不记 `anon_read`。

`max_rps` 只在 YAML 侧可设：AWS XML 没有对应字段，硬塞进 `?website` 会破坏 SDK
兼容；动态条目的 JSON 保留该字段但 API 不暴露。

## 8. 可观测性

- `lights3_website_events_total{event}`：`anon_read` / `index_rewrite` /
  `error_document` / `redirect` / `throttled`，每个决策点各记一次，含义见
  [usage/static-website.md §7](../usage/static-website.md)。
- 精确状态码 `lights3_responses_by_status_total{status}` 用来读 206/304 比例，是
  网站/CDN 场景的关键量。
- 访问日志：匿名请求 access_key 为空，与"鉴权关闭"共用同一约定；重定向与错误页
  都是普通响应，走同一条访问行。

## 9. 未尽事项

- **独立网站端点**：AWS 用 `s3-website-<region>` 域名区分网站语义与 REST 语义，本
  实现在同一端点上以"匿名 + website 桶"触发，好处是 path-style 也能用、无需额外
  DNS；代价是 §6 的路径前缀差异。若要严格对齐，可加 `website_base_domain`，仅该
  域名下的请求进匿名面并恒以桶根为 `/`。
- **静态条目的热重载**：`website` 列表目前需重启生效；动态 API 已覆盖运行期增删，
  静态条目热重载收益有限。
