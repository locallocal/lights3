# 静态网站托管（桶内静态文件）

目标：把一个桶当静态站点直接对浏览器提供服务。对象读路径本身早已"网站
就绪"（Content-Type 持久化、ETag/304 条件请求、Range、Cache-Control 等，
见 docs/s3-protocol.md），缺口按三阶段补齐：

| 阶段 | 内容 | 状态 |
|------|------|------|
| ① | 按桶匿名只读（§1–§2） | **已实现** |
| ② | index / error 文档语义、匿名错误页 HTML 化（§3） | **已实现** |
| ③ | `?website` 动态 API（持久化到 `.sys`）、`x-amz-website-redirect-location`（§4） | **已实现** |
| ④ | 302 补斜杠、RedirectAllRequestsTo / RoutingRules、按桶匿名限速（§6，roadmap §2.3） | **已实现** |

## 1. 配置与匿名读语义

```yaml
website:
  - bucket: my-site          # 精确桶名，不支持 glob——通配符打错字不能悄悄公开别的桶
    index_suffix: index.html # 可选，默认 index.html；不得含 '/'（否则 "docs/" 会映射到目录之外）
    error_key: error.html    # 可选；匿名 4xx/5xx 的响应体来源，缺省用内置 HTML 页
```

列出的桶接受**匿名 GET/HEAD 对象读**：请求不带任何签名材料（既无
Authorization 头也无 presigned query 参数）时，直接以一个合成的只读
policy（仅该桶、仅 Read）进入正常授权链。除此之外一切不变：

- **只有对象级裸 GET/HEAD**。桶/服务级列举、一切写删、带 query flag 的
  操作（`?uploadId` 是 ListParts）对匿名一律 `AccessDenied`——用路由匹配
  硬性限定（flag == ""、Action::Read），不只依赖 policy。空 key 会先被
  §3 的 index 改写换成对象读，因此匿名列举在构造上不可能出现
  （`?list-type=2` 会落进 GetObject 的 query 白名单，得到 501）。
- **签名材料永远走验签**。带 Authorization 头或任何 `X-Amz-Algorithm` /
  `X-Amz-Signature` / `X-Amz-Credential` query 参数（哪怕残缺）的请求
  照常验签：坏签名必须仍是 `SignatureDoesNotMatch`，不能静默降级成匿名
  成功——那会掩盖客户端配置错误，还让过期链接"看起来能用"。
- **匿名拒绝 `response-*` 覆盖参数**（`InvalidRequest`，与 AWS 一致）：
  公开桶上一条构造链接就能在桶域名下挂任意 Content-Disposition
  （src/s3/handlers/objects.cc §5.3 注释）。
- 匿名读享受与签名读完全相同的 GET 行为：Range/206、条件请求/304、
  存储的 Content-Type 与标准元数据回显。

## 2. 边界与防护

- **显式 opt-in**：仅 website 表中的桶接受匿名；表为空即整个特性关闭。
  桶名在启动时用与用户请求相同的 `validate_bucket_name` 闸门校验，
  `.sys` 等保留名直接启动失败，不会潜伏到请求期。
- 鉴权全局关闭（未配置任何凭证）时本特性不参与——一切本来就是开放的；
  配置了 website 列表会打 WARN 提醒。
- 匿名请求的 access_key 为空（与"鉴权关闭"共用访问日志约定）。
- 放大面：匿名 GET 无签名成本，除全局 `runtime.max_inflight_requests`
  外，`website[].max_rps`（0=不限，令牌桶、burst=rps）按桶限制匿名请求
  速率——超限回**轻量 XML 503**（不取 error 文档，否则限速器保护的正是
  它要花掉的那次后端读）；签名请求不受限。

## 3. index / error 文档语义（仅匿名请求）

- **index**：key 为空（桶根，带不带尾斜杠都算）或以 `/` 结尾（目录式
  key，如 `docs/`）→ 追加 `index_suffix` 再走 GetObject。
  `GET /my-site` 与 `GET /my-site/` 都返回 `index.html`。
  `GET /my-site/docs`（无尾斜杠、key 不存在）在 `docs/<index_suffix>`
  存在时 **302 到 `/my-site/docs/`**（对齐 AWS 网站端点；roadmap §2.3），
  否则照常进 error 文档路径。
- **error**：匿名请求抛出任何 S3 错误（404/403/501/500…）时，若配置了
  `error_key`，以该对象为响应体、Content-Type 用 error 对象自己的，
  **保留原状态码**——404 用 200 包装会污染缓存、误导爬虫。error 对象
  直接从后端读取，不重入 dispatch（无递归）；对象缺失/不可读时回退
  内置极简 HTML 页（站长配错不能把 404 变成 500），并打 WARN。
- **内置错误页**：`<title>404 NoSuchKey</title>` + 转义后的错误消息
  （消息可能引用请求输入，如 query 参数名——不转义就是桶域名下的 XSS）。
- HEAD 的错误响应保留状态码与头部、不带响应体。
- 签名请求不受影响，错误仍为 XML；取消/超时路径（503 SlowDown）也
  维持 XML——那是给 SDK 的重试信号，不是页面。

## 4. 动态 API 与对象级重定向（阶段③）

- **`PUT/GET/DELETE /bucket?website`**（AWS XML 形状：`IndexDocument.Suffix`
  / `ErrorDocument.Key` / `RedirectAllRequestsTo` / `RoutingRules`，见 §6）。
  仅 root（静态凭证）可用——网站配置会把桶公开成匿名可读，这是运维决策
  而非租户决策，两级模型与 admin 面一致。校验规则与 YAML 侧完全相同；
  PUT 到不存在的桶回 `NoSuchBucket`；DELETE 幂等（无配置也是 204）。
  运维入口：`s3adm website get/set/delete <bucket>`（[cli.md §3.3](cli.md)）。
- **持久化与多实例**：动态条目写入 `.sys/website/<bucket>`（JSON，
  write-through：先存储后内存），重启自动恢复；`auth.sync_interval`
  开启周期增量同步（新增/变更拉入、消失的动态条目移除，tombstone 防
  删除与列举交错时的复活——完全照抄凭证管理模式，但**无**空表保护：
  website 表清空只会关闭匿名访问，不存在锁死风险）。YAML 静态条目
  优先于同名动态条目（WARN），且拒绝经 API 修改（405，配置文件所有）。
- **`x-amz-website-redirect-location`**：随 `kStdMetaFields` 存储/回显
  （自描述 kv 序列化，存量数据零迁移）。匿名面读到该对象即 **301 +
  Location**（不下发对象体）；签名（REST）请求照常拿对象体 + 回显头，
  与 AWS 一致。值必须以 `/`、`http://` 或 `https://` 开头（PUT 时 400
  拒绝——该值会原样落进 Location 头，自由 scheme 是注入面）。注意
  path-style 部署下 `/` 开头的目标相对**主机根**而非桶根，站内跳转要
  写成 `/bucket/key`（vhost 部署下即桶根，与 AWS 网站端点一致）。
- 501 黑名单已同步删除 `website` 子资源与该请求头两项。

## 5. 未尽事项（可选备忘）

- 寻址：各阶段在**同一端点**上触发（匿名 + website 桶即网站语义，
  path-style 可用）；严格对齐 AWS 双端点模型的独立
  `website_base_domain` 留作可选项。

## 6. 重定向与限速（阶段④，roadmap §2.3）

- **RedirectAllRequestsTo**（HostName + 可选 Protocol，XML 与 YAML
  `redirect_all_host`/`redirect_all_protocol` 双入口）：与
  IndexDocument/ErrorDocument/RoutingRules 互斥（AWS 形状）；命中的桶每个
  匿名请求都 301 到 `<protocol>://<host>/<key>`，protocol 缺省跟随请求
  scheme（`X-Forwarded-Proto`，直连为 http）。
- **RoutingRules**（XML API 专属，≤50 条，首条命中生效）：
  Condition 支持 `KeyPrefixEquals` 与 `HttpErrorCodeReturnedEquals`
  （4xx/5xx）；Redirect 支持 Protocol/HostName/ReplaceKeyPrefixWith/
  ReplaceKeyWith（两个 Replace 互斥）/HttpRedirectCode（3xx，默认 301）。
  仅带前缀条件的规则在**取对象之前**按原始 key 评估；带错误码条件的规则
  在错误发生时评估，优先于 302 补斜杠与 error 文档。HostName/Protocol 均
  缺省时生成本网关相对路径（path-style 下带 `/bucket` 前缀）。
- **按桶匿名限速**：`website[].max_rps`（YAML/JSON；AWS XML 无此字段，
  不经 `?website` API 暴露），见 §2。
