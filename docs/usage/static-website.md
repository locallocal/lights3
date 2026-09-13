# 静态网站托管

把一个桶当静态站点直接对浏览器提供服务：桶内对象可匿名 GET/HEAD，桶根与目录式
key 映射到 index 文档，4xx/5xx 用站长自定义的 error 页面应答，并支持 AWS 形状的
重定向规则与按桶匿名限速。对象读路径本身已经"网站就绪"（Content-Type 持久化、
ETag/304 条件请求、Range/206、Cache-Control 回显，见
[s3-protocol.md](../architecture/s3-protocol.md)），本特性只在其上叠加匿名面。

本文是使用手册；设计取舍与实现原理见
[architecture/static-website.md](../architecture/static-website.md)。

## 1. 前提与工作方式

- **鉴权必须开启**（配置了至少一个凭证）。鉴权全局关闭时一切请求本来就是开放的，
  本特性不参与；此时若还配置了 website 列表，启动只打一条 WARN 提醒。
- **同一端点**：网站语义与 S3 REST 共用同一个监听地址，path-style
  `http://gw/<bucket>/<key>` 与 vhost `http://<bucket>.gw/<key>` 都可用，没有独立的
  网站域名（AWS 的 `s3-website-*` 双端点模型未实现，见
  [architecture/static-website.md §9](../architecture/static-website.md)）。
- **匿名的定义**：请求不带任何签名材料，即既没有 `Authorization` 头，也没有任何
  `X-Amz-Algorithm` / `X-Amz-Signature` / `X-Amz-Credential` query 参数。带了签名材料
  （哪怕残缺或过期）的请求照常验签，不会降级成匿名。
- 只有**显式列出**的桶接受匿名访问；列表为空即整个特性关闭。

## 2. 启用方式

同一个桶的网站配置可以来自配置文件（静态条目）或运行期 API（动态条目），两者
校验规则完全相同。

### 2.1 配置文件（静态条目）

```yaml
website:
  - bucket: my-site            # 精确桶名，不支持通配
    index_suffix: index.html   # 可选，默认 index.html；不得含 '/'
    error_key: error.html      # 可选；缺省用内置错误页
    max_rps: 0                 # 可选；匿名请求/秒，0 = 不限
    redirect_all_host: ""      # 可选；非空则该桶全部匿名请求 301 到此主机
    redirect_all_protocol: ""  # 可选；http|https，空 = 跟随请求 scheme
```

- 桶名在启动时按与用户请求相同的规则校验，`.sys` 等保留名或非法名直接**启动失败**。
- 静态条目归配置文件所有：经 `?website` API 修改或删除回 405；与同名动态条目
  并存时静态条目生效并打 WARN。
- `website` 列表属于需重启生效的配置（不在
  [config-reload.md](config-reload.md) 的可热更新子集内）；运行期增删桶请用 §2.2。
- RoutingRules 不能写在 YAML 里，只能经 API 配置。

### 2.2 运行期 API（动态条目）

`PUT / GET / DELETE /<bucket>?website`，请求体与响应体为 AWS `WebsiteConfiguration`
XML。**仅 root（静态配置的凭证）可调用**：把桶公开成匿名可读是运维决策而非租户
决策，非 root 凭证回 403。

```bash
# 启用：index.html 为目录索引，error.html 为错误页
cat > site.xml <<'EOF'
<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IndexDocument><Suffix>index.html</Suffix></IndexDocument>
  <ErrorDocument><Key>error.html</Key></ErrorDocument>
</WebsiteConfiguration>
EOF
curl --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:$SK" -X PUT --data-binary @site.xml \
     "http://127.0.0.1:9000/my-site?website"

# 查看 / 删除
curl --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:$SK" "http://127.0.0.1:9000/my-site?website"
curl --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:$SK" -X DELETE "http://127.0.0.1:9000/my-site?website"
```

| 操作 | 成功 | 常见错误 |
| --- | --- | --- |
| `PUT ?website` | 200，空体 | 桶不存在 `NoSuchBucket`(404)；XML 不合法 `MalformedXML` / `InvalidArgument`(400)；静态条目 405；非 root 403 |
| `GET ?website` | 200 + XML | 未配置 `NoSuchWebsiteConfiguration`(404) |
| `DELETE ?website` | 204（幂等，未配置也是 204） | 静态条目 405；非 root 403 |

`lights3-ctl website get/set/delete <bucket>`（[cli.md §3.3](cli.md)）包装了 index /
error 两项常用配置；RedirectAllRequestsTo 与 RoutingRules 请直接提交 XML。

```bash
lights3-ctl website set my-site --index-suffix=index.html --error-key=404.html
lights3-ctl website get my-site
lights3-ctl website delete my-site
```

动态条目持久化到存储后端的 `.sys/website/<bucket>`（JSON），重启自动恢复。多网关
共享同一后端时，`auth.sync_interval` 开启周期同步：一台网关上 PUT/DELETE 的配置在
下一个同步周期内传播到其他网关（与凭证同步共用同一个开关）。

### 2.3 配置项对照

| 功能 | YAML 键 | XML 元素 | 说明 |
| --- | --- | --- | --- |
| 目录索引 | `index_suffix` | `IndexDocument.Suffix` | 非空、不含 `/`；XML 中必填（除非使用 RedirectAllRequestsTo） |
| 错误页 | `error_key` | `ErrorDocument.Key` | 可选 |
| 整桶跳转 | `redirect_all_host` / `redirect_all_protocol` | `RedirectAllRequestsTo.HostName` / `.Protocol` | 与 index / error / RoutingRules **互斥** |
| 路由规则 | 无 | `RoutingRules` | 仅 API；≤50 条 |
| 匿名限速 | `max_rps` | 无 | AWS XML 无此字段，API 不暴露；只能在 YAML 静态条目上设置 |

## 3. 匿名访问范围

列出的桶接受匿名请求时，允许的**只有对象级裸 GET/HEAD**：

| 请求 | 匿名结果 |
| --- | --- |
| `GET/HEAD /<bucket>/<key>` | 与签名读完全相同：Range/206、条件请求/304、存储的 Content-Type 与标准元数据回显 |
| `GET /<bucket>` 或 `GET /<bucket>/` | 改写为 index 文档读（§4），不会变成列举 |
| 桶/服务级列举（`?list-type=2`、`?uploads`、`GET /`） | 拒绝（403 或 501，都不是 2xx） |
| 任何写、删、multipart 操作 | `AccessDenied`(403) |
| 带 `response-content-type` 等 `response-*` 覆盖参数 | `InvalidRequest`(400)，与 AWS 一致 |
| 带 `?uploadId` 等 query flag 的操作 | `AccessDenied`(403) |
| 不在 website 列表的桶 | `AccessDenied`(403) |

另外几条规则：

- **签名材料永远验签**：坏签名或过期的 presigned 链接得到 `SignatureDoesNotMatch` /
  `AccessDenied`，不会静默变成匿名成功。
- **绑定证书优先**：mTLS 下已绑定到凭证的客户端证书按该凭证身份访问，不走匿名；
  该凭证无权读此桶就是 403（[tls.md](tls.md)）。
- **访问日志**：匿名请求的 access_key 字段为空，与"鉴权关闭"共用同一约定。

## 4. index 与 error 文档

只对匿名请求生效；签名请求的错误仍是 XML。

**index 文档**

- key 为空（桶根，带不带尾斜杠都算）或以 `/` 结尾（目录式 key，如 `docs/`）时，追加
  `index_suffix` 再读对象：`GET /my-site`、`GET /my-site/` 都返回 `index.html`，
  `GET /my-site/docs/` 返回 `docs/index.html`。
- `GET /my-site/docs`（无尾斜杠）且 `docs` 这个 key 不存在时，若 `docs/index.html`
  存在则 **302 到 `/my-site/docs/`**（对齐 AWS 网站端点）；否则按 404 走 error 文档。

**error 文档**

- 匿名请求产生任何 S3 错误（404/403/501/500 …）时，若配置了 `error_key`，以该对象
  作为响应体，Content-Type 用 error 对象自己的，**状态码保持原值**（不会把 404 包装
  成 200）。
- error 对象缺失或不可读时回退内置的极简 HTML 页并打一条 WARN，站长配错不会把 404
  变成 500。
- 内置错误页形如 `<title>404 NoSuchKey</title>` + 错误消息（已 HTML 转义）。
- HEAD 请求的错误响应保留状态码与头部、不带响应体。
- 请求被取消或超时（503 `SlowDown`）以及触发限速（§6）时仍回 XML，不取 error 文档。

## 5. 重定向

三种重定向来源，按下列顺序判定（先命中者生效）：

1. RedirectAllRequestsTo（§5.2）；
2. 只带 `KeyPrefixEquals` 条件的 RoutingRules（§5.3），在读对象**之前**按原始 key 评估；
3. 对象上的 `x-amz-website-redirect-location`（§5.1），读到对象后生效；
4. 发生错误后：带 `HttpErrorCodeReturnedEquals` 的 RoutingRules → §4 的 302 补斜杠 →
   error 文档。

### 5.1 对象级：`x-amz-website-redirect-location`

上传对象时带该请求头，值随对象元数据持久化：

```bash
curl --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:$SK" -X PUT --data-binary 'moved' \
     -H 'x-amz-website-redirect-location: /my-site/index.html' \
     "http://127.0.0.1:9000/my-site/old"
```

- 匿名读到该对象即 **301 + Location**，不下发对象体；签名（REST）请求照常拿到对象体，
  并把该头回显。
- 值必须以 `/`、`http://` 或 `https://` 开头，否则 PUT 时 400。
- **path-style 部署下 `/` 开头的目标相对主机根而非桶根**，站内跳转要写成
  `/<bucket>/<key>`；vhost 部署下 `/` 即桶根，与 AWS 网站端点一致。

### 5.2 整桶跳转：RedirectAllRequestsTo

```xml
<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <RedirectAllRequestsTo>
    <HostName>www.example.com</HostName>
    <Protocol>https</Protocol>   <!-- 可选；缺省跟随请求 scheme -->
  </RedirectAllRequestsTo>
</WebsiteConfiguration>
```

- 命中的桶每个匿名请求都 301 到 `<protocol>://<host>/<key>`。
- `Protocol` 缺省跟随请求 scheme：反向代理透传的 `X-Forwarded-Proto`，直连为 http。
- 与 IndexDocument / ErrorDocument / RoutingRules 互斥，同时出现回 `InvalidArgument`。
- YAML 侧对应 `redirect_all_host` / `redirect_all_protocol`。

### 5.3 路由规则：RoutingRules

```xml
<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IndexDocument><Suffix>index.html</Suffix></IndexDocument>
  <RoutingRules>
    <!-- 旧路径迁移：/docs/* -> /documents/* -->
    <RoutingRule>
      <Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition>
      <Redirect><ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith></Redirect>
    </RoutingRule>
    <!-- 404 交给单页应用入口 -->
    <RoutingRule>
      <Condition><HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals></Condition>
      <Redirect><ReplaceKeyWith>index.html</ReplaceKeyWith><HttpRedirectCode>302</HttpRedirectCode></Redirect>
    </RoutingRule>
  </RoutingRules>
</WebsiteConfiguration>
```

| 元素 | 取值 | 说明 |
| --- | --- | --- |
| `Condition.KeyPrefixEquals` | 前缀 | 可选 |
| `Condition.HttpErrorCodeReturnedEquals` | 4xx / 5xx | 可选；有此条件的规则只在错误发生时评估 |
| `Redirect.Protocol` | http / https | 可选 |
| `Redirect.HostName` | 主机名 | 可选；与 Protocol 同时缺省则跳转为本网关相对路径（path-style 下带 `/<bucket>` 前缀） |
| `Redirect.ReplaceKeyPrefixWith` | 字符串 | 替换命中的前缀；与 ReplaceKeyWith 互斥 |
| `Redirect.ReplaceKeyWith` | 字符串 | 整个 key 替换 |
| `Redirect.HttpRedirectCode` | 3xx | 默认 301 |

- 最多 50 条，按顺序首条命中生效；两个条件都省略的规则匹配全部请求。
- `Redirect` 至少要改变 Protocol / HostName / key 之一，否则 400（原地跳转无意义）。
- 前缀规则在取对象之前按**原始 key**（index 改写前）评估，因此 `KeyPrefixEquals`
  为空字符串时桶根请求也会命中。

## 6. 匿名限速

`website[].max_rps`（仅 YAML 静态条目；`?website` XML 无此字段）按桶限制匿名请求
速率：令牌桶，容量 = 速率，0 = 不限。超限回**轻量 XML 503**
`SlowDown`，不取 error 文档。签名请求不受此限制，只受全局
`runtime.max_inflight_requests` 与按 AK 的并发限制约束。

## 7. 监控

`GET /-/metrics` 暴露 `lights3_website_events_total{event}`：

| event | 含义 |
| --- | --- |
| `anon_read` | 进入匿名面的请求数（被限速拒绝的不计） |
| `index_rewrite` | key 被改写为 index 文档 |
| `error_document` | 错误以 error 文档或内置页面应答 |
| `redirect` | 任一来源的 3xx（RedirectAllRequestsTo / RoutingRules / 302 补斜杠 / 对象级重定向头） |
| `throttled` | 被 `max_rps` 拒绝 |

配合 `lights3_responses_by_status_total{status}` 可读出 206/304 比例（CDN 回源场景
的关键量）。Prometheus / Grafana 消费侧见 [monitoring.md](monitoring.md)。

## 8. 常见问题

- **匿名读得到 403**：桶不在 website 列表（`GET ?website` 用 root 凭证查）；或请求
  带了残缺的签名参数；或 mTLS 下客户端证书已绑定到无权限的凭证。
- **过期的 presigned 链接没有"变成"匿名可读**：设计如此，带签名材料就必须验签。
- **站内跳转 404**：path-style 部署下重定向目标要写 `/<bucket>/<key>`，不是 `/<key>`。
- **error 页面没有生效、日志有 WARN `website: error document ... unreadable`**：
  `error_key` 指向的对象不存在或读失败，已回退内置页；检查对象是否上传到该桶。
- **PUT ?website 回 405**：该桶在 YAML 里是静态条目，改配置文件后重启。
- **动态配置在另一台网关上不生效**：确认 `auth.sync_interval` 已开启且两台网关共享
  同一后端；等待一个同步周期。
- **想让 SPA 所有路径都回 index.html**：用 §5.3 的 `HttpErrorCodeReturnedEquals=404`
  规则；注意浏览器拿到的是 3xx 而非 200 内容替换。
