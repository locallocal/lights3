# TLS：全驱动 HTTPS、证书热重载与 TLS 旋钮（roadmap §4.1）

> 状态：四项全部落地（2026-09-05）。代码：`src/http/tls.{h,cc}`（OpenSSL 共享层）、
> 四个驱动各自的接入点（`src/http/drivers/*/`）；单测 `tests/unit/test_tls.cc`
> 与 `test_http_drivers.cc` 的 TLS 用例；e2e `run_e2e.sh` 末尾的 TLS smoke。

## 1. 现状与目标

此前只有 httplib/beast 支持 `tls_cert`+`tls_key`，**默认驱动 builtin 与性能路径
seastar 都没有 TLS**，且证书轮换必须重启、没有 mTLS/cipher/SNI/最低版本任何旋钮。
本次：

| 条目 | 结果 |
| --- | --- |
| builtin / seastar 无 TLS | builtin 在连接线程上以 OpenSSL 阻塞 I/O 包裹 socket；seastar 经 `seastar::tls` 包裹 listener |
| 证书热重载 | OpenSSL 驱动：`tls_reload_interval` 轮询文件 mtime/size，变了就重载、新握手生效、失败保留旧素材；seastar：可重载凭证自带文件监视 |
| TLS 旋钮 | `tls_client_ca` + `tls_client_auth`（mTLS）、`tls_min_version`、`tls_ciphers` / `tls_ciphersuites`、`tls_sni` 多证书 |
| 短期替代 | §6 nginx / caddy 反代终结样例 |

## 2. 配置

```yaml
http:
  tls_cert: /etc/lights3/server.crt   # PEM 证书链（叶子在前，随后中间证书）
  tls_key: /etc/lights3/server.key    # PEM 私钥；两者都给才启用 HTTPS
  tls_min_version: "1.2"              # 1.2 | 1.3（下限；1.0/1.1 永远拒绝）
  tls_client_ca: /etc/lights3/clients-ca.pem   # 客户端证书的 CA bundle（mTLS）
  tls_client_auth: require            # off | optional | require（后两者必须给 tls_client_ca）
  tls_ciphers: "ECDHE+AESGCM:ECDHE+CHACHA20"   # TLS ≤ 1.2 的 OpenSSL cipher list；空 = 库默认
  tls_ciphersuites: "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256"  # TLS 1.3 suites；空 = 库默认
  tls_reload_interval: 60s            # 证书文件轮询周期；0s = 不热重载
  tls_sni:                            # 按 SNI 选用的额外证书（可多条）
    - hosts: "s3.example.com, *.s3.example.com"   # 逗号分隔；"*." 通配恰好一个标签
      cert: /etc/lights3/s3.example.com.crt
      key: /etc/lights3/s3.example.com.key
```

校验规则（启动即失败，不留"看似配了其实没生效"）：任何 `tls_*` 旋钮都要求
`tls_cert`/`tls_key` 同时存在；`tls_client_auth` 非 `off` 要求 `tls_client_ca`；
`tls_min_version` 只认 `1.2`/`1.3`；`tls_sni` 每条须 hosts+cert+key；证书/私钥/
CA 文件在构造期加载，坏路径、坏 PEM、私钥与证书不匹配、cipher 串匹配不到任何
套件都在启动期抛错。

### 2.1 mTLS

`tls_client_ca` 给出信任的 CA bundle 后：`optional` = 请求客户端证书，不给也放行，
给了必须能验证；`require` = 必须给且验证通过。验证结果在握手内决定（TLS 1.3 下
客户端会在首个读上看到 alert）。通过验证的客户端证书目前**不映射为身份**——
SigV4 仍是唯一的身份来源，mTLS 是传输层准入（"没有公司 CA 签发的证书连
握手都过不了"）。把客户端证书 CN 映射到凭证/租户是后续项。

### 2.2 版本与套件

`tls_min_version` 默认 1.2（等价于原先硬编码的 no_tlsv1/no_tlsv1_1）；设为
`1.3` 后 1.2 客户端握手失败。`tls_ciphers` 是 OpenSSL 的 TLS ≤1.2 cipher list，
`tls_ciphersuites` 是 TLS 1.3 的 suite 列表；两者都保留 `SSL_OP_CIPHER_SERVER_PREFERENCE`
（服务端顺序优先）。压缩与重协商恒关。

### 2.3 SNI 多证书

`tls_sni` 里的每条证书按客户端 ClientHello 的 servername 选用：先精确匹配，再
`*.` 通配（只匹配一个标签，`*.example.com` 不匹配 `example.com` 也不匹配
`a.b.example.com`），都不中就用默认证书；不带 SNI 的客户端（IP 直连、老工具）
得到默认证书。大小写不敏感。

### 2.4 热重载

OpenSSL 驱动每 `tls_reload_interval` 在定时器线程 stat 一次全部证书文件
（默认证书、SNI 证书、客户端 CA），size 或 mtime 变化即重新加载**整套**素材：
成功 → 新快照对之后的握手生效，进行中的连接继续用旧快照（引用计数，不会被
释放）；失败（一半轮换、坏 PEM、私钥不匹配）→ WARN 并保留旧素材，下一轮重试。
轮换建议"先写 key 再写 cert"或原子 rename；一次轮询恰好落在两者之间只会多等
一轮。没有 SIGHUP 语义——轮询已足够，且不与主进程的信号处理耦合。

## 3. 实现：一个证书回调解决三件事

`src/http/tls.h`：

```text
tls::Material   不可变快照：默认证书 + SNI 证书（叶子/链/私钥）+ 客户端 CA store
tls::Holder     每个 server 一份：当前快照（mutex + shared_ptr）、轮询定时器、
                configure(SSL_CTX*) 把静态旋钮写进驱动自己的 SSL_CTX 并安装证书回调
```

关键选择是 **`SSL_CTX_set_cert_cb`**：证书不再在 SSL_CTX 上配置，而是每次握手
由回调按 servername 从当前快照挑出 bundle，`SSL_use_cert_and_key(ssl, leaf, key,
chain, override=1)` 装进本连接；客户端 CA 亦按连接 `SSL_set1_verify_cert_store`。
于是：

- SNI 与热重载共用同一处代码，驱动不需要知道任何一项；
- 各驱动继续拥有自己的 SSL_CTX（beast 的 `asio::ssl::context`、httplib 的
  `SSLServer`、builtin 自建），只把它交给 `Holder::configure` 一次；
- 快照替换是一次 shared_ptr 赋值，无锁争用（回调只取快照）。

驱动接入：

| 驱动 | 接入 | 备注 |
| --- | --- | --- |
| builtin | 连接线程 `SSL_new`/`SSL_set_fd`/`SSL_accept`，`Io` 抽象统一 recv/send 走 `SSL_read`/`SSL_write` | 阻塞模型不变，socket 超时仍是唯一超时机制；启用 TLS 时进程忽略 SIGPIPE（`SSL_write` 走 write(2)） |
| beast | `asio::ssl::context` 仅承载静态旋钮 | 会话循环模板化不变 |
| httplib | `SSLServer(setup_ssl_ctx_callback)` 把 ctx 交给 holder | 不再用 `update_certs`（它不装中间证书链） |
| seastar | `credentials_builder` → `build_reloadable_server_credentials` → `tls::listen` 包裹每 shard 的 listener | 见 §4 |

## 4. seastar 的差异

seastar 的 TLS 走它自己的 `seastar::tls`（默认 GnuTLS 后端，可编 OpenSSL 后端），
不经过 `tls::Holder`：

| 旋钮 | seastar 行为 |
| --- | --- |
| `tls_cert`/`tls_key`/`tls_client_ca`/`tls_client_auth` | 完整支持（`set_x509_key_file` / `set_x509_trust_file` / `set_client_auth`） |
| `tls_min_version` | 映射为 GnuTLS priority string（`-VERS-ALL:+VERS-TLS1.2:+VERS-TLS1.3` 或仅 1.3），同时调用 OpenSSL 后端的 `set_minimum_tls_version` |
| `tls_ciphers` / `tls_ciphersuites` | 只对 OpenSSL 后端生效；GnuTLS 后端启动 WARN 忽略 |
| `tls_sni` | **不支持**（每 listener 一套凭证），配置了在构造期抛错 |
| `tls_reload_interval` | >0 时用 `build_reloadable_server_credentials`（seastar 自己监视文件，不按周期轮询）；0 = 不重载 |

## 5. 观测与排障

- 启动日志一行汇总：`... https server listening on ... (tls: min 1.2, 2 SNI cert(s), client auth require, reload every 60s)`；
- 握手失败一行 WARN（明文客户端、被拒的客户端证书、低版本）；
- 重载成功 INFO（含默认证书 subject），失败 WARN 并保留旧素材；
- `openssl s_client -connect host:9000 -servername s3.example.com -tls1_2` 可逐项验证。

## 6. 短期替代：反向代理终结 TLS

不想让网关持有证书时，前置 nginx / caddy 终结 TLS 并转发明文到 lights3。
两点要求：**透传 `Host`**（vhost 寻址与 SigV4 的 host 签名头依赖它）；
**`X-Forwarded-Proto: https`**（CompleteMultipartUpload 的 `Location` 用它拼
scheme）。请求体不能被缓冲改写（SigV4 aws-chunked 逐块签名对字节敏感）。

```nginx
server {
    listen 443 ssl http2;
    server_name s3.example.com *.s3.example.com;
    ssl_certificate     /etc/nginx/certs/fullchain.pem;
    ssl_certificate_key /etc/nginx/certs/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    client_max_body_size 0;          # 大对象/分片上传不受限
    proxy_request_buffering off;     # 流式转发请求体（aws-chunked 逐块签名）
    proxy_buffering off;
    location / {
        proxy_pass http://127.0.0.1:9000;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto https;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 300s;
    }
}
```

```caddyfile
s3.example.com, *.s3.example.com {
    reverse_proxy 127.0.0.1:9000 {
        flush_interval -1
        header_up Host {host}
        header_up X-Forwarded-Proto https
    }
}
```

caddy 自动签发/续期 Let's Encrypt 证书（通配名需要 DNS challenge 插件）；此时
lights3 端保持明文即可，`X-Forwarded-Proto` 让 Location 正确。

## 7. 测试

- `tests/unit/test_tls.cc`：三个 OpenSSL 驱动（有 seastar 构建时版本/mTLS 用例
  也覆盖 seastar）逐一验证——HTTPS 往返（含 body 流）与 1.1 拒绝；`1.3` 下限 +
  ciphersuite 限制；mTLS require/optional（无证书、他 CA 证书拒绝，合法证书放行）；
  SNI 精确/通配/大小写/无 SNI 回落；热重载（换证书后新握手看到新 CN，坏文件不替换）；
  `Holder` 的重载语义（cert/key 半轮换保留旧素材、旧快照对持有者仍有效、坏路径抛错带文件名）；
  配置校验。证书由 `tests/unit/tls_testcerts.h` 运行时生成（EC P-256），无固定文件。
- `test_http_drivers.cc`：TLS 往返/明文拒绝/坏证书抛错覆盖全部驱动；seastar 配
  `tls_sni` 抛错。
- e2e：`run_e2e.sh` 末尾用 openssl CLI 自签证书起一个 HTTPS 实例，`curl --cacert`
  做 SigV4 PUT/GET 往返（builtin 驱动即默认驱动）。
