# TLS: HTTPS on Every Driver, Certificate Hot Reload and TLS Knobs (roadmap §4.1)

> Status: all four items landed (2026-09-05). Code: `src/http/tls.{h,cc}` (the
> shared OpenSSL layer) and each driver's hook (`src/http/drivers/*/`); unit tests
> in `tests/unit/test_tls.cc` plus the TLS cases of `test_http_drivers.cc`; e2e in
> the TLS smoke at the end of `run_e2e.sh`.

## 1. Starting Point and Goals

Only httplib/beast used to honor `tls_cert`+`tls_key`; **the default driver
builtin and the performance path seastar had no TLS at all**, certificate
rotation required a restart, and there was no mTLS/cipher/SNI/minimum-version
knob. This round:

| Item | Result |
| --- | --- |
| builtin / seastar without TLS | builtin wraps the socket with blocking OpenSSL I/O on the connection thread; seastar wraps its listener with `seastar::tls` |
| certificate hot reload | OpenSSL drivers: `tls_reload_interval` polls file mtime/size, reloads on change, new handshakes pick it up, failures keep the old material; seastar: reloadable credentials watch the files themselves |
| TLS knobs | `tls_client_ca` + `tls_client_auth` (mTLS), `tls_min_version`, `tls_ciphers` / `tls_ciphersuites`, `tls_sni` multi-certificate |
| short-term alternative | §6 nginx / caddy termination examples |

## 2. Configuration

```yaml
http:
  tls_cert: /etc/lights3/server.crt   # PEM chain (leaf first, then intermediates)
  tls_key: /etc/lights3/server.key    # PEM private key; both enable HTTPS
  tls_min_version: "1.2"              # 1.2 | 1.3 (floor; 1.0/1.1 are always refused)
  tls_client_ca: /etc/lights3/clients-ca.pem   # CA bundle for client certificates (mTLS)
  tls_client_auth: require            # off | optional | require (the latter two need tls_client_ca)
  tls_ciphers: "ECDHE+AESGCM:ECDHE+CHACHA20"   # OpenSSL cipher list for TLS <= 1.2; empty = library default
  tls_ciphersuites: "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256"  # TLS 1.3 suites; empty = default
  tls_reload_interval: 60s            # certificate file polling period; 0s = no hot reload
  tls_sni:                            # extra certificates chosen by SNI (any number)
    - hosts: "s3.example.com, *.s3.example.com"   # comma-separated; "*." matches exactly one label
      cert: /etc/lights3/s3.example.com.crt
      key: /etc/lights3/s3.example.com.key
```

Validation (fails at startup, never "looks configured but is not"): every
`tls_*` knob requires `tls_cert`/`tls_key`; `tls_client_auth` other than `off`
requires `tls_client_ca`; `tls_min_version` accepts only `1.2`/`1.3`; every
`tls_sni` entry needs hosts+cert+key; certificate/key/CA files load at
construction — a bad path, bad PEM, key not matching the certificate, or a cipher
string matching no suite throws before listening.

### 2.1 mTLS

With `tls_client_ca` set: `optional` requests a client certificate, admits
connections without one, and refuses one that fails verification; `require`
demands one that verifies. The verdict is decided inside the handshake (under
TLS 1.3 the client sees the alert on its first read). A verified client
certificate is currently **not mapped to an identity** — SigV4 remains the only
identity source; mTLS is transport admission ("without a certificate from the
company CA you cannot even complete the handshake"). Mapping the client
certificate CN to a credential/tenant is a later item.

### 2.2 Versions and Suites

`tls_min_version` defaults to 1.2 (the previous hard-coded no_tlsv1/no_tlsv1_1);
`1.3` makes 1.2 clients fail the handshake. `tls_ciphers` is OpenSSL's TLS ≤1.2
cipher list, `tls_ciphersuites` the TLS 1.3 suite list; both keep
`SSL_OP_CIPHER_SERVER_PREFERENCE` (server order wins). Compression and
renegotiation are always off.

### 2.3 SNI Multi-Certificate

Each `tls_sni` certificate is chosen by the ClientHello servername: exact match
first, then `*.` wildcards (exactly one label: `*.example.com` matches neither
`example.com` nor `a.b.example.com`), else the default certificate; clients
without SNI (IP connections, old tools) get the default. Case-insensitive.

### 2.4 Hot Reload

Every `tls_reload_interval` the OpenSSL drivers stat all certificate files on
the timer thread (default certificate, SNI certificates, client CA); a size or
mtime change reloads the **whole** material: success → the new snapshot serves
subsequent handshakes while in-flight connections keep the old one (reference
counted, never freed under them); failure (half a rotation, bad PEM, key
mismatch) → WARN, old material kept, retried next round. Rotate "key first,
then certificate" or with atomic renames; a poll landing between the two only
costs one more round. There is no SIGHUP semantics — polling suffices and stays
out of the process's signal handling.

## 3. Implementation: One Certificate Callback for Three Items

`src/http/tls.h`:

```text
tls::Material   immutable snapshot: default + SNI certificates (leaf/chain/key) + client CA store
tls::Holder     one per server: current snapshot (mutex + shared_ptr), the polling timer,
                configure(SSL_CTX*) writes the static knobs into the driver's own SSL_CTX
                and installs the certificate callback
```

The key choice is **`SSL_CTX_set_cert_cb`**: certificates are no longer
configured on the SSL_CTX; every handshake the callback picks the bundle for the
servername from the current snapshot and installs it into that connection with
`SSL_use_cert_and_key(ssl, leaf, key, chain, override=1)`; the client CA store is
likewise set per connection (`SSL_set1_verify_cert_store`). Hence:

- SNI and hot reload share one piece of code and no driver knows about either;
- every driver keeps its own SSL_CTX (beast's `asio::ssl::context`, httplib's
  `SSLServer`, builtin's own) and hands it to `Holder::configure` once;
- swapping a snapshot is one shared_ptr assignment with no contention (the
  callback only reads it).

Driver hooks:

| Driver | Hook | Notes |
| --- | --- | --- |
| builtin | `SSL_new`/`SSL_set_fd`/`SSL_accept` on the connection thread; an `Io` abstraction routes recv/send through `SSL_read`/`SSL_write` | blocking model unchanged, socket timeouts remain the only timeout; SIGPIPE is ignored process-wide when TLS is on (`SSL_write` uses write(2)) |
| beast | the `asio::ssl::context` carries only the static knobs | templated session loop unchanged |
| httplib | `SSLServer(setup_ssl_ctx_callback)` hands the ctx to the holder | `update_certs` is not used (it does not install intermediate chains) |
| seastar | `credentials_builder` → `build_reloadable_server_credentials` → `tls::listen` wraps every shard's listener | see §4 |

## 4. seastar Differences

seastar's TLS is its own `seastar::tls` (GnuTLS backend by default, OpenSSL
backend possible) and bypasses `tls::Holder`:

| Knob | seastar behavior |
| --- | --- |
| `tls_cert`/`tls_key`/`tls_client_ca`/`tls_client_auth` | fully supported (`set_x509_key_file` / `set_x509_trust_file` / `set_client_auth`) |
| `tls_min_version` | mapped to a GnuTLS priority string (`-VERS-ALL:+VERS-TLS1.2:+VERS-TLS1.3`, or 1.3 only) plus the OpenSSL backend's `set_minimum_tls_version` |
| `tls_ciphers` / `tls_ciphersuites` | only reach the OpenSSL backend; the GnuTLS backend WARNs at startup and ignores them |
| `tls_sni` | **unsupported** (one credential set per listener); configuring it throws at construction |
| `tls_reload_interval` | >0 uses `build_reloadable_server_credentials` (seastar watches the files itself, no periodic polling); 0 = no reload |

## 5. Observability and Troubleshooting

- One startup line summarizes the setup: `... https server listening on ... (tls: min 1.2, 2 SNI cert(s), client auth require, reload every 60s)`;
- one WARN per failed handshake (plaintext client, refused client certificate, too-old version);
- reload success INFO (with the default certificate subject), failure WARN with the old material kept;
- `openssl s_client -connect host:9000 -servername s3.example.com -tls1_2` verifies each item.

## 6. Short-Term Alternative: Terminate TLS at a Reverse Proxy

When the gateway should not hold certificates, put nginx / caddy in front and
forward plaintext to lights3. Two requirements: **pass `Host` through** (vhost
addressing and the SigV4 host header depend on it) and **`X-Forwarded-Proto:
https`** (CompleteMultipartUpload's `Location` takes its scheme from it). The
request body must not be buffered/rewritten (SigV4 aws-chunked per-chunk
signatures are byte-sensitive).

```nginx
server {
    listen 443 ssl http2;
    server_name s3.example.com *.s3.example.com;
    ssl_certificate     /etc/nginx/certs/fullchain.pem;
    ssl_certificate_key /etc/nginx/certs/privkey.pem;
    ssl_protocols TLSv1.2 TLSv1.3;
    client_max_body_size 0;          # large objects / multipart uploads unbounded
    proxy_request_buffering off;     # stream the request body (aws-chunked per-chunk signatures)
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

caddy issues/renews Let's Encrypt certificates itself (wildcards need a DNS
challenge plugin); lights3 then stays plaintext and `X-Forwarded-Proto` keeps
`Location` correct.

## 7. Tests

- `tests/unit/test_tls.cc`: every OpenSSL driver (and seastar for the
  version/mTLS cases when that build exists) — HTTPS round trip with a streamed
  body and a refused 1.1 client; `1.3` floor + ciphersuite restriction; mTLS
  require/optional (no certificate and a foreign-CA certificate refused, a valid
  one admitted); SNI exact/wildcard/case/no-SNI fallback; hot reload (new CN on
  the next handshake, a broken file never replaces working material); `Holder`
  reload semantics (a half rotation keeps the old material, old snapshots stay
  valid for their holders, bad paths throw naming the file); config validation.
  Certificates are generated at runtime by `tests/unit/tls_testcerts.h` (EC
  P-256), no fixture files.
- `test_http_drivers.cc`: TLS round trip / plaintext rejection / bad-certificate
  throw across all drivers; seastar with `tls_sni` throws.
- e2e: the end of `run_e2e.sh` starts an HTTPS instance with an openssl-CLI
  self-signed certificate and does a SigV4 PUT/GET round trip with
  `curl --cacert` (builtin, the default driver).
