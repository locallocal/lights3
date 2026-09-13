# Static website hosting

Serve a bucket to browsers directly as a static site: objects in the bucket are
readable with anonymous GET/HEAD, the bucket root and directory-style keys map to
an index document, 4xx/5xx answer with the site owner's error page, and AWS-shaped
redirect rules plus per-bucket anonymous rate limiting are available. The object
read path is already "website-ready" (persisted Content-Type, ETag/304 conditional
requests, Range/206, Cache-Control echo — see
[s3-protocol.md](../architecture/s3-protocol.md)); this feature only layers an
anonymous plane on top of it.

This is the user manual; design trade-offs and implementation details live in
[architecture/static-website.md](../architecture/static-website.md).

## 1. Prerequisites and how it works

- **Authentication must be enabled** (at least one credential configured). With
  authentication globally disabled every request is open anyway and this feature
  does not participate; a configured website list then only logs one WARN at
  startup.
- **Same endpoint**: website semantics share the listener with the S3 REST API.
  Both path-style `http://gw/<bucket>/<key>` and vhost `http://<bucket>.gw/<key>`
  work; there is no separate website domain (the AWS `s3-website-*` dual-endpoint
  model is not implemented, see
  [architecture/static-website.md §9](../architecture/static-website.md)).
- **Definition of anonymous**: a request carrying no signature material at all —
  neither an `Authorization` header nor any of the `X-Amz-Algorithm` /
  `X-Amz-Signature` / `X-Amz-Credential` query parameters. A request that carries
  signature material (even partial or expired) is verified as usual and never
  degrades to anonymous.
- Only **explicitly listed** buckets accept anonymous access; an empty list turns
  the feature off entirely.

## 2. Enabling a site

A bucket's website configuration can come from the config file (static entry) or
from the runtime API (dynamic entry); both are validated by the same rules.

### 2.1 Config file (static entries)

```yaml
website:
  - bucket: my-site            # exact bucket name, no globs
    index_suffix: index.html   # optional, default index.html; must not contain '/'
    error_key: error.html      # optional; built-in error page when omitted
    max_rps: 0                 # optional; anonymous requests/second, 0 = unlimited
    redirect_all_host: ""      # optional; non-empty: every anonymous request 301s to this host
    redirect_all_protocol: ""  # optional; http|https, empty = follow the request scheme
```

- Bucket names are validated at startup with the same rules as user requests; a
  reserved name such as `.sys` or an invalid name **fails startup**.
- Static entries belong to the config file: modifying or deleting them through the
  `?website` API answers 405; when a same-name dynamic entry exists the static one
  wins and a WARN is logged.
- The `website` list requires a restart (it is outside the hot-reloadable subset in
  [config-reload.md](config-reload.md)); use §2.2 to add or remove sites at runtime.
- RoutingRules cannot be written in YAML; they are API-only.

### 2.2 Runtime API (dynamic entries)

`PUT / GET / DELETE /<bucket>?website`, request and response bodies in the AWS
`WebsiteConfiguration` XML shape. **Root (statically configured credential)
only**: making a bucket anonymously readable is an operator decision, not a tenant
one; non-root credentials get 403.

```bash
# enable: index.html as directory index, error.html as error page
cat > site.xml <<'EOF'
<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IndexDocument><Suffix>index.html</Suffix></IndexDocument>
  <ErrorDocument><Key>error.html</Key></ErrorDocument>
</WebsiteConfiguration>
EOF
curl --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:$SK" -X PUT --data-binary @site.xml \
     "http://127.0.0.1:9000/my-site?website"

# read / delete
curl --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:$SK" "http://127.0.0.1:9000/my-site?website"
curl --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:$SK" -X DELETE "http://127.0.0.1:9000/my-site?website"
```

| Operation | Success | Common errors |
| --- | --- | --- |
| `PUT ?website` | 200, empty body | bucket missing `NoSuchBucket` (404); invalid XML `MalformedXML` / `InvalidArgument` (400); static entry 405; non-root 403 |
| `GET ?website` | 200 + XML | not configured `NoSuchWebsiteConfiguration` (404) |
| `DELETE ?website` | 204 (idempotent, 204 without a configuration too) | static entry 405; non-root 403 |

`lights3-ctl website get/set/delete <bucket>` ([cli.md §3.3](cli.md)) wraps the two
common settings, index and error; submit XML directly for RedirectAllRequestsTo and
RoutingRules.

```bash
lights3-ctl website set my-site --index-suffix=index.html --error-key=404.html
lights3-ctl website get my-site
lights3-ctl website delete my-site
```

Dynamic entries persist to `.sys/website/<bucket>` (JSON) in the storage backend and
are restored on restart. When several gateways share one backend,
`auth.sync_interval` enables periodic sync: a configuration PUT/DELETEd on one
gateway propagates to the others within the next sync period (the same knob as
credential sync).

### 2.3 Setting reference

| Feature | YAML key | XML element | Notes |
| --- | --- | --- | --- |
| Directory index | `index_suffix` | `IndexDocument.Suffix` | non-empty, no `/`; required in XML (unless RedirectAllRequestsTo is used) |
| Error page | `error_key` | `ErrorDocument.Key` | optional |
| Whole-bucket redirect | `redirect_all_host` / `redirect_all_protocol` | `RedirectAllRequestsTo.HostName` / `.Protocol` | **exclusive** with index / error / RoutingRules |
| Routing rules | — | `RoutingRules` | API only; ≤ 50 rules |
| Anonymous rate limit | `max_rps` | — | no such field in the AWS XML, not exposed by the API; static YAML entries only |

## 3. Scope of anonymous access

On a listed bucket, the only thing anonymous requests may do is a **bare object-level
GET/HEAD**:

| Request | Anonymous result |
| --- | --- |
| `GET/HEAD /<bucket>/<key>` | identical to a signed read: Range/206, conditional requests/304, stored Content-Type and standard metadata echoed |
| `GET /<bucket>` or `GET /<bucket>/` | rewritten to an index-document read (§4), never a listing |
| bucket/service-level listing (`?list-type=2`, `?uploads`, `GET /`) | refused (403 or 501, never 2xx) |
| any write, delete or multipart operation | `AccessDenied` (403) |
| `response-content-type` and other `response-*` overrides | `InvalidRequest` (400), matching AWS |
| operations steered by a query flag such as `?uploadId` | `AccessDenied` (403) |
| a bucket not in the website list | `AccessDenied` (403) |

Further rules:

- **Signature material always verifies**: a bad signature or an expired presigned
  link gets `SignatureDoesNotMatch` / `AccessDenied`, never a silent anonymous
  success.
- **Bound certificates win**: under mTLS a client certificate bound to a credential
  acts as that credential, not anonymously; if that credential may not read the
  bucket the answer is 403 ([tls.md](tls.md)).
- **Access log**: anonymous requests carry an empty access_key, the same convention
  as "auth disabled".

## 4. index and error documents

Anonymous requests only; signed requests keep XML errors.

**index document**

- When the key is empty (bucket root, with or without the trailing slash) or ends
  with `/` (directory-style keys such as `docs/`), `index_suffix` is appended before
  the object read: `GET /my-site` and `GET /my-site/` both return `index.html`,
  `GET /my-site/docs/` returns `docs/index.html`.
- `GET /my-site/docs` (no trailing slash) with no object at key `docs` answers
  **302 to `/my-site/docs/`** when `docs/index.html` exists (AWS website-endpoint
  behaviour); otherwise it goes to the error document as a 404.

**error document**

- When an anonymous request raises any S3 error (404/403/501/500 …) and `error_key`
  is configured, that object becomes the response body with its own Content-Type,
  and the **status code is kept** (a 404 is never wrapped in a 200).
- If the error object is missing or unreadable, a built-in minimal HTML page is
  served instead with one WARN log; a site owner's misconfiguration never turns a 404
  into a 500.
- The built-in page looks like `<title>404 NoSuchKey</title>` plus the HTML-escaped
  error message.
- HEAD error responses keep status and headers without a body.
- Cancelled/timed-out requests (503 `SlowDown`) and rate-limited requests (§6) stay
  XML; the error document is not fetched for them.

## 5. Redirects

Three redirect sources, decided in this order (first hit wins):

1. RedirectAllRequestsTo (§5.2);
2. RoutingRules with only a `KeyPrefixEquals` condition (§5.3), evaluated on the
   original key **before** the object read;
3. `x-amz-website-redirect-location` on the object (§5.1), after the object was read;
4. after an error: RoutingRules with `HttpErrorCodeReturnedEquals` → the 302 slash
   fix-up of §4 → the error document.

### 5.1 Object-level: `x-amz-website-redirect-location`

Send the header when uploading; the value is persisted with the object metadata:

```bash
curl --aws-sigv4 "aws:amz:us-east-1:s3" --user "$AK:$SK" -X PUT --data-binary 'moved' \
     -H 'x-amz-website-redirect-location: /my-site/index.html' \
     "http://127.0.0.1:9000/my-site/old"
```

- An anonymous read of that object answers **301 + Location** without the body;
  signed (REST) requests get the body as usual with the header echoed.
- The value must start with `/`, `http://` or `https://`, otherwise the PUT is a 400.
- **Under path-style addressing a `/`-prefixed target is relative to the host root,
  not the bucket**: write in-site targets as `/<bucket>/<key>`. Under vhost `/` is
  the bucket root, matching the AWS website endpoint.

### 5.2 Whole-bucket redirect: RedirectAllRequestsTo

```xml
<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <RedirectAllRequestsTo>
    <HostName>www.example.com</HostName>
    <Protocol>https</Protocol>   <!-- optional; defaults to the request scheme -->
  </RedirectAllRequestsTo>
</WebsiteConfiguration>
```

- Every anonymous request to the bucket is 301-redirected to
  `<protocol>://<host>/<key>`.
- `Protocol` defaults to the request scheme: `X-Forwarded-Proto` from a reverse
  proxy, http when connecting directly.
- Exclusive with IndexDocument / ErrorDocument / RoutingRules; combining them answers
  `InvalidArgument`.
- YAML equivalents: `redirect_all_host` / `redirect_all_protocol`.

### 5.3 Routing rules: RoutingRules

```xml
<WebsiteConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <IndexDocument><Suffix>index.html</Suffix></IndexDocument>
  <RoutingRules>
    <!-- legacy path migration: /docs/* -> /documents/* -->
    <RoutingRule>
      <Condition><KeyPrefixEquals>docs/</KeyPrefixEquals></Condition>
      <Redirect><ReplaceKeyPrefixWith>documents/</ReplaceKeyPrefixWith></Redirect>
    </RoutingRule>
    <!-- hand 404s to the single-page-app entry point -->
    <RoutingRule>
      <Condition><HttpErrorCodeReturnedEquals>404</HttpErrorCodeReturnedEquals></Condition>
      <Redirect><ReplaceKeyWith>index.html</ReplaceKeyWith><HttpRedirectCode>302</HttpRedirectCode></Redirect>
    </RoutingRule>
  </RoutingRules>
</WebsiteConfiguration>
```

| Element | Value | Notes |
| --- | --- | --- |
| `Condition.KeyPrefixEquals` | prefix | optional |
| `Condition.HttpErrorCodeReturnedEquals` | 4xx / 5xx | optional; rules with it are evaluated only when an error occurs |
| `Redirect.Protocol` | http / https | optional |
| `Redirect.HostName` | host name | optional; with Protocol also absent the redirect is a path relative to this gateway (with the `/<bucket>` prefix under path-style) |
| `Redirect.ReplaceKeyPrefixWith` | string | replaces the matched prefix; exclusive with ReplaceKeyWith |
| `Redirect.ReplaceKeyWith` | string | replaces the whole key |
| `Redirect.HttpRedirectCode` | 3xx | default 301 |

- At most 50 rules, evaluated in order, first match wins; a rule with both
  conditions omitted matches every request.
- A `Redirect` must change at least one of Protocol / HostName / key, otherwise 400
  (a redirect to the same place is meaningless).
- Prefix rules are evaluated on the **original key** (before the index rewrite)
  ahead of the object read, so an empty `KeyPrefixEquals` also matches bucket-root
  requests.

## 6. Anonymous rate limiting

`website[].max_rps` (static YAML entries only; the `?website` XML has no such field)
limits anonymous requests per bucket: a token bucket with capacity = rate, 0 =
unlimited. Over-limit requests answer a **lightweight XML 503** `SlowDown`; the
error document is not fetched. Signed requests are not subject to it and only see
the global `runtime.max_inflight_requests` and the per-access-key concurrency
limit.

## 7. Monitoring

`GET /-/metrics` exposes `lights3_website_events_total{event}`:

| event | Meaning |
| --- | --- |
| `anon_read` | requests entering the anonymous plane (rate-limited rejections not counted) |
| `index_rewrite` | key rewritten to the index document |
| `error_document` | error answered with the error document or the built-in page |
| `redirect` | any 3xx (RedirectAllRequestsTo / RoutingRules / the slash 302 / the object-level redirect header) |
| `throttled` | rejected by `max_rps` |

Together with `lights3_responses_by_status_total{status}` the 206/304 share can be
read off (the key quantity for CDN origin scenarios). Prometheus / Grafana consumers
are described in [monitoring.md](monitoring.md).

## 8. Troubleshooting

- **Anonymous reads get 403**: the bucket is not in the website list (check with
  `GET ?website` as root); or the request carries partial signature parameters; or
  under mTLS the client certificate is bound to a credential without access.
- **An expired presigned link did not "become" anonymously readable**: by design —
  signature material must verify.
- **In-site redirects 404**: under path-style addressing the target must be
  `/<bucket>/<key>`, not `/<key>`.
- **The error page has no effect and the log shows WARN `website: error document
  ... unreadable`**: the object named by `error_key` is missing or unreadable and
  the built-in page was served; check that it was uploaded to that bucket.
- **PUT ?website answers 405**: the bucket is a static YAML entry; change the config
  file and restart.
- **A dynamic configuration does not apply on another gateway**: make sure
  `auth.sync_interval` is enabled and both gateways share the same backend; wait one
  sync period.
- **Every SPA path should serve index.html**: use the
  `HttpErrorCodeReturnedEquals=404` rule of §5.3; note that the browser receives a
  3xx rather than a 200 with substituted content.
