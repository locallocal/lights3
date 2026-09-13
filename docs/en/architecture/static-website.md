# Static website hosting: design of the anonymous read plane

> Code: `src/s3/service.cc` (the anonymous branch of dispatch, `website_error_page`,
> the redirect helpers, `website_rate_admit`), `src/s3/website_store.{h,cc}`
> (configuration store), `src/s3/handlers/bucket_website.cc` (`?website` XML codec
> and validation), `WebsiteBucket` / `WebsiteRoutingRule` in `src/core/config.h`.
> User manual: [usage/static-website.md](../usage/static-website.md); section numbers
> of this document do not correspond to it.

## 1. Goals and non-goals

**Goals**

- Serve a bucket to browsers directly as a static site, without any signing client;
- Match the observable behaviour of the AWS website endpoint: index / error
  documents, directory slash fix-up, `x-amz-website-redirect-location`,
  `RedirectAllRequestsTo`, `RoutingRules`;
- The anonymous plane may **only** widen one capability, "read one object";
  everything else stays identical to signed requests;
- Configuration works statically (YAML) and dynamically (`?website` API), and
  converges automatically across gateways sharing one backend.

**Non-goals**

- A separate website domain / dual-endpoint model (§9);
- Bucket-policy / ACL style general anonymous authorization: the anonymous plane is
  a by-product of the website feature, not a general public-read facility;
- Server-side rendering or content rewriting: the gateway only handles routing,
  status codes and Location; object bodies are served verbatim.

## 2. Anonymous decision and the authorization chain

The object read path was already "website-ready" (persisted Content-Type,
ETag/304, Range/206, Cache-Control echo, see [s3-protocol.md](s3-protocol.md)), so
the feature adds no new data path. It only adds one decision in dispatch **before
signature verification** that swaps qualifying requests to a synthesized identity
and then hands them to exactly the same authorization chain signed requests use.

**Conditions** (`S3Service::anonymous_website_read`), all required:

1. the website table is non-empty, the bucket name is non-empty and listed (service-
   scope requests are never anonymous — ListBuckets must not be public);
2. the method is GET or HEAD;
3. the request carries **no signature material at all**: no `Authorization` header
   and none of the `X-Amz-Algorithm` / `X-Amz-Signature` / `X-Amz-Credential` query
   parameters;
4. authentication is globally enabled;
5. no TLS client certificate bound to a credential.

Why before verify: verify treats a missing Authorization header as `AccessDenied`,
so anonymous must bypass it; with authentication disabled verify admits everything,
the anonymous branch is never entered, and the synthesized read-only policy could
only be stricter than "unrestricted". "The feature does not participate when auth
is off" therefore falls out naturally with no special case.

**Why partial signatures still verify**: if a presigned link with an expired
`X-Amz-Signature` degraded to an anonymous success, client misconfiguration would
be masked and expired links would "work". The rule: once a request expresses "I
have an identity" it is handled as that identity, and a bad signature stays
`SignatureDoesNotMatch`.

**Why bound certificates win**: under mTLS a certificate bound to a credential is an
identity, not an anonymous reader; the more specific identity wins, and a credential
without access to the bucket gets 403. Operators can therefore roll out certificate
bindings before switching modes without the anonymous plane bypassing them
([tls.md](../usage/tls.md)).

**Two gates.** An anonymous request gets `VerifiedIdentity{}` plus a synthesized
policy (`buckets = {bucket}`, `readonly = true`), and the policy block later
re-checks it through the same `allows()` path every credential goes through. But
policy is not the only line: before the policy is set, dispatch pins anonymous
requests by **route matching** to object-level routes with `flag == ""` and
`Action::Read`. Both are needed:

- policy alone would let query-flag read operations such as `?uploadId` (ListParts,
  Action::Read) through;
- routing alone would open every future object-level read route to anonymous by
  default, while the policy states "this bucket only, read only" as explicit data.

Anonymous listing is impossible by construction: an empty key is rewritten to an
object read by the index rule first (§3), and a `?list-type=2` request falls into
GetObject's inverted query whitelist and gets 501; neither path reaches
ListObjects. `response-*` overrides are `InvalidRequest` for anonymous requests: on
a public bucket a crafted link could otherwise hang an arbitrary Content-Disposition
off the bucket's origin, and AWS refuses them too.

## 3. Request processing order

The anonymous plane's decision points in dispatch, in order:

| Stage | Decision | Outcome |
| --- | --- | --- |
| rate limit | `max_rps` token bucket refuses | 503 `SlowDown`, XML, `anon_read` not counted |
| enter the plane | count `anon_read`, remember the original key | — |
| pre-request redirect | `RedirectAllRequestsTo`; else the first RoutingRule **without an error-code condition** whose prefix matches | 3xx, routing and policy skipped |
| index rewrite | key empty or ending in `/` | append `index_suffix`, count `index_rewrite` |
| route gate | not a bare GET/HEAD object route | 403 |
| `response-*` | override parameters present | 400 |
| policy | synthesized read-only policy re-checked | 403 |
| route() | the normal object read | 200/206/304 … |
| object-level redirect | 200/206 response carrying `x-amz-website-redirect-location` | 301 + Location, body dropped |
| error stage ① | first RoutingRule whose `HttpErrorCodeReturnedEquals` equals this status and whose prefix matches | 3xx |
| error stage ② | `NoSuchKey`, original key non-empty and not ending in `/`, `<key>/<index_suffix>` exists | 302 to `<key>/` |
| error stage ③ | any other error | error document or built-in page, status unchanged |

Ordering trade-offs:

- **Pre-request rules use the original key**, before the index rewrite: AWS's
  `KeyPrefixEquals` semantics refer to the path the user requested, not the rewritten
  object name. Error-stage rules use the original key too.
- **Error-code rules precede the slash fix-up and the error document**: explicit
  configuration wins over defaults; otherwise the common SPA rule "404 →
  index.html" would be stolen by the slash fix-up.
- **The slash fix-up applies to `NoSuchKey` only** and only after probing that the
  directory index really exists (one `head_object`): any probe failure keeps the
  original error, so a transient backend error is never amplified into a misleading
  302.
- **The object-level redirect comes after route()**: the value is part of the object
  metadata and is only known once the object was read; signed requests go through
  the same route() but skip this step, receiving the body with the header echoed,
  matching AWS.
- **The cancel/timeout path stays XML**: 503 `SlowDown` is retry signalling for
  SDKs, not a page. The same goes for rate limiting, and fetching the error document
  on a throttled request would spend exactly the backend read the limiter exists to
  protect.

## 4. Configuration store: WebsiteStore

`WebsiteStore` copies the three-part model of `CredentialStore` (static / dynamic /
periodic sync, see
[credential-management.md §10.3](credential-management.md)), minus what does not
apply.

- **Static entries** come from YAML, validated at startup with
  `validate_bucket_name` (reserved names such as `.sys` fail startup) and answer 405
  to API mutation: the config file is their only source of truth.
- **Dynamic entries** are written by `PUT ?website` to `.sys/website/<bucket>`
  (JSON), **write-through**: storage first, then memory, so on a crash storage is
  authoritative. On a name clash the static entry wins with a WARN.
- **Snapshots**: `snapshot()` returns an immutable
  `shared_ptr<const vector<WebsiteBucket>>`; dispatch takes one at request start and
  holds it to the end, with `anon_site` pointing into it. A concurrent PUT/DELETE
  only swaps the current snapshot and cannot dangle in-flight requests.
- **Periodic sync**: with `auth.sync_interval` on, `.sys/website/` is re-listed;
  new/changed entries are pulled in and dynamic entries gone from storage are
  dropped. Entries just removed locally get a tombstone so an interleaved listing
  cannot resurrect them. The difference from credential sync is the **absence of an
  empty-table guard**: an emptied website table only closes anonymous access; nobody
  can be locked out.
- **Startup tolerance**: a missing `.sys` counts as empty; a corrupt JSON object is
  skipped with a WARN. The worst case is one site answering 403, which must not
  block the process from starting (only corrupt credentials deserve a startup
  failure).
- **Assemblies without a backend** (unit tests / purely static deployments) use
  `make_static`; the dynamic API answers `InvalidRequest`.

`?website` is open to root (the statically configured credential) only, the same
two-tier model as the admin plane: making a bucket public is an operator decision,
and a tenant credential cannot publish even a bucket it owns. Validation is one set
of rules on both the XML and the YAML side (`index_suffix` non-empty without `/`,
`RedirectAllRequestsTo` exclusive with the other elements, RoutingRules ≤ 50 with
every Redirect changing at least one thing), so the validation story is the same
whichever plane the entry came through.

## 5. Error page rendering

`website_error_page` turns the `S3Error` raised by an anonymous request into a page:

- **The original status is kept.** Wrapping a 404 in a 200 would poison CDN and
  browser caches and mislead crawlers; AWS keeps it too. Headers carried by the
  error itself (such as `Allow` on 405) are passed through.
- **The backend is read directly, without re-entering dispatch**: the error object
  goes through `router_.resolve(bucket)` straight to `get_object`, skipping routing,
  policy and redirect evaluation, so there is no recursion and the page cannot be
  rewritten by the site's own RoutingRules.
- **Fallback to the built-in page**: when the error object is missing or unreadable
  a WARN is logged and a minimal HTML page is served; a site owner's
  misconfiguration must not turn a 404 into a 500.
- **XSS escaping**: the built-in page embeds `S3Error::message`, and some messages
  quote request input (query parameter names, keys); unescaped they would be a
  reflected XSS on the bucket's origin. `html_escape` handles `& < > "`.
- **HEAD**: status and headers are kept, no body is sent, Content-Length is the size
  of the error object.
- **Fetched outside catch**: a coroutine cannot `co_await` inside a catch block, so
  catch only records the error and the three error stages (§3) run after it.

## 6. Building redirect Locations

Every 3xx Location is produced by `website_location`:

- HostName and Protocol both absent → a relative path staying on this gateway.
  **Path-style gets the `/<bucket>` prefix**; vhost gets `/<key>`. This is the
  only observable divergence from AWS: the AWS website endpoint is always vhost, so
  `/` is the bucket root; this implementation shares the REST endpoint, and under
  path-style `/` is the host root.
- Either present → an absolute URL. A missing Host takes the request's `Host`
  header (keeping this gateway's addressing style when redirecting back to itself);
  a missing Protocol takes `X-Forwarded-Proto`, http for direct connections (the
  scheme can only be relayed by a reverse proxy; direct connections are plaintext).
- The key goes through `aws_uri_encode` (slashes untouched): it lands in a response
  header and cannot carry control characters verbatim.

`x-amz-website-redirect-location` bypasses that function and lands verbatim in
Location, which is why the PUT already restricts it to values starting with `/`,
`http://` or `https://`: free-form schemes (`javascript:`, `data:`) are an injection
surface. The header is stored and echoed via `kStdMetaFields`; metadata
serialization is self-describing kv, so existing data needs no migration.

## 7. Amplification and rate limiting

Anonymous GETs cost no signature work, so a public bucket is a free bandwidth
amplifier. Three layers of defence:

1. the global `runtime.max_inflight_requests` admission gate, applied to every
   request alike;
2. the per-bucket `website[].max_rps` token bucket (capacity = rate, a fresh bucket
   starts full), counting anonymous requests only; signed requests are unaffected;
3. the rate-limit rejection is decided before entering the anonymous plane and
   answers a lightweight XML 503, without the error document and without counting
   `anon_read`.

`max_rps` is YAML-only: the AWS XML has no corresponding field, and forcing one
into `?website` would break SDK compatibility; the dynamic entry's JSON keeps the
field but the API does not expose it.

## 8. Observability

- `lights3_website_events_total{event}`: `anon_read` / `index_rewrite` /
  `error_document` / `redirect` / `throttled`, one increment per decision point;
  meanings in [usage/static-website.md §7](../usage/static-website.md).
- The exact-status series `lights3_responses_by_status_total{status}` gives the
  206/304 share, the key quantity for website/CDN scenarios.
- Access log: anonymous requests carry an empty access_key, the same convention as
  "auth disabled"; redirects and error pages are ordinary responses on the same
  access line.

## 9. Open items

- **A separate website endpoint**: AWS separates website semantics from REST
  semantics with `s3-website-<region>` domains; this implementation triggers on
  "anonymous + website bucket" on the same endpoint, which keeps path-style usable
  and needs no extra DNS, at the cost of the path-prefix difference of §6. Strict
  alignment could add a `website_base_domain` under which only that domain's
  requests enter the anonymous plane, with `/` always meaning the bucket root.
- **Hot reload of static entries**: the `website` list currently requires a
  restart; the dynamic API already covers runtime additions and removals, so a hot
  reload of static entries has limited value.
