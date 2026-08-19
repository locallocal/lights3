# Static website hosting (static files in a bucket)

Goal: serve a bucket to browsers directly as a static site. The object read
path has long been "website-ready" (persisted Content-Type, ETag/304
conditional requests, Range, Cache-Control — see docs/s3-protocol.md); the
gaps are closed in three phases:

| Phase | Content | Status |
|-------|---------|--------|
| ① | Per-bucket anonymous read-only (§1–§2) | **Implemented** |
| ② | index / error document semantics, HTML error pages for anonymous (§3) | **Implemented** |
| ③ | Dynamic `?website` API (persisted to `.sys`), `x-amz-website-redirect-location` | Planned |

## 1. Configuration and anonymous read semantics

```yaml
website:
  - bucket: my-site          # exact names, no globs — a pattern typo must not silently make extra buckets public
    index_suffix: index.html # optional, default index.html; must not contain '/' (a slash would map "docs/" outside that directory)
    error_key: error.html    # optional; body source for anonymous 4xx/5xx, built-in HTML page when omitted
```

Listed buckets accept **anonymous GET/HEAD object reads**: a request carrying
no signature material at all (neither an Authorization header nor presigned
query parameters) enters the normal authorization chain under a synthesized
read-only policy (that bucket only, Read only). Everything else is unchanged:

- **Bare object-level GET/HEAD only.** Bucket/service-level listing, all
  writes/deletes, and operations steered by a query flag (`?uploadId` is
  ListParts) stay `AccessDenied` for anonymous — pinned by route matching
  (flag == "", Action::Read), not just by policy. An empty key is first
  rewritten by the §3 index rule into an object read, so anonymous listing is
  impossible by construction (`?list-type=2` falls into GetObject's query
  whitelist and gets a 501).
- **Signature material always verifies.** Requests with an Authorization
  header or any `X-Amz-Algorithm` / `X-Amz-Signature` / `X-Amz-Credential`
  query parameter (even a partial set) go through verification as usual: a
  bad signature must stay `SignatureDoesNotMatch` and never silently degrade
  into an anonymous success — that would mask client misconfiguration and
  make expired links "work".
- **`response-*` overrides are refused for anonymous requests**
  (`InvalidRequest`, matching AWS): on a public bucket a crafted link could
  otherwise hang an arbitrary Content-Disposition off the bucket's domain
  (see the §5.3 comment in src/s3/handlers/objects.cc).
- Anonymous reads get exactly the same GET behaviour as signed reads:
  Range/206, conditional requests/304, stored Content-Type and standard
  metadata echoed back.

## 2. Boundaries and defenses

- **Explicit opt-in**: only buckets in the website list accept anonymous
  requests; an empty list turns the feature off entirely. Bucket names are
  validated at startup with the same `validate_bucket_name` gate as user
  requests — a reserved name (`.sys`) fails startup instead of lying dormant.
- With authentication globally disabled (no credentials configured) the
  feature does not participate — everything is open anyway; a configured
  website list logs a WARN as a reminder.
- Anonymous requests carry an empty access_key (sharing the access-log
  convention with "auth disabled").
- Amplification: anonymous GETs cost no signature work;
  `runtime.max_inflight_requests` is the only throttle today, per-bucket rate
  limiting is future work.

## 3. index / error document semantics (anonymous requests only)

- **index**: a key that is empty (bucket root, with or without the trailing
  slash) or ends with `/` (directory-style keys such as `docs/`) → append
  `index_suffix`, then run GetObject. `GET /my-site` and `GET /my-site/`
  both return `index.html`.
  Note: `GET /my-site/docs` (no trailing slash, key missing) does **not**
  issue the AWS website endpoint's 302 add-slash redirect — it goes straight
  to the error document path. Write in-site links with the trailing slash;
  the 302 semantics are a phase ③ candidate.
- **error**: when an anonymous request throws any S3 error (404/403/501/
  500…), and `error_key` is configured, that object becomes the response
  body with its own Content-Type while **keeping the original status code**
  — wrapping a 404 in a 200 would poison caches and mislead crawlers. The
  error object is read directly from the backend without re-entering
  dispatch (no recursion); if it is missing/unreadable, a built-in minimal
  HTML page is served instead (the site owner's misconfiguration must not
  turn a 404 into a 500) with a WARN log.
- **Built-in error page**: `<title>404 NoSuchKey</title>` plus the escaped
  error message (messages can quote request input such as query parameter
  names — unescaped they would be an XSS surface on the bucket's origin).
- HEAD error responses keep the status and headers, with no body.
- Signed requests are unaffected and keep XML errors; the cancel/timeout
  path (503 SlowDown) also stays XML — that is retry signaling for SDKs,
  not a page.

## 4. Later phases (design notes)

- **Phase ③**: `PUT/GET/DELETE /bucket?website` (root credential only)
  persisted to `.sys/website/<bucket>`, multi-instance convergence reusing
  credential management's `sync_interval` pattern;
  `x-amz-website-redirect-location` joins `kStdMetaFields` for storage and
  the website plane answers 301 when it is set; on implementation the
  corresponding entries leave the 501 blocklists. Optional: `GET /prefix`
  (no trailing slash) 302-redirects to `/prefix/` when `prefix/index`
  exists (aligning with the AWS website endpoint).
- Addressing: phases ①② trigger on the **same endpoint** (anonymous +
  website bucket = website semantics, works path-style); a separate
  `website_base_domain` strictly mirroring AWS's dual-endpoint model remains
  an option.
