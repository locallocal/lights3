# Static website hosting (static files in a bucket)

Goal: serve a bucket to browsers directly as a static site. The object read
path has long been "website-ready" (persisted Content-Type, ETag/304
conditional requests, Range, Cache-Control — see docs/s3-protocol.md); the
gaps are closed in three phases:

| Phase | Content | Status |
|-------|---------|--------|
| ① | Per-bucket anonymous read-only (§1–§2 below) | **Implemented** |
| ② | index / error document semantics, HTML error pages for anonymous | Planned |
| ③ | Dynamic `?website` API (persisted to `.sys`), `x-amz-website-redirect-location` | Planned |

## 1. Configuration and semantics (phase ①)

```yaml
website:
  - bucket: my-site   # exact names, no globs — a pattern typo must not silently make extra buckets public
```

Listed buckets accept **anonymous GET/HEAD object reads**: a request carrying
no signature material at all (neither an Authorization header nor presigned
query parameters) enters the normal authorization chain under a synthesized
read-only policy (that bucket only, Read only). Everything else is unchanged:

- **Bare object-level GET/HEAD only.** Bucket/service-level reads
  (ListObjectsV2, ListBuckets), all writes/deletes, and operations steered by
  a query flag (`?uploadId` is ListParts) stay `AccessDenied` for anonymous —
  pinned by route matching (flag == "", Action::Read), not just by policy.
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
  metadata echoed back. A missing object returns a real 404.

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

## 3. Later phases (design notes)

- **Phase ②**: a key that is empty or ends with `/` → append `index_suffix`
  and run GetObject; anonymous 404/403 → if `error_key` is configured, serve
  that object as the body while **keeping the original status code** (missing
  error object falls back to a built-in HTML page, one level of recursion
  guard); only anonymous requests get HTML errors, signed requests keep XML.
  Config grows `index_suffix` / `error_key` fields on website entries.
- **Phase ③**: `PUT/GET/DELETE /bucket?website` (root credential only)
  persisted to `.sys/website/<bucket>`, multi-instance convergence reusing
  credential management's `sync_interval` pattern;
  `x-amz-website-redirect-location` joins `kStdMetaFields` for storage and
  the website plane answers 301 when it is set; on implementation the
  corresponding entries leave the 501 blocklists.
- Addressing: phases ①② trigger on the **same endpoint** (anonymous +
  website bucket = website semantics, works path-style); a separate
  `website_base_domain` strictly mirroring AWS's dual-endpoint model remains
  an option.
