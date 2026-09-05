# Implementation order for the deferred items

An order for the ten "kept for a later phase" entries of
[backlog.md §1](backlog.md). Only three criteria: **dependencies** (what paves
the way for what), **value ÷ difficulty**, and **finish one theme at a time**
(fewer context switches, shared test scaffolding). Each item states scope,
entry points, acceptance and an effort estimate (one person familiar with the
code, docs in both languages and tests included). When an item is done, delete
it from backlog.md §1, strike its row here and record the date.

## 0. The order at a glance

| # | Phase | Item | Value / difficulty | Estimate | Why here |
| --- | --- | --- | --- | --- | --- |
| 1 | A quick wins | tiered local-tier used-bytes gauge | medium / low | 0.5 d | zero-risk small change; the dashboard stops inferring the watermark from eviction rate |
| 2 | A quick wins | separate admin port | low / low | 1 d | every later admin endpoint lands on this port -- lay the foundation first |
| 3 | A quick wins | admin endpoint for scrub / fsck | low / low | 0.5 d | right after ②: the first new endpoint on the admin port, exercising the isolation |
| 4 | B multi-instance consistency | multi-instance STS session table | medium / medium | 2–3 d | the only feature that "breaks when the next request hits another instance"; reuses the credential sync mechanism, warm-up for ⑤ |
| 5 | B multi-instance consistency | cross-gateway meta cache invalidation | medium / high | 4–5 d | same theme as ④ (shared state across gateways), shares the two-instance e2e scaffolding; lifts the "cache off by default" restriction on shared redis meta |
| 6 | C identity | mTLS client certificate → credential / tenant identity | medium / medium | 2–3 d | independent of B; the tenant model and `tls_client_auth` exist, only the binding rule is missing |
| 7 | D operational depth | hot add / remove of backend instances | medium / high | 4–5 d | touches the Application lifecycle and routing-table replacement -- the riskiest item, after the multi-instance and identity lines settle |
| 8 | D operational depth | duostore meta incremental backup / PITR | medium / high | 5+ d (per engine) | four engines, four natural small iterations; blocks nothing else |
| 9 | E opportunistic | structured error codes upstream in client-c | low / medium | 1–2 d + upstream cycle | paced by an external project, fits any gap; no functional impact |
| 10 | E opportunistic | `HeaderMap` linear scan / `BlockQueue` double copy | low / low | per profile | only with profile evidence; without it, it stays last |

Only two hard dependencies: ② → ③ (the endpoint lands on the new port) and
④ → ⑤ (shared multi-instance e2e scaffolding and the `.sys` shared-state
pattern). Everything else is independent: phases A/B/C can run in parallel;
start ⑦ of phase D only after B has landed.

## 1. Phase A: quick wins (about 2 days in total)

### ① tiered local-tier used-bytes gauge

- **Scope**: add `space_usage()` to `ITierLocal` (used / total / available,
  `std::optional`, empty when the probe fails); `LocalFsTierLocal` uses the
  `statvfs` already called by `cache_space_ok` in
  `src/storage/tiered/tier_local_fs.cc`, `DuoStoreTierLocal` sums local extent
  bytes. `TieredBackend::init_metrics` registers `gauge_callback`s
  `lights3_tiered_local_used_bytes` / `_total_bytes`, plus
  `lights3_tiered_local_high_watermark_bytes` (derived from config, for the
  dashboard line).
- **Entry points**: `src/storage/tiered/tier_local.h`, `tier_local_fs.cc`,
  `tier_local_duo.cc`, `tiered_backend.cc`; `deploy/grafana/gen_dashboard.py`
  (the tiered watermark panel draws used / high_watermark directly, the
  eviction-rate panel stays); one "approaching high watermark" alert in
  `deploy/prometheus/lights3.rules.yml`.
- **Acceptance**: `test_tiered.cc` asserts the gauges exist and grow after
  writes; ctest `monitoring_assets` passes (the dashboard must be regenerated
  by the generator, byte-identical); one line each in
  [monitoring.md](monitoring.md) and [tiered-storage.md](tiered-storage.md).

### ② separate admin port

- **Scope**: `http.admin_bind` / `http.admin_port` (empty by default = today's
  behavior, every face on the data-plane port). When set, a second
  `IHttpServer` of the same driver serves only the `/-/` routes; `/-/admin/*`
  and `/-/metrics` on the data-plane port become 404 (`/-/healthz|readyz`
  stay on both). TLS material comes from the same `tls::Holder`. The
  `metrics_access: root` semantics do not change.
- **Entry points**: `src/app/app.cc` (a second `server_`, shutdown ordering
  shares `shutdown_grace`), the `internal` decision at dispatch in
  `src/s3/service.cc` (an "is this listener the admin face" flag),
  `src/core/config.{h,cc}` validation (the admin port must differ from the
  data-plane port).
- **Acceptance**: a unit test starts both ports and asserts 404 on the data
  plane / 200 on the admin face; `config-reload` does not cover the admin port
  (listed under `requires_restart`); the `s3adm --endpoint` docs say admin
  commands point at the admin port; [http-adapter.md §2.1](http-adapter.md),
  [cli.md §3.1](cli.md).

### ③ admin endpoint for scrub / fsck

- **Scope**: `POST /-/admin/fsck/<backend>?max_mbps=N` (root) runs one
  `run_scrub_once` round in the background and returns a job id immediately;
  `GET /-/admin/fsck/<backend>` reports progress and the last result
  (findings, duration, running or not). One job per backend at a time.
  `s3adm fsck --offline <backend>` wraps the two endpoints (the existing online
  `s3adm fsck <bucket>` keeps its semantics).
- **Entry points**: the admin route table in `src/s3/service.cc`,
  `src/storage/*/run_scrub_once`, `src/tools/s3adm_fsck.cc`; throttling reuses
  `scrub_throttle.h`.
- **Acceptance**: e2e starts a localfs gateway, triggers a round, polls to
  completion, corrupts one object and reruns to see a finding; a concurrent
  second trigger returns 409; [cli.md §3.5](cli.md),
  [storage/localfs.md §11](../storage/localfs.md).

## 2. Phase B: multi-instance consistency (about 1.5 weeks in total)

### ④ multi-instance STS session table

- **Scope**: session records move from `CredentialStore::sessions_` (process
  memory) to write-through objects `.sys/sts/<session-ak>` on the default
  backend (token hash, TTL, inherited policy, issuer), with the local memory
  copy as a cache; another instance that meets an unknown session AK on the
  data plane reads it back once on demand (60 s negative cache so enumeration
  cannot hammer the backend), and the periodic `auth.sync_interval` sync picks
  up new sessions and drops expired ones. Expiry is lazy per instance plus the
  periodic sweep.
- **Entry points**: `src/s3/auth/credential_store.{h,cc}` (`sessions_` and the
  sync task), `src/s3/handlers/sts.cc`; the persisted format reuses the
  credential object JSON and its `rev` counter.
- **Acceptance**: unit tests -- instance A issues, instance B verifies, both
  reject after expiry; an e2e section "STS across two gateways sharing one
  localfs root" (modeled on the cloudproxy two-instance scaffolding);
  [credential-management.md](credential-management.md) §STS and
  [s3-protocol.md](s3-protocol.md) updated.

### ⑤ cross-gateway meta cache invalidation

- **Scope**: redis engine only: `RedisMetaStore` does `PUBLISH duo:inv
  <bucket>\0<key>` after every write commit, each gateway keeps one subscriber
  connection feeding the local `MetaCache::invalidate`. While the subscription
  is down the whole table is cleared and the connection re-established
  (conservative). With invalidation in place, shared redis meta may raise
  `meta_cache_ttl` to larger values within `gc_grace`; the default stays off.
  tikv has no pub/sub and keeps the bounded-staleness TTL contract, stated in
  the docs.
- **Entry points**: `src/storage/duostore/redis_meta_store.cc` (publish points
  = after each commit), `src/storage/meta_cache.h` (already has per-shard
  invalidation generations and fill tokens, no change), the cache wiring and
  config validation in `duostore_backend.cc`.
- **Acceptance**: `test_duostore_redis.cc` (runs with an instance) -- two
  backend instances share one redis, A writes and B's read hits the new value;
  after the subscriber connection is killed B clears its cache and recovers;
  the e2e `duostore-redis` section gains a two-instance case;
  [storage/duostore-core.md §7.1](../storage/duostore-core.md),
  [duostore-redis-meta.md](duostore-redis-meta.md).

## 3. Phase C: identity (about 2–3 days)

### ⑥ mTLS client certificate → credential / tenant identity

- **Scope**: `auth.tls_identity: off|subject-cn|san-uri`. When on, a
  connection that passed `tls_client_auth: require` carries the certificate
  subject (CN or the first URI SAN) as an identity candidate: an **unsigned**
  request maps through `.sys/tls-identities/<subject>` to a credential
  (inheriting its policy / tenant / role) and is treated as signed by it; a
  signed request must carry a certificate identity of the same tenant as the
  signing credential, otherwise 403. root maintains the mapping through
  `/-/admin/tls-identities` and `s3adm cred bind-cert`.
- **Entry points**: `src/http/tls.h` (peer certificate subject after the
  handshake; the four drivers put it into a new `HttpRequest.tls_identity`
  field next to `remote_addr`), the pre-verification branch in
  `src/s3/service.cc`, the mapping table in `credential_store` (the
  `SysConfigStore` template fits directly).
- **Acceptance**: `test_tls.cc` + `test_service.cc`: unsigned + valid
  certificate passes, no certificate 401, certificate tenant ≠ signing tenant
  403; e2e generates a client certificate with openssl and runs builtin/beast;
  [tls.md](tls.md) §mTLS, [multi-tenancy.md](multi-tenancy.md).

## 4. Phase D: operational depth (about 2 weeks in total, splittable)

### ⑦ hot add / remove of backend instances

- **Scope**: `reload_config` accepts new `backends[]` entries and the removal
  of entries **referenced by no routing rule and not `default_backend`**;
  parameter changes on existing entries still need a restart. Add: build from
  config → wrap with `meter_backends` → atomic `BucketRouter::update`. Remove:
  take it out of the routing table first, wait for in-flight requests to drain
  (an in-flight counter per backend in the spirit of
  `AsyncSemaphore::wait_drained`), then `close()`. tiered's local/cloud
  references and cloudproxy connection pools must close correctly on the
  removal path.
- **Entry points**: `src/app/app.cc` `reload_config` / `diff_config`,
  `storage/registry.h`, `storage/bucket_router.h`,
  `storage/metered_backend.h`.
- **Acceptance**: `test_reload.cc`: add a memory backend and route to it,
  remove it, removing a referenced one reports `requires_restart`; e2e removes
  a backend during a streaming GET and the close happens only after the
  request completes; the hot-reload matrix in
  [config-reload.md](config-reload.md) updated.

### ⑧ duostore meta incremental backup / PITR

- **Scope**: four small iterations per engine, easiest first: sqlite (WAL
  archiving + `dump --since` by checkpoint sequence) → rocksdb (`BackupEngine`
  incremental + `CreateCheckpoint`) → redis (relies on AOF archiving; the
  gateway only documents the consistency of `dump --since <offset>`) → tikv
  (BR/CDC belong to the cluster side; the gateway exports the "current TSO"
  as the restore point). One CLI: `lights3 duostore backup <backend>
  --incremental --to <dir>`, `restore --to-ts`.
- **Entry points**: `export_since` next to `snapshot()` in
  `src/storage/duostore/*_meta_store.cc`, the duostore subcommands in
  `src/main.cc`, [storage/duostore-core.md §11](../storage/duostore-core.md).
- **Acceptance**: per engine one "full + two incrementals + restore to the
  middle point" unit test (instance-gated ones follow the existing SKIP
  convention); a section in each meta document.

## 5. Phase E: opportunistic

### ⑨ structured error codes upstream in client-c

- **Scope**: turn the sidecar's "kvrpcpb structured conflict classification +
  string-match defense in depth" into an upstream PR (`third_party/client-c`)
  so `Backoffer` / `RegionCache` throw exceptions carrying an enum code; once
  merged, drop the string-matching branch here.
- **Acceptance**: upstream merge + submodule pointer update + the conflict
  cases in `test_duostore_tikv.cc` still pass.

### ⑩ `HeaderMap` linear scan / `BlockQueue` double copy

- **Trigger**: `HeaderMap::find` or `BlockQueue::push/pop` together above 2%
  of request CPU in a `perf` flame graph. Without that evidence, leave it.
- **If done**: keep `HeaderMap` a vector but add an 8-bit hash of the
  lower-cased key as a pre-filter; let `BlockQueue::push` accept
  `std::string&&` so httplib's content receiver hands the buffer over.

## 6. Ledger

| # | Item | Status | Date / branch |
| --- | --- | --- | --- |
| ① | tiered local-tier used-bytes gauge | not started | |
| ② | separate admin port | not started | |
| ③ | admin endpoint for scrub / fsck | not started | |
| ④ | multi-instance STS session table | not started | |
| ⑤ | cross-gateway meta cache invalidation | not started | |
| ⑥ | mTLS identity mapping | not started | |
| ⑦ | hot add / remove of backend instances | not started | |
| ⑧ | duostore meta incremental backup / PITR | not started | |
| ⑨ | client-c upstream contribution | not started | |
| ⑩ | HeaderMap / BlockQueue | waiting for profile evidence | |
