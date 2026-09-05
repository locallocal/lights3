# Monitoring Consumers: Prometheus Rules and the Grafana Dashboard (roadmap §5.5)

`GET /-/metrics` already speaks canonical Prometheus text with uniform
`lights3_*` naming ([s3-protocol.md §7](s3-protocol.md)); this document is its
consumer side: zero C++ changes, three ready-made assets under `deploy/` plus a
test that reconciles them against the source's metric catalog.

## 1. Assets

| File | Content |
| --- | --- |
| `deploy/prometheus/scrape.yml` | scrape config: `job_name: lights3`, `metrics_path: /-/metrics`, loads the rules; runnable standalone or merged into an existing prometheus.yml |
| `deploy/prometheus/lights3.rules.yml` | 9 groups, 48 rules: 7 recording rules (5xx ratio, P99s, backend error ratio, cloudproxy retry ratio, keep-alive reuse) + 41 alerts |
| `deploy/grafana/lights3.json` | the dashboard (uid `lights3-overview`, 63 panels / 9 rows), variables `DS` / `instance` / `backend` |
| `deploy/grafana/gen_dashboard.py` | dashboard generator — the single source of truth for panels; rerun after editing |
| `tests/monitoring/check_assets.py` | asset validation (§5), ctest name `monitoring_assets` |

## 2. Wiring

```bash
prometheus --config.file=deploy/prometheus/scrape.yml       # single-gateway lab
# or merge the scrape_configs entry into an existing config, rule_files -> lights3.rules.yml
```

Grafana: Dashboards → Import → upload `lights3.json`, bind the Prometheus data
source to the `DS` variable. Several gateways are selected/aggregated through
`instance`; backend-level panels filter on `backend` (taken from the backend
label of `lights3_backend_op_seconds_count`).

**Exposure of `/-/metrics`**: Prometheus cannot sign SigV4, so scraping needs
`http.metrics_access: anonymous` (the default). Deployments with the `root`
gate either scrape through a signing sidecar/proxy or keep the listener on a
private network with the gate off. TLS deployments switch `scheme: https` and
fill in `tls_config`.

## 3. Alert catalog

Thresholds are single-gateway starting points; tune `for` and the ratios to
the traffic shape. Three severities: critical (act now) / warning (act today)
/ info (trend hints).

| Group | Alert | Fires when | Severity |
| --- | --- | --- | --- |
| availability | `Lights3Down` | `up == 0` for 1m | critical |
| | `Lights3High5xxRate` | 5xx ratio > 2% at > 1 req/s, 5m | critical |
| | `Lights3HighP99Latency` | request P99 (headers ready) > 2s, 10m | warning |
| | `Lights3ApiP99Latency` | metadata-class API P99 > 1s (body-carrying Get/Put/UploadPart/Copy/Complete excluded) | warning |
| capacity | `Lights3AdmissionSaturated` | permits exhausted with a queue, 5m | warning |
| | `Lights3AdmissionQueueWait` | admission wait P99 > 1s | warning |
| | `Lights3AdmissionCancellations` | > 10 cancelled while queued (503) in 10m | warning |
| | `Lights3PoolBacklogged` | shared pool backlogged 5m (the per-backend-pool criterion of [concurrency.md §3.1](concurrency.md)) | warning |
| | `Lights3TimerLag` | timer head-of-queue lag > 5s | warning |
| | `Lights3TransferStalls` | > 20 stall-guard cuts in 10m | info |
| http | `Lights3ConnectionLimit` | `max_connections` refusals | warning |
| | `Lights3ParseErrorSpike` | malformed requests > 5/s | info |
| | `Lights3TlsHandshakeFailures` | handshake failures > 1/s | info |
| | `Lights3RateLimitRejections` | rate-limit rejections > 10/s, 10m | info |
| backends | `Lights3BackendErrors` | backend 5xx/transport error ratio > 1% (4xx excluded) | critical |
| | `Lights3XlocalfsUringFallback` | `lights3_xlocalfs_uring_fallback == 1` | warning |
| | `Lights3LocalfsXattrFallback` | `lights3_localfs_xattr_fallback == 1` | info |
| | `Lights3MetaCacheStale` | metadata cache stale hits > 5% | info |
| duostore | `Lights3DuostoreGcNotConverging` | rounds run but the queue head is > 1h old (**GC not converging**) | warning |
| | `Lights3DuostoreGcStalled` | backlog but no GC round in 1h | warning |
| | `Lights3DuostoreGcRoundSlow` | GC round P99 > 10min | info |
| | `Lights3DuostoreCorruption` | read CRC mismatch / corrupt pack record | critical |
| | `Lights3DuostorePacksQuarantined` | quarantined packs > 0 | warning |
| | `Lights3DuostoreCompactionDeferred` | compaction deferred for 6h | info |
| | `Lights3DuostoreSqliteBusy` / `RedisReconnects` / `TikvSafepoint` / `RadosErrors` | health signals of the meta/data engines | info–critical |
| tiered | `Lights3TieredGcFailures` | cloud deletions failing for 30m | warning |
| | `Lights3TieredGcBacklog` | deferred cloud deletions > 100 for 1h | warning |
| | `Lights3TieredQuarantine` | quarantine entries > 0 (refs_missing = data-loss signal) | critical |
| | `Lights3TieredEvictionPressure` | every scan evicts for 1h (the **watermark** signal, §4) | info |
| | `Lights3TieredCloudReadHeavy` | > 50% of reads recall from the cloud, 30m | info |
| cloudproxy | `Lights3CloudproxyRetryRatio` | retry ratio > 10%, 10m | warning |
| | `Lights3CloudproxyRemoteErrors` | remote 5xx/transport errors > 0.5/s | critical |
| | `Lights3CloudproxyEtagMismatch` | upload ETag mismatch | critical |
| | `Lights3CloudproxyPoolWait` | connection-pool wait P99 > 1s | warning |
| tenancy | `Lights3QuotaRejections` | quota rejections within 1h | info |
| | `Lights3UsageScanStale` | usage reconciliation not run for 2 days | info |

Recording rules (shared by dashboard and alerts): `lights3:requests:rate5m`,
`lights3:responses_5xx_ratio:rate5m`, `lights3:request_duration_seconds:p99_5m`,
`lights3:api_request_duration_seconds:p99_5m`, `lights3:backend_error_ratio:rate5m`,
`lights3:cloudproxy_retry_ratio:rate5m`, `lights3:keepalive_reuse:rate5m`.

## 4. Dashboard rows

| Row | Panels |
| --- | --- |
| Overview | four stats (req/s, 5xx ratio, P99, in flight); breakdown by method / class / exact status code, S3 error codes, byte throughput |
| APIs | rate, 5xx and P99 per api × backend (roadmap §5.1) |
| HTTP layer | connections, keep-alive reuse (requests ÷ accepted), timeouts by phase, malformed requests / TLS handshakes, rate-limit rejections |
| Admission & pools | permit capacity/available/waiting, admission wait P50/P99, stall cuts, shared and backend pools, timer thread |
| Storage backends | backend op P99 / errors / rate, metadata cache, **degradation flags** (uring / xattr fallback), localfs op errors |
| Buckets / usage / website | top buckets, usage bytes, quota rejections, website-plane events, active multipart uploads |
| DuoStore | GC queue depth and head age (convergence criterion), rounds / reclaims / duration, skip reasons, pack live vs total, quarantine, corruption and orphans, the four meta-engine signals |
| Tiered | read source, demotion / promotion / eviction, cloud GC, scans and quarantine, range cache, op errors |
| CloudProxy | remote request P99, retry ratio, error codes, pool wait and ETag mismatches |

**Tiered watermark**: five gauges give the position directly --
`lights3_tiered_local_used_bytes` / `_total_bytes` / `_high_watermark_bytes`
(statvfs, exactly what the watermark logic sees) and
`lights3_tiered_local_cached_bytes` / `_quota_bytes` (booked local bytes and
the logical quota). Alerts: `Lights3TieredLocalAboveHighWatermark` (still above
the high watermark after 30 minutes = eviction cannot keep up) and
`Lights3TieredQuotaNearlyFull` (booked bytes above 90% of the quota).
`Lights3TieredEvictionPressure` (eviction rate + scan rounds) stays as the
"every round evicts" complement.

## 5. Tests

`tests/monitoring/check_assets.py` (ctest `monitoring_assets`, needs python3;
without PyYAML the structural checks of rules / scrape config degrade to a
failure message):

1. The rules file parses; every rule is a record or an alert; alerts carry
   `severity` (critical|warning|info), `summary`, `description`; names are
   unique and follow the `Lights3*` / `lights3:<a>:<b>` conventions; with
   promtool installed, `promtool check rules/config` runs too.
2. The scrape config has the `lights3` job on `/-/metrics` and loads the rules.
3. `lights3.json` is **byte-identical** to what `gen_dashboard.py` renders
   (no drift between hand-edited JSON and the generator); every panel has
   targets, ids are unique, the three template variables exist.
4. Every `lights3_*` metric referenced by a rule or dashboard expression is
   found in the source catalog (a grep for `lights3_[a-z0-9_]+` literals under
   `src/`), histogram `_bucket/_sum/_count` suffixes allowed — renaming or
   dropping a metric fails the test instead of silently breaking the assets.
5. A memory-backend gateway is started, a few requests are sent, `/-/metrics`
   is scraped: every line follows the exposition format, every family has a
   `# TYPE`, and every referenced family that is **not backend-specific** is
   present (the backend-specific prefixes duostore/tiered/cloudproxy/localfs/…
   are only reconciled against the catalog).

Change workflow: edit `gen_dashboard.py` → `python3 deploy/grafana/gen_dashboard.py`
→ `ctest -R monitoring_assets`.
