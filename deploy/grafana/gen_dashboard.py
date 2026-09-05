#!/usr/bin/env python3
"""Generates deploy/grafana/lights3.json (roadmap §5.5, docs/monitoring.md §4).

The dashboard is kept as generated JSON so it imports into Grafana directly;
this script is the source of truth for its panels. Regenerate after editing:

    python3 deploy/grafana/gen_dashboard.py

tests/monitoring/check_assets.py fails when the checked-in JSON is stale and
when a PromQL expression names a metric the gateway does not emit.
"""
import json
import os
import sys

# Panel geometry: a 24-column grid, two panels per row unless `wide`
W_HALF, W_THIRD, H = 12, 8, 8


class Grid:
    def __init__(self):
        self.y = 0
        self.x = 0
        self.row_h = 0

    def place(self, w, h):
        if self.x + w > 24:
            self.x = 0
            self.y += self.row_h
            self.row_h = 0
        pos = {"x": self.x, "y": self.y, "w": w, "h": h}
        self.x += w
        self.row_h = max(self.row_h, h)
        return pos

    def newline(self):
        if self.x:
            self.x = 0
            self.y += self.row_h
            self.row_h = 0


grid = Grid()
panels = []
_id = [0]


def next_id():
    _id[0] += 1
    return _id[0]


def target(expr, legend):
    return {"datasource": {"type": "prometheus", "uid": "${DS}"}, "expr": expr,
            "legendFormat": legend, "refId": chr(ord("A") + target.n)}


target.n = 0


def targets(pairs):
    target.n = 0
    out = []
    for expr, legend in pairs:
        out.append(target(expr, legend))
        target.n += 1
    return out


def row(title, collapsed=False):
    grid.newline()
    panels.append({"type": "row", "id": next_id(), "title": title, "collapsed": collapsed,
                   "gridPos": grid.place(24, 1), "panels": []})
    grid.newline()


def timeseries(title, pairs, unit="short", w=W_HALF, h=H, description="", stack=False, maxv=None):
    fc = {"defaults": {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 8,
                                                  "stacking": {"mode": "normal" if stack else "none"}}},
          "overrides": []}
    if maxv is not None:
        fc["defaults"]["max"] = maxv
        fc["defaults"]["min"] = 0
    panels.append({"type": "timeseries", "id": next_id(), "title": title,
                   "description": description, "gridPos": grid.place(w, h),
                   "datasource": {"type": "prometheus", "uid": "${DS}"},
                   "fieldConfig": fc,
                   "options": {"legend": {"displayMode": "list", "placement": "bottom"},
                               "tooltip": {"mode": "multi", "sort": "desc"}},
                   "targets": targets(pairs)})


def stat(title, expr, unit="short", w=6, h=4, description="", thresholds=None, legend=""):
    steps = [{"color": "green", "value": None}]
    if thresholds:
        for value, color in thresholds:
            steps.append({"color": color, "value": value})
    panels.append({"type": "stat", "id": next_id(), "title": title, "description": description,
                   "gridPos": grid.place(w, h),
                   "datasource": {"type": "prometheus", "uid": "${DS}"},
                   "fieldConfig": {"defaults": {"unit": unit,
                                                "thresholds": {"mode": "absolute", "steps": steps}},
                                   "overrides": []},
                   "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                               "colorMode": "value", "graphMode": "area", "textMode": "auto"},
                   "targets": targets([(expr, legend)])})


INST = 'instance=~"$instance"'
BK = 'backend=~"$backend"'
P99 = 'histogram_quantile(0.99, sum by (le{by}) (rate({metric}_bucket{{{sel}}}[$__rate_interval])))'
P50 = 'histogram_quantile(0.50, sum by (le{by}) (rate({metric}_bucket{{{sel}}}[$__rate_interval])))'


def q(metric, by="", sel=INST, quantile=P99):
    return quantile.format(metric=metric, by=by, sel=sel)


# ---------------- Overview ----------------
row("Overview")
stat("Requests / s", f'sum(rate(lights3_requests_total{{{INST}}}[$__rate_interval]))', unit="reqps")
stat("5xx ratio",
     f'sum(rate(lights3_responses_total{{{INST},class="5xx"}}[$__rate_interval])) / clamp_min(sum(rate(lights3_requests_total{{{INST}}}[$__rate_interval])), 1e-9)',
     unit="percentunit", thresholds=[(0.01, "orange"), (0.02, "red")])
stat("P99 time-to-headers", q("lights3_request_duration_seconds"), unit="s",
     thresholds=[(1, "orange"), (2, "red")])
stat("In flight", f'sum(lights3_inflight_requests{{{INST}}})', description="Requests inside dispatch (admission permits held incl. streaming bodies: lights3_admission_capacity - lights3_admission_available)")
timeseries("Requests by method", [(f'sum by (method) (rate(lights3_requests_total{{{INST}}}[$__rate_interval]))', "{{method}}")], unit="reqps", stack=True)
timeseries("Responses by class", [(f'sum by (class) (rate(lights3_responses_total{{{INST}}}[$__rate_interval]))', "{{class}}")], unit="reqps", stack=True)
timeseries("Request latency (time-to-headers)",
           [(q("lights3_request_duration_seconds", quantile=P50), "p50"),
            (q("lights3_request_duration_seconds"), "p99")], unit="s",
           description="Histogram closes when the response headers are ready (the access log's ttfb); streaming transfer time is not included")
timeseries("Exact status codes",
           [(f'sum by (status) (rate(lights3_responses_by_status_total{{{INST}}}[$__rate_interval]))', "{{status}}")],
           unit="reqps", stack=True, description="206/304 share is the website / CDN signal (roadmap §5.3)")
timeseries("S3 error codes", [(f'sum by (code) (rate(lights3_s3_errors_total{{{INST}}}[$__rate_interval]))', "{{code}}")], unit="reqps")
timeseries("Bytes in / out", [(f'sum by (direction) (rate(lights3_bytes_total{{{INST}}}[$__rate_interval]))', "{{direction}}")], unit="Bps")

# ---------------- APIs ----------------
row("APIs (api × backend, roadmap §5.1)")
timeseries("Request rate by API", [(f'sum by (api) (rate(lights3_api_requests_total{{{INST},{BK}}}[$__rate_interval]))', "{{api}}")], unit="reqps")
timeseries("5xx by API", [(f'sum by (api, backend) (rate(lights3_api_requests_total{{{INST},{BK},class="5xx"}}[$__rate_interval]))', "{{api}} @ {{backend}}")], unit="reqps")
timeseries("P99 by API", [(q("lights3_api_request_duration_seconds", by=", api", sel=f'{INST},{BK}'), "{{api}}")], unit="s")
timeseries("P99 by backend (all APIs)", [(q("lights3_api_request_duration_seconds", by=", backend", sel=f'{INST},{BK}'), "{{backend}}")], unit="s")

# ---------------- HTTP / L1 ----------------
row("HTTP layer (L1, roadmap §4.2 / §5.3)")
timeseries("Connections", [(f'sum(lights3_http_connections_active{{{INST}}})', "active"),
                           (f'sum(rate(lights3_http_connections_total{{{INST},result="accepted"}}[$__rate_interval]))', "accepted/s"),
                           (f'sum(rate(lights3_http_connections_total{{{INST},result="rejected_limit"}}[$__rate_interval]))', "rejected (limit)/s")])
timeseries("Keep-alive reuse (requests per connection)",
           [(f'sum(rate(lights3_http_requests_total{{{INST}}}[$__rate_interval])) / clamp_min(sum(rate(lights3_http_connections_total{{{INST},result="accepted"}}[$__rate_interval])), 1e-9)', "requests / accepted"),
            (f'sum(rate(lights3_http_keepalive_closes_total{{{INST}}}[$__rate_interval]))', "budget closes/s")],
           description="httplib runs upstream's accept loop and reports 0 accepted (docs/http-adapter.md §2.2)")
timeseries("Timeouts by phase", [(f'sum by (phase) (rate(lights3_http_timeouts_total{{{INST}}}[$__rate_interval]))', "{{phase}}")], unit="reqps")
timeseries("Malformed requests / TLS handshakes",
           [(f'sum(rate(lights3_http_parse_errors_total{{{INST}}}[$__rate_interval]))', "parse errors/s"),
            (f'sum by (result) (rate(lights3_http_tls_handshakes_total{{{INST}}}[$__rate_interval]))', "tls {{result}}/s")], unit="reqps")
timeseries("Rate-limit rejections", [(f'sum by (scope) (rate(lights3_ratelimit_rejections_total{{{INST}}}[$__rate_interval]))', "{{scope}}")], unit="reqps")

# ---------------- Admission / pools ----------------
row("Admission gate & thread pools")
timeseries("Admission permits", [(f'sum(lights3_admission_capacity{{{INST}}})', "capacity"),
                                 (f'sum(lights3_admission_available{{{INST}}})', "available"),
                                 (f'sum(lights3_admission_waiting{{{INST}}})', "waiting")])
timeseries("Admission wait", [(q("lights3_admission_wait_seconds", quantile=P50), "p50"),
                              (q("lights3_admission_wait_seconds"), "p99"),
                              (f'sum(rate(lights3_admission_queued_total{{{INST}}}[$__rate_interval]))', "queued/s"),
                              (f'sum(rate(lights3_admission_cancelled_total{{{INST}}}[$__rate_interval]))', "cancelled/s")], unit="s",
           description="Wait for a runtime.max_inflight_requests permit; queued/cancelled are counts per second on the same axis")
timeseries("Transfer stalls cut", [(f'sum by (direction) (rate(lights3_transfer_stalls_total{{{INST}}}[$__rate_interval]))', "{{direction}}")], unit="reqps")
timeseries("Shared pool", [(f'sum(lights3_pool_queue_depth{{{INST}}})', "queue depth"),
                           (f'sum(lights3_pool_backlogged{{{INST}}})', "backlogged"),
                           (q("lights3_pool_wait_seconds"), "wait p99 (s)")])
timeseries("Backend pools (queue depth)", [(f'lights3_backend_pool_queue_depth{{{INST},{BK}}}', "{{backend}}")])
timeseries("Timer thread", [(f'max(lights3_timer_lag_seconds{{{INST}}})', "lag (s)"),
                            (f'sum(lights3_timer_pending{{{INST}}})', "pending"),
                            (f'sum(rate(lights3_timer_slow_callbacks_total{{{INST}}}[$__rate_interval]))', "slow callbacks/s")])

# ---------------- Backends ----------------
row("Storage backends (metering decorator, roadmap §5.1)")
timeseries("Backend op P99", [(q("lights3_backend_op_seconds", by=", backend, op", sel=f'{INST},{BK}'), "{{backend}} {{op}}")], unit="s")
timeseries("Backend errors (5xx / transport)", [(f'sum by (backend, op) (rate(lights3_backend_errors_total{{{INST},{BK}}}[$__rate_interval]))', "{{backend}} {{op}}")], unit="reqps")
timeseries("Backend op rate", [(f'sum by (backend) (rate(lights3_backend_op_seconds_count{{{INST},{BK}}}[$__rate_interval]))', "{{backend}}")], unit="reqps")
timeseries("Metadata cache", [(f'sum by (backend, result) (rate(lights3_meta_cache_lookups_total{{{INST},{BK}}}[$__rate_interval]))', "{{backend}} {{result}}"),
                              (f'sum by (backend) (lights3_meta_cache_entries{{{INST},{BK}}})', "{{backend}} entries")])
timeseries("Fallback flags (1 = degraded)", [(f'lights3_xlocalfs_uring_fallback{{{INST},{BK}}}', "uring fallback {{backend}}"),
                                              (f'lights3_localfs_xattr_fallback{{{INST},{BK}}}', "xattr fallback {{backend}}")], maxv=1)
timeseries("localfs ops / errors", [(f'sum by (backend, op) (rate(lights3_localfs_op_errors_total{{{INST},{BK}}}[$__rate_interval]))', "err {{backend}} {{op}}"),
                                    (f'sum by (backend) (rate(lights3_localfs_ops_total{{{INST},{BK}}}[$__rate_interval]))', "ops {{backend}}")], unit="reqps")

# ---------------- Buckets / tenancy ----------------
row("Buckets, usage & website")
timeseries("Top buckets by request rate", [(f'topk(10, sum by (bucket) (rate(lights3_bucket_requests_total{{{INST}}}[$__rate_interval])))', "{{bucket}}")], unit="reqps")
timeseries("Bucket usage (bytes)", [(f'topk(10, lights3_bucket_usage_bytes{{{INST}}})', "{{bucket}}")], unit="bytes")
timeseries("Quota rejections / usage scans", [(f'sum by (scope) (rate(lights3_quota_rejections_total{{{INST}}}[$__rate_interval]))', "quota reject {{scope}}"),
                                              (f'sum(rate(lights3_usage_scans_total{{{INST}}}[$__rate_interval]))', "usage scans/s")], unit="reqps")
timeseries("Website events", [(f'sum by (event) (rate(lights3_website_events_total{{{INST}}}[$__rate_interval]))', "{{event}}")], unit="reqps", stack=True)
timeseries("Multipart uploads active", [(f'sum(lights3_multipart_active{{{INST}}})', "active")])

# ---------------- Duostore ----------------
row("DuoStore (GC, packs, meta engines)", collapsed=False)
timeseries("GC queue", [(f'lights3_duostore_gcq_depth{{{INST},{BK}}}', "depth {{backend}}"),
                        (f'lights3_duostore_gcq_oldest_age_seconds{{{INST},{BK}}}', "oldest age (s) {{backend}}")],
           description="Convergence: depth should return to 0 after rounds; the head's age growing while rounds run is the 'not converging' alert")
timeseries("GC rounds & reclaims", [(f'sum by (backend) (rate(lights3_duostore_gc_runs_total{{{INST},{BK}}}[$__rate_interval]))', "rounds/s {{backend}}"),
                                    (f'sum by (backend, reason) (rate(lights3_duostore_gc_reclaims_by_reason_total{{{INST},{BK}}}[$__rate_interval]))', "reclaim {{reason}} {{backend}}"),
                                    (q("lights3_duostore_gc_round_seconds", by=", backend", sel=f'{INST},{BK}'), "round p99 (s) {{backend}}")])
timeseries("GC skips (gauge)", [(f'lights3_duostore_gc_skipped_grace{{{INST},{BK}}}', "grace {{backend}}"),
                                (f'lights3_duostore_gc_skipped_leased{{{INST},{BK}}}', "leased {{backend}}"),
                                (f'lights3_duostore_gc_skipped_pinned{{{INST},{BK}}}', "pinned {{backend}}"),
                                (f'lights3_duostore_gc_compact_deferred{{{INST},{BK}}}', "compact deferred {{backend}}")])
timeseries("Pack bytes (live vs total)", [(f'lights3_duostore_pack_bytes{{{INST},{BK}}}', "pack {{backend}}"),
                                          (f'lights3_duostore_pack_live_bytes{{{INST},{BK}}}', "live {{backend}}"),
                                          (f'lights3_duostore_chunk_bytes{{{INST},{BK}}}', "chunks {{backend}}")], unit="bytes")
timeseries("Packs", [(f'lights3_duostore_packs{{{INST},{BK}}}', "packs {{backend}}"),
                     (f'lights3_duostore_packs_quarantined{{{INST},{BK}}}', "quarantined {{backend}}"),
                     (f'sum by (backend) (rate(lights3_duostore_gc_packs_compacted_total{{{INST},{BK}}}[$__rate_interval]))', "compacted/s {{backend}}")])
timeseries("Corruption & orphans", [(f'sum by (backend) (increase(lights3_duostore_read_corruption_total{{{INST},{BK}}}[$__rate_interval]))', "read corruption {{backend}}"),
                                    (f'sum by (backend) (increase(lights3_duostore_pack_corrupt_records_total{{{INST},{BK}}}[$__rate_interval]))', "corrupt records {{backend}}"),
                                    (f'lights3_duostore_orphan_refs_missing{{{INST},{BK}}}', "orphan refs missing {{backend}}")])
timeseries("Meta engines", [(f'sum by (backend) (rate(lights3_duostore_sqlite_busy_total{{{INST},{BK}}}[$__rate_interval]))', "sqlite busy/s {{backend}}"),
                            (f'sum by (backend) (rate(lights3_duostore_redis_cas_retries_total{{{INST},{BK}}}[$__rate_interval]))', "redis CAS retries/s {{backend}}"),
                            (f'sum by (backend) (rate(lights3_duostore_tikv_txn_conflict_retries_total{{{INST},{BK}}}[$__rate_interval]))', "tikv conflicts/s {{backend}}"),
                            (f'sum by (backend) (rate(lights3_duostore_rados_op_errors_total{{{INST},{BK}}}[$__rate_interval]))', "rados errors/s {{backend}}"),
                            (f'lights3_duostore_rocksdb_estimate_num_keys{{{INST},{BK}}}', "rocksdb keys {{backend}}")])

# ---------------- Tiered ----------------
row("Tiered storage")
timeseries("Reads by source", [(f'sum by (backend, source) (rate(lights3_tiered_get_source_total{{{INST},{BK}}}[$__rate_interval]))', "{{backend}} {{source}}")], unit="reqps", stack=True)
timeseries("Demotion / promotion / eviction", [(f'sum by (backend) (rate(lights3_tiered_demoted_objects_total{{{INST},{BK}}}[$__rate_interval]))', "demoted/s {{backend}}"),
                                                (f'sum by (backend) (rate(lights3_tiered_promoted_objects_total{{{INST},{BK}}}[$__rate_interval]))', "promoted/s {{backend}}"),
                                                (f'sum by (backend) (rate(lights3_tiered_evicted_bytes_total{{{INST},{BK}}}[$__rate_interval]))', "evicted B/s {{backend}}")],
           description="Sustained eviction = the local tier sits above space_high_watermark (no local-usage gauge is exported)")
timeseries("Cloud GC", [(f'lights3_tiered_gc_deferred{{{INST},{BK}}}', "deferred {{backend}}"),
                        (f'sum by (backend) (rate(lights3_tiered_gc_failed_total{{{INST},{BK}}}[$__rate_interval]))', "failed/s {{backend}}"),
                        (f'sum by (backend) (rate(lights3_tiered_gc_removed_cloud_total{{{INST},{BK}}}[$__rate_interval]))', "removed/s {{backend}}")])
timeseries("Scans & quarantine", [(q("lights3_tiered_scan_seconds", by=", backend", sel=f'{INST},{BK}'), "scan p99 (s) {{backend}}"),
                                  (f'sum by (backend, mode) (rate(lights3_tiered_scan_rounds_total{{{INST},{BK}}}[$__rate_interval]))', "rounds/s {{mode}} {{backend}}"),
                                  (f'lights3_tiered_quarantine_entries{{{INST},{BK}}}', "quarantine {{kind}} {{backend}}")])
timeseries("Range cache", [(f'sum by (backend, result) (rate(lights3_tiered_range_cache_total{{{INST},{BK}}}[$__rate_interval]))', "{{result}} {{backend}}")], unit="reqps", stack=True)
timeseries("Tiered op errors", [(f'sum by (backend, op) (rate(lights3_tiered_op_errors_total{{{INST},{BK}}}[$__rate_interval]))', "{{backend}} {{op}}")], unit="reqps")

# ---------------- Cloudproxy ----------------
row("CloudProxy (remote S3)")
timeseries("Remote request P99 by op", [(q("lights3_cloudproxy_remote_request_seconds", by=", backend, op", sel=f'{INST},{BK}'), "{{backend}} {{op}}")], unit="s")
timeseries("Retry ratio", [(f'sum by (backend) (rate(lights3_cloudproxy_retries_total{{{INST},{BK}}}[$__rate_interval])) / clamp_min(sum by (backend) (rate(lights3_cloudproxy_remote_request_seconds_count{{{INST},{BK}}}[$__rate_interval])), 1e-9)', "{{backend}}")],
           unit="percentunit", maxv=1)
timeseries("Remote errors by code", [(f'sum by (backend, code) (rate(lights3_cloudproxy_remote_errors_total{{{INST},{BK}}}[$__rate_interval]))', "{{backend}} {{code}}")], unit="reqps")
timeseries("Connection pool wait / ETag mismatches", [(q("lights3_cloudproxy_pool_wait_seconds", by=", backend", sel=f'{INST},{BK}'), "pool wait p99 (s) {{backend}}"),
                                                       (f'sum by (backend) (increase(lights3_cloudproxy_etag_mismatch_total{{{INST},{BK}}}[$__rate_interval]))', "etag mismatch {{backend}}")])

dashboard = {
    "__inputs": [{"name": "DS", "label": "Prometheus", "type": "datasource", "pluginId": "prometheus"}],
    "title": "lights3",
    "uid": "lights3-overview",
    "tags": ["lights3", "s3"],
    "timezone": "browser",
    "editable": True,
    "graphTooltip": 1,
    "schemaVersion": 39,
    "version": 1,
    "refresh": "30s",
    "time": {"from": "now-1h", "to": "now"},
    "templating": {"list": [
        {"name": "DS", "label": "Prometheus", "type": "datasource", "query": "prometheus", "current": {}, "hide": 0},
        {"name": "instance", "label": "Instance", "type": "query", "datasource": {"type": "prometheus", "uid": "${DS}"},
         "query": {"query": 'label_values(lights3_requests_total, instance)', "refId": "inst"},
         "includeAll": True, "multi": True, "allValue": ".*", "current": {"text": "All", "value": "$__all"}, "refresh": 2, "sort": 1},
        {"name": "backend", "label": "Backend", "type": "query", "datasource": {"type": "prometheus", "uid": "${DS}"},
         "query": {"query": 'label_values(lights3_backend_op_seconds_count{instance=~"$instance"}, backend)', "refId": "bk"},
         "includeAll": True, "multi": True, "allValue": ".*", "current": {"text": "All", "value": "$__all"}, "refresh": 2, "sort": 1},
    ]},
    "annotations": {"list": [{"builtIn": 1, "datasource": {"type": "grafana", "uid": "-- Grafana --"}, "enable": True,
                              "hide": True, "iconColor": "rgba(0, 211, 255, 1)", "name": "Annotations & Alerts", "type": "dashboard"}]},
    "panels": panels,
}


def render():
    return json.dumps(dashboard, indent=2, sort_keys=False) + "\n"


if __name__ == "__main__":
    out = os.path.join(os.path.dirname(os.path.abspath(__file__)), "lights3.json")
    if len(sys.argv) > 1 and sys.argv[1] == "--check":
        with open(out) as f:
            if f.read() != render():
                print("lights3.json is stale: run python3 deploy/grafana/gen_dashboard.py", file=sys.stderr)
                sys.exit(1)
        print("lights3.json up to date")
    else:
        with open(out, "w") as f:
            f.write(render())
        print(f"wrote {out}: {len(panels)} panels")
