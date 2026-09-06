# Configuration Hot Reload (roadmap §4.4)

> Status: landed (2026-09-05). Code: `Application::reload_config`
> (`src/app/app.cc`), `storage::BucketRouter::update`,
> `AsyncSemaphore::set_capacity`, `POST /-/admin/config/reload`
> (`src/s3/handlers/admin_tenants.cc`), `s3adm reload`. Unit tests in
> `tests/unit/test_reload.cc`, e2e in the "roadmap §4.4" section of `run_e2e.sh`.

## 1. Triggers

| Trigger | Notes |
| --- | --- |
| `kill -HUP <pid>` | delivered through the self-pipe to the watchdog thread (the signal handler writes one byte), the same mechanism as SIGINT/SIGTERM; a systemd unit can use `ExecReload=/bin/kill -HUP $MAINPID` |
| `POST /-/admin/config/reload` | root static credential; returns the JSON report below; writes a `config.reload` audit record |
| `s3adm reload` | CLI wrapper of the above; exit code 0/1 mirrors `ok` |

Both paths share one lock and run serially; a reload never blocks the request
path (file IO and the apply steps run on the watchdog thread or inside the admin
request's coroutine).

## 2. Semantics: Validate Whole, Apply the Subset, Report the Rest

1. `Config::load(path)` again — **exactly the startup parser and validation**. Any
   error (syntax, ranges, cross-item consistency, a bucket rule naming an unknown
   backend) refuses the whole reload; the running configuration is untouched and
   the report says `ok=false` + `error`.
2. Once valid, only the **hot-reloadable subset** (§3) is applied, each item
   logged as INFO `config reload: applied …`.
3. Keys outside the subset that changed on disk are listed under
   `requires_restart` and WARNed one by one — the operator sees "changed but not
   in effect" immediately instead of never.

Report shape (admin API / `s3adm reload` output):

```json
{
  "ok": true,
  "applied": ["log.level: info -> debug", "http.request_timeout: 300 -> 120",
              "buckets.rules: 0 -> 1 rule(s)", "http.tls: certificate material re-read"],
  "requires_restart": ["http.max_connections"]
}
```

## 3. Hot-Reloadable Subset

| Key | How it takes effect |
| --- | --- |
| `log.level` | spdlog's global level switches immediately |
| `log.slow_request_threshold` | from the next request (dispatch reads an atomic at its end; streaming responses judge at end of body) |
| `http.request_timeout` | from the next request (dispatch reads an atomic per request) |
| `http.transfer_stall_timeout` | from the next request (the admission handler reads an atomic per request) |
| `http.min_part_size` | from the next CompleteMultipartUpload |
| `http.metrics_access` | from the next `GET /-/metrics` (dispatch reads an atomic) |
| `runtime.max_inflight_requests` | `AsyncSemaphore::set_capacity`: growing wakes queued requests at once; shrinking waits for in-flight permits to return (`available` may go negative briefly, nothing new is admitted meanwhile) |
| `ratelimit.per_ip_* / per_ak_*` | limiters are rebuilt and swapped atomically; in-flight requests hold the old instance until they finish, so nothing dangles (`max_tracked` excepted: restart only) |
| `buckets.rules` | `BucketRouter::update` swaps the rule table atomically; the router copies held by `S3Service`, the lifecycle runner and the usage tracker share one table; a request in flight keeps the table it resolved against |
| `backends[]` **new entries** | backlog-sequence ⑦: built per config by `StorageRegistry::build` (a new tiered entry may name running backends), wrapped by `meter_backends`, swapped into the router in the **same snapshot** as the rules (rules may point at the new backend at once); the fsck job table and the `backend=` metric label follow. A construction failure (e.g. localfs without root) refuses the reload as a whole and the instances built so far are closed again |
| `backends[]` **removed entries** | Conditions: not the `default_backend`, no tiered entry in the file still references it, no fsck job running on it (the last two refuse the whole reload, the first defers into requires_restart). The backend leaves the routing table first (new requests follow the remaining rules immediately); a **retiring thread** then waits for its in-flight leases to reach zero (`MeteredBackend::wait_idle` — every call and every open get_object stream holds one) before `close()`, and finally drops its metric series; log line `backend <name> removed: closed after in-flight requests drained`. The wait is unbounded (a line every 10 s while waiting); process shutdown cuts it short and closes |
| `backends[]` parameter change of an existing entry | **Not applied**: instances carry state, rebuilding equals a restart; reported per entry in requires_restart (`backends (<name>: type/parameters changed …)`), the running instance keeps its startup configuration. Reordering entries is not a change (matched by name) |
| TLS certificate material | every reload forces `Holder::reload_now()` (no waiting for the `tls_reload_interval` poll); seastar's reloadable credentials watch the files themselves |

## 4. Explicitly Not Hot-Reloadable (reported as requires_restart)

- `http.driver / bind / port / admin_bind / admin_port / io_threads / max_header_size`, the four
  connection timeouts, `max_requests_per_connection`, `max_connections` (fixed
  at driver construction);
- the TLS **paths and knobs** (`tls_cert/tls_key` paths, `tls_client_*`,
  `tls_min_version`, ciphers, `tls_sni`, `tls_reload_interval`) — certificate
  **contents** reload, parameters do not;
- parameter changes of existing `backends[]` entries (see §3: add / remove
  yes, modify no) and `buckets.default_backend` (hosts `.sys` and every store
  loaded from it; removing the default backend is deferred for the same reason);
- `auth.*` (static root credentials, credentials-file path, sync period, the
  `tls_identity` mode) — dynamic credentials, the credentials file and the
  certificate binding table already have their own reload / sync channels;
- static `website` entries (dynamic ones go through `?website`),
  `lifecycle.scan_interval`, `usage.*`, `audit.*`, `ratelimit.max_tracked`,
  shutdown/backpressure boundaries;
- `log.format / file / max_size / max_files / async*` — sink and formatter are
  built once in `Logger::init` (roadmap §5.2).

## 5. Deferred

- Hot add / remove of backend instances landed (§3, 2026-09-06); **changing
  parameters** of a running instance still needs a restart — rebuilding a
  stateful instance (duostore meta handles, tiered demotion tables, the
  cloudproxy connection pool) is a restart in all but name.
- Automatic mtime polling of the config file: SIGHUP / the admin API are enough
  and more deliberate, and avoid applying a half-written file.

## 6. Tests

- `test_reload.cc`: semaphore resize (growing wakes a queued waiter, shrinking
  goes negative and recovers); atomic router swap (copies share it, unknown
  backend / changed default backend refused with the old table kept);
  `Application`-level end to end (empty report on no change; each subset key
  applied and a startup-only key reported; a broken file refused as a whole with
  running values unchanged; a rule naming an unknown backend refused before
  anything is applied); the admin endpoint (unsigned / non-root 403, GET 405,
  the JSON report, failure 400). Backend hot add / remove (⑦): rules and
  backend set swapped as one snapshot, old snapshots untouched, the default
  backend cannot be dropped; `MeteredBackend` leases (an open get_object stream
  counts as in flight, `wait_idle` wakes when the last lease returns);
  the dynamic `FsckJobs` set; `Application`-level — a memory backend added and
  routed to (real HTTP requests land on it), removal refused while a rule still
  names it, a parameter change only reported, removal with a stream open applies
  at once while the instance closes only after the stream, removing the default
  backend deferred, a tiered entry naming a removed backend / a backend that does
  not construct refused as a whole.
- e2e: `SIGHUP` after changing `log.level` and the log line; `request_timeout`
  applied through the admin API and `s3adm reload`; non-root 403; an invalid
  file answers 400; a memory backend `hot` added with a `hot-*` rule (PUT lands
  on it, `backend="hot"` appears on `/-/metrics`, `s3adm object inspect` sees
  it), removed while a rate-limited GET streams from it — the reload reports the
  removal at once, `hot-*` routes to the default backend immediately, the close
  log line appears only after the stream ends, the metric label disappears;
  removing the default backend is only deferred.
