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
| TLS certificate material | every reload forces `Holder::reload_now()` (no waiting for the `tls_reload_interval` poll); seastar's reloadable credentials watch the files themselves |

## 4. Explicitly Not Hot-Reloadable (reported as requires_restart)

- `http.driver / bind / port / admin_bind / admin_port / io_threads / max_header_size`, the four
  connection timeouts, `max_requests_per_connection`, `max_connections` (fixed
  at driver construction);
- the TLS **paths and knobs** (`tls_cert/tls_key` paths, `tls_client_*`,
  `tls_min_version`, ciphers, `tls_sni`, `tls_reload_interval`) — certificate
  **contents** reload, parameters do not;
- `backends` (adding/removing instances or changing their parameters touches
  lifetimes and data) and `buckets.default_backend` (hosts `.sys` and every
  store loaded from it);
- `auth.*` (static root credentials, credentials-file path, sync period) —
  dynamic credentials and the credentials file already have their own reload
  channels;
- static `website` entries (dynamic ones go through `?website`),
  `lifecycle.scan_interval`, `usage.*`, `audit.*`, `ratelimit.max_tracked`,
  shutdown/backpressure boundaries;
- `log.format / file / max_size / max_files / async*` — sink and formatter are
  built once in `Logger::init` (roadmap §5.2).

## 5. Deferred

- Hot add/remove of backend instances: touches `StorageRegistry` lifetimes and
  `.sys` placement; decide the target scenario first (the roadmap's "separate
  discussion").
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
  the JSON report, failure 400).
- e2e: `SIGHUP` after changing `log.level` and the log line; `request_timeout`
  applied through the admin API and `s3adm reload`; non-root 403; an invalid
  file answers 400.
