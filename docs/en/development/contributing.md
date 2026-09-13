# Development guide

> English translation of [../../development/contributing.md](../../development/contributing.md).
> Section numbering matches the Chinese original.

For anyone changing code or documentation: how the repository is organised,
how to build and verify, and the conventions code and docs follow. The test
system is detailed in [testing.md](testing.md), performance reproduction in
[performance-baseline.md](performance-baseline.md), open items in
[todo.md](todo.md).

## 1. Repository layout

| Directory | Contents |
| --- | --- |
| `src/core` | Cross-cutting pieces: coroutine `Task` / Executor / thread pool, config, logging, metrics, TLS |
| `src/http` | The neutral HTTP model and the four drivers (builtin / beast / httplib / seastar) |
| `src/s3` | Router, SigV4, S3 handlers, XML codec, error mapping, website / credential / tenant planes |
| `src/storage` | `IStorageBackend` and the backends: localfs / xlocalfs / memory / tiered / cloudproxy / duostore |
| `src/tables` | S3 Tables (Iceberg REST catalog) |
| `src/app`, `src/cli`, `src/tools` | Process assembly, `lights3` subcommands, `lights3-ctl` |
| `tests/unit`, `tests/e2e`, `tests/fuzz`, `tests/fixtures` | Unit tests (the in-tree `mini_test.h` framework), e2e scripts, libFuzzer harnesses and corpora, fixtures |
| `config/lights3.yaml` | The authoritative description of every config key and default |
| `scripts/`, `packaging/`, `docker/`, `deploy/` | Install / packaging / container / monitoring assets |
| `third_party/` | Submodules (ccmd, spdlog, httplib, json, rocksdb, hiredis, sqlite, client-c, seastar) and local patches |

Layering and the request lifecycle: [architecture/overview.md](../architecture/overview.md).

## 2. Building

```bash
./build.sh --test                 # submodules + cmake + ninja + ctest in one go
make debug / make release         # Debug into build/, Release into build-rel/
make test                         # Debug build, then the quick ctest set (no perf / soak / mint)
make coverage                     # -O0 --coverage build in build-cov/ plus a line-coverage report
make package                      # Release + CPack (deb / rpm / tgz)
```

- `build.sh` switches: `--redis` / `--sqlite` / `--tikv` / `--rados` enable the
  optional duostore engines, `--seastar` the seastar driver, `--asan` / `--tsan` /
  `--ubsan` / `--coverage` / `--fuzz` the corresponding variants. **These CMake
  options are sticky in the cache**: give every variant its own directory
  (`-B build-redis`, `build-tikv`, `build-rados`, `build-asan`, …) instead of
  flipping `cmake -D` inside `build/`; rebuild incrementally with
  `cmake --build build-xxx`.
- Parallelism defaults to half the cores (`make JOBS=N` overrides). Under a large
  parallel build io_uring may return `ENOMEM`; if an xlocalfs unit test fails
  sporadically, rerun with a lower `-j`.
- The ccmd submodule needs `--recursive` (it nests cflag); `third_party/patches/`
  holds patches not yet merged upstream, with their notes.

## 3. Verifying

- Quick set: `make test`, or `ctest --test-dir build -LE "perf|soak|mint"`. The
  e2e sections bind fixed ports and share temp directories, so ctest runs serially.
- Full matrix: `scripts/check-all.sh` does an incremental build + ctest for every
  existing `build-*` directory; `--with-perf` / `--with-soak` /
  `--with-tables-smoke` add the gates and the smoke runs. Data-plane changes also
  run `scripts/bench_gate.sh`. The project does not use GitHub Actions; all
  verification is local scripts.
- New test cases go inside the test file's `#endif` guard; when an external
  dependency (redis / PD / rados) is missing the case SKIPs explicitly, never
  passes silently.
- `unit_tests` output contains NUL bytes, so filter with `grep -a`; for a
  sporadic terminate, capture a stack with `catch throw` / `ulimit -c` rather
  than relying on an outer `timeout`.
- Filter build output with `grep -E "error|FAILED"` and check the binary
  timestamps to confirm it really relinked.

## 4. Code conventions

- Formatting: `.clang-format` (Google style, 4-space indent, 120 columns).
  `make format` first moves trailing comments above their code
  (`scripts/check_comments.py`), then runs clang-format; `make format-check`
  only reports. Both process the files in `git ls-files`, so **`git add -N` a
  new file before formatting**.
- Comment rule: a `//` comment sits on its own line above the code it describes;
  the only trailing comments allowed are closers such as `}  // namespace x` and
  `#endif  // GUARD`.
- Referencing design documents: comments say `docs/<group>/<name>.md §N` (for
  example `docs/architecture/storage/duostore-design.md §5.2`); section numbers
  are identical in both languages, so the reference works for either. `roadmap
  §N` / `backlog §N` / `backlog-sequence ①…⑩` / `gaps` / `issues T<n>` refer to
  the read-only ledgers under [../../archive/](../../archive/) (Chinese only).
- Coroutines: no `co_await` inside a `catch` block; waiting inside a coroutine
  goes through the `drive` path of `sync_wait` / `Started::wait` or it deadlocks;
  GCC 15 ICEs on `co_await` of a member pointer, so store it in a local first;
  replace structured bindings inside coroutines with a named pair to avoid
  `maybe-uninitialized`. Details in
  [architecture/coroutine-internals.md](../architecture/coroutine-internals.md).
- Third-party targets always get `target_compile_options(-w)`; debug and
  release builds stay at zero warnings.
- A new `?flag` route goes before the `flag=""` fallback; new metrics attach
  through `MetricsScope` (see `DuoStoreBackend::init_metrics`).
- To change the Grafana dashboard edit `deploy/grafana/gen_dashboard.py` first,
  then regenerate the json.

## 5. Documentation conventions

- Three groups: [architecture/](../architecture/) (design and implementation
  rationale), [usage/](../usage/) (deployment and operations),
  [development/](./) (build, tests, open items). Chinese is the original,
  `docs/en/` mirrors it with **identical section numbering**; a change in one
  is mirrored in the other. The 13 storage implementation documents exist in
  Chinese only.
- Design documents explain "why", implementation documents explain "how the
  code does it"; they cross-link and do not repeat each other.
- Open items live only in [todo.md](todo.md) and are deleted when done, with
  the implementation written back into the relevant design document; no
  strike-through history. The archived ledgers under `docs/archive/` are
  read-only.
- Config keys are documented by the comments in `config/lights3.yaml`; the
  docs do not restate defaults.

## 6. Branches and commits

- Branch off `main` (`feat/…`, `fix/…`, `chore/…`, `docs/…`) and **check the
  branch before committing**.
- Commit messages state the motivation and how the change was verified; "fixed"
  means the relevant test was actually run.
- Push the branch and open a PR on GitHub; run the quick set of §3 before
  merging, and the matching `build-*` when an optional engine is touched.
- Never run `git submodule update` inside a worktree (it empties the main
  tree's submodules).
