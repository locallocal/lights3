# Build and distribution: version stamp, install tree, deb/rpm, containers, rollback

The roadmap §6.3 write-up. Five things: the **version / build identity** inside
the binaries, the `cmake --install` **install tree**, CPack-generated
**deb / rpm** packages, **Dockerfile + compose** (a quick-start demo that also
runs the redis / tikv / rados e2e paths which stay SKIP on a developer box), and
**rollback / uninstall** for `install.sh` upgrades. Every channel shares one
post-install helper (`packaging/lights3-setup.sh`), so they behave the same.

## 1. Version and build identity

`project(lights3 VERSION x.y.z)` is the single source of the version number. The
git commit is stamped at **build** time by `cmake/GenerateVersion.cmake` (script
mode, run by the `lights3_version_gen` target on every build) into
`build/generated/version_info.cc`; the file is rewritten only when its content
changes, so incremental builds that do not move HEAD do not relink. Trees
without `.git` (source tarballs, Docker build contexts) pass
`-DLIGHTS3_GIT_COMMIT=<hash>`, otherwise the commit reads `unknown`; uncommitted
changes to tracked files append `-dirty`.

Visible in three places:

```text
$ lights3 --version                 # s3adm --version has the same shape
lights3 0.1.0 (git d6f38292dab0, RelWithDebInfo, 2026-09-05)
drivers:  builtin beast httplib
features: memory localfs xlocalfs tiered cloudproxy duostore duostore-redis-meta duostore-sqlite-meta
```

- The first startup log line: `lights3 0.1.0 (git …, RelWithDebInfo) started: driver=… backends=… pool=…`.
- The gauge `lights3_build_info{version,commit,build_type} 1` (the Prometheus
  `*_build_info` idiom): a fleet dashboard groups by label to see which build
  each instance runs.

`features` lists compile-time switches (the `LIGHTS3_*` CMake options), not the
backends configured at runtime; `--check-config` checks `backends[].type`
against this list. `--version` wins over every other root-level flag. The API is
`src/core/version.h`.

## 2. Install tree (`cmake --install`)

```bash
cmake -B build -DLIGHTS3_BUILD_TESTS=OFF && cmake --build build -j
sudo cmake --install build                    # default prefix /usr/local
cmake --install build --prefix ~/.local       # non-root: everything under the prefix
DESTDIR=/tmp/stage cmake --install build --prefix /usr   # staging for packaging
```

| Path | Content |
| --- | --- |
| `<bindir>/lights3`, `<bindir>/s3adm` | the binaries |
| `<sbindir>/lights3ctl` | `scripts/systemctl.sh` (start/stop/restart/status/logs …) |
| `<prefix>/lib/systemd/system/lights3.service` | rendered from `scripts/lights3.service.in` **at install time** with the prefix's `ExecStart` / `EnvironmentFile`; carries `ExecReload=kill -HUP` (hot reload, [config-reload.md](config-reload.md)) |
| `<confdir>/lights3.yaml` | the `config/lights3.yaml` sample; **an existing file is preserved** |
| `<datadir>/lights3/lights3-setup.sh` | the post-install helper (§3.3) |
| `<datadir>/lights3/deploy/{prometheus,grafana}` | monitoring assets ([monitoring.md](monitoring.md)) |
| `<docdir>/README.md`, `<docdir>/docs/` | documentation (minus `archive/`) |

The config directory `<confdir>` is `/etc/lights3` when the prefix is `/usr` or
`/usr/local` (what `install.sh` and every document assume) and
`<prefix>/etc/lights3` for any other prefix; `-DLIGHTS3_CONFIG_DIR=<absolute>`
overrides. Both the unit file and the config directory are resolved at install
time against `--prefix`, so `cmake --install --prefix X` relocates consistently.
Third-party subdirectories (rocksdb/spdlog/…) are added with `EXCLUDE_FROM_ALL`,
so their own `install()` rules stay out of the tree.

ctest `install_tree` (`tests/packaging/check_install.sh`) installs the tree
into a scratch prefix on every run and checks layout, unit relocation,
config preservation, the `--version` format and `sh -n` on the maintainer
scripts — no root, no packaging tools needed.

## 3. deb / rpm (CPack)

```bash
cmake --build build --target package        # or: cd build && cpack
cpack --config build/CPackConfig.cmake -G DEB   # explicit generator: DEB | RPM | TGZ
ls build/packages/                          # lights3_0.1.0_amd64.deb / lights3-0.1.0-1.x86_64.rpm
```

Generators follow the tools present: `dpkg-deb` → DEB, `rpmbuild` → RPM,
neither → TGZ (the install tree as an archive; absolute paths such as `/etc`
end up under the archive's `usr/` — it is a build artifact, not a system
image). The packaged prefix is `/usr`; binaries are stripped.

### 3.1 deb

- Dependencies computed by `dpkg-shlibdeps` (`libc6 / libstdc++6 / libssl3t64 …`)
  plus `adduser, openssl`; a hand-written list is the fallback without shlibdeps.
- `/etc/lights3/lights3.yaml` is a **conffile**: upgrades go through dpkg's
  standard keep / merge prompt, never a silent overwrite.
- The maintainer scripts (`packaging/deb/`) only forward: `postinst configure`
  → `lights3-setup.sh configure`; `prerm remove` → `remove`; `postrm purge` →
  `purge` (deletes `/etc/lights3`, `/var/lib/lights3`, `/var/log/lights3` and
  the `lights3` user; a plain `remove` keeps all of them).

```bash
sudo apt install ./build/packages/lights3_0.1.0_amd64.deb
sudo lights3ctl status
sudo apt remove lights3      # keeps config / data / secrets
sudo apt purge lights3       # removes them too
```

### 3.2 rpm

`%post` / `%preun` / `%postun` come from `packaging/rpm/` and forward to the
helper as well; `lights3.yaml` is `%config(noreplace)` (a new sample lands as
`.rpmnew` on upgrade); `Requires: shadow-utils, openssl, systemd` plus automatic
shared-library requirements. rpm has no purge: secrets / data / user stay behind
on purpose (`postun.sh` comments the manual cleanup). The development box behind
this repository has no `rpmbuild`, so the RPM rules are **not verified locally**.

### 3.3 Post-install logic: `packaging/lights3-setup.sh`

POSIX sh (dpkg/rpm run maintainer scripts under dash), three subcommands:

| Subcommand | Action |
| --- | --- |
| `configure [--no-enable] [--no-start]` | create the `lights3` system user/group; `/etc/lights3` (0750 root:lights3), `/var/lib/lights3`, `/var/log/lights3`; on the **first** install generate `/etc/lights3/lights3.env` (`LIGHTS3_SECRET_1` + `LIGHTS3_MASTER_KEY` from `openssl rand`, 0640); `daemon-reload` → enable → validate the live config with the new binary's `--check-config` and **only then** start / restart, otherwise warn and leave the service alone |
| `remove` | stop + disable |
| `purge` | delete secrets, config, data, logs, user/group |

Paths come from `LIGHTS3_BIN` / `LIGHTS3_CONFIG_DIR` / `LIGHTS3_STATE_DIR` /
`LIGHTS3_LOG_DIR`, which is how `install.sh` reuses it for the `/usr/local`
layout. Without systemd (containers) every systemctl call is skipped.

## 4. Docker and compose

### 4.1 The image

Three stages in `Dockerfile`: `builder` (ubuntu:24.04 + toolchain,
`cmake --install` into `/stage`) → `runtime` (the two binaries, the setup
helper and the monitoring assets only; non-root user `lights3`; `HEALTHCHECK`
on `/-/healthz`) → `e2e` (builder + curl/python3, optionally the ceph CLI; the
test runner of §4.3).

```bash
git submodule update --init third_party/{spdlog,httplib,json,rocksdb,hiredis,sqlite}
git submodule update --init --recursive third_party/ccmd     # or run ./build.sh once
docker build -t lights3 --build-arg LIGHTS3_GIT_COMMIT=$(git rev-parse --short=12 HEAD) .
docker run -d -p 9000:9000 -e LIGHTS3_SECRET_1=my-secret -v lights3-data:/var/lib/lights3 lights3
docker run --rm lights3 --version
docker run --rm lights3 s3adm --help
```

| build-arg | Default | Meaning |
| --- | --- | --- |
| `LIGHTS3_REDIS` / `LIGHTS3_SQLITE` | `ON` | duostore's zero-external-dependency meta engines |
| `LIGHTS3_RADOS` | `OFF` | `librados-dev` at build time, `librados2` at runtime |
| `LIGHTS3_TIKV` | `OFF` | `libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev libpoco-dev libabsl-dev` ([duostore-tikv-meta.md §8](duostore-tikv-meta.md)) |
| `LIGHTS3_GIT_COMMIT` | `unknown` | the build context has no `.git` (`.dockerignore`); the commit comes in here |
| `CMAKE_BUILD_TYPE` | `Release` | |
| `BASE` | `ubuntu:24.04` | a Debian base works too (`libssl3` name fallback) |

The image config `deploy/docker/lights3.yaml`: builtin driver, `0.0.0.0:9000`,
localfs under the `/var/lib/lights3` volume; `LIGHTS3_SECRET_1` is mandatory
(the entrypoint exits 2 without it instead of starting an instance that
rejects every request); `LIGHTS3_ACCESS_KEY` / `LIGHTS3_REGION` /
`LIGHTS3_LOG_LEVEL` / `LIGHTS3_LOG_FORMAT` are optional. Entrypoint rule: no
arguments, or a first argument starting with `-` / `duostore` / `tier` /
`fsck` / `help` → `lights3 --config=… "$@"`; anything else (`s3adm …`, `sh`) is
exec'd as given.

The seastar driver is out of scope for the image (`.dockerignore` drops its
submodule). **The docker daemon on this repository's development box is
unreachable, so the image build and the compose stacks are not verified
locally**; `docker compose config` passes and the apt package names were
checked against Ubuntu 24.04.

### 4.2 compose quick start

```bash
docker compose up -d                          # :9000, AKIDEXAMPLE / lights3-demo-secret
LIGHTS3_SECRET_1=... docker compose up -d      # your own secret
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
docker compose --profile redis up -d          # adds :9001 = duostore + redis meta
docker compose --profile tikv up -d           # :9002 = duostore + TiKV meta (pd0 + tikv0, image lights3:full)
docker compose --profile rados up -d          # :9003 = duostore + RADOS data (ceph/demo single node, image lights3:full)
```

| profile | Services | Notes |
| --- | --- | --- |
| (default) | `lights3` | image `lights3:local`, localfs |
| `redis` | `redis` (`redis:7-alpine`, AOF on), `lights3-redis` | `deploy/docker/lights3-redis.yaml` mounted read-only as `/etc/lights3/lights3.yaml` |
| `tikv` | `pd0`, `tikv0` (`pingcap/{pd,tikv}:${TIKV_VERSION:-v8.5.2}`), `lights3-tikv` | single PD, single TiKV; `lights3:full` is built with `LIGHTS3_RADOS=ON LIGHTS3_TIKV=ON` |
| `rados` | `ceph` (`${CEPH_IMAGE:-quay.io/ceph/demo:latest}`, fixed IP 172.28.0.10), `rados-init` (one-shot pool creation), `lights3-rados` | `ceph.conf` + admin keyring shared read-only through the `ceph-etc` volume; the keyring is root-only, so the consumers run as root |

`LIGHTS3_GIT_COMMIT=$(git rev-parse --short=12 HEAD) docker compose build`
stamps the images. Data lives in named volumes (`lights3-data`, …);
`docker compose down -v` wipes them.

### 4.3 Running the SKIP'd e2e paths

The `duostore-redis` / `duostore-tikv` / `duostore-rados` branches of
`run_e2e.sh` each probe an external dependency and SKIP explicitly without it
([testing.md §1](testing.md)). The `e2e` compose profile brings all three
dependencies up and runs ctest inside the `lights3:e2e` image:

```bash
docker compose --profile e2e run --rm e2e                                  # all three
docker compose --profile e2e run --rm -e E2E_TESTS=e2e_duostore_redis e2e  # one of them
docker compose --profile e2e down -v
```

`deploy/docker/e2e.sh` waits for the services (redis TCP; a store in `Up` state
on PD's `/pd/api/v1/stores`; `ceph -s` succeeding and the `lights3-e2e` pool
created), then runs `ctest -R "$E2E_TESTS"`. The environment variables are the
probes `run_e2e.sh` already had:

| Variable | Value | Effect |
| --- | --- | --- |
| `LIGHTS3_TEST_REDIS_URI` | `redis://redis:6379` | **new**: point at an external instance instead of spawning `redis-server`; each run isolates itself with a unique `redis_prefix` (`e2e-<pid>-<rand>:`), the TLS second instance appends `tls-` |
| `LIGHTS3_TEST_PD_ADDR` | `pd0:2379` | existing; unique `tikv_prefix` per run |
| `LIGHTS3_TEST_RADOS_CONF` / `_POOL` | `/etc/ceph/ceph.conf` / `lights3-e2e` | existing; unique `rados_namespace` per run |

`LIGHTS3_TEST_REDIS_URI` works on a developer box as well
(`redis-server --port 16399 &`, then
`LIGHTS3_TEST_REDIS_URI=redis://127.0.0.1:16399 ctest -R e2e_duostore_redis`);
both the external and the spawned path were verified locally at 202/202.

## 5. Upgrade, rollback, uninstall (the `install.sh` channel)

```bash
sudo ./scripts/install.sh            # upgrade: keeps config / secrets, old binaries become *.prev
sudo ./scripts/rollback.sh           # swap *.prev back and restart; running it again swaps forward
sudo ./scripts/uninstall.sh          # remove binaries / unit / lights3ctl, keep /etc/lights3 and data
sudo ./scripts/uninstall.sh --purge  # also delete config, secrets, data, logs and the user
```

- **install.sh** validates the live `/etc/lights3/lights3.yaml` with the
  **new** binary's `--check-config` before touching anything (sourcing
  `lights3.env` so `${LIGHTS3_SECRET_1}` expands) and aborts when it is
  rejected — an upgrade never restarts the service into a config it cannot
  start with. Binaries are replaced by atomic rename and the replaced file
  becomes `<name>.prev` (one generation; identical content keeps none). The
  unit file is rendered from `scripts/lights3.service.in` with sed for
  `/usr/local/bin` and `/etc/lights3`. User / directories / secrets /
  start-stop are delegated to `lights3-setup.sh` (§3.3).
- **rollback.sh**: runs `--check-config` with `lights3.prev` first (an older
  binary rejects config keys it does not know — revert the config by hand in
  that case), then swaps `lights3` / `s3adm` with their `.prev` in three
  renames and restarts the service if it is running. Config and data are left
  alone.
- **uninstall.sh**: `remove` → delete files → `daemon-reload`; `--purge` adds
  the helper's `purge`. Package installs are removed with the package manager
  (§3.1).

## 6. Local verification record (2026-09-05)

| Item | Result |
| --- | --- |
| `--version` / startup log / `lights3_build_info` | pass; a no-change incremental build does not relink |
| `cmake --install` in three scenarios (scratch prefix, `DESTDIR` + `/usr/local`, second install preserving the config) | pass (ctest `install_tree`) |
| `cpack -G DEB` | `lights3_0.1.0_amd64.deb` generated: conffile, postinst/prerm/postrm, shlibdeps dependencies all correct; `dpkg -i` needs root, not run |
| `cpack -G RPM` | no `rpmbuild` on this box, not verified |
| `run_e2e.sh duostore-redis`, external URI and spawned paths | 202/202 |
| Dockerfile / compose | `docker compose config` passes; daemon unreachable, not built |
