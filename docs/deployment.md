# 构建与分发：版本号、安装树、deb/rpm、容器、回滚

roadmap §6.3 的落地说明。覆盖五件事：二进制里的**版本与构建标识**、
`cmake --install` **安装树**、CPack 生成的 **deb / rpm**、**Dockerfile + compose**
（上手 demo，同时把 redis / tikv / rados 三条在开发机上长期 SKIP 的 e2e 路径跑起来），
以及 `install.sh` 升级的**回滚与卸载**。各分发渠道共用同一份安装后逻辑
（`packaging/lights3-setup.sh`），行为一致。

## 1. 版本与构建标识

`project(lights3 VERSION x.y.z)` 是唯一的版本号来源；git commit 在**构建时**由
`cmake/GenerateVersion.cmake`（脚本模式，`lights3_version_gen` 目标每次构建都跑）
写进 `build/generated/version_info.cc`，内容不变则不重写，因此不改 HEAD 的增量
构建不会重链。没有 `.git` 的树（源码 tarball、Docker 构建上下文）用
`-DLIGHTS3_GIT_COMMIT=<hash>` 传入，否则记作 `unknown`；工作区有未提交改动时
追加 `-dirty`。

三处可见：

```text
$ lights3 --version                 # s3adm --version 同样格式
lights3 0.1.0 (git d6f38292dab0, RelWithDebInfo, 2026-09-05)
drivers:  builtin beast httplib
features: memory localfs xlocalfs tiered cloudproxy duostore duostore-redis-meta duostore-sqlite-meta
```

- 启动日志首行：`lights3 0.1.0 (git …, RelWithDebInfo) started: driver=… backends=… pool=…`。
- 指标 `lights3_build_info{version,commit,build_type} 1`（Prometheus `*_build_info`
  惯例），集群面板可按 label 分组看每个实例在跑哪个构建。

`features` 列的是编译期开关（`LIGHTS3_*` CMake 选项），不是运行时配置了哪些后端；
`--check-config` 校验 `backends[].type` 是否在这份清单里。`--version` 优先于其他
根级选项。接口在 `src/core/version.h`。

## 2. 安装树（`cmake --install`）

```bash
cmake -B build -DLIGHTS3_BUILD_TESTS=OFF && cmake --build build -j
sudo cmake --install build                    # 默认 prefix /usr/local
cmake --install build --prefix ~/.local       # 非 root：全部落在 prefix 下
DESTDIR=/tmp/stage cmake --install build --prefix /usr   # 打包用 staging
```

| 路径 | 内容 |
| --- | --- |
| `<bindir>/lights3`、`<bindir>/s3adm` | 二进制 |
| `<sbindir>/lights3ctl` | `scripts/systemctl.sh`（start/stop/restart/status/logs …） |
| `<prefix>/lib/systemd/system/lights3.service` | 由 `scripts/lights3.service.in` 在**安装时**按 prefix 填好 `ExecStart` / `EnvironmentFile`，含 `ExecReload=kill -HUP`（热重载，[config-reload.md](config-reload.md)） |
| `<confdir>/lights3.yaml` | `config/lights3.yaml` 样例；**目标已存在则保留不覆盖** |
| `<datadir>/lights3/lights3-setup.sh` | 安装后逻辑（§3.3） |
| `<datadir>/lights3/deploy/{prometheus,grafana}` | 监控资产（[monitoring.md](monitoring.md)） |
| `<docdir>/README.md`、`<docdir>/docs/` | 文档（`archive/` 除外） |

配置目录 `<confdir>`：prefix 为 `/usr` 或 `/usr/local` 时是 `/etc/lights3`
（与 `install.sh` 和全部文档一致），其他 prefix 下是 `<prefix>/etc/lights3`；
`-DLIGHTS3_CONFIG_DIR=<绝对路径>` 覆盖。unit 文件和配置目录都在安装时按
`--prefix` 解析，`cmake --install --prefix X` 整体搬迁自洽。三方子模块
（rocksdb/spdlog/…）都以 `EXCLUDE_FROM_ALL` 引入，它们自己的 `install()` 规则
不会进入安装树。

ctest `install_tree`（`tests/packaging/check_install.sh`）每次都把这棵树装进临时
prefix 校验：布局、unit 搬迁、保留已有配置、`--version` 输出格式、维护脚本
`sh -n`——不需要 root 和打包工具。

## 3. deb / rpm（CPack）

```bash
cmake --build build --target package        # 或：cd build && cpack
cpack --config build/CPackConfig.cmake -G DEB   # 显式选生成器：DEB | RPM | TGZ
ls build/packages/                          # lights3_0.1.0_amd64.deb / lights3-0.1.0-1.x86_64.rpm
```

生成器随工具在场自动选：有 `dpkg-deb` 出 DEB，有 `rpmbuild` 出 RPM，两者都没有
退到 TGZ（安装树打包，绝对路径 `/etc` 会落在归档的 `usr/` 下——它是构建产物，
不是系统镜像）。包内 prefix 固定 `/usr`，二进制 strip。

### 3.1 deb

- 依赖由 `dpkg-shlibdeps` 自动算（`libc6 / libstdc++6 / libssl3t64 …`）+
  `adduser, openssl`；无 shlibdeps 时退到手写清单。
- `/etc/lights3/lights3.yaml` 登记为 **conffile**：升级时 dpkg 走标准的
  保留 / 三方合并提示，不会静默覆盖。
- 维护脚本（`packaging/deb/`）只做转发：`postinst configure` →
  `lights3-setup.sh configure`；`prerm remove` → `remove`；`postrm purge` →
  `purge`（删除 `/etc/lights3`、`/var/lib/lights3`、`/var/log/lights3` 和
  `lights3` 用户；普通 `remove` 全保留）。

```bash
sudo apt install ./build/packages/lights3_0.1.0_amd64.deb
sudo lights3ctl status
sudo apt remove lights3      # 保留配置/数据/密钥
sudo apt purge lights3       # 一并删除
```

### 3.2 rpm

`%post` / `%preun` / `%postun` 来自 `packaging/rpm/`，同样转发给 helper；
`lights3.yaml` 标 `%config(noreplace)`（升级时新样例落成 `.rpmnew`）；
`Requires: shadow-utils, openssl, systemd` + 自动 so 依赖。rpm 没有 purge，
密钥/数据/用户卸载后有意保留（`postun.sh` 注释给了手动清理命令）。本仓库的
开发机没有 `rpmbuild`，RPM 生成规则**未在本机验证**。

### 3.3 安装后逻辑：`packaging/lights3-setup.sh`

POSIX sh（dpkg/rpm 用 dash 跑维护脚本），三个子命令：

| 子命令 | 动作 |
| --- | --- |
| `configure [--no-enable] [--no-start]` | 建 `lights3` 系统用户/组；`/etc/lights3`(0750 root:lights3)、`/var/lib/lights3`、`/var/log/lights3`；**首次**生成 `/etc/lights3/lights3.env`（`LIGHTS3_SECRET_1` + `LIGHTS3_MASTER_KEY`，`openssl rand`，0640）；`daemon-reload` → enable → 用新二进制 `--check-config` 校验现有配置，**通过才** start / restart，否则打 warning 不动服务 |
| `remove` | stop + disable |
| `purge` | 删密钥、配置、数据、日志、用户/组 |

路径由环境变量 `LIGHTS3_BIN` / `LIGHTS3_CONFIG_DIR` / `LIGHTS3_STATE_DIR` /
`LIGHTS3_LOG_DIR` 覆盖，`install.sh` 就是这样把它用在 `/usr/local` 布局上的。
没有 systemd 的环境（容器）跳过全部 systemctl 调用。

## 4. Docker 与 compose

### 4.1 镜像

`Dockerfile` 三阶段：`builder`（ubuntu:24.04 + 工具链，`cmake --install` 到
`/stage`）→ `runtime`（只带两个二进制、setup helper、监控资产；非 root 用户
`lights3`；`HEALTHCHECK` 打 `/-/healthz`）→ `e2e`（builder + curl/python3，
可选 ceph CLI；§4.3 的测试跑手）。

```bash
git submodule update --init third_party/{spdlog,httplib,json,rocksdb,hiredis,sqlite}
git submodule update --init --recursive third_party/ccmd     # 或直接 ./build.sh 一次
docker build -t lights3 --build-arg LIGHTS3_GIT_COMMIT=$(git rev-parse --short=12 HEAD) .
docker run -d -p 9000:9000 -e LIGHTS3_SECRET_1=my-secret -v lights3-data:/var/lib/lights3 lights3
docker run --rm lights3 --version
docker run --rm lights3 s3adm --help
```

| build-arg | 默认 | 说明 |
| --- | --- | --- |
| `LIGHTS3_REDIS` / `LIGHTS3_SQLITE` | `ON` | duostore 的零外部依赖 meta 引擎 |
| `LIGHTS3_RADOS` | `OFF` | `librados-dev` / 运行时 `librados2` |
| `LIGHTS3_TIKV` | `OFF` | `libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev libpoco-dev libabsl-dev`（[duostore-tikv-meta.md §8](duostore-tikv-meta.md)） |
| `LIGHTS3_GIT_COMMIT` | `unknown` | 构建上下文不含 `.git`（`.dockerignore`），commit 由此传入 |
| `CMAKE_BUILD_TYPE` | `Release` | |
| `BASE` | `ubuntu:24.04` | Debian 基底也可（`libssl3` 名字已兜底） |

镜像配置 `deploy/docker/lights3.yaml`：builtin 驱动、`0.0.0.0:9000`、localfs
在 `/var/lib/lights3` 卷下；`LIGHTS3_SECRET_1` 必给（entrypoint 缺它直接退出
2，而不是起一个拒绝所有请求的实例）；`LIGHTS3_ACCESS_KEY` / `LIGHTS3_REGION` /
`LIGHTS3_LOG_LEVEL` / `LIGHTS3_LOG_FORMAT` 可选。entrypoint 规则：无参数或
以 `-` / `duostore` / `tier` / `fsck` / `help` 开头 → `lights3 --config=… "$@"`，
其他（`s3adm …`、`sh`）原样 exec。

seastar 驱动不进镜像（`.dockerignore` 排除其子模块）。**本仓库的开发机 docker
daemon 不可达，镜像构建与 compose 拉起未在本机验证**；`docker compose config`
通过，Dockerfile 用的 apt 包名按 Ubuntu 24.04 核对。

### 4.2 compose 上手 demo

```bash
docker compose up -d                          # :9000，AKIDEXAMPLE / lights3-demo-secret
LIGHTS3_SECRET_1=... docker compose up -d      # 自定义密钥
aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
docker compose --profile redis up -d          # 再加 :9001 = duostore + redis meta
docker compose --profile tikv up -d           # :9002 = duostore + TiKV meta（pd0 + tikv0，镜像 lights3:full）
docker compose --profile rados up -d          # :9003 = duostore + RADOS data（ceph/demo 单节点，镜像 lights3:full）
```

| profile | 服务 | 说明 |
| --- | --- | --- |
| （默认） | `lights3` | `lights3:local` 镜像，localfs |
| `redis` | `redis`（`redis:7-alpine`，AOF 开）、`lights3-redis` | 配置 `deploy/docker/lights3-redis.yaml` 只读挂进 `/etc/lights3/lights3.yaml` |
| `tikv` | `pd0`、`tikv0`（`pingcap/{pd,tikv}:${TIKV_VERSION:-v8.5.2}`）、`lights3-tikv` | 单 PD 单 TiKV；`lights3:full` 由 `LIGHTS3_RADOS=ON LIGHTS3_TIKV=ON` 构建 |
| `rados` | `ceph`（`${CEPH_IMAGE:-quay.io/ceph/demo:latest}`，固定 IP 172.28.0.10）、`rados-init`（建池一次性任务）、`lights3-rados` | `ceph.conf` + admin keyring 经 `ceph-etc` 卷只读共享；keyring 仅 root 可读，消费者以 root 运行 |

`LIGHTS3_GIT_COMMIT=$(git rev-parse --short=12 HEAD) docker compose build` 给
镜像打 commit。数据卷：`lights3-data` 等命名卷，`docker compose down -v` 清空。

### 4.3 让 SKIP 的 e2e 路径跑起来

`run_e2e.sh` 的 `duostore-redis` / `duostore-tikv` / `duostore-rados` 分支各自探测
外部依赖，缺则显式 SKIP（[testing.md §1](testing.md)）。compose 的 `e2e` profile
把三套依赖拉起来，再在 `lights3:e2e` 镜像里跑 ctest：

```bash
docker compose --profile e2e run --rm e2e                                  # 三条都跑
docker compose --profile e2e run --rm -e E2E_TESTS=e2e_duostore_redis e2e  # 只跑一条
docker compose --profile e2e down -v
```

`deploy/docker/e2e.sh` 先等服务就绪（redis TCP；PD 的 `/pd/api/v1/stores` 出现
`Up` 的 store；`ceph -s` 通过并建好 `lights3-e2e` 池），再
`ctest -R "$E2E_TESTS"`。环境变量就是 `run_e2e.sh` 原有的探测口：

| 变量 | 值 | 效果 |
| --- | --- | --- |
| `LIGHTS3_TEST_REDIS_URI` | `redis://redis:6379` | **新增**：指向外部实例，不再自起 `redis-server`；每次运行用唯一 `redis_prefix`（`e2e-<pid>-<rand>:`）隔离，TLS 二实例再加 `tls-` 后缀 |
| `LIGHTS3_TEST_PD_ADDR` | `pd0:2379` | 既有；每次运行唯一 `tikv_prefix` |
| `LIGHTS3_TEST_RADOS_CONF` / `_POOL` | `/etc/ceph/ceph.conf` / `lights3-e2e` | 既有；每次运行唯一 `rados_namespace` |

`LIGHTS3_TEST_REDIS_URI` 在开发机上同样可用（`redis-server --port 16399 &` 后
`LIGHTS3_TEST_REDIS_URI=redis://127.0.0.1:16399 ctest -R e2e_duostore_redis`），
外部与自起两条路径都已本机验证 202/202。

## 5. 升级、回滚、卸载（`install.sh` 渠道）

```bash
sudo ./scripts/install.sh            # 升级：保留配置/密钥，旧二进制留作 *.prev
sudo ./scripts/rollback.sh           # 换回 *.prev 并重启；再跑一次又换回来
sudo ./scripts/uninstall.sh          # 删二进制/unit/lights3ctl，保留 /etc/lights3 与数据
sudo ./scripts/uninstall.sh --purge  # 连配置、密钥、数据、日志、用户一起删
```

- **install.sh** 在动任何文件之前先用**新**二进制 `--check-config` 校验现有
  `/etc/lights3/lights3.yaml`（sourcing `lights3.env` 供 `${LIGHTS3_SECRET_1}`
  展开），被拒即中止——升级不会把服务重启进一个起不来的配置。替换二进制时
  原子 rename，被替换的文件改名为 `<name>.prev`（只留一代；内容相同不留）。
  unit 文件由 `scripts/lights3.service.in` 用 sed 填 `/usr/local/bin` 与
  `/etc/lights3`。用户/目录/密钥/启停全部交给 `lights3-setup.sh`（§3.3）。
- **rollback.sh**：先用 `lights3.prev` 跑 `--check-config`（旧二进制不认识新
  配置键时会被拒，此时先手动回退配置），再三步 rename 交换 `lights3` /
  `s3adm` 与其 `.prev`，服务在运行则 restart。配置与数据不动。
- **uninstall.sh**：`remove` → 删文件 → `daemon-reload`；`--purge` 追加 helper 的
  `purge`。包安装的实例用包管理器卸载（§3.1）。

## 6. 本机验证记录（2026-09-05）

| 项 | 结果 |
| --- | --- |
| `--version` / 启动日志 / `lights3_build_info` | 通过；无改动增量构建不重链 |
| `cmake --install` 三场景（scratch prefix、`DESTDIR` + `/usr/local`、二次安装保留配置） | 通过（ctest `install_tree`） |
| `cpack -G DEB` | 生成 `lights3_0.1.0_amd64.deb`：conffile、postinst/prerm/postrm、shlibdeps 依赖均正确；`dpkg -i` 需 root 未做 |
| `cpack -G RPM` | 本机无 `rpmbuild`，未验证 |
| `run_e2e.sh duostore-redis` 外部 URI / 自起两路径 | 202/202 |
| Dockerfile / compose | `docker compose config` 通过；daemon 不可达，未构建 |
