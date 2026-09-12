# 命令行工具：`lights3` 与 `lights3-ctl`

本文是两个可执行文件的命令参考。二者都基于 `third_party/ccmd`
（header-only 子命令框架，内嵌 `cflag` 做选项解析），共享同一套命令行语义，
先在 §1 说清，后文不再重复。启动装配流程见 [architecture.md §4](architecture.md#4-进程结构与启动流程)，
凭证管理面见 [credential-management.md](credential-management.md)，
静态网站见 [static-website.md](static-website.md)。

## 1. ccmd 通用语义

- **命令树**：`<程序> [<命令组> [<子命令>]] [位置参数] [选项]`。
  `<程序> help [<命令组> [<子命令>]]` 或任一层级的 `-h/--help` 打印该层帮助。
- **选项不向下继承**：每个叶子子命令拥有独立的选项集，选项必须写在叶子
  子命令之后（`lights3-ctl cred list --endpoint=…`，而不是 `lights3-ctl --endpoint=… cred list`）。
- **长选项取值只接受 `--name=value`**；`--name value` 会被 cflag 当作缺值报错。
  短选项两种都可以：`-e http://…` 或 `-ehttp://…`。bool 选项裸写即为 true
  （`--insecure`、`--keep`）。
  例外：`lights3` 主程序在进入 ccmd 前把 `--config <path>`（以及
  `--backend`/`--file`）折叠成 `=` 形式，因此空格写法对 `lights3` 也可用
  （e2e 脚本与旧文档沿用这一写法）；`lights3-ctl` 没有这层兼容。
- **`--` 终止选项解析**，其后全部视为位置参数。
- **退出码**：`0` 成功；`1` 运行期失败（请求被拒、IO 错误、服务启动异常）；
  `2` 用法错误（缺位置参数、缺凭证、数值越界、裸命令组）。ccmd 自身对
  未知命令/未知选项以 `1` 退出并在 stderr 给出提示。

## 2. `lights3` —— 服务进程

```text
lights3 [--config=<path>]                                  启动服务
lights3 --version                                          版本 / git commit / 编译进来的驱动与后端
lights3 --check-config [--config=<path>]                   只校验配置（§2.1）
lights3 duostore dump <backend> <file> [--config=<path>]   导出 duostore meta
lights3 duostore load <backend> <file> [--config=<path>]   导入 duostore meta
lights3 duostore backup <backend> --to=<dir> [--incremental] [--config=<path>]  追加一条 meta 备份链条目
lights3 duostore restore <backend> --from=<dir> [--to-id=<n>|--to-ts=<t>] [--config=<path>]  按链恢复 meta 到某点
lights3 duostore gc <backend> [--config=<path>]            立即跑一轮 duostore GC
lights3 duostore scan <backend> [--config=<path>]          立即跑一轮孤儿扫描
lights3 duostore quarantine list|release|purge <backend> [<pack_id>] 损坏 pack 隔离区
lights3 tier scan|gc|reconcile <backend> [--config=<path>] tiered 后台任务手动触发
lights3 tier quarantine list|forget|purge <backend> [<bucket> <key>] tiered 对账隔离区
lights3 fsck <backend> [--max-mbps=<n>] [--config=<path>]  离线数据完整性巡检
lights3 help [duostore [<sub>] | tier [<sub>] | fsck]
```

| 选项 | 适用 | 默认 | 说明 |
| --- | --- | --- | --- |
| `-c, --config=<path>` | 全部 | `config/lights3.yaml` | YAML 配置文件（格式见 [architecture.md §5](architecture.md#5-配置文件示例)） |
| `--version` | 根命令（`lights3-ctl` 同） | — | 打印 `lights3 <ver> (git <commit>, <build type>, <date>)` + `drivers:` / `features:` 两行后退出 0；优先于 `--check-config`（roadmap §6.3，[deployment.md §1](deployment.md)） |
| `--backend=<name>` | `duostore *`、`tier *`、`fsck` | — | 后端名，等价于第一个位置参数 |
| `--file=<path>` | `duostore dump|load` | — | dump 文件路径，等价于第二个位置参数 |
| `--to=<dir>` / `--from=<dir>` | `duostore backup` / `restore` | — | 备份链目录（必填） |
| `--incremental` | `duostore backup` | `false` | 追加自上一条以来的增量而非全量副本 |
| `--to-id=<n>` / `--to-ts=<t>` | `duostore restore` | 最新 | 恢复到 manifest 条目 n / 到时间 t（ISO 8601 或 unix ms）之前的最后一条；二者互斥 |
| `--max-mbps=<n>` | `fsck` | `0` | 读限速（MB/s），`0` 不限速 |

### 2.1 启动服务

无子命令即为启动：`Application(config)` → `open_storage()` → `start_server()`
→ `run()`，阻塞到 SIGINT/SIGTERM，按 [architecture.md §4](architecture.md#4-进程结构与启动流程)
的顺序优雅关闭，`run()` 的返回值即退出码：`0` 干净退出；`3` 关停不干净
（roadmap §4.5）——在途请求在 `http.shutdown_grace` 内没有排空，或某个后端
`close()` / 线程池 join 失败（各自 LOG_ERROR，此前进程仍以 0 退出，进程管理器
无从察觉）。排空死线就是 `http.shutdown_grace`，同一个量同时约束驱动的连接排空
与许可归还，不再有独立硬编码的 10s。启动期任何异常（配置解析失败、
后端打开失败、端口占用等）在 stderr 打 `fatal: …` 并以 `1` 退出；已构建
的后端经 `~Application` 关闭（duostore 封存 active pack、rados flush）。

```bash
export LIGHTS3_SECRET_1=my-secret
./build/lights3 --config=config/lights3.yaml
./build/lights3 -c /etc/lights3/lights3.yaml
```

**`--check-config`**（roadmap §6.2）：只做配置解析与校验的 dry-run——不打开后端、
不绑端口。走与启动完全相同的 `Config::load` 校验，再核对 `http.driver` 与每个
`backends[].type` 是否编进了本二进制；`type: duostore` 的后端还按构造函数的
同一套 `from_params` 解析参数（引擎选择、取值范围、未编入的引擎在此即报错），
共享 meta（redis / tikv）配本地 fs data 的单网关组合以 `config warning:` 打到
stderr、不改退出码（[archive/multi-gateway-multipart-design.md §4 ④](archive/multi-gateway-multipart-design.md)）；
然后打印配置解析出的摘要（驱动/监听/TLS、线程数、凭证数、后端列表——duostore
带 `meta=… data=…`、路由规则数、网站条目数、日志与审计设置）。退出码
`0` = 此文件能启动（运行期失败如数据目录不可写除外），`1` = 被拒，错误信息与
启动时的 `fatal:` 同源。部署脚本在 reload/重启前先跑它：

```bash
./build/lights3 --check-config --config=/etc/lights3/lights3.yaml && systemctl reload lights3
```

### 2.2 `duostore dump` / `duostore load`

DuoStore 的逻辑 meta 备份与恢复（流格式与不变量见
[storage/duostore-core.md §11](storage/duostore-core.md#11-meta-dumpload)）。
仅在 `LIGHTS3_DUOSTORE` 构建中注册；两者都在**不监听端口**的前提下构建全部
后端，执行完即退出。`<backend>` 必须是配置中 `type: duostore` 的后端名，
否则报错退出。共享 meta 引擎旁路在线网关执行时：`dump` 在 rocksdb/sqlite/
tikv 上走引擎快照、在线一致（roadmap §3.7）；redis 无 MVCC，其他网关持续
写入时 dump 不保证一致（入口 WARN 提示，须停写）。`load` 恒要求目标端写
静默。

- `dump`：写出该后端全部 bucket/object 记录与已封存 pack 账本到 `<file>`
  （覆盖写）。
- `load`：逐条重放 `<file>` 到该后端（bucket 幂等，可中断重跑），末尾强制
  一次 orphan scan。

备份顺序：先拷贝数据目录再 `dump`；恢复时先放回数据再 `load`。

```bash
./build/lights3 duostore dump duo /backup/duo-meta.dump --config=/etc/lights3/lights3.yaml
./build/lights3 duostore load --backend=duo --file=/backup/duo-meta.dump -c /etc/lights3/lights3.yaml
```

### 2.2.1 `duostore backup` / `duostore restore`

备份链与恢复到中间点（PITR；目录布局、manifest、各引擎载荷见
[storage/duostore-core.md §11.1](storage/duostore-core.md#111-备份链与-pitrmeta_backuph--meta_backupccbacklog-sequence-)）。
`backup` 向 `--to=<dir>` 追加一条：默认全量，`--incremental` 为自上一条以来的
增量（目录里还没有全量时拒绝）。引擎差异：sqlite 增量 = WAL 段，需要后端配置
`sqlite_wal_archive` 指向同一目录；rocksdb 走 BackupEngine，每条都能独立恢复；
redis / tikv 没有网关侧增量——`backup` 落成逻辑 dump 并记下恢复点标记（复制
offset / TSO），`--incremental` 拒绝。本地引擎（sqlite / rocksdb）持文件锁，服务
须已停。

`restore` 按 `--to-id` / `--to-ts`（默认最新）在链上取前缀：sqlite / rocksdb 先在
**不构建后端**的前提下做文件级恢复，再构建后端跑一次强制孤儿扫描；redis / tikv
要求集群已回到该条目的标记，然后 load 该 dump。恢复顺序同 §2.2：先放回数据
目录再 `restore`。

```bash
./build/lights3 duostore backup duo --to=/backup/duo-meta -c /etc/lights3/lights3.yaml
./build/lights3 duostore backup duo --to=/backup/duo-meta --incremental -c /etc/lights3/lights3.yaml
./build/lights3 duostore restore duo --from=/backup/duo-meta --to-ts=2026-09-06T12:00:00Z -c /etc/lights3/lights3.yaml
```

### 2.3 `fsck`

> 在线网关上同一套 scrub 也能经 admin 面触发与轮询：`POST/GET /-/admin/fsck/<backend>`
> 与 `lights3-ctl fsck --offline <backend>`（§3.5）。离线 CLI 与 admin 端点共用
> `app/admin_jobs.h` 的类型分派与结论定义。

离线数据完整性巡检（roadmap §3.1，实现细节见
[storage/duostore-core.md §8.4](storage/duostore-core.md) 与
[storage/localfs.md §11](storage/localfs.md)）。与
dump/load 同模式：构建全部后端、不监听端口，跑完即退出；**纯只读**，任何
发现只记日志与计数，绝不修复。按 `<backend>` 的实际类型分派：

- **duostore**：以 meta 为驱动读回每个对象与进行中 multipart 分片的全部
  extent，逐段重算 crc32c 与 manifest 对照（与 `verify_chunk_crc` 开关无关），
  并把 chunk/rados refs 台账与 manifest 双向对账；
- **localfs / xlocalfs**：重读每个对象内容、重算 MD5 与存储的 ETag 对照
  （multipart 复合 ETag 按记录的 part 布局重算；无布局的存量对象计为
  unverifiable）；
- 其余类型（memory/cloudproxy/tiered）报错退出；
- **S3 Tables 目录对账**（`tables.enabled` 且 `<backend>` 是默认后端时追加，
  [s3-tables/step-3-validation-diagnostics.md §9](s3-tables/step-3-validation-diagnostics.md)）：
  `.sys/tables/` 的表桶标记与 `.sys/tables-catalog/<bucket>/` 的目录状态对照表桶本身：
  `tables.orphan_state`（标记或目录状态对应的桶不存在 / 未启用）、`tables.dangling_pointer`
  （表指针指向不存在的 metadata 对象）、`tables.stale_renaming`（表处于 RENAMING 但 intent
  已不在）、`tables.inconsistent_rename`（intent 的阶段与源/目标条目不符）、
  `tables.malformed_entry`。明细在结论的 `stats.tables`，每条计一个 finding；修复走
  `POST …/tables/{t}/catalog/recovery`，fsck 本身不改任何对象。在线 `POST /-/admin/fsck/<默认后端>`
  同样附带。

退出码：`0` 干净；`1` 存在完整性发现（duostore 的 corrupt/unreadable/
refs_missing/meta_errors，localfs 的 mismatches/read_errors，tables 的全部 finding）。警告级计数
（refs_stale、unverifiable、孤儿 sidecar）只记日志不影响退出码——
refs_stale 可能是巡检期间 MPU complete 造成的暂态，复跑确认。对运行中的
实例也可安全执行（duostore 侧代价是巡检期间 GC 停摆）。

```bash
./build/lights3 fsck duodata --max-mbps=100 --config=/etc/lights3/lights3.yaml
./build/lights3 fsck localdata -c /etc/lights3/lights3.yaml && echo clean
```

### 2.4 后台任务手动触发：`duostore gc|scan|quarantine`、`tier scan|gc|reconcile|quarantine`

后台钩子的 CLI 出口（roadmap §3.2）：`run_gc_once` / `run_orphan_scan_once` /
`scan_once` / `run_gc_once`(tiered) / `run_reconcile_once` 此前只被定时器与单
测调用，想立即回收空间只能等下一个 tick（GC 默认 5min、孤儿扫描与对账默认
1 天）。与 dump/load 同模式：构建后端、不监听端口、跑一轮即退出，统计打进
日志。**退出码恒 0/1（成功/异常）**——refs_missing 等丢失信号照常 LOG_ERROR
但不改变退出码，完整性裁决面归 `lights3 fsck`。

- `duostore gc <backend>`：一轮完整 GC（mpu_ttl 清理 → gcq 消费 → 按龄封存 +
  压实 → 整空 pack 删除，[storage/duostore-core.md §8.1](storage/duostore-core.md)）。
  本地 meta 引擎（rocksdb/sqlite）持文件锁，须停服执行；共享引擎
  （redis/tikv）可与在线网关并行——GC 租约自会协调。`gc_enabled=false` 的
  从网关配置不影响手动钩子。
- `duostore scan <backend>`：一轮孤儿扫描（盘面与 refs/packstat 双向对账，
  §8.3），顺带打出 chunk/pack 盘面用量。
- `duostore quarantine list <backend>`：打印损坏 pack 隔离区账本（pack id /
  live/corrupt 记录数 / 入区时间 / 是否已 purge，
  [storage/duostore-core.md §8.6](storage/duostore-core.md)）；
  `duostore quarantine release <backend> <pack_id>` 删条目让压实重试（修好
  盘面/从备份放回文件后用）；`duostore quarantine purge <backend> <pack_id>`
  物理删 pack 文件、保留记账（承认剩余损坏记录丢失；有在途读 pin 时拒绝），
  之后删除引用它的对象即可由常规 GC 收尾。pack id 接受日志里的 16 位十六进
  制、`0x` 前缀十六进制或十进制。
- `tier scan <backend>`：一轮扫描（启动后首轮/每 `full_scan_interval` 为全量枚举，
  其余为时间轮增量轮）：判冷 + 水位回收 + 崩溃恢复 + 访问记录刷写，日志打出
  本轮 `TierScanStats`；
- `tier gc <backend>`：消费一轮 tiered GC 队列（孤儿云副本删除，指数退避
  账随条目持久化）；
- `tier reconcile <backend>`：一轮本地/云双向对账（云有本地无 → 重建
  stub；本地 remote 云缺 → 告警绝不删 stub），重复出现的发现进隔离区账本
  只报一次；
- `tier quarantine list <backend>`：打印隔离区账本（kind / bucket / key / etag /
  首末次发现 / 次数）；`tier quarantine forget <backend> <bucket> <key>` 只删条目
  （下轮仍复现会再记）；`tier quarantine purge <backend> <bucket> <key>` 针对
  `refs_missing`：HEAD 复核云副本仍不存在后删除这个已死的本地 stub（承认数据
  丢失，对象从列表消失；副本回来了则保留 stub、销账并返回退出码 1）。
  见 [tiered-design.md §9](storage/tiered-design.md)。

同一批轮次也能在**运行中的网关**里立即触发（`lights3-ctl duostore|tier`，§3.12），
不必停服；离线入口保留给未启动网关、以及账本的 `release` / `purge` / `forget`。

```bash
./build/lights3 duostore gc duodata --config=/etc/lights3/lights3.yaml   # 立即回收空间
./build/lights3 tier reconcile tierdata -c /etc/lights3/lights3.yaml
./build/lights3 tier quarantine list tierdata -c /etc/lights3/lights3.yaml
./build/lights3 tier quarantine purge tierdata archive photos/2024/a.jpg -c /etc/lights3/lights3.yaml
```

### 2.6 `tables export` / `tables import`

S3 Tables 目录状态在两种后备之间迁移（[s3-tables-design.md §12](s3-tables-design.md)，
`tables.catalog_backing: object | duostore`）。离线：后端构建、不监听；先停掉全部网关。

```bash
lights3 tables export catalog.jsonl --config=/etc/lights3/lights3.yaml                 # 读配置里的后备
lights3 tables import catalog.jsonl --backing=duostore --config=/etc/lights3/lights3.yaml
```

每行 `{"bucket","key","body"}`：`key` 是两种后备共用的目录键（`tables-catalog/<bucket>/…`），
`body` 是对象 JSON；import 覆盖同键。表桶标记（`.sys/tables/`）不在其中——两种后备都读
`.sys`。`--backing` 覆盖配置值，迁移即"旧配置 export → 改配置 → import"。

### 2.5 配置热重载：`SIGHUP`

`kill -HUP <pid>` 让服务进程重新读取 `--config` 指定的文件（roadmap §4.4，
[config-reload.md](config-reload.md)）：整体校验后只应用可热更新子集（日志级别、
`request_timeout`/`transfer_stall_timeout`、`max_inflight_requests`、`min_part_size`、
限流、bucket 路由规则、后端实例的增删、TLS 证书内容），其余改动逐项 WARN "需重启"；文件校验失败
则一字不改。systemd 单元可配 `ExecReload=/bin/kill -HUP $MAINPID`。同一动作也可经
`lights3-ctl reload`（§3.9）触发并拿到报告。

## 3. `lights3-ctl` —— 运维 CLI

`src/tools/lights3-ctl*.cc`，构建产物与 `lights3` 同目录。命令组：`cred`（凭证
管理面）、`website`（桶静态网站配置）、`bench`（压测）、`fsck`（在线对象
校验）、`quota`（桶配额）、`tenant`（租户与桶归属）、`usage`（用量计数器，
roadmap §3.9，见 [multi-tenancy.md](multi-tenancy.md)）、`reload`（配置热重载，
[config-reload.md](config-reload.md)）、`tables`（S3 Tables 目录：表桶、列表、维护、
诊断，§3.13）。全部子命令以 SigV4
自签名直连 lights3 的 HTTP 端点，无需 aws cli。

### 3.1 连接与凭证选项（所有叶子子命令共有）

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `-e, --endpoint=<url>` | `http://127.0.0.1:9000` | `scheme://host[:port]`；https 需要带 OpenSSL 的构建。服务端配置了 `http.admin_port` 时，`cred` / `website` / `quota` / `tenant` / `usage` / `reload` / `object` / `mpu` 这些走 `/-/admin/*` 的命令组必须指向 **admin 端口**（数据面端口对它们答 404），`bench` / `fsck` 走数据面端口（[http-adapter.md §2.1](http-adapter.md)） |
| `--ak=<key>` / `--sk=<key>` | 环境变量 | 缺省回退 `LIGHTS3_ADMIN_AK` / `LIGHTS3_ADMIN_SK`；SK 建议走环境变量（argv 对本机 `ps` 可见） |
| `--region=<r>` | `us-east-1` | SigV4 region，须与服务端 `auth.region` 一致 |
| `--insecure` | false | https 跳过证书校验（自签名部署） |
| `--timeout-sec=<n>` | 10 | 连接/读/写超时 |
| `--cert=<pem>` / `--key=<pem>` | 无 | 客户端证书与私钥，服务端 `tls_client_auth: optional\|require` 时使用（[tls.md §2.1](tls.md)）；须成对给出 |

`website`、`quota set/clear`、`tenant` 的变更操作要求 **root 静态凭证**
（配置文件 `auth.credentials` 中的条目，见
[credential-management.md §3](credential-management.md)）；`cred`、
`tenant list/get`、`usage` 也接受**租户 admin**（作用域限于本租户，
[multi-tenancy.md §4.4](multi-tenancy.md)）；其它凭证调用会得到 403。
`bench` 用任意有权访问目标桶的凭证即可。

```bash
export LIGHTS3_ADMIN_AK=AKIDEXAMPLE
export LIGHTS3_ADMIN_SK=my-secret
```

### 3.2 `cred` —— 凭证管理

与 `/-/admin/credentials` 的四个接口一一对应，外加 `/-/admin/tls-identities` 的
证书绑定三命令；响应 JSON 原样输出到 stdout。

```text
lights3-ctl cred list                          列出全部凭证（SK 掩码；含静态/文件/动态三来源）
lights3-ctl cred get <ak> [-s|--show-secret]   查询单个凭证；--show-secret 返回明文 SK（仅动态/文件凭证，服务端记审计日志）
lights3-ctl cred create [-c|--comment=<text>] [-p|--policy=<json>|@<file>] [-t|--tenant=<id>] [-r|--role=user|admin]
                                         生成一对 AK/SK（唯一一次返回完整 SK）；--tenant 归属租户（租户 admin 调用时
                                         服务端固定为本租户，可省略），--role=admin 授予本租户管理面
lights3-ctl cred delete <ak>                   吊销动态凭证（静态凭证归配置文件管，服务端拒绝）
lights3-ctl cred bind-cert <ak> -S|--subject=<subject> [-c|--comment=<text>]
                                         把客户端证书主体（CN，或 auth.tls_identity: san-uri 下的 URI SAN）绑到凭证：
                                         该证书上的未签名请求视同此凭证签名，已签名请求须同租户（tls.md §2.1）；
                                         root 专属，重复绑定即覆盖（201 新建 / 200 覆盖）
lights3-ctl cred unbind-cert -S|--subject=<subject>
                                         解除绑定（幂等）；root 专属
lights3-ctl cred list-certs                    列出全部绑定与服务端的 auth.tls_identity 模式；root 专属
```

`--policy` 取内联 JSON 或 `@file`，结构
`{"buckets":[...],"prefixes":[...],"readonly":bool,"actions":[...]}`，语义见
[credential-management.md §11](credential-management.md)。

```bash
lights3-ctl cred create --comment=tenant-a --policy='{"buckets":["tenant-a-*"],"readonly":false}'
lights3-ctl cred create -c ci-runner -p @policies/ci.json
lights3-ctl cred get L3AK7Q2MXX5EIY4BJZW3 --show-secret
lights3-ctl cred list --endpoint=https://s3.example.com --insecure
lights3-ctl cred delete L3AK7Q2MXX5EIY4BJZW3
lights3-ctl cred create --tenant=acme --role=admin --comment='acme operator'
lights3-ctl cred bind-cert L3AK7Q2MXX5EIY4BJZW3 --subject=alice --endpoint=https://s3.example.com --cert=ops.crt --key=ops.key
lights3-ctl cred bind-cert L3AK7Q2MXX5EIY4BJZW3 --subject=spiffe://example.org/ns/prod/sa/api   # san-uri 模式
lights3-ctl cred unbind-cert --subject=alice
```

### 3.3 `website` —— 桶静态网站配置

操作 `?website` 子资源（[static-website.md §4](static-website.md)）。`set`
后桶变为匿名可读（仅 GET/HEAD 对象），带 index/error 文档语义；YAML 中
静态配置的桶服务端拒绝动态修改（405）。

```text
lights3-ctl website get <bucket>                            打印配置 XML（未配置为 404 → 退出码 1）
lights3-ctl website set <bucket> [-i|--index-suffix=<s>] [-k|--error-key=<key>]
                                                      启用/替换配置；index-suffix 默认 index.html，不得含 '/'；
                                                      error-key 为空用内置错误页
lights3-ctl website delete <bucket>                         删除配置（幂等），桶不再匿名可读
```

```bash
lights3-ctl website set my-site --index-suffix=index.html --error-key=404.html
lights3-ctl website get my-site
lights3-ctl website delete my-site
```

### 3.4 `bench` —— 压测

对数据面（put/get）与非 IO 接口（stat/list/list-buckets）做闭环压测：
`--concurrency` 个 worker 各持一条连接，在 `--prefix` 下 `--objects` 个键组成
的池上循环请求 `--duration-sec` 秒；每秒打印一行区间统计，结束打印汇总
（ops、ops/s、MiB/s、avg/p50/p90/p99/max 延迟）。

```text
lights3-ctl bench put           上传（池内键轮转覆盖写）
lights3-ctl bench get           下载（先上传整池）
lights3-ctl bench stat          HeadObject（先上传整池）
lights3-ctl bench list          ListObjectsV2（先上传整池，--max-keys 控制每页）
lights3-ctl bench list-buckets  ListBuckets（不需要 --bucket）
```

| 选项 | 默认 | 范围/说明 |
| --- | --- | --- |
| `-b, --bucket=<name>` | — | 目标桶，不存在则创建；`list-buckets` 外必填 |
| `-j, --concurrency=<n>` | 4 | 1–256 |
| `-d, --duration-sec=<n>` | 10 | 1–86400 |
| `-n, --objects=<n>` | 64 | 1–1000000，键池大小 |
| `-s, --size=<sz>` | put/get `1M`，stat/list `4K` | 字节或 K/M/G 后缀，上限 1G |
| `--prefix=<p>` | `lights3-ctl-bench/` | 键前缀 |
| `--max-keys=<n>` | 100 | 仅 `list` |
| `--keep` | false | 结束后保留对象（默认删除整池） |
| `-o, --output=text\|json` | `text` | `json`：stdout 只输出一个 JSON 对象（mode、wall_s、workers、keys、size、ops、errors、ops_per_s、mib_per_s、latency_ms{avg,p50,p90,p99,max}），每秒表格与准备/清理提示改到 stderr——`scripts/bench_gate.sh` 的基线比对输入（roadmap §6.2） |

首个错误打印到 stderr（`lights3-ctl: bench: first error: …`），其余只计入 err
计数；准备阶段（建桶/预上传）失败直接以 `1` 退出。

```bash
lights3-ctl bench put --bucket=test --size=4M --concurrency=8 --duration-sec=30
lights3-ctl bench get -b test -s 4M -j 8 -d 30 --keep
lights3-ctl bench stat -b test -j 16
lights3-ctl bench list -b test -n 10000 --max-keys=1000
lights3-ctl bench list-buckets -j 16
```

### 3.5 `fsck` —— 在线对象校验 / 服务端 scrub

**`--offline <backend>`**（backlog-sequence ③）：不走 S3 API，而是让**运行中的网关**
对指定后端跑一轮离线 scrub（duostore 的 manifest/crc/refs 对账，localfs/xlocalfs 的
ETag 全量 verify——与 `lights3 fsck` 同一实现），经 admin 面：

```text
POST /-/admin/fsck/<backend>[?max_mbps=N]   root；202 {"backend","job_id","running":true}
                                            409 ScrubInProgress = 该后端已有一轮在跑
                                            404 未知后端；400 该后端类型无离线 scrub（memory/cloudproxy/tiered）
GET  /-/admin/fsck/<backend>                200 {"running","job_id","started_at_ms","max_mbps",
                                                 完成后再加 "finished_at_ms","duration_ms","kind","findings","aborted","stats"{…}}
```

`lights3-ctl fsck --offline <backend> [--max-mbps=N] [--no-wait]` 发起后每 0.5s 轮询到
结束并打印结论文档，`findings > 0`（duostore：corrupt + unreadable + refs_missing +
meta_errors；localfs：etag_mismatches + read_errors）或 `aborted` 时退出码 1；
`--no-wait` 只打印 job id 立即返回；`lights3-ctl fsck --status <backend>` 只查询。
同一后端同一时刻一个 job；网关关停会中断在跑的 scrub（`aborted: true`）。
限速与离线 CLI 同一 `scrub_throttle.h`。审计事件 `fsck.start`。

```bash
lights3-ctl fsck --offline duodata --max-mbps=200     # 等到结束，打印 JSON 结论
lights3-ctl fsck --offline localdata --no-wait        # 只拿 job id
lights3-ctl fsck --status localdata                   # 进度 / 上次结论
```

原有在线模式（下）语义不变：

`lights3 fsck`（§2.3）的在线补集：走 S3 API 端到端校验——ListObjectsV2 逐页
列举，逐对象流式 GET 并在客户端重算 MD5 与 ETag 对照，顺带覆盖了网关读
路径本身。multipart 复合 ETag 经 `GET ?partNumber=i` 逐分片下载重算
（服务端无布局的存量对象回 501，计为 UNVERIFIABLE 而非 MISMATCH）。
纯只读；任何能读目标桶的凭证即可。代价是全部字节走一遍 HTTP——深检
（duostore crc/refs 对账）仍需服务器侧的 `lights3 fsck`。

```text
lights3-ctl fsck <bucket> [-p|--prefix=<p>] [--max-mbps=<n>]
```

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `-p, --prefix=<p>` | — | 只校验该前缀下的 key |
| `--max-mbps=<n>` | `0` | 下载限速（MB/s），`0` 不限速 |

逐条打印 `MISMATCH <key>`（stdout）/ 传输错误（stderr），结尾一行汇总
（objects/bytes/mismatches/errors/unverifiable/skipped，skipped = 列举与
GET 之间被删除的对象）。退出码：`0` 干净；`1` 有 mismatch 或错误。

```bash
lights3-ctl fsck my-bucket --endpoint=https://s3.example.com
lights3-ctl fsck my-bucket --prefix=photos/ --max-mbps=50
```

### 3.6 `quota` —— 桶配额

操作 `?quota` 子资源（[multi-tenancy.md §3](multi-tenancy.md)）。`set` 整体
替换限额，至少一轴 > 0；`get` 任何能访问该桶的凭证可用，`set`/`clear` root
专属。超限的写请求得 `QuotaExceeded`(403)。

```text
lights3-ctl quota get <bucket>                                    打印配额 XML（未配置 404 → 退出码 1）
lights3-ctl quota set <bucket> [-b|--max-bytes=<sz>] [-o|--max-objects=<n>]
                                                            设置/替换配额；sz 接受 KiB/MiB/GiB 后缀，0 = 该轴不限
lights3-ctl quota clear <bucket>                                  删除配额（幂等）
```

```bash
lights3-ctl quota set logs --max-bytes=50GiB --max-objects=1000000
lights3-ctl quota get logs
lights3-ctl quota clear logs
```

### 3.7 `tenant` —— 租户与桶归属

操作 `/-/admin/tenants`（[multi-tenancy.md §6](multi-tenancy.md)）。变更
root 专属；`list`/`get` 租户 admin 可查本租户。响应 JSON 原样输出。

```text
lights3-ctl tenant list                                            列出租户（含配额、所有桶、聚合用量、凭证数）
lights3-ctl tenant get <id>                                        单个租户
lights3-ctl tenant create <id> [--display-name=<s>] [--max-bytes=<sz>] [--max-objects=<n>] [--max-buckets=<n>]
                                                             创建；id 形如 [a-z0-9][a-z0-9._-]{0,63}
lights3-ctl tenant update <id> [--display-name=<s>] [配额三项 | --clear-quota]
                                                             配额整体替换：未给出的轴变为不限
lights3-ctl tenant delete <id>                                     仍拥有桶或凭证时被拒（409）
lights3-ctl tenant assign <id> <bucket> [--force]                  把已有桶归给租户；已属他租户须 --force
lights3-ctl tenant unassign <id> <bucket>                          解除归属（桶变为未归属）
```

```bash
lights3-ctl tenant create acme --display-name='ACME Corp' --max-bytes=1TiB --max-buckets=20
lights3-ctl tenant assign acme legacy-logs
lights3-ctl tenant get acme
```

### 3.8 `usage` —— 用量计数器

读取 `/-/admin/usage`（[multi-tenancy.md §2](multi-tenancy.md)）。root 看
全部桶，租户 admin 看本租户的桶；`--rescan` 对单个桶同步做一次全量计数并
打印结果（`usage.enabled=false` 时拒绝）。

```text
lights3-ctl usage [bucket] [-r|--rescan] [-t|--tenant=<id>]
```

```bash
lights3-ctl usage                       # 全部桶：objects / bytes / mpu_bytes / scanned_at
lights3-ctl usage --tenant=acme         # 只看 acme 所有的桶（root）
lights3-ctl usage logs --rescan         # 立即重算 logs 桶
```

### 3.9 `reload` —— 配置热重载

`POST /-/admin/config/reload`（root 专属）的 CLI 包装，与 `SIGHUP` 同一条路径，
但把结果返回给调用者：`applied`（已生效项，含 `backends: added <name> (<type>)` /
`backends: removed <name> (closing after in-flight requests drain)`）与
`requires_restart`（改了但需重启的键）。配置校验失败、新后端构建失败、删除仍被
tiered 条目或运行中的 fsck job 引用的后端时服务端回 400、命令退出码 1。

```text
lights3-ctl reload
```

```bash
lights3-ctl reload --endpoint=https://s3.example.com
```

### 3.10 `object` —— 对象内部布局（roadmap §6.2）

`GET /-/admin/objects/<bucket>/<key>`（root 专属）的 CLI 包装：打印对象在路由到的
后端里的物理布局，排障不再靠读日志或 hexdump。

```text
lights3-ctl object inspect <bucket> <key> [-o|--output=json|text]
```

各引擎报告的内容：

| 引擎 | attrs | extents |
| --- | --- | --- |
| localfs / xlocalfs | `data_path`、`inode`、`on_disk_bytes` vs `logical_size`、`etag`、`content_type`、`last_modified`、`meta_xattr`（present/absent）、`sidecar`、`tier`（stub 时附 `remote_etag`/`remote_at`） | 一个 `file`（id = inode） |
| duostore | `meta_version`、`tier`、`extents` 数、`stored_bytes` 等 | 每个 extent 的 `kind`（chunk/pack/rados）、`id`（文件/对象号）、`offset`、`length`、`crc32c` |
| tiered | 分层视图 `tier`/`local_bytes`/`local_mtime`（+ `remote_*`）与 `local_engine`，再以 `local.` 前缀附本地引擎的全部 attrs | 本地引擎的 extents |
| memory / cloudproxy | `layout: null` + `note` | — |

```bash
lights3-ctl object inspect photos 2026/01/a.jpg              # 服务端 JSON 原样
lights3-ctl object inspect photos 2026/01/a.jpg -o text      # 表格
```

### 3.11 `mpu` —— 僵尸 multipart 清理（roadmap §6.2）

走标准 S3 API（ListMultipartUploads / AbortMultipartUpload），任何对桶有权限的
凭证都能用，不涉及 admin 面。`list` 翻完所有分页，每个上传一行（发起时间、
年龄、uploadId、key）；`--older-than`/`--prefix` 决定选集，`abort --all` 对同一
选集动手。

```text
lights3-ctl mpu list <bucket> [--prefix=<p>] [--older-than=<dur>] [-o text|json]
lights3-ctl mpu abort <bucket> <key> <upload-id>
lights3-ctl mpu abort <bucket> --all [--prefix=<p>] [--older-than=<dur>]
```

```bash
lights3-ctl mpu list photos --older-than=1d
lights3-ctl mpu abort photos --all --older-than=7d          # 已消失的（404）按完成计
```

### 3.12 `duostore` / `tier` —— 在线网关上的后台任务与隔离区账本

§2.4 那几轮后台任务（duostore GC / 孤儿扫描，tiered 扫描 / GC / 对账）在**运行中的
网关**内立即跑一轮，不必另起进程——本地 meta 引擎（rocksdb/sqlite）持文件锁，离线
`lights3 duostore gc` 必须停服，这里则直接借网关自己的后端实例。与 `fsck --offline`
同一 job 模型（`app/admin_jobs.h`）：POST 发起、202 带 job id、GET 轮询，**同一后端
同一时刻只跑一个 job，不分操作**（各轮共用后端的维护状态；fsck 在跑时 gc 也被拒），
409 码为 `JobInProgress`（fsck 保留 `ScrubInProgress`）。隔离区账本只读：动账的
`release` / `purge` / `forget` 仍是离线 CLI 的事。

```text
POST /-/admin/duostore/<backend>/gc|scan            root；202 {"backend","op","job_id","running":true,"busy":true}
POST /-/admin/tier/<backend>/scan|gc|reconcile      409 JobInProgress = 该后端已有 job 在跑（任何操作）
                                                    404 未知后端；400 组里没有这个操作 / 后端类型不符
GET  /-/admin/duostore/<backend>/gc|scan            200 {"backend","op","running","busy","job_id","started_at_ms",
GET  /-/admin/tier/<backend>/scan|gc|reconcile           完成后再加 "finished_at_ms","duration_ms","kind","findings","aborted","stats"{…}}
GET  /-/admin/duostore/<backend>/quarantine         200 {"backend","kind":"duostore","entries":[{"pack_id","live_recs","corrupt_records","quarantined_at_ms","purged"}]}
GET  /-/admin/tier/<backend>/quarantine             200 {"backend","kind":"tiered","entries":[{"kind","bucket","key","etag","first_seen_ms","last_seen_ms","count"}]}
```

每个操作各留一份最近一次的文档（`running` 只指这个操作，`busy` 指该后端任一操作），
`stats` 逐字段镜像对应的 `*Stats` 结构（`DuoGcStats` / `DuoOrphanStats` /
`TierScanStats` / `TierGcStats` / `TierReconcileStats`），`findings` 是其中的丢失信号
之和：duostore gc = `records_corrupt + packs_quarantined`，duostore scan =
`refs_missing + pack_stats_missing`，tier reconcile = `refs_missing`，tier scan / gc
恒 0。网关关停会中断在跑的一轮（`aborted: true`）。审计事件 `<group>.<op>.start`。

```text
lights3-ctl duostore gc|scan <backend> [--no-wait | --status]
lights3-ctl duostore quarantine list <backend>
lights3-ctl tier scan|gc|reconcile <backend> [--no-wait | --status]
lights3-ctl tier quarantine list <backend>
```

与 `fsck --offline` 同一套驱动（`lights3_ctl_jobs.cc`）：发起后每 0.5s 轮询到结束并
打印结论文档，退出码 **1 = job 抛异常 / `aborted` / `findings > 0`**（注意这与离线
`lights3 duostore|tier` 的"恒 0/1"不同——在线入口沿用 fsck 的裁决约定，便于脚本判
断）；`--no-wait` 只打印 202 文档立即返回；`--status` 不发起、只查询。`quarantine list`
原样打印账本 JSON。

```bash
lights3-ctl duostore gc duodata                   # 立即回收空间，等到结束打印 DuoGcStats
lights3-ctl duostore scan duodata --no-wait       # 只拿 job id
lights3-ctl tier reconcile tierdata               # refs_missing > 0 时退出码 1
lights3-ctl tier scan tierdata --status           # 进度 / 上次结论
lights3-ctl tier quarantine list tierdata
```

## 4. 新增子命令的约定

- 每个命令组一个源文件（`lights3_ctl_<group>.cc/.h`，`make_<group>()` 返回根节点），
  在 `lights3_ctl.cc` 中 `add_subcommand`；连接选项经 `lights3_ctl_common.h` 的
  `add_conn_flags` / `read_conn_opts` 复用。同一机制的几个组可共用一个文件（`duostore` / `tier`
  与 `fsck --offline` 的 job 驱动同在 `lights3_ctl_jobs.cc`）。
- 回调无返回值，退出码通过 `lights3_ctl::g_exit` 传出，遵守 §1 的 0/1/2 约定；
  位置参数经 `c->args()` 读取并自行校验数量。
- 服务进程侧的运维入口放在 `lights3` 二进制的命令树下（如 `duostore`），按同样的方式
  拆分：`src/main.cc` 只保留根命令与 `main`，每个命令组一个 `src/cli/cli_<group>.cc/.h`
  （`make_<group>()`），公共助手（`--config`、`<backend>` 解析、`g_exit`）在
  `src/cli/cli_common.h`；仅在对应编译开关内注册（`cli_duostore.cc` 只在
  `LIGHTS3_DUOSTORE` 下参与编译），保证裁剪构建不出现不可用命令。

### 3.13 `tables` —— S3 Tables 目录：表桶、列表、维护、诊断

[s3-tables-design.md](s3-tables-design.md) 的运维入口（实现记录见
[s3-tables/step-4-maintenance.md §10](s3-tables/step-4-maintenance.md)）。目录调用打
`<prefix>/v1/...`（`--catalog-prefix`，默认 `/iceberg`，即 `tables.path_prefix`），签名
service 固定 `s3`；`plan` / `run` 走管理面作业（root 凭证），与 §3.12 同一 job 模型
（202 + job id、每 0.3s 轮询到结束、打印结论文档）。表以 `<namespace>.<table>` 给出，
多级 namespace 用 `.` 连接。

```text
lights3-ctl tables enable|disable|status <bucket>          PUT / DELETE / GET <prefix>/v1/buckets/<bucket>（enable / disable 须 root）
lights3-ctl tables list <bucket> [--namespace=a.b] [--json] 每行 "<namespace>\t<table>"；不指定 namespace 则遍历整棵树
lights3-ctl tables config <bucket> <ns.table> [--set=<json>]
                                                            GET / PUT …/maintenance/config：effective / table-config / defaults；
                                                            --set 的键：retain_recent_metadata_files delete_enabled max_snapshot_age_ms
                                                            min_snapshots_to_keep orphan_cleanup（省略的键回落到 tables.maintenance）
lights3-ctl tables plan <bucket> <ns.table> [--no-wait]     POST /-/admin/tables/<bucket>/<ns 各级>/<table>/plan，只读；"stats" 即 plan
lights3-ctl tables run <bucket> <ns.table> [--plan-job=<id>] [--yes] [--no-wait]
                                                            执行最近一次 plan（或 --plan-job）：快照过期是普通提交；delete_enabled
                                                            为真时才删候选文件，且没有 --yes 直接拒绝（退出码 2）；表已变 → StalePlan
lights3-ctl tables purge <bucket> <ns.table> --yes [--no-wait]
                                                            DELETE …?purgeRequested=true：先墓碑，再由作业删保留目录、location 前缀、
                                                            commit 记录与墓碑；不可逆，--yes 必填
lights3-ctl tables diagnose <bucket> <ns.table>            GET …/catalog/diagnostics（③）
lights3-ctl tables recover <bucket> <ns.table> [--prune]   POST …/catalog/recovery（③）
```

退出码：0 成功；1 请求失败或作业 `error`；2 用法错误（含 `run` 缺 `--yes`、`purge` 缺
`--yes`）。plan 文档里 `manual-review: true` 的表 `run` 会被服务端拒绝（400）——先看
`notes`（ref 带自己的保留规则、表属性与维护配置冲突、manifest 解析失败等）。

```bash
lights3-ctl tables enable lake
lights3-ctl tables list lake
lights3-ctl tables config lake sales.orders --set='{"delete_enabled":true,"max_snapshot_age_ms":432000000}'
lights3-ctl tables plan lake sales.orders          # 看候选与要过期的快照
lights3-ctl tables run lake sales.orders --yes      # 提交过期 + 删候选
lights3-ctl tables purge lake sales.tmp --yes
```
