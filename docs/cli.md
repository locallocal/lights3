# 命令行工具：`lights3` 与 `s3adm`

本文是两个可执行文件的命令参考。二者都基于 `third_party/ccmd`
（header-only 子命令框架，内嵌 `cflag` 做选项解析），共享同一套命令行语义，
先在 §1 说清，后文不再重复。启动装配流程见 [architecture.md §4](architecture.md#4-进程结构与启动流程)，
凭证管理面见 [credential-management.md](credential-management.md)，
静态网站见 [static-website.md](static-website.md)。

## 1. ccmd 通用语义

- **命令树**：`<程序> [<命令组> [<子命令>]] [位置参数] [选项]`。
  `<程序> help [<命令组> [<子命令>]]` 或任一层级的 `-h/--help` 打印该层帮助。
- **选项不向下继承**：每个叶子子命令拥有独立的选项集，选项必须写在叶子
  子命令之后（`s3adm cred list --endpoint=…`，而不是 `s3adm --endpoint=… cred list`）。
- **长选项取值只接受 `--name=value`**；`--name value` 会被 cflag 当作缺值报错。
  短选项两种都可以：`-e http://…` 或 `-ehttp://…`。bool 选项裸写即为 true
  （`--insecure`、`--keep`）。
  例外：`lights3` 主程序在进入 ccmd 前把 `--config <path>`（以及
  `--backend`/`--file`）折叠成 `=` 形式，因此空格写法对 `lights3` 也可用
  （e2e 脚本与旧文档沿用这一写法）；`s3adm` 没有这层兼容。
- **`--` 终止选项解析**，其后全部视为位置参数。
- **退出码**：`0` 成功；`1` 运行期失败（请求被拒、IO 错误、服务启动异常）；
  `2` 用法错误（缺位置参数、缺凭证、数值越界、裸命令组）。ccmd 自身对
  未知命令/未知选项以 `1` 退出并在 stderr 给出提示。

## 2. `lights3` —— 服务进程

```text
lights3 [--config=<path>]                                  启动服务
lights3 duostore dump <backend> <file> [--config=<path>]   导出 duostore meta
lights3 duostore load <backend> <file> [--config=<path>]   导入 duostore meta
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
| `--backend=<name>` | `duostore *`、`tier *`、`fsck` | — | 后端名，等价于第一个位置参数 |
| `--file=<path>` | `duostore dump|load` | — | dump 文件路径，等价于第二个位置参数 |
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
`backends[].type` 是否编进了本二进制，然后打印配置解析出的摘要（驱动/监听/TLS、
线程数、凭证数、后端列表、路由规则数、网站条目数、日志与审计设置）。退出码
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

### 2.3 `fsck`

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
- 其余类型（memory/cloudproxy/tiered）报错退出。

退出码：`0` 干净；`1` 存在完整性发现（duostore 的 corrupt/unreadable/
refs_missing/meta_errors，localfs 的 mismatches/read_errors）。警告级计数
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
  见 [tiered-storage.md §9](tiered-storage.md)。

```bash
./build/lights3 duostore gc duodata --config=/etc/lights3/lights3.yaml   # 立即回收空间
./build/lights3 tier reconcile tierdata -c /etc/lights3/lights3.yaml
./build/lights3 tier quarantine list tierdata -c /etc/lights3/lights3.yaml
./build/lights3 tier quarantine purge tierdata archive photos/2024/a.jpg -c /etc/lights3/lights3.yaml
```

### 2.5 配置热重载：`SIGHUP`

`kill -HUP <pid>` 让服务进程重新读取 `--config` 指定的文件（roadmap §4.4，
[config-reload.md](config-reload.md)）：整体校验后只应用可热更新子集（日志级别、
`request_timeout`/`transfer_stall_timeout`、`max_inflight_requests`、`min_part_size`、
限流、bucket 路由规则、TLS 证书内容），其余改动逐项 WARN "需重启"；文件校验失败
则一字不改。systemd 单元可配 `ExecReload=/bin/kill -HUP $MAINPID`。同一动作也可经
`s3adm reload`（§3.9）触发并拿到报告。

## 3. `s3adm` —— 运维 CLI

`src/tools/s3adm*.cc`，构建产物与 `lights3` 同目录。命令组：`cred`（凭证
管理面）、`website`（桶静态网站配置）、`bench`（压测）、`fsck`（在线对象
校验）、`quota`（桶配额）、`tenant`（租户与桶归属）、`usage`（用量计数器，
roadmap §3.9，见 [multi-tenancy.md](multi-tenancy.md)）、`reload`（配置热重载，
[config-reload.md](config-reload.md)）。全部子命令以 SigV4
自签名直连 lights3 的 HTTP 端点，无需 aws cli。

### 3.1 连接与凭证选项（所有叶子子命令共有）

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `-e, --endpoint=<url>` | `http://127.0.0.1:9000` | `scheme://host[:port]`；https 需要带 OpenSSL 的构建 |
| `--ak=<key>` / `--sk=<key>` | 环境变量 | 缺省回退 `LIGHTS3_ADMIN_AK` / `LIGHTS3_ADMIN_SK`；SK 建议走环境变量（argv 对本机 `ps` 可见） |
| `--region=<r>` | `us-east-1` | SigV4 region，须与服务端 `auth.region` 一致 |
| `--insecure` | false | https 跳过证书校验（自签名部署） |
| `--timeout-sec=<n>` | 10 | 连接/读/写超时 |

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

与 `/-/admin/credentials` 的四个接口一一对应；响应 JSON 原样输出到 stdout。

```text
s3adm cred list                          列出全部凭证（SK 掩码；含静态/文件/动态三来源）
s3adm cred get <ak> [-s|--show-secret]   查询单个凭证；--show-secret 返回明文 SK（仅动态/文件凭证，服务端记审计日志）
s3adm cred create [-c|--comment=<text>] [-p|--policy=<json>|@<file>] [-t|--tenant=<id>] [-r|--role=user|admin]
                                         生成一对 AK/SK（唯一一次返回完整 SK）；--tenant 归属租户（租户 admin 调用时
                                         服务端固定为本租户，可省略），--role=admin 授予本租户管理面
s3adm cred delete <ak>                   吊销动态凭证（静态凭证归配置文件管，服务端拒绝）
```

`--policy` 取内联 JSON 或 `@file`，结构
`{"buckets":[...],"prefixes":[...],"readonly":bool,"actions":[...]}`，语义见
[credential-management.md §11](credential-management.md)。

```bash
s3adm cred create --comment=tenant-a --policy='{"buckets":["tenant-a-*"],"readonly":false}'
s3adm cred create -c ci-runner -p @policies/ci.json
s3adm cred get L3AK7Q2MXX5EIY4BJZW3 --show-secret
s3adm cred list --endpoint=https://s3.example.com --insecure
s3adm cred delete L3AK7Q2MXX5EIY4BJZW3
s3adm cred create --tenant=acme --role=admin --comment='acme operator'
```

### 3.3 `website` —— 桶静态网站配置

操作 `?website` 子资源（[static-website.md §4](static-website.md)）。`set`
后桶变为匿名可读（仅 GET/HEAD 对象），带 index/error 文档语义；YAML 中
静态配置的桶服务端拒绝动态修改（405）。

```text
s3adm website get <bucket>                            打印配置 XML（未配置为 404 → 退出码 1）
s3adm website set <bucket> [-i|--index-suffix=<s>] [-k|--error-key=<key>]
                                                      启用/替换配置；index-suffix 默认 index.html，不得含 '/'；
                                                      error-key 为空用内置错误页
s3adm website delete <bucket>                         删除配置（幂等），桶不再匿名可读
```

```bash
s3adm website set my-site --index-suffix=index.html --error-key=404.html
s3adm website get my-site
s3adm website delete my-site
```

### 3.4 `bench` —— 压测

对数据面（put/get）与非 IO 接口（stat/list/list-buckets）做闭环压测：
`--concurrency` 个 worker 各持一条连接，在 `--prefix` 下 `--objects` 个键组成
的池上循环请求 `--duration-sec` 秒；每秒打印一行区间统计，结束打印汇总
（ops、ops/s、MiB/s、avg/p50/p90/p99/max 延迟）。

```text
s3adm bench put           上传（池内键轮转覆盖写）
s3adm bench get           下载（先上传整池）
s3adm bench stat          HeadObject（先上传整池）
s3adm bench list          ListObjectsV2（先上传整池，--max-keys 控制每页）
s3adm bench list-buckets  ListBuckets（不需要 --bucket）
```

| 选项 | 默认 | 范围/说明 |
| --- | --- | --- |
| `-b, --bucket=<name>` | — | 目标桶，不存在则创建；`list-buckets` 外必填 |
| `-j, --concurrency=<n>` | 4 | 1–256 |
| `-d, --duration-sec=<n>` | 10 | 1–86400 |
| `-n, --objects=<n>` | 64 | 1–1000000，键池大小 |
| `-s, --size=<sz>` | put/get `1M`，stat/list `4K` | 字节或 K/M/G 后缀，上限 1G |
| `--prefix=<p>` | `s3adm-bench/` | 键前缀 |
| `--max-keys=<n>` | 100 | 仅 `list` |
| `--keep` | false | 结束后保留对象（默认删除整池） |
| `-o, --output=text\|json` | `text` | `json`：stdout 只输出一个 JSON 对象（mode、wall_s、workers、keys、size、ops、errors、ops_per_s、mib_per_s、latency_ms{avg,p50,p90,p99,max}），每秒表格与准备/清理提示改到 stderr——`scripts/bench_gate.sh` 的基线比对输入（roadmap §6.2） |

首个错误打印到 stderr（`s3adm: bench: first error: …`），其余只计入 err
计数；准备阶段（建桶/预上传）失败直接以 `1` 退出。

```bash
s3adm bench put --bucket=test --size=4M --concurrency=8 --duration-sec=30
s3adm bench get -b test -s 4M -j 8 -d 30 --keep
s3adm bench stat -b test -j 16
s3adm bench list -b test -n 10000 --max-keys=1000
s3adm bench list-buckets -j 16
```

### 3.5 `fsck` —— 在线对象校验

`lights3 fsck`（§2.3）的在线补集：走 S3 API 端到端校验——ListObjectsV2 逐页
列举，逐对象流式 GET 并在客户端重算 MD5 与 ETag 对照，顺带覆盖了网关读
路径本身。multipart 复合 ETag 经 `GET ?partNumber=i` 逐分片下载重算
（服务端无布局的存量对象回 501，计为 UNVERIFIABLE 而非 MISMATCH）。
纯只读；任何能读目标桶的凭证即可。代价是全部字节走一遍 HTTP——深检
（duostore crc/refs 对账）仍需服务器侧的 `lights3 fsck`。

```text
s3adm fsck <bucket> [-p|--prefix=<p>] [--max-mbps=<n>]
```

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `-p, --prefix=<p>` | — | 只校验该前缀下的 key |
| `--max-mbps=<n>` | `0` | 下载限速（MB/s），`0` 不限速 |

逐条打印 `MISMATCH <key>`（stdout）/ 传输错误（stderr），结尾一行汇总
（objects/bytes/mismatches/errors/unverifiable/skipped，skipped = 列举与
GET 之间被删除的对象）。退出码：`0` 干净；`1` 有 mismatch 或错误。

```bash
s3adm fsck my-bucket --endpoint=https://s3.example.com
s3adm fsck my-bucket --prefix=photos/ --max-mbps=50
```

### 3.6 `quota` —— 桶配额

操作 `?quota` 子资源（[multi-tenancy.md §3](multi-tenancy.md)）。`set` 整体
替换限额，至少一轴 > 0；`get` 任何能访问该桶的凭证可用，`set`/`clear` root
专属。超限的写请求得 `QuotaExceeded`(403)。

```text
s3adm quota get <bucket>                                    打印配额 XML（未配置 404 → 退出码 1）
s3adm quota set <bucket> [-b|--max-bytes=<sz>] [-o|--max-objects=<n>]
                                                            设置/替换配额；sz 接受 KiB/MiB/GiB 后缀，0 = 该轴不限
s3adm quota clear <bucket>                                  删除配额（幂等）
```

```bash
s3adm quota set logs --max-bytes=50GiB --max-objects=1000000
s3adm quota get logs
s3adm quota clear logs
```

### 3.7 `tenant` —— 租户与桶归属

操作 `/-/admin/tenants`（[multi-tenancy.md §6](multi-tenancy.md)）。变更
root 专属；`list`/`get` 租户 admin 可查本租户。响应 JSON 原样输出。

```text
s3adm tenant list                                            列出租户（含配额、所有桶、聚合用量、凭证数）
s3adm tenant get <id>                                        单个租户
s3adm tenant create <id> [--display-name=<s>] [--max-bytes=<sz>] [--max-objects=<n>] [--max-buckets=<n>]
                                                             创建；id 形如 [a-z0-9][a-z0-9._-]{0,63}
s3adm tenant update <id> [--display-name=<s>] [配额三项 | --clear-quota]
                                                             配额整体替换：未给出的轴变为不限
s3adm tenant delete <id>                                     仍拥有桶或凭证时被拒（409）
s3adm tenant assign <id> <bucket> [--force]                  把已有桶归给租户；已属他租户须 --force
s3adm tenant unassign <id> <bucket>                          解除归属（桶变为未归属）
```

```bash
s3adm tenant create acme --display-name='ACME Corp' --max-bytes=1TiB --max-buckets=20
s3adm tenant assign acme legacy-logs
s3adm tenant get acme
```

### 3.8 `usage` —— 用量计数器

读取 `/-/admin/usage`（[multi-tenancy.md §2](multi-tenancy.md)）。root 看
全部桶，租户 admin 看本租户的桶；`--rescan` 对单个桶同步做一次全量计数并
打印结果（`usage.enabled=false` 时拒绝）。

```text
s3adm usage [bucket] [-r|--rescan] [-t|--tenant=<id>]
```

```bash
s3adm usage                       # 全部桶：objects / bytes / mpu_bytes / scanned_at
s3adm usage --tenant=acme         # 只看 acme 所有的桶（root）
s3adm usage logs --rescan         # 立即重算 logs 桶
```

### 3.9 `reload` —— 配置热重载

`POST /-/admin/config/reload`（root 专属）的 CLI 包装，与 `SIGHUP` 同一条路径，
但把结果返回给调用者：`applied`（已生效项）与 `requires_restart`（改了但需重启
的键）。配置校验失败时服务端回 400、命令退出码 1。

```text
s3adm reload
```

```bash
s3adm reload --endpoint=https://s3.example.com
```

### 3.10 `object` —— 对象内部布局（roadmap §6.2）

`GET /-/admin/objects/<bucket>/<key>`（root 专属）的 CLI 包装：打印对象在路由到的
后端里的物理布局，排障不再靠读日志或 hexdump。

```text
s3adm object inspect <bucket> <key> [-o|--output=json|text]
```

各引擎报告的内容：

| 引擎 | attrs | extents |
| --- | --- | --- |
| localfs / xlocalfs | `data_path`、`inode`、`on_disk_bytes` vs `logical_size`、`etag`、`content_type`、`last_modified`、`meta_xattr`（present/absent）、`sidecar`、`tier`（stub 时附 `remote_etag`/`remote_at`） | 一个 `file`（id = inode） |
| duostore | `meta_version`、`tier`、`extents` 数、`stored_bytes` 等 | 每个 extent 的 `kind`（chunk/pack/rados）、`id`（文件/对象号）、`offset`、`length`、`crc32c` |
| tiered | 分层视图 `tier`/`local_bytes`/`local_mtime`（+ `remote_*`）与 `local_engine`，再以 `local.` 前缀附本地引擎的全部 attrs | 本地引擎的 extents |
| memory / cloudproxy | `layout: null` + `note` | — |

```bash
s3adm object inspect photos 2026/01/a.jpg              # 服务端 JSON 原样
s3adm object inspect photos 2026/01/a.jpg -o text      # 表格
```

### 3.11 `mpu` —— 僵尸 multipart 清理（roadmap §6.2）

走标准 S3 API（ListMultipartUploads / AbortMultipartUpload），任何对桶有权限的
凭证都能用，不涉及 admin 面。`list` 翻完所有分页，每个上传一行（发起时间、
年龄、uploadId、key）；`--older-than`/`--prefix` 决定选集，`abort --all` 对同一
选集动手。

```text
s3adm mpu list <bucket> [--prefix=<p>] [--older-than=<dur>] [-o text|json]
s3adm mpu abort <bucket> <key> <upload-id>
s3adm mpu abort <bucket> --all [--prefix=<p>] [--older-than=<dur>]
```

```bash
s3adm mpu list photos --older-than=1d
s3adm mpu abort photos --all --older-than=7d          # 已消失的（404）按完成计
```

## 4. 新增子命令的约定

- 每个命令组一个源文件（`s3adm_<group>.cc/.h`，`make_<group>()` 返回根节点），
  在 `s3adm.cc` 中 `add_subcommand`；连接选项经 `s3adm_common.h` 的
  `add_conn_flags` / `read_conn_opts` 复用。
- 回调无返回值，退出码通过 `s3adm::g_exit` 传出，遵守 §1 的 0/1/2 约定；
  位置参数经 `c->args()` 读取并自行校验数量。
- 服务进程侧的运维入口放在 `src/main.cc` 的命令树下（如 `duostore`），
  仅在对应编译开关内注册，保证裁剪构建不出现不可用命令。
