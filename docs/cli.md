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
lights3 help [duostore [dump|load]]
```

| 选项 | 适用 | 默认 | 说明 |
| --- | --- | --- | --- |
| `-c, --config=<path>` | 全部 | `config/lights3.yaml` | YAML 配置文件（格式见 [architecture.md §5](architecture.md#5-配置文件示例)） |
| `--backend=<name>` | `duostore *` | — | 后端名，等价于第一个位置参数 |
| `--file=<path>` | `duostore *` | — | dump 文件路径，等价于第二个位置参数 |

### 2.1 启动服务

无子命令即为启动：`Application(config)` → `open_storage()` → `start_server()`
→ `run()`，阻塞到 SIGINT/SIGTERM，按 [architecture.md §4](architecture.md#4-进程结构与启动流程)
的顺序优雅关闭，`run()` 的返回值即退出码。启动期任何异常（配置解析失败、
后端打开失败、端口占用等）在 stderr 打 `fatal: …` 并以 `1` 退出；已构建
的后端经 `~Application` 关闭（duostore 封存 active pack、rados flush）。

```bash
export LIGHTS3_SECRET_1=my-secret
./build/lights3 --config=config/lights3.yaml
./build/lights3 -c /etc/lights3/lights3.yaml
```

### 2.2 `duostore dump` / `duostore load`

DuoStore 的逻辑 meta 备份与恢复（流格式与不变量见
[storage/duostore-core.md §11](storage/duostore-core.md#11-meta-dumpload)）。
仅在 `LIGHTS3_DUOSTORE` 构建中注册；两者都在**不监听端口**的前提下构建全部
后端，执行完即退出，写静默由此天然成立。`<backend>` 必须是配置中
`type: duostore` 的后端名，否则报错退出。

- `dump`：写出该后端全部 bucket/object 记录与已封存 pack 账本到 `<file>`
  （覆盖写）。
- `load`：逐条重放 `<file>` 到该后端（bucket 幂等，可中断重跑），末尾强制
  一次 orphan scan。

备份顺序：先拷贝数据目录再 `dump`；恢复时先放回数据再 `load`。

```bash
./build/lights3 duostore dump duo /backup/duo-meta.dump --config=/etc/lights3/lights3.yaml
./build/lights3 duostore load --backend=duo --file=/backup/duo-meta.dump -c /etc/lights3/lights3.yaml
```

## 3. `s3adm` —— 运维 CLI

`src/tools/s3adm*.cc`，构建产物与 `lights3` 同目录。三个命令组：`cred`
（凭证管理面）、`website`（桶静态网站配置）、`bench`（压测）。全部子命令
以 SigV4 自签名直连 lights3 的 HTTP 端点，无需 aws cli。

### 3.1 连接与凭证选项（所有叶子子命令共有）

| 选项 | 默认 | 说明 |
| --- | --- | --- |
| `-e, --endpoint=<url>` | `http://127.0.0.1:9000` | `scheme://host[:port]`；https 需要带 OpenSSL 的构建 |
| `--ak=<key>` / `--sk=<key>` | 环境变量 | 缺省回退 `LIGHTS3_ADMIN_AK` / `LIGHTS3_ADMIN_SK`；SK 建议走环境变量（argv 对本机 `ps` 可见） |
| `--region=<r>` | `us-east-1` | SigV4 region，须与服务端 `auth.region` 一致 |
| `--insecure` | false | https 跳过证书校验（自签名部署） |
| `--timeout-sec=<n>` | 10 | 连接/读/写超时 |

`cred` 与 `website` 要求 **root 静态凭证**（配置文件 `auth.credentials` 中的
条目，见 [credential-management.md §3](credential-management.md)），租户
凭证调用会得到 403。`bench` 用任意有权访问目标桶的凭证即可。

```bash
export LIGHTS3_ADMIN_AK=AKIDEXAMPLE
export LIGHTS3_ADMIN_SK=my-secret
```

### 3.2 `cred` —— 凭证管理

与 `/-/admin/credentials` 的四个接口一一对应；响应 JSON 原样输出到 stdout。

```text
s3adm cred list                          列出全部凭证（SK 掩码；含静态/文件/动态三来源）
s3adm cred get <ak> [-s|--show-secret]   查询单个凭证；--show-secret 返回明文 SK（仅动态/文件凭证，服务端记审计日志）
s3adm cred create [-c|--comment=<text>] [-p|--policy=<json>|@<file>]
                                         生成一对 AK/SK（唯一一次返回完整 SK）
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

首个错误打印到 stderr（`s3adm: bench: first error: …`），其余只计入 err
计数；准备阶段（建桶/预上传）失败直接以 `1` 退出。

```bash
s3adm bench put --bucket=test --size=4M --concurrency=8 --duration-sec=30
s3adm bench get -b test -s 4M -j 8 -d 30 --keep
s3adm bench stat -b test -j 16
s3adm bench list -b test -n 10000 --max-keys=1000
s3adm bench list-buckets -j 16
```

## 4. 新增子命令的约定

- 每个命令组一个源文件（`s3adm_<group>.cc/.h`，`make_<group>()` 返回根节点），
  在 `s3adm.cc` 中 `add_subcommand`；连接选项经 `s3adm_common.h` 的
  `add_conn_flags` / `read_conn_opts` 复用。
- 回调无返回值，退出码通过 `s3adm::g_exit` 传出，遵守 §1 的 0/1/2 约定；
  位置参数经 `c->args()` 读取并自行校验数量。
- 服务进程侧的运维入口放在 `src/main.cc` 的命令树下（如 `duostore`），
  仅在对应编译开关内注册，保证裁剪构建不出现不可用命令。
