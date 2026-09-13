# 开发指南

面向要改代码或写文档的人：仓库怎么组织、怎么构建与验证、代码与文档各守什么约定。
测试体系的细节在 [testing.md](testing.md)，性能复现在
[performance-baseline.md](performance-baseline.md)，未做的事在 [todo.md](todo.md)。

## 1. 仓库布局

| 目录 | 内容 |
| --- | --- |
| `src/core` | 协程 `Task` / Executor / 线程池、配置、日志、指标、TLS 等横切件 |
| `src/http` | 中立 HTTP 模型与四个驱动（builtin / beast / httplib / seastar） |
| `src/s3` | 路由、SigV4、S3 handler、XML 编解码、错误映射、website / 凭证 / 租户面 |
| `src/storage` | `IStorageBackend` 与各后端：localfs / xlocalfs / memory / tiered / cloudproxy / duostore |
| `src/tables` | S3 Tables（Iceberg REST catalog） |
| `src/app`、`src/cli`、`src/tools` | 进程装配、`lights3` 子命令、`lights3-ctl` |
| `tests/unit`、`tests/e2e`、`tests/fuzz`、`tests/fixtures` | 单测（自带的 `mini_test.h` 框架）、e2e 脚本、libFuzzer harness 与语料、固件 |
| `config/lights3.yaml` | 全部配置键及默认值的权威说明 |
| `scripts/`、`packaging/`、`docker/`、`deploy/` | 安装 / 打包 / 容器 / 监控资产 |
| `third_party/` | 子模块（ccmd、spdlog、httplib、json、rocksdb、hiredis、sqlite、client-c、seastar）与本地补丁 |

分层与请求生命周期见 [architecture/overview.md](../architecture/overview.md)。

## 2. 构建

```bash
./build.sh --test                 # 初始化子模块 + cmake + ninja + ctest 一条龙
make debug / make release         # Debug 到 build/，Release 到 build-rel/
make test                         # Debug 构建后跑快速 ctest 集（去掉 perf / soak / mint）
make coverage                     # -O0 --coverage 构建到 build-cov/ 并出行覆盖率报告
make package                      # Release + CPack（deb / rpm / tgz）
```

- `build.sh` 的开关：`--redis` / `--sqlite` / `--tikv` / `--rados` 打开 duostore 的可选
  引擎，`--seastar` 打开 seastar 驱动，`--asan` / `--tsan` / `--ubsan` / `--coverage` /
  `--fuzz` 出对应变体。**这些 CMake 选项在 cache 里是粘性的**：变体一律用独立目录
  （`-B build-redis`、`build-tikv`、`build-rados`、`build-asan`…），不要在 `build/`
  里反复 `cmake -D` 翻转选项；增量重建用 `cmake --build build-xxx`。
- 并行度默认取一半核（`make JOBS=N` 覆盖）；并行大构建下 io_uring 可能 `ENOMEM`，
  xlocalfs 单测偶发失败时降 `-j` 重跑即可。
- ccmd 子模块需要 `--recursive`（内嵌 cflag）；`third_party/patches/` 里是尚未合入
  上游的补丁及其说明。

## 3. 验证

- 快速集：`make test`，或 `ctest --test-dir build -LE "perf|soak|mint"`。e2e 段绑定
  固定端口、共享临时目录，ctest 必须串行。
- 全矩阵：`scripts/check-all.sh` 对每个存在的 `build-*` 目录做增量构建 + ctest，
  `--with-perf` / `--with-soak` / `--with-tables-smoke` 加上门禁与冒烟；改动数据面
  另跑 `scripts/bench_gate.sh`。项目不使用 GitHub Actions，验证全靠本地脚本。
- 新测试用例放在测试文件的 `#endif` 守卫之内；外部依赖（redis / PD / rados）缺失时
  用例显式 SKIP，不要静默通过。
- `unit_tests` 的输出含 NUL 字节，过滤时 `grep` 加 `-a`；偶发 terminate 先用
  `catch throw` / `ulimit -c` 抓栈，不要靠外层 `timeout` 兜底。
- 构建输出用 `grep -E "error|FAILED"` 过滤并核对二进制时间戳，确认真的重链了。

## 4. 代码约定

- 格式：`.clang-format`（Google 风格，4 空格缩进，120 列）。`make format` 先把行尾
  注释挪到上一行（`scripts/check_comments.py`）再跑 clang-format；`make format-check`
  只报告。二者只处理 `git ls-files` 里的文件，**新文件先 `git add -N` 再 format**。
- 注释规则：`//` 注释独占一行放在所描述代码上方，行尾注释只允许 `}  // namespace x`、
  `#endif  // GUARD` 这类收尾标注。
- 引用设计文档：注释里写 `docs/<group>/<name>.md §N`（例如
  `docs/architecture/storage/duostore-design.md §5.2`），中英文版章节号一致，
  两边通用。`roadmap §N` / `backlog §N` / `backlog-sequence ①…⑩` /
  `gaps` / `issues T<n>` 指 [../archive/](../archive/) 下的只读历史账本。
- 协程：`catch` 块内不能 `co_await`；协程内等待用 `drive` 路径的 `sync_wait` /
  `Started::wait`，否则死锁；GCC 15 对 `co_await` 成员指针有 ICE，先存局部变量；
  协程内的结构化绑定改具名 pair，避免 `maybe-uninitialized`。细节见
  [architecture/coroutine-internals.md](../architecture/coroutine-internals.md)。
- 第三方目标一律 `target_compile_options(-w)`；debug / release 构建保持零告警。
- 新 `?flag` 路由要放在 `flag=""` 兜底之前；新指标经 `MetricsScope` 接入
  （示范 `DuoStoreBackend::init_metrics`）。
- 改 Grafana dashboard 先改 `deploy/grafana/gen_dashboard.py` 再生成 json。

## 5. 文档约定

- 目录分三组：[architecture/](../architecture/)（设计与实现原理）、
  [usage/](../usage/)（部署与运维）、[development/](./)（构建、测试、待办）。
  中文是原文，`docs/en/` 是镜像，**章节编号一一对应**；改一处就同步另一处。
  存储层的 13 篇实现级文档只有中文。
- 设计文档写"为什么"，实现文档写"代码怎么落地"，两者互链不重复。
- 待办只记在 [todo.md](todo.md)，做完即删，实现细节写回对应设计文档；不留划线
  历史。归档账本 `docs/archive/` 只读。
- 配置键以 `config/lights3.yaml` 的注释为准，文档不重复抄默认值。

## 6. 分支与提交

- 从 `main` 切分支（`feat/…`、`fix/…`、`chore/…`、`docs/…`），提交前 **先核对分支**。
- 提交说明写清动机与验证方式；说"已修"必须真的跑过对应用例。
- 推分支后在 GitHub 上开 PR；合并前跑一遍 §3 的快速集，涉及变体引擎的改动跑
  对应 `build-*`。
- worktree 里不要执行 `git submodule update`（会清空主树的子模块）。
