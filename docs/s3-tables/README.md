# S3 Tables 实施文档（按 [../s3-tables-design.md](../s3-tables-design.md) §14 分步）

> 状态：**实施稿（2026-09-11）；①②③④ 已实现（2026-09-12），⑤⑥ 未动代码**。设计层文档是
> [../s3-tables-design.md](../s3-tables-design.md)（下文简称"设计 §N"）；本目录
> 把 §14 的六个步骤各展开成一份可直接照着写代码的实现文档：新增/修改的文件、
> 数据结构与函数签名、逐步流程、接入点（现有代码的文件与函数）、配置、单测清单、
> 陷阱。每步独立可合并；做完一步把该步文档顶部的状态改为"已实现 + 日期"，
> 并把偏离设计的决定回写到设计文档。实现级文档只有中文版（与
> [../storage/README.md](../storage/README.md) 的约定一致）。

| 步骤 | 文档 | 内容 | 依赖 |
| --- | --- | --- | --- |
| ① **已实现** | [step-1-catalog-core.md](step-1-catalog-core.md) | `tables:` 配置、表桶标记、`ITableCatalogStore` + `ObjectCatalogStore`、Iceberg 元数据模型（requirements / updates / 迁移不变量）、提交协议与幂等、REST 最小集（config / buckets / namespaces / tables / metadata-location）、错误模型、dispatch 分支、表桶守卫（保留前缀只读 + DeleteBucket）、审计与指标 | 无 |
| ② **已实现** | [step-2-authz-credentials.md](step-2-authz-credentials.md) | policy 三元组映射、租户隔离、`s3tables` 签名名、`mint_session` 收窄参数与 `vended-credentials`、`GET …/credentials`、lifecycle 排除 | ① |
| ③ **已实现** | [step-3-validation-diagnostics.md](step-3-validation-diagnostics.md) | Avro OCF 读取器、快照图与冲突复核、`catalog/diagnostics` / `recovery`、rename 恢复、`fsck` 对账、LoadTable 的 ETag / If-None-Match | ① |
| ④ **已实现** | [step-4-maintenance.md](step-4-maintenance.md) | `JobOp::Table*`、plan / run / purge、`purgeRequested=true`、周期 runner、CLI、墓碑清理 | ①③ |
| ⑤ | [step-5-multi-gateway-docs.md](step-5-multi-gateway-docs.md) | 双网关用例、`--check-config` 误配 WARN、deployment.md 矩阵、文档转正与 `docs/en/` 同步 | ①–④ |
| ⑥ | [step-6-optional.md](step-6-optional.md) | views、`/_iceberg/v1` 别名、`reportMetrics` 落审计、compaction 候选规划、duostore-meta 后备 | 按需 |

## 贯穿各步的约定

- **命名空间与目录**：全部新代码在 `src/tables/`，命名空间 `lights3::tables`；
  Iceberg 纯函数在 `src/tables/iceberg/`，命名空间 `lights3::tables::iceberg`。
  只依赖 `core/`、`http/model.h`、`storage/backend.h`、`s3/errors.h`、
  `s3/auth/policy.h`；**不**反向依赖 `s3/service.h`（service 依赖 tables，不能成环）。
- **错误**：目录内部统一抛 `tables::RestError`（`rest_error.h`），只在 REST 边界
  渲染成 JSON；存储层抛上来的 `s3::S3Error` 在 `catalog.cc` 的一处
  `map_storage_error` 转换（设计 §6.6）。
- **JSON**：nlohmann/json，`.cc` 里用；头文件只前置声明或用 `nlohmann::json`
  已在 `service.h` 公开的事实（`lights3_core` 已 PUBLIC 链接）。
- **协程**：所有 IO 接口返回 `Task<T>`；纯计算（JSON / Avro 解析）在调用方先
  `co_await pool->schedule()` 后同步执行；协程 `catch` 内不能 `co_await`
  （先存 `std::exception_ptr` 再出 catch）。
- **测试**：`tests/unit/mini_test.h`（`TEST` / `CHECK` / `CHECK_EQ` /
  `CHECK_THROWS_S3`），进程内 `S3Service` + `MemoryBackend`（`test_service.cc`
  的 `make_router` / `make_req` / `body_of` 模式）；新用例放在测试文件的
  `#endif` 守卫之内；e2e 段用 `tests/e2e/run_e2e.sh` 的 `s3curl` 函数。
- **文档回写**：每步完成后更新 `docs/s3-protocol.md §1`（API 表加 Tables 行）、
  `docs/cli.md`（新子命令）、`docs/config-reload.md §4`（restart-only 键）、
  `docs/testing.md §1/§2`（新 ctest 用例与 e2e 段）。
