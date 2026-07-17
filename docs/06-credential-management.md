# 06 凭证管理：AK/SK 的生成、查询与持久化（方案）

> 状态：已实现（一期，见 §9）。承接 docs/05 §3.5 预留的 `ICredentialProvider` 扩展点。
> 代码：`src/s3/auth/credential_store.{h,cc}`、`src/s3/handlers/admin_credentials.cc`。

## 1. 目标与非目标

**目标**

- 运行期通过 API 生成 / 查询 / 吊销 AK/SK，无需改配置文件或重启进程；
- 生成的凭证经 `IStorageBackend` 持久化，进程重启后自动恢复；
- 验签路径保持现状的同步内存查表，不因动态凭证引入异步或明显开销；
- 配置文件静态凭证的行为完全不变（向后兼容）。

**非目标（本期不做）**

- IAM 式细粒度 policy——所有凭证仍等价于超级用户的数据面权限（见 §3 两级模型）；
- STS 临时凭证 / 凭证轮换到期；
- 多实例共享后端时的跨节点失效通知（限制见 §7）。

## 2. API 设计

沿用 `/-/` 保留路径（现有 `/-/healthz`、`/-/metrics` 先例），挂在
`/-/admin/credentials` 下。与 `/-/healthz` 等匿名端点不同，admin API
**必须通过 SigV4 验签且请求方为 root 凭证**（定义见 §3）。

| 方法与路径 | 操作 | 成功响应 |
| --- | --- | --- |
| `POST /-/admin/credentials` | 生成一对 AK/SK，可带 `?comment=` 备注 | `201` + JSON（唯一一次完整返回 SK） |
| `GET /-/admin/credentials` | 列出全部凭证（含静态凭证，SK 掩码） | `200` + JSON 列表 |
| `GET /-/admin/credentials/{ak}` | 查询单个凭证元数据；`?show-secret=true` 时返回明文 SK | `200` + JSON |
| `DELETE /-/admin/credentials/{ak}` | 吊销（仅限动态凭证，静态凭证归配置文件管） | `204` |

响应用 JSON，序列化/解析引入 [nlohmann/json](https://github.com/nlohmann/json)
（header-only，git 子模块进 `third_party/`，与 gflags/spdlog/httplib 同一套
管理方式，引入方式见 §5.4）。管理面是新造的 API，没有 S3 兼容包袱，JSON
对人和脚本都更友好；数据面 S3 协议继续走 `s3/xml.cc`，两者互不影响。示例：

```json
// POST 响应（201）
{
  "access_key": "L3AK7Q2MXX5EIY4BJZW3",
  "secret_key": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY0",
  "comment": "ci-runner",
  "created_at": "2026-07-17T12:00:00Z"
}

// GET 列表响应（200；SK 掩码，source 区分静态/动态）
{
  "credentials": [
    { "access_key": "AKIDEXAMPLE", "secret_key_masked": "wJal****KEY0",
      "source": "static" },
    { "access_key": "L3AK7Q2MXX5EIY4BJZW3", "secret_key_masked": "wJal****KEY0",
      "source": "dynamic", "comment": "ci-runner",
      "created_at": "2026-07-17T12:00:00Z" }
  ]
}
```

错误码沿用现有 `S3Error` 体系，但 admin 分支自行捕获并渲染成 JSON 体
`{"code": "AccessDenied", "message": "..."}`（HTTP 状态码不变）；只有漏到
dispatch 外层兜底 catch 的意外异常才落回 S3 XML 的 500。映射：

| 场景 | 错误 |
| --- | --- |
| 非 root 凭证调用 admin API | `AccessDenied` (403) |
| 认证整体关闭（无静态凭证） | `AccessDenied`——否则任何人可造凭证，见 §3 |
| 查询/删除的 AK 不存在 | `InvalidAccessKeyId` (403，与验签路径一致) |
| 删除静态凭证 | `MethodNotAllowed` (405) |
| 生成时 AK 碰撞重试仍失败 | `InternalError` (500) |

**设计取舍——查询是否返回 SK**：默认掩码、`?show-secret=true` 显式索取。
SigV4 是 HMAC 方案，服务端必须保存可逆的 SK（不能只存哈希），因此"查询
返回明文"在能力上无法避免；但默认掩码可以防止列表页、日志、终端回显等
低成本泄露面。

## 3. 权限模型：两级凭证

```text
静态凭证（config auth.credentials）  = root：数据面 + admin API
动态凭证（API 生成，storage 持久化） = 普通：仅数据面
```

- 动态凭证不能再调用 admin API，杜绝"凭证生凭证"的提权链；
- 认证关闭（静态表为空）时 admin API 一并拒绝：没有 root 就没有管理面；
- AK 归属判定就在 `CredentialStore` 内完成（静态/动态两个来源标记）。

## 4. 存储布局

### 4.1 位置：保留系统 bucket `.sys`

- 凭证写入 `default_backend` 上名为 `.sys` 的 bucket，对象键
  `credentials/{ak}`，一凭证一对象；
- `validate_bucket_name()` 在各后端内部也会调用，因此对保留名 `.sys`
  **放行**（src/storage/validate.cc）；用户请求的拦截上移到 L2：dispatch
  在路由前拒绝一切 `.` 开头的 bucket（InvalidBucketName），`.sys` 仅
  CredentialStore 可达；`ListBuckets` 聚合时同样过滤 `.` 前缀；
- 首次写入前用 `create_bucket(".sys")` 惰性建桶（幂等，已存在则忽略）。

选 `IStorageBackend` 而非旁路本地文件的理由：复用现成的原子写（LocalFs
staging + rename）、换后端自动跟随；代价是 memory 后端下凭证不持久——
文档与启动日志明示该限制即可（memory 本就是测试后端）。

### 4.2 对象内容

管理面已引入 nlohmann/json（§2），落盘格式同用 JSON，读写共享一套
序列化代码：

```json
{
  "version": 1,
  "sk": "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY0",
  "created": "2026-07-17T12:00:00Z",
  "comment": "ci-runner"
}
```

SK 以明文落盘，密级等同于配置文件里的静态 SK（同一台机器、同一套文件
权限）。`version` 字段为二期的 at-rest 加密（master key 来自环境变量，
AES-256-GCM，OpenSSL 已在依赖里）预留升级路径。

## 5. 组件与数据流

### 5.1 新增 `s3/auth/credential_store.{h,cc}`（L2）

```cpp
class CredentialStore final : public ICredentialProvider {
public:
    // 启动时全量加载：list(".sys", "credentials/") + 逐个 get，
    // 与静态表合并（同 AK 时静态优先并告警）
    static Task<std::shared_ptr<CredentialStore>> load(
        std::shared_ptr<storage::IStorageBackend> backend, const AuthConfig& static_cfg);

    // 验签热路径：同步内存查表（shared_mutex 读锁）
    std::optional<std::string> secret_for(std::string_view ak) const override;
    bool has_credentials() const override;
    bool is_root(std::string_view ak) const;   // 静态来源 → root

    // 管理面：先写 storage 成功再改内存（write-through，崩溃时以 storage
    // 为准，内存顶多"少"不会"多"）。注意不持锁 co_await（协程可能换线程
    // 恢复，std::mutex 跨线程解锁是 UB），并发唯一性靠 AK 随机空间保证
    Task<CredentialInfo> generate(std::string comment);
    Task<void>           remove(std::string_view ak);
    std::optional<CredentialInfo> find(std::string_view ak) const;
    std::vector<CredentialInfo>   list() const;
};
```

### 5.2 `SigV4Authenticator` 改造

把私有的 `std::map creds_` 换成 docs/05 §3.5 预留的接口：

```cpp
struct ICredentialProvider {
    virtual std::optional<std::string> secret_for(std::string_view ak) const = 0;
    virtual bool has_credentials() const = 0;  // enabled() 的动态化
};
```

- `verify()` 内的查表改为 `provider_->secret_for(ak)`，其余逻辑零改动；
- 现有 `build(AuthConfig)` 保留：内部把静态表包成一个纯内存 provider，
  单测与不启用凭证 API 的部署路径完全不受影响；
- `CredentialStore` 实现该接口，main 装配时注入。

### 5.3 请求流（生成为例）

```text
POST /-/admin/credentials?comment=ci
  → dispatch: 路径前缀 /-/admin/ → auth_.verify(req)（复用现有验签）
  → store.is_root(ak) 不通过 → AccessDenied
  → store.generate("ci")
      ├─ CSPRNG 生成 AK/SK（§6），内存查重，碰撞则重试（≤3 次）
      ├─ put_object(".sys", "credentials/{ak}", JSON)      // 先持久化
      └─ 写锁更新内存 map                                   // 后生效
  → 201 + JSON（含明文 SK）
```

dispatch 中 `/-/admin/` 分支插在现有匿名 `/-/` 端点之后、S3 寻址之前
（src/s3/service.cc 的 dispatch 已有该 if-else 链）。

### 5.4 nlohmann/json 依赖引入

- git 子模块：`third_party/json`（header-only，无编译产物）；
- CMake：`add_subdirectory(third_party/json EXCLUDE_FROM_ALL)` 后
  `target_link_libraries(lights3_core PRIVATE nlohmann_json::nlohmann_json)`，
  与 gflags/spdlog 的接入方式一致；
- build.sh 的常规子模块列表（`LIGHT_MODULES`）追加一项；
- 使用面收敛在 admin handler 与 `CredentialStore` 的序列化处，不向
  L1/L3/L4 头文件泄漏（`#include <nlohmann/json.hpp>` 只出现在 .cc）。

## 6. AK/SK 生成

- **AK**：`L3AK` 前缀 + 16 位 base32（`A-Z2-7`），共 20 字符——长度与
  字符集对齐 AWS 的 `AKIA…` 形态，任何按 AWS 规则做输入校验的客户端都
  能通过；前缀便于日志里一眼识别动态凭证；
- **SK**：30 随机字节 base64 编码为 40 字符，对齐 AWS SK 长度；
- 随机源统一走 `getentropy(2)`（CSPRNG，无种子管理问题）；**禁止**
  `std::mt19937/rand`；
- 唯一性：内存 map 查重即可（单进程写路径已串行）；碰撞概率 2^-80 量级，
  重试仅为防御。

## 7. 并发与一致性

- 读（验签查表）：`shared_mutex` 读锁，热路径无阻塞写竞争；
- 写（生成/吊销）：不持锁跨 `co_await`（协程可能在别的线程恢复，
  `std::mutex` 跨线程解锁是 UB）；改为"存储写完，写锁短暂更新内存"，
  并发 generate 的唯一性由 AK 随机空间（2^80）保证；
- 吊销语义：删除后**新请求**立即失效；已通过验签、尚在处理中的请求
  自然完成（与 AWS 的最终一致行为相同）；
- 多实例限制：多个网关实例共享同一后端时，实例间无失效/新增通知，
  各自的内存表只在启动时加载。本期明确不支持该拓扑（单进程假设，
  docs/01 的部署模型），二期可选方案：定期增量 reload 或管理面广播。

## 8. 测试计划

**单测**（tests/unit，沿用现有测试框架）

- CredentialStore：生成 → lookup 命中；吊销 → lookup 失效；memory 后端
  写入后新建 store 重新 load 能恢复（模拟重启）；静态/动态同 AK 冲突时
  静态优先；
- SigV4 集成：用 generate 出的凭证 sign（`SigV4Authenticator::sign` 现成）
  再 verify，全链路通过；
- 权限：动态凭证调 admin API → AccessDenied；认证关闭时 → AccessDenied；
- 边界：删静态凭证 405、查不存在的 AK、AK/SK 字符集与长度断言。

**e2e**（tests/e2e/run_e2e.sh 追加一节）

```text
root 凭证 POST 生成 → 解析响应 JSON 取出新 AK/SK（sed/grep 提取字段
  即可，不给 e2e 脚本引入 jq 依赖）
  → 用新凭证 PUT/GET object（curl --aws-sigv4）
  → GET 列表确认存在且 SK 掩码
  → DELETE 吊销 → 新凭证再请求 → 403 InvalidAccessKeyId
  → 重启 server → 生成的另一凭证仍可用（持久化验证，localfs 后端）
```

## 9. 分期

| 期 | 内容 |
| --- | --- |
| 一期 | 本方案全部：4 个 API、`.sys` 持久化、动态生效、两级权限、单测 + e2e |
| 二期 | SK at-rest 加密（master key）、外部 IdP/文件热加载 provider、多实例失效同步、per-credential policy |
