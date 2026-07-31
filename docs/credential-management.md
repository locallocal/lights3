# 凭证管理：AK/SK 的生成、查询与持久化（方案）

> 状态：一期、二期均已实现（分期见 §9，二期设计见 §10）。承接 docs/s3-protocol.md §3.5 预留的 `ICredentialProvider` 扩展点。
> 代码：`src/s3/auth/credential_store.{h,cc}`、`src/s3/handlers/admin_credentials.cc`。

## 1. 目标与非目标

**目标**

- 运行期通过 API 生成 / 查询 / 吊销 AK/SK，无需改配置文件或重启进程；
- 生成的凭证经 `IStorageBackend` 持久化，进程重启后自动恢复；
- 验签路径保持现状的同步内存查表，不因动态凭证引入异步或明显开销；
- 配置文件静态凭证的行为完全不变（向后兼容）。

**非目标（一期不做；标注项已在二期补齐，见 §10）**

- IAM 式细粒度 policy——所有凭证仍等价于超级用户的数据面权限（见 §3 两级模型）。
  二期补了轻量的 per-credential policy（bucket 白名单 + readonly，§10.4），仍非 IAM；
- STS 临时凭证 / 凭证轮换到期；
- 多实例共享后端时的跨节点失效通知（限制见 §7）——二期以定期增量 reload 补齐（§10.3）。

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

未设置 master key 时 SK 以明文落盘（version=1），密级等同于配置文件里的静态
SK（同一台机器、同一套文件权限）。设置 `LIGHTS3_MASTER_KEY` 后落盘为
version=2 的 AES-256-GCM 加密格式，存量 v1 对象在启动时就地升级（§10.1）。

## 5. 组件与数据流

### 5.1 新增 `s3/auth/credential_store.{h,cc}`（L2）

```cpp
class CredentialStore final : public ICredentialProvider {
public:
    // 启动时全量加载：list(".sys", "credentials/") + 逐个 get，
    // 与静态表合并（同 AK 时静态优先并告警）。二期起收整个 AuthConfig
    //（含 credentials_file / credentials_file_reload / sync_interval 配置）
    static Task<std::shared_ptr<CredentialStore>> load(
        std::shared_ptr<storage::IStorageBackend> backend, const AuthConfig& cfg);

    // 验签热路径：同步内存查表（shared_mutex 读锁）
    std::optional<std::string> secret_for(std::string_view ak) const override;
    bool has_credentials() const override;
    bool is_root(std::string_view ak) const;   // 静态来源 → root

    // 管理面：先写 storage 成功再改内存（write-through，崩溃时以 storage
    // 为准，内存顶多"少"不会"多"）。注意不持锁 co_await（协程可能换线程
    // 恢复，std::mutex 跨线程解锁是 UB），并发唯一性靠 AK 随机空间保证
    // policy 为二期扩展（§10.4），缺省无 policy
    Task<CredentialInfo> generate(std::string comment,
                                  std::optional<CredentialPolicy> policy = std::nullopt);
    Task<void>           remove(std::string_view ak);
    std::optional<CredentialInfo> find(std::string_view ak) const;
    std::vector<CredentialInfo>   list() const;
};
```

### 5.2 `SigV4Authenticator` 改造

把私有的 `std::map creds_` 换成 docs/s3-protocol.md §3.5 预留的接口：

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
  各自的内存表默认只在启动时加载。二期提供 `auth.sync_interval` 定期增量
  reload（§10.3）；不开启时仍是单进程假设（docs/architecture.md 的部署模型）。

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

| 期 | 内容 | 状态 |
| --- | --- | --- |
| 一期 | 本方案全部：4 个 API、`.sys` 持久化、动态生效、两级权限、单测 + e2e | 已实现 |
| 二期 | SK at-rest 加密（master key）、文件热加载 provider、多实例失效同步、per-credential policy（设计见 §10） | 已实现 |

## 10. 二期设计

一期后的凭证来源从两级扩展为三来源：

```text
静态凭证（config auth.credentials）        = root：数据面 + admin API
文件凭证（auth.credentials_file，热加载）  = 普通：仅数据面，可带 policy
动态凭证（API 生成，storage 持久化）       = 普通：仅数据面，可带 policy
```

同 AK 冲突优先级 static > file > dynamic，均有启动/加载告警。
`CredentialInfo::source` 三值枚举替代一期的 `is_static` 布尔（`is_static()`
仍保留为基于 source 的便捷方法）；admin API 的
`source` 字段相应多出 `"file"`。

### 10.1 SK at-rest 加密

- 开关即环境变量 `LIGHTS3_MASTER_KEY`（64 个 hex 字符 = 32 字节，
  `openssl rand -hex 32` 生成）；不进配置文件，避免与被加密物同处一份文件；
- 算法 AES-256-GCM（OpenSSL EVP，封装在 `core/util/crypto.h`：
  `aes256gcm_seal/open`，布局 `12B nonce || ciphertext || 16B tag`）；
- 落盘对象 `version: 2`，`sk` 字段换成 `sk_enc`（seal 输出的 hex）；
- 兼容与升级：load 同时接受 v1/v2；设置 master key 后启动时把存量 v1
  对象**就地重写为 v2**（对象个数有限，一次性代价可忽略）；
- fail-fast：遇到 v2 对象而 key 未设置 / key 不对（GCM tag 校验失败）时
  **启动报错**而非跳过——静默丢凭证会把用户锁在门外还无从排查；JSON 损坏
  仍是跳过 + 告警（与一期一致）。运行期 sync（§10.3）里同类错误降级为告警，
  不让单个坏对象打断同步；
- 无降级路径：想撤掉 master key，先用 `?show-secret=true` 导出再重建凭证。

### 10.2 外部凭证文件热加载 provider

面向"凭证由外部系统（IdP/配置管理）生成下发"的场景：lights3 只消费文件，
不做协议对接——外部系统负责把凭证渲染成 JSON 文件放到指定路径。

- 配置 `auth.credentials_file`，格式：
  `{"credentials": [{"access_key", "secret_key", "comment"?, "policy"?}]}`；
- 热加载：`auth.credentials_file_reload`（默认 30s，0s = 仅启动时加载）周期
  轮询 mtime，变更即整表替换 file 来源的条目（删掉的凭证随之失效）。选
  mtime 轮询而非 inotify：跨文件系统可靠、代码量小，30s 级延迟对凭证下发
  完全够用；
- 启动时解析失败 fail-fast（配置错误）；运行期 reload 失败告警并**保留旧表**
  （宁可旧凭证多活一轮，不可解析错误清空全表）；
- 文件凭证仅数据面（不能调 admin API），也不能经 admin API 吊销（405，
  归文件管——从文件里删掉即吊销）。

### 10.3 多实例失效同步（定期增量 reload）

补齐 §7 的多实例限制有两个候选：管理面广播（实例间互通失效/新增事件）
与定期增量 reload。选**定期增量 reload**：无须新增管理面广播通道与成员
发现，代价是失效延迟一个周期（凭证下发/吊销本就是分钟级运维操作）。

- 配置 `auth.sync_interval`（默认 0s = 关闭，单实例部署零开销）；
- 每轮：先采内存中动态凭证的 AK 快照，再 list `.sys/credentials/`——
  storage 有而内存无的拉取入表（新增），快照有而 storage 无的移除（吊销）。
  快照**必须先于 list**：write-through 保证快照里的凭证当时已持久化，
  "快照有 + list 无"只能是别处吊销；list 期间本实例新生成的凭证不在快照里，
  不会被误删；
- 已存在的 AK 不重拉：SK 与 policy 在凭证生命周期内不可变（无 update API），
  增量只有增删两种；
- 定时器模式与 duostore GC 相同（`BackgroundTaskGroup` + `TimerQueue`，
  完成后重臂不重叠），tick 先 `pool_->schedule()` 挪到池线程再做 IO。

### 10.4 per-credential policy

刻意保持在"够用"档，不引入 IAM 的 statement/effect/action 语法：

```json
{ "policy": { "buckets": ["logs-*", "backup"], "readonly": true } }
```

- `buckets`：bucket glob 白名单（fnmatch，同 bucket 路由规则）；空/缺省 = 全部；
- `readonly`：true 时仅允许 GET/HEAD；
- 携带方式：`POST /-/admin/credentials` 的 JSON body
  `{"comment"?, "policy"?}`（`?comment=` 查询参数仍兼容，body 优先），或
  credentials_file 条目的 `policy` 字段；创建后不可改（无 update API，重建即可）；
- 执行点：dispatch 验签通过、bucket 解析后统一 `authorize(ak, bucket, is_write)`
  （src/s3/service.cc），非 GET/HEAD 即视为写。静态凭证与无 policy 凭证恒通过；
  拒绝为 `AccessDenied`(403) 且先于 NoSuchBucket 等数据面错误暴露；
- 校验从严：POST body / policy 出现未知字段直接 `InvalidRequest`——拼错的
  限制字段被静默忽略等于放权；
- 已知取舍：`ListBuckets` 对 policy 凭证放行且**不过滤**结果（只泄露桶名，
  过滤需把 AK 下探到 handler，不值得）；吊销/policy 均不影响已通过验签的
  在途请求（§7 语义）。
