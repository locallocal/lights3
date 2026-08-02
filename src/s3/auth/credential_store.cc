#include "s3/auth/credential_store.h"

#include <fnmatch.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdlib>
#include <fstream>
#include <nlohmann/json.hpp>
#include <set>
#include <sstream>
#include <stdexcept>

#include "core/log.h"
#include "core/util/hex.h"
#include "core/util/time.h"
#include "http/model.h"
#include "s3/errors.h"

namespace lights3::s3 {

namespace {

using nlohmann::json;

// ---------- CSPRNG 生成（docs/credential-management.md §6）----------

void fill_random(uint8_t* buf, size_t n) {
    // getentropy 单次上限 256 字节，这里最多 30 字节
    if (::getentropy(buf, n) != 0)
        throw std::runtime_error("getentropy failed: cannot generate credentials");
}

// AK：L3AK + 16 位 base32（A-Z2-7），共 20 字符，对齐 AWS AKIA… 形态
std::string random_access_key() {
    static constexpr char kBase32[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    uint8_t raw[16];
    fill_random(raw, sizeof(raw));
    std::string ak = "L3AK";
    for (uint8_t b : raw) ak.push_back(kBase32[b % 32]);
    return ak;
}

// SK：30 随机字节 base64 → 40 字符，对齐 AWS SK 长度
std::string random_secret_key() {
    static constexpr char kBase64[] =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    uint8_t raw[30];
    fill_random(raw, sizeof(raw));
    std::string sk;
    for (size_t i = 0; i < sizeof(raw); i += 3) {
        uint32_t v = (raw[i] << 16) | (raw[i + 1] << 8) | raw[i + 2];
        sk.push_back(kBase64[(v >> 18) & 63]);
        sk.push_back(kBase64[(v >> 12) & 63]);
        sk.push_back(kBase64[(v >> 6) & 63]);
        sk.push_back(kBase64[v & 63]);
    }
    return sk;
}

// ---------- policy JSON 约定（docs/credential-management.md §10.4）----------

CredentialPolicy policy_from_json_obj(const json& j) {
    if (!j.is_object())
        throw S3Error(S3ErrorCode::InvalidRequest, "policy must be a JSON object.");
    CredentialPolicy p;
    for (auto& [k, v] : j.items()) {
        if (k == "buckets") {
            if (!v.is_array())
                throw S3Error(S3ErrorCode::InvalidRequest,
                              "policy.buckets must be an array of bucket glob strings.");
            for (auto& g : v) {
                if (!g.is_string() || g.get<std::string>().empty())
                    throw S3Error(S3ErrorCode::InvalidRequest,
                                  "policy.buckets entries must be non-empty strings.");
                p.buckets.push_back(g.get<std::string>());
            }
        } else if (k == "readonly") {
            if (!v.is_boolean())
                throw S3Error(S3ErrorCode::InvalidRequest, "policy.readonly must be a boolean.");
            p.readonly = v.get<bool>();
        } else {
            // 未知字段严格拒绝：静默忽略拼错的限制字段等于放权
            throw S3Error(S3ErrorCode::InvalidRequest, "unknown policy field '" + k + "'.");
        }
    }
    return p;
}

json policy_to_json_obj(const CredentialPolicy& p) {
    json j = json::object();
    if (!p.buckets.empty()) j["buckets"] = p.buckets;
    if (p.readonly) j["readonly"] = true;
    return j;
}

// ---------- 落盘格式（docs/credential-management.md §4.2 / §10.1）----------

std::string object_key(std::string_view ak) {
    return std::string(kCredPrefix) + std::string(ak);
}

std::string serialize(const CredentialInfo& c, const std::optional<util::Aes256Key>& key) {
    json j;
    if (key) {
        j["version"] = 2;
        std::string sealed = util::aes256gcm_seal(*key, c.secret_key);
        j["sk_enc"] = util::to_hex(
            std::span(reinterpret_cast<const uint8_t*>(sealed.data()), sealed.size()));
    } else {
        j["version"] = 1;
        j["sk"] = c.secret_key;
    }
    j["created"] = util::iso8601(c.created);  // 给人看
    j["created_unix"] = std::chrono::duration_cast<std::chrono::seconds>(
                            c.created.time_since_epoch())
                            .count();  // 供解析（iso8601 无现成反解）
    j["comment"] = c.comment;
    if (c.policy) j["policy"] = policy_to_json_obj(*c.policy);
    return j.dump(2) + "\n";
}

// JSON 损坏返回 nullopt（跳过不阻塞启动）；version=2 而无 key / 解密失败抛
// runtime_error——那是配置错误，静默丢凭证会把用户锁在门外，宁可拦下启动。
// was_plaintext: 对象仍为 v1 明文（load 时据此做 v1→v2 升级）
std::optional<CredentialInfo> deserialize(std::string_view ak, const std::string& body,
                                          const std::optional<util::Aes256Key>& key,
                                          bool* was_plaintext = nullptr) {
    try {
        json j = json::parse(body);
        int ver = j.at("version").get<int>();
        CredentialInfo c;
        c.access_key = std::string(ak);
        c.source = CredSource::kDynamic;
        if (ver == 1) {
            c.secret_key = j.at("sk").get<std::string>();
            if (was_plaintext) *was_plaintext = true;
        } else if (ver == 2) {
            if (!key)
                throw std::runtime_error(
                    "credential object " + std::string(ak) +
                    " is encrypted (version 2) but " + kMasterKeyEnv + " is not set");
            auto blob = util::from_hex(j.at("sk_enc").get<std::string>());
            auto sk = util::aes256gcm_open(
                *key, std::string_view(reinterpret_cast<const char*>(blob.data()),
                                       blob.size()));
            if (!sk)
                throw std::runtime_error("credential object " + std::string(ak) +
                                         ": decryption failed (wrong " + kMasterKeyEnv +
                                         " or corrupted object)");
            c.secret_key = std::move(*sk);
        } else {
            return std::nullopt;
        }
        c.created = std::chrono::system_clock::time_point(
            std::chrono::seconds(j.value("created_unix", int64_t{0})));
        c.comment = j.value("comment", "");
        if (j.contains("policy")) c.policy = policy_from_json_obj(j.at("policy"));
        return c;
    } catch (const json::exception&) {
        return std::nullopt;
    } catch (const S3Error&) {  // policy 字段损坏 = 对象损坏
        return std::nullopt;
    }
}

Task<std::string> read_all(http::BodyReader& body, size_t max_size = 64 * 1024) {
    std::string out;
    std::byte buf[16 * 1024];
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        if (out.size() + n > max_size)
            throw std::runtime_error("credential object exceeds size limit");
        out.append(reinterpret_cast<const char*>(buf), n);
    }
    co_return out;
}

// ---------- credentials_file 解析（docs/credential-management.md §10.2）----------
// {"credentials": [{"access_key","secret_key","comment"?,"policy"?}]}
// 失败抛 runtime_error（启动时 fail-fast；热加载时由调用方告警并保留旧表）

std::vector<CredentialInfo> parse_credentials_file_text(const std::string& text,
                                                        const std::string& path) {
    std::vector<CredentialInfo> out;
    try {
        json j = json::parse(text);
        for (auto& e : j.at("credentials")) {
            CredentialInfo c;
            c.access_key = e.at("access_key").get<std::string>();
            c.secret_key = e.at("secret_key").get<std::string>();
            if (c.access_key.empty() || c.secret_key.empty())
                throw std::runtime_error("access_key/secret_key must be non-empty");
            c.source = CredSource::kFile;
            c.comment = e.value("comment", "");
            if (e.contains("policy")) c.policy = policy_from_json_obj(e.at("policy"));
            out.push_back(std::move(c));
        }
    } catch (const json::exception& ex) {
        throw std::runtime_error("credentials file " + path + ": " + ex.what());
    } catch (const S3Error& ex) {
        throw std::runtime_error("credentials file " + path + ": " + ex.message);
    }
    return out;
}

std::string read_file_text(const std::string& path) {
    std::ifstream in(path, std::ios::binary);
    if (!in) throw std::runtime_error("cannot open credentials file " + path);
    std::ostringstream ss;
    ss << in.rdbuf();
    return ss.str();
}

}  // namespace

// ---------- policy ----------

bool CredentialPolicy::allows(std::string_view bucket, bool is_write) const {
    if (readonly && is_write) return false;
    // bucket 为空 = 账户级操作（ListBuckets）：放行。注意 ListBuckets 结果不按
    // policy 过滤（只见桶名，见 docs/credential-management.md §10.4 的取舍）
    if (buckets.empty() || bucket.empty()) return true;
    std::string b(bucket);
    for (auto& g : buckets)
        if (::fnmatch(g.c_str(), b.c_str(), 0) == 0) return true;
    return false;
}

CredentialPolicy parse_policy_json(const std::string& text) {
    try {
        return policy_from_json_obj(json::parse(text));
    } catch (const json::exception&) {
        throw S3Error(S3ErrorCode::InvalidRequest, "policy is not valid JSON.");
    }
}

std::string policy_to_json(const CredentialPolicy& p) { return policy_to_json_obj(p).dump(); }

// ---------- 加载（docs/credential-management.md §5.1 / §10）----------

Task<std::shared_ptr<CredentialStore>> CredentialStore::load(
    std::shared_ptr<storage::IStorageBackend> backend, const AuthConfig& cfg) {
    auto store = std::shared_ptr<CredentialStore>(new CredentialStore);
    store->backend_ = std::move(backend);
    store->cfg_ = cfg;

    // master key（§10.1）：64 hex → 32 字节；格式错误直接 fail-fast
    if (const char* env = std::getenv(kMasterKeyEnv); env && *env) {
        auto bytes = util::from_hex(env);
        if (bytes.size() != sizeof(util::Aes256Key))
            throw std::runtime_error(std::string(kMasterKeyEnv) +
                                     " must be 64 hex chars (32 bytes), e.g. from"
                                     " `openssl rand -hex 32`");
        util::Aes256Key k;
        std::copy(bytes.begin(), bytes.end(), k.begin());
        store->master_key_ = k;
    }

    // 动态凭证：.sys 不存在 = 从未生成过
    std::vector<std::string> plaintext_aks;  // v1 对象，load 后升级为 v2（§10.1）
    if (co_await store->backend_->bucket_exists(kSysBucket)) {
        store->sys_bucket_ready_ = true;
        storage::ListOptions opt;
        opt.prefix = std::string(kCredPrefix);
        for (;;) {
            auto page = co_await store->backend_->list_objects(kSysBucket, opt);
            for (auto& obj : page.objects) {
                std::string_view ak(obj.key);
                ak.remove_prefix(kCredPrefix.size());
                auto stream = co_await store->backend_->get_object(kSysBucket, obj.key,
                                                                  std::nullopt);
                auto body = co_await read_all(*stream.body);
                bool was_plaintext = false;
                if (auto c = deserialize(ak, body, store->master_key_, &was_plaintext)) {
                    if (was_plaintext && store->master_key_)
                        plaintext_aks.push_back(c->access_key);
                    store->creds_.emplace(c->access_key, std::move(*c));
                } else {
                    LOG_WARN("skipping malformed credential object {}/{}", kSysBucket, obj.key);
                }
            }
            if (!page.is_truncated) break;
            opt.start_after = page.next_token;
        }
    }
    size_t dynamic_count = store->creds_.size();

    // v1 → v2 升级：设置了 master key 后，存量明文对象就地重写为加密格式
    for (auto& ak : plaintext_aks) {
        auto it = store->creds_.find(ak);
        storage::ObjectMeta meta;
        meta.content_type = "application/json";
        http::StringBodyReader body(serialize(it->second, store->master_key_));
        co_await store->backend_->put_object(kSysBucket, object_key(ak), std::move(meta),
                                             body);
    }
    if (!plaintext_aks.empty())
        LOG_INFO("re-encrypted {} plaintext credential object(s) with {}",
                 plaintext_aks.size(), kMasterKeyEnv);

    // 外部凭证文件（§10.2）：启动时解析失败 fail-fast（热加载失败才容忍保留旧表）
    if (!cfg.credentials_file.empty()) {
        std::error_code ec;
        auto mtime = std::filesystem::last_write_time(cfg.credentials_file, ec);
        if (ec)
            throw std::runtime_error("cannot stat credentials file " +
                                     cfg.credentials_file + ": " + ec.message());
        auto file_creds = parse_credentials_file_text(
            read_file_text(cfg.credentials_file), cfg.credentials_file);
        size_t n = file_creds.size();
        store->apply_file_credentials(std::move(file_creds));
        store->file_mtime_ = mtime;
        LOG_INFO("loaded {} credential(s) from {}", n, cfg.credentials_file);
    }

    // 静态表：同 AK 时静态优先（docs/credential-management.md §5.1）
    for (auto& c : cfg.credentials) {
        CredentialInfo info;
        info.access_key = c.access_key;
        info.secret_key = c.secret_key;
        info.source = CredSource::kStatic;
        auto [it, inserted] = store->creds_.insert_or_assign(c.access_key, std::move(info));
        (void)it;
        if (!inserted)
            LOG_WARN("credential {} exists in both config and storage/file: config wins",
                     c.access_key);
    }
    if (dynamic_count > 0)
        LOG_INFO("loaded {} dynamic credential(s) from {}", dynamic_count, kSysBucket);
    co_return store;
}

// ---------- 查表（验签热路径）----------

std::optional<std::string> CredentialStore::secret_for(std::string_view ak) const {
    std::shared_lock lk(mu_);
    auto it = creds_.find(ak);
    if (it == creds_.end()) return std::nullopt;
    return it->second.secret_key;
}

bool CredentialStore::has_credentials() const {
    std::shared_lock lk(mu_);
    return !creds_.empty();
}

bool CredentialStore::is_root(std::string_view ak) const {
    std::shared_lock lk(mu_);
    auto it = creds_.find(ak);
    return it != creds_.end() && it->second.is_static();
}

void CredentialStore::authorize(std::string_view ak, std::string_view bucket,
                                bool is_write) const {
    if (ak.empty()) return;  // 认证关闭
    std::shared_lock lk(mu_);
    auto it = creds_.find(ak);
    if (it == creds_.end()) return;  // 在途吊销竞态：已验签请求自然完成（§7）
    if (!it->second.policy) return;
    if (!it->second.policy->allows(bucket, is_write))
        throw S3Error(S3ErrorCode::AccessDenied,
                      "Access denied by credential policy.");
}

std::optional<CredentialInfo> CredentialStore::find(std::string_view ak) const {
    std::shared_lock lk(mu_);
    auto it = creds_.find(ak);
    if (it == creds_.end()) return std::nullopt;
    return it->second;
}

std::vector<CredentialInfo> CredentialStore::list() const {
    std::shared_lock lk(mu_);
    std::vector<CredentialInfo> out;
    out.reserve(creds_.size());
    for (auto& [_, c] : creds_) out.push_back(c);
    return out;  // map 本身按 AK 有序
}

// ---------- 管理面 ----------

Task<CredentialInfo> CredentialStore::generate(std::string comment,
                                               std::optional<CredentialPolicy> policy) {
    CredentialInfo c;
    c.comment = std::move(comment);
    c.policy = std::move(policy);
    c.created = std::chrono::system_clock::now();
    for (int attempt = 0;; ++attempt) {
        c.access_key = random_access_key();
        std::shared_lock lk(mu_);
        if (!creds_.contains(c.access_key)) break;
        if (attempt >= 3)
            throw S3Error(S3ErrorCode::InternalError,
                          "failed to generate a unique access key");
    }
    c.secret_key = random_secret_key();

    // 惰性建桶（幂等；已存在的并发竞态吞掉 AlreadyOwned 即可）
    if (!sys_bucket_ready_) {
        try {
            co_await backend_->create_bucket(kSysBucket);
        } catch (const S3Error& e) {
            if (e.code != S3ErrorCode::BucketAlreadyOwnedByYou) throw;
        }
        sys_bucket_ready_ = true;
    }

    // 先持久化后生效（write-through）
    storage::ObjectMeta meta;
    meta.content_type = "application/json";
    http::StringBodyReader body(serialize(c, master_key_));
    co_await backend_->put_object(kSysBucket, object_key(c.access_key), std::move(meta), body);

    {
        std::unique_lock lk(mu_);
        creds_[c.access_key] = c;
    }
    LOG_INFO("generated credential {} ({})", c.access_key,
             c.comment.empty() ? "no comment" : c.comment);
    co_return c;
}

Task<void> CredentialStore::remove(std::string_view ak) {
    {
        std::shared_lock lk(mu_);
        auto it = creds_.find(ak);
        if (it == creds_.end())
            throw S3Error(S3ErrorCode::InvalidAccessKeyId,
                          "The specified access key does not exist.");
        if (it->second.source == CredSource::kStatic)
            throw S3Error(S3ErrorCode::MethodNotAllowed,
                          "Static credentials are managed via the config file.");
        if (it->second.source == CredSource::kFile)
            throw S3Error(S3ErrorCode::MethodNotAllowed,
                          "File-sourced credentials are managed via the credentials file.");
    }
    // tombstone 先于 delete 落表：与 sync_now 的 list 交错时（list 早于 delete 生效、
    // emplace 晚于下面的 erase），新增分支据此拒绝把刚吊销的 AK 拉回内存
    {
        std::unique_lock lk(mu_);
        tombstones_[std::string(ak)] = std::chrono::steady_clock::now();
    }
    // 先删存储（幂等）再删内存；失败则内存保留，与存储一致
    try {
        co_await backend_->delete_object(kSysBucket, object_key(ak));
    } catch (...) {
        std::unique_lock lk(mu_);
        tombstones_.erase(std::string(ak));  // 未吊销成功，不得挡住后续 sync
        throw;
    }
    {
        std::unique_lock lk(mu_);
        creds_.erase(std::string(ak));
    }
    LOG_INFO("revoked credential {}", ak);
}

// ---------- 文件热加载（§10.2）----------

void CredentialStore::apply_file_credentials(std::vector<CredentialInfo> creds) {
    std::unique_lock lk(mu_);
    // fail-open 防护（README §1.2）：拒绝把非空表清成空表——文件被误编辑成
    // `{"credentials": []}` 是合法 JSON，照单全收会让 enabled() 依赖的表变空。
    // 保留旧表、置 degraded（readyz 转 503），文件修好后下一轮恢复
    if (!creds_.empty() && creds.empty() &&
        std::all_of(creds_.begin(), creds_.end(),
                    [](auto& kv) { return kv.second.source == CredSource::kFile; })) {
        LOG_ERROR("credentials file would empty the credential table; keeping previous "
                  "table (authentication stays enabled)");
        degraded_.store(true, std::memory_order_relaxed);
        return;
    }
    degraded_.store(false, std::memory_order_relaxed);
    // 整体替换 file 来源的条目：旧文件里删掉的凭证随之失效
    for (auto it = creds_.begin(); it != creds_.end();)
        it = (it->second.source == CredSource::kFile) ? creds_.erase(it) : std::next(it);
    for (auto& c : creds) {
        auto it = creds_.find(c.access_key);
        if (it != creds_.end()) {
            if (it->second.source == CredSource::kStatic) {
                LOG_WARN("credential {} exists in both config and credentials file: "
                         "config wins", c.access_key);
                continue;
            }
            LOG_WARN("credential {} exists in both credentials file and storage: "
                     "file wins", c.access_key);
        }
        creds_.insert_or_assign(c.access_key, std::move(c));
    }
}

void CredentialStore::reload_file_now() {
    if (cfg_.credentials_file.empty()) return;
    std::error_code ec;
    auto mtime = std::filesystem::last_write_time(cfg_.credentials_file, ec);
    if (ec) {
        // 文件暂时不可见（编辑器原子替换的中间态等）：保留旧表下轮再试
        LOG_WARN("credentials file {} not readable ({}): keeping previous table",
                 cfg_.credentials_file, ec.message());
        return;
    }
    try {
        auto creds = parse_credentials_file_text(read_file_text(cfg_.credentials_file),
                                                 cfg_.credentials_file);
        size_t n = creds.size();
        apply_file_credentials(std::move(creds));
        file_mtime_ = mtime;
        LOG_INFO("reloaded {} credential(s) from {}", n, cfg_.credentials_file);
    } catch (const std::exception& e) {
        // 热加载失败保留旧表：宁可旧凭证多活一轮，不可解析错误清空全表
        LOG_ERROR("credentials file reload failed: {}", e.what());
    }
}

// ---------- 多实例增量同步（§10.3）----------

Task<void> CredentialStore::sync_now() {
    // 内存快照须在 list 之前采集：write-through 保证快照里的动态凭证在快照时刻
    // 已持久化，因此"快照有 + list 无"只能是别处吊销；list 期间/之后本实例新生成
    // 的凭证不在快照里，不会被误删
    std::vector<std::string> snapshot;
    {
        std::shared_lock lk(mu_);
        for (auto& [ak, c] : creds_)
            if (c.source == CredSource::kDynamic) snapshot.push_back(ak);
    }

    // 存储上现存的动态凭证 AK 全集
    std::set<std::string, std::less<>> on_storage;
    if (co_await backend_->bucket_exists(kSysBucket)) {
        storage::ListOptions opt;
        opt.prefix = std::string(kCredPrefix);
        for (;;) {
            auto page = co_await backend_->list_objects(kSysBucket, opt);
            for (auto& obj : page.objects)
                on_storage.insert(obj.key.substr(kCredPrefix.size()));
            if (!page.is_truncated) break;
            opt.start_after = page.next_token;
        }
    }
    size_t added = 0, removed = 0;

    // tombstone 清理与快照：过期条目剔除；近期吊销的 AK 在新增分支跳过
    //（remove 与本轮 list 交错时对象可能仍被 list 到，不加防护会复活已吊销凭证）
    const auto now = std::chrono::steady_clock::now();
    const auto ttl = std::chrono::seconds(std::max(60, 2 * cfg_.sync_interval_sec));
    std::set<std::string, std::less<>> recently_revoked;
    {
        std::unique_lock lk(mu_);
        for (auto it = tombstones_.begin(); it != tombstones_.end();) {
            if (now - it->second > ttl) {
                it = tombstones_.erase(it);
            } else {
                recently_revoked.insert(it->first);
                ++it;
            }
        }
    }

    // 新增：storage 有、内存无 → 拉取入表
    for (auto& ak : on_storage) {
        if (find(ak)) continue;
        if (recently_revoked.contains(ak)) continue;
        try {
            auto stream = co_await backend_->get_object(kSysBucket, object_key(ak),
                                                        std::nullopt);
            auto body = co_await read_all(*stream.body);
            auto c = deserialize(ak, body, master_key_);
            if (!c) {
                LOG_WARN("sync: skipping malformed credential object {}/{}{}", kSysBucket,
                         kCredPrefix, ak);
                continue;
            }
            std::unique_lock lk(mu_);
            if (creds_.emplace(c->access_key, std::move(*c)).second) ++added;
        } catch (const std::exception& e) {
            // 运行期不因单个对象拉取失败中断同步（与启动 fail-fast 不同）
            LOG_WARN("sync: failed to load credential {}: {}", ak, e.what());
        }
    }

    // 消失：快照里的动态凭证不在 storage 上 → 别处已吊销，本地失效。
    // fail-open 防护（README §1.2）：.sys 被外部整体清空时不把表清成空表
    for (auto& ak : snapshot) {
        if (on_storage.contains(ak)) continue;
        std::unique_lock lk(mu_);
        auto it = creds_.find(ak);
        if (it != creds_.end() && it->second.source == CredSource::kDynamic) {
            if (creds_.size() == 1) {
                LOG_ERROR("credential sync would empty the credential table (\"{}\" gone "
                          "from storage); keeping it (authentication stays enabled)", ak);
                degraded_.store(true, std::memory_order_relaxed);
                break;
            }
            creds_.erase(it);
            ++removed;
        }
    }
    if (added || removed)
        LOG_INFO("credential sync: {} added, {} revoked", added, removed);
}

// ---------- 后台任务装配（§10.2/§10.3；模式同 duostore GC：完成后重臂不重叠）----------

Task<void> CredentialStore::file_tick() {
    co_await pool_->schedule();  // 定时器线程只做派发，文件 IO 挪到池线程
    std::exception_ptr err;
    try {
        std::error_code ec;
        auto mtime = std::filesystem::last_write_time(cfg_.credentials_file, ec);
        if (!ec && mtime != file_mtime_) reload_file_now();
    } catch (...) {
        err = std::current_exception();
    }
    schedule_file_reload();
    if (err) std::rethrow_exception(err);  // 交 BackgroundTaskGroup 记日志
}

Task<void> CredentialStore::sync_tick() {
    co_await pool_->schedule();
    std::exception_ptr err;
    try {
        co_await sync_now();
    } catch (...) {
        err = std::current_exception();
    }
    schedule_sync();
    if (err) std::rethrow_exception(err);
}

void CredentialStore::schedule_file_reload() {
    if (cfg_.credentials_file.empty() || cfg_.credentials_file_reload_sec <= 0) return;
    bg_.if_open([&] {
        file_timer_ = TimerQueue::instance().add(
            std::chrono::seconds(cfg_.credentials_file_reload_sec),
            [this] { bg_.spawn(file_tick()); });
    });
}

void CredentialStore::schedule_sync() {
    if (cfg_.sync_interval_sec <= 0) return;
    bg_.if_open([&] {
        sync_timer_ = TimerQueue::instance().add(std::chrono::seconds(cfg_.sync_interval_sec),
                                                 [this] { bg_.spawn(sync_tick()); });
    });
}

void CredentialStore::start_background(std::shared_ptr<ThreadPool> pool) {
    pool_ = std::move(pool);
    schedule_file_reload();
    schedule_sync();
}

void CredentialStore::shutdown_background() {
    bg_.begin_close();
    // cancel 须在组锁外调用（TimerQueue::cancel 阻塞等在途回调，回调内要拿组锁）
    TimerQueue::instance().cancel(file_timer_);
    TimerQueue::instance().cancel(sync_timer_);
    bg_.wait_idle();
}

}  // namespace lights3::s3
