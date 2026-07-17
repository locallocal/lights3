#include "s3/auth/credential_store.h"

#include <unistd.h>

#include <atomic>
#include <nlohmann/json.hpp>
#include <stdexcept>

#include "core/log.h"
#include "core/util/time.h"
#include "http/model.h"
#include "s3/errors.h"

namespace lights3::s3 {

namespace {

using nlohmann::json;

// ---------- CSPRNG 生成（docs/06 §6）----------

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

// ---------- 落盘格式（docs/06 §4.2）----------

std::string object_key(std::string_view ak) {
    return std::string(kCredPrefix) + std::string(ak);
}

std::string serialize(const CredentialInfo& c) {
    json j;
    j["version"] = 1;
    j["sk"] = c.secret_key;
    j["created"] = util::iso8601(c.created);  // 给人看
    j["created_unix"] = std::chrono::duration_cast<std::chrono::seconds>(
                            c.created.time_since_epoch())
                            .count();  // 供解析（iso8601 无现成反解）
    j["comment"] = c.comment;
    return j.dump(2) + "\n";
}

// 失败返回 nullopt（损坏对象跳过不阻塞启动）
std::optional<CredentialInfo> deserialize(std::string_view ak, const std::string& body) {
    try {
        json j = json::parse(body);
        if (j.at("version").get<int>() != 1) return std::nullopt;
        CredentialInfo c;
        c.access_key = std::string(ak);
        c.secret_key = j.at("sk").get<std::string>();
        c.created = std::chrono::system_clock::time_point(
            std::chrono::seconds(j.value("created_unix", int64_t{0})));
        c.comment = j.value("comment", "");
        return c;
    } catch (const json::exception&) {
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

}  // namespace

// ---------- 加载（docs/06 §5.1）----------

Task<std::shared_ptr<CredentialStore>> CredentialStore::load(
    std::shared_ptr<storage::IStorageBackend> backend, const AuthConfig& static_cfg) {
    auto store = std::shared_ptr<CredentialStore>(new CredentialStore);
    store->backend_ = std::move(backend);

    // 动态凭证：.sys 不存在 = 从未生成过
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
                if (auto c = deserialize(ak, body)) {
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

    // 静态表：同 AK 时静态优先（docs/06 §5.1）
    for (auto& c : static_cfg.credentials) {
        CredentialInfo info;
        info.access_key = c.access_key;
        info.secret_key = c.secret_key;
        info.is_static = true;
        auto [it, inserted] = store->creds_.insert_or_assign(c.access_key, std::move(info));
        (void)it;
        if (!inserted) {
            LOG_WARN("credential {} exists in both config and storage: config wins",
                     c.access_key);
            --dynamic_count;
        }
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
    return it != creds_.end() && it->second.is_static;
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

Task<CredentialInfo> CredentialStore::generate(std::string comment) {
    CredentialInfo c;
    c.comment = std::move(comment);
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
    http::StringBodyReader body(serialize(c));
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
        if (it->second.is_static)
            throw S3Error(S3ErrorCode::MethodNotAllowed,
                          "Static credentials are managed via the config file.");
    }
    // 先删存储（幂等）再删内存；失败则内存保留，与存储一致
    co_await backend_->delete_object(kSysBucket, object_key(ak));
    {
        std::unique_lock lk(mu_);
        creds_.erase(std::string(ak));
    }
    LOG_INFO("revoked credential {}", ak);
}

}  // namespace lights3::s3
