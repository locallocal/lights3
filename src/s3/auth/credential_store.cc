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
#include "core/util/checksum.h"
#include "core/util/hex.h"
#include "core/util/time.h"
#include "http/model.h"
#include "s3/errors.h"

namespace lights3::s3 {

namespace {

using nlohmann::json;

// ---------- CSPRNG generation (docs/credential-management.md §6) ----------

void fill_random(uint8_t* buf, size_t n) {
    // getentropy caps a single call at 256 bytes; at most 30 bytes here
    if (::getentropy(buf, n) != 0)
        throw std::runtime_error("getentropy failed: cannot generate credentials");
}

// AK: L3AK + 16 base32 chars (A-Z2-7), 20 characters total, aligned with the AWS AKIA... shape
std::string random_access_key() {
    static constexpr char kBase32[] = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";
    uint8_t raw[16];
    fill_random(raw, sizeof(raw));
    std::string ak = "L3AK";
    for (uint8_t b : raw) ak.push_back(kBase32[b % 32]);
    return ak;
}

// SK: 30 random bytes base64 -> 40 characters, aligned with the AWS SK length
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

// ---------- policy JSON conventions (docs/credential-management.md §10.4) ----------

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
        } else if (k == "prefixes") {
            // Key prefix allowlist (docs/archive/gaps.md §5.10): without it, multi-tenant shared buckets degrade into
            // "one bucket per tenant"
            if (!v.is_array())
                throw S3Error(S3ErrorCode::InvalidRequest,
                              "policy.prefixes must be an array of key prefix strings.");
            for (auto& g : v) {
                if (!g.is_string() || g.get<std::string>().empty())
                    throw S3Error(S3ErrorCode::InvalidRequest,
                                  "policy.prefixes entries must be non-empty strings.");
                p.prefixes.push_back(g.get<std::string>());
            }
        } else if (k == "actions") {
            if (!v.is_array())
                throw S3Error(S3ErrorCode::InvalidRequest,
                              "policy.actions must be an array of action names.");
            for (auto& a : v) {
                if (!a.is_string())
                    throw S3Error(S3ErrorCode::InvalidRequest,
                                  "policy.actions entries must be strings.");
                auto act = action_from_name(a.get<std::string>());
                if (!act)
                    throw S3Error(S3ErrorCode::InvalidRequest,
                                  "unknown policy action '" + a.get<std::string>() +
                                      "' (expected read/write/delete).");
                p.actions.push_back(*act);
            }
            if (p.actions.empty())
                throw S3Error(S3ErrorCode::InvalidRequest,
                              "policy.actions must not be empty (omit the field for no limit).");
        } else if (k == "readonly") {
            if (!v.is_boolean())
                throw S3Error(S3ErrorCode::InvalidRequest, "policy.readonly must be a boolean.");
            p.readonly = v.get<bool>();
        } else {
            // Unknown fields strictly rejected: silently ignoring a misspelled restriction field grants access
            throw S3Error(S3ErrorCode::InvalidRequest, "unknown policy field '" + k + "'.");
        }
    }
    return p;
}

json policy_to_json_obj(const CredentialPolicy& p) {
    json j = json::object();
    if (!p.buckets.empty()) j["buckets"] = p.buckets;
    if (!p.prefixes.empty()) j["prefixes"] = p.prefixes;
    if (p.readonly) j["readonly"] = true;
    if (!p.actions.empty()) {
        json a = json::array();
        for (auto act : p.actions) a.push_back(action_name(act));
        j["actions"] = std::move(a);
    }
    return j;
}

// ---------- On-disk format (docs/credential-management.md §4.2 / §10.1) ----------

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
        // v1 is the plaintext form without a master key; the SK has to hit disk anyway, and this json buffer cannot be wiped
        j["sk"] = static_cast<const std::string&>(c.secret_key);
    }
    j["created"] = util::iso8601(c.created);  // for humans
    j["created_unix"] = std::chrono::duration_cast<std::chrono::seconds>(
                            c.created.time_since_epoch())
                            .count();  // for parsing (no ready-made inverse for iso8601)
    j["comment"] = c.comment;
    j["rev"] = c.rev;  // monotonic edit counter (roadmap §2.5), independent of "version"
    if (c.policy) j["policy"] = policy_to_json_obj(*c.policy);
    if (!c.tenant.empty()) {
        j["tenant"] = c.tenant;
        j["role"] = c.tenant_admin ? "admin" : "user";
    }
    return j.dump(2) + "\n";
}

bool parse_role(const std::string& role) { return parse_credential_role(role); }

// ---- STS session objects (backlog-sequence ④): .sys/sts/<ak> ----
// Same secrecy rule as credential SKs: with a master key both the SK and the
// token are AES-256-GCM sealed (version 2), without one they are plaintext
// (version 1). An instance without the key cannot verify sessions minted by an
// instance with it (deserialize throws -> logged, session stays unknown there)
std::string session_object_key(std::string_view ak) {
    return std::string(kStsPrefix) + std::string(ak);
}

std::string seal_or_plain(const std::string& v, const std::optional<util::Aes256Key>& key) {
    if (!key) return v;
    std::string sealed = util::aes256gcm_seal(*key, v);
    return util::to_hex(std::span(reinterpret_cast<const uint8_t*>(sealed.data()), sealed.size()));
}

std::optional<std::string> open_or_plain(const std::string& v, const std::optional<util::Aes256Key>& key,
                                         int version, std::string_view ak, const char* field) {
    if (version == 1) return v;
    if (!key)
        throw std::runtime_error("session object " + std::string(ak) + " is encrypted (version 2) but " +
                                 kMasterKeyEnv + " is not set");
    auto blob = util::from_hex(v);
    auto out = util::aes256gcm_open(
        *key, std::string_view(reinterpret_cast<const char*>(blob.data()), blob.size()));
    if (!out)
        throw std::runtime_error("session object " + std::string(ak) + ": " + field +
                                 " decryption failed (wrong " + kMasterKeyEnv + " or corrupted object)");
    return out;
}

// Corrupt JSON returns nullopt (skipped without blocking startup); version=2 with no key / failed decryption
// throws runtime_error -- that is a configuration error, and silently dropping credentials would lock users out;
// better to block startup.
// was_plaintext: the object is still v1 plaintext (load uses this for the v1->v2 upgrade)
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
        c.rev = j.value("rev", uint64_t{1});
        if (j.contains("policy")) c.policy = policy_from_json_obj(j.at("policy"));
        c.tenant = j.value("tenant", "");
        c.tenant_admin = !c.tenant.empty() && parse_role(j.value("role", "user"));
        return c;
    } catch (const json::exception&) {
        return std::nullopt;
    } catch (const S3Error&) {  // corrupt policy field = corrupt object
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

// ---------- credentials_file parsing (docs/credential-management.md §10.2) ----------
// {"credentials": [{"access_key","secret_key","comment"?,"policy"?}]}
// Failure throws runtime_error (fail-fast at startup; on hot reload the caller warns and keeps the old table)

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
            c.tenant = e.value("tenant", "");
            c.tenant_admin = !c.tenant.empty() && parse_role(e.value("role", "user"));
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

const char* action_name(Action a) {
    switch (a) {
        case Action::Read: return "read";
        case Action::Write: return "write";
        case Action::Delete: return "delete";
    }
    return "read";
}

std::optional<Action> action_from_name(std::string_view s) {
    if (s == "read") return Action::Read;
    if (s == "write") return Action::Write;
    if (s == "delete") return Action::Delete;
    return std::nullopt;
}

bool CredentialPolicy::allows_action(Action a) const {
    if (!actions.empty()) {
        for (auto x : actions)
            if (x == a) return true;
        return false;
    }
    // Without actions listed, fall back to readonly: true = read-only, false = unrestricted
    return !readonly || a == Action::Read;
}

bool CredentialPolicy::allows_bucket(std::string_view bucket) const {
    if (buckets.empty() || bucket.empty()) return true;
    std::string b(bucket);
    for (auto& g : buckets)
        // FNM_PATHNAME (docs/archive/gaps.md §5.10): without it '*' crosses '/', and when the same matcher is applied
        // to key prefixes, "logs/*" would admit "logs/a/b" as well
        if (::fnmatch(g.c_str(), b.c_str(), FNM_PATHNAME) == 0) return true;
    return false;
}

bool CredentialPolicy::allows(std::string_view bucket, std::string_view key,
                              Action action) const {
    if (!allows_action(action)) return false;
    // Empty bucket = account-level operation (ListBuckets): admitted; results are policy-filtered by the caller
    if (!allows_bucket(bucket)) return false;
    // Empty key = this check is unrelated to a specific object (create bucket, list bucket, etc.); prefixes not checked
    if (prefixes.empty() || key.empty()) return true;
    return allows_key(key);
}

bool CredentialPolicy::allows_key(std::string_view key) const {
    if (prefixes.empty()) return true;
    for (auto& pre : prefixes)
        if (key.size() >= pre.size() && key.compare(0, pre.size(), pre) == 0) return true;
    return false;
}

bool CredentialPolicy::prefix_may_contain(std::string_view group_prefix) const {
    if (prefixes.empty()) return true;
    // Either the group itself lies within an allowlisted prefix ("logs/2024/" vs allowlist "logs/"), or the
    // allowlisted prefix lies deeper inside the group (group "logs/" vs allowlist "logs/app1/") -- the latter
    // group also contains keys outside the allowlist, but the group name derives only from hierarchy that also
    // exists inside the allowlist, so it is not a leak
    for (auto& pre : prefixes) {
        size_t n = std::min(pre.size(), group_prefix.size());
        if (pre.compare(0, n, group_prefix.data(), n) == 0) return true;
    }
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

// ---------- Loading (docs/credential-management.md §5.1 / §10) ----------

Task<std::shared_ptr<CredentialStore>> CredentialStore::load(
    std::shared_ptr<storage::IStorageBackend> backend, const AuthConfig& cfg) {
    auto store = std::shared_ptr<CredentialStore>(new CredentialStore);
    store->backend_ = std::move(backend);
    store->cfg_ = cfg;

    // master key (§10.1): 64 hex -> 32 bytes; malformed input fails fast
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

    // Dynamic credentials: no .sys = none ever generated
    std::vector<std::string> plaintext_aks;  // v1 objects, upgraded to v2 after load (§10.1)
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
                    c->storage_etag = obj.etag;  // sync change detection (roadmap §2.5)
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
    // STS sessions minted before this instance started (or by others): usable at once
    co_await store->sync_sessions(/*startup=*/true);

    // v1 -> v2 upgrade: once a master key is set, existing plaintext objects are rewritten in place in encrypted form
    for (auto& ak : plaintext_aks) {
        auto it = store->creds_.find(ak);
        storage::ObjectMeta meta;
        meta.content_type = "application/json";
        http::StringBodyReader body(serialize(it->second, store->master_key_));
        auto pr = co_await store->backend_->put_object(kSysBucket, object_key(ak),
                                                       std::move(meta), body);
        it->second.storage_etag = pr.etag;
    }
    if (!plaintext_aks.empty())
        LOG_INFO("re-encrypted {} plaintext credential object(s) with {}",
                 plaintext_aks.size(), kMasterKeyEnv);

    // External credentials file (§10.2): a parse failure at startup fails fast (only hot-reload failures tolerate keeping the old table)
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

    // Static table: on same AK, static wins (docs/credential-management.md §5.1)
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

// ---------- Lookup (verification hot path) ----------

std::optional<CredentialLookup> CredentialStore::lookup(std::string_view ak) const {
    std::shared_lock lk(mu_);
    auto it = creds_.find(ak);
    if (it != creds_.end()) {
        CredentialLookup l{it->second.secret_key, it->second.policy};
        l.tenant = it->second.tenant;
        l.tenant_admin = it->second.tenant_admin;
        return l;
    }
    // STS sessions (roadmap §2.6): expired entries are still returned — verify turns
    // them into ExpiredToken, which tells the SDK to re-assume; a plain
    // InvalidAccessKeyId would read as a configuration error
    auto sit = sessions_.find(ak);
    if (sit == sessions_.end()) return std::nullopt;
    CredentialLookup l{sit->second.secret_key, sit->second.policy, sit->second.token,
                       sit->second.expires};
    l.tenant = sit->second.tenant;
    return l;
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
                                std::string_view key, Action action) const {
    if (ak.empty()) return;  // auth disabled
    std::shared_lock lk(mu_);
    auto it = creds_.find(ak);
    if (it == creds_.end()) return;  // in-flight revocation race: already-verified requests complete naturally (§7)
    if (!it->second.policy) return;
    if (!it->second.policy->allows(bucket, key, action))
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
    return out;  // the map is already ordered by AK
}

std::vector<CredentialInfo> CredentialStore::list_tenant(std::string_view tenant) const {
    std::shared_lock lk(mu_);
    std::vector<CredentialInfo> out;
    for (auto& [_, c] : creds_)
        if (c.tenant == tenant) out.push_back(c);
    return out;
}

// "user" (default) / "admin"; anything else is InvalidRequest so a typo cannot silently
// grant or withhold the admin role
bool parse_credential_role(const std::string& role) {
    if (role == "admin") return true;
    if (role == "user" || role.empty()) return false;
    throw S3Error(S3ErrorCode::InvalidRequest, "role must be \"user\" or \"admin\".");
}

// ---------- Admin plane ----------

Task<CredentialInfo> CredentialStore::generate(std::string comment,
                                               std::optional<CredentialPolicy> policy,
                                               std::string tenant, bool tenant_admin) {
    CredentialInfo c;
    c.comment = std::move(comment);
    c.policy = std::move(policy);
    c.tenant = std::move(tenant);
    c.tenant_admin = !c.tenant.empty() && tenant_admin;
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

    // Lazy bucket creation (idempotent; concurrent already-exists races just swallow AlreadyOwned)
    if (!sys_bucket_ready_) {
        try {
            co_await backend_->create_bucket(kSysBucket);
        } catch (const S3Error& e) {
            if (e.code != S3ErrorCode::BucketAlreadyOwnedByYou) throw;
        }
        sys_bucket_ready_ = true;
    }

    // Persist first, then take effect (write-through)
    storage::ObjectMeta meta;
    meta.content_type = "application/json";
    http::StringBodyReader body(serialize(c, master_key_));
    auto pr =
        co_await backend_->put_object(kSysBucket, object_key(c.access_key), std::move(meta), body);
    c.storage_etag = pr.etag;

    {
        std::unique_lock lk(mu_);
        creds_[c.access_key] = c;
    }
    LOG_INFO("generated credential {} ({}{})", c.access_key,
             c.comment.empty() ? "no comment" : c.comment,
             c.tenant.empty() ? "" : ", tenant " + c.tenant + (c.tenant_admin ? " admin" : ""));
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
    // The tombstone is recorded before the delete: when interleaved with sync_now's list (list takes effect
    // before delete, emplace after the erase below), the add branch uses it to refuse pulling the just-revoked AK back into memory
    {
        std::unique_lock lk(mu_);
        tombstones_[std::string(ak)] = std::chrono::steady_clock::now();
    }
    // Delete from storage first (idempotent), then memory; on failure memory is kept, consistent with storage
    try {
        co_await backend_->delete_object(kSysBucket, object_key(ak));
    } catch (...) {
        std::unique_lock lk(mu_);
        tombstones_.erase(std::string(ak));  // revocation failed, must not block subsequent sync
        throw;
    }
    {
        std::unique_lock lk(mu_);
        creds_.erase(std::string(ak));
    }
    LOG_INFO("revoked credential {}", ak);
}

Task<CredentialInfo> CredentialStore::update(std::string_view ak, Update upd) {
    CredentialInfo c;
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
        c = it->second;
    }
    if (upd.comment) c.comment = std::move(*upd.comment);
    if (upd.set_policy) c.policy = std::move(upd.policy);
    if (upd.tenant) c.tenant = std::move(*upd.tenant);
    if (upd.tenant_admin) c.tenant_admin = *upd.tenant_admin;
    if (c.tenant.empty()) c.tenant_admin = false;  // the role only exists inside a tenant
    ++c.rev;

    // Write-through, same ordering as generate: storage first, then memory
    storage::ObjectMeta meta;
    meta.content_type = "application/json";
    http::StringBodyReader body(serialize(c, master_key_));
    auto pr =
        co_await backend_->put_object(kSysBucket, object_key(c.access_key), std::move(meta), body);
    c.storage_etag = pr.etag;
    {
        std::unique_lock lk(mu_);
        auto it = creds_.find(ak);
        // Concurrently revoked while we were writing: do not resurrect — remove the
        // object this update just re-created and report the miss
        if (it == creds_.end() || it->second.source != CredSource::kDynamic) {
            lk.unlock();
            try {
                co_await backend_->delete_object(kSysBucket, object_key(c.access_key));
            } catch (...) {
            }
            throw S3Error(S3ErrorCode::InvalidAccessKeyId,
                          "The specified access key does not exist.");
        }
        it->second = c;
    }
    LOG_INFO("updated credential {} (rev {})", c.access_key, c.rev);
    co_return c;
}

// ---------- STS sessions (roadmap §2.6) ----------

namespace {
int64_t unix_secs(std::chrono::system_clock::time_point t) {
    return std::chrono::duration_cast<std::chrono::seconds>(t.time_since_epoch()).count();
}
std::chrono::system_clock::time_point from_unix(int64_t s) {
    return std::chrono::system_clock::time_point(std::chrono::seconds(s));
}
}  // namespace

Task<std::string> CredentialStore::persist_session(const std::string& ak, const SessionEntry& e) {
    json j;
    j["version"] = master_key_ ? 2 : 1;
    j["sk"] = seal_or_plain(static_cast<const std::string&>(e.secret_key), master_key_);
    j["token"] = seal_or_plain(e.token, master_key_);
    j["expires_unix"] = unix_secs(e.expires);
    j["expires"] = util::iso8601(e.expires);
    j["created_unix"] = unix_secs(e.created);
    j["parent"] = e.parent;
    if (e.policy) j["policy"] = policy_to_json_obj(*e.policy);
    if (!e.tenant.empty()) j["tenant"] = e.tenant;
    if (!sys_bucket_ready_) {
        try {
            co_await backend_->create_bucket(kSysBucket);
        } catch (const S3Error& err) {
            if (err.code != S3ErrorCode::BucketAlreadyOwnedByYou) throw;
        }
        sys_bucket_ready_ = true;
    }
    storage::ObjectMeta meta;
    meta.content_type = "application/json";
    http::StringBodyReader body(j.dump(2) + "\n");
    auto pr = co_await backend_->put_object(kSysBucket, session_object_key(ak), std::move(meta), body);
    co_return pr.etag;
}

Task<CredentialStore::SessionCredential> CredentialStore::mint_session(std::string_view parent_ak,
                                                                       int duration_sec) {
    // Bound the table: an unauthenticated caller cannot reach here, but a runaway
    // client must not grow the map without limit
    constexpr size_t kMaxSessions = 100000;

    SessionCredential out;
    out.access_key = std::string(kSessionAkPrefix) + random_access_key().substr(4);  // session-prefixed AK shape
    out.secret_key = random_secret_key();
    {
        uint8_t raw[48];
        fill_random(raw, sizeof(raw));
        out.token = util::base64_encode(std::span(raw, sizeof(raw)));
    }
    auto now = std::chrono::system_clock::now();
    out.expires = now + std::chrono::seconds(duration_sec);

    SessionEntry entry;
    {
        std::unique_lock lk(mu_);
        // Opportunistic sweep of expired sessions (they are also harmless in place:
        // verify answers ExpiredToken for them)
        std::erase_if(sessions_, [&](auto& kv) { return kv.second.expires < now; });
        if (sessions_.size() >= kMaxSessions)
            throw S3Error(S3ErrorCode::SlowDown, "Too many active sessions; retry later.");
        auto pit = creds_.find(parent_ak);
        // A session AK lives in sessions_, not creds_: a session can never assume again
        if (pit == creds_.end())
            throw S3Error(S3ErrorCode::AccessDenied,
                          "Session credentials cannot call AssumeRole.");
        entry = SessionEntry{out.secret_key, out.token, out.expires, pit->second.policy,
                             pit->second.tenant, std::string(parent_ak), now};
    }
    // Persist first, then take effect (write-through, same rule as generate): another
    // instance that sees the session AK before this put lands answers
    // InvalidAccessKeyId and the SDK retries
    co_await persist_session(out.access_key, entry);
    {
        std::unique_lock lk(mu_);
        sessions_[out.access_key] = std::move(entry);
    }
    co_return out;
}

Task<void> CredentialStore::ensure_session_loaded(std::string_view ak_view) {
    if (ak_view.rfind(kSessionAkPrefix, 0) != 0) co_return;
    std::string ak(ak_view);
    {
        std::shared_lock lk(mu_);
        if (sessions_.contains(ak)) co_return;
        auto mit = session_misses_.find(ak);
        if (mit != session_misses_.end() &&
            std::chrono::steady_clock::now() - mit->second < kSessionMissTtl)
            co_return;
    }
    std::optional<SessionEntry> entry;
    bool missing = false;
    try {
        auto stream = co_await backend_->get_object(kSysBucket, session_object_key(ak), std::nullopt);
        auto body = co_await read_all(*stream.body);
        json j = json::parse(body);
        int ver = j.at("version").get<int>();
        SessionEntry e;
        e.secret_key = *open_or_plain(j.at("sk").get<std::string>(), master_key_, ver, ak, "sk");
        e.token = *open_or_plain(j.at("token").get<std::string>(), master_key_, ver, ak, "token");
        e.expires = from_unix(j.at("expires_unix").get<int64_t>());
        e.created = from_unix(j.value("created_unix", int64_t{0}));
        e.parent = j.value("parent", "");
        e.tenant = j.value("tenant", "");
        if (j.contains("policy")) e.policy = policy_from_json_obj(j.at("policy"));
        entry = std::move(e);
    } catch (const S3Error& e) {
        if (e.code == S3ErrorCode::NoSuchKey || e.code == S3ErrorCode::NoSuchBucket) missing = true;
        else LOG_WARN("sts: reading session {} failed: {}", ak, e.message);
    } catch (const std::exception& e) {
        LOG_WARN("sts: session object {} unusable: {}", ak, e.what());
        missing = true;  // malformed / undecryptable: do not retry on every request
    }
    std::unique_lock lk(mu_);
    if (entry) {
        session_misses_.erase(ak);
        sessions_.emplace(ak, std::move(*entry));
        LOG_INFO("sts: session {} loaded from {} (minted elsewhere)", ak, kSysBucket);
    } else if (missing) {
        auto now = std::chrono::steady_clock::now();
        if (session_misses_.size() >= kMaxSessionMisses)
            std::erase_if(session_misses_, [&](auto& kv) { return now - kv.second >= kSessionMissTtl; });
        if (session_misses_.size() < kMaxSessionMisses) session_misses_[ak] = now;
    }
}

Task<void> CredentialStore::sync_sessions(bool startup) {
    if (!co_await backend_->bucket_exists(kSysBucket)) co_return;
    std::vector<std::string> on_storage;
    storage::ListOptions opt;
    opt.prefix = std::string(kStsPrefix);
    for (;;) {
        auto page = co_await backend_->list_objects(kSysBucket, opt);
        for (auto& obj : page.objects) on_storage.push_back(obj.key.substr(kStsPrefix.size()));
        if (!page.is_truncated) break;
        opt.start_after = page.next_token;
    }
    const auto now = std::chrono::system_clock::now();
    size_t added = 0, expired = 0;
    for (auto& ak : on_storage) {
        bool known;
        {
            std::shared_lock lk(mu_);
            known = sessions_.contains(ak);
        }
        if (!known) {
            std::unique_lock lk(mu_);
            session_misses_.erase(ak);  // a listing beats a stale miss
        }
        if (!known) {
            co_await ensure_session_loaded(ak);
            std::shared_lock lk(mu_);
            if (sessions_.contains(ak)) ++added;
        }
        bool is_expired = false;
        {
            std::shared_lock lk(mu_);
            auto it = sessions_.find(ak);
            is_expired = it != sessions_.end() && it->second.expires < now;
        }
        if (is_expired) {
            // Any instance may reap an expired object; a concurrent reap is a harmless NoSuchKey
            try {
                co_await backend_->delete_object(kSysBucket, session_object_key(ak));
            } catch (const S3Error& e) {
                if (e.code != S3ErrorCode::NoSuchKey) LOG_WARN("sts: reaping session {} failed: {}", ak, e.message);
            } catch (const std::exception& e) {
                LOG_WARN("sts: reaping session {} failed: {}", ak, e.what());
            }
            std::unique_lock lk(mu_);
            sessions_.erase(ak);
            ++expired;
        }
    }
    // Sessions in memory whose object vanished (reaped elsewhere): drop them too
    {
        std::set<std::string, std::less<>> listed(on_storage.begin(), on_storage.end());
        std::unique_lock lk(mu_);
        std::erase_if(sessions_, [&](auto& kv) {
            return !listed.contains(kv.first) && kv.second.expires < now;
        });
    }
    if (added || expired)
        LOG_INFO("sts: session sync{}: {} loaded, {} expired reaped", startup ? " (startup)" : "",
                 added, expired);
}

size_t CredentialStore::session_count() const {
    std::shared_lock lk(mu_);
    return sessions_.size();
}

// ---------- File hot reload (§10.2) ----------

void CredentialStore::apply_file_credentials(std::vector<CredentialInfo> creds) {
    std::unique_lock lk(mu_);
    // fail-open guard (README §1.2): refuse to wipe a non-empty table to empty -- a file mis-edited into
    // `{"credentials": []}` is valid JSON, and accepting it would empty the table enabled() depends on.
    // Keep the old table, set degraded (readyz turns 503); the next round recovers once the file is fixed
    if (!creds_.empty() && creds.empty() &&
        std::all_of(creds_.begin(), creds_.end(),
                    [](auto& kv) { return kv.second.source == CredSource::kFile; })) {
        LOG_ERROR("credentials file would empty the credential table; keeping previous "
                  "table (authentication stays enabled)");
        degraded_.store(true, std::memory_order_relaxed);
        return;
    }
    degraded_.store(false, std::memory_order_relaxed);
    // Wholesale replacement of file-sourced entries: credentials removed from the old file are invalidated with it
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
        // File temporarily invisible (mid-state of an editor's atomic replace, etc.): keep the old table and retry next round
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
        // Hot reload failure keeps the old table: better that old credentials live one more round than a parse error wiping the whole table
        LOG_ERROR("credentials file reload failed: {}", e.what());
    }
}

// ---------- Multi-instance incremental sync (§10.3) ----------

Task<void> CredentialStore::sync_now() {
    // The memory snapshot must be taken before the list: write-through guarantees every dynamic credential in the
    // snapshot was persisted at snapshot time, so "in snapshot + not in list" can only mean revoked elsewhere;
    // credentials newly generated by this instance during/after the list are not in the snapshot and cannot be wrongly deleted
    std::vector<std::string> snapshot;
    {
        std::shared_lock lk(mu_);
        for (auto& [ak, c] : creds_)
            if (c.source == CredSource::kDynamic) snapshot.push_back(ak);
    }

    // Full set of dynamic-credential AKs currently in storage, with the listed object
    // ETag: an ETag differing from the one this instance last saw means the credential
    // was edited elsewhere (policy update, roadmap §2.5) and must be re-read
    std::map<std::string, std::string, std::less<>> on_storage;  // ak -> etag
    if (co_await backend_->bucket_exists(kSysBucket)) {
        storage::ListOptions opt;
        opt.prefix = std::string(kCredPrefix);
        for (;;) {
            auto page = co_await backend_->list_objects(kSysBucket, opt);
            for (auto& obj : page.objects)
                on_storage.emplace(obj.key.substr(kCredPrefix.size()), obj.etag);
            if (!page.is_truncated) break;
            opt.start_after = page.next_token;
        }
    }
    size_t added = 0, removed = 0;

    // Tombstone cleanup and snapshot: expired entries are dropped; recently revoked AKs are skipped by the add branch
    // (when remove interleaves with this round's list, the object may still be listed; without the guard a revoked credential would be resurrected)
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

    // Additions and edits: in storage but not in memory -> pull in; in memory with a
    // different storage ETag -> re-read and replace (edit made elsewhere, roadmap §2.5)
    for (auto& [ak, etag] : on_storage) {
        bool changed = false;
        if (auto cur = find(ak)) {
            if (cur->source != CredSource::kDynamic) continue;  // static/file shadows it
            if (etag.empty() || cur->storage_etag == etag) continue;  // unchanged
            changed = true;
        }
        if (!changed && recently_revoked.contains(ak)) continue;
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
            c->storage_etag = etag;
            std::unique_lock lk(mu_);
            if (changed) {
                auto it = creds_.find(ak);
                // Replace only a still-dynamic entry with a strictly newer rev — an
                // equal-rev ETag drift (e.g. a v1->v2 re-encryption) keeps local state
                if (it != creds_.end() && it->second.source == CredSource::kDynamic &&
                    c->rev >= it->second.rev) {
                    bool newer = c->rev > it->second.rev;
                    it->second = std::move(*c);
                    if (newer) ++added;
                }
            } else if (creds_.emplace(c->access_key, std::move(*c)).second) {
                ++added;
            }
        } catch (const std::exception& e) {
            // At runtime, a single failed object fetch does not abort the sync (unlike startup's fail-fast)
            LOG_WARN("sync: failed to load credential {}: {}", ak, e.what());
        }
    }

    // Disappearances: a dynamic credential in the snapshot but not in storage -> revoked elsewhere, invalidate locally.
    // fail-open guard (README §1.2): if .sys is wiped externally, do not empty the table
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
    // STS sessions (backlog-sequence ④): new ones minted elsewhere, expired ones reaped
    co_await sync_sessions(/*startup=*/false);
}

// ---------- Background task assembly (§10.2/§10.3; same pattern as duostore GC: re-arm after completion, no overlap) ----------

Task<void> CredentialStore::file_tick() {
    co_await pool_->schedule();  // the timer thread only dispatches; file IO moves to a pool thread
    std::exception_ptr err;
    try {
        std::error_code ec;
        auto mtime = std::filesystem::last_write_time(cfg_.credentials_file, ec);
        if (!ec && mtime != file_mtime_) reload_file_now();
    } catch (...) {
        err = std::current_exception();
    }
    schedule_file_reload();
    if (err) std::rethrow_exception(err);  // hand off to BackgroundTaskGroup for logging
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
    // cancel must be called outside the group lock (TimerQueue::cancel blocks on in-flight callbacks, which take the group lock)
    TimerQueue::instance().cancel(file_timer_);
    TimerQueue::instance().cancel(sync_timer_);
    bg_.wait_idle();
}

}  // namespace lights3::s3
