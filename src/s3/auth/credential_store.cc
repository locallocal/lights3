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
    if (c.policy) j["policy"] = policy_to_json_obj(*c.policy);
    return j.dump(2) + "\n";
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
        if (j.contains("policy")) c.policy = policy_from_json_obj(j.at("policy"));
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

    // v1 -> v2 upgrade: once a master key is set, existing plaintext objects are rewritten in place in encrypted form
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
    if (it == creds_.end()) return std::nullopt;
    return CredentialLookup{it->second.secret_key, it->second.policy};
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

// ---------- Admin plane ----------

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

    // Full set of dynamic-credential AKs currently in storage
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

    // Additions: in storage, not in memory -> pull into the table
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
