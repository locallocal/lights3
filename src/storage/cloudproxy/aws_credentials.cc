#include "storage/cloudproxy/aws_credentials.h"

#include <httplib/httplib.h>

#include <cstdlib>
#include <nlohmann/json.hpp>

#include "core/log.h"
#include "core/util/time.h"

namespace lights3::storage::cloudproxy {

namespace {

constexpr auto kRefreshMargin = std::chrono::minutes(5);
constexpr auto kNegativeCacheTtl = std::chrono::seconds(60);

std::string env(const char* k) {
    const char* v = std::getenv(k);
    return v ? std::string(v) : std::string();
}

// Both the container endpoint and IMDS serve the same JSON credential document
// ({AccessKeyId, SecretAccessKey, Token, Expiration}); parse failures return
// invalid creds and the chain moves on / retries later
AwsCreds parse_credential_doc(const std::string& body, const std::string& what) {
    AwsCreds c;
    try {
        auto j = nlohmann::json::parse(body);
        c.access_key = j.value("AccessKeyId", "");
        c.secret_key = j.value("SecretAccessKey", "");
        c.session_token = j.value("Token", "");
        if (auto exp = j.value("Expiration", ""); !exp.empty()) {
            if (auto t = util::parse_iso8601(exp)) c.expiry = *t;
        }
    } catch (const std::exception& e) {
        LOG_WARN("cloudproxy credentials: unparsable document from {}: {}", what, e.what());
        return {};
    }
    if (!c.valid()) LOG_WARN("cloudproxy credentials: incomplete document from {}", what);
    return c;
}

// Metadata endpoints answer in single-digit milliseconds on-host; short timeouts
// keep a non-EC2 misconfiguration from stalling requests
httplib::Client metadata_client(const std::string& base) {
    httplib::Client c(base);
    c.set_connection_timeout(std::chrono::seconds(1));
    c.set_read_timeout(std::chrono::seconds(2));
    c.set_write_timeout(std::chrono::seconds(2));
    return c;
}

}  // namespace

CredentialProvider::CredentialProvider(std::string name, std::string imds_endpoint)
    : name_(std::move(name)), imds_endpoint_(std::move(imds_endpoint)) {}

AwsCreds CredentialProvider::from_env() {
    AwsCreds c;
    c.access_key = env("AWS_ACCESS_KEY_ID");
    c.secret_key = env("AWS_SECRET_ACCESS_KEY");
    c.session_token = env("AWS_SESSION_TOKEN");
    return c.valid() ? c : AwsCreds{};
}

AwsCreds CredentialProvider::from_container() {
    std::string url;
    if (auto rel = env("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI"); !rel.empty())
        url = "http://169.254.170.2" + rel;
    else if (auto full = env("AWS_CONTAINER_CREDENTIALS_FULL_URI"); !full.empty())
        url = full;
    if (url.empty()) return {};
    // Split scheme://host[:port] from the path for httplib
    auto scheme_end = url.find("://");
    if (scheme_end == std::string::npos) return {};
    auto path_start = url.find('/', scheme_end + 3);
    std::string base = path_start == std::string::npos ? url : url.substr(0, path_start);
    std::string path = path_start == std::string::npos ? "/" : url.substr(path_start);
    auto cli = metadata_client(base);
    httplib::Headers headers;
    if (auto tok = env("AWS_CONTAINER_AUTHORIZATION_TOKEN"); !tok.empty())
        headers.emplace("Authorization", tok);
    auto r = cli.Get(path, headers);
    if (!r || r->status != 200) {
        LOG_WARN("cloudproxy '{}': container credential endpoint {} unreachable ({})", name_,
                 base, r ? "HTTP " + std::to_string(r->status) : httplib::to_string(r.error()));
        return {};
    }
    return parse_credential_doc(r->body, "container endpoint");
}

AwsCreds CredentialProvider::from_imds() {
    auto cli = metadata_client(imds_endpoint_);
    // IMDSv2 only: the session token PUT also serves as the hop-limit/SSRF guard;
    // no v1 fallback (v1 is disabled on hardened instances anyway)
    auto tok = cli.Put("/latest/api/token", httplib::Headers{{
                           "X-aws-ec2-metadata-token-ttl-seconds", "21600"}},
                       "", "text/plain");
    if (!tok || tok->status != 200 || tok->body.empty()) return {};
    httplib::Headers auth{{"X-aws-ec2-metadata-token", tok->body}};
    auto role = cli.Get("/latest/meta-data/iam/security-credentials/", auth);
    if (!role || role->status != 200) {
        LOG_WARN("cloudproxy '{}': IMDS reachable but no instance role attached", name_);
        return {};
    }
    std::string role_name = role->body.substr(0, role->body.find('\n'));
    if (role_name.empty()) return {};
    auto doc = cli.Get("/latest/meta-data/iam/security-credentials/" + role_name, auth);
    if (!doc || doc->status != 200) return {};
    return parse_credential_doc(doc->body, "IMDS role " + role_name);
}

AwsCreds CredentialProvider::resolve() {
    if (auto c = from_env(); c.valid()) {
        if (source_ != "environment")
            LOG_INFO("cloudproxy '{}': using AWS credentials from the environment", name_);
        source_ = "environment";
        return c;
    }
    if (auto c = from_container(); c.valid()) {
        if (source_ != "container")
            LOG_INFO("cloudproxy '{}': using AWS credentials from the container endpoint",
                     name_);
        source_ = "container";
        return c;
    }
    if (auto c = from_imds(); c.valid()) {
        if (source_ != "imds")
            LOG_INFO("cloudproxy '{}': using AWS credentials from EC2 IMDSv2", name_);
        source_ = "imds";
        return c;
    }
    return {};
}

AwsCreds CredentialProvider::get() {
    std::lock_guard lk(m_);
    auto now = std::chrono::system_clock::now();
    bool stale = !cached_.valid()
                     ? now >= next_attempt_
                     : cached_.expiry != std::chrono::system_clock::time_point{} &&
                           now >= cached_.expiry - kRefreshMargin;
    if (stale) {
        auto fresh = resolve();
        if (fresh.valid()) {
            cached_ = std::move(fresh);
        } else {
            // Keep previous credentials (valid until their expiry beats us); probe
            // again only after the negative-cache window — a non-EC2 host must not
            // pay a metadata connect timeout per request
            next_attempt_ = now + kNegativeCacheTtl;
            if (!cached_.valid())
                LOG_WARN("cloudproxy '{}': no AWS credentials found (static config, "
                         "environment, container endpoint, IMDS all empty); requests "
                         "will be signed with empty keys",
                         name_);
        }
    }
    return cached_;
}

}  // namespace lights3::storage::cloudproxy
