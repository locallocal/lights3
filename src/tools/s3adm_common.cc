#include "tools/s3adm_common.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <stdexcept>
#include <utility>

#include "core/util/crypto.h"
#include "http/model.h"

namespace s3adm {

namespace util = lights3::util;
namespace s3 = lights3::s3;

Endpoint Endpoint::parse(const std::string& url) {
    Endpoint ep;
    std::string rest;
    if (url.rfind("https://", 0) == 0) {
        ep.https = true;
        rest = url.substr(8);
    } else if (url.rfind("http://", 0) == 0) {
        rest = url.substr(7);
    } else {
        throw std::runtime_error("endpoint must start with http:// or https://: " + url);
    }
    if (!rest.empty() && rest.back() == '/') rest.pop_back();
    if (rest.empty() || rest.find('/') != std::string::npos)
        throw std::runtime_error("endpoint must be scheme://host[:port]: " + url);
    auto colon = rest.find(':');
    if (colon == std::string::npos) {
        ep.host = rest;
        ep.port = ep.https ? 443 : 80;
    } else {
        ep.host = rest.substr(0, colon);
        try {
            ep.port = std::stoi(rest.substr(colon + 1));
        } catch (...) {
            throw std::runtime_error("endpoint has an invalid port: " + url);
        }
        if (ep.port < 1 || ep.port > 65535)
            throw std::runtime_error("endpoint has an invalid port: " + url);
    }
    if (ep.host.empty()) throw std::runtime_error("endpoint has an empty host: " + url);
    bool default_port = ep.port == (ep.https ? 443 : 80);
    ep.signed_host = default_port ? ep.host : ep.host + ":" + std::to_string(ep.port);
    ep.base_url = std::string(ep.https ? "https://" : "http://") + ep.host + ":" +
                  std::to_string(ep.port);
    return ep;
}

void add_conn_flags(const std::shared_ptr<ccmd::c_command>& cmd) {
    cmd->varp<std::string>("endpoint", "e", "http://127.0.0.1:9000",
                           "lights3 endpoint (scheme://host[:port]).");
    cmd->var<std::string>("ak", "", "access key; falls back to env LIGHTS3_ADMIN_AK.");
    cmd->var<std::string>("sk", "",
                          "secret key; falls back to env LIGHTS3_ADMIN_SK "
                          "(preferred: argv is visible to local ps).");
    cmd->var<std::string>("region", "us-east-1", "SigV4 region; must match the server auth.region.");
    cmd->var<bool>("insecure", false, "skip server certificate verification for https (self-signed deployments).");
    cmd->var<int>("timeout-sec", 10, "connect/read/write timeout in seconds.");
}

bool read_conn_opts(const std::shared_ptr<ccmd::c_command>& cmd, ConnOpts& out) {
    out.ak = cmd->var<std::string>("ak");
    out.sk = cmd->var<std::string>("sk");
    if (out.ak.empty())
        if (const char* e = std::getenv("LIGHTS3_ADMIN_AK")) out.ak = e;
    if (out.sk.empty())
        if (const char* e = std::getenv("LIGHTS3_ADMIN_SK")) out.sk = e;
    if (out.ak.empty() || out.sk.empty()) {
        fprintf(stderr,
                "s3adm: missing credentials; pass --ak=/--sk= or set env "
                "LIGHTS3_ADMIN_AK/LIGHTS3_ADMIN_SK\n");
        g_exit = 2;
        return false;
    }
    out.ep = Endpoint::parse(cmd->var<std::string>("endpoint"));
#ifndef CPPHTTPLIB_OPENSSL_SUPPORT
    if (out.ep.https) {
        fprintf(stderr, "s3adm: this build lacks OpenSSL support; https endpoints are unavailable\n");
        g_exit = 2;
        return false;
    }
#endif
    out.region = cmd->var<std::string>("region");
    out.timeout_sec = cmd->var<int>("timeout-sec");
    out.insecure = cmd->var<bool>("insecure");
    return true;
}

SignedClient::SignedClient(const ConnOpts& conn)
    : ep_(conn.ep),
      cli_(conn.ep.base_url),
      auth_(s3::SigV4Authenticator::build(lights3::AuthConfig{
          .credentials = {}, .region = conn.region, .service = "s3"})),
      cred_(lights3::Credential{conn.ak, util::SecretString(std::string(conn.sk))}) {
    auto t = std::chrono::seconds(conn.timeout_sec);
    cli_.set_connection_timeout(t);
    cli_.set_read_timeout(t);
    cli_.set_write_timeout(t);
    cli_.set_keep_alive(true);
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
    if (ep_.https) cli_.enable_server_certificate_verification(!conn.insecure);
#endif
}

httplib::Result SignedClient::get(const std::string& path, const std::string& query) {
    return cli_.Get(path + (query.empty() ? "" : "?" + query),
                    sign("GET", path, query, ""));
}

httplib::Result SignedClient::get_discard(const std::string& path, uint64_t* bytes) {
    return cli_.Get(path, sign("GET", path, "", ""),
                    [bytes](const char*, size_t n) {
                        *bytes += n;
                        return true;
                    });
}

httplib::Result SignedClient::get_hashed(const std::string& path, const std::string& query,
                                         std::string* md5_hex, uint64_t* bytes) {
    util::HashStream md5(util::HashStream::Algo::Md5);
    auto r = cli_.Get(path + (query.empty() ? "" : "?" + query), sign("GET", path, query, ""),
                      [&](const char* data, size_t n) {
                          md5.update(std::span(reinterpret_cast<const uint8_t*>(data), n));
                          *bytes += n;
                          return true;
                      });
    *md5_hex = md5.final_hex();
    return r;
}

httplib::Result SignedClient::head(const std::string& path, const std::string& query) {
    return cli_.Head(path + (query.empty() ? "" : "?" + query),
                     sign("HEAD", path, query, ""));
}

httplib::Result SignedClient::put_unsigned(const std::string& path, const std::string& body,
                                           const std::string& query) {
    return cli_.Put(path + (query.empty() ? "" : "?" + query),
                    sign("PUT", path, query, "UNSIGNED-PAYLOAD"), body,
                    "application/octet-stream");
}

httplib::Result SignedClient::post_json(const std::string& path, const std::string& body) {
    // Content-Type does not go into SignedHeaders (sign only takes host + x-amz-*); passed as an httplib parameter
    return cli_.Post(path, sign("POST", path, "", util::sha256_hex(body)), body,
                     "application/json");
}

httplib::Result SignedClient::put_json(const std::string& path, const std::string& body,
                                       const std::string& query) {
    return cli_.Put(path + (query.empty() ? "" : "?" + query),
                    sign("PUT", path, query, util::sha256_hex(body)), body, "application/json");
}

httplib::Result SignedClient::post_empty(const std::string& path, const std::string& query) {
    return cli_.Post(path + (query.empty() ? "" : "?" + query),
                     sign("POST", path, query, util::sha256_hex("")), "", "application/json");
}

httplib::Result SignedClient::del(const std::string& path, const std::string& query) {
    return cli_.Delete(path + (query.empty() ? "" : "?" + query),
                       sign("DELETE", path, query, ""));
}

int finish(const httplib::Result& r, int expect, const std::string& ok_note) {
    if (!r) {
        fprintf(stderr, "s3adm: transport error: %s\n", httplib::to_string(r.error()).c_str());
        return 1;
    }
    if (r->status != expect) {
        fprintf(stderr, "s3adm: HTTP %d\n%s", r->status, r->body.c_str());
        if (!r->body.empty() && r->body.back() != '\n') fputc('\n', stderr);
        return 1;
    }
    if (!r->body.empty())
        fputs(r->body.c_str(), stdout);
    else if (!ok_note.empty())
        printf("%s\n", ok_note.c_str());
    return 0;
}

httplib::Headers SignedClient::sign(const std::string& method, const std::string& raw_path,
                                    const std::string& raw_query,
                                    const std::string& payload_hash) {
    lights3::http::HttpRequest req;
    req.method = method;
    req.raw_path = raw_path;
    req.raw_query = raw_query;
    req.headers.set("Host", ep_.signed_host);
    auth_.sign(req, cred_, payload_hash);
    httplib::Headers out;
    for (auto& [k, v] : req.headers.items()) out.emplace(k, v);
    return out;
}

}  // namespace s3adm
