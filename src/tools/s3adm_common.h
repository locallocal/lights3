// s3adm shared support: process exit code, endpoint parsing, connection
// options and the SigV4 self-signing httplib client used by every command
// group (cred: s3adm_cred.cc, bench: s3adm_bench.cc).
#pragma once

#include <ccmd.h>
#include <httplib/httplib.h>

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>

#include "core/config.h"
#include "s3/auth/sigv4.h"

namespace s3adm {

// ccmd callbacks return nothing; the process exit code is carried out through
// this (0 success / 1 request failure / 2 usage error). Defined in s3adm.cc.
extern int g_exit;

// Endpoint parsing. signed_host matches the Host header httplib actually
// sends byte for byte (default port sends only host, otherwise host:port) —
// the same convention as cloudproxy Endpoint::parse; SigV4's SignedHeaders
// includes host, and any mismatch means SignatureDoesNotMatch
struct Endpoint {
    bool https = false;
    std::string host;
    int port = 0;
    std::string signed_host;
    std::string base_url;

    static Endpoint parse(const std::string& url);
};

// Connection options read from a leaf subcommand's flags (+ env fallback for
// ak/sk). Kept as plain values so callers can build any number of clients
// (bench needs one per worker thread).
struct ConnOpts {
    Endpoint ep;
    std::string ak;
    std::string sk;
    std::string region;
    int timeout_sec = 10;
    bool insecure = false;
};

// Common connection options: each ccmd subcommand's option set is independent, so register them one by one
void add_conn_flags(const std::shared_ptr<ccmd::c_command>& cmd);

// Reads the connection flags registered by add_conn_flags, with env fallback
// LIGHTS3_ADMIN_AK/LIGHTS3_ADMIN_SK. On a usage error (missing credentials,
// https without OpenSSL) prints to stderr, sets g_exit = 2 and returns false;
// a malformed endpoint throws (callers route exceptions to exit code 1).
bool read_conn_opts(const std::shared_ptr<ccmd::c_command>& cmd, ConnOpts& out);

// SigV4 self-signing synchronous HTTP client (signing side of s3/auth/sigv4 +
// httplib). Not thread-safe: one instance per thread. keep-alive is enabled —
// requests on one instance reuse the TCP connection.
class SignedClient {
public:
    explicit SignedClient(const ConnOpts& conn);

    httplib::Result get(const std::string& path, const std::string& query);
    // GET that streams the body into the void, adding its size to *bytes —
    // benchmark downloads must not buffer whole objects in memory
    httplib::Result get_discard(const std::string& path, uint64_t* bytes);
    // GET that streams the body through an MD5 (fsck verification): *md5_hex
    // receives the digest of whatever body arrived — only meaningful when the
    // status is the expected success code
    httplib::Result get_hashed(const std::string& path, const std::string& query,
                               std::string* md5_hex, uint64_t* bytes);
    httplib::Result head(const std::string& path, const std::string& query = "");
    // Signs the literal UNSIGNED-PAYLOAD instead of hashing the body (the
    // server uses the header value verbatim in the canonical request), so
    // benchmark uploads do not pay a client-side SHA-256 per request
    httplib::Result put_unsigned(const std::string& path, const std::string& body,
                                 const std::string& query = "");
    httplib::Result post_json(const std::string& path, const std::string& body);
    // JSON PUT (admin plane) and empty-body POST (admin actions such as a usage rescan)
    httplib::Result put_json(const std::string& path, const std::string& body,
                             const std::string& query = "");
    httplib::Result post_empty(const std::string& path, const std::string& query = "");
    httplib::Result del(const std::string& path, const std::string& query = "");

private:
    // Builds a minimal HttpRequest solely for signing (same technique as
    // cloudproxy RemoteContext::signed_headers): empty payload_hash = empty body
    httplib::Headers sign(const std::string& method, const std::string& raw_path,
                          const std::string& raw_query, const std::string& payload_hash);

    Endpoint ep_;
    httplib::Client cli_;
    lights3::s3::SigV4Authenticator auth_;
    lights3::Credential cred_;
};

// Unified wrap-up: expected status code -> print the response body verbatim (or ok_note
// when the body is empty); otherwise stderr + exit code 1
int finish(const httplib::Result& r, int expect, const std::string& ok_note = "");

// Reads connection options + env-var fallback, builds the client and runs fn; exceptions all land here as exit codes
template <class Fn>
void run_admin(const std::shared_ptr<ccmd::c_command>& cmd, Fn&& fn) {
    try {
        ConnOpts conn;
        if (!read_conn_opts(cmd, conn)) return;
        SignedClient cli(conn);
        g_exit = fn(cli);
    } catch (const lights3::s3::S3Error& e) {
        fprintf(stderr, "s3adm: %s\n", e.message.c_str());
        g_exit = 1;
    } catch (const std::exception& e) {
        fprintf(stderr, "s3adm: %s\n", e.what());
        g_exit = 1;
    }
}

}  // namespace s3adm
