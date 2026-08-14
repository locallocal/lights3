// 运维 CLI：s3adm —— 用运维面（root 静态凭证）的 AK/SK 管理租户凭证，
// 对接 /-/admin/credentials（docs/credential-management.md §2/§3）：
//   list / get / create / delete。请求经 SigV4 自签名（复用 s3/auth/sigv4 的
// 签名端）+ httplib 同步客户端；响应原样打印服务端 JSON。
#include <gflags/gflags.h>
#include <httplib/httplib.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <sstream>
#include <string>

#include <nlohmann/json.hpp>

#include "core/config.h"
#include "core/util/crypto.h"
#include "core/util/uri.h"
#include "http/model.h"
#include "s3/auth/policy.h"
#include "s3/auth/sigv4.h"
#include "s3/errors.h"

DEFINE_string(endpoint, "http://127.0.0.1:9000",
              "lights3 endpoint（scheme://host[:port]）");
DEFINE_string(ak, "", "root（静态）AK；留空取环境变量 LIGHTS3_ADMIN_AK");
DEFINE_string(sk, "",
              "root（静态）SK；留空取环境变量 LIGHTS3_ADMIN_SK。命令行参数对本机 "
              "ps 可见，优先用环境变量传 SK");
DEFINE_string(region, "us-east-1", "SigV4 region，须与服务端 auth.region 一致");
DEFINE_string(comment, "", "create：凭证备注");
DEFINE_string(policy, "",
              "create：policy JSON（{\"buckets\":[...],\"prefixes\":[...],"
              "\"readonly\":bool,\"actions\":[...]}），或 @file 从文件读");
DEFINE_bool(show_secret, false,
            "get：取回明文 SK（仅动态/文件凭证；高敏动作，服务端留审计日志）");
DEFINE_bool(insecure, false, "https 时跳过服务端证书校验（自签名部署用）");
DEFINE_int32(timeout_sec, 10, "连接/读/写超时（秒）");

namespace {

using lights3::Credential;
using nlohmann::json;
namespace s3 = lights3::s3;
namespace util = lights3::util;

constexpr const char* kBase = "/-/admin/credentials";

constexpr const char* kUsage = R"(用法: s3adm [flags] <command> [args]

命令:
  list          列出全部凭证（SK 掩码）
  get <ak>      查询单个凭证；--show_secret 取回明文 SK
  create        生成一对租户 AK/SK；可带 --comment / --policy
  delete <ak>   吊销动态凭证

认证用 root（静态配置）AK/SK：--ak/--sk 或环境变量 LIGHTS3_ADMIN_AK/LIGHTS3_ADMIN_SK。)";

// 端点解析。signed_host 与 httplib 实际发出的 Host 头逐字节一致（默认端口只发
// host，否则 host:port）——与 cloudproxy Endpoint::parse 同一约定，SigV4 的
// SignedHeaders 含 host，二者不一致即 SignatureDoesNotMatch
struct Endpoint {
    bool https = false;
    std::string host;
    int port = 0;
    std::string signed_host;
    std::string base_url;

    static Endpoint parse(const std::string& url) {
        Endpoint ep;
        std::string rest;
        if (url.rfind("https://", 0) == 0) {
            ep.https = true;
            rest = url.substr(8);
        } else if (url.rfind("http://", 0) == 0) {
            rest = url.substr(7);
        } else {
            throw std::runtime_error("endpoint 须以 http:// 或 https:// 开头: " + url);
        }
        if (!rest.empty() && rest.back() == '/') rest.pop_back();
        if (rest.empty() || rest.find('/') != std::string::npos)
            throw std::runtime_error("endpoint 须为 scheme://host[:port]: " + url);
        auto colon = rest.find(':');
        if (colon == std::string::npos) {
            ep.host = rest;
            ep.port = ep.https ? 443 : 80;
        } else {
            ep.host = rest.substr(0, colon);
            try {
                ep.port = std::stoi(rest.substr(colon + 1));
            } catch (...) {
                throw std::runtime_error("endpoint 端口非法: " + url);
            }
            if (ep.port < 1 || ep.port > 65535)
                throw std::runtime_error("endpoint 端口非法: " + url);
        }
        if (ep.host.empty()) throw std::runtime_error("endpoint host 为空: " + url);
        bool default_port = ep.port == (ep.https ? 443 : 80);
        ep.signed_host = default_port ? ep.host : ep.host + ":" + std::to_string(ep.port);
        ep.base_url = std::string(ep.https ? "https://" : "http://") + ep.host + ":" +
                      std::to_string(ep.port);
        return ep;
    }
};

class AdminClient {
public:
    AdminClient(const Endpoint& ep, Credential cred)
        : ep_(ep),
          cli_(ep.base_url),
          auth_(s3::SigV4Authenticator::build(lights3::AuthConfig{
              .credentials = {}, .region = FLAGS_region, .service = "s3"})),
          cred_(std::move(cred)) {
        auto t = std::chrono::seconds(FLAGS_timeout_sec);
        cli_.set_connection_timeout(t);
        cli_.set_read_timeout(t);
        cli_.set_write_timeout(t);
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
        if (ep.https) cli_.enable_server_certificate_verification(!FLAGS_insecure);
#endif
    }

    httplib::Result get(const std::string& path, const std::string& query) {
        return cli_.Get(path + (query.empty() ? "" : "?" + query),
                        sign("GET", path, query, ""));
    }
    httplib::Result post(const std::string& path, const std::string& body) {
        // Content-Type 不进 SignedHeaders（sign 只收 host + x-amz-*），走 httplib 参数
        return cli_.Post(path, sign("POST", path, "", util::sha256_hex(body)), body,
                         "application/json");
    }
    httplib::Result del(const std::string& path) {
        return cli_.Delete(path, sign("DELETE", path, "", ""));
    }

private:
    // 构造最小 HttpRequest 只为签名（与 cloudproxy RemoteContext::signed_headers
    // 同一手法）：payload_hash 空 = 空 body
    httplib::Headers sign(const std::string& method, const std::string& raw_path,
                          const std::string& raw_query, const std::string& payload_hash) {
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

    Endpoint ep_;
    httplib::Client cli_;
    s3::SigV4Authenticator auth_;
    Credential cred_;
};

// 统一收尾：期望状态码 → 打印响应体（服务端已是缩进 JSON）；否则 stderr + 非零
int finish(const httplib::Result& r, int expect, const std::string& ok_note = "") {
    if (!r) {
        fprintf(stderr, "s3adm: 传输错误: %s\n", httplib::to_string(r.error()).c_str());
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

std::string ak_path(const std::string& ak) {
    return std::string(kBase) + "/" + util::aws_uri_encode(ak, /*encode_slash=*/true);
}

std::string load_policy_arg(const std::string& arg) {
    std::string text = arg;
    if (!arg.empty() && arg.front() == '@') {  // @file：curl 惯例
        std::ifstream f(arg.substr(1));
        if (!f) throw std::runtime_error("无法读取 policy 文件: " + arg.substr(1));
        std::ostringstream ss;
        ss << f.rdbuf();
        text = ss.str();
    }
    // 客户端先过一遍服务端同款解析，本地报错快于一次往返
    s3::parse_policy_json(text);
    return text;
}

}  // namespace

int main(int argc, char** argv) {
    gflags::SetUsageMessage(kUsage);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (argc < 2) {
        fprintf(stderr, "%s\n", kUsage);
        return 2;
    }
    std::string cmd = argv[1];

    std::string ak = FLAGS_ak;
    std::string sk = FLAGS_sk;
    if (ak.empty())
        if (const char* e = std::getenv("LIGHTS3_ADMIN_AK")) ak = e;
    if (sk.empty())
        if (const char* e = std::getenv("LIGHTS3_ADMIN_SK")) sk = e;
    if (ak.empty() || sk.empty()) {
        fprintf(stderr,
                "s3adm: 缺少凭证。--ak/--sk 或环境变量 LIGHTS3_ADMIN_AK/LIGHTS3_ADMIN_SK\n");
        return 2;
    }

    try {
        auto ep = Endpoint::parse(FLAGS_endpoint);
#ifndef CPPHTTPLIB_OPENSSL_SUPPORT
        if (ep.https) {
            fprintf(stderr, "s3adm: 本构建未启用 OpenSSL，不支持 https endpoint\n");
            return 2;
        }
#endif
        AdminClient cli(ep, Credential{ak, util::SecretString(std::move(sk))});

        if (cmd == "list" && argc == 2) {
            return finish(cli.get(kBase, ""), 200);
        }
        if (cmd == "get" && argc == 3) {
            return finish(
                cli.get(ak_path(argv[2]), FLAGS_show_secret ? "show-secret=true" : ""),
                200);
        }
        if (cmd == "create" && argc == 2) {
            json body = json::object();
            if (!FLAGS_comment.empty()) body["comment"] = FLAGS_comment;
            if (!FLAGS_policy.empty())
                body["policy"] = json::parse(load_policy_arg(FLAGS_policy));
            // 空对象也发 body：POST 语义单一，服务端对 {} 与无 body 同义
            return finish(cli.post(kBase, body.dump()), 201);
        }
        if (cmd == "delete" && argc == 3) {
            return finish(cli.del(ak_path(argv[2])), 204,
                          std::string("revoked ") + argv[2]);
        }
    } catch (const s3::S3Error& e) {
        fprintf(stderr, "s3adm: %s\n", e.message.c_str());
        return 1;
    } catch (const std::exception& e) {
        fprintf(stderr, "s3adm: %s\n", e.what());
        return 1;
    }

    fprintf(stderr, "s3adm: 未知命令或参数个数不对: %s\n\n%s\n", cmd.c_str(), kUsage);
    return 2;
}
