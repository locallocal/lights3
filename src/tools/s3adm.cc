// 运维 CLI：s3adm —— 用运维面（root 静态凭证）的 AK/SK 管理租户凭证，
// 对接 /-/admin/credentials（docs/credential-management.md §2/§3）。
// 子命令框架用 ccmd（third_party/ccmd）：list / get / create / delete 各为
// 独立子命令、各持独立选项集——ccmd 的 root 选项不下传，连接类选项须写在
// 子命令之后（s3adm list --endpoint=...），长选项取值只认 --name=value 形式。
// 请求经 SigV4 自签名（复用 s3/auth/sigv4 的签名端）+ httplib 同步客户端；
// 响应原样打印服务端 JSON。
#include <ccmd.h>
#include <httplib/httplib.h>

#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
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

namespace {

using lights3::Credential;
using nlohmann::json;
namespace s3 = lights3::s3;
namespace util = lights3::util;

constexpr const char* kBase = "/-/admin/credentials";

// ccmd 回调无返回值，进程退出码经此带出（0 成功 / 1 请求失败 / 2 用法错误）
int g_exit = 0;

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
    AdminClient(const Endpoint& ep, Credential cred, const std::string& region,
                int timeout_sec, bool insecure)
        : ep_(ep),
          cli_(ep.base_url),
          auth_(s3::SigV4Authenticator::build(lights3::AuthConfig{
              .credentials = {}, .region = region, .service = "s3"})),
          cred_(std::move(cred)) {
        auto t = std::chrono::seconds(timeout_sec);
        cli_.set_connection_timeout(t);
        cli_.set_read_timeout(t);
        cli_.set_write_timeout(t);
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
        if (ep.https) cli_.enable_server_certificate_verification(!insecure);
#else
        (void)insecure;
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

// 连接类公共选项：ccmd 每个子命令的选项集独立，逐个注册
void add_conn_flags(const std::shared_ptr<ccmd::c_command>& cmd) {
    cmd->varp<std::string>("endpoint", "e", "http://127.0.0.1:9000",
                           "lights3 endpoint（scheme://host[:port]）.");
    cmd->var<std::string>("ak", "", "root（静态）AK；留空取环境变量 LIGHTS3_ADMIN_AK.");
    cmd->var<std::string>("sk", "",
                          "root（静态）SK；留空取环境变量 LIGHTS3_ADMIN_SK（推荐：argv "
                          "对本机 ps 可见）.");
    cmd->var<std::string>("region", "us-east-1", "SigV4 region，须与服务端 auth.region 一致.");
    cmd->var<bool>("insecure", false, "https 时跳过服务端证书校验（自签名部署用）.");
    cmd->var<int>("timeout-sec", 10, "连接/读/写超时（秒）.");
}

// 读连接选项 + 环境变量兜底，构造客户端执行 fn；异常统一在此落成退出码
template <class Fn>
void run_admin(const std::shared_ptr<ccmd::c_command>& cmd, Fn&& fn) {
    try {
        std::string ak = cmd->var<std::string>("ak");
        std::string sk = cmd->var<std::string>("sk");
        if (ak.empty())
            if (const char* e = std::getenv("LIGHTS3_ADMIN_AK")) ak = e;
        if (sk.empty())
            if (const char* e = std::getenv("LIGHTS3_ADMIN_SK")) sk = e;
        if (ak.empty() || sk.empty()) {
            fprintf(stderr,
                    "s3adm: 缺少凭证。--ak/--sk 或环境变量 "
                    "LIGHTS3_ADMIN_AK/LIGHTS3_ADMIN_SK\n");
            g_exit = 2;
            return;
        }
        auto ep = Endpoint::parse(cmd->var<std::string>("endpoint"));
#ifndef CPPHTTPLIB_OPENSSL_SUPPORT
        if (ep.https) {
            fprintf(stderr, "s3adm: 本构建未启用 OpenSSL，不支持 https endpoint\n");
            g_exit = 2;
            return;
        }
#endif
        AdminClient cli(ep, Credential{ak, util::SecretString(std::move(sk))},
                        cmd->var<std::string>("region"), cmd->var<int>("timeout-sec"),
                        cmd->var<bool>("insecure"));
        g_exit = fn(cli);
    } catch (const s3::S3Error& e) {
        fprintf(stderr, "s3adm: %s\n", e.message.c_str());
        g_exit = 1;
    } catch (const std::exception& e) {
        fprintf(stderr, "s3adm: %s\n", e.what());
        g_exit = 1;
    }
}

// 位置参数须恰为一个 AK；不满足打印子命令用法并置用法错误退出码
bool one_ak_arg(const std::shared_ptr<ccmd::c_command>& cmd, std::string& ak) {
    if (cmd->args().size() != 1) {
        fprintf(stderr, "s3adm: 用法: %s\n", cmd->usage().c_str());
        g_exit = 2;
        return false;
    }
    ak = cmd->args().front();
    return true;
}

std::shared_ptr<ccmd::c_command> make_list() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "list", "s3adm list --endpoint=http://127.0.0.1:9000",
        "s3adm list [options]", "列出全部凭证（SK 掩码，含静态与文件来源）.",
        "列出全部凭证（SK 掩码）.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            run_admin(c, [](AdminClient& cli) { return finish(cli.get(kBase, ""), 200); });
        });
    add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_get() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "get", "s3adm get L3AKXXXX --show-secret", "s3adm get <ak> [options]",
        "查询单个凭证元数据；--show-secret 取回明文 SK（仅动态/文件凭证，"
        "高敏动作，服务端留审计日志）.",
        "查询单个凭证.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            std::string ak;
            if (!one_ak_arg(c, ak)) return;
            bool show = c->var<bool>("show-secret");
            run_admin(c, [&](AdminClient& cli) {
                return finish(cli.get(ak_path(ak), show ? "show-secret=true" : ""), 200);
            });
        });
    cmd->varp<bool>("show-secret", "s", false, "取回明文 SK（仅动态/文件凭证）.");
    add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_create() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "create",
        R"(s3adm create --comment=tenant-a --policy='{"buckets":["tenant-a-*"]}')",
        "s3adm create [options]",
        "生成一对租户 AK/SK（响应是唯一一次完整返回 SK 的机会）。--policy 传 "
        "policy JSON（{\"buckets\":[...],\"prefixes\":[...],\"readonly\":bool,"
        "\"actions\":[...]}）或 @file 从文件读.",
        "生成一对租户 AK/SK.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            if (!c->args().empty()) {
                fprintf(stderr, "s3adm: 用法: %s\n", c->usage().c_str());
                g_exit = 2;
                return;
            }
            run_admin(c, [&](AdminClient& cli) {
                json body = json::object();
                auto comment = c->var<std::string>("comment");
                auto policy = c->var<std::string>("policy");
                if (!comment.empty()) body["comment"] = comment;
                if (!policy.empty()) body["policy"] = json::parse(load_policy_arg(policy));
                // 空对象也发 body：POST 语义单一，服务端对 {} 与无 body 同义
                return finish(cli.post(kBase, body.dump()), 201);
            });
        });
    cmd->varp<std::string>("comment", "c", "", "凭证备注.");
    cmd->varp<std::string>("policy", "p", "", "policy JSON，或 @file 从文件读.");
    add_conn_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_delete() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "delete", "s3adm delete L3AKXXXX", "s3adm delete <ak> [options]",
        "吊销动态凭证（静态凭证归配置文件管，服务端拒绝）.", "吊销动态凭证.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            std::string ak;
            if (!one_ak_arg(c, ak)) return;
            run_admin(c, [&](AdminClient& cli) {
                return finish(cli.del(ak_path(ak)), 204, "revoked " + ak);
            });
        });
    add_conn_flags(cmd);
    return cmd;
}

}  // namespace

int main(int argc, char* argv[]) {
    auto root = std::make_shared<ccmd::c_command>(
        "s3adm", "s3adm list --endpoint=http://127.0.0.1:9000",
        "s3adm <command> [options]",
        "用 root（静态）AK/SK 管理租户凭证，对接 /-/admin/credentials"
        "（docs/credential-management.md）。凭证经各子命令的 --ak=/--sk= 或环境变量 "
        "LIGHTS3_ADMIN_AK/LIGHTS3_ADMIN_SK 传入；选项须写在子命令之后，长选项取值"
        "用 --name=value 形式.",
        "lights3 租户凭证管理.",
        // 裸 s3adm / s3adm -x：没有可执行的操作，打印帮助并按用法错误退出
        [](const std::shared_ptr<ccmd::c_command>& c) {
            c->print_help();
            g_exit = 2;
        });
    root->add_subcommand(make_list());
    root->add_subcommand(make_get());
    root->add_subcommand(make_create());
    root->add_subcommand(make_delete());
    root->execute(argc, argv);
    return g_exit;
}
