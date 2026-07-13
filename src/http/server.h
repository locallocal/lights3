// L1: HTTP 服务器接口与驱动工厂（见 docs/02-http-adapter.md）
#pragma once

#include <functional>
#include <memory>
#include <string>

#include "core/config.h"
#include "http/model.h"

namespace lights3::http {

using Handler = std::function<Task<HttpResponse>(HttpRequest)>;

struct IHttpServer {
    virtual void set_handler(Handler h) = 0;
    virtual void listen(const std::string& addr, uint16_t port) = 0;
    virtual void run() = 0;                 // 阻塞运行直至 shutdown
    virtual void shutdown() = 0;            // 线程安全 & 信号安全
    virtual uint16_t bound_port() const = 0;  // listen 后实际端口（port=0 时有用）
    virtual ~IHttpServer() = default;
};

using DriverFactory = std::function<std::unique_ptr<IHttpServer>(const HttpConfig&)>;

struct HttpServerFactory {
    static std::unique_ptr<IHttpServer> create(const std::string& driver, const HttpConfig& cfg);
    static void register_driver(const std::string& name, DriverFactory factory);
    static std::vector<std::string> drivers();
};

}  // namespace lights3::http
