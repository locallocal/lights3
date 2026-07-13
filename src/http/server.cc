#include "http/server.h"

#include <map>
#include <stdexcept>

namespace lights3::http {

// 编译进来的驱动在这里挂接注册（静态库中靠 static 初始化注册会被链接器丢弃，
// 因此采用显式注册；新驱动在 CMake 定义宏并在此声明即可）
#ifdef LIGHTS3_DRIVER_BUILTIN
void register_builtin_driver();
#endif
#ifdef LIGHTS3_DRIVER_BEAST
void register_beast_driver();
#endif
#ifdef LIGHTS3_DRIVER_HTTPLIB
void register_httplib_driver();
#endif

namespace {

std::map<std::string, DriverFactory>& registry() {
    static std::map<std::string, DriverFactory> r;
    return r;
}

void ensure_registered() {
    static bool done = [] {
#ifdef LIGHTS3_DRIVER_BUILTIN
        register_builtin_driver();
#endif
#ifdef LIGHTS3_DRIVER_BEAST
        register_beast_driver();
#endif
#ifdef LIGHTS3_DRIVER_HTTPLIB
        register_httplib_driver();
#endif
        return true;
    }();
    (void)done;
}

}  // namespace

void HttpServerFactory::register_driver(const std::string& name, DriverFactory factory) {
    registry()[name] = std::move(factory);
}

std::unique_ptr<IHttpServer> HttpServerFactory::create(const std::string& driver,
                                                       const HttpConfig& cfg) {
    ensure_registered();
    auto it = registry().find(driver);
    if (it == registry().end())
        throw std::runtime_error("unknown http driver: " + driver +
                                 " (not compiled in or misspelled)");
    return it->second(cfg);
}

std::vector<std::string> HttpServerFactory::drivers() {
    ensure_registered();
    std::vector<std::string> out;
    for (auto& [k, _] : registry()) out.push_back(k);
    return out;
}

}  // namespace lights3::http
