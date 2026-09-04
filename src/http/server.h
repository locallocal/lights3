// L1: HTTP server interface and driver factory (see docs/http-adapter.md)
#pragma once

#include <functional>
#include <memory>
#include <string>

#include "core/config.h"
#include "http/model.h"

namespace lights3::http {

using Handler = std::function<Task<HttpResponse>(HttpRequest)>;

// L1 connection counters (roadmap §4.2), rendered on /-/metrics. Every driver
// fills what its model can observe; httplib (upstream accept loop) reports only
// what its socket hook sees. Timeouts are attributed to the phase that expired
struct ConnStats {
    uint64_t accepted = 0;          // connections accepted
    uint64_t rejected_limit = 0;    // refused by http.max_connections
    uint64_t active = 0;            // currently open
    uint64_t keepalive_closes = 0;  // closed after http.max_requests_per_connection
    uint64_t timeouts_idle = 0;     // keep-alive wait expired (idle_timeout)
    uint64_t timeouts_header = 0;   // request line / headers (header_timeout)
    uint64_t timeouts_body = 0;     // body read (body_timeout)
    uint64_t timeouts_write = 0;    // response write (write_timeout)
};

struct IHttpServer {
    virtual void set_handler(Handler h) = 0;
    virtual void listen(const std::string& addr, uint16_t port) = 0;
    virtual void run() = 0;                 // Blocks until shutdown
    virtual void shutdown() = 0;            // Thread-safe & signal-safe
    virtual uint16_t bound_port() const = 0;  // Actual port after listen (useful when port=0)
    virtual ConnStats stats() const { return {}; }  // Thread-safe snapshot of the counters
    virtual ~IHttpServer() = default;
};

using DriverFactory = std::function<std::unique_ptr<IHttpServer>(const HttpConfig&)>;

struct HttpServerFactory {
    static std::unique_ptr<IHttpServer> create(const std::string& driver, const HttpConfig& cfg);
    static void register_driver(const std::string& name, DriverFactory factory);
    static std::vector<std::string> drivers();
};

}  // namespace lights3::http
