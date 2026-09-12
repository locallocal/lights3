#include "cli/cli_server.h"

#include <algorithm>
#include <cstdio>
#include <map>
#include <string>

#include "app/app.h"
#include "core/version.h"
#include "http/server.h"
#include "storage/registry.h"
#ifdef LIGHTS3_DUOSTORE
#include "storage/duostore/duostore_backend.h"
#endif

namespace lights3_cli {

int check_config(const std::string& path) {
    using namespace lights3;
    Config cfg;
    try {
        cfg = Config::load(path);
    } catch (const std::exception& e) {
        fprintf(stderr, "config error: %s\n", e.what());
        return 1;
    }
    int problems = 0;
    auto drivers = http::HttpServerFactory::drivers();
    if (std::find(drivers.begin(), drivers.end(), cfg.http.driver) == drivers.end()) {
        fprintf(stderr, "config error: http.driver '%s' is not compiled into this binary\n", cfg.http.driver.c_str());
        ++problems;
    }
    auto types = storage::StorageRegistry::registered_types();
    for (auto& b : cfg.backends)
        if (std::find(types.begin(), types.end(), b.type) == types.end()) {
            fprintf(stderr, "config error: backends[%s].type '%s' is not compiled into this binary\n", b.name.c_str(),
                    b.type.c_str());
            ++problems;
        }
    // duostore parameters are parsed the way the backend constructor would (engine
    // selection, ranges, compiled-in engines) and the deployment sanity warning is
    // surfaced here too (docs/archive/multi-gateway-multipart-design.md §4 ④): a
    // dry run must say what startup would say. Other backends validate lazily
    // backend name -> "meta=… data=…"
    std::map<std::string, std::string> engines;
#ifdef LIGHTS3_DUOSTORE
    for (auto& b : cfg.backends) {
        if (b.type != "duostore") continue;
        try {
            auto dc = storage::DuoStoreConfig::from_params(b.name, b.params);
            engines[b.name] = std::string("meta=") + dc.meta_kind_name() + " data=" + dc.data_kind_name();
            if (auto w = dc.deployment_warning())
                fprintf(stderr, "config warning: backends[%s]: %s\n", b.name.c_str(), w->c_str());
        } catch (const std::exception& e) {
            fprintf(stderr, "config error: %s\n", e.what());
            ++problems;
        }
    }
#endif
    // S3 Tables on a single-gateway default backend with multi-gateway signals
    // (docs/s3-tables-design.md §5.5): the same line startup logs
    if (auto w = lights3::tables_deployment_warning(cfg)) fprintf(stderr, "config warning: %s\n", w->c_str());
    printf("config %s: %s\n", path.c_str(), problems ? "REJECTED" : "ok");
    printf("  http      driver=%s bind=%s:%u tls=%s metrics_access=%s\n", cfg.http.driver.c_str(),
           cfg.http.bind.c_str(), unsigned(cfg.http.port), cfg.http.tls_cert.empty() ? "off" : "on",
           cfg.http.metrics_access.c_str());
    if (cfg.http.admin_port >= 0)
        printf("  admin     bind=%s:%d (/-/ face served here; data-plane port answers 404)\n",
               (cfg.http.admin_bind.empty() ? cfg.http.bind : cfg.http.admin_bind).c_str(), cfg.http.admin_port);
    printf("  runtime   io_threads=%d max_inflight_requests=%d\n", cfg.runtime.io_threads,
           cfg.runtime.max_inflight_requests);
    printf("  auth      static_credentials=%zu credentials_file=%s region=%s\n", cfg.auth.credentials.size(),
           cfg.auth.credentials_file.empty() ? "-" : cfg.auth.credentials_file.c_str(), cfg.auth.region.c_str());
    printf("  backends  %zu\n", cfg.backends.size());
    for (auto& b : cfg.backends) {
        auto it = engines.find(b.name);
        printf("    - %s: type=%s%s%s\n", b.name.c_str(), b.type.c_str(), it == engines.end() ? "" : " ",
               it == engines.end() ? "" : it->second.c_str());
    }
    printf("  buckets   default_backend=%s rules=%zu\n", cfg.buckets.default_backend.c_str(), cfg.buckets.rules.size());
    printf("  website   static_entries=%zu\n", cfg.website.buckets.size());
    printf("  log       level=%s format=%s sink=%s async=%s slow_request_threshold=%dms\n", cfg.log.level.c_str(),
           cfg.log.format.c_str(), cfg.log.file.empty() ? "stderr" : cfg.log.file.c_str(), cfg.log.async ? "on" : "off",
           cfg.log.slow_request_threshold_ms);
    printf("  audit     %s\n", cfg.audit.path.empty() ? "off" : cfg.audit.path.c_str());
    return problems ? 1 : 0;
}

// A late startup failure unwinds through ~Application, which closes every
// backend already built: duostore's active pack gets sealed and rados flushed
// even on the error path (see Application::shutdown)
void run_server(const Cmd& c) {
    if (c->var<bool>("version")) {
        fputs(lights3::version_report("lights3").c_str(), stdout);
        return;
    }
    if (c->var<bool>("check-config")) {
        g_exit = check_config(c->var<std::string>("config"));
        return;
    }
    lights3::Application app(c->var<std::string>("config"));
    app.open_storage();
    app.start_server();
    g_exit = app.run();
}

}  // namespace lights3_cli
