// Process entry point: flag parsing + Application lifecycle (src/app.h).
// Assembly and startup/shutdown ordering live in lights3::Application
// (docs/architecture.md §4)
#include <gflags/gflags.h>

#include "app.h"
#include "core/log.h"
#ifdef LIGHTS3_DUOSTORE
#include <fstream>

#include "storage/duostore/duostore_backend.h"
#endif

DEFINE_string(config, "config/lights3.yaml", "Path to the lights3 YAML config file");
DEFINE_string(duostore_admin, "",
              "duostore meta admin (docs/gaps.md §6.1): 'dump:<backend>:<file>' or "
              "'load:<backend>:<file>'. Runs before the server starts (no traffic) and "
              "exits; load ends with a forced orphan scan. Backup order: copy the data "
              "dir first, then dump meta; restore data first, then load.");

#ifdef LIGHTS3_DUOSTORE
namespace {

// --duostore_admin entry point: backends are built, the server has not
// started (write quiescence holds trivially). Returns the process exit code;
// any failure throws loudly and converges through main's fallback path
int run_duostore_admin(
    const std::string& spec,
    const std::map<std::string, std::shared_ptr<lights3::storage::IStorageBackend>>& backends) {
    using namespace lights3;
    auto c1 = spec.find(':');
    auto c2 = c1 == std::string::npos ? std::string::npos : spec.find(':', c1 + 1);
    if (c2 == std::string::npos)
        throw std::runtime_error("--duostore_admin expects dump:<backend>:<file> or "
                                 "load:<backend>:<file>, got: " + spec);
    std::string cmd = spec.substr(0, c1);
    std::string name = spec.substr(c1 + 1, c2 - c1 - 1);
    std::string path = spec.substr(c2 + 1);
    auto it = backends.find(name);
    if (it == backends.end())
        throw std::runtime_error("--duostore_admin: no backend named '" + name + "'");
    auto* duo = dynamic_cast<storage::DuoStoreBackend*>(it->second.get());
    if (!duo)
        throw std::runtime_error("--duostore_admin: backend '" + name + "' is not duostore");
    if (cmd == "dump") {
        std::ofstream f(path, std::ios::binary | std::ios::trunc);
        if (!f) throw std::runtime_error("--duostore_admin: cannot open for write: " + path);
        auto st = sync_wait(duo->run_meta_dump(f));
        LOG_INFO("duostore admin: dumped {} buckets / {} objects / {} sealed packs to {}",
                 st.buckets, st.objects, st.sealed_packs, path);
    } else if (cmd == "load") {
        std::ifstream f(path, std::ios::binary);
        if (!f) throw std::runtime_error("--duostore_admin: cannot open for read: " + path);
        auto st = sync_wait(duo->run_meta_load(f));
        LOG_INFO("duostore admin: loaded {} buckets / {} objects / {} sealed packs from {}",
                 st.buckets, st.objects, st.sealed_packs, path);
    } else {
        throw std::runtime_error("--duostore_admin: unknown command '" + cmd + "'");
    }
    return 0;
}

}  // namespace
#endif  // LIGHTS3_DUOSTORE

int main(int argc, char** argv) {
    using namespace lights3;

    gflags::SetUsageMessage("S3-compatible object storage server.\nusage: lights3 [--config <path>]");
    gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

    try {
        // A late startup failure unwinds through ~Application, which closes
        // every backend already built: duostore's active pack gets sealed and
        // rados flushed even on the error path (see Application::shutdown)
        Application app(FLAGS_config);
        app.open_storage();
        if (!FLAGS_duostore_admin.empty()) {
#ifdef LIGHTS3_DUOSTORE
            int rc = run_duostore_admin(FLAGS_duostore_admin, app.backends());
            app.shutdown();
            return rc;
#else
            throw std::runtime_error("--duostore_admin requires a build with LIGHTS3_DUOSTORE");
#endif
        }
        app.start_server();
        return app.run();
    } catch (const std::exception& e) {
        fprintf(stderr, "fatal: %s\n", e.what());
        return 1;
    }
}
