#include "storage/registry.h"

#include <stdexcept>

#include "storage/localfs/localfs_backend.h"
#include "storage/memory/memory_backend.h"

namespace lights3::storage {

namespace {

std::map<std::string, BackendFactory>& registry() {
    static std::map<std::string, BackendFactory> r;
    return r;
}

void ensure_registered() {
    static bool done = [] {
        StorageRegistry::register_backend(
            "localfs", [](const BackendConfig& cfg, std::shared_ptr<ThreadPool> pool) {
                auto root = cfg.params.count("root") ? cfg.params.at("root") : "";
                if (root.empty())
                    throw std::runtime_error("localfs backend '" + cfg.name + "' needs root");
                auto staging = cfg.params.count("staging") ? cfg.params.at("staging")
                                                           : root + "/.lights3-staging";
                return std::make_shared<LocalFsBackend>(root, staging, std::move(pool));
            });
        StorageRegistry::register_backend(
            "memory", [](const BackendConfig&, std::shared_ptr<ThreadPool>) {
                return std::make_shared<MemoryBackend>();
            });
        // cloudproxy：依赖云 SDK，后续 CMake 选项接入（docs/04 §4）
        return true;
    }();
    (void)done;
}

}  // namespace

void StorageRegistry::register_backend(const std::string& type, BackendFactory factory) {
    registry()[type] = std::move(factory);
}

std::map<std::string, std::shared_ptr<IStorageBackend>> StorageRegistry::build(
    const std::vector<BackendConfig>& configs, std::shared_ptr<ThreadPool> pool) {
    ensure_registered();
    std::map<std::string, std::shared_ptr<IStorageBackend>> out;
    for (auto& cfg : configs) {
        auto it = registry().find(cfg.type);
        if (it == registry().end())
            throw std::runtime_error("unknown storage backend type: " + cfg.type);
        if (out.count(cfg.name))
            throw std::runtime_error("duplicate backend name: " + cfg.name);
        out[cfg.name] = it->second(cfg, pool);
    }
    return out;
}

}  // namespace lights3::storage
