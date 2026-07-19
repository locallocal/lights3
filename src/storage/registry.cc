#include "storage/registry.h"

#include <set>
#include <stdexcept>

#include "storage/localfs/localfs_backend.h"
#include "storage/memory/memory_backend.h"
#include "storage/tiered/tiered_backend.h"
#include "storage/xlocalfs/xlocalfs_backend.h"
#ifdef LIGHTS3_CLOUDPROXY
#include "storage/cloudproxy/cloudproxy_backend.h"
#endif

namespace lights3::storage {

namespace {

std::map<std::string, BackendFactory>& registry() {
    static std::map<std::string, BackendFactory> r;
    return r;
}

// localfs 与 xlocalfs 共用的 root/staging 参数解析
std::pair<std::string, std::string> fs_backend_paths(const BackendConfig& cfg) {
    auto root = cfg.params.count("root") ? cfg.params.at("root") : "";
    if (root.empty())
        throw std::runtime_error(cfg.type + " backend '" + cfg.name + "' needs root");
    auto staging = cfg.params.count("staging") ? cfg.params.at("staging")
                                               : root + "/.lights3-staging";
    return {root, staging};
}

void ensure_registered() {
    static bool done = [] {
        StorageRegistry::register_backend(
            "localfs", [](const BackendConfig& cfg, std::shared_ptr<ThreadPool> pool) {
                auto [root, staging] = fs_backend_paths(cfg);
                return std::make_shared<LocalFsBackend>(root, staging, std::move(pool));
            });
        StorageRegistry::register_backend(
            "xlocalfs", [](const BackendConfig& cfg, std::shared_ptr<ThreadPool> pool) {
                auto [root, staging] = fs_backend_paths(cfg);
                unsigned depth = cfg.params.count("queue_depth")
                                     ? std::stoul(cfg.params.at("queue_depth"))
                                     : 256;
                return std::make_shared<XLocalFsBackend>(root, staging, std::move(pool),
                                                         depth);
            });
        StorageRegistry::register_backend(
            "memory", [](const BackendConfig&, std::shared_ptr<ThreadPool>) {
                return std::make_shared<MemoryBackend>();
            });
#ifdef LIGHTS3_CLOUDPROXY
        StorageRegistry::register_backend(
            "cloudproxy", [](const BackendConfig& cfg, std::shared_ptr<ThreadPool> pool) {
                auto c = CloudProxyConfig::from_params(cfg.name, cfg.params);
                return std::make_shared<CloudProxyBackend>(std::move(c), std::move(pool));
            });
#endif
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
    // 两阶段构建（docs/08 §2）：先构造全部叶子后端，再按依赖迭代构造组合后端
    std::map<std::string, std::shared_ptr<IStorageBackend>> out;
    std::vector<const BackendConfig*> deferred;
    {
        std::set<std::string> names;
        for (auto& cfg : configs)
            if (!names.insert(cfg.name).second)
                throw std::runtime_error("duplicate backend name: " + cfg.name);
    }
    for (auto& cfg : configs) {
        if (cfg.type == "tiered") {
            deferred.push_back(&cfg);
            continue;
        }
        auto it = registry().find(cfg.type);
        if (it == registry().end())
            throw std::runtime_error("unknown storage backend type: " + cfg.type);
        out[cfg.name] = it->second(cfg, pool);
    }
    // tiered 引用 name 已就绪即可构造；tiered 套 tiered 按依赖序解开，解不开
    // 的（循环/未知引用）视为配置错误
    bool progress = true;
    while (!deferred.empty() && progress) {
        progress = false;
        for (auto it = deferred.begin(); it != deferred.end();) {
            auto& cfg = **it;
            auto local = cfg.params.count("local") ? cfg.params.at("local") : "";
            auto cloud = cfg.params.count("cloud") ? cfg.params.at("cloud") : "";
            if (local.empty() || cloud.empty())
                throw std::runtime_error("tiered backend '" + cfg.name + "' needs local + cloud");
            if (out.count(local) && out.count(cloud)) {
                out[cfg.name] = TieredBackend::from_config(cfg, out, pool);
                it = deferred.erase(it);
                progress = true;
            } else {
                ++it;
            }
        }
    }
    if (!deferred.empty())
        throw std::runtime_error("tiered backend '" + deferred.front()->name +
                                 "' has unknown or circular local/cloud reference");
    return out;
}

}  // namespace lights3::storage
