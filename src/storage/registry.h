// L3: backend type registry: type string → factory (docs/storage-backend.md §6 extension guide)
#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>

#include "core/config.h"
#include "core/metrics.h"
#include "core/thread_pool.h"
#include "storage/backend.h"

namespace lights3::storage {

// scope carries the backend=<name> base label; factories that don't consume metrics can ignore it
using BackendFactory = std::function<std::shared_ptr<IStorageBackend>(
    const BackendConfig&, std::shared_ptr<ThreadPool>, MetricsScope)>;

class StorageRegistry {
public:
    static void register_backend(const std::string& type, BackendFactory factory);
    // Registered type names, sorted (`lights3 --check-config` validates backends[].type
    // against it without opening anything, roadmap §6.2)
    static std::vector<std::string> registered_types();

    // Construct all backends per config; returns name → instance. metrics may be null
    // (unit-test assembly path skips the registry)
    static std::map<std::string, std::shared_ptr<IStorageBackend>> build(
        const std::vector<BackendConfig>& configs, std::shared_ptr<ThreadPool> pool,
        std::shared_ptr<MetricsRegistry> metrics = nullptr);
};

}  // namespace lights3::storage
