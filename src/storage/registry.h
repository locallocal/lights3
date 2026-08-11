// L3: 后端类型注册表：type 字符串 → 工厂（docs/storage-backend.md §6 扩展指南）
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

// scope 携带 backend=<name> 基础标签；工厂不消费指标时忽略即可
using BackendFactory = std::function<std::shared_ptr<IStorageBackend>(
    const BackendConfig&, std::shared_ptr<ThreadPool>, MetricsScope)>;

class StorageRegistry {
public:
    static void register_backend(const std::string& type, BackendFactory factory);

    // 按配置构造所有后端；返回 name → 实例。metrics 可空（单测装配径免注册表）
    static std::map<std::string, std::shared_ptr<IStorageBackend>> build(
        const std::vector<BackendConfig>& configs, std::shared_ptr<ThreadPool> pool,
        std::shared_ptr<MetricsRegistry> metrics = nullptr);
};

}  // namespace lights3::storage
