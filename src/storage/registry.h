// L3: 后端类型注册表：type 字符串 → 工厂（docs/04 §5 扩展指南）
#pragma once

#include <functional>
#include <map>
#include <memory>
#include <string>

#include "core/config.h"
#include "core/thread_pool.h"
#include "storage/backend.h"

namespace lights3::storage {

using BackendFactory = std::function<std::shared_ptr<IStorageBackend>(
    const BackendConfig&, std::shared_ptr<ThreadPool>)>;

class StorageRegistry {
public:
    static void register_backend(const std::string& type, BackendFactory factory);

    // 按配置构造所有后端；返回 name → 实例
    static std::map<std::string, std::shared_ptr<IStorageBackend>> build(
        const std::vector<BackendConfig>& configs, std::shared_ptr<ThreadPool> pool);
};

}  // namespace lights3::storage
