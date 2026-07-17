// L3: bucket → 后端路由（glob 规则，声明序匹配，见 docs/04 §2）
#pragma once

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "core/config.h"
#include "storage/backend.h"

namespace lights3::storage {

class BucketRouter {
public:
    static BucketRouter build(const BucketsConfig& cfg,
                              std::map<std::string, std::shared_ptr<IStorageBackend>> backends);

    IStorageBackend& resolve(std::string_view bucket) const;
    const std::map<std::string, std::shared_ptr<IStorageBackend>>& backends() const {
        return backends_;
    }
    // 内部数据（如凭证持久化，docs/06 §4.1）固定落在默认后端
    std::shared_ptr<IStorageBackend> default_backend() const { return default_; }

private:
    struct Rule {
        std::string glob;
        std::shared_ptr<IStorageBackend> backend;
    };
    std::vector<Rule> rules_;
    std::shared_ptr<IStorageBackend> default_;
    std::map<std::string, std::shared_ptr<IStorageBackend>> backends_;
};

}  // namespace lights3::storage
