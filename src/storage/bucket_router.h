// L3: bucket → 后端路由（glob 规则，声明序匹配，见 docs/storage-backend.md §2）。
// 构建期校验（docs/gaps.md §6.3）：glob 语法坏 / 含桶名不可能出现的字面字符 /
// 规则不可达（跟在 catch-all 之后、与先前规则重复）都在启动时报错——写错的
// pattern 静默永不匹配会把桶悄悄路由去默认后端。"!pattern" 为否定规则。
// key 前缀路由刻意不做：桶级操作（list/delete-bucket）无法跨后端聚合，允许一个
// 桶横跨两个后端会破坏它们的原子性与一致性语义
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
    // 内部数据（如凭证持久化，docs/credential-management.md §4.1）固定落在默认后端
    std::shared_ptr<IStorageBackend> default_backend() const { return default_; }

private:
    struct Rule {
        std::string glob;
        bool negate = false;  // "!pattern"：不匹配 pattern 的桶命中本规则
        std::shared_ptr<IStorageBackend> backend;
    };
    std::vector<Rule> rules_;
    std::shared_ptr<IStorageBackend> default_;
    std::map<std::string, std::shared_ptr<IStorageBackend>> backends_;
};

}  // namespace lights3::storage
