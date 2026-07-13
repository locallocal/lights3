// L3: ListObjects 的共享分页/delimiter 逻辑（memory 与 localfs 复用）
#pragma once

#include <functional>
#include <string>
#include <vector>

#include "storage/backend.h"

namespace lights3::storage {

// sorted_keys 必须已按字典序排序；fetch 只对最终返回的 key 调用
ListResult apply_listing(const std::vector<std::string>& sorted_keys, const ListOptions& opt,
                         const std::function<ObjectMeta(const std::string&)>& fetch);

}  // namespace lights3::storage
