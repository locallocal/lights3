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

// multipart 两个列表的分页语义（docs/gaps.md §5.1）：全部后端共用一份实现，
// 免得"截断规则"在六个后端里各长一个样。
// 入参须已按各自的契约序排好（parts 按 part_no 升序，uploads 按 (key,upload_id)
// 升序）；能把 marker 下推给引擎的后端（sqlite/rocks/tikv）仍可先下推、取
// max+1 条再交给这里，语义完全一致。
// max<=0 一律返回空且 is_truncated=false——空 marker + truncated 会让按
// IsTruncated 循环续传的客户端原地死循环（同 apply_listing 的 max_keys=0）
ListPartsResult apply_parts_page(std::vector<PartMeta> sorted, const ListPartsOptions& opt);
ListUploadsResult apply_uploads_page(std::vector<UploadInfo> sorted,
                                     const ListUploadsOptions& opt);

// marker 比较：uploads 的游标是 (key, upload_id) 二元组
inline bool upload_after_marker(const UploadInfo& u, const ListUploadsOptions& opt) {
    if (opt.key_marker.empty() && opt.upload_id_marker.empty()) return true;
    if (u.key != opt.key_marker) return u.key > opt.key_marker;
    // 同 key：upload_id_marker 为空表示整个 key 都已翻过
    return opt.upload_id_marker.empty() ? false : u.upload_id > opt.upload_id_marker;
}

}  // namespace lights3::storage
