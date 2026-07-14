// L3: multipart 的后端无关辅助（upload_id 生成、拼接 ETag、parts 校验）
#pragma once

#include <string>
#include <string_view>
#include <vector>

#include "storage/backend.h"

namespace lights3::storage {

// 32 个随机 hex 字符；用作 upload_id（也是 localfs 的 mpu 目录名）
std::string new_upload_id();
bool is_valid_upload_id(std::string_view id);

// S3 multipart 总 ETag 规则：md5(各分片 md5 二进制拼接) 的 hex + "-N"
std::string combined_etag(const std::vector<std::string>& part_md5_hex);

// 去掉 ETag 两侧的引号（客户端可能带 W3C 引号形式）
std::string_view strip_etag_quotes(std::string_view etag);

// complete 前置校验：parts 非空且分片号严格递增，否则抛 InvalidPart
void validate_part_order(std::span<const PartInfo> parts);

// part_no ∈ [1,10000]，否则抛 InvalidArgument
void validate_part_number(int part_no);

}  // namespace lights3::storage
