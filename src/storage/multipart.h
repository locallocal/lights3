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

// complete 前置校验：parts 非空抛 InvalidPart，分片号非严格递增抛 InvalidPartOrder
void validate_part_order(std::span<const PartInfo> parts);

// part_no ∈ [1,kMaxParts]，否则抛 InvalidArgument
void validate_part_number(int part_no);

// AWS 硬约束（docs/gaps.md §5.7）。最小分片只在 complete 时判定，且是 S3 协议
// 规则而非存储规则——因此校验点在 L2（handlers/multipart.cc），存储层不设限：
// 直接用后端 API 的调用方（含各后端一致性套件）不受 5MiB 约束
inline constexpr int kMaxParts = 10000;
inline constexpr uint64_t kMinPartSize = 5ull * 1024 * 1024;

}  // namespace lights3::storage
