// L2/L3 边界：存储后端接口（见 docs/04-storage-backend.md）
// 错误约定：后端抛 s3::S3Error，不感知 HTTP。
#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "core/task.h"
#include "http/model.h"
#include "s3/errors.h"

namespace lights3::storage {

// HTTP Range 的三种形态：a-b(first,last) / a-(first) / -n(仅 last，后缀 n 字节)
struct ByteRange {
    std::optional<uint64_t> first;
    std::optional<uint64_t> last;
};

// 按对象大小解析为闭区间 [first,last]；不可满足时抛 InvalidRange(416)
std::pair<uint64_t, uint64_t> resolve_range(const ByteRange& r, uint64_t size);

struct ObjectMeta {
    std::string key;
    uint64_t size = 0;
    std::string etag;  // 未加引号的 hex
    std::string content_type = "binary/octet-stream";
    std::chrono::system_clock::time_point last_modified;
    std::map<std::string, std::string> user_meta;  // x-amz-meta-* 去前缀后的 kv
};

struct ObjectStream {
    ObjectMeta meta;                         // size 为对象全长
    std::unique_ptr<http::BodyReader> body;  // 已按 range 裁剪
    std::optional<ByteRange> range;          // 实际生效的 range（已解析 suffix/裁剪）
};

struct PutResult {
    std::string etag;
};

struct ListOptions {
    std::string prefix;
    std::string delimiter;    // 空或 "/"
    int max_keys = 1000;
    std::string start_after;  // continuation-token / start-after（key 值）
};

struct ListResult {
    std::vector<ObjectMeta> objects;
    std::vector<std::string> common_prefixes;
    bool is_truncated = false;
    std::string next_token;
};

struct BucketInfo {
    std::string name;
    std::chrono::system_clock::time_point created;
};

// CompleteMultipartUpload 请求中的一项：客户端声明的分片号与 ETag
struct PartInfo {
    int part_no = 0;
    std::string etag;  // 允许带引号，比较前统一去除
};

struct IStorageBackend {
    // ---- bucket ----
    virtual Task<void> create_bucket(std::string_view bucket) = 0;
    virtual Task<void> delete_bucket(std::string_view bucket) = 0;  // 须为空
    virtual Task<bool> bucket_exists(std::string_view bucket) = 0;
    virtual Task<std::vector<BucketInfo>> list_buckets() = 0;

    // ---- object ----
    virtual Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                          std::optional<ByteRange> range) = 0;
    virtual Task<PutResult> put_object(std::string_view bucket, std::string_view key,
                                       ObjectMeta meta, http::BodyReader& body) = 0;
    virtual Task<ObjectMeta> head_object(std::string_view bucket, std::string_view key) = 0;
    // S3 语义：对不存在的 key 也返回成功（幂等删除）
    virtual Task<void> delete_object(std::string_view bucket, std::string_view key) = 0;
    virtual Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) = 0;

    // ---- multipart（docs/04 §1/§3.2）----
    // 返回 upload_id；meta 为期望的 content_type/user_meta，complete 时生效
    virtual Task<std::string> create_multipart(std::string_view bucket, std::string_view key,
                                               ObjectMeta meta) = 0;
    // part_no ∈ [1,10000]；同号重传 last-write-wins；返回该分片的 ETag（内容 MD5）
    virtual Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                        std::string_view upload_id, int part_no,
                                        http::BodyReader& body) = 0;
    // parts 须分片号严格递增且 ETag 与已上传分片一致；
    // 总 ETag = md5(各分片 md5 二进制拼接)-N（与 S3 规则一致）
    virtual Task<PutResult> complete_multipart(std::string_view bucket, std::string_view key,
                                               std::string_view upload_id,
                                               std::span<const PartInfo> parts) = 0;
    virtual Task<void> abort_multipart(std::string_view bucket, std::string_view key,
                                       std::string_view upload_id) = 0;

    virtual Task<void> close() { co_return; }
    virtual ~IStorageBackend() = default;
};

// bucket/key 合法性校验（各后端共用），非法时抛 S3Error
void validate_bucket_name(std::string_view bucket);
void validate_object_key(std::string_view key);

}  // namespace lights3::storage
