// L3: localfs 系后端共用的落盘原语（tmp 文件、TSV sidecar/manifest、原子提交）。
// localfs 与 xlocalfs 共享同一磁盘布局（docs/04 §3.1/§3.2），差异仅在数据面 IO 方式。
#pragma once

#include <filesystem>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "storage/backend.h"

namespace lights3::storage::fsutil {

inline constexpr const char* kSidecarSuffix = ".lights3-meta";
inline constexpr const char* kBucketMarker = ".lights3-bucket";

std::string next_tmp_name();

// 未提交则析构时删除
struct TmpFile {
    std::filesystem::path path;
    int fd = -1;
    bool committed = false;
    ~TmpFile();
};

// key 不得使用内部保留名（sidecar/marker），避免与数据文件冲突
void reject_reserved_key(std::string_view key);

[[noreturn]] void throw_errno(const std::string& what);

// k<TAB>v 行格式，tmp+rename 原子写
void write_tsv(const std::filesystem::path& dest, const std::filesystem::path& tmp_dir,
               const std::vector<std::pair<std::string, std::string>>& kv);
std::vector<std::pair<std::string, std::string>> read_tsv(const std::filesystem::path& path);

// 建父目录 + 目录冲突检查 + 先 sidecar 后数据 rename（docs/04 §3.1 写入原子性）；
// PUT 与 complete_multipart 共用
void commit_object_file(const std::filesystem::path& dest, TmpFile& tmp, const ObjectMeta& meta,
                        const std::filesystem::path& staging_put, std::string_view key);

// ---- multipart 布局（docs/04 §3.2）：<staging>/mpu/<id>/{manifest, part.NNNNN, .md5} ----

std::string part_file_name(int part_no);

struct UploadState {
    std::filesystem::path dir;
    ObjectMeta meta;  // manifest 中记录的 content_type / user_meta
};

// upload_id 合法性 + manifest 存在 + bucket/key 匹配，任一不满足视为 NoSuchUpload
UploadState require_upload(const std::filesystem::path& staging, std::string_view bucket,
                           std::string_view key, std::string_view upload_id,
                           const std::vector<std::pair<std::string, std::string>>& manifest);

// 读 manifest 前先做 id 格式与存在性检查（id 会拼进路径，格式校验兼防逃逸）
std::vector<std::pair<std::string, std::string>> load_manifest(
    const std::filesystem::path& staging, std::string_view upload_id);

}  // namespace lights3::storage::fsutil
