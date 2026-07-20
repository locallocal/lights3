// L3: localfs 系后端共用的落盘原语（tmp 文件、TSV sidecar/manifest、原子提交）。
// localfs 与 xlocalfs 共享同一磁盘布局（docs/storage-backend.md §3.1/§3.2），差异仅在数据面 IO 方式。
#pragma once

#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/thread_pool.h"
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

// 建父目录 + 目录冲突检查 + 先 sidecar 后数据 rename（docs/storage-backend.md §3.1 写入原子性）；
// PUT 与 complete_multipart 共用
void commit_object_file(const std::filesystem::path& dest, TmpFile& tmp, const ObjectMeta& meta,
                        const std::filesystem::path& staging_put, std::string_view key);

// ---- 分层存储的 sidecar 扩展（docs/tiered-storage.md §4）----

enum class Tier { kLocal, kRemote, kCached };

struct TierInfo {
    Tier tier = Tier::kLocal;
    std::string remote_etag;  // 云端副本 ETag（去引号 hex；校验与 GC 用，不外泄）
    std::string remote_at;    // 上传时间（iso8601）
};

// stat 数据文件 + 读 sidecar；tier != local 时 size 以 sidecar 为准
//（stub 数据文件为 0 长度，docs/tiered-storage.md §4.1）。缺失/非普通文件抛 NoSuchKey。
ObjectMeta load_object_meta(const std::filesystem::path& data_path, std::string key,
                            TierInfo* tier_out = nullptr);

// GET 在 open(data) 与读 sidecar 之间对象被 stub 化：持有的 fd 是 0 长度新
// inode，而 sidecar 宣称 size>0。TieredBackend 捕获后改走云端重试；
// 独立 localfs 遇到（误配到 tiered 布局）则按 InternalError 映射 500。
struct StubRace : s3::S3Error {
    explicit StubRace(std::string key)
        : S3Error(s3::S3ErrorCode::InternalError, "object is a tier stub", std::move(key)) {}
};

// stub 化提交（docs/tiered-storage.md §5.2 步骤 b/c）：先写 tier=remote 的 sidecar，
// 再用 0 长度 tmp rename 盖过数据文件。幂等；调用方须持 per-key 锁。
void commit_stub(const std::filesystem::path& dest, const ObjectMeta& meta, const TierInfo& tier,
                 const std::filesystem::path& staging_put);

// 缓存回填提交（docs/tiered-storage.md §6.2）：先 rename 数据 tmp、再写 tier=cached 的 sidecar
//（中间崩溃时 sidecar 仍为 remote，读走云端不受影响）。
// dest 此前必为 stub（父目录已存在），不再做目录冲突检查。
void commit_cached(const std::filesystem::path& dest, TmpFile& tmp, const ObjectMeta& meta,
                   const TierInfo& tier, const std::filesystem::path& staging_put);

// pread 流式读取；每块经线程池执行（阻塞 IO 不占 HTTP 执行环境）。
// fd 所有权移交本 reader；文件被外部截断时提前 EOF。
// localfs GET 与 tiered 下沉上传共用。
class FdStreamReader final : public http::BodyReader {
public:
    FdStreamReader(int fd, uint64_t offset, uint64_t remaining, std::shared_ptr<ThreadPool> pool)
        : fd_(fd), offset_(offset), remaining_(remaining), total_(remaining),
          pool_(std::move(pool)) {}
    ~FdStreamReader() override;

    Task<size_t> read(std::span<std::byte> buf) override;
    std::optional<uint64_t> length() const override { return total_; }

private:
    int fd_;
    uint64_t offset_;
    uint64_t remaining_;
    uint64_t total_;
    std::shared_ptr<ThreadPool> pool_;
};

// ---- multipart 布局（docs/storage-backend.md §3.2）：<staging>/mpu/<id>/{manifest, part.NNNNN, .md5} ----

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
