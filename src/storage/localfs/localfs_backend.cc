#include "storage/localfs/localfs_backend.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <cstring>
#include <fstream>
#include <system_error>
#include <tuple>

#include "core/log.h"
#include "core/util/crypto.h"
#include "storage/listing.h"
#include "storage/multipart.h"

namespace fs = std::filesystem;

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;

// 落盘原语在 fs_util 中，与 xlocalfs 共用
using fsutil::TmpFile;
using fsutil::commit_object_file;
using fsutil::load_manifest;
using fsutil::next_tmp_name;
using fsutil::part_file_name;
using fsutil::read_tsv;
using fsutil::reject_reserved_key;
using fsutil::require_upload;
using fsutil::throw_errno;
using fsutil::write_tsv;

LocalFsBackend::LocalFsBackend(fs::path root, fs::path staging, std::shared_ptr<ThreadPool> pool)
    : root_(std::move(root)), staging_(std::move(staging)), pool_(std::move(pool)) {
    fs::create_directories(root_);
    fs::create_directories(staging_ / "put");
    fs::create_directories(staging_ / "mpu");
    commit_locks_.reserve(kLockStripes);
    for (size_t i = 0; i < kLockStripes; ++i)
        commit_locks_.push_back(std::make_unique<AsyncSemaphore>(1));
    cleanup_stale_uploads();
}

AsyncSemaphore& LocalFsBackend::commit_lock(std::string_view bucket, std::string_view key) {
    size_t h = std::hash<std::string_view>()(bucket) * 1315423911u ^
               std::hash<std::string_view>()(key);
    return *commit_locks_[h % kLockStripes];
}

fs::path LocalFsBackend::bucket_dir(std::string_view bucket) const {
    return root_ / fs::path(std::string(bucket));
}

fs::path LocalFsBackend::object_path(std::string_view bucket, std::string_view key) const {
    fs::path p = bucket_dir(bucket) / fs::path(std::string(key));
    // 纵深防御（docs/gaps.md §1.1）：bucket/key 已在 L2 与各入口校验过，路径不该
    // 逃出 root_。但 fs::path::operator/ 遇绝对路径右操作数会**替换整条路径**
    // （root_ / "/etc" == "/etc"），单点失误的代价是任意文件读——所以在真正
    // 触碰文件系统之前再确认一次。lexically_normal 折叠 ".."，无需触盘
    fs::path norm = p.lexically_normal();
    fs::path base = root_.lexically_normal();
    auto [it, _] = std::mismatch(base.begin(), base.end(), norm.begin(), norm.end());
    if (it != base.end())
        throw S3Error(S3ErrorCode::InvalidBucketName,
                      "The specified bucket is not valid.", std::string(bucket));
    return p;
}

void LocalFsBackend::require_bucket(std::string_view bucket) const {
    if (!fs::exists(bucket_dir(bucket) / kBucketMarker))
        throw S3Error(S3ErrorCode::NoSuchBucket, "The specified bucket does not exist",
                      std::string(bucket));
}

ObjectMeta LocalFsBackend::load_meta(const fs::path& data_path, std::string key) const {
    // tier 感知（stub 的 size 以 sidecar 为准）在共享实现里处理（docs/tiered-storage.md §4.1）
    return fsutil::load_object_meta(data_path, std::move(key));
}

// ---------- bucket ----------

Task<void> LocalFsBackend::create_bucket(std::string_view bucket) {
    validate_bucket_name(bucket, kAllowReserved);
    co_await pool_->schedule();
    fs::path dir = bucket_dir(bucket);
    if (fs::exists(dir / kBucketMarker))
        throw S3Error(S3ErrorCode::BucketAlreadyOwnedByYou, "Bucket already exists",
                      std::string(bucket));
    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) throw S3Error(S3ErrorCode::InternalError, "create bucket dir: " + ec.message());
    std::ofstream marker(dir / kBucketMarker);
    if (!marker) throw_errno("create bucket marker");
    co_return;
}

Task<void> LocalFsBackend::delete_bucket(std::string_view bucket) {
    validate_bucket_name(bucket, kAllowReserved);
    co_await pool_->schedule();
    require_bucket(bucket);
    fs::path dir = bucket_dir(bucket);
    for (auto& e : fs::directory_iterator(dir)) {
        if (e.path().filename() != kBucketMarker)
            throw S3Error(S3ErrorCode::BucketNotEmpty,
                          "The bucket you tried to delete is not empty", std::string(bucket));
    }
    fs::remove(dir / kBucketMarker);
    fs::remove(dir);
    co_return;
}

Task<bool> LocalFsBackend::bucket_exists(std::string_view bucket) {
    validate_bucket_name(bucket, kAllowReserved);
    co_await pool_->schedule();
    co_return fs::exists(bucket_dir(bucket) / kBucketMarker);
}

Task<std::vector<BucketInfo>> LocalFsBackend::list_buckets() {
    co_await pool_->schedule();
    std::vector<BucketInfo> out;
    for (auto& e : fs::directory_iterator(root_)) {
        if (!e.is_directory()) continue;
        struct stat st{};
        fs::path marker = e.path() / kBucketMarker;
        if (::stat(marker.c_str(), &st) != 0) continue;
        out.push_back({e.path().filename().string(),
                       std::chrono::system_clock::from_time_t(st.st_mtime)});
    }
    std::sort(out.begin(), out.end(),
              [](const BucketInfo& a, const BucketInfo& b) { return a.name < b.name; });
    co_return out;
}

// ---------- object ----------

Task<PutResult> LocalFsBackend::put_object(std::string_view bucket, std::string_view key,
                                           ObjectMeta meta, http::BodyReader& body,
                                           PutCondition cond) {
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);

    // 1. 流式写入 staging 临时文件，边写边算 MD5
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_errno("open staging tmp");

    util::HashStream md5(util::HashStream::Algo::Md5);
    uint64_t total = 0;
    std::byte buf[64 * 1024];
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf), n));
        const char* p = reinterpret_cast<const char*>(buf);
        size_t left = n;
        while (left > 0) {
            ssize_t w = ::write(tmp.fd, p, left);
            if (w < 0) throw_errno("write staging tmp");
            p += w;
            left -= static_cast<size_t>(w);
        }
        total += n;
    }
    ::close(tmp.fd);
    tmp.fd = -1;

    meta.key = std::string(key);
    meta.size = total;
    meta.etag = md5.final_hex();
    meta.last_modified = std::chrono::system_clock::now();

    // 2. 冲突检查 + 数据 rename + sidecar 提交。per-key 锁：提交段是两次
    // rename，并发同 key PUT 交错会产生"数据=A、etag=B"的撕裂对象。
    // 条件 PUT 的检查同在锁内（PutCondition 契约）：检查与提交对并发写者原子
    auto lk = co_await commit_lock(bucket, key).acquire();
    co_await pool_->schedule();  // 锁唤醒可能在别的线程恢复，阻塞 IO 回池线程做
    fsutil::check_put_condition(object_path(bucket, key), cond, key);
    commit_object_file(object_path(bucket, key), tmp, meta, staging_ / "put", key);
    co_return PutResult{meta.etag};
}

Task<ObjectStream> LocalFsBackend::get_object(std::string_view bucket, std::string_view key,
                                              std::optional<ByteRange> range) {
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    // 桶存在性是无条件前置：此前只在 open 失败分支才查，成功路径完全不检查桶，
    // 于是任何能构造出落在 root_ 之外的路径的 bucket 都能直接读到文件
    require_bucket(bucket);

    fs::path path = object_path(bucket, key);
    int fd = ::open(path.c_str(), O_RDONLY);
    if (fd < 0)
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    struct stat st{};
    if (::fstat(fd, &st) != 0 || !S_ISREG(st.st_mode)) {
        ::close(fd);
        throw S3Error(S3ErrorCode::NoSuchKey, "The specified key does not exist",
                      std::string(key));
    }

    ObjectStream out;
    try {
        fsutil::TierInfo tier;
        // 用已打开 fd 的 fstat（而非对路径二次 stat）：并发覆盖写后路径指向新
        // inode，size/mtime 会与 fd 持有的旧 body 错位（短包或截断，静默损坏）
        out.meta = fsutil::load_object_meta_stat(path, std::string(key), st, &tier);
        // open 与读 sidecar 之间被 stub 化：fd 指向 0 长度新 inode，无法兑现
        // sidecar 宣称的 size——报给 tiered 改走云端（docs/tiered-storage.md §7.3 冲突矩阵）
        if (tier.tier != fsutil::Tier::kLocal && out.meta.size > 0 &&
            static_cast<uint64_t>(st.st_size) != out.meta.size)
            throw fsutil::StubRace(std::string(key));
        uint64_t f = 0, l = out.meta.size ? out.meta.size - 1 : 0;
        uint64_t len = out.meta.size;
        if (range) {
            std::tie(f, l) = resolve_range(*range, out.meta.size);
            out.range = ByteRange{f, l};
            len = l - f + 1;
        } else if (out.meta.size == 0) {
            len = 0;
        }
        out.body = std::make_unique<fsutil::FdStreamReader>(fd, f, len, pool_);  // fd 所有权移交
    } catch (...) {
        ::close(fd);
        throw;
    }
    co_return out;
}

Task<ObjectMeta> LocalFsBackend::head_object(std::string_view bucket, std::string_view key) {
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);
    co_return load_meta(object_path(bucket, key), std::string(key));
}

Task<void> LocalFsBackend::delete_object(std::string_view bucket, std::string_view key) {
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);

    fs::path path = object_path(bucket, key);
    std::error_code ec;
    fs::remove(path, ec);                             // 幂等：不存在也算成功
    fs::remove(path.string() + kSidecarSuffix, ec);
    // 清理空父目录直到 bucket 根
    fs::path dir = path.parent_path(), root = bucket_dir(bucket);
    while (dir != root && dir.string().size() > root.string().size()) {
        if (!fs::is_empty(dir, ec) || ec) break;
        fs::remove(dir, ec);
        if (ec) break;
        dir = dir.parent_path();
    }
    co_return;
}

namespace {

// list_objects 的有序目录遍历（docs/gaps.md §2.7 剪枝）。
// 目录条目的排序键：文件用 name，目录用 name+"/"——其下所有 key 均以该串为前缀，
// 混排后的输出顺序与"全量收集 + 全排序"逐字节一致，无需物化整棵树。
struct ListWalker {
    const std::string& prefix;
    const std::string& start_after;  // 仅严格大于此 key 的条目可见
    // 返回 false 即整体终止（截断）；回调可设置 skip_prefix 以跳过 delimiter 组
    std::function<bool(std::string&&)> on_key;
    std::string skip_prefix;  // 非空：凡以此为前缀的 key/子树整体跳过
    bool stopped = false;

    // 子树（key 前缀 q）是否可能含有匹配的 key：与 prefix 相交，
    // 且 start_after 不整体大于该子树的全部 key
    bool subtree_may_match(const std::string& q) const {
        size_t n = std::min(q.size(), prefix.size());
        if (q.compare(0, n, prefix, 0, n) != 0) return false;
        if (!start_after.empty() && start_after.compare(0, q.size(), q) > 0) return false;
        return true;
    }

    // rel：该目录对应的 key 前缀（"" 或 "a/b/"）
    void walk(const fs::path& dir, const std::string& rel) {
        struct Entry {
            std::string sort_key;  // 文件：name；目录：name+"/"
            bool is_dir;
        };
        std::vector<Entry> es;
        std::error_code ec;
        for (auto& e : fs::directory_iterator(dir, ec)) {
            std::string name = e.path().filename().string();
            std::error_code tec;
            if (e.is_directory(tec)) {
                es.push_back({name + "/", true});
                continue;
            }
            if (name == fsutil::kBucketMarker) continue;
            if (name.ends_with(fsutil::kSidecarSuffix)) {
                // 孤儿 sidecar 自愈：delete_object 是"先删数据后删 sidecar"两步，
                // 之间崩溃会遗留一直占空间的孤儿。剪枝后只覆盖被访问到的目录
                //（best-effort，未访问部分留待后续 list）
                std::string data = e.path().string();
                data.resize(data.size() - std::strlen(fsutil::kSidecarSuffix));
                if (!fs::exists(data, tec)) fs::remove(e.path(), tec);
                continue;
            }
            if (e.is_regular_file(tec)) es.push_back({std::move(name), false});
        }
        std::sort(es.begin(), es.end(),
                  [](const Entry& a, const Entry& b) { return a.sort_key < b.sort_key; });

        for (auto& e : es) {
            if (stopped) return;
            std::string q = rel + e.sort_key;  // 文件：完整 key；目录：子树前缀
            // delimiter 组跳过：整项落在组内直接略过；组前缀在子树内部继续
            //（skip_prefix 比 q 长且以 q 开头）时仍需下钻
            if (!skip_prefix.empty()) {
                if (q.compare(0, skip_prefix.size(), skip_prefix) == 0) continue;
                if (!(e.is_dir && skip_prefix.compare(0, q.size(), q) == 0))
                    skip_prefix.clear();
            }
            if (e.is_dir) {
                if (!subtree_may_match(q)) continue;
                fs::path sub = dir / e.sort_key.substr(0, e.sort_key.size() - 1);
                walk(sub, q);
                continue;
            }
            if (q.compare(0, prefix.size(), prefix) != 0) {
                if (q > prefix) return;  // 已排序：出 prefix 区间即止（本目录层面）
                continue;
            }
            if (!start_after.empty() && q <= start_after) continue;
            if (!on_key(std::move(q))) {
                stopped = true;
                return;
            }
        }
    }
};

// 以 want 为前缀的最大 key（降序扫描，首个命中即最大）；无则空串。
// 截断边界落在 common prefix 上时用它求组尾，保持 next_token 语义
//（"组内最后一个 key"）与全量实现一致
std::string max_key_with_prefix(const fs::path& dir, const std::string& rel,
                                const std::string& want) {
    struct Entry {
        std::string sort_key;
        bool is_dir;
    };
    std::vector<Entry> es;
    std::error_code ec;
    for (auto& e : fs::directory_iterator(dir, ec)) {
        std::string name = e.path().filename().string();
        std::error_code tec;
        if (e.is_directory(tec)) es.push_back({name + "/", true});
        else if (name != fsutil::kBucketMarker && !name.ends_with(fsutil::kSidecarSuffix) &&
                 e.is_regular_file(tec))
            es.push_back({std::move(name), false});
    }
    std::sort(es.begin(), es.end(),
              [](const Entry& a, const Entry& b) { return a.sort_key > b.sort_key; });
    for (auto& e : es) {
        std::string q = rel + e.sort_key;
        if (e.is_dir) {
            // 子树整体在组内（q 以 want 开头），或组前缀穿过该子树（want 以 q 开头）
            if (q.compare(0, want.size(), want) != 0 && want.compare(0, q.size(), q) != 0)
                continue;
            fs::path sub = dir / e.sort_key.substr(0, e.sort_key.size() - 1);
            auto r = max_key_with_prefix(sub, q, want);
            if (!r.empty()) return r;
        } else if (q.compare(0, want.size(), want) == 0) {
            return q;
        }
    }
    return {};
}

}  // namespace

Task<ListResult> LocalFsBackend::list_objects(std::string_view bucket, const ListOptions& opt) {
    validate_bucket_name(bucket, kAllowReserved);
    co_await pool_->schedule();
    require_bucket(bucket);

    fs::path base = bucket_dir(bucket);
    ListResult out;
    // S3：max-keys=0 返回空结果且 IsTruncated=false（与 apply_listing 一致）
    if (opt.max_keys <= 0) co_return out;

    // prefix 剪枝：最后一个 '/' 之前的部分直接定位起始目录，不存在即无匹配
    std::string dir_rel;
    if (auto slash = opt.prefix.rfind('/'); slash != std::string::npos)
        dir_rel = opt.prefix.substr(0, slash + 1);
    fs::path start_dir = dir_rel.empty() ? base : base / fs::path(dir_rel);
    std::error_code ec;
    if (!fs::is_directory(start_dir, ec)) co_return out;

    const std::string& delim = opt.delimiter;
    int count = 0;
    std::string last_emitted;   // 最后发出的条目（key 或组名）
    bool last_is_group = false;

    ListWalker walker{opt.prefix, opt.start_after, nullptr, {}, false};
    auto truncate = [&] {
        out.is_truncated = true;
        // 组尾即 token（与全量实现一致）；并发删除使组消失时回落组名本身
        if (last_is_group) {
            std::string tail = max_key_with_prefix(start_dir, dir_rel, last_emitted);
            out.next_token = tail.empty() ? last_emitted : tail;
        } else {
            out.next_token = last_emitted;
        }
    };
    walker.on_key = [&](std::string&& key) {
        if (!delim.empty()) {
            auto pos = key.find(delim, opt.prefix.size());
            if (pos != std::string::npos) {
                std::string group = key.substr(0, pos + delim.size());
                if (count >= opt.max_keys) {
                    truncate();
                    return false;
                }
                out.common_prefixes.push_back(group);
                walker.skip_prefix = group;  // 同组其余 key/子树整体剪掉
                last_emitted = std::move(group);
                last_is_group = true;
                ++count;
                return true;
            }
        }
        if (count >= opt.max_keys) {
            truncate();
            return false;
        }
        out.objects.push_back(load_meta(base / fs::path(key), key));
        last_emitted = std::move(key);
        last_is_group = false;
        ++count;
        return true;
    };
    walker.walk(start_dir, dir_rel);
    co_return out;
}

// ---------- multipart（docs/storage-backend.md §3.2）----------
// 布局：<staging>/mpu/<upload_id>/{manifest, part.NNNNN, part.NNNNN.md5}
// 分片先 fsync 数据、后写 .md5：.md5 的出现即分片数据已持久（complete 信任
// .md5 不重算校验和，该信任必须以此顺序为前提）

Task<std::string> LocalFsBackend::create_multipart(std::string_view bucket,
                                                   std::string_view key, ObjectMeta meta) {
    validate_bucket_name(bucket, kAllowReserved);
    validate_object_key(key);
    reject_reserved_key(key);
    co_await pool_->schedule();
    require_bucket(bucket);

    std::string id = new_upload_id();
    fs::path dir = staging_ / "mpu" / id;
    std::error_code ec;
    fs::create_directories(dir, ec);
    if (ec) throw S3Error(S3ErrorCode::InternalError, "create mpu dir: " + ec.message());

    std::vector<std::pair<std::string, std::string>> kv{
        {"bucket", std::string(bucket)},
        {"key", std::string(key)},
        {"content_type", meta.content_type}};
    for (auto& [k, v] : meta.user_meta) kv.emplace_back("meta." + k, v);
    write_tsv(dir / "manifest", staging_ / "put", kv);
    co_return id;
}

Task<PutResult> LocalFsBackend::upload_part(std::string_view bucket, std::string_view key,
                                            std::string_view upload_id, int part_no,
                                            http::BodyReader& body) {
    validate_part_number(part_no);
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));

    // 流式写 staging 临时文件，边写边算分片 MD5（同 PUT）
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_errno("open part tmp");

    util::HashStream md5(util::HashStream::Algo::Md5);
    std::byte buf[64 * 1024];
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        if (n == 0) break;
        md5.update(std::span(reinterpret_cast<const uint8_t*>(buf), n));
        const char* p = reinterpret_cast<const char*>(buf);
        size_t left = n;
        while (left > 0) {
            ssize_t w = ::write(tmp.fd, p, left);
            if (w < 0) throw_errno("write part tmp");
            p += w;
            left -= static_cast<size_t>(w);
        }
    }
    fsutil::fsync_file(tmp.fd);  // 分片数据先落盘：.md5 的出现才是数据已持久的证据
    ::close(tmp.fd);
    tmp.fd = -1;
    std::string etag = md5.final_hex();

    // 顺序：先数据 rename、后写 .md5（同号重传 last-write-wins，rename 覆盖）。
    // 反序（旧实现）下"分片 200 → 掉电"会留下 fsync 过的 .md5 配零块数据，
    // 而 complete 信任 .md5 不重算，产出 ETag 合法、内容为零块的对象
    std::string name = part_file_name(part_no);
    std::error_code ec;
    fs::rename(tmp.path, up.dir / name, ec);
    if (ec) {
        // 读 body 期间上传可能已被 abort（目录被删）
        if (!fs::exists(up.dir))
            throw S3Error(S3ErrorCode::NoSuchUpload,
                          "The specified multipart upload does not exist.",
                          std::string(upload_id));
        throw S3Error(S3ErrorCode::InternalError, "rename part failed");
    }
    tmp.committed = true;
    fsutil::fsync_dir(up.dir);
    write_tsv(up.dir / (name + ".md5"), staging_ / "put", {{"md5", etag}});
    co_return PutResult{etag};
}

Task<PutResult> LocalFsBackend::complete_multipart(std::string_view bucket,
                                                   std::string_view key,
                                                   std::string_view upload_id,
                                                   std::span<const PartInfo> parts) {
    validate_part_order(parts);
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));
    require_bucket(bucket);

    // 1. 校验每个声明的分片：存在且 ETag 匹配
    std::vector<std::string> md5s;
    std::vector<fs::path> paths;
    md5s.reserve(parts.size());
    for (auto& p : parts) {
        std::string name = part_file_name(p.part_no);
        std::string stored;
        for (auto& [k, v] : read_tsv(up.dir / (name + ".md5")))
            if (k == "md5") stored = v;
        if (stored.empty() || !fs::exists(up.dir / name) ||
            stored != strip_etag_quotes(p.etag))
            throw S3Error(S3ErrorCode::InvalidPart,
                          "One or more of the specified parts could not be found or the "
                          "ETag did not match.",
                          std::string(key));
        md5s.push_back(stored);
        paths.push_back(up.dir / name);
    }

    // 2. 按声明顺序拼接到最终临时文件
    TmpFile tmp{staging_ / "put" / next_tmp_name()};
    tmp.fd = ::open(tmp.path.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0644);
    if (tmp.fd < 0) throw_errno("open complete tmp");
    uint64_t total = 0;
    std::vector<char> buf(256 * 1024);
    for (auto& path : paths) {
        int in = ::open(path.c_str(), O_RDONLY);
        if (in < 0) throw_errno("open part");
        for (;;) {
            ssize_t n = ::read(in, buf.data(), buf.size());
            if (n < 0) {
                ::close(in);
                throw_errno("read part");
            }
            if (n == 0) break;
            const char* p = buf.data();
            size_t left = static_cast<size_t>(n);
            while (left > 0) {
                ssize_t w = ::write(tmp.fd, p, left);
                if (w < 0) {
                    ::close(in);
                    throw_errno("write complete tmp");
                }
                p += w;
                left -= static_cast<size_t>(w);
            }
            total += static_cast<uint64_t>(n);
        }
        ::close(in);
    }
    ::close(tmp.fd);
    tmp.fd = -1;

    // 3. 提交（与 PUT 相同的原子路径），随后清理 mpu 目录
    ObjectMeta meta = std::move(up.meta);
    meta.key = std::string(key);
    meta.size = total;
    meta.etag = combined_etag(md5s);
    meta.last_modified = std::chrono::system_clock::now();
    {
        auto lk = co_await commit_lock(bucket, key).acquire();  // 同 PUT：提交段串行化
        co_await pool_->schedule();
        commit_object_file(object_path(bucket, key), tmp, meta, staging_ / "put", key);
    }

    std::error_code ec;
    fs::remove_all(up.dir, ec);
    co_return PutResult{meta.etag};
}

Task<void> LocalFsBackend::abort_multipart(std::string_view bucket, std::string_view key,
                                           std::string_view upload_id) {
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));
    std::error_code ec;
    fs::remove_all(up.dir, ec);
    if (ec) throw S3Error(S3ErrorCode::InternalError, "remove mpu dir: " + ec.message());
    co_return;
}

Task<std::vector<PartMeta>> LocalFsBackend::list_parts(std::string_view bucket,
                                                       std::string_view key,
                                                       std::string_view upload_id) {
    co_await pool_->schedule();
    auto up = require_upload(staging_, bucket, key, upload_id,
                             load_manifest(staging_, upload_id));
    std::vector<PartMeta> out;
    for (auto& e : fs::directory_iterator(up.dir)) {
        std::string name = e.path().filename().string();
        // part.NNNNN（跳过 manifest 与 .md5 sidecar）
        if (name.rfind("part.", 0) != 0 || name.ends_with(".md5")) continue;
        int no = 0;
        try {
            no = std::stoi(name.substr(5));
        } catch (...) {
            continue;
        }
        struct stat st{};
        if (::stat(e.path().c_str(), &st) != 0) continue;
        std::string etag;
        for (auto& [k, v] : read_tsv(up.dir / (name + ".md5")))
            if (k == "md5") etag = v;
        out.push_back({no, static_cast<uint64_t>(st.st_size), etag,
                       std::chrono::system_clock::from_time_t(st.st_mtime)});
    }
    std::sort(out.begin(), out.end(),
              [](const PartMeta& a, const PartMeta& b) { return a.part_no < b.part_no; });
    co_return out;
}

Task<std::vector<UploadInfo>> LocalFsBackend::list_multipart_uploads(std::string_view bucket) {
    co_await pool_->schedule();
    require_bucket(bucket);
    std::vector<UploadInfo> out;
    for (auto& e : fs::directory_iterator(staging_ / "mpu")) {
        if (!e.is_directory()) continue;
        std::string id = e.path().filename().string();
        std::string m_bucket, m_key;
        for (auto& [k, v] : read_tsv(e.path() / "manifest")) {
            if (k == "bucket") m_bucket = v;
            else if (k == "key") m_key = v;
        }
        if (m_bucket != bucket) continue;
        struct stat st{};
        auto initiated = ::stat((e.path() / "manifest").c_str(), &st) == 0
                             ? std::chrono::system_clock::from_time_t(st.st_mtime)
                             : std::chrono::system_clock::now();
        out.push_back({m_key, id, initiated});
    }
    std::sort(out.begin(), out.end(), [](const UploadInfo& a, const UploadInfo& b) {
        return std::tie(a.key, a.upload_id) < std::tie(b.key, b.upload_id);
    });
    co_return out;
}

void LocalFsBackend::cleanup_stale_uploads() {
    std::error_code ec;
    auto now = fs::file_time_type::clock::now();
    for (auto& e : fs::directory_iterator(staging_ / "mpu", ec)) {
        if (!e.is_directory()) continue;
        std::error_code tec;
        fs::path manifest = e.path() / "manifest";
        auto t = fs::exists(manifest, tec) ? fs::last_write_time(manifest, tec)
                                           : fs::last_write_time(e.path(), tec);
        if (tec || now - t <= kMpuTtl) continue;
        fs::remove_all(e.path(), tec);
        if (!tec)
            LOG_INFO("localfs: removed stale multipart upload {}",
                     e.path().filename().string());
    }
}

}  // namespace lights3::storage
