// L3: on-disk primitives shared by the localfs family of backends (tmp files, TSV
// sidecar/manifest, atomic commit).
// localfs and xlocalfs share the same disk layout (docs/storage-backend.md §3.1/§3.2); they differ only in data-plane IO style.
#pragma once

#include <sys/stat.h>

#include <atomic>
#include <filesystem>
#include <memory>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "core/metrics.h"
#include "core/thread_pool.h"
#include "storage/backend.h"

namespace lights3::storage::fsutil {

inline constexpr const char* kSidecarSuffix = ".lights3-meta";
inline constexpr const char* kBucketMarker = ".lights3-bucket";
// Directory-marker object (docs/archive/gaps.md §6.3): a trailing '/' in a key has no
// corresponding file name on the filesystem; its carrier is this reserved file inside the
// directory -- "a/b/" ⇔ <bucket>/a/b/.lights3-dir. Listing restores it to the key "a/b/"
// (its sort key is the empty string, so it sorts right before the other keys in the same
// directory, consistent with the total order)
inline constexpr const char* kDirMarker = ".lights3-dir";
// Metadata extended attribute on the data file (content = same TSV as the sidecar):
// travels with the inode and is committed by the same rename as the data, so it can never
// be misaligned with the data it describes (docs/storage-backend.md §3.1)
inline constexpr const char* kMetaXattr = "user.lights3.meta";

std::string next_tmp_name();

// Deleted on destruction if not committed
struct TmpFile {
    std::filesystem::path path;
    int fd = -1;
    bool committed = false;
    ~TmpFile();
};

// Keys must not use internal reserved names (sidecar/marker), to avoid clashing with data files
void reject_reserved_key(std::string_view key);

[[noreturn]] void throw_errno(const std::string& what);

// Durability primitives (all no-ops when LIGHTS3_FSYNC=0, docs/storage-backend.md §3.1):
// fsync_file runs fdatasync on an open fd (throws on failure); fsync_dir persists the
// directory entry (rename only guarantees atomicity, not that the parent directory has
// been persisted; failure is silent -- it should not take down the write path)
void fsync_file(int fd);
void fsync_dir(const std::filesystem::path& dir);
// The switch itself (LIGHTS3_FSYNC): xlocalfs uses it to decide whether to submit
// io_uring's FSYNC SQE
bool fsync_enabled();

// k<TAB>v line format, atomic tmp+rename write
void write_tsv(const std::filesystem::path& dest, const std::filesystem::path& tmp_dir,
               const std::vector<std::pair<std::string, std::string>>& kv);
std::vector<std::pair<std::string, std::string>> read_tsv(const std::filesystem::path& path);

// Metadata-xattr accounting and policy (roadmap §3.5): setxattr failure used to be a
// single startup-time WARN line -- silently falling back to the two-rename sidecar
// consistency model. The backend owns one of these, wires the gauge/counter into its
// MetricsScope, and hands a pointer down every write path. required=true turns a failed
// xattr write into an InternalError **before** the data rename (fail-fast: the object is
// never committed without its atomic metadata), for deployments that must not run on a
// filesystem without xattr support. All fields are optional; a null policy = the legacy
// "warn once, degrade" behavior
struct MetaXattrPolicy {
    bool required = false;
    std::shared_ptr<MetricGauge> fallback;    // 1 once any xattr write has failed (resident)
    std::shared_ptr<MetricCounter> failures;  // every failed setxattr
    std::atomic<uint64_t> failure_count{0};
    void note_failure() {
        failure_count.fetch_add(1, std::memory_order_relaxed);
        if (failures) failures->inc();
        if (fallback) fallback->set(1);
    }
};

// One-shot capability probe: creates a scratch file under dir, writes and reads back
// kMetaXattr, unlinks it. Returns the failing errno (0 = supported). Used at backend
// construction so the fallback gauge is live before the first PUT, and to fail fast under
// require_xattr
int probe_meta_xattr(const std::filesystem::path& dir);

// Sidecar write policy (roadmap §3.5): the xattr is the authoritative read source, the
// sidecar exists for external tools / legacy objects / filesystems without xattr. In the
// default kSync mode a small-object PUT costs 4 fsyncs + 2 renames, half of it the
// sidecar. kAsync takes the sidecar off the latency path (written by a background task
// after the response); kLazy does not write it at all while the xattr succeeded (and
// unlinks a stale one left by an earlier sync-mode write). Both fall back to a
// synchronous sidecar write whenever the xattr write failed -- then the sidecar is the
// only metadata source and must be committed before the caller answers
enum class SidecarMode { kSync, kAsync, kLazy };
SidecarMode parse_sidecar_mode(std::string_view s);  // sync|async|lazy, else runtime_error
const char* sidecar_mode_name(SidecarMode m);

struct CommitOptions {
    // The caller already performed "write xattr → persist data" in commit_object_file's
    // original order itself (xlocalfs replaces that blocking fdatasync with io_uring's
    // FSYNC SQE), so those two steps are skipped; xattr_ok then reports that step's outcome
    bool prepared = false;
    bool xattr_ok = true;
    SidecarMode sidecar = SidecarMode::kSync;
    MetaXattrPolicy* xattr = nullptr;
};

// Create parent dirs + directory-conflict check + data rename + sidecar
// (docs/storage-backend.md §3.1 write atomicity); shared by PUT and complete_multipart.
// Returns true when the sidecar write was **deferred to the caller** (SidecarMode::kAsync
// with a successful xattr): the caller must schedule write_object_sidecar off the request
// path. Every other mode returns false with the on-disk state complete
bool commit_object_file(const std::filesystem::path& dest, TmpFile& tmp, const ObjectMeta& meta,
                        const std::filesystem::path& staging_put, std::string_view key,
                        const CommitOptions& opt = {});

// The two synchronous halves of commit_object_file, exposed separately so xlocalfs can run
// the data rename and the directory fsync in between through io_uring (RENAMEAT + FSYNC
// SQE, roadmap §3.4 ③) with byte-identical on-disk semantics:
// prepare_object_dest = create parent dirs + directory-conflict checks;
// write_object_sidecar = the trailing sidecar write
void prepare_object_dest(const std::filesystem::path& dest, std::string_view key);
void write_object_sidecar(const std::filesystem::path& dest, const ObjectMeta& meta,
                          const std::filesystem::path& staging_put);
// The mode-aware trailing sidecar step of commit_object_file (also used by xlocalfs's
// ring-based commit): sync → write now; lazy+xattr_ok → unlink a stale sidecar, write
// nothing; async+xattr_ok → write nothing and return true (caller defers); any mode with
// xattr_ok=false → write now (the sidecar is the only source). Returns "deferred"
bool finish_object_sidecar(const std::filesystem::path& dest, const ObjectMeta& meta,
                           const std::filesystem::path& staging_put, SidecarMode mode,
                           bool xattr_ok);

// Commit-point check for conditional PUT (PutCondition contract, storage/backend.h): the
// caller must hold the commit lock for the same key so the check and the following rename
// commit are atomic. Metadata is read via xattr/sidecar and is equally authoritative for
// tier stubs (a stub keeps the original etag); tiered reuses this check directly
void check_put_condition(const std::filesystem::path& data_path, const PutCondition& cond,
                         std::string_view key);

// ---- Sidecar extensions for tiered storage (docs/tiered-storage.md §4) ----

enum class Tier { kLocal, kRemote, kCached };

struct TierInfo {
    Tier tier = Tier::kLocal;
    std::string remote_etag;  // cloud replica ETag (unquoted hex; for verification and GC, never exposed)
    std::string remote_at;    // upload time (iso8601)
};

// Write metadata into the data file's xattr (used inside commit_object_file; xlocalfs
// needs to write the xattr itself first in the same order, so it can swap the subsequent
// fdatasync for io_uring's FSYNC SQE). Returns false when the write failed and the object
// will rely on its sidecar (accounted on policy when given; throws InternalError instead
// under policy->required)
bool set_meta_xattr(const std::filesystem::path& path, const ObjectMeta& meta,
                    const TierInfo& tier, MetaXattrPolicy* policy = nullptr);

// stat the data file + read metadata (xattr first, fall back to sidecar); when
// tier != local the size comes from the metadata (a stub data file has zero length,
// docs/tiered-storage.md §4.1).
// Missing / not a regular file throws NoSuchKey.
ObjectMeta load_object_meta(const std::filesystem::path& data_path, std::string key,
                            TierInfo* tier_out = nullptr);
// Whether the data file carries the metadata xattr (operator introspection, roadmap §6.2)
bool has_meta_xattr(const std::filesystem::path& data_path);

// Same as above, but reuses a stat result the caller already holds. GET must use **fstat
// on the already-open fd**: a second stat on the path after a concurrent overwrite would
// pick up the new object's size/etag while paired with the old inode's body (size grew →
// pread hits early EOF, short body; shrank → body truncated), silent corruption
ObjectMeta load_object_meta_stat(const std::filesystem::path& data_path, std::string key,
                                 const struct stat& st, TierInfo* tier_out = nullptr);

// The object got stubbed between GET's open(data) and reading the sidecar: the held fd is
// a 0-length new inode while the sidecar claims size>0. TieredBackend catches this and
// retries via the cloud; a standalone localfs hitting it (misconfigured onto a tiered
// layout) maps it to InternalError 500.
struct StubRace : s3::S3Error {
    explicit StubRace(std::string key)
        : S3Error(s3::S3ErrorCode::InternalError, "object is a tier stub", std::move(key)) {}
};

// Stubbing commit (docs/tiered-storage.md §5.2 steps b/c): first write the tier=remote
// sidecar, then rename a 0-length tmp over the data file. Idempotent; the caller must hold
// the per-key lock.
// In-place metadata rewrite for an existing object (roadmap §2.5 ?tagging): xattr
// first (authoritative), sidecar after — same consistency model as commit paths.
// Under SidecarMode::kLazy the sidecar is rewritten only if one exists or the xattr
// failed (a rare operator path: async deferral is not worth its complexity here).
// Caller holds the per-key commit lock
void rewrite_object_meta(const std::filesystem::path& data_path, const ObjectMeta& meta,
                         const TierInfo& tier, const std::filesystem::path& staging_put,
                         SidecarMode mode = SidecarMode::kSync,
                         MetaXattrPolicy* policy = nullptr);

void commit_stub(const std::filesystem::path& dest, const ObjectMeta& meta, const TierInfo& tier,
                 const std::filesystem::path& staging_put);

// Cache backfill commit (docs/tiered-storage.md §6.2): rename the data tmp first, then
// write the tier=cached sidecar
// (on a crash in between, the sidecar still says remote and reads keep going to the cloud unaffected).
// dest must previously be a stub (parent directory already exists), so no directory-conflict check.
void commit_cached(const std::filesystem::path& dest, TmpFile& tmp, const ObjectMeta& meta,
                   const TierInfo& tier, const std::filesystem::path& staging_put);

// pread streaming reader; each chunk runs on the thread pool (blocking IO stays off the
// HTTP execution environment).
// fd ownership transfers to this reader; early EOF if the file is truncated externally.
// Shared by localfs GET and tiered demotion upload.
class FdStreamReader final : public http::BodyReader {
public:
    FdStreamReader(int fd, uint64_t offset, uint64_t remaining, std::shared_ptr<ThreadPool> pool)
        : fd_(fd), offset_(offset), remaining_(remaining), total_(remaining),
          pool_(std::move(pool)) {}
    ~FdStreamReader() override;

    Task<size_t> read(std::span<std::byte> buf) override;
    std::optional<uint64_t> length() const override { return total_; }
    // sendfile exit (roadmap §4.3 ④): the remaining range, as-is
    std::optional<http::FileSpan> try_as_file() override {
        return http::FileSpan{fd_, offset_, remaining_};
    }
    void file_bytes_sent(uint64_t n) override {
        n = std::min(n, remaining_);
        offset_ += n;
        remaining_ -= n;
    }

private:
    int fd_;
    uint64_t offset_;
    uint64_t remaining_;
    uint64_t total_;
    std::shared_ptr<ThreadPool> pool_;
};

// ---- multipart layout (docs/storage-backend.md §3.2): <staging>/mpu/<id>/{manifest, part.NNNNN, .md5} ----

std::string part_file_name(int part_no);

struct UploadState {
    std::filesystem::path dir;
    ObjectMeta meta;  // content_type / user_meta recorded in the manifest
};

// upload_id validity + manifest existence + bucket/key match; any failure counts as NoSuchUpload
UploadState require_upload(const std::filesystem::path& staging, std::string_view bucket,
                           std::string_view key, std::string_view upload_id,
                           const std::vector<std::pair<std::string, std::string>>& manifest);

// Check the id's format and existence before reading the manifest (the id is spliced into
// a path, so format validation doubles as escape prevention)
std::vector<std::pair<std::string, std::string>> load_manifest(
    const std::filesystem::path& staging, std::string_view upload_id);

}  // namespace lights3::storage::fsutil
