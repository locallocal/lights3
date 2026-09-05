// L3: xlocalfs -- io_uring data-plane variant of localfs (see docs/storage-backend.md §3.3).
// Disk layout and metadata logic are fully reused from LocalFsBackend; the byte transfers in
// GET streaming reads, PUT/part streaming writes, and complete-time concatenation go through
// io_uring with multiple ops in flight per stream (uring_stream.h read-ahead / pipelined
// writes, roadmap §3.4 ①), the commit-phase fdatasync rides as a linked SQE behind the
// final write (③), and -- kernel permitting -- open/statx/rename/unlink on the data paths
// go through the ring's metadata opcodes instead of blocking a pool thread (③).
// Directory traversal (listing) still uses the thread pool: io_uring has no getdents.
#pragma once

#include "storage/localfs/localfs_backend.h"
#include "storage/xlocalfs/uring.h"
#include "storage/xlocalfs/uring_stream.h"

namespace lights3::storage {

class XLocalFsBackend final : public LocalFsBackend {
    const char* engine_name() const override { return "xlocalfs"; }

public:
    XLocalFsBackend(std::filesystem::path root, std::filesystem::path staging,
                    std::shared_ptr<ThreadPool> pool, UringOptions uring_opt = {},
                    LocalFsOptions fs_opt = {}, MetricsScope metrics = {});

    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta,
                               http::BodyReader& body,
                               PutCondition cond = {}) override;
    using LocalFsBackend::upload_part;
    Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                std::string_view upload_id, int part_no, http::BodyReader& body,
                                const std::optional<PartChecksum>& checksum) override;
    Task<PutResult> complete_multipart(std::string_view bucket, std::string_view key,
                                       std::string_view upload_id,
                                       std::span<const PartInfo> parts) override;
    // UNLINKAT via the ring when the kernel has it (roadmap §3.4 ③): unlinking a large
    // file is real disk work on ext4/xfs; falls through to the base implementation otherwise
    Task<void> delete_object(std::string_view bucket, std::string_view key) override;
    Task<void> close() override;  // stop the uring reaper threads

private:
    // Stream the body into the write pipeline, yielding (byte count, MD5 hex) via
    // out-params. The pipeline is deliberately left unfinished: the caller writes the
    // meta xattr first (original commit order) and then finish()es, so the final block
    // and the fdatasync go out as one linked chain.
    // Out-params instead of a return value: when a co_await result carries a std::string,
    // a throw from body makes the compiler destroy a never-constructed binding target
    // (double free / SEGV, docs/archive/gaps.md §5.6)
    Task<void> drain_to_tmp(http::BodyReader& body, UringWriteStream& ws, uint64_t& total_out,
                            std::string& etag_out);
    // The rename + directory-fsync + sidecar tail of fsutil::commit_object_file, with the
    // rename going through RENAMEAT and the directory fsync through an FSYNC SQE when
    // available (same on-disk result; the caller already persisted xattr + data).
    // By-value paths: coroutine parameters must not bind temporaries. xattr_ok is the
    // outcome of the caller's set_meta_xattr (drives the sidecar policy, roadmap §3.5)
    Task<void> commit_prepared(std::filesystem::path dest, fsutil::TmpFile& tmp,
                               const ObjectMeta& meta, std::string_view key, bool xattr_ok);
    // fsync the directory entry via an FSYNC SQE (silent-failure semantics of
    // fsutil::fsync_dir); no-op under LIGHTS3_FSYNC=0
    Task<void> sync_dir(std::filesystem::path dir);

    std::shared_ptr<UringEngine> uring_;
};

}  // namespace lights3::storage
