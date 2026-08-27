// L3: xlocalfs -- io_uring data-plane variant of localfs (see docs/storage-backend.md §3.3).
// Disk layout and metadata logic are fully reused from LocalFsBackend; the byte transfers in
// GET streaming reads, PUT/part streaming writes, and complete-time concatenation go through
// io_uring asynchronously instead, no longer tying up pool threads waiting on disk.
// Directory traversal and metadata operations still use the thread pool (io_uring has no
// directory primitives such as getdents).
#pragma once

#include "storage/localfs/localfs_backend.h"
#include "storage/xlocalfs/uring.h"

namespace lights3::storage {

class XLocalFsBackend final : public LocalFsBackend {
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
    Task<void> close() override;  // stop the uring reaper thread

private:
    // Stream in the body and write it to the staging temp file via io_uring, yielding
    // (byte count, MD5 hex).
    // Out-params instead of a return value: see the note in the .cc (when a co_await result
    // carries a std::string, a throw from body makes the compiler destroy a never-constructed
    // binding target)
    Task<void> drain_to_tmp(http::BodyReader& body, int fd, uint64_t& total_out,
                            std::string& etag_out);
    // The kernel may short-write; loop until everything is written; throw InternalError on failure
    Task<void> write_all(int fd, std::span<const std::byte> data, uint64_t off);
    // Data persistence (docs/archive/gaps.md §6.3): use io_uring's FSYNC SQE so the submit phase no
    // longer blocks a pool thread in fdatasync -- exactly the kind of wait xlocalfs is meant
    // to eliminate. Falls back to the existing synchronous path when the kernel lacks the
    // FSYNC opcode (per probe) or LIGHTS3_FSYNC=0
    Task<void> sync_fd(int fd);

    std::shared_ptr<UringEngine> uring_;
};

}  // namespace lights3::storage
