// L3: xlocalfs——localfs 的 io_uring 数据面变体（见 docs/04-storage-backend.md §3.3）。
// 磁盘布局与元数据逻辑完全复用 LocalFsBackend；GET 流式读、PUT/分片流式写、
// complete 拼接中的字节搬运改经 io_uring 异步执行，不再占用池线程等待磁盘。
// 目录遍历与元数据操作仍走线程池（io_uring 无 getdents 等目录原语）。
#pragma once

#include "storage/localfs/localfs_backend.h"
#include "storage/xlocalfs/uring.h"

namespace lights3::storage {

class XLocalFsBackend final : public LocalFsBackend {
public:
    XLocalFsBackend(std::filesystem::path root, std::filesystem::path staging,
                    std::shared_ptr<ThreadPool> pool, unsigned queue_depth = 256);

    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta,
                               http::BodyReader& body) override;
    Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                std::string_view upload_id, int part_no,
                                http::BodyReader& body) override;
    Task<PutResult> complete_multipart(std::string_view bucket, std::string_view key,
                                       std::string_view upload_id,
                                       std::span<const PartInfo> parts) override;
    Task<void> close() override;  // 停止 uring 收割线程

private:
    // 流式收 body 并经 io_uring 写入 staging 临时文件，返回 (字节数, MD5 hex)
    Task<std::pair<uint64_t, std::string>> drain_to_tmp(http::BodyReader& body, int fd);
    // 内核可能短写，循环续写直到写满；失败抛 InternalError
    Task<void> write_all(int fd, std::span<const std::byte> data, uint64_t off);

    std::shared_ptr<UringEngine> uring_;
};

}  // namespace lights3::storage
