// L3: xlocalfs——localfs 的 io_uring 数据面变体（见 docs/storage-backend.md §3.3）。
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
                    std::shared_ptr<ThreadPool> pool, UringOptions uring_opt = {},
                    LocalFsOptions fs_opt = {}, MetricsScope metrics = {});

    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta,
                               http::BodyReader& body,
                               PutCondition cond = {}) override;
    Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                std::string_view upload_id, int part_no,
                                http::BodyReader& body) override;
    Task<PutResult> complete_multipart(std::string_view bucket, std::string_view key,
                                       std::string_view upload_id,
                                       std::span<const PartInfo> parts) override;
    Task<void> close() override;  // 停止 uring 收割线程

private:
    // 流式收 body 并经 io_uring 写入 staging 临时文件，返回 (字节数, MD5 hex)
    // 出参而非返回值：见 .cc 中的说明（co_await 结果里带 std::string 时，
    // body 抛异常会让编译器析构从未构造的绑定目标）
    Task<void> drain_to_tmp(http::BodyReader& body, int fd, uint64_t& total_out,
                            std::string& etag_out);
    // 内核可能短写，循环续写直到写满；失败抛 InternalError
    Task<void> write_all(int fd, std::span<const std::byte> data, uint64_t off);
    // 数据落盘（docs/gaps.md §6.3）：走 io_uring 的 FSYNC SQE，提交段不再占池线程
    // 阻塞在 fdatasync 上——这正是 xlocalfs 要消除的那类等待。内核不支持 FSYNC
    // opcode（探测结果）或 LIGHTS3_FSYNC=0 时回落既有同步路径
    Task<void> sync_fd(int fd);

    std::shared_ptr<UringEngine> uring_;
};

}  // namespace lights3::storage
