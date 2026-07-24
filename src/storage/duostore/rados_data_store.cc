#include "storage/duostore/rados_data_store.h"

#include <rados/librados.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <vector>

#include "core/log.h"
#include "s3/errors.h"
#include "storage/duostore/codec.h"

namespace lights3::storage::duostore {

using s3::S3Error;
using s3::S3ErrorCode;

namespace {

// librados 返回负 errno；InternalError 统一出口（仿 fs/rocks 版 throw_status，§6.3）
[[noreturn]] void throw_rados(const char* what, int ret) {
    LOG_ERROR("duostore-rados: {} failed: {} (errno {})", what, std::strerror(-ret), -ret);
    throw S3Error(S3ErrorCode::InternalError,
                  std::string("duostore-rados: ") + what + " failed");
}

rados_ioctx_t io(const std::shared_ptr<RadosDataStore::Conn>& c);  // fwd（定义见下）

}  // namespace

std::string RadosDataStore::object_name(uint64_t file_id) {
    char name[24];
    std::snprintf(name, sizeof name, "c.%016llx", (unsigned long long)file_id);
    return name;
}

void RadosDataStore::Conn::shutdown() {
    if (ioctx) {
        rados_ioctx_destroy(static_cast<rados_ioctx_t>(ioctx));
        ioctx = nullptr;
    }
    if (cluster) {
        rados_shutdown(static_cast<rados_t>(cluster));
        cluster = nullptr;
    }
}

namespace {

rados_ioctx_t io(const std::shared_ptr<RadosDataStore::Conn>& c) {
    // close 后任何调用干净地抛 500 而非崩溃（§6.5 守卫，仿 rocks 版 db()）
    if (!c || c->closed.load(std::memory_order_acquire))
        throw S3Error(S3ErrorCode::InternalError, "duostore-rados: store is closed");
    return static_cast<rados_ioctx_t>(c->ioctx);
}

}  // namespace

// ---------- 构造 / 关闭 ----------

RadosDataStore::RadosDataStore(RadosDataOptions opt, std::shared_ptr<ThreadPool> pool,
                               FileIdAlloc alloc)
    : conn_(std::make_shared<Conn>()), opt_(std::move(opt)), pool_(std::move(pool)),
      alloc_(std::move(alloc)), exec_(*pool_),
      buffer_sem_(std::max<long>(1, long(opt_.buffer_total / opt_.chunk_size)), &exec_) {
    // 建连序列（§6.1）：失败即构造失败——配置/环境错误在启动期暴露（fail fast）
    auto fail = [this](const char* what, int ret) {
        conn_->shutdown();
        throw std::runtime_error(std::string("duostore-rados: ") + what + " failed: " +
                                 std::strerror(-ret));
    };
    rados_t cluster = nullptr;
    int r = rados_create2(&cluster, "ceph", opt_.client_name.c_str(), 0);
    if (r < 0) fail("rados_create2", r);
    conn_->cluster = cluster;
    if ((r = rados_conf_read_file(cluster, opt_.conf_path.c_str())) < 0)
        fail("rados_conf_read_file", r);
    std::string mount_timeout = std::to_string(opt_.connect_timeout_sec);
    if ((r = rados_conf_set(cluster, "client_mount_timeout", mount_timeout.c_str())) < 0)
        fail("rados_conf_set client_mount_timeout", r);
    if (opt_.op_timeout_sec > 0) {
        // 非 0 时 -ETIMEDOUT 的 op 结果不明——writer 进 failed 态处置（§4.4/§6.4）
        std::string op_timeout = std::to_string(opt_.op_timeout_sec);
        if ((r = rados_conf_set(cluster, "rados_osd_op_timeout", op_timeout.c_str())) < 0)
            fail("rados_conf_set rados_osd_op_timeout", r);
    }
    if ((r = rados_connect(cluster)) < 0) fail("rados_connect", r);
    rados_ioctx_t ioctx = nullptr;
    if ((r = rados_ioctx_create(cluster, opt_.pool.c_str(), &ioctx)) < 0)
        fail("rados_ioctx_create", r);
    conn_->ioctx = ioctx;
    // ioctx 属性（namespace）此后不再变更——单 ioctx 进程级共享的线程安全前提（§6.1）
    rados_ioctx_set_namespace(ioctx, opt_.ns.c_str());
}

RadosDataStore::~RadosDataStore() = default;  // conn_ 由最后一个持有者（含逃逸 reader）释放

Task<void> RadosDataStore::close() {
    // C1 同步 op 无在途 aio 需要 flush（C3 起 rados_aio_flush）；置 closed 后释放
    // 本方引用——逃逸 reader 持有的 Conn 由其最后一个 shared_ptr 落地时 shutdown
    if (conn_) conn_->closed.store(true, std::memory_order_release);
    conn_.reset();
    co_return;
}

// ---------- RadosChunkWriter：切片缓冲 + write_full（§4）----------

class RadosChunkWriter final : public DataWriter {
public:
    explicit RadosChunkWriter(RadosDataStore* store) : store_(store) {}

    // 未 finish 即析构 = 丢弃：已写出对象成为无主对象，由上层 remove 兜底或孤儿
    // 扫描回收（§4.3）——析构中不做网络 IO；缓冲额度随 Permit RAII 归还
    ~RadosChunkWriter() override = default;

    Task<void> write(std::span<const std::byte> buf) override {
        require_usable();
        if (buf_.capacity() == 0) {
            // 首次 write 获取缓冲额度；耗尽则挂起，背压沿协程链传导回 socket 读循环（§4.2）
            permit_ = co_await store_->buffer_sem_.acquire();
            buf_.reserve(store_->opt_.chunk_size);
        }
        while (!buf.empty()) {
            size_t n = std::min<uint64_t>(store_->opt_.chunk_size - buf_.size(), buf.size());
            buf_.insert(buf_.end(), buf.begin(), buf.begin() + n);
            buf = buf.subspan(n);
            if (buf_.size() == store_->opt_.chunk_size) co_await flush_chunk();
        }
    }

    Task<DataRef> finish() override {
        require_usable();
        // 小对象/末段：缓冲至 EOF 的退化情形（§4.2）；总长 0 = 空 DataRef
        if (!buf_.empty()) co_await flush_chunk();
        finished_ = true;
        co_return DataRef{std::move(extents_)};
    }

private:
    void require_usable() {
        if (finished_ || failed_)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore-rados: writer reused after finish/failure");
    }

    // 缓冲满（或 EOF）→ 一个不可变对象一次 write_full：单对象 op 原子（无 torn
    // chunk）且回执即全副本持久（§4.1/§4.3）；失败进 failed 态不重试不复用（§4.4）
    Task<void> flush_chunk() {
        co_await store_->pool_->schedule();
        uint64_t id = store_->alloc_(Extent::Kind::kRados);
        uint32_t crc = codec::crc32c_of(std::span<const std::byte>(buf_));
        int r = rados_write_full(io(store_->conn_), RadosDataStore::object_name(id).c_str(),
                                 reinterpret_cast<const char*>(buf_.data()), buf_.size());
        if (r < 0) {
            failed_ = true;
            throw_rados("write_full", r);
        }
        extents_.push_back({Extent::Kind::kRados, id, 0, buf_.size(), crc});
        buf_.clear();
    }

    RadosDataStore* store_;
    std::vector<std::byte> buf_;
    std::vector<Extent> extents_;
    AsyncSemaphore::Permit permit_;
    bool finished_ = false;
    bool failed_ = false;
};

// ---------- RadosExtentReader：多对象链流式读（§5）----------
// 结构对照 fs 版 ExtentChainReader。自包含：持 Conn 的 shared_ptr 而非 store 指针
// ——ObjectStream 会随 HTTP 响应逃逸出 backend 生命周期。无 fd 概念，逐次按名读；
// rados 无"已打开 fd 不受 unlink 影响"的 POSIX 兜底，GC 竞态防护全靠 pin+grace（§8.1）。

namespace {

class RadosExtentReader final : public http::BodyReader {
public:
    RadosExtentReader(std::shared_ptr<RadosDataStore::Conn> conn, bool verify_crc,
                      std::vector<Extent> extents, uint64_t first, uint64_t last,
                      std::shared_ptr<ThreadPool> pool)
        : conn_(std::move(conn)), verify_crc_(verify_crc), extents_(std::move(extents)),
          remaining_(last - first + 1), total_(remaining_), pool_(std::move(pool)) {
        uint64_t off = first;
        while (idx_ < extents_.size() && off >= extents_[idx_].length) {
            off -= extents_[idx_].length;
            ++idx_;
        }
        cur_off_ = off;
        at_start_ = true;
    }

    std::optional<uint64_t> length() const override { return total_; }

    Task<size_t> read(std::span<std::byte> buf) override {
        if (remaining_ == 0 || buf.empty()) co_return 0;
        co_await pool_->schedule();
        const Extent& e = extents_[idx_];
        if (at_start_) {
            // crc 校验只在"从段首完整读到段尾"时可行（Range 命中中段无从校验，§5）
            crc_active_ = verify_crc_ && cur_off_ == 0 && remaining_ >= e.length;
            crc_acc_ = 0;
            at_start_ = false;
        }
        size_t want = size_t(std::min<uint64_t>({buf.size(), e.length - cur_off_, remaining_}));
        int n = rados_read(io(conn_), RadosDataStore::object_name(e.file_id).c_str(),
                           reinterpret_cast<char*>(buf.data()), want, cur_off_);
        if (n == -ENOENT) {
            // refs 在而对象缺 = 数据丢失征兆，或 pin/grace 失效（§6.3；主文档 §10 同款）
            LOG_ERROR("duostore-rados: extent object {} missing",
                      RadosDataStore::object_name(e.file_id));
            throw S3Error(S3ErrorCode::InternalError, "duostore-rados: extent object missing");
        }
        if (n < 0) throw_rados("read", n);
        if (n == 0)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore-rados: extent shorter than manifest");
        if (crc_active_) crc_acc_ = codec::crc32c_update(crc_acc_, buf.first(size_t(n)));
        cur_off_ += uint64_t(n);
        remaining_ -= uint64_t(n);
        if (cur_off_ == e.length) {
            if (crc_active_ && crc_acc_ != e.crc32c) {
                LOG_ERROR("duostore-rados: object {} crc mismatch (stored {:08x} got {:08x})",
                          RadosDataStore::object_name(e.file_id), e.crc32c, crc_acc_);
                throw S3Error(S3ErrorCode::InternalError, "duostore-rados: chunk crc mismatch");
            }
            ++idx_;
            cur_off_ = 0;
            at_start_ = true;
        }
        co_return size_t(n);
    }

private:
    std::shared_ptr<RadosDataStore::Conn> conn_;
    bool verify_crc_;
    std::vector<Extent> extents_;
    size_t idx_ = 0;
    uint64_t cur_off_ = 0;
    uint64_t remaining_;
    uint64_t total_;
    bool at_start_ = true;
    bool crc_active_ = false;
    uint32_t crc_acc_ = 0;
    std::shared_ptr<ThreadPool> pool_;
};

}  // namespace

// ---------- IDataStore ----------

Task<std::unique_ptr<DataWriter>> RadosDataStore::open_writer(WriteHint hint) {
    (void)hint;  // 未知长度流与已知长度流同一条缓冲切片路径（§3.3/§4.2）
    io(conn_);   // close 守卫
    co_return std::make_unique<RadosChunkWriter>(this);
}

Task<std::unique_ptr<http::BodyReader>> RadosDataStore::open_reader(DataRef ref, uint64_t first,
                                                                    uint64_t last) {
    uint64_t total = ref.total();
    if (first > last || last >= total)
        throw S3Error(S3ErrorCode::InternalError,
                      "duostore-rados: reader range beyond manifest");  // 调用方已 resolve_range
    io(conn_);  // close 守卫
    co_return std::make_unique<RadosExtentReader>(conn_, opt_.verify_chunk_crc,
                                                  std::move(ref.extents), first, last, pool_);
}

Task<void> RadosDataStore::remove(std::span<const Extent> extents) {
    co_await pool_->schedule();
    rados_ioctx_t ctx = io(conn_);  // close 守卫（空 extents 也生效）
    for (const auto& e : extents) {
        if (e.kind != Extent::Kind::kRados) continue;  // 异种 extent（数据引擎切换遗留）不归本店
        int r = rados_remove(ctx, object_name(e.file_id).c_str());
        if (r < 0 && r != -ENOENT) throw_rados("remove", r);  // 幂等：-ENOENT 忽略（§7.3）
    }
    co_return;
}

Task<GcRewrite> RadosDataStore::rewrite_pack(uint64_t pack_id) {
    // 无 pack：meta 永无 kRados 的 pack 记录，压实候选恒空，实际不会被调用（§3.3）
    (void)pack_id;
    co_return GcRewrite{};
}

}  // namespace lights3::storage::duostore
