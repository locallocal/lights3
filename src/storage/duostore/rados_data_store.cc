#include "storage/duostore/rados_data_store.h"

#include <rados/librados.h>

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <stdexcept>
#include <utility>
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

// ---------- aio 协程桥接（§6.2）----------
// 在途单：一次 rados_aio_* 的完成会合点。completion 回调跑在 librados finisher
// 线程，纪律 = 只记录结果并把停车的续体投递回本进程池 executor，绝不在 ceph
// 线程上继续业务逻辑（阻塞它会反压 librados 内部管线）。
// 堆分配 + 双引用（发起方/回调）：写侧流水允许"发起后先不等待"，发起方弃单
//（writer 未 finish 即析构）时缓冲与额度都活到回调落地——librados 在途读写的
// 内存恒有效，额度记账与实际驻留内存恒一致。
struct AioPending {
    static constexpr uintptr_t kDone = 1;  // state：0 → 等待者 handle 地址 → kDone
    std::atomic<uintptr_t> state{0};
    std::atomic<int> ret{0};
    IExecutor* ex = nullptr;
    rados_completion_t comp = nullptr;
    std::shared_ptr<MetricHistogram> lat;  // 可空；提交 → 回调的耗时
    std::chrono::steady_clock::time_point t0;
    // 写侧专用：在途缓冲的所有权 + 其记账额度 + 收账元数据
    std::vector<std::byte> data;
    AsyncSemaphore::Permit permit;
    uint64_t file_id = 0;
    uint64_t len = 0;
    uint32_t crc = 0;
    std::atomic<int> refs{2};

    void unref() {
        if (refs.fetch_sub(1, std::memory_order_acq_rel) == 1) {
            if (comp) rados_aio_release(comp);
            delete this;
        }
    }

    static void on_complete(rados_completion_t c, void* arg) {
        auto* p = static_cast<AioPending*>(arg);
        p->ret.store(rados_aio_get_return_value(c), std::memory_order_relaxed);
        if (p->lat)
            p->lat->observe(
                std::chrono::duration<double>(std::chrono::steady_clock::now() - p->t0)
                    .count());
        IExecutor* ex = p->ex;
        uintptr_t prev = p->state.exchange(kDone, std::memory_order_acq_rel);
        p->unref();  // 此后不得再触碰 p：停车的等待者恢复后即可能释放最后一个引用
        if (prev)
            ex->post(std::coroutine_handle<>::from_address(reinterpret_cast<void*>(prev)));
    }
};

// co_await AioAwait{p}：回调未到则停车（由回调经 executor 投递恢复，§6.2 纪律），
// 已到则不挂起就地续行。返回 op 的 rados 返回值，语义处置归调用方
struct AioAwait {
    AioPending* p;
    bool await_ready() const noexcept {
        return p->state.load(std::memory_order_acquire) == AioPending::kDone;
    }
    bool await_suspend(std::coroutine_handle<> h) noexcept {
        uintptr_t expected = 0;
        return p->state.compare_exchange_strong(
            expected, reinterpret_cast<uintptr_t>(h.address()), std::memory_order_release,
            std::memory_order_acquire);
    }
    int await_resume() const noexcept { return p->ret.load(std::memory_order_relaxed); }
};

// 新建在途单并绑 completion；提交动作由调用方执行（op 形参不一）。提交即失败
//（返回负值）时回调不会来，调用方须 unref 两次回收
AioPending* make_pending(IExecutor& ex, std::shared_ptr<MetricHistogram> lat) {
    auto* p = new AioPending;
    p->ex = &ex;
    p->lat = std::move(lat);
    p->t0 = std::chrono::steady_clock::now();
    rados_aio_create_completion2(p, &AioPending::on_complete, &p->comp);
    return p;
}

// 本店对象名 c.<016x> 解析；其余（外来/异版本）不归本店（§8.2，对齐 fs 版对
// 非 *.chk 文件的处置）
bool parse_object_name(const char* name, uint64_t& id) {
    if (std::strlen(name) != 18 || name[0] != 'c' || name[1] != '.') return false;
    auto r = std::from_chars(name + 2, name + 18, id, 16);
    return r.ec == std::errc() && r.ptr == name + 18;
}

}  // namespace

// ---------- 构造 / 关闭 ----------

RadosDataStore::RadosDataStore(RadosDataOptions opt, std::shared_ptr<ThreadPool> pool,
                               FileIdAlloc alloc)
    : conn_(std::make_shared<Conn>()), opt_(std::move(opt)), pool_(std::move(pool)),
      alloc_(std::move(alloc)), exec_(*pool_),
      buffer_sem_(std::max<long>(1, long(opt_.buffer_total / opt_.chunk_size)), &exec_) {
    // op 延迟/错误指标（C4，§10）：构造期注册 0 值可见；空 scope 返回孤立实例
    const std::vector<double> bounds{0.001, 0.005, 0.02, 0.1, 0.5, 2, 10};
    auto lat = [&](const char* op) {
        return opt_.metrics.histogram(
            "lights3_duostore_rados_op_duration_seconds",
            "RADOS data op latency from submit to completion callback", bounds, {{"op", op}});
    };
    auto err = [&](const char* op) {
        return opt_.metrics.counter(
            "lights3_duostore_rados_op_errors_total",
            "RADOS data op failures (idempotent -ENOENT on remove excluded)", {{"op", op}});
    };
    m_lat_write_ = lat("write_full");
    m_lat_read_ = lat("read");
    m_lat_remove_ = lat("remove");
    m_err_write_ = err("write_full");
    m_err_read_ = err("read");
    m_err_remove_ = err("remove");
    m_err_scan_ = err("scan");

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

RadosDataStore::~RadosDataStore() {
    // 兜底（正路先 close()）：弃单在途写的回调引用本方 exec_/buffer_sem_，flush
    // 连回调一起等完后才可析构成员；Conn 仍由最后一个持有者（含逃逸 reader）释放
    if (conn_ && !conn_->closed.load(std::memory_order_acquire))
        rados_aio_flush(static_cast<rados_ioctx_t>(conn_->ioctx));
}

Task<void> RadosDataStore::close() {
    // 在途写清尾（§6.5）：rados_aio_flush 连 completion 回调一起等完——弃单 writer
    // 的缓冲/额度归还随之落地，此后 ceph 线程不再触碰 exec_/buffer_sem_。读侧在途
    // op 由逃逸 reader 自身的 Conn 引用兜住；置 closed 后新 op 干净地抛 500
    if (conn_ && !conn_->closed.load(std::memory_order_acquire)) {
        co_await pool_->schedule();
        rados_aio_flush(static_cast<rados_ioctx_t>(conn_->ioctx));
        conn_->closed.store(true, std::memory_order_release);
    }
    conn_.reset();
    co_return;
}

// ---------- RadosChunkWriter：切片缓冲 + aio write_full 双缓冲流水（§4）----------

class RadosChunkWriter final : public DataWriter {
public:
    explicit RadosChunkWriter(RadosDataStore* store) : store_(store) {}

    // 未 finish 即析构 = 丢弃（§4.3）：不等待在途单——其缓冲/额度由 completion
    // 回调落地时释放（AioPending 双引用），已写出对象成无主对象，由上层 remove
    // 兜底或孤儿扫描回收；析构中不做网络 IO
    ~RadosChunkWriter() override {
        if (pending_) pending_->unref();
    }

    Task<void> write(std::span<const std::byte> buf) override {
        require_usable();
        if (!permit_) {
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
        if (pending_) co_await harvest_pending();
        // 小对象/末段：缓冲至 EOF 的退化情形（§4.2），尾片立等；总长 0 = 空 DataRef
        if (!buf_.empty()) {
            co_await start_flush();
            co_await harvest_pending();
        }
        finished_ = true;
        co_return DataRef{std::move(extents_)};
    }

private:
    void require_usable() {
        if (finished_ || failed_)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore-rados: writer reused after finish/failure");
    }

    // 缓冲满：收上一单（至多 1 单在途，extent 顺序天然保序）→ 发起本片 → 备好
    // 下一片的缓冲。双缓冲流水（C3，§4.2）：优先复用刚收回的缓冲；首片则 try
    // 第二份额度——拿不到绝不排队（各 writer 已持一份时嵌套阻塞等待会互等死锁），
    // 退化为单缓冲串行（C1 行为），背压语义不变
    Task<void> flush_chunk() {
        if (pending_) co_await harvest_pending();
        co_await start_flush();
        if (spare_permit_) {
            permit_ = std::move(spare_permit_);
            buf_ = std::move(spare_);
        } else if (auto pm = store_->buffer_sem_.try_acquire()) {
            permit_ = std::move(*pm);
            buf_ = {};
            buf_.reserve(store_->opt_.chunk_size);
        } else {
            co_await harvest_pending();  // 串行退化：立等本片落地，收回缓冲继续
            permit_ = std::move(spare_permit_);
            buf_ = std::move(spare_);
        }
    }

    // 发起当前缓冲的 write_full（不等待）：单对象 op 原子（无 torn chunk）且回执
    // 即全副本持久（§4.1/§4.3）。缓冲与额度所有权移交在途单——弃单时 librados
    // 仍在读的内存与其记账额度都活到回调落地
    Task<void> start_flush() {
        co_await store_->pool_->schedule();  // crc 计算（CPU）不占 HTTP 驱动线程
        rados_ioctx_t ctx = io(store_->conn_);
        AioPending* p = make_pending(store_->exec_, store_->m_lat_write_);
        p->file_id = store_->alloc_(Extent::Kind::kRados);
        p->len = buf_.size();
        p->crc = codec::crc32c_of(std::span<const std::byte>(buf_));
        p->data = std::move(buf_);
        p->permit = std::move(permit_);
        int r = rados_aio_write_full(ctx, RadosDataStore::object_name(p->file_id).c_str(),
                                     p->comp, reinterpret_cast<const char*>(p->data.data()),
                                     p->data.size());
        if (r < 0) {
            p->unref();
            p->unref();
            failed_ = true;
            store_->m_err_write_->inc();
            throw_rados("aio_write_full submit", r);
        }
        pending_ = p;
    }

    // 等待在途单落地并收账：成功 → extent 入列、缓冲/额度收回备用；失败进 failed
    // 态不重试不复用（§4.4）
    Task<void> harvest_pending() {
        AioPending* p = std::exchange(pending_, nullptr);
        int r = co_await AioAwait{p};
        spare_ = std::move(p->data);
        spare_.clear();
        spare_permit_ = std::move(p->permit);
        uint64_t id = p->file_id, len = p->len;
        uint32_t crc = p->crc;
        p->unref();
        if (r < 0) {
            failed_ = true;
            store_->m_err_write_->inc();
            throw_rados("write_full", r);
        }
        extents_.push_back({Extent::Kind::kRados, id, 0, len, crc});
    }

    RadosDataStore* store_;
    std::vector<std::byte> buf_;    // 接收中的活跃缓冲（额度 = permit_）
    std::vector<std::byte> spare_;  // 上一单收回的缓冲（额度 = spare_permit_）
    std::vector<Extent> extents_;
    AsyncSemaphore::Permit permit_;
    AsyncSemaphore::Permit spare_permit_;
    AioPending* pending_ = nullptr;  // 在途单（至多 1：写第 N 片时接收 N+1）
    bool finished_ = false;
    bool failed_ = false;
};

// ---------- RadosExtentReader：多对象链流式读（§5）----------
// 结构对照 fs 版 ExtentChainReader。自包含：持 Conn 的 shared_ptr 而非 store 指针
// ——ObjectStream 会随 HTTP 响应逃逸出 backend 生命周期。无 fd 概念，逐次按名
// aio 读（C3：发起后停车，completion 经自持的池 executor 恢复，池线程不再阻塞
// 等网络）；rados 无"已打开 fd 不受 unlink 影响"的 POSIX 兜底，GC 竞态防护全靠
// pin+grace（§8.1）。

namespace {

class RadosExtentReader final : public http::BodyReader {
public:
    RadosExtentReader(std::shared_ptr<RadosDataStore::Conn> conn,
                      std::shared_ptr<ThreadPool> pool, bool verify_crc,
                      std::function<void()> on_corruption,
                      std::shared_ptr<MetricHistogram> lat, std::shared_ptr<MetricCounter> err,
                      std::vector<Extent> extents, uint64_t first, uint64_t last)
        : conn_(std::move(conn)), pool_(std::move(pool)), exec_(*pool_),
          verify_crc_(verify_crc), on_corruption_(std::move(on_corruption)),
          lat_(std::move(lat)), err_(std::move(err)), extents_(std::move(extents)),
          remaining_(last - first + 1), total_(remaining_) {
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
        rados_ioctx_t ctx = io(conn_);
        const Extent& e = extents_[idx_];
        if (at_start_) {
            // crc 校验只在"从段首完整读到段尾"时可行（Range 命中中段无从校验，§5）
            crc_active_ = verify_crc_ && cur_off_ == 0 && remaining_ >= e.length;
            crc_acc_ = 0;
            at_start_ = false;
        }
        size_t want = size_t(std::min<uint64_t>({buf.size(), e.length - cur_off_, remaining_}));
        AioPending* p = make_pending(exec_, lat_);
        int r = rados_aio_read(ctx, RadosDataStore::object_name(e.file_id).c_str(), p->comp,
                               reinterpret_cast<char*>(buf.data()), want, cur_off_);
        if (r < 0) {
            p->unref();
            p->unref();
            err_->inc();
            throw_rados("aio_read submit", r);
        }
        int n = co_await AioAwait{p};  // completion → 池线程恢复（§6.2）
        p->unref();
        if (n == -ENOENT) {
            // refs 在而对象缺 = 数据丢失征兆，或 pin/grace 失效（§6.3；主文档 §10 同款）
            err_->inc();
            LOG_ERROR("duostore-rados: extent object {} missing",
                      RadosDataStore::object_name(e.file_id));
            throw S3Error(S3ErrorCode::InternalError, "duostore-rados: extent object missing");
        }
        if (n < 0) {
            err_->inc();
            throw_rados("read", n);
        }
        if (n == 0) {
            err_->inc();
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore-rados: extent shorter than manifest");
        }
        if (crc_active_) crc_acc_ = codec::crc32c_update(crc_acc_, buf.first(size_t(n)));
        cur_off_ += uint64_t(n);
        remaining_ -= uint64_t(n);
        if (cur_off_ == e.length) {
            if (crc_active_ && crc_acc_ != e.crc32c) {
                LOG_ERROR("duostore-rados: object {} crc mismatch (stored {:08x} got {:08x})",
                          RadosDataStore::object_name(e.file_id), e.crc32c, crc_acc_);
                if (on_corruption_) on_corruption_();
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
    std::shared_ptr<ThreadPool> pool_;
    ThreadPoolExecutor exec_;  // aio 恢复投递；自持（reader 逃逸出 store 生命周期）
    bool verify_crc_;
    std::function<void()> on_corruption_;
    std::shared_ptr<MetricHistogram> lat_;
    std::shared_ptr<MetricCounter> err_;
    std::vector<Extent> extents_;
    size_t idx_ = 0;
    uint64_t cur_off_ = 0;
    uint64_t remaining_;
    uint64_t total_;
    bool at_start_ = true;
    bool crc_active_ = false;
    uint32_t crc_acc_ = 0;
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
    co_return std::make_unique<RadosExtentReader>(conn_, pool_, opt_.verify_chunk_crc,
                                                  opt_.on_corruption, m_lat_read_, m_err_read_,
                                                  std::move(ref.extents), first, last);
}

Task<void> RadosDataStore::remove(std::span<const Extent> extents) {
    co_await pool_->schedule();
    rados_ioctx_t ctx = io(conn_);  // close 守卫（空 extents 也生效）
    // C3：窗口化并发 aio remove——GC 大 manifest（TiB 级对象数十万 extent）不无界
    // 压集群；-ENOENT 幂等忽略（§7.3），其余负值收齐本批后抛首个（先收后抛，
    // 在途单不悬空）
    constexpr size_t kWindow = 16;
    size_t i = 0;
    while (i < extents.size()) {
        AioPending* batch[kWindow];
        size_t n = 0;
        int first_err = 0;
        for (; i < extents.size() && n < kWindow; ++i) {
            const auto& e = extents[i];
            if (e.kind != Extent::Kind::kRados) continue;  // 异种 extent（数据引擎切换遗留）不归本店
            AioPending* p = make_pending(exec_, m_lat_remove_);
            int r = rados_aio_remove(ctx, object_name(e.file_id).c_str(), p->comp);
            if (r < 0) {
                p->unref();
                p->unref();
                first_err = r;
                break;
            }
            batch[n++] = p;
        }
        for (size_t j = 0; j < n; ++j) {
            int r = co_await AioAwait{batch[j]};
            batch[j]->unref();
            if (r < 0 && r != -ENOENT && first_err == 0) first_err = r;
        }
        if (first_err < 0) {
            m_err_remove_->inc();
            throw_rados("remove", first_err);
        }
    }
    co_return;
}

Task<void> RadosDataStore::remove_pack(uint64_t pack_id) {
    // 无 pack：pack_stats 恒空，实际不会被调用（§3.3）；显式 no-op 而非接口默认
    (void)pack_id;
    co_return;
}

Task<GcRewrite> RadosDataStore::rewrite_pack(uint64_t pack_id) {
    // 无 pack：meta 永无 kRados 的 pack 记录，压实候选恒空，实际不会被调用（§3.3）
    (void)pack_id;
    co_return GcRewrite{};
}

Task<void> RadosDataStore::scan_chunks(
    const std::function<void(uint64_t file_id, int64_t mtime_ms)>& cb) {
    // C4（§8.2）：namespace 内全量列举（ioctx 已限定，多实例互不可见）→ 对象名解析
    // file_id → stat 取 mtime。列举/统计无 aio 版，低频（默认 1/d）在池线程同步
    // 阻塞可接受（对齐 fs 版整扫描驻池线程的先例）；孤儿判定（refs/grace/pin）
    // 是调用方的事，本店只枚举
    co_await pool_->schedule();
    rados_ioctx_t ctx = io(conn_);
    rados_list_ctx_t lc = nullptr;
    int r = rados_nobjects_list_open(ctx, &lc);
    if (r < 0) {
        m_err_scan_->inc();
        throw_rados("nobjects_list_open", r);
    }
    const char* entry = nullptr;
    while ((r = rados_nobjects_list_next(lc, &entry, nullptr, nullptr)) == 0) {
        uint64_t id = 0;
        if (!parse_object_name(entry, id)) continue;
        uint64_t size = 0;
        time_t mtime = 0;
        // 秒级 mtime 足够（宽限判定分钟级）；stat 失败 = 并发 remove 竞态，容忍跳过
        if (rados_stat(ctx, entry, &size, &mtime) != 0) continue;
        cb(id, int64_t(mtime) * 1000);
    }
    rados_nobjects_list_close(lc);
    if (r != -ENOENT) {  // 列举正常收尾返回 -ENOENT
        m_err_scan_->inc();
        throw_rados("nobjects_list_next", r);
    }
    co_return;
}

}  // namespace lights3::storage::duostore
