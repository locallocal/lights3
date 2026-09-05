#include "storage/duostore/rados_data_store.h"

#include "core/fault.h"

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

// librados returns negative errno; unified InternalError exit (modeled on the fs/rocks throw_status, §6.3)
[[noreturn]] void throw_rados(const char* what, int ret) {
    LOG_ERROR("duostore-rados: {} failed: {} (errno {})", what, std::strerror(-ret), -ret);
    throw S3Error(S3ErrorCode::InternalError,
                  std::string("duostore-rados: ") + what + " failed");
}

rados_ioctx_t io(const std::shared_ptr<RadosDataStore::Conn>& c);  // fwd (defined below)

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
    // After close, any call throws a clean 500 rather than crashing (§6.5 guard, modeled on the rocks db())
    if (!c || c->closed.load(std::memory_order_acquire))
        throw S3Error(S3ErrorCode::InternalError, "duostore-rados: store is closed");
    return static_cast<rados_ioctx_t>(c->ioctx);
}

// ---------- aio-to-coroutine bridge (§6.2) ----------
// In-flight ticket: the rendezvous point for one rados_aio_* completion. The
// completion callback runs on a librados finisher thread; the discipline is to
// only record the result and post the parked continuation back to this process's
// pool executor, never continuing business logic on a ceph thread (blocking it
// backpressures librados's internal pipeline).
// Heap-allocated + two references (initiator/callback): the write-side pipeline
// allows "submit without waiting"; when the initiator abandons the ticket
// (writer destroyed without finish), the buffer and the permit both live until
// the callback lands — memory librados is still reading/writing stays valid, and
// permit accounting always matches actual resident memory.
struct AioPending {
    static constexpr uintptr_t kDone = 1;  // state: 0 → waiter's handle address → kDone
    std::atomic<uintptr_t> state{0};
    std::atomic<int> ret{0};
    IExecutor* ex = nullptr;
    rados_completion_t comp = nullptr;
    std::shared_ptr<MetricHistogram> lat;  // nullable; time from submit to callback
    std::chrono::steady_clock::time_point t0;
    // Write-side only: ownership of the in-flight buffer + its accounted permit + settlement metadata
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
        p->unref();  // must not touch p after this: a parked waiter may release the last reference once resumed
        if (prev)
            ex->post(std::coroutine_handle<>::from_address(reinterpret_cast<void*>(prev)));
    }
};

// co_await AioAwait{p}: parks if the callback has not arrived yet (resumed by the
// callback posting through the executor, §6.2 discipline); if it has, continues
// in place without suspending. Returns the op's rados return value; semantic
// handling belongs to the caller
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

// Create a new in-flight ticket and bind its completion; submission is performed
// by the caller (op signatures differ). If submission itself fails (negative
// return), the callback will never come and the caller must unref twice to reclaim
AioPending* make_pending(IExecutor& ex, std::shared_ptr<MetricHistogram> lat) {
    auto* p = new AioPending;
    p->ex = &ex;
    p->lat = std::move(lat);
    p->t0 = std::chrono::steady_clock::now();
    rados_aio_create_completion2(p, &AioPending::on_complete, &p->comp);
    return p;
}

// Parse our object name c.<016x>; everything else (foreign/other version) does
// not belong to this store (§8.2, matching the fs version's handling of non-*.chk files)
bool parse_object_name(const char* name, uint64_t& id) {
    if (std::strlen(name) != 18 || name[0] != 'c' || name[1] != '.') return false;
    auto r = std::from_chars(name + 2, name + 18, id, 16);
    return r.ec == std::errc() && r.ptr == name + 18;
}

}  // namespace

// ---------- construction / shutdown ----------

RadosDataStore::RadosDataStore(RadosDataOptions opt, std::shared_ptr<ThreadPool> pool,
                               FileIdAlloc alloc)
    : conn_(std::make_shared<Conn>()), opt_(std::move(opt)), pool_(std::move(pool)),
      alloc_(std::move(alloc)), exec_(*pool_),
      buffer_sem_(std::max<long>(1, long(opt_.buffer_total / opt_.chunk_size)), &exec_) {
    // op latency/error metrics (C4, §10): registered at construction so zero values are visible; an empty scope yields an isolated instance
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

    // Connection sequence (§6.1): failure means construction failure — configuration/environment errors surface at startup (fail fast)
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
        // When non-zero, a -ETIMEDOUT op's outcome is indeterminate — the writer enters failed state to handle it (§4.4/§6.4)
        std::string op_timeout = std::to_string(opt_.op_timeout_sec);
        if ((r = rados_conf_set(cluster, "rados_osd_op_timeout", op_timeout.c_str())) < 0)
            fail("rados_conf_set rados_osd_op_timeout", r);
    }
    if ((r = rados_connect(cluster)) < 0) fail("rados_connect", r);
    rados_ioctx_t ioctx = nullptr;
    if ((r = rados_ioctx_create(cluster, opt_.pool.c_str(), &ioctx)) < 0)
        fail("rados_ioctx_create", r);
    conn_->ioctx = ioctx;
    // ioctx attributes (namespace) never change after this — the thread-safety precondition for process-wide sharing of a single ioctx (§6.1)
    rados_ioctx_set_namespace(ioctx, opt_.ns.c_str());
}

RadosDataStore::~RadosDataStore() {
    // Fallback (normal path calls close() first): callbacks of abandoned in-flight
    // writes reference our exec_/buffer_sem_, so flush must wait for the callbacks
    // too before members may be destroyed; Conn is still released by the last
    // holder (including escaped readers)
    if (conn_ && !conn_->closed.load(std::memory_order_acquire))
        rados_aio_flush(static_cast<rados_ioctx_t>(conn_->ioctx));
}

Task<void> RadosDataStore::close() {
    // Drain in-flight writes (§6.5): rados_aio_flush waits for the completion
    // callbacks too — buffer/permit returns of abandoned writers land with it,
    // after which ceph threads never touch exec_/buffer_sem_ again. Read-side
    // in-flight ops are covered by the escaped reader's own Conn reference; after
    // closed is set, new ops throw a clean 500
    if (conn_ && !conn_->closed.load(std::memory_order_acquire)) {
        co_await pool_->schedule();
        rados_aio_flush(static_cast<rados_ioctx_t>(conn_->ioctx));
        conn_->closed.store(true, std::memory_order_release);
    }
    conn_.reset();
    co_return;
}

// ---------- RadosChunkWriter: slice buffering + aio write_full double-buffered pipeline (§4) ----------

// Owner xattr name (docs/archive/gaps.md §6.1): WriteHint.owner used to be dropped —
// disaster recovery with all meta lost had no way to determine object ownership.
// The three owner forms (codec::parse_pack_owner) are stored verbatim as the
// value; offline salvage reverse-looks-up via rados getxattr
constexpr char kOwnerXattr[] = "lights3.owner";

class RadosChunkWriter final : public DataWriter {
public:
    RadosChunkWriter(RadosDataStore* store, std::string owner)
        : store_(store), owner_(std::move(owner)) {}

    // Destruction without finish = discard (§4.3): does not wait for the in-flight
    // ticket — its buffer/permit are released when the completion callback lands
    // (AioPending's two references); already-written objects become ownerless,
    // reclaimed by the caller's fallback remove or the orphan scan; no network IO
    // in the destructor
    ~RadosChunkWriter() override {
        if (pending_) pending_->unref();
        // Discard without finish: pins this writer established have no one to take
        // them over, so release in place (after a successful finish, pinned_ is
        // already cleared, so the batch handed to the caller is never mistakenly released)
        for (uint64_t id : pinned_) store_->opt_.pins.unpin_one(id);
    }

    Task<void> write(std::span<const std::byte> buf) override {
        require_usable();
        if (!permit_) {
            // First write acquires a buffer permit; when exhausted it suspends, and backpressure propagates along the coroutine chain back to the socket read loop (§4.2)
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
        // Small object / final slice: the degenerate buffered-until-EOF case (§4.2), the tail slice waits synchronously; total length 0 = empty DataRef
        if (!buf_.empty()) {
            co_await start_flush();
            co_await harvest_pending();
        }
        finished_ = true;
        pinned_.clear();  // unpin responsibility transfers to the caller with the DataRef (same semantics as the fs version)
        co_return DataRef{std::move(extents_)};
    }

private:
    void require_usable() {
        if (finished_ || failed_)
            throw S3Error(S3ErrorCode::InternalError,
                          "duostore-rados: writer reused after finish/failure");
    }

    // Buffer full: harvest the previous ticket (at most 1 in flight, so extent
    // order is naturally preserved) → submit this slice → prepare the next slice's
    // buffer. Double-buffered pipeline (C3, §4.2): prefer reusing the buffer just
    // reclaimed; for the first slice, try_acquire a second permit — never queue
    // when it fails (nested blocking waits while every writer already holds one
    // permit would deadlock in mutual waiting), instead degrade to
    // single-buffered serial (C1 behavior), backpressure semantics unchanged
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
            co_await harvest_pending();  // serial degradation: wait for this slice to land, reclaim the buffer, continue
            permit_ = std::move(spare_permit_);
            buf_ = std::move(spare_);
        }
    }

    // Submit a write_full of the current buffer (without waiting): a single-object
    // op is atomic (no torn chunks) and its ack means durability on all replicas
    // (§4.1/§4.3). Buffer and permit ownership transfer to the in-flight ticket —
    // if abandoned, the memory librados is still reading and its accounted permit
    // both live until the callback lands
    Task<void> start_flush() {
        co_await store_->pool_->schedule();  // crc computation (CPU) stays off the HTTP driver thread
        rados_ioctx_t ctx = io(store_->conn_);
        // Batch-allocate contiguous runs with geometric growth (docs/archive/gaps.md §3.9,
        // same strategy as fs ChunkWriter): interleaved dispatch across concurrent
        // writers would defeat the manifest's run encoding. Segment allocation and
        // pinning must precede make_pending: alloc_/pin_one can throw, and at that
        // point there is no AioPending/completion yet to reclaim
        if (run_next_ == run_limit_) {
            run_len_ = run_len_ == 0 ? 1 : std::min<uint32_t>(run_len_ * 2, kMaxIdRun);
            run_next_ = store_->alloc_(Extent::Kind::kRados, run_len_);
            run_limit_ = run_next_ + run_len_;
        }
        const uint64_t file_id = run_next_++;
        // Pin upon allocation (docs/archive/gaps.md §1.2): this slice's object lands at T0,
        // but the whole PUT commits meta only at T0+Δ. If Δ exceeds gc_grace, the
        // orphan scan sees an object that is "absent from refs, mtime beyond grace,
        // unpinned" and deletes it outright; the PUT then commits successfully and
        // yields a bad object referencing deleted data. The pin is held until the
        // caller releases it after finish, or until this writer's destructor releases it
        store_->opt_.pins.pin_one(file_id);
        pinned_.push_back(file_id);
        AioPending* p = make_pending(store_->exec_, store_->m_lat_write_);
        p->file_id = file_id;
        p->len = buf_.size();
        p->crc = codec::crc32c_of(std::span<const std::byte>(buf_));
        p->data = std::move(buf_);
        p->permit = std::move(permit_);
        int r;
        if (int fe = fault::check("rados.submit")) {  // roadmap §6.1
            r = -fe;
        } else if (!owner_.empty()) {
            // Ownership persisted with the object (§6.1): a single-object write_op
            // is atomic — data and the owner xattr are either both present or both
            // absent, never the intermediate "object without ownership" state
            rados_write_op_t op = rados_create_write_op();
            rados_write_op_write_full(op, reinterpret_cast<const char*>(p->data.data()),
                                      p->data.size());
            rados_write_op_setxattr(op, kOwnerXattr, owner_.data(), owner_.size());
            r = rados_aio_write_op_operate(op, ctx, p->comp,
                                           RadosDataStore::object_name(p->file_id).c_str(),
                                           nullptr, 0);
            rados_release_write_op(op);
        } else {
            r = rados_aio_write_full(ctx, RadosDataStore::object_name(p->file_id).c_str(),
                                     p->comp, reinterpret_cast<const char*>(p->data.data()),
                                     p->data.size());
        }
        if (r < 0) {
            p->unref();
            p->unref();
            failed_ = true;
            store_->m_err_write_->inc();
            throw_rados("aio_write_full submit", r);
        }
        pending_ = p;
    }

    // Wait for the in-flight ticket to land and settle: success → extent enqueued,
    // buffer/permit reclaimed for reuse; failure enters failed state, no retry, no
    // reuse (§4.4)
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
    std::vector<std::byte> buf_;    // active buffer being filled (permit = permit_)
    std::vector<std::byte> spare_;  // buffer reclaimed from the previous ticket (permit = spare_permit_)
    std::vector<Extent> extents_;
    AsyncSemaphore::Permit permit_;
    AsyncSemaphore::Permit spare_permit_;
    AioPending* pending_ = nullptr;  // in-flight ticket (at most 1: receiving slice N+1 while writing slice N)
    std::vector<uint64_t> pinned_;   // write-side pins this writer established (cleared after finish)
    std::string owner_;              // each slice object gets kOwnerXattr on write (empty = skip)
    uint64_t run_next_ = 0, run_limit_ = 0;  // this session's contiguous id run (§3.9 batch allocation)
    uint32_t run_len_ = 0;
    bool finished_ = false;
    bool failed_ = false;
};

// ---------- RadosExtentReader: multi-object-chain streaming read (§5) ----------
// Structure mirrors the fs version's ExtentChainReader. Self-contained: holds a
// shared_ptr to Conn rather than a store pointer — the ObjectStream escapes the
// backend's lifetime along with the HTTP response. No fd concept; reads by name
// via aio one at a time (C3: park after submit, completion resumes through the
// self-held pool executor, pool threads no longer block waiting on the network);
// rados has no POSIX "an open fd is unaffected by unlink" fallback, so GC race
// protection relies entirely on pin+grace (§8.1).

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
            // crc verification is only feasible when reading the full extent from start to end (a Range hitting the middle cannot be verified, §5)
            crc_active_ = verify_crc_ && cur_off_ == 0 && remaining_ >= e.length;
            crc_acc_ = 0;
            at_start_ = false;
        }
        size_t want = size_t(std::min<uint64_t>({buf.size(), e.length - cur_off_, remaining_}));
        AioPending* p = make_pending(exec_, lat_);
        // Buffer handover with the same strategy as the write side (docs/archive/gaps.md
        // §3.9): the aio reads into the ticket's own buffer, copied to the caller
        // after completion. If it wrote into buf directly, a read timeout/cancel
        // destroying the coroutine frame along with the caller's buffer would let
        // the remote completion still write through freed memory
        p->data.resize(want);
        int r = fault::check("rados.submit");
        if (r) r = -r;
        else r = rados_aio_read(ctx, RadosDataStore::object_name(e.file_id).c_str(), p->comp,
                                reinterpret_cast<char*>(p->data.data()), want, cur_off_);
        if (r < 0) {
            p->unref();
            p->unref();
            err_->inc();
            throw_rados("aio_read submit", r);
        }
        int n = co_await AioAwait{p};  // completion → resumed on a pool thread (§6.2)
        if (n > 0) std::memcpy(buf.data(), p->data.data(), std::min(size_t(n), want));
        p->unref();
        if (n == -ENOENT) {
            // refs present but object missing = sign of data loss, or pin/grace failure (§6.3; same as main doc §10)
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
    ThreadPoolExecutor exec_;  // aio resume posting; self-held (the reader escapes the store's lifetime)
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
    // Unknown-length and known-length streams share the same buffered-slicing path
    // (§3.3/§4.2); content_length has no use here, and owner is persisted as an
    // xattr on each slice object (§6.1 disaster-recovery ownership)
    io(conn_);  // close guard
    co_return std::make_unique<RadosChunkWriter>(this, std::move(hint.owner));
}

Task<std::unique_ptr<http::BodyReader>> RadosDataStore::open_reader(DataRef ref, uint64_t first,
                                                                    uint64_t last) {
    uint64_t total = ref.total();
    if (first > last || last >= total)
        throw S3Error(S3ErrorCode::InternalError,
                      "duostore-rados: reader range beyond manifest");  // caller already ran resolve_range
    io(conn_);  // close guard
    co_return std::make_unique<RadosExtentReader>(conn_, pool_, opt_.verify_chunk_crc,
                                                  opt_.on_corruption, m_lat_read_, m_err_read_,
                                                  std::move(ref.extents), first, last);
}

Task<void> RadosDataStore::remove(std::span<const Extent> extents) {
    co_await pool_->schedule();
    rados_ioctx_t ctx = io(conn_);  // close guard (effective even for empty extents)
    // C3: windowed concurrent aio removes — GC of a large manifest (TiB-scale
    // objects, hundreds of thousands of extents) does not press the cluster
    // unboundedly; -ENOENT is idempotently ignored (§7.3), other negatives are
    // collected for the whole batch then the first is thrown (collect before
    // throw, so no in-flight ticket dangles)
    constexpr size_t kWindow = 16;
    size_t i = 0;
    while (i < extents.size()) {
        AioPending* batch[kWindow];
        size_t n = 0;
        int first_err = 0;
        for (; i < extents.size() && n < kWindow; ++i) {
            const auto& e = extents[i];
            if (e.kind != Extent::Kind::kRados) continue;  // foreign-kind extents (leftovers from a data engine switch) do not belong to this store
            AioPending* p = make_pending(exec_, m_lat_remove_);
            int r = fault::check("rados.submit");
            if (r) r = -r;
            else r = rados_aio_remove(ctx, object_name(e.file_id).c_str(), p->comp);
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

// The absence of a pack layer is a design boundary, not a debt (as characterized
// in docs/archive/gaps.md §6.1): pack aggregation targets the local fs's per-file cost
// (inode/fd/directory entry), while small-object amplification on the RADOS side
// is borne by BlueStore's min_alloc_size and the pool's replication policy;
// stacking another pack layer at the gateway would only introduce cross-object
// compaction and read-modify-write amplification (RADOS objects cannot be
// punch-holed anyway). Small-object-heavy deployments should configure
// EC/compression on the pool rather than expect gateway aggregation

Task<void> RadosDataStore::remove_pack(uint64_t pack_id) {
    // No packs: pack_stats is always empty, so this is never actually called (§3.3); explicit no-op rather than an interface default
    (void)pack_id;
    co_return;
}

Task<GcRewrite> RadosDataStore::rewrite_pack(uint64_t pack_id) {
    // No packs: meta never has a kRados pack record, the compaction candidate set is always empty, so this is never actually called (§3.3)
    (void)pack_id;
    co_return GcRewrite{};
}

Task<void> RadosDataStore::scan_chunks(
    const std::function<void(uint64_t file_id, int64_t mtime_ms, uint64_t size)>& cb) {
    // C4 (§8.2): full listing within the namespace (ioctx already scoped, multiple
    // instances invisible to each other) → parse file_id from the object name →
    // stat for mtime. Listing/stat have no aio variants; low frequency (default
    // 1/day) makes synchronous blocking on a pool thread acceptable (matching the
    // fs version's precedent of the whole scan residing on a pool thread); orphan
    // determination (refs/grace/pin) is the caller's job, this store only enumerates
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
        // Second-resolution mtime suffices (grace determination is minute-scale); stat failure = concurrent-remove race, tolerated and skipped
        if (rados_stat(ctx, entry, &size, &mtime) != 0) continue;
        cb(id, int64_t(mtime) * 1000, size);
    }
    rados_nobjects_list_close(lc);
    if (r != -ENOENT) {  // listing ends normally with -ENOENT
        m_err_scan_->inc();
        throw_rados("nobjects_list_next", r);
    }
    co_return;
}

}  // namespace lights3::storage::duostore
