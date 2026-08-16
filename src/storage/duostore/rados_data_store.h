// L3: Ceph/RADOS implementation of IDataStore (docs/duostore-rados-data.md).
// Single path: slice buffering → one write_full per rados object (no packs, no
// compaction, no torn tail, §3.3). Since C3 all IO goes through rados_aio_*:
// completion callbacks only post the continuation back to this process's pool
// executor (§6.2 discipline); the write side double-buffers as a pipeline
// (receiving slice N+1 while writing slice N, §4.2); since C4 scan_chunks orphan
// enumeration and op latency/error metrics (§8.2/§10).
// Pruned at compile time via LIGHTS3_DUOSTORE_RADOS_DATA (§9.2).
#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <memory>
#include <string>

#include "core/metrics.h"
#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "storage/duostore/data_store.h"

namespace lights3::storage::duostore {

struct RadosDataOptions {
    std::string conf_path = "/etc/ceph/ceph.conf";  // mon addresses and keyring reference
    std::string client_name = "client.admin";       // cephx identity
    std::string pool;                               // required; replication/EC policy is pool-level (§3.2)
    std::string ns;                                 // rados namespace (empty = default)
    uint64_t chunk_size = 8ull << 20;               // slice granularity = per-object cap (§3.4)
    uint64_t buffer_total = 256ull << 20;           // total writer buffer budget (§4.2)
    int connect_timeout_sec = 5;                    // client_mount_timeout
    int op_timeout_sec = 0;                         // 0 = unset (§6.4)
    bool verify_chunk_crc = false;                  // same semantics as the fs version (§5)
    // Reporting of crc mismatches on the read path (P5 corruption metric; empty =
    // no reporting); same lifetime constraints as the fs version
    std::function<void()> on_corruption;
    // op latency/error metrics (C4, §10); an empty scope means an isolated
    // instance — tests construct directly without assembly
    MetricsScope metrics;
    // Write-side pins (docs/gaps.md §1.2): pin upon allocating a file_id, unpin on
    // destruction without finish. Without this, the early parts of a large-object
    // PUT get deleted by the orphan scan as unreferenced files before the meta commit
    ChunkPinHooks pins;
};

class RadosChunkWriter;

class RadosDataStore final : public IDataStore {
public:
    // Returns the first id of a contiguous run [first, first+n)
    // (IMetaStore::alloc_file_run, docs/gaps.md §3.9: only contiguous ids make the
    // manifest's run encoding effective)
    using FileIdAlloc = std::function<uint64_t(Extent::Kind, uint32_t)>;

    // Connects on construction (fail fast, §6.1); throws std::runtime_error on
    // failure (configuration/environment error class)
    RadosDataStore(RadosDataOptions opt, std::shared_ptr<ThreadPool> pool, FileIdAlloc alloc);
    // Fallback (normal path calls close() first): drain in-flight writes, then
    // release our Conn reference — an abandoned writer's completion callbacks
    // reference exec_/buffer_sem_, so rados_aio_flush must wait for the callbacks
    // to finish first
    ~RadosDataStore() override;
    RadosDataStore(const RadosDataStore&) = delete;

    Task<std::unique_ptr<DataWriter>> open_writer(WriteHint hint) override;
    Task<std::unique_ptr<http::BodyReader>> open_reader(DataRef ref, uint64_t first,
                                                        uint64_t last) override;
    Task<void> remove(std::span<const Extent> extents) override;
    Task<void> remove_pack(uint64_t pack_id) override;        // no-op (no packs, §3.3)
    Task<GcRewrite> rewrite_pack(uint64_t pack_id) override;  // always {} (no packs, §3.3)
    // Orphan-scan enumeration (C4, §8.2): rados_nobjects_list_* (ioctx already
    // limited to the namespace) + rados_stat; foreign objects not matching our
    // naming (not c.<016x>) are ignored
    Task<void> scan_chunks(
        const std::function<void(uint64_t file_id, int64_t mtime_ms, uint64_t size)>& cb)
        override;
    Task<void> close() override;

    // Object naming: c.<file_id:016x> (§3.1); for test observation
    static std::string object_name(uint64_t file_id);

    // Connection state shared via reference counting (implementation detail;
    // public only so reader/guard functions in the .cc can name it): readers
    // escape the backend's lifetime along with the HTTP response (matching
    // ExtentChainReader's self-contained semantics), ioctx/cluster are released by
    // the last holder; after close() sets closed, new ops throw a clean 500
    // (§6.5 guard)
    struct Conn {
        void* cluster = nullptr;  // rados_t
        void* ioctx = nullptr;    // rados_ioctx_t
        std::atomic<bool> closed{false};
        void shutdown();  // idempotent: ioctx_destroy + rados_shutdown
        ~Conn() { shutdown(); }
    };

private:
    friend class RadosChunkWriter;

    std::shared_ptr<Conn> conn_;
    RadosDataOptions opt_;
    std::shared_ptr<ThreadPool> pool_;
    FileIdAlloc alloc_;
    ThreadPoolExecutor exec_;    // semaphore wakeups and aio completion continuations all post via the pool (§6.2)
    AsyncSemaphore buffer_sem_;  // permits = buffer_total / chunk_size (§4.2)

    // op latency histograms (submit → completion callback, including the cluster
    // round trip) and error counters (C4, §10); registered at construction so
    // zero values are visible. shared_ptr keeps increments safe after
    // readers/in-flight ops escape
    std::shared_ptr<MetricHistogram> m_lat_write_, m_lat_read_, m_lat_remove_;
    std::shared_ptr<MetricCounter> m_err_write_, m_err_read_, m_err_remove_, m_err_scan_;
};

}  // namespace lights3::storage::duostore
