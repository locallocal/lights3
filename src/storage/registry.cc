#include "storage/registry.h"

#include <charconv>
#include <set>
#include <stdexcept>

#include "storage/localfs/localfs_backend.h"
#include "storage/memory/memory_backend.h"
#include "storage/tiered/tiered_backend.h"
#include "storage/xlocalfs/xlocalfs_backend.h"
#ifdef LIGHTS3_CLOUDPROXY
#include "storage/cloudproxy/cloudproxy_backend.h"
#endif
#ifdef LIGHTS3_DUOSTORE
#include "storage/duostore/duostore_backend.h"
#endif

namespace lights3::storage {

namespace {

std::map<std::string, BackendFactory>& registry() {
    static std::map<std::string, BackendFactory> r;
    return r;
}

// root/staging parameter parsing shared by localfs and xlocalfs
std::pair<std::string, std::string> fs_backend_paths(const BackendConfig& cfg) {
    auto root = cfg.params.count("root") ? cfg.params.at("root") : "";
    if (root.empty())
        throw std::runtime_error(cfg.type + " backend '" + cfg.name + "' needs root");
    auto staging = cfg.params.count("staging") ? cfg.params.at("staging")
                                               : root + "/.lights3-staging";
    return {root, staging};
}

// mpu_ttl / mpu_scan_interval (docs/archive/gaps.md §6.3): previously the 7-day TTL was hardcoded
// and only scanned once at startup
LocalFsOptions fs_backend_opts(const BackendConfig& cfg) {
    LocalFsOptions o;
    if (cfg.params.count("mpu_ttl")) o.mpu_ttl_sec = parse_duration_sec(cfg.params.at("mpu_ttl"));
    if (cfg.params.count("mpu_scan_interval"))
        o.mpu_scan_interval_sec = parse_duration_sec(cfg.params.at("mpu_scan_interval"));
    // roadmap §3.5 knobs (docs/storage/localfs.md)
    if (cfg.params.count("require_xattr")) o.require_xattr = parse_bool(cfg.params.at("require_xattr"));
    if (cfg.params.count("sidecar")) o.sidecar = fsutil::parse_sidecar_mode(cfg.params.at("sidecar"));
    auto small_int = [&](const char* k, int lo, int hi) {
        const std::string& v = cfg.params.at(k);
        int n = 0;
        auto r = std::from_chars(v.data(), v.data() + v.size(), n);
        if (r.ec != std::errc() || r.ptr != v.data() + v.size() || n < lo || n > hi)
            throw std::runtime_error(cfg.type + " backend '" + cfg.name + "': " + k +
                                     " must be an integer in [" + std::to_string(lo) + "," +
                                     std::to_string(hi) + "]");
        return n;
    };
    if (cfg.params.count("list_meta_concurrency"))
        o.list_meta_concurrency = small_int("list_meta_concurrency", 1, 256);
    if (cfg.params.count("list_cache_entries"))
        o.list_cache_entries = parse_size(cfg.params.at("list_cache_entries"));  // plain count; K/M suffixes allowed
    if (cfg.params.count("list_cache_min_dir_entries"))
        o.list_cache_min_dir_entries = size_t(small_int("list_cache_min_dir_entries", 1, 1 << 30));
    if (cfg.params.count("sidecar_scan_interval"))
        o.sidecar_scan_interval_sec = parse_duration_sec(cfg.params.at("sidecar_scan_interval"));
    return o;
}

// Per-backend dedicated IO pool (fulfilling the reservation in docs/concurrency.md §3.1):
// a backend whose params carry io_threads (>=1) gets its own ThreadPool -- slow cloud
// requests filling the shared pool would starve the local-disk path; isolating pools per
// backend keeps them from dragging each other down. Default is the shared global pool (the
// right choice for most deployments; isolation is a targeted measure to "enable once
// starvation symptoms are confirmed"). Generic key: effective for any type; each backend's
// from_params does not guard against unknown keys, so no per-backend registration needed.
// Lifetime: the backend holds the shared_ptr, destruction joins (ThreadPool dtor); the
// metrics callback closures also hold a copy, so stats() stays safe to call for the
// registry's lifetime (read-only after join).
// Observability: the dedicated pool hangs gauge callbacks with the backend label (instant
// values pulled at render time), namespaced apart from the global pool's lights3_pool_*
// (rendered by s3::Metrics without labels) to avoid duplicate TYPE lines of the same name
std::shared_ptr<ThreadPool> backend_pool(const BackendConfig& cfg,
                                         const std::shared_ptr<ThreadPool>& shared,
                                         const MetricsScope& scope) {
    auto it = cfg.params.find("io_threads");
    if (it == cfg.params.end()) return shared;
    const std::string& v = it->second;
    int n = 0;
    auto r = std::from_chars(v.data(), v.data() + v.size(), n);
    if (r.ec != std::errc() || r.ptr != v.data() + v.size() || n < 1 || n > 1024)
        throw std::runtime_error("backend '" + cfg.name +
                                 "': io_threads must be an integer in [1,1024]");
    auto p = std::make_shared<ThreadPool>(size_t(n));
    scope.gauge_callback("lights3_backend_pool_threads",
                         "Dedicated per-backend IO pool size (io_threads)",
                         [n] { return double(n); });
    scope.gauge_callback("lights3_backend_pool_queue_depth",
                         "Dedicated per-backend IO pool ready-queue depth",
                         [p] { return double(p->stats().queue_depth); });
    scope.gauge_callback("lights3_backend_pool_backlogged",
                         "Dedicated per-backend IO pool schedule() tasks held by backpressure",
                         [p] { return double(p->stats().backlogged); });
    scope.gauge_callback("lights3_backend_pool_completed",
                         "Dedicated per-backend IO pool completed tasks (monotonic)",
                         [p] { return double(p->stats().completed); });
    return p;
}

void ensure_registered() {
    static bool done = [] {
        StorageRegistry::register_backend(
            "localfs",
            [](const BackendConfig& cfg, std::shared_ptr<ThreadPool> pool, MetricsScope m) {
                auto [root, staging] = fs_backend_paths(cfg);
                return std::make_shared<LocalFsBackend>(root, staging, std::move(pool),
                                                        fs_backend_opts(cfg), std::move(m));
            });
        StorageRegistry::register_backend(
            "xlocalfs",
            [](const BackendConfig& cfg, std::shared_ptr<ThreadPool> pool,
               MetricsScope m) -> std::shared_ptr<IStorageBackend> {
                auto [root, staging] = fs_backend_paths(cfg);
                UringOptions uo;
                if (cfg.params.count("queue_depth"))
                    uo.entries = unsigned(std::stoul(cfg.params.at("queue_depth")));
                if (cfg.params.count("sqpoll")) uo.sqpoll = parse_bool(cfg.params.at("sqpoll"));
                if (cfg.params.count("sqpoll_idle"))
                    uo.sqpoll_idle_ms = parse_duration_sec(cfg.params.at("sqpoll_idle")) * 1000;
                // roadmap §3.4: ring sharding, registered buffers/files, stream depths
                if (cfg.params.count("rings"))
                    uo.rings = unsigned(std::stoul(cfg.params.at("rings")));  // 0 = auto
                if (cfg.params.count("fixed_buffers"))
                    uo.fixed_buffers = unsigned(std::stoul(cfg.params.at("fixed_buffers")));
                if (cfg.params.count("fixed_files"))
                    uo.fixed_files = unsigned(std::stoul(cfg.params.at("fixed_files")));
                if (cfg.params.count("block_size"))
                    uo.block_size = unsigned(parse_size(cfg.params.at("block_size")));
                if (cfg.params.count("read_depth"))
                    uo.read_depth = unsigned(std::stoul(cfg.params.at("read_depth")));
                if (cfg.params.count("write_depth"))
                    uo.write_depth = unsigned(std::stoul(cfg.params.at("write_depth")));
                if (cfg.params.count("meta_ops"))
                    uo.meta_ops = parse_bool(cfg.params.at("meta_ops"));
                try {
                    return std::make_shared<XLocalFsBackend>(root, staging, pool, uo,
                                                             fs_backend_opts(cfg), m);
                } catch (const std::exception& e) {
                    // io_uring being unavailable (old kernel, container seccomp blocking
                    // io_uring_setup, insufficient memlock quota) used to crash the whole
                    // process (docs/archive/gaps.md §6.3). xlocalfs and localfs share the exact
                    // same on-disk layout and metadata semantics -- the fallback is
                    // lossless, only async IO is lost. Warn loudly, no silent degradation
                    LOG_WARN("xlocalfs backend '{}': io_uring unavailable ({}); falling back "
                             "to the localfs data path (same on-disk layout, synchronous IO)",
                             cfg.name, e.what());
                    // The warning is a single log line at startup that vanishes after
                    // rotation; a persistent gauge keeps "thought we were running async IO
                    // but actually fell back to sync" visible on the monitoring plane
                    m.gauge("lights3_xlocalfs_uring_fallback",
                            "io_uring unavailable, fell back to localfs backend")
                        ->set(1);
                    return std::make_shared<LocalFsBackend>(root, staging, std::move(pool),
                                                            fs_backend_opts(cfg), std::move(m));
                }
            });
        StorageRegistry::register_backend(
            "memory",
            [](const BackendConfig& cfg, std::shared_ptr<ThreadPool>, MetricsScope m) {
                MemoryOptions mo;
                if (cfg.params.count("max_bytes"))
                    mo.max_bytes = parse_size(cfg.params.at("max_bytes"));
                if (cfg.params.count("mpu_ttl"))
                    mo.mpu_ttl_sec = parse_duration_sec(cfg.params.at("mpu_ttl"));
                auto b = std::make_shared<MemoryBackend>(mo);
                // Usage observability (docs/archive/gaps.md §6.3): a misconfigured memory backend
                // costs an OOM, so at least make "how far from the limit" visible. The
                // callback gauge reads the value only at render time
                m.gauge_callback("lights3_memory_backend_used_bytes",
                                 "Bytes resident in the memory backend (objects + inflight parts)",
                                 [w = std::weak_ptr<MemoryBackend>(b)] {
                                     auto sp = w.lock();
                                     return sp ? double(sp->used_bytes()) : 0.0;
                                 });
                return b;
            });
#ifdef LIGHTS3_CLOUDPROXY
        StorageRegistry::register_backend(
            "cloudproxy",
            [](const BackendConfig& cfg, std::shared_ptr<ThreadPool> pool, MetricsScope m) {
                auto c = CloudProxyConfig::from_params(cfg.name, cfg.params);
                return std::make_shared<CloudProxyBackend>(std::move(c), std::move(pool),
                                                           std::move(m));
            });
#endif
#ifdef LIGHTS3_DUOSTORE
        StorageRegistry::register_backend(
            "duostore",
            [](const BackendConfig& cfg, std::shared_ptr<ThreadPool> pool, MetricsScope m) {
                auto c = DuoStoreConfig::from_params(cfg.name, cfg.params);
                return std::make_shared<DuoStoreBackend>(std::move(c), std::move(pool),
                                                         std::move(m));
            });
#endif
        return true;
    }();
    (void)done;
}

}  // namespace

void StorageRegistry::register_backend(const std::string& type, BackendFactory factory) {
    registry()[type] = std::move(factory);
}

std::map<std::string, std::shared_ptr<IStorageBackend>> StorageRegistry::build(
    const std::vector<BackendConfig>& configs, std::shared_ptr<ThreadPool> pool,
    std::shared_ptr<MetricsRegistry> metrics) {
    ensure_registered();
    // Two-phase build (docs/tiered-storage.md §2): construct all leaf backends first, then
    // construct composite backends iteratively by dependency
    std::map<std::string, std::shared_ptr<IStorageBackend>> out;
    std::vector<const BackendConfig*> deferred;
    {
        std::set<std::string> names;
        for (auto& cfg : configs)
            if (!names.insert(cfg.name).second)
                throw std::runtime_error("duplicate backend name: " + cfg.name);
    }
    // A mid-build failure must roll back: already-built backends each hold a dedicated
    // ThreadPool (threads already started) and a set of metric gauge callbacks (closures
    // holding the pool's shared_ptr). Letting the exception escape directly would leave
    // orphan pools and callbacks reading stale stats, and main never reaches the close()
    // path. The guard clears out on the exception path (backend dtor -> pool join) and
    // removes the metrics registered by this build
    struct BuildRollback {
        std::map<std::string, std::shared_ptr<IStorageBackend>>* out;
        MetricsRegistry* metrics;
        std::vector<std::string> scopes;
        bool done = false;
        ~BuildRollback() {
            if (done) return;
            if (metrics)  // test harnesses may run without a registry
                for (auto& name : scopes) metrics->remove_labeled("backend", name);
            out->clear();
        }
    } rollback{&out, metrics.get(), {}};

    for (auto& cfg : configs) {
        if (cfg.type == "tiered") {
            deferred.push_back(&cfg);
            continue;
        }
        auto it = registry().find(cfg.type);
        if (it == registry().end())
            throw std::runtime_error("unknown storage backend type: " + cfg.type);
        // One scope per backend: instance-level backend=<name> label; factories add more
        // dimensions as needed
        MetricsScope scope(metrics, {{"backend", cfg.name}});
        rollback.scopes.push_back(cfg.name);
        out[cfg.name] = it->second(cfg, backend_pool(cfg, pool, scope), scope);
    }
    // A tiered backend can be constructed once its referenced names are ready; tiered
    // nesting tiered unwinds in dependency order, and anything that cannot be unwound
    // (cycles / unknown references) is a configuration error
    bool progress = true;
    while (!deferred.empty() && progress) {
        progress = false;
        for (auto it = deferred.begin(); it != deferred.end();) {
            auto& cfg = **it;
            auto local = cfg.params.count("local") ? cfg.params.at("local") : "";
            auto cloud = cfg.params.count("cloud") ? cfg.params.at("cloud") : "";
            if (local.empty() || cloud.empty())
                throw std::runtime_error("tiered backend '" + cfg.name + "' needs local + cloud");
            if (out.count(local) && out.count(cloud)) {
                // Composite backends support a dedicated pool too (tiered's own
                // demotion/promotion transfers run on it).
                // Register the scope before constructing: if the tiered build throws, its
                // gauge callbacks are already registered and hold the pool's shared_ptr;
                // missing the registration means the threads never join after rollback --
                // exactly the scenario this guard exists to prevent (docs/archive/gaps.md §3.9)
                rollback.scopes.push_back(cfg.name);
                // Pool metrics and the backend's own metrics share the same scope (the
                // registry's get-or-create is idempotent; re-constructing with the same
                // label is harmless)
                MetricsScope scope(metrics, {{"backend", cfg.name}});
                out[cfg.name] = TieredBackend::from_config(
                    cfg, out, backend_pool(cfg, pool, scope), scope);
                it = deferred.erase(it);
                progress = true;
            } else {
                ++it;
            }
        }
    }
    if (!deferred.empty())
        throw std::runtime_error("tiered backend '" + deferred.front()->name +
                                 "' has unknown or circular local/cloud reference");
    rollback.done = true;  // everything ready: disarm the guard, hand over the backend table
    return out;
}

}  // namespace lights3::storage
