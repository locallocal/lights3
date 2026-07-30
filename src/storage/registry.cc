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

// localfs 与 xlocalfs 共用的 root/staging 参数解析
std::pair<std::string, std::string> fs_backend_paths(const BackendConfig& cfg) {
    auto root = cfg.params.count("root") ? cfg.params.at("root") : "";
    if (root.empty())
        throw std::runtime_error(cfg.type + " backend '" + cfg.name + "' needs root");
    auto staging = cfg.params.count("staging") ? cfg.params.at("staging")
                                               : root + "/.lights3-staging";
    return {root, staging};
}

// per-backend 独立 IO 池（docs/concurrency.md §3.1 预留的兑现，todo.md §3.2）：
// params 带 io_threads（≥1）的后端获得专属 ThreadPool——云端慢请求占满共享池
// 会饿死本地盘路径，池按 backend 隔离即互不牵制；缺省共享全局池（多数部署的
// 正确选择，隔离是"确认了饿死征兆再开"的定向手段）。通用键：任意 type 生效，
// 各后端 from_params 对未知键不设防、无需逐个登记。
// 生命周期：后端持 shared_ptr，析构即 join（ThreadPool dtor）；指标回调闭包
// 亦持一份，注册表存活期 stats() 恒可安全调用（join 后只读）。
// 观测：专属池以 gauge 回调挂 backend 标签（渲染时拉取瞬时值），与全局池的
// lights3_pool_*（s3::Metrics 无标签渲染）名字空间错开，避免同名双 TYPE 行
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
            [](const BackendConfig& cfg, std::shared_ptr<ThreadPool> pool, MetricsScope) {
                auto [root, staging] = fs_backend_paths(cfg);
                return std::make_shared<LocalFsBackend>(root, staging, std::move(pool));
            });
        StorageRegistry::register_backend(
            "xlocalfs",
            [](const BackendConfig& cfg, std::shared_ptr<ThreadPool> pool, MetricsScope) {
                auto [root, staging] = fs_backend_paths(cfg);
                unsigned depth = cfg.params.count("queue_depth")
                                     ? std::stoul(cfg.params.at("queue_depth"))
                                     : 256;
                return std::make_shared<XLocalFsBackend>(root, staging, std::move(pool),
                                                         depth);
            });
        StorageRegistry::register_backend(
            "memory", [](const BackendConfig&, std::shared_ptr<ThreadPool>, MetricsScope) {
                return std::make_shared<MemoryBackend>();
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
    // 两阶段构建（docs/tiered-storage.md §2）：先构造全部叶子后端，再按依赖迭代构造组合后端
    std::map<std::string, std::shared_ptr<IStorageBackend>> out;
    std::vector<const BackendConfig*> deferred;
    {
        std::set<std::string> names;
        for (auto& cfg : configs)
            if (!names.insert(cfg.name).second)
                throw std::runtime_error("duplicate backend name: " + cfg.name);
    }
    for (auto& cfg : configs) {
        if (cfg.type == "tiered") {
            deferred.push_back(&cfg);
            continue;
        }
        auto it = registry().find(cfg.type);
        if (it == registry().end())
            throw std::runtime_error("unknown storage backend type: " + cfg.type);
        // 每后端一个 scope：实例级 backend=<name> 标签，工厂内按需再加维度
        MetricsScope scope(metrics, {{"backend", cfg.name}});
        out[cfg.name] = it->second(cfg, backend_pool(cfg, pool, scope), scope);
    }
    // tiered 引用 name 已就绪即可构造；tiered 套 tiered 按依赖序解开，解不开
    // 的（循环/未知引用）视为配置错误
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
                // 组合后端同样支持专属池（tiered 自身的下沉/回迁传输走它）
                out[cfg.name] = TieredBackend::from_config(
                    cfg, out, backend_pool(cfg, pool, MetricsScope(metrics, {{"backend", cfg.name}})));
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
    return out;
}

}  // namespace lights3::storage
