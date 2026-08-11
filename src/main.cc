// 进程装配与启动流程（docs/architecture.md §4）
#include <csignal>
#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <thread>

#include <gflags/gflags.h>

#include "core/config.h"
#include "core/log.h"
#include "core/metrics.h"
#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "http/server.h"
#include "http/stall_guard.h"
#include "s3/auth/credential_store.h"
#include "s3/errors.h"
#include "s3/service.h"
#include "storage/bucket_router.h"
#include "storage/registry.h"
#ifdef LIGHTS3_DUOSTORE
#include <fstream>

#include "storage/duostore/duostore_backend.h"
#endif

namespace {

// self-pipe：信号处理器里只做一次 write（async-signal-safe），真正的 shutdown 由
// 守护线程执行。直接在处理器里调 server->shutdown() 是不安全的——httplib 驱动的
// 实现要取内部锁，信号恰好落在已持该锁的线程上即自死锁（docs/gaps.md §3.9）
int g_sig_pipe[2] = {-1, -1};

void on_signal(int sig) {
    unsigned char b = static_cast<unsigned char>(sig);
    ssize_t n = ::write(g_sig_pipe[1], &b, 1);
    (void)n;  // 管道满说明已有待处理信号，丢弃即可
}

lights3::LogLevel parse_level(const std::string& s) {
    if (s == "debug") return lights3::LogLevel::Debug;
    if (s == "warn") return lights3::LogLevel::Warn;
    if (s == "error") return lights3::LogLevel::Error;
    return lights3::LogLevel::Info;
}

}  // namespace

DEFINE_string(config, "config/lights3.yaml", "Path to the lights3 YAML config file");
DEFINE_string(duostore_admin, "",
              "duostore meta admin (docs/gaps.md §6.1): 'dump:<backend>:<file>' or "
              "'load:<backend>:<file>'. Runs before the server starts (no traffic) and "
              "exits; load ends with a forced orphan scan. Backup order: copy the data "
              "dir first, then dump meta; restore data first, then load.");

#ifdef LIGHTS3_DUOSTORE
namespace {

// --duostore_admin 入口：backends 已构建、server 未启动（停写天然成立）。
// 返回进程退出码；任何失败响亮抛出由 main 的兜底路径收敛
int run_duostore_admin(
    const std::string& spec,
    const std::map<std::string, std::shared_ptr<lights3::storage::IStorageBackend>>& backends) {
    using namespace lights3;
    auto c1 = spec.find(':');
    auto c2 = c1 == std::string::npos ? std::string::npos : spec.find(':', c1 + 1);
    if (c2 == std::string::npos)
        throw std::runtime_error("--duostore_admin expects dump:<backend>:<file> or "
                                 "load:<backend>:<file>, got: " + spec);
    std::string cmd = spec.substr(0, c1);
    std::string name = spec.substr(c1 + 1, c2 - c1 - 1);
    std::string path = spec.substr(c2 + 1);
    auto it = backends.find(name);
    if (it == backends.end())
        throw std::runtime_error("--duostore_admin: no backend named '" + name + "'");
    auto* duo = dynamic_cast<storage::DuoStoreBackend*>(it->second.get());
    if (!duo)
        throw std::runtime_error("--duostore_admin: backend '" + name + "' is not duostore");
    if (cmd == "dump") {
        std::ofstream f(path, std::ios::binary | std::ios::trunc);
        if (!f) throw std::runtime_error("--duostore_admin: cannot open for write: " + path);
        auto st = sync_wait(duo->run_meta_dump(f));
        LOG_INFO("duostore admin: dumped {} buckets / {} objects / {} sealed packs to {}",
                 st.buckets, st.objects, st.sealed_packs, path);
    } else if (cmd == "load") {
        std::ifstream f(path, std::ios::binary);
        if (!f) throw std::runtime_error("--duostore_admin: cannot open for read: " + path);
        auto st = sync_wait(duo->run_meta_load(f));
        LOG_INFO("duostore admin: loaded {} buckets / {} objects / {} sealed packs from {}",
                 st.buckets, st.objects, st.sealed_packs, path);
    } else {
        throw std::runtime_error("--duostore_admin: unknown command '" + cmd + "'");
    }
    return 0;
}

}  // namespace
#endif  // LIGHTS3_DUOSTORE

int main(int argc, char** argv) {
    using namespace lights3;

    gflags::SetUsageMessage("S3-compatible object storage server.\nusage: lights3 [--config <path>]");
    gflags::ParseCommandLineFlags(&argc, &argv, /*remove_flags=*/true);

    // 关停时要逐个 close()：router 只按桶路由，拿不到全集。声明在 try 之外，
    // 让启动后期失败的异常路径也能走同一份冲刷逻辑
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> all_backends;
    auto close_backends = [&all_backends] {
        // close() 须在 pool->join() **之前**——它内部要 co_await pool->schedule()
        //（join 后的池会抛 post-after-join），且析构兜底不等于 close（duostore 跳过
        // active pack 封存与 rados flush、tiered 丢 atime 快照）。逐个 close，
        // 单个失败不阻断其余；重复调用无害（close 幂等）
        for (auto& [name, backend] : all_backends) {
            try {
                sync_wait(backend->close());
            } catch (const std::exception& e) {
                LOG_ERROR("backend {} close failed: {}", name, e.what());
            }
        }
    };

    try {
        auto cfg = Config::load(FLAGS_config);
        Logger::init(parse_level(cfg.log_level));

        auto pool = std::make_shared<ThreadPool>(cfg.runtime.io_threads);
        // 后端级 metrics 注册表：build 给每个后端派发
        // backend=<name> 标签的 scope，/-/metrics 追加渲染
        auto metrics = std::make_shared<MetricsRegistry>();
        auto backends = storage::StorageRegistry::build(cfg.backends, pool, metrics);
        all_backends = backends;
        if (!FLAGS_duostore_admin.empty()) {
#ifdef LIGHTS3_DUOSTORE
            int rc = run_duostore_admin(FLAGS_duostore_admin, all_backends);
            // 后端析构须先于 pool->join()（析构里还要用池，见下方正常关停注释）
            close_backends();
            backends.clear();
            all_backends.clear();
            pool->join();
            return rc;
#else
            throw std::runtime_error("--duostore_admin requires a build with LIGHTS3_DUOSTORE");
#endif
        }
        auto router = storage::BucketRouter::build(cfg.buckets, std::move(backends));
        auto auth = s3::SigV4Authenticator::build(cfg.auth);
        // 动态凭证（docs/credential-management.md）：从默认后端加载并替换静态查表
        auto cred_store =
            sync_wait(s3::CredentialStore::load(router.default_backend(), cfg.auth));
        auth.set_provider(cred_store);
        if (!auth.enabled())
            LOG_WARN("no credentials configured: authentication is DISABLED");
        auto service = std::make_shared<s3::S3Service>(std::move(router), std::move(auth),
                                                       cfg.http.base_domain);
        service->set_pool_stats([pool] { return pool->stats(); });
        service->set_request_timeout(std::chrono::seconds(cfg.http.request_timeout_sec));
        service->set_min_part_size(cfg.http.min_part_size);
        service->set_backend_metrics(metrics);
        service->set_credential_store(cred_store);
        // 二期后台任务（docs/credential-management.md §10.2/§10.3）：
        // credentials_file 热加载轮询 + 多实例定期增量同步（均按配置门控）
        cred_store->start_background(pool);

        auto server = http::HttpServerFactory::create(cfg.http.driver, cfg.http);
        // dispatch 入口限流（docs/concurrency.md §6）：超限请求在信号量上排队而非拒绝；
        // 等待者经池 executor 唤醒，避免在释放方调用栈上内联跑整条请求协程链
        auto pool_exec = std::make_shared<ThreadPoolExecutor>(*pool);
        auto inflight = std::make_shared<AsyncSemaphore>(cfg.runtime.max_inflight_requests,
                                                         pool_exec.get());
        // 准入闸门与定时器线程的可观测性（docs/gaps.md §7）：压测时"卡在准入"
        // 还是"卡在池"、"定时器被慢回调堵了多久"都从 /-/metrics 直接读
        service->set_admission_stats(
            [inflight, cap = cfg.runtime.max_inflight_requests]() -> s3::AdmissionStats {
                return {cap, inflight->available(), inflight->waiting()};
            });
        service->set_timer_stats([] { return TimerQueue::instance().stats(); });
        // 进程关停广播（docs/concurrency.md §5 的第三个取消源）：run() 返回后触发，
        // 在途请求从最近的可取消挂起点收敛，不必干等各自的 request_timeout
        auto shutdown_src = std::make_shared<CancelSource>();
        const int max_inflight = cfg.runtime.max_inflight_requests;
        auto stall = std::chrono::seconds(cfg.http.transfer_stall_timeout_sec);
        server->set_handler([service, inflight, pool_exec, shutdown_src, stall](
                                http::HttpRequest req) -> Task<http::HttpResponse> {
            // driver 已挂上本连接的 token 时保留它，否则至少接上关停源
            if (!req.cancel.valid()) req.cancel = shutdown_src->token();
            CancelToken tok = req.cancel;
            try {
                auto permit = co_await inflight->acquire(tok);
                // 传输停滞守卫（docs/gaps.md §3.3）：收发两个方向都包一层。装在
                // L1/L2 交界处，四驱动一次性生效
                req.body = http::guard_stalls(std::move(req.body), stall);
                auto resp = co_await service->dispatch(std::move(req));
                resp.stream_body = http::guard_stalls(std::move(resp.stream_body), stall);
                co_return resp;
            } catch (const OperationCancelled&) {
                // 排队期间被关停/超时取消：503 让 SDK 重试
                http::HttpResponse r;
                r.status = 503;
                r.headers.set("Content-Type", "application/xml");
                r.small_body = s3::error_xml(
                    s3::S3Error(s3::S3ErrorCode::SlowDown,
                                "Request cancelled while queued (server shutting down or "
                                "request timed out)."),
                    "-");
                co_return r;
            }
        });
        server->listen(cfg.http.bind, cfg.http.port);

        // 信号 → self-pipe → 守护线程 shutdown（见 on_signal 的注释）
        if (::pipe2(g_sig_pipe, O_CLOEXEC) != 0)
            throw std::runtime_error("cannot create signal pipe");
        std::thread sig_thread([&server] {
            unsigned char b = 0;
            while (::read(g_sig_pipe[0], &b, 1) == 1) {
                LOG_INFO("signal {} received, shutting down", int(b));
                server->shutdown();
            }
        });
        struct sigaction sa{};
        sa.sa_handler = on_signal;
        sigemptyset(&sa.sa_mask);  // 未初始化的 mask 是未定义的阻塞集合
        sa.sa_flags = SA_RESTART;  // 自管道方案下无需以 EINTR 打断系统调用
        sigaction(SIGINT, &sa, nullptr);
        sigaction(SIGTERM, &sa, nullptr);
        signal(SIGPIPE, SIG_IGN);

        LOG_INFO("lights3 started: driver={} backends={} pool={}", cfg.http.driver,
                 cfg.backends.size(), cfg.runtime.io_threads);
        server->run();  // 阻塞直至 SIGINT/SIGTERM

        // 关停守护线程先收：之后才能安全析构 server
        ::close(g_sig_pipe[1]);
        g_sig_pipe[1] = -1;
        if (sig_thread.joinable()) sig_thread.join();
        ::close(g_sig_pipe[0]);
        g_sig_pipe[0] = -1;

        // run() 返回**不等于**在途请求已归零（驱动是宽限 + 强关后无条件返回，
        // docs/gaps.md §2.1）。先广播取消让在途请求从挂起点收敛，再等许可归位，
        // 否则下面的 close() 会与仍在跑的请求并发碰同一个后端
        shutdown_src->request_cancel();
        inflight->close();
        {
            // 驱动侧的连接宽限是另一个量（drivers/common.h kShutdownGrace），这里
            // 等的是许可归位。字面量只写一处：日志里再抄一份 "10s" 迟早对不上
            constexpr auto kDrainDeadline = std::chrono::seconds(10);
            auto deadline = std::chrono::steady_clock::now() + kDrainDeadline;
            while (inflight->available() < max_inflight &&
                   std::chrono::steady_clock::now() < deadline)
                std::this_thread::sleep_for(std::chrono::milliseconds(20));
            if (inflight->available() < max_inflight)
                LOG_ERROR("{} request(s) still in flight after {}s; proceeding with shutdown",
                          max_inflight - inflight->available(), kDrainDeadline.count());
        }

        cred_store->shutdown_background();  // 定时器/在途同步须先于线程池收尾
        close_backends();
        // 后端的 shared_ptr 还被 service（经 router）与 handler（经 server）持有，
        // 单清 all_backends 不会触发析构。按持有关系反序释放，使后端析构发生在
        // pool->join() **之前**——析构里还要用池（docs/gaps.md §3.9）
        server.reset();
        service.reset();
        cred_store.reset();
        all_backends.clear();
        pool->join();
        LOG_INFO("lights3 exited cleanly");
        return 0;
    } catch (const std::exception& e) {
        // 启动后期失败（listen 冲突、装配抛出）此前直接 return 1，跳过全部 close()：
        // duostore 的 active pack 不封存、rados 不 flush，下次启动要走崩溃恢复
        close_backends();
        fprintf(stderr, "fatal: %s\n", e.what());
        return 1;
    }
}
