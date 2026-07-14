// 进程装配与启动流程（docs/01-architecture.md §4）
#include <csignal>

#include "core/config.h"
#include "core/log.h"
#include "core/semaphore.h"
#include "core/thread_pool.h"
#include "http/server.h"
#include "s3/service.h"
#include "storage/bucket_router.h"
#include "storage/registry.h"

namespace {

lights3::http::IHttpServer* g_server = nullptr;

void on_signal(int) {
    if (g_server) g_server->shutdown();  // 仅 async-signal-safe 操作
}

lights3::LogLevel parse_level(const std::string& s) {
    if (s == "debug") return lights3::LogLevel::Debug;
    if (s == "warn") return lights3::LogLevel::Warn;
    if (s == "error") return lights3::LogLevel::Error;
    return lights3::LogLevel::Info;
}

}  // namespace

int main(int argc, char** argv) {
    using namespace lights3;

    std::string config_path = "config/lights3.yaml";
    for (int i = 1; i < argc; ++i) {
        std::string arg = argv[i];
        if ((arg == "--config" || arg == "-c") && i + 1 < argc) config_path = argv[++i];
        else if (arg == "--help" || arg == "-h") {
            printf("usage: lights3 [--config <path>]\n");
            return 0;
        }
    }

    try {
        auto cfg = Config::load(config_path);
        Logger::init(parse_level(cfg.log_level));

        auto pool = std::make_shared<ThreadPool>(cfg.runtime.io_threads);
        auto backends = storage::StorageRegistry::build(cfg.backends, pool);
        auto router = storage::BucketRouter::build(cfg.buckets, std::move(backends));
        auto auth = s3::SigV4Authenticator::build(cfg.auth);
        if (!auth.enabled())
            LOG_WARN("no credentials configured: authentication is DISABLED");
        auto service = std::make_shared<s3::S3Service>(std::move(router), std::move(auth),
                                                       cfg.http.base_domain);
        service->set_pool_stats([pool] { return pool->stats(); });

        auto server = http::HttpServerFactory::create(cfg.http.driver, cfg.http);
        // dispatch 入口限流（docs/03 §6）：超限请求在信号量上排队而非拒绝；
        // 等待者经池 executor 唤醒，避免在释放方调用栈上内联跑整条请求协程链
        auto pool_exec = std::make_shared<ThreadPoolExecutor>(*pool);
        auto inflight = std::make_shared<AsyncSemaphore>(cfg.runtime.max_inflight_requests,
                                                         pool_exec.get());
        server->set_handler([service, inflight, pool_exec](
                                http::HttpRequest req) -> Task<http::HttpResponse> {
            auto permit = co_await inflight->acquire();
            co_return co_await service->dispatch(std::move(req));
        });
        server->listen(cfg.http.bind, cfg.http.port);

        g_server = server.get();
        struct sigaction sa{};
        sa.sa_handler = on_signal;
        sigaction(SIGINT, &sa, nullptr);
        sigaction(SIGTERM, &sa, nullptr);
        signal(SIGPIPE, SIG_IGN);

        LOG_INFO("lights3 started: driver=", cfg.http.driver,
                 " backends=", cfg.backends.size(), " pool=", cfg.runtime.io_threads);
        server->run();  // 阻塞直至 SIGINT/SIGTERM

        g_server = nullptr;
        // 各后端冲刷 + 线程池收尾
        pool->join();
        LOG_INFO("lights3 exited cleanly");
        return 0;
    } catch (const std::exception& e) {
        fprintf(stderr, "fatal: %s\n", e.what());
        return 1;
    }
}
