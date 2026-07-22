// RedisMetaStore 专项单测（docs/duostore-redis-meta.md §9）：meta 一致性套件、
// 注入组合跑后端套件、前缀隔离、NOSCRIPT 自愈、swap_extents CAS、多网关共 meta、
// 并发 CAS 收敛。真实 redis 获取：探测 PATH 中的 redis-server，以 unix socket
// 拉起私有实例（--save '' --appendonly no）；找不到则显式 SKIP（不算失败）。
// LIGHTS3_TEST_REDIS_URI 可覆盖为外部实例（隔离靠每用例随机 key 前缀）。
#if defined(LIGHTS3_DUOSTORE) && defined(LIGHTS3_DUOSTORE_REDIS_META)

#include <hiredis.h>
#include <signal.h>
#include <sys/wait.h>
#include <unistd.h>

#include <atomic>
#include <cstdlib>
#include <memory>
#include <set>
#include <string>
#include <thread>

#include "core/thread_pool.h"
#include "storage/duostore/duostore_backend.h"
#include "storage/duostore/fs_data_store.h"
#include "storage/duostore/redis_meta_store.h"
#include "unit/backend_suite.h"
#include "unit/meta_store_suite.h"
#include "unit/mini_test.h"

namespace fs = std::filesystem;
using namespace lights3;
using namespace lights3::storage;
using namespace lights3::storage::duostore;

namespace {

// 私有 redis-server 的进程级单例：首个用例惰性拉起，进程退出时回收。
// unix socket 免端口分配冲突；--appendonly no（单测不测崩溃语义，与 rocks
// 单测 sync=false 同一取舍）。
class RedisTestServer {
public:
    static RedisTestServer& instance() {
        static RedisTestServer srv;
        return srv;
    }

    bool available = false;
    std::string uri;

    // 直连服务端发管理命令（SCRIPT FLUSH 等测试注入用）
    bool raw_command(const char* cmd) {
        redisContext* ctx = connect_raw();
        if (!ctx) return false;
        auto* r = static_cast<redisReply*>(redisCommand(ctx, cmd));
        bool ok = r && r->type != REDIS_REPLY_ERROR;
        if (r) freeReplyObject(r);
        redisFree(ctx);
        return ok;
    }

private:
    RedisTestServer() {
        if (const char* env = std::getenv("LIGHTS3_TEST_REDIS_URI"); env && *env) {
            uri = env;
            available = true;
            return;
        }
        char tmpl[] = "/tmp/lights3-redis-test-XXXXXX";
        if (!mkdtemp(tmpl)) return;
        dir_ = tmpl;
        sock_ = dir_ + "/redis.sock";
        pid_ = fork();
        if (pid_ == 0) {
            // 子进程：日志留在临时目录便于排障；execlp 失败即退出（父侧超时判 skip）
            std::string logfile = dir_ + "/redis.log";
            if (FILE* f = fopen(logfile.c_str(), "w")) {
                dup2(fileno(f), 1);
                dup2(fileno(f), 2);
            }
            execlp("redis-server", "redis-server", "--port", "0", "--unixsocket",
                   sock_.c_str(), "--save", "", "--appendonly", "no", "--dir", dir_.c_str(),
                   (char*)nullptr);
            _exit(127);
        }
        if (pid_ < 0) return;
        uri = "unix://" + sock_;
        // 就绪轮询：PING 通即可用（≤5s）
        for (int i = 0; i < 50; ++i) {
            if (redisContext* ctx = connect_raw()) {
                auto* r = static_cast<redisReply*>(redisCommand(ctx, "PING"));
                bool ok = r && r->type == REDIS_REPLY_STATUS;
                if (r) freeReplyObject(r);
                redisFree(ctx);
                if (ok) {
                    available = true;
                    return;
                }
            }
            int status = 0;
            if (waitpid(pid_, &status, WNOHANG) == pid_) {  // exec 失败/启动即死
                pid_ = -1;
                break;
            }
            usleep(100 * 1000);
        }
    }

    ~RedisTestServer() {
        if (pid_ > 0) {
            kill(pid_, SIGTERM);
            waitpid(pid_, nullptr, 0);
        }
        if (!dir_.empty()) {
            std::error_code ec;
            fs::remove_all(dir_, ec);
        }
    }

    redisContext* connect_raw() {
        timeval tv{1, 0};
        redisContext* ctx = nullptr;
        if (uri.rfind("unix://", 0) == 0)
            ctx = redisConnectUnixWithTimeout(uri.c_str() + 7, tv);
        else if (uri.rfind("redis://", 0) == 0) {
            // 覆盖实例只支持最简 host:port 形态（测试注入用）
            std::string rest = uri.substr(8);
            if (auto slash = rest.find('/'); slash != std::string::npos) rest.resize(slash);
            std::string host = rest;
            int port = 6379;
            if (auto colon = rest.rfind(':'); colon != std::string::npos) {
                host = rest.substr(0, colon);
                port = atoi(rest.c_str() + colon + 1);
            }
            ctx = redisConnectWithTimeout(host.c_str(), port, tv);
        }
        if (ctx && ctx->err) {
            redisFree(ctx);
            return nullptr;
        }
        return ctx;
    }

    std::string dir_, sock_;
    pid_t pid_ = -1;
};

// 每用例独立前缀：多套用例（以及外部覆盖实例上的多次运行）互不污染（§2.1）
std::string unique_prefix() {
    static std::atomic<int> counter{0};
    return "t" + std::to_string(getpid()) + "-" + std::to_string(counter++) + ":";
}

RedisMetaOptions redis_opts(const std::string& prefix) {
    return {RedisTestServer::instance().uri, prefix, /*timeout_ms=*/3000, /*pool_size=*/4};
}

#define REDIS_OR_SKIP()                                                       \
    if (!RedisTestServer::instance().available) {                             \
        printf("       [SKIP] redis-server not available\n");                \
        return;                                                               \
    }

using backend_suite::TmpDir;
using meta_store_suite::chunk_extent;
using meta_store_suite::make_rec;

}  // namespace

// 同一 meta 语义基线（与 RocksMetaStore 共享套件，docs/duostore-redis-meta.md §9.1）
TEST(duostore_redis_meta_store_suite) {
    REDIS_OR_SKIP();
    std::string prefix = unique_prefix();
    meta_store_suite::run_meta_store_suite(
        [&] { return std::make_unique<RedisMetaStore>(redis_opts(prefix)); });
}

// 注入组合（RedisMetaStore + FsDataStore）跑后端一致性套件（§9.3）
TEST(duostore_redis_backend_suite) {
    REDIS_OR_SKIP();
    TmpDir tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    auto meta = std::make_unique<RedisMetaStore>(redis_opts(unique_prefix()));
    IMetaStore* mp = meta.get();
    DuoStoreConfig cfg;
    cfg.name = "redis-suite";
    cfg.root = tmp.path / "duo";
    fs::create_directories(cfg.root);
    auto data = std::make_unique<FsDataStore>(
        FsDataOptions{cfg.root, cfg.chunk_size, cfg.verify_chunk_crc}, pool,
        [mp](Extent::Kind kind) { return mp->alloc_file_id(kind); });
    auto b = std::make_shared<DuoStoreBackend>(cfg, pool, std::move(meta), std::move(data));
    backend_suite::run_backend_suite(*b);
    sync_wait(b->close());
}

// 前缀隔离（§2.1）：两个 store 共用一个 server，互不可见
TEST(duostore_redis_prefix_isolation) {
    REDIS_OR_SKIP();
    RedisMetaStore a(redis_opts(unique_prefix()));
    RedisMetaStore b(redis_opts(unique_prefix()));
    a.create_bucket("iso");
    CHECK(a.bucket_exists("iso"));
    CHECK(!b.bucket_exists("iso"));
    CHECK_EQ(b.list_buckets().size(), size_t(0));
    a.delete_bucket("iso");
    a.close();
    b.close();
}

// 多网关共 meta（§3.4）：同前缀的两个实例即共享元数据；号段派发互不碰撞
TEST(duostore_redis_multi_gateway_shared_meta) {
    REDIS_OR_SKIP();
    std::string prefix = unique_prefix();
    RedisMetaStore g1(redis_opts(prefix));
    RedisMetaStore g2(redis_opts(prefix));
    g1.create_bucket("shared");
    CHECK(g2.bucket_exists("shared"));
    g2.put_object("shared", "k", make_rec("k", {}));
    CHECK(g1.get_object("shared", "k").has_value());
    CHECK_THROWS_S3(g2.create_bucket("shared"), s3::S3ErrorCode::BucketAlreadyOwnedByYou);

    // 两网关各取一批 file_id：全局唯一（INCRBY 号段，§4）
    std::set<uint64_t> ids;
    for (int i = 0; i < 5000; ++i) {
        CHECK(ids.insert(g1.alloc_file_id(Extent::Kind::kChunk)).second);
        CHECK(ids.insert(g2.alloc_file_id(Extent::Kind::kChunk)).second);
    }
    CHECK(g1.delete_object("shared", "k"));
    g1.delete_bucket("shared");
    g1.close();
    g2.close();
}

// NOSCRIPT 自愈（§3.5）：SCRIPT FLUSH 后提交与 list 照常（回退 EVAL 重载）
TEST(duostore_redis_noscript_selfheal) {
    REDIS_OR_SKIP();
    RedisMetaStore m(redis_opts(unique_prefix()));
    m.create_bucket("heal");
    m.put_object("heal", "k1", make_rec("k1", {}));
    CHECK(RedisTestServer::instance().raw_command("SCRIPT FLUSH"));
    m.put_object("heal", "k2", make_rec("k2", {}));  // 提交脚本自愈
    auto r = m.list_objects("heal", {});             // list 脚本自愈
    CHECK_EQ(r.objects.size(), size_t(2));
    for (auto k : {"k1", "k2"}) m.delete_object("heal", k);
    m.delete_bucket("heal");
    m.close();
}

// swap_extents 的 CAS 放弃路径（§3.3/§9.2）：version 或 extents 不符 → false 不落写
TEST(duostore_redis_swap_extents_cas) {
    REDIS_OR_SKIP();
    RedisMetaStore m(redis_opts(unique_prefix()));
    m.create_bucket("swap");
    uint64_t id1 = m.alloc_file_id(Extent::Kind::kChunk);
    uint64_t id2 = m.alloc_file_id(Extent::Kind::kChunk);
    DataRef from{{chunk_extent(id1, 8)}};
    DataRef to{{chunk_extent(id2, 8)}};
    m.put_object("swap", "k", make_rec("k", from.extents));  // version=1

    CHECK(!m.swap_extents("swap", "k", /*expect_version=*/2, from, to));  // version 不符
    CHECK(m.chunk_referenced(id1));
    CHECK(!m.chunk_referenced(id2));

    CHECK(m.swap_extents("swap", "k", /*expect_version=*/1, from, to));
    auto rec = m.get_object("swap", "k");
    CHECK_EQ(rec->version, uint64_t(2));
    CHECK(rec->data.extents == to.extents);
    CHECK(!m.chunk_referenced(id1));
    CHECK(m.chunk_referenced(id2));

    // 换过之后旧 from 过期 → 再换必失败
    CHECK(!m.swap_extents("swap", "k", /*expect_version=*/2, from, to));
    CHECK(m.delete_object("swap", "k"));
    m.delete_bucket("swap");
    m.close();
}

// 并发 CAS 收敛（§3.2）：两个"网关"对同一 key 竞争覆盖写——version 严格计数、
// refs 只剩最终 extent、gcq 恰好 (总写数-1) 条（每次覆盖旧 ref 入账一次）
TEST(duostore_redis_concurrent_cas_converges) {
    REDIS_OR_SKIP();
    std::string prefix = unique_prefix();
    RedisMetaStore g1(redis_opts(prefix));
    RedisMetaStore g2(redis_opts(prefix));
    g1.create_bucket("race");
    constexpr int kPerWriter = 25;

    // 线程内异常经 exception_ptr 传回主线程重抛（否则 terminate 掩盖断言信息）
    std::exception_ptr errs[2];
    auto writer = [&](RedisMetaStore& m, std::exception_ptr& err) {
        try {
            for (int i = 0; i < kPerWriter; ++i) {
                uint64_t id = m.alloc_file_id(Extent::Kind::kChunk);
                m.put_object("race", "hot", make_rec("hot", {chunk_extent(id, 1)}));
            }
        } catch (...) {
            err = std::current_exception();
        }
    };
    std::thread t1(writer, std::ref(g1), std::ref(errs[0]));
    std::thread t2(writer, std::ref(g2), std::ref(errs[1]));
    t1.join();
    t2.join();
    for (auto& e : errs)
        if (e) std::rethrow_exception(e);

    auto rec = g1.get_object("race", "hot");
    CHECK(rec.has_value());
    CHECK_EQ(rec->version, uint64_t(2 * kPerWriter));
    CHECK(g1.chunk_referenced(rec->data.extents.at(0).file_id));
    CHECK_EQ(g1.peek_reclaims(1000).size(), size_t(2 * kPerWriter - 1));

    CHECK(g1.delete_object("race", "hot"));
    g1.delete_bucket("race");
    g1.close();
    g2.close();
}

// close 后调用干净失败（500），而非崩溃（§5.5）
TEST(duostore_redis_closed_store_throws) {
    REDIS_OR_SKIP();
    RedisMetaStore m(redis_opts(unique_prefix()));
    m.close();
    CHECK_THROWS_S3(m.bucket_exists("x"), s3::S3ErrorCode::InternalError);
}

#endif  // LIGHTS3_DUOSTORE && LIGHTS3_DUOSTORE_REDIS_META
