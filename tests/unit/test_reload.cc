// roadmap §4.4: configuration hot reload (docs/config-reload.md) — the runtime
// primitives (semaphore capacity, router table swap), the Application-level
// reload with its applied / requires-restart report, and the admin endpoint
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <nlohmann/json.hpp>

#include "app/admin_jobs.h"
#include "app/app.h"
#include "core/semaphore.h"
#include "core/util/crypto.h"
#include "s3/auth/credential_store.h"
#include "s3/service.h"
#include "storage/bucket_router.h"
#include "storage/memory/memory_backend.h"
#include "storage/metered_backend.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::s3;
using nlohmann::json;

namespace {

std::string temp_path(const char* stem) { return "/tmp/lights3-reload-" + std::to_string(::getpid()) + "-" + stem; }
void write_file(const std::string& p, const std::string& text) {
    std::ofstream f(p);
    f << text;
}
bool contains(const std::string& s, const std::string& sub) { return s.find(sub) != std::string::npos; }
bool has(const std::vector<std::string>& v, const std::string& sub) {
    for (auto& s : v)
        if (contains(s, sub)) return true;
    return false;
}

// Free coroutine functions rather than capturing lambdas: the lambda object would
// be destroyed at the end of the statement while the frame still refers to it
Task<AsyncSemaphore::Permit> acquire_task(AsyncSemaphore& s) { co_return co_await s.acquire(); }
Task<void> acquire_and_flag(AsyncSemaphore& s, bool& got) {
    auto p = co_await s.acquire();
    got = true;
}

std::string base_config(const std::string& extra, const std::string& rules = "  rules: []\n") {
    return "http:\n  driver: builtin\n  bind: 127.0.0.1\n  port: 0\n" + extra +
           "backends:\n  - name: a\n    type: memory\n  - name: b\n    type: memory\n"
           "buckets:\n  default_backend: a\n" +
           rules + "log:\n  level: info\n";
}

// Request through the assembled service (auth is off in these configs, so no
// signature); returns the HTTP status
int call(Application& app, std::string method, std::string path, std::string body = "") {
    http::HttpRequest req;
    req.method = std::move(method);
    req.raw_path = path;
    req.path = std::move(path);
    req.headers.add("Host", "localhost");
    req.headers.add("Content-Length", std::to_string(body.size()));
    if (!body.empty()) req.body = std::make_unique<http::StringBodyReader>(std::move(body));
    return sync_wait(app.service()->dispatch(std::move(req))).status;
}
// Free coroutine (a lambda's frame would not outlive the sync_wait)
Task<storage::ObjectStream> open_object(storage::IStorageBackend& b, std::string bucket, std::string key) {
    co_return co_await b.get_object(bucket, key, std::nullopt);
}
Task<void> put_small(storage::IStorageBackend& b, std::string bucket, std::string key, std::string data) {
    http::StringBodyReader body(std::move(data));
    co_await b.put_object(bucket, key, {}, body, {});
}

}  // namespace

TEST(reload_semaphore_capacity_grows_and_shrinks) {
    AsyncSemaphore sem(1);
    CHECK_EQ(sem.capacity(), 1L);
    auto p1 = sync_wait(acquire_task(sem));
    CHECK_EQ(sem.available(), 0L);
    // A second acquirer queues; growing the capacity wakes it
    bool got = false;
    std::thread th([&] { sync_wait(acquire_and_flag(sem, got)); });
    for (int i = 0; i < 100 && sem.waiting() == 0; ++i) std::this_thread::sleep_for(std::chrono::milliseconds(10));
    CHECK_EQ(sem.waiting(), size_t(1));
    sem.set_capacity(2);
    th.join();
    CHECK(got);
    CHECK_EQ(sem.capacity(), 2L);
    // Shrinking below the in-flight count: available goes negative, recovers as permits return
    sem.set_capacity(0);
    CHECK_EQ(sem.available(), -1L);
    p1.release();
    CHECK_EQ(sem.available(), 0L);
    CHECK(!sem.try_acquire().has_value());
    sem.set_capacity(1);
    CHECK(sem.try_acquire().has_value());
}

TEST(shutdown_drain_waits_on_the_semaphore) {
    // roadmap §4.5: the drain deadline waits on a condition variable that fires when
    // the last permit returns, instead of polling
    AsyncSemaphore sem(2);
    // nothing out: immediately
    CHECK(sem.wait_drained(std::chrono::milliseconds(10)));
    auto p = sync_wait(acquire_task(sem));
    auto t0 = std::chrono::steady_clock::now();
    CHECK(!sem.wait_drained(std::chrono::milliseconds(100)));
    CHECK(std::chrono::steady_clock::now() - t0 >= std::chrono::milliseconds(90));
    std::thread releaser([&] {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        p.release();
    });
    CHECK(sem.wait_drained(std::chrono::seconds(5)));
    releaser.join();
}

TEST(reload_bucket_router_table_swap) {
    auto a = std::make_shared<storage::MemoryBackend>();
    auto b = std::make_shared<storage::MemoryBackend>();
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends{{"a", a}, {"b", b}};
    BucketsConfig cfg;
    cfg.default_backend = "a";
    auto router = storage::BucketRouter::build(cfg, backends);
    // shares the table
    storage::BucketRouter copy = router;
    CHECK_EQ(&router.resolve("logs-1"), static_cast<storage::IStorageBackend*>(a.get()));
    BucketsConfig fresh = cfg;
    fresh.rules.push_back({"logs-*", "b"});
    router.update(fresh);
    CHECK_EQ(&router.resolve("logs-1"), static_cast<storage::IStorageBackend*>(b.get()));
    // the copy sees it
    CHECK_EQ(&copy.resolve("logs-1"), static_cast<storage::IStorageBackend*>(b.get()));
    CHECK_EQ(&copy.resolve("other"), static_cast<storage::IStorageBackend*>(a.get()));
    CHECK_EQ(router.rule_count(), size_t(1));
    // Refused updates leave the table untouched
    BucketsConfig bad = fresh;
    bad.rules.push_back({"x-*", "nope"});
    bool threw = false;
    try {
        router.update(bad);
    } catch (const std::runtime_error& e) {
        threw = contains(e.what(), "unknown backend");
    }
    CHECK(threw);
    BucketsConfig other_default = fresh;
    other_default.default_backend = "b";
    threw = false;
    try {
        router.update(other_default);
    } catch (const std::runtime_error& e) {
        threw = contains(e.what(), "default_backend");
    }
    CHECK(threw);
    CHECK_EQ(router.rule_count(), size_t(1));
    CHECK_EQ(&copy.resolve("logs-9"), static_cast<storage::IStorageBackend*>(b.get()));
}

TEST(reload_application_applies_subset_and_reports_rest) {
    std::string path = temp_path("app.yaml");
    write_file(path, base_config("  request_timeout: 300s\n  transfer_stall_timeout: 300s\n"
                                 "  min_part_size: 5MiB\nruntime:\n  max_inflight_requests: 100\n"));
    Application app(path);
    app.open_storage();
    app.start_server();
    // No change: clean report
    auto r0 = app.reload_config();
    CHECK(r0.ok && r0.applied.empty() && r0.requires_restart.empty());
    // Reloadable subset + a startup-only key + new routing rules
    write_file(path, base_config("  request_timeout: 120s\n  transfer_stall_timeout: 60s\n"
                                 "  min_part_size: 0\n  max_connections: 99\n"
                                 "  metrics_access: root\nruntime:\n"
                                 "  max_inflight_requests: 200\nratelimit:\n  per_ip_rps: 50\n",
                                 "  rules:\n    - match: \"logs-*\"\n      backend: b\n"));
    // log level lives at the end of base_config; rewrite it to debug
    {
        std::ifstream in(path);
        std::string text((std::istreambuf_iterator<char>(in)), {});
        text.replace(text.find("level: info"), 11, "level: debug\n  slow_request_threshold: 250ms\n  format: json");
        write_file(path, text);
    }
    auto r1 = app.reload_config();
    CHECK(r1.ok);
    CHECK(has(r1.applied, "log.level: info -> debug"));
    CHECK(has(r1.applied, "log.slow_request_threshold(ms): 0 -> 250"));
    CHECK(has(r1.applied, "http.metrics_access: anonymous -> root"));
    CHECK(has(r1.requires_restart, "log.format/file/max_size/max_files/async*"));
    CHECK(has(r1.applied, "http.request_timeout: 300 -> 120"));
    CHECK(has(r1.applied, "http.transfer_stall_timeout: 300 -> 60"));
    CHECK(has(r1.applied, "http.min_part_size"));
    CHECK(has(r1.applied, "runtime.max_inflight_requests: 100 -> 200"));
    CHECK(has(r1.applied, "ratelimit: per-ip rps=50"));
    CHECK(has(r1.applied, "buckets.rules: 0 -> 1"));
    CHECK(has(r1.requires_restart, "http.max_connections"));
    CHECK_EQ(r1.requires_restart.size(), size_t(2));
    CHECK_EQ(app.config().log.slow_request_threshold_ms, 250);
    // the running value stays
    CHECK_EQ(app.config().log.format, "text");
    CHECK_EQ(app.config().http.request_timeout_sec, 120);
    CHECK_EQ(app.config().runtime.max_inflight_requests, 200);
    CHECK_EQ(app.config().buckets.rules.size(), size_t(1));
    // the running value stays
    CHECK_EQ(app.config().http.max_connections, 4096);
    // keep the test log readable
    Logger::set_level(LogLevel::Info);
    // A broken file is refused as a whole and the running config is untouched
    write_file(path, base_config("  request_timeout: 5s\n  idle_timeout: 0s\n"));
    auto r2 = app.reload_config();
    CHECK(!r2.ok);
    CHECK(contains(r2.error, "idle_timeout"));
    CHECK_EQ(app.config().http.request_timeout_sec, 120);
    // A rule naming an unknown backend is refused before anything is applied
    write_file(path, base_config("  request_timeout: 5s\n  transfer_stall_timeout: 0s\n",
                                 "  rules:\n    - match: \"x-*\"\n      backend: ghost\n"));
    auto r3 = app.reload_config();
    if (r3.ok || !contains(r3.error, "ghost"))
        throw mini_test::Failure("r3: ok=" + std::to_string(r3.ok) + " error='" + r3.error +
                                 "' applied=" + std::to_string(r3.applied.size()));
    CHECK_EQ(app.config().http.request_timeout_sec, 120);
    app.shutdown();
    // roadmap §4.5: a clean teardown reports clean
    CHECK(app.shutdown_clean());
    std::filesystem::remove(path);
}

// Separate admin listener (http.admin_port, backlog-sequence ②): the application
// binds a second listener, and moving it is a restart-only change
TEST(application_admin_listener_bound_and_restart_only) {
    std::string path = temp_path("admin.yaml");
    write_file(path, base_config("  admin_port: 0\n"));
    Application app(path);
    app.open_storage();
    app.start_server();
    CHECK(app.bound_port() != 0);
    CHECK(app.admin_bound_port() != 0);
    CHECK(app.bound_port() != app.admin_bound_port());
    auto r0 = app.reload_config();
    CHECK(r0.ok && r0.requires_restart.empty());
    write_file(path, base_config("  admin_port: 0\n  admin_bind: 127.0.0.2\n"));
    auto r1 = app.reload_config();
    CHECK(r1.ok);
    CHECK(has(r1.requires_restart, "http.admin_bind/admin_port"));
    CHECK_EQ(r1.requires_restart.size(), size_t(1));
    // dropping the listener is restart-only too
    write_file(path, base_config(""));
    auto r2 = app.reload_config();
    CHECK(has(r2.requires_restart, "http.admin_bind/admin_port"));
    // still bound: nothing was applied
    CHECK(app.admin_bound_port() != 0);
    app.shutdown();
    CHECK(app.shutdown_clean());

    // Without admin_port there is no second listener
    write_file(path, base_config(""));
    Application single(path);
    single.open_storage();
    single.start_server();
    CHECK_EQ(single.admin_bound_port(), uint16_t(0));
    single.shutdown();
}

TEST(reload_admin_endpoint_root_only) {
    AuthConfig acfg;
    acfg.credentials = {{"RLDROOT", "root-sk"}};
    auto backend = std::make_shared<storage::MemoryBackend>();
    auto store = sync_wait(CredentialStore::load(backend, acfg));
    std::map<std::string, std::shared_ptr<storage::IStorageBackend>> backends{{"mem", backend}};
    BucketsConfig bcfg;
    bcfg.default_backend = "mem";
    auto auth = SigV4Authenticator::build(acfg);
    auth.set_provider(store);
    S3Service svc(storage::BucketRouter::build(bcfg, backends), std::move(auth));
    svc.set_credential_store(store);
    int calls = 0;
    svc.set_reload_hook([&] {
        ConfigReloadReport r;
        r.ok = ++calls == 1;
        if (r.ok) {
            r.applied = {"log.level: info -> debug"};
            r.requires_restart = {"http.port"};
        } else {
            r.error = "config: bad";
        }
        return r;
    });
    SigV4Authenticator signer = SigV4Authenticator::build(acfg);
    Credential root{"RLDROOT", util::SecretString(std::string("root-sk"))};
    auto call = [&](const std::string& method, const Credential* cred) {
        http::HttpRequest req;
        req.method = method;
        req.raw_path = "/-/admin/config/reload";
        req.path = req.raw_path;
        req.headers.add("Host", "localhost");
        req.headers.add("Content-Length", "0");
        if (cred) signer.sign(req, *cred, util::sha256_hex(""));
        return sync_wait(svc.dispatch(std::move(req)));
    };
    CHECK_EQ(call("POST", nullptr).status, 403);
    auto plain = sync_wait(store->generate("plain"));
    Credential p{plain.access_key, plain.secret_key};
    CHECK_EQ(call("POST", &p).status, 403);
    CHECK_EQ(call("GET", &root).status, 405);
    auto ok = call("POST", &root);
    CHECK_EQ(ok.status, 200);
    auto j = json::parse(ok.small_body);
    CHECK(j["ok"].get<bool>());
    CHECK_EQ(j["applied"].size(), size_t(1));
    CHECK_EQ(j["requires_restart"][0].get<std::string>(), std::string("http.port"));
    auto bad = call("POST", &root);
    CHECK_EQ(bad.status, 400);
    CHECK_EQ(json::parse(bad.small_body)["error"].get<std::string>(), std::string("config: bad"));
    CHECK_EQ(calls, 2);
}

// ---------- backlog-sequence ⑦: backend instances added / removed at runtime ----------

// The router swaps rules and backend set in one snapshot; iteration snapshots stay
// stable; the default backend must survive (same instance)
TEST(reload_bucket_router_backend_set_swap) {
    auto a = std::make_shared<storage::MemoryBackend>();
    auto b = std::make_shared<storage::MemoryBackend>();
    auto c = std::make_shared<storage::MemoryBackend>();
    BucketsConfig cfg;
    cfg.default_backend = "a";
    auto router = storage::BucketRouter::build(cfg, {{"a", a}, {"b", b}});
    storage::BucketRouter copy = router;
    // snapshot taken before the swap
    auto before = router.backends();
    BucketsConfig fresh = cfg;
    fresh.rules.push_back({"c-*", "c"});
    // A rule naming c without c in the set is refused
    bool threw = false;
    try {
        router.update(fresh);
    } catch (const std::runtime_error& e) {
        threw = contains(e.what(), "unknown backend");
    }
    CHECK(threw);
    // b dropped, c added, rule routes to it
    router.update(fresh, {{"a", a}, {"c", c}});
    CHECK_EQ(&copy.resolve("c-1"), static_cast<storage::IStorageBackend*>(c.get()));
    CHECK_EQ(&copy.resolve("other"), static_cast<storage::IStorageBackend*>(a.get()));
    CHECK_EQ(router.backends()->size(), size_t(2));
    CHECK(router.backends()->count("c") == 1 && router.backends()->count("b") == 0);
    // the old snapshot is untouched
    CHECK_EQ(before->size(), size_t(2));
    CHECK(before->count("b") == 1);
    // Dropping the default backend, or swapping it for another instance, is refused
    for (auto bad :
         {storage::BucketRouter::BackendMap{{"c", c}}, storage::BucketRouter::BackendMap{{"a", c}, {"c", c}}}) {
        threw = false;
        try {
            router.update(cfg, bad);
        } catch (const std::runtime_error& e) {
            threw = contains(e.what(), "default backend");
        }
        CHECK(threw);
    }
    // still the previous set
    CHECK_EQ(router.backends()->size(), size_t(2));
}

// The metered decorator counts calls in progress and open get_object streams;
// wait_idle wakes when the last lease returns
TEST(metered_backend_inflight_leases) {
    auto inner = std::make_shared<storage::MemoryBackend>();
    auto m = std::make_shared<storage::MeteredBackend>("m", inner, nullptr);
    sync_wait(m->create_bucket("bkt"));
    sync_wait(put_small(*m, "bkt", "k", "hello"));
    // every call released its lease
    CHECK_EQ(m->inflight(), 0L);
    CHECK(m->wait_idle(std::chrono::milliseconds(1)));
    {
        auto stream = sync_wait(open_object(*m, "bkt", "k"));
        // the open stream holds the lease
        CHECK_EQ(m->inflight(), 1L);
        auto t0 = std::chrono::steady_clock::now();
        CHECK(!m->wait_idle(std::chrono::milliseconds(60)));
        CHECK(std::chrono::steady_clock::now() - t0 >= std::chrono::milliseconds(50));
        // The stream still reads through the lease wrapper
        std::byte buf[16];
        CHECK_EQ(sync_wait(stream.body->read(std::span(buf))), size_t(5));
        std::thread releaser([&] {
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
            stream.body.reset();
        });
        CHECK(m->wait_idle(std::chrono::seconds(5)));
        releaser.join();
    }
    CHECK_EQ(m->inflight(), 0L);
}

TEST(admin_jobs_dynamic_backend_set) {
    auto a = std::make_shared<storage::MemoryBackend>();
    AdminJobs jobs({{"a", a}});
    CHECK(!jobs.busy("a"));
    bool threw = false;
    try {
        jobs.status("c", JobOp::Fsck);
    } catch (const AdminJobs::Failure& f) {
        threw = f.code == AdminJobs::Error::NoSuchBackend;
    }
    CHECK(threw);
    jobs.add_backend("c", std::make_shared<storage::MemoryBackend>());
    CHECK(!jobs.status("c", JobOp::Fsck)["running"].get<bool>());
    CHECK(jobs.remove_backend("c"));
    // idempotent
    CHECK(jobs.remove_backend("c"));
    threw = false;
    try {
        jobs.status("c", JobOp::Fsck);
    } catch (const AdminJobs::Failure&) {
        threw = true;
    }
    CHECK(threw);
}

// Application-level: add a backend and route to it, refuse removals the file still
// references, remove it once unreferenced (closed after draining), report the rest
TEST(reload_application_adds_and_removes_backends) {
    std::string path = temp_path("backends.yaml");
    write_file(path, base_config(""));
    Application app(path);
    app.open_storage();
    app.start_server();
    CHECK_EQ(app.backends().size(), size_t(2));

    // Add c and route c-* to it
    std::string with_c =
        "http:\n  driver: builtin\n  bind: 127.0.0.1\n  port: 0\n"
        "backends:\n  - name: a\n    type: memory\n  - name: b\n    type: memory\n"
        "  - name: c\n    type: memory\n    max_bytes: 1MiB\n"
        "buckets:\n  default_backend: a\n  rules:\n    - match: \"c-*\"\n      backend: c\n"
        "log:\n  level: info\n";
    write_file(path, with_c);
    auto r1 = app.reload_config();
    CHECK(r1.ok);
    CHECK(has(r1.applied, "backends: added c (memory)"));
    CHECK(has(r1.applied, "buckets.rules: 0 -> 1"));
    CHECK(r1.requires_restart.empty());
    CHECK_EQ(app.backends().size(), size_t(3));
    CHECK_EQ(app.config().backends.size(), size_t(3));
    CHECK_EQ(call(app, "PUT", "/c-data"), 200);
    CHECK_EQ(call(app, "PUT", "/c-data/k", "on-c"), 200);
    // The bucket lives on c, nowhere else
    auto* c = app.backends().at("c").get();
    CHECK(sync_wait(c->bucket_exists("c-data")));
    CHECK(!sync_wait(app.backends().at("a")->bucket_exists("c-data")));

    // Removing c while the rule still names it: the file fails validation as a whole
    write_file(path, base_config("", "  rules:\n    - match: \"c-*\"\n      backend: c\n"));
    auto r2 = app.reload_config();
    CHECK(!r2.ok);
    CHECK(contains(r2.error, "unknown backend c"));
    CHECK_EQ(app.backends().size(), size_t(3));

    // Changing c's parameters: reported, the running instance keeps its startup config
    write_file(path, [&] {
        std::string t = with_c;
        t.replace(t.find("max_bytes: 1MiB"), 15, "max_bytes: 2MiB");
        return t;
    }());
    auto r3 = app.reload_config();
    CHECK(r3.ok);
    CHECK(has(r3.requires_restart, "backends (c: type/parameters changed"));
    CHECK_EQ(r3.requires_restart.size(), size_t(1));

    // Remove c (and its rule) while a stream from it is open: the removal applies at
    // once, the close waits for the stream
    auto stream = sync_wait(open_object(*app.backends().at("c"), "c-data", "k"));
    write_file(path, base_config(""));
    auto r4 = app.reload_config();
    CHECK(r4.ok);
    CHECK(has(r4.applied, "backends: removed c (closing after in-flight requests drain)"));
    CHECK(has(r4.applied, "buckets.rules: 1 -> 0"));
    CHECK_EQ(app.backends().size(), size_t(2));
    CHECK_EQ(app.config().backends.size(), size_t(2));
    // c-* now lands on the default backend, and the old bucket is gone with c
    CHECK_EQ(call(app, "HEAD", "/c-data"), 404);
    CHECK_EQ(call(app, "PUT", "/c-new"), 200);
    CHECK(sync_wait(app.backends().at("a")->bucket_exists("c-new")));
    // The stream taken from c keeps reading (the instance closes only after it is dropped)
    std::byte buf[16];
    CHECK_EQ(sync_wait(stream.body->read(std::span(buf))), size_t(4));
    stream.body.reset();
    app.join_retiring();
    // No change now: clean report
    auto r5 = app.reload_config();
    CHECK(r5.ok && r5.applied.empty() && r5.requires_restart.empty());

    // Removing the default backend is deferred (with the changed default reported),
    // everything else in the same file still applies
    write_file(path,
               "http:\n  driver: builtin\n  bind: 127.0.0.1\n  port: 0\n"
               "backends:\n  - name: b\n    type: memory\n"
               "buckets:\n  default_backend: b\nlog:\n  level: warn\n");
    auto r6 = app.reload_config();
    CHECK(r6.ok);
    CHECK(has(r6.applied, "log.level: info -> warn"));
    CHECK(has(r6.requires_restart, "buckets.default_backend"));
    CHECK(has(r6.requires_restart, "backends (a: the default backend cannot be removed"));
    CHECK_EQ(app.backends().size(), size_t(2));
    Logger::set_level(LogLevel::Info);

    // A tiered entry referencing a removed backend, and a backend that fails to
    // construct, are refused as a whole
    write_file(path,
               "http:\n  driver: builtin\n  bind: 127.0.0.1\n  port: 0\n"
               "backends:\n  - name: a\n    type: memory\n"
               "  - name: t\n    type: tiered\n    local: b\n    cloud: a\n"
               "buckets:\n  default_backend: a\nlog:\n  level: info\n");
    auto r7 = app.reload_config();
    CHECK(!r7.ok);
    CHECK(contains(r7.error, "cannot remove 'b'") && contains(r7.error, "tiered backend 't'"));
    // A backend that does not construct (localfs without root)
    write_file(path,
               "http:\n  driver: builtin\n  bind: 127.0.0.1\n  port: 0\n"
               "backends:\n  - name: a\n    type: memory\n  - name: b\n    type: memory\n"
               "  - name: bad\n    type: localfs\n"
               "buckets:\n  default_backend: a\nlog:\n  level: info\n");
    auto r8 = app.reload_config();
    CHECK(!r8.ok);
    CHECK(contains(r8.error, "backends:") && contains(r8.error, "bad"));
    CHECK_EQ(app.backends().size(), size_t(2));
    app.shutdown();
    CHECK(app.shutdown_clean());
    std::filesystem::remove(path);
}
