// app/fsck_jobs.h (backlog-sequence ③): the offline scrub as an application job --
// type dispatch, one job per backend, polling document, outcome after completion,
// and the server-side handler's error mapping through fake hooks
#include <chrono>
#include <fstream>
#include <thread>

#include "app/fsck_jobs.h"
#include "core/thread_pool.h"
#include "s3/auth/credential_store.h"
#include "s3/auth/sigv4.h"
#include "s3/service.h"
#include "storage/bucket_router.h"
#include "storage/memory/memory_backend.h"
#include "storage/registry.h"
#include "unit/backend_suite.h"
#include "unit/mini_test.h"

using namespace lights3;
using namespace lights3::storage;
using nlohmann::json;

namespace {

struct TmpDirF {
    std::filesystem::path path;
    TmpDirF() {
        path = std::filesystem::temp_directory_path() /
               ("lights3-fsckjobs-" + std::to_string(::getpid()) + "-" +
                std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
        std::filesystem::create_directories(path);
    }
    ~TmpDirF() { std::filesystem::remove_all(path); }
};

json wait_done(FsckJobs& jobs, const std::string& backend) {
    // Up to 15 s: the throttled round below takes ~4 s on an idle box, more under load
    for (int i = 0; i < 1500; ++i) {
        json s = jobs.status(backend);
        if (!s["running"].get<bool>()) return s;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return jobs.status(backend);
}

}  // namespace

TEST(fsck_jobs_run_scrub_dispatches_and_reports) {
    TmpDirF tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    std::vector<BackendConfig> cfgs;
    cfgs.push_back({"fs", "localfs",
                    {{"root", (tmp.path / "d").string()}, {"staging", (tmp.path / "s").string()}}});
    cfgs.push_back({"mem", "memory", {}});
    auto out = StorageRegistry::build(cfgs, pool);
    auto& fs = *out.at("fs");
    sync_wait(fs.create_bucket("bkt"));
    backend_suite::put(fs, "bkt", "a", std::string(100000, 'a'));
    backend_suite::put(fs, "bkt", "b", std::string(50000, 'b'));

    auto o = run_scrub(fs, 0);
    CHECK_EQ(o.kind, "localfs");
    CHECK_EQ(o.stats["objects_scanned"].get<uint64_t>(), uint64_t(2));
    CHECK_EQ(o.findings, uint64_t(0));
    CHECK(!o.aborted);
    bool threw = false;
    try {
        run_scrub(*out.at("mem"), 0);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    CHECK(threw);

    // Corrupt one object's bytes on disk: the next round reports it as a finding
    for (auto& e : std::filesystem::recursive_directory_iterator(tmp.path / "d")) {
        if (!e.is_regular_file() || e.file_size() != 100000) continue;
        std::fstream f(e.path(), std::ios::in | std::ios::out | std::ios::binary);
        f.seekp(10);
        f.put('Z');
    }
    auto o2 = run_scrub(fs, 0);
    CHECK_EQ(o2.stats["etag_mismatches"].get<uint64_t>(), uint64_t(1));
    CHECK_EQ(o2.findings, uint64_t(1));
    for (auto& [n, b] : out) sync_wait(b->close());
}

TEST(fsck_jobs_one_job_per_backend_with_polling) {
    TmpDirF tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    std::vector<BackendConfig> cfgs;
    cfgs.push_back({"fs", "localfs",
                    {{"root", (tmp.path / "d").string()}, {"staging", (tmp.path / "s").string()}}});
    cfgs.push_back({"mem", "memory", {}});
    auto out = StorageRegistry::build(cfgs, pool);
    auto& fs = *out.at("fs");
    sync_wait(fs.create_bucket("bkt"));
    for (int i = 0; i < 4; ++i)
        backend_suite::put(fs, "bkt", "k" + std::to_string(i), std::string(1 << 20, char('a' + i)));

    FsckJobs jobs(out);
    auto fails = [&](auto fn, FsckJobs::Error code) {
        try {
            fn();
        } catch (const FsckJobs::Failure& f) {
            return f.code == code;
        }
        return false;
    };
    CHECK(fails([&] { jobs.start("nope", 0); }, FsckJobs::Error::NoSuchBackend));
    CHECK(fails([&] { jobs.status("nope"); }, FsckJobs::Error::NoSuchBackend));
    CHECK(fails([&] { jobs.start("mem", 0); }, FsckJobs::Error::Unsupported));
    json before = jobs.status("fs");
    CHECK(!before["running"].get<bool>());
    CHECK(before["job_id"].is_null());

    // 1 MB/s over 4 MiB keeps the round busy for seconds: the second start is refused
    uint64_t id = jobs.start("fs", 1000 * 1000);
    CHECK_EQ(id, uint64_t(1));
    CHECK(fails([&] { jobs.start("fs", 0); }, FsckJobs::Error::Busy));
    json running = jobs.status("fs");
    CHECK(running["running"].get<bool>());
    CHECK_EQ(running["job_id"].get<uint64_t>(), uint64_t(1));
    CHECK_EQ(running["max_mbps"].get<uint64_t>(), uint64_t(1));
    CHECK(!running.contains("stats"));

    json done = wait_done(jobs, "fs");
    CHECK(!done["running"].get<bool>());
    CHECK_EQ(done["kind"].get<std::string>(), "localfs");
    CHECK_EQ(done["findings"].get<uint64_t>(), uint64_t(0));
    CHECK_EQ(done["stats"]["objects_scanned"].get<uint64_t>(), uint64_t(4));
    CHECK(done["duration_ms"].get<int64_t>() >= 1000);  // throttled: ~4 s
    CHECK(!done["aborted"].get<bool>());

    // A second job gets the next id and can start once the first is done
    CHECK_EQ(jobs.start("fs", 0), uint64_t(2));
    json done2 = wait_done(jobs, "fs");
    CHECK_EQ(done2["job_id"].get<uint64_t>(), uint64_t(2));
    jobs.shutdown();
    for (auto& [n, b] : out) sync_wait(b->close());
}

// The handler: root gate, path shape, method mapping, hook error mapping (the
// hooks are fakes -- the real ones are the application's FsckJobs wrappers)
TEST(service_admin_fsck_endpoint) {
    using namespace lights3::s3;
    AuthConfig acfg;
    acfg.credentials = {{"FSCKROOT", "root-sk"}};
    auto backend = std::make_shared<MemoryBackend>();
    auto store = sync_wait(CredentialStore::load(backend, acfg));
    std::map<std::string, std::shared_ptr<IStorageBackend>> backends{{"mem", backend}};
    BucketsConfig bcfg;
    bcfg.default_backend = "mem";
    auto auth = SigV4Authenticator::build(acfg);
    auth.set_provider(store);
    S3Service svc(BucketRouter::build(bcfg, backends), std::move(auth));
    svc.set_credential_store(store);
    bool running = false;
    std::string last_backend;
    uint64_t last_bps = 0;
    svc.set_fsck_hooks(
        [&](const std::string& b, uint64_t bps) {
            if (b != "fs") throw S3Error(S3ErrorCode::NoSuchKey, "no backend named '" + b + "'");
            if (running) throw S3Error(S3ErrorCode::ScrubInProgress, "busy");
            running = true;
            last_backend = b;
            last_bps = bps;
            return json{{"backend", b}, {"job_id", 7}, {"running", true}};
        },
        [&](const std::string& b) {
            if (b != "fs") throw S3Error(S3ErrorCode::NoSuchKey, "no backend named '" + b + "'");
            return json{{"backend", b}, {"running", running}, {"job_id", 7}};
        });
    SigV4Authenticator signer = SigV4Authenticator::build(acfg);
    Credential root{"FSCKROOT", util::SecretString(std::string("root-sk"))};
    auto call = [&](const std::string& method, const std::string& path, const std::string& query,
                    const Credential* cred) {
        http::HttpRequest req;
        req.method = method;
        req.raw_path = path;
        req.path = path;
        req.raw_query = query;
        if (!query.empty()) {
            auto eq = query.find('=');
            req.query.emplace_back(query.substr(0, eq), query.substr(eq + 1));
        }
        req.headers.add("Host", "localhost");
        req.headers.add("Content-Length", "0");
        if (cred) signer.sign(req, *cred, util::sha256_hex(""));
        return sync_wait(svc.dispatch(std::move(req)));
    };
    CHECK_EQ(call("POST", "/-/admin/fsck/fs", "", nullptr).status, 403);
    auto plain = sync_wait(store->generate("plain"));
    Credential p{plain.access_key, plain.secret_key};
    CHECK_EQ(call("POST", "/-/admin/fsck/fs", "", &p).status, 403);
    CHECK_EQ(call("POST", "/-/admin/fsck/", "", &root).status, 400);
    CHECK_EQ(call("POST", "/-/admin/fsck/fs/extra", "", &root).status, 400);
    CHECK_EQ(call("DELETE", "/-/admin/fsck/fs", "", &root).status, 405);
    CHECK_EQ(call("POST", "/-/admin/fsck/nope", "", &root).status, 404);
    CHECK_EQ(call("POST", "/-/admin/fsck/fs", "max_mbps=abc", &root).status, 400);
    auto started = call("POST", "/-/admin/fsck/fs", "max_mbps=3", &root);
    CHECK_EQ(started.status, 202);
    CHECK_EQ(json::parse(started.small_body)["job_id"].get<int>(), 7);
    CHECK_EQ(last_backend, "fs");
    CHECK_EQ(last_bps, uint64_t(3000000));
    auto busy = call("POST", "/-/admin/fsck/fs", "", &root);
    CHECK_EQ(busy.status, 409);
    CHECK_EQ(json::parse(busy.small_body)["code"].get<std::string>(), "ScrubInProgress");
    auto st = call("GET", "/-/admin/fsck/fs", "", &root);
    CHECK_EQ(st.status, 200);
    CHECK(json::parse(st.small_body)["running"].get<bool>());
    CHECK_EQ(call("GET", "/-/admin/fsck/nope", "", &root).status, 404);
}
