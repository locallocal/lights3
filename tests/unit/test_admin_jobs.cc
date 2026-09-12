// app/admin_jobs.h: the maintenance jobs of a live gateway -- the offline scrub
// (backlog-sequence ③) plus the duostore / tier background rounds and the
// quarantine ledgers (docs/cli.md §3.12): type dispatch, one job per backend
// whatever the op, polling document, outcome after completion, and the
// server-side handlers' path / method / error mapping through fake hooks
#include <chrono>
#include <fstream>
#include <thread>

#include "app/admin_jobs.h"
#include "core/thread_pool.h"
#include "s3/auth/credential_store.h"
#include "s3/auth/sigv4.h"
#include "s3/service.h"
#include "storage/bucket_router.h"
#include "storage/memory/memory_backend.h"
#include "storage/registry.h"
#include "storage/tiered/tiered_backend.h"
#ifdef LIGHTS3_DUOSTORE
#include "storage/duostore/duostore_backend.h"
#endif
#ifdef LIGHTS3_TABLES
#include "tables/catalog.h"
#include "tables/fsck.h"
#include "tables/object_catalog_store.h"
#endif
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

json wait_done(AdminJobs& jobs, const std::string& backend, JobOp op = JobOp::Fsck) {
    // Up to 15 s: the throttled round below takes ~4 s on an idle box, more under load
    for (int i = 0; i < 1500; ++i) {
        json s = jobs.status(backend, op);
        if (!s["running"].get<bool>()) return s;
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
    return jobs.status(backend, op);
}

// Start op on backend and wait for its outcome document
json run_and_wait(AdminJobs& jobs, const std::string& backend, JobOp op) {
    jobs.start(backend, op);
    return wait_done(jobs, backend, op);
}

template <class Fn>
bool fails_with(Fn fn, AdminJobs::Error code) {
    try {
        fn();
    } catch (const AdminJobs::Failure& f) {
        return f.code == code;
    }
    return false;
}

}  // namespace

TEST(admin_jobs_run_scrub_dispatches_and_reports) {
    TmpDirF tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    std::vector<BackendConfig> cfgs;
    cfgs.push_back({"fs", "localfs", {{"root", (tmp.path / "d").string()}, {"staging", (tmp.path / "s").string()}}});
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

TEST(admin_jobs_one_job_per_backend_with_polling) {
    TmpDirF tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    std::vector<BackendConfig> cfgs;
    cfgs.push_back({"fs", "localfs", {{"root", (tmp.path / "d").string()}, {"staging", (tmp.path / "s").string()}}});
    cfgs.push_back({"mem", "memory", {}});
    auto out = StorageRegistry::build(cfgs, pool);
    auto& fs = *out.at("fs");
    sync_wait(fs.create_bucket("bkt"));
    for (int i = 0; i < 4; ++i)
        backend_suite::put(fs, "bkt", "k" + std::to_string(i), std::string(1 << 20, char('a' + i)));

    AdminJobs jobs(out);
    auto fails = [&](auto fn, AdminJobs::Error code) { return fails_with(fn, code); };
    CHECK(fails([&] { jobs.start("nope", JobOp::Fsck); }, AdminJobs::Error::NoSuchBackend));
    CHECK(fails([&] { jobs.status("nope", JobOp::Fsck); }, AdminJobs::Error::NoSuchBackend));
    CHECK(fails([&] { jobs.start("mem", JobOp::Fsck); }, AdminJobs::Error::Unsupported));
    // The rounds of the other groups need their backend type; the refusal comes
    // before any slot is taken, so the backend is not "busy" afterwards
    CHECK(fails([&] { jobs.start("fs", JobOp::DuoGc); }, AdminJobs::Error::Unsupported));
    CHECK(fails([&] { jobs.start("fs", JobOp::TierScan); }, AdminJobs::Error::Unsupported));
    CHECK(fails([&] { jobs.start("mem", JobOp::TierReconcile); }, AdminJobs::Error::Unsupported));
    CHECK(fails([&] { jobs.quarantine("fs", "duostore"); }, AdminJobs::Error::Unsupported));
    CHECK(fails([&] { jobs.quarantine("fs", "tier"); }, AdminJobs::Error::Unsupported));
    CHECK(fails([&] { jobs.quarantine("nope", "tier"); }, AdminJobs::Error::NoSuchBackend));
    CHECK(!jobs.busy("fs"));
    json before = jobs.status("fs", JobOp::Fsck);
    CHECK(!before["running"].get<bool>());
    CHECK(!before["busy"].get<bool>());
    CHECK(before["job_id"].is_null());
    CHECK_EQ(before["op"].get<std::string>(), "fsck");

    // 1 MB/s over 4 MiB keeps the round busy for seconds: the second start is refused
    uint64_t id = jobs.start("fs", JobOp::Fsck, 1000 * 1000);
    CHECK_EQ(id, uint64_t(1));
    CHECK(fails([&] { jobs.start("fs", JobOp::Fsck); }, AdminJobs::Error::Busy));
    CHECK(jobs.busy("fs"));
    json running = jobs.status("fs", JobOp::Fsck);
    CHECK(running["running"].get<bool>());
    CHECK(running["busy"].get<bool>());
    CHECK_EQ(running["job_id"].get<uint64_t>(), uint64_t(1));
    CHECK_EQ(running["max_mbps"].get<uint64_t>(), uint64_t(1));
    CHECK(!running.contains("stats"));

    json done = wait_done(jobs, "fs");
    CHECK(!done["running"].get<bool>());
    CHECK_EQ(done["kind"].get<std::string>(), "localfs");
    CHECK_EQ(done["findings"].get<uint64_t>(), uint64_t(0));
    CHECK_EQ(done["stats"]["objects_scanned"].get<uint64_t>(), uint64_t(4));
    // throttled: ~4 s
    CHECK(done["duration_ms"].get<int64_t>() >= 1000);
    CHECK(!done["aborted"].get<bool>());

    // A second job gets the next id and can start once the first is done
    CHECK_EQ(jobs.start("fs", JobOp::Fsck), uint64_t(2));
    json done2 = wait_done(jobs, "fs");
    CHECK_EQ(done2["job_id"].get<uint64_t>(), uint64_t(2));
    jobs.shutdown();
    for (auto& [n, b] : out) sync_wait(b->close());
}

TEST(admin_jobs_op_names_and_parse) {
    CHECK_EQ(std::string(job_op_name(JobOp::Fsck)), "fsck");
    CHECK_EQ(std::string(job_group_name(JobOp::Fsck)), "fsck");
    CHECK_EQ(std::string(job_op_name(JobOp::DuoGc)), "gc");
    CHECK_EQ(std::string(job_group_name(JobOp::DuoScan)), "duostore");
    CHECK_EQ(std::string(job_op_name(JobOp::TierReconcile)), "reconcile");
    CHECK_EQ(std::string(job_group_name(JobOp::TierGc)), "tier");
    // Every op round-trips through its (group, op) pair
    for (JobOp op : {JobOp::Fsck, JobOp::DuoGc, JobOp::DuoScan, JobOp::TierScan, JobOp::TierGc, JobOp::TierReconcile}) {
        auto back = parse_job_op(job_group_name(op), job_op_name(op));
        CHECK(back && *back == op);
    }
    // The ops are per group: "gc" means different things under duostore and tier,
    // reconcile exists only under tier, fsck only under fsck
    CHECK(!parse_job_op("duostore", "reconcile"));
    CHECK(!parse_job_op("tier", "fsck"));
    CHECK(!parse_job_op("fsck", "gc"));
    CHECK(!parse_job_op("duostore", "quarantine"));
    CHECK(!parse_job_op("nope", "gc"));
}

// The tiered group: each round runs against a tiered backend, reports its
// *Stats struct, and the reconciliation ledger reads back; the fsck group refuses
// the tiered backend (no offline scrub) and the duostore group refuses it too
TEST(admin_jobs_tier_rounds_and_ledger) {
    TmpDirF tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    std::vector<BackendConfig> cfgs;
    cfgs.push_back(
        {"localdata", "localfs", {{"root", (tmp.path / "d").string()}, {"staging", (tmp.path / "s").string()}}});
    cfgs.push_back({"cloud", "memory", {}});
    cfgs.push_back({"tier",
                    "tiered",
                    {{"local", "localdata"},
                     {"cloud", "cloud"},
                     {"scan_interval", "0s"},
                     {"cold_after", "30d"},
                     {"space_high_watermark", "85%"},
                     {"quota_bytes", "64MiB"}}});
    auto out = StorageRegistry::build(cfgs, pool);
    auto& t = *out.at("tier");
    sync_wait(t.create_bucket("bkt"));
    backend_suite::put(t, "bkt", "a", std::string(1000, 'a'));
    backend_suite::put(t, "bkt", "b", std::string(2000, 'b'));

    // The synchronous entry: kind + the stats members flattened
    auto scan = run_job(JobOp::TierScan, t, 0);
    CHECK_EQ(scan.kind, "tiered");
    CHECK(scan.stats.contains("full"));
    CHECK(scan.stats["walked"].get<uint64_t>() >= 2);
    CHECK_EQ(scan.findings, uint64_t(0));
    bool threw = false;
    try {
        run_job(JobOp::TierScan, *out.at("cloud"), 0);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    CHECK(threw);
    threw = false;
    try {
        // tiered has no offline scrub
        run_job(JobOp::Fsck, t, 0);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    CHECK(threw);
    json ledger = quarantine_ledger("tier", t);
    CHECK_EQ(ledger["kind"].get<std::string>(), "tiered");
    CHECK(ledger["entries"].is_array() && ledger["entries"].empty());
    threw = false;
    try {
        quarantine_ledger("duostore", t);
    } catch (const std::invalid_argument&) {
        threw = true;
    }
    CHECK(threw);

    // The job manager: one slot per op, documents per op, the ledger per group
    AdminJobs jobs(out);
    CHECK(fails_with([&] { jobs.start("tier", JobOp::Fsck); }, AdminJobs::Error::Unsupported));
    CHECK(fails_with([&] { jobs.start("tier", JobOp::DuoGc); }, AdminJobs::Error::Unsupported));
    CHECK(fails_with([&] { jobs.quarantine("tier", "duostore"); }, AdminJobs::Error::Unsupported));
    CHECK(fails_with([&] { jobs.quarantine("tier", "nope"); }, AdminJobs::Error::Unsupported));
    json fresh = jobs.status("tier", JobOp::TierGc);
    CHECK_EQ(fresh["op"].get<std::string>(), "gc");
    CHECK(fresh["job_id"].is_null());

    json d1 = run_and_wait(jobs, "tier", JobOp::TierScan);
    CHECK_EQ(d1["job_id"].get<uint64_t>(), uint64_t(1));
    CHECK_EQ(d1["op"].get<std::string>(), "scan");
    CHECK_EQ(d1["kind"].get<std::string>(), "tiered");
    // fsck-only field
    CHECK(!d1.contains("max_mbps"));
    CHECK(!d1.contains("error"));
    CHECK(d1["stats"].contains("cold_picked"));
    CHECK_EQ(d1["findings"].get<uint64_t>(), uint64_t(0));
    json d2 = run_and_wait(jobs, "tier", JobOp::TierGc);
    CHECK_EQ(d2["job_id"].get<uint64_t>(), uint64_t(2));
    CHECK_EQ(d2["stats"]["resolved"].get<uint64_t>(), uint64_t(0));
    CHECK(d2["stats"].contains("removed_cloud"));
    json d3 = run_and_wait(jobs, "tier", JobOp::TierReconcile);
    CHECK_EQ(d3["job_id"].get<uint64_t>(), uint64_t(3));
    CHECK_EQ(d3["stats"]["refs_missing"].get<uint64_t>(), uint64_t(0));
    // nothing demoted
    CHECK_EQ(d3["stats"]["cloud_objects"].get<uint64_t>(), uint64_t(0));
    CHECK_EQ(d3["findings"].get<uint64_t>(), uint64_t(0));
    // Each op keeps its own last document
    CHECK_EQ(jobs.status("tier", JobOp::TierScan)["job_id"].get<uint64_t>(), uint64_t(1));
    CHECK_EQ(jobs.status("tier", JobOp::TierGc)["job_id"].get<uint64_t>(), uint64_t(2));
    CHECK(!jobs.busy("tier"));
    json q = jobs.quarantine("tier", "tier");
    CHECK_EQ(q["backend"].get<std::string>(), "tier");
    CHECK_EQ(q["kind"].get<std::string>(), "tiered");
    CHECK(q["entries"].empty());
    jobs.shutdown();
    for (auto& [n, b] : out) sync_wait(b->close());
}

#ifdef LIGHTS3_DUOSTORE
// The duostore group: gc / scan rounds and the corrupt-pack ledger, and the
// one-job-per-backend rule across ops (a throttled fsck blocks gc and scan, and
// their documents say "busy" without "running")
TEST(admin_jobs_duostore_rounds_and_cross_op_busy) {
    TmpDirF tmp;
    auto pool = std::make_shared<ThreadPool>(4);
    storage::DuoStoreConfig cfg;
    cfg.name = "duo";
    cfg.root = tmp.path / "duo";
    cfg.meta_path = cfg.root / "meta";
    cfg.meta_sync = false;
    auto duo = std::make_shared<storage::DuoStoreBackend>(std::move(cfg), pool);
    sync_wait(duo->create_bucket("bkt"));
    backend_suite::put(*duo, "bkt", "k1", std::string(1 << 20, 'a'));
    backend_suite::put(*duo, "bkt", "k2", std::string(1 << 20, 'b'));
    std::map<std::string, std::shared_ptr<IStorageBackend>> backends{{"duo", duo}};
    AdminJobs jobs(backends);

    // 1 MB/s over 2 MiB: the scrub runs for ~2 s, during which every other op is Busy
    CHECK_EQ(jobs.start("duo", JobOp::Fsck, 1000 * 1000), uint64_t(1));
    CHECK(fails_with([&] { jobs.start("duo", JobOp::DuoGc); }, AdminJobs::Error::Busy));
    CHECK(fails_with([&] { jobs.start("duo", JobOp::DuoScan); }, AdminJobs::Error::Busy));
    json gc_doc = jobs.status("duo", JobOp::DuoGc);
    CHECK(gc_doc["busy"].get<bool>());
    CHECK(!gc_doc["running"].get<bool>());
    CHECK(gc_doc["job_id"].is_null());
    json fsck_done = wait_done(jobs, "duo", JobOp::Fsck);
    CHECK_EQ(fsck_done["kind"].get<std::string>(), "duostore");
    CHECK_EQ(fsck_done["findings"].get<uint64_t>(), uint64_t(0));

    json gc = run_and_wait(jobs, "duo", JobOp::DuoGc);
    CHECK_EQ(gc["job_id"].get<uint64_t>(), uint64_t(2));
    CHECK_EQ(gc["kind"].get<std::string>(), "duostore");
    CHECK(gc["stats"].contains("reclaims_acked"));
    CHECK_EQ(gc["stats"]["packs_quarantined"].get<uint64_t>(), uint64_t(0));
    CHECK_EQ(gc["findings"].get<uint64_t>(), uint64_t(0));
    json scan = run_and_wait(jobs, "duo", JobOp::DuoScan);
    CHECK_EQ(scan["job_id"].get<uint64_t>(), uint64_t(3));
    CHECK(scan["stats"]["chunks_scanned"].get<uint64_t>() >= 1);
    CHECK_EQ(scan["stats"]["refs_missing"].get<uint64_t>(), uint64_t(0));
    CHECK_EQ(scan["findings"].get<uint64_t>(), uint64_t(0));
    CHECK(fails_with([&] { jobs.start("duo", JobOp::TierScan); }, AdminJobs::Error::Unsupported));
    json q = jobs.quarantine("duo", "duostore");
    CHECK_EQ(q["kind"].get<std::string>(), "duostore");
    CHECK(q["entries"].empty());
    CHECK(fails_with([&] { jobs.quarantine("duo", "tier"); }, AdminJobs::Error::Unsupported));
    jobs.shutdown();
    sync_wait(duo->close());
}
#endif

// The handlers: root gate, path shape, method mapping, hook error mapping (the
// hooks are fakes -- the real ones are the application's AdminJobs wrappers).
// The (group, op) pairs are resolved like the application does (parse_job_op)
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
    std::string last_backend, last_op;
    uint64_t last_bps = 0;
    // "fs" takes fsck, "tier" the tier ops, "duo" the duostore ops -- as AdminJobs
    // would refuse them (InvalidRequest), plus the unknown-op mapping of the app
    auto check_op = [](const std::string& b, const std::string& group, const std::string& op) {
        auto o = parse_job_op(group, op);
        if (!o) throw S3Error(S3ErrorCode::InvalidRequest, "no operation '" + op + "' in '" + group + "'.");
        if (b != "fs" && b != "tier" && b != "duo")
            throw S3Error(S3ErrorCode::NoSuchKey, "no backend named '" + b + "'");
        std::string want = group == "fsck" ? "fs" : group == "tier" ? "tier" : "duo";
        if (b != want) throw S3Error(S3ErrorCode::InvalidRequest, "wrong backend type");
        return *o;
    };
    svc.set_job_hooks(
        [&](const std::string& b, const std::string& group, const std::string& op, uint64_t bps) {
            JobOp o = check_op(b, group, op);
            if (running)
                throw S3Error(o == JobOp::Fsck ? S3ErrorCode::ScrubInProgress : S3ErrorCode::JobInProgress, "busy");
            running = true;
            last_backend = b;
            last_op = group + "." + op;
            last_bps = bps;
            return json{{"backend", b}, {"op", op}, {"job_id", 7}, {"running", true}};
        },
        [&](const std::string& b, const std::string& group, const std::string& op) {
            check_op(b, group, op);
            return json{{"backend", b}, {"op", op}, {"running", running}, {"job_id", 7}};
        },
        [&](const std::string& b, const std::string& group) {
            if (b != "tier" && b != "duo") throw S3Error(S3ErrorCode::NoSuchKey, "no backend named '" + b + "'");
            if ((group == "tier") != (b == "tier")) throw S3Error(S3ErrorCode::InvalidRequest, "wrong backend type");
            return json{{"backend", b}, {"kind", b == "tier" ? "tiered" : "duostore"}, {"entries", json::array()}};
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
    running = false;

    // ---- /-/admin/duostore|tier/<backend>/<op> (handlers/admin_jobs.cc) ----
    CHECK_EQ(call("POST", "/-/admin/tier/tier/scan", "", nullptr).status, 403);
    CHECK_EQ(call("POST", "/-/admin/tier/tier/scan", "", &p).status, 403);
    // no backend
    CHECK_EQ(call("POST", "/-/admin/tier/", "", &root).status, 400);
    // no op
    CHECK_EQ(call("POST", "/-/admin/tier/tier", "", &root).status, 400);
    CHECK_EQ(call("POST", "/-/admin/tier/tier/", "", &root).status, 400);
    CHECK_EQ(call("POST", "/-/admin/tier//scan", "", &root).status, 400);
    CHECK_EQ(call("POST", "/-/admin/tier/tier/scan/extra", "", &root).status, 400);
    CHECK_EQ(call("DELETE", "/-/admin/tier/tier/scan", "", &root).status, 405);
    // not a tier op
    CHECK_EQ(call("POST", "/-/admin/tier/tier/fsck", "", &root).status, 400);
    CHECK_EQ(call("POST", "/-/admin/duostore/duo/reconcile", "", &root).status, 400);
    CHECK_EQ(call("POST", "/-/admin/tier/nope/scan", "", &root).status, 404);
    // wrong type
    CHECK_EQ(call("POST", "/-/admin/duostore/tier/gc", "", &root).status, 400);
    for (const char* path : {"/-/admin/tier/tier/scan", "/-/admin/tier/tier/gc", "/-/admin/tier/tier/reconcile",
                             "/-/admin/duostore/duo/gc", "/-/admin/duostore/duo/scan"}) {
        auto idle = call("GET", path, "", &root);
        CHECK_EQ(idle.status, 200);
        CHECK(!json::parse(idle.small_body)["running"].get<bool>());
    }
    auto tstart = call("POST", "/-/admin/tier/tier/reconcile", "", &root);
    CHECK_EQ(tstart.status, 202);
    CHECK_EQ(json::parse(tstart.small_body)["job_id"].get<int>(), 7);
    CHECK_EQ(json::parse(tstart.small_body)["op"].get<std::string>(), "reconcile");
    CHECK_EQ(last_backend, "tier");
    CHECK_EQ(last_op, "tier.reconcile");
    // the rounds take no throttle
    CHECK_EQ(last_bps, uint64_t(0));
    auto tbusy = call("POST", "/-/admin/tier/tier/gc", "", &root);
    CHECK_EQ(tbusy.status, 409);
    CHECK_EQ(json::parse(tbusy.small_body)["code"].get<std::string>(), "JobInProgress");
    auto tst = call("GET", "/-/admin/tier/tier/reconcile", "", &root);
    CHECK_EQ(tst.status, 200);
    CHECK(json::parse(tst.small_body)["running"].get<bool>());
    running = false;
    auto dstart = call("POST", "/-/admin/duostore/duo/gc", "", &root);
    CHECK_EQ(dstart.status, 202);
    CHECK_EQ(last_op, "duostore.gc");

    // The ledgers: GET only, group must match the backend type
    auto tq = call("GET", "/-/admin/tier/tier/quarantine", "", &root);
    CHECK_EQ(tq.status, 200);
    CHECK_EQ(json::parse(tq.small_body)["kind"].get<std::string>(), "tiered");
    CHECK(json::parse(tq.small_body)["entries"].empty());
    auto dq = call("GET", "/-/admin/duostore/duo/quarantine", "", &root);
    CHECK_EQ(dq.status, 200);
    CHECK_EQ(json::parse(dq.small_body)["kind"].get<std::string>(), "duostore");
    CHECK_EQ(call("POST", "/-/admin/tier/tier/quarantine", "", &root).status, 405);
    CHECK_EQ(call("GET", "/-/admin/tier/duo/quarantine", "", &root).status, 400);
    CHECK_EQ(call("GET", "/-/admin/duostore/nope/quarantine", "", &root).status, 404);
    CHECK_EQ(call("GET", "/-/admin/tier/tier/quarantine", "", &p).status, 403);
}

// ---------- S3 Tables catalog reconciliation on fsck (docs/s3-tables/step-3-validation-diagnostics.md §9) ----------

TEST(admin_jobs_fsck_extension_merges_into_the_scrub) {
    TmpDirF tmp;
    auto pool = std::make_shared<ThreadPool>(2);
    std::vector<BackendConfig> cfgs;
    cfgs.push_back({"fs", "localfs", {{"root", (tmp.path / "d").string()}, {"staging", (tmp.path / "s").string()}}});
    auto out = StorageRegistry::build(cfgs, pool);
    auto& fs = *out.at("fs");
    sync_wait(fs.create_bucket("bkt"));
    backend_suite::put(fs, "bkt", "a", "aaaa");
    std::map<std::string, std::shared_ptr<IStorageBackend>> backends{{"fs", out.at("fs")}};
    AdminJobs jobs(backends);
    std::string seen;
    jobs.set_fsck_extension([&](const std::string& name) -> std::optional<JobOutcome> {
        seen = name;
        JobOutcome o;
        o.kind = "tables";
        o.findings = 2;
        o.stats = {{"tables", 1}, {"findings", json::array({{{"kind", "tables.dangling_pointer"}}})}};
        return o;
    });
    json s = run_and_wait(jobs, "fs", JobOp::Fsck);
    CHECK_EQ(seen, "fs");
    CHECK_EQ(s["kind"].get<std::string>(), "localfs");
    CHECK_EQ(s["findings"].get<uint64_t>(), uint64_t(2));
    CHECK_EQ(s["stats"]["tables"]["tables"].get<int>(), 1);
    CHECK_EQ(s["stats"]["objects_scanned"].get<int>(), 1);
    // the extension is fsck-only: a tier op never sees it
    JobOutcome base;
    base.kind = "localfs";
    base.stats = {{"objects_scanned", 3}};
    JobOutcome ext;
    ext.kind = "tables";
    ext.findings = 1;
    ext.stats = {{"tables", 4}};
    merge_outcome(base, ext);
    CHECK_EQ(base.findings, uint64_t(1));
    CHECK_EQ(base.stats["tables"]["tables"].get<int>(), 4);
    CHECK_EQ(base.stats["objects_scanned"].get<int>(), 3);
    for (auto& b : backends) sync_wait(b.second->close());
}

#ifdef LIGHTS3_TABLES
TEST(admin_jobs_tables_reconcile_findings) {
    using namespace lights3::tables;
    auto backend = std::make_shared<MemoryBackend>();
    std::map<std::string, std::shared_ptr<IStorageBackend>> bmap{{"mem", backend}};
    BucketsConfig bcfg;
    bcfg.default_backend = "mem";
    auto router = BucketRouter::build(bcfg, bmap);
    // nothing enabled: an empty report
    auto empty = sync_wait(reconcile_catalog(backend, router));
    CHECK_EQ(empty.table_buckets, uint64_t(0));
    CHECK(empty.findings.empty());
    TablesConfig cfg;
    cfg.enabled = true;
    auto buckets = sync_wait(TableBucketStore::load(backend));
    auto store = std::make_shared<ObjectCatalogStore>(backend);
    Catalog catalog(store, buckets, router, nullptr, cfg, MetricsScope{});
    sync_wait(backend->create_bucket("tbk"));
    sync_wait(catalog.enable_bucket("tbk"));
    Levels n{"n"};
    sync_wait(catalog.create_namespace("tbk", n, {}));
    CreateTableRequest r;
    r.name = "t";
    r.schema = json::parse(R"({"type":"struct","fields":[{"id":1,"name":"id","required":true,"type":"long"}]})");
    auto t = sync_wait(catalog.create_table("tbk", n, r, {}));
    r.name = "u";
    auto u = sync_wait(catalog.create_table("tbk", n, r, {}));
    r.name = "v";
    sync_wait(catalog.create_table("tbk", n, r, {}));
    auto clean = sync_wait(reconcile_catalog(backend, router));
    CHECK_EQ(clean.table_buckets, uint64_t(1));
    CHECK_EQ(clean.tables, uint64_t(3));
    CHECK(clean.findings.empty());
    // ① the pointer of t names a metadata object that is gone
    sync_wait(backend->delete_object("tbk", t.entry.metadata_location));
    // ② u is stuck in RENAMING without an intent
    TableEntry stuck = u.entry;
    stuck.state = TableState::Renaming;
    stuck.rename_id = "gone";
    sync_wait(store->put_table("tbk", n, "u", stuck, {}));
    // ③ catalog state for a bucket that is not table-enabled
    {
        ObjectMeta meta;
        http::StringBodyReader body("{}");
        sync_wait(backend->put_object(".sys", ObjectCatalogStore::ns_key("ghost", Levels{"x"}), std::move(meta), body));
    }
    // ④ an intent whose destination was never written although its stage says so
    RenameIntent bad;
    bad.rename_id = "r-bad";
    bad.src_levels = n;
    bad.dst_levels = n;
    bad.src_name = "v";
    bad.dst_name = "w";
    bad.src_etag = "x";
    bad.stage = RenameIntent::Stage::DestinationWritten;
    sync_wait(store->put_rename("tbk", bad, {}));
    auto rep = sync_wait(reconcile_catalog(backend, router));
    std::map<std::string, int> kinds;
    for (auto& f : rep.findings) ++kinds[f.kind];
    CHECK_EQ(kinds["tables.dangling_pointer"], 1);
    CHECK_EQ(kinds["tables.stale_renaming"], 1);
    CHECK_EQ(kinds["tables.orphan_state"], 1);
    CHECK(kinds["tables.inconsistent_rename"] >= 1);
    CHECK_EQ(rep.intents, uint64_t(1));
    json j = rep.to_json();
    CHECK_EQ(j["findings"].size(), rep.findings.size());
    CHECK_EQ(j["table_buckets"].get<int>(), 1);
    // a marker whose bucket vanished
    sync_wait(backend->create_bucket("tb2"));
    sync_wait(catalog.enable_bucket("tb2"));
    sync_wait(backend->delete_bucket("tb2"));
    rep = sync_wait(reconcile_catalog(backend, router));
    int orphan_markers = 0;
    for (auto& f : rep.findings)
        if (f.kind == "tables.orphan_state" && f.bucket == "tb2") ++orphan_markers;
    CHECK_EQ(orphan_markers, 1);
}
#endif
