// s3adm `bench` command group — closed-loop load generator over the shared
// SigV4 self-signing client (s3adm_common.h).
// IO modes: put / get (object upload/download, throughput in MiB/s); non-IO
// modes: stat (HeadObject) / list (ListObjectsV2) / list-buckets. Each worker
// thread owns one keep-alive connection and issues requests back to back;
// a reporter thread prints one stats line per second (interval ops/s, MiB/s,
// avg/max latency), and the run ends with a cumulative summary whose
// percentiles come from a log2 histogram (hence the ~ prefix: bucket-
// interpolated, not exact).
// put/get/stat/list work over a fixed pool of --objects keys under --prefix
// (put overwrites round-robin, so garbage stays bounded); get/stat/list
// pre-populate the pool before timing starts, and the pool is deleted at the
// end unless --keep. Uploads sign UNSIGNED-PAYLOAD, downloads stream the body
// into a byte counter — neither side pays a per-request SHA-256 or buffers
// whole objects.
#include "tools/s3adm_bench.h"

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <chrono>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "core/util/uri.h"
#include "s3/errors.h"
#include "tools/s3adm_common.h"

namespace {

using s3adm::ConnOpts;
using s3adm::g_exit;
using s3adm::SignedClient;
namespace util = lights3::util;
using Clock = std::chrono::steady_clock;

enum class Mode { Put, Get, Stat, List, ListBuckets };

struct BenchOpts {
    ConnOpts conn;
    std::string bucket;
    std::string prefix;
    uint64_t size = 0;
    int concurrency = 4;
    int duration_sec = 10;
    int objects = 64;
    int max_keys = 100;
    bool keep = false;
};

// "4096" / "8K" / "1M" / "2G" (binary units); capped so the shared upload buffer stays sane
uint64_t parse_size(const std::string& s) {
    if (s.empty()) throw std::runtime_error("empty --size");
    uint64_t mult = 1;
    std::string num = s;
    switch (s.back()) {
        case 'k': case 'K': mult = 1024ULL; break;
        case 'm': case 'M': mult = 1024ULL * 1024; break;
        case 'g': case 'G': mult = 1024ULL * 1024 * 1024; break;
        default: break;
    }
    if (mult != 1) num.pop_back();
    uint64_t v;
    try {
        size_t pos = 0;
        v = std::stoull(num, &pos);
        if (pos != num.size()) throw std::runtime_error("");
    } catch (...) {
        throw std::runtime_error("invalid --size: " + s + " (use bytes or K/M/G suffix)");
    }
    v *= mult;
    if (v > 1024ULL * 1024 * 1024)
        throw std::runtime_error("--size too large (max 1G): " + s);
    return v;
}

std::string human_size(uint64_t b) {
    char buf[32];
    if (b >= 1024ULL * 1024 * 1024)
        snprintf(buf, sizeof buf, "%.1f GiB", double(b) / (1024.0 * 1024 * 1024));
    else if (b >= 1024ULL * 1024)
        snprintf(buf, sizeof buf, "%.1f MiB", double(b) / (1024.0 * 1024));
    else if (b >= 1024)
        snprintf(buf, sizeof buf, "%.1f KiB", double(b) / 1024.0);
    else
        snprintf(buf, sizeof buf, "%llu B", (unsigned long long)b);
    return buf;
}

std::string obj_key(const BenchOpts& o, int i) {
    char buf[16];
    snprintf(buf, sizeof buf, "obj-%06d", i);
    return o.prefix + buf;
}

std::string obj_path(const BenchOpts& o, int i) {
    return "/" + o.bucket + "/" + util::aws_uri_encode(obj_key(o, i), /*encode_slash=*/false);
}

// Shared counters; workers only touch atomics on the hot path. Latencies land
// in a log2(us) histogram — constant memory at any rate, percentiles are
// bucket-interpolated approximations
struct Stats {
    std::atomic<uint64_t> ops{0}, bytes{0}, errs{0}, lat_sum_us{0};
    std::atomic<uint64_t> interval_max_us{0};  // reporter resets each tick
    std::atomic<uint64_t> total_max_us{0};
    std::array<std::atomic<uint64_t>, 65> hist{};
    std::mutex err_mu;
    bool err_printed = false;

    void record(uint64_t us, uint64_t nbytes) {
        ops.fetch_add(1, std::memory_order_relaxed);
        bytes.fetch_add(nbytes, std::memory_order_relaxed);
        lat_sum_us.fetch_add(us, std::memory_order_relaxed);
        hist[std::bit_width(us)].fetch_add(1, std::memory_order_relaxed);
        for (auto* m : {&interval_max_us, &total_max_us}) {
            uint64_t cur = m->load(std::memory_order_relaxed);
            while (us > cur && !m->compare_exchange_weak(cur, us, std::memory_order_relaxed)) {
            }
        }
    }

    // Errors count toward neither latency nor bytes; the first one is echoed to stderr so a 100%-failure run is diagnosable
    void record_err(const std::string& msg) {
        errs.fetch_add(1, std::memory_order_relaxed);
        std::lock_guard<std::mutex> lk(err_mu);
        if (!err_printed) {
            err_printed = true;
            fprintf(stderr, "s3adm: bench: first error: %s\n", msg.c_str());
        }
    }
};

std::string result_err(const httplib::Result& r) {
    if (!r) return "transport error: " + httplib::to_string(r.error());
    std::string body = r->body.substr(0, 200);
    for (auto& c : body)
        if (c == '\n') c = ' ';
    return "HTTP " + std::to_string(r->status) + (body.empty() ? "" : " " + body);
}

// q in (0,1]; approximate percentile in us from the log2 histogram, linear within the bucket
double hist_pct(const std::array<std::atomic<uint64_t>, 65>& hist, uint64_t total, double q) {
    if (total == 0) return 0;
    uint64_t need = (uint64_t)(q * double(total));
    if (need < 1) need = 1;
    uint64_t cum = 0;
    for (size_t b = 0; b < hist.size(); ++b) {
        uint64_t c = hist[b].load();
        if (c && cum + c >= need) {
            double lo = b == 0 ? 0.0 : double(1ULL << (b - 1));
            double hi = b == 0 ? 1.0 : double(1ULL << b);
            return lo + (double(need - cum) / double(c)) * (hi - lo);
        }
        cum += c;
    }
    return 0;
}

// One benchmark iteration; returns bytes moved via *nbytes and ok/expected-status
bool do_op(Mode mode, SignedClient& cli, const BenchOpts& o, int idx, const std::string& body,
           uint64_t* nbytes, std::string* err) {
    httplib::Result r;
    int expect = 200;
    switch (mode) {
        case Mode::Put:
            r = cli.put_unsigned(obj_path(o, idx), body);
            *nbytes = body.size();
            break;
        case Mode::Get:
            r = cli.get_discard(obj_path(o, idx), nbytes);
            break;
        case Mode::Stat:
            r = cli.head(obj_path(o, idx));
            break;
        case Mode::List:
            r = cli.get("/" + o.bucket,
                        "list-type=2&max-keys=" + std::to_string(o.max_keys) +
                            "&prefix=" + util::aws_uri_encode(o.prefix, /*encode_slash=*/true));
            if (r) *nbytes = r->body.size();
            break;
        case Mode::ListBuckets:
            r = cli.get("/", "");
            if (r) *nbytes = r->body.size();
            break;
    }
    if (r && r->status == expect) return true;
    *err = result_err(r);
    return false;
}

void worker(Mode mode, const BenchOpts& o, int id, const std::string& body,
            std::atomic<bool>& stop, Stats& st) {
    try {
        SignedClient cli(o.conn);
        // Stagger start offsets so concurrent GET/STAT workers spread over the key pool
        uint64_t i = o.objects ? uint64_t(id) * uint64_t(o.objects) / uint64_t(o.concurrency) : 0;
        while (!stop.load(std::memory_order_relaxed)) {
            uint64_t nbytes = 0;
            std::string err;
            auto t0 = Clock::now();
            bool ok = do_op(mode, cli, o, int(i % uint64_t(o.objects ? o.objects : 1)), body,
                            &nbytes, &err);
            auto us = uint64_t(
                std::chrono::duration_cast<std::chrono::microseconds>(Clock::now() - t0)
                    .count());
            if (ok)
                st.record(us, nbytes);
            else
                st.record_err(err);
            ++i;
        }
    } catch (const std::exception& e) {
        st.record_err(e.what());
        // A worker that cannot even build its client stops; the others keep the run alive
    }
}

void reporter(Stats& st, std::atomic<bool>& done) {
    printf("%5s %10s %8s %10s %8s %8s %8s\n", "sec", "ops", "ops/s", "MiB/s", "avg-ms",
           "max-ms", "errs");
    fflush(stdout);
    uint64_t l_ops = 0, l_bytes = 0, l_lat = 0, l_errs = 0;
    int sec = 0;
    auto next = Clock::now() + std::chrono::seconds(1);
    while (!done.load()) {
        // Sleep in slices so the thread exits promptly when the run ends mid-tick
        while (!done.load() && Clock::now() < next)
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (done.load()) break;
        next += std::chrono::seconds(1);
        ++sec;
        uint64_t ops = st.ops.load(), bytes = st.bytes.load(), lat = st.lat_sum_us.load(),
                 errs = st.errs.load();
        uint64_t d_ops = ops - l_ops, d_bytes = bytes - l_bytes, d_lat = lat - l_lat,
                 d_errs = errs - l_errs;
        uint64_t imax = st.interval_max_us.exchange(0);
        // ops column is cumulative; ops/s and the rest are this interval's
        printf("%5d %10llu %8llu %10.2f %8.2f %8.1f %8llu\n", sec, (unsigned long long)ops,
               (unsigned long long)d_ops, d_bytes / (1024.0 * 1024.0),
               d_ops ? double(d_lat) / double(d_ops) / 1000.0 : 0.0, imax / 1000.0,
               (unsigned long long)d_errs);
        fflush(stdout);
        l_ops = ops;
        l_bytes = bytes;
        l_lat = lat;
        l_errs = errs;
    }
}

// CreateBucket; a pre-existing bucket (409) is fine
bool ensure_bucket(SignedClient& cli, const BenchOpts& o) {
    auto r = cli.put_unsigned("/" + o.bucket, "");
    if (r && (r->status == 200 || r->status == 409)) return true;
    fprintf(stderr, "s3adm: bench: create bucket %s failed: %s\n", o.bucket.c_str(),
            result_err(r).c_str());
    return false;
}

// Uploads the key pool before timing starts (get/stat/list need existing objects)
bool prepare_pool(const BenchOpts& o, const std::string& body) {
    int threads = std::min(o.concurrency, o.objects);
    std::atomic<bool> failed{false};
    std::atomic<int> next{0};
    std::vector<std::thread> ts;
    for (int t = 0; t < threads; ++t)
        ts.emplace_back([&] {
            try {
                SignedClient cli(o.conn);
                for (int i; (i = next.fetch_add(1)) < o.objects && !failed.load();) {
                    auto r = cli.put_unsigned(obj_path(o, i), body);
                    if (!r || r->status != 200) {
                        if (!failed.exchange(true))
                            fprintf(stderr, "s3adm: bench: prepare %s failed: %s\n",
                                    obj_key(o, i).c_str(), result_err(r).c_str());
                    }
                }
            } catch (const std::exception& e) {
                if (!failed.exchange(true))
                    fprintf(stderr, "s3adm: bench: prepare failed: %s\n", e.what());
            }
        });
    for (auto& t : ts) t.join();
    return !failed.load();
}

void cleanup_pool(const BenchOpts& o) {
    try {
        SignedClient cli(o.conn);
        int deleted = 0;
        for (int i = 0; i < o.objects; ++i) {
            auto r = cli.del(obj_path(o, i));
            if (r && r->status == 204) ++deleted;
        }
        printf("cleanup: deleted %d/%d objects under %s/%s (skip with --keep)\n", deleted,
               o.objects, o.bucket.c_str(), o.prefix.c_str());
    } catch (const std::exception& e) {
        fprintf(stderr, "s3adm: bench: cleanup failed: %s\n", e.what());
    }
}

const char* mode_name(Mode m) {
    switch (m) {
        case Mode::Put: return "put";
        case Mode::Get: return "get";
        case Mode::Stat: return "stat";
        case Mode::List: return "list";
        case Mode::ListBuckets: return "list-buckets";
    }
    return "?";
}

int run_bench(Mode mode, const BenchOpts& o) {
    bool has_pool = mode != Mode::ListBuckets;
    bool needs_prepare = mode == Mode::Get || mode == Mode::Stat || mode == Mode::List;

    // One shared read-only buffer of incompressible-ish data for every upload
    std::string body;
    if (mode == Mode::Put || needs_prepare) {
        body.resize(o.size);
        std::mt19937_64 rng(0x5335334d42454e43ULL);  // fixed seed: runs are comparable
        for (size_t i = 0; i + 8 <= body.size(); i += 8) {
            uint64_t v = rng();
            memcpy(&body[i], &v, 8);
        }
    }

    if (has_pool) {
        SignedClient setup(o.conn);
        if (!ensure_bucket(setup, o)) return 1;
        if (needs_prepare) {
            printf("preparing %d objects of %s under %s/%s ...\n", o.objects,
                   human_size(o.size).c_str(), o.bucket.c_str(), o.prefix.c_str());
            fflush(stdout);
            if (!prepare_pool(o, body)) return 1;
        }
    }

    printf("bench %s: %d workers, %d s%s\n", mode_name(mode), o.concurrency, o.duration_sec,
           mode == Mode::Put || mode == Mode::Get
               ? (", " + human_size(o.size) + " objects, " + std::to_string(o.objects) + " keys")
                     .c_str()
               : "");
    fflush(stdout);

    Stats st;
    std::atomic<bool> stop{false}, done{false};
    auto t_start = Clock::now();
    std::vector<std::thread> workers;
    for (int i = 0; i < o.concurrency; ++i)
        workers.emplace_back(worker, mode, std::cref(o), i, std::cref(body), std::ref(stop),
                             std::ref(st));
    std::thread rep(reporter, std::ref(st), std::ref(done));

    std::this_thread::sleep_for(std::chrono::seconds(o.duration_sec));
    stop.store(true);
    for (auto& t : workers) t.join();
    done.store(true);
    rep.join();
    double wall = std::chrono::duration<double>(Clock::now() - t_start).count();

    uint64_t ops = st.ops.load(), errs = st.errs.load(), bytes = st.bytes.load();
    printf("---- bench %s summary ----\n", mode_name(mode));
    printf("wall %.2f s   workers %d%s\n", wall, o.concurrency,
           has_pool ? ("   keys " + std::to_string(o.objects)).c_str() : "");
    printf("ops %llu ok, %llu err   %.1f ops/s   %.2f MiB/s\n", (unsigned long long)ops,
           (unsigned long long)errs, ops / wall, bytes / (1024.0 * 1024.0) / wall);
    if (ops)
        printf("latency ms: avg %.2f   p50 ~%.2f   p90 ~%.2f   p99 ~%.2f   max %.2f\n",
               double(st.lat_sum_us.load()) / double(ops) / 1000.0,
               hist_pct(st.hist, ops, 0.50) / 1000.0, hist_pct(st.hist, ops, 0.90) / 1000.0,
               hist_pct(st.hist, ops, 0.99) / 1000.0, st.total_max_us.load() / 1000.0);
    fflush(stdout);

    if (has_pool && !o.keep) cleanup_pool(o);
    return errs ? 1 : 0;
}

// ---- flags & subcommand wiring ----

void add_bench_flags(const std::shared_ptr<ccmd::c_command>& cmd, const char* def_size,
                     bool with_pool) {
    if (with_pool) {
        cmd->varp<std::string>("bucket", "b", "", "target bucket (created if missing).");
        cmd->var<std::string>("prefix", "s3adm-bench/", "key prefix for benchmark objects.");
        cmd->varp<std::string>("size", "s", def_size,
                               "object size (bytes, or K/M/G suffix, max 1G).");
        cmd->varp<int>("objects", "n", 64, "key pool size (put overwrites round-robin).");
        cmd->var<bool>("keep", false, "keep benchmark objects instead of deleting them at the end.");
    }
    cmd->varp<int>("concurrency", "j", 4, "worker threads (one connection each).");
    cmd->varp<int>("duration-sec", "d", 10, "measured run time in seconds.");
    s3adm::add_conn_flags(cmd);
}

// Flag validation + BenchOpts assembly; false = usage error already reported
bool read_bench_opts(const std::shared_ptr<ccmd::c_command>& c, bool with_pool, BenchOpts& o) {
    if (!c->args().empty()) {
        fprintf(stderr, "s3adm: usage: %s\n", c->usage().c_str());
        g_exit = 2;
        return false;
    }
    if (!s3adm::read_conn_opts(c, o.conn)) return false;
    o.concurrency = c->var<int>("concurrency");
    o.duration_sec = c->var<int>("duration-sec");
    if (o.concurrency < 1 || o.concurrency > 256) {
        fprintf(stderr, "s3adm: --concurrency must be in [1,256]\n");
        g_exit = 2;
        return false;
    }
    if (o.duration_sec < 1 || o.duration_sec > 86400) {
        fprintf(stderr, "s3adm: --duration-sec must be in [1,86400]\n");
        g_exit = 2;
        return false;
    }
    if (!with_pool) return true;
    o.bucket = c->var<std::string>("bucket");
    o.prefix = c->var<std::string>("prefix");
    o.objects = c->var<int>("objects");
    o.keep = c->var<bool>("keep");
    o.size = parse_size(c->var<std::string>("size"));
    if (o.bucket.empty()) {
        fprintf(stderr, "s3adm: --bucket is required\n");
        g_exit = 2;
        return false;
    }
    if (o.objects < 1 || o.objects > 1000000) {
        fprintf(stderr, "s3adm: --objects must be in [1,1000000]\n");
        g_exit = 2;
        return false;
    }
    return true;
}

void run_mode(const std::shared_ptr<ccmd::c_command>& c, Mode mode) {
    try {
        BenchOpts o;
        if (!read_bench_opts(c, mode != Mode::ListBuckets, o)) return;
        if (mode == Mode::List) o.max_keys = c->var<int>("max-keys");
        g_exit = run_bench(mode, o);
    } catch (const lights3::s3::S3Error& e) {
        fprintf(stderr, "s3adm: %s\n", e.message.c_str());
        g_exit = 1;
    } catch (const std::exception& e) {
        fprintf(stderr, "s3adm: %s\n", e.what());
        g_exit = 1;
    }
}

std::shared_ptr<ccmd::c_command> make_mode(Mode mode, const char* name, const char* example,
                                           const char* usage, const char* desc,
                                           const char* brief, const char* def_size) {
    auto cmd = std::make_shared<ccmd::c_command>(
        name, example, usage, desc, brief,
        [mode](const std::shared_ptr<ccmd::c_command>& c) { run_mode(c, mode); });
    if (mode == Mode::List)
        cmd->var<int>("max-keys", 100, "max-keys per ListObjectsV2 request.");
    add_bench_flags(cmd, def_size, mode != Mode::ListBuckets);
    return cmd;
}

}  // namespace

namespace s3adm {

// `bench` command group: pure dispatcher, holds no options of its own (same
// convention as `cred`)
std::shared_ptr<ccmd::c_command> make_bench() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "bench", "s3adm bench put --bucket=test --size=1M --concurrency=8 --duration-sec=30",
        "s3adm bench <command> [options]",
        "Benchmark the S3 data plane (put/get) and non-IO APIs (stat/list/list-buckets) "
        "with per-second stats. Workers run closed-loop over a pool of --objects keys "
        "under --prefix; get/stat/list upload the pool first, and it is deleted at the "
        "end unless --keep. Credentials come from each subcommand's --ak=/--sk= or from "
        "env LIGHTS3_ADMIN_AK/LIGHTS3_ADMIN_SK; options must follow the leaf subcommand "
        "as --name=value.",
        "benchmark S3 IO and non-IO APIs.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_mode(
        Mode::Put, "put", "s3adm bench put --bucket=test --size=4M --concurrency=8",
        "s3adm bench put [options]",
        "Upload benchmark: PUT objects of --size round-robin over the key pool "
        "(UNSIGNED-PAYLOAD, so no client-side SHA-256).",
        "object upload benchmark.", "1M"));
    cmd->add_subcommand(make_mode(
        Mode::Get, "get", "s3adm bench get --bucket=test --size=4M --concurrency=8",
        "s3adm bench get [options]",
        "Download benchmark: pre-uploads the key pool, then GETs it round-robin "
        "(bodies are streamed and discarded).",
        "object download benchmark.", "1M"));
    cmd->add_subcommand(make_mode(
        Mode::Stat, "stat", "s3adm bench stat --bucket=test --concurrency=16",
        "s3adm bench stat [options]",
        "HeadObject benchmark (non-IO): pre-uploads the key pool, then HEADs it "
        "round-robin.",
        "HeadObject (metadata) benchmark.", "4K"));
    cmd->add_subcommand(make_mode(
        Mode::List, "list", "s3adm bench list --bucket=test --max-keys=100",
        "s3adm bench list [options]",
        "ListObjectsV2 benchmark (non-IO): pre-uploads the key pool, then lists "
        "under --prefix with --max-keys per request.",
        "ListObjectsV2 benchmark.", "4K"));
    cmd->add_subcommand(make_mode(
        Mode::ListBuckets, "list-buckets", "s3adm bench list-buckets --concurrency=16",
        "s3adm bench list-buckets [options]",
        "ListBuckets benchmark (non-IO): GET / in a loop; needs no bucket and "
        "creates no objects.",
        "ListBuckets benchmark.", "4K"));
    return cmd;
}

}  // namespace s3adm
