// Process entry point: command-line parsing (ccmd, third_party/ccmd) +
// Application lifecycle (src/app/app.h). Assembly and startup/shutdown
// ordering live in lights3::Application (docs/architecture.md §4).
//
// Command tree:
//   lights3 [--config=<path>]                     start the server
//   lights3 duostore dump <backend> <file> [...]  duostore meta admin
//   lights3 duostore load <backend> <file> [...]  (docs/storage/duostore-core.md §11)
//   lights3 duostore gc|scan <backend> [...]      run one GC / orphan-scan round (roadmap §3.2)
//   lights3 duostore quarantine list|release|purge ...  corrupt-pack quarantine (roadmap §3.7)
//   lights3 tier scan|gc|reconcile <backend> [..] tiered background tasks on demand (roadmap §3.2)
//   lights3 tier quarantine list|forget|purge ...  tiered reconcile quarantine ledger (roadmap §3.6)
//   lights3 fsck <backend> [--max-mbps=<n>] [...] offline integrity scrub (roadmap §3.1)
//
// ccmd's root options do not propagate down, so --config is registered on
// every leaf. cflag accepts long-option values only as --name=value; the
// `--name value` form used by the e2e scripts and older docs is folded into
// that shape by normalize_argv below, so both keep working.
#include <ccmd.h>

#include <algorithm>

#include <charconv>
#include <cstdio>
#include <memory>
#include <stdexcept>
#include <string>
#include <tuple>
#include <vector>

#include "app/app.h"
#include "http/server.h"
#include "storage/registry.h"
#include "core/log.h"
#include "storage/localfs/localfs_backend.h"
#include "core/util/time.h"
#include "storage/tiered/tiered_backend.h"
#ifdef LIGHTS3_DUOSTORE
#include <fstream>

#include "storage/duostore/duostore_backend.h"
#endif

namespace {

using Cmd = std::shared_ptr<ccmd::c_command>;

constexpr const char* kDefaultConfig = "config/lights3.yaml";

// ccmd callbacks return nothing; the process exit code is carried out through
// this (0 success / 1 runtime failure / 2 usage error)
int g_exit = 0;

void add_config_flag(const Cmd& cmd) {
    cmd->varp<std::string>("config", "c", kDefaultConfig, "Path to the lights3 YAML config file");
}

// Rewrite `--config <v>` into `--config=<v>` (the only form cflag understands
// for non-bool long options). Applies to the value-taking long options
// registered in this file; everything after `--` is left untouched.
std::vector<std::string> normalize_argv(int argc, char** argv) {
    static const char* const kValueFlags[] = {"--config", "--backend", "--file", "--max-mbps"};
    std::vector<std::string> out;
    out.reserve(static_cast<size_t>(argc));
    for (int i = 0; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--") {
            for (; i < argc; ++i) out.emplace_back(argv[i]);
            break;
        }
        bool folded = false;
        for (const char* f : kValueFlags) {
            if (a == f && i + 1 < argc) {
                out.push_back(a + "=" + argv[i + 1]);
                ++i;
                folded = true;
                break;
            }
        }
        if (!folded) out.push_back(std::move(a));
    }
    return out;
}

// Default action: full server lifecycle. A late startup failure unwinds
// through ~Application, which closes every backend already built: duostore's
// active pack gets sealed and rados flushed even on the error path (see
// Application::shutdown)
// `lights3 --check-config` (roadmap §6.2): parse + validate the configuration and
// print what it resolves to, without opening a backend or binding a port. Exit 0 =
// the server would start with this file (modulo runtime failures such as an
// unreachable data directory), 1 = the file is rejected, with the same message the
// server would print as `fatal:`
int check_config(const std::string& path) {
    using namespace lights3;
    Config cfg;
    try {
        cfg = Config::load(path);
    } catch (const std::exception& e) {
        fprintf(stderr, "config error: %s\n", e.what());
        return 1;
    }
    int problems = 0;
    auto drivers = http::HttpServerFactory::drivers();
    if (std::find(drivers.begin(), drivers.end(), cfg.http.driver) == drivers.end()) {
        fprintf(stderr, "config error: http.driver '%s' is not compiled into this binary\n",
                cfg.http.driver.c_str());
        ++problems;
    }
    auto types = storage::StorageRegistry::registered_types();
    for (auto& b : cfg.backends)
        if (std::find(types.begin(), types.end(), b.type) == types.end()) {
            fprintf(stderr, "config error: backends[%s].type '%s' is not compiled into this binary\n",
                    b.name.c_str(), b.type.c_str());
            ++problems;
        }
    printf("config %s: %s\n", path.c_str(), problems ? "REJECTED" : "ok");
    printf("  http      driver=%s bind=%s:%u tls=%s metrics_access=%s\n", cfg.http.driver.c_str(),
           cfg.http.bind.c_str(), unsigned(cfg.http.port), cfg.http.tls_cert.empty() ? "off" : "on",
           cfg.http.metrics_access.c_str());
    printf("  runtime   io_threads=%d max_inflight_requests=%d\n", cfg.runtime.io_threads,
           cfg.runtime.max_inflight_requests);
    printf("  auth      static_credentials=%zu credentials_file=%s region=%s\n",
           cfg.auth.credentials.size(),
           cfg.auth.credentials_file.empty() ? "-" : cfg.auth.credentials_file.c_str(),
           cfg.auth.region.c_str());
    printf("  backends  %zu\n", cfg.backends.size());
    for (auto& b : cfg.backends) printf("    - %s: type=%s\n", b.name.c_str(), b.type.c_str());
    printf("  buckets   default_backend=%s rules=%zu\n", cfg.buckets.default_backend.c_str(),
           cfg.buckets.rules.size());
    printf("  website   static_entries=%zu\n", cfg.website.buckets.size());
    printf("  log       level=%s format=%s sink=%s async=%s slow_request_threshold=%dms\n",
           cfg.log.level.c_str(), cfg.log.format.c_str(),
           cfg.log.file.empty() ? "stderr" : cfg.log.file.c_str(), cfg.log.async ? "on" : "off",
           cfg.log.slow_request_threshold_ms);
    printf("  audit     %s\n", cfg.audit.path.empty() ? "off" : cfg.audit.path.c_str());
    return problems ? 1 : 0;
}

void run_server(const Cmd& c) {
    if (c->var<bool>("check-config")) {
        g_exit = check_config(c->var<std::string>("config"));
        return;
    }
    lights3::Application app(c->var<std::string>("config"));
    app.open_storage();
    app.start_server();
    g_exit = app.run();
}

// Shared by every single-backend admin leaf (duostore gc/scan, tier *, fsck):
// `<backend>` positional or --backend=, positional wins
std::string one_backend_arg(const Cmd& c) {
    std::string backend = c->var<std::string>("backend");
    const auto& pos = c->args();
    if (pos.size() > 1) {
        g_exit = 2;
        throw std::runtime_error(c->name() + ": too many arguments");
    }
    if (pos.size() == 1) backend = pos[0];
    if (backend.empty()) {
        c->print_help();
        g_exit = 2;
        throw std::runtime_error(c->name() + ": <backend> is required");
    }
    return backend;
}

// Leaf factory for commands taking only `<backend>` (+ --config)
Cmd make_backend_leaf(const char* name, const char* example, const char* usage,
                      const char* help_long, const char* help_short, void (*run)(const Cmd&)) {
    auto cmd = std::make_shared<ccmd::c_command>(name, example, usage, help_long, help_short, run);
    add_config_flag(cmd);
    cmd->var<std::string>("backend", "", "backend name (alternative to the positional)");
    return cmd;
}

#ifdef LIGHTS3_DUOSTORE
// duostore meta admin: backends are built, the server has not started (write
// quiescence holds trivially). Any failure throws loudly and converges
// through main's fallback path
struct AdminArgs {
    std::string config, backend, file;
};

// `<backend> <file>` positionals, or --backend=/--file= (either spelling,
// positionals win when both are given)
AdminArgs admin_args(const Cmd& c) {
    AdminArgs a;
    a.config = c->var<std::string>("config");
    a.backend = c->var<std::string>("backend");
    a.file = c->var<std::string>("file");
    const auto& pos = c->args();
    if (pos.size() > 2) {
        g_exit = 2;
        throw std::runtime_error("duostore " + c->name() + ": too many arguments");
    }
    if (pos.size() >= 1) a.backend = pos[0];
    if (pos.size() == 2) a.file = pos[1];
    if (a.backend.empty() || a.file.empty()) {
        c->print_help();
        g_exit = 2;
        throw std::runtime_error("duostore " + c->name() + ": <backend> and <file> are required");
    }
    return a;
}

lights3::storage::DuoStoreBackend* find_duostore(lights3::Application& app, const std::string& name) {
    const auto& backends = app.backends();
    auto it = backends.find(name);
    if (it == backends.end()) throw std::runtime_error("duostore: no backend named '" + name + "'");
    auto* duo = dynamic_cast<lights3::storage::DuoStoreBackend*>(it->second.get());
    if (!duo) throw std::runtime_error("duostore: backend '" + name + "' is not duostore");
    return duo;
}

void run_dump(const Cmd& c) {
    using namespace lights3;
    AdminArgs a = admin_args(c);
    Application app(a.config);
    app.open_storage();
    auto* duo = find_duostore(app, a.backend);
    std::ofstream f(a.file, std::ios::binary | std::ios::trunc);
    if (!f) throw std::runtime_error("duostore dump: cannot open for write: " + a.file);
    auto st = sync_wait(duo->run_meta_dump(f));
    LOG_INFO("duostore admin: dumped {} buckets / {} objects / {} sealed packs to {}",
             st.buckets, st.objects, st.sealed_packs, a.file);
    app.shutdown();
}

void run_load(const Cmd& c) {
    using namespace lights3;
    AdminArgs a = admin_args(c);
    Application app(a.config);
    app.open_storage();
    auto* duo = find_duostore(app, a.backend);
    std::ifstream f(a.file, std::ios::binary);
    if (!f) throw std::runtime_error("duostore load: cannot open for read: " + a.file);
    auto st = sync_wait(duo->run_meta_load(f));
    LOG_INFO("duostore admin: loaded {} buckets / {} objects / {} sealed packs from {}",
             st.buckets, st.objects, st.sealed_packs, a.file);
    app.shutdown();
}

// Background tasks on demand (roadmap §3.2): the run_*_once hooks were only
// reachable through timers (GC every 5min, orphan scan daily by default) —
// an operator wanting space back *now* had nothing to call. Offline like
// dump/load: with a local meta engine (rocksdb/sqlite) the file lock demands
// the server be stopped; with a shared engine (redis/tikv) this can run next
// to live gateways — the GC lease coordinates. Stats are logged; exit code
// stays 0 (running the task succeeded — refs_missing etc. are already
// LOG_ERROR'd, and integrity verdicts belong to `lights3 fsck`)
void run_duo_gc(const Cmd& c) {
    using namespace lights3;
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* duo = find_duostore(app, one_backend_arg(c));
    auto st = sync_wait(duo->run_gc_once());
    LOG_INFO("duostore admin: gc round: reclaims {} (grace-skipped {}, pinned {}, "
             "leased {}), files removed {}, packs removed {}, uploads expired {}, packs "
             "sealed-aged {}, compacted {} (deferred {}), records migrated {}, corrupt {}, "
             "packs quarantined {}",
             st.reclaims_acked, st.skipped_grace, st.skipped_pinned, st.skipped_leased,
             st.files_removed, st.packs_removed, st.uploads_expired, st.packs_sealed_aged,
             st.packs_compacted, st.packs_compact_deferred, st.records_migrated,
             st.records_corrupt, st.packs_quarantined);
    app.shutdown();
}

void run_duo_scan(const Cmd& c) {
    using namespace lights3;
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* duo = find_duostore(app, one_backend_arg(c));
    auto st = sync_wait(duo->run_orphan_scan_once());
    LOG_INFO("duostore admin: orphan scan: {} chunks ({} bytes) / {} packs ({} bytes) "
             "scanned; orphans removed {} (grace-skipped {}, pinned {}, gcq-pending {}), "
             "orphan packs removed {} (skipped active {}); refs missing {}, packstats "
             "missing {}",
             st.chunks_scanned, st.chunk_bytes, st.packs_scanned, st.pack_bytes,
             st.orphans_removed, st.skipped_grace, st.skipped_pinned, st.skipped_gcq,
             st.orphan_packs_removed, st.packs_skipped_active, st.refs_missing,
             st.pack_stats_missing);
    app.shutdown();
}

// Corrupt-pack quarantine (roadmap §3.7): packs whose compaction cannot converge
// because of corrupt records are parked by GC; these are the operator exits.
// Pack ids accept the 16-digit hex the logs print, 0x-prefixed hex, or decimal
uint64_t parse_pack_id(const std::string& s) {
    uint64_t id = 0;
    int base = 10;
    size_t off = 0;
    if (s.size() > 2 && s[0] == '0' && (s[1] == 'x' || s[1] == 'X')) {
        base = 16;
        off = 2;
    } else if (s.size() == 16) {
        base = 16;  // the {:016x} form GC logs and `quarantine list` print
    }
    auto res = std::from_chars(s.data() + off, s.data() + s.size(), id, base);
    if (res.ec != std::errc{} || res.ptr != s.data() + s.size())
        throw std::runtime_error("invalid pack id '" + s +
                                 "' (16-digit hex as logged, 0x-prefixed hex, or decimal)");
    return id;
}

std::pair<std::string, uint64_t> backend_pack_args(const Cmd& c) {
    std::string backend = c->var<std::string>("backend");
    std::vector<std::string> pos = c->args();
    if (!backend.empty() && pos.size() == 1) pos.insert(pos.begin(), backend);
    if (pos.size() != 2) {
        c->print_help();
        g_exit = 2;
        throw std::runtime_error(c->name() + ": expected <backend> <pack_id>");
    }
    return {pos[0], parse_pack_id(pos[1])};
}

void run_duo_quarantine_list(const Cmd& c) {
    using namespace lights3;
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* duo = find_duostore(app, one_backend_arg(c));
    auto entries = duo->quarantine_list();
    if (entries.empty()) {
        std::printf("no quarantined packs\n");
    } else {
        std::printf("%-18s %-10s %-8s %-20s %s\n", "PACK", "LIVE_RECS", "CORRUPT",
                    "QUARANTINED", "PURGED");
        for (const auto& e : entries)
            std::printf("%016llx   %-10lld %-8llu %-20s %s\n",
                        static_cast<unsigned long long>(e.pack_id),
                        static_cast<long long>(e.live_recs),
                        static_cast<unsigned long long>(e.corrupt_records),
                        util::iso8601(std::chrono::system_clock::from_time_t(
                                          e.quarantined_ms / 1000))
                            .c_str(),
                        e.purged ? "yes" : "no");
    }
    app.shutdown();
}

void run_duo_quarantine_release(const Cmd& c) {
    using namespace lights3;
    auto [backend, pack_id] = backend_pack_args(c);
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* duo = find_duostore(app, backend);
    if (duo->quarantine_release(pack_id)) {
        LOG_INFO("duostore admin: pack {:016x} released from quarantine (compaction retries "
                 "next GC round)", pack_id);
    } else {
        LOG_WARN("duostore admin: pack {:016x} is not quarantined", pack_id);
        g_exit = 1;
    }
    app.shutdown();
}

void run_duo_quarantine_purge(const Cmd& c) {
    using namespace lights3;
    auto [backend, pack_id] = backend_pack_args(c);
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* duo = find_duostore(app, backend);
    if (sync_wait(duo->quarantine_purge(pack_id))) {
        LOG_WARN("duostore admin: quarantined pack {:016x} purged (data loss acknowledged); "
                 "delete the owning objects to drain its accounting", pack_id);
    } else {
        LOG_WARN("duostore admin: pack {:016x} not purged (not quarantined, or already purged)",
                 pack_id);
        g_exit = 1;
    }
    app.shutdown();
}

Cmd make_duo_quarantine() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "quarantine", "lights3 duostore quarantine list local",
        "lights3 duostore quarantine <list|release|purge> <backend> [<pack_id>] [--config=<path>]",
        "Corrupt-pack quarantine (docs/storage/duostore-core.md §8.6): packs whose "
        "compaction found corrupt records and made no progress for consecutive scans are "
        "parked here instead of retrying forever. list shows them; release drops an entry "
        "so compaction retries (use after restoring the pack file from backup); purge "
        "deletes the pack file, accepting the loss of its remaining records — the "
        "accounting drains as the owning objects are deleted.",
        "duostore corrupt-pack quarantine (list/release/purge)",
        [](const Cmd& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_backend_leaf(
        "list", "lights3 duostore quarantine list local",
        "lights3 duostore quarantine list <backend> [--config=<path>]",
        "Print every quarantined pack with its live/corrupt record counts and entry time.",
        "list quarantined packs", run_duo_quarantine_list));
    cmd->add_subcommand(make_backend_leaf(
        "release", "lights3 duostore quarantine release local 000000000000a001",
        "lights3 duostore quarantine release <backend> <pack_id> [--config=<path>]",
        "Drop the quarantine entry so the next GC round rescans the pack (it returns "
        "after three fruitless scans if the corruption persists).",
        "release a pack back to compaction", run_duo_quarantine_release));
    cmd->add_subcommand(make_backend_leaf(
        "purge", "lights3 duostore quarantine purge local 000000000000a001",
        "lights3 duostore quarantine purge <backend> <pack_id> [--config=<path>]",
        "Delete the quarantined pack's file, accepting the loss of its remaining "
        "records (their reads become missing-extent errors). Refused while an in-flight "
        "reader pins the pack. The liveness accounting is kept until the owning objects "
        "are deleted; GC then retires it.",
        "purge a quarantined pack from disk (data loss)", run_duo_quarantine_purge));
    return cmd;
}

Cmd make_admin_leaf(const char* name, const char* example, const char* usage, const char* help_long,
                    const char* help_short, void (*run)(const Cmd&)) {
    auto cmd = std::make_shared<ccmd::c_command>(name, example, usage, help_long, help_short, run);
    add_config_flag(cmd);
    cmd->var<std::string>("backend", "", "duostore backend name (alternative to the positional)");
    cmd->var<std::string>("file", "", "dump file path (alternative to the positional)");
    return cmd;
}

Cmd make_duostore() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "duostore", "lights3 duostore dump local meta.dump --config=config/lights3.yaml",
        "lights3 duostore <dump|load|gc|scan|quarantine> <backend> [<file>|<pack_id>] "
        "[--config=<path>]",
        "DuoStore admin: meta dump/load (docs/storage/duostore-core.md §11), on-demand "
        "GC / orphan-scan rounds (§8), and the corrupt-pack quarantine (§8.1). All run "
        "with the backends built but no server listening, then exit; load ends with a "
        "forced orphan scan. Backup order: copy the data dir first, then dump meta "
        "(online-consistent on rocksdb/sqlite/tikv; stop writes on redis); restore data "
        "first, then load.",
        "duostore admin (dump/load/gc/scan/quarantine)",
        [](const Cmd& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_admin_leaf(
        "dump", "lights3 duostore dump local meta.dump",
        "lights3 duostore dump <backend> <file> [--config=<path>]",
        "Write the backend's full meta (buckets, objects, sealed packs) to <file>.",
        "dump duostore meta to a file", run_dump));
    cmd->add_subcommand(make_admin_leaf(
        "load", "lights3 duostore load local meta.dump",
        "lights3 duostore load <backend> <file> [--config=<path>]",
        "Replay a meta dump from <file> into the backend, then run an orphan scan.",
        "load duostore meta from a file", run_load));
    cmd->add_subcommand(make_backend_leaf(
        "gc", "lights3 duostore gc local",
        "lights3 duostore gc <backend> [--config=<path>]",
        "Run one GC round now (docs/storage/duostore-core.md §8.1): mpu_ttl expiry "
        "cleanup, gcq consumption, aged-pack sealing + compaction, whole-empty-pack "
        "deletion. Same round the background worker runs on its timer.",
        "run one duostore GC round", run_duo_gc));
    cmd->add_subcommand(make_backend_leaf(
        "scan", "lights3 duostore scan local",
        "lights3 duostore scan <backend> [--config=<path>]",
        "Run one orphan-scan round now (docs/storage/duostore-core.md §8.3): two-way "
        "reconciliation of on-disk chunks/packs against refs/packstat; unreferenced "
        "residue beyond gc_grace is unlinked, loss signals are warned and counted.",
        "run one duostore orphan-scan round", run_duo_scan));
    cmd->add_subcommand(make_duo_quarantine());
    return cmd;
}
#endif  // LIGHTS3_DUOSTORE

// Tiered background tasks on demand (roadmap §3.2; same offline pattern —
// tiered sits over localfs + a cloud backend, no exclusive lock issue, but the
// coldness verdict is atime-based and an offline run sees its own process's
// access table, so `tier scan` demotes by mtime/atime as recorded on disk)
lights3::storage::TieredBackend* find_tiered(lights3::Application& app, const std::string& name) {
    const auto& backends = app.backends();
    auto it = backends.find(name);
    if (it == backends.end()) throw std::runtime_error("tier: no backend named '" + name + "'");
    auto* t = dynamic_cast<lights3::storage::TieredBackend*>(it->second.get());
    if (!t) throw std::runtime_error("tier: backend '" + name + "' is not tiered");
    return t;
}

void run_tier_scan(const Cmd& c) {
    using namespace lights3;
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* t = find_tiered(app, one_backend_arg(c));
    auto st = sync_wait(t->scan_once());  // demotions/reclaim are logged by the scan itself
    LOG_INFO("tier admin: {} scan round complete: {} {}, cold {}, recovered {}, enrolled {}, "
             "evicted {} ({} bytes), watermark short by {}",
             st.full ? "full" : "incremental", st.walked,
             st.full ? "objects walked" : "wheel candidates", st.cold_picked, st.recovered,
             st.enrolled, st.evicted, st.evicted_bytes, st.need_remaining);
    app.shutdown();
}

void run_tier_gc(const Cmd& c) {
    using namespace lights3;
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* t = find_tiered(app, one_backend_arg(c));
    auto st = sync_wait(t->run_gc_once());
    LOG_INFO("tier admin: gc round: resolved {} (cloud replicas removed {}), deferred {}, "
             "failed {}",
             st.resolved, st.removed_cloud, st.deferred, st.failed);
    app.shutdown();
}

void run_tier_reconcile(const Cmd& c) {
    using namespace lights3;
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* t = find_tiered(app, one_backend_arg(c));
    auto st = sync_wait(t->run_reconcile_once());
    LOG_INFO("tier admin: reconcile round: {} cloud objects walked; stubs rebuilt {}, "
             "orphans deleted {}, orphans skipped {}, refs missing {}; quarantine new {}, "
             "resolved {}",
             st.cloud_objects, st.stubs_rebuilt, st.orphans_deleted, st.orphans_skipped,
             st.refs_missing, st.quarantined_new, st.quarantined_resolved);
    app.shutdown();
}

// Quarantine ledger (roadmap §3.6 ④): list / forget / purge. forget and purge take
// `<backend> <bucket> <key>` positionals (or --backend= plus two positionals)
std::tuple<std::string, std::string, std::string> backend_key_args(const Cmd& c) {
    std::string backend = c->var<std::string>("backend");
    std::vector<std::string> pos = c->args();
    if (!backend.empty() && pos.size() == 2) pos.insert(pos.begin(), backend);
    if (pos.size() != 3) {
        c->print_help();
        g_exit = 2;
        throw std::runtime_error(c->name() + ": expected <backend> <bucket> <key>");
    }
    return {pos[0], pos[1], pos[2]};
}

void run_tier_quarantine_list(const Cmd& c) {
    using namespace lights3;
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* t = find_tiered(app, one_backend_arg(c));
    auto entries = t->quarantine_list();
    if (entries.empty()) {
        std::printf("no quarantined findings\n");
    } else {
        std::printf("%-13s %-20s %-40s %-34s %-20s %-20s %s\n", "KIND", "BUCKET", "KEY", "ETAG",
                    "FIRST_SEEN", "LAST_SEEN", "COUNT");
        for (const auto& e : entries)
            std::printf("%-13s %-20s %-40s %-34s %-20s %-20s %llu\n", e.kind.c_str(),
                        e.bucket.c_str(), e.key.c_str(), e.etag.c_str(),
                        util::iso8601(std::chrono::system_clock::from_time_t(e.first_seen)).c_str(),
                        util::iso8601(std::chrono::system_clock::from_time_t(e.last_seen)).c_str(),
                        static_cast<unsigned long long>(e.count));
    }
    app.shutdown();
}

void run_tier_quarantine_forget(const Cmd& c) {
    using namespace lights3;
    auto [backend, bucket, key] = backend_key_args(c);
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* t = find_tiered(app, backend);
    if (t->quarantine_forget(bucket, key)) {
        LOG_INFO("tier admin: quarantine entry {}/{} forgotten", bucket, key);
    } else {
        LOG_WARN("tier admin: {}/{} is not quarantined", bucket, key);
        g_exit = 1;
    }
    app.shutdown();
}

void run_tier_quarantine_purge(const Cmd& c) {
    using namespace lights3;
    auto [backend, bucket, key] = backend_key_args(c);
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* t = find_tiered(app, backend);
    if (sync_wait(t->quarantine_purge(bucket, key))) {
        LOG_WARN("tier admin: dead stub {}/{} purged (data loss acknowledged)", bucket, key);
    } else {
        LOG_WARN("tier admin: {}/{} not purged (not a refs_missing finding, or the cloud copy "
                 "is back — entry dropped)",
                 bucket, key);
        g_exit = 1;
    }
    app.shutdown();
}

Cmd make_tier_quarantine() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "quarantine", "lights3 tier quarantine list tierdata",
        "lights3 tier quarantine <list|forget|purge> <backend> [<bucket> <key>] [--config=<path>]",
        "Reconciliation quarantine ledger (docs/tiered-storage.md §9): findings that "
        "repeat every round (stub whose cloud copy is gone; foreign cloud object without "
        "lights3 headers) are recorded once and stop re-alerting. list shows them; forget "
        "drops an entry without touching data; purge deletes the dead local stub of a "
        "refs_missing finding after re-verifying the cloud copy is still gone.",
        "tiered quarantine ledger (list/forget/purge)",
        [](const Cmd& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_backend_leaf(
        "list", "lights3 tier quarantine list tierdata",
        "lights3 tier quarantine list <backend> [--config=<path>]",
        "Print every quarantined finding with its first/last sighting and repeat count.",
        "list quarantined findings", run_tier_quarantine_list));
    cmd->add_subcommand(make_backend_leaf(
        "forget", "lights3 tier quarantine forget tierdata archive photos/2024/a.jpg",
        "lights3 tier quarantine forget <backend> <bucket> <key> [--config=<path>]",
        "Drop the entry for <bucket>/<key> (all kinds) without touching any data; it "
        "comes back on the next reconcile round if the finding still reproduces.",
        "drop a quarantine entry", run_tier_quarantine_forget));
    cmd->add_subcommand(make_backend_leaf(
        "purge", "lights3 tier quarantine purge tierdata archive photos/2024/a.jpg",
        "lights3 tier quarantine purge <backend> <bucket> <key> [--config=<path>]",
        "Resolve a refs_missing finding by deleting the dead local stub — the object "
        "disappears from listings. Re-verifies with a HEAD first: if the cloud copy is back "
        "the stub is kept and the entry dropped instead.",
        "delete the dead stub of a refs_missing finding", run_tier_quarantine_purge));
    return cmd;
}

Cmd make_tier() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "tier", "lights3 tier scan tierdata --config=config/lights3.yaml",
        "lights3 tier <scan|gc|reconcile|quarantine> <backend> [--config=<path>]",
        "Tiered-storage background tasks on demand (docs/tiered-storage.md §9): the "
        "same rounds the timers run, for operators who cannot wait for the next tick. "
        "Runs with the backends built but no server listening, then exits.",
        "tiered background tasks (scan/gc/reconcile) + quarantine ledger",
        [](const Cmd& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_backend_leaf(
        "scan", "lights3 tier scan tierdata",
        "lights3 tier scan <backend> [--config=<path>]",
        "Run one scan round now: coldness detection + space-watermark reclamation + "
        "crash recovery + atime snapshot.",
        "run one tiered scan round", run_tier_scan));
    cmd->add_subcommand(make_backend_leaf(
        "gc", "lights3 tier gc tierdata",
        "lights3 tier gc <backend> [--config=<path>]",
        "Consume one round of the tiered GC queue now: delete orphan cloud replicas "
        "(etag-verified); failures reschedule with their persisted backoff.",
        "run one tiered GC round", run_tier_gc));
    cmd->add_subcommand(make_backend_leaf(
        "reconcile", "lights3 tier reconcile tierdata",
        "lights3 tier reconcile <backend> [--config=<path>]",
        "Run one bidirectional local/cloud reconciliation now: rebuild missing stubs "
        "from redundant headers (or delete orphans when configured); local-remote with "
        "cloud missing warns and never deletes the stub.",
        "run one tiered reconcile round", run_tier_reconcile));
    cmd->add_subcommand(make_tier_quarantine());
    return cmd;
}

// Offline integrity scrub (roadmap §3.1): backends built, no server listening —
// same shape as duostore dump/load. Dispatches on the backend's concrete type:
// duostore gets the deep manifest/crc/refs scrub, localfs/xlocalfs the ETag
// full-verify. Findings set exit code 1 (fsck convention); warning-grade
// counters (refs_stale, unverifiable, orphan sidecars) do not — they are logged
// and can be transient or expected on legacy data
void run_fsck(const Cmd& c) {
    using namespace lights3;
    std::string backend = one_backend_arg(c);
    int mbps = c->var<int>("max-mbps");
    if (mbps < 0) {
        g_exit = 2;
        throw std::runtime_error("fsck: --max-mbps must be >= 0");
    }
    const uint64_t bps = uint64_t(mbps) * 1000 * 1000;

    Application app(c->var<std::string>("config"));
    app.open_storage();
    const auto& backends = app.backends();
    auto it = backends.find(backend);
    if (it == backends.end()) throw std::runtime_error("fsck: no backend named '" + backend + "'");
    uint64_t findings = 0;
    bool aborted = false;
#ifdef LIGHTS3_DUOSTORE
    if (auto* duo = dynamic_cast<storage::DuoStoreBackend*>(it->second.get())) {
        storage::duostore::DuoScrubOptions opt;
        opt.max_bytes_per_sec = bps;
        auto st = sync_wait(duo->run_scrub_once(opt));
        LOG_INFO("fsck '{}': {} objects / {} parts / {} extents / {} bytes read; corrupt {}, "
                 "unreadable {}, refs missing {}, refs stale {}, meta errors {}",
                 backend, st.objects_scanned, st.parts_scanned, st.extents_checked,
                 st.bytes_read, st.corrupt_extents, st.unreadable_extents, st.refs_missing,
                 st.refs_stale, st.meta_errors);
        findings =
            st.corrupt_extents + st.unreadable_extents + st.refs_missing + st.meta_errors;
        aborted = st.aborted;
    } else
#endif
        if (auto* lfs = dynamic_cast<storage::LocalFsBackend*>(it->second.get())) {
        storage::FsScrubOptions opt;
        opt.max_bytes_per_sec = bps;
        auto st = sync_wait(lfs->run_scrub_once(opt));
        LOG_INFO("fsck '{}': {} objects / {} bytes read; mismatches {}, read errors {}, "
                 "unverifiable {}, stubs skipped {}, races skipped {}, orphan sidecars {}",
                 backend, st.objects_scanned, st.bytes_read, st.etag_mismatches,
                 st.read_errors, st.unverifiable, st.skipped_stubs, st.skipped_races,
                 st.orphan_sidecars);
        findings = st.etag_mismatches + st.read_errors;
        aborted = st.aborted;
    } else {
        throw std::runtime_error("fsck: backend '" + backend +
                                 "' does not support offline fsck (duostore/localfs/xlocalfs)");
    }
    app.shutdown();
    if (aborted) throw std::runtime_error("fsck: scrub aborted before completion");
    if (findings > 0) g_exit = 1;
}

Cmd make_fsck() {
    auto cmd = make_backend_leaf(
        "fsck", "lights3 fsck local --max-mbps=100 --config=config/lights3.yaml",
        "lights3 fsck <backend> [--max-mbps=<n>] [--config=<path>]",
        "Offline data-integrity scrub (read-only). duostore: read back every extent of "
        "every object and in-flight multipart part, recompute crc32c against the "
        "manifest, and reconcile the refs ledger both ways. localfs/xlocalfs: re-read "
        "every object and compare the recomputed MD5 with the stored ETag (multipart "
        "composites via the recorded part layout). Runs with the backends built but no "
        "server listening; exit code 1 when integrity findings exist.",
        "offline data-integrity scrub", run_fsck);
    cmd->var<int>("max-mbps", 0, "read throttle in MB/s (0 = unthrottled)");
    return cmd;
}

}  // namespace

int main(int argc, char** argv) {
    auto root = std::make_shared<ccmd::c_command>(
        "lights3", "lights3 --config=config/lights3.yaml", "lights3 [--config=<path>] | lights3 <command> ...",
        "S3-compatible object storage server. With no command the server starts and "
        "runs until SIGINT/SIGTERM; run `lights3 help <command>` for the admin commands.",
        "S3-compatible object storage server", run_server);
    add_config_flag(root);
    root->var<bool>("check-config", false,
                    "Parse and validate the config, print what it resolves to, exit 0/1 "
                    "without opening backends or binding a port (roadmap §6.2)");
#ifdef LIGHTS3_DUOSTORE
    root->add_subcommand(make_duostore());
#endif
    root->add_subcommand(make_tier());
    root->add_subcommand(make_fsck());

    try {
        root->execute(normalize_argv(argc, argv));
        return g_exit;
    } catch (const std::exception& e) {
        fprintf(stderr, "fatal: %s\n", e.what());
        return g_exit == 0 ? 1 : g_exit;
    }
}
