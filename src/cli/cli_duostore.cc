#include "cli/cli_duostore.h"

#include <charconv>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <fstream>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "core/log.h"
#include "core/util/time.h"
#include "storage/duostore/duostore_backend.h"
#include "storage/duostore/meta_backup.h"
#ifdef LIGHTS3_DUOSTORE_SQLITE_META
#include "storage/duostore/sqlite_meta_store.h"
#endif
#include "storage/duostore/rocks_meta_store.h"

namespace lights3_cli {

namespace {

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
    return find_backend_as<lights3::storage::DuoStoreBackend>(app, name, "duostore", "duostore");
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
    LOG_INFO("duostore admin: dumped {} buckets / {} objects / {} sealed packs to {}", st.buckets, st.objects,
             st.sealed_packs, a.file);
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
    LOG_INFO("duostore admin: loaded {} buckets / {} objects / {} sealed packs from {}", st.buckets, st.objects,
             st.sealed_packs, a.file);
    app.shutdown();
}

// ---- Backup chains / PITR (backlog-sequence ⑧, docs/storage/duostore-core.md §11.1) ----

// `<backend>` positional or --backend=; the directory comes from --to= / --from=
std::string backend_dir_args(const Cmd& c, const char* dir_flag, std::string& dir) {
    std::string backend = c->var<std::string>("backend");
    dir = c->var<std::string>(dir_flag);
    const auto& pos = c->args();
    if (pos.size() > 1) {
        g_exit = 2;
        throw std::runtime_error("duostore " + c->name() + ": too many arguments");
    }
    if (pos.size() == 1) backend = pos[0];
    if (backend.empty() || dir.empty()) {
        c->print_help();
        g_exit = 2;
        throw std::runtime_error("duostore " + c->name() + ": <backend> and --" + dir_flag + "=<dir> are required");
    }
    return backend;
}

void run_backup(const Cmd& c) {
    using namespace lights3;
    std::string dir;
    std::string backend = backend_dir_args(c, "to", dir);
    bool incremental = c->var<bool>("incremental");
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* duo = find_duostore(app, backend);
    auto e = sync_wait(duo->run_meta_backup(dir, incremental));
    LOG_INFO("duostore admin: backup entry {} ({}) of '{}' written to {}: {} bytes{}", e.id,
             e.full ? "full" : "incremental", backend, dir, e.bytes,
             e.marker.empty() ? std::string() : ", restore marker " + e.marker);
    if (!duo->meta_physical_backup())
        LOG_INFO(
            "duostore admin: {} meta keeps its incremental copies cluster-side -- to "
            "restore to a point in time, bring the {} back to marker {} first, then "
            "`duostore restore` this entry",
            duo->config().meta_kind_name(),
            duo->config().meta_kind_name() == std::string("redis") ? "AOF archive" : "cluster (BR --backupts)",
            e.marker);
    app.shutdown();
}

void run_restore(const Cmd& c) {
    using namespace lights3;
    using namespace lights3::storage::duostore;
    std::string dir;
    std::string backend = backend_dir_args(c, "from", dir);
    std::optional<uint64_t> to_id;
    std::optional<int64_t> to_ts;
    if (auto v = c->var<std::string>("to-id"); !v.empty()) to_id = std::stoull(v);
    if (auto v = c->var<std::string>("to-ts"); !v.empty()) {
        to_ts = parse_restore_ts(v);
        if (!to_ts) {
            g_exit = 2;
            throw std::runtime_error("duostore restore: --to-ts must be ISO 8601 or unix ms, got '" + v + "'");
        }
    }
    auto manifest = BackupManifest::load(dir);
    auto chain = manifest.plan(to_id, to_ts);
    const auto& last = chain.back();
    LOG_INFO(
        "duostore admin: restoring '{}' from {} ({} engine) to entry {} ({}) -- {} entries "
        "to replay",
        backend, dir, manifest.engine, last.id,
        util::iso8601(std::chrono::system_clock::time_point(std::chrono::milliseconds(last.ts_ms))), chain.size());

    // The backend's meta paths come from the config alone: a local engine is
    // restored at file level with nothing open, so the backends are built only
    // afterwards (and the forced orphan scan then runs on the restored meta)
    auto cfg = Config::load(c->var<std::string>("config"));
    const BackendConfig* bc = nullptr;
    for (auto& b : cfg.backends)
        if (b.name == backend) bc = &b;
    if (!bc) throw std::runtime_error("duostore: no backend named '" + backend + "'");
    if (bc->type != "duostore") throw std::runtime_error("duostore: backend '" + backend + "' is not duostore");
    auto duo_cfg = storage::DuoStoreConfig::from_params(bc->name, bc->params);
    if (manifest.engine != duo_cfg.meta_kind_name())
        throw std::runtime_error("duostore restore: " + dir + " holds a " + manifest.engine + " chain, backend '" +
                                 backend + "' uses " + duo_cfg.meta_kind_name());
    bool physical = false;
    if (manifest.engine == "sqlite") {
#ifdef LIGHTS3_DUOSTORE_SQLITE_META
        SqliteMetaStore::restore_physical(dir, chain, duo_cfg.sqlite_path);
        physical = true;
#else
        throw std::runtime_error("duostore restore: sqlite meta is not compiled in");
#endif
    } else if (manifest.engine == "rocksdb") {
        RocksMetaStore::restore_physical(dir, last.marker, duo_cfg.meta_path);
        physical = true;
    }
    if (physical)
        LOG_INFO(
            "duostore admin: meta files of '{}' restored to entry {}; opening the backend for "
            "the forced orphan scan",
            backend, last.id);
    else
        LOG_INFO(
            "duostore admin: {} meta: the cluster must already be at marker {} (entry {}); "
            "loading the logical dump {}",
            manifest.engine, last.marker, last.id, last.file);

    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* duo = find_duostore(app, backend);
    if (physical) {
        auto st = sync_wait(duo->run_orphan_scan_once());
        LOG_INFO(
            "duostore admin: restore done; orphan scan: {} chunks / {} packs reclaimed, "
            "{} refs missing",
            st.orphans_removed, st.orphan_packs_removed, st.refs_missing);
    } else {
        std::ifstream f(dir + "/" + last.file, std::ios::binary);
        if (!f) throw std::runtime_error("duostore restore: cannot open " + dir + "/" + last.file);
        auto st = sync_wait(duo->run_meta_load(f));
        LOG_INFO("duostore admin: restore done: loaded {} buckets / {} objects / {} sealed packs", st.buckets,
                 st.objects, st.sealed_packs);
    }
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
    LOG_INFO(
        "duostore admin: gc round: reclaims {} (grace-skipped {}, pinned {}, "
        "leased {}), files removed {}, packs removed {}, uploads expired {}, packs "
        "sealed-aged {}, compacted {} (deferred {}), records migrated {}, corrupt {}, "
        "packs quarantined {}",
        st.reclaims_acked, st.skipped_grace, st.skipped_pinned, st.skipped_leased, st.files_removed, st.packs_removed,
        st.uploads_expired, st.packs_sealed_aged, st.packs_compacted, st.packs_compact_deferred, st.records_migrated,
        st.records_corrupt, st.packs_quarantined);
    app.shutdown();
}

void run_duo_scan(const Cmd& c) {
    using namespace lights3;
    Application app(c->var<std::string>("config"));
    app.open_storage();
    auto* duo = find_duostore(app, one_backend_arg(c));
    auto st = sync_wait(duo->run_orphan_scan_once());
    LOG_INFO(
        "duostore admin: orphan scan: {} chunks ({} bytes) / {} packs ({} bytes) "
        "scanned; orphans removed {} (grace-skipped {}, pinned {}, gcq-pending {}, "
        "write-leased {}), orphan packs removed {} (skipped active {}); refs missing "
        "{}, packstats missing {}",
        st.chunks_scanned, st.chunk_bytes, st.packs_scanned, st.pack_bytes, st.orphans_removed, st.skipped_grace,
        st.skipped_pinned, st.skipped_gcq, st.skipped_leased, st.orphan_packs_removed, st.packs_skipped_active,
        st.refs_missing, st.pack_stats_missing);
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
        // the {:016x} form GC logs and `quarantine list` print
        base = 16;
    }
    auto res = std::from_chars(s.data() + off, s.data() + s.size(), id, base);
    if (res.ec != std::errc{} || res.ptr != s.data() + s.size())
        throw std::runtime_error("invalid pack id '" + s + "' (16-digit hex as logged, 0x-prefixed hex, or decimal)");
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
        std::printf("%-18s %-10s %-8s %-20s %s\n", "PACK", "LIVE_RECS", "CORRUPT", "QUARANTINED", "PURGED");
        for (const auto& e : entries)
            std::printf("%016llx   %-10lld %-8llu %-20s %s\n", static_cast<unsigned long long>(e.pack_id),
                        static_cast<long long>(e.live_recs), static_cast<unsigned long long>(e.corrupt_records),
                        util::iso8601(std::chrono::system_clock::from_time_t(e.quarantined_ms / 1000)).c_str(),
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
        LOG_INFO(
            "duostore admin: pack {:016x} released from quarantine (compaction retries "
            "next GC round)",
            pack_id);
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
        LOG_WARN(
            "duostore admin: quarantined pack {:016x} purged (data loss acknowledged); "
            "delete the owning objects to drain its accounting",
            pack_id);
    } else {
        LOG_WARN("duostore admin: pack {:016x} not purged (not quarantined, or already purged)", pack_id);
        g_exit = 1;
    }
    app.shutdown();
}

Cmd make_duo_quarantine() {
    auto cmd = make_group("quarantine", "lights3 duostore quarantine list local",
                          "lights3 duostore quarantine <list|release|purge> <backend> [<pack_id>] [--config=<path>]",
                          "Corrupt-pack quarantine (docs/storage/duostore-core.md §8.6): packs whose "
                          "compaction found corrupt records and made no progress for consecutive scans are "
                          "parked here instead of retrying forever. list shows them; release drops an entry "
                          "so compaction retries (use after restoring the pack file from backup); purge "
                          "deletes the pack file, accepting the loss of its remaining records — the "
                          "accounting drains as the owning objects are deleted.",
                          "duostore corrupt-pack quarantine (list/release/purge)");
    cmd->add_subcommand(
        make_backend_leaf("list", "lights3 duostore quarantine list local",
                          "lights3 duostore quarantine list <backend> [--config=<path>]",
                          "Print every quarantined pack with its live/corrupt record counts and entry time.",
                          "list quarantined packs", run_duo_quarantine_list));
    cmd->add_subcommand(make_backend_leaf("release", "lights3 duostore quarantine release local 000000000000a001",
                                          "lights3 duostore quarantine release <backend> <pack_id> [--config=<path>]",
                                          "Drop the quarantine entry so the next GC round rescans the pack (it returns "
                                          "after three fruitless scans if the corruption persists).",
                                          "release a pack back to compaction", run_duo_quarantine_release));
    cmd->add_subcommand(
        make_backend_leaf("purge", "lights3 duostore quarantine purge local 000000000000a001",
                          "lights3 duostore quarantine purge <backend> <pack_id> [--config=<path>]",
                          "Delete the quarantined pack's file, accepting the loss of its remaining "
                          "records (their reads become missing-extent errors). Refused while an in-flight "
                          "reader pins the pack. The liveness accounting is kept until the owning objects "
                          "are deleted; GC then retires it.",
                          "purge a quarantined pack from disk (data loss)", run_duo_quarantine_purge));
    return cmd;
}

// Leaf taking `<backend> <file>` (+ --config), i.e. dump / load
Cmd make_admin_leaf(const char* name, const char* example, const char* usage, const char* help_long,
                    const char* help_short, void (*run)(const Cmd&)) {
    auto cmd = std::make_shared<ccmd::c_command>(name, example, usage, help_long, help_short, run);
    add_config_flag(cmd);
    cmd->var<std::string>("backend", "", "duostore backend name (alternative to the positional)");
    cmd->var<std::string>("file", "", "dump file path (alternative to the positional)");
    return cmd;
}

}  // namespace

Cmd make_duostore() {
    auto cmd = make_group("duostore", "lights3 duostore dump local meta.dump --config=config/lights3.yaml",
                          "lights3 duostore <dump|load|backup|restore|gc|scan|quarantine> <backend> [<file>|<pack_id>] "
                          "[--config=<path>]",
                          "DuoStore admin: meta dump/load (docs/storage/duostore-core.md §11), backup chains "
                          "with point-in-time restore (§11.1), on-demand "
                          "GC / orphan-scan rounds (§8), and the corrupt-pack quarantine (§8.1). All run "
                          "with the backends built but no server listening, then exit; load ends with a "
                          "forced orphan scan. Backup order: copy the data dir first, then dump meta "
                          "(online-consistent on rocksdb/sqlite/tikv; stop writes on redis); restore data "
                          "first, then load.",
                          "duostore admin (dump/load/gc/scan/quarantine)");
    cmd->add_subcommand(make_admin_leaf("dump", "lights3 duostore dump local meta.dump",
                                        "lights3 duostore dump <backend> <file> [--config=<path>]",
                                        "Write the backend's full meta (buckets, objects, sealed packs) to <file>.",
                                        "dump duostore meta to a file", run_dump));
    cmd->add_subcommand(make_admin_leaf("load", "lights3 duostore load local meta.dump",
                                        "lights3 duostore load <backend> <file> [--config=<path>]",
                                        "Replay a meta dump from <file> into the backend, then run an orphan scan.",
                                        "load duostore meta from a file", run_load));
    {
        auto bk = std::make_shared<ccmd::c_command>(
            "backup", "lights3 duostore backup local --to=/backup/local-meta --incremental",
            "lights3 duostore backup <backend> --to=<dir> [--incremental] [--config=<path>]",
            "Append one entry to the meta backup chain in <dir> (docs/storage/duostore-core.md "
            "§11.1). sqlite: a full copy, or with --incremental the WAL segment since the "
            "previous entry (needs sqlite_wal_archive pointing at <dir>); rocksdb: a "
            "BackupEngine backup (incremental by construction, every entry restores on its "
            "own); redis / tikv: a logical dump plus the restore marker (replication offset / "
            "TSO) for the cluster-side archive, --incremental is refused. Local engines need "
            "the server stopped (file lock).",
            "append an entry to the meta backup chain", run_backup);
        add_config_flag(bk);
        bk->var<std::string>("backend", "", "duostore backend name (alternative to the positional)");
        bk->var<std::string>("to", "", "backup chain directory");
        bk->var<bool>("incremental", false, "delta since the previous entry instead of a full copy");
        cmd->add_subcommand(bk);
        auto rs = std::make_shared<ccmd::c_command>(
            "restore", "lights3 duostore restore local --from=/backup/local-meta --to-ts=2026-09-06T12:00:00Z",
            "lights3 duostore restore <backend> --from=<dir> [--to-id=<n> | --to-ts=<iso8601>] "
            "[--config=<path>]",
            "Restore the backend's meta from the chain in <dir>: every entry up to --to-id / "
            "--to-ts (default: the latest). sqlite / rocksdb: file-level restore of the "
            "closed meta, then the backend opens and a forced orphan scan reconciles the "
            "data side; redis / tikv: the cluster must already be at the entry's marker, "
            "then the logical dump is loaded (writes stopped). Put the data directory "
            "back first (§11 order).",
            "restore meta from a backup chain (point in time)", run_restore);
        add_config_flag(rs);
        rs->var<std::string>("backend", "", "duostore backend name (alternative to the positional)");
        rs->var<std::string>("from", "", "backup chain directory");
        rs->var<std::string>("to-id", "", "restore through this manifest entry id");
        rs->var<std::string>("to-ts", "",
                             "restore through the last entry at or before this time (ISO 8601 or unix ms)");
        cmd->add_subcommand(rs);
    }
    cmd->add_subcommand(make_backend_leaf("gc", "lights3 duostore gc local",
                                          "lights3 duostore gc <backend> [--config=<path>]",
                                          "Run one GC round now (docs/storage/duostore-core.md §8.1): mpu_ttl expiry "
                                          "cleanup, gcq consumption, aged-pack sealing + compaction, whole-empty-pack "
                                          "deletion. Same round the background worker runs on its timer.",
                                          "run one duostore GC round", run_duo_gc));
    cmd->add_subcommand(make_backend_leaf("scan", "lights3 duostore scan local",
                                          "lights3 duostore scan <backend> [--config=<path>]",
                                          "Run one orphan-scan round now (docs/storage/duostore-core.md §8.3): two-way "
                                          "reconciliation of on-disk chunks/packs against refs/packstat; unreferenced "
                                          "residue beyond gc_grace is unlinked, loss signals are warned and counted.",
                                          "run one duostore orphan-scan round", run_duo_scan));
    cmd->add_subcommand(make_duo_quarantine());
    return cmd;
}

}  // namespace lights3_cli
