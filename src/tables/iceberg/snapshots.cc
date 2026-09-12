#include "tables/iceberg/snapshots.h"

#include <map>
#include <optional>
#include <set>

#include "core/log.h"
#include "s3/errors.h"
#include "tables/iceberg/avro_reader.h"
#include "tables/iceberg/manifest.h"
#include "tables/identifier.h"
#include "tables/rest_error.h"

namespace lights3::tables::iceberg {

namespace {

bool is_missing(const s3::S3Error& e) {
    return e.code == s3::S3ErrorCode::NoSuchKey || e.code == s3::S3ErrorCode::NoSuchBucket;
}

// bucket-relative key of a referenced path, refusing foreign buckets and the reserved prefix
std::string checked_key(const SnapshotCheckContext& ctx, const std::string& path) {
    std::string key = path_to_key(ctx.bucket, path);
    if (key.rfind(ctx.reserved_prefix, 0) == 0)
        throw commit_failed("referenced file '" + path + "' lies under the reserved catalog prefix");
    return key;
}

Task<void> require_object(const SnapshotCheckContext& ctx, const std::string& path) {
    std::string key = checked_key(ctx, path);
    bool exists = true;
    try {
        co_await ctx.backend.head_object(ctx.bucket, key);
    } catch (const s3::S3Error& e) {
        if (!is_missing(e)) throw;
        exists = false;
    }
    if (!exists) throw commit_failed("referenced file '" + path + "' does not exist");
}

struct Blob {
    std::string bytes;
    uint64_t size = 0;
};

// The whole object (bounded); missing → 409
Task<Blob> read_all(const SnapshotCheckContext& ctx, const std::string& path, const std::string& key, size_t max,
                    const char* what) {
    storage::ObjectStream stream;
    bool missing = false;
    try {
        stream = co_await ctx.backend.get_object(ctx.bucket, key, std::nullopt);
    } catch (const s3::S3Error& e) {
        if (!is_missing(e)) throw;
        missing = true;
    }
    if (missing) throw commit_failed(std::string(what) + " '" + path + "' does not exist");
    Blob b;
    b.size = stream.meta.size;
    if (b.size > max) throw commit_failed(std::string(what) + " '" + path + "' exceeds the size limit");
    std::byte buf[64 * 1024];
    for (;;) {
        size_t n = co_await stream.body->read(std::span(buf));
        if (n == 0) break;
        if (b.bytes.size() + n > max) throw commit_failed(std::string(what) + " '" + path + "' exceeds the size limit");
        b.bytes.append(reinterpret_cast<const char*>(buf), n);
    }
    co_return b;
}

// One manifest entry with the manifest it came from
struct Entry {
    DataFile file;
    int64_t manifest_added_snapshot = 0;
    int manifest_content = 0;
};

struct ManifestRead {
    std::vector<Entry> entries;
    bool unsupported = false;
};

Task<ManifestRead> read_manifest(const SnapshotCheckContext& ctx, ManifestFile m, const DeepCheckOptions& opt) {
    std::string key = checked_key(ctx, m.path);
    Blob b = co_await read_all(ctx, m.path, key, opt.max_avro, "manifest");
    if (m.length > 0 && b.size != static_cast<uint64_t>(m.length))
        throw commit_failed("manifest '" + m.path + "' is " + std::to_string(b.size) +
                            " bytes but the manifest list records " + std::to_string(m.length));
    ManifestRead out;
    avro::Reader reader(b.bytes);
    std::vector<DataFile> files;
    try {
        files = parse_manifest(reader, opt.max_files, m.sequence_number);
    } catch (const avro::UnsupportedCodec& e) {
        LOG_WARN("tables: manifest {} skipped: {}", m.path, e.what());
        out.unsupported = true;
        co_return out;
    }
    out.entries.reserve(files.size());
    for (auto& f : files) out.entries.push_back(Entry{std::move(f), m.added_snapshot_id, m.content});
    co_return out;
}

struct ListRead {
    std::vector<ManifestFile> manifests;
    bool unsupported = false;
};

// (coroutine parameters that could be temporaries are taken by value: the task is lazy)
Task<ListRead> read_manifest_list(const SnapshotCheckContext& ctx, std::string path, const DeepCheckOptions& opt) {
    std::string key = checked_key(ctx, path);
    Blob b = co_await read_all(ctx, path, key, opt.max_avro, "manifest list");
    ListRead out;
    avro::Reader reader(b.bytes);
    try {
        out.manifests = parse_manifest_list(reader, opt.max_manifests);
    } catch (const avro::UnsupportedCodec& e) {
        LOG_WARN("tables: manifest list {} skipped: {}", path, e.what());
        out.unsupported = true;
    }
    co_return out;
}

template <class T>
Task<std::vector<T>> run_batched(std::vector<Task<T>> tasks, int concurrency) {
    std::vector<T> out;
    out.reserve(tasks.size());
    size_t i = 0;
    size_t batch = concurrency < 1 ? 1 : static_cast<size_t>(concurrency);
    while (i < tasks.size()) {
        std::vector<Task<T>> chunk;
        for (; i < tasks.size() && chunk.size() < batch; ++i) chunk.push_back(std::move(tasks[i]));
        auto res = co_await when_all(std::move(chunk));
        for (auto& r : res) out.push_back(std::move(r));
    }
    co_return out;
}

// The manifests a snapshot names: its manifest list (v2 / v1 with a list) or the v1
// inline "manifests" array (lengths unknown → 0)
struct SnapshotManifests {
    std::vector<ManifestFile> manifests;
    bool unsupported = false;
};

Task<SnapshotManifests> manifests_of(const SnapshotCheckContext& ctx, const Json& s, const DeepCheckOptions& opt) {
    SnapshotManifests out;
    if (s.contains("manifest-list") && s["manifest-list"].is_string()) {
        ListRead lr = co_await read_manifest_list(ctx, s["manifest-list"].get<std::string>(), opt);
        out.manifests = std::move(lr.manifests);
        out.unsupported = lr.unsupported;
    } else if (s.contains("manifests") && s["manifests"].is_array()) {
        for (auto& m : s["manifests"]) {
            if (!m.is_string()) throw bad_request("snapshot manifests must be strings");
            ManifestFile mf;
            mf.path = m.get<std::string>();
            mf.added_snapshot_id = s.value("snapshot-id", int64_t(0));
            out.manifests.push_back(std::move(mf));
        }
    }
    if (out.manifests.size() > opt.max_manifests)
        throw commit_failed("snapshot references more than " + std::to_string(opt.max_manifests) + " manifests");
    co_return out;
}

// Every entry of every manifest of the snapshot; nullopt when a codec could not be read
Task<std::optional<std::vector<Entry>>> entries_of(const SnapshotCheckContext& ctx,
                                                   const std::vector<ManifestFile>& manifests,
                                                   const DeepCheckOptions& opt, size_t& total_files) {
    std::vector<Task<ManifestRead>> tasks;
    for (auto& m : manifests) tasks.push_back(read_manifest(ctx, m, opt));
    auto reads = co_await run_batched(std::move(tasks), opt.concurrency);
    std::vector<Entry> all;
    for (auto& r : reads) {
        if (r.unsupported) co_return std::nullopt;
        for (auto& e : r.entries) {
            if (e.file.status != 2) ++total_files;
            if (total_files > opt.max_files)
                throw commit_failed("snapshot references more than " + std::to_string(opt.max_files) + " files");
            all.push_back(std::move(e));
        }
    }
    co_return all;
}

Task<int> check_data_file(const SnapshotCheckContext& ctx, const DataFile& f) {
    std::string key = checked_key(ctx, f.path);
    std::optional<storage::ObjectMeta> meta;
    try {
        meta = co_await ctx.backend.head_object(ctx.bucket, key);
    } catch (const s3::S3Error& e) {
        if (!is_missing(e)) throw;
    }
    if (!meta) throw commit_failed("data file '" + f.path + "' does not exist");
    if (f.size_bytes > 0 && meta->size != static_cast<uint64_t>(f.size_bytes))
        throw commit_failed("data file size mismatch: '" + f.path + "' is " + std::to_string(meta->size) +
                            " bytes but the manifest records " + std::to_string(f.size_bytes));
    co_return 0;
}

// Statistics / partition-statistics files: exist and start with the format magic
Task<int> check_magic(const SnapshotCheckContext& ctx, std::string path, const char* magic, const char* what) {
    std::string key = checked_key(ctx, path);
    storage::ObjectStream stream;
    bool missing = false;
    try {
        stream = co_await ctx.backend.get_object(ctx.bucket, key, storage::ByteRange{0, 3});
    } catch (const s3::S3Error& e) {
        if (!is_missing(e)) throw;
        missing = true;
    }
    if (missing) throw commit_failed(std::string(what) + " '" + path + "' does not exist");
    std::string head;
    std::byte buf[16];
    while (head.size() < 4) {
        size_t n = co_await stream.body->read(std::span(buf));
        if (n == 0) break;
        head.append(reinterpret_cast<const char*>(buf), n);
    }
    if (head.size() < 4 || head.compare(0, 4, magic) != 0)
        throw commit_failed(std::string(what) + " '" + path + "' is not a " + magic + " file");
    co_return 0;
}

bool attributed_to(const Entry& e, int64_t snapshot_id) {
    return e.file.snapshot_id ? *e.file.snapshot_id == snapshot_id : e.manifest_added_snapshot == snapshot_id;
}

}  // namespace

Task<void> check_new_snapshots_shallow(const SnapshotCheckContext& ctx, const Json& current, const Json& next) {
    std::set<int64_t> known;
    if (current.contains("snapshots"))
        for (auto& s : current["snapshots"]) known.insert(s["snapshot-id"].get<int64_t>());
    for (auto& s : next["snapshots"]) {
        if (known.count(s["snapshot-id"].get<int64_t>())) continue;
        if (s.contains("manifest-list") && s["manifest-list"].is_string()) {
            co_await require_object(ctx, s["manifest-list"].get<std::string>());
        } else if (s.contains("manifests") && s["manifests"].is_array()) {
            for (auto& m : s["manifests"]) {
                if (!m.is_string()) throw bad_request("snapshot manifests must be strings");
                co_await require_object(ctx, m.get<std::string>());
            }
        }
    }
}

Task<DeepCheckReport> check_new_snapshots_deep(const SnapshotCheckContext& ctx, const Json& current, const Json& next,
                                               const DeepCheckOptions& opt) {
    DeepCheckReport report;
    std::set<int64_t> known;
    if (current.contains("snapshots"))
        for (auto& s : current["snapshots"]) known.insert(s["snapshot-id"].get<int64_t>());
    // live file sets of parent snapshots, computed at most once per commit
    std::map<int64_t, std::optional<std::set<std::string>>> live_cache;
    std::set<int64_t> fresh;
    size_t total_files = 0;
    for (auto& s : next["snapshots"]) {
        int64_t sid = s["snapshot-id"].get<int64_t>();
        if (known.count(sid)) continue;
        fresh.insert(sid);
        SnapshotManifests sm = co_await manifests_of(ctx, s, opt);
        if (sm.unsupported) {
            if (!opt.allow_unsupported_codec)
                throw commit_failed("snapshot " + std::to_string(sid) + " uses an Avro codec this build cannot read");
            report.skipped_codec = true;
            continue;
        }
        report.manifests += sm.manifests.size();
        auto entries = co_await entries_of(ctx, sm.manifests, opt, total_files);
        if (!entries) {
            if (!opt.allow_unsupported_codec)
                throw commit_failed("snapshot " + std::to_string(sid) + " uses an Avro codec this build cannot read");
            report.skipped_codec = true;
            continue;
        }
        // every live entry names an object of the recorded size
        std::vector<Task<int>> heads;
        for (auto& e : *entries) {
            if (e.file.status == 2) continue;
            heads.push_back(check_data_file(ctx, e.file));
        }
        report.files += heads.size();
        co_await run_batched(std::move(heads), opt.concurrency);
        // conflict re-check (design §7.4) against the parent's live set
        std::string op = s.contains("summary") && s["summary"].is_object() ? s["summary"].value("operation", "") : "";
        if (op == "append") {
            for (auto& e : *entries) {
                if (!attributed_to(e, sid)) continue;
                if (e.file.status == 2)
                    throw commit_failed("snapshot " + std::to_string(sid) + " is an append but deletes '" +
                                        e.file.path + "'");
                if (e.file.content != 0 || e.manifest_content != 0)
                    throw commit_failed("snapshot " + std::to_string(sid) + " is an append but adds delete file '" +
                                        e.file.path + "'");
            }
        }
        if (s.contains("parent-snapshot-id") && s["parent-snapshot-id"].is_number_integer()) {
            int64_t parent = s["parent-snapshot-id"].get<int64_t>();
            auto it = live_cache.find(parent);
            if (it == live_cache.end()) {
                std::optional<std::set<std::string>> live;
                if (const Json* p = find_snapshot(next, parent)) {
                    SnapshotManifests pm = co_await manifests_of(ctx, *p, opt);
                    if (!pm.unsupported) {
                        size_t dummy = 0;
                        auto pe = co_await entries_of(ctx, pm.manifests, opt, dummy);
                        if (pe) {
                            live.emplace();
                            for (auto& e : *pe)
                                if (e.file.status != 2) live->insert(e.file.path);
                        }
                    }
                    if (!live) report.skipped_codec = true;
                }
                it = live_cache.emplace(parent, std::move(live)).first;
            }
            if (it->second) {
                const auto& live = *it->second;
                for (auto& e : *entries) {
                    if (!attributed_to(e, sid)) continue;
                    if (e.file.status == 1 && live.count(e.file.path))
                        throw commit_failed("snapshot " + std::to_string(sid) + " re-adds live file '" + e.file.path +
                                            "'");
                    if (e.file.status == 2 && !live.count(e.file.path))
                        throw commit_failed("snapshot " + std::to_string(sid) + " deletes non-live file '" +
                                            e.file.path + "'");
                }
            }
        }
    }
    // statistics files of the new snapshots
    std::vector<Task<int>> stats;
    if (next.contains("statistics") && next["statistics"].is_array())
        for (auto& st : next["statistics"])
            if (st.is_object() && st.contains("statistics-path") && st["statistics-path"].is_string() &&
                fresh.count(st.value("snapshot-id", int64_t(-1))))
                stats.push_back(check_magic(ctx, st["statistics-path"].get<std::string>(), "PFA1", "statistics file"));
    if (next.contains("partition-statistics") && next["partition-statistics"].is_array())
        for (auto& st : next["partition-statistics"])
            if (st.is_object() && st.contains("statistics-path") && st["statistics-path"].is_string() &&
                fresh.count(st.value("snapshot-id", int64_t(-1))))
                stats.push_back(
                    check_magic(ctx, st["statistics-path"].get<std::string>(), "PAR1", "partition statistics file"));
    if (!stats.empty()) co_await run_batched(std::move(stats), opt.concurrency);
    co_return report;
}

Task<std::optional<std::set<std::string>>> reachable_files(const SnapshotCheckContext& ctx, const Json& md,
                                                           const DeepCheckOptions& opt, size_t& manifests_seen) {
    std::set<std::string> reach;
    if (!md.contains("snapshots") || !md["snapshots"].is_array()) co_return reach;
    for (auto& s : md["snapshots"]) {
        if (!s.is_object()) continue;
        if (s.contains("manifest-list") && s["manifest-list"].is_string())
            reach.insert(checked_key(ctx, s["manifest-list"].get<std::string>()));
        SnapshotManifests sm = co_await manifests_of(ctx, s, opt);
        if (sm.unsupported) co_return std::nullopt;
        manifests_seen += sm.manifests.size();
        if (manifests_seen > opt.max_manifests)
            throw commit_failed("table references more than " + std::to_string(opt.max_manifests) + " manifests");
        for (auto& m : sm.manifests) reach.insert(checked_key(ctx, m.path));
        size_t files = 0;
        auto entries = co_await entries_of(ctx, sm.manifests, opt, files);
        if (!entries) co_return std::nullopt;
        for (auto& e : *entries) reach.insert(checked_key(ctx, e.file.path));
    }
    co_return reach;
}

}  // namespace lights3::tables::iceberg
