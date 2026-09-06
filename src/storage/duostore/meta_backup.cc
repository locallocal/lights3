#include "storage/duostore/meta_backup.h"

#include <nlohmann/json.hpp>

#include <cstdio>
#include <fstream>
#include <sstream>

#include "core/util/time.h"

namespace lights3::storage::duostore {

using nlohmann::json;

namespace {
[[noreturn]] void bad(const std::string& why) {
    throw s3::S3Error(s3::S3ErrorCode::InternalError, "duostore meta backup: " + why);
}
}  // namespace

int64_t backup_now_ms() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

std::optional<int64_t> parse_restore_ts(const std::string& s) {
    if (s.empty()) return std::nullopt;
    bool digits = true;
    for (char c : s)
        if (c < '0' || c > '9') digits = false;
    if (digits) return std::stoll(s);
    if (auto t = util::parse_iso8601(s))
        return std::chrono::duration_cast<std::chrono::milliseconds>(t->time_since_epoch()).count();
    return std::nullopt;
}

BackupManifest BackupManifest::load(const std::filesystem::path& dir) {
    BackupManifest m;
    std::ifstream f(dir / kBackupManifest);
    if (!f) return m;
    json j;
    try {
        f >> j;
    } catch (const json::exception& e) {
        bad(std::string("malformed ") + kBackupManifest + ": " + e.what());
    }
    if (!j.is_object() || !j.contains("engine") || !j.contains("entries") ||
        !j["entries"].is_array())
        bad(std::string("malformed ") + kBackupManifest + ": missing engine/entries");
    m.engine = j.value("engine", "");
    m.backend = j.value("backend", "");
    uint64_t expect = 1;
    for (auto& e : j["entries"]) {
        MetaBackupEntry x;
        x.id = e.value("id", uint64_t{0});
        x.full = e.value("full", false);
        x.ts_ms = e.value("ts_ms", int64_t{0});
        x.file = e.value("file", "");
        x.marker = e.value("marker", "");
        x.bytes = e.value("bytes", uint64_t{0});
        if (x.id != expect) bad("entry ids are not consecutive (expected " + std::to_string(expect) + ")");
        if (expect == 1 && !x.full) bad("the first entry must be a full backup");
        m.entries.push_back(std::move(x));
        ++expect;
    }
    return m;
}

void BackupManifest::save(const std::filesystem::path& dir) const {
    json j;
    j["engine"] = engine;
    j["backend"] = backend;
    j["entries"] = json::array();
    for (auto& e : entries) {
        json x;
        x["id"] = e.id;
        x["full"] = e.full;
        x["ts_ms"] = e.ts_ms;
        x["ts"] = util::iso8601(std::chrono::system_clock::time_point(std::chrono::milliseconds(e.ts_ms)));
        x["file"] = e.file;
        x["marker"] = e.marker;
        x["bytes"] = e.bytes;
        j["entries"].push_back(std::move(x));
    }
    std::filesystem::create_directories(dir);
    auto tmp = dir / (std::string(kBackupManifest) + ".tmp");
    {
        std::ofstream f(tmp, std::ios::trunc);
        if (!f) bad("cannot write " + tmp.string());
        f << j.dump(2) << "\n";
        if (!f) bad("cannot write " + tmp.string());
    }
    std::error_code ec;
    std::filesystem::rename(tmp, dir / kBackupManifest, ec);
    if (ec) bad("cannot rename " + tmp.string() + ": " + ec.message());
}

std::vector<MetaBackupEntry> BackupManifest::plan(std::optional<uint64_t> to_id,
                                                  std::optional<int64_t> to_ts_ms) const {
    auto invalid = [](const std::string& why) {
        throw s3::S3Error(s3::S3ErrorCode::InvalidRequest, "duostore meta restore: " + why);
    };
    if (entries.empty()) invalid("the backup directory has no entries");
    if (to_id && to_ts_ms) invalid("give either --to-id or --to-ts, not both");
    std::vector<MetaBackupEntry> out;
    if (to_id) {
        if (*to_id < 1 || *to_id > entries.back().id)
            invalid("no backup entry with id " + std::to_string(*to_id));
        for (auto& e : entries)
            if (e.id <= *to_id) out.push_back(e);
    } else if (to_ts_ms) {
        for (auto& e : entries)
            if (e.ts_ms <= *to_ts_ms) out.push_back(e);
        if (out.empty()) invalid("the target time precedes the first full backup");
    } else {
        out = entries;
    }
    // A prefix always starts with the full entry; a later full entry restarts the
    // chain, so replay only from the last full entry inside the prefix
    size_t start = 0;
    for (size_t i = 0; i < out.size(); ++i)
        if (out[i].full) start = i;
    out.erase(out.begin(), out.begin() + static_cast<long>(start));
    return out;
}

}  // namespace lights3::storage::duostore
