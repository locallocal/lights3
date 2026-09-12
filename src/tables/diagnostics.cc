#include "tables/diagnostics.h"

#include <map>
#include <set>

#include "core/fault.h"
#include "core/log.h"
#include "s3/errors.h"
#include "tables/identifier.h"
#include "tables/rest_error.h"

namespace lights3::tables {

using nlohmann::json;
using s3::S3Error;
using s3::S3ErrorCode;

namespace {

bool is_precondition(const S3Error& e) {
    return e.code == S3ErrorCode::PreconditionFailed || e.code == S3ErrorCode::NoSuchKey;
}

bool is_missing(const S3Error& e) { return e.code == S3ErrorCode::NoSuchKey || e.code == S3ErrorCode::NoSuchBucket; }

struct Classified {
    TableEntry entry;
    std::string etag;
    std::vector<CommitDiagnosis> commits;
};

// The commit records of the table against its pointer (design §5.4 / step ③ §6)
Task<Classified> classify(ITableCatalogStore& store, std::string_view bucket, const Levels& levels,
                          std::string_view name) {
    auto cur = co_await store.get_table(bucket, levels, name);
    if (!cur) throw not_found_table("table " + ns_display(levels) + "." + std::string(name) + " does not exist");
    Classified out;
    out.entry = cur->value;
    out.etag = cur->etag;
    auto records = co_await store.list_commits(bucket, out.entry.table_id);
    // the committed chain: pointer → prev → prev ..., through COMMITTED records
    std::map<std::string, const CommitRecord*> by_new;
    for (auto& r : records)
        if (r.status == "COMMITTED" && !r.new_metadata_location.empty()) by_new[r.new_metadata_location] = &r;
    std::set<std::string> chain;
    bool cycle = false;
    std::string loc = out.entry.metadata_location;
    while (!loc.empty()) {
        if (!chain.insert(loc).second) {
            cycle = true;
            break;
        }
        auto it = by_new.find(loc);
        if (it == by_new.end()) break;
        loc = it->second->prev_metadata_location;
    }
    for (auto& r : records) {
        CommitDiagnosis d;
        d.record = r;
        if (r.new_token.empty() || r.new_metadata_location.empty() || r.expected_token.empty()) {
            d.state = CommitState::ManualReview;
            d.note = "record lacks token or location fields";
        } else if (r.status == "COMMITTED") {
            d.state = CommitState::Committed;
        } else if (out.entry.version_token == r.new_token) {
            d.state = CommitState::FinalizationRequired;
            d.note = "pointer carries the record's new token";
        } else if (out.entry.version_token == r.expected_token) {
            d.state = CommitState::StagedBeforeTableUpdate;
            d.note = "pointer still at the record's expected token; replayable";
        } else if (chain.count(r.new_metadata_location)) {
            d.state = CommitState::FinalizationRequired;
            d.note = "new metadata sits on the committed chain";
        } else if (cycle) {
            d.state = CommitState::ManualReview;
            d.note = "metadata chain has a cycle";
        } else {
            d.state = CommitState::Superseded;
            d.note = "another commit moved the pointer past this record";
        }
        out.commits.push_back(std::move(d));
    }
    co_return out;
}

Task<std::string> read_text(storage::IStorageBackend& backend, std::string_view bucket, std::string_view key,
                            size_t max) {
    storage::ObjectStream stream;
    try {
        stream = co_await backend.get_object(bucket, key, std::nullopt);
    } catch (const S3Error& e) {
        if (is_missing(e)) co_return std::string();
        throw;
    }
    std::string text;
    std::byte buf[64 * 1024];
    for (;;) {
        size_t n = co_await stream.body->read(std::span(buf));
        if (n == 0) break;
        if (text.size() + n > max) co_return std::string();
        text.append(reinterpret_cast<const char*>(buf), n);
    }
    co_return text;
}

}  // namespace

const char* commit_state_name(CommitState s) {
    switch (s) {
        case CommitState::Committed:
            return "Committed";
        case CommitState::StagedBeforeTableUpdate:
            return "StagedBeforeTableUpdate";
        case CommitState::FinalizationRequired:
            return "FinalizationRequired";
        case CommitState::Superseded:
            return "Superseded";
        case CommitState::ManualReview:
            return "ManualReview";
    }
    return "ManualReview";
}

const char* rename_outcome_name(RenameOutcome o) {
    switch (o) {
        case RenameOutcome::Completed:
            return "completed";
        case RenameOutcome::Abandoned:
            return "abandoned";
        case RenameOutcome::DestinationTaken:
            return "destination-taken";
        case RenameOutcome::Contended:
            return "contended";
    }
    return "?";
}

json TableDiagnostics::to_json(std::string_view bucket) const {
    json j;
    json e = tables::to_json(entry);
    e["metadata_location"] = key_to_location(bucket, entry.metadata_location);
    e["etag"] = etag;
    j["table"] = e;
    j["commits"] = json::array();
    for (auto& c : commits) {
        json r;
        r["commit-id"] = c.record.commit_id;
        r["status"] = c.record.status;
        r["state"] = commit_state_name(c.state);
        r["note"] = c.note;
        r["new-metadata-location"] = key_to_location(bucket, c.record.new_metadata_location);
        r["created-unix"] = c.record.created_unix;
        j["commits"].push_back(r);
    }
    j["unreferenced-metadata"] = json::array();
    for (auto& k : unreferenced_metadata) j["unreferenced-metadata"].push_back(key_to_location(bucket, k));
    return j;
}

json RecoveryReport::to_json() const {
    json j;
    j["finalized"] = finalized;
    j["pruned"] = pruned;
    j["manual"] = manual;
    return j;
}

Task<TableDiagnostics> diagnose_table(ITableCatalogStore& store, storage::IStorageBackend& bucket_backend,
                                      std::string_view bucket, const Levels& levels, std::string_view name,
                                      std::string_view metadata_dir) {
    Classified c = co_await classify(store, bucket, levels, name);
    TableDiagnostics out;
    out.entry = std::move(c.entry);
    out.etag = std::move(c.etag);
    out.commits = std::move(c.commits);
    // referenced = pointer + metadata-log + every record's new location
    std::set<std::string> referenced;
    referenced.insert(out.entry.metadata_location);
    for (auto& d : out.commits) {
        referenced.insert(d.record.new_metadata_location);
        referenced.insert(d.record.prev_metadata_location);
    }
    std::string text = co_await read_text(bucket_backend, bucket, out.entry.metadata_location, 64u << 20);
    if (!text.empty()) {
        try {
            json md = json::parse(text);
            if (md.contains("metadata-log") && md["metadata-log"].is_array())
                for (auto& e : md["metadata-log"])
                    if (e.is_object() && e.contains("metadata-file") && e["metadata-file"].is_string()) {
                        try {
                            referenced.insert(path_to_key(bucket, e["metadata-file"].get<std::string>()));
                        } catch (const RestError&) {
                        }
                    }
        } catch (const json::exception& e) {
            LOG_WARN("tables: diagnostics of {}.{} could not parse the current metadata: {}", ns_display(levels), name,
                     e.what());
        }
    }
    storage::ListOptions opt;
    opt.prefix = std::string(metadata_dir);
    for (;;) {
        auto page = co_await bucket_backend.list_objects(bucket, opt);
        for (auto& o : page.objects)
            if (!referenced.count(o.key)) out.unreferenced_metadata.push_back(o.key);
        if (!page.is_truncated) break;
        opt.start_after = page.next_token;
    }
    co_return out;
}

Task<RecoveryReport> recover_table(ITableCatalogStore& store, std::string_view bucket, const Levels& levels,
                                   std::string_view name, bool prune) {
    Classified c = co_await classify(store, bucket, levels, name);
    RecoveryReport rep;
    for (auto& d : c.commits) {
        switch (d.state) {
            case CommitState::FinalizationRequired: {
                CommitRecord fin = d.record;
                fin.status = "COMMITTED";
                co_await store.put_commit(bucket, c.entry.table_id, fin, {});
                ++rep.finalized;
                LOG_INFO("tables: commit {} of {}.{} finalized by recovery", fin.commit_id, ns_display(levels), name);
                break;
            }
            case CommitState::Superseded:
            case CommitState::StagedBeforeTableUpdate:
                if (prune) {
                    co_await store.delete_commit(bucket, c.entry.table_id, d.record.commit_id);
                    ++rep.pruned;
                }
                break;
            case CommitState::ManualReview:
                ++rep.manual;
                break;
            case CommitState::Committed:
                break;
        }
    }
    co_return rep;
}

// ---------- rename driver (design §5.6) ----------

namespace {

[[noreturn]] void injected(const char* what) {
    throw S3Error(S3ErrorCode::InternalError, std::string("injected failure ") + what);
}

// advance the intent's stage under CAS; false = someone else moved it
Task<bool> advance(ITableCatalogStore& store, std::string_view bucket, Versioned<RenameIntent>& intent,
                   RenameIntent::Stage stage) {
    RenameIntent next = intent.value;
    next.stage = stage;
    storage::PutCondition cas;
    cas.if_match_etag = intent.etag;
    std::string etag;
    try {
        etag = co_await store.put_rename(bucket, next, cas);
    } catch (const S3Error& e) {
        if (!is_precondition(e)) throw;
        co_return false;
    }
    intent.value = std::move(next);
    intent.etag = std::move(etag);
    co_return true;
}

// The source back to Active when this intent fenced it (any other state is left alone)
Task<void> unfence(ITableCatalogStore& store, std::string_view bucket, const RenameIntent& intent) {
    auto src = co_await store.get_table(bucket, intent.src_levels, intent.src_name);
    if (!src || src->value.state != TableState::Renaming || src->value.rename_id != intent.rename_id) co_return;
    TableEntry back = src->value;
    back.state = TableState::Active;
    back.rename_id.clear();
    back.updated_unix = now_unix();
    storage::PutCondition cas;
    cas.if_match_etag = src->etag;
    try {
        co_await store.put_table(bucket, intent.src_levels, intent.src_name, back, cas);
    } catch (const S3Error& e) {
        if (!is_precondition(e)) throw;
        LOG_WARN("tables: rename {} rollback lost a race on the source; leaving it", intent.rename_id);
    }
}

}  // namespace

Task<RenameOutcome> drive_rename(ITableCatalogStore& store, std::string_view bucket, Versioned<RenameIntent> intent,
                                 int64_t prepared_ttl_sec) {
    using Stage = RenameIntent::Stage;
    const RenameIntent& in = intent.value;
    const std::string id = in.rename_id;

    // ② fence the source
    if (in.stage == Stage::Prepared) {
        for (int attempt = 0;; ++attempt) {
            auto src = co_await store.get_table(bucket, in.src_levels, in.src_name);
            bool fenced = src && src->value.state == TableState::Renaming && src->value.rename_id == id;
            if (fenced) break;
            bool stale = !src || src->value.state != TableState::Active || src->etag != in.src_etag;
            if (!stale && prepared_ttl_sec > 0 && now_unix() - in.created_unix > prepared_ttl_sec) stale = true;
            if (stale || attempt > 2) {
                co_await store.delete_rename(bucket, id);
                LOG_INFO("tables: rename {} abandoned: the source moved before it was fenced", id);
                co_return RenameOutcome::Abandoned;
            }
            TableEntry s = src->value;
            s.state = TableState::Renaming;
            s.rename_id = id;
            s.updated_unix = now_unix();
            storage::PutCondition cas;
            cas.if_match_etag = src->etag;
            bool lost = false;
            try {
                co_await store.put_table(bucket, in.src_levels, in.src_name, s, cas);
            } catch (const S3Error& e) {
                if (!is_precondition(e)) throw;
                lost = true;
            }
            if (!lost) break;
        }
        if (fault::check("tables.rename.after_fence")) injected("after fencing the rename source");
        if (!co_await advance(store, bucket, intent, Stage::SourceFenced)) co_return RenameOutcome::Contended;
    }

    // ③ write the destination
    if (in.stage == Stage::SourceFenced) {
        auto src = co_await store.get_table(bucket, in.src_levels, in.src_name);
        if (!src) {
            co_await store.delete_rename(bucket, id);
            LOG_WARN("tables: rename {} abandoned: the fenced source vanished", id);
            co_return RenameOutcome::Abandoned;
        }
        for (int attempt = 0;; ++attempt) {
            auto dst = co_await store.get_table(bucket, in.dst_levels, in.dst_name);
            if (dst && dst->value.table_id == src->value.table_id && dst->value.state == TableState::Active) break;
            bool taken = dst && dst->value.state != TableState::Deleted;
            if (taken || attempt > 2) {
                co_await unfence(store, bucket, in);
                co_await store.delete_rename(bucket, id);
                LOG_INFO("tables: rename {} rolled back: destination {}.{} is taken", id, ns_display(in.dst_levels),
                         in.dst_name);
                co_return RenameOutcome::DestinationTaken;
            }
            TableEntry d = src->value;
            d.levels = in.dst_levels;
            d.name = in.dst_name;
            d.state = TableState::Active;
            d.rename_id.clear();
            d.updated_unix = now_unix();
            storage::PutCondition cond;
            if (dst)
                cond.if_match_etag = dst->etag;
            else
                cond.if_none_match = true;
            bool lost = false;
            try {
                co_await store.put_table(bucket, in.dst_levels, in.dst_name, d, cond);
            } catch (const S3Error& e) {
                if (!is_precondition(e)) throw;
                lost = true;
            }
            if (!lost) break;
        }
        if (fault::check("tables.rename.after_destination")) injected("after writing the rename destination");
        if (!co_await advance(store, bucket, intent, Stage::DestinationWritten)) co_return RenameOutcome::Contended;
    }

    // ④ tombstone the source
    if (in.stage == Stage::DestinationWritten) {
        for (int attempt = 0;; ++attempt) {
            auto src = co_await store.get_table(bucket, in.src_levels, in.src_name);
            if (!src || src->value.state == TableState::Deleted) break;
            if (src->value.state != TableState::Renaming || src->value.rename_id != id || attempt > 2) {
                LOG_WARN("tables: rename {}: source {}.{} is no longer fenced by this intent; not tombstoning it", id,
                         ns_display(in.src_levels), in.src_name);
                break;
            }
            TableEntry s = src->value;
            s.state = TableState::Deleted;
            s.rename_id.clear();
            s.updated_unix = now_unix();
            storage::PutCondition cas;
            cas.if_match_etag = src->etag;
            bool lost = false;
            try {
                co_await store.put_table(bucket, in.src_levels, in.src_name, s, cas);
            } catch (const S3Error& e) {
                if (!is_precondition(e)) throw;
                lost = true;
            }
            if (!lost) break;
        }
        if (fault::check("tables.rename.after_tombstone")) injected("after tombstoning the rename source");
        if (!co_await advance(store, bucket, intent, Stage::SourceTombstoned)) co_return RenameOutcome::Contended;
    }

    // ⑤ done
    if (fault::check("tables.rename.before_cleanup")) injected("before deleting the rename intent");
    co_await store.delete_rename(bucket, id);
    co_return RenameOutcome::Completed;
}

Task<int> recover_renames(ITableCatalogStore& store, std::string_view bucket, int64_t prepared_ttl_sec) {
    int driven = 0;
    auto intents = co_await store.list_renames(bucket);
    for (auto& i : intents) {
        auto fresh = co_await store.get_rename(bucket, i.rename_id);
        if (!fresh) continue;
        std::exception_ptr err;
        RenameOutcome outcome = RenameOutcome::Contended;
        try {
            outcome = co_await drive_rename(store, bucket, std::move(*fresh), prepared_ttl_sec);
        } catch (...) {
            err = std::current_exception();
        }
        if (err) {
            try {
                std::rethrow_exception(err);
            } catch (const std::exception& e) {
                LOG_WARN("tables: recovery of rename {} in {} failed: {}", i.rename_id, bucket, e.what());
            }
            continue;
        }
        ++driven;
        LOG_INFO("tables: rename {} in {} driven to {} by recovery", i.rename_id, bucket, rename_outcome_name(outcome));
    }
    co_return driven;
}

}  // namespace lights3::tables
