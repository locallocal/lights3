// s3adm `mpu list|abort` — zombie multipart upload cleanup over the standard S3
// API (ListMultipartUploads / AbortMultipartUpload), no admin endpoint involved,
// so any credential allowed on the bucket works (roadmap §6.2, docs/cli.md §3.11).
// `list` walks every page (key-marker / upload-id-marker cursor) and prints one
// line per upload with its age; `--older-than` filters by initiation time and
// `abort --all` removes everything the same filter selects.
#include "tools/s3adm_mpu.h"

#include <chrono>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include <nlohmann/json.hpp>

#include "core/config.h"
#include "core/util/time.h"
#include "core/util/uri.h"
#include "s3/xml.h"
#include "tools/s3adm_common.h"

namespace {

using s3adm::finish;
using s3adm::g_exit;
using s3adm::run_admin;
using s3adm::SignedClient;
namespace util = lights3::util;

struct Upload {
    std::string key, upload_id, initiated;
    int64_t age_sec = -1;  // -1 = unparsable Initiated
};

// Every page of ListMultipartUploads under prefix; throws S3Error on a non-200
std::vector<Upload> list_all(SignedClient& cli, const std::string& bucket,
                             const std::string& prefix) {
    std::vector<Upload> out;
    std::string key_marker, id_marker;
    auto now = std::chrono::system_clock::now();
    for (;;) {
        std::string q = "uploads&max-uploads=1000";
        if (!prefix.empty()) q += "&prefix=" + util::aws_uri_encode(prefix, true);
        if (!key_marker.empty()) q += "&key-marker=" + util::aws_uri_encode(key_marker, true);
        if (!id_marker.empty()) q += "&upload-id-marker=" + util::aws_uri_encode(id_marker, true);
        auto r = cli.get("/" + util::aws_uri_encode(bucket, true), q);
        if (!r)
            throw lights3::s3::S3Error(lights3::s3::S3ErrorCode::InternalError,
                                       "transport error: " + httplib::to_string(r.error()));
        if (r->status != 200)
            throw lights3::s3::S3Error(lights3::s3::S3ErrorCode::InternalError,
                                       "HTTP " + std::to_string(r->status) + "\n" + r->body);
        auto root = lights3::s3::xml_parse(r->body);
        for (auto& n : root.children) {
            if (n.name != "Upload") continue;
            Upload u{n.get("Key"), n.get("UploadId"), n.get("Initiated")};
            if (auto t = util::parse_iso8601(u.initiated))
                u.age_sec = std::chrono::duration_cast<std::chrono::seconds>(now - *t).count();
            out.push_back(std::move(u));
        }
        if (root.get("IsTruncated") != "true") break;
        key_marker = root.get("NextKeyMarker");
        id_marker = root.get("NextUploadIdMarker");
        if (key_marker.empty() && id_marker.empty()) break;  // defensive: never loop on a broken cursor
    }
    return out;
}

// --older-than filter: 0 = everything
std::vector<Upload> select(std::vector<Upload> all, int64_t older_than_sec) {
    if (older_than_sec <= 0) return all;
    std::vector<Upload> out;
    for (auto& u : all)
        if (u.age_sec >= older_than_sec) out.push_back(std::move(u));
    return out;
}

std::string human_age(int64_t sec) {
    if (sec < 0) return "?";
    if (sec < 90) return std::to_string(sec) + "s";
    if (sec < 5400) return std::to_string(sec / 60) + "m";
    if (sec < 172800) return std::to_string(sec / 3600) + "h";
    return std::to_string(sec / 86400) + "d";
}

bool read_common(const std::shared_ptr<ccmd::c_command>& c, std::string& bucket,
                 std::string& prefix, int64_t& older_sec, std::string& output) {
    prefix = c->var<std::string>("prefix");
    output = c->var<std::string>("output");
    if (output != "text" && output != "json") {
        fprintf(stderr, "s3adm: --output must be text|json\n");
        g_exit = 2;
        return false;
    }
    auto older = c->var<std::string>("older-than");
    older_sec = 0;
    if (!older.empty()) {
        try {
            older_sec = lights3::parse_duration_sec(older);
        } catch (const std::exception& e) {
            fprintf(stderr, "s3adm: --older-than: %s\n", e.what());
            g_exit = 2;
            return false;
        }
    }
    if (c->args().empty()) {
        fprintf(stderr, "s3adm: usage: %s\n", c->usage().c_str());
        g_exit = 2;
        return false;
    }
    bucket = c->args().front();
    return true;
}

void add_common_flags(const std::shared_ptr<ccmd::c_command>& cmd) {
    cmd->varp<std::string>("prefix", "p", "", "only uploads whose key starts with this prefix.");
    cmd->var<std::string>("older-than", "",
                          "only uploads initiated at least this long ago (e.g. 1h, 2d); default all.");
    cmd->varp<std::string>("output", "o", "text", "text (one line per upload) | json.");
    s3adm::add_conn_flags(cmd);
}

void print_uploads(const std::vector<Upload>& ups, const std::string& output, const std::string& bucket) {
    if (output == "json") {
        nlohmann::json j;
        j["bucket"] = bucket;
        j["uploads"] = nlohmann::json::array();
        for (auto& u : ups) {
            nlohmann::json x;
            x["key"] = u.key;
            x["upload_id"] = u.upload_id;
            x["initiated"] = u.initiated;
            x["age_sec"] = u.age_sec;
            j["uploads"].push_back(x);
        }
        printf("%s\n", j.dump(2).c_str());
        return;
    }
    for (auto& u : ups)
        printf("%-24s %8s  %s  %s\n", u.initiated.c_str(), human_age(u.age_sec).c_str(),
               u.upload_id.c_str(), u.key.c_str());
    printf("%zu upload(s)\n", ups.size());
}

std::shared_ptr<ccmd::c_command> make_list() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "list", "s3adm mpu list photos --older-than=1d", "s3adm mpu list <bucket> [options]",
        "List in-progress multipart uploads of a bucket (every page), one line per upload: "
        "initiated, age, upload id, key. --older-than / --prefix narrow the set — the same "
        "selection `abort --all` acts on.",
        "list in-progress multipart uploads.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            std::string bucket, prefix, output;
            int64_t older = 0;
            if (!read_common(c, bucket, prefix, older, output)) return;
            if (c->args().size() != 1) {
                fprintf(stderr, "s3adm: usage: %s\n", c->usage().c_str());
                g_exit = 2;
                return;
            }
            run_admin(c, [&](SignedClient& cli) {
                print_uploads(select(list_all(cli, bucket, prefix), older), output, bucket);
                return 0;
            });
        });
    add_common_flags(cmd);
    return cmd;
}

std::shared_ptr<ccmd::c_command> make_abort() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "abort", "s3adm mpu abort photos --all --older-than=7d",
        "s3adm mpu abort <bucket> (<key> <upload-id> | --all) [options]",
        "Abort multipart uploads: one upload given as <key> <upload-id>, or --all for every "
        "upload the --prefix / --older-than selection matches (zombie cleanup). Prints one "
        "line per aborted upload; a 404 (already gone) counts as done.",
        "abort multipart uploads.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            std::string bucket, prefix, output;
            int64_t older = 0;
            if (!read_common(c, bucket, prefix, older, output)) return;
            bool all = c->var<bool>("all");
            if ((all && c->args().size() != 1) || (!all && c->args().size() != 3)) {
                fprintf(stderr, "s3adm: usage: %s\n", c->usage().c_str());
                g_exit = 2;
                return;
            }
            run_admin(c, [&](SignedClient& cli) {
                std::vector<Upload> targets;
                if (all) targets = select(list_all(cli, bucket, prefix), older);
                else targets.push_back({c->args()[1], c->args()[2], "", -1});
                int rc = 0;
                size_t aborted = 0;
                for (auto& u : targets) {
                    auto r = cli.del("/" + util::aws_uri_encode(bucket, true) + "/" +
                                         util::aws_uri_encode(u.key, false),
                                     "uploadId=" + util::aws_uri_encode(u.upload_id, true));
                    if (r && (r->status == 204 || r->status == 404)) {
                        ++aborted;
                        if (output == "text") printf("aborted %s %s\n", u.upload_id.c_str(), u.key.c_str());
                    } else {
                        rc = 1;
                        fprintf(stderr, "s3adm: abort %s %s: %s\n", u.upload_id.c_str(), u.key.c_str(),
                                r ? ("HTTP " + std::to_string(r->status)).c_str()
                                  : httplib::to_string(r.error()).c_str());
                    }
                }
                if (output == "json") {
                    nlohmann::json j;
                    j["bucket"] = bucket;
                    j["selected"] = targets.size();
                    j["aborted"] = aborted;
                    printf("%s\n", j.dump(2).c_str());
                } else {
                    printf("%zu of %zu upload(s) aborted\n", aborted, targets.size());
                }
                return rc;
            });
        });
    cmd->varp<bool>("all", "a", false, "abort every upload the selection matches instead of one.");
    add_common_flags(cmd);
    return cmd;
}

}  // namespace

namespace s3adm {

std::shared_ptr<ccmd::c_command> make_mpu() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "mpu", "s3adm mpu list photos --older-than=1d", "s3adm mpu <command> [options]",
        "Multipart upload housekeeping over the standard S3 API (ListMultipartUploads / "
        "AbortMultipartUpload): list in-progress uploads with their age, abort one or every "
        "stale one (roadmap §6.2). Works with any credential allowed on the bucket. Options "
        "must follow the leaf subcommand as --name=value.",
        "list / abort multipart uploads.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            c->print_help();
            g_exit = 2;
        });
    cmd->add_subcommand(make_list());
    cmd->add_subcommand(make_abort());
    return cmd;
}

}  // namespace s3adm
