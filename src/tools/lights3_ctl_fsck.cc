// lights3-ctl `fsck` — online integrity verification through the data plane
// (roadmap §3.1): ListObjectsV2 pages, then for every key a streaming GET with
// the MD5 recomputed client-side and compared against the ETag. Multipart
// composites ("-N" ETags) are verified per part via GET ?partNumber=i and
// storage::combined_etag over the per-part digests; objects the server cannot
// slice (pre-layout legacy, 501) count as unverifiable, never as mismatches.
// This is the end-to-end complement of the offline `lights3 fsck`: it also
// covers the gateway read path, at the cost of pulling every byte over HTTP.
#include "tools/lights3_ctl_fsck.h"

#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <string>
#include <thread>
#include <vector>

#include "core/util/uri.h"
#include "s3/xml.h"
#include "storage/multipart.h"
#include <nlohmann/json.hpp>
#include "tools/lights3_ctl_common.h"
#include "tools/lights3_ctl_jobs.h"

namespace {

using lights3_ctl::g_exit;
using lights3_ctl::run_admin;
using lights3_ctl::SignedClient;
namespace util = lights3::util;

struct FsckStats {
    uint64_t objects = 0;
    uint64_t bytes = 0;
    uint64_t mismatches = 0;
    uint64_t errors = 0;        // transport/HTTP failures
    uint64_t unverifiable = 0;  // non-MD5 ETag or legacy multipart without a part layout
    uint64_t skipped = 0;       // deleted between list and GET
};

// Client-side pacing: the tool is synchronous, so a plain sleep suffices.
// Catch-up burst after a slow request is capped at one second's budget
struct RateLimiter {
    uint64_t bps;
    std::chrono::steady_clock::time_point next{};
    void pace(uint64_t n) {
        if (bps == 0 || n == 0) return;
        auto now = std::chrono::steady_clock::now();
        if (next == std::chrono::steady_clock::time_point{}) next = now;
        if (next < now - std::chrono::seconds(1)) next = now - std::chrono::seconds(1);
        next += std::chrono::nanoseconds(uint64_t(n * 1'000'000'000.0 / double(bps)));
        if (next > now) std::this_thread::sleep_for(next - now);
    }
};

bool is_md5_hex(std::string_view s) {
    if (s.size() != 32) return false;
    for (char c : s)
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
    return true;
}

void verify_object(SignedClient& cli, const std::string& bucket, const std::string& key,
                   const std::string& etag, FsckStats& st, RateLimiter& rl) {
    ++st.objects;
    std::string path = "/" + bucket + "/" + util::aws_uri_encode(key, /*encode_slash=*/false);
    auto dash = etag.find('-');
    if (!is_md5_hex(dash == std::string::npos ? std::string_view(etag)
                                              : std::string_view(etag).substr(0, dash))) {
        ++st.unverifiable;
        printf("UNVERIFIABLE %s (ETag is not an MD5: %s)\n", key.c_str(), etag.c_str());
        return;
    }
    std::string computed;
    if (dash == std::string::npos) {
        uint64_t before = st.bytes;
        auto r = cli.get_hashed(path, "", &computed, &st.bytes);
        rl.pace(st.bytes - before);
        if (!r) {
            ++st.errors;
            fprintf(stderr, "lights3-ctl: fsck: GET %s: %s\n", key.c_str(),
                    httplib::to_string(r.error()).c_str());
            return;
        }
        if (r->status == 404) {
            ++st.skipped;  // deleted between list and GET
            return;
        }
        if (r->status != 200) {
            ++st.errors;
            fprintf(stderr, "lights3-ctl: fsck: GET %s: HTTP %d\n", key.c_str(), r->status);
            return;
        }
    } else {
        // Composite ETag: the declared part count comes from the suffix, the
        // authoritative one from x-amz-mp-parts-count; a 501 means the object
        // predates the recorded part layout and cannot be sliced
        auto h = cli.head(path, "partNumber=1");
        if (!h) {
            ++st.errors;
            fprintf(stderr, "lights3-ctl: fsck: HEAD %s: %s\n", key.c_str(),
                    httplib::to_string(h.error()).c_str());
            return;
        }
        if (h->status == 404) {
            ++st.skipped;
            return;
        }
        if (h->status == 501) {
            ++st.unverifiable;
            printf("UNVERIFIABLE %s (multipart object without a recorded part layout)\n",
                   key.c_str());
            return;
        }
        if (h->status != 206) {
            ++st.errors;
            fprintf(stderr, "lights3-ctl: fsck: HEAD %s?partNumber=1: HTTP %d\n", key.c_str(),
                    h->status);
            return;
        }
        int parts = atoi(h->get_header_value("x-amz-mp-parts-count").c_str());
        long declared = strtol(etag.c_str() + dash + 1, nullptr, 10);
        if (parts <= 0 || parts != declared) {
            ++st.unverifiable;
            printf("UNVERIFIABLE %s (parts count %d does not match ETag suffix %ld)\n",
                   key.c_str(), parts, declared);
            return;
        }
        std::vector<std::string> md5s;
        md5s.reserve(size_t(parts));
        for (int i = 1; i <= parts; ++i) {
            std::string part_hex;
            uint64_t before = st.bytes;
            auto r = cli.get_hashed(path, "partNumber=" + std::to_string(i), &part_hex,
                                    &st.bytes);
            rl.pace(st.bytes - before);
            if (!r || r->status != 206) {
                ++st.errors;
                fprintf(stderr, "lights3-ctl: fsck: GET %s?partNumber=%d: %s\n", key.c_str(), i,
                        r ? ("HTTP " + std::to_string(r->status)).c_str()
                          : httplib::to_string(r.error()).c_str());
                return;
            }
            md5s.push_back(std::move(part_hex));
        }
        computed = lights3::storage::combined_etag(md5s);
    }
    if (computed != etag) {
        ++st.mismatches;
        printf("MISMATCH %s (stored %s, computed %s)\n", key.c_str(), etag.c_str(),
               computed.c_str());
    }
}

int run_fsck(SignedClient& cli, const std::string& bucket, const std::string& prefix,
             uint64_t bps) {
    FsckStats st;
    RateLimiter rl{bps};
    std::string token;
    for (;;) {
        std::string q = "list-type=2&max-keys=1000";
        if (!prefix.empty()) q += "&prefix=" + util::aws_uri_encode(prefix, /*encode_slash=*/true);
        if (!token.empty())
            q += "&continuation-token=" + util::aws_uri_encode(token, /*encode_slash=*/true);
        auto r = cli.get("/" + bucket, q);
        if (!r) {
            fprintf(stderr, "lights3-ctl: fsck: list: %s\n", httplib::to_string(r.error()).c_str());
            return 1;
        }
        if (r->status != 200) {
            fprintf(stderr, "lights3-ctl: fsck: list: HTTP %d\n%s\n", r->status, r->body.c_str());
            return 1;
        }
        auto root = lights3::s3::xml_parse(r->body, 16 << 20);
        for (const auto& child : root.children) {
            if (child.name != "Contents") continue;
            std::string etag = child.get("ETag");
            // The XML parser resolves &quot;; strip the literal quotes S3 wraps ETags in
            if (etag.size() >= 2 && etag.front() == '"' && etag.back() == '"')
                etag = etag.substr(1, etag.size() - 2);
            verify_object(cli, bucket, child.get("Key"), etag, st, rl);
        }
        if (root.get("IsTruncated") != "true") break;
        token = root.get("NextContinuationToken");
        if (token.empty()) break;  // defensive: a truncated page must carry a token
    }
    printf("fsck %s: %llu objects, %llu bytes; %llu mismatches, %llu errors, "
           "%llu unverifiable, %llu skipped\n",
           bucket.c_str(), (unsigned long long)st.objects, (unsigned long long)st.bytes,
           (unsigned long long)st.mismatches, (unsigned long long)st.errors,
           (unsigned long long)st.unverifiable, (unsigned long long)st.skipped);
    return (st.mismatches || st.errors) ? 1 : 0;
}

}  // namespace

namespace lights3_ctl {

// --offline: the server-side scrub (backlog-sequence ③) through the admin plane,
// on the shared job driver (lights3_ctl_jobs.h): POST starts one round (409
// while one runs), GET polls; the final document is printed as JSON and
// findings > 0 make the exit code 1 like `lights3 fsck`
int run_fsck_offline(SignedClient& cli, const std::string& backend, uint64_t mbps, bool wait,
                     bool status_only) {
    return run_job(cli, "/-/admin/fsck/" + backend, "fsck", mbps, wait, status_only);
}

std::shared_ptr<ccmd::c_command> make_fsck() {
    auto cmd = std::make_shared<ccmd::c_command>(
        "fsck", "lights3-ctl fsck my-bucket --prefix=photos/ --max-mbps=50",
        "lights3-ctl fsck <bucket> [options] | lights3-ctl fsck --offline <backend> [--max-mbps=N] [--no-wait] | lights3-ctl fsck --status <backend>",
        "Verify a bucket's objects end to end through the S3 API: every listed object "
        "is downloaded and its MD5 recomputed against the ETag (multipart composites "
        "via GET ?partNumber per part). Read-only; prints MISMATCH/UNVERIFIABLE lines "
        "plus a summary, exit code 1 when mismatches or errors were found. Works with "
        "any credential that can read the bucket.\n"
        "--offline <backend>: run the server-side scrub instead (duostore crc/refs, "
        "localfs/xlocalfs ETag; POST /-/admin/fsck/<backend>, root credential), wait for "
        "it and print the outcome document (exit 1 on findings; --no-wait returns the "
        "job id at once). --status <backend>: print the running/last outcome. One job "
        "per backend at a time (409 ScrubInProgress).",
        "verify a bucket's objects against their ETags.",
        [](const std::shared_ptr<ccmd::c_command>& c) {
            bool offline = c->var<bool>("offline");
            bool status = c->var<bool>("status");
            if (c->args().size() != 1 || (offline && status)) {
                fprintf(stderr, "lights3-ctl: usage: %s\n", c->usage().c_str());
                g_exit = 2;
                return;
            }
            std::string target = c->args().front();
            auto prefix = c->var<std::string>("prefix");
            int mbps = c->var<int>("max-mbps");
            if (mbps < 0) {
                fprintf(stderr, "lights3-ctl: fsck: --max-mbps must be >= 0\n");
                g_exit = 2;
                return;
            }
            if (offline || status) {
                bool wait = !c->var<bool>("no-wait");
                run_admin(c, [&](SignedClient& cli) {
                    return run_fsck_offline(cli, target, uint64_t(mbps), wait, status);
                });
                return;
            }
            run_admin(c, [&](SignedClient& cli) {
                return run_fsck(cli, target, prefix, uint64_t(mbps) * 1000 * 1000);
            });
        });
    cmd->varp<std::string>("prefix", "p", "", "only verify keys under this prefix.");
    cmd->var<int>("max-mbps", 0, "read throttle in MB/s (0 = unthrottled); online: download, --offline: server-side scrub.");
    cmd->var<bool>("offline", false, "<backend> is a backend name: run the server-side scrub via /-/admin/fsck (root).");
    cmd->var<bool>("status", false, "<backend> is a backend name: print the running/last scrub outcome.");
    cmd->var<bool>("no-wait", false, "with --offline: return right after starting the job.");
    lights3_ctl::add_conn_flags(cmd);
    return cmd;
}

}  // namespace lights3_ctl
