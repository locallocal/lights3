// L2 entry point: S3Service::dispatch (auth -> routing -> handler -> error mapping)
#pragma once

#include <chrono>
#include <span>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "core/cancel.h"
#include "core/metrics.h"
#include "core/semaphore.h"
#include "core/task.h"
#include "core/thread_pool.h"
#include "http/model.h"
#include "s3/auth/policy.h"
#include "s3/auth/sigv4.h"
#include "s3/metrics.h"
#include "s3/website_store.h"
#include "storage/bucket_router.h"
#include "storage/multipart.h"

namespace lights3::s3 {

class CredentialStore;  // auth/credential_store.h (only the admin handler's .cc needs the full definition)

struct RequestContext {
    std::string request_id;
    // x-amz-id-2 / <HostId> (docs/archive/gaps.md §5.9): one of the two ids AWS support tickets ask for; clients only
    // relay the pair they saw, so the log side must be able to match it
    std::string host_id;
    // Cancellation signal: client disconnect (detected by the driver), request timeout, process shutdown (docs/concurrency.md §5);
    // defaults to "never cancelled". Long loops (between chunks of streaming reads/writes) and pool.schedule() observe it
    CancelToken cancel;
};

class S3Service {
public:
    // Non-empty base_domain enables virtual-host style addressing (docs/s3-protocol.md §2)
    S3Service(storage::BucketRouter router, SigV4Authenticator auth,
              std::string base_domain = "")
        : router_(std::move(router)),
          auth_(std::move(auth)),
          base_domain_(std::move(base_domain)) {
        // Host matching is done uniformly in lowercase (resolve_address); the config side normalizes the same way
        for (char& c : base_domain_)
            if (c >= 'A' && c <= 'Z') c += 'a' - 'A';
    }

    // Top-level entry: catches all exceptions internally and maps them to S3 error responses, never throws to L1
    Task<http::HttpResponse> dispatch(http::HttpRequest req);

    // Thread pool metrics source for /-/metrics (optional, injected during main assembly)
    void set_pool_stats(std::function<ThreadPool::Stats()> fn) { pool_stats_ = std::move(fn); }

    // Ingress throttling admission snapshot (docs/archive/gaps.md §7, optional, injected during main assembly)
    void set_admission_stats(std::function<AdmissionStats()> fn) {
        admission_stats_ = std::move(fn);
    }

    // Timer thread health (docs/archive/gaps.md §7, optional)
    void set_timer_stats(std::function<TimerQueue::Stats()> fn) {
        timer_stats_ = std::move(fn);
    }

    // Backend-level metrics registry (optional): rendered appended after the L2 request metrics
    void set_backend_metrics(std::shared_ptr<MetricsRegistry> m) {
        backend_metrics_ = std::move(m);
    }

    // Dynamic credential management (docs/credential-management.md): when not injected, /-/admin/credentials is always AccessDenied
    void set_credential_store(std::shared_ptr<CredentialStore> s) {
        cred_store_ = std::move(s);
    }

    // Per-request timeout (docs/archive/gaps.md §3.3): 0 = disabled. On expiry, cooperative cancellation interrupts the
    // whole handler chain; suspension points throw OperationCancelled -> 503
    void set_request_timeout(std::chrono::milliseconds t) { request_timeout_ = t; }

    // Minimum multipart part size (docs/archive/gaps.md §5.7): defaults to AWS's 5MiB, 0 = unlimited.
    // A knob rather than hardcoded because toolchains in front of the gateway may not honor the rule
    // (bouncing small-part uploads costs more than making ops fix the tool), and so that a
    // "proxy to another lights3" deployment is not judged once per layer
    void set_min_part_size(uint64_t n) { min_part_size_ = n; }
    uint64_t min_part_size() const { return min_part_size_; }

    // Static website hosting (docs/static-website.md): buckets accepting anonymous
    // GET/HEAD object reads, with index/error document semantics. Names are validated
    // here (startup) with the same gate as user requests — a config entry for a reserved
    // bucket (.sys) must fail loudly, not sit dormant until dispatch happens to reject it.
    // Convenience for tests/static-only assemblies: wraps the entries in a
    // non-persistent WebsiteStore (the ?website API then rejects mutations)
    void set_website_buckets(std::vector<WebsiteBucket> buckets);
    // Full store (phase ③): static entries + .sys-persisted dynamic entries + sync
    void set_website_store(std::shared_ptr<WebsiteStore> store) {
        website_store_ = std::move(store);
    }

    // Verification result passed down the dispatch chain to handlers (docs/archive/gaps.md §5.10): ListBuckets must
    // filter results by policy, and the policy previously lived only in dispatch's local variable
    struct RequestAuth {
        std::string_view access_key;             // empty when auth is disabled
        const CredentialPolicy* policy = nullptr;  // nullptr = unrestricted
    };

    // Explicit dispatch table (docs/s3-protocol.md §2): (method, scope, query-flag) -> handler, matched in declaration order
    enum class Scope { Service, Bucket, Object };
    using Handler = Task<http::HttpResponse> (*)(S3Service&, http::HttpRequest&, std::string,
                                                 std::string, const RequestAuth&);
    struct Route {
        std::string_view method;
        Scope scope;
        std::string_view flag;  // "" = fallback; "k" matches on query presence; "k=v" matches on value
        // Query allowlist (docs/archive/gaps.md §3.5): extra query keys this route permits (space-separated).
        // The flag key and presigned signature params are inherently allowed; a key outside the list -> 501.
        // The structural flaw of a blocklist fallback is that any omission silently degrades into
        // "read/write the whole object" -- ?attributes returns the whole object body, ?partNumber returns the whole object, response-* gets swallowed
        std::string_view extra_query;
        // The action this route corresponds to (docs/archive/gaps.md §5.10): authorization is decided by it, not guessed
        // from the HTTP method -- DeleteObjects is a POST yet clearly a delete, CreateMultipartUpload is also a
        // POST yet a write; the method dimension simply cannot separate the two
        Action action;
        Handler fn;
    };

    // Dispatch table matching (authorization needs to know the action before the handler is called, hence separate from route)
    static std::span<const Route> route_table();
    const Route* match_route(const http::HttpRequest& req, Scope scope) const;

private:
    Task<http::HttpResponse> route(http::HttpRequest& req, std::string bucket, std::string key,
                                   const RequestAuth& auth);

    // handlers/buckets.cc
    Task<http::HttpResponse> list_buckets(const RequestAuth& auth);
    Task<http::HttpResponse> create_bucket(http::HttpRequest& req, std::string bucket);
    Task<http::HttpResponse> head_bucket(std::string bucket);
    // ?website subresource (docs/static-website.md phase ③, root credential only)
    Task<http::HttpResponse> get_bucket_website(std::string bucket, const RequestAuth& auth);
    Task<http::HttpResponse> put_bucket_website(http::HttpRequest& req, std::string bucket,
                                                const RequestAuth& auth);
    Task<http::HttpResponse> delete_bucket_website(std::string bucket,
                                                   const RequestAuth& auth);
    Task<http::HttpResponse> delete_bucket(std::string bucket);
    Task<http::HttpResponse> get_bucket_location(std::string bucket);
    // handlers/objects.cc
    Task<http::HttpResponse> put_object(http::HttpRequest& req, std::string bucket,
                                        std::string key);
    Task<http::HttpResponse> copy_object(http::HttpRequest& req, std::string bucket,
                                         std::string key);
    Task<http::HttpResponse> get_object(http::HttpRequest& req, std::string bucket,
                                        std::string key, bool head_only);
    Task<http::HttpResponse> delete_object(std::string bucket, std::string key);
    Task<http::HttpResponse> delete_objects(http::HttpRequest& req, std::string bucket,
                                            const RequestAuth& auth);
    // handlers/list_objects.cc
    Task<http::HttpResponse> list_objects(http::HttpRequest& req, std::string bucket,
                                          const RequestAuth& auth);
    // handlers/multipart.cc
    Task<http::HttpResponse> create_multipart(http::HttpRequest& req, std::string bucket,
                                              std::string key);
    Task<http::HttpResponse> upload_part(http::HttpRequest& req, std::string bucket,
                                         std::string key);
    Task<http::HttpResponse> complete_multipart(http::HttpRequest& req, std::string bucket,
                                                std::string key);
    Task<http::HttpResponse> abort_multipart(http::HttpRequest& req, std::string bucket,
                                             std::string key);
    Task<http::HttpResponse> list_parts(http::HttpRequest& req, std::string bucket,
                                        std::string key);
    Task<http::HttpResponse> list_multipart_uploads(http::HttpRequest& req, std::string bucket,
                                                    const RequestAuth& auth);

    Task<http::HttpResponse> readyz();

    // handlers/admin_credentials.cc (docs/credential-management.md §2): performs verification and root check
    // internally, renders errors as JSON bodies; access_key out-param feeds the access log
    Task<http::HttpResponse> admin_credentials(http::HttpRequest& req,
                                               std::string& access_key);

    // virtual-host style: when Host matches *.base_domain, the bucket is prepended for path parsing.
    // The vhost flag steers internal-endpoint routing (docs/archive/gaps.md §3.8): under vhost, req.path is the key,
    // and "/-/metrics" may be a legitimate object key in mybucket that internal endpoints must not shadow
    struct Address {
        std::string bucket, key;
        bool vhost = false;
    };
    Address resolve_address(const http::HttpRequest& req) const;

    // True when the request may take the anonymous website-read path: no signature
    // material at all (neither Authorization header nor presigned query parameters),
    // GET/HEAD on a listed website bucket (docs/static-website.md). Bucket-scope reads
    // are admitted here because the index rewrite in dispatch turns them into object
    // reads; anything still non-object after the rewrite is refused by the route gate.
    // The snapshot is taken once per request in dispatch: pointers into it must stay
    // valid across co_awaits even if a concurrent ?website PUT swaps the store
    bool anonymous_website_read(const http::HttpRequest& req, const Address& addr,
                                const WebsiteStore::Snapshot& snap) const;
    // Anonymous error page (docs/static-website.md phase ②): the configured error
    // object's content under the ORIGINAL status code, or a built-in HTML page
    Task<http::HttpResponse> website_error_page(const S3Error& e, const WebsiteBucket& site,
                                                bool head_only);

    storage::BucketRouter router_;
    SigV4Authenticator auth_;
    std::string base_domain_;
    Metrics metrics_;
    std::function<ThreadPool::Stats()> pool_stats_;
    std::function<AdmissionStats()> admission_stats_;
    std::function<TimerQueue::Stats()> timer_stats_;
    std::chrono::milliseconds request_timeout_{0};
    uint64_t min_part_size_ = storage::kMinPartSize;
    std::shared_ptr<MetricsRegistry> backend_metrics_;
    std::shared_ptr<CredentialStore> cred_store_;
    std::shared_ptr<WebsiteStore> website_store_;  // null = website hosting off

    // Short cache for /-/readyz results (anonymously reachable, and the probe issues real calls to every backend:
    // without a cache, an anonymous loop can amplify into billed/rate-limited upstream calls)
    std::mutex readyz_mu_;
    std::chrono::steady_clock::time_point readyz_at_{};
    int readyz_status_ = 0;  // 0 = no result yet
    std::string readyz_body_;
    bool readyz_inflight_ = false;
};

}  // namespace lights3::s3
