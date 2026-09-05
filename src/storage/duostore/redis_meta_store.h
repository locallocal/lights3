// L3: Redis implementation of IMetaStore (docs/duostore-redis-meta.md).
// Commit-class operations = one generic guarded-commit Lua script (check-and-commit) + client-
// side optimistic CAS retry (§3.2); the script executes atomically on the single-threaded Redis
// server — script atomicity is global atomicity, so multiple gateways sharing one redis share
// the meta, holding no business mutexes (§3.4, only the small id-segment allocation lock stays).
// Value encoding is 100% reused from codec.cc (§2.1); Redis Cluster is unsupported (§1 non-goal).
#pragma once

#include <atomic>
#include <cstdint>
#include <memory>
#include <mutex>
#include <thread>
#include <optional>
#include <string>
#include <vector>

#include "core/metrics.h"
#include "storage/duostore/meta_store.h"

struct redisReply;

namespace lights3::storage::duostore {

struct RedisMetaOptions {
    std::string uri;              // redis://[:pass@]host[:port][/db] or unix://<path>
    std::string prefix = "duo:";  // prefix for all keys (§2.1 multi-instance/test isolation)
    int timeout_ms = 3000;        // connect + per-command timeout (§5.4)
    int pool_size = 8;            // connection pool size (§5.2)
    int wait_replicas = 0;        // replicas to WAIT for after commit-class commands (§6; 0 = no wait)
    MetricsScope metrics;         // CAS retry / reconnect counters (R4; empty scope = detached instances)
};

class RedisBatch;

struct RedisReplyDeleter {
    void operator()(redisReply* r) const;  // freeReplyObject
};
using RedisReplyPtr = std::unique_ptr<redisReply, RedisReplyDeleter>;

class RedisMetaStore final : public IMetaStore {
public:
    explicit RedisMetaStore(RedisMetaOptions opt);
    ~RedisMetaStore() override;
    RedisMetaStore(const RedisMetaStore&) = delete;

    void create_bucket(std::string_view b) override;
    void delete_bucket(std::string_view b) override;
    bool bucket_exists(std::string_view b) override;
    std::vector<BucketInfo> list_buckets() override;

    std::optional<ObjectRec> get_object(std::string_view b, std::string_view k) override;
    std::optional<ObjectMeta> head_object(std::string_view b, std::string_view k) override;
    void put_object(std::string_view b, std::string_view k, ObjectRec rec,
                    PutCondition cond = {}) override;
    bool delete_object(std::string_view b, std::string_view k) override;
    ListResult list_objects(std::string_view b, const ListOptions& opt) override;

    std::string create_upload(std::string_view b, std::string_view k, ObjectMeta meta) override;
    UploadRec require_upload(std::string_view b, std::string_view k,
                             std::string_view id) override;
    void put_part(std::string_view b, std::string_view k, std::string_view id,
                  PartRec p) override;
    std::vector<PartRec> list_parts(std::string_view b, std::string_view k,
                                    std::string_view id) override;
    std::vector<UploadInfo> list_uploads(std::string_view b, std::string_view key_marker = {},
                                         std::string_view id_marker = {}, int limit = 0,
                                         std::string_view prefix = {}) override;
    std::string complete_upload(std::string_view b, std::string_view k, std::string_view id,
                                std::span<const PartInfo> parts) override;
    void abort_upload(std::string_view b, std::string_view k, std::string_view id) override;

    uint64_t alloc_file_run(Extent::Kind kind, uint32_t n) override;
    std::vector<std::pair<uint64_t, Reclaim>> peek_reclaims(size_t max, uint64_t min_seq = 0,
                                                            size_t max_extents = SIZE_MAX) override;
    void ack_reclaim(uint64_t seq) override;
    void ack_reclaims(std::span<const uint64_t> seqs) override;
    bool try_gc_lease(std::string_view owner, int64_t ttl_ms) override;
    // Multi-gateway read lease (roadmap §3.7): per-owner key with PX expiry
    // (crashed publishers yield automatically); min via SCAN + MGET. Note redis
    // does NOT implement IMetaStore::snapshot() — no MVCC to pin, so the online
    // meta dump falls back to the writes-stopped contract on this engine
    bool publish_read_lease(std::string_view owner, int64_t oldest_ms,
                            int64_t ttl_ms) override;
    std::optional<int64_t> min_read_lease() override;
    std::vector<PackStat> pack_stats() override;
    void seal_pack(uint64_t pack_id, uint64_t file_size) override;
    void drop_pack_stat(uint64_t pack_id) override;
    bool swap_extents(std::string_view b, std::string_view k, uint64_t expect_version,
                      const DataRef& from, const DataRef& to) override;
    bool chunk_referenced(uint64_t file_id) override;
    void scan_refs(const std::function<void(uint64_t file_id)>& cb) override;
    // Invalidation feed (backlog-sequence ⑤, docs/duostore-redis-meta.md §3.6): every
    // commit that changes an object record PUBLISHes "<bucket>\0<key>" on <prefix>inv
    // from inside the commit script (atomic with the write, no extra round trip);
    // this starts a dedicated subscriber connection + thread that feeds on_key, calls
    // on_reset on every (re)connect, and reconnects with backoff. One subscription
    // per store; a second call replaces nothing and returns false
    bool subscribe_invalidations(InvalidationSink on_key,
                                 std::function<void()> on_reset) override;
    std::string invalidation_channel() const;
    // Payload of one invalidation message: "<origin>\0<bucket>\0<key>"; origin is this
    // store's random id, so a subscriber skips its own commits (the write path already
    // invalidated exactly; a late self-message would only evict a fresh fill)
    std::string invalidation_payload(std::string_view b, std::string_view k) const;
    void close() override;

private:
    friend class RedisBatch;

    struct Conn;  // hiredis connection (defined in the .cc; the header leaks no hiredis types)
    using ReplyPtr = RedisReplyPtr;

    // Id-segment reservation (§4, isomorphic to RocksMetaStore): one INCRBY of +kIdSegment, then
    // in-memory handout. burned: the first reservation burns one extra segment to skip ids
    // possibly handed out within the AOF everysec loss window
    struct IdRange {
        uint64_t next = 0, limit = 0;
        bool burned = false;
    };
    static constexpr uint64_t kIdSegment = 4096;

    // ---- Connection pool (§5.2): mutex-protected idle stack, RAII acquire/release ----
    std::unique_ptr<Conn> acquire();
    void release(std::unique_ptr<Conn> c);
    std::unique_ptr<Conn> make_conn();  // connect + AUTH/SELECT (§5.4)

    // Command execution (always redisCommandArgv, binary-safe, §5.1). read_retry: read-only may
    // retry once on a fresh connection; commit-class IO failure = result unknown → InternalError
    // (§3.5 no-blind-retry rule)
    ReplyPtr exec(const std::vector<std::string>& args, bool read_retry);
    // After a successful commit-class command, WAIT on the same connection (§6, when
    // wait_replicas > 0); insufficient replicas only WARN — the write already took effect on the
    // primary, and erroring would mislead the client into retrying. Returns false = connection bad (discard)
    bool wait_for_replicas(Conn& c);
    // CAS retry backoff (§3.2): attempt > 0 counts toward the retry metric
    void cas_backoff(int attempt);
    // EVALSHA + NOSCRIPT self-heal (the script definitively did not run; reload-and-resend is safe, §3.5)
    ReplyPtr eval(const std::string& sha, const char* body, std::vector<std::string> keys,
                  std::vector<std::string> argv, bool read_retry);

    // ---- Key construction (§2.2; prefix + '\0'-separated compound segments) ----
    std::string key(std::string_view suffix) const;
    std::string buckets_key() const;
    std::string objects_key(std::string_view b) const;   // o:<b>   HASH
    std::string zindex_key(std::string_view b) const;    // oz:<b>  ZSET
    std::string uploads_key(std::string_view b) const;   // up:<b>  HASH
    std::string uploads_zkey(std::string_view b) const;  // uz:<b>  ZSET (lex index over up:<b> fields, roadmap §3.5)
    // Full HSCAN of up:<b> plus reconciliation of uz:<b> against it (legacy tables written
    // before the index existed, or by an older gateway sharing the meta). Returns everything
    std::vector<UploadInfo> list_uploads_rebuild(std::string_view b);
    std::string parts_key(std::string_view b, std::string_view k, std::string_view id) const;
    std::string refs_key() const;
    std::string gcq_key() const;
    std::string pack_key(uint64_t pack_id) const;  // pack:<id> HASH (§2.2 pack liveness accounting)

    // ---- High-level helpers ----
    void require_bucket(std::string_view b);  // missing → NoSuchBucket (read-only precheck)
    std::optional<std::string> hget_raw(const std::string& k, std::string_view field);
    std::optional<std::string> upload_raw(std::string_view b, std::string_view k,
                                          std::string_view id);
    uint64_t alloc_id(std::string_view counter_suffix, IdRange& r, uint32_t n = 1);
    // gcq enqueue (§2.2): member = be64(seq) ‖ encode_reclaim; seq pre-allocation keeps the script deterministic
    void enqueue_reclaim(RedisBatch& bt, const DataRef& ref, ReclaimReason reason);
    void batch_refs(RedisBatch& bt, const DataRef& ref, bool add, std::string_view owner);
    // Maintains the pack liveness account in the same batch (pack:<id> HINCRBY, §2.2). Separate
    // from batch_refs: complete's refs transfer (owner rewrite) must be a no-op for packs —
    // merging them would double-count.
    // rec_overhead: per-record header overhead (codec::pack_rec_overhead*); live_bytes uses the
    // same accounting basis as file_size (docs/archive/gaps.md §2.3a)
    void batch_pack_delta(RedisBatch& bt, const DataRef& ref, int sign, int64_t rec_overhead);
    // Read the parts HASH: raw values (for the sha1 fingerprint) + decoded records, ascending by part_no
    std::vector<std::pair<std::string, PartRec>> scan_parts(std::string_view b,
                                                            std::string_view k,
                                                            std::string_view id);

    RedisMetaOptions opt_;
    // Parsed connection address (parsed once at construction)
    std::string host_;
    int port_ = 6379;
    std::string unix_path_;
    std::string password_;
    int db_ = 0;
    // Script SHAs (SCRIPT LOAD at construction; content-addressed, reloading yields the same value)
    std::string sha_commit_;
    std::string sha_list_;

    std::mutex pool_mu_;
    std::vector<std::unique_ptr<Conn>> idle_;
    bool closed_ = false;

    // Separate small lock for id-segment handout (alloc is called on the data plane whenever a chunk opens; must not queue behind business commits)
    std::mutex alloc_mu_;
    IdRange file_ids_[2];  // indexed by Extent::Kind
    IdRange seqs_;         // gcq seq

    // Invalidation subscriber (backlog-sequence ⑤)
    void subscriber_loop();
    std::string origin_;  // random per-store id stamped into published invalidations
    std::thread sub_thread_;
    std::atomic<bool> sub_stop_{false};
    InvalidationSink sub_on_key_;
    std::function<void()> sub_on_reset_;
    // R4 metrics (registered at construction; zero values visible)
    std::shared_ptr<MetricCounter> m_cas_retries_;
    std::shared_ptr<MetricCounter> m_reconnects_;
    std::shared_ptr<MetricCounter> m_sub_reconnects_;
};

}  // namespace lights3::storage::duostore
