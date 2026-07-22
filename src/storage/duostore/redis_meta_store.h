// L3: IMetaStore 的 Redis 实现（docs/duostore-redis-meta.md）。
// 提交类操作 = 一个通用守卫式提交 Lua 脚本（check-and-commit）+ 客户端乐观 CAS
// 重试（§3.2）；脚本在 Redis 服务端单线程原子执行——脚本原子性即全局原子性，
// 多网关共享同一 redis 即共享 meta，不持业务互斥（§3.4，仅保留号段派发小锁）。
// value 编码 100% 复用 codec.cc（§2.1）；不支持 Redis Cluster（§1 非目标）。
#pragma once

#include <cstdint>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

#include "storage/duostore/meta_store.h"

struct redisReply;

namespace lights3::storage::duostore {

struct RedisMetaOptions {
    std::string uri;              // redis://[:pass@]host[:port][/db] 或 unix://<path>
    std::string prefix = "duo:";  // 全部 key 的前缀（§2.1 多实例/测试隔离）
    int timeout_ms = 3000;        // 建连 + 单命令超时（§5.4）
    int pool_size = 8;            // 连接池大小（§5.2）
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
    void put_object(std::string_view b, std::string_view k, ObjectRec rec) override;
    bool delete_object(std::string_view b, std::string_view k) override;
    ListResult list_objects(std::string_view b, const ListOptions& opt) override;

    std::string create_upload(std::string_view b, std::string_view k, ObjectMeta meta) override;
    UploadRec require_upload(std::string_view b, std::string_view k,
                             std::string_view id) override;
    void put_part(std::string_view b, std::string_view k, std::string_view id,
                  PartRec p) override;
    std::vector<PartRec> list_parts(std::string_view b, std::string_view k,
                                    std::string_view id) override;
    std::vector<UploadInfo> list_uploads(std::string_view b) override;
    std::string complete_upload(std::string_view b, std::string_view k, std::string_view id,
                                std::span<const PartInfo> parts) override;
    void abort_upload(std::string_view b, std::string_view k, std::string_view id) override;

    uint64_t alloc_file_id(Extent::Kind kind) override;
    std::vector<std::pair<uint64_t, Reclaim>> peek_reclaims(size_t max) override;
    void ack_reclaim(uint64_t seq) override;
    std::vector<PackStat> pack_stats() override;
    bool swap_extents(std::string_view b, std::string_view k, uint64_t expect_version,
                      const DataRef& from, const DataRef& to) override;
    bool chunk_referenced(uint64_t file_id) override;
    void close() override;

private:
    friend class RedisBatch;

    struct Conn;  // hiredis 连接（.cc 内定义，头文件不泄漏 hiredis 类型）
    using ReplyPtr = RedisReplyPtr;

    // 号段预留（§4，与 RocksMetaStore 同构）：INCRBY 一次 +kIdSegment、内存派发。
    // burned：首次预留额外空烧一个号段，跳过 AOF everysec 丢失窗口内可能已派发的 id
    struct IdRange {
        uint64_t next = 0, limit = 0;
        bool burned = false;
    };
    static constexpr uint64_t kIdSegment = 4096;

    // ---- 连接池（§5.2）：mutex 保护的空闲栈，RAII 取还 ----
    std::unique_ptr<Conn> acquire();
    void release(std::unique_ptr<Conn> c);
    std::unique_ptr<Conn> make_conn();  // 建连 + AUTH/SELECT（§5.4）

    // 命令执行（一律 redisCommandArgv，二进制安全，§5.1）。read_retry：纯读允许
    // 换新连接重试一次；提交类 IO 失败 = 结果不明 → InternalError（§3.5 盲重试禁令）
    ReplyPtr exec(const std::vector<std::string>& args, bool read_retry);
    // EVALSHA + NOSCRIPT 自愈（脚本明确未执行，重载后重发安全，§3.5）
    ReplyPtr eval(const std::string& sha, const char* body, std::vector<std::string> keys,
                  std::vector<std::string> argv, bool read_retry);

    // ---- key 构造（§2.2；prefix + '\0' 分隔复合段）----
    std::string key(std::string_view suffix) const;
    std::string buckets_key() const;
    std::string objects_key(std::string_view b) const;   // o:<b>   HASH
    std::string zindex_key(std::string_view b) const;    // oz:<b>  ZSET
    std::string uploads_key(std::string_view b) const;   // up:<b>  HASH
    std::string parts_key(std::string_view b, std::string_view k, std::string_view id) const;
    std::string refs_key() const;
    std::string gcq_key() const;

    // ---- 高层辅助 ----
    void require_bucket(std::string_view b);  // 缺 → NoSuchBucket（纯读预检）
    std::optional<std::string> hget_raw(const std::string& k, std::string_view field);
    std::optional<std::string> upload_raw(std::string_view b, std::string_view k,
                                          std::string_view id);
    uint64_t alloc_id(std::string_view counter_suffix, IdRange& r);
    // gcq 入账（§2.2）：member = be64(seq) ‖ encode_reclaim；seq 预派发保持脚本确定性
    void enqueue_reclaim(RedisBatch& bt, const DataRef& ref);
    void batch_refs(RedisBatch& bt, const DataRef& ref, bool add, std::string_view owner);
    // 读 parts HASH：raw value（sha1 指纹用）+ 解码记录，按 part_no 升序
    std::vector<std::pair<std::string, PartRec>> scan_parts(std::string_view b,
                                                            std::string_view k,
                                                            std::string_view id);

    RedisMetaOptions opt_;
    // 解析后的连接地址（构造时解析一次）
    std::string host_;
    int port_ = 6379;
    std::string unix_path_;
    std::string password_;
    int db_ = 0;
    // 脚本 SHA（构造时 SCRIPT LOAD；内容寻址，重载得到相同值）
    std::string sha_commit_;
    std::string sha_list_;

    std::mutex pool_mu_;
    std::vector<std::unique_ptr<Conn>> idle_;
    bool closed_ = false;

    // 号段派发独立小锁（alloc 在数据面每个 chunk 打开时调用，不排队业务提交）
    std::mutex alloc_mu_;
    IdRange file_ids_[2];  // 按 Extent::Kind 下标
    IdRange seqs_;         // gcq seq
};

}  // namespace lights3::storage::duostore
