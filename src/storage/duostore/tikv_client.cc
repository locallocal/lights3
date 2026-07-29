// L3: TiKV 客户端侧车实现（docs/duostore-tikv-meta.md §6.3）。
// 提交器骨架改写自 client-c 的 kv/2pc.cc（Apache-2.0，tikv/client-c@78a557e）：
// 区域分组/分批、prewrite 锁解析、region 错误重试均保持同构；差异点——
//   1. mutation 带 op（Put/Del/Lock/Insert），上游恒 Put；
//   2. AlreadyExist / WriteConflict 透出为结构化异常（上游笼统 LogicalError/Unknown）；
//   3. commit 显式两分支：primary 明确拒绝 = 已回滚（安全重试）、RPC 层异常 =
//      结果不明（TikvUndetermined，§4.6 盲重试禁令）；上游此处是 TODO；
//   4. prewrite 失败后 best-effort BatchRollback 清锁（上游 TODO），缩短并发方
//      在残锁上的等待（提交点 TSO 取号失败同受此保护）；失败无害——残锁由
//      TTL/读者 LockResolver 收敛；
//   5. commit 的 ts 纪律：primary 阶段区域重试前重取 TSO、CommitTsExpired 原地
//      换号重试（读者推高 min_commit_ts 的正常路径）；secondaries 恒用 primary
//      定格的 commit_ts——上游对 secondary 也重取 TSO，会造成主从 commit_ts
//      分叉，不跟随；
//   6. 同 key 多 mutation 构造期合并（后者胜，对齐 WriteBatch 按序覆盖语义）；
//   7. "更新乐观事务的活锁"在 prewrite 错误处先行判定为冲突，不依赖上游
//      resolveLocksForWrite 的裸 Exception("write conflict") 消息串（保留消息
//      串匹配作纵深兜底）；
//   8. 增补 batch_get（KvBatchGet）与 last_key（key-only 反向扫）两个读原语，
//      上游 Snapshot/Scanner 未覆盖。
#include "storage/duostore/tikv_client.h"

#include <Poco/AutoPtr.h>
#include <Poco/Channel.h>
#include <Poco/Logger.h>
#include <Poco/Message.h>
#include <Poco/URI.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include <kvproto/pdpb.grpc.pb.h>
#include <pingcap/Exception.h>
#include <pingcap/kv/Backoff.h>
#include <pingcap/kv/Cluster.h>
#include <pingcap/kv/LockResolver.h>
#include <pingcap/kv/RegionClient.h>
#include <pingcap/kv/Scanner.h>
#include <pingcap/kv/Snapshot.h>

#include <algorithm>
#include <chrono>
#include <cmath>
#include <mutex>
#include <tuple>
#include <unordered_map>

#include "core/log.h"

namespace lights3::storage::duostore {

namespace {

using pingcap::Exception;
using pingcap::kv::Backoffer;
using pingcap::kv::Cluster;
using pingcap::kv::RegionClient;
using pingcap::kv::RegionVerID;

// 与 client-c txnCommitBatchSize 一致：单区域批的 key+value 字节上界
constexpr uint64_t kTxnCommitBatchSize = 16 * 1024;

// lock_ttl 伸缩上限（上游 2pc.cc managedLockTTL，未导出故复制）：大事务 prewrite
// 分批串行发送耗时上升，TTL 随之放大，否则读者会把仍在 prewrite 的 primary 判死
// 回滚（§6.3 万分片 complete 专项）
constexpr uint64_t kManagedLockTTL = 20000;

// 事务 lock_ttl（上游 txnLockTTL 的侧车版）：小事务恒 defaultLockTTL(3s)；
// 超过单批字节上界的按 ttlFactor·√MiB 放大、夹在 [3s, 20s]。与上游差异：不加
// elapsed 项——我们在组批后立即提交，无 TiDB 式"攒 buffer 再 commit"的间隔
uint64_t txn_lock_ttl(uint64_t mutation_bytes) {
    if (mutation_bytes < kTxnCommitBatchSize) return pingcap::kv::defaultLockTTL;
    uint64_t mb = std::max<uint64_t>(mutation_bytes >> 20, 1);
    auto ttl = uint64_t(double(pingcap::kv::ttlFactor) * std::sqrt(double(mb)));
    return std::clamp(ttl, pingcap::kv::defaultLockTTL, kManagedLockTTL);
}

// Poco 日志桥（§6.2，T5）：client-c 全部经 Poco::Logger 输出，默认 ConsoleChannel
// 自带格式与 spdlog 两张皮。root logger 换成本桥后，其余 pingcap.* logger 沿
// 继承链汇入统一 stderr 格式（源名保留为消息前缀）。须在首个 pingcap logger
// 创建（= 首个 Cluster 构造）之前安装——Poco 子 logger 在创建时继承 channel
class PocoSpdlogChannel : public Poco::Channel {
public:
    void log(const Poco::Message& m) override {
        switch (m.getPriority()) {
            case Poco::Message::PRIO_FATAL:
            case Poco::Message::PRIO_CRITICAL:
            case Poco::Message::PRIO_ERROR:
                LOG_ERROR("{}: {}", m.getSource(), m.getText());
                break;
            case Poco::Message::PRIO_WARNING:
                LOG_WARN("{}: {}", m.getSource(), m.getText());
                break;
            case Poco::Message::PRIO_NOTICE:
            case Poco::Message::PRIO_INFORMATION:
                LOG_INFO("{}: {}", m.getSource(), m.getText());
                break;
            default:
                LOG_DEBUG("{}: {}", m.getSource(), m.getText());
        }
    }
};

void bridge_poco_logs_once() {
    static std::once_flag once;
    std::call_once(once, [] {
        Poco::Logger::root().setChannel(Poco::AutoPtr<Poco::Channel>(new PocoSpdlogChannel));
        // information 起送：pingcap 的 debug 噪声在源头掐掉（省 format 开销）；
        // information 及以上进 spdlog 后仍受全局日志级别二次过滤
        Poco::Logger::root().setLevel(Poco::Message::PRIO_INFORMATION);
    });
}

::kvrpcpb::Op to_pb_op(TikvOp op) {
    switch (op) {
        case TikvOp::kPut: return ::kvrpcpb::Put;
        case TikvOp::kDel: return ::kvrpcpb::Del;
        case TikvOp::kLock: return ::kvrpcpb::Lock;
        case TikvOp::kInsert: return ::kvrpcpb::Insert;
    }
    __builtin_unreachable();
}

struct Batch {
    RegionVerID region;
    std::vector<std::string> keys;
};

class Committer {
public:
    Committer(Cluster* cluster, uint64_t start_ts, const std::vector<TikvMutation>& muts,
              int backoff_budget_ms)
        : cluster_(cluster), start_ts_(start_ts) {
        keys_.reserve(muts.size());
        uint64_t bytes = 0;
        for (const auto& m : muts) {
            // 同 key 多条 mutation：后者胜（WriteBatch 按序覆盖语义；Percolator
            // 每 key 只发一个 op）。keys_ 保持首现序去重——primary 仍是首 key
            auto [it, inserted] = by_key_.try_emplace(m.key, &m);
            if (inserted)
                keys_.push_back(m.key);
            else
                it->second = &m;
        }
        for (auto& [k, m] : by_key_) bytes += k.size() + m->value.size();
        lock_ttl_ = txn_lock_ttl(bytes);
        primary_ = keys_.front();
        // 预算参数化（§6.1，T5）：commit 用 2× 对齐上游 commit:prewrite ≈ 2:1
        prewrite_budget_ = backoff_budget_ms > 0 ? backoff_budget_ms
                                                 : pingcap::kv::prewriteMaxBackoff;
        commit_budget_ = backoff_budget_ms > 0 ? 2 * backoff_budget_ms
                                               : pingcap::kv::commitMaxBackoff;
    }

    void execute() {
        try {
            Backoffer bo(prewrite_budget_);
            prewrite_keys(bo, keys_);
            // 提交点 TSO 取号也在清锁保护内：PD 故障 = 明确未提交，残锁必须清
            //（否则 delete_bucket 等宽事务的守卫锁会卡住同桶并发写整个 TTL）
            commit_ts_ = cluster_->pd_client->getTS();
        } catch (...) {
            cleanup_locks();
            throw;
        }
        // ---- 提交点：primary 单独一批（§4.6）----
        try {
            Backoffer bo(commit_budget_);
            commit_keys(bo, {primary_}, /*primary_phase=*/true);
        } catch (TikvConflict&) {
            // TiKV 对已提交事务的 commit 幂等返回 ok，明确拒绝 = 已被回滚——安全重试
            throw;
        } catch (Exception& e) {
            throw TikvUndetermined{e.displayText()};
        }
        // ---- primary 落地即事务成功；secondaries 失败仅延迟收敛（读者经
        // LockResolver 回查 primary），吞掉并告警 ----
        if (keys_.size() > 1) {
            try {
                Backoffer bo(commit_budget_);
                commit_keys(bo, {keys_.begin() + 1, keys_.end()}, /*primary_phase=*/false);
            } catch (const TikvConflict& c) {
                LOG_WARN("tikv: secondary commit rejected (resolves lazily): {}", c.what);
            } catch (const Exception& e) {
                LOG_WARN("tikv: secondary commit failed (resolves lazily): {}", e.displayText());
            }
        }
    }

private:
    // 区域分组 + 字节上界分批（照搬 client-c doActionOnKeys 的形态）
    std::vector<Batch> make_batches(Backoffer& bo, const std::vector<std::string>& keys,
                                    bool with_values) {
        auto [groups, first_region] = cluster_->region_cache->groupKeysByRegion(bo, keys);
        std::ignore = first_region;
        std::vector<Batch> batches;
        for (auto& [region, group_keys] : groups) {
            uint64_t size = 0;
            std::vector<std::string> sub;
            for (auto& k : group_keys) {
                uint64_t s = k.size() + (with_values ? by_key_.at(k)->value.size() : 0);
                if (!sub.empty() && size + s > kTxnCommitBatchSize) {
                    batches.push_back(Batch{region, std::move(sub)});
                    sub.clear();
                    size = 0;
                }
                sub.push_back(k);
                size += s;
            }
            if (!sub.empty()) batches.push_back(Batch{region, std::move(sub)});
        }
        return batches;
    }

    void prewrite_keys(Backoffer& bo, const std::vector<std::string>& keys) {
        for (auto& b : make_batches(bo, keys, /*with_values=*/true)) prewrite_batch(bo, b);
    }

    void prewrite_batch(Backoffer& bo, const Batch& batch) {
        for (;;) {
            ::kvrpcpb::PrewriteRequest req;
            for (auto& k : batch.keys) {
                const TikvMutation& m = *by_key_.at(k);
                auto* mu = req.add_mutations();
                mu->set_op(to_pb_op(m.op));
                mu->set_key(k);
                if (m.op == TikvOp::kPut || m.op == TikvOp::kInsert) mu->set_value(m.value);
            }
            req.set_primary_lock(primary_);
            req.set_start_version(start_ts_);
            req.set_lock_ttl(lock_ttl_);  // 事务规模伸缩（txn_lock_ttl，§6.3）
            req.set_txn_size(keys_.size());
            req.set_min_commit_ts(start_ts_ + 1);

            ::kvrpcpb::PrewriteResponse resp;
            RegionClient rc(cluster_, batch.region);
            try {
                rc.sendReqToRegion<pingcap::kv::RPC_NAME(KvPrewrite)>(bo, req, &resp);
            } catch (Exception& e) {
                // region 级错误（split/迁移）：退避后重取路由、按新分组重做本批
                bo.backoff(pingcap::kv::boRegionMiss, e);
                prewrite_keys(bo, batch.keys);
                return;
            }
            if (resp.errors_size() == 0) return;
            std::vector<pingcap::kv::LockPtr> locks;
            for (const auto& err : resp.errors()) {
                if (err.has_already_exist()) throw TikvAlreadyExist{err.already_exist().key()};
                if (err.has_conflict()) throw TikvConflict{err.conflict().ShortDebugString()};
                if (!err.retryable().empty()) throw TikvConflict{err.retryable()};
                auto lock = pingcap::kv::extractLockFromKeyErr(err);  // 未知错误内部抛
                // 更新乐观事务的活锁 = resolveLocksForWrite 的必抛条件（上游以裸
                // Exception("write conflict") 表达，LockResolver.cc 自注 TODO）——
                // 先行判定，不把分类押在消息串上。对方若实已死亡：本轮多退避一次，
                // 重试取到的新 start_ts 必大于其 txn_id（其为过去时刻的 TSO），
                // 走 resolver 清理路径推进，收敛安全
                if (lock->lock_type != ::kvrpcpb::PessimisticLock && lock->txn_id > start_ts_)
                    throw TikvConflict{"blocked by newer optimistic txn " +
                                       std::to_string(lock->txn_id)};
                locks.push_back(std::move(lock));
            }
            int64_t before_expired = 0;
            try {
                before_expired = cluster_->lock_resolver->resolveLocksForWrite(bo, start_ts_, locks);
            } catch (Exception& e) {
                // 上游对"更新事务的活锁"抛裸 Exception("write conflict")（LockResolver.cc
                // 自注 TODO，无结构化错误码，@78a557e 消息稳定）——prewrite 阶段明确
                // 未提交，归入冲突重试
                if (e.displayText().find("write conflict") != std::string::npos)
                    throw TikvConflict{e.displayText()};
                throw;
            }
            if (before_expired > 0) {
                bo.backoffWithMaxSleep(
                    pingcap::kv::boTxnLock, static_cast<int>(before_expired),
                    Exception("prewrite blocked by " + std::to_string(locks.size()) + " locks",
                              pingcap::ErrorCodes::LockError));
            }
        }
    }

    void commit_keys(Backoffer& bo, const std::vector<std::string>& keys, bool primary_phase) {
        for (auto& b : make_batches(bo, keys, /*with_values=*/false))
            commit_batch(bo, b, primary_phase);
    }

    void commit_batch(Backoffer& bo, const Batch& batch, bool primary_phase) {
        // primary 提交的 commit_ts 换号重试上限：CommitTsExpired 由并发读者推高
        // min_commit_ts 触发，换新 TSO 即消解；上限防读侧持续推高下的原地打转
        //（超限=明确未提交，抛冲突走整事务重试）
        constexpr int kMaxTsRefresh = 4;
        for (int ts_refresh = 0;; ++ts_refresh) {
            ::kvrpcpb::CommitRequest req;
            for (auto& k : batch.keys) req.add_keys(k);
            req.set_start_version(start_ts_);
            req.set_commit_version(commit_ts_);

            ::kvrpcpb::CommitResponse resp;
            RegionClient rc(cluster_, batch.region);
            try {
                rc.sendReqToRegion<pingcap::kv::RPC_NAME(KvCommit)>(bo, req, &resp);
            } catch (Exception& e) {
                bo.backoff(pingcap::kv::boRegionMiss, e);
                // primary 重放前重取 TSO：退避期间读者可能已推高 min_commit_ts，旧
                // ts 重放会遭 CommitTsExpired。重放安全：commit 对 start_ts 幂等——
                // 先前尝试若已生效，TiKV 按既有 write 记录直接返回 ok，换 ts 无碍。
                // secondaries 恒用 primary 定格的 commit_ts_（上游对 secondary 也
                // 重取 TSO，会造成主从 commit_ts 分叉，属其 test-grade 缺陷，不跟随）
                if (primary_phase) commit_ts_ = cluster_->pd_client->getTS();
                commit_keys(bo, batch.keys, primary_phase);
                return;
            }
            if (!resp.has_error()) return;
            if (primary_phase && resp.error().has_commit_ts_expired() &&
                ts_refresh < kMaxTsRefresh) {
                commit_ts_ = cluster_->pd_client->getTS();  // 取号自身延迟即节流
                continue;
            }
            throw TikvConflict{resp.error().ShortDebugString()};
        }
    }

    // prewrite 失败后的 best-effort 清锁（文件头差异 4）。对未曾 prewrite 的 key
    // 多写 Rollback 墓碑无害（本 start_ts 不复用）
    void cleanup_locks() noexcept {
        try {
            Backoffer bo(prewrite_budget_);
            for (auto& b : make_batches(bo, keys_, /*with_values=*/false)) {
                ::kvrpcpb::BatchRollbackRequest req;
                req.set_start_version(start_ts_);
                for (auto& k : b.keys) req.add_keys(k);
                ::kvrpcpb::BatchRollbackResponse resp;
                RegionClient rc(cluster_, b.region);
                rc.sendReqToRegion<pingcap::kv::RPC_NAME(KvBatchRollback)>(bo, req, &resp);
            }
        } catch (...) {
            // 清不掉就交给 TTL / 读者 LockResolver
        }
    }

    Cluster* cluster_;
    uint64_t start_ts_;
    uint64_t commit_ts_ = 0;
    uint64_t lock_ttl_ = pingcap::kv::defaultLockTTL;
    int prewrite_budget_ = pingcap::kv::prewriteMaxBackoff;
    int commit_budget_ = pingcap::kv::commitMaxBackoff;
    std::vector<std::string> keys_;
    std::unordered_map<std::string, const TikvMutation*> by_key_;
    std::string primary_;
};

}  // namespace

struct TikvClient::Impl {
    std::unique_ptr<Cluster> cluster;
    pingcap::ClusterConfig cluster_cfg;  // safepoint 直连通道复用同一 TLS 配置
    int backoff_budget_ms = 0;

    // ---- GC safepoint 的 PD leader 直连 stub（§7.3）----
    // client-c 的 PDConnClient/stub 全私有，safepoint 三件未封装——侧车自建
    // channel 到 getLeaderUrl()（公开接口）。惰建缓存；RPC 报错或 leader 变更
    // 即弃缓存重建（下次调用重解析 leader）。低频运维路径，锁全程无妨
    std::mutex pd_mu;
    std::string pd_url;
    std::unique_ptr<::pdpb::PD::Stub> pd_stub;

    template <typename Resp, typename Rpc>
    Resp pd_call(const char* what, const Rpc& rpc) {
        std::lock_guard lk(pd_mu);
        std::string url = cluster->pd_client->getLeaderUrl();
        if (!pd_stub || url != pd_url) {
            Poco::URI uri(url);  // leader URL 形如 http(s)://host:port，grpc 只要 authority
            auto creds = cluster_cfg.hasTlsConfig()
                             ? grpc::SslCredentials(cluster_cfg.getGrpcCredentials())
                             : grpc::InsecureChannelCredentials();
            pd_stub = ::pdpb::PD::NewStub(grpc::CreateChannel(uri.getAuthority(), creds));
            pd_url = url;
        }
        grpc::ClientContext ctx;
        ctx.set_deadline(std::chrono::system_clock::now() + std::chrono::seconds(10));
        Resp resp;
        grpc::Status st = rpc(&ctx, *pd_stub, &resp);
        if (!st.ok()) {
            pd_stub.reset();
            throw Exception(std::string(what) + " failed: " + std::to_string(st.error_code()) +
                                ": " + st.error_message(),
                            pingcap::ErrorCodes::GRPCErrorCode);
        }
        if (resp.header().has_error()) {  // 非 leader 等 PD 层错误
            pd_stub.reset();
            throw Exception(std::string(what) + " rejected: " + resp.header().error().message(),
                            pingcap::ErrorCodes::UnknownError);
        }
        return resp;
    }

    ::pdpb::RequestHeader* pd_header() {
        auto* h = new ::pdpb::RequestHeader();  // set_allocated_* 接管所有权
        h->set_cluster_id(cluster->pd_client->getClusterID());
        return h;
    }
};

TikvClient::TikvClient(const TikvOptions& opt) : impl_(std::make_unique<Impl>()) {
    bridge_poco_logs_once();  // 先于首个 pingcap logger 创建（注释见桥定义）
    pingcap::ClusterConfig cfg;
    cfg.ca_path = opt.ca_path;
    cfg.cert_path = opt.cert_path;
    cfg.key_path = opt.key_path;
    impl_->cluster_cfg = cfg;
    impl_->backoff_budget_ms = opt.backoff_budget_ms;
    impl_->cluster = std::make_unique<Cluster>(opt.pd_endpoints, cfg);
}

// Cluster 析构自会停 rpc/region cache/后台线程（client-c Cluster::~Cluster）
TikvClient::~TikvClient() = default;

uint64_t TikvClient::get_ts() { return impl_->cluster->pd_client->getTS(); }

std::optional<std::string> TikvClient::get(uint64_t version, const std::string& key) {
    pingcap::kv::Snapshot snap(impl_->cluster.get(), version);
    std::string v = snap.Get(key);
    if (v.empty()) return std::nullopt;  // codec 值恒非空，空串即缺（§3.1）
    return v;
}

std::vector<std::pair<std::string, std::string>> TikvClient::scan(uint64_t version,
                                                                  const std::string& begin,
                                                                  const std::string& end,
                                                                  size_t limit) {
    pingcap::kv::Snapshot snap(impl_->cluster.get(), version);
    // limit 下推为批大小（Snapshot::Scan 恒 256）：limit=1 的存在性探测只拉 1 条
    int batch = int(std::min<size_t>(std::max<size_t>(limit, 1), 1024));
    pingcap::kv::Scanner scanner(snap, begin, end, batch);
    std::vector<std::pair<std::string, std::string>> out;
    while (scanner.valid && out.size() < limit) {
        out.emplace_back(scanner.key(), scanner.value());
        scanner.next();
    }
    return out;
}

std::vector<std::optional<std::string>> TikvClient::batch_get(
    uint64_t version, const std::vector<std::string>& keys) {
    using pingcap::kv::LockPtr;
    std::unordered_map<std::string, std::string> found;
    std::vector<std::string> pending = keys;
    Backoffer bo(impl_->backoff_budget_ms > 0 ? impl_->backoff_budget_ms
                                              : pingcap::kv::GetMaxBackoff);
    while (!pending.empty()) {
        auto [groups, first_region] = impl_->cluster->region_cache->groupKeysByRegion(bo, pending);
        std::ignore = first_region;
        std::vector<std::string> retry;
        for (auto& [region, group_keys] : groups) {
            ::kvrpcpb::BatchGetRequest req;
            for (auto& k : group_keys) req.add_keys(k);
            req.set_version(version);
            ::kvrpcpb::BatchGetResponse resp;
            RegionClient rc(impl_->cluster.get(), region);
            try {
                rc.sendReqToRegion<pingcap::kv::RPC_NAME(KvBatchGet)>(bo, req, &resp);
            } catch (Exception& e) {
                bo.backoff(pingcap::kv::boRegionMiss, e);  // region 变化：下轮重分组
                retry.insert(retry.end(), group_keys.begin(), group_keys.end());
                continue;
            }
            if (resp.has_error()) {
                // 响应级锁错误：pairs 为空，解析后整组重做（proto 注释语义）
                std::vector<LockPtr> locks{pingcap::kv::extractLockFromKeyErr(resp.error())};
                std::vector<uint64_t> pushed;
                auto ms = impl_->cluster->lock_resolver->resolveLocks(bo, version, locks, pushed);
                if (ms > 0)
                    bo.backoffWithMaxSleep(
                        pingcap::kv::boTxnLockFast, static_cast<int>(ms),
                        Exception("batch_get blocked by lock", pingcap::ErrorCodes::LockError));
                retry.insert(retry.end(), group_keys.begin(), group_keys.end());
                continue;
            }
            pingcap::kv::Snapshot snap(impl_->cluster.get(), version);
            for (int i = 0; i < resp.pairs_size(); ++i) {
                const auto& pair = resp.pairs(i);
                if (pair.has_error()) {
                    // 单 key 带锁：退化为 Get 兜底（内部解析锁；Scanner 同款手法）
                    auto lock = pingcap::kv::extractLockFromKeyErr(pair.error());
                    found[lock->key] = snap.Get(bo, lock->key);
                } else {
                    found[pair.key()] = pair.value();
                }
            }
        }
        pending = std::move(retry);
    }
    std::vector<std::optional<std::string>> out;
    out.reserve(keys.size());
    for (auto& k : keys) {
        auto it = found.find(k);
        // 缺失或空串（codec 值恒非空）都视作不存在，与 get() 同一消歧
        if (it == found.end() || it->second.empty()) out.emplace_back(std::nullopt);
        else out.emplace_back(it->second);
    }
    return out;
}

std::optional<std::string> TikvClient::last_key(uint64_t version, const std::string& lo,
                                                const std::string& hi) {
    using pingcap::kv::KeyLocation;
    using pingcap::kv::LockPtr;
    Backoffer bo(impl_->backoff_budget_ms > 0 ? impl_->backoff_budget_ms
                                              : pingcap::kv::scanMaxBackoff);
    for (;;) {  // 外层：region 拓扑变化时整体重来
        // 前向行走收集 [lo, hi) 覆盖的 region（cache 命中为主，零数据传输），
        // 再自尾向前逐 region 反向扫 limit=1——绕开"按上界定位前驱 region"
        // 这一 client-c 未提供的原语
        std::vector<KeyLocation> regions;
        std::string cur = lo;
        for (;;) {
            auto loc = impl_->cluster->region_cache->locateKey(bo, cur);
            bool covers_hi = loc.end_key.empty() || loc.end_key >= hi;
            regions.push_back(std::move(loc));
            if (covers_hi) break;
            cur = regions.back().end_key;
        }
        bool topo_changed = false;
        for (auto it = regions.rbegin(); it != regions.rend() && !topo_changed; ++it) {
            // 与 [lo, hi) 的交集（start_key 空 = 键空间起点，恒 < lo）
            std::string r_hi = (it->end_key.empty() || it->end_key >= hi) ? hi : it->end_key;
            std::string r_lo = it->start_key < lo ? lo : it->start_key;
            for (;;) {  // 本 region 的锁解析重试
                ::kvrpcpb::ScanRequest req;
                req.set_start_key(r_hi);  // reverse：扫 [end_key, start_key) 降序
                req.set_end_key(r_lo);
                req.set_limit(1);
                req.set_version(version);
                req.set_key_only(true);
                req.set_reverse(true);
                ::kvrpcpb::ScanResponse resp;
                RegionClient rc(impl_->cluster.get(), it->region);
                try {
                    rc.sendReqToRegion<pingcap::kv::RPC_NAME(KvScan)>(bo, req, &resp);
                } catch (Exception& e) {
                    bo.backoff(pingcap::kv::boRegionMiss, e);
                    topo_changed = true;
                    break;
                }
                if (resp.has_error()) {
                    std::vector<LockPtr> locks{pingcap::kv::extractLockFromKeyErr(resp.error())};
                    std::vector<uint64_t> pushed;
                    auto ms =
                        impl_->cluster->lock_resolver->resolveLocks(bo, version, locks, pushed);
                    if (ms > 0)
                        bo.backoffWithMaxSleep(pingcap::kv::boTxnLockFast, static_cast<int>(ms),
                                               Exception("last_key blocked by lock",
                                                         pingcap::ErrorCodes::LockError));
                    continue;
                }
                if (resp.pairs_size() == 0) break;  // 本 region 交集无 key，试前一 region
                const auto& pair = resp.pairs(0);
                if (pair.has_error()) {
                    // 尾 key 带锁（可能是未提交插入）：解析后重扫本 region
                    std::vector<LockPtr> locks{pingcap::kv::extractLockFromKeyErr(pair.error())};
                    std::vector<uint64_t> pushed;
                    auto ms =
                        impl_->cluster->lock_resolver->resolveLocks(bo, version, locks, pushed);
                    if (ms > 0)
                        bo.backoffWithMaxSleep(pingcap::kv::boTxnLockFast, static_cast<int>(ms),
                                               Exception("last_key blocked by lock",
                                                         pingcap::ErrorCodes::LockError));
                    continue;
                }
                return pair.key();
            }
        }
        if (!topo_changed) return std::nullopt;
    }
}

void TikvClient::commit(uint64_t start_ts, const std::vector<TikvMutation>& muts) {
    if (muts.empty()) return;
    Committer(impl_->cluster.get(), start_ts, muts, impl_->backoff_budget_ms).execute();
}

// ---------- GC safepoint（§7.3）----------

uint64_t TikvClient::update_service_gc_safepoint(const std::string& service_id, int64_t ttl_s,
                                                 uint64_t safe_point) {
    ::pdpb::UpdateServiceGCSafePointRequest req;
    req.set_allocated_header(impl_->pd_header());
    req.set_service_id(service_id);
    req.set_ttl(ttl_s);
    req.set_safe_point(safe_point);
    auto resp = impl_->pd_call<::pdpb::UpdateServiceGCSafePointResponse>(
        "update_service_gc_safepoint",
        [&](grpc::ClientContext* ctx, ::pdpb::PD::Stub& stub,
            ::pdpb::UpdateServiceGCSafePointResponse* r) {
            return stub.UpdateServiceGCSafePoint(ctx, req, r);
        });
    return resp.min_safe_point();
}

uint64_t TikvClient::update_gc_safepoint(uint64_t safe_point) {
    ::pdpb::UpdateGCSafePointRequest req;
    req.set_allocated_header(impl_->pd_header());
    req.set_safe_point(safe_point);
    auto resp = impl_->pd_call<::pdpb::UpdateGCSafePointResponse>(
        "update_gc_safepoint",
        [&](grpc::ClientContext* ctx, ::pdpb::PD::Stub& stub,
            ::pdpb::UpdateGCSafePointResponse* r) { return stub.UpdateGCSafePoint(ctx, req, r); });
    return resp.new_safe_point();
}

uint64_t TikvClient::get_gc_safepoint() {
    // 不走 client-c getGCSafePoint()（已标 deprecated，且异常路径翻转其
    // check_leader 内部状态）；与 update 两件同一直连通道
    ::pdpb::GetGCSafePointRequest req;
    req.set_allocated_header(impl_->pd_header());
    auto resp = impl_->pd_call<::pdpb::GetGCSafePointResponse>(
        "get_gc_safepoint",
        [&](grpc::ClientContext* ctx, ::pdpb::PD::Stub& stub, ::pdpb::GetGCSafePointResponse* r) {
            return stub.GetGCSafePoint(ctx, req, r);
        });
    return resp.safe_point();
}

pingcap::kv::Cluster* TikvClient::cluster() { return impl_->cluster.get(); }

}  // namespace lights3::storage::duostore
