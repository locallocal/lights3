// L3: TiKV 客户端侧车（docs/duostore-tikv-meta.md §6.3）。client-c 的事务提交层
// test-grade（mutation 只有 Put、commit 异常路径含 TODO），本文件以其公开的传输
// 基建（Cluster/RegionCache/RegionClient/Backoffer/LockResolver）为地基，实现带
// op（Put/Del/Lock/Insert）的乐观 2PC 提交器——不 fork submodule，保持上游 pristine；
// 待上游合入等价能力后本侧车退役。pingcap 头（拖 grpc/Poco）全部锁在 .cc。
#pragma once

#include <cstdint>
#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace pingcap::kv {
struct Cluster;
}

namespace lights3::storage::duostore {

struct TikvOptions {
    std::vector<std::string> pd_endpoints;  // "host:port" 列表
    // mTLS 三件套（可选，三者同时给定才启用，docs/duostore-tikv-meta.md §9）
    std::string ca_path;
    std::string cert_path;
    std::string key_path;
};

// mutation op（kvrpcpb::Op 子集，§4.4 用到的四种）
enum class TikvOp : uint8_t {
    kPut,
    kDel,
    kLock,    // 占位锁记录：物化只读前置条件的写偏斜冲突（§4.3 守卫分片）
    kInsert,  // put + 必须不存在（create_bucket → BucketAlreadyOwnedByYou）
};

struct TikvMutation {
    TikvOp op;
    std::string key;
    std::string value;  // kDel/kLock 恒空
};

// ---- 提交结果分类（§4.1/§4.6；meta store 依类别决定重试/映射）----
// WriteConflict / prewrite 锁竞争超预算：明确未提交，取新 start_ts 重读重试
struct TikvConflict {
    std::string what;
};
// Op::Insert 撞已存在 key：明确未提交（唯一使用点 create_bucket）
struct TikvAlreadyExist {
    std::string key;
};
// primary commit 结果不明（盲重试禁令，§4.6）：一律上抛 InternalError
struct TikvUndetermined {
    std::string what;
};

class TikvClient {
public:
    explicit TikvClient(const TikvOptions& opt);
    ~TikvClient();
    TikvClient(const TikvClient&) = delete;
    TikvClient& operator=(const TikvClient&) = delete;

    // PD TSO（事务 start_ts / list 快照版本）
    uint64_t get_ts();

    // 单 key 快照读；不存在返回 nullopt（client-c Get 以空串表缺，codec 值恒
    // 非空使该消歧无损，§3.1）
    std::optional<std::string> get(uint64_t version, const std::string& key);

    // 批量快照读（KvBatchGet，按 region 分组）；返回与 keys 等长、同序的值数组。
    // 带锁的 key 退化为单 key Get 兜底（内部解析锁）
    std::vector<std::optional<std::string>> batch_get(uint64_t version,
                                                      const std::vector<std::string>& keys);

    // 范围扫 [begin, end)，最多 limit 条（end 空 = 无上界）。limit 精确下推为
    // 扫描批大小——存在性探测（limit=1）只取 1 条，不隐式多拉。同一 version 的
    // 多次调用构成一致视图（MVCC，§3.3）
    std::vector<std::pair<std::string, std::string>> scan(uint64_t version,
                                                          const std::string& begin,
                                                          const std::string& end, size_t limit);

    // [lo, hi) 内最后一个 key；无则 nullopt。key-only 反向扫：region 定位走
    // 缓存 + 通常 1 次 RPC——list 组尾 token 的 O(1) 原语（§3.3，对应 RocksDB
    // 版的 SeekForPrev；client-c Scanner 未封装 reverse，此处用裸 KvScan）
    std::optional<std::string> last_key(uint64_t version, const std::string& lo,
                                        const std::string& hi);

    // 乐观 2PC 提交（§4）。muts 非空；primary = muts[0].key。同 key 多条
    // mutation 按出现序合并、后者胜（对齐 WriteBatch 按序覆盖语义——Percolator
    // 每 key 只允许一个 op）。异常：
    //   TikvConflict      —— 写写冲突/锁竞争，安全重试
    //   TikvAlreadyExist  —— Insert 撞键，安全失败
    //   TikvUndetermined  —— primary commit 结果不明，禁止盲重试
    //   pingcap::Exception —— 其余（网络/集群），prewrite 阶段者明确未提交
    void commit(uint64_t start_ts, const std::vector<TikvMutation>& muts);

    // 测试钩子：暴露底层集群（splitRegion 制造多 region 等，§10）
    pingcap::kv::Cluster* cluster();

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};

}  // namespace lights3::storage::duostore
