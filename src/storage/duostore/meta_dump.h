// L3: duostore meta 的备份/恢复与跨引擎迁移（docs/gaps.md §6.1）。以 IMetaStore
// 为中介的逻辑 dump/load：dump 把全部桶/对象记录（含 extent manifest）与封存
// pack 账写成自描述二进制流，load 经 put_object 逐条重放——record 级重放天然
// 兼作四引擎（rocksdb/sqlite/redis/tikv）之间的 meta 迁移工具，value 布局差异
// 被接口吸收。
//
// 备份/恢复的固化顺序（运维契约，两端都要求 **停写**）：
//   备份：先 data（拷 chunks/packs 目录或 rados pool 快照）→ 后 dump meta。
//         顺序保证"meta 引用的数据必在备份里"（§6 数据先行的镜像论证）：data
//         多出的文件只是孤儿，反向不成立。
//   恢复：先放回 data → load meta → **强制孤儿扫描**（DuoStoreBackend::
//         run_meta_load 内置）——回收备份窗口内 data 侧多余的文件。
//
// 刻意不入档的状态（恢复后由既有机制收敛）：
//   - 进行中 multipart：upload_id 由目标引擎生成、无法保值，跨迁移的续传本就
//     不可能成立。其分片数据在恢复侧无引用 → 孤儿扫描回收。
//   - gcq 待回收账：接口无入队原语；这些 extent 在恢复侧无引用 → 同上兜底。
//   - 桶创建时间：create_bucket 重打（信息性字段，S3 语义无依赖）。
//   - 对象 version：目标端 put_object 重新起算（仅供运行期压实 CAS，无持久语义）。
// 未封存 pack 的账经对象重放重建 delta 行；封存态与 file_size 由 'S' 记录恢复，
// 全死的 pack（无对象引用）恢复为 live=0 的封存账 → 下轮 GC 整删。
#pragma once

#include <cstdint>
#include <iosfwd>

#include "storage/duostore/meta_store.h"

namespace lights3::storage::duostore {

struct MetaDumpStats {
    uint64_t buckets = 0;
    uint64_t objects = 0;
    uint64_t sealed_packs = 0;
};

// 全量导出到 out（调用方保证期间无并发写；backend 层入口另持 GC 互斥）。
// 流自带魔数、逐记录长度前缀、结尾计数 + crc32c——load 端全量校验
MetaDumpStats dump_meta(IMetaStore& src, std::ostream& out);

// 从 in 重放到 dst（应为新建的空库；桶已存在时幂等跳过，支持中断后重跑）。
// 结尾把 chunk/pack 文件号计数器抬到已见最大 id 之上——防止恢复后新写分配
// 到与存量文件冲突的 id。流截断/crc 失配/计数不符抛 InternalError
MetaDumpStats load_meta(IMetaStore& dst, std::istream& in);

}  // namespace lights3::storage::duostore
