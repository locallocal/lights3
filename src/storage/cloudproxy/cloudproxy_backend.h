// L3: CloudProxyBackend —— 映射公有云的代理后端（docs/cloudproxy-backend.md）。
// 自签 SigV4 + vendored httplib 直连远端 S3 兼容端点；本头文件不暴露 httplib 类型
// （httplib 细节全部收在 remote_client.h/.cc 内部）。
#pragma once

#include <map>
#include <memory>
#include <string>
#include <type_traits>

#include "core/metrics.h"
#include "core/thread_pool.h"
#include "storage/backend.h"

namespace lights3::storage {

struct CloudProxyConfig {
    std::string endpoint;  // scheme://host[:port]
    std::string region = "us-east-1";
    std::string access_key;
    std::string secret_key;
    std::string bucket_prefix;  // 远端 bucket = 前缀 + 本地名
    // false = virtual-hosted style（docs/cloudproxy-backend.md §7）：连接与 SNI 恒指
    // endpoint，仅 Host/签名与路径按 bucket 变化——要求远端在 endpoint 证书下受理
    // vhost Host（AWS 区域端点/S3 兼容网关的常见形态）
    bool force_path_style = true;
    // 控制面短请求走私有线程而非共享池（docs/cloudproxy-backend.md §2.3）：高 RTT
    // 远端不占池线程；代价 = 每控制请求一次线程创建。压测结论默认 false（见 §2.3）
    bool control_in_pump = false;
    bool tls_verify = true;
    std::string ca_cert;
    int connect_timeout_ms = 5000;
    int request_timeout_ms = 60000;
    int retry_max = 3;
    int retry_base_ms = 100;
    int max_connections = 16;
    bool verify_etag = true;             // docs/cloudproxy-backend.md §6：单段 PUT 与远端 ETag 比对 MD5
    size_t queue_cap_bytes = 1 << 20;    // 数据面 BlockQueue 容量（背压水位）

    // BackendConfig::params → 配置；非法值在配置加载期抛 std::runtime_error
    static CloudProxyConfig from_params(const std::string& name,
                                        const std::map<std::string, std::string>& params);
};

namespace cloudproxy {
struct RemoteContext;  // remote_client.h：ClientPool + 签名管线 + 错误映射
}

class CloudProxyBackend final : public IStorageBackend {
public:
    // metrics 默认空 scope（docs/cloudproxy-backend.md §8.2）：测试直构免装配，
    // 计数落孤立实例
    CloudProxyBackend(CloudProxyConfig cfg, std::shared_ptr<ThreadPool> pool,
                      MetricsScope metrics = {});
    ~CloudProxyBackend() override;

    Task<void> create_bucket(std::string_view bucket) override;
    Task<void> delete_bucket(std::string_view bucket) override;
    Task<bool> bucket_exists(std::string_view bucket) override;
    Task<std::vector<BucketInfo>> list_buckets() override;

    Task<ObjectStream> get_object(std::string_view bucket, std::string_view key,
                                  std::optional<ByteRange> range) override;
    Task<PutResult> put_object(std::string_view bucket, std::string_view key, ObjectMeta meta,
                               http::BodyReader& body,
                               PutCondition cond = {}) override;
    Task<ObjectMeta> head_object(std::string_view bucket, std::string_view key) override;
    Task<void> delete_object(std::string_view bucket, std::string_view key) override;
    Task<ListResult> list_objects(std::string_view bucket, const ListOptions& opt) override;

    Task<std::string> create_multipart(std::string_view bucket, std::string_view key,
                                       ObjectMeta meta) override;
    Task<PutResult> upload_part(std::string_view bucket, std::string_view key,
                                std::string_view upload_id, int part_no,
                                http::BodyReader& body) override;
    Task<PutResult> complete_multipart(std::string_view bucket, std::string_view key,
                                       std::string_view upload_id,
                                       std::span<const PartInfo> parts) override;
    Task<void> abort_multipart(std::string_view bucket, std::string_view key,
                               std::string_view upload_id) override;
    Task<ListPartsResult> list_parts(std::string_view bucket, std::string_view key,
                                     std::string_view upload_id,
                                     const ListPartsOptions& opt) override;
    Task<ListUploadsResult> list_multipart_uploads(std::string_view bucket,
                                                   const ListUploadsOptions& opt) override;

private:
    // 本地名校验 + 前缀映射；映射后超 63 字节抛 InvalidBucketName
    std::string remote_bucket(std::string_view bucket) const;
    // 控制面阻塞段执行环境（docs/cloudproxy-backend.md §2.3）：缺省池线程；
    // control_in_pump=true 走一次性私有线程。定义在 .cc（仅本 TU 使用）
    template <class Fn>
    Task<std::invoke_result_t<Fn>> control_io(Fn fn);
    // PUT / upload_part 共用的流式上行（docs/cloudproxy-backend.md §3.2）。resource 为客户端视角的
    // "/bucket/key"（进错误 XML，不泄漏带前缀的远端路径）；multipart_ctx 决定
    // 无错误体 404 的语义兜底（Upload / Bucket）
    Task<PutResult> stream_upload(std::string raw_path, std::string raw_query,
                                  std::string host, std::string content_type,
                                  std::vector<std::pair<std::string, std::string>> extra,
                                  http::BodyReader& body, std::string resource,
                                  bool multipart_ctx);

    std::shared_ptr<cloudproxy::RemoteContext> ctx_;
    std::shared_ptr<ThreadPool> pool_;
    ThreadPoolExecutor exec_{*pool_};  // control_in_pump 私有线程完成后的续体投递（§2.3）
};

}  // namespace lights3::storage
