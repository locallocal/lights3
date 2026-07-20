// CloudProxyBackend 实现（docs/cloudproxy-backend.md）。
// 通用管线：构造最小 HttpRequest 签名 → 搬运 headers → ClientPool 发送 → 映射错误。
// 数据面经 http/pushpull.h 的 BlockQueue 在私有 pump 线程与 handler 协程间翻转
// 推/拉模型；控制面短请求在共享池线程同步调用（docs/cloudproxy-backend.md §2.3）。
#include "storage/cloudproxy/cloudproxy_backend.h"

#include <future>
#include <thread>

#include "core/log.h"
#include "core/util/crypto.h"
#include "core/util/hex.h"
#include "core/util/time.h"
#include "core/util/uri.h"
#include "http/pushpull.h"
#include "s3/xml.h"
#include "storage/cloudproxy/remote_client.h"
#include "storage/multipart.h"

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;
using namespace cloudproxy;

namespace {

// ETag 引号处理与 md5 形态判定复用共享助手（storage/multipart.h、util::from_hex）

bool is_md5_hex(const std::string& s) { return util::from_hex(s).size() == 16; }

std::string bucket_path(const std::string& rb) {
    return "/" + util::aws_uri_encode(rb, /*encode_slash=*/false);
}

std::string object_path(const std::string& rb, std::string_view key) {
    return "/" + util::aws_uri_encode(rb + "/" + std::string(key), /*encode_slash=*/false);
}

std::string qv(std::string_view v) { return util::aws_uri_encode(v, /*encode_slash=*/true); }

std::string format_range(const ByteRange& r) {
    std::string out = "bytes=";
    if (r.first) out += std::to_string(*r.first);
    out += "-";
    if (r.last) out += std::to_string(*r.last);
    return out;
}

// x-amz-meta-* 头（签名会把它们收进 SignedHeaders）
std::vector<std::pair<std::string, std::string>> meta_headers(const ObjectMeta& meta) {
    std::vector<std::pair<std::string, std::string>> out;
    for (auto& [k, v] : meta.user_meta) out.emplace_back("x-amz-meta-" + k, v);
    return out;
}

uint64_t parse_u64(const std::string& s) {
    try {
        return std::stoull(s);
    } catch (...) {
        return 0;
    }
}

// 对象全长必须可知（backend.h 契约：meta.size 为对象全长）；远端不给
// Content-Length（如 chunked 响应）时宁可报错，不得以 0 长度静默截断
uint64_t require_content_length(const httplib::Response& res) {
    if (!res.has_header("Content-Length"))
        throw S3Error(S3ErrorCode::InternalError,
                      "cloudproxy: remote response lacks Content-Length");
    try {
        return std::stoull(res.get_header_value("Content-Length"));
    } catch (...) {
        throw S3Error(S3ErrorCode::InternalError,
                      "cloudproxy: remote sent invalid Content-Length");
    }
}

ObjectMeta meta_from_response(std::string_view key, const httplib::Response& res) {
    ObjectMeta m;
    m.key = std::string(key);
    m.etag = strip_etag_quotes(res.get_header_value("ETag"));
    m.size = require_content_length(res);
    if (res.has_header("Content-Type")) m.content_type = res.get_header_value("Content-Type");
    if (auto t = util::parse_http_date(res.get_header_value("Last-Modified")))
        m.last_modified = *t;
    for (auto& [k, v] : res.headers) {
        constexpr std::string_view kMetaPrefix = "x-amz-meta-";
        if (k.size() > kMetaPrefix.size() &&
            http::HeaderMap::ieq(std::string_view(k).substr(0, kMetaPrefix.size()),
                                 kMetaPrefix)) {
            std::string mk = k.substr(kMetaPrefix.size());
            for (char& c : mk) c = http::HeaderMap::lower(c);
            m.user_meta[mk] = v;
        }
    }
    return m;
}

// GET 头部到达后交付给等待协程的载荷（docs/cloudproxy-backend.md §3.1 ①）
struct GetHead {
    ObjectMeta meta;
    std::optional<ByteRange> range;
    std::optional<uint64_t> body_len;
};

GetHead head_from_response(std::string_view key, const httplib::Response& res) {
    GetHead h;
    h.meta = meta_from_response(key, res);
    h.body_len = h.meta.size;
    if (res.status == 206) {
        // Content-Range: bytes a-b/total —— meta.size 为对象全长（接口约定）。
        // 解析不了（含 RFC 允许的 "bytes a-b/*" 未知总长形态）必须报错：
        // 静默走全量语义会把部分内容当完整对象回给客户端
        std::string cr = res.get_header_value("Content-Range");
        unsigned long long a = 0, b = 0, total = 0;
        if (sscanf(cr.c_str(), "bytes %llu-%llu/%llu", &a, &b, &total) != 3)
            throw S3Error(S3ErrorCode::InternalError,
                          "cloudproxy: remote 206 has missing or unparsable Content-Range: " +
                              cr);
        h.meta.size = total;
        h.range = ByteRange{a, b};
        h.body_len = b - a + 1;
    }
    return h;
}

// 从另一线程中止一次在途的 httplib 传输。cancel 队列只能解开阻塞在 push 的
// pump；若 pump 正阻塞在 socket 读/写（远端停滞），必须 client.stop() 打断，
// 否则中止方要陪绑到 read/write timeout（默认 60s）。被 stop 的连接归还池后
// 由 httplib 在下次请求时自动重连。
struct TransferAbort {
    std::mutex m;
    httplib::Client* active = nullptr;
    bool aborted = false;

    void arm(httplib::Client& c) {
        std::lock_guard lk(m);
        active = &c;
        if (aborted) c.stop();
    }
    void disarm() {
        std::lock_guard lk(m);
        active = nullptr;
    }
    void abort() {
        std::lock_guard lk(m);
        aborted = true;
        if (active) active->stop();
    }
    bool is_aborted() {
        std::lock_guard lk(m);
        return aborted;
    }
};

// 析构即 cancel + 打断在途传输 + join pump：客户端断连/handler 异常时
// 中止远端传输（docs/cloudproxy-backend.md §3.1）
class PumpBodyReader final : public http::BodyReader {
public:
    PumpBodyReader(std::shared_ptr<http::BlockQueue> q, std::optional<uint64_t> len,
                   std::shared_ptr<TransferAbort> abort, std::thread pump)
        : q_(q), inner_(std::move(q), len), abort_(std::move(abort)), pump_(std::move(pump)) {}
    ~PumpBodyReader() override {
        q_->cancel();
        abort_->abort();
        if (pump_.joinable()) pump_.join();
    }
    Task<size_t> read(std::span<std::byte> buf) override { return inner_.read(buf); }
    std::optional<uint64_t> length() const override { return inner_.length(); }

private:
    std::shared_ptr<http::BlockQueue> q_;
    http::QueueBodyReader inner_;
    std::shared_ptr<TransferAbort> abort_;
    std::thread pump_;
};

std::string resource_of(std::string_view bucket, std::string_view key = "") {
    std::string r = "/" + std::string(bucket);
    if (!key.empty()) r += "/" + std::string(key);
    return r;
}

// 总 ETag 规则复用 combined_etag（docs/cloudproxy-backend.md §5.2 complete 歧义消解用）
std::string expected_total_etag(std::span<const PartInfo> parts) {
    std::vector<std::string> md5s;
    md5s.reserve(parts.size());
    for (auto& p : parts) {
        std::string hex(strip_etag_quotes(p.etag));
        if (!is_md5_hex(hex)) return "";  // 非 md5 形态的分片 etag：无法预测
        md5s.push_back(std::move(hex));
    }
    return combined_etag(md5s);
}

}  // namespace

// ---------- 构造 ----------

CloudProxyBackend::CloudProxyBackend(CloudProxyConfig cfg, std::shared_ptr<ThreadPool> pool)
    : pool_(std::move(pool)) {
    auto ep = Endpoint::parse(cfg.endpoint);
    ctx_ = std::make_shared<RemoteContext>(std::move(cfg), ep);
    LOG_INFO("cloudproxy backend: endpoint={} region={} prefix='{}'", ctx_->cfg.endpoint,
             ctx_->cfg.region, ctx_->cfg.bucket_prefix);
}

CloudProxyBackend::~CloudProxyBackend() = default;

std::string CloudProxyBackend::remote_bucket(std::string_view bucket) const {
    validate_bucket_name(bucket);
    std::string rb = ctx_->cfg.bucket_prefix + std::string(bucket);
    if (rb.size() > 63)
        throw S3Error(S3ErrorCode::InvalidBucketName,
                      "bucket name with cloudproxy bucket_prefix exceeds 63 bytes",
                      std::string(bucket));
    return rb;
}

// ---------- bucket 操作（docs/cloudproxy-backend.md §4.3）----------

Task<void> CloudProxyBackend::create_bucket(std::string_view bucket) {
    auto rb = remote_bucket(bucket);
    auto path = bucket_path(rb);
    auto resource = resource_of(bucket);
    std::string body;
    if (ctx_->cfg.region != "us-east-1") {
        s3::XmlWriter w;
        w.open("CreateBucketConfiguration");
        w.element("LocationConstraint", ctx_->cfg.region);
        w.close();
        body = w.str();
    }
    co_await pool_->schedule();
    auto res = ctx_->with_retry([&](httplib::Client& c) {
        auto headers = ctx_->signed_headers("PUT", path, "", {},
                                            body.empty() ? "" : util::sha256_hex(body));
        return c.Put(path, headers, body, body.empty() ? "" : "application/xml");
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status / 100 == 2) co_return;
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::None, resource);
}

Task<void> CloudProxyBackend::delete_bucket(std::string_view bucket) {
    auto rb = remote_bucket(bucket);
    auto path = bucket_path(rb);
    co_await pool_->schedule();
    auto res = ctx_->with_retry([&](httplib::Client& c) {
        return c.Delete(path, ctx_->signed_headers("DELETE", path, "", {}, ""));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status / 100 == 2) co_return;
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Bucket, resource_of(bucket));
}

Task<bool> CloudProxyBackend::bucket_exists(std::string_view bucket) {
    auto rb = remote_bucket(bucket);
    auto path = bucket_path(rb);
    co_await pool_->schedule();
    auto res = ctx_->with_retry([&](httplib::Client& c) {
        return c.Head(path, ctx_->signed_headers("HEAD", path, "", {}, ""));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status / 100 == 2) co_return true;
    if (res->status == 404) co_return false;
    if (res->status == 403) {
        // AWS HeadBucket 语义：存在但无权也是 403，视为存在（docs/cloudproxy-backend.md §4.3）
        LOG_WARN("cloudproxy: HEAD bucket {} returned 403, treating as exists", rb);
        co_return true;
    }
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Bucket, resource_of(bucket));
}

Task<std::vector<BucketInfo>> CloudProxyBackend::list_buckets() {
    co_await pool_->schedule();
    auto res = ctx_->with_retry([&](httplib::Client& c) {
        return c.Get("/", ctx_->signed_headers("GET", "/", "", {}, ""));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status != 200)
        ctx_->throw_remote_error(res->status, res->body, ErrCtx::None, "/");
    std::vector<BucketInfo> out;
    auto root = s3::xml_parse(res->body);
    const auto& prefix = ctx_->cfg.bucket_prefix;
    if (auto* buckets = root.find("Buckets")) {
        for (auto& b : buckets->children) {
            if (b.name != "Bucket") continue;
            std::string name = b.get("Name");
            // 只保留带前缀的，剥前缀返回；其余是远端账号下的无关 bucket
            if (name.size() <= prefix.size() || name.compare(0, prefix.size(), prefix) != 0)
                continue;
            BucketInfo info;
            info.name = name.substr(prefix.size());
            if (auto t = util::parse_iso8601(b.get("CreationDate"))) info.created = *t;
            out.push_back(std::move(info));
        }
    }
    co_return out;
}

// ---------- 对象数据面（docs/cloudproxy-backend.md §3）----------

Task<ObjectStream> CloudProxyBackend::get_object(std::string_view bucket, std::string_view key,
                                                 std::optional<ByteRange> range) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto path = object_path(rb, key);
    auto resource = resource_of(bucket, key);
    std::vector<std::pair<std::string, std::string>> extra;
    if (range) extra.emplace_back("Range", format_range(*range));

    co_await pool_->schedule();
    auto ctx = ctx_;
    auto queue = std::make_shared<http::BlockQueue>(ctx->cfg.queue_cap_bytes);
    auto abortst = std::make_shared<TransferAbort>();
    auto prom = std::make_shared<std::promise<GetHead>>();
    auto fut = prom->get_future();
    std::string keycopy(key);

    // pump：ResponseHandler 到达即交付 meta；ContentReceiver 推转拉进队列（§3.1）
    std::thread pump([ctx, queue, abortst, prom, path, extra, resource, keycopy] {
        bool delivered = false;
        try {
            for (int attempt = 0;; ++attempt) {
                std::string err_body;
                {
                    auto lease = ctx->pool.acquire();
                    abortst->arm(lease.client());
                    auto headers = ctx->signed_headers("GET", path, "", extra, "");
                    auto res = lease.client().Get(
                        path, headers,
                        [&](const httplib::Response& r) {
                            if (r.status != 200 && r.status != 206) return true;
                            try {
                                auto h = head_from_response(keycopy, r);
                                delivered = true;
                                prom->set_value(std::move(h));
                                return true;
                            } catch (...) {
                                // 首部不合契约（如 206 缺 Content-Range）：
                                // 交付异常并中止传输，绝不静默按全量语义走
                                delivered = true;
                                prom->set_exception(std::current_exception());
                                return false;
                            }
                        },
                        [&](const char* data, size_t n) {
                            if (delivered) return queue->push(data, n);
                            if (err_body.size() < 64 * 1024) err_body.append(data, n);
                            return true;
                        });
                    abortst->disarm();
                    if (delivered) {
                        queue->close(static_cast<bool>(res));  // res 空 = 传输中途失败
                        return;
                    }
                    // headers 未交付：重试或交付映射后的异常
                    bool retry = !res ? RemoteContext::retryable_transport(res.error())
                                      : ctx->retryable_status(res->status);
                    if (!(retry && attempt < ctx->cfg.retry_max)) {
                        try {
                            if (!res) ctx->throw_transport_error(res.error());
                            ctx->throw_remote_error(res->status, err_body, ErrCtx::Key,
                                                    resource);
                        } catch (...) {
                            prom->set_exception(std::current_exception());
                        }
                        return;
                    }
                }  // 先归还连接再退避
                ctx->backoff(attempt);
            }
        } catch (...) {
            // acquire 超时等意外：视交付阶段选择传播路径
            if (!delivered)
                prom->set_exception(std::current_exception());
            else
                queue->close(false);
        }
    });

    GetHead head;
    try {
        head = fut.get();  // 池线程阻塞等首部；pump 在私有线程推进，无互等（§2.3）
    } catch (...) {
        pump.join();
        throw;
    }
    ObjectStream out;
    out.meta = std::move(head.meta);
    out.range = head.range;
    out.body =
        std::make_unique<PumpBodyReader>(queue, head.body_len, abortst, std::move(pump));
    co_return out;
}

Task<PutResult> CloudProxyBackend::stream_upload(
    std::string raw_path, std::string raw_query, std::string content_type,
    std::vector<std::pair<std::string, std::string>> extra, http::BodyReader& body,
    std::string resource, bool multipart_ctx) {
    auto len_opt = body.length();
    // AWS 不接受裸 chunked 上行；无长度是罕见路径，首期不做 TRAILER 组帧（§3.2）
    if (!len_opt)
        throw S3Error(S3ErrorCode::NotImplemented,
                      "cloudproxy: upload without content length is not supported");
    const uint64_t len = *len_opt;
    std::string full = raw_query.empty() ? raw_path : raw_path + "?" + raw_query;

    co_await pool_->schedule();
    auto ctx = ctx_;
    auto queue = std::make_shared<http::BlockQueue>(ctx->cfg.queue_cap_bytes);
    auto abortst = std::make_shared<TransferAbort>();

    struct Outcome {
        bool has_response = false;
        int status = 0;
        std::string resp_body;
        std::string etag;
        httplib::Error err = httplib::Error::Unknown;
        std::exception_ptr exc;
    };
    auto out = std::make_shared<Outcome>();

    // pump：拉转拉，Provider 从队列取数写 DataSink（§3.2）
    std::thread pump([ctx, queue, abortst, out, raw_path, raw_query, full, content_type, extra,
                      len] {
        try {
            for (int attempt = 0;; ++attempt) {
                // pump 单线程读写即可，无需原子：仅用于连接阶段重试判定
                bool provider_called = false;
                auto lease = ctx->pool.acquire();
                abortst->arm(lease.client());
                auto headers = ctx->signed_headers("PUT", raw_path, raw_query, extra,
                                                   kUnsignedPayload);
                auto res = lease.client().Put(
                    full, headers, static_cast<size_t>(len),
                    [&](size_t /*offset*/, size_t length, httplib::DataSink& sink) {
                        provider_called = true;
                        std::byte buf[64 * 1024];
                        size_t want = std::min(sizeof(buf), length);
                        size_t n = 0;
                        try {
                            n = queue->pop(std::span(buf, want));
                        } catch (...) {
                            return false;  // 生产方（客户端上行）中途失败
                        }
                        if (n == 0) return false;  // EOF 早于 Content-Length：中止
                        return sink.write(reinterpret_cast<const char*>(buf), n);
                    },
                    content_type);
                abortst->disarm();
                // 仅连接建立阶段失败且 Provider 从未被调用（队列未被消费）可重试
                // （§5.2）；已被主动中止的传输不重试
                if (!res && !provider_called && !abortst->is_aborted() &&
                    RemoteContext::connection_stage_error(res.error()) &&
                    attempt < ctx->cfg.retry_max) {
                    ctx->backoff(attempt);
                    continue;
                }
                if (res) {
                    out->has_response = true;
                    out->status = res->status;
                    out->resp_body = res->body;
                    out->etag = res->get_header_value("ETag");
                } else {
                    out->err = res.error();
                }
                break;
            }
        } catch (...) {
            out->exc = std::current_exception();
        }
        queue->cancel();  // 解除生产者可能的 push 阻塞
    });

    // 生产者：handler 协程链驱动 body.read，增量 MD5（§6 端到端校验）
    util::HashStream md5(util::HashStream::Algo::Md5);
    uint64_t sent = 0;
    bool remote_gone = false;
    std::exception_ptr read_err;
    std::vector<std::byte> buf(64 * 1024);
    try {
        while (sent < len) {
            size_t n = co_await body.read(std::span(buf));
            // body.read 可能把协程恢复到 L1 驱动线程（beast 经对称转移回 strand）；
            // push 会因背压阻塞，必须回池线程再做，不得占住事件循环（§2.3）
            co_await pool_->schedule();
            if (n == 0) break;
            md5.update(std::span(reinterpret_cast<const uint8_t*>(buf.data()), n));
            if (!queue->push(reinterpret_cast<const char*>(buf.data()), n)) {
                remote_gone = true;
                break;
            }
            sent += n;
        }
    } catch (...) {
        read_err = std::current_exception();
    }
    queue->close(!read_err && !remote_gone && sent == len);
    // 上行断流时 pump 可能正阻塞在 socket 写等远端收数：主动打断，别陪绑超时
    if (read_err) abortst->abort();
    co_await pool_->schedule();  // join 最长等一个远端响应周期，同样不占驱动线程
    pump.join();

    if (out->exc) std::rethrow_exception(out->exc);
    if (read_err) std::rethrow_exception(read_err);  // 客户端上行断流
    if (!out->has_response) ctx->throw_transport_error(out->err);
    if (out->status / 100 == 2) {
        std::string etag(strip_etag_quotes(out->etag));
        if (ctx->cfg.verify_etag && is_md5_hex(etag)) {
            if (etag != md5.final_hex())
                throw S3Error(S3ErrorCode::InternalError,
                              "cloudproxy: upload corrupted in transit (remote etag != "
                              "local md5)");
        }
        co_return PutResult{etag};
    }
    ctx->throw_remote_error(out->status, out->resp_body,
                            multipart_ctx ? ErrCtx::Upload : ErrCtx::Bucket, resource);
}

Task<PutResult> CloudProxyBackend::put_object(std::string_view bucket, std::string_view key,
                                              ObjectMeta meta, http::BodyReader& body) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    co_return co_await stream_upload(object_path(rb, key), "", meta.content_type,
                                     meta_headers(meta), body, resource_of(bucket, key),
                                     /*multipart_ctx=*/false);
}

Task<ObjectMeta> CloudProxyBackend::head_object(std::string_view bucket,
                                                std::string_view key) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto path = object_path(rb, key);
    co_await pool_->schedule();
    auto res = ctx_->with_retry([&](httplib::Client& c) {
        return c.Head(path, ctx_->signed_headers("HEAD", path, "", {}, ""));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status == 200) co_return meta_from_response(key, *res);
    // HEAD 无错误体：404 按上下文补 NoSuchKey（docs/cloudproxy-backend.md §4.1/§5.1）
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Key, resource_of(bucket, key));
}

Task<void> CloudProxyBackend::delete_object(std::string_view bucket, std::string_view key) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto path = object_path(rb, key);
    co_await pool_->schedule();
    auto res = ctx_->with_retry([&](httplib::Client& c) {
        return c.Delete(path, ctx_->signed_headers("DELETE", path, "", {}, ""));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    // 204 与 404 都视为成功（S3 幂等删除语义）
    if (res->status / 100 == 2 || res->status == 404) co_return;
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Key, resource_of(bucket, key));
}

// ---------- list（docs/cloudproxy-backend.md §4.2：恒用 start-after 分页）----------

Task<ListResult> CloudProxyBackend::list_objects(std::string_view bucket,
                                                 const ListOptions& opt) {
    auto rb = remote_bucket(bucket);
    auto path = bucket_path(rb);
    std::string query = "list-type=2&max-keys=" + std::to_string(opt.max_keys);
    if (!opt.prefix.empty()) query += "&prefix=" + qv(opt.prefix);
    if (!opt.delimiter.empty()) query += "&delimiter=" + qv(opt.delimiter);
    if (!opt.start_after.empty()) query += "&start-after=" + qv(opt.start_after);
    std::string full = path + "?" + query;

    co_await pool_->schedule();
    auto res = ctx_->with_retry([&](httplib::Client& c) {
        return c.Get(full, ctx_->signed_headers("GET", path, query, {}, ""));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status != 200)
        ctx_->throw_remote_error(res->status, res->body, ErrCtx::Bucket, resource_of(bucket));

    ListResult out;
    auto root = s3::xml_parse(res->body);
    std::string last_key, last_prefix;
    for (auto& child : root.children) {
        if (child.name == "Contents") {
            ObjectMeta m;
            m.key = child.get("Key");
            m.etag = std::string(strip_etag_quotes(child.get("ETag")));
            m.size = parse_u64(child.get("Size"));
            if (auto t = util::parse_iso8601(child.get("LastModified"))) m.last_modified = *t;
            last_key = m.key;
            out.objects.push_back(std::move(m));
        } else if (child.name == "CommonPrefixes") {
            last_prefix = child.get("Prefix");
            out.common_prefixes.push_back(last_prefix);
        }
    }
    out.is_truncated = root.get("IsTruncated") == "true";
    if (out.is_truncated) {
        // token = 本页最后一个元素；组（common prefix）用其字典序上界跳过整组
        if (last_prefix > last_key)
            out.next_token = group_skip_token(last_prefix);
        else
            out.next_token = last_key;
    }
    co_return out;
}

// ---------- multipart 透传（docs/cloudproxy-backend.md §4.4）----------

Task<std::string> CloudProxyBackend::create_multipart(std::string_view bucket,
                                                      std::string_view key, ObjectMeta meta) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto path = object_path(rb, key);
    std::string query = "uploads";
    std::string full = path + "?" + query;
    auto extra = meta_headers(meta);
    co_await pool_->schedule();
    auto res = ctx_->with_retry([&](httplib::Client& c) {
        return c.Post(full, ctx_->signed_headers("POST", path, query, extra, ""), "",
                      meta.content_type);
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status != 200)
        ctx_->throw_remote_error(res->status, res->body, ErrCtx::Bucket,
                                 resource_of(bucket, key));
    auto root = s3::xml_parse(res->body);
    std::string id = root.get("UploadId");
    if (root.name != "InitiateMultipartUploadResult" || id.empty())
        throw S3Error(S3ErrorCode::InternalError,
                      "cloudproxy: remote returned unexpected CreateMultipartUpload body");
    co_return id;
}

Task<PutResult> CloudProxyBackend::upload_part(std::string_view bucket, std::string_view key,
                                               std::string_view upload_id, int part_no,
                                               http::BodyReader& body) {
    validate_object_key(key);
    validate_part_number(part_no);
    auto rb = remote_bucket(bucket);
    std::string query =
        "partNumber=" + std::to_string(part_no) + "&uploadId=" + qv(upload_id);
    co_return co_await stream_upload(object_path(rb, key), query, "", {}, body,
                                     resource_of(bucket, key), /*multipart_ctx=*/true);
}

Task<PutResult> CloudProxyBackend::complete_multipart(std::string_view bucket,
                                                      std::string_view key,
                                                      std::string_view upload_id,
                                                      std::span<const PartInfo> parts) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto path = object_path(rb, key);
    auto resource = resource_of(bucket, key);
    std::string query = "uploadId=" + qv(upload_id);
    std::string full = path + "?" + query;

    s3::XmlWriter w;
    w.open("CompleteMultipartUpload");
    for (auto& p : parts) {
        w.open("Part");
        w.element("PartNumber", static_cast<uint64_t>(p.part_no));
        w.element("ETag", "\"" + std::string(strip_etag_quotes(p.etag)) + "\"");
        w.close();
    }
    w.close();
    const std::string body = w.str();
    const std::string body_hash = util::sha256_hex(body);

    co_await pool_->schedule();
    std::string etag_out;
    std::exception_ptr ambiguous_nosuch;
    for (int attempt = 0;; ++attempt) {
        auto res = [&] {
            auto lease = ctx_->pool.acquire();
            return lease.client().Post(
                full, ctx_->signed_headers("POST", path, query, {}, body_hash), body,
                "application/xml");
        }();
        bool retry = !res ? RemoteContext::retryable_transport(res.error())
                          : ctx_->retryable_status(res->status);
        if (retry && attempt < ctx_->cfg.retry_max) {
            ctx_->backoff(attempt);
            continue;
        }
        const bool retried = attempt > 0;
        if (!res) ctx_->throw_transport_error(res.error());
        try {
            if (res->status != 200)
                ctx_->throw_remote_error(res->status, res->body, ErrCtx::Upload, resource);
            // S3 特有：complete 耗时长时先回 200，错误在 body 里（docs/cloudproxy-backend.md §4.4）
            s3::XmlNode root;
            try {
                root = s3::xml_parse(res->body);
            } catch (...) {
                throw S3Error(S3ErrorCode::InternalError,
                              "cloudproxy: remote returned unparsable CompleteMultipartUpload "
                              "body");
            }
            if (root.name == "Error") {
                auto code = map_remote_code(root.get("Code"));
                throw S3Error(code.value_or(S3ErrorCode::InternalError), root.get("Message"),
                              resource);
            }
            etag_out = std::string(strip_etag_quotes(root.get("ETag")));
        } catch (const S3Error& e) {
            // 重试后收 NoSuchUpload：前一次可能实际已成功 → HEAD 验证（docs/cloudproxy-backend.md §5.2）
            if (e.code == S3ErrorCode::NoSuchUpload && retried) {
                ambiguous_nosuch = std::current_exception();
                break;
            }
            throw;
        }
        break;
    }
    if (ambiguous_nosuch) {
        std::string expect = expected_total_etag(parts);
        bool completed_before = false;
        if (!expect.empty()) {
            try {
                auto m = co_await head_object(bucket, key);
                completed_before = m.etag == expect;
            } catch (...) {
                completed_before = false;
            }
        }
        if (!completed_before) std::rethrow_exception(ambiguous_nosuch);
        etag_out = expect;
    }
    co_return PutResult{etag_out};
}

Task<void> CloudProxyBackend::abort_multipart(std::string_view bucket, std::string_view key,
                                              std::string_view upload_id) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto path = object_path(rb, key);
    std::string query = "uploadId=" + qv(upload_id);
    std::string full = path + "?" + query;
    co_await pool_->schedule();
    auto res = ctx_->with_retry([&](httplib::Client& c) {
        return c.Delete(full, ctx_->signed_headers("DELETE", path, query, {}, ""));
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status / 100 == 2) co_return;
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Upload,
                             resource_of(bucket, key));
}

Task<std::vector<PartMeta>> CloudProxyBackend::list_parts(std::string_view bucket,
                                                          std::string_view key,
                                                          std::string_view upload_id) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto path = object_path(rb, key);
    auto resource = resource_of(bucket, key);
    co_await pool_->schedule();
    std::vector<PartMeta> out;
    std::string marker;
    for (;;) {
        std::string query = "uploadId=" + qv(upload_id) + "&max-parts=1000";
        if (!marker.empty()) query += "&part-number-marker=" + marker;
        std::string full = path + "?" + query;
        auto res = ctx_->with_retry([&](httplib::Client& c) {
            return c.Get(full, ctx_->signed_headers("GET", path, query, {}, ""));
        });
        if (!res) ctx_->throw_transport_error(res.error());
        if (res->status != 200)
            ctx_->throw_remote_error(res->status, res->body, ErrCtx::Upload, resource);
        auto root = s3::xml_parse(res->body);
        for (auto& child : root.children) {
            if (child.name != "Part") continue;
            PartMeta p;
            p.part_no = static_cast<int>(parse_u64(child.get("PartNumber")));
            p.size = parse_u64(child.get("Size"));
            p.etag = std::string(strip_etag_quotes(child.get("ETag")));
            if (auto t = util::parse_iso8601(child.get("LastModified"))) p.last_modified = *t;
            out.push_back(std::move(p));
        }
        if (root.get("IsTruncated") != "true") break;
        marker = root.get("NextPartNumberMarker");
        if (marker.empty()) break;
    }
    co_return out;
}

Task<std::vector<UploadInfo>> CloudProxyBackend::list_multipart_uploads(
    std::string_view bucket) {
    auto rb = remote_bucket(bucket);
    auto path = bucket_path(rb);
    auto resource = resource_of(bucket);
    co_await pool_->schedule();
    std::vector<UploadInfo> out;
    std::string key_marker, id_marker;
    for (;;) {
        std::string query = "uploads&max-uploads=1000";
        if (!key_marker.empty())
            query += "&key-marker=" + qv(key_marker) + "&upload-id-marker=" + qv(id_marker);
        std::string full = path + "?" + query;
        auto res = ctx_->with_retry([&](httplib::Client& c) {
            return c.Get(full, ctx_->signed_headers("GET", path, query, {}, ""));
        });
        if (!res) ctx_->throw_transport_error(res.error());
        if (res->status != 200)
            ctx_->throw_remote_error(res->status, res->body, ErrCtx::Bucket, resource);
        auto root = s3::xml_parse(res->body);
        for (auto& child : root.children) {
            if (child.name != "Upload") continue;
            UploadInfo u;
            u.key = child.get("Key");
            u.upload_id = child.get("UploadId");
            if (auto t = util::parse_iso8601(child.get("Initiated"))) u.initiated = *t;
            out.push_back(std::move(u));
        }
        if (root.get("IsTruncated") != "true") break;
        key_marker = root.get("NextKeyMarker");
        id_marker = root.get("NextUploadIdMarker");
        if (key_marker.empty() && id_marker.empty()) break;
    }
    co_return out;
}

}  // namespace lights3::storage
