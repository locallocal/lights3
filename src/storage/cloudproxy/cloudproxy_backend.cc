// CloudProxyBackend 实现（docs/cloudproxy-backend.md）。
// 通用管线：构造最小 HttpRequest 签名 → 搬运 headers → ClientPool 发送 → 映射错误。
// 数据面经 http/pushpull.h 的 BlockQueue 在私有 pump 线程与 handler 协程间翻转
// 推/拉模型；控制面短请求在共享池线程同步调用（docs/cloudproxy-backend.md §2.3）。
#include "storage/cloudproxy/cloudproxy_backend.h"

#include <fcntl.h>
#include <unistd.h>

#include <chrono>
#include <filesystem>
#include <future>
#include <optional>
#include <semaphore>
#include <thread>

#include "core/log.h"
#include "core/util/crypto.h"
#include "core/util/hex.h"
#include "core/util/time.h"
#include "core/util/uri.h"
#include "http/pushpull.h"
#include "s3/xml.h"
#include "storage/cloudproxy/remote_client.h"
#include "storage/localfs/fs_util.h"
#include "storage/multipart.h"

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;
using namespace cloudproxy;

namespace {

// ETag 引号处理与 md5 形态判定复用共享助手（storage/multipart.h、util::from_hex）

bool is_md5_hex(const std::string& s) { return util::from_hex(s).size() == 16; }

// key 的路径段（"/key" 编码后）；bucket 段由 Target::prefix 承载（path-style 含
// "/<rb>"、vhost 为空，docs/cloudproxy-backend.md §7）
std::string key_path(std::string_view key) {
    return "/" + util::aws_uri_encode(key, /*encode_slash=*/false);
}

std::string qv(std::string_view v) { return util::aws_uri_encode(v, /*encode_slash=*/true); }

std::string format_range(const ByteRange& r) {
    std::string out = "bytes=";
    if (r.first) out += std::to_string(*r.first);
    out += "-";
    if (r.last) out += std::to_string(*r.last);
    return out;
}

// x-amz-meta-* 与一等元数据头（签名会把它们一并收进 SignedHeaders，无需改签名侧）
std::vector<std::pair<std::string, std::string>> meta_headers(const ObjectMeta& meta) {
    std::vector<std::pair<std::string, std::string>> out;
    for (auto& f : kStdMetaFields)
        if (!(meta.*f.field).empty()) out.emplace_back(f.header, meta.*f.field);
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
    for (auto& f : kStdMetaFields)
        if (res.has_header(f.header)) m.*f.field = res.get_header_value(f.header);
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
                   std::shared_ptr<TransferAbort> abort, std::thread pump,
                   std::shared_ptr<ThreadPool> pool)
        : q_(q), inner_(std::move(q), len), abort_(std::move(abort)), pump_(std::move(pump)),
          pool_(std::move(pool)) {}
    ~PumpBodyReader() override {
        q_->cancel();
        abort_->abort();
        if (pump_.joinable()) pump_.join();
    }
    // QueueBodyReader::pop 是 cv 阻塞：远端慢于客户端时（cloudproxy 常态）几乎
    // 每次读都要等 pump 推数。调用方可能是 beast 的 io 线程 / seastar reactor
    // shard（读协程在那里恢复），在其上阻塞会让整个事件循环停摆——先切池线程再
    // 阻塞，与项目内其余流式 reader 的约定一致（出方向 stream_upload 已如此）
    Task<size_t> read(std::span<std::byte> buf) override {
        co_await pool_->schedule();
        co_return co_await inner_.read(buf);
    }
    std::optional<uint64_t> length() const override { return inner_.length(); }

private:
    std::shared_ptr<http::BlockQueue> q_;
    http::QueueBodyReader inner_;
    std::shared_ptr<TransferAbort> abort_;
    std::thread pump_;
    std::shared_ptr<ThreadPool> pool_;
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

CloudProxyBackend::CloudProxyBackend(CloudProxyConfig cfg, std::shared_ptr<ThreadPool> pool,
                                     MetricsScope metrics)
    : pool_(std::move(pool)) {
    auto ep = Endpoint::parse(cfg.endpoint);
    ctx_ = std::make_shared<RemoteContext>(std::move(cfg), ep, metrics);
    LOG_INFO("cloudproxy backend: endpoint={} region={} prefix='{}' style={} control={}",
             ctx_->cfg.endpoint, ctx_->cfg.region, ctx_->cfg.bucket_prefix,
             ctx_->cfg.force_path_style ? "path" : "vhost",
             ctx_->cfg.control_in_pump ? "pump" : "pool");
}

CloudProxyBackend::~CloudProxyBackend() = default;

// 控制面阻塞段的执行环境（docs/cloudproxy-backend.md §2.3）：缺省切共享池线程
// （单次占用 ~ 一次远端往返 + 重试退避）；control_in_pump=true 起一次性私有线程
// （与数据面 pump 同族），完成后续体经池 executor 恢复——高 RTT 远端不占池线程，
// 代价是每控制请求一次线程创建（~几十 µs，压测见 §2.3）。fn 为纯阻塞函数，
// 不得内含 co_await；异常经 exception_ptr 原样透传
template <class Fn>
Task<std::invoke_result_t<Fn>> CloudProxyBackend::control_io(Fn fn) {
    using R = std::invoke_result_t<Fn>;
    if (!ctx_->cfg.control_in_pump) {
        co_await pool_->schedule();
        co_return fn();
    }
    struct Awaiter {
        Fn* fn;
        IExecutor* ex;
        std::optional<R> result;
        std::exception_ptr err;
        std::thread th;
        std::binary_semaphore gate{0};  // 闸住线程体：th 移动赋值完成前不得跑
        bool await_ready() const noexcept { return false; }
        void await_suspend(std::coroutine_handle<> h) {
            // 不闸会竞态：极快的 fn 会在 `th = ...` 赋值收尾前就 post，池线程
            // 恢复协程并析构本 awaiter（读 th），与本线程的移动赋值互踩
            th = std::thread([this, h] {
                gate.acquire();
                try {
                    result.emplace((*fn)());
                } catch (...) {
                    err = std::current_exception();
                }
                ex->post(h);  // 私有线程只投递，业务续行回池线程
            });
            gate.release();  // 此后不得再触碰任何成员（协程可能已在池线程恢复）
        }
        R await_resume() {
            th.join();  // post 后线程即收尾，join 仅微秒级
            if (err) std::rethrow_exception(err);
            return std::move(*result);
        }
    };
    co_return co_await Awaiter{&fn, &exec_};
}

std::string CloudProxyBackend::remote_bucket(std::string_view bucket) const {
    validate_bucket_name(bucket, kAllowReserved);
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
    auto t = ctx_->target(rb);
    auto path = t.bucket_path();
    auto resource = resource_of(bucket);
    std::string body;
    if (ctx_->cfg.region != "us-east-1") {
        s3::XmlWriter w;
        w.open("CreateBucketConfiguration");
        w.element("LocationConstraint", ctx_->cfg.region);
        w.close();
        body = w.str();
    }
    auto res = co_await control_io([&] {
        return ctx_->with_retry("create_bucket", [&](httplib::Client& c) {
            auto headers = ctx_->signed_headers("PUT", path, "", {},
                                                body.empty() ? "" : util::sha256_hex(body),
                                                t.host);
            return c.Put(path, headers, body, body.empty() ? "" : "application/xml");
        });
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status / 100 == 2) co_return;
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::None, resource);
}

Task<void> CloudProxyBackend::delete_bucket(std::string_view bucket) {
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.bucket_path();
    auto res = co_await control_io([&] {
        return ctx_->with_retry("delete_bucket", [&](httplib::Client& c) {
            return c.Delete(path, ctx_->signed_headers("DELETE", path, "", {}, "", t.host));
        });
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status / 100 == 2) co_return;
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Bucket, resource_of(bucket));
}

Task<bool> CloudProxyBackend::bucket_exists(std::string_view bucket) {
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.bucket_path();
    auto res = co_await control_io([&] {
        return ctx_->with_retry("head_bucket", [&](httplib::Client& c) {
            return c.Head(path, ctx_->signed_headers("HEAD", path, "", {}, "", t.host));
        });
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
    // 服务级操作恒走 endpoint 本身，与寻址风格无关
    auto res = co_await control_io([&] {
        return ctx_->with_retry("list_buckets", [&](httplib::Client& c) {
            return c.Get("/", ctx_->signed_headers("GET", "/", "", {}, ""));
        });
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
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
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
    std::thread pump([ctx, queue, abortst, prom, path, extra, resource, keycopy,
                      host = t.host] {
        auto op_hist = ctx->metrics.op_seconds("get");  // §8.2：整段传输为一次观测
        bool delivered = false;
        try {
            for (int attempt = 0;; ++attempt) {
                std::string err_body;
                {
                    auto lease = ctx->pool.acquire();
                    abortst->arm(lease.client());
                    auto headers = ctx->signed_headers("GET", path, "", extra, "", host);
                    auto t0 = std::chrono::steady_clock::now();
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
                    op_hist->observe(std::chrono::duration<double>(
                                         std::chrono::steady_clock::now() - t0)
                                         .count());
                    if (delivered) {
                        queue->close(static_cast<bool>(res));  // res 空 = 传输中途失败
                        return;
                    }
                    // headers 未交付：重试或交付映射后的异常
                    bool retry = (!res ? RemoteContext::retryable_transport(res.error())
                                       : ctx->retryable_status(res->status)) &&
                                 attempt < ctx->cfg.retry_max;
                    if (!retry) {
                        try {
                            if (!res) ctx->throw_transport_error(res.error());
                            ctx->throw_remote_error(res->status, err_body, ErrCtx::Key,
                                                    resource);
                        } catch (...) {
                            prom->set_exception(std::current_exception());
                        }
                        return;
                    }
                    ctx->metrics.count_retry("get");
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
        // 池线程阻塞等首部；pump 在私有线程推进，无互等（§2.3）。等待须有上界
        //（docs §3.1：超时 = request_timeout）——滴流远端否则可让单次 Get 永不完成，
        // 并发 GET ≈ 池大小时把共享池占满、全局停摆。留一份连接建立预算的余量：
        // 重试链最坏是 (retry_max+1) 轮，每轮的实际 IO 由 httplib 自身超时兜住
        auto budget = std::chrono::milliseconds(ctx_->cfg.request_timeout_ms) *
                      (ctx_->cfg.retry_max + 1);
        if (fut.wait_for(budget) != std::future_status::ready) {
            abortst->abort();   // 打断在途 socket 读写，别陪绑到 httplib 超时
            queue->cancel();
            pump.join();
            ctx_->metrics.count_error("transport");
            throw S3Error(S3ErrorCode::SlowDown,
                          "cloudproxy: timed out waiting for remote response headers");
        }
        head = fut.get();
    } catch (...) {
        if (pump.joinable()) pump.join();
        throw;
    }
    ObjectStream out;
    out.meta = std::move(head.meta);
    out.range = head.range;
    out.body = std::make_unique<PumpBodyReader>(queue, head.body_len, abortst,
                                                std::move(pump), pool_);
    co_return out;
}

Task<PutResult> CloudProxyBackend::stream_upload(
    std::string raw_path, std::string raw_query, std::string host, std::string content_type,
    std::vector<std::pair<std::string, std::string>> extra, http::BodyReader& body,
    std::string resource, bool multipart_ctx) {
    auto len_opt = body.length();
    // AWS 不接受裸 chunked 上行（§3.2）。无长度（chunked 且无
    // x-amz-decoded-content-length）先 spool 到本地临时文件取得长度再上传
    // （docs/gaps.md §6.2——此前直接 NotImplemented，这类 PUT 在本后端整个不可用）
    if (!len_opt) {
        if (ctx_->cfg.spool_max_bytes == 0)
            throw S3Error(S3ErrorCode::NotImplemented,
                          "cloudproxy: upload without content length is not supported "
                          "(spool disabled)");
        co_return co_await spool_and_upload(std::move(raw_path), std::move(raw_query),
                                            std::move(host), std::move(content_type),
                                            std::move(extra), body, std::move(resource),
                                            multipart_ctx);
    }
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
    const char* op = multipart_ctx ? "upload_part" : "put";
    std::thread pump([ctx, queue, abortst, out, raw_path, raw_query, host, full, content_type,
                      extra, len, op] {
        auto op_hist = ctx->metrics.op_seconds(op);  // §8.2：整段传输为一次观测
        try {
            for (int attempt = 0;; ++attempt) {
                // pump 单线程读写即可，无需原子：仅用于连接阶段重试判定
                bool provider_called = false;
                auto lease = ctx->pool.acquire();
                abortst->arm(lease.client());
                auto headers = ctx->signed_headers("PUT", raw_path, raw_query, extra,
                                                   kUnsignedPayload, host);
                auto t0 = std::chrono::steady_clock::now();
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
                op_hist->observe(std::chrono::duration<double>(
                                     std::chrono::steady_clock::now() - t0)
                                     .count());
                // 仅连接建立阶段失败且 Provider 从未被调用（队列未被消费）可重试
                // （§5.2）；已被主动中止的传输不重试
                if (!res && !provider_called && !abortst->is_aborted() &&
                    RemoteContext::connection_stage_error(res.error()) &&
                    attempt < ctx->cfg.retry_max) {
                    ctx->metrics.count_retry(op);
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
        // 读到 EOF（n==0）为止而非 sent==len 即停：storage-backend 契约要求把 body
        // 读干——验签装饰器（sha256/chunked 校验）挂在读满/EOF 处，读满自停会跳过校验
        for (;;) {
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
            if (etag != md5.final_hex()) {
                ctx->metrics.etag_mismatch->inc();  // §8.2：在途损坏信号
                throw S3Error(S3ErrorCode::InternalError,
                              "cloudproxy: upload corrupted in transit (remote etag != "
                              "local md5)");
            }
        }
        co_return PutResult{etag};
    }
    ctx->throw_remote_error(out->status, out->resp_body,
                            multipart_ctx ? ErrCtx::Upload : ErrCtx::Bucket, resource);
}

// 无长度上行的 spool（docs/gaps.md §6.2）：body 全量落临时文件（O_TMPFILE 匿名
// inode，进程崩溃即自动回收；不支持的文件系统回退 unlink-after-open），得到长度
// 后经 FdStreamReader 走已知长度的 stream_upload。代价是一次本地盘写读与到齐
// 延迟——对"罕见路径可用性"的取舍
Task<PutResult> CloudProxyBackend::spool_and_upload(
    std::string raw_path, std::string raw_query, std::string host, std::string content_type,
    std::vector<std::pair<std::string, std::string>> extra, http::BodyReader& body,
    std::string resource, bool multipart_ctx) {
    co_await pool_->schedule();
    namespace fs = std::filesystem;
    fs::path dir = ctx_->cfg.spool_dir.empty() ? fs::temp_directory_path()
                                               : fs::path(ctx_->cfg.spool_dir);
    int fd = ::open(dir.c_str(), O_TMPFILE | O_RDWR, 0600);
    if (fd < 0) {
        // O_TMPFILE 不被支持（老内核/NFS）：具名建后立即 unlink，同样无残留
        fs::path p = dir / ("lights3-spool-" + std::to_string(::getpid()) + "-" +
                            std::to_string(reinterpret_cast<uintptr_t>(&body)));
        fd = ::open(p.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
        if (fd < 0)
            throw S3Error(S3ErrorCode::InternalError, "cloudproxy: cannot create spool file");
        ::unlink(p.c_str());
    }
    struct FdGuard {
        int fd;
        ~FdGuard() { ::close(fd); }
    } guard{fd};

    uint64_t total = 0;
    std::vector<std::byte> buf(256 * 1024);
    for (;;) {
        size_t n = co_await body.read(std::span(buf));
        co_await pool_->schedule();  // read 可能把协程恢复到驱动线程；写盘回池
        if (n == 0) break;
        total += n;
        if (total > ctx_->cfg.spool_max_bytes)
            throw S3Error(S3ErrorCode::EntityTooLarge,
                          "cloudproxy: unsized upload exceeds spool_max_bytes");
        const char* p = reinterpret_cast<const char*>(buf.data());
        size_t left = n;
        while (left > 0) {
            ssize_t w = ::write(fd, p, left);
            if (w < 0) throw S3Error(S3ErrorCode::InternalError, "cloudproxy: spool write failed");
            p += w;
            left -= size_t(w);
        }
    }
    // FdStreamReader 接管 fd 所有权；guard 让位
    int owned = fd;
    guard.fd = -1;
    fsutil::FdStreamReader replay(owned, 0, total, pool_);
    co_return co_await stream_upload(std::move(raw_path), std::move(raw_query), std::move(host),
                                     std::move(content_type), std::move(extra), replay,
                                     std::move(resource), multipart_ctx);
}

// 服务端 COPY（docs/gaps.md §6.2）：此前同 cloudproxy 后端内的 copy 会"下载到网关
// 再传回去"，2 倍跨网流量与费用；远端本可一个 x-amz-copy-source 完成。恒发
// REPLACE + 我方元数据——handler 已把 COPY/REPLACE 语义折算进 meta，远端只管照抄
Task<std::optional<PutResult>> CloudProxyBackend::copy_object_fast(
    std::string_view src_bucket, std::string_view src_key, std::string_view dst_bucket,
    std::string_view dst_key, ObjectMeta meta) {
    validate_object_key(src_key);
    validate_object_key(dst_key);
    auto src_rb = remote_bucket(src_bucket);
    auto dst_rb = remote_bucket(dst_bucket);
    auto tgt = ctx_->target(dst_rb);
    auto path = tgt.object_path(key_path(dst_key));
    auto resource = resource_of(dst_bucket, dst_key);

    std::vector<std::pair<std::string, std::string>> extra = meta_headers(meta);
    extra.emplace_back("x-amz-copy-source",
                       "/" + util::aws_uri_encode(src_rb, /*encode_slash=*/false) +
                           std::string(key_path(src_key)));
    extra.emplace_back("x-amz-metadata-directive", "REPLACE");

    auto res = co_await control_io([&] {
        auto op_hist = ctx_->metrics.op_seconds("copy");
        auto lease = ctx_->pool.acquire();
        auto t0 = std::chrono::steady_clock::now();
        auto headers = ctx_->signed_headers("PUT", path, "", extra,
                                            util::sha256_hex(""), tgt.host);
        auto r = lease.client().Put(path, headers, "", meta.content_type.empty()
                                                          ? "application/octet-stream"
                                                          : meta.content_type.c_str());
        op_hist->observe(
            std::chrono::duration<double>(std::chrono::steady_clock::now() - t0).count());
        return r;
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status != 200)
        ctx_->throw_remote_error(res->status, res->body, ErrCtx::Key, resource);
    // COPY 与 complete 同款陷阱：慢 copy 会先回 200、错误在 body（§4.4）
    s3::XmlNode root;
    try {
        root = s3::xml_parse(res->body);
    } catch (...) {
        throw S3Error(S3ErrorCode::InternalError,
                      "cloudproxy: remote returned unparsable CopyObjectResult body");
    }
    if (root.name == "Error") {
        auto code = map_remote_code(root.get("Code"));
        throw S3Error(code.value_or(S3ErrorCode::InternalError), root.get("Message"), resource);
    }
    co_return PutResult{std::string(strip_etag_quotes(root.get("ETag")))};
}

Task<PutResult> CloudProxyBackend::put_object(std::string_view bucket, std::string_view key,
                                              ObjectMeta meta, http::BodyReader& body,
                                              PutCondition cond) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    // 条件写透传给上游（S3 conditional writes）：检查与提交的原子性只能由持有
    // 对象的一方保证，代理侧任何 head+put 组合都守不住。上游若不支持会以
    // 4xx/501 拒绝，由 throw_remote_error 原样映射；412 → PreconditionFailed
    auto extra = meta_headers(meta);
    if (cond.if_none_match) extra.emplace_back("If-None-Match", "*");
    if (cond.if_match_etag) extra.emplace_back("If-Match", "\"" + *cond.if_match_etag + "\"");
    co_return co_await stream_upload(t.object_path(key_path(key)), "", t.host,
                                     meta.content_type, std::move(extra), body,
                                     resource_of(bucket, key), /*multipart_ctx=*/false);
}

Task<ObjectMeta> CloudProxyBackend::head_object(std::string_view bucket,
                                                std::string_view key) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
    auto res = co_await control_io([&] {
        return ctx_->with_retry("head", [&](httplib::Client& c) {
            return c.Head(path, ctx_->signed_headers("HEAD", path, "", {}, "", t.host));
        });
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status == 200) co_return meta_from_response(key, *res);
    // HEAD 无错误体：404 按上下文补 NoSuchKey（docs/cloudproxy-backend.md §4.1/§5.1）
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Key, resource_of(bucket, key));
}

Task<void> CloudProxyBackend::delete_object(std::string_view bucket, std::string_view key) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
    auto res = co_await control_io([&] {
        return ctx_->with_retry("delete", [&](httplib::Client& c) {
            return c.Delete(path, ctx_->signed_headers("DELETE", path, "", {}, "", t.host));
        });
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
    auto t = ctx_->target(rb);
    auto path = t.bucket_path();
    std::string query = "list-type=2&max-keys=" + std::to_string(opt.max_keys);
    if (!opt.prefix.empty()) query += "&prefix=" + qv(opt.prefix);
    if (!opt.delimiter.empty()) query += "&delimiter=" + qv(opt.delimiter);
    if (!opt.start_after.empty()) query += "&start-after=" + qv(opt.start_after);
    std::string full = path + "?" + query;

    auto res = co_await control_io([&] {
        return ctx_->with_retry("list", [&](httplib::Client& c) {
            return c.Get(full, ctx_->signed_headers("GET", path, query, {}, "", t.host));
        });
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
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
    std::string query = "uploads";
    std::string full = path + "?" + query;
    auto extra = meta_headers(meta);
    // 注：create 重试可能在远端留空孤儿 upload（§5.2 已知权衡，业界通行做法）——
    // 建议远端账号配 AbortIncompleteMultipartUpload 生命周期规则
    auto res = co_await control_io([&] {
        return ctx_->with_retry("create_multipart", [&](httplib::Client& c) {
            return c.Post(full, ctx_->signed_headers("POST", path, query, extra, "", t.host),
                          "", meta.content_type);
        });
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
    auto t = ctx_->target(rb);
    std::string query =
        "partNumber=" + std::to_string(part_no) + "&uploadId=" + qv(upload_id);
    co_return co_await stream_upload(t.object_path(key_path(key)), query, t.host, "", {},
                                     body, resource_of(bucket, key), /*multipart_ctx=*/true);
}

Task<PutResult> CloudProxyBackend::complete_multipart(std::string_view bucket,
                                                      std::string_view key,
                                                      std::string_view upload_id,
                                                      std::span<const PartInfo> parts) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto tgt = ctx_->target(rb);
    auto path = tgt.object_path(key_path(key));
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

    // 重试循环整段提取为阻塞函数：control_in_pump=true 时连同退避一起在私有
    // 线程执行（§2.3）；歧义消解要 co_await head_object，留在协程侧
    struct CompleteOutcome {
        std::string etag;
        std::exception_ptr ambiguous;
    };
    auto outcome = co_await control_io([&]() -> CompleteOutcome {
        auto op_hist = ctx_->metrics.op_seconds("complete_multipart");
        std::string etag_out;
        std::exception_ptr ambiguous_nosuch;
        for (int attempt = 0;; ++attempt) {
            auto res = [&] {
                auto lease = ctx_->pool.acquire();
                auto t0 = std::chrono::steady_clock::now();
                auto r = lease.client().Post(
                    full, ctx_->signed_headers("POST", path, query, {}, body_hash, tgt.host),
                    body, "application/xml");
                op_hist->observe(std::chrono::duration<double>(
                                     std::chrono::steady_clock::now() - t0)
                                     .count());
                return r;
            }();
            bool retry = !res ? RemoteContext::retryable_transport(res.error())
                              : ctx_->retryable_status(res->status);
            // S3 特有：complete 耗时长时先回 200，错误在 body 里（§4.4）。body 里的
            // InternalError/SlowDown 与同名 HTTP 状态是一回事，同样值得重试——不重试
            // 就直接变成对客户端的 500，客户端重试再走 NoSuchUpload 歧义消解，白白
            // 多一轮往返。重试后的 NoSuchUpload 由下面的 retried 分支消解
            if (!retry && res && res->status == 200 &&
                res->body.find("<Error") != std::string::npos) {
                try {
                    auto root = s3::xml_parse(res->body);
                    if (root.name == "Error") {
                        auto code = map_remote_code(root.get("Code"));
                        retry = code == S3ErrorCode::InternalError ||
                                code == S3ErrorCode::SlowDown;
                    }
                } catch (...) {  // 解析不了：留给下面的统一处置
                }
            }
            if (retry && attempt < ctx_->cfg.retry_max) {
                ctx_->metrics.count_retry("complete_multipart");
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
                                  "cloudproxy: remote returned unparsable "
                                  "CompleteMultipartUpload body");
                }
                if (root.name == "Error") {
                    auto code = map_remote_code(root.get("Code"));
                    throw S3Error(code.value_or(S3ErrorCode::InternalError),
                                  root.get("Message"), resource);
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
        return {std::move(etag_out), ambiguous_nosuch};
    });
    std::string etag_out = std::move(outcome.etag);
    if (outcome.ambiguous) {
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
        if (!completed_before) std::rethrow_exception(outcome.ambiguous);
        etag_out = expect;
    }
    co_return PutResult{etag_out};
}

Task<void> CloudProxyBackend::abort_multipart(std::string_view bucket, std::string_view key,
                                              std::string_view upload_id) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
    std::string query = "uploadId=" + qv(upload_id);
    std::string full = path + "?" + query;
    auto res = co_await control_io([&] {
        return ctx_->with_retry("abort_multipart", [&](httplib::Client& c) {
            return c.Delete(full, ctx_->signed_headers("DELETE", path, query, {}, "", t.host));
        });
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status / 100 == 2) co_return;
    ctx_->throw_remote_error(res->status, res->body, ErrCtx::Upload,
                             resource_of(bucket, key));
}

// 契约带上分页位之后，这里由"累积全部页再返回"改为**转发一页**（docs/gaps.md
// §5.1）：客户端的 marker 直接成为远端的 marker，远端的 IsTruncated 原样回传。
// 此前为了满足裸 vector 契约必须把远端所有页拉完，客户端要第一页也得等全量
Task<ListPartsResult> CloudProxyBackend::list_parts(std::string_view bucket,
                                                    std::string_view key,
                                                    std::string_view upload_id,
                                                    const ListPartsOptions& opt) {
    validate_object_key(key);
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.object_path(key_path(key));
    auto resource = resource_of(bucket, key);
    ListPartsResult out;
    if (opt.max_parts <= 0) co_return out;

    std::string query = "uploadId=" + qv(upload_id) +
                        "&max-parts=" + std::to_string(opt.max_parts);
    if (opt.part_number_marker > 0)
        query += "&part-number-marker=" + std::to_string(opt.part_number_marker);
    std::string full = path + "?" + query;
    auto res = co_await control_io([&] {
        return ctx_->with_retry("list_parts", [&](httplib::Client& c) {
            return c.Get(full, ctx_->signed_headers("GET", path, query, {}, "", t.host));
        });
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
        if (auto ts = util::parse_iso8601(child.get("LastModified"))) p.last_modified = *ts;
        out.parts.push_back(std::move(p));
    }
    out.is_truncated = root.get("IsTruncated") == "true";
    if (out.is_truncated) {
        out.next_part_number_marker =
            static_cast<int>(parse_u64(root.get("NextPartNumberMarker")));
        // 远端截断了却不给游标：续传无从下手，宁可如实报到尾也不让客户端死循环
        if (out.next_part_number_marker == 0 && !out.parts.empty())
            out.next_part_number_marker = out.parts.back().part_no;
    }
    co_return out;
}

Task<ListUploadsResult> CloudProxyBackend::list_multipart_uploads(
    std::string_view bucket, const ListUploadsOptions& opt) {
    auto rb = remote_bucket(bucket);
    auto t = ctx_->target(rb);
    auto path = t.bucket_path();
    auto resource = resource_of(bucket);
    ListUploadsResult out;
    if (opt.max_uploads <= 0) co_return out;

    std::string query = "uploads&max-uploads=" + std::to_string(opt.max_uploads);
    if (!opt.prefix.empty()) query += "&prefix=" + qv(opt.prefix);
    if (!opt.delimiter.empty()) query += "&delimiter=" + qv(opt.delimiter);
    if (!opt.key_marker.empty())
        query += "&key-marker=" + qv(opt.key_marker) +
                 "&upload-id-marker=" + qv(opt.upload_id_marker);
    std::string full = path + "?" + query;
    auto res = co_await control_io([&] {
        return ctx_->with_retry("list_uploads", [&](httplib::Client& c) {
            return c.Get(full, ctx_->signed_headers("GET", path, query, {}, "", t.host));
        });
    });
    if (!res) ctx_->throw_transport_error(res.error());
    if (res->status != 200)
        ctx_->throw_remote_error(res->status, res->body, ErrCtx::Bucket, resource);
    auto root = s3::xml_parse(res->body);
    for (auto& child : root.children) {
        if (child.name == "Upload") {
            UploadInfo u;
            u.key = child.get("Key");
            u.upload_id = child.get("UploadId");
            if (auto ts = util::parse_iso8601(child.get("Initiated"))) u.initiated = *ts;
            out.uploads.push_back(std::move(u));
        } else if (child.name == "CommonPrefixes") {
            out.common_prefixes.push_back(child.get("Prefix"));
        }
    }
    out.is_truncated = root.get("IsTruncated") == "true";
    if (out.is_truncated) {
        out.next_key_marker = root.get("NextKeyMarker");
        out.next_upload_id_marker = root.get("NextUploadIdMarker");
        // 同上：远端不给游标就别把 truncated 传下去，否则客户端原地打转
        if (out.next_key_marker.empty() && !out.uploads.empty()) {
            out.next_key_marker = out.uploads.back().key;
            out.next_upload_id_marker = out.uploads.back().upload_id;
        } else if (out.next_key_marker.empty()) {
            out.is_truncated = false;
        }
    }
    co_return out;
}

}  // namespace lights3::storage
