// L1: cpp-httplib 驱动 —— 同步模型，thread-per-request（docs/http-adapter.md §3.2）。
// 请求线程 sync_wait(handler(req)) 阻塞至协程完成；home executor 为 inline。
// httplib 的 ContentReader 是推模型，由 pump 线程经有界缓冲队列翻转成拉模型，
// 队列容量即背压：存储写得慢，pump 就停在 push，socket 停止收数据。
// 定位：功能验证、低并发场景；不是性能路径。
#include <httplib/httplib.h>

#include <atomic>
#include <cstring>
#include <thread>

#include "core/log.h"
#include "core/task.h"
#include "core/util/time.h"
#include "http/drivers/common.h"
#include "http/pushpull.h"
#include "http/server.h"

namespace lights3::http {

namespace {

// 推转拉的 BlockQueue / QueueBodyReader 已提取为共享组件（http/pushpull.h，
// cloudproxy 后端同用，docs/cloudproxy-backend.md §3.1）

// httplib 在 process_request 里塞进 headers 的连接信息伪头，不属于 HTTP 报文
bool is_pseudo_header(const std::string& k) {
    return k == "REMOTE_ADDR" || k == "REMOTE_PORT" || k == "LOCAL_ADDR" || k == "LOCAL_PORT";
}

// 兜底响应搬进 httplib::Response（docs/gaps.md §4）：此前各处只搬 status 与 body，
// x-amz-request-id 头被丢在 HttpResponse 里——XML 里有 id 而头里没有，恰是另三驱动
// 都不会出现的不一致
void apply_fallback(httplib::Response& rs, const HttpResponse& src) {
    rs.status = src.status;
    rs.set_content(src.small_body, "application/xml");
    if (auto* rid = src.headers.find("x-amz-request-id")) rs.set_header("x-amz-request-id", *rid);
}

class HttplibServer final : public IHttpServer {
public:
    explicit HttplibServer(const HttpConfig& cfg) : cfg_(cfg) {
        // 上游对**单行**头有编译期上限 CPPHTTPLIB_HEADER_MAX_LENGTH（8KiB），
        // 不可配。配置值大于它时超长单行仍会被上游先拒掉，行为与另三驱动不同——
        // 明确告警而不是让它悄悄生效
        if (cfg.max_header_size > CPPHTTPLIB_HEADER_MAX_LENGTH)
            LOG_WARN(
                "httplib driver: single header lines above {} bytes are rejected by the upstream "
                "parser regardless of http.max_header_size={}",
                int(CPPHTTPLIB_HEADER_MAX_LENGTH), cfg.max_header_size);
        svr_.new_task_queue = [n = std::max(cfg.io_threads, 8)] {
            return new httplib::ThreadPool(static_cast<size_t>(n));
        };
        svr_.set_tcp_nodelay(true);
        svr_.set_read_timeout(cfg.idle_timeout_sec);
        svr_.set_write_timeout(cfg.idle_timeout_sec);
        svr_.set_keep_alive_timeout(cfg.idle_timeout_sec);
        svr_.set_keep_alive_max_count(1024);
        svr_.set_exception_handler(
            [](const httplib::Request&, httplib::Response& rs, std::exception_ptr ep) {
                std::string what;
                try {
                    if (ep) std::rethrow_exception(ep);
                } catch (const std::exception& e) {
                    what = e.what();
                } catch (...) {
                    what = "<non-std exception>";
                }
                apply_fallback(rs, driver::internal_error_response(what));
            });
        // Expect: 100-continue（docs/http-adapter.md §3.1 要求延迟应答）。
        // 上游 API 只有三种出路：立即回 100 / 回 417 / 以最终响应关连接——
        // "抑制自动应答、handler 决定后再回 100"在 v0.20 不可表达，属该驱动的
        // 已知限制（本驱动定位功能验证，误发 100 后的无效 body 由 handle() 的
        // 4MiB 有界排空 + 关连接兜住）。此处能做的：消息边界违规在邀请客户端
        // 上传之前就以 400 拒绝，不再是"先回 100 再拒"
        svr_.set_expect_100_continue_handler(
            [](const httplib::Request& rq, httplib::Response& rs) {
                HeaderMap headers;
                for (auto& [k, v] : rq.headers)
                    if (!is_pseudo_header(k)) headers.add(k, v);
                if (!driver::parse_body_framing(headers).valid) {
                    auto bad = driver::bad_request_response("Invalid message framing.");
                    apply_fallback(rs, bad);
                    return bad.status;  // 非 100/417：上游发出该响应并关连接
                }
                return static_cast<int>(httplib::StatusCode::Continue_100);
            });

        // 上游自产的错误响应（未注册方法 → 路由 404、请求行/头非法 → 400 等）
        // 此前直接回 httplib 的非 S3 报文，破坏"四驱动接受/拒绝集合一致"。这里
        // 统一补成 S3 XML；body 非空说明是 handler 自己渲染的错误，不覆盖。
        // 路由 404 只可能是"方法没注册"（下面对全部业务方法注册了 ".*"），
        // 与另三驱动一致地翻译成 405 MethodNotAllowed
        svr_.set_error_handler([](const httplib::Request&, httplib::Response& rs) {
            using HR = httplib::Server::HandlerResponse;
            if (!rs.body.empty()) return HR::Unhandled;
            s3::S3ErrorCode code = s3::S3ErrorCode::InternalError;
            switch (rs.status) {
                case 404: rs.status = 405; code = s3::S3ErrorCode::MethodNotAllowed; break;
                case 405: code = s3::S3ErrorCode::MethodNotAllowed; break;
                case 413: code = s3::S3ErrorCode::EntityTooLarge; break;
                case 400:
                case 431: code = s3::S3ErrorCode::InvalidRequest; break;
                case 501:
                case 505: code = s3::S3ErrorCode::NotImplemented; break;
                default: if (rs.status < 500) code = s3::S3ErrorCode::InvalidRequest; break;
            }
            int status = rs.status;  // 上面已按上游语义定好，不能被兜底的映射覆盖
            apply_fallback(rs,
                           driver::upstream_error_response(code, "Request rejected by the HTTP layer."));
            rs.status = status;
            return HR::Handled;
        });

        const std::string pat = ".*";
        auto no_body = [this](const httplib::Request& rq, httplib::Response& rs) {
            handle(rq, rs, nullptr);
        };
        auto with_body = [this](const httplib::Request& rq, httplib::Response& rs,
                                const httplib::ContentReader& cr) { handle(rq, rs, &cr); };
        svr_.Get(pat, no_body);       // HEAD 由 httplib 复用 Get 路由
        svr_.Options(pat, no_body);
        svr_.Post(pat, with_body);
        svr_.Put(pat, with_body);
        svr_.Patch(pat, with_body);
        svr_.Delete(pat, with_body);
    }

    void set_handler(Handler h) override { handler_ = std::move(h); }

    void listen(const std::string& addr, uint16_t port) override {
        if (port == 0) {
            int p = svr_.bind_to_any_port(addr);
            if (p <= 0) throw std::runtime_error("httplib bind failed on " + addr);
            port_ = static_cast<uint16_t>(p);
        } else {
            if (!svr_.bind_to_port(addr, port))
                throw std::runtime_error("httplib bind failed on " + addr + ":" +
                                         std::to_string(port));
            port_ = port;
        }
        LOG_INFO("httplib http server listening on {}:{}", addr, port_);
    }

    uint16_t bound_port() const override { return port_; }

    void run() override {
        // shutdown 早于 run 的补偿（另三驱动同款）：不检查则该顺序下 stop() 是
        // no-op（is_running_ 未置位），listen 永不返回
        if (!stopping_.load()) {
            svr_.listen_after_bind();  // 返回前 httplib 线程池已 join（在途请求跑完）
        }
        LOG_INFO("httplib http server stopped");
    }

    void shutdown() override {
        stopping_.store(true);
        // 顺序敏感：stop() 会把 is_decommissioned 复位，必须先 stop 后 decommission。
        // 已在运行 → stop() 关监听 socket 使循环退出；尚未运行 → decommission()
        // 使随后的 listen_after_bind() 立即返回。上游 listen_internal 的
        // decommission 检查与 is_running_ 置位之间仍有纳秒级窗口，属上游 API 限制
        svr_.stop();
        svr_.decommission();
    }

private:
    void handle(const httplib::Request& rq, httplib::Response& rs,
                const httplib::ContentReader* content_reader) {
        // Range 由 L2 处理并直接回 206；清掉 ranges 防止 httplib 对 2xx 响应二次切片
        const_cast<httplib::Request&>(rq).ranges.clear();

        HttpRequest req;
        req.method = rq.method;
        driver::parse_target(rq.target, req);
        for (auto& [k, v] : rq.headers)
            if (!is_pseudo_header(k)) req.headers.add(k, v);
        req.remote_addr = rq.remote_addr;

        // 消息边界校验（drivers/common.h parse_body_framing）：httplib 自身对
        // CL/TE 冲突、非数字 Content-Length 等更宽松，一律在 L1 拒绝并关连接，
        // 保证四个驱动接受/拒绝的请求集合一致
        auto reject = [&](const char* why) {
            apply_fallback(rs, driver::bad_request_response(why));
            rs.set_header("Connection", "close");  // 边界已存疑，不复用连接
        };
        auto framing = driver::parse_body_framing(req.headers);
        if (!framing.valid) {
            reject("Invalid message framing.");
            return;
        }
        // http.max_header_size 此前被本驱动完全忽略（另三驱动都按它拒收），
        // 四驱动的接受集合因此不一致。上游只有编译期的单行上限
        // CPPHTTPLIB_HEADER_MAX_LENGTH，配置值在这里补做整体校验
        size_t header_bytes = 0;
        for (auto& [k, v] : rq.headers)
            if (!is_pseudo_header(k)) header_bytes += k.size() + v.size() + 4;  // ": " + CRLF
        if (header_bytes > cfg_.max_header_size) {
            apply_fallback(rs, driver::upstream_error_response(s3::S3ErrorCode::InvalidRequest,
                                                              "Request header fields too large."));
            rs.status = 431;  // InvalidRequest 映射 400，此处按上游语义回 431
            rs.set_header("Connection", "close");
            return;
        }
        std::optional<uint64_t> content_length = framing.content_length;
        bool chunked = framing.chunked;
        // Connection 是 token 列表：上游只做全等比较，"close, Upgrade" 会被当成
        // keep-alive，响应里也不会带 Connection: close（另三驱动都按 token 判定）。
        // 这里自行判定并补上响应头，客户端据此关连接
        bool client_wants_close = req.headers.has_token("Connection", "close");

        // httplib 不给 GET/OPTIONS 路由提供 ContentReader：带非零 body 的这类
        // 请求无法满足 BodyReader 契约（length() 报 N 但立即 EOF），直接 400
        if (!content_reader && ((content_length && *content_length > 0) || chunked)) {
            reject("Request body is not supported for this method.");
            return;
        }

        // 推转拉：pump 线程驱动 ContentReader 往队列灌；请求线程在
        // sync_wait_pumping 里运行 req_exec 队列，body 的 cv 阻塞切回请求线程
        // 执行（docs/gaps.md §2.10），不占共享池线程
        PumpExecutor req_exec;
        std::shared_ptr<BlockQueue> queue;
        std::thread pump;
        if (content_reader && (chunked || (content_length && *content_length > 0))) {
            queue = std::make_shared<BlockQueue>(256 * 1024);
            req.body = std::make_unique<QueueBodyReader>(queue, content_length, &req_exec);
            pump = std::thread([content_reader, queue] {
                bool ok = (*content_reader)(
                    [&](const char* data, size_t n) { return queue->push(data, n); });
                queue->close(ok);
            });
        } else if (content_length) {
            req.body = std::make_unique<StringBodyReader>("");  // Content-Length: 0
        }

        HttpResponse resp;
        try {
            resp = sync_wait_pumping(req_exec, handler_(std::move(req)));
        } catch (const std::exception& e) {
            // L2 会兜底一切异常，到这里说明 L2 之外出了问题（契约 2）
            resp = driver::internal_error_response(e.what());
        }

        if (pump.joinable()) {
            // handler 可能没读完 body：有限排空以保住连接，过大则取消（连接随后关闭）
            try {
                std::byte tmp[driver::kScratchBytes];
                uint64_t drained = 0;
                for (;;) {
                    size_t n = queue->pop(std::span(tmp));
                    if (n == 0) break;
                    drained += n;
                    if (drained > driver::kDrainMaxBytes) {
                        queue->cancel();
                        break;
                    }
                }
            } catch (...) {
                // 客户端断连：pump 已收尾
            }
            queue->cancel();
            pump.join();
        }

        write_response(std::move(resp), rs, rq.method == "HEAD");
        if (client_wants_close) rs.set_header("Connection", "close");
    }

    void write_response(HttpResponse resp, httplib::Response& rs, bool head_request) {
        rs.status = resp.status;
        std::string content_type = "application/octet-stream";
        bool has_content_type = false;
        for (auto& [k, v] : resp.headers.items()) {
            if (HeaderMap::ieq(k, "Content-Type")) {
                content_type = v;
                has_content_type = true;
                continue;  // set_content 系列会写入，避免重复
            }
            // 长度/编码/连接管理是 httplib 的内部职责（契约 5）
            if (HeaderMap::ieq(k, "Content-Length") || HeaderMap::ieq(k, "Transfer-Encoding") ||
                HeaderMap::ieq(k, "Connection") || HeaderMap::ieq(k, "Keep-Alive"))
                continue;
            rs.set_header(k, v);
        }
        if (!rs.has_header("Date"))
            rs.set_header("Date", util::http_date(std::chrono::system_clock::now()));

        if (head_request) {
            // HEAD 不经 set_content 系列（httplib 不写 body），头部直接给出。
            // 长度未知（流式无 content_length）时两个框架头都不写并关连接
            //（drivers/common.h 契约 6，四驱动统一）——写 0 是撒谎
            if (has_content_type) rs.set_header("Content-Type", content_type);
            if (driver::head_length_known(resp))
                rs.set_header("Content-Length",
                              std::to_string(resp.content_length.value_or(resp.small_body.size())));
            else
                rs.set_header("Connection", "close");
            return;
        }

        if (!resp.stream_body) {
            if (!resp.small_body.empty())
                rs.set_content(std::move(resp.small_body), content_type);
            return;
        }

        // 流式响应：httplib 的 content provider 本身是拉模型，逐块 sync_wait 即可。
        // reader 与复用缓冲的所有权都交给闭包（响应写出发生在本回调返回之后）；
        // 缓冲随闭包存活（docs/gaps.md §4：此前每 64KiB 块构造一次 vector）
        std::shared_ptr<BodyReader> body(std::move(resp.stream_body));
        auto buf = std::make_shared<std::vector<std::byte>>(driver::kIoChunkBytes);
        if (resp.content_length) {
            rs.set_content_provider(
                static_cast<size_t>(*resp.content_length), content_type,
                [body, buf](size_t /*offset*/, size_t length, httplib::DataSink& sink) {
                    size_t want = std::min<size_t>(length, buf->size());
                    size_t n = 0;
                    try {
                        n = sync_wait(body->read(std::span(buf->data(), want)));
                    } catch (const std::exception& e) {
                        LOG_ERROR("stream body read failed mid-response: {}", e.what());
                        return false;  // 响应头已发出，只能断连（契约 3）
                    }
                    if (n == 0) return false;  // 长度未到就 EOF，视为错误
                    return sink.write(reinterpret_cast<const char*>(buf->data()), n);
                });
        } else {
            rs.set_chunked_content_provider(
                content_type, [body, buf](size_t /*offset*/, httplib::DataSink& sink) {
                    size_t n = 0;
                    try {
                        n = sync_wait(body->read(std::span(*buf)));
                    } catch (const std::exception& e) {
                        LOG_ERROR("stream body read failed mid-response: {}", e.what());
                        return false;
                    }
                    if (n == 0) {
                        sink.done();
                        return true;
                    }
                    return sink.write(reinterpret_cast<const char*>(buf->data()), n);
                });
        }
    }

    HttpConfig cfg_;
    Handler handler_;
    httplib::Server svr_;
    uint16_t port_ = 0;
    std::atomic<bool> stopping_{false};
};

}  // namespace

void register_httplib_driver() {
    HttpServerFactory::register_driver("httplib", [](const HttpConfig& cfg) {
        return std::make_unique<HttplibServer>(cfg);
    });
}

}  // namespace lights3::http
