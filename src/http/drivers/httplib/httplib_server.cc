// L1: cpp-httplib driver — synchronous model, thread-per-request (docs/http-adapter.md §3.2).
// The request thread blocks in sync_wait(handler(req)) until the coroutine
// completes; the home executor is inline. httplib's ContentReader is a push
// model, inverted into a pull model by a pump thread through a bounded buffer
// queue; the queue capacity is the backpressure: if storage writes slowly, the
// pump stalls in push and the socket stops receiving.
// Positioning: functional validation, low-concurrency scenarios; not a performance path.
#include <httplib/httplib.h>

#include <atomic>
#include <cstring>
#include <memory>
#include <thread>

#include "core/log.h"
#include "core/task.h"
#include "core/util/time.h"
#include "http/drivers/common.h"
#include "http/pushpull.h"
#include "http/server.h"

namespace lights3::http {

namespace {

// The push-to-pull BlockQueue / QueueBodyReader have been extracted into a
// shared component (http/pushpull.h, also used by the cloudproxy backend,
// docs/cloudproxy-backend.md §3.1)

// Connection-info pseudo-headers httplib injects into headers in process_request; not part of the HTTP message
bool is_pseudo_header(const std::string& k) {
    return k == "REMOTE_ADDR" || k == "REMOTE_PORT" || k == "LOCAL_ADDR" || k == "LOCAL_PORT";
}

// Copies a fallback response into httplib::Response (docs/archive/gaps.md §4):
// previously each call site copied only status and body, leaving the
// x-amz-request-id header behind in HttpResponse — id in the XML but not in
// the headers, an inconsistency none of the other three drivers exhibit
void apply_fallback(httplib::Response& rs, const HttpResponse& src) {
    rs.status = src.status;
    rs.set_content(src.small_body, "application/xml");
    if (auto* rid = src.headers.find("x-amz-request-id")) rs.set_header("x-amz-request-id", *rid);
}

class HttplibServer final : public IHttpServer {
public:
    explicit HttplibServer(const HttpConfig& cfg) : cfg_(cfg) {
        // TLS (docs/archive/gaps.md §7): SSLServer is a subclass of Server and loads
        // the certificate at construction. Failure must throw right here
        // (when is_valid() is false, listen just fails silently)
        if (!cfg.tls_cert.empty()) {
#ifdef CPPHTTPLIB_OPENSSL_SUPPORT
            auto ssl = std::make_unique<httplib::SSLServer>(cfg.tls_cert.c_str(),
                                                            cfg.tls_key.c_str());
            if (!ssl->is_valid())
                throw std::runtime_error("httplib driver: failed to load TLS cert/key (" +
                                         cfg.tls_cert + ", " + cfg.tls_key + ")");
            svr_ = std::move(ssl);
            tls_ = true;
#else
            // The macro is defined whenever any httplib consumer (this driver
            // or cloudproxy) is enabled on the CMake side; reaching here means
            // the build configuration was hand-edited
            throw std::runtime_error(
                "httplib driver was built without CPPHTTPLIB_OPENSSL_SUPPORT; TLS unavailable");
#endif
        } else {
            svr_ = std::make_unique<httplib::Server>();
        }
        // Upstream has a compile-time limit on a **single** header line,
        // CPPHTTPLIB_HEADER_MAX_LENGTH (8KiB), not configurable. When the
        // configured value exceeds it, over-long single lines are still
        // rejected by upstream first — behavior differing from the other three
        // drivers — so warn explicitly instead of letting it apply silently
        if (cfg.max_header_size > CPPHTTPLIB_HEADER_MAX_LENGTH)
            LOG_WARN(
                "httplib driver: single header lines above {} bytes are rejected by the upstream "
                "parser regardless of http.max_header_size={}",
                int(CPPHTTPLIB_HEADER_MAX_LENGTH), cfg.max_header_size);
        svr_->new_task_queue = [n = std::max(cfg.io_threads, 8)] {
            return new httplib::ThreadPool(static_cast<size_t>(n));
        };
        svr_->set_tcp_nodelay(true);
        svr_->set_read_timeout(cfg.idle_timeout_sec);
        svr_->set_write_timeout(cfg.idle_timeout_sec);
        svr_->set_keep_alive_timeout(cfg.idle_timeout_sec);
        svr_->set_keep_alive_max_count(1024);
        svr_->set_exception_handler(
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
        // Expect: 100-continue (docs/http-adapter.md §3.1 requires deferred
        // reply). The upstream API offers only three outcomes: reply 100
        // immediately / reply 417 / close the connection with a final
        // response — "suppress the automatic reply and send 100 after the
        // handler decides" is not expressible in v0.20; a known limitation of
        // this driver (it targets functional validation, and the useless body
        // after a mistaken 100 is contained by handle()'s 4MiB bounded drain
        // + connection close). What we can do here: reject framing violations
        // with 400 before inviting the client to upload, instead of
        // "reply 100 first, then reject"
        svr_->set_expect_100_continue_handler(
            [](const httplib::Request& rq, httplib::Response& rs) {
                HeaderMap headers;
                for (auto& [k, v] : rq.headers)
                    if (!is_pseudo_header(k)) headers.add(k, v);
                if (!driver::parse_body_framing(headers).valid) {
                    auto bad = driver::bad_request_response("Invalid message framing.");
                    apply_fallback(rs, bad);
                    return bad.status;  // Not 100/417: upstream sends this response and closes the connection
                }
                return static_cast<int>(httplib::StatusCode::Continue_100);
            });

        // Error responses produced by upstream itself (unregistered method ->
        // routing 404, invalid request line/headers -> 400, etc.) previously
        // went out as httplib's non-S3 messages, breaking "all four drivers
        // accept/reject the same set". Normalize them into S3 XML here; a
        // non-empty body means the handler rendered its own error, so leave
        // it alone. A routing 404 can only mean "method not registered"
        // (".*" is registered below for all business methods), so translate
        // it to 405 MethodNotAllowed like the other three drivers
        svr_->set_error_handler([](const httplib::Request&, httplib::Response& rs) {
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
            int status = rs.status;  // Already set per upstream semantics above; must not be overridden by the fallback mapping
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
        svr_->Get(pat, no_body);       // HEAD reuses the Get route in httplib
        svr_->Options(pat, no_body);
        svr_->Post(pat, with_body);
        svr_->Put(pat, with_body);
        svr_->Patch(pat, with_body);
        svr_->Delete(pat, with_body);
    }

    void set_handler(Handler h) override { handler_ = std::move(h); }

    void listen(const std::string& addr, uint16_t port) override {
        if (port == 0) {
            int p = svr_->bind_to_any_port(addr);
            if (p <= 0) throw std::runtime_error("httplib bind failed on " + addr);
            port_ = static_cast<uint16_t>(p);
        } else {
            if (!svr_->bind_to_port(addr, port))
                throw std::runtime_error("httplib bind failed on " + addr + ":" +
                                         std::to_string(port));
            port_ = port;
        }
        LOG_INFO("httplib http{} server listening on {}:{}", tls_ ? "s" : "", addr, port_);
    }

    uint16_t bound_port() const override { return port_; }

    void run() override {
        // Compensates for shutdown arriving before run (same as the other
        // three drivers): without this check, stop() in that ordering is a
        // no-op (is_running_ not yet set) and listen never returns
        if (!stopping_.load()) {
            svr_->listen_after_bind();  // httplib's thread pool is joined before returning (in-flight requests finished)
        }
        LOG_INFO("httplib http server stopped");
    }

    void shutdown() override {
        stopping_.store(true);
        // Order-sensitive: stop() resets is_decommissioned, so it must come
        // before decommission. Already running -> stop() closes the listening
        // socket so the loop exits; not yet running -> decommission() makes
        // the subsequent listen_after_bind() return immediately. A
        // nanosecond-scale window still exists between upstream
        // listen_internal's decommission check and setting is_running_ —
        // an upstream API limitation
        svr_->stop();
        svr_->decommission();
    }

private:
    void handle(const httplib::Request& rq, httplib::Response& rs,
                const httplib::ContentReader* content_reader) {
        // Range is handled by L2, which replies 206 directly; clear ranges so httplib does not re-slice 2xx responses
        const_cast<httplib::Request&>(rq).ranges.clear();

        HttpRequest req;
        req.method = rq.method;
        driver::parse_target(rq.target, req);
        for (auto& [k, v] : rq.headers)
            if (!is_pseudo_header(k)) req.headers.add(k, v);
        req.remote_addr = rq.remote_addr;

        // Message framing validation (drivers/common.h parse_body_framing):
        // httplib itself is more lenient about CL/TE conflicts, non-numeric
        // Content-Length, etc.; reject all of them at L1 and close the
        // connection, so all four drivers accept/reject the same request set
        auto reject = [&](const char* why) {
            apply_fallback(rs, driver::bad_request_response(why));
            rs.set_header("Connection", "close");  // Framing is suspect; do not reuse the connection
        };
        auto framing = driver::parse_body_framing(req.headers);
        if (!framing.valid) {
            reject("Invalid message framing.");
            return;
        }
        // http.max_header_size was previously ignored entirely by this driver
        // (the other three all reject by it), making the four drivers'
        // accepted sets inconsistent. Upstream only has the compile-time
        // single-line cap CPPHTTPLIB_HEADER_MAX_LENGTH; the configured value
        // gets an aggregate check here
        size_t header_bytes = 0;
        for (auto& [k, v] : rq.headers)
            if (!is_pseudo_header(k)) header_bytes += k.size() + v.size() + 4;  // ": " + CRLF
        if (header_bytes > cfg_.max_header_size) {
            apply_fallback(rs, driver::upstream_error_response(s3::S3ErrorCode::InvalidRequest,
                                                              "Request header fields too large."));
            rs.status = 431;  // InvalidRequest maps to 400; reply 431 here per upstream semantics
            rs.set_header("Connection", "close");
            return;
        }
        std::optional<uint64_t> content_length = framing.content_length;
        bool chunked = framing.chunked;
        // Connection is a token list: upstream only compares for full
        // equality, so "close, Upgrade" would be treated as keep-alive and
        // the response would lack Connection: close (the other three drivers
        // all match by token). Decide here ourselves and add the response
        // header so the client closes the connection
        bool client_wants_close = req.headers.has_token("Connection", "close");

        // httplib provides no ContentReader for GET/OPTIONS routes: such
        // requests with a non-zero body cannot satisfy the BodyReader
        // contract (length() reports N but EOF is immediate), so reply 400
        if (!content_reader && ((content_length && *content_length > 0) || chunked)) {
            reject("Request body is not supported for this method.");
            return;
        }

        // Push-to-pull: the pump thread drives the ContentReader to fill the
        // queue; the request thread runs the req_exec queue inside
        // sync_wait_pumping, and the body's cv blocking switches back to the
        // request thread to execute (docs/archive/gaps.md §2.10), not occupying a
        // shared pool thread
        PumpExecutor req_exec;
        std::shared_ptr<BlockQueue> queue;
        std::thread pump;
        if (content_reader && (chunked || (content_length && *content_length > 0))) {
            queue = std::make_shared<BlockQueue>(cfg_.body_queue_cap);
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
            // L2 catches all exceptions; reaching here means something failed outside L2 (contract 2)
            resp = driver::internal_error_response(e.what());
        }

        if (pump.joinable()) {
            // The handler may not have read the whole body: drain a bounded amount to keep the connection, cancel if too large (connection closes afterwards)
            try {
                std::byte tmp[driver::kScratchBytes];
                uint64_t drained = 0;
                for (;;) {
                    size_t n = queue->pop(std::span(tmp));
                    if (n == 0) break;
                    drained += n;
                    if (drained > cfg_.drain_limit) {
                        queue->cancel();
                        break;
                    }
                }
            } catch (...) {
                // Client disconnected: the pump has already wrapped up
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
                continue;  // The set_content family writes it; avoid duplication
            }
            // Length/encoding/connection management is httplib's internal responsibility (contract 5)
            if (HeaderMap::ieq(k, "Content-Length") || HeaderMap::ieq(k, "Transfer-Encoding") ||
                HeaderMap::ieq(k, "Connection") || HeaderMap::ieq(k, "Keep-Alive"))
                continue;
            rs.set_header(k, v);
        }
        if (!rs.has_header("Date"))
            rs.set_header("Date", util::http_date(std::chrono::system_clock::now()));

        if (head_request) {
            // HEAD skips the set_content family (httplib writes no body); the
            // headers are set directly. When the length is unknown (streaming
            // without content_length), write neither framing header and close
            // the connection (drivers/common.h contract 6, uniform across the
            // four drivers) — writing 0 would be a lie
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

        // Streaming response: httplib's content provider is itself a pull
        // model, so sync_wait per chunk suffices. Ownership of the reader and
        // the reused buffer goes to the closure (the response is written out
        // after this callback returns); the buffer lives with the closure
        // (docs/archive/gaps.md §4: previously a vector was constructed per 64KiB chunk)
        std::shared_ptr<BodyReader> body(std::move(resp.stream_body));
        auto buf = std::make_shared<std::vector<std::byte>>(cfg_.io_chunk_size);
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
                        return false;  // Response head already sent; can only disconnect (contract 3)
                    }
                    if (n == 0) return false;  // EOF before the length is reached counts as an error
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
    std::unique_ptr<httplib::Server> svr_;  // Actually an SSLServer under TLS (decided in the constructor)
    bool tls_ = false;
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
