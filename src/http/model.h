// L1/L2 boundary: HTTP-neutral model (see docs/http-adapter.md)
// This header depends only on the standard library and core/task.h; no HTTP
// library types may appear here.
#pragma once

#include <cstddef>
#include <cstdint>
#include <cstring>
#include <memory>
#include <optional>
#include <span>
#include <string>
#include <utility>
#include <vector>

#include "core/task.h"

namespace lights3::http {

// Case-insensitive, order-preserving header table
class HeaderMap {
public:
    void add(std::string key, std::string value) {
        items_.emplace_back(std::move(key), std::move(value));
    }
    void set(const std::string& key, std::string value) {
        for (auto& [k, v] : items_)
            if (ieq(k, key)) {
                v = std::move(value);
                return;
            }
        add(key, std::move(value));
    }
    // Pointer to the first matching header, nullptr if absent. L1/L2 look up
    // a dozen-plus headers per request, and get()'s optional<string> copies
    // the value each time — use this for existence checks / comparisons
    const std::string* find(std::string_view key) const {
        for (auto& [k, v] : items_)
            if (ieq(k, key)) return &v;
        return nullptr;
    }
    std::optional<std::string> get(std::string_view key) const {
        if (auto* v = find(key)) return *v;
        return std::nullopt;
    }
    bool has(std::string_view key) const { return find(key) != nullptr; }

    // A header name may appear multiple times (Set-Cookie, comma-splittable
    // list headers, etc.). Taking only the first misses cases; callers
    // previously had to iterate items() themselves (parse_body_framing worked
    // around get that way)
    std::vector<const std::string*> get_all(std::string_view key) const {
        std::vector<const std::string*> out;
        for (auto& [k, v] : items_)
            if (ieq(k, key)) out.push_back(&v);
        return out;
    }
    size_t count(std::string_view key) const {
        size_t n = 0;
        for (auto& [k, v] : items_)
            if (ieq(k, key)) ++n;
        return n;
    }

    // Removes all headers with this name, returns the number removed
    size_t remove(std::string_view key) {
        size_t before = items_.size();
        std::erase_if(items_, [&](const auto& kv) { return ieq(kv.first, key); });
        return before - items_.size();
    }

    const std::vector<std::pair<std::string, std::string>>& items() const { return items_; }

    // Whether a comma-separated list header contains a token (case-insensitive,
    // surrounding whitespace ignored). Comparing list headers like Connection
    // for full equality would miss valid forms such as "close, Upgrade"
    bool has_token(std::string_view key, std::string_view token) const {
        for (auto& [k, v] : items_) {
            if (!ieq(k, key)) continue;
            size_t start = 0;
            while (start <= v.size()) {
                size_t comma = v.find(',', start);
                if (comma == std::string::npos) comma = v.size();
                std::string_view t(v.data() + start, comma - start);
                while (!t.empty() && (t.front() == ' ' || t.front() == '\t')) t.remove_prefix(1);
                while (!t.empty() && (t.back() == ' ' || t.back() == '\t')) t.remove_suffix(1);
                if (ieq(t, token)) return true;
                start = comma + 1;
            }
        }
        return false;
    }

    static bool ieq(std::string_view a, std::string_view b) {
        if (a.size() != b.size()) return false;
        for (size_t i = 0; i < a.size(); ++i)
            if (lower(a[i]) != lower(b[i])) return false;
        return true;
    }
    static char lower(char c) { return (c >= 'A' && c <= 'Z') ? char(c - 'A' + 'a') : c; }

private:
    std::vector<std::pair<std::string, std::string>> items_;
};

// Streaming request/response body: pull model. Returns bytes read; 0 means EOF.
struct BodyReader {
    virtual Task<size_t> read(std::span<std::byte> buf) = 0;
    virtual std::optional<uint64_t> length() const = 0;  // nullopt when chunked
    virtual ~BodyReader() = default;
};

// BodyReader over an in-memory string (small bodies, unit tests)
class StringBodyReader final : public BodyReader {
public:
    explicit StringBodyReader(std::string data) : data_(std::move(data)) {}
    Task<size_t> read(std::span<std::byte> buf) override {
        size_t n = std::min(buf.size(), data_.size() - pos_);
        if (n > 0) {
            std::memcpy(buf.data(), data_.data() + pos_, n);
            pos_ += n;
        }
        co_return n;
    }
    std::optional<uint64_t> length() const override { return data_.size(); }

private:
    std::string data_;
    size_t pos_ = 0;
};

struct HttpRequest {
    std::string method;    // "GET" "PUT" ...
    std::string raw_path;  // Undecoded (needed for the SigV4 canonical URI)
    std::string raw_query; // Undecoded raw query string (needed for SigV4)
    std::string path;      // Decoded
    std::vector<std::pair<std::string, std::string>> query;  // Decoded, order-preserving
    HeaderMap headers;
    std::string remote_addr;
    std::unique_ptr<BodyReader> body;  // May be nullptr (no body)
    // Cancellation signal (docs/concurrency.md §5): the driver/assembly layer
    // attaches this request's token, L2 merges it with the request-level
    // timeout into one source, and the whole coroutine chain unwinds from it.
    // Defaults to "never cancelled"
    CancelToken cancel;

    std::optional<std::string> query_get(std::string_view key) const {
        for (auto& [k, v] : query)
            if (k == key) return v;
        return std::nullopt;
    }
    bool query_has(std::string_view key) const { return query_get(key).has_value(); }
};

struct HttpResponse {
    int status = 200;
    HeaderMap headers;
    // Body is one of the two: small_body for small responses, stream_body for large ones
    std::string small_body;
    std::unique_ptr<BodyReader> stream_body;
    std::optional<uint64_t> content_length;  // Set with stream_body; otherwise the driver uses chunked
};

}  // namespace lights3::http
