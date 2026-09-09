// L1/L2 boundary: HTTP-neutral model (see docs/http-adapter.md)
// This header depends only on the standard library and core/task.h; no HTTP
// library types may appear here.
#pragma once

#include <array>
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

// Case-insensitive, order-preserving header table. Storage stays a vector
// (a request carries a few dozen headers, insertion order matters to the
// drivers); every lookup is a linear scan filtered by an 8-bit tag of the
// lowercased name kept next to each item (backlog-sequence ⑩) -- the
// case-folding compare runs only on the rare tag collision. A 256-bit set of
// the tags present answers the miss (an optional header L2 probes for and the
// request does not carry: the common case) without scanning at all
class HeaderMap {
public:
    void add(std::string key, std::string value) {
        const uint8_t t = tag(key);
        tags_.push_back(t);
        present_[t >> 6] |= uint64_t(1) << (t & 63);
        items_.emplace_back(std::move(key), std::move(value));
    }
    void set(const std::string& key, std::string value) {
        if (auto* v = find_mut(key)) {
            *v = std::move(value);
            return;
        }
        add(key, std::move(value));
    }
    // Pointer to the first matching header, nullptr if absent. L1/L2 look up
    // a dozen-plus headers per request, and get()'s optional<string> copies
    // the value each time — use this for existence checks / comparisons
    const std::string* find(std::string_view key) const {
        const uint8_t t = tag(key);
        if (!maybe(t)) return nullptr;
        for (size_t i = 0; i < items_.size(); ++i)
            if (tags_[i] == t && ieq(items_[i].first, key)) return &items_[i].second;
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
        const uint8_t t = tag(key);
        if (!maybe(t)) return out;
        for (size_t i = 0; i < items_.size(); ++i)
            if (tags_[i] == t && ieq(items_[i].first, key)) out.push_back(&items_[i].second);
        return out;
    }
    size_t count(std::string_view key) const {
        size_t n = 0;
        const uint8_t t = tag(key);
        if (!maybe(t)) return 0;
        for (size_t i = 0; i < items_.size(); ++i)
            if (tags_[i] == t && ieq(items_[i].first, key)) ++n;
        return n;
    }

    // Removes all headers with this name, returns the number removed
    size_t remove(std::string_view key) {
        const uint8_t t = tag(key);
        if (!maybe(t)) return 0;
        size_t w = 0;
        for (size_t r = 0; r < items_.size(); ++r) {
            if (tags_[r] == t && ieq(items_[r].first, key)) continue;
            if (w != r) {
                items_[w] = std::move(items_[r]);
                tags_[w] = tags_[r];
            }
            ++w;
        }
        size_t removed = items_.size() - w;
        items_.resize(w);
        tags_.resize(w);
        if (removed) {  // another header may share the tag: rebuild the set from what is left
            present_ = {};
            for (uint8_t r : tags_) present_[r >> 6] |= uint64_t(1) << (r & 63);
        }
        return removed;
    }

    const std::vector<std::pair<std::string, std::string>>& items() const { return items_; }

    // The prefilter tag: length and the lowercased first / last characters
    // folded into 8 bits. O(1) on purpose -- hashing the whole name costs as
    // much as the scan it saves (measured: a full FNV-1a tag made hits slower
    // than the plain scan). Exposed for tests
    static uint8_t tag(std::string_view key) {
        if (key.empty()) return 0;
        const uint32_t f = static_cast<uint8_t>(lower(key.front()));
        const uint32_t l = static_cast<uint8_t>(lower(key.back()));
        return static_cast<uint8_t>((key.size() * 0x9Du) ^ (f * 0x35u) ^ (l << 3));
    }

    // Whether a comma-separated list header contains a token (case-insensitive,
    // surrounding whitespace ignored). Comparing list headers like Connection
    // for full equality would miss valid forms such as "close, Upgrade"
    bool has_token(std::string_view key, std::string_view token) const {
        const uint8_t t = tag(key);
        if (!maybe(t)) return false;
        for (size_t i = 0; i < items_.size(); ++i) {
            if (tags_[i] != t || !ieq(items_[i].first, key)) continue;
            const std::string& v = items_[i].second;
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
    bool maybe(uint8_t t) const { return (present_[t >> 6] >> (t & 63)) & 1; }
    std::string* find_mut(std::string_view key) {
        const uint8_t t = tag(key);
        if (!maybe(t)) return nullptr;
        for (size_t i = 0; i < items_.size(); ++i)
            if (tags_[i] == t && ieq(items_[i].first, key)) return &items_[i].second;
        return nullptr;
    }

    std::vector<std::pair<std::string, std::string>> items_;
    std::vector<uint8_t> tags_;          // tag(items_[i].first), kept in lockstep with items_
    std::array<uint64_t, 4> present_{};  // set of tags in tags_ (a superset after remove is fine, but it is rebuilt
                                         // exactly)
};

// Zero-copy exit for file-backed bodies (roadmap §4.3 ④, docs/http-adapter.md §1):
// the reader's *remaining* bytes are exactly this contiguous range of fd
struct FileSpan {
    int fd;
    uint64_t offset;
    uint64_t length;
};

// Streaming request/response body: pull model. Returns bytes read; 0 means EOF.
struct BodyReader {
    virtual Task<size_t> read(std::span<std::byte> buf) = 0;
    virtual std::optional<uint64_t> length() const = 0;  // nullopt when chunked
    // Optional sendfile(2) fast path. A reader whose remaining bytes are one
    // contiguous file range may expose it; a driver that takes the offer moves
    // the bytes kernel-side and reports them through file_bytes_sent() so the
    // reader's position and any accounting decorator stay consistent (a driver
    // may also stop taking the offer midway and go back to read()). Default:
    // no fast path. Decorators that only observe bytes forward both; decorators
    // that transform or inspect bytes (checksums, tees) keep the default
    virtual std::optional<FileSpan> try_as_file() { return std::nullopt; }
    virtual void file_bytes_sent(uint64_t /*n*/) {}
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

// The identity-bearing fields of a verified client certificate (mTLS)
struct TlsIdentity {
    std::string subject_cn;  // subject commonName (empty when the subject has none)
    std::string san_uri;     // first URI subjectAltName (empty when there is none)
};

struct HttpRequest {
    std::string method;                                      // "GET" "PUT" ...
    std::string raw_path;                                    // Undecoded (needed for the SigV4 canonical URI)
    std::string raw_query;                                   // Undecoded raw query string (needed for SigV4)
    std::string path;                                        // Decoded
    std::vector<std::pair<std::string, std::string>> query;  // Decoded, order-preserving
    HeaderMap headers;
    std::string remote_addr;
    std::unique_ptr<BodyReader> body;  // May be nullptr (no body)
    // Cancellation signal (docs/concurrency.md §5): the driver/assembly layer
    // attaches this request's token, L2 merges it with the request-level
    // timeout into one source, and the whole coroutine chain unwinds from it.
    // Defaults to "never cancelled"
    CancelToken cancel;
    // Which listener accepted the request when a separate admin port is configured
    // (http.admin_port, backlog-sequence ②): the application's admin-listener
    // handler sets it, the service gates the /-/ face on it. Meaningless (false)
    // without the split
    bool admin_face = false;
    // Verified client certificate of the connection (mTLS, backlog-sequence ⑥,
    // docs/tls.md §2.1): set by the driver after a handshake in which the peer
    // presented a certificate that verified against http.tls_client_ca; absent on
    // plaintext, without a client certificate, or when client auth is off. L1
    // reports both candidate subjects, L2 picks one by auth.tls_identity
    std::optional<TlsIdentity> tls_identity;

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
