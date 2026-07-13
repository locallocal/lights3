// L1/L2 边界：HTTP 中立模型（见 docs/02-http-adapter.md）
// 本头文件只依赖标准库与 core/task.h，任何 HTTP 库的类型都不得出现在这里。
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

// 大小写不敏感、保序的头部表
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
    std::optional<std::string> get(std::string_view key) const {
        for (auto& [k, v] : items_)
            if (ieq(k, key)) return v;
        return std::nullopt;
    }
    bool has(std::string_view key) const { return get(key).has_value(); }
    const std::vector<std::pair<std::string, std::string>>& items() const { return items_; }

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

// 流式请求/响应体：拉模型。返回读到的字节数；0 表示 EOF。
struct BodyReader {
    virtual Task<size_t> read(std::span<std::byte> buf) = 0;
    virtual std::optional<uint64_t> length() const = 0;  // chunked 时 nullopt
    virtual ~BodyReader() = default;
};

// 内存串的 BodyReader（小 body、单测用）
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
    std::string raw_path;  // 未解码（SigV4 canonical URI 需要）
    std::string raw_query; // 未解码原始 query 串（SigV4 需要）
    std::string path;      // 已解码
    std::vector<std::pair<std::string, std::string>> query;  // 已解码、保序
    HeaderMap headers;
    std::string remote_addr;
    std::unique_ptr<BodyReader> body;  // 可能为 nullptr（无 body）

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
    // body 二选一：小响应用 small_body；大响应用 stream_body
    std::string small_body;
    std::unique_ptr<BodyReader> stream_body;
    std::optional<uint64_t> content_length;  // stream_body 时给出，否则驱动走 chunked
};

}  // namespace lights3::http
