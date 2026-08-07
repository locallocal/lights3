// L1/L2 边界：HTTP 中立模型（见 docs/http-adapter.md）
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
    // 首个匹配头的指针，不存在返回 nullptr。L1/L2 每请求要查十几次头，
    // get() 的 optional<string> 每次都拷一份值——判存在/比较时用这个
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

    // 同名头可以出现多次（Set-Cookie、逗号可拆的列表头等）。只取首个会漏判，
    // 调用方此前只能自己遍历 items()（parse_body_framing 就是这么绕开 get 的）
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

    // 删除全部同名头，返回删除条数
    size_t remove(std::string_view key) {
        size_t before = items_.size();
        std::erase_if(items_, [&](const auto& kv) { return ieq(kv.first, key); });
        return before - items_.size();
    }

    const std::vector<std::pair<std::string, std::string>>& items() const { return items_; }

    // 逗号分隔的列表头里是否含某个 token（大小写不敏感，忽略两侧空白）。
    // Connection 之类的列表头用全等比较会漏判 "close, Upgrade" 这种合法写法
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
    // 取消信号（docs/concurrency.md §5）：驱动/装配层挂上本请求的 token，L2 把它
    // 与请求级超时并到同一个源，整条协程链据此收敛。默认"永不取消"
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
    // body 二选一：小响应用 small_body；大响应用 stream_body
    std::string small_body;
    std::unique_ptr<BodyReader> stream_body;
    std::optional<uint64_t> content_length;  // stream_body 时给出，否则驱动走 chunked
};

}  // namespace lights3::http
