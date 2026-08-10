#include "storage/bucket_router.h"

#include <fnmatch.h>

#include <stdexcept>

namespace lights3::storage {

namespace {

// glob 语法校验（docs/gaps.md §6.3）：fnmatch 对坏 pattern 不报错只报"不匹配"，
// 写错的规则会静默永不命中——桶被悄悄路由去默认后端，数据落错引擎后再迁移就是
// 一次全量搬运。能在构建期确定的错误一律构建期拒绝：
// ① 未闭合的 '[' 字符类；② pattern 含桶名字符集（小写/数字/'-'/'.'）之外的
// 字面字符（大写、'_'、'/' 等）——合法桶名永远不可能命中它
void validate_glob(const std::string& pattern) {
    auto fail = [&](const std::string& why) {
        throw std::runtime_error("bucket rule glob '" + pattern + "': " + why);
    };
    if (pattern.empty()) fail("empty pattern");
    bool in_class = false;
    for (size_t i = 0; i < pattern.size(); ++i) {
        char c = pattern[i];
        if (in_class) {
            if (c == ']') in_class = false;
            continue;
        }
        if (c == '[') {
            in_class = true;
            // '[]a]' 形式：紧跟的 ']' 是字面成员，不闭类
            if (i + 1 < pattern.size() && pattern[i + 1] == ']') ++i;
            continue;
        }
        if (c == '*' || c == '?' || c == '\\') continue;
        if (!((c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-' || c == '.'))
            fail(std::string("literal character '") + c +
                 "' can never appear in a valid bucket name (rule would silently never match)");
    }
    if (in_class) fail("unclosed '[' character class");
}

bool glob_match(const std::string& glob, std::string_view bucket) {
    // 桶名 ≤ 63 字节（validate_bucket_name），栈缓冲免去每请求堆分配
    char buf[64];
    size_t n = std::min(bucket.size(), sizeof(buf) - 1);
    bucket.copy(buf, n);
    buf[n] = '\0';
    return ::fnmatch(glob.c_str(), buf, 0) == 0;
}

}  // namespace

BucketRouter BucketRouter::build(
    const BucketsConfig& cfg, std::map<std::string, std::shared_ptr<IStorageBackend>> backends) {
    BucketRouter r;
    r.backends_ = std::move(backends);
    auto find = [&](const std::string& name) {
        auto it = r.backends_.find(name);
        if (it == r.backends_.end())
            throw std::runtime_error("bucket rule references unknown backend: " + name);
        return it->second;
    };
    bool saw_catch_all = false;
    for (auto& rule : cfg.rules) {
        // 否定规则（docs/gaps.md §6.3）："!pattern" = 不匹配 pattern 的桶命中本规则
        bool negate = !rule.match.empty() && rule.match.front() == '!';
        std::string glob = negate ? rule.match.substr(1) : rule.match;
        validate_glob(glob);
        // 不可达检测：声明序匹配下，排在 catch-all 之后的规则永远轮不到——
        // 这是配置错误（多半是把默认规则写在了前面），静默忽略只会让人困惑
        if (saw_catch_all)
            throw std::runtime_error("bucket rule '" + rule.match +
                                     "' is unreachable: it follows a catch-all rule");
        for (auto& prev : r.rules_)
            if (prev.glob == glob && prev.negate == negate)
                throw std::runtime_error("bucket rule '" + rule.match +
                                         "' is unreachable: duplicate of an earlier rule");
        if (!negate && (glob == "*" || glob == "**")) saw_catch_all = true;
        if (negate && glob.find_first_of("*?[") == std::string::npos &&
            glob.find('\\') == std::string::npos) {
            // "!固定串" 对除一个名字外的所有桶都命中——它自己也是 catch-all
            saw_catch_all = true;
        }
        r.rules_.push_back({std::move(glob), negate, find(rule.backend)});
    }
    r.default_ = find(cfg.default_backend);
    return r;
}

IStorageBackend& BucketRouter::resolve(std::string_view bucket) const {
    for (auto& rule : rules_)
        if (glob_match(rule.glob, bucket) != rule.negate) return *rule.backend;
    return *default_;
}

}  // namespace lights3::storage
