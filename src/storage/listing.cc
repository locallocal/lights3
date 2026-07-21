#include "storage/listing.h"

#include <algorithm>

namespace lights3::storage {

std::pair<uint64_t, uint64_t> resolve_range(const ByteRange& r, uint64_t size) {
    auto fail = [&] {
        throw s3::S3Error(s3::S3ErrorCode::InvalidRange,
                          "The requested range is not satisfiable");
    };
    if (size == 0) fail();
    if (r.first) {
        uint64_t f = *r.first;
        if (f >= size) fail();
        uint64_t l = r.last ? std::min(*r.last, size - 1) : size - 1;
        if (l < f) fail();
        return {f, l};
    }
    if (r.last) {  // 后缀 n 字节
        uint64_t n = *r.last;
        if (n == 0) fail();
        uint64_t f = n >= size ? 0 : size - n;
        return {f, size - 1};
    }
    fail();
    return {0, 0};  // unreachable
}

ListResult apply_listing(const std::vector<std::string>& keys, const ListOptions& opt,
                         const std::function<ObjectMeta(const std::string&)>& fetch) {
    ListResult out;
    // S3：max-keys=0 返回空结果且 IsTruncated=false（否则空 token + truncated
    // 会让按 IsTruncated 循环续传的客户端原地死循环）
    if (opt.max_keys <= 0) return out;
    const auto& prefix = opt.prefix;
    const auto& delim = opt.delimiter;
    int count = 0;

    // 定位起点：>= prefix 且 > start_after
    auto it = std::lower_bound(keys.begin(), keys.end(), prefix);
    if (!opt.start_after.empty())
        it = std::upper_bound(it, keys.end(), opt.start_after);

    std::string last_emitted_key;
    for (; it != keys.end(); ++it) {
        const std::string& key = *it;
        if (key.compare(0, prefix.size(), prefix) != 0) break;  // 已排序，出前缀区间即止

        if (count >= opt.max_keys) {
            out.is_truncated = true;
            out.next_token = last_emitted_key;
            return out;
        }

        if (!delim.empty()) {
            auto pos = key.find(delim, prefix.size());
            if (pos != std::string::npos) {
                std::string group = key.substr(0, pos + delim.size());
                out.common_prefixes.push_back(group);
                ++count;
                // 跳过同组的其余 key，token 语义（"start after"）才能落在组尾
                while (std::next(it) != keys.end() &&
                       std::next(it)->compare(0, group.size(), group) == 0)
                    ++it;
                last_emitted_key = *it;
                continue;
            }
        }
        out.objects.push_back(fetch(key));
        last_emitted_key = key;
        ++count;
    }
    return out;
}

}  // namespace lights3::storage
