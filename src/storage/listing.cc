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
    if (r.last) {  // suffix of n bytes
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
    // S3: max-keys=0 returns an empty result with IsTruncated=false (otherwise an empty
    // token + truncated would send clients that loop on IsTruncated into an infinite loop)
    if (opt.max_keys <= 0) return out;
    const auto& prefix = opt.prefix;
    const auto& delim = opt.delimiter;
    int count = 0;

    // Locate the start: >= prefix and > start_after
    auto it = std::lower_bound(keys.begin(), keys.end(), prefix);
    if (!opt.start_after.empty())
        it = std::upper_bound(it, keys.end(), opt.start_after);

    std::string last_emitted_key;
    for (; it != keys.end(); ++it) {
        const std::string& key = *it;
        if (key.compare(0, prefix.size(), prefix) != 0) break;  // sorted, stop once past the prefix range

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
                // Skip the remaining keys of the same group so the token semantics
                // ("start after") land on the group's tail
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

ListPartsResult apply_parts_page(std::vector<PartMeta> sorted, const ListPartsOptions& opt) {
    ListPartsResult out;
    if (opt.max_parts <= 0) return out;
    for (auto& p : sorted) {
        if (p.part_no <= opt.part_number_marker) continue;  // the marker means "strictly greater than"
        if (out.parts.size() >= size_t(opt.max_parts)) {
            out.is_truncated = true;
            // the next page starts after the last returned part number
            out.next_part_number_marker = out.parts.back().part_no;
            return out;
        }
        out.parts.push_back(std::move(p));
    }
    return out;
}

ListUploadsResult apply_uploads_page(std::vector<UploadInfo> sorted,
                                     const ListUploadsOptions& opt) {
    ListUploadsResult out;
    if (opt.max_uploads <= 0) return out;
    for (size_t i = 0; i < sorted.size(); ++i) {
        const UploadInfo& u = sorted[i];
        if (u.key.compare(0, opt.prefix.size(), opt.prefix) != 0) continue;
        if (!upload_after_marker(u, opt)) continue;
        // delimiter grouping: the part up to the first delimiter after the prefix becomes a CommonPrefix
        std::string group;
        if (!opt.delimiter.empty()) {
            auto pos = u.key.find(opt.delimiter, opt.prefix.size());
            if (pos != std::string::npos) group = u.key.substr(0, pos + opt.delimiter.size());
        }

        if (out.uploads.size() + out.common_prefixes.size() >= size_t(opt.max_uploads)) {
            out.is_truncated = true;
            return out;  // next_* already record the position of the previous round's output item
        }

        if (!group.empty()) {
            out.common_prefixes.push_back(group);
            // Skip the remaining uploads of the same group; the cursor lands on **the last
            // entry of the group** -- if the group name itself were used as the cursor,
            // "a/" < "a/x" and the next page would list the whole group again
            // (apply_listing handles this the same way)
            while (i + 1 < sorted.size() &&
                   sorted[i + 1].key.compare(0, group.size(), group) == 0)
                ++i;
            out.next_key_marker = sorted[i].key;
            out.next_upload_id_marker = sorted[i].upload_id;
        } else {
            out.next_key_marker = u.key;
            out.next_upload_id_marker = u.upload_id;
            out.uploads.push_back(u);
        }
    }
    // If not truncated, do not return next_* (S3 only provides them when IsTruncated=true)
    out.next_key_marker.clear();
    out.next_upload_id_marker.clear();
    return out;
}

}  // namespace lights3::storage
