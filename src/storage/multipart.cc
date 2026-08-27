#include "storage/multipart.h"

#include <unistd.h>

#include <charconv>
#include <span>

#include "core/util/checksum.h"
#include "core/util/crypto.h"
#include "core/util/hex.h"

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;

std::string new_upload_id() {
    // upload_id is returned directly to the client and is the sole credential for
    // aborting/completing someone else's upload: mt19937_64's internal state can be
    // recovered from ~2496 outputs, and a random_device seed carries only 32 bits of
    // entropy -- predictable means enumerable/forgeable (docs/archive/gaps.md §3.9). Must use a CSPRNG
    uint8_t bytes[16];
    if (::getentropy(bytes, sizeof(bytes)) != 0)
        throw S3Error(S3ErrorCode::InternalError, "cannot generate upload id");
    return util::to_hex(bytes);
}

bool is_valid_upload_id(std::string_view id) {
    if (id.size() != 32) return false;
    for (char c : id)
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) return false;
    return true;
}

std::string combined_etag(const std::vector<std::string>& part_md5_hex) {
    util::HashStream md5(util::HashStream::Algo::Md5);
    for (auto& hex : part_md5_hex) {
        auto digest = util::from_hex(hex);
        md5.update(digest);
    }
    return md5.final_hex() + "-" + std::to_string(part_md5_hex.size());
}

std::string_view strip_etag_quotes(std::string_view etag) {
    if (etag.size() >= 2 && etag.front() == '"' && etag.back() == '"')
        return etag.substr(1, etag.size() - 2);
    return etag;
}

void validate_part_order(std::span<const PartInfo> parts) {
    if (parts.empty())
        throw S3Error(S3ErrorCode::InvalidPart, "You must specify at least one part.");
    int prev = 0;
    for (auto& p : parts) {
        // Out-of-order has its own error code (docs/archive/gaps.md §5.7): InvalidPart means "this
        // part is bad", which makes clients re-upload the part; what is actually needed is
        // to sort the list and resubmit
        if (p.part_no <= prev)
            throw S3Error(S3ErrorCode::InvalidPartOrder,
                          "The list of parts was not in ascending order. Parts must be "
                          "ordered by part number.");
        prev = p.part_no;
    }
}

void validate_part_number(int part_no) {
    if (part_no < 1 || part_no > 10000)
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Part number must be an integer between 1 and 10000.");
}

// ---- Checksum closure (roadmap §2.2) ----

std::optional<std::string> composite_checksum(std::string_view algorithm,
                                              const std::vector<std::string>& part_values_b64) {
    if (part_values_b64.empty()) return std::nullopt;
    // Raw digests concatenated in part order
    std::string concat;
    size_t digest_len = 0;
    for (auto& b64 : part_values_b64) {
        auto raw = util::base64_decode(b64);
        if (!raw) return std::nullopt;
        if (digest_len == 0) digest_len = raw->size();
        if (raw->size() != digest_len || digest_len == 0) return std::nullopt;
        concat += *raw;
    }
    auto span_of = [&] {
        return std::span(reinterpret_cast<const std::byte*>(concat.data()), concat.size());
    };
    std::string out;
    if (algorithm == "SHA1" || algorithm == "SHA256") {
        util::HashStream h(algorithm == "SHA1" ? util::HashStream::Algo::Sha1
                                               : util::HashStream::Algo::Sha256);
        h.update(std::span(reinterpret_cast<const uint8_t*>(concat.data()), concat.size()));
        auto d = h.final_bytes();
        out.assign(d.begin(), d.end());
    } else if (algorithm == "CRC32" || algorithm == "CRC32C") {
        uint32_t crc = algorithm == "CRC32" ? util::crc32_update(0, span_of())
                                            : util::crc32c_update(0, span_of());
        for (int s = 24; s >= 0; s -= 8) out.push_back(char((crc >> s) & 0xff));
    } else {
        return std::nullopt;  // CRC64NVME composites are not a thing (full-object only)
    }
    return util::base64_encode(
               std::span(reinterpret_cast<const uint8_t*>(out.data()), out.size())) +
           "-" + std::to_string(part_values_b64.size());
}

void apply_composite_checksum(const std::vector<PartDigest>& parts, ObjectMeta& meta,
                              PutResult& result) {
    if (parts.empty()) return;
    const std::string& algo = parts.front().algorithm;
    for (auto& p : parts)
        if (p.algorithm.empty() || p.value.empty() || p.algorithm != algo) return;
    std::vector<std::string> values;
    values.reserve(parts.size());
    for (auto& p : parts) values.push_back(p.value);
    auto composite = composite_checksum(algo, values);
    if (!composite) return;
    meta.checksum_algorithm = algo;
    meta.checksum_value = *composite;
    meta.checksum_type = "COMPOSITE";
    meta.checksum_pending.reset();
    result.checksum_algorithm = algo;
    result.checksum_value = *composite;
    result.checksum_type = "COMPOSITE";
}

// ---- part_sizes wire form (declared in backend.h) ----

std::string join_part_sizes(const std::vector<uint64_t>& sizes) {
    std::string out;
    for (auto s : sizes) {
        if (!out.empty()) out += ',';
        out += std::to_string(s);
    }
    return out;
}

std::vector<uint64_t> parse_part_sizes(std::string_view s) {
    std::vector<uint64_t> out;
    size_t pos = 0;
    while (pos < s.size()) {
        size_t comma = s.find(',', pos);
        if (comma == std::string_view::npos) comma = s.size();
        uint64_t v = 0;
        auto sub = s.substr(pos, comma - pos);
        auto [p, ec] = std::from_chars(sub.data(), sub.data() + sub.size(), v);
        if (ec != std::errc() || p != sub.data() + sub.size()) return {};  // malformed → unknown
        out.push_back(v);
        pos = comma + 1;
    }
    return out;
}

}  // namespace lights3::storage
