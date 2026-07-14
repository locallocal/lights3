#include "storage/multipart.h"

#include <random>

#include "core/util/crypto.h"
#include "core/util/hex.h"

namespace lights3::storage {

using s3::S3Error;
using s3::S3ErrorCode;

std::string new_upload_id() {
    static thread_local std::mt19937_64 rng{std::random_device{}()};
    uint8_t bytes[16];
    for (size_t i = 0; i < sizeof(bytes); i += 8) {
        uint64_t v = rng();
        for (size_t j = 0; j < 8; ++j) bytes[i + j] = static_cast<uint8_t>(v >> (j * 8));
    }
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
        if (p.part_no <= prev)
            throw S3Error(S3ErrorCode::InvalidPart,
                          "The list of parts was not in ascending order.");
        prev = p.part_no;
    }
}

void validate_part_number(int part_no) {
    if (part_no < 1 || part_no > 10000)
        throw S3Error(S3ErrorCode::InvalidArgument,
                      "Part number must be an integer between 1 and 10000.");
}

}  // namespace lights3::storage
