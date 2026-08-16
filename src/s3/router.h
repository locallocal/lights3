// L2: URL -> (bucket, key) parsing (path-style; virtual-host style in phase 2)
#pragma once

#include <string>
#include <utility>

namespace lights3::s3 {

// "/mybucket/dir/a.bin" → {"mybucket", "dir/a.bin"}；"/" → {"", ""}
std::pair<std::string, std::string> parse_bucket_key(const std::string& decoded_path);

}  // namespace lights3::s3
