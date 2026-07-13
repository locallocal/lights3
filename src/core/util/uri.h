// L4: URI 编解码；aws_uri_encode 遵循 SigV4 规范（非保留字符不编码）
#pragma once

#include <string>
#include <string_view>

namespace lights3::util {

std::string percent_decode(std::string_view s);

// SigV4 canonical 编码：A-Za-z0-9 - _ . ~ 保留，其余 %XX（大写）
// encode_slash=false 用于 path（'/' 不编码）
std::string aws_uri_encode(std::string_view s, bool encode_slash);

}  // namespace lights3::util
