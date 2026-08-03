// L4: URI 编解码；aws_uri_encode 遵循 SigV4 规范（非保留字符不编码）
#pragma once

#include <string>
#include <string_view>

namespace lights3::util {

// RFC 3986 解码：只解 %XX，字面 '+' 原样保留。
// 用于 path / copy-source / SigV4 canonical query——这些上下文里 '+' 是合法字符，
// 解成空格会静默改写 key（`a+b.txt` 变 `a b.txt`）并造成 SignatureDoesNotMatch
std::string percent_decode(std::string_view s);

// query 参数值解码：在 percent_decode 之上把 '+' 解成空格（HTML form 约定）
std::string percent_decode_query(std::string_view s);

// SigV4 canonical 编码：A-Za-z0-9 - _ . ~ 保留，其余 %XX（大写）
// encode_slash=false 用于 path（'/' 不编码）
std::string aws_uri_encode(std::string_view s, bool encode_slash);

}  // namespace lights3::util
