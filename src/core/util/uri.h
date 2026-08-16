// L4: URI encoding/decoding; aws_uri_encode follows the SigV4 spec (unreserved characters are not encoded)
#pragma once

#include <string>
#include <string_view>

namespace lights3::util {

// RFC 3986 decoding: only decodes %XX; a literal '+' is kept as is.
// Used for path / copy-source / SigV4 canonical query — in those contexts '+' is a
// legal character, and decoding it to a space would silently rewrite the key
// (`a+b.txt` becomes `a b.txt`) and cause SignatureDoesNotMatch
std::string percent_decode(std::string_view s);

// Query parameter value decoding: on top of percent_decode, decodes '+' to a space (HTML form convention)
std::string percent_decode_query(std::string_view s);

// SigV4 canonical encoding: A-Za-z0-9 - _ . ~ are kept, everything else becomes
// %XX (uppercase). encode_slash=false is for paths ('/' is not encoded)
std::string aws_uri_encode(std::string_view s, bool encode_slash);

}  // namespace lights3::util
