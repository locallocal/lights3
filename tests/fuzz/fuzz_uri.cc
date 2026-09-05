// Fuzz target: percent decoding / AWS URI encoding (core/util/uri.h), the first
// thing every request path and query goes through. Decoding must never throw on
// arbitrary bytes, and encode(decode(x)) must be re-decodable to the same bytes
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>

#include "core/util/uri.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    using namespace lights3::util;
    std::string_view in(reinterpret_cast<const char*>(data), size);
    std::string d = percent_decode(in);
    std::string q = percent_decode_query(in);
    for (bool slash : {true, false}) {
        std::string enc = aws_uri_encode(d, slash);
        if (percent_decode(enc) != d) std::abort();  // round trip is a hard invariant
    }
    (void)q;
    return 0;
}
