// Fuzz target: the L1 request parsing shared by the builtin / seastar drivers
// and the framing validator shared by all four (http/drivers/common.h):
// request target, Content-Length, chunk-size line, CL/TE conflict rules. Input
// layout: first byte selects the routine, the rest is the line / header block
#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

#include "http/drivers/common.h"
#include "http/model.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    using namespace lights3::http;
    if (size == 0) return 0;
    std::string_view rest(reinterpret_cast<const char*>(data + 1), size - 1);
    switch (data[0] % 4) {
        case 0: {
            HttpRequest req;
            driver::parse_target(rest, req);
            break;
        }
        case 1: {
            uint64_t v = 0;
            (void)driver::parse_content_length(rest, v);
            break;
        }
        case 2: {
            uint64_t v = 0;
            (void)driver::parse_chunk_size(rest, v);
            break;
        }
        default: {
            // "name: value\n" lines -> HeaderMap -> framing verdict
            HeaderMap headers;
            size_t pos = 0;
            while (pos < rest.size()) {
                size_t nl = rest.find('\n', pos);
                std::string_view line = rest.substr(pos, nl == std::string_view::npos ? std::string_view::npos : nl - pos);
                pos = nl == std::string_view::npos ? rest.size() : nl + 1;
                size_t colon = line.find(':');
                if (colon == std::string_view::npos || colon == 0) continue;
                headers.add(std::string(line.substr(0, colon)), std::string(line.substr(colon + 1)));
            }
            auto f = driver::parse_body_framing(headers);
            (void)f.valid;
            break;
        }
    }
    return 0;
}
