// Fuzz target: the S3 request XML parser (s3/xml.h). Anything reachable by an
// unauthenticated client (CompleteMultipartUpload / DeleteObjects / ?website /
// ?cors / ?lifecycle bodies) goes through xml_parse; it must throw MalformedXML,
// never crash or hang
#include <cstddef>
#include <cstdint>
#include <string_view>

#include "s3/errors.h"
#include "s3/xml.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    std::string_view in(reinterpret_cast<const char*>(data), size);
    try {
        auto node = lights3::s3::xml_parse(in, 64 * 1024);
        (void)node.get("Key");
        (void)node.find("Part");
    } catch (const lights3::s3::S3Error&) {
    }
    return 0;
}
