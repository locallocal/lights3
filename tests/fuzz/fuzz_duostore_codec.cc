// Fuzz target: duostore's binary value codec (storage/duostore/codec.h). Meta
// records come back from RocksDB / Redis / SQLite / TiKV — a shared meta store
// means another gateway (or a corrupt engine) wrote them, so every decode must
// reject garbage with an exception rather than read out of bounds. First byte
// selects the record kind
#include <cstddef>
#include <cstdint>
#include <exception>
#include <string>
#include <string_view>

#include "storage/duostore/codec.h"

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    using namespace lights3::storage::duostore::codec;
    if (size == 0) return 0;
    std::string_view v(reinterpret_cast<const char*>(data + 1), size - 1);
    try {
        switch (data[0] % 7) {
            case 0: (void)decode_object("bkt\0key", v); break;
            case 1: (void)decode_object_meta("bkt\0key", v); break;
            case 2: (void)decode_upload("bkt\0key", "upl", v); break;
            case 3: (void)decode_part(1, v); break;
            case 4: (void)decode_reclaim(v); break;
            case 5: (void)decode_extents(v); break;
            default:
                (void)decode_counter(v);
                (void)decode_bucket(v);
                break;
        }
    } catch (const std::exception&) {
    }
    return 0;
}
