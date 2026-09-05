// L4: fault injection points (roadmap §6.1). Named points sit on the IO paths a
// deployment can actually lose — disk write / rename / fsync (EIO, ENOSPC), the
// pack append and its fdatasync, the Redis connection, the RADOS submit — and are
// armed from the environment at startup or programmatically by tests:
//
//   LIGHTS3_FAULTS="localfs.write:1:EIO,duostore.pack.fdatasync:0:ENOSPC"
//
// spec = point[:count][:errno], comma separated. count = how many hits fire
// (default 1; 0 = every hit until reset), errno = symbolic (EIO, ENOSPC,
// ETIMEDOUT, ECONNRESET, ...) or numeric (default EIO). The check on the hot
// path is one relaxed atomic load when nothing is armed; an armed point costs a
// mutex-guarded lookup. Off by default, no build flag: the same binary that runs
// in production is the one the fault tests exercise (docs/testing.md §4)
#pragma once

#include <atomic>
#include <string>
#include <string_view>

namespace lights3::fault {

namespace detail {
extern std::atomic<int> g_armed;  // number of armed points
int check_slow(std::string_view point);
}  // namespace detail

// errno to inject at this point, 0 = proceed normally. One-shot counts decrement
inline int check(std::string_view point) {
    if (detail::g_armed.load(std::memory_order_relaxed) == 0) return 0;
    return detail::check_slow(point);
}

// Arms the points in spec (replacing same-named ones); throws std::runtime_error
// on a malformed spec or an unknown errno name
void arm(std::string_view spec);
// LIGHTS3_FAULTS from the environment; silently a no-op when unset
void arm_from_env();
void reset();
// "point:remaining:errno, ..." for logs/tests ("" when nothing is armed)
std::string describe();

// The points wired into the tree (kept in one list so docs/tests and the code
// cannot drift apart; tests assert every entry here appears in the sources)
constexpr std::string_view kPoints[] = {
    "localfs.write",           // ::write into the staging tmp (put / upload_part / copy)
    "localfs.rename",          // the commit rename of an object / cached data
    "localfs.fsync",           // fdatasync of a staged file
    "xlocalfs.write",          // the io_uring staging write pipeline (xlocalfs backend)
    "duostore.pack.pwrite",    // pack record append
    "duostore.pack.fdatasync", // pack durability sync
    "redis.command",           // hiredis command: simulated connection failure
    "rados.submit",            // rados_aio_* submission: returns -errno
};

}  // namespace lights3::fault
