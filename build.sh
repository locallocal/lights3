#!/usr/bin/env bash
# One-shot build: init submodules -> cmake configure -> compile -> (optional) tests
set -euo pipefail
cd "$(dirname "$0")"

usage() {
    cat <<'EOF'
Usage: ./build.sh [options]
  --seastar     Enable the seastar driver (heavy dependency, off by default;
                see docs/http-adapter.md §3.3). The switch is sticky once
                written into the CMake cache; use --clean to turn it off
  --tikv        Enable the TiKV meta backend for duostore (client-c submodule
                plus system-level gRPC/Poco dependencies, off by default; see
                docs/duostore-tikv-meta.md §8). Same sticky semantics as
                --seastar; recommend -B build-tikv to isolate from regular builds
  --redis       Enable the Redis meta backend for duostore (hiredis submodule,
                off by default; see docs/duostore-redis-meta.md). Same sticky
                semantics as --seastar
  --sqlite      Enable the SQLite meta backend for duostore (sqlite submodule,
                off by default; see docs/duostore-sqlite-meta.md). Same sticky
                semantics as --seastar
  --rados       Enable the RADOS data backend for duostore (off by default; see
                docs/duostore-rados-data.md §9). Requires system librados
                (apt install librados-dev, or unpack to a custom path and point
                -DLIGHTS3_RADOS_ROOT=... at it). Same sticky semantics as
                --seastar; recommend -B build-rados to isolate from regular builds
  --debug       Debug build (default RelWithDebInfo)
  --asan        AddressSanitizer build; the build directory defaults to
                build-asan, isolated from regular builds (override with -B)
  --tsan        ThreadSanitizer build; the build directory defaults to
                build-tsan. Mutually exclusive with --asan
  --ubsan       UndefinedBehaviorSanitizer build; defaults to build-ubsan.
                Mutually exclusive with --asan/--tsan (docs/testing.md §7)
  --coverage    gcov instrumentation (-O0 --coverage); defaults to build-cov;
                scripts/coverage.sh builds, runs and reports (docs/testing.md §7)
  --fuzz        libFuzzer harnesses (requires clang; CC/CXX are switched to
                clang unless already set); defaults to build-fuzz
                (docs/testing.md §3)
  --clean       Remove the build directory first, then do a full build
  --test        Run ctest after the build (unit + per-driver e2e)
  -j N          Build parallelism (default nproc)
  -B DIR        Build directory (default build)
  -D...         Any remaining -D arguments are passed through to cmake as-is
  -h, --help    Show this help
EOF
}

BUILD_DIR=""
BUILD_TYPE=""
SAN=""      # "" | address | thread | undefined
COVERAGE=0
FUZZ=0
SEASTAR=0
TIKV=0
REDIS=0
SQLITE=0
RADOS=0
CLEAN=0
RUN_TEST=0
JOBS=$(nproc)
CMAKE_EXTRA=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --seastar) SEASTAR=1 ;;
        --tikv)    TIKV=1 ;;
        --redis)   REDIS=1 ;;
        --sqlite)  SQLITE=1 ;;
        --rados)   RADOS=1 ;;
        --debug)   BUILD_TYPE=Debug ;;
        --asan)    [[ $SAN == thread ]] && { echo "--asan and --tsan are mutually exclusive" >&2; exit 2; }
                   SAN=address ;;
        --tsan)    [[ $SAN == address ]] && { echo "--asan and --tsan are mutually exclusive" >&2; exit 2; }
                   SAN=thread ;;
        --ubsan)   [[ -n $SAN && $SAN != undefined ]] && { echo "sanitizer flags are mutually exclusive" >&2; exit 2; }
                   SAN=undefined ;;
        --coverage) COVERAGE=1 ;;
        --fuzz)    FUZZ=1 ;;
        --clean)   CLEAN=1 ;;
        --test)    RUN_TEST=1 ;;
        -j)        JOBS="${2:?-j requires an argument}"; shift ;;
        -j*)       JOBS="${1#-j}" ;;
        -B)        BUILD_DIR="${2:?-B requires an argument}"; shift ;;
        -D*)       CMAKE_EXTRA+=("$1") ;;
        -h|--help) usage; exit 0 ;;
        *)         echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

# When no build dir is given explicitly, pick a default per build variant so
# sanitizer and regular builds do not pollute each other's caches
if [[ -z $BUILD_DIR ]]; then
    case "$SAN" in
        address)   BUILD_DIR=build-asan ;;
        thread)    BUILD_DIR=build-tsan ;;
        undefined) BUILD_DIR=build-ubsan ;;
        *)         BUILD_DIR=build ;;
    esac
    [[ $COVERAGE -eq 1 ]] && BUILD_DIR=build-cov
    [[ $FUZZ -eq 1 ]] && BUILD_DIR=build-fuzz
fi

# Submodules: always init the regular ones (rocksdb is a shallow clone; with all
# compression disabled it has zero system-level deps, so no lazy fetch,
# docs/duostore-backend.md §13.2); the seastar clone is huge, fetch only when needed
# (its bundled dpdk submodule is unused at build time, so no recursive init)
LIGHT_MODULES=(third_party/spdlog third_party/httplib third_party/json
               third_party/rocksdb third_party/hiredis third_party/sqlite)
git submodule update --init "${LIGHT_MODULES[@]}"
# ccmd (command-line framework for lights3 + s3adm) nests its cflag dependency
git submodule update --init --recursive third_party/ccmd
if [[ $SEASTAR -eq 1 ]]; then
    git submodule update --init third_party/seastar
fi
# client-c needs a system-level gRPC/Poco toolchain, so fetch it lazily
# (docs/duostore-tikv-meta.md §8.1); of its nested submodules only kvproto/libfiu
# are taken (abseil uniformly uses the system copy, googletest is not built)
if [[ $TIKV -eq 1 ]]; then
    git submodule update --init third_party/client-c
    git -C third_party/client-c submodule update --init \
        third_party/kvproto third_party/libfiu
fi

[[ $CLEAN -eq 1 ]] && rm -rf "$BUILD_DIR"

CMAKE_ARGS=()
command -v ninja >/dev/null && CMAKE_ARGS+=(-G Ninja)
[[ -n $BUILD_TYPE ]] && CMAKE_ARGS+=(-DCMAKE_BUILD_TYPE="$BUILD_TYPE")
# Never pass OFF: avoid overriding an ON already in the cache (sticky semantics, see usage)
[[ $SEASTAR -eq 1 ]] && CMAKE_ARGS+=(-DLIGHTS3_DRIVER_SEASTAR=ON)
[[ $TIKV -eq 1 ]] && CMAKE_ARGS+=(-DLIGHTS3_DUOSTORE_TIKV_META=ON)
[[ $REDIS -eq 1 ]] && CMAKE_ARGS+=(-DLIGHTS3_DUOSTORE_REDIS_META=ON)
[[ $SQLITE -eq 1 ]] && CMAKE_ARGS+=(-DLIGHTS3_DUOSTORE_SQLITE_META=ON)
# librados has no submodule and comes from the system package; when it is not found,
# CMake raises FATAL_ERROR suggesting to install librados-dev or set
# LIGHTS3_RADOS_ROOT (CMakeLists.txt)
[[ $RADOS -eq 1 ]] && CMAKE_ARGS+=(-DLIGHTS3_DUOSTORE_RADOS_DATA=ON)
# Coverage: -O0 keeps line attribution honest; gcov data lands next to the objects
if [[ $COVERAGE -eq 1 ]]; then
    CMAKE_ARGS+=(-DCMAKE_BUILD_TYPE=Debug
                 -DCMAKE_CXX_FLAGS="-O0 --coverage -fno-inline"
                 -DCMAKE_EXE_LINKER_FLAGS="--coverage")
fi
# libFuzzer needs clang: switch the compiler for this build dir unless the caller pinned one
if [[ $FUZZ -eq 1 ]]; then
    if [[ -z ${CXX:-} ]]; then
        command -v clang++ >/dev/null || { echo "--fuzz needs clang++ (or set CXX)" >&2; exit 2; }
        export CC=clang CXX=clang++
    fi
    CMAKE_ARGS+=(-DLIGHTS3_FUZZ_LIBFUZZER=ON)
    [[ -z $SAN ]] && SAN=address  # fuzzers run under ASan by default
fi
if [[ -n $SAN ]]; then
    CMAKE_ARGS+=(-DCMAKE_CXX_FLAGS="-fsanitize=$SAN -fno-omit-frame-pointer"
                 -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=$SAN")
fi

# Empty-array guard for both arrays: bash < 4.4 reports unbound when expanding an empty array under set -u
cmake -B "$BUILD_DIR" ${CMAKE_ARGS[@]+"${CMAKE_ARGS[@]}"} ${CMAKE_EXTRA[@]+"${CMAKE_EXTRA[@]}"}
cmake --build "$BUILD_DIR" -j "$JOBS"

if [[ $RUN_TEST -eq 1 ]]; then
    ctest --test-dir "$BUILD_DIR" --output-on-failure
fi

echo
echo "build complete: $BUILD_DIR/lights3"
