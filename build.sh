#!/usr/bin/env bash
# 一键构建：初始化子模块 → cmake 配置 → 编译 →（可选）测试
set -euo pipefail
cd "$(dirname "$0")"

usage() {
    cat <<'EOF'
用法: ./build.sh [选项]
  --seastar     开启 seastar 驱动（重依赖，默认关；见 docs/http-adapter.md §3.3）。
                该开关写入 CMake 缓存后是粘性的，要关掉请配合 --clean
  --debug       Debug 构建（默认 RelWithDebInfo）
  --asan        AddressSanitizer 构建；构建目录默认改用 build-asan，
                与普通构建隔离（可再用 -B 覆盖）
  --tsan        ThreadSanitizer 构建；构建目录默认改用 build-tsan。
                与 --asan 互斥
  --clean       先删除构建目录再全量构建
  --test        构建完成后运行 ctest（unit + 各驱动 e2e）
  -j N          编译并行度（默认 nproc）
  -B DIR        构建目录（默认 build）
  -D...         其余 -D 开头的参数原样透传给 cmake
  -h, --help    本帮助
EOF
}

BUILD_DIR=""
BUILD_TYPE=""
SAN=""      # "" | address | thread
SEASTAR=0
CLEAN=0
RUN_TEST=0
JOBS=$(nproc)
CMAKE_EXTRA=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        --seastar) SEASTAR=1 ;;
        --debug)   BUILD_TYPE=Debug ;;
        --asan)    [[ $SAN == thread ]] && { echo "--asan 与 --tsan 互斥" >&2; exit 2; }
                   SAN=address ;;
        --tsan)    [[ $SAN == address ]] && { echo "--asan 与 --tsan 互斥" >&2; exit 2; }
                   SAN=thread ;;
        --clean)   CLEAN=1 ;;
        --test)    RUN_TEST=1 ;;
        -j)        JOBS="${2:?-j 需要参数}"; shift ;;
        -j*)       JOBS="${1#-j}" ;;
        -B)        BUILD_DIR="${2:?-B 需要参数}"; shift ;;
        -D*)       CMAKE_EXTRA+=("$1") ;;
        -h|--help) usage; exit 0 ;;
        *)         echo "未知参数: $1" >&2; usage >&2; exit 2 ;;
    esac
    shift
done

# 构建目录未显式指定时按构建变体选默认，sanitizer 与普通构建互不污染缓存
if [[ -z $BUILD_DIR ]]; then
    case "$SAN" in
        address) BUILD_DIR=build-asan ;;
        thread)  BUILD_DIR=build-tsan ;;
        *)       BUILD_DIR=build ;;
    esac
fi

# 子模块：常规件始终初始化（rocksdb 为 shallow 克隆，压缩全关后零系统级依赖，
# 不做惰性拉取，docs/duostore-backend.md §13.2）；seastar 克隆很大，仅在需要时拉取
# （其自带的 dpdk 子模块构建时不用，不做递归初始化）
LIGHT_MODULES=(third_party/gflags third_party/spdlog third_party/httplib third_party/json
               third_party/rocksdb third_party/hiredis)
git submodule update --init "${LIGHT_MODULES[@]}"
if [[ $SEASTAR -eq 1 ]]; then
    git submodule update --init third_party/seastar
fi

[[ $CLEAN -eq 1 ]] && rm -rf "$BUILD_DIR"

CMAKE_ARGS=()
command -v ninja >/dev/null && CMAKE_ARGS+=(-G Ninja)
[[ -n $BUILD_TYPE ]] && CMAKE_ARGS+=(-DCMAKE_BUILD_TYPE="$BUILD_TYPE")
# 不传 OFF：避免覆盖既有缓存里的 ON（粘性语义，见 usage）
[[ $SEASTAR -eq 1 ]] && CMAKE_ARGS+=(-DLIGHTS3_DRIVER_SEASTAR=ON)
if [[ -n $SAN ]]; then
    CMAKE_ARGS+=(-DCMAKE_CXX_FLAGS="-fsanitize=$SAN -fno-omit-frame-pointer"
                 -DCMAKE_EXE_LINKER_FLAGS="-fsanitize=$SAN")
fi

cmake -B "$BUILD_DIR" "${CMAKE_ARGS[@]}" ${CMAKE_EXTRA[@]+"${CMAKE_EXTRA[@]}"}
cmake --build "$BUILD_DIR" -j "$JOBS"

if [[ $RUN_TEST -eq 1 ]]; then
    ctest --test-dir "$BUILD_DIR" --output-on-failure
fi

echo
echo "构建完成: $BUILD_DIR/lights3"
