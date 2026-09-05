#!/usr/bin/env bash
# One-shot verification matrix (roadmap §6.1, docs/testing.md §8): incremental
# build + ctest for every build directory that exists (or that --configure
# creates through build.sh), with a summary table at the end. Variants are
# selected by directory name; sanitizer variants run under the matching
# *SAN_OPTIONS so a finding fails the run instead of scrolling by.
#
# Usage: check-all.sh [--only build,build-asan,...] [--configure] [--with-perf]
#                     [--with-soak] [-j N] [--ctest-args "..."]
set -u
cd "$(dirname "$0")/.."
JOBS=$(( $(nproc) / 2 )); [[ $JOBS -lt 1 ]] && JOBS=1
ONLY=""; CONFIGURE=0; WITH_PERF=0; WITH_SOAK=0; CTEST_ARGS=""
while [[ $# -gt 0 ]]; do
    case "$1" in
        --only) ONLY="$2"; shift ;;
        --configure) CONFIGURE=1 ;;
        --with-perf) WITH_PERF=1 ;;
        --with-soak) WITH_SOAK=1 ;;
        -j) JOBS="$2"; shift ;;
        --ctest-args) CTEST_ARGS="$2"; shift ;;
        -h|--help) sed -n 2,10p "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
    shift
done

# name | build.sh flags to create it | ctest label exclusions
VARIANTS=(
    "build||"
    "build-asan|--asan|"
    "build-tsan|--tsan|"
    "build-ubsan|--ubsan|"
    "build-cov|--coverage|"
    "build-sqlite|--sqlite|"
    "build-redis|--redis|"
    "build-rados|--rados|"
    "build-tikv|--tikv|"
    "build-seastar|--seastar|"
    "build-fuzz|--fuzz|"
)
EXCLUDE="mint"
[[ $WITH_PERF -eq 0 ]] && EXCLUDE="$EXCLUDE|perf"
[[ $WITH_SOAK -eq 0 ]] && EXCLUDE="$EXCLUDE|soak"

declare -a RESULTS
run_variant() {  # run_variant <dir> <flags>
    local dir=$1 flags=$2 t0 rc build_rc env_prefix=()
    if [[ ! -d $dir ]]; then
        if [[ $CONFIGURE -eq 1 ]]; then
            ./build.sh $flags -B "$dir" -j "$JOBS" || { RESULTS+=("$dir|configure FAILED|-"); return; }
        else
            RESULTS+=("$dir|skipped (no dir; --configure creates it)|-"); return
        fi
    fi
    t0=$(date +%s)
    echo "===== $dir: build ====="
    cmake --build "$dir" -j "$JOBS" > "$dir/check-all-build.log" 2>&1
    build_rc=$?
    if [[ $build_rc -ne 0 ]]; then
        tail -30 "$dir/check-all-build.log"
        RESULTS+=("$dir|build FAILED|$(( $(date +%s) - t0 ))s"); return
    fi
    case "$dir" in
        *asan*)  env_prefix=(env ASAN_OPTIONS=detect_leaks=1:abort_on_error=1 LSAN_OPTIONS=suppressions=/dev/null) ;;
        *tsan*)  env_prefix=(env TSAN_OPTIONS=halt_on_error=1) ;;
        *ubsan*) env_prefix=(env UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1) ;;
    esac
    echo "===== $dir: ctest (-LE '$EXCLUDE') ====="
    "${env_prefix[@]}" ctest --test-dir "$dir" -LE "$EXCLUDE" --output-on-failure $CTEST_ARGS > "$dir/check-all-ctest.log" 2>&1
    rc=$?
    local summary
    summary=$(grep -E "tests passed|tests failed" "$dir/check-all-ctest.log" | tail -1)
    if [[ $rc -ne 0 ]]; then
        grep -E "^\s*[0-9]+ - .*\((Failed|Timeout|SEGFAULT|Subprocess aborted)\)" "$dir/check-all-ctest.log" | head -20
    fi
    RESULTS+=("$dir|${summary:-ctest rc=$rc}|$(( $(date +%s) - t0 ))s")
    [[ $rc -ne 0 ]] && ANY_FAIL=1
    return 0
}

ANY_FAIL=0
for v in "${VARIANTS[@]}"; do
    IFS='|' read -r dir flags _ <<< "$v"
    if [[ -n $ONLY ]]; then
        [[ ",$ONLY," == *",$dir,"* ]] || continue
    fi
    run_variant "$dir" "$flags"
done

echo
echo "===== check-all summary ====="
printf "%-14s %-52s %s\n" "variant" "result" "time"
for r in "${RESULTS[@]}"; do
    IFS='|' read -r d res t <<< "$r"
    printf "%-14s %-52s %s\n" "$d" "$res" "$t"
done
exit $ANY_FAIL
