#!/usr/bin/env bash
# Line-coverage report (roadmap §6.1, docs/testing.md §7). Builds the coverage
# variant (build-cov: -O0 --coverage via build.sh --coverage), runs the unit
# tests (add --e2e for the e2e suites too), then reports with whichever tool is
# installed: gcovr (HTML + summary) > lcov/genhtml > plain gcov aggregation of
# the src/ tree (always available: gcov ships with gcc).
#
# Usage: coverage.sh [--e2e] [--no-build] [--no-test] [-j N] [-B build-cov]
set -euo pipefail
cd "$(dirname "$0")/.."
E2E=0; BUILD=1; TEST=1; JOBS=$(( $(nproc) / 2 )); [[ $JOBS -lt 1 ]] && JOBS=1; BUILD_DIR=build-cov
while [[ $# -gt 0 ]]; do
    case "$1" in
        --e2e) E2E=1 ;;
        --no-build) BUILD=0 ;;
        --no-test) TEST=0 ;;  # report from the counters of the previous run
        -j) JOBS="$2"; shift ;;
        -B) BUILD_DIR="$2"; shift ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
    shift
done
if [[ $BUILD -eq 1 ]]; then
    ./build.sh --coverage -B "$BUILD_DIR" -j "$JOBS"
fi
if [[ $TEST -eq 1 ]]; then
    # Fresh counters for this run
    find "$BUILD_DIR" -name '*.gcda' -delete
    if [[ $E2E -eq 1 ]]; then
        ctest --test-dir "$BUILD_DIR" -LE "perf|soak|mint" --output-on-failure || true
    else
        ctest --test-dir "$BUILD_DIR" -R '^unit_tests$|fuzz_regression' --output-on-failure || true
    fi
fi
OUT="$BUILD_DIR/coverage"
mkdir -p "$OUT"
if command -v gcovr >/dev/null; then
    gcovr -r . --object-directory "$BUILD_DIR" -f 'src/.*' --html-details "$OUT/index.html" --print-summary | tee "$OUT/summary.txt"
    echo "HTML: $OUT/index.html"
elif command -v lcov >/dev/null && command -v genhtml >/dev/null; then
    lcov --capture --directory "$BUILD_DIR" --output-file "$OUT/all.info" --quiet
    lcov --extract "$OUT/all.info" "$PWD/src/*" --output-file "$OUT/src.info" --quiet
    genhtml "$OUT/src.info" --output-directory "$OUT/html" --quiet
    lcov --summary "$OUT/src.info" | tee "$OUT/summary.txt"
    echo "HTML: $OUT/html/index.html"
else
    # gcov's JSON format lists every instrumented line per function, so template
    # instantiations and headers included from many TUs repeat lines: aggregate by
    # (file, line number) union across all gcda files. Note: GCC's gcov barely
    # instruments coroutine bodies (the ramp function only), so coroutine-heavy
    # files under-report — gcovr/lcov share the limitation
    echo "gcovr/lcov not installed: aggregating gcov JSON for src/ (pip install gcovr for HTML)"
    ( cd "$BUILD_DIR" && find . -name '*.gcda' -print0 | xargs -0 -r -n 1 gcov --json-format --stdout 2>/dev/null ) |
        python3 "$(dirname "$0")/coverage_aggregate.py" "$PWD/src/" | tee "$OUT/summary.txt"
fi
