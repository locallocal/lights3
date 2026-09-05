#!/usr/bin/env bash
# Performance baseline matrix (roadmap §4.3, docs/performance-baseline.md):
# `s3adm bench put|get` against one gateway per (driver × TLS) cell, localfs
# backend on a scratch directory, results as a Markdown table plus one JSON
# line per cell. Drivers not compiled into the binary are skipped.
#
# Usage: bench_matrix.sh <lights3> <s3adm> [--drivers builtin,beast,httplib,seastar]
#          [--tls on|off|both] [--duration N] [--concurrency N] [--size SZ] [--objects N]
#          [--modes put,get] [--io-threads N] [--json FILE] [--label TEXT] [--keep-log]
set -u
BIN="${1:?usage: bench_matrix.sh <lights3> <s3adm> [options]}"
S3ADM="${2:?usage: bench_matrix.sh <lights3> <s3adm> [options]}"
shift 2
DRIVERS=""
TLS="both"
DURATION="${LIGHTS3_BENCH_DURATION:-10}"
CONC="${LIGHTS3_BENCH_CONCURRENCY:-8}"
SIZE="${LIGHTS3_BENCH_SIZE:-4M}"
OBJECTS="${LIGHTS3_BENCH_OBJECTS:-32}"
MODES="put,get"
IO_THREADS=8
JSON_OUT=""
LABEL=""
KEEP_LOG=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --drivers) DRIVERS="$2"; shift ;;
        --tls) TLS="$2"; shift ;;
        --duration) DURATION="$2"; shift ;;
        --concurrency) CONC="$2"; shift ;;
        --size) SIZE="$2"; shift ;;
        --objects) OBJECTS="$2"; shift ;;
        --modes) MODES="$2"; shift ;;
        --io-threads) IO_THREADS="$2"; shift ;;
        --json) JSON_OUT="$2"; shift ;;
        --label) LABEL="$2"; shift ;;
        --keep-log) KEEP_LOG=1 ;;
        -h|--help) sed -n 2,10p "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
    shift
done

AK=BENCHACCESSKEY
SK=bench-secret-key
REGION=us-east-1
WORK=$(mktemp -d /tmp/lights3-benchmatrix.XXXXXX)
SRV_PID=""
cleanup() {
    [[ -n "$SRV_PID" ]] && kill -TERM "$SRV_PID" 2>/dev/null && wait "$SRV_PID" 2>/dev/null
    [[ $KEEP_LOG -eq 1 ]] && echo "logs kept in $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

# Drivers compiled into this binary (lights3 --version, roadmap §6.3); a binary
# from before --version existed is benchmarked as whatever --drivers says
VERSION=$("$BIN" --version 2>/dev/null | head -n 1)
BUILT=$("$BIN" --version 2>/dev/null | sed -n 's/^drivers:  *//p')
if [[ -z "$VERSION" ]]; then
    [[ -n "$DRIVERS" ]] || { echo "this lights3 has no --version; pass --drivers explicitly" >&2; exit 2; }
    VERSION="lights3 (pre --version build)"
    BUILT=${DRIVERS//,/ }
fi
[[ -z "$DRIVERS" ]] && DRIVERS=${BUILT// /,}

case "$TLS" in
    on) TLS_CELLS="on" ;;
    off) TLS_CELLS="off" ;;
    both) TLS_CELLS="off on" ;;
    *) echo "--tls must be on|off|both" >&2; exit 2 ;;
esac
if [[ "$TLS_CELLS" == *on* ]]; then
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
        -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
        -keyout "$WORK/tls.key" -out "$WORK/tls.crt" > /dev/null 2>&1 || { echo "openssl req failed" >&2; exit 1; }
fi

start_gateway() {  # start_gateway <driver> <tls on|off>; sets PORT / BASE
    local driver=$1 tls=$2
    rm -rf "$WORK/data" "$WORK/staging"
    mkdir -p "$WORK/data" "$WORK/staging"
    {
        echo "http:"
        echo "  driver: $driver"
        echo "  bind: 127.0.0.1"
        echo "  port: 0"
        echo "  io_threads: $IO_THREADS"
        if [[ $tls == on ]]; then
            echo "  tls_cert: $WORK/tls.crt"
            echo "  tls_key: $WORK/tls.key"
            echo "  tls_reload_interval: 0s"
        fi
        echo "runtime:"
        echo "  io_threads: 16"
        echo "auth:"
        echo "  credentials:"
        echo "    - access_key: $AK"
        echo "      secret_key: $SK"
        echo "  region: $REGION"
        echo "backends:"
        echo "  - name: fs"
        echo "    type: localfs"
        echo "    root: $WORK/data"
        echo "    staging: $WORK/staging"
        echo "buckets:"
        echo "  default_backend: fs"
        echo "log:"
        echo "  level: info"   # the "listening on" line is how the port is discovered
    } > "$WORK/config.yaml"
    "$BIN" --config "$WORK/config.yaml" > "$WORK/server-$driver-$tls.log" 2>&1 &
    SRV_PID=$!
    PORT=""
    for _ in $(seq 1 100); do
        PORT=$(sed -n 's/.*listening on 127.0.0.1:\([0-9]*\).*/\1/p' "$WORK/server-$driver-$tls.log" | head -1)
        [[ -n "$PORT" ]] && break
        kill -0 "$SRV_PID" 2>/dev/null || { echo "gateway ($driver, tls=$tls) died at startup:" >&2; cat "$WORK/server-$driver-$tls.log" >&2; SRV_PID=""; return 1; }
        sleep 0.1
    done
    [[ -z "$PORT" ]] && { echo "gateway ($driver, tls=$tls) did not report its port:" >&2; tail -n 5 "$WORK/server-$driver-$tls.log" >&2; stop_gateway; return 1; }
    if [[ $tls == on ]]; then BASE="https://127.0.0.1:$PORT"; else BASE="http://127.0.0.1:$PORT"; fi
    return 0
}
stop_gateway() {
    [[ -n "$SRV_PID" ]] && kill -TERM "$SRV_PID" 2>/dev/null && wait "$SRV_PID" 2>/dev/null
    SRV_PID=""
}

ROWS=()
JSON_LINES=()
run_cell() {  # run_cell <driver> <tls> <mode>
    local driver=$1 tls=$2 mode=$3 out parsed extra=()
    [[ $tls == on ]] && extra+=(--insecure)
    out=$(LIGHTS3_ADMIN_AK=$AK LIGHTS3_ADMIN_SK=$SK "$S3ADM" bench "$mode" --bucket=benchmatrix \
            --concurrency="$CONC" --duration-sec="$DURATION" --size="$SIZE" --objects="$OBJECTS" \
            --output=json --endpoint="$BASE" --region="$REGION" "${extra[@]}" 2>"$WORK/bench-$driver-$tls-$mode.err") \
        || { echo "s3adm bench $mode ($driver, tls=$tls) failed:" >&2; cat "$WORK/bench-$driver-$tls-$mode.err" >&2; return 1; }
    parsed=$(echo "$out" | python3 -c '
import json, sys
j = json.load(sys.stdin); l = j.get("latency_ms", {})
print(j["ops"], j["errors"], j["ops_per_s"], j["mib_per_s"], l.get("avg",""), l.get("p50",""), l.get("p99",""))') \
        || { echo "could not parse bench JSON:" >&2; echo "$out" >&2; return 1; }
    out=$(echo "$out" | python3 -c 'import json, sys; print(json.dumps(json.load(sys.stdin), separators=(",", ":")))')  # one line per cell
    read -r ops err opss mibs avg p50 p99 <<< "$parsed"
    ROWS+=("| $driver | $tls | $mode | $ops | $err | $opss | $mibs | $avg | $p50 | $p99 |")
    JSON_LINES+=("{\"label\":\"$LABEL\",\"version\":\"$VERSION\",\"driver\":\"$driver\",\"tls\":\"$tls\",\"mode\":\"$mode\",\"size\":\"$SIZE\",\"concurrency\":$CONC,\"duration_s\":$DURATION,\"result\":$out}")
    printf "  %-8s tls=%-3s %-4s %8s ops/s %9s MiB/s  p50 %7s ms  p99 %7s ms\n" "$driver" "$tls" "$mode" "$opss" "$mibs" "$p50" "$p99" >&2
}

echo "bench matrix: $VERSION" >&2
echo "  size=$SIZE concurrency=$CONC duration=${DURATION}s objects=$OBJECTS io_threads=$IO_THREADS drivers=$DRIVERS tls=$TLS" >&2
RC=0
IFS=',' read -r -a DRV <<< "$DRIVERS"
for driver in "${DRV[@]}"; do
    if [[ " $BUILT " != *" $driver "* ]]; then echo "  $driver: not compiled in, skipped" >&2; continue; fi
    for tls in $TLS_CELLS; do
        start_gateway "$driver" "$tls" || { RC=1; continue; }
        IFS=',' read -r -a MD <<< "$MODES"
        for mode in "${MD[@]}"; do run_cell "$driver" "$tls" "$mode" || RC=1; done
        stop_gateway
    done
done

echo
echo "<!-- $VERSION; size=$SIZE concurrency=$CONC duration=${DURATION}s objects=$OBJECTS io_threads=$IO_THREADS${LABEL:+; $LABEL} -->"
echo "| driver | tls | mode | ops | err | ops/s | MiB/s | avg ms | p50 ms | p99 ms |"
echo "| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |"
for r in "${ROWS[@]}"; do echo "$r"; done
if [[ -n "$JSON_OUT" ]]; then
    : > "$JSON_OUT"
    for j in "${JSON_LINES[@]}"; do echo "$j" >> "$JSON_OUT"; done
fi
exit $RC
