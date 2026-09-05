#!/usr/bin/env bash
# Performance regression gate (roadmap §6.1, docs/testing.md §5): starts a
# memory-backend gateway, runs `s3adm bench put` and `get`, and fails when the
# throughput floor or the p99 ceiling is missed. Thresholds are deliberately
# loose defaults for a shared/loaded developer box — tighten per environment
# through the flags or the LIGHTS3_BENCH_* variables.
#
# Usage: bench_gate.sh <lights3> <s3adm> [--duration N] [--concurrency N] [--size SZ]
#                      [--min-put-ops N] [--min-get-ops N] [--max-p99-ms N] [--keep-log]
set -u
BIN="${1:?usage: bench_gate.sh <lights3> <s3adm> [options]}"
S3ADM="${2:?usage: bench_gate.sh <lights3> <s3adm> [options]}"
shift 2
DURATION="${LIGHTS3_BENCH_DURATION:-5}"
CONC="${LIGHTS3_BENCH_CONCURRENCY:-4}"
SIZE="${LIGHTS3_BENCH_SIZE:-16K}"
MIN_PUT="${LIGHTS3_BENCH_MIN_PUT_OPS:-300}"
MIN_GET="${LIGHTS3_BENCH_MIN_GET_OPS:-300}"
MAX_P99="${LIGHTS3_BENCH_MAX_P99_MS:-500}"
KEEP_LOG=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --duration) DURATION="$2"; shift ;;
        --concurrency) CONC="$2"; shift ;;
        --size) SIZE="$2"; shift ;;
        --min-put-ops) MIN_PUT="$2"; shift ;;
        --min-get-ops) MIN_GET="$2"; shift ;;
        --max-p99-ms) MAX_P99="$2"; shift ;;
        --keep-log) KEEP_LOG=1 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
    shift
done

AK=BENCHACCESSKEY
SK=bench-secret-key
REGION=us-east-1
WORK=$(mktemp -d /tmp/lights3-bench.XXXXXX)
SRV_PID=""
cleanup() {
    [[ -n "$SRV_PID" ]] && kill -TERM "$SRV_PID" 2>/dev/null && wait "$SRV_PID" 2>/dev/null
    [[ $KEEP_LOG -eq 1 ]] && echo "logs kept in $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

cat > "$WORK/config.yaml" <<YAML
http:
  driver: builtin
  bind: 127.0.0.1
  port: 0
runtime:
  io_threads: 8
auth:
  credentials:
    - access_key: $AK
      secret_key: $SK
  region: $REGION
backends:
  - name: mem
    type: memory
buckets:
  default_backend: mem
log:
  level: info
YAML
"$BIN" --config "$WORK/config.yaml" > "$WORK/server.log" 2>&1 &
SRV_PID=$!
PORT=""
for _ in $(seq 1 50); do
    PORT=$(sed -n 's/.*listening on 127.0.0.1:\([0-9]*\).*/\1/p' "$WORK/server.log" | head -1)
    [[ -n "$PORT" ]] && break
    kill -0 "$SRV_PID" 2>/dev/null || { echo "gateway died at startup:"; cat "$WORK/server.log"; exit 1; }
    sleep 0.1
done
[[ -z "$PORT" ]] && { echo "gateway did not report its port"; cat "$WORK/server.log"; exit 1; }
BASE="http://127.0.0.1:$PORT"

# `s3adm bench --output=json` (roadmap §6.2) is the parsing contract: one JSON object on stdout
run_mode() {  # run_mode <put|get> <min-ops>
    local mode=$1 min=$2 out parsed ok err ops p99
    out=$(LIGHTS3_ADMIN_AK=$AK LIGHTS3_ADMIN_SK=$SK "$S3ADM" bench "$mode" --bucket=benchgate \
            --concurrency="$CONC" --duration-sec="$DURATION" --size="$SIZE" --objects=64 --output=json \
            --endpoint="$BASE" --region="$REGION" 2>"$WORK/bench-$mode.err") || { echo "s3adm bench $mode failed:"; cat "$WORK/bench-$mode.err"; echo "$out"; return 1; }
    parsed=$(echo "$out" | python3 -c '
import json, sys
j = json.load(sys.stdin)
print(j["ops"], j["errors"], j["ops_per_s"], j.get("latency_ms", {}).get("p99", ""))' 2>/dev/null) || { echo "could not parse bench JSON:"; echo "$out"; return 1; }
    read -r ok err ops p99 <<< "$parsed"
    printf "bench %-3s: %s ok, %s err, %s ops/s, p99 %s ms (floor %s ops/s, ceiling %s ms)\n" \
        "$mode" "$ok" "$err" "$ops" "${p99:-?}" "$min" "$MAX_P99"
    local rc=0
    [[ "$err" != "0" ]] && { echo "  FAIL: $err errors"; rc=1; }
    awk -v v="$ops" -v m="$min" 'BEGIN { exit !(v+0 < m+0) }' && { echo "  FAIL: throughput below floor"; rc=1; }
    [[ -n "$p99" ]] && awk -v v="$p99" -v m="$MAX_P99" 'BEGIN { exit !(v+0 > m+0) }' && { echo "  FAIL: p99 above ceiling"; rc=1; }
    return $rc
}
RC=0
run_mode put "$MIN_PUT" || RC=1
run_mode get "$MIN_GET" || RC=1
[[ $RC -eq 0 ]] && echo "bench gate: PASS" || echo "bench gate: FAIL"
exit $RC
