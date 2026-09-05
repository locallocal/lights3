#!/usr/bin/env bash
# Soak / long-stability run (roadmap §6.1, docs/testing.md §5): loops s3adm bench
# rounds (put / get / stat / list / multipart-heavy put) against a gateway while
# sampling the process's RSS, open fds and a few convergence gauges from
# /-/metrics. At the end it asserts:
#   - RSS growth between the warm-up sample and the last sample < --max-rss-growth (%)
#   - open fds at the end <= fds after warm-up + --max-fd-growth
#   - duostore GC queue (when the backend is duostore) is back at 0 and the
#     multipart-active gauge is 0 (nothing leaked)
# Default backend is localfs (disk-backed, exercises staging/rename/fsync);
# --backend duostore runs the pack/GC engine so GC convergence is covered too.
#
# Usage: soak.sh <lights3> <s3adm> [--seconds N] [--backend localfs|duostore|memory]
#                [--concurrency N] [--max-rss-growth PCT] [--max-fd-growth N] [--keep-log]
set -u
BIN="${1:?usage: soak.sh <lights3> <s3adm> [options]}"
S3ADM="${2:?usage: soak.sh <lights3> <s3adm> [options]}"
shift 2
SECONDS_TOTAL="${LIGHTS3_SOAK_SECONDS:-600}"
BACKEND="${LIGHTS3_SOAK_BACKEND:-localfs}"
CONC="${LIGHTS3_SOAK_CONCURRENCY:-4}"
MAX_RSS="${LIGHTS3_SOAK_MAX_RSS_GROWTH:-25}"
MAX_FD="${LIGHTS3_SOAK_MAX_FD_GROWTH:-16}"
KEEP_LOG=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --seconds) SECONDS_TOTAL="$2"; shift ;;
        --backend) BACKEND="$2"; shift ;;
        --concurrency) CONC="$2"; shift ;;
        --max-rss-growth) MAX_RSS="$2"; shift ;;
        --max-fd-growth) MAX_FD="$2"; shift ;;
        --keep-log) KEEP_LOG=1 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
    shift
done

AK=SOAKACCESSKEY
SK=soak-secret-key
REGION=us-east-1
WORK=$(mktemp -d /tmp/lights3-soak.XXXXXX)
SRV_PID=""
cleanup() {
    [[ -n "$SRV_PID" ]] && kill -TERM "$SRV_PID" 2>/dev/null && wait "$SRV_PID" 2>/dev/null
    [[ $KEEP_LOG -eq 1 ]] && echo "logs kept in $WORK" || rm -rf "$WORK"
}
trap cleanup EXIT

case "$BACKEND" in
    memory)   BACKEND_YAML="  - name: data
    type: memory" ;;
    localfs)  BACKEND_YAML="  - name: data
    type: localfs
    root: $WORK/data
    staging: $WORK/staging" ;;
    duostore) BACKEND_YAML="  - name: data
    type: duostore
    root: $WORK/duo
    gc_interval: 5s
    gc_grace: 1s" ;;
    *) echo "unknown backend $BACKEND" >&2; exit 2 ;;
esac
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
$BACKEND_YAML
buckets:
  default_backend: data
log:
  level: info
YAML
LIGHTS3_FSYNC=0 "$BIN" --config "$WORK/config.yaml" > "$WORK/server.log" 2>&1 &
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

rss_kb() { awk '/^VmRSS:/ {print $2}' "/proc/$SRV_PID/status"; }
fd_count() { ls "/proc/$SRV_PID/fd" 2>/dev/null | wc -l; }
metric() { curl -s "$BASE/-/metrics" | awk -v n="$1" '$1 == n {print $2; exit}'; }
bench() {  # bench <mode> <seconds> [extra...]
    local mode=$1 secs=$2; shift 2
    LIGHTS3_ADMIN_AK=$AK LIGHTS3_ADMIN_SK=$SK "$S3ADM" bench "$mode" --bucket=soak \
        --concurrency="$CONC" --duration-sec="$secs" --objects=128 "$@" \
        --endpoint="$BASE" --region="$REGION" 2>&1 | sed -n 's/^ops \(.*\)/  '"$mode"': ops \1/p'
}

ROUND_SECS=$(( SECONDS_TOTAL / 10 )); [[ $ROUND_SECS -lt 3 ]] && ROUND_SECS=3
echo "soak: backend=$BACKEND total=${SECONDS_TOTAL}s round=${ROUND_SECS}s pid=$SRV_PID"
# Warm-up round: allocator arenas, thread stacks and pooled buffers settle here
bench put "$ROUND_SECS" --size=64K --keep >/dev/null
RSS0=$(rss_kb); FD0=$(fd_count)
echo "after warm-up: rss=${RSS0}KiB fds=$FD0"
START=$(date +%s)
ROUND=0
while (( $(date +%s) - START < SECONDS_TOTAL )); do
    ROUND=$((ROUND+1))
    case $((ROUND % 5)) in
        1) bench put  "$ROUND_SECS" --size=256K --keep ;;
        2) bench get  "$ROUND_SECS" --size=64K --keep ;;
        3) bench stat "$ROUND_SECS" ;;
        4) bench list "$ROUND_SECS" --max-keys=50 ;;
        0) bench put  "$ROUND_SECS" --size=8K ;;  # deletes its pool at the end -> GC work
    esac
    printf "  round %d: rss=%sKiB fds=%s gcq=%s mpu_active=%s\n" "$ROUND" "$(rss_kb)" "$(fd_count)" \
        "$(metric 'lights3_duostore_gcq_depth{backend="data"}' || true)" "$(metric lights3_multipart_active)"
    kill -0 "$SRV_PID" 2>/dev/null || { echo "gateway died mid-soak:"; tail -50 "$WORK/server.log"; exit 1; }
done
# Settle: let GC / flushes catch up before the final readings
sleep "$ROUND_SECS"
RSS1=$(rss_kb); FD1=$(fd_count)
GROWTH=$(awk -v a="$RSS0" -v b="$RSS1" 'BEGIN { if (a == 0) print 0; else printf "%.1f", (b - a) * 100.0 / a }')
MPU=$(metric lights3_multipart_active)
GCQ=$(metric 'lights3_duostore_gcq_depth{backend="data"}')
echo "final: rss=${RSS1}KiB (+${GROWTH}%) fds=$FD1 (warm-up $FD0) mpu_active=${MPU:-0} gcq=${GCQ:--}"
RC=0
awk -v g="$GROWTH" -v m="$MAX_RSS" 'BEGIN { exit !(g+0 > m+0) }' && { echo "FAIL: RSS grew ${GROWTH}% > ${MAX_RSS}%"; RC=1; }
[[ $FD1 -gt $((FD0 + MAX_FD)) ]] && { echo "FAIL: fds grew from $FD0 to $FD1 (> +$MAX_FD)"; RC=1; }
[[ "${MPU:-0}" != "0" ]] && { echo "FAIL: multipart uploads leaked: $MPU active"; RC=1; }
if [[ "$BACKEND" == "duostore" && -n "$GCQ" && "$GCQ" != "0" ]]; then
    echo "FAIL: duostore GC queue did not converge: depth $GCQ"; RC=1
fi
grep -q -i "error\|corrupt" "$WORK/server.log" && { echo "server log has ERROR lines:"; grep -i "error\|corrupt" "$WORK/server.log" | head -5; RC=1; }
[[ $RC -eq 0 ]] && echo "soak: PASS ($ROUND rounds)" || echo "soak: FAIL"
exit $RC
