#!/usr/bin/env bash
# MinIO mint compatibility test suite (docs/s3-protocol.md §8): start a real lights3
# process and run the minio/mint container against it. mint depends on docker --
# explicit SKIP when unavailable (same probing pattern as the duostore-rados/tikv e2e;
# lacking docker permission on this machine does not count as failure).
#
# Usage: run_mint.sh <path-to-lights3-binary> [mint test names...]
#   Test names are passed through to mint (e.g. s3cmd awscli aws-sdk-go); with none
#   given, the full suite runs.
#   Note: the full suite covers APIs lights3 explicitly does not support
#   (versioning/tagging etc.), so partial failures are expected; use the core subset
#   (s3cmd, awscli) as the regression gate and grow it with protocol coverage.
set -u

BIN="${1:?usage: run_mint.sh <path-to-lights3-binary> [mint tests...]}"
shift || true

# 77 = ctest SKIP_RETURN_CODE (CMakeLists.txt): the test shows as "Not Run" rather than passed
if ! docker info >/dev/null 2>&1; then
    echo "[SKIP] mint: docker not available (daemon unreachable or no permission)"
    exit 77
fi

AK=MINTACCESSKEY
SK=mint-secret-key
WORK=$(mktemp -d /tmp/lights3-mint.XXXXXX)
SRV_PID=""
cleanup() {
    [[ -n "$SRV_PID" ]] && kill "$SRV_PID" 2>/dev/null
    [[ -n "$SRV_PID" ]] && wait "$SRV_PID" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

cat > "$WORK/config.yaml" <<EOF
http:
  driver: builtin
  bind: 0.0.0.0
  port: 0
runtime:
  io_threads: 8
auth:
  credentials:
    - access_key: $AK
      secret_key: $SK
  region: us-east-1
backends:
  - name: data
    type: localfs
    root: $WORK/data
    staging: $WORK/staging
buckets:
  default_backend: data
log:
  level: info
EOF

"$BIN" --config "$WORK/config.yaml" > "$WORK/server.log" 2>&1 &
SRV_PID=$!
PORT=""
for _ in $(seq 1 50); do
    PORT=$(sed -n 's/.*listening on 0.0.0.0:\([0-9]*\).*/\1/p' "$WORK/server.log" | head -1)
    [[ -n "$PORT" ]] && break
    kill -0 "$SRV_PID" 2>/dev/null || { echo "lights3 died at startup:"; cat "$WORK/server.log"; exit 1; }
    sleep 0.1
done
[[ -z "$PORT" ]] && { echo "lights3 did not report port"; cat "$WORK/server.log"; exit 1; }
echo "lights3 up: 127.0.0.1:$PORT (pid $SRV_PID)"

# --network host: the container reaches host ports directly (linux); mint results land
# in /mint/log inside the container, mounted out for failure triage
mkdir -p "$WORK/mint-log"
docker run --rm --network host \
    -e "SERVER_ENDPOINT=127.0.0.1:$PORT" \
    -e "ACCESS_KEY=$AK" \
    -e "SECRET_KEY=$SK" \
    -e "ENABLE_HTTPS=0" \
    -v "$WORK/mint-log:/mint/log" \
    minio/mint "$@"
RC=$?
# Baseline bookkeeping (docs/testing.md §6): mint's log.json lists one line per
# test with a status; summarize pass/fail per suite so a run leaves a number behind
if [[ -f "$WORK/mint-log/log.json" ]]; then
    echo "--- mint baseline ($(date -u +%Y-%m-%dT%H:%M:%SZ)) ---"
    python3 - "$WORK/mint-log/log.json" <<'PY' 2>/dev/null || true
import json, sys, collections
c = collections.defaultdict(lambda: collections.Counter())
for line in open(sys.argv[1]):
    line = line.strip()
    if not line: continue
    try: j = json.loads(line)
    except ValueError: continue
    c[j.get("name", "?")][j.get("status", "?")] += 1
for name, st in sorted(c.items()):
    total = sum(st.values())
    print(f"{name:12s} PASS {st.get('PASS', 0):4d} / {total:4d}   FAIL {st.get('FAIL', 0)}   NA {st.get('NA', 0)}")
PY
fi
[[ $RC -ne 0 ]] && { echo "--- mint log ---"; cat "$WORK/mint-log"/*.log 2>/dev/null; }
exit $RC
