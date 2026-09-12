#!/usr/bin/env bash
# S3 Tables client smoke (docs/s3-tables-design.md §13):
# a gateway on a memory backend with tables enabled, then scripts/tables/pyiceberg_smoke.py
# (table kept) and scripts/tables/duckdb_smoke.py against it. Opt-in: exit 77 (ctest SKIP)
# unless LIGHTS3_TABLES_SMOKE=1 and the python packages import. duckdb is optional (a
# missing package skips that half with a note; a failure is a failure).
#   run_tables_smoke.sh <lights3-binary>
set -u
BIN=${1:?lights3 binary}
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
PY=${PYTHON3:-python3}
if [[ "${LIGHTS3_TABLES_SMOKE:-0}" != "1" ]]; then
    echo "tables smoke: LIGHTS3_TABLES_SMOKE=1 not set; skipping"
    exit 77
fi
if ! "$PY" -c "import pyiceberg, pyarrow, botocore, boto3" 2>/dev/null; then
    echo "tables smoke: pyiceberg / pyarrow / botocore / boto3 not importable by $PY (set PYTHONPATH); skipping"
    exit 77
fi
WORK=$(mktemp -d)
SRV_PID=""
cleanup() {
    [[ -n "$SRV_PID" ]] && kill -TERM "$SRV_PID" 2>/dev/null && wait "$SRV_PID" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT
AK=SMOKEAKSMOKEAKSMOKE; SK=smoke-secret-key-smoke-secret-key
cat > "$WORK/config.yaml" <<YAML
http:
  bind: 127.0.0.1
  port: 0
  driver: builtin
auth:
  region: us-east-1
  credentials:
    - access_key: $AK
      secret_key: $SK
backends:
  - name: mem
    type: memory
buckets:
  default_backend: mem
tables:
  enabled: true
  maintenance:
    safety_window: 0s
log:
  level: info
YAML
"$BIN" --config "$WORK/config.yaml" > "$WORK/server.log" 2>&1 &
SRV_PID=$!
PORT=""
for _ in $(seq 1 100); do
    PORT=$(sed -n 's/.*listening on 127.0.0.1:\([0-9]*\).*/\1/p' "$WORK/server.log" | head -1)
    [[ -n "$PORT" ]] && break
    kill -0 "$SRV_PID" 2>/dev/null || { echo "gateway died:"; cat "$WORK/server.log"; exit 1; }
    sleep 0.1
done
[[ -n "$PORT" ]] || { echo "gateway did not report its port"; cat "$WORK/server.log"; exit 1; }
export LIGHTS3_ENDPOINT="http://127.0.0.1:$PORT" LIGHTS3_AK=$AK LIGHTS3_SK=$SK LIGHTS3_REGION=us-east-1
rc=0
echo "===== pyiceberg smoke ====="
LIGHTS3_SMOKE_KEEP=1 "$PY" "$ROOT/scripts/tables/pyiceberg_smoke.py" || rc=1
echo "===== duckdb smoke ====="
if "$PY" -c "import duckdb" 2>/dev/null; then
    "$PY" "$ROOT/scripts/tables/duckdb_smoke.py" || rc=1
else
    echo "duckdb not importable; skipped"
fi
echo "===== pyiceberg smoke: purge ====="
"$PY" "$ROOT/scripts/tables/pyiceberg_smoke.py" || rc=1
[[ $rc -eq 0 ]] && echo "tables smoke: PASS" || { echo "tables smoke: FAIL"; tail -30 "$WORK/server.log"; }
exit $rc
