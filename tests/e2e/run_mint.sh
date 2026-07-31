#!/usr/bin/env bash
# MinIO mint 兼容性测试集（docs/s3-protocol.md §8）：起真实 lights3 进程，
# 用 minio/mint 容器对打。mint 依赖 docker——不可用时显式 SKIP（与
# duostore-rados/tikv e2e 的探测模式一致，本机无 docker 权限不算失败）。
#
# 用法: run_mint.sh <path-to-lights3-binary> [mint 测试名...]
#   测试名透传给 mint（如 s3cmd awscli aws-sdk-go）；不传则跑全集。
#   注意：全集覆盖 versioning/tagging 等 lights3 明确不支持的 API，预期部分
#   失败；建议以核心子集（s3cmd、awscli）作回归门槛，随协议覆盖扩集。
set -u

BIN="${1:?usage: run_mint.sh <path-to-lights3-binary> [mint tests...]}"
shift || true

if ! docker info >/dev/null 2>&1; then
    echo "[SKIP] mint: docker not available (daemon unreachable or no permission)"
    exit 0
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

# --network host：容器内直接访问宿主机端口（linux）；mint 结果落在容器内
# /mint/log，挂出来便于失败排查
mkdir -p "$WORK/mint-log"
docker run --rm --network host \
    -e "SERVER_ENDPOINT=127.0.0.1:$PORT" \
    -e "ACCESS_KEY=$AK" \
    -e "SECRET_KEY=$SK" \
    -e "ENABLE_HTTPS=0" \
    -v "$WORK/mint-log:/mint/log" \
    minio/mint "$@"
RC=$?
[[ $RC -ne 0 ]] && { echo "--- mint log ---"; cat "$WORK/mint-log"/*.log 2>/dev/null; }
exit $RC
