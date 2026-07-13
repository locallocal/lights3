#!/usr/bin/env bash
# e2e：起真实 lights3 进程（localfs 后端 + SigV4 认证），用 curl --aws-sigv4 全流程验证
set -u

BIN="${1:?usage: run_e2e.sh <path-to-lights3-binary> [driver]}"
DRIVER="${2:-builtin}"
AK=E2EACCESSKEY
SK=e2e-secret-key
REGION=us-east-1
WORK=$(mktemp -d /tmp/lights3-e2e.XXXXXX)
PASS=0; FAIL=0

cleanup() {
    [[ -n "${SRV_PID:-}" ]] && kill "$SRV_PID" 2>/dev/null
    [[ -n "${SRV_PID:-}" ]] && wait "$SRV_PID" 2>/dev/null
    rm -rf "$WORK"
}
trap cleanup EXIT

check() {  # check <描述> <期望> <实际>
    if [[ "$2" == "$3" ]]; then
        echo "[ OK ] $1"
        PASS=$((PASS+1))
    else
        echo "[FAIL] $1: expected '$2' got '$3'"
        FAIL=$((FAIL+1))
    fi
}

# ---------- 配置与启动 ----------
cat > "$WORK/config.yaml" <<EOF
http:
  driver: $DRIVER
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
  - name: localdata
    type: localfs
    root: $WORK/data
    staging: $WORK/staging
buckets:
  default_backend: localdata
log:
  level: info
EOF

"$BIN" --config "$WORK/config.yaml" > "$WORK/server.log" 2>&1 &
SRV_PID=$!

PORT=""
for _ in $(seq 1 50); do
    PORT=$(sed -n 's/.*listening on 127.0.0.1:\([0-9]*\).*/\1/p' "$WORK/server.log" | head -1)
    [[ -n "$PORT" ]] && break
    kill -0 "$SRV_PID" 2>/dev/null || { echo "server died at startup:"; cat "$WORK/server.log"; exit 1; }
    sleep 0.1
done
[[ -z "$PORT" ]] && { echo "server did not report port"; cat "$WORK/server.log"; exit 1; }
BASE="http://127.0.0.1:$PORT"
echo "server up: $BASE (pid $SRV_PID)"

s3curl() {  # s3curl <curl args...> —— 带 SigV4 签名
    curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK:$SK" "$@"
}

# ---------- 用例 ----------
check "healthz（免认证）" "200" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/-/healthz")"
check "未签名请求被拒" "403" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/mybucket")"
check "错误密钥被拒" "403" \
    "$(curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK:wrong-secret" -o /dev/null -w '%{http_code}' "$BASE/mybucket" -X PUT)"

check "CreateBucket" "200" "$(s3curl -o /dev/null -w '%{http_code}' -X PUT "$BASE/mybucket")"
check "HeadBucket" "200" "$(s3curl -o /dev/null -w '%{http_code}' -I "$BASE/mybucket")"
check "重复创建 409" "409" "$(s3curl -o /dev/null -w '%{http_code}' -X PUT "$BASE/mybucket")"

# 5MB 随机文件 PUT/GET 往返
dd if=/dev/urandom of="$WORK/big.bin" bs=1M count=5 2>/dev/null
MD5=$(md5sum "$WORK/big.bin" | cut -d' ' -f1)
check "PutObject(5MB)" "200" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X PUT --data-binary "@$WORK/big.bin" \
       -H 'Content-Type: application/x-lights3-test' "$BASE/mybucket/dir/big.bin")"

s3curl -o "$WORK/big.out" "$BASE/mybucket/dir/big.bin"
check "GetObject 内容一致" "$MD5" "$(md5sum "$WORK/big.out" | cut -d' ' -f1)"

HEAD_OUT=$(s3curl -I "$BASE/mybucket/dir/big.bin")
check "HeadObject ETag=MD5" "\"$MD5\"" "$(echo "$HEAD_OUT" | tr -d '\r' | sed -n 's/^etag: //Ip')"
check "HeadObject Content-Type 保留" "application/x-lights3-test" \
    "$(echo "$HEAD_OUT" | tr -d '\r' | sed -n 's/^content-type: //Ip')"
check "HeadObject Content-Length" "5242880" \
    "$(echo "$HEAD_OUT" | tr -d '\r' | sed -n 's/^content-length: //Ip')"

# Range 下载
s3curl -o "$WORK/part.out" -r 1024-2047 "$BASE/mybucket/dir/big.bin"
dd if="$WORK/big.bin" of="$WORK/part.ref" bs=1 skip=1024 count=1024 2>/dev/null
check "Range 下载(1KiB@1KiB)" "$(md5sum "$WORK/part.ref" | cut -d' ' -f1)" \
    "$(md5sum "$WORK/part.out" | cut -d' ' -f1)"
check "Range 响应 206" "206" \
    "$(s3curl -o /dev/null -w '%{http_code}' -r 0-99 "$BASE/mybucket/dir/big.bin")"

# List
s3curl -o /dev/null -X PUT --data-binary 'x' "$BASE/mybucket/dir/small.txt"
s3curl -o /dev/null -X PUT --data-binary 'y' "$BASE/mybucket/top.txt"
LIST=$(s3curl "$BASE/mybucket?list-type=2&delimiter=%2F")
echo "$LIST" | grep -q '<Key>top.txt</Key>' && echo "$LIST" | grep -q '<Prefix>dir/</Prefix>'
check "ListObjectsV2 + delimiter" "0" "$?"
LIST2=$(s3curl "$BASE/mybucket?list-type=2&prefix=dir%2F")
check "ListObjectsV2 prefix 计数" "2" "$(echo "$LIST2" | grep -o '<Key>' | wc -l)"

# ListBuckets
check "ListBuckets 包含 bucket" "0" \
    "$(s3curl "$BASE/" | grep -q '<Name>mybucket</Name>'; echo $?)"

# 删除与 404
check "DeleteObject" "204" "$(s3curl -o /dev/null -w '%{http_code}' -X DELETE "$BASE/mybucket/dir/big.bin")"
check "删后 GET 404" "404" "$(s3curl -o /dev/null -w '%{http_code}' "$BASE/mybucket/dir/big.bin")"
GET404=$(s3curl "$BASE/mybucket/dir/big.bin")
check "404 错误 XML" "0" "$(echo "$GET404" | grep -q '<Code>NoSuchKey</Code>'; echo $?)"

s3curl -o /dev/null -X DELETE "$BASE/mybucket/dir/small.txt"
s3curl -o /dev/null -X DELETE "$BASE/mybucket/top.txt"
check "DeleteBucket" "204" "$(s3curl -o /dev/null -w '%{http_code}' -X DELETE "$BASE/mybucket")"

# 优雅退出
kill -TERM "$SRV_PID"
EXITED=1
for _ in $(seq 1 50); do
    kill -0 "$SRV_PID" 2>/dev/null || { EXITED=0; break; }
    sleep 0.1
done
check "SIGTERM 优雅退出" "0" "$EXITED"
wait "$SRV_PID" 2>/dev/null
SRV_PID=""

echo
echo "e2e: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]] || { echo "--- server.log ---"; cat "$WORK/server.log"; exit 1; }
exit 0
