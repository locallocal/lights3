#!/usr/bin/env bash
# e2e: start a real lights3 process (localfs backend + SigV4 auth) and verify the full flow with curl --aws-sigv4
set -u

BIN="${1:?usage: run_e2e.sh <path-to-lights3-binary> [driver] [backend-type]}"
DRIVER="${2:-builtin}"
# localfs | xlocalfs | tiered (localfs+memory, docs/tiered-storage.md)
# | cloudproxy | tiered-cloudproxy (two instances: instance B acts as the "cloud", docs/cloudproxy-backend.md §10)
# | tiered-duostore (duostore as the cloud) | tiered-duolocal (duostore as the local/hot side, roadmap §3.6 ⑥)
BACKEND="${3:-localfs}"
AK=E2EACCESSKEY
SK=e2e-secret-key
REGION=us-east-1
WORK=$(mktemp -d /tmp/lights3-e2e.XXXXXX)
PASS=0; FAIL=0

cleanup() {
    [[ -n "${SRV_PID:-}" ]] && kill "$SRV_PID" 2>/dev/null
    [[ -n "${SRV_PID:-}" ]] && wait "$SRV_PID" 2>/dev/null
    [[ -n "${RSRV_PID:-}" ]] && kill "$RSRV_PID" 2>/dev/null
    [[ -n "${RSRV_PID:-}" ]] && wait "$RSRV_PID" 2>/dev/null
    [[ -n "${REDIS_PID:-}" ]] && kill "$REDIS_PID" 2>/dev/null
    [[ -n "${REDIS_PID:-}" ]] && wait "$REDIS_PID" 2>/dev/null
    # rados data-plane object cleanup (best-effort; before GC materializes, DELETE only records and does not remove objects)
    if [[ -n "${RADOS_NS:-}" ]] && command -v rados >/dev/null; then
        rados -c "$LIGHTS3_TEST_RADOS_CONF" -p "$LIGHTS3_TEST_RADOS_POOL" \
            --namespace "$RADOS_NS" ls 2>/dev/null | while read -r obj; do
            rados -c "$LIGHTS3_TEST_RADOS_CONF" -p "$LIGHTS3_TEST_RADOS_POOL" \
                --namespace "$RADOS_NS" rm "$obj" 2>/dev/null
        done
    fi
    rm -rf "$WORK"
}
trap cleanup EXIT

# ---------- duostore-redis scenario: spawn a private redis (docs/duostore-redis-meta.md §9) ----------
# LIGHTS3_TEST_REDIS_URI=redis://host:port points at an external instance instead
# (docker compose --profile e2e, docs/deployment.md §4): the run isolates itself with
# a unique key prefix and leaves the instance running
REDIS_PID=""
REDIS_URI=""
REDIS_PREFIX=""
if [[ "$BACKEND" == "duostore-redis" ]]; then
    REDIS_PREFIX="duo:"  # the backend default; unique per run on a shared external instance
    if [[ -n "${LIGHTS3_TEST_REDIS_URI:-}" ]]; then
        REDIS_URI="$LIGHTS3_TEST_REDIS_URI"
        REDIS_PREFIX="e2e-$$-$RANDOM:"
        echo "redis: external $REDIS_URI prefix=$REDIS_PREFIX"
    elif ! command -v redis-server >/dev/null; then
        echo "[SKIP] duostore-redis: redis-server not available (set LIGHTS3_TEST_REDIS_URI for an external one)"
        exit 0
    fi
fi
if [[ "$BACKEND" == "duostore-redis" && -z "$REDIS_URI" ]]; then
    redis-server --port 0 --unixsocket "$WORK/redis.sock" --save '' --appendonly no \
        --dir "$WORK" > "$WORK/redis.log" 2>&1 &
    REDIS_PID=$!
    for _ in $(seq 1 50); do
        [[ -S "$WORK/redis.sock" ]] && break
        kill -0 "$REDIS_PID" 2>/dev/null || { echo "redis-server died at startup:"; cat "$WORK/redis.log"; exit 1; }
        sleep 0.1
    done
    [[ -S "$WORK/redis.sock" ]] || { echo "redis-server did not come up"; cat "$WORK/redis.log"; exit 1; }
    echo "redis up: $WORK/redis.sock (pid $REDIS_PID)"
    REDIS_URI="unix://$WORK/redis.sock"
fi

# ---------- duostore-rados scenario: probe for a real cluster (docs/duostore-rados-data.md §11) ----------
# A cluster cannot be spun up casually like redis (full mon+osd+cephx dependency set);
# runs only when both env vars LIGHTS3_TEST_RADOS_CONF + LIGHTS3_TEST_RADOS_POOL are set, otherwise explicit SKIP
RADOS_NS=""
if [[ "$BACKEND" == "duostore-rados" ]]; then
    if [[ -z "${LIGHTS3_TEST_RADOS_CONF:-}" || -z "${LIGHTS3_TEST_RADOS_POOL:-}" ]]; then
        echo "[SKIP] duostore-rados: LIGHTS3_TEST_RADOS_CONF/LIGHTS3_TEST_RADOS_POOL not set"
        exit 0
    fi
    RADOS_NS="e2e-$$-$RANDOM"  # unique namespace isolation, the pool can be reused
    echo "rados: conf=$LIGHTS3_TEST_RADOS_CONF pool=$LIGHTS3_TEST_RADOS_POOL ns=$RADOS_NS"
fi

# ---------- duostore-tikv scenario: probe for a real cluster (docs/duostore-tikv-meta.md §10) ----------
# PD+TiKV cannot be spun up casually (tiup/multi-process dependencies); runs only when
# LIGHTS3_TEST_PD_ADDR is set, otherwise explicit SKIP.
# Cluster-side residue: TxnKV has no client-reachable delete-by-prefix (tikv-ctl is not
# assumed present); the keys left per run are bounded and prefix-unique -- the cases
# delete their own buckets/objects, so the residue is only schema, counters, and version
# garbage of already-settled gcq entries, reclaimed as the cluster GC safepoint advances
# (docs/duostore-tikv-meta.md §7.3)
TIKV_PREFIX=""
if [[ "$BACKEND" == "duostore-tikv" ]]; then
    if [[ -z "${LIGHTS3_TEST_PD_ADDR:-}" ]]; then
        echo "[SKIP] duostore-tikv: LIGHTS3_TEST_PD_ADDR not set"
        exit 0
    fi
    TIKV_PREFIX="e2e-$$-$RANDOM:"  # unique prefix isolation, the cluster can be reused
    echo "tikv: pd=$LIGHTS3_TEST_PD_ADDR prefix=$TIKV_PREFIX"
fi

# ---------- cloudproxy scenario: first start the "cloud" instance B (itself also lights3) ----------
CLOUD_AK=E2ECLOUDKEY
CLOUD_SK=e2e-cloud-secret
RPORT=""
RSRV_PID=""
if [[ "$BACKEND" == "cloudproxy" || "$BACKEND" == "tiered-cloudproxy" ]]; then
    cat > "$WORK/remote.yaml" <<EOF
http:
  driver: builtin
  bind: 127.0.0.1
  port: 0
runtime:
  io_threads: 8
auth:
  credentials:
    - access_key: $CLOUD_AK
      secret_key: $CLOUD_SK
  region: $REGION
backends:
  - name: clouddata
    type: localfs
    root: $WORK/remote-data
    staging: $WORK/remote-staging
buckets:
  default_backend: clouddata
log:
  level: info
EOF
    "$BIN" --config "$WORK/remote.yaml" > "$WORK/remote.log" 2>&1 &
    RSRV_PID=$!
    for _ in $(seq 1 50); do
        RPORT=$(sed -n 's/.*listening on 127.0.0.1:\([0-9]*\).*/\1/p' "$WORK/remote.log" | head -1)
        [[ -n "$RPORT" ]] && break
        kill -0 "$RSRV_PID" 2>/dev/null || { echo "remote instance died at startup:"; cat "$WORK/remote.log"; exit 1; }
        sleep 0.1
    done
    [[ -z "$RPORT" ]] && { echo "remote instance did not report port"; cat "$WORK/remote.log"; exit 1; }
    echo "remote (cloud) instance up: 127.0.0.1:$RPORT (pid $RSRV_PID)"
fi

check() {  # check <description> <expected> <actual>
    if [[ "$2" == "$3" ]]; then
        echo "[ OK ] $1"
        PASS=$((PASS+1))
    else
        echo "[FAIL] $1: expected '$2' got '$3'"
        FAIL=$((FAIL+1))
    fi
}

# ---------- Config and startup ----------
# External credentials file (credential-management.md §10.2): hot-reload provider, data plane only
FILE_AK=E2EFILEKEY
FILE_SK=e2e-file-secret
cat > "$WORK/creds.json" <<EOF
{"credentials": [{"access_key": "$FILE_AK", "secret_key": "$FILE_SK"}]}
EOF
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
  credentials_file: $WORK/creds.json
backends:
$(if [[ "$BACKEND" == "tiered" ]]; then cat <<TIER
  - name: localdata
    type: localfs
    root: $WORK/data
    staging: $WORK/staging
  - name: cloudmem
    type: memory
  - name: tierdata
    type: tiered
    local: localdata
    cloud: cloudmem
    scan_interval: 0s
TIER
elif [[ "$BACKEND" == "cloudproxy" ]]; then cat <<CLOUD
  - name: tierdata
    type: cloudproxy
    endpoint: http://127.0.0.1:$RPORT
    region: $REGION
    access_key: $CLOUD_AK
    secret_key: $CLOUD_SK
    bucket_prefix: e2e-
CLOUD
elif [[ "$BACKEND" == "xlocalfs" ]]; then cat <<XLFS
  - name: tierdata
    type: xlocalfs
    root: $WORK/data
    staging: $WORK/staging
    rings: 2
    read_depth: 3
    write_depth: 3
    fixed_buffers: 8
XLFS
elif [[ "$BACKEND" == "duostore-uring" ]]; then cat <<DUOURING
  - name: tierdata
    type: duostore
    root: $WORK/data
    fs_uring: true
DUOURING
elif [[ "$BACKEND" == "duostore-redis" ]]; then cat <<DUOREDIS
  - name: tierdata
    type: duostore
    root: $WORK/data
    meta: redis
    redis_uri: $REDIS_URI
    redis_prefix: "$REDIS_PREFIX"
DUOREDIS
elif [[ "$BACKEND" == "duostore-sqlite" ]]; then cat <<DUOSQLITE
  - name: tierdata
    type: duostore
    root: $WORK/data
    meta: sqlite
DUOSQLITE
elif [[ "$BACKEND" == "duostore-rados" ]]; then cat <<DUORADOS
  - name: tierdata
    type: duostore
    root: $WORK/data
    data: rados
    rados_conf: ${LIGHTS3_TEST_RADOS_CONF:-}
    rados_client: ${LIGHTS3_TEST_RADOS_CLIENT:-client.admin}
    rados_pool: ${LIGHTS3_TEST_RADOS_POOL:-}
    rados_namespace: $RADOS_NS
DUORADOS
elif [[ "$BACKEND" == "duostore-tikv" ]]; then cat <<DUOTIKV
  - name: tierdata
    type: duostore
    root: $WORK/data
    meta: tikv
    pd_endpoints: "${LIGHTS3_TEST_PD_ADDR:-}"
    tikv_prefix: "$TIKV_PREFIX"
DUOTIKV
elif [[ "$BACKEND" == "tiered-duostore" ]]; then cat <<TIERDUO
  - name: localdata
    type: localfs
    root: $WORK/data
    staging: $WORK/staging
  - name: cloudduo
    type: duostore
    root: $WORK/cloud-duo
  - name: tierdata
    type: tiered
    local: localdata
    cloud: cloudduo
    scan_interval: 0s
TIERDUO
elif [[ "$BACKEND" == "tiered-duolocal" ]]; then cat <<TIERDUOLOCAL
  - name: duolocal
    type: duostore
    root: $WORK/duo-local
  - name: cloudmem
    type: memory
  - name: tierdata
    type: tiered
    local: duolocal
    cloud: cloudmem
    scan_interval: 0s
TIERDUOLOCAL
elif [[ "$BACKEND" == "tiered-cloudproxy" ]]; then cat <<TIERCLOUD
  - name: localdata
    type: localfs
    root: $WORK/data
    staging: $WORK/staging
  - name: cloudpx
    type: cloudproxy
    endpoint: http://127.0.0.1:$RPORT
    region: $REGION
    access_key: $CLOUD_AK
    secret_key: $CLOUD_SK
    bucket_prefix: e2e-
  - name: tierdata
    type: tiered
    local: localdata
    cloud: cloudpx
    scan_interval: 0s
TIERCLOUD
else cat <<PLAIN
  - name: tierdata
    type: $BACKEND
    root: $WORK/data
    staging: $WORK/staging
PLAIN
fi)
website:
  - bucket: e2esite
    index_suffix: index.html
    error_key: error.html
buckets:
  default_backend: tierdata
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

s3curl() {  # s3curl <curl args...> -- with SigV4 signing
    curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK:$SK" "$@"
}

# ---------- roadmap §6.2: lights3 --check-config (dry run, no backend opened) ----------
check "lights3 --check-config accepts the running config" "0" \
    "$("$BIN" --check-config --config="$WORK/config.yaml" > "$WORK/check-config.out" 2>&1; echo $?)"
check "check-config prints the resolved summary" "0" "$(grep -q '^config .*: ok$' "$WORK/check-config.out" && grep -q '^  backends ' "$WORK/check-config.out"; echo $?)"
sed 's/^  default_backend: tierdata$/  default_backend: ghost/' "$WORK/config.yaml" > "$WORK/config-bad.yaml"
check "lights3 --check-config rejects a broken config with exit 1" "1" \
    "$("$BIN" --check-config --config="$WORK/config-bad.yaml" >/dev/null 2>&1; echo $?)"
check "check-config leaves no data directories behind" "0" "$([[ ! -e "$WORK/check-config-data" ]]; echo $?)"

# ---------- Test cases ----------
check "healthz (no auth)" "200" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/-/healthz")"
check "unsigned request rejected" "403" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/mybucket")"
check "wrong secret rejected" "403" \
    "$(curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK:wrong-secret" -o /dev/null -w '%{http_code}' "$BASE/mybucket" -X PUT)"

check "CreateBucket" "200" "$(s3curl -o /dev/null -w '%{http_code}' -X PUT "$BASE/mybucket")"
check "HeadBucket" "200" "$(s3curl -o /dev/null -w '%{http_code}' -I "$BASE/mybucket")"
check "duplicate create 409" "409" "$(s3curl -o /dev/null -w '%{http_code}' -X PUT "$BASE/mybucket")"

# roadmap §5.4: W3C trace context — the client's trace id is kept on the access
# line (with the caller's span as parent) and echoed in traceresponse; the
# cloudproxy hop forwards a child span so the remote instance logs the same id
E2E_TRACE=4bf92f3577b34da6a3ce929d0e0e4736
TRACE_HDRS=$(s3curl -D - -o /dev/null -X PUT --data-binary "trace me" \
    -H "traceparent: 00-$E2E_TRACE-00f067aa0ba902b7-01" "$BASE/mybucket/trace.txt")
check "traceresponse echoes the client's trace id" "0" \
    "$(echo "$TRACE_HDRS" | grep -qi "^traceresponse: 00-$E2E_TRACE-[0-9a-f]\{16\}-01"; echo $?)"
sleep 0.2
check "access log carries trace id and parent span" "0" \
    "$(grep -q "access .* PUT \"/mybucket/trace.txt\" 200 .* trace=$E2E_TRACE/[0-9a-f]* parent=00f067aa0ba902b7" "$WORK/server.log"; echo $?)"
# Only the pure cloudproxy backend forwards the PUT synchronously; under tiered the
# object lands in the hot tier and reaches the remote via background demotion,
# which carries no request context (by design: no trace headers)
if [[ "$BACKEND" == "cloudproxy" ]]; then
    GW_SPAN=$(grep "access .* PUT \"/mybucket/trace.txt\" 200 " "$WORK/server.log" | sed -n "s/.* trace=$E2E_TRACE\/\([0-9a-f]*\).*/\1/p" | head -1)
    check "remote instance logs the same trace id under the gateway's span" "0" \
        "$(grep -q "access .* PUT .* trace=$E2E_TRACE/[0-9a-f]* parent=$GW_SPAN" "$WORK/remote.log"; echo $?)"
fi
NOTRACE_HDRS=$(s3curl -D - -o /dev/null -I "$BASE/mybucket/trace.txt")
check "traceresponse present without a client trace" "0" \
    "$(echo "$NOTRACE_HDRS" | grep -qi "^traceresponse: 00-[0-9a-f]\{32\}-[0-9a-f]\{16\}-01"; echo $?)"
s3curl -o /dev/null -X DELETE "$BASE/mybucket/trace.txt"  # keep the later listing / DeleteBucket cases unchanged

# 5MB random file PUT/GET round trip
dd if=/dev/urandom of="$WORK/big.bin" bs=1M count=5 2>/dev/null
MD5=$(md5sum "$WORK/big.bin" | cut -d' ' -f1)
check "PutObject(5MB)" "200" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X PUT --data-binary "@$WORK/big.bin" \
       -H 'Content-Type: application/x-lights3-test' "$BASE/mybucket/dir/big.bin")"

s3curl -o "$WORK/big.out" "$BASE/mybucket/dir/big.bin"
check "GetObject content matches" "$MD5" "$(md5sum "$WORK/big.out" | cut -d' ' -f1)"

HEAD_OUT=$(s3curl -I "$BASE/mybucket/dir/big.bin")
check "HeadObject ETag=MD5" "\"$MD5\"" "$(echo "$HEAD_OUT" | tr -d '\r' | sed -n 's/^etag: //Ip')"
check "HeadObject Content-Type preserved" "application/x-lights3-test" \
    "$(echo "$HEAD_OUT" | tr -d '\r' | sed -n 's/^content-type: //Ip')"
check "HeadObject Content-Length" "5242880" \
    "$(echo "$HEAD_OUT" | tr -d '\r' | sed -n 's/^content-length: //Ip')"

# Range download
s3curl -o "$WORK/part.out" -r 1024-2047 "$BASE/mybucket/dir/big.bin"
dd if="$WORK/big.bin" of="$WORK/part.ref" bs=1 skip=1024 count=1024 2>/dev/null
check "Range download (1KiB@1KiB)" "$(md5sum "$WORK/part.ref" | cut -d' ' -f1)" \
    "$(md5sum "$WORK/part.out" | cut -d' ' -f1)"
check "Range response 206" "206" \
    "$(s3curl -o /dev/null -w '%{http_code}' -r 0-99 "$BASE/mybucket/dir/big.bin")"

# List
s3curl -o /dev/null -X PUT --data-binary 'x' "$BASE/mybucket/dir/small.txt"
s3curl -o /dev/null -X PUT --data-binary 'y' "$BASE/mybucket/top.txt"
LIST=$(s3curl "$BASE/mybucket?list-type=2&delimiter=%2F")
echo "$LIST" | grep -q '<Key>top.txt</Key>' && echo "$LIST" | grep -q '<Prefix>dir/</Prefix>'
check "ListObjectsV2 + delimiter" "0" "$?"
LIST2=$(s3curl "$BASE/mybucket?list-type=2&prefix=dir%2F")
check "ListObjectsV2 prefix count" "2" "$(echo "$LIST2" | grep -o '<Key>' | wc -l)"

# ListBuckets
check "ListBuckets contains bucket" "0" \
    "$(s3curl "$BASE/" | grep -q '<Name>mybucket</Name>'; echo $?)"

# GetBucketLocation (clients like the MinIO SDK depend on it)
check "GetBucketLocation" "0" \
    "$(s3curl "$BASE/mybucket?location" | grep -q 'LocationConstraint'; echo $?)"

# Conditional request: If-None-Match hit -> 304
ETAG=$(s3curl -I "$BASE/mybucket/top.txt" | tr -d '\r' | sed -n 's/^etag: //Ip')
check "If-None-Match 304" "304" \
    "$(s3curl -o /dev/null -w '%{http_code}' -H "If-None-Match: $ETAG" "$BASE/mybucket/top.txt")"

# CopyObject
check "CopyObject" "0" \
    "$(s3curl -X PUT -H 'x-amz-copy-source: /mybucket/top.txt' "$BASE/mybucket/copy.txt" \
       | grep -q 'CopyObjectResult'; echo $?)"
check "Copy content matches" "y" "$(s3curl "$BASE/mybucket/copy.txt")"

# Multipart: two 5MiB parts (real flow, docs/s3-protocol.md §8). 5MiB is AWS's minimum
# for non-final parts (docs/archive/gaps.md §5.7) -- 3MiB was used before, which real AWS would reject too
dd if=/dev/urandom of="$WORK/p1" bs=1M count=5 2>/dev/null
dd if=/dev/urandom of="$WORK/p2" bs=1M count=5 2>/dev/null
INIT=$(s3curl -X POST "$BASE/mybucket/mpu.bin?uploads")
UPLOAD_ID=$(echo "$INIT" | sed -n 's/.*<UploadId>\(.*\)<\/UploadId>.*/\1/p')
check "CreateMultipartUpload returns UploadId" "0" "$([[ -n "$UPLOAD_ID" ]]; echo $?)"
s3curl -o /dev/null -D "$WORK/h1" --data-binary "@$WORK/p1" -X PUT \
    "$BASE/mybucket/mpu.bin?partNumber=1&uploadId=$UPLOAD_ID"
s3curl -o /dev/null -D "$WORK/h2" --data-binary "@$WORK/p2" -X PUT \
    "$BASE/mybucket/mpu.bin?partNumber=2&uploadId=$UPLOAD_ID"
E1=$(tr -d '\r' < "$WORK/h1" | sed -n 's/^etag: //Ip')
E2=$(tr -d '\r' < "$WORK/h2" | sed -n 's/^etag: //Ip')
check "ListParts has two parts" "2" \
    "$(s3curl "$BASE/mybucket/mpu.bin?uploadId=$UPLOAD_ID" | grep -o '<PartNumber>' | wc -l)"
COMPLETE_XML="<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>$E1</ETag></Part><Part><PartNumber>2</PartNumber><ETag>$E2</ETag></Part></CompleteMultipartUpload>"
DONE=$(s3curl -X POST --data-binary "$COMPLETE_XML" "$BASE/mybucket/mpu.bin?uploadId=$UPLOAD_ID")
check "CompleteMultipartUpload composite ETag(-2)" "0" \
    "$(echo "$DONE" | grep -q -- '-2&quot;</ETag>'; echo $?)"
s3curl -o "$WORK/mpu.out" "$BASE/mybucket/mpu.bin"
check "Multipart download content matches" \
    "$(cat "$WORK/p1" "$WORK/p2" | md5sum | cut -d' ' -f1)" \
    "$(md5sum "$WORK/mpu.out" | cut -d' ' -f1)"

# UploadPartCopy (docs/s3-protocol.md §1): full-object copy + range copy, then assemble
INIT2=$(s3curl -X POST "$BASE/mybucket/upc.bin?uploads")
UPC_ID=$(echo "$INIT2" | sed -n 's/.*<UploadId>\(.*\)<\/UploadId>.*/\1/p')
UPC1=$(s3curl -X PUT -H 'x-amz-copy-source: /mybucket/mpu.bin' \
    "$BASE/mybucket/upc.bin?partNumber=1&uploadId=$UPC_ID")
check "UploadPartCopy full object" "0" "$(echo "$UPC1" | grep -q 'CopyPartResult'; echo $?)"
UPC2=$(s3curl -X PUT -H 'x-amz-copy-source: /mybucket/mpu.bin' \
    -H 'x-amz-copy-source-range: bytes=0-1048575' \
    "$BASE/mybucket/upc.bin?partNumber=2&uploadId=$UPC_ID")
check "UploadPartCopy range" "0" "$(echo "$UPC2" | grep -q 'CopyPartResult'; echo $?)"
UE1=$(echo "$UPC1" | sed -n 's/.*<ETag>\(.*\)<\/ETag>.*/\1/p')
UE2=$(echo "$UPC2" | sed -n 's/.*<ETag>\(.*\)<\/ETag>.*/\1/p')
UPC_XML="<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>$UE1</ETag></Part><Part><PartNumber>2</PartNumber><ETag>$UE2</ETag></Part></CompleteMultipartUpload>"
s3curl -o /dev/null -X POST --data-binary "$UPC_XML" "$BASE/mybucket/upc.bin?uploadId=$UPC_ID"
s3curl -o "$WORK/upc.out" "$BASE/mybucket/upc.bin"
check "UploadPartCopy assembled content matches" \
    "$(cat "$WORK/p1" "$WORK/p2" <(head -c 1048576 "$WORK/p1") | md5sum | cut -d' ' -f1)" \
    "$(md5sum "$WORK/upc.out" | cut -d' ' -f1)"
check "UploadPartCopy out-of-range rejected" "400" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X PUT \
       -H 'x-amz-copy-source: /mybucket/mpu.bin' \
       -H 'x-amz-copy-source-range: bytes=0-99999999' \
       "$BASE/mybucket/upc.bin?partNumber=3&uploadId=$UPC_ID")"
s3curl -o /dev/null -X DELETE "$BASE/mybucket/upc.bin"

# Non-final part smaller than 5MiB -> EntityTooSmall (docs/archive/gaps.md §5.7)
INIT3=$(s3curl -X POST "$BASE/mybucket/small.bin?uploads")
SM_ID=$(echo "$INIT3" | sed -n 's/.*<UploadId>\(.*\)<\/UploadId>.*/\1/p')
s3curl -o /dev/null -D "$WORK/hs1" --data-binary 'tiny-part-one' -X PUT \
    "$BASE/mybucket/small.bin?partNumber=1&uploadId=$SM_ID"
s3curl -o /dev/null -D "$WORK/hs2" --data-binary 'tiny-part-two' -X PUT \
    "$BASE/mybucket/small.bin?partNumber=2&uploadId=$SM_ID"
S1=$(tr -d '\r' < "$WORK/hs1" | sed -n 's/^etag: //Ip')
S2=$(tr -d '\r' < "$WORK/hs2" | sed -n 's/^etag: //Ip')
SM_XML="<CompleteMultipartUpload><Part><PartNumber>1</PartNumber><ETag>$S1</ETag></Part><Part><PartNumber>2</PartNumber><ETag>$S2</ETag></Part></CompleteMultipartUpload>"
check "undersized non-final part EntityTooSmall" "0" \
    "$(s3curl -X POST --data-binary "$SM_XML" "$BASE/mybucket/small.bin?uploadId=$SM_ID" \
        | grep -q 'EntityTooSmall'; echo $?)"
# Out-of-order parts have a dedicated error code
SM_REV="<CompleteMultipartUpload><Part><PartNumber>2</PartNumber><ETag>$S2</ETag></Part><Part><PartNumber>1</PartNumber><ETag>$S1</ETag></Part></CompleteMultipartUpload>"
check "out-of-order InvalidPartOrder" "0" \
    "$(s3curl -X POST --data-binary "$SM_REV" "$BASE/mybucket/small.bin?uploadId=$SM_ID" \
        | grep -q 'InvalidPartOrder'; echo $?)"
s3curl -o /dev/null -X DELETE "$BASE/mybucket/small.bin?uploadId=$SM_ID"

# DeleteObjects batch deletion (AWS requires Content-MD5, docs/archive/gaps.md §5.6)
DEL_XML='<Delete><Object><Key>copy.txt</Key></Object><Object><Key>mpu.bin</Key></Object></Delete>'
DEL_MD5=$(printf '%s' "$DEL_XML" | openssl dgst -md5 -binary | openssl base64)
check "DeleteObjects missing integrity header 400" "400" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X POST --data-binary "$DEL_XML" "$BASE/mybucket?delete")"
check "DeleteObjects digest mismatch 400" "400" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X POST -H "Content-MD5: $DEL_MD5" \
        --data-binary "<Delete><Object><Key>copy.txt</Key></Object></Delete>" \
        "$BASE/mybucket?delete")"
DEL_OUT=$(s3curl -X POST -H "Content-MD5: $DEL_MD5" --data-binary "$DEL_XML" "$BASE/mybucket?delete")
check "DeleteObjects batch" "0" "$(echo "$DEL_OUT" | grep -q '<Deleted><Key>copy.txt</Key>'; echo $?)"
check "batch delete took effect" "404" "$(s3curl -o /dev/null -w '%{http_code}' "$BASE/mybucket/mpu.bin")"

# PutObject's Content-MD5: mismatch -> BadDigest, malformed -> InvalidDigest
PUT_MD5=$(printf 'md5-checked' | openssl dgst -md5 -binary | openssl base64)
check "PutObject Content-MD5 match" "200" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X PUT -H "Content-MD5: $PUT_MD5" \
        --data-binary 'md5-checked' "$BASE/mybucket/md5ok.bin")"
check "PutObject Content-MD5 mismatch" "400" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X PUT -H "Content-MD5: $PUT_MD5" \
        --data-binary 'tampered!!!' "$BASE/mybucket/md5bad.bin")"
check "PutObject Content-MD5 malformed" "400" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X PUT -H 'Content-MD5: not-base64' \
        --data-binary 'x' "$BASE/mybucket/md5junk.bin")"
# The two objects with mismatched/malformed digests must not be persisted; clean up the
# matching one, otherwise the final DeleteBucket returns 409
check "mismatched-digest object not persisted" "404" \
    "$(s3curl -o /dev/null -w '%{http_code}' "$BASE/mybucket/md5bad.bin")"
s3curl -o /dev/null -X DELETE "$BASE/mybucket/md5ok.bin"

# Observability endpoints
check "metrics output" "0" \
    "$(curl -s "$BASE/-/metrics" | grep -q 'lights3_requests_total'; echo $?)"
check "readyz" "200" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/-/readyz")"

# Unsupported subresource -> 501
check "?acl explicit 501" "501" "$(s3curl -o /dev/null -w '%{http_code}' "$BASE/mybucket?acl")"

# Deletion and 404
check "DeleteObject" "204" "$(s3curl -o /dev/null -w '%{http_code}' -X DELETE "$BASE/mybucket/dir/big.bin")"
check "GET after delete 404" "404" "$(s3curl -o /dev/null -w '%{http_code}' "$BASE/mybucket/dir/big.bin")"
GET404=$(s3curl "$BASE/mybucket/dir/big.bin")
check "404 error XML" "0" "$(echo "$GET404" | grep -q '<Code>NoSuchKey</Code>'; echo $?)"

s3curl -o /dev/null -X DELETE "$BASE/mybucket/dir/small.txt"
s3curl -o /dev/null -X DELETE "$BASE/mybucket/top.txt"
check "DeleteBucket" "204" "$(s3curl -o /dev/null -w '%{http_code}' -X DELETE "$BASE/mybucket")"

# ---------- Dynamic credential management (docs/credential-management.md) ----------
json_field() {  # json_field <key> -- extract a string field from the indented JSON on stdin
    sed -n "s/.*\"$1\": \"\([^\"]*\)\".*/\1/p" | head -1
}

CRED_OUT=$(s3curl -X POST "$BASE/-/admin/credentials?comment=e2e")
DYN_AK=$(echo "$CRED_OUT" | json_field access_key)
DYN_SK=$(echo "$CRED_OUT" | json_field secret_key)
check "create dynamic credential" "0" "$([[ -n "$DYN_AK" && -n "$DYN_SK" ]]; echo $?)"

dyncurl() {  # sign with the dynamic credential
    curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$DYN_AK:$DYN_SK" "$@"
}
check "dynamic credential CreateBucket" "200" \
    "$(dyncurl -o /dev/null -w '%{http_code}' -X PUT "$BASE/credbkt")"
check "dynamic credential PutObject" "200" \
    "$(dyncurl -o /dev/null -w '%{http_code}' -X PUT --data-binary 'cred-data' "$BASE/credbkt/k")"
check "dynamic credential GetObject" "cred-data" "$(dyncurl "$BASE/credbkt/k")"

LIST_CREDS=$(s3curl "$BASE/-/admin/credentials")
check "credential list contains new AK" "0" "$(echo "$LIST_CREDS" | grep -q "$DYN_AK"; echo $?)"
check "credential list masks SK" "1" "$(echo "$LIST_CREDS" | grep -q "$DYN_SK"; echo $?)"
check "show-secret returns SK" "$DYN_SK" \
    "$(s3curl "$BASE/-/admin/credentials/$DYN_AK?show-secret=true" | json_field secret_key)"
check "dynamic credential admin call rejected" "403" \
    "$(dyncurl -o /dev/null -w '%{http_code}' -X POST "$BASE/-/admin/credentials")"

# The second credential is only for the post-restart persistence check
CRED2_OUT=$(s3curl -X POST "$BASE/-/admin/credentials?comment=survivor")
AK2=$(echo "$CRED2_OUT" | json_field access_key)
SK2=$(echo "$CRED2_OUT" | json_field secret_key)

check "revoke dynamic credential" "204" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X DELETE "$BASE/-/admin/credentials/$DYN_AK")"
check "data plane rejected after revocation" "403" \
    "$(dyncurl -o /dev/null -w '%{http_code}' "$BASE/credbkt/k")"
check ".sys unreachable by users" "400" \
    "$(s3curl -o /dev/null -w '%{http_code}' "$BASE/.sys/credentials/$AK2")"
check "ListBuckets excludes .sys" "1" "$(s3curl "$BASE/" | grep -qF '.sys'; echo $?)"

# ---------- Phase 2: file credentials + per-credential policy (credential-management.md §10) ----------
filecurl() {  # sign with the credential from credentials_file
    curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$FILE_AK:$FILE_SK" "$@"
}
check "file credential works on data plane" "cred-data" "$(filecurl "$BASE/credbkt/k")"
check "file credential admin call rejected" "403" \
    "$(filecurl -o /dev/null -w '%{http_code}' -X POST "$BASE/-/admin/credentials")"
check "file credential not revocable via API" "405" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X DELETE "$BASE/-/admin/credentials/$FILE_AK")"

POL_OUT=$(s3curl -X POST \
    --data-binary '{"comment":"scoped","policy":{"buckets":["credbkt"],"readonly":true}}' \
    "$BASE/-/admin/credentials")
POL_AK=$(echo "$POL_OUT" | json_field access_key)
POL_SK=$(echo "$POL_OUT" | json_field secret_key)
check "create credential with policy" "0" "$([[ -n "$POL_AK" && -n "$POL_SK" ]]; echo $?)"
polcurl() {
    curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$POL_AK:$POL_SK" "$@"
}
check "policy allows read inside whitelist" "cred-data" "$(polcurl "$BASE/credbkt/k")"
check "policy readonly write rejected" "403" \
    "$(polcurl -o /dev/null -w '%{http_code}' -X PUT --data-binary 'x' "$BASE/credbkt/blocked")"
check "policy rejects outside whitelist" "403" \
    "$(polcurl -o /dev/null -w '%{http_code}' "$BASE/otherbkt/x")"
check "POST unknown field rejected" "400" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X POST --data-binary '{"bogus":1}' \
       "$BASE/-/admin/credentials")"

# Action/prefix granularity of policies and ListBuckets filtering (docs/archive/gaps.md §5.10)
# First create a real bucket outside the whitelist as root: otherwise no real bucket
# exists outside the whitelist and the assertion would pass vacuously even if filtering
# were entirely broken (empty assertion)
s3curl -o /dev/null -X PUT "$BASE/otherbkt"
POL_LIST=$(polcurl "$BASE/")
check "ListBuckets filtered by policy" "0" \
    "$(echo "$POL_LIST" | grep -q '<Name>otherbkt</Name>' && echo 1 || echo 0)"
check "ListBuckets shows whitelisted bucket" "0" \
    "$(echo "$POL_LIST" | grep -q '<Name>credbkt</Name>' && echo 0 || echo 1)"
BK_OUT=$(s3curl -X POST \
    --data-binary '{"policy":{"buckets":["credbkt"],"actions":["read","write"],"prefixes":["keep/"]}}' \
    "$BASE/-/admin/credentials")
BK_AK=$(echo "$BK_OUT" | json_field access_key)
BK_SK=$(echo "$BK_OUT" | json_field secret_key)
bkcurl() {
    curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$BK_AK:$BK_SK" "$@"
}
check "action allows write" "200" \
    "$(bkcurl -o /dev/null -w '%{http_code}' -X PUT --data-binary 'v' "$BASE/credbkt/keep/a")"
check "delete rejected when action lacks delete" "403" \
    "$(bkcurl -o /dev/null -w '%{http_code}' -X DELETE "$BASE/credbkt/keep/a")"
check "write outside prefix rejected" "403" \
    "$(bkcurl -o /dev/null -w '%{http_code}' -X PUT --data-binary 'v' "$BASE/credbkt/other/a")"

# ListObjects filtered by policy prefix: real objects outside the whitelist must not leak through a listing without a prefix
s3curl -o /dev/null -X PUT --data-binary 'v' "$BASE/credbkt/other/x"
PLIST=$(bkcurl "$BASE/credbkt")
check "ListObjects excludes keys outside prefix" "0" \
    "$(echo "$PLIST" | grep -q '<Key>other/x</Key>' && echo 1 || echo 0)"
check "ListObjects includes keys inside prefix" "0" \
    "$(echo "$PLIST" | grep -q '<Key>keep/a</Key>' && echo 0 || echo 1)"

# DeleteObjects applies the policy per key: the batch-delete API must not bypass the prefix whitelist (same judgment as single delete)
DELP_OUT=$(s3curl -X POST \
    --data-binary '{"policy":{"buckets":["credbkt"],"actions":["read","delete"],"prefixes":["keep/"]}}' \
    "$BASE/-/admin/credentials")
DELP_AK=$(echo "$DELP_OUT" | json_field access_key)
DELP_SK=$(echo "$DELP_OUT" | json_field secret_key)
delpcurl() {
    curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$DELP_AK:$DELP_SK" "$@"
}
BATCH_XML='<Delete><Object><Key>other/x</Key></Object><Object><Key>keep/a</Key></Object></Delete>'
BATCH_MD5=$(printf '%s' "$BATCH_XML" | openssl dgst -md5 -binary | openssl base64)
BATCH_OUT=$(delpcurl -X POST -H "Content-MD5: $BATCH_MD5" --data-binary "$BATCH_XML" \
    "$BASE/credbkt?delete")
check "batch delete returns AccessDenied for key outside prefix" "0" \
    "$(echo "$BATCH_OUT" | grep -q '<Error><Key>other/x</Key><Code>AccessDenied</Code>' && echo 0 || echo 1)"
check "batch delete removes key inside prefix" "0" \
    "$(echo "$BATCH_OUT" | grep -q '<Deleted><Key>keep/a</Key>' && echo 0 || echo 1)"
check "object outside prefix not batch-deleted" "200" \
    "$(s3curl -o /dev/null -w '%{http_code}' "$BASE/credbkt/other/x")"
s3curl -o /dev/null -X DELETE "$BASE/credbkt/other/x"
s3curl -o /dev/null -X DELETE "$BASE/otherbkt"

check "static credential SK not returned via admin" "0" \
    "$(s3curl "$BASE/-/admin/credentials/$AK?show-secret=true" | grep -q '"secret_key"' && echo 1 || echo 0)"
s3curl -o /dev/null -X DELETE "$BASE/credbkt/keep/a"

# ---------- roadmap §6.1: static website hosting e2e (docs/static-website.md) ----------
# The static entry e2esite comes from config.yaml; the anonymous plane is the
# security-sensitive face: reads of listed buckets only, never listing / writes /
# other buckets; the index/error documents and the object-level 301 all through
# real HTTP with no signature material
check "website: owner creates bucket" "200" "$(s3curl -o /dev/null -w '%{http_code}' -X PUT "$BASE/e2esite")"
s3curl -o /dev/null -X PUT --data-binary '<h1>home</h1>' -H 'Content-Type: text/html' "$BASE/e2esite/index.html"
s3curl -o /dev/null -X PUT --data-binary '<h1>docs</h1>' -H 'Content-Type: text/html' "$BASE/e2esite/docs/index.html"
s3curl -o /dev/null -X PUT --data-binary '<h1>custom 404</h1>' -H 'Content-Type: text/html' "$BASE/e2esite/error.html"
s3curl -o /dev/null -X PUT --data-binary 'moved' -H 'x-amz-website-redirect-location: /index.html' "$BASE/e2esite/old"
check "website: anonymous object read" "<h1>home</h1>" "$(curl -s "$BASE/e2esite/index.html")"
check "website: anonymous read keeps Content-Type" "text/html" "$(curl -s -o /dev/null -w '%{content_type}' "$BASE/e2esite/index.html")"
check "website: bucket root serves the index document" "<h1>home</h1>" "$(curl -s "$BASE/e2esite/")"
check "website: directory key serves its index" "<h1>docs</h1>" "$(curl -s "$BASE/e2esite/docs/")"
check "website: GET /prefix without slash 302s to /prefix/" "302" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/e2esite/docs")"
check "website: missing key answers the error document with 404" "404" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/e2esite/nope")"
check "website: error document body served" "<h1>custom 404</h1>" "$(curl -s "$BASE/e2esite/nope")"
check "website: x-amz-website-redirect-location -> 301" "301" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/e2esite/old")"
check "website: 301 Location" "/index.html" "$(curl -s -o /dev/null -w '%{redirect_url}' "$BASE/e2esite/old" | sed "s#^$BASE##")"
check "website: anonymous listing refused (no ListBucketResult)" "1" \
    "$(curl -s "$BASE/e2esite?list-type=2" | grep -q '<ListBucketResult'; echo $?)"
# A query flag on the anonymous plane is refused by the object route's query
# allowlist (501) before any policy decision -- refused either way, never served
check "website: anonymous listing refused (non-2xx)" "1" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/e2esite?list-type=2" | grep -q '^2'; echo $?)"
check "website: anonymous write refused" "403" "$(curl -s -o /dev/null -w '%{http_code}' -X PUT --data-binary x "$BASE/e2esite/evil")"
check "website: anonymous delete refused" "403" "$(curl -s -o /dev/null -w '%{http_code}' -X DELETE "$BASE/e2esite/index.html")"
check "website: anonymous read of a non-website bucket refused" "403" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/mybucket/dir/big.bin")"
check "website: ListMultipartUploads on the anonymous plane refused (non-2xx)" "1" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/e2esite?uploads" | grep -q '^2'; echo $?)"
check "website: HEAD anonymous" "200" "$(curl -s -o /dev/null -w '%{http_code}' -I "$BASE/e2esite/index.html")"
check "website: static entry is immutable via the API (405)" "405" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X DELETE "$BASE/e2esite?website")"
# Dynamic ?website entry (root only) through the API, torn down again: the anonymous
# plane must close the moment the configuration is gone
s3curl -o /dev/null -X PUT "$BASE/dynsite"
s3curl -o /dev/null -X PUT --data-binary 'dyn' "$BASE/dynsite/index.html"
check "website: anonymous read closed before ?website" "403" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/dynsite/index.html")"
check "website: PutBucketWebsite (root)" "200" "$(s3curl -o /dev/null -w '%{http_code}' -X PUT --data-binary '<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>' "$BASE/dynsite?website")"
check "website: PutBucketWebsite denied for non-root" "403" \
    "$(curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK2:$SK2" -o /dev/null -w '%{http_code}' -X PUT --data-binary '<WebsiteConfiguration><IndexDocument><Suffix>index.html</Suffix></IndexDocument></WebsiteConfiguration>' "$BASE/dynsite?website")"
check "website: anonymous read open after ?website" "dyn" "$(curl -s "$BASE/dynsite/")"
check "website: GetBucketWebsite" "0" "$(s3curl "$BASE/dynsite?website" | grep -q '<Suffix>index.html</Suffix>'; echo $?)"
check "website: DeleteBucketWebsite" "204" "$(s3curl -o /dev/null -w '%{http_code}' -X DELETE "$BASE/dynsite?website")"
check "website: anonymous read closed again" "403" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/dynsite/index.html")"
check "website: metrics count the anonymous plane" "0" \
    "$(curl -s "$BASE/-/metrics" | grep -q 'lights3_website_events_total{event="anon_read"} [1-9]'; echo $?)"
for k in index.html docs/index.html error.html old; do s3curl -o /dev/null -X DELETE "$BASE/e2esite/$k"; done
s3curl -o /dev/null -X DELETE "$BASE/e2esite"
s3curl -o /dev/null -X DELETE "$BASE/dynsite/index.html"
s3curl -o /dev/null -X DELETE "$BASE/dynsite"

# ---------- roadmap §6.1: s3adm cross-validation (self-signed client vs. the server's verifier) ----------
# curl signs with libcurl's SigV4, s3adm with its own implementation: the same
# flows through both catch a drift in either signer
S3ADM="$(dirname "$BIN")/s3adm"
if [[ -x "$S3ADM" ]]; then
    adm() { LIGHTS3_ADMIN_AK=$AK LIGHTS3_ADMIN_SK=$SK "$S3ADM" "$@" --endpoint="$BASE" --region="$REGION"; }
    ADM_CRED=$(adm cred create --comment=s3adm-e2e 2>&1)
    ADM_AK=$(echo "$ADM_CRED" | json_field access_key)
    check "s3adm cred create returns an AK" "1" "$([[ ${#ADM_AK} -ge 16 ]] && echo 1 || echo 0)"
    check "s3adm cred list shows it" "0" "$(adm cred list | grep -q "$ADM_AK"; echo $?)"
    ADM_SK=$(adm cred get "$ADM_AK" --show-secret | json_field secret_key)
    check "s3adm cred get --show-secret returns the secret" "1" "$([[ -n "$ADM_SK" ]] && echo 1 || echo 0)"
    check "curl signs with the credential s3adm minted (ListBuckets)" "200" \
        "$(curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$ADM_AK:$ADM_SK" -o /dev/null -w '%{http_code}' "$BASE/")"
    check "s3adm cred delete" "0" "$(adm cred delete "$ADM_AK" >/dev/null 2>&1; echo $?)"
    check "curl rejected with the credential s3adm revoked" "403" \
        "$(curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$ADM_AK:$ADM_SK" -o /dev/null -w '%{http_code}' "$BASE/")"
    s3curl -o /dev/null -X PUT "$BASE/admsite"
    check "s3adm website set" "0" "$(adm website set admsite --index-suffix=index.html --error-key=404.html >/dev/null 2>&1; echo $?)"
    check "curl reads the configuration s3adm wrote" "0" "$(s3curl "$BASE/admsite?website" | grep -q '<Key>404.html</Key>'; echo $?)"
    check "s3adm website get" "0" "$(adm website get admsite | grep -q '<Suffix>index.html</Suffix>'; echo $?)"
    check "s3adm website delete" "0" "$(adm website delete admsite >/dev/null 2>&1; echo $?)"
    check "s3adm website get after delete exits 1" "1" "$(adm website get admsite >/dev/null 2>&1; echo $?)"
    s3curl -o /dev/null -X DELETE "$BASE/admsite"
    BENCH_OUT=$(adm bench put --bucket=admbench --concurrency=2 --duration-sec=1 --objects=8 --size=4K --keep 2>&1)
    check "s3adm bench put runs error-free" "0" "$(echo "$BENCH_OUT" | grep -q '^ops [0-9]* ok, 0 err'; echo $?)"
    # backlog-sequence ③: the offline scrub through the admin plane (root only; the
    # localfs-family and duostore backends have one, memory/cloudproxy/tiered do not)
    FSCK_KIND=""
    case "$BACKEND" in
        localfs|xlocalfs|duostore|duostore-uring|duostore-redis|duostore-sqlite|duostore-rados|duostore-tikv) FSCK_KIND=yes ;;
    esac
    if [[ -n "$FSCK_KIND" ]]; then
        check "admin fsck: unsigned refused" "403" "$(curl -s -o /dev/null -w '%{http_code}' -X POST "$BASE/-/admin/fsck/tierdata")"
        check "admin fsck: unknown backend 404" "404" "$(s3curl -o /dev/null -w '%{http_code}' -X POST "$BASE/-/admin/fsck/nope")"
        check "admin fsck: status before any job" "0" "$(s3curl "$BASE/-/admin/fsck/tierdata" | grep -q '"running": false'; echo $?)"
        OFF_OUT=$(adm fsck --offline tierdata 2>&1); OFF_RC=$?
        check "s3adm fsck --offline completes clean" "0" "$OFF_RC"
        check "s3adm fsck --offline prints the outcome document" "0" "$(echo "$OFF_OUT" | grep -q '"findings": 0'; echo $?)"
        check "admin fsck: status shows the finished job" "0" "$(s3curl "$BASE/-/admin/fsck/tierdata" | grep -q '"job_id": 1'; echo $?)"
        check "s3adm fsck --status" "0" "$(adm fsck --status tierdata 2>&1 | grep -q '"running": false'; echo $?)"
        # A throttled round keeps the backend busy: a second start is refused with 409
        check "admin fsck: throttled start accepted" "202" "$(s3curl -o /dev/null -w '%{http_code}' -X POST "$BASE/-/admin/fsck/tierdata?max_mbps=1")"
        check "admin fsck: concurrent start refused (409 ScrubInProgress)" "409" "$(s3curl -o /dev/null -w '%{http_code}' -X POST "$BASE/-/admin/fsck/tierdata")"
        for _ in $(seq 1 600); do s3curl "$BASE/-/admin/fsck/tierdata" | grep -q '"running": false' && break; sleep 0.1; done
        check "admin fsck: throttled round finished" "0" "$(s3curl "$BASE/-/admin/fsck/tierdata" | grep -q '"job_id": 2'; echo $?)"
        if [[ "$BACKEND" == "localfs" || "$BACKEND" == "xlocalfs" ]]; then
            # Flip a byte inside a stored object's data file: the next round must report
            # exactly one ETag mismatch and s3adm must exit 1
            head -c 200000 /dev/urandom > "$WORK/fsck-victim.bin"
            s3curl -o /dev/null -X PUT "$BASE/fsckbkt"
            s3curl -o /dev/null -X PUT --data-binary "@$WORK/fsck-victim.bin" "$BASE/fsckbkt/victim"
            corrupt=$(find "$WORK/data/fsckbkt" -type f -size +100k ! -name '.*' | head -1)
            check "admin fsck: victim object stored on disk" "0" "$([[ -n "$corrupt" ]]; echo $?)"
            if [[ -n "$corrupt" ]]; then
                printf 'Z' | dd of="$corrupt" bs=1 seek=64 conv=notrunc 2>/dev/null
                CORR_OUT=$(adm fsck --offline tierdata 2>&1); CORR_RC=$?
                check "s3adm fsck --offline exits 1 on a corrupted object" "1" "$CORR_RC"
                check "admin fsck: the corruption is one etag mismatch" "0" "$(echo "$CORR_OUT" | grep -q '"etag_mismatches": 1'; echo $?)"
                check "admin fsck: findings counted" "0" "$(echo "$CORR_OUT" | grep -q '"findings": 1'; echo $?)"
            fi
            s3curl -o /dev/null -X DELETE "$BASE/fsckbkt/victim"
            s3curl -o /dev/null -X DELETE "$BASE/fsckbkt"
        fi
    fi
    FSCK_OUT=$(adm fsck admbench 2>&1); FSCK_RC=$?
    check "s3adm fsck verifies the bench objects" "0" "$FSCK_RC"
    [[ $FSCK_RC -ne 0 ]] && echo "$FSCK_OUT"
    check "s3adm fsck reports zero mismatches" "0" "$(echo "$FSCK_OUT" | grep -q ' 0 mismatches, 0 errors'; echo $?)"
    check "s3adm bench get runs error-free" "0" "$(adm bench get --bucket=admbench --concurrency=2 --duration-sec=1 --objects=8 --size=4K 2>&1 | grep -q '^ops [0-9]* ok, 0 err'; echo $?)"
    # roadmap §6.2: machine-readable bench summary (stdout is exactly one JSON object)
    BENCH_JSON=$(adm bench put --bucket=admbench --concurrency=2 --duration-sec=1 --objects=8 --size=4K --output=json 2>/dev/null)
    check "s3adm bench --output=json is a JSON object with the summary fields" "0" \
        "$(echo "$BENCH_JSON" | python3 -c 'import json,sys; j=json.load(sys.stdin); assert j["mode"]=="put" and j["errors"]==0 and j["ops"]>0 and j["ops_per_s"]>0 and "p99" in j["latency_ms"]' 2>/dev/null; echo $?)"
    # roadmap §6.2: object layout introspection through the admin endpoint
    s3curl -o /dev/null -X PUT --data-binary 'layout me' "$BASE/admbench/dir/layout.txt"
    INSPECT=$(adm object inspect admbench dir/layout.txt 2>&1)
    if [[ "$BACKEND" == "cloudproxy" ]]; then
        # cloudproxy exposes no internal layout by design: the routing is still reported
        check "s3adm object inspect reports the routed backend and no layout (cloudproxy)" "0" \
            "$(echo "$INSPECT" | python3 -c 'import json,sys; j=json.load(sys.stdin); assert j["bucket"]=="admbench" and j["key"]=="dir/layout.txt" and j["backend"]=="tierdata" and j["layout"] is None and j["note"]' 2>/dev/null; echo $?)"
        check "s3adm object inspect --output=text says (none)" "0" "$(adm object inspect admbench dir/layout.txt --output=text | grep -q '^layout   (none)'; echo $?)"
    else
        check "s3adm object inspect returns the routed backend and a layout" "0" \
            "$(echo "$INSPECT" | python3 -c 'import json,sys; j=json.load(sys.stdin); assert j["bucket"]=="admbench" and j["key"]=="dir/layout.txt" and j["backend"]=="tierdata" and j["layout"] is not None and j["layout"]["engine"] and isinstance(j["layout"]["extents"], list)' 2>/dev/null; echo $?)"
        check "s3adm object inspect --output=text prints the engine" "0" "$(adm object inspect admbench dir/layout.txt --output=text | grep -q '^engine   '; echo $?)"
    fi
    check "s3adm object inspect on a missing key fails" "1" "$(adm object inspect admbench nope >/dev/null 2>&1; echo $?)"
    check "object inspect denied for non-root" "403" \
        "$(curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK2:$SK2" -o /dev/null -w '%{http_code}' "$BASE/-/admin/objects/admbench/dir/layout.txt")"
    s3curl -o /dev/null -X DELETE "$BASE/admbench/dir/layout.txt"
    # roadmap §6.2: zombie multipart uploads listed and aborted through s3adm
    s3curl -o /dev/null -X POST "$BASE/admbench/zombie-1?uploads"
    s3curl -o /dev/null -X POST "$BASE/admbench/zombie-2?uploads"
    check "s3adm mpu list shows both uploads" "2" "$(adm mpu list admbench | grep -c 'zombie-')"
    check "s3adm mpu list --output=json" "2" "$(adm mpu list admbench --output=json | python3 -c 'import json,sys; print(len(json.load(sys.stdin)["uploads"]))')"
    check "s3adm mpu list --older-than filters out fresh uploads" "0" "$(adm mpu list admbench --older-than=1h | grep -c 'zombie-')"
    Z1=$(adm mpu list admbench --output=json | python3 -c 'import json,sys; u=[x for x in json.load(sys.stdin)["uploads"] if x["key"]=="zombie-1"][0]; print(u["upload_id"])')
    check "s3adm mpu abort one upload" "0" "$(adm mpu abort admbench zombie-1 "$Z1" >/dev/null 2>&1; echo $?)"
    check "s3adm mpu abort --all clears the rest" "1 of 1 upload(s) aborted" "$(adm mpu abort admbench --all | tail -1)"
    check "s3adm mpu list empty after abort" "0 upload(s)" "$(adm mpu list admbench | tail -1)"
    s3curl -o /dev/null -X DELETE "$BASE/admbench"
fi

# ---------- roadmap §3.9: usage accounting / quotas / tenants (docs/multi-tenancy.md) ----------
json_num() {  # json_num <key> -- extract a numeric field from the indented JSON on stdin
    sed -n "s/.*\"$1\": \([0-9]*\).*/\1/p" | head -1
}
S3ADM="$(dirname "$BIN")/s3adm"
s3curl -o /dev/null -X PUT "$BASE/qbkt"
s3curl -o /dev/null -X PUT --data-binary '0123456789' "$BASE/qbkt/ten"
USAGE_OUT=$(s3curl "$BASE/-/admin/usage/qbkt")
check "usage bytes after PUT" "10" "$(echo "$USAGE_OUT" | json_num bytes)"
check "usage objects after PUT" "1" "$(echo "$USAGE_OUT" | json_num objects)"
check "usage rescan agrees with counters" "10" \
    "$(s3curl -X POST "$BASE/-/admin/usage/qbkt/rescan" | json_num bytes)"
check "usage API denied for non-root" "403" \
    "$(curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK2:$SK2" -o /dev/null -w '%{http_code}' "$BASE/-/admin/usage")"
QUOTA_XML='<QuotaConfiguration><MaxBytes>15</MaxBytes><MaxObjects>0</MaxObjects></QuotaConfiguration>'
check "GET ?quota before set is 404" "404" "$(s3curl -o /dev/null -w '%{http_code}' "$BASE/qbkt?quota")"
check "PUT ?quota" "200" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X PUT --data-binary "$QUOTA_XML" "$BASE/qbkt?quota")"
check "GET ?quota round-trips" "0" "$(s3curl "$BASE/qbkt?quota" | grep -q '<MaxBytes>15</MaxBytes>'; echo $?)"
check "PUT over quota rejected" "403" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X PUT --data-binary '0123456789' "$BASE/qbkt/second")"
check "QuotaExceeded error code" "0" \
    "$(s3curl -X PUT --data-binary '0123456789' "$BASE/qbkt/second" | grep -q 'QuotaExceeded'; echo $?)"
check "PUT within quota" "200" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X PUT --data-binary '01234' "$BASE/qbkt/second")"
check "DELETE ?quota" "204" "$(s3curl -o /dev/null -w '%{http_code}' -X DELETE "$BASE/qbkt?quota")"
check "quota gone after delete" "404" "$(s3curl -o /dev/null -w '%{http_code}' "$BASE/qbkt?quota")"

TENANT_OUT=$(s3curl -X POST -H 'Content-Type: application/json' \
    --data-binary '{"id":"acme","display_name":"ACME","quota":{"max_buckets":1}}' "$BASE/-/admin/tenants")
check "create tenant" "acme" "$(echo "$TENANT_OUT" | json_field id)"
TCRED_OUT=$(s3curl -X POST -H 'Content-Type: application/json' --data-binary '{"tenant":"acme"}' \
    "$BASE/-/admin/credentials")
T_AK=$(echo "$TCRED_OUT" | json_field access_key)
T_SK=$(echo "$TCRED_OUT" | json_field secret_key)
check "tenant credential carries tenant" "acme" "$(echo "$TCRED_OUT" | json_field tenant)"
tcurl() {  # sign with the tenant credential
    curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$T_AK:$T_SK" "$@"
}
check "tenant creates its own bucket" "200" "$(tcurl -o /dev/null -w '%{http_code}' -X PUT "$BASE/acme-data")"
check "tenant bucket limit enforced" "403" "$(tcurl -o /dev/null -w '%{http_code}' -X PUT "$BASE/acme-two")"
check "tenant cannot read a foreign bucket" "403" "$(tcurl -o /dev/null -w '%{http_code}' "$BASE/qbkt/ten")"
check "tenant PutObject in own bucket" "200" \
    "$(tcurl -o /dev/null -w '%{http_code}' -X PUT --data-binary 'tenant-data' "$BASE/acme-data/k")"
TLIST=$(tcurl "$BASE/")
check "tenant ListBuckets hides foreign buckets" "1" "$(echo "$TLIST" | grep -qF '<Name>qbkt</Name>'; echo $?)"
check "tenant ListBuckets shows own bucket + Owner" "0" \
    "$(echo "$TLIST" | grep -qF '<Name>acme-data</Name>' && echo "$TLIST" | grep -qF '<ID>acme</ID>'; echo $?)"
check "tenant user cannot use admin plane" "403" "$(tcurl -o /dev/null -w '%{http_code}' "$BASE/-/admin/tenants")"
check "tenant record lists bucket + usage" "11" "$(s3curl "$BASE/-/admin/tenants/acme" | json_num bytes)"
check "tenant delete refused while it owns buckets" "409" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X DELETE "$BASE/-/admin/tenants/acme")"
if [[ -x "$S3ADM" ]]; then
    adm() { LIGHTS3_ADMIN_AK=$AK LIGHTS3_ADMIN_SK=$SK "$S3ADM" "$@" --endpoint="$BASE" --region="$REGION"; }
    check "s3adm usage shows bucket" "0" "$(adm usage qbkt | grep -q '"bucket": "qbkt"'; echo $?)"
    check "s3adm quota set" "0" "$(adm quota set qbkt --max-objects=100 >/dev/null; echo $?)"
    check "s3adm quota get" "0" "$(adm quota get qbkt | grep -q '<MaxObjects>100</MaxObjects>'; echo $?)"
    check "s3adm quota clear" "0" "$(adm quota clear qbkt >/dev/null; echo $?)"
    check "s3adm tenant get" "0" "$(adm tenant get acme | grep -q '"acme-data"'; echo $?)"
    check "s3adm tenant list" "0" "$(adm tenant list | grep -q '"id": "acme"'; echo $?)"
fi
tcurl -o /dev/null -X DELETE "$BASE/acme-data/k"
check "tenant deletes its bucket" "204" "$(tcurl -o /dev/null -w '%{http_code}' -X DELETE "$BASE/acme-data")"
s3curl -o /dev/null -X DELETE "$BASE/-/admin/credentials/$T_AK"
check "delete tenant once empty" "204" "$(s3curl -o /dev/null -w '%{http_code}' -X DELETE "$BASE/-/admin/tenants/acme")"
s3curl -o /dev/null -X DELETE "$BASE/qbkt/ten"
s3curl -o /dev/null -X DELETE "$BASE/qbkt/second"
s3curl -o /dev/null -X DELETE "$BASE/qbkt"

# ---------- roadmap §4.4: config hot reload (SIGHUP + s3adm reload, docs/config-reload.md) ----------
sed -i 's/^  level: info$/  level: debug/' "$WORK/config.yaml"
kill -HUP "$SRV_PID"
for _ in $(seq 1 50); do
    grep -q "config reload: applied log.level: info -> debug" "$WORK/server.log" && break
    sleep 0.1
done
check "SIGHUP applies log.level" "0" \
    "$(grep -q 'config reload: applied log.level: info -> debug' "$WORK/server.log"; echo $?)"
sed -i 's/^  level: debug$/  level: info/' "$WORK/config.yaml"
# 600s keeps request_timeout >= transfer_stall_timeout (300s default): the reload
# runs the full startup validation and refuses inconsistent pairs
sed -i 's/^  port: 0$/  port: 0\n  request_timeout: 600s/' "$WORK/config.yaml"
RELOAD_OUT=$(s3curl -X POST "$BASE/-/admin/config/reload")
check "admin reload reports applied request_timeout" "0" \
    "$(echo "$RELOAD_OUT" | grep -q 'http.request_timeout: 300 -> 600'; echo $?)"
check "admin reload denied for non-root" "403" \
    "$(curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK2:$SK2" -o /dev/null -w '%{http_code}' -X POST "$BASE/-/admin/config/reload")"
if [[ -x "$S3ADM" ]]; then
    check "s3adm reload" "0" "$(LIGHTS3_ADMIN_AK=$AK LIGHTS3_ADMIN_SK=$SK "$S3ADM" reload --endpoint="$BASE" --region="$REGION" | grep -q '"ok": true'; echo $?)"
fi
sed -i 's/^  port: 0$/  port: 0\n  idle_timeout: 0s/' "$WORK/config.yaml"
check "invalid config refused on reload" "400" \
    "$(s3curl -o /dev/null -w '%{http_code}' -X POST "$BASE/-/admin/config/reload")"
sed -i '/^  idle_timeout: 0s$/d' "$WORK/config.yaml"
sed -i '/^  request_timeout: 600s$/d' "$WORK/config.yaml"  # restore for the restart phase

# ---------- roadmap §4.2: L1 connection counters + rate-limit series on /-/metrics ----------
METRICS_OUT=$(curl -s "$BASE/-/metrics")
check "metrics: connection counters present" "0" \
    "$(echo "$METRICS_OUT" | grep -q 'lights3_http_connections_total{result="accepted"}'; echo $?)"
check "metrics: timeout phases present" "0" \
    "$(echo "$METRICS_OUT" | grep -q 'lights3_http_timeouts_total{phase="header"}'; echo $?)"
check "metrics: rate-limit series present" "0" \
    "$(echo "$METRICS_OUT" | grep -q 'lights3_ratelimit_rejections_total{scope="ip"}'; echo $?)"
# roadmap §5.1: API x backend series, per-backend op histograms, backend time in the access log
check "metrics: api x backend series present" "0" \
    "$(echo "$METRICS_OUT" | grep -q 'lights3_api_requests_total{api="PutObject",backend="tierdata",class="2xx"}'; echo $?)"
check "metrics: backend op histogram present" "0" \
    "$(echo "$METRICS_OUT" | grep -q 'lights3_backend_op_seconds_count{backend="tierdata",op="put_object"}'; echo $?)"
# roadmap §5.3: L1 request/parse series, admission wait histogram, exact status
# codes, website-plane events
check "metrics: L1 request counter present" "0" \
    "$(echo "$METRICS_OUT" | grep -q '^lights3_http_requests_total [1-9]'; echo $?)"
check "metrics: L1 parse-error / TLS series present" "0" \
    "$(echo "$METRICS_OUT" | grep -q '^lights3_http_parse_errors_total ' && echo "$METRICS_OUT" | grep -q 'lights3_http_tls_handshakes_total{result="ok"}'; echo $?)"
check "metrics: admission wait histogram present" "0" \
    "$(echo "$METRICS_OUT" | grep -q '^lights3_admission_wait_seconds_count [1-9]'; echo $?)"
check "metrics: transfer stall series present" "0" \
    "$(echo "$METRICS_OUT" | grep -q 'lights3_transfer_stalls_total{direction="out"} 0'; echo $?)"
check "metrics: exact status code series present" "0" \
    "$(echo "$METRICS_OUT" | grep -q 'lights3_responses_by_status_total{status="200"} [1-9]'; echo $?)"
check "metrics: website event series present" "0" \
    "$(echo "$METRICS_OUT" | grep -q 'lights3_website_events_total{event="anon_read"} '; echo $?)"
# /-/metrics root gate, switched on and off through the hot reload
sed -i 's/^  port: 0$/  port: 0\n  metrics_access: root/' "$WORK/config.yaml"
RELOAD_OUT=$(s3curl -X POST "$BASE/-/admin/config/reload")
check "reload applies http.metrics_access" "0" \
    "$(echo "$RELOAD_OUT" | grep -q 'http.metrics_access: anonymous -> root'; echo $?)"
check "metrics: anonymous scrape denied under root gate" "403" \
    "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/-/metrics")"
check "metrics: root-signed scrape admitted under root gate" "200" \
    "$(s3curl -o /dev/null -w '%{http_code}' "$BASE/-/metrics")"
check "metrics: healthz stays anonymous under root gate" "200" \
    "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/-/healthz")"
sed -i '/^  metrics_access: root$/d' "$WORK/config.yaml"
s3curl -o /dev/null -X POST "$BASE/-/admin/config/reload"
check "metrics: anonymous scrape back after reload" "200" \
    "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/-/metrics")"
check "access log carries api and backend time" "0" \
    "$(grep -q 'access .* PUT "/mybucket/dir/big.bin" 200 .* api=PutObject backend=tierdata:' "$WORK/server.log"; echo $?)"
# roadmap §5.2: quoted path, remote address, bucket and TTFB slots; the streaming GET
# line is written at end of body with the bytes actually sent
check "access log carries remote/bucket/ttfb slots" "0" \
    "$(grep -q 'access .* PUT "/mybucket/dir/big.bin" 200 .* remote=127.0.0.1 bucket=mybucket ttfb=[0-9.]*ms ua="' "$WORK/server.log"; echo $?)"
check "access log: streaming GET reports the bytes sent" "0" \
    "$(grep -q 'access .* GET "/mybucket/dir/big.bin" 200 [1-9][0-9]* .* api=GetObject ' "$WORK/server.log"; echo $?)"

# Graceful shutdown
kill -TERM "$SRV_PID"
EXITED=1
for _ in $(seq 1 50); do
    kill -0 "$SRV_PID" 2>/dev/null || { EXITED=0; break; }
    sleep 0.1
done
check "SIGTERM graceful shutdown" "0" "$EXITED"
wait "$SRV_PID" 2>/dev/null
check "clean shutdown exit code" "0" "$?"  # roadmap §4.5: unclean teardown would exit 3
SRV_PID=""

# ---------- Restart: dynamic credential persistence check (docs/credential-management.md §8) ----------
# Restart with a master key: v1 plaintext objects are upgraded in place to v2 encrypted
# at load time (§10.1), signature verification keeps working
MASTER_KEY=000102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f
LIGHTS3_MASTER_KEY=$MASTER_KEY "$BIN" --config "$WORK/config.yaml" > "$WORK/server2.log" 2>&1 &
SRV_PID=$!
PORT=""
for _ in $(seq 1 50); do
    PORT=$(sed -n 's/.*listening on 127.0.0.1:\([0-9]*\).*/\1/p' "$WORK/server2.log" | head -1)
    [[ -n "$PORT" ]] && break
    kill -0 "$SRV_PID" 2>/dev/null || break
    sleep 0.1
done
check "server ready after restart" "0" "$([[ -n "$PORT" ]]; echo $?)"
BASE="http://127.0.0.1:$PORT"
check "dynamic credential still works after restart" "200" \
    "$(curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK2:$SK2" \
       -o /dev/null -w '%{http_code}' -I "$BASE/credbkt")"
check "revoked credential still rejected after restart" "403" \
    "$(dyncurl -o /dev/null -w '%{http_code}' "$BASE/credbkt/k")"
check "show-secret reversible under master key" "$SK2" \
    "$(s3curl "$BASE/-/admin/credentials/$AK2?show-secret=true" | json_field secret_key)"
if [[ "$BACKEND" == "localfs" || "$BACKEND" == "xlocalfs" ]]; then
    # Only localfs lets us inspect the .sys object files directly: after the upgrade no plaintext SK should remain on disk
    check "credential object encrypted (no plaintext SK)" "1" \
        "$(grep -rqF "$SK2" "$WORK/data" 2>/dev/null; echo $?)"
    check "credential object contains sk_enc" "0" \
        "$(grep -rq 'sk_enc' "$WORK/data" 2>/dev/null; echo $?)"
fi
kill -TERM "$SRV_PID"
wait "$SRV_PID" 2>/dev/null
SRV_PID=""

# ---------- roadmap §4.1: TLS smoke on the same driver (docs/tls.md) ----------
# A second instance with an openssl-CLI self-signed certificate: SigV4 over HTTPS
# end to end through curl, plus a plaintext probe refused on the TLS port
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes -days 2 \
    -subj "/CN=localhost" -addext "subjectAltName=DNS:localhost,IP:127.0.0.1" \
    -keyout "$WORK/tls.key" -out "$WORK/tls.crt" > /dev/null 2>&1
# Fresh storage roots and a private redis key prefix: the instance must not see the
# earlier run's metadata. The master key is the restart phase's — a cloudproxy remote
# already re-encrypted its .sys credential objects with it
sed -e "s#^  port: 0#  port: 0\n  tls_cert: $WORK/tls.crt\n  tls_key: $WORK/tls.key\n  tls_min_version: \"1.2\"\n  tls_reload_interval: 0s#" \
    -e "s#$WORK/data#$WORK/tls-data#g" -e "s#$WORK/staging#$WORK/tls-staging#g" \
    -e "s#$WORK/cloud-duo#$WORK/tls-cloud-duo#g" -e "s#$WORK/duo-local#$WORK/tls-duo-local#g" \
    -e "s#^    redis_prefix: \"\(.*\)\"#    redis_prefix: \"\1tls-\"#" \
    -e "s#^    tikv_prefix: \"\(.*\)\"#    tikv_prefix: \"\1tls-\"#" \
    -e "s#^    rados_namespace: \(.*\)#    rados_namespace: \1-tls#" \
    "$WORK/config.yaml" > "$WORK/config-tls.yaml"
LIGHTS3_MASTER_KEY=$MASTER_KEY "$BIN" --config "$WORK/config-tls.yaml" > "$WORK/server-tls.log" 2>&1 &
TLS_PID=$!
TPORT=""
for _ in $(seq 1 50); do
    TPORT=$(sed -n 's/.*listening on 127.0.0.1:\([0-9]*\).*/\1/p' "$WORK/server-tls.log" | head -1)
    [[ -n "$TPORT" ]] && break
    kill -0 "$TLS_PID" 2>/dev/null || break
    sleep 0.1
done
check "TLS instance started (https listener)" "0" "$([[ -n "$TPORT" ]] && grep -q "https server listening" "$WORK/server-tls.log"; echo $?)"
[[ -z "$TPORT" ]] && { echo "--- server-tls.log ---"; cat "$WORK/server-tls.log"; }
if [[ -n "$TPORT" ]]; then
    TBASE="https://127.0.0.1:$TPORT"
    tlscurl() { curl -sS --cacert "$WORK/tls.crt" --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK:$SK" "$@"; }
    check "TLS CreateBucket" "200" "$(tlscurl -o /dev/null -w '%{http_code}' -X PUT "$TBASE/tlsbkt")"
    check "TLS PutObject" "200" "$(tlscurl -o /dev/null -w '%{http_code}' -X PUT --data-binary 'over-tls' "$TBASE/tlsbkt/k")"
    check "TLS GetObject" "over-tls" "$(tlscurl "$TBASE/tlsbkt/k")"
    check "TLS 1.1 refused" "1" "$(curl -s --tls-max 1.1 --cacert "$WORK/tls.crt" -o /dev/null "$TBASE/-/healthz" 2>/dev/null && echo 0 || echo 1)"
    check "plaintext on the TLS port refused" "1" "$(curl -s -o /dev/null -w '%{http_code}' "http://127.0.0.1:$TPORT/-/healthz" | grep -q 200; echo $?)"
    tlscurl -o /dev/null -X DELETE "$TBASE/tlsbkt/k"
    tlscurl -o /dev/null -X DELETE "$TBASE/tlsbkt"
fi
kill -TERM "$TLS_PID" 2>/dev/null
wait "$TLS_PID" 2>/dev/null

# ---------- backlog-sequence ②: separate admin listener (docs/http-adapter.md §2.1) ----------
# A third instance with http.admin_port: the /-/ face moves to the admin port, the
# data-plane port answers 404 for it, probes stay on both, s3adm points at the admin port
sed -e "s#^  port: 0#  port: 0\n  admin_port: 0#" \
    -e "s#$WORK/data#$WORK/adm-data#g" -e "s#$WORK/staging#$WORK/adm-staging#g" \
    -e "s#$WORK/cloud-duo#$WORK/adm-cloud-duo#g" -e "s#$WORK/duo-local#$WORK/adm-duo-local#g" \
    -e "s#^    redis_prefix: \"\(.*\)\"#    redis_prefix: \"\1adm-\"#" \
    -e "s#^    tikv_prefix: \"\(.*\)\"#    tikv_prefix: \"\1adm-\"#" \
    -e "s#^    rados_namespace: \(.*\)#    rados_namespace: \1-adm#" \
    "$WORK/config.yaml" > "$WORK/config-admin.yaml"
LIGHTS3_MASTER_KEY=$MASTER_KEY "$BIN" --config "$WORK/config-admin.yaml" > "$WORK/server-admin.log" 2>&1 &
ADM_PID=$!
DPORT=""; APORT=""
for _ in $(seq 1 50); do
    DPORT=$(sed -n 's/.*http server listening on 127.0.0.1:\([0-9]*\).*/\1/p' "$WORK/server-admin.log" | head -1)
    APORT=$(sed -n 's/.*admin listener.*127.0.0.1:\([0-9]*\).*/\1/p' "$WORK/server-admin.log" | head -1)
    [[ -n "$DPORT" && -n "$APORT" ]] && break
    kill -0 "$ADM_PID" 2>/dev/null || break
    sleep 0.1
done
check "admin-port instance started (two listeners)" "0" "$([[ -n "$DPORT" && -n "$APORT" && "$DPORT" != "$APORT" ]]; echo $?)"
[[ -z "$APORT" ]] && { echo "--- server-admin.log ---"; cat "$WORK/server-admin.log"; }
if [[ -n "$DPORT" && -n "$APORT" ]]; then
    DB="http://127.0.0.1:$DPORT"; AB="http://127.0.0.1:$APORT"
    acurl() { curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK:$SK" "$@"; }
    check "probes on the data-plane port" "200 200" "$(curl -s -o /dev/null -w '%{http_code}' "$DB/-/healthz") $(curl -s -o /dev/null -w '%{http_code}' "$DB/-/readyz")"
    check "probes on the admin port" "200 200" "$(curl -s -o /dev/null -w '%{http_code}' "$AB/-/healthz") $(curl -s -o /dev/null -w '%{http_code}' "$AB/-/readyz")"
    check "metrics 404 on the data-plane port" "404" "$(curl -s -o /dev/null -w '%{http_code}' "$DB/-/metrics")"
    check "metrics 200 on the admin port" "200" "$(curl -s -o /dev/null -w '%{http_code}' "$AB/-/metrics")"
    check "admin API 404 on the data-plane port" "404" "$(acurl -o /dev/null -w '%{http_code}' "$DB/-/admin/credentials")"
    check "admin API 200 on the admin port" "200" "$(acurl -o /dev/null -w '%{http_code}' "$AB/-/admin/credentials")"
    check "data plane works on the data-plane port" "200" "$(acurl -o /dev/null -w '%{http_code}' -X PUT "$DB/admbkt")"
    check "data plane 404 on the admin port" "404" "$(acurl -o /dev/null -w '%{http_code}' -X PUT "$AB/admbkt2")"
    check "s3adm cred list against the admin port" "0" "$(LIGHTS3_ADMIN_AK=$AK LIGHTS3_ADMIN_SK=$SK "$S3ADM" cred list --endpoint="$AB" --region="$REGION" > /dev/null 2>&1; echo $?)"
    check "s3adm reload against the admin port" "0" "$(LIGHTS3_ADMIN_AK=$AK LIGHTS3_ADMIN_SK=$SK "$S3ADM" reload --endpoint="$AB" --region="$REGION" > /dev/null 2>&1; echo $?)"
    # httplib runs the upstream accept loop and reports no connection counters, so the
    # request counter (kept by every driver) is the "both listeners feed one view" probe
    check "request counter covers both listeners" "0" "$(curl -s "$AB/-/metrics" | grep -q '^lights3_http_requests_total [1-9]'; echo $?)"
    acurl -o /dev/null -X DELETE "$DB/admbkt"
fi
kill -TERM "$ADM_PID" 2>/dev/null
wait "$ADM_PID" 2>/dev/null
for _ in $(seq 1 20); do grep -q "lights3 exited cleanly" "$WORK/server-admin.log" && break; sleep 0.1; done
check "admin-port instance exited cleanly" "0" "$(grep -q "lights3 exited cleanly" "$WORK/server-admin.log"; echo $?)"

# ---------- roadmap §6.1: fault injection through the whole stack (docs/testing.md §4) ----------
# A second instance armed via LIGHTS3_FAULTS: the first staging write fails with
# EIO -> the PUT answers 500 InternalError, the object does not exist, the retry
# succeeds, and the backend error shows up on /-/metrics. Only backends with a
# localfs data path reach the point (memory/cloudproxy do not); duostore's own
# points are covered by the unit tests
if [[ "$BACKEND" == "localfs" || "$BACKEND" == "xlocalfs" || "$BACKEND" == "tiered" ]]; then
    sed -e "s#$WORK/data#$WORK/fault-data#g" -e "s#$WORK/staging#$WORK/fault-staging#g" \
        -e "s#$WORK/cloud-duo#$WORK/fault-cloud-duo#g" -e "s#$WORK/duo-local#$WORK/fault-duo-local#g" \
        "$WORK/config.yaml" > "$WORK/config-fault.yaml"
    LIGHTS3_FAULTS="localfs.write:1:EIO,xlocalfs.write:1:EIO" LIGHTS3_MASTER_KEY=$MASTER_KEY "$BIN" --config "$WORK/config-fault.yaml" > "$WORK/server-fault.log" 2>&1 &
    FAULT_PID=$!
    FPORT=""
    for _ in $(seq 1 50); do
        FPORT=$(sed -n 's/.*listening on 127.0.0.1:\([0-9]*\).*/\1/p' "$WORK/server-fault.log" | head -1)
        [[ -n "$FPORT" ]] && break
        kill -0 "$FAULT_PID" 2>/dev/null || break
        sleep 0.1
    done
    check "fault instance started with the point armed" "0" "$([[ -n "$FPORT" ]] && grep -q "fault injection armed: localfs.write:1:5, xlocalfs.write:1:5" "$WORK/server-fault.log"; echo $?)"
    if [[ -n "$FPORT" ]]; then
        FBASE="http://127.0.0.1:$FPORT"
        fcurl() { curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK:$SK" "$@"; }
        fcurl -o /dev/null -X PUT "$FBASE/fbkt"
        check "fault: first PUT fails with 500" "500" "$(fcurl -o /dev/null -w '%{http_code}' -X PUT --data-binary 'first write' "$FBASE/fbkt/k")"
        check "fault: object does not exist after the failed PUT" "404" "$(fcurl -o /dev/null -w '%{http_code}' -I "$FBASE/fbkt/k")"
        check "fault: one-shot point clears, retry succeeds" "200" "$(fcurl -o /dev/null -w '%{http_code}' -X PUT --data-binary 'second write' "$FBASE/fbkt/k")"
        check "fault: retried object readable" "second write" "$(fcurl "$FBASE/fbkt/k")"
        check "fault: backend error counted on /-/metrics" "0" \
            "$(curl -s "$FBASE/-/metrics" | grep -q 'lights3_backend_errors_total{backend="[a-z]*",op="put_object"} 1'; echo $?)"
        check "fault: exact status series shows the 500" "0" \
            "$(curl -s "$FBASE/-/metrics" | grep -q 'lights3_responses_by_status_total{status="500"} 1'; echo $?)"
        fcurl -o /dev/null -X DELETE "$FBASE/fbkt/k"
        fcurl -o /dev/null -X DELETE "$FBASE/fbkt"
    else
        echo "--- server-fault.log ---"; cat "$WORK/server-fault.log"
    fi
    kill -TERM "$FAULT_PID" 2>/dev/null
    wait "$FAULT_PID" 2>/dev/null
fi

echo
echo "e2e: $PASS passed, $FAIL failed"
if [[ $FAIL -ne 0 ]]; then
    echo "--- server.log ---"; cat "$WORK/server.log"
    [[ -f "$WORK/remote.log" ]] && { echo "--- remote.log ---"; cat "$WORK/remote.log"; }
    exit 1
fi
exit 0
