#!/usr/bin/env bash
# e2e: start a real lights3 process (localfs backend + SigV4 auth) and verify the full flow with curl --aws-sigv4
set -u

BIN="${1:?usage: run_e2e.sh <path-to-lights3-binary> [driver] [backend-type]}"
DRIVER="${2:-builtin}"
# localfs | xlocalfs | tiered (localfs+memory, docs/tiered-storage.md)
# | cloudproxy | tiered-cloudproxy (two instances: instance B acts as the "cloud", docs/cloudproxy-backend.md §10)
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
REDIS_PID=""
if [[ "$BACKEND" == "duostore-redis" ]]; then
    if ! command -v redis-server >/dev/null; then
        echo "[SKIP] duostore-redis: redis-server not available"
        exit 0
    fi
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
elif [[ "$BACKEND" == "duostore-redis" ]]; then cat <<DUOREDIS
  - name: tierdata
    type: duostore
    root: $WORK/data
    meta: redis
    redis_uri: unix://$WORK/redis.sock
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

# ---------- Test cases ----------
check "healthz (no auth)" "200" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/-/healthz")"
check "unsigned request rejected" "403" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/mybucket")"
check "wrong secret rejected" "403" \
    "$(curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK:wrong-secret" -o /dev/null -w '%{http_code}' "$BASE/mybucket" -X PUT)"

check "CreateBucket" "200" "$(s3curl -o /dev/null -w '%{http_code}' -X PUT "$BASE/mybucket")"
check "HeadBucket" "200" "$(s3curl -o /dev/null -w '%{http_code}' -I "$BASE/mybucket")"
check "duplicate create 409" "409" "$(s3curl -o /dev/null -w '%{http_code}' -X PUT "$BASE/mybucket")"

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

# Graceful shutdown
kill -TERM "$SRV_PID"
EXITED=1
for _ in $(seq 1 50); do
    kill -0 "$SRV_PID" 2>/dev/null || { EXITED=0; break; }
    sleep 0.1
done
check "SIGTERM graceful shutdown" "0" "$EXITED"
wait "$SRV_PID" 2>/dev/null
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

echo
echo "e2e: $PASS passed, $FAIL failed"
if [[ $FAIL -ne 0 ]]; then
    echo "--- server.log ---"; cat "$WORK/server.log"
    [[ -f "$WORK/remote.log" ]] && { echo "--- remote.log ---"; cat "$WORK/remote.log"; }
    exit 1
fi
exit 0
