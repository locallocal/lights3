#!/bin/bash
# Multi-gateway multipart e2e (docs/archive/multi-gateway-multipart-design.md §4 ②)
# against the `multi` compose profile: two lights3 on one redis meta + one rados pool
# behind an nginx round-robin. A 5-part multipart whose steps land on whichever
# gateway nginx picks; verified by the S3 combined ETag (recomputed here from the
# per-part ETags), a byte-for-byte GET, and both gateways' request counters moving.
#
#   docker compose --profile multi run --rm e2e-multi
#
#   LIGHTS3_E2E_ENDPOINT   the balancer (default http://nginx-multi:9000)
#   LIGHTS3_E2E_GATEWAYS   space-separated gateway base URLs for the spread check
#   LIGHTS3_ACCESS_KEY / LIGHTS3_SECRET_1 / LIGHTS3_REGION   as the gateways' config
set -u
ENDPOINT=${LIGHTS3_E2E_ENDPOINT:-http://nginx-multi:9000}
GATEWAYS=${LIGHTS3_E2E_GATEWAYS:-"http://lights3-multi-a:9000 http://lights3-multi-b:9000"}
AK=${LIGHTS3_ACCESS_KEY:-AKIDEXAMPLE}
SK=${LIGHTS3_SECRET_1:-lights3-demo-secret}
REGION=${LIGHTS3_REGION:-us-east-1}
WAIT_SECS=${WAIT_SECS:-240}
PARTS=${PARTS:-5}
WORK=$(mktemp -d /tmp/lights3-e2e-multi.XXXXXX)
trap 'rm -rf "$WORK"' EXIT

PASS=0; FAIL=0
check() {  # check <description> <expected> <actual>
    if [[ "$2" == "$3" ]]; then PASS=$((PASS + 1)); echo "[ OK ] $1"
    else FAIL=$((FAIL + 1)); echo "[FAIL] $1: expected '$2', got '$3'"; fi
}
s3curl() { curl -sS --aws-sigv4 "aws:amz:$REGION:s3" --user "$AK:$SK" "$@"; }
requests_served() {  # requests_served <gateway base url> -- sum of lights3_requests_total
    curl -s "$1/-/metrics" | awk '/^lights3_requests_total\{/ { s += $2 } END { print s + 0 }'
}

for gw in $GATEWAYS $ENDPOINT; do
    for ((i = 0; i < WAIT_SECS; i++)); do
        curl -fsS "$gw/-/healthz" >/dev/null 2>&1 && break
        sleep 1
    done
    curl -fsS "$gw/-/healthz" >/dev/null 2>&1 || { echo "gateway $gw not healthy" >&2; exit 1; }
done
declare -A BEFORE
for gw in $GATEWAYS; do BEFORE[$gw]=$(requests_served "$gw"); done

s3curl -o /dev/null -X PUT "$ENDPOINT/mgw"
INIT=$(s3curl -X POST "$ENDPOINT/mgw/spread.bin?uploads")
ID=$(echo "$INIT" | sed -n 's/.*<UploadId>\(.*\)<\/UploadId>.*/\1/p')
check "CreateMultipartUpload through the balancer" "0" "$([[ -n "$ID" ]]; echo $?)"
ETAGS=()
XML="<CompleteMultipartUpload>"
for ((n = 1; n <= PARTS; n++)); do
    # AWS's 5 MiB minimum for every part but the last
    if [[ $n -lt $PARTS ]]; then dd if=/dev/urandom of="$WORK/p$n" bs=1M count=5 2>/dev/null
    else dd if=/dev/urandom of="$WORK/p$n" bs=1K count=700 2>/dev/null; fi
    s3curl -o /dev/null -D "$WORK/h$n" --data-binary "@$WORK/p$n" -X PUT \
        "$ENDPOINT/mgw/spread.bin?partNumber=$n&uploadId=$ID"
    ET=$(tr -d '\r' < "$WORK/h$n" | sed -n 's/^etag: //Ip')
    ETAGS+=("$ET")
    XML+="<Part><PartNumber>$n</PartNumber><ETag>$ET</ETag></Part>"
done
XML+="</CompleteMultipartUpload>"
check "every part returned an ETag" "$PARTS" "$(printf '%s\n' "${ETAGS[@]}" | grep -c '^"[0-9a-f]\{32\}"$')"
check "ListParts sees all parts wherever they landed" "$PARTS" \
    "$(s3curl "$ENDPOINT/mgw/spread.bin?uploadId=$ID" | grep -o '<PartNumber>' | wc -l)"
EXPECT=$(python3 -c 'import hashlib, sys
parts = [p.strip("\"") for p in sys.argv[1:]]
print(hashlib.md5(b"".join(bytes.fromhex(p) for p in parts)).hexdigest() + "-%d" % len(parts))' "${ETAGS[@]}")
DONE=$(s3curl -X POST --data-binary "$XML" "$ENDPOINT/mgw/spread.bin?uploadId=$ID")
check "CompleteMultipartUpload returns the combined ETag" "$EXPECT" \
    "$(echo "$DONE" | sed -n 's/.*<ETag>&quot;\([^&]*\)&quot;<\/ETag>.*/\1/p')"
check "HEAD carries the combined ETag" "\"$EXPECT\"" \
    "$(s3curl -sI "$ENDPOINT/mgw/spread.bin" | tr -d '\r' | sed -n 's/^etag: //Ip')"
s3curl -o "$WORK/out" "$ENDPOINT/mgw/spread.bin"
check "GET returns the bytes the parts carried" \
    "$(for ((n = 1; n <= PARTS; n++)); do cat "$WORK/p$n"; done | md5sum | cut -d' ' -f1)" \
    "$(md5sum "$WORK/out" | cut -d' ' -f1)"
check "the upload is retired" "0" "$(s3curl "$ENDPOINT/mgw?uploads" | grep -c '<UploadId>')"
for gw in $GATEWAYS; do
    check "gateway $gw served part of the flow" "1" \
        "$([[ $(requests_served "$gw") -gt ${BEFORE[$gw]} ]] && echo 1 || echo 0)"
done
s3curl -o /dev/null -X DELETE "$ENDPOINT/mgw/spread.bin"
s3curl -o /dev/null -X DELETE "$ENDPOINT/mgw"
echo "e2e-multi: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
