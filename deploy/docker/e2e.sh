#!/bin/bash
# e2e runner inside the `e2e` image (docker compose --profile e2e): wait for the
# compose services the selected tests need, prepare the rados pool, run ctest.
#
#   E2E_TESTS   ctest -R regex (default: the three externally-backed duostore paths)
#   LIGHTS3_TEST_REDIS_URI / LIGHTS3_TEST_PD_ADDR / LIGHTS3_TEST_RADOS_CONF+POOL
#               are read by tests/e2e/run_e2e.sh; unset ones make that test SKIP
set -u
BUILD_DIR=${BUILD_DIR:-/src/build}
E2E_TESTS=${E2E_TESTS:-'e2e_duostore_(redis|tikv|rados)'}
WAIT_SECS=${WAIT_SECS:-240}

wait_tcp() {  # wait_tcp host port
    local i
    for ((i = 0; i < WAIT_SECS; i++)); do
        if (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; then exec 3>&-; return 0; fi
        sleep 1
    done
    echo "timeout waiting for $1:$2" >&2
    return 1
}

if [[ -n "${LIGHTS3_TEST_REDIS_URI:-}" ]]; then
    hp=${LIGHTS3_TEST_REDIS_URI#redis://}; hp=${hp#*@}; hp=${hp%%/*}
    wait_tcp "${hp%%:*}" "${hp##*:}" || exit 1
    echo "redis reachable: $LIGHTS3_TEST_REDIS_URI"
fi
if [[ -n "${LIGHTS3_TEST_PD_ADDR:-}" ]]; then
    first=${LIGHTS3_TEST_PD_ADDR%%,*}
    wait_tcp "${first%%:*}" "${first##*:}" || exit 1
    # PD answers before the first TiKV store registers; a store must be Up for
    # the client to place any key range
    for ((i = 0; i < WAIT_SECS; i++)); do
        if curl -sf "http://$first/pd/api/v1/stores" 2>/dev/null | grep -q '"state_name": *"Up"'; then break; fi
        sleep 1
    done
    echo "pd reachable: $LIGHTS3_TEST_PD_ADDR"
fi
if [[ -n "${LIGHTS3_TEST_RADOS_CONF:-}" ]]; then
    for ((i = 0; i < WAIT_SECS; i++)); do
        if [[ -f "$LIGHTS3_TEST_RADOS_CONF" ]] && ceph -c "$LIGHTS3_TEST_RADOS_CONF" -s >/dev/null 2>&1; then break; fi
        sleep 1
    done
    ceph -c "$LIGHTS3_TEST_RADOS_CONF" -s >/dev/null 2>&1 || { echo "ceph cluster not reachable" >&2; exit 1; }
    pool=${LIGHTS3_TEST_RADOS_POOL:?LIGHTS3_TEST_RADOS_POOL}
    if ! ceph -c "$LIGHTS3_TEST_RADOS_CONF" osd pool ls | grep -qx "$pool"; then
        ceph -c "$LIGHTS3_TEST_RADOS_CONF" osd pool create "$pool" 8 >/dev/null
        ceph -c "$LIGHTS3_TEST_RADOS_CONF" osd pool application enable "$pool" rados >/dev/null
    fi
    echo "rados reachable: pool $pool"
fi

cd "$BUILD_DIR" || exit 1
# Only the server binary was built in the builder stage; the selected tests are
# scripts around it, so no further compilation is needed
exec ctest --output-on-failure -R "$E2E_TESTS" "$@"
