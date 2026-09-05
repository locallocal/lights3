#!/bin/sh
# Container entrypoint: no arguments or a leading flag / admin subcommand runs the
# server with the image config; anything else (s3adm, sh) is exec'd as given.
set -eu
CONFIG="${LIGHTS3_CONFIG:-/etc/lights3/lights3.yaml}"

run_server=0
if [ $# -eq 0 ]; then
    run_server=1
else
    case "$1" in
        -*|duostore|tier|fsck|help) run_server=1 ;;
    esac
fi

if [ "$run_server" -eq 1 ]; then
    if [ -z "${LIGHTS3_SECRET_1:-}" ] && ! grep -q '^\s*credentials_file:' "$CONFIG" 2>/dev/null; then
        echo "lights3: LIGHTS3_SECRET_1 is not set; the root credential AKIDEXAMPLE would reject every request." >&2
        echo "         docker run -e LIGHTS3_SECRET_1=... (docs/deployment.md §4)" >&2
        exit 2
    fi
    exec lights3 --config="$CONFIG" "$@"
fi
exec "$@"
