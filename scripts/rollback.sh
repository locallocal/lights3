#!/usr/bin/env bash
# Swap the /usr/local binaries back to the generation kept by scripts/install.sh
# (<name>.prev) and restart the service. Running it twice swaps forward again:
# the current binary becomes the new .prev. Configuration and data are left
# alone -- a config change that only the newer binary understands must be
# reverted by hand (lights3 --check-config tells).
set -euo pipefail

readonly BIN_DIR="/usr/local/bin"
readonly CONFIG_FILE="/etc/lights3/lights3.yaml"
readonly ENV_FILE="/etc/lights3/lights3.env"
readonly SERVICE="lights3.service"
RESTART=1

usage() {
    cat <<'EOF2'
Usage: sudo ./scripts/rollback.sh [--no-restart]

Swap /usr/local/bin/lights3 (and s3adm) with the *.prev copy kept by the last
scripts/install.sh run, then restart lights3.service if it is running.
EOF2
}

die() {
    echo "error: $*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --no-restart) RESTART=0 ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; die "unknown argument: $1" ;;
    esac
    shift
done

[[ $EUID -eq 0 ]] || die "run as root (for example with sudo)"
[[ -x "$BIN_DIR/lights3.prev" ]] || die "no previous generation: $BIN_DIR/lights3.prev does not exist"

echo "current:  $("$BIN_DIR/lights3" --version | head -n 1)"
echo "previous: $("$BIN_DIR/lights3.prev" --version 2>/dev/null | head -n 1 || echo 'older build without --version')"

# The previous binary must accept today's config, otherwise the restart would loop
if [[ -f "$CONFIG_FILE" ]] && "$BIN_DIR/lights3.prev" --help 2>/dev/null | grep -q -- '--check-config'; then
    (
        set -a
        # shellcheck disable=SC1090
        [[ -f "$ENV_FILE" ]] && . "$ENV_FILE"
        set +a
        "$BIN_DIR/lights3.prev" --check-config --config="$CONFIG_FILE" >/dev/null
    ) || die "$CONFIG_FILE is rejected by the previous binary; revert the config first"
fi

# Three renames per binary: current -> tmp, prev -> current, tmp -> prev. Each
# rename is atomic, so a running service always sees a complete executable
swap() {
    local target="$1"
    local temporary="${target}.swap.$$"
    [[ -x "${target}.prev" ]] || return 0
    mv -f -- "$target" "$temporary"
    mv -f -- "${target}.prev" "$target"
    mv -f -- "$temporary" "${target}.prev"
    echo "swapped:  $target <-> ${target}.prev"
}
swap "$BIN_DIR/lights3"
swap "$BIN_DIR/s3adm"

if [[ $RESTART -eq 1 ]] && command -v systemctl >/dev/null 2>&1 && systemctl is-active --quiet "$SERVICE"; then
    systemctl restart "$SERVICE"
    echo "restarted $SERVICE"
fi
echo "now:      $("$BIN_DIR/lights3" --version | head -n 1)"
