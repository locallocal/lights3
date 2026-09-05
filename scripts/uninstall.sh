#!/usr/bin/env bash
# Remove a scripts/install.sh installation (/usr/local layout). Without --purge
# the configuration, secrets, data and the service user stay behind; --purge
# deletes them as well. Package installs are removed with the package manager
# instead (apt remove/purge lights3, dnf remove lights3).
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
readonly BIN_DIR="/usr/local/bin"
readonly SBIN_DIR="/usr/local/sbin"
readonly SHARE_DIR="/usr/local/share/lights3"
readonly CONFIG_DIR="/etc/lights3"
readonly STATE_DIR="/var/lib/lights3"
readonly UNIT_FILE="/etc/systemd/system/lights3.service"
PURGE=0

usage() {
    cat <<'EOF2'
Usage: sudo ./scripts/uninstall.sh [--purge]

Stop and disable lights3.service, remove the binaries, lights3ctl and the unit
file. --purge additionally removes /etc/lights3 (config + secrets),
/var/lib/lights3 (data), /var/log/lights3 and the lights3 service user.
EOF2
}

die() {
    echo "error: $*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --purge) PURGE=1 ;;
        -h|--help) usage; exit 0 ;;
        *) usage >&2; die "unknown argument: $1" ;;
    esac
    shift
done

[[ $EUID -eq 0 ]] || die "run as root (for example with sudo)"

helper="$SHARE_DIR/lights3-setup.sh"
[[ -f "$helper" ]] || helper="$SCRIPT_DIR/../packaging/lights3-setup.sh"
[[ -f "$helper" ]] || die "missing lights3-setup.sh (neither $SHARE_DIR nor the source tree)"

LIGHTS3_CONFIG_DIR="$CONFIG_DIR" LIGHTS3_STATE_DIR="$STATE_DIR" sh "$helper" remove

rm -f "$BIN_DIR/lights3" "$BIN_DIR/lights3.prev" "$BIN_DIR/s3adm" "$BIN_DIR/s3adm.prev" \
      "$SBIN_DIR/lights3ctl" "$SBIN_DIR/lights3ctl.prev" "$UNIT_FILE"
if command -v systemctl >/dev/null 2>&1 && [[ -d /run/systemd/system ]]; then
    systemctl daemon-reload
fi

if [[ $PURGE -eq 1 ]]; then
    LIGHTS3_CONFIG_DIR="$CONFIG_DIR" LIGHTS3_STATE_DIR="$STATE_DIR" sh "$helper" purge
    echo "removed binaries, unit, $CONFIG_DIR, $STATE_DIR, /var/log/lights3 and the lights3 user"
else
    echo "removed binaries and unit; kept $CONFIG_DIR (config + secrets), $STATE_DIR (data) and the lights3 user"
    echo "  (rerun with --purge to delete them)"
fi
rm -rf "$SHARE_DIR"
