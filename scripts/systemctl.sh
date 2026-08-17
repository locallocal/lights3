#!/usr/bin/env bash
# Small, fixed-service wrapper for common LightS3 systemd operations.
set -euo pipefail

readonly SERVICE_NAME="lights3.service"

usage() {
    cat <<'EOF'
Usage: systemctl.sh <command>

Commands:
  start          Start LightS3
  stop           Stop LightS3 gracefully
  restart        Restart LightS3
  status         Show the full service status
  enable         Enable LightS3 at boot and start it now
  disable        Stop LightS3 and disable it at boot
  is-active      Return whether LightS3 is running
  logs [LINES]   Show recent logs (default: 100 lines)
  follow         Follow service logs
  daemon-reload  Reload systemd unit files
  help           Show this help

Mutating commands normally need root privileges, for example:
  sudo /usr/local/sbin/lights3ctl restart
EOF
}

die() {
    echo "error: $*" >&2
    exit 2
}

command -v systemctl >/dev/null 2>&1 || die "systemctl is not installed"

command_name="${1:-help}"
[[ $# -eq 0 ]] || shift

case "$command_name" in
    start|stop|restart)
        [[ $# -eq 0 ]] || die "$command_name does not accept arguments"
        exec systemctl "$command_name" "$SERVICE_NAME"
        ;;
    status)
        [[ $# -eq 0 ]] || die "status does not accept arguments"
        exec systemctl status --no-pager --full "$SERVICE_NAME"
        ;;
    enable)
        [[ $# -eq 0 ]] || die "enable does not accept arguments"
        exec systemctl enable --now "$SERVICE_NAME"
        ;;
    disable)
        [[ $# -eq 0 ]] || die "disable does not accept arguments"
        exec systemctl disable --now "$SERVICE_NAME"
        ;;
    is-active)
        [[ $# -eq 0 ]] || die "is-active does not accept arguments"
        exec systemctl is-active "$SERVICE_NAME"
        ;;
    logs)
        lines="${1:-100}"
        [[ $# -le 1 ]] || die "logs accepts at most one line-count argument"
        [[ "$lines" =~ ^[1-9][0-9]*$ ]] || die "LINES must be a positive integer"
        command -v journalctl >/dev/null 2>&1 || die "journalctl is not installed"
        exec journalctl --unit "$SERVICE_NAME" --lines "$lines" --no-pager
        ;;
    follow)
        [[ $# -eq 0 ]] || die "follow does not accept arguments"
        command -v journalctl >/dev/null 2>&1 || die "journalctl is not installed"
        exec journalctl --unit "$SERVICE_NAME" --lines 100 --follow
        ;;
    daemon-reload)
        [[ $# -eq 0 ]] || die "daemon-reload does not accept arguments"
        exec systemctl daemon-reload
        ;;
    help|-h|--help)
        [[ $# -eq 0 ]] || die "help does not accept arguments"
        usage
        ;;
    *)
        usage >&2
        die "unknown command: $command_name"
        ;;
esac
