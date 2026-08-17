#!/usr/bin/env bash
# Install an already-built LightS3 tree as a systemd service.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
readonly PROJECT_ROOT

BUILD_DIR="$PROJECT_ROOT/build"
CONFIG_SOURCE="$PROJECT_ROOT/config/lights3.yaml"
ENABLE_SERVICE=1
START_SERVICE=1

readonly SERVICE_USER="lights3"
readonly SERVICE_GROUP="lights3"
readonly BIN_DIR="/usr/local/bin"
readonly SBIN_DIR="/usr/local/sbin"
readonly CONFIG_DIR="/etc/lights3"
readonly CONFIG_FILE="$CONFIG_DIR/lights3.yaml"
readonly ENV_FILE="$CONFIG_DIR/lights3.env"
readonly STATE_DIR="/var/lib/lights3"
readonly UNIT_FILE="/etc/systemd/system/lights3.service"

usage() {
    cat <<'EOF'
Usage: sudo ./scripts/install.sh [options]

Install LightS3 binaries, configuration, credentials, and its systemd unit.
Run ./build.sh first; existing configuration and credentials are preserved.

Options:
  -B, --build-dir DIR  Build directory (default: ./build)
  --config FILE        Initial configuration file (default: config/lights3.yaml)
  --no-enable          Do not enable the service at boot
  --no-start           Do not start or restart the service after installation
  -h, --help           Show this help
EOF
}

die() {
    echo "error: $*" >&2
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        -B|--build-dir)
            [[ $# -ge 2 ]] || die "$1 requires a directory"
            BUILD_DIR="$2"
            shift
            ;;
        --config)
            [[ $# -ge 2 ]] || die "$1 requires a file"
            CONFIG_SOURCE="$2"
            shift
            ;;
        --no-enable)
            ENABLE_SERVICE=0
            ;;
        --no-start)
            START_SERVICE=0
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            usage >&2
            die "unknown argument: $1"
            ;;
    esac
    shift
done

if [[ "$BUILD_DIR" != /* ]]; then
    BUILD_DIR="$PROJECT_ROOT/$BUILD_DIR"
fi
if [[ "$CONFIG_SOURCE" != /* ]]; then
    CONFIG_SOURCE="$PROJECT_ROOT/$CONFIG_SOURCE"
fi

[[ $EUID -eq 0 ]] || die "run this installer as root (for example with sudo)"
[[ -x "$BUILD_DIR/lights3" ]] ||
    die "missing $BUILD_DIR/lights3; build it first with ./build.sh"
[[ -f "$CONFIG_SOURCE" ]] || die "configuration file not found: $CONFIG_SOURCE"
[[ -f "$SCRIPT_DIR/lights3.service" ]] || die "missing $SCRIPT_DIR/lights3.service"
[[ -f "$SCRIPT_DIR/systemctl.sh" ]] || die "missing $SCRIPT_DIR/systemctl.sh"

for required_command in install mv chown chmod getent id useradd groupadd openssl systemctl; do
    command -v "$required_command" >/dev/null 2>&1 ||
        die "required command is not installed: $required_command"
done

if ! getent group "$SERVICE_GROUP" >/dev/null; then
    groupadd --system "$SERVICE_GROUP"
fi
if ! id -u "$SERVICE_USER" >/dev/null 2>&1; then
    nologin_shell="$(command -v nologin || true)"
    [[ -n "$nologin_shell" ]] || nologin_shell="/usr/sbin/nologin"
    useradd --system --gid "$SERVICE_GROUP" --home-dir "$STATE_DIR" \
        --no-create-home --shell "$nologin_shell" --comment "LightS3 service" \
        "$SERVICE_USER"
fi

install -d -m 0755 -o root -g root "$BIN_DIR" "$SBIN_DIR"
install -d -m 0750 -o root -g "$SERVICE_GROUP" "$CONFIG_DIR"
install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$STATE_DIR"

# Replace executable files atomically so upgrading a running service never
# truncates the inode that the current process is executing.
install_atomic() {
    local source="$1"
    local target="$2"
    local mode="$3"
    local temporary="${target}.new.$$"

    install -m "$mode" -o root -g root "$source" "$temporary"
    mv -f -- "$temporary" "$target"
}

install_atomic "$BUILD_DIR/lights3" "$BIN_DIR/lights3" 0755
if [[ -x "$BUILD_DIR/s3adm" ]]; then
    install_atomic "$BUILD_DIR/s3adm" "$BIN_DIR/s3adm" 0755
else
    echo "warning: $BUILD_DIR/s3adm is missing; the optional admin CLI was not installed" >&2
fi
install_atomic "$SCRIPT_DIR/systemctl.sh" "$SBIN_DIR/lights3ctl" 0755

config_created=0
if [[ ! -e "$CONFIG_FILE" ]]; then
    install -m 0640 -o root -g "$SERVICE_GROUP" "$CONFIG_SOURCE" "$CONFIG_FILE"
    config_created=1
else
    chown root:"$SERVICE_GROUP" "$CONFIG_FILE"
    chmod 0640 "$CONFIG_FILE"
fi

env_created=0
if [[ ! -e "$ENV_FILE" ]]; then
    secret_key="$(openssl rand -hex 32)"
    master_key="$(openssl rand -hex 32)"
    env_temporary="${ENV_FILE}.new.$$"
    umask 077
    {
        echo "# LightS3 secrets read by systemd. Keep this file private."
        echo "LIGHTS3_SECRET_1=$secret_key"
        echo "LIGHTS3_MASTER_KEY=$master_key"
    } >"$env_temporary"
    chown root:"$SERVICE_GROUP" "$env_temporary"
    chmod 0640 "$env_temporary"
    mv -f -- "$env_temporary" "$ENV_FILE"
    unset secret_key master_key
    env_created=1
else
    chown root:"$SERVICE_GROUP" "$ENV_FILE"
    chmod 0640 "$ENV_FILE"
fi

service_was_active=0
if systemctl is-active --quiet lights3.service; then
    service_was_active=1
fi

install_atomic "$SCRIPT_DIR/lights3.service" "$UNIT_FILE" 0644
systemctl daemon-reload

if [[ $ENABLE_SERVICE -eq 1 ]]; then
    systemctl enable lights3.service
fi
if [[ $START_SERVICE -eq 1 ]]; then
    if [[ $service_was_active -eq 1 ]]; then
        systemctl restart lights3.service
    else
        systemctl start lights3.service
    fi
fi

echo
echo "LightS3 installation complete"
echo "  binary:      $BIN_DIR/lights3"
echo "  config:      $CONFIG_FILE"
echo "  environment: $ENV_FILE"
echo "  data:        $STATE_DIR"
echo "  service:     $UNIT_FILE"
echo "  management:  $SBIN_DIR/lights3ctl"
if [[ $config_created -eq 0 ]]; then
    echo "  note: existing configuration was preserved"
fi
if [[ $env_created -eq 1 ]]; then
    echo "  credentials: access key AKIDEXAMPLE; secret key is stored in $ENV_FILE"
else
    echo "  note: existing environment and credentials were preserved"
fi
if [[ $START_SERVICE -eq 0 ]]; then
    echo "  next: sudo $SBIN_DIR/lights3ctl start"
else
    echo "  next: sudo $SBIN_DIR/lights3ctl status"
fi
