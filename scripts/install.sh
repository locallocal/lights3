#!/usr/bin/env bash
# Install an already-built LightS3 tree as a systemd service (/usr/local layout).
# Packages (`cpack -G DEB|RPM`, docs/deployment.md) are the alternative for /usr;
# both channels share packaging/lights3-setup.sh for the user / directory /
# secrets / systemd handling, so the behavior is identical.
#
# Upgrades keep one previous copy of every binary (<name>.prev) so
# scripts/rollback.sh can swap back; scripts/uninstall.sh removes everything.
set -euo pipefail

SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
readonly SCRIPT_DIR
PROJECT_ROOT="$(cd -- "$SCRIPT_DIR/.." && pwd)"
readonly PROJECT_ROOT

BUILD_DIR="$PROJECT_ROOT/build"
CONFIG_SOURCE="$PROJECT_ROOT/config/lights3.yaml"
ENABLE_SERVICE=1
START_SERVICE=1

readonly SERVICE_GROUP="lights3"
readonly BIN_DIR="/usr/local/bin"
readonly SBIN_DIR="/usr/local/sbin"
readonly SHARE_DIR="/usr/local/share/lights3"
readonly CONFIG_DIR="/etc/lights3"
readonly CONFIG_FILE="$CONFIG_DIR/lights3.yaml"
readonly ENV_FILE="$CONFIG_DIR/lights3.env"
readonly STATE_DIR="/var/lib/lights3"
readonly UNIT_FILE="/etc/systemd/system/lights3.service"
readonly SETUP_HELPER="$PROJECT_ROOT/packaging/lights3-setup.sh"

usage() {
    cat <<'EOF2'
Usage: sudo ./scripts/install.sh [options]

Install LightS3 binaries, configuration, credentials, and its systemd unit.
Run ./build.sh first; existing configuration and credentials are preserved,
and the previously installed binaries are kept as *.prev for
scripts/rollback.sh.

Options:
  -B, --build-dir DIR  Build directory (default: ./build)
  --config FILE        Initial configuration file (default: config/lights3.yaml)
  --no-enable          Do not enable the service at boot
  --no-start           Do not start or restart the service after installation
  -h, --help           Show this help
EOF2
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
[[ -f "$SCRIPT_DIR/lights3.service.in" ]] || die "missing $SCRIPT_DIR/lights3.service.in"
[[ -f "$SCRIPT_DIR/systemctl.sh" ]] || die "missing $SCRIPT_DIR/systemctl.sh"
[[ -f "$SETUP_HELPER" ]] || die "missing $SETUP_HELPER"

for required_command in install mv chown chmod getent id useradd groupadd openssl systemctl sed; do
    command -v "$required_command" >/dev/null 2>&1 ||
        die "required command is not installed: $required_command"
done

# Validate the live config with the *new* binary before touching anything: an
# upgrade that would restart into a rejected config stops here instead
if [[ -f "$CONFIG_FILE" ]]; then
    if ! (
        set -a
        # shellcheck disable=SC1090
        [[ -f "$ENV_FILE" ]] && . "$ENV_FILE"
        set +a
        "$BUILD_DIR/lights3" --check-config --config="$CONFIG_FILE" >/dev/null
    ); then
        die "$CONFIG_FILE is rejected by the new binary ($BUILD_DIR/lights3 --check-config); fix it or pass --no-start"
    fi
fi

# Service account, /etc/lights3, /var/lib/lights3, secrets (first install only)
LIGHTS3_BIN="$BIN_DIR/lights3" LIGHTS3_CONFIG_DIR="$CONFIG_DIR" LIGHTS3_STATE_DIR="$STATE_DIR" \
    sh "$SETUP_HELPER" configure --no-enable --no-start

install -d -m 0755 -o root -g root "$BIN_DIR" "$SBIN_DIR" "$SHARE_DIR"

# Replace executable files atomically so upgrading a running service never
# truncates the inode that the current process is executing; the file being
# replaced is kept as <target>.prev (one generation) for scripts/rollback.sh
install_atomic() {
    local source="$1"
    local target="$2"
    local mode="$3"
    local temporary="${target}.new.$$"

    install -m "$mode" -o root -g root "$source" "$temporary"
    if [[ -e "$target" ]] && ! cmp -s "$source" "$target"; then
        mv -f -- "$target" "${target}.prev"
    fi
    mv -f -- "$temporary" "$target"
}

previous_version=""
if [[ -x "$BIN_DIR/lights3" ]]; then
    previous_version="$("$BIN_DIR/lights3" --version 2>/dev/null | head -n 1 || true)"
fi

install_atomic "$BUILD_DIR/lights3" "$BIN_DIR/lights3" 0755
if [[ -x "$BUILD_DIR/s3adm" ]]; then
    install_atomic "$BUILD_DIR/s3adm" "$BIN_DIR/s3adm" 0755
else
    echo "warning: $BUILD_DIR/s3adm is missing; the optional admin CLI was not installed" >&2
fi
install_atomic "$SCRIPT_DIR/systemctl.sh" "$SBIN_DIR/lights3ctl" 0755
install -m 0755 -o root -g root "$SETUP_HELPER" "$SHARE_DIR/lights3-setup.sh"

config_created=0
if [[ ! -e "$CONFIG_FILE" ]]; then
    install -m 0640 -o root -g "$SERVICE_GROUP" "$CONFIG_SOURCE" "$CONFIG_FILE"
    config_created=1
fi

# Unit file from the template, with this layout's paths
unit_temporary="${UNIT_FILE}.new.$$"
sed -e "s|@LIGHTS3_BINDIR@|$BIN_DIR|g" -e "s|@LIGHTS3_CONFIG_DIR@|$CONFIG_DIR|g" \
    "$SCRIPT_DIR/lights3.service.in" >"$unit_temporary"
chown root:root "$unit_temporary"
chmod 0644 "$unit_temporary"
mv -f -- "$unit_temporary" "$UNIT_FILE"

# daemon-reload, enable, start-or-restart (config re-validated by the helper)
helper_args=()
[[ $ENABLE_SERVICE -eq 1 ]] || helper_args+=(--no-enable)
[[ $START_SERVICE -eq 1 ]] || helper_args+=(--no-start)
LIGHTS3_BIN="$BIN_DIR/lights3" LIGHTS3_CONFIG_DIR="$CONFIG_DIR" LIGHTS3_STATE_DIR="$STATE_DIR" \
    sh "$SETUP_HELPER" configure ${helper_args[@]+"${helper_args[@]}"}

echo
echo "LightS3 installation complete"
echo "  version:     $("$BIN_DIR/lights3" --version | head -n 1)"
if [[ -n "$previous_version" && -e "$BIN_DIR/lights3.prev" ]]; then
    echo "  previous:    $previous_version (kept as $BIN_DIR/lights3.prev; scripts/rollback.sh swaps back)"
fi
echo "  binary:      $BIN_DIR/lights3"
echo "  config:      $CONFIG_FILE"
echo "  environment: $ENV_FILE"
echo "  data:        $STATE_DIR"
echo "  service:     $UNIT_FILE"
echo "  management:  $SBIN_DIR/lights3ctl"
if [[ $config_created -eq 0 ]]; then
    echo "  note: existing configuration was preserved"
fi
echo "  credentials: root access key AKIDEXAMPLE; the secret key lives in $ENV_FILE"
if [[ $START_SERVICE -eq 0 ]]; then
    echo "  next: sudo $SBIN_DIR/lights3ctl start"
else
    echo "  next: sudo $SBIN_DIR/lights3ctl status"
fi
