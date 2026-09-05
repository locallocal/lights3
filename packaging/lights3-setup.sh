#!/bin/sh
# Shared post-install / pre-remove logic for every distribution channel
# (roadmap §6.3): scripts/install.sh, the deb maintainer scripts and the rpm
# scriptlets all call this file instead of carrying their own copy of the
# user / directory / secrets / systemd handling. POSIX sh: dpkg and rpm run
# maintainer scripts under /bin/sh, which is dash on Debian.
#
# Usage: lights3-setup.sh configure [--no-enable] [--no-start]
#        lights3-setup.sh remove
#        lights3-setup.sh purge
#
# Environment (all optional, defaults are the package layout):
#   LIGHTS3_BIN         server binary            (/usr/bin/lights3)
#   LIGHTS3_CONFIG_DIR  config + env directory   (/etc/lights3)
#   LIGHTS3_STATE_DIR   working / data directory (/var/lib/lights3)
#   LIGHTS3_LOG_DIR     log directory            (/var/log/lights3)
set -eu

LIGHTS3_BIN="${LIGHTS3_BIN:-/usr/bin/lights3}"
LIGHTS3_CONFIG_DIR="${LIGHTS3_CONFIG_DIR:-/etc/lights3}"
LIGHTS3_STATE_DIR="${LIGHTS3_STATE_DIR:-/var/lib/lights3}"
LIGHTS3_LOG_DIR="${LIGHTS3_LOG_DIR:-/var/log/lights3}"
SERVICE_USER=lights3
SERVICE_GROUP=lights3
SERVICE=lights3.service
CONFIG_FILE="$LIGHTS3_CONFIG_DIR/lights3.yaml"
ENV_FILE="$LIGHTS3_CONFIG_DIR/lights3.env"

have_systemd() {
    command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]
}

ensure_user() {
    if ! getent group "$SERVICE_GROUP" >/dev/null; then
        groupadd --system "$SERVICE_GROUP"
    fi
    if ! getent passwd "$SERVICE_USER" >/dev/null; then
        shell="$(command -v nologin || true)"
        [ -n "$shell" ] || shell=/usr/sbin/nologin
        useradd --system --gid "$SERVICE_GROUP" --home-dir "$LIGHTS3_STATE_DIR" \
            --no-create-home --shell "$shell" --comment "LightS3 service" "$SERVICE_USER"
    fi
}

ensure_dirs() {
    install -d -m 0750 -o root -g "$SERVICE_GROUP" "$LIGHTS3_CONFIG_DIR"
    install -d -m 0750 -o "$SERVICE_USER" -g "$SERVICE_GROUP" "$LIGHTS3_STATE_DIR" "$LIGHTS3_LOG_DIR"
    if [ -e "$CONFIG_FILE" ]; then
        chown root:"$SERVICE_GROUP" "$CONFIG_FILE"
        chmod 0640 "$CONFIG_FILE"
    fi
}

# First install only: random root credential secret + at-rest master key.
# Existing files are never touched (they hold the live secrets).
ensure_env() {
    if [ -e "$ENV_FILE" ]; then
        chown root:"$SERVICE_GROUP" "$ENV_FILE"
        chmod 0640 "$ENV_FILE"
        return 1
    fi
    tmp="$ENV_FILE.new.$$"
    umask 077
    {
        echo "# LightS3 secrets read by systemd. Keep this file private."
        echo "LIGHTS3_SECRET_1=$(openssl rand -hex 32)"
        echo "LIGHTS3_MASTER_KEY=$(openssl rand -hex 32)"
    } >"$tmp"
    chown root:"$SERVICE_GROUP" "$tmp"
    chmod 0640 "$tmp"
    mv -f "$tmp" "$ENV_FILE"
    return 0
}

# Dry-run the config with the freshly installed binary before (re)starting the
# service: a rejected file is reported, never silently restarted into a crash loop
config_ok() {
    [ -x "$LIGHTS3_BIN" ] || return 0
    [ -f "$CONFIG_FILE" ] || return 0
    (
        set -a
        # shellcheck disable=SC1090
        [ -f "$ENV_FILE" ] && . "$ENV_FILE"
        set +a
        "$LIGHTS3_BIN" --check-config --config="$CONFIG_FILE" >/dev/null
    )
}

configure() {
    enable=1
    start=1
    for arg in "$@"; do
        case "$arg" in
            --no-enable) enable=0 ;;
            --no-start) start=0 ;;
            *) echo "lights3-setup: unknown option $arg" >&2; exit 2 ;;
        esac
    done
    ensure_user
    ensure_dirs
    if ensure_env; then
        echo "lights3: generated $ENV_FILE (root access key AKIDEXAMPLE; secret inside)"
    fi
    have_systemd || return 0
    systemctl daemon-reload
    if [ "$enable" -eq 1 ]; then systemctl enable "$SERVICE"; fi
    [ "$start" -eq 1 ] || return 0
    if ! config_ok; then
        echo "lights3: $CONFIG_FILE is rejected by $LIGHTS3_BIN --check-config; not (re)starting $SERVICE" >&2
        return 0
    fi
    if systemctl is-active --quiet "$SERVICE"; then
        systemctl restart "$SERVICE"
    else
        systemctl start "$SERVICE"
    fi
}

remove() {
    have_systemd || return 0
    if systemctl is-active --quiet "$SERVICE"; then
        systemctl stop "$SERVICE"
    fi
    systemctl disable "$SERVICE" >/dev/null 2>&1 || true
}

purge() {
    rm -f "$ENV_FILE" "$CONFIG_FILE"
    rm -rf "$LIGHTS3_STATE_DIR" "$LIGHTS3_LOG_DIR"
    rmdir "$LIGHTS3_CONFIG_DIR" 2>/dev/null || true
    if getent passwd "$SERVICE_USER" >/dev/null; then
        userdel "$SERVICE_USER" 2>/dev/null || true
    fi
    if getent group "$SERVICE_GROUP" >/dev/null; then
        groupdel "$SERVICE_GROUP" 2>/dev/null || true
    fi
    if have_systemd; then
        systemctl daemon-reload
    fi
}

cmd="${1:-}"
[ $# -eq 0 ] || shift
case "$cmd" in
    configure) configure "$@" ;;
    remove) remove ;;
    purge) purge ;;
    *)
        echo "usage: lights3-setup.sh configure [--no-enable] [--no-start] | remove | purge" >&2
        exit 2
        ;;
esac
