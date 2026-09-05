# %postun: $1 == 0 final removal, 1 upgrade. rpm has no purge: secrets, data and
# the service user stay behind on purpose; remove them with
# `rm -rf /etc/lights3 /var/lib/lights3 /var/log/lights3; userdel lights3`
if command -v systemctl >/dev/null 2>&1 && [ -d /run/systemd/system ]; then
    systemctl daemon-reload
fi
