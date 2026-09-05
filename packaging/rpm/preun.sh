# %preun: $1 == 0 final removal, 1 upgrade
if [ "$1" -eq 0 ]; then
    /usr/share/lights3/lights3-setup.sh remove
fi
