#!/usr/bin/env bash
# Install-tree smoke test (roadmap §6.3, docs/deployment.md §2): `cmake --install`
# into a scratch prefix, then assert the layout, the relocated unit file, the
# preserve-existing-config rule and the --version output of the installed
# binaries. Runs as ctest `install_tree`; no root, no packaging tools needed.
set -u
BUILD_DIR="${1:?usage: check_install.sh <build-dir>}"
WORK=$(mktemp -d /tmp/lights3-install.XXXXXX)
trap 'rm -rf "$WORK"' EXIT
PASS=0; FAIL=0
ok()   { PASS=$((PASS+1)); echo "  [PASS] $1"; }
bad()  { FAIL=$((FAIL+1)); echo "  [FAIL] $1"; }
check() { if eval "$2"; then ok "$1"; else bad "$1"; fi; }

PREFIX="$WORK/prefix"
echo "install into $PREFIX"
if ! cmake --install "$BUILD_DIR" --prefix "$PREFIX" >"$WORK/install.log" 2>&1; then
    cat "$WORK/install.log"; echo "cmake --install failed"; exit 1
fi

for f in bin/lights3 bin/s3adm sbin/lights3ctl lib/systemd/system/lights3.service \
         etc/lights3/lights3.yaml share/lights3/lights3-setup.sh \
         share/lights3/deploy/prometheus/lights3.rules.yml share/lights3/deploy/grafana/lights3.json \
         share/doc/lights3/README.md share/doc/lights3/docs/deployment.md; do
    check "installed $f" "[[ -e '$PREFIX/$f' ]]"
done
check "no __pycache__ shipped" "! find '$PREFIX/share/lights3' -name __pycache__ | grep -q ."
check "no archive docs shipped" "[[ ! -e '$PREFIX/share/doc/lights3/docs/archive' ]]"
check "lights3ctl executable" "[[ -x '$PREFIX/sbin/lights3ctl' ]]"
check "setup helper executable + sh -n clean" "[[ -x '$PREFIX/share/lights3/lights3-setup.sh' ]] && sh -n '$PREFIX/share/lights3/lights3-setup.sh'"
check "config mode 0640" "[[ \$(stat -c %a '$PREFIX/etc/lights3/lights3.yaml') == 640 ]]"

UNIT="$PREFIX/lib/systemd/system/lights3.service"
check "unit ExecStart points at the installed binary" "grep -q '^ExecStart=$PREFIX/bin/lights3 --config=$PREFIX/etc/lights3/lights3.yaml\$' '$UNIT'"
check "unit EnvironmentFile relocated" "grep -q '^EnvironmentFile=$PREFIX/etc/lights3/lights3.env\$' '$UNIT'"
check "unit has ExecReload (SIGHUP hot reload)" "grep -q '^ExecReload=/bin/kill -HUP \$MAINPID\$' '$UNIT'"
check "no template placeholders left" "! grep -q '@LIGHTS3_' '$UNIT'"

# A second install must not clobber an edited config
echo "# operator edit" >"$PREFIX/etc/lights3/lights3.yaml"
cmake --install "$BUILD_DIR" --prefix "$PREFIX" >"$WORK/install2.log" 2>&1
check "re-install preserves existing config" "[[ \$(cat '$PREFIX/etc/lights3/lights3.yaml') == '# operator edit' ]] && grep -q 'Preserving existing config' '$WORK/install2.log'"

# --version on the installed binaries: first line "<prog> <semver> (git <commit>, <type>, <date>)",
# then the drivers / features lines; both binaries report the same build
V=$("$PREFIX/bin/lights3" --version); rc=$?
check "lights3 --version exit 0" "[[ $rc -eq 0 ]]"
check "lights3 --version first line format" "echo \"\$V\" | head -n1 | grep -Eq '^lights3 [0-9]+\.[0-9]+\.[0-9]+ \(git [0-9a-f]{12}(-dirty)?|git unknown, [A-Za-z]+, [0-9]{4}-[0-9]{2}-[0-9]{2}\)\$'"
check "lights3 --version lists drivers" "echo \"\$V\" | grep -Eq '^drivers:  (builtin|beast|httplib|seastar)( (builtin|beast|httplib|seastar))*\$'"
check "lights3 --version lists features" "echo \"\$V\" | grep -q '^features: memory localfs xlocalfs tiered'"
S=$("$PREFIX/bin/s3adm" --version); rc=$?
check "s3adm --version exit 0" "[[ $rc -eq 0 ]]"
check "s3adm reports the same build" "[[ \"\${S#s3adm }\" == \"\${V#lights3 }\" ]]"
check "--version wins over --check-config" "'$PREFIX/bin/lights3' --version --check-config --config=/nonexistent | grep -q '^lights3 '"

# Packaging inputs: maintainer scripts are POSIX sh and executable where dpkg requires it
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
for f in deb/postinst deb/prerm deb/postrm; do
    check "packaging/$f executable + sh -n" "[[ -x '$ROOT/packaging/$f' ]] && sh -n '$ROOT/packaging/$f'"
done
for f in rpm/post.sh rpm/preun.sh rpm/postun.sh; do
    check "packaging/$f sh -n" "sh -n '$ROOT/packaging/$f'"
done
check "deb conffiles lists the config" "[[ \$(cat '$ROOT/packaging/deb/conffiles') == /etc/lights3/lights3.yaml ]]"
for f in install.sh rollback.sh uninstall.sh; do
    check "scripts/$f bash -n" "bash -n '$ROOT/scripts/$f'"
done

echo "install_tree: $PASS passed, $FAIL failed"
[[ $FAIL -eq 0 ]]
