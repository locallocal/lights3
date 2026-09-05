# Install tree + CPack packages (roadmap §6.3, docs/deployment.md).
#
#   cmake --install build [--prefix /usr/local]   # binaries, lights3ctl, unit, sample config
#   cpack --config build/CPackConfig.cmake -G DEB  # or RPM; `cmake --build build --target package`
#
# Layout (prefix /usr for packages, /usr/local for a plain install):
#   <bindir>/lights3, <bindir>/s3adm
#   <sbindir>/lights3ctl                      (scripts/systemctl.sh)
#   <prefix>/lib/systemd/system/lights3.service
#   <confdir>/lights3.yaml                    preserved when it already exists
#   <datadir>/lights3/lights3-setup.sh        shared post-install helper
#   <datadir>/lights3/deploy/{prometheus,grafana}
#   <docdir>/README.md, docs/
#
# The config directory is /etc/lights3 for the two system prefixes (/usr,
# /usr/local -- what scripts/install.sh and every document assume) and
# <prefix>/etc/lights3 elsewhere (user-local trees, DESTDIR-less staging);
# override with -DLIGHTS3_CONFIG_DIR=<absolute>. Both the config dir and the
# unit file are resolved at *install* time so `cmake --install --prefix X`
# relocates everything consistently.
set(LIGHTS3_CONFIG_DIR "" CACHE STRING
    "Configuration directory (absolute); empty = /etc/lights3 under /usr and /usr/local, <prefix>/etc/lights3 otherwise")

# Directories created by the install rules get 0755 regardless of the build
# user's umask (package tools reject group-writable system directories)
set(CMAKE_INSTALL_DEFAULT_DIRECTORY_PERMISSIONS
    OWNER_READ OWNER_WRITE OWNER_EXECUTE GROUP_READ GROUP_EXECUTE WORLD_READ WORLD_EXECUTE)

install(TARGETS lights3 s3adm RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR})
install(PROGRAMS scripts/systemctl.sh DESTINATION ${CMAKE_INSTALL_SBINDIR} RENAME lights3ctl)
install(PROGRAMS packaging/lights3-setup.sh DESTINATION ${CMAKE_INSTALL_DATADIR}/lights3)
install(DIRECTORY deploy/prometheus deploy/grafana
        DESTINATION ${CMAKE_INSTALL_DATADIR}/lights3/deploy
        PATTERN "__pycache__" EXCLUDE PATTERN "*.pyc" EXCLUDE)
install(FILES README.md DESTINATION ${CMAKE_INSTALL_DOCDIR})
install(DIRECTORY docs/ DESTINATION ${CMAKE_INSTALL_DOCDIR}/docs
        FILES_MATCHING PATTERN "*.md" PATTERN "archive" EXCLUDE)

# Unit file + sample config, resolved against the install-time prefix. The
# sample config never clobbers a live file (same rule as scripts/install.sh and
# the deb conffile / rpm %config(noreplace) markers); CPack stages into an empty
# DESTDIR, so packages always carry it
install(CODE "
  set(_confdir \"${LIGHTS3_CONFIG_DIR}\")
  if(NOT _confdir)
    if(CMAKE_INSTALL_PREFIX MATCHES \"^/usr(/local)?/?$\")
      set(_confdir /etc/lights3)
    else()
      set(_confdir \"\${CMAKE_INSTALL_PREFIX}/etc/lights3\")
    endif()
  endif()
  set(LIGHTS3_CONFIG_DIR \"\${_confdir}\")
  set(LIGHTS3_BINDIR \"\${CMAKE_INSTALL_PREFIX}/${CMAKE_INSTALL_BINDIR}\")
  configure_file(\"${CMAKE_SOURCE_DIR}/scripts/lights3.service.in\"
                 \"${CMAKE_BINARY_DIR}/lights3.service\" @ONLY)
  file(INSTALL DESTINATION \"\${CMAKE_INSTALL_PREFIX}/lib/systemd/system\" TYPE FILE
       FILES \"${CMAKE_BINARY_DIR}/lights3.service\")
  set(_dst \"\$ENV{DESTDIR}\${_confdir}/lights3.yaml\")
  if(EXISTS \"\${_dst}\")
    message(STATUS \"Preserving existing config: \${_dst}\")
  else()
    file(INSTALL DESTINATION \"\${_confdir}\" TYPE FILE
         PERMISSIONS OWNER_READ OWNER_WRITE GROUP_READ
         FILES \"${CMAKE_SOURCE_DIR}/config/lights3.yaml\")
  endif()")

# ---- CPack ----
set(CPACK_PACKAGE_NAME lights3)
set(CPACK_PACKAGE_VENDOR "lights3")
set(CPACK_PACKAGE_CONTACT "lights3 maintainers <https://github.com/locallocal/lights3>")
set(CPACK_PACKAGE_HOMEPAGE_URL "https://github.com/locallocal/lights3")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "S3-compatible object storage gateway")
set(CPACK_PACKAGE_DESCRIPTION
    "LightS3 is a single-binary S3-compatible object storage server with pluggable HTTP drivers and storage backends (local filesystem, DuoStore, tiered cloud, cloud proxy). Ships the lights3 server, the s3adm ops CLI, a systemd unit and monitoring assets.")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGING_INSTALL_PREFIX /usr)
set(CPACK_STRIP_FILES ON)
set(CPACK_PACKAGE_DIRECTORY ${CMAKE_BINARY_DIR}/packages)
set(CPACK_SOURCE_IGNORE_FILES "/build.*/" "/\\\\.git/" "/\\\\.claude/")
# Generators follow the tools present: dpkg-deb for DEB, rpmbuild for RPM;
# with neither, a TGZ of the install tree (absolute paths such as /etc end up
# under the archive's usr/ prefix -- it is a build artifact, not a system image)
find_program(LIGHTS3_DPKG_DEB dpkg-deb)
find_program(LIGHTS3_RPMBUILD rpmbuild)
set(CPACK_GENERATOR "")
if(LIGHTS3_DPKG_DEB)
  list(APPEND CPACK_GENERATOR DEB)
endif()
if(LIGHTS3_RPMBUILD)
  list(APPEND CPACK_GENERATOR RPM)
endif()
if(NOT CPACK_GENERATOR)
  set(CPACK_GENERATOR TGZ)
endif()

# DEB: conffile + maintainer scripts from packaging/deb, shared library
# dependencies resolved by dpkg-shlibdeps (falls back to a manual list)
set(CPACK_DEBIAN_FILE_NAME DEB-DEFAULT)
set(CPACK_DEBIAN_PACKAGE_SECTION net)
set(CPACK_DEBIAN_PACKAGE_PRIORITY optional)
set(CPACK_DEBIAN_PACKAGE_DEPENDS "adduser, openssl")
find_program(LIGHTS3_DPKG_SHLIBDEPS dpkg-shlibdeps)
if(LIGHTS3_DPKG_SHLIBDEPS)
  set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON)
else()
  set(CPACK_DEBIAN_PACKAGE_DEPENDS "${CPACK_DEBIAN_PACKAGE_DEPENDS}, libc6, libstdc++6, libssl3 | libssl3t64")
endif()
set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA
    ${CMAKE_SOURCE_DIR}/packaging/deb/postinst
    ${CMAKE_SOURCE_DIR}/packaging/deb/prerm
    ${CMAKE_SOURCE_DIR}/packaging/deb/postrm
    ${CMAKE_SOURCE_DIR}/packaging/deb/conffiles)
set(CPACK_DEBIAN_PACKAGE_CONTROL_STRICT_PERMISSION ON)

# RPM: scriptlets from packaging/rpm, config marked noreplace, system-owned
# directories excluded from the file list so rpm does not claim /etc or /usr/lib/systemd
set(CPACK_RPM_FILE_NAME RPM-DEFAULT)
set(CPACK_RPM_PACKAGE_LICENSE "unspecified")  # the repository ships no LICENSE file yet
set(CPACK_RPM_PACKAGE_GROUP "System Environment/Daemons")
set(CPACK_RPM_PACKAGE_REQUIRES "shadow-utils, openssl, systemd")
set(CPACK_RPM_PACKAGE_AUTOREQ ON)
set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/packaging/rpm/post.sh)
set(CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/packaging/rpm/preun.sh)
set(CPACK_RPM_POST_UNINSTALL_SCRIPT_FILE ${CMAKE_SOURCE_DIR}/packaging/rpm/postun.sh)
set(CPACK_RPM_USER_FILELIST "%config(noreplace) /etc/lights3/lights3.yaml")
set(CPACK_RPM_EXCLUDE_FROM_AUTO_FILELIST_ADDITION
    /etc /usr/lib /usr/lib/systemd /usr/lib/systemd/system /usr/share/doc)

include(CPack)
