# syntax=docker/dockerfile:1
# LightS3 container image (roadmap §6.3, docs/deployment.md §4).
#
#   docker build -t lights3 .                                     # builtin/beast/httplib drivers,
#                                                                 # localfs/xlocalfs/tiered/cloudproxy/duostore(+redis,+sqlite)
#   docker build -t lights3:full --build-arg LIGHTS3_RADOS=ON --build-arg LIGHTS3_TIKV=ON .
#   docker build --build-arg LIGHTS3_GIT_COMMIT=$(git rev-parse --short=12 HEAD) .   # stamp the build
#
# Stages: builder (full toolchain, cmake --install into /stage), runtime (the
# shipped image: binaries + setup helper + monitoring assets, non-root user),
# e2e (builder + curl/python3/ceph CLI, runs ctest against the compose services).
# Submodules come from the build context, so check them out on the host first
# (.dockerignore explains); the seastar driver is out of scope for the image.

ARG BASE=ubuntu:24.04

# ---------------------------------------------------------------- builder ----
FROM ${BASE} AS builder
ARG DEBIAN_FRONTEND=noninteractive
ARG LIGHTS3_REDIS=ON
ARG LIGHTS3_SQLITE=ON
ARG LIGHTS3_RADOS=OFF
ARG LIGHTS3_TIKV=OFF
ARG LIGHTS3_GIT_COMMIT=unknown
ARG CMAKE_BUILD_TYPE=Release
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential cmake ninja-build pkg-config ca-certificates \
        libssl-dev libboost-dev tcl \
    && if [ "$LIGHTS3_RADOS" = "ON" ]; then \
        apt-get install -y --no-install-recommends librados-dev; fi \
    && if [ "$LIGHTS3_TIKV" = "ON" ]; then \
        apt-get install -y --no-install-recommends \
            libgrpc++-dev protobuf-compiler-grpc libprotobuf-dev libpoco-dev libabsl-dev; fi \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# Tests stay registered (ctest in the e2e stage) but only the two binaries are
# compiled here; unit_tests is built on demand by the e2e stage
RUN cmake -S . -B build -G Ninja \
        -DCMAKE_BUILD_TYPE="$CMAKE_BUILD_TYPE" \
        -DLIGHTS3_GIT_COMMIT="$LIGHTS3_GIT_COMMIT" \
        -DLIGHTS3_DUOSTORE_REDIS_META="$LIGHTS3_REDIS" \
        -DLIGHTS3_DUOSTORE_SQLITE_META="$LIGHTS3_SQLITE" \
        -DLIGHTS3_DUOSTORE_RADOS_DATA="$LIGHTS3_RADOS" \
        -DLIGHTS3_DUOSTORE_TIKV_META="$LIGHTS3_TIKV" \
    && cmake --build build -j"$(nproc)" --target lights3 s3adm \
    && DESTDIR=/stage cmake --install build --prefix /usr \
    && /stage/usr/bin/lights3 --version

# ---------------------------------------------------------------- runtime ----
FROM ${BASE} AS runtime
ARG DEBIAN_FRONTEND=noninteractive
ARG LIGHTS3_RADOS=OFF
ARG LIGHTS3_TIKV=OFF
# libssl3t64 is Ubuntu 24.04's name, libssl3 Debian's; curl serves the HEALTHCHECK.
# Optional backends pull their client libraries through the -dev packages: the
# names are stable across releases, the runtime-only packages are not
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && (apt-get install -y --no-install-recommends libssl3t64 \
        || apt-get install -y --no-install-recommends libssl3) \
    && if [ "$LIGHTS3_RADOS" = "ON" ]; then \
        apt-get install -y --no-install-recommends librados2; fi \
    && if [ "$LIGHTS3_TIKV" = "ON" ]; then \
        apt-get install -y --no-install-recommends \
            libgrpc++-dev libprotobuf-dev libpoco-dev libabsl-dev; fi \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --system lights3 \
    && useradd --system --gid lights3 --home-dir /var/lib/lights3 --no-create-home \
        --shell /usr/sbin/nologin --comment "LightS3 service" lights3 \
    && install -d -m 0750 -o root -g lights3 /etc/lights3 \
    && install -d -m 0750 -o lights3 -g lights3 /var/lib/lights3 /var/log/lights3
COPY --from=builder /stage/usr/bin/lights3 /stage/usr/bin/s3adm /usr/bin/
COPY --from=builder /stage/usr/share/lights3 /usr/share/lights3
COPY --chown=root:lights3 --chmod=0640 deploy/docker/lights3.yaml /etc/lights3/lights3.yaml
COPY --chmod=0755 deploy/docker/entrypoint.sh /usr/local/bin/lights3-entrypoint
VOLUME ["/var/lib/lights3"]
EXPOSE 9000
USER lights3
WORKDIR /var/lib/lights3
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD curl -fsS http://127.0.0.1:9000/-/healthz || exit 1
ENTRYPOINT ["lights3-entrypoint"]
# Any `lights3 ...` flag or admin subcommand; `s3adm ...` / `sh` run as-is
CMD []

# -------------------------------------------------------------------- e2e ----
# Test runner for `docker compose --profile e2e up` (docs/deployment.md §4.3):
# the redis / tikv / rados e2e paths that stay SKIP on a developer box
FROM builder AS e2e
ARG DEBIAN_FRONTEND=noninteractive
ARG LIGHTS3_RADOS=OFF
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl python3 openssl \
    && if [ "$LIGHTS3_RADOS" = "ON" ]; then \
        apt-get install -y --no-install-recommends ceph-common; fi \
    && rm -rf /var/lib/apt/lists/*
COPY --chmod=0755 deploy/docker/e2e.sh /usr/local/bin/lights3-e2e
WORKDIR /src
ENTRYPOINT ["lights3-e2e"]
