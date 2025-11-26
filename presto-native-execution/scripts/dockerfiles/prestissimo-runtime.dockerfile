#===================
# Global ARGs
#===================
ARG DEPENDENCY_IMAGE=presto/prestissimo-dependency:centos9
ARG BASE_IMAGE=quay.io/centos/centos:stream9
# ============================
# Stage 1 — dependency builder
# ============================
FROM ${DEPENDENCY_IMAGE} as prestissimo-build

ARG BUILD_TYPE=Release
ARG NUM_THREADS=8
ARG EXTRA_CMAKE_FLAGS=''
ENV BUILD_BASE_DIR=_build
ENV BUILD_DIR=release

# ----------------------------
# STEP 1 — copy only build scripts first (cache friendly)
# ----------------------------
WORKDIR /prestissimo
COPY Makefile ./
COPY cmake/ cmake/
COPY presto_cpp/ presto_cpp/
COPY velox/ velox/
COPY CMakeLists.txt ./

# Pre-copy submodules (large, but rarely change)
COPY .gitmodules .gitmodules
#COPY third_party/ third_party/

# Create build dirs
RUN mkdir -p ${BUILD_BASE_DIR}/${BUILD_DIR}
RUN mkdir -p /runtime-libraries

# ----------------------------
# STEP 2 — CMake configure step (cacheable)
# ----------------------------
RUN --mount=type=cache,target=/root/.ccache \
    --mount=type=cache,target=/prestissimo/${BUILD_BASE_DIR}/${BUILD_DIR} \
    cmake -B ${BUILD_BASE_DIR}/${BUILD_DIR} \
          -DCMAKE_BUILD_TYPE=${BUILD_TYPE} \
          ${EXTRA_CMAKE_FLAGS}

# ----------------------------
# STEP 3 — source code that changes frequently
# (this triggers minimal rebuild)
# ----------------------------
COPY . /prestissimo/

# ----------------------------
# STEP 4 — incremental build
# ----------------------------
RUN --mount=type=cache,target=/root/.ccache \
    --mount=type=cache,target=/prestissimo/${BUILD_BASE_DIR}/${BUILD_DIR} \
    cmake --build ${BUILD_BASE_DIR}/${BUILD_DIR} -j ${NUM_THREADS} && \
    ccache -sz

# ----------------------------
# STEP 5 — dependency scan
# ----------------------------
RUN ldd /prestissimo/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server \
    | awk 'NF == 4 { system("cp " $3 " /runtime-libraries") }'


# ===================================
# Stage 2 — final runtime image
# ===================================
FROM ${BASE_IMAGE}

ENV BUILD_BASE_DIR=_build
ENV BUILD_DIR=release

COPY --chmod=0775 --from=prestissimo-build /prestissimo/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server /usr/bin/
COPY --chmod=0775 --from=prestissimo-build /runtime-libraries/* /usr/lib64/prestissimo-libs/
COPY --chmod=0755 ./etc /opt/presto-server/etc
COPY --chmod=0775 ./entrypoint.sh /opt/entrypoint.sh

RUN echo "/usr/lib64/prestissimo-libs" > /etc/ld.so.conf.d/prestissimo.conf && ldconfig
ENTRYPOINT ["/opt/entrypoint.sh"]
