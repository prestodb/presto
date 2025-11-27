# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# YScope Presto Builder Image
# ===========================
# A unified builder for presto (Java) and prestocpp (C++).
#
# Adapted from upstream's ubuntu-22.04-dependency.dockerfile, with additions:
# - Pre-warmed ccache for faster C++ builds
# - Pre-downloaded Maven dependencies for faster Java builds
# - Pre-downloaded Node.js/Yarn for frontend builds
#
# Tagged by hash of dependency files, rebuilt only when deps change.

FROM ghcr.io/y-scope/docker-github-actions-runner:ubuntu-jammy

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

# ============================================================================
# Dependency Installation (from upstream ubuntu-22.04-dependency.dockerfile)
# ============================================================================

COPY ./presto-native-execution/scripts /presto/scripts/
COPY ./presto-native-execution/velox/scripts /presto/velox/scripts/

# Required to avoid tzdata prompting for region selection
ARG DEBIAN_FRONTEND="noninteractive"
ARG tz="Etc/UTC"
ENV TZ=${tz}
ENV PROMPT_ALWAYS_RESPOND=n
ENV SUDO=" "

# Build parallelism for 32-core self-hosted runners
# See: https://github.com/y-scope/velox/pull/45
ARG NUM_THREADS=16
ARG MAX_HIGH_MEM_JOBS=16
ARG MAX_LINK_JOBS=12
ENV MAX_HIGH_MEM_JOBS=${MAX_HIGH_MEM_JOBS}
ENV MAX_LINK_JOBS=${MAX_LINK_JOBS}

# Install CMake 3.28.3 (required - setup script's pip cmake causes fastfloat issues)
RUN apt-get update && \
    apt-get install -y --no-install-recommends wget && \
    apt-get clean && rm -rf /var/lib/apt/lists/* && \
    wget -q https://github.com/Kitware/CMake/releases/download/v3.28.3/cmake-3.28.3-linux-x86_64.tar.gz && \
    tar -xzf cmake-3.28.3-linux-x86_64.tar.gz -C /opt && \
    rm cmake-3.28.3-linux-x86_64.tar.gz && \
    ln -sf /opt/cmake-3.28.3-linux-x86_64/bin/cmake /usr/local/bin/cmake && \
    ln -sf /opt/cmake-3.28.3-linux-x86_64/bin/ctest /usr/local/bin/ctest && \
    ln -sf /opt/cmake-3.28.3-linux-x86_64/bin/cpack /usr/local/bin/cpack

# Run setup scripts - same pattern as upstream ubuntu-22.04-dependency.dockerfile
# rpm is needed for MinIO installation (S3-compatible storage for tests)
RUN mkdir -p /build && \
    cd /build && \
    /presto/scripts/setup-ubuntu.sh && \
    apt install -y rpm && \
    /presto/velox/scripts/setup-ubuntu.sh install_adapters && \
    /presto/scripts/setup-adapters.sh && \
    rm -rf /build

ENV PATH="/presto/.venv/bin:${PATH}"
ENV VIRTUAL_ENV="/presto/.venv"

# ============================================================================
# ccache Warmup (YScope addition for faster C++ builds)
# See: https://github.com/y-scope/velox/pull/45
# ============================================================================

# ccache settings for portable cache (works across different checkout paths)
# - CCACHE_DIR: Standard location in /var/cache for system caches
# - CCACHE_BASEDIR: Set at runtime via GITHUB_WORKSPACE for portability
# - CCACHE_COMPRESSLEVEL=0: Disabled for faster CI execution (disk space not a concern)
# - CCACHE_NOHASHDIR: Ignore directory paths in hash for cache hits across checkouts
ENV CCACHE_DIR=/var/cache/ccache
ENV CCACHE_COMPRESSLEVEL=0
ENV CCACHE_MAX_SIZE=5G
ENV CCACHE_NOHASHDIR=true

RUN mkdir -p ${CCACHE_DIR} && chmod 777 ${CCACHE_DIR}

COPY . /workspace/
WORKDIR /workspace

# Build prestocpp once to populate ccache
# Build flags must match CI builds exactly for cache hits (see prestocpp-linux-build-and-unit-test.yml)
# CCACHE_BASEDIR set to /workspace for the warmup build
RUN ccache -z && \
    export CCACHE_BASEDIR=/workspace && \
    cd presto-native-execution && \
    cmake \
      -B _build/release \
      -GNinja \
      -DTREAT_WARNINGS_AS_ERRORS=1 \
      -DENABLE_ALL_WARNINGS=1 \
      -DCMAKE_BUILD_TYPE=Release \
      -DPRESTO_ENABLE_PARQUET=ON \
      -DPRESTO_ENABLE_REMOTE_FUNCTIONS=ON \
      -DPRESTO_ENABLE_JWT=ON \
      -DPRESTO_STATS_REPORTER_TYPE=PROMETHEUS \
      -DPRESTO_MEMORY_CHECKER_TYPE=LINUX_MEMORY_CHECKER \
      -DCMAKE_PREFIX_PATH=/usr/local \
      -DThrift_ROOT=/usr/local \
      -DCMAKE_CXX_COMPILER_LAUNCHER=ccache \
      -DMAX_LINK_JOBS=${MAX_LINK_JOBS} && \
    ninja -C _build/release -j ${NUM_THREADS} && \
    ccache -svz

# ============================================================================
# Maven/Node.js Cache (YScope addition for faster Java builds)
# ============================================================================

ENV MAVEN_REPO=/opt/maven/repository
RUN mkdir -p ${MAVEN_REPO}

# Download dependencies using temporary Java installation
RUN wget -q https://github.com/adoptium/temurin8-binaries/releases/download/jdk8u442-b06/OpenJDK8U-jdk_x64_linux_hotspot_8u442b06.tar.gz && \
    tar -xzf OpenJDK8U-jdk_x64_linux_hotspot_8u442b06.tar.gz -C /tmp && \
    rm OpenJDK8U-jdk_x64_linux_hotspot_8u442b06.tar.gz && \
    export JAVA_HOME=/tmp/jdk8u442-b06 && \
    export PATH=${JAVA_HOME}/bin:${PATH} && \
    export RUNNER_OS=Linux && \
    export RUNNER_ARCH=X64 && \
    cd /workspace && \
    .github/bin/download_nodejs && \
    ./mvnw dependency:resolve-plugins dependency:resolve -B --no-transfer-progress \
      -Dmaven.repo.local=${MAVEN_REPO} || true && \
    rm -rf /tmp/jdk8u442-b06

# Clean up source, keep only caches
RUN rm -rf /workspace/*

WORKDIR /workspace
