# Copyright (c) Facebook, Inc. and its affiliates.
#
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

########################
# Stage 1: Base Build  #
########################
ARG base=quay.io/fedora/fedora:42-x86_64
FROM $base AS base-build

COPY scripts/setup-helper-functions.sh /
COPY scripts/setup-versions.sh /
COPY scripts/setup-common.sh /
COPY scripts/setup-centos9.sh /
COPY scripts/setup-fedora.sh /
COPY CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch /

ARG VELOX_BUILD_SHARED=ON
# Building libvelox.so requires folly and gflags to be built shared as well for now
ENV VELOX_BUILD_SHARED=${VELOX_BUILD_SHARED}

RUN mkdir build
WORKDIR /build

# We don't want UV symlinks to be copied into the following stages
ENV UV_TOOL_BIN_DIR=/usr/local/bin \
    UV_INSTALL_DIR=/usr/local/bin \
    INSTALL_PREFIX=/deps

# CMake 4.0 removed support for cmake minimums of <=3.5 and will fail builds, this overrides it
ENV CMAKE_POLICY_VERSION_MINIMUM="3.5" \
    VELOX_ARROW_CMAKE_PATCH=/cmake-compatibility.patch

# Some CMake configs contain the hard coded prefix '/deps', we need to replace that with
# the future location to avoid build errors in the base-image
RUN bash /setup-fedora.sh && \
    find $INSTALL_PREFIX/lib/cmake -type f -name '*.cmake' -exec sed -i 's|/deps/|/usr/local/|g' {} \;

########################
# Stage 2: Base Image  #
########################
FROM $base AS fedora

COPY scripts/setup-helper-functions.sh /
COPY scripts/setup-versions.sh /
COPY scripts/setup-common.sh /
COPY scripts/setup-centos9.sh /
COPY scripts/setup-fedora.sh /

# This way it's on the PATH and doesn't clash with the version installed in manylinux
ENV UV_TOOL_BIN_DIR=/usr/local/bin \
    UV_INSTALL_DIR=/usr/local/bin

RUN /bin/bash -c 'source /setup-fedora.sh && \
      install_build_prerequisites && \
      install_velox_deps_from_dnf && \
      dnf clean all'

RUN ln -s $(which python3) /usr/bin/python

COPY --from=base-build /deps /usr/local
