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
# Build the test and build container for presto_cpp

# This multi-stage Dockerfile contains the following relevant targets:
#   - centos9: Our base CI build
#   - adapters: Based on centos9 with all optional dependencies installed
#   - pyvelox: Image used by cibuildwheel to build pyvelox

########################
# Stage 1: Base Build  #
########################
ARG image=quay.io/centos/centos:stream9
FROM $image AS base-build

COPY scripts/setup-helper-functions.sh /
COPY scripts/setup-versions.sh /
COPY scripts/setup-common.sh /
COPY scripts/setup-centos9.sh /

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
ENV CMAKE_POLICY_VERSION_MINIMUM="3.5"

# Some CMake configs contain the hard coded prefix '/deps', we need to replace that with
# the future location to avoid build errors in the base-image
RUN bash /setup-centos9.sh && \
    find $INSTALL_PREFIX/lib/cmake -type f -name '*.cmake' -exec sed -i 's|/deps/|/usr/local/|g' {} \;

########################
# Stage 2: Base Image  #
########################
FROM $image AS base-image

COPY scripts/setup-helper-functions.sh /
COPY scripts/setup-versions.sh /
COPY scripts/setup-common.sh /
COPY scripts/setup-centos9.sh /

# This way it's on the PATH and doesn't clash with the version installed in manylinux
ENV UV_TOOL_BIN_DIR=/usr/local/bin \
    UV_INSTALL_DIR=/usr/local/bin

RUN /bin/bash -c 'source /setup-centos9.sh && \
      install_build_prerequisites && \
      install_velox_deps_from_dnf && \
      dnf clean all'

RUN ln -s $(which python3) /usr/bin/python

COPY --from=base-build /deps /usr/local

########################
# Stage: Centos 9      #
########################
FROM base-image AS centos9

# Install tools needed for CI
RUN /bin/bash -c "source /setup-centos9.sh && \
      dnf_install 'dnf-command(config-manager)' && \
      dnf config-manager --add-repo 'https://cli.github.com/packages/rpm/gh-cli.repo' && \
      dnf_install gh jq && \
      dnf clean all"

ENV CC=/opt/rh/gcc-toolset-12/root/bin/gcc \
    CXX=/opt/rh/gcc-toolset-12/root/bin/g++

ENTRYPOINT ["/bin/bash", "-c", "source /opt/rh/gcc-toolset-12/enable && exec \"$@\"", "--"]
CMD ["/bin/bash"]

########################
# Stage: PyVelox       #
########################
FROM base-image AS pyvelox

ENV LD_LIBRARY_PATH="/usr/local/lib:/usr/local/lib64:$LD_LIBRARY_PATH"

########################
# Stage: Adapters Build#
########################
FROM $image AS adapters-build

COPY scripts/setup-helper-functions.sh /
COPY scripts/setup-versions.sh /
COPY scripts/setup-common.sh /
COPY scripts/setup-centos9.sh /
COPY scripts/setup-centos-adapters.sh /

RUN mkdir build
WORKDIR /build

ENV UV_TOOL_BIN_DIR=/usr/local/bin \
    UV_INSTALL_DIR=/usr/local/bin \
    INSTALL_PREFIX=/deps

RUN bash -c 'source /setup-centos9.sh && install_build_prerequisites'

RUN bash /setup-centos-adapters.sh install_adapters && \
    find $INSTALL_PREFIX/lib/cmake -type f -name '*.cmake' -exec sed -i 's|/deps/|/usr/local/|g' {} \;

########################
# Stage: Adapters      #
########################
FROM centos9 AS adapters

COPY scripts/setup-centos-adapters.sh /

RUN bash /setup-centos-adapters.sh install_cuda && \
      dnf clean all

RUN bash /setup-centos-adapters.sh install_adapters_deps_from_dnf && \
      dnf clean all

# put CUDA binaries on the PATH
ENV PATH=/usr/local/cuda/bin:${PATH}

# configuration for nvidia-container-toolkit
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES="compute,utility"

# Install test dependencies
RUN uv pip install --system https://github.com/googleapis/storage-testbench/archive/refs/tags/v0.36.0.tar.gz

RUN curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.40.3/install.sh | bash
RUN . /root/.nvm/nvm.sh && nvm install 22

# Install azurite and symlink nvm managed binaries into /usr/local/bin which is on the path
RUN . /root/.nvm/nvm.sh && npm install -g azurite && \
      ln -s $(dirname $(which node))/* /usr/local/bin

ENV HADOOP_HOME=/usr/local/hadoop \
    HADOOP_ROOT_LOGGER="WARN,DRFA" \
    LC_ALL=C \
    PATH=/usr/local/hadoop/bin:${PATH} \
    JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk \
    PATH=/usr/lib/jvm/java-1.8.0-openjdk/bin:${PATH}

COPY --from=adapters-build /deps /usr/local

COPY scripts/setup-classpath.sh /
ENTRYPOINT ["/bin/bash", "-c", "source /setup-classpath.sh && source /opt/rh/gcc-toolset-12/enable && exec \"$@\"", "--"]
CMD ["/bin/bash"]
