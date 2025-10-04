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

ARG base=ubuntu:22.04
FROM ${base}

# Set a default timezone, can be overriden via ARG
ARG tz="America/New_York"
ARG DEBIAN_FRONTEND="noninteractive"

# Set this when build arm with common flags
# from https://github.com/facebookincubator/velox/pull/14366
ARG ARM_BUILD_TARGET

ENV PROMPT_ALWAYS_RESPOND=y
ENV SUDO=" "
# TZ and DEBIAN_FRONTEND="noninteractive"
# are required to avoid tzdata installation
# to prompt for region selection.
ENV TZ=${tz}
ENV ARM_BUILD_TARGET=${ARM_BUILD_TARGET}

RUN mkdir -p /scripts /velox/scripts
COPY scripts /scripts
COPY velox/scripts /velox/scripts
# Copy extra script called during setup.
# from https://github.com/facebookincubator/velox/pull/14016
COPY velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch /velox
ENV VELOX_ARROW_CMAKE_PATCH=/velox/cmake-compatibility.patch

RUN if [ "$(dpkg --print-architecture)" = "arm64" ]; then \
      apt update && \
      apt install -y software-properties-common && \
      add-apt-repository -y ppa:ubuntu-toolchain-r/test && \
      apt update && \
      apt install -y gcc-12 g++-12; \
    fi

# install rpm needed for minio install.
RUN mkdir build && \
    (cd build && apt update && apt install -y rpm sudo && \
                 ../scripts/setup-ubuntu.sh && \
                 ../velox/scripts/setup-ubuntu.sh install_adapters && \
                 ../scripts/setup-adapters.sh ) && \
    rm -rf build
