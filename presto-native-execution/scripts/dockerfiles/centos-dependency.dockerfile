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

FROM quay.io/centos/centos:stream9

# Set this when build arm with common flags
# from https://github.com/facebookincubator/velox/pull/14366
ARG ARM_BUILD_TARGET

# This defaults to 12.9 but can be overridden with a build arg
ARG CUDA_VERSION

ENV PROMPT_ALWAYS_RESPOND=y
ENV CC=/opt/rh/gcc-toolset-12/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-12/root/bin/g++
ENV ARM_BUILD_TARGET=${ARM_BUILD_TARGET}
ENV CUDA_VERSION=${CUDA_VERSION:-12.9}
ENV UCX_VERSION="1.19.0"

RUN mkdir -p /scripts /velox/scripts
COPY scripts /scripts
COPY velox/scripts /velox/scripts
# Copy extra script called during setup.
# from https://github.com/facebookincubator/velox/pull/14016
COPY velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch /velox
COPY velox/CMake/resolve_dependency_modules/arrow/arrow-testing-boost.patch /velox
COPY CMake/arrow/arrow-flight.patch /scripts
COPY velox/CMake/resolve_dependency_modules/fbthrift/compactv1-protocol-refiller.patch /velox
# NOTE: VELOX_ARROW_CMAKE_PATCH is a space-separated list and, when
# set, OVERRIDES velox's auto-resolution in install_arrow — every
# patch velox would have applied must be COPYed above and listed here.
# Adding a new patch to velox/CMake/resolve_dependency_modules/arrow/
# without also adding it below will silently fail to apply.
ENV VELOX_ARROW_CMAKE_PATCH="/velox/cmake-compatibility.patch /velox/arrow-testing-boost.patch"
ENV EXTRA_ARROW_PATCH=/scripts/arrow-flight.patch
ENV VELOX_FBTHRIFT_CMAKE_PATCH=/velox/compactv1-protocol-refiller.patch
RUN bash -c "mkdir build && \
    (cd build && ../scripts/setup-centos.sh && \
                 ../scripts/setup-adapters.sh && \
                 source ../velox/scripts/setup-centos9.sh && \
                 source ../velox/scripts/setup-centos-adapters.sh && \
                 install_adapters && \
                 install_clang15 && \
                 install_cuda ${CUDA_VERSION} && \
                 install_ucx) && \
    rm -rf build"

# put CUDA binaries on the PATH
ENV PATH=/usr/local/cuda/bin:${PATH}

# configuration for nvidia-container-toolkit
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES="compute,utility"
