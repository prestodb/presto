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

ARG CUDA_VERSION

# Optional override; when unset, Velox's install_ucx picks its own default version.
ARG UCX_VERSION

ENV PROMPT_ALWAYS_RESPOND=y
ENV CC=/opt/rh/gcc-toolset-12/root/bin/gcc
ENV CXX=/opt/rh/gcc-toolset-12/root/bin/g++
ENV ARM_BUILD_TARGET=${ARM_BUILD_TARGET}
ENV CUDA_VERSION=${CUDA_VERSION:-13.0}

RUN mkdir -p /scripts /velox/scripts
COPY scripts /scripts
COPY velox/scripts /velox/scripts
# Copy extra script called during setup.
# from https://github.com/facebookincubator/velox/pull/14016
COPY velox/CMake/resolve_dependency_modules/arrow/cmake-compatibility.patch /velox
COPY velox/CMake/resolve_dependency_modules/arrow/arrow-testing-boost.patch /velox
ENV VELOX_ARROW_CMAKE_PATCH="/velox/cmake-compatibility.patch /velox/arrow-testing-boost.patch"
# from https://github.com/facebookincubator/velox/pull/18470
COPY velox/CMake/resolve_dependency_modules/openzl/openzl-cxx-standard.patch /velox
ENV VELOX_OPENZL_CMAKE_PATCH=/velox/openzl-cxx-standard.patch
COPY CMake/arrow/arrow-flight.patch /scripts
ENV EXTRA_ARROW_PATCH=/scripts/arrow-flight.patch
RUN bash -c "mkdir build && \
    (cd build && export VELOX_BUILD_SHARED=ON && \
                 ../scripts/setup-centos.sh && \
                 ../scripts/setup-adapters.sh && \
                 source ../velox/scripts/setup-centos9.sh && \
                 source ../velox/scripts/setup-centos-adapters.sh && \
                 install_adapters && \
                 install_clang15 && \
                 install_cuda ${CUDA_VERSION} && \
                 install_ucx) && \
    rm -rf build"

# Install sccache for optional S3-backed compile caching.
# Use NVIDIA's RAPIDS fork, not upstream mozilla/sccache: upstream has no nvcc device
# LTO support (no -ltoir/.ltoir handling as of v0.17.0 and main), so cudf's JIT LTO
# fragments -- nvcc -x cu -rdc=true -fatbin with code=[compute_XX,lto_XX] -- die with
# "fatbinary fatal : Could not open input file 'kernel.ptx'". The fork implements those
# paths and is what RAPIDS CI uses.
# See: https://github.com/rapidsai/sccache
# Checksums come from the release's .sha256 files; update them together with the version.
ARG SCCACHE_VERSION="0.17.0-rapids.2"
ARG SCCACHE_SHA256_X86_64="19bd470e09ecaf38d195c7ddbc3585e4c1164d64d65fb5f15120cc7df3f15e67"
ARG SCCACHE_SHA256_AARCH64="f04441eeca46bb14212f03ebed6aaff874d6174d55b540aef9bce10adf7805ff"
RUN set -eu && \
    arch="$(uname -m)" && \
    case "${arch}" in \
      x86_64) sha256="${SCCACHE_SHA256_X86_64}" ;; \
      aarch64) sha256="${SCCACHE_SHA256_AARCH64}" ;; \
      *) echo "Unsupported architecture for sccache: ${arch}" >&2; exit 1 ;; \
    esac && \
    tarball="/tmp/sccache.tar.gz" && \
    wget -q -O "${tarball}" "https://github.com/rapidsai/sccache/releases/download/v${SCCACHE_VERSION}/sccache-v${SCCACHE_VERSION}-${arch}-unknown-linux-musl.tar.gz" && \
    echo "${sha256}  ${tarball}" | sha256sum -c - && \
    tar -C /usr/bin -zxf "${tarball}" --wildcards --strip-components=1 '*/sccache' && \
    rm -f "${tarball}" && \
    chmod +x /usr/bin/sccache && \
    sccache --version

# put CUDA binaries on the PATH
ENV PATH=/usr/local/cuda/bin:${PATH}

# configuration for nvidia-container-toolkit
ENV NVIDIA_VISIBLE_DEVICES=all
ENV NVIDIA_DRIVER_CAPABILITIES="compute,utility"
