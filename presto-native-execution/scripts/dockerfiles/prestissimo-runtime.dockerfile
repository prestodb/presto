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

ARG DEPENDENCY_IMAGE=presto/prestissimo-dependency:centos9
ARG BASE_IMAGE=quay.io/centos/centos:stream9
FROM ${DEPENDENCY_IMAGE} as prestissimo-image

ARG OSNAME=centos
ARG BUILD_TYPE=Release
ARG PRESTO_OPTIONAL_FEATURES=''
ARG EXTRA_CMAKE_FLAGS=''
ARG NUM_THREADS=8
ARG CUDA_ARCHITECTURES=""
ENV CUDA_ARCHITECTURES=${CUDA_ARCHITECTURES}

# Optional: sccache with S3 backend for shared compile caching across builds
# Set SCCACHE_BUCKET to enable (e.g., --build-arg SCCACHE_BUCKET=my-bucket)
ARG SCCACHE_BUCKET=""
ARG SCCACHE_REGION=""
ARG SCCACHE_S3_KEY_PREFIX=""

ENV PROMPT_ALWAYS_RESPOND=n
ENV BUILD_BASE_DIR=_build
ENV BUILD_DIR=""

RUN mkdir -p /prestissimo /runtime-libraries
COPY . /prestissimo/
RUN --mount=type=cache,target=/root/.ccache,sharing=locked \
    /bin/bash -c '\
    if [[ "${EXTRA_CMAKE_FLAGS}" =~ -DPRESTO_ENABLE_CUDF=ON ]] || [[ ",${PRESTO_OPTIONAL_FEATURES}," =~ ,cudf, ]]; then unset CC; unset CXX; source /opt/rh/gcc-toolset-14/enable; fi && \
    COMPILER_LAUNCHER_FLAGS="" && \
    if [ -n "${SCCACHE_BUCKET}" ] && command -v sccache &>/dev/null; then \
        export SCCACHE_BUCKET="${SCCACHE_BUCKET}" SCCACHE_CACHE_SIZE=2G; \
        [ -n "${SCCACHE_REGION}" ] && export SCCACHE_REGION="${SCCACHE_REGION}"; \
        [ -n "${SCCACHE_S3_KEY_PREFIX}" ] && export SCCACHE_S3_KEY_PREFIX="${SCCACHE_S3_KEY_PREFIX}"; \
        sccache --start-server && \
        COMPILER_LAUNCHER_FLAGS="-DCMAKE_C_COMPILER_LAUNCHER=sccache -DCMAKE_CXX_COMPILER_LAUNCHER=sccache -DCMAKE_CUDA_COMPILER_LAUNCHER=sccache"; \
        echo "Using sccache with S3 backend: s3://${SCCACHE_BUCKET}/${SCCACHE_S3_KEY_PREFIX:-}"; \
    else \
        echo "Using ccache (local)"; \
    fi && \
    PRESTO_OPTIONAL_FEATURES=${PRESTO_OPTIONAL_FEATURES} \
    EXTRA_CMAKE_FLAGS="${EXTRA_CMAKE_FLAGS} ${COMPILER_LAUNCHER_FLAGS}" \
    NUM_THREADS=${NUM_THREADS} make --directory="/prestissimo/" cmake-and-build BUILD_TYPE=${BUILD_TYPE} BUILD_DIR=${BUILD_DIR} BUILD_BASE_DIR=${BUILD_BASE_DIR} && \
    if [ -n "${SCCACHE_BUCKET}" ] && command -v sccache &>/dev/null; then (sccache --stop-server && sccache --show-stats) || true ; else ccache -sz -v; fi'
RUN !(LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64 ldd /prestissimo/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server | grep "not found" | grep -v libcuda) && \
    LD_LIBRARY_PATH=${LD_LIBRARY_PATH}:/usr/local/lib:/usr/local/lib64 ldd /prestissimo/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server | awk 'NF == 4 { system("cp " $3 " /runtime-libraries") }'

RUN cp -rf /usr/local/lib/ucx /runtime-libraries/ucx
RUN echo "${CUDA_VERSION}" > /cuda_version

#/////////////////////////////////////////////
#          prestissimo-runtime
#//////////////////////////////////////////////

FROM ${BASE_IMAGE}

ENV BUILD_BASE_DIR=_build
ENV BUILD_DIR=""

# Copy scripts and CUDA version from build stage
COPY --from=prestissimo-image /prestissimo/velox/scripts/ /tmp/scripts/
COPY --from=prestissimo-image /cuda_version /tmp/

# Install CUDA runtime packages and RDMA libraries
RUN CUDA_VERSION=$(cat /tmp/cuda_version) && \
    source /tmp/scripts/setup-centos-adapters.sh && \
    install_cuda_runtime "${CUDA_VERSION}" && \
    dnf install -y librdmacm libibverbs && \
    dnf clean all && \
    rm -rf /var/cache/dnf /tmp/scripts /tmp/cuda_version

COPY --chmod=0775 --from=prestissimo-image /prestissimo/${BUILD_BASE_DIR}/${BUILD_DIR}/presto_cpp/main/presto_server /usr/bin/
COPY --chmod=0775 --from=prestissimo-image /runtime-libraries/* /usr/lib64/prestissimo-libs/
COPY --chmod=0775 --from=prestissimo-image /runtime-libraries/ucx /usr/lib64/prestissimo-libs/ucx
COPY --chmod=0755 ./etc /opt/presto-server/etc
COPY --chmod=0775 ./entrypoint.sh /opt/entrypoint.sh
RUN echo "/usr/lib64/prestissimo-libs" > /etc/ld.so.conf.d/prestissimo.conf && \
    echo "/usr/lib64/prestissimo-libs/ucx" >> /etc/ld.so.conf.d/prestissimo.conf && \
    echo "/usr/local/cuda/lib64" > /etc/ld.so.conf.d/cuda.conf && \
    ldconfig

ENTRYPOINT ["/opt/entrypoint.sh"]
